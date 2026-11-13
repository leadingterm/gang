//! Behaviour of the listening endpoints: plain accepts, client authentication, revocation and
//! certificate rotation over in-memory transports.
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::listeners::{Accepting, TlsPolicy};
use crate::{HandshakeSettings, ServerError};
use gang_clock::{Clock, TestClock};
use gang_mtls::{CertificateAuthority, SubjectName};
use gang_spi::{Accepted, Peer};
use gang_testkit::TestSigner;
use gang_tls::{Alpn, ClientAuth, RevocationSet, ServerTrust, TransportError};
use gang_types::{NonZeroDuration, Percent, TrustDomain};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use crate::test_support::{
    MemoryListener, authority, client_certificate, memory_listener, start_clock, tls_client,
    tls_client_over,
};

const HOST: &str = "api.example.test";
const PROVIDER: &str = "example-idp";
const PRINCIPAL: &str = "alice";
/// How far the injected clock moves to force a rotation: past the 50% reissue point of the
/// one-hour certificate, and before the one-hour client certificate issued at the same start
/// expires.
const ROTATION_ADVANCE: Duration = Duration::from_mins(31);
/// How long a listener waits after a failed accept before accepting again.
const ACCEPT_ERROR_BACKOFF: NonZeroDuration = NonZeroDuration::from_millis(100);
/// The reissue point of the test endpoints: half of the certificate's lifetime.
const REISSUE_AT: Percent = Percent::from_percent(50);
/// ALPN identifiers of HTTP/2 and HTTP/1.1.
const H2: &[u8] = b"h2";
const HTTP1: &[u8] = b"http/1.1";

type Tls = Accepting<MemoryListener, TestSigner, TestClock>;

/// Polls `future` once with a no-op waker. `Pending` means nothing was ready at that point.
fn poll_once<F>(future: Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Drives `client` to completion while `endpoint` must accept nothing. A served client fails the
/// test; otherwise the client's own outcome is returned for the caller to assert on.
async fn refused_without_service(client: impl Future<Output = bool>, endpoint: &mut Tls) -> bool {
    tokio::select! {
        cut_off = client => cut_off,
        _ = endpoint.accept() => panic!("the endpoint served a client it must refuse"),
    }
}

fn handshake_settings() -> HandshakeSettings {
    HandshakeSettings {
        handshake_timeout: NonZeroDuration::from_secs(10),
        max_pending_handshakes: NonZeroUsize::new(256).expect("256 is non-zero"),
        refusal_linger: NonZeroDuration::from_secs(2),
    }
}

fn names() -> Vec<SubjectName> {
    vec![SubjectName::Dns(HOST.to_owned())]
}

/// The endpoint policy shared by the tests: a one-hour certificate for the test host, with the
/// client authentication, ALPN, revoked_certificates, and reissue point each test needs.
fn policy(
    client_auth: ClientAuth,
    alpn: Alpn,
    revoked_certificates: RevocationSet,
    handshake: HandshakeSettings,
    reissue_at_percent: Percent,
) -> TlsPolicy {
    TlsPolicy {
        names: names(),
        alpn,
        client_auth,
        revoked_certificates,
        certificate_ttl: NonZeroDuration::from_hours(1),
        reissue_at_percent,
        handshake,
        accept_error_backoff: ACCEPT_ERROR_BACKOFF,
    }
}

/// A client that completes no usable connection: its handshake fails, or the server cuts it off
/// before any application data arrives.
async fn refused(
    dialer: &crate::test_support::MemoryDialer,
    ca: &gang_types::CertificatePem,
    clock: &Arc<TestClock>,
) -> bool {
    match tls_client(dialer, HOST, ca, None, clock).await {
        Err(_) => true,
        Ok(mut stream) => {
            let mut byte = [0_u8; 1];
            matches!(stream.read(&mut byte).await, Err(_) | Ok(0))
        }
    }
}

/// Connects to the server named `host` through `dialer` over TLS 1.3, offering the application
/// protocols `alpn` in order and trusting the certificate authority `ca`.
///
/// # Errors
///
/// Returns the transport error when the handshake fails.
async fn tls_client_offering(
    dialer: &crate::test_support::MemoryDialer,
    host: &str,
    ca: &gang_types::CertificatePem,
    alpn: &[&[u8]],
    clock: &Arc<TestClock>,
) -> Result<tokio_rustls::client::TlsStream<DuplexStream>, TransportError> {
    let clock: Arc<dyn Clock> = clock.clone();
    let config = gang_tls::client_config(ServerTrust::Ca(ca), None, alpn, clock)?;
    let name =
        gang_tls::ServerName::try_from(host.to_owned()).expect("test host is a valid server name");
    let connect = tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, dialer.dial());
    connect.await.map_err(TransportError::Handshake)
}

/// The DER bytes of the server leaf certificate `client` was presented during its handshake.
fn server_leaf(client: &tokio_rustls::client::TlsStream<DuplexStream>) -> Vec<u8> {
    let certificates = client
        .get_ref()
        .1
        .peer_certificates()
        .expect("the server presented a certificate chain");
    certificates[0].to_vec()
}

#[tokio::test]
async fn plain_endpoint_reports_an_anonymous_caller() {
    let (listener, dialer) = memory_listener();
    let mut endpoint = Accepting::<MemoryListener, TestSigner, TestClock>::plain(
        listener,
        ACCEPT_ERROR_BACKOFF,
        start_clock(),
    );

    let mut client = dialer.dial();
    client
        .write_all(b"ping")
        .await
        .expect("duplex accepts bytes");
    let Accepted {
        mut connection,
        peer,
    } = endpoint.accept().await.expect("plain endpoint accepts");

    assert_eq!(peer, Peer::default());
    let mut received = [0_u8; 4];
    connection
        .read_exact(&mut received)
        .await
        .expect("connection carries the client's bytes");
    assert_eq!(&received, b"ping");
}

#[tokio::test]
async fn required_client_certificate_identifies_the_caller() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;

    let (client, accepted) = tokio::join!(
        tls_client(
            &dialer,
            HOST,
            authority.certificate(),
            Some((&issued.certificate, &key)),
            &clock,
        ),
        endpoint.accept(),
    );

    client.expect("client with a certificate connects");
    let accepted = accepted.expect("endpoint accepts the client");
    assert_eq!(accepted.peer.certificate, Some(issued.serial.clone()));
}

#[tokio::test]
async fn required_client_certificate_refuses_anonymous_clients() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let cut_off = refused_without_service(
        refused(&dialer, authority.certificate(), &clock),
        &mut endpoint,
    )
    .await;

    assert!(cut_off, "anonymous client must not be served");
}

#[tokio::test]
async fn optional_client_auth_accepts_anonymous_clients() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let (client, accepted) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );

    client.expect("anonymous client connects");
    let accepted = accepted.expect("endpoint accepts the anonymous client");
    assert_eq!(accepted.peer.certificate, None);
}

#[tokio::test]
async fn revoked_client_certificate_is_refused() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;
    let revoked_certificates = RevocationSet::from_serials(vec![issued.serial.clone()]);
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let cut_off = refused_without_service(
        async {
            match tls_client(
                &dialer,
                HOST,
                authority.certificate(),
                Some((&issued.certificate, &key)),
                &clock,
            )
            .await
            {
                Err(_) => true,
                Ok(mut stream) => {
                    let mut byte = [0_u8; 1];
                    matches!(stream.read(&mut byte).await, Err(_) | Ok(0))
                }
            }
        },
        &mut endpoint,
    )
    .await;

    assert!(cut_off, "revoked client must not be served");
}

#[tokio::test]
async fn rotation_keeps_established_connections_and_admits_new_ones() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;

    let (first_client, first) = tokio::join!(
        tls_client(
            &dialer,
            HOST,
            authority.certificate(),
            Some((&issued.certificate, &key)),
            &clock,
        ),
        endpoint.accept(),
    );
    let mut first_client = first_client.expect("first client connects");
    let Accepted {
        connection: mut first_server,
        ..
    } = first.expect("first connection is accepted");

    clock.advance(ROTATION_ADVANCE);

    let (second_client, second) = tokio::join!(
        tls_client(
            &dialer,
            HOST,
            authority.certificate(),
            Some((&issued.certificate, &key)),
            &clock,
        ),
        endpoint.accept(),
    );
    second_client.expect("client connects after rotation");
    let second = second.expect("endpoint accepts after rotation");
    assert_eq!(second.peer.certificate, Some(issued.serial.clone()));

    first_client
        .write_all(b"ping")
        .await
        .expect("established client writes");
    let mut received = [0_u8; 4];
    first_server
        .read_exact(&mut received)
        .await
        .expect("established server reads");
    assert_eq!(&received, b"ping");
    first_server
        .write_all(b"pong")
        .await
        .expect("established server writes");
    let mut echoed = [0_u8; 4];
    first_client
        .read_exact(&mut echoed)
        .await
        .expect("established client reads");
    assert_eq!(&echoed, b"pong");
}

#[tokio::test]
async fn rotation_delivers_connections_buffered_in_the_replaced_acceptor() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;

    // Nothing accepts while the client handshakes, so the server side waits in the acceptor.
    let _client = tls_client(
        &dialer,
        HOST,
        authority.certificate(),
        Some((&issued.certificate, &key)),
        &clock,
    )
    .await
    .expect("client connects before rotation");
    // The replaced acceptor keeps driving the handshake it has started, so the connection is
    // delivered whether it finished before or after rotation.
    clock.advance(ROTATION_ADVANCE);

    let accepted = endpoint
        .accept()
        .await
        .expect("buffered connection survives rotation");
    assert_eq!(accepted.peer.certificate, Some(issued.serial.clone()));
}

#[tokio::test]
async fn replaced_acceptor_is_retired_and_the_endpoint_keeps_serving() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;

    clock.advance(ROTATION_ADVANCE);
    // No client connects: this poll rotates the certificate, then waits with nothing to return.
    {
        let mut idle = pin!(endpoint.accept());
        assert!(
            poll_once(idle.as_mut()).is_pending(),
            "no client connected while idle"
        );
    }

    let (client, accepted) = tokio::join!(
        tls_client(
            &dialer,
            HOST,
            authority.certificate(),
            Some((&issued.certificate, &key)),
            &clock,
        ),
        endpoint.accept(),
    );
    client.expect("client connects after the retirement deadline");
    let accepted = accepted.expect("endpoint accepts after the retirement deadline");
    assert_eq!(accepted.peer.certificate, Some(issued.serial.clone()));
}

#[tokio::test]
async fn retired_acceptor_takes_no_new_connections() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");
    let (issued, key) = client_certificate(&authority, PROVIDER, PRINCIPAL).await;

    clock.advance(ROTATION_ADVANCE);
    // Rotates the endpoint, then idles. The replaced acceptor is retired 10 s after rotation,
    // and the new acceptor is the only one still waiting on the listener.
    {
        let mut idle = pin!(endpoint.accept());
        assert!(
            poll_once(idle.as_mut()).is_pending(),
            "no client connected while idle"
        );
    }

    // The dialed socket is taken by the acceptor waiting on the listener, whose handshake
    // window opens 1 s after rotation and closes 11 s after rotation.
    let stream = dialer.dial();
    let (client, accepted) = tokio::join!(
        async {
            // The clock advances 9.5 s from the dial, 10.5 s after rotation: past the retirement
            // deadline (10 s) and inside the handshake window.
            clock.advance(Duration::from_millis(9_500));
            tls_client_over(
                stream,
                HOST,
                authority.certificate(),
                Some((&issued.certificate, &key)),
                &clock,
            )
            .await
        },
        endpoint.accept(),
    );
    client.expect("client completes its handshake inside the handshake window");
    let accepted = accepted.expect("endpoint accepts the connection");
    assert_eq!(accepted.peer.certificate, Some(issued.serial.clone()));
}

#[tokio::test]
async fn a_reissue_presents_a_new_server_certificate() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let (first_client, first) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let mut first_client = first_client.expect("first client connects");
    let Accepted {
        connection: mut first_server,
        ..
    } = first.expect("first connection is accepted");
    let first_leaf = server_leaf(&first_client);

    // Past the 30 min reissue point and past the 60 min expiry of the first certificate.
    clock.advance(Duration::from_mins(61));

    let (second_client, second) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let second_client = second_client.expect("client connects after the reissue");
    let _second = second.expect("endpoint accepts after the reissue");
    let second_leaf = server_leaf(&second_client);
    assert_ne!(
        second_leaf, first_leaf,
        "a reissue presents a new server certificate"
    );

    // Past the 10 s handshake timeout the replaced acceptor was given, so the endpoint retires
    // it at its deadline. New clients are still accepted on the new certificate.
    clock.advance(Duration::from_secs(11));
    let (third_client, third) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let third_client = third_client.expect("client connects after the retirement deadline");
    let _third = third.expect("endpoint accepts after the retirement deadline");
    assert_eq!(server_leaf(&third_client), second_leaf);

    // The connection made before the reissue is still served.
    first_client
        .write_all(b"ping")
        .await
        .expect("established client writes");
    let mut received = [0_u8; 4];
    first_server
        .read_exact(&mut received)
        .await
        .expect("established server reads");
    assert_eq!(&received, b"ping");
}

#[tokio::test]
async fn a_failed_reissue_keeps_the_old_certificate_and_retries_after_the_backoff() {
    let clock = start_clock();
    let outage = Arc::new(AtomicBool::new(false));
    let trust_domain = TrustDomain::try_new("example.test").expect("test trust domain is valid");
    let authority = Arc::new(
        CertificateAuthority::establish(
            TestSigner::authority()
                .expect("the test signer builds")
                .with_outage(Arc::clone(&outage)),
            Arc::clone(&clock),
            trust_domain,
            Duration::from_mins(1),
        )
        .await
        .expect("test authority establishes"),
    );
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let (first_client, first) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let first_client = first_client.expect("first client connects");
    let _first = first.expect("first connection is accepted");
    let first_leaf = server_leaf(&first_client);

    // The outage starts before the reissue point, so the reissue fails.
    outage.store(true, Ordering::SeqCst);
    clock.advance(ROTATION_ADVANCE);
    let (second_client, second) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let second_client = second_client.expect("client connects while the reissue fails");
    let _second = second.expect("endpoint keeps accepting while the reissue fails");
    assert_eq!(
        server_leaf(&second_client),
        first_leaf,
        "a failed reissue keeps the old server certificate"
    );

    // The outage clears; the retry is due one accept error backoff after the failed attempt.
    outage.store(false, Ordering::SeqCst);
    clock.advance(ACCEPT_ERROR_BACKOFF.get());
    let (third_client, third) = tokio::join!(
        tls_client(&dialer, HOST, authority.certificate(), None, &clock),
        endpoint.accept(),
    );
    let third_client = third_client.expect("client connects after the retry");
    let _third = third.expect("endpoint accepts after the retry");
    assert_ne!(
        server_leaf(&third_client),
        first_leaf,
        "the retry presents a new server certificate"
    );
}

/// Builds a TLS endpoint whose certificate is reissued at `reissue_at_percent`, and reports
/// whether the endpoint was secured.
async fn secure_with_reissue_percent(reissue_at_percent: Percent) -> Result<(), ServerError> {
    let clock = start_clock();
    let authority = authority(&clock).await;
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, _dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Required,
        Alpn::H2,
        revoked_certificates,
        handshake,
        reissue_at_percent,
    );
    Tls::secured(listener, Arc::new(authority), policy, clock)
        .await
        .map(|_endpoint| ())
}

#[tokio::test]
async fn reissue_percent_of_one_is_accepted() {
    assert!(
        secure_with_reissue_percent(Percent::from_percent(1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn reissue_percent_of_one_hundred_is_accepted() {
    assert!(
        secure_with_reissue_percent(Percent::from_percent(100))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn public_listener_negotiates_http1_with_an_http1_client() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2AndHttp1,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let (client, accepted) = tokio::join!(
        tls_client_offering(&dialer, HOST, authority.certificate(), &[HTTP1], &clock),
        endpoint.accept(),
    );

    let client = client.expect("http/1.1-only client connects");
    assert_eq!(client.get_ref().1.alpn_protocol(), Some(HTTP1));
    accepted.expect("endpoint accepts the http/1.1 client");
}

#[tokio::test]
async fn public_listener_negotiates_h2_with_a_dual_client() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2AndHttp1,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let (client, accepted) = tokio::join!(
        tls_client_offering(&dialer, HOST, authority.certificate(), &[H2, HTTP1], &clock),
        endpoint.accept(),
    );

    let client = client.expect("dual client connects");
    assert_eq!(client.get_ref().1.alpn_protocol(), Some(H2));
    accepted.expect("endpoint accepts the dual client");
}

#[tokio::test]
async fn h2_listener_refuses_an_http1_only_client() {
    let clock = start_clock();
    let authority = Arc::new(authority(&clock).await);
    let revoked_certificates = RevocationSet::from_serials(Vec::new());
    let handshake = handshake_settings();
    let (listener, dialer) = memory_listener();
    let policy = policy(
        ClientAuth::Optional,
        Alpn::H2,
        revoked_certificates,
        handshake,
        REISSUE_AT,
    );
    let mut endpoint = Tls::secured(listener, Arc::clone(&authority), policy, Arc::clone(&clock))
        .await
        .expect("endpoint is secured");

    let cut_off = refused_without_service(
        async {
            tls_client_offering(&dialer, HOST, authority.certificate(), &[HTTP1], &clock)
                .await
                .is_err()
        },
        &mut endpoint,
    )
    .await;

    assert!(cut_off, "http/1.1-only client must fail the handshake");
}
