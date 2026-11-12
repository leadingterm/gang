//! Handshake tests: what a connection that succeeds carries, and which peers are refused.

use std::fmt::Debug;

use rustls::{CertificateError, ProtocolVersion};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream};

use gang_tls::{Alpn, ClientAuth, ConnectorSettings, ServerTrust, TlsConnector};
use gang_types::NonZeroDuration;

use crate::support::{ALPN_H2, ALPN_HTTP1, TransportFixture, certificate_refusal, echo_once};

#[tokio::test(start_paused = true)]
async fn valid_certificate_connects_over_tls13_with_h2_and_identifies_the_client() {
    let fixture = TransportFixture::new();
    let client = fixture.client(1, "alpha");
    let connection = fixture.connect(&client).await;
    let mut client_io = connection.client.expect("client handshake completes");
    let mut server = connection.server.expect("server handshake completes");

    assert_eq!(server.peer_serial(), Some(&client.serial));
    assert_eq!(
        client_io.get_ref().1.protocol_version(),
        Some(ProtocolVersion::TLSv1_3)
    );
    assert_eq!(
        server.get_ref().get_ref().1.protocol_version(),
        Some(ProtocolVersion::TLSv1_3)
    );
    assert_eq!(client_io.get_ref().1.alpn_protocol(), Some(ALPN_H2));
    assert_eq!(server.get_ref().get_ref().1.alpn_protocol(), Some(ALPN_H2));

    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn server_presents_leaf_and_ca_chain() {
    let fixture = TransportFixture::new();
    let client = fixture.client(11, "alpha");
    let connection = fixture.connect(&client).await;
    let client_io = connection.client.expect("client handshake completes");

    let chain = client_io
        .get_ref()
        .1
        .peer_certificates()
        .expect("client observes the server certificate chain");
    assert_eq!(chain.len(), 2, "server must present [leaf, CA]");
}

/// Runs a handshake in which the server must refuse the client.
///
/// TLS 1.3 completes the client side before the server validates the client certificate, so the
/// client's handshake succeeds and the rejection surfaces on its first read.
async fn assert_refused<C, E>(
    fixture: &TransportFixture,
    server_io: DuplexStream,
    client: impl Future<Output = Result<C, E>>,
) where
    C: AsyncRead + AsyncWrite + Unpin,
    E: Debug,
{
    let (server, client) = tokio::join!(fixture.acceptor().accept(server_io), client);
    assert!(server.is_err(), "server accepted a refused client");

    let mut client = client.expect("tls 1.3 client finishes before the server rejects it");
    let mut byte = [0u8; 1];
    assert!(
        client.read_exact(&mut byte).await.is_err(),
        "client read succeeded after the server refused it"
    );
}

#[tokio::test(start_paused = true)]
async fn client_without_certificate_is_refused() {
    let fixture = TransportFixture::new();
    let (client_io, server_io) = TransportFixture::pipe();
    let connector = tokio_rustls::TlsConnector::from(fixture.raw_client_config(None, false, true));

    assert_refused(
        &fixture,
        server_io,
        connector.connect(TransportFixture::server_name(), client_io),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn certificate_from_foreign_ca_is_refused() {
    let fixture = TransportFixture::new();
    let client = fixture.foreign_client(2, "mallory");
    let (client_io, server_io) = TransportFixture::pipe();

    assert_refused(
        &fixture,
        server_io,
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn tls12_only_client_is_refused() {
    let fixture = TransportFixture::new();
    let client = fixture.client(3, "beta");
    let (client_io, server_io) = TransportFixture::pipe();
    let connector =
        tokio_rustls::TlsConnector::from(fixture.raw_client_config(Some(&client), true, true));

    // The server speaks TLS 1.3 only, so it rejects the TLS 1.2 hello and the client fails too.
    let (server, client_result) = tokio::join!(
        fixture.acceptor().accept(server_io),
        connector.connect(TransportFixture::server_name(), client_io),
    );
    assert!(server.is_err(), "server accepted a tls 1.2 client");
    assert!(
        client_result.is_err(),
        "tls 1.2 client completed a handshake"
    );
}

#[tokio::test(start_paused = true)]
async fn client_certificate_without_client_auth_usage_is_refused() {
    let fixture = TransportFixture::new();
    let client = fixture.client_lacking_client_auth(4, "delta");
    let (client_io, server_io) = TransportFixture::pipe();

    assert_refused(
        &fixture,
        server_io,
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn server_certificate_for_wrong_name_is_refused_by_client() {
    let fixture = TransportFixture::new();
    let client = fixture.client(5, "gamma");
    let wrong_name = fixture.acceptor_presenting("wrong.example");
    let (client_io, server_io) = TransportFixture::pipe();

    let (server, client_result) = tokio::join!(
        wrong_name.accept(server_io),
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    );
    assert!(
        server.is_err(),
        "server completed a handshake the client aborted"
    );
    assert!(
        matches!(client_result, Err(gang_tls::TransportError::Handshake(_))),
        "client did not refuse the wrong server name with a handshake error"
    );
}

/// A connector that trusts the server only through the bundled Mozilla roots.
fn bundled_connector(fixture: &TransportFixture) -> TlsConnector {
    TlsConnector::try_new(&ConnectorSettings {
        trust: ServerTrust::Bundled,
        identity: None,
        handshake_timeout: NonZeroDuration::from_secs(10),
        clock: fixture.clock(),
        alpn: Alpn::H2,
    })
    .expect("bundled connector builds")
}

#[tokio::test(start_paused = true)]
async fn server_certificate_outside_bundled_roots_is_refused() {
    let fixture = TransportFixture::new();
    let connector = bundled_connector(&fixture);
    let (client_io, server_io) = TransportFixture::pipe();

    let (server, client) = tokio::join!(
        fixture.acceptor().accept(server_io),
        connector.connect(TransportFixture::server_name(), client_io),
    );
    assert!(
        server.is_err(),
        "server completed a handshake the client aborted"
    );
    assert_eq!(
        certificate_refusal(client),
        CertificateError::UnknownIssuer,
        "the test ca is not in the bundled roots"
    );
}

#[tokio::test(start_paused = true)]
async fn anonymous_client_connects_when_client_auth_is_optional() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let connector = fixture.anonymous_connector();
    let connection = fixture.connect_with(&connector).await;
    let mut client_io = connection
        .client
        .expect("anonymous client handshake completes");
    let mut server = connection
        .server
        .expect("anonymous server handshake completes");

    assert!(
        server.peer_serial().is_none(),
        "anonymous client has no identity"
    );
    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn valid_certificate_is_identified_when_client_auth_is_optional() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let client = fixture.client(12, "alpha");
    let connection = fixture.connect(&client).await;
    let mut client_io = connection.client.expect("client handshake completes");
    let mut server = connection.server.expect("server handshake completes");

    assert_eq!(server.peer_serial(), Some(&client.serial));
    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn revoked_certificate_is_refused_when_client_auth_is_optional() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let client = fixture.client(13, "alpha");
    fixture.revocations().revoke(client.serial.clone());

    let connection = fixture.connect(&client).await;
    assert!(
        connection.server.is_err(),
        "server must refuse a revoked client certificate"
    );
    let mut client_io = connection
        .client
        .expect("TLS 1.3 client handshake completes before the rejection");
    assert!(client_io.read_exact(&mut [0_u8; 1]).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn foreign_ca_certificate_is_refused_when_client_auth_is_optional() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let client = fixture.foreign_client(14, "mallory");
    let (client_io, server_io) = TransportFixture::pipe();

    assert_refused(
        &fixture,
        server_io,
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn anonymous_client_connects_when_client_auth_is_none() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::None);
    let connector = fixture.anonymous_connector();
    let connection = fixture.connect_with(&connector).await;
    let mut client_io = connection
        .client
        .expect("anonymous client handshake completes");
    let mut server = connection
        .server
        .expect("anonymous server handshake completes");

    assert!(
        server.peer_serial().is_none(),
        "anonymous client has no identity"
    );
    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn valid_certificate_is_not_identified_when_client_auth_is_none() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::None);
    let client = fixture.client(15, "alpha");
    let connection = fixture.connect(&client).await;
    let mut client_io = connection.client.expect("client handshake completes");
    let mut server = connection.server.expect("server handshake completes");

    assert!(
        server.peer_serial().is_none(),
        "server never asked for a client certificate, so none is identified"
    );
    echo_once(&mut client_io, &mut server).await;
}

/// Completes a handshake in which the client offers `protocols` against an acceptor offering
/// `alpn`, and asserts that both ends agree on `expected`.
async fn assert_negotiates(alpn: Alpn, protocols: &[&[u8]], expected: &[u8]) {
    let fixture = TransportFixture::with_alpn(alpn);
    let connection = fixture.connect_offering(protocols).await;
    let mut client_io = connection.client.expect("client handshake completes");
    let mut server = connection.server.expect("server handshake completes");

    assert_eq!(client_io.get_ref().1.alpn_protocol(), Some(expected));
    assert_eq!(server.get_ref().get_ref().1.alpn_protocol(), Some(expected));
    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn dual_acceptor_negotiates_h2_with_a_client_offering_both() {
    assert_negotiates(Alpn::H2AndHttp1, &[ALPN_H2, ALPN_HTTP1], ALPN_H2).await;
}

#[tokio::test(start_paused = true)]
async fn dual_acceptor_negotiates_http1_with_a_client_offering_http1_only() {
    assert_negotiates(Alpn::H2AndHttp1, &[ALPN_HTTP1], ALPN_HTTP1).await;
}

#[tokio::test(start_paused = true)]
async fn dual_acceptor_negotiates_h2_with_an_h2_only_client() {
    assert_negotiates(Alpn::H2AndHttp1, &[ALPN_H2], ALPN_H2).await;
}

#[tokio::test(start_paused = true)]
async fn h2_only_acceptor_negotiates_h2_with_a_client_offering_both() {
    assert_negotiates(Alpn::H2, &[ALPN_H2, ALPN_HTTP1], ALPN_H2).await;
}

#[tokio::test(start_paused = true)]
async fn h2_only_acceptor_refuses_a_client_offering_http1_only() {
    let fixture = TransportFixture::with_alpn(Alpn::H2);
    let connection = fixture.connect_offering(&[ALPN_HTTP1]).await;

    assert!(
        connection.client.is_err(),
        "client handshake fails without a shared application protocol"
    );
    assert!(
        connection.server.is_err(),
        "server handshake fails without a shared application protocol"
    );
}
