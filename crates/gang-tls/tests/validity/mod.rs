//! Certificate validity follows the injected clock, not the host: a certificate is accepted inside
//! its window, refused as expired once the clock passes `not_after`, and refused as not yet valid
//! before `not_before`. The acceptor judges client certificates and the connector judges the
//! server certificate, also when the client pins the CA.

use std::time::Duration;

use gang_testkit::Window;
use gang_tls::TransportError;
use rustls::CertificateError;

use crate::support::{ServerChain, TestClient, TransportFixture, certificate_refusal, refused_by};

/// A window containing the fixture clock, which starts on 2030-06-01.
const VALID_NOW: Window = ((2030, 5, 1), (2030, 6, 10));

/// A window that starts after the fixture clock.
const NOT_YET_VALID: Window = ((2030, 7, 1), (2030, 8, 1));

/// How far the clock moves past the end of [`VALID_NOW`].
const PAST_EXPIRY: Duration = Duration::from_hours(20 * 24);

/// Runs a handshake from `client` against the fixture's acceptor and returns the acceptor's
/// certificate refusal.
async fn acceptor_refusal(fixture: &TransportFixture, client: &TestClient) -> CertificateError {
    let (client_io, server_io) = TransportFixture::pipe();
    let (server, _client) = tokio::join!(
        fixture.acceptor().accept(server_io),
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    );
    certificate_refusal(server)
}

fn is_expired(refusal: &CertificateError) -> bool {
    matches!(
        refusal,
        CertificateError::Expired | CertificateError::ExpiredContext { .. }
    )
}

fn is_not_yet_valid(refusal: &CertificateError) -> bool {
    matches!(
        refusal,
        CertificateError::NotValidYet | CertificateError::NotValidYetContext { .. }
    )
}

#[tokio::test(start_paused = true)]
async fn acceptor_accepts_client_certificate_within_its_window() {
    let fixture = TransportFixture::new();
    let client = fixture.client_within(1, "in-window", VALID_NOW);

    let (client_io, server_io) = TransportFixture::pipe();
    let (server, client_result) = tokio::join!(
        fixture.acceptor().accept(server_io),
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
    );
    client_result.expect("client handshake completes");
    server.expect("acceptor accepts a certificate inside its window");
}

#[tokio::test(start_paused = true)]
async fn acceptor_refuses_client_certificate_once_its_window_has_passed() {
    let fixture = TransportFixture::new();
    let client = fixture.client_within(2, "expires", VALID_NOW);
    fixture.clock().advance(PAST_EXPIRY);

    let refusal = acceptor_refusal(&fixture, &client).await;
    assert!(
        is_expired(&refusal),
        "a client certificate past not_after is refused as expired, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn acceptor_refuses_client_certificate_before_its_window_opens() {
    let fixture = TransportFixture::new();
    let client = fixture.client_within(3, "backdated", NOT_YET_VALID);

    let refusal = acceptor_refusal(&fixture, &client).await;
    assert!(
        is_not_yet_valid(&refusal),
        "a client certificate before not_before is refused as not yet valid, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn connector_accepts_server_certificate_within_its_window() {
    let fixture = TransportFixture::with_server_window(VALID_NOW);

    let result = refused_by(
        fixture.anonymous_connector(),
        tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafAndCa)),
    )
    .await;
    result.expect("connector accepts a server certificate inside its window");
}

#[tokio::test(start_paused = true)]
async fn connector_refuses_server_certificate_once_its_window_has_passed() {
    let fixture = TransportFixture::with_server_window(VALID_NOW);
    fixture.clock().advance(PAST_EXPIRY);

    let result = refused_by(
        fixture.anonymous_connector(),
        tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafAndCa)),
    )
    .await;
    let refusal = certificate_refusal(result);
    assert!(
        is_expired(&refusal),
        "a server certificate past not_after is refused as expired, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn connector_refuses_server_certificate_before_its_window_opens() {
    let fixture = TransportFixture::with_server_window(NOT_YET_VALID);

    let result = refused_by(
        fixture.anonymous_connector(),
        tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafAndCa)),
    )
    .await;
    let refusal = certificate_refusal(result);
    assert!(
        is_not_yet_valid(&refusal),
        "a server certificate before not_before is refused as not yet valid, got {refusal:?}"
    );
}

/// A client pinned to the fixture's CA handshakes with the fixture's server certificate.
async fn pinned_handshake(fixture: &TransportFixture) -> Result<(), TransportError> {
    let pin = fixture.ca_pin();
    refused_by(
        fixture.pinned_connector(&pin, None),
        tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafAndCa)),
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn pinned_connector_accepts_server_certificate_within_its_window() {
    let fixture = TransportFixture::with_server_window(VALID_NOW);

    pinned_handshake(&fixture)
        .await
        .expect("pinned connector accepts a server certificate inside its window");
}

#[tokio::test(start_paused = true)]
async fn pinned_connector_refuses_server_certificate_once_its_window_has_passed() {
    let fixture = TransportFixture::with_server_window(VALID_NOW);
    fixture.clock().advance(PAST_EXPIRY);

    let refusal = certificate_refusal(pinned_handshake(&fixture).await);
    assert!(
        is_expired(&refusal),
        "a pinned server certificate past not_after is refused as expired, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn pinned_connector_refuses_server_certificate_before_its_window_opens() {
    let fixture = TransportFixture::with_server_window(NOT_YET_VALID);

    let refusal = certificate_refusal(pinned_handshake(&fixture).await);
    assert!(
        is_not_yet_valid(&refusal),
        "pinned server certificate before not_before is not yet valid, got {refusal:?}"
    );
}
