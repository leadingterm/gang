//! Session resumption is disabled on the server: every connection runs a full handshake and
//! re-verifies the client certificate, so revocation cannot be bypassed by resuming.

use std::io;
use std::sync::Arc;

use gang_tls::{RevocableStream, TransportError};
use rustls::{ClientConfig, HandshakeKind};
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio_rustls::client::TlsStream as ClientStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::support::{TransportFixture, echo_once};

type ServerStream = RevocableStream<ServerTlsStream<DuplexStream>>;

/// Runs one handshake with a raw client configuration over an in-memory pipe.
async fn raw_connect(
    fixture: &TransportFixture,
    config: &Arc<ClientConfig>,
) -> (
    io::Result<ClientStream<DuplexStream>>,
    Result<ServerStream, TransportError>,
) {
    let (client_io, server_io) = TransportFixture::pipe();
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(config));
    tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        fixture.acceptor().accept(server_io),
    )
}

#[tokio::test(start_paused = true)]
async fn second_connection_with_resumption_enabled_is_full_handshake() {
    let fixture = TransportFixture::new();
    let client = fixture.client(8, "alpha");
    let config = fixture.raw_client_config(Some(&client), false, true);

    let (first_client, first_server) = raw_connect(&fixture, &config).await;
    let mut first_client = first_client.expect("first client handshake completes");
    let mut first_server = first_server.expect("first server handshake completes");
    echo_once(&mut first_client, &mut first_server).await;
    drop(first_client);
    drop(first_server);

    let (second_client, second_server) = raw_connect(&fixture, &config).await;
    let mut second_client = second_client.expect("second client handshake completes");
    let mut second_server = second_server.expect("second server handshake completes");
    assert_eq!(
        second_client.get_ref().1.handshake_kind(),
        Some(HandshakeKind::Full),
        "resumption is disabled on the server, so the second handshake must be full"
    );
    assert_eq!(
        second_server.get_ref().get_ref().1.handshake_kind(),
        Some(HandshakeKind::Full)
    );
    echo_once(&mut second_client, &mut second_server).await;
}

#[tokio::test(start_paused = true)]
async fn revoked_client_cannot_resume_past_revocation() {
    let fixture = TransportFixture::new();
    let client = fixture.client(9, "alpha");
    let config = fixture.raw_client_config(Some(&client), false, true);

    let (first_client, first_server) = raw_connect(&fixture, &config).await;
    let mut first_client = first_client.expect("first client handshake completes");
    let mut first_server = first_server.expect("first server handshake completes");
    echo_once(&mut first_client, &mut first_server).await;
    drop(first_client);
    drop(first_server);

    fixture.revocations().revoke(client.serial.clone());

    let (second_client, second_server) = raw_connect(&fixture, &config).await;
    assert!(
        second_server.is_err(),
        "revoked client must be refused on the second connection"
    );
    let mut second_client =
        second_client.expect("TLS 1.3 client handshake completes before the rejection");
    let mut buf = [0_u8; 1];
    assert!(
        second_client.read(&mut buf).await.is_err(),
        "client read must fail after the server rejects the revoked certificate"
    );
}
