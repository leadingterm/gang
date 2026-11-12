//! Handshake deadlines: silent peers are cut by the handshake timeout.

use std::pin::pin;
use std::task::{Context, Waker};

use gang_tls::TransportError;
use gang_types::NonZeroDuration;

use crate::support::TransportFixture;

#[tokio::test]
async fn silent_client_is_cut_by_handshake_timeout() {
    let fixture = TransportFixture::with_handshake_timeout(NonZeroDuration::from_secs(3));
    let (_silent_client, server_io) = TransportFixture::pipe();
    let acceptor = fixture.acceptor();
    let mut handshake = pin!(acceptor.accept(server_io));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(handshake.as_mut().poll(&mut cx).is_pending());
    fixture.clock().advance(NonZeroDuration::from_secs(3).get());
    let result = handshake.await;
    assert!(
        matches!(result, Err(TransportError::HandshakeTimeout)),
        "a silent client must hit the handshake timeout"
    );
}

#[tokio::test]
async fn silent_server_is_cut_by_handshake_timeout() {
    let fixture = TransportFixture::with_handshake_timeout(NonZeroDuration::from_secs(3));
    let (client_io, _silent_server) = TransportFixture::pipe();
    let connector = fixture.anonymous_connector();
    let mut handshake = pin!(connector.connect(TransportFixture::server_name(), client_io));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(handshake.as_mut().poll(&mut cx).is_pending());
    fixture.clock().advance(NonZeroDuration::from_secs(3).get());
    let result = handshake.await;
    assert!(
        matches!(result, Err(TransportError::HandshakeTimeout)),
        "a silent server must hit the handshake timeout"
    );
}
