//! `TlsAcceptor::incoming`: failed connections are reported and skipped, bounds are enforced, and
//! dropping the stream stops the listener.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;
use gang_tls::{Failure, IncomingSettings, Listener, TransportError};
use gang_types::NonZeroDuration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::support::{FakeListener, TransportFixture, echo_once};

/// Pause after a failed accept in the tests that do not exercise backoff.
const BACKOFF: NonZeroDuration = NonZeroDuration::from_millis(100);

/// How long refused connections stay open in these tests.
const LINGER: NonZeroDuration = NonZeroDuration::from_secs(2);

/// The first bytes an HTTP/2 client writes after its TLS handshake.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// What the failure callback saw, without the non-`Clone` error payload.
#[derive(Debug, PartialEq)]
enum Outcome {
    Accept,
    Timeout,
    Handshake(u32),
}

/// Accept bounds for a test: `max_pending` handshakes in flight and `accept_error_backoff` after a
/// failed accept.
fn settings(max_pending: usize, accept_error_backoff: NonZeroDuration) -> IncomingSettings {
    IncomingSettings {
        max_pending_handshakes: NonZeroUsize::new(max_pending)
            .expect("test handshake bound is non-zero"),
        accept_error_backoff,
        refusal_linger: LINGER,
    }
}

/// A failure callback that sends each reported failure to the returned receiver, so that tests
/// wait on the report itself rather than on elapsed time.
fn recorder() -> (
    mpsc::UnboundedReceiver<Outcome>,
    impl Fn(&Failure<u32>) + Send + Sync + 'static,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let callback = move |failure: &Failure<u32>| {
        let outcome = match failure {
            Failure::Accept(_) => Outcome::Accept,
            Failure::Handshake {
                error: TransportError::HandshakeTimeout,
                ..
            } => Outcome::Timeout,
            Failure::Handshake { peer, .. } => Outcome::Handshake(*peer),
        };
        tx.send(outcome).ok();
    };
    (rx, callback)
}

/// The next reported failure. Waits until the failure is reported.
async fn next_failure(failures: &mut mpsc::UnboundedReceiver<Outcome>) -> Outcome {
    failures
        .recv()
        .await
        .expect("the failure callback is alive until the incoming stream is dropped")
}

async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

async fn poll_once<S: Stream + Unpin>(stream: &mut S) -> Poll<Option<S::Item>> {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_next(cx))).await
}

/// A [`FakeListener`] whose peers are IPv4 addresses: each `u32` the feed queues is the peer's IP.
struct AddressedListener(FakeListener);

impl Listener for AddressedListener {
    type Io = DuplexStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> io::Result<(DuplexStream, SocketAddr)> {
        let (io, peer) = self.0.accept().await?;
        Ok((io, SocketAddr::new(IpAddr::V4(Ipv4Addr::from(peer)), 0)))
    }

    fn peer_ip(addr: &SocketAddr) -> Option<IpAddr> {
        Some(addr.ip())
    }
}

/// A listener that reports when its first accepted connection is read during a handshake.
struct ObservedListener {
    inner: FakeListener,
    reading: Option<oneshot::Sender<()>>,
}

/// A connection that signals the first handshake read.
struct ObservedIo {
    inner: DuplexStream,
    reading: Option<oneshot::Sender<()>>,
}

impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(reading) = self.reading.take() {
            let _ = reading.send(());
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Listener for ObservedListener {
    type Io = ObservedIo;
    type Addr = u32;

    async fn accept(&mut self) -> io::Result<(ObservedIo, u32)> {
        let (inner, peer) = self.inner.accept().await?;
        Ok((
            ObservedIo {
                inner,
                reading: self.reading.take(),
            },
            peer,
        ))
    }
}

#[tokio::test(start_paused = true)]
async fn valid_client_yields_revocable_stream() {
    let fixture = TransportFixture::new();
    let client = fixture.client(20, "alpha");
    let (listener, feed) = FakeListener::new();
    let mut incoming =
        fixture
            .acceptor()
            .incoming(listener, settings(256, BACKOFF), |_: &Failure<u32>| {});
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));

    let (client_result, server) = tokio::join!(
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    let mut client_io = client_result.expect("client handshake completes");
    let mut server = server.expect("stream yields the valid client");
    echo_once(&mut client_io, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn established_connection_reports_peer_ip() {
    let fixture = TransportFixture::new();
    let client = fixture.client(20, "alpha");
    let (listener, feed) = FakeListener::new();
    let mut incoming = fixture.acceptor().incoming(
        AddressedListener(listener),
        settings(256, BACKOFF),
        |_: &Failure<SocketAddr>| {},
    );
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 0x0A00_0001)));

    let (client_result, server) = tokio::join!(
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    client_result.expect("client handshake completes");
    let server = server.expect("stream yields the valid client");
    assert_eq!(
        server.address(),
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
    );
}

#[tokio::test(start_paused = true)]
async fn listener_without_peer_ip_reports_no_address() {
    let fixture = TransportFixture::new();
    let client = fixture.client(20, "alpha");
    let (listener, feed) = FakeListener::new();
    let mut incoming =
        fixture
            .acceptor()
            .incoming(listener, settings(256, BACKOFF), |_: &Failure<u32>| {});
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));

    let (client_result, server) = tokio::join!(
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    client_result.expect("client handshake completes");
    let server = server.expect("stream yields the valid client");
    assert_eq!(server.address(), None);
}

#[tokio::test(start_paused = true)]
async fn failed_handshakes_are_reported_and_skipped() {
    let fixture = TransportFixture::new();
    let good = fixture.client(21, "alpha");
    let foreign = fixture.foreign_client(22, "mallory");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, BACKOFF), callback);

    let (foreign_client_io, foreign_server_io) = TransportFixture::pipe();
    let (anon_client_io, anon_server_io) = TransportFixture::pipe();
    let (good_client_io, good_server_io) = TransportFixture::pipe();
    feed.push(Ok((foreign_server_io, 1)));
    feed.push(Ok((anon_server_io, 2)));
    feed.push(Ok((good_server_io, 3)));

    let anon_connector =
        tokio_rustls::TlsConnector::from(fixture.raw_client_config(None, false, false));
    let (_foreign_result, _anon_result, good_result, server) = tokio::join!(
        foreign
            .connector
            .connect(TransportFixture::server_name(), foreign_client_io),
        anon_connector.connect(TransportFixture::server_name(), anon_client_io),
        good.connector
            .connect(TransportFixture::server_name(), good_client_io),
        next(&mut incoming)
    );
    good_result.expect("good client handshake completes");
    assert!(
        server.is_some(),
        "the stream keeps yielding after failed handshakes"
    );

    let mut reported = vec![
        next_failure(&mut failures).await,
        next_failure(&mut failures).await,
    ];
    reported.sort_by_key(|outcome| match outcome {
        Outcome::Handshake(peer) => *peer,
        Outcome::Accept | Outcome::Timeout => 0,
    });
    assert_eq!(reported, [Outcome::Handshake(1), Outcome::Handshake(2)]);
}

#[tokio::test(start_paused = true)]
async fn a_refused_client_that_writes_before_it_reads_still_receives_the_revoked_alert() {
    let (fixture, revoked) = TransportFixture::with_revoked_client(40, "alpha");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let _incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, BACKOFF), callback);
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));
    let mut client = revoked
        .connector
        .connect(TransportFixture::server_name(), client_io)
        .await
        .expect(
            "a TLS 1.3 client completes its handshake before the server verifies its certificate",
        );
    assert_eq!(next_failure(&mut failures).await, Outcome::Handshake(1));
    client
        .write_all(H2_PREFACE)
        .await
        .expect("the refused connection still takes the client's bytes");
    client
        .flush()
        .await
        .expect("the refused connection still takes the client's bytes");
    let error = client
        .read(&mut [0_u8; 1])
        .await
        .expect_err("the client reads the server's alert");
    assert!(matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>()),
        Some(rustls::Error::AlertReceived(
            rustls::AlertDescription::CertificateRevoked
        ))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_refused_connection_frees_its_handshake_slot_when_the_client_closes() {
    let (fixture, revoked) = TransportFixture::with_revoked_client(41, "alpha");
    let good = fixture.client(42, "beta");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(1, BACKOFF), callback);
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));
    let mut client = revoked
        .connector
        .connect(TransportFixture::server_name(), client_io)
        .await
        .expect(
            "a TLS 1.3 client completes its handshake before the server verifies its certificate",
        );
    assert_eq!(next_failure(&mut failures).await, Outcome::Handshake(1));
    client
        .read(&mut [0_u8; 1])
        .await
        .expect_err("the client reads the server's alert");
    drop(client);

    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 2)));
    let (client_result, server) = tokio::join!(
        good.connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    client_result.expect("good client handshake completes");
    assert!(server.is_some(), "the stream yields the good client");
}

#[tokio::test(start_paused = true)]
async fn a_refused_connection_holds_its_slot_until_the_linger_ends_even_while_its_client_keeps_sending()
 {
    let (fixture, revoked) = TransportFixture::with_revoked_client(43, "alpha");
    let good = fixture.client(44, "beta");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(1, BACKOFF), callback);
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));
    let mut client = revoked
        .connector
        .connect(TransportFixture::server_name(), client_io)
        .await
        .expect(
            "a TLS 1.3 client completes its handshake before the server verifies its certificate",
        );
    assert_eq!(next_failure(&mut failures).await, Outcome::Handshake(1));
    let (started, writing) = oneshot::channel();
    let writer = tokio::spawn(async move {
        client
            .write_all(&[0_u8; 1024])
            .await
            .expect("the refused connection accepts client bytes");
        let _ = started.send(());
        loop {
            if client.write_all(&[0_u8; 1024]).await.is_err() {
                break;
            }
        }
    });
    writing.await.expect("the writer starts sending");

    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 2)));
    let mut second = pin!(
        good.connector
            .connect(TransportFixture::server_name(), client_io)
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        second.as_mut().poll(&mut cx).is_pending(),
        "the refused connection holds the only handshake slot during the linger"
    );
    fixture.clock().advance(LINGER.get());
    let (client_result, server) = tokio::join!(second, next(&mut incoming));
    client_result.expect("good client handshake completes");
    assert!(server.is_some(), "the stream yields the good client");
    writer.await.expect("the writer task completes");
}

#[tokio::test(start_paused = true)]
async fn a_refused_connection_discards_more_than_any_buffer_holds() {
    let (fixture, revoked) = TransportFixture::with_revoked_client(45, "alpha");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let _incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, BACKOFF), callback);
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 1)));
    let mut client = revoked
        .connector
        .connect(TransportFixture::server_name(), client_io)
        .await
        .expect(
            "a TLS 1.3 client completes its handshake before the server verifies its certificate",
        );
    assert_eq!(next_failure(&mut failures).await, Outcome::Handshake(1));

    // One MiB exceeds the test pipe's 64 KiB capacity.
    for _ in 0..1024 {
        client
            .write_all(&[0_u8; 1024])
            .await
            .expect("the refused connection continues to drain client bytes");
    }
    client
        .flush()
        .await
        .expect("the refused connection continues to drain client bytes");
    let error = client
        .read(&mut [0_u8; 1])
        .await
        .expect_err("the client reads the server's alert");
    assert!(matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>()),
        Some(rustls::Error::AlertReceived(
            rustls::AlertDescription::CertificateRevoked
        ))
    ));
}

#[tokio::test]
async fn accept_error_is_reported_and_accepting_continues_after_backoff() {
    let fixture = TransportFixture::new();
    let good = fixture.client(23, "alpha");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let backoff = NonZeroDuration::from_secs(2);
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, backoff), callback);

    feed.push(Err(std::io::Error::other("accept failed")));
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 4)));

    assert_eq!(next_failure(&mut failures).await, Outcome::Accept);
    // The accept loop is now sleeping out its backoff. Nothing is delivered before the
    // backoff elapses, so a single poll is still pending.
    assert!(poll_once(&mut incoming).await.is_pending());

    fixture.clock().advance(backoff.get());
    let (client_result, server) = tokio::join!(
        good.connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    client_result.expect("client handshake completes");
    assert!(
        server.is_some(),
        "accepting continues after an accept error"
    );
}
/// An arbitrary errno; the test only checks that its value arrives unchanged.
const INJECTED_ERRNO: i32 = 24;

/// A distinguishable error payload for testing accept error reporting.
#[derive(Debug)]
struct InjectedAcceptError;

impl std::fmt::Display for InjectedAcceptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("injected accept error")
    }
}

impl std::error::Error for InjectedAcceptError {}

#[tokio::test(start_paused = true)]
async fn accept_error_preserves_raw_os_error() {
    let fixture = TransportFixture::new();
    let (listener, feed) = FakeListener::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _incoming = fixture.acceptor().incoming(
        listener,
        settings(256, BACKOFF),
        move |failure: &Failure<u32>| {
            if let Failure::Accept(error) = failure {
                tx.send(error.raw_os_error()).ok();
            }
        },
    );

    feed.push(Err(io::Error::from_raw_os_error(INJECTED_ERRNO)));
    assert_eq!(rx.recv().await, Some(Some(INJECTED_ERRNO)));
}

#[tokio::test(start_paused = true)]
async fn accept_error_preserves_inner_error_type() {
    let fixture = TransportFixture::new();
    let (listener, feed) = FakeListener::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _incoming =
        fixture.acceptor().incoming(
            listener,
            settings(256, BACKOFF),
            move |failure: &Failure<u32>| {
                if let Failure::Accept(error) = failure {
                    tx.send(error.get_ref().is_some_and(|inner| {
                        inner.downcast_ref::<InjectedAcceptError>().is_some()
                    }))
                    .ok();
                }
            },
        );

    feed.push(Err(io::Error::other(InjectedAcceptError)));
    assert_eq!(rx.recv().await, Some(true));
}

#[tokio::test(start_paused = true)]
async fn pending_handshake_limit_blocks_until_stalled_peer_is_dropped() {
    let fixture = TransportFixture::with_handshake_timeout(NonZeroDuration::from_hours(1));
    let good = fixture.client(24, "alpha");
    let (listener, feed) = FakeListener::new();
    let (mut failures, callback) = recorder();
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(1, BACKOFF), callback);

    let (stalled_client, stalled_server) = TransportFixture::pipe();
    feed.push(Ok((stalled_server, 5)));
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 6)));

    // The stalled handshake holds the only permit, so the second handshake cannot complete and
    // its single poll is still pending.
    let mut second = pin!(
        good.connector
            .connect(TransportFixture::server_name(), client_io)
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        second.as_mut().poll(&mut cx).is_pending(),
        "second client must wait while the stalled handshake holds the only permit"
    );

    drop(stalled_client);
    let (client_result, server) = tokio::join!(second, next(&mut incoming));
    client_result.expect("client handshake completes");
    assert!(
        server.is_some(),
        "second client arrives once the stalled peer is gone"
    );
    assert_eq!(next_failure(&mut failures).await, Outcome::Handshake(5));
}

#[tokio::test]
async fn stalled_client_is_cut_by_handshake_timeout() {
    let fixture = TransportFixture::with_handshake_timeout(NonZeroDuration::from_secs(3));
    let (listener, feed) = FakeListener::new();
    let (reading, started) = oneshot::channel();
    let listener = ObservedListener {
        inner: listener,
        reading: Some(reading),
    };
    let (mut failures, callback) = recorder();
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, BACKOFF), callback);

    let (_stalled_client, stalled_server) = TransportFixture::pipe();
    feed.push(Ok((stalled_server, 7)));
    started.await.expect("the handshake starts reading");
    fixture.clock().advance(NonZeroDuration::from_secs(3).get());

    assert_eq!(next_failure(&mut failures).await, Outcome::Timeout);
    assert!(poll_once(&mut incoming).await.is_pending());
}

#[tokio::test(start_paused = true)]
async fn dropping_incoming_closes_the_listener() {
    let fixture = TransportFixture::new();
    let (listener, mut feed) = FakeListener::new();
    let incoming =
        fixture
            .acceptor()
            .incoming(listener, settings(256, BACKOFF), |_: &Failure<u32>| {});

    drop(incoming);
    feed.listener_dropped().await;
}

#[tokio::test(start_paused = true)]
async fn revoked_serial_aborts_connection_accepted_by_incoming() {
    let fixture = TransportFixture::new();
    let client = fixture.client(25, "alpha");
    let (listener, feed) = FakeListener::new();
    let mut incoming =
        fixture
            .acceptor()
            .incoming(listener, settings(256, BACKOFF), |_: &Failure<u32>| {});
    let (client_io, server_io) = TransportFixture::pipe();
    feed.push(Ok((server_io, 8)));

    let (client_result, server) = tokio::join!(
        client
            .connector
            .connect(TransportFixture::server_name(), client_io),
        next(&mut incoming)
    );
    let mut client_io = client_result.expect("client handshake completes");
    let mut server = server.expect("stream yields the valid client");
    echo_once(&mut client_io, &mut server).await;

    fixture.revocations().revoke(client.serial.clone());

    let error = server
        .read(&mut [0_u8; 1])
        .await
        .expect_err("revoked connection must not read");
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
}

#[tokio::test]
async fn real_tcp_listener_yields_working_connection() {
    let fixture = TransportFixture::new();
    let client = fixture.client(26, "alpha");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback bind");
    let addr: SocketAddr = listener.local_addr().expect("local address");
    let mut incoming = fixture
        .acceptor()
        .incoming(listener, settings(256, BACKOFF), |_| {});

    let tcp = TcpStream::connect(addr).await.expect("loopback connect");
    let (client_result, server) = tokio::join!(
        client
            .connector
            .connect(TransportFixture::server_name(), tcp),
        next(&mut incoming)
    );
    let mut client_io = client_result.expect("client handshake completes");
    let mut server = server.expect("stream yields the tcp client");
    assert_eq!(server.address(), Some(addr.ip()));
    echo_once(&mut client_io, &mut server).await;
}
