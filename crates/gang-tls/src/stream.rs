use std::fmt;
use std::io::{self, IoSlice};
use std::net::IpAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use gang_types::CertSerial;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::revocation::RevocationWatch;

/// A TLS stream that fails with `ConnectionAborted` as soon as its peer's certificate is revoked.
///
/// A stream without a watch has no peer certificate to revoke and never fails for revocation.
pub struct RevocableStream<S> {
    stream: S,
    watch: Option<RevocationWatch>,
    address: Option<IpAddr>,
}

/// Which half of the stream an operation polls. Each direction has its own revocation waker.
#[derive(Clone, Copy)]
enum Direction {
    Read,
    Write,
}

impl<S> RevocableStream<S> {
    /// Wraps `stream`. With a watch, every operation fails once the watch reports the peer's
    /// serial as revoked; without one, the stream is never revoked.
    #[must_use]
    pub fn new(stream: S, watch: Option<RevocationWatch>) -> Self {
        Self {
            stream,
            watch,
            address: None,
        }
    }

    /// Records the IP address of the connection's remote end. A stream whose peers have no IP
    /// address records `None`.
    #[must_use]
    pub fn with_address(mut self, address: Option<IpAddr>) -> Self {
        self.address = address;
        self
    }

    /// The IP address of the connection's remote end, or `None` when it is unknown.
    #[must_use]
    pub fn address(&self) -> Option<IpAddr> {
        self.address
    }

    /// The serial of the authenticated peer's certificate, or `None` when the stream has no
    /// watch, which happens when the peer presented no certificate.
    #[must_use]
    pub fn peer_serial(&self) -> Option<&CertSerial> {
        self.watch.as_ref().map(RevocationWatch::serial)
    }

    /// The wrapped stream.
    #[must_use]
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Polls `direction`'s revocation signal, registering the task waker so a pending operation
    /// wakes on revocation. Fails with `ConnectionAborted` once the peer's serial is revoked.
    ///
    /// The watch keeps reporting readiness once revoked, so later polls keep failing.
    fn guard(&mut self, cx: &mut Context<'_>, direction: Direction) -> io::Result<()> {
        let Some(watch) = self.watch.as_mut() else {
            return Ok(());
        };
        let revoked = match direction {
            Direction::Read => watch.poll_read_revoked(cx),
            Direction::Write => watch.poll_write_revoked(cx),
        };
        if revoked.is_ready() {
            Err(revoked_error())
        } else {
            Ok(())
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for RevocableStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.guard(cx, Direction::Read) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RevocableStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.guard(cx, Direction::Write) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.guard(cx, Direction::Write) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.guard(cx, Direction::Write) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    /// Forwarded even after revocation so the inner stream can still send its close notification.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// Failure reported by a [`RevocableStream`] whose peer certificate has been revoked.
#[derive(Debug)]
struct Revoked;

impl fmt::Display for Revoked {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("certificate revoked")
    }
}

impl std::error::Error for Revoked {}

fn revoked_error() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, Revoked)
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, IoSlice};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use gang_types::CertSerial;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
    use tokio::sync::oneshot;

    use super::RevocableStream;
    use crate::revocation::RevocationSet;
    use gang_testkit::park_then_signal;

    fn serial() -> CertSerial {
        CertSerial::try_new("aa11bb22cc33dd44ee55ff66778899").expect("valid serial")
    }

    /// Counts explicit task wakeups without relying on executor scheduling.
    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn pair(revocations: &RevocationSet) -> (RevocableStream<DuplexStream>, DuplexStream) {
        let (client, server) = tokio::io::duplex(1024);
        let stream = RevocableStream::new(client, Some(revocations.watch(&serial())));
        (stream, server)
    }

    fn assert_send_unpin<T: Send + Unpin>() {}

    #[test]
    fn stream_is_send_and_unpin() {
        assert_send_unpin::<RevocableStream<DuplexStream>>();
    }

    #[tokio::test]
    async fn reads_and_writes_pass_through_before_revocation() {
        let revocations = RevocationSet::default();
        let (mut stream, mut server) = pair(&revocations);

        stream
            .write_all(b"ping")
            .await
            .expect("write before revocation");
        stream.flush().await.expect("flush before revocation");
        let mut received = [0u8; 4];
        server
            .read_exact(&mut received)
            .await
            .expect("server reads");
        assert_eq!(&received, b"ping");

        server.write_all(b"pong").await.expect("server writes");
        stream
            .read_exact(&mut received)
            .await
            .expect("read before revocation");
        assert_eq!(&received, b"pong");
    }

    #[tokio::test(start_paused = true)]
    async fn pending_read_wakes_with_connection_aborted_on_revoke() {
        let revocations = RevocationSet::default();
        let (mut stream, _server) = pair(&revocations);

        let (armed_tx, armed_rx) = oneshot::channel();
        let reader = tokio::spawn(async move {
            let mut byte = [0u8; 1];
            park_then_signal(stream.read(&mut byte), armed_tx).await
        });
        armed_rx.await.expect("reader parks before revocation");
        revocations.revoke(serial());

        let error = reader
            .await
            .expect("reader task completes")
            .expect_err("revoked read fails");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        assert_eq!(error.to_string(), "certificate revoked");
    }

    #[test]
    fn revoke_wakes_pending_read_and_write() {
        let revocations = RevocationSet::default();
        let (client, _server) = tokio::io::duplex(1);
        let mut stream = RevocableStream::new(client, Some(revocations.watch(&serial())));
        let read_count = Arc::new(WakeCount::default());
        let write_count = Arc::new(WakeCount::default());
        let read_waker = Waker::from(Arc::clone(&read_count));
        let write_waker = Waker::from(Arc::clone(&write_count));
        let mut read_context = Context::from_waker(&read_waker);
        let mut write_context = Context::from_waker(&write_waker);
        let mut byte = [0_u8; 1];
        let mut read_buf = ReadBuf::new(&mut byte);

        assert!(
            Pin::new(&mut stream)
                .poll_read(&mut read_context, &mut read_buf)
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut write_context, b"x"),
            Poll::Ready(Ok(1))
        ));
        assert!(
            Pin::new(&mut stream)
                .poll_write(&mut write_context, b"x")
                .is_pending()
        );

        revocations.revoke(serial());

        assert_eq!(read_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(write_count.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            Pin::new(&mut stream).poll_read(&mut read_context, &mut read_buf),
            Poll::Ready(Err(error)) if error.kind() == ErrorKind::ConnectionAborted
        ));
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut write_context, b"x"),
            Poll::Ready(Err(error)) if error.kind() == ErrorKind::ConnectionAborted
        ));
    }

    #[test]
    fn dropping_stream_releases_both_waker_slots() {
        let revocations = RevocationSet::default();
        let observer = revocations.watch(&serial());
        let (mut stream, _server) = pair(&revocations);
        let mut context = Context::from_waker(Waker::noop());
        let mut byte = [0_u8; 1];
        let mut read_buf = ReadBuf::new(&mut byte);

        assert!(
            Pin::new(&mut stream)
                .poll_read(&mut context, &mut read_buf)
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, b"x"),
            Poll::Ready(Ok(1))
        ));
        assert_eq!(observer.waker_count(), 2);

        drop(stream);

        assert_eq!(observer.waker_count(), 0);
    }

    #[test]
    fn stream_watched_after_revocation_fails_on_first_read_and_write() {
        let revocations = RevocationSet::from_serials([serial()]);
        let (client, _server) = tokio::io::duplex(1);
        let mut stream = RevocableStream::new(client, Some(revocations.watch(&serial())));
        let mut context = Context::from_waker(Waker::noop());
        let mut byte = [0_u8; 1];
        let mut read_buf = ReadBuf::new(&mut byte);

        assert!(matches!(
            Pin::new(&mut stream).poll_read(&mut context, &mut read_buf),
            Poll::Ready(Err(error)) if error.kind() == ErrorKind::ConnectionAborted
        ));
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, b"x"),
            Poll::Ready(Err(error)) if error.kind() == ErrorKind::ConnectionAborted
        ));
    }

    #[tokio::test]
    async fn operations_fail_after_revocation() {
        let revocations = RevocationSet::default();
        let (mut stream, _server) = pair(&revocations);
        revocations.revoke(serial());

        let write = stream
            .write(b"late")
            .await
            .expect_err("write after revocation fails");
        assert_eq!(write.kind(), std::io::ErrorKind::ConnectionAborted);
        let flush = stream
            .flush()
            .await
            .expect_err("flush after revocation fails");
        assert_eq!(flush.kind(), std::io::ErrorKind::ConnectionAborted);
        let mut byte = [0u8; 1];
        let read = stream
            .read(&mut byte)
            .await
            .expect_err("read after revocation fails");
        assert_eq!(read.kind(), std::io::ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn shutdown_reaches_inner_stream_after_revocation() {
        let revocations = RevocationSet::default();
        let (mut stream, mut server) = pair(&revocations);
        revocations.revoke(serial());

        stream
            .shutdown()
            .await
            .expect("shutdown is forwarded after revocation");
        let mut rest = Vec::new();
        let end = server
            .read_to_end(&mut rest)
            .await
            .expect("server observes close");
        assert_eq!(end, 0);
    }

    #[test]
    fn peer_serial_is_the_watched_serial() {
        let revocations = RevocationSet::default();
        let stream = RevocableStream::new(b"abc".as_slice(), Some(revocations.watch(&serial())));

        assert_eq!(stream.peer_serial(), Some(&serial()));
    }

    #[test]
    fn stream_without_a_watch_has_no_peer_serial() {
        let inner: &[u8] = b"abc";
        let stream = RevocableStream::new(inner, None);

        assert_eq!(stream.peer_serial(), None);
    }

    #[test]
    fn revoked_watch_is_ready_on_first_poll_in_both_directions() {
        let revocations = RevocationSet::from_serials([serial()]);
        let mut watch = revocations.watch(&serial());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(watch.poll_read_revoked(&mut cx), Poll::Ready(())));
        assert!(matches!(watch.poll_write_revoked(&mut cx), Poll::Ready(())));
    }

    #[tokio::test(start_paused = true)]
    async fn pending_read_wakes_on_revoke_after_writer_registers_its_waker() {
        let revocations = RevocationSet::default();
        let (stream, _server) = pair(&revocations);
        let (mut reader, mut writer) = tokio::io::split(stream);

        let (armed_tx, armed_rx) = oneshot::channel();
        let read = tokio::spawn(async move {
            let mut byte = [0u8; 1];
            park_then_signal(reader.read(&mut byte), armed_tx).await
        });
        armed_rx.await.expect("reader parks before the write");

        let write = tokio::spawn(async move {
            writer
                .write_all(b"x")
                .await
                .expect("write before revocation");
            writer.flush().await.expect("flush before revocation");
        });
        write.await.expect("writer task completes");

        revocations.revoke(serial());

        let error = read
            .await
            .expect("reader task completes")
            .expect_err("pending read fails once revoked");
        assert_eq!(error.kind(), ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn vectored_writes_pass_through_until_revocation() {
        let revocations = RevocationSet::default();
        let (mut stream, mut server) = pair(&revocations);
        assert_eq!(
            AsyncWrite::is_write_vectored(&stream),
            AsyncWrite::is_write_vectored(stream.get_ref()),
        );

        let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
        let written = stream
            .write_vectored(&bufs)
            .await
            .expect("vectored write before revocation");
        assert!(written > 0);
        let mut received = vec![0u8; written];
        server
            .read_exact(&mut received)
            .await
            .expect("server reads the written bytes");
        assert_eq!(received.as_slice(), &b"abcd"[..written]);

        revocations.revoke(serial());

        let error = stream
            .write_vectored(&bufs)
            .await
            .expect_err("vectored write after revocation fails");
        assert_eq!(error.kind(), ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn stream_without_a_watch_is_never_revoked() {
        let revocations = RevocationSet::from_serials([serial()]);
        let (client, mut server) = tokio::io::duplex(1024);
        let mut stream = RevocableStream::new(client, None);

        stream
            .write_all(b"ok")
            .await
            .expect("write succeeds without identity");
        stream
            .flush()
            .await
            .expect("flush succeeds without identity");
        let mut received = [0u8; 2];
        server
            .read_exact(&mut received)
            .await
            .expect("server reads");
        assert_eq!(&received, b"ok");

        server.write_all(b"hi").await.expect("server writes");
        stream
            .read_exact(&mut received)
            .await
            .expect("read succeeds without identity");
        assert_eq!(&received, b"hi");
        assert!(
            revocations.is_revoked(&serial()),
            "the serial is revoked, yet the watch-less stream keeps working"
        );
    }
}
