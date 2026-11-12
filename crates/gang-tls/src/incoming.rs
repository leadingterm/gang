use std::fmt::Debug;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use gang_clock::Clock;
use gang_types::NonZeroDuration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

use crate::acceptor::TlsAcceptor;
use crate::errors::TransportError;
use crate::stream::RevocableStream;

/// A source of raw connections, such as a bound TCP listener.
pub trait Listener: Send + 'static {
    /// The transport of an accepted connection.
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// The peer address reported with each connection.
    type Addr: Debug + Send + 'static;

    /// Waits for the next raw connection.
    fn accept(&mut self) -> impl Future<Output = io::Result<(Self::Io, Self::Addr)>> + Send;

    /// The IP address that `addr` names, when it names one. The default is `None`: a listener
    /// whose peers have no IP address (an in-memory transport) reports none.
    fn peer_ip(_addr: &Self::Addr) -> Option<IpAddr> {
        None
    }
}

impl Listener for TcpListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
        TcpListener::accept(self).await
    }

    fn peer_ip(addr: &SocketAddr) -> Option<IpAddr> {
        Some(addr.ip())
    }
}

/// Bounds and pacing of an [`Incoming`] stream.
#[derive(Debug, Clone, Copy)]
pub struct IncomingSettings {
    /// Handshakes in flight at once; further connections wait in the OS backlog.
    pub max_pending_handshakes: NonZeroUsize,
    /// Pause after a failed `accept` before the next attempt.
    pub accept_error_backoff: NonZeroDuration,
}

/// Why a connection did not reach the consumer of an [`Incoming`] stream.
#[derive(Debug)]
pub enum Failure<A> {
    /// The listener failed to accept a connection.
    Accept(io::Error),
    /// A connection was accepted but its TLS handshake failed.
    Handshake {
        /// The peer address of the failed connection.
        peer: A,
        /// The handshake error.
        error: TransportError,
    },
}

/// A stream of established TLS connections from a [`Listener`].
///
/// Failed connections are reported to the callback given to [`TlsAcceptor::incoming`] and
/// skipped, so the stream only ends when it is dropped, which aborts the accept loop, every pending
/// handshake and the listener.
pub struct Incoming<S> {
    deliveries: ReceiverStream<S>,
    _accept_loop: AbortOnDropHandle<()>,
}

impl<S> Stream for Incoming<S> {
    type Item = S;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S>> {
        Pin::new(&mut self.deliveries).poll_next(cx)
    }
}

impl TlsAcceptor {
    /// Accepts connections from `listener` and completes their TLS handshakes in the background.
    ///
    /// At most `settings.max_pending_handshakes` connections are in handshake or waiting to be
    /// buffered for the consumer at once; a permit is held until the connection is buffered, so
    /// further connections wait in the listener's backlog. `on_failure` observes every failed
    /// accept and handshake.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, as [`tokio::spawn`] does.
    pub fn incoming<L: Listener>(
        &self,
        listener: L,
        settings: IncomingSettings,
        on_failure: impl Fn(&Failure<L::Addr>) + Send + Sync + 'static,
    ) -> Incoming<RevocableStream<tokio_rustls::server::TlsStream<L::Io>>> {
        let (deliveries, received) = mpsc::channel(settings.max_pending_handshakes.get());
        let accept_loop = tokio::spawn(accept_loop(
            self.clone(),
            listener,
            settings,
            Arc::new(on_failure),
            deliveries,
        ));
        Incoming {
            deliveries: ReceiverStream::new(received),
            _accept_loop: AbortOnDropHandle::new(accept_loop),
        }
    }
}

/// Accepts the next connection from `listener`, retrying after each failure.
///
/// This is the one owner of the accept-retry policy: every failed `accept` is passed unchanged
/// and by ownership to `on_error`, then the loop pauses for `backoff` before trying again, so a
/// listener that keeps failing (for example one out of file descriptors) is polled at a bounded
/// rate.
pub async fn accept_retrying<L: Listener>(
    listener: &mut L,
    backoff: NonZeroDuration,
    clock: &impl Clock,
    mut on_error: impl FnMut(io::Error),
) -> (L::Io, L::Addr) {
    loop {
        match listener.accept().await {
            Ok(connection) => return connection,
            Err(error) => {
                on_error(error);
                clock.sleep(backoff.get()).await;
            }
        }
    }
}

async fn accept_loop<L, F>(
    acceptor: TlsAcceptor,
    mut listener: L,
    settings: IncomingSettings,
    on_failure: Arc<F>,
    deliveries: mpsc::Sender<RevocableStream<tokio_rustls::server::TlsStream<L::Io>>>,
) where
    L: Listener,
    F: Fn(&Failure<L::Addr>) + Send + Sync + 'static,
{
    let permits = Arc::new(Semaphore::new(settings.max_pending_handshakes.get()));
    let mut handshakes: JoinSet<()> = JoinSet::new();
    loop {
        // Completed handshake tasks hold only their join slot; reap them so the set stays small.
        while handshakes.try_join_next().is_some() {}

        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (io, peer) = accept_retrying(
            &mut listener,
            settings.accept_error_backoff,
            &acceptor.clock,
            |error| on_failure(&Failure::Accept(error)),
        )
        .await;
        let address = L::peer_ip(&peer);

        let acceptor = acceptor.clone();
        let deliveries = deliveries.clone();
        let on_failure = Arc::clone(&on_failure);
        handshakes.spawn(async move {
            // Held until the connection is buffered (or dropped), so it counts against the bound.
            let _permit = permit;
            match acceptor.handshake(io).await {
                Ok(stream) => {
                    // The consumer is gone when the send fails; the stream is then dropped, which
                    // closes the connection.
                    deliveries.send(stream.with_address(address)).await.ok();
                }
                Err(error) => on_failure(&Failure::Handshake { peer, error }),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::pin::pin;
    use std::task::{Context, Waker};

    use gang_clock::TestClock;
    use tokio::io::DuplexStream;

    use super::*;

    const BACKOFF: NonZeroDuration = NonZeroDuration::from_millis(100);

    /// A listener that replays a fixed script of accept results.
    struct ScriptedListener(VecDeque<io::Result<(DuplexStream, u8)>>);

    impl Listener for ScriptedListener {
        type Io = DuplexStream;
        type Addr = u8;

        async fn accept(&mut self) -> io::Result<(DuplexStream, u8)> {
            self.0
                .pop_front()
                .expect("scripted listener has no result left")
        }
    }

    #[tokio::test]
    async fn accept_retrying_backs_off_after_each_failure() {
        let (client, _server) = tokio::io::duplex(64);
        let mut listener = ScriptedListener(VecDeque::from([
            Err(io::Error::other("first failure")),
            Err(io::Error::other("second failure")),
            Ok((client, 7)),
        ]));
        let failures = Cell::new(0);
        let clock = TestClock::default();
        let mut retry = pin!(accept_retrying(&mut listener, BACKOFF, &clock, |_| {
            failures.set(failures.get() + 1);
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(retry.as_mut().poll(&mut cx).is_pending());
        assert_eq!(failures.get(), 1);
        clock.advance(BACKOFF.get());
        assert!(retry.as_mut().poll(&mut cx).is_pending());
        assert_eq!(failures.get(), 2);
        clock.advance(BACKOFF.get());
        let (_io, peer) = retry.await;
        assert_eq!(peer, 7);
        assert_eq!(failures.get(), 2);
    }
}
