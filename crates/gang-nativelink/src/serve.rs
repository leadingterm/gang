//! The serve loop: runs one axum router over the connections the caller hands in.
//!
//! The caller accepts and authenticates every connection. Each one is served in a task
//! owned by the serve future, so dropping that future closes every connection. Once all listener
//! streams have ended, client connections shut down gracefully; worker connections do so once
//! all workers are released, or are dropped when `shutdown_grace` expires.
//! HTTP/1.1 connections are closed if a request head stalls past `header_read_timeout`;
//! HTTP/2 connections are kept alive by `keepalive_interval` pings.
//! Each HTTP/2 connection, and each of its streams, may receive `receive_window` bytes before the
//! server reads them, so a client far away uploads a whole window per round trip. A listener serves
//! a bounded number of connections at once; connections earn their place by an authenticated
//! request. An HTTP/2 connection runs at most `max_concurrent_streams` requests at once; with the
//! window, these bound the request data held (see [`Http2Tuning`]).

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use axum::Extension;
use axum::Router;
use axum::middleware::from_fn_with_state;
use futures::StreamExt;
use gang_clock::Clock;
use gang_spi::{Listening, RequestGate};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::config::Http2Tuning;
use crate::errors::NativeLinkEmbedError;
use crate::gate::admit;
use crate::handoff::HandOff;
use crate::refusals::Refusals;
use crate::standing::Standing;

/// The listener a serve loop runs, which decides the HTTP versions its connections speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listener {
    /// The public listener: detects HTTP/1.1 or HTTP/2 on each connection, including HTTP/2 with
    /// prior knowledge.
    Public,
    /// The execution listener has the same client transport and limits as public.
    Execution,
    /// The worker listener: serves HTTP/2 only. A connection that does not speak HTTP/2 is
    /// closed.
    Worker,
}

impl Listener {
    /// The listener's name in logs.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Execution => "execution",
            Self::Worker => "worker",
        }
    }
}

/// Coordinates stream endings and the shared controller hand-off.
#[derive(Clone)]
pub(crate) struct ServeShutdown {
    pub(crate) ended: tokio::sync::mpsc::UnboundedSender<()>,
    pub(crate) handoff: HandOff,
}

/// Serves `router` on the connections of `listening` until the stream ends and the served
/// connections have closed.
///
/// The listener's gate runs over every request before any route or fallback of `router` sees it.
/// Each connection's peer is put in the request extensions, so the gate can read it.
///
/// Each connection speaks the HTTP versions of `listener`.
///
/// After hand-off begins, client connections shut down gracefully; worker connections do so
/// after all workers are released. Connections still open after `tuning.shutdown_grace` are dropped.
///
/// HTTP/1.1 request heads must arrive within `tuning.header_read_timeout`, or the connection is
/// closed. HTTP/2 keeps connections alive with pings every `tuning.keepalive_interval`.
/// HTTP/2 clients may send `tuning.receive_window` bytes before the server reads them, and run at
/// most `tuning.max_concurrent_streams` requests at once on one connection.
///
/// At most `max_connections` connections are served at once. A connection earns its place once
/// the gate admits an authenticated request on it. At the limit, a new connection replaces the
/// oldest connection that has not earned its place, an idle one first, and is closed at once only
/// when every connection has; the stream is always read, so its end is seen however many
/// connections are open. Refusals are logged at most once per `tuning.refusal_log_interval` of
/// `clock`. On the public and execution listeners, a connection that has not earned its place is also closed once
/// it has had no request in flight for `tuning.unproven_idle_timeout`, before its first request
/// included; the worker listener's connections are bounded by the fleets, and a worker's store
/// connections may stay idle until it runs an action.
///
/// A failing connection is logged and never stops the loop.
pub(crate) async fn serve<G: RequestGate, C: Clock + Clone + Send + Sync + 'static>(
    listening: Listening<G>,
    router: Router,
    tuning: &Http2Tuning,
    listener: Listener,
    max_connections: NonZeroUsize,
    clock: C,
    shutdown: ServeShutdown,
) -> Result<(), NativeLinkEmbedError> {
    let Listening {
        mut connections,
        gate,
    } = listening;
    let gated = router.layer(from_fn_with_state(gate, admit::<G>));
    let mut http = match listener {
        Listener::Worker => auto::Builder::new(TokioExecutor::new()).http2_only(),
        Listener::Public | Listener::Execution => {
            let mut http = auto::Builder::new(TokioExecutor::new());
            http.http1()
                .timer(TokioTimer::new())
                .header_read_timeout(tuning.header_read_timeout.get());
            http
        }
    };
    let window = tuning.window_for(listener);
    http.http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(tuning.keepalive_interval.get())
        .max_concurrent_streams(tuning.max_concurrent_streams.get())
        .initial_connection_window_size(window)
        .initial_stream_window_size(window);
    let graceful = GracefulShutdown::new();
    let mut served = JoinSet::new();
    let mut refusals = Refusals::new(listener, max_connections, tuning.refusal_log_interval);
    let mut open: VecDeque<Open> = VecDeque::new();
    let idle = (listener != Listener::Worker).then_some(tuning.unproven_idle_timeout.get());
    loop {
        tokio::select! {
            next = connections.next() => if let Some(accepted) = next {
                    // A connection that has ended frees its place before the limit is checked.
                    while let Some(finished) = served.try_join_next_with_id() {
                        reap(finished, &mut open);
                    }
                    if !make_room(&mut open, max_connections) {
                        // Every place is earned: dropping the connection closes it.
                        if let Some(line) = refusals.refuse(clock.monotonic()) {
                            tracing::warn!("{line}");
                        }
                        continue;
                    }
                    // The peer and the standing are outermost, so the gate sees them on every request.
                    let standing = Arc::new(Standing::default());
                    let service = gated
                        .clone()
                        .layer(Extension(accepted.peer))
                        .layer(Extension(Arc::clone(&standing)));
                    let watcher = graceful.watcher();
                    let http = http.clone();
                    let clock = clock.clone();
                    let expiring = Arc::clone(&standing);
                    let abort = served.spawn(async move {
                        let connection = watcher.watch(http.serve_connection(
                            TokioIo::new(accepted.connection),
                            TowerToHyperService::new(service),
                        ));
                        let expired = async {
                            match idle {
                                Some(idle) => expiring.expired(&clock, idle).await,
                                None => std::future::pending().await,
                            }
                        };
                        tokio::select! {
                            served = connection => if let Err(error) = served {
                                tracing::warn!(error = %gang_types::error_chain(error.as_ref()), "connection ended with an error");
                            },
                            () = expired => {}
                        }
                    });
                    open.push_back(Open {
                        id: abort.id(),
                        standing,
                        abort,
                    });
            } else {
                break;
            },
            Some(finished) = served.join_next_with_id(), if !served.is_empty() => reap(finished, &mut open),
        }
    }
    // Close the listening sockets before waiting for existing connections to drain.
    drop(connections);
    let _ = shutdown.ended.send(());
    drain_connections(
        &clock,
        tuning,
        listener,
        &shutdown.handoff,
        graceful,
        &mut served,
        &mut open,
    )
    .await;
    Ok(())
}

/// Waits for the common hand-off, then drains connections within the grace period.
async fn drain_connections<C: Clock>(
    clock: &C,
    tuning: &Http2Tuning,
    listener: Listener,
    handoff: &HandOff,
    graceful: GracefulShutdown,
    served: &mut JoinSet<()>,
    open: &mut VecDeque<Open>,
) {
    loop {
        tokio::select! {
            () = handoff.begun() => break,
            Some(finished) = served.join_next_with_id(), if !served.is_empty() => reap(finished, open),
        }
    }
    let drained = clock
        .timeout(tuning.shutdown_grace, async {
            if listener == Listener::Worker {
                loop {
                    if served.is_empty() {
                        return;
                    }
                    tokio::select! {
                        () = handoff.workers_released() => break,
                        Some(finished) = served.join_next_with_id() => reap(finished, open),
                    }
                }
            }
            graceful.shutdown().await;
            while let Some(finished) = served.join_next_with_id().await {
                reap(finished, open);
            }
        })
        .await;
    if drained.is_err() {
        tracing::warn!(
            open = served.len(),
            "connections did not close within the shutdown grace"
        );
    }
}

/// A served connection, in the order connections were handed in.
struct Open {
    /// The id of the task serving it.
    id: tokio::task::Id,
    /// Whether it has earned its place.
    standing: Arc<Standing>,
    /// Closes it.
    abort: AbortHandle,
}

/// Makes room for a new connection when `open` holds `max_connections`: closes the oldest
/// connection that has not earned its place and has no request in flight, else the oldest that
/// has not earned its place. Returns whether there is room; there is none when every place is
/// earned.
fn make_room(open: &mut VecDeque<Open>, max_connections: NonZeroUsize) -> bool {
    if open.len() < max_connections.get() {
        return true;
    }
    let Some(oldest) = open
        .iter()
        .position(|open| !open.standing.is_proven() && !open.standing.is_busy())
        .or_else(|| open.iter().position(|open| !open.standing.is_proven()))
    else {
        return false;
    };
    if let Some(replaced) = open.remove(oldest) {
        replaced.abort.abort();
    }
    true
}

/// Forgets a finished connection, and logs one whose task panicked; a connection closed to make
/// room for another was cancelled on purpose.
fn reap(finished: Result<(tokio::task::Id, ()), JoinError>, open: &mut VecDeque<Open>) {
    let id = match finished {
        Ok((id, ())) => id,
        Err(error) => {
            if error.is_panic() {
                tracing::warn!(error = %gang_types::error_chain(&error), "connection task did not finish");
            }
            error.id()
        }
    };
    open.retain(|open| open.id != id);
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, Bytes};
    use axum::routing::{get, post};
    use futures::stream;
    use gang_clock::{SystemClock, TestClock, Timestamp};
    use gang_spi::{Accepted, Admitted, Authenticated, Peer, PortError};
    use gang_testkit::TestResult;
    use gang_types::health::CLIENT_KEEPALIVE_TIME;
    use gang_types::{CertSerial, Identity, NonZeroDuration, Principal, ProviderName};
    use http_body_util::BodyExt;
    use hyper::client::conn::http1;
    use hyper::client::conn::http2::{SendRequest, handshake};
    use hyper::{Request, StatusCode};
    use parking_lot::Mutex;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::config::ReceiveWindow;

    /// Buffer of each in-memory connection.
    const PIPE_BUFFER: usize = 64 * 1024;
    /// Largest body a test reads from the router.
    const BODY_LIMIT: usize = 1024;
    /// Round trip of the simulated link.
    const ROUND_TRIP: Duration = Duration::from_millis(100);
    /// Size of each body chunk the client sends.
    const CHUNK: usize = 1024 * 1024;
    /// Content of every upload chunk.
    static ZEROS: [u8; CHUNK] = [0; CHUNK];
    /// One mebibyte.
    const MIB: usize = 1024 * 1024;

    #[tokio::test]
    async fn stopped_listener_closes_before_connections_drain() -> TestResult {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let connections = stream::unfold(listener, |listener| async move {
            let (connection, _) = listener.accept().await.ok()?;
            Some((
                Accepted {
                    connection: Box::new(connection),
                    peer: Peer::default(),
                },
                listener,
            ))
        })
        .take_until(async {
            let _ = stopped.await;
        })
        .boxed();
        let (entered, mut entered_rx) = mpsc::unbounded_channel();
        let router = Router::new().route(
            "/",
            get(move || {
                let entered = entered.clone();
                async move {
                    let _ = entered.send(());
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let handoff = HandOff::new();
        let (ended, mut endings) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let tuning = Http2Tuning::default();
            serve(
                Listening {
                    connections,
                    gate: Allow,
                },
                router,
                &tuning,
                Listener::Public,
                tuning.max_connections,
                SystemClock::new(),
                ServeShutdown { ended, handoff },
            )
            .await
        });
        let mut client = tokio::net::TcpStream::connect(address).await?;
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        entered_rx.recv().await.ok_or("handler did not start")?;
        stop.send(()).map_err(|()| "listener stopped early")?;
        endings.recv().await.ok_or("serve stopped early")?;
        assert!(!task.is_finished(), "connection should still be draining");
        let error = tokio::net::TcpStream::connect(address)
            .await
            .expect_err("listener still bound");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
        task.abort();
        Ok(())
    }

    fn router() -> Router {
        Router::new().route("/", get(|| async { "ok" }))
    }

    /// Answers `GET /`, and drains an upload to `/upload` and returns the number of bytes received.
    fn upload_router() -> Router {
        router().route(
            "/upload",
            post(|body: Body| async move {
                let mut body = body.into_data_stream();
                let mut total = 0;
                while let Some(chunk) = body.next().await {
                    total += chunk.map_err(|_| StatusCode::BAD_REQUEST)?.len();
                }
                Ok::<_, StatusCode>(total.to_string())
            }),
        )
    }

    fn caller() -> Authenticated {
        Authenticated {
            identity: Arc::new(Identity::new(
                ProviderName::try_new("test").expect("valid provider"),
                Principal::try_new("tester").expect("valid principal"),
            )),
            valid_until: None,
        }
    }

    /// Admits every request.
    #[derive(Clone)]
    struct Allow;

    impl RequestGate for Allow {
        fn admit(
            &self,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            std::future::ready(Ok(Admitted::Authenticated(caller())))
        }
    }

    /// Records the peer of every request it sees, and admits them all.
    #[derive(Clone, Default)]
    struct Recording {
        peers: Arc<Mutex<Vec<Peer>>>,
    }

    impl RequestGate for Recording {
        fn admit(
            &self,
            peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            let peers = Arc::clone(&self.peers);
            let peer = peer.clone();
            async move {
                peers.lock().push(peer);
                Ok(Admitted::Authenticated(caller()))
            }
        }
    }

    /// Admits every request: authenticated when it carries an `authorization` header, anonymous
    /// otherwise.
    #[derive(Clone)]
    struct ByAuthorization;

    impl RequestGate for ByAuthorization {
        fn admit(
            &self,
            _peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            let admitted = if parts.headers.contains_key(http::header::AUTHORIZATION) {
                Admitted::Authenticated(caller())
            } else {
                Admitted::Anonymous
            };
            std::future::ready(Ok(admitted))
        }
    }

    /// Runs `serve` behind `gate` with `tuning` as `listener` in a task, fed by the returned
    /// sender. Dropping the sender ends the stream.
    fn start_gated<G: RequestGate>(
        router: Router,
        gate: G,
        tuning: Http2Tuning,
        listener: Listener,
    ) -> (
        mpsc::UnboundedSender<Accepted>,
        JoinHandle<Result<(), NativeLinkEmbedError>>,
    ) {
        start_clocked(router, gate, tuning, listener, SystemClock::new())
    }

    /// Runs `serve` with an injected clock, returning its connection feed and task.
    fn start_clocked<G: RequestGate, C: Clock + Clone + Send + Sync + 'static>(
        router: Router,
        gate: G,
        tuning: Http2Tuning,
        listener: Listener,
        clock: C,
    ) -> (
        mpsc::UnboundedSender<Accepted>,
        JoinHandle<Result<(), NativeLinkEmbedError>>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel::<Accepted>();
        let connections = stream::unfold(receiver, |mut receiver| async move {
            let accepted = receiver.recv().await?;
            Some((accepted, receiver))
        })
        .boxed();
        let listening = Listening { connections, gate };
        let handoff = HandOff::new();
        let (ended, mut endings) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let serving = serve(
                listening,
                router,
                &tuning,
                listener,
                tuning.max_connections,
                clock,
                ServeShutdown {
                    ended,
                    handoff: handoff.clone(),
                },
            );
            tokio::pin!(serving);
            tokio::select! {
                result = &mut serving => result,
                _ = endings.recv() => {
                    handoff.begin();
                    serving.await
                }
            }
        });
        (sender, task)
    }

    /// Runs `serve` with `tuning` and an admitting gate, fed by the returned sender. See
    /// [`start_gated`].
    fn start_tuned(
        router: Router,
        tuning: Http2Tuning,
    ) -> (
        mpsc::UnboundedSender<Accepted>,
        JoinHandle<Result<(), NativeLinkEmbedError>>,
    ) {
        start_gated(router, Allow, tuning, Listener::Worker)
    }

    /// Runs `serve` with the default tuning, fed by the returned sender. See [`start_tuned`].
    fn start(
        router: Router,
    ) -> (
        mpsc::UnboundedSender<Accepted>,
        JoinHandle<Result<(), NativeLinkEmbedError>>,
    ) {
        start_tuned(router, Http2Tuning::default())
    }

    /// Hands the server half of a new in-memory connection to `serve`, with the default peer, and
    /// returns the client half.
    fn connect(feed: &mpsc::UnboundedSender<Accepted>) -> Result<DuplexStream, Box<dyn Error>> {
        connect_as(feed, Peer::default())
    }

    /// Hands the server half of a new in-memory connection to `serve`, with `peer`, and returns
    /// the client half.
    fn connect_as(
        feed: &mpsc::UnboundedSender<Accepted>,
        peer: Peer,
    ) -> Result<DuplexStream, Box<dyn Error>> {
        let (server, client) = tokio::io::duplex(PIPE_BUFFER);
        feed.send(Accepted {
            connection: Box::new(server),
            peer,
        })
        .map_err(|_| "serve has stopped")?;
        Ok(client)
    }

    /// Delays bytes in one direction; bytes in flight are bounded only by the peers' flow control,
    /// never by the link. Both tasks end once either side of the link closes.
    fn delayed<R, W>(mut from: R, mut to: W, one_way: Duration)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                match from.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(count) => {
                        let deliver_at = tokio::time::Instant::now() + one_way;
                        if sender.send((deliver_at, buffer[..count].to_vec())).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        tokio::spawn(async move {
            while let Some((deliver_at, bytes)) = receiver.recv().await {
                tokio::time::sleep_until(deliver_at).await;
                if to.write_all(&bytes).await.is_err() {
                    return;
                }
            }
            let _ = to.shutdown().await;
        });
    }

    /// Hands `serve` a connection whose round trip is `round_trip`.
    fn connect_over(
        feed: &mpsc::UnboundedSender<Accepted>,
        round_trip: Duration,
    ) -> Result<DuplexStream, Box<dyn Error>> {
        let server = connect(feed)?;
        let (client, link) = tokio::io::duplex(PIPE_BUFFER);
        let (server_read, server_write) = tokio::io::split(server);
        let (link_read, link_write) = tokio::io::split(link);
        delayed(link_read, server_write, round_trip / 2);
        delayed(server_read, link_write, round_trip / 2);
        Ok(client)
    }

    /// Opens an HTTP/2 client on the client half of a connection. The task drives the connection.
    async fn open(
        client: DuplexStream,
    ) -> Result<(SendRequest<Body>, JoinHandle<Result<(), hyper::Error>>), Box<dyn Error>> {
        let (sender, connection) =
            handshake::<_, _, Body>(TokioExecutor::new(), TokioIo::new(client)).await?;
        Ok((sender, tokio::spawn(connection)))
    }

    /// Sends `GET /` on `sender` and returns the status and body.
    async fn get_root(
        sender: &mut SendRequest<Body>,
    ) -> Result<(StatusCode, Vec<u8>), Box<dyn Error>> {
        sender.ready().await?;
        let request = Request::get("http://localhost/").body(Body::empty())?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let body = axum::body::to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
        Ok((status, body.to_vec()))
    }

    /// Sends an authenticated `GET /` and returns its status.
    async fn get_root_authorized(sender: &mut SendRequest<Body>) -> TestResult<StatusCode> {
        sender.ready().await?;
        let request = Request::get("http://localhost/")
            .header(http::header::AUTHORIZATION, "token")
            .body(Body::empty())?;
        Ok(sender.send_request(request).await?.status())
    }

    /// Sends an anonymous `GET /` and returns its status.
    async fn get_root_anonymous(sender: &mut SendRequest<Body>) -> TestResult<StatusCode> {
        sender.ready().await?;
        let request = Request::get("http://localhost/").body(Body::empty())?;
        Ok(sender.send_request(request).await?.status())
    }

    /// Lets every task run until all are idle: on the paused runtime, time moves only then.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    /// Opens an HTTP/1.1 client on the client half of a connection. The task drives it.
    async fn open_http1(
        client: DuplexStream,
    ) -> Result<
        (
            http1::SendRequest<Body>,
            JoinHandle<Result<(), hyper::Error>>,
        ),
        Box<dyn Error>,
    > {
        let (sender, connection) = http1::handshake::<_, Body>(TokioIo::new(client)).await?;
        Ok((sender, tokio::spawn(connection)))
    }

    /// Sends `GET /` on `sender` over HTTP/1.1 and returns the status and body.
    async fn get_root_http1(
        sender: &mut http1::SendRequest<Body>,
    ) -> Result<(StatusCode, Vec<u8>), Box<dyn Error>> {
        sender.ready().await?;
        let request = Request::get("http://localhost/").body(Body::empty())?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let body = axum::body::to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
        Ok((status, body.to_vec()))
    }

    /// Waits for `serve` to return and checks that it returned `Ok`.
    async fn finished(task: JoinHandle<Result<(), NativeLinkEmbedError>>) -> TestResult {
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn serves_requests_on_a_handed_connection() -> TestResult {
        let (feed, task) = start(router());
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, body) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn serves_several_connections() -> TestResult {
        let (feed, task) = start(router());
        let (mut first, first_connection) = open(connect(&feed)?).await?;
        let (mut second, second_connection) = open(connect(&feed)?).await?;
        for sender in [&mut first, &mut second] {
            let (status, body) = get_root(sender).await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, b"ok");
        }
        first_connection.abort();
        second_connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn serve_returns_when_the_stream_ends_and_idle_connections_close() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Listener::Public);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, _) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        drop(feed);
        // The client keeps its end open. Graceful shutdown ends the idle connection, so `serve`
        // returns without the client closing it.
        finished(task).await?;
        // The server has closed its end, so the client's connection ends as well.
        let _outcome: Result<(), hyper::Error> = connection.await?;
        Ok(())
    }

    #[tokio::test]
    async fn worker_connection_remains_usable_after_handoff() -> TestResult {
        let (feed, receiver) = mpsc::unbounded_channel::<Accepted>();
        let connections = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|accepted| (accepted, receiver))
        })
        .boxed();
        let handoff = HandOff::new();
        let (ended, mut endings) = mpsc::unbounded_channel();
        let tuning = Http2Tuning::default();
        let shutdown = ServeShutdown {
            ended,
            handoff: handoff.clone(),
        };
        let task = tokio::spawn(async move {
            serve(
                Listening {
                    connections,
                    gate: Allow,
                },
                router(),
                &tuning,
                Listener::Worker,
                tuning.max_connections,
                SystemClock::new(),
                shutdown,
            )
            .await
        });
        let (mut sender, connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root(&mut sender).await?.0, StatusCode::OK);
        drop(feed);
        endings.recv().await.ok_or("worker stream did not end")?;
        handoff.begin();
        assert_eq!(get_root(&mut sender).await?.0, StatusCode::OK);
        connection.abort();
        finished(task).await
    }

    #[tokio::test]
    async fn worker_connections_drain_when_workers_are_released() -> TestResult {
        let (feed, receiver) = mpsc::unbounded_channel::<Accepted>();
        let connections = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|accepted| (accepted, receiver))
        })
        .boxed();
        let handoff = HandOff::new();
        let (ended, mut endings) = mpsc::unbounded_channel();
        let shutdown = ServeShutdown {
            ended,
            handoff: handoff.clone(),
        };
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(0)));
        let task = tokio::spawn(async move {
            let tuning = Http2Tuning::default();
            serve(
                Listening {
                    connections,
                    gate: Allow,
                },
                router(),
                &tuning,
                Listener::Worker,
                tuning.max_connections,
                clock,
                shutdown,
            )
            .await
        });
        let (mut sender, connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root(&mut sender).await?.0, StatusCode::OK);
        drop(feed);
        endings.recv().await.ok_or("worker stream did not end")?;
        handoff.begin();
        assert_eq!(get_root(&mut sender).await?.0, StatusCode::OK);
        handoff.release_workers();
        let _outcome: Result<(), hyper::Error> = connection.await?;
        finished(task).await
    }

    #[tokio::test]
    async fn an_open_stream_is_cut_after_the_shutdown_grace() -> TestResult {
        let (entered, mut entries) = mpsc::unbounded_channel::<()>();
        let hanging = Router::new().route(
            "/hang",
            get(move || {
                let entered = entered.clone();
                async move {
                    // The handler never answers, so the request stays open.
                    let _ = entered.send(());
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let tuning = Http2Tuning {
            shutdown_grace: Duration::ZERO,
            ..Http2Tuning::default()
        };
        let (feed, task) = start_tuned(hanging, tuning);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        sender.ready().await?;
        let request = Request::get("http://localhost/hang").body(Body::empty())?;
        let request_task = tokio::spawn(async move { sender.send_request(request).await });
        entries.recv().await.ok_or("the handler never started")?;
        drop(feed);
        // The grace is zero, so `serve` cuts the open request as soon as the stream has ended.
        finished(task).await?;
        let _outcome: Result<(), hyper::Error> = connection.await?;
        request_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn dropping_serve_closes_connections() -> TestResult {
        let (feed, task) = start(router());
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, _) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        task.abort();
        let aborted = task.await;
        assert!(aborted.is_err_and(|error| error.is_cancelled()));
        // The client's connection ends once the server side is gone. Its own result may be an
        // error or a clean close; the test only requires that it finishes.
        let _outcome: Result<(), hyper::Error> = connection.await?;
        drop(feed);
        Ok(())
    }

    #[tokio::test]
    async fn a_broken_connection_does_not_stop_serving() -> TestResult {
        let (feed, task) = start(router());
        let mut broken = connect(&feed)?;
        broken
            .write_all(b"this is not an http/2 connection preface\r\n\r\n")
            .await?;
        drop(broken);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, body) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_gate_sees_the_connections_peer() -> TestResult {
        let gate = Recording::default();
        let seen = Arc::clone(&gate.peers);
        let (feed, task) = start_gated(router(), gate, Http2Tuning::default(), Listener::Worker);
        let peer = Peer {
            certificate: Some(CertSerial::from_be_bytes(&[0x2a])?),
            ..Peer::default()
        };
        let (mut sender, connection) = open(connect_as(&feed, peer.clone())?).await?;
        let (status, _) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(*seen.lock(), vec![peer]);
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_gate_also_sees_requests_no_route_handles() -> TestResult {
        let gate = Recording::default();
        let seen = Arc::clone(&gate.peers);
        let (feed, task) = start_gated(router(), gate, Http2Tuning::default(), Listener::Worker);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        sender.ready().await?;
        let request = Request::get("http://localhost/unknown").body(Body::empty())?;
        let response = sender.send_request(request).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(seen.lock().len(), 1);
        connection.abort();
        drop(feed);
        finished(task).await
    }

    /// Bazel pings every `CLIENT_KEEPALIVE_TIME` while an Execute or `WaitExecution` call waits, for
    /// as long as the action runs. No listener may answer that with a GOAWAY (gRPC's
    /// `too_many_pings`) or by ending the call. The span outlasts every server-side timer.
    #[tokio::test(start_paused = true)]
    async fn all_listeners_keep_a_pending_request_under_client_keepalive_pings() -> TestResult {
        const IDLE_SPAN: Duration = Duration::from_hours(1);
        let pings_in_span = IDLE_SPAN.as_secs() / CLIENT_KEEPALIVE_TIME.as_secs();

        for listener in [Listener::Public, Listener::Execution, Listener::Worker] {
            let (entered, mut entered_rx) = mpsc::unbounded_channel();
            let router = Router::new().route(
                "/",
                get(move || {
                    let entered = entered.clone();
                    async move {
                        let _ = entered.send(());
                        std::future::pending::<&'static str>().await
                    }
                }),
            );
            let (feed, task) = start_gated(router, Allow, Http2Tuning::default(), listener);
            let (mut sender, mut connection) = h2::client::handshake::<_>(connect(&feed)?).await?;
            let mut pings = connection.ping_pong().ok_or("ping handle unavailable")?;
            let connection = tokio::spawn(connection);
            let request = Request::get("http://localhost/").body(())?;
            let (response, _stream) = sender.send_request(request, true)?;
            tokio::pin!(response);
            entered_rx.recv().await.ok_or("handler did not start")?;

            for _ in 0..pings_in_span {
                tokio::time::advance(CLIENT_KEEPALIVE_TIME).await;
                pings.ping(h2::Ping::opaque()).await?;
            }

            assert!(
                futures::poll!(&mut response).is_pending(),
                "{listener:?} reset or completed the pending request"
            );
            assert!(!connection.is_finished(), "{listener:?} connection ended");

            // Starting another stream also detects a graceful GOAWAY that leaves existing streams open.
            sender = sender.ready().await?;
            let request = Request::get("http://localhost/").body(())?;
            let (_second_response, _second_stream) = sender.send_request(request, true)?;
            entered_rx
                .recv()
                .await
                .ok_or("second handler did not start")?;
            connection.abort();
            drop(feed);
            finished(task).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn the_public_listener_serves_http2_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Listener::Public);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, body) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_public_listener_serves_http1_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Listener::Public);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, body) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_execution_listener_serves_http1_and_http2() -> TestResult {
        let (feed, task) =
            start_gated(router(), Allow, Http2Tuning::default(), Listener::Execution);
        let (mut h1, h1_connection) = open_http1(connect(&feed)?).await?;
        let (mut h2, h2_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_http1(&mut h1).await?.0, StatusCode::OK);
        assert_eq!(get_root(&mut h2).await?.0, StatusCode::OK);
        h1_connection.abort();
        h2_connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_worker_listener_rejects_http1_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Listener::Worker);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let request = Request::get("http://localhost/").body(Body::empty())?;
        // The server reads the request as a broken HTTP/2 preface and closes the connection, which
        // may happen before or after the request is sent. No response may succeed.
        let answered = match sender.ready().await {
            Ok(()) => sender.send_request(request).await.is_ok(),
            Err(_) => false,
        };
        assert!(!answered);
        let _outcome: Result<(), hyper::Error> = connection.await?;
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn serve_returns_with_an_idle_http1_connection_open() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Listener::Public);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, _) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        drop(feed);
        // The client keeps its end open. Graceful shutdown closes the idle HTTP/1.1 connection, so
        // `serve` returns without the client closing it.
        finished(task).await?;
        // The server has closed its end, so the client's connection ends as well.
        let _outcome: Result<(), hyper::Error> = connection.await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_http1_request_head_is_closed_after_the_header_read_timeout() -> TestResult {
        let tuning = Http2Tuning {
            header_read_timeout: NonZeroDuration::from_secs(5),
            ..Http2Tuning::default()
        };
        let (feed, task) = start_gated(router(), Allow, tuning, Listener::Public);
        let mut client = connect(&feed)?;
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await?;
        tokio::time::sleep(Duration::from_secs(6)).await;
        // The server closes the stalled connection, so the client reads to end of stream. The
        // bound keeps the test from waiting forever if the connection stays open.
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut received))
            .await
            .map_err(|_| "the stalled connection stayed open")??;
        drop(feed);
        finished(task).await
    }

    #[tokio::test(start_paused = true)]
    async fn a_complete_http1_request_is_served_within_the_header_read_timeout() -> TestResult {
        let tuning = Http2Tuning {
            header_read_timeout: NonZeroDuration::from_secs(5),
            ..Http2Tuning::default()
        };
        let (feed, task) = start_gated(router(), Allow, tuning, Listener::Public);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, body) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    /// A body of `len` zero bytes, sent in chunks of at most `CHUNK` bytes.
    fn zeros(len: usize) -> Body {
        let chunks = (0..len).step_by(CHUNK).map(move |start| {
            Ok::<_, Infallible>(Bytes::from_static(&ZEROS[..CHUNK.min(len - start)]))
        });
        Body::from_stream(stream::iter(chunks))
    }

    /// Uploads one body of each of `sizes` at once on one connection `ROUND_TRIP` away, served
    /// with `tuning` as `listener`, and returns how many whole round trips the uploads took. A
    /// request answered first puts the server's settings and connection window in effect.
    async fn upload_round_trips(
        tuning: Http2Tuning,
        listener: Listener,
        sizes: &[usize],
    ) -> TestResult<u128> {
        let (feed, task) = start_gated(upload_router(), Allow, tuning, listener);
        let (mut sender, connection) = open(connect_over(&feed, ROUND_TRIP)?).await?;
        let (status, _) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        let started = tokio::time::Instant::now();
        let uploads = sizes.iter().map(|&size| {
            let mut sender = sender.clone();
            async move {
                sender.ready().await?;
                let request = Request::post("http://localhost/upload").body(zeros(size))?;
                let response = sender.send_request(request).await?;
                let received =
                    axum::body::to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
                assert_eq!(received, size.to_string().as_bytes());
                TestResult::Ok(())
            }
        });
        futures::future::try_join_all(uploads).await?;
        let round_trips = started.elapsed().as_nanos() / ROUND_TRIP.as_nanos();
        connection.abort();
        drop(feed);
        finished(task).await?;
        Ok(round_trips)
    }

    /// A client a long round trip away sends a whole receive window before the server's first
    /// acknowledgement: uploading exactly the window takes one round trip and one byte more takes
    /// two, whether one stream sends it or two streams share the connection's window. The default
    /// window is 16 MiB except on execution, where it is capped at 4 MiB by default.
    #[tokio::test(start_paused = true)]
    async fn a_client_sends_one_receive_window_per_round_trip() -> TestResult {
        let default = Http2Tuning::default;
        let four_mib = || Http2Tuning {
            receive_window: ReceiveWindow::from_units(4 * 1024 * 1024),
            ..Http2Tuning::default()
        };
        let cases = [
            (default(), Listener::Worker, vec![16 * MIB], 1),
            (default(), Listener::Worker, vec![16 * MIB + 1], 2),
            (default(), Listener::Worker, vec![8 * MIB, 8 * MIB], 1),
            (default(), Listener::Worker, vec![8 * MIB, 8 * MIB + 1], 2),
            (default(), Listener::Public, vec![16 * MIB], 1),
            (default(), Listener::Public, vec![16 * MIB + 1], 2),
            (default(), Listener::Execution, vec![4 * MIB], 1),
            (default(), Listener::Execution, vec![4 * MIB + 1], 2),
            (four_mib(), Listener::Worker, vec![4 * MIB], 1),
            (four_mib(), Listener::Worker, vec![4 * MIB + 1], 2),
        ];
        for (tuning, listener, sizes, expected) in cases {
            let round_trips = upload_round_trips(tuning, listener, &sizes).await?;
            assert_eq!(round_trips, expected, "{listener:?} uploading {sizes:?}");
        }
        Ok(())
    }

    /// Silent connections cannot prevent authenticated clients from earning the available places.
    #[tokio::test(start_paused = true)]
    async fn an_anonymous_flood_at_the_limit_never_blocks_an_authenticated_client() -> TestResult {
        let tuning = Http2Tuning {
            max_connections: NonZeroUsize::new(2).ok_or("nonzero")?,
            ..Http2Tuning::default()
        };
        let (feed, task) = start_gated(router(), ByAuthorization, tuning, Listener::Public);
        let _first_raw = connect(&feed)?;
        let _second_raw = connect(&feed)?;
        let (mut first, first_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut first).await?, StatusCode::OK);
        let (mut second, second_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut second).await?, StatusCode::OK);
        let mut raw = connect(&feed)?;
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), raw.read_to_end(&mut received))
            .await
            .map_err(|_| "the connection beyond the earned places stayed open")??;
        assert!(received.is_empty());
        assert_eq!(get_root_authorized(&mut first).await?, StatusCode::OK);
        assert_eq!(get_root_authorized(&mut second).await?, StatusCode::OK);
        first_connection.abort();
        second_connection.abort();
        drop(feed);
        finished(task).await
    }

    /// The oldest connection that has not earned its place is displaced first.
    #[tokio::test(start_paused = true)]
    async fn at_the_limit_the_oldest_unproven_connection_makes_room() -> TestResult {
        let tuning = Http2Tuning {
            max_connections: NonZeroUsize::new(3).ok_or("nonzero")?,
            ..Http2Tuning::default()
        };
        let (feed, task) = start_gated(router(), ByAuthorization, tuning, Listener::Public);
        let (mut proven, proven_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut proven).await?, StatusCode::OK);
        let mut first = connect(&feed)?;
        let mut second = connect(&feed)?;
        let (mut newcomer, newcomer_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut newcomer).await?, StatusCode::OK);
        tokio::time::timeout(Duration::from_secs(1), first.read_to_end(&mut Vec::new()))
            .await
            .map_err(|_| "the oldest unproven connection stayed open")??;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), second.read(&mut [0; 1]))
                .await
                .is_err(),
            "the newer unproven connection was closed"
        );
        assert_eq!(get_root_authorized(&mut proven).await?, StatusCode::OK);
        proven_connection.abort();
        newcomer_connection.abort();
        drop(second);
        drop(feed);
        finished(task).await
    }

    /// An idle unproven connection is displaced before a request in flight.
    #[tokio::test(start_paused = true)]
    async fn an_idle_unproven_connection_makes_room_before_a_busy_one() -> TestResult {
        let tuning = Http2Tuning {
            max_connections: NonZeroUsize::new(3).ok_or("nonzero")?,
            ..Http2Tuning::default()
        };
        let hanging = router().route("/hang", get(std::future::pending::<&'static str>));
        let (feed, task) = start_gated(hanging, ByAuthorization, tuning, Listener::Public);
        let (mut proven, proven_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut proven).await?, StatusCode::OK);
        let (mut worker, worker_connection) = open(connect(&feed)?).await?;
        let w_request = tokio::spawn(async move {
            let request = Request::get("http://localhost/hang").body(Body::empty())?;
            worker.send_request(request).await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
        settle().await;
        let mut first = connect(&feed)?;
        let mut second = connect(&feed)?;
        tokio::time::timeout(Duration::from_secs(1), first.read_to_end(&mut Vec::new()))
            .await
            .map_err(|_| "the idle unproven connection stayed open")??;
        settle().await;
        assert!(!w_request.is_finished());
        let third = connect(&feed)?;
        tokio::time::timeout(Duration::from_secs(1), second.read_to_end(&mut Vec::new()))
            .await
            .map_err(|_| "the idle unproven connection stayed open")??;
        settle().await;
        assert!(!w_request.is_finished());
        w_request.abort();
        worker_connection.abort();
        proven_connection.abort();
        drop(third);
        drop(feed);
        finished(task).await
    }

    /// When every unproven connection has a request in flight, an authenticated client still gets in: the oldest unproven connection makes room.
    #[tokio::test(start_paused = true)]
    async fn when_every_unproven_connection_is_busy_the_oldest_makes_room() -> TestResult {
        let tuning = Http2Tuning {
            max_connections: NonZeroUsize::new(2).ok_or("nonzero")?,
            ..Http2Tuning::default()
        };
        let hanging = router().route("/hang", get(std::future::pending::<&'static str>));
        let (feed, task) = start_gated(hanging, ByAuthorization, tuning, Listener::Public);
        let (mut a, a_connection) = open(connect(&feed)?).await?;
        let request = Request::get("http://localhost/hang").body(Body::empty())?;
        let a_request = tokio::spawn(async move { a.send_request(request).await });
        let (mut b, b_connection) = open(connect(&feed)?).await?;
        let request = Request::get("http://localhost/hang").body(Body::empty())?;
        let b_request = tokio::spawn(async move { b.send_request(request).await });
        settle().await;
        let (mut c, c_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut c).await?, StatusCode::OK);
        settle().await;
        assert!(
            a_request.is_finished(),
            "the oldest busy unproven connection stayed open"
        );
        assert!(a_request.await?.is_err());
        assert!(!b_request.is_finished());
        b_request.abort();
        a_connection.abort();
        b_connection.abort();
        c_connection.abort();
        drop(feed);
        finished(task).await
    }

    /// Worker connections do not expire while unproven and idle.
    #[tokio::test(start_paused = true)]
    async fn an_unproven_worker_connection_does_not_expire() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(0)));
        let tuning = Http2Tuning::default();
        let idle = tuning.unproven_idle_timeout.get();
        let (feed, task) = start_clocked(
            router(),
            ByAuthorization,
            tuning,
            Listener::Worker,
            Arc::clone(&clock),
        );
        let (mut sender, connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_anonymous(&mut sender).await?, StatusCode::OK);
        settle().await;
        clock.advance(idle * 10);
        settle().await;
        assert_eq!(get_root_anonymous(&mut sender).await?, StatusCode::OK);
        connection.abort();
        drop(feed);
        finished(task).await
    }

    /// An anonymous response keeps its connection busy until its streamed body ends.
    #[tokio::test(start_paused = true)]
    async fn an_anonymous_streamed_response_keeps_its_connection_until_it_ends() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(0)));
        let tuning = Http2Tuning::default();
        let idle = tuning.unproven_idle_timeout.get();
        let (chunks, receiver) = mpsc::unbounded_channel::<Result<Bytes, Infallible>>();
        let receiver = Arc::new(Mutex::new(Some(receiver)));
        let streaming = router().route(
            "/stream",
            get(move || {
                let receiver = receiver.lock().take();
                async move {
                    Body::from_stream(stream::unfold(receiver, |receiver| async move {
                        let mut receiver = receiver?;
                        let chunk = receiver.recv().await?;
                        Some((chunk, Some(receiver)))
                    }))
                }
            }),
        );
        let (feed, task) = start_clocked(
            streaming,
            ByAuthorization,
            tuning,
            Listener::Public,
            Arc::clone(&clock),
        );
        let (mut sender, connection) = open(connect(&feed)?).await?;
        sender.ready().await?;
        let request = Request::get("http://localhost/stream").body(Body::empty())?;
        let mut response = sender.send_request(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        settle().await;
        clock.advance(idle * 2);
        settle().await;
        chunks.send(Ok(Bytes::from_static(b"alive")))?;
        let frame = response
            .body_mut()
            .frame()
            .await
            .ok_or("missing body frame")??;
        assert_eq!(
            frame.into_data().map_err(|_| "expected data frame")?,
            "alive"
        );
        drop(chunks);
        while let Some(frame) = response.body_mut().frame().await {
            frame?;
        }
        settle().await;
        clock.advance(idle);
        settle().await;
        assert!(
            get_root_anonymous(&mut sender).await.is_err(),
            "the unproven connection outlived its idle timeout after its response ended"
        );
        connection.abort();
        drop(feed);
        finished(task).await
    }

    /// The injected clock expires a silent public connection but not an authenticated one.
    #[tokio::test]
    async fn an_unproven_public_connection_expires_after_the_idle_timeout_on_the_clock()
    -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(0)));
        let tuning = Http2Tuning::default();
        let idle = tuning.unproven_idle_timeout.get();
        let (feed, task) = start_clocked(
            router(),
            ByAuthorization,
            tuning,
            Listener::Public,
            Arc::clone(&clock),
        );
        let (mut proven, proven_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut proven).await?, StatusCode::OK);
        let mut raw = connect(&feed)?;
        // A later connection answering a request means the silent connection's task was spawned
        // before it and, on this current-thread runtime that runs tasks in order, has been polled,
        // so its idle timer is set.
        let (mut barrier, barrier_connection) = open(connect(&feed)?).await?;
        assert_eq!(get_root_authorized(&mut barrier).await?, StatusCode::OK);
        clock.advance(idle);
        tokio::time::timeout(Duration::from_secs(5), raw.read_to_end(&mut Vec::new()))
            .await
            .map_err(|_| "the unproven connection outlived its idle timeout")??;
        clock.advance(idle * 10);
        assert_eq!(get_root_authorized(&mut proven).await?, StatusCode::OK);
        proven_connection.abort();
        barrier_connection.abort();
        drop(feed);
        finished(task).await
    }

    /// Sends twice `tuning.max_concurrent_streams` requests that never finish on one connection,
    /// and returns how many of them the handler saw.
    async fn concurrent_requests(tuning: Http2Tuning) -> TestResult<u32> {
        let (entered, mut entries) = mpsc::unbounded_channel::<()>();
        let hanging = router().route(
            "/hang",
            get(move || {
                let entered = entered.clone();
                async move {
                    let _ = entered.send(());
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let sent = 2 * tuning.max_concurrent_streams.get();
        let (feed, task) = start_tuned(hanging, tuning);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        // Once a request is answered, the client honours the server's settings.
        let (status, _) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        let requests: Vec<_> = (0..sent)
            .map(|_| {
                let mut sender = sender.clone();
                tokio::spawn(async move {
                    sender.ready().await?;
                    let request = Request::get("http://localhost/hang").body(Body::empty())?;
                    sender.send_request(request).await?;
                    Ok::<_, Box<dyn Error + Send + Sync>>(())
                })
            })
            .collect();
        // The paused clock advances only once every task is idle, so the timeout fires only after
        // every request that can reach the handler has reached it.
        let mut seen = 0;
        while let Ok(Some(())) = tokio::time::timeout(Duration::from_secs(1), entries.recv()).await
        {
            seen += 1;
        }
        for request in requests {
            request.abort();
        }
        connection.abort();
        drop(feed);
        finished(task).await?;
        Ok(seen)
    }

    /// One HTTP/2 connection runs at most `max_concurrent_streams` requests at once: 100 by
    /// default, or as tuned; its client queues the rest.
    #[tokio::test(start_paused = true)]
    async fn a_connection_runs_at_most_max_concurrent_streams_requests() -> TestResult {
        let three = Http2Tuning {
            max_concurrent_streams: NonZeroU32::MIN.saturating_add(2),
            ..Http2Tuning::default()
        };
        assert_eq!(concurrent_requests(Http2Tuning::default()).await?, 100);
        assert_eq!(concurrent_requests(three).await?, 3);
        Ok(())
    }
}
