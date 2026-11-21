//! The serve loop: runs one axum router over the connections the caller hands in.
//!
//! The caller accepts and authenticates every connection. Each one is served in a task
//! owned by the serve future, so dropping that future closes every connection. Once all listener
//! streams have ended, connections close. The runtime ends listeners only after the original
//! controller has drained and the host is unrouted.
//! HTTP/1.1 connections are closed if a request head stalls past `header_read_timeout`;
//! HTTP/2 connections are kept alive by `keepalive_interval` pings.
//! Each HTTP/2 connection, and each of its streams, may receive `receive_window` bytes before the
//! server reads them, so a client far away uploads a whole window per round trip. A listener serves
//! a bounded number of connections at once; connections earn their place by an authenticated
//! request. An HTTP/2 connection runs at most `max_concurrent_streams` requests at once; with the
//! window, these bound the request data held (see [`Http2Config`]).

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use axum::Extension;
use axum::Router;
use axum::middleware::from_fn_with_state;
use futures::StreamExt;
use gang_clock::Clock;
use gang_config::Http2Config;
use gang_spi::{Listening, RequestGate, RequestScope};
use gang_types::SaturatingInto;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::errors::NativeLinkEmbedError;
use crate::gate::{GateState, admit};
use crate::handoff::HandOff;
use crate::refusals::Refusals;
use crate::standing::Standing;

/// The listener a serve loop runs, which decides the HTTP versions its connections speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listener {
    /// The public listener: detects HTTP/1.1 or HTTP/2 on each connection, including HTTP/2 with
    /// prior knowledge.
    Public,
    /// The worker listener: serves HTTP/2 only. A connection that does not speak HTTP/2 is
    /// closed.
    Worker,
}

impl Listener {
    /// The listener's name in logs.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Worker => "worker",
        }
    }
}

/// The configured receive window is bounded by HTTP/2's RFC limits.
fn window_for(tuning: &Http2Config, _listener: Listener) -> u32 {
    // ReceiveWindow is between 65,535 and 2^31 - 1 bytes.
    tuning.receive_window.get().get().saturating_into()
}

fn http_server(tuning: &Http2Config, listener: Listener) -> auto::Builder<TokioExecutor> {
    let mut http = match listener {
        Listener::Worker => auto::Builder::new(TokioExecutor::new()).http2_only(),
        Listener::Public => {
            let mut http = auto::Builder::new(TokioExecutor::new());
            http.http1()
                .timer(TokioTimer::new())
                .header_read_timeout(tuning.header_read_timeout.get());
            http
        }
    };
    let window = window_for(tuning, listener);
    http.http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(tuning.keepalive_interval.get())
        .max_concurrent_streams(tuning.max_concurrent_streams.get())
        .initial_connection_window_size(window)
        .initial_stream_window_size(window);
    http
}

/// Routers grouped by their admission scope, with the fallback owned by `shared`.
pub(crate) struct ScopedRoutes {
    /// Leader-independent services and the listener's fallback.
    pub(crate) shared: Router,
    /// Leader-only services, without a separate fallback.
    pub(crate) leader: Router,
}

impl ScopedRoutes {
    fn gated<G: RequestGate>(self, gate: G, listener: Listener) -> Router {
        let shared = self.shared.layer(from_fn_with_state(
            GateState {
                gate: gate.clone(),
                scope: if listener == Listener::Worker {
                    RequestScope::WorkerStore
                } else {
                    RequestScope::Controller
                },
            },
            admit::<G>,
        ));
        let leader = self.leader.layer(from_fn_with_state(
            GateState {
                gate,
                scope: RequestScope::Controller,
            },
            admit::<G>,
        ));
        shared.merge(leader)
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
/// Hand-off retires pre-existing admitted public connections without closing fresh refusal
/// connections before their complete not-started response.
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
/// `clock`. On the public listener, a connection that has not earned its place is also closed once
/// it has had no request in flight for `tuning.unproven_idle_timeout`, before its first request
/// included; the worker listener's connections are bounded by the fleets, and a worker's store
/// connections may stay idle until it runs an action.
///
/// A failing connection is logged and never stops the loop.
pub(crate) async fn serve<G: RequestGate, C: Clock + Clone + Send + Sync + 'static>(
    listening: Listening<G>,
    routes: ScopedRoutes,
    tuning: &Http2Config,
    listener: Listener,
    max_connections: NonZeroUsize,
    clock: C,
    shutdown: ServeShutdown,
) -> Result<(), NativeLinkEmbedError> {
    let Listening {
        mut connections,
        gate,
    } = listening;
    let gated = routes.gated(gate, listener);
    let http = http_server(tuning, listener);
    let (graceful, _) = watch::channel(());
    let mut served = JoinSet::new();
    let mut refusals = Refusals::new(listener, max_connections, tuning.refusal_log_interval);
    let mut open: VecDeque<Open> = VecDeque::new();
    let idle = (listener == Listener::Public).then_some(tuning.unproven_idle_timeout.get());
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
                    if listener == Listener::Public {
                        shutdown.handoff.public_connection(&standing);
                    }
                    let service = gated
                        .clone()
                        .layer(Extension(accepted.peer))
                        .layer(Extension(Arc::clone(&standing)));
                    let mut shutdown = graceful.subscribe();
                    let http = http.clone();
                    let clock = clock.clone();
                    let expiring = Arc::clone(&standing);
                    let abort = served.spawn(async move {
                        let connection = http.serve_connection(
                            TokioIo::new(accepted.connection),
                            TowerToHyperService::new(service),
                        );
                        tokio::pin!(connection);
                        let expired = async {
                            match idle {
                                Some(idle) => expiring.expired(&clock, idle).await,
                                None => std::future::pending().await,
                            }
                        };
                        tokio::pin!(expired);
                        let result = tokio::select! {
                            result = &mut connection => Some(result),
                            () = &mut expired => None,
                            () = graceful_trigger(&mut shutdown, &expiring) => {
                                // After actual routing-away, an unadmitted peer with no
                                // request may be dropped even if its HTTP/2 preface stalled.
                                // A refusal already in flight must send its complete response.
                                if !expiring.is_admitted() && !expiring.is_busy() {
                                    return;
                                }
                                connection.as_mut().graceful_shutdown();
                                tokio::select! {
                                    result = &mut connection => Some(result),
                                    () = &mut expired => None,
                                }
                            }
                        };
                        if let Some(Err(error)) = result {
                            tracing::warn!(error = %gang_types::error_chain(error.as_ref()), "connection ended with an error");
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
    // Release listening sockets and pending shared accept guards before connection drain.
    drop(connections);
    let _ = shutdown.ended.send(());
    loop {
        tokio::select! {
            () = shutdown.handoff.begun() => break,
            Some(finished) = served.join_next_with_id(), if !served.is_empty() => reap(finished, &mut open),
        }
    }
    if listener == Listener::Worker {
        loop {
            if served.is_empty() {
                return Ok(());
            }
            tokio::select! {
                () = shutdown.handoff.workers_released() => break,
                Some(finished) = served.join_next_with_id() => reap(finished, &mut open),
            }
        }
    }
    let _ = graceful.send(());
    while let Some(finished) = served.join_next_with_id().await {
        reap(finished, &mut open);
    }
    Ok(())
}

/// Waits for listener shutdown or this connection's retry decision.
async fn graceful_trigger(shutdown: &mut watch::Receiver<()>, standing: &Standing) {
    tokio::select! {
        _ = shutdown.changed() => {}
        () = standing.retry_requested() => {}
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
    use gang_spi::{Accepted, Admitted, Authenticated, GateDecision, Peer, PortError};
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
    use gang_config::ReceiveWindow;

    #[test]
    fn public_and_worker_windows_preserve_the_configured_rfc_bounds() {
        let mut tuning = Http2Config::default();
        assert_eq!(window_for(&tuning, Listener::Public), 16 * 1024 * 1024);
        assert_eq!(window_for(&tuning, Listener::Worker), 16 * 1024 * 1024);
        tuning.max_message_size =
            gang_types::NonZeroBytes::new(gang_types::Bytes::new(1)).expect("nonzero");
        for window in [65_535_u32, 2_147_483_647] {
            tuning.receive_window = ReceiveWindow::from_units(u64::from(window));
            assert_eq!(window_for(&tuning, Listener::Public), window);
            assert_eq!(window_for(&tuning, Listener::Worker), window);
        }
    }

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
            let tuning = Http2Config::default();
            serve(
                Listening {
                    connections,
                    gate: Allow,
                },
                shared_routes(router),
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

    fn shared_intake(
        listener: Arc<tokio::sync::Mutex<tokio::net::TcpListener>>,
        locked: mpsc::UnboundedSender<()>,
    ) -> futures::stream::BoxStream<'static, Accepted> {
        stream::unfold(listener, move |listener| {
            let locked = locked.clone();
            async move {
                let (connection, _) = {
                    let guard = listener.lock().await;
                    let _ = locked.send(());
                    guard.accept().await.ok()?
                };
                Some((
                    Accepted {
                        connection: Box::new(connection),
                        peer: Peer::default(),
                    },
                    listener,
                ))
            }
        })
        .boxed()
    }

    #[tokio::test]
    async fn ended_intake_releases_shared_accept_guard_before_drain() -> TestResult {
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let listener = Arc::new(tokio::sync::Mutex::new(listener));
            let (locked, mut locks) = mpsc::unbounded_channel();
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let connections = shared_intake(Arc::clone(&listener), locked)
                .take_until(async {
                    let _ = stopped.await;
                })
                .boxed();
            let (entered, mut entries) = mpsc::unbounded_channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let released = Arc::clone(&release);
            let router = Router::new().route(
                "/",
                get(move || {
                    let entered = entered.clone();
                    let released = Arc::clone(&released);
                    async move {
                        let _ = entered.send(());
                        released.notified().await;
                        "completed"
                    }
                }),
            );
            let handoff = HandOff::new();
            let (ended, mut endings) = mpsc::unbounded_channel();
            let shutdown = ServeShutdown {
                ended,
                handoff: handoff.clone(),
            };
            let task = tokio::spawn(async move {
                let tuning = Http2Config::default();
                serve(
                    Listening {
                        connections,
                        gate: Allow,
                    },
                    shared_routes(router),
                    &tuning,
                    Listener::Public,
                    tuning.max_connections,
                    SystemClock::new(),
                    shutdown,
                )
                .await
            });
            let response = tokio::spawn(async move {
                let mut client = tokio::net::TcpStream::connect(address).await?;
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await?;
                let mut response = Vec::new();
                client.read_to_end(&mut response).await?;
                Ok::<_, std::io::Error>(response)
            });
            entries
                .recv()
                .await
                .ok_or("admitted request did not start")?;
            // The second acquisition is the suspended next accept, not the completed first one.
            for _ in 0..2 {
                locks
                    .recv()
                    .await
                    .ok_or("intake never held its shared accept guard")?;
            }
            stop.send(()).map_err(|()| "intake stopped early")?;
            endings.recv().await.ok_or("intake end was not reported")?;
            assert!(
                !task.is_finished(),
                "the old admitted request still holds drain"
            );
            let next_accept = async {
                let guard = listener.lock().await;
                guard.accept().await
            };
            let (accepted, fresh) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::try_join!(next_accept, tokio::net::TcpStream::connect(address))
            })
            .await??;
            drop((accepted, fresh));
            handoff.begin();
            handoff.release_workers();
            release.notify_one();
            let response = tokio::time::timeout(Duration::from_secs(1), response).await???;
            assert!(response.ends_with(b"completed"));
            finished(task).await?;
        }
        Ok(())
    }

    fn shared_routes(shared: Router) -> ScopedRoutes {
        ScopedRoutes {
            shared,
            leader: Router::new(),
        }
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
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(Ok(GateDecision::Admit(Admitted::Authenticated(caller()))))
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
            _scope: RequestScope,
            peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            let peers = Arc::clone(&self.peers);
            let peer = peer.clone();
            async move {
                peers.lock().push(peer);
                Ok(GateDecision::Admit(Admitted::Authenticated(caller())))
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
            _scope: RequestScope,
            _peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            let admitted = if parts.headers.contains_key(http::header::AUTHORIZATION) {
                Admitted::Authenticated(caller())
            } else {
                Admitted::Anonymous
            };
            std::future::ready(Ok(GateDecision::Admit(admitted)))
        }
    }

    /// Selects the admission outcome with a test-only request header.
    #[derive(Clone)]
    struct ByOutcome;

    impl RequestGate for ByOutcome {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            let outcome = match parts
                .headers
                .get("x-test-outcome")
                .and_then(|v| v.to_str().ok())
            {
                Some("authenticated") => Ok(GateDecision::Admit(Admitted::Authenticated(caller()))),
                Some("unavailable") => Err(PortError::unavailable("test unavailable")),
                Some("invalid") => Err(PortError::refused(gang_spi::Rejection::InvalidProof)),
                _ => Ok(GateDecision::Admit(Admitted::Anonymous)),
            };
            std::future::ready(outcome)
        }
    }

    /// Runs `serve` behind `gate` with `tuning` as `listener` in a task, fed by the returned
    /// sender. Dropping the sender ends the stream.
    fn start_gated<G: RequestGate>(
        router: Router,
        gate: G,
        tuning: Http2Config,
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
        tuning: Http2Config,
        listener: Listener,
        clock: C,
    ) -> (
        mpsc::UnboundedSender<Accepted>,
        JoinHandle<Result<(), NativeLinkEmbedError>>,
    ) {
        start_scoped_in_mode(shared_routes(router), gate, tuning, listener, clock)
    }

    fn start_scoped_in_mode<G: RequestGate, C: Clock + Clone + Send + Sync + 'static>(
        routes: ScopedRoutes,
        gate: G,
        tuning: Http2Config,
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
                routes,
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
                    handoff.release_workers();
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
        tuning: Http2Config,
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
        start_tuned(router, Http2Config::default())
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

    /// Sends a gRPC-shaped request and drains its response before the next request.
    async fn request_outcome(
        sender: &mut SendRequest<Body>,
        outcome: &str,
    ) -> TestResult<(StatusCode, Option<String>)> {
        sender.ready().await?;
        let request = Request::get("http://localhost/")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("x-test-outcome", outcome)
            .body(Body::empty())?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let grpc_status = response
            .headers()
            .get("grpc-status")
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()?;
        axum::body::to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
        Ok((status, grpc_status))
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
        let (feed, task) = start_gated(router(), Allow, Http2Config::default(), Listener::Public);
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
        let tuning = Http2Config::default();
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
                shared_routes(router()),
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
            let tuning = Http2Config::default();
            serve(
                Listening {
                    connections,
                    gate: Allow,
                },
                shared_routes(router()),
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
    async fn transport_waits_for_explicit_handoff_before_closing() -> TestResult {
        let (entered, mut entries) = mpsc::unbounded_channel::<()>();
        let release = Arc::new(tokio::sync::Notify::new());
        let released = Arc::clone(&release);
        let hanging = Router::new().route(
            // This explicit response release models completion of the original owned work.
            "/hang",
            get(move || {
                let entered = entered.clone();
                let released = Arc::clone(&released);
                async move {
                    // Hold an actual response until its owner permits completion.
                    let _ = entered.send(());
                    released.notified().await;
                    "complete"
                }
            }),
        );
        let tuning = Http2Config::default();
        let (feed, task) = start_tuned(hanging, tuning);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        sender.ready().await?;
        let request = Request::get("http://localhost/hang").body(Body::empty())?;
        let request_task = tokio::spawn(async move { sender.send_request(request).await });
        entries.recv().await.ok_or("the handler never started")?;
        drop(feed);
        assert!(!task.is_finished(), "no timer may cut a live response");
        release.notify_one();
        request_task.await??;
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
        let (feed, task) = start_gated(router(), gate, Http2Config::default(), Listener::Worker);
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
        let (feed, task) = start_gated(router(), gate, Http2Config::default(), Listener::Worker);
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

    #[derive(Clone)]
    struct ScopedTestGate {
        entered: mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
    }

    impl RequestGate for ScopedTestGate {
        async fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> Result<GateDecision, PortError> {
            if parts.uri.path() == "/leader" {
                return Ok(GateDecision::NotLeading);
            }
            if parts.headers.contains_key("x-hold-admission") {
                let _ = self.entered.send(());
                self.release.notified().await;
            }
            let admitted = if parts.headers.contains_key(http::header::AUTHORIZATION) {
                Admitted::Authenticated(caller())
            } else {
                Admitted::Anonymous
            };
            Ok(GateDecision::Admit(admitted))
        }
    }

    fn retirement_routes(
        calls: Arc<std::sync::atomic::AtomicUsize>,
        entered: mpsc::UnboundedSender<()>,
        released: Arc<tokio::sync::Notify>,
    ) -> ScopedRoutes {
        use std::sync::atomic::Ordering;

        let shared = Router::new()
            .route("/", get(|| async { "ok" }))
            .route(
                "/held",
                get(move || {
                    let entered = entered.clone();
                    let released = Arc::clone(&released);
                    async move {
                        let _ = entered.send(());
                        released.notified().await;
                        "completed"
                    }
                }),
            )
            .route(
                "/api",
                get(|| async {
                    let mut response = axum::response::IntoResponse::into_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                    response
                        .extensions_mut()
                        .insert(gang_spi::LeaderUnavailable);
                    response
                }),
            );
        let leader = Router::new().route(
            "/leader",
            post(move || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    "must not run"
                }
            }),
        );
        ScopedRoutes { shared, leader }
    }

    fn hold_server_eof(server: DuplexStream) -> (DuplexStream, JoinHandle<()>, JoinHandle<()>) {
        let (client, proxy) = tokio::io::duplex(PIPE_BUFFER);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let (mut proxy_read, mut proxy_write) = tokio::io::split(proxy);
        let upstream = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut proxy_read, &mut server_write).await;
        });
        // Retain the client-facing write half after server EOF: a clean client
        // connection completion therefore requires HTTP/2 GOAWAY, not socket EOF.
        let downstream = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut server_read, &mut proxy_write).await;
            std::future::pending::<()>().await;
            drop(proxy_write);
        });
        (client, upstream, downstream)
    }

    async fn assert_retirement_preserves_shared_streams(
        authenticated: bool,
        listener: Listener,
        api_marker: bool,
    ) -> TestResult {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, mut entries) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let (gate_entered, mut gate_entries) = mpsc::unbounded_channel();
        let gate_release = Arc::new(tokio::sync::Notify::new());
        let routes = retirement_routes(Arc::clone(&calls), entered, Arc::clone(&release));
        let (feed, task) = start_scoped_in_mode(
            routes,
            ScopedTestGate {
                entered: gate_entered,
                release: Arc::clone(&gate_release),
            },
            Http2Config::default(),
            listener,
            SystemClock::new(),
        );
        let (client, upstream, downstream) = hold_server_eof(connect(&feed)?);
        let (mut sender, connection) = h2::client::handshake(client).await?;
        let connection = tokio::spawn(connection);
        let mut first = Request::get("http://localhost/");
        if authenticated {
            first = first.header(http::header::AUTHORIZATION, "token");
        }
        let (response, _) = sender.send_request(first.body(())?, true)?;
        assert_eq!(response.await?.status(), StatusCode::OK);
        let mut held = Vec::new();
        sender = sender.ready().await?;
        let (response, _) =
            sender.send_request(Request::get("http://localhost/held").body(())?, true)?;
        held.push(response);
        entries.recv().await.ok_or("shared handler did not start")?;
        sender = sender.ready().await?;
        let (response, _) = sender.send_request(
            Request::get("http://localhost/held")
                .header("x-hold-admission", "true")
                .body(())?,
            true,
        )?;
        held.push(response);
        gate_entries
            .recv()
            .await
            .ok_or("shared admission did not start")?;
        sender = sender.ready().await?;
        let request = if api_marker {
            Request::get("http://localhost/api")
        } else {
            Request::post("http://localhost/leader")
                .header(http::header::CONTENT_TYPE, "application/grpc")
        };
        let (response, _) = sender.send_request(request.body(())?, true)?;
        let response = response.await?;
        if api_marker {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        } else {
            assert_eq!(response.headers()["grpc-status"], "14");
            assert_eq!(
                response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
                "true"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!connection.is_finished());
        gate_release.notify_one();
        entries
            .recv()
            .await
            .ok_or("accepted shared request was rejected")?;
        release.notify_waiters();
        for response in held {
            let mut body = response.await?.into_body();
            assert_eq!(
                body.data().await.ok_or("missing shared body")??,
                "completed"
            );
        }
        tokio::time::timeout(Duration::from_secs(1), connection).await???;
        upstream.abort();
        downstream.abort();
        drop(sender);
        drop(feed);
        finished(task).await
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_retirement_sends_goaway_without_cancelling_accepted_shared_streams()
    -> TestResult {
        for authenticated in [false, true] {
            for (listener, api_marker) in [
                (Listener::Public, false),
                (Listener::Public, true),
                (Listener::Worker, false),
            ] {
                assert_retirement_preserves_shared_streams(authenticated, listener, api_marker)
                    .await?;
            }
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_unavailable_grpc_retires_both_http2_listeners() -> TestResult {
        for listener in [Listener::Public, Listener::Worker] {
            let (feed, task) = start_gated(router(), ByOutcome, Http2Config::default(), listener);
            let (mut sender, connection) = open(connect(&feed)?).await?;
            assert_eq!(
                request_outcome(&mut sender, "unavailable").await?,
                (StatusCode::OK, Some("14".to_owned()))
            );
            // Keep the sender alive: closure must come from the server, not the client.
            tokio::time::timeout(Duration::from_secs(1), connection).await???;
            drop(sender);
            drop(feed);
            finished(task).await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_unavailable_http1_returns_503_then_closes() -> TestResult {
        let (feed, task) = start_gated(
            router(),
            ByOutcome,
            Http2Config::default(),
            Listener::Public,
        );
        let mut client = connect(&feed)?;
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nx-test-outcome: unavailable\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut response)).await??;
        assert!(
            response.starts_with(b"HTTP/1.1 503 "),
            "unexpected response: {}",
            String::from_utf8_lossy(&response)
        );
        drop(client);
        drop(feed);
        finished(task).await
    }

    #[tokio::test(start_paused = true)]
    async fn admitted_http2_connections_survive_later_unavailable() -> TestResult {
        for listener in [Listener::Public, Listener::Worker] {
            for admitted in ["authenticated", "anonymous"] {
                let (feed, task) =
                    start_gated(router(), ByOutcome, Http2Config::default(), listener);
                let (mut sender, connection) = open(connect(&feed)?).await?;
                assert_eq!(
                    request_outcome(&mut sender, admitted).await?,
                    (StatusCode::OK, None)
                );
                assert_eq!(
                    request_outcome(&mut sender, "unavailable").await?,
                    (StatusCode::OK, Some("14".to_owned()))
                );
                assert_eq!(
                    request_outcome(&mut sender, admitted).await?,
                    (StatusCode::OK, None)
                );
                connection.abort();
                drop(feed);
                finished(task).await?;
            }
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_invalid_proof_does_not_retire_http2_connection() -> TestResult {
        for listener in [Listener::Public, Listener::Worker] {
            let (feed, task) = start_gated(router(), ByOutcome, Http2Config::default(), listener);
            let (mut sender, connection) = open(connect(&feed)?).await?;
            assert_eq!(
                request_outcome(&mut sender, "invalid").await?,
                (StatusCode::OK, Some("16".to_owned()))
            );
            assert_eq!(
                request_outcome(&mut sender, "anonymous").await?,
                (StatusCode::OK, None)
            );
            connection.abort();
            drop(feed);
            finished(task).await?;
        }
        Ok(())
    }

    /// Bazel pings every `CLIENT_KEEPALIVE_TIME` while an Execute or `WaitExecution` call waits, for
    /// as long as the action runs. No listener may answer that with a GOAWAY (gRPC's
    /// `too_many_pings`) or by ending the call. The span outlasts every server-side timer.
    #[tokio::test(start_paused = true)]
    async fn all_listeners_keep_a_pending_request_under_client_keepalive_pings() -> TestResult {
        const IDLE_SPAN: Duration = Duration::from_hours(1);
        let pings_in_span = IDLE_SPAN.as_secs() / CLIENT_KEEPALIVE_TIME.as_secs();

        for listener in [Listener::Public, Listener::Worker] {
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
            let (feed, task) = start_gated(router, Allow, Http2Config::default(), listener);
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
        let (feed, task) = start_gated(router(), Allow, Http2Config::default(), Listener::Public);
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
        let (feed, task) = start_gated(router(), Allow, Http2Config::default(), Listener::Public);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, body) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn the_worker_listener_rejects_http1_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Config::default(), Listener::Worker);
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
        let (feed, task) = start_gated(router(), Allow, Http2Config::default(), Listener::Public);
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
        let tuning = Http2Config {
            header_read_timeout: NonZeroDuration::from_secs(5),
            ..Http2Config::default()
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
        let tuning = Http2Config {
            header_read_timeout: NonZeroDuration::from_secs(5),
            ..Http2Config::default()
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
        tuning: Http2Config,
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
    /// window is 16 MiB on both listeners.
    #[tokio::test(start_paused = true)]
    async fn a_client_sends_one_receive_window_per_round_trip() -> TestResult {
        let default = Http2Config::default;
        let four_mib = || Http2Config {
            receive_window: ReceiveWindow::from_units(4 * 1024 * 1024),
            ..Http2Config::default()
        };
        let cases = [
            (default(), Listener::Worker, vec![16 * MIB], 1),
            (default(), Listener::Worker, vec![16 * MIB + 1], 2),
            (default(), Listener::Worker, vec![8 * MIB, 8 * MIB], 1),
            (default(), Listener::Worker, vec![8 * MIB, 8 * MIB + 1], 2),
            (default(), Listener::Public, vec![16 * MIB], 1),
            (default(), Listener::Public, vec![16 * MIB + 1], 2),
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
        let tuning = Http2Config {
            max_connections: NonZeroUsize::new(2).ok_or("nonzero")?,
            ..Http2Config::default()
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
        let tuning = Http2Config {
            max_connections: NonZeroUsize::new(3).ok_or("nonzero")?,
            ..Http2Config::default()
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
        let tuning = Http2Config {
            max_connections: NonZeroUsize::new(3).ok_or("nonzero")?,
            ..Http2Config::default()
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
        let tuning = Http2Config {
            max_connections: NonZeroUsize::new(2).ok_or("nonzero")?,
            ..Http2Config::default()
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
        let tuning = Http2Config::default();
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
        let tuning = Http2Config::default();
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
        let tuning = Http2Config::default();
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
    async fn concurrent_requests(tuning: Http2Config) -> TestResult<u32> {
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
        let three = Http2Config {
            max_concurrent_streams: NonZeroU32::MIN.saturating_add(2),
            ..Http2Config::default()
        };
        assert_eq!(concurrent_requests(Http2Config::default()).await?, 100);
        assert_eq!(concurrent_requests(three).await?, 3);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn final_transport_shutdown_closes_unadmitted_stalled_prefaces() -> TestResult {
        for listener in [Listener::Public, Listener::Worker] {
            for prefix in [b"".as_slice(), b"PRI * HTTP/2.0\r\n".as_slice()] {
                let clock = Arc::new(TestClock::new(Timestamp::UNIX_EPOCH));
                let tuning = Http2Config {
                    unproven_idle_timeout: NonZeroDuration::from_secs(5),
                    ..Http2Config::default()
                };
                let (feed, task) =
                    start_clocked(Router::new(), Allow, tuning, listener, Arc::clone(&clock));
                let mut peer = connect(&feed)?;
                peer.write_all(prefix).await?;
                settle().await;
                drop(feed);
                settle().await;
                tokio::time::timeout(Duration::from_secs(1), finished(task)).await??;
                peer.read_to_end(&mut Vec::new()).await?;
            }
        }
        Ok(())
    }

    #[derive(Clone)]
    struct SignalledRefusal {
        entered: mpsc::UnboundedSender<()>,
        release: Option<Arc<tokio::sync::Notify>>,
    }

    impl RequestGate for SignalledRefusal {
        async fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> Result<GateDecision, PortError> {
            let _ = self.entered.send(());
            if let Some(release) = &self.release {
                release.notified().await;
            }
            Err(PortError::unavailable("controller is draining"))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_marked_503_finishes_after_refusal_intake_stops() -> TestResult {
        let (entered, mut entries) = mpsc::unbounded_channel();
        let (feed, task) = start_clocked(
            Router::new().fallback(|| async { StatusCode::NOT_FOUND }),
            SignalledRefusal {
                entered,
                release: None,
            },
            Http2Config::default(),
            Listener::Public,
            SystemClock::new(),
        );
        // One byte of downstream buffering keeps the actual response pending until read.
        let (server, mut peer) = tokio::io::duplex(1);
        feed.send(Accepted {
            connection: Box::new(server),
            peer: Peer::default(),
        })
        .map_err(|_| "refusal listener stopped")?;
        peer.write_all(b"POST /v1/login HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await?;
        entries
            .recv()
            .await
            .ok_or("request never reached the refusal gate")?;
        drop(feed);
        settle().await;
        assert!(
            !task.is_finished(),
            "the pending response must not be cancelled"
        );
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut response)).await??;
        let response = String::from_utf8(response)?;
        assert!(response.starts_with("HTTP/1.1 503 "));
        assert!(response.contains("gang-request-not-started: true\r\n"));
        assert!(response.ends_with("unavailable"));
        finished(task).await
    }

    #[tokio::test(start_paused = true)]
    async fn pending_grpc_refusals_finish_after_intake_stops_on_both_roles() -> TestResult {
        for listener in [Listener::Public, Listener::Worker] {
            let (entered, mut entries) = mpsc::unbounded_channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let (feed, task) = start_clocked(
                Router::new().fallback(|| async { StatusCode::NOT_FOUND }),
                SignalledRefusal {
                    entered,
                    release: Some(Arc::clone(&release)),
                },
                Http2Config::default(),
                listener,
                SystemClock::new(),
            );
            let (mut sender, connection) = open(connect(&feed)?).await?;
            let response = tokio::spawn(async move {
                sender.ready().await?;
                sender
                    .send_request(
                        Request::post("http://localhost/grpc.Service/Method")
                            .header(http::header::CONTENT_TYPE, "application/grpc")
                            .body(Body::empty())
                            .expect("valid request"),
                    )
                    .await
            });
            entries
                .recv()
                .await
                .ok_or("request never reached the refusal gate")?;
            drop(feed);
            settle().await;
            assert!(
                !task.is_finished(),
                "the pending refusal must not be cancelled"
            );
            release.notify_one();
            let response = response.await??;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["grpc-status"], "14");
            assert_eq!(
                response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
                "true"
            );
            let body = axum::body::to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
            assert!(body.is_empty());
            tokio::time::timeout(Duration::from_secs(1), connection).await???;
            finished(task).await?;
        }
        Ok(())
    }
}
