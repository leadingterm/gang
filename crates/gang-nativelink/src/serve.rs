//! The serve loop: runs one axum router over the connections the caller hands in.
//!
//! The caller accepts and authenticates every connection. Each one is served in a task
//! owned by the serve future, so dropping that future closes every connection. When the stream of
//! connections ends, every open connection is shut down gracefully and given `shutdown_grace` to
//! finish before it is dropped. HTTP/1.1 connections are closed if a request head stalls past
//! `header_read_timeout`; HTTP/2 connections are kept alive by `keepalive_interval` pings.

use axum::Extension;
use axum::Router;
use axum::middleware::from_fn_with_state;
use futures::StreamExt;
use gang_spi::{Listening, RequestGate};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::task::{JoinError, JoinSet};
use tokio::time::timeout;

use crate::config::Http2Tuning;
use crate::errors::NativeLinkEmbedError;
use crate::gate::admit;

/// The HTTP versions a listener serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocols {
    /// Serves HTTP/2 only. A connection that does not speak HTTP/2 is closed.
    H2,
    /// Detects HTTP/1.1 or HTTP/2 on each connection, including HTTP/2 with prior knowledge.
    Auto,
}

/// Serves `router` on the connections of `listening` until the stream ends and the served
/// connections have closed.
///
/// The listener's gate runs over every request before any route or fallback of `router` sees it.
/// Each connection's peer is put in the request extensions, so the gate can read it.
///
/// Each connection speaks the HTTP versions that `protocols` selects.
///
/// When the stream ends, every open connection is shut down gracefully: it finishes its in-flight
/// requests and closes. Connections still open after `tuning.shutdown_grace` are dropped.
///
/// HTTP/1.1 request heads must arrive within `tuning.header_read_timeout`, or the connection is
/// closed. HTTP/2 keeps connections alive with pings every `tuning.keepalive_interval`.
///
/// A failing connection is logged and never stops the loop.
pub(crate) async fn serve<G: RequestGate>(
    listening: Listening<G>,
    router: Router,
    tuning: &Http2Tuning,
    protocols: Protocols,
) -> Result<(), NativeLinkEmbedError> {
    let Listening {
        mut connections,
        gate,
    } = listening;
    let gated = router.layer(from_fn_with_state(gate, admit::<G>));
    let http = auto::Builder::new(TokioExecutor::new());
    let mut http = match protocols {
        Protocols::H2 => http.http2_only(),
        Protocols::Auto => http,
    };
    http.http1()
        .timer(TokioTimer::new())
        .header_read_timeout(tuning.header_read_timeout.get());
    http.http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(tuning.keepalive_interval.get());
    let graceful = GracefulShutdown::new();
    let mut served = JoinSet::new();
    loop {
        tokio::select! {
            next = connections.next() => match next {
                Some(accepted) => {
                    // The peer is the outermost layer, so the gate sees it on every request.
                    let service = gated.clone().layer(Extension(accepted.peer));
                    let watcher = graceful.watcher();
                    let http = http.clone();
                    served.spawn(async move {
                        let connection = http.serve_connection(
                            TokioIo::new(accepted.connection),
                            TowerToHyperService::new(service),
                        );
                        if let Err(error) = watcher.watch(connection).await {
                            tracing::warn!(error = %gang_types::error_chain(error.as_ref()), "connection ended with an error");
                        }
                    });
                }
                None => break,
            },
            Some(finished) = served.join_next(), if !served.is_empty() => reap(finished),
        }
    }
    let drained = timeout(tuning.shutdown_grace, async {
        graceful.shutdown().await;
        while let Some(finished) = served.join_next().await {
            reap(finished);
        }
    })
    .await;
    if drained.is_err() {
        // Dropping `served` on return aborts the connections that are still open.
        tracing::warn!(
            open = served.len(),
            "connections did not close within the shutdown grace"
        );
    }
    Ok(())
}

/// Logs a connection task that panicked or was cancelled; its connection is already gone.
fn reap(finished: Result<(), JoinError>) {
    if let Err(error) = finished {
        tracing::warn!(error = %gang_types::error_chain(&error), "connection task did not finish");
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::routing::get;
    use futures::stream;
    use gang_spi::{Accepted, Peer, PortError};
    use gang_testkit::TestResult;
    use gang_types::{CertSerial, NonZeroDuration};
    use hyper::client::conn::http1;
    use hyper::client::conn::http2::{SendRequest, handshake};
    use hyper::{Request, StatusCode};
    use parking_lot::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use super::*;

    /// Buffer of each in-memory connection.
    const PIPE_BUFFER: usize = 64 * 1024;
    /// Largest body a test reads from the router.
    const BODY_LIMIT: usize = 1024;

    fn router() -> Router {
        Router::new().route("/", get(|| async { "ok" }))
    }

    /// Admits every request.
    #[derive(Clone)]
    struct Allow;

    impl RequestGate for Allow {
        fn admit(
            &self,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<(), PortError>> + Send {
            std::future::ready(Ok(()))
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
        ) -> impl Future<Output = Result<(), PortError>> + Send {
            let peers = Arc::clone(&self.peers);
            let peer = peer.clone();
            async move {
                peers.lock().push(peer);
                Ok(())
            }
        }
    }

    /// Runs `serve` behind `gate` with `tuning` and `protocols` in a task, fed by the returned
    /// sender. Dropping the sender ends the stream.
    fn start_gated<G: RequestGate>(
        router: Router,
        gate: G,
        tuning: Http2Tuning,
        protocols: Protocols,
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
        let task = tokio::spawn(async move { serve(listening, router, &tuning, protocols).await });
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
        start_gated(router, Allow, tuning, Protocols::H2)
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
        let (feed, task) = start(router());
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
        let (feed, task) = start_gated(router(), gate, Http2Tuning::default(), Protocols::H2);
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
        let (feed, task) = start_gated(router(), gate, Http2Tuning::default(), Protocols::H2);
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

    #[tokio::test]
    async fn auto_serves_http2_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Protocols::Auto);
        let (mut sender, connection) = open(connect(&feed)?).await?;
        let (status, body) = get_root(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn auto_serves_http1_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Protocols::Auto);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, body) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }

    #[tokio::test]
    async fn h2_only_rejects_http1_connections() -> TestResult {
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Protocols::H2);
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
        let (feed, task) = start_gated(router(), Allow, Http2Tuning::default(), Protocols::Auto);
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
        let (feed, task) = start_gated(router(), Allow, tuning, Protocols::Auto);
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
        let (feed, task) = start_gated(router(), Allow, tuning, Protocols::Auto);
        let (mut sender, connection) = open_http1(connect(&feed)?).await?;
        let (status, body) = get_root_http1(&mut sender).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        connection.abort();
        drop(feed);
        finished(task).await
    }
}
