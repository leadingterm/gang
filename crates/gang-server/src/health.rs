//! Plaintext load-balancer health checks.

use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use gang_clock::Clock;
use gang_tls::Listener;
use gang_types::{NonZeroDuration, health::health_check};
use http::{Response, StatusCode};
use http_body_util::Empty;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::task::JoinSet;

#[derive(Debug, Default)]
struct Flags {
    serving: AtomicBool,
    leading: AtomicBool,
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Readiness(Arc<Flags>);

impl Readiness {
    pub(crate) fn serve(&self) {
        self.0.serving.store(true, Ordering::Release);
    }

    pub(crate) fn lead(&self) {
        self.0.leading.store(true, Ordering::Release);
    }

    pub(crate) fn stop(&self) {
        self.0.leading.store(false, Ordering::Release);
        self.0.serving.store(false, Ordering::Release);
    }

    pub(crate) fn status(&self, path: &str) -> StatusCode {
        let check = health_check();
        let ready = if path == check.serve_path() {
            self.0.serving.load(Ordering::Acquire)
        } else if path == check.leader_path() {
            self.0.leading.load(Ordering::Acquire)
        } else {
            return StatusCode::NOT_FOUND;
        };
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// The bounds of the health listener. Each exchange must finish within the load balancer's check
/// timeout, a protocol fact rather than a setting: an answer after it is never read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HealthLimits {
    /// How many connections may be open at once; further connections wait in the OS backlog.
    pub(crate) max_connections: NonZeroUsize,
    /// The largest request head a connection may send, in bytes.
    pub(crate) max_header_size: usize,
    /// Pause after a failed `accept` before accepting again.
    pub(crate) accept_error_backoff: NonZeroDuration,
}

/// Runs on the same runtime as the other listeners: a frozen or hung process cannot answer,
/// so the load balancer's timeout marks it unhealthy.
pub(crate) async fn serve_health<L: Listener, C: Clock>(
    mut listener: L,
    readiness: Readiness,
    clock: &C,
    limits: HealthLimits,
) -> Infallible {
    let mut http = http1::Builder::new();
    http.keep_alive(false).max_buf_size(limits.max_header_size);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept(), if connections.len() < limits.max_connections.get() => match accepted {
                Ok((io, _)) => {
                    let readiness = readiness.clone();
                    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let status = readiness.status(request.uri().path());
                        async move {
                            let mut response = Response::new(Empty::<Bytes>::new());
                            *response.status_mut() = status;
                            Ok::<_, Infallible>(response)
                        }
                    });
                    // The deadline starts at accept, before the connection task first runs.
                    let exchange = clock.timeout(
                        health_check().timeout(),
                        http.serve_connection(TokioIo::new(io), service),
                    );
                    connections.spawn(async move {
                        let _ = exchange.await;
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "health listener accept failed");
                    clock.sleep(limits.accept_error_backoff.get()).await;
                }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::num::NonZeroUsize;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use gang_tls::Listener;
    use gang_types::{NonZeroDuration, health::health_check};
    use http::{Request, StatusCode};
    use http_body_util::Empty;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncReadExt, DuplexStream};

    use super::{HealthLimits, Readiness, serve_health};
    use crate::test_support_memory::{MemoryDialer, MemoryListener, memory_listener};

    /// Limits that admit `max_connections` connections with the smallest header buffer.
    fn limits(max_connections: usize) -> HealthLimits {
        HealthLimits {
            max_connections: NonZeroUsize::new(max_connections)
                .expect("a test admits a connection"),
            max_header_size: 8 * 1024,
            accept_error_backoff: NonZeroDuration::from_secs(1),
        }
    }

    /// A memory listener that counts the connections it accepted.
    struct Counted {
        listener: MemoryListener,
        accepted: Arc<AtomicUsize>,
    }

    impl Listener for Counted {
        type Io = DuplexStream;
        type Addr = u32;

        async fn accept(&mut self) -> io::Result<(DuplexStream, u32)> {
            let accepted = self.listener.accept().await?;
            self.accepted.fetch_add(1, Ordering::SeqCst);
            Ok(accepted)
        }
    }

    fn assert_statuses(readiness: &Readiness, serve: StatusCode, leader: StatusCode) {
        let check = health_check();
        assert_eq!(readiness.status(check.serve_path()), serve);
        assert_eq!(readiness.status(check.leader_path()), leader);
        assert_eq!(readiness.status("/other"), StatusCode::NOT_FOUND);
    }

    #[test]
    fn status_tracks_lifecycle() {
        let readiness = Readiness::default();
        assert_statuses(
            &readiness,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        );
        readiness.serve();
        assert_statuses(&readiness, StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE);
        readiness.lead();
        assert_statuses(&readiness, StatusCode::OK, StatusCode::OK);
        readiness.stop();
        assert_statuses(
            &readiness,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        );
    }

    async fn request(dialer: &MemoryDialer, path: &str) -> gang_testkit::TestResult<StatusCode> {
        request_on(dialer.dial(), path).await
    }

    async fn request_on(io: DuplexStream, path: &str) -> gang_testkit::TestResult<StatusCode> {
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let request = Request::builder().uri(path).body(Empty::<Bytes>::new())?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        drop(response);
        drop(sender);
        driver.await??;
        Ok(status)
    }

    async fn assert_http(
        dialer: &MemoryDialer,
        serve: StatusCode,
        leader: StatusCode,
    ) -> gang_testkit::TestResult {
        let check = health_check();
        assert_eq!(request(dialer, check.serve_path()).await?, serve);
        assert_eq!(request(dialer, check.leader_path()).await?, leader);
        assert_eq!(request(dialer, "/other").await?, StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn listener_serves_lifecycle_statuses() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let readiness = Readiness::default();
        let clock = crate::test_support_clock::start();
        let server_readiness = readiness.clone();
        let task = tokio::spawn(async move {
            serve_health(listener, server_readiness, &*clock, limits(4)).await
        });
        assert_http(
            &dialer,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await?;
        readiness.serve();
        assert_http(&dialer, StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE).await?;
        readiness.lead();
        assert_http(&dialer, StatusCode::OK, StatusCode::OK).await?;

        readiness.stop();
        assert_http(
            &dialer,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn silent_connection_closes_at_the_health_check_timeout() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let server_clock = Arc::clone(&clock);
        let task = tokio::spawn(async move {
            serve_health(listener, Readiness::default(), &*server_clock, limits(2)).await
        });
        let mut silent = dialer.dial();
        // Dials are accepted in order: an answer on a later connection means the silent one was
        // accepted first, so its deadline is already running.
        assert_eq!(
            request(&dialer, health_check().serve_path()).await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        clock.advance(health_check().timeout());
        let mut unread = [0; 1];
        assert_eq!(
            silent.read(&mut unread).await?,
            0,
            "the server closes a connection that sends nothing within the check timeout"
        );
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn maximum_connection_limit_serves_requests() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let mut server = pin!(serve_health(
            listener,
            Readiness::default(),
            &*clock,
            limits(usize::MAX)
        ));
        let status = tokio::select! {
            never = &mut server => match never {},
            status = request_on(dialer.dial(), health_check().serve_path()) => status?,
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[tokio::test]
    async fn a_full_listener_accepts_the_next_connection_once_a_slot_frees()
    -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let accepted = Arc::new(AtomicUsize::new(0));
        let listener = Counted {
            listener,
            accepted: Arc::clone(&accepted),
        };
        let clock = crate::test_support_clock::start();
        let mut server = pin!(serve_health(
            listener,
            Readiness::default(),
            &*clock,
            limits(1)
        ));
        let mut silent = dialer.dial();
        let waiting = dialer.dial();
        // One poll accepts every connection it may without waiting.
        assert!(futures::poll!(server.as_mut()).is_pending());
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "a full listener accepts no further connection"
        );

        clock.advance(health_check().timeout());
        let status = tokio::select! {
            never = &mut server => match never {},
            status = request_on(waiting, health_check().serve_path()) => status?,
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let mut unread = [0; 1];
        assert_eq!(
            silent.read(&mut unread).await?,
            0,
            "the expired connection gave its slot to the waiting one"
        );
        Ok(())
    }

    #[tokio::test]
    async fn leader_is_unhealthy_while_controller_drains() -> gang_testkit::TestResult {
        use gang_spi::ControllerReady;
        use tokio::sync::oneshot;

        let (parts, _, handles) = crate::test_support::plain_parts();
        let release = handles.scheduler.hold_drain();
        let snapshots = handles.scheduler.snapshots();
        let drains = handles.scheduler.drains();
        let (health, dialer) = memory_listener();
        let server = crate::test_support::prepare_plain(parts)
            .await
            .with_health(Some(health));
        let (stop, stopped) = oneshot::channel();
        let run = server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        );
        let exercise = async {
            snapshots.reached(1).await;
            assert_http(&dialer, StatusCode::OK, StatusCode::OK).await?;
            stop.send(()).expect("server is running");
            drains.reached(1).await;
            assert_http(
                &dialer,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::SERVICE_UNAVAILABLE,
            )
            .await?;
            release.send(()).expect("controller is draining");
            gang_testkit::TestResult::Ok(())
        };
        let (result, exercised) = tokio::join!(run, exercise);
        exercised?;
        result?;
        Ok(())
    }

    #[tokio::test]
    async fn follower_is_not_leader_while_lease_is_held() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{ControllerId, ControllerReady, LeaderLeasePort};
        use tokio::sync::oneshot;

        let (mut parts, _, handles) = crate::test_support::plain_parts();
        let now = handles.clock.wall();
        handles
            .repository
            .acquire(
                &ControllerId::new("other"),
                now,
                now + std::time::Duration::from_mins(1),
            )
            .await?;
        parts.listeners.leader = crate::LeaderListeners::Dedicated {
            execution: "127.0.0.1:31001".parse()?,
            worker: "127.0.0.1:31002".parse()?,
            binder: crate::test_support_deployment::MemoryBinder::default(),
        };
        let mut settings = crate::test_support::plain_settings();
        settings.execution_endpoint = Some(gang_types::Url::parse("http://127.0.0.1:31001/")?);
        let (health, dialer) = memory_listener();
        let server = crate::Server::prepare(parts, settings)
            .await?
            .with_health(Some(health));
        let (stop, stopped) = oneshot::channel();
        let (ready, readiness) = ControllerReady::channel();
        let run = server.run(
            async {
                let _ = stopped.await;
            },
            ready,
        );
        let exercise = async {
            readiness.await?;
            assert_http(&dialer, StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE).await?;
            stop.send(()).expect("server is running");
            gang_testkit::TestResult::Ok(())
        };
        let (result, exercised) = tokio::join!(run, exercise);
        exercised?;
        result?;
        Ok(())
    }
}
