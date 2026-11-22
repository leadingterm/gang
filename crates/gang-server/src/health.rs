//! Plaintext load-balancer health checks.

use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::proxy_protocol::{HeaderKind, prepare};
use bytes::Bytes;
use gang_clock::Clock;
use gang_config::ProxyProtocolConfig;
use gang_spi::Fence;
use gang_tls::Listener;
use gang_types::{NonZeroDuration, health::health_check};
use http::{Response, StatusCode};
use http_body_util::Empty;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use tokio::task::JoinSet;

#[derive(Default)]
struct Flags {
    serving: AtomicBool,
    leading: AtomicBool,
    promoted: AtomicBool,
    fence: Mutex<Option<Fence>>,
}

#[derive(Clone, Default)]
pub(crate) struct Readiness(Arc<Flags>);

impl Readiness {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        let clock = crate::test_support_clock::start();
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        keeper.extend_to(clock.monotonic() + std::time::Duration::from_mins(1));
        let readiness = Self::default().with_fence(fence);
        readiness.serve();
        readiness.lead();
        readiness
    }

    #[cfg(test)]
    pub(crate) fn with_fence(self, fence: Fence) -> Self {
        *self.0.fence.lock() = Some(fence);
        self
    }

    pub(crate) fn begin_term(&self, fence: Fence) {
        self.stop();
        self.0.promoted.store(false, Ordering::Release);
        *self.0.fence.lock() = Some(fence);
    }

    pub(crate) fn serve(&self) {
        self.0.serving.store(true, Ordering::Release);
    }

    pub(crate) fn lead(&self) {
        self.0.promoted.store(true, Ordering::Release);
        self.0.leading.store(true, Ordering::Release);
    }

    pub(crate) fn stop(&self) {
        self.0.leading.store(false, Ordering::Release);
        self.0.serving.store(false, Ordering::Release);
    }

    pub(crate) fn is_leading(&self) -> bool {
        self.0.serving.load(Ordering::Acquire)
            && self.0.leading.load(Ordering::Acquire)
            && self.0.fence.lock().as_ref().is_some_and(Fence::holds)
    }

    pub(crate) fn ready(&self) -> bool {
        self.is_leading()
    }

    /// A promoted engine keeps fail-static admission through lease lapse until actual handoff.
    pub(crate) fn admitting(&self) -> bool {
        self.0.promoted.load(Ordering::Acquire) && self.0.serving.load(Ordering::Acquire)
    }
    /// Only a previously promoted original engine may keep authenticated worker store I/O.
    pub(crate) fn worker_store_admitting(&self) -> bool {
        self.0.promoted.load(Ordering::Acquire)
    }

    pub(crate) fn status(&self, path: &str) -> StatusCode {
        let available = if path == health_check().ready_path() {
            self.ready()
        } else if path == "/prepared" {
            self.0.serving.load(Ordering::Acquire)
        } else {
            return StatusCode::NOT_FOUND;
        };
        if available {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// The bounds of the health listener. Each exchange must finish within the load balancer's check
/// timeout, a protocol fact rather than a setting: an answer after it is never read.
#[derive(Debug, Clone)]
pub(crate) struct HealthLimits {
    /// How many connections may be open at once; further connections wait in the OS backlog.
    pub(crate) max_connections: NonZeroUsize,
    /// The largest request head a connection may send, in bytes.
    pub(crate) max_header_size: usize,
    /// Pause after a failed `accept` before accepting again.
    pub(crate) accept_error_backoff: NonZeroDuration,
    pub(crate) proxy_protocol: Option<Arc<ProxyProtocolConfig>>,
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
                Ok((io, addr)) => {
                    let raw = L::peer_ip(&addr);
                    let config = limits.proxy_protocol.clone();
                    let header_clock = config
                        .as_ref()
                        .filter(|config| config.header_timeout.get() < health_check().timeout())
                        .map(|config| clock.sleep(config.header_timeout.get()));
                    let http = http.clone();
                    let readiness = readiness.clone();
                    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let status = readiness.status(request.uri().path());
                        let leading = readiness.is_leading();
                        async move {
                            let mut response = Response::new(Empty::<Bytes>::new());
                            *response.status_mut() = status;
                            if status != StatusCode::NOT_FOUND {
                                response.headers_mut().insert(
                                    gang_types::health::LEADING_HEADER,
                                    if leading {
                                        http::HeaderValue::from_static("true")
                                    } else {
                                        http::HeaderValue::from_static("false")
                                    },
                                );
                            }
                            Ok::<_, Infallible>(response)
                        }
                    });
                    // The deadline starts at accept, before the connection task first runs.
                    let exchange = clock.timeout(health_check().timeout(), async move {
                        let io = if let Some(config) = config {
                            let preface = prepare(io, raw, &config, HeaderKind::Health);
                            tokio::select! {
                                result = preface => match result {
                                    Ok((io, _)) => io,
                                    Err(error) => {
                                        tracing::debug!(%error, "health proxy preface rejected");
                                        return;
                                    }
                                },
                                () = async {
                                    match header_clock {
                                        Some(sleep) => sleep.await,
                                        None => std::future::pending().await,
                                    }
                                } => return,
                            }
                        } else {
                            io
                        };
                        let _ = http.serve_connection(TokioIo::new(io), service).await;
                    });
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
    use std::time::Duration;

    use bytes::Bytes;
    use gang_clock::Clock;
    use gang_spi::{Fence, Keeper};
    use gang_tls::Listener;
    use gang_types::{NonZeroDuration, health::health_check};
    use http::{Request, StatusCode};
    use http_body_util::Empty;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::{HealthLimits, Readiness, serve_health};
    use crate::test_support_memory::{MemoryDialer, MemoryListener, memory_listener};

    fn valid_fence() -> (Fence, Keeper) {
        let clock = crate::test_support_clock::start();
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        keeper.extend_to(clock.monotonic() + Duration::from_mins(1));
        (fence, keeper)
    }

    /// Limits that admit `max_connections` connections with the smallest header buffer.
    fn limits(max_connections: usize) -> HealthLimits {
        HealthLimits {
            max_connections: NonZeroUsize::new(max_connections)
                .expect("a test admits a connection"),
            max_header_size: 8 * 1024,
            accept_error_backoff: NonZeroDuration::from_secs(1),
            proxy_protocol: None,
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

    struct Trusted(MemoryListener);

    impl Listener for Trusted {
        type Io = DuplexStream;
        type Addr = u32;

        async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
            self.0.accept().await
        }

        fn peer_ip(_: &u32) -> Option<std::net::IpAddr> {
            Some(std::net::Ipv4Addr::new(10, 0, 1, 4).into())
        }
    }

    struct ProbePeer<const LOOPBACK: bool>(MemoryListener);

    impl<const LOOPBACK: bool> Listener for ProbePeer<LOOPBACK> {
        type Io = DuplexStream;
        type Addr = u32;

        async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
            self.0.accept().await
        }

        fn peer_ip(_: &u32) -> Option<std::net::IpAddr> {
            Some(
                if LOOPBACK {
                    std::net::Ipv4Addr::LOCALHOST
                } else {
                    std::net::Ipv4Addr::new(10, 0, 1, 99)
                }
                .into(),
            )
        }
    }

    fn assert_statuses(readiness: &Readiness, ready: StatusCode) {
        assert_eq!(readiness.status(health_check().ready_path()), ready);
        assert_eq!(readiness.status("/serve"), StatusCode::NOT_FOUND);
        assert_eq!(readiness.status("/leader"), StatusCode::NOT_FOUND);
        assert_eq!(readiness.status("/other"), StatusCode::NOT_FOUND);
    }

    #[test]
    fn only_the_promoted_holder_is_ready_and_drain_keeps_worker_store_admission() {
        let readiness = Readiness::default();
        let (fence, _keeper) = valid_fence();
        readiness.begin_term(fence);
        readiness.serve();
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!readiness.admitting());
        readiness.lead();
        assert_statuses(&readiness, StatusCode::OK);
        assert!(readiness.admitting());
        readiness.stop();
        assert!(!readiness.admitting());
        assert!(readiness.worker_store_admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        let (next_fence, _next_keeper) = valid_fence();
        readiness.begin_term(next_fence);
        readiness.serve();
        assert!(!readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn status_tracks_lifecycle() {
        let mut readiness = Readiness::default();
        assert!(!readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        readiness.serve();
        assert!(!readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        readiness.lead();
        assert!(readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        let (fence, keeper) = valid_fence();
        readiness = readiness.with_fence(fence);
        assert!(readiness.admitting());
        assert_statuses(&readiness, StatusCode::OK);
        keeper.revoke();
        assert!(readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        readiness.stop();
        assert!(!readiness.admitting());
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn readiness_tracks_lease_validity() {
        use gang_clock::Clock;

        let clock = crate::test_support_clock::start();
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        let readiness = Readiness::default().with_fence(fence);
        readiness.serve();
        readiness.lead();
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!readiness.is_leading());
        keeper.extend_to(clock.monotonic() + std::time::Duration::from_secs(2));
        assert!(readiness.is_leading());
        assert_statuses(&readiness, StatusCode::OK);
        clock.advance(std::time::Duration::from_secs(3));
        assert_statuses(&readiness, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!readiness.is_leading());
        keeper.extend_to(clock.monotonic() + std::time::Duration::from_secs(2));
        assert_statuses(&readiness, StatusCode::OK);
    }

    async fn request(dialer: &MemoryDialer, path: &str) -> gang_testkit::TestResult<StatusCode> {
        request_on(dialer.dial(), path).await
    }

    async fn request_on(io: DuplexStream, path: &str) -> gang_testkit::TestResult<StatusCode> {
        Ok(request_with_leading(io, path).await?.0)
    }

    async fn request_with_leading(
        io: DuplexStream,
        path: &str,
    ) -> gang_testkit::TestResult<(StatusCode, Option<http::HeaderValue>)> {
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let request = Request::builder().uri(path).body(Empty::<Bytes>::new())?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let leading = response
            .headers()
            .get(gang_types::health::LEADING_HEADER)
            .cloned();
        drop(response);
        drop(sender);
        driver.await??;
        Ok((status, leading))
    }

    async fn assert_http(dialer: &MemoryDialer, ready: StatusCode) -> gang_testkit::TestResult {
        assert_eq!(request(dialer, health_check().ready_path()).await?, ready);
        assert_eq!(request(dialer, "/serve").await?, StatusCode::NOT_FOUND);
        assert_eq!(request(dialer, "/leader").await?, StatusCode::NOT_FOUND);
        assert_eq!(request(dialer, "/other").await?, StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn listener_serves_lifecycle_statuses() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(2));
        let readiness = Readiness::default().with_fence(fence);
        let server_readiness = readiness.clone();
        let server_clock = Arc::clone(&clock);
        let task = tokio::spawn(async move {
            serve_health(listener, server_readiness, &*server_clock, limits(4)).await
        });
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(
            request(&dialer, "/prepared").await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        readiness.serve();
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(request(&dialer, "/prepared").await?, StatusCode::OK);
        readiness.lead();
        assert_http(&dialer, StatusCode::OK).await?;
        assert_eq!(request(&dialer, "/prepared").await?, StatusCode::OK);
        clock.advance(Duration::from_secs(3));
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(request(&dialer, "/prepared").await?, StatusCode::OK);
        readiness.stop();
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(
            request(&dialer, "/prepared").await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn never_ready_controller_cannot_report_prepared_or_ready() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let (fence, _keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        let readiness = Readiness::default().with_fence(fence);
        let server_readiness = readiness.clone();
        let task = tokio::spawn(async move {
            serve_health(listener, server_readiness, &*clock, limits(4)).await
        });
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(
            request(&dialer, "/prepared").await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        readiness.lead();
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        assert_eq!(
            request(&dialer, "/prepared").await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        readiness.serve();
        let (status, leading) = request_with_leading(dialer.dial(), "/prepared").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            leading.as_ref().and_then(|value| value.to_str().ok()),
            Some("false")
        );
        assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn health_header_reports_fenced_leadership_not_warm_readiness() -> gang_testkit::TestResult
    {
        let (listener, dialer) = memory_listener();
        let (fence, keeper) = valid_fence();
        let readiness = Readiness::default().with_fence(fence);
        let clock = crate::test_support_clock::start();
        let server_readiness = readiness.clone();
        let task = tokio::spawn(async move {
            serve_health(listener, server_readiness, &*clock, limits(4)).await
        });
        for (step, (status, leading)) in [
            (StatusCode::SERVICE_UNAVAILABLE, "false"),
            (StatusCode::SERVICE_UNAVAILABLE, "false"),
            (StatusCode::OK, "true"),
            (StatusCode::SERVICE_UNAVAILABLE, "false"),
            (StatusCode::SERVICE_UNAVAILABLE, "false"),
        ]
        .into_iter()
        .enumerate()
        {
            let (actual_status, actual_leading) =
                request_with_leading(dialer.dial(), health_check().ready_path()).await?;
            assert_eq!(actual_status, status);
            assert_eq!(
                actual_leading
                    .as_ref()
                    .and_then(|value| value.to_str().ok()),
                Some(leading)
            );
            match step {
                0 => {
                    readiness.serve();
                }
                1 => readiness.lead(),
                2 => keeper.revoke(),
                _ => readiness.stop(),
            }
        }
        let (status, leading) = request_with_leading(dialer.dial(), "/other").await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(leading.is_none());
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn proxy_health_preserves_readiness_and_rejects_missing_headers()
    -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let (fence, _keeper) = valid_fence();
        let readiness = Readiness::default().with_fence(fence);
        let server_readiness = readiness.clone();
        let mut limits = limits(4);
        limits.proxy_protocol = Some(Arc::new(gang_config::ProxyProtocolConfig {
            trusted_cidrs: vec!["10.0.1.4/32".parse()?],
            ..gang_config::ProxyProtocolConfig::default()
        }));
        let task = tokio::spawn(async move {
            serve_health(Trusted(listener), server_readiness, &*clock, limits).await
        });
        for (step, (ready, prepared)) in [
            (
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (StatusCode::SERVICE_UNAVAILABLE, StatusCode::OK),
            (StatusCode::OK, StatusCode::OK),
        ]
        .into_iter()
        .enumerate()
        {
            for (path, status) in [
                (health_check().ready_path(), ready),
                ("/prepared", prepared),
            ] {
                let mut io = dialer.dial();
                io.write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
                assert_eq!(request_on(io, path).await?, status);
            }
            if step == 0 {
                readiness.serve();
            } else if step == 1 {
                readiness.lead();
            }
        }
        let mut framed = dialer.dial();
        framed
            .write_all(
                b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c\xc0\0\x02\x01\xc0\0\x02\x02\0\x01\0\x02",
            )
            .await?;
        assert_eq!(
            request_on(framed, health_check().ready_path()).await?,
            StatusCode::OK
        );
        let mut io = dialer.dial();
        io.write_all(b"GET /prepared HTTP/1.1\r\n\r\n").await?;
        let mut byte = [0; 1];
        assert_eq!(io.read(&mut byte).await?, 0);
        let mut invalid = dialer.dial();
        invalid
            .write_all(b"\r\n\r\n\0\r\nQUIT\n\x31\x11\0\x64")
            .await?;
        let mut byte = [0; 1];
        assert_eq!(invalid.read(&mut byte).await?, 0);
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn loopback_health_requires_local_proxy_header() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let (fence, _keeper) = valid_fence();
        let readiness = Readiness::default().with_fence(fence);
        let server_readiness = readiness.clone();
        let mut limits = limits(4);
        limits.proxy_protocol = Some(Arc::new(gang_config::ProxyProtocolConfig {
            trusted_cidrs: vec!["10.0.1.4/32".parse()?],
            ..gang_config::ProxyProtocolConfig::default()
        }));
        let task = tokio::spawn(async move {
            serve_health(
                ProbePeer::<true>(listener),
                server_readiness,
                &*clock,
                limits,
            )
            .await
        });
        readiness.serve();
        for (path, expected) in [
            ("/prepared", StatusCode::OK),
            (health_check().ready_path(), StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let mut io = dialer.dial();
            io.write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
            assert_eq!(request_on(io, path).await?, expected);
        }
        readiness.lead();
        let mut io = dialer.dial();
        io.write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
        assert_eq!(
            request_on(io, health_check().ready_path()).await?,
            StatusCode::OK
        );

        for bytes in [
            &b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c\xc0\0\x02\x01\xc0\0\x02\x02\0\x01\0\x02"[..],
            &b"GET /prepared HTTP/1.1\r\n\r\n"[..],
        ] {
            let mut io = dialer.dial();
            io.write_all(bytes).await?;
            let mut byte = [0; 1];
            assert_eq!(io.read(&mut byte).await?, 0);
        }
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn untrusted_remote_health_rejects_local_proxy_header() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let mut limits = limits(4);
        limits.proxy_protocol = Some(Arc::new(gang_config::ProxyProtocolConfig {
            trusted_cidrs: vec!["10.0.1.4/32".parse()?],
            ..gang_config::ProxyProtocolConfig::default()
        }));
        let task = tokio::spawn(async move {
            serve_health(
                ProbePeer::<false>(listener),
                Readiness::default(),
                &*clock,
                limits,
            )
            .await
        });
        let mut io = dialer.dial();
        io.write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
        let mut byte = [0; 1];
        assert_eq!(io.read(&mut byte).await?, 0);
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn health_proxy_header_uses_its_shorter_deadline() -> gang_testkit::TestResult {
        let (listener, dialer) = memory_listener();
        let clock = crate::test_support_clock::start();
        let server_clock = Arc::clone(&clock);
        let mut limits = limits(2);
        limits.proxy_protocol = Some(Arc::new(gang_config::ProxyProtocolConfig {
            trusted_cidrs: vec!["10.0.1.4/32".parse()?],
            header_timeout: NonZeroDuration::from_secs(1),
            ..gang_config::ProxyProtocolConfig::default()
        }));
        let task = tokio::spawn(async move {
            serve_health(
                Trusted(listener),
                Readiness::default(),
                &*server_clock,
                limits,
            )
            .await
        });
        let mut stalled = dialer.dial();
        stalled.write_all(b"\r\n").await?;
        let mut framed = dialer.dial();
        framed.write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
        assert_eq!(
            request_on(framed, health_check().ready_path()).await?,
            StatusCode::SERVICE_UNAVAILABLE
        );
        clock.advance(Duration::from_secs(1));
        let mut byte = [0; 1];
        assert_eq!(stalled.read(&mut byte).await?, 0);
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
            request(&dialer, health_check().ready_path()).await?,
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
            status = request_on(dialer.dial(), health_check().ready_path()) => status?,
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
            status = request_on(waiting, health_check().ready_path()) => status?,
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
            assert_http(&dialer, StatusCode::OK).await?;
            assert_eq!(request(&dialer, "/prepared").await?, StatusCode::OK);
            stop.send(()).expect("server is running");
            drains.reached(1).await;
            assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
            assert_eq!(
                request(&dialer, "/prepared").await?,
                StatusCode::SERVICE_UNAVAILABLE
            );
            release.send(()).expect("controller is draining");
            handles.clock.advance(
                crate::test_support::plain_settings()
                    .leadership
                    .renew_interval
                    .get(),
            );
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
        use gang_spi::{
            CandidateGeneration, ControllerId, ControllerReady, LeaderLeasePort, LeaseCandidate,
            Takeover,
        };
        use tokio::sync::oneshot;

        let (parts, _, handles) = crate::test_support::plain_parts();
        let now = handles.clock.wall();
        handles
            .repository
            .acquire(
                &LeaseCandidate {
                    holder: ControllerId::new("other"),
                    priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(
                        u64::MAX,
                    )),
                },
                now,
                now,
                now + std::time::Duration::from_mins(1),
                Takeover::Observe,
            )
            .await?;
        let settings = crate::test_support::plain_settings();
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
            assert_http(&dialer, StatusCode::SERVICE_UNAVAILABLE).await?;
            assert_eq!(request(&dialer, "/prepared").await?, StatusCode::OK);
            stop.send(()).expect("server is running");
            gang_testkit::TestResult::Ok(())
        };
        let (result, exercised) = tokio::join!(run, exercise);
        exercised?;
        result?;
        Ok(())
    }
}
