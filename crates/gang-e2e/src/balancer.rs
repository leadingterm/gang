//! Loopback NLB model: HTTP health checks on the health port, fail-open routing, and
//! connection termination off. Frozen processes still complete TCP handshakes, so an
//! application response is needed to establish liveness.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use gang_clock::Clock;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::{Condvar, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::errors::E2eError;

/// The independent listeners of the controller load balancer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Frontend {
    /// Public traffic, served by every controller.
    Serve,
    /// Execution traffic, served by the leader.
    Execute,
    /// Worker traffic, served by the leader.
    Workers,
}

impl Frontend {
    /// The controller health path checked for this frontend.
    #[must_use]
    pub fn path(self) -> &'static str {
        let health = gang_types::health::health_check();
        match self {
            Self::Serve => health.serve_path(),
            Self::Execute | Self::Workers => health.leader_path(),
        }
    }
}

/// A controller's distinct traffic and HTTP health endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Target {
    /// Address receiving proxied traffic.
    pub traffic: SocketAddr,
    /// Address receiving HTTP health checks.
    pub health: SocketAddr,
}

/// Health-check frequency and consecutive-result threshold.
#[derive(Clone, Copy, Debug)]
pub struct Probe {
    /// Time between probe rounds.
    pub interval: Duration,
    /// Maximum duration of one HTTP check.
    pub timeout: Duration,
    /// Consecutive successes or failures needed to change health.
    pub threshold: NonZeroU32,
}

impl Default for Probe {
    fn default() -> Self {
        let health = gang_types::health::health_check();
        Self {
            interval: health.interval(),
            timeout: health.timeout(),
            threshold: health.threshold(),
        }
    }
}

struct TargetHealth {
    health: SocketAddr,
    healthy: bool,
    successes: u32,
    failures: u32,
    #[cfg(test)]
    started: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    finished: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    completed: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct Targets {
    entries: BTreeMap<SocketAddr, Arc<Mutex<TargetHealth>>>,
    next: usize,
}

#[derive(Default)]
struct Changes {
    revision: Mutex<u64>,
    changed: Condvar,
}

impl Changes {
    fn notify(&self) {
        let mut revision = self.revision.lock();
        *revision = revision.wrapping_add(1);
        self.changed.notify_all();
    }
}

impl Targets {
    fn choose(&mut self) -> Option<SocketAddr> {
        let healthy = self.entries.values().any(|target| target.lock().healthy);
        let candidates = || {
            self.entries
                .iter()
                .filter(|(_, target)| !healthy || target.lock().healthy)
        };
        let count = candidates().count();
        if count == 0 {
            return None;
        }
        let index = self.next % count;
        let selected = candidates().nth(index).map(|(address, _)| *address);
        self.next = index + 1;
        selected
    }
}

struct Listener {
    address: SocketAddr,
    targets: Arc<Mutex<Targets>>,
    accept: JoinHandle<()>,
    health: JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.accept.abort();
        self.health.abort();
    }
}

/// NLB model with HTTP health checks, fail-open routing, and connection termination off.
/// TCP handshakes alone cannot detect a frozen controller.
///
/// Dropping the balancer aborts listeners, probes, and active flows. Deregistration and
/// health changes do not affect flows that have already selected a target.
pub struct Balancer {
    serve: Listener,
    execute: Listener,
    workers: Listener,
    changes: Arc<Changes>,
}

impl Balancer {
    /// Binds the three listeners and starts their accept and health-check tasks.
    ///
    /// # Errors
    /// Returns [`E2eError::Io`] if a loopback listener cannot be bound or inspected.
    pub async fn start<C: Clock + 'static>(clock: Arc<C>, probe: Probe) -> Result<Self, E2eError> {
        let changes = Arc::new(Changes::default());
        Ok(Self {
            serve: Listener::start(
                Arc::clone(&clock),
                probe,
                Frontend::Serve,
                Arc::clone(&changes),
            )
            .await?,
            execute: Listener::start(
                Arc::clone(&clock),
                probe,
                Frontend::Execute,
                Arc::clone(&changes),
            )
            .await?,
            workers: Listener::start(clock, probe, Frontend::Workers, Arc::clone(&changes)).await?,
            changes,
        })
    }

    fn listener(&self, frontend: Frontend) -> &Listener {
        match frontend {
            Frontend::Serve => &self.serve,
            Frontend::Execute => &self.execute,
            Frontend::Workers => &self.workers,
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        *self.changes.revision.lock()
    }

    pub(crate) fn notify_change(&self) {
        self.changes.notify();
    }

    pub(crate) fn wait_change(&self, revision: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut current = self.changes.revision.lock();
        while *current == revision {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            self.changes.changed.wait_for(&mut current, remaining);
        }
        true
    }

    /// Returns the loopback address of a frontend.
    #[must_use]
    pub fn address(&self, frontend: Frontend) -> SocketAddr {
        self.listener(frontend).address
    }

    /// Registers a target as initially unhealthy; registering it again has no effect.
    pub fn register(&self, frontend: Frontend, target: Target) {
        self.listener(frontend)
            .targets
            .lock()
            .entries
            .entry(target.traffic)
            .or_insert_with(|| {
                Arc::new(Mutex::new(TargetHealth {
                    health: target.health,
                    healthy: false,
                    successes: 0,
                    failures: 0,
                    #[cfg(test)]
                    started: Arc::new(tokio::sync::Notify::new()),
                    #[cfg(test)]
                    finished: Arc::new(tokio::sync::Notify::new()),
                    #[cfg(test)]
                    completed: Arc::new(tokio::sync::Notify::new()),
                }))
            });
        self.changes.notify();
    }

    /// Removes a target from future health checks and connection selection.
    pub fn deregister(&self, frontend: Frontend, address: SocketAddr) {
        self.listener(frontend)
            .targets
            .lock()
            .entries
            .remove(&address);
        self.changes.notify();
    }

    /// Returns the healthy registered targets in ascending address order.
    #[must_use]
    pub fn healthy(&self, frontend: Frontend) -> Vec<SocketAddr> {
        self.listener(frontend)
            .targets
            .lock()
            .entries
            .iter()
            .filter(|(_, target)| target.lock().healthy)
            .map(|(address, _)| *address)
            .collect()
    }
}

impl Listener {
    async fn start<C: Clock + 'static>(
        clock: Arc<C>,
        probe: Probe,
        frontend: Frontend,
        changes: Arc<Changes>,
    ) -> Result<Self, E2eError> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(E2eError::io("binding balancer listener"))?;
        let address = listener
            .local_addr()
            .map_err(E2eError::io("reading balancer address"))?;
        let targets = Arc::new(Mutex::new(Targets::default()));
        let accept = tokio::spawn(accept_connections(listener, Arc::clone(&targets)));
        // Construct the first sleep before spawning so an immediate test-clock advance is observed.
        let sleep = clock.sleep(probe.interval);
        let health = tokio::spawn(check_health(
            clock,
            probe,
            frontend,
            Arc::clone(&targets),
            sleep,
            changes,
        ));
        Ok(Self {
            address,
            targets,
            accept,
            health,
        })
    }
}

async fn accept_connections(listener: TcpListener, targets: Arc<Mutex<Targets>>) {
    let mut flows = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((mut incoming, _)) = accepted else { break };
                let target = targets.lock().choose();
                if let Some(target) = target {
                    flows.spawn(async move {
                        if let Ok(mut outgoing) = TcpStream::connect(target).await {
                            let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                        }
                    });
                }
            }
            Some(_) = flows.join_next(), if !flows.is_empty() => {}
        }
    }
}

async fn check_health<C: Clock + 'static>(
    clock: Arc<C>,
    probe: Probe,
    frontend: Frontend,
    targets: Arc<Mutex<Targets>>,
    mut sleep: gang_clock::Sleep,
    changes: Arc<Changes>,
) {
    loop {
        sleep.await;
        // Start the next interval now, not after a slow probe finishes.
        sleep = clock.sleep(probe.interval);
        let snapshot: Vec<_> = targets
            .lock()
            .entries
            .values()
            .map(|target| (target.lock().health, Arc::clone(target)))
            .collect();
        let mut checks = JoinSet::new();
        for (address, target) in snapshot {
            let clock = Arc::clone(&clock);
            checks.spawn(async move {
                let timeout = clock.timeout(probe.timeout, async {
                    let Ok(stream) = TcpStream::connect(address).await else { return false };
                    let Ok((mut sender, connection)) = hyper::client::conn::http1::handshake::<_, Body>(TokioIo::new(stream)).await else { return false };
                    let mut connections = JoinSet::new();
                    connections.spawn(connection);
                    let Ok(request) = Request::get(frontend.path())
                        .header(hyper::header::HOST, address.to_string())
                        .body(Body::empty()) else { return false };
                    #[cfg(test)]
                    target.lock().started.notify_one();
                    matches!(sender.send_request(request).await, Ok(response) if response.status() == StatusCode::OK)
                });
                let success = timeout.await.unwrap_or(false);
                #[cfg(test)]
                target.lock().finished.notify_one();
                (target, success)
            });
        }
        let mut results = Vec::new();
        while let Some(result) = checks.join_next().await {
            if let Ok(result) = result {
                results.push(result);
            }
        }
        // Serialize publication with routing, so its candidate set cannot change mid-selection.
        let guard = targets.lock();
        let mut transitioned = false;
        for (target, success) in results {
            let mut target = target.lock();
            let before = target.healthy;
            if success {
                target.failures = 0;
                target.successes = target.successes.saturating_add(1);
                if target.successes >= probe.threshold.get() {
                    target.healthy = true;
                }
            } else {
                target.successes = 0;
                target.failures = target.failures.saturating_add(1);
                if target.failures >= probe.threshold.get() {
                    target.healthy = false;
                }
            }
            transitioned |= before != target.healthy;
            #[cfg(test)]
            target.completed.notify_one();
        }
        drop(guard);
        if transitioned {
            changes.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, extract::State, routing::get};
    use gang_clock::TestClock;
    use gang_testkit::TestResult;
    use std::sync::atomic::{AtomicU16, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    struct Echo {
        address: SocketAddr,
        task: JoinHandle<()>,
    }

    impl Echo {
        async fn start(id: u8) -> TestResult<Self> {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let address = listener.local_addr()?;
            let task = tokio::spawn(async move {
                let mut clients = JoinSet::new();
                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            let Ok((mut stream, _)) = result else { break };
                            clients.spawn(async move {
                                let mut byte = [0];
                                while stream.read_exact(&mut byte).await.is_ok() {
                                    if stream.write_all(&[id, byte[0]]).await.is_err() { break; }
                                }
                            });
                        }
                        Some(_) = clients.join_next(), if !clients.is_empty() => {}
                    }
                }
            });
            Ok(Self { address, task })
        }
    }

    impl Drop for Echo {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn exchange(stream: &mut TcpStream, byte: u8) -> TestResult<u8> {
        stream.write_all(&[byte]).await?;
        let mut response = [0; 2];
        stream.read_exact(&mut response).await?;
        assert_eq!(response[1], byte);
        Ok(response[0])
    }

    async fn request(balancer: &Balancer, frontend: Frontend) -> TestResult<u8> {
        let mut stream = TcpStream::connect(balancer.address(frontend)).await?;
        exchange(&mut stream, 42).await
    }

    struct HttpTarget {
        address: SocketAddr,
        status: Arc<AtomicU16>,
        task: JoinHandle<()>,
    }

    impl HttpTarget {
        async fn start(status: StatusCode) -> TestResult<Self> {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let address = listener.local_addr()?;
            let status = Arc::new(AtomicU16::new(status.as_u16()));
            let app = Router::new()
                .route(
                    gang_types::health::health_check().serve_path(),
                    get(Self::respond),
                )
                .route(
                    gang_types::health::health_check().leader_path(),
                    get(Self::respond),
                )
                .with_state(Arc::clone(&status));
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Ok(Self {
                address,
                status,
                task,
            })
        }

        fn respond(State(status): State<Arc<AtomicU16>>) -> impl Future<Output = StatusCode> {
            std::future::ready(
                StatusCode::from_u16(status.load(Ordering::SeqCst)).expect("test status is valid"),
            )
        }
    }

    impl Drop for HttpTarget {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn register(
        balancer: &Balancer,
        frontend: Frontend,
        traffic: SocketAddr,
        health: SocketAddr,
    ) -> (Arc<Notify>, Arc<Notify>, Arc<Notify>) {
        balancer.register(frontend, Target { traffic, health });
        let targets = balancer.listener(frontend).targets.lock();
        let target = targets
            .entries
            .get(&traffic)
            .expect("target was registered")
            .lock();
        (
            Arc::clone(&target.started),
            Arc::clone(&target.finished),
            Arc::clone(&target.completed),
        )
    }

    async fn round(
        clock: &TestClock,
        targets: &[(Arc<Notify>, Arc<Notify>, Arc<Notify>)],
        frozen: bool,
    ) {
        let probe = Probe::default();
        clock.advance(probe.interval);
        for (started, _, _) in targets {
            started.notified().await;
        }
        if frozen {
            for (_, finished, _) in targets.iter().skip(1) {
                finished.notified().await;
            }
            clock.advance(probe.timeout);
        }
        for (_, _, completed) in targets {
            completed.notified().await;
        }
    }

    #[test]
    fn frontend_paths_follow_deployment_health_check() {
        let health = gang_types::health::health_check();
        assert_eq!(Frontend::Serve.path(), health.serve_path());
        assert_eq!(Frontend::Execute.path(), health.leader_path());
        assert_eq!(Frontend::Workers.path(), health.leader_path());
    }

    #[tokio::test]
    async fn target_changes_wake_health_waiters() -> TestResult {
        let balancer = Balancer::start(Arc::new(TestClock::default()), Probe::default()).await?;
        let address = SocketAddr::from(([127, 0, 0, 1], 12345));
        let revision = balancer.revision();
        balancer.register(
            Frontend::Execute,
            Target {
                traffic: address,
                health: address,
            },
        );
        assert!(balancer.wait_change(revision, Duration::ZERO));
        Ok(())
    }

    #[tokio::test]
    async fn http_success_and_503_obey_threshold() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let good = HttpTarget::start(StatusCode::OK).await?;
        let bad = HttpTarget::start(StatusCode::SERVICE_UNAVAILABLE).await?;
        let targets = [
            register(&balancer, Frontend::Serve, good.address, good.address),
            register(&balancer, Frontend::Serve, bad.address, bad.address),
        ];
        let threshold = Probe::default().threshold.get();
        for count in 1..=threshold {
            round(&clock, &targets, false).await;
            assert_eq!(
                balancer.healthy(Frontend::Serve).contains(&good.address),
                count == threshold
            );
            assert!(!balancer.healthy(Frontend::Serve).contains(&bad.address));
        }
        good.status
            .store(StatusCode::SERVICE_UNAVAILABLE.as_u16(), Ordering::SeqCst);
        for count in 1..=threshold {
            round(&clock, &targets, false).await;
            assert_eq!(
                balancer.healthy(Frontend::Serve).contains(&good.address),
                count < threshold
            );
            assert!(!balancer.healthy(Frontend::Serve).contains(&bad.address));
        }
        Ok(())
    }

    #[tokio::test]
    async fn frozen_tcp_handshake_succeeds_but_http_probe_is_unhealthy() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let frozen = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = frozen.local_addr()?;
        let _handshake = TcpStream::connect(address).await?;
        let targets = [register(&balancer, Frontend::Serve, address, address)];
        for _ in 0..Probe::default().threshold.get() {
            round(&clock, &targets, true).await;
        }
        let targets_lock = balancer.listener(Frontend::Serve).targets.lock();
        let state = targets_lock
            .entries
            .get(&address)
            .expect("target exists")
            .lock();
        assert_eq!(state.failures, Probe::default().threshold.get());
        assert!(!state.healthy);
        Ok(())
    }

    #[tokio::test]
    async fn frozen_target_keeps_established_flow_but_new_flows_use_healthy_target() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let first = Echo::start(1).await?;
        let second = Echo::start(2).await?;
        let frozen = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let live = HttpTarget::start(StatusCode::OK).await?;
        let first_signal = register(
            &balancer,
            Frontend::Execute,
            first.address,
            frozen.local_addr()?,
        );
        let mut established = TcpStream::connect(balancer.address(Frontend::Execute)).await?;
        assert_eq!(exchange(&mut established, 1).await?, 1);
        let second_signal = register(&balancer, Frontend::Execute, second.address, live.address);
        let targets = [first_signal, second_signal];
        for _ in 0..Probe::default().threshold.get() {
            round(&clock, &targets, true).await;
        }
        assert_eq!(balancer.healthy(Frontend::Execute), vec![second.address]);
        for _ in 0..4 {
            assert_eq!(request(&balancer, Frontend::Execute).await?, 2);
        }
        assert_eq!(exchange(&mut established, 2).await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn deregistration_preserves_established_flow() -> TestResult {
        let balancer = Balancer::start(Arc::new(TestClock::default()), Probe::default()).await?;
        let echo = Echo::start(3).await?;
        balancer.register(
            Frontend::Workers,
            Target {
                traffic: echo.address,
                health: echo.address,
            },
        );
        let mut stream = TcpStream::connect(balancer.address(Frontend::Workers)).await?;
        assert_eq!(exchange(&mut stream, 1).await?, 3);
        balancer.deregister(Frontend::Workers, echo.address);
        assert_eq!(exchange(&mut stream, 2).await?, 3);
        let mut rejected = TcpStream::connect(balancer.address(Frontend::Workers)).await?;
        let mut byte = [0];
        assert_eq!(rejected.read(&mut byte).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn healthy_targets_round_robin() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let first = Echo::start(1).await?;
        let second = Echo::start(2).await?;
        let health = HttpTarget::start(StatusCode::OK).await?;
        let targets = [
            register(&balancer, Frontend::Serve, first.address, health.address),
            register(&balancer, Frontend::Serve, second.address, health.address),
        ];
        for _ in 0..Probe::default().threshold.get() {
            round(&clock, &targets, false).await;
        }
        let mut sorted = vec![first.address, second.address];
        sorted.sort_unstable();
        assert_eq!(balancer.healthy(Frontend::Serve), sorted);
        let a = request(&balancer, Frontend::Serve).await?;
        let b = request(&balancer, Frontend::Serve).await?;
        assert_ne!(a, b);
        assert_eq!(request(&balancer, Frontend::Serve).await?, a);
        assert_eq!(request(&balancer, Frontend::Serve).await?, b);
        Ok(())
    }
}
