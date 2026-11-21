//! Loopback NLB model: HTTP health checks on the health port, fail-open routing, and
//! connection termination off. Optional PROXY v2 framing follows each selected flow's
//! policy snapshot, including health probes. Frozen processes still complete TCP
//! handshakes, so an application response is needed to establish liveness.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use gang_clock::Clock;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::{Condvar, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::errors::E2eError;

/// The independent listeners of the controller load balancer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Frontend {
    /// Client cache, execution, API and dashboard traffic, served by the leader.
    Serve,
    /// Worker traffic, served by the leader.
    Workers,
}

impl Frontend {
    /// The controller health path checked for this frontend.
    #[must_use]
    pub fn path(self) -> &'static str {
        gang_types::health::health_check().ready_path()
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

#[derive(Clone)]
pub(crate) struct Policy {
    pub(crate) path: String,
    pub(crate) interval: Duration,
    pub(crate) timeout: Duration,
    pub(crate) healthy_threshold: u32,
    pub(crate) unhealthy_threshold: u32,
    pub(crate) proxy_protocol_v2: bool,
}

struct PolicyState {
    policy: Policy,
    generation: u64,
}

impl Policy {
    fn initial(frontend: Frontend, probe: Probe) -> Self {
        Self {
            path: frontend.path().to_owned(),
            interval: probe.interval,
            timeout: probe.timeout,
            healthy_threshold: probe.threshold.get(),
            unhealthy_threshold: probe.threshold.get(),
            proxy_protocol_v2: false,
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

struct Targets {
    entries: BTreeMap<SocketAddr, Arc<Mutex<TargetHealth>>>,
    next: usize,
    policy: PolicyState,
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
    fn new(policy: Policy) -> Self {
        Self {
            entries: BTreeMap::new(),
            next: 0,
            policy: PolicyState {
                policy,
                generation: 0,
            },
        }
    }

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

pub(crate) struct Listener {
    address: SocketAddr,
    targets: Arc<Mutex<Targets>>,
    accept: JoinHandle<()>,
    health: JoinHandle<()>,
    policy_changed: Arc<tokio::sync::Notify>,
    changes: Arc<Changes>,
    #[cfg(test)]
    policy_applied: Arc<tokio::sync::Notify>,
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
    workers: Listener,
    changes: Arc<Changes>,
}

impl Balancer {
    /// Binds the client and worker listeners and starts their accept and health-check tasks.
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
            workers: Listener::start(clock, probe, Frontend::Workers, Arc::clone(&changes)).await?,
            changes,
        })
    }

    fn listener(&self, frontend: Frontend) -> &Listener {
        match frontend {
            Frontend::Serve => &self.serve,
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
        self.listener(frontend).register(target);
    }

    /// Removes a target from future health checks and connection selection.
    pub fn deregister(&self, frontend: Frontend, address: SocketAddr) {
        self.listener(frontend).deregister(address);
    }

    /// Returns the healthy registered targets in ascending address order.
    #[must_use]
    pub fn healthy(&self, frontend: Frontend) -> Vec<SocketAddr> {
        self.listener(frontend).healthy()
    }

    pub(crate) fn set_policy(&self, frontend: Frontend, policy: Policy) {
        self.listener(frontend).set_policy(policy);
    }
}

impl Listener {
    pub(crate) fn register(&self, target: Target) {
        self.targets
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

    pub(crate) fn deregister(&self, address: SocketAddr) {
        self.targets.lock().entries.remove(&address);
        self.changes.notify();
    }

    pub(crate) fn healthy(&self) -> Vec<SocketAddr> {
        self.targets
            .lock()
            .entries
            .iter()
            .filter(|(_, target)| target.lock().healthy)
            .map(|(address, _)| *address)
            .collect()
    }

    pub(crate) fn set_policy(&self, policy: Policy) {
        let mut targets = self.targets.lock();
        let generation = targets.policy.generation.wrapping_add(1);
        for target in targets.entries.values() {
            let mut target = target.lock();
            target.successes = 0;
            target.failures = 0;
        }
        targets.policy = PolicyState { policy, generation };
        drop(targets);
        self.policy_changed.notify_one();
        self.changes.notify();
    }

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
        let targets = Arc::new(Mutex::new(Targets::new(Policy::initial(frontend, probe))));
        let policy_changed = Arc::new(tokio::sync::Notify::new());
        #[cfg(test)]
        let policy_applied = Arc::new(tokio::sync::Notify::new());
        let accept = tokio::spawn(accept_connections(listener, Arc::clone(&targets)));
        // Construct the first sleep before spawning so an immediate test-clock advance is observed.
        let sleep = clock.sleep(probe.interval);
        let health = tokio::spawn(check_health(
            clock,
            Arc::clone(&policy_changed),
            #[cfg(test)]
            Arc::clone(&policy_applied),
            Arc::clone(&targets),
            sleep,
            Arc::clone(&changes),
        ));
        Ok(Self {
            address,
            targets,
            accept,
            health,
            policy_changed,
            changes,
            #[cfg(test)]
            policy_applied,
        })
    }
}

const PROXY_V2_HEALTH_HEADER: &[u8; 16] = b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0";

pub(crate) fn proxy_v2_tcp_header(source: SocketAddrV4, destination: SocketAddrV4) -> [u8; 28] {
    let mut header = [0; 28];
    header[..16].copy_from_slice(b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c");
    header[16..20].copy_from_slice(&source.ip().octets());
    header[20..24].copy_from_slice(&destination.ip().octets());
    header[24..26].copy_from_slice(&source.port().to_be_bytes());
    header[26..28].copy_from_slice(&destination.port().to_be_bytes());
    header
}

async fn accept_connections(listener: TcpListener, targets: Arc<Mutex<Targets>>) {
    let mut flows = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((mut incoming, source)) = accepted else { break };
                let selected = {
                    let mut guard = targets.lock();
                    guard.choose().map(|target| (target, guard.policy.policy.proxy_protocol_v2))
                };
                if let Some((target, proxy_protocol_v2)) = selected {
                    flows.spawn(async move {
                        if let Ok(mut outgoing) = TcpStream::connect(target).await {
                            if proxy_protocol_v2 {
                                let Ok(destination) = incoming.local_addr() else { return };
                                // Every frontend binds IPv4 loopback; never substitute backend addresses.
                                let (SocketAddr::V4(source), SocketAddr::V4(destination)) = (source, destination) else { return };
                                if outgoing.write_all(&proxy_v2_tcp_header(source, destination)).await.is_err() {
                                    return;
                                }
                            }
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
    policy_changed: Arc<tokio::sync::Notify>,
    #[cfg(test)] policy_applied: Arc<tokio::sync::Notify>,
    targets: Arc<Mutex<Targets>>,
    mut sleep: gang_clock::Sleep,
    changes: Arc<Changes>,
) {
    loop {
        tokio::select! {
            () = sleep => {}
            () = policy_changed.notified() => {
                let interval = targets.lock().policy.policy.interval;
                sleep = clock.sleep(interval);
                #[cfg(test)]
                policy_applied.notify_one();
                continue;
            }
        }
        let (policy, generation, snapshot) = {
            let guard = targets.lock();
            let state = &guard.policy;
            let snapshot: Vec<_> = guard
                .entries
                .values()
                .map(|target| (target.lock().health, Arc::clone(target)))
                .collect();
            (state.policy.clone(), state.generation, snapshot)
        };
        sleep = clock.sleep(policy.interval);
        let mut checks = JoinSet::new();
        for (address, target) in snapshot {
            let clock = Arc::clone(&clock);
            let policy = policy.clone();
            checks.spawn(async move {
                let timeout = clock.timeout(policy.timeout, async {
                    let Ok(mut stream) = TcpStream::connect(address).await else { return false };
                    if policy.proxy_protocol_v2 && stream.write_all(PROXY_V2_HEALTH_HEADER).await.is_err() {
                        return false;
                    }
                    let Ok((mut sender, connection)) = hyper::client::conn::http1::handshake::<_, Body>(TokioIo::new(stream)).await else { return false };
                    let mut connections = JoinSet::new();
                    connections.spawn(connection);
                    let Ok(request) = Request::get(policy.path.as_str())
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
        if guard.policy.generation != generation {
            continue;
        }
        let mut transitioned = false;
        for (target, success) in results {
            let mut target = target.lock();
            let before = target.healthy;
            if success {
                target.failures = 0;
                target.successes = target.successes.saturating_add(1);
                if target.successes >= policy.healthy_threshold {
                    target.healthy = true;
                }
            } else {
                target.successes = 0;
                target.failures = target.failures.saturating_add(1);
                if target.failures >= policy.unhealthy_threshold {
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
                    gang_types::health::health_check().ready_path(),
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
    fn proxy_v2_tcp_frame_encodes_original_addresses_and_network_order_ports() {
        let source = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 17), 0x1234);
        let destination = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 29), 0xabcd);
        assert_eq!(
            &proxy_v2_tcp_header(source, destination),
            b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c\xc0\0\x02\x11\xc6\x33\x64\x1d\x12\x34\xab\xcd"
        );
    }

    async fn expect_bytes(stream: &mut TcpStream, expected: &[u8]) -> TestResult {
        let mut actual = vec![0; expected.len()];
        stream.read_exact(&mut actual).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn proxy_policy_changes_only_new_flows_and_preserves_deregistered_flows() -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            let balancer =
                Balancer::start(Arc::new(TestClock::default()), Probe::default()).await?;
            let backend = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let address = backend.local_addr()?;
            let frontend = balancer.address(Frontend::Workers);
            balancer.register(
                Frontend::Workers,
                Target {
                    traffic: address,
                    health: address,
                },
            );
            let mut direct = TcpStream::connect(frontend).await?;
            let (mut direct_backend, _) = backend.accept().await?;
            direct.write_all(b"direct").await?;
            expect_bytes(&mut direct_backend, b"direct").await?;

            let mut policy = Policy::initial(Frontend::Workers, Probe::default());
            policy.proxy_protocol_v2 = true;
            balancer.set_policy(Frontend::Workers, policy.clone());
            let mut framed = TcpStream::connect(frontend).await?;
            let (mut framed_backend, _) = backend.accept().await?;
            framed.write_all(b"framed").await?;
            let mut header = [0; 28];
            framed_backend.read_exact(&mut header).await?;
            assert_eq!(&header[..16], b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c");
            assert_eq!(&header[16..20], &[127, 0, 0, 1]);
            assert_eq!(&header[20..24], &[127, 0, 0, 1]);
            assert_eq!(&header[24..26], &framed.local_addr()?.port().to_be_bytes());
            assert_eq!(&header[26..28], &frontend.port().to_be_bytes());
            expect_bytes(&mut framed_backend, b"framed").await?;
            framed_backend.write_all(b"reply").await?;
            expect_bytes(&mut framed, b"reply").await?;
            direct.write_all(b"still-direct").await?;
            expect_bytes(&mut direct_backend, b"still-direct").await?;

            policy.proxy_protocol_v2 = false;
            balancer.set_policy(Frontend::Workers, policy);
            let mut replacement = TcpStream::connect(frontend).await?;
            let (mut replacement_backend, _) = backend.accept().await?;
            replacement.write_all(b"raw-again").await?;
            expect_bytes(&mut replacement_backend, b"raw-again").await?;
            framed.write_all(b"still-framed").await?;
            expect_bytes(&mut framed_backend, b"still-framed").await?;

            balancer.deregister(Frontend::Workers, address);
            direct.write_all(b"draining-direct").await?;
            expect_bytes(&mut direct_backend, b"draining-direct").await?;
            framed.write_all(b"draining-framed").await?;
            expect_bytes(&mut framed_backend, b"draining-framed").await?;
            drop(balancer);
            let mut byte = [0];
            assert_eq!(direct.read(&mut byte).await?, 0);
            assert_eq!(framed.read(&mut byte).await?, 0);
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn health_policy_frames_http_only_when_enabled() -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            for proxy_protocol_v2 in [false, true] {
                let clock = Arc::new(TestClock::default());
                let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
                let health = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
                let address = health.local_addr()?;
                let signal = register(&balancer, Frontend::Serve, address, address);
                let mut policy = Policy::initial(Frontend::Serve, Probe::default());
                policy.path = "/fixture-ready".into();
                policy.healthy_threshold = 1;
                policy.proxy_protocol_v2 = proxy_protocol_v2;
                balancer.set_policy(Frontend::Serve, policy);
                balancer
                    .listener(Frontend::Serve)
                    .policy_applied
                    .notified()
                    .await;
                clock.advance(Probe::default().interval);
                let (mut stream, _) = health.accept().await?;
                if proxy_protocol_v2 {
                    // The health parser accepts LOCAL v2, with no claimed client address.
                    expect_bytes(&mut stream, b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0").await?;
                }
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await?;
                    request.push(byte[0]);
                }
                let request = std::str::from_utf8(&request)?;
                assert!(request.starts_with("GET /fixture-ready HTTP/1.1\r\n"));
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains(&format!("\r\nhost: {address}\r\n"))
                );
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await?;
                signal.2.notified().await;
                assert_eq!(balancer.healthy(Frontend::Serve), vec![address]);
            }
            Ok(())
        })
        .await?
    }

    #[test]
    fn frontend_paths_follow_deployment_health_check() {
        let health = gang_types::health::health_check();
        assert_eq!(Frontend::Serve.path(), health.ready_path());
        assert_eq!(Frontend::Workers.path(), health.ready_path());
    }

    #[tokio::test]
    async fn target_changes_wake_health_waiters() -> TestResult {
        let balancer = Balancer::start(Arc::new(TestClock::default()), Probe::default()).await?;
        let address = SocketAddr::from(([127, 0, 0, 1], 12345));
        let revision = balancer.revision();
        balancer.register(
            Frontend::Serve,
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
            Frontend::Serve,
            first.address,
            frozen.local_addr()?,
        );
        let mut established = TcpStream::connect(balancer.address(Frontend::Serve)).await?;
        assert_eq!(exchange(&mut established, 1).await?, 1);
        let second_signal = register(&balancer, Frontend::Serve, second.address, live.address);
        let targets = [first_signal, second_signal];
        for _ in 0..Probe::default().threshold.get() {
            round(&clock, &targets, true).await;
        }
        assert_eq!(balancer.healthy(Frontend::Serve), vec![second.address]);
        for _ in 0..4 {
            assert_eq!(request(&balancer, Frontend::Serve).await?, 2);
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
    async fn controller_deregistration_preserves_all_frontend_flows() -> TestResult {
        let balancer = Balancer::start(Arc::new(TestClock::default()), Probe::default()).await?;
        let old = Echo::start(1).await?;
        let new = Echo::start(2).await?;
        let frontends = [Frontend::Serve, Frontend::Workers];
        let mut flows = Vec::new();
        for frontend in frontends {
            balancer.register(
                frontend,
                Target {
                    traffic: old.address,
                    health: old.address,
                },
            );
            let mut flow = TcpStream::connect(balancer.address(frontend)).await?;
            assert_eq!(exchange(&mut flow, 1).await?, 1);
            flows.push(flow);
            balancer.register(
                frontend,
                Target {
                    traffic: new.address,
                    health: new.address,
                },
            );
        }
        for frontend in frontends {
            balancer.deregister(frontend, old.address);
        }
        for (frontend, mut flow) in frontends.into_iter().zip(flows) {
            assert_eq!(exchange(&mut flow, 2).await?, 1);
            assert_eq!(request(&balancer, frontend).await?, 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn policy_change_applies_to_all_targets_without_closing_flows() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let old = Echo::start(1).await?;
        let new = Echo::start(2).await?;
        let health = HttpTarget::start(StatusCode::OK).await?;
        let old_signal = register(&balancer, Frontend::Serve, old.address, health.address);
        let mut flow = TcpStream::connect(balancer.address(Frontend::Serve)).await?;
        assert_eq!(exchange(&mut flow, 1).await?, 1);
        for _ in 0..Probe::default().threshold.get() {
            round(&clock, std::slice::from_ref(&old_signal), false).await;
        }
        let new_signal = register(&balancer, Frontend::Serve, new.address, health.address);
        balancer.set_policy(
            Frontend::Serve,
            Policy {
                path: "/not-present".into(),
                interval: Probe::default().interval,
                timeout: Probe::default().timeout,
                healthy_threshold: 1,
                unhealthy_threshold: 2,
                proxy_protocol_v2: false,
            },
        );
        balancer
            .listener(Frontend::Serve)
            .policy_applied
            .notified()
            .await;
        assert_eq!(balancer.healthy(Frontend::Serve), vec![old.address]);
        let signals = [old_signal, new_signal];
        round(&clock, &signals, false).await;
        assert_eq!(balancer.healthy(Frontend::Serve), vec![old.address]);
        round(&clock, &signals, false).await;
        assert!(balancer.healthy(Frontend::Serve).is_empty());
        assert_eq!(exchange(&mut flow, 2).await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn stale_probe_does_not_apply_after_policy_change() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let balancer = Balancer::start(Arc::clone(&clock), Probe::default()).await?;
        let frozen = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = frozen.local_addr()?;
        let signal = register(&balancer, Frontend::Serve, address, address);
        clock.advance(Probe::default().interval);
        signal.0.notified().await;
        balancer.set_policy(
            Frontend::Serve,
            Policy {
                path: "/new".into(),
                interval: Probe::default().interval,
                timeout: Probe::default().timeout,
                healthy_threshold: 1,
                unhealthy_threshold: 1,
                proxy_protocol_v2: false,
            },
        );
        clock.advance(Probe::default().timeout);
        signal.1.notified().await;
        balancer
            .listener(Frontend::Serve)
            .policy_applied
            .notified()
            .await;
        let targets = balancer.listener(Frontend::Serve).targets.lock();
        let target = targets
            .entries
            .get(&address)
            .expect("registered target")
            .lock();
        assert_eq!(target.failures, 0);
        assert!(!target.healthy);
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
