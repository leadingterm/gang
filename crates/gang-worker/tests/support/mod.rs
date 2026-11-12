//! Fakes and fixtures shared by the `gang-worker` integration tests.
//!
//! Every fake keeps its state in an `Arc<Mutex<..>>`, so a test keeps a handle after it moves a
//! clone into the code under test.

use std::fs;
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use gang_clock::{Clock, TestClock, Tick, Timestamp};
use gang_config::{
    ExecutionConfig, FleetMember, FleetPolicy, Fleets, InstanceBounds, WorkerConfig,
};
use gang_spi::{InstanceIdentity, PortError};
use gang_types::{
    Bytes, CertificatePem, FleetName, InstanceId, Isa, NonZeroBytes, PrivateKeyPem, Proof,
    WorkerLifetime,
};
use gang_worker::{
    Credentials, EnrollError, Enroller, ServerCaError, ServerCaSource, Transport, WorkerDeployment,
    WorkerParts, WorkerSettings,
};

pub use gang_testkit::{Behaviour, FakeCache, FakeScheduler, RecordedSpec, RecordedTls};
use gang_testkit::{
    BoxedSection, CallCount, CallLog, FakeProvisioner, Opaque, RecordingLimits, SandboxConfig,
    SandboxKey, TestLimits, TestPlugins,
};

/// One gibibyte in bytes.
pub const GIB: u64 = 1024 * 1024 * 1024;

/// The capacity of the mounted scratch volume: 10 GiB.
const CAPACITY: u64 = 10 * GIB;

/// The sandbox file text the layout tests write.
pub const SANDBOX: &str = "[sandbox]\nmode = \"strict\"\n";

/// The fleets of the test composition, in preference order. `example-fleet` runs the `noop`
/// sandbox and `example-boxed` runs the `boxed` sandbox, so tests can tell them apart.
#[must_use]
pub fn fleets() -> Fleets<TestPlugins> {
    Fleets::try_new(vec![
        fleet_member("example-fleet", SandboxKey::Noop, 4, 8),
        fleet_member("example-boxed", SandboxKey::Boxed, 2, 4),
    ])
    .expect("the test fleets are within the fleet count limit")
}

/// One x86-64 fleet member of `vcpus` vCPUs and `memory_gib` GiB per instance.
fn fleet_member(
    name: &str,
    sandbox: SandboxKey,
    vcpus: u32,
    memory_gib: u64,
) -> FleetMember<TestPlugins> {
    let policy = FleetPolicy {
        name: FleetName::try_new(name).expect("test fleet name is valid"),
        isa: Isa::X86_64,
        instances: InstanceBounds::up_to(NonZeroU32::MIN.saturating_add(3)),
        vcpus_per_instance: NonZeroU32::new(vcpus).expect("test fleets have vCPUs"),
        memory_per_instance: NonZeroBytes::gib(memory_gib),
        disk_per_instance: NonZeroBytes::gib(5),
        target_actions_per_instance: NonZeroU32::new(4).expect("four actions is non-zero"),
        idle_timeout: Duration::from_secs(15),
        max_drain_wait: Duration::from_mins(1),
        boot_grace: Duration::from_secs(30),
    };
    FleetMember {
        policy,
        sandbox,
        options: Opaque::default(),
    }
}

/// Credentials whose PEM blocks carry `body`, so two sets can be told apart on disk.
#[must_use]
pub fn credentials(body: &str) -> Credentials {
    Credentials {
        certificate: CertificatePem::try_new(format!(
            "-----BEGIN CERTIFICATE-----\n{body}\n-----END CERTIFICATE-----"
        ))
        .expect("certificate block is well formed"),
        private_key: PrivateKeyPem::try_new(format!(
            "-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----"
        ))
        .expect("private key block is well formed"),
        ca_certificate: CertificatePem::try_new(
            "-----BEGIN CERTIFICATE-----\nROOT\n-----END CERTIFICATE-----",
        )
        .expect("CA block is well formed"),
        not_after: Timestamp::from_unix_millis(100_000),
    }
}

/// A scripted run of failures: the next calls fail one by one, or every call fails.
#[derive(Default)]
struct Script {
    failures_left: usize,
    forever: bool,
}

impl Script {
    /// Consumes one scripted failure and reports whether this call fails.
    fn next_fails(&mut self) -> bool {
        if self.forever {
            return true;
        }
        if self.failures_left == 0 {
            return false;
        }
        self.failures_left -= 1;
        true
    }
}

struct EnrollerState {
    proofs: Vec<String>,
    failures: Script,
    rate_limits: Script,
    rate_limit_retry_after: Option<Duration>,
    not_after: Option<Timestamp>,
}

/// Enroller fake: returns credentials that name their enrollment, and records every proof it is
/// given.
#[derive(Clone)]
pub struct FakeEnroller {
    state: Arc<Mutex<EnrollerState>>,
    enrollments: CallCount,
}

impl FakeEnroller {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(EnrollerState {
                proofs: Vec::new(),
                rate_limits: Script::default(),
                rate_limit_retry_after: None,
                failures: Script::default(),
                not_after: None,
            })),
            enrollments: CallCount::new(),
        }
    }

    /// The credentials the `call`-th successful enrollment returns, counting from 1. Each call
    /// gets its own set, so two generations can be told apart on disk.
    #[must_use]
    pub fn issued_on_call(call: usize) -> Credentials {
        credentials(&format!("ISSUED-{call}"))
    }

    /// The credentials the first enrollment returns.
    #[must_use]
    pub fn issued() -> Credentials {
        Self::issued_on_call(1)
    }

    /// Makes the next `count` enrollments fail; later enrollments succeed.
    pub fn fail_next(&self, count: usize) {
        self.state.lock().failures.failures_left = count;
    }

    /// Makes every enrollment fail.
    pub fn fail_forever(&self) {
        self.state.lock().failures.forever = true;
    }

    /// Makes the next `count` enrollments refuse with a rate limit carrying `retry_after`.
    pub fn rate_limit_next(&self, count: usize, retry_after: Option<Duration>) {
        let mut state = self.state.lock();
        state.rate_limits.failures_left = count;
        state.rate_limit_retry_after = retry_after;
    }

    /// Makes every enrollment refuse with a rate limit carrying `retry_after`.
    pub fn rate_limit_forever(&self, retry_after: Option<Duration>) {
        let mut state = self.state.lock();
        state.rate_limits.forever = true;
        state.rate_limit_retry_after = retry_after;
    }

    /// Sets the expiration returned on subsequent successful enrollments.
    pub fn set_not_after(&self, not_after: Timestamp) {
        self.state.lock().not_after = Some(not_after);
    }

    /// How many times `enroll` was called.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.enrollments.get()
    }

    /// The enrollment attempts made so far, counted before each one is answered.
    #[must_use]
    pub fn enrollments(&self) -> CallCount {
        self.enrollments.clone()
    }

    /// The proofs `enroll` was given, in call order.
    #[must_use]
    pub fn proofs(&self) -> Vec<String> {
        self.state.lock().proofs.clone()
    }
}

impl Enroller for FakeEnroller {
    async fn enroll(&self, proof: Proof) -> Result<Credentials, EnrollError> {
        self.enrollments.bump();
        let call = self.enrollments.get();
        let mut state = self.state.lock();
        state.proofs.push(proof.expose().to_owned());
        if state.rate_limits.next_fails() {
            return Err(EnrollError::rate_limited(
                io::Error::other("rate limited"),
                state.rate_limit_retry_after,
            ));
        }
        if state.failures.next_fails() {
            return Err(EnrollError::new(io::Error::other(
                "authority refused the proof",
            )));
        }
        let mut credentials = Self::issued_on_call(call);
        if let Some(not_after) = state.not_after {
            credentials.not_after = not_after;
        }
        Ok(credentials)
    }
}

/// What the server CA fake answers.
#[derive(Clone)]
pub enum ServerCaAnswer {
    /// The CA certificate, or `None` for the system roots.
    Ca(Option<CertificatePem>),
    /// [`ServerCaError::Missing`].
    Missing,
}

/// Server CA source fake: counts its calls, answers as set, and can hold calls forever.
#[derive(Clone)]
pub struct FakeServerCa {
    answer: Arc<Mutex<ServerCaAnswer>>,
    held: Arc<AtomicBool>,
    calls: CallCount,
}

impl FakeServerCa {
    /// Creates an unheld source that returns system roots.
    fn new() -> Self {
        Self {
            answer: Arc::new(Mutex::new(ServerCaAnswer::Ca(None))),
            held: Arc::new(AtomicBool::new(false)),
            calls: CallCount::new(),
        }
    }

    /// Sets the answer returned by subsequent calls.
    pub fn set_answer(&self, answer: ServerCaAnswer) {
        *self.answer.lock() = answer;
    }

    /// Holds every later call forever, after counting it.
    pub fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// The number of calls made so far.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.get()
    }

    /// A handle for waiting until a number of calls have been made.
    #[must_use]
    pub fn call_count(&self) -> CallCount {
        self.calls.clone()
    }
}

impl ServerCaSource for FakeServerCa {
    async fn server_ca(&self) -> Result<Option<CertificatePem>, ServerCaError> {
        self.calls.bump();
        if self.held.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        match self.answer.lock().clone() {
            ServerCaAnswer::Ca(ca) => Ok(ca),
            ServerCaAnswer::Missing => Err(ServerCaError::Missing),
        }
    }
}

/// The deployment the tests run the worker on.
pub struct FakeDeployment;

impl WorkerDeployment for FakeDeployment {
    type Provisioner = FakeProvisioner;
    type Cache = FakeCache;
    type Scheduler = FakeScheduler;
    type Limits = RecordingLimits;
    type Clock = Arc<TestClock>;
    type Enroller = FakeEnroller;
    type ServerCa = FakeServerCa;
}

/// The virtual time [`Harness::run_until`] and [`Harness::drive`] advance the clock by per step.
pub const STEP: Duration = Duration::from_secs(1);

/// A future the harness tracks. Its flag is `true` while the task is idle: polled to `Pending`
/// with no wake since. It is `false` before the first poll and after any wake.
struct Tracked<F> {
    /// Whether the task is idle.
    idle: Arc<AtomicBool>,
    /// The wrapped future.
    inner: Pin<Box<F>>,
}

impl<F> Tracked<F> {
    /// Wraps `inner`, which is not idle until it is first polled.
    fn new(inner: F) -> Self {
        Self {
            idle: Arc::new(AtomicBool::new(false)),
            inner: Box::pin(inner),
        }
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        // Idle before the poll, so a wake during or after it leaves the flag `false`.
        this.idle.store(true, Ordering::SeqCst);
        let waker = Waker::from(Arc::new(IdleWaker {
            idle: Arc::clone(&this.idle),
            inner: cx.waker().clone(),
        }));
        this.inner.as_mut().poll(&mut Context::from_waker(&waker))
    }
}

impl<F> Drop for Tracked<F> {
    fn drop(&mut self) {
        // A dropped task is never polled again, so it must not hold settling back.
        self.idle.store(true, Ordering::SeqCst);
    }
}

/// The waker a [`Tracked`] task is polled with: a wake clears the task's idle flag, then wakes
/// the waker the task was polled with.
struct IdleWaker {
    /// The idle flag of the task.
    idle: Arc<AtomicBool>,
    /// The waker the task was polled with.
    inner: Waker,
}

impl Wake for IdleWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.idle.store(false, Ordering::SeqCst);
        self.inner.wake_by_ref();
    }
}

/// Owns a temporary root holding a scratch volume and a fake `gang` executable, and the fakes
/// wired to them.
pub struct Harness {
    _root: tempfile::TempDir,
    /// The mounted scratch volume.
    pub volume: PathBuf,
    /// The fake `gang` executable the entrypoint links to.
    pub executable: PathBuf,
    /// The directory holding the fake sandbox program, which is the worker's `PATH`.
    pub bin: PathBuf,
    /// The provisioner fake.
    pub provisioner: FakeProvisioner,
    /// The cache fake.
    pub cache: FakeCache,
    /// The scheduler fake.
    pub scheduler: FakeScheduler,
    /// The action limits double.
    pub limits: RecordingLimits,
    /// The enroller fake.
    pub enroller: FakeEnroller,
    /// The server CA source fake.
    pub server_ca: FakeServerCa,
    /// The clock the worker reads, advanced by the test.
    pub clock: Arc<TestClock>,
    /// The idle flags of the tasks spawned through [`Harness::spawn`].
    idle: Mutex<Vec<Arc<AtomicBool>>>,
}

impl Harness {
    /// A harness whose instance is `i-0abc123`, of fleet `example-fleet` running instruction set
    /// `isa`, with workers of `lifetime`. The scheduler's worker is pending until a test changes it.
    #[must_use]
    pub fn new(isa: Isa, lifetime: WorkerLifetime) -> Self {
        let root = tempfile::tempdir().expect("a temporary directory can be created");
        let volume = root.path().join("scratch");
        let executable = root.path().join("gang");
        fs::create_dir(&volume).expect("scratch volume can be created");
        fs::write(&executable, b"").expect("executable can be created");
        let bin = root.path().join("bin");
        fs::create_dir(&bin).expect("bin directory can be created");
        let sandbox = bin.join("fake-sandbox");
        fs::write(&sandbox, b"").expect("sandbox program can be created");
        fs::set_permissions(&sandbox, fs::Permissions::from_mode(0o755))
            .expect("sandbox program can be made executable");
        let scheduler = FakeScheduler::new().expect("the scheduler endpoint is a valid URL");
        let limits = RecordingLimits::new(&CallLog::default()).observing_prepare({
            let starts = scheduler.worker_starts();
            move || starts.get()
        });
        let identity = InstanceIdentity {
            instance: InstanceId::try_new("i-0abc123").expect("instance id is valid"),
            fleet: FleetName::try_new("example-fleet").expect("test fleet name is valid"),
            isa,
        };
        Self {
            _root: root,
            provisioner: FakeProvisioner::new(
                identity,
                lifetime,
                volume.clone(),
                Bytes::new(CAPACITY),
            ),
            volume,
            executable,
            bin,
            cache: FakeCache,
            scheduler,
            limits,
            enroller: FakeEnroller::new(),
            server_ca: FakeServerCa::new(),
            clock: Arc::new(TestClock::default()),
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Spawns `future` on the runtime and tracks it: the harness moves the clock only once every
    /// tracked task is idle. Spawn every task the test expects to wait on the clock or on a fake
    /// through this method.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let tracked = Tracked::new(future);
        self.idle.lock().push(Arc::clone(&tracked.idle));
        tokio::spawn(tracked)
    }

    /// Whether every tracked task is idle.
    fn all_idle(&self) -> bool {
        self.idle
            .lock()
            .iter()
            .all(|flag| flag.load(Ordering::SeqCst))
    }

    /// Lets every tracked task run until it waits on the clock or on a fake: returns once all of
    /// them are idle, yielding to the runtime while any of them is still runnable.
    pub async fn settle(&self) {
        while !self.all_idle() {
            tokio::task::yield_now().await;
        }
    }

    /// The ports of a worker; the enroller is included only when `tls` is set.
    #[must_use]
    pub fn parts(&self, tls: bool) -> WorkerParts<FakeDeployment> {
        self.scheduler
            .set_worker_scheme(if tls { "grpcs" } else { "grpc" })
            .expect("the test scheduler scheme is valid");
        WorkerParts {
            provisioner: self.provisioner.clone(),
            cache: self.cache,
            scheduler: self.scheduler.clone(),
            clock: self.clock.clone(),
            transport: if tls {
                Transport::Mutual(self.enroller.clone())
            } else {
                Transport::Plain
            },
        }
    }

    /// The ports of a server-only TLS worker, whose CA comes from [`Harness::server_ca`].
    #[must_use]
    pub fn server_only_parts(&self) -> WorkerParts<FakeDeployment> {
        let mut parts = self.parts(true);
        parts.transport = Transport::ServerOnly(self.server_ca.clone());
        parts
    }

    /// The settings of a worker with the default `[worker]` section and the test fleets.
    #[must_use]
    pub fn settings(&self) -> WorkerSettings<TestPlugins> {
        WorkerSettings {
            fleets: fleets(),
            worker: WorkerConfig::default(),
            sandbox: SandboxConfig {
                noop: Some(Opaque::default()),
                boxed: Some(BoxedSection {
                    program: PathBuf::from("fake-sandbox"),
                }),
                ..SandboxConfig::default()
            },
            execution: ExecutionConfig::default(),
            limits: Some(TestLimits { max_pids: 512 }),
            search_path: self.bin.clone().into_os_string(),
            executable: self.executable.clone(),
        }
    }

    /// The limits builder the worker is run with: it hands out a clone of the harness's limits
    /// handle, which shares the calls it records.
    pub fn build_limits(
        &self,
    ) -> impl FnOnce(TestLimits) -> Result<RecordingLimits, PortError> + use<> {
        let limits = self.limits.clone();
        move |_| Ok(limits)
    }

    /// Advances the clock by `by`, then lets every tracked task run until it is idle.
    pub async fn advance(&self, by: Duration) {
        self.clock.advance(by);
        self.settle().await;
    }

    /// The virtual time elapsed since the harness was created.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.clock
            .monotonic()
            .saturating_duration_since(Tick::ORIGIN)
    }

    /// Lets every tracked task run until it is idle, then advances the clock one [`STEP`] at a
    /// time until `done` holds, and returns the virtual time at which it first held. The initial
    /// settle lets tasks spawned just before this call reach their first wait at virtual time
    /// zero, so the first step does not shift the deadlines they create.
    pub async fn run_until(&self, mut done: impl FnMut() -> bool) -> Duration {
        self.settle().await;
        while !done() {
            self.advance(STEP).await;
        }
        self.elapsed()
    }

    /// Polls `future`, advancing the clock one [`STEP`] whenever it is parked and every tracked
    /// task is idle, and returns its output with the virtual time at which it completed. A wake
    /// of `future` since its last poll means it has more to do, so the harness yields instead.
    pub async fn drive<F: Future>(&self, future: F) -> (F::Output, Duration) {
        let mut tracked = Tracked::new(future);
        loop {
            let polled =
                std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut tracked).poll(cx))).await;
            match polled {
                Poll::Ready(output) => return (output, self.elapsed()),
                Poll::Pending if tracked.idle.load(Ordering::SeqCst) && self.all_idle() => {
                    self.clock.advance(STEP);
                }
                Poll::Pending => tokio::task::yield_now().await,
            }
        }
    }
}
