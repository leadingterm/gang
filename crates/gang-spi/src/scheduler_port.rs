//! Scheduler: the embedded remote execution scheduler, its workers, and what it holds.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ::bytes::Bytes as BodyBytes;
use futures::stream::BoxStream;
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

use gang_clock::{Clock, Tick, Timestamp};
use gang_types::{
    ActionEnvironment, ActionResources, BuildId, Bytes, FleetCatalog, FleetName, FleetProperties,
    FleetSet, InstanceId, NonZeroBytes, NonZeroDuration, Refusal, SourceId, Url, WorkerId,
    WorkerLifetime,
};

use crate::{
    ActionMemoryPool, Authenticated, BoxedError, CompletionRates, FleetSupply, ObservationSink,
    Peer, PortError, RecordKey, RenderedStoreSpec, port,
};

/// The remote execution scheduler Gang scales workers for. The scheduler runs inside the `gang`
/// process: `gang server` runs the controller and `gang worker` runs a worker, each through the
/// future a method returns.
#[port]
pub trait SchedulerPort: Send + Sync {
    /// Upper bound on the encoded body of one learned history from this scheduler.
    /// Available from controller readiness through drain.
    ///
    /// # Errors
    /// Returns a port error if the scheduler cannot determine the bound.
    fn learned_history_byte_limit(&self) -> Result<std::num::NonZeroUsize, PortError>;

    /// Reuses `into` for the current live learned histories, oldest first.
    ///
    /// # Errors
    /// Returns a port error when the scheduler cannot provide its learned state.
    async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError>;

    /// Replaces learned state with compatible live records in supplied recency order.
    ///
    /// # Errors
    /// Returns a port error if the scheduler cannot restore its learned state.
    async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError>;

    /// Overwrites `into` with the connected workers and the queued actions. A worker Gang does
    /// not manage is left out. Workers have unique ids and are sorted by `WorkerId` ascending.
    /// The buffers of `into` are reused. On `Err`, `into` is unspecified; callers skip the pass.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving or acquisition times out;
    /// [`PortError`] when the scheduler cannot be queried.
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError>;

    /// Overwrites `into` with the actions the scheduler holds: the executing ones, ordered by
    /// operation, and the pending ones, highest priority first, then oldest. Each list is cut
    /// to `limits`; the `truncated_*` counts of `into` say how many entries were left out. The
    /// buffers of `into` are reused. On `Err`, `into` is unspecified.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError`] when the
    /// scheduler cannot be queried.
    async fn actions(&self, limits: ListingLimits, into: &mut ActionsView)
    -> Result<(), PortError>;

    /// Stops (`true`) or resumes (`false`) assignments to every current and future connection
    /// of the physical instance. A valid intent succeeds even with no current connection.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving or acquisition times out.
    /// An acquisition timeout occurs before any control mutation starts. A backend error may
    /// occur when a connected worker could not be updated; a failed update retains the intent.
    async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError>;

    /// Retains physical intents seen in a complete provider census, allowing `grace` of absence.
    ///
    /// `now` uses the injected server clock. Reappearance resets the absence interval. Each
    /// supply includes every lifecycle and configuration generation. The implementation scans
    /// the complete borrowed census synchronously after acquiring its asynchronous write lock.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving or acquisition times out.
    /// An acquisition timeout occurs before any control mutation starts.
    async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError>;

    /// The URL the controller's worker listener is dialed on.
    fn endpoints(&self) -> ControllerEndpoints;

    /// Prepares the process-owned shared stores and services once, builds the term's scheduler
    /// and leader services, and serves the supplied connections. Public connections carry client
    /// services and `api` over HTTP/1.1 or HTTP/2; worker connections carry worker services over
    /// HTTP/2. The caller may supply both streams before promotion. Each request passes its
    /// listener's gate with its service scope, and leader-scoped dispatch requires admission.
    /// Listening, TLS, and authentication belong to the caller.
    /// It sends `ready` once the controller is built and before it serves the first connection;
    /// a run that fails before that drops `ready` unsent.
    ///
    /// Once every connection stream has ended, the controller hands off: it assigns no new
    /// action to any worker, ends with `UNAVAILABLE` every client operation stream whose action
    /// is not executing (so the client submits it again elsewhere), asks every client
    /// connection to go away once its streams finish, and releases each worker the moment it
    /// runs nothing (the worker reconnects). Worker connections are never asked to go away, so a
    /// worker reports the actions it runs on the connection that dispatched them. The run
    /// returns once no connection remains or the plugin's shutdown grace has elapsed; dropping
    /// the returned future closes every connection at once.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid controller;
    /// [`PortError`] when a store cannot be started or the controller fails.
    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError>;

    /// Serves the prepared shared services using the supplied gates and API while deliberately
    /// refusing leader-scoped requests before dispatch. This can run while an existing controller
    /// drains: it must not build or publish another controller, signal readiness, schedule work,
    /// or access learned history.
    ///
    /// Once both streams end, finishes pending responses within the backend's configured
    /// shutdown bounds. Dropping the future closes its remaining connections.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] if shared services have not been prepared; a port error if
    /// the standby transport cannot be configured or fails.
    async fn serve_standby<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        connections: ControllerConnections<P, W, A>,
    ) -> Result<(), PortError>;

    /// Builds one worker, connects it to the controller's worker listener, and runs it until
    /// `stop` completes, the worker fails, or a [`WorkerLifetime::SingleUse`] worker has
    /// finished its one action and uploaded the result.
    ///
    /// Once `stop` completes the worker takes no new action, ends every action it runs (the
    /// action's whole process group) and removes their directories, then returns `Ok(())`, so
    /// the next worker may use the same `work_directory` at once. Dropping the returned future
    /// instead abandons the running actions; callers stop a worker through `stop`.
    ///
    /// `tls` is how the worker secures its connection to the worker endpoint of
    /// [`SchedulerPort::endpoints`]: [`WorkerTls::scheme`] must be that endpoint's URL scheme.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid worker, including a `tls` whose
    /// scheme is not the endpoint's; [`PortError`] when the worker cannot be started or fails.
    /// [`PortError::Backend`] when the controller refuses the worker's certificate as revoked;
    /// the worker then never reconnects.
    async fn run_worker(
        &self,
        spec: &WorkerSpec,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError>;
}

/// One versioned learned history and its original expiration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearnedHistory {
    /// Resource or duration history key.
    pub key: RecordKey,
    /// Opaque engine-versioned payload.
    pub body: Vec<u8>,
    /// The original expiration, never extended by persistence.
    pub expires_at: Timestamp,
}

/// Conservative usable memory for one configured fleet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetMemoryCapacity {
    /// Index in the configured fleet catalog, matching queued demand candidates.
    pub fleet: usize,
    /// Usable memory, initially estimated and lowered by physical observations.
    pub capacity: NonZeroBytes,
}

/// What the scheduler holds at one moment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchedulerSnapshot {
    /// Actions that reached a terminal result on a worker since the controller started. Monotonic.
    pub completed: u64,
    /// Completion cells, unique and sorted by `(source, preferred_fleet)`.
    pub source_rates: Vec<CompletionRates>,
    /// Rate-row read time, sampled under the rate mutex before any listing awaits.
    pub source_rates_at: Tick,
    /// Controller-owned physical intents only, unique and sorted by `(fleet, instance)`.
    pub cordons: Vec<InstanceCordon>,
    /// Current conservative usable memory per configured fleet, initially estimated until observed.
    /// This buffer is reused across snapshots.
    pub fleet_memory: Vec<FleetMemoryCapacity>,
    /// The connected workers, with unique ids sorted by `WorkerId` ascending.
    pub workers: Vec<WorkerState>,
    /// The queued actions, highest priority first, then oldest.
    pub queued: Vec<QueuedDemand>,
    /// Executing actions, referring to workers by their index in `workers`.
    pub executing: Vec<ExecutingDemand>,
    /// Engine-bounded stores and their cumulative eviction counters.
    pub stores: Vec<EngineStore>,
}

/// One controller-owned cordon, shared across all connections of a physical instance.
#[derive(Debug, PartialEq, Eq)]
pub struct InstanceCordon {
    /// Fleet's catalog index, matching completion-rate rows.
    pub fleet: usize,
    /// Immutable physical instance identity.
    pub instance: Arc<InstanceId>,
    /// Whether this cordon permanently replaces an outdated worker build.
    pub replacing: bool,
}

impl Clone for InstanceCordon {
    fn clone(&self) -> Self {
        Self {
            fleet: self.fleet,
            instance: Arc::clone(&self.instance),
            replacing: self.replacing,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.fleet = source.fleet;
        self.instance.clone_from(&source.instance);
        self.replacing = source.replacing;
    }
}

/// A bounded engine store's usage and cumulative evictions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineStore {
    /// The store's role.
    pub store: StoreKind,
    /// Bytes currently held.
    pub used: u64,
    /// Eviction budget in bytes.
    pub budget: u64,
    /// Bytes evicted since the engine started.
    pub evicted: u64,
}

/// The role of an engine-bounded store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoreKind {
    /// The content-addressed store.
    #[default]
    Content,
    /// The action cache.
    Action,
}

/// Measurements from one worker's fast and slow stores.
///
/// Counters cover the lifetime of the worker's stores, and reset when those stores restart.
/// Reads count requests that return data, including partial reads that fail, plus successful
/// empty reads. Bytes are actual transferred bytes; direct filesystem materializations report
/// the blob's logical bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerCacheStats {
    /// Counted reads from the local fast store.
    pub local_read_requests: u64,
    /// Bytes transferred by reads from the local fast store.
    pub local_read_bytes: u64,
    /// Counted reads from the slow store, which may be remote or controller-backed.
    pub slow_read_requests: u64,
    /// Bytes transferred by reads from the slow store.
    pub slow_read_bytes: u64,
    /// Evicted entries reported by the native fast-store eviction map; replacements are excluded.
    pub evictions: u64,
    /// Evicted bytes reported by the native fast-store eviction map; replacements are excluded.
    pub evicted_bytes: u64,
    /// Current size in bytes of the fast store.
    pub size_bytes: u64,
    /// Fast store eviction budget in bytes.
    pub budget_bytes: u64,
}

/// A connected worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerState {
    /// The scheduler's id for the worker; it starts with the id of the instance that hosts it.
    pub id: WorkerId,
    /// The fleet the worker registered.
    pub fleet: FleetName,
    /// The action environment actually served, if the worker reported a valid identity.
    pub environment: Option<ActionEnvironment>,
    /// Whether the registered Gang package version matches this controller.
    pub current_build: bool,
    /// Actions it is running.
    pub running_actions: u32,
    /// Actions it accepts at once; `None` when the worker accepts any number.
    pub max_inflight_actions: Option<NonZeroU32>,
    /// Worker resource capacity from the scheduler ledger, absent when unavailable or malformed.
    pub capacity: Option<ActionResources>,
    /// Resources currently reserved in the scheduler ledger, absent when unavailable or malformed.
    pub reserved: Option<ActionResources>,
    /// Store measurements, absent when the worker does not report them.
    pub cache: Option<WorkerCacheStats>,
    /// Whether the scheduler has stopped assigning it new actions.
    pub draining: bool,
    /// Whether the scheduler has paused the worker. Not the same as draining, which a drain
    /// request sets.
    pub paused: bool,
    /// When the worker last spoke to the scheduler.
    pub last_seen: Timestamp,
}

/// An action waiting for a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedDemand {
    /// Authenticated first-creator source, or unattributed legacy work.
    pub source: Option<SourceId>,
    /// The fleets that accept the action, as indices into [`ControllerSpec::fleets`], in
    /// preference order; empty when none accepts it.
    pub candidates: FleetSet,
    /// The resources the action requests, each dimension zero when the action does not request it.
    pub resources: ActionResources,
    /// When it was queued.
    pub queued_since: Timestamp,
    /// Scheduling priority; higher runs first.
    pub priority: i32,
    /// Learned mean slot time, or `None` when no history is available.
    pub expected: Option<Duration>,
}

/// An action executing on a connected worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutingDemand {
    /// Authenticated first-creator source, or unattributed legacy work.
    pub source: Option<SourceId>,
    /// Accepting fleets in preference order; the first is preferred, not the actual worker's fleet.
    pub candidates: FleetSet,
    /// Action reservations, independent of the worker's fleet.
    pub resources: ActionResources,
    /// Index into the snapshot's workers, sorted by worker ID.
    pub worker: usize,
    /// When execution started.
    pub started: Timestamp,
    /// Learned mean slot time, or `None` when no history is available.
    pub expected: Option<Duration>,
}

/// Limits on how many actions [`SchedulerPort::actions`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListingLimits {
    /// The most executing actions to return.
    pub running: usize,
    /// The most pending actions to return.
    pub queued: usize,
}

/// The actions the scheduler holds, as [`SchedulerPort::actions`] reports them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActionsView {
    /// The running actions, ordered by operation.
    pub running: Vec<RunningActionView>,
    /// The queued actions, highest priority first, then oldest.
    pub queued: Vec<QueuedActionView>,
    /// The running actions left out by [`ListingLimits::running`].
    pub truncated_running: u64,
    /// The queued actions left out by [`ListingLimits::queued`].
    pub truncated_queued: u64,
}

/// The part of an action that a running and a queued action share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionInfo {
    /// The operation name the scheduler assigned.
    pub op: String,
    /// The action digest, `<hash>/<size>`.
    pub digest: String,
    /// The Bazel request target; empty when the request carries none.
    pub target: String,
    /// The Bazel request mnemonic; empty when the request carries none.
    pub mnemonic: String,
    /// Scheduling priority; higher runs first.
    pub priority: i32,
    /// The build the action's first submitter belongs to; `None` when unattributable.
    pub build: Option<BuildId>,
    /// Resources reserved by the scheduler, including zero-valued dimensions.
    pub reservation: ActionResources,
    /// Pre-fit learned reservation when CPU or memory exceeded the fitted reservation.
    pub learned_reservation: Option<ActionResources>,
}

/// A running action reported by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningActionView {
    /// The metadata the action shares with a queued action.
    pub info: ActionInfo,
    /// The worker executing the action.
    pub worker: WorkerId,
    /// When the worker started the action.
    pub started: Timestamp,
}

/// An action waiting for a worker, with the metadata [`SchedulerPort::actions`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedActionView {
    /// The metadata the action shares with a running action.
    pub info: ActionInfo,
    /// When it was queued.
    pub queued_since: Timestamp,
    /// The fleets that accept the action; empty when none accepts it.
    pub candidates: FleetSet,
    /// Why no fleet accepts the action; set exactly when `candidates` is empty.
    pub refusal: Option<Refusal>,
}

/// A byte stream the controller serves one HTTP/2 connection on: an accepted socket, a TLS
/// stream, or an in-memory pipe.
pub trait Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Connection for T {}

/// One boxed [`Connection`].
pub type BoxedConnection = Box<dyn Connection>;

/// An accepted connection and what the transport verified about it.
pub struct Accepted {
    /// The connection to serve.
    pub connection: BoxedConnection,
    /// What the transport verified about the connection, such as the client certificate.
    pub peer: Peer,
}

/// What a gate learned about the caller of a request it admits: the one answer to whether, and
/// as whom, the caller authenticated. A connection whose request was admitted
/// [`Admitted::Authenticated`] has earned its place among the listener's connections, and the
/// services behind the gate act for that caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    /// The caller proved who it is.
    Authenticated(Authenticated),
    /// The caller is anonymous, as an open path of the public listener admits it.
    Anonymous,
}

/// The service plane a request needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestScope {
    /// Services available independently of leadership after preparation.
    Shared,
    /// Services that require the controller to admit leader work.
    Leader,
}

/// The gate's decision before a request reaches a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    /// The request may proceed with the given caller identity.
    Admit(Admitted),
    /// Leader-scoped work is unavailable on this controller.
    NotLeading,
}

/// Response extension marking a leadership refusal made before handler dispatch.
///
/// This is in-process transport metadata, not an HTTP header or wire error reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderUnavailable;

/// The authentication gate of a listener. The controller applies it to every request before any
/// service sees it, so the scheduler stays free of TLS, tokens and revocation.
pub trait RequestGate: Clone + Send + Sync + 'static {
    /// Decides whether a request in `scope` may proceed, and as whom. The gate may add values
    /// to `parts.extensions` for the services behind it. An admitted request carries
    /// [`Admitted::Authenticated`] when its caller proved who it is, and
    /// [`Admitted::Anonymous`] otherwise. [`GateDecision::NotLeading`] refuses leader work
    /// before dispatch.
    ///
    /// # Errors
    ///
    /// [`PortError::Rejected`] with [`Rejection::InvalidProof`](crate::Rejection::InvalidProof)
    /// when the caller is not authenticated, with
    /// [`Rejection::PolicyDenied`](crate::Rejection::PolicyDenied) when it is not allowed, with
    /// [`Rejection::PrincipalRevoked`](crate::Rejection::PrincipalRevoked) when the caller's
    /// principal is revoked;
    /// [`PortError::Unavailable`] when a dependency of the gate is down.
    fn admit(
        &self,
        scope: RequestScope,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> impl Future<Output = Result<GateDecision, PortError>> + Send;
}

/// The request body of an [`ApiService`].
pub type ApiRequestBody = http_body_util::combinators::UnsyncBoxBody<BodyBytes, BoxedError>;

/// The response body of an [`ApiService`]: boxed so an answer can be a stream.
pub type ApiResponseBody = http_body_util::combinators::UnsyncBoxBody<BodyBytes, BoxedError>;

/// A complete in-memory body.
#[must_use]
pub fn full_body(bytes: impl Into<BodyBytes>) -> ApiResponseBody {
    http_body_util::Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// The Gang API (`/v1/*`) that the controller serves beside its own services, for every request
/// no controller service handles. It runs after the listener's gate and finds the caller's
/// identity in the request extensions.
pub trait ApiService: Clone + Send + Sync + 'static {
    /// Answers one request.
    fn call(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> impl Future<Output = http::Response<ApiResponseBody>> + Send;
}

/// One listener's connections and the gate applied to its requests.
pub struct Listening<G> {
    /// The connections the caller accepted. A stream ending means no more connections.
    pub connections: BoxStream<'static, Accepted>,
    /// The gate every request on these connections passes.
    pub gate: G,
}

/// What the controller serves: the public listener, which carries all client services and the
/// Gang API, and the private worker listener. Their gates may differ.
pub struct ControllerConnections<P, W, A> {
    /// The public listener: Bazel and `gang` clients. Requests no client service handles go to
    /// `api`.
    pub public: Listening<P>,
    /// The worker listener: workers.
    pub worker: Listening<W>,
    /// The Gang API.
    pub api: A,
}

/// The signal [`SchedulerPort::run_controller`] sends once its controller is built and about to
/// serve, so every startup check of the controller has passed. Dropped unsent, it tells the
/// waiter that the controller never became ready.
#[derive(Debug)]
pub struct ControllerReady(oneshot::Sender<()>);

impl ControllerReady {
    /// A signal and the receiver that resolves once the signal is sent, or fails once it is
    /// dropped unsent.
    #[must_use]
    pub fn channel() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (Self(sender), receiver)
    }

    /// Tells the waiter that the controller is ready.
    pub fn signal(self) {
        // A waiter that has gone needs no signal.
        let _ = self.0.send(());
    }
}

/// How a worker secures its connection to the controller's worker listener.
#[derive(Debug, Clone, Copy)]
pub enum WorkerTls<'a> {
    /// No TLS: the worker endpoint's scheme is `grpc`.
    Plain,
    /// TLS that authenticates the server only; the worker presents no certificate.
    ServerOnly {
        /// The CA that the controller's certificate must chain to; `None` trusts the system
        /// roots, as for a TLS-terminating proxy with a public certificate.
        server_ca: Option<&'a Path>,
    },
    /// Mutual TLS: the worker also presents its own certificate.
    Mutual(ClientTls<'a>),
}

impl WorkerTls<'_> {
    /// The URL scheme of a worker endpoint reached this way: `grpc` for [`WorkerTls::Plain`],
    /// `grpcs` otherwise.
    #[must_use]
    pub const fn scheme(&self) -> &'static str {
        match self {
            Self::Plain => "grpc",
            Self::ServerOnly { .. } | Self::Mutual(_) => "grpcs",
        }
    }
}

/// The certificate material a worker presents on a mutual TLS connection to the controller.
#[derive(Debug, Clone, Copy)]
pub struct ClientTls<'a> {
    /// The worker's certificate chain.
    pub certificate: &'a Path,
    /// The worker's private key.
    pub private_key: &'a Path,
    /// The CA that the controller's certificate must chain to.
    pub server_ca: &'a Path,
}

/// Inputs of the controller.
#[derive(Clone)]
pub struct ControllerSpec<'a> {
    /// Shared action scheduling and enforcement policy.
    pub actions: Arc<gang_config::ActionsConfig>,
    /// Shared rate estimation and scaling policy.
    pub scaling: Arc<gang_config::ScalingConfig>,
    /// The server's existing clock, shared with completion producers and reconciliation.
    pub clock: Arc<dyn Clock>,
    /// Bound on control lock acquisition and read preparation, derived from the server's poll
    /// cadence. Once a control mutation begins it is not timed out or cancelled by this bound.
    pub control_acquire_timeout: NonZeroDuration,
    /// The content-addressable store.
    pub content_store: &'a RenderedStoreSpec,
    /// The action store.
    pub action_store: &'a RenderedStoreSpec,
    /// Every fleet's properties in preference order: what the scheduler declares and how
    /// queued actions are matched.
    pub fleets: &'a FleetCatalog,
    /// The most workers the fleets run at once, one per instance: the sum of their
    /// `max_instances`.
    pub max_workers: u64,
    /// The timeout the controller applies to client actions.
    pub client_action_timeout: Duration,
    /// Where the controller's observers send what builds do.
    pub observations: &'a ObservationSink,
}

impl std::fmt::Debug for ControllerSpec<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControllerSpec")
            .field("actions", &self.actions)
            .field("scaling", &self.scaling)
            .field("control_acquire_timeout", &self.control_acquire_timeout)
            .field("content_store", &self.content_store)
            .field("action_store", &self.action_store)
            .field("fleets", &self.fleets)
            .field("max_workers", &self.max_workers)
            .field("client_action_timeout", &self.client_action_timeout)
            .field("observations", &self.observations)
            .finish_non_exhaustive()
    }
}

/// The dialable URL of the controller's worker listener, with its host name, as the plugin's
/// configuration names it (`worker_endpoint`). Workers dial it. `gang-mtls` takes the host name
/// for the worker server certificate's subject alternative name from it. The caller listens
/// where it leads and secures the connections it hands to the controller; a `grpcs` scheme says
/// the listener uses TLS. The public endpoint that clients dial is the caller's own setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerEndpoints {
    /// The URL that workers dial.
    pub worker: Url,
}

/// How a worker reaches the stores of [`ControllerSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreAccess {
    /// The workers open the store themselves, as with an object store that every node can reach.
    Direct,
    /// Only the controller can open the store, as with a local directory; workers reach it
    /// through the controller's worker listener.
    ViaController,
}

/// Inputs of one worker.
#[derive(Debug, Clone)]
pub struct WorkerSpec {
    /// Shared action scheduling and enforcement policy.
    pub actions: Arc<gang_config::ActionsConfig>,
    /// The worker's action cgroup pool, if supported.
    pub memory_pool: Option<ActionMemoryPool>,
    /// The instance the worker runs on; the plugin names the worker with
    /// [`InstanceId::worker_name`].
    pub instance: InstanceId,
    /// The properties the worker registers: its fleet's.
    pub properties: FleetProperties,
    /// Whether the worker serves one action or remains available for more.
    pub lifetime: WorkerLifetime,
    /// Where actions are built: one directory per worker. The worker empties it at start.
    pub work_directory: PathBuf,
    /// The worker-local fast content-addressable cache directory, under the mounted scratch
    /// volume; one per worker.
    pub fast_cas_directory: PathBuf,
    /// The most the fast cache may hold before it evicts; derived from the scratch volume's
    /// capacity.
    pub fast_cas_budget: Bytes,
    /// The content-addressable store.
    pub content_store: RenderedStoreSpec,
    /// The action store.
    pub action_store: RenderedStoreSpec,
    /// How the worker reaches the two stores above.
    pub store_access: StoreAccess,
    /// The `gang-exec` link to the `gang` binary, which runs as the worker's action
    /// entrypoint (`gang exec`). The sandbox file `gang exec` reads is its sibling
    /// (`gang_config::SandboxFile::beside`); the directory is the worker's, never writable by
    /// actions.
    pub entrypoint: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use http_body_util::BodyExt;

    use super::{BoxedError, InstanceCordon, full_body};

    #[test]
    fn instance_cordon_clone_from_shares_the_immutable_identity() -> Result<(), BoxedError> {
        let source = InstanceCordon {
            fleet: 3,
            instance: std::sync::Arc::new(gang_types::InstanceId::try_new("instance-a")?),
            replacing: true,
        };
        let mut target = InstanceCordon {
            fleet: 1,
            instance: std::sync::Arc::new(gang_types::InstanceId::try_new("instance-b")?),
            replacing: false,
        };
        target.clone_from(&source);
        assert_eq!(target, source);
        assert!(std::sync::Arc::ptr_eq(&target.instance, &source.instance));
        let cloned = source.clone();
        assert_eq!(cloned, source);
        assert!(std::sync::Arc::ptr_eq(&cloned.instance, &source.instance));
        Ok(())
    }

    #[test]
    fn full_body_collects_to_its_bytes() -> Result<(), BoxedError> {
        let mut future = pin!(full_body("answer").collect());
        let mut task_context = Context::from_waker(Waker::noop());
        let Poll::Ready(collected) = future.as_mut().poll(&mut task_context) else {
            return Err("a full body is ready on its first poll".into());
        };
        assert_eq!(collected?.to_bytes(), "answer");
        Ok(())
    }
}
