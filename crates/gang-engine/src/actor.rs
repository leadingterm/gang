//! Single-owner controller actor shell, 3-lane biased inbox, and synchronous effect executor
//! (`§5.10`, `§6.1`, `§8.2`).
//!
//! Owns [`Scheduler`] exclusively on one Tokio task with zero locks. Every iteration drains work
//! in strict priority order:
//! 1. Due monotonic timers (`gang_clock::Clock::sleep_until`),
//! 2. `control_rx` (drained completely),
//! 3. `worker_rx` (up to `scheduler_batch`),
//! 4. `client_rx` (up to `scheduler_batch`).
//!
//! All outbound worker directives use non-blocking `try_send`; a full or closed worker outbound
//! channel evicts that worker via [`Scheduler::on_worker_lost`] rather than blocking the actor.
//! Client operation watchers use lag-safe `tokio::sync::watch` channels so slow clients never
//! stall scheduling.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes as RawBytes;
use gang_clock::{Clock, Tick, Timestamp};
use gang_config::ServerConfig;
use gang_reapi::build::bazel::remote::execution::v2::ActionResult;
use gang_reapi::google::rpc::Status as RpcStatus;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_reapi::leadingterm::gang::worker::v1::controller_message::Kind as ControllerKind;
use gang_scheduler::{
    AdmissionRequest, CapacityError, Census, CompletedObservation, CordonError, Effect, Effects,
    ExecuteError, GateAudit, HandOffError, KillReason, Now, OperationKey, OperationUpdate,
    RegisterError, RehomeReason, Scheduler, SketchError, WorkerEvent, WorkerKey,
    WorkerRegistration,
};
use gang_spi::{
    ActionsView, ControllerDrain, Executed as ObservedExecuted, ExecutionTimes,
    FleetResourceCapacity, LearnedHistory, ListingLimits, Observation, ObservationSink, Origin,
    Phase, SchedulerSnapshot,
};
use gang_types::{
    ActionResources, AttemptId, Bytes, CpuWeight, Identity, InstanceId, NonZeroDuration,
    OperationName, WorkerId,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::controller::LearnedSlot;
use crate::handoff::HandOff;

/// Default bounded capacity of each actor inbox (`control_rx`, `worker_rx`, `client_rx`).
pub const DEFAULT_SCHEDULER_QUEUE_DEPTH: NonZeroUsize =
    NonZeroUsize::new(4096).expect("4096 is non-zero");

/// Default worker/client messages processed per actor wake iteration.
pub const DEFAULT_SCHEDULER_BATCH: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Default per-worker outbound channel capacity.
pub const DEFAULT_WORKER_OUTBOUND_DEPTH: NonZeroUsize =
    NonZeroUsize::new(256).expect("256 is non-zero");

/// Default maximum combined `Action` + `Command` bytes inlined in `worker.v1::Assign` (`§8.2`).
pub const DEFAULT_ASSIGN_INLINE_MAX: Bytes = Bytes::kib(64);

/// Runtime tuning parameters for the single-owner controller actor (`§5.10`, `§8.2`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorConfig {
    /// Bounded capacity of each actor inbox (`control_rx`, `worker_rx`, `client_rx`).
    pub scheduler_queue_depth: NonZeroUsize,
    /// Maximum messages drained from `worker_rx` or `client_rx` per wake iteration.
    pub scheduler_batch: NonZeroUsize,
    /// Bounded capacity of each connected worker's outbound channel.
    pub worker_outbound_depth: NonZeroUsize,
    /// Maximum combined `Action` + `Command` bytes inlined in `Assign`.
    pub assign_inline_max: Bytes,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            scheduler_queue_depth: DEFAULT_SCHEDULER_QUEUE_DEPTH,
            scheduler_batch: DEFAULT_SCHEDULER_BATCH,
            worker_outbound_depth: DEFAULT_WORKER_OUTBOUND_DEPTH,
            assign_inline_max: DEFAULT_ASSIGN_INLINE_MAX,
        }
    }
}

impl ActorConfig {
    /// Derives [`ActorConfig`] from the server's parsed `[server]` configuration.
    #[must_use]
    pub const fn from_server_config(server: &ServerConfig) -> Self {
        Self {
            scheduler_queue_depth: server.scheduler_queue_depth,
            scheduler_batch: server.scheduler_batch,
            worker_outbound_depth: server.worker_outbound_depth,
            assign_inline_max: server.assign_inline_max,
        }
    }
}

type ExistenceInvalidator = dyn Fn(&[gang_types::Digest]) + Send + Sync;

/// Optional shell collaborators wired into a running controller actor.
#[derive(Clone, Default)]
pub struct ControllerRuntime {
    /// Optional build observation sink for completed attempts (`Effect::RecordObservation`).
    pub observations: Option<ObservationSink>,
    /// Optional cooperative handoff coordinator (`handoff.rs`).
    pub(crate) handoff: Option<HandOff>,
    /// Optional acquisition timeout bounding control-plane enqueueing (`§5.10`).
    pub control_acquire_timeout: Option<NonZeroDuration>,
    pub(crate) invalidate_existence: Option<Arc<ExistenceInvalidator>>,
}

impl ControllerRuntime {
    /// Creates an empty [`ControllerRuntime`] with no optional collaborators.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches a cooperative [`HandOff`] coordinator.
    #[must_use]
    #[allow(dead_code)] // Wired by controller runtime (WP D6).
    pub(crate) fn with_handoff(mut self, handoff: HandOff) -> Self {
        self.handoff = Some(handoff);
        self
    }
}

/// Serialized `REAPI v2` `Action` and `Command` pair eligible for inline delivery in `Assign`
/// (`§8.2`).
///
/// Both fields are supplied together; the actor inlines both only when
/// `action.len() + command.len() <= assign_inline_max`, otherwise both are omitted so the worker
/// fetches them from the CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineActionPayload {
    /// Serialized `REAPI v2` `Action` protobuf bytes.
    pub action: RawBytes,
    /// Serialized `REAPI v2` `Command` protobuf bytes.
    pub command: RawBytes,
}

/// Full client `Execute` request passed into the controller actor.
#[derive(Debug, Clone)]
pub struct ExecuteSpec {
    /// Pure scheduling admission request.
    pub request: AdmissionRequest,
    /// Authenticated caller identity for gate-tier (`priority >= 200`) audit logging (`§3.2`).
    pub caller: Option<Arc<Identity>>,
    /// Optional build observation origin for `ObservationSink` recording on completion.
    pub origin: Option<Origin>,
    /// Optional serialized `Action` + `Command` pair for inline `Assign` delivery (`§8.2`).
    pub inline: Option<InlineActionPayload>,
    /// Effective action execution timeout carried in `Assign.timeout`.
    pub timeout: Option<Duration>,
}

impl ExecuteSpec {
    /// Wraps a bare [`AdmissionRequest`] with no optional shell metadata.
    #[must_use]
    pub const fn new(request: AdmissionRequest) -> Self {
        Self {
            request,
            caller: None,
            origin: None,
            inline: None,
            timeout: None,
        }
    }
}

/// Optional shell-side completion payload carried alongside [`WorkerEvent::Completed`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompletionPayload {
    /// Full `REAPI v2` `ActionResult` when execution and upload succeeded.
    pub result: Option<ActionResult>,
    /// Non-OK `google.rpc.Status` when upload failed or execution did not produce a result.
    pub status: Option<RpcStatus>,
    /// Action process exit code for build observation recording.
    pub exit_code: i32,
    /// Whether the attempt carried an execution error for build observation recording.
    pub error: bool,
}

/// Outbound controller directive delivered on a worker session's bounded channel (`§8.2`).
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerOutbound {
    /// Assigns an action attempt to the worker.
    Assign(Box<proto::Assign>),
    /// Requests immediate termination of an in-flight attempt.
    Kill(proto::Kill),
    /// Updates the worker's controller-driven draining flag.
    SetDraining(bool),
    /// Ends this session after attempts finish so the worker redials the new leader.
    Rehome(RehomeReason),
}

impl WorkerOutbound {
    /// Converts this outbound directive into its `worker.v1` [`proto::ControllerMessage`] frame.
    #[must_use]
    pub fn into_proto(self) -> proto::ControllerMessage {
        let kind = match self {
            Self::Assign(assign) => ControllerKind::Assign(*assign),
            Self::Kill(kill) => ControllerKind::Kill(kill),
            Self::SetDraining(draining) => {
                ControllerKind::SetDraining(proto::SetDraining { draining })
            }
            Self::Rehome(_) => ControllerKind::Rehome(proto::Rehome {}),
        };
        proto::ControllerMessage { kind: Some(kind) }
    }
}

/// Handle returned to a worker session task after successful registration.
#[derive(Debug)]
pub struct RegisteredWorker {
    /// Generation-checked scheduler worker handle.
    pub key: WorkerKey,
    /// Controller-minted [`WorkerId`] returned to the worker in `Registered`.
    pub worker_id: WorkerId,
    /// Bounded outbound channel carrying controller directives.
    pub outbound: mpsc::Receiver<WorkerOutbound>,
}

/// Shell-level operation lifecycle state delivered over an [`OperationWatch`] stream.
#[derive(Debug, Clone, PartialEq)]
pub struct OperationWatchState {
    /// Canonical `REAPI` operation resource name.
    pub operation: OperationName,
    /// Current scheduler lifecycle stage update.
    pub update: OperationUpdate,
    /// Terminal `REAPI` `ActionResult` when completed with a result.
    pub result: Option<ActionResult>,
    /// Terminal `google.rpc.Status` when completed with an error status.
    pub status: Option<RpcStatus>,
}

impl OperationWatchState {
    /// Constructs a [`OperationUpdate::Queued`] watch state for `operation`.
    #[must_use]
    pub const fn queued(operation: OperationName) -> Self {
        Self {
            operation,
            update: OperationUpdate::Queued,
            result: None,
            status: None,
        }
    }

    /// Returns `true` when this state represents a terminal operation lifecycle event.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        is_terminal_update(&self.update)
    }
}

/// Active client subscription to an operation's lifecycle updates (`§5.10`).
///
/// Backed by a lag-safe `tokio::sync::watch` channel so the scheduler actor never blocks on slow
/// clients. Dropping this guard automatically notifies the actor (`ClientMsg::Unwatch`) so the
/// scheduler's watcher count stays accurate when client gRPC streams cancel or end.
#[derive(Debug)]
pub struct OperationWatch {
    key: OperationKey,
    name: OperationName,
    receiver: watch::Receiver<OperationWatchState>,
    client_tx: mpsc::Sender<ClientMsg>,
    unwatched: bool,
}

impl OperationWatch {
    pub(crate) const fn new(
        key: OperationKey,
        name: OperationName,
        receiver: watch::Receiver<OperationWatchState>,
        client_tx: mpsc::Sender<ClientMsg>,
        track_watcher: bool,
    ) -> Self {
        Self {
            key,
            name,
            receiver,
            client_tx,
            unwatched: !track_watcher,
        }
    }

    /// Returns the scheduler's generation-checked [`OperationKey`].
    #[must_use]
    pub const fn key(&self) -> OperationKey {
        self.key
    }

    /// Returns the canonical `REAPI` [`OperationName`].
    #[must_use]
    pub const fn name(&self) -> &OperationName {
        &self.name
    }

    /// Clones the most recently published [`OperationWatchState`].
    #[must_use]
    pub fn current(&self) -> OperationWatchState {
        self.receiver.borrow().clone()
    }

    /// Borrows the latest state and marks it seen.
    #[must_use]
    pub fn borrow_and_update(&mut self) -> watch::Ref<'_, OperationWatchState> {
        self.receiver.borrow_and_update()
    }

    /// Waits for the next state transition and returns the updated [`OperationWatchState`].
    ///
    /// # Errors
    ///
    /// Returns [`watch::error::RecvError`] once the operation has terminated and no further unseen
    /// updates remain.
    pub async fn changed(&mut self) -> Result<OperationWatchState, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.receiver.borrow_and_update().clone())
    }

    /// Explicitly detaches this watcher by awaiting `ClientMsg::Unwatch` delivery.
    pub async fn close(mut self) {
        if !self.unwatched {
            self.unwatched = true;
            let _ = self
                .client_tx
                .send(ClientMsg::Unwatch {
                    operation: self.key,
                })
                .await;
        }
    }
}

impl Drop for OperationWatch {
    fn drop(&mut self) {
        if self.unwatched {
            return;
        }
        self.unwatched = true;
        let msg = ClientMsg::Unwatch {
            operation: self.key,
        };
        if let Err(mpsc::error::TrySendError::Full(msg)) = self.client_tx.try_send(msg)
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let tx = self.client_tx.clone();
            handle.spawn(async move {
                let _ = tx.send(msg).await;
            });
        }
    }
}

/// Control-plane commands processed at priority 2 (strictly above worker and client lanes).
#[derive(Debug)]
pub enum ControlMsg {
    /// Project current scheduler state into a reused [`SchedulerSnapshot`].
    Snapshot {
        /// Caller-owned reusable buffer.
        into: SchedulerSnapshot,
        /// Reply channel returning the filled buffer.
        reply: oneshot::Sender<SchedulerSnapshot>,
    },
    /// Project running and queued actions bounded by `limits` into a reused [`ActionsView`].
    Actions {
        /// Listing truncation limits.
        limits: ListingLimits,
        /// Caller-owned reusable buffer.
        into: ActionsView,
        /// Reply channel returning the filled buffer.
        reply: oneshot::Sender<ActionsView>,
    },
    /// Set or clear persistent physical drain intent for `(fleet, instance)`.
    SetDraining {
        /// Target fleet name.
        fleet: gang_types::FleetName,
        /// Target physical instance identifier.
        instance: InstanceId,
        /// Desired draining state.
        draining: bool,
        /// Reply channel reporting success or [`CordonError`].
        reply: oneshot::Sender<Result<(), CordonError>>,
    },
    /// Retain physical cordons present in `census`, pruning entries absent longer than `grace`.
    RetainDrains {
        /// Current monotonic tick.
        now: Tick,
        /// Absence grace period before pruning cordons.
        grace: Duration,
        /// Compact provider instance census.
        census: Census,
        /// Reply channel signalling completion.
        reply: oneshot::Sender<()>,
    },
    /// Scan connected workers for stale builds and permanently cordon their instances.
    StaleBuildDrains {
        /// Reply channel returning newly affected worker keys.
        reply: oneshot::Sender<Vec<WorkerKey>>,
    },
    /// Freeze dispatch/registration, drain connected workers, initiate public GOAWAY, and fill
    /// `into`.
    HandOff {
        /// Whether pre-handoff public streams are complete (overridden by attached [`HandOff`]).
        public_complete: bool,
        /// Caller-owned reusable drain snapshot.
        into: ControllerDrain,
        /// Reply channel returning the filled drain snapshot or [`HandOffError`].
        reply: oneshot::Sender<Result<ControllerDrain, HandOffError>>,
    },
    /// Refresh drain progress without activating handoff if inactive.
    RefreshDrain {
        /// Whether pre-handoff public streams are complete (overridden by attached [`HandOff`]).
        public_complete: bool,
        /// Caller-owned reusable drain snapshot.
        into: ControllerDrain,
        /// Reply channel returning the filled drain snapshot or [`HandOffError`].
        reply: oneshot::Sender<Result<ControllerDrain, HandOffError>>,
    },
    /// Snapshot live learned resource and duration histories into `into`.
    LearnedHistory {
        /// Wall-clock reading for expiration filtering.
        now: Timestamp,
        /// Caller-owned reusable buffer.
        into: Vec<LearnedHistory>,
        /// Reply channel returning the filled buffer.
        reply: oneshot::Sender<Vec<LearnedHistory>>,
    },
    /// Restore compatible live learned resource and duration histories.
    RestoreLearnedHistory {
        /// Wall-clock reading for expiration filtering.
        now: Timestamp,
        /// Persisted records to restore.
        records: Vec<LearnedHistory>,
        /// Reply channel reporting success or [`SketchError`].
        reply: oneshot::Sender<Result<(), SketchError>>,
    },
    /// Replace peer-local physical capacity observations.
    SetPeerCapacity {
        /// Peer observations by fleet index.
        observations: Vec<FleetResourceCapacity>,
        /// Reply channel reporting whether effective capacity changed or [`CapacityError`].
        reply: oneshot::Sender<Result<bool, CapacityError>>,
    },
    /// Recompute local observed fleet physical capacities from connected workers.
    RefreshLocalCapacity {
        /// Reply channel reporting whether effective capacity changed.
        reply: oneshot::Sender<bool>,
    },
    /// Cooperatively stop the actor and retain [`gang_scheduler::Learned`] in [`LearnedSlot`].
    Shutdown {
        /// Reply channel signalled after learned state is retained and channels are closed.
        reply: oneshot::Sender<()>,
    },
}

/// Worker-plane messages processed at priority 3 (below timers and control, above clients).
#[derive(Debug)]
pub enum WorkerMsg {
    /// Register a newly connected worker generation and bind its bounded outbound channel.
    Register {
        /// Registration payload from the worker's initial `Register` frame.
        registration: Box<WorkerRegistration>,
        /// Reply channel delivering the registered handle and outbound receiver.
        reply: oneshot::Sender<Result<RegisteredWorker, RegisterError>>,
    },
    /// Deliver one inbound worker protocol event with optional shell-side completion payload.
    Event {
        /// Connected worker handle.
        worker: WorkerKey,
        /// Pure scheduler worker event.
        event: WorkerEvent,
        /// Optional shell-side `REAPI` completion payload for [`WorkerEvent::Completed`].
        completion: Option<Box<CompletionPayload>>,
    },
    /// Report that a worker session disconnected.
    Lost {
        /// Disconnected worker handle.
        worker: WorkerKey,
    },
}

/// Client-plane messages processed at priority 4 (lowest priority, batched by `scheduler_batch`).
#[derive(Debug)]
pub enum ClientMsg {
    /// Admit or deduplicate a client `Execute` request and subscribe to its lifecycle updates.
    Execute {
        /// Full execution specification and optional shell metadata.
        spec: Box<ExecuteSpec>,
        /// Reply channel delivering an [`OperationWatch`] or [`ExecuteError`].
        reply: oneshot::Sender<Result<OperationWatch, ExecuteError>>,
    },
    /// Attach a watcher to an existing operation by canonical name.
    Watch {
        /// Canonical `REAPI` operation resource name.
        operation: OperationName,
        /// Reply channel delivering an [`OperationWatch`], or `None` if unknown.
        reply: oneshot::Sender<Option<OperationWatch>>,
    },
    /// Detach one watcher from an operation, arming `client_action_timeout` if count hits zero.
    Unwatch {
        /// Target operation handle.
        operation: OperationKey,
    },
    /// Cancel an operation immediately, dequeuing or emitting `Kill(Cancelled)`.
    Cancel {
        /// Target operation handle.
        operation: OperationKey,
        /// Reply channel signalled once cancellation has been applied.
        reply: oneshot::Sender<()>,
    },
}

/// Bounded actor inbox receivers owned exclusively by [`run_actor`].
#[derive(Debug)]
pub(crate) struct ActorInboxes {
    pub(crate) control_rx: mpsc::Receiver<ControlMsg>,
    pub(crate) worker_rx: mpsc::Receiver<WorkerMsg>,
    pub(crate) client_rx: mpsc::Receiver<ClientMsg>,
    pub(crate) client_tx: mpsc::Sender<ClientMsg>,
}

/// Shell-side metadata retained for non-terminal operations until terminal completion/expiry.
#[derive(Debug)]
struct OperationShellMeta {
    platform: BTreeMap<String, String>,
    origin: Option<Origin>,
    inline: Option<InlineActionPayload>,
    timeout: Option<Duration>,
    queued_wall: Timestamp,
}

/// Context queued alongside `Effect::AuditGateTier` during an `Execute` admission step.
#[derive(Debug)]
struct AuditContext {
    caller: Option<Arc<Identity>>,
    operation: OperationName,
}

/// Selects the inline `(action, command)` pair for `worker.v1::Assign` (`§8.2`).
///
/// Both fields are returned `Some` if and only if `payload` is present and
/// `action.len() + command.len() <= assign_inline_max`; otherwise both are `None`.
#[must_use]
pub fn select_inline_payload(
    payload: Option<&InlineActionPayload>,
    assign_inline_max: Bytes,
) -> (Option<RawBytes>, Option<RawBytes>) {
    let Some(pair) = payload else {
        return (None, None);
    };
    let total = pair.action.len().saturating_add(pair.command.len());
    if total <= assign_inline_max.as_usize() {
        (Some(pair.action.clone()), Some(pair.command.clone()))
    } else {
        (None, None)
    }
}

/// Maps a scheduler [`KillReason`] onto its wire `worker.v1::KillReason` enum value.
#[must_use]
pub const fn map_kill_reason(reason: KillReason) -> proto::KillReason {
    match reason {
        KillReason::Cancelled | KillReason::AckTimeout => proto::KillReason::Cancelled,
        KillReason::DeadlineExceeded => proto::KillReason::DeadlineExceeded,
    }
}

const fn is_terminal_update(update: &OperationUpdate) -> bool {
    matches!(
        update,
        OperationUpdate::Completed | OperationUpdate::Failed(_) | OperationUpdate::Cancelled
    )
}

fn timestamp_to_proto(ts: Timestamp) -> prost_types::Timestamp {
    let millis = ts.as_unix_millis();
    let seconds = i64::try_from(millis / 1_000).unwrap_or(i64::MAX);
    let nanos = i32::try_from((millis % 1_000) * 1_000_000).unwrap_or(0);
    prost_types::Timestamp { seconds, nanos }
}

fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos = i32::try_from(duration.subsec_nanos()).unwrap_or(0);
    prost_types::Duration { seconds, nanos }
}

/// Runs the single-owner scheduler actor loop until [`ControlMsg::Shutdown`] or all inboxes close,
/// then stores [`Scheduler::into_learned`] into `learned_slot` before dropping receivers (`§5.10`,
/// `§6.1`).
pub(crate) async fn run_actor<C: Clock + Send + Sync + 'static>(
    scheduler: Scheduler,
    clock: Arc<C>,
    config: ActorConfig,
    learned_slot: LearnedSlot,
    runtime: ControllerRuntime,
    inboxes: ActorInboxes,
) {
    let mut actor = ControllerActor::new(scheduler, clock, config, runtime, inboxes);

    actor.run_loop().await;

    // §6.1: store `Retained(learned)` BEFORE dropping `control_rx` so any concurrent reader whose
    // `Live(tx)` send/reply fails is guaranteed to see `Retained` on re-lock.
    let ControllerActor {
        scheduler,
        shutdown_reply,
        control_rx,
        worker_rx,
        client_rx,
        client_tx,
        worker_outbound,
        op_watches,
        ..
    } = actor;
    let learned = scheduler.into_learned();
    learned_slot.retain(learned);

    // Drop all inbox, outbound, and watcher channels before acknowledging Shutdown.
    drop((
        control_rx,
        worker_rx,
        client_rx,
        client_tx,
        worker_outbound,
        op_watches,
    ));
    if let Some(reply) = shutdown_reply {
        let _ = reply.send(());
    }
}

struct RetainedOperation {
    key: OperationKey,
    state: OperationWatchState,
}

struct ControllerActor<C> {
    scheduler: Scheduler,
    clock: Arc<C>,
    config: ActorConfig,
    observations: Option<ObservationSink>,
    handoff: Option<HandOff>,
    invalidate_existence: Option<Arc<ExistenceInvalidator>>,
    control_rx: mpsc::Receiver<ControlMsg>,
    worker_rx: mpsc::Receiver<WorkerMsg>,
    client_rx: mpsc::Receiver<ClientMsg>,
    client_tx: mpsc::Sender<ClientMsg>,
    control_closed: bool,
    worker_closed: bool,
    client_closed: bool,
    worker_outbound: HashMap<WorkerKey, mpsc::Sender<WorkerOutbound>>,
    op_watches: HashMap<OperationKey, watch::Sender<OperationWatchState>>,
    op_meta: HashMap<OperationKey, OperationShellMeta>,
    effects: Effects,
    effect_scratch: Effects,
    terminal_cleanup: Vec<OperationKey>,
    terminal_operations: HashMap<OperationName, RetainedOperation>,
    terminal_order: VecDeque<(OperationKey, OperationName)>,
    pending_audit: VecDeque<AuditContext>,
    pending_completion: Option<CompletionPayload>,
    shutdown_reply: Option<oneshot::Sender<()>>,
}

impl<C: Clock> ControllerActor<C> {
    fn new(
        scheduler: Scheduler,
        clock: Arc<C>,
        config: ActorConfig,
        runtime: ControllerRuntime,
        inboxes: ActorInboxes,
    ) -> Self {
        Self {
            scheduler,
            clock,
            config,
            observations: runtime.observations,
            handoff: runtime.handoff,
            invalidate_existence: runtime.invalidate_existence,
            control_rx: inboxes.control_rx,
            worker_rx: inboxes.worker_rx,
            client_rx: inboxes.client_rx,
            client_tx: inboxes.client_tx,
            control_closed: false,
            worker_closed: false,
            client_closed: false,
            worker_outbound: HashMap::new(),
            op_watches: HashMap::new(),
            op_meta: HashMap::new(),
            effects: Effects::with_capacity(64),
            effect_scratch: Effects::with_capacity(64),
            terminal_cleanup: Vec::new(),
            terminal_operations: HashMap::new(),
            terminal_order: VecDeque::new(),
            pending_audit: VecDeque::new(),
            pending_completion: None,
            shutdown_reply: None,
        }
    }

    fn now(&self) -> Now {
        Now {
            tick: self.clock.monotonic(),
            wall: self.clock.wall(),
        }
    }

    async fn run_loop(&mut self) {
        while !(self.control_closed && self.worker_closed && self.client_closed) {
            if self.run_iteration().await {
                break;
            }
        }
    }

    async fn run_iteration(&mut self) -> bool {
        let next_deadline = self.scheduler.next_deadline();
        let sleep = next_deadline.map(|deadline| self.clock.sleep_until(deadline));
        let has_deadline = sleep.is_some();
        let timer_fut = async {
            if let Some(sleep) = sleep {
                sleep.await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(timer_fut);

        let control_open = !self.control_closed;
        let worker_open = !self.worker_closed;
        let client_open = !self.client_closed;

        let mut control = None;
        let mut worker = None;
        let mut client = None;
        tokio::select! {
            () = &mut timer_fut, if has_deadline => {}
            msg = self.control_rx.recv(), if control_open => {
                control = msg;
                self.control_closed = control.is_none();
            }
            msg = self.worker_rx.recv(), if worker_open => {
                worker = msg;
                self.worker_closed = worker.is_none();
            }
            msg = self.client_rx.recv(), if client_open => {
                client = msg;
                self.client_closed = client.is_none();
            }
        }
        self.advance_due_timers(self.now());
        if let Some(first) = control.or_else(|| self.control_rx.try_recv().ok())
            && self.handle_control_batch(first)
        {
            return true;
        }
        if let Some(first) = worker.or_else(|| self.worker_rx.try_recv().ok()) {
            self.handle_worker_batch(first);
        }
        if let Some(first) = client.or_else(|| self.client_rx.try_recv().ok()) {
            self.handle_client_batch(first);
        }
        false
    }

    fn advance_due_timers(&mut self, now: Now) {
        if self
            .scheduler
            .next_deadline()
            .is_some_and(|deadline| deadline <= now.tick)
        {
            self.scheduler.on_timer(now, &mut self.effects);
            self.drain_effects(now);
        }
    }

    /// Drains `first` and all currently queued `control_rx` messages (`§5.10`).
    /// Returns `true` when [`ControlMsg::Shutdown`] requests actor termination.
    fn handle_control_batch(&mut self, first: ControlMsg) -> bool {
        if self.handle_control_msg(first) {
            return true;
        }
        loop {
            match self.control_rx.try_recv() {
                Ok(msg) => {
                    if self.handle_control_msg(msg) {
                        return true;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => return false,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.control_closed = true;
                    return false;
                }
            }
        }
    }

    fn handle_control_msg(&mut self, msg: ControlMsg) -> bool {
        let now = self.now();
        match msg {
            ControlMsg::Snapshot { mut into, reply } => {
                self.advance_due_timers(now);
                self.scheduler.snapshot(now, &mut into);
                let _ = reply.send(into);
            }
            ControlMsg::Actions {
                limits,
                mut into,
                reply,
            } => {
                self.advance_due_timers(now);
                self.scheduler.actions(limits, &mut into);
                let _ = reply.send(into);
            }
            ControlMsg::SetDraining {
                fleet,
                instance,
                draining,
                reply,
            } => {
                let res = self.scheduler.set_draining(
                    now,
                    &fleet,
                    &instance,
                    draining,
                    &mut self.effects,
                );
                self.drain_effects(now);
                let _ = reply.send(res);
            }
            ControlMsg::RetainDrains {
                now: tick,
                grace,
                census,
                reply,
            } => {
                self.scheduler.retain_drains(tick, grace, &census);
                let _ = reply.send(());
            }
            ControlMsg::StaleBuildDrains { reply } => {
                let affected = self.scheduler.stale_build_drains(&mut self.effects);
                self.drain_effects(now);
                let _ = reply.send(affected);
            }
            ControlMsg::HandOff {
                public_complete,
                into,
                reply,
            } => {
                self.handle_hand_off(now, public_complete, into, reply);
            }
            ControlMsg::RefreshDrain {
                public_complete,
                into,
                reply,
            } => {
                self.handle_refresh_drain(now, public_complete, into, reply);
            }
            ControlMsg::LearnedHistory {
                now: wall,
                mut into,
                reply,
            } => {
                self.scheduler.learned_history(wall, &mut into);
                let _ = reply.send(into);
            }
            ControlMsg::RestoreLearnedHistory {
                now: wall,
                records,
                reply,
            } => {
                let res = self.scheduler.restore_learned_history(wall, &records);
                let _ = reply.send(res);
            }
            ControlMsg::SetPeerCapacity {
                observations,
                reply,
            } => {
                let res = self.scheduler.set_peer_capacity_observations(observations);
                let _ = reply.send(res);
            }
            ControlMsg::RefreshLocalCapacity { reply } => {
                let changed = self.scheduler.refresh_local_capacity_observations();
                let _ = reply.send(changed);
            }
            ControlMsg::Shutdown { reply } => {
                self.shutdown_reply = Some(reply);
                return true;
            }
        }
        false
    }

    fn handle_hand_off(
        &mut self,
        now: Now,
        public_complete: bool,
        mut into: ControllerDrain,
        reply: oneshot::Sender<Result<ControllerDrain, HandOffError>>,
    ) {
        self.advance_due_timers(now);
        let effective_public_complete = if let Some(handoff) = &self.handoff {
            handoff.begin();
            handoff.public_complete()
        } else {
            public_complete
        };
        let res =
            self.scheduler
                .hand_off(now, effective_public_complete, &mut into, &mut self.effects);
        self.drain_effects(now);
        let _ = reply.send(res.map(|()| into));
    }

    fn handle_refresh_drain(
        &mut self,
        now: Now,
        public_complete: bool,
        mut into: ControllerDrain,
        reply: oneshot::Sender<Result<ControllerDrain, HandOffError>>,
    ) {
        self.advance_due_timers(now);
        let effective_public_complete = self
            .handoff
            .as_ref()
            .map_or(public_complete, HandOff::public_complete);
        let res = if self.scheduler.handoff().is_active() {
            self.scheduler
                .hand_off(now, effective_public_complete, &mut into, &mut self.effects)
        } else {
            let mut rehome = Vec::new();
            self.scheduler.handoff().refresh_drain(
                now.wall,
                self.scheduler.workers(),
                effective_public_complete,
                |worker| self.scheduler.attempts().latest_attempt_deadline(worker),
                &mut into,
                &mut rehome,
            )
        };
        self.drain_effects(now);
        let _ = reply.send(res.map(|()| into));
    }

    fn handle_worker_batch(&mut self, first: WorkerMsg) {
        self.handle_worker_msg(first);
        let batch_limit = self.config.scheduler_batch.get();
        let mut processed = 1;
        while processed < batch_limit {
            match self.worker_rx.try_recv() {
                Ok(msg) => {
                    self.handle_worker_msg(msg);
                    processed += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.worker_closed = true;
                    break;
                }
            }
        }
    }

    fn handle_worker_msg(&mut self, msg: WorkerMsg) {
        let now = self.now();
        match msg {
            WorkerMsg::Register {
                registration,
                reply,
            } => {
                match self
                    .scheduler
                    .register(now, *registration, &mut self.effects)
                {
                    Ok(key) => {
                        let Some(worker_id) =
                            self.scheduler.workers().get(key).map(|w| w.id.clone())
                        else {
                            self.drain_effects(now);
                            return;
                        };
                        let (out_tx, out_rx) =
                            mpsc::channel(self.config.worker_outbound_depth.get());
                        self.worker_outbound.insert(key, out_tx);
                        let registered = RegisteredWorker {
                            key,
                            worker_id,
                            outbound: out_rx,
                        };
                        if reply.send(Ok(registered)).is_err() {
                            // Caller dropped before taking the outbound stream: evict cleanly.
                            self.worker_outbound.remove(&key);
                            self.scheduler.on_worker_lost(now, key, &mut self.effects);
                        }
                        self.drain_effects(now);
                    }
                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                }
            }
            WorkerMsg::Event {
                worker,
                event,
                completion,
            } => {
                self.pending_completion = completion.map(|payload| *payload);
                self.scheduler
                    .on_worker(now, worker, event, &mut self.effects);
                self.drain_effects(now);
            }
            WorkerMsg::Lost { worker } => {
                self.worker_outbound.remove(&worker);
                self.scheduler
                    .on_worker_lost(now, worker, &mut self.effects);
                self.drain_effects(now);
            }
        }
    }

    fn handle_client_batch(&mut self, first: ClientMsg) {
        self.handle_client_msg(first);
        let batch_limit = self.config.scheduler_batch.get();
        let mut processed = 1;
        while processed < batch_limit {
            match self.client_rx.try_recv() {
                Ok(msg) => {
                    self.handle_client_msg(msg);
                    processed += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.client_closed = true;
                    break;
                }
            }
        }
    }

    fn handle_client_msg(&mut self, msg: ClientMsg) {
        let now = self.now();
        match msg {
            ClientMsg::Execute { spec, reply } => {
                let ExecuteSpec {
                    request,
                    caller,
                    origin,
                    inline,
                    timeout,
                } = *spec;
                let platform = request.platform_properties.clone();
                let track_watcher = request.initial_watchers > 0;
                match self.scheduler.execute(now, request, &mut self.effects) {
                    Ok(key) => {
                        let Some(op_name) = self
                            .scheduler
                            .operations()
                            .get(key)
                            .map(|op| op.name.clone())
                        else {
                            self.drain_effects(now);
                            return;
                        };
                        self.pending_audit.push_back(AuditContext {
                            caller,
                            operation: op_name.clone(),
                        });
                        let receiver = if let Some(tx) = self.op_watches.get(&key) {
                            tx.subscribe()
                        } else {
                            let (tx, rx) =
                                watch::channel(OperationWatchState::queued(op_name.clone()));
                            self.op_watches.insert(key, tx);
                            self.op_meta.insert(
                                key,
                                OperationShellMeta {
                                    platform,
                                    origin,
                                    inline,
                                    timeout,
                                    queued_wall: now.wall,
                                },
                            );
                            rx
                        };
                        self.drain_effects(now);
                        let watch = OperationWatch::new(
                            key,
                            op_name,
                            receiver,
                            self.client_tx.clone(),
                            track_watcher,
                        );
                        let _ = reply.send(Ok(watch));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                }
            }
            ClientMsg::Watch { operation, reply } => {
                let watch = self.handle_watch(now, &operation);
                let _ = reply.send(watch);
            }
            ClientMsg::Unwatch { operation } => {
                self.scheduler.unwatch(now, operation, &mut self.effects);
                self.drain_effects(now);
            }
            ClientMsg::Cancel { operation, reply } => {
                self.scheduler.cancel(now, operation, &mut self.effects);
                self.drain_effects(now);
                let _ = reply.send(());
            }
        }
    }

    fn handle_watch(&mut self, now: Now, operation: &OperationName) -> Option<OperationWatch> {
        if let Some(key) = self.scheduler.operations().lookup_name(operation)
            && let Some(tx) = self.op_watches.get(&key)
        {
            self.scheduler.watch(now, operation)?;
            return Some(OperationWatch::new(
                key,
                operation.clone(),
                tx.subscribe(),
                self.client_tx.clone(),
                true,
            ));
        }
        let retained = self.terminal_operations.get(operation)?;
        let (_tx, rx) = watch::channel(retained.state.clone());
        Some(OperationWatch::new(
            retained.key,
            operation.clone(),
            rx,
            self.client_tx.clone(),
            false,
        ))
    }

    fn drain_effects(&mut self, now: Now) {
        let mut batch = std::mem::take(&mut self.effect_scratch);
        while !self.effects.is_empty() {
            std::mem::swap(&mut self.effects, &mut batch);
            self.terminal_cleanup.clear();
            for effect in batch.drain() {
                self.apply_effect(now, effect);
            }
            // Completion observations in this batch still need the operation's origin.
            for op_key in self.terminal_cleanup.drain(..) {
                self.op_meta.remove(&op_key);
            }
            self.pending_completion = None;
            self.pending_audit.clear();
        }
        self.effect_scratch = batch;
    }

    fn apply_effect(&mut self, now: Now, effect: Effect) {
        match effect {
            Effect::AssignWorker {
                worker,
                attempt_id,
                operation,
                cpu_weight,
                assign_seq,
                reservation,
                deadline_wall,
                ..
            } => {
                if let Some(assign) = self.build_assign(
                    &attempt_id,
                    operation,
                    cpu_weight,
                    assign_seq,
                    reservation,
                    deadline_wall,
                ) {
                    self.send_to_worker(worker, WorkerOutbound::Assign(Box::new(assign)), now);
                }
            }
            Effect::KillWorker {
                worker,
                attempt_id,
                reason,
                ..
            } => {
                let kill = proto::Kill {
                    attempt: attempt_id.to_string(),
                    reason: map_kill_reason(reason) as i32,
                };
                self.send_to_worker(worker, WorkerOutbound::Kill(kill), now);
            }
            Effect::SetWorkerDraining { worker, draining } => {
                self.send_to_worker(worker, WorkerOutbound::SetDraining(draining), now);
            }
            Effect::RehomeWorker { worker, reason } => {
                if let Some(tx) = self.worker_outbound.remove(&worker)
                    && tx.try_send(WorkerOutbound::Rehome(reason)).is_err()
                {
                    self.scheduler
                        .on_worker_lost(now, worker, &mut self.effects);
                }
            }
            Effect::CloseWorker { worker } => {
                self.worker_outbound.remove(&worker);
            }
            Effect::NotifyWatchers { operation, event } => {
                if let OperationUpdate::Failed(gang_scheduler::TerminalFailure::MissingBlobs {
                    digests,
                    ..
                }) = &event
                    && let Some(invalidate) = &self.invalidate_existence
                {
                    invalidate(digests);
                }
                self.notify_watchers(operation, event);
            }
            Effect::AuditGateTier(audit) => {
                self.emit_gate_tier_audit(audit);
            }
            Effect::RecordObservation(obs) => {
                self.record_observation(obs);
            }
        }
    }

    fn build_assign(
        &self,
        attempt_id: &AttemptId,
        operation: OperationKey,
        cpu_weight: CpuWeight,
        assign_seq: u64,
        reservation: ActionResources,
        deadline_wall: Timestamp,
    ) -> Option<proto::Assign> {
        let op = self.scheduler.operations().get(operation)?;
        let meta = self.op_meta.get(&operation);
        let (action, command) = select_inline_payload(
            meta.and_then(|m| m.inline.as_ref()),
            self.config.assign_inline_max,
        );
        let mut platform: HashMap<String, String> = meta.map_or_else(HashMap::new, |m| {
            m.platform
                .iter()
                .filter(|(k, v)| {
                    !(k.as_str() == gang_types::property::NETWORK
                        && (v.is_empty()
                            || v.as_str() == gang_types::NetworkRequest::NotRequested.as_str()))
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        });
        if reservation.memory.get() > 0 {
            platform.insert(
                gang_types::property::MEMORY_KB.to_owned(),
                reservation.memory.get().div_ceil(1024).to_string(),
            );
        }
        if reservation.disk.get() > 0 {
            platform.insert(
                gang_types::property::DISK_KB.to_owned(),
                reservation.disk.get().div_ceil(1024).to_string(),
            );
        }
        if reservation.cpu_millicores > 0 {
            platform.insert(
                gang_types::property::CPU_MILLICORES.to_owned(),
                reservation.cpu_millicores.to_string(),
            );
        }
        let timeout = meta.and_then(|m| m.timeout).map(duration_to_proto);
        Some(proto::Assign {
            attempt: attempt_id.to_string(),
            operation: op.name.as_str().to_owned(),
            action_digest: gang_reapi::to_proto(&op.action_digest).ok(),
            action,
            command,
            platform,
            reservation: Some(proto::Resources {
                cpu_millicores: reservation.cpu_millicores,
                memory_bytes: reservation.memory.get(),
                disk_bytes: reservation.disk.get(),
            }),
            attempt_deadline: Some(timestamp_to_proto(deadline_wall)),
            timeout,
            target: op
                .target
                .as_ref()
                .map_or_else(String::new, |t| t.as_str().to_owned()),
            mnemonic: op
                .mnemonic
                .as_ref()
                .map_or_else(String::new, |m| m.as_str().to_owned()),
            skip_cache_lookup: op.skip_cache_lookup,
            do_not_cache: op.do_not_cache,
            cpu_weight: cpu_weight.get(),
            assign_seq,
        })
    }

    fn send_to_worker(&mut self, worker: WorkerKey, msg: WorkerOutbound, now: Now) {
        let evict_reason = match self.worker_outbound.get(&worker) {
            Some(tx) => match tx.try_send(msg) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(_)) => Some("outbound channel full"),
                Err(mpsc::error::TrySendError::Closed(_)) => Some("outbound channel closed"),
            },
            None => Some("outbound channel missing"),
        };
        if let Some(reason) = evict_reason {
            tracing::warn!(
                worker_index = worker.index(),
                worker_generation = worker.generation(),
                reason,
                "evicting stalled or disconnected worker"
            );
            self.worker_outbound.remove(&worker);
            self.scheduler
                .on_worker_lost(now, worker, &mut self.effects);
        }
    }

    fn notify_watchers(&mut self, operation: OperationKey, event: OperationUpdate) {
        let terminal = is_terminal_update(&event);
        let (mut result, status) = if matches!(event, OperationUpdate::Completed) {
            self.pending_completion
                .as_mut()
                .map_or((None, None), |p| (p.result.take(), p.status.take()))
        } else {
            (None, None)
        };
        if let Some(action_result) = result.as_mut()
            && let Some(exec_meta) = action_result.execution_metadata.as_mut()
            && exec_meta.queued_timestamp.is_none()
            && let Some(op_meta) = self.op_meta.get(&operation)
        {
            exec_meta.queued_timestamp = Some(timestamp_to_proto(op_meta.queued_wall));
        }

        if terminal {
            self.terminal_cleanup.push(operation);
            if let Some(tx) = self.op_watches.remove(&operation) {
                let op_name = tx.borrow().operation.clone();
                let state = OperationWatchState {
                    operation: op_name.clone(),
                    update: event,
                    result,
                    status,
                };
                if tx.receiver_count() > 0 {
                    tx.send_replace(state.clone());
                }
                let limit = self.scheduler.policy().max_retained_terminal_operations;
                while self.terminal_order.len() >= limit {
                    let Some((key, oldest)) = self.terminal_order.pop_front() else {
                        break;
                    };
                    if self
                        .terminal_operations
                        .get(&oldest)
                        .is_some_and(|entry| entry.key == key)
                    {
                        self.terminal_operations.remove(&oldest);
                    }
                }
                self.terminal_order.push_back((operation, op_name.clone()));
                self.terminal_operations.insert(
                    op_name,
                    RetainedOperation {
                        key: operation,
                        state,
                    },
                );
            }
        } else if let Some(tx) = self.op_watches.get(&operation) {
            let current = tx.borrow();
            if current.update == event {
                return;
            }
            let op_name = current.operation.clone();
            drop(current);
            tx.send_replace(OperationWatchState {
                operation: op_name,
                update: event,
                result,
                status,
            });
        }
    }

    fn emit_gate_tier_audit(&mut self, audit: GateAudit) {
        let context = self.pending_audit.pop_front();
        let (provider, principal, operation) = match &context {
            Some(ctx) => (
                ctx.caller
                    .as_ref()
                    .map_or("anonymous", |id| id.provider.as_str()),
                ctx.caller
                    .as_ref()
                    .map_or("anonymous", |id| id.principal.as_str()),
                ctx.operation.as_str(),
            ),
            None => ("anonymous", "anonymous", ""),
        };
        tracing::info!(
            target: "gang::audit::gate_tier",
            provider,
            principal,
            priority = audit.priority,
            operation,
            "admitted gate-tier execution request"
        );
    }

    fn record_observation(&self, obs: CompletedObservation) {
        let Some(sink) = &self.observations else {
            return;
        };
        let Some(origin) = self
            .op_meta
            .get(&obs.operation)
            .and_then(|meta| meta.origin.clone())
        else {
            return;
        };
        let (exit_code, error) = self
            .pending_completion
            .as_ref()
            .map_or((0, false), |p| (p.exit_code, p.error));
        let times = ExecutionTimes {
            queued: Phase::reported(obs.queued_at, obs.started_at),
            input_fetch: obs
                .execution_timestamps
                .and_then(|(exec_start, _)| Phase::reported(obs.started_at, exec_start)),
            executing: obs
                .execution_timestamps
                .and_then(|(exec_start, exec_end)| Phase::reported(exec_start, exec_end)),
            output_upload: obs
                .execution_timestamps
                .and_then(|(_, exec_end)| Phase::reported(exec_end, obs.completed_at)),
            whole: Phase::reported(obs.queued_at, obs.completed_at),
        };
        sink.record(Observation::Executed(ObservedExecuted {
            origin,
            digest: obs.action_digest.to_string(),
            worker: Some(obs.worker_id),
            exit_code,
            error,
            times,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::TestClock;
    use gang_scheduler::{Learned, SchedulerPolicy};
    use gang_types::{
        Digest, FleetActionPolicy, FleetCatalog, FleetName, FleetProperties, Isa, Resources,
        SourceId, property,
    };

    fn actor() -> (
        ControllerActor<TestClock>,
        mpsc::Sender<ControlMsg>,
        mpsc::Sender<WorkerMsg>,
    ) {
        let catalog = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("small").expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("environment"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )])
        .expect("catalog");
        let policy = SchedulerPolicy {
            max_retained_terminal_operations: 2,
            client_action_timeout: Duration::from_secs(15),
            ..SchedulerPolicy::default()
        };
        let learned =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
        let scheduler = Scheduler::new(policy, &catalog, learned).expect("scheduler");
        let (control_tx, control_rx) = mpsc::channel(64);
        let (worker_tx, worker_rx) = mpsc::channel(64);
        let (client_tx, client_rx) = mpsc::channel(64);
        let actor = ControllerActor::new(
            scheduler,
            Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000))),
            ActorConfig {
                scheduler_batch: NonZeroUsize::new(2).expect("batch"),
                ..ActorConfig::default()
            },
            ControllerRuntime::new(),
            ActorInboxes {
                control_rx,
                worker_rx,
                client_rx,
                client_tx,
            },
        );
        (actor, control_tx, worker_tx)
    }

    fn execute(actor: &mut ControllerActor<TestClock>, name: &str) -> OperationWatch {
        let request = AdmissionRequest {
            name: name.parse().expect("operation"),
            action_digest: Digest::from_bytes(name.as_bytes()),
            command_digest: "command".to_owned(),
            platform_properties: [(property::ISA.to_owned(), "x86-64".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: true,
            do_not_cache: true,
            source: SourceId::from_identity_str("test:client"),
            build: None,
            target: None,
            mnemonic: None,
            priority: 0,
            initial_watchers: 0,
        };
        let mut spec = ExecuteSpec::new(request);
        spec.inline = Some(InlineActionPayload {
            action: RawBytes::from_static(b"action"),
            command: RawBytes::from_static(b"command"),
        });
        let (reply, mut receiver) = oneshot::channel();
        actor.handle_client_msg(ClientMsg::Execute {
            spec: Box::new(spec),
            reply,
        });
        receiver.try_recv().expect("reply").expect("admitted")
    }

    #[test]
    fn clientless_queued_expiration_releases_shell_metadata() {
        let (mut actor, _control_tx, _worker_tx) = actor();
        let watch = execute(&mut actor, "operations/expired");
        let key = watch.key();
        assert!(actor.op_meta.get(&key).expect("metadata").inline.is_some());
        assert!(actor.op_watches.contains_key(&key));
        drop(watch);
        actor.clock.advance(Duration::from_secs(15));
        actor.advance_due_timers(actor.now());
        assert!(
            !actor.op_watches.contains_key(&key),
            "expiration must release watch sender"
        );
        assert!(
            !actor.op_meta.contains_key(&key),
            "expiration must release inline protos"
        );
        let replay = actor
            .handle_watch(actor.now(), &"operations/expired".parse().expect("name"))
            .expect("retained cancellation");
        assert_eq!(replay.current().update, OperationUpdate::Cancelled);
    }

    #[test]
    fn terminal_replay_evicts_oldest_at_scheduler_retention_limit() {
        let (mut actor, _control_tx, _worker_tx) = actor();
        for name in ["operations/first", "operations/second", "operations/third"] {
            let watch = execute(&mut actor, name);
            actor
                .scheduler
                .cancel(actor.now(), watch.key(), &mut actor.effects);
            actor.drain_effects(actor.now());
            assert_eq!(watch.current().update, OperationUpdate::Cancelled);
            assert!(!actor.op_watches.contains_key(&watch.key()));
            assert!(!actor.op_meta.contains_key(&watch.key()));
        }
        assert_eq!(actor.terminal_operations.len(), 2);
        assert_eq!(actor.terminal_order.len(), 2);
        assert!(
            actor
                .handle_watch(actor.now(), &"operations/first".parse().expect("name"))
                .is_none()
        );
        for name in ["operations/second", "operations/third"] {
            let watch = actor
                .handle_watch(actor.now(), &name.parse().expect("name"))
                .expect("retained operation");
            assert_eq!(watch.current().update, OperationUpdate::Cancelled);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reused_expired_name_keeps_replay_current_and_retention_bounded() {
        let (mut actor, _control_tx, _worker_tx) = actor();
        let name: OperationName = "operations/reused".parse().expect("name");
        for _ in 0..4 {
            let watch = execute(&mut actor, name.as_str());
            let replay = actor
                .handle_watch(actor.now(), &name)
                .expect("active watch");
            assert_eq!(replay.current().update, OperationUpdate::Queued);
            replay.close().await;
            let unwatch = actor.client_rx.try_recv().expect("unwatch");
            actor.handle_client_msg(unwatch);
            drop(watch);
            actor.clock.advance(Duration::from_secs(15));
            actor.advance_due_timers(actor.now());
            assert!(actor.terminal_operations.len() <= 2);
            assert!(actor.terminal_order.len() <= 2);
            let terminal = actor
                .handle_watch(actor.now(), &name)
                .expect("retained cancellation");
            assert_eq!(terminal.current().update, OperationUpdate::Cancelled);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_backlog_services_client_in_same_iteration() {
        let (mut actor, _control_tx, worker_tx) = actor();
        let name: OperationName = "operations/cancel".parse().expect("name");
        let watch = execute(&mut actor, name.as_str());
        // Register first so the flood uses valid worker heartbeats.
        let registration = WorkerRegistration {
            instance: Arc::new(InstanceId::try_new("i-fair").expect("instance")),
            fleet: FleetName::try_new("small").expect("fleet"),
            gang_version: actor.scheduler.policy().expected_gang_version.clone(),
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores: 8_000,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            ceilings: ActionResources {
                cpu_millicores: 8_000,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            max_inflight: None,
            single_use: false,
            environment: Some("host:test".parse().expect("environment")),
            uuid_bytes: [1; 16],
        };
        let (reply, mut receiver) = oneshot::channel();
        actor.handle_worker_msg(WorkerMsg::Register {
            registration: Box::new(registration),
            reply,
        });
        let worker = receiver.try_recv().expect("reply").expect("registered");
        for _ in 0..8 {
            worker_tx
                .try_send(WorkerMsg::Event {
                    worker: worker.key,
                    event: WorkerEvent::KeepAlive {
                        cache: None,
                        memory_capacity: None,
                        load: None,
                        inflight: Vec::new(),
                    },
                    completion: None,
                })
                .expect("heartbeat");
        }
        let (reply, mut cancelled) = oneshot::channel();
        actor
            .client_tx
            .try_send(ClientMsg::Cancel {
                operation: watch.key(),
                reply,
            })
            .expect("cancel");
        assert!(!actor.run_iteration().await);
        assert_eq!(
            actor.worker_rx.len(),
            6,
            "exactly one worker batch must drain"
        );
        cancelled
            .try_recv()
            .expect("client cancel must finish in the same iteration");
        assert_eq!(watch.current().update, OperationUpdate::Cancelled);
    }
}
