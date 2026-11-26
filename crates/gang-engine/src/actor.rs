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
    ExecuteError, GateAudit, HandOffError, KillReason, Now, OperationKey, OperationStage,
    OperationUpdate, RegisterError, RehomeReason, Scheduler, SketchError, WorkerEvent, WorkerKey,
    WorkerRegistration,
};
use gang_spi::{
    ActionsView, ControllerDrain, Executed as ObservedExecuted, ExecutionTimes,
    FleetMemoryCapacity, LearnedHistory, ListingLimits, Observation, ObservationSink, Origin,
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

/// Optional shell collaborators wired into a running controller actor.
#[derive(Clone, Default)]
pub struct ControllerRuntime {
    /// Optional build observation sink for completed attempts (`Effect::RecordObservation`).
    pub observations: Option<ObservationSink>,
    /// Optional cooperative handoff coordinator (`handoff.rs`).
    pub(crate) handoff: Option<HandOff>,
    /// Optional acquisition timeout bounding control-plane enqueueing (`§5.10`).
    pub control_acquire_timeout: Option<NonZeroDuration>,
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
    /// Replace peer-local memory capacity observations.
    SetPeerMemory {
        /// Peer observations by fleet index.
        observations: Vec<FleetMemoryCapacity>,
        /// Reply channel reporting whether effective capacity changed or [`CapacityError`].
        reply: oneshot::Sender<Result<bool, CapacityError>>,
    },
    /// Recompute local observed fleet memory capacities from connected workers.
    RefreshLocalMemory {
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
    let mut actor = ControllerActor {
        scheduler,
        clock,
        config,
        observations: runtime.observations,
        handoff: runtime.handoff,
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
        pending_audit: VecDeque::new(),
        pending_completion: None,
        shutdown_reply: None,
    };

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

struct ControllerActor<C> {
    scheduler: Scheduler,
    clock: Arc<C>,
    config: ActorConfig,
    observations: Option<ObservationSink>,
    handoff: Option<HandOff>,
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
    pending_audit: VecDeque<AuditContext>,
    pending_completion: Option<CompletionPayload>,
    shutdown_reply: Option<oneshot::Sender<()>>,
}

impl<C: Clock> ControllerActor<C> {
    fn now(&self) -> Now {
        Now {
            tick: self.clock.monotonic(),
            wall: self.clock.wall(),
        }
    }

    async fn run_loop(&mut self) {
        loop {
            if self.control_closed && self.worker_closed && self.client_closed {
                break;
            }

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

            tokio::select! {
                biased;
                () = &mut timer_fut, if has_deadline => {
                    let now = self.now();
                    self.scheduler.on_timer(now, &mut self.effects);
                    self.drain_effects(now);
                }
                msg = self.control_rx.recv(), if control_open => {
                    match msg {
                        Some(first) => {
                            if self.handle_control_batch(first) {
                                break;
                            }
                        }
                        None => self.control_closed = true,
                    }
                }
                msg = self.worker_rx.recv(), if worker_open => {
                    match msg {
                        Some(first) => self.handle_worker_batch(first),
                        None => self.worker_closed = true,
                    }
                }
                msg = self.client_rx.recv(), if client_open => {
                    match msg {
                        Some(first) => self.handle_client_batch(first),
                        None => self.client_closed = true,
                    }
                }
            }
        }
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
            ControlMsg::SetPeerMemory {
                observations,
                reply,
            } => {
                let res = self.scheduler.set_peer_memory_observations(observations);
                let _ = reply.send(res);
            }
            ControlMsg::RefreshLocalMemory { reply } => {
                let changed = self.scheduler.refresh_local_memory_observations();
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
        let key = self.scheduler.watch(now, operation)?;
        let op = self.scheduler.operations().get(key)?;
        let op_name = op.name.clone();
        if let Some(tx) = self.op_watches.get(&key) {
            return Some(OperationWatch::new(
                key,
                op_name,
                tx.subscribe(),
                self.client_tx.clone(),
                true,
            ));
        }
        // Operation already reached a terminal stage and was pruned from active watches.
        let terminal_update = match op.stage {
            OperationStage::Completed => OperationUpdate::Completed,
            OperationStage::Cancelled => OperationUpdate::Cancelled,
            OperationStage::Queued | OperationStage::Running => OperationUpdate::Queued,
        };
        let (_tx, rx) = watch::channel(OperationWatchState {
            operation: op_name.clone(),
            update: terminal_update,
            result: None,
            status: None,
        });
        Some(OperationWatch::new(
            key,
            op_name,
            rx,
            self.client_tx.clone(),
            true,
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
                reservation,
                deadline_wall,
                ..
            } => {
                if let Some(assign) = self.build_assign(
                    &attempt_id,
                    operation,
                    cpu_weight,
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
        reservation: ActionResources,
        deadline_wall: Timestamp,
    ) -> Option<proto::Assign> {
        let op = self.scheduler.operations().get(operation)?;
        let meta = self.op_meta.get(&operation);
        let (action, command) = select_inline_payload(
            meta.and_then(|m| m.inline.as_ref()),
            self.config.assign_inline_max,
        );
        let platform = meta.map_or_else(HashMap::new, |m| {
            m.platform
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        });
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
        let (result, status) = if matches!(event, OperationUpdate::Completed) {
            self.pending_completion
                .as_ref()
                .map_or((None, None), |p| (p.result.clone(), p.status.clone()))
        } else {
            (None, None)
        };

        if terminal {
            self.terminal_cleanup.push(operation);
            if let Some(tx) = self.op_watches.remove(&operation) {
                let op_name = tx.borrow().operation.clone();
                tx.send_replace(OperationWatchState {
                    operation: op_name,
                    update: event,
                    result,
                    status,
                });
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
