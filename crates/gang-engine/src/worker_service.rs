//! Bidirectional `leadingterm.gang.worker.v1.WorkerService` server session bridge (`§6.1`, `§6.2`,
//! `§8.2`).
//!
//! Bridges each incoming worker gRPC `Connect` stream to the single-owner controller actor via
//! [`ControllerHandle`]:
//! - Enforces a bounded registration handshake expecting `WorkerMessage::Register` as frame 1,
//!   validating all wire fields into [`WorkerRegistration`].
//! - Emits `ControllerMessage::Registered` (with controller-minted [`WorkerId`] and negotiated
//!   `keepalive_interval`) as the first outbound frame, followed in FIFO order by scheduler
//!   directives (`SetDraining`, `Assign`, `Kill`, `Rehome`) from the worker's bounded outbound
//!   channel.
//! - Forwards inbound worker frames (`Accept`, `Decline`, `Executed`, `Completed`, `KeepAlive`,
//!   `GoingAway`) onto the controller's worker lane, carrying `Executed` timestamps and exit code
//!   into the paired `Completed` [`CompletionPayload`].
//! - Guarantees exact-once [`ControllerHandle::worker_lost`] delivery and inbound-task cleanup on
//!   every termination path (client EOF, transport error, stream drop, protocol violation,
//!   `Rehome`, keepalive timeout, or controller shutdown) via an RAII drop guard.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::stream::{self, BoxStream};
use gang_clock::{Clock, SystemClock, Timestamp};
use gang_reapi::build::bazel::remote::execution::v2::ActionResult;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_reapi::leadingterm::gang::worker::v1::completed::Outcome as CompletedOutcome;
use gang_reapi::leadingterm::gang::worker::v1::controller_message::Kind as ControllerKind;
use gang_reapi::leadingterm::gang::worker::v1::worker_message::Kind as WorkerKind;
use gang_reapi::leadingterm::gang::worker::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use gang_scheduler::{
    AttemptUsage, CompletionStatus, DeclineReason, KillCause, RegisterError, RegistrationError,
    WorkerCompleted, WorkerEvent, WorkerExecuted, WorkerKey, WorkerRegistration,
};
use gang_spi::{ContentStore, WorkerCacheStats};
use gang_types::{
    ActionEnvironment, ActionResources, AttemptId, Bytes, Digest, FleetName, InstanceId,
    NonZeroDuration, WorkerId,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status, Streaming};

use crate::actor::{CompletionPayload, WorkerOutbound};
use crate::controller::{ControllerHandle, ControllerRegisterError};

/// Default application `KeepAlive` cadence advertised in `ControllerMessage::Registered` (5 s).
pub const DEFAULT_WORKER_KEEPALIVE_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(5);

/// Default timeout for receiving a worker's initial `WorkerMessage::Register` frame (10 s).
pub const DEFAULT_REGISTRATION_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// Minimum non-zero advertised `KeepAlive` cadence derived from `worker_timeout`.
const MIN_KEEPALIVE_INTERVAL: Duration = Duration::from_millis(1);

/// Upper bound on per-session cached `Executed` metadata awaiting its paired `Completed` frame.
const MAX_TRACKED_EXECUTED_META: usize = 4096;

type ExistenceInvalidator = dyn Fn(&[Digest]) + Send + Sync;

/// Configuration for [`WorkerGrpcService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerServiceConfig {
    /// Expected application `KeepAlive` interval advertised in `ControllerMessage::Registered`.
    pub keepalive_interval: NonZeroDuration,
    /// Maximum time allowed for the worker to send its initial `Register` frame.
    pub registration_timeout: NonZeroDuration,
}

impl Default for WorkerServiceConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: DEFAULT_WORKER_KEEPALIVE_INTERVAL,
            registration_timeout: DEFAULT_REGISTRATION_TIMEOUT,
        }
    }
}

impl WorkerServiceConfig {
    /// Derives [`WorkerServiceConfig`] from `[server] worker_timeout`, advertising a `KeepAlive`
    /// cadence of `worker_timeout / 2` (floored to 1 ms) and bounding handshake wait by
    /// `worker_timeout`.
    #[must_use]
    pub fn from_worker_timeout(worker_timeout: NonZeroDuration) -> Self {
        let half = (worker_timeout.get() / 2).max(MIN_KEEPALIVE_INTERVAL);
        let keepalive_interval = NonZeroDuration::try_new(half).unwrap_or(worker_timeout);
        Self {
            keepalive_interval,
            registration_timeout: worker_timeout,
        }
    }
}

/// `leadingterm.gang.worker.v1.WorkerService` gRPC server backed by [`ControllerHandle`].
#[derive(Clone)]
pub struct WorkerGrpcService {
    controller: ControllerHandle,
    config: WorkerServiceConfig,
    clock: Arc<dyn Clock>,
    invalidate_existence: Arc<ExistenceInvalidator>,
}

impl fmt::Debug for WorkerGrpcService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerGrpcService")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl WorkerGrpcService {
    /// Creates a [`WorkerGrpcService`] backed by `controller` and `config` using [`SystemClock`].
    #[must_use]
    pub fn new<C: ContentStore>(
        controller: ControllerHandle,
        config: WorkerServiceConfig,
        content: Arc<C>,
    ) -> Self {
        Self::with_clock(controller, config, Arc::new(SystemClock::new()), content)
    }

    /// Creates a [`WorkerGrpcService`] with [`WorkerServiceConfig::default`] and [`SystemClock`].
    #[must_use]
    pub fn with_defaults<C: ContentStore>(controller: ControllerHandle, content: Arc<C>) -> Self {
        Self::new(controller, WorkerServiceConfig::default(), content)
    }

    /// Creates a [`WorkerGrpcService`] with an explicit injectable [`Clock`] (for deterministic
    /// handshake timeout testing).
    #[must_use]
    pub fn with_clock<C: ContentStore>(
        controller: ControllerHandle,
        config: WorkerServiceConfig,
        clock: Arc<dyn Clock>,
        content: Arc<C>,
    ) -> Self {
        Self {
            controller,
            config,
            clock,
            invalidate_existence: Arc::new(move |digests| content.invalidate_existence(digests)),
        }
    }

    /// Returns the service's active [`WorkerServiceConfig`].
    #[must_use]
    pub const fn config(&self) -> WorkerServiceConfig {
        self.config
    }

    /// Wraps this service in a tonic [`WorkerServiceServer`].
    #[must_use]
    pub fn into_server(self) -> WorkerServiceServer<Self> {
        WorkerServiceServer::new(self)
    }
}

/// Converts a `worker.v1` [`proto::CacheStats`] message into [`WorkerCacheStats`].
#[must_use]
pub const fn cache_stats_from_proto(stats: proto::CacheStats) -> WorkerCacheStats {
    WorkerCacheStats {
        local_read_requests: stats.local_read_requests,
        local_read_bytes: stats.local_read_bytes,
        slow_read_requests: stats.slow_read_requests,
        slow_read_bytes: stats.slow_read_bytes,
        evictions: stats.evictions,
        evicted_bytes: stats.evicted_bytes,
        size_bytes: stats.size_bytes,
        budget_bytes: stats.budget_bytes,
    }
}

/// Per-attempt metadata captured from `WorkerMessage::Executed` and paired with its subsequent
/// `WorkerMessage::Completed` frame on the same session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExecutedMeta {
    timestamps: Option<(Timestamp, Timestamp)>,
    exit_code: i32,
    had_error_status: bool,
}

/// Shared exact-once notifier ensuring [`ControllerHandle::worker_lost`] fires at most once across
/// the inbound reader task and outbound stream teardown paths.
pub(crate) struct WorkerLossGuard {
    controller: ControllerHandle,
    worker: WorkerKey,
    worker_id: WorkerId,
    runtime: tokio::runtime::Handle,
    lost_sent: AtomicBool,
}

impl WorkerLossGuard {
    pub(crate) const fn new(
        controller: ControllerHandle,
        worker: WorkerKey,
        worker_id: WorkerId,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            controller,
            worker,
            worker_id,
            runtime,
            lost_sent: AtomicBool::new(false),
        }
    }

    /// Delivers [`ControllerHandle::worker_lost`] asynchronously if not already sent.
    pub(crate) async fn notify_lost(&self) {
        if !self.lost_sent.swap(true, Ordering::AcqRel) {
            tracing::debug!(
                worker_id = %self.worker_id,
                worker_index = self.worker.index(),
                worker_generation = self.worker.generation(),
                "notifying controller of worker session loss"
            );
            let _ = self.controller.worker_lost(self.worker).await;
        }
    }

    /// Delivers [`ControllerHandle::worker_lost`] from a synchronous [`Drop`] path by spawning on
    /// the captured Tokio runtime handle if not already sent.
    pub(crate) fn notify_lost_background(&self) {
        if !self.lost_sent.swap(true, Ordering::AcqRel) {
            tracing::debug!(
                worker_id = %self.worker_id,
                worker_index = self.worker.index(),
                worker_generation = self.worker.generation(),
                "spawning deferred worker_lost notification on session drop"
            );
            let controller = self.controller.clone();
            let worker = self.worker;
            self.runtime.spawn(async move {
                let _ = controller.worker_lost(worker).await;
            });
        }
    }
}

/// RAII guard owned by the outbound gRPC response stream: aborts the inbound reader task and
/// guarantees exact-once `worker_lost` delivery whenever the response stream finishes or drops.
pub(crate) struct SessionDropGuard {
    pub(crate) loss: Arc<WorkerLossGuard>,
    pub(crate) inbound_task: JoinHandle<()>,
}

impl Drop for SessionDropGuard {
    fn drop(&mut self) {
        self.inbound_task.abort();
        self.loss.notify_lost_background();
    }
}

/// Fallback drop guard inside the inbound reader task so even external cancellation triggers
/// exact-once `worker_lost`.
pub(crate) struct InboundDropGuard {
    pub(crate) loss: Arc<WorkerLossGuard>,
}

impl Drop for InboundDropGuard {
    fn drop(&mut self) {
        self.loss.notify_lost_background();
    }
}

struct OutboundState {
    initial_registered: Option<proto::ControllerMessage>,
    outbound_rx: mpsc::Receiver<WorkerOutbound>,
    protocol_error_rx: Option<oneshot::Receiver<Status>>,
    finished: bool,
    guard: SessionDropGuard,
}

enum NextOutbound {
    ProtocolError(Status),
    Directive(WorkerOutbound),
    Closed,
}

#[tonic::async_trait]
impl WorkerService for WorkerGrpcService {
    type ConnectStream = BoxStream<'static, Result<proto::ControllerMessage, Status>>;

    async fn connect(
        &self,
        request: Request<Streaming<proto::WorkerMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let mut inbound = request.into_inner();
        let first_frame = self
            .clock
            .timeout(self.config.registration_timeout.get(), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("timed out waiting for initial Register frame"))?
            .map_err(|status| {
                tracing::debug!(%status, "worker stream failed before registration");
                status
            })?;

        let register_proto = match first_frame {
            Some(proto::WorkerMessage {
                kind: Some(WorkerKind::Register(reg)),
            }) => reg,
            Some(proto::WorkerMessage { kind: Some(_) }) => {
                return Err(Status::invalid_argument(
                    "first worker frame must be Register",
                ));
            }
            Some(proto::WorkerMessage { kind: None }) => {
                return Err(Status::invalid_argument(
                    "first worker frame missing kind; expected Register",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "worker stream closed before sending Register",
                ));
            }
        };

        let registration = parse_register_frame(register_proto)?;
        let registered = self
            .controller
            .register_worker(registration)
            .await
            .map_err(map_register_error)?;

        let runtime = tokio::runtime::Handle::current();
        let loss = Arc::new(WorkerLossGuard::new(
            self.controller.clone(),
            registered.key,
            registered.worker_id.clone(),
            runtime,
        ));
        let (protocol_error_tx, protocol_error_rx) = oneshot::channel();

        let inbound_task = tokio::spawn(run_inbound_loop(
            inbound,
            self.controller.clone(),
            registered.key,
            registered.worker_id.clone(),
            Arc::clone(&loss),
            protocol_error_tx,
            Arc::clone(&self.invalidate_existence),
        ));

        let initial_registered = proto::ControllerMessage {
            kind: Some(ControllerKind::Registered(proto::Registered {
                worker_id: registered.worker_id.as_str().to_owned(),
                keepalive_interval: Some(duration_to_proto(self.config.keepalive_interval.get())),
            })),
        };

        let state = OutboundState {
            initial_registered: Some(initial_registered),
            outbound_rx: registered.outbound,
            protocol_error_rx: Some(protocol_error_rx),
            finished: false,
            guard: SessionDropGuard { loss, inbound_task },
        };

        let outbound_stream = stream::unfold(state, drive_outbound_step);
        Ok(Response::new(Box::pin(outbound_stream)))
    }
}

async fn drive_outbound_step(
    mut state: OutboundState,
) -> Option<(Result<proto::ControllerMessage, Status>, OutboundState)> {
    if state.finished {
        return None;
    }

    if let Some(registered) = state.initial_registered.take() {
        return Some((Ok(registered), state));
    }

    let next = poll_next_outbound(&mut state).await;
    match next {
        NextOutbound::ProtocolError(status) => {
            state.finished = true;
            state.guard.inbound_task.abort();
            state.guard.loss.notify_lost().await;
            Some((Err(status), state))
        }
        NextOutbound::Directive(directive) => Some((Ok(directive.into_proto()), state)),
        NextOutbound::Closed => {
            state.finished = true;
            state.guard.inbound_task.abort();
            state.guard.loss.notify_lost().await;
            None
        }
    }
}

async fn poll_next_outbound(state: &mut OutboundState) -> NextOutbound {
    loop {
        if let Some(ref mut err_rx) = state.protocol_error_rx {
            tokio::select! {
                biased;
                err = err_rx => {
                    state.protocol_error_rx = None;
                    if let Ok(status) = err {
                        return NextOutbound::ProtocolError(status);
                    }
                    // Inbound task exited without a protocol error; continue draining outbound_rx.
                }
                item = state.outbound_rx.recv() => {
                    return match item {
                        Some(directive) => NextOutbound::Directive(directive),
                        None => NextOutbound::Closed,
                    };
                }
            }
        } else {
            return match state.outbound_rx.recv().await {
                Some(directive) => NextOutbound::Directive(directive),
                None => NextOutbound::Closed,
            };
        }
    }
}

async fn run_inbound_loop(
    mut inbound: Streaming<proto::WorkerMessage>,
    controller: ControllerHandle,
    worker_key: WorkerKey,
    worker_id: WorkerId,
    loss: Arc<WorkerLossGuard>,
    protocol_error_tx: oneshot::Sender<Status>,
    invalidate_existence: Arc<ExistenceInvalidator>,
) {
    let _drop_guard = InboundDropGuard {
        loss: Arc::clone(&loss),
    };
    let mut executed_meta: HashMap<AttemptId, ExecutedMeta> = HashMap::new();
    let mut protocol_error_tx = Some(protocol_error_tx);

    loop {
        let frame = match inbound.message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                tracing::debug!(%worker_id, "worker inbound stream closed cleanly (EOF)");
                break;
            }
            Err(status) => {
                tracing::debug!(%worker_id, %status, "worker inbound stream ended with transport error");
                break;
            }
        };

        let result = dispatch_inbound_frame(
            frame,
            &controller,
            worker_key,
            &worker_id,
            &mut executed_meta,
            &*invalidate_existence,
        )
        .await;

        match result {
            Ok(()) => {}
            Err(InboundError::Protocol(status)) => {
                tracing::warn!(%worker_id, %status, "closing worker session on protocol violation");
                if let Some(tx) = protocol_error_tx.take() {
                    let _ = tx.send(status);
                }
                break;
            }
            Err(InboundError::ControllerClosed) => {
                tracing::debug!(%worker_id, "controller closed while forwarding worker frame");
                break;
            }
        }
    }

    loss.notify_lost().await;
}

#[derive(Debug)]
enum InboundError {
    Protocol(Status),
    ControllerClosed,
}

async fn dispatch_inbound_frame(
    frame: proto::WorkerMessage,
    controller: &ControllerHandle,
    worker_key: WorkerKey,
    worker_id: &WorkerId,
    executed_meta: &mut HashMap<AttemptId, ExecutedMeta>,
    invalidate_existence: &(dyn Fn(&[Digest]) + Send + Sync),
) -> Result<(), InboundError> {
    let Some(kind) = frame.kind else {
        return Err(InboundError::Protocol(Status::invalid_argument(
            "WorkerMessage frame missing kind",
        )));
    };

    match kind {
        WorkerKind::Register(_) => Err(InboundError::Protocol(Status::invalid_argument(
            "duplicate Register frame on established worker session",
        ))),
        WorkerKind::Accept(accept) => {
            let attempt_id = parse_attempt_id(accept.attempt, "Accept.attempt")?;
            controller
                .worker_event(worker_key, WorkerEvent::Accept { attempt_id })
                .await
                .map_err(|_| InboundError::ControllerClosed)
        }
        WorkerKind::Decline(decline) => {
            let attempt_id = parse_attempt_id(decline.attempt, "Decline.attempt")?;
            let reason = parse_decline_reason(decline.reason)?;
            tracing::debug!(
                %worker_id,
                %attempt_id,
                ?reason,
                free_bytes = decline.free_bytes,
                "worker declined assigned attempt"
            );
            controller
                .worker_event(worker_key, WorkerEvent::Decline { attempt_id, reason })
                .await
                .map_err(|_| InboundError::ControllerClosed)
        }
        WorkerKind::Executed(executed) => {
            let (worker_executed, meta) = parse_executed_frame(executed)?;
            if executed_meta.len() < MAX_TRACKED_EXECUTED_META
                || executed_meta.contains_key(&worker_executed.attempt_id)
            {
                executed_meta.insert(worker_executed.attempt_id.clone(), meta);
            }
            controller
                .worker_event(worker_key, WorkerEvent::Executed(worker_executed))
                .await
                .map_err(|_| InboundError::ControllerClosed)
        }
        WorkerKind::Completed(completed) => {
            let (worker_completed, payload) = parse_completed_frame(completed, executed_meta)?;
            if let CompletionStatus::MissingBlobs { digests, .. } = &worker_completed.status {
                invalidate_existence(digests);
            }
            controller
                .worker_event_with_completion(
                    worker_key,
                    WorkerEvent::Completed(worker_completed),
                    payload,
                )
                .await
                .map_err(|_| InboundError::ControllerClosed)
        }
        WorkerKind::KeepAlive(keep_alive) => {
            let cache = keep_alive.cache.map(cache_stats_from_proto);
            let memory_capacity = keep_alive.memory_capacity.map(Bytes::new);
            controller
                .worker_event(
                    worker_key,
                    WorkerEvent::KeepAlive {
                        cache,
                        memory_capacity,
                    },
                )
                .await
                .map_err(|_| InboundError::ControllerClosed)
        }
        WorkerKind::GoingAway(_) => controller
            .worker_event(worker_key, WorkerEvent::GoingAway)
            .await
            .map_err(|_| InboundError::ControllerClosed),
    }
}

fn parse_register_frame(reg: proto::Register) -> Result<WorkerRegistration, Status> {
    let instance = InstanceId::try_new(reg.instance)
        .map_err(|err| Status::invalid_argument(format!("invalid register.instance: {err}")))?;
    let fleet = FleetName::try_new(reg.fleet)
        .map_err(|err| Status::invalid_argument(format!("invalid register.fleet: {err}")))?;
    if reg.gang_version.is_empty() {
        return Err(Status::invalid_argument(
            "register.gang_version must not be empty",
        ));
    }

    let mut properties = BTreeMap::new();
    for (key, value) in reg.properties {
        if key.is_empty() {
            return Err(Status::invalid_argument(
                "register.properties must not contain an empty key",
            ));
        }
        properties.insert(key, value);
    }

    let capacity = parse_resources(reg.capacity, "register.capacity")?;
    let ceilings = parse_resources(reg.ceilings, "register.ceilings")?;
    let max_inflight = NonZeroU32::new(reg.max_inflight);
    let environment = reg
        .environment
        .map(|env| {
            ActionEnvironment::try_from(env.identity).map_err(|err| {
                Status::invalid_argument(format!("invalid register.environment: {err}"))
            })
        })
        .transpose()?;

    let mut uuid_bytes = [0_u8; 16];
    getrandom::fill(&mut uuid_bytes)
        .map_err(|err| Status::internal(format!("failed to read OS entropy: {err}")))?;

    Ok(WorkerRegistration {
        instance: Arc::new(instance),
        fleet,
        gang_version: reg.gang_version,
        properties,
        capacity,
        ceilings,
        max_inflight,
        single_use: reg.single_use,
        environment,
        uuid_bytes,
    })
}

fn parse_resources(
    proto_res: Option<proto::Resources>,
    field: &'static str,
) -> Result<ActionResources, Status> {
    let res = proto_res.ok_or_else(|| Status::invalid_argument(format!("missing {field}")))?;
    if res.cpu_millicores == 0 || res.memory_bytes == 0 || res.disk_bytes == 0 {
        return Err(Status::invalid_argument(format!(
            "{field} must declare non-zero cpu_millicores, memory_bytes, and disk_bytes"
        )));
    }
    Ok(ActionResources {
        cpu_millicores: res.cpu_millicores,
        memory: Bytes::new(res.memory_bytes),
        disk: Bytes::new(res.disk_bytes),
    })
}

pub(crate) fn map_register_error(err: ControllerRegisterError) -> Status {
    match err {
        ControllerRegisterError::Closed => Status::unavailable("controller is shutting down"),
        ControllerRegisterError::Register(RegisterError::HandingOff) => {
            Status::failed_precondition("controller is handing off leadership")
        }
        ControllerRegisterError::Register(RegisterError::Registration(reg_err)) => match reg_err {
            RegistrationError::UnknownFleet { .. } | RegistrationError::InvalidWorkerId(_) => {
                Status::invalid_argument(reg_err.to_string())
            }
            RegistrationError::DuplicateWorkerId { .. } => {
                Status::already_exists(reg_err.to_string())
            }
            RegistrationError::SlabFull => Status::resource_exhausted(reg_err.to_string()),
        },
    }
}

fn parse_attempt_id(raw: String, field: &'static str) -> Result<AttemptId, InboundError> {
    AttemptId::try_new(raw)
        .map_err(|err| InboundError::Protocol(Status::invalid_argument(format!("{field}: {err}"))))
}

fn parse_decline_reason(raw: i32) -> Result<DeclineReason, InboundError> {
    match proto::DeclineReason::try_from(raw) {
        Ok(proto::DeclineReason::DiskShortage) => Ok(DeclineReason::DiskShortage),
        Ok(proto::DeclineReason::Draining) => Ok(DeclineReason::Draining),
        Ok(proto::DeclineReason::Unspecified) => Err(InboundError::Protocol(
            Status::invalid_argument("Decline.reason must not be DECLINE_REASON_UNSPECIFIED"),
        )),
        Err(_) => Err(InboundError::Protocol(Status::invalid_argument(format!(
            "unknown Decline.reason {raw}"
        )))),
    }
}

fn parse_executed_frame(
    executed: proto::Executed,
) -> Result<(WorkerExecuted, ExecutedMeta), InboundError> {
    let attempt_id = parse_attempt_id(executed.attempt, "Executed.attempt")?;
    let started_proto = executed.started.as_ref();
    let finished_proto = executed.finished.as_ref();
    let timestamps = parse_timestamp_pair(started_proto, finished_proto);
    let wall_time = wall_duration_between(started_proto, finished_proto).unwrap_or(Duration::ZERO);

    let (peak_memory, peak_disk, cpu_time, oom_kills) = match executed.usage {
        Some(usage) => (
            usage.peak_memory_bytes.map(Bytes::new),
            usage.peak_disk_bytes.map(Bytes::new),
            usage
                .cpu_time
                .as_ref()
                .and_then(proto_to_duration)
                .unwrap_or(Duration::ZERO),
            usage.oom_kills,
        ),
        None => (None, None, Duration::ZERO, 0),
    };

    let kill = parse_executed_kill(executed.kill.as_ref(), &attempt_id, oom_kills)?;
    let had_error_status = executed.status.as_ref().is_some_and(|st| st.code != 0);

    let worker_executed = WorkerExecuted {
        attempt_id,
        usage: AttemptUsage {
            peak_memory,
            peak_disk,
            cpu_time,
            wall_time,
            oom_kills,
        },
        kill,
        side_channel_memory_failure: false,
        deadline_exceeded: executed.kill.as_ref().is_some_and(|kill| {
            matches!(
                proto::KillReason::try_from(kill.reason),
                Ok(proto::KillReason::Timeout | proto::KillReason::DeadlineExceeded)
            )
        }),
    };
    let meta = ExecutedMeta {
        timestamps,
        exit_code: executed.exit_code,
        had_error_status,
    };
    Ok((worker_executed, meta))
}

fn parse_executed_kill(
    kill: Option<&proto::Kill>,
    attempt_id: &AttemptId,
    oom_kills: u64,
) -> Result<Option<KillCause>, InboundError> {
    let Some(kill) = kill else {
        return Ok(None);
    };
    if !kill.attempt.is_empty() && kill.attempt != attempt_id.as_str() {
        return Err(InboundError::Protocol(Status::invalid_argument(
            "Executed.kill.attempt does not match Executed.attempt",
        )));
    }
    let reason = proto::KillReason::try_from(kill.reason).map_err(|_| {
        InboundError::Protocol(Status::invalid_argument(format!(
            "unknown Executed.kill.reason {}",
            kill.reason
        )))
    })?;
    match reason {
        proto::KillReason::Unspecified => Err(InboundError::Protocol(Status::invalid_argument(
            "Executed.kill.reason must not be KILL_REASON_UNSPECIFIED",
        ))),
        proto::KillReason::Memory => {
            if oom_kills > 0 {
                Ok(Some(KillCause::Oom))
            } else {
                Ok(Some(KillCause::MemoryPressure))
            }
        }
        proto::KillReason::DiskPressure => Ok(Some(KillCause::DiskPressure)),
        proto::KillReason::DiskLimit => Ok(Some(KillCause::SoftDisk)),
        proto::KillReason::Cancelled
        | proto::KillReason::DeadlineExceeded
        | proto::KillReason::Timeout => Ok(None),
    }
}

fn parse_completed_frame(
    completed: proto::Completed,
    executed_meta: &mut HashMap<AttemptId, ExecutedMeta>,
) -> Result<(WorkerCompleted, CompletionPayload), InboundError> {
    let attempt_id = parse_attempt_id(completed.attempt, "Completed.attempt")?;
    let meta = executed_meta.remove(&attempt_id);

    let Some(outcome) = completed.outcome else {
        return Err(InboundError::Protocol(Status::invalid_argument(
            "Completed frame missing outcome",
        )));
    };

    match outcome {
        CompletedOutcome::Result(action_result) => {
            let execution_timestamps = meta
                .and_then(|m| m.timestamps)
                .or_else(|| action_result_timestamps(&action_result));
            let exit_code = action_result.exit_code;
            let error = meta.is_some_and(|m| m.had_error_status);
            let worker_completed = WorkerCompleted {
                attempt_id,
                status: CompletionStatus::Ok,
                execution_timestamps,
            };
            let payload = CompletionPayload {
                result: Some(action_result),
                status: None,
                exit_code,
                error,
            };
            Ok((worker_completed, payload))
        }
        CompletedOutcome::Status(rpc_status) => {
            let execution_timestamps = meta.and_then(|m| m.timestamps);
            let exit_code = meta.map_or(0, |m| m.exit_code);
            let message = if rpc_status.message.is_empty() {
                format!(
                    "worker completed attempt with status code {}",
                    rpc_status.code
                )
            } else {
                rpc_status.message.clone()
            };
            let worker_completed = WorkerCompleted {
                attempt_id,
                status: {
                    let digests = gang_reapi::extract_missing_blobs(&rpc_status);
                    if digests.is_empty() {
                        CompletionStatus::Failed { message }
                    } else {
                        CompletionStatus::MissingBlobs { digests, message }
                    }
                },
                execution_timestamps,
            };
            let payload = CompletionPayload {
                result: None,
                status: Some(rpc_status),
                exit_code,
                error: true,
            };
            Ok((worker_completed, payload))
        }
    }
}

fn action_result_timestamps(result: &ActionResult) -> Option<(Timestamp, Timestamp)> {
    let meta = result.execution_metadata.as_ref()?;
    parse_timestamp_pair(
        meta.execution_start_timestamp.as_ref(),
        meta.execution_completed_timestamp.as_ref(),
    )
}

fn parse_timestamp_pair(
    started: Option<&prost_types::Timestamp>,
    finished: Option<&prost_types::Timestamp>,
) -> Option<(Timestamp, Timestamp)> {
    let start = proto_to_timestamp(started?)?;
    let end = proto_to_timestamp(finished?)?;
    (end >= start).then_some((start, end))
}

fn proto_to_timestamp(ts: &prost_types::Timestamp) -> Option<Timestamp> {
    if ts.seconds < 0 || !(0..1_000_000_000).contains(&ts.nanos) {
        return None;
    }
    let secs = u64::try_from(ts.seconds).ok()?;
    let nanos = u64::try_from(ts.nanos).ok()?;
    let millis = secs.checked_mul(1_000)?.checked_add(nanos / 1_000_000)?;
    Some(Timestamp::from_unix_millis(millis))
}

fn wall_duration_between(
    started: Option<&prost_types::Timestamp>,
    finished: Option<&prost_types::Timestamp>,
) -> Option<Duration> {
    let start_ns = proto_timestamp_nanos(started?)?;
    let end_ns = proto_timestamp_nanos(finished?)?;
    let diff = end_ns.checked_sub(start_ns)?;
    let secs = u64::try_from(diff / 1_000_000_000).ok()?;
    let nanos = u32::try_from(diff % 1_000_000_000).ok()?;
    Some(Duration::new(secs, nanos))
}

fn proto_timestamp_nanos(ts: &prost_types::Timestamp) -> Option<u128> {
    if ts.seconds < 0 || !(0..1_000_000_000).contains(&ts.nanos) {
        return None;
    }
    let secs = u128::try_from(ts.seconds).ok()?;
    let nanos = u128::try_from(ts.nanos).ok()?;
    secs.checked_mul(1_000_000_000)?.checked_add(nanos)
}

fn proto_to_duration(duration: &prost_types::Duration) -> Option<Duration> {
    if duration.seconds < 0 || !(0..1_000_000_000).contains(&duration.nanos) {
        return None;
    }
    let secs = u64::try_from(duration.seconds).ok()?;
    let nanos = u32::try_from(duration.nanos).ok()?;
    Some(Duration::new(secs, nanos))
}

fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos = i32::try_from(duration.subsec_nanos()).unwrap_or(999_999_999);
    prost_types::Duration { seconds, nanos }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes as RawBytes;
    use futures::stream;
    use gang_clock::{SystemClock, TestClock, Timestamp};
    use gang_config::{ActionsConfig, WholeKib};
    use gang_reapi::build::bazel::remote::execution::v2::ActionResult;
    use gang_reapi::leadingterm::gang::worker::v1 as proto;
    use gang_reapi::leadingterm::gang::worker::v1::worker_service_client::WorkerServiceClient;
    use gang_scheduler::{AdmissionRequest, Learned, OperationUpdate, Scheduler, SchedulerPolicy};
    use gang_spi::{
        ControllerDrain, Observation, ObservationSink, Origin, SchedulerSnapshot, WorkerCacheStats,
    };
    use gang_testkit::TestResult;
    use gang_types::{
        BuildId, BuildKey, Bytes, Digest, FleetActionPolicy, FleetCatalog, FleetName,
        FleetProperties, Identity, InstanceId, InvocationId, Isa, Mnemonic, NonZeroBytes,
        OperationName, Principal, ProviderName, Resources, SourceId, TargetLabel, property,
    };
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, mpsc};
    use tonic::Code;
    use tonic::transport::{Endpoint, Server};

    use super::*;
    use crate::actor::{ActorConfig, ControllerRuntime, ExecuteSpec, InlineActionPayload};
    use crate::controller::{LearnedSlot, spawn_controller};
    use crate::worker_session::{
        ConnectError, ConnectedChannel, SessionConfig, SessionConnector, SessionEvent, run_session,
    };

    #[derive(Debug, Default)]
    struct TrackingStore {
        inner: gang_testkit::MemoryContentStore,
        invalidated: parking_lot::Mutex<Vec<Digest>>,
    }

    impl ContentStore for TrackingStore {
        fn invalidate_existence(&self, digests: &[Digest]) {
            self.invalidated.lock().extend_from_slice(digests);
        }
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), gang_spi::PortError> {
            self.inner.find_missing(digests, missing).await
        }
        async fn read(
            &self,
            digest: Digest,
            range: gang_spi::ByteRange,
        ) -> Result<Option<gang_spi::BlobStream>, gang_spi::PortError> {
            self.inner.read(digest, range).await
        }
        async fn write(
            &self,
            digest: Digest,
            body: gang_spi::BlobStream,
        ) -> Result<(), gang_spi::PortError> {
            self.inner.write(digest, body).await
        }
        fn usage(&self) -> Option<gang_spi::EngineStore> {
            self.inner.usage()
        }
    }

    #[tokio::test]
    async fn missing_blob_completion_invalidates_and_finishes_without_retry() -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let content = Arc::new(TrackingStore::default());
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::clone(&content),
        ))
        .await?;
        let mut worker = LiveSessionClient::spawn(addr, "inst-missing");
        let SessionEvent::Registered { outbound, .. } = recv_event(&mut worker.events).await?
        else {
            return Err("expected registration".into());
        };
        let mut watch = controller
            .execute(exec_spec("operations/missing", b"missing-action", None))
            .await?;
        let SessionEvent::Assign(assign) = recv_event(&mut worker.events).await? else {
            return Err("expected assignment".into());
        };
        outbound.accept(&assign.attempt).await?;
        let digest = Digest::from_bytes(b"missing blob");
        outbound
            .completed(proto::Completed {
                attempt: assign.attempt,
                outcome: Some(CompletedOutcome::Status(
                    gang_reapi::missing_blobs_rpc_status(&[digest], "missing"),
                )),
            })
            .await?;
        let terminal = wait_terminal(&mut watch).await?;
        assert!(
            matches!(terminal.update, OperationUpdate::Failed(gang_scheduler::TerminalFailure::MissingBlobs { digests, .. }) if digests == vec![digest])
        );
        assert_eq!(*content.invalidated.lock(), vec![digest]);
        assert!(
            worker.events.try_recv().is_err(),
            "missing blobs must not be retried"
        );
        worker.stop_and_join().await?;
        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    fn missing_input_action(content: &TrackingStore, kind: u8) -> TestResult<(Digest, Digest)> {
        use gang_reapi::build::bazel::remote::execution::v2 as reapi;
        use prost::Message as _;
        let missing_file = Digest::from_bytes(format!("absent-{kind}").as_bytes());
        let child = reapi::Directory {
            files: vec![reapi::FileNode {
                name: "file".to_owned(),
                digest: Some(gang_reapi::to_proto(&missing_file)?),
                ..Default::default()
            }],
            ..Default::default()
        };
        let child_digest = content.inner.insert(RawBytes::from(child.encode_to_vec()));
        let root = match kind {
            0 => {
                let _ = content.inner.remove(child_digest);
                child_digest
            }
            1 => content.inner.insert(RawBytes::from(
                reapi::Directory {
                    directories: vec![reapi::DirectoryNode {
                        name: "sub".to_owned(),
                        digest: Some(gang_reapi::to_proto(&child_digest)?),
                    }],
                    ..Default::default()
                }
                .encode_to_vec(),
            )),
            _ => child_digest,
        };
        let command = reapi::Command {
            arguments: vec!["true".to_owned()],
            ..Default::default()
        };
        let command_digest = content
            .inner
            .insert(RawBytes::from(command.encode_to_vec()));
        let action = reapi::Action {
            command_digest: Some(gang_reapi::to_proto(&command_digest)?),
            input_root_digest: Some(gang_reapi::to_proto(&root)?),
            ..Default::default()
        };
        let action_digest = content.inner.insert(RawBytes::from(action.encode_to_vec()));
        let missing = if kind == 0 {
            child_digest
        } else {
            missing_file
        };
        Ok((action_digest, missing))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_returns_missing_precondition_from_real_worker() -> TestResult {
        use futures::StreamExt as _;
        use gang_reapi::build::bazel::remote::execution::v2 as reapi;
        use gang_reapi::build::bazel::remote::execution::v2::execution_server::Execution as _;
        use gang_reapi::google::longrunning::operation;
        use gang_types::WorkerLifetime;
        use prost::Message as _;

        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let content = Arc::new(TrackingStore::default());
        let actions = Arc::new(gang_testkit::MemoryActionStore::new());
        let (addr, server) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::clone(&content),
        ))
        .await?;
        let execution = crate::reapi_execution::ExecutionService::with_defaults(
            controller.clone(),
            Arc::clone(&content),
            Arc::clone(&actions),
        );
        for kind in 0..3 {
            let tmp = tempfile::TempDir::new()?;
            let mut spec =
                crate::worker::tests::test_spec(tmp.path(), WorkerLifetime::LongLived, kind == 1)?;
            spec.properties = fleet_props("small", 8);
            spec.instance = InstanceId::try_new(format!("inst-integrated-{kind}"))?;
            spec.worker.tree_cache_min_files = NonZeroUsize::MIN;
            let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
            let worker = crate::worker::tests::spawn_direct_worker(
                spec,
                Arc::clone(&content),
                Arc::clone(&actions),
                addr,
                stop_rx,
            );
            let _ = wait_for_snapshot(&controller, |snapshot| !snapshot.workers.is_empty()).await?;
            let (action_digest, missing) = missing_input_action(&content, kind)?;
            let mut stream = execution
                .execute(tonic::Request::new(reapi::ExecuteRequest {
                    action_digest: Some(gang_reapi::to_proto(&action_digest)?),
                    ..Default::default()
                }))
                .await?
                .into_inner();
            let terminal = tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(operation) = stream.next().await {
                    let operation = operation?;
                    if operation.done {
                        return Ok::<_, tonic::Status>(Some(operation));
                    }
                }
                Ok(None)
            })
            .await??
            .ok_or("missing terminal operation")?;
            let Some(operation::Result::Response(response)) = terminal.result else {
                return Err("missing execute response".into());
            };
            let response = reapi::ExecuteResponse::decode(response.value.as_slice())?;
            let status = response.status.ok_or("missing status")?;
            assert_eq!(gang_reapi::extract_missing_blobs(&status), vec![missing]);
            assert_eq!(
                status.code,
                gang_reapi::google::rpc::Code::FailedPrecondition as i32
            );
            assert!(content.invalidated.lock().contains(&missing));
            let _ = stop_tx.send(());
            worker.await??;
            let _ = wait_for_snapshot(&controller, |snapshot| snapshot.workers.is_empty()).await?;
        }
        controller.shutdown().await?;
        actor_task.await?;
        server.abort();
        Ok(())
    }

    #[test]
    fn timeout_kills_are_reported_to_the_scheduler() {
        for reason in [
            proto::KillReason::Timeout,
            proto::KillReason::DeadlineExceeded,
        ] {
            let id = AttemptId::from_random_bytes([23; 16]);
            let (executed, _) = parse_executed_frame(proto::Executed {
                attempt: id.as_str().to_owned(),
                kill: Some(proto::Kill {
                    attempt: id.as_str().to_owned(),
                    reason: reason as i32,
                }),
                ..Default::default()
            })
            .expect("executed");
            assert!(executed.deadline_exceeded);
        }
    }

    fn fleet_props(name: &str, mem_gib: u64) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )
    }

    fn single_fleet_catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet_props("small", 8)]).expect("catalog")
    }

    fn test_policy() -> SchedulerPolicy {
        let mut actions = ActionsConfig::default();
        actions.cold_start.cpu_millicores = NonZeroU32::new(2_000).expect("2000");
        actions.cold_start.memory = WholeKib::from_units(2 * 1024 * 1024);
        actions.cold_start.disk = WholeKib::from_units(4 * 1024 * 1024);
        SchedulerPolicy {
            actions,
            expected_gang_version: "0.3.9".to_owned(),
            dispatch_ack_timeout: Duration::from_secs(5),
            worker_timeout: Duration::from_secs(10),
            client_action_timeout: Duration::from_secs(15),
            attempt_budget: Duration::from_secs(30),
            ..SchedulerPolicy::default()
        }
    }

    fn make_scheduler(policy: SchedulerPolicy) -> Scheduler {
        let catalog = single_fleet_catalog();
        let learned =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
        Scheduler::new(policy, &catalog, learned).expect("scheduler")
    }

    fn sample_register(instance: &str) -> proto::Register {
        proto::Register {
            instance: instance.to_owned(),
            fleet: "small".to_owned(),
            gang_version: "0.3.9".to_owned(),
            properties: HashMap::from([(property::ISA.to_owned(), "x86-64".to_owned())]),
            capacity: Some(proto::Resources {
                cpu_millicores: 16_000,
                memory_bytes: 8 << 30,
                disk_bytes: 100 << 30,
            }),
            ceilings: Some(proto::Resources {
                cpu_millicores: 8_000,
                memory_bytes: 8 << 30,
                disk_bytes: 100 << 30,
            }),
            max_inflight: 4,
            single_use: false,
            environment: Some(proto::ActionEnvironment {
                identity: "host:test".to_owned(),
            }),
        }
    }

    fn exec_spec(name: &str, seed: &[u8], origin: Option<Origin>) -> ExecuteSpec {
        let request = AdmissionRequest {
            name: OperationName::try_new(name).expect("op"),
            action_digest: Digest::from_bytes(seed),
            command_digest: format!("cmd-{name}"),
            platform_properties: [(property::ISA.to_owned(), "x86-64".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: SourceId::from_identity_str("github-actions:alice"),
            build: Some("12rsfjsg79g6h7f3".parse::<BuildId>().expect("build")),
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
            priority: 100,
            initial_watchers: 1,
        };
        ExecuteSpec {
            request,
            caller: None,
            origin,
            inline: Some(InlineActionPayload {
                action: RawBytes::from_static(b"inline-action"),
                command: RawBytes::from_static(b"inline-command"),
            }),
            timeout: Some(Duration::from_secs(20)),
        }
    }

    fn test_origin() -> Origin {
        let identity = Arc::new(Identity::new(
            ProviderName::try_new("github-actions").expect("provider"),
            Principal::try_new("alice").expect("principal"),
        ));
        Origin {
            build: BuildKey {
                principal: identity,
                invocation: InvocationId::try_new("inv-1").expect("inv"),
            },
            correlated: None,
            tool: None,
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
        }
    }

    fn session_config(instance: &str) -> SessionConfig {
        SessionConfig::new(
            sample_register(instance),
            NonZeroDuration::from_secs(5),
            NonZeroBytes::mib(4),
        )
    }

    fn endpoint_connector(addr: SocketAddr) -> impl SessionConnector {
        let uri = format!("http://{addr}");
        move || {
            let uri = uri.clone();
            async move {
                let endpoint = Endpoint::from_shared(uri)
                    .map_err(|err| ConnectError::Transient(Box::new(err)))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|err| ConnectError::from_transport(Box::new(err)))?;
                Ok(ConnectedChannel {
                    channel,
                    revoked: None,
                })
            }
        }
    }

    async fn spawn_worker_server(
        service: WorkerGrpcService,
    ) -> TestResult<(SocketAddr, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = stream::unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await.ok()?;
            Some((Ok::<_, std::io::Error>(stream), listener))
        });
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(service.into_server())
                .serve_with_incoming(incoming)
                .await;
        });
        Ok((addr, handle))
    }

    async fn wait_for_snapshot(
        controller: &ControllerHandle,
        predicate: impl Fn(&SchedulerSnapshot) -> bool,
    ) -> TestResult<SchedulerSnapshot> {
        let mut snap = SchedulerSnapshot::default();
        for _ in 0..100 {
            controller.snapshot(&mut snap).await?;
            if predicate(&snap) {
                return Ok(snap);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("timed out waiting for snapshot condition: {snap:?}").into())
    }

    async fn recv_event(rx: &mut mpsc::Receiver<SessionEvent>) -> TestResult<SessionEvent> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .map_err(|_| "timed out waiting for SessionEvent")?
            .ok_or_else(|| "SessionEvent channel closed unexpectedly".into())
    }

    struct LiveSessionClient {
        events: mpsc::Receiver<SessionEvent>,
        stop: Arc<Notify>,
        task: JoinHandle<Result<(), crate::worker_session::SessionError>>,
    }

    impl LiveSessionClient {
        fn spawn(addr: SocketAddr, instance: &str) -> Self {
            let (events_tx, events) = mpsc::channel(32);
            let stop = Arc::new(Notify::new());
            let stop_wait = Arc::clone(&stop);
            let cfg = session_config(instance);
            let task = tokio::spawn(async move {
                let mut connector = endpoint_connector(addr);
                run_session(
                    &cfg,
                    &mut connector,
                    &|| (WorkerCacheStats::default(), Some(8 << 30)),
                    &SystemClock::new(),
                    events_tx,
                    stop_wait.notified(),
                )
                .await
            });
            Self { events, stop, task }
        }

        async fn stop_and_join(self) -> TestResult {
            self.stop.notify_one();
            self.task.await??;
            Ok(())
        }
    }

    struct RawWorkerConn {
        tx: mpsc::Sender<proto::WorkerMessage>,
        inbound: tonic::Streaming<proto::ControllerMessage>,
        worker_id: String,
    }

    async fn connect_raw_worker(addr: SocketAddr, instance: &str) -> TestResult<RawWorkerConn> {
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect()
            .await?;
        let mut client = WorkerServiceClient::new(channel);
        let (tx, rx) = mpsc::channel::<proto::WorkerMessage>(16);
        tx.send(proto::WorkerMessage {
            kind: Some(WorkerKind::Register(sample_register(instance))),
        })
        .await?;
        let outbound_stream =
            stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|msg| (msg, rx)) },
            );
        let mut inbound = client.connect(outbound_stream).await?.into_inner();
        let Some(proto::ControllerMessage {
            kind: Some(ControllerKind::Registered(reg)),
        }) = inbound.message().await?
        else {
            return Err("expected Registered frame".into());
        };
        Ok(RawWorkerConn {
            tx,
            inbound,
            worker_id: reg.worker_id,
        })
    }

    fn sample_executed(attempt: String) -> proto::Executed {
        proto::Executed {
            attempt,
            usage: Some(proto::ResourceUsage {
                cpu_time: Some(prost_types::Duration {
                    seconds: 1,
                    nanos: 250_000_000,
                }),
                peak_memory_bytes: Some(256 << 20),
                peak_disk_bytes: Some(128 << 20),
                oom_kills: 0,
                sampled: true,
            }),
            kill: None,
            status: None,
            exit_code: 0,
            started: Some(prost_types::Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
            finished: Some(prost_types::Timestamp {
                seconds: 1_700_000_003,
                nanos: 500_000_000,
            }),
        }
    }

    async fn wait_terminal(
        watch: &mut crate::actor::OperationWatch,
    ) -> TestResult<crate::actor::OperationWatchState> {
        let mut state = watch.current();
        while !state.is_terminal() {
            state = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await??;
        }
        Ok(state)
    }

    #[tokio::test]
    async fn end_to_end_worker_session_executes_assign_accept_executed_completed_and_publishes_result()
    -> TestResult {
        let (obs_sink, mut obs_rx) = ObservationSink::channel(
            NonZeroUsize::new(16).expect("16"),
            NonZeroUsize::new(16).expect("16"),
        );
        let runtime = ControllerRuntime {
            observations: Some(obs_sink),
            ..ControllerRuntime::new()
        };
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            runtime,
        );

        let svc_config = WorkerServiceConfig {
            keepalive_interval: NonZeroDuration::from_secs(3),
            registration_timeout: NonZeroDuration::from_secs(5),
        };
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::new(
            controller.clone(),
            svc_config,
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;
        let mut worker = LiveSessionClient::spawn(addr, "inst-e2e");

        let SessionEvent::Registered {
            worker_id,
            keepalive_interval,
            outbound,
            ..
        } = recv_event(&mut worker.events).await?
        else {
            return Err("expected SessionEvent::Registered".into());
        };
        assert!(worker_id.as_str().starts_with("inst-e2e-"));
        assert_eq!(keepalive_interval, Duration::from_secs(3));

        // Submit an execution operation and verify Assign arrives at the real worker session.
        let mut watch = controller
            .execute(exec_spec(
                "operations/e2e-1",
                b"action-e2e-1",
                Some(test_origin()),
            ))
            .await?;

        let SessionEvent::Assign(assign) = recv_event(&mut worker.events).await? else {
            return Err("expected SessionEvent::Assign".into());
        };
        assert_eq!(assign.operation, "operations/e2e-1");
        assert_eq!(assign.action.as_deref(), Some(&b"inline-action"[..]));
        assert_eq!(assign.command.as_deref(), Some(&b"inline-command"[..]));
        assert_eq!(assign.target, "//app:bin");
        assert_eq!(assign.mnemonic, "Rustc");

        // Accept -> watcher observes Executing.
        outbound.accept(&assign.attempt).await?;
        let state = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await??;
        assert!(matches!(state.update, OperationUpdate::Executing { .. }));

        // Executed -> releases compute early and caches timestamps + exit_code.
        outbound
            .executed(sample_executed(assign.attempt.clone()))
            .await?;

        // Completed with exact ActionResult -> watcher receives exact ActionResult!
        let expected_result = ActionResult {
            exit_code: 0,
            stdout_raw: RawBytes::from_static(b"hello from worker"),
            stderr_raw: RawBytes::new(),
            ..Default::default()
        };
        outbound
            .completed(proto::Completed {
                attempt: assign.attempt.clone(),
                outcome: Some(CompletedOutcome::Result(expected_result.clone())),
            })
            .await?;

        let terminal = wait_terminal(&mut watch).await?;
        assert_eq!(terminal.update, OperationUpdate::Completed);
        assert_eq!(terminal.result, Some(expected_result));
        assert_eq!(terminal.status, None);

        // Build observation received the execution timestamps carried from Executed.
        let obs = tokio::time::timeout(Duration::from_secs(5), obs_rx.next())
            .await?
            .and_then(gang_spi::Delivery::into_observation)
            .ok_or("expected Observation::Executed")?;
        let Observation::Executed(executed_obs) = obs else {
            return Err("expected Observation::Executed variant".into());
        };
        assert_eq!(executed_obs.exit_code, 0);
        assert!(!executed_obs.error);
        assert_eq!(executed_obs.worker, Some(worker_id));
        assert_eq!(
            executed_obs.times.executing.map(|p| p.duration()),
            Some(Duration::from_millis(2_500))
        );

        // Stop worker cleanly (sends GoingAway + closes stream) -> controller removes worker.
        worker.stop_and_join().await?;
        let snap = wait_for_snapshot(&controller, |s| s.workers.is_empty()).await?;
        assert!(snap.workers.is_empty());

        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn decline_disk_shortage_and_kill_roundtrip() -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        // Start Worker 1.
        let (ev1_tx, mut ev1_rx) = mpsc::channel(32);
        let stop1 = Arc::new(Notify::new());
        let stop1_wait = Arc::clone(&stop1);
        let cfg1 = session_config("inst-decline-1");
        let w1_task = tokio::spawn(async move {
            let mut connector = endpoint_connector(addr);
            run_session(
                &cfg1,
                &mut connector,
                &|| (WorkerCacheStats::default(), None),
                &SystemClock::new(),
                ev1_tx,
                stop1_wait.notified(),
            )
            .await
        });
        let SessionEvent::Registered { outbound: out1, .. } = recv_event(&mut ev1_rx).await? else {
            return Err("expected worker 1 Registered".into());
        };

        // Submit op 1 -> assigned to Worker 1 -> Worker 1 declines with DiskShortage.
        let mut watch1 = controller
            .execute(exec_spec("operations/decline-op", b"decline-op", None))
            .await?;
        let SessionEvent::Assign(assign1) = recv_event(&mut ev1_rx).await? else {
            return Err("expected Assign on worker 1".into());
        };
        out1.decline(&assign1.attempt, proto::DeclineReason::DiskShortage, 4096)
            .await?;

        // Op 1 must be requeued while Worker 1 holds disk_hold.
        let snap = wait_for_snapshot(&controller, |s| s.queued.len() == 1).await?;
        assert_eq!(snap.queued.len(), 1);
        assert_eq!(snap.executing.len(), 0);

        // Start Worker 2 -> immediately picks up the requeued op 1.
        let (ev2_tx, mut ev2_rx) = mpsc::channel(32);
        let stop2 = Arc::new(Notify::new());
        let stop2_wait = Arc::clone(&stop2);
        let cfg2 = session_config("inst-decline-2");
        let w2_task = tokio::spawn(async move {
            let mut connector = endpoint_connector(addr);
            run_session(
                &cfg2,
                &mut connector,
                &|| (WorkerCacheStats::default(), None),
                &SystemClock::new(),
                ev2_tx,
                stop2_wait.notified(),
            )
            .await
        });
        let SessionEvent::Registered { outbound: out2, .. } = recv_event(&mut ev2_rx).await? else {
            return Err("expected worker 2 Registered".into());
        };

        let SessionEvent::Assign(assign2) = recv_event(&mut ev2_rx).await? else {
            return Err("expected requeued Assign on worker 2".into());
        };
        assert_eq!(assign2.operation, "operations/decline-op");
        out2.accept(&assign2.attempt).await?;
        let _ = tokio::time::timeout(Duration::from_secs(5), watch1.changed()).await??;

        // Cancel op 1 while running on Worker 2 -> Worker 2 must receive Kill(Cancelled)!
        controller.cancel(watch1.key()).await?;
        let SessionEvent::Kill(kill) = recv_event(&mut ev2_rx).await? else {
            return Err("expected Kill on worker 2".into());
        };
        assert_eq!(kill.attempt, assign2.attempt);
        assert_eq!(kill.reason, proto::KillReason::Cancelled as i32);

        stop1.notify_one();
        stop2.notify_one();
        w1_task.await??;
        w2_task.await??;
        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn set_draining_on_initial_cordon_and_live_mid_session_cordon() -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        let fleet = FleetName::try_new("small")?;
        let instance = InstanceId::try_new("inst-cordon")?;

        // Pre-cordon the physical instance BEFORE the worker connects.
        controller.set_draining(&fleet, &instance, true).await?;

        let (events_tx, mut events_rx) = mpsc::channel(32);
        let stop = Arc::new(Notify::new());
        let stop_wait = Arc::clone(&stop);
        let cfg = session_config("inst-cordon");
        let client_task = tokio::spawn(async move {
            let mut connector = endpoint_connector(addr);
            run_session(
                &cfg,
                &mut connector,
                &|| (WorkerCacheStats::default(), None),
                &SystemClock::new(),
                events_tx,
                stop_wait.notified(),
            )
            .await
        });

        // Frame 1: Registered.
        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::Registered { .. }
        ));
        // Frame 2: immediate SetDraining(true) because instance was pre-cordoned!
        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::SetDraining(true)
        ));

        // Live mid-session uncordon -> SetDraining(false).
        controller.set_draining(&fleet, &instance, false).await?;
        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::SetDraining(false)
        ));

        // Live mid-session cordon -> SetDraining(true).
        controller.set_draining(&fleet, &instance, true).await?;
        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::SetDraining(true)
        ));

        stop.notify_one();
        client_task.await??;
        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn rehome_during_controller_hand_off_drains_and_closes_session() -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        let (events_tx, mut events_rx) = mpsc::channel(32);
        let stop = Arc::new(Notify::new());
        let stop_wait = Arc::clone(&stop);
        let cfg = session_config("inst-rehome");
        let client_task = tokio::spawn(async move {
            let mut connector = endpoint_connector(addr);
            run_session(
                &cfg,
                &mut connector,
                &|| (WorkerCacheStats::default(), None),
                &SystemClock::new(),
                events_tx,
                stop_wait.notified(),
            )
            .await
        });

        let SessionEvent::Registered { outbound, .. } = recv_event(&mut events_rx).await? else {
            return Err("expected Registered".into());
        };

        // Start an active attempt so hand_off drains first and waits to Rehome until completion.
        let mut watch = controller
            .execute(exec_spec("operations/rehome-op", b"rehome-op", None))
            .await?;
        let SessionEvent::Assign(assign) = recv_event(&mut events_rx).await? else {
            return Err("expected Assign".into());
        };
        outbound.accept(&assign.attempt).await?;
        let _ = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await??;

        let mut drain = ControllerDrain::default();
        controller.hand_off(true, &mut drain).await?;
        assert!(
            !drain.complete,
            "active attempt must delay drain completion"
        );

        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::SetDraining(true)
        ));

        // Finish the active attempt -> controller immediately emits Rehome!
        outbound
            .executed(proto::Executed {
                attempt: assign.attempt.clone(),
                usage: Some(proto::ResourceUsage::default()),
                kill: None,
                status: None,
                exit_code: 0,
                started: None,
                finished: None,
            })
            .await?;
        outbound
            .completed(proto::Completed {
                attempt: assign.attempt.clone(),
                outcome: Some(CompletedOutcome::Result(ActionResult::default())),
            })
            .await?;

        assert!(matches!(
            recv_event(&mut events_rx).await?,
            SessionEvent::Rehome
        ));
        let SessionEvent::Disconnected { ack, .. } = recv_event(&mut events_rx).await? else {
            return Err("expected Disconnected".into());
        };
        let _ = ack.send(());

        // Stop the worker before it keeps retrying against the handing-off controller.
        stop.notify_one();
        client_task.await??;

        // Verify worker_lost fired cleanly and drain is now complete.
        let snap = wait_for_snapshot(&controller, |s| s.workers.is_empty()).await?;
        assert!(snap.workers.is_empty());
        controller.refresh_drain(true, &mut drain).await?;
        assert!(drain.complete);

        // A new worker attempting to register during handoff gets FAILED_PRECONDITION.
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect()
            .await?;
        let mut raw_client = WorkerServiceClient::new(channel);
        let req_stream = stream::iter(vec![proto::WorkerMessage {
            kind: Some(WorkerKind::Register(sample_register("inst-late"))),
        }]);
        let err = raw_client
            .connect(req_stream)
            .await
            .expect_err("registration during handoff must fail");
        assert_eq!(err.code(), Code::FailedPrecondition);

        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn abrupt_worker_disconnect_fires_worker_lost_and_requeues_attempt() -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        let mut crashed = connect_raw_worker(addr, "inst-crash").await?;

        // Dispatch an operation and have the worker accept it.
        let mut watch = controller
            .execute(exec_spec("operations/crash-op", b"crash-op", None))
            .await?;
        let Some(ControllerKind::Assign(assign)) =
            crashed.inbound.message().await?.and_then(|m| m.kind)
        else {
            return Err("expected Assign frame".into());
        };
        crashed
            .tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Accept(proto::Accept {
                    attempt: assign.attempt,
                })),
            })
            .await?;
        let _ = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await??;
        let _ = wait_for_snapshot(&controller, |s| {
            s.workers.len() == 1 && s.executing.len() == 1
        })
        .await?;

        // Abruptly drop BOTH client sides (simulating a worker crash / TCP RST).
        drop(crashed);

        // Controller MUST observe worker_lost, evict the crashed worker, and requeue the attempt!
        let after_crash = wait_for_snapshot(&controller, |s| {
            s.workers.is_empty() && s.queued.len() == 1 && s.executing.is_empty()
        })
        .await?;
        assert!(after_crash.workers.is_empty());
        assert_eq!(after_crash.queued.len(), 1);

        // A replacement worker immediately receives and completes the requeued operation.
        let mut rescue = LiveSessionClient::spawn(addr, "inst-rescue");
        let SessionEvent::Registered { outbound, .. } = recv_event(&mut rescue.events).await?
        else {
            return Err("expected rescue Registered".into());
        };
        let SessionEvent::Assign(rescue_assign) = recv_event(&mut rescue.events).await? else {
            return Err("expected rescue Assign".into());
        };
        assert_eq!(rescue_assign.operation, "operations/crash-op");
        outbound.accept(&rescue_assign.attempt).await?;
        outbound
            .executed(proto::Executed {
                attempt: rescue_assign.attempt.clone(),
                usage: Some(proto::ResourceUsage::default()),
                kill: None,
                status: None,
                exit_code: 0,
                started: None,
                finished: None,
            })
            .await?;
        outbound
            .completed(proto::Completed {
                attempt: rescue_assign.attempt,
                outcome: Some(CompletedOutcome::Result(ActionResult {
                    exit_code: 0,
                    stdout_raw: RawBytes::from_static(b"rescued"),
                    ..Default::default()
                })),
            })
            .await?;

        let terminal = wait_terminal(&mut watch).await?;
        assert_eq!(terminal.update, OperationUpdate::Completed);
        assert_eq!(
            terminal.result.as_ref().map(|r| r.stdout_raw.as_ref()),
            Some(&b"rescued"[..])
        );

        rescue.stop_and_join().await?;
        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_first_frame_invalid_register_and_duplicate_register() -> TestResult {
        let test_clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            Arc::clone(&test_clock),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let svc_config = WorkerServiceConfig {
            keepalive_interval: NonZeroDuration::from_secs(5),
            registration_timeout: NonZeroDuration::from_secs(2),
        };
        let svc_clock = Arc::clone(&test_clock);
        let dyn_clock: Arc<dyn Clock> = svc_clock;
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_clock(
            controller.clone(),
            svc_config,
            dyn_clock,
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .connect()
            .await?;
        let mut client = WorkerServiceClient::new(channel);

        // 1. Non-Register first frame -> INVALID_ARGUMENT.
        let err = client
            .connect(stream::iter(vec![proto::WorkerMessage {
                kind: Some(WorkerKind::KeepAlive(proto::KeepAlive::default())),
            }]))
            .await
            .expect_err("KeepAlive as first frame must fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        // 2. Empty first frame -> INVALID_ARGUMENT.
        let err = client
            .connect(stream::iter(vec![proto::WorkerMessage { kind: None }]))
            .await
            .expect_err("empty first frame must fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        // 3. Immediate EOF before Register -> INVALID_ARGUMENT.
        let err = client
            .connect(stream::empty())
            .await
            .expect_err("empty stream must fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        // 4. Register with unknown fleet / missing capacity -> INVALID_ARGUMENT.
        let mut unknown_fleet = sample_register("inst-bad-fleet");
        unknown_fleet.fleet = "does-not-exist".to_owned();
        let err = client
            .connect(stream::iter(vec![proto::WorkerMessage {
                kind: Some(WorkerKind::Register(unknown_fleet)),
            }]))
            .await
            .expect_err("unknown fleet must fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        let mut missing_cap = sample_register("inst-no-cap");
        missing_cap.capacity = None;
        let err = client
            .connect(stream::iter(vec![proto::WorkerMessage {
                kind: Some(WorkerKind::Register(missing_cap)),
            }]))
            .await
            .expect_err("missing capacity must fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        // 5. Registration handshake timeout -> DEADLINE_EXCEEDED.
        let (_silent_tx, silent_rx) = mpsc::channel::<proto::WorkerMessage>(1);
        let silent_stream = stream::unfold(silent_rx, |mut rx| async move {
            rx.recv().await.map(|msg| (msg, rx))
        });
        let mut timeout_client = client.clone();
        let timeout_call = tokio::spawn(async move { timeout_client.connect(silent_stream).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        test_clock.advance(Duration::from_secs(3));
        let timeout_err = timeout_call
            .await?
            .expect_err("silent stream must time out");
        assert_eq!(timeout_err.code(), Code::DeadlineExceeded);

        // 6. Duplicate Register after registration -> server closes stream with INVALID_ARGUMENT
        //    and evicts worker via worker_lost.
        let (dup_tx, dup_rx) = mpsc::channel::<proto::WorkerMessage>(4);
        dup_tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Register(sample_register("inst-dup"))),
            })
            .await?;
        let dup_stream = stream::unfold(dup_rx, |mut rx| async move {
            rx.recv().await.map(|msg| (msg, rx))
        });
        let mut dup_inbound = client.connect(dup_stream).await?.into_inner();
        let first = dup_inbound.message().await?.ok_or("expected Registered")?;
        assert!(matches!(first.kind, Some(ControllerKind::Registered(_))));

        dup_tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Register(sample_register("inst-dup"))),
            })
            .await?;
        let proto_err = dup_inbound
            .message()
            .await
            .expect_err("duplicate Register must fail stream with INVALID_ARGUMENT");
        assert_eq!(proto_err.code(), Code::InvalidArgument);

        let snap = wait_for_snapshot(&controller, |s| s.workers.is_empty()).await?;
        assert!(snap.workers.is_empty());

        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn overlapping_renewal_generations_coexist_and_dropping_gen1_never_evicts_gen2()
    -> TestResult {
        let (controller, actor_task) = spawn_controller(
            make_scheduler(test_policy()),
            SystemClock::new(),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );
        let (addr, server_task) = spawn_worker_server(WorkerGrpcService::with_defaults(
            controller.clone(),
            Arc::new(gang_testkit::MemoryContentStore::new()),
        ))
        .await?;

        // Connect gen1 and gen2 for the EXACT SAME (small, inst-renew) concurrently.
        let gen1 = connect_raw_worker(addr, "inst-renew").await?;
        let mut gen2 = connect_raw_worker(addr, "inst-renew").await?;

        assert!(gen1.worker_id.starts_with("inst-renew-"));
        assert!(gen2.worker_id.starts_with("inst-renew-"));
        assert_ne!(gen1.worker_id, gen2.worker_id);

        let both = wait_for_snapshot(&controller, |s| s.workers.len() == 2).await?;
        assert_eq!(both.workers.len(), 2);

        // Drop gen1 abruptly; gen2 must remain connected and healthy!
        drop(gen1);

        let only_gen2 = wait_for_snapshot(&controller, |s| s.workers.len() == 1).await?;
        assert_eq!(only_gen2.workers.len(), 1);
        assert_eq!(only_gen2.workers[0].id.as_str(), gen2.worker_id);

        // Execute an action and confirm gen2 receives, runs, and completes it cleanly.
        let mut watch = controller
            .execute(exec_spec("operations/gen2-op", b"gen2-op", None))
            .await?;
        let Some(ControllerKind::Assign(assign)) =
            gen2.inbound.message().await?.and_then(|m| m.kind)
        else {
            return Err("expected Assign frame on gen2".into());
        };
        gen2.tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Accept(proto::Accept {
                    attempt: assign.attempt.clone(),
                })),
            })
            .await?;
        gen2.tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Executed(proto::Executed {
                    attempt: assign.attempt.clone(),
                    usage: Some(proto::ResourceUsage::default()),
                    kill: None,
                    status: None,
                    exit_code: 0,
                    started: None,
                    finished: None,
                })),
            })
            .await?;
        gen2.tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::Completed(proto::Completed {
                    attempt: assign.attempt,
                    outcome: Some(CompletedOutcome::Result(ActionResult {
                        exit_code: 0,
                        stdout_raw: RawBytes::from_static(b"gen2-ok"),
                        ..Default::default()
                    })),
                })),
            })
            .await?;

        let terminal = wait_terminal(&mut watch).await?;
        assert_eq!(terminal.update, OperationUpdate::Completed);
        assert_eq!(
            terminal.result.as_ref().map(|r| r.stdout_raw.as_ref()),
            Some(&b"gen2-ok"[..])
        );

        drop(gen2);
        let empty = wait_for_snapshot(&controller, |s| s.workers.is_empty()).await?;
        assert!(empty.workers.is_empty());

        controller.shutdown().await?;
        actor_task.await?;
        server_task.abort();
        Ok(())
    }
}
