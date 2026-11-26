//! `REAPI v2` `Execution` and `google.longrunning.Operations` gRPC services (`§3.2`, `§5.10`,
//! `§8.1`).
//!
//! Implements `Execution/{Execute, WaitExecution}` and
//! `Operations/{GetOperation, CancelOperation}` over [`ControllerHandle`], [`ContentStore`], and
//! [`ActionStore`]. `ListOperations`, `DeleteOperation`, and `WaitOperation` return
//! `UNIMPLEMENTED` (`§8.1`, Q3).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use gang_clock::{Clock, SystemClock};
use gang_reapi::build::bazel::remote::execution::v2::execution_server::{
    Execution, ExecutionServer,
};
use gang_reapi::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest as ProtoDigest, ExecuteOperationMetadata, ExecuteRequest,
    ExecuteResponse, RequestMetadata, WaitExecutionRequest, digest_function, execution_stage,
};
use gang_reapi::google::longrunning::operations_server::{Operations, OperationsServer};
use gang_reapi::google::longrunning::{
    CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest,
    ListOperationsResponse, Operation, WaitOperationRequest, operation,
};
use gang_reapi::google::rpc::{Code as RpcCode, Status as RpcStatus};
use gang_reapi::to_proto;
use gang_scheduler::{
    AdmissionError, AdmissionRequest, ExecuteError, OperationUpdate, TerminalFailure,
};
use gang_spi::{ActionStore, Authenticated, ByteRange, ContentStore, Origin};
use gang_types::{
    AttemptId, Digest, Identity, Mnemonic, NonZeroDuration, OperationName, SourceId, TargetLabel,
};
use opentelemetry::Context as OtelContext;
use opentelemetry::baggage::BaggageExt as _;
use parking_lot::Mutex;
use prost::Message as _;
use tonic::{Request, Response, Status};

use crate::actor::{ExecuteSpec, InlineActionPayload, OperationWatch, OperationWatchState};
use crate::controller::{ControllerExecuteError, ControllerHandle};
use crate::observer::{ENDUSER_ID, origin as build_origin};
use crate::reapi_cas::{map_port_error, parse_proto_digest, validate_sha256_function};

/// Default interval between keepalive `Operation` frames on an idle stream.
const DEFAULT_KEEPALIVE_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(30);

/// Default maximum number of operation metadata entries retained for `GetOperation` /
/// `WaitExecution` lookups after actor watch pruning.
const DEFAULT_MAX_TRACKED_OPERATIONS: usize = 16_384;

/// Standard gRPC binary metadata header carrying serialized Bazel `RequestMetadata`.
const REQUEST_METADATA_HEADER: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

/// OpenTelemetry baggage key used to carry base64-encoded Bazel `RequestMetadata`.
const BAZEL_METADATA_KEY: &str = "bazel.metadata";

/// Protobuf `Any` type URL for `ExecuteOperationMetadata`.
const EXECUTE_OPERATION_METADATA_TYPE_URL: &str =
    "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata";

/// Protobuf `Any` type URL for `ExecuteResponse`.
const EXECUTE_RESPONSE_TYPE_URL: &str =
    "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse";

/// Configuration for [`ExecutionService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServiceConfig {
    /// Interval at which open `Execute` and `WaitExecution` streams emit a keepalive `Operation`
    /// frame when no stage change has occurred (`§8.1`).
    pub keepalive_interval: NonZeroDuration,
}

impl Default for ExecutionServiceConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
        }
    }
}

#[derive(Debug, Clone)]
struct OperationEntry {
    action_digest: Digest,
    terminal: Option<OperationWatchState>,
}

#[derive(Debug, Default)]
struct RegistryState {
    entries: HashMap<OperationName, OperationEntry>,
    order: VecDeque<OperationName>,
}

/// Bounded in-memory cache of operation `action_digest` and terminal `OperationWatchState` values
/// shared between [`ExecutionService`] and [`OperationsService`].
#[derive(Debug)]
struct OperationRegistry {
    max_entries: usize,
    state: Mutex<RegistryState>,
}

impl OperationRegistry {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            state: Mutex::new(RegistryState::default()),
        }
    }

    fn record_admitted(&self, operation: &OperationName, action_digest: Digest) {
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.get_mut(operation) {
            entry.action_digest = action_digest;
            return;
        }
        while state.entries.len() >= self.max_entries {
            if let Some(oldest) = state.order.pop_front() {
                state.entries.remove(&oldest);
            } else {
                break;
            }
        }
        state.entries.insert(
            operation.clone(),
            OperationEntry {
                action_digest,
                terminal: None,
            },
        );
        state.order.push_back(operation.clone());
    }

    fn record_terminal(&self, watch_state: &OperationWatchState) {
        if !watch_state.is_terminal() {
            return;
        }
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.get_mut(&watch_state.operation)
            && entry.terminal.is_none()
        {
            entry.terminal = Some(watch_state.clone());
        }
    }

    fn action_digest(&self, operation: &OperationName) -> Option<Digest> {
        self.state
            .lock()
            .entries
            .get(operation)
            .map(|entry| entry.action_digest)
    }

    fn terminal_state(&self, operation: &OperationName) -> Option<OperationWatchState> {
        self.state
            .lock()
            .entries
            .get(operation)
            .and_then(|entry| entry.terminal.clone())
    }
}

/// `REAPI v2` `Execution` gRPC server backed by [`ControllerHandle`], [`ContentStore`], and
/// [`ActionStore`].
#[derive(Clone)]
pub struct ExecutionService<C, A> {
    controller: ControllerHandle,
    content: Arc<C>,
    actions: Arc<A>,
    clock: Arc<dyn Clock>,
    config: ExecutionServiceConfig,
    registry: Arc<OperationRegistry>,
}

impl<C, A> std::fmt::Debug for ExecutionService<C, A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionService")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<C: ContentStore, A: ActionStore> ExecutionService<C, A> {
    /// Creates an [`ExecutionService`] with explicit `config` and the system clock.
    #[must_use]
    pub fn new(
        controller: ControllerHandle,
        content: Arc<C>,
        actions: Arc<A>,
        config: ExecutionServiceConfig,
    ) -> Self {
        Self {
            controller,
            content,
            actions,
            clock: Arc::new(SystemClock::new()),
            config,
            registry: Arc::new(OperationRegistry::new(DEFAULT_MAX_TRACKED_OPERATIONS)),
        }
    }

    /// Creates an [`ExecutionService`] with default [`ExecutionServiceConfig`].
    #[must_use]
    pub fn with_defaults(controller: ControllerHandle, content: Arc<C>, actions: Arc<A>) -> Self {
        Self::new(
            controller,
            content,
            actions,
            ExecutionServiceConfig::default(),
        )
    }

    /// Overrides the [`Clock`] used for stream keepalive intervals (useful in tests).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Returns an [`OperationsService`] bound to the same controller and operation metadata
    /// registry as this [`ExecutionService`].
    #[must_use]
    pub fn operations_service(&self) -> OperationsService {
        OperationsService {
            controller: self.controller.clone(),
            registry: Some(Arc::clone(&self.registry)),
        }
    }

    /// Wraps this service in a tonic [`ExecutionServer`].
    #[must_use]
    pub fn into_server(self) -> ExecutionServer<Self> {
        ExecutionServer::new(self)
    }
}

/// `google.longrunning.Operations` gRPC server backed by [`ControllerHandle`].
///
/// Implements `GetOperation` and `CancelOperation`. `ListOperations`, `DeleteOperation`, and
/// `WaitOperation` return `UNIMPLEMENTED` (`§8.1`, Q3).
#[derive(Debug, Clone)]
pub struct OperationsService {
    controller: ControllerHandle,
    registry: Option<Arc<OperationRegistry>>,
}

impl OperationsService {
    /// Creates a standalone [`OperationsService`] backed by `controller`.
    #[must_use]
    pub const fn new(controller: ControllerHandle) -> Self {
        Self {
            controller,
            registry: None,
        }
    }

    /// Wraps this service in a tonic [`OperationsServer`].
    #[must_use]
    pub fn into_server(self) -> OperationsServer<Self> {
        OperationsServer::new(self)
    }
}

struct CallerMetadata {
    caller: Option<Arc<Identity>>,
    origin: Option<Origin>,
    source: Option<SourceId>,
    build: Option<gang_types::BuildId>,
    target: Option<TargetLabel>,
    mnemonic: Option<Mnemonic>,
}

fn decode_base64_request_metadata(encoded: &str) -> Option<RequestMetadata> {
    let decoded = BASE64_STANDARD_NO_PAD
        .decode(encoded.as_bytes())
        .or_else(|_| BASE64_STANDARD.decode(encoded.as_bytes()))
        .ok()?;
    RequestMetadata::decode(decoded.as_slice()).ok()
}

fn extract_caller_metadata<T>(request: &Request<T>) -> CallerMetadata {
    let otel = OtelContext::current();
    let caller = request
        .extensions()
        .get::<Authenticated>()
        .map(|auth| Arc::clone(&auth.identity))
        .or_else(|| {
            otel.baggage()
                .get(ENDUSER_ID)
                .and_then(|value| value.as_str().parse::<Identity>().ok().map(Arc::new))
        });

    let metadata = request
        .extensions()
        .get::<RequestMetadata>()
        .cloned()
        .or_else(|| {
            otel.baggage()
                .get(BAZEL_METADATA_KEY)
                .and_then(|value| decode_base64_request_metadata(value.as_str()))
        })
        .or_else(|| {
            request
                .metadata()
                .get_bin(REQUEST_METADATA_HEADER)
                .and_then(|value| value.to_bytes().ok())
                .and_then(|bytes| RequestMetadata::decode(bytes.as_ref()).ok())
        });

    let origin = match (&caller, &metadata) {
        (Some(identity), Some(meta)) => build_origin(Arc::clone(identity), meta),
        _ => None,
    };
    let source = caller.as_deref().map(SourceId::from);
    let build = origin.as_ref().map(|o| o.build.id());
    let target = origin.as_ref().and_then(|o| o.target.clone()).or_else(|| {
        metadata
            .as_ref()
            .filter(|m| !m.target_id.is_empty())
            .and_then(|m| TargetLabel::try_new(&m.target_id).ok())
    });
    let mnemonic = origin
        .as_ref()
        .and_then(|o| o.mnemonic.clone())
        .or_else(|| {
            metadata
                .as_ref()
                .filter(|m| !m.action_mnemonic.is_empty())
                .and_then(|m| Mnemonic::try_new(&m.action_mnemonic).ok())
        });

    CallerMetadata {
        caller,
        origin,
        source,
        build,
        target,
        mnemonic,
    }
}

fn mint_operation_name(instance_name: &str) -> Result<OperationName, Status> {
    let uuid = AttemptId::generate()
        .map_err(|err| Status::internal(format!("failed to generate operation id: {err}")))?;
    let trimmed = instance_name.trim_matches('/');
    let raw = if trimmed.is_empty() {
        format!("operations/{uuid}")
    } else {
        format!("{trimmed}/operations/{uuid}")
    };
    OperationName::try_new(raw)
        .map_err(|err| Status::invalid_argument(format!("invalid instance_name: {err}")))
}

fn parse_operation_name(raw: &str) -> Result<OperationName, Status> {
    OperationName::try_new(raw)
        .map_err(|err| Status::invalid_argument(format!("invalid operation name: {err}")))
}

fn parse_action_timeout(
    timeout: Option<&prost_types::Duration>,
) -> Result<Option<Duration>, Status> {
    let Some(duration) = timeout else {
        return Ok(None);
    };
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(format!(
            "invalid action.timeout: seconds={}, nanos={}",
            duration.seconds, duration.nanos
        )));
    }
    let secs = u64::try_from(duration.seconds)
        .map_err(|_| Status::invalid_argument("action.timeout seconds overflow"))?;
    let nanos = u32::try_from(duration.nanos)
        .map_err(|_| Status::invalid_argument("action.timeout nanos overflow"))?;
    let parsed = Duration::new(secs, nanos);
    Ok((!parsed.is_zero()).then_some(parsed))
}

fn extract_platform_properties(action: &Action, command: &Command) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::new();
    #[allow(deprecated)]
    if let Some(platform) = &command.platform {
        for prop in &platform.properties {
            properties.insert(prop.name.clone(), prop.value.clone());
        }
    }
    if let Some(platform) = &action.platform {
        for prop in &platform.properties {
            properties.insert(prop.name.clone(), prop.value.clone());
        }
    }
    properties
}

async fn read_cas_blob<C: ContentStore>(
    content: &C,
    digest: Digest,
    kind: &str,
) -> Result<Bytes, Status> {
    if digest.is_empty() {
        return Ok(Bytes::new());
    }
    let Some(stream) = content
        .read(digest, ByteRange::FULL)
        .await
        .map_err(|err| map_port_error(&err))?
    else {
        return Err(Status::failed_precondition(format!(
            "missing {kind} blob blobs/{}/{}",
            digest.hex(),
            digest.size()
        )));
    };
    stream.collect().await.map_err(|err| map_port_error(&err))
}

fn map_execute_error(error: &ControllerExecuteError) -> Status {
    match error {
        ControllerExecuteError::Closed
        | ControllerExecuteError::Execute(ExecuteError::HandingOff) => {
            Status::unavailable(error.to_string())
        }
        ControllerExecuteError::Execute(ExecuteError::Admission(AdmissionError::Refused(_))) => {
            Status::invalid_argument(error.to_string())
        }
        ControllerExecuteError::Execute(ExecuteError::Admission(AdmissionError::Registry(
            reg_err,
        ))) => match reg_err {
            gang_scheduler::AdmitError::DuplicateName => Status::already_exists(error.to_string()),
            gang_scheduler::AdmitError::SlabFull => Status::resource_exhausted(error.to_string()),
        },
    }
}

fn ok_rpc_status() -> RpcStatus {
    RpcStatus {
        code: RpcCode::Ok as i32,
        message: String::new(),
        details: Vec::new(),
    }
}

fn terminal_failure_message(failure: &TerminalFailure) -> String {
    match failure {
        TerminalFailure::DeadlineExceeded { operation } => {
            format!("action {operation} exceeded execution deadline")
        }
        TerminalFailure::Escalation(esc) => esc.to_string(),
        TerminalFailure::GenericRetriesExhausted {
            operation,
            attempts,
            message,
        } => {
            format!("action {operation} failed after {attempts} attempts: {message}")
        }
        TerminalFailure::WorkerLossExhausted { operation, losses } => {
            format!("action {operation} lost {losses} workers before completion")
        }
    }
}

fn terminal_failure_status(failure: &TerminalFailure) -> RpcStatus {
    let code = match failure {
        TerminalFailure::DeadlineExceeded { .. } => RpcCode::DeadlineExceeded,
        TerminalFailure::Escalation(_) => RpcCode::ResourceExhausted,
        TerminalFailure::GenericRetriesExhausted { .. } => RpcCode::Internal,
        TerminalFailure::WorkerLossExhausted { .. } => RpcCode::Unavailable,
    };
    RpcStatus {
        code: code as i32,
        message: terminal_failure_message(failure),
        details: Vec::new(),
    }
}

fn pack_any<M: prost::Message>(type_url: &str, message: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.to_owned(),
        value: message.encode_to_vec(),
    }
}

fn build_operation_metadata(
    stage: execution_stage::Value,
    action_digest: Option<Digest>,
) -> Result<prost_types::Any, Status> {
    let (proto_digest, digest_fn) = match action_digest {
        Some(digest) => {
            let proto: ProtoDigest = to_proto(&digest)
                .map_err(|err| Status::internal(format!("digest conversion failed: {err}")))?;
            (Some(proto), digest_function::Value::Sha256 as i32)
        }
        None => (None, digest_function::Value::Unknown as i32),
    };
    let meta = ExecuteOperationMetadata {
        stage: stage as i32,
        action_digest: proto_digest,
        digest_function: digest_fn,
        ..Default::default()
    };
    Ok(pack_any(EXECUTE_OPERATION_METADATA_TYPE_URL, &meta))
}

fn build_cache_hit_operation(
    op_name: &OperationName,
    action_digest: Digest,
    action_result: ActionResult,
) -> Result<Operation, Status> {
    let metadata =
        build_operation_metadata(execution_stage::Value::Completed, Some(action_digest))?;
    let response = ExecuteResponse {
        result: Some(action_result),
        cached_result: true,
        status: Some(ok_rpc_status()),
        ..Default::default()
    };
    Ok(Operation {
        name: op_name.as_str().to_owned(),
        metadata: Some(metadata),
        done: true,
        result: Some(operation::Result::Response(pack_any(
            EXECUTE_RESPONSE_TYPE_URL,
            &response,
        ))),
    })
}

fn build_operation(
    state: &OperationWatchState,
    action_digest: Option<Digest>,
) -> Result<Operation, Status> {
    let exec_stage = match &state.update {
        OperationUpdate::Queued => execution_stage::Value::Queued,
        OperationUpdate::Executing { .. } => execution_stage::Value::Executing,
        OperationUpdate::Completed | OperationUpdate::Failed(_) | OperationUpdate::Cancelled => {
            execution_stage::Value::Completed
        }
    };
    let metadata = build_operation_metadata(exec_stage, action_digest)?;

    let (done, result) = match &state.update {
        OperationUpdate::Queued | OperationUpdate::Executing { .. } => (false, None),
        OperationUpdate::Completed => {
            let response = ExecuteResponse {
                result: state.result.clone(),
                cached_result: false,
                status: Some(state.status.clone().unwrap_or_else(ok_rpc_status)),
                ..Default::default()
            };
            (
                true,
                Some(operation::Result::Response(pack_any(
                    EXECUTE_RESPONSE_TYPE_URL,
                    &response,
                ))),
            )
        }
        OperationUpdate::Failed(failure) => {
            let response = ExecuteResponse {
                result: state.result.clone(),
                cached_result: false,
                status: Some(terminal_failure_status(failure)),
                message: terminal_failure_message(failure),
                ..Default::default()
            };
            (
                true,
                Some(operation::Result::Response(pack_any(
                    EXECUTE_RESPONSE_TYPE_URL,
                    &response,
                ))),
            )
        }
        OperationUpdate::Cancelled => (
            true,
            Some(operation::Result::Error(RpcStatus {
                code: RpcCode::Cancelled as i32,
                message: "operation cancelled".to_owned(),
                details: Vec::new(),
            })),
        ),
    };

    Ok(Operation {
        name: state.operation.as_str().to_owned(),
        metadata: Some(metadata),
        done,
        result,
    })
}

fn resolve_watch_state(
    watch: &mut OperationWatch,
    registry: Option<&OperationRegistry>,
) -> (OperationWatchState, Option<Digest>) {
    let raw_state = watch.borrow_and_update().clone();
    let action_digest = registry.and_then(|reg| reg.action_digest(&raw_state.operation));
    let state = if raw_state.is_terminal()
        && raw_state.result.is_none()
        && raw_state.status.is_none()
        && let Some(cached) = registry.and_then(|reg| reg.terminal_state(&raw_state.operation))
    {
        cached
    } else {
        if raw_state.is_terminal()
            && let Some(reg) = registry
        {
            reg.record_terminal(&raw_state);
        }
        raw_state
    };
    (state, action_digest)
}

struct WatchStreamState {
    watch: OperationWatch,
    clock: Arc<dyn Clock>,
    registry: Arc<OperationRegistry>,
    action_digest: Option<Digest>,
    keepalive_interval: Duration,
    last_state: OperationWatchState,
    sent_initial: bool,
    finished: bool,
}

fn operation_stream(
    mut watch: OperationWatch,
    clock: Arc<dyn Clock>,
    registry: Arc<OperationRegistry>,
    keepalive_interval: NonZeroDuration,
) -> BoxStream<'static, Result<Operation, Status>> {
    let (initial_state, action_digest) = resolve_watch_state(&mut watch, Some(&registry));
    let state = WatchStreamState {
        watch,
        clock,
        registry,
        action_digest,
        keepalive_interval: keepalive_interval.get(),
        last_state: initial_state,
        sent_initial: false,
        finished: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.finished {
            return None;
        }
        if !state.sent_initial {
            state.sent_initial = true;
            state.finished = state.last_state.is_terminal();
            let item = build_operation(&state.last_state, state.action_digest);
            return Some((item, state));
        }

        tokio::select! {
            changed = state.watch.changed() => {
                if let Ok(next_state) = changed {
                    state.registry.record_terminal(&next_state);
                    state.finished = next_state.is_terminal();
                    state.last_state = next_state;
                    let item = build_operation(&state.last_state, state.action_digest);
                    Some((item, state))
                } else {
                    state.finished = true;
                    let current = state.watch.current();
                    if current.is_terminal() && current != state.last_state {
                        state.registry.record_terminal(&current);
                        let item = build_operation(&current, state.action_digest);
                        Some((item, state))
                    } else {
                        Some((Err(Status::unavailable("controller actor closed")), state))
                    }
                }
            }
            () = state.clock.sleep(state.keepalive_interval) => {
                let item = build_operation(&state.last_state, state.action_digest);
                Some((item, state))
            }
        }
    }))
}

#[tonic::async_trait]
impl<C: ContentStore, A: ActionStore> Execution for ExecutionService<C, A> {
    type ExecuteStream = BoxStream<'static, Result<Operation, Status>>;
    type WaitExecutionStream = BoxStream<'static, Result<Operation, Status>>;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let caller_meta = extract_caller_metadata(&request);
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;
        let action_digest = parse_proto_digest(req.action_digest.as_ref(), "action_digest")?;
        let op_name = mint_operation_name(&req.instance_name)?;

        if !req.skip_cache_lookup
            && let Some(raw_result) = self
                .actions
                .get(action_digest)
                .await
                .map_err(|err| map_port_error(&err))?
        {
            let action_result = ActionResult::decode(raw_result).map_err(|err| {
                Status::internal(format!(
                    "corrupt cached ActionResult for {action_digest}: {err}"
                ))
            })?;
            let operation = build_cache_hit_operation(&op_name, action_digest, action_result)?;
            return Ok(Response::new(Box::pin(stream::once(std::future::ready(
                Ok(operation),
            )))));
        }

        let action_bytes = read_cas_blob(self.content.as_ref(), action_digest, "action").await?;
        let action = Action::decode(action_bytes.as_ref()).map_err(|err| {
            Status::invalid_argument(format!("malformed Action proto for {action_digest}: {err}"))
        })?;
        let command_digest =
            parse_proto_digest(action.command_digest.as_ref(), "action.command_digest")?;
        let timeout = parse_action_timeout(action.timeout.as_ref())?;

        let command_bytes = read_cas_blob(self.content.as_ref(), command_digest, "command").await?;
        let command = Command::decode(command_bytes.as_ref()).map_err(|err| {
            Status::invalid_argument(format!(
                "malformed Command proto for {command_digest}: {err}"
            ))
        })?;

        let platform_properties = extract_platform_properties(&action, &command);
        let priority = req
            .execution_policy
            .as_ref()
            .map_or(0, |policy| policy.priority);

        let spec = ExecuteSpec {
            request: AdmissionRequest {
                name: op_name,
                action_digest,
                command_digest: command_digest.to_string(),
                platform_properties,
                skip_cache_lookup: req.skip_cache_lookup,
                do_not_cache: action.do_not_cache,
                source: caller_meta.source,
                build: caller_meta.build,
                target: caller_meta.target,
                mnemonic: caller_meta.mnemonic,
                priority,
                initial_watchers: 1,
            },
            caller: caller_meta.caller,
            origin: caller_meta.origin,
            inline: Some(InlineActionPayload {
                action: action_bytes,
                command: command_bytes,
            }),
            timeout,
        };

        let watch = self
            .controller
            .execute(spec)
            .await
            .map_err(|err| map_execute_error(&err))?;
        self.registry.record_admitted(watch.name(), action_digest);

        let stream = operation_stream(
            watch,
            Arc::clone(&self.clock),
            Arc::clone(&self.registry),
            self.config.keepalive_interval,
        );
        Ok(Response::new(stream))
    }

    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        let req = request.into_inner();
        let op_name = parse_operation_name(&req.name)?;
        let Some(watch) = self
            .controller
            .watch(op_name.clone())
            .await
            .map_err(|_| Status::unavailable("controller is shutting down"))?
        else {
            return Err(Status::not_found(format!("operation {op_name} not found")));
        };

        let stream = operation_stream(
            watch,
            Arc::clone(&self.clock),
            Arc::clone(&self.registry),
            self.config.keepalive_interval,
        );
        Ok(Response::new(stream))
    }
}

#[tonic::async_trait]
impl Operations for OperationsService {
    async fn list_operations(
        &self,
        _request: Request<ListOperationsRequest>,
    ) -> Result<Response<ListOperationsResponse>, Status> {
        Err(Status::unimplemented(
            "Operations.ListOperations is not supported",
        ))
    }

    async fn get_operation(
        &self,
        request: Request<GetOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        let req = request.into_inner();
        let op_name = parse_operation_name(&req.name)?;
        let Some(mut watch) = self
            .controller
            .watch(op_name.clone())
            .await
            .map_err(|_| Status::unavailable("controller is shutting down"))?
        else {
            return Err(Status::not_found(format!("operation {op_name} not found")));
        };

        let (state, action_digest) = resolve_watch_state(&mut watch, self.registry.as_deref());
        watch.close().await;
        let operation = build_operation(&state, action_digest)?;
        Ok(Response::new(operation))
    }

    async fn delete_operation(
        &self,
        _request: Request<DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "Operations.DeleteOperation is not supported",
        ))
    }

    async fn cancel_operation(
        &self,
        request: Request<CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let op_name = parse_operation_name(&req.name)?;
        let Some(watch) = self
            .controller
            .watch(op_name.clone())
            .await
            .map_err(|_| Status::unavailable("controller is shutting down"))?
        else {
            return Err(Status::not_found(format!("operation {op_name} not found")));
        };

        let key = watch.key();
        watch.close().await;
        self.controller
            .cancel(key)
            .await
            .map_err(|_| Status::unavailable("controller is shutting down"))?;
        Ok(Response::new(()))
    }

    async fn wait_operation(
        &self,
        _request: Request<WaitOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        Err(Status::unimplemented(
            "Operations.WaitOperation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes as RawBytes;
    use futures::StreamExt as _;
    use gang_clock::{Clock as _, TestClock, Timestamp};
    use gang_config::{ActionsConfig, WholeKib};
    use gang_reapi::build::bazel::remote::execution::v2::execution_server::Execution as _;
    use gang_reapi::build::bazel::remote::execution::v2::platform::Property;
    use gang_reapi::build::bazel::remote::execution::v2::{
        Action, ActionResult, Command, ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse,
        ExecutionPolicy, Platform, RequestMetadata, ToolDetails, WaitExecutionRequest,
        digest_function, execution_stage,
    };
    use gang_reapi::google::longrunning::operations_server::Operations as _;
    use gang_reapi::google::longrunning::{
        CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest,
        WaitOperationRequest, operation,
    };
    use gang_reapi::google::rpc::Code as RpcCode;
    use gang_reapi::leadingterm::gang::worker::v1 as proto;
    use gang_reapi::to_proto;
    use gang_scheduler::{
        AttemptUsage, CompletionStatus, Learned, Scheduler, SchedulerPolicy, WorkerCompleted,
        WorkerEvent, WorkerExecuted, WorkerRegistration,
    };
    use gang_spi::{Authenticated, Observation, ObservationSink, SchedulerSnapshot};
    use gang_testkit::{MemoryActionStore, MemoryContentStore};
    use gang_types::{
        ActionResources, AttemptId, Bytes, CpuWeight, Digest, FleetActionPolicy, FleetCatalog,
        FleetName, FleetProperties, Identity, InstanceId, Isa, NonZeroDuration, Principal,
        ProviderName, Resources, property,
    };
    use prost::Message as _;
    use tonic::metadata::MetadataValue;
    use tonic::{Code, Request};

    use super::{
        EXECUTE_OPERATION_METADATA_TYPE_URL, EXECUTE_RESPONSE_TYPE_URL, ExecutionService,
        ExecutionServiceConfig, OperationsService, REQUEST_METADATA_HEADER,
    };
    use crate::actor::{ActorConfig, CompletionPayload, ControllerRuntime, WorkerOutbound};
    use crate::controller::{ControllerHandle, LearnedSlot, spawn_controller};

    fn test_catalog() -> FleetCatalog {
        let fleet = FleetProperties::new(
            &FleetName::try_new("small").expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        );
        FleetCatalog::try_new(vec![fleet]).expect("catalog")
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
            client_action_timeout: Duration::from_secs(8),
            attempt_budget: Duration::from_secs(30),
            ..SchedulerPolicy::default()
        }
    }

    fn make_scheduler(policy: SchedulerPolicy) -> Scheduler {
        let catalog = test_catalog();
        let learned =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
        Scheduler::new(policy, &catalog, learned).expect("scheduler")
    }

    fn worker_reg(instance: &str, seed: u8) -> WorkerRegistration {
        WorkerRegistration {
            instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
            fleet: FleetName::try_new("small").expect("fleet"),
            gang_version: "0.3.9".to_owned(),
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores: 16_000,
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
            environment: Some("host:test".parse().expect("env")),
            uuid_bytes: [seed; 16],
        }
    }

    fn store_action_and_command(
        content: &MemoryContentStore,
        args: &[&str],
        isa: &str,
        timeout_secs: Option<i64>,
        do_not_cache: bool,
    ) -> (Digest, Digest) {
        let command = Command {
            arguments: args.iter().map(|s| (*s).to_owned()).collect(),
            ..Default::default()
        };
        let command_bytes = RawBytes::from(command.encode_to_vec());
        let command_digest = content.insert(command_bytes);

        let action = Action {
            command_digest: Some(to_proto(&command_digest).expect("command digest")),
            timeout: timeout_secs.map(|seconds| prost_types::Duration { seconds, nanos: 0 }),
            do_not_cache,
            platform: Some(Platform {
                properties: vec![Property {
                    name: property::ISA.to_owned(),
                    value: isa.to_owned(),
                }],
            }),
            ..Default::default()
        };
        let action_bytes = RawBytes::from(action.encode_to_vec());
        let action_digest = content.insert(action_bytes);
        (action_digest, command_digest)
    }

    fn decode_metadata(
        op: &gang_reapi::google::longrunning::Operation,
    ) -> ExecuteOperationMetadata {
        let any = op.metadata.as_ref().expect("operation metadata");
        assert_eq!(any.type_url, EXECUTE_OPERATION_METADATA_TYPE_URL);
        ExecuteOperationMetadata::decode(any.value.as_ref())
            .expect("decode ExecuteOperationMetadata")
    }

    fn decode_response(op: &gang_reapi::google::longrunning::Operation) -> ExecuteResponse {
        match op.result.as_ref().expect("operation result") {
            operation::Result::Response(any) => {
                assert_eq!(any.type_url, EXECUTE_RESPONSE_TYPE_URL);
                ExecuteResponse::decode(any.value.as_ref()).expect("decode ExecuteResponse")
            }
            operation::Result::Error(err) => panic!("expected Response, got Error({err:?})"),
        }
    }

    struct TestHarness {
        controller: ControllerHandle,
        task: tokio::task::JoinHandle<()>,
        content: Arc<MemoryContentStore>,
        actions: Arc<MemoryActionStore>,
        execution: ExecutionService<MemoryContentStore, MemoryActionStore>,
        operations: OperationsService,
    }

    fn setup_service(clock: &Arc<TestClock>, observations: Option<ObservationSink>) -> TestHarness {
        let runtime = ControllerRuntime {
            observations,
            handoff: None,
            control_acquire_timeout: None,
        };
        let (controller, task) = spawn_controller(
            make_scheduler(test_policy()),
            Arc::clone(clock),
            ActorConfig::default(),
            LearnedSlot::empty(),
            runtime,
        );
        let content = Arc::new(MemoryContentStore::new());
        let actions = Arc::new(MemoryActionStore::new());
        let execution = ExecutionService::new(
            controller.clone(),
            Arc::clone(&content),
            Arc::clone(&actions),
            ExecutionServiceConfig {
                keepalive_interval: NonZeroDuration::from_secs(5),
            },
        )
        .with_clock(Arc::clone(clock) as Arc<dyn gang_clock::Clock>);
        let operations = execution.operations_service();
        TestHarness {
            controller,
            task,
            content,
            actions,
            execution,
            operations,
        }
    }

    async fn drive_worker_ok(
        controller: &ControllerHandle,
        clock: &TestClock,
        worker: gang_scheduler::WorkerKey,
        attempt_id: AttemptId,
        stream: &mut futures::stream::BoxStream<
            'static,
            Result<gang_reapi::google::longrunning::Operation, tonic::Status>,
        >,
        result: ActionResult,
    ) {
        let exec_start = clock.wall();
        controller
            .worker_event(
                worker,
                WorkerEvent::Accept {
                    attempt_id: attempt_id.clone(),
                },
            )
            .await
            .expect("accept");
        let exec_op = stream
            .next()
            .await
            .expect("executing")
            .expect("ok executing");
        assert!(!exec_op.done);
        assert_eq!(
            decode_metadata(&exec_op).stage,
            execution_stage::Value::Executing as i32
        );

        clock.advance(Duration::from_secs(2));
        let exec_end = clock.wall();
        controller
            .worker_event(
                worker,
                WorkerEvent::Executed(WorkerExecuted {
                    attempt_id: attempt_id.clone(),
                    usage: AttemptUsage::default(),
                    kill: None,
                    side_channel_memory_failure: false,
                }),
            )
            .await
            .expect("executed");
        controller
            .worker_event_with_completion(
                worker,
                WorkerEvent::Completed(WorkerCompleted {
                    attempt_id,
                    status: CompletionStatus::Ok,
                    execution_timestamps: Some((exec_start, exec_end)),
                }),
                CompletionPayload {
                    result: Some(result),
                    status: None,
                    exit_code: 0,
                    error: false,
                },
            )
            .await
            .expect("completed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn action_cache_hit_short_circuits_without_dispatch() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);
        let mut worker = h
            .controller
            .register_worker(worker_reg("i-cache", 1))
            .await
            .expect("register");

        let (action_digest, _) =
            store_action_and_command(&h.content, &["echo", "hi"], "x86-64", None, false);
        let cached = ActionResult {
            exit_code: 0,
            stdout_raw: RawBytes::from_static(b"cached-stdout"),
            ..Default::default()
        };
        h.actions.insert(action_digest, cached.encode_to_vec());

        // Remove Action from CAS to prove AC hit does not touch CAS or dispatch to workers.
        let _ = h.content.remove(action_digest);

        let mut hit_stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                instance_name: String::new(),
                skip_cache_lookup: false,
                action_digest: Some(to_proto(&action_digest).expect("digest")),
                digest_function: digest_function::Value::Sha256 as i32,
                ..Default::default()
            }))
            .await
            .expect("ac hit")
            .into_inner();

        let hit_op = hit_stream.next().await.expect("single op").expect("ok op");
        assert!(hit_op.done);
        let meta = decode_metadata(&hit_op);
        assert_eq!(meta.stage, execution_stage::Value::Completed as i32);
        assert_eq!(
            meta.action_digest,
            Some(to_proto(&action_digest).expect("digest"))
        );
        let resp = decode_response(&hit_op);
        assert!(resp.cached_result);
        assert_eq!(resp.result, Some(cached));
        assert_eq!(
            resp.status.as_ref().map(|s| s.code),
            Some(RpcCode::Ok as i32)
        );
        assert!(hit_stream.next().await.is_none(), "stream must end");
        assert!(
            worker.outbound.try_recv().is_err(),
            "AC hit must not dispatch to workers"
        );

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn skip_cache_lookup_bypasses_hit_and_streams_full_lifecycle() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);
        let mut worker = h
            .controller
            .register_worker(worker_reg("i-bypass", 1))
            .await
            .expect("register");

        let (action_digest, _) =
            store_action_and_command(&h.content, &["echo", "hi"], "x86-64", None, false);
        h.actions.insert(
            action_digest,
            ActionResult {
                exit_code: 0,
                stdout_raw: RawBytes::from_static(b"cached"),
                ..Default::default()
            }
            .encode_to_vec(),
        );

        let mut bypass_stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                instance_name: "tenant".to_owned(),
                skip_cache_lookup: true,
                action_digest: Some(to_proto(&action_digest).expect("digest")),
                digest_function: digest_function::Value::Sha256 as i32,
                ..Default::default()
            }))
            .await
            .expect("skip cache")
            .into_inner();

        let queued_op = bypass_stream
            .next()
            .await
            .expect("queued")
            .expect("ok queued");
        assert!(!queued_op.done);
        assert!(queued_op.name.starts_with("tenant/operations/"));
        assert_eq!(
            decode_metadata(&queued_op).stage,
            execution_stage::Value::Queued as i32
        );

        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign after skip_cache_lookup");
        };
        assert!(assign.skip_cache_lookup);
        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt");

        let fresh_result = ActionResult {
            exit_code: 0,
            stdout_raw: RawBytes::from_static(b"fresh-stdout"),
            ..Default::default()
        };
        drive_worker_ok(
            &h.controller,
            &clock,
            worker.key,
            attempt_id,
            &mut bypass_stream,
            fresh_result.clone(),
        )
        .await;

        let done_op = bypass_stream
            .next()
            .await
            .expect("completed")
            .expect("ok completed");
        assert!(done_op.done);
        assert_eq!(
            decode_metadata(&done_op).stage,
            execution_stage::Value::Completed as i32
        );
        let done_resp = decode_response(&done_op);
        assert!(!done_resp.cached_result);
        assert_eq!(done_resp.result, Some(fresh_result));
        assert!(bypass_stream.next().await.is_none());

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_action_or_command_returns_failed_precondition() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);

        // 1. Missing Action in CAS -> FAILED_PRECONDITION.
        let missing_action = Digest::from_bytes(b"missing-action");
        let err = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&missing_action).expect("digest")),
                ..Default::default()
            }))
            .await
            .err()
            .expect("missing action error");
        assert_eq!(err.code(), Code::FailedPrecondition);

        // 2. Action present, Command missing in CAS -> FAILED_PRECONDITION.
        let (action_with_missing_cmd, cmd_digest) =
            store_action_and_command(&h.content, &["true"], "x86-64", None, false);
        let _ = h.content.remove(cmd_digest);
        let err = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&action_with_missing_cmd).expect("digest")),
                ..Default::default()
            }))
            .await
            .err()
            .expect("missing command error");
        assert_eq!(err.code(), Code::FailedPrecondition);

        // 3. Malformed Action protobuf -> INVALID_ARGUMENT.
        let corrupt_action = h.content.insert(RawBytes::from_static(&[0xff, 0xff, 0xff]));
        let err = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&corrupt_action).expect("digest")),
                ..Default::default()
            }))
            .await
            .err()
            .expect("corrupt action error");
        assert_eq!(err.code(), Code::InvalidArgument);

        // 4. Malformed action_digest -> INVALID_ARGUMENT.
        let err = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(gang_reapi::build::bazel::remote::execution::v2::Digest {
                    hash: "bad".to_owned(),
                    size_bytes: 1,
                }),
                ..Default::default()
            }))
            .await
            .err()
            .expect("malformed digest error");
        assert_eq!(err.code(), Code::InvalidArgument);

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_stream_emits_keepalive_on_interval() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(5_000)));
        let h = setup_service(&clock, None);
        let (action, _) = store_action_and_command(&h.content, &["wait"], "x86-64", None, false);
        let mut stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&action).expect("digest")),
                ..Default::default()
            }))
            .await
            .expect("admitted")
            .into_inner();

        let queued = stream.next().await.expect("queued").expect("ok");
        assert_eq!(
            decode_metadata(&queued).stage,
            execution_stage::Value::Queued as i32
        );

        let next_fut = stream.next();
        tokio::pin!(next_fut);
        assert!(futures::poll!(&mut next_fut).is_pending());

        clock.advance(Duration::from_secs(5));
        let keepalive = next_fut
            .await
            .expect("keepalive frame")
            .expect("ok keepalive");
        assert!(!keepalive.done);
        assert_eq!(
            decode_metadata(&keepalive).stage,
            execution_stage::Value::Queued as i32
        );

        drop(stream);
        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_extracts_metadata_gate_priority_and_negative_priority() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(5_000)));
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::new(8).expect("8"), NonZeroUsize::MIN);
        let h = setup_service(&clock, Some(sink));

        let (low_action, _) =
            store_action_and_command(&h.content, &["low"], "x86-64", Some(15), true);
        let mut low_stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&low_action).expect("digest")),
                execution_policy: Some(ExecutionPolicy { priority: -10 }),
                ..Default::default()
            }))
            .await
            .expect("negative priority admitted")
            .into_inner();
        let low_queued = low_stream.next().await.expect("low queued").expect("ok");

        let (gate_action, _) =
            store_action_and_command(&h.content, &["gate"], "x86-64", Some(42), false);
        let mut gate_req = Request::new(ExecuteRequest {
            action_digest: Some(to_proto(&gate_action).expect("digest")),
            execution_policy: Some(ExecutionPolicy { priority: 250 }),
            ..Default::default()
        });
        let identity = Arc::new(Identity::new(
            ProviderName::try_new("github-actions").expect("provider"),
            Principal::try_new("alice").expect("principal"),
        ));
        gate_req.extensions_mut().insert(Authenticated {
            identity,
            valid_until: None,
        });
        let req_meta = RequestMetadata {
            tool_invocation_id: "inv-42".to_owned(),
            target_id: "//pkg:gate_bin".to_owned(),
            action_mnemonic: "Rustc".to_owned(),
            tool_details: Some(ToolDetails {
                tool_name: "bazel".to_owned(),
                tool_version: "8.0.0".to_owned(),
            }),
            ..Default::default()
        };
        gate_req.metadata_mut().insert_bin(
            REQUEST_METADATA_HEADER,
            MetadataValue::from_bytes(&req_meta.encode_to_vec()),
        );

        let mut gate_stream = h
            .execution
            .execute(gate_req)
            .await
            .expect("gate admitted")
            .into_inner();
        let gate_queued = gate_stream.next().await.expect("gate queued").expect("ok");

        // Worker with max_inflight = 1 dispatches gate (250) BEFORE low (-10).
        let mut reg = worker_reg("i-prio", 2);
        reg.max_inflight = Some(NonZeroU32::new(1).expect("1"));
        let mut worker = h.controller.register_worker(reg).await.expect("register");

        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        assert_eq!(assign.operation, gate_queued.name);
        assert_eq!(
            assign.cpu_weight,
            CpuWeight::for_priority_and_millicores(250, 2_000).get()
        );
        assert_eq!(assign.target, "//pkg:gate_bin");
        assert_eq!(assign.mnemonic, "Rustc");
        assert_eq!(assign.timeout.as_ref().map(|d| d.seconds), Some(42));
        assert!(!assign.do_not_cache);

        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt");
        drive_worker_ok(
            &h.controller,
            &clock,
            worker.key,
            attempt_id,
            &mut gate_stream,
            ActionResult {
                exit_code: 0,
                ..Default::default()
            },
        )
        .await;
        assert!(gate_stream.next().await.expect("done").expect("ok").done);

        let delivery = observations.next().await.expect("observation");
        let Some(Observation::Executed(obs)) = delivery.into_observation() else {
            panic!("expected Executed observation");
        };
        assert_eq!(obs.origin.build.invocation.as_str(), "inv-42");

        let Some(WorkerOutbound::Assign(low_assign)) = worker.outbound.recv().await else {
            panic!("expected low Assign");
        };
        assert_eq!(low_assign.operation, low_queued.name);
        assert!(low_assign.do_not_cache);
        assert_eq!(low_assign.timeout.as_ref().map(|d| d.seconds), Some(15));

        drop(low_stream);
        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_execution_follows_active_operation_and_rejects_unknown() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);
        let mut worker = h
            .controller
            .register_worker(worker_reg("i-wait", 3))
            .await
            .expect("register");

        let wait_missing = h
            .execution
            .wait_execution(Request::new(WaitExecutionRequest {
                name: "operations/unknown".to_owned(),
            }))
            .await
            .err()
            .expect("wait missing");
        assert_eq!(wait_missing.code(), Code::NotFound);

        let (digest, _) = store_action_and_command(&h.content, &["run1"], "x86-64", None, false);
        let mut exec_stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&digest).expect("digest")),
                ..Default::default()
            }))
            .await
            .expect("execute")
            .into_inner();
        let op_init = exec_stream.next().await.expect("init").expect("ok");
        let op_name = op_init.name.clone();

        let mut wait_stream = h
            .execution
            .wait_execution(Request::new(WaitExecutionRequest {
                name: op_name.clone(),
            }))
            .await
            .expect("wait execution")
            .into_inner();
        let wait_init = wait_stream.next().await.expect("wait init").expect("ok");
        assert_eq!(wait_init.name, op_name);
        assert!(!wait_init.done);
        assert_eq!(
            decode_metadata(&wait_init).action_digest,
            Some(to_proto(&digest).expect("digest"))
        );

        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        let attempt: AttemptId = assign.attempt.parse().expect("attempt");
        let result = ActionResult {
            exit_code: 0,
            stdout_raw: RawBytes::from_static(b"done-1"),
            ..Default::default()
        };
        drive_worker_ok(
            &h.controller,
            &clock,
            worker.key,
            attempt,
            &mut wait_stream,
            result.clone(),
        )
        .await;

        let wait_done = wait_stream.next().await.expect("wait done").expect("ok");
        assert!(wait_done.done);
        assert_eq!(decode_response(&wait_done).result, Some(result.clone()));
        assert!(wait_stream.next().await.is_none());

        // GetOperation after completion preserves action_digest and ActionResult.
        let get_done = h
            .operations
            .get_operation(Request::new(GetOperationRequest { name: op_name }))
            .await
            .expect("get done")
            .into_inner();
        assert!(get_done.done);
        assert_eq!(decode_response(&get_done).result, Some(result));
        assert_eq!(
            decode_metadata(&get_done).action_digest,
            Some(to_proto(&digest).expect("digest"))
        );

        drop(exec_stream);
        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operations_unimplemented_methods_return_unimplemented() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);

        assert_eq!(
            h.operations
                .list_operations(Request::new(ListOperationsRequest::default()))
                .await
                .expect_err("list")
                .code(),
            Code::Unimplemented
        );
        assert_eq!(
            h.operations
                .delete_operation(Request::new(DeleteOperationRequest {
                    name: "operations/any".to_owned(),
                }))
                .await
                .expect_err("delete")
                .code(),
            Code::Unimplemented
        );
        assert_eq!(
            h.operations
                .wait_operation(Request::new(WaitOperationRequest {
                    name: "operations/any".to_owned(),
                    ..Default::default()
                }))
                .await
                .expect_err("wait")
                .code(),
            Code::Unimplemented
        );

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operations_service_handles_get_and_cancel() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);
        let mut worker = h
            .controller
            .register_worker(worker_reg("i-ops", 5))
            .await
            .expect("register");

        assert_eq!(
            h.operations
                .get_operation(Request::new(GetOperationRequest {
                    name: "operations/unknown".to_owned(),
                }))
                .await
                .expect_err("get missing")
                .code(),
            Code::NotFound
        );
        assert_eq!(
            h.operations
                .cancel_operation(Request::new(CancelOperationRequest {
                    name: "operations/unknown".to_owned(),
                }))
                .await
                .expect_err("cancel missing")
                .code(),
            Code::NotFound
        );

        let (digest, _) = store_action_and_command(&h.content, &["run2"], "x86-64", None, false);
        let mut stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&digest).expect("digest")),
                ..Default::default()
            }))
            .await
            .expect("execute")
            .into_inner();
        let op_name = stream.next().await.expect("init").expect("ok").name;

        let snap = h
            .operations
            .get_operation(Request::new(GetOperationRequest {
                name: op_name.clone(),
            }))
            .await
            .expect("get operation")
            .into_inner();
        assert!(!snap.done);
        assert_eq!(snap.name, op_name);

        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        let attempt: AttemptId = assign.attempt.parse().expect("attempt");
        h.controller
            .worker_event(
                worker.key,
                WorkerEvent::Accept {
                    attempt_id: attempt,
                },
            )
            .await
            .expect("accept");
        let _ = stream.next().await.expect("exec").expect("ok");

        h.operations
            .cancel_operation(Request::new(CancelOperationRequest { name: op_name }))
            .await
            .expect("cancel op");

        let kill = worker.outbound.recv().await.expect("kill");
        assert!(matches!(
            kill,
            WorkerOutbound::Kill(proto::Kill { attempt, reason })
                if attempt == assign.attempt && reason == proto::KillReason::Cancelled as i32
        ));

        let cancelled_op = stream.next().await.expect("cancelled").expect("ok");
        assert!(cancelled_op.done);
        match cancelled_op.result {
            Some(operation::Result::Error(status)) => {
                assert_eq!(status.code, RpcCode::Cancelled as i32);
            }
            other => panic!("expected Error(Cancelled), got {other:?}"),
        }

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_client_stream_triggers_unwatch_and_client_less_expiry() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let h = setup_service(&clock, None);
        let mut worker = h
            .controller
            .register_worker(worker_reg("i-drop", 4))
            .await
            .expect("register");

        let (digest, _) =
            store_action_and_command(&h.content, &["sleep", "60"], "x86-64", None, false);
        let mut stream = h
            .execution
            .execute(Request::new(ExecuteRequest {
                action_digest: Some(to_proto(&digest).expect("digest")),
                ..Default::default()
            }))
            .await
            .expect("execute")
            .into_inner();
        let _ = stream.next().await.expect("queued").expect("ok");

        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt");
        h.controller
            .worker_event(worker.key, WorkerEvent::Accept { attempt_id })
            .await
            .expect("accept");
        let _ = stream.next().await.expect("executing").expect("ok");

        // Drop client stream -> OperationWatch::drop sends Unwatch -> arms client_action_timeout (8s).
        drop(stream);
        let mut snap = SchedulerSnapshot::default();
        h.controller
            .snapshot(&mut snap)
            .await
            .expect("sync unwatch");

        clock.advance(Duration::from_secs(8));
        let kill = worker.outbound.recv().await.expect("client-less kill");
        assert!(matches!(
            kill,
            WorkerOutbound::Kill(proto::Kill { attempt, reason })
                if attempt == assign.attempt && reason == proto::KillReason::Cancelled as i32
        ));

        h.controller.shutdown().await.expect("shutdown");
        h.task.await.expect("actor joined");
    }
}
