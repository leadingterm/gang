//! Native execution engine adapter implementing [`gang_spi::SchedulerPort`].
//!
//! Houses the controller actor shell, public REAPI v2 and internal worker gRPC services, listener
//! and handoff transport plumbing, and the worker action execution runtime.

mod acquisition;
mod actor;
mod config;
mod controller;
mod engine;
mod gate;
mod handoff;
mod legacy_worker;
mod observer;
mod reapi_ac;
mod reapi_bytestream;
mod reapi_capabilities;
mod reapi_cas;
mod reapi_execution;
mod refusals;
mod serve;
mod standing;
mod worker;
mod worker_graveyard;
mod worker_runner;
mod worker_sampling;
mod worker_service;
mod worker_session;
mod worker_staging;
mod worker_upload;

pub use actor::{
    ActorConfig, ClientMsg, CompletionPayload, ControlMsg, ControllerRuntime,
    DEFAULT_ASSIGN_INLINE_MAX, DEFAULT_SCHEDULER_BATCH, DEFAULT_SCHEDULER_QUEUE_DEPTH,
    DEFAULT_WORKER_OUTBOUND_DEPTH, ExecuteSpec, InlineActionPayload, OperationWatch,
    OperationWatchState, RegisteredWorker, WorkerMsg, WorkerOutbound, map_kill_reason,
    select_inline_payload,
};
pub use config::{EngineConfig, EngineConfigError};
pub use controller::{
    ControllerClosed, ControllerExecuteError, ControllerHandle, ControllerRegisterError,
    LearnedSlot, spawn_controller,
};
pub use engine::{Engine, EngineControllerSpec, EngineError, TlsSessionConnector};
pub use gang_spi::{ControllerStores, WorkerStores};
pub use reapi_ac::ActionCacheService;
pub use reapi_bytestream::{ByteStreamConfig, ByteStreamService};
pub use reapi_capabilities::{CapabilitiesConfig, CapabilitiesService};
pub use reapi_cas::{BlobPermit, CasConfig, CasService, InflightBlobBudget};
pub use reapi_execution::{ExecutionService, ExecutionServiceConfig, OperationsService};
pub use worker::{DEFAULT_WORKER_MAX_MESSAGE_SIZE, WorkerError, WorkerRuntimeSpec, run_worker};
pub use worker_graveyard::{GRAVEYARD_DIR, Graveyard, GraveyardError};
pub use worker_runner::{
    CPU_WEIGHT_FILE, CapturedOutput, DEFAULT_MAX_OUTPUT_BYTES, ExecutionTimestamps,
    ORPHAN_REAP_INTERVAL, READ_ONLY_TREES_FILE, REPORT_FILE, ReadOnlyBind, RunOutcome, RunSpec,
    RunnerError, SIDE_CHANNELS_DIR, SideChannelDir, SideChannelInputs, SideChannelReport,
    become_subreaper, build_action_environment, encode_read_only_trees, parse_side_channel_report,
    reap_orphaned_zombies, run_attempt, start_orphan_reaper,
};
pub use worker_sampling::{
    AttemptCgroupSample, AttemptRegistration, AttemptUsage, CpuPoolSample, GraveyardPending,
    KillTrigger, KillWaiter, MemoryPoolSample, SampleProbe, SamplingConfig, SamplingRegistry,
    SystemProbe, TrackedAttemptGuard, WorkerSampler, kill_channel, parse_cgroup_u64,
    parse_cpu_stat_usage, parse_memory_events_oom_kills, read_cgroup_sample,
    read_cgroup_side_channel, read_memory_pool_sample,
};
pub use worker_service::{
    DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_WORKER_KEEPALIVE_INTERVAL, WorkerGrpcService,
    WorkerServiceConfig, cache_stats_from_proto,
};
pub use worker_session::{
    ConnectError, ConnectedChannel, SessionConfig, SessionConnector, SessionError, SessionEvent,
    SessionOutbound, SessionSendError, TelemetrySource, backoff_delay, cache_stats_to_proto,
    generate_worker_id, run_session,
};
pub use worker_staging::{
    BoundTree, DirectoryCache, RemoteTreeSource, StageConfig, StagedAttempt, StagingError,
    stage_attempt,
};
pub use worker_upload::{
    CapturedOutput as UploadCapturedOutput, ExecutionTiming, UploadConfig, UploadError,
    UploadInput, UploadOutcome, upload_outputs,
};
