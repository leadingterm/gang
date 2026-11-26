//! Native execution engine adapter implementing [`gang_spi::SchedulerPort`].
//!
//! Houses the controller actor shell, public REAPI v2 and internal worker gRPC services, listener
//! and handoff transport plumbing, and the worker action execution runtime.

mod acquisition;
mod actor;
mod controller;
mod gate;
mod handoff;
mod observer;
mod reapi_ac;
mod reapi_bytestream;
mod reapi_capabilities;
mod reapi_cas;
mod refusals;
mod serve;
mod standing;
mod worker_runner;
mod worker_sampling;
mod worker_session;

pub use actor::{
    ActorConfig, ClientMsg, CompletionPayload, ControlMsg, ControllerRuntime,
    DEFAULT_ASSIGN_INLINE_MAX, DEFAULT_SCHEDULER_BATCH, DEFAULT_SCHEDULER_QUEUE_DEPTH,
    DEFAULT_WORKER_OUTBOUND_DEPTH, ExecuteSpec, InlineActionPayload, OperationWatch,
    OperationWatchState, RegisteredWorker, WorkerMsg, WorkerOutbound, map_kill_reason,
    select_inline_payload,
};
pub use controller::{
    ControllerClosed, ControllerExecuteError, ControllerHandle, ControllerRegisterError,
    LearnedSlot, spawn_controller,
};
pub use reapi_ac::ActionCacheService;
pub use reapi_bytestream::{ByteStreamConfig, ByteStreamService};
pub use reapi_capabilities::{CapabilitiesConfig, CapabilitiesService};
pub use reapi_cas::{BlobPermit, CasConfig, CasService, InflightBlobBudget};
pub use worker_runner::{
    CPU_WEIGHT_FILE, CapturedOutput, DEFAULT_MAX_OUTPUT_BYTES, ExecutionTimestamps,
    ORPHAN_REAP_INTERVAL, READ_ONLY_TREES_FILE, REPORT_FILE, ReadOnlyBind, RunOutcome, RunSpec,
    RunnerError, SIDE_CHANNELS_DIR, SideChannelDir, SideChannelInputs, SideChannelReport,
    become_subreaper, build_action_environment, encode_read_only_trees, parse_side_channel_report,
    reap_orphaned_zombies, run_attempt, start_orphan_reaper,
};
pub use worker_sampling::{
    AttemptCgroupSample, AttemptRegistration, AttemptUsage, GraveyardPending, KillTrigger,
    KillWaiter, MemoryPoolSample, SampleProbe, SamplingConfig, SamplingRegistry, SystemProbe,
    TrackedAttemptGuard, WorkerSampler, kill_channel, parse_cgroup_u64, parse_cpu_stat_usage,
    parse_memory_events_oom_kills, read_cgroup_sample, read_cgroup_side_channel,
    read_memory_pool_sample,
};
pub use worker_session::{
    ConnectError, ConnectedChannel, SessionConfig, SessionConnector, SessionError, SessionEvent,
    SessionOutbound, SessionSendError, TelemetrySource, backoff_delay, cache_stats_to_proto,
    generate_worker_id, run_session,
};
