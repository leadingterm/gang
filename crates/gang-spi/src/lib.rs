//! Capability ports and the compile-time plugin contract for Gang.
//!
//! A backend implements [`Plugin`] and the ports of its domain; `gang-main` composes the
//! backends with [`compose!`]. Ports are declared with [`port`] and native `async fn`.
//!
//! ```
//! use gang_spi::{Plugin, PluginContext, PortError};
//! use serde::{Deserialize, Serialize};
//!
//! struct Echo;
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! #[serde(deny_unknown_fields)]
//! struct EchoConfig {}
//!
//! impl Plugin for Echo {
//!     type Config = EchoConfig;
//!
//!     fn build(_config: EchoConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
//!         Ok(Self)
//!     }
//! }
//!
//! let context = PluginContext {
//!     config_path: std::path::Path::new("gang.toml"),
//! };
//! assert!(Echo::build(EchoConfig {}, &context).is_ok());
//! ```

mod authentication_port;
mod cache_port;
mod candidate_priority;
mod counter;
mod credential_port;
mod errors;
mod expiring_port;
mod fence;
mod fleet_options;
mod host_port;
mod identity_port;
mod infrastructure;
mod isolation_port;
mod lease_port;
mod lifecycle_port;
mod limits_port;
mod observation_port;
mod plugin;
mod private;
mod provisioner_port;
mod repository_port;
mod scheduler_port;
mod schema;
mod scratch_port;
mod signer_port;
mod stored_scaling;

pub use authentication_port::{
    Authenticated, AuthenticationHost, AuthenticationPort, AuthorityRequest, Capabilities,
    CertificateIssue, CertificateTarget, ClientCertificates, CredentialLifecyclePort, Evidence,
    IssuedCertificate, IssuedToken, Peer, Scheme, TokenIssue,
};
pub use cache_port::{CacheStorePort, MeasuredUsage, RenderedStoreSpec};
pub use candidate_priority::{CandidatePriority, CandidatePriorityRef};
pub use credential_port::WorkloadCredentialPort;
pub use errors::{BoxedError, BuildError, CandidatePriorityError, PortError, Rejection, Subject};
pub use expiring_port::{
    ApplyRecords, ExpiringRecordPort, Insert, MAX_RECORD_BODY_BYTES,
    MAX_RECORD_BODY_BYTES_PER_BATCH, MAX_RECORD_CHANGES, Record, RecordBatchError, RecordBatchSeq,
    RecordChange, RecordId, RecordKey, RecordKind, RecordVersion, Remove, Replace,
    validate_record_changes,
};
pub use fence::{Deadline, Fence, Held, Keeper, LeaseLapsed, RunError, Work};
pub use fleet_options::{FleetOptions, OptionSpec};
pub use gang_macros::{FleetOptions, PortFailure, compose, port};
pub use host_port::{ControllerHostIdentity, ControllerHostPort, HostMilestone};
pub use identity_port::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport,
    ProofVerifierPort,
};
pub use infrastructure::{
    FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind,
    InfrastructureInterruptionAction, InfrastructureTerminationReason,
};
pub use isolation_port::{
    ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort,
    EXEC_LINK_NAME, NETWORK_ENV, NetworkAccess, RESET_LINK_NAME, SandboxedAction, Termination,
};
pub use lease_port::{
    Acquisition, CandidateGeneration, ControllerId, LeaderLeasePort, Lease, LeaseCandidate,
    LeaseEpoch, Renewal, Takeover,
};
pub use lifecycle_port::{InstanceIdentity, NodeLifecyclePort, PreemptionNotice};
pub use limits_port::{
    ActionLimits, ActionLimitsPort, ActionMemoryPool, ActionScope, ActionUsage, CPU_COUNT_ENV,
    GUARD_LINK_NAME, JOIN_LINK_NAME, MEMORY_KB_ENV, SIDE_CHANNEL_FILE_ENV,
};
pub use observation_port::{
    Call, CallOutcome, Delivery, Executed, ExecutionTimes, Observation, ObservationSink,
    Observations, Origin, Phase, Rpc, Traffic, Transferred,
};
pub use plugin::{Plugin, PluginContext, SandboxPrograms};
pub use private::__private;
pub use provisioner_port::{
    FleetInstance, FleetSupply, InstanceExit, LaunchFailure, ProvisionerPort, RetirementReason,
    ServerEndpoints, Sidecar,
};
pub use repository_port::{
    CertStatus, IssuedCertRecord, PrincipalRecord, RecordedScalingEvent, RevocationEpoch,
    ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort,
};
pub use scheduler_port::{
    Accepted, ActionInfo, ActionsView, Admitted, ApiRequestBody, ApiResponseBody, ApiService,
    BoxedConnection, ClientTls, Connection, ControllerConnections, ControllerEndpoints,
    ControllerReady, ControllerSpec, EngineStore, ExecutingDemand, GateDecision, LeaderUnavailable,
    LearnedHistory, Listening, ListingLimits, QueuedActionView, QueuedDemand, RequestGate,
    RequestScope, RunningActionView, SchedulerPort, SchedulerSnapshot, Slots, StoreAccess,
    StoreKind, WorkerSpec, WorkerState, WorkerTls, full_body,
};
pub use schema::{SchemaPlan, SchemaTooNew, SchemaVersion};
pub use scratch_port::{MountedScratchVolume, ScratchVolumePort};
pub use signer_port::{CertificateSignerPort, SignatureAlgorithm};
pub use stored_scaling::{StoredScalingKind, StoredScalingKindError};
