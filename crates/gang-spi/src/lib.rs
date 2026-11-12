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
mod credential_port;
mod errors;
mod fleet_options;
mod identity_port;
mod isolation_port;
mod lifecycle_port;
mod limits_port;
mod plugin;
mod private;
mod provisioner_port;
mod repository_port;
mod scheduler_port;
mod scratch_port;
mod signer_port;

pub use authentication_port::{
    Authenticated, AuthenticationHost, AuthenticationPort, AuthorityRequest, Capabilities,
    CertificateIssue, CertificateTarget, ClientCertificates, CredentialLifecyclePort, Evidence,
    IssuedCertificate, IssuedToken, Peer, Scheme, TokenIssue,
};
pub use cache_port::{CacheStorePort, RenderedStoreSpec};
pub use credential_port::WorkloadCredentialPort;
pub use errors::{BoxedError, BuildError, PortError, Rejection, Subject};
pub use fleet_options::{FleetOptions, OptionSpec};
pub use gang_macros::{FleetOptions, PortFailure, compose, port};
pub use identity_port::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport,
    ProofVerifierPort,
};
pub use isolation_port::{
    ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort,
    EXEC_LINK_NAME, NETWORK_ENV, NetworkRequest, RESET_LINK_NAME, SandboxedAction, Termination,
};
pub use lifecycle_port::{InstanceIdentity, NodeLifecyclePort, PreemptionNotice};
pub use limits_port::{
    ActionLimits, ActionLimitsPort, ActionScope, ActionUsage, CPU_COUNT_ENV, GUARD_LINK_NAME,
    JOIN_LINK_NAME, MEMORY_KB_ENV,
};
pub use plugin::{Plugin, PluginContext, SandboxPrograms};
pub use private::__private;
pub use provisioner_port::{
    FleetInstance, FleetSupply, InstanceExit, ProvisionerPort, ServerEndpoints, Sidecar,
};
pub use repository_port::{
    CertStatus, IssuedCertRecord, PrincipalRecord, RecordedScalingEvent, ScalingAuditEvent,
    ScalingEventKind, ScalingEventSeq, StateRepositoryPort,
};
pub use scheduler_port::{
    Accepted, ActionInfo, ActionsView, ApiRequestBody, ApiResponseBody, ApiService,
    BoxedConnection, ClientTls, Connection, ControllerConnections, ControllerEndpoints,
    ControllerReady, ControllerSpec, Listening, ListingLimits, QueuedActionView, QueuedDemand,
    RequestGate, RunningActionView, SchedulerPort, SchedulerSnapshot, Slots, StoreAccess,
    WorkerSpec, WorkerState, WorkerTls, full_body,
};
pub use scratch_port::{MountedScratchVolume, ScratchVolumePort};
pub use signer_port::{CertificateSignerPort, SignatureAlgorithm};
