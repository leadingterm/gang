//! Controller candidate identities stored in the leader lease repository.

use gang_clock::Timestamp;
use gang_types::InstanceId;

use crate::{PortError, port};

/// Supplies the stable controller candidate identity used by the lease repository.
#[port]
pub trait ControllerIdentityPort: Send + Sync {
    /// Returns this controller's stable identity.
    ///
    /// # Errors
    /// Returns an error if the identity cannot be obtained.
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError>;
}

/// The stable host identity used to rank controller processes for handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerHostIdentity {
    /// A logical host whose first registration in the database fixes its priority.
    Persistent(InstanceId),
    /// A host whose immutable launch metadata fixes its priority.
    Launched {
        /// The host's launch time.
        at: Timestamp,
        /// The instance identity, breaking ties between equal launch times.
        instance: InstanceId,
    },
}
