//! Workload credentials: how a worker proves who it is without a stored secret.

use gang_types::Proof;

use crate::{PortError, port};

/// The credential a workload on this provisioner can obtain.
#[port]
pub trait WorkloadCredentialPort: Send + Sync {
    /// Obtains the proof the workload presents to the identity provider named by the caller.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot issue the proof.
    async fn workload_credential(&self) -> Result<Proof, PortError>;
}
