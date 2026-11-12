//! Node lifecycle: who this node is, whether the provisioner is taking it back, and whether it is parked.

use gang_clock::Timestamp;
use gang_types::{FleetName, InstanceId, Isa};

use crate::{PortError, port};

/// The node the process runs on.
#[port]
pub trait NodeLifecyclePort: Send + Sync {
    /// The node's instance id and instruction set.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot describe the node.
    async fn identity(&self) -> Result<InstanceIdentity, PortError>;

    /// The pending preemption of this node, if the provisioner announced one. Callers poll.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be asked.
    async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, PortError>;

    /// Whether the provisioner keeps this node out of service (an instance heading into, or
    /// resting in, a warm pool). A parked node does not enroll or register workers. Callers poll.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be asked.
    async fn parked(&self) -> Result<bool, PortError>;
}

/// Who a node is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceIdentity {
    /// The provisioner's id for the instance.
    pub instance: InstanceId,
    /// The fleet the instance belongs to, as the provisioner recorded it.
    pub fleet: FleetName,
    /// The instruction set the hardware runs; the hardware is the truth.
    pub isa: Isa,
}

/// A provisioner announcement that the node will be terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreemptionNotice {
    /// When the provisioner terminates the node.
    pub terminate_at: Timestamp,
}
