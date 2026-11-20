//! The host a controller runs on, as the provisioner manages it: when the provisioner retires
//! it, and the milestones the provisioner waits for before it moves the host on.

use gang_clock::Timestamp;
use gang_types::InstanceId;

use crate::{PortError, port};

/// The controller's host under the provisioner.
#[port]
pub trait ControllerHostPort: Send + Sync {
    /// Returns the host incarnation that keeps its handoff priority across process restarts.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the host's stable identity cannot be obtained.
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError>;

    /// Resolves once the provisioner starts retiring the host, so the controller hands off.
    /// Never resolves where nothing retires hosts.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be asked.
    async fn retiring(&self) -> Result<(), PortError>;

    /// Tells the provisioner the host reached `milestone`. Where the provisioner holds the host
    /// for a milestone (an Auto Scaling lifecycle hook), this lets it proceed; elsewhere there is
    /// nothing to tell.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be told.
    async fn reached(&self, milestone: HostMilestone) -> Result<(), PortError>;
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

/// A point in a controller host's life the provisioner may wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMilestone {
    /// The controller passed every startup check and serves clients: the host may take traffic.
    Serving,
    /// The controller handed off and is exiting: the host may be removed.
    HandedOff,
}
