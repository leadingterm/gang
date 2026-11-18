//! The host a controller runs on, as the provisioner manages it: when the provisioner retires
//! it, and the milestones the provisioner waits for before it moves the host on.

use crate::{PortError, port};

/// The controller's host under the provisioner.
#[port]
pub trait ControllerHostPort: Send + Sync {
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

/// A point in a controller host's life the provisioner may wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMilestone {
    /// The controller passed every startup check and serves clients: the host may take traffic.
    Serving,
    /// The controller handed off and is exiting: the host may be removed.
    HandedOff,
}
