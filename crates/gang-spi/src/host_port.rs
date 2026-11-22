//! The host a controller runs on, as the provisioner manages it: when the provisioner retires
//! it, and the milestones the provisioner waits for before it moves the host on.

use gang_clock::Timestamp;
use gang_types::InstanceId;

use crate::{PortError, port};

/// The controller's host under the provisioner.
#[port]
pub trait ControllerHostPort: Send + Sync {
    /// Returns the current lifecycle state; unknown state must be reported as an error.
    ///
    /// # Errors
    /// Returns an error when the platform cannot determine its state.
    async fn state(&self) -> Result<ControllerHostState, PortError>;

    /// Returns the maximum platform-supported drain duration, if one applies.
    ///
    /// # Errors
    /// Returns an error when the limit cannot be determined.
    async fn drain_limit(&self) -> Result<Option<std::time::Duration>, PortError>;

    /// Renews an outstanding retirement lifecycle hook.
    ///
    /// # Errors
    /// Returns an error when the platform cannot renew the hook.
    async fn heartbeat(&self) -> Result<(), PortError>;

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

    /// Advances or observes the platform lifecycle associated with `milestone`. Preparation
    /// registers the host without completing its launch hook; serving completes that hook only
    /// after promotion. Platforms without managed registration may complete preparation at once.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be told.
    async fn reached(&self, milestone: HostMilestone) -> Result<(), PortError>;
}

/// The platform lifecycle state relevant to controller admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerHostState {
    /// The host may acquire leadership.
    Active,
    /// The host is retiring and must not acquire leadership.
    Retiring,
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
    /// Transport, history, authentication and dashboard are prepared. Register this instance
    /// in all of its target groups and wait for registration to complete while readiness
    /// remains unavailable. The launch hook remains pending until [`Self::Serving`].
    Prepared,
    /// The promoted controller passed its startup checks and serves clients; complete its launch hook.
    Serving,
    /// The platform has removed the host from routing in every target group.
    Unrouted,
    /// The controller handed off and is exiting: the host may be removed.
    HandedOff,
}
