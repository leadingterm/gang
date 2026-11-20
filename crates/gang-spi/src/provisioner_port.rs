//! Provisioner: how many workers exist and how to change that.

use std::fmt;
use std::net::SocketAddr;

use futures::future::BoxFuture;
use gang_clock::Timestamp;
use gang_types::{Fleet, InstanceId, InstanceLifecycle, Url, WorkerLifetime};

use crate::{Held, InfrastructureEvent, PortError, port};

/// The worker fleets, on the compute backend that runs the instances.
///
/// A method that changes anything takes a [`Held`], the fence's permission: reads take none, so
/// no read can change a fleet, and no change starts once the leader lease could have passed to
/// another controller.
#[port]
pub trait ProvisionerPort: Send + Sync {
    /// The typed `[fleets.options]` table of the provisioner. `compose!` requires the types of
    /// the composed provisioners to implement [`FleetOptions`](crate::FleetOptions).
    type FleetOptions: std::fmt::Debug + Send + Sync + 'static;

    /// Clears the configured fleet set before registering all fleets for a refresh.
    fn reset_configured_fleets(&self) {}

    /// Registers a configured fleet before any fleet is converged.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the fleet cannot be configured.
    fn configure_fleet(&self, fleet: Fleet<'_, Self::FleetOptions>) -> Result<(), PortError> {
        let _ = fleet;
        Ok(())
    }

    /// Overwrites `into` with the fleet's current size, instances, and observed early exits. It
    /// only reads: it never changes the fleet or the backend. Its buffers are reused so callers
    /// can pass the same value on every pass. On `Err`, `into` is unspecified; callers skip the
    /// pass.
    ///
    /// Unacknowledged exits are repeated across calls and restarts until acknowledged. A
    /// provisioner must persist an exit before retiring the corresponding instance.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot be queried.
    async fn supply(
        &self,
        fleet: Fleet<'_, Self::FleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError>;

    /// Acknowledges an exit after its audit event has been durably recorded. Unknown instance
    /// IDs are harmless, so repeated acknowledgements succeed. Delivery is at least once: a
    /// crash or an ambiguous audit append or acknowledgement failure may repeat an audit event.
    ///
    /// The default is only for provisioners that produce no exits.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot acknowledge the exit.
    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Self::FleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        let _ = (fleet, instance);
        held.check()?;
        Ok(())
    }

    /// Acknowledges an infrastructure observation after it has been durably recorded.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the permission has expired or the observation cannot be acknowledged.
    async fn acknowledge_event(
        &self,
        held: &Held,
        fleet: Fleet<'_, Self::FleetOptions>,
        event_id: &str,
    ) -> Result<(), PortError> {
        let _ = (fleet, event_id);
        held.check()?;
        Ok(())
    }

    /// Does what the backend would do by itself to keep the fleet at its desired capacity:
    /// retires instances that died, replaces them, and ends terminations past their grace. The
    /// leader calls it before every [`supply`](Self::supply). The default changes nothing, for a
    /// backend that converges by itself, such as an auto scaling group, and refuses an expired
    /// permission like every other change.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot converge the fleet or `held` has expired.
    async fn converge(
        &self,
        held: &Held,
        fleet: Fleet<'_, Self::FleetOptions>,
    ) -> Result<(), PortError> {
        let _ = fleet;
        held.check()?;
        Ok(())
    }

    /// Sets the instances the fleet asks for, each [`Fleet::instance`] in size. A higher
    /// `desired` raises the request; a lower one withdraws requested instances that do not exist
    /// yet, never below the instances that exist (pending or in service, not terminating). It
    /// never removes an instance: scale-in drains a worker and then calls
    /// [`terminate`](Self::terminate). A call that keeps `desired` unchanged withdraws abandoned
    /// requests; any other call changes only the requested count.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner rejects the request.
    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Self::FleetOptions>,
        desired: u32,
    ) -> Result<(), PortError>;

    /// Removes one instance from the fleet and lowers the desired capacity with it; a
    /// provisioner with a standby pool may keep the instance stopped for reuse. `Ok` when the
    /// instance is already gone, so a repeated call is harmless. A
    /// [`Pending`](InstanceLifecycle::Pending) instance is cancelled.
    /// Terminating an instance that is not a member of the fleet (it never existed, is already
    /// gone, or belongs to another fleet) is Ok and changes nothing.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot terminate the instance.
    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Self::FleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), PortError>;

    /// Whether the workers it starts serve many actions or exactly one. Does no I/O. A
    /// provisioner whose workers could reuse volumes returns
    /// [`LongLived`](WorkerLifetime::LongLived).
    fn worker_lifetime(&self) -> WorkerLifetime;

    /// Attaches the provisioner to the server that is about to serve: it learns where clients
    /// and workers reach the server, and may return a sidecar task the server runs while it
    /// serves. The default has nothing to attach. It takes no [`Held`]: attaching changes no
    /// fleet, and a leader whose lease lapsed keeps serving its workers (fail static), so it
    /// may attach again after its sidecar stopped.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the provisioner cannot attach.
    async fn attach(&self, endpoints: &ServerEndpoints) -> Result<Option<Sidecar>, PortError> {
        let _ = endpoints;
        Ok(None)
    }
}

/// A fleet as the provisioner reports it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FleetSupply {
    /// The instances that exist and have not ended, pending ones included. It falls by itself
    /// when an instance exits and is not replaced (a finished pod or job); an autoscaling
    /// group keeps its stored value.
    pub desired: u32,
    /// The actual request, when different from the planner's capacity; `None` means `desired`.
    pub requested: Option<u32>,
    /// Instances still requested from the backend but excluded from `desired` because the
    /// provisioner gave up launching them (for example, a down Spot group's unmet request).
    /// A `set_desired_capacity` call at the current `desired` withdraws them; zero if requests
    /// are never abandoned.
    pub abandoned: u32,
    /// The instances that exist, in any state.
    pub instances: Vec<FleetInstance>,
    /// The instances parked in a warm pool; `0` without one.
    pub warm: u32,
    /// The instances whose worker failed soon after launch and whose exits have not yet been
    /// acknowledged; empty for a provisioner that cannot tell.
    pub exits: Vec<InstanceExit>,
    /// Provider infrastructure observations awaiting acknowledgement or still in history.
    pub events: Vec<InfrastructureEvent>,
    /// Why the provisioner cannot launch the instances `desired` asks for beyond those that
    /// exist: the newest launch attempt for the fleet failed and no launch is in flight.
    /// `None` while the request is met, while a launch is in flight, or when the provisioner
    /// cannot tell.
    pub launch_failure: Option<LaunchFailure>,
}

/// A failed launch attempt for a fleet whose request remains unmet and has no launch in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchFailure {
    /// The provider's own message, verbatim.
    pub reason: String,
}

/// An instance whose worker failed soon after its launch, as a provisioner observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceExit {
    /// The instance whose worker failed.
    pub instance: InstanceId,
    /// The worker's own failure record.
    pub failure: String,
}

/// One instance of a fleet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetInstance {
    /// The provisioner's id; the worker on it is named after it.
    pub id: InstanceId,
    /// Where the instance is in its life.
    pub lifecycle: InstanceLifecycle,
    /// When the instance began running (left [`Pending`](InstanceLifecycle::Pending)); for a
    /// pending instance, the request time, which the reconciler ignores. Separates a booting
    /// instance from an orphan.
    pub launched_at: Timestamp,
    /// Release this instance before others when the fleet has surplus capacity; `false` for
    /// every provisioner that has no cheaper-first preference.
    pub release_first: bool,
}

/// The addresses of the server that a provisioner attaches to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEndpoints {
    /// The URL clients reach the server's public listener on.
    pub public: Url,
    /// The URL workers reach the server's worker listener on.
    pub worker: Url,
}

/// A task the provisioner needs the server to run beside it, for example the mock
/// instance-metadata service of a local provisioner. `local_addr` is where it listens. Its
/// future never completes normally: when it completes, the server treats that as a failure
/// and the output is the error.
pub struct Sidecar {
    local_addr: SocketAddr,
    serve: BoxFuture<'static, PortError>,
}

impl Sidecar {
    /// Wraps `serve`, the task that listens on `local_addr`.
    #[must_use]
    pub fn new(
        local_addr: SocketAddr,
        serve: impl Future<Output = PortError> + Send + 'static,
    ) -> Self {
        Self {
            local_addr,
            serve: Box::pin(serve),
        }
    }

    /// The address the sidecar listens on.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Runs the task; the future resolves to the error it ended with.
    #[must_use]
    pub fn serve(self) -> BoxFuture<'static, PortError> {
        self.serve
    }
}

impl fmt::Debug for Sidecar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sidecar")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_reports_the_address_it_was_given() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9169));
        let sidecar = Sidecar::new(addr, std::future::pending::<PortError>());
        assert_eq!(sidecar.local_addr(), addr);
    }

    #[tokio::test]
    async fn serve_resolves_to_the_error_of_the_future() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9169));
        let sidecar = Sidecar::new(addr, async {
            PortError::config("metadata listener stopped")
        });
        let error = sidecar.serve().await;
        assert!(matches!(
            &error,
            PortError::Config(cause) if cause.to_string() == "metadata listener stopped"
        ));
    }
}
