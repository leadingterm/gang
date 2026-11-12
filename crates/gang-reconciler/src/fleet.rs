//! One fleet as a reconcile pass sees it: its configuration, its supply, and its outlook.

use gang_config::FleetPolicy;
use gang_spi::FleetSupply;

use crate::Demand;

/// The connected workers and in-service unconnected instances of a fleet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkerCounts {
    /// Connected workers that are busy.
    pub busy: u32,
    /// Connected workers that are idle.
    pub idle: u32,
    /// Connected workers that are draining.
    pub draining: u32,
    /// In-service instances without a worker.
    pub unconnected: u32,
    /// Actions running on all workers, including draining workers.
    pub running: u32,
}

/// What one fleet's queue and demand came to in the latest pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FleetOutlook {
    /// The instances the demand needs under the fleet's policy.
    pub needed: u32,
    /// The demand charged to the fleet: its attributed queued actions and the running actions of
    /// its non-draining workers.
    pub demand: Demand,
    /// The queued actions attributed to the fleet. Exclusive: each placeable action counts for
    /// one fleet only.
    pub queued: u32,
    /// The queued actions that name the fleet as a candidate. Non-exclusive: one action can count
    /// for several fleets.
    pub candidates: u32,
    /// The fleet's workers, unconnected instances, and running actions.
    pub workers: WorkerCounts,
}

/// One fleet in a pass: its owned policy, the provisioner supply the caller refreshes before the
/// pass, and the outlook and boot-grace verdicts the reconciler writes.
#[derive(Debug, Clone)]
pub struct FleetPass {
    /// The fleet's policy.
    pub policy: FleetPolicy,
    /// The provisioner's supply of the fleet, refreshed by the caller before the pass.
    pub supply: FleetSupply,
    /// The outlook of the fleet, written by the pass.
    pub outlook: FleetOutlook,
    /// For each instance of `supply`, in order, whether it has been in service without a worker
    /// for at least `boot_grace` by both clocks; written by the pass.
    pub past_boot_grace: Vec<bool>,
}

impl FleetPass {
    /// A fleet for a pass, with the default supply and the default outlook.
    #[must_use]
    pub fn new(policy: FleetPolicy) -> Self {
        Self {
            policy,
            supply: FleetSupply::default(),
            outlook: FleetOutlook::default(),
            past_boot_grace: Vec::new(),
        }
    }
}
