//! Demand for one fleet, accumulated from queued and running actions.
//!
//! Pure demand math: how many workers one fleet's queued and running actions need.
//!
//! ```
//! use std::time::Duration;
//!
//! use gang_config::{FleetPolicy, InstanceBounds};
//! use gang_reconciler::Demand;
//! use gang_types::{FleetName, Isa, NonZeroBytes, Resources, nonzero_u32};
//!
//! let policy = FleetPolicy {
//!     name: FleetName::try_new("workers")?,
//!     isa: Isa::Aarch64,
//!     instances: InstanceBounds::up_to(nonzero_u32(100)),
//!     vcpus_per_instance: nonzero_u32(15),
//!     memory_per_instance: NonZeroBytes::gib(29),
//!     disk_per_instance: NonZeroBytes::gib(100),
//!     target_actions_per_instance: nonzero_u32(16),
//!     idle_timeout: Duration::from_mins(10),
//!     max_drain_wait: Duration::from_mins(25),
//!     boot_grace: Duration::from_mins(10),
//! };
//!
//! let mut demand = Demand::default();
//! for _ in 0..17 {
//!     demand.add_queued(Resources::default());
//! }
//! assert_eq!(demand.instances(&policy), 2);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::num::NonZeroU64;

use gang_config::FleetPolicy;
use gang_types::{FleetSet, Resources, SaturatingInto};

/// Work waiting for or occupying one fleet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Demand {
    actions: u64,
    vcpus: u64,
    memory: u64,
    disk: u64,
}

impl Demand {
    /// Adds a queued action: one action slot and the resources it requests.
    pub fn add_queued(&mut self, resources: Resources) {
        self.actions = self.actions.saturating_add(1);
        self.vcpus = self.vcpus.saturating_add(u64::from(resources.vcpus));
        self.memory = self.memory.saturating_add(resources.memory.get());
        self.disk = self.disk.saturating_add(resources.disk.get());
    }

    /// Adds actions running on workers that keep serving.
    pub fn add_running(&mut self, actions: u32) {
        self.actions = self.actions.saturating_add(u64::from(actions));
    }

    /// Returns the actions counted in this demand: every queued action and every running action.
    #[must_use]
    pub const fn actions(&self) -> u64 {
        self.actions
    }

    /// Returns the vCPUs of the queued actions whose resources were measured.
    #[must_use]
    pub const fn vcpus(&self) -> u64 {
        self.vcpus
    }

    /// Returns the memory in bytes of the queued actions whose resources were measured.
    #[must_use]
    pub const fn memory(&self) -> u64 {
        self.memory
    }

    /// Returns the instances needed to serve this demand under `policy`.
    ///
    /// The result is saturated to `u32`, raised to the fleet's minimum, then capped at its
    /// maximum.
    #[must_use]
    pub fn instances(&self, policy: &FleetPolicy) -> u32 {
        let need: u32 = self.uncapped_need(policy).saturating_into();
        need.max(policy.instances.min())
            .min(policy.instances.max().get())
    }

    /// Returns whether this demand fits within the fleet's maximum instance count.
    ///
    /// The minimum plays no part: only the uncapped need is compared.
    #[must_use]
    pub fn fits(&self, policy: &FleetPolicy) -> bool {
        self.uncapped_need(policy) <= u64::from(policy.instances.max().get())
    }

    /// Returns the uncapped instance need under `policy`.
    ///
    /// Action slots, vCPUs, memory and disk are each divided by their non-zero
    /// per-instance capacity, rounded up. The largest of those bounds wins.
    fn uncapped_need(&self, policy: &FleetPolicy) -> u64 {
        let by_actions = div_ceil(
            self.actions,
            NonZeroU64::from(policy.target_actions_per_instance),
        );
        let by_vcpus = div_ceil(self.vcpus, NonZeroU64::from(policy.vcpus_per_instance));
        let by_memory = div_ceil(self.memory, policy.memory_per_instance.as_nonzero_u64());
        let by_disk = div_ceil(self.disk, policy.disk_per_instance.as_nonzero_u64());
        by_actions.max(by_vcpus).max(by_memory).max(by_disk)
    }
}

fn div_ceil(total: u64, per: NonZeroU64) -> u64 {
    total / per + u64::from(total % per != 0)
}

/// Chooses the fleet that a queued action is charged to.
///
/// Walks `candidates` in ascending index order and returns the first fleet whose demand still
/// fits after adding the action. When none fits, returns the last candidate that names a fleet
/// (its capped demand then queues). Returns `None` when no candidate names a fleet.
///
/// `fleets` maps a fleet index to its policy and current demand. It returns `None` for an index
/// that names no fleet, and the walk stops at the first such index. `place` charges nothing: the
/// caller adds the action to the demand of the chosen fleet.
pub(crate) fn place<'a>(
    candidates: FleetSet,
    resources: Resources,
    fleets: impl Fn(usize) -> Option<(&'a FleetPolicy, Demand)>,
) -> Option<usize> {
    let mut last = None;
    for index in candidates {
        let Some((policy, demand)) = fleets(index) else {
            break;
        };
        let mut trial = demand;
        trial.add_queued(resources);
        if trial.fits(policy) {
            return Some(index);
        }
        last = Some(index);
    }
    last
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use gang_config::InstanceBounds;
    use gang_testkit::fleet_policy;
    use gang_types::{Bytes, NameError, NonZeroBytes, Resources, nonzero_u32};

    use super::*;

    #[test]
    fn division_rounds_up_at_boundaries() {
        let three = NonZeroU64::from(nonzero_u32(3));
        assert_eq!(div_ceil(0, three), 0);
        assert_eq!(div_ceil(6, three), 2);
        assert_eq!(div_ceil(7, three), 3);
        assert_eq!(div_ceil(u64::MAX, NonZeroU64::MIN), u64::MAX);
        assert_eq!(div_ceil(u64::MAX, three), u64::MAX / 3);
    }

    fn policy() -> Result<FleetPolicy, NameError> {
        Ok(FleetPolicy {
            instances: InstanceBounds::up_to(nonzero_u32(100)),
            vcpus_per_instance: nonzero_u32(15),
            memory_per_instance: NonZeroBytes::gib(29),
            target_actions_per_instance: nonzero_u32(16),
            idle_timeout: Duration::from_mins(10),
            max_drain_wait: Duration::from_mins(25),
            boot_grace: Duration::from_mins(10),
            ..fleet_policy("workers")?
        })
    }

    fn resources(vcpus: u32, memory: Bytes) -> Resources {
        Resources {
            vcpus,
            memory,
            disk: Bytes::gib(1),
        }
    }

    fn one_action_per_instance(
        max_instances: u32,
    ) -> Result<FleetPolicy, Box<dyn std::error::Error>> {
        Ok(FleetPolicy {
            instances: InstanceBounds::up_to(NonZeroU32::new(max_instances).ok_or("zero maximum")?),
            target_actions_per_instance: nonzero_u32(1),
            ..policy()?
        })
    }

    /// Charges one queued action through `place`, as the reconciler does.
    fn attribute(
        demands: &mut [Demand],
        policies: &[FleetPolicy],
        candidates: FleetSet,
        resources: Resources,
    ) -> Option<usize> {
        let index = place(candidates, resources, |index| {
            policies.get(index).zip(demands.get(index).copied())
        })?;
        demands[index].add_queued(resources);
        Some(index)
    }

    #[test]
    fn single_candidate_is_charged() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::try_from_indices([1])?;

        assert_eq!(
            attribute(&mut demands, &policies, candidates, Resources::default()),
            Some(1)
        );
        assert_eq!(demands[0], Demand::default());
        assert_eq!(demands[1].instances(&policies[1]), 1);
        Ok(())
    }

    #[test]
    fn action_spills_to_next_candidate_once_first_no_longer_fits()
    -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::prefix(2)?;

        let charged: Vec<_> = (0..3)
            .map(|_| attribute(&mut demands, &policies, candidates, Resources::default()))
            .collect();

        assert_eq!(charged, [Some(0), Some(0), Some(1)]);
        assert_eq!(demands[0].instances(&policies[0]), 2);
        assert_eq!(demands[1].instances(&policies[1]), 1);
        Ok(())
    }

    #[test]
    fn last_candidate_takes_overflow_capped_at_max() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::prefix(2)?;
        for _ in 0..12 {
            attribute(&mut demands, &policies, candidates, Resources::default());
        }

        assert_eq!(
            attribute(&mut demands, &policies, candidates, Resources::default()),
            Some(1)
        );
        assert!(!demands[1].fits(&policies[1]));
        assert_eq!(demands[1].instances(&policies[1]), 10);
        assert_eq!(demands[0].instances(&policies[0]), 2);
        Ok(())
    }

    #[test]
    fn no_candidates_charge_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?];
        let mut demands = vec![Demand::default(); 1];
        let before = demands.clone();

        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                FleetSet::EMPTY,
                Resources::default()
            ),
            None
        );
        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                FleetSet::EMPTY,
                resources(4, Bytes::gib(1))
            ),
            None
        );
        assert_eq!(demands, before);
        Ok(())
    }

    #[test]
    fn candidates_are_tried_in_ascending_index_regardless_of_insertion_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::try_from_indices([1, 0])?;

        assert_eq!(
            attribute(&mut demands, &policies, candidates, Resources::default()),
            Some(0)
        );
        assert_eq!(demands[1], Demand::default());
        Ok(())
    }

    #[test]
    fn measured_vcpus_count_toward_fit_when_attributing() -> Result<(), Box<dyn std::error::Error>>
    {
        let vcpu_bound = FleetPolicy {
            instances: InstanceBounds::up_to(NonZeroU32::MIN.saturating_add(1)),
            ..policy()?
        };
        let policies = [vcpu_bound, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::prefix(2)?;

        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                resources(20, Bytes::new(0))
            ),
            Some(0)
        );
        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                resources(16, Bytes::new(0))
            ),
            Some(1)
        );
        assert_eq!(demands[0].instances(&policies[0]), 2);
        assert_eq!(demands[1].instances(&policies[1]), 2);
        Ok(())
    }

    #[test]
    fn only_out_of_range_candidates_charge_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let before = demands.clone();
        let candidates = FleetSet::try_from_indices([7])?;

        assert_eq!(
            attribute(&mut demands, &policies, candidates, Resources::default()),
            None
        );
        assert_eq!(demands, before);
        Ok(())
    }

    #[test]
    fn out_of_range_candidate_is_ignored_when_mixed_with_fleets()
    -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mixed = FleetSet::try_from_indices([0, 7])?;
        let only_first = FleetSet::prefix(1)?;
        let mut demands = vec![Demand::default(); 2];
        let mut reference = demands.clone();

        for _ in 0..3 {
            assert_eq!(
                attribute(&mut demands, &policies, mixed, Resources::default()),
                Some(0)
            );
            assert_eq!(
                attribute(&mut reference, &policies, only_first, Resources::default()),
                Some(0)
            );
        }

        assert_eq!(demands, reference);
        assert_eq!(demands[1], Demand::default());
        Ok(())
    }
}
