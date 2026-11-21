//! Demand for one fleet, accumulated from queued and running actions.
//!
//! Pure demand math: how many workers one fleet's queued and running actions need.
//!
//! ```
//! use std::time::Duration;
//!
//! use gang_config::{FleetPolicy, InstanceBounds};
//! use gang_reconciler::Demand;
//! use gang_types::{ActionResources, FleetName, Isa, NonZeroBytes, nonzero_u32};
//!
//! let policy = FleetPolicy {
//!     name: FleetName::try_new("workers")?,
//!     isa: Isa::Aarch64,
//!     action_environment: "host:test".parse()?,
//!     instances: InstanceBounds::try_new(3, nonzero_u32(100))?,
//!     vcpus_per_instance: nonzero_u32(15),
//!     memory_per_instance: NonZeroBytes::gib(29),
//!     disk_per_instance: NonZeroBytes::gib(100),
//!     idle_timeout: Duration::from_mins(10),
//!     max_drain_wait: Duration::from_mins(25),
//!     boot_grace: Duration::from_mins(10),
//!     actions: Default::default(),
//! };
//!
//! let mut demand = Demand::default();
//! for _ in 0..17 {
//!     demand.add_queued(ActionResources::default());
//! }
//! assert_eq!(demand.instances(&policy, nonzero_u32(2)), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::num::{NonZeroU32, NonZeroU64};

use gang_config::FleetPolicy;
use gang_types::{ActionResources, Bytes, FleetSet, SaturatingInto, property::MILLICORES_PER_CORE};

/// Work waiting for or occupying one fleet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Demand {
    actions: u64,
    millicores: u64,
    memory: u64,
    disk: u64,
}

impl Demand {
    /// Adds a queued action and the resources it requests.
    pub fn add_queued(&mut self, resources: ActionResources) {
        self.actions = self.actions.saturating_add(1);
        self.millicores = self.millicores.saturating_add(resources.cpu_millicores);
        self.memory = self.memory.saturating_add(resources.memory.get());
        self.disk = self.disk.saturating_add(resources.disk.get());
    }

    /// Adds actions running on workers that keep serving.
    pub fn add_running(&mut self, actions: u32) {
        self.actions = self.actions.saturating_add(u64::from(actions));
    }

    /// Accounts for a running action's reservations after its worker supplied the count.
    pub(crate) fn add_running_resources(&mut self, resources: ActionResources) {
        self.millicores = self.millicores.saturating_add(resources.cpu_millicores);
        self.memory = self.memory.saturating_add(resources.memory.get());
        self.disk = self.disk.saturating_add(resources.disk.get());
    }

    /// Returns the actions counted in this demand: every queued action and every running action.
    #[must_use]
    pub const fn actions(&self) -> u64 {
        self.actions
    }

    /// Returns the total queued CPU rounded up to whole vCPUs.
    #[must_use]
    pub fn vcpus(&self) -> u64 {
        self.millicores.div_ceil(MILLICORES_PER_CORE)
    }

    /// Returns the memory in bytes of the queued actions whose resources were measured.
    #[must_use]
    pub const fn memory(&self) -> u64 {
        self.memory
    }

    pub(crate) fn resources(&self) -> ActionResources {
        ActionResources {
            cpu_millicores: self.millicores,
            memory: Bytes::new(self.memory),
            disk: Bytes::new(self.disk),
        }
    }

    /// Returns the instances needed to serve this demand under `policy`.
    ///
    /// The result is saturated to `u32` and capped at the fleet's maximum. The fleet's minimum
    /// plays no part: the pass raises the need to the minimum in force.
    #[must_use]
    pub fn instances(&self, policy: &FleetPolicy, cpu_oversubscription: NonZeroU32) -> u32 {
        let need: u32 = self
            .uncapped_need(policy, cpu_oversubscription)
            .saturating_into();
        need.min(policy.instances.max().get())
    }

    /// Returns whether this demand fits within the fleet's maximum instance count.
    ///
    /// The minimum plays no part: only the uncapped need is compared.
    #[must_use]
    pub fn fits(&self, policy: &FleetPolicy, cpu_oversubscription: NonZeroU32) -> bool {
        self.uncapped_need(policy, cpu_oversubscription) <= u64::from(policy.instances.max().get())
    }

    fn uncapped_need(&self, policy: &FleetPolicy, cpu_oversubscription: NonZeroU32) -> u64 {
        let cpu_capacity = u64::from(policy.vcpus_per_instance.get())
            .saturating_mul(MILLICORES_PER_CORE)
            .saturating_mul(u64::from(cpu_oversubscription.get()));
        let cpu_need = self.millicores.div_ceil(cpu_capacity);
        let by_memory = div_ceil(self.memory, policy.memory_per_instance.as_nonzero_u64());
        let by_disk = div_ceil(self.disk, policy.disk_per_instance.as_nonzero_u64());
        cpu_need
            .max(by_memory)
            .max(by_disk)
            .max(u64::from(self.actions > 0))
    }
}

fn div_ceil(total: u64, per: NonZeroU64) -> u64 {
    total / per + u64::from(total % per != 0)
}

/// Chooses the fleet that a queued action is charged to.
///
/// Walks `candidates` in ascending index order and returns the first fleet whose demand still
/// fits its available instance limit after adding the action. When none fits, returns the last
/// candidate that names a fleet (its capped demand then queues). Returns `None` when no
/// candidate names a fleet.
///
/// `fleets` maps a fleet index to its policy, current demand and available instance limit.
/// The limit is capped by the policy maximum. It returns `None` for an index that names no
/// fleet, and the walk stops at the first such index. `place` charges nothing: the caller adds
/// the action to the demand of the chosen fleet.
pub(crate) fn place<'a>(
    candidates: FleetSet,
    resources: ActionResources,
    cpu_oversubscription: NonZeroU32,
    fleets: impl Fn(usize) -> Option<(&'a FleetPolicy, Demand, u32)>,
) -> Option<usize> {
    let mut last = None;
    for index in candidates {
        let Some((policy, demand, limit)) = fleets(index) else {
            break;
        };
        let mut trial = demand;
        trial.add_queued(resources);
        if trial.uncapped_need(policy, cpu_oversubscription)
            <= u64::from(limit.min(policy.instances.max().get()))
        {
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
    use gang_testkit::{TestResult, fleet_policy};
    use gang_types::{Bytes, NonZeroBytes, nonzero_u32};

    use super::*;

    #[test]
    fn running_reservations_raise_resource_ceiling_without_double_counting_actions()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = policy()?;
        let mut demand = Demand::default();
        demand.add_running(1);
        demand.add_running_resources(ActionResources {
            cpu_millicores: 30_000,
            ..ActionResources::default()
        });
        assert_eq!(demand.actions(), 1);
        assert_eq!(demand.instances(&policy, nonzero_u32(1)), 2);
        Ok(())
    }

    #[test]
    fn division_rounds_up_at_boundaries() {
        let three = NonZeroU64::from(nonzero_u32(3));
        assert_eq!(div_ceil(0, three), 0);
        assert_eq!(div_ceil(6, three), 2);
        assert_eq!(div_ceil(7, three), 3);
        assert_eq!(div_ceil(u64::MAX, NonZeroU64::MIN), u64::MAX);
        assert_eq!(div_ceil(u64::MAX, three), u64::MAX / 3);
    }

    fn policy() -> TestResult<FleetPolicy> {
        Ok(FleetPolicy {
            instances: InstanceBounds::up_to(nonzero_u32(100)),
            vcpus_per_instance: nonzero_u32(15),
            memory_per_instance: NonZeroBytes::gib(29),
            idle_timeout: Duration::from_mins(10),
            max_drain_wait: Duration::from_mins(25),
            boot_grace: Duration::from_mins(10),
            ..fleet_policy("workers")?
        })
    }

    #[test]
    fn resource_bounds_and_zero_reservation_reachability() -> TestResult<()> {
        let policy = FleetPolicy {
            instances: InstanceBounds::up_to(nonzero_u32(2)),
            ..policy()?
        };
        let mut zero = Demand::default();
        for _ in 0..1000 {
            zero.add_queued(ActionResources::default());
        }
        assert_eq!(zero.instances(&policy, nonzero_u32(1)), 1);
        assert!(zero.fits(&policy, nonzero_u32(1)));
        let mut cpu = Demand::default();
        cpu.add_queued(ActionResources {
            cpu_millicores: 30_001,
            ..ActionResources::default()
        });
        assert_eq!(cpu.instances(&policy, nonzero_u32(1)), 2);
        assert!(!cpu.fits(&policy, nonzero_u32(1)));
        assert!(cpu.fits(&policy, nonzero_u32(2)));
        let mut memory = Demand::default();
        memory.add_queued(ActionResources {
            memory: Bytes::new(policy.memory_per_instance.get().get() * 2 + 1),
            ..ActionResources::default()
        });
        assert!(!memory.fits(&policy, nonzero_u32(2)));
        assert_eq!(memory.instances(&policy, nonzero_u32(2)), 2);
        let mut disk = Demand::default();
        disk.add_queued(ActionResources {
            disk: Bytes::new(policy.disk_per_instance.get().get() * 2 + 1),
            ..ActionResources::default()
        });
        assert!(!disk.fits(&policy, nonzero_u32(2)));
        assert_eq!(disk.instances(&policy, nonzero_u32(2)), 2);
        Ok(())
    }

    fn resources(cpu_millicores: u64, memory: Bytes) -> ActionResources {
        ActionResources {
            cpu_millicores,
            memory,
            disk: Bytes::gib(1),
        }
    }

    fn one_action_per_instance(
        max_instances: u32,
    ) -> Result<FleetPolicy, Box<dyn std::error::Error>> {
        Ok(FleetPolicy {
            instances: InstanceBounds::up_to(NonZeroU32::new(max_instances).ok_or("zero maximum")?),
            ..policy()?
        })
    }

    /// Charges one queued action through `place`, as the reconciler does.
    fn attribute(
        demands: &mut [Demand],
        policies: &[FleetPolicy],
        candidates: FleetSet,
        resources: ActionResources,
    ) -> Option<usize> {
        let index = place(candidates, resources, nonzero_u32(1), |index| {
            policies
                .get(index)
                .zip(demands.get(index).copied())
                .map(|(policy, demand)| (policy, demand, policy.instances.max().get()))
        })?;
        demands[index].add_queued(resources);
        Some(index)
    }

    #[test]
    fn fractional_cpu_is_summed_before_rounding() -> Result<(), Box<dyn std::error::Error>> {
        let policy = FleetPolicy {
            vcpus_per_instance: nonzero_u32(8),
            ..policy()?
        };
        let mut demand = Demand::default();
        for _ in 0..16 {
            demand.add_queued(ActionResources {
                cpu_millicores: 250,
                ..ActionResources::default()
            });
        }
        assert_eq!(
            demand.instances(&policy, nonzero_u32(1)),
            1,
            "fractional CPU must be summed before rounding"
        );
        for _ in 0..32 {
            demand.add_queued(ActionResources {
                cpu_millicores: 250,
                ..ActionResources::default()
            });
        }
        assert_eq!(demand.instances(&policy, nonzero_u32(1)), 2);
        assert_eq!(demand.instances(&policy, nonzero_u32(2)), 1);
        Ok(())
    }

    #[test]
    fn queued_cpu_has_one_owner() -> Result<(), Box<dyn std::error::Error>> {
        let policy = FleetPolicy {
            vcpus_per_instance: nonzero_u32(2),
            ..policy()?
        };
        let mut four_cores = Demand::default();
        four_cores.add_queued(ActionResources {
            cpu_millicores: 4000,
            ..ActionResources::default()
        });
        assert_eq!(
            four_cores.instances(&policy, nonzero_u32(1)),
            2,
            "4000m requires two 2-vCPU instances"
        );
        let mut fractional = Demand::default();
        fractional.add_queued(ActionResources {
            cpu_millicores: 250,
            ..ActionResources::default()
        });
        assert_eq!(
            fractional.instances(&policy, nonzero_u32(1)),
            1,
            "250m requires one instance"
        );
        Ok(())
    }

    #[test]
    fn single_candidate_is_charged() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::try_from_indices([1])?;

        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                ActionResources::default()
            ),
            Some(1)
        );
        assert_eq!(demands[0], Demand::default());
        assert_eq!(demands[1].instances(&policies[1], nonzero_u32(1)), 1);
        Ok(())
    }

    #[test]
    fn action_spills_to_next_candidate_once_first_no_longer_fits()
    -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::prefix(2)?;

        let charged: Vec<_> = (0..3)
            .map(|_| {
                attribute(
                    &mut demands,
                    &policies,
                    candidates,
                    resources(15_000, Bytes::new(0)),
                )
            })
            .collect();

        assert_eq!(charged, [Some(0), Some(0), Some(1)]);
        assert_eq!(demands[0].instances(&policies[0], nonzero_u32(1)), 2);
        assert_eq!(demands[1].instances(&policies[1], nonzero_u32(1)), 1);
        Ok(())
    }

    #[test]
    fn placement_trials_use_cpu_oversubscription() -> TestResult<()> {
        let policies = [one_action_per_instance(1)?, one_action_per_instance(2)?];
        let mut first = Demand::default();
        first.add_queued(resources(15_000, Bytes::new(0)));
        let demands = [first, Demand::default()];
        let candidates = FleetSet::prefix(2)?;
        let trial = resources(15_000, Bytes::new(0));
        assert_eq!(
            place(candidates, trial, nonzero_u32(1), |i| {
                policies
                    .get(i)
                    .zip(demands.get(i).copied())
                    .map(|(policy, demand)| (policy, demand, policy.instances.max().get()))
            }),
            Some(1)
        );
        assert_eq!(
            place(candidates, trial, nonzero_u32(2), |i| {
                policies
                    .get(i)
                    .zip(demands.get(i).copied())
                    .map(|(policy, demand)| (policy, demand, policy.instances.max().get()))
            }),
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn placement_limits_retain_physical_credit_and_reject_failed_requests() -> TestResult<()> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(2)?];
        let candidates = FleetSet::prefix(2)?;
        for available in [0, 1] {
            let limits = [available, 2];
            let chosen = place(candidates, ActionResources::default(), nonzero_u32(1), |i| {
                Some((&policies[i], Demand::default(), limits[i]))
            });
            assert_eq!(chosen, Some(usize::from(available == 0)));
        }
        let mut occupied = Demand::default();
        occupied.add_queued(resources(15_000, Bytes::new(0)));
        assert_eq!(
            place(candidates, resources(1, Bytes::new(0)), nonzero_u32(1), |i| {
                let demand = if i == 0 { occupied } else { Demand::default() };
                Some((&policies[i], demand, 1))
            }),
            Some(1)
        );
        assert_eq!(
            place(
                FleetSet::prefix(1)?,
                ActionResources::default(),
                nonzero_u32(1),
                |_| Some((&policies[0], Demand::default(), 0)),
            ),
            Some(0),
            "a failed sole candidate retains demand until it can launch again"
        );
        Ok(())
    }

    #[test]
    fn last_candidate_takes_overflow_capped_at_max() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let candidates = FleetSet::prefix(2)?;
        for _ in 0..12 {
            attribute(
                &mut demands,
                &policies,
                candidates,
                resources(15_000, Bytes::new(0)),
            );
        }

        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                resources(15_000, Bytes::new(0))
            ),
            Some(1)
        );
        assert!(!demands[1].fits(&policies[1], nonzero_u32(1)));
        assert_eq!(demands[1].instances(&policies[1], nonzero_u32(1)), 10);
        assert_eq!(demands[0].instances(&policies[0], nonzero_u32(1)), 2);
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
                ActionResources::default()
            ),
            None
        );
        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                FleetSet::EMPTY,
                resources(4000, Bytes::gib(1))
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
            attribute(
                &mut demands,
                &policies,
                candidates,
                ActionResources::default()
            ),
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
                resources(20000, Bytes::new(0))
            ),
            Some(0)
        );
        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                resources(16000, Bytes::new(0))
            ),
            Some(1)
        );
        assert_eq!(demands[0].instances(&policies[0], nonzero_u32(1)), 2);
        assert_eq!(demands[1].instances(&policies[1], nonzero_u32(1)), 2);
        Ok(())
    }

    #[test]
    fn only_out_of_range_candidates_charge_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let policies = [one_action_per_instance(2)?, one_action_per_instance(10)?];
        let mut demands = vec![Demand::default(); 2];
        let before = demands.clone();
        let candidates = FleetSet::try_from_indices([7])?;

        assert_eq!(
            attribute(
                &mut demands,
                &policies,
                candidates,
                ActionResources::default()
            ),
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
                attribute(&mut demands, &policies, mixed, ActionResources::default()),
                Some(0)
            );
            assert_eq!(
                attribute(
                    &mut reference,
                    &policies,
                    only_first,
                    ActionResources::default()
                ),
                Some(0)
            );
        }

        assert_eq!(demands, reference);
        assert_eq!(demands[1], Demand::default());
        Ok(())
    }
}
