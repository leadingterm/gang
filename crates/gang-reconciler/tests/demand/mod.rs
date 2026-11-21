//! Integration tests of `Demand` through the public API.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_config::{FleetPolicy, InstanceBounds};
use gang_reconciler::Demand;
use gang_testkit::{TestResult, fleet_policy};
use gang_types::{ActionResources, Bytes, NonZeroBytes};

fn policy() -> TestResult<FleetPolicy> {
    Ok(FleetPolicy {
        instances: InstanceBounds::up_to(gang_types::nonzero_u32(100)),
        vcpus_per_instance: gang_types::nonzero_u32(15),
        memory_per_instance: NonZeroBytes::gib(29),
        idle_timeout: Duration::from_mins(10),
        max_drain_wait: Duration::from_mins(25),
        boot_grace: Duration::from_mins(10),
        ..fleet_policy("workers")?
    })
}

fn resources(cpu_millicores: u64, memory: Bytes) -> ActionResources {
    ActionResources {
        cpu_millicores,
        memory,
        disk: Bytes::gib(1),
    }
}

#[test]
fn accessors_report_queued_resources_and_running_actions() {
    let mut demand = Demand::default();
    demand.add_queued(ActionResources {
        cpu_millicores: 2000,
        memory: Bytes::gib(1),
        disk: Bytes::new(0),
    });
    demand.add_queued(ActionResources::default());
    demand.add_running(3);

    assert_eq!(demand.actions(), 5);
    assert_eq!(demand.vcpus(), 2);
    assert_eq!(demand.memory(), 1 << 30);
}

#[test]
fn zero_resource_actions_share_one_instance()
-> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..17 {
        demand.add_queued(ActionResources::default());
    }

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn zero_resource_actions_need_one_instance() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..16 {
        demand.add_queued(ActionResources::default());
    }

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn running_action_counts_do_not_imply_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_running(17);

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn vcpus_dominate_other_resources() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..3 {
        demand.add_queued(resources(10000, Bytes::gib(1)));
    }

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 2);
    Ok(())
}

#[test]
fn memory_dominates_vcpus()
-> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_queued(resources(1000, Bytes::gib(30)));

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 2);
    Ok(())
}

#[test]
fn memory_only_actions_each_requiring_one_instance_need_sixteen_instances()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = policy()?;
    let mut demand = Demand::default();
    for _ in 0..16 {
        demand.add_queued(ActionResources {
            cpu_millicores: 0,
            memory: policy.memory_per_instance.get(),
            disk: Bytes::new(0),
        });
    }

    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 16);
    Ok(())
}

#[test]
fn actions_without_resource_requests_add_no_vcpus() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        vcpus_per_instance: NonZeroU32::MIN,
        ..policy()?
    };
    let mut demand = Demand::default();
    for _ in 0..3 {
        demand.add_queued(ActionResources::default());
    }

    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn instances_ignore_the_minimum() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(3, NonZeroU32::MIN.saturating_add(99))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    demand.add_queued(ActionResources::default());

    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn max_instances_cap_beats_huge_demand() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(4))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    demand.add_queued(resources(100_000, Bytes::new(0)));

    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 5);
    Ok(())
}

#[test]
fn running_actions_saturate_without_overflow() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_running(u32::MAX);
    demand.add_queued(ActionResources::default());

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 1);
    Ok(())
}

#[test]
fn fits_at_exactly_max_instances() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(1))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    for _ in 0..32 {
        demand.add_queued(resources(937, Bytes::new(0)));
    }

    assert!(demand.fits(&policy, NonZeroU32::MIN));
    Ok(())
}

#[test]
fn fits_fails_one_action_over_max_instances() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(1))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    demand.add_queued(resources(30_001, Bytes::new(0)));

    assert!(!demand.fits(&policy, NonZeroU32::MIN));
    Ok(())
}

#[test]
fn fits_ignores_min_instances() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(2, NonZeroU32::MIN.saturating_add(1))?,
        ..policy()?
    };

    assert!(Demand::default().fits(&policy, NonZeroU32::MIN));
    Ok(())
}

#[test]
fn fits_counts_measured_vcpus() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN)?,
        ..policy()?
    };
    let mut at_limit = Demand::default();
    at_limit.add_queued(resources(15000, Bytes::new(0)));
    let mut over_limit = Demand::default();
    over_limit.add_queued(resources(16000, Bytes::new(0)));

    assert!(at_limit.fits(&policy, NonZeroU32::MIN));
    assert!(!over_limit.fits(&policy, NonZeroU32::MIN));
    Ok(())
}

#[test]
fn disk_alone_drives_the_instance_count() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..3 {
        demand.add_queued(ActionResources {
            cpu_millicores: 1000,
            memory: Bytes::gib(1),
            disk: Bytes::gib(60),
        });
    }

    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 2);
    Ok(())
}

#[test]
fn fractional_cpu_and_oversubscription_are_summed_before_rounding() -> TestResult {
    let mut demand = Demand::default();
    for _ in 0..61 {
        demand.add_queued(ActionResources {
            cpu_millicores: 500,
            ..ActionResources::default()
        });
    }
    let factor = NonZeroU32::new(2).ok_or("nonzero factor")?;
    assert_eq!(demand.instances(&policy()?, factor), 2);
    Ok(())
}

#[test]
fn complementary_reservations_sum_per_dimension() -> TestResult {
    let mut demand = Demand::default();
    demand.add_queued(ActionResources {
        cpu_millicores: 14_000,
        memory: Bytes::gib(20),
        disk: Bytes::new(0),
    });
    demand.add_queued(ActionResources {
        cpu_millicores: 2_000,
        memory: Bytes::gib(10),
        disk: Bytes::new(0),
    });
    assert_eq!(demand.instances(&policy()?, NonZeroU32::MIN), 2);
    Ok(())
}

#[test]
fn instance_demand_tracks_live_maximum_changes() -> TestResult {
    let mut policy = policy()?;
    let mut demand = Demand::default();
    demand.add_queued(resources(150_000, Bytes::new(0)));
    policy.instances = InstanceBounds::up_to(NonZeroU32::new(4).ok_or("nonzero max")?);
    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 4);
    policy.instances = InstanceBounds::up_to(NonZeroU32::new(8).ok_or("nonzero max")?);
    assert_eq!(demand.instances(&policy, NonZeroU32::MIN), 8);
    Ok(())
}
