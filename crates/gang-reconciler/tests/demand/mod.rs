//! Integration tests of `Demand` through the public API.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_config::{FleetPolicy, InstanceBounds};
use gang_reconciler::Demand;
use gang_testkit::fleet_policy;
use gang_types::{Bytes, NameError, NonZeroBytes, Resources};

fn policy() -> Result<FleetPolicy, NameError> {
    Ok(FleetPolicy {
        instances: InstanceBounds::up_to(gang_types::nonzero_u32(100)),
        vcpus_per_instance: gang_types::nonzero_u32(15),
        memory_per_instance: NonZeroBytes::gib(29),
        target_actions_per_instance: gang_types::nonzero_u32(16),
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

#[test]
fn accessors_report_queued_resources_and_running_actions() {
    let mut demand = Demand::default();
    demand.add_queued(Resources {
        vcpus: 2,
        memory: Bytes::gib(1),
        disk: Bytes::new(0),
    });
    demand.add_queued(Resources::default());
    demand.add_running(3);

    assert_eq!(demand.actions(), 5);
    assert_eq!(demand.vcpus(), 2);
    assert_eq!(demand.memory(), 1 << 30);
}

#[test]
fn seventeen_queued_actions_without_resource_requests_need_two_instances()
-> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..17 {
        demand.add_queued(Resources::default());
    }

    assert_eq!(demand.instances(&policy()?), 2);
    Ok(())
}

#[test]
fn actions_exactly_at_target_need_one_instance() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..16 {
        demand.add_queued(Resources::default());
    }

    assert_eq!(demand.instances(&policy()?), 1);
    Ok(())
}

#[test]
fn running_actions_count_like_queued_actions() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_running(17);

    assert_eq!(demand.instances(&policy()?), 2);
    Ok(())
}

#[test]
fn vcpus_dominate_when_they_exceed_action_slots() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..3 {
        demand.add_queued(resources(10, Bytes::gib(1)));
    }

    assert_eq!(demand.instances(&policy()?), 2);
    Ok(())
}

#[test]
fn memory_dominates_when_it_exceeds_action_slots_and_vcpus()
-> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_queued(resources(1, Bytes::gib(30)));

    assert_eq!(demand.instances(&policy()?), 2);
    Ok(())
}

#[test]
fn memory_only_actions_each_requiring_one_instance_need_sixteen_instances()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = policy()?;
    let mut demand = Demand::default();
    for _ in 0..16 {
        demand.add_queued(Resources {
            vcpus: 0,
            memory: policy.memory_per_instance.get(),
            disk: Bytes::new(0),
        });
    }

    assert_eq!(demand.instances(&policy), 16);
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
        demand.add_queued(Resources::default());
    }

    assert_eq!(demand.instances(&policy), 1);
    Ok(())
}

#[test]
fn min_instances_floor_beats_small_demand() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(3, NonZeroU32::MIN.saturating_add(99))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    demand.add_queued(Resources::default());

    assert_eq!(demand.instances(&policy), 3);
    Ok(())
}

#[test]
fn max_instances_cap_beats_huge_demand() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(4))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    demand.add_running(1000);

    assert_eq!(demand.instances(&policy), 5);
    Ok(())
}

#[test]
fn running_actions_saturate_without_overflow() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    demand.add_running(u32::MAX);
    demand.add_queued(Resources::default());

    assert_eq!(demand.instances(&policy()?), 100);
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
        demand.add_queued(Resources::default());
    }

    assert!(demand.fits(&policy));
    Ok(())
}

#[test]
fn fits_fails_one_action_over_max_instances() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(1))?,
        ..policy()?
    };
    let mut demand = Demand::default();
    for _ in 0..33 {
        demand.add_queued(Resources::default());
    }

    assert!(!demand.fits(&policy));
    Ok(())
}

#[test]
fn fits_ignores_min_instances() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(2, NonZeroU32::MIN.saturating_add(1))?,
        ..policy()?
    };

    assert!(Demand::default().fits(&policy));
    Ok(())
}

#[test]
fn fits_counts_measured_vcpus() -> Result<(), Box<dyn std::error::Error>> {
    let policy = FleetPolicy {
        instances: InstanceBounds::try_new(0, NonZeroU32::MIN)?,
        ..policy()?
    };
    let mut at_limit = Demand::default();
    at_limit.add_queued(resources(15, Bytes::new(0)));
    let mut over_limit = Demand::default();
    over_limit.add_queued(resources(16, Bytes::new(0)));

    assert!(at_limit.fits(&policy));
    assert!(!over_limit.fits(&policy));
    Ok(())
}

#[test]
fn disk_alone_drives_the_instance_count() -> Result<(), Box<dyn std::error::Error>> {
    let mut demand = Demand::default();
    for _ in 0..3 {
        demand.add_queued(Resources {
            vcpus: 1,
            memory: Bytes::gib(1),
            disk: Bytes::gib(60),
        });
    }

    assert_eq!(demand.instances(&policy()?), 2);
    Ok(())
}
