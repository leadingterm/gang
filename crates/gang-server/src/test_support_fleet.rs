//! Fleet composition for the `gang-server` behavioural tests.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::{FleetMember, FleetPolicy, Fleets, InstanceBounds};
use gang_spi::QueuedDemand;
use gang_testkit::{Opaque, SandboxKey, TestPlugins};
use gang_types::{FleetName, FleetSet, Isa, NonZeroBytes, Resources};

/// The fleets of the test composition in preference order: one x86-64 fleet, `example-fleet`,
/// of up to four 4-vCPU, 8 `GiB` workers.
#[must_use]
pub fn fleets() -> Fleets<TestPlugins> {
    fleets_of(vec![("example-fleet", 4)])
}

/// The fleets of the test composition for the given `(name, max_instances)` pairs, in preference
/// order. Every fleet is x86-64 with the same 4-vCPU, 8 `GiB` instances as [`fleets`].
#[must_use]
pub fn fleets_of(members: Vec<(&str, u32)>) -> Fleets<TestPlugins> {
    Fleets::try_new(
        members
            .into_iter()
            .map(|(name, max_instances)| fleet_member(name, max_instances))
            .collect(),
    )
    .expect("the test fleets are within the fleet count limit")
}

/// One x86-64 fleet member named `name`, running the `noop` sandbox.
fn fleet_member(name: &str, max_instances: u32) -> FleetMember<TestPlugins> {
    let policy = FleetPolicy {
        name: FleetName::try_new(name).expect("test fleet name is valid"),
        isa: Isa::X86_64,
        instances: InstanceBounds::up_to(
            NonZeroU32::new(max_instances).expect("test maximum is nonzero"),
        ),
        vcpus_per_instance: NonZeroU32::new(4).expect("four vCPUs is non-zero"),
        memory_per_instance: NonZeroBytes::gib(8),
        disk_per_instance: NonZeroBytes::gib(100),
        target_actions_per_instance: NonZeroU32::new(4).expect("four actions is non-zero"),
        idle_timeout: Duration::from_secs(15),
        max_drain_wait: Duration::from_mins(1),
        boot_grace: Duration::from_secs(30),
    };
    FleetMember {
        policy,
        sandbox: SandboxKey::Noop,
        options: Opaque::default(),
    }
}

/// A queued demand that may run on any fleet in `candidates`, with no resource request, queued
/// at the Unix epoch with priority 0.
#[must_use]
pub fn queued_demand(candidates: FleetSet) -> QueuedDemand {
    QueuedDemand {
        candidates,
        resources: Resources::default(),
        queued_since: Timestamp::UNIX_EPOCH,
        priority: 0,
    }
}
