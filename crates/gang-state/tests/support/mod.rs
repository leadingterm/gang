//! Fixtures shared by the state builder's behaviour tests: hand-built scheduler and provisioner
//! inputs, fleet policies and the empty state frame that a builder fills.
//!
//! Integration tests pull this in with `mod support;` and import what they need with
//! `use crate::support::{..}`.

use std::error::Error;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::{FleetPolicy, InstanceBounds};
use gang_spi::{FleetInstance, QueuedDemand, WorkerState};
use gang_state::BuilderConfig;
use gang_testkit::fleet_policy;
use gang_types::{
    FleetName, FleetProperties, FleetSet, InstanceId, InstanceLifecycle, Isa, Resources, WorkerId,
};
use gang_wire::{State, name};

/// The timestamp `millis` milliseconds after the Unix epoch.
pub fn ts(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

/// The builder configuration of the tests: ten events, a five-second staleness bound, a
/// one-second interval and the `test-provisioner` provisioner.
pub fn config() -> BuilderConfig {
    BuilderConfig {
        events_limit: 10,
        stale_after: Duration::from_secs(5),
        interval: Duration::from_secs(1),
        provisioner: "test-provisioner".to_owned(),
    }
}

/// A fleet policy named `name` for aarch64 with four vCPUs and 8 GiB per instance, at most
/// `max_instances` instances, and `boot_grace_ms` milliseconds of boot grace.
pub fn policy(
    name: &str,
    boot_grace_ms: u64,
    max_instances: u32,
) -> Result<FleetPolicy, Box<dyn Error>> {
    Ok(FleetPolicy {
        isa: Isa::Aarch64,
        instances: InstanceBounds::try_new(
            0,
            NonZeroU32::new(max_instances).ok_or("zero maximum")?,
        )?,
        target_actions_per_instance: gang_types::nonzero_u32(2),
        idle_timeout: Duration::from_mins(1),
        max_drain_wait: Duration::from_mins(5),
        boot_grace: Duration::from_millis(boot_grace_ms),
        ..fleet_policy(name)?
    })
}

/// The fleet properties a provisioner sees for `policy` in sandbox `sandbox-a`, with `options`
/// as the custom options.
pub fn properties(policy: &FleetPolicy, options: Vec<(&'static str, String)>) -> FleetProperties {
    policy.properties(&"sandbox-a", options)
}

/// The instance id `i-{index}`.
pub fn instance_id(index: u64) -> Result<InstanceId, Box<dyn Error>> {
    Ok(InstanceId::try_new(format!("i-{index}"))?)
}

/// A supply entry for `id` in `lifecycle`, launched at `launched` milliseconds.
pub fn fleet_instance(
    id: &InstanceId,
    lifecycle: InstanceLifecycle,
    launched: u64,
) -> FleetInstance {
    FleetInstance {
        id: id.clone(),
        lifecycle,
        launched_at: ts(launched),
        release_first: false,
    }
}

/// The scheduler's view of worker `id` in `fleet`: `running` executing actions, the given
/// draining flag and concurrency limit, not paused.
pub fn worker_state(
    id: &WorkerId,
    fleet: &str,
    running: u32,
    draining: bool,
    max_inflight: Option<NonZeroU32>,
) -> Result<WorkerState, Box<dyn Error>> {
    Ok(WorkerState {
        id: id.clone(),
        fleet: FleetName::try_new(fleet)?,
        running_actions: running,
        max_inflight_actions: max_inflight,
        draining,
        paused: false,
        last_seen: ts(0),
    })
}

/// A queued action since `since` milliseconds, with no resource request and priority zero.
pub fn queued(since: u64, candidates: FleetSet) -> QueuedDemand {
    QueuedDemand {
        candidates,
        resources: Resources::default(),
        queued_since: ts(since),
        priority: 0,
    }
}

/// An empty state frame: no fleets, workers, instances or actions, and a passing pass.
pub fn blank() -> State {
    State {
        name: name::STATE.to_owned(),
        update_time: ts(0),
        ..State::default()
    }
}
