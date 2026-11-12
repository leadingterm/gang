//! The per-fleet outlook a pass writes to each `FleetPass`.

use std::time::Duration;

use gang_reconciler::WorkerCounts;
use gang_testkit::TestResult;
use gang_types::{FleetName, FleetSet, InstanceLifecycle, Isa};

use crate::support::{ARM, Harness, X86, policy, policy_named, x86_resources};

/// The harness with the x86-64 fleet at [`X86`] and an aarch64 fleet at [`ARM`].
fn harness() -> TestResult<Harness> {
    Ok(Harness::with_fleets(vec![
        policy()?,
        policy_named("gang-arm", Isa::Aarch64)?,
    ]))
}

/// Each fleet reports the instances its own demand needs, running actions included.
#[test]
fn needed_follows_each_fleets_demand() -> TestResult {
    let mut h = harness()?;
    h.connect(X86, "local-1")?;
    h.set_running("local-1", 2)?;
    h.queue(X86, 9)?;
    h.reconcile();

    let x86 = h.outlook(X86)?;
    assert_eq!(x86.demand.actions(), 11);
    assert_eq!(x86.needed, 3);
    assert_eq!(h.outlook(ARM)?.needed, 0);
    Ok(())
}

/// Worker counts include draining actions and only in-service unconnected instances.
#[test]
fn counts_workers_and_unconnected_instances() -> TestResult {
    let mut h = harness()?;
    h.connect(X86, "local-1")?;
    h.set_running("local-1", 2)?;
    h.connect(X86, "local-2")?;
    h.connect(X86, "local-3")?;
    h.set_running("local-3", 3)?;
    h.set_draining("local-3", true)?;
    h.connect(ARM, "local-4")?;
    h.set_running("local-4", 7)?;
    h.connect_as(FleetName::try_new("unknown")?, "local-5")?;
    h.set_running("local-5", 11)?;
    h.launch(X86, "local-6", InstanceLifecycle::InService, Duration::ZERO)?;
    h.launch(X86, "local-7", InstanceLifecycle::Pending, Duration::ZERO)?;
    h.launch(
        X86,
        "local-8",
        InstanceLifecycle::Terminating,
        Duration::ZERO,
    )?;
    h.reconcile();

    assert_eq!(
        h.outlook(X86)?.workers,
        WorkerCounts {
            busy: 1,
            idle: 1,
            draining: 1,
            unconnected: 1,
            running: 5,
        }
    );
    assert_eq!(h.outlook(X86)?.demand.actions(), 2);
    assert_eq!(h.outlook(ARM)?.workers.busy, 1);
    assert_eq!(h.outlook(ARM)?.workers.running, 7);

    h.disconnect("local-1")?;
    h.reconcile();
    assert_eq!(h.outlook(X86)?.workers.busy, 0);
    assert_eq!(h.outlook(X86)?.workers.running, 3);
    assert_eq!(h.outlook(X86)?.workers.unconnected, 1);
    Ok(())
}

/// A queued action is charged to one fleet, but counts as a candidate of every fleet it names.
#[test]
fn queued_is_exclusive_while_candidates_overlap() -> TestResult {
    let mut h = harness()?;
    h.queue_candidates(FleetSet::prefix(2)?, 20);
    h.reconcile();

    let x86 = h.outlook(X86)?;
    let arm = h.outlook(ARM)?;
    // The first fleet takes actions until its four instances are full, which is 16 actions.
    assert_eq!(x86.queued, 16);
    assert_eq!(arm.queued, 4);
    assert_eq!(x86.candidates, 20);
    assert_eq!(arm.candidates, 20);
    Ok(())
}

/// The vCPUs and memory of measured queued actions are charged to the fleet they are attributed
/// to, and an unmeasured action counts as one action only.
#[test]
fn demand_carries_measured_resources_of_attributed_actions() -> TestResult {
    let mut h = harness()?;
    h.queue_sized(X86, 2, x86_resources(2))?;
    h.queue(X86, 1)?;
    h.reconcile();

    let x86 = h.outlook(X86)?;
    assert_eq!(x86.demand.actions(), 3);
    assert_eq!(x86.demand.vcpus(), 4);
    assert_eq!(x86.demand.memory(), 2 << 30);
    assert_eq!(h.outlook(ARM)?.demand.vcpus(), 0);
    Ok(())
}

/// An action that names no fleet is charged to none and counts as a candidate of none.
#[test]
fn unplaceable_action_is_charged_to_no_fleet() -> TestResult {
    let mut h = harness()?;
    h.queue_candidates(FleetSet::EMPTY, 2);
    h.queue_candidates(FleetSet::try_from_indices([5])?, 3);
    h.reconcile();

    for index in [X86, ARM] {
        let fleet = h.outlook(index)?;
        assert_eq!(fleet.queued, 0);
        assert_eq!(fleet.candidates, 0);
        assert_eq!(fleet.demand.actions(), 0);
    }
    Ok(())
}

#[test]
fn outlook_is_refilled_on_each_pass() -> TestResult {
    let mut h = harness()?;
    h.queue(X86, 2)?;
    h.reconcile();
    assert_eq!(h.outlook(X86)?.queued, 2);

    h.snapshot.queued.clear();
    h.reconcile();
    assert_eq!(h.outlook(X86)?.queued, 0);
    assert_eq!(h.outlook(X86)?.demand.actions(), 0);
    Ok(())
}
