use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Clock;
use gang_config::{FleetPolicy, InstanceBounds};
use gang_reconciler::{Action, PlanStep, TerminationCause};
use gang_spi::{ScalingAuditEvent, ScalingEventKind};
use gang_testkit::{TestResult, worker_id};
use gang_types::{FleetName, FleetSet, InstanceId, InstanceLifecycle, Isa, ScalingCause, WorkerId};

use crate::support::*;

/// Index of the spot fleet in the preference test.
const SPOT: usize = 0;

/// Index of the on-demand fleet in the preference test.
const ONDEMAND: usize = 1;

/// A second worker on `instance`, sorting after the shared test worker.
fn renewed_worker(instance: &InstanceId) -> TestResult<WorkerId> {
    Ok(WorkerId::try_new(format!(
        "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
        instance.worker_name()
    ))?)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn x86_name() -> TestResult<FleetName> {
    Ok(FleetName::try_new("gang-x86")?)
}

fn instance(name: &str) -> TestResult<InstanceId> {
    Ok(InstanceId::try_new(name)?)
}

fn terminate(name: &str, cause: TerminationCause) -> TestResult<PlanStep> {
    Ok(PlanStep {
        fleet: X86,
        action: Action::Terminate {
            instance: instance(name)?,
            cause,
        },
    })
}

fn cancel_drain(name: &str) -> TestResult<PlanStep> {
    Ok(PlanStep {
        fleet: X86,
        action: Action::CancelDrain(worker_id(name)?),
    })
}

/// Launches `name` in the fleet at index `fleet` in service `launched_ago` ago, raises that
/// fleet's desired capacity to count it, and connects its idle worker.
fn serve_in(h: &mut Harness, fleet: usize, name: &str, launched_ago: Duration) -> TestResult {
    let desired = h.supply(fleet)?.desired;
    h.launch(fleet, name, InstanceLifecycle::InService, launched_ago)?;
    h.set_desired_capacity(fleet, desired + 1);
    h.connect(fleet, name)?;
    Ok(())
}

/// Launches `name` in service `launched_ago` ago, raises the desired capacity to count it, and
/// connects its idle worker.
fn serve(h: &mut Harness, name: &str, launched_ago: Duration) -> TestResult {
    serve_in(h, X86, name, launched_ago)
}

/// Launches `name` as a release-first instance in the fleet at index `fleet` in service
/// `launched_ago` ago, raises that fleet's desired capacity to count it, and connects its idle
/// worker.
fn serve_release_first(
    h: &mut Harness,
    fleet: usize,
    name: &str,
    launched_ago: Duration,
) -> TestResult {
    let desired = h.supply(fleet)?.desired;
    h.launch_release_first(fleet, name, InstanceLifecycle::InService, launched_ago)?;
    h.set_desired_capacity(fleet, desired + 1);
    h.connect(fleet, name)?;
    Ok(())
}

/// The x86-64 policy with the ten-minute idle timeout that the release-order scenarios assume.
fn ten_minute_policy() -> TestResult<FleetPolicy> {
    let mut policy = policy()?;
    policy.idle_timeout = Duration::from_mins(10);
    Ok(policy)
}

/// Four serving instances, of which `local-3` and `local-4` are idle and draining. Four
/// unmeasured actions are queued, so one instance is needed and the two draining ones are
/// surplus.
fn surplus_draining_fleet() -> TestResult<Harness> {
    let mut h = Harness::new()?;
    for name in ["local-1", "local-2", "local-3", "local-4"] {
        serve(&mut h, name, secs(3600))?;
    }
    h.set_draining("local-3", true)?;
    h.set_draining("local-4", true)?;
    h.queue(X86, 4)?;
    Ok(h)
}

#[test]
fn cold_scale_out_from_zero() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(X86, 1)?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 0, to: 1 },
        }]
    );
    Ok(())
}

#[test]
fn scale_out_is_clamped_at_max_instances() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(X86, 100)?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 0, to: 4 },
        }]
    );
    Ok(())
}

#[test]
fn booting_instance_does_not_repeat_scale_out() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(X86, 1)?;
    h.run();
    h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(1))?;
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn restart_mid_scale_out_does_not_repeat_it() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(X86, 1)?;
    h.run();
    h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(1))?;
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn scale_out_counts_running_actions() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 4)?;
    h.queue(X86, 1)?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 1, to: 2 },
        }]
    );
    Ok(())
}

#[test]
fn measured_resources_drive_scale_out() -> TestResult {
    let mut h = Harness::new()?;
    h.queue_sized(X86, 2, x86_resources(4))?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 0, to: 2 },
        }]
    );
    Ok(())
}

#[test]
fn draining_worker_is_not_capacity() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-1", true)?;
    h.queue(X86, 9)?;
    assert_eq!(
        h.run(),
        vec![
            cancel_drain("local-1")?,
            PlanStep {
                fleet: X86,
                action: Action::ScaleOut { from: 1, to: 3 },
            },
        ]
    );
    Ok(())
}

#[test]
fn demand_returning_resumes_a_draining_worker_before_scaling_out() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 2)?;
    h.set_draining("local-1", true)?;
    h.queue(X86, 1)?;
    assert_eq!(h.run(), vec![cancel_drain("local-1")?]);
    assert_eq!(h.reconcile(), Vec::new());
    assert!(h.snapshot.workers.iter().all(|worker| !worker.draining));
    Ok(())
}

#[test]
fn a_drained_worker_is_resumed_instead_of_terminated_when_demand_returns() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_draining("local-1", true)?;
    h.queue(X86, 1)?;
    assert_eq!(h.run(), vec![cancel_drain("local-1")?]);
    Ok(())
}

#[test]
fn resumes_the_busiest_draining_workers_first_then_scales_out() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    serve(&mut h, "local-2", Duration::ZERO)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-1", true)?;
    h.set_running("local-2", 3)?;
    h.set_draining("local-2", true)?;
    h.queue(X86, 9)?;
    assert_eq!(
        h.run(),
        vec![
            cancel_drain("local-2")?,
            cancel_drain("local-1")?,
            PlanStep {
                fleet: X86,
                action: Action::ScaleOut { from: 2, to: 3 },
            },
        ]
    );
    Ok(())
}

#[test]
fn resumes_only_the_shortfall_and_terminates_the_other_finished_drain() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    serve(&mut h, "local-2", Duration::ZERO)?;
    h.set_draining("local-1", true)?;
    h.set_draining("local-2", true)?;
    h.queue(X86, 1)?;
    assert_eq!(
        h.run(),
        vec![
            cancel_drain("local-1")?,
            terminate("local-2", TerminationCause::DrainFinished)?
        ]
    );
    Ok(())
}

#[test]
fn resume_ties_break_by_worker_id() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-2", Duration::ZERO)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-2", 1)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-2", true)?;
    h.set_draining("local-1", true)?;
    h.queue(X86, 1)?;
    assert_eq!(h.run(), vec![cancel_drain("local-1")?]);
    Ok(())
}

#[test]
fn draining_workers_are_not_resumed_without_demand() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn a_renewed_worker_keeps_its_instance_while_the_drained_old_one_lingers() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(3600))?;
    h.set_draining("local-1", true)?;
    let mut renewed = h.snapshot.workers[0].clone();
    renewed.id = renewed_worker(&instance("local-1")?)?;
    renewed.draining = false;
    renewed.running_actions = 2;
    renewed.last_seen = h.clock.wall();
    h.snapshot.workers.push(renewed);

    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn an_instance_drains_only_when_all_its_workers_drain() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(3600))?;
    h.set_draining("local-1", true)?;
    let mut renewed = h.snapshot.workers[0].clone();
    renewed.id = renewed_worker(&instance("local-1")?)?;
    renewed.running_actions = 1;
    h.snapshot.workers.push(renewed);

    assert_eq!(h.reconcile(), Vec::new());
    h.snapshot.workers[1].running_actions = 0;
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::DrainFinished)?]
    );
    Ok(())
}

#[test]
fn an_idle_instance_with_two_connections_drains_its_newest_worker() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    let mut renewed = h.snapshot.workers[0].clone();
    renewed.id = renewed_worker(&instance("local-1")?)?;
    renewed.last_seen = h.clock.wall();
    let newest = renewed.id.clone();
    h.snapshot.workers.push(renewed);
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(15));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(newest),
        }]
    );
    Ok(())
}

#[test]
fn idle_worker_drains_after_idle_timeout_then_terminates_once_drained() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    assert_eq!(h.run(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.run(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Terminate {
                instance: instance("local-1")?,
                cause: TerminationCause::DrainFinished,
            },
        }]
    );
    assert_eq!(h.supply(X86)?.desired, 0);
    assert!(h.supply(X86)?.instances.is_empty());
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn busy_worker_resets_its_idle_timer() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(10));
    h.set_running("local-1", 1)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.set_running("local-1", 0)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

#[test]
fn min_instances_keeps_an_idle_worker() -> TestResult {
    let mut policy: FleetPolicy = policy()?;
    policy.instances = InstanceBounds::try_new(1, policy.instances.max())?;
    let mut h = Harness::with_policy(policy);
    serve(&mut h, "local-1", secs(60))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(Duration::from_hours(1));
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn drain_waits_for_running_actions_until_max_drain_wait() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    h.set_running("local-1", 2)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(59));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Terminate {
                instance: instance("local-1")?,
                cause: TerminationCause::MaxDrainWait,
            },
        }]
    );
    Ok(())
}

#[test]
fn drain_ends_when_actions_finish() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    h.set_running("local-1", 2)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.set_running("local-1", 0)?;
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Terminate {
                instance: instance("local-1")?,
                cause: TerminationCause::DrainFinished,
            },
        }]
    );
    Ok(())
}

#[test]
fn restart_mid_drain_resumes_from_the_drain_flag() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    h.set_running("local-1", 0)?;
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Terminate {
                instance: instance("local-1")?,
                cause: TerminationCause::DrainFinished,
            },
        }]
    );
    Ok(())
}

#[test]
fn restart_mid_drain_restarts_the_drain_wait() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_running("local-1", 1)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30));
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(59));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Terminate {
                instance: instance("local-1")?,
                cause: TerminationCause::MaxDrainWait,
            },
        }]
    );
    Ok(())
}

#[test]
fn restart_restarts_idle_timers() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.reconcile(), Vec::new());
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

#[test]
fn worker_that_disconnects_restarts_its_idle_timer() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.reconcile(), Vec::new());
    h.disconnect("local-1")?;
    assert_eq!(h.reconcile(), Vec::new());
    h.connect(X86, "local-1")?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(14));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

#[test]
fn booting_instance_is_not_an_orphan_within_boot_grace() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(10))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(19));
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn orphan_instance_is_terminated_after_boot_grace() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(X86, "local-1", InstanceLifecycle::InService, secs(30))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(29));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.run(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    assert_eq!(h.supply(X86)?.desired, 0);
    Ok(())
}

#[test]
fn orphan_is_terminated_before_the_same_pass_scales_out() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 2);
    h.launch(X86, "local-1", InstanceLifecycle::InService, secs(3600))?;
    h.launch(X86, "local-2", InstanceLifecycle::InService, secs(3600))?;
    h.connect(X86, "local-2")?;
    h.set_running("local-2", 4)?;
    h.queue(X86, 1)?;
    h.reconcile();
    h.advance(secs(30));
    assert_eq!(
        h.reconcile(),
        vec![
            terminate("local-1", TerminationCause::Orphan)?,
            PlanStep {
                fleet: X86,
                action: Action::ScaleOut { from: 1, to: 2 },
            },
        ]
    );
    Ok(())
}

#[test]
fn pending_instance_is_never_an_orphan_however_long_it_waits() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.queue(X86, 1)?;
    h.launch(
        X86,
        "local-1",
        InstanceLifecycle::Pending,
        Duration::from_hours(1),
    )?;
    for _ in 0..5 {
        assert_eq!(h.reconcile(), Vec::new());
        h.advance(secs(60));
    }
    assert_eq!(h.supply(X86)?.desired, 1);
    Ok(())
}

#[test]
fn a_long_pending_instance_gets_its_boot_grace_once_in_service() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(60));
    assert_eq!(h.reconcile(), Vec::new());
    h.set_lifecycle(X86, "local-1", InstanceLifecycle::InService, secs(3660))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30));
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    Ok(())
}

#[test]
fn fleets_sharing_an_instance_id_keep_separate_timers() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]);
    serve_in(&mut h, X86, "local-1", secs(3600))?;
    h.set_running("local-1", 1)?;
    h.set_desired_capacity(ARM, 1);
    h.launch(ARM, "local-1", InstanceLifecycle::InService, secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30));
    let mut step = terminate("local-1", TerminationCause::Orphan)?;
    step.fleet = ARM;
    assert_eq!(h.reconcile(), vec![step]);
    Ok(())
}

#[test]
fn pending_instance_is_terminated_only_after_boot_grace_from_when_it_starts_running() -> TestResult
{
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(
        X86,
        "local-1",
        InstanceLifecycle::Pending,
        Duration::from_hours(1),
    )?;
    h.advance(secs(60));
    assert_eq!(h.reconcile(), Vec::new());
    h.set_lifecycle(X86, "local-1", InstanceLifecycle::InService, Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(29));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.run(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    assert_eq!(h.supply(X86)?.desired, 0);
    Ok(())
}

#[test]
fn long_running_instance_that_loses_its_worker_is_not_an_orphan_at_once() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.disconnect("local-1")?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(29));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    Ok(())
}

#[test]
fn restart_with_an_empty_snapshot_does_not_orphan_long_running_instances() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(3600))?;
    h.disconnect("local-1")?;
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(29));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    Ok(())
}

#[test]
fn an_ordered_termination_is_not_repeated_or_recounted_while_the_provisioner_lags() -> TestResult {
    let mut h = surplus_draining_fleet()?;
    assert_eq!(
        h.reconcile(),
        vec![
            terminate("local-3", TerminationCause::DrainFinished)?,
            terminate("local-4", TerminationCause::DrainFinished)?,
        ]
    );
    // The provisioner has not applied the terminations: it still lists both instances and its
    // desired capacity is unchanged.
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn an_ordered_termination_does_not_drain_below_min_instances() -> TestResult {
    let mut policy: FleetPolicy = policy()?;
    policy.instances = InstanceBounds::try_new(1, policy.instances.max())?;
    let idle_timeout = policy.idle_timeout;
    let mut h = Harness::with_policy(policy);
    serve(&mut h, "local-1", secs(3600))?;
    serve(&mut h, "local-2", secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(idle_timeout);
    h.set_draining("local-2", true)?;
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-2", TerminationCause::DrainFinished)?]
    );
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn an_ordered_termination_does_not_hide_a_needed_scale_out() -> TestResult {
    let mut policy: FleetPolicy = policy()?;
    policy.instances = InstanceBounds::try_new(1, policy.instances.max())?;
    let idle_timeout = policy.idle_timeout;
    let queued = usize::try_from(policy.target_actions_per_instance.get())? + 1;
    let mut h = Harness::with_policy(policy);
    serve(&mut h, "local-1", secs(3600))?;
    serve(&mut h, "local-2", secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(idle_timeout);
    h.set_draining("local-2", true)?;
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-2", TerminationCause::DrainFinished)?]
    );
    h.queue(X86, queued)?;
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 1, to: 2 },
        }]
    );
    Ok(())
}

#[test]
/// The order counts as lost once `boot_grace` has passed, so it is issued again.
fn a_lost_termination_order_is_issued_again_after_boot_grace() -> TestResult {
    let mut h = surplus_draining_fleet()?;
    h.set_running("local-1", 1)?;
    h.set_running("local-2", 1)?;
    assert_eq!(
        h.reconcile(),
        vec![
            terminate("local-3", TerminationCause::DrainFinished)?,
            terminate("local-4", TerminationCause::DrainFinished)?,
        ]
    );
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(29));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.reconcile(),
        vec![
            terminate("local-3", TerminationCause::DrainFinished)?,
            terminate("local-4", TerminationCause::DrainFinished)?,
        ]
    );
    Ok(())
}

#[test]
fn a_withdrawn_termination_order_is_issued_again_at_once() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(X86, "local-1", InstanceLifecycle::InService, secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30));
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    assert_eq!(h.reconcile(), Vec::new());
    h.reconciler.forget_order(X86, &instance("local-1")?);
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::Orphan)?]
    );
    Ok(())
}

#[test]
fn terminating_instances_are_forgotten_after_an_order() -> TestResult {
    let mut h = surplus_draining_fleet()?;
    assert_eq!(
        h.reconcile(),
        vec![
            terminate("local-3", TerminationCause::DrainFinished)?,
            terminate("local-4", TerminationCause::DrainFinished)?,
        ]
    );
    let supply = h.supply_mut(X86)?;
    let ordered = [instance("local-3")?, instance("local-4")?];
    for entry in &mut supply.instances {
        if ordered.contains(&entry.id) {
            entry.lifecycle = InstanceLifecycle::Terminating;
        }
    }
    h.set_desired_capacity(X86, 2);
    h.set_running("local-1", 1)?;
    h.set_running("local-2", 1)?;
    h.advance(secs(31));
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn terminating_instances_are_left_alone() -> TestResult {
    let mut h = Harness::new()?;
    h.launch(X86, "local-1", InstanceLifecycle::Terminating, secs(3600))?;
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

#[test]
fn drains_longest_idle_first_up_to_the_excess() -> TestResult {
    let mut policy: FleetPolicy = policy()?;
    policy.instances = InstanceBounds::try_new(1, policy.instances.max())?;
    let mut h = Harness::with_policy(policy);
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5));
    serve(&mut h, "local-2", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5));
    serve(&mut h, "local-3", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(15));
    assert_eq!(
        h.reconcile(),
        vec![
            PlanStep {
                fleet: X86,
                action: Action::StartDrain(worker_id("local-1")?),
            },
            PlanStep {
                fleet: X86,
                action: Action::StartDrain(worker_id("local-2")?),
            },
        ]
    );
    Ok(())
}

#[test]
fn surplus_drain_ties_break_by_worker_id() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-3", Duration::ZERO)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.queue(X86, 1)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(16));
    assert_eq!(
        h.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

#[test]
fn fleets_are_independent_per_isa() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]);
    h.queue(ARM, 1)?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: ARM,
            action: Action::ScaleOut { from: 0, to: 1 },
        }]
    );
    assert_eq!(h.supply(X86)?.desired, 0);
    Ok(())
}

#[test]
fn demand_for_an_unconfigured_isa_is_ignored() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(ARM, 1)?;
    assert_eq!(h.run(), Vec::new());
    Ok(())
}

#[test]
fn audit_events_record_scale_out() -> TestResult {
    let mut h = Harness::new()?;
    h.queue(X86, 1)?;
    h.run();
    let events: Vec<ScalingAuditEvent> = h.audit_events()?;
    assert_eq!(
        events,
        vec![ScalingAuditEvent {
            at: h.clock.wall(),
            fleet: x86_name()?,
            kind: ScalingEventKind::ScaledOut { from: 0, to: 1 },
            cause: Some(ScalingCause::Demand),
        }]
    );
    Ok(())
}

#[test]
fn audit_events_record_drain_start() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(15));
    h.reconcile();
    let events: Vec<ScalingAuditEvent> = h.audit_events()?;
    assert_eq!(
        events,
        vec![ScalingAuditEvent {
            at: h.clock.wall(),
            fleet: x86_name()?,
            kind: ScalingEventKind::DrainStarted {
                worker: worker_id("local-1")?,
            },
            cause: Some(ScalingCause::IdleTimeout),
        }]
    );
    Ok(())
}

#[test]
fn audit_events_record_termination() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    h.set_running("local-1", 0)?;
    h.set_draining("local-1", true)?;
    h.reconcile();
    let events: Vec<ScalingAuditEvent> = h.audit_events()?;
    assert_eq!(
        events,
        vec![ScalingAuditEvent {
            at: h.clock.wall(),
            fleet: x86_name()?,
            kind: ScalingEventKind::Terminated {
                instance: instance("local-1")?,
            },
            cause: Some(ScalingCause::DrainFinished),
        }]
    );
    Ok(())
}

#[test]
fn cancel_drain_is_audited() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    h.set_draining("local-1", true)?;
    h.queue(X86, 1)?;
    assert_eq!(h.reconcile(), vec![cancel_drain("local-1")?]);
    let events: Vec<ScalingAuditEvent> = h.audit_events()?;
    assert_eq!(
        events,
        vec![ScalingAuditEvent {
            at: h.clock.wall(),
            fleet: x86_name()?,
            kind: ScalingEventKind::DrainCancelled {
                worker: worker_id("local-1")?,
            },
            cause: Some(ScalingCause::Demand),
        }]
    );
    Ok(())
}

#[test]
fn same_inputs_give_the_same_plan() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    h.queue(X86, 5)?;
    h.reconcile();
    let first = h.plan.clone();
    h.restart();
    h.reconcile();
    assert_eq!(h.plan, first);
    Ok(())
}

#[test]
fn preference_fills_the_first_fleet_then_overflows_to_the_next() -> TestResult {
    let mut spot = policy_named("gang-spot", Isa::X86_64)?;
    spot.instances = InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(1))?;
    spot.target_actions_per_instance = NonZeroU32::MIN;
    let mut ondemand = policy_named("gang-ondemand", Isa::X86_64)?;
    ondemand.instances = InstanceBounds::try_new(0, NonZeroU32::MIN.saturating_add(4))?;
    ondemand.target_actions_per_instance = NonZeroU32::MIN;
    let mut h = Harness::with_fleets(vec![spot, ondemand]);
    h.queue_candidates(FleetSet::try_from_indices([SPOT, ONDEMAND])?, 3);
    assert_eq!(
        h.run(),
        vec![
            PlanStep {
                fleet: SPOT,
                action: Action::ScaleOut { from: 0, to: 2 },
            },
            PlanStep {
                fleet: ONDEMAND,
                action: Action::ScaleOut { from: 0, to: 1 },
            },
        ]
    );
    Ok(())
}

#[test]
fn pinned_action_scales_only_its_candidate_fleet() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]);
    h.queue_candidates(FleetSet::try_from_indices([ARM])?, 1);
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: ARM,
            action: Action::ScaleOut { from: 0, to: 1 },
        }]
    );
    assert_eq!(h.supply(X86)?.desired, 0);
    Ok(())
}

#[test]
fn action_without_candidates_scales_nothing() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]);
    h.queue_candidates(FleetSet::EMPTY, 3);
    assert_eq!(h.run(), Vec::new());
    assert_eq!(h.supply(X86)?.desired, 0);
    assert_eq!(h.supply(ARM)?.desired, 0);
    Ok(())
}

#[test]
fn running_actions_count_on_their_own_fleet_only() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]);
    serve_in(&mut h, ARM, "local-2", Duration::ZERO)?;
    h.set_running("local-2", 4)?;
    // A worker of a fleet that is not configured runs four actions too; it must be ignored.
    h.connect_as(FleetName::try_new("gang-unknown")?, "local-9")?;
    h.set_running("local-9", 4)?;
    h.queue(X86, 1)?;
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::ScaleOut { from: 0, to: 1 },
        }]
    );
    assert_eq!(h.supply(ARM)?.desired, 1);
    Ok(())
}

#[test]
fn fleets_with_the_same_isa_are_reconciled_independently() -> TestResult {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-x86-b", Isa::X86_64)?]);
    serve(&mut h, "local-1", secs(60))?;
    serve_in(&mut h, ONDEMAND, "local-2", secs(60))?;
    h.set_running("local-2", 1)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(15));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    assert_eq!(
        h.run(),
        vec![terminate("local-1", TerminationCause::DrainFinished)?]
    );
    assert_eq!(h.supply(ONDEMAND)?.desired, 1);
    assert_eq!(h.supply(ONDEMAND)?.instances.len(), 1);
    Ok(())
}

/// R1: the on-demand (release-first) worker drains although the Spot worker has been idle longer.
#[test]
fn release_order_drains_the_on_demand_worker_before_a_longer_idle_spot_worker() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(19 * 60));
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(11 * 60));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-2")?),
        }]
    );
    Ok(())
}

#[test]
fn audit_events_record_an_orphan_termination() -> TestResult {
    let mut h = Harness::new()?;
    h.set_desired_capacity(X86, 1);
    h.launch(X86, "local-1", InstanceLifecycle::InService, secs(3600))?;
    h.reconcile();
    h.advance(secs(30));
    h.reconcile();
    let events: Vec<ScalingAuditEvent> = h.audit_events()?;
    assert_eq!(
        events,
        vec![ScalingAuditEvent {
            at: h.clock.wall(),
            fleet: x86_name()?,
            kind: ScalingEventKind::Terminated {
                instance: instance("local-1")?,
            },
            cause: Some(ScalingCause::Orphan),
        }]
    );
    Ok(())
}

/// R2: an on-demand worker is not eligible before its own idle timeout, so the Spot worker is kept.
#[test]
fn on_demand_worker_is_not_eligible_before_its_idle_timeout() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(25 * 60));
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5 * 60));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5 * 60));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-2")?),
        }]
    );
    Ok(())
}

/// R3: a busy on-demand worker holds back an idle Spot worker.
#[test]
fn busy_on_demand_worker_holds_back_an_idle_spot_worker() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30 * 60));
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    h.set_running("local-2", 1)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(10 * 60));
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

/// R3 (surplus 2): one busy on-demand worker and two idle Spot workers; exactly one Spot drains.
#[test]
fn surplus_beyond_a_busy_on_demand_worker_drains_one_spot_worker() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    serve(&mut h, "local-3", Duration::ZERO)?;
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    h.set_running("local-2", 1)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(10 * 60));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

/// R4: a busy Spot worker is never a drain candidate, however large the surplus.
#[test]
fn busy_spot_workers_are_never_drain_candidates() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    for name in ["local-1", "local-2", "local-3"] {
        serve(&mut h, name, Duration::ZERO)?;
        h.set_running(name, 1)?;
    }
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30 * 60));
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

/// R5: a plain fleet (no release-first instance) drains its longest-idle worker first.
#[test]
fn plain_fleet_drains_the_longest_idle_worker_first() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5 * 60));
    serve(&mut h, "local-2", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(5 * 60));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    Ok(())
}

/// R5: a plain fleet drains an idle worker, then terminates it once it has drained.
#[test]
fn plain_fleet_drains_an_idle_worker_then_terminates_it() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(10 * 60));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-1")?),
        }]
    );
    assert_eq!(
        h.run(),
        vec![terminate("local-1", TerminationCause::DrainFinished)?]
    );
    Ok(())
}

/// R6: a booting on-demand instance is included in the release set, so the idle Spot worker is kept.
#[test]
fn booting_on_demand_instance_counts_toward_release_first_capacity() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(30 * 60));
    h.launch_release_first(X86, "local-2", InstanceLifecycle::Pending, secs(1))?;
    h.set_desired_capacity(X86, 2);
    assert_eq!(h.reconcile(), Vec::new());
    Ok(())
}

/// R7: with one shortfall, the Spot worker is resumed first; the on-demand worker is terminated
/// once it has finished draining.
#[test]
fn resume_takes_the_spot_worker_before_the_on_demand_one() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    serve_release_first(&mut h, X86, "local-1", Duration::ZERO)?;
    serve(&mut h, "local-2", Duration::ZERO)?;
    h.set_draining("local-1", true)?;
    h.set_draining("local-2", true)?;
    h.queue(X86, 1)?;
    let first = h.run();
    assert!(first.contains(&cancel_drain("local-2")?));
    assert!(!first.contains(&cancel_drain("local-1")?));
    let mut terminated = first.contains(&terminate("local-1", TerminationCause::DrainFinished)?);
    terminated |= h
        .run()
        .contains(&terminate("local-1", TerminationCause::DrainFinished)?);
    assert!(terminated);
    Ok(())
}

/// R8: a restarted reconciler recovers an on-demand drain from the worker's drain flag and
/// terminates the finished instance.
#[test]
fn restart_recovers_a_release_first_drain_from_the_worker_flag() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    serve(&mut h, "local-1", Duration::ZERO)?;
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    h.queue(X86, 1)?;
    h.set_draining("local-2", true)?;
    h.restart();
    assert_eq!(h.supply(X86)?.desired, 2);
    assert_eq!(
        h.run(),
        vec![terminate("local-2", TerminationCause::DrainFinished)?]
    );
    Ok(())
}

/// R9: a restarted reconciler waits a full idle timeout in the new process, then drains the
/// release-first worker before the other worker of equal idle start.
#[test]
fn restart_resets_idle_timers_and_drains_the_release_first_worker_first() -> TestResult {
    let mut h = Harness::with_policy(ten_minute_policy()?);
    h.queue(X86, 1)?;
    serve(&mut h, "local-1", Duration::ZERO)?;
    serve_release_first(&mut h, X86, "local-2", Duration::ZERO)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(9 * 60));
    h.restart();
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(10 * 60 - 1));
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(1));
    assert_eq!(
        h.run(),
        vec![PlanStep {
            fleet: X86,
            action: Action::StartDrain(worker_id("local-2")?),
        }]
    );
    Ok(())
}

#[test]
fn a_timed_out_drain_reports_drain_finished_once_its_worker_is_empty() -> TestResult {
    let mut h = Harness::new()?;
    serve(&mut h, "local-1", secs(60))?;
    h.set_running("local-1", 2)?;
    h.set_draining("local-1", true)?;
    assert_eq!(h.reconcile(), Vec::new());
    h.advance(secs(60));
    h.set_running("local-1", 0)?;
    assert_eq!(
        h.reconcile(),
        vec![terminate("local-1", TerminationCause::DrainFinished)?]
    );
    Ok(())
}
