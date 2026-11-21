mod episodes;
mod rate_history;
mod work_aware;

use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{Clock, Timestamp};
use gang_config::{InstanceBounds, Weekday};
use gang_reconciler::{Action, DrainReason, NeedCause, PlanStep, TerminationCause};
use gang_spi::{ScalingEventKind, TerminationMode, WorkerState};
use gang_testkit::{TestResult, scheduled_minimum, worker_id};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_types::{FleetSet, InstanceId, InstanceLifecycle, Isa, ScalingCause, WorkerId};

use crate::support::{ARM, Harness, X86, policy, policy_named};
use work_aware::{Workload, fleet, purchases, secs, settings, source};

#[test]
fn mismatched_and_unknown_workers_supply_capacity_without_early_draining() -> TestResult {
    for environment in [Some("host:old".parse()?), None] {
        let mut policy = policy()?;
        policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
        let mut h = Harness::with_policy(policy);
        h.settle_term()?;
        serve(&mut h, "local-1", secs(60))?;
        h.snapshot.workers[0].environment = environment;
        h.queue(X86, 1)?;
        for _ in 0..2 {
            let steps = h.run();
            assert!(!steps.iter().any(|step| matches!(
                step.action,
                Action::StartDrain { .. } | Action::Terminate { .. } | Action::ScaleOut { .. }
            )));
            assert_eq!(h.outlook(X86)?.expected_wait, Some(Duration::ZERO));
        }
    }
    Ok(())
}

#[test]
fn stale_busy_instance_is_replaced_at_max_without_duplicate_purchase() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(2, NonZeroU32::new(2).unwrap())?;
    let max_wait = policy.max_drain_wait;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", secs(60))?;
    serve(&mut h, "local-current", secs(60))?;
    h.set_running("local-old", 1)?;
    h.snapshot
        .workers
        .iter_mut()
        .find(|w| w.id.instance_name() == Some("local-old"))
        .unwrap()
        .current_build = false;

    let first = changes(&h.run());
    assert!(first.contains(&Change::Reinforce(X86, "local-old".into())));
    assert_eq!(
        first
            .iter()
            .filter(|change| matches!(change, Change::Reinforce(..)))
            .count(),
        1
    );
    assert!(first.contains(&Change::Scale(X86, 2, 3, NeedCause::Minimum)));
    assert_eq!(h.outlook(X86)?.workers.running, 1);
    assert!(h.snapshot.cordons[0].replacing);
    assert!(
        !changes(&h.run())
            .iter()
            .any(|change| matches!(change, Change::Scale(..) | Change::Cancel(..)))
    );

    h.launch(X86, "local-new", InstanceLifecycle::InService, secs(0))?;
    h.connect(X86, "local-new")?;
    h.advance(max_wait + secs(1));
    assert!(!changes(&h.run()).iter().any(|change| matches!(
        change,
        Change::Terminate(..) | Change::Scale(..) | Change::Cancel(..)
    )));
    h.set_running("local-old", 0)?;
    let finished_steps = h.run();
    assert_termination_mode(&finished_steps, "local-old", TerminationMode::Replace);
    let finished = changes(&finished_steps);
    assert!(finished.contains(&Change::Terminate(
        X86,
        "local-old".into(),
        TerminationCause::DrainFinished
    )));
    assert!(
        !finished
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );
    assert_eq!(h.supply(X86)?.desired, 2);
    assert!(h.snapshot.workers.iter().all(|worker| worker.current_build));
    assert!(
        !changes(&h.run())
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );
    Ok(())
}

#[test]
fn busy_stale_work_creates_replacement_demand_without_a_minimum() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(0, NonZeroU32::MIN)?;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", secs(60))?;
    h.set_running("local-old", 1)?;
    h.snapshot.workers[0].current_build = false;

    let first = changes(&h.run());
    assert!(first.contains(&Change::Reinforce(X86, "local-old".into())));
    assert!(first.contains(&Change::Scale(X86, 1, 2, NeedCause::Demand)));
    assert_eq!(h.outlook(X86)?.needed, 1);
    assert!(
        !changes(&h.run())
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );

    h.launch(X86, "local-new", InstanceLifecycle::InService, secs(0))?;
    h.connect(X86, "local-new")?;
    assert!(
        !changes(&h.run())
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );
    assert_eq!(h.outlook(X86)?.needed, 1);
    h.set_running("local-old", 0)?;
    let finished_steps = h.run();
    assert_termination_mode(&finished_steps, "local-old", TerminationMode::Replace);
    let finished = changes(&finished_steps);
    assert!(finished.contains(&Change::Terminate(
        X86,
        "local-old".into(),
        TerminationCause::DrainFinished
    )));
    assert!(
        !finished
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );
    assert_eq!(h.outlook(X86)?.needed, 0);
    assert_eq!(h.supply(X86)?.desired, 1);
    Ok(())
}

#[test]
fn replacement_intent_survives_current_reconnect_and_model_restart() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", secs(60))?;
    h.set_running("local-old", 1)?;
    h.snapshot.workers[0].current_build = false;
    h.run();
    h.disconnect("local-old")?;
    h.connect(X86, "local-old")?;
    h.snapshot.workers[0].draining = false;
    h.set_running("local-old", 1)?;
    h.restart();
    let steps = changes(&h.run());
    assert!(
        !steps
            .iter()
            .any(|change| matches!(change, Change::Cancel(..) | Change::Scale(..)))
    );
    assert!(steps.contains(&Change::Reinforce(X86, "local-old".into())));
    assert!(h.snapshot.workers[0].current_build);
    h.advance(CLIENT_REROUTE_BOUND);
    h.launch(X86, "local-new", InstanceLifecycle::InService, secs(0))?;
    h.connect(X86, "local-new")?;
    h.set_running("local-old", 0)?;
    let finished = h.run();
    assert_termination_mode(&finished, "local-old", TerminationMode::Replace);
    Ok(())
}

#[test]
fn disconnected_replacement_terminates_after_connection_grace_without_rebuying() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
    let grace = policy.boot_grace;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", grace + secs(1))?;
    h.set_running("local-old", 1)?;
    h.snapshot.workers[0].current_build = false;
    h.run();
    h.disconnect("local-old")?;
    h.launch(X86, "local-new", InstanceLifecycle::InService, secs(0))?;
    h.connect(X86, "local-new")?;
    assert!(
        !changes(&h.run())
            .iter()
            .any(|change| matches!(change, Change::Terminate(..)))
    );
    h.advance(grace);
    let retirement = h.run();
    assert_termination_mode(&retirement, "local-old", TerminationMode::Replace);
    let steps = changes(&retirement);
    assert!(steps.contains(&Change::Terminate(
        X86,
        "local-old".into(),
        TerminationCause::Orphan
    )));
    assert!(
        !steps
            .iter()
            .any(|change| matches!(change, Change::Scale(..)))
    );
    assert_eq!(h.supply(X86)?.desired, 1);
    Ok(())
}

#[test]
fn one_stale_connection_replaces_the_entire_physical_instance() -> TestResult {
    for (stale_running, current_running) in [(1, 0), (0, 1), (1, 1)] {
        let mut policy = policy()?;
        policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
        let mut h = Harness::with_policy(policy);
        h.settle_term()?;
        serve(&mut h, "local-old", secs(60))?;
        h.set_running("local-old", stale_running)?;
        let mut current = renewed(&h.snapshot.workers[0], 1)?;
        current.running_actions = current_running;
        h.snapshot.workers[0].current_build = false;
        h.snapshot.workers.push(current);
        let plan = h.run();
        let steps = changes(&plan);
        assert!(steps.contains(&Change::Reinforce(X86, "local-old".into())));
        assert!(
            steps.contains(&Change::Scale(X86, 1, 2, NeedCause::Demand)),
            "expected one replacement for the physical instance: {plan:?}"
        );
        assert!(h.snapshot.workers.iter().all(|worker| worker.draining));
        assert!(h.snapshot.cordons[0].replacing);
        let outlook = h.outlook(X86)?;
        assert_eq!(outlook.workers.running, stale_running + current_running);
        assert_eq!(
            outlook.demand.actions(),
            u64::from(stale_running + current_running)
        );
        assert_eq!(outlook.needed, 1);
        assert!(
            !steps
                .iter()
                .any(|change| matches!(change, Change::Cancel(..) | Change::Terminate(..))),
            "busy replacement must keep draining: {plan:?}"
        );
    }
    Ok(())
}

#[test]
fn idle_stale_draining_instance_is_destroyed() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", secs(60))?;
    h.snapshot.workers[0].current_build = false;
    h.snapshot.workers[0].draining = true;

    let retirement = h.run();
    assert_termination_mode(&retirement, "local-old", TerminationMode::Replace);
    Ok(())
}

#[test]
fn already_draining_stale_instance_gets_replacement_intent() -> TestResult {
    let mut policy = policy()?;
    policy.instances = InstanceBounds::try_new(1, NonZeroU32::MIN)?;
    let mut h = Harness::with_policy(policy);
    h.settle_term()?;
    serve(&mut h, "local-old", secs(60))?;
    h.set_running("local-old", 1)?;
    h.snapshot.workers[0].current_build = false;
    h.snapshot.workers[0].draining = true;

    let plan = h.run();
    let steps = changes(&plan);
    assert!(steps.contains(&Change::Reinforce(X86, "local-old".into())));
    assert!(
        steps.contains(&Change::Scale(X86, 1, 2, NeedCause::Demand)),
        "expected replacement demand from the busy stale drain: {plan:?}"
    );
    assert!(h.snapshot.cordons[0].replacing);
    assert!(
        !steps
            .iter()
            .any(|change| matches!(change, Change::Cancel(..) | Change::Terminate(..)))
    );
    Ok(())
}

fn serve(h: &mut Harness, name: &str, launched_ago: Duration) -> TestResult {
    let desired = h.supply(X86)?.desired;
    h.launch(X86, name, InstanceLifecycle::InService, launched_ago)?;
    h.set_desired_capacity(X86, desired + 1);
    h.connect(X86, name)
}

// Legacy lifecycle scenarios intentionally compare decisions, not newly added diagnostics.
// The rate scenarios inspect ScaleDecision and CordonDecision directly.
#[derive(Debug, PartialEq, Eq)]
enum Change {
    Scale(usize, u32, u32, NeedCause),
    Withdraw(usize, u32, u32),
    Drain(usize, String, DrainReason),
    Cancel(usize, String, NeedCause),
    Terminate(usize, String, TerminationCause),
    Reinforce(usize, String),
}

fn changes(steps: &[PlanStep]) -> Vec<Change> {
    steps
        .iter()
        .map(|step| match &step.action {
            Action::ScaleOut { from, to, need, .. } => Change::Scale(step.fleet, *from, *to, *need),
            Action::Withdraw { from, to } => Change::Withdraw(step.fleet, *from, *to),
            Action::StartDrain { worker, reason, .. } => Change::Drain(
                step.fleet,
                worker.instance_name().unwrap_or(worker.as_str()).to_owned(),
                *reason,
            ),
            Action::CancelDrain(worker, need) => Change::Cancel(
                step.fleet,
                worker.instance_name().unwrap_or(worker.as_str()).to_owned(),
                *need,
            ),
            Action::Terminate {
                instance, cause, ..
            } => Change::Terminate(step.fleet, instance.as_str().to_owned(), *cause),
            Action::ReinforceDrain(worker) => Change::Reinforce(
                step.fleet,
                worker.instance_name().unwrap_or(worker.as_str()).to_owned(),
            ),
        })
        .collect()
}

fn assert_termination_mode(steps: &[PlanStep], name: &str, expected: TerminationMode) {
    assert!(
        steps.iter().any(|step| matches!(
            &step.action,
            Action::Terminate { instance, mode, .. }
                if instance.as_str() == name && *mode == expected
        )),
        "missing {expected:?} termination for {name}: {steps:?}"
    );
}

fn idle_world(minimum: u32, timeout: u64) -> TestResult<Workload> {
    let mut p = fleet("gang-x86", 6, 16)?;
    p.instances = InstanceBounds::try_new(minimum, p.instances.max())?;
    p.idle_timeout = secs(timeout);
    let mut scaling = settings(50)?;
    scaling.max_cordons = 0;
    Ok(Workload {
        h: Harness::with_scaling(vec![p], scaling),
    })
}

fn renewed(old: &WorkerState, ordinal: u32) -> TestResult<WorkerState> {
    let mut worker = old.clone();
    worker.id = WorkerId::try_new(format!(
        "{}6c5e4f3a-9b1d-4e2a-8f7c-{ordinal:012}",
        old.id
            .instance_name()
            .ok_or("missing physical id")?
            .to_owned()
            + "-"
    ))?;
    Ok(worker)
}

fn settle_low(w: &mut Workload) {
    w.pass();
    w.h.advance(CLIENT_REROUTE_BOUND);
    w.pass();
}

#[test]
fn minimum_scales_and_rescues_before_buying_with_audited_cause() -> TestResult {
    let mut w = idle_world(1, 600)?;
    assert_eq!(
        changes(&w.run()),
        vec![Change::Scale(X86, 0, 1, NeedCause::Minimum)]
    );
    let events = w.h.audit_events()?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].cause, Some(ScalingCause::Minimum));
    assert!(
        matches!(&events[0].kind, ScalingEventKind::ScaledOut { from: 0, to: 1, detail: Some(detail) } if !detail.is_empty())
    );
    w.h.launch(X86, "local-1", InstanceLifecycle::InService, secs(60))?;
    w.h.connect(X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    assert_eq!(
        changes(&w.run()),
        vec![Change::Cancel(X86, "local-1".into(), NeedCause::Minimum)]
    );
    assert_eq!(w.h.audit_events()?[0].cause, Some(ScalingCause::Minimum));
    assert!(w.h.snapshot.cordons.is_empty());
    Ok(())
}

#[test]
fn scheduled_minimum_opens_closes_and_observes_daylight_saving() -> TestResult {
    for start in [
        "2026-03-27T06:59:00Z",
        "2026-03-30T05:59:00Z",
        "2026-11-16T06:59:00Z",
    ] {
        let mut p = fleet("gang-x86", 6, 16)?;
        p.instances = InstanceBounds::try_new(0, p.instances.max())?.with_schedule(vec![
            scheduled_minimum(1, &Weekday::ALL[..5], "08:00", "19:00", "Europe/Paris")?,
        ])?;
        let mut h = Harness::with_scaling(vec![p], settings(50)?);
        h.clock = gang_clock::TestClock::new(Timestamp::from_rfc3339(start)?);
        assert!(h.reconcile().is_empty());
        h.advance(secs(60));
        assert_eq!(purchases(&h.run()), vec![(X86, 0, 1, NeedCause::Minimum)]);
        h.launch(X86, "local-1", InstanceLifecycle::InService, secs(60))?;
        h.connect(X86, "local-1")?;
        h.reconcile();
        h.advance(secs(11 * 3600 - 1));
        assert!(h.reconcile().is_empty());
        h.advance(secs(1));
        assert_eq!(
            changes(&h.reconcile()),
            vec![Change::Drain(
                X86,
                "local-1".into(),
                DrainReason::IdleTimeout
            )]
        );
    }
    Ok(())
}

#[test]
fn demand_equal_to_minimum_has_demand_cause_and_maximum_is_respected() -> TestResult {
    let mut w = idle_world(1, 600)?;
    w.queue(Some(source(1)?), X86, 1, None)?;
    assert_eq!(purchases(&w.pass()), vec![(X86, 0, 1, NeedCause::Demand)]);
    let mut bounded = Workload::new(4, 16, 50)?;
    bounded.queue(Some(source(1)?), X86, 2000, Some(secs(3600)))?;
    assert_eq!(
        purchases(&bounded.pass()),
        vec![(X86, 0, 4, NeedCause::Demand)]
    );
    Ok(())
}

#[test]
fn idle_timeout_drain_terminates_once_and_records_original_causes() -> TestResult {
    let mut w = idle_world(0, 600)?;
    w.h.settle_term()?;
    w.serve(X86, "local-1")?;
    assert!(w.run().is_empty());
    w.h.advance(secs(599));
    assert!(w.run().is_empty());
    w.h.advance(secs(1));
    assert_eq!(
        changes(&w.run()),
        vec![Change::Drain(
            X86,
            "local-1".into(),
            DrainReason::IdleTimeout
        )]
    );
    assert_eq!(
        w.h.audit_events()?[0].cause,
        Some(ScalingCause::IdleTimeout)
    );
    let retirement = w.run();
    assert_termination_mode(&retirement, "local-1", TerminationMode::ScaleIn);
    assert_eq!(
        changes(&retirement),
        vec![Change::Terminate(
            X86,
            "local-1".into(),
            TerminationCause::DrainFinished
        )]
    );
    assert_eq!(
        w.h.audit_events()?[0].cause,
        Some(ScalingCause::DrainFinished)
    );
    assert_eq!(w.h.supply(X86)?.desired, 0);
    assert!(w.run().is_empty());
    Ok(())
}

#[test]
fn activity_disconnect_and_new_term_restart_idle_timer() -> TestResult {
    for reset in ["busy", "disconnect", "term"] {
        let mut w = idle_world(0, 600)?;
        w.h.settle_term()?;
        w.serve(X86, "local-1")?;
        w.pass();
        w.h.advance(secs(599));
        match reset {
            "busy" => {
                w.running("local-1", 1, None, Duration::ZERO, None)?;
                w.pass();
                w.running("local-1", 0, None, Duration::ZERO, None)?;
            }
            "disconnect" => {
                w.h.disconnect("local-1")?;
                w.pass();
                w.h.connect(X86, "local-1")?;
            }
            _ => w.h.restart(),
        }
        assert!(w.pass().is_empty());
        w.h.advance(secs(599));
        assert!(w.pass().is_empty());
        w.h.advance(secs(1));
        assert!(w.pass().iter().any(|step| matches!(
            step.action,
            Action::StartDrain {
                reason: DrainReason::IdleTimeout,
                ..
            }
        )));
    }
    Ok(())
}

#[test]
fn term_settle_protects_previous_leaders_unseen_demand() -> TestResult {
    let mut w = idle_world(1, 15)?;
    w.serve(X86, "local-1")?;
    w.h.set_desired_capacity(X86, 2);
    assert!(w.run().is_empty());
    w.h.advance(CLIENT_REROUTE_BOUND.saturating_sub(Duration::from_millis(1)));
    assert!(w.run().is_empty());
    w.queue(Some(source(1)?), X86, 32, None)?;
    assert!(
        w.run()
            .iter()
            .all(|step| !matches!(step.action, Action::StartDrain { .. }))
    );
    assert_eq!(w.h.supply(X86)?.desired, 2);
    Ok(())
}

#[test]
fn busy_owned_drain_finishes_or_times_out_and_term_replacement_keeps_intent() -> TestResult {
    for (finish, restart) in [(true, false), (false, false), (true, true), (false, true)] {
        let mut w = idle_world(0, 600)?;
        w.h.passes[X86].policy.max_drain_wait = secs(60);
        w.serve(X86, "local-1")?;
        // A sole busy cordon must be resumed by the waterline rule. Keep one
        // healthy instance so this case isolates completion/max-drain lifetime.
        w.serve(X86, "local-2")?;
        w.running(
            "local-1",
            2,
            Some(source(1)?),
            Duration::ZERO,
            Some(secs(600)),
        )?;
        w.h.set_draining("local-1", true)?;
        assert!(w.pass().is_empty());
        if restart {
            w.h.advance(secs(30));
            w.h.restart();
            w.pass();
        }
        w.h.advance(secs(59));
        assert!(w.pass().is_empty());
        if finish {
            w.running("local-1", 0, Some(source(1)?), Duration::ZERO, None)?;
        }
        w.h.advance(secs(1));
        let cause = if finish {
            TerminationCause::DrainFinished
        } else {
            TerminationCause::MaxDrainWait
        };
        assert_eq!(
            changes(&w.pass()),
            vec![Change::Terminate(X86, "local-1".into(), cause)]
        );
    }
    Ok(())
}

#[test]
fn orphan_grace_starts_on_observed_inservice_or_disconnect_not_launch() -> TestResult {
    for from_pending in [false, true] {
        let mut w = idle_world(0, 600)?;
        w.h.passes[X86].policy.boot_grace = secs(30);
        w.h.set_desired_capacity(X86, 1);
        if from_pending {
            w.h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(3600))?;
            w.pass();
            w.h.advance(secs(600));
            assert!(w.pass().is_empty());
            w.h.set_lifecycle(X86, "local-1", InstanceLifecycle::InService, secs(4200))?;
        } else {
            w.h.launch(X86, "local-1", InstanceLifecycle::InService, secs(3600))?;
            w.h.connect(X86, "local-1")?;
            w.pass();
            w.h.disconnect("local-1")?;
        }
        assert!(w.pass().is_empty());
        w.h.advance(secs(29));
        assert!(w.pass().is_empty());
        w.h.advance(secs(1));
        assert_eq!(
            changes(&w.run()),
            vec![Change::Terminate(
                X86,
                "local-1".into(),
                TerminationCause::Orphan
            )]
        );
        assert!(w.h.supply(X86)?.instances.is_empty());
    }
    Ok(())
}

#[test]
fn pending_and_terminating_members_are_not_orphans() -> TestResult {
    let mut w = idle_world(0, 600)?;
    w.h.set_desired_capacity(X86, 1);
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(7200))?;
    w.h.launch(X86, "local-2", InstanceLifecycle::Terminating, secs(7200))?;
    for _ in 0..5 {
        assert!(w.pass().is_empty());
        w.h.advance(secs(600));
    }
    Ok(())
}

#[test]
fn ordered_termination_not_repeated_recounted_or_kept_after_forget() -> TestResult {
    let mut w = idle_world(0, 600)?;
    w.serve(X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    let expected = vec![Change::Terminate(
        X86,
        "local-1".into(),
        TerminationCause::DrainFinished,
    )];
    assert_eq!(changes(&w.pass()), expected);
    assert!(
        w.pass().is_empty(),
        "eventually consistent census still lists the ordered host"
    );
    w.h.reconciler
        .forget_order(X86, &InstanceId::try_new("local-1")?);
    assert_eq!(changes(&w.pass()), expected);
    w.h.advance(secs(600));
    assert_eq!(
        changes(&w.pass()),
        expected,
        "a lost order expires at boot_grace"
    );
    Ok(())
}

#[test]
fn failed_unborn_withdraws_cancelled_burst_with_and_without_serving_worker() -> TestResult {
    for serving in [0, 1] {
        let mut w = idle_world(0, 600)?;
        if serving == 1 {
            w.serve(X86, "local-1")?;
        }
        w.h.set_desired_capacity(X86, 6);
        w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
            reason: "Spot capacity".into(),
        });
        assert_eq!(changes(&w.pass()), vec![Change::Withdraw(X86, 6, serving)]);
    }
    Ok(())
}

#[test]
fn failed_unborn_surplus_is_withdrawn_even_when_usable_is_below_needed() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        16,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(30)),
    )?;
    w.h.set_desired_capacity(X86, 6);
    w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "Spot capacity".into(),
    });
    // Forty-eight CPU reservations require three 16-vCPU workers; failed unborn hosts
    // provide no usable capacity or booting credit.
    w.queue(Some(source(1)?), X86, 32, Some(secs(330)))?;
    assert_eq!(w.h.supply(X86)?.desired, 6);
    assert_eq!(changes(&w.pass()), vec![Change::Withdraw(X86, 6, 3)]);
    assert_eq!(w.h.outlook(X86)?.needed, 3);
    Ok(())
}

#[test]
fn spot_abandoned_withdrawal_precedes_exact_twin_raise() -> TestResult {
    let mut w = Workload {
        h: Harness::with_scaling(
            vec![fleet("gang-spot", 2, 1)?, fleet("gang-twin", 4, 1)?],
            settings(50)?,
        ),
    };
    w.learn_profile(source(1)?, secs(1800))?;
    w.h.supply_mut(0)?.abandoned = 3;
    w.h.queue_candidates(FleetSet::prefix(2)?, 3);
    for row in &mut w.h.snapshot.queued {
        row.resources.cpu_millicores = 1000;
        row.source = Some(source(1)?);
        row.expected = Some(secs(3600));
    }
    assert_eq!(
        changes(&w.pass()),
        vec![
            Change::Withdraw(0, 3, 0),
            Change::Scale(0, 0, 2, NeedCause::Demand),
            Change::Scale(1, 0, 1, NeedCause::Demand)
        ]
    );
    Ok(())
}

#[test]
fn failed_launch_request_is_not_repeated_or_abandoned_by_a_timer() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 1, None)?;
    assert_eq!(purchases(&w.run()), vec![(X86, 0, 1, NeedCause::Demand)]);
    w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".into(),
    });
    for _ in 0..30 {
        w.h.advance(secs(86400));
        assert!(w.pass().is_empty());
        assert_eq!(w.h.outlook(X86)?.needed, 1);
    }
    Ok(())
}

#[test]
fn busy_drains_rescue_before_purchase_and_cancel_suppresses_withdraw() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    for (name, running) in [("local-1", 1), ("local-2", 3)] {
        w.serve(X86, name)?;
        w.running(
            name,
            running,
            Some(source(1)?),
            Duration::ZERO,
            Some(secs(600)),
        )?;
        w.h.set_draining(name, true)?;
    }
    w.h.set_desired_capacity(X86, 4);
    w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".into(),
    });
    w.queue(Some(source(1)?), X86, 48, None)?;
    let decisions = changes(&w.run());
    assert_eq!(
        decisions.first(),
        Some(&Change::Cancel(X86, "local-2".into(), NeedCause::Demand))
    );
    assert!(decisions.contains(&Change::Cancel(X86, "local-1".into(), NeedCause::Demand)));
    assert!(
        !decisions
            .iter()
            .any(|change| matches!(change, Change::Withdraw(..) | Change::Drain(..)))
    );
    Ok(())
}

#[test]
fn empty_drain_is_rescued_for_ordinary_demand_not_waterline_alone() -> TestResult {
    let mut w = idle_world(0, 600)?;
    w.serve(X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    w.queue(Some(source(1)?), X86, 1, None)?;
    assert_eq!(
        changes(&w.run()),
        vec![Change::Cancel(X86, "local-1".into(), NeedCause::Demand)]
    );
    w.h.snapshot.queued.clear();
    w.h.set_draining("local-1", true)?;
    assert_eq!(
        changes(&w.pass()),
        vec![Change::Terminate(
            X86,
            "local-1".into(),
            TerminationCause::DrainFinished
        )]
    );
    Ok(())
}

#[test]
fn zero_horizon_fractional_demand_reopens_busy_drain_above_waterline() -> TestResult {
    let mut w = idle_world(0, 0)?;
    w.h.passes[X86].policy.vcpus_per_instance = NonZeroU32::new(10).ok_or("zero vcpus")?;
    for name in ["local-serving", "local-draining"] {
        serve(&mut w.h, name, Duration::ZERO)?;
    }
    w.running("local-draining", 1, None, Duration::ZERO, Some(secs(30)))?;
    w.h.set_draining("local-draining", true)?;
    w.h.queue_sized(
        X86,
        1,
        gang_types::ActionResources {
            cpu_millicores: 8000,
            ..gang_types::ActionResources::default()
        },
    )?;
    let steps = w.pass();
    assert_eq!(w.h.outlook(X86)?.readiness, Some(Duration::ZERO));
    assert_eq!(w.h.outlook(X86)?.needed, 1);
    assert_eq!(
        changes(&steps),
        vec![Change::Cancel(
            X86,
            "local-draining".into(),
            NeedCause::Demand
        )]
    );
    Ok(())
}

#[test]
fn projected_wait_resumes_only_owned_unordered_nonterminating_workers() -> TestResult {
    for excluded in ["unowned", "ordered", "terminating"] {
        let mut w = idle_world(0, 600)?;
        w.serve(X86, "local-excluded")?;
        match excluded {
            "ordered" => {
                w.h.set_draining("local-excluded", true)?;
                assert!(changes(&w.pass()).contains(&Change::Terminate(
                    X86,
                    "local-excluded".into(),
                    TerminationCause::DrainFinished,
                )));
            }
            "terminating" => {
                w.h.supply_mut(X86)?.instances[0].lifecycle = InstanceLifecycle::Terminating;
            }
            _ => w.h.snapshot.workers[0].draining = true,
        }
        w.serve(X86, "local-owned")?;
        w.running("local-owned", 1, None, Duration::ZERO, Some(secs(30)))?;
        let memory = w.h.passes[X86].policy.memory_per_instance.get();
        w.h.snapshot.executing[0].resources.memory = memory;
        w.h.set_draining("local-owned", true)?;
        w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
            reason: "capacity".into(),
        });
        w.h.queue_sized(
            X86,
            1,
            gang_types::ActionResources {
                memory,
                ..gang_types::ActionResources::default()
            },
        )?;
        let steps = w.pass();
        let resumed: Vec<_> = steps
            .iter()
            .filter_map(|step| match &step.action {
                Action::CancelDrain(worker, _) => worker.instance_name(),
                _ => None,
            })
            .collect();
        assert_eq!(resumed, vec!["local-owned"], "{excluded}");
        assert_eq!(
            w.h.outlook(X86)?.expected_wait,
            Some(secs(30)),
            "{excluded}"
        );
    }
    Ok(())
}

#[test]
fn projected_wait_and_rescue_share_busiest_release_and_worker_id_order() -> TestResult {
    for priority in ["running", "release-first", "worker-id"] {
        let mut w = idle_world(0, 600)?;
        for name in ["local-2", "local-1"] {
            w.serve(X86, name)?;
        }
        w.running("local-2", 1, None, Duration::ZERO, Some(secs(5)))?;
        let busiest = if priority == "running" { 2 } else { 1 };
        w.running("local-1", busiest, None, Duration::ZERO, Some(secs(30)))?;
        let memory = w.h.passes[X86].policy.memory_per_instance.get();
        for execution in &mut w.h.snapshot.executing {
            execution.resources.memory = gang_types::Bytes::new(memory.get() / 10);
        }
        if priority != "worker-id" {
            // Busiest wins even when it is release-first; release-first breaks equal loads.
            let release = usize::from(priority == "running");
            w.h.supply_mut(X86)?.instances[release].release_first = true;
        }
        for name in ["local-2", "local-1"] {
            w.h.set_draining(name, true)?;
        }
        w.h.queue_sized(
            X86,
            1,
            gang_types::ActionResources {
                memory,
                ..gang_types::ActionResources::default()
            },
        )?;
        let steps = w.pass();
        assert_eq!(
            changes(&steps),
            vec![Change::Cancel(X86, "local-1".into(), NeedCause::Demand)],
            "{priority}",
        );
        assert_eq!(
            w.h.outlook(X86)?.expected_wait,
            Some(secs(30)),
            "{priority}"
        );
    }
    Ok(())
}

#[test]
fn resume_order_prefers_non_release_first_then_running_then_worker_id() -> TestResult {
    let mut w = idle_world(1, 600)?;
    for name in ["local-2", "local-1"] {
        w.serve(X86, name)?;
        w.h.set_draining(name, true)?;
    }
    w.h.supply_mut(X86)?.instances[1].release_first = true;
    let steps = changes(&w.run());
    assert!(steps.contains(&Change::Cancel(X86, "local-2".into(), NeedCause::Minimum)));
    assert!(!steps.contains(&Change::Cancel(X86, "local-1".into(), NeedCause::Minimum)));
    Ok(())
}

#[test]
fn idle_drain_order_release_first_then_longest_idle_then_worker_id() -> TestResult {
    let mut w = idle_world(1, 600)?;
    w.h.settle_term()?;
    w.serve(X86, "local-3")?;
    w.pass();
    w.h.advance(secs(5));
    w.serve(X86, "local-2")?;
    w.serve(X86, "local-1")?;
    w.h.supply_mut(X86)?.instances[1].release_first = true;
    w.pass();
    w.h.advance(secs(600));
    assert_eq!(
        changes(&w.run()),
        vec![
            Change::Drain(X86, "local-2".into(), DrainReason::IdleTimeout),
            Change::Drain(X86, "local-3".into(), DrainReason::IdleTimeout)
        ]
    );
    Ok(())
}

#[test]
fn release_first_busy_or_booting_member_holds_back_idle_spot() -> TestResult {
    for booting in [false, true] {
        let mut w = idle_world(0, 600)?;
        w.h.settle_term()?;
        w.serve(X86, "local-1")?;
        if booting {
            w.h.launch_release_first(X86, "local-2", InstanceLifecycle::Pending, Duration::ZERO)?;
            w.h.set_desired_capacity(X86, 2);
        } else {
            w.serve(X86, "local-2")?;
            w.h.supply_mut(X86)?.instances[1].release_first = true;
            w.running("local-2", 1, None, Duration::ZERO, Some(secs(3600)))?;
        }
        w.queue(None, X86, 1, None)?;
        w.pass();
        w.h.advance(secs(600));
        assert!(
            !w.pass()
                .iter()
                .any(|step| matches!(step.action, Action::StartDrain { .. }))
        );
    }
    Ok(())
}

fn spread() -> TestResult<Workload> {
    let mut w = Workload::new(6, 16, 50)?;
    w.h.passes[X86].policy.idle_timeout = secs(3600);
    for name in ["local-1", "local-2", "local-3", "local-4"] {
        w.serve(X86, name)?;
        w.running(name, 2, Some(source(1)?), Duration::ZERO, Some(secs(600)))?;
    }
    Ok(w)
}

#[test]
fn n_minus_one_dwell_cordons_one_busy_physical_instance() -> TestResult {
    let mut w = spread()?;
    settle_low(&mut w);
    w.h.advance(secs(299));
    assert!(w.pass().is_empty());
    w.h.advance(secs(1));
    let steps = w.run();
    assert_eq!(
        changes(&steps),
        vec![Change::Drain(
            X86,
            "local-1".into(),
            DrainReason::Utilization
        )]
    );
    let Action::StartDrain {
        summary: Some(summary),
        ..
    } = steps[0].action
    else {
        return Err("missing cordon summary".into());
    };
    assert_eq!(summary.dwell, secs(300));
    assert_eq!(summary.serving_instances, 4);
    assert_eq!(summary.waterline_percent, 70);
    assert_eq!(
        w.h.audit_events()?[0].cause,
        Some(ScalingCause::Utilization)
    );
    w.h.advance(secs(300));
    assert!(!w.pass().iter().any(|step| matches!(
        step.action,
        Action::StartDrain {
            reason: DrainReason::Utilization,
            ..
        }
    )));
    Ok(())
}

#[test]
fn cordon_victim_order_empty_time_release_first_running_then_id() -> TestResult {
    for tie in ["empty", "release", "running", "id"] {
        let mut w = spread()?;
        for name in ["local-1", "local-2", "local-3", "local-4"] {
            w.running(name, 2, Some(source(1)?), Duration::ZERO, Some(secs(1200)))?;
        }
        w.running(
            "local-2",
            if tie == "running" { 1 } else { 2 },
            Some(source(1)?),
            Duration::ZERO,
            Some(secs(if tie == "empty" { 900 } else { 1200 })),
        )?;
        if tie == "release" {
            w.h.supply_mut(X86)?.instances[1].release_first = true;
        }
        settle_low(&mut w);
        w.h.advance(secs(300));
        let victim = if tie == "id" { "local-1" } else { "local-2" };
        assert_eq!(
            changes(&w.pass()),
            vec![Change::Drain(X86, victim.into(), DrainReason::Utilization)],
            "tie={tie}"
        );
    }
    Ok(())
}

#[test]
fn skipped_pass_resets_dwell_and_keeps_existing_cordons() -> TestResult {
    let mut w = spread()?;
    settle_low(&mut w);
    w.h.advance(secs(299));
    w.pass();
    w.h.reconciler.interrupt();
    w.h.advance(secs(100));
    assert!(w.pass().is_empty());
    w.h.advance(secs(299));
    assert!(w.pass().is_empty());
    w.h.advance(secs(1));
    assert_eq!(
        changes(&w.run()),
        vec![Change::Drain(
            X86,
            "local-1".into(),
            DrainReason::Utilization
        )]
    );
    w.h.reconciler.interrupt();
    assert_eq!(w.h.snapshot.cordons.len(), 1);
    Ok(())
}

#[test]
fn max_cordons_zero_soak_keeps_idle_timeout_drains_enabled() -> TestResult {
    let mut w = spread()?;
    let mut scaling = *w.h.scaling;
    scaling.max_cordons = 0;
    let policies = w.h.passes.iter().map(|pass| pass.policy.clone()).collect();
    let mut h = Harness::with_scaling(policies, scaling);
    h.snapshot = w.h.snapshot;
    for (to, from) in h.passes.iter_mut().zip(w.h.passes) {
        to.supply = from.supply;
    }
    w.h = h;
    settle_low(&mut w);
    w.h.advance(secs(300));
    assert!(w.pass().is_empty());
    for name in ["local-1", "local-2", "local-3", "local-4"] {
        w.running(name, 0, None, Duration::ZERO, None)?;
    }
    w.pass();
    w.h.advance(secs(3600));
    let steps = w.pass();
    assert!(steps.iter().any(|step| matches!(
        step.action,
        Action::StartDrain {
            reason: DrainReason::IdleTimeout,
            ..
        }
    )));
    assert!(steps.iter().all(|step| !matches!(
        step.action,
        Action::StartDrain {
            reason: DrainReason::Utilization,
            ..
        }
    )));
    Ok(())
}

#[test]
fn n_one_to_zero_after_observed_quiet_before_idle_timeout() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.h.passes[X86].policy.idle_timeout = secs(3600);
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        1,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(10)),
    )?;
    w.pass();
    w.h.advance(secs(10));
    w.running("local-1", 0, Some(source(1)?), Duration::ZERO, None)?;
    w.complete(
        Some(source(1)?),
        X86,
        1,
        Some(secs(10)),
        gang_types::ActionResources::default(),
    );
    w.pass();
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(secs(60));
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    // Settle must have elapsed before N−1 begins proving its dwell.
    w.h.advance(CLIENT_REROUTE_BOUND);
    w.pass();
    w.h.advance(secs(300));
    assert_eq!(
        changes(&w.run()),
        vec![Change::Drain(
            X86,
            "local-1".into(),
            DrainReason::Utilization
        )]
    );
    assert!(w.h.clock.monotonic().elapsed() < secs(3600));
    Ok(())
}

#[test]
fn returning_busy_load_uncordons_before_buying_and_resets_dwell() -> TestResult {
    let mut w = spread()?;
    settle_low(&mut w);
    w.h.advance(secs(300));
    w.run();
    for name in ["local-1", "local-2", "local-3", "local-4"] {
        w.running(name, 12, Some(source(1)?), Duration::ZERO, Some(secs(600)))?;
    }
    let steps = w.run();
    assert!(
        steps
            .iter()
            .any(|step| matches!(step.action, Action::CancelDrain(..)))
    );
    assert!(purchases(&steps).is_empty());
    assert!(steps.iter().all(|step| !matches!(
        step.action,
        Action::Withdraw { .. } | Action::StartDrain { .. }
    )));
    for name in ["local-1", "local-2", "local-3", "local-4"] {
        w.running(name, 2, Some(source(1)?), Duration::ZERO, Some(secs(600)))?;
    }
    assert!(
        w.pass()
            .iter()
            .all(|step| !matches!(step.action, Action::StartDrain { .. }))
    );
    Ok(())
}

#[test]
fn all_generation_cordon_and_partial_reinforcement_are_physical_and_silent() -> TestResult {
    let mut w = spread()?;
    let mut next = renewed(&w.h.snapshot.workers[0], 2)?;
    next.running_actions = 0;
    w.h.snapshot.workers.push(next);
    settle_low(&mut w);
    w.h.advance(secs(300));
    w.run();
    let id = InstanceId::try_new("local-1")?;
    assert!(
        w.h.snapshot
            .workers
            .iter()
            .filter(|worker| id.hosts(&worker.id))
            .all(|worker| worker.draining)
    );
    let connected = worker_id("local-1")?;
    let index =
        w.h.snapshot
            .workers
            .iter()
            .position(|worker| worker.id == connected)
            .ok_or("missing worker")?;
    w.h.snapshot.workers[index].draining = false;
    for _ in 0..5 {
        let steps = w.pass();
        assert!(
            steps
                .iter()
                .any(|step| matches!(step.action, Action::ReinforceDrain(_)))
        );
        assert!(steps.iter().all(|step| !matches!(
            step.action,
            Action::Terminate { .. } | Action::StartDrain { .. }
        )));
        assert!(
            w.h.audit_events()?.is_empty(),
            "repair writes zero audit rows"
        );
    }
    w.h.apply();
    assert!(
        w.pass()
            .iter()
            .all(|step| !matches!(step.action, Action::ReinforceDrain(_)))
    );
    Ok(())
}

#[test]
fn unowned_self_drain_refresh_overlap_and_gap_never_resume_terminate_or_buy() -> TestResult {
    let mut w = idle_world(1, 600)?;
    w.serve(X86, "local-1")?;
    w.pass();
    w.h.snapshot.workers[0].draining = true;
    assert!(w.h.snapshot.cordons.is_empty());
    assert!(w.pass().is_empty(), "all-draining refresh gap is transient");
    w.h.advance(secs(30));
    let mut next = renewed(&w.h.snapshot.workers[0], 2)?;
    next.draining = false;
    next.running_actions = 1;
    w.h.snapshot.workers.push(next);
    assert!(
        w.pass().is_empty(),
        "overlapping generations are one physical instance"
    );
    w.h.snapshot.workers.remove(0);
    assert!(w.pass().is_empty());
    Ok(())
}

#[test]
fn unowned_drain_after_grace_terminates_and_replaces_exactly_once() -> TestResult {
    let mut w = idle_world(2, 600)?;
    for name in ["local-1", "local-2"] {
        w.serve(X86, name)?;
    }
    w.h.snapshot.workers[1].draining = true;
    assert!(w.pass().is_empty());
    w.h.advance(secs(600));
    assert_eq!(
        changes(&w.pass()),
        vec![
            Change::Terminate(X86, "local-2".into(), TerminationCause::DrainFinished),
            Change::Scale(X86, 1, 2, NeedCause::Minimum)
        ]
    );
    // Provider desired reflects the commands but the census still contains the ordered member.
    w.h.supply_mut(X86)?.desired = 2;
    assert!(w.pass().is_empty());
    Ok(())
}

#[test]
fn needed_at_max_with_retained_busy_drain_never_requests_over_maximum() -> TestResult {
    let mut w = idle_world(6, 600)?;
    for i in 1..=6 {
        w.serve(X86, &format!("local-{i}"))?;
    }
    w.running("local-1", 1, None, Duration::ZERO, Some(secs(3600)))?;
    // A self-drain remains retained after grace, not rescuable controller capacity.
    w.h.snapshot.workers[0].draining = true;
    w.pass();
    w.h.advance(secs(600));
    assert!(purchases(&w.pass()).is_empty());
    assert_eq!(w.h.supply(X86)?.desired, 6);
    Ok(())
}

#[test]
fn transient_neighbour_never_justifies_draining_healthy_minimum_or_rescuing_unrelated_cordon()
-> TestResult {
    let mut w = idle_world(1, 600)?;
    w.h.settle_term()?;
    w.serve(X86, "local-1")?;
    w.serve(X86, "local-2")?;
    w.h.snapshot.workers[1].draining = true;
    w.pass();
    w.h.advance(secs(599));
    assert!(
        w.pass().is_empty(),
        "transient is need coverage, not surplus coverage"
    );
    let mut other = idle_world(2, 600)?;
    for name in ["local-1", "local-2", "local-3"] {
        other.serve(X86, name)?;
    }
    other.h.snapshot.workers[1].draining = true;
    other.running("local-3", 1, None, Duration::ZERO, Some(secs(3600)))?;
    other.h.set_draining("local-3", true)?;
    assert!(
        !other
            .pass()
            .iter()
            .any(|step| matches!(step.action, Action::CancelDrain(..)))
    );
    Ok(())
}

#[test]
fn zero_connection_owned_cordon_is_not_capacity_and_rescues_only_after_reconnect() -> TestResult {
    let mut w = idle_world(1, 600)?;
    w.serve(X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    w.h.disconnect("local-1")?;
    assert!(w.pass().is_empty());
    assert_eq!(w.h.outlook(X86)?.utilization, None);
    w.h.connect(X86, "local-1")?;
    assert!(w.h.snapshot.workers[0].draining);
    assert_eq!(
        changes(&w.run()),
        vec![Change::Cancel(X86, "local-1".into(), NeedCause::Minimum)]
    );
    assert_eq!(w.h.audit_events()?.len(), 1);
    Ok(())
}

#[test]
fn absent_and_ordered_cordon_registry_entries_do_not_consume_live_budget() -> TestResult {
    let mut w = spread()?;
    w.h.snapshot.cordons.push(gang_spi::InstanceCordon {
        fleet: X86,
        instance: std::sync::Arc::new(InstanceId::try_new("local-absent")?),
        replacing: false,
    });
    settle_low(&mut w);
    w.h.advance(secs(300));
    assert!(w.pass().iter().any(|step| matches!(
        step.action,
        Action::StartDrain {
            reason: DrainReason::Utilization,
            ..
        }
    )));
    Ok(())
}

#[test]
fn distinct_fleets_candidate_attribution_and_unknown_workers_stay_isolated() -> TestResult {
    let mut w = Workload {
        h: Harness::with_fleets(vec![policy()?, policy_named("gang-arm", Isa::Aarch64)?]),
    };
    w.serve(ARM, "local-2")?;
    w.running("local-2", 4, None, Duration::ZERO, None)?;
    w.h.connect_as(gang_types::FleetName::try_new("gang-unknown")?, "local-9")?;
    w.h.set_running("local-9", 4)?;
    w.queue(None, X86, 1, None)?;
    assert_eq!(purchases(&w.run()), vec![(X86, 0, 1, NeedCause::Demand)]);
    assert_eq!(w.h.supply(ARM)?.desired, 1);
    w.h.snapshot.queued.clear();
    w.h.queue_candidates(FleetSet::EMPTY, 3);
    w.h.queue_candidates(FleetSet::try_from_indices([5])?, 3);
    assert!(purchases(&w.pass()).is_empty());
    Ok(())
}

#[test]
fn removing_workers_remaps_executing_without_losing_sources_or_resources() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    for name in ["local-1", "local-2", "local-3"] {
        w.serve(X86, name)?;
    }
    w.running("local-1", 1, Some(source(1)?), secs(1), Some(secs(3)))?;
    w.running("local-2", 1, Some(source(2)?), secs(2), Some(secs(5)))?;
    w.h.disconnect("local-1")?;
    assert_eq!(w.h.snapshot.executing.len(), 1);
    assert_eq!(w.h.snapshot.executing[0].worker, 0);
    assert_eq!(w.h.snapshot.executing[0].source, Some(source(2)?));
    w.h.plan.steps = vec![PlanStep {
        fleet: X86,
        action: Action::Terminate {
            instance: InstanceId::try_new("local-2")?,
            cause: TerminationCause::DrainFinished,
            mode: TerminationMode::ScaleIn,
        },
    }];
    w.h.apply();
    assert!(w.h.snapshot.executing.is_empty());
    Ok(())
}

#[test]
fn same_inputs_in_new_term_produce_same_decision() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 1, None)?;
    let first = w.pass();
    w.h.restart();
    assert_eq!(w.pass(), first);
    Ok(())
}

#[test]
fn ordered_live_cordon_does_not_consume_busy_cordon_budget() -> TestResult {
    let mut w = spread()?;
    w.serve(X86, "local-5")?;
    w.h.set_draining("local-5", true)?;
    assert!(
        w.pass()
            .iter()
            .any(|step| matches!(step.action, Action::Terminate { .. }))
    );
    // Successful provider command, eventually consistent listing retains local-5.
    w.h.supply_mut(X86)?.desired = 4;
    w.h.advance(CLIENT_REROUTE_BOUND);
    w.pass();
    w.h.advance(secs(300));
    assert!(w.pass().iter().any(|step| matches!(
        step.action,
        Action::StartDrain {
            reason: DrainReason::Utilization,
            ..
        }
    )));
    Ok(())
}

#[test]
fn pending_unborn_and_zero_connection_capacity_block_n_minus_one() -> TestResult {
    for class in ["pending", "unborn", "unconnected", "owned-empty"] {
        let mut w = spread()?;
        w.h.set_desired_capacity(X86, 5);
        if class != "unborn" {
            let lifecycle = if class == "pending" {
                InstanceLifecycle::Pending
            } else {
                InstanceLifecycle::InService
            };
            w.h.launch(X86, "local-5", lifecycle, Duration::ZERO)?;
        }
        if class == "owned-empty" {
            w.h.connect(X86, "local-5")?;
            w.h.set_draining("local-5", true)?;
            w.h.disconnect("local-5")?;
        }
        settle_low(&mut w);
        w.h.advance(secs(300));
        assert!(
            w.pass().iter().all(|step| !matches!(
                step.action,
                Action::StartDrain {
                    reason: DrainReason::Utilization,
                    ..
                }
            )),
            "unreliable class {class}"
        );
    }
    Ok(())
}

#[test]
fn scale_out_resets_utilization_dwell() -> TestResult {
    let mut w = spread()?;
    settle_low(&mut w);
    w.h.advance(secs(299));
    w.pass();
    w.queue(Some(source(2)?), X86, 2000, Some(secs(3600)))?;
    assert!(!purchases(&w.pass()).is_empty());
    w.h.snapshot.queued.clear();
    w.h.advance(secs(1));
    assert!(
        w.pass()
            .iter()
            .all(|step| !matches!(step.action, Action::StartDrain { .. }))
    );
    Ok(())
}

#[test]
fn fresh_engine_has_no_controller_intent_but_same_engine_model_replacement_does() -> TestResult {
    let mut w = idle_world(1, 600)?;
    w.serve(X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    w.h.restart();
    assert!(
        w.pass()
            .iter()
            .any(|step| matches!(step.action, Action::CancelDrain(..)))
    );
    w.h.snapshot.cordons.clear();
    w.h.restart();
    assert!(w.pass().iter().all(|step| !matches!(
        step.action,
        Action::CancelDrain(..) | Action::ReinforceDrain(_)
    )));
    Ok(())
}
