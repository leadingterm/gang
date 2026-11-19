use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Clock;
use gang_config::InstanceBounds;
use gang_reconciler::{Action, NeedCause, PlanStep};
use gang_spi::ExecutingDemand;
use gang_testkit::{TestResult, worker_id};
use gang_types::{FleetSet, InstanceLifecycle, Isa};

use crate::support::{Harness, X86, policy_named, x86_resources};

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn fleet(name: &str, max: u32, target: u32) -> TestResult<gang_config::FleetPolicy> {
    let mut p = policy_named(name, Isa::X86_64)?;
    p.instances = InstanceBounds::try_new(0, NonZeroU32::new(max).ok_or("zero maximum")?)?;
    p.target_actions_per_instance = NonZeroU32::new(target).ok_or("zero target")?;
    p.boot_grace = secs(150);
    Ok(p)
}

fn scale(fleet: usize, from: u32, to: u32, need: NeedCause) -> PlanStep {
    PlanStep {
        fleet,
        action: Action::ScaleOut { from, to, need },
    }
}

struct Workload {
    h: Harness,
}

impl Workload {
    fn new(h: Harness) -> Self {
        Self { h }
    }

    fn queue(&mut self, fleet: usize, count: usize, expected: Option<Duration>) -> TestResult {
        let start = self.h.snapshot.queued.len();
        self.h.queue(fleet, count)?;
        for queued in &mut self.h.snapshot.queued[start..] {
            queued.expected = expected;
        }
        Ok(())
    }

    fn candidates(&mut self, candidates: FleetSet, count: usize, expected: Option<Duration>) {
        let start = self.h.snapshot.queued.len();
        self.h.queue_candidates(candidates, count);
        for queued in &mut self.h.snapshot.queued[start..] {
            queued.expected = expected;
        }
    }

    fn running(
        &mut self,
        name: &str,
        count: u32,
        elapsed: Duration,
        expected: Option<Duration>,
    ) -> TestResult {
        self.h.set_running(name, count)?;
        let id = worker_id(name)?;
        let worker = self
            .h
            .snapshot
            .workers
            .iter()
            .position(|worker| worker.id == id)
            .ok_or("connected worker missing")?;
        let started = self.h.clock.wall() - elapsed;
        self.h
            .snapshot
            .executing
            .retain(|execution| execution.worker != worker);
        self.h
            .snapshot
            .executing
            .extend((0..count).map(|_| ExecutingDemand {
                worker,
                started,
                expected,
            }));
        Ok(())
    }

    fn run(&mut self) -> Vec<PlanStep> {
        self.h.run()
    }

    fn reconcile(&mut self) -> Vec<PlanStep> {
        self.h.reconcile()
    }
}

fn serve(h: &mut Harness, fleet: usize, name: &str) -> TestResult {
    let desired = h.supply(fleet)?.desired;
    h.launch(fleet, name, InstanceLifecycle::InService, secs(300))?;
    h.set_desired_capacity(fleet, desired + 1);
    h.connect(fleet, name)?;
    let worker = h
        .snapshot
        .workers
        .last_mut()
        .ok_or("missing connected worker")?;
    worker.max_inflight_actions = NonZeroU32::new(16);
    Ok(())
}

fn workload(max: u32, target: u32) -> TestResult<Workload> {
    Ok(Workload::new(Harness::with_policy(fleet(
        "gang-x86", max, target,
    )?)))
}

#[test]
fn event_burst_does_not_ratchet() -> TestResult {
    let mut w = workload(6, 16)?;
    w.queue(X86, 16, Some(secs(2)))?;
    let mut steps = w.run();
    assert_eq!(w.h.outlook(X86)?.expected_wait, Some(secs(150)));
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    w.h.advance(secs(8));
    w.queue(X86, 32, Some(secs(2)))?;
    steps.extend(w.run());
    w.h.advance(secs(1));
    w.queue(X86, 112, Some(secs(2)))?;
    steps.extend(w.run());
    assert_eq!(steps, vec![scale(X86, 0, 1, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn short_wave_absorbs_queued_action() -> TestResult {
    let mut w = workload(3, 16)?;
    for name in ["local-1", "local-2"] {
        serve(&mut w.h, X86, name)?;
        w.running(name, 16, Duration::ZERO, Some(secs(2)))?;
    }
    w.queue(X86, 1, Some(secs(1800)))?;
    assert_eq!(w.reconcile(), vec![]);
    assert_eq!(w.h.outlook(X86)?.expected_wait, Some(secs(2)));
    Ok(())
}

#[test]
fn starving_short_action_buys_by_wait() -> TestResult {
    let mut w = workload(3, 16)?;
    for name in ["local-1", "local-2"] {
        serve(&mut w.h, X86, name)?;
        w.running(name, 16, secs(600), Some(secs(1800)))?;
    }
    w.queue(X86, 1, Some(secs(2)))?;
    assert_eq!(w.reconcile(), vec![scale(X86, 2, 3, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn long_running_overrun_keeps_slot() -> TestResult {
    let mut w = workload(2, 1)?;
    serve(&mut w.h, X86, "local-1")?;
    w.running("local-1", 1, secs(300), Some(secs(2)))?;
    w.queue(X86, 1, Some(secs(2)))?;
    assert_eq!(w.reconcile(), vec![scale(X86, 1, 2, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn large_backlog_buys_to_bound() -> TestResult {
    let mut w = workload(20, 16)?;
    serve(&mut w.h, X86, "local-1")?;
    serve(&mut w.h, X86, "local-2")?;
    w.queue(X86, 2000, Some(secs(30)))?;
    assert_eq!(w.reconcile(), vec![scale(X86, 2, 20, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn stream_after_two_readiness() -> TestResult {
    let mut w = workload(13, 16)?;
    // A waiting queue without a serving worker must not start the stream clock.
    w.h.set_desired_capacity(X86, 3);
    w.queue(X86, 1, Some(secs(2)))?;
    w.run();
    w.h.advance(secs(300));
    for name in ["local-1", "local-2", "local-3"] {
        w.h.launch(X86, name, InstanceLifecycle::InService, secs(301))?;
        w.h.connect(X86, name)?;
        w.h.snapshot
            .workers
            .last_mut()
            .ok_or("missing worker")?
            .max_inflight_actions = NonZeroU32::new(16);
    }
    w.h.set_desired_capacity(X86, 3);
    w.queue(X86, 199, Some(secs(2)))?;
    w.run();
    w.h.advance(secs(299));
    let before = w.run();
    w.h.advance(secs(1));
    let at = w.run();
    assert_eq!(before, vec![]);
    assert_eq!(at, vec![scale(X86, 3, 13, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn unlisted_connected_worker_starts_stream_clock() -> TestResult {
    let mut w = workload(13, 16)?;
    w.h.set_desired_capacity(X86, 1);
    w.h.connect(X86, "local-1")?;
    w.queue(X86, 200, Some(secs(2)))?;
    assert_eq!(w.reconcile(), vec![]);
    w.h.advance(secs(299));
    assert_eq!(w.reconcile(), vec![]);
    w.h.advance(secs(1));
    assert_eq!(w.reconcile(), vec![scale(X86, 1, 13, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn unknown_durations_size_as_today() -> TestResult {
    let mut cold = workload(6, 16)?;
    cold.queue(X86, 160, None)?;
    assert_eq!(cold.reconcile(), vec![scale(X86, 0, 6, NeedCause::Demand)]);
    let mut idle = workload(6, 16)?;
    serve(&mut idle.h, X86, "local-1")?;
    idle.queue(X86, 17, None)?;
    assert_eq!(idle.reconcile(), vec![scale(X86, 1, 2, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn non_monotone_completion_gain() -> TestResult {
    let mut w = workload(4, 1)?;
    w.queue(X86, 4, Some(secs(150)))?;
    assert_eq!(w.reconcile(), vec![scale(X86, 0, 4, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn exhaustive_search_crosses_payback_gap() -> TestResult {
    let mut w = Workload::new(Harness::with_fleets(vec![
        fleet("gang-first", 3, 1)?,
        fleet("gang-second", 1, 1)?,
    ]));
    serve(&mut w.h, 0, "local-1")?;
    serve(&mut w.h, 1, "local-2")?;
    w.running("local-1", 1, secs(100), Some(secs(400)))?;
    w.running("local-2", 1, secs(100), Some(secs(300)))?;
    w.queue(0, 1, Some(secs(100)))?;
    w.candidates(FleetSet::try_from_indices([0, 1])?, 1, Some(secs(200)));
    w.queue(1, 1, Some(secs(1000)))?;
    assert_eq!(w.reconcile(), vec![scale(0, 1, 3, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn unknown_running_never_exceeds_today() -> TestResult {
    let mut w = workload(6, 16)?;
    serve(&mut w.h, X86, "local-1")?;
    w.running("local-1", 16, secs(100), None)?;
    w.queue(X86, 16, None)?;
    assert_eq!(w.reconcile(), vec![]);
    Ok(())
}

#[test]
fn floor_respects_earlier_fleet_pending() -> TestResult {
    let candidates = FleetSet::try_from_indices([0, 1])?;
    let make = || -> TestResult<Workload> {
        Ok(Workload::new(Harness::with_fleets(vec![
            fleet("gang-first", 1, 1)?,
            fleet("gang-second", 2, 1)?,
        ])))
    };
    let mut pending = make()?;
    pending.h.set_desired_capacity(0, 1);
    pending
        .h
        .launch(0, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    pending.candidates(candidates, 2, Some(secs(2)));
    assert!(pending.reconcile().iter().all(|s| s.fleet != 1));
    let mut failed = make()?;
    failed.h.set_desired_capacity(0, 1);
    failed.h.supply_mut(0)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "failed".to_owned(),
    });
    failed.candidates(candidates, 2, Some(secs(2)));
    assert!(
        failed
            .reconcile()
            .contains(&scale(1, 0, 1, NeedCause::Demand))
    );
    Ok(())
}

#[test]
fn draining_only_fleet_resumes_not_boots() -> TestResult {
    let mut w = workload(3, 16)?;
    serve(&mut w.h, X86, "local-1")?;
    w.h.set_draining("local-1", true)?;
    w.queue(X86, 1, Some(secs(2)))?;
    assert_eq!(
        w.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::CancelDrain(worker_id("local-1")?, NeedCause::Demand)
        }]
    );
    Ok(())
}

#[test]
fn readiness_sample_excludes_reconnects() -> TestResult {
    let mut w = workload(3, 1)?;
    serve(&mut w.h, X86, "local-1")?;
    w.h.disconnect("local-1")?;
    w.run();
    assert_eq!(w.h.outlook(X86)?.readiness, None);
    w.h.connect(X86, "local-1")?;
    w.running("local-1", 1, secs(100), Some(secs(2)))?;
    w.queue(X86, 1, Some(secs(2)))?;
    // With the 150s prior, the slot frees before a new instance could join.
    assert_eq!(w.reconcile(), vec![]);
    assert_eq!(w.h.outlook(X86)?.readiness, None);

    let mut observed = workload(3, 1)?;
    observed.run();
    assert_eq!(observed.h.outlook(X86)?.readiness, None);
    observed.h.advance(secs(1));
    observed.h.set_desired_capacity(X86, 1);
    observed
        .h
        .launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    observed.run();
    observed.h.advance(secs(10));
    observed
        .h
        .set_lifecycle(X86, "local-1", InstanceLifecycle::InService, secs(10))?;
    observed.h.connect(X86, "local-1")?;
    observed.run();
    assert_eq!(observed.h.outlook(X86)?.readiness, Some(secs(10)));
    observed.running("local-1", 1, secs(100), Some(secs(2)))?;
    observed.queue(X86, 1, Some(secs(2)))?;
    // A 10s observed boot makes buying a slot worthwhile for this long wait.
    assert_eq!(
        observed.reconcile(),
        vec![scale(X86, 1, 2, NeedCause::Demand)]
    );
    Ok(())
}

#[test]
fn readiness_median_keeps_latest_samples_and_ignores_reconnects() -> TestResult {
    let mut w = workload(3, 1)?;
    w.reconcile();
    assert_eq!(w.h.outlook(X86)?.readiness, None);
    for (name, delay, median) in [
        ("local-1", 10, 10),
        ("local-2", 20, 15),
        ("local-3", 30, 20),
        ("local-4", 40, 30),
    ] {
        w.h.launch(X86, name, InstanceLifecycle::Pending, Duration::ZERO)?;
        w.reconcile();
        w.h.advance(secs(delay));
        w.h.connect(X86, name)?;
        w.reconcile();
        assert_eq!(w.h.outlook(X86)?.readiness, Some(secs(median)));
    }
    w.h.disconnect("local-4")?;
    w.reconcile();
    w.h.advance(secs(60));
    w.h.connect(X86, "local-4")?;
    w.reconcile();
    assert_eq!(w.h.outlook(X86)?.readiness, Some(secs(30)));
    Ok(())
}

#[test]
fn stream_clock_resets_when_queue_clears() -> TestResult {
    let mut w = workload(13, 16)?;
    for name in ["local-1", "local-2", "local-3"] {
        serve(&mut w.h, X86, name)?;
    }
    w.queue(X86, 200, Some(secs(2)))?;
    w.reconcile();
    w.h.advance(secs(299));
    assert_eq!(w.reconcile(), vec![]);
    w.h.snapshot.queued.clear();
    w.reconcile();
    w.queue(X86, 200, Some(secs(2)))?;
    w.reconcile();
    w.h.advance(secs(299));
    assert_eq!(w.reconcile(), vec![]);
    w.h.advance(secs(1));
    assert_eq!(w.reconcile(), vec![scale(X86, 3, 13, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn overflow_fleet_not_scaled_when_first_drains() -> TestResult {
    let mut w = Workload::new(Harness::with_fleets(vec![
        fleet("gang-first", 1, 1)?,
        fleet("gang-second", 2, 1)?,
    ]));
    serve(&mut w.h, 0, "local-1")?;
    w.candidates(FleetSet::try_from_indices([0, 1])?, 2, Some(secs(2)));
    assert_eq!(w.reconcile(), vec![]);
    Ok(())
}

#[test]
fn launch_failure_excludes_unborn_slots() -> TestResult {
    let mut w = workload(4, 4)?;
    serve(&mut w.h, X86, "local-1")?;
    w.running("local-1", 4, Duration::ZERO, None)?;
    w.h.set_desired_capacity(X86, 2);
    w.h.supply_mut(X86)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "failed".to_owned(),
    });
    w.queue(X86, 8, None)?;
    assert_eq!(w.reconcile(), vec![scale(X86, 2, 3, NeedCause::Demand)]);
    Ok(())
}

fn fail_launch(w: &mut Workload, fleet: usize, desired: u32) -> TestResult {
    w.h.set_desired_capacity(fleet, desired);
    w.h.supply_mut(fleet)?.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "failed".to_owned(),
    });
    Ok(())
}

#[test]
fn failed_unborn_short_burst_does_not_ratchet() -> TestResult {
    let mut w = workload(6, 16)?;
    w.queue(X86, 16, Some(secs(2)))?;
    assert_eq!(w.run(), vec![scale(X86, 0, 1, NeedCause::Demand)]);
    fail_launch(&mut w, X86, 1)?;

    w.h.advance(secs(8));
    w.queue(X86, 32, Some(secs(2)))?;
    assert_eq!(w.run(), vec![]);
    assert_eq!(w.h.supply(X86)?.desired, 1);

    w.h.advance(secs(1));
    w.queue(X86, 112, Some(secs(2)))?;
    assert_eq!(w.run(), vec![]);
    assert_eq!(w.h.supply(X86)?.desired, 1);
    assert_eq!(w.run(), vec![]);
    assert_eq!(w.h.supply(X86)?.desired, 1);
    Ok(())
}

#[test]
fn failed_unborn_is_withdrawn_when_short_running_work_absorbs_queue() -> TestResult {
    let mut w = workload(4, 4)?;
    serve(&mut w.h, X86, "local-1")?;
    w.running("local-1", 4, Duration::ZERO, Some(secs(2)))?;
    fail_launch(&mut w, X86, 2)?;
    w.queue(X86, 1, Some(secs(2)))?;
    assert_eq!(
        w.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Withdraw { from: 2, to: 1 },
        }]
    );
    Ok(())
}

#[test]
fn failed_unborn_is_withdrawn_when_only_running_work_remains() -> TestResult {
    let mut w = workload(6, 1)?;
    serve(&mut w.h, X86, "local-1")?;
    w.running("local-1", 4, Duration::ZERO, Some(secs(2)))?;
    fail_launch(&mut w, X86, 3)?;
    assert_eq!(
        w.reconcile(),
        vec![PlanStep {
            fleet: X86,
            action: Action::Withdraw { from: 3, to: 1 },
        }]
    );
    Ok(())
}

#[test]
fn failed_unborn_withdrawal_preserves_busy_unresumed_drain() -> TestResult {
    let mut w = workload(4, 16)?;
    for name in ["local-1", "local-2"] {
        serve(&mut w.h, X86, name)?;
        w.h.set_draining(name, true)?;
        w.running(name, 1, Duration::ZERO, Some(secs(2)))?;
    }
    fail_launch(&mut w, X86, 4)?;
    w.queue(X86, 17, Some(secs(2)))?;
    assert_eq!(
        w.reconcile(),
        vec![
            PlanStep {
                fleet: X86,
                action: Action::CancelDrain(worker_id("local-1")?, NeedCause::Demand),
            },
            PlanStep {
                fleet: X86,
                action: Action::Withdraw { from: 4, to: 2 },
            },
        ]
    );
    Ok(())
}

#[test]
fn failed_fresh_slots_do_not_suppress_overflow_scale_out() -> TestResult {
    let mut w = Workload::new(Harness::with_fleets(vec![
        fleet("gang-first", 2, 1)?,
        fleet("gang-second", 2, 1)?,
    ]));
    fail_launch(&mut w, 0, 1)?;
    w.candidates(FleetSet::try_from_indices([0, 1])?, 3, Some(secs(2)));
    assert_eq!(w.reconcile(), vec![scale(1, 0, 1, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn resumed_drain_prevents_overflow_despite_failed_unborn() -> TestResult {
    let mut w = Workload::new(Harness::with_fleets(vec![
        fleet("gang-first", 1, 1)?,
        fleet("gang-second", 2, 1)?,
    ]));
    serve(&mut w.h, 0, "local-1")?;
    w.h.set_draining("local-1", true)?;
    w.running("local-1", 1, Duration::ZERO, Some(secs(2)))?;
    fail_launch(&mut w, 0, 2)?;
    w.candidates(FleetSet::try_from_indices([0, 1])?, 2, Some(secs(2)));
    assert_eq!(
        w.reconcile(),
        vec![
            PlanStep {
                fleet: 0,
                action: Action::CancelDrain(worker_id("local-1")?, NeedCause::Demand),
            },
            PlanStep {
                fleet: 0,
                action: Action::Withdraw { from: 2, to: 1 },
            },
        ]
    );
    Ok(())
}

#[test]
fn scheduled_minimum_floor() -> TestResult {
    let mut w = Workload::new(super::scheduled_harness("2026-11-16T16:00:00Z")?);
    assert_eq!(w.reconcile(), vec![scale(X86, 0, 1, NeedCause::Minimum)]);
    Ok(())
}

#[test]
fn settle_window_filling_queue() -> TestResult {
    let mut w = workload(6, 16)?;
    let mut steps = Vec::new();
    for (delay, count) in [(0, 16), (8, 32), (1, 112), (40, 0)] {
        w.h.advance(secs(delay));
        w.queue(X86, count, Some(secs(2)))?;
        steps.extend(w.run());
        if delay == 0 {
            w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
        }
    }
    assert_eq!(steps, vec![scale(X86, 0, 1, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn resource_bound_limits_slots() -> TestResult {
    let mut w = workload(6, 16)?;
    w.h.queue_sized(X86, 2, x86_resources(4000))?;
    assert_eq!(w.reconcile(), vec![scale(X86, 0, 2, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn removing_workers_remaps_executing_actions() -> TestResult {
    use gang_reconciler::TerminationCause;
    use gang_types::InstanceId;

    let mut w = workload(6, 4)?;
    w.h.connect(X86, "local-1")?;
    w.h.connect(X86, "local-2")?;
    w.running("local-1", 1, secs(1), Some(secs(3)))?;
    w.running("local-2", 1, secs(2), Some(secs(5)))?;
    w.h.disconnect("local-1")?;
    assert_eq!(w.h.snapshot.executing.len(), 1);
    assert_eq!(w.h.snapshot.executing[0].worker, 0);
    assert_eq!(w.h.snapshot.executing[0].expected, Some(secs(5)));
    assert_eq!(w.h.snapshot.workers[0].id, worker_id("local-2")?);

    w.h.connect(X86, "local-3")?;
    w.running("local-3", 1, secs(3), Some(secs(7)))?;
    w.h.plan.steps = vec![PlanStep {
        fleet: X86,
        action: Action::Terminate {
            instance: InstanceId::try_new("local-2")?,
            cause: TerminationCause::DrainFinished,
        },
    }];
    w.h.apply();
    assert_eq!(w.h.snapshot.executing.len(), 1);
    assert_eq!(w.h.snapshot.executing[0].worker, 0);
    assert_eq!(w.h.snapshot.executing[0].expected, Some(secs(7)));
    assert_eq!(w.h.snapshot.workers[0].id, worker_id("local-3")?);
    Ok(())
}
