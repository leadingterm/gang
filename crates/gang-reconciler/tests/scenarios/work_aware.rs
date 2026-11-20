// Fixture arithmetic deliberately crosses integer resource units and floating rate units.
#![allow(clippy::cast_precision_loss)]
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{Clock, Tick};
use gang_config::{FleetPolicy, InstanceBounds, PaybackPercent, ScalingConfig};
use gang_reconciler::{Action, NeedCause, PassTime, PlanStep, ScaleDecision};
use gang_spi::{CompletionRates, NonNegativeFinite, ResourceRates};
use gang_testkit::TestResult;
use gang_types::{ActionResources, FleetSet, InstanceLifecycle, Isa, NonZeroDuration, SourceId};

use crate::support::{Harness, X86, policy_named};

pub(super) fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

pub(super) fn source(n: u64) -> TestResult<SourceId> {
    Ok(format!("{n:016x}").parse()?)
}

pub(super) fn fleet(name: &str, max: u32, target: u32) -> TestResult<FleetPolicy> {
    let mut p = policy_named(name, Isa::X86_64)?;
    p.instances = InstanceBounds::try_new(0, NonZeroU32::new(max).ok_or("zero maximum")?)?;
    p.target_actions_per_instance = NonZeroU32::new(target).ok_or("zero target")?;
    p.boot_grace = secs(600);
    p.idle_timeout = secs(600);
    p.max_drain_wait = secs(3600);
    Ok(p)
}

pub(super) fn settings(payback: u16) -> TestResult<ScalingConfig> {
    Ok(ScalingConfig {
        payback_percent: PaybackPercent::try_new(payback)?,
        readiness_prior: NonZeroDuration::from_secs(45),
        ..ScalingConfig::default()
    })
}

pub(super) struct Workload {
    pub h: Harness,
}

impl Workload {
    pub fn new(max: u32, target: u32, payback: u16) -> TestResult<Self> {
        Ok(Self {
            h: Harness::with_scaling(vec![fleet("gang-x86", max, target)?], settings(payback)?),
        })
    }

    pub fn queue(
        &mut self,
        source: Option<SourceId>,
        fleet: usize,
        count: usize,
        expected: Option<Duration>,
    ) -> TestResult {
        let start = self.h.snapshot.queued.len();
        self.h.queue(fleet, count)?;
        for row in &mut self.h.snapshot.queued[start..] {
            row.source = source;
            row.expected = expected;
        }
        Ok(())
    }

    pub fn serve(&mut self, fleet: usize, name: &str) -> TestResult {
        let desired = self.h.supply(fleet)?.desired;
        self.h
            .launch(fleet, name, InstanceLifecycle::InService, secs(3600))?;
        self.h.set_desired_capacity(fleet, desired + 1);
        self.h.connect(fleet, name)?;
        self.h
            .snapshot
            .workers
            .last_mut()
            .ok_or("missing worker")?
            .max_inflight_actions = NonZeroU32::new(16);
        Ok(())
    }

    pub fn running(
        &mut self,
        name: &str,
        count: u32,
        source: Option<SourceId>,
        elapsed: Duration,
        expected: Option<Duration>,
    ) -> TestResult {
        self.h.set_running(name, count)?;
        let id = gang_testkit::worker_id(name)?;
        let index = self
            .h
            .snapshot
            .workers
            .iter()
            .position(|row| row.id == id)
            .ok_or("missing worker")?;
        for row in self
            .h
            .snapshot
            .executing
            .iter_mut()
            .filter(|row| row.worker == index)
        {
            row.source = source;
            row.started = self.h.clock.wall() - elapsed;
            row.expected = expected;
        }
        Ok(())
    }

    // Independent fixture producer: the same exponential event kernel as the SPI contract,
    // never synthesized from the queue gauge or the planner's output.
    #[allow(clippy::expect_used)] // finite inputs and a bounded positive half-life prove construction.
    pub fn complete(
        &mut self,
        source: Option<SourceId>,
        preferred_fleet: usize,
        count: u32,
        timed: Option<Duration>,
        resources: ActionResources,
    ) {
        let now = self.h.clock.monotonic();
        let kappa =
            std::f64::consts::LN_2 / Duration::from(self.h.scaling.load_half_life).as_secs_f64();
        let row = self
            .h
            .snapshot
            .source_rates
            .iter()
            .position(|row| row.source == source && row.preferred_fleet == preferred_fleet);
        let old = row.map(|i| self.h.snapshot.source_rates[i]);
        let decay = old.map_or(0.0, |old| {
            (-kappa * now.saturating_duration_since(old.at).as_secs_f64()).exp()
        });
        let n = f64::from(count) * kappa;
        let work = timed.map_or(0.0, |duration| n * duration.as_secs_f64());
        let finite = |value| {
            NonNegativeFinite::try_new(value).expect("fixture moments are finite and nonnegative")
        };
        let old_work = old.map_or(ResourceRates::default(), |old| old.work);
        let next = CompletionRates {
            source,
            preferred_fleet,
            generation: old.map_or(1, |old| old.generation),
            created_at: old.map_or(now, |old| old.created_at),
            at: now,
            completions: finite(old.map_or(0.0, |old| old.completions.get()) * decay + n),
            timed_completions: finite(
                old.map_or(0.0, |old| old.timed_completions.get()) * decay
                    + if timed.is_some() { n } else { 0.0 },
            ),
            work: ResourceRates {
                actions: finite(old_work.actions.get() * decay + work),
                cpu_millicores: finite(
                    old_work.cpu_millicores.get() * decay + work * resources.cpu_millicores as f64,
                ),
                memory: finite(
                    old_work.memory.get() * decay + work * resources.memory.get() as f64,
                ),
                disk: finite(old_work.disk.get() * decay + work * resources.disk.get() as f64),
            },
        };
        if let Some(index) = row {
            self.h.snapshot.source_rates[index] = next;
        } else {
            self.h.snapshot.source_rates.push(next);
            self.h
                .snapshot
                .source_rates
                .sort_unstable_by_key(|row| (row.source, row.preferred_fleet));
        }
    }

    pub fn pass(&mut self) -> Vec<PlanStep> {
        self.h.snapshot.source_rates_at = self.h.clock.monotonic();
        self.h.reconcile()
    }

    pub fn run(&mut self) -> Vec<PlanStep> {
        let steps = self.pass();
        self.h.apply();
        steps
    }

    pub fn observe_for(&mut self, duration: Duration) {
        let cadence = Duration::from(self.h.scaling.source_idle) / 2;
        let mut remaining = duration;
        while !remaining.is_zero() {
            let advance = remaining.min(cadence);
            self.h.advance(advance);
            self.pass();
            remaining -= advance;
        }
    }

    pub fn learn_profile(&mut self, source: SourceId, age: Duration) -> TestResult {
        self.queue(Some(source), X86, 1, Some(secs(1)))?;
        self.pass();
        self.observe_for(age);
        self.h.snapshot.queued.clear();
        self.pass();
        self.h.advance(Duration::from(self.h.scaling.source_idle));
        self.pass();
        let mut records = Vec::new();
        self.h
            .profiles
            .snapshot(PassTime::read(&self.h.clock), &mut records)?;
        assert!(
            !records.is_empty(),
            "a completed source period must be durable"
        );
        self.h
            .profiles
            .restore(PassTime::read(&self.h.clock), &records);
        self.h.restart();
        Ok(())
    }
}

pub(super) fn purchases(steps: &[PlanStep]) -> Vec<(usize, u32, u32, NeedCause)> {
    steps
        .iter()
        .filter_map(|step| match step.action {
            Action::ScaleOut { from, to, need, .. } => Some((step.fleet, from, to, need)),
            _ => None,
        })
        .collect()
}

pub(super) fn summary(steps: &[PlanStep]) -> TestResult<ScaleDecision> {
    steps
        .iter()
        .find_map(|step| match step.action {
            Action::ScaleOut { summary, .. } => Some(summary),
            _ => None,
        })
        .ok_or_else(|| "expected a purchase summary".into())
}

#[test]
fn cold_burst_buys_one() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 16, None)?;
    let mut steps = w.run();
    let decision = summary(&steps)?;
    assert_eq!(decision.readiness, secs(45));
    assert_eq!(decision.horizon, Duration::from_millis(322_500));
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    for (delay, additional) in [(8, 32), (1, 112)] {
        w.h.advance(secs(delay));
        w.queue(Some(source(1)?), X86, additional, None)?;
        steps.extend(w.run());
        assert_eq!(
            w.h.outlook(X86)?.needed,
            1,
            "pending capacity remains in the work baseline"
        );
    }
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    assert_eq!(w.h.supply(X86)?.desired, 1);
    Ok(())
}

#[test]
fn burst_payback_uses_idle_tail() -> TestResult {
    for (percent, expected) in [
        (100, vec![]),
        (50, vec![]),
        (25, vec![(X86, 1, 3, NeedCause::Demand)]),
    ] {
        let mut w = Workload::new(6, 16, percent)?;
        w.h.set_desired_capacity(X86, 1);
        w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
        w.queue(Some(source(1)?), X86, 160, Some(secs(60)))?;
        assert_eq!(purchases(&w.pass()), expected, "payback={percent}");
    }
    Ok(())
}

#[test]
fn young_dump_is_transient_and_static_batch_never_fabricates_refill() -> TestResult {
    for known in [Some(secs(30)), None] {
        let mut w = Workload::new(6, 16, 50)?;
        w.serve(X86, "local-1")?;
        w.running("local-1", 16, Some(source(1)?), Duration::ZERO, known)?;
        w.queue(Some(source(1)?), X86, 144, known)?;
        assert!(purchases(&w.pass()).is_empty());
        for _ in 0..10 {
            w.h.advance(secs(30));
            let completed = w.h.snapshot.executing.len().min(16);
            w.complete(
                Some(source(1)?),
                X86,
                u32::try_from(completed)?,
                Some(secs(30)),
                ActionResources::default(),
            );
            let start = w.h.snapshot.queued.len().min(16);
            w.h.snapshot.queued.drain(..start);
            w.running(
                "local-1",
                u32::try_from(start)?,
                Some(source(1)?),
                Duration::ZERO,
                known,
            )?;
            let steps = w.pass();
            assert!(
                purchases(&steps).is_empty(),
                "static dump bought: {steps:?}"
            );
            assert_eq!(
                w.h.outlook(X86)?.load,
                Some(0),
                "known gauge survives first counter birth"
            );
        }
    }
    Ok(())
}

#[test]
fn seventeen_action_static_build_does_not_ratchet() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 17, None)?;
    assert_eq!(purchases(&w.run()), vec![(X86, 0, 1, NeedCause::Demand)]);
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    w.h.advance(secs(8));
    assert!(purchases(&w.pass()).is_empty());
    assert_eq!(w.h.outlook(X86)?.needed, 1);
    Ok(())
}

#[test]
fn persistent_queue_nets_booting_and_unborn_capacity_once() -> TestResult {
    for (serving, queue, booting, needed) in [(1, 40, 2, 3), (0, 32, 1, 2)] {
        let mut w = Workload::new(6, 16, 50)?;
        w.learn_profile(source(1)?, secs(1800))?;
        if serving == 1 {
            w.serve(X86, "local-1")?;
        }
        w.h.set_desired_capacity(X86, serving + booting);
        w.queue(Some(source(1)?), X86, queue, None)?;
        let steps = w.pass();
        assert_eq!(w.h.outlook(X86)?.needed, needed);
        if needed > serving + booting {
            assert_eq!(
                purchases(&steps),
                vec![(X86, serving + booting, needed, NeedCause::Demand)]
            );
        } else {
            assert!(purchases(&steps).is_empty());
        }
    }
    Ok(())
}

#[test]
fn known_source_profile_trusts_immediately() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    w.queue(Some(source(1)?), X86, 48, None)?;
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 0, 3, NeedCause::Demand)]);
    assert_eq!(summary(&steps)?.persistent_streams, 1);
    Ok(())
}

#[test]
fn closed_loop_aged_source_buys_unmet_concurrency() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.serve(X86, "local-1")?;
    w.queue(Some(source(1)?), X86, 40, None)?;
    w.running(
        "local-1",
        16,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(30)),
    )?;
    w.pass();
    for _ in 0..13 {
        w.h.advance(secs(30));
        w.complete(
            Some(source(1)?),
            X86,
            16,
            Some(secs(30)),
            ActionResources::default(),
        );
        w.running(
            "local-1",
            16,
            Some(source(1)?),
            Duration::ZERO,
            Some(secs(30)),
        )?;
        let steps = w.pass();
        if w.h.clock.monotonic().elapsed() < secs(368) {
            assert!(purchases(&steps).is_empty());
        } else {
            assert_eq!(purchases(&steps), vec![(X86, 1, 3, NeedCause::Demand)]);
        }
    }
    Ok(())
}

#[test]
fn steady_ci_stream_scales_stepwise_and_bounded() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        16,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(20)),
    )?;
    w.pass();
    for queued in [4, 8, 12, 16, 32] {
        w.h.advance(secs(20));
        w.complete(
            Some(source(1)?),
            X86,
            16,
            Some(secs(20)),
            ActionResources::default(),
        );
        w.h.snapshot.queued.clear();
        w.queue(Some(source(1)?), X86, queued, Some(secs(20)))?;
        let steps = w.run();
        assert!(w.h.supply(X86)?.desired <= 3);
        if queued < 16 {
            assert!(purchases(&steps).is_empty());
        }
        if queued == 16 {
            assert_eq!(purchases(&steps), vec![(X86, 1, 2, NeedCause::Demand)]);
        }
        if queued == 32 {
            assert_eq!(purchases(&steps), vec![(X86, 2, 3, NeedCause::Demand)]);
        }
    }
    Ok(())
}

#[test]
fn two_sources_share_capacity_without_cross_attribution() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        12,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(20)),
    )?;
    w.pass();
    for _ in 0..120 {
        w.h.advance(secs(5));
        w.complete(
            Some(source(1)?),
            X86,
            3,
            Some(secs(20)),
            ActionResources::default(),
        );
        w.pass();
    }
    w.queue(Some(source(2)?), X86, 200, Some(secs(30)))?;
    let first = w.pass();
    assert!(
        purchases(&first).is_empty(),
        "source A's profile must not make B persistent"
    );
    assert!(
        w.h.outlook(X86)?
            .load
            .is_some_and(|load| (110..=130).contains(&load))
    );
    for tick in 1..=78 {
        w.h.advance(secs(5));
        w.complete(
            Some(source(1)?),
            X86,
            3,
            Some(secs(20)),
            ActionResources::default(),
        );
        if tick % 6 == 0 {
            w.complete(
                Some(source(2)?),
                X86,
                4,
                Some(secs(30)),
                ActionResources::default(),
            );
        }
        w.pass();
    }
    let aged = w.h.plan.steps.clone();
    assert_eq!(purchases(&aged), vec![(X86, 1, 6, NeedCause::Demand)]);
    assert_eq!(summary(&aged)?.persistent_streams, 2);
    Ok(())
}

#[test]
fn long_tail_never_buys_and_idle_surplus_leaves() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    for name in [
        "local-1", "local-2", "local-3", "local-4", "local-5", "local-6",
    ] {
        w.serve(X86, name)?;
    }
    w.running("local-1", 1, Some(source(1)?), secs(1800), Some(secs(1800)))?;
    w.run();
    w.h.advance(secs(600));
    let steps = w.run();
    assert!(purchases(&steps).is_empty());
    assert!(
        steps
            .iter()
            .any(|step| matches!(step.action, Action::StartDrain { .. }))
    );
    assert!(!w.h.snapshot.workers[0].draining);
    w.run();
    assert!(w.h.supply(X86)?.desired < 6);
    Ok(())
}

#[test]
fn final_dispatch_has_two_free_slots_and_never_changes_purchase() -> TestResult {
    let mut plans = Vec::new();
    for durations in [[100, 1], [1, 100]] {
        let mut w = Workload::new(6, 2, 50)?;
        w.serve(X86, "local-1")?;
        for duration in durations {
            w.queue(Some(source(1)?), X86, 1, Some(secs(duration)))?;
        }
        plans.push(purchases(&w.pass()));
        assert_eq!(w.h.outlook(X86)?.expected_wait, Some(Duration::ZERO));
    }
    assert_eq!(plans, vec![vec![], vec![]]);
    Ok(())
}

#[test]
fn reachability_floor_respects_other_candidates_pending_and_failure() -> TestResult {
    for failed in [false, true] {
        let mut w = Workload {
            h: Harness::with_scaling(
                vec![fleet("gang-first", 1, 16)?, fleet("gang-second", 2, 16)?],
                settings(50)?,
            ),
        };
        w.h.set_desired_capacity(0, 1);
        if failed {
            w.h.supply_mut(0)?.launch_failure = Some(gang_spi::LaunchFailure {
                reason: "capacity unavailable".into(),
            });
        } else {
            w.h.launch(0, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
        }
        w.h.queue_candidates(FleetSet::prefix(2)?, 32);
        let steps = w.pass();
        assert_eq!(purchases(&steps).iter().any(|step| step.0 == 1), failed);
    }
    Ok(())
}

#[test]
fn unknown_duration_prefers_authenticated_source_mean_not_readiness() -> TestResult {
    for authenticated in [true, false] {
        let mut w = Workload::new(6, 16, 50)?;
        let src = authenticated.then(|| source(1)).transpose()?;
        w.serve(X86, "local-1")?;
        w.running("local-1", 1, src, Duration::ZERO, Some(secs(600)))?;
        w.queue(src, X86, 160, None)?;
        let steps = w.pass();
        if authenticated {
            assert!(!purchases(&steps).is_empty());
        } else {
            assert!(purchases(&steps).is_empty());
        }
    }
    Ok(())
}

#[test]
fn complementary_resources_are_summed_before_the_maximum() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    let p = &w.h.passes[X86].policy;
    let cpu = u64::from(p.vcpus_per_instance.get()) * 1000;
    let memory = p.memory_per_instance.get();
    w.queue(Some(source(1)?), X86, 1, None)?;
    w.h.snapshot.queued[0].resources = ActionResources {
        cpu_millicores: cpu,
        ..ActionResources::default()
    };
    w.queue(Some(source(1)?), X86, 1, None)?;
    w.h.snapshot.queued[1].resources = ActionResources {
        memory,
        ..ActionResources::default()
    };
    assert_eq!(purchases(&w.pass()), vec![(X86, 0, 1, NeedCause::Demand)]);
    Ok(())
}

#[test]
fn zero_free_action_slots_do_not_invent_resource_capacity() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        16,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(3600)),
    )?;
    let memory = w.h.passes[X86].policy.memory_per_instance.get();
    w.queue(Some(source(2)?), X86, 16, Some(secs(600)))?;
    for row in &mut w.h.snapshot.queued {
        row.resources.memory = memory;
    }
    let steps = w.pass();
    assert!(
        !purchases(&steps).is_empty(),
        "occupied action slots must gate spare memory"
    );
    Ok(())
}

#[test]
fn measured_readiness_zero_and_idle_zero_use_analytic_limit() -> TestResult {
    let mut p = fleet("gang-x86", 6, 16)?;
    p.boot_grace = Duration::ZERO;
    p.idle_timeout = Duration::ZERO;
    let mut w = Workload {
        h: Harness::with_scaling(vec![p], settings(1000)?),
    };
    w.queue(Some(source(1)?), X86, 48, None)?;
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 0, 3, NeedCause::Demand)]);
    let decision = summary(&steps)?;
    assert!(decision.zero_overhead);
    assert_eq!(decision.horizon, Duration::ZERO);
    assert!(decision.transient_extra.get().is_finite());
    w.h.snapshot.queued.clear();
    assert!(purchases(&w.pass()).is_empty());
    Ok(())
}

#[test]
fn huge_payback_duration_is_finite_and_does_not_overflow() -> TestResult {
    let mut p = fleet("gang-x86", 6, 16)?;
    p.idle_timeout = Duration::MAX;
    let mut w = Workload {
        h: Harness::with_scaling(vec![p], settings(1000)?),
    };
    w.queue(Some(source(1)?), X86, 160, None)?;
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    let decision = summary(&steps)?;
    assert!(decision.horizon >= secs(45));
    assert!(decision.transient_extra.get().is_finite());
    Ok(())
}

#[test]
fn source_rates_use_the_already_advanced_shared_clock() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.h.advance(secs(4 * 3600));
    w.pass();
    w.h.advance(secs(1));
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert!(w.h.snapshot.source_rates[0].created_at >= Tick::from_elapsed(secs(4 * 3600)));
    assert!(w.h.outlook(X86)?.backlog.unwrap_or_default().is_zero());
    assert!(purchases(&w.h.plan.steps).is_empty());
    Ok(())
}

#[test]
fn open_loop_excess_accumulates_until_one_whole_instance_repays() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    w.serve(X86, "local-1")?;
    w.running(
        "local-1",
        16,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(20)),
    )?;
    w.pass();
    for queue in [4, 8, 12, 16] {
        w.h.advance(secs(20));
        w.complete(
            Some(source(1)?),
            X86,
            16,
            Some(secs(20)),
            ActionResources::default(),
        );
        w.h.snapshot.queued.clear();
        w.queue(Some(source(1)?), X86, queue, Some(secs(20)))?;
        let steps = w.pass();
        assert_eq!(purchases(&steps).len(), usize::from(queue == 16));
        if queue == 16 {
            assert_eq!(purchases(&steps), vec![(X86, 1, 2, NeedCause::Demand)]);
        }
    }
    Ok(())
}

#[test]
fn readiness_median_keeps_latest_samples_and_excludes_reconnects() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.pass();
    assert_eq!(w.h.outlook(X86)?.readiness, None);
    for (index, delay, median) in [(1, 10, 10), (2, 20, 15), (3, 30, 20), (4, 40, 25)] {
        let name = format!("local-{index}");
        w.h.set_desired_capacity(X86, index);
        w.h.launch(X86, &name, InstanceLifecycle::Pending, Duration::ZERO)?;
        w.pass();
        w.h.advance(secs(delay));
        w.h.connect(X86, &name)?;
        w.pass();
        assert_eq!(w.h.outlook(X86)?.readiness, Some(secs(median)));
    }
    w.h.disconnect("local-4")?;
    w.pass();
    w.h.advance(secs(60));
    w.h.connect(X86, "local-4")?;
    w.pass();
    assert_eq!(w.h.outlook(X86)?.readiness, Some(secs(25)));
    Ok(())
}

#[test]
fn unknown_running_overrun_keeps_slot_occupied_without_buying_for_wait() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.serve(X86, "local-1")?;
    w.running("local-1", 16, Some(source(1)?), secs(300), None)?;
    w.queue(Some(source(1)?), X86, 16, None)?;
    assert!(
        purchases(&w.pass()).is_empty(),
        "display wait must not be a purchase trigger"
    );
    assert!(
        w.h.outlook(X86)?
            .expected_wait
            .is_some_and(|wait| wait >= secs(300))
    );
    Ok(())
}

#[test]
fn booting_capacity_reduces_transient_backlog_after_early_readiness() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.h.set_desired_capacity(X86, 1);
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, secs(44))?;
    w.queue(Some(source(1)?), X86, 160, Some(secs(30)))?;
    assert!(purchases(&w.pass()).is_empty());
    w.h.advance(secs(1));
    assert!(purchases(&w.pass()).is_empty());
    assert_eq!(w.h.outlook(X86)?.needed, 1);
    Ok(())
}

#[test]
fn measured_zero_readiness_and_zero_idle_use_concurrency_limit() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.h.passes[X86].policy.idle_timeout = Duration::ZERO;
    w.pass();
    w.h.set_desired_capacity(X86, 1);
    w.h.launch(X86, "local-1", InstanceLifecycle::Pending, Duration::ZERO)?;
    w.pass();
    w.h.connect(X86, "local-1")?;
    w.pass();
    assert_eq!(w.h.outlook(X86)?.readiness, Some(Duration::ZERO));
    w.queue(Some(source(1)?), X86, 48, None)?;
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 1, 3, NeedCause::Demand)]);
    assert!(summary(&steps)?.zero_overhead);
    Ok(())
}

#[test]
fn final_dispatch_queue_order_changes_wait_but_not_scale_decisions() -> TestResult {
    let mut decisions = Vec::new();
    let mut waits = Vec::new();
    for durations in [[100, 100, 1], [1, 100, 100]] {
        let mut w = Workload::new(6, 2, 50)?;
        w.serve(X86, "local-1")?;
        for duration in durations {
            w.queue(Some(source(1)?), X86, 1, Some(secs(duration)))?;
        }
        decisions.push(purchases(&w.pass()));
        waits.push(w.h.outlook(X86)?.expected_wait);
    }
    assert_eq!(decisions, vec![vec![], vec![]]);
    assert_eq!(waits, vec![Some(secs(100)), Some(secs(1))]);
    Ok(())
}
