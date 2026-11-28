//! Steady-state passes must not allocate once the reconciler's scratch buffers have grown.

use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock, TestClock};
use gang_config::{FleetPolicy, InstanceBounds, Weekday};
use gang_reconciler::{FleetPass, HostedSnapshot, PassTime, Plan, Reconciler};
use gang_spi::{SchedulerSnapshot, WorkerState};
use gang_testkit::{TestResult, scheduled_minimum};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_types::{FleetSet, InstanceId, InstanceLifecycle, Isa};

use crate::support::{Harness, X86, policy, policy_named, x86_resources};

/// Index of the second x86-64 fleet. It has no instances and is never needed in steady state.
const SPARE: usize = 1;

/// Passes run before measurement so the observations map and scratch buffers reach their
/// steady-state size.
const WARM_UP_PASSES: usize = 3;

/// Passes measured for allocations.
const MEASURED_PASSES: usize = 10;

/// The world a reconciler runs against, owned by the test rather than the harness.
///
/// The fleet passes are built once, so only [`Reconciler::reconcile`] itself is measured.
struct World {
    passes: Vec<FleetPass>,
    snapshot: SchedulerSnapshot,
    hosted: HostedSnapshot,
    clock: TestClock,
    reconciler: Reconciler,
    plan: Plan,
}

impl World {
    /// Takes the fleets, provisioner supply, scheduler snapshot and clock of `h`.
    fn new(h: &Harness) -> Self {
        let passes = h.passes.clone();
        let policies: Vec<_> = passes.iter().map(|pass| pass.policy.clone()).collect();
        let mut snapshot = h.snapshot.clone();
        snapshot.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot.clone());
        Self {
            passes,
            snapshot,
            hosted,
            clock: TestClock::new(h.clock.wall()),
            reconciler: Reconciler::new(&policies, Arc::clone(&h.scaling), h.profiles.clone()),
            plan: Plan::new(),
        }
    }

    /// Adds a connected worker to the scheduler and re-indexes the snapshot.
    fn connect(&mut self, worker: WorkerState) {
        self.snapshot.workers.push(worker);
        self.snapshot
            .workers
            .sort_unstable_by(|a, b| a.id.cmp(&b.id));
        self.hosted.replace(&mut self.snapshot.clone());
    }

    /// Runs one reconcile pass and returns the allocations of the reconcile call alone.
    fn counted_reconcile(&mut self) -> u64 {
        let time = PassTime::read(&self.clock);
        let info = allocation_counter::measure(|| {
            self.reconciler
                .reconcile(&mut self.passes, &self.hosted, time, &mut self.plan);
        });
        info.count_total
    }

    /// Advances the clock one second, runs one pass that must plan nothing, and returns the
    /// allocations of that pass.
    fn quiet_pass(&mut self) -> u64 {
        self.clock.advance(Duration::from_secs(1));
        let allocated = self.counted_reconcile();
        assert!(
            self.plan.steps.is_empty(),
            "steady-state pass planned steps"
        );
        allocated
    }
}

/// Builds a fleet in steady state that needs exactly as many instances as it has.
///
/// Four instances are in service and connected: `local-1` and `local-3` are busy, `local-2` is
/// idle, and `local-4` is draining with one action still running. `local-5` is booting. The
/// desired capacity of five leaves four usable instances once the draining one is excluded.
/// The two busy workers reserve one worker's CPU in total; three full-worker queued actions
/// bring the demand to four. Six unreserved actions may run on either x86-64 fleet, but the
/// first fleet already covers them all.
fn steady_state() -> TestResult<Harness> {
    steady_state_with(false)
}

/// Builds the same steady state as [`steady_state`]; when `release_first_idle` is set, the idle
/// `local-2` is a release-first instance.
fn steady_state_with(release_first_idle: bool) -> TestResult<Harness> {
    let window = scheduled_minimum(1, &Weekday::ALL[..5], "08:00", "19:00", "Europe/Paris")?;
    let mut primary = policy()?;
    primary.instances =
        InstanceBounds::try_new(0, primary.instances.max())?.with_schedule(vec![window])?;
    // local-4's drain outlasts the simulation: the warm-up's settle, then one second for each
    // warm-up and measured pass, and one more.
    let passes = u64::try_from(WARM_UP_PASSES + MEASURED_PASSES + 1)?;
    primary.max_drain_wait = settle_advance(&primary) + Duration::from_secs(passes);
    let scaling = gang_config::ScalingConfig {
        payback_percent: gang_config::PaybackPercent::try_new(100)?,
        ..gang_config::ScalingConfig::default()
    };
    let mut h = Harness::with_scaling(
        vec![primary, policy_named("gang-spare", Isa::X86_64)?],
        scaling,
    );
    let long_ago = Duration::from_hours(1);
    for instance in ["local-1", "local-2", "local-3", "local-4"] {
        if release_first_idle && instance == "local-2" {
            h.launch_release_first(X86, instance, InstanceLifecycle::InService, long_ago)?;
        } else {
            h.launch(X86, instance, InstanceLifecycle::InService, long_ago)?;
        }
        h.connect(X86, instance)?;
    }
    h.launch(
        X86,
        "local-5",
        InstanceLifecycle::Pending,
        Duration::from_secs(1),
    )?;
    h.set_running("local-1", 2)?;
    h.set_running("local-3", 2)?;
    h.set_running("local-4", 1)?;
    h.set_draining("local-4", true)?;
    for execution in &mut h.snapshot.executing {
        if !h.snapshot.workers[execution.worker].draining {
            execution.resources = x86_resources(1000);
        }
    }
    h.queue_candidates(FleetSet::try_from_indices([X86, SPARE])?, 6);
    h.queue_sized(X86, 3, x86_resources(4000))?;
    h.set_desired_capacity(X86, 5);
    Ok(h)
}

/// How far the warm-up advances after the term's first pass: past `idle_timeout`, so `local-2` is
/// a drain candidate, and past [`CLIENT_REROUTE_BOUND`], so drains are no longer held back.
fn settle_advance(policy: &FleetPolicy) -> Duration {
    (policy.idle_timeout + Duration::from_secs(1)).max(CLIENT_REROUTE_BOUND)
}

/// Runs the unmeasured warm-up passes.
///
/// The first pass starts the reconciler's term; the clock then advances by [`settle_advance`],
/// so every measured pass reaches the drain path. Without it the measured passes never do.
fn warm_up(world: &mut World) {
    world.counted_reconcile();
    assert!(world.plan.steps.is_empty(), "warm-up pass planned steps");
    world
        .clock
        .advance(settle_advance(&world.passes[X86].policy));
    for _ in 0..WARM_UP_PASSES {
        world.quiet_pass();
    }
}

/// Runs `passes` quiet passes and returns how many allocations the reconcile calls made.
fn measure_quiet_passes(world: &mut World, passes: usize) -> u64 {
    (0..passes).map(|_| world.quiet_pass()).sum()
}

#[test]
fn steady_state_passes_do_not_allocate() -> TestResult {
    let h = steady_state()?;
    let mut world = World::new(&h);
    // Warm-up advances past local-2's idle timeout so it is a drain candidate while measured.
    warm_up(&mut world);

    let allocated = measure_quiet_passes(&mut world, MEASURED_PASSES);

    assert_eq!(allocated, 0, "steady-state passes allocated");
    Ok(())
}

#[test]
fn steady_state_with_a_release_first_instance_does_not_allocate() -> TestResult {
    let h = steady_state_with(true)?;
    let mut world = World::new(&h);
    warm_up(&mut world);

    let allocated = measure_quiet_passes(&mut world, MEASURED_PASSES);

    assert_eq!(
        allocated, 0,
        "steady-state passes with a release-first instance allocated"
    );
    Ok(())
}

#[test]
fn a_new_worker_allocates_only_when_it_first_appears() -> TestResult {
    let mut h = steady_state()?;
    // The worker is connected through the harness so its scheduler state is built the same way
    // as the others, then withdrawn; it joins the world below when its instance is in service.
    h.connect(X86, "local-5")?;
    let appeared = h.snapshot.workers.pop().ok_or("local-5 is not connected")?;
    let mut world = World::new(&h);
    warm_up(&mut world);

    let booted = InstanceId::try_new("local-5")?;
    for instance in &mut world.passes[X86].supply.instances {
        if instance.id == booted {
            instance.lifecycle = InstanceLifecycle::InService;
        }
    }
    world.connect(appeared);

    // The first pass that sees the new worker may allocate while it registers the worker.
    world.quiet_pass();

    let allocated = measure_quiet_passes(&mut world, 5);

    assert_eq!(allocated, 0, "passes after the worker appeared allocated");
    Ok(())
}

#[test]
fn queued_vector_planning_and_final_dispatch_do_not_allocate_after_warmup() -> TestResult {
    let mut primary = policy()?;
    primary.instances =
        InstanceBounds::try_new(0, std::num::NonZeroU32::new(20).ok_or("zero maximum")?)?;
    primary.boot_grace = Duration::from_secs(150);
    let mut h = Harness::with_policy(primary);
    for name in ["local-1", "local-2"] {
        h.launch(
            X86,
            name,
            InstanceLifecycle::InService,
            Duration::from_mins(5),
        )?;
        h.connect(X86, name)?;
        h.snapshot
            .workers
            .last_mut()
            .ok_or("missing worker")?
            .max_inflight_actions = std::num::NonZeroU32::new(16);
    }
    h.set_desired_capacity(X86, 2);
    h.queue_sized(X86, 1000, x86_resources(1000))?;
    for action in &mut h.snapshot.queued {
        action.expected = Some(Duration::from_secs(30));
    }
    let mut world = World::new(&h);
    for _ in 0..WARM_UP_PASSES {
        world.counted_reconcile();
    }
    assert!(world.plan.steps.iter().any(|step| matches!(
        step.action,
        gang_reconciler::Action::ScaleOut { from: 2, .. }
    )));
    let allocated: u64 = (0..MEASURED_PASSES)
        .map(|_| world.counted_reconcile())
        .sum();
    assert_eq!(
        allocated, 0,
        "queued vector planning allocated after warmup"
    );
    Ok(())
}

#[test]
fn warmed_authenticated_source_rates_profiles_and_dispatch_allocate_zero() -> TestResult {
    let mut p = policy()?;
    p.instances = InstanceBounds::try_new(1, p.instances.max())?;
    p.idle_timeout = Duration::from_mins(10);
    let scaling = gang_config::ScalingConfig {
        max_sources: gang_config::SourceLimit::try_new(2)?,
        ..gang_config::ScalingConfig::default()
    };
    let mut h = Harness::with_scaling(vec![p], scaling);
    h.launch(
        X86,
        "local-1",
        InstanceLifecycle::InService,
        Duration::from_hours(1),
    )?;
    h.connect(X86, "local-1")?;
    h.set_desired_capacity(X86, 1);
    h.queue(X86, 2)?;
    for (index, row) in h.snapshot.queued.iter_mut().enumerate() {
        row.source = Some(format!("{:016x}", index + 1).parse()?);
        row.expected = Some(Duration::from_secs(2));
    }
    let mut world = World::new(&h);
    for _ in 0..WARM_UP_PASSES {
        world.counted_reconcile();
        world.clock.advance(Duration::from_secs(1));
    }
    let allocated = measure_quiet_passes(&mut world, MEASURED_PASSES);
    assert_eq!(
        allocated, 0,
        "warmed source observation/profile/dispatch allocated"
    );
    Ok(())
}

#[test]
fn warmed_completion_frame_merge_does_not_allocate() -> TestResult {
    let mut h = steady_state()?;
    let source: gang_types::SourceId = "0000000000000001".parse()?;
    for row in &mut h.snapshot.queued {
        row.source = Some(source);
    }
    let kappa = std::f64::consts::LN_2 / 60.0;
    let count = gang_spi::NonNegativeFinite::try_new(kappa).ok_or("invalid fixture count")?;
    let actions =
        gang_spi::NonNegativeFinite::try_new(kappa * 30.0).ok_or("invalid fixture work")?;
    h.snapshot.source_rates.push(gang_spi::CompletionRates {
        source: Some(source),
        preferred_fleet: X86,
        generation: 1,
        created_at: gang_clock::Tick::ORIGIN,
        at: gang_clock::Tick::ORIGIN,
        completions: count,
        timed_completions: count,
        work: gang_spi::ResourceRates {
            actions,
            ..gang_spi::ResourceRates::default()
        },
    });
    let mut world = World::new(&h);
    warm_up(&mut world);
    assert_eq!(measure_quiet_passes(&mut world, MEASURED_PASSES), 0);
    Ok(())
}
