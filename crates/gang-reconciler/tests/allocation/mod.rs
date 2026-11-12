//! Steady-state passes must not allocate once the reconciler's scratch buffers have grown.

use std::time::Duration;

use gang_clock::{Clock, TestClock};
use gang_reconciler::{FleetPass, HostedSnapshot, PassTime, Plan, Reconciler};
use gang_spi::{SchedulerSnapshot, WorkerState};
use gang_testkit::TestResult;
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
            reconciler: Reconciler::new(&policies),
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
/// desired capacity of five leaves four usable instances once the draining one is excluded. The
/// queued demand needs exactly four instances, so no pass should change anything. The queued
/// actions may run on either x86-64 fleet, but the first fleet already covers them all.
fn steady_state() -> TestResult<Harness> {
    steady_state_with(false)
}

/// Builds the same steady state as [`steady_state`]; when `release_first_idle` is set, the idle
/// `local-2` is a release-first instance.
fn steady_state_with(release_first_idle: bool) -> TestResult<Harness> {
    let mut h = Harness::with_fleets(vec![policy()?, policy_named("gang-spare", Isa::X86_64)?]);
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
    h.queue_candidates(FleetSet::try_from_indices([X86, SPARE])?, 6);
    h.queue_sized(X86, 2, x86_resources(8))?;
    h.set_desired_capacity(X86, 5);
    Ok(h)
}

/// Runs the unmeasured warm-up passes.
///
/// Advances the clock past `idle_timeout` first so `local-2` is a drain candidate during every
/// measured pass; without it the measured passes never reach the drain-candidate path.
fn warm_up(world: &mut World) {
    world
        .clock
        .advance(world.passes[X86].policy.idle_timeout + Duration::from_secs(1));
    world.counted_reconcile();
    assert!(world.plan.steps.is_empty(), "warm-up pass planned steps");
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
