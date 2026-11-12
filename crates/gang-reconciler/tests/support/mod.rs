//! Shared test harness: a simulated provisioner and scheduler around the [`Reconciler`].
//!
//! Integration tests pull this in with `mod support;`. The harness owns the world state (fleet
//! supply, scheduler snapshot, clock) and applies each plan the way the provisioner and the
//! scheduler would, so tests can drive several passes in sequence.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{Clock, TestClock, Timestamp};
use gang_config::FleetPolicy;
use gang_reconciler::{
    Action, FleetOutlook, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler,
};
use gang_spi::{
    FleetInstance, FleetSupply, QueuedDemand, ScalingAuditEvent, SchedulerSnapshot, WorkerState,
};
use gang_testkit::{TestResult, worker_id};
use gang_types::{
    Bytes, FleetName, FleetSet, InstanceId, InstanceLifecycle, Isa, NameError, Resources,
};

/// Index of the x86-64 test fleet in the harness's fleets.
pub const X86: usize = 0;

/// Index of the aarch64 test fleet in the harness's fleets.
pub const ARM: usize = 1;

/// Harness misuse: a test named a worker or fleet the harness does not have.
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    /// No connected worker runs on the named instance.
    #[error("unknown worker {0}")]
    UnknownWorker(String),
    /// The harness has no fleet at this index.
    #[error("no fleet at index {0}")]
    NoFleet(usize),
    /// No instance of the fleet has the given id.
    #[error("unknown instance {0}")]
    UnknownInstance(String),
    /// The fleet name is not a valid name.
    #[error("invalid fleet name: {0}")]
    FleetName(#[from] NameError),
}

/// Concurrent actions a connected worker accepts in these tests.
const MAX_INFLIGHT_ACTIONS: Option<NonZeroU32> = NonZeroU32::new(4);

/// A fleet policy named `name` for `isa`, otherwise as [`gang_testkit::fleet_policy`].
pub fn policy_named(name: &str, isa: Isa) -> Result<FleetPolicy, HarnessError> {
    Ok(FleetPolicy {
        isa,
        ..gang_testkit::fleet_policy(name)?
    })
}

/// The x86-64 fleet policy used by the harness, named `gang-x86`.
pub fn policy() -> Result<FleetPolicy, HarnessError> {
    policy_named("gang-x86", Isa::X86_64)
}

/// Resources of an action that needs `vcpus` virtual CPUs, 1 GiB of memory and 1 GiB of disk.
#[must_use]
pub fn x86_resources(vcpus: u32) -> Resources {
    Resources {
        vcpus,
        memory: Bytes::new(1 << 30),
        disk: Bytes::new(1 << 30),
    }
}

/// The provisioner and scheduler around one [`Reconciler`].
pub struct Harness {
    /// Fleet policies, supplies and outlooks in declaration order.
    pub passes: Vec<FleetPass>,
    /// What the scheduler holds right now.
    pub snapshot: SchedulerSnapshot,
    /// The indexed snapshot the latest pass ran against.
    pub hosted: HostedSnapshot,
    /// The clock every pass reads.
    pub clock: TestClock,
    /// The plan written by the most recent pass.
    pub plan: Plan,
    /// The reconciler under test.
    pub reconciler: Reconciler,
}

impl Harness {
    /// A harness with one x86-64 fleet built from [`policy`], an empty scheduler, and a
    /// provisioner with no instances.
    pub fn new() -> Result<Self, HarnessError> {
        Ok(Self::with_policy(policy()?))
    }

    /// A harness with one fleet built from `policy`.
    #[must_use]
    pub fn with_policy(policy: FleetPolicy) -> Self {
        Self::with_fleets(vec![policy])
    }

    /// A harness with the given fleets, each starting with an empty provisioner supply.
    #[must_use]
    pub fn with_fleets(fleets: Vec<FleetPolicy>) -> Self {
        let reconciler = Reconciler::new(&fleets);
        let passes = fleets.into_iter().map(FleetPass::new).collect();
        Self {
            passes,
            snapshot: SchedulerSnapshot::default(),
            hosted: HostedSnapshot::default(),
            clock: TestClock::new(Timestamp::from_unix_millis(1_000_000)),
            plan: Plan::new(),
            reconciler,
        }
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&mut self, by: Duration) {
        self.clock.advance(by);
    }

    /// Queues `count` actions with no resource requests on the fleet at index `fleet`.
    pub fn queue(&mut self, fleet: usize, count: usize) -> TestResult {
        self.queue_sized(fleet, count, Resources::default())
    }

    /// Queues `count` actions with the given resources on the fleet at index `fleet`.
    pub fn queue_sized(&mut self, fleet: usize, count: usize, resources: Resources) -> TestResult {
        self.queue_candidates_sized(FleetSet::try_from_indices([fleet])?, count, resources);
        Ok(())
    }

    /// Queues `count` actions with no resource requests on any fleet in `candidates`.
    pub fn queue_candidates(&mut self, candidates: FleetSet, count: usize) {
        self.queue_candidates_sized(candidates, count, Resources::default());
    }

    fn queue_candidates_sized(&mut self, candidates: FleetSet, count: usize, resources: Resources) {
        let queued_since = self.clock.wall();
        for _ in 0..count {
            self.snapshot.queued.push(QueuedDemand {
                candidates,
                resources,
                queued_since,
                priority: 0,
            });
        }
    }

    /// Adds an instance to the provisioner's fleet at index `fleet`, launched `launched_ago`
    /// before now. Does not change the desired capacity.
    pub fn launch(
        &mut self,
        fleet: usize,
        instance: &str,
        lifecycle: InstanceLifecycle,
        launched_ago: Duration,
    ) -> TestResult {
        self.push_instance(fleet, instance, lifecycle, launched_ago, false)
    }

    /// Like [`Harness::launch`], but the instance is release-first: when its fleet has a surplus,
    /// the reconciler drains it before the other instances, as it does for an on-demand member
    /// of a fleet that falls back from Spot.
    pub fn launch_release_first(
        &mut self,
        fleet: usize,
        instance: &str,
        lifecycle: InstanceLifecycle,
        launched_ago: Duration,
    ) -> TestResult {
        self.push_instance(fleet, instance, lifecycle, launched_ago, true)
    }

    fn push_instance(
        &mut self,
        fleet: usize,
        instance: &str,
        lifecycle: InstanceLifecycle,
        launched_ago: Duration,
        release_first: bool,
    ) -> TestResult {
        let launched_at = self.clock.wall() - launched_ago;
        let id = InstanceId::try_new(instance)?;
        self.supply_mut(fleet)?.instances.push(FleetInstance {
            id,
            lifecycle,
            launched_at,
            release_first,
        });
        Ok(())
    }

    /// Sets the lifecycle of the instance `instance` in the fleet at index `fleet`, and the time
    /// it entered that lifecycle to `launched_ago` before now, as the provisioner does when an
    /// instance begins running.
    pub fn set_lifecycle(
        &mut self,
        fleet: usize,
        instance: &str,
        lifecycle: InstanceLifecycle,
        launched_ago: Duration,
    ) -> TestResult {
        let launched_at = self.clock.wall() - launched_ago;
        let id = InstanceId::try_new(instance)?;
        let entry = self
            .supply_mut(fleet)?
            .instances
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or_else(|| HarnessError::UnknownInstance(instance.to_owned()))?;
        entry.lifecycle = lifecycle;
        entry.launched_at = launched_at;
        Ok(())
    }

    /// Sets the provisioner's desired capacity for the fleet at index `fleet`. Does nothing
    /// when there is no such fleet.
    pub fn set_desired_capacity(&mut self, fleet: usize, desired: u32) {
        if let Some(pass) = self.passes.get_mut(fleet) {
            pass.supply.desired = desired;
        }
    }

    /// Connects an idle worker, running on `instance`, to the scheduler as a worker of the
    /// fleet at index `fleet`.
    pub fn connect(&mut self, fleet: usize, instance: &str) -> TestResult {
        let name = self
            .passes
            .get(fleet)
            .ok_or(HarnessError::NoFleet(fleet))?
            .policy
            .name
            .clone();
        self.connect_as(name, instance)
    }

    /// Connects an idle worker, running on `instance`, to the scheduler as a worker of the
    /// fleet named `fleet`, which need not be a configured fleet.
    pub fn connect_as(&mut self, fleet: FleetName, instance: &str) -> TestResult {
        let last_seen = self.clock.wall();
        self.snapshot.workers.push(WorkerState {
            id: worker_id(instance)?,
            fleet,
            running_actions: 0,
            max_inflight_actions: MAX_INFLIGHT_ACTIONS,
            paused: false,
            draining: false,
            last_seen,
        });
        Ok(())
    }

    /// Removes the worker running on `instance` from the scheduler.
    pub fn disconnect(&mut self, instance: &str) -> TestResult {
        let id = worker_id(instance)?;
        self.snapshot.workers.retain(|worker| worker.id != id);
        Ok(())
    }

    /// Sets how many actions the worker on `instance` is running.
    pub fn set_running(&mut self, instance: &str, running: u32) -> TestResult {
        self.worker_mut(instance)?.running_actions = running;
        Ok(())
    }

    /// Sets whether the scheduler stops assigning new actions to the worker on `instance`.
    pub fn set_draining(&mut self, instance: &str, draining: bool) -> TestResult {
        self.worker_mut(instance)?.draining = draining;
        Ok(())
    }

    /// Replaces the reconciler with a fresh one, losing all in-process memory. The world state
    /// is kept.
    pub fn restart(&mut self) {
        let policies: Vec<_> = self.passes.iter().map(|pass| pass.policy.clone()).collect();
        self.reconciler = Reconciler::new(&policies);
    }

    /// Runs one pass and returns the steps it planned.
    pub fn reconcile(&mut self) -> Vec<PlanStep> {
        let mut scratch = self.snapshot.clone();
        // Tests connect workers in arbitrary order; the scheduler port returns them sorted.
        scratch.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        self.hosted.replace(&mut scratch);
        self.reconciler.reconcile(
            &mut self.passes,
            &self.hosted,
            PassTime::read(&self.clock),
            &mut self.plan,
        );
        self.plan.steps.clone()
    }

    /// Runs one pass, applies its steps, and returns them.
    pub fn run(&mut self) -> Vec<PlanStep> {
        let steps = self.reconcile();
        self.apply();
        steps
    }

    /// Performs the steps of the most recent plan the way the provisioner and scheduler would.
    ///
    /// * `ScaleOut` sets the desired capacity of the step's fleet to `to`. Instances are
    ///   launched by tests explicitly.
    /// * `StartDrain` marks the worker as draining.
    /// * `CancelDrain` clears the worker's draining flag.
    /// * `Terminate` removes the instance and its worker, removes the instance from the fleet's
    ///   supply, and lowers the fleet's desired capacity by one.
    pub fn apply(&mut self) {
        let steps = self.plan.steps.clone();
        for step in steps {
            match step.action {
                Action::ScaleOut { to, .. } => {
                    if let Some(pass) = self.passes.get_mut(step.fleet) {
                        pass.supply.desired = to;
                    }
                }
                Action::StartDrain(worker) => {
                    for state in &mut self.snapshot.workers {
                        if state.id == worker {
                            state.draining = true;
                        }
                    }
                }
                Action::CancelDrain(worker) => {
                    for state in &mut self.snapshot.workers {
                        if state.id == worker {
                            state.draining = false;
                        }
                    }
                }
                Action::Terminate { instance, .. } => {
                    self.snapshot
                        .workers
                        .retain(|worker| !instance.hosts(&worker.id));
                    if let Some(pass) = self.passes.get_mut(step.fleet) {
                        pass.supply.instances.retain(|entry| entry.id != instance);
                        pass.supply.desired = pass.supply.desired.saturating_sub(1);
                    }
                }
            }
        }
    }

    /// The provisioner's view of the fleet at index `fleet`.
    pub fn supply(&self, fleet: usize) -> TestResult<&FleetSupply> {
        self.passes
            .get(fleet)
            .map(|pass| &pass.supply)
            .ok_or_else(|| HarnessError::NoFleet(fleet).into())
    }

    /// The provisioner's mutable view of the fleet at index `fleet`.
    pub fn supply_mut(&mut self, fleet: usize) -> TestResult<&mut FleetSupply> {
        self.passes
            .get_mut(fleet)
            .map(|pass| &mut pass.supply)
            .ok_or_else(|| HarnessError::NoFleet(fleet).into())
    }

    /// The outlook the fleet at index `fleet` had in the latest pass.
    pub fn outlook(&self, fleet: usize) -> TestResult<FleetOutlook> {
        self.passes
            .get(fleet)
            .map(|pass| pass.outlook)
            .ok_or_else(|| HarnessError::NoFleet(fleet).into())
    }

    /// The audit event of every step of the most recent plan, stamped with the plan's time.
    pub fn audit_events(&self) -> TestResult<Vec<ScalingAuditEvent>> {
        let mut events = Vec::with_capacity(self.plan.steps.len());
        for step in &self.plan.steps {
            let policy = self
                .passes
                .get(step.fleet)
                .ok_or(HarnessError::NoFleet(step.fleet))?;
            events.push(step.audit_event(self.plan.at, &policy.policy.name));
        }
        Ok(events)
    }

    fn worker_mut(&mut self, instance: &str) -> TestResult<&mut WorkerState> {
        let id = worker_id(instance)?;
        self.snapshot
            .workers
            .iter_mut()
            .find(|worker| worker.id == id)
            .ok_or_else(|| HarnessError::UnknownWorker(instance.to_owned()).into())
    }
}
