//! Shared test harness: a simulated provisioner and scheduler around the [`Reconciler`].
//!
//! Integration tests pull this in with `mod support;`. The harness owns the world state (fleet
//! supply, scheduler snapshot, clock) and applies each plan the way the provisioner and the
//! scheduler would, so tests can drive several passes in sequence.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock, TestClock, Timestamp};
use gang_config::{ActionsConfig, FleetPolicy, ScalingConfig};
use gang_reconciler::{
    Action, FleetOutlook, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler,
    SourceProfiles,
};
use gang_spi::{
    ExecutingDemand, FleetInstance, FleetSupply, InstanceCordon, QueuedDemand, ScalingAuditEvent,
    SchedulerSnapshot, WorkerState,
};
use gang_testkit::{TestResult, worker_id};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_types::{ActionResources, Bytes, FleetName, FleetSet, InstanceId, InstanceLifecycle, Isa};

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
}

/// Concurrent actions a connected worker accepts in these tests.
const MAX_INFLIGHT_ACTIONS: Option<NonZeroU32> = NonZeroU32::new(4);

/// A fleet policy named `name` for `isa`, otherwise as [`gang_testkit::fleet_policy`].
///
/// # Errors
///
/// Returns the fixture construction error if the fleet name or environment is invalid.
pub fn policy_named(name: &str, isa: Isa) -> TestResult<FleetPolicy> {
    Ok(FleetPolicy {
        isa,
        ..gang_testkit::fleet_policy(name)?
    })
}

/// The x86-64 fleet policy used by the harness, named `gang-x86`.
///
/// # Errors
///
/// Returns the fixture construction error if the fleet name or environment is invalid.
pub fn policy() -> TestResult<FleetPolicy> {
    policy_named("gang-x86", Isa::X86_64)
}

/// Resources of an action requesting CPU in millicores, 1 GiB of memory and 1 GiB of disk.
#[must_use]
pub fn x86_resources(cpu_millicores: u64) -> ActionResources {
    ActionResources {
        cpu_millicores,
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
    /// Shared policy used by the adapter and reconciler in this modeled world.
    pub scaling: Arc<ScalingConfig>,
    /// Durable term profile handle, retained across model replacement.
    pub profiles: SourceProfiles,
}

impl Harness {
    /// A harness with one fleet built from `policy`.
    #[must_use]
    pub fn with_policy(policy: FleetPolicy) -> Self {
        Self::with_policy_at(policy, Timestamp::from_unix_millis(1_000_000))
    }

    /// A harness with one fleet and the given initial wall time.
    #[must_use]
    pub fn with_policy_at(policy: FleetPolicy, wall: Timestamp) -> Self {
        let mut harness = Self::with_fleets(vec![policy]);
        harness.clock = TestClock::new(wall);
        harness
    }

    /// A harness with the given fleets, each starting with an empty provisioner supply.
    #[must_use]
    pub fn with_fleets(fleets: Vec<FleetPolicy>) -> Self {
        Self::with_scaling(fleets, ScalingConfig::default())
    }

    /// Builds a deterministic world with explicitly configured rate policy.
    pub fn with_scaling(fleets: Vec<FleetPolicy>, scaling: ScalingConfig) -> Self {
        let scaling = Arc::new(scaling);
        let profiles = SourceProfiles::new(&scaling, ActionsConfig::default().history_window);
        let reconciler = Reconciler::new(&fleets, Arc::clone(&scaling), profiles.clone());
        let passes = fleets.into_iter().map(FleetPass::new).collect();
        Self {
            passes,
            snapshot: SchedulerSnapshot::default(),
            hosted: HostedSnapshot::default(),
            clock: TestClock::new(Timestamp::from_unix_millis(1_000_000)),
            plan: Plan::new(),
            reconciler,
            scaling,
            profiles,
        }
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&mut self, by: Duration) {
        self.clock.advance(by);
    }

    /// Queues `count` actions with no resource requests on the fleet at index `fleet`.
    pub fn queue(&mut self, fleet: usize, count: usize) -> TestResult {
        self.queue_sized(fleet, count, ActionResources::default())
    }

    /// Queues `count` actions with the given resources on the fleet at index `fleet`.
    pub fn queue_sized(
        &mut self,
        fleet: usize,
        count: usize,
        resources: ActionResources,
    ) -> TestResult {
        self.queue_candidates_sized(FleetSet::try_from_indices([fleet])?, count, resources);
        Ok(())
    }

    /// Queues `count` actions with no resource requests on any fleet in `candidates`.
    pub fn queue_candidates(&mut self, candidates: FleetSet, count: usize) {
        self.queue_candidates_sized(candidates, count, ActionResources::default());
    }

    fn queue_candidates_sized(
        &mut self,
        candidates: FleetSet,
        count: usize,
        resources: ActionResources,
    ) {
        let queued_since = self.clock.wall();
        for _ in 0..count {
            self.snapshot.queued.push(QueuedDemand {
                source: None,
                candidates,
                resources,
                queued_since,
                priority: 0,
                expected: None,
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

    /// Sets the provisioner's desired capacity for the fleet at index `fleet`. Lowering the
    /// request never removes live instances. Does nothing when there is no such fleet.
    pub fn set_desired_capacity(&mut self, fleet: usize, desired: u32) {
        if let Some(pass) = self.passes.get_mut(fleet) {
            let live = pass
                .supply
                .instances
                .iter()
                .filter(|instance| instance.lifecycle != InstanceLifecycle::Terminating)
                .count();
            pass.supply.desired = if desired < pass.supply.desired {
                desired.max(u32::try_from(live).unwrap_or(u32::MAX))
            } else {
                desired
            };
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
        let draining = self.snapshot.cordons.iter().any(|cordon| {
            instance == cordon.instance.as_str()
                && self
                    .passes
                    .get(cordon.fleet)
                    .is_some_and(|pass| pass.policy.name == fleet)
        });
        self.snapshot.workers.push(WorkerState {
            id: worker_id(instance)?,
            environment: Some(
                self.passes
                    .iter()
                    .find(|pass| pass.policy.name == fleet)
                    .unwrap_or(&self.passes[0])
                    .policy
                    .action_environment
                    .clone(),
            ),
            fleet,
            running_actions: 0,
            max_inflight_actions: MAX_INFLIGHT_ACTIONS,
            paused: false,
            draining,
            last_seen,
        });
        Ok(())
    }

    /// Removes the worker running on `instance` from the scheduler.
    pub fn disconnect(&mut self, instance: &str) -> TestResult {
        let id = worker_id(instance)?;
        self.retain_workers(|worker| worker.id != id);
        Ok(())
    }

    fn retain_workers(&mut self, mut keep: impl FnMut(&WorkerState) -> bool) {
        let mut indices = Vec::with_capacity(self.snapshot.workers.len());
        let mut next = 0;
        self.snapshot.workers.retain(|worker| {
            if keep(worker) {
                indices.push(Some(next));
                next += 1;
                true
            } else {
                indices.push(None);
                false
            }
        });
        self.snapshot
            .executing
            .retain_mut(|execution| match indices.get(execution.worker) {
                Some(Some(index)) => {
                    execution.worker = *index;
                    true
                }
                Some(None) => false,
                None => true,
            });
    }

    /// Sets how many actions the worker on `instance` is running.
    pub fn set_running(&mut self, instance: &str, running: u32) -> TestResult {
        let id = worker_id(instance)?;
        let worker = self
            .snapshot
            .workers
            .iter()
            .position(|worker| worker.id == id)
            .ok_or_else(|| HarnessError::UnknownWorker(instance.to_owned()))?;
        let fleet = self
            .passes
            .iter()
            .position(|pass| pass.policy.name == self.snapshot.workers[worker].fleet);
        self.snapshot.workers[worker].running_actions = running;
        self.snapshot.executing.retain(|row| row.worker != worker);
        let candidates = match fleet {
            Some(fleet) => FleetSet::try_from_indices([fleet])?,
            None => FleetSet::EMPTY,
        };
        self.snapshot
            .executing
            .extend((0..running).map(|_| ExecutingDemand {
                source: None,
                candidates,
                resources: ActionResources::default(),
                worker,
                started: self.clock.wall(),
                expected: None,
            }));
        Ok(())
    }

    /// Sets whether the scheduler stops assigning new actions to the worker on `instance`.
    pub fn set_draining(&mut self, instance: &str, draining: bool) -> TestResult {
        let id = InstanceId::try_new(instance)?;
        let fleet_name = self
            .snapshot
            .workers
            .iter()
            .find(|worker| id.hosts(&worker.id))
            .map(|worker| worker.fleet.clone())
            .ok_or_else(|| HarnessError::UnknownWorker(instance.to_owned()))?;
        let fleet = self
            .passes
            .iter()
            .position(|pass| pass.policy.name == fleet_name)
            .ok_or(HarnessError::NoFleet(usize::MAX))?;
        self.snapshot
            .cordons
            .retain(|cordon| cordon.fleet != fleet || *cordon.instance != id);
        if draining {
            self.snapshot.cordons.push(InstanceCordon {
                fleet,
                instance: Arc::new(id.clone()),
            });
            self.snapshot
                .cordons
                .sort_unstable_by(|a, b| (a.fleet, &a.instance).cmp(&(b.fleet, &b.instance)));
        }
        for worker in &mut self.snapshot.workers {
            if worker.fleet == fleet_name && id.hosts(&worker.id) {
                worker.draining = draining;
            }
        }
        Ok(())
    }

    /// Replaces the reconciler with a fresh one, losing all in-process memory. The world state
    /// is kept.
    pub fn restart(&mut self) {
        let policies: Vec<_> = self.passes.iter().map(|pass| pass.policy.clone()).collect();
        self.reconciler =
            Reconciler::new(&policies, Arc::clone(&self.scaling), self.profiles.clone());
    }

    /// Runs the first pass of the reconciler's leadership term, then moves the clock past
    /// [`CLIENT_REROUTE_BOUND`], after which the term may start drains. The pass's steps are not
    /// applied, so it must order no termination: the reconciler remembers an ordered termination
    /// and does not plan it again within `boot_grace`. Such a pass is an error.
    pub fn settle_term(&mut self) -> TestResult {
        let steps = self.reconcile();
        if steps
            .iter()
            .any(|step| matches!(step.action, Action::Terminate { .. }))
        {
            return Err(format!("the settling pass ordered a termination: {steps:?}").into());
        }
        self.advance(CLIENT_REROUTE_BOUND);
        Ok(())
    }

    /// Runs one pass and returns the steps it planned.
    pub fn reconcile(&mut self) -> Vec<PlanStep> {
        let mut scratch = self.snapshot.clone();
        // Tests connect workers in arbitrary order; the scheduler port returns them sorted.
        let mut workers: Vec<_> = scratch.workers.drain(..).enumerate().collect();
        workers.sort_unstable_by(|a, b| a.1.id.cmp(&b.1.id));
        let mut sorted_index = vec![0; workers.len()];
        for (new, (old, worker)) in workers.into_iter().enumerate() {
            sorted_index[old] = new;
            scratch.workers.push(worker);
        }
        for execution in &mut scratch.executing {
            if let Some(&index) = sorted_index.get(execution.worker) {
                execution.worker = index;
            }
        }
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
    /// * `ScaleOut` and `Withdraw` set the desired capacity of the step's fleet to `to`.
    ///   Instances are launched by tests explicitly.
    /// * `StartDrain` marks the worker as draining.
    /// * `CancelDrain` clears the worker's draining flag.
    /// * `Terminate` removes the instance and its worker, removes the instance from the fleet's
    ///   supply, and lowers the fleet's desired capacity by one.
    #[allow(clippy::expect_used)] // An applied plan can name only a worker in this snapshot.
    pub fn apply(&mut self) {
        let steps = self.plan.steps.clone();
        for step in steps {
            match step.action {
                Action::ScaleOut { to, .. } | Action::Withdraw { to, .. } => {
                    self.set_desired_capacity(step.fleet, to);
                }
                Action::StartDrain { worker, .. } | Action::ReinforceDrain(worker) => {
                    if let Some(instance) = worker.instance_name() {
                        self.set_draining(instance, true)
                            .expect("planned worker is connected");
                    }
                }
                Action::CancelDrain(worker, _) => {
                    if let Some(instance) = worker.instance_name() {
                        self.set_draining(instance, false)
                            .expect("planned worker is connected");
                    }
                }
                Action::Terminate { instance, .. } => {
                    self.retain_workers(|worker| !instance.hosts(&worker.id));
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
            if let Some(event) = step.audit_event(self.plan.at, &policy.policy.name) {
                events.push(event);
            }
        }
        Ok(events)
    }
}
