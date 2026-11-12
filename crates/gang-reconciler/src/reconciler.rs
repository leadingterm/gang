//! The autoscaler's reconcile pass.
//!
//! A pass reads the scheduler's queue and workers and each fleet's provisioner supply, and returns
//! the steps that resize, resume, drain, and terminate workers. It performs no I/O. The caller
//! applies the steps in order; see [`Plan`] for why each step may be repeated.
//!
//! Release order is set by [`FleetInstance::release_first`]: when the fleet has a surplus, its
//! release-first instances are drained before the others. When demand returns, the others are
//! resumed before the release-first ones.

use gang_clock::{Tick, Timestamp};
use gang_config::FleetPolicy;
use gang_spi::{FleetInstance, FleetSupply, WorkerState};
use gang_types::{InstanceId, InstanceLifecycle, SaturatingInto};

use crate::activity::Activity;
use crate::demand::place;
use crate::fleet::{FleetOutlook, FleetPass, WorkerCounts};
use crate::hosted_snapshot::HostedSnapshot;
use crate::observations::{Observations, Seen};
use crate::pass_time::PassTime;
use crate::plan::{Action, Plan, PlanStep, TerminationCause};

/// A serving worker that may be drained, with the tick since which it has been idle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    since: Tick,
    worker: usize,
    /// Whether the instance is released before the others (`FleetInstance::release_first`).
    release_first: bool,
}

/// Activities observed across all workers hosted by one instance.
#[derive(Debug, Default, Clone, Copy)]
struct Hosted {
    /// Whether any worker is busy.
    busy: bool,
    /// The latest idle start and the newest idle connection's index.
    idle: Option<(Tick, usize)>,
    /// The latest drain start and the newest draining connection's index.
    draining: Option<(Tick, usize)>,
    /// The saturating sum of running actions on draining workers.
    running: u32,
}

/// The aggregate activity of an instance's hosted workers.
#[derive(Debug, Clone, Copy)]
enum HostedActivity {
    /// No worker is connected.
    Unconnected,
    /// At least one worker is busy.
    Busy,
    /// No worker is busy and at least one is idle.
    Idle {
        /// The latest idle start.
        since: Tick,
        /// The newest idle connection's index.
        worker: usize,
    },
    /// Every worker is draining.
    Draining {
        /// The latest drain start.
        since: Tick,
        /// The newest draining connection's index.
        worker: usize,
        /// The saturating sum of running actions.
        running: u32,
    },
}

impl Hosted {
    /// Adds one observed worker to the aggregate.
    fn add(&mut self, index: usize, seen: Seen, workers: &[WorkerState]) {
        let group = match seen.activity {
            Activity::Busy => {
                self.busy = true;
                return;
            }
            Activity::Idle => &mut self.idle,
            Activity::Draining => {
                self.running = self.running.saturating_add(workers[index].running_actions);
                &mut self.draining
            }
        };
        *group = Some(match *group {
            Some((since, target)) => (since.max(seen.since), newest(index, target, workers)),
            None => (seen.since, index),
        });
    }

    /// Classifies the aggregate, giving busy workers precedence over idle and draining workers.
    fn activity(self) -> HostedActivity {
        match (self.busy, self.idle, self.draining) {
            (true, _, _) => HostedActivity::Busy,
            (false, Some((since, worker)), _) => HostedActivity::Idle { since, worker },
            (false, None, Some((since, worker))) => HostedActivity::Draining {
                since,
                worker,
                running: self.running,
            },
            (false, None, None) => HostedActivity::Unconnected,
        }
    }
}

/// Returns the index of the connection with the greatest last-seen time and worker id.
fn newest(a: usize, b: usize, workers: &[WorkerState]) -> usize {
    if (workers[a].last_seen, &workers[a].id) > (workers[b].last_seen, &workers[b].id) {
        a
    } else {
        b
    }
}

/// An instance classified from all its workers: busy if any is busy, idle if none is busy and
/// one is idle, and draining only when all drain.
///
/// `worker` indexes the newest connection and `instance` the fleet's instances. `end` is set
/// when none of its workers runs an action, or when all have drained for `max_drain_wait`.
/// `StartDrain` and `CancelDrain` target the newest connection in the applicable activity group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Draining {
    worker: usize,
    instance: usize,
    /// The actions its instance's workers run.
    running: u32,
    end: Option<TerminationCause>,
    /// Whether the instance is released before the others (`FleetInstance::release_first`).
    release_first: bool,
}

/// What an instance contributes to the fleet's capacity in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// Capacity: a serving, idle or booting worker, or an instance not yet an orphan.
    Capacity,
    /// Not capacity: an orphan ordered to terminate, a draining worker, or an instance with an
    /// outstanding termination order.
    Other,
}

/// The fleet's live instances, classified capacity, and release-first capacity.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Census {
    /// The fleet's instances that are not terminating.
    live: u32,
    /// The live instances classified as capacity.
    capacity: u32,
    /// The release-first instances that count as capacity.
    release_first: u32,
}

/// One fleet's inputs for the pass: its index in the `fleets` slice, its policy, its provisioner
/// supply, and the instances its demand needs.
#[derive(Debug, Clone, Copy)]
struct FleetView<'a> {
    /// The position of the fleet in the slice passed to [`Reconciler::reconcile`].
    index: usize,
    policy: &'a FleetPolicy,
    supply: &'a FleetSupply,
    needed: u32,
}

/// The inputs shared by every fleet in one pass.
#[derive(Debug, Clone, Copy)]
struct Pass<'a> {
    hosted: &'a HostedSnapshot,
    wall: Timestamp,
    now: Tick,
}

/// Reusable scratch state for [`Reconciler::reconcile`].
///
/// Create one per process with [`Reconciler::new`] and reuse it across passes so that
/// steady-state passes do not allocate. It also remembers what it has observed between passes
/// (see the observations module). Nothing persists across a restart, so a restarted reconciler
/// starts every timer from zero and only acts on a worker or instance it has seen for a full
/// timeout in the current process.
#[derive(Debug)]
pub struct Reconciler {
    /// Since when each worker has shown its current activity, since when each instance has been
    /// in service without a worker, and any termination ordered for it.
    observations: Observations,
    /// The serving workers of the fleet being reconciled that may be drained.
    candidates: Vec<Candidate>,
    /// The draining workers of the fleet being reconciled.
    draining: Vec<Draining>,
}

impl Reconciler {
    /// Sizes the scratch buffers for `fleets`.
    ///
    /// The instance buffers are sized for the sum of the fleets' `max_instances`.
    #[must_use]
    pub fn new(fleets: &[FleetPolicy]) -> Self {
        let capacity = fleets
            .iter()
            .map(|policy| policy.instances.max().get() as usize)
            .sum();
        Self {
            observations: Observations::new(fleets.len()),
            candidates: Vec::with_capacity(capacity),
            draining: Vec::with_capacity(capacity),
        }
    }

    /// Runs one deterministic pass and clears and refills `plan` with its steps.
    /// Uses `time` for all pass decisions and the plan's audit time.
    ///
    /// Each non-draining worker adds its running actions to the demand of the fleet with its
    /// name, and workers of unknown fleets are ignored. Each queued action is placed on one of its
    /// candidate fleets: the first candidate whose demand still fits once the action is added, or
    /// else the last candidate that names a fleet.
    ///
    /// The outlook of each fleet is written to its `fleets[i].outlook`: its needed instances,
    /// demand, queued actions, candidate actions, and worker counts.
    ///
    /// The pass classifies every instance that is not terminating, then plans each fleet:
    ///
    /// * An instance is busy if any worker is busy, idle if none is busy and one is idle, and
    ///   draining only if all workers drain. A drain ends when none runs an action or all have
    ///   drained for `max_drain_wait`. `StartDrain` and `CancelDrain` target the newest connection.
    /// * An instance with no connected worker is an orphan only when it is `InService` and both
    ///   clocks agree it has gone unserved for `boot_grace`. A `Pending` instance is capacity and
    ///   is never an orphan, however long it waits. The wall clock is read from the launch time,
    ///   which the provisioner supplies and which survives a restart. The monotonic clock runs
    ///   from when this process first saw the instance in service without a worker, which a
    ///   restart resets. Either clock alone would terminate a healthy instance straight after a
    ///   restart, while its worker is still reconnecting.
    /// * An instance whose termination was ordered less than `boot_grace` ago has an outstanding
    ///   order. It is neither capacity nor a drain candidate, and it is not ordered again. Once
    ///   `boot_grace` has passed, the order counts as lost and the termination is issued again.
    /// * Capacity is counted forward: instances classified as capacity (serving, idle, booting,
    ///   or not yet an orphan), plus instances requested but not yet listed by the provisioner
    ///   (`supply.desired` less listed instances that are not terminating), plus draining workers
    ///   resumed this pass. Orphans ordered to terminate, instances with outstanding termination
    ///   orders, and draining workers not resumed are not capacity even while still listed.
    /// * When demand needs more capacity than remains, draining workers are resumed first,
    ///   busiest first, then the workers whose instance is not `release_first`, and the fleet is
    ///   scaled out only for what resuming cannot cover. When capacity is in surplus, the surplus
    ///   is released from the `release_first` tier first: its longest-idle eligible workers past
    ///   `idle_timeout` are drained, and the other eligible workers are drained only for the
    ///   surplus that tier cannot absorb (see [`Reconciler::reconcile_fleet`]).
    ///
    /// Every step is safe to apply again. A pass that runs before the provisioner or scheduler
    /// has reflected the previous pass plans the same steps again, so the caller converges
    /// without compounding them. The one exception is termination: an ordered termination is
    /// withheld until `boot_grace` has passed, so it is not repeated while it is in flight.
    /// A caller whose termination could not be applied reports it with
    /// [`Reconciler::forget_order`], so the next pass plans it again.
    pub fn reconcile(
        &mut self,
        fleets: &mut [FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        plan: &mut Plan,
    ) {
        let PassTime { wall, now } = time;
        let snapshot = hosted.snapshot();
        plan.steps.clear();
        plan.at = wall;

        self.observations.begin_pass();
        for fleet in fleets.iter_mut() {
            fleet.outlook = FleetOutlook::default();
            fleet.past_boot_grace.clear();
            fleet
                .past_boot_grace
                .resize(fleet.supply.instances.len(), false);
        }
        for worker in &snapshot.workers {
            let activity = Activity::from(worker);
            if let Some(fleet) = fleets
                .iter_mut()
                .find(|fleet| fleet.policy.name == worker.fleet)
            {
                let counts = &mut fleet.outlook.workers;
                let count = match activity {
                    Activity::Busy => &mut counts.busy,
                    Activity::Idle => &mut counts.idle,
                    Activity::Draining => &mut counts.draining,
                };
                *count = count.saturating_add(1);
                counts.running = counts.running.saturating_add(worker.running_actions);
                if activity != Activity::Draining {
                    fleet.outlook.demand.add_running(worker.running_actions);
                }
            }
        }
        for action in &snapshot.queued {
            for index in action.candidates {
                if let Some(fleet) = fleets.get_mut(index) {
                    fleet.outlook.candidates = fleet.outlook.candidates.saturating_add(1);
                }
            }
            let chosen = place(action.candidates, action.resources, |index| {
                fleets
                    .get(index)
                    .map(|fleet| (&fleet.policy, fleet.outlook.demand))
            });
            if let Some(fleet) = chosen.and_then(|index| fleets.get_mut(index)) {
                fleet.outlook.demand.add_queued(action.resources);
                fleet.outlook.queued = fleet.outlook.queued.saturating_add(1);
            }
        }

        let pass = Pass { hosted, wall, now };
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.needed = fleet.outlook.demand.instances(&fleet.policy);
            let view = FleetView {
                index,
                policy: &fleet.policy,
                supply: &fleet.supply,
                needed: fleet.outlook.needed,
            };
            self.reconcile_fleet(
                &pass,
                &view,
                &mut fleet.past_boot_grace,
                &mut fleet.outlook.workers,
                plan,
            );
        }

        self.observations.finish_pass();
    }

    /// Reports that the termination ordered for `instance` was not applied, so the next pass
    /// plans it again instead of waiting out `boot_grace`. A no-op for an instance without an
    /// outstanding order. `fleet` is the step's fleet index.
    pub fn forget_order(&mut self, fleet: usize, instance: &InstanceId) {
        self.observations.forget_order(fleet, instance);
    }

    /// Plans the steps for one fleet and appends them to `plan`.
    ///
    /// The steps are pushed in this order: orphan terminations in supply order, then
    /// [`Action::CancelDrain`] for the resumed workers, then terminations of finished drains,
    /// then either one [`Action::ScaleOut`] or the [`Action::StartDrain`]s.
    ///
    /// Capacity is counted forward: instances classified as capacity (serving, idle, booting, or
    /// not yet an orphan), plus instances requested but not yet listed by the provisioner
    /// (`supply.desired` less listed instances that are not terminating), plus draining workers
    /// resumed this pass. Orphans ordered to terminate, instances with outstanding termination
    /// orders, and draining workers not resumed are not capacity even while still listed.
    ///
    /// Resuming comes before scaling out. When demand needs more capacity than remains, only the
    /// shortfall is resumed: the busiest draining worker first, then a worker whose instance is
    /// not `release_first`, then by worker id. Capacity that is already running serves again
    /// before a new instance boots. If the fleet is still short, it is scaled out to the needed
    /// count, which `Demand::instances` caps at `max_instances`.
    ///
    /// Otherwise the surplus is released in two tiers. Let `R` be the release-first instances
    /// that count as capacity, busy and booting ones included, `Ce` the eligible release-first
    /// candidates, and `Cs` the other eligible candidates, each sorted longest idle first, then
    /// by worker id. The first `min(surplus, |Ce|)` of `Ce` drain, then the first
    /// `min(surplus - R, |Cs|)` of `Cs`, so a worker that is not release-first drains only for
    /// the surplus that the release-first tier cannot absorb. With no release-first instance `R`
    /// is zero and the surplus drains the longest-idle candidates, ties broken by worker id. Only
    /// idle workers past `idle_timeout` are candidates, so a busy worker is never drained for
    /// scale-in.
    fn reconcile_fleet(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        past_boot_grace: &mut [bool],
        workers: &mut WorkerCounts,
        plan: &mut Plan,
    ) {
        self.candidates.clear();
        self.draining.clear();
        let Census {
            live,
            capacity: serving,
            release_first,
        } = self.classify(pass, view, past_boot_grace, workers, plan);

        let workers = &pass.hosted.snapshot().workers;
        let draining: u32 = self.draining.len().saturating_into();
        self.draining.sort_unstable_by(|a, b| {
            b.running
                .cmp(&a.running)
                .then_with(|| a.release_first.cmp(&b.release_first))
                .then_with(|| workers[a.worker].id.cmp(&workers[b.worker].id))
        });

        // Instances the provisioner was asked for but has not launched yet.
        let unborn = view.supply.desired.saturating_sub(live);
        // The capacity before any draining worker is resumed.
        let standing = serving.saturating_add(unborn);
        let resume = view.needed.saturating_sub(standing).min(draining);
        self.resume_and_terminate(pass, view, resume, plan);
        let capacity = standing.saturating_add(resume);
        if view.needed > capacity {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::ScaleOut {
                    from: capacity,
                    to: view.needed,
                },
            });
        } else {
            self.start_drains(pass, view, capacity - view.needed, release_first, plan);
        }
    }

    /// Classifies every instance of one fleet that is not terminating. Terminates the orphans,
    /// collects the drain candidates and the draining workers, and counts live instances and
    /// capacity, including its release-first instances.
    fn classify(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        past_boot_grace: &mut [bool],
        workers: &mut WorkerCounts,
        plan: &mut Plan,
    ) -> Census {
        let mut census = Census::default();
        for (position, instance) in view.supply.instances.iter().enumerate() {
            if instance.lifecycle == InstanceLifecycle::Terminating {
                continue;
            }
            census.live += 1;
            if self.classify_instance(
                pass,
                view,
                position,
                instance,
                (past_boot_grace, workers),
                plan,
            ) == Standing::Capacity
            {
                census.capacity += 1;
                if instance.release_first {
                    census.release_first += 1;
                }
            }
        }
        census
    }

    /// Classifies one instance that is not terminating as capacity or not. `position` is the
    /// index of the instance in `view.supply.instances`.
    ///
    /// An instance with an outstanding termination order is skipped until `boot_grace` has passed.
    /// All hosted workers are observed: any busy worker makes the instance busy, otherwise any
    /// idle worker makes it idle, and only all draining workers make it draining. The newest
    /// connection in the relevant group is the `StartDrain` or `CancelDrain` target. An idle instance
    /// becomes a candidate after all idle workers have been idle for `idle_timeout`; a drain ends
    /// when no hosted worker runs an action or all have drained for `max_drain_wait`.
    /// An unconnected instance is an orphan only once it is `InService`, was launched at least
    /// `boot_grace` ago, and this process first saw it in service without a worker at least
    /// `boot_grace` ago by the monotonic clock.
    /// A `Pending` instance is never an orphan and counts as capacity.
    fn classify_instance(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        position: usize,
        instance: &FleetInstance,
        outlook: (&mut [bool], &mut WorkerCounts),
        plan: &mut Plan,
    ) -> Standing {
        let (past_boot_grace, counts) = outlook;
        let policy = view.policy;
        let workers = &pass.hosted.snapshot().workers;
        let mut hosted = Hosted::default();
        for w in pass.hosted.workers_on(&policy.name, &instance.id) {
            let seen = self.observations.observe_worker(&workers[w], pass.now);
            hosted.add(w, seen, workers);
        }
        let activity = hosted.activity();
        let unserved = matches!(activity, HostedActivity::Unconnected)
            && instance.lifecycle == InstanceLifecycle::InService;
        if unserved {
            counts.unconnected = counts.unconnected.saturating_add(1);
        }
        let seen = self
            .observations
            .observe_instance(view.index, &instance.id, unserved, pass.now);
        let launched_for = pass.wall.saturating_duration_since(instance.launched_at);
        let past_grace = unserved
            && launched_for >= policy.boot_grace
            && seen.unserved_since.is_some_and(|since| {
                pass.now.saturating_duration_since(since) >= policy.boot_grace
            });
        past_boot_grace[position] = past_grace;
        if let Some(at) = seen.ordered_at
            && pass.now.saturating_duration_since(at) < policy.boot_grace
        {
            return Standing::Other;
        }

        match activity {
            HostedActivity::Unconnected => {
                if !past_grace {
                    return Standing::Capacity;
                }
                plan.steps.push(PlanStep {
                    fleet: view.index,
                    action: Action::Terminate {
                        instance: instance.id.clone(),
                        cause: TerminationCause::Orphan,
                    },
                });
                self.observations
                    .order_termination(view.index, &instance.id, pass.now);
                Standing::Other
            }
            HostedActivity::Busy => Standing::Capacity,
            HostedActivity::Idle { since, worker } => {
                if pass.now.saturating_duration_since(since) >= policy.idle_timeout {
                    self.candidates.push(Candidate {
                        since,
                        worker,
                        release_first: instance.release_first,
                    });
                }
                Standing::Capacity
            }
            HostedActivity::Draining {
                since,
                worker,
                running,
            } => {
                let end = if running == 0 {
                    Some(TerminationCause::DrainFinished)
                } else if pass.now.saturating_duration_since(since) >= policy.max_drain_wait {
                    Some(TerminationCause::MaxDrainWait)
                } else {
                    None
                };
                self.draining.push(Draining {
                    worker,
                    instance: position,
                    running,
                    end,
                    release_first: instance.release_first,
                });
                Standing::Other
            }
        }
    }

    /// Resumes the first `resume` draining workers, and terminates the finished drains among the
    /// rest.
    fn resume_and_terminate(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        resume: u32,
        plan: &mut Plan,
    ) {
        let resumed: usize = resume.saturating_into();
        let workers = &pass.hosted.snapshot().workers;
        for draining in &self.draining[..resumed] {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::CancelDrain(workers[draining.worker].id.clone()),
            });
        }

        for draining in &self.draining[resumed..] {
            let Some(cause) = draining.end else {
                continue;
            };
            let instance = &view.supply.instances[draining.instance].id;
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::Terminate {
                    instance: instance.clone(),
                    cause,
                },
            });
            self.observations
                .order_termination(view.index, instance, pass.now);
        }
    }

    /// Drains the surplus from the two release tiers. The first `surplus` eligible release-first
    /// candidates drain, then the first `surplus - release_first_capacity` other eligible
    /// candidates. Each tier is sorted longest idle first, ties broken by worker id. See
    /// [`Reconciler::reconcile_fleet`].
    fn start_drains(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        surplus: u32,
        release_first_capacity: u32,
        plan: &mut Plan,
    ) {
        let surplus: usize = surplus.saturating_into();
        let release_first_capacity: usize = release_first_capacity.saturating_into();
        let workers = &pass.hosted.snapshot().workers;
        self.candidates.sort_unstable_by(|a, b| {
            b.release_first
                .cmp(&a.release_first)
                .then_with(|| a.since.cmp(&b.since))
                .then_with(|| workers[a.worker].id.cmp(&workers[b.worker].id))
        });
        let release_first_len = self.candidates.partition_point(|c| c.release_first);
        let first = surplus.min(release_first_len);
        let rest = surplus
            .saturating_sub(release_first_capacity)
            .min(self.candidates.len() - release_first_len);
        let drained = self.candidates[..first]
            .iter()
            .chain(&self.candidates[release_first_len..release_first_len + rest]);
        for candidate in drained {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::StartDrain(workers[candidate.worker].id.clone()),
            });
        }
    }
}
