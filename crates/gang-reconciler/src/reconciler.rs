//! The autoscaler's reconcile pass.
//!
//! A pass reads the scheduler's queue and workers and each fleet's provisioner supply, and returns
//! the steps that resize, resume, drain, and terminate workers. It performs no I/O. The caller
//! applies the steps in order; see [`Plan`] for why each step may be repeated.
//!
//! Release order is set by [`FleetInstance::release_first`]: when the fleet has a surplus, its
//! release-first instances are drained before the others. When demand returns, the others are
//! resumed before the release-first ones.

use std::ops::Range;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::FleetPolicy;
use gang_spi::{FleetInstance, FleetSupply, SchedulerSnapshot, WorkerState};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_types::{InstanceId, InstanceLifecycle, SaturatingInto};

use crate::activity::Activity;
use crate::demand::place;
use crate::fleet::{FleetOutlook, FleetPass, WorkerCounts};
use crate::hosted_snapshot::HostedSnapshot;
use crate::observations::{Observations, Seen};
use crate::pass_time::PassTime;
use crate::plan::{Action, NeedCause, Plan, PlanStep, TerminationCause};
use crate::wait::{Capacity, WorkPlanner};

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
    stale: bool,
    release_first: bool,
}

/// What an instance contributes to the fleet's capacity in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// Capacity: a serving, idle or booting worker, or an instance not yet an orphan.
    Capacity,
    /// A live obsolete instance not ordered to terminate.
    Stale,
    /// A live obsolete instance ordered to terminate, including this pass.
    TerminatingStale,
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
    /// Whether a live instance cannot serve the current launch environment.
    stale_live: bool,
    /// Stale instances not already ordered to terminate, including this pass.
    unterminated_stale: u32,
    /// The release-first instances that count as capacity.
    release_first: u32,
}

#[derive(Debug, Default)]
struct FleetCensus {
    census: Census,
    candidates: Range<usize>,
    draining: Range<usize>,
    orphans: usize,
}

/// One fleet's inputs for the pass: its index in the `fleets` slice, its policy, its provisioner
/// supply, the instances needed after applying the minimum in force, and what sets that need.
#[derive(Debug, Clone, Copy)]
struct FleetView<'a> {
    /// The position of the fleet in the slice passed to [`Reconciler::reconcile`].
    index: usize,
    policy: &'a FleetPolicy,
    supply: &'a FleetSupply,
    needed: u32,
    need: NeedCause,
}

/// The inputs shared by every fleet in one pass.
#[derive(Debug, Clone, Copy)]
struct Pass<'a> {
    hosted: &'a HostedSnapshot,
    wall: Timestamp,
    now: Tick,
    /// Whether [`CLIENT_REROUTE_BOUND`] has passed since this reconciler's first pass, so the
    /// demand a lost leader's clients held has had time to reach this one.
    demand_settled: bool,
}

/// Reusable scratch state for [`Reconciler::reconcile`].
///
/// Create one per process with [`Reconciler::new`] and reuse it across passes so that
/// steady-state passes do not allocate. It also remembers what it has observed between passes
/// (see the observations module). Nothing persists across a restart, so a restarted reconciler
/// starts every timer from zero and only acts on a worker or instance it has seen for a full
/// timeout in the current process. One reconciler serves one leadership term: its first pass
/// starts the term's demand settle.
#[derive(Debug)]
pub struct Reconciler {
    /// Since when each worker has shown its current activity, since when each instance has been
    /// in service without a worker, and any termination ordered for it.
    observations: Observations,
    /// The serving workers of the fleet being reconciled that may be drained.
    candidates: Vec<Candidate>,
    /// The draining workers of the fleet being reconciled.
    draining: Vec<Draining>,
    /// The tick of this reconciler's first pass; `None` until it has run one.
    first_pass: Option<Tick>,
    first_wall: Option<Timestamp>,
    planner: WorkPlanner,
    capacities: Vec<Capacity>,
    census: Vec<FleetCensus>,
    readiness: Vec<Option<Duration>>,
    stream: Vec<bool>,
    orphan_plan: Plan,
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
        let mut observations = Observations::new(fleets.len());
        observations.reserve_readiness(fleets.iter().map(|p| p.instances.max().get() as usize));
        Self {
            observations,
            candidates: Vec::with_capacity(capacity),
            draining: Vec::with_capacity(capacity),
            first_pass: None,
            first_wall: None,
            planner: WorkPlanner::new(fleets),
            capacities: fleets
                .iter()
                .map(|p| Capacity {
                    live: 0,
                    standing: 0,
                    reliable: 0,
                    draining: 0,
                    instances: Vec::with_capacity(p.instances.max().get() as usize),
                })
                .collect(),
            census: (0..fleets.len()).map(|_| FleetCensus::default()).collect(),
            readiness: vec![None; fleets.len()],
            stream: vec![false; fleets.len()],
            orphan_plan: Plan::new(),
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
    /// the work's need raised to the minimum in force, plus that minimum, demand, queued actions,
    /// candidate actions, and worker counts. Steps that add capacity carry a [`NeedCause`].
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
    /// * When the fleet needs more capacity than remains, draining workers are resumed first,
    ///   busiest first, then the workers whose instance is not `release_first`, and the fleet is
    ///   scaled out only for what resuming cannot cover. When capacity is in surplus, failed
    ///   unlaunched requests are withdrawn first; requests with a launch in flight are retained.
    ///   The remaining surplus is released from the `release_first` tier first: its longest-idle
    ///   eligible workers past `idle_timeout` drain before other eligible workers.
    /// * No drain starts until [`CLIENT_REROUTE_BOUND`] has passed since the reconciler's first
    ///   pass. Demand queued on a lost leader reaches the new one only once its clients have
    ///   detected the loss and resubmitted, so an idle worker is not surplus before then. Drains
    ///   already started still resume and end, and scale-out and withdrawals are unaffected.
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
        self.first_wall.get_or_insert(wall);
        let first_pass = *self.first_pass.get_or_insert(now);
        let demand_settled = now.saturating_duration_since(first_pass) >= CLIENT_REROUTE_BOUND;
        let snapshot = hosted.snapshot();
        plan.steps.clear();
        plan.at = wall;

        self.observations.begin_pass();
        self.planner.begin_pass();
        self.candidates.clear();
        self.draining.clear();
        self.stream.fill(false);
        for capacity in &mut self.capacities {
            capacity.instances.clear();
        }
        for fleet in fleets.iter_mut() {
            fleet.outlook = FleetOutlook::default();
            fleet.past_boot_grace.clear();
            fleet
                .past_boot_grace
                .resize(fleet.supply.instances.len(), false);
        }

        self.collect_demand(fleets, snapshot);

        let pass = Pass {
            hosted,
            wall,
            now,
            demand_settled,
        };
        let mut orphan_plan = std::mem::take(&mut self.orphan_plan);
        self.classify_all(fleets, &pass, &mut orphan_plan);
        self.planner.plan(
            fleets,
            hosted,
            time,
            &self.capacities,
            &self.readiness,
            &self.stream,
        );
        let mut orphans = orphan_plan.steps.drain(..);
        for (index, fleet) in fleets.iter_mut().enumerate() {
            let work = self.planner.work(index);
            let need = if work < fleet.outlook.needed {
                NeedCause::Minimum
            } else {
                NeedCause::Demand
            };
            let view = FleetView {
                index,
                policy: &fleet.policy,
                supply: &fleet.supply,
                needed: fleet.outlook.needed,
                need,
            };
            let planned = plan.steps.len();
            plan.steps
                .extend(orphans.by_ref().take(self.census[index].orphans));
            self.reconcile_fleet(&pass, &view, planned, plan);
        }
        drop(orphans);
        self.orphan_plan = orphan_plan;

        self.observations.finish_pass();
    }

    fn collect_demand(&mut self, fleets: &mut [FleetPass], snapshot: &SchedulerSnapshot) {
        for worker in &snapshot.workers {
            let activity = Activity::from(worker);
            if let Some((index, fleet)) = fleets
                .iter_mut()
                .enumerate()
                .find(|(_, fleet)| fleet.policy.name == worker.fleet)
            {
                let counts = &mut fleet.outlook.workers;
                let count = match activity {
                    Activity::Busy => &mut counts.busy,
                    Activity::Idle => &mut counts.idle,
                    Activity::Draining => &mut counts.draining,
                };
                *count = count.saturating_add(1);
                counts.running = counts.running.saturating_add(worker.running_actions);
                if activity != Activity::Draining
                    && worker.environment.as_ref() == Some(&fleet.policy.action_environment)
                {
                    self.stream[index] = true;
                    fleet.outlook.demand.add_running(worker.running_actions);
                }
            }
        }
        for action in &snapshot.queued {
            let mut matching = gang_types::FleetSet::EMPTY;
            for index in action.candidates {
                if let Some(fleet) = fleets.get_mut(index) {
                    fleet.outlook.candidates = fleet.outlook.candidates.saturating_add(1);
                    if fleet.policy.action_environment == action.environment {
                        matching.insert(index);
                    }
                }
            }
            let chosen = place(matching, action.resources, |index| {
                fleets
                    .get(index)
                    .filter(|fleet| fleet.policy.action_environment == action.environment)
                    .map(|fleet| (&fleet.policy, fleet.outlook.demand))
            });
            self.planner.attribute(chosen, matching);
            if let Some(fleet) = chosen.and_then(|index| fleets.get_mut(index)) {
                fleet.outlook.demand.add_queued(action.resources);
                fleet.outlook.queued = fleet.outlook.queued.saturating_add(1);
            }
        }
    }

    /// Classifies every fleet before planning so the simulation sees the whole census.
    fn classify_all(&mut self, fleets: &mut [FleetPass], pass: &Pass<'_>, orphan_plan: &mut Plan) {
        orphan_plan.steps.clear();
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.minimum = fleet.policy.instances.min_at(pass.wall);
            let view = FleetView {
                index,
                policy: &fleet.policy,
                supply: &fleet.supply,
                needed: 0,
                need: NeedCause::Demand,
            };
            let candidate_start = self.candidates.len();
            let draining_start = self.draining.len();
            let orphan_start = orphan_plan.steps.len();
            let census = self.classify(
                pass,
                &view,
                &mut fleet.past_boot_grace,
                &mut fleet.outlook.workers,
                orphan_plan,
            );
            self.capacities[index].live = census.live;
            self.capacities[index].standing = census
                .capacity
                .saturating_add(fleet.supply.desired.saturating_sub(census.live));
            self.capacities[index].reliable = if fleet.supply.launch_failure.is_some() {
                census.capacity
            } else {
                self.capacities[index].standing
            };
            self.capacities[index].draining = self.draining[draining_start..]
                .iter()
                .filter(|draining| !draining.stale)
                .count()
                .saturating_into();
            self.census[index] = FleetCensus {
                census,
                candidates: candidate_start..self.candidates.len(),
                draining: draining_start..self.draining.len(),
                orphans: orphan_plan.steps.len() - orphan_start,
            };
            let readiness = self.observations.readiness(index);
            self.readiness[index] = readiness;
            self.stream[index] = self.observations.stream(
                index,
                fleet.outlook.queued > 0 && self.stream[index],
                pass.now,
                readiness.unwrap_or(fleet.policy.boot_grace),
            );
        }
    }

    /// Reports that the termination ordered for `instance` was not applied, so the next pass
    /// plans it again instead of waiting out `boot_grace`. A no-op for an instance without an
    /// outstanding order. `fleet` is the step's fleet index.
    pub fn forget_order(&mut self, fleet: usize, instance: &InstanceId) {
        self.observations.forget_order(fleet, instance);
    }

    /// Plans the steps for one fleet and appends them to `plan`.
    ///
    /// The steps begin with orphan terminations and stale idle drains in supply order, then
    /// [`Action::CancelDrain`] for the resumed workers, then terminations of finished drains,
    /// then a withdrawal of abandoned requests, then either one [`Action::ScaleOut`] or a
    /// possible surplus [`Action::Withdraw`] followed by the [`Action::StartDrain`]s.
    ///
    /// Capacity is counted forward: instances classified as capacity (serving, idle, booting, or
    /// not yet an orphan), plus instances requested but not yet listed by the provisioner
    /// (`supply.desired` less listed instances that are not terminating), plus draining workers
    /// resumed this pass. Orphans ordered to terminate, instances with outstanding termination
    /// orders, and draining workers not resumed are not capacity even while still listed.
    /// Connected instances serving only an obsolete environment are not launch capacity.
    /// Idle stale instances drain regardless of minimum; busy ones finish without interruption.
    /// Stale drains are never resumed, and replacement waits for room under the fleet maximum.
    ///
    /// Resuming comes before scaling out. When the fleet needs more capacity than remains, only
    /// the shortfall is resumed: the busiest draining worker first, then a worker whose instance is
    /// not `release_first`, then by worker id. Capacity that is already running serves again
    /// before a new instance boots. If the fleet is still short, it is scaled out to the work's
    /// need raised to the minimum in force. Both steps carry the [`NeedCause`] for that need.
    /// Failed launches' unborn requests do not count when deciding how many workers to resume.
    ///
    /// Abandoned requests are withdrawn at the desired capacity after terminations. On a launch
    /// failure, at most the surplus of the instances requested but not yet listed is withdrawn
    /// separately. In-flight requests are never withdrawn.
    /// The remaining surplus is released in two tiers. Let `R` be the release-first instances
    /// that count as capacity, busy and booting ones included, `Ce` the eligible release-first
    /// candidates, and `Cs` the other eligible candidates, each sorted longest idle first, then
    /// by worker id. The first `min(surplus, |Ce|)` of `Ce` drain, then the first
    /// `min(surplus - R, |Cs|)` of `Cs`, so a worker that is not release-first drains only for
    /// the surplus that the release-first tier cannot absorb. With no release-first instance `R`
    /// is zero and the surplus drains the longest-idle candidates, ties broken by worker id. Only
    /// idle workers past `idle_timeout` are candidates, so a busy worker is never drained for
    /// scale-in. No drain starts before the pass's demand has settled.
    fn reconcile_fleet(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        planned: usize,
        plan: &mut Plan,
    ) {
        let Census {
            release_first,
            stale_live,
            unterminated_stale,
            ..
        } = self.census[view.index].census;
        let capacity = &self.capacities[view.index];
        let live = capacity.live;
        let standing = capacity.standing;
        let reliable = capacity.reliable;
        let draining = capacity.draining;
        let range = self.census[view.index].draining.clone();

        let workers = &pass.hosted.snapshot().workers;
        self.draining[range].sort_unstable_by(|a, b| {
            a.stale
                .cmp(&b.stale)
                .then_with(|| b.running.cmp(&a.running))
                .then_with(|| a.release_first.cmp(&b.release_first))
                .then_with(|| workers[a.worker].id.cmp(&workers[b.worker].id))
        });

        // Instances the provisioner was asked for but has not launched yet.
        let unborn = view.supply.desired.saturating_sub(live);
        // The capacity before any draining worker is resumed comes from the census.
        let resume = view.needed.saturating_sub(reliable).min(draining);
        self.resume_and_terminate(pass, view, resume, plan);
        let terminated: u32 = plan.steps[planned..]
            .iter()
            .filter(|step| matches!(step.action, Action::Terminate { .. }))
            .count()
            .saturating_into();
        let from = view.supply.desired.saturating_sub(terminated);
        if view.supply.abandoned > 0 {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::Withdraw {
                    from: from.saturating_add(view.supply.abandoned),
                    to: from,
                },
            });
        }
        let capacity = standing.saturating_add(resume);
        let to = if stale_live {
            view.needed
                .saturating_add(unterminated_stale)
                .min(from.saturating_add(view.policy.instances.max().get().saturating_sub(live)))
                .min(view.policy.instances.max().get())
        } else {
            view.needed
        };
        if view.needed > capacity
            && (!stale_live || (to > from && live < view.policy.instances.max().get()))
        {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::ScaleOut {
                    from: capacity,
                    to,
                    need: view.need,
                },
            });
        } else if capacity >= view.needed {
            let mut surplus = capacity - view.needed;
            let withdrawn = if view.supply.launch_failure.is_some() {
                unborn.min(surplus)
            } else {
                0
            };
            if withdrawn > 0 {
                plan.steps.push(PlanStep {
                    fleet: view.index,
                    action: Action::Withdraw {
                        from,
                        to: from.saturating_sub(withdrawn),
                    },
                });
                surplus = surplus.saturating_sub(withdrawn);
            }
            if pass.demand_settled {
                self.start_drains(pass, view, surplus, release_first, plan);
            }
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
            match self.classify_instance(
                pass,
                view,
                position,
                instance,
                (past_boot_grace, workers),
                plan,
            ) {
                Standing::Capacity => {
                    census.capacity += 1;
                    self.capacities[view.index].instances.push(position);
                    if instance.release_first {
                        census.release_first += 1;
                    }
                }
                Standing::Stale => {
                    census.stale_live = true;
                    census.unterminated_stale += 1;
                }
                Standing::TerminatingStale => census.stale_live = true,
                Standing::Other => {}
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
    /// Stale idle instances drain immediately; a drain with known stale identities ends only
    /// after its work finishes. Unknown-identity drains retain the normal timeout.
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
        let (activity, stale, known_stale) = self.hosted_activity(pass, view, instance);
        let unserved = matches!(activity, HostedActivity::Unconnected)
            && instance.lifecycle == InstanceLifecycle::InService;
        if unserved {
            counts.unconnected = counts.unconnected.saturating_add(1);
        }
        let seen = self
            .observations
            .observe_instance(view.index, &instance.id, unserved, pass.now);
        let launched_for = pass.wall.saturating_duration_since(instance.launched_at);
        if !matches!(activity, HostedActivity::Unconnected) {
            self.observe_startup(view.index, instance, launched_for);
        }
        let past_grace = unserved
            && launched_for >= policy.boot_grace
            && seen.unserved_since.is_some_and(|since| {
                pass.now.saturating_duration_since(since) >= policy.boot_grace
            });
        past_boot_grace[position] = past_grace;
        if let Some(at) = seen.ordered_at
            && pass.now.saturating_duration_since(at) < policy.boot_grace
        {
            return if stale {
                Standing::TerminatingStale
            } else {
                Standing::Other
            };
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
            HostedActivity::Busy if stale => Standing::Stale,
            HostedActivity::Busy => Standing::Capacity,
            HostedActivity::Idle { worker, .. } if stale => {
                plan.steps.push(PlanStep {
                    fleet: view.index,
                    action: Action::StartDrain(workers[worker].id.clone()),
                });
                Standing::Stale
            }
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
                let end = Self::drain_end(
                    stale,
                    known_stale,
                    running,
                    pass.now.saturating_duration_since(since),
                    policy.max_drain_wait,
                );
                self.draining.push(Draining {
                    worker,
                    instance: position,
                    running,
                    end,
                    stale,
                    release_first: instance.release_first,
                });
                if stale && end.is_some() {
                    Standing::TerminatingStale
                } else if stale {
                    Standing::Stale
                } else {
                    Standing::Other
                }
            }
        }
    }

    fn observe_startup(&mut self, fleet: usize, instance: &FleetInstance, launched_for: Duration) {
        let sample = self
            .first_wall
            .filter(|first| instance.launched_at >= *first)
            .map(|_| launched_for);
        self.observations
            .observe_connection(fleet, &instance.id, sample);
    }

    /// Selects the retirement cause without interrupting work on a known stale environment.
    fn drain_end(
        stale: bool,
        known_stale: bool,
        running: u32,
        elapsed: Duration,
        max_drain_wait: Duration,
    ) -> Option<TerminationCause> {
        if running == 0 {
            Some(if stale {
                TerminationCause::StaleEnvironment
            } else {
                TerminationCause::DrainFinished
            })
        } else if !known_stale && elapsed >= max_drain_wait {
            Some(TerminationCause::MaxDrainWait)
        } else {
            None
        }
    }

    fn hosted_activity(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        instance: &FleetInstance,
    ) -> (HostedActivity, bool, bool) {
        let workers = &pass.hosted.snapshot().workers;
        let mut hosted = Hosted::default();
        let mut has_worker = false;
        let mut stale = true;
        let mut known_stale = true;
        for index in pass.hosted.workers_on(&view.policy.name, &instance.id) {
            has_worker = true;
            stale &= workers[index].environment.as_ref() != Some(&view.policy.action_environment);
            known_stale &= workers[index]
                .environment
                .as_ref()
                .is_some_and(|environment| environment != &view.policy.action_environment);
            let seen = self.observations.observe_worker(&workers[index], pass.now);
            hosted.add(index, seen, workers);
        }
        (
            hosted.activity(),
            stale && has_worker,
            known_stale && has_worker,
        )
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
        let draining = &self.draining[self.census[view.index].draining.clone()];
        for draining in &draining[..resumed] {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::CancelDrain(workers[draining.worker].id.clone(), view.need),
            });
        }

        for draining in &draining[resumed..] {
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
        let candidates = &mut self.candidates[self.census[view.index].candidates.clone()];
        candidates.sort_unstable_by(|a, b| {
            b.release_first
                .cmp(&a.release_first)
                .then_with(|| a.since.cmp(&b.since))
                .then_with(|| workers[a.worker].id.cmp(&workers[b.worker].id))
        });
        let release_first_len = candidates.partition_point(|c| c.release_first);
        let first = surplus.min(release_first_len);
        let rest = surplus
            .saturating_sub(release_first_capacity)
            .min(candidates.len() - release_first_len);
        let drained = candidates[..first]
            .iter()
            .chain(&candidates[release_first_len..release_first_len + rest]);
        for candidate in drained {
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::StartDrain(workers[candidate.worker].id.clone()),
            });
        }
    }
}
