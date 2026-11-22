//! Pure, deterministic rate-based fleet reconciliation.
//!
//! Capacity is counted per physical instance. Controller cordon intent is authoritative;
//! connection-local self drains never grant the controller permission to resume a worker.

use std::num::NonZeroU32;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::{FleetPolicy, ScalingConfig};
use gang_spi::{FleetInstance, FleetSupply, TerminationMode, WorkerState};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_types::{InstanceId, InstanceLifecycle, SaturatingInto};

use crate::activity::Activity;
use crate::demand::place_with_credit;
use crate::fleet::{FleetOutlook, FleetPass, WorkerCounts};
use crate::hosted_snapshot::HostedSnapshot;
use crate::observations::{Observations, Seen};
use crate::pass_time::PassTime;
use crate::plan::{Action, DrainReason, NeedCause, Plan, PlanStep, TerminationCause};
use crate::profile::SourceProfiles;
use crate::vectors::worker_resources;
use crate::wait::{Capacity, WorkPlanner};

#[derive(Debug, Clone, Copy)]
struct Candidate {
    since: Tick,
    worker: usize,
    instance: usize,
    release_first: bool,
}

/// All connections contribute to physical occupancy, including partially applied cordons.
#[derive(Debug, Default, Clone, Copy)]
struct Hosted {
    worker: Option<usize>,
    idle_since: Option<Tick>,
    draining_since: Option<Tick>,
    serving: bool,
    busy: bool,
    running: u32,
}

impl Hosted {
    fn add(&mut self, index: usize, seen: Seen, workers: &[WorkerState]) {
        self.running = self.running.saturating_add(workers[index].running_actions);
        self.worker = Some(
            self.worker
                .map_or(index, |previous| newest(index, previous, workers)),
        );
        match seen.activity {
            Activity::Busy => {
                self.serving = true;
                self.busy = true;
            }
            Activity::Idle => {
                self.serving = true;
                self.idle_since = Some(
                    self.idle_since
                        .map_or(seen.since, |since| since.max(seen.since)),
                );
            }
            Activity::Draining => {
                self.draining_since = Some(
                    self.draining_since
                        .map_or(seen.since, |since| since.max(seen.since)),
                );
            }
        }
    }
}

fn newest(a: usize, b: usize, workers: &[WorkerState]) -> usize {
    if (workers[a].last_seen, &workers[a].id) > (workers[b].last_seen, &workers[b].id) {
        a
    } else {
        b
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainKind {
    Worker,
    Surplus,
    Replacement,
}

#[derive(Debug, Clone, Copy)]
struct Draining {
    worker: usize,
    instance: usize,
    running: u32,
    end: Option<TerminationCause>,
    release_first: bool,
    kind: DrainKind,
    resumed: bool,
}

#[derive(Debug, Clone, Copy)]
struct Serving {
    worker: usize,
    instance: usize,
    running: u32,
    release_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    Usable,
    Transient,
    Retained,
    Ordered,
}

#[derive(Debug, Default, Clone, Copy)]
struct Census {
    live: u32,
    usable: u32,
    transient: u32,
    serving: u32,
    release_first: u32,
    busy_cordons: u32,
    replacing: u32,
    unconnected_cordon: bool,
    handover_retained: u32,
}

#[derive(Debug, Default)]
struct FleetCensus {
    census: Census,
    candidates: Range<usize>,
    draining: Range<usize>,
    serving: Range<usize>,
    orphans: usize,
}

#[derive(Debug, Clone, Copy)]
struct FleetView<'a> {
    index: usize,
    policy: &'a FleetPolicy,
    supply: &'a FleetSupply,
    demand: crate::Demand,
    needed: u32,
    minimum: u32,
    ceiling: u32,
    need: NeedCause,
}

#[derive(Debug, Clone, Copy)]
struct Pass<'a> {
    hosted: &'a HostedSnapshot,
    wall: Timestamp,
    now: Tick,
    demand_settled: bool,
}

/// Reused scratch and term-local observations. Physical cordons belong to the scheduler engine,
/// not this model; replacing a model preserves intent but starts a fresh dwell and rate history.
#[derive(Debug)]
pub struct Reconciler {
    observations: Observations,
    candidates: Vec<Candidate>,
    draining: Vec<Draining>,
    serving: Vec<Serving>,
    /// Owned repair identifiers are moved between successive plans, not cloned every pass.
    repair_workers: Vec<gang_types::WorkerId>,
    first_pass: Option<Tick>,
    first_wall: Option<Timestamp>,
    scaling: Arc<ScalingConfig>,
    cpu_oversubscription: NonZeroU32,
    running_reservations: Vec<crate::Demand>,
    planner: WorkPlanner,
    capacities: Vec<Capacity>,
    census: Vec<FleetCensus>,
    readiness: Vec<Option<Duration>>,
    projected_purchases: Vec<u32>,
    orphan_plan: Plan,
}

impl Reconciler {
    /// Sizes physical-instance scratch for the configured fleet maxima.
    #[must_use]
    pub fn new(
        fleets: &[FleetPolicy],
        scaling: Arc<ScalingConfig>,
        profiles: SourceProfiles,
        cpu_oversubscription: NonZeroU32,
    ) -> Self {
        let capacity = fleets
            .iter()
            .map(|p| p.instances.max().get() as usize)
            .sum();
        let mut observations = Observations::new(fleets.len());
        observations.reserve_readiness(fleets.iter().map(|p| p.instances.max().get() as usize));
        Self {
            observations,
            candidates: Vec::with_capacity(capacity),
            draining: Vec::with_capacity(capacity),
            serving: Vec::with_capacity(capacity),
            repair_workers: Vec::with_capacity(capacity),
            first_pass: None,
            first_wall: None,
            planner: WorkPlanner::new(fleets, Arc::clone(&scaling), profiles, cpu_oversubscription),
            cpu_oversubscription,
            running_reservations: Vec::with_capacity(capacity),
            scaling,
            capacities: fleets
                .iter()
                .map(|p| Capacity {
                    live: 0,
                    standing: 0,
                    reliable: 0,
                    resumable: Vec::with_capacity(p.instances.max().get() as usize),
                    instances: Vec::with_capacity(p.instances.max().get() as usize),
                })
                .collect(),
            census: (0..fleets.len()).map(|_| FleetCensus::default()).collect(),
            readiness: vec![None; fleets.len()],
            projected_purchases: vec![0; fleets.len()],
            orphan_plan: Plan::new(),
        }
    }

    /// A skipped pass breaks observed dwell and quiet continuity, not cordons or rate gauges.
    pub fn interrupt(&mut self) {
        self.observations.interrupt();
        self.planner.interrupt();
    }

    /// Clears and fills `plan` and each fleet's outlook using a single observed pass time.
    /// Queue attribution remains first-fit in configured fleet order.
    pub fn reconcile(
        &mut self,
        fleets: &mut [FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        plan: &mut Plan,
    ) {
        let PassTime { wall, now } = time;
        self.first_wall.get_or_insert(wall);
        let first = *self.first_pass.get_or_insert(now);
        let pass = Pass {
            hosted,
            wall,
            now,
            demand_settled: now.saturating_duration_since(first) >= CLIENT_REROUTE_BOUND,
        };
        let snapshot = hosted.snapshot();
        for observation in &snapshot.fleet_capacity {
            if let Some(fleet) = fleets.get_mut(observation.fleet) {
                let capacity = observation.capacity;
                if let (Some(vcpus), Some(memory), Some(disk)) = (
                    NonZeroU32::new(capacity.vcpus),
                    gang_types::NonZeroBytes::new(capacity.memory),
                    gang_types::NonZeroBytes::new(capacity.disk),
                ) {
                    fleet.policy.vcpus_per_instance = vcpus;
                    fleet.policy.memory_per_instance = memory;
                    fleet.policy.disk_per_instance = disk;
                }
            }
        }
        self.repair_workers.clear();
        for step in plan.steps.drain(..) {
            if let Action::ReinforceDrain(worker) = step.action {
                self.repair_workers.push(worker);
            }
        }
        plan.at = wall;
        self.observations.begin_pass();
        self.planner.begin_pass();
        self.candidates.clear();
        self.draining.clear();
        self.serving.clear();
        for capacity in &mut self.capacities {
            capacity.instances.clear();
            capacity.resumable.clear();
        }
        for fleet in fleets.iter_mut() {
            fleet.outlook = FleetOutlook::default();
            fleet.past_boot_grace.clear();
            fleet
                .past_boot_grace
                .resize(fleet.supply.instances.len(), false);
        }
        self.charge_running(fleets, hosted);
        let mut orphan_plan = std::mem::take(&mut self.orphan_plan);
        self.classify_all(fleets, &pass, &mut orphan_plan);
        self.planner
            .prepare(fleets, hosted, time, &self.capacities, &self.readiness);
        for action in &snapshot.queued {
            for index in action.candidates {
                if let Some(fleet) = fleets.get_mut(index) {
                    fleet.outlook.candidates = fleet.outlook.candidates.saturating_add(1);
                }
            }
            let chosen = place_with_credit(
                action.candidates,
                action.resources,
                self.cpu_oversubscription,
                |index| {
                    fleets.get(index).map(|fleet| {
                        let limit = if fleet.supply.launch_failure.is_some() {
                            let capacity = &self.capacities[index];
                            capacity.reliable.saturating_add(capacity.draining())
                        } else {
                            fleet.policy.instances.max().get()
                        };
                        (&fleet.policy, fleet.outlook.demand, limit)
                    })
                },
                |index| {
                    let fleet = &fleets[index];
                    let capacity = &self.capacities[index];
                    let limit = if fleet.supply.launch_failure.is_some() {
                        capacity.reliable.saturating_add(capacity.draining())
                    } else {
                        fleet.policy.instances.max().get()
                    };
                    self.planner.placement_credit(index, &fleet.policy, capacity, limit)
                },
            );
            self.planner.attribute(chosen, action.candidates);
            if let Some(fleet) = chosen.and_then(|index| fleets.get_mut(index)) {
                fleet.outlook.demand.add_queued(action.resources);
                fleet.outlook.queued = fleet.outlook.queued.saturating_add(1);
            }
        }
        self.planner.observe(fleets, hosted, time);
        for (index, fleet) in fleets.iter_mut().enumerate() {
            self.resume_drains(&pass, index, fleet);
        }
        self.planner.finalize(fleets, hosted, time);
        let mut orphans = orphan_plan.steps.drain(..);
        for (index, fleet) in fleets.iter_mut().enumerate() {
            let view = FleetView {
                index,
                policy: &fleet.policy,
                supply: &fleet.supply,
                demand: fleet.outlook.demand,
                needed: fleet.outlook.needed,
                minimum: fleet.outlook.minimum,
                ceiling: self.planner.work(index),
                need: if self.planner.work(index) < fleet.outlook.needed {
                    NeedCause::Minimum
                } else {
                    NeedCause::Demand
                },
            };
            let planned = plan.steps.len();
            plan.steps
                .extend(orphans.by_ref().take(self.census[index].orphans));
            let resumed = self.finish_drains(&pass, &view, plan);
            let purchases = self.reconcile_fleet(&pass, &view, planned, resumed, plan);
            self.projected_purchases[index] = purchases;
        }
        self.planner.project_wait(fleets, hosted, &self.projected_purchases);
        drop(orphans);
        self.orphan_plan = orphan_plan;
        self.observations.finish_pass();
    }

    fn charge_running(&mut self, fleets: &mut [FleetPass], hosted: &HostedSnapshot) {
        let snapshot = hosted.snapshot();
        self.running_reservations.clear();
        self.running_reservations
            .resize(snapshot.workers.len(), crate::Demand::default());
        for execution in &snapshot.executing {
            if let Some(reservations) = self.running_reservations.get_mut(execution.worker) {
                reservations.add_queued(execution.resources);
            }
        }
        for (worker_index, worker) in snapshot.workers.iter().enumerate() {
            if let Some((index, fleet)) = fleets
                .iter_mut()
                .enumerate()
                .find(|(_, f)| f.policy.name == worker.fleet)
            {
                let counts = &mut fleet.outlook.workers;
                let count = match Activity::from(worker) {
                    Activity::Busy => &mut counts.busy,
                    Activity::Idle => &mut counts.idle,
                    Activity::Draining => &mut counts.draining,
                };
                *count = count.saturating_add(1);
                counts.running = counts.running.saturating_add(worker.running_actions);
                let owned = worker
                    .id
                    .instance_name()
                    .is_some_and(|id| hosted.is_cordoned(index, id));
                let replacing = worker
                    .id
                    .instance_name()
                    .is_some_and(|id| hosted.is_replacing(index, &worker.fleet, id));
                if replacing || (!worker.draining && !owned) {
                    Self::charge_worker(&mut fleet.outlook.demand, worker, self.running_reservations[worker_index]);
                }
            }
        }
    }

    fn charge_worker(demand: &mut crate::Demand, worker: &WorkerState, known: crate::Demand) {
        demand.add_running(worker.running_actions);
        let mut resources = known.resources();
        if known.actions() < u64::from(worker.running_actions) {
            let capacity = worker_resources(worker);
            resources.cpu_millicores = resources.cpu_millicores.max(capacity.cpu_millicores);
            resources.memory = resources.memory.max(capacity.memory);
            resources.disk = resources.disk.max(capacity.disk);
        }
        demand.add_running_resources(resources);
    }

    /// Forgets a failed termination so the next good pass may retry it.
    pub fn forget_order(&mut self, fleet: usize, instance: &InstanceId) {
        self.observations.forget_order(fleet, instance);
    }

    fn classify_all(&mut self, fleets: &mut [FleetPass], pass: &Pass<'_>, plan: &mut Plan) {
        plan.steps.clear();
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.minimum = fleet.policy.instances.min_at(pass.wall);
            let view = FleetView {
                index,
                policy: &fleet.policy,
                supply: &fleet.supply,
                demand: fleet.outlook.demand,
                needed: 0,
                minimum: 0,
                ceiling: 0,
                need: NeedCause::Demand,
            };
            let candidates = self.candidates.len();
            let draining = self.draining.len();
            let serving = self.serving.len();
            let orphans = plan.steps.len();
            let mut census = Census::default();
            for (position, instance) in fleet.supply.instances.iter().enumerate() {
                let protected = fleet.handover_protected.binary_search(&instance.id).is_ok()
                    && pass
                        .hosted
                        .workers_on(&fleet.policy.name, &instance.id)
                        .next()
                        .is_none();
                if instance.lifecycle == InstanceLifecycle::Terminating && !protected {
                    continue;
                }
                census.live = census.live.saturating_add(1);
                let class = if protected {
                    census.handover_retained = census.handover_retained.saturating_add(1);
                    fleet.past_boot_grace[position] = false;
                    Standing::Retained
                } else {
                    self.classify_instance(
                        pass,
                        &view,
                        (position, instance),
                        (&mut fleet.past_boot_grace, &mut fleet.outlook.workers),
                        &mut census,
                        plan,
                    )
                };
                match class {
                    Standing::Usable => {
                        census.usable = census.usable.saturating_add(1);
                        self.capacities[index].instances.push(position);
                        if instance.release_first {
                            census.release_first = census.release_first.saturating_add(1);
                        }
                    }
                    Standing::Transient => census.transient = census.transient.saturating_add(1),
                    Standing::Retained | Standing::Ordered => {}
                }
            }
            let unborn = fleet.supply.desired.saturating_sub(census.live);
            let capacity = &mut self.capacities[index];
            capacity.live = census.live;
            capacity.standing = census.usable.saturating_add(unborn);
            capacity.reliable = if fleet.supply.launch_failure.is_none() {
                capacity.standing
            } else {
                census.usable
            };
            let workers = &pass.hosted.snapshot().workers;
            self.draining[draining..].sort_unstable_by(|a, b| {
                b.running
                    .cmp(&a.running)
                    .then_with(|| a.release_first.cmp(&b.release_first))
                    .then_with(|| workers[a.worker].id.cmp(&workers[b.worker].id))
            });
            capacity.resumable.extend(
                self.draining[draining..]
                    .iter()
                    .filter(|drain| drain.kind == DrainKind::Surplus)
                    .map(|drain| drain.instance),
            );
            self.census[index] = FleetCensus {
                census,
                candidates: candidates..self.candidates.len(),
                draining: draining..self.draining.len(),
                serving: serving..self.serving.len(),
                orphans: plan.steps.len() - orphans,
            };
            self.readiness[index] = self.observations.readiness(index);
        }
    }

    fn hosted(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        instance: &FleetInstance,
    ) -> Hosted {
        let workers = &pass.hosted.snapshot().workers;
        let mut hosted = Hosted::default();
        for index in pass.hosted.workers_on(&view.policy.name, &instance.id) {
            let seen = self.observations.observe_worker(&workers[index], pass.now);
            hosted.add(index, seen, workers);
        }
        hosted
    }

    fn classify_instance(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        entry: (usize, &FleetInstance),
        outlook: (&mut [bool], &mut WorkerCounts),
        census: &mut Census,
        plan: &mut Plan,
    ) -> Standing {
        let (position, instance) = entry;
        let (past_boot_grace, counts) = outlook;
        let hosted = self.hosted(pass, view, instance);
        let owned = pass.hosted.is_cordoned(view.index, instance.id.as_str());
        let replacing =
            pass.hosted
                .is_replacing(view.index, &view.policy.name, instance.id.as_str());
        if owned {
            self.observations.observe_cordon(view.index, &instance.id);
        }
        let unserved =
            hosted.worker.is_none() && instance.lifecycle == InstanceLifecycle::InService;
        if unserved {
            counts.unconnected = counts.unconnected.saturating_add(1);
        }
        let seen = self
            .observations
            .observe_instance(view.index, &instance.id, unserved, pass.now);
        let draining_since = self.observations.observe_drain(
            view.index,
            &instance.id,
            owned || replacing || (hosted.worker.is_some() && !hosted.serving),
            pass.now,
        );
        let launched_for = pass.wall.saturating_duration_since(instance.launched_at);
        if hosted.worker.is_some() {
            let sample = self
                .first_wall
                .filter(|first| instance.launched_at >= *first)
                .map(|_| launched_for);
            self.observations
                .observe_connection(view.index, &instance.id, sample);
        }
        let past_grace = unserved
            && launched_for >= view.policy.boot_grace
            && seen.unserved_since.is_some_and(|since| {
                pass.now.saturating_duration_since(since) >= view.policy.boot_grace
            });
        past_boot_grace[position] = past_grace;
        if seen
            .ordered_at
            .is_some_and(|at| pass.now.saturating_duration_since(at) < view.policy.boot_grace)
        {
            return Standing::Ordered;
        }
        if replacing {
            Self::classify_replacement(pass, view, instance, hosted, census, plan);
        }
        if owned || hosted.draining_since.is_some() {
            census.busy_cordons = census.busy_cordons.saturating_add(1);
        }
        if owned && hosted.worker.is_none() {
            census.unconnected_cordon = true;
        }
        let Some(worker) = hosted.worker else {
            if !past_grace {
                return if replacing {
                    Standing::Retained
                } else if owned {
                    Standing::Transient
                } else {
                    Standing::Usable
                };
            }
            self.terminate(pass, view, &instance.id, TerminationCause::Orphan, plan);
            return Standing::Ordered;
        };
        if owned || replacing || !hosted.serving {
            return self.classify_drain(
                pass,
                view,
                entry,
                hosted,
                (worker, owned, replacing, draining_since.unwrap_or(pass.now)),
            );
        }
        census.serving = census.serving.saturating_add(1);
        self.serving.push(Serving {
            worker,
            instance: position,
            running: hosted.running,
            release_first: instance.release_first,
        });
        if !hosted.busy
            && hosted.running == 0
            && let Some(since) = hosted.idle_since
            && pass.now.saturating_duration_since(since) >= view.policy.idle_timeout
        {
            self.candidates.push(Candidate {
                since,
                worker,
                instance: position,
                release_first: instance.release_first,
            });
        }
        Standing::Usable
    }

    fn classify_replacement(
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        instance: &FleetInstance,
        hosted: Hosted,
        census: &mut Census,
        plan: &mut Plan,
    ) {
        census.replacing = census.replacing.saturating_add(1);
        if hosted.serving {
            return;
        }
        if let Some(worker) = hosted.worker {
            let replacing_cordon = pass.hosted.snapshot().cordons.iter().any(|cordon| {
                cordon.fleet == view.index
                    && cordon.instance.as_str() == instance.id.as_str()
                    && cordon.replacing
            });
            if !replacing_cordon {
                let worker = pass
                    .hosted
                    .workers_on(&view.policy.name, &instance.id)
                    .find(|&index| !pass.hosted.snapshot().workers[index].current_build)
                    .unwrap_or(worker);
                plan.steps.push(PlanStep {
                    fleet: view.index,
                    action: Action::ReinforceDrain(
                        pass.hosted.snapshot().workers[worker].id.clone(),
                    ),
                });
            }
        }
    }

    fn classify_drain(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        (position, instance): (usize, &FleetInstance),
        hosted: Hosted,
        (worker, owned, replacing, since): (usize, bool, bool, Tick),
    ) -> Standing {
        let kind = if replacing {
            DrainKind::Replacement
        } else if owned {
            DrainKind::Surplus
        } else {
            DrainKind::Worker
        };
        if kind == DrainKind::Worker
            && pass.now.saturating_duration_since(since) < view.policy.boot_grace
        {
            return Standing::Transient;
        }
        let end = if !hosted.serving && hosted.running == 0 {
            Some(TerminationCause::DrainFinished)
        } else if !hosted.serving
            && kind != DrainKind::Replacement
            && pass.now.saturating_duration_since(since) >= view.policy.max_drain_wait
        {
            Some(TerminationCause::MaxDrainWait)
        } else {
            None
        };
        let workers = &pass.hosted.snapshot().workers;
        let target = self
            .observations
            .victim(view.index, &instance.id)
            .and_then(|victim| {
                pass.hosted
                    .workers_on(&view.policy.name, &instance.id)
                    .find(|&index| workers[index].id == *victim)
            })
            .unwrap_or(worker);
        self.draining.push(Draining {
            worker: target,
            instance: position,
            running: hosted.running,
            end,
            release_first: instance.release_first,
            kind,
            resumed: false,
        });
        Standing::Retained
    }

    fn reconcile_fleet(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        planned: usize,
        resumed: u32,
        plan: &mut Plan,
    ) -> u32 {
        let index = view.index;
        let census = self.census[index].census;
        let unborn = view.supply.desired.saturating_sub(census.live);
        let usable = self.capacities[index].reliable;
        let usable = usable.saturating_add(resumed);
        let terminated: u32 = plan.steps[planned..]
            .iter()
            .filter(|s| matches!(s.action, Action::Terminate { .. }))
            .count()
            .saturating_into();
        let from = view.supply.desired.saturating_sub(terminated);
        let to = self.purchase_target_after_drains(pass, view, planned, from, plan);
        if view.supply.abandoned > 0 && resumed == 0 {
            plan.steps.push(PlanStep {
                fleet: index,
                action: Action::Withdraw {
                    from: from.saturating_add(view.supply.abandoned),
                    to: from,
                },
            });
        }
        if to > from {
            let purchases = to - from;
            let mut summary = self.planner.scale_decision(index);
            summary.extra_instances = purchases;
            plan.steps.push(PlanStep {
                fleet: index,
                action: Action::ScaleOut {
                    from,
                    to,
                    need: view.need,
                    summary,
                },
            });
            self.observations.reset_dwell(index);
            return purchases;
        }
        if resumed > 0 {
            self.observations.reset_dwell(index);
            return 0;
        }
        let withdrawn = if view.supply.launch_failure.is_some() {
            unborn.min(usable.saturating_add(unborn).saturating_sub(view.needed))
        } else {
            0
        };
        if withdrawn > 0 {
            plan.steps.push(PlanStep {
                fleet: index,
                action: Action::Withdraw {
                    from,
                    to: from.saturating_sub(withdrawn),
                },
            });
        }
        let before_drains = plan.steps.len();
        if pass.demand_settled {
            self.start_idle_drains(
                pass,
                view,
                usable.saturating_sub(view.needed),
                census.release_first,
                plan,
            );
        }
        let other_change = terminated > 0
            || withdrawn > 0
            || view.supply.abandoned > 0
            || plan.steps.len() > before_drains;
        if other_change || !pass.demand_settled {
            self.observations.reset_dwell(index);
            return 0;
        }
        self.cordon(pass, view, plan);
        0
    }

    fn purchase_target_after_drains(
        &self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        planned: usize,
        from: u32,
        plan: &Plan,
    ) -> u32 {
        let census = &self.census[view.index].census;
        let retaining: u32 = self.draining[self.census[view.index].draining.clone()]
            .iter()
            .filter(|d| {
                d.kind != DrainKind::Replacement && !d.resumed && d.end.is_none() && d.running > 0
            })
            .count()
            .saturating_into();
        let max = view.policy.instances.max().get();
        let terminated_replacing: u32 = plan.steps[planned..]
            .iter()
            .filter(|step| match &step.action {
                Action::Terminate { instance, .. } => {
                    pass.hosted
                        .is_replacing(view.index, &view.policy.name, instance.as_str())
                }
                _ => false,
            })
            .count()
            .saturating_into();
        let to = purchase_target(
            view.needed,
            retaining.saturating_add(census.handover_retained),
            max,
        )
        .saturating_add(census.replacing.saturating_sub(terminated_replacing));
        if census.handover_retained > 0 {
            to.min(from.saturating_add(max.saturating_sub(census.live)))
        } else {
            to
        }
    }

    fn resume_drains(&mut self, pass: &Pass<'_>, index: usize, fleet: &mut FleetPass) {
        let census = self.census[index].census;
        let workers = &pass.hosted.snapshot().workers;
        let range = self.census[index].draining.clone();
        let mut resumed = 0u32;
        for position in range {
            let drain = self.draining[position];
            if drain.kind != DrainKind::Surplus {
                continue;
            }
            let available = self.capacities[index].reliable
                .saturating_add(census.transient)
                .saturating_add(resumed);
            let ordinary = fleet.outlook.needed > available;
            let waterline = drain.running > 0 && self.planner.needs_resume(index);
            if !ordinary && !waterline {
                continue;
            }
            self.draining[position].resumed = true;
            resumed = resumed.saturating_add(1);
            let instance = &fleet.supply.instances[drain.instance].id;
            for worker_index in pass.hosted.workers_on(&fleet.policy.name, instance) {
                Self::charge_worker(
                    &mut fleet.outlook.demand,
                    &workers[worker_index],
                    self.running_reservations[worker_index],
                );
            }
            self.planner.resume(index, drain.instance, &fleet.policy);
            self.planner.refresh_demand(index, fleet);
        }
    }

    fn finish_drains(&mut self, pass: &Pass<'_>, view: &FleetView<'_>, plan: &mut Plan) -> u32 {
        let index = view.index;
        let workers = &pass.hosted.snapshot().workers;
        let mut resumed = 0u32;
        for position in self.census[index].draining.clone() {
            let drain = self.draining[position];
            if drain.resumed {
                resumed = resumed.saturating_add(1);
                plan.steps.push(PlanStep {
                    fleet: index,
                    action: Action::CancelDrain(workers[drain.worker].id.clone(), view.need),
                });
                continue;
            }
            let instance = &view.supply.instances[drain.instance].id;
            if let Some(cause) = drain.end {
                self.terminate(pass, view, instance, cause, plan);
            } else if drain.kind != DrainKind::Worker {
                for worker in pass.hosted.workers_on(&view.policy.name, instance) {
                    if !workers[worker].draining {
                        let id = &workers[worker].id;
                        let cached = self.repair_workers.iter().position(|worker| worker == id);
                        let worker = cached.map_or_else(
                            || id.clone(),
                            |position| self.repair_workers.swap_remove(position),
                        );
                        plan.steps.push(PlanStep {
                            fleet: index,
                            action: Action::ReinforceDrain(worker),
                        });
                    }
                }
            }
        }
        resumed
    }

    fn terminate(
        &mut self,
        pass: &Pass<'_>,
        view: &FleetView<'_>,
        instance: &InstanceId,
        cause: TerminationCause,
        plan: &mut Plan,
    ) {
        plan.steps.push(PlanStep {
            fleet: view.index,
            action: Action::Terminate {
                instance: instance.clone(),
                cause,
                mode: if pass
                    .hosted
                    .is_replacing(view.index, &view.policy.name, instance.as_str())
                {
                    TerminationMode::Replace
                } else {
                    TerminationMode::ScaleIn
                },
            },
        });
        self.observations
            .order_termination(view.index, instance, pass.now);
        self.observations.reset_dwell(view.index);
    }

    fn start_idle_drains(
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
        let release_len = candidates.partition_point(|c| c.release_first);
        let first = surplus.min(release_len);
        let rest = surplus
            .saturating_sub(release_first_capacity)
            .min(candidates.len() - release_len);
        let mut credit = self.planner.credit(view.index);
        let mut remaining = view
            .needed
            .saturating_add(u32::try_from(surplus).unwrap_or(u32::MAX));
        for candidate in candidates[..first]
            .iter()
            .chain(&candidates[release_len..release_len + rest])
        {
            let mut after = credit;
            for (value, victim) in after.iter_mut().zip(self.planner.instance_credit(
                view.index,
                candidate.instance,
                view.policy,
            )) {
                *value = value.saturating_sub(victim);
            }
            let next = remaining.saturating_sub(1);
            if view.minimum.max(view.demand.instances_with_credit(
                view.policy,
                self.cpu_oversubscription,
                after,
            )) > next
            {
                continue;
            }
            credit = after;
            remaining = next;
            let worker = &workers[candidate.worker].id;
            self.observations.remember_victim(
                view.index,
                &view.supply.instances[candidate.instance].id,
                worker,
            );
            plan.steps.push(PlanStep {
                fleet: view.index,
                action: Action::StartDrain {
                    worker: worker.clone(),
                    reason: DrainReason::IdleTimeout,
                    summary: None,
                },
            });
        }
    }

    fn cordon(&mut self, pass: &Pass<'_>, view: &FleetView<'_>, plan: &mut Plan) {
        let index = view.index;
        let census = self.census[index].census;
        let remaining = census.serving.saturating_sub(1);
        let unborn = view.supply.desired.saturating_sub(census.live);
        if census.serving == 0
            || self.capacities[index].reliable != census.serving
            || unborn != 0
            || census.transient > 0
            || census.unconnected_cordon
            || census.busy_cordons >= u32::from(self.scaling.max_cordons)
            || view.minimum > remaining
            || view.ceiling > remaining
            || self.planner.work(index) > remaining
        {
            self.observations.reset_dwell(index);
            return;
        }
        let workers = &pass.hosted.snapshot().workers;
        let mut victim: Option<(Serving, crate::CordonDecision)> = None;
        for &candidate in &self.serving[self.census[index].serving.clone()] {
            let instance = &view.supply.instances[candidate.instance].id;
            if pass
                .hosted
                .workers_on(&view.policy.name, instance)
                .any(|worker| {
                    let worker_state = &workers[worker];
                    let unknown = self.running_reservations[worker].actions()
                        < u64::from(worker_state.running_actions);
                    let capacity = worker_state.capacity.unwrap_or_default();
                    unknown
                        && (capacity.cpu_millicores == 0
                            || capacity.memory.get() == 0
                            || capacity.disk.get() == 0)
                })
            {
                continue;
            }
            let mut credit = self.planner.credit(index);
            for (value, victim) in credit.iter_mut().zip(self.planner.instance_credit(
                index,
                candidate.instance,
                view.policy,
            )) {
                *value = value.saturating_sub(victim);
            }
            if view
                .demand
                .instances_with_credit(view.policy, self.cpu_oversubscription, credit)
                > remaining
            {
                continue;
            }
            let Some(summary) = self.planner.removal(index, candidate.instance) else {
                continue;
            };
            let replace = victim.as_ref().is_none_or(|(held, previous)| {
                victim_order(
                    (
                        summary.empty_after,
                        candidate.release_first,
                        candidate.running,
                        &workers[candidate.worker].id,
                    ),
                    (
                        previous.empty_after,
                        held.release_first,
                        held.running,
                        &workers[held.worker].id,
                    ),
                )
                .is_lt()
            });
            if replace {
                victim = Some((candidate, summary));
            }
        }
        let Some((victim, mut summary)) = victim else {
            self.observations.reset_dwell(index);
            return;
        };
        summary.dwell = self.observations.dwell(index, pass.now);
        if summary.dwell < self.scaling.scale_in_dwell.get() {
            return;
        }
        let worker = &workers[victim.worker].id;
        self.observations.remember_victim(
            index,
            &view.supply.instances[victim.instance].id,
            worker,
        );
        plan.steps.push(PlanStep {
            fleet: index,
            action: Action::StartDrain {
                worker: worker.clone(),
                reason: DrainReason::Utilization,
                summary: Some(summary),
            },
        });
        self.observations.reset_dwell(index);
    }
}

fn purchase_target(needed: u32, retaining: u32, maximum: u32) -> u32 {
    needed.saturating_add(retaining).min(maximum)
}

fn victim_order(
    (empty, release_first, running, worker): (Duration, bool, u32, &gang_types::WorkerId),
    (other_empty, other_release_first, other_running, other_worker): (
        Duration,
        bool,
        u32,
        &gang_types::WorkerId,
    ),
) -> std::cmp::Ordering {
    empty
        .cmp(&other_empty)
        .then_with(|| other_release_first.cmp(&release_first))
        .then_with(|| running.cmp(&other_running))
        .then_with(|| worker.cmp(other_worker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::{TestResult, worker_id};

    fn fixture(
        minimum: u32,
        draining: bool,
        owned: bool,
    ) -> TestResult<(Reconciler, Vec<FleetPass>, HostedSnapshot)> {
        let mut policy = gang_testkit::fleet_policy("workers")?;
        policy.instances =
            gang_config::InstanceBounds::try_new(minimum, gang_types::nonzero_u32(4))?;
        policy.idle_timeout = Duration::from_hours(1);
        let scaling = Arc::new(ScalingConfig::default());
        let profiles = SourceProfiles::new(&scaling, gang_config::HistoryWindow::from_units(1));
        let reconciler = Reconciler::new(
            std::slice::from_ref(&policy),
            scaling,
            profiles,
            gang_types::nonzero_u32(2),
        );
        let instance = InstanceId::try_new("i-0abc123")?;
        let mut fleet = FleetPass::new(policy.clone());
        fleet.supply.desired = 1;
        fleet.supply.instances.push(FleetInstance {
            id: instance.clone(),
            lifecycle: InstanceLifecycle::InService,
            launched_at: Timestamp::default(),
            release_first: false,
        });
        let mut snapshot = gang_spi::SchedulerSnapshot {
            workers: vec![WorkerState {
                id: worker_id(instance.as_str())?,
                environment: Some(policy.action_environment.clone()),
                fleet: policy.name,
                running_actions: 0,
                max_inflight_actions: None,
                capacity: Some(gang_types::ActionResources {
                    cpu_millicores: u64::from(policy.vcpus_per_instance.get()) * 2_000,
                    memory: policy.memory_per_instance.get(),
                    disk: policy.disk_per_instance.get(),
                }),
                reserved: None,
                cache: None,
                paused: false,
                draining,
                current_build: true,
                last_seen: Timestamp::default(),
            }],
            ..gang_spi::SchedulerSnapshot::default()
        };
        if owned {
            snapshot.cordons.push(gang_spi::InstanceCordon {
                fleet: 0,
                instance: Arc::new(instance),
                replacing: false,
            });
        }
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        Ok((reconciler, vec![fleet], hosted))
    }

    fn time(seconds: u64) -> PassTime {
        PassTime {
            now: Tick::from_elapsed(Duration::from_secs(seconds)),
            wall: Timestamp::from_unix_millis(100_000 + seconds * 1_000),
        }
    }

    #[test]
    fn resumed_large_worker_avoids_an_unnecessary_purchase() -> TestResult {
        let (mut reconciler, mut fleets, mut hosted) = fixture(0, true, true)?;
        let nominal = fleets[0].policy.memory_per_instance.get().get();
        let mut snapshot = hosted.snapshot().clone();
        snapshot.workers[0].capacity.as_mut().unwrap().memory = gang_types::Bytes::new(nominal * 2);
        snapshot.queued.push(gang_spi::QueuedDemand {
            source: None,
            candidates: gang_types::FleetSet::prefix(1)?,
            resources: gang_types::ActionResources {
                memory: gang_types::Bytes::new(nominal + nominal / 2),
                ..Default::default()
            },
            queued_since: Timestamp::default(),
            priority: 0,
            expected: None,
        });
        hosted.replace(&mut snapshot);
        let mut plan = Plan::new();
        reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
        assert!(
            plan.steps
                .iter()
                .any(|step| matches!(step.action, Action::CancelDrain(..)))
        );
        assert!(
            !plan
                .steps
                .iter()
                .any(|step| matches!(step.action, Action::ScaleOut { .. }))
        );
        Ok(())
    }

    #[test]
    fn missing_running_metadata_occupies_one_worker_without_double_charging_known_work()
    -> TestResult {
        let (mut reconciler, mut fleets, mut hosted) = fixture(0, false, false)?;
        let mut snapshot = hosted.snapshot().clone();
        snapshot.workers[0].running_actions = 2;
        let capacity = gang_types::ActionResources {
            cpu_millicores: 123_000,
            memory: gang_types::Bytes::new(131_558_744_064),
            disk: gang_types::Bytes::gib(222),
        };
        snapshot.workers[0].capacity = Some(capacity);
        snapshot.executing.push(gang_spi::ExecutingDemand {
            source: None,
            candidates: gang_types::FleetSet::prefix(1)?,
            resources: gang_types::ActionResources {
                cpu_millicores: 70_000,
                memory: gang_types::Bytes::gib(70),
                disk: gang_types::Bytes::gib(70),
            },
            worker: 0,
            started: Timestamp::default(),
            expected: None,
        });
        hosted.replace(&mut snapshot);
        reconciler.charge_running(&mut fleets, &hosted);
        let resources = fleets[0].outlook.demand.resources();
        assert_eq!(resources, capacity);
        assert_eq!(fleets[0].outlook.demand.actions(), 2);
        Ok(())
    }

    #[test]
    fn missing_running_and_capacity_metadata_keeps_known_reservations() -> TestResult {
        let (mut reconciler, mut fleets, mut hosted) = fixture(0, false, false)?;
        let mut snapshot = hosted.snapshot().clone();
        snapshot.workers[0].running_actions = 2;
        snapshot.workers[0].capacity = None;
        snapshot.executing.push(gang_spi::ExecutingDemand {
            source: None,
            candidates: gang_types::FleetSet::prefix(1)?,
            resources: gang_types::ActionResources {
                cpu_millicores: 1_000,
                ..Default::default()
            },
            worker: 0,
            started: Timestamp::default(),
            expected: None,
        });
        hosted.replace(&mut snapshot);
        reconciler.charge_running(&mut fleets, &hosted);
        assert_eq!(fleets[0].outlook.demand.resources().cpu_millicores, 1_000);
        assert_eq!(fleets[0].outlook.demand.actions(), 2);
        Ok(())
    }

    #[test]
    fn observed_fleet_capacity_tracks_all_dimensions_and_retains_when_missing() -> TestResult {
        use gang_types::{ActionResources, Bytes, FleetName, FleetSet, NonZeroBytes, Resources};

        for dimension in 0..3 {
            let (_, mut fleets, mut hosted) = fixture(0, false, false)?;
            fleets[0].policy.vcpus_per_instance = gang_types::nonzero_u32(16);
            fleets[0].policy.memory_per_instance = NonZeroBytes::gib(63);
            fleets[0].policy.disk_per_instance = NonZeroBytes::gib(63);
            let mut other = fleets[0].policy.clone();
            other.name = FleetName::try_new("other")?;
            fleets.push(FleetPass::new(other));
            let policies: Vec<_> = fleets.iter().map(|fleet| fleet.policy.clone()).collect();
            let scaling = Arc::new(ScalingConfig::default());
            let profiles = SourceProfiles::new(&scaling, gang_config::HistoryWindow::from_units(1));
            let factor = gang_types::nonzero_u32(2);
            let mut reconciler = Reconciler::new(&policies, scaling, profiles, factor);
            let mut plan = Plan::new();
            let observed = Resources {
                vcpus: 8,
                memory: Bytes::mib(62_229),
                disk: Bytes::gib(60),
            };
            let configured = Resources {
                vcpus: 16,
                memory: Bytes::gib(63),
                disk: Bytes::gib(63),
            };
            let resources = match dimension {
                0 => ActionResources { cpu_millicores: 32_000, ..ActionResources::default() },
                1 => ActionResources { memory: Bytes::gib(63), ..ActionResources::default() },
                _ => ActionResources { disk: Bytes::gib(63), ..ActionResources::default() },
            };
            let mut snapshot = gang_spi::SchedulerSnapshot {
                fleet_capacity: vec![
                    gang_spi::FleetResourceCapacity { fleet: 0, capacity: observed },
                    gang_spi::FleetResourceCapacity { fleet: 9, capacity: observed },
                ],
                queued: vec![gang_spi::QueuedDemand {
                    source: None,
                    candidates: FleetSet::prefix(1)?,
                    resources,
                    queued_since: Timestamp::default(),
                    priority: 0,
                    expected: None,
                }],
                ..gang_spi::SchedulerSnapshot::default()
            };
            let queued = snapshot.queued.clone();
            hosted.replace(&mut snapshot);
            reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
            let mut expected = policies[0].clone();
            expected.vcpus_per_instance = gang_types::nonzero_u32(observed.vcpus);
            expected.memory_per_instance = NonZeroBytes::new(observed.memory).unwrap();
            expected.disk_per_instance = NonZeroBytes::new(observed.disk).unwrap();
            assert_eq!(fleets[0].policy, expected);
            assert_eq!(fleets[1].policy, policies[1]);
            assert_eq!(fleets[0].outlook.demand.instances(&fleets[0].policy, factor), 2);
            assert_eq!(fleets[0].outlook.demand.instances(&policies[0], factor), 1);

            let mut snapshot = gang_spi::SchedulerSnapshot {
                fleet_capacity: vec![
                    gang_spi::FleetResourceCapacity { fleet: 0, capacity: configured },
                    gang_spi::FleetResourceCapacity { fleet: 1, capacity: observed },
                ],
                queued,
                ..gang_spi::SchedulerSnapshot::default()
            };
            hosted.replace(&mut snapshot);
            reconciler.reconcile(&mut fleets, &hosted, time(1), &mut plan);
            assert_eq!(fleets[0].policy, policies[0]);
            let mut other_expected = policies[1].clone();
            other_expected.vcpus_per_instance = gang_types::nonzero_u32(observed.vcpus);
            other_expected.memory_per_instance = NonZeroBytes::new(observed.memory).unwrap();
            other_expected.disk_per_instance = NonZeroBytes::new(observed.disk).unwrap();
            assert_eq!(fleets[1].policy, other_expected);
            assert_eq!(fleets[0].outlook.demand.instances(&fleets[0].policy, factor), 1);

            let mut snapshot = gang_spi::SchedulerSnapshot::default();
            hosted.replace(&mut snapshot);
            reconciler.reconcile(&mut fleets, &hosted, time(2), &mut plan);
            assert_eq!(fleets[0].policy, policies[0]);
            assert_eq!(fleets[1].policy, other_expected);
        }
        Ok(())
    }

    #[test]
    fn unowned_refresh_gap_is_transient_then_terminates_and_replaces_once() -> TestResult {
        let (mut reconciler, mut fleets, hosted) = fixture(1, true, false)?;
        let mut plan = Plan::new();
        reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
        assert!(plan.steps.is_empty());
        assert_eq!(reconciler.census[0].census.transient, 1);
        reconciler.reconcile(&mut fleets, &hosted, time(29), &mut plan);
        assert!(plan.steps.is_empty());
        reconciler.reconcile(&mut fleets, &hosted, time(30), &mut plan);
        assert!(matches!(
            plan.steps.as_slice(),
            [
                PlanStep {
                    action: Action::Terminate {
                        cause: TerminationCause::DrainFinished,
                        ..
                    },
                    ..
                },
                PlanStep {
                    action: Action::ScaleOut { from: 0, to: 1, .. },
                    ..
                },
            ]
        ));
        // A provider may still list the ordered instance; that does not compound purchases.
        reconciler.reconcile(&mut fleets, &hosted, time(31), &mut plan);
        assert!(plan.steps.is_empty());
        Ok(())
    }

    #[test]
    fn partial_owned_cordon_repairs_silently_without_terminating() -> TestResult {
        let (mut reconciler, mut fleets, hosted) = fixture(0, false, true)?;
        let mut plan = Plan::new();
        for seconds in [0, 60, 600] {
            reconciler.reconcile(&mut fleets, &hosted, time(seconds), &mut plan);
            assert!(matches!(
                plan.steps.as_slice(),
                [PlanStep {
                    action: Action::ReinforceDrain(_),
                    ..
                }]
            ));
            assert!(
                plan.steps
                    .iter()
                    .all(|step| step.audit_event(plan.at, &fleets[0].policy.name).is_none())
            );
        }
        Ok(())
    }

    #[test]
    fn warmed_partial_cordon_repair_reuses_owned_worker_ids() -> TestResult {
        let (mut reconciler, mut fleets, hosted) = fixture(0, false, true)?;
        let mut plan = Plan::new();
        for _ in 0..3 {
            reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
        }
        let allocations = allocation_counter::measure(|| {
            reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
        });
        assert_eq!(allocations.count_total, 0);
        assert!(matches!(
            plan.steps.as_slice(),
            [PlanStep {
                action: Action::ReinforceDrain(_),
                ..
            }]
        ));
        Ok(())
    }

    #[test]
    fn launch_failure_with_need_between_usable_and_desired_withdraws() -> TestResult {
        let (mut reconciler, mut fleets, hosted) = fixture(3, false, false)?;
        fleets[0].supply.desired = 6;
        fleets[0].supply.launch_failure = Some(gang_spi::LaunchFailure {
            reason: "capacity".into(),
        });
        let mut plan = Plan::new();
        reconciler.reconcile(&mut fleets, &hosted, time(0), &mut plan);
        assert!(matches!(
            plan.steps.as_slice(),
            [PlanStep {
                action: Action::Withdraw { from: 6, to: 3 },
                ..
            },]
        ));
        Ok(())
    }

    #[test]
    fn skipped_pass_resets_n_one_to_zero_dwell_without_waiting_for_idle_timeout() -> TestResult {
        let (mut reconciler, mut fleets, hosted) = fixture(0, false, false)?;
        let mut plan = Plan::new();
        for seconds in [0, 60, 359] {
            reconciler.reconcile(&mut fleets, &hosted, time(seconds), &mut plan);
            assert!(plan.steps.is_empty());
        }
        reconciler.interrupt();
        for seconds in [1_000, 1_299] {
            reconciler.reconcile(&mut fleets, &hosted, time(seconds), &mut plan);
            assert!(plan.steps.is_empty());
        }
        reconciler.reconcile(&mut fleets, &hosted, time(1_300), &mut plan);
        assert!(matches!(plan.steps.as_slice(), [
            PlanStep { action: Action::StartDrain { reason: DrainReason::Utilization, summary: Some(summary), .. }, .. },
        ] if summary.dwell == Duration::from_mins(5)));
        Ok(())
    }

    #[test]
    fn purchase_target_caps_retained_drains_and_never_overflows() {
        assert_eq!(purchase_target(4, 1, 4), 4);
        assert_eq!(purchase_target(2, 1, 4), 3);
        assert_eq!(purchase_target(u32::MAX, 1, 4), 4);
    }

    #[test]
    fn victim_order_prefers_empty_time_before_release_tier_or_running_count() -> TestResult {
        let a = worker_id("i-0abc123")?;
        let b = worker_id("i-0abc124")?;
        assert!(
            victim_order(
                (Duration::from_secs(1), false, 5, &a),
                (Duration::from_secs(2), true, 1, &b)
            )
            .is_lt()
        );
        assert!(
            victim_order(
                (Duration::ZERO, true, 5, &b),
                (Duration::ZERO, false, 1, &a)
            )
            .is_lt()
        );
        assert!(
            victim_order(
                (Duration::ZERO, false, 1, &b),
                (Duration::ZERO, false, 2, &a)
            )
            .is_lt()
        );
        assert!(
            victim_order(
                (Duration::ZERO, false, 1, &a),
                (Duration::ZERO, false, 1, &b)
            )
            .is_lt()
        );
        Ok(())
    }

    #[test]
    fn hosted_counts_running_across_partially_draining_connections() -> TestResult {
        let workers = [
            WorkerState {
                id: worker_id("i-0abc123")?,
                fleet: gang_types::FleetName::try_new("workers")?,
                environment: None,
                running_actions: 3,
                max_inflight_actions: None,
                capacity: None,
                reserved: None,
                cache: None,
                paused: false,
                draining: false,
                current_build: true,
                last_seen: Timestamp::default(),
            },
            WorkerState {
                id: worker_id("i-0abc124")?,
                fleet: gang_types::FleetName::try_new("workers")?,
                environment: None,
                running_actions: 2,
                max_inflight_actions: None,
                capacity: None,
                reserved: None,
                cache: None,
                paused: false,
                draining: true,
                current_build: true,
                last_seen: Timestamp::default(),
            },
        ];
        let mut hosted = Hosted::default();
        for (index, worker) in workers.iter().enumerate() {
            hosted.add(
                index,
                Seen {
                    activity: Activity::from(worker),
                    since: Tick::default(),
                },
                &workers,
            );
        }
        assert_eq!(hosted.running, 5);
        assert!(hosted.serving && hosted.busy);
        Ok(())
    }
}
