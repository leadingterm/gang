//! Work-aware capacity planning over the scheduler's ordered queue.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use gang_config::FleetPolicy;
use gang_spi::SchedulerSnapshot;

use crate::fleet::FleetPass;
use crate::hosted_snapshot::HostedSnapshot;
use crate::pass_time::PassTime;

#[derive(Debug)]
pub(crate) struct Capacity {
    pub live: u32,
    pub standing: u32,
    pub reliable: u32,
    pub draining: u32,
    pub instances: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Slot {
    free: u64,
    uses: u64,
    index: u32,
    instance: u32,
}

#[derive(Debug, Default)]
struct Simulation {
    heaps: Vec<BinaryHeap<Reverse<Slot>>>,
    waits: Vec<Option<u64>>,
    last: Vec<u64>,
    completion: u64,
}

impl Simulation {
    fn new(fleets: usize) -> Self {
        Self {
            heaps: (0..fleets).map(|_| BinaryHeap::new()).collect(),
            waits: vec![None; fleets],
            last: Vec::new(),
            completion: 0,
        }
    }

    fn copy_from(&mut self, other: &Self) {
        for (dst, src) in self.heaps.iter_mut().zip(&other.heaps) {
            dst.clone_from(src);
        }
        self.waits.clone_from(&other.waits);
        self.last.clone_from(&other.last);
        self.completion = other.completion;
    }

    fn add_instance(&mut self, fleet: usize, ordinal: u32, slots: u32, free: u64) {
        for index in 0..slots {
            self.heaps[fleet].push(Reverse(Slot {
                free,
                uses: 0,
                index,
                instance: ordinal,
            }));
        }
    }

    fn best(&self, candidates: gang_types::FleetSet) -> Option<(Slot, usize)> {
        candidates
            .iter()
            .filter_map(|fleet| self.heaps.get(fleet)?.peek().map(|slot| (slot.0, fleet)))
            .min()
    }

    fn dispatch(
        &mut self,
        snapshot: &SchedulerSnapshot,
        placement: (&[Option<usize>], &[gang_types::FleetSet]),
        boots: &[u64],
        start: usize,
        stop_at: Option<u64>,
        tracked: Option<(usize, u32)>,
    ) -> usize {
        let (attribution, matching) = placement;
        for (index, action) in snapshot.queued.iter().enumerate().skip(start) {
            let Some(fleet) = attribution
                .get(index)
                .copied()
                .flatten()
                .filter(|f| *f < boots.len())
            else {
                continue;
            };
            let best = self.best(matching[index]);
            if stop_at.is_some_and(|boot| best.is_none_or(|(slot, _)| slot.free >= boot)) {
                return index;
            }
            let Some((mut slot, chosen)) = best else {
                continue;
            };
            self.heaps[chosen].pop();
            let begin = slot.free;
            slot.free = begin.saturating_add(action.expected.map_or(boots[fleet], millis));
            slot.uses = slot.uses.saturating_add(1);
            self.completion = self.completion.max(slot.free);
            self.waits[fleet] = Some(self.waits[fleet].unwrap_or(0).max(begin));
            if let Some((tracked_fleet, first)) = tracked
                && chosen == tracked_fleet
                && slot.instance >= first
                && let Some(last) = self.last.get_mut((slot.instance - first) as usize)
            {
                *last = (*last).max(slot.free);
            }
            self.heaps[chosen].push(Reverse(slot));
        }
        snapshot.queued.len()
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn project_running(remaining: &mut Vec<u64>, cap: usize) {
    if cap == 0 {
        remaining.clear();
    } else if remaining.len() > cap {
        let cut = remaining.len() - cap;
        remaining.select_nth_unstable(cut);
        remaining.drain(..cut);
    }
    remaining.sort_unstable();
}

#[derive(Debug)]
pub(crate) struct WorkPlanner {
    attribution: Vec<Option<usize>>,
    matching: Vec<gang_types::FleetSet>,
    work: Vec<u32>,
    slots: Vec<u32>,
    boots: Vec<u64>,
    ordinals: Vec<u32>,
    remaining: Vec<u64>,
    base: Simulation,
    checkpoint: Simulation,
    trial: Simulation,
}

impl WorkPlanner {
    pub(crate) fn new(fleets: &[FleetPolicy]) -> Self {
        let n = fleets.len();
        Self {
            attribution: Vec::new(),
            matching: Vec::new(),
            work: vec![0; n],
            slots: vec![0; n],
            boots: vec![0; n],
            ordinals: vec![0; n],
            remaining: Vec::new(),
            base: Simulation::new(n),
            checkpoint: Simulation::new(n),
            trial: Simulation::new(n),
        }
    }

    pub(crate) fn begin_pass(&mut self) {
        self.attribution.clear();
        self.matching.clear();
    }
    pub(crate) fn attribute(&mut self, chosen: Option<usize>, matching: gang_types::FleetSet) {
        self.attribution.push(chosen);
        self.matching.push(matching);
    }
    pub(crate) fn work(&self, index: usize) -> u32 {
        self.work[index]
    }

    fn initialize(&mut self, fleets: &mut [FleetPass], readiness: &[Option<Duration>]) {
        for (index, fleet) in fleets.iter_mut().enumerate() {
            self.boots[index] = millis(readiness[index].unwrap_or(fleet.policy.boot_grace));
            self.slots[index] = fleet.outlook.demand.slots_per_instance(&fleet.policy);
            fleet.outlook.readiness = readiness[index];
            fleet.outlook.expected_wait = None;
            self.base.heaps[index].clear();
            self.base.waits[index] = None;
            self.ordinals[index] = 0;
        }
        self.base.completion = 0;
        self.base.last.clear();
    }

    fn build_timeline(
        &mut self,
        fleets: &[FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        capacities: &[Capacity],
    ) {
        let snapshot = hosted.snapshot();
        for (index, fleet) in fleets.iter().enumerate() {
            let slots = self.slots[index];
            let boot = self.boots[index];
            for &position in &capacities[index].instances {
                let Some(instance) = fleet.supply.instances.get(position) else {
                    continue;
                };
                let ordinal = self.ordinals[index];
                self.ordinals[index] = ordinal.saturating_add(1);
                self.remaining.clear();
                let mut limit = 0u32;
                let mut connected = false;
                for worker_index in hosted.workers_on(&fleet.policy.name, &instance.id) {
                    let worker = &snapshot.workers[worker_index];
                    if worker.draining
                        || worker.environment.as_ref() != Some(&fleet.policy.action_environment)
                    {
                        continue;
                    }
                    connected = true;
                    limit = limit.saturating_add(
                        worker
                            .max_inflight_actions
                            .map_or(slots, std::num::NonZeroU32::get),
                    );
                    let before = self.remaining.len();
                    for execution in snapshot
                        .executing
                        .iter()
                        .filter(|e| e.worker == worker_index)
                    {
                        let elapsed =
                            millis(time.wall.saturating_duration_since(execution.started));
                        let expected = execution.expected.map_or(boot, millis);
                        self.remaining
                            .push(expected.saturating_sub(elapsed).max(elapsed));
                    }
                    let found = self.remaining.len() - before;
                    let missing = (worker.running_actions as usize).saturating_sub(found);
                    self.remaining
                        .resize(self.remaining.len().saturating_add(missing), boot);
                }
                if !connected {
                    let free = millis(
                        (instance.launched_at + Duration::from_millis(boot))
                            .saturating_duration_since(time.wall),
                    );
                    self.base.add_instance(index, ordinal, slots, free);
                    continue;
                }
                let cap = limit.min(slots) as usize;
                project_running(&mut self.remaining, cap);
                for slot in 0..cap {
                    let free = self.remaining.get(slot).copied().unwrap_or(0);
                    self.base.heaps[index].push(Reverse(Slot {
                        free,
                        uses: u64::from(slot < self.remaining.len()),
                        index: u32::try_from(slot).unwrap_or(u32::MAX),
                        instance: ordinal,
                    }));
                }
            }
            if fleet.supply.launch_failure.is_none() {
                let unborn = fleet.supply.desired.saturating_sub(capacities[index].live);
                for _ in 0..unborn {
                    self.base
                        .add_instance(index, self.ordinals[index], slots, boot);
                    self.ordinals[index] = self.ordinals[index].saturating_add(1);
                }
            }
        }
    }

    fn repaying_count(&mut self, index: usize, headroom: u32, snapshot: &SchedulerSnapshot) -> u32 {
        let floor = u32::from(snapshot.queued.iter().enumerate().any(|(i, _)| {
            self.attribution.get(i) == Some(&Some(index))
                && self.base.best(self.matching[i]).is_none()
        }));
        self.checkpoint.copy_from(&self.base);
        let prefix = self.checkpoint.dispatch(
            snapshot,
            (&self.attribution, &self.matching),
            &self.boots,
            0,
            Some(self.boots[index]),
            None,
        );
        let mut previous = 0u64;
        let mut best = 0;
        for n in 0..=headroom {
            self.trial.copy_from(&self.checkpoint);
            self.trial.last.resize(n as usize, 0);
            self.trial.last.fill(0);
            for offset in 0..n {
                self.trial.add_instance(
                    index,
                    self.ordinals[index].saturating_add(offset),
                    self.slots[index],
                    self.boots[index],
                );
            }
            self.trial.dispatch(
                snapshot,
                (&self.attribution, &self.matching),
                &self.boots,
                prefix,
                None,
                Some((index, self.ordinals[index])),
            );
            let span = self
                .trial
                .last
                .iter()
                .map(|end| end.saturating_sub(self.boots[index]))
                .min()
                .unwrap_or(0);
            if n >= floor
                && (n == floor
                    || span >= self.boots[index]
                    || previous.saturating_sub(self.trial.completion) >= self.boots[index])
            {
                best = n;
            }
            previous = self.trial.completion;
        }
        best
    }

    pub(crate) fn plan(
        &mut self,
        fleets: &mut [FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        capacities: &[Capacity],
        readiness: &[Option<Duration>],
        stream: &[bool],
    ) {
        self.initialize(fleets, readiness);
        if self.attribution.iter().all(Option::is_none) {
            for (index, fleet) in fleets.iter_mut().enumerate() {
                self.work[index] = fleet
                    .outlook
                    .demand
                    .instances(&fleet.policy)
                    .min(capacities[index].reliable);
                fleet.outlook.needed = fleet.outlook.minimum.max(self.work[index]);
            }
            return;
        }
        self.build_timeline(fleets, hosted, time, capacities);
        let snapshot = hosted.snapshot();
        for (index, fleet) in fleets.iter_mut().enumerate() {
            let bound = fleet.outlook.demand.instances(&fleet.policy);
            let reliable = capacities[index].reliable;
            let queued = self.attribution.contains(&Some(index));
            let headroom = bound.saturating_sub(reliable);
            let best = if queued && headroom > 0 {
                self.repaying_count(index, headroom, snapshot)
            } else {
                0
            };
            self.work[index] = if queued && stream[index] {
                bound
            } else {
                bound.min(reliable.saturating_add(best))
            };
            fleet.outlook.needed = fleet.outlook.minimum.max(self.work[index]);
            let additional = fleet.outlook.needed.saturating_sub(reliable);
            let planned = if fleet.supply.launch_failure.is_some() {
                additional.min(capacities[index].draining)
            } else {
                additional
            };
            for _ in 0..planned {
                self.base.add_instance(
                    index,
                    self.ordinals[index],
                    self.slots[index],
                    self.boots[index],
                );
                self.ordinals[index] = self.ordinals[index].saturating_add(1);
            }
        }
        self.base.dispatch(
            snapshot,
            (&self.attribution, &self.matching),
            &self.boots,
            0,
            None,
            None,
        );
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.expected_wait = self.base.waits[index].map(Duration::from_millis);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::Timestamp;
    use gang_spi::QueuedDemand;
    use gang_types::{ActionResources, FleetSet};

    fn action(fleets: &[usize], duration: u64) -> QueuedDemand {
        QueuedDemand {
            candidates: FleetSet::try_from_indices(fleets.iter().copied())
                .expect("valid fleet indices"),
            environment: gang_testkit::fleet_policy("test")
                .expect("valid fleet")
                .action_environment,
            resources: ActionResources::default(),
            queued_since: Timestamp::from_unix_millis(0),
            priority: 0,
            expected: Some(Duration::from_millis(duration)),
        }
    }

    #[test]
    fn dispatch_uses_only_environment_matching_fleets() {
        let snapshot = SchedulerSnapshot {
            queued: vec![action(&[0, 1], 10)],
            ..SchedulerSnapshot::default()
        };
        let mut sim = Simulation::new(2);
        sim.add_instance(0, 0, 1, 0);
        sim.add_instance(1, 0, 1, 50);
        sim.dispatch(
            &snapshot,
            (
                &[Some(1)],
                &[FleetSet::try_from_indices([1]).expect("valid fleet")],
            ),
            &[100, 100],
            0,
            None,
            None,
        );
        assert_eq!(sim.waits[1], Some(50));
        assert_eq!(sim.heaps[0].peek().expect("slot").0.free, 0);
    }

    #[test]
    fn completion_repayment_is_not_monotone() {
        let snapshot = SchedulerSnapshot {
            queued: vec![action(&[0], 100); 4],
            ..SchedulerSnapshot::default()
        };
        let mut times = Vec::new();
        for n in 1..=4 {
            let mut sim = Simulation::new(1);
            for i in 0..n {
                sim.add_instance(0, i, 1, 150);
            }
            sim.dispatch(
                &snapshot,
                (&[Some(0); 4], &[FleetSet::prefix(1).expect("valid"); 4]),
                &[150],
                0,
                None,
                None,
            );
            times.push(sim.completion);
        }
        assert_eq!(times, [550, 350, 350, 250]);
        assert_eq!(
            [
                times[0] - times[1],
                times[1] - times[2],
                times[2] - times[3]
            ],
            [200, 0, 100]
        );
    }

    #[test]
    fn overlapping_fleets_can_make_a_gap_in_repayment() {
        let snapshot = SchedulerSnapshot {
            queued: vec![action(&[0], 100), action(&[0, 1], 200), action(&[1], 1000)],
            ..SchedulerSnapshot::default()
        };
        let mut times = Vec::new();
        for n in 0..=2 {
            let mut sim = Simulation::new(2);
            sim.add_instance(0, 0, 1, 300);
            sim.add_instance(1, 0, 1, 200);
            for i in 0..n {
                sim.add_instance(0, i + 1, 1, 150);
            }
            sim.dispatch(
                &snapshot,
                (
                    &[Some(0), Some(0), Some(1)],
                    &[
                        FleetSet::prefix(1).expect("valid"),
                        FleetSet::prefix(2).expect("valid"),
                        FleetSet::try_from_indices([1]).expect("valid"),
                    ],
                ),
                &[150, 150],
                0,
                None,
                None,
            );
            times.push(sim.completion);
        }
        assert_eq!(times, [1400, 1400, 1200]);
    }

    #[test]
    fn ties_spread_by_slot_then_instance_and_fleet() {
        let mut sim = Simulation::new(2);
        sim.add_instance(0, 0, 2, 0);
        sim.add_instance(0, 1, 2, 0);
        sim.add_instance(1, 0, 2, 0);
        let both = FleetSet::prefix(2).expect("two fleets fit");
        let first = sim.best(both).expect("slot exists");
        assert_eq!((first.1, first.0.index, first.0.instance), (0, 0, 0));
        sim.heaps[0].pop();
        let second = sim.best(both).expect("slot exists");
        assert_eq!((second.1, second.0.index, second.0.instance), (1, 0, 0));
        sim.heaps[1].pop();
        let third = sim.best(both).expect("slot exists");
        assert_eq!((third.1, third.0.index, third.0.instance), (0, 0, 1));
    }

    fn plan_queue(count: usize, duration: Option<u64>, standing: u32, maximum: u32) -> u32 {
        use gang_clock::TestClock;
        use gang_config::InstanceBounds;
        use gang_spi::FleetInstance;
        use gang_types::{InstanceId, InstanceLifecycle, nonzero_u32};

        let mut policy = gang_testkit::fleet_policy("workers").expect("valid fleet name");
        policy.instances = InstanceBounds::up_to(nonzero_u32(maximum));
        policy.target_actions_per_instance = nonzero_u32(16);
        policy.boot_grace = Duration::from_millis(150);
        let mut fleet = FleetPass::new(policy.clone());
        fleet.supply.desired = standing;
        for i in 0..standing {
            fleet.supply.instances.push(FleetInstance {
                id: InstanceId::try_new(format!("worker-{i}")).expect("valid instance id"),
                lifecycle: InstanceLifecycle::InService,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            });
        }
        let mut snapshot = SchedulerSnapshot::default();
        for _ in 0..count {
            let mut queued = action(&[0], duration.unwrap_or(150));
            queued.expected = duration.map(Duration::from_millis);
            fleet.outlook.demand.add_queued(queued.resources);
            snapshot.queued.push(queued);
        }
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let mut planner = WorkPlanner::new(&[policy]);
        for _ in 0..count {
            planner.attribute(Some(0), FleetSet::prefix(1).expect("valid"));
        }
        let capacity = Capacity {
            live: standing,
            standing,
            reliable: standing,
            draining: 0,
            instances: (0..standing as usize).collect(),
        };
        let clock = TestClock::new(Timestamp::from_unix_millis(1000));
        let mut fleets = [fleet];
        planner.plan(
            &mut fleets,
            &hosted,
            PassTime::read(&clock),
            &[capacity],
            &[None],
            &[false],
        );
        planner.work(0)
    }

    #[test]
    fn unknown_cold_queue_reaches_the_concurrency_bound() {
        assert_eq!(plan_queue(160, None, 0, 6), 6);
    }

    #[test]
    fn large_backlog_uses_all_headroom() {
        assert_eq!(plan_queue(2000, Some(30), 2, 20), 20);
    }

    #[test]
    fn projection_keeps_longest_remaining_independently_of_record_order() {
        let mut forward = vec![50, 150, 1000];
        let mut reverse = vec![1000, 150, 50];
        project_running(&mut forward, 2);
        project_running(&mut reverse, 2);
        assert_eq!(forward, [150, 1000]);
        assert_eq!(reverse, forward);
        project_running(&mut forward, 0);
        assert!(forward.is_empty());
    }

    #[test]
    fn missing_running_records_reserve_boot_time_before_projection() {
        use gang_clock::TestClock;
        use gang_config::InstanceBounds;
        use gang_spi::{ExecutingDemand, FleetInstance, WorkerState};
        use gang_types::{InstanceId, InstanceLifecycle, nonzero_u32};

        let mut policy = gang_testkit::fleet_policy("workers").expect("valid fleet");
        policy.instances = InstanceBounds::up_to(nonzero_u32(2));
        policy.target_actions_per_instance = nonzero_u32(2);
        policy.boot_grace = Duration::from_millis(150);
        let now = Timestamp::from_unix_millis(1000);
        let mut fleet = FleetPass::new(policy.clone());
        fleet.supply.desired = 1;
        fleet.supply.instances.push(FleetInstance {
            id: InstanceId::try_new("worker").expect("valid instance"),
            lifecycle: InstanceLifecycle::InService,
            launched_at: Timestamp::from_unix_millis(0),
            release_first: false,
        });
        fleet.outlook.demand.add_running(3);
        fleet.outlook.demand.add_queued(ActionResources::default());
        let mut snapshot = SchedulerSnapshot {
            workers: vec![WorkerState {
                id: gang_testkit::worker_id("worker").expect("valid worker"),
                fleet: policy.name.clone(),
                environment: Some(policy.action_environment.clone()),
                running_actions: 3,
                max_inflight_actions: Some(nonzero_u32(2)),
                draining: false,
                paused: false,
                last_seen: now,
            }],
            queued: vec![action(&[0], 10)],
            executing: vec![
                ExecutingDemand {
                    worker: 0,
                    started: now,
                    expected: Some(Duration::from_millis(50)),
                },
                ExecutingDemand {
                    worker: 0,
                    started: now,
                    expected: Some(Duration::from_secs(1)),
                },
            ],
            ..SchedulerSnapshot::default()
        };
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let mut planner = WorkPlanner::new(&[policy]);
        planner.attribute(Some(0), FleetSet::prefix(1).expect("valid"));
        let capacity = Capacity {
            live: 1,
            standing: 1,
            reliable: 1,
            draining: 0,
            instances: vec![0],
        };
        let mut fleets = [fleet];
        planner.plan(
            &mut fleets,
            &hosted,
            PassTime::read(&TestClock::new(now)),
            &[capacity],
            &[None],
            &[false],
        );
        assert_eq!(
            fleets[0].outlook.expected_wait,
            Some(Duration::from_millis(150))
        );
        assert_eq!(planner.work(0), 1);
    }

    #[test]
    fn newly_arriving_slots_win_ties_with_running_work() {
        use gang_clock::TestClock;
        use gang_config::InstanceBounds;
        use gang_spi::{ExecutingDemand, FleetInstance, WorkerState};
        use gang_types::{InstanceId, InstanceLifecycle, nonzero_u32};

        let mut policy = gang_testkit::fleet_policy("workers").expect("valid fleet");
        policy.instances = InstanceBounds::up_to(nonzero_u32(2));
        policy.target_actions_per_instance = nonzero_u32(16);
        policy.boot_grace = Duration::from_millis(150);
        let now = Timestamp::from_unix_millis(1000);
        let mut fleet = FleetPass::new(policy.clone());
        fleet.supply.desired = 1;
        fleet.supply.instances.push(FleetInstance {
            id: InstanceId::try_new("worker").expect("valid instance"),
            lifecycle: InstanceLifecycle::InService,
            launched_at: Timestamp::from_unix_millis(0),
            release_first: false,
        });
        fleet.outlook.demand.add_running(16);
        let mut snapshot = SchedulerSnapshot {
            workers: vec![WorkerState {
                id: gang_testkit::worker_id("worker").expect("valid worker"),
                fleet: policy.name.clone(),
                environment: Some(policy.action_environment.clone()),
                running_actions: 16,
                max_inflight_actions: Some(nonzero_u32(16)),
                draining: false,
                paused: false,
                last_seen: now,
            }],
            executing: vec![
                ExecutingDemand {
                    worker: 0,
                    started: now,
                    expected: Some(Duration::from_millis(150)),
                };
                16
            ],
            ..SchedulerSnapshot::default()
        };
        for _ in 0..16 {
            let mut queued = action(&[0], 150);
            queued.expected = None;
            fleet.outlook.demand.add_queued(queued.resources);
            snapshot.queued.push(queued);
        }
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let mut planner = WorkPlanner::new(&[policy]);
        for _ in 0..16 {
            planner.attribute(Some(0), FleetSet::prefix(1).expect("valid"));
        }
        let capacity = Capacity {
            live: 1,
            standing: 1,
            reliable: 1,
            draining: 0,
            instances: vec![0],
        };
        let mut fleets = [fleet];
        planner.plan(
            &mut fleets,
            &hosted,
            PassTime::read(&TestClock::new(now)),
            &[capacity],
            &[None],
            &[false],
        );
        assert_eq!(planner.work(0), 2);
    }
}
