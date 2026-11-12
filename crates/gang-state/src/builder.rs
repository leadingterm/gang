//! Builds each live state frame from the outputs of one control pass.
//!
//! A frame is filled into a caller-owned [`State`] so that its vectors and strings keep their
//! capacity from one pass to the next: a completed pass rewrites the rows in place and allocates
//! nothing once the frame is warm. Workers and instances are kept ordered by resource name, so
//! the phase clock of a worker is carried over from the previous frame by a binary search of the
//! previous frame's rows, which are sorted by name too.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_reconciler::{Activity, FleetPass, HostedSnapshot};
use gang_spi::{FleetInstance, FleetSupply, QueuedActionView, SchedulerSnapshot};
use gang_types::{FleetCatalog, FleetProperties, InstanceLifecycle, SaturatingInto};
use gang_wire::{
    Actions, Demand, Event, Fleet, Instance, InstanceCounts, InstanceSize, Isa, Lifecycle, Pass,
    Phase, Policy, Queue, QueuedAction, RunningAction, SkipReason, State, Truncated, Worker,
    WorkerCounts, name,
};

use crate::events::newer;
use crate::inputs::{ActionsSample, BuilderConfig, Inputs, PassStatus};

/// Fills state frames, and remembers the pass history that a skipped pass reports.
#[derive(Debug)]
pub struct Builder {
    config: BuilderConfig,
    last_ok: Option<Timestamp>,
    skipped: u32,
    scratch: Scratch,
}

/// Buffers reused by completed passes after growing to fit the snapshot.
#[derive(Debug, Default)]
struct Scratch {
    /// The launch time of the supply instance each snapshot worker runs on, by worker index;
    /// None when that instance is not in the worker's fleet's supply.
    launches: Vec<Option<Timestamp>>,
    /// Indices of sampled running actions, ordered by encoded action name.
    running_order: Vec<usize>,
}

impl Builder {
    /// Creates a builder that has seen no successful pass yet.
    #[must_use]
    pub fn new(config: BuilderConfig) -> Self {
        Self {
            config,
            last_ok: None,
            skipped: 0,
            scratch: Scratch::default(),
        }
    }

    /// The frame served before the first pass: no pass has run, so it reports the pass skipped
    /// because the scheduler has not been read, with no rows, numbered 0, at `wall`.
    #[must_use]
    pub fn initial(&self, wall: Timestamp, principals_epoch: u64) -> State {
        State {
            name: name::STATE.to_owned(),
            update_time: wall,
            provisioner: self.config.provisioner.clone(),
            pass: Pass::skipped(
                self.config.interval,
                SkipReason::SchedulerUnavailable,
                None,
                0,
            ),
            events_limit: self.config.events_limit.saturating_into(),
            principals_epoch,
            ..State::default()
        }
    }

    /// Fills `cur` with the frame for `inputs`, numbered `seq`.
    ///
    /// A completed pass rewrites every row of `cur` from `inputs`. A skipped pass copies the rows
    /// of `prev` unchanged and reports the skip. The actions part is refreshed in both cases.
    pub fn fill(&mut self, cur: &mut State, prev: &State, seq: u64, inputs: &Inputs<'_>) {
        name::STATE.clone_into(&mut cur.name);
        cur.seq = seq;
        cur.update_time = inputs.wall;
        cur.provisioner.clone_from(&self.config.provisioner);
        cur.events_limit = self.config.events_limit.saturating_into();
        cur.principals_epoch = inputs.principals_epoch;
        fill_events(cur, inputs.events, self.config.events_limit);
        let interval = self.config.interval;
        match &inputs.pass {
            PassStatus::Ok {
                applied,
                failed,
                hosted,
            } => {
                let snapshot = hosted.snapshot();
                self.last_ok = Some(inputs.wall);
                self.skipped = 0;
                cur.pass = Pass::ok(interval, *applied, *failed);
                cur.completed = snapshot.completed;
                fill_instances(cur, inputs.fleets, hosted, &mut self.scratch);
                fill_workers(cur, prev, inputs.wall, snapshot, &self.scratch);
                fill_fleets(cur, inputs.fleets, inputs.catalog);
                fill_queue(cur, inputs.fleets, snapshot);
            }
            PassStatus::Skipped(reason) => {
                self.skipped = self.skipped.saturating_add(1);
                cur.pass = Pass::skipped(interval, reason.clone(), self.last_ok, self.skipped);
                cur.fleets.clone_from(&prev.fleets);
                cur.workers.clone_from(&prev.workers);
                cur.instances.clone_from(&prev.instances);
                cur.queue.clone_from(&prev.queue);
                cur.completed = prev.completed;
            }
        }
        fill_actions(
            cur,
            prev,
            inputs,
            &self.config,
            &mut self.scratch.running_order,
        );
    }
}

/// Overwrites `rows` in place from `source`: the next source item is filled into the next existing row, or into a default row pushed when there is none; a row `fill` declines (returns false) is reused for the next item; rows past the last kept one are dropped. Allocates only while `rows` grows.
fn refill<T: Default, S>(
    rows: &mut Vec<T>,
    source: impl IntoIterator<Item = S>,
    mut fill: impl FnMut(&mut T, S) -> bool,
) {
    let mut kept = 0;
    for item in source {
        if kept == rows.len() {
            rows.push(T::default());
        }
        if fill(&mut rows[kept], item) {
            kept += 1;
        }
    }
    rows.truncate(kept);
}

/// Overwrites `into` with the resource name of collection `C` for the text of an ID that the
/// caller knows is valid for `C`. The name is the one [`name::write_of`] builds, but no typed ID
/// is made from the text, so nothing is allocated.
fn write_text_of<C: name::Collection>(into: &mut String, id: &str) {
    into.clear();
    into.push_str(C::NAME);
    into.push('/');
    name::push_segment(into, id);
}

/// Fills the worker rows, ordered by name. A worker whose instance is not in its fleet's supply
/// keeps the launch time of its earlier row, which is the wall time the first time the worker is
/// seen. A worker whose id names no instance is left out.
fn fill_workers(
    cur: &mut State,
    prev: &State,
    wall: Timestamp,
    snapshot: &SchedulerSnapshot,
    scratch: &Scratch,
) {
    // Worker ids contain only unreserved characters, so worker name order is id order.
    let mut previous = prev.workers.iter().peekable();
    refill(
        &mut cur.workers,
        snapshot.workers.iter().enumerate(),
        |row, (index, worker)| {
            let Some(key) = worker.id.instance_name() else {
                return false;
            };
            name::write_of::<name::Workers>(&mut row.name, &worker.id);
            while previous
                .peek()
                .is_some_and(|earlier| earlier.name.as_str() < row.name.as_str())
            {
                previous.next();
            }
            let earlier = previous
                .peek()
                .copied()
                .filter(|earlier| earlier.name == row.name);
            let phase = phase(Activity::from(worker));
            row.phase_time = earlier
                .filter(|row| row.phase == phase)
                .map_or(wall, |row| row.phase_time);
            name::write_of::<name::Fleets>(&mut row.fleet, &worker.fleet);
            write_text_of::<name::Instances>(&mut row.instance, key);
            row.phase = phase;
            row.running = worker.running_actions;
            row.slots = worker.max_inflight_actions.map(NonZeroU32::get);
            row.paused = worker.paused;
            row.launch_time = scratch
                .launches
                .get(index)
                .copied()
                .flatten()
                .unwrap_or_else(|| earlier.map_or(wall, |row| row.launch_time));
            true
        },
    );
}

/// Maps an autoscaler activity to its wire phase.
const fn phase(activity: Activity) -> Phase {
    match activity {
        Activity::Busy => Phase::Busy,
        Activity::Idle => Phase::Idle,
        Activity::Draining => Phase::Draining,
    }
}

/// Replaces the events with the window `events`, oldest first. The window slides forward, so the
/// events `cur` already holds are kept and only the newer tail is cloned.
fn fill_events(cur: &mut State, events: &[Event], limit: usize) {
    let events = &events[events.len().saturating_sub(limit)..];
    let Some(first) = events.first() else {
        cur.events.clear();
        return;
    };
    if let Some(expired) = cur.events.iter().position(|event| event.name == first.name) {
        cur.events.drain(..expired);
    }
    if let Some(tail) = newer(&cur.events, events) {
        cur.events.extend_from_slice(tail);
    } else {
        cur.events.clear();
        cur.events.extend_from_slice(events);
    }
}

/// Fills instances without a hosting worker, ordered by name across fleets. An instance counts
/// as hosted only when a worker of its own fleet runs on it. Every worker of the fleet on a
/// hosted instance gets the instance's launch time in `scratch.launches`.
fn fill_instances(
    cur: &mut State,
    fleets: &[FleetPass],
    hosted: &HostedSnapshot,
    scratch: &mut Scratch,
) {
    let launches = &mut scratch.launches;
    let workers = &hosted.snapshot().workers;
    launches.clear();
    launches.resize(workers.len(), None);
    let triples = fleets.iter().flat_map(|pass| {
        pass.supply
            .instances
            .iter()
            .enumerate()
            .map(move |(position, instance)| (pass, position, instance))
    });
    refill(
        &mut cur.instances,
        triples,
        |row, (pass, position, instance)| {
            let mut hosts = hosted
                .workers_on(&pass.policy.name, &instance.id)
                .peekable();
            if hosts.peek().is_some() {
                for worker in hosts {
                    if let Some(launch) = launches.get_mut(worker) {
                        launch.get_or_insert(instance.launched_at);
                    }
                }
                return false;
            }
            name::write_of::<name::Instances>(&mut row.name, &instance.id);
            fill_instance(row, pass, instance, position);
            true
        },
    );
    cur.instances.sort_unstable_by(|a, b| a.name.cmp(&b.name));
}

/// Fills the fields of one unconnected instance row; its name is already in `row`.
fn fill_instance(row: &mut Instance, pass: &FleetPass, instance: &FleetInstance, position: usize) {
    name::write_of::<name::Fleets>(&mut row.fleet, &pass.policy.name);
    row.lifecycle = Lifecycle::from(instance.lifecycle);
    row.launch_time = instance.launched_at;
    row.past_boot_grace = pass.past_boot_grace.get(position).is_some_and(|&past| past);
}

/// Fills one row per fleet, in declaration order, from the pass outlook.
fn fill_fleets(cur: &mut State, fleets: &[FleetPass], catalog: &FleetCatalog) {
    refill(
        &mut cur.fleets,
        fleets.iter().zip(catalog.fleets()),
        |row, (pass, properties)| {
            fill_fleet(row, pass, properties);
            true
        },
    );
}

/// Overwrites one fleet row from its policy, supply, and outlook. The properties are rebuilt only
/// when they differ from the row's.
fn fill_fleet(row: &mut Fleet, pass: &FleetPass, properties: &FleetProperties) {
    let policy = &pass.policy;
    name::write_of::<name::Fleets>(&mut row.name, &policy.name);
    row.isa = Isa::from(policy.isa);
    row.sandbox.clear();
    row.sandbox.push_str(properties.sandbox());
    if !same_properties(&row.properties, properties) {
        row.properties.clear();
        row.properties.extend(
            properties
                .options()
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        );
    }
    row.instance_size = Some(InstanceSize {
        vcpus: policy.vcpus_per_instance.get(),
        memory: policy.memory_per_instance.as_u64(),
    });
    row.policy = Policy {
        min: policy.instances.min(),
        max: policy.instances.max().get(),
        target_actions: policy.target_actions_per_instance.get(),
        idle_timeout: policy.idle_timeout,
        max_drain_wait: policy.max_drain_wait,
        boot_grace: policy.boot_grace,
    };
    row.desired = pass.supply.desired;
    row.instances = instance_counts(&pass.supply);
    row.workers = WorkerCounts {
        busy: pass.outlook.workers.busy,
        idle: pass.outlook.workers.idle,
        draining: pass.outlook.workers.draining,
        unconnected: pass.outlook.workers.unconnected,
    };
    row.demand = Demand {
        running: pass.outlook.workers.running,
        queued: pass.outlook.queued,
        candidates: pass.outlook.candidates,
        needed: pass.outlook.needed,
        vcpus: pass.outlook.demand.vcpus().saturating_into(),
        memory: pass.outlook.demand.memory(),
    };
}

/// Counts the supply instances of a fleet by lifecycle, and reports its warm pool size.
fn instance_counts(supply: &FleetSupply) -> InstanceCounts {
    let mut counts = InstanceCounts {
        pending: 0,
        in_service: 0,
        terminating: 0,
        warm: supply.warm,
    };
    for instance in &supply.instances {
        let count = match instance.lifecycle {
            InstanceLifecycle::Pending => &mut counts.pending,
            InstanceLifecycle::InService => &mut counts.in_service,
            InstanceLifecycle::Terminating => &mut counts.terminating,
        };
        *count = count.saturating_add(1);
    }
    counts
}

/// Whether `row` holds exactly the provisioner options of `properties`. A row with a key that
/// the options repeat holds fewer entries than the options, so it counts as different.
fn same_properties(row: &BTreeMap<String, String>, properties: &FleetProperties) -> bool {
    row.len() == properties.options().count()
        && properties
            .options()
            .all(|(key, value)| row.get(key).is_some_and(|held| held.as_str() == value))
}

/// Fills the queue counts. Actions no fleet was charged for are unplaceable.
fn fill_queue(cur: &mut State, fleets: &[FleetPass], snapshot: &SchedulerSnapshot) {
    let total: u32 = snapshot.queued.len().saturating_into();
    let charged = fleets
        .iter()
        .fold(0_u32, |sum, pass| sum.saturating_add(pass.outlook.queued));
    cur.queue = Queue {
        total,
        unplaceable: total.saturating_sub(charged),
        oldest_queue_time: snapshot
            .queued
            .iter()
            .map(|action| action.queued_since)
            .min(),
    };
}

/// Fills the sampled actions, dropping the running rows whose worker is no longer in
/// `cur.workers`. When the controller has no sample, keeps the previous frame's actions as they
/// are, running rows of vanished workers included: a stale sample is flagged by its own
/// `sample_time` and `stale_after`.
fn fill_actions(
    cur: &mut State,
    prev: &State,
    inputs: &Inputs<'_>,
    config: &BuilderConfig,
    running_order: &mut Vec<usize>,
) {
    let Some(sample) = inputs.actions else {
        cur.actions.clone_from(&prev.actions);
        return;
    };
    let State {
        workers, actions, ..
    } = cur;
    let actions = actions.get_or_insert_with(Actions::default);
    fill_action_header(actions, sample, config.stale_after);
    fill_running(
        &mut actions.running,
        workers.as_slice(),
        sample,
        running_order,
    );
    fill_queued(&mut actions.queued, sample, inputs);
}

/// Sets the sample time, staleness bound and truncation counts of an actions part.
fn fill_action_header(actions: &mut Actions, sample: &ActionsSample, stale_after: Duration) {
    actions.sample_time = sample.at;
    actions.stale_after = stale_after;
    actions.truncated = Truncated {
        running: sample.view.truncated_running.saturating_into(),
        queued: sample.view.truncated_queued.saturating_into(),
    };
}

/// Fills the running rows in place. Each row takes its fleet from its worker, which is found by
/// name in `workers`, sorted by name; a row whose worker is absent is not kept.
fn fill_running(
    rows: &mut Vec<RunningAction>,
    workers: &[Worker],
    sample: &ActionsSample,
    order: &mut Vec<usize>,
) {
    let running = &sample.view.running;
    order.clear();
    order.extend(0..running.len());
    order.sort_unstable_by(|&a, &b| {
        name::cmp_of::<name::Actions>(&running[a].info.op, &running[b].info.op)
    });
    refill(
        rows,
        order.iter().map(|&index| &running[index]),
        |row, action| {
            name::write_of::<name::Workers>(&mut row.worker, &action.worker);
            let Some(worker) = find_worker(workers, &row.worker) else {
                return false;
            };
            row.fleet.clone_from(&worker.fleet);
            name::write_of::<name::Actions>(&mut row.name, &action.info.op);
            row.digest.clone_from(&action.info.digest);
            row.target.clone_from(&action.info.target);
            row.mnemonic.clone_from(&action.info.mnemonic);
            row.start_time = action.started;
            row.priority = action.info.priority;
            true
        },
    );
}

/// Finds the worker named `name` among `workers`, which is sorted by name.
fn find_worker<'a>(workers: &'a [Worker], name: &str) -> Option<&'a Worker> {
    workers
        .binary_search_by(|worker| worker.name.as_str().cmp(name))
        .ok()
        .and_then(|index| workers.get(index))
}

/// Fills the queued rows in place, one per queued action.
fn fill_queued(rows: &mut Vec<QueuedAction>, sample: &ActionsSample, inputs: &Inputs<'_>) {
    refill(rows, sample.view.queued.iter(), |row, queued| {
        fill_queued_row(row, queued, inputs);
        true
    });
}

/// Overwrites one queued row, naming the fleets of its candidate set in index order.
fn fill_queued_row(row: &mut QueuedAction, queued: &QueuedActionView, inputs: &Inputs<'_>) {
    name::write_of::<name::Actions>(&mut row.name, &queued.info.op);
    row.digest.clone_from(&queued.info.digest);
    row.target.clone_from(&queued.info.target);
    row.mnemonic.clone_from(&queued.info.mnemonic);
    row.queue_time = queued.queued_since;
    row.priority = queued.info.priority;
    refill(
        &mut row.candidates,
        queued
            .candidates
            .iter()
            .filter_map(|index| inputs.fleets.get(index)),
        |candidate, pass| {
            name::write_of::<name::Fleets>(candidate, &pass.policy.name);
            true
        },
    );
}
