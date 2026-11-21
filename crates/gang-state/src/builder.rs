//! Builds live state frames from control passes and independent observations.
//!
//! Control passes update pass metadata and reconciler outlook. Independent observations alone
//! update scheduler and provisioner measurements and their cumulative baselines. Local traffic,
//! builds, and cache measurements are sampled on every publication.
//!
//! Frames use caller-owned buffers. Workers and instances are ordered by resource name, so a
//! worker's phase clock is carried over by binary search in the preceding frame.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_reconciler::{Activity, FleetPass, HostedSnapshot};
use gang_spi::{
    FleetInstance, FleetSupply, QueuedActionView, SchedulerSnapshot, StoreKind, Traffic,
};
use gang_types::{FleetCatalog, FleetProperties, InstanceLifecycle, SaturatingInto};
use gang_wire::{
    Actions, BucketUsage, CacheStore, Demand, Event, Fleet, Instance, InstanceCounts, InstanceSize,
    Isa, Lifecycle, Pass, Phase, PhaseSpan, Policy, Queue, QueuedAction, Recent, RunningAction,
    SkipReason, State, StoreUsage, Truncated, Worker, WorkerCounts, name,
};

use crate::events::newer;
use crate::inputs::{ActionsSample, BuilderConfig, Inputs, PassStatus};

/// Cache counters observed at leadership start, before sampling begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheBaseline {
    /// Farm-wide traffic already counted before leadership.
    pub traffic: Traffic,
    /// Engine eviction total, or `None` if the engine could not be sampled.
    pub evicted: Option<u64>,
}

impl Default for CacheBaseline {
    fn default() -> Self {
        Self {
            traffic: Traffic::default(),
            evicted: Some(0),
        }
    }
}

/// Fills state frames, and remembers the pass history that a skipped pass reports.
#[derive(Debug)]
pub struct Builder {
    config: BuilderConfig,
    last_ok: Option<Timestamp>,
    skipped: u32,
    last_traffic: Traffic,
    last_evicted: Option<u64>,
    last_completed: Option<u64>,
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
    status_order: Vec<usize>,
    status_name: String,
    instance_name: String,
}

impl Builder {
    /// Creates a builder with cache counters observed at the start of leadership.
    #[must_use]
    pub fn new(config: BuilderConfig, baseline: CacheBaseline) -> Self {
        Self {
            config,
            last_ok: None,
            skipped: 0,
            last_traffic: baseline.traffic,
            last_evicted: baseline.evicted,
            last_completed: Some(0),
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
            recent: Recent::new(self.config.history_window, self.config.history_step),
            principals_epoch,
            ..State::default()
        }
    }

    /// Fills `cur` from a control pass or observation, numbered `seq`.
    pub fn fill(&mut self, cur: &mut State, prev: &State, seq: u64, inputs: &Inputs<'_>) {
        self.fill_header(cur, prev, seq, inputs);
        let traffic = self.traffic_increase(inputs.traffic);
        let mut completed = 0;
        let mut evicted = 0;
        let mut scheduler_known = None;
        let mut passed = false;
        let mut skipped = false;
        match &inputs.pass {
            PassStatus::Ok { applied, failed } => {
                self.last_ok = Some(inputs.wall);
                self.skipped = 0;
                cur.pass = Pass::ok(self.config.interval, *applied, *failed);
                copy_skipped_state(cur, prev);
                for (index, (pass, properties)) in inputs
                    .fleets
                    .iter()
                    .zip(inputs.catalog.fleets())
                    .enumerate()
                {
                    if let Some(row) = cur.fleets.get_mut(index) {
                        fill_fleet(row, pass, properties);
                    } else {
                        let mut row = Fleet::default();
                        fill_fleet(&mut row, pass, properties);
                        cur.fleets.push(row);
                    }
                }
                fill_infrastructure(
                    cur,
                    inputs.fleets,
                    inputs.infrastructure,
                    self.config.events_limit,
                    &mut self.scratch,
                );
                passed = true;
            }
            PassStatus::Skipped(reason) => {
                self.skipped = self.skipped.saturating_add(1);
                cur.pass = Pass::skipped(
                    self.config.interval,
                    reason.clone(),
                    self.last_ok,
                    self.skipped,
                );
                copy_skipped_state(cur, prev);
                skipped = true;
                // Only the independent observer establishes scheduler availability.
            }
            PassStatus::Observed { hosted, supplies } => {
                let (observed_completed, observed_evicted, known) =
                    self.observe(cur, prev, inputs, *hosted, supplies);
                completed = observed_completed;
                evicted = observed_evicted;
                scheduler_known = Some(known);
            }
        }
        let running = cur
            .fleets
            .iter()
            .fold(0_u32, |sum, fleet| sum.saturating_add(fleet.demand.running));
        let queued = cur.queue.total;
        cur.recent.sample_at(inputs.wall, |sample| {
            sample.lookups = sample
                .lookups
                .saturating_add(traffic.lookups.saturating_into());
            sample.hits = sample.hits.saturating_add(traffic.hits.saturating_into());
            sample.uploaded_bytes = sample.uploaded_bytes.saturating_add(traffic.uploaded);
            sample.downloaded_bytes = sample.downloaded_bytes.saturating_add(traffic.downloaded);
            sample.completed = sample.completed.saturating_add(completed.saturating_into());
            sample.evicted_bytes = sample.evicted_bytes.saturating_add(evicted);
            if let Some(known) = scheduler_known {
                sample.scheduler_known = Some(known && sample.scheduler_known != Some(false));
                if known {
                    sample.running = running;
                    sample.queued = queued;
                }
            }
            if passed {
                sample.passes = sample.passes.saturating_add(1);
            }
            if skipped {
                sample.skipped = sample.skipped.saturating_add(1);
            }
        });
        fill_actions(cur, prev, inputs, &self.config, &mut self.scratch);
    }

    fn fill_header(&self, cur: &mut State, prev: &State, seq: u64, inputs: &Inputs<'_>) {
        name::STATE.clone_into(&mut cur.name);
        cur.seq = seq;
        cur.update_time = inputs.wall;
        cur.provisioner.clone_from(&self.config.provisioner);
        cur.events_limit = self.config.events_limit.saturating_into();
        cur.principals_epoch = inputs.principals_epoch;
        cur.recent.samples.clone_from(&prev.recent.samples);
        cur.recent.window = self.config.history_window;
        cur.recent.step = self.config.history_step;
        fill_events(cur, inputs.events, self.config.events_limit);
        let since = Timestamp::from_unix_millis(
            inputs
                .wall
                .as_unix_millis()
                .saturating_sub(gang_clock::millis_of(self.config.history_window)),
        );
        inputs
            .builds
            .fill_recent(since, inputs.wall, &mut cur.builds);
        cur.cache.bucket = inputs.measured.map(|measured| BucketUsage {
            bytes: measured.bytes,
            objects: measured.objects,
            measure_time: measured.measured,
        });
    }

    fn observe(
        &mut self,
        cur: &mut State,
        prev: &State,
        inputs: &Inputs<'_>,
        hosted: Option<&HostedSnapshot>,
        supplies: &[bool],
    ) -> (u64, u64, bool) {
        cur.pass.clone_from(&prev.pass);
        copy_skipped_state(cur, prev);
        observe_fleets(
            cur,
            inputs.fleets,
            inputs.catalog,
            supplies,
            hosted,
            &mut self.scratch,
        );
        if supplies.len() >= inputs.fleets.len()
            && supplies
                .iter()
                .take(inputs.fleets.len())
                .all(|fresh| *fresh)
            && let Some(hosted) = hosted
        {
            fill_instances(cur, inputs.fleets, hosted, &mut self.scratch);
        }
        let Some(hosted) = hosted else {
            self.last_completed = None;
            self.last_evicted = None;
            return (0, 0, false);
        };
        let snapshot = hosted.snapshot();
        fill_workers(
            cur,
            prev,
            inputs.wall,
            snapshot,
            &self.scratch,
            self.config.history_window,
        );
        observe_scheduler(cur, inputs.fleets, snapshot);
        cur.completed = snapshot.completed;
        let known = self.last_completed.is_some();
        let (completed, evicted) = self.scheduler_increase(cur, snapshot);
        for worker in &mut cur.workers {
            if let Ok(index) = prev
                .workers
                .binary_search_by(|old| old.name.cmp(&worker.name))
            {
                copy_if_changed(
                    &mut worker.infrastructure,
                    &prev.workers[index].infrastructure,
                );
            } else {
                worker.infrastructure = None;
            }
        }
        for instance in &mut cur.instances {
            if let Ok(index) = prev
                .instances
                .binary_search_by(|old| old.name.cmp(&instance.name))
            {
                copy_if_changed(
                    &mut instance.infrastructure,
                    &prev.instances[index].infrastructure,
                );
            } else {
                instance.infrastructure = None;
            }
        }
        (completed, evicted, known)
    }

    /// Advances the independent local traffic baseline.
    fn traffic_increase(&mut self, traffic: Traffic) -> Traffic {
        let last = self.last_traffic;
        let delta = if traffic.lookups < last.lookups
            || traffic.hits < last.hits
            || traffic.uploaded < last.uploaded
            || traffic.downloaded < last.downloaded
        {
            traffic
        } else {
            Traffic {
                lookups: traffic.lookups - last.lookups,
                hits: traffic.hits - last.hits,
                uploaded: traffic.uploaded - last.uploaded,
                downloaded: traffic.downloaded - last.downloaded,
            }
        };
        self.last_traffic = traffic;
        delta
    }

    /// Refreshes scheduler stores and advances scheduler-owned counter baselines.
    fn scheduler_increase(&mut self, cur: &mut State, snapshot: &SchedulerSnapshot) -> (u64, u64) {
        refill(&mut cur.cache.stores, &snapshot.stores, |row, store| {
            *row = StoreUsage {
                store: match store.store {
                    StoreKind::Content => CacheStore::Content,
                    StoreKind::Action => CacheStore::Action,
                },
                used_bytes: store.used,
                budget_bytes: store.budget,
            };
            true
        });
        let evicted_total = snapshot
            .stores
            .iter()
            .fold(0_u64, |sum, store| sum.saturating_add(store.evicted));
        let evicted = self
            .last_evicted
            .map_or(0, |last| increase(evicted_total, last));
        let completed = self
            .last_completed
            .map_or(0, |last| increase(snapshot.completed, last));
        self.last_evicted = Some(evicted_total);
        self.last_completed = Some(snapshot.completed);
        (completed, evicted)
    }
}

/// Projects live operational observations without changing physical fleet counts.
fn observe_fleets(
    cur: &mut State,
    fleets: &[FleetPass],
    catalog: &FleetCatalog,
    supplies: &[bool],
    hosted: Option<&HostedSnapshot>,
    scratch: &mut Scratch,
) {
    let all_hosted = hosted.is_some()
        && supplies.len() >= fleets.len()
        && supplies.iter().take(fleets.len()).all(|fresh| *fresh);
    for (index, (pass, properties)) in fleets.iter().zip(catalog.fleets()).enumerate() {
        if cur.fleets.get(index).is_none() {
            let mut row = Fleet::default();
            fill_fleet(&mut row, pass, properties);
            cur.fleets.push(row);
        }
        if !supplies.get(index).copied().unwrap_or(false) {
            continue;
        }
        let row = &mut cur.fleets[index];
        row.desired = pass.supply.requested.unwrap_or(pass.supply.desired);
        row.instances = instance_counts(&pass.supply);
        if let Some(failure) = &pass.supply.launch_failure {
            row.launch_failure
                .get_or_insert_with(|| gang_wire::LaunchFailure {
                    reason: String::new(),
                })
                .reason
                .clone_from(&failure.reason);
        } else {
            row.launch_failure = None;
        }
        if all_hosted {
            continue;
        }
        cur.instances.retain(|existing| {
            if existing.fleet != row.name {
                return true;
            }
            pass.supply.instances.iter().any(|instance| {
                name::write_of::<name::Instances>(&mut scratch.instance_name, &instance.id);
                existing.name == scratch.instance_name
                    && hosted.is_none_or(|hosted| {
                        hosted
                            .workers_on(&pass.policy.name, &instance.id)
                            .next()
                            .is_none()
                    })
            })
        });
        for (position, instance) in pass.supply.instances.iter().enumerate() {
            name::write_of::<name::Instances>(&mut scratch.instance_name, &instance.id);
            let connected = hosted.map_or_else(
                || {
                    cur.workers.iter().any(|worker| {
                        worker.fleet == row.name && worker.instance == scratch.instance_name
                    })
                },
                |hosted| {
                    hosted
                        .workers_on(&pass.policy.name, &instance.id)
                        .next()
                        .is_some()
                },
            );
            if !connected {
                let index = match cur
                    .instances
                    .binary_search_by(|existing| existing.name.cmp(&scratch.instance_name))
                {
                    Ok(index) => index,
                    Err(index) => {
                        let mut new = Instance::default();
                        new.name.clone_from(&scratch.instance_name);
                        cur.instances.insert(index, new);
                        index
                    }
                };
                fill_instance(&mut cur.instances[index], pass, instance, position);
            }
        }
    }
    cur.instances.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    if let Some(hosted) = hosted {
        scratch.launches.clear();
        scratch
            .launches
            .resize(hosted.snapshot().workers.len(), None);
        for (index, pass) in fleets.iter().enumerate() {
            if supplies.get(index).copied().unwrap_or(false) {
                for instance in &pass.supply.instances {
                    for worker in hosted.workers_on(&pass.policy.name, &instance.id) {
                        if let Some(launch) = scratch.launches.get_mut(worker) {
                            *launch = Some(instance.launched_at);
                        }
                    }
                }
            }
        }
    }
}

fn observe_scheduler(cur: &mut State, fleets: &[FleetPass], snapshot: &SchedulerSnapshot) {
    for row in &mut cur.fleets {
        row.workers = WorkerCounts::default();
        row.demand.running = 0;
        row.demand.queued = 0;
    }
    for worker in &snapshot.workers {
        if let Some((index, _)) = fleets
            .iter()
            .enumerate()
            .find(|(_, pass)| pass.policy.name == worker.fleet)
            && let Some(row) = cur.fleets.get_mut(index)
        {
            let count = match Activity::from(worker) {
                Activity::Busy => &mut row.workers.busy,
                Activity::Idle => &mut row.workers.idle,
                Activity::Draining => &mut row.workers.draining,
            };
            *count = count.saturating_add(1);
            row.demand.running = row.demand.running.saturating_add(worker.running_actions);
        }
    }
    for instance in &cur.instances {
        if instance.lifecycle != Lifecycle::InService {
            continue;
        }
        if let Some(row) = cur.fleets.iter_mut().find(|row| row.name == instance.fleet) {
            row.workers.unconnected = row.workers.unconnected.saturating_add(1);
        }
    }
    for action in &snapshot.queued {
        if let Some(row) = action
            .candidates
            .iter()
            .next()
            .and_then(|index| cur.fleets.get_mut(index))
        {
            row.demand.queued = row.demand.queued.saturating_add(1);
        }
    }
    let total: u32 = snapshot.queued.len().saturating_into();
    let charged = cur
        .fleets
        .iter()
        .fold(0_u32, |sum, row| sum.saturating_add(row.demand.queued));
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

fn fill_infrastructure(
    cur: &mut State,
    fleets: &[FleetPass],
    records: &[gang_spi::FleetInfrastructureEvent],
    limit: usize,
    scratch: &mut Scratch,
) {
    scratch.status_order.clear();
    scratch.status_order.extend(0..records.len());
    scratch.status_order.sort_unstable_by(|&a, &b| {
        records[a].event.at.cmp(&records[b].event.at).then_with(|| {
            name::cmp_of::<name::AwsEvents>(&records[a].event.id, &records[b].event.id)
        })
    });
    let start = scratch.status_order.len().saturating_sub(limit);
    let selected = &scratch.status_order[start..];
    for (index, &source) in selected.iter().enumerate() {
        let record = &records[source];
        let pass = fleets.iter().find(|pass| pass.policy.name == record.fleet);
        if let Some(offset) = cur.status_events.iter().skip(index).position(|event| {
            matches!(&event.change, gang_wire::EventChange::Aws { event } if event.id == record.event.id)
        }) {
            cur.status_events.swap(index, index + offset);
        }
        if let Some(existing) = cur.status_events.get_mut(index) {
            if !crate::infrastructure::status_event_matches(
                existing,
                record,
                pass,
                records,
                &mut scratch.status_name,
            ) {
                *existing = crate::infrastructure::status_event(record, pass, records);
            }
        } else {
            cur.status_events
                .push(crate::infrastructure::status_event(record, pass, records));
        }
    }
    cur.status_events.truncate(selected.len());
    for (pass, fleet) in fleets.iter().zip(&cur.fleets) {
        for worker in cur
            .workers
            .iter_mut()
            .filter(|worker| worker.fleet == fleet.name)
        {
            let member = pass.supply.instances.iter().find(|instance| {
                name::write_of::<name::Instances>(&mut scratch.instance_name, &instance.id);
                worker.instance == scratch.instance_name
            });
            let status = member.and_then(|member| {
                crate::infrastructure::member_status_ref(pass, records, &member.id)
            });
            update_infrastructure(&mut worker.infrastructure, status, &mut scratch.status_name);
        }
        for row in cur
            .instances
            .iter_mut()
            .filter(|row| row.fleet == fleet.name)
        {
            let member = pass.supply.instances.iter().find(|instance| {
                name::write_of::<name::Instances>(&mut scratch.instance_name, &instance.id);
                row.name == scratch.instance_name
            });
            let status = member.and_then(|member| {
                crate::infrastructure::member_status_ref(pass, records, &member.id)
            });
            update_infrastructure(&mut row.infrastructure, status, &mut scratch.status_name);
        }
    }
    for worker in &mut cur.workers {
        if !cur.fleets.iter().any(|fleet| fleet.name == worker.fleet) {
            worker.infrastructure = None;
        }
    }
}

fn update_infrastructure(
    slot: &mut Option<gang_wire::InfrastructureStatus>,
    status: Option<crate::infrastructure::MemberStatus<'_>>,
    scratch: &mut String,
) {
    let Some(status) = status else {
        *slot = None;
        return;
    };
    if let Some(other) = status.other {
        name::write_of::<name::Instances>(scratch, other);
    }
    if let Some(existing) = slot {
        existing.phase = status.phase;
        existing.reason = status.reason;
        if status.other.is_some() {
            existing
                .other_instance
                .get_or_insert_with(String::new)
                .clone_from(scratch);
        } else {
            existing.other_instance = None;
        }
    } else {
        *slot = Some(gang_wire::InfrastructureStatus {
            phase: status.phase,
            reason: status.reason,
            other_instance: status.other.map(|_| scratch.clone()),
        });
    }
}

/// A counter decrease indicates a restarted source.
fn increase(total: u64, previous: u64) -> u64 {
    total.checked_sub(previous).unwrap_or(total)
}

fn copy_skipped_state(cur: &mut State, prev: &State) {
    copy_if_changed(&mut cur.fleets, &prev.fleets);
    copy_if_changed(&mut cur.workers, &prev.workers);
    copy_if_changed(&mut cur.instances, &prev.instances);
    copy_if_changed(&mut cur.status_events, &prev.status_events);
    copy_if_changed(&mut cur.queue, &prev.queue);
    cur.cache.stores.clone_from(&prev.cache.stores);
    cur.completed = prev.completed;
}

// Derived row `clone_from` reallocates unchanged nested data.
fn copy_if_changed<T: Clone + PartialEq>(into: &mut T, from: &T) {
    if into != from {
        into.clone_from(from);
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
    history_window: Duration,
) {
    let horizon = Timestamp::from_unix_millis(
        wall.as_unix_millis()
            .saturating_sub(gang_clock::millis_of(history_window)),
    );
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
            if let Some(earlier) = earlier {
                row.phases.clone_from(&earlier.phases);
            } else {
                row.phases.clear();
            }
            if row.phases.last().is_none_or(|span| span.phase != row.phase) {
                row.phases.push(PhaseSpan {
                    phase: row.phase.clone(),
                    start: row.phase_time,
                });
            }
            let ended = row
                .phases
                .windows(2)
                .take_while(|spans| spans[1].start <= horizon)
                .count();
            row.phases.drain(..ended);
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
        min: pass.outlook.minimum,
        max: policy.instances.max().get(),
        target_actions: policy.target_actions_per_instance.get(),
        idle_timeout: policy.idle_timeout,
        max_drain_wait: policy.max_drain_wait,
        boot_grace: policy.boot_grace,
    };
    let running = row.demand.running;
    let queued = row.demand.queued;
    row.demand = Demand {
        running,
        queued,
        candidates: pass.outlook.candidates,
        needed: pass.outlook.needed,
        vcpus: pass.outlook.demand.vcpus().saturating_into(),
        memory: pass.outlook.demand.memory(),
        readiness: pass.outlook.readiness,
        expected_wait: pass.outlook.expected_wait,
        load: pass.outlook.load,
        backlog: pass.outlook.backlog,
        utilization: pass.outlook.utilization,
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

/// Fills the sampled actions, dropping the running rows whose worker is no longer in
/// `cur.workers`. When the controller has no sample, keeps the previous frame's actions as they
/// are, running rows of vanished workers included: a stale sample is flagged by its own
/// `sample_time` and `stale_after`.
fn fill_actions(
    cur: &mut State,
    prev: &State,
    inputs: &Inputs<'_>,
    config: &BuilderConfig,
    scratch: &mut Scratch,
) {
    let Some(sample) = inputs.actions else {
        copy_if_changed(&mut cur.actions, &prev.actions);
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
        &mut scratch.running_order,
        &mut scratch.instance_name,
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
    worker_name: &mut String,
) {
    let running = &sample.view.running;
    order.clear();
    order.extend(0..running.len());
    order.sort_unstable_by(|&a, &b| {
        name::cmp_of::<name::Actions>(&running[a].info.op, &running[b].info.op)
    });
    let matched = order.iter().filter_map(|&index| {
        let action = &running[index];
        name::write_of::<name::Workers>(worker_name, &action.worker);
        find_worker(workers, worker_name).map(|worker| (action, worker))
    });
    refill(rows, matched, |row, (action, worker)| {
        row.worker.clone_from(&worker.name);
        row.fleet.clone_from(&worker.fleet);
        name::write_of::<name::Actions>(&mut row.name, &action.info.op);
        row.digest.clone_from(&action.info.digest);
        row.target.clone_from(&action.info.target);
        row.mnemonic.clone_from(&action.info.mnemonic);
        if let Some(build) = action.info.build {
            name::write_of::<name::Builds>(&mut row.build, &build);
        } else {
            row.build.clear();
        }
        row.start_time = action.started;
        row.priority = action.info.priority;
        true
    });
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
    if let Some(build) = queued.info.build {
        name::write_of::<name::Builds>(&mut row.build, &build);
    } else {
        row.build.clear();
    }
    row.queue_time = queued.queued_since;
    row.priority = queued.info.priority;
    if let Some(refusal) = &queued.refusal {
        let text = row.refusal.get_or_insert_with(String::new);
        text.clear();
        let _ = write!(text, "{refusal}");
    } else {
        row.refusal = None;
    }
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
