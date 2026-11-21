//! The `state` singleton: the live view of the fleets, workers, instances and sampled actions
//! that `GET /v1/state` serves and `GET /v1/state:watch` streams.
//!
//! Timestamps are RFC 3339 and durations are Google `Duration` strings; sizes are bytes. The
//! recorded fixtures in `ui/fixtures` round trip through these types in the crate's tests.

use std::collections::BTreeMap;
use std::time::Duration;

use gang_clock::{Timestamp, millis_of};
use serde::{Deserialize, Serialize};

use crate::{Build, Event, Isa};

wire_enum! {
    /// Why a scheduling pass was skipped.
    pub enum SkipReason {
        /// The scheduler could not be reached.
        SchedulerUnavailable = "SCHEDULER_UNAVAILABLE",
        /// The provisioner could not be reached.
        ProvisionerUnavailable = "PROVISIONER_UNAVAILABLE",
        /// The leader's confirmed lease is no longer valid.
        LeadershipLapsed = "LEADERSHIP_LAPSED",
    }
}

wire_enum! {
    /// Whether the last scheduling pass ran or was skipped.
    pub enum PassState default Ok {
        /// The pass completed.
        Ok = "OK",
        /// The pass was skipped.
        Skipped = "SKIPPED",
    }
}

wire_enum! {
    /// The phase of a worker.
    pub enum Phase default Idle {
        /// The worker is running at least one action.
        Busy = "BUSY",
        /// The worker is connected and has no action running.
        Idle = "IDLE",
        /// The worker is draining before retirement.
        Draining = "DRAINING",
    }
}

wire_enum! {
    /// The lifecycle of an instance.
    pub enum Lifecycle default Pending from gang_types::InstanceLifecycle {
        /// The instance is launching.
        Pending = "PENDING",
        /// The instance is in service.
        InService = "IN_SERVICE",
        /// The instance is shutting down.
        Terminating = "TERMINATING",
    }
}

wire_enum! {
    /// A bounded engine cache store; readers tolerate unknown values.
    pub enum CacheStore default Content {
        /// Content-addressable store.
        Content = "CAS",
        /// Action cache.
        Action = "AC",
    }
}

wire_enum! {
    /// The role of an instance in an infrastructure replacement.
    pub enum InfrastructurePhase {
        /// The instance is being replaced.
        Replacing = "REPLACING",
        /// The instance is a replacement.
        Replacement = "REPLACEMENT",
    }
}

wire_enum! {
    /// The observation behind an infrastructure replacement.
    pub enum InfrastructureReason {
        /// A spot rebalance recommendation or replacement.
        SpotRebalance = "SPOT_REBALANCE",
        /// A spot interruption notice.
        SpotInterruption = "SPOT_INTERRUPTION",
    }
}

/// A derived infrastructure status for one instance.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct InfrastructureStatus {
    /// Whether this instance is being replaced or is the replacement.
    pub phase: InfrastructurePhase,
    /// The observation behind the status.
    pub reason: InfrastructureReason,
    /// Reference `instances/{instance}` of the paired instance, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other_instance: Option<String>,
}

/// The live state, the body of `GET /v1/state` and the payload of a `snapshot` frame.
///
/// The default is an empty frame; served frames are built by the state builder.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct State {
    /// Resource name, `state` in served frames.
    pub name: String,
    /// Sequence number of the latest frame; it advances only with a delta.
    #[ts(type = "number")]
    pub seq: u64,
    /// The server clock of the latest frame: the snapshot's, then each delta's.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub update_time: Timestamp,
    /// The configured provisioner plugin's key, such as `aws` or `local`.
    pub provisioner: String,
    /// The outcome of the last scheduling pass.
    pub pass: Pass,
    /// The action queue.
    pub queue: Queue,
    /// Fleets in declaration order, which is preference order.
    pub fleets: Vec<Fleet>,
    /// Workers ordered by name.
    pub workers: Vec<Worker>,
    /// Instances without a connected worker, ordered by name.
    pub instances: Vec<Instance>,
    /// Absent until the first action sample after controller start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Actions>,
    /// The most recent scaling events, oldest first; at most `events_limit`.
    pub events: Vec<Event>,
    /// Live provider observations; separate from the durable scaling audit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<Event>>", optional)]
    pub status_events: Vec<Event>,
    /// How many streamed events a client keeps; constant for the process.
    pub events_limit: u32,
    /// Actions that reached a terminal result since the controller started; a decrease is a
    /// restart.
    #[ts(type = "number")]
    pub completed: u64,
    /// What this server observed over its recent window.
    #[serde(default)]
    pub recent: Recent,
    /// The leader's bounded stores and latest external bucket measurement.
    #[serde(default)]
    pub cache: Cache,
    /// Public listener connections and cumulative refusals.
    #[serde(default)]
    pub connections: Connections,
    /// Builds with an observation within the recent window, basic view, ordered by name; at most `max_live`.
    pub builds: Vec<Build>,
    /// Epoch of the principal set, which advances when principals change.
    #[ts(type = "number")]
    pub principals_epoch: u64,
}

/// Public listener only: current active and limit gauges, and refusals since server process start.
/// Defaults to zero when reading snapshots produced before this field existed.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Connections {
    /// Currently served public connections.
    #[ts(type = "number")]
    pub active: u64,
    /// Configured public listener limit.
    #[ts(type = "number")]
    pub limit: u64,
    /// Public connections refused since process start.
    #[ts(type = "number")]
    pub refused: u64,
}

/// Cache occupancy at the latest completed pass and provider measurement.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Cache {
    /// Bounded engine stores, omitted when none are reported.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<StoreUsage>>", optional)]
    pub stores: Vec<StoreUsage>,
    /// Latest provider measurement, absent until available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<BucketUsage>,
}

/// Occupancy of one bounded engine store.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct StoreUsage {
    /// Which store was measured.
    pub store: CacheStore,
    /// Bytes held, rounded to filesystem blocks.
    #[ts(type = "number")]
    pub used_bytes: u64,
    /// Eviction budget in bytes.
    #[ts(type = "number")]
    pub budget_bytes: u64,
}

/// Provider measurement of the entire external bucket.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct BucketUsage {
    /// Bytes held in the bucket.
    #[ts(type = "number")]
    pub bytes: u64,
    /// Objects held in the bucket.
    #[ts(type = "number")]
    pub objects: u64,
    /// When the provider measured the bucket.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub measure_time: Timestamp,
}

/// What this server observed over its recent window, oldest first. Every dashboard chart and rate
/// is drawn from it, so a page shows the whole window from its first frame. It lives in the
/// server's memory: a server that starts has none, and the steps before its first sample are not
/// recorded (shown as such, never as zero).
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Recent {
    /// How far back samples reach; constant for the process.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub window: Duration,
    /// The span of one sample; constant for the process. `window` is a whole number of steps.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub step: Duration,
    /// One sample per step, oldest first, without holes; the last is the step in progress. At
    /// most `window / step` samples.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<Sample>>", optional)]
    pub samples: Vec<Sample>,
}

/// One step of [`Recent`].
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Sample {
    /// Start of the step: a whole multiple of the step since the Unix epoch.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start: Timestamp,
    /// Actions that reached a terminal result during the step.
    pub completed: u32,
    /// Actions executing at the step's latest completed pass; 0 when `passes` is 0.
    pub running: u32,
    /// Actions queued at the step's latest completed pass; 0 when `passes` is 0.
    pub queued: u32,
    /// Control passes that completed during the step.
    pub passes: u32,
    /// Control passes skipped during the step.
    pub skipped: u32,
    /// `GetActionResult` calls answered during the step.
    pub lookups: u32,
    /// Of `lookups`, remote cache hits.
    pub hits: u32,
    /// Client-sent cache-service message bytes during the step.
    #[serde(default)]
    #[ts(type = "number")]
    pub uploaded_bytes: u64,
    /// Client-received cache-service message bytes during the step.
    #[serde(default)]
    #[ts(type = "number")]
    pub downloaded_bytes: u64,
    /// Bytes evicted by bounded stores during the step.
    #[serde(default)]
    #[ts(type = "number")]
    pub evicted_bytes: u64,
}

/// A phase a worker was in, from `start` until the next span's start (the last one lasts).
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct PhaseSpan {
    /// The phase.
    pub phase: Phase,
    /// When the worker entered it.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start: Timestamp,
}

impl Recent {
    /// An empty window of `window`, sampled every `step`.
    #[must_use]
    pub const fn new(window: Duration, step: Duration) -> Self {
        Self {
            window,
            step,
            samples: Vec::new(),
        }
    }

    /// How many samples the window holds: `window / step`, at least 1.
    #[must_use]
    pub fn capacity(&self) -> usize {
        usize::try_from(millis_of(self.window) / millis_of(self.step).max(1))
            .unwrap_or(usize::MAX)
            .max(1)
    }

    /// Makes the sample of the step holding `at` the last one and lets `count` update it.
    /// A time before the last sample's step counts into the last sample (the clock stepped back).
    /// Steps skipped since the last sample get empty samples; a jump of a whole window or more
    /// drops every sample first. At most `capacity` samples are kept, oldest dropped.
    pub fn sample_at(&mut self, at: Timestamp, count: impl FnOnce(&mut Sample)) {
        let step = millis_of(self.step).max(1);
        let start = at.as_unix_millis() / step * step;
        let capacity = self.capacity();
        match self.samples.last() {
            None => self.samples.push(Sample {
                start: Timestamp::from_unix_millis(start),
                ..Sample::default()
            }),
            Some(last) => {
                let last = last.start.as_unix_millis();
                if start > last {
                    let steps = (start - last) / step;
                    if steps >= u64::try_from(capacity).unwrap_or(u64::MAX) {
                        self.samples.clear();
                        self.samples.push(Sample {
                            start: Timestamp::from_unix_millis(start),
                            ..Sample::default()
                        });
                    } else {
                        for offset in 1..=steps {
                            self.samples.push(Sample {
                                start: Timestamp::from_unix_millis(last + offset * step),
                                ..Sample::default()
                            });
                        }
                    }
                }
            }
        }
        self.samples
            .drain(..self.samples.len().saturating_sub(capacity));
        if let Some(sample) = self.samples.last_mut() {
            count(sample);
        }
    }

    /// The increment that moves `prev` to `self`: `window`, `step`, and the samples of `self` from
    /// the first one that `prev` lacks or holds differently. `None` when there is no such sample.
    #[must_use]
    pub fn increment(&self, prev: &Self) -> Option<Self> {
        let first = self.samples.iter().position(|sample| {
            prev.samples
                .binary_search_by_key(&sample.start, |prior| prior.start)
                .ok()
                .is_none_or(|index| prev.samples[index] != *sample)
        })?;
        Some(Self {
            window: self.window,
            step: self.step,
            samples: self.samples[first..].to_vec(),
        })
    }

    /// Applies an increment: takes its `window` and `step`, drops the samples whose start is at
    /// or after the increment's first, appends the increment's samples, then drops the samples
    /// that start a whole window or more before the last one.
    pub fn apply(&mut self, increment: &Self) {
        self.window = increment.window;
        self.step = increment.step;
        if let Some(first) = increment.samples.first() {
            let keep = self
                .samples
                .partition_point(|sample| sample.start < first.start);
            self.samples.truncate(keep);
            self.samples.extend_from_slice(&increment.samples);
            if let Some(last) = self.samples.last() {
                let last = last.start.as_unix_millis();
                let window = millis_of(self.window);
                let drop = self.samples.partition_point(|sample| {
                    let start = sample.start.as_unix_millis();
                    start < last && last - start >= window
                });
                self.samples.drain(..drop);
            }
        }
    }
}

/// The outcome of the last scheduling pass.
///
/// `skip_reason`, `last_ok_time` and `skipped` describe a skipped pass and are absent otherwise.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Pass {
    /// Whether the pass ran or was skipped.
    pub state: PassState,
    /// Time between passes.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub interval: Duration,
    /// Steps the pass applied; 0 when skipped.
    pub applied: u32,
    /// Steps the pass failed to apply; 0 when skipped.
    pub failed: u32,
    /// Why the pass was skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<SkipReason>,
    /// The time of the last good pass; absent when no pass succeeded since start.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    #[ts(type = "Timestamp")]
    pub last_ok_time: Option<Timestamp>,
    /// Consecutive skipped passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<u32>,
}

impl Pass {
    /// A pass that completed, applying `applied` steps and failing `failed`.
    #[must_use]
    pub const fn ok(interval: Duration, applied: u32, failed: u32) -> Self {
        Self {
            state: PassState::Ok,
            interval,
            applied,
            failed,
            skip_reason: None,
            last_ok_time: None,
            skipped: None,
        }
    }

    /// A pass skipped for `reason`, the `skipped`-th in a row, after the last good pass at
    /// `last_ok_time`.
    #[must_use]
    pub const fn skipped(
        interval: Duration,
        reason: SkipReason,
        last_ok_time: Option<Timestamp>,
        skipped: u32,
    ) -> Self {
        Self {
            state: PassState::Skipped,
            interval,
            applied: 0,
            failed: 0,
            skip_reason: Some(reason),
            last_ok_time,
            skipped: Some(skipped),
        }
    }
}

/// The action queue.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Queue {
    /// The sum of every fleet's queued demand plus `unplaceable`.
    pub total: u32,
    /// Queued actions that no fleet accepts.
    pub unplaceable: u32,
    /// Enqueue time of the oldest queued action; absent when the queue is empty.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    #[ts(type = "Timestamp")]
    pub oldest_queue_time: Option<Timestamp>,
}

/// The size of one instance of a fleet.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSize {
    /// Number of virtual CPUs.
    pub vcpus: u32,
    /// Memory in bytes.
    #[ts(type = "Bytes")]
    pub memory: u64,
}

/// A fleet and its current state, named `fleets/{fleet}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Fleet {
    /// Resource name, `fleets/{fleet}`.
    pub name: String,
    /// The instruction set architecture of the fleet's workers.
    pub isa: Isa,
    /// The sandbox that runs actions on the fleet's workers.
    pub sandbox: String,
    /// The provisioner's selectable vocabulary; `capacity` is `spot` or `on-demand` when present.
    /// The keys are data and are not renamed.
    pub properties: BTreeMap<String, String>,
    /// The size of one instance; absent when the provisioner does not state it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_size: Option<InstanceSize>,
    /// The scaling policy.
    pub policy: Policy,
    /// The number of instances the fleet wants.
    pub desired: u32,
    /// Why the newest launch attempt failed while the request is unmet and no launch is in
    /// flight; absent when the request is met, a launch is in flight, or the provider cannot tell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_failure: Option<LaunchFailure>,
    /// Instances by lifecycle.
    pub instances: InstanceCounts,
    /// Workers by phase.
    pub workers: WorkerCounts,
    /// The fleet's share of demand.
    pub demand: Demand,
}

/// The provisioner's reason for an unmet fleet request with no launch in flight.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct LaunchFailure {
    /// The provider's failure message, verbatim.
    pub reason: String,
}

/// The scaling bounds and timeouts of a fleet.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    /// The fewest instances the fleet keeps at this pass: `min_instances`, raised by a scheduled minimum in force.
    pub min: u32,
    /// The most instances the fleet runs.
    pub max: u32,
    /// The number of actions one instance is expected to take.
    pub target_actions: u32,
    /// How long a worker may stay idle before it drains.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub idle_timeout: Duration,
    /// The longest a draining worker is waited for.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub max_drain_wait: Duration,
    /// How long a new instance may take to connect before it counts as orphaned.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub boot_grace: Duration,
}

/// Instance counts of a fleet by lifecycle.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct InstanceCounts {
    /// Instances that are launching.
    pub pending: u32,
    /// Instances that are in service.
    pub in_service: u32,
    /// Instances that are shutting down.
    pub terminating: u32,
    /// Instances kept warm and ready to take work.
    pub warm: u32,
}

/// Worker counts of a fleet by phase.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct WorkerCounts {
    /// Workers running at least one action.
    pub busy: u32,
    /// Workers with no action running.
    pub idle: u32,
    /// Workers draining before retirement.
    pub draining: u32,
    /// In-service instances with no connected worker.
    pub unconnected: u32,
}

/// The demand a fleet faces.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Demand {
    /// Actions running on the fleet.
    pub running: u32,
    /// Queued actions charged to this fleet only.
    pub queued: u32,
    /// Queued actions this fleet could run; overlaps across fleets.
    pub candidates: u32,
    /// Instances the fleet needs for its demand.
    pub needed: u32,
    /// Virtual CPUs the demand asks for.
    pub vcpus: u32,
    /// Memory in bytes the demand asks for.
    #[ts(type = "Bytes")]
    pub memory: u64,
    /// Measured instance readiness, absent until a measurement is available.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::duration::option"
    )]
    #[ts(type = "Duration")]
    pub readiness: Option<Duration>,
    /// Latest expected queued action start at the planned capacity, absent without attributed queue.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::duration::option"
    )]
    #[ts(type = "Duration")]
    pub expected_wait: Option<Duration>,
    /// Persistent load in integer deci-slots, absent until measured; zero is a measurement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<u32>,
    /// Transient queued slot-time before readiness subtraction, rounded to whole seconds.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::duration::option"
    )]
    #[ts(type = "Duration")]
    pub backlog: Option<Duration>,
    /// Instantaneous serving resource occupancy in permille, absent without serving capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utilization: Option<u16>,
}

/// A connected worker, named `workers/{worker}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Worker {
    /// Resource name, `workers/{worker}`.
    pub name: String,
    /// Reference `fleets/{fleet}` of the fleet the worker belongs to.
    pub fleet: String,
    /// Reference `instances/{instance}` of the instance the worker runs on.
    pub instance: String,
    /// The worker's phase.
    pub phase: Phase,
    /// Derived infrastructure status of the worker's instance, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub infrastructure: Option<InfrastructureStatus>,
    /// When the worker entered its phase.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub phase_time: Timestamp,
    /// Actions the worker is running.
    pub running: u32,
    /// How many actions the worker may run at once; absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<u32>,
    /// Whether the worker is paused.
    pub paused: bool,
    /// When the instance launched.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub launch_time: Timestamp,
    /// The phases this server saw the worker in within the recent window, oldest first; the last
    /// is the current phase, starting at `phase_time`. Before the first, the phase is not recorded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<PhaseSpan>>", optional)]
    pub phases: Vec<PhaseSpan>,
}

/// An instance without a connected worker, named `instances/{instance}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    /// Resource name, `instances/{instance}`.
    pub name: String,
    /// Reference `fleets/{fleet}` of the fleet the instance belongs to.
    pub fleet: String,
    /// The instance's lifecycle.
    pub lifecycle: Lifecycle,
    /// Derived infrastructure status, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub infrastructure: Option<InfrastructureStatus>,
    /// When the instance launched.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub launch_time: Timestamp,
    /// Whether the instance has been in service with no worker for at least its fleet's boot
    /// grace, as the autoscaler measures it (from both its launch time and when this controller
    /// first saw it unserved).
    pub past_boot_grace: bool,
}

/// The sampled actions of the controller.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Actions {
    /// The sample time.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub sample_time: Timestamp,
    /// The sample is stale when the server clock minus `sample_time` exceeds this. Constant for
    /// the process.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub stale_after: Duration,
    /// Running actions, ordered by name.
    pub running: Vec<RunningAction>,
    /// Queued actions, ordered by priority, then age.
    pub queued: Vec<QueuedAction>,
    /// How many actions were left out of each list.
    pub truncated: Truncated,
}

/// How many actions a sample left out of each list.
#[derive(
    gang_macros::Known,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Truncated {
    /// Running actions left out.
    pub running: u32,
    /// Queued actions left out.
    pub queued: u32,
}

/// An action that is running on a worker, named `actions/{op}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct RunningAction {
    /// Resource name, `actions/{op}`.
    pub name: String,
    /// The build of the action's first submitter, `builds/{id}`; absent when unattributable.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub build: String,
    /// Reference `fleets/{fleet}` of the fleet the action runs on.
    pub fleet: String,
    /// Reference `workers/{worker}` of the worker that runs the action.
    pub worker: String,
    /// The action's digest.
    pub digest: String,
    /// The action's target.
    pub target: String,
    /// The action's mnemonic.
    pub mnemonic: String,
    /// When the action started.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start_time: Timestamp,
    /// The action's priority.
    pub priority: i32,
}

/// An action that waits for a worker, named `actions/{op}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct QueuedAction {
    /// Resource name, `actions/{op}`.
    pub name: String,
    /// The build of the action's first submitter, `builds/{id}`; absent when unattributable.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub build: String,
    /// The action's digest.
    pub digest: String,
    /// The action's target.
    pub target: String,
    /// The action's mnemonic.
    pub mnemonic: String,
    /// When the action was queued.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub queue_time: Timestamp,
    /// The action's priority.
    pub priority: i32,
    /// References `fleets/{fleet}` of the fleets that accept the action; empty when
    /// unplaceable.
    pub candidates: Vec<String>,
    /// Why no fleet accepts this action; absent when a fleet accepts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

/// A resource that carries its resource name.
pub trait Named {
    /// The resource name, `collection/{id}`.
    fn name(&self) -> &str;
}

impl Named for Fleet {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Named for Worker {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Named for Instance {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Named for RunningAction {
    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn legacy_state_defaults_public_connections() {
        let mut value = serde_json::to_value(State::default()).expect("state serializes");
        assert_eq!(
            value["connections"],
            json!({"active": 0, "limit": 0, "refused": 0})
        );
        value.as_object_mut().expect("object").remove("connections");
        let old: State = serde_json::from_value(value).expect("old state parses");
        assert_eq!(old.connections, Connections::default());
    }

    #[test]
    fn infrastructure_status_round_trips_without_optional_pair() {
        let value = json!({"phase": "REPLACING", "reason": "SPOT_INTERRUPTION"});
        let status: InfrastructureStatus =
            serde_json::from_value(value.clone()).expect("status parses");
        assert_eq!(status.other_instance, None);
        assert_eq!(
            serde_json::to_value(status).expect("status serializes"),
            value
        );
        let state = serde_json::to_value(State::default()).expect("state serializes");
        assert!(state.get("statusEvents").is_none());
        let decoded: State = serde_json::from_value(state).expect("old state parses");
        assert!(decoded.status_events.is_empty());
    }

    #[test]
    fn cache_and_old_sample_wire_contract() {
        let mut value = serde_json::to_value(State::default()).expect("state serializes");
        assert_eq!(value["cache"], json!({}));
        value.as_object_mut().expect("object").remove("cache");
        let old: State = serde_json::from_value(value).expect("old state parses");
        assert_eq!(old.cache, Cache::default());
        let sample: Sample = serde_json::from_value(json!({
            "start": "1970-01-01T00:00:00.000Z", "completed": 0, "running": 0,
            "queued": 0, "passes": 0, "skipped": 0, "lookups": 0, "hits": 0
        }))
        .expect("old sample parses");
        assert_eq!(
            (
                sample.uploaded_bytes,
                sample.downloaded_bytes,
                sample.evicted_bytes
            ),
            (0, 0, 0)
        );
        let cache = Cache {
            stores: vec![StoreUsage {
                store: CacheStore::Action,
                used_bytes: 4,
                budget_bytes: 9,
            }],
            bucket: Some(BucketUsage {
                bytes: 100,
                objects: 2,
                measure_time: Timestamp::from_unix_millis(0),
            }),
        };
        let encoded = serde_json::to_value(&cache).expect("cache serializes");
        assert_eq!(
            encoded,
            json!({"stores": [{"store": "AC", "usedBytes": 4, "budgetBytes": 9}],
            "bucket": {"bytes": 100, "objects": 2, "measureTime": "1970-01-01T00:00:00.000Z"}})
        );
        assert_eq!(
            serde_json::from_value::<Cache>(encoded).expect("cache parses"),
            cache
        );
        assert_eq!(CacheStore::default(), CacheStore::Content);
    }

    #[test]
    fn recent_increment_replaces_changed_byte_counts() {
        let mut previous = Recent::new(Duration::from_secs(30), Duration::from_secs(10));
        previous.sample_at(Timestamp::from_unix_millis(0), |_| {});
        let mut current = previous.clone();
        current.sample_at(Timestamp::from_unix_millis(0), |sample| {
            sample.uploaded_bytes = 10;
            sample.downloaded_bytes = 20;
            sample.evicted_bytes = 30;
        });
        let increment = current.increment(&previous).expect("bytes changed");
        previous.apply(&increment);
        assert_eq!(previous, current);
        assert_eq!(
            (
                previous.samples[0].uploaded_bytes,
                previous.samples[0].downloaded_bytes,
                previous.samples[0].evicted_bytes
            ),
            (10, 20, 30)
        );
    }

    #[test]
    fn recent_samples_follow_steps_and_bound_the_window() {
        let mut recent = Recent::new(Duration::from_secs(30), Duration::from_secs(10));
        recent.sample_at(Timestamp::from_unix_millis(10_999), |sample| {
            sample.completed = 3;
        });
        recent.sample_at(Timestamp::from_unix_millis(19_999), |sample| {
            sample.passes = 1;
        });
        recent.sample_at(Timestamp::from_unix_millis(1_000), |sample| {
            sample.skipped = 2;
        });
        assert_eq!(recent.samples.len(), 1);
        assert_eq!(recent.samples[0].completed, 3);
        assert_eq!(recent.samples[0].passes, 1);
        assert_eq!(recent.samples[0].skipped, 2);
        recent.sample_at(Timestamp::from_unix_millis(30_000), |sample| {
            sample.running = 4;
        });
        assert_eq!(
            recent
                .samples
                .iter()
                .map(|s| s.start.as_unix_millis())
                .collect::<Vec<_>>(),
            vec![10_000, 20_000, 30_000]
        );
        assert_eq!(
            recent.samples[1],
            Sample {
                start: Timestamp::from_unix_millis(20_000),
                ..Sample::default()
            }
        );
        recent.sample_at(Timestamp::from_unix_millis(40_000), |_| {});
        assert_eq!(recent.samples.len(), recent.capacity());
        assert_eq!(recent.samples[0].start.as_unix_millis(), 20_000);
        recent.sample_at(Timestamp::from_unix_millis(70_000), |_| {});
        assert_eq!(recent.samples.len(), 1);
        assert_eq!(recent.samples[0].start.as_unix_millis(), 70_000);
    }

    #[test]
    fn recent_increments_reconstruct_each_successive_window() {
        for sequence in [
            &[0, 1_000, 10_000, 30_000, 40_000, 80_000][..],
            &[50_000, 20_000, 60_000, 70_000, 90_000][..],
            &[10_000, 30_000, 50_000, 60_000, 70_000][..],
        ] {
            let mut current = Recent::new(Duration::from_secs(30), Duration::from_secs(10));
            let mut previous = current.clone();
            assert_eq!(current.increment(&previous), None);
            for &at in sequence {
                current.sample_at(Timestamp::from_unix_millis(at), |sample| {
                    sample.completed = sample.completed.saturating_add(1);
                });
                let increment = current.increment(&previous).expect("sample changed");
                previous.apply(&increment);
                assert_eq!(previous, current);
                assert_eq!(current.increment(&previous), None);
            }
        }
    }

    #[test]
    fn recent_and_worker_phases_have_wire_json() {
        let start = Timestamp::from_unix_millis(0);
        let mut recent = Recent::new(Duration::from_mins(15), Duration::from_secs(10));
        recent.sample_at(start, |sample| {
            *sample = Sample {
                start,
                completed: 1,
                running: 2,
                queued: 3,
                passes: 4,
                skipped: 5,
                lookups: 6,
                hits: 7,
                uploaded_bytes: 8,
                downloaded_bytes: 9,
                evicted_bytes: 10,
            };
        });
        let state = State {
            recent,
            ..State::default()
        };
        let value = serde_json::to_value(&state).expect("state serializes");
        assert_eq!(value["builds"], json!([]));
        assert_eq!(
            value["recent"],
            json!({
                "window": "900s", "step": "10s",
                "samples": [{"start": "1970-01-01T00:00:00.000Z", "completed": 1,
                    "running": 2, "queued": 3, "passes": 4, "skipped": 5, "lookups": 6, "hits": 7,
                    "uploadedBytes": 8, "downloadedBytes": 9, "evictedBytes": 10}]
            })
        );
        let mut worker = Worker::default();
        assert!(
            serde_json::to_value(&worker)
                .expect("worker serializes")
                .get("phases")
                .is_none()
        );
        worker.phases.push(PhaseSpan {
            phase: Phase::Busy,
            start,
        });
        assert_eq!(
            serde_json::to_value(&worker).expect("worker serializes")["phases"],
            json!([{"phase": "BUSY", "start": "1970-01-01T00:00:00.000Z"}])
        );
    }

    fn round_trip<T>(value: &Value) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let parsed: T = serde_json::from_value(value.clone()).expect("value parses");
        assert_eq!(
            &serde_json::to_value(&parsed).expect("value serializes"),
            value
        );
        parsed
    }

    fn fleet_value() -> Value {
        json!({
            "name": "fleets/amd64-spot",
            "isa": "x86-64",
            "sandbox": "linux-sandbox",
            "properties": {"capacity": "spot", "Pool": "A"},
            "instanceSize": {"vcpus": 8, "memory": 34_359_738_368_u64},
            "policy": {
                "min": 0,
                "max": 20,
                "targetActions": 4,
                "idleTimeout": "300s",
                "maxDrainWait": "1800s",
                "bootGrace": "0.250s",
            },
            "desired": 6,
            "instances": {"pending": 1, "inService": 5, "terminating": 0, "warm": 2},
            "workers": {"busy": 3, "idle": 1, "draining": 1, "unconnected": 0},
            "demand": {
                "running": 3,
                "queued": 7,
                "candidates": 9,
                "needed": 3,
                "vcpus": 24,
                "memory": 103_079_215_104_u64,
            },
        })
    }

    #[test]
    fn ok_pass_omits_the_skipped_members() {
        let value = json!({"state": "OK", "interval": "2s", "applied": 2, "failed": 0});
        let pass: Pass = round_trip(&value);
        assert_eq!(pass.state, PassState::Ok);
        assert_eq!(pass.interval, Duration::from_secs(2));
        assert_eq!(pass.skip_reason, None);
        assert_eq!(pass.last_ok_time, None);
        assert_eq!(pass.skipped, None);
    }

    #[test]
    fn skipped_pass_carries_reason_last_ok_and_count() {
        let value = json!({
            "state": "SKIPPED",
            "interval": "2s",
            "applied": 1,
            "failed": 2,
            "skipReason": "PROVISIONER_UNAVAILABLE",
            "lastOkTime": "2026-01-14T09:17:16.000Z",
            "skipped": 3,
        });
        let pass: Pass = round_trip(&value);
        assert_eq!(pass.skip_reason, Some(SkipReason::ProvisionerUnavailable));
        assert_eq!(pass.skipped, Some(3));
    }

    #[test]
    fn skipped_pass_without_a_good_pass_omits_last_ok_time() {
        let value = json!({
            "state": "SKIPPED",
            "interval": "2s",
            "applied": 0,
            "failed": 0,
            "skipReason": "SCHEDULER_UNAVAILABLE",
            "skipped": 1,
        });
        let pass: Pass = round_trip(&value);
        assert_eq!(pass.last_ok_time, None);
    }

    #[test]
    fn unknown_enum_values_are_kept() {
        let value = json!({
            "state": "PAUSED",
            "interval": "2s",
            "applied": 0,
            "failed": 0,
            "skipReason": "DISK_FULL",
        });
        let pass: Pass = round_trip(&value);
        assert_eq!(pass.state, PassState::Unrecognized("PAUSED".to_owned()));
        assert_eq!(
            pass.skip_reason,
            Some(SkipReason::Unrecognized("DISK_FULL".to_owned()))
        );
    }

    #[test]
    fn queue_omits_an_absent_oldest_queue_time() {
        let empty: Queue = round_trip(&json!({"total": 0, "unplaceable": 0}));
        assert_eq!(empty.oldest_queue_time, None);
        let waiting: Queue = round_trip(&json!({
            "total": 4,
            "unplaceable": 1,
            "oldestQueueTime": "2026-01-14T09:19:00.000Z",
        }));
        assert!(waiting.oldest_queue_time.is_some());
    }

    #[test]
    fn demand_optional_durations_round_trip() {
        let legacy = json!({
            "running": 3, "queued": 7, "candidates": 9,
            "needed": 3, "vcpus": 24, "memory": 1024,
        });
        let demand: Demand = round_trip(&legacy);
        assert_eq!(demand.readiness, None);
        assert_eq!(demand.expected_wait, None);

        let mut both = legacy.clone();
        both["readiness"] = json!("0.250s");
        both["expectedWait"] = json!("0s");
        let demand: Demand = round_trip(&both);
        assert_eq!(demand.readiness, Some(Duration::from_millis(250)));
        assert_eq!(demand.expected_wait, Some(Duration::ZERO));

        let mut readiness_only = both.clone();
        readiness_only
            .as_object_mut()
            .expect("demand object")
            .remove("expectedWait");
        let demand: Demand = round_trip(&readiness_only);
        assert_eq!(demand.readiness, Some(Duration::from_millis(250)));
        assert_eq!(demand.expected_wait, None);

        let mut wait_only = both;
        wait_only
            .as_object_mut()
            .expect("demand object")
            .remove("readiness");
        let demand: Demand = round_trip(&wait_only);
        assert_eq!(demand.readiness, None);
        assert_eq!(demand.expected_wait, Some(Duration::ZERO));
    }

    #[test]
    fn demand_rate_diagnostics_round_trip_independently_including_zero() {
        let legacy = json!({
            "running": 3, "queued": 7, "candidates": 9,
            "needed": 3, "vcpus": 24, "memory": 1024,
        });
        let demand: Demand = round_trip(&legacy);
        assert_eq!(
            (demand.load, demand.backlog, demand.utilization),
            (None, None, None)
        );
        for (load, backlog, utilization) in [
            (Some(125), Some(Duration::from_secs(42)), Some(735)),
            (Some(0), Some(Duration::ZERO), Some(0)),
            (Some(1), None, None),
            (None, Some(Duration::ZERO), None),
            (None, None, Some(0)),
        ] {
            let demand = Demand {
                load,
                backlog,
                utilization,
                ..demand
            };
            let value = serde_json::to_value(demand).expect("demand serializes");
            assert_eq!(value.get("load"), load.map(|v| json!(v)).as_ref());
            assert_eq!(
                value.get("utilization"),
                utilization.map(|v| json!(v)).as_ref()
            );
            let decoded: Demand = round_trip(&value);
            assert_eq!(decoded, demand);
            assert_eq!(
                value.get("backlog"),
                backlog.map(|v| json!(format!("{}s", v.as_secs()))).as_ref()
            );
        }
    }

    #[test]
    fn fleet_round_trips_with_unit_less_durations_and_unrenamed_property_keys() {
        let fleet: Fleet = round_trip(&fleet_value());
        assert_eq!(fleet.policy.boot_grace, Duration::from_millis(250));
        assert_eq!(fleet.properties.get("Pool").map(String::as_str), Some("A"));
        assert_eq!(fleet.instance_size.map(|size| size.vcpus), Some(8));
    }

    #[test]
    fn fleet_omits_an_absent_instance_size() {
        let mut value = fleet_value();
        value
            .as_object_mut()
            .expect("fleet is an object")
            .remove("instanceSize");
        let fleet: Fleet = round_trip(&value);
        assert_eq!(fleet.instance_size, None);
    }

    #[test]
    fn worker_and_instance_round_trip_and_omit_absent_slots() {
        let worker: Worker = round_trip(&json!({
            "name": "workers/i-0e10-0194",
            "fleet": "fleets/amd64-spot",
            "instance": "instances/i-0e10",
            "phase": "DRAINING",
            "phaseTime": "2026-01-14T09:19:30.000Z",
            "running": 0,
            "paused": false,
            "launchTime": "2026-01-14T08:00:00.000Z",
        }));
        assert_eq!(worker.phase, Phase::Draining);
        assert_eq!(worker.slots, None);
        let with_slots: Worker = round_trip(&json!({
            "name": "workers/i-0e10-0194",
            "fleet": "fleets/amd64-spot",
            "instance": "instances/i-0e10",
            "phase": "BUSY",
            "phaseTime": "2026-01-14T09:19:30.000Z",
            "running": 2,
            "slots": 4,
            "paused": true,
            "launchTime": "2026-01-14T08:00:00.000Z",
        }));
        assert_eq!(with_slots.slots, Some(4));

        let instance: Instance = round_trip(&json!({
            "name": "instances/i-0e11",
            "fleet": "fleets/amd64-spot",
            "lifecycle": "IN_SERVICE",
            "launchTime": "2026-01-14T08:00:00.000Z",
            "pastBootGrace": true,
        }));
        assert_eq!(instance.lifecycle, Lifecycle::InService);
    }

    #[test]
    fn actions_round_trip() {
        let actions: Actions = round_trip(&json!({
            "sampleTime": "2026-01-14T09:19:30.000Z",
            "staleAfter": "10s",
            "running": [{
                "name": "actions/op-1",
                "fleet": "fleets/amd64-spot",
                "worker": "workers/w1",
                "digest": "abc/12",
                "target": "//app:lib",
                "mnemonic": "CppCompile",
                "startTime": "2026-01-14T09:19:00.000Z",
                "priority": -5,
            }],
            "queued": [{
                "name": "actions/op-2",
                "digest": "def/34",
                "target": "//app:bin",
                "mnemonic": "Link",
                "queueTime": "2026-01-14T09:19:10.000Z",
                "priority": 0,
                "candidates": ["fleets/amd64-spot"],
            }],
            "truncated": {"running": 0, "queued": 12},
        }));
        assert_eq!(actions.stale_after, Duration::from_secs(10));
    }

    #[test]
    fn state_omits_absent_actions_and_never_carries_a_schema() {
        let value = json!({
            "name": "state",
            "seq": 48_111,
            "updateTime": "2026-01-14T09:19:30.000Z",
            "provisioner": "aws",
            "pass": {"state": "OK", "interval": "2s", "applied": 0, "failed": 0},
            "queue": {"total": 0, "unplaceable": 0},
            "fleets": [fleet_value()],
            "workers": [],
            "instances": [],
            "events": [{
                "name": "events/995",
                "createTime": "2026-01-14T09:19:00.000Z",
                "fleet": "fleets/amd64-spot",
                "scaledOut": {"from": 0, "to": 6},
            }],
            "eventsLimit": 200,
            "completed": 17,
            "recent": {"window": "0s", "step": "0s"},
            "cache": {},
            "builds": [],
            "principalsEpoch": 3,
        });
        let state: State = round_trip(&value);
        assert_eq!(state.actions, None);
        assert_eq!(state.events.len(), 1);
    }

    #[test]
    fn state_ignores_unknown_fields_and_rejects_a_missing_name() {
        let mut value = json!({
            "name": "state",
            "seq": 1,
            "updateTime": "2026-01-14T09:19:30.000Z",
            "provisioner": "local",
            "pass": {"state": "OK", "interval": "2s", "applied": 0, "failed": 0},
            "queue": {"total": 0, "unplaceable": 0},
            "fleets": [],
            "workers": [],
            "instances": [],
            "events": [],
            "eventsLimit": 200,
            "completed": 0,
            "builds": [],
            "principalsEpoch": 0,
            "schema": 1,
        });
        assert!(serde_json::from_value::<State>(value.clone()).is_ok());
        value
            .as_object_mut()
            .expect("state is an object")
            .remove("name");
        assert!(serde_json::from_value::<State>(value).is_err());
    }

    #[test]
    fn default_worker_is_idle_and_default_instance_is_pending() {
        assert_eq!(Worker::default().phase, Phase::Idle);
        assert_eq!(Instance::default().lifecycle, Lifecycle::Pending);
    }

    #[test]
    fn named_returns_the_resource_name_of_each_type() {
        let fleet = Fleet {
            name: "fleets/amd64-spot".to_owned(),
            ..Fleet::default()
        };
        let worker = Worker {
            name: "workers/i-0e10-0194".to_owned(),
            ..Worker::default()
        };
        let instance = Instance {
            name: "instances/i-0e10".to_owned(),
            ..Instance::default()
        };
        let action = RunningAction {
            name: "actions/op-1".to_owned(),
            ..RunningAction::default()
        };
        assert_eq!(fleet.name(), "fleets/amd64-spot");
        assert_eq!(worker.name(), "workers/i-0e10-0194");
        assert_eq!(instance.name(), "instances/i-0e10");
        assert_eq!(action.name(), "actions/op-1");
    }
}
