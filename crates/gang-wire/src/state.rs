//! The `state` singleton: the live view of the fleets, workers, instances and sampled actions
//! that `GET /v1/state` serves and `GET /v1/state:watch` streams.
//!
//! Timestamps are RFC 3339 and durations are Google `Duration` strings; sizes are bytes. The
//! recorded fixtures in `ui/fixtures` round trip through these types in the crate's tests.

use std::collections::BTreeMap;
use std::time::Duration;

use gang_clock::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{Event, Isa};

wire_enum! {
    /// Why a scheduling pass was skipped.
    pub enum SkipReason {
        /// The scheduler could not be reached.
        SchedulerUnavailable = "SCHEDULER_UNAVAILABLE",
        /// The provisioner could not be reached.
        ProvisionerUnavailable = "PROVISIONER_UNAVAILABLE",
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
    /// How many streamed events a client keeps; constant for the process.
    pub events_limit: u32,
    /// Actions that reached a terminal result since the controller started; a decrease is a
    /// restart.
    #[ts(type = "number")]
    pub completed: u64,
    /// Epoch of the principal set, which advances when principals change.
    #[ts(type = "number")]
    pub principals_epoch: u64,
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
    /// Instances by lifecycle.
    pub instances: InstanceCounts,
    /// Workers by phase.
    pub workers: WorkerCounts,
    /// The fleet's share of demand.
    pub demand: Demand,
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
    /// The fewest instances the fleet keeps.
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
pub struct QueuedAction {
    /// Resource name, `actions/{op}`.
    pub name: String,
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
