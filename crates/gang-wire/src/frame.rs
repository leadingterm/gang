//! The frames of `GET /v1/state:watch`: a snapshot, then deltas, ticks and a final `closed`.
//!
//! Frames are not serde types: on the stream each one is a server-sent event whose name is
//! [`Frame::event_name`] and whose `data` is the member's JSON, see [`FrameRef::write_data`].
//! [`Frame::from_parts`] is the inverse for decoders.

use std::time::Duration;

use gang_clock::Timestamp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::state::{
    Fleet, Instance, Pass, Queue, QueuedAction, RunningAction, State, Truncated, Worker,
};
use crate::{Event, FrameError};

/// Event name of a [`Frame::Snapshot`].
pub const SNAPSHOT_EVENT: &str = "snapshot";

/// Event name of a [`Frame::Delta`].
pub const DELTA_EVENT: &str = "delta";

/// Event name of a [`Frame::Tick`].
pub const TICK_EVENT: &str = "tick";

/// Event name of a [`Frame::Closed`].
pub const CLOSED_EVENT: &str = "closed";

/// Keyed changes to one list: rows to insert or replace, and names to drop. A name appears in at
/// most one of the two lists; `gang-state` applies removals before upserts.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct Keyed<T> {
    /// Rows to insert or replace, keyed by `name`.
    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<T>>", optional)]
    pub upsert: Vec<T>,
    /// Names to drop. A name here must not also appear in `upsert`.
    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<String>>", optional)]
    pub remove: Vec<String>,
}

/// Changes to the sampled actions.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ActionsDelta {
    /// The new sample time.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    #[ts(type = "Timestamp")]
    pub sample_time: Option<Timestamp>,
    /// Present only when the first sample arrives in a delta.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::duration::option"
    )]
    #[ts(type = "Duration")]
    pub stale_after: Option<Duration>,
    /// Changes to the running actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<Keyed<RunningAction>>,
    /// A whole-list replace of the queued actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued: Option<Vec<QueuedAction>>,
    /// A replacement of the truncation counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<Truncated>,
}

impl ActionsDelta {
    /// Whether no part of the sampled actions changed: every optional part is `None`.
    ///
    /// The single place that lists the parts of an [`ActionsDelta`].
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.sample_time.is_none()
            && self.stale_after.is_none()
            && self.running.is_none()
            && self.queued.is_none()
            && self.truncated.is_none()
    }
}

/// A `delta` frame: only the parts that changed.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Delta {
    /// The sequence number this delta moves the state to.
    #[ts(type = "number")]
    pub seq: u64,
    /// The sequence number this delta applies to.
    #[ts(type = "number")]
    pub base: u64,
    /// The server clock when the delta was built.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub update_time: Timestamp,
    /// A new scheduling pass outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass: Option<Pass>,
    /// A new action queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<Queue>,
    /// Changes to the fleets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fleets: Option<Keyed<Fleet>>,
    /// Changes to the workers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Keyed<Worker>>,
    /// Changes to the instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instances: Option<Keyed<Instance>>,
    /// Changes to the sampled actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<ActionsDelta>,
    /// Scaling events, appended to the client's list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<Event>>", optional)]
    pub events: Vec<Event>,
    /// A new count of completed actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "number")]
    pub completed: Option<u64>,
    /// A new principal epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(type = "number")]
    pub principals_epoch: Option<u64>,
}

impl Delta {
    /// Whether this delta changes nothing beyond `seq`, `base` and `update_time`: every optional
    /// part is absent and every list or keyed part is empty.
    ///
    /// The single place that lists the parts of a [`Delta`]; a new part is added here too.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pass.is_none()
            && self.queue.is_none()
            && keyed_is_empty(self.fleets.as_ref())
            && keyed_is_empty(self.workers.as_ref())
            && keyed_is_empty(self.instances.as_ref())
            && self.actions.as_ref().is_none_or(ActionsDelta::is_empty)
            && self.events.is_empty()
            && self.completed.is_none()
            && self.principals_epoch.is_none()
    }
}

/// A `tick` frame, sent on every heartbeat.
#[derive(
    gang_macros::Known, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
pub struct Tick {
    /// The current sequence number; it advances only with a delta.
    #[ts(type = "number")]
    pub seq: u64,
    /// The server clock.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub time: Timestamp,
}

wire_enum! {
    /// Why the server ended a stream deliberately.
    pub enum ClosedReason {
        /// The caller's access was revoked.
        Revoked = "REVOKED",
        /// The caller's session expired.
        Expired = "EXPIRED",
        /// Too many viewers are connected.
        ViewerCap = "VIEWER_CAP",
        /// The server is shutting down.
        Shutdown = "SHUTDOWN",
    }
}

/// A `closed` frame, sent before the server ends a stream deliberately.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Closed {
    /// Why the stream ended.
    pub reason: ClosedReason,
    /// How long the client waits before it reconnects.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::duration::option"
    )]
    #[ts(type = "Duration")]
    pub retry_delay: Option<Duration>,
}

/// One frame of the state stream.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(tag = "event", content = "data", rename_all = "lowercase")]
pub enum Frame {
    /// The whole state; the first frame of every stream.
    Snapshot(State),
    /// The parts of the state that changed since the previous frame.
    Delta(Delta),
    /// A heartbeat.
    Tick(Tick),
    /// The server ends the stream deliberately.
    Closed(Closed),
}

impl Frame {
    /// The server-sent event name of this frame.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        FrameRef::from(self).event_name()
    }

    /// The server-sent event id of this frame: the sequence number it carries, if any.
    #[must_use]
    pub fn id(&self) -> Option<u64> {
        FrameRef::from(self).id()
    }

    /// Decodes the frame of a server-sent event from its name and `data`.
    ///
    /// # Errors
    ///
    /// Returns [`FrameError::UnknownEvent`] for a name that is not a frame, and
    /// [`FrameError::Data`] when `data` is not the JSON of that frame's member.
    pub fn from_parts(event: &str, data: &str) -> Result<Self, FrameError> {
        match event {
            SNAPSHOT_EVENT => decode(SNAPSHOT_EVENT, data).map(Self::Snapshot),
            DELTA_EVENT => decode(DELTA_EVENT, data).map(Self::Delta),
            TICK_EVENT => decode(TICK_EVENT, data).map(Self::Tick),
            CLOSED_EVENT => decode(CLOSED_EVENT, data).map(Self::Closed),
            _ => Err(FrameError::UnknownEvent {
                name: event.to_owned(),
            }),
        }
    }
}

/// A borrowed view of one frame, so encoders write a frame without cloning it.
#[derive(gang_macros::Known, Debug, Clone, Copy)]
pub enum FrameRef<'a> {
    /// The whole state; the first frame of every stream.
    Snapshot(&'a State),
    /// The parts of the state that changed since the previous frame.
    Delta(&'a Delta),
    /// A heartbeat.
    Tick(Tick),
    /// The server ends the stream deliberately.
    Closed(&'a Closed),
}

impl FrameRef<'_> {
    /// The server-sent event name of this frame.
    #[must_use]
    pub const fn event_name(&self) -> &'static str {
        match self {
            Self::Snapshot(_) => SNAPSHOT_EVENT,
            Self::Delta(_) => DELTA_EVENT,
            Self::Tick(_) => TICK_EVENT,
            Self::Closed(_) => CLOSED_EVENT,
        }
    }

    /// The server-sent event id of this frame: the sequence number it carries, if any.
    #[must_use]
    pub const fn id(&self) -> Option<u64> {
        match self {
            Self::Snapshot(state) => Some(state.seq),
            Self::Delta(delta) => Some(delta.seq),
            Self::Tick(tick) => Some(tick.seq),
            Self::Closed(_) => None,
        }
    }

    /// How long the client waits before it reconnects, when the frame says so.
    #[must_use]
    pub const fn retry(&self) -> Option<Duration> {
        match self {
            Self::Closed(closed) => closed.retry_delay,
            Self::Snapshot(_) | Self::Delta(_) | Self::Tick(_) => None,
        }
    }

    /// Writes the `data` of this frame's server-sent event: the member serialized as JSON.
    ///
    /// # Errors
    ///
    /// Returns the serializer's error when the member cannot be written.
    pub fn write_data<W: std::io::Write>(&self, writer: W) -> serde_json::Result<()> {
        match self {
            Self::Snapshot(state) => serde_json::to_writer(writer, state),
            Self::Delta(delta) => serde_json::to_writer(writer, delta),
            Self::Tick(tick) => serde_json::to_writer(writer, tick),
            Self::Closed(closed) => serde_json::to_writer(writer, closed),
        }
    }
}

impl<'a> From<&'a Frame> for FrameRef<'a> {
    fn from(frame: &'a Frame) -> Self {
        match frame {
            Frame::Snapshot(state) => Self::Snapshot(state),
            Frame::Delta(delta) => Self::Delta(delta),
            Frame::Tick(tick) => Self::Tick(*tick),
            Frame::Closed(closed) => Self::Closed(closed),
        }
    }
}

/// Parses the JSON `data` of the frame named `event`.
fn decode<T: DeserializeOwned>(event: &'static str, data: &str) -> Result<T, FrameError> {
    serde_json::from_str(data).map_err(|source| FrameError::Data {
        event,
        reason: source.to_string(),
    })
}

/// Whether a keyed part carries no upserts and no removals; an absent part counts as empty.
fn keyed_is_empty<T>(keyed: Option<&Keyed<T>>) -> bool {
    keyed.is_none_or(|keyed| keyed.upsert.is_empty() && keyed.remove.is_empty())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn state_value() -> Value {
        json!({
            "name": "state",
            "seq": 48_111,
            "updateTime": "2026-01-14T09:19:30.000Z",
            "provisioner": "aws",
            "pass": {"state": "OK", "interval": "2s", "applied": 0, "failed": 0},
            "queue": {"total": 0, "unplaceable": 0},
            "fleets": [],
            "workers": [],
            "instances": [],
            "events": [],
            "eventsLimit": 200,
            "completed": 0,
            "principalsEpoch": 0,
        })
    }

    fn data_of(frame: &Frame) -> String {
        let mut written = Vec::new();
        FrameRef::from(frame)
            .write_data(&mut written)
            .expect("frame writes");
        String::from_utf8(written).expect("data is utf-8")
    }

    fn decoded(event: &str, value: &Value) -> Frame {
        let data = value.to_string();
        let frame = Frame::from_parts(event, &data).expect("frame decodes");
        assert_eq!(frame.event_name(), event);
        let encoded: Value = serde_json::from_str(&data_of(&frame)).expect("data is json");
        assert_eq!(&encoded, value);
        frame
    }

    #[test]
    fn event_names_match_typescript_discriminants() {
        let declaration = <Frame as ts_rs::TS>::decl(&ts_rs::Config::default());
        for frame in frames() {
            let discriminant = format!("\"event\": \"{}\"", frame.event_name());
            assert!(declaration.contains(&discriminant), "{declaration}");
        }
    }

    #[test]
    fn snapshot_frame_round_trips_and_is_identified_by_its_seq() {
        let frame = decoded(SNAPSHOT_EVENT, &state_value());
        assert!(matches!(frame, Frame::Snapshot(_)));
        assert_eq!(frame.id(), Some(48_111));
    }

    #[test]
    fn empty_delta_serializes_only_seq_base_and_update_time() {
        let value =
            json!({"seq": 48_112, "base": 48_111, "updateTime": "2026-01-14T09:19:32.000Z"});
        let frame = decoded(DELTA_EVENT, &value);
        let Frame::Delta(delta) = &frame else {
            panic!("expected a delta, got {frame:?}");
        };
        assert!(delta.events.is_empty());
        assert_eq!(frame.id(), Some(48_112));
    }

    #[test]
    fn delta_with_every_part_round_trips() {
        let value = json!({
            "seq": 8,
            "base": 7,
            "updateTime": "2026-01-14T09:19:32.000Z",
            "pass": {"state": "OK", "interval": "2s", "applied": 1, "failed": 0},
            "queue": {"total": 1, "unplaceable": 0, "oldestQueueTime": "2026-01-14T09:19:31.000Z"},
            "fleets": {"remove": ["fleets/gone"]},
            "workers": {"upsert": [{
                "name": "workers/w1",
                "fleet": "fleets/f",
                "instance": "instances/i",
                "phase": "IDLE",
                "phaseTime": "2026-01-14T09:19:32.000Z",
                "running": 0,
                "paused": false,
                "launchTime": "2026-01-14T09:00:00.000Z",
            }], "remove": ["workers/w0"]},
            "instances": {},
            "actions": {
                "sampleTime": "2026-01-14T09:19:32.000Z",
                "staleAfter": "10s",
                "running": {"remove": ["actions/op-1"]},
                "queued": [],
                "truncated": {"running": 0, "queued": 0},
            },
            "events": [{
                "name": "events/1",
                "createTime": "2026-01-14T09:19:32.000Z",
                "fleet": "fleets/f",
                "workerRegistered": {"worker": "workers/w1"},
            }],
            "completed": 5,
            "principalsEpoch": 2,
        });
        let frame = decoded(DELTA_EVENT, &value);
        let Frame::Delta(delta) = frame else {
            panic!("expected a delta");
        };
        assert_eq!(delta.events.len(), 1);
        assert_eq!(delta.completed, Some(5));
    }

    #[test]
    fn keyed_omits_empty_parts_and_tolerates_absent_ones() {
        let empty: Keyed<Truncated> = Keyed {
            upsert: Vec::new(),
            remove: Vec::new(),
        };
        assert_eq!(serde_json::to_value(&empty).expect("serializes"), json!({}));
        let removal: Keyed<Truncated> = Keyed {
            upsert: Vec::new(),
            remove: vec!["actions/op-1".to_owned()],
        };
        assert_eq!(
            serde_json::to_value(&removal).expect("serializes"),
            json!({"remove": ["actions/op-1"]})
        );
        let parsed: Keyed<Truncated> = serde_json::from_value(json!({})).expect("parses");
        assert_eq!(parsed, empty);
    }

    #[test]
    fn tick_frame_carries_seq_and_time() {
        let value = json!({"seq": 9, "time": "2026-01-14T09:19:33.000Z"});
        let frame = decoded(TICK_EVENT, &value);
        assert_eq!(frame.id(), Some(9));
    }

    #[test]
    fn closed_frame_has_no_id_and_a_google_duration_retry_delay() {
        let value = json!({"reason": "VIEWER_CAP", "retryDelay": "5s"});
        let frame = decoded(CLOSED_EVENT, &value);
        assert_eq!(frame.id(), None);
        assert_eq!(
            frame,
            Frame::Closed(Closed {
                reason: ClosedReason::ViewerCap,
                retry_delay: Some(Duration::from_secs(5)),
            })
        );
        let bare = decoded(CLOSED_EVENT, &json!({"reason": "SHUTDOWN"}));
        assert_eq!(
            bare,
            Frame::Closed(Closed {
                reason: ClosedReason::Shutdown,
                retry_delay: None,
            })
        );
    }

    #[test]
    fn unknown_closed_reason_is_kept() {
        let frame = decoded(CLOSED_EVENT, &json!({"reason": "MAINTENANCE"}));
        assert_eq!(
            frame,
            Frame::Closed(Closed {
                reason: ClosedReason::Unrecognized("MAINTENANCE".to_owned()),
                retry_delay: None,
            })
        );
    }

    #[test]
    fn unknown_event_names_are_rejected() {
        assert_eq!(
            Frame::from_parts("message", "{}"),
            Err(FrameError::UnknownEvent {
                name: "message".to_owned()
            })
        );
    }

    #[test]
    fn invalid_data_is_rejected_with_the_event_name() {
        let error = Frame::from_parts(TICK_EVENT, r#"{"seq": 1}"#).expect_err("tick without time");
        assert!(
            matches!(error, FrameError::Data { event: "tick", .. }),
            "{error}"
        );
    }

    fn frames() -> Vec<Frame> {
        vec![
            Frame::from_parts(SNAPSHOT_EVENT, &state_value().to_string())
                .expect("snapshot decodes"),
            Frame::from_parts(DELTA_EVENT, &bare_delta_value().to_string()).expect("delta decodes"),
            Frame::from_parts(
                TICK_EVENT,
                &json!({"seq": 9, "time": "2026-01-14T09:19:33.000Z"}).to_string(),
            )
            .expect("tick decodes"),
            Frame::from_parts(
                CLOSED_EVENT,
                &json!({"reason": "VIEWER_CAP", "retryDelay": "5s"}).to_string(),
            )
            .expect("closed decodes"),
        ]
    }

    fn bare_delta_value() -> Value {
        json!({"seq": 48_112, "base": 48_111, "updateTime": "2026-01-14T09:19:32.000Z"})
    }

    #[test]
    fn frame_ref_names_and_ids_follow_each_variant() {
        let frames = frames();
        let names = [SNAPSHOT_EVENT, DELTA_EVENT, TICK_EVENT, CLOSED_EVENT];
        let ids = [Some(48_111), Some(48_112), Some(9), None];
        for ((frame, name), id) in frames.iter().zip(names).zip(ids) {
            let frame_ref = FrameRef::from(frame);
            assert_eq!(frame_ref.event_name(), name);
            assert_eq!(frame_ref.id(), id);
            assert_eq!(frame.event_name(), name);
            assert_eq!(frame.id(), id);
        }
    }

    #[test]
    fn frame_ref_retry_is_the_closed_retry_delay() {
        let frames = frames();
        let retries = [None, None, None, Some(Duration::from_secs(5))];
        for (frame, retry) in frames.iter().zip(retries) {
            assert_eq!(FrameRef::from(frame).retry(), retry);
        }
        let bare =
            Frame::from_parts(CLOSED_EVENT, r#"{"reason": "SHUTDOWN"}"#).expect("closed decodes");
        assert_eq!(FrameRef::from(&bare).retry(), None);
    }

    #[test]
    fn frame_ref_write_data_round_trips_through_from_parts() {
        for frame in frames() {
            let mut written = Vec::new();
            FrameRef::from(&frame)
                .write_data(&mut written)
                .expect("frame writes");
            let data = String::from_utf8(written).expect("data is utf-8");
            let decoded = Frame::from_parts(frame.event_name(), &data).expect("frame decodes");
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn bare_delta_is_empty_and_each_part_makes_it_non_empty() {
        let bare: Delta = serde_json::from_value(bare_delta_value()).expect("delta decodes");
        assert!(bare.is_empty());

        let parts = [
            (
                "pass",
                json!({"state": "OK", "interval": "2s", "applied": 0, "failed": 0}),
            ),
            ("queue", json!({"total": 0, "unplaceable": 0})),
            ("fleets", json!({"remove": ["fleets/gone"]})),
            ("workers", json!({"remove": ["workers/w0"]})),
            ("instances", json!({"remove": ["instances/i"]})),
            ("actions", json!({"truncated": {"running": 0, "queued": 0}})),
            (
                "events",
                json!([{
                    "name": "events/1",
                    "createTime": "2026-01-14T09:19:32.000Z",
                    "fleet": "fleets/f",
                    "workerRegistered": {"worker": "workers/w1"},
                }]),
            ),
            ("completed", json!(0)),
            ("principalsEpoch", json!(0)),
        ];
        for (part, value) in parts {
            let mut with_part = bare_delta_value();
            with_part[part] = value;
            let delta: Delta = serde_json::from_value(with_part).expect("delta decodes");
            assert!(!delta.is_empty(), "{part} should make the delta non-empty");
        }
    }

    #[test]
    fn delta_with_only_empty_parts_is_empty() {
        let mut value = bare_delta_value();
        value["fleets"] = json!({});
        value["workers"] = json!({});
        value["instances"] = json!({});
        value["events"] = json!([]);
        value["actions"] = json!({});
        let delta: Delta = serde_json::from_value(value).expect("delta decodes");
        assert!(delta.is_empty());
    }

    #[test]
    fn actions_delta_is_empty_until_one_part_is_set() {
        let bare: ActionsDelta = serde_json::from_value(json!({})).expect("actions decodes");
        assert!(bare.is_empty());

        let parts = [
            ("sampleTime", json!("2026-01-14T09:19:32.000Z")),
            ("staleAfter", json!("10s")),
            ("running", json!({"remove": ["actions/op-1"]})),
            ("queued", json!([])),
            ("truncated", json!({"running": 0, "queued": 0})),
        ];
        for (part, value) in parts {
            let mut with_part = json!({});
            with_part[part] = value;
            let actions: ActionsDelta = serde_json::from_value(with_part).expect("actions decodes");
            assert!(
                !actions.is_empty(),
                "{part} should make the actions non-empty"
            );
        }
    }
}
