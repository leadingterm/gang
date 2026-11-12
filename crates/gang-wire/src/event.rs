//! The `events/{seq}` resource: the audit log of what the server did while it scaled its fleets.

use gang_clock::Timestamp;
use gang_types::ScalingCause;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::page::PageToken;

/// One entry of the scaling audit log, named `events/{seq}`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
#[ts(rename = "ScalingEvent")]
pub struct Event {
    /// Resource name, `events/{seq}`; the sequence number is strictly increasing.
    pub name: String,
    /// When the change was recorded.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub create_time: Timestamp,
    /// Reference `fleets/{fleet}` of the fleet the change applies to.
    pub fleet: String,
    /// The autoscaler decision behind the event, when there is one. Absent for worker joins and
    /// leaves, and for rows recorded before causes were kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Cause>,
    /// The change itself; its member key and body are serialized beside the fields above.
    #[serde(flatten)]
    pub change: EventChange,
}

wire_enum! {
    /// Why the autoscaler made a scaling decision, as written on the wire.
    pub enum Cause from ScalingCause {
        /// Queued work needed another worker.
        Demand = "DEMAND",
        /// A worker stayed idle past its timeout.
        IdleTimeout = "IDLE_TIMEOUT",
        /// A worker lost its host and no longer has a reason to run.
        Orphan = "ORPHAN",
        /// A draining worker finished its work and was retired.
        DrainFinished = "DRAIN_FINISHED",
        /// A draining worker reached the longest allowed wait and was retired anyway.
        MaxDrainWait = "MAX_DRAIN_WAIT",
    }
}

/// What an [`Event`] changed: exactly one member key is present.
///
/// A member this version does not know deserializes to [`EventChange::Unrecognized`] and
/// serializes back unchanged. So does an event that carries a field beside the member that this
/// version does not know, and a known member whose body does not parse.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum EventChange {
    /// The fleet's size changed from `from` to `to` instances.
    ScaledOut {
        /// Size before the change.
        from: u32,
        /// Size after the change.
        to: u32,
    },
    /// A worker began draining.
    DrainStarted {
        /// Reference `workers/{worker}` of the worker that started to drain.
        worker: String,
    },
    /// A draining worker stopped draining.
    DrainCancelled {
        /// Reference `workers/{worker}` of the worker whose drain was cancelled.
        worker: String,
    },
    /// An instance was terminated.
    Terminated {
        /// Reference `instances/{instance}` of the instance that was terminated.
        instance: String,
    },
    /// An instance's worker failed soon after its launch, before it could serve; its fleet
    /// relaunches it after a backoff.
    ExitedEarly {
        /// Reference `instances/{instance}` of the instance whose worker failed.
        instance: String,
        /// The worker's own failure record: the end of its log from its last `fail` record.
        failure: String,
    },
    /// A worker connected to the scheduler.
    WorkerRegistered {
        /// Reference `workers/{worker}` of the worker that connected.
        worker: String,
    },
    /// A worker that was connected is no longer.
    WorkerDeregistered {
        /// Reference `workers/{worker}` of the worker that disconnected.
        worker: String,
    },
    /// A change this version does not know, kept so it round-trips.
    #[serde(untagged)]
    #[ts(skip)]
    Unrecognized(Map<String, Value>),
}

/// One page of events, newest first: the body of `GET /v1/events`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ListEventsResponse {
    /// The events on this page.
    pub events: Vec<Event>,
    /// Token that resumes the list; absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<PageToken>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn round_trip(value: &Value) -> Event {
        let event: Event = serde_json::from_value(value.clone()).expect("event parses");
        assert_eq!(
            &serde_json::to_value(&event).expect("event serializes"),
            value
        );
        event
    }

    #[test]
    fn drain_started_with_cause_round_trips_exactly() {
        let event = round_trip(&json!({
            "name": "events/996",
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/amd64-spot",
            "cause": "IDLE_TIMEOUT",
            "drainStarted": {"worker": "workers/i-0e1000000000000a-0194d5a2"},
        }));
        assert_eq!(event.cause, Some(Cause::IdleTimeout));
        assert_eq!(
            event.change,
            EventChange::DrainStarted {
                worker: "workers/i-0e1000000000000a-0194d5a2".to_owned()
            }
        );
    }

    #[test]
    fn every_member_round_trips_without_a_cause() {
        let bodies = [
            json!({"scaledOut": {"from": 0, "to": 6}}),
            json!({"drainStarted": {"worker": "workers/w"}}),
            json!({"drainCancelled": {"worker": "workers/w"}}),
            json!({"terminated": {"instance": "instances/i"}}),
            json!({"exitedEarly": {"instance": "instances/local-1", "failure": "fail  gang worker: the scratch volume is too small"}}),
            json!({"workerRegistered": {"worker": "workers/w"}}),
            json!({"workerDeregistered": {"worker": "workers/w"}}),
        ];
        for body in bodies {
            let mut value = json!({
                "name": "events/1",
                "createTime": "2026-01-14T09:19:30.000Z",
                "fleet": "fleets/amd64-spot",
            });
            value
                .as_object_mut()
                .expect("event is an object")
                .extend(body.as_object().expect("body is an object").clone());
            let event = round_trip(&value);
            assert_eq!(event.cause, None);
            assert!(
                !matches!(event.change, EventChange::Unrecognized(_)),
                "{value}"
            );
        }
    }

    #[test]
    fn exited_early_preserves_the_failure_record() {
        let event = round_trip(&json!({
            "name": "events/2",
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/local",
            "exitedEarly": {
                "instance": "instances/local-1",
                "failure": "fail  gang worker: the scratch volume is too small",
            },
        }));
        assert_eq!(event.cause, None);
        assert_eq!(
            event.change,
            EventChange::ExitedEarly {
                instance: "instances/local-1".to_owned(),
                failure: "fail  gang worker: the scratch volume is too small".to_owned(),
            }
        );
    }

    #[test]
    fn unknown_member_is_kept_and_round_trips() {
        let event = round_trip(&json!({
            "name": "events/2",
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/amd64-spot",
            "cause": "DEMAND",
            "hibernated": {"instance": "instances/i"},
        }));
        assert_eq!(
            event.change,
            EventChange::Unrecognized(
                json!({"hibernated": {"instance": "instances/i"}})
                    .as_object()
                    .expect("member is an object")
                    .clone()
            )
        );
    }

    #[test]
    fn unknown_fields_of_a_known_member_are_ignored() {
        let event: Event = serde_json::from_value(json!({
            "name": "events/3",
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/amd64-spot",
            "scaledOut": {"from": 1, "to": 2, "reason": "future"},
        }))
        .expect("event parses");
        assert_eq!(event.change, EventChange::ScaledOut { from: 1, to: 2 });
    }

    #[test]
    fn event_without_a_name_is_rejected() {
        let value = json!({
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/amd64-spot",
            "scaledOut": {"from": 0, "to": 1},
        });
        assert!(serde_json::from_value::<Event>(value).is_err());
    }

    #[test]
    fn list_response_omits_an_absent_token() {
        let value = json!({"events": []});
        let parsed: ListEventsResponse =
            serde_json::from_value(value.clone()).expect("response parses");
        assert_eq!(parsed.next_page_token, None);
        assert_eq!(
            serde_json::to_value(&parsed).expect("response serializes"),
            value
        );
    }

    #[test]
    fn unrecognized_cause_round_trips_exactly() {
        let event = round_trip(&json!({
            "name": "events/4",
            "createTime": "2026-01-14T09:19:30.000Z",
            "fleet": "fleets/amd64-spot",
            "cause": "SOMETHING_NEW",
            "scaledOut": {"from": 1, "to": 2},
        }));
        assert_eq!(
            event.cause,
            Some(Cause::Unrecognized("SOMETHING_NEW".to_owned()))
        );
    }

    #[test]
    fn cause_wire_text_is_the_upper_snake_case_scaling_name() {
        for cause in ScalingCause::ALL {
            assert_eq!(
                Cause::from(cause).as_str(),
                cause.as_str().to_ascii_uppercase()
            );
        }
    }
}
