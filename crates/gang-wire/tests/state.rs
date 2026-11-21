//! The wire types round trip the recorded UI fixtures. Each frame of the `.sse` streams is decoded
//! with `Frame::from_parts`, re-encoded with `FrameRef::write_data`, and compared with the fixture
//! as a JSON value (with defaults added for fields absent from older fixtures), as is each JSON
//! body, so a field that the Rust types drop, rename or retype fails here. The files reach the test
//! through the `test_env` that `crates/gang-wire/BUILD.bazel` sets.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Debug;
use std::path::PathBuf;

use gang_wire::{
    Frame, FrameRef, Known, ListEventsResponse, ListPrincipalsResponse, ListRolesResponse, Session,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The runfiles variable that lists the UI fixture files, space separated.
const FIXTURES_VAR: &str = "GANG_UI_FIXTURES";

/// One server-sent event of a recorded stream.
struct Message {
    event: String,
    id: Option<u64>,
    data: String,
}

/// Returns the fixture files keyed by file name.
fn fixtures() -> Result<BTreeMap<String, PathBuf>, Box<dyn Error>> {
    let listed = std::env::var(FIXTURES_VAR)
        .map_err(|_| format!("{FIXTURES_VAR} must be set by the Bazel test_env"))?;
    let root = std::env::current_dir()?;
    let mut files = BTreeMap::new();
    for raw in listed.split_whitespace() {
        let path = root.join(raw);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("fixture path has no file name: {raw}"))?
            .to_owned();
        files.insert(name, path);
    }
    Ok(files)
}

/// Splits an SSE stream into messages. Comment lines (`: at <ms>`) carry nothing the typed
/// schema models, so they are skipped.
fn messages(text: &str) -> Result<Vec<Message>, Box<dyn Error>> {
    let mut out = Vec::new();
    let mut event: Option<String> = None;
    let mut id: Option<u64> = None;
    let mut data: Vec<&str> = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            if let Some(event) = event.take() {
                out.push(Message {
                    event,
                    id: id.take(),
                    data: data.join("\n"),
                });
            }
            id = None;
            data.clear();
        } else if let Some(name) = line.strip_prefix("event: ") {
            event = Some(name.to_owned());
        } else if let Some(value) = line.strip_prefix("id: ") {
            id = Some(value.parse()?);
        } else if let Some(value) = line.strip_prefix("data: ") {
            data.push(value);
        }
    }
    if let Some(event) = event.take() {
        out.push(Message {
            event,
            id,
            data: data.join("\n"),
        });
    }
    Ok(out)
}

/// Fails when a typed value of `name` holds an `Unrecognized` member: the wire enums and untagged
/// members round-trip any text, so a misspelled value would otherwise pass the round trip.
fn check_known<T: Known>(name: &str, typed: &T) {
    assert!(
        typed.is_known(),
        "{name} contains an enum value or member this version does not know"
    );
}

/// Reads one JSON fixture and checks that `T` round trips it.
fn check_json<T>(files: &BTreeMap<String, PathBuf>, name: &str) -> Result<(), Box<dyn Error>>
where
    T: DeserializeOwned + Serialize + Debug + Known,
{
    let path = files
        .get(name)
        .ok_or_else(|| format!("fixture {name} is not listed in {FIXTURES_VAR}"))?;
    let text = std::fs::read_to_string(path)?;
    let expected: Value = serde_json::from_str(&text)?;
    let typed = serde_json::from_str::<T>(&text)?;
    check_known(name, &typed);
    let actual = serde_json::to_value(&typed)?;
    assert_eq!(actual, expected, "{name} round trip");
    Ok(())
}

/// Older fixtures predate fleet cache totals, which deserialize to an explicit default.
fn add_default_fleet_cache(event: &str, expected: &mut Value) {
    let fleets = match event {
        "snapshot" => expected.get_mut("fleets"),
        "delta" => expected
            .get_mut("fleets")
            .and_then(|fleets| fleets.get_mut("upsert")),
        _ => None,
    };
    if let Some(fleets) = fleets.and_then(Value::as_array_mut) {
        for fleet in fleets {
            if let Some(fleet) = fleet.as_object_mut() {
                fleet
                    .entry("workerCache")
                    .or_insert_with(|| serde_json::json!(gang_wire::FleetWorkerCache::default()));
            }
        }
    }
}

#[test]
fn sse_frames_round_trip() -> Result<(), Box<dyn Error>> {
    let mut checked = 0_usize;
    for (name, path) in fixtures()? {
        if path.extension().and_then(|extension| extension.to_str()) != Some("sse") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        for message in messages(&text)? {
            let mut expected: Value = serde_json::from_str(&message.data)?;
            add_default_fleet_cache(&message.event, &mut expected);
            let frame = Frame::from_parts(&message.event, &message.data)
                .map_err(|error| format!("{name}: {error}"))?;
            check_known(&name, &frame);
            let mut buffer = Vec::new();
            FrameRef::from(&frame).write_data(&mut buffer)?;
            let actual: Value = serde_json::from_slice(&buffer)?;
            assert_eq!(
                actual, expected,
                "{name}: {} frame round trip",
                message.event
            );
            assert_eq!(frame.event_name(), message.event, "{name}: event name");
            assert_eq!(frame.id(), message.id, "{name}: {} frame id", message.event);
            checked += 1;
        }
    }
    assert!(checked > 0, "no SSE frames were found in {FIXTURES_VAR}");
    Ok(())
}

#[test]
fn json_bodies_round_trip() -> Result<(), Box<dyn Error>> {
    let files = fixtures()?;
    check_json::<Session>(&files, "session.json")?;
    check_json::<ListPrincipalsResponse>(&files, "principals.json")?;
    check_json::<ListRolesResponse>(&files, "roles.json")?;
    check_json::<ListEventsResponse>(&files, "events.json")?;
    check_json::<gang_wire::DeviceAuthorization>(&files, "device-authorization.json")?;
    Ok(())
}

#[test]
fn fleet_isa_preserves_unknown_values_and_marks_them_unknown() -> Result<(), Box<dyn Error>> {
    let mut value = serde_json::to_value(gang_wire::Fleet::default())?;
    value["isa"] = serde_json::json!("riscv64");
    let fleet: gang_wire::Fleet = serde_json::from_value(value.clone())?;
    assert_eq!(
        fleet.isa,
        gang_wire::Isa::Unrecognized("riscv64".to_owned())
    );
    assert!(!fleet.is_known());
    assert_eq!(serde_json::to_value(fleet)?, value);
    Ok(())
}

#[test]
fn action_priorities_reject_values_outside_int32() -> Result<(), Box<dyn Error>> {
    let mut running = serde_json::to_value(gang_wire::RunningAction::default())?;
    let mut queued = serde_json::to_value(gang_wire::QueuedAction::default())?;
    for priority in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
        running["priority"] = serde_json::json!(priority);
        queued["priority"] = serde_json::json!(priority);
        assert!(serde_json::from_value::<gang_wire::RunningAction>(running.clone()).is_err());
        assert!(serde_json::from_value::<gang_wire::QueuedAction>(queued.clone()).is_err());
    }
    Ok(())
}

#[test]
fn fleet_launch_failure_json_round_trips_and_is_omitted_when_absent() -> Result<(), Box<dyn Error>>
{
    let mut fleet = gang_wire::Fleet::default();
    let absent = serde_json::to_value(&fleet)?;
    assert!(absent.get("launchFailure").is_none());
    assert_eq!(
        serde_json::from_value::<gang_wire::Fleet>(absent.clone())?,
        fleet
    );

    fleet.launch_failure = Some(gang_wire::LaunchFailure {
        reason: "capacity unavailable".to_owned(),
    });
    let mut expected = absent;
    expected["launchFailure"] = serde_json::json!({"reason": "capacity unavailable"});
    assert_eq!(serde_json::to_value(&fleet)?, expected);
    assert_eq!(serde_json::from_value::<gang_wire::Fleet>(expected)?, fleet);
    Ok(())
}

#[test]
fn queued_refusal_json_round_trips_and_is_omitted_when_absent() -> Result<(), Box<dyn Error>> {
    let mut action = gang_wire::QueuedAction::default();
    let absent = serde_json::to_value(&action)?;
    assert!(absent.get("refusal").is_none());
    assert_eq!(
        serde_json::from_value::<gang_wire::QueuedAction>(absent.clone())?,
        action
    );

    action.refusal = Some("no single fleet accepts every requested property".to_owned());
    let mut expected = absent;
    expected["refusal"] = serde_json::json!("no single fleet accepts every requested property");
    assert_eq!(serde_json::to_value(&action)?, expected);
    assert_eq!(
        serde_json::from_value::<gang_wire::QueuedAction>(expected)?,
        action
    );
    Ok(())
}

#[test]
fn worker_cache_round_trips_absent_zero_and_values() -> Result<(), Box<dyn Error>> {
    use gang_wire::{CacheReads, Worker, WorkerCache};

    let mut worker = Worker::default();
    let absent = serde_json::to_value(&worker)?;
    assert!(absent.get("cache").is_none());
    assert_eq!(serde_json::from_value::<Worker>(absent)?, worker);

    worker.cache = Some(WorkerCache::default());
    let zero = serde_json::to_value(&worker)?;
    assert_eq!(
        zero["cache"],
        serde_json::json!({
            "local": {"requests": 0, "bytes": 0},
            "remote": {"requests": 0, "bytes": 0},
            "evictions": 0, "evicted": 0, "used": 0, "budget": 0
        })
    );
    assert_eq!(serde_json::from_value::<Worker>(zero)?, worker);

    worker.cache = Some(WorkerCache {
        local: CacheReads {
            requests: 1,
            bytes: 2,
        },
        remote: CacheReads {
            requests: 3,
            bytes: 4,
        },
        evictions: 5,
        evicted: 6,
        used: 7,
        budget: 8,
    });
    let measured = serde_json::to_value(&worker)?;
    assert_eq!(
        measured["cache"],
        serde_json::json!({
            "local": {"requests": 1, "bytes": 2},
            "remote": {"requests": 3, "bytes": 4},
            "evictions": 5, "evicted": 6, "used": 7, "budget": 8
        })
    );
    assert_eq!(serde_json::from_value::<Worker>(measured)?, worker);
    Ok(())
}

#[test]
fn fleet_worker_cache_defaults_when_absent_and_round_trips() -> Result<(), Box<dyn Error>> {
    use gang_wire::{Fleet, FleetWorkerCache};

    let fleet = Fleet::default();
    let mut legacy = serde_json::to_value(&fleet)?;
    legacy.as_object_mut().unwrap().remove("workerCache");
    assert_eq!(serde_json::from_value::<Fleet>(legacy)?, fleet);

    let mut fleet = fleet;
    fleet.worker_cache = FleetWorkerCache {
        reporting: 1,
        workers: 2,
        ..FleetWorkerCache::default()
    };
    let json = serde_json::to_value(&fleet)?;
    assert_eq!(json["workerCache"]["reporting"], 1);
    assert_eq!(json["workerCache"]["workers"], 2);
    assert_eq!(json["workerCache"]["totals"]["local"]["bytes"], 0);
    assert_eq!(serde_json::from_value::<Fleet>(json)?, fleet);
    Ok(())
}

#[test]
fn reservation_fields_round_trip_absent_zero_and_learned() -> Result<(), Box<dyn Error>> {
    use gang_wire::{QueuedAction, Reservation, RunningAction, Worker};

    let zero = Reservation::default();
    let fitted = Reservation {
        cpu_millicores: 500,
        memory: 1024,
    };
    let learned = Reservation {
        cpu_millicores: 1000,
        memory: 2048,
    };

    let mut worker = Worker::default();
    let old_worker = serde_json::to_value(&worker)?;
    assert!(old_worker.get("reserved").is_none());
    assert!(old_worker.get("capacity").is_none());
    assert_eq!(serde_json::from_value::<Worker>(old_worker)?, worker);
    worker.reserved = Some(zero);
    worker.capacity = Some(fitted);
    let json = serde_json::to_value(&worker)?;
    assert_eq!(
        json["reserved"],
        serde_json::json!({"cpuMillicores": 0, "memory": 0})
    );
    assert_eq!(
        json["capacity"],
        serde_json::json!({"cpuMillicores": 500, "memory": 1024})
    );
    assert_eq!(serde_json::from_value::<Worker>(json)?, worker);

    let mut running = RunningAction::default();
    let old_running = serde_json::to_value(&running)?;
    assert!(old_running.get("reservation").is_none());
    assert!(old_running.get("learnedReservation").is_none());
    assert_eq!(
        serde_json::from_value::<RunningAction>(old_running)?,
        running
    );
    running.reservation = Some(zero);
    let json = serde_json::to_value(&running)?;
    assert_eq!(
        json["reservation"],
        serde_json::json!({"cpuMillicores": 0, "memory": 0})
    );
    assert_eq!(serde_json::from_value::<RunningAction>(json)?, running);
    running.reservation = Some(fitted);
    running.learned_reservation = Some(learned);
    let json = serde_json::to_value(&running)?;
    assert_eq!(
        json["learnedReservation"],
        serde_json::json!({"cpuMillicores": 1000, "memory": 2048})
    );
    assert_eq!(serde_json::from_value::<RunningAction>(json)?, running);

    let mut queued = QueuedAction::default();
    let old_queued = serde_json::to_value(&queued)?;
    assert!(old_queued.get("reservation").is_none());
    assert!(old_queued.get("learnedReservation").is_none());
    assert_eq!(serde_json::from_value::<QueuedAction>(old_queued)?, queued);
    queued.reservation = Some(fitted);
    queued.learned_reservation = Some(learned);
    let json = serde_json::to_value(&queued)?;
    assert_eq!(
        json["reservation"],
        serde_json::json!({"cpuMillicores": 500, "memory": 1024})
    );
    assert_eq!(
        json["learnedReservation"],
        serde_json::json!({"cpuMillicores": 1000, "memory": 2048})
    );
    assert_eq!(serde_json::from_value::<QueuedAction>(json)?, queued);
    Ok(())
}

#[test]
fn launch_failed_and_withdrawn_changes_round_trip_exact_json() -> Result<(), Box<dyn Error>> {
    for (change, expected) in [
        (
            gang_wire::EventChange::LaunchFailed {
                from: 2,
                to: 4,
                reason: "capacity unavailable".to_owned(),
            },
            serde_json::json!({
                "launchFailed": {"from": 2, "to": 4, "reason": "capacity unavailable"}
            }),
        ),
        (
            gang_wire::EventChange::Withdrawn { from: 4, to: 2 },
            serde_json::json!({"withdrawn": {"from": 4, "to": 2}}),
        ),
    ] {
        assert_eq!(serde_json::to_value(&change)?, expected);
        assert_eq!(
            serde_json::from_value::<gang_wire::EventChange>(expected)?,
            change
        );
    }
    Ok(())
}
