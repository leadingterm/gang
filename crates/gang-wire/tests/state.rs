//! The wire types round trip the recorded UI fixtures. Each frame of the `.sse` streams is decoded
//! with `Frame::from_parts`, re-encoded with `FrameRef::write_data`, and compared with the fixture
//! as a JSON value, as is each JSON body, so a field that the Rust types drop, rename or retype fails
//! here. The files reach the test through the `test_env` that `crates/gang-wire/BUILD.bazel` sets.

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

#[test]
fn sse_frames_round_trip() -> Result<(), Box<dyn Error>> {
    let mut checked = 0_usize;
    for (name, path) in fixtures()? {
        if path.extension().and_then(|extension| extension.to_str()) != Some("sse") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        for message in messages(&text)? {
            let expected: Value = serde_json::from_str(&message.data)?;
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
