//! The SSE and JSON encoders reproduce the designer's recorded streams, and the decoder reads them
//! back. Each `.sse` fixture is decoded into frames, encoded again, and the produced frames are
//! compared with the fixture's `event`, `id` and `data` lines (the data as a JSON value). The
//! files reach the test through the `test_env` that `crates/gang-state/BUILD.bazel` sets.

use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_state::DecodeError;
use gang_state::sse::{self, Decoder};
use gang_wire::{Closed, ClosedReason, Delta, Frame, FrameError, FrameRef, Tick, name};
use gang_wire::{Pass, State};
use serde_json::Value;

/// The runfiles variable that lists the UI fixture files, space separated.
const FIXTURES_VAR: &str = "GANG_UI_FIXTURES";

/// One parsed SSE frame: its event name, its `id` when it has one, and its single data line.
#[derive(Debug, PartialEq, Eq)]
struct RawFrame {
    event: String,
    id: Option<String>,
    data: String,
}

/// Returns the fixture `.sse` and `.json` files that the test runfiles list.
fn fixtures() -> Result<Vec<(String, PathBuf)>, Box<dyn Error>> {
    let listed = std::env::var(FIXTURES_VAR)
        .map_err(|_| format!("{FIXTURES_VAR} must be set by the Bazel test_env"))?;
    let root = std::env::current_dir()?;
    let mut files = Vec::new();
    for raw in listed.split_whitespace() {
        let path = root.join(raw);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("fixture path has no file name: {raw}"))?
            .to_owned();
        files.push((name, path));
    }
    Ok(files)
}

/// Splits an SSE stream into frames. Comment lines (`: at <ms>`) and `retry:` lines are skipped.
fn raw_frames(text: &str) -> Vec<RawFrame> {
    let mut out = Vec::new();
    let mut event: Option<String> = None;
    let mut id: Option<String> = None;
    let mut data: Vec<&str> = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            if let Some(name) = event.take() {
                out.push(RawFrame {
                    event: name,
                    id: id.take(),
                    data: data.join("\n"),
                });
            }
            id = None;
            data.clear();
        } else if let Some(name) = line.strip_prefix("event: ") {
            event = Some(name.to_owned());
        } else if let Some(value) = line.strip_prefix("id: ") {
            id = Some(value.to_owned());
        } else if let Some(value) = line.strip_prefix("data: ") {
            data.push(value);
        }
    }
    out
}

/// Returns the one frame that `text` holds, failing when it holds any other count.
fn single(text: &str) -> Result<RawFrame, Box<dyn Error>> {
    let [frame] = <[RawFrame; 1]>::try_from(raw_frames(text))
        .map_err(|_| format!("expected exactly one frame in {text:?}"))?;
    Ok(frame)
}

/// Checks the framing rules: the frame ends with a blank line, holds no other blank line, has one
/// `data:` line, and every line is a known SSE field.
fn assert_well_formed(text: &str) {
    assert!(
        text.ends_with("\n\n"),
        "frame must end with a blank line: {text:?}"
    );
    let body = &text[..text.len() - 2];
    assert!(!body.contains("\n\n"), "frame holds a blank line: {text:?}");
    assert_eq!(
        body.lines()
            .filter(|line| line.starts_with("data: "))
            .count(),
        1,
        "frame must hold exactly one data line: {text:?}"
    );
    for line in body.lines() {
        assert!(
            ["event: ", "id: ", "data: ", "retry: "]
                .iter()
                .any(|prefix| line.starts_with(prefix)),
            "unexpected line {line:?} in {text:?}"
        );
    }
}

/// Parses a JSON document as a value, so the comparison ignores key order and spacing.
fn json(text: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(text)
}

/// Decodes `text` as a whole stream.
fn decode(text: &str) -> Result<Vec<Frame>, DecodeError> {
    Decoder::new().push(text.as_bytes())
}

/// Decodes `bytes` fed to one decoder in chunks of `size` bytes.
fn decode_chunked(bytes: &[u8], size: usize) -> Result<Vec<Frame>, DecodeError> {
    let mut decoder = Decoder::new();
    let mut frames = Vec::new();
    for chunk in bytes.chunks(size) {
        frames.extend(decoder.push(chunk)?);
    }
    Ok(frames)
}

/// Returns the first `snapshot` frame of the live fixture as a typed state.
fn live_state() -> Result<State, Box<dyn Error>> {
    let (_, path) = fixtures()?
        .into_iter()
        .find(|(name, _)| name == "live.sse")
        .ok_or_else(|| format!("live.sse is not listed in {FIXTURES_VAR}"))?;
    decode(&std::fs::read_to_string(path)?)?
        .into_iter()
        .find_map(|frame| match frame {
            Frame::Snapshot(state) => Some(state),
            Frame::Delta(_) | Frame::Tick(_) | Frame::Closed(_) => None,
        })
        .ok_or_else(|| "live.sse holds no snapshot frame".into())
}

/// A small state, so that the codec tests do not depend on the fixtures.
fn state(seq: u64) -> State {
    State {
        name: name::STATE.to_owned(),
        seq,
        update_time: Timestamp::from_unix_millis(1_768_471_770_000),
        provisioner: "test".to_owned(),
        pass: Pass::ok(Duration::from_secs(2), 0, 0),
        events_limit: 200,
        ..State::default()
    }
}

/// A delta that changes nothing but its sequence number and clock.
fn delta(seq: u64) -> Delta {
    Delta {
        seq,
        base: seq - 1,
        update_time: Timestamp::from_unix_millis(1_768_471_772_000),
        pass: None,
        queue: None,
        fleets: None,
        workers: None,
        instances: None,
        actions: None,
        events: Vec::new(),
        completed: None,
        principals_epoch: None,
    }
}

fn tick(seq: u64) -> Tick {
    Tick {
        seq,
        time: Timestamp::from_unix_millis(1_768_471_780_000),
    }
}

fn closed(reason: ClosedReason, retry_delay: Option<Duration>) -> Closed {
    Closed {
        reason,
        retry_delay,
    }
}

#[test]
fn fixture_frames_reproduce_through_the_codec() -> Result<(), Box<dyn Error>> {
    let mut checked = 0_usize;
    for (name, path) in fixtures()? {
        if path.extension().and_then(|extension| extension.to_str()) != Some("sse") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let decoded = decode(&text)?;
        let expected = raw_frames(&text);
        assert_eq!(decoded.len(), expected.len(), "{name}: frame count");
        for (frame, expected) in decoded.iter().zip(&expected) {
            let encoded = sse::encode(frame)?;
            let text = std::str::from_utf8(&encoded)?;
            assert_well_formed(text);
            let produced = single(text)?;
            assert_eq!(produced.event, expected.event, "{name}: event");
            assert_eq!(produced.id, expected.id, "{name}: {} id", expected.event);
            assert_eq!(
                json(&produced.data)?,
                json(&expected.data)?,
                "{name}: {} data",
                expected.event
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no SSE frames were found in {FIXTURES_VAR}");
    Ok(())
}

#[test]
fn snapshot_frame_carries_seq_as_id_and_state_json_as_data() -> Result<(), Box<dyn Error>> {
    let state = live_state()?;
    let body = sse::state_json(&state)?;
    let body = std::str::from_utf8(&body)?;
    let frame = sse::encode(FrameRef::Snapshot(&state))?;
    let expected = format!("event: snapshot\nid: {}\ndata: {body}\n\n", state.seq);
    assert_eq!(std::str::from_utf8(&frame)?, expected);
    assert_well_formed(&expected);
    assert_eq!(json(body)?, serde_json::to_value(&state)?);
    assert_eq!(frame, sse::encode(&Frame::Snapshot(state))?);
    Ok(())
}

#[test]
fn event_matches_encode_for_the_same_json() -> Result<(), Box<dyn Error>> {
    let state = state(9);
    let json = serde_json::to_vec(&state)?;
    assert_eq!(
        sse::event("snapshot", Some(state.seq), None, &json),
        sse::encode(FrameRef::Snapshot(&state))?
    );
    Ok(())
}

#[test]
fn delta_frame_carries_seq_as_id_and_the_delta_json_as_data() -> Result<(), Box<dyn Error>> {
    let delta = delta(2);
    let frame = sse::encode(FrameRef::Delta(&delta))?;
    assert_eq!(
        std::str::from_utf8(&frame)?,
        "event: delta\nid: 2\ndata: {\"seq\":2,\"base\":1,\"updateTime\":\"2026-01-15T10:09:32.000Z\"}\n\n"
    );
    assert_eq!(frame, sse::encode(&Frame::Delta(delta))?);
    Ok(())
}

#[test]
fn tick_frame_is_byte_exact() -> Result<(), Box<dyn Error>> {
    let frame = sse::encode(&Frame::Tick(tick(48_211)))?;
    assert_eq!(
        frame,
        "event: tick\nid: 48211\ndata: {\"seq\":48211,\"time\":\"2026-01-15T10:09:40.000Z\"}\n\n"
    );
    Ok(())
}

#[test]
fn closed_frame_with_retry_sets_the_retry_line_and_retry_delay() -> Result<(), Box<dyn Error>> {
    let sent = closed(ClosedReason::ViewerCap, Some(Duration::from_secs(5)));
    let frame = sse::encode(&Frame::Closed(sent.clone()))?;
    assert_eq!(
        frame,
        "retry: 5000\nevent: closed\ndata: {\"reason\":\"VIEWER_CAP\",\"retryDelay\":\"5s\"}\n\n"
    );
    assert_eq!(decode(std::str::from_utf8(&frame)?)?, [Frame::Closed(sent)]);
    Ok(())
}

#[test]
fn closed_frame_without_retry_omits_the_retry_line_and_retry_delay() -> Result<(), Box<dyn Error>> {
    let sent = closed(ClosedReason::Revoked, None);
    let frame = sse::encode(&Frame::Closed(sent.clone()))?;
    assert_eq!(frame, "event: closed\ndata: {\"reason\":\"REVOKED\"}\n\n");
    assert_eq!(decode(std::str::from_utf8(&frame)?)?, [Frame::Closed(sent)]);
    Ok(())
}

#[test]
fn closed_reasons_use_their_wire_names() -> Result<(), Box<dyn Error>> {
    let cases = [
        (ClosedReason::Revoked, "REVOKED"),
        (ClosedReason::Expired, "EXPIRED"),
        (ClosedReason::ViewerCap, "VIEWER_CAP"),
        (ClosedReason::Shutdown, "SHUTDOWN"),
    ];
    for (reason, wire) in cases {
        let sent = Frame::Closed(closed(reason.clone(), None));
        let frame = sse::encode(&sent)?;
        assert_eq!(decode(std::str::from_utf8(&frame)?)?, [sent]);
        assert_eq!(
            serde_json::to_value(&reason)?,
            Value::String(wire.to_owned())
        );
    }
    Ok(())
}

/// One frame of every kind, as a stream sends them.
fn every_kind() -> Vec<Frame> {
    vec![
        Frame::Snapshot(state(7)),
        Frame::Delta(delta(8)),
        Frame::Tick(tick(8)),
        Frame::Closed(closed(ClosedReason::Shutdown, Some(Duration::from_secs(5)))),
        Frame::Closed(closed(ClosedReason::Expired, None)),
    ]
}

#[test]
fn every_frame_kind_round_trips() -> Result<(), Box<dyn Error>> {
    let frames = every_kind();
    let mut stream = Vec::new();
    for frame in &frames {
        stream.extend_from_slice(&sse::encode(frame)?);
    }
    assert_eq!(decode_chunked(&stream, stream.len())?, frames);
    for size in 1..=9 {
        assert_eq!(decode_chunked(&stream, size)?, frames, "chunks of {size}");
    }
    Ok(())
}

#[test]
fn decoder_reads_crlf_comments_and_multi_line_data() -> Result<(), Box<dyn Error>> {
    let text = ": a comment\r\n\
                event: tick\r\n\
                id: 3\r\n\
                retry: 1000\r\n\
                data: {\"seq\":3,\r\n\
                data: \"time\":\"2026-01-15T10:09:40.000Z\"}\r\n\
                \r\n";
    let expected = [Frame::Tick(Tick {
        seq: 3,
        time: Timestamp::from_unix_millis(1_768_471_780_000),
    })];
    assert_eq!(decode(text)?, expected);
    assert_eq!(decode(&text.replace("\r\n", "\n"))?, expected);
    for size in 1..=5 {
        assert_eq!(decode_chunked(text.as_bytes(), size)?, expected);
    }
    Ok(())
}

#[test]
fn decoder_accepts_fields_without_the_space_after_the_colon() -> Result<(), Box<dyn Error>> {
    let text = "event:tick\ndata:{\"seq\":3,\"time\":\"2026-01-15T10:09:40.000Z\"}\n\n";
    assert_eq!(decode(text)?, [Frame::Tick(tick(3))]);
    Ok(())
}

#[test]
fn decoder_drops_events_without_data_and_unfinished_events() -> Result<(), Box<dyn Error>> {
    let mut decoder = Decoder::new();
    assert!(decoder.push(b"event: tick\n\n: beat\n\n")?.is_empty());
    let partial = b"event: tick\ndata: {\"seq\":3,\"time\":\"2026-01-15T10:09:40.000Z\"}\n";
    assert!(decoder.push(partial)?.is_empty());
    assert_eq!(decoder.push(b"\n")?, [Frame::Tick(tick(3))]);
    Ok(())
}

#[test]
fn decoder_rejects_unknown_events_and_bad_payloads() {
    let Err(DecodeError::Frame(FrameError::UnknownEvent { name })) =
        decode("event: bogus\ndata: {}\n\n")
    else {
        panic!("an unknown event must be rejected");
    };
    assert_eq!(name, "bogus");
    let nameless = decode("data: {}\n\n");
    assert!(
        matches!(
            nameless,
            Err(DecodeError::Frame(FrameError::UnknownEvent { .. }))
        ),
        "{nameless:?}"
    );
    let payload = decode("event: tick\ndata: {\"seq\":\"x\"}\n\n");
    assert!(
        matches!(payload, Err(DecodeError::Frame(FrameError::Data { .. }))),
        "{payload:?}"
    );
}

#[test]
fn decoder_rejects_lines_that_are_not_utf8() {
    let error = Decoder::new().push(b"event: \xff\n");
    assert!(matches!(error, Err(DecodeError::Utf8(_))), "{error:?}");
}

#[test]
fn a_frame_with_multi_byte_text_decodes_from_one_byte_chunks() -> Result<(), Box<dyn Error>> {
    let mut snapshot = state(7);
    snapshot.provisioner = "café-✓-🚀".to_owned();
    let bytes = sse::encode(FrameRef::Snapshot(&snapshot))?;
    let mut decoder = Decoder::new();
    let mut frames = Vec::new();
    for byte in &bytes {
        frames.extend(decoder.push(&[*byte])?);
    }
    assert_eq!(frames, vec![Frame::Snapshot(snapshot)]);
    Ok(())
}
