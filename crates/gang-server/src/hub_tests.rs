use std::num::NonZeroUsize;
use std::time::Duration;

use crate::hub::{HubConfig, Joined, Next, StateHub, ViewerCap};
use bytes::Bytes;
use futures::FutureExt;
use gang_clock::Timestamp;
use gang_types::FleetName;
use gang_wire::{Event, EventChange, Pass, State, name};

use crate::test_support::decode_sse;

const ONE: NonZeroUsize = NonZeroUsize::MIN;
const TWO: NonZeroUsize = NonZeroUsize::new(2).expect("two is non-zero");
const FOUR: NonZeroUsize = NonZeroUsize::new(4).expect("four is non-zero");
const EIGHT: NonZeroUsize = NonZeroUsize::new(8).expect("eight is non-zero");

fn config(max_viewers: NonZeroUsize, lag_frames: NonZeroUsize, recent: NonZeroUsize) -> HubConfig {
    HubConfig {
        max_viewers,
        retry_after: Duration::from_secs(5),
        lag_frames,
        recent_events: recent,
    }
}

fn hub() -> StateHub {
    StateHub::new(config(FOUR, EIGHT, EIGHT), initial())
}

fn initial() -> State {
    State {
        name: name::STATE.to_owned(),
        update_time: Timestamp::from_unix_millis(1_000_000),
        provisioner: "local".to_owned(),
        pass: Pass::ok(Duration::from_secs(1), 0, 0),
        events_limit: 100,
        ..State::default()
    }
}

fn scaling_event(seq: u64) -> Event {
    let fleet = match FleetName::try_new("example-fleet") {
        Ok(fleet) => fleet,
        Err(error) => panic!("example-fleet is a valid fleet name: {error:?}"),
    };
    Event {
        name: name::of::<name::Events>(&seq),
        create_time: Timestamp::from_unix_millis(1_000_000 + seq),
        fleet: name::of::<name::Fleets>(&fleet),
        cause: None,
        change: EventChange::ScaledOut { from: 0, to: 1 },
    }
}

/// Publishes a state whose only change is `completed`.
fn set_completed(hub: &StateHub, completed: u64) {
    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.completed = completed;
    });
}

/// The JSON body of a `GET /v1/state` response.
fn json(body: &[u8]) -> serde_json::Value {
    match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => panic!("state body is JSON: {error}"),
    }
}

/// The decoded JSON payload of an SSE frame.
fn data(frame: &[u8]) -> serde_json::Value {
    match decode_sse(frame) {
        gang_wire::Frame::Snapshot(value) => serde_json::to_value(value),
        gang_wire::Frame::Delta(value) => serde_json::to_value(value),
        gang_wire::Frame::Tick(value) => serde_json::to_value(value),
        gang_wire::Frame::Closed(value) => serde_json::to_value(value),
    }
    .expect("frame payload serializes")
}

/// The sequence number carried by an SSE frame's `id:` line.
fn id(frame: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(frame).ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix("id: "))
        .and_then(|id| id.parse().ok())
}

async fn delta(joined: &mut Joined) -> Bytes {
    match joined.next().await {
        Next::Frame(bytes) => bytes,
        other => panic!("expected a delta frame, got {other:?}"),
    }
}

#[tokio::test]
async fn frames_carry_consecutive_sequence_numbers() {
    let hub = hub();
    let mut joined = hub.join().expect("first viewer joins");
    assert_eq!(id(joined.snapshot()), Some(0));

    set_completed(&hub, 1);
    set_completed(&hub, 2);
    set_completed(&hub, 3);
    assert_eq!(hub.seq(), 3);

    for expected in 1..=3 {
        let frame = delta(&mut joined).await;
        assert_eq!(data(&frame)["seq"].as_u64(), Some(expected));
    }
}

#[tokio::test]
async fn unchanged_publish_sends_nothing_and_keeps_sequence() {
    let hub = hub();
    let mut joined = hub.join().expect("viewer joins");

    set_completed(&hub, 1);
    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
    });
    assert_eq!(hub.seq(), 1);
    set_completed(&hub, 2);

    let first = delta(&mut joined).await;
    assert_eq!(data(&first)["seq"].as_u64(), Some(1));
    let second = delta(&mut joined).await;
    let body = data(&second);
    assert_eq!(body["seq"].as_u64(), Some(2));
    assert_eq!(body["base"].as_u64(), Some(1));
}

#[tokio::test]
async fn publishes_without_viewers_keep_content_and_sequence() {
    let hub = hub();
    set_completed(&hub, 5);
    assert_eq!(hub.seq(), 0);

    let state = json(&hub.state_json().expect("state encodes"));
    assert_eq!(state["completed"].as_u64(), Some(5));

    let mut joined = hub.join().expect("viewer joins");
    assert_eq!(id(joined.snapshot()), Some(0));
    assert_eq!(data(joined.snapshot())["completed"].as_u64(), Some(5));

    set_completed(&hub, 6);
    let frame = delta(&mut joined).await;
    let body = data(&frame);
    assert_eq!(body["seq"].as_u64(), Some(1));
    assert_eq!(body["base"].as_u64(), Some(0));
}

#[tokio::test]
async fn lagging_viewer_is_resynced_then_continues() {
    let hub = StateHub::new(config(FOUR, ONE, EIGHT), initial());
    let mut joined = hub.join().expect("viewer joins");

    set_completed(&hub, 1);
    set_completed(&hub, 2);
    set_completed(&hub, 3);

    let Next::Resync(snapshot) = joined.next().await else {
        panic!("a lagging viewer is resynced, not closed");
    };
    assert_eq!(id(&snapshot), Some(3));
    assert_eq!(data(&snapshot)["completed"].as_u64(), Some(3));

    set_completed(&hub, 4);
    let frame = delta(&mut joined).await;
    let body = data(&frame);
    assert_eq!(body["seq"].as_u64(), Some(4));
    assert_eq!(body["base"].as_u64(), Some(3));
}

#[tokio::test]
async fn viewer_cap_refuses_and_guard_frees_slot_on_drop() {
    let hub = StateHub::new(config(ONE, EIGHT, EIGHT), initial());
    let first = hub.join().expect("first viewer joins");

    assert_eq!(hub.join().err(), Some(ViewerCap));

    drop(first);
    assert!(
        hub.join().is_ok(),
        "the slot is free after the viewer drops"
    );
}

#[tokio::test]
async fn preload_keeps_the_newest_recent_events() {
    let hub = StateHub::new(config(FOUR, EIGHT, TWO), initial());
    hub.preload(vec![scaling_event(1), scaling_event(2), scaling_event(3)]);

    let joined = hub.join().expect("viewer joins");
    let events = data(joined.snapshot())["events"].clone();
    let names: Vec<&str> = events
        .as_array()
        .expect("events is an array")
        .iter()
        .filter_map(|event| event["name"].as_str())
        .collect();
    assert_eq!(names, vec!["events/2", "events/3"]);
}

#[tokio::test]
async fn recorded_event_reaches_clients_with_the_next_publish() {
    let hub = hub();
    let mut joined = hub.join().expect("viewer joins");

    hub.record_event(scaling_event(9));
    hub.publish(|cur, prev, seq, events| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.events = events.to_vec();
    });

    let frame = delta(&mut joined).await;
    let body = data(&frame);
    let events = body["events"].as_array().expect("delta carries events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"].as_str(), Some("events/9"));
}

#[tokio::test]
async fn principals_change_publishes_only_the_epoch() {
    let hub = hub();
    let mut joined = hub.join().expect("viewer joins");
    let initial_time = data(joined.snapshot())["updateTime"].clone();

    hub.principals_changed(3);
    hub.principals_changed(3);

    let frame = delta(&mut joined).await;
    assert_eq!(
        data(&frame),
        serde_json::json!({
            "seq": 1,
            "base": 0,
            "updateTime": initial_time,
            "principalsEpoch": 3,
        })
    );
    assert_eq!(hub.seq(), 1);
    assert!(joined.next().now_or_never().is_none());
    let joining = hub.join().expect("another viewer joins");
    assert_eq!(
        data(joining.snapshot())["principalsEpoch"].as_u64(),
        Some(3)
    );
    assert_eq!(id(joining.snapshot()), Some(1));
}

#[test]
fn principals_change_without_viewers_updates_state_without_encoding_a_frame() {
    let hub = hub();
    let before = hub.state_json().expect("state encodes");

    let allocations = allocation_counter::measure(|| hub.principals_changed(3));
    assert_eq!(allocations.count_total, 0);
    assert_eq!(hub.seq(), 0);
    let after = hub.state_json().expect("state encodes");
    assert_ne!(before, after);
    assert_eq!(json(&after)["principalsEpoch"].as_u64(), Some(3));
}

#[tokio::test]
async fn shutdown_signal_turns_true() {
    let hub = hub();
    let mut signal = hub.shutdown_signal();
    assert!(!*signal.borrow());

    hub.shutdown();
    signal.changed().await.expect("hub is alive");
    assert!(*signal.borrow());
}

#[tokio::test]
async fn state_json_is_memoised_until_a_swap() {
    let hub = hub();
    let first = hub.state_json().expect("state encodes");
    let again = hub.state_json().expect("state encodes");
    assert_eq!(first.as_ptr(), again.as_ptr());

    set_completed(&hub, 1);
    let after = hub.state_json().expect("state encodes");
    assert_ne!(first, after);
    assert_eq!(json(&after)["completed"].as_u64(), Some(1));
}

#[tokio::test]
async fn joins_without_a_publish_reuse_the_snapshot() {
    let hub = hub();
    let mut first = None;
    let warm = allocation_counter::measure(|| {
        first = Some(hub.join().expect("first viewer joins"));
    });
    let mut second = None;
    let again = allocation_counter::measure(|| {
        second = Some(hub.join().expect("second viewer joins"));
    });

    assert!(
        again.count_total < warm.count_total,
        "a join without a publish reuses the snapshot, so it allocates less"
    );
    let first = first.expect("first viewer joined");
    let second = second.expect("second viewer joined");
    assert_eq!(first.snapshot(), second.snapshot());
}

#[tokio::test]
async fn a_publish_replaces_the_shared_snapshot() {
    let hub = hub();
    let before = hub.join().expect("viewer joins");

    set_completed(&hub, 1);
    let after = hub.join().expect("viewer joins after the publish");
    assert_ne!(before.snapshot(), after.snapshot());
    assert_eq!(data(after.snapshot())["completed"].as_u64(), Some(1));
}

#[tokio::test]
async fn a_publish_that_changes_nothing_keeps_the_memoized_snapshot() {
    let hub = hub();
    let before_json = hub.state_json().expect("state encodes");

    let mut first = None;
    let warm = allocation_counter::measure(|| {
        first = Some(hub.join().expect("first viewer joins"));
    });
    let viewer = first.expect("first viewer joined");

    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
    });
    assert_eq!(hub.seq(), 0);
    let after_json = hub.state_json().expect("state encodes");
    assert_eq!(before_json, after_json);

    let mut second = None;
    let again = allocation_counter::measure(|| {
        second = Some(hub.join().expect("second viewer joins"));
    });
    assert!(
        again.count_total < warm.count_total,
        "a publish that changes nothing keeps the snapshot, so the join allocates less"
    );
    let second = second.expect("second viewer joined");
    assert_eq!(viewer.snapshot(), second.snapshot());
}

#[tokio::test]
async fn a_publish_that_only_moves_the_update_time_refreshes_the_memoized_state() {
    let hub = hub();
    let before = hub.state_json().expect("state encodes");
    let _viewer = hub.join().expect("viewer joins");

    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.update_time = Timestamp::from_unix_millis(2_000_000);
    });

    let after = hub.state_json().expect("state encodes");
    assert_ne!(
        json(&after)["updateTime"],
        json(&before)["updateTime"],
        "the state a watched hub serves carries the latest update time"
    );
    let late = hub.join().expect("a later viewer joins");
    assert_eq!(
        data(late.snapshot())["updateTime"],
        json(&after)["updateTime"]
    );
}

#[tokio::test]
async fn resyncs_after_a_lag_reuse_the_snapshot_encoding() {
    let hub = StateHub::new(config(FOUR, ONE, EIGHT), initial());
    let mut a = hub.join().expect("first viewer joins");
    let mut b = hub.join().expect("second viewer joins");

    set_completed(&hub, 1);
    set_completed(&hub, 2);
    set_completed(&hub, 3);

    let Next::Resync(first) = a.next().await else {
        panic!("a lagging viewer is resynced, not closed");
    };
    let Next::Resync(second) = b.next().await else {
        panic!("a lagging viewer is resynced, not closed");
    };
    assert_eq!(first, second);

    let mut reused = None;
    let reuse = allocation_counter::measure(|| {
        reused = Some(hub.join().expect("viewer joins"));
    });
    let reused = reused.expect("viewer joined");
    assert_eq!(first, reused.snapshot());

    set_completed(&hub, 4);
    let mut encoded = None;
    let encode = allocation_counter::measure(|| {
        encoded = Some(hub.join().expect("viewer joins after the publish"));
    });
    assert!(
        reuse.count_total < encode.count_total,
        "a join after a resync reuses the snapshot, so it allocates less"
    );
    let encoded = encoded.expect("viewer joined");
    assert_eq!(data(encoded.snapshot())["completed"].as_u64(), Some(4));
}

#[tokio::test]
async fn the_viewer_cap_holds_across_a_resync() {
    let hub = StateHub::new(config(ONE, ONE, EIGHT), initial());
    let mut first = hub.join().expect("first viewer joins");

    set_completed(&hub, 1);
    set_completed(&hub, 2);
    let Next::Resync(_) = first.next().await else {
        panic!("a lagging viewer is resynced, not closed");
    };

    assert_eq!(hub.join().err(), Some(ViewerCap));
    drop(first);
    assert!(
        hub.join().is_ok(),
        "the slot is free after the resynced viewer drops"
    );
}
