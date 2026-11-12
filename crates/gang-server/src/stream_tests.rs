use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::hub::{HubConfig, StateHub};
use crate::principals::RevokedPrincipals;
use crate::stream::{StreamParams, open_stream};
use futures::FutureExt;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_spi::ApiResponseBody;
use gang_types::{Identity, NonZeroDuration};
use gang_wire::{Pass, State, Tick, name};
use http_body_util::BodyExt;

use crate::test_support::{decode_sse, identity, start_clock};

const ONE: NonZeroUsize = NonZeroUsize::MIN;
const TWO: NonZeroUsize = NonZeroUsize::new(2).expect("two is non-zero");
const FOUR: NonZeroUsize = NonZeroUsize::new(4).expect("four is non-zero");
const EIGHT: NonZeroUsize = NonZeroUsize::new(8).expect("eight is non-zero");

/// The heartbeat interval that keeps ticks out of tests that do not look for them.
const QUIET: NonZeroDuration = NonZeroDuration::from_hours(1);

fn hub(max_viewers: NonZeroUsize, lag_frames: NonZeroUsize) -> StateHub {
    StateHub::new(
        HubConfig {
            max_viewers,
            retry_after: Duration::from_secs(5),
            lag_frames,
            recent_events: EIGHT,
        },
        initial(),
    )
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

/// Publishes a state whose only change is `completed`.
fn set_completed(hub: &StateHub, completed: u64) {
    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.completed = completed;
    });
}

/// Opens a stream for `who` and returns its body.
fn open(
    hub: &StateHub,
    clock: &Arc<TestClock>,
    heartbeat: NonZeroDuration,
    valid_until: Option<Timestamp>,
    revoked_principals: &RevokedPrincipals,
    who: &Identity,
) -> ApiResponseBody {
    let params = StreamParams {
        hub: hub.clone(),
        clock: clock.clone(),
        heartbeat,
        identity: Arc::new(who.clone()),
        valid_until,
        revoked_principals: revoked_principals.clone(),
    };
    open_stream(params).into_body()
}

/// The next SSE frame of `body`, or `None` once the stream has ended.
async fn frame_text(body: &mut ApiResponseBody) -> Option<String> {
    let frame = match body.frame().await {
        None => return None,
        Some(Ok(frame)) => frame,
        Some(Err(error)) => panic!("stream body failed: {error}"),
    };
    let Ok(bytes) = frame.into_data() else {
        panic!("stream frames are data");
    };
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// The event name of an SSE frame.
fn event(text: &str) -> &'static str {
    decode_sse(text.as_bytes()).event_name()
}

/// The JSON payload of an SSE frame.
fn payload(text: &str) -> serde_json::Value {
    match decode_sse(text.as_bytes()) {
        gang_wire::Frame::Snapshot(value) => serde_json::to_value(value),
        gang_wire::Frame::Delta(value) => serde_json::to_value(value),
        gang_wire::Frame::Tick(value) => serde_json::to_value(value),
        gang_wire::Frame::Closed(value) => serde_json::to_value(value),
    }
    .expect("frame payload serializes")
}

#[tokio::test]
async fn snapshot_comes_first_with_the_state_sequence_as_its_id() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        None,
        &revoked_principals,
        &identity("ada@example.com"),
    );

    let first = frame_text(&mut body).await.expect("snapshot is sent");
    assert_eq!(event(&first), "snapshot");
    assert!(
        first.contains("\nid: 0\n"),
        "snapshot carries the seq: {first}"
    );
    assert_eq!(payload(&first)["seq"], 0);
}

#[tokio::test]
async fn heartbeat_reports_the_current_seq_and_does_not_advance_it() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let heartbeat = NonZeroDuration::from_secs(10);
    let mut body = open(
        &hub,
        &clock,
        heartbeat,
        None,
        &revoked_principals,
        &identity("ada@example.com"),
    );
    frame_text(&mut body).await.expect("snapshot is sent");

    let mut next = Box::pin(frame_text(&mut body));
    clock.advance(heartbeat.get() - Duration::from_millis(1));
    assert!(
        next.as_mut().now_or_never().is_none(),
        "no heartbeat before the interval"
    );
    clock.advance(Duration::from_millis(1));
    let tick = next
        .await
        .expect("a heartbeat arrives one interval after the stream opened");

    assert_eq!(event(&tick), "tick");
    let tick: Tick = serde_json::from_value(payload(&tick)).expect("the tick payload decodes");
    assert_eq!(tick.seq, 0);
    assert_eq!(tick.time, clock.wall(), "tick carries the server clock");
    assert_eq!(hub.seq(), 0, "a heartbeat never advances the sequence");

    set_completed(&hub, 1);
    let delta = frame_text(&mut body).await.expect("delta is sent");
    assert_eq!(event(&delta), "delta");
    clock.advance(heartbeat.get());
    let tick = frame_text(&mut body)
        .await
        .expect("the next heartbeat arrives");
    let tick: Tick = serde_json::from_value(payload(&tick)).expect("the tick payload decodes");
    assert_eq!(tick.seq, 1);
    assert_eq!(hub.seq(), 1, "a heartbeat never advances the sequence");
}

#[tokio::test]
async fn deltas_are_relayed_in_order() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        None,
        &revoked_principals,
        &identity("ada@example.com"),
    );
    frame_text(&mut body).await.expect("snapshot is sent");

    set_completed(&hub, 1);
    set_completed(&hub, 2);

    let first = frame_text(&mut body).await.expect("first delta is sent");
    let second = frame_text(&mut body).await.expect("second delta is sent");
    assert_eq!(event(&first), "delta");
    assert_eq!(payload(&first)["seq"], 1);
    assert_eq!(payload(&first)["base"], 0);
    assert_eq!(event(&second), "delta");
    assert_eq!(payload(&second)["seq"], 2);
    assert_eq!(payload(&second)["base"], 1);
}

#[tokio::test]
async fn revocation_closes_the_stream_at_once_and_not_before() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let ada = identity("ada@example.com");
    let mut body = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    frame_text(&mut body).await.expect("snapshot is sent");

    let mut next = Box::pin(frame_text(&mut body));
    clock.advance(Duration::from_secs(30));
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "a live principal's stream stays open"
    );
    revoked_principals.revoke(ada.clone());
    let closed = next.await.expect("revocation closes the stream");

    assert_eq!(event(&closed), "closed");
    assert_eq!(payload(&closed)["reason"], "REVOKED");
    assert!(
        payload(&closed).get("retryDelay").is_none() && !closed.contains("retry:"),
        "revocation sends no retry hint"
    );
    assert!(frame_text(&mut body).await.is_none(), "the stream ends");
}

#[tokio::test]
async fn session_expiry_closes_the_stream_exactly_at_valid_until() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let valid_until = clock.wall() + Duration::from_secs(90);
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        Some(valid_until),
        &revoked_principals,
        &identity("ada@example.com"),
    );
    frame_text(&mut body).await.expect("snapshot is sent");

    let mut next = Box::pin(frame_text(&mut body));
    clock.advance(Duration::from_secs(89));
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "the stream stays open before the session expires"
    );
    clock.advance(Duration::from_secs(1));
    let closed = next.await.expect("expiry closes the stream");

    assert_eq!(event(&closed), "closed");
    assert_eq!(payload(&closed)["reason"], "EXPIRED");
}

#[tokio::test]
async fn an_expired_session_closes_at_once() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let past = Timestamp::from_unix_millis(1);
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        Some(past),
        &revoked_principals,
        &identity("ada@example.com"),
    );

    let snapshot = frame_text(&mut body).await.expect("snapshot is sent");
    assert_eq!(event(&snapshot), "snapshot");
    let closed = frame_text(&mut body)
        .await
        .expect("expiry closes the stream");
    assert_eq!(payload(&closed)["reason"], "EXPIRED");
    assert!(payload(&closed).get("retryDelay").is_none());
}

#[tokio::test]
async fn a_refused_viewer_gets_only_the_viewer_cap_frame() {
    let hub = hub(ONE, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let ada = identity("ada@example.com");
    let mut first = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    frame_text(&mut first)
        .await
        .expect("first viewer is served");

    let mut second = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    let refused = frame_text(&mut second).await.expect("refusal is sent");

    assert_eq!(event(&refused), "closed");
    assert_eq!(payload(&refused)["reason"], "VIEWER_CAP");
    assert!(
        refused.starts_with("retry: 5000\n"),
        "retry hint: {refused}"
    );
    assert_eq!(payload(&refused)["retryDelay"], "5s");
    assert!(
        frame_text(&mut second).await.is_none(),
        "the refusal ends the stream"
    );
}

#[tokio::test]
async fn shutdown_closes_the_stream_with_a_retry_hint() {
    let hub = hub(FOUR, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        None,
        &revoked_principals,
        &identity("ada@example.com"),
    );
    frame_text(&mut body).await.expect("snapshot is sent");

    hub.shutdown();
    let closed = frame_text(&mut body)
        .await
        .expect("shutdown closes the stream");

    assert_eq!(event(&closed), "closed");
    assert_eq!(payload(&closed)["reason"], "SHUTDOWN");
    assert_eq!(payload(&closed)["retryDelay"], "5s");
    assert!(closed.starts_with("retry: 5000\n"), "retry hint: {closed}");
    assert!(frame_text(&mut body).await.is_none(), "the stream ends");
}

#[tokio::test]
async fn a_lagging_viewer_is_resynced_not_closed() {
    let hub = hub(FOUR, TWO);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let mut body = open(
        &hub,
        &clock,
        QUIET,
        None,
        &revoked_principals,
        &identity("ada@example.com"),
    );
    frame_text(&mut body).await.expect("snapshot is sent");

    for completed in 1..=5 {
        set_completed(&hub, completed);
    }

    let resync = frame_text(&mut body).await.expect("a resync is sent");
    assert_eq!(event(&resync), "snapshot");
    assert_eq!(payload(&resync)["seq"], 5);

    set_completed(&hub, 6);
    let delta = frame_text(&mut body).await.expect("deltas continue");
    assert_eq!(event(&delta), "delta");
    assert_eq!(payload(&delta)["seq"], 6);
}

#[tokio::test]
async fn dropping_the_body_frees_the_viewer_slot() {
    let hub = hub(ONE, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let ada = identity("ada@example.com");
    let mut body = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    frame_text(&mut body).await.expect("snapshot is sent");

    drop(body);

    let mut next = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    let snapshot = frame_text(&mut next).await.expect("the slot is free");
    assert_eq!(event(&snapshot), "snapshot");
}

#[tokio::test]
async fn closing_frame_releases_the_viewer_slot_before_body_is_dropped() {
    let hub = hub(ONE, EIGHT);
    let clock = start_clock();
    let revoked_principals = RevokedPrincipals::new();
    let ada = identity("ada@example.com");
    let grace = identity("grace@example.com");
    let mut body = open(&hub, &clock, QUIET, None, &revoked_principals, &ada);
    frame_text(&mut body).await.expect("snapshot is sent");

    revoked_principals.revoke(ada);
    let closed = frame_text(&mut body)
        .await
        .expect("revocation closes the stream");
    assert_eq!(payload(&closed)["reason"], "REVOKED");

    let mut next = open(&hub, &clock, QUIET, None, &revoked_principals, &grace);
    let snapshot = frame_text(&mut next).await.expect("the slot is free");
    assert_eq!(event(&snapshot), "snapshot");
    assert!(frame_text(&mut body).await.is_none());
}
