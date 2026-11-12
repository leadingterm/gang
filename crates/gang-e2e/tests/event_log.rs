//! `EventFollower` builds the ledger of scaling events from the state stream and the history list:
//! the snapshot seeds it, a full snapshot window is backfilled from the list, deltas extend it by
//! name, and a stream that cannot be followed is reopened or fails with what the server said.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::error::Error;
use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{self, Form};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use http_body_util::channel::Channel;
use parking_lot::Mutex;

use gang_clock::Timestamp;
use gang_e2e::{E2eError, EventFollower};
use gang_state::sse;
use gang_types::{FleetName, Url, error_chain};
use gang_wire::page::PageToken;
use gang_wire::{
    Cause, Closed, ClosedReason, Delta, Event, EventChange, Frame, ListEventsResponse, Pass,
    PassState, Queue, State, Tick, name,
};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};

type TestResult = Result<(), Box<dyn Error>>;

/// One scripted answer of the stub server.
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
    /// Whether the stub keeps the connection open after the body, as a live stream does.
    hold: bool,
}

/// The event `events/{seq}`: a scale-out of one fleet.
fn event(seq: u64) -> Result<Event, Box<dyn Error>> {
    let fleet: FleetName = "fleet-arm64".parse()?;
    Ok(Event {
        name: name::of::<name::Events>(&seq),
        create_time: Timestamp::from_unix_millis(seq),
        fleet: name::of::<name::Fleets>(&fleet),
        cause: Some(Cause::Demand),
        change: EventChange::ScaledOut { from: 0, to: 1 },
    })
}

fn events(seqs: &[u64]) -> Result<Vec<Event>, Box<dyn Error>> {
    seqs.iter().copied().map(event).collect()
}

/// A snapshot numbered `seq` that carries the events `held` out of a window of `limit`.
fn snapshot(seq: u64, held: &[u64], limit: u32) -> Result<Frame, Box<dyn Error>> {
    Ok(Frame::Snapshot(State {
        name: name::STATE.to_owned(),
        seq,
        update_time: Timestamp::from_unix_millis(seq),
        provisioner: "local".to_owned(),
        pass: Pass {
            state: PassState::Ok,
            interval: Duration::from_millis(500),
            applied: 0,
            failed: 0,
            skip_reason: None,
            last_ok_time: None,
            skipped: None,
        },
        queue: Queue {
            total: 0,
            unplaceable: 0,
            oldest_queue_time: None,
        },
        fleets: Vec::new(),
        workers: Vec::new(),
        instances: Vec::new(),
        actions: None,
        events: events(held)?,
        events_limit: limit,
        completed: 0,
        principals_epoch: 0,
    }))
}

/// A delta from `base` to `seq` that carries the events `carried`.
fn delta(base: u64, seq: u64, carried: &[u64]) -> Result<Frame, Box<dyn Error>> {
    Ok(Frame::Delta(Delta {
        seq,
        base,
        update_time: Timestamp::from_unix_millis(seq),
        pass: None,
        queue: None,
        fleets: None,
        workers: None,
        instances: None,
        actions: None,
        events: events(carried)?,
        completed: None,
        principals_epoch: None,
    }))
}

fn tick(seq: u64) -> Frame {
    Frame::Tick(Tick {
        seq,
        time: Timestamp::from_unix_millis(seq),
    })
}

fn closed(reason: ClosedReason) -> Frame {
    Frame::Closed(Closed {
        reason,
        retry_delay: None,
    })
}

/// The answer that sends `frames` on the stream and then ends it.
fn stream(frames: &[Frame]) -> Result<Reply, Box<dyn Error>> {
    let mut body = Vec::new();
    for frame in frames {
        body.extend_from_slice(&sse::encode(frame)?);
    }
    Ok(Reply {
        status: StatusCode::OK,
        body,
        hold: false,
    })
}

/// The answer to a list request: `seqs` newest first, and the token of the next page if any.
fn page(seqs: &[u64], next: Option<&PageToken>) -> Result<Reply, Box<dyn Error>> {
    let body = serde_json::to_vec(&ListEventsResponse {
        events: events(seqs)?,
        next_page_token: next.cloned(),
    })?;
    Ok(Reply {
        status: StatusCode::OK,
        body,
        hold: false,
    })
}

/// A stub server on an ephemeral loopback port. It answers each request with the next entry of
/// its script, whatever the request, and ends each answer by closing the connection, which is how
/// a stream ends. A request beyond the script gets a 500; a test scripts exactly the requests it
/// expects and compares them with [`received`].
struct Stub {
    script: Arc<Mutex<Script>>,
    _runtime: Runtime,
}

/// What the stub still has to answer, and what it has been asked so far.
struct Script {
    replies: VecDeque<Reply>,
    seen: Vec<Seen>,
}

/// A request the stub received.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Watch,
    List(ListQuery),
}

/// The query of a history list request.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    page_size: u32,
    page_token: Option<PageToken>,
}

/// Starts a stub server that answers with `replies`, in order, and returns its url.
fn serve(replies: Vec<Reply>) -> io::Result<(Url, Stub)> {
    let script = Arc::new(Mutex::new(Script {
        replies: replies.into(),
        seen: Vec::new(),
    }));
    let router = Router::new()
        .route("/v1/state:watch", get(watch))
        .route("/v1/events", get(list))
        .with_state(Arc::clone(&script));

    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let url =
        Url::parse(&format!("http://{}/", listener.local_addr()?)).map_err(io::Error::other)?;
    runtime.spawn(async move { axum::serve(listener, router).await });
    Ok((
        url,
        Stub {
            script,
            _runtime: runtime,
        },
    ))
}

async fn watch(extract::State(script): extract::State<Arc<Mutex<Script>>>) -> Response {
    answer(&script, Seen::Watch)
}

async fn list(
    extract::State(script): extract::State<Arc<Mutex<Script>>>,
    Form(query): Form<ListQuery>,
) -> Response {
    answer(&script, Seen::List(query))
}

/// Records `seen` and answers it with the next scripted reply.
fn answer(script: &Mutex<Script>, seen: Seen) -> Response {
    let reply = {
        let mut script = script.lock();
        script.seen.push(seen);
        script.replies.pop_front()
    };
    let Some(reply) = reply else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no reply is scripted for this request",
        )
            .into_response();
    };
    let body = if reply.hold {
        held(reply.body)
    } else {
        Body::from(reply.body)
    };
    (
        reply.status,
        [(CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

/// A body that sends `bytes` and then stays open until the stub stops.
fn held(bytes: Vec<u8>) -> Body {
    let (mut sender, channel) = Channel::<Bytes, Infallible>::new(1);
    tokio::spawn(async move {
        if sender.send_data(Bytes::from(bytes)).await.is_ok() {
            std::future::pending::<()>().await;
        }
    });
    Body::new(channel)
}

/// Takes the requests the stub has received since the last call, in order.
fn received(stub: &Stub) -> Vec<Seen> {
    let mut script = stub.script.lock();
    std::mem::take(&mut script.seen)
}

fn names(held: &[Event]) -> Vec<&str> {
    held.iter().map(|event| event.name.as_str()).collect()
}

/// The list request of the first history page.
const LIST: Seen = Seen::List(ListQuery {
    page_size: 1000,
    page_token: None,
});
const WATCH: Seen = Seen::Watch;

/// The list request that resumes after `token`.
fn list_after(token: &PageToken) -> Seen {
    Seen::List(ListQuery {
        page_size: 1000,
        page_token: Some(token.clone()),
    })
}

/// A deadline that no test reaches: the stub answers at once.
const WAIT: Duration = Duration::from_mins(3);

#[test]
fn silent_server_times_out_before_stream_headers() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = Url::parse(&format!("https://{}/", listener.local_addr()?))?;
    let (release, wait) = mpsc::channel::<()>();
    let server = std::thread::spawn(move || -> io::Result<()> {
        let (_socket, _) = listener.accept()?;
        let _ = wait.recv();
        Ok(())
    });
    let timeout = Duration::from_millis(1);
    let result = EventFollower::open_with(&url, timeout);
    drop(release);
    server.join().map_err(|_| "server panicked")??;
    assert!(matches!(result, Err(E2eError::Timeout { waited, .. }) if waited == timeout));
    Ok(())
}

#[test]
fn a_snapshot_that_is_not_full_is_the_whole_ledger() -> TestResult {
    let (url, server) = serve(vec![stream(&[snapshot(3, &[1, 2, 3], 100)?])?])?;

    let follower = EventFollower::open_with(&url, WAIT)?;

    assert_eq!(
        names(follower.events()),
        ["events/1", "events/2", "events/3"]
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_full_window_is_backfilled_from_the_history() -> TestResult {
    // The ledger holds 1 to 4, and the window of two shows 3 and 4. The list's only page has no
    // next token, so the backfill ends there, having added the events 1 and 2.
    let (url, server) = serve(vec![
        stream(&[snapshot(4, &[3, 4], 2)?])?,
        page(&[4, 3, 2, 1], None)?,
    ])?;

    let follower = EventFollower::open_with(&url, WAIT)?;

    assert_eq!(
        names(follower.events()),
        ["events/1", "events/2", "events/3", "events/4"]
    );
    assert_eq!(received(&server), [WATCH, LIST]);
    Ok(())
}

#[test]
fn history_pages_are_followed_past_held_events_on_first_open() -> TestResult {
    // Events 5 and 6 were recorded after the snapshot. With no event held before the first
    // snapshot there is no floor, so the walk does not stop at the page of the held events 4 and
    // 3: it follows every token until a page has none.
    let first = PageToken::new("", "5");
    let second = PageToken::new("", "3");
    let (url, server) = serve(vec![
        stream(&[snapshot(4, &[3, 4], 2)?])?,
        page(&[6, 5], Some(&first))?,
        page(&[4, 3], Some(&second))?,
        page(&[2, 1], None)?,
    ])?;

    let follower = EventFollower::open_with(&url, WAIT)?;

    assert_eq!(
        names(follower.events()),
        [
            "events/1", "events/2", "events/3", "events/4", "events/5", "events/6"
        ]
    );
    assert_eq!(
        received(&server),
        [WATCH, LIST, list_after(&first), list_after(&second)]
    );
    Ok(())
}

#[test]
fn first_open_follows_every_page_of_a_long_history() -> TestResult {
    // The window of five shows 6 to 10. No event is held before the first snapshot, so there is
    // no floor: all three pages are followed, until the last one has no next token.
    let first = PageToken::new("", "6");
    let second = PageToken::new("", "3");
    let (url, server) = serve(vec![
        stream(&[snapshot(10, &[6, 7, 8, 9, 10], 5)?])?,
        page(&[10, 9, 8, 7, 6], Some(&first))?,
        page(&[5, 4, 3], Some(&second))?,
        page(&[2, 1], None)?,
    ])?;

    let follower = EventFollower::open_with(&url, WAIT)?;

    assert_eq!(
        names(follower.events()),
        [
            "events/1",
            "events/2",
            "events/3",
            "events/4",
            "events/5",
            "events/6",
            "events/7",
            "events/8",
            "events/9",
            "events/10"
        ]
    );
    assert_eq!(
        received(&server),
        [WATCH, LIST, list_after(&first), list_after(&second)]
    );
    Ok(())
}

#[test]
fn the_pages_end_the_backfill_when_the_history_is_exhausted() -> TestResult {
    let (url, server) = serve(vec![
        stream(&[snapshot(4, &[3, 4], 2)?])?,
        page(&[2, 1], None)?,
    ])?;

    let follower = EventFollower::open_with(&url, WAIT)?;

    assert_eq!(
        names(follower.events()),
        ["events/1", "events/2", "events/3", "events/4"]
    );
    assert_eq!(received(&server), [WATCH, LIST]);
    Ok(())
}

#[test]
fn deltas_extend_the_events_once_per_name_and_ticks_change_nothing() -> TestResult {
    let (url, server) = serve(vec![stream(&[
        snapshot(1, &[1], 100)?,
        delta(1, 2, &[2, 1])?,
        tick(2),
        delta(2, 3, &[3])?,
    ])?])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 3, WAIT)?;

    assert_eq!(names(held), ["events/1", "events/2", "events/3"]);
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn the_condition_is_judged_on_the_events_held_before_any_frame_is_read() -> TestResult {
    let (url, server) = serve(vec![stream(&[snapshot(1, &[1], 100)?])?])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 1, WAIT)?;

    assert_eq!(names(held), ["events/1"]);
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_later_snapshot_is_merged_and_the_history_behind_it_backfilled() -> TestResult {
    // The second snapshot is the hub's resync: its window of two shows 3 and 4. The newest event
    // held before it is 1, the floor, so the list is followed only until a page reaches 1.
    let (url, server) = serve(vec![
        stream(&[snapshot(1, &[1], 2)?, snapshot(4, &[3, 4], 2)?])?,
        page(&[4, 3, 2, 1], None)?,
    ])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 4, WAIT)?;

    assert_eq!(
        names(held),
        ["events/1", "events/2", "events/3", "events/4"]
    );
    assert_eq!(received(&server), [WATCH, LIST]);
    Ok(())
}

#[test]
fn a_delta_that_does_not_follow_the_last_one_reopens_the_stream() -> TestResult {
    // The delta from 5 does not follow the snapshot at 1, so its events are not applied: the
    // new stream's snapshot is the state to merge.
    let (url, server) = serve(vec![
        stream(&[snapshot(1, &[1], 100)?, delta(5, 6, &[9])?])?,
        stream(&[snapshot(7, &[1, 2], 100)?])?,
    ])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 2, WAIT)?;

    assert_eq!(names(held), ["events/1", "events/2"]);
    assert_eq!(received(&server), [WATCH, WATCH]);
    Ok(())
}

#[test]
fn a_reopened_stream_stops_at_the_newest_event_held_before_it() -> TestResult {
    // The first snapshot is full, so its history is followed to the end. The delta from 5 does
    // not follow the snapshot at 3, so the stream reopens. The newest event held before the new
    // snapshot is 3, the floor: the walk stops at the page that reaches it and requests no more.
    let first = PageToken::new("", "4");
    let last = PageToken::new("", "1");
    let (url, server) = serve(vec![
        stream(&[snapshot(3, &[1, 2, 3], 3)?, delta(5, 6, &[9])?])?,
        page(&[3, 2, 1], None)?,
        stream(&[snapshot(8, &[6, 7, 8], 3)?])?,
        page(&[8, 7, 6, 5, 4], Some(&first))?,
        page(&[3, 2, 1], Some(&last))?,
    ])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 8, WAIT)?;

    assert_eq!(
        names(held),
        [
            "events/1", "events/2", "events/3", "events/4", "events/5", "events/6", "events/7",
            "events/8"
        ]
    );
    assert_eq!(
        received(&server),
        [WATCH, LIST, WATCH, LIST, list_after(&first)]
    );
    Ok(())
}

#[test]
fn a_closed_frame_fails_with_its_reason() -> TestResult {
    let (url, server) = serve(vec![stream(&[
        snapshot(1, &[1], 100)?,
        closed(ClosedReason::ViewerCap),
    ])?])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let error = follower
        .wait_for(|_| false, WAIT)
        .expect_err("a closed stream ends the wait");
    assert!(
        error_chain(&error).to_string().contains("VIEWER_CAP"),
        "{}",
        error_chain(&error)
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_stream_that_ends_fails_the_wait() -> TestResult {
    let (url, server) = serve(vec![stream(&[snapshot(1, &[1], 100)?])?])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let error = follower
        .wait_for(|_| false, WAIT)
        .expect_err("an ended stream ends the wait");
    assert!(
        error_chain(&error).to_string().contains("ended"),
        "{}",
        error_chain(&error)
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_stream_that_stays_silent_ends_the_wait_at_its_deadline() -> TestResult {
    // The stub keeps the stream open and sends nothing more, so only the deadline can end the
    // wait. A deadline that has already passed is enough: it is the unit under test.
    let (url, server) = serve(vec![Reply {
        hold: true,
        ..stream(&[snapshot(1, &[1], 100)?])?
    }])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let error = follower
        .wait_for(|held| held.len() == 2, Duration::ZERO)
        .expect_err("no event arrives before the deadline");

    assert!(
        matches!(error, E2eError::Timeout { waited, .. } if waited == Duration::ZERO),
        "{error:?}"
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn the_deadline_does_not_end_a_wait_that_is_already_satisfied() -> TestResult {
    let (url, server) = serve(vec![Reply {
        hold: true,
        ..stream(&[snapshot(1, &[1], 100)?])?
    }])?;

    let mut follower = EventFollower::open_with(&url, WAIT)?;
    let held = follower.wait_for(|held| held.len() == 1, Duration::ZERO)?;

    assert_eq!(names(held), ["events/1"]);
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_first_frame_that_is_not_a_snapshot_is_an_error() -> TestResult {
    let (url, server) = serve(vec![stream(&[tick(1)])?])?;

    let error = EventFollower::open_with(&url, WAIT)
        .err()
        .ok_or("a stream must start with a snapshot")?;
    assert!(
        error_chain(&error).to_string().contains("`tick`"),
        "{}",
        error_chain(&error)
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_stream_that_is_closed_before_its_snapshot_names_the_reason() -> TestResult {
    let (url, server) = serve(vec![stream(&[closed(ClosedReason::Shutdown)])?])?;

    let error = EventFollower::open_with(&url, WAIT)
        .err()
        .ok_or("a stream must start with a snapshot")?;

    assert!(
        error_chain(&error).to_string().contains("SHUTDOWN"),
        "{}",
        error_chain(&error)
    );
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_refused_stream_carries_the_error_envelope() -> TestResult {
    let envelope = r#"{"error":{"code":401,"message":"Credentials are missing.","status":"UNAUTHENTICATED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"CREDENTIALS_MISSING","domain":"gang"}]}}"#;
    let (url, server) = serve(vec![Reply {
        status: StatusCode::UNAUTHORIZED,
        body: envelope.as_bytes().to_vec(),
        hold: false,
    }])?;

    let error = EventFollower::open_with(&url, WAIT)
        .err()
        .ok_or("a 401 answer is not a stream")?;
    let message = error_chain(&error).to_string();

    assert!(message.contains("401"), "{message}");
    assert!(message.contains("CREDENTIALS_MISSING"), "{message}");
    assert_eq!(received(&server), [WATCH]);
    Ok(())
}

#[test]
fn a_refused_history_list_names_the_status_and_body() -> TestResult {
    let (url, server) = serve(vec![
        stream(&[snapshot(2, &[1, 2], 2)?])?,
        Reply {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"events unavailable".to_vec(),
            hold: false,
        },
    ])?;

    let error = EventFollower::open_with(&url, WAIT)
        .err()
        .ok_or("a 503 answer is not a history")?;
    let message = error_chain(&error).to_string();

    assert!(message.contains("503"), "{message}");
    assert!(message.contains("events unavailable"), "{message}");
    assert_eq!(received(&server), [WATCH, LIST]);
    Ok(())
}
