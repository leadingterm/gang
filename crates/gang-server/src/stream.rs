//! The live state stream: one pull-driven `text/event-stream` response per viewer. The viewer
//! receives the snapshot first, then deltas, heartbeats while the state is quiet, and a `closed`
//! frame when the server ends the stream on purpose. Dropping the body releases the viewer slot.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use gang_clock::{Clock, Interval, Timestamp};
use gang_spi::{ApiResponseBody, BoxedError};
use gang_state::sse;
use gang_types::{Identity, NonZeroDuration};
use gang_wire::{Closed, ClosedReason, Frame, Tick};
use http::header::{CONTENT_TYPE, HeaderValue};
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::watch;

use crate::hub::{Joined, Next, StateHub};
use crate::principals::{Revoked, RevokedPrincipals};

/// What one live stream needs to serve its viewer.
pub(crate) struct StreamParams<C> {
    /// The hub that publishes the state.
    pub(crate) hub: StateHub,
    /// The clock that stamps heartbeats and measures the session's lifetime.
    pub(crate) clock: C,
    /// The interval between heartbeats while the state is quiet.
    pub(crate) heartbeat: NonZeroDuration,
    /// The principal the stream was opened for.
    pub(crate) identity: Arc<Identity>,
    /// When the viewer's session expires, if it has an expiry.
    pub(crate) valid_until: Option<Timestamp>,
    /// The revoked principals; revoking `identity` ends the stream.
    pub(crate) revoked_principals: RevokedPrincipals,
}

/// Opens the live state stream for one viewer.
///
/// Frames are produced as the response body is polled. A viewer beyond the hub's cap receives
/// only a `closed` frame with the retry hint.
pub(crate) fn open_stream<C: Clock + Send + Sync + 'static>(
    params: StreamParams<C>,
) -> http::Response<ApiResponseBody> {
    let StreamParams {
        hub,
        clock,
        heartbeat,
        identity,
        valid_until,
        revoked_principals,
    } = params;
    let body = if let Ok(joined) = hub.join() {
        let clock = Arc::new(clock);
        let first_beat = clock.monotonic() + heartbeat.get();
        let ending = Ending {
            expires_at: valid_until
                .map(|until| clock.monotonic() + until.saturating_duration_since(clock.wall())),
            shutdown: hub.shutdown_signal(),
            retry: hub.retry_after(),
            identity,
            revoked: revoked_principals.subscribe(),
        };
        let viewer = Viewer {
            joined: Some(joined),
            ending,
            beats: Interval::new(clock.clone(), first_beat, heartbeat),
            clock,
            snapshot_sent: false,
        };
        let frames = stream::unfold(viewer, |mut viewer| async move {
            let joined = viewer.joined.as_mut()?;
            let (bytes, ending) = if viewer.snapshot_sent {
                tokio::select! {
                    biased;
                    last = viewer.ending.closed(&viewer.clock) => (last, true),
                    next = joined.next() => match next {
                        Next::Frame(bytes) | Next::Resync(bytes) => (bytes, false),
                        Next::Closed => return None,
                    },
                    () = viewer.beats.tick() => {
                        (tick_frame(joined.hub_seq(), viewer.clock.wall()), false)
                    }
                }
            } else {
                viewer.snapshot_sent = true;
                (joined.snapshot().clone(), false)
            };
            if ending {
                viewer.joined = None;
            }
            Some((Ok::<_, BoxedError>(http_body::Frame::data(bytes)), viewer))
        });
        StreamBody::new(frames).boxed_unsync()
    } else {
        gang_spi::full_body(closed_frame(
            ClosedReason::ViewerCap,
            Some(hub.retry_after()),
        ))
    };
    let mut response = http::Response::new(body);
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    crate::responses::no_store(headers);
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

/// The `closed` frame that tells the viewer why the stream ended and, when it may come back, how
/// long to wait.
fn closed_frame(reason: ClosedReason, retry_delay: Option<Duration>) -> Bytes {
    encoded(&Frame::Closed(Closed {
        reason,
        retry_delay,
    }))
}

/// The heartbeat `tick` frame for sequence number `seq` at wall time `time`.
fn tick_frame(seq: u64, time: Timestamp) -> Bytes {
    encoded(&Frame::Tick(Tick { seq, time }))
}

/// Encodes `frame`. A frame with a fixed shape cannot fail to encode; if it ever does, the error
/// is logged and the viewer receives an empty frame.
fn encoded(frame: &Frame) -> Bytes {
    sse::encode(frame).unwrap_or_else(|error| {
        tracing::error!(%error, "cannot encode a live state frame");
        Bytes::new()
    })
}

/// The reasons the server ends one viewer's stream on its own.
struct Ending {
    identity: Arc<Identity>,
    /// The live revoked principals; revoking `identity` ends the stream.
    revoked: watch::Receiver<Revoked>,
    expires_at: Option<gang_clock::Tick>,
    shutdown: watch::Receiver<bool>,
    retry: Duration,
}

impl Ending {
    /// Resolves with the `closed` frame once the stream must end. Cancel-safe: it holds no state
    /// across an await point, so a select may drop it at any time.
    async fn closed(&mut self, clock: &impl Clock) -> Bytes {
        loop {
            if self.revoked.borrow_and_update().contains(&self.identity) {
                return closed_frame(ClosedReason::Revoked, None);
            }
            tokio::select! {
                biased;
                () = stopping(&mut self.shutdown) => {
                    return closed_frame(ClosedReason::Shutdown, Some(self.retry));
                }
                () = expire(clock, self.expires_at) => {
                    return closed_frame(ClosedReason::Expired, None);
                }
                changed = self.revoked.changed() => {
                    if changed.is_err() {
                        // Every sender is gone, so the set can no longer change; wait for the
                        // other reasons instead of spinning on a closed channel.
                        pending::<()>().await;
                    }
                }
            }
        }
    }
}

/// Resolves once the server has begun shutting down.
async fn stopping(signal: &mut watch::Receiver<bool>) {
    let stopped = signal.wait_for(|stopping| *stopping).await.is_ok();
    if !stopped {
        pending::<()>().await;
    }
}

/// Resolves when the session expires on `clock`; never when it has no expiry.
async fn expire(clock: &impl Clock, at: Option<gang_clock::Tick>) {
    match at {
        Some(at) => clock.sleep_until(at).await,
        None => pending::<()>().await,
    }
}

/// State retained by a response body while its viewer is connected.
struct Viewer<C> {
    joined: Option<Joined>,
    ending: Ending,
    beats: Interval<C>,
    clock: C,
    snapshot_sent: bool,
}
