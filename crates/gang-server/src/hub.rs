//! The live state of the fleet, shared by every viewer of the admin dashboard.
//!
//! The hub holds the latest [`State`] and its sequence number. Each publish builds the next state
//! into a scratch buffer, diffs it against the latest, and swaps it in. While viewers are
//! connected, every change is encoded once as a delta frame and broadcast to all of them. A
//! viewer joins with a snapshot taken under the same lock that publishes use, so its first frame
//! always applies to the snapshot's sequence number. The snapshot of the latest state is encoded
//! once and shared until the state changes, so joins and resyncs in between reuse one allocation.

use std::collections::VecDeque;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gang_state::{EncodeError, diff, sse};
use gang_wire::{Delta, Event, FrameRef, State};
use parking_lot::Mutex;
use tokio::sync::{broadcast, watch};

/// Sizing of the hub.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HubConfig {
    /// The most viewers that may be connected at once.
    pub(crate) max_viewers: NonZeroUsize,
    /// How long a refused or shut-down viewer waits before it reconnects.
    pub(crate) retry_after: Duration,
    /// How many frames a viewer may fall behind before it is resynced with a snapshot.
    pub(crate) lag_frames: NonZeroUsize,
    /// How many scaling events the hub keeps for the next state.
    pub(crate) recent_events: NonZeroUsize,
}

/// The shared live state. Clones share one hub.
#[derive(Clone)]
pub(crate) struct StateHub(Arc<Shared>);

/// One encoded delta frame and the sequence number it moves the state to.
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    /// The sequence number the delta moves the state to.
    pub(crate) seq: u64,
    /// The `delta` SSE frame.
    pub(crate) bytes: Bytes,
}

/// A join was refused because the hub already has its maximum number of viewers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the live state already has its maximum number of viewers")]
pub(crate) struct ViewerCap;

/// A viewer connected to the hub. Its receiver holds one viewer slot; dropping it frees its
/// viewer slot.
pub(crate) struct Joined {
    hub: StateHub,
    snapshot: Bytes,
    receiver: broadcast::Receiver<Frame>,
    seq: u64,
}

/// What a viewer receives next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Next {
    /// The next `delta` frame, in order.
    Frame(Bytes),
    /// A fresh `snapshot` frame that replaces everything the viewer holds.
    Resync(Bytes),
    /// The hub has no more frames for this viewer.
    Closed,
}

struct Shared {
    config: HubConfig,
    inner: Mutex<Inner>,
    frames: broadcast::Sender<Frame>,
    shutdown: watch::Sender<bool>,
}

struct Inner {
    latest: State,
    scratch: State,
    ring: VecDeque<Event>,
    memo: Option<Bytes>,
    snapshot: Option<Bytes>,
}

impl Inner {
    /// Drops the memoized frames. The next reader encodes them again from the latest state.
    fn invalidate(&mut self) {
        self.memo = None;
        self.snapshot = None;
    }

    /// The `GET /v1/state` body of the latest state, encoded once and shared until the state
    /// changes.
    fn state_json(&mut self) -> Result<Bytes, EncodeError> {
        if let Some(memo) = &self.memo {
            return Ok(memo.clone());
        }
        let bytes = sse::state_json(&self.latest)?;
        self.memo = Some(bytes.clone());
        Ok(bytes)
    }

    /// The `snapshot` frame of the latest state, encoded once and shared until the state changes.
    /// If the state cannot be serialized, the error is logged and the frame is empty; the
    /// viewer's next delta then resyncs.
    fn snapshot(&mut self) -> Bytes {
        if let Some(snapshot) = &self.snapshot {
            return snapshot.clone();
        }
        let snapshot = match self.state_json() {
            Ok(json) => sse::event(
                gang_wire::SNAPSHOT_EVENT,
                Some(self.latest.seq),
                None,
                &json,
            ),
            Err(error) => {
                tracing::error!(%error, "cannot encode a live state snapshot");
                Bytes::new()
            }
        };
        self.snapshot = Some(snapshot.clone());
        snapshot
    }
}

impl StateHub {
    /// A hub that serves `initial` until the first publish.
    #[must_use]
    pub(crate) fn new(config: HubConfig, initial: State) -> Self {
        let (frames, _) = broadcast::channel(config.lag_frames.get());
        let scratch = initial.clone();
        Self(Arc::new(Shared {
            config,
            inner: Mutex::new(Inner {
                latest: initial,
                scratch,
                ring: VecDeque::with_capacity(config.recent_events.get()),
                memo: None,
                snapshot: None,
            }),
            frames,
            shutdown: watch::channel(false).0,
        }))
    }

    /// Replaces the recent events with `events`, oldest first, keeping the newest
    /// `recent_events` of them. The latest state carries the same events. Intended for startup,
    /// before any viewer joins.
    pub(crate) fn preload(&self, events: Vec<Event>) {
        let mut guard = self.0.inner.lock();
        let inner = &mut *guard;
        let keep = self.0.config.recent_events.get();
        let skip = events.len().saturating_sub(keep);
        inner.ring.clear();
        inner.ring.extend(events.into_iter().skip(skip));
        inner.latest.events = inner.ring.iter().cloned().collect();
        inner.invalidate();
    }

    /// Appends `event` to the recent events. It reaches the viewers with the next publish.
    pub(crate) fn record_event(&self, event: Event) {
        let mut inner = self.0.inner.lock();
        inner.ring.push_back(event);
        while inner.ring.len() > self.0.config.recent_events.get() {
            inner.ring.pop_front();
        }
    }

    /// Builds the next state and publishes it. `build` receives the scratch state to fill, the
    /// latest state, the sequence number the new state takes when it changes, and the recent
    /// events, oldest first.
    ///
    /// The sequence number advances only when a delta is broadcast. A build that changes nothing
    /// sends no frame and keeps the sequence number.
    pub(crate) fn publish(&self, build: impl FnOnce(&mut State, &State, u64, &[Event])) {
        let mut guard = self.0.inner.lock();
        let inner = &mut *guard;
        let next = inner.latest.seq + 1;
        build(
            &mut inner.scratch,
            &inner.latest,
            next,
            inner.ring.make_contiguous(),
        );
        settle(inner, &self.0.frames);
    }

    /// Publishes a state whose only change is the principal epoch `epoch`. Does nothing when the
    /// latest state already has that epoch.
    pub(crate) fn principals_changed(&self, epoch: u64) {
        let mut guard = self.0.inner.lock();
        let inner = &mut *guard;
        if inner.latest.principals_epoch == epoch {
            return;
        }
        inner.latest.principals_epoch = epoch;
        inner.invalidate();
        if self.0.frames.receiver_count() == 0 {
            return;
        }
        let base = inner.latest.seq;
        inner.latest.seq = base + 1;
        let delta = Delta {
            seq: inner.latest.seq,
            base,
            update_time: inner.latest.update_time,
            pass: None,
            queue: None,
            fleets: None,
            workers: None,
            instances: None,
            actions: None,
            events: Vec::new(),
            completed: None,
            principals_epoch: Some(epoch),
        };
        match sse::encode(FrameRef::Delta(&delta)) {
            Ok(bytes) => {
                let _ = self.0.frames.send(Frame {
                    seq: delta.seq,
                    bytes,
                });
            }
            Err(error) => tracing::error!(%error, "cannot encode a live state delta"),
        }
    }

    /// The `GET /v1/state` body of the latest state. The same bytes are returned until the
    /// latest state changes.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] when the state cannot be serialized as JSON.
    pub(crate) fn state_json(&self) -> Result<Bytes, EncodeError> {
        self.0.inner.lock().state_json()
    }

    /// The sequence number of the latest state.
    #[must_use]
    pub(crate) fn seq(&self) -> u64 {
        self.0.inner.lock().latest.seq
    }

    /// Connects a viewer, which receives the latest state as its snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ViewerCap`] when the hub already has its maximum number of viewers.
    pub(crate) fn join(&self) -> Result<Joined, ViewerCap> {
        let mut inner = self.0.inner.lock();
        if self.0.frames.receiver_count() >= self.0.config.max_viewers.get() {
            return Err(ViewerCap);
        }
        let seq = inner.latest.seq;
        let snapshot = inner.snapshot();
        let receiver = self.0.frames.subscribe();
        Ok(Joined {
            hub: self.clone(),
            snapshot,
            receiver,
            seq,
        })
    }

    /// Signals every watcher of [`Self::shutdown_signal`] that the hub is shutting down.
    pub(crate) fn shutdown(&self) {
        self.0.shutdown.send_replace(true);
    }

    /// A receiver that turns `true` once [`Self::shutdown`] has been called.
    #[must_use]
    pub(crate) fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.0.shutdown.subscribe()
    }

    /// How long a refused or shut-down viewer waits before it reconnects.
    #[must_use]
    pub(crate) fn retry_after(&self) -> Duration {
        self.0.config.retry_after
    }
}

impl Joined {
    /// The `snapshot` frame of the state at join time.
    #[must_use]
    pub(crate) fn snapshot(&self) -> &Bytes {
        &self.snapshot
    }

    /// The latest sequence number published by the hub.
    pub(crate) fn hub_seq(&self) -> u64 {
        self.hub.seq()
    }

    /// Waits for the next frame. A viewer that fell behind the broadcast is resynced with a fresh
    /// snapshot rather than closed.
    pub(crate) async fn next(&mut self) -> Next {
        match self.receiver.recv().await {
            Ok(frame) if frame.seq == self.seq + 1 => {
                self.seq = frame.seq;
                Next::Frame(frame.bytes)
            }
            // A frame that does not follow the last one means a delta was lost: a broadcast
            // lagged, or a delta could not be encoded. Resync rather than apply it.
            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => self.resync(),
            Err(broadcast::error::RecvError::Closed) => Next::Closed,
        }
    }

    /// Replaces the receiver with one that starts after the latest state, and snapshots that
    /// state. Both happen under the hub lock, so no delta falls between them.
    fn resync(&mut self) -> Next {
        let mut inner = self.hub.0.inner.lock();
        self.receiver = self.hub.0.frames.subscribe();
        self.seq = inner.latest.seq;
        Next::Resync(inner.snapshot())
    }
}

/// Makes the scratch state the latest one and broadcasts the change to the viewers. The scratch
/// state must already hold the next sequence number, which the diff uses when it changed.
/// The memoized frames are dropped only when the state or its update time changed.
fn settle(inner: &mut Inner, frames: &broadcast::Sender<Frame>) {
    let watching = frames.receiver_count() > 0;
    let delta = if watching {
        diff(&inner.latest, &inner.scratch)
    } else {
        None
    };
    // `diff` ignores the update time, but the memoized frames carry it.
    let changed =
        !watching || delta.is_some() || inner.latest.update_time != inner.scratch.update_time;
    match delta {
        Some(delta) => match sse::encode(FrameRef::Delta(&delta)) {
            Ok(bytes) => {
                // Nobody may be listening, which is fine: the frame is only for live viewers.
                let _ = frames.send(Frame {
                    seq: delta.seq,
                    bytes,
                });
            }
            Err(error) => tracing::error!(%error, "cannot encode a live state delta"),
        },
        None => inner.scratch.seq = inner.latest.seq,
    }
    mem::swap(&mut inner.latest, &mut inner.scratch);
    if changed {
        inner.invalidate();
    }
}
