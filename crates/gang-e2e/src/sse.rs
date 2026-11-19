//! The stream of the live state as a scenario pulls it: frames, one at a time, from an open
//! response of the state stream route.

use std::time::{Duration, Instant};

use gang_wire::{Frame, State};

use crate::errors::{E2eError, Result};

/// The HTTP status of a state stream that carries frames.
pub(crate) const STREAM_OK: u16 = 200;

/// An open response of the state stream route: how the server answered, and the frames it sends.
pub trait EventStream {
    /// The HTTP status of the response. Only a `200` carries frames.
    fn status(&self) -> u16;

    /// The `Content-Type` of the response, when it has one.
    fn content_type(&self) -> Option<&str>;

    /// Waits for the next frame. `None` is the end of the stream, which is also what a response
    /// that is not a `200` yields at once.
    ///
    /// # Errors
    ///
    /// When the connection fails or a frame cannot be decoded.
    fn next_event(&mut self) -> Result<Option<Frame>>;

    /// Waits at most `budget` for the next frame. `None` is the end of the stream, as for
    /// [`next_event`](Self::next_event).
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when no frame arrives within `budget`; otherwise as
    /// [`next_event`](Self::next_event).
    fn next_event_within(&mut self, budget: Duration) -> Result<Option<Frame>>;
}

/// Reads frames from `stream` until `done` holds for the frames read so far, and returns them,
/// oldest first. `done` is evaluated after each frame read, never on no frames.
///
/// The wait is bounded by one deadline, `timeout` after the call: heartbeats and other frames do
/// not extend it.
///
/// # Errors
///
/// [`E2eError::Timeout`] when `done` still rejects the frames once `timeout` has passed;
/// [`E2eError::System`] when the stream ends before `done` holds; otherwise the errors of
/// [`EventStream::next_event_within`].
pub fn await_frames(
    stream: &mut impl EventStream,
    timeout: Duration,
    done: &mut dyn FnMut(&[Frame]) -> bool,
) -> Result<Vec<Frame>> {
    let deadline = Instant::now() + timeout;
    let mut frames = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(timed_out(timeout));
        }
        match stream.next_event_within(remaining) {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => {
                return Err(E2eError::system(
                    "the state stream ended before the awaited condition held",
                ));
            }
            Err(E2eError::Timeout { .. }) => return Err(timed_out(timeout)),
            Err(error) => return Err(error),
        }
        if done(&frames) {
            return Ok(frames);
        }
    }
}

/// Waits for a streamed state satisfying `done`, applying each delta to the retained snapshot.
/// Ticks recheck the retained state, and no frame extends the original deadline.
pub(crate) fn await_state(
    stream: &mut impl EventStream,
    timeout: Duration,
    done: &mut dyn FnMut(&State) -> bool,
) -> Result<State> {
    let deadline = Instant::now() + timeout;
    let mut state = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(timed_out(timeout));
        }
        let frame = match stream.next_event_within(remaining) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(E2eError::system(
                    "the state stream ended before the awaited condition held",
                ));
            }
            Err(E2eError::Timeout { .. }) => return Err(timed_out(timeout)),
            Err(error) => return Err(error),
        };
        match frame {
            Frame::Snapshot(snapshot) => state = Some(snapshot),
            Frame::Delta(delta) => {
                let current = state.as_mut().ok_or_else(|| {
                    E2eError::system("the state stream delivered a delta before a snapshot")
                })?;
                gang_state::apply(current, &delta).map_err(E2eError::system_from(
                    "applying a delta of the state stream",
                ))?;
            }
            Frame::Tick(_) => {}
            Frame::Closed(closed) => {
                return Err(E2eError::system(format!(
                    "the state stream closed before the awaited condition held: {:?}",
                    closed.reason
                )));
            }
        }
        if state.as_ref().is_some_and(&mut *done) {
            return state.ok_or_else(|| {
                E2eError::system("the state stream ended without a matching state")
            });
        }
    }
}

/// The timeout of an awaited condition of the state stream that `waited` long.
fn timed_out(waited: Duration) -> E2eError {
    E2eError::timeout("the state stream to satisfy the awaited condition", waited)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_wire::{Closed, ClosedReason, Delta, Frame, State, Tick};

    use super::{EventStream, await_frames, await_state};
    use crate::errors::{E2eError, Result};

    /// A stream that answers every read with the next heartbeat and never ends.
    struct Ticks(u64);

    impl EventStream for Ticks {
        fn status(&self) -> u16 {
            200
        }

        fn content_type(&self) -> Option<&str> {
            Some("text/event-stream")
        }

        fn next_event(&mut self) -> Result<Option<Frame>> {
            self.next_event_within(Duration::MAX)
        }

        fn next_event_within(&mut self, _budget: Duration) -> Result<Option<Frame>> {
            let seq = self.0;
            self.0 += 1;
            Ok(Some(Frame::Tick(Tick {
                seq,
                time: Timestamp::from_unix_millis(0),
            })))
        }
    }

    #[test]
    fn heartbeats_do_not_reset_the_deadline() {
        let result = await_frames(&mut Ticks(0), Duration::from_millis(1), &mut |_| false);

        assert!(matches!(result, Err(E2eError::Timeout { .. })));
    }

    struct Queued(VecDeque<Frame>);

    impl EventStream for Queued {
        fn status(&self) -> u16 {
            200
        }

        fn content_type(&self) -> Option<&str> {
            Some("text/event-stream")
        }

        fn next_event(&mut self) -> Result<Option<Frame>> {
            Ok(self.0.pop_front())
        }

        fn next_event_within(&mut self, _budget: Duration) -> Result<Option<Frame>> {
            self.next_event()
        }
    }

    fn snapshot(seq: u64) -> Frame {
        Frame::Snapshot(State {
            seq,
            ..State::default()
        })
    }

    fn delta(base: u64, seq: u64, completed: u64) -> Frame {
        Frame::Delta(Delta {
            seq,
            base,
            update_time: Timestamp::from_unix_millis(0),
            pass: None,
            queue: None,
            cache: None,
            fleets: None,
            workers: None,
            instances: None,
            actions: None,
            events: Vec::new(),
            completed: Some(completed),
            recent: None,
            builds: None,
            principals_epoch: None,
        })
    }

    fn tick() -> Frame {
        Frame::Tick(Tick {
            seq: 1,
            time: Timestamp::from_unix_millis(0),
        })
    }

    #[test]
    fn observes_transient_delta_before_superseding_frame() {
        let mut stream = Queued(VecDeque::from([
            snapshot(1),
            delta(1, 2, 1),
            delta(2, 3, 0),
        ]));
        let state = await_state(&mut stream, Duration::from_secs(30), &mut |state| {
            state.completed == 1
        })
        .expect("the transient state should be observed");

        assert_eq!(state.seq, 2);
        assert_eq!(state.completed, 1);
        assert_eq!(stream.0.len(), 1);
    }

    #[test]
    fn tick_rechecks_retained_snapshot() {
        let mut stream = Queued(VecDeque::from([tick(), snapshot(1), tick()]));
        let mut calls = 0;
        let state = await_state(&mut stream, Duration::from_secs(30), &mut |state| {
            assert_eq!(state.seq, 1);
            calls += 1;
            calls == 2
        })
        .expect("the tick should recheck the snapshot");

        assert_eq!(state.seq, 1);
        assert_eq!(calls, 2);
    }

    #[test]
    fn snapshot_replaces_retained_state() {
        let mut stream = Queued(VecDeque::from([snapshot(1), snapshot(4), delta(4, 5, 1)]));
        let state = await_state(&mut stream, Duration::from_secs(30), &mut |state| {
            state.completed == 1
        })
        .expect("the new snapshot should replace the old one");

        assert_eq!(state.seq, 5);
    }

    #[test]
    fn delta_before_snapshot_is_rejected() {
        let mut stream = Queued(VecDeque::from([delta(0, 1, 1)]));
        let error = await_state(&mut stream, Duration::from_secs(30), &mut |_| true)
            .expect_err("a delta requires a snapshot");

        assert!(matches!(error, E2eError::System { .. }));
        assert!(error.to_string().contains("delta before a snapshot"));
    }

    #[test]
    fn delta_with_wrong_base_is_rejected() {
        let mut stream = Queued(VecDeque::from([snapshot(1), delta(0, 2, 1)]));
        let error = await_state(&mut stream, Duration::from_secs(30), &mut |_| false)
            .expect_err("a delta must match the retained sequence");

        assert!(matches!(
            error,
            E2eError::System {
                source: Some(_),
                ..
            }
        ));
        assert!(error.to_string().contains("applying a delta"));
    }

    #[test]
    fn closed_before_condition_is_rejected() {
        let mut stream = Queued(VecDeque::from([
            snapshot(1),
            Frame::Closed(Closed {
                reason: ClosedReason::Shutdown,
                retry_delay: None,
            }),
        ]));
        let error = await_state(&mut stream, Duration::from_secs(30), &mut |_| false)
            .expect_err("a closed stream cannot satisfy the condition");

        assert!(matches!(error, E2eError::System { .. }));
        assert!(error.to_string().contains("closed before"));
    }

    #[test]
    fn end_before_condition_is_rejected() {
        let mut stream = Queued(VecDeque::from([snapshot(1)]));
        let error = await_state(&mut stream, Duration::from_secs(30), &mut |_| false)
            .expect_err("an ended stream cannot satisfy the condition");

        assert!(matches!(error, E2eError::System { .. }));
        assert!(error.to_string().contains("ended before"));
    }

    #[test]
    fn state_wait_uses_the_standard_timeout() {
        let mut stream = Queued(VecDeque::from([snapshot(1)]));
        let error = await_state(&mut stream, Duration::ZERO, &mut |_| true)
            .expect_err("a zero deadline should expire before reading");

        assert!(matches!(
            error,
            E2eError::Timeout { ref condition, waited }
                if condition == "the state stream to satisfy the awaited condition"
                    && waited == Duration::ZERO
        ));
        assert_eq!(stream.0.len(), 1);
    }
}
