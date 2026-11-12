//! The stream of the live state as a scenario pulls it: frames, one at a time, from an open
//! response of the state stream route.

use std::time::{Duration, Instant};

use gang_wire::Frame;

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

/// The timeout of an awaited condition of the state stream that `waited` long.
fn timed_out(waited: Duration) -> E2eError {
    E2eError::timeout("the state stream to satisfy the awaited condition", waited)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_wire::{Frame, Tick};

    use super::{EventStream, await_frames};
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
}
