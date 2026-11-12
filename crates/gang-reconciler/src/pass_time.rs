//! The instant of one control pass.

use gang_clock::{Clock, Tick, Timestamp};

/// The two clocks of one control pass, read once so that every decision and record of the
/// pass agrees on when it happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassTime {
    /// The wall clock: launch times, timestamps of events and of the published frame.
    pub wall: Timestamp,
    /// The monotonic clock: the reconciler's idle, drain and boot-grace timers.
    pub now: Tick,
}

impl PassTime {
    /// Reads both clocks of `clock` once.
    #[must_use]
    pub fn read(clock: &impl Clock) -> Self {
        Self {
            wall: clock.wall(),
            now: clock.monotonic(),
        }
    }
}
