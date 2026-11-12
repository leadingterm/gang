use std::time::Duration;

use gang_types::{NonZeroDuration, SaturatingInto};

/// How often and for how long something is polled: the number of attempts that cover
/// `timeout` when they are `interval` apart.
///
/// A poll loop checks, then waits [`Poll::interval`] on its [`Clock`](crate::Clock) between
/// attempts, at most [`Poll::attempts`] times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Poll {
    timeout: Duration,
    interval: NonZeroDuration,
}

impl Poll {
    /// Polls every `interval` for up to `timeout`.
    #[must_use]
    pub const fn new(timeout: Duration, interval: NonZeroDuration) -> Self {
        Self { timeout, interval }
    }

    /// The time between two attempts.
    #[must_use]
    pub const fn interval(&self) -> NonZeroDuration {
        self.interval
    }

    /// `ceil(timeout / interval)` attempts, and at least one, so that a zero timeout still checks
    /// once; saturates at `u32::MAX`.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        let attempts = self
            .timeout
            .as_nanos()
            .div_ceil(self.interval.get().as_nanos())
            .max(1);
        u64::try_from(attempts)
            .unwrap_or(u64::MAX)
            .saturating_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poll(timeout: Duration, interval: Duration) -> Poll {
        Poll::new(
            timeout,
            NonZeroDuration::try_new(interval)
                .expect("the interval is a positive whole number of milliseconds"),
        )
    }

    #[test]
    fn attempts_round_the_timeout_up_to_whole_intervals() {
        assert_eq!(
            poll(Duration::from_secs(10), Duration::from_secs(5)).attempts(),
            2
        );
        assert_eq!(
            poll(Duration::from_secs(11), Duration::from_secs(5)).attempts(),
            3
        );
        assert_eq!(
            poll(Duration::from_secs(1), Duration::from_secs(5)).attempts(),
            1
        );
    }

    #[test]
    fn a_zero_timeout_still_attempts_once() {
        assert_eq!(poll(Duration::ZERO, Duration::from_secs(1)).attempts(), 1);
    }

    #[test]
    fn attempts_saturate() {
        assert_eq!(
            poll(Duration::MAX, Duration::from_millis(1)).attempts(),
            u32::MAX
        );
    }
}
