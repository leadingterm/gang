use std::time::{Duration, SystemTime};

use tokio::time::Instant;

use crate::clock::Clock;
use crate::sleep::Sleep;
use crate::tick::Tick;
use crate::timestamp::Timestamp;

/// How far ahead a deadline that overflows the runtime timer is placed.
const FAR_FUTURE: Duration = Duration::from_hours(30 * 365 * 24);

/// A [`Clock`] backed by the host operating system.
///
/// Wall readings come from [`SystemTime`]; monotonic readings and sleeps come from the runtime
/// timer, measured from the moment the clock was created, so a runtime with paused time moves
/// the clock too.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// Creates a system clock whose monotonic origin is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    /// Returns the host wall clock. A host clock set before the Unix epoch reads as the epoch.
    fn wall(&self) -> Timestamp {
        SystemTime::now().into()
    }

    fn monotonic(&self) -> Tick {
        Tick::from_elapsed(self.origin.elapsed())
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        let elapsed = deadline.elapsed();
        let end = self
            .origin
            .checked_add(elapsed)
            .unwrap_or_else(|| self.origin + FAR_FUTURE);
        Sleep::on_runtime_until(end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn sleep_advances_the_monotonic_reading_by_the_duration() {
        let clock = SystemClock::new();
        let before = clock.monotonic();

        clock.sleep(Duration::from_secs(5)).await;

        assert_eq!(
            clock.monotonic().saturating_duration_since(before),
            Duration::from_secs(5)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_in_the_past_completes_at_once() {
        let clock = SystemClock::new();
        clock.sleep(Duration::from_secs(2)).await;
        let now = clock.monotonic();

        clock.sleep_until(Tick::ORIGIN).await;

        assert_eq!(clock.monotonic(), now);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_deadline_does_not_panic() {
        use std::task::{Context, Poll, Waker};

        let clock = SystemClock::new();
        let sleep = clock.sleep_until(Tick::from_elapsed(Duration::MAX));
        let mut sleep = std::pin::pin!(sleep);
        let mut context = Context::from_waker(Waker::noop());

        assert_eq!(sleep.as_mut().poll(&mut context), Poll::Pending);

        tokio::time::advance(Duration::from_hours(24)).await;

        assert_eq!(sleep.as_mut().poll(&mut context), Poll::Pending);
    }
}
