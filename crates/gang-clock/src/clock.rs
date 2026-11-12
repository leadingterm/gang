use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use crate::{Sleep, Tick, Timeout, Timestamp};

/// A source of time.
///
/// Every component that reads time or waits takes a `Clock` by injection instead of calling the
/// host clock or the runtime timer directly. Tests use [`crate::TestClock`] so that verdicts
/// never depend on the host clock.
pub trait Clock: Send + Sync {
    /// Returns the current wall-clock instant.
    #[must_use]
    fn wall(&self) -> Timestamp;

    /// Returns the current monotonic reading.
    ///
    /// Readings are only comparable with other readings from the same clock.
    #[must_use]
    fn monotonic(&self) -> Tick;

    /// Waits until the monotonic reading reaches `deadline`; a deadline in the past completes
    /// at once.
    ///
    /// The default waits on the runtime timer for the time that remains, which is right for a
    /// clock that follows real (or the runtime's paused) time. A clock that is driven by hand
    /// overrides it.
    fn sleep_until(&self, deadline: Tick) -> Sleep {
        Sleep::on_runtime(deadline.saturating_duration_since(self.monotonic()))
    }

    /// Waits for `duration` of monotonic time.
    fn sleep(&self, duration: Duration) -> Sleep {
        self.sleep_until(self.monotonic() + duration)
    }

    /// Runs `future` until it completes or `duration` of monotonic time passes, whichever is first.
    ///
    /// If both are ready, the future wins. Dropping the returned timeout drops the future,
    /// cancelling it. On a `&dyn Clock`, call `Clock::timeout(&clock, ..)`: the reference is the
    /// sized clock.
    fn timeout<F: IntoFuture>(&self, duration: Duration, future: F) -> Timeout<F::IntoFuture>
    where
        Self: Sized,
    {
        Timeout::new(future.into_future(), self.sleep(duration))
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn wall(&self) -> Timestamp {
        (**self).wall()
    }

    fn monotonic(&self) -> Tick {
        (**self).monotonic()
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        (**self).sleep_until(deadline)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        (**self).sleep(duration)
    }
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn wall(&self) -> Timestamp {
        (**self).wall()
    }

    fn monotonic(&self) -> Tick {
        (**self).monotonic()
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        (**self).sleep_until(deadline)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        (**self).sleep(duration)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Waker};

    use super::*;
    use crate::TestClock;

    /// A clock driven by hand that does not override `sleep_until`, so it uses the default.
    struct Manual {
        nanos: AtomicU64,
    }

    impl Manual {
        fn at(reading: Duration) -> Self {
            Self {
                nanos: AtomicU64::new(
                    u64::try_from(reading.as_nanos()).expect("test readings fit in u64 nanos"),
                ),
            }
        }
    }

    impl Clock for Manual {
        fn wall(&self) -> Timestamp {
            Timestamp::UNIX_EPOCH
        }

        fn monotonic(&self) -> Tick {
            Tick::from_elapsed(Duration::from_nanos(self.nanos.load(Ordering::SeqCst)))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_default_sleep_waits_on_the_runtime_timer_for_the_remaining_time() {
        let clock = Manual::at(Duration::from_secs(3));
        let start = tokio::time::Instant::now();

        clock
            .sleep_until(Tick::from_elapsed(Duration::from_secs(8)))
            .await;

        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_in_the_past_completes_without_waiting() {
        let clock = Manual::at(Duration::from_secs(5));
        let start = tokio::time::Instant::now();

        clock
            .sleep_until(Tick::from_elapsed(Duration::from_secs(2)))
            .await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_is_relative_to_the_current_reading() {
        let clock = Manual::at(Duration::from_secs(3));
        let start = tokio::time::Instant::now();

        clock.sleep(Duration::from_secs(2)).await;

        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn reference_forwards_the_sleep_to_the_clock() {
        let deadline = Tick::from_elapsed(Duration::from_secs(1));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let clock = TestClock::default();
        let by_ref: &TestClock = &clock;
        let mut sleep = pin!(<&TestClock as Clock>::sleep_until(&by_ref, deadline));
        assert!(sleep.as_mut().poll(&mut cx).is_pending());
        clock.advance(Duration::from_secs(1));
        assert!(sleep.as_mut().poll(&mut cx).is_ready());
    }
}
