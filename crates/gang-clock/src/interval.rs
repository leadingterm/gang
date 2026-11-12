//! A deadline that recurs on a [`Clock`].

use gang_types::NonZeroDuration;

use crate::{Clock, Tick};

/// A deadline that recurs every `period` on a [`Clock`].
///
/// The next deadline is one period after this one, or after the moment the tick was asked for
/// when that came later, so work that overruns the period delays the ticks after it. A deadline
/// that has already passed when the tick completes moves to one period from then, so ticks never
/// run back to back.
#[derive(Debug)]
pub struct Interval<C> {
    clock: C,
    next: Tick,
    period: NonZeroDuration,
}

impl<C: Clock> Interval<C> {
    /// An interval whose first tick completes at `first`, then one every `period`.
    #[must_use]
    pub fn new(clock: C, first: Tick, period: NonZeroDuration) -> Self {
        Self {
            clock,
            next: first,
            period,
        }
    }

    /// Waits for the next deadline.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future before it completes leaves the schedule unchanged.
    pub async fn tick(&mut self) {
        let asked = self.clock.monotonic();
        self.clock.sleep_until(self.next).await;
        let now = self.clock.monotonic();
        let next = self.next.max(asked) + self.period.get();
        self.next = if next > now {
            next
        } else {
            now + self.period.get()
        };
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Waker};
    use std::time::Duration;

    use super::*;
    use crate::TestClock;

    const PERIOD: NonZeroDuration = NonZeroDuration::from_secs(10);

    #[tokio::test]
    async fn the_first_tick_completes_at_first_and_not_before() {
        let clock = TestClock::default();
        let mut interval = Interval::new(&clock, Tick::ORIGIN + Duration::from_secs(10), PERIOD);
        let mut cx = Context::from_waker(Waker::noop());

        {
            let mut tick = pin!(interval.tick());
            assert!(tick.as_mut().poll(&mut cx).is_pending());
        }
        clock.advance(Duration::from_secs(9));
        {
            let mut tick = pin!(interval.tick());
            assert!(tick.as_mut().poll(&mut cx).is_pending());
        }
        clock.advance(Duration::from_secs(1));
        let mut tick = pin!(interval.tick());
        assert!(tick.as_mut().poll(&mut cx).is_ready());
    }

    #[tokio::test]
    async fn a_tick_that_waited_keeps_the_schedule() {
        let clock = TestClock::default();
        let mut interval = Interval::new(&clock, Tick::ORIGIN + Duration::from_secs(10), PERIOD);
        let mut cx = Context::from_waker(Waker::noop());

        // Asked at 0, deadline 10; the schedule moves to 20.
        {
            let mut first = pin!(interval.tick());
            assert!(first.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(10));
            assert!(first.as_mut().poll(&mut cx).is_ready());
        }
        // Asked at 10, deadline 20; completes at 23, so the schedule moves to 30.
        {
            let mut second = pin!(interval.tick());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(13));
            assert!(second.as_mut().poll(&mut cx).is_ready());
        }
        // Asked at 23, deadline 30.
        {
            let mut third = pin!(interval.tick());
            assert!(third.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(7));
            assert!(third.as_mut().poll(&mut cx).is_ready());
        }
    }

    #[tokio::test]
    async fn a_late_tick_completes_at_once_and_delays_the_next() {
        let clock = TestClock::default();
        let mut interval = Interval::new(&clock, Tick::ORIGIN + Duration::from_secs(10), PERIOD);
        let mut cx = Context::from_waker(Waker::noop());

        clock.advance(Duration::from_secs(25));
        // Asked at 25, deadline 10 has passed; the schedule moves to max(10, 25) + 10 = 35.
        {
            let mut first = pin!(interval.tick());
            assert!(first.as_mut().poll(&mut cx).is_ready());
        }
        {
            let mut second = pin!(interval.tick());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(9));
            assert!(second.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(1));
            assert!(second.as_mut().poll(&mut cx).is_ready());
        }
    }

    #[tokio::test]
    async fn a_dropped_tick_leaves_the_schedule_unchanged() {
        let clock = TestClock::default();
        let mut interval = Interval::new(&clock, Tick::ORIGIN + Duration::from_secs(10), PERIOD);
        let mut cx = Context::from_waker(Waker::noop());

        {
            let mut dropped = pin!(interval.tick());
            assert!(dropped.as_mut().poll(&mut cx).is_pending());
        }
        clock.advance(Duration::from_secs(10));
        // Still due at 10, so it completes at once and the next deadline is 20.
        {
            let mut first = pin!(interval.tick());
            assert!(first.as_mut().poll(&mut cx).is_ready());
        }
        {
            let mut second = pin!(interval.tick());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(10));
            assert!(second.as_mut().poll(&mut cx).is_ready());
        }
    }

    #[tokio::test]
    async fn a_tick_woken_a_whole_period_late_delays_the_next() {
        let clock = TestClock::default();
        let mut interval = Interval::new(&clock, Tick::ORIGIN + Duration::from_secs(10), PERIOD);
        let mut cx = Context::from_waker(Waker::noop());

        // Asked at 0, deadline 10; completes at 10, so the schedule moves to 20.
        {
            let mut first = pin!(interval.tick());
            assert!(first.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(10));
            assert!(first.as_mut().poll(&mut cx).is_ready());
        }
        // Asked at 10, deadline 20; completes at 35, so the schedule moves to 45.
        {
            let mut second = pin!(interval.tick());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(25));
            assert!(second.as_mut().poll(&mut cx).is_ready());
        }
        // Asked at 35, deadline 45.
        {
            let mut third = pin!(interval.tick());
            assert!(third.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(9));
            assert!(third.as_mut().poll(&mut cx).is_pending());
            clock.advance(Duration::from_secs(1));
            assert!(third.as_mut().poll(&mut cx).is_ready());
        }
    }
}
