use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;
use crate::sleep::Sleep;
use crate::tick::Tick;
use crate::timers::Timers;
use crate::timestamp::Timestamp;

/// A deterministic [`Clock`] that moves only when [`TestClock::advance`] is called.
///
/// Wall and monotonic readings advance together, and an advance completes every
/// [`Clock::sleep_until`] whose deadline it reaches. Methods take `&self`, so a single
/// `TestClock` can be shared by reference or inside an `Arc`.
#[derive(Debug)]
pub struct TestClock {
    origin_wall_millis: u64,
    timers: Arc<Timers>,
}

impl TestClock {
    /// Creates a test clock whose wall reading is `wall` and whose monotonic reading is [`Tick::ORIGIN`].
    #[must_use]
    pub fn new(wall: Timestamp) -> Self {
        Self {
            origin_wall_millis: wall.as_unix_millis(),
            timers: Arc::new(Timers::default()),
        }
    }

    /// Moves the monotonic reading forward by `by`, saturating at `u64::MAX` nanoseconds,
    /// and wakes the sleeps whose deadline is reached.
    ///
    /// The wall reading reflects the total elapsed time in whole milliseconds.
    pub fn advance(&self, by: Duration) {
        self.timers
            .advance(u64::try_from(by.as_nanos()).unwrap_or(u64::MAX));
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new(Timestamp::UNIX_EPOCH)
    }
}

impl Clock for TestClock {
    fn wall(&self) -> Timestamp {
        Timestamp::from_unix_millis(
            self.origin_wall_millis
                .saturating_add(self.timers.now_nanos() / 1_000_000),
        )
    }

    fn monotonic(&self) -> Tick {
        self.timers.now()
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        Sleep::on_timers(Arc::clone(&self.timers), deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wall_of<C: Clock>(clock: C) -> Timestamp {
        clock.wall()
    }

    fn tick_of<C: Clock>(clock: C) -> Tick {
        clock.monotonic()
    }

    #[test]
    fn starts_at_given_wall_and_origin_tick() {
        let clock = TestClock::new(Timestamp::from_unix_millis(42_000));
        assert_eq!(clock.wall(), Timestamp::from_unix_millis(42_000));
        assert_eq!(clock.monotonic(), Tick::ORIGIN);
    }

    #[test]
    fn default_starts_at_unix_epoch() {
        let clock = TestClock::default();
        assert_eq!(clock.wall(), Timestamp::UNIX_EPOCH);
        assert_eq!(clock.monotonic(), Tick::ORIGIN);
    }

    #[test]
    fn advance_moves_wall_and_monotonic_by_exact_amount() {
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
        let wall_before = clock.wall();
        let tick_before = clock.monotonic();

        clock.advance(Duration::from_millis(500));

        assert_eq!(
            clock.wall().saturating_duration_since(wall_before),
            Duration::from_millis(500)
        );
        assert_eq!(
            clock.monotonic().saturating_duration_since(tick_before),
            Duration::from_millis(500)
        );
        assert_eq!(clock.wall().as_unix_millis(), 1_500);
    }

    #[test]
    fn repeated_advances_accumulate() {
        let clock = TestClock::new(Timestamp::from_unix_millis(0));
        clock.advance(Duration::from_millis(250));
        clock.advance(Duration::from_millis(250));
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.wall().as_unix_millis(), 1_500);
        assert_eq!(clock.monotonic().elapsed(), Duration::from_millis(1_500));
    }

    #[test]
    fn repeated_sub_millisecond_advances_accumulate_on_both_readings() {
        let clock = TestClock::new(Timestamp::from_unix_millis(7));
        let wall_before = clock.wall().as_unix_millis();
        let tick_before = clock.monotonic();

        for _ in 0..1_000 {
            clock.advance(Duration::from_micros(500));
        }

        assert_eq!(clock.wall().as_unix_millis() - wall_before, 500);
        assert_eq!(
            clock.monotonic().saturating_duration_since(tick_before),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn advance_through_shared_reference_is_visible_via_clock_impl_for_reference() {
        let clock = TestClock::new(Timestamp::from_unix_millis(10));
        let shared: &TestClock = &clock;
        shared.advance(Duration::from_millis(90));
        assert_eq!(wall_of(&clock).as_unix_millis(), 100);
        assert_eq!(wall_of(shared).as_unix_millis(), 100);
        assert_eq!(tick_of(&clock).elapsed(), Duration::from_millis(90));
    }

    #[test]
    fn sub_millisecond_advance_moves_only_monotonic_reading() {
        let clock = TestClock::new(Timestamp::from_unix_millis(7));
        clock.advance(Duration::from_micros(400));
        assert_eq!(clock.wall().as_unix_millis(), 7);
        assert_eq!(clock.monotonic().elapsed(), Duration::from_micros(400));
    }

    use std::pin::{Pin, pin};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Default)]
    struct Counter(AtomicUsize);

    impl Counter {
        fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_once(sleep: Pin<&mut Sleep>, counter: &Arc<Counter>) -> Poll<()> {
        let waker = Waker::from(Arc::clone(counter));
        let mut cx = Context::from_waker(&waker);
        sleep.poll(&mut cx)
    }

    #[test]
    fn a_sleep_is_pending_until_the_clock_reaches_its_deadline() {
        let clock = TestClock::default();
        let counter = Counter::shared();
        let mut sleep = pin!(clock.sleep(Duration::from_secs(10)));

        assert!(poll_once(sleep.as_mut(), &counter).is_pending());

        clock.advance(Duration::from_secs(9));
        assert!(poll_once(sleep.as_mut(), &counter).is_pending());
        assert_eq!(counter.count(), 0);

        clock.advance(Duration::from_secs(1));
        assert_eq!(counter.count(), 1);
        assert!(poll_once(sleep.as_mut(), &counter).is_ready());
    }

    #[test]
    fn a_deadline_in_the_past_is_ready_at_once() {
        let clock = TestClock::default();
        let counter = Counter::shared();
        clock.advance(Duration::from_secs(5));

        let mut sleep = pin!(clock.sleep_until(Tick::ORIGIN));
        assert!(poll_once(sleep.as_mut(), &counter).is_ready());
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn one_advance_wakes_every_due_sleeper_and_only_those() {
        let clock = TestClock::default();
        let first_counter = Counter::shared();
        let second_counter = Counter::shared();
        let mut first = pin!(clock.sleep(Duration::from_secs(1)));
        let mut second = pin!(clock.sleep(Duration::from_secs(3)));

        assert!(poll_once(first.as_mut(), &first_counter).is_pending());
        assert!(poll_once(second.as_mut(), &second_counter).is_pending());

        clock.advance(Duration::from_secs(2));
        assert_eq!(first_counter.count(), 1);
        assert_eq!(second_counter.count(), 0);

        assert!(poll_once(first.as_mut(), &first_counter).is_ready());
        assert!(poll_once(second.as_mut(), &second_counter).is_pending());
    }

    #[test]
    fn a_dropped_sleep_is_not_woken() {
        let clock = TestClock::default();
        let counter = Counter::shared();
        let mut sleep = Box::pin(clock.sleep(Duration::from_secs(1)));

        assert!(poll_once(sleep.as_mut(), &counter).is_pending());
        drop(sleep);

        clock.advance(Duration::from_secs(2));
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn sleep_shares_the_clock_through_an_arc() {
        let clock = Arc::new(TestClock::default());
        let shared: Arc<dyn Clock> = clock.clone();
        let counter = Counter::shared();
        let mut sleep = pin!(shared.sleep(Duration::from_secs(1)));

        assert!(poll_once(sleep.as_mut(), &counter).is_pending());

        clock.advance(Duration::from_secs(1));
        assert_eq!(counter.count(), 1);
        assert!(poll_once(sleep.as_mut(), &counter).is_ready());
    }

    #[test]
    fn advance_saturates_instead_of_overflowing() {
        let origin = Timestamp::from_unix_millis(42);
        let clock = TestClock::new(origin);
        let mut sleep = pin!(clock.sleep(Duration::from_secs(1)));
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);

        clock.advance(Duration::MAX);

        assert_eq!(
            clock.wall().as_unix_millis(),
            origin.as_unix_millis()
                + u64::try_from(clock.monotonic().elapsed().as_millis())
                    .expect("elapsed nanoseconds fit in u64")
        );
        assert_eq!(clock.monotonic().elapsed(), Duration::from_nanos(u64::MAX));
        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Ready(()));
    }
}
