//! The deterministic timer queue behind [`crate::TestClock`]: sleepers and the monotonic reading
//! they wait on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use parking_lot::Mutex;

use crate::Tick;

/// A task waiting for the monotonic reading to reach its deadline.
#[derive(Debug)]
struct Sleeper {
    id: u64,
    deadline: Tick,
    waker: Waker,
}

#[derive(Debug, Default)]
struct State {
    monotonic_nanos: u64,
    next_id: u64,
    sleepers: Vec<Sleeper>,
}

/// The monotonic reading of a test clock and the sleepers that wait on it.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    monotonic_nanos: AtomicU64,
    state: Mutex<State>,
}

impl Timers {
    /// The current monotonic reading in nanoseconds.
    pub(crate) fn now_nanos(&self) -> u64 {
        self.monotonic_nanos.load(Ordering::SeqCst)
    }

    /// The current monotonic reading.
    pub(crate) fn now(&self) -> Tick {
        Tick::from_elapsed(Duration::from_nanos(self.now_nanos()))
    }

    /// Hands out the identity a sleeper registers under.
    pub(crate) fn next_id(&self) -> u64 {
        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id = id.wrapping_add(1);
        id
    }

    /// Moves the reading forward by `nanos`, saturating, and wakes every sleeper whose deadline
    /// has been reached.
    pub(crate) fn advance(&self, nanos: u64) {
        let mut due = Vec::new();
        {
            let mut state = self.state.lock();
            state.monotonic_nanos = state.monotonic_nanos.saturating_add(nanos);
            self.monotonic_nanos
                .store(state.monotonic_nanos, Ordering::SeqCst);
            let now = Tick::from_elapsed(Duration::from_nanos(state.monotonic_nanos));
            state.sleepers.retain(|sleeper| {
                let reached = sleeper.deadline <= now;
                if reached {
                    due.push(sleeper.waker.clone());
                }
                !reached
            });
        }
        for waker in due {
            waker.wake();
        }
    }

    /// Polls the sleeper `id`: ready once the reading reached `deadline`, otherwise it is
    /// registered to be woken by the advance that reaches it.
    pub(crate) fn poll(&self, id: u64, deadline: Tick, context: &Context<'_>) -> Poll<()> {
        let mut state = self.state.lock();
        let now = Tick::from_elapsed(Duration::from_nanos(state.monotonic_nanos));
        if now >= deadline {
            return Poll::Ready(());
        }
        match state.sleepers.iter_mut().find(|sleeper| sleeper.id == id) {
            Some(sleeper) => sleeper.waker.clone_from(context.waker()),
            None => state.sleepers.push(Sleeper {
                id,
                deadline,
                waker: context.waker().clone(),
            }),
        }
        Poll::Pending
    }

    /// Removes the sleeper `id`, if it is registered.
    pub(crate) fn forget(&self, id: u64) {
        self.state
            .lock()
            .sleepers
            .retain(|sleeper| sleeper.id != id);
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use crate::{Clock, TestClock, Timestamp};

    /// A waker that counts how many times it has been woken.
    #[derive(Default)]
    struct CountingWaker {
        wakes: AtomicUsize,
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_sleep_repolled_with_a_new_waker_wakes_only_the_new_waker() {
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let waker_a = Arc::new(CountingWaker::default());
        let waker_b = Arc::new(CountingWaker::default());
        let handle_a = Waker::from(Arc::clone(&waker_a));
        let handle_b = Waker::from(Arc::clone(&waker_b));
        let mut sleep = pin!(clock.sleep(Duration::from_secs(1)));

        let mut cx_a = Context::from_waker(&handle_a);
        assert!(sleep.as_mut().poll(&mut cx_a).is_pending());
        let mut cx_b = Context::from_waker(&handle_b);
        assert!(sleep.as_mut().poll(&mut cx_b).is_pending());

        clock.advance(Duration::from_secs(1));

        assert_eq!(waker_b.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(waker_a.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(sleep.as_mut().poll(&mut cx_b), Poll::Ready(()));
    }
}
