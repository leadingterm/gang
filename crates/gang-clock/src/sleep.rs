use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;

use crate::Tick;
use crate::timers::Timers;

pin_project! {
    /// The future of [`Clock::sleep_until`](crate::Clock::sleep_until): it completes once the
    /// clock's monotonic reading reaches the deadline.
    ///
    /// It is a concrete type so that `dyn Clock` stays usable. A [`SystemClock`](crate::SystemClock)
    /// sleep rides the runtime timer; a [`TestClock`](crate::TestClock) sleep completes when
    /// [`TestClock::advance`](crate::TestClock::advance) reaches its deadline.
    #[must_use = "a sleep does nothing unless awaited"]
    pub struct Sleep {
        #[pin]
        kind: Kind,
    }
}

pin_project! {
    #[project = KindProjection]
    enum Kind {
        Runtime {
            #[pin]
            sleep: tokio::time::Sleep,
        },
        Test {
            timer: TestTimer,
        },
    }
}

/// A sleeper registered with a test clock's timers.
struct TestTimer {
    timers: Arc<Timers>,
    id: u64,
    deadline: Tick,
    registered: bool,
}

impl Drop for TestTimer {
    fn drop(&mut self) {
        if self.registered {
            self.timers.forget(self.id);
        }
    }
}

impl Sleep {
    /// A sleep on the runtime timer, over `remaining` from now.
    pub(crate) fn on_runtime(remaining: Duration) -> Self {
        Self {
            kind: Kind::Runtime {
                sleep: tokio::time::sleep(remaining),
            },
        }
    }

    /// A sleep on the runtime timer that ends at `deadline`.
    pub(crate) fn on_runtime_until(deadline: tokio::time::Instant) -> Self {
        Self {
            kind: Kind::Runtime {
                sleep: tokio::time::sleep_until(deadline),
            },
        }
    }

    /// A sleep that ends when `timers` reach `deadline`.
    pub(crate) fn on_timers(timers: Arc<Timers>, deadline: Tick) -> Self {
        let id = timers.next_id();
        Self {
            kind: Kind::Test {
                timer: TestTimer {
                    timers,
                    id,
                    deadline,
                    registered: false,
                },
            },
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        match self.project().kind.project() {
            KindProjection::Runtime { sleep } => sleep.poll(context),
            KindProjection::Test { timer } => {
                let result = timer.timers.poll(timer.id, timer.deadline, context);
                timer.registered = result.is_pending();
                result
            }
        }
    }
}
