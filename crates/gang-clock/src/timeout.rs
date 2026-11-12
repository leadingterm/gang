use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{Elapsed, Sleep};

pin_project_lite::pin_project! {
    /// A future bounded by a monotonic deadline.
    ///
    /// Dropping this timeout drops the wrapped future, cancelling it.
    #[must_use = "a timeout does nothing unless awaited"]
    pub struct Timeout<F> {
        #[pin]
        future: F,
        #[pin]
        deadline: Sleep,
    }
}

impl<F> Timeout<F> {
    pub(crate) fn new(future: F, deadline: Sleep) -> Self {
        Self { future, deadline }
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(value) = this.future.poll(cx) {
            return Poll::Ready(Ok(value));
        }
        if this.deadline.poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use super::*;
    use crate::{Clock, TestClock};

    #[test]
    fn pending_future_times_out_exactly_at_deadline() {
        let clock = TestClock::default();
        let mut timeout = pin!(clock.timeout(Duration::from_secs(1), pending::<()>()));
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance(Duration::from_secs(1) - Duration::from_nanos(1));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance(Duration::from_nanos(1));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Err(Elapsed)));
    }

    #[test]
    fn future_completing_before_deadline_returns_value() {
        let clock = TestClock::default();
        let future = async {
            clock.sleep(Duration::from_secs(1)).await;
            42
        };
        let mut timeout = pin!(clock.timeout(Duration::from_secs(2), future));
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance(Duration::from_secs(1));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Ok(42)));
    }

    #[test]
    fn ready_future_wins_when_deadline_has_passed() {
        let clock = TestClock::default();
        let mut timeout = pin!(clock.timeout(Duration::from_secs(1), ready(42)));
        let mut cx = Context::from_waker(Waker::noop());

        clock.advance(Duration::from_secs(1));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Ok(42)));
    }

    #[test]
    fn trait_object_reference_and_arc_use_injected_clock() {
        let clock = Arc::new(TestClock::default());
        let by_ref: &dyn Clock = clock.as_ref();
        let by_arc: Arc<dyn Clock> = clock.clone();
        // A `&dyn Clock` is itself a sized `Clock`, so the call goes through the reference.
        let mut ref_timeout = pin!(Clock::timeout(
            &by_ref,
            Duration::from_secs(1),
            pending::<()>()
        ));
        let mut arc_timeout = pin!(by_arc.timeout(Duration::from_secs(2), pending::<()>()));
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(ref_timeout.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(arc_timeout.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance(Duration::from_secs(1));
        assert_eq!(
            ref_timeout.as_mut().poll(&mut cx),
            Poll::Ready(Err(Elapsed))
        );
        assert_eq!(arc_timeout.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance(Duration::from_secs(1));
        assert_eq!(
            arc_timeout.as_mut().poll(&mut cx),
            Poll::Ready(Err(Elapsed))
        );
    }
}
