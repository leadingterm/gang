use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use tokio::sync::oneshot;

/// Resolves `future`, sending on `armed` the moment it first returns `Pending`.
///
/// The signal is sent from inside the poll, after the future has registered its waker. A test
/// that awaits `armed` therefore knows the task is parked on the awaited condition, so any later
/// wake-up is caused by the code under test and not by scheduling order.
///
/// The signal is sent at most once. If `future` completes without ever returning `Pending`, nothing
/// is sent and `armed` is dropped, so a receiver awaiting it observes a `RecvError`. If the
/// receiver has already been dropped, the send is silently skipped and `future` still runs to
/// completion.
pub async fn park_then_signal<F: Future>(future: F, armed: oneshot::Sender<()>) -> F::Output {
    let mut future = pin!(future);
    let mut armed = Some(armed);
    poll_fn(|cx| {
        let poll = future.as_mut().poll(cx);
        if poll.is_pending()
            && let Some(armed) = armed.take()
        {
            // A receiver that has already gone away no longer waits for the signal.
            armed.send(()).ok();
        }
        poll
    })
    .await
}

/// Polls `future` once and returns its output when it completes without waiting, or `None` when
/// it would wait. Tests use it to assert that an operation never waits, without any timeout.
pub fn ready_now<F: Future>(future: F) -> Option<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    match pin!(future).poll(&mut context) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

/// Polls `future` once and returns its output.
///
/// Panics at the caller if the future is pending.
#[track_caller]
pub fn must_ready<F: Future>(future: F) -> F::Output {
    match ready_now(future) {
        Some(output) => output,
        None => unreachable!("future was pending when it must be ready"),
    }
}

#[cfg(test)]
mod tests {
    use super::must_ready;

    #[test]
    fn must_ready_returns_the_future_output() {
        assert_eq!(must_ready(async { 42 }), 42);
    }

    #[test]
    #[should_panic(expected = "future was pending when it must be ready")]
    fn must_ready_panics_when_the_future_is_pending() {
        must_ready(std::future::pending::<()>());
    }
}
