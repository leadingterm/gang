//! A served connection's standing: whether it has earned its place among the listener's
//! connections, and whether a request on it is in flight.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use gang_clock::Clock;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use tokio::sync::Notify;

/// Admission and retirement compete for the first decision on a connection.
#[repr(u8)]
enum Admission {
    Fresh = 0,
    Admitted = 1,
    Retrying = 2,
}

/// Whether a connection has earned its place, and how many of its requests are in flight.
#[derive(Debug, Default)]
pub(crate) struct Standing {
    /// Set once the gate admits an authenticated request on the connection; never cleared.
    proven: AtomicBool,
    /// Requests whose responses are not yet sent or dropped.
    in_flight: AtomicUsize,
    /// Set when a request reaches the gate, before its response can be buffered by Hyper.
    received_request: AtomicBool,
    /// Woken when proof or the request count changes.
    changed: Notify,
    /// The canonical first admission or retry decision.
    admission: AtomicU8,
    /// Explicit leadership retirement is independent of the first admission decision.
    leadership_retired: AtomicBool,
    /// Marks a pre-handoff connection for retirement if its pending admission later succeeds.
    handoff_pending: AtomicBool,
    /// Wakes the connection's sole retry waiter.
    retry: Notify,
}

impl Standing {
    /// Permits a request unless retirement won before any request was admitted.
    pub(crate) fn admit(&self) -> bool {
        let admitted = match self.admission.compare_exchange(
            Admission::Fresh as u8,
            Admission::Admitted as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(state) => state == Admission::Admitted as u8,
        };
        if admitted && self.handoff_pending.load(Ordering::Acquire) {
            self.retire_leadership();
        }
        admitted
    }

    /// Requests graceful retirement only if no request has yet been admitted.
    pub(crate) fn request_retry(&self) {
        if self
            .admission
            .compare_exchange(
                Admission::Fresh as u8,
                Admission::Retrying as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.retry.notify_one();
        }
    }

    /// Retires the connection without preventing already accepted requests from admission.
    pub(crate) fn retire_leadership(&self) {
        self.leadership_retired.store(true, Ordering::Release);
        self.retry.notify_one();
    }

    /// Waits for the irreversible retry decision.
    pub(crate) async fn retry_requested(&self) {
        loop {
            let notified = self.retry.notified();
            if self.is_retiring() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn hand_off(&self) {
        self.handoff_pending.store(true, Ordering::Release);
        if self.is_admitted() {
            self.retire_leadership();
        }
    }

    fn is_retiring(&self) -> bool {
        self.leadership_retired.load(Ordering::Acquire)
            || self.admission.load(Ordering::Acquire) == Admission::Retrying as u8
    }

    pub(crate) fn is_admitted(&self) -> bool {
        self.admission.load(Ordering::Acquire) == Admission::Admitted as u8
    }

    /// Whether the gate has admitted an authenticated request on the connection.
    pub(crate) fn is_proven(&self) -> bool {
        self.proven.load(Ordering::Acquire)
    }

    /// Whether a request on the connection is in flight.
    pub(crate) fn is_busy(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) > 0
    }

    pub(crate) fn has_request(&self) -> bool {
        self.received_request.load(Ordering::Acquire)
    }

    /// Records that the gate admitted an authenticated request on the connection.
    pub(crate) fn prove(&self) {
        self.proven.store(true, Ordering::Release);
        self.changed.notify_one();
    }

    /// Counts a request in flight until the returned guard drops.
    pub(crate) fn begin(self: &Arc<Self>) -> Busy {
        self.received_request.store(true, Ordering::Release);
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_one();
        Busy(Arc::clone(self))
    }

    /// Completes once the connection has stayed unproven with no request in flight for `idle` of
    /// `clock`, counted afresh each time it becomes idle. Retirement is owned by graceful flush.
    pub(crate) async fn expired(&self, clock: &impl Clock, idle: Duration) {
        loop {
            let changed = self.changed.notified();
            if self.is_proven() || self.is_retiring() {
                return std::future::pending::<()>().await;
            }
            if self.is_busy() {
                changed.await;
            } else {
                tokio::select! {
                    () = clock.sleep(idle) => {
                        if !self.is_proven() && !self.is_retiring() && !self.is_busy() { return; }
                    },
                    () = changed => {}
                }
            }
        }
    }
}

/// A request in flight on a connection; dropping it ends the request.
#[derive(Debug)]
pub(crate) struct Busy(Arc<Standing>);

impl Drop for Busy {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

/// Keeps `busy` until the body of `response` has been sent or dropped, so a streamed response
/// counts as in flight.
pub(crate) fn hold(response: Response, busy: Busy) -> Response {
    response.map(|body| Body::new(Held { body, _busy: busy }))
}

/// A response body that holds its request's [`Busy`].
struct Held {
    /// The response body.
    body: Body,
    /// Ends the request when the body is dropped.
    _busy: Busy,
}

impl HttpBody for Held {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::{TestClock, Timestamp};

    /// The idle timeout used by the standing tests.
    const IDLE: Duration = Duration::from_secs(30);
    const TICK: Duration = Duration::from_millis(1);
    const BEFORE_IDLE: Duration = IDLE.checked_sub(TICK).unwrap();

    #[tokio::test(flavor = "current_thread")]
    async fn admission_prevents_retry() {
        let standing = Standing::default();
        let retry = standing.retry_requested();
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_pending());
        assert!(standing.admit());
        assert!(standing.admit());
        standing.request_retry();
        standing.request_retry();
        assert!(futures::poll!(&mut retry).is_pending());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_prevents_admission() {
        let standing = Standing::default();
        let retry = standing.retry_requested();
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_pending());
        standing.request_retry();
        standing.request_retry();
        assert!(futures::poll!(&mut retry).is_ready());
        assert!(!standing.admit());
        assert!(!standing.admit());
        let retry_again = standing.retry_requested();
        tokio::pin!(retry_again);
        assert!(futures::poll!(&mut retry_again).is_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn leadership_retirement_is_independent_of_admission() {
        for admitted_first in [false, true] {
            let standing = Standing::default();
            if admitted_first {
                assert!(standing.admit());
            }
            let retry = standing.retry_requested();
            tokio::pin!(retry);
            assert!(futures::poll!(&mut retry).is_pending());
            standing.retire_leadership();
            assert!(futures::poll!(&mut retry).is_ready());
            assert!(standing.admit());
            let later = standing.retry_requested();
            tokio::pin!(later);
            assert!(futures::poll!(&mut later).is_ready());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_unproven_idle_connection_expires_after_the_idle_timeout() {
        let standing = Standing::default();
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let expired = standing.expired(&clock, IDLE);
        tokio::pin!(expired);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(BEFORE_IDLE);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(TICK);
        assert!(futures::poll!(&mut expired).is_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_idle_timer_does_not_expire_a_request_that_began_before_repoll() {
        for notify_expiry in [false, true] {
            let standing = Arc::new(Standing::default());
            let clock = TestClock::new(Timestamp::UNIX_EPOCH);
            let changed = standing.changed.notified();
            tokio::pin!(changed);
            if !notify_expiry {
                // Notify is FIFO: reserve the request's wake so only the timer can win.
                // The other case leaves both select branches ready.
                assert!(futures::poll!(&mut changed).is_pending());
            }
            let expired = standing.expired(&clock, IDLE);
            tokio::pin!(expired);
            assert!(futures::poll!(&mut expired).is_pending());
            clock.advance(IDLE);
            let busy = standing.begin();
            if !notify_expiry {
                assert!(futures::poll!(&mut changed).is_ready());
            }
            assert!(
                futures::poll!(&mut expired).is_pending(),
                "a ready idle timer must recheck request ownership"
            );
            clock.advance(IDLE * 2);
            assert!(futures::poll!(&mut expired).is_pending());
            drop(busy);
            assert!(futures::poll!(&mut expired).is_pending());
            clock.advance(BEFORE_IDLE);
            assert!(futures::poll!(&mut expired).is_pending());
            clock.advance(TICK);
            assert!(futures::poll!(&mut expired).is_ready());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_request_in_flight_keeps_an_unproven_connection_and_idleness_restarts_after_it() {
        let standing = Arc::new(Standing::default());
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let busy = standing.begin();
        let expired = standing.expired(&clock, IDLE);
        tokio::pin!(expired);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(2 * IDLE);
        assert!(futures::poll!(&mut expired).is_pending());
        drop(busy);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(BEFORE_IDLE);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(TICK);
        assert!(futures::poll!(&mut expired).is_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_proven_connection_never_expires() {
        let standing = Standing::default();
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        standing.prove();
        let expired = standing.expired(&clock, IDLE);
        tokio::pin!(expired);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(10 * IDLE);
        assert!(futures::poll!(&mut expired).is_pending());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn holding_a_response_keeps_the_request_in_flight_until_its_body_drops() {
        let standing = Arc::new(Standing::default());
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let response = hold(Response::new(Body::empty()), standing.begin());
        let expired = standing.expired(&clock, IDLE);
        tokio::pin!(expired);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(2 * IDLE);
        assert!(futures::poll!(&mut expired).is_pending());
        drop(response);
        assert!(futures::poll!(&mut expired).is_pending());
        clock.advance(IDLE);
        assert!(futures::poll!(&mut expired).is_ready());
    }
}
