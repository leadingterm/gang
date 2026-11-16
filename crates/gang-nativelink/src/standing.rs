//! A served connection's standing: whether it has earned its place among the listener's
//! connections, and whether a request on it is in flight.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use gang_clock::Clock;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use tokio::sync::Notify;

/// Whether a connection has earned its place, and how many of its requests are in flight.
#[derive(Debug, Default)]
pub(crate) struct Standing {
    /// Set once the gate admits an authenticated request on the connection; never cleared.
    proven: AtomicBool,
    /// Requests whose responses are not yet sent or dropped.
    in_flight: AtomicUsize,
    /// Woken on every change of the two above.
    changed: Notify,
}

impl Standing {
    /// Whether the gate has admitted an authenticated request on the connection.
    pub(crate) fn is_proven(&self) -> bool {
        self.proven.load(Ordering::Acquire)
    }

    /// Whether a request on the connection is in flight.
    pub(crate) fn is_busy(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) > 0
    }

    /// Records that the gate admitted an authenticated request on the connection.
    pub(crate) fn prove(&self) {
        self.proven.store(true, Ordering::Release);
        self.changed.notify_one();
    }

    /// Counts a request in flight until the returned guard drops.
    pub(crate) fn begin(self: &Arc<Self>) -> Busy {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_one();
        Busy(Arc::clone(self))
    }

    /// Completes once the connection has stayed unproven with no request in flight for `idle` of
    /// `clock`, counted afresh each time it becomes idle; never once it is proven.
    pub(crate) async fn expired(&self, clock: &impl Clock, idle: Duration) {
        loop {
            let changed = self.changed.notified();
            if self.is_proven() {
                return std::future::pending::<()>().await;
            }
            if self.is_busy() {
                changed.await;
            } else {
                tokio::select! {
                    () = clock.sleep(idle) => return,
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
