//! The fence on fleet changes. Every provisioner method that changes anything takes a [`Held`],
//! and only [`Fence::run`] lends one, for one call: while the leader lease holds for the whole
//! call, or while this process holds an exclusive lock that keeps every controller out. A read
//! has no `Held` to pass, so it cannot change a fleet. The call borrows its `Held` and cannot keep
//! it, and the fence drops the call at the permission's deadline. Work moved to a blocking
//! thread carries a [`Deadline`], registers with [`Deadline::begin_work`], and checks authority
//! before each change. Revocation waits for registered work before releasing the lease.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gang_clock::{Clock, Elapsed, Tick};
use gang_os::FileLock;
use gang_types::NonZeroDuration;
use parking_lot::Mutex;

use crate::PortError;

/// The leader lease no longer holds long enough for a fleet change.
#[derive(Debug, thiserror::Error)]
#[error("the leader lease lapsed; fleet changes wait for its renewal")]
pub struct LeaseLapsed;

impl From<LeaseLapsed> for PortError {
    fn from(lapsed: LeaseLapsed) -> Self {
        Self::unavailable(lapsed)
    }
}

/// Why [`Fence::run`] returned no value.
#[derive(Debug, thiserror::Error)]
pub enum RunError<E> {
    /// The lease did not outlast the call: it lapsed before the call could start, or while it
    /// ran. Whatever the provisioner wrapped a failure in, the lapse caused it.
    #[error(transparent)]
    Lapsed(#[from] LeaseLapsed),
    /// The call ran past its permission's deadline while the lease held, and was dropped.
    #[error("the provisioner call overran its permission's deadline")]
    Overran(#[source] Elapsed),
    /// The call failed while the lease held.
    #[error(transparent)]
    Failed(E),
}

impl<E: Into<PortError>> From<RunError<E>> for PortError {
    fn from(error: RunError<E>) -> Self {
        match error {
            RunError::Lapsed(lapsed) => lapsed.into(),
            RunError::Overran(elapsed) => Self::unavailable(elapsed),
            RunError::Failed(error) => error.into(),
        }
    }
}

/// Until when this process may change the fleets. Clones share it.
#[derive(Clone)]
pub struct Fence(Bound);

#[derive(Clone)]
enum Bound {
    Lease(Arc<Validity>),
    Exclusive(Arc<FileLock>),
}

struct Call {
    cancelled: AtomicBool,
    changed: tokio::sync::Notify,
}

struct LeaseState {
    until: Option<Tick>,
    revoked: bool,
    work: usize,
}

/// The leader lease's validity as the fence reads it.
struct Validity {
    state: Mutex<LeaseState>,
    clock: Arc<dyn Clock>,
    call_timeout: NonZeroDuration,
    lapsed: AtomicBool,
    changed: tokio::sync::Notify,
}

/// Extends the validity of a [`Fence::lease`] fence; only the lease's keeper holds one.
pub struct Keeper(Arc<Validity>);

/// Permission to change the fleets for one call. Only [`Fence::run`] makes one, and lends it to
/// the call it runs. A provisioner calls [`check`](Self::check) before each step that changes
/// something; work it moves to a blocking thread takes the [`deadline`](Self::deadline) instead.
///
/// Nothing else takes a permission:
///
/// ```compile_fail
/// fn take(fence: &gang_spi::Fence) {
///     let _ = fence.hold();
/// }
/// ```
///
/// and no call keeps one past its end:
///
/// ```compile_fail
/// fn keep(held: &gang_spi::Held) -> gang_spi::Held {
///     held.clone()
/// }
/// ```
pub struct Held(Deadline);

/// A call's shared authority, including cancellation and its lease or exclusive lock.
#[derive(Clone)]
pub struct Deadline {
    call: Arc<Call>,
    bound: Until,
}

#[derive(Clone)]
enum Until {
    Lease {
        validity: Arc<Validity>,
        at: Tick,
    },
    /// The lock stays held while the deadline lives.
    Exclusive {
        _lock: Arc<FileLock>,
    },
}

/// Registered work that must finish before a revoked lease can be released.
#[must_use]
pub struct Work {
    deadline: Deadline,
}

impl Fence {
    /// A fence on the leader lease that `clock` times: it holds at no time until its [`Keeper`]
    /// extends it, and admits a change only while the lease outlasts `call_timeout` from now.
    /// A zero call timeout would admit no change, so the type rules it out:
    ///
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// fn lease(clock: Arc<dyn gang_clock::Clock>) {
    ///     let _ = gang_spi::Fence::lease(clock, Duration::ZERO);
    /// }
    /// ```
    #[must_use]
    pub fn lease(clock: Arc<dyn Clock>, call_timeout: NonZeroDuration) -> (Self, Keeper) {
        let lease = Arc::new(Validity {
            state: Mutex::new(LeaseState {
                until: None,
                revoked: false,
                work: 0,
            }),
            clock,
            call_timeout,
            lapsed: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        });
        (Self(Bound::Lease(Arc::clone(&lease))), Keeper(lease))
    }

    /// A fence for `gang local down`, which keeps the local server out by holding `lock`: the
    /// server lock `gang local up` hands to the one server it starts on a state directory. It
    /// always holds and keeps `lock` held while it or a [`Deadline`] it lent lives. A server
    /// started any other way takes no such lock, so this fence fits only that single-server
    /// directory.
    #[must_use]
    pub fn exclusive(lock: FileLock) -> Self {
        Self(Bound::Exclusive(Arc::new(lock)))
    }

    /// Whether the fence holds now, read on its own clock. It takes no instant: a tick from
    /// another clock does not compare with the lease's.
    ///
    /// ```compile_fail
    /// fn holds(fence: &gang_spi::Fence, now: gang_clock::Tick) -> bool {
    ///     fence.holds_at(now)
    /// }
    /// ```
    #[must_use]
    pub fn holds(&self) -> bool {
        match &self.0 {
            Bound::Lease(lease) => lease.holds_at(lease.clock.monotonic()),
            Bound::Exclusive(_) => true,
        }
    }

    /// Permission for one change that ends within the lease's call timeout from now.
    fn hold(&self) -> Result<Held, LeaseLapsed> {
        let bound = match &self.0 {
            Bound::Lease(lease) => {
                let at = lease.clock.monotonic() + lease.call_timeout.get();
                let holds = lease.holds_at(at);
                if lease.lapsed.swap(!holds, Ordering::SeqCst) == holds {
                    if holds {
                        tracing::info!("fleet changes unfenced: the leader lease was renewed");
                    } else {
                        tracing::warn!("fleet changes fenced: the leader lease's validity lapsed");
                    }
                }
                if !holds {
                    return Err(LeaseLapsed);
                }
                Until::Lease {
                    validity: Arc::clone(lease),
                    at,
                }
            }
            Bound::Exclusive(lock) => Until::Exclusive {
                _lock: Arc::clone(lock),
            },
        };
        let call = Arc::new(Call {
            cancelled: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        });
        Ok(Held(Deadline { call, bound }))
    }

    /// Lends `call` a permission for one change, and drops the call at the permission's deadline.
    ///
    /// # Errors
    ///
    /// [`RunError::Lapsed`] when the lease does not outlast the call timeout, or no longer holds
    /// once the call failed or overran. [`RunError::Overran`] when the call ran past its deadline
    /// while the lease held; [`RunError::Failed`] with the call's error otherwise.
    pub async fn run<T, E>(
        &self,
        call: impl AsyncFnOnce(&Held) -> Result<T, E>,
    ) -> Result<T, RunError<E>> {
        let held = self.hold()?;
        match &held.0.bound {
            Until::Lease { validity, at } => {
                tokio::select! {
                    biased;
                    () = validity.revoked() => Err(LeaseLapsed.into()),
                    result = validity.clock.timeout_at(*at, call(&held)) => match result {
                        Ok(Ok(value)) => Ok(value),
                        _ if !self.holds() => Err(LeaseLapsed.into()),
                        Ok(Err(error)) => Err(RunError::Failed(error)),
                        Err(elapsed) => Err(RunError::Overran(elapsed)),
                    }
                }
            }
            Until::Exclusive { .. } => call(&held).await.map_err(RunError::Failed),
        }
    }
}

impl Validity {
    fn holds_at(&self, now: Tick) -> bool {
        let state = self.state.lock();
        !state.revoked && state.until.is_some_and(|until| now < until)
    }

    async fn revoked(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().revoked {
                return;
            }
            notified.await;
        }
    }
}

impl Keeper {
    /// Makes the lease valid until `until`, unless it has been revoked.
    pub fn extend_to(&self, until: Tick) {
        let mut state = self.0.state.lock();
        if !state.revoked {
            state.until = Some(until);
        }
        drop(state);
        self.0.changed.notify_waiters();
    }

    /// Permanently withdraws authority from this lease and all its calls.
    pub fn revoke(&self) {
        self.0.state.lock().revoked = true;
        self.0.changed.notify_waiters();
    }

    /// Waits for all registered work to finish. Revoke the lease before calling this.
    pub async fn quiesced(&self) {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.state.lock().work == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Held {
    /// Confirms that the permission still holds; call it before each step that changes
    /// something.
    ///
    /// # Errors
    ///
    /// [`LeaseLapsed`] if the call has ended or been cancelled, or if a lease-backed
    /// permission's deadline has passed or its lease has been revoked or no longer holds.
    pub fn check(&self) -> Result<(), LeaseLapsed> {
        self.0.check()
    }

    /// The permission's deadline, for work that leaves the call.
    #[must_use]
    pub fn deadline(&self) -> Deadline {
        self.0.clone()
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.call.cancelled.store(true, Ordering::Release);
        self.0.call.changed.notify_waiters();
    }
}

impl Deadline {
    /// Confirms that the permission still holds; call it before each step that changes
    /// something.
    ///
    /// # Errors
    ///
    /// [`LeaseLapsed`] if the call has ended or been cancelled, or if a lease-backed
    /// permission's deadline has passed or its lease has been revoked or no longer holds.
    pub fn check(&self) -> Result<(), LeaseLapsed> {
        if self.call.cancelled.load(Ordering::Acquire) {
            return Err(LeaseLapsed);
        }
        match &self.bound {
            Until::Lease { validity, at } => {
                let state = validity.state.lock();
                self.check_lease(validity, &state, *at)
            }
            Until::Exclusive { .. } => Ok(()),
        }
    }

    fn check_lease(
        &self,
        validity: &Validity,
        state: &LeaseState,
        at: Tick,
    ) -> Result<(), LeaseLapsed> {
        let now = validity.clock.monotonic();
        if self.call.cancelled.load(Ordering::Acquire)
            || state.revoked
            || state.until.is_none_or(|until| now >= until)
            || now >= at
        {
            Err(LeaseLapsed)
        } else {
            Ok(())
        }
    }

    /// Registers work against this authority so revocation can wait for it.
    ///
    /// # Errors
    /// Returns [`LeaseLapsed`] if the call or lease has already ended.
    pub fn begin_work(&self) -> Result<Work, LeaseLapsed> {
        match &self.bound {
            Until::Lease { validity, at } => {
                let mut state = validity.state.lock();
                self.check_lease(validity, &state, *at)?;
                state.work += 1;
            }
            Until::Exclusive { .. } => self.check()?,
        }
        Ok(Work {
            deadline: self.clone(),
        })
    }

    /// Waits until this call is cancelled, revoked, or reaches a time boundary.
    pub async fn invalidated(&self) {
        loop {
            let call_changed = self.call.changed.notified();
            tokio::pin!(call_changed);
            call_changed.as_mut().enable();
            match &self.bound {
                Until::Lease { validity, at } => {
                    let lease_changed = validity.changed.notified();
                    tokio::pin!(lease_changed);
                    lease_changed.as_mut().enable();
                    let boundary = {
                        let state = validity.state.lock();
                        if self.check_lease(validity, &state, *at).is_err() {
                            return;
                        }
                        state.until.map_or(*at, |until| until.min(*at))
                    };
                    tokio::select! {
                        () = &mut call_changed => {},
                        () = &mut lease_changed => {},
                        () = validity.clock.sleep_until(boundary) => {},
                    }
                }
                Until::Exclusive { .. } => {
                    if self.check().is_err() {
                        return;
                    }
                    call_changed.await;
                }
            }
        }
    }
}

impl Work {
    /// The authority under which this work was registered.
    #[must_use]
    pub fn deadline(&self) -> &Deadline {
        &self.deadline
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        if let Until::Lease { validity, .. } = &self.deadline.bound {
            let mut state = validity.state.lock();
            state.work -= 1;
            let quiescent = state.work == 0;
            drop(state);
            if quiescent {
                validity.changed.notify_waiters();
            }
        }
    }
}

impl fmt::Debug for Fence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Fence").finish_non_exhaustive()
    }
}

impl fmt::Debug for Keeper {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Keeper").finish_non_exhaustive()
    }
}

impl fmt::Debug for Held {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Held").finish_non_exhaustive()
    }
}

impl fmt::Debug for Deadline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Deadline").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::pending;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use gang_clock::{Clock, TestClock, Tick};
    use gang_os::FileLock;
    use gang_types::NonZeroDuration;

    use super::{Fence, PortError, RunError};

    #[tokio::test]
    async fn hold_refuses_before_the_first_extension() {
        let clock = Arc::new(TestClock::default());
        let (fence, _keeper) = Fence::lease(clock, NonZeroDuration::from_secs(2));
        let called = AtomicBool::new(false);
        let result = fence
            .run(async |_| {
                called.store(true, Ordering::SeqCst);
                Ok::<_, Infallible>(())
            })
            .await;
        assert!(matches!(result, Err(RunError::Lapsed(_))));
        assert!(!called.load(Ordering::SeqCst));
        assert!(!fence.holds());
    }

    #[tokio::test]
    async fn hold_admits_only_while_the_lease_outlasts_the_call_timeout() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        clock.advance(Duration::from_secs(7));
        assert!(fence.run(async |_| Ok::<_, Infallible>(())).await.is_ok());
        clock.advance(Duration::from_secs(1));
        assert!(matches!(
            fence.run(async |_| Ok::<_, Infallible>(())).await,
            Err(RunError::Lapsed(_))
        ));
    }

    #[tokio::test]
    async fn a_held_permission_lapses_at_its_deadline() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let result = fence
            .run(async |held| {
                assert!(held.check().is_ok());
                clock.advance(Duration::from_secs(2));
                assert!(held.check().is_err());
                Ok::<_, Infallible>(())
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn revoke_invalidates_live_call_and_cannot_be_extended() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let (tx, rx) = std::sync::mpsc::channel();
        let mut call = Box::pin(fence.run(async |held| {
            tx.send(held.deadline()).unwrap();
            pending::<Result<(), Infallible>>().await
        }));
        assert!(futures::poll!(call.as_mut()).is_pending());
        let deadline = rx.recv().unwrap();
        let mut invalidated = pin!(deadline.invalidated());
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        keeper.revoke();
        keeper.extend_to(clock.monotonic() + Duration::from_secs(100));
        assert!(!fence.holds());
        assert!(deadline.check().is_err());
        assert!(deadline.begin_work().is_err());
        assert!(futures::poll!(invalidated.as_mut()).is_ready());
        assert!(matches!(
            futures::poll!(call.as_mut()),
            Poll::Ready(Err(RunError::Lapsed(_)))
        ));
        assert!(fence.hold().is_err());
    }

    #[tokio::test]
    async fn quiescence_waits_for_every_registered_work_guard() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let held = fence.hold().unwrap();
        let first = held.deadline().begin_work().unwrap();
        let second = held.deadline().begin_work().unwrap();
        keeper.revoke();
        let mut quiesced = pin!(keeper.quiesced());
        assert!(futures::poll!(quiesced.as_mut()).is_pending());
        drop(first);
        assert!(futures::poll!(quiesced.as_mut()).is_pending());
        drop(second);
        assert!(futures::poll!(quiesced.as_mut()).is_ready());
    }

    #[tokio::test]
    async fn invalidated_wakes_on_call_cancellation() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let held = fence.hold().unwrap();
        let deadline = held.deadline();
        let mut invalidated = pin!(deadline.invalidated());
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        drop(held);
        assert!(futures::poll!(invalidated.as_mut()).is_ready());
    }

    #[tokio::test]
    async fn invalidated_tracks_lease_expiry_and_extension() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(10));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(20));
        let held = fence.hold().unwrap();
        let deadline = held.deadline();
        keeper.extend_to(clock.monotonic() + Duration::from_secs(2));
        let mut invalidated = pin!(deadline.invalidated());
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        keeper.extend_to(clock.monotonic() + Duration::from_secs(5));
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        clock.advance(Duration::from_secs(2));
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(invalidated.as_mut()).is_ready());
    }

    #[tokio::test]
    async fn invalidated_wakes_on_exclusive_call_cancellation() {
        let dir = tempfile::TempDir::new().unwrap();
        let lock = FileLock::try_exclusive(&dir.path().join("lock"))
            .unwrap()
            .unwrap();
        let fence = Fence::exclusive(lock);
        let held = fence.hold().unwrap();
        let deadline = held.deadline();
        let mut invalidated = pin!(deadline.invalidated());
        assert!(futures::poll!(invalidated.as_mut()).is_pending());
        drop(held);
        assert!(futures::poll!(invalidated.as_mut()).is_ready());
    }

    #[tokio::test]
    async fn returning_from_run_invalidates_its_deadlines() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock, NonZeroDuration::from_secs(1));
        keeper.extend_to(Tick::from_elapsed(Duration::from_secs(10)));

        let deadline = fence
            .run(async |held| Ok::<_, Infallible>(held.deadline()))
            .await
            .unwrap();

        assert!(deadline.check().is_err());
    }

    #[tokio::test]
    async fn dropping_a_pending_run_invalidates_its_deadlines() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock, NonZeroDuration::from_secs(1));
        keeper.extend_to(Tick::from_elapsed(Duration::from_secs(10)));

        let (tx, rx) = std::sync::mpsc::channel();
        let mut call = Box::pin(fence.run(async |held| {
            tx.send(held.deadline()).expect("receiver exists");
            pending::<Result<(), Infallible>>().await
        }));
        assert!(futures::poll!(call.as_mut()).is_pending());
        let deadline = rx.recv().expect("deadline exported");
        assert!(deadline.check().is_ok());

        drop(call);
        assert!(deadline.check().is_err());
    }

    #[tokio::test]
    async fn run_gives_up_at_the_call_timeout() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let mut call = pin!(fence.run(async |_| pending::<Result<(), PortError>>().await));
        assert!(matches!(futures::poll!(call.as_mut()), Poll::Pending));
        clock.advance(Duration::from_secs(2));
        assert!(matches!(
            futures::poll!(call.as_mut()),
            Poll::Ready(Err(RunError::Overran(_)))
        ));
    }

    #[tokio::test]
    async fn run_drops_a_pending_call_at_the_deadline_exactly() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        let mut call = pin!(fence.run(async |_| pending::<Result<(), Infallible>>().await));
        assert!(futures::poll!(call.as_mut()).is_pending());
        clock.advance(Duration::from_secs(2) - Duration::from_nanos(1));
        assert!(futures::poll!(call.as_mut()).is_pending());
        clock.advance(Duration::from_nanos(1));
        assert!(matches!(
            futures::poll!(call.as_mut()),
            Poll::Ready(Err(RunError::Overran(_)))
        ));
    }

    #[tokio::test]
    async fn run_reports_a_failure_after_the_lease_lapsed_as_the_lapse() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(Tick::from_elapsed(Duration::from_secs(10)));
        clock.advance(Duration::from_secs(7));
        let result = fence
            .run(async |_| {
                clock.advance(Duration::from_secs(4));
                Err::<(), _>(PortError::backend(std::io::Error::other(
                    "connection reset",
                )))
            })
            .await;
        assert!(matches!(result, Err(RunError::Lapsed(_))));
    }

    #[tokio::test]
    async fn run_reports_a_failure_while_the_lease_holds_as_it_is() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        clock.advance(Duration::from_secs(7));
        let result = fence
            .run(async |_| {
                Err::<(), _>(PortError::backend(std::io::Error::other(
                    "connection reset",
                )))
            })
            .await;
        assert!(
            matches!(result, Err(RunError::Failed(PortError::Backend(cause))) if cause.to_string() == "connection reset")
        );
    }

    #[tokio::test]
    async fn run_refuses_without_calling_once_lapsed() {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(2));
        keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
        clock.advance(Duration::from_secs(10));
        let called = AtomicBool::new(false);
        let result = fence
            .run(async |_| {
                called.store(true, Ordering::SeqCst);
                Ok::<_, Infallible>(())
            })
            .await;
        assert!(matches!(result, Err(RunError::Lapsed(_))));
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn an_exclusive_fence_always_holds() {
        let dir = tempfile::TempDir::new().unwrap();
        let lock = FileLock::try_exclusive(&dir.path().join("lock"))
            .unwrap()
            .unwrap();
        let fence = Fence::exclusive(lock);
        fence
            .run(async |held| {
                assert!(held.check().is_ok());
                Ok::<_, Infallible>(())
            })
            .await
            .unwrap();
        assert!(fence.holds());
    }
}
