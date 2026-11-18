//! Deterministic fencing permissions for tests.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, mpsc};
use std::task::{Context, Waker};
use std::time::Duration;

use gang_clock::{TestClock, Tick};
use gang_spi::{Deadline, Fence, Held, RunError};
use gang_types::NonZeroDuration;

/// Creates a lease fence whose clock remains fixed and whose lease is valid indefinitely.
#[must_use]
pub fn valid_fence() -> Fence {
    let (fence, keeper) = Fence::lease(
        Arc::new(TestClock::default()),
        NonZeroDuration::from_secs(1),
    );
    keeper.extend_to(Tick::from_elapsed(Duration::MAX));
    fence
}

/// Runs a call with a permission that remains valid.
///
/// # Errors
///
/// Returns the fence or call error if the call fails.
pub async fn with_held<T, E>(
    call: impl AsyncFnOnce(&Held) -> Result<T, E>,
) -> Result<T, RunError<E>> {
    valid_fence().run(call).await
}

/// Runs a call after its permission has expired, while the lease still holds.
///
/// # Errors
///
/// Returns the fence or call error if the call fails or overruns.
pub async fn with_expired_held<T, E>(
    call: impl AsyncFnOnce(&Held) -> Result<T, E>,
) -> Result<T, RunError<E>> {
    let clock = Arc::new(TestClock::default());
    let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
    keeper.extend_to(Tick::from_elapsed(Duration::from_secs(10)));
    fence
        .run(async |held| {
            clock.advance(Duration::from_secs(1));
            call(held).await
        })
        .await
}

/// Keeps a fenced call alive for as long as its test deadline is needed.
///
/// Dropping the scope cancels the call and invalidates all copies of its deadline.
pub struct DeadlineScope {
    // Retained solely to keep the call alive until the scope is dropped.
    _run: Pin<Box<dyn Future<Output = ()> + Send>>,
    deadline: Deadline,
    clock: Arc<TestClock>,
}

impl DeadlineScope {
    /// Creates a scope with a one-second call deadline and a ten-second lease.
    ///
    /// # Errors
    ///
    /// Returns an error if the fenced call cannot yield a valid deadline.
    pub fn try_new() -> io::Result<Self> {
        let clock = Arc::new(TestClock::default());
        let (fence, keeper) = Fence::lease(clock.clone(), NonZeroDuration::from_secs(1));
        keeper.extend_to(Tick::from_elapsed(Duration::from_secs(10)));
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut run: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            let _keeper = keeper;
            let _ = fence
                .run(async |held| {
                    sender.send(held.deadline()).map_err(io::Error::other)?;
                    std::future::pending::<io::Result<()>>().await
                })
                .await;
        });
        let mut context = Context::from_waker(Waker::noop());
        if run.as_mut().poll(&mut context).is_ready() {
            return Err(io::Error::other(
                "fenced test call completed before yielding its deadline",
            ));
        }
        let deadline = receiver.try_recv().map_err(io::Error::other)?;
        deadline.check().map_err(io::Error::other)?;
        Ok(Self {
            _run: run,
            deadline,
            clock,
        })
    }

    /// Returns the deadline owned by this scope.
    #[must_use]
    pub fn deadline(&self) -> &Deadline {
        &self.deadline
    }

    /// Returns the clock used by the deadline and lease.
    #[must_use]
    pub fn clock(&self) -> &Arc<TestClock> {
        &self.clock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_scope_cancels_cloned_deadline() -> io::Result<()> {
        let scope = DeadlineScope::try_new()?;
        let deadline = scope.deadline().clone();
        assert!(deadline.check().is_ok());
        drop(scope);
        assert!(deadline.check().is_err());
        Ok(())
    }

    #[test]
    fn advancing_clock_expires_deadline() -> io::Result<()> {
        let scope = DeadlineScope::try_new()?;
        assert!(scope.deadline().check().is_ok());
        scope.clock().advance(Duration::from_secs(1));
        assert!(scope.deadline().check().is_err());
        Ok(())
    }
}
