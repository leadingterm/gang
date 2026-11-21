//! One deadline for cancellation-safe scheduler control preparation.

use std::future::Future;

use gang_clock::{Clock, Tick};
use gang_spi::PortError;
use gang_types::NonZeroDuration;

use crate::errors::SchedulerAcquireTimeout;

pub(crate) struct Acquisition<'a> {
    clock: &'a dyn Clock,
    deadline: Tick,
}

impl<'a> Acquisition<'a> {
    pub(crate) fn new(clock: &'a dyn Clock, bound: NonZeroDuration) -> Self {
        Self {
            clock,
            deadline: clock.monotonic() + bound.get(),
        }
    }

    /// Only use this for cancellation-safe acquisition or read-only preparation.
    pub(crate) async fn wait<F: Future>(&self, future: F) -> Result<F::Output, PortError> {
        if self.clock.monotonic() >= self.deadline {
            return Err(PortError::unavailable(SchedulerAcquireTimeout));
        }
        let value = Clock::timeout_at(&self.clock, self.deadline, future)
            .await
            .map_err(|_| PortError::unavailable(SchedulerAcquireTimeout))?;
        if self.clock.monotonic() >= self.deadline {
            return Err(PortError::unavailable(SchedulerAcquireTimeout));
        }
        Ok(value)
    }
}

#[cfg(test)]
pub(crate) fn test_acquisition() -> &'static Acquisition<'static> {
    static ACQUISITION: std::sync::LazyLock<Acquisition<'static>> =
        std::sync::LazyLock::new(|| {
            static CLOCK: std::sync::LazyLock<gang_clock::TestClock> =
                std::sync::LazyLock::new(gang_clock::TestClock::default);
            Acquisition::new(&*CLOCK, NonZeroDuration::from_secs(1))
        });
    &ACQUISITION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scratch_lock_is_bounded_and_retryable() {
        let clock = gang_clock::TestClock::default();
        let bound = NonZeroDuration::from_secs(1);
        let scratch = tokio::sync::Mutex::new(crate::snapshot::Scratch::default());
        let held = scratch.lock().await;
        let acquisition = Acquisition::new(&clock, bound);
        let read = acquisition.wait(scratch.lock());
        tokio::pin!(read);
        assert!(futures::poll!(read.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(matches!(read.await, Err(PortError::Unavailable(_))));
        drop(held);
        let acquisition = Acquisition::new(&clock, bound);
        assert!(acquisition.wait(scratch.lock()).await.is_ok());
    }
}
