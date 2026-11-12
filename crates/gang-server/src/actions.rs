//! The scheduler's actions, sampled on an interval so that a state request never waits on the
//! scheduler.

use std::convert::Infallible;
use std::sync::Arc;

use gang_clock::{Clock, Interval, Timestamp};
use gang_spi::{ActionsView, ListingLimits, PortError, SchedulerPort};
use gang_state::ActionsSample;
use gang_types::NonZeroDuration;
use parking_lot::Mutex;

/// The latest actions sample the refresher took. Clones share one sample.
#[derive(Clone, Default)]
pub(crate) struct ActionsCache(Arc<Mutex<Option<ActionsSample>>>);

impl ActionsCache {
    /// Calls `f` with the latest sample, or `None` before the first successful refresh. The
    /// sample stays locked while `f` runs, so `f` should not block.
    #[must_use]
    pub(crate) fn read<R>(&self, f: impl FnOnce(Option<&ActionsSample>) -> R) -> R {
        f(self.0.lock().as_ref())
    }

    /// Makes `scratch` the latest sample, taken at `at`, and leaves the previous sample in
    /// `scratch` as the buffer for the next refresh. Allocates nothing.
    fn install(&self, scratch: &mut ActionsSample, at: Timestamp) {
        scratch.at = at;
        let mut slot = self.0.lock();
        let current = slot.get_or_insert_with(|| ActionsSample {
            at,
            view: ActionsView::default(),
        });
        std::mem::swap(current, scratch);
    }
}

/// Refreshes an [`ActionsCache`] from the scheduler once per interval, whether or not anyone is
/// viewing the state.
pub(crate) struct ActionsRefresher<'a, S, C> {
    scheduler: &'a S,
    clock: &'a C,
    cache: ActionsCache,
    limits: ListingLimits,
    interval: NonZeroDuration,
    /// The buffers of the next refresh; after a swap they hold the previous sample.
    scratch: ActionsSample,
}

impl<'a, S: SchedulerPort, C: Clock> ActionsRefresher<'a, S, C> {
    /// A refresher that asks `scheduler` for the actions within `limits` every `interval`, and
    /// stores each answer in `cache`.
    #[must_use]
    pub(crate) fn new(
        scheduler: &'a S,
        clock: &'a C,
        cache: ActionsCache,
        limits: ListingLimits,
        interval: NonZeroDuration,
    ) -> Self {
        Self {
            scheduler,
            clock,
            cache,
            limits,
            interval,
            scratch: ActionsSample {
                at: clock.wall(),
                view: ActionsView::default(),
            },
        }
    }

    /// Refreshes the cache at every tick, starting at once. A failed refresh keeps the previous
    /// sample. Runs until dropped.
    pub(crate) async fn run(mut self) -> Infallible {
        let mut ticks = Interval::new(self.clock, self.clock.monotonic(), self.interval);
        loop {
            ticks.tick().await;
            self.refresh().await;
        }
    }

    async fn refresh(&mut self) {
        match self
            .scheduler
            .actions(self.limits, &mut self.scratch.view)
            .await
        {
            Ok(()) => self.cache.install(&mut self.scratch, self.clock.wall()),
            Err(error @ PortError::Unavailable(_)) => {
                tracing::debug!(%error, "actions refresh skipped: scheduler not serving");
            }
            Err(error) => tracing::warn!(%error, "actions refresh failed"),
        }
    }
}
