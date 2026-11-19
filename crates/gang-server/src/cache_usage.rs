//! The leader's latest measurement of the cache storage the engine does not bound, fetched from
//! the cache provider outside the control pass.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{CacheStorePort, MeasuredUsage};
use parking_lot::Mutex;

use crate::leadership::Leading;

/// The provider's latest successful measurement, written by the sampler and read by the
/// publisher. Cloning it shares the same measurement.
#[derive(Debug, Clone, Default)]
pub(crate) struct CacheUsage(Arc<Mutex<Option<MeasuredUsage>>>);

impl CacheUsage {
    /// The latest successful measurement, if the provider has given one.
    pub(crate) fn get(&self) -> Option<MeasuredUsage> {
        *self.0.lock()
    }

    /// Once this controller leads, asks `cache` for its measurement at once and then every
    /// `step`. A failed fetch is logged and, like a fetch without a measurement, keeps the last
    /// one. Never completes.
    pub(crate) async fn sample<P: CacheStorePort, C: Clock>(
        &self,
        cache: &P,
        clock: &C,
        leading: &Leading,
        step: Duration,
    ) -> Infallible {
        leading.wait().await;
        loop {
            let started = clock.monotonic();
            match cache.measured_usage().await {
                Ok(Some(measured)) => *self.0.lock() = Some(measured),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    %error,
                    "cannot measure the cache storage; keeping the last measurement"
                ),
            }
            clock.sleep_until(started + step).await;
        }
    }
}
