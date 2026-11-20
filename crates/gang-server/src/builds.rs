//! Shared live builds and the observation recorder.

use std::convert::Infallible;
use std::sync::Arc;

use gang_clock::Clock;
use gang_config::BuildsConfig;
use gang_insight::Builds;
use gang_spi::{ObservationSink, Observations};
use parking_lot::Mutex;

/// The builds held by the recorder and read by the API and publisher.
#[derive(Clone)]
pub(crate) struct BuildsCell(Arc<Mutex<Builds>>);

impl BuildsCell {
    /// Creates an empty store with the configured bounds.
    pub(crate) fn new(config: &BuildsConfig) -> Self {
        Self(Arc::new(Mutex::new(Builds::new(config))))
    }

    /// Creates a store and the bounded channel feeding its recorder.
    pub(crate) fn channel(config: &BuildsConfig) -> (Self, ObservationSink, Observations) {
        let (sink, observations) = ObservationSink::channel(config.queue, config.max_live);
        (Self::new(config), sink, observations)
    }

    /// Reads the builds while holding the store lock.
    pub(crate) fn read<T>(&self, read: impl FnOnce(&Builds) -> T) -> T {
        read(&self.0.lock())
    }

    /// Folds a fixture observation at an explicit time.
    #[cfg(test)]
    pub(crate) fn observe(&self, at: gang_clock::Timestamp, observation: &gang_spi::Observation) {
        self.0.lock().observe(at, observation);
    }

    /// Folds observations and losses until the recorder is cancelled.
    pub(crate) async fn record<C: Clock>(
        &self,
        mut observations: Observations,
        clock: &C,
    ) -> Infallible {
        while let Some(delivery) = observations.next().await {
            if let Some(observation) = delivery.into_observation() {
                self.0.lock().observe(clock.wall(), &observation);
            }
            self.drain_lost(&observations, clock);
        }
        self.drain_lost(&observations, clock);
        std::future::pending().await
    }

    /// Folds all loss counts observed so far.
    fn drain_lost<C: Clock>(&self, observations: &Observations, clock: &C) {
        let at = clock.wall();
        let mut builds = self.0.lock();
        let unattributed = observations.take_lost(|key, count| builds.lose(at, &key, count));
        if unattributed != 0 {
            tracing::warn!(count = unattributed, "unattributed build observations lost");
        }
    }
}
