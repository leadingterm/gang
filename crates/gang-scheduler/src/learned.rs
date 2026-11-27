//! Controller-term-spanning learned state bundle (`§5.6`, `§6.1`).
//!
//! [`Learned`] owns the controller's [`Histories`] ([`ResourceSketch`] + [`ActionDurations`]) and
//! [`SourceRates`] so it can be constructed prior to a [`Scheduler`](crate::Scheduler) term,
//! restored from persisted records, driven synchronously during execution, and returned via
//! [`Scheduler::into_learned`](crate::Scheduler::into_learned) for post-drain persistence flushes.

use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_config::{ActionsConfig, ScalingConfig};
use gang_spi::LearnedHistory;

use crate::durations::ActionDurations;
use crate::history::Histories;
use crate::rates::{RatesError, SourceRates};
use crate::sketch::{Budget, Policy, ResourceSketch, SketchError};

/// Learned resource estimates, action durations, and source completion rates (`§5.1`, `§6.1`).
#[derive(Debug)]
pub struct Learned {
    histories: Histories,
    rates: SourceRates,
}

impl Learned {
    /// Constructs a fresh [`Learned`] bundle sized from `actions`, `scaling`, and `fleet_count`.
    ///
    /// # Errors
    ///
    /// Returns [`RatesError::TableTooLarge`] when `scaling.max_sources * fleet_count` exceeds safe
    /// memory bounds.
    pub fn new(
        actions: &ActionsConfig,
        scaling: &ScalingConfig,
        fleet_count: usize,
    ) -> Result<Self, RatesError> {
        let policy = Policy {
            headroom_percent: u64::from(actions.reservation_headroom_percent.get()),
            cpu_reservation_percentile: actions.cpu_reservation_percentile,
            classes: Vec::new(),
            reserve_cpu: true,
            reserve_memory: true,
            reserve_disk: true,
        };
        let budget = Budget::from_bytes(actions.history_memory_budget.as_u64());
        let resource = ResourceSketch::new(policy, budget, actions.history_window.get());
        let durations = ActionDurations::new(actions.history_keys, actions.history_window);
        let histories = Histories::new(resource, durations);
        let rates = SourceRates::new(scaling, fleet_count)?;
        Ok(Self { histories, rates })
    }

    /// Assembles a [`Learned`] bundle from prebuilt [`Histories`] and [`SourceRates`].
    #[must_use]
    pub const fn from_parts(histories: Histories, rates: SourceRates) -> Self {
        Self { histories, rates }
    }

    /// Consumes this bundle into its inner [`Histories`] and [`SourceRates`].
    #[must_use]
    pub fn into_parts(self) -> (Histories, SourceRates) {
        (self.histories, self.rates)
    }

    /// Returns a shared reference to [`Histories`].
    #[must_use]
    pub const fn histories(&self) -> &Histories {
        &self.histories
    }

    /// Returns a mutable reference to [`Histories`].
    pub const fn histories_mut(&mut self) -> &mut Histories {
        &mut self.histories
    }

    /// Returns a shared reference to [`SourceRates`].
    #[must_use]
    pub const fn rates(&self) -> &SourceRates {
        &self.rates
    }

    /// Returns a mutable reference to [`SourceRates`].
    pub const fn rates_mut(&mut self) -> &mut SourceRates {
        &mut self.rates
    }

    /// Mutably borrows [`ActionDurations`] and [`SourceRates`] simultaneously.
    pub const fn durations_and_rates_mut(&mut self) -> (&mut ActionDurations, &mut SourceRates) {
        (self.histories.durations_mut(), &mut self.rates)
    }

    /// Maximum encoded body size in bytes for one [`LearnedHistory`] record (`§5.6`).
    #[must_use]
    pub fn byte_limit(&self) -> NonZeroUsize {
        self.histories.byte_limit()
    }

    /// Snapshots all live, non-seeded resource and duration records into `into`, clearing `into`
    /// first (`§5.6`, `§6.1`).
    pub fn snapshot(&self, now: Timestamp, into: &mut Vec<LearnedHistory>) {
        self.histories.snapshot(now, into);
    }

    /// Restores compatible live resource and duration records, discarding expired or mismatched
    /// entries (`§5.6`, `§6.1`).
    ///
    /// # Errors
    ///
    /// Returns [`SketchError`] if [`ResourceSketch::load`] rejects the validated batch.
    pub fn restore(
        &mut self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), SketchError> {
        self.histories.restore(now, records)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::Tick;
    use gang_types::{ActionResources, Bytes, SourceId};

    use super::*;
    use crate::sketch::{Observation, SketchKey};

    fn ts(secs: u64) -> Timestamp {
        Timestamp::from_unix_millis(secs * 1_000)
    }

    #[test]
    fn learned_snapshots_restores_and_respects_byte_limit() {
        let actions = ActionsConfig::default();
        let scaling = ScalingConfig::default();
        let mut learned = Learned::new(&actions, &scaling, 2).expect("learned");
        assert!(learned.byte_limit().get() > 9);

        let key = SketchKey::for_attempt(Some("//app:lib"), Some("Rustc"), "");
        assert!(learned.histories_mut().resource_mut().observe(
            key.clone(),
            &Observation {
                peak_memory_kb: 512_000,
                cpu_millicores: Some(2_000),
                peak_disk_kb: 64_000,
                now_s: 10,
                ..Observation::default()
            }
        ));
        learned
            .histories_mut()
            .durations_mut()
            .record("//app:lib", "Rustc", 250, ts(10));
        let source = SourceId::from_identity_str("local:alice");
        learned.rates_mut().record(
            source,
            0,
            ActionResources {
                cpu_millicores: 2_000,
                memory: Bytes::mib(500),
                disk: Bytes::mib(64),
            },
            Some(Duration::from_millis(250)),
            Tick::ORIGIN + Duration::from_secs(10),
        );

        let mut records = Vec::new();
        learned.snapshot(ts(11), &mut records);
        assert_eq!(records.len(), 3);
        assert!(
            records
                .iter()
                .all(|record| record.body.len() <= learned.byte_limit().get())
        );

        let mut restored = Learned::new(&actions, &scaling, 2).expect("restored");
        restored.restore(ts(12), &records).expect("restore");
        assert!(
            restored
                .histories_mut()
                .resource_mut()
                .lookup(&key, 12)
                .is_some()
        );
        assert_eq!(
            restored
                .histories_mut()
                .durations_mut()
                .expected("//app:lib", "Rustc", ts(12)),
            Some(Duration::from_millis(250))
        );
    }
}
