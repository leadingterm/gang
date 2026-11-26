//! Bounded first-creator identity memo and event-time completion kernels.
//!
//! Ported from `crates/gang-nativelink/src/source_rates.rs` as a pure, single-owner state machine
//! taking explicit `Tick`s. Scheduler-generated drops are deliberately not completions, and
//! evicting a source can lose unsampled events; both effects bias service estimates conservatively.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::ScalingConfig;
use gang_spi::{CompletionRates, NonNegativeFinite, ResourceRates};
use gang_types::{ActionResources, SourceId};

use crate::sketch::LruMap;

/// Errors constructing [`SourceRates`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RatesError {
    /// Configured `max_sources * fleets` exceeds safe memory bounds.
    #[error("source rate table is too large")]
    TableTooLarge,
}

/// Event-time exponential completion-rate and resource-work estimator per source and preferred
/// fleet.
#[derive(Debug)]
pub struct SourceRates {
    memo: LruMap<String, SourceId>,
    sources: LruMap<SourceId, usize>,
    slots: Vec<Slot>,
    sorted: Vec<usize>,
    fleets: usize,
    kappa: f64,
    generation: u64,
}

#[derive(Debug)]
struct Slot {
    source: Option<SourceId>,
    generation: u64,
    cells: Vec<Option<CompletionRates>>,
}

impl SourceRates {
    /// Constructs a bounded completion-rate table for `fleets` fleets under `config`.
    ///
    /// # Errors
    ///
    /// Returns [`RatesError::TableTooLarge`] when `max_sources` and `fleets` would overflow safe
    /// allocation bounds.
    #[allow(
        clippy::cast_precision_loss,
        reason = "exponential decay constant intentionally converts half-life seconds to f64"
    )]
    pub fn new(config: &ScalingConfig, fleets: usize) -> Result<Self, RatesError> {
        let capacity: NonZeroUsize = config.max_sources.into();
        let count = capacity
            .get()
            .checked_add(1)
            .filter(|count| {
                count
                    .checked_mul(fleets)
                    .and_then(|cells| {
                        cells.checked_mul(std::mem::size_of::<Option<CompletionRates>>())
                    })
                    .is_some_and(|bytes| bytes <= usize::MAX / 2)
            })
            .ok_or(RatesError::TableTooLarge)?;
        let slots = (0..count)
            .map(|_| Slot {
                source: None,
                cells: vec![None; fleets],
                generation: 0,
            })
            .collect();
        let mut sorted = Vec::with_capacity(count);
        sorted.push(0);
        Ok(Self {
            memo: LruMap::new(capacity),
            sources: LruMap::new(capacity),
            slots,
            sorted,
            fleets,
            kappa: std::f64::consts::LN_2 / config.load_half_life.get().as_secs_f64(),
            generation: 0,
        })
    }

    /// Resolves and memoizes a `provider:principal` identity string into a [`SourceId`].
    ///
    /// Holding queued or running work refreshes an existing rate slot's recency without
    /// allocating a new slot. Warm hits allocate nothing.
    pub fn source(&mut self, identity: Option<&str>) -> Option<SourceId> {
        let identity = identity?;
        let source = if let Some(source) = self.memo.get(identity) {
            *source
        } else {
            let source = SourceId::from_identity_str(identity)?;
            self.memo.put(identity.to_owned(), source);
            source
        };
        // Holding queued/running work protects an existing kernel, but never creates one.
        self.sources.get(&source);
        Some(source)
    }

    /// Records a verified action completion at monotonic `now`.
    #[allow(
        clippy::cast_precision_loss,
        reason = "resource-time moments intentionally use floating estimates rather than integer truncation"
    )]
    pub fn record(
        &mut self,
        source: Option<SourceId>,
        preferred_fleet: usize,
        resources: ActionResources,
        timing: Option<Duration>,
        now: Tick,
    ) {
        if preferred_fleet >= self.fleets {
            return;
        }
        let index = if let Some(source) = source {
            if let Some(index) = self.sources.get(&source) {
                *index
            } else {
                let index = if self.sources.len() == self.sources.cap().get() {
                    let Some((_, index)) = self.sources.pop_lru() else {
                        return;
                    };
                    self.sorted.retain(|slot| *slot != index);
                    index
                } else {
                    self.sources.len() + 1
                };
                self.slots[index].source = Some(source);
                self.slots[index].cells.fill(None);
                self.sources.put(source, index);
                let position = self
                    .sorted
                    .partition_point(|&slot| self.slots[slot].source < Some(source));
                self.sorted.insert(position, index);
                self.generation = self.generation.wrapping_add(1);
                self.slots[index].generation = self.generation;
                index
            }
        } else {
            0
        };
        let generation = self.slots[index].generation;
        let cell = &mut self.slots[index].cells[preferred_fleet];
        let row = cell.get_or_insert_with(|| CompletionRates {
            source,
            preferred_fleet,
            generation,
            created_at: now,
            at: now,
            completions: NonNegativeFinite::default(),
            timed_completions: NonNegativeFinite::default(),
            work: ResourceRates::default(),
        });
        let decay = (-self.kappa * now.saturating_duration_since(row.at).as_secs_f64()).exp();
        row.completions = moment(row.completions, decay, self.kappa);
        let timed = timing.map_or(0.0, |_| self.kappa);
        row.timed_completions = moment(row.timed_completions, decay, timed);
        let work = timing.map_or(0.0, |duration| self.kappa * duration.as_secs_f64());
        row.work.actions = moment(row.work.actions, decay, work);
        row.work.cpu_millicores = moment(
            row.work.cpu_millicores,
            decay,
            work * resources.cpu_millicores as f64,
        );
        row.work.memory = moment(row.work.memory, decay, work * resources.memory.get() as f64);
        row.work.disk = moment(row.work.disk, decay, work * resources.disk.get() as f64);
        row.at = now;
    }

    /// Writes all populated rows sorted by `(source, preferred_fleet)` into `into`, reusing its
    /// allocation.
    pub fn snapshot_into(&self, into: &mut Vec<CompletionRates>) {
        into.clear();
        for &index in &self.sorted {
            into.extend(self.slots[index].cells.iter().flatten().copied());
        }
    }
}

fn moment(previous: NonNegativeFinite, decay: f64, impulse: f64) -> NonNegativeFinite {
    // Finite nonnegative operands, with overflow saturated rather than exported as infinity.
    NonNegativeFinite::try_new((previous.get() * decay + impulse).min(f64::MAX)).unwrap_or_default()
}

/// Extracts a valid execution duration from worker start and completion timestamps.
///
/// Returns `None` if either timestamp is the Unix epoch (unset) or if completion precedes start.
#[must_use]
pub fn execution_timing(start: Timestamp, completed: Timestamp) -> Option<Duration> {
    if start == Timestamp::UNIX_EPOCH || completed == Timestamp::UNIX_EPOCH {
        return None;
    }
    completed
        .as_unix_millis()
        .checked_sub(start.as_unix_millis())
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::Bytes;

    fn rates(max: usize, fleets: usize) -> SourceRates {
        let config = ScalingConfig {
            max_sources: gang_config::SourceLimit::try_new(max).expect("bounded limit"),
            ..Default::default()
        };
        SourceRates::new(&config, fleets).expect("bounded table")
    }

    fn resources() -> ActionResources {
        ActionResources {
            cpu_millicores: 2000,
            memory: Bytes::new(4000),
            disk: Bytes::new(8000),
        }
    }

    #[test]
    fn memo_is_first_creator_metadata_independent_and_rejects_invalid() {
        let mut rates = rates(2, 2);
        let a = rates.source(Some("local:alice"));
        assert!(a.is_some());
        assert_ne!(a, rates.source(Some("local:bob")));
        assert_ne!(a, rates.source(Some("oidc:alice")));
        assert_eq!(rates.source(None), None);
        assert_eq!(rates.source(Some("")), None);
        assert_eq!(rates.source(Some("local:alice")), a);
    }

    #[test]
    fn per_source_slots_keep_both_fleets_and_reserved_none() {
        let mut rates = rates(1, 2);
        let source = SourceId::from_identity_str("local:alice");
        let t0 = Tick::ORIGIN;
        let t1 = t0 + Duration::from_secs(1);
        rates.record(source, 1, resources(), Some(Duration::from_secs(2)), t0);
        rates.record(None, 0, resources(), None, t1);
        rates.record(source, 0, resources(), Some(Duration::ZERO), t1);
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].source, None);
        assert_eq!((rows[1].preferred_fleet, rows[2].preferred_fleet), (0, 1));
        assert_eq!(rows[1].generation, rows[2].generation);
        assert_eq!(rows[1].created_at, t1);
        assert_eq!(rows[2].created_at, t0);
        let old = rows[1].generation;
        rates.record(
            SourceId::from_identity_str("local:bob"),
            0,
            resources(),
            None,
            t1,
        );
        rates.snapshot_into(&mut rows);
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[1].generation, old);
        assert_eq!(rows[1].work, ResourceRates::default());
    }

    #[test]
    fn moments_use_event_time_raw_resources_and_read_invariance() {
        let mut rates = rates(2, 1);
        let t0 = Tick::ORIGIN + Duration::from_hours(4);
        rates.record(None, 0, resources(), Some(Duration::from_secs(2)), t0);
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        let first = rows[0];
        assert_eq!(first.at, t0);
        assert_eq!(first.created_at, t0);
        let kappa = std::f64::consts::LN_2 / 60.0;
        assert_eq!(first.completions.get().to_bits(), kappa.to_bits());
        assert_eq!(first.timed_completions.get().to_bits(), kappa.to_bits());
        assert_eq!(first.work.actions.get().to_bits(), (2.0 * kappa).to_bits());
        assert_eq!(
            first.work.cpu_millicores.get().to_bits(),
            (4000.0 * kappa).to_bits()
        );

        let t1 = t0 + Duration::from_secs(5);
        rates.record(None, 0, resources(), None, t1);
        rates.snapshot_into(&mut rows);
        let decay = (-kappa * 5.0).exp();
        assert_eq!(
            rows[0].completions.get().to_bits(),
            (kappa * decay + kappa).to_bits()
        );
        assert_eq!(
            rows[0].timed_completions.get().to_bits(),
            (kappa * decay).to_bits()
        );
        assert_eq!(
            rows[0].work.memory.get().to_bits(),
            (8000.0 * kappa * decay).to_bits()
        );
    }

    #[test]
    fn memo_eviction_does_not_reset_moments_and_activity_protects_slots() {
        let mut rates = rates(2, 1);
        let a = rates.source(Some("local:alice"));
        let b = rates.source(Some("local:bob"));
        rates.record(a, 0, resources(), None, Tick::ORIGIN);
        rates.record(b, 0, resources(), None, Tick::ORIGIN);
        rates.source(Some("local:charlie"));
        rates.source(Some("local:alice"));
        rates.record(
            SourceId::from_identity_str("local:dave"),
            0,
            resources(),
            None,
            Tick::ORIGIN,
        );
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert!(rows.iter().any(|row| row.source == a));
        assert!(!rows.iter().any(|row| row.source == b));
    }

    #[test]
    fn invalid_timestamps_are_untimed_but_equal_is_valid() {
        let t = Timestamp::from_unix_millis(10_000);
        let earlier = Timestamp::from_unix_millis(9_000);
        assert_eq!(
            execution_timing(Timestamp::UNIX_EPOCH, Timestamp::UNIX_EPOCH),
            None
        );
        assert_eq!(execution_timing(t, Timestamp::UNIX_EPOCH), None);
        assert_eq!(execution_timing(Timestamp::UNIX_EPOCH, t), None);
        assert_eq!(execution_timing(t, earlier), None);
        assert_eq!(execution_timing(t, t), Some(Duration::ZERO));
        assert_eq!(execution_timing(earlier, t), Some(Duration::from_secs(1)));
    }

    #[test]
    fn rows_are_unique_and_sorted_independent_of_source_arrival_order() {
        let mut rates = rates(3, 2);
        let mut sources = ["local:alice", "local:bob", "local:charlie"]
            .map(|identity| SourceId::from_identity_str(identity).expect("valid identity"));
        sources.sort_unstable();
        for &source in sources.iter().rev() {
            rates.record(Some(source), 1, resources(), None, Tick::ORIGIN);
            rates.record(Some(source), 0, resources(), None, Tick::ORIGIN);
        }
        rates.record(None, 1, resources(), None, Tick::ORIGIN);
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        let mut expected = vec![(None, 1)];
        for source in sources {
            expected.extend([(Some(source), 0), (Some(source), 1)]);
        }
        assert_eq!(
            rows.iter()
                .map(|row| (row.source, row.preferred_fleet))
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn memo_is_bounded_and_invalid_origins_are_not_retained() {
        let mut rates = rates(1, 1);
        rates.source(Some("local:alice"));
        rates.source(Some("local:bob"));
        rates.source(Some("invalid"));
        rates.source(Some(""));
        assert_eq!(rates.memo.len(), 1);
        assert!(rates.memo.peek("local:bob").is_some());
        assert!(rates.memo.peek("local:alice").is_none());
    }

    #[test]
    fn large_resource_times_remain_finite_and_allocation_sizes_are_checked() {
        let mut rates = rates(1, 1);
        rates.record(
            None,
            0,
            ActionResources {
                cpu_millicores: u64::MAX,
                memory: Bytes::new(u64::MAX),
                disk: Bytes::new(u64::MAX),
            },
            Some(Duration::MAX),
            Tick::ORIGIN,
        );
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert!(rows[0].work.actions.get() > 0.0);
        assert!(rows[0].work.cpu_millicores.get() > rows[0].work.actions.get());
        let max = NonNegativeFinite::try_new(f64::MAX).expect("finite");
        assert_eq!(moment(max, 1.0, f64::MAX), max);
        assert_eq!(
            SourceRates::new(&ScalingConfig::default(), usize::MAX).err(),
            Some(RatesError::TableTooLarge)
        );
    }
}
