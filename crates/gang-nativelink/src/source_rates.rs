//! Bounded first-creator identity memo and event-time completion kernels.
//!
//! Scheduler-generated drops are deliberately not completions. Evicting a source can lose
//! events not yet sampled; both effects bias service estimates conservatively.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gang_clock::{Clock, Tick};
use gang_config::ScalingConfig;
use gang_spi::{CompletionRates, NonNegativeFinite, ResourceRates};
use gang_types::{ActionResources, SourceId};
use lru::LruCache;
use nativelink_error::{Code, Error, make_err};
use nativelink_util::action_messages::ExecutionMetadata;
use nativelink_util::origin_event::OriginMetadata;
use parking_lot::Mutex;

pub(crate) struct SourceRates {
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
}

struct State {
    memo: LruCache<String, SourceId>,
    sources: LruCache<SourceId, usize>,
    slots: Vec<Slot>,
    sorted: Vec<usize>,
    fleets: usize,
    kappa: f64,
    generation: u64,
}

struct Slot {
    source: Option<SourceId>,
    generation: u64,
    cells: Vec<Option<CompletionRates>>,
}

impl SourceRates {
    pub(crate) fn new(
        config: &ScalingConfig,
        fleets: usize,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
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
            .ok_or_else(|| make_err!(Code::ResourceExhausted, "source rate table is too large"))?;
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
            clock,
            state: Mutex::new(State {
                memo: LruCache::new(capacity),
                sources: LruCache::new(capacity),
                slots,
                sorted,
                fleets,
                kappa: std::f64::consts::LN_2 / config.load_half_life.get().as_secs_f64(),
                generation: 0,
            }),
        })
    }

    pub(crate) fn source(&self, origin: Option<&OriginMetadata>) -> Option<SourceId> {
        let identity = &origin?.identity;
        let mut state = self.state.lock();
        let source = if let Some(source) = state.memo.get(identity.as_str()) {
            *source
        } else {
            let source = SourceId::from_identity_str(identity)?;
            state.memo.put(identity.clone(), source);
            source
        };
        // Holding queued/running work protects an existing kernel, but never creates one.
        state.sources.get(&source);
        Some(source)
    }

    pub(crate) fn record(
        &self,
        source: Option<SourceId>,
        preferred_fleet: usize,
        resources: ActionResources,
        timing: Option<Duration>,
    ) {
        let mut state = self.state.lock();
        if preferred_fleet >= state.fleets {
            return;
        }
        // Receipt is sampled INSIDE the update lock, after all operation-reading awaits.
        let now = self.clock.monotonic();
        state.record(source, preferred_fleet, resources, timing, now);
    }

    pub(crate) fn snapshot_into(&self, into: &mut Vec<CompletionRates>) -> Tick {
        let state = self.state.lock();
        into.clear();
        for &index in &state.sorted {
            into.extend(state.slots[index].cells.iter().flatten().copied());
        }
        self.clock.monotonic()
    }
}

impl State {
    // Resource-time moments intentionally use floating estimates rather than integer truncation.
    #[allow(clippy::cast_precision_loss)]
    fn record(
        &mut self,
        source: Option<SourceId>,
        fleet: usize,
        resources: ActionResources,
        timing: Option<Duration>,
        now: Tick,
    ) {
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
        let cell = &mut self.slots[index].cells[fleet];
        let row = cell.get_or_insert_with(|| CompletionRates {
            source,
            preferred_fleet: fleet,
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
}

fn moment(previous: NonNegativeFinite, decay: f64, impulse: f64) -> NonNegativeFinite {
    // Finite nonnegative operands, with overflow saturated rather than exported as infinity.
    NonNegativeFinite::try_new((previous.get() * decay + impulse).min(f64::MAX)).unwrap_or_default()
}

pub(crate) fn timing(metadata: &ExecutionMetadata) -> Option<Duration> {
    let start = metadata.worker_start_timestamp;
    let end = metadata.worker_completed_timestamp;
    if start == SystemTime::UNIX_EPOCH || end == SystemTime::UNIX_EPOCH {
        return None;
    }
    end.duration_since(start).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::{TestClock, Timestamp};
    use gang_types::Bytes;

    fn rates(max: usize, fleets: usize) -> (SourceRates, Arc<TestClock>) {
        let config = ScalingConfig {
            max_sources: gang_config::SourceLimit::try_new(max).expect("bounded limit"),
            ..Default::default()
        };
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(100_000)));
        let table = SourceRates::new(&config, fleets, clock.clone()).expect("bounded table");
        (table, clock)
    }

    fn origin(identity: &str) -> OriginMetadata {
        OriginMetadata {
            identity: identity.to_owned(),
            bazel_metadata: None,
        }
    }

    fn resources() -> ActionResources {
        ActionResources {
            cpu_millicores: 2000,
            memory: Bytes::new(4000),
            disk: Bytes::new(8000),
        }
    }

    #[test]
    fn memo_is_first_creator_metadata_independent_and_warmed_allocation_free() {
        let (rates, _) = rates(2, 2);
        let alice = origin("local:alice");
        let bob = origin("local:bob");
        let a = rates.source(Some(&alice));
        assert!(a.is_some());
        assert_ne!(a, rates.source(Some(&bob)));
        assert_ne!(a, rates.source(Some(&origin("oidc:alice"))));
        assert_eq!(rates.source(None), None);
        assert_eq!(rates.source(Some(&origin(""))), None);
        rates.source(Some(&alice));
        let measured = allocation_counter::measure(|| {
            assert_eq!(rates.source(Some(&alice)), a);
        });
        assert_eq!(measured.count_total, 0);
    }

    #[test]
    fn per_source_slots_keep_both_fleets_and_reserved_none() {
        let (rates, clock) = rates(1, 2);
        let source = SourceId::from_identity_str("local:alice");
        rates.record(source, 1, resources(), Some(Duration::from_secs(2)));
        clock.advance(Duration::from_secs(1));
        rates.record(None, 0, resources(), None);
        rates.record(source, 0, resources(), Some(Duration::ZERO));
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].source, None);
        assert_eq!((rows[1].preferred_fleet, rows[2].preferred_fleet), (0, 1));
        assert_eq!(rows[1].generation, rows[2].generation);
        assert_eq!(rows[1].created_at, Tick::ORIGIN + Duration::from_secs(1));
        assert_eq!(rows[2].created_at, Tick::ORIGIN);
        let old = rows[1].generation;
        rates.record(
            SourceId::from_identity_str("local:bob"),
            0,
            resources(),
            None,
        );
        rates.snapshot_into(&mut rows);
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[1].generation, old);
        assert_eq!(rows[1].work, ResourceRates::default());
    }

    #[test]
    fn moments_use_event_time_raw_resources_and_read_invariance() {
        let (rates, clock) = rates(2, 1);
        clock.advance(Duration::from_hours(4));
        rates.record(None, 0, resources(), Some(Duration::from_secs(2)));
        let mut rows = Vec::new();
        let at = rates.snapshot_into(&mut rows);
        let first = rows[0];
        assert_eq!(first.at, at);
        assert_eq!(first.created_at, at);
        let kappa = std::f64::consts::LN_2 / 60.0;
        assert_eq!(first.completions.get().to_bits(), kappa.to_bits());
        assert_eq!(first.timed_completions.get().to_bits(), kappa.to_bits());
        assert_eq!(first.work.actions.get().to_bits(), (2.0 * kappa).to_bits());
        assert_eq!(
            first.work.cpu_millicores.get().to_bits(),
            (4000.0 * kappa).to_bits()
        );
        clock.advance(Duration::from_secs(1));
        rates.snapshot_into(&mut rows);
        assert_eq!(rows[0], first);
        clock.advance(Duration::from_secs(4));
        rates.snapshot_into(&mut rows);
        assert_eq!(rows[0], first);
        rates.record(None, 0, resources(), None);
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
    fn zero_delta_adds_and_snapshot_reuses_without_allocating() {
        let (rates, _) = rates(1, 1);
        let source = SourceId::from_identity_str("local:alice");
        rates.record(source, 0, resources(), Some(Duration::ZERO));
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        let kappa = rows[0].completions.get();
        let measured = allocation_counter::measure(|| {
            rates.record(source, 0, resources(), None);
            rates.snapshot_into(&mut rows);
        });
        assert_eq!(measured.count_total, 0);
        assert_eq!(rows[0].completions.get().to_bits(), (2.0 * kappa).to_bits());
        assert_eq!(rows[0].timed_completions.get().to_bits(), kappa.to_bits());
        assert_eq!(rows[0].work, ResourceRates::default());
    }

    #[test]
    fn memo_eviction_does_not_reset_moments_and_activity_protects_slots() {
        let (rates, _) = rates(2, 1);
        let alice = origin("local:alice");
        let a = rates.source(Some(&alice));
        let b = rates.source(Some(&origin("local:bob")));
        rates.record(a, 0, resources(), None);
        rates.record(b, 0, resources(), None);
        rates.source(Some(&origin("local:charlie")));
        rates.source(Some(&alice));
        rates.record(
            SourceId::from_identity_str("local:dave"),
            0,
            resources(),
            None,
        );
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert!(rows.iter().any(|row| row.source == a));
        assert!(!rows.iter().any(|row| row.source == b));
    }

    #[test]
    fn invalid_timestamps_are_untimed_but_equal_is_valid() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let mut metadata = ExecutionMetadata::default();
        assert_eq!(timing(&metadata), None);
        metadata.worker_start_timestamp = t;
        assert_eq!(timing(&metadata), None);
        metadata.worker_completed_timestamp = t - Duration::from_secs(1);
        assert_eq!(timing(&metadata), None);
        metadata.worker_completed_timestamp = t;
        assert_eq!(timing(&metadata), Some(Duration::ZERO));
    }
    #[test]
    fn rows_are_unique_and_sorted_independent_of_source_arrival_order() {
        let (rates, _) = rates(3, 2);
        let mut sources = ["local:alice", "local:bob", "local:charlie"]
            .map(|identity| SourceId::from_identity_str(identity).expect("valid identity"));
        sources.sort_unstable();
        for &source in sources.iter().rev() {
            rates.record(Some(source), 1, resources(), None);
            rates.record(Some(source), 0, resources(), None);
        }
        rates.record(None, 1, resources(), None);
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
        let (rates, _) = rates(1, 1);
        rates.source(Some(&origin("local:alice")));
        rates.source(Some(&origin("local:bob")));
        rates.source(Some(&origin("invalid")));
        rates.source(Some(&origin("")));
        assert_eq!(rates.state.lock().memo.len(), 1);
        assert!(rates.state.lock().memo.peek("local:bob").is_some());
        assert!(rates.state.lock().memo.peek("local:alice").is_none());
    }

    #[test]
    fn large_resource_times_remain_finite_and_allocation_sizes_are_checked() {
        let (rates, _) = rates(1, 1);
        rates.record(
            None,
            0,
            ActionResources {
                cpu_millicores: u64::MAX,
                memory: Bytes::new(u64::MAX),
                disk: Bytes::new(u64::MAX),
            },
            Some(Duration::MAX),
        );
        let mut rows = Vec::new();
        rates.snapshot_into(&mut rows);
        assert!(rows[0].work.actions.get() > 0.0);
        assert!(rows[0].work.cpu_millicores.get() > rows[0].work.actions.get());
        let max = NonNegativeFinite::try_new(f64::MAX).expect("finite");
        assert_eq!(moment(max, 1.0, f64::MAX), max);
        assert!(
            SourceRates::new(
                &ScalingConfig::default(),
                usize::MAX,
                Arc::new(TestClock::default()),
            )
            .is_err()
        );
    }
}
