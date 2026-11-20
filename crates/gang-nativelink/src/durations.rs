use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::HistoryWindow;
use lru::LruCache;
use serde::{Deserialize, Serialize};

use crate::history::history_id;
use gang_spi::{LearnedHistory, PortError, RecordKey, RecordKind};

const FORMAT_VERSION: u32 = 1;
const SAMPLER_GENERATION: u32 = nativelink_scheduler::resource_sketch::SAMPLER_GENERATION;

#[derive(Serialize, Deserialize)]
struct StoredDuration {
    version: u32,
    sampler_generation: u32,
    window_ms: u64,
    key: Key,
    entry: Entry,
}

#[derive(Serialize)]
struct StoredDurationRef<'a> {
    version: u32,
    sampler_generation: u32,
    window_ms: u64,
    key: &'a Key,
    entry: &'a Entry,
}

impl Key {
    fn record_key(&self) -> RecordKey {
        let id = match self {
            Self::Target { mnemonic, target } => history_id(0, &[mnemonic, target]),
            Self::Mnemonic(mnemonic) => history_id(1, &[mnemonic]),
        };
        RecordKey {
            kind: RecordKind::DurationHistory,
            id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Key {
    Target { mnemonic: String, target: String },
    Mnemonic(String),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum KeyParts<'a> {
    Target { mnemonic: &'a str, target: &'a str },
    Mnemonic(&'a str),
}

trait KeyView {
    fn parts(&self) -> KeyParts<'_>;
}

impl KeyView for Key {
    fn parts(&self) -> KeyParts<'_> {
        match self {
            Self::Target { mnemonic, target } => KeyParts::Target { mnemonic, target },
            Self::Mnemonic(mnemonic) => KeyParts::Mnemonic(mnemonic),
        }
    }
}

impl KeyView for KeyParts<'_> {
    fn parts(&self) -> KeyParts<'_> {
        *self
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn KeyView + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn KeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn KeyView + '_ {}

impl<'a> Borrow<dyn KeyView + 'a> for Key {
    fn borrow(&self) -> &(dyn KeyView + 'a) {
        self
    }
}

impl ToOwned for dyn KeyView + '_ {
    type Owned = Key;

    fn to_owned(&self) -> Key {
        match self.parts() {
            KeyParts::Target { mnemonic, target } => Key::Target {
                mnemonic: mnemonic.to_owned(),
                target: target.to_owned(),
            },
            KeyParts::Mnemonic(mnemonic) => Key::Mnemonic(mnemonic.to_owned()),
        }
    }
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
struct Half {
    sum_ms: u64,
    count: u32,
}

impl Half {
    fn record(&mut self, slot_ms: u64) {
        if let (Some(sum_ms), Some(count)) =
            (self.sum_ms.checked_add(slot_ms), self.count.checked_add(1))
        {
            self.sum_ms = sum_ms;
            self.count = count;
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    period: u64,
    current: Half,
    previous: Half,
    last_seen_ms: u64,
}

impl Entry {
    fn new(period: u64) -> Self {
        Self {
            period,
            current: Half::default(),
            previous: Half::default(),
            last_seen_ms: 0,
        }
    }

    fn settle(&mut self, period: u64) {
        match period.saturating_sub(self.period) {
            0 => return,
            1 => self.previous = self.current,
            _ => self.previous = Half::default(),
        }
        self.current = Half::default();
        self.period = period;
    }

    fn expected(&self) -> Option<Duration> {
        let count = u64::from(self.current.count) + u64::from(self.previous.count);
        if count == 0 {
            return None;
        }
        let sum = u128::from(self.current.sum_ms) + u128::from(self.previous.sum_ms);
        // Each sample is a u64, so their mean cannot exceed u64::MAX.
        let mean = u64::try_from(sum / u128::from(count)).unwrap_or(u64::MAX);
        Some(Duration::from_millis(mean))
    }
}

pub(crate) struct ActionDurations {
    half_window_ms: u64,
    entries: LruCache<Key, Entry>,
}

impl ActionDurations {
    pub(crate) fn new(keys: NonZeroUsize, window: HistoryWindow) -> Self {
        Self {
            // The whole-day window is bounded in nanoseconds by u64.
            half_window_ms: window.get().as_secs() * 1_000 / 2,
            entries: LruCache::new(keys),
        }
    }

    pub(crate) fn record(&mut self, target: &str, mnemonic: &str, slot_ms: u64, now: Timestamp) {
        if mnemonic.is_empty() {
            return;
        }
        let period = now.as_unix_millis() / self.half_window_ms;
        if !target.is_empty() {
            self.record_key(
                KeyParts::Target { mnemonic, target },
                slot_ms,
                period,
                now.as_unix_millis(),
            );
        }
        self.record_key(
            KeyParts::Mnemonic(mnemonic),
            slot_ms,
            period,
            now.as_unix_millis(),
        );
    }

    fn record_key(&mut self, parts: KeyParts<'_>, slot_ms: u64, period: u64, now_ms: u64) {
        let query: &dyn KeyView = &parts;
        let entry = self
            .entries
            .get_or_insert_mut_ref(query, || Entry::new(period));
        entry.settle(period);
        entry.last_seen_ms = now_ms;
        entry.current.record(slot_ms);
    }

    pub(crate) fn expected(
        &mut self,
        target: &str,
        mnemonic: &str,
        now: Timestamp,
    ) -> Option<Duration> {
        if mnemonic.is_empty() {
            return None;
        }
        let period = now.as_unix_millis() / self.half_window_ms;
        if !target.is_empty()
            && let Some(duration) = self.expected_key(KeyParts::Target { mnemonic, target }, period)
        {
            return Some(duration);
        }
        self.expected_key(KeyParts::Mnemonic(mnemonic), period)
    }

    fn expected_key(&mut self, parts: KeyParts<'_>, period: u64) -> Option<Duration> {
        let query: &dyn KeyView = &parts;
        let entry = self.entries.get_mut(query)?;
        entry.settle(period);
        entry.expected()
    }

    pub(crate) fn snapshot(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        let window_ms = self.half_window_ms * 2;
        for (key, entry) in self.entries.iter().rev() {
            let expires = entry.last_seen_ms.saturating_add(window_ms);
            let mut settled = entry.clone();
            settled.settle(now.as_unix_millis() / self.half_window_ms);
            if expires <= now.as_unix_millis() || settled.expected().is_none() {
                continue;
            }
            let stored = StoredDurationRef {
                version: FORMAT_VERSION,
                sampler_generation: SAMPLER_GENERATION,
                window_ms,
                key,
                entry,
            };
            let body = serde_json::to_vec(&stored).map_err(PortError::backend)?;
            into.push(LearnedHistory {
                key: key.record_key(),
                body,
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
        Ok(())
    }

    pub(crate) fn restore(&mut self, now: Timestamp, records: &[LearnedHistory]) {
        self.entries.clear();
        let window_ms = self.half_window_ms * 2;
        let mut entries = Vec::new();
        for record in records {
            if record.key.kind != RecordKind::DurationHistory || record.expires_at <= now {
                continue;
            }
            let Ok(stored) = serde_json::from_slice::<StoredDuration>(&record.body) else {
                continue;
            };
            if stored.version != FORMAT_VERSION
                || stored.sampler_generation != SAMPLER_GENERATION
                || stored.window_ms != window_ms
                || stored.key.record_key() != record.key
                || stored.entry.last_seen_ms.saturating_add(window_ms)
                    != record.expires_at.as_unix_millis()
            {
                continue;
            }
            let mut settled = stored.entry.clone();
            settled.settle(now.as_unix_millis() / self.half_window_ms);
            if settled.expected().is_some() {
                entries.push(stored);
            }
        }
        entries.sort_by_key(|stored| stored.entry.last_seen_ms);
        for stored in entries {
            self.entries.put(stored.key, stored.entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    const HALF_DAY: u64 = 43_200_000;

    fn history(capacity: usize) -> ActionDurations {
        ActionDurations::new(
            NonZeroUsize::new(capacity).expect("nonzero capacity"),
            HistoryWindow::from_units(1),
        )
    }

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }

    #[test]
    fn persisted_durations_keep_age_caps_and_compatible_configuration() {
        let mut source = history(2);
        source.record("", "old", 10, at(1));
        source.record("", "new", 20, at(2));
        let mut records = Vec::new();
        source.snapshot(at(3), &mut records).unwrap();
        let mut later = Vec::new();
        source.snapshot(at(4), &mut later).unwrap();
        assert_eq!(records, later);

        let mut restored = history(1);
        let mut reverse_order = records.clone();
        reverse_order.reverse();
        restored.restore(at(3), &reverse_order);
        assert_eq!(restored.expected("", "old", at(3)), None);
        assert_eq!(
            restored.expected("", "new", at(3)),
            Some(Duration::from_millis(20))
        );
        restored.restore(at(2 + 2 * HALF_DAY), &records);
        assert_eq!(restored.expected("", "new", at(2 + 2 * HALF_DAY)), None);

        let mut incompatible =
            ActionDurations::new(NonZeroUsize::MIN, HistoryWindow::from_units(2));
        incompatible.restore(at(3), &records);
        assert_eq!(incompatible.expected("", "new", at(3)), None);
        let mut corrupt = records.clone();
        let mut body: serde_json::Value = serde_json::from_slice(&corrupt[1].body).unwrap();
        body["sampler_generation"] = serde_json::json!(999);
        corrupt[1].body = serde_json::to_vec(&body).unwrap();
        restored.restore(at(3), &corrupt);
        assert_eq!(restored.expected("", "new", at(3)), None);
    }

    #[test]
    fn target_then_mnemonic_fallback_and_empty_fields() {
        let mut h = history(8);
        h.record("a", "compile", 10, at(0));
        h.record("b", "compile", 30, at(0));
        h.record("ignored", "", 99, at(0));
        assert_eq!(
            h.expected("a", "compile", at(0)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("unknown", "compile", at(0)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            h.expected("", "compile", at(0)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(h.expected("ignored", "", at(0)), None);
        h.record("", "compile", 50, at(0));
        assert_eq!(
            h.expected("a", "compile", at(0)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("", "compile", at(0)),
            Some(Duration::from_millis(30))
        );
    }

    #[test]
    fn borrowed_keys_have_identical_hash_and_distinct_variants() {
        fn hash<T: Hash + ?Sized>(value: &T) -> u64 {
            let mut h = DefaultHasher::new();
            value.hash(&mut h);
            h.finish()
        }
        let owned = Key::Target {
            mnemonic: "x".into(),
            target: "x".into(),
        };
        let parts = KeyParts::Target {
            mnemonic: "x",
            target: "x",
        };
        let query: &dyn KeyView = &parts;
        let borrowed: &dyn KeyView = owned.borrow();
        assert!(borrowed == query);
        assert_eq!(hash(&owned), hash(query));
        let mnemonic = KeyParts::Mnemonic("x");
        let other: &dyn KeyView = &mnemonic;
        assert!(!(query == other));
        let owned_mnemonic = Key::Mnemonic("x".into());
        assert_eq!(hash(&owned_mnemonic), hash(other));
        let mut h = history(4);
        h.record("x", "x", 10, at(0));
        h.record("", "x", 30, at(0));
        assert_eq!(h.expected("x", "x", at(0)), Some(Duration::from_millis(10)));
        assert_eq!(h.expected("", "x", at(0)), Some(Duration::from_millis(20)));
    }

    #[test]
    fn rotations_are_epoch_aligned_and_expire_after_two_periods() {
        let mut h = history(4);
        h.record("a", "m", 10, at(HALF_DAY - 1));
        assert_eq!(
            h.expected("a", "m", at(HALF_DAY - 1)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("a", "m", at(HALF_DAY)),
            Some(Duration::from_millis(10))
        );
        h.record("a", "m", 30, at(HALF_DAY));
        assert_eq!(
            h.expected("a", "m", at(2 * HALF_DAY - 1)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            h.expected("a", "m", at(2 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(h.expected("a", "m", at(3 * HALF_DAY)), None);
    }

    #[test]
    fn configured_window_controls_expiry() {
        let mut h = ActionDurations::new(
            NonZeroUsize::new(5).expect("nonzero capacity"),
            gang_config::HistoryWindow::from_units(3),
        );
        h.record("a", "m", 17, at(0));
        assert_eq!(
            h.expected("a", "m", at(3 * HALF_DAY)),
            Some(Duration::from_millis(17))
        );
        assert_eq!(h.expected("a", "m", at(6 * HALF_DAY)), None);
    }

    #[test]
    fn multi_period_jump_and_backward_clock_do_not_resurrect_history() {
        let mut h = history(4);
        h.record("a", "m", 10, at(0));
        assert_eq!(h.expected("a", "m", at(3 * HALF_DAY)), None);
        assert_eq!(h.expected("a", "m", at(0)), None);
        h.record("a", "m", 30, at(HALF_DAY));
        assert_eq!(
            h.expected("a", "m", at(3 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(
            h.expected("a", "m", at(4 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(h.expected("a", "m", at(5 * HALF_DAY)), None);
    }

    #[test]
    fn capacity_one_and_active_mnemonic_recency() {
        let mut one = history(1);
        one.record("a", "m", 10, at(0));
        assert_eq!(
            one.expected("a", "m", at(0)),
            Some(Duration::from_millis(10))
        );
        one.record("b", "m", 30, at(0));
        assert_eq!(
            one.expected("a", "m", at(0)),
            Some(Duration::from_millis(30))
        );

        let mut h = history(3);
        h.record("a", "m", 10, at(0));
        h.record("b", "m", 20, at(0));
        h.record("c", "m", 60, at(0));
        assert_eq!(h.expected("a", "m", at(0)), Some(Duration::from_millis(30)));
        assert_eq!(h.expected("b", "m", at(0)), Some(Duration::from_millis(20)));
        assert_eq!(h.expected("c", "m", at(0)), Some(Duration::from_millis(60)));
    }

    #[test]
    fn overflow_skips_whole_sample_and_combined_mean_is_wide() {
        let mut half = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        half.record(1);
        assert_eq!((half.sum_ms, half.count), (u64::MAX, 1));
        half = Half {
            sum_ms: 0,
            count: u32::MAX,
        };
        half.record(1);
        assert_eq!((half.sum_ms, half.count), (0, u32::MAX));
        let mut entry = Entry::new(0);
        entry.current = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        entry.previous = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        assert_eq!(entry.expected(), Some(Duration::from_millis(u64::MAX)));
        entry.current = Half {
            sum_ms: u64::MAX,
            count: u32::MAX,
        };
        entry.previous = entry.current;
        assert_eq!(
            entry.expected(),
            Some(Duration::from_millis(u64::MAX / u64::from(u32::MAX)))
        );
    }

    #[test]
    fn expected_hit_fallback_and_miss_allocate_nothing() {
        let mut h = history(8);
        h.record("a", "m", 10, at(0));
        let mut results = (None, None, None);
        let allocations = allocation_counter::measure(|| {
            results.0 = h.expected("a", "m", at(0));
            results.1 = h.expected("other", "m", at(0));
            results.2 = h.expected("other", "missing", at(0));
        });
        assert_eq!(allocations.count_total, 0);
        assert_eq!(
            results,
            (
                Some(Duration::from_millis(10)),
                Some(Duration::from_millis(10)),
                None
            )
        );
    }
}
