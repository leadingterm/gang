use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use gang_clock::Timestamp;
use lru::LruCache;

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Default)]
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

struct Entry {
    period: u64,
    current: Half,
    previous: Half,
}

impl Entry {
    fn new(period: u64) -> Self {
        Self {
            period,
            current: Half::default(),
            previous: Half::default(),
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
    pub(crate) fn new(keys: NonZeroUsize, window_days: NonZeroU32) -> Self {
        Self {
            half_window_ms: u64::from(window_days.get()) * 86_400_000 / 2,
            entries: LruCache::new(keys),
        }
    }

    pub(crate) fn record(&mut self, target: &str, mnemonic: &str, slot_ms: u64, now: Timestamp) {
        if mnemonic.is_empty() {
            return;
        }
        let period = now.as_unix_millis() / self.half_window_ms;
        if !target.is_empty() {
            self.record_key(KeyParts::Target { mnemonic, target }, slot_ms, period);
        }
        self.record_key(KeyParts::Mnemonic(mnemonic), slot_ms, period);
    }

    fn record_key(&mut self, parts: KeyParts<'_>, slot_ms: u64, period: u64) {
        let query: &dyn KeyView = &parts;
        let entry = self
            .entries
            .get_or_insert_mut_ref(query, || Entry::new(period));
        entry.settle(period);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    const HALF_DAY: u64 = 43_200_000;

    fn history(capacity: usize) -> ActionDurations {
        ActionDurations::new(
            NonZeroUsize::new(capacity).expect("nonzero capacity"),
            NonZeroU32::new(1).expect("nonzero window"),
        )
    }

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
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
