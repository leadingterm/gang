//! A bounded fixed-TTL map ordered by write time, not by reads.

use std::borrow::Borrow;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Tick;
use lru::LruCache;

/// A fixed-TTL map whose oldest write is evicted when capacity is exhausted.
///
/// Eviction can make a principal's next sighting due early, or invalidate a pending login.
pub(crate) struct TtlMap<K, V> {
    entries: LruCache<K, (V, Tick)>,
    ttl: Duration,
}

impl<K: Hash + Eq, V> TtlMap<K, V> {
    /// Creates an empty map with the given capacity and lifetime.
    pub(crate) fn new(capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            entries: LruCache::new(capacity),
            ttl,
        }
    }

    /// Writes a value, removing expired oldest entries first when full.
    pub(crate) fn insert(&mut self, key: K, value: V, now: Tick) {
        self.entries.pop(&key);
        if self.entries.len() >= self.entries.cap().get() {
            while let Some((_, (_, expires_at))) = self.entries.peek_lru() {
                if now < *expires_at {
                    break;
                }
                self.entries.pop_lru();
            }
        }
        self.entries.push(key, (value, now + self.ttl));
    }

    /// Returns a live value without changing write order.
    pub(crate) fn get_live<Q: Hash + Eq + ?Sized>(&self, key: &Q, now: Tick) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let (value, expires_at) = self.entries.peek(key)?;
        (now < *expires_at).then_some(value)
    }

    /// Returns a mutable live value without changing write order.
    pub(crate) fn get_live_mut<Q: Hash + Eq + ?Sized>(
        &mut self,
        key: &Q,
        now: Tick,
    ) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        let (value, expires_at) = self.entries.peek_mut(key)?;
        (now < *expires_at).then_some(value)
    }

    /// Removes a value and returns it only if it is still live.
    pub(crate) fn take_live<Q: Hash + Eq + ?Sized>(&mut self, key: &Q, now: Tick) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let (value, expires_at) = self.entries.pop(key)?;
        (now < expires_at).then_some(value)
    }

    /// Returns the number of stored entries, including any expired entries.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use gang_clock::Tick;

    use super::TtlMap;

    /// Constructs a map with two slots and a ten-second lifetime.
    fn map() -> TtlMap<&'static str, u8> {
        TtlMap::new(
            NonZeroUsize::new(2).expect("two is non-zero"),
            Duration::from_secs(10),
        )
    }

    /// Returns a test tick at the given second.
    fn at(seconds: u64) -> Tick {
        Tick::ORIGIN + Duration::from_secs(seconds)
    }

    #[test]
    fn expires_at_exactly_the_ttl() {
        let mut entries = map();
        entries.insert("a", 1, at(0));
        assert_eq!(entries.get_live(&"a", at(9)), Some(&1));
        assert_eq!(entries.get_live(&"a", at(10)), None);
        assert_eq!(entries.take_live(&"a", at(10)), None);
    }

    #[test]
    fn get_live_mut_changes_a_live_entry_only() {
        let mut entries = map();
        entries.insert("a", 1, at(0));
        entries.insert("b", 2, at(1));
        *entries.get_live_mut(&"a", at(2)).expect("a is live") = 3;
        assert_eq!(entries.get_live(&"a", at(2)), Some(&3));
        assert!(entries.get_live_mut(&"missing", at(2)).is_none());
        entries.insert("c", 4, at(3));
        assert!(entries.get_live(&"a", at(3)).is_none());
        assert_eq!(entries.get_live(&"b", at(3)), Some(&2));
        assert!(entries.get_live_mut(&"b", at(11)).is_none());
        assert_eq!(entries.take_live(&"b", at(10)), Some(2));
    }

    #[test]
    fn evicts_expired_then_oldest_even_after_reads() {
        let mut entries = map();
        entries.insert("a", 1, at(0));
        entries.insert("b", 2, at(5));
        entries.insert("c", 3, at(10));
        assert_eq!(entries.get_live(&"b", at(10)), Some(&2));
        entries.insert("d", 4, at(11));
        assert_eq!(entries.get_live(&"b", at(11)), None);
        assert_eq!(entries.get_live(&"c", at(11)), Some(&3));
        assert_eq!(entries.get_live(&"d", at(11)), Some(&4));
    }

    #[test]
    fn never_exceeds_capacity() {
        let mut entries = map();
        for key in ["a", "b", "c", "d"] {
            entries.insert(key, 1, at(0));
            assert!(entries.len() <= 2);
        }
        assert_eq!(entries.len(), 2);
    }
}
