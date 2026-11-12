//! A bounded cache from a key, such as a credential, to the value it was verified as.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use parking_lot::Mutex;

/// A cached value, the instant after which it is no longer valid, and the order it was inserted in.
#[derive(Debug)]
struct Entry<V> {
    value: V,
    valid_until: Timestamp,
    sequence: u64,
}

/// The state behind the lock. `expiry` indexes every entry by `(valid_until, sequence)`, so the
/// first key is the next entry to expire, and ties fall to the oldest insertion.
#[derive(Debug)]
struct Inner<K, V> {
    entries: HashMap<K, Entry<V>>,
    expiry: BTreeMap<(Timestamp, u64), K>,
    next_sequence: u64,
    /// Advanced by every removal, so an insert that read before one is declined.
    generation: u64,
}

impl<K: Hash + Eq, V> Inner<K, V> {
    /// Drops every entry whose `valid_until` is at or before `now`.
    fn drop_expired(&mut self, now: Timestamp) {
        while self
            .expiry
            .first_key_value()
            .is_some_and(|((valid_until, _), _)| *valid_until <= now)
        {
            self.remove_next_to_expire();
        }
    }

    /// Drops the entry that expires soonest, live or not.
    fn remove_next_to_expire(&mut self) {
        if let Some((_, key)) = self.expiry.pop_first() {
            self.entries.remove(&key);
        }
    }
}

/// Maps a key to a value, such as the identity it verified as, until a fixed instant.
///
/// An entry is live while `now` is before its `valid_until`. The map never holds more than its
/// capacity: an insert of a new key into a full map first drops the expired entries, then, if the
/// map is still full, the live entry that expires soonest. All methods take `&self`; the memo
/// locks its own state.
#[derive(Debug)]
pub(crate) struct Memo<K, V> {
    inner: Mutex<Inner<K, V>>,
    capacity: NonZeroUsize,
}

impl<K: Hash + Eq + Clone, V: Clone> Memo<K, V> {
    /// An empty memo that holds at most `capacity` entries.
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::with_capacity(capacity.get()),
                expiry: BTreeMap::new(),
                next_sequence: 0,
                generation: 0,
            }),
            capacity,
        }
    }

    /// The value cached under `key`, if it is still live at `now`. An expired entry is removed.
    pub(crate) fn get<Q>(&self, key: &Q, now: Timestamp) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut inner = self.inner.lock();
        let entry = inner.entries.get(key)?;
        if now < entry.valid_until {
            return Some(entry.value.clone());
        }
        let index = (entry.valid_until, entry.sequence);
        inner.entries.remove(key);
        inner.expiry.remove(&index);
        None
    }

    /// Caches `value` under `key` until `valid_until`.
    ///
    /// Entries already expired at `now` are ignored without changing the memo. Replacing an
    /// existing key never evicts another entry. Inserting a new key into a full map first drops
    /// the expired entries, and then the live entry that expires soonest.
    pub(crate) fn insert(&self, key: K, value: V, valid_until: Timestamp, now: Timestamp) {
        let mut inner = self.inner.lock();
        self.store(&mut inner, key, value, valid_until, now);
    }

    /// Caches `value` under `key` until `valid_until`, unless it is already expired at `now` or an
    /// entry has been removed since `generation` was read with [`Memo::generation`]. Reports
    /// whether it cached the value.
    pub(crate) fn insert_if_unchanged(
        &self,
        generation: u64,
        key: K,
        value: V,
        valid_until: Timestamp,
        now: Timestamp,
    ) -> bool {
        let mut inner = self.inner.lock();
        if inner.generation != generation {
            return false;
        }
        self.store(&mut inner, key, value, valid_until, now)
    }

    /// The generation, which every [`Memo::remove`] advances and nothing else does.
    pub(crate) fn generation(&self) -> u64 {
        self.inner.lock().generation
    }

    /// Drops the entry under `key`, if any. Advances the generation even when there is none.
    pub(crate) fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut inner = self.inner.lock();
        inner.generation = inner.generation.wrapping_add(1);
        if let Some(entry) = inner.entries.remove(key) {
            inner.expiry.remove(&(entry.valid_until, entry.sequence));
        }
    }

    /// Writes `value` into the locked state unless it is already expired at `now`; returns whether
    /// it was stored. See [`Memo::insert`].
    fn store(
        &self,
        inner: &mut Inner<K, V>,
        key: K,
        value: V,
        valid_until: Timestamp,
        now: Timestamp,
    ) -> bool {
        if valid_until <= now {
            return false;
        }
        if let Some(replaced) = inner.entries.remove(&key) {
            inner
                .expiry
                .remove(&(replaced.valid_until, replaced.sequence));
        } else if inner.entries.len() >= self.capacity.get() {
            inner.drop_expired(now);
            if inner.entries.len() >= self.capacity.get() {
                inner.remove_next_to_expire();
            }
        }
        let sequence = inner.next_sequence;
        inner.next_sequence = inner.next_sequence.wrapping_add(1);
        inner.expiry.insert((valid_until, sequence), key.clone());
        inner.entries.insert(
            key,
            Entry {
                value,
                valid_until,
                sequence,
            },
        );
        true
    }

    /// The number of entries held, live or not.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether the memo holds no entries.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use gang_clock::Timestamp;

    use super::Memo;

    fn at(seconds: u64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn capacity(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test capacities are non-zero")
    }

    #[test]
    fn get_hits_a_live_entry_and_misses_an_unknown_key() {
        let memo = Memo::new(capacity(4));
        memo.insert("token-a".to_owned(), "alice", at(100), at(0));

        assert_eq!(memo.get("token-a", at(50)), Some("alice"));
        assert_eq!(memo.get("token-b", at(50)), None);
    }

    #[test]
    fn get_removes_an_expired_entry() {
        let memo = Memo::new(capacity(4));
        memo.insert("token-a".to_owned(), "alice", at(100), at(0));

        assert_eq!(
            memo.get("token-a", at(100)),
            None,
            "valid_until is exclusive"
        );
        assert_eq!(memo.len(), 0);
    }

    #[test]
    fn remove_drops_the_entry() {
        let memo = Memo::new(capacity(4));
        memo.insert("token-a".to_owned(), "alice", at(100), at(0));

        memo.remove("token-a");

        assert!(memo.is_empty());
        assert_eq!(memo.get("token-a", at(50)), None);
    }

    #[test]
    fn full_memo_evicts_the_entry_that_expires_soonest() {
        let memo = Memo::new(capacity(2));
        memo.insert("long".to_owned(), "alice", at(200), at(0));
        memo.insert("short".to_owned(), "bob", at(50), at(0));

        memo.insert("new".to_owned(), "carol", at(300), at(0));

        assert_eq!(memo.len(), 2);
        assert_eq!(memo.get("short", at(0)), None);
        assert!(memo.get("long", at(0)).is_some());
        assert!(memo.get("new", at(0)).is_some());
    }

    #[test]
    fn expired_entry_is_dropped_before_a_live_one_is_evicted() {
        let memo = Memo::new(capacity(2));
        memo.insert("expired".to_owned(), "alice", at(10), at(0));
        memo.insert("live".to_owned(), "bob", at(100), at(0));

        memo.insert("new".to_owned(), "carol", at(300), at(20));

        assert_eq!(memo.len(), 2);
        assert_eq!(memo.get("expired", at(20)), None);
        assert!(memo.get("live", at(20)).is_some());
        assert!(memo.get("new", at(20)).is_some());
    }

    #[test]
    fn inserting_an_entry_already_expired_keeps_the_live_entries() {
        let memo = Memo::new(capacity(1));
        memo.insert("live".to_owned(), "alice", at(100), at(0));

        memo.insert("expired".to_owned(), "bob", at(50), at(50));

        assert_eq!(memo.len(), 1);
        assert_eq!(memo.get("live", at(50)), Some("alice"));
    }

    #[test]
    fn insert_if_unchanged_declines_an_expired_entry() {
        let memo: Memo<String, &str> = Memo::new(capacity(1));
        let generation = memo.generation();

        let inserted =
            memo.insert_if_unchanged(generation, "expired".to_owned(), "alice", at(50), at(50));

        assert!(!inserted);
        assert!(memo.is_empty());
    }

    #[test]
    fn expired_entries_are_dropped_before_a_live_one_is_evicted() {
        let memo = Memo::new(capacity(3));
        memo.insert("e1".to_owned(), "alice", at(10), at(0));
        memo.insert("e2".to_owned(), "bob", at(20), at(0));
        memo.insert("live".to_owned(), "carol", at(30), at(0));

        memo.insert("new".to_owned(), "dave", at(300), at(25));

        assert_eq!(memo.len(), 2);
        assert_eq!(memo.get("e1", at(25)), None);
        assert_eq!(memo.get("e2", at(25)), None);
        assert_eq!(memo.get("live", at(25)), Some("carol"));
        assert_eq!(memo.get("new", at(25)), Some("dave"));
    }

    #[test]
    fn reinserting_an_existing_key_replaces_it_without_eviction() {
        let memo = Memo::new(capacity(1));
        memo.insert("token-a".to_owned(), "alice", at(100), at(0));

        memo.insert("token-a".to_owned(), "bob", at(200), at(0));

        assert_eq!(memo.len(), 1);
        assert_eq!(memo.get("token-a", at(150)), Some("bob"));
    }

    #[test]
    fn replacing_a_key_moves_its_expiry_and_never_evicts_another() {
        let memo = Memo::new(capacity(2));
        memo.insert("a".to_owned(), "a1", at(100), at(0));
        memo.insert("b".to_owned(), "b1", at(200), at(0));

        memo.insert("a".to_owned(), "a2", at(300), at(0));

        assert_eq!(memo.len(), 2);
        assert_eq!(memo.get("b", at(0)), Some("b1"), "replacement evicted b");

        memo.insert("c".to_owned(), "c1", at(250), at(0));

        assert_eq!(memo.get("b", at(0)), None, "b expires soonest");
        assert_eq!(memo.get("a", at(0)), Some("a2"));

        memo.insert("c".to_owned(), "c2", at(150), at(0));
        memo.insert("d".to_owned(), "d1", at(400), at(0));

        assert_eq!(memo.get("c", at(0)), None, "c now expires soonest");
        assert_eq!(memo.get("a", at(0)), Some("a2"));
        assert_eq!(memo.get("d", at(0)), Some("d1"));
    }

    #[test]
    fn ties_in_expiry_evict_the_oldest_insert_first() {
        let memo = Memo::new(capacity(2));
        memo.insert("z".to_owned(), "alice", at(100), at(0));
        memo.insert("a".to_owned(), "bob", at(100), at(0));

        memo.insert("new".to_owned(), "carol", at(300), at(0));

        assert_eq!(memo.get("z", at(0)), None, "the oldest insert goes first");
        assert_eq!(memo.get("a", at(0)), Some("bob"));
        assert_eq!(memo.get("new", at(0)), Some("carol"));
    }

    #[test]
    fn generation_advances_on_every_remove_even_when_the_key_is_absent() {
        let memo: Memo<String, &str> = Memo::new(capacity(4));
        memo.insert("token-a".to_owned(), "alice", at(100), at(0));
        let before = memo.generation();

        memo.remove("token-a");
        let after_removing_it = memo.generation();
        memo.remove("token-a");

        assert!(after_removing_it > before);
        assert!(memo.generation() > after_removing_it);
    }

    #[test]
    fn generation_is_unchanged_by_insert_and_get() {
        let memo: Memo<String, &str> = Memo::new(capacity(4));
        let before = memo.generation();

        memo.insert("token-a".to_owned(), "alice", at(100), at(0));
        memo.get("token-a", at(50));
        memo.get("token-a", at(200));

        assert_eq!(memo.generation(), before);
    }

    #[test]
    fn insert_if_unchanged_inserts_while_the_generation_holds() {
        let memo: Memo<String, &str> = Memo::new(capacity(4));
        let generation = memo.generation();

        let inserted =
            memo.insert_if_unchanged(generation, "token-a".to_owned(), "alice", at(100), at(0));

        assert!(inserted);
        assert_eq!(memo.get("token-a", at(50)), Some("alice"));
    }

    #[test]
    fn insert_if_unchanged_declines_after_removing_an_absent_key() {
        let memo: Memo<String, &str> = Memo::new(capacity(4));
        let generation = memo.generation();
        memo.remove("token-a");

        let inserted =
            memo.insert_if_unchanged(generation, "token-a".to_owned(), "alice", at(100), at(0));

        assert!(!inserted);
        assert_eq!(memo.get("token-a", at(50)), None);
    }

    #[test]
    fn insert_if_unchanged_declines_after_removing_another_key() {
        let memo: Memo<String, &str> = Memo::new(capacity(4));
        memo.insert("token-b".to_owned(), "bob", at(100), at(0));
        let generation = memo.generation();
        memo.remove("token-b");

        let inserted =
            memo.insert_if_unchanged(generation, "token-a".to_owned(), "alice", at(100), at(0));

        assert!(!inserted);
        assert_eq!(memo.get("token-a", at(50)), None);
    }

    #[test]
    fn declined_insert_evicts_nothing() {
        let memo: Memo<String, &str> = Memo::new(capacity(1));
        memo.insert("token-b".to_owned(), "bob", at(100), at(0));
        let generation = memo.generation();
        memo.remove("token-c");

        let inserted =
            memo.insert_if_unchanged(generation, "token-a".to_owned(), "alice", at(100), at(0));

        assert!(!inserted);
        assert_eq!(memo.len(), 1);
        assert_eq!(memo.get("token-b", at(50)), Some("bob"));
    }
}
