//! Sharded O(1) intrusive-slab LRU cache index with attempt and tree pinning.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use gang_types::Digest;
use parking_lot::Mutex;

/// Number of independent LRU shards, keyed by the first nibble of the digest hash.
pub const SHARD_COUNT: usize = 16;

/// On-disk permission variant for a cached blob.
///
/// Plain (`0444`) and executable (`0555`) variants occupy separate inodes and distinct LRU keys
/// so hardlinked inputs never share permissions across modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BlobMode {
    /// Non-executable regular blob (`0444`).
    Plain,
    /// Executable regular blob (`0555`).
    Executable,
}

impl BlobMode {
    /// Converts a boolean executable flag into a [`BlobMode`].
    #[must_use]
    pub const fn from_executable(executable: bool) -> Self {
        if executable {
            Self::Executable
        } else {
            Self::Plain
        }
    }

    /// Returns `true` when this mode represents an executable blob.
    #[must_use]
    pub const fn is_executable(self) -> bool {
        matches!(self, Self::Executable)
    }
}

/// Composite cache key pairing a content [`Digest`] with its [`BlobMode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Content digest of the blob.
    pub digest: Digest,
    /// Permission mode variant.
    pub mode: BlobMode,
}

impl CacheKey {
    /// Creates a [`CacheKey`] from `digest` and `mode`.
    #[must_use]
    pub const fn new(digest: Digest, mode: BlobMode) -> Self {
        Self { digest, mode }
    }
}

/// Which subsystem holds a pin on a cached entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinKind {
    /// Pinned by an in-flight action attempt while staging or executing.
    Attempt,
    /// Pinned by a materialized directory in `TreeCache`.
    Tree,
}

/// An entry removed by [`ShardedLru::evict_unpinned_until`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictedEntry {
    /// Key of the evicted entry.
    pub key: CacheKey,
    /// Logical byte size of the evicted blob.
    pub size: u64,
}

/// Outcome of inserting a key into [`ShardedLru`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The entry was newly inserted and added `size` bytes to the cache.
    Inserted {
        /// Byte size added to the cache.
        size: u64,
    },
    /// The entry was already resident; its recency was refreshed without changing `size_bytes`.
    AlreadyPresent,
}

#[derive(Debug)]
struct Node {
    key: CacheKey,
    size: u64,
    stamp: u64,
    attempt_pins: u32,
    tree_pins: u32,
    prev: Option<usize>,
    next: Option<usize>,
    in_unpinned_list: bool,
}

impl Node {
    const fn is_pinned(&self) -> bool {
        self.attempt_pins > 0 || self.tree_pins > 0
    }
}

#[derive(Debug)]
enum Slot {
    Occupied(Node),
    Vacant { next_free: Option<usize> },
}

#[derive(Debug, Default)]
struct LruShard {
    index: HashMap<CacheKey, usize>,
    slots: Vec<Slot>,
    free_head: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
}

impl LruShard {
    fn contains(&self, key: CacheKey) -> bool {
        self.index.contains_key(&key)
    }

    fn alloc_slot(&mut self, node: Node) -> usize {
        if let Some(idx) = self.free_head {
            let next_free = match self.slots.get(idx) {
                Some(Slot::Vacant { next_free }) => *next_free,
                _ => None,
            };
            self.free_head = next_free;
            if let Some(slot) = self.slots.get_mut(idx) {
                *slot = Slot::Occupied(node);
            }
            return idx;
        }
        let idx = self.slots.len();
        self.slots.push(Slot::Occupied(node));
        idx
    }

    fn free_slot(&mut self, idx: usize) {
        if let Some(slot) = self.slots.get_mut(idx) {
            *slot = Slot::Vacant {
                next_free: self.free_head,
            };
            self.free_head = Some(idx);
        }
    }

    fn node(&self, idx: usize) -> Option<&Node> {
        match self.slots.get(idx) {
            Some(Slot::Occupied(node)) => Some(node),
            _ => None,
        }
    }

    fn node_mut(&mut self, idx: usize) -> Option<&mut Node> {
        match self.slots.get_mut(idx) {
            Some(Slot::Occupied(node)) => Some(node),
            _ => None,
        }
    }

    fn unlink(&mut self, idx: usize) {
        let Some((prev, next, in_list)) = self
            .node(idx)
            .map(|node| (node.prev, node.next, node.in_unpinned_list))
        else {
            return;
        };
        if !in_list {
            return;
        }
        if let Some(prev_idx) = prev {
            if let Some(prev_node) = self.node_mut(prev_idx) {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_idx) = next {
            if let Some(next_node) = self.node_mut(next_idx) {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }
        if let Some(node) = self.node_mut(idx) {
            node.prev = None;
            node.next = None;
            node.in_unpinned_list = false;
        }
    }

    fn link_front(&mut self, idx: usize) {
        let old_head = self.head;
        if let Some(node) = self.node_mut(idx) {
            if node.in_unpinned_list {
                return;
            }
            node.prev = None;
            node.next = old_head;
            node.in_unpinned_list = true;
        } else {
            return;
        }
        if let Some(head_idx) = old_head
            && let Some(head_node) = self.node_mut(head_idx)
        {
            head_node.prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        let is_already_head = self.head == Some(idx);
        let in_list = self.node(idx).is_some_and(|node| node.in_unpinned_list);
        if !in_list || is_already_head {
            return;
        }
        self.unlink(idx);
        self.link_front(idx);
    }

    fn oldest_unpinned_stamp(&self) -> Option<u64> {
        self.tail
            .and_then(|idx| self.node(idx))
            .map(|node| node.stamp)
    }

    fn pop_unpinned_tail(&mut self) -> Option<(CacheKey, u64)> {
        let idx = self.tail?;
        let (key, size) = self.node(idx).map(|node| (node.key, node.size))?;
        self.unlink(idx);
        self.index.remove(&key);
        self.free_slot(idx);
        Some((key, size))
    }

    fn rebuild_unpinned_by_stamp(&mut self) {
        let mut entries: Vec<(u64, usize)> = self
            .index
            .values()
            .copied()
            .filter_map(|idx| {
                let node = self.node(idx)?;
                if node.is_pinned() {
                    None
                } else {
                    Some((node.stamp, idx))
                }
            })
            .collect();
        entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));

        self.head = None;
        self.tail = None;
        for slot in &mut self.slots {
            if let Slot::Occupied(node) = slot {
                node.prev = None;
                node.next = None;
                node.in_unpinned_list = false;
            }
        }
        let mut prev: Option<usize> = None;
        for (_, idx) in entries {
            if let Some(node) = self.node_mut(idx) {
                node.prev = prev;
                node.next = None;
                node.in_unpinned_list = true;
            }
            if let Some(prev_idx) = prev {
                if let Some(prev_node) = self.node_mut(prev_idx) {
                    prev_node.next = Some(idx);
                }
            } else {
                self.head = Some(idx);
            }
            prev = Some(idx);
        }
        self.tail = prev;
    }
}

/// Sixteen-way sharded O(1) LRU index for [`DiskCas`](crate::DiskCas).
///
/// Each shard protects an intrusive doubly linked list over an index-backed slab with a
/// `parking_lot::Mutex` that is never held across `.await` or filesystem I/O. Pinned entries are
/// unlinked from the evictable LRU chain on `0 -> 1` pin transitions and re-inserted at the MRU
/// head when their total pin count returns to zero, keeping eviction O(1).
#[derive(Debug)]
pub struct ShardedLru {
    shards: [Mutex<LruShard>; SHARD_COUNT],
    stamp_clock: AtomicU64,
    size_bytes: AtomicU64,
    pinned_bytes: AtomicU64,
    tree_pinned_bytes: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    entry_count: AtomicU64,
}

impl Default for ShardedLru {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedLru {
    /// Creates an empty 16-shard [`ShardedLru`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(LruShard::default())),
            stamp_clock: AtomicU64::new(1),
            size_bytes: AtomicU64::new(0),
            pinned_bytes: AtomicU64::new(0),
            tree_pinned_bytes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            evicted_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
        }
    }

    /// Maps `digest` to its shard index (`0..16`) using the first nibble of its SHA-256 hash.
    #[must_use]
    pub fn shard_index(digest: &Digest) -> usize {
        usize::from(digest.hash()[0] >> 4)
    }

    fn next_stamp(&self) -> u64 {
        self.stamp_clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Returns `true` if `key` is currently indexed.
    #[must_use]
    pub fn contains(&self, key: CacheKey) -> bool {
        let shard = self.shards[Self::shard_index(&key.digest)].lock();
        shard.contains(key)
    }

    /// Inserts `key` of `size` bytes as an unpinned MRU entry, or touches it if already present.
    pub fn insert(&self, key: CacheKey, size: u64) -> InsertOutcome {
        self.insert_inner(key, size, None, None)
    }

    /// Atomically inserts `key` of `size` bytes and acquires a pin of `kind`.
    ///
    /// If `key` is already present, its recency is updated and the pin count is incremented
    /// atomically under the shard lock so concurrent eviction cannot reclaim the entry first.
    pub fn insert_pinned(&self, key: CacheKey, size: u64, kind: PinKind) -> InsertOutcome {
        self.insert_inner(key, size, None, Some(kind))
    }

    /// Inserts `key` with an explicit startup `stamp`, advancing the internal stamp clock past it.
    pub fn insert_with_stamp(&self, key: CacheKey, size: u64, stamp: u64) -> InsertOutcome {
        self.stamp_clock
            .fetch_max(stamp.saturating_add(1), Ordering::Relaxed);
        self.insert_inner(key, size, Some(stamp), None)
    }

    /// Sorts each shard's unpinned LRU chain by `stamp` so startup-scanned `atime`/`ctime` order
    /// governs subsequent evictions.
    pub fn rebuild_startup_order(&self) {
        for shard_lock in &self.shards {
            shard_lock.lock().rebuild_unpinned_by_stamp();
        }
    }

    fn insert_inner(
        &self,
        key: CacheKey,
        size: u64,
        explicit_stamp: Option<u64>,
        initial_pin: Option<PinKind>,
    ) -> InsertOutcome {
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        self.insert_in_shard(&mut shard, key, size, explicit_stamp, initial_pin)
    }

    pub(crate) fn publish<E>(
        &self,
        key: CacheKey,
        size: u64,
        publish: impl FnOnce() -> Result<(), E>,
    ) -> Result<InsertOutcome, E> {
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        publish()?;
        Ok(self.insert_in_shard(&mut shard, key, size, None, None))
    }

    fn insert_in_shard(
        &self,
        shard: &mut LruShard,
        key: CacheKey,
        size: u64,
        explicit_stamp: Option<u64>,
        initial_pin: Option<PinKind>,
    ) -> InsertOutcome {
        let stamp = explicit_stamp.unwrap_or_else(|| self.next_stamp());
        if let Some(&idx) = shard.index.get(&key) {
            let Some(node) = shard.node_mut(idx) else {
                return InsertOutcome::AlreadyPresent;
            };
            let entry_size = node.size;
            let was_pinned = node.is_pinned();
            let was_tree_pinned = node.tree_pins > 0;
            node.stamp = stamp;
            match initial_pin {
                Some(PinKind::Attempt) => {
                    node.attempt_pins = node.attempt_pins.saturating_add(1);
                }
                Some(PinKind::Tree) => {
                    node.tree_pins = node.tree_pins.saturating_add(1);
                }
                None => {}
            }
            let now_pinned = node.is_pinned();
            let now_tree_pinned = node.tree_pins > 0;
            if now_pinned && !was_pinned {
                shard.unlink(idx);
                self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);
            } else if !now_pinned {
                shard.move_to_front(idx);
            }
            if now_tree_pinned && !was_tree_pinned {
                self.tree_pinned_bytes
                    .fetch_add(entry_size, Ordering::Relaxed);
            }
            return InsertOutcome::AlreadyPresent;
        }

        let (attempt_pins, tree_pins) = match initial_pin {
            Some(PinKind::Attempt) => (1, 0),
            Some(PinKind::Tree) => (0, 1),
            None => (0, 0),
        };
        let node = Node {
            key,
            size,
            stamp,
            attempt_pins,
            tree_pins,
            prev: None,
            next: None,
            in_unpinned_list: false,
        };
        let pinned = node.is_pinned();
        let idx = shard.alloc_slot(node);
        shard.index.insert(key, idx);
        if !pinned {
            shard.link_front(idx);
        }
        self.size_bytes.fetch_add(size, Ordering::Release);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        if pinned {
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            if tree_pins > 0 {
                self.tree_pinned_bytes.fetch_add(size, Ordering::Relaxed);
            }
        }
        InsertOutcome::Inserted { size }
    }

    /// Marks `key` as most recently used, returning `false` if `key` is not in the index.
    pub fn touch(&self, key: CacheKey) -> bool {
        let stamp = self.next_stamp();
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        let Some(&idx) = shard.index.get(&key) else {
            return false;
        };
        let Some(node) = shard.node_mut(idx) else {
            return false;
        };
        node.stamp = stamp;
        if !node.is_pinned() {
            shard.move_to_front(idx);
        }
        true
    }

    /// Acquires a pin of `kind` on `key`, returning `false` if `key` is absent.
    pub fn pin(&self, key: CacheKey, kind: PinKind) -> bool {
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        let Some(&idx) = shard.index.get(&key) else {
            return false;
        };
        let Some(node) = shard.node_mut(idx) else {
            return false;
        };
        let size = node.size;
        let was_pinned = node.is_pinned();
        let was_tree_pinned = node.tree_pins > 0;
        match kind {
            PinKind::Attempt => node.attempt_pins = node.attempt_pins.saturating_add(1),
            PinKind::Tree => node.tree_pins = node.tree_pins.saturating_add(1),
        }
        let now_tree_pinned = node.tree_pins > 0;
        if !was_pinned {
            shard.unlink(idx);
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
        }
        if !was_tree_pinned && now_tree_pinned {
            self.tree_pinned_bytes.fetch_add(size, Ordering::Relaxed);
        }
        true
    }

    /// Acquires a [`PinKind::Tree`] pin on `key` only if distinct `tree_pinned_bytes` stays within
    /// `max_tree_pinned_bytes`.
    ///
    /// Entries already holding at least one tree pin incur zero additional distinct bytes and
    /// therefore always succeed. Returns `false` if `key` is absent or the cap would be exceeded.
    pub fn pin_tree_within_cap(&self, key: CacheKey, max_tree_pinned_bytes: u64) -> bool {
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        let Some(&idx) = shard.index.get(&key) else {
            return false;
        };
        let Some(node) = shard.node(idx) else {
            return false;
        };
        let size = node.size;
        let already_tree_pinned = node.tree_pins > 0;

        if !already_tree_pinned {
            let reserved = self
                .tree_pinned_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    let next = current.checked_add(size)?;
                    (next <= max_tree_pinned_bytes).then_some(next)
                })
                .is_ok();
            if !reserved {
                return false;
            }
        }

        let Some(node) = shard.node_mut(idx) else {
            return false;
        };
        let was_pinned = node.is_pinned();
        node.tree_pins = node.tree_pins.saturating_add(1);
        if !was_pinned {
            shard.unlink(idx);
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
        }
        true
    }

    /// Releases one pin of `kind` from `key`, returning `false` if `key` is absent or had no pin
    /// of `kind`.
    pub fn unpin(&self, key: CacheKey, kind: PinKind) -> bool {
        let stamp = self.next_stamp();
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        let Some(&idx) = shard.index.get(&key) else {
            return false;
        };
        let Some(node) = shard.node_mut(idx) else {
            return false;
        };
        let size = node.size;
        let was_pinned = node.is_pinned();
        let was_tree_pinned = node.tree_pins > 0;
        match kind {
            PinKind::Attempt => {
                if node.attempt_pins == 0 {
                    return false;
                }
                node.attempt_pins -= 1;
            }
            PinKind::Tree => {
                if node.tree_pins == 0 {
                    return false;
                }
                node.tree_pins -= 1;
            }
        }
        let now_pinned = node.is_pinned();
        let now_tree_pinned = node.tree_pins > 0;

        if was_tree_pinned && !now_tree_pinned {
            let _ = self.tree_pinned_bytes.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(size)),
            );
        }
        if was_pinned && !now_pinned {
            let _ =
                self.pinned_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(size))
                    });
            node.stamp = stamp;
            shard.link_front(idx);
        }
        true
    }

    /// Evicts unpinned entries in global LRU order until `size_bytes <= target_bytes` or no
    /// unpinned entries remain.
    ///
    /// Each step inspects the oldest unpinned tail across all 16 shards (without holding two shard
    /// locks simultaneously) and pops from the shard with the smallest stamp.
    pub fn evict_unpinned_until(&self, target_bytes: u64) -> Vec<EvictedEntry> {
        self.try_evict_unpinned_until(target_bytes, &mut |_| Ok::<_, std::convert::Infallible>(()))
            .unwrap_or_else(|never| match never {})
    }

    pub(crate) fn try_evict_unpinned_until<E>(
        &self,
        target_bytes: u64,
        before_remove: &mut dyn FnMut(CacheKey) -> Result<(), E>,
    ) -> Result<Vec<EvictedEntry>, E> {
        let mut evicted = Vec::new();
        while self.size_bytes.load(Ordering::Acquire) > target_bytes {
            let mut best: Option<(usize, u64)> = None;
            for (shard_idx, shard_lock) in self.shards.iter().enumerate() {
                let Some(stamp) = shard_lock.lock().oldest_unpinned_stamp() else {
                    continue;
                };
                if best.is_none_or(|(_, best_stamp)| stamp < best_stamp) {
                    best = Some((shard_idx, stamp));
                }
            }
            let Some((winner_idx, _)) = best else {
                break;
            };

            let popped = {
                let mut shard = self.shards[winner_idx].lock();
                if self.size_bytes.load(Ordering::Acquire) <= target_bytes {
                    break;
                }
                if let Some(node) = shard.tail.and_then(|idx| shard.node(idx)) {
                    before_remove(node.key)?;
                }
                shard.pop_unpinned_tail()
            };

            let Some((key, size)) = popped else {
                continue;
            };
            let _ = self
                .size_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(size))
                });
            let _ =
                self.entry_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(1))
                    });
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.evicted_bytes.fetch_add(size, Ordering::Relaxed);
            evicted.push(EvictedEntry { key, size });
        }
        Ok(evicted)
    }

    /// Removes `key` from the index without recording an eviction (e.g., when a file vanished).
    pub fn remove(&self, key: CacheKey) -> Option<u64> {
        let mut shard = self.shards[Self::shard_index(&key.digest)].lock();
        let idx = shard.index.remove(&key)?;
        let (size, was_pinned, was_tree_pinned) = shard
            .node(idx)
            .map(|node| (node.size, node.is_pinned(), node.tree_pins > 0))?;
        shard.unlink(idx);
        shard.free_slot(idx);
        let _ = self
            .size_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(size))
            });
        let _ = self
            .entry_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
        if was_pinned {
            let _ =
                self.pinned_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(size))
                    });
        }
        if was_tree_pinned {
            let _ = self.tree_pinned_bytes.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(size)),
            );
        }
        Some(size)
    }

    /// Returns total resident bytes tracked in the index (pinned plus unpinned).
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Acquire)
    }

    /// Returns total bytes across entries with at least one attempt or tree pin.
    #[must_use]
    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }

    /// Returns total distinct bytes across entries with at least one tree pin.
    #[must_use]
    pub fn tree_pinned_bytes(&self) -> u64 {
        self.tree_pinned_bytes.load(Ordering::Relaxed)
    }

    /// Returns cumulative evicted entry count.
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Returns cumulative evicted byte count.
    #[must_use]
    pub fn evicted_bytes(&self) -> u64 {
        self.evicted_bytes.load(Ordering::Relaxed)
    }

    /// Returns the number of indexed entries.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Returns `true` when no entries are indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use gang_types::Digest;

    use super::{
        BlobMode, CacheKey, EvictedEntry, InsertOutcome, PinKind, SHARD_COUNT, ShardedLru,
    };

    fn key(label: &[u8], mode: BlobMode) -> (CacheKey, u64) {
        let digest = Digest::from_bytes(label);
        let size = digest.size();
        (CacheKey::new(digest, mode), size)
    }

    #[test]
    fn shards_span_all_sixteen_first_nibbles() {
        let mut seen = [false; SHARD_COUNT];
        for i in 0_u32..512 {
            let digest = Digest::from_bytes(&i.to_le_bytes());
            seen[ShardedLru::shard_index(&digest)] = true;
        }
        assert!(seen.iter().all(|present| *present));
    }

    #[test]
    fn duplicate_insert_touches_without_inflating_size_or_evictions() {
        let lru = ShardedLru::new();
        let (k, size) = key(b"dup-blob", BlobMode::Plain);
        assert_eq!(lru.insert(k, size), InsertOutcome::Inserted { size });
        assert_eq!(lru.insert(k, size), InsertOutcome::AlreadyPresent);
        assert_eq!(lru.size_bytes(), size);
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.evictions(), 0);
        assert_eq!(lru.evicted_bytes(), 0);
    }

    #[test]
    fn evicts_oldest_across_shards_and_respects_touch() {
        let lru = ShardedLru::new();
        let (k1, s1) = key(b"alpha-item", BlobMode::Plain);
        let (k2, s2) = key(b"beta-item", BlobMode::Plain);
        let (k3, s3) = key(b"gamma-item", BlobMode::Plain);

        lru.insert(k1, s1);
        lru.insert(k2, s2);
        lru.insert(k3, s3);

        // Touch k1 so k2 becomes the global oldest.
        assert!(lru.touch(k1));

        let target = s1 + s3;
        let evicted = lru.evict_unpinned_until(target);
        assert_eq!(evicted, vec![EvictedEntry { key: k2, size: s2 }]);
        assert!(!lru.contains(k2));
        assert!(lru.contains(k1));
        assert!(lru.contains(k3));
        assert_eq!(lru.size_bytes(), target);
        assert_eq!(lru.evictions(), 1);
        assert_eq!(lru.evicted_bytes(), s2);
    }

    #[test]
    fn pinned_entries_are_skipped_until_unpinned() {
        let lru = ShardedLru::new();
        let (k1, s1) = key(b"pinned-oldest", BlobMode::Plain);
        let (k2, s2) = key(b"unpinned-newer", BlobMode::Executable);

        assert_eq!(
            lru.insert_pinned(k1, s1, PinKind::Attempt),
            InsertOutcome::Inserted { size: s1 }
        );
        lru.insert(k2, s2);
        assert_eq!(lru.pinned_bytes(), s1);

        // Only k2 can be evicted while k1 is pinned.
        let evicted = lru.evict_unpinned_until(0);
        assert_eq!(evicted, vec![EvictedEntry { key: k2, size: s2 }]);
        assert!(lru.contains(k1));
        assert_eq!(lru.size_bytes(), s1);

        // Unpinning returns k1 to the evictable chain.
        assert!(lru.unpin(k1, PinKind::Attempt));
        assert_eq!(lru.pinned_bytes(), 0);
        let evicted_after = lru.evict_unpinned_until(0);
        assert_eq!(evicted_after, vec![EvictedEntry { key: k1, size: s1 }]);
        assert!(lru.is_empty());
        assert_eq!(lru.size_bytes(), 0);
    }

    #[test]
    fn tree_pins_track_zero_to_one_transitions_and_cap() {
        let lru = ShardedLru::new();
        let (k1, s1) = key(b"tree-blob-1", BlobMode::Plain);
        let (k2, s2) = key(b"tree-blob-2-longer", BlobMode::Plain);
        lru.insert(k1, s1);
        lru.insert(k2, s2);

        let cap = s1 + s2 - 1;
        assert!(lru.pin_tree_within_cap(k1, cap));
        assert_eq!(lru.tree_pinned_bytes(), s1);

        // Adding another tree pin to the same entry succeeds even when cap is tighter.
        assert!(lru.pin_tree_within_cap(k1, s1));
        assert_eq!(lru.tree_pinned_bytes(), s1);

        // Pinning k2 would exceed cap.
        assert!(!lru.pin_tree_within_cap(k2, cap));
        assert_eq!(lru.tree_pinned_bytes(), s1);

        // First unpin leaves one tree pin remaining; second drops tree_pinned_bytes to 0.
        assert!(lru.unpin(k1, PinKind::Tree));
        assert_eq!(lru.tree_pinned_bytes(), s1);
        assert!(lru.unpin(k1, PinKind::Tree));
        assert_eq!(lru.tree_pinned_bytes(), 0);
        assert_eq!(lru.pinned_bytes(), 0);
    }

    #[test]
    fn startup_order_evicts_smallest_stamp_first() {
        let lru = ShardedLru::new();
        let (k1, s1) = key(b"startup-newer", BlobMode::Plain);
        let (k2, s2) = key(b"startup-oldest", BlobMode::Plain);
        let (k3, s3) = key(b"startup-middle", BlobMode::Plain);

        lru.insert_with_stamp(k1, s1, 300);
        lru.insert_with_stamp(k2, s2, 100);
        lru.insert_with_stamp(k3, s3, 200);
        lru.rebuild_startup_order();

        let evicted = lru.evict_unpinned_until(s1);
        assert_eq!(
            evicted,
            vec![
                EvictedEntry { key: k2, size: s2 },
                EvictedEntry { key: k3, size: s3 },
            ]
        );
    }
}
