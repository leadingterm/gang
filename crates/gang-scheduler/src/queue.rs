//! Priority-ordered queue indices for pending scheduler operations.
//!
//! Queue ordering is higher-priority-first (`Reverse<i32>`), breaking ties by oldest admission
//! sequence (`u64`), and finally by [`OperationKey`] for strict uniqueness.

use std::cmp::Reverse;
use std::collections::BTreeSet;

use gang_types::{FleetCountError, FleetSet, MAX_FLEETS};

use crate::operations::OperationKey;

/// Total order key for queued operations: highest priority first, then oldest `seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueKey {
    /// Inverted priority so higher numeric priority sorts first.
    pub priority: Reverse<i32>,
    /// Monotonic admission sequence number, preserved across deduplication and requeues.
    pub seq: u64,
    /// Generation-checked slab key of the queued operation.
    pub key: OperationKey,
}

impl QueueKey {
    /// Constructs a [`QueueKey`] from priority, admission sequence, and operation key.
    #[must_use]
    pub const fn new(priority: i32, seq: u64, key: OperationKey) -> Self {
        Self {
            priority: Reverse(priority),
            seq,
            key,
        }
    }
}

/// Global and per-fleet priority queue indices for queued operations.
///
/// An operation sits in `global` and in every candidate fleet's `BTreeSet`, giving
/// `O((|candidates| + 1) * log n)` enqueue/dequeue and `O(limit)` ordered iteration for both
/// snapshots and worker-driven placement scans.
#[derive(Debug, Clone)]
pub struct OperationQueue {
    global: BTreeSet<QueueKey>,
    by_fleet: Vec<BTreeSet<QueueKey>>,
}

impl Default for OperationQueue {
    fn default() -> Self {
        Self {
            global: BTreeSet::new(),
            by_fleet: vec![BTreeSet::new(); MAX_FLEETS],
        }
    }
}

impl OperationQueue {
    /// Creates an empty queue supporting `fleet_count` fleets.
    ///
    /// # Errors
    ///
    /// Returns [`FleetCountError`] if `fleet_count > MAX_FLEETS`.
    pub fn new(fleet_count: usize) -> Result<Self, FleetCountError> {
        if fleet_count > MAX_FLEETS {
            return Err(FleetCountError { count: fleet_count });
        }
        Ok(Self {
            global: BTreeSet::new(),
            by_fleet: vec![BTreeSet::new(); fleet_count],
        })
    }

    /// Returns the number of configured fleets indexed by this queue.
    #[must_use]
    pub fn fleet_count(&self) -> usize {
        self.by_fleet.len()
    }

    /// Returns the total number of distinct queued operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.global.len()
    }

    /// Returns `true` when no operations are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.global.is_empty()
    }

    /// Returns the number of queued operations eligible for `fleet`.
    #[must_use]
    pub fn fleet_len(&self, fleet: usize) -> usize {
        self.by_fleet.get(fleet).map_or(0, BTreeSet::len)
    }

    /// Inserts an operation into the global index and every candidate fleet index.
    pub fn enqueue(&mut self, key: OperationKey, candidates: FleetSet, priority: i32, seq: u64) {
        let entry = QueueKey::new(priority, seq, key);
        self.global.insert(entry);
        for fleet in candidates {
            if let Some(index) = self.by_fleet.get_mut(fleet) {
                index.insert(entry);
            }
        }
    }

    /// Removes an operation from the global index and every candidate fleet index.
    ///
    /// Returns `true` if the operation was present in the global index.
    pub fn dequeue(
        &mut self,
        key: OperationKey,
        candidates: FleetSet,
        priority: i32,
        seq: u64,
    ) -> bool {
        let entry = QueueKey::new(priority, seq, key);
        let removed = self.global.remove(&entry);
        for fleet in candidates {
            if let Some(index) = self.by_fleet.get_mut(fleet) {
                index.remove(&entry);
            }
        }
        removed
    }

    /// Re-indexes a queued operation after priority promotion, preserving its original `seq`.
    ///
    /// Returns `true` if the operation was present in the global index before promotion.
    pub fn promote(
        &mut self,
        key: OperationKey,
        candidates: FleetSet,
        old_priority: i32,
        new_priority: i32,
        seq: u64,
    ) -> bool {
        if old_priority == new_priority {
            return self.global.contains(&QueueKey::new(old_priority, seq, key));
        }
        if !self.dequeue(key, candidates, old_priority, seq) {
            return false;
        }
        self.enqueue(key, candidates, new_priority, seq);
        true
    }

    /// Updates the candidate fleet indices for an already-queued operation.
    pub fn update_candidates(
        &mut self,
        key: OperationKey,
        old_candidates: FleetSet,
        new_candidates: FleetSet,
        priority: i32,
        seq: u64,
    ) {
        let entry = QueueKey::new(priority, seq, key);
        if !self.global.contains(&entry) {
            return;
        }
        for fleet in old_candidates {
            if !new_candidates.contains(fleet)
                && let Some(index) = self.by_fleet.get_mut(fleet)
            {
                index.remove(&entry);
            }
        }
        for fleet in new_candidates {
            if !old_candidates.contains(fleet)
                && let Some(index) = self.by_fleet.get_mut(fleet)
            {
                index.insert(entry);
            }
        }
    }

    /// Iterates over all queued operations in priority/sequence order.
    pub fn global_iter(&self) -> impl Iterator<Item = QueueKey> + '_ {
        self.global.iter().copied()
    }

    /// Iterates over queued operations eligible for `fleet` in priority/sequence order.
    #[must_use]
    pub fn fleet_iter(&self, fleet: usize) -> Option<impl Iterator<Item = QueueKey> + '_> {
        self.by_fleet.get(fleet).map(|index| index.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::Slab;

    fn test_key(slab: &mut Slab<u8>, tag: u8) -> OperationKey {
        let (index, generation) = slab.insert(tag).expect("slab insert");
        OperationKey::from_raw(index, generation)
    }

    #[test]
    fn orders_by_higher_priority_then_oldest_seq() {
        let mut slab = Slab::new();
        let k_low = test_key(&mut slab, 1);
        let k_mid_old = test_key(&mut slab, 2);
        let k_mid_new = test_key(&mut slab, 3);
        let k_gate = test_key(&mut slab, 4);
        let k_neg = test_key(&mut slab, 5);

        let fleets = FleetSet::try_from_indices([0, 2]).expect("valid fleets");
        let mut queue = OperationQueue::new(3).expect("valid fleet count");
        queue.enqueue(k_neg, fleets, -50, 1);
        queue.enqueue(k_low, fleets, 0, 2);
        queue.enqueue(k_mid_new, fleets, 100, 4);
        queue.enqueue(k_mid_old, fleets, 100, 3);
        queue.enqueue(k_gate, fleets, 200, 5);

        let expected = [
            QueueKey::new(200, 5, k_gate),
            QueueKey::new(100, 3, k_mid_old),
            QueueKey::new(100, 4, k_mid_new),
            QueueKey::new(0, 2, k_low),
            QueueKey::new(-50, 1, k_neg),
        ];

        let global: Vec<_> = queue.global_iter().collect();
        assert_eq!(global, expected);

        let f0: Vec<_> = queue.fleet_iter(0).expect("fleet 0").collect();
        let f1: Vec<_> = queue.fleet_iter(1).expect("fleet 1").collect();
        let f2: Vec<_> = queue.fleet_iter(2).expect("fleet 2").collect();
        assert_eq!(f0, expected);
        assert!(f1.is_empty());
        assert_eq!(f2, expected);
    }

    #[test]
    fn promote_reindexes_keeping_original_seq() {
        let mut slab = Slab::new();
        let first = test_key(&mut slab, 1);
        let second = test_key(&mut slab, 2);
        let third = test_key(&mut slab, 3);

        let fleets = FleetSet::try_from_indices([0]).expect("valid fleet");
        let mut queue = OperationQueue::new(1).expect("valid queue");
        queue.enqueue(first, fleets, 100, 1);
        queue.enqueue(second, fleets, 0, 2);
        queue.enqueue(third, fleets, 100, 3);

        assert!(queue.promote(second, fleets, 0, 100, 2));
        let ordered: Vec<_> = queue.global_iter().map(|entry| entry.key).collect();
        // `second` had seq=2, so at priority 100 it sits between `first` (seq=1) and `third` (seq=3).
        assert_eq!(ordered, [first, second, third]);
    }

    #[test]
    fn update_candidates_moves_only_affected_fleet_indices() {
        let mut slab = Slab::new();
        let op = test_key(&mut slab, 1);
        let old = FleetSet::try_from_indices([0, 1]).expect("old");
        let new = FleetSet::try_from_indices([1, 2]).expect("new");
        let mut queue = OperationQueue::new(3).expect("queue");
        queue.enqueue(op, old, 100, 7);
        queue.update_candidates(op, old, new, 100, 7);

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.fleet_len(0), 0);
        assert_eq!(queue.fleet_len(1), 1);
        assert_eq!(queue.fleet_len(2), 1);
    }

    #[test]
    fn rejects_excessive_fleet_count() {
        assert!(OperationQueue::new(MAX_FLEETS + 1).is_err());
    }
}
