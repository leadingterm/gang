//! Slab-indexed operation state, deduplication with priority promotion, watchers, and timeouts.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_types::{
    ActionResources, BuildId, CpuWeight, Digest, FleetSet, Mnemonic, OperationName, SourceId,
    TargetLabel,
};

use crate::qos;
use crate::queue::OperationQueue;

/// Monotonic and wall-clock reading passed explicitly into deterministic scheduler methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Now {
    /// Monotonic tick used for deadlines, timeouts, and elapsed intervals.
    pub tick: Tick,
    /// Wall-clock timestamp used for persistence, snapshots, and projections.
    pub wall: Timestamp,
}

/// Compact generation-checked `u32` slab reused by operations and workers.
#[derive(Debug, Clone)]
pub(crate) struct Slab<T> {
    slots: Vec<Slot<T>>,
    free_head: Option<u32>,
    len: usize,
}

#[derive(Debug, Clone)]
enum Slot<T> {
    Occupied {
        generation: u32,
        value: T,
    },
    Free {
        generation: u32,
        next_free: Option<u32>,
    },
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Slab<T> {
    /// Creates an empty slab.
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_head: None,
            len: 0,
        }
    }

    /// Returns the number of live entries in the slab.
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when the slab holds no live entries.
    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Inserts `value` and returns its `(index, generation)` handle, or `None` if `u32` capacity
    /// is exhausted.
    pub(crate) fn insert(&mut self, value: T) -> Option<(u32, u32)> {
        if let Some(index) = self.free_head {
            let slot_index = usize::try_from(index).ok()?;
            let slot = self.slots.get_mut(slot_index)?;
            let (generation, next_free) = match slot {
                Slot::Free {
                    generation,
                    next_free,
                } => (*generation, *next_free),
                Slot::Occupied { .. } => return None,
            };
            *slot = Slot::Occupied { generation, value };
            self.free_head = next_free;
            self.len += 1;
            return Some((index, generation));
        }

        let index = u32::try_from(self.slots.len()).ok()?;
        let generation = 1;
        self.slots.push(Slot::Occupied { generation, value });
        self.len += 1;
        Some((index, generation))
    }

    /// Borrows a live entry matching `(index, generation)`.
    #[must_use]
    pub(crate) fn get(&self, index: u32, generation: u32) -> Option<&T> {
        let slot_index = usize::try_from(index).ok()?;
        match self.slots.get(slot_index)? {
            Slot::Occupied {
                generation: slot_gen,
                value,
            } if *slot_gen == generation => Some(value),
            _ => None,
        }
    }

    /// Mutably borrows a live entry matching `(index, generation)`.
    pub(crate) fn get_mut(&mut self, index: u32, generation: u32) -> Option<&mut T> {
        let slot_index = usize::try_from(index).ok()?;
        match self.slots.get_mut(slot_index)? {
            Slot::Occupied {
                generation: slot_gen,
                value,
            } if *slot_gen == generation => Some(value),
            _ => None,
        }
    }

    /// Removes a live entry matching `(index, generation)`, bumping the slot's generation.
    pub(crate) fn remove(&mut self, index: u32, generation: u32) -> Option<T> {
        let slot_index = usize::try_from(index).ok()?;
        let slot = self.slots.get_mut(slot_index)?;
        let matches = matches!(
            slot,
            Slot::Occupied {
                generation: slot_gen,
                ..
            } if *slot_gen == generation
        );
        if !matches {
            return None;
        }
        let next_gen = generation.wrapping_add(1).max(1);
        let previous = std::mem::replace(
            slot,
            Slot::Free {
                generation: next_gen,
                next_free: self.free_head,
            },
        );
        self.free_head = Some(index);
        self.len -= 1;
        match previous {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Free { .. } => None,
        }
    }

    /// Iterates over all live `(index, generation, &value)` entries.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u32, u32, &T)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| match slot {
                Slot::Occupied { generation, value } => {
                    let index = u32::try_from(idx).ok()?;
                    Some((index, *generation, value))
                }
                Slot::Free { .. } => None,
            })
    }
}

/// Generation-checked `u32` handle for an [`Operation`] stored in [`Operations`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationKey {
    index: u32,
    generation: u32,
}

impl OperationKey {
    /// Constructs a key from raw `(index, generation)` parts.
    #[must_use]
    pub(crate) const fn from_raw(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    /// Returns the slab slot index.
    #[must_use]
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Returns the slot generation counter.
    #[must_use]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

/// Lifecycle stage of an [`Operation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationStage {
    /// Waiting in the priority queue for an eligible worker.
    Queued,
    /// Dispatched to a worker (offered or actively executing).
    Running,
    /// Reached a terminal execution/cache outcome.
    Completed,
    /// Cancelled explicitly or dropped after client-less timeout.
    Cancelled,
}

impl OperationStage {
    /// Returns `true` when the operation has reached a terminal stage.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

/// Deduplication key for non-`skip_cache_lookup` operations.
///
/// Operations with `skip_cache_lookup = true` are never inserted into or matched against the
/// deduplication table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey {
    /// Digest of the `REAPI` `Action`.
    pub action_digest: Digest,
    /// Canonicalized platform key distinguishing incompatible execution environments.
    pub platform_key: String,
}

impl DedupKey {
    /// Constructs a deduplication key from an action digest and canonical platform key.
    #[must_use]
    pub fn new(action_digest: Digest, platform_key: impl Into<String>) -> Self {
        Self {
            action_digest,
            platform_key: platform_key.into(),
        }
    }
}

/// Builds a canonical, collision-free platform key from borrowed `(key, value)` property pairs.
///
/// Pairs are sorted lexicographically and length-prefixed so arbitrary keys and values never
/// collide across delimiter boundaries.
#[must_use]
pub fn canonical_platform_key<'a>(
    properties: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
    let mut pairs: Vec<(&'a str, &'a str)> = properties.into_iter().collect();
    pairs.sort_unstable();
    let mut encoded = String::new();
    for (key, value) in pairs {
        let _ = write!(encoded, "{}:{key}={}:{value};", key.len(), value.len());
    }
    encoded
}

/// Retry and escalation step counters owned by an [`Operation`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryCounts {
    /// Generic non-resource execution retries consumed (`max_job_retries`).
    pub generic: u32,
    /// Worker-loss / ack-timeout retries consumed (`max_worker_loss_retries`).
    pub worker_loss: u32,
    /// Memory escalation steps consumed.
    pub memory_steps: u32,
    /// Disk escalation steps consumed.
    pub disk_steps: u32,
}

/// Parameters required to admit a new [`Operation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOperation {
    /// Unique `REAPI` operation resource name.
    pub name: OperationName,
    /// Digest of the `REAPI` `Action`.
    pub action_digest: Digest,
    /// Canonical platform key for deduplication.
    pub platform_key: String,
    /// Whether `ActionCache` lookup was skipped (`true` disables deduplication).
    pub skip_cache_lookup: bool,
    /// Whether the worker must skip writing the `ActionResult` to `ActionCache`.
    pub do_not_cache: bool,
    /// Authenticated first-creator source, if present.
    pub source: Option<SourceId>,
    /// First submitter's build identifier, if present.
    pub build: Option<BuildId>,
    /// Bazel target label from request metadata, if present.
    pub target: Option<TargetLabel>,
    /// Bazel mnemonic from request metadata, if present.
    pub mnemonic: Option<Mnemonic>,
    /// Candidate fleet indices in preference order.
    pub candidates: FleetSet,
    /// Initial `ExecutionPolicy.priority`.
    pub priority: i32,
    /// Fitted resource reservation for this operation.
    pub reservation: ActionResources,
    /// Pre-fit learned resource reservation, if different from `reservation`.
    pub learned_reservation: Option<ActionResources>,
    /// Initial active watcher count (normally `1` for `Execute`).
    pub initial_watchers: u32,
}

/// An admitted operation tracked by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    /// Unique `REAPI` operation resource name.
    pub name: OperationName,
    /// Digest of the `REAPI` `Action`.
    pub action_digest: Digest,
    /// Canonical platform key used for deduplication.
    pub platform_key: String,
    /// Whether `ActionCache` lookup was skipped.
    pub skip_cache_lookup: bool,
    /// Whether writing the `ActionResult` to `ActionCache` is disabled.
    pub do_not_cache: bool,
    /// First creator's source identifier.
    pub source: Option<SourceId>,
    /// First creator's build identifier.
    pub build: Option<BuildId>,
    /// First creator's target label.
    pub target: Option<TargetLabel>,
    /// First creator's action mnemonic.
    pub mnemonic: Option<Mnemonic>,
    /// Candidate fleets in preference order.
    pub candidates: FleetSet,
    /// Current effective priority (never lowered by deduplication).
    pub priority: i32,
    /// Current `cgroup` v2 `cpu.weight` derived from `priority`.
    pub cpu_weight: CpuWeight,
    /// Monotonic admission sequence number, preserved across promotions and requeues.
    pub seq: u64,
    /// Current resource reservation.
    pub reservation: ActionResources,
    /// Initial learned reservation snapshot prior to fitting.
    pub learned_reservation: Option<ActionResources>,
    /// Current lifecycle stage.
    pub stage: OperationStage,
    /// Number of active client streams watching this operation.
    pub watchers: u32,
    /// Monotonic tick when `watchers` dropped to zero while non-terminal.
    pub client_less_since: Option<Tick>,
    /// Timestamp and tick when the operation was first admitted/queued.
    pub queued_since: Now,
    /// Consumed retry and escalation step counters.
    pub retry_counts: RetryCounts,
}

/// Outcome of admitting an operation through [`Operations::admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitOutcome {
    /// A brand-new operation was inserted and enqueued.
    Inserted {
        /// Key of the newly inserted operation.
        key: OperationKey,
        /// Assigned monotonic sequence number.
        seq: u64,
        /// Assigned `cgroup` v2 CPU weight.
        cpu_weight: CpuWeight,
    },
    /// The request deduplicated onto an active operation.
    Deduplicated {
        /// Key of the existing active operation.
        key: OperationKey,
        /// `true` when incoming priority strictly exceeded the existing operation's priority.
        promoted: bool,
        /// Resulting priority after any promotion.
        priority: i32,
        /// Resulting `cpu.weight` after any promotion.
        cpu_weight: CpuWeight,
    },
    /// Admission failed because the operation name already exists or slab capacity is exhausted.
    Rejected(AdmitError),
}

/// Why [`Operations::admit`] refused to insert a new operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError {
    /// An operation with the same [`OperationName`] already exists.
    #[error("duplicate operation name")]
    DuplicateName,
    /// The operation slab reached `u32::MAX` entries.
    #[error("operation slab capacity exhausted")]
    SlabFull,
}

/// Result of calling [`Operations::unwatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnwatchResult {
    /// Remaining watcher count after decrementing.
    pub watchers: u32,
    /// `true` when this call dropped watchers to zero on a non-terminal operation.
    pub became_client_less: bool,
}

/// Outcome of expiring a client-less operation after `client_action_timeout`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientLessExpiry {
    /// Key of the expired operation.
    pub key: OperationKey,
    /// `REAPI` operation name.
    pub name: OperationName,
    /// Stage before expiry (`Queued` was dropped; `Running` was marked `Cancelled`).
    pub previous_stage: OperationStage,
    /// `true` when an active worker attempt must be sent `Kill(Cancelled)`.
    pub needs_kill: bool,
}

/// Slab-indexed registry of operations and their deduplication map.
#[derive(Debug, Clone, Default)]
pub struct Operations {
    slab: Slab<Operation>,
    by_name: HashMap<OperationName, OperationKey>,
    dedup: HashMap<DedupKey, OperationKey>,
    next_seq: u64,
}

impl Operations {
    /// Creates an empty operation registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slab: Slab::new(),
            by_name: HashMap::new(),
            dedup: HashMap::new(),
            next_seq: 1,
        }
    }

    /// Returns the number of operations currently retained in the slab.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slab.len()
    }

    /// Returns `true` when no operations are held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    /// Returns the number of active deduplication entries.
    #[must_use]
    pub fn dedup_len(&self) -> usize {
        self.dedup.len()
    }

    /// Borrows an operation by its generation-checked [`OperationKey`].
    #[must_use]
    pub fn get(&self, key: OperationKey) -> Option<&Operation> {
        self.slab.get(key.index, key.generation)
    }

    /// Mutably borrows an operation by its generation-checked [`OperationKey`].
    pub fn get_mut(&mut self, key: OperationKey) -> Option<&mut Operation> {
        self.slab.get_mut(key.index, key.generation)
    }

    /// Resolves an [`OperationName`] to its [`OperationKey`].
    #[must_use]
    pub fn lookup_name(&self, name: &OperationName) -> Option<OperationKey> {
        self.by_name.get(name).copied()
    }

    /// Looks up an active non-terminal operation by `(action_digest, platform_key)`.
    #[must_use]
    pub fn lookup_dedup(&self, action_digest: Digest, platform_key: &str) -> Option<OperationKey> {
        let lookup_key = DedupKey::new(action_digest, platform_key);
        let key = self.dedup.get(&lookup_key).copied()?;
        let op = self.get(key)?;
        (!op.stage.is_terminal()).then_some(key)
    }

    /// Admits a new operation or deduplicates onto an active operation with optional priority
    /// promotion.
    ///
    /// When deduplication hits a `Queued` operation with strictly higher priority, `priority` and
    /// `cpu_weight` are raised and `queue` is re-indexed preserving the operation's original
    /// `seq`. When deduplication hits a `Running` operation with higher priority, the promoted
    /// `priority` and `cpu_weight` are recorded on the operation for any future retry.
    pub fn admit(
        &mut self,
        spec: NewOperation,
        now: Now,
        queue: &mut OperationQueue,
    ) -> AdmitOutcome {
        if !spec.skip_cache_lookup
            && let Some(existing_key) = self.lookup_dedup(spec.action_digest, &spec.platform_key)
        {
            return self.deduplicate_onto(
                existing_key,
                spec.priority,
                spec.initial_watchers,
                queue,
            );
        }

        if self.by_name.contains_key(&spec.name) {
            return AdmitOutcome::Rejected(AdmitError::DuplicateName);
        }

        let seq = self.next_seq;
        let cpu_weight = qos::weight(spec.priority);
        let client_less_since = (spec.initial_watchers == 0).then_some(now.tick);
        let dedup_key = (!spec.skip_cache_lookup)
            .then(|| DedupKey::new(spec.action_digest, spec.platform_key.clone()));

        let operation = Operation {
            name: spec.name.clone(),
            action_digest: spec.action_digest,
            platform_key: spec.platform_key,
            skip_cache_lookup: spec.skip_cache_lookup,
            do_not_cache: spec.do_not_cache,
            source: spec.source,
            build: spec.build,
            target: spec.target,
            mnemonic: spec.mnemonic,
            candidates: spec.candidates,
            priority: spec.priority,
            cpu_weight,
            seq,
            reservation: spec.reservation,
            learned_reservation: spec.learned_reservation,
            stage: OperationStage::Queued,
            watchers: spec.initial_watchers,
            client_less_since,
            queued_since: now,
            retry_counts: RetryCounts::default(),
        };

        let Some((index, generation)) = self.slab.insert(operation) else {
            return AdmitOutcome::Rejected(AdmitError::SlabFull);
        };
        self.next_seq = self.next_seq.saturating_add(1);
        let key = OperationKey::from_raw(index, generation);
        self.by_name.insert(spec.name, key);
        if let Some(dedup_key) = dedup_key {
            self.dedup.insert(dedup_key, key);
        }
        queue.enqueue(key, spec.candidates, spec.priority, seq);

        AdmitOutcome::Inserted {
            key,
            seq,
            cpu_weight,
        }
    }

    fn deduplicate_onto(
        &mut self,
        key: OperationKey,
        incoming_priority: i32,
        incoming_watchers: u32,
        queue: &mut OperationQueue,
    ) -> AdmitOutcome {
        let Some(op) = self.slab.get_mut(key.index, key.generation) else {
            return AdmitOutcome::Rejected(AdmitError::DuplicateName);
        };

        if incoming_watchers > 0 {
            op.watchers = op.watchers.saturating_add(incoming_watchers);
            op.client_less_since = None;
        }

        let mut promoted = false;
        if incoming_priority > op.priority {
            let old_priority = op.priority;
            op.priority = incoming_priority;
            op.cpu_weight = qos::weight(incoming_priority);
            promoted = true;
            if op.stage == OperationStage::Queued {
                queue.promote(key, op.candidates, old_priority, op.priority, op.seq);
            }
        }

        AdmitOutcome::Deduplicated {
            key,
            promoted,
            priority: op.priority,
            cpu_weight: op.cpu_weight,
        }
    }

    /// Attaches a watcher to `key`, clearing any pending client-less deadline.
    ///
    /// Returns the updated watcher count, or `None` when `key` is invalid.
    pub fn watch(&mut self, key: OperationKey) -> Option<u32> {
        let op = self.get_mut(key)?;
        op.watchers = op.watchers.saturating_add(1);
        op.client_less_since = None;
        Some(op.watchers)
    }

    /// Attaches a watcher by [`OperationName`], returning its [`OperationKey`] when found.
    pub fn watch_by_name(&mut self, name: &OperationName) -> Option<OperationKey> {
        let key = self.lookup_name(name)?;
        self.watch(key)?;
        Some(key)
    }

    /// Detaches one watcher from `key`.
    ///
    /// When `watchers` reaches zero on a non-terminal operation, `client_less_since` is stamped
    /// with `now`.
    pub fn unwatch(&mut self, key: OperationKey, now: Tick) -> Option<UnwatchResult> {
        let op = self.get_mut(key)?;
        op.watchers = op.watchers.saturating_sub(1);
        let became_client_less =
            if op.watchers == 0 && !op.stage.is_terminal() && op.client_less_since.is_none() {
                op.client_less_since = Some(now);
                true
            } else {
                false
            };
        Some(UnwatchResult {
            watchers: op.watchers,
            became_client_less,
        })
    }

    /// Returns `true` when `key` is non-terminal, has zero watchers, and has been client-less
    /// for at least `timeout` at `now`.
    #[must_use]
    pub fn is_client_less_expired(&self, key: OperationKey, now: Tick, timeout: Duration) -> bool {
        let Some(op) = self.get(key) else {
            return false;
        };
        !op.stage.is_terminal()
            && op.watchers == 0
            && op
                .client_less_since
                .is_some_and(|since| now.saturating_duration_since(since) >= timeout)
    }

    /// Expires `key` if its client-less timeout has elapsed.
    ///
    /// Queued operations are dequeued and dropped from the registry; running operations are marked
    /// [`OperationStage::Cancelled`] and removed from the deduplication table, returning
    /// `needs_kill = true`.
    pub fn try_expire_client_less(
        &mut self,
        key: OperationKey,
        now: Tick,
        timeout: Duration,
        queue: &mut OperationQueue,
    ) -> Option<ClientLessExpiry> {
        if !self.is_client_less_expired(key, now, timeout) {
            return None;
        }
        let previous_stage = self.get(key)?.stage;
        match previous_stage {
            OperationStage::Queued => {
                let op = self.remove_internal(key, queue)?;
                Some(ClientLessExpiry {
                    key,
                    name: op.name,
                    previous_stage,
                    needs_kill: false,
                })
            }
            OperationStage::Running => {
                let name = {
                    let op = self.get_mut(key)?;
                    op.stage = OperationStage::Cancelled;
                    op.client_less_since = None;
                    op.name.clone()
                };
                self.remove_dedup_for(key);
                Some(ClientLessExpiry {
                    key,
                    name,
                    previous_stage,
                    needs_kill: true,
                })
            }
            OperationStage::Completed | OperationStage::Cancelled => None,
        }
    }

    /// Transitions a [`OperationStage::Queued`] operation to [`OperationStage::Running`] and
    /// removes it from `queue`.
    pub fn mark_running(&mut self, key: OperationKey, queue: &mut OperationQueue) -> bool {
        let Some(op) = self.get_mut(key) else {
            return false;
        };
        if op.stage != OperationStage::Queued {
            return false;
        }
        queue.dequeue(key, op.candidates, op.priority, op.seq);
        op.stage = OperationStage::Running;
        true
    }

    /// Requeues a [`OperationStage::Running`] operation at its current `priority` (including any
    /// promotion received while running) and original `seq`.
    pub fn requeue(&mut self, key: OperationKey, queue: &mut OperationQueue) -> bool {
        let Some(op) = self.get_mut(key) else {
            return false;
        };
        if op.stage != OperationStage::Running {
            return false;
        }
        op.stage = OperationStage::Queued;
        queue.enqueue(key, op.candidates, op.priority, op.seq);
        true
    }

    /// Transitions a non-terminal operation to [`OperationStage::Completed`], dequeuing if needed
    /// and removing its deduplication entry.
    pub fn complete(&mut self, key: OperationKey, queue: &mut OperationQueue) -> bool {
        let Some(op) = self.get_mut(key) else {
            return false;
        };
        if op.stage.is_terminal() {
            return false;
        }
        if op.stage == OperationStage::Queued {
            queue.dequeue(key, op.candidates, op.priority, op.seq);
        }
        op.stage = OperationStage::Completed;
        op.client_less_since = None;
        self.remove_dedup_for(key);
        true
    }

    /// Transitions a non-terminal operation to [`OperationStage::Cancelled`], returning its
    /// previous stage.
    pub fn cancel(
        &mut self,
        key: OperationKey,
        queue: &mut OperationQueue,
    ) -> Option<OperationStage> {
        let op = self.get_mut(key)?;
        if op.stage.is_terminal() {
            return None;
        }
        let previous = op.stage;
        if previous == OperationStage::Queued {
            queue.dequeue(key, op.candidates, op.priority, op.seq);
        }
        op.stage = OperationStage::Cancelled;
        op.client_less_since = None;
        self.remove_dedup_for(key);
        Some(previous)
    }

    /// Removes `key` from all indices and the slab.
    pub fn remove(&mut self, key: OperationKey, queue: &mut OperationQueue) -> Option<Operation> {
        self.remove_internal(key, queue)
    }

    /// Iterates over all live operations with their keys.
    pub fn iter(&self) -> impl Iterator<Item = (OperationKey, &Operation)> {
        self.slab
            .iter()
            .map(|(index, generation, op)| (OperationKey::from_raw(index, generation), op))
    }

    fn remove_dedup_for(&mut self, key: OperationKey) {
        let Some(op) = self.get(key) else {
            return;
        };
        if !op.skip_cache_lookup {
            let dedup_key = DedupKey::new(op.action_digest, op.platform_key.clone());
            if self.dedup.get(&dedup_key) == Some(&key) {
                self.dedup.remove(&dedup_key);
            }
        }
    }

    fn remove_internal(
        &mut self,
        key: OperationKey,
        queue: &mut OperationQueue,
    ) -> Option<Operation> {
        let op = self.slab.remove(key.index, key.generation)?;
        if op.stage == OperationStage::Queued {
            queue.dequeue(key, op.candidates, op.priority, op.seq);
        }
        if self.by_name.get(&op.name) == Some(&key) {
            self.by_name.remove(&op.name);
        }
        if !op.skip_cache_lookup {
            let dedup_key = DedupKey::new(op.action_digest, op.platform_key.clone());
            if self.dedup.get(&dedup_key) == Some(&key) {
                self.dedup.remove(&dedup_key);
            }
        }
        Some(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn new_op(name: &str, digest_seed: &[u8], priority: i32) -> NewOperation {
        NewOperation {
            name: OperationName::try_new(name).expect("valid op name"),
            action_digest: Digest::from_bytes(digest_seed),
            platform_key: canonical_platform_key([("ISA", "x86-64"), ("fleet", "default")]),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: None,
            build: None,
            target: None,
            mnemonic: None,
            candidates: FleetSet::try_from_indices([0]).expect("valid fleet"),
            priority,
            reservation: ActionResources::default(),
            learned_reservation: None,
            initial_watchers: 1,
        }
    }

    #[test]
    fn dedup_promotes_queued_operation_preserving_seq_and_never_lowers_priority() {
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");

        let AdmitOutcome::Inserted {
            key: first,
            seq: seq_first,
            ..
        } = ops.admit(new_op("operations/1", b"a", 100), now(1), &mut queue)
        else {
            panic!("expected insert");
        };
        let AdmitOutcome::Inserted {
            key: second,
            seq: seq_second,
            ..
        } = ops.admit(new_op("operations/2", b"b", 0), now(2), &mut queue)
        else {
            panic!("expected insert");
        };
        let AdmitOutcome::Inserted {
            key: third,
            seq: seq_third,
            ..
        } = ops.admit(new_op("operations/3", b"c", 100), now(3), &mut queue)
        else {
            panic!("expected insert");
        };
        assert_eq!((seq_first, seq_second, seq_third), (1, 2, 3));

        // Dedup onto `second` with priority 100 -> promotes and updates CpuWeight, keeping seq=2.
        let outcome = ops.admit(new_op("operations/2-dup", b"b", 100), now(4), &mut queue);
        assert_eq!(
            outcome,
            AdmitOutcome::Deduplicated {
                key: second,
                promoted: true,
                priority: 100,
                cpu_weight: CpuWeight::INTERACTIVE,
            }
        );
        let second_op = ops.get(second).expect("second exists");
        assert_eq!(second_op.seq, 2);
        assert_eq!(second_op.watchers, 2);
        assert_eq!(second_op.priority, 100);
        assert_eq!(second_op.cpu_weight, CpuWeight::INTERACTIVE);

        let order: Vec<_> = queue.global_iter().map(|q| q.key).collect();
        assert_eq!(order, [first, second, third]);

        // Lower incoming priority must not demote.
        let lower = ops.admit(new_op("operations/2-low", b"b", -10), now(5), &mut queue);
        assert_eq!(
            lower,
            AdmitOutcome::Deduplicated {
                key: second,
                promoted: false,
                priority: 100,
                cpu_weight: CpuWeight::INTERACTIVE,
            }
        );
        assert_eq!(ops.get(second).expect("exists").watchers, 3);

        // Higher incoming priority promotes to Gate tier (1000).
        let gate = ops.admit(new_op("operations/2-gate", b"b", 250), now(6), &mut queue);
        assert_eq!(
            gate,
            AdmitOutcome::Deduplicated {
                key: second,
                promoted: true,
                priority: 250,
                cpu_weight: CpuWeight::GATE,
            }
        );
        let order_after_gate: Vec<_> = queue.global_iter().map(|q| q.key).collect();
        assert_eq!(order_after_gate, [second, first, third]);
    }

    #[test]
    fn dedup_on_running_operation_applies_on_requeue_and_skips_skip_cache_lookup() {
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");

        let AdmitOutcome::Inserted { key, .. } =
            ops.admit(new_op("operations/run", b"work", 10), now(1), &mut queue)
        else {
            panic!("expected insert");
        };
        assert!(ops.mark_running(key, &mut queue));
        assert!(queue.is_empty());

        // Deduplicating while Running records the promotion without enqueueing yet.
        let dedup = ops.admit(
            new_op("operations/run-dup", b"work", 200),
            now(2),
            &mut queue,
        );
        assert_eq!(
            dedup,
            AdmitOutcome::Deduplicated {
                key,
                promoted: true,
                priority: 200,
                cpu_weight: CpuWeight::GATE,
            }
        );
        assert!(queue.is_empty());

        // Requeue uses the promoted priority and original seq=1.
        assert!(ops.requeue(key, &mut queue));
        let head = queue.global_iter().next().expect("queued");
        assert_eq!(head, crate::queue::QueueKey::new(200, 1, key));

        // `skip_cache_lookup = true` never deduplicates and never registers in dedup map.
        let mut no_cache = new_op("operations/nocache", b"work", 10);
        no_cache.skip_cache_lookup = true;
        let AdmitOutcome::Inserted { key: k2, .. } = ops.admit(no_cache, now(3), &mut queue) else {
            panic!("skip_cache_lookup must insert a distinct operation");
        };
        assert_ne!(k2, key);

        // Distinct platform_key does not deduplicate either.
        let mut other_plat = new_op("operations/arm", b"work", 10);
        other_plat.platform_key = canonical_platform_key([("ISA", "aarch64")]);
        let AdmitOutcome::Inserted { key: k3, .. } = ops.admit(other_plat, now(4), &mut queue)
        else {
            panic!("distinct platform must insert a distinct operation");
        };
        assert_ne!(k3, key);
    }

    #[test]
    fn client_less_timeout_drops_queued_and_cancels_running() {
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let timeout = Duration::from_mins(1);

        let AdmitOutcome::Inserted { key: queued_k, .. } =
            ops.admit(new_op("operations/q", b"q", 0), now(10), &mut queue)
        else {
            panic!("expected insert");
        };
        let AdmitOutcome::Inserted { key: running_k, .. } =
            ops.admit(new_op("operations/r", b"r", 0), now(10), &mut queue)
        else {
            panic!("expected insert");
        };
        assert!(ops.mark_running(running_k, &mut queue));

        // Detach watcher at t=20s.
        assert_eq!(
            ops.unwatch(queued_k, now(20).tick),
            Some(UnwatchResult {
                watchers: 0,
                became_client_less: true,
            })
        );
        assert_eq!(
            ops.unwatch(running_k, now(20).tick),
            Some(UnwatchResult {
                watchers: 0,
                became_client_less: true,
            })
        );

        // Before deadline (t=79s): neither expires.
        assert_eq!(
            ops.try_expire_client_less(queued_k, now(79).tick, timeout, &mut queue),
            None
        );
        assert_eq!(
            ops.try_expire_client_less(running_k, now(79).tick, timeout, &mut queue),
            None
        );

        // Re-watching cancels the client-less timer.
        assert_eq!(ops.watch(queued_k), Some(1));
        assert_eq!(
            ops.try_expire_client_less(queued_k, now(100).tick, timeout, &mut queue),
            None
        );

        // Unwatch again at t=100s, expire at t=160s.
        ops.unwatch(queued_k, now(100).tick);
        let q_expired = ops
            .try_expire_client_less(queued_k, now(160).tick, timeout, &mut queue)
            .expect("queued expires");
        assert_eq!(q_expired.previous_stage, OperationStage::Queued);
        assert!(!q_expired.needs_kill);
        assert!(ops.get(queued_k).is_none());
        assert!(queue.is_empty());

        let r_expired = ops
            .try_expire_client_less(running_k, now(80).tick, timeout, &mut queue)
            .expect("running expires");
        assert_eq!(r_expired.previous_stage, OperationStage::Running);
        assert!(r_expired.needs_kill);
        assert_eq!(
            ops.get(running_k).expect("retained cancelled op").stage,
            OperationStage::Cancelled
        );
        assert_eq!(ops.dedup_len(), 0);
    }

    #[test]
    fn slab_generation_prevents_stale_key_aliasing() {
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let AdmitOutcome::Inserted { key: old_key, .. } =
            ops.admit(new_op("operations/a", b"1", 0), now(1), &mut queue)
        else {
            panic!("expected insert");
        };
        assert!(ops.remove(old_key, &mut queue).is_some());
        let AdmitOutcome::Inserted { key: new_key, .. } =
            ops.admit(new_op("operations/b", b"2", 0), now(2), &mut queue)
        else {
            panic!("expected insert");
        };
        assert_eq!(old_key.index(), new_key.index());
        assert_ne!(old_key.generation(), new_key.generation());
        assert!(ops.get(old_key).is_none());
        assert!(ops.get(new_key).is_some());
    }
}
