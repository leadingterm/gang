//! Bounded log2 resource histograms, per-key `LRU` families, and v3 snapshot codec.
//!
//! Ported as a pure, single-owner state machine from the `NativeLink` fork's `resource_sketch`.
//! A sample counts for between half a window and a whole one (two half-window histograms per
//! entry). Kill floors expire with the window. Command keys stay on probation until their second
//! sighting or a kill.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher, Hash};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_types::Percent;

/// Buckets of the log2 histograms: bucket `0` for values under `1000`, bucket `i` for
/// `1000 * 2^(i - 1) <= v < 1000 * 2^i`.
pub const HIST_BUCKETS: usize = 25;

/// Sampler generation carried in snapshots and v1 persisted records.
pub const SAMPLER_GENERATION: u32 = 2;

/// Fixed byte charge for an entry and its map/`Arc` bookkeeping.
pub const ENTRY_BYTES: u64 = (size_of::<Entry>() + size_of::<SketchKey>() + 160 + 256) as u64;

/// Conservative byte charge for a probation node, its `Arc` header, and its hash slot.
pub const PROBATION_ENTRY_BYTES: u64 = 256;

/// Reserved hash-table storage per configured slot, even when the `LRU` is empty.
const HASH_SLOT_BYTES: u64 = 64;

/// Fixed overhead charged for maps, shard headers, and sentinel slots.
const FIXED_BYTES: u64 = (SHARDS
    * (size_of::<Inner>() + 6 * (size_of::<Entry>() + size_of::<SketchKey>() + 32) + 2 * 128)
    + size_of::<ResourceSketch>()
    + 1024) as u64;

/// Default shard count across which families are distributed by key hash.
pub const SHARDS: usize = 32;

/// Default `memory_budget_bytes`: about 300k keys.
pub const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 128 << 20;

/// Default retention window in days.
pub const DEFAULT_WINDOW_DAYS: u64 = 7;

/// Default reservation headroom percentage.
pub const DEFAULT_HEADROOM_PERCENT: u64 = 20;

const SNAPSHOT_MAGIC: &[u8; 4] = b"NLHS";
const SNAPSHOT_VERSION: u8 = 3;

/// An `O(1)` bounded `LRU` map backed by an index-linked list and a `HashMap`.
#[derive(Debug, Clone)]
pub(crate) struct LruMap<K, V> {
    cap: NonZeroUsize,
    map: HashMap<K, usize>,
    slots: Vec<Option<Slot<K, V>>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
}

#[derive(Debug, Clone)]
struct Slot<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

impl<K, V> LruMap<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new(cap: NonZeroUsize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    pub(crate) const fn cap(&self) -> NonZeroUsize {
        self.cap
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.slots.clear();
        self.free.clear();
        self.head = None;
        self.tail = None;
    }

    pub(crate) fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.map.contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn peek<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = *self.map.get(key)?;
        self.slots
            .get(index)
            .and_then(Option::as_ref)
            .map(|slot| &slot.value)
    }

    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = *self.map.get(key)?;
        self.move_to_front(index);
        self.slots
            .get(index)
            .and_then(Option::as_ref)
            .map(|slot| &slot.value)
    }

    pub(crate) fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = *self.map.get(key)?;
        self.move_to_front(index);
        self.slots
            .get_mut(index)
            .and_then(Option::as_mut)
            .map(|slot| &mut slot.value)
    }

    pub(crate) fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if let Some(&index) = self.map.get(&key) {
            self.move_to_front(index);
            if let Some(Some(slot)) = self.slots.get_mut(index) {
                let old = std::mem::replace(&mut slot.value, value);
                return Some((slot.key.clone(), old));
            }
        }
        let evicted = if self.len() >= self.cap.get() {
            self.pop_lru()
        } else {
            None
        };
        let index = if let Some(free) = self.free.pop() {
            free
        } else {
            let idx = self.slots.len();
            self.slots.push(None);
            idx
        };
        let old_head = self.head;
        self.slots[index] = Some(Slot {
            key: key.clone(),
            value,
            prev: None,
            next: old_head,
        });
        if let Some(head_idx) = old_head
            && let Some(Some(head)) = self.slots.get_mut(head_idx)
        {
            head.prev = Some(index);
        }
        self.head = Some(index);
        if self.tail.is_none() {
            self.tail = Some(index);
        }
        self.map.insert(key, index);
        evicted
    }

    pub(crate) fn pop<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = self.map.remove(key)?;
        self.remove_slot(index).map(|(_, value)| value)
    }

    pub(crate) fn pop_lru(&mut self) -> Option<(K, V)> {
        let index = self.tail?;
        let removed = self.remove_slot(index)?;
        self.map.remove(&removed.0);
        Some(removed)
    }

    /// Iterates from most recently used to least recently used.
    pub(crate) fn iter(&self) -> LruIter<'_, K, V> {
        LruIter {
            slots: &self.slots,
            next: self.head,
        }
    }

    /// Iterates from least recently used to most recently used.
    pub(crate) fn iter_rev(&self) -> LruRevIter<'_, K, V> {
        LruRevIter {
            slots: &self.slots,
            prev: self.tail,
        }
    }

    /// Iterates mutably from most recently used to least recently used without changing recency.
    pub(crate) fn for_each_mut(&mut self, mut visit: impl FnMut(&K, &mut V)) {
        let mut cursor = self.head;
        while let Some(index) = cursor {
            let Some(Some(slot)) = self.slots.get_mut(index) else {
                break;
            };
            let next = slot.next;
            visit(&slot.key, &mut slot.value);
            cursor = next;
        }
    }

    fn move_to_front(&mut self, index: usize) {
        if self.head == Some(index) {
            return;
        }
        let Some(Some(slot)) = self.slots.get(index) else {
            return;
        };
        let prev = slot.prev;
        let next = slot.next;
        if let Some(prev_idx) = prev
            && let Some(Some(prev_slot)) = self.slots.get_mut(prev_idx)
        {
            prev_slot.next = next;
        }
        if let Some(next_idx) = next
            && let Some(Some(next_slot)) = self.slots.get_mut(next_idx)
        {
            next_slot.prev = prev;
        } else if self.tail == Some(index) {
            self.tail = prev;
        }
        let old_head = self.head;
        if let Some(Some(slot)) = self.slots.get_mut(index) {
            slot.prev = None;
            slot.next = old_head;
        }
        if let Some(head_idx) = old_head
            && let Some(Some(head_slot)) = self.slots.get_mut(head_idx)
        {
            head_slot.prev = Some(index);
        }
        self.head = Some(index);
        if self.tail.is_none() {
            self.tail = Some(index);
        }
    }

    fn remove_slot(&mut self, index: usize) -> Option<(K, V)> {
        let slot = self.slots.get_mut(index)?.take()?;
        self.free.push(index);
        if let Some(prev_idx) = slot.prev
            && let Some(Some(prev_slot)) = self.slots.get_mut(prev_idx)
        {
            prev_slot.next = slot.next;
        } else {
            self.head = slot.next;
        }
        if let Some(next_idx) = slot.next
            && let Some(Some(next_slot)) = self.slots.get_mut(next_idx)
        {
            next_slot.prev = slot.prev;
        } else {
            self.tail = slot.prev;
        }
        Some((slot.key, slot.value))
    }
}

pub(crate) struct LruIter<'a, K, V> {
    slots: &'a [Option<Slot<K, V>>],
    next: Option<usize>,
}

impl<'a, K, V> Iterator for LruIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next?;
        let slot = self.slots.get(index)?.as_ref()?;
        self.next = slot.next;
        Some((&slot.key, &slot.value))
    }
}

pub(crate) struct LruRevIter<'a, K, V> {
    slots: &'a [Option<Slot<K, V>>],
    prev: Option<usize>,
}

impl<'a, K, V> Iterator for LruRevIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.prev?;
        let slot = self.slots.get(index)?.as_ref()?;
        self.prev = slot.prev;
        Some((&slot.key, &slot.value))
    }
}

/// Errors decoding or loading a [`Snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SketchError {
    /// The encoded payload does not start with the `NLHS` magic header.
    #[error("resource sketch snapshot: not a snapshot")]
    InvalidMagic,
    /// The snapshot format version is not supported.
    #[error("resource sketch snapshot: unsupported version {version}")]
    UnsupportedVersion {
        /// Version byte found in the header.
        version: u8,
    },
    /// The sampler generation does not match [`SAMPLER_GENERATION`].
    #[error(
        "resource sketch snapshot: incompatible sampler generation {found} (expected {expected})"
    )]
    IncompatibleSampler {
        /// Sampler generation in the snapshot.
        found: u32,
        /// Current sampler generation.
        expected: u32,
    },
    /// The encoded entry count exceeds the caller's budget or payload bounds.
    #[error("resource sketch snapshot: entry count exceeds budget or payload")]
    InvalidEntryCount,
    /// A key tag byte is unknown.
    #[error("resource sketch snapshot: unknown key tag {tag}")]
    UnknownKeyTag {
        /// Key tag byte found.
        tag: u8,
    },
    /// A string field is not valid UTF-8.
    #[error("resource sketch snapshot: invalid UTF-8 string")]
    InvalidUtf8,
    /// A histogram declares more than [`HIST_BUCKETS`] entries.
    #[error("resource sketch snapshot: invalid histogram count {count}")]
    InvalidHistogramCount {
        /// Nonzero bucket count declared.
        count: usize,
    },
    /// A histogram entry names an out-of-range bucket index.
    #[error("resource sketch snapshot: invalid histogram bucket {bucket}")]
    InvalidHistogramBucket {
        /// Bucket index found.
        bucket: usize,
    },
    /// The payload ended before the declared fields finished.
    #[error("resource sketch snapshot: truncated at byte {offset}")]
    Truncated {
        /// Byte offset where decoding failed.
        offset: usize,
    },
    /// Unparsed trailing bytes remained after decoding all entries.
    #[error("resource sketch snapshot: {remaining} trailing bytes")]
    TrailingBytes {
        /// Number of trailing bytes.
        remaining: usize,
    },
}

/// What a measurement is filed under, chosen from what the client sent.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum SketchKey {
    /// The client named a target; `mnemonic` is `None` when it named no mnemonic.
    Target {
        /// Target label.
        target: Arc<str>,
        /// Optional action mnemonic.
        mnemonic: Option<Arc<str>>,
    },
    /// The client named a mnemonic and no target.
    Mnemonic {
        /// Action mnemonic.
        mnemonic: Arc<str>,
    },
    /// The client named neither: the command digest as `<hash>/<size>` or `<hash>-<size>`.
    Command {
        /// Command digest string.
        digest: Arc<str>,
    },
}

impl SketchKey {
    /// The key for an attempt: the target when the client sent one, else the mnemonic, else the
    /// command digest. Empty strings count as absent.
    #[must_use]
    pub fn for_attempt(target: Option<&str>, mnemonic: Option<&str>, command_digest: &str) -> Self {
        let target = target.filter(|s| !s.is_empty());
        let mnemonic = mnemonic.filter(|s| !s.is_empty());
        match (target, mnemonic) {
            (Some(target), mnemonic) => Self::Target {
                target: Arc::from(target),
                mnemonic: mnemonic.map(Arc::from),
            },
            (None, Some(mnemonic)) => Self::Mnemonic {
                mnemonic: Arc::from(mnemonic),
            },
            (None, None) => Self::Command {
                digest: Arc::from(command_digest),
            },
        }
    }

    /// Returns which `LRU` family owns this key.
    #[must_use]
    pub const fn family(&self) -> Family {
        match self {
            Self::Target { .. } => Family::Target,
            Self::Mnemonic { .. } => Family::Mnemonic,
            Self::Command { .. } => Family::Command,
        }
    }
}

/// The three independent `LRU` families so a flood of nameless actions cannot evict target keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    /// Target-keyed entries.
    Target,
    /// Mnemonic-only aggregate entries.
    Mnemonic,
    /// Command-digest entries.
    Command,
}

impl Family {
    /// All families in canonical index order.
    pub const ALL: [Self; 3] = [Self::Target, Self::Mnemonic, Self::Command];

    /// Stable human-readable family name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Target => "target",
            Self::Mnemonic => "mnemonic",
            Self::Command => "command",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Target => 0,
            Self::Mnemonic => 1,
            Self::Command => 2,
        }
    }
}

/// A log2 histogram matching the rollup buckets (`0`: `< 1000`, `i`: `[1000*2^(i-1), 1000*2^i)`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hist(pub [u64; HIST_BUCKETS]);

impl Hist {
    /// Returns the bucket index for `value`.
    #[must_use]
    pub fn bucket(value: u64) -> usize {
        if value < 1000 {
            return 0;
        }
        let exponent = usize::try_from((value / 1000).ilog2()).unwrap_or(HIST_BUCKETS) + 1;
        exponent.min(HIST_BUCKETS - 1)
    }

    /// Records one observation into its log2 bucket.
    pub fn add(&mut self, value: u64) {
        let bucket = &mut self.0[Self::bucket(value)];
        *bucket = bucket.saturating_add(1);
    }

    /// Returns the total sample count across all buckets.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.0.iter().fold(0_u64, |sum, &n| sum.saturating_add(n))
    }

    /// Upper bound of the bucket containing the 95th percentile sample, or `0` when empty.
    #[must_use]
    pub fn p95_upper(&self) -> u64 {
        self.p95_upper_with(&Self::default())
    }

    /// Upper bound of the 95th percentile bucket across `self` and `other` combined.
    #[must_use]
    pub fn p95_upper_with(&self, other: &Self) -> u64 {
        self.percentile_upper_with(other, Percent::from_percent(95))
    }

    /// Upper bound of the selected percentile bucket across `self` and `other` combined.
    #[must_use]
    pub fn percentile_upper_with(&self, other: &Self, percentile: Percent) -> u64 {
        let total: u128 = self
            .0
            .iter()
            .chain(other.0.iter())
            .map(|&n| u128::from(n))
            .sum();
        if total == 0 {
            return 0;
        }
        let target = (total * u128::from(percentile.get())).div_ceil(100);
        let mut seen = 0_u128;
        for (i, (&a, &b)) in self.0.iter().zip(other.0.iter()).enumerate() {
            seen += u128::from(a) + u128::from(b);
            if seen >= target {
                return 1000_u64 << i;
            }
        }
        0
    }
}

/// Samples collected during a single half-window: histograms and exact maxima.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Half {
    /// Peak memory samples in `KiB`.
    pub memory: Hist,
    /// Lifetime-average CPU samples in millicores (`cpu_time_ms * 1000 / wall_time_ms`).
    pub cpu: Hist,
    /// Peak private disk samples in `KiB`.
    pub disk: Hist,
    /// Exact maximum memory peak in `KiB`.
    pub max_memory_kb: u64,
    /// Exact maximum lifetime-average CPU in millicores.
    pub max_cpu_millicores: u64,
    /// Exact maximum disk peak in `KiB`.
    pub max_disk_kb: u64,
}

/// Learned measurements and kill floors for one key across two half-windows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    /// Cumulative number of samples recorded for this entry.
    pub samples: u32,
    /// Current half-window samples.
    pub current: Half,
    /// Previous half-window samples.
    pub previous: Half,
    /// Largest measured memory kill peak (or reservation when unavailable) in `KiB`.
    pub killed_peak_memory_kb: u64,
    /// Unix seconds when `killed_peak_memory_kb` was recorded.
    pub killed_memory_s: u64,
    /// Largest measured disk kill peak (or reservation when unavailable) in `KiB`.
    pub killed_peak_disk_kb: u64,
    /// Unix seconds when `killed_peak_disk_kb` was recorded.
    pub killed_disk_s: u64,
    /// Unix seconds of the most recent sample.
    pub last_seen_s: u64,
    /// Half-window period index `now_s / (window_s / 2)` for `current`.
    pub period: u64,
    /// Whether this entry came from a hints seed (replaced whole by the first real sample).
    pub seeded: bool,
}

/// One attempt's resource measurements passed to [`ResourceSketch::observe`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Observation {
    /// Peak memory observed in `KiB`.
    pub peak_memory_kb: u64,
    /// Lifetime-average CPU millicores, or `None` when no valid wall time was reported.
    pub cpu_millicores: Option<u64>,
    /// Peak disk observed in `KiB`.
    pub peak_disk_kb: u64,
    /// Measured peak (or reservation) if the attempt suffered a memory kill.
    pub killed_peak_memory_kb: Option<u64>,
    /// Measured peak (or reservation) if the attempt suffered a disk kill.
    pub killed_peak_disk_kb: Option<u64>,
    /// Whether the attempt ended in any resource kill (bypasses command probation).
    pub killed: bool,
    /// Observation timestamp in seconds since the Unix epoch.
    pub now_s: u64,
}

/// A reservation shaped from an [`Entry`]. Zero indicates an unstated dimension.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Shaped {
    /// CPU reservation in millicores.
    pub cpu_count: u64,
    /// Memory reservation in `KiB`.
    pub memory_kb: u64,
    /// Disk reservation in `KiB`.
    pub disk_kb: u64,
    /// Optional matched size class name.
    pub class: Option<String>,
}

impl Shaped {
    /// Returns whether all three resource dimensions are zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.cpu_count == 0 && self.memory_kb == 0 && self.disk_kb == 0
    }
}

/// A named point on a fleet's size ladder (optional; zero leaves a dimension unstated).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SizeClass {
    /// Class name.
    pub name: String,
    /// CPU reservation in millicores.
    pub cpu_count: u64,
    /// Memory reservation in `KiB`.
    pub memory_kb: u64,
    /// Disk reservation in `KiB`.
    pub disk_kb: u64,
}

/// Policy parameters governing how [`ResourceSketch::shape`] computes reservations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Percentage headroom added on top of learned statistics and kill floors.
    pub headroom_percent: u64,
    /// Percentile of lifetime-average millicores used for CPU reservations.
    pub cpu_reservation_percentile: Percent,
    /// Optional ascending size classes.
    pub classes: Vec<SizeClass>,
    /// Whether the fleet reserves CPU.
    pub reserve_cpu: bool,
    /// Whether the fleet reserves memory.
    pub reserve_memory: bool,
    /// Whether the fleet reserves disk.
    pub reserve_disk: bool,
}

/// Per-family key capacities derived from a byte budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Capacity of the `Target` family.
    pub target: usize,
    /// Capacity of the `Mnemonic` family.
    pub mnemonic: usize,
    /// Capacity of the `Command` family.
    pub command: usize,
    /// Capacity of the `Command` probation cache.
    pub probation: usize,
}

impl Budget {
    /// Computes per-family entry limits from a total memory budget in bytes.
    ///
    /// Reserves three quarters for snapshot encoding/loading and transient storage; of the
    /// resident quarter, 10% funds probation and the remainder splits 80/10/10.
    #[must_use]
    pub fn from_bytes(bytes: u64) -> Self {
        let resident = bytes.saturating_sub(FIXED_BYTES) / 4;
        let probation = resident / 10;
        let entries = usize::try_from((resident - probation) / ENTRY_BYTES).unwrap_or(usize::MAX);
        let tenth = entries / 10;
        Self {
            target: (entries - 2 * tenth).max(1),
            mnemonic: tenth.max(1),
            command: tenth.max(1),
            probation: usize::try_from(probation / PROBATION_ENTRY_BYTES)
                .unwrap_or(usize::MAX)
                .max(1),
        }
    }
}

/// Occupancy and eviction counters across all shards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Current entry counts per family (`[Target, Mnemonic, Command]`).
    pub entries: [usize; 3],
    /// Configured capacity per family.
    pub capacity: [usize; 3],
    /// Cumulative evictions per family.
    pub evictions: [u64; 3],
    /// Cumulative recorded observations per family.
    pub samples: [u64; 3],
    /// Current entries in the command probation cache.
    pub probation: usize,
}

struct Inner {
    families: [LruMap<SketchKey, Entry>; 3],
    probation: LruMap<Arc<str>, ()>,
    evictions: [u64; 3],
    samples: [u64; 3],
    bytes: [u64; 3],
    probation_bytes: u64,
}

fn key_bytes(key: &SketchKey) -> u64 {
    match key {
        SketchKey::Target { target, mnemonic } => {
            (target.len() + mnemonic.as_ref().map_or(0, |m| m.len())) as u64
        }
        SketchKey::Mnemonic { mnemonic } => mnemonic.len() as u64,
        SketchKey::Command { digest } => digest.len() as u64,
    }
}

fn entry_charge(key: &SketchKey) -> u64 {
    ENTRY_BYTES - HASH_SLOT_BYTES - 256 + key_bytes(key)
}

const fn probation_charge(digest: &str) -> u64 {
    PROBATION_ENTRY_BYTES - HASH_SLOT_BYTES - 128 + digest.len() as u64
}

impl Inner {
    fn insert(&mut self, key: SketchKey, entry: Entry) -> bool {
        // Snapshot strings use u32 lengths; never retain a key that would be truncated.
        let encodable = match &key {
            SketchKey::Target { target, mnemonic } => {
                u32::try_from(target.len()).is_ok()
                    && mnemonic
                        .as_ref()
                        .is_none_or(|value| u32::try_from(value.len()).is_ok())
            }
            SketchKey::Mnemonic { mnemonic } => u32::try_from(mnemonic.len()).is_ok(),
            SketchKey::Command { digest } => u32::try_from(digest.len()).is_ok(),
        };
        if !encodable {
            return false;
        }
        let i = key.family().index();
        let map = &mut self.families[i];
        let allowance = map.cap().get() as u64 * (ENTRY_BYTES - HASH_SLOT_BYTES);
        let charge = entry_charge(&key);
        if charge > allowance {
            return false;
        }
        while self.bytes[i] + charge > allowance || map.len() == map.cap().get() {
            let Some((removed, _)) = map.pop_lru() else {
                break;
            };
            self.bytes[i] -= entry_charge(&removed);
            self.evictions[i] += 1;
        }
        map.put(key, entry);
        self.bytes[i] += charge;
        true
    }

    fn remember_probation(&mut self, digest: Arc<str>) {
        let allowance =
            self.probation.cap().get() as u64 * (PROBATION_ENTRY_BYTES - HASH_SLOT_BYTES);
        let charge = probation_charge(&digest);
        if charge > allowance {
            return;
        }
        while self.probation_bytes + charge > allowance
            || self.probation.len() == self.probation.cap().get()
        {
            let Some((removed, ())) = self.probation.pop_lru() else {
                break;
            };
            self.probation_bytes -= probation_charge(&removed);
        }
        self.probation.put(digest, ());
        self.probation_bytes += charge;
    }
}

/// Bounded per-key resource measurement sketch across three `LRU` families.
pub struct ResourceSketch {
    shards: Box<[Inner]>,
    hasher: BuildHasherDefault<DefaultHasher>,
    policy: Policy,
    window: Duration,
    budget: Budget,
}

impl std::fmt::Debug for ResourceSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceSketch")
            .field("budget", &self.budget)
            .field("window", &self.window)
            .field("shards", &self.shards.len())
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn cache(capacity: usize) -> LruMap<SketchKey, Entry> {
    LruMap::new(NonZeroUsize::new(capacity.max(1)).unwrap_or(NonZeroUsize::MIN))
}

impl ResourceSketch {
    /// Creates a new sketch using the default [`SHARDS`] count.
    #[must_use]
    pub fn new(policy: Policy, budget: Budget, window: Duration) -> Self {
        Self::with_shards(policy, budget, window, SHARDS)
    }

    /// Creates a sketch distributed across `shards` (clamped to `1..=SHARDS` and the minimum
    /// family capacity). Passing `1` yields a single deterministic `LRU` order.
    #[must_use]
    pub fn with_shards(policy: Policy, budget: Budget, window: Duration, shards: usize) -> Self {
        let shards = shards.clamp(1, SHARDS).min(
            budget
                .target
                .min(budget.mnemonic)
                .min(budget.command)
                .min(budget.probation)
                .max(1),
        );
        let per = |capacity: usize, shard: usize| {
            (capacity / shards + usize::from(shard < capacity % shards)).max(1)
        };
        let inners = (0..shards)
            .map(|shard| Inner {
                families: [
                    cache(per(budget.target, shard)),
                    cache(per(budget.mnemonic, shard)),
                    cache(per(budget.command, shard)),
                ],
                probation: LruMap::new(
                    NonZeroUsize::new(per(budget.probation, shard)).unwrap_or(NonZeroUsize::MIN),
                ),
                evictions: [0; 3],
                samples: [0; 3],
                bytes: [0; 3],
                probation_bytes: 0,
            })
            .collect();
        Self {
            shards: inners,
            hasher: BuildHasherDefault::default(),
            policy,
            window,
            budget,
        }
    }

    fn shard_mut(&mut self, key: &SketchKey) -> &mut Inner {
        let hash = self.hasher.hash_one(key);
        let index = usize::try_from(hash % self.shards.len() as u64).unwrap_or(0);
        &mut self.shards[index]
    }

    const fn window_s(&self) -> u64 {
        self.window.as_secs()
    }

    /// Returns the shaping policy.
    #[must_use]
    pub const fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Returns the family capacities.
    #[must_use]
    pub const fn budget(&self) -> Budget {
        self.budget
    }

    /// Returns the retention window.
    #[must_use]
    pub const fn window(&self) -> Duration {
        self.window
    }

    /// Removes all learned entries and probation while retaining cumulative statistics.
    pub fn clear(&mut self) {
        for inner in &mut self.shards {
            for family in &mut inner.families {
                family.clear();
            }
            inner.probation.clear();
            inner.bytes = [0; 3];
            inner.probation_bytes = 0;
        }
    }

    /// Records one attempt observation.
    ///
    /// A `Command` key with no entry yet goes on probation on its first non-killed sighting and
    /// is recorded on its second sighting or on a kill. A `Target` key with a mnemonic also
    /// records into that mnemonic's aggregate family. Returns whether a sample was recorded.
    pub fn observe(&mut self, key: SketchKey, observation: &Observation) -> bool {
        let mnemonic = match &key {
            SketchKey::Target {
                mnemonic: Some(mnemonic),
                ..
            } => Some(mnemonic.clone()),
            _ => None,
        };
        let recorded = self.observe_one(key, observation);
        if recorded && let Some(mnemonic) = mnemonic {
            self.observe_one(SketchKey::Mnemonic { mnemonic }, observation);
        }
        recorded
    }

    fn observe_one(&mut self, key: SketchKey, observation: &Observation) -> bool {
        let family = key.family();
        let window_s = self.window_s();
        let inner = self.shard_mut(&key);
        if let SketchKey::Command { digest } = &key
            && !inner.families[family.index()].contains(&key)
            && !observation.killed
        {
            if inner.probation.pop(digest).is_some() {
                inner.probation_bytes -= probation_charge(digest);
            } else {
                inner.remember_probation(digest.clone());
                return false;
            }
        }
        inner.samples[family.index()] += 1;
        let map = &mut inner.families[family.index()];
        if let Some(entry) = map.get_mut(&key) {
            entry.record(observation, window_s);
            return true;
        }
        let mut entry = Entry::default();
        entry.record(observation, window_s);
        inner.insert(key, entry)
    }

    /// Inserts a seeded entry when `key` has no existing entry.
    pub fn seed(&mut self, key: SketchKey, entry: Entry) -> bool {
        let family = key.family();
        let inner = self.shard_mut(&key);
        if inner.families[family.index()].contains(&key) {
            return false;
        }
        inner.insert(key, entry)
    }

    /// Looks up `key`, promoting it to most recently used if present and unexpired.
    ///
    /// Expired entries are dropped and expired kill floors are cleared on the returned copy.
    pub fn lookup(&mut self, key: &SketchKey, now_s: u64) -> Option<Entry> {
        let window_s = self.window_s();
        let cutoff = now_s.saturating_sub(window_s);
        let inner = self.shard_mut(key);
        let map = &mut inner.families[key.family().index()];
        let entry = map.get(key)?;
        if !entry.seeded && entry.last_seen_s < cutoff {
            map.pop(key);
            inner.bytes[key.family().index()] -= entry_charge(key);
            return None;
        }
        let mut entry = entry.clone();
        if now_s.saturating_sub(entry.killed_memory_s) > window_s {
            entry.killed_peak_memory_kb = 0;
            entry.killed_memory_s = 0;
        }
        if now_s.saturating_sub(entry.killed_disk_s) > window_s {
            entry.killed_peak_disk_kb = 0;
            entry.killed_disk_s = 0;
        }
        Some(entry)
    }

    /// Shapes a reservation from `entry` using this sketch's [`Policy`].
    #[must_use]
    pub fn shape(&self, entry: &Entry) -> Shaped {
        shape(&self.policy, entry)
    }

    /// Returns aggregate occupancy, capacity, sample, and eviction counters.
    #[must_use]
    pub fn stats(&self) -> Stats {
        let mut stats = Stats::default();
        for inner in &self.shards {
            stats.probation += inner.probation.len();
            for family in Family::ALL {
                let i = family.index();
                stats.entries[i] += inner.families[i].len();
                stats.capacity[i] += inner.families[i].cap().get();
                stats.evictions[i] += inner.evictions[i];
                stats.samples[i] += inner.samples[i];
            }
        }
        stats
    }

    /// Ages every entry to `now_s` and drops non-seeded entries older than the window.
    ///
    /// Returns the number of dropped keys.
    pub fn age_out(&mut self, now_s: u64) -> usize {
        let window_s = self.window_s();
        let cutoff = now_s.saturating_sub(window_s);
        let mut dropped = 0_usize;
        for inner in &mut self.shards {
            for family in Family::ALL {
                let map = &mut inner.families[family.index()];
                let mut stale = Vec::new();
                map.for_each_mut(|key, entry| {
                    if !entry.seeded && entry.last_seen_s < cutoff {
                        stale.push(key.clone());
                    } else {
                        entry.settle(now_s, window_s);
                    }
                });
                dropped += stale.len();
                let reclaimed = stale.iter().map(entry_charge).sum::<u64>();
                for key in stale {
                    map.pop(&key);
                }
                inner.bytes[family.index()] -= reclaimed;
            }
        }
        dropped
    }

    /// Captures all live entries, least recently used first within each shard and family.
    #[must_use]
    pub fn snapshot(&self, now_s: u64) -> Snapshot {
        let cutoff = now_s.saturating_sub(self.window_s());
        let mut entries = Vec::new();
        for inner in &self.shards {
            for family in Family::ALL {
                let map = &inner.families[family.index()];
                entries.extend(
                    map.iter_rev()
                        .filter(|(_, entry)| entry.seeded || entry.last_seen_s >= cutoff)
                        .map(|(key, entry)| (key.clone(), entry.clone())),
                );
            }
        }
        Snapshot {
            sampler_generation: SAMPLER_GENERATION,
            written_at_s: now_s,
            entries,
        }
    }

    /// Upper bound in bytes on an encoded [`Snapshot`] produced by this sketch.
    #[must_use]
    pub const fn snapshot_byte_limit(&self) -> u64 {
        25 + (self.budget.target + self.budget.mnemonic + self.budget.command) as u64 * ENTRY_BYTES
    }

    /// Encodes all live entries directly into the v3 `NLHS` binary format.
    #[must_use]
    pub fn encode_snapshot(&self, now_s: u64) -> Vec<u8> {
        let limit = self.snapshot_byte_limit();
        let mut bytes = Vec::with_capacity(usize::try_from(limit).unwrap_or(usize::MAX));
        let header = Snapshot {
            sampler_generation: SAMPLER_GENERATION,
            written_at_s: now_s,
            entries: Vec::new(),
        }
        .encode();
        bytes.extend_from_slice(&header);
        let cutoff = now_s.saturating_sub(self.window_s());
        let mut count = 0_u64;
        for inner in &self.shards {
            for map in &inner.families {
                for (key, entry) in map.iter_rev() {
                    if entry.seeded || entry.last_seen_s >= cutoff {
                        encode_entry(&mut bytes, key, entry);
                        count += 1;
                    }
                }
            }
        }
        bytes[17..25].copy_from_slice(&count.to_le_bytes());
        bytes
    }

    /// Conservative resident memory estimate in bytes, including unused hash slots.
    #[must_use]
    pub fn charged_bytes(&self) -> u64 {
        let mut bytes = FIXED_BYTES;
        for inner in &self.shards {
            bytes += inner.bytes.iter().sum::<u64>() + inner.probation_bytes;
            bytes += (inner.families.iter().map(|m| m.cap().get()).sum::<usize>()
                + inner.probation.cap().get()) as u64
                * HASH_SLOT_BYTES;
        }
        bytes
    }

    /// Restores non-expired entries from `snapshot` without overwriting existing entries.
    ///
    /// Returns the number of newly loaded entries.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSampler`] if `snapshot.sampler_generation` does not
    /// match [`SAMPLER_GENERATION`].
    pub fn load(&mut self, snapshot: Snapshot, now_s: u64) -> Result<usize, SketchError> {
        if snapshot.sampler_generation != SAMPLER_GENERATION {
            return Err(SketchError::IncompatibleSampler {
                found: snapshot.sampler_generation,
                expected: SAMPLER_GENERATION,
            });
        }
        let cutoff = now_s.saturating_sub(self.window_s());
        let mut loaded = 0_usize;
        for (key, entry) in snapshot.entries {
            if !entry.seeded && entry.last_seen_s < cutoff {
                continue;
            }
            let family = key.family();
            let inner = self.shard_mut(&key);
            if inner.families[family.index()].contains(&key) || !inner.insert(key, entry) {
                continue;
            }
            loaded += 1;
        }
        Ok(loaded)
    }

    /// Returns up to `limit` shaped hint rows ordered most recently seen first.
    #[must_use]
    pub fn hints(&self, family: Option<Family>, limit: usize) -> Vec<HintRow> {
        let mut rows = Vec::new();
        for inner in &self.shards {
            for f in Family::ALL {
                if family.is_some_and(|wanted| wanted != f) {
                    continue;
                }
                for (key, entry) in inner.families[f.index()].iter().take(limit) {
                    rows.push(HintRow {
                        key: key.clone(),
                        shaped: shape(&self.policy, entry),
                        samples: entry.samples,
                        last_seen_s: entry.last_seen_s,
                        seeded: entry.seeded,
                    });
                }
            }
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.last_seen_s));
        rows.truncate(limit);
        rows
    }
}

/// One admin summary row pairing a [`SketchKey`] with its currently shaped reservation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HintRow {
    /// Entry key.
    pub key: SketchKey,
    /// Current shaped reservation.
    pub shaped: Shaped,
    /// Recorded sample count.
    pub samples: u32,
    /// Last observation time in Unix seconds.
    pub last_seen_s: u64,
    /// Whether this row came from a static seed.
    pub seeded: bool,
}

impl Entry {
    /// Maximum peak memory in `KiB` across both half-windows.
    #[must_use]
    pub fn max_memory_kb(&self) -> u64 {
        self.current.max_memory_kb.max(self.previous.max_memory_kb)
    }

    /// Maximum lifetime-average CPU in millicores across both half-windows.
    #[must_use]
    pub fn max_cpu_millicores(&self) -> u64 {
        self.current
            .max_cpu_millicores
            .max(self.previous.max_cpu_millicores)
    }

    /// Maximum peak disk in `KiB` across both half-windows.
    #[must_use]
    pub fn max_disk_kb(&self) -> u64 {
        self.current.max_disk_kb.max(self.previous.max_disk_kb)
    }

    /// 95th percentile memory bucket upper bound across both half-windows.
    #[must_use]
    pub fn memory_p95_upper(&self) -> u64 {
        self.current.memory.p95_upper_with(&self.previous.memory)
    }

    /// 95th percentile CPU bucket upper bound across both half-windows.
    #[must_use]
    pub fn cpu_p95_upper(&self) -> u64 {
        self.current.cpu.p95_upper_with(&self.previous.cpu)
    }

    /// 95th percentile disk bucket upper bound across both half-windows.
    #[must_use]
    pub fn disk_p95_upper(&self) -> u64 {
        self.current.disk.p95_upper_with(&self.previous.disk)
    }

    /// Total memory samples retained across both half-windows.
    #[must_use]
    pub fn memory_samples(&self) -> u64 {
        self.current
            .memory
            .total()
            .saturating_add(self.previous.memory.total())
    }

    fn half_s(window_s: u64) -> u64 {
        (window_s / 2).max(1)
    }

    /// Rotates half-windows to `now_s` and expires stale kill floors. Seeded entries are left
    /// intact until their first real observation.
    pub fn settle(&mut self, now_s: u64, window_s: u64) {
        if self.seeded {
            return;
        }
        let period = now_s / Self::half_s(window_s);
        if period > self.period {
            if period == self.period + 1 {
                self.previous = std::mem::take(&mut self.current);
            } else {
                self.previous = Half::default();
                self.current = Half::default();
            }
            self.period = period;
        }
        if self.killed_peak_memory_kb > 0 && now_s.saturating_sub(self.killed_memory_s) > window_s {
            self.killed_peak_memory_kb = 0;
            self.killed_memory_s = 0;
        }
        if self.killed_peak_disk_kb > 0 && now_s.saturating_sub(self.killed_disk_s) > window_s {
            self.killed_peak_disk_kb = 0;
            self.killed_disk_s = 0;
        }
    }

    fn record(&mut self, observation: &Observation, window_s: u64) {
        if self.seeded {
            *self = Self::default();
        }
        if self.period == 0 {
            self.period = observation.now_s / Self::half_s(window_s);
        }
        self.settle(observation.now_s, window_s);
        self.samples = self.samples.saturating_add(1);
        self.last_seen_s = self.last_seen_s.max(observation.now_s);
        let half = &mut self.current;
        if observation.peak_memory_kb > 0 {
            half.memory.add(observation.peak_memory_kb);
            half.max_memory_kb = half.max_memory_kb.max(observation.peak_memory_kb);
        }
        if let Some(millicores) = observation.cpu_millicores {
            half.cpu.add(millicores);
            half.max_cpu_millicores = half.max_cpu_millicores.max(millicores);
        }
        if observation.peak_disk_kb > 0 {
            half.disk.add(observation.peak_disk_kb);
            half.max_disk_kb = half.max_disk_kb.max(observation.peak_disk_kb);
        }
        if let Some(peak) = observation.killed_peak_memory_kb
            && peak > 0
            && peak >= self.killed_peak_memory_kb
        {
            self.killed_peak_memory_kb = peak;
            self.killed_memory_s = observation.now_s;
        }
        if let Some(peak) = observation.killed_peak_disk_kb
            && peak > 0
            && peak >= self.killed_peak_disk_kb
        {
            self.killed_peak_disk_kb = peak;
            self.killed_disk_s = observation.now_s;
        }
        self.seeded = false;
    }

    /// Constructs a seeded entry standing in for a hints-file entry until its first real sample.
    #[must_use]
    pub fn seeded(cpu: u64, memory_kb: u64, disk_kb: u64, samples: u32, last_seen_s: u64) -> Self {
        let mut entry = Self {
            samples: samples.max(1),
            last_seen_s,
            seeded: true,
            ..Self::default()
        };
        if memory_kb > 0 {
            entry.current.memory.add(memory_kb);
            entry.current.max_memory_kb = memory_kb;
        }
        if cpu > 0 {
            entry.current.cpu.add(cpu);
            entry.current.max_cpu_millicores = cpu;
        }
        if disk_kb > 0 {
            entry.current.disk.add(disk_kb);
            entry.current.max_disk_kb = disk_kb;
        }
        entry
    }
}

const fn percentile_capped(upper: u64, max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    if upper == 0 || max < upper {
        max
    } else {
        upper
    }
}

fn with_headroom(value: u64, headroom_percent: u64) -> u64 {
    if value == 0 {
        return 0;
    }
    let product = u128::from(value).saturating_mul(100 + u128::from(headroom_percent));
    u64::try_from(product.div_ceil(100)).unwrap_or(u64::MAX)
}

fn smallest_dominating_class(
    classes: &[SizeClass],
    memory_kb: u64,
    cpu: u64,
    disk_kb: u64,
) -> Option<&SizeClass> {
    classes.last()?;
    for class in classes {
        if class.memory_kb < memory_kb {
            continue;
        }
        if class.cpu_count > 0 && cpu > 0 && class.cpu_count < cpu {
            continue;
        }
        if class.disk_kb > 0 && disk_kb > 0 && class.disk_kb < disk_kb {
            continue;
        }
        return Some(class);
    }
    None
}

fn shape(policy: &Policy, entry: &Entry) -> Shaped {
    let headroom = policy.headroom_percent;
    let mut memory_kb = with_headroom(
        percentile_capped(entry.memory_p95_upper(), entry.max_memory_kb()),
        headroom,
    );
    memory_kb = memory_kb.max(with_headroom(entry.killed_peak_memory_kb, headroom));
    let mut cpu = with_headroom(
        percentile_capped(
            entry
                .current
                .cpu
                .percentile_upper_with(&entry.previous.cpu, policy.cpu_reservation_percentile),
            entry.max_cpu_millicores(),
        ),
        headroom,
    );
    let mut disk_kb = with_headroom(
        percentile_capped(entry.disk_p95_upper(), entry.max_disk_kb()),
        headroom,
    );
    disk_kb = disk_kb.max(with_headroom(entry.killed_peak_disk_kb, headroom));
    if !policy.reserve_cpu {
        cpu = 0;
    }
    if !policy.reserve_memory {
        memory_kb = 0;
    }
    if !policy.reserve_disk {
        disk_kb = 0;
    }
    if memory_kb == 0 && cpu == 0 && disk_kb == 0 {
        return Shaped::default();
    }
    let mut class = None;
    let found = smallest_dominating_class(&policy.classes, memory_kb, cpu, disk_kb).or_else(|| {
        policy.classes.last().filter(|top| {
            (!policy.reserve_memory
                || top.memory_kb >= with_headroom(entry.killed_peak_memory_kb, headroom))
                && (!policy.reserve_disk
                    || top.disk_kb >= with_headroom(entry.killed_peak_disk_kb, headroom))
        })
    });
    if let Some(found) = found {
        class = Some(found.name.clone());
        if found.memory_kb > 0 {
            memory_kb = found.memory_kb;
        }
        if found.cpu_count > 0 {
            cpu = found.cpu_count;
        }
        if found.disk_kb > 0 {
            disk_kb = found.disk_kb;
        }
    }
    Shaped {
        cpu_count: if policy.reserve_cpu { cpu } else { 0 },
        memory_kb: if policy.reserve_memory { memory_kb } else { 0 },
        disk_kb: if policy.reserve_disk { disk_kb } else { 0 },
        class,
    }
}

/// Binary snapshot of resource sketch entries (`NLHS` v3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Sampler generation that produced this snapshot.
    pub sampler_generation: u32,
    /// Snapshot creation time in Unix seconds.
    pub written_at_s: u64,
    /// Entries ordered least recently used first within each shard so sequential replay preserves
    /// recency.
    pub entries: Vec<(SketchKey, Entry)>,
}

impl Snapshot {
    /// Header bytes needed to inspect generation and write time without decoding entries.
    pub const HEADER_BYTES: u64 = 17;

    /// Encodes the snapshot into its compact `NLHS` v3 binary form.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + self.entries.len() * 160);
        out.extend_from_slice(SNAPSHOT_MAGIC);
        out.push(SNAPSHOT_VERSION);
        out.extend_from_slice(&self.sampler_generation.to_le_bytes());
        out.extend_from_slice(&self.written_at_s.to_le_bytes());
        out.extend_from_slice(
            &u64::try_from(self.entries.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for (key, entry) in &self.entries {
            encode_entry(&mut out, key, entry);
        }
        out
    }

    fn read_header(r: &mut Reader<'_>) -> Result<(u32, u64), SketchError> {
        if r.take(4)? != SNAPSHOT_MAGIC {
            return Err(SketchError::InvalidMagic);
        }
        let version = r.u8()?;
        if version != SNAPSHOT_VERSION {
            return Err(SketchError::UnsupportedVersion { version });
        }
        Ok((r.u32()?, r.u64()?))
    }

    /// Reads the snapshot timestamp only when its format and sampler generation are compatible.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError`] if the header is invalid or the sampler generation mismatches.
    pub fn compatible_written_at(bytes: &[u8]) -> Result<u64, SketchError> {
        let (generation, written_at) = Self::read_header(&mut Reader { bytes, at: 0 })?;
        if generation != SAMPLER_GENERATION {
            return Err(SketchError::IncompatibleSampler {
                found: generation,
                expected: SAMPLER_GENERATION,
            });
        }
        Ok(written_at)
    }

    /// Decodes a snapshot while enforcing an upper bound of `max_entries`.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError`] if the header, entry count, UTF-8 strings, histograms, or framing
    /// are invalid.
    pub fn decode(bytes: &[u8], max_entries: usize) -> Result<Self, SketchError> {
        const MIN_ENTRY_BYTES: usize = 96;
        let mut r = Reader { bytes, at: 0 };
        let (sampler_generation, written_at_s) = Self::read_header(&mut r)?;
        let count = usize::try_from(r.u64()?).map_err(|_| SketchError::InvalidEntryCount)?;
        if count > max_entries || count > (bytes.len() - r.at) / MIN_ENTRY_BYTES {
            return Err(SketchError::InvalidEntryCount);
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = match r.u8()? {
                0 => {
                    let target = r.str()?;
                    let mnemonic = if r.u8()? == 1 { Some(r.str()?) } else { None };
                    SketchKey::Target { target, mnemonic }
                }
                1 => SketchKey::Mnemonic { mnemonic: r.str()? },
                2 => SketchKey::Command { digest: r.str()? },
                tag => return Err(SketchError::UnknownKeyTag { tag }),
            };
            let samples = r.u32()?;
            let current = r.half()?;
            let previous = r.half()?;
            let killed_peak_memory_kb = r.u64()?;
            let killed_memory_s = r.u64()?;
            let killed_peak_disk_kb = r.u64()?;
            let killed_disk_s = r.u64()?;
            let last_seen_s = r.u64()?;
            let period = r.u64()?;
            let seeded = r.u8()? == 1;
            entries.push((
                key,
                Entry {
                    samples,
                    current,
                    previous,
                    killed_peak_memory_kb,
                    killed_memory_s,
                    killed_peak_disk_kb,
                    killed_disk_s,
                    last_seen_s,
                    period,
                    seeded,
                },
            ));
        }
        if r.at != bytes.len() {
            return Err(SketchError::TrailingBytes {
                remaining: bytes.len() - r.at,
            });
        }
        Ok(Self {
            sampler_generation,
            written_at_s,
            entries,
        })
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    let len = u32::try_from(s.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&s.as_bytes()[..len as usize]);
}

fn put_hist(out: &mut Vec<u8>, hist: &Hist) {
    let nonzero = hist.0.iter().filter(|&&n| n > 0).count();
    out.push(u8::try_from(nonzero).unwrap_or(u8::MAX));
    for (i, &n) in hist.0.iter().enumerate() {
        if n > 0 {
            out.push(u8::try_from(i).unwrap_or(u8::MAX));
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], SketchError> {
        let Some(end) = self
            .at
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
        else {
            return Err(SketchError::Truncated { offset: self.at });
        };
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, SketchError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, SketchError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, SketchError> {
        let b = self.take(8)?;
        let mut arr = [0_u8; 8];
        arr.copy_from_slice(b);
        Ok(u64::from_le_bytes(arr))
    }

    fn str(&mut self) -> Result<Arc<str>, SketchError> {
        let len = self.u32()? as usize;
        let b = self.take(len)?;
        std::str::from_utf8(b)
            .map(Arc::from)
            .map_err(|_| SketchError::InvalidUtf8)
    }

    fn half(&mut self) -> Result<Half, SketchError> {
        Ok(Half {
            memory: self.hist()?,
            cpu: self.hist()?,
            disk: self.hist()?,
            max_memory_kb: self.u64()?,
            max_cpu_millicores: self.u64()?,
            max_disk_kb: self.u64()?,
        })
    }

    fn hist(&mut self) -> Result<Hist, SketchError> {
        let nonzero = usize::from(self.u8()?);
        if nonzero > HIST_BUCKETS {
            return Err(SketchError::InvalidHistogramCount { count: nonzero });
        }
        let mut hist = Hist::default();
        for _ in 0..nonzero {
            let i = usize::from(self.u8()?);
            let n = self.u64()?;
            if i >= HIST_BUCKETS {
                return Err(SketchError::InvalidHistogramBucket { bucket: i });
            }
            hist.0[i] = n;
        }
        Ok(hist)
    }
}

fn encode_entry(out: &mut Vec<u8>, key: &SketchKey, entry: &Entry) {
    match key {
        SketchKey::Target { target, mnemonic } => {
            out.push(0);
            put_str(out, target);
            match mnemonic {
                Some(mnemonic) => {
                    out.push(1);
                    put_str(out, mnemonic);
                }
                None => out.push(0),
            }
        }
        SketchKey::Mnemonic { mnemonic } => {
            out.push(1);
            put_str(out, mnemonic);
        }
        SketchKey::Command { digest } => {
            out.push(2);
            put_str(out, digest);
        }
    }
    out.extend_from_slice(&entry.samples.to_le_bytes());
    for half in [&entry.current, &entry.previous] {
        for hist in [&half.memory, &half.cpu, &half.disk] {
            put_hist(out, hist);
        }
        for value in [
            half.max_memory_kb,
            half.max_cpu_millicores,
            half.max_disk_kb,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    for value in [
        entry.killed_peak_memory_kb,
        entry.killed_memory_s,
        entry.killed_peak_disk_kb,
        entry.killed_disk_s,
        entry.last_seen_s,
        entry.period,
    ] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out.push(u8::from(entry.seeded));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(headroom_percent: u64, percentile: u8, classes: Vec<SizeClass>) -> Policy {
        Policy {
            headroom_percent,
            cpu_reservation_percentile: Percent::from_percent(percentile),
            classes,
            reserve_cpu: true,
            reserve_memory: true,
            reserve_disk: true,
        }
    }

    fn small_budget() -> Budget {
        Budget {
            target: 4,
            mnemonic: 4,
            command: 4,
            probation: 4,
        }
    }

    #[test]
    fn bucket_edges_and_percentile_upper_match_spec() {
        assert_eq!(Hist::bucket(0), 0);
        assert_eq!(Hist::bucket(999), 0);
        assert_eq!(Hist::bucket(1000), 1);
        assert_eq!(Hist::bucket(1999), 1);
        assert_eq!(Hist::bucket(2000), 2);
        assert_eq!(Hist::bucket(u64::MAX), HIST_BUCKETS - 1);

        let mut h = Hist::default();
        for _ in 0..18 {
            h.add(500);
        }
        for _ in 0..2 {
            h.add(12_000);
        }
        assert_eq!(
            h.percentile_upper_with(&Hist::default(), Percent::from_percent(50)),
            1000
        );
        assert_eq!(h.p95_upper(), 16_000);
    }

    #[test]
    fn command_probation_and_target_mnemonic_dual_recording() {
        let mut sketch = ResourceSketch::with_shards(
            policy(20, 50, vec![]),
            small_budget(),
            Duration::from_hours(24),
            1,
        );

        let cmd = SketchKey::for_attempt(None, None, "abc/123");
        let obs = Observation {
            peak_memory_kb: 22_387,
            cpu_millicores: Some(1500),
            peak_disk_kb: 1536,
            now_s: 100,
            ..Observation::default()
        };
        assert!(!sketch.observe(cmd.clone(), &obs));
        assert!(sketch.lookup(&cmd, 100).is_none());
        assert!(sketch.observe(cmd.clone(), &obs));
        let entry = sketch
            .lookup(&cmd, 100)
            .expect("recorded on second sighting");
        assert_eq!(entry.samples, 1);
        assert_eq!(sketch.shape(&entry).memory_kb, 26_865);

        let killed_cmd = SketchKey::for_attempt(None, None, "killed/1");
        assert!(sketch.observe(
            killed_cmd.clone(),
            &Observation {
                killed: true,
                killed_peak_memory_kb: Some(40_000),
                now_s: 100,
                ..Observation::default()
            }
        ));
        let killed_entry = sketch
            .lookup(&killed_cmd, 100)
            .expect("killed bypasses probation");
        assert_eq!(sketch.shape(&killed_entry).memory_kb, 48_000);

        let target = SketchKey::for_attempt(Some("//pkg:tgt"), Some("Rustc"), "");
        assert!(sketch.observe(target.clone(), &obs));
        assert!(sketch.lookup(&target, 100).is_some());
        let mnemonic = SketchKey::for_attempt(None, Some("Rustc"), "");
        assert!(sketch.lookup(&mnemonic, 100).is_some());
    }

    #[test]
    fn snapshot_encode_decode_preserves_bytes_and_lru_order() {
        let mut sketch = ResourceSketch::with_shards(
            policy(20, 50, vec![]),
            small_budget(),
            Duration::from_hours(24),
            1,
        );
        for (idx, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
            sketch.observe(
                SketchKey::for_attempt(Some(name), Some("cc"), ""),
                &Observation {
                    peak_memory_kb: (idx as u64 + 1) * 10_000,
                    cpu_millicores: Some(1500),
                    peak_disk_kb: 2048,
                    now_s: 100 + idx as u64,
                    ..Observation::default()
                },
            );
        }
        // Promote "a" so "b" becomes LRU.
        assert!(
            sketch
                .lookup(&SketchKey::for_attempt(Some("a"), Some("cc"), ""), 105)
                .is_some()
        );

        let bytes = sketch.encode_snapshot(105);
        assert_eq!(Snapshot::compatible_written_at(&bytes), Ok(105));
        let decoded = Snapshot::decode(&bytes, usize::MAX).expect("valid snapshot");
        assert_eq!(decoded.encode(), bytes);

        let mut restored = ResourceSketch::with_shards(
            policy(20, 50, vec![]),
            small_budget(),
            Duration::from_hours(24),
            1,
        );
        assert_eq!(restored.load(decoded, 105), Ok(5));

        // Adding one more target evicts "b" (the LRU), not "a".
        restored.observe(
            SketchKey::for_attempt(Some("e"), Some("cc"), ""),
            &Observation {
                peak_memory_kb: 5000,
                now_s: 106,
                ..Observation::default()
            },
        );
        assert!(
            restored
                .lookup(&SketchKey::for_attempt(Some("b"), Some("cc"), ""), 106)
                .is_none()
        );
        assert!(
            restored
                .lookup(&SketchKey::for_attempt(Some("a"), Some("cc"), ""), 106)
                .is_some()
        );
    }

    #[test]
    fn snapshot_decode_rejects_corruption_and_bounds() {
        let mut sketch = ResourceSketch::with_shards(
            policy(0, 95, vec![]),
            small_budget(),
            Duration::from_hours(24),
            1,
        );
        sketch.observe(
            SketchKey::for_attempt(Some("t"), None, ""),
            &Observation {
                peak_memory_kb: 1024,
                now_s: 10,
                ..Observation::default()
            },
        );
        let bytes = sketch.encode_snapshot(10);
        assert_eq!(
            Snapshot::decode(&bytes, 0),
            Err(SketchError::InvalidEntryCount)
        );
        assert!(matches!(
            Snapshot::decode(&bytes[..bytes.len() - 1], usize::MAX),
            Err(SketchError::Truncated { .. })
        ));
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            Snapshot::decode(&trailing, usize::MAX),
            Err(SketchError::TrailingBytes { remaining: 1 })
        );
        let mut bad_magic = bytes;
        bad_magic[0] = b'X';
        assert_eq!(
            Snapshot::decode(&bad_magic, usize::MAX),
            Err(SketchError::InvalidMagic)
        );
    }
}
