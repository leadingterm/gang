//! Periodic resource sampling and worker-wide pressure enforcement (`§5.8` & `§6.4`).
//!
//! Tracks active execution attempts, samples per-attempt `cgroup` v2 memory/`CPU`/`OOM` counters
//! and scratch-directory disk usage, polls worker-wide memory `PSI` and `statvfs` free space
//! (incorporating bounded graveyard `reclaimable_bytes` credit and the 2% emergency floor), and
//! drives [`MemoryPressureMonitor`], [`DiskPressureMonitor`], [`SoftCeilingMonitor`],
//! [`QuiescenceGate`], and [`pick_pressure_victim`] to terminate victims cleanly.

#[cfg(any(target_os = "linux", test))]
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gang_clock::{Clock, Tick};
use gang_config::DiskEnforcement;
use gang_config::DiskTuning;
use gang_config::MemoryEnforcement;
use gang_config::MemoryTuning;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_scheduler::DiskPressureMonitor;
use gang_scheduler::DiskSample;
use gang_scheduler::MemoryPressureMonitor;
use gang_scheduler::PressureCandidate;
use gang_scheduler::QuiescenceGate;
use gang_scheduler::SoftCeilingMonitor;
use gang_scheduler::SoftVerdict;
use gang_scheduler::parse_cgroup_limit;
use gang_scheduler::parse_psi_total_us;
use gang_scheduler::pick_pressure_victim;
use gang_scheduler::pool_available_bytes;
use gang_scheduler::reclaimable_bytes;
use gang_scheduler::soft_ceiling;
use gang_types::AttemptId;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// Filename written by `gang exec` inside `.side_channels/<attempt_id>/` carrying the absolute
/// action `cgroup` path.
pub(crate) const REPORT_CGROUP_FILE: &str = "report.cgroup";

/// `cgroup` v2 file reporting current memory usage in bytes.
const MEMORY_CURRENT_FILE: &str = "memory.current";
/// `cgroup` v2 file reporting peak memory usage in bytes.
const MEMORY_PEAK_FILE: &str = "memory.peak";
/// `cgroup` v2 file reporting memory `PSI` stall counters.
const MEMORY_PRESSURE_FILE: &str = "memory.pressure";
/// `cgroup` v2 file reporting hard memory limit (`"max"` or bytes).
const MEMORY_MAX_FILE: &str = "memory.max";
/// `cgroup` v2 file reporting soft/throttling memory limit (`"max"` or bytes).
const MEMORY_HIGH_FILE: &str = "memory.high";
/// `cgroup` v2 file reporting hierarchical memory events (`oom_kill`).
const MEMORY_EVENTS_FILE: &str = "memory.events";
/// `cgroup` v2 file reporting `CPU` time counters (`usage_usec`).
const CPU_STAT_FILE: &str = "cpu.stat";

/// Measured resource usage accumulated for one execution attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AttemptUsage {
    /// Cumulative `CPU` time consumed by the attempt, when measured.
    pub cpu_time: Option<Duration>,
    /// Peak memory observed in the attempt's `cgroup` or side-channel report, in bytes.
    pub peak_memory_bytes: Option<u64>,
    /// Peak action work-directory disk usage observed for the attempt, in bytes.
    pub peak_disk_bytes: Option<u64>,
    /// Hierarchical kernel `OOM` kill count observed in the attempt's `cgroup` or report.
    pub oom_kills: u64,
    /// Whether periodic resource sampling observed the running attempt.
    pub sampled: bool,
}

impl AttemptUsage {
    /// Records a memory observation in bytes, retaining the maximum peak.
    pub fn record_memory(&mut self, bytes: u64) {
        self.peak_memory_bytes = Some(self.peak_memory_bytes.map_or(bytes, |prev| prev.max(bytes)));
    }

    /// Records a disk usage observation in bytes, retaining the maximum peak.
    pub fn record_disk(&mut self, bytes: u64) {
        self.peak_disk_bytes = Some(self.peak_disk_bytes.map_or(bytes, |prev| prev.max(bytes)));
    }

    /// Records cumulative `CPU` time, retaining the maximum monotonic reading.
    pub fn record_cpu_time(&mut self, cpu_time: Duration) {
        self.cpu_time = Some(self.cpu_time.map_or(cpu_time, |prev| prev.max(cpu_time)));
    }

    /// Records hierarchical `OOM` kills, retaining the maximum monotonic count.
    pub fn record_oom_kills(&mut self, oom_kills: u64) {
        self.oom_kills = self.oom_kills.max(oom_kills);
    }

    /// Merges another [`AttemptUsage`] reading into `self`, keeping maxima and logical-OR `sampled`.
    pub fn merge(&mut self, other: Self) {
        if let Some(cpu) = other.cpu_time {
            self.record_cpu_time(cpu);
        }
        if let Some(mem) = other.peak_memory_bytes {
            self.record_memory(mem);
        }
        if let Some(disk) = other.peak_disk_bytes {
            self.record_disk(disk);
        }
        self.record_oom_kills(other.oom_kills);
        self.sampled |= other.sampled;
    }

    /// Converts measured usage into its `worker.v1` protobuf representation.
    #[must_use]
    pub fn to_proto(&self) -> proto::ResourceUsage {
        proto::ResourceUsage {
            cpu_time: self.cpu_time.map(duration_to_proto),
            peak_memory_bytes: self.peak_memory_bytes,
            peak_disk_bytes: self.peak_disk_bytes,
            oom_kills: self.oom_kills,
            sampled: self.sampled,
        }
    }
}

/// Converts a non-negative [`Duration`] to [`prost_types::Duration`], clamping seconds at `i64::MAX`.
#[must_use]
pub(crate) fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos = i32::try_from(duration.subsec_nanos()).unwrap_or(999_999_999);
    prost_types::Duration { seconds, nanos }
}

/// Shared state behind a [`KillTrigger`]/[`KillWaiter`] pair.
#[derive(Debug, Default)]
struct KillChannelInner {
    reason: Mutex<Option<proto::KillReason>>,
    notify: Notify,
}

/// Handle used to request termination of one running attempt.
///
/// Cloning shares the same first-writer-wins kill latch so both controller `Kill` frames and
/// worker pressure/soft-ceiling monitors can race cleanly without double-killing.
#[derive(Debug, Clone)]
pub struct KillTrigger {
    inner: Arc<KillChannelInner>,
}

impl KillTrigger {
    /// Signals that the attempt must be terminated for `reason`.
    ///
    /// Returns `true` if this call was the first to trigger termination, or `false` if a kill
    /// reason was already recorded.
    #[must_use]
    pub fn kill(&self, reason: proto::KillReason) -> bool {
        let won = {
            let mut slot = self.inner.reason.lock();
            if slot.is_some() {
                false
            } else {
                *slot = Some(reason);
                true
            }
        };
        if won {
            self.inner.notify.notify_waiters();
        }
        won
    }

    /// Returns whether termination has already been triggered.
    #[must_use]
    pub fn is_killed(&self) -> bool {
        self.inner.reason.lock().is_some()
    }

    /// Returns the recorded kill reason, if triggered.
    #[must_use]
    pub fn reason(&self) -> Option<proto::KillReason> {
        *self.inner.reason.lock()
    }
}

/// Receiver awaited by the attempt runner for external or pressure-triggered kills.
#[derive(Debug, Clone)]
pub struct KillWaiter {
    inner: Arc<KillChannelInner>,
}

impl KillWaiter {
    /// Waits until a [`KillTrigger::kill`] call fires and returns the winning [`proto::KillReason`].
    ///
    /// Cancellation-safe and never resolves spuriously if trigger clones are dropped without killing.
    pub async fn wait(&self) -> proto::KillReason {
        loop {
            let notified = self.inner.notify.notified();
            if let Some(reason) = *self.inner.reason.lock() {
                return reason;
            }
            notified.await;
        }
    }

    /// Returns the recorded kill reason immediately if already triggered.
    #[must_use]
    pub fn reason(&self) -> Option<proto::KillReason> {
        *self.inner.reason.lock()
    }
}

/// Creates a paired [`KillTrigger`] and [`KillWaiter`] for one execution attempt.
#[must_use]
pub fn kill_channel() -> (KillTrigger, KillWaiter) {
    let inner = Arc::new(KillChannelInner::default());
    (
        KillTrigger {
            inner: Arc::clone(&inner),
        },
        KillWaiter { inner },
    )
}

/// Tracks total reclaimable bytes currently queued or deleting in `.graveyard` (`§6.5`).
#[derive(Debug, Default)]
pub struct GraveyardPending {
    bytes: AtomicU64,
}

impl GraveyardPending {
    /// Creates an empty graveyard byte counter.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
        }
    }

    /// Records a newly enqueued graveyard entry using [`reclaimable_bytes`] and returns the
    /// reclaimable byte count to pass back to [`Self::complete`] when deletion finishes or fails.
    #[must_use]
    pub fn enqueue(&self, walk_bytes: u64, cas_shared_bytes: u64) -> u64 {
        let reclaimable = reclaimable_bytes(walk_bytes, cas_shared_bytes);
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(reclaimable))
            });
        reclaimable
    }

    /// Decrements pending reclaimable bytes once a graveyard entry finishes or fails.
    pub fn complete(&self, reclaimable: u64) {
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(reclaimable))
            });
    }

    /// Returns the current sum of reclaimable bytes awaiting deletion in `.graveyard`.
    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// One point-in-time `cgroup` v2 reading for a single running attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AttemptCgroupSample {
    /// Current memory usage from `memory.current`, in bytes.
    pub current_memory_bytes: Option<u64>,
    /// Peak memory usage from `memory.peak`, in bytes.
    pub peak_memory_bytes: Option<u64>,
    /// Cumulative `CPU` time from `cpu.stat` (`usage_usec`).
    pub cpu_time: Option<Duration>,
    /// Hierarchical `OOM` kills from `memory.events` (`oom_kill`).
    pub oom_kills: u64,
}

impl AttemptCgroupSample {
    /// Returns `true` when at least one `cgroup` counter was read.
    #[must_use]
    pub const fn observed(&self) -> bool {
        self.current_memory_bytes.is_some()
            || self.peak_memory_bytes.is_some()
            || self.cpu_time.is_some()
            || self.oom_kills > 0
    }
}

/// One point-in-time reading of the worker-wide action memory pool `cgroup`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryPoolSample {
    /// Cumulative `PSI` stall time in microseconds from `memory.pressure`.
    pub psi_total_us: Option<u64>,
    /// Available bytes remaining in the pool (`min(memory.max, memory.high) - memory.current`).
    pub available_bytes: Option<u64>,
}

/// Abstraction over OS/filesystem probes for sampling and pressure monitors.
///
/// Production workers use [`SystemProbe`]; tests inject deterministic synthetic probes without
/// touching `/sys/fs/cgroup` or host disk volumes.
pub trait SampleProbe: Send + Sync {
    /// Reads the validated absolute action `cgroup` path from `<side_channel_dir>/report.cgroup`.
    fn read_attempt_cgroup_path(&self, side_channel_dir: &Path) -> Option<PathBuf>;

    /// Reads per-attempt `cgroup` counters (`memory.current`, `memory.peak`, `cpu.stat`,
    /// `memory.events`).
    fn read_attempt_cgroup(&self, cgroup: &Path) -> AttemptCgroupSample;

    /// Measures allocated bytes currently held in `<work>/<attempt_id>`.
    fn read_attempt_disk(&self, attempt_dir: &Path) -> Option<u64>;

    /// Reads resident memory bytes across the attempt's process group when no `cgroup` is reported.
    fn read_attempt_process_group_memory(&self, _pgid: u32) -> Option<u64> {
        None
    }

    /// Measures an action tree rooted at the trusted sandbox PID, when available.
    fn read_attempt_tree_memory(&self, wrapper: u32, _sandbox: Option<u32>) -> Option<u64> {
        self.read_attempt_process_group_memory(wrapper)
    }

    /// Reads worker-wide memory `PSI` and pool availability from the action memory pool `cgroup`.
    fn read_memory_pool(&self, pool_cgroup: &Path) -> MemoryPoolSample;

    /// Queries `statvfs` free/total space on `work_dir` alongside current graveyard pending bytes.
    fn read_disk_space(&self, work_dir: &Path) -> Option<DiskSample>;
}

/// Production [`SampleProbe`] reading `cgroup` v2 files, `gang_os::fs::usage`, `gang_os::fs::space`,
/// and [`GraveyardPending`].
#[derive(Debug, Clone)]
pub struct SystemProbe {
    graveyard: Arc<GraveyardPending>,
}

impl SystemProbe {
    /// Creates a [`SystemProbe`] incorporating `graveyard` pending bytes into disk samples.
    #[must_use]
    pub const fn new(graveyard: Arc<GraveyardPending>) -> Self {
        Self { graveyard }
    }

    /// Returns the shared [`GraveyardPending`] counter.
    #[must_use]
    pub fn graveyard(&self) -> &Arc<GraveyardPending> {
        &self.graveyard
    }
}

impl SampleProbe for SystemProbe {
    fn read_attempt_cgroup_path(&self, side_channel_dir: &Path) -> Option<PathBuf> {
        read_cgroup_side_channel(side_channel_dir)
    }

    fn read_attempt_cgroup(&self, cgroup: &Path) -> AttemptCgroupSample {
        read_cgroup_sample(cgroup)
    }

    fn read_attempt_disk(&self, attempt_dir: &Path) -> Option<u64> {
        gang_os::fs::usage(attempt_dir)
            .ok()
            .map(gang_types::Bytes::get)
    }

    fn read_attempt_process_group_memory(&self, pgid: u32) -> Option<u64> {
        read_process_tree_bytes(pgid, None)
    }

    fn read_attempt_tree_memory(&self, wrapper: u32, sandbox: Option<u32>) -> Option<u64> {
        read_process_tree_bytes(wrapper, sandbox)
    }

    fn read_memory_pool(&self, pool_cgroup: &Path) -> MemoryPoolSample {
        read_memory_pool_sample(pool_cgroup)
    }

    fn read_disk_space(&self, work_dir: &Path) -> Option<DiskSample> {
        let space = gang_os::fs::space(work_dir).ok()?;
        Some(DiskSample {
            free_bytes: space.available.get(),
            total_bytes: space.total.get(),
            graveyard_pending_bytes: self.graveyard.pending_bytes(),
        })
    }
}

/// Parses a single decimal `u64` from a `cgroup` interface file (`memory.current`, `memory.peak`).
#[must_use]
pub fn parse_cgroup_u64(text: &str) -> Option<u64> {
    text.trim().parse::<u64>().ok()
}

/// Parses cumulative `CPU` time from the `usage_usec` line of a `cgroup` v2 `cpu.stat` file.
#[must_use]
pub fn parse_cpu_stat_usage(text: &str) -> Option<Duration> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some("usage_usec"), Some(value)) => {
                value.parse::<u64>().ok().map(Duration::from_micros)
            }
            _ => None,
        }
    })
}

/// Parses hierarchical `oom_kill` count from a `cgroup` v2 `memory.events` file.
#[must_use]
pub fn parse_memory_events_oom_kills(text: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some("oom_kill"), Some(value)) => value.parse::<u64>().ok(),
            _ => None,
        }
    })
}

/// Reads and validates `<side_channel_dir>/report.cgroup` written by `gang exec`.
///
/// Rejects empty, non-absolute, `NUL`-containing, or `..`-traversing paths.
#[must_use]
pub fn read_cgroup_side_channel(side_channel_dir: &Path) -> Option<PathBuf> {
    let bytes = gang_os::fs::read_optional(&side_channel_dir.join(REPORT_CGROUP_FILE))
        .ok()
        .flatten()?;
    if bytes.is_empty() || bytes.contains(&0) {
        return None;
    }
    let path = PathBuf::from(OsStr::from_bytes(&bytes));
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return None;
    }
    Some(path)
}

/// Reads per-attempt `cgroup` v2 counters from `cgroup`.
#[must_use]
pub fn read_cgroup_sample(cgroup: &Path) -> AttemptCgroupSample {
    let current_memory_bytes =
        gang_os::fs::read_to_string_optional(&cgroup.join(MEMORY_CURRENT_FILE))
            .ok()
            .flatten()
            .and_then(|text| parse_cgroup_u64(&text));
    let peak_memory_bytes = gang_os::fs::read_to_string_optional(&cgroup.join(MEMORY_PEAK_FILE))
        .ok()
        .flatten()
        .and_then(|text| parse_cgroup_u64(&text));
    let cpu_time = gang_os::fs::read_to_string_optional(&cgroup.join(CPU_STAT_FILE))
        .ok()
        .flatten()
        .and_then(|text| parse_cpu_stat_usage(&text));
    let oom_kills = gang_os::fs::read_to_string_optional(&cgroup.join(MEMORY_EVENTS_FILE))
        .ok()
        .flatten()
        .and_then(|text| parse_memory_events_oom_kills(&text))
        .unwrap_or(0);
    AttemptCgroupSample {
        current_memory_bytes,
        peak_memory_bytes,
        cpu_time,
        oom_kills,
    }
}

/// Parses the parent, group and session IDs from a Linux process stat line.
#[cfg(any(target_os = "linux", test))]
fn parse_proc_stat(stat: &str) -> Option<(u32, u32, u32)> {
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    fields.next()?;
    Some((
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
    ))
}

#[cfg(any(target_os = "linux", test))]
fn parse_memory_kib(text: &str, field: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(field)?;
        let kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kib.saturating_mul(1024))
    })
}

#[cfg(any(target_os = "linux", test))]
fn collect_process_tree_pids(
    processes: impl IntoIterator<Item = (u32, u32, u32, u32)>,
    root: u32,
    include_session: bool,
) -> HashSet<u32> {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut selected = HashSet::new();
    for (pid, parent, group, session) in processes {
        children.entry(parent).or_default().push(pid);
        if pid == root || group == root || (include_session && session == root) {
            selected.insert(pid);
        }
    }
    let mut queue: VecDeque<u32> = selected.iter().copied().collect();
    while let Some(pid) = queue.pop_front() {
        if let Some(descendants) = children.get(&pid) {
            for child in descendants {
                if selected.insert(*child) {
                    queue.push_back(*child);
                }
            }
        }
    }
    selected
}

#[cfg(target_os = "linux")]
fn read_process_tree_bytes(wrapper: u32, sandbox: Option<u32>) -> Option<u64> {
    read_process_tree_pss_from(Path::new("/proc"), wrapper, sandbox)
}

#[cfg(target_os = "linux")]
fn read_process_tree_pss_from(proc_root: &Path, wrapper: u32, sandbox: Option<u32>) -> Option<u64> {
    let mut processes = Vec::new();
    for entry in std::fs::read_dir(proc_root).ok()?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some((parent, group, session)) = parse_proc_stat(&stat) else {
            continue;
        };
        processes.push((pid, parent, group, session));
    }
    let mut selected = collect_process_tree_pids(
        processes.iter().copied(),
        sandbox.unwrap_or(wrapper),
        sandbox.is_some(),
    );
    if selected.is_empty() && sandbox.is_some() {
        selected = collect_process_tree_pids(processes, wrapper, false);
    }
    let mut total = 0_u64;
    let mut matched = false;
    for pid in selected {
        let path = proc_root.join(pid.to_string()).join("smaps_rollup");
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        let bytes = parse_memory_kib(&text, "Pss:")?;
        total = total.saturating_add(bytes);
        matched = true;
    }
    matched.then_some(total)
}

#[cfg(not(target_os = "linux"))]
const fn read_process_tree_bytes(_wrapper: u32, _sandbox: Option<u32>) -> Option<u64> {
    None
}

/// Reads worker-wide action memory pool `PSI` and available bytes from `pool_cgroup`.
#[must_use]
pub fn read_memory_pool_sample(pool_cgroup: &Path) -> MemoryPoolSample {
    let psi_total_us =
        gang_os::fs::read_to_string_optional(&pool_cgroup.join(MEMORY_PRESSURE_FILE))
            .ok()
            .flatten()
            .and_then(|text| parse_psi_total_us(&text));
    let current = gang_os::fs::read_to_string_optional(&pool_cgroup.join(MEMORY_CURRENT_FILE))
        .ok()
        .flatten()
        .and_then(|text| parse_cgroup_u64(&text));
    let memory_max = gang_os::fs::read_to_string_optional(&pool_cgroup.join(MEMORY_MAX_FILE))
        .ok()
        .flatten()
        .and_then(|text| parse_cgroup_limit(&text));
    let memory_high =
        match gang_os::fs::read_to_string_optional(&pool_cgroup.join(MEMORY_HIGH_FILE)) {
            Ok(Some(text)) => parse_cgroup_limit(&text),
            Ok(None) => Some(None),
            Err(_) => None,
        };
    let available_bytes = match (current, memory_max, memory_high) {
        (Some(cur), Some(max), Some(high)) => pool_available_bytes(cur, max, high),
        _ => None,
    };
    MemoryPoolSample {
        psi_total_us,
        available_bytes,
    }
}

/// Configuration for [`WorkerSampler`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamplingConfig {
    /// Memory sampling, pressure, and soft-ceiling policy.
    pub memory: MemoryTuning,
    /// Disk sampling, pressure, and soft-ceiling policy.
    pub disk: DiskTuning,
    /// Absolute path to the worker's action memory pool `cgroup`, when limits are active.
    pub memory_pool: Option<PathBuf>,
    /// Worker scratch directory monitored by `statvfs`.
    pub work_directory: PathBuf,
    /// Instance scratch disk capacity used to cap graveyard credit at 10%.
    pub disk_per_instance_bytes: u64,
}

/// Registration payload for one running attempt tracked by [`SamplingRegistry`].
#[derive(Debug, Clone)]
pub struct AttemptRegistration {
    /// Attempt identifier.
    pub attempt: AttemptId,
    /// Path to `<work>/<attempt_id>`.
    pub attempt_dir: PathBuf,
    /// Path to `<work>/.side_channels/<attempt_id>`.
    pub side_channel_dir: PathBuf,
    /// Memory reservation in bytes (`0` when unreserved).
    pub reserved_memory_bytes: u64,
    /// Disk reservation in bytes (`0` when unreserved).
    pub reserved_disk_bytes: u64,
    /// Kill handle used by pressure and soft-ceiling monitors to terminate this attempt.
    pub kill: KillTrigger,
}

/// Per-attempt state maintained inside [`SamplingRegistry`].
#[derive(Debug)]
struct TrackedAttempt {
    attempt: AttemptId,
    attempt_dir: PathBuf,
    side_channel_dir: PathBuf,
    cgroup: Option<PathBuf>,
    pgid: Option<u32>,
    accounting_pid: Option<u32>,
    reserved_memory_bytes: u64,
    reserved_disk_bytes: u64,
    last_memory_bytes: u64,
    last_disk_bytes: u64,
    usage: AttemptUsage,
    memory_soft: Option<SoftCeilingMonitor>,
    disk_soft: Option<SoftCeilingMonitor>,
    killing: bool,
    kill: KillTrigger,
}

/// Shared mutable state for active attempts and the post-kill quiescence gate.
#[derive(Debug)]
struct RegistryState {
    memory_policy: MemoryTuning,
    disk_policy: DiskTuning,
    attempts: Vec<TrackedAttempt>,
    quiescence: QuiescenceGate<AttemptId>,
}

/// Thread-safe handle connecting [`WorkerSampler`] with running attempt executions.
#[derive(Debug, Clone)]
pub struct SamplingRegistry {
    state: Arc<Mutex<RegistryState>>,
}

impl SamplingRegistry {
    /// Creates a new registry configured with `memory` and `disk` soft-ceiling policies.
    #[must_use]
    pub fn new(memory: MemoryTuning, disk: DiskTuning) -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState {
                memory_policy: memory,
                disk_policy: disk,
                attempts: Vec::new(),
                quiescence: QuiescenceGate::new(),
            })),
        }
    }

    /// Registers a running attempt and returns an RAII guard that unregisters it on drop if not
    /// explicitly finished via [`TrackedAttemptGuard::finish`].
    #[must_use]
    pub fn register(&self, registration: AttemptRegistration) -> TrackedAttemptGuard {
        let attempt = registration.attempt.clone();
        let mut state = self.state.lock();
        let memory_soft = (state.memory_policy.enforcement == MemoryEnforcement::Soft)
            .then(|| {
                soft_ceiling(
                    registration.reserved_memory_bytes,
                    state.memory_policy.headroom_percent,
                )
                .map(|limit| {
                    SoftCeilingMonitor::new(
                        limit,
                        state.memory_policy.soft_samples,
                        Some(state.memory_policy.soft_spike_percent.get()),
                    )
                })
            })
            .flatten();
        let disk_soft = (state.disk_policy.enforcement == DiskEnforcement::Soft)
            .then(|| {
                soft_ceiling(
                    registration.reserved_disk_bytes,
                    state.disk_policy.headroom_percent,
                )
                .map(|limit| SoftCeilingMonitor::new(limit, state.disk_policy.soft_samples, None))
            })
            .flatten();
        state.attempts.retain(|entry| entry.attempt != attempt);
        state.attempts.push(TrackedAttempt {
            attempt: registration.attempt,
            attempt_dir: registration.attempt_dir,
            side_channel_dir: registration.side_channel_dir,
            cgroup: None,
            pgid: None,
            accounting_pid: None,
            reserved_memory_bytes: registration.reserved_memory_bytes,
            reserved_disk_bytes: registration.reserved_disk_bytes,
            last_memory_bytes: 0,
            last_disk_bytes: 0,
            usage: AttemptUsage::default(),
            memory_soft,
            disk_soft,
            killing: false,
            kill: registration.kill,
        });
        drop(state);
        TrackedAttemptGuard {
            registry: self.clone(),
            attempt: Some(attempt),
        }
    }

    /// Records the process group ID for `attempt` so soft memory sampling can fall back to
    /// process-group RSS when no `cgroup` is attached.
    pub fn set_pgid(&self, attempt: &AttemptId, pgid: u32) {
        let mut state = self.state.lock();
        if let Some(entry) = state
            .attempts
            .iter_mut()
            .find(|entry| &entry.attempt == attempt)
        {
            entry.pgid = Some(pgid);
        }
    }

    /// Marks `attempt` as already terminating so pressure monitors will not select it again.
    pub fn mark_killing(&self, attempt: &AttemptId) {
        let mut state = self.state.lock();
        if let Some(entry) = state
            .attempts
            .iter_mut()
            .find(|entry| &entry.attempt == attempt)
        {
            entry.killing = true;
        }
    }

    /// Removes `attempt` from active tracking, releases the victim side of the quiescence gate,
    /// and returns its accumulated [`AttemptUsage`].
    #[must_use]
    pub fn finish(&self, attempt: &AttemptId) -> AttemptUsage {
        let mut state = self.state.lock();
        let usage = state
            .attempts
            .iter()
            .position(|entry| &entry.attempt == attempt)
            .map_or_else(AttemptUsage::default, |index| {
                state.attempts.remove(index).usage
            });
        state.quiescence.on_victim_removed(attempt);
        usage
    }

    /// Notifies the quiescence gate that `attempt`'s `.graveyard` entry has finished (or failed).
    pub fn on_graveyard_processed(&self, attempt: &AttemptId) {
        self.state.lock().quiescence.on_graveyard_processed(attempt);
    }

    /// Returns whether no prior pressure kill is waiting for attempt or graveyard cleanup.
    #[must_use]
    pub fn is_quiescence_settled(&self) -> bool {
        self.state.lock().quiescence.is_settled()
    }

    /// Returns the last discovered `cgroup` path for `attempt`, if any.
    #[must_use]
    pub fn cgroup_path(&self, attempt: &AttemptId) -> Option<PathBuf> {
        self.state
            .lock()
            .attempts
            .iter()
            .find(|entry| &entry.attempt == attempt)
            .and_then(|entry| entry.cgroup.clone())
    }

    /// Returns a snapshot of accumulated [`AttemptUsage`] for `attempt` while it is still active.
    #[must_use]
    pub fn current_usage(&self, attempt: &AttemptId) -> Option<AttemptUsage> {
        self.state
            .lock()
            .attempts
            .iter()
            .find(|entry| &entry.attempt == attempt)
            .map(|entry| entry.usage)
    }

    /// Returns the number of currently registered active attempts.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.state.lock().attempts.len()
    }
}

/// RAII guard unregistering an active attempt from [`SamplingRegistry`] on drop.
#[derive(Debug)]
pub struct TrackedAttemptGuard {
    registry: SamplingRegistry,
    attempt: Option<AttemptId>,
}

impl TrackedAttemptGuard {
    /// Unregisters the attempt and returns its accumulated [`AttemptUsage`].
    #[must_use]
    pub fn finish(mut self) -> AttemptUsage {
        self.attempt
            .take()
            .map_or_else(AttemptUsage::default, |attempt| {
                self.registry.finish(&attempt)
            })
    }
}

impl Drop for TrackedAttemptGuard {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            let _ = self.registry.finish(&attempt);
        }
    }
}

/// Snapshot of one active attempt's paths collected under lock before performing filesystem I/O.
struct SampleTarget {
    attempt: AttemptId,
    attempt_dir: PathBuf,
    side_channel_dir: PathBuf,
    cgroup: Option<PathBuf>,
    pgid: Option<u32>,
    accounting_pid: Option<u32>,
}

/// Periodic worker resource sampler and pressure-kill driver (`§5.8`, `§6.4`).
#[derive(Debug)]
pub struct WorkerSampler<P> {
    config: SamplingConfig,
    registry: SamplingRegistry,
    probe: P,
    memory_pressure: MemoryPressureMonitor,
    disk_pressure: DiskPressureMonitor,
}

impl<P: SampleProbe> WorkerSampler<P> {
    /// Creates a new [`WorkerSampler`] with its own [`SamplingRegistry`].
    #[must_use]
    pub fn new(config: SamplingConfig, probe: P) -> Self {
        let registry = SamplingRegistry::new(config.memory, config.disk);
        Self::with_registry(config, registry, probe)
    }

    /// Creates a [`WorkerSampler`] attached to an existing [`SamplingRegistry`].
    #[must_use]
    pub fn with_registry(config: SamplingConfig, registry: SamplingRegistry, probe: P) -> Self {
        let memory_pressure = MemoryPressureMonitor::new(
            config.memory.pressure_stall_percent,
            config.memory.reserve.get().get(),
            config.memory.pressure_kill_after.get(),
        );
        let disk_pressure = DiskPressureMonitor::new(
            config.disk.pressure_free_percent,
            config.disk_per_instance_bytes,
            config.disk.pressure_kill_after.get(),
        );
        Self {
            config,
            registry,
            probe,
            memory_pressure,
            disk_pressure,
        }
    }

    /// Returns a cloneable [`SamplingRegistry`] handle for registering active attempts.
    #[must_use]
    pub fn registry(&self) -> SamplingRegistry {
        self.registry.clone()
    }

    /// Returns a reference to the underlying probe.
    #[must_use]
    pub const fn probe(&self) -> &P {
        &self.probe
    }

    /// Samples every active attempt's `cgroup` memory, `CPU`, and `OOM` counters, and enforces
    /// per-attempt soft memory ceilings when configured.
    ///
    /// Returns the list of attempts terminated by soft memory ceiling breaches during this pass.
    pub fn sample_memory_attempts(&self) -> Vec<AttemptId> {
        let targets = self.collect_targets();
        let mut killed = Vec::new();
        for target in targets {
            let cgroup = target.cgroup.or_else(|| {
                self.probe
                    .read_attempt_cgroup_path(&target.side_channel_dir)
            });
            if let Some(cgroup) = cgroup {
                let sample = self.probe.read_attempt_cgroup(&cgroup);
                if let Some(victim) =
                    self.apply_memory_sample(&target.attempt, Some(cgroup), sample)
                {
                    killed.push(victim);
                }
            } else if let Some(pgid) = target.pgid {
                let accounting_pid = target.accounting_pid.or_else(|| {
                    let bytes =
                        gang_os::fs::read_optional(&target.side_channel_dir.join("report.pid"))
                            .ok()??;
                    std::str::from_utf8(&bytes)
                        .ok()?
                        .parse::<u32>()
                        .ok()
                        .filter(|pid| *pid > 0)
                });
                if let Some(pid) = accounting_pid {
                    let mut state = self.registry.state.lock();
                    if let Some(entry) = state
                        .attempts
                        .iter_mut()
                        .find(|entry| entry.attempt == target.attempt)
                    {
                        entry.accounting_pid = Some(pid);
                    }
                }
                let Some(bytes) = self.probe.read_attempt_tree_memory(pgid, accounting_pid) else {
                    continue;
                };
                let sample = AttemptCgroupSample {
                    current_memory_bytes: Some(bytes),
                    ..AttemptCgroupSample::default()
                };
                if let Some(victim) = self.apply_memory_sample(&target.attempt, None, sample) {
                    killed.push(victim);
                }
            }
        }
        killed
    }

    /// Samples every active attempt's scratch directory disk usage and enforces per-attempt soft
    /// disk ceilings (`KillReason::DiskLimit`) when configured.
    ///
    /// Returns the list of attempts terminated by soft disk ceiling breaches during this pass.
    pub fn sample_disk_attempts(&self) -> Vec<AttemptId> {
        let targets = self.collect_targets();
        let mut killed = Vec::new();
        for target in targets {
            let Some(bytes) = self.probe.read_attempt_disk(&target.attempt_dir) else {
                continue;
            };
            if let Some(victim) = self.apply_disk_sample(&target.attempt, bytes) {
                killed.push(victim);
            }
        }
        killed
    }

    /// Polls the worker's action memory pool `PSI` and available bytes at monotonic `now`.
    ///
    /// When sustained shortage triggers a kill, refreshes active attempts' memory usage, selects
    /// the victim via [`pick_pressure_victim`], triggers [`proto::KillReason::Memory`], and opens
    /// the post-kill [`QuiescenceGate`].
    pub fn poll_memory_pressure(&mut self, now: Tick) -> Option<AttemptId> {
        if self.config.memory.enforcement != MemoryEnforcement::Pressure {
            return None;
        }
        let pool = self.config.memory_pool.as_deref()?;
        let sample = self.probe.read_memory_pool(pool);
        let settled = self.registry.is_quiescence_settled();
        if !self
            .memory_pressure
            .observe(sample.psi_total_us, sample.available_bytes, settled, now)
        {
            return None;
        }
        let _ = self.sample_memory_attempts();
        self.kill_pressure_victim(PressureResource::Memory)
    }

    /// Polls `statvfs` free space (with graveyard credit) at monotonic `now`.
    ///
    /// When sustained disk pressure triggers a kill, refreshes active attempts' disk usage, selects
    /// the victim via [`pick_pressure_victim`], triggers [`proto::KillReason::DiskPressure`], and
    /// opens the post-kill [`QuiescenceGate`] awaiting graveyard cleanup.
    pub fn poll_disk_pressure(&mut self, now: Tick) -> Option<AttemptId> {
        if self.config.disk.enforcement != DiskEnforcement::Pressure {
            return None;
        }
        let sample = self.probe.read_disk_space(&self.config.work_directory);
        let settled = self.registry.is_quiescence_settled();
        if !self.disk_pressure.observe(sample, settled, now) {
            return None;
        }
        let _ = self.sample_disk_attempts();
        self.kill_pressure_victim(PressureResource::Disk)
    }

    /// Runs the periodic sampling and pressure-polling loop until `stop` completes.
    pub async fn run<K: Clock>(mut self, clock: &K, stop: impl Future<Output = ()> + Send) {
        let mut stop = pin!(stop);
        let start = clock.monotonic();
        let mem_sample_every = self.config.memory.sample_interval.get();
        let disk_sample_every = self.config.disk.sample_interval.get();
        let mem_pressure_every = (self.config.memory.enforcement == MemoryEnforcement::Pressure
            && self.config.memory_pool.is_some())
        .then(|| self.config.memory.pressure_poll_interval.get());
        let disk_pressure_every = (self.config.disk.enforcement == DiskEnforcement::Pressure)
            .then(|| self.config.disk.pressure_poll_interval.get());

        let mut next_mem_sample = start + mem_sample_every;
        let mut next_disk_sample = start + disk_sample_every;
        let mut next_mem_pressure = mem_pressure_every.map(|interval| start + interval);
        let mut next_disk_pressure = disk_pressure_every.map(|interval| start + interval);

        loop {
            let mut next_due = next_mem_sample.min(next_disk_sample);
            if let Some(tick) = next_mem_pressure {
                next_due = next_due.min(tick);
            }
            if let Some(tick) = next_disk_pressure {
                next_due = next_due.min(tick);
            }
            tokio::select! {
                biased;
                () = &mut stop => return,
                () = clock.sleep_until(next_due) => {}
            }
            let now = clock.monotonic();
            if now >= next_mem_sample {
                let _ = self.sample_memory_attempts();
                next_mem_sample = advance_schedule(next_mem_sample, now, mem_sample_every);
            }
            if now >= next_disk_sample {
                let _ = self.sample_disk_attempts();
                next_disk_sample = advance_schedule(next_disk_sample, now, disk_sample_every);
            }
            if let (Some(due), Some(interval)) = (next_mem_pressure, mem_pressure_every)
                && now >= due
            {
                let _ = self.poll_memory_pressure(now);
                next_mem_pressure = Some(advance_schedule(due, now, interval));
            }
            if let (Some(due), Some(interval)) = (next_disk_pressure, disk_pressure_every)
                && now >= due
            {
                let _ = self.poll_disk_pressure(now);
                next_disk_pressure = Some(advance_schedule(due, now, interval));
            }
        }
    }

    fn collect_targets(&self) -> Vec<SampleTarget> {
        let state = self.registry.state.lock();
        state
            .attempts
            .iter()
            .map(|entry| SampleTarget {
                attempt: entry.attempt.clone(),
                attempt_dir: entry.attempt_dir.clone(),
                side_channel_dir: entry.side_channel_dir.clone(),
                cgroup: entry.cgroup.clone(),
                pgid: entry.pgid,
                accounting_pid: entry.accounting_pid,
            })
            .collect()
    }

    fn apply_memory_sample(
        &self,
        attempt: &AttemptId,
        cgroup: Option<PathBuf>,
        sample: AttemptCgroupSample,
    ) -> Option<AttemptId> {
        let trigger = {
            let mut state = self.registry.state.lock();
            let entry = state
                .attempts
                .iter_mut()
                .find(|entry| &entry.attempt == attempt)?;
            if let Some(cgroup) = cgroup {
                entry.cgroup = Some(cgroup);
            }
            if !sample.observed() {
                return None;
            }
            entry.usage.sampled = true;
            if let Some(current) = sample.current_memory_bytes {
                entry.usage.record_memory(current);
            }
            if let Some(peak) = sample.peak_memory_bytes {
                entry.usage.record_memory(peak);
            }
            if let Some(cpu_time) = sample.cpu_time {
                entry.usage.record_cpu_time(cpu_time);
            }
            entry.usage.record_oom_kills(sample.oom_kills);
            let observed = sample
                .current_memory_bytes
                .or(sample.peak_memory_bytes)
                .unwrap_or(0);
            entry.last_memory_bytes = observed;
            if !entry.killing
                && !entry.kill.is_killed()
                && let Some(soft) = entry.memory_soft.as_mut()
                && soft.observe(observed) == SoftVerdict::Kill
            {
                entry.killing = true;
                Some(entry.kill.clone())
            } else {
                None
            }
        };
        let kill = trigger?;
        if kill.kill(proto::KillReason::Memory) {
            tracing::warn!(%attempt, "killing attempt for exceeding soft memory ceiling");
            Some(attempt.clone())
        } else {
            None
        }
    }

    fn apply_disk_sample(&self, attempt: &AttemptId, bytes: u64) -> Option<AttemptId> {
        let trigger = {
            let mut state = self.registry.state.lock();
            let entry = state
                .attempts
                .iter_mut()
                .find(|entry| &entry.attempt == attempt)?;
            entry.usage.sampled = true;
            entry.usage.record_disk(bytes);
            entry.last_disk_bytes = bytes;
            if !entry.killing
                && !entry.kill.is_killed()
                && let Some(soft) = entry.disk_soft.as_mut()
                && soft.observe(bytes) == SoftVerdict::Kill
            {
                entry.killing = true;
                Some(entry.kill.clone())
            } else {
                None
            }
        };
        let kill = trigger?;
        if kill.kill(proto::KillReason::DiskLimit) {
            tracing::warn!(%attempt, bytes, "killing attempt for exceeding soft disk ceiling");
            Some(attempt.clone())
        } else {
            None
        }
    }

    fn kill_pressure_victim(&mut self, resource: PressureResource) -> Option<AttemptId> {
        let selected = {
            let mut state = self.registry.state.lock();
            if !state.quiescence.is_settled() {
                return None;
            }
            let candidates = state.attempts.iter().map(|entry| {
                let (usage, reserved) = match resource {
                    PressureResource::Memory => {
                        (entry.last_memory_bytes, entry.reserved_memory_bytes)
                    }
                    PressureResource::Disk => (entry.last_disk_bytes, entry.reserved_disk_bytes),
                };
                PressureCandidate {
                    item: entry.attempt.clone(),
                    usage,
                    reserved,
                    finished_or_killing: entry.killing || entry.kill.is_killed(),
                }
            });
            let victim = pick_pressure_victim(candidates)?;
            let entry = state
                .attempts
                .iter_mut()
                .find(|entry| entry.attempt == victim)?;
            entry.killing = true;
            let trigger = entry.kill.clone();
            state
                .quiescence
                .begin_kill(victim.clone(), matches!(resource, PressureResource::Disk));
            Some((victim, trigger))
        };
        let (victim, trigger) = selected?;
        let reason = match resource {
            PressureResource::Memory => proto::KillReason::Memory,
            PressureResource::Disk => proto::KillReason::DiskPressure,
        };
        self.memory_pressure.reset();
        self.disk_pressure.reset();
        if trigger.kill(reason) {
            tracing::warn!(%victim, ?reason, "killing pressure victim");
            Some(victim)
        } else {
            None
        }
    }
}

/// Which resource triggered a worker-wide pressure kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PressureResource {
    Memory,
    Disk,
}

/// Advances a periodic tick schedule strictly past `now` without catching up missed ticks.
fn advance_schedule(previous_due: Tick, now: Tick, interval: Duration) -> Tick {
    let next = previous_due + interval;
    if next > now { next } else { now + interval }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{TestClock, Tick};
    use gang_config::DiskEnforcement;
    use gang_config::DiskTuning;
    use gang_config::MemoryEnforcement;
    use gang_config::MemoryTuning;
    use gang_reapi::leadingterm::gang::worker::v1::KillReason;
    use gang_scheduler::DiskSample;
    use gang_types::{AttemptId, HeadroomPercent};
    use gang_types::{NonZeroBytes, NonZeroDuration, Percent};
    use parking_lot::Mutex;

    use super::*;

    #[test]
    fn process_tree_includes_reparented_session_members_and_descendants() {
        let processes = [
            (10, 1, 10, 10),
            (20, 10, 20, 20),
            (21, 20, 21, 20),
            (22, 1, 22, 20),
            (23, 22, 23, 23),
            (30, 1, 30, 30),
        ];
        assert_eq!(
            collect_process_tree_pids(processes, 20, true),
            [20, 21, 22, 23].into_iter().collect()
        );
        assert_eq!(
            collect_process_tree_pids(processes, 10, false),
            [10, 20, 21].into_iter().collect()
        );
    }

    #[test]
    fn parses_process_stat_and_proportional_memory() {
        assert_eq!(
            parse_proc_stat("123 (foo (bar)) S 42 123 123 0"),
            Some((42, 123, 123))
        );
        assert_eq!(parse_proc_stat("123 (broken) S 42"), None);
        assert_eq!(
            parse_memory_kib("Rss: 30 kB\nPss: 17 kB\n", "Pss:"),
            Some(17 * 1024)
        );
        assert_eq!(parse_memory_kib("Rss: 30 kB\n", "Pss:"), None);
    }

    #[derive(Default)]
    struct FakeProbeState {
        cgroups: HashMap<PathBuf, PathBuf>,
        attempt_cgroup: HashMap<PathBuf, AttemptCgroupSample>,
        attempt_disk: HashMap<PathBuf, u64>,
        memory_pool: MemoryPoolSample,
        disk_space: Option<DiskSample>,
    }

    #[derive(Clone, Default)]
    struct FakeProbe {
        state: Arc<Mutex<FakeProbeState>>,
    }

    impl FakeProbe {
        fn set_cgroup_path(&self, side_channel_dir: &Path, cgroup: PathBuf) {
            self.state
                .lock()
                .cgroups
                .insert(side_channel_dir.to_path_buf(), cgroup);
        }

        fn set_attempt_cgroup(&self, cgroup: &Path, sample: AttemptCgroupSample) {
            self.state
                .lock()
                .attempt_cgroup
                .insert(cgroup.to_path_buf(), sample);
        }

        fn set_attempt_disk(&self, attempt_dir: &Path, bytes: u64) {
            self.state
                .lock()
                .attempt_disk
                .insert(attempt_dir.to_path_buf(), bytes);
        }

        fn set_memory_pool(&self, sample: MemoryPoolSample) {
            self.state.lock().memory_pool = sample;
        }

        fn set_disk_space(&self, sample: Option<DiskSample>) {
            self.state.lock().disk_space = sample;
        }
    }

    impl SampleProbe for FakeProbe {
        fn read_attempt_cgroup_path(&self, side_channel_dir: &Path) -> Option<PathBuf> {
            self.state.lock().cgroups.get(side_channel_dir).cloned()
        }

        fn read_attempt_cgroup(&self, cgroup: &Path) -> AttemptCgroupSample {
            self.state
                .lock()
                .attempt_cgroup
                .get(cgroup)
                .copied()
                .unwrap_or_default()
        }

        fn read_attempt_disk(&self, attempt_dir: &Path) -> Option<u64> {
            self.state.lock().attempt_disk.get(attempt_dir).copied()
        }

        fn read_memory_pool(&self, _pool_cgroup: &Path) -> MemoryPoolSample {
            self.state.lock().memory_pool
        }

        fn read_disk_space(&self, _work_dir: &Path) -> Option<DiskSample> {
            self.state.lock().disk_space
        }
    }

    fn t(ms: u64) -> Tick {
        Tick::ORIGIN + Duration::from_millis(ms)
    }

    fn base_config() -> SamplingConfig {
        SamplingConfig {
            memory: MemoryTuning {
                enforcement: MemoryEnforcement::Pressure,
                reserve: NonZeroBytes::mib(100),
                pressure_stall_percent: Percent::from_percent(10),
                pressure_poll_interval: NonZeroDuration::from_millis(100),
                pressure_kill_after: NonZeroDuration::from_millis(200),
                sample_interval: NonZeroDuration::from_millis(100),
                headroom_percent: HeadroomPercent::from_percent(20),
                soft_samples: NonZeroU32::MIN.saturating_add(1),
                ..MemoryTuning::default()
            },
            disk: DiskTuning {
                enforcement: DiskEnforcement::Pressure,
                pressure_free_percent: Percent::from_percent(5),
                pressure_poll_interval: NonZeroDuration::from_millis(100),
                pressure_kill_after: NonZeroDuration::from_millis(200),
                sample_interval: NonZeroDuration::from_millis(100),
                headroom_percent: HeadroomPercent::from_percent(20),
                soft_samples: NonZeroU32::MIN,
                ..DiskTuning::default()
            },
            memory_pool: Some(PathBuf::from("/sys/fs/cgroup/actions")),
            work_directory: PathBuf::from("/work"),
            disk_per_instance_bytes: 80_000,
        }
    }

    #[test]
    fn memory_psi_pressure_kills_greatest_excess_victim_and_honors_quiescence()
    -> Result<(), Box<dyn std::error::Error>> {
        let probe = FakeProbe::default();
        let mut sampler = WorkerSampler::new(base_config(), probe.clone());
        let registry = sampler.registry();

        let a1 = AttemptId::try_new("attempt-1")?;
        let a2 = AttemptId::try_new("attempt-2")?;
        let (tx1, rx1) = kill_channel();
        let (tx2, rx2) = kill_channel();

        let _g1 = registry.register(AttemptRegistration {
            attempt: a1.clone(),
            attempt_dir: PathBuf::from("/work/attempt-1"),
            side_channel_dir: PathBuf::from("/work/.side_channels/attempt-1"),
            reserved_memory_bytes: 500_000,
            reserved_disk_bytes: 0,
            kill: tx1,
        });
        let g2 = registry.register(AttemptRegistration {
            attempt: a2.clone(),
            attempt_dir: PathBuf::from("/work/attempt-2"),
            side_channel_dir: PathBuf::from("/work/.side_channels/attempt-2"),
            reserved_memory_bytes: 100_000,
            reserved_disk_bytes: 0,
            kill: tx2,
        });

        // a1 uses 400k (under its 500k reservation); a2 uses 250k (150k over its 100k reservation).
        probe.set_cgroup_path(
            Path::new("/work/.side_channels/attempt-1"),
            PathBuf::from("/cg/a1"),
        );
        probe.set_cgroup_path(
            Path::new("/work/.side_channels/attempt-2"),
            PathBuf::from("/cg/a2"),
        );
        probe.set_attempt_cgroup(
            Path::new("/cg/a1"),
            AttemptCgroupSample {
                current_memory_bytes: Some(400_000),
                peak_memory_bytes: Some(420_000),
                cpu_time: Some(Duration::from_millis(50)),
                oom_kills: 0,
            },
        );
        probe.set_attempt_cgroup(
            Path::new("/cg/a2"),
            AttemptCgroupSample {
                current_memory_bytes: Some(250_000),
                peak_memory_bytes: Some(260_000),
                cpu_time: Some(Duration::from_millis(80)),
                oom_kills: 0,
            },
        );

        let reserve = base_config().memory.reserve.get().get();
        // Sustained 20% PSI stall with available < reserve over 200ms.
        probe.set_memory_pool(MemoryPoolSample {
            psi_total_us: Some(1_000),
            available_bytes: Some(reserve - 1),
        });
        assert_eq!(sampler.poll_memory_pressure(t(0)), None);
        probe.set_memory_pool(MemoryPoolSample {
            psi_total_us: Some(21_000),
            available_bytes: Some(reserve - 1),
        });
        assert_eq!(sampler.poll_memory_pressure(t(100)), None);
        probe.set_memory_pool(MemoryPoolSample {
            psi_total_us: Some(41_000),
            available_bytes: Some(reserve - 1),
        });
        let victim = sampler.poll_memory_pressure(t(200));
        assert_eq!(victim, Some(a2.clone()));
        assert_eq!(rx2.reason(), Some(KillReason::Memory));
        assert_eq!(rx1.reason(), None);
        assert!(!registry.is_quiescence_settled());

        // While a2 has not exited, further pressure does not kill a1 (quiescence gate).
        for step in 3..=6 {
            probe.set_memory_pool(MemoryPoolSample {
                psi_total_us: Some(41_000 + step * 20_000),
                available_bytes: Some(reserve - 1),
            });
            assert_eq!(sampler.poll_memory_pressure(t(step * 100)), None);
        }
        assert_eq!(rx1.reason(), None);

        // Once a2 finishes, usage is captured and quiescence settles.
        let usage = g2.finish();
        assert!(usage.sampled);
        assert_eq!(usage.peak_memory_bytes, Some(260_000));
        assert_eq!(usage.cpu_time, Some(Duration::from_millis(80)));
        assert!(registry.is_quiescence_settled());
        Ok(())
    }

    #[test]
    fn disk_pressure_honors_graveyard_credit_emergency_floor_and_quiescence()
    -> Result<(), Box<dyn std::error::Error>> {
        let probe = FakeProbe::default();
        let mut sampler = WorkerSampler::new(base_config(), probe.clone());
        let registry = sampler.registry();

        let a1 = AttemptId::try_new("disk-small")?;
        let a2 = AttemptId::try_new("disk-big")?;
        let (tx1, rx1) = kill_channel();
        let (tx2, rx2) = kill_channel();

        let _g1 = registry.register(AttemptRegistration {
            attempt: a1.clone(),
            attempt_dir: PathBuf::from("/work/disk-small"),
            side_channel_dir: PathBuf::from("/work/.side_channels/disk-small"),
            reserved_memory_bytes: 0,
            reserved_disk_bytes: 500,
            kill: tx1,
        });
        let g2 = registry.register(AttemptRegistration {
            attempt: a2.clone(),
            attempt_dir: PathBuf::from("/work/disk-big"),
            side_channel_dir: PathBuf::from("/work/.side_channels/disk-big"),
            reserved_memory_bytes: 0,
            reserved_disk_bytes: 500,
            kill: tx2,
        });
        probe.set_attempt_disk(Path::new("/work/disk-small"), 400);
        probe.set_attempt_disk(Path::new("/work/disk-big"), 900);

        // 3% raw free + 4% graveyard credit = 7% > 5% threshold -> healthy, no kill.
        probe.set_disk_space(Some(DiskSample {
            free_bytes: 3_000,
            total_bytes: 100_000,
            graveyard_pending_bytes: 4_000,
        }));
        assert_eq!(sampler.poll_disk_pressure(t(0)), None);
        assert_eq!(sampler.poll_disk_pressure(t(300)), None);

        // Below 2% raw free floor: even huge graveyard credit cannot mask disk pressure!
        probe.set_disk_space(Some(DiskSample {
            free_bytes: 1_500,
            total_bytes: 100_000,
            graveyard_pending_bytes: 50_000,
        }));
        assert_eq!(sampler.poll_disk_pressure(t(400)), None);
        assert_eq!(sampler.poll_disk_pressure(t(500)), None);
        let victim = sampler.poll_disk_pressure(t(600));
        assert_eq!(victim, Some(a2.clone()));
        assert_eq!(rx2.reason(), Some(KillReason::DiskPressure));
        assert_eq!(rx1.reason(), None);

        // Disk pressure quiescence waits for BOTH attempt exit AND graveyard processing.
        let usage = g2.finish();
        assert_eq!(usage.peak_disk_bytes, Some(900));
        assert!(!registry.is_quiescence_settled());
        registry.on_graveyard_processed(&a2);
        assert!(registry.is_quiescence_settled());
        Ok(())
    }

    #[test]
    fn unavailable_pss_never_triggers_false_soft_memory_kill()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = base_config();
        config.memory.enforcement = MemoryEnforcement::Soft;
        config.memory.soft_samples = NonZeroU32::MIN;
        config.memory_pool = None;
        let sampler = WorkerSampler::new(config, FakeProbe::default());
        let registry = sampler.registry();
        let attempt = AttemptId::try_new("unavailable-pss")?;
        let (tx, rx) = kill_channel();
        let _guard = registry.register(AttemptRegistration {
            attempt: attempt.clone(),
            attempt_dir: PathBuf::from("/work/unavailable-pss"),
            side_channel_dir: PathBuf::from("/work/.side_channels/unavailable-pss"),
            reserved_memory_bytes: 1,
            reserved_disk_bytes: 0,
            kill: tx,
        });
        registry.set_pgid(&attempt, 42);
        for _ in 0..3 {
            assert!(sampler.sample_memory_attempts().is_empty());
            assert_eq!(rx.reason(), None);
        }
        Ok(())
    }

    #[test]
    fn soft_disk_ceiling_kills_with_disk_limit() -> Result<(), Box<dyn std::error::Error>> {
        let probe = FakeProbe::default();
        let mut config = base_config();
        config.disk.enforcement = DiskEnforcement::Soft;
        config.disk.headroom_percent = HeadroomPercent::from_percent(20);
        config.disk.soft_samples = NonZeroU32::MIN;
        let sampler = WorkerSampler::new(config, probe.clone());
        let registry = sampler.registry();

        let attempt = AttemptId::try_new("soft-disk")?;
        let (tx, rx) = kill_channel();
        let guard = registry.register(AttemptRegistration {
            attempt: attempt.clone(),
            attempt_dir: PathBuf::from("/work/soft-disk"),
            side_channel_dir: PathBuf::from("/work/.side_channels/soft-disk"),
            reserved_memory_bytes: 0,
            reserved_disk_bytes: 1_000,
            kill: tx,
        });

        // Soft ceiling is 1_000 * 120% = 1_200.
        probe.set_attempt_disk(Path::new("/work/soft-disk"), 1_200);
        assert!(sampler.sample_disk_attempts().is_empty());
        assert_eq!(rx.reason(), None);

        probe.set_attempt_disk(Path::new("/work/soft-disk"), 1_201);
        assert_eq!(sampler.sample_disk_attempts(), vec![attempt]);
        assert_eq!(rx.reason(), Some(KillReason::DiskLimit));
        let usage = guard.finish();
        assert_eq!(usage.peak_disk_bytes, Some(1_201));
        assert!(usage.sampled);
        Ok(())
    }

    #[test]
    fn system_probe_reads_real_cgroup_files_and_graveyard_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let cgroup = dir.path().join("cg");
        std::fs::create_dir_all(&cgroup)?;
        std::fs::write(cgroup.join("memory.current"), "4096\n")?;
        std::fs::write(cgroup.join("memory.peak"), "8192\n")?;
        std::fs::write(
            cgroup.join("cpu.stat"),
            "usage_usec 123456\nuser_usec 100\n",
        )?;
        std::fs::write(cgroup.join("memory.events"), "low 0\noom_kill 2\n")?;
        std::fs::write(
            cgroup.join("memory.pressure"),
            "some avg10=1.00 avg60=0.00 avg300=0.00 total=98765\n",
        )?;
        std::fs::write(cgroup.join("memory.max"), "10000\n")?;
        std::fs::write(cgroup.join("memory.high"), "max\n")?;

        let side_channel = dir.path().join("sc");
        std::fs::create_dir_all(&side_channel)?;
        std::fs::write(
            side_channel.join(REPORT_CGROUP_FILE),
            cgroup.as_os_str().as_bytes(),
        )?;

        let graveyard = Arc::new(GraveyardPending::new());
        let reclaimable = graveyard.enqueue(10_000, 3_500);
        assert_eq!(reclaimable, 6_500);
        assert_eq!(graveyard.pending_bytes(), 6_500);

        let probe = SystemProbe::new(Arc::clone(&graveyard));
        assert_eq!(
            probe.read_attempt_cgroup_path(&side_channel),
            Some(cgroup.clone())
        );

        let cg_sample = probe.read_attempt_cgroup(&cgroup);
        assert_eq!(cg_sample.current_memory_bytes, Some(4096));
        assert_eq!(cg_sample.peak_memory_bytes, Some(8192));
        assert_eq!(cg_sample.cpu_time, Some(Duration::from_micros(123_456)));
        assert_eq!(cg_sample.oom_kills, 2);

        let pool = probe.read_memory_pool(&cgroup);
        assert_eq!(pool.psi_total_us, Some(98_765));
        assert_eq!(pool.available_bytes, Some(10_000 - 4_096));

        let disk = probe.read_disk_space(dir.path()).expect("statvfs succeeds");
        assert!(disk.total_bytes > 0);
        assert_eq!(disk.graveyard_pending_bytes, 6_500);

        graveyard.complete(reclaimable);
        assert_eq!(graveyard.pending_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn sampler_run_loop_drives_periodic_sampling_and_stops_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(TestClock::default());
        let probe = FakeProbe::default();
        let sampler = WorkerSampler::new(base_config(), probe.clone());
        let registry = sampler.registry();

        let attempt = AttemptId::try_new("loop-attempt")?;
        let (tx, _rx) = kill_channel();
        let guard = registry.register(AttemptRegistration {
            attempt: attempt.clone(),
            attempt_dir: PathBuf::from("/work/loop-attempt"),
            side_channel_dir: PathBuf::from("/work/.side_channels/loop-attempt"),
            reserved_memory_bytes: 1024,
            reserved_disk_bytes: 1024,
            kill: tx,
        });
        probe.set_cgroup_path(
            Path::new("/work/.side_channels/loop-attempt"),
            PathBuf::from("/cg/loop"),
        );
        probe.set_attempt_cgroup(
            Path::new("/cg/loop"),
            AttemptCgroupSample {
                current_memory_bytes: Some(777),
                peak_memory_bytes: Some(888),
                cpu_time: Some(Duration::from_millis(12)),
                oom_kills: 0,
            },
        );
        probe.set_attempt_disk(Path::new("/work/loop-attempt"), 333);

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let loop_clock = Arc::clone(&clock);
        let task = tokio::spawn(async move {
            sampler
                .run(loop_clock.as_ref(), async move {
                    let _ = stop_rx.await;
                })
                .await;
        });

        // Allow task to park on the first timer, then advance past sample_interval (100ms).
        tokio::task::yield_now().await;
        clock.advance(Duration::from_millis(150));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let _ = stop_tx.send(());
        task.await?;

        let usage = guard.finish();
        assert!(usage.sampled);
        assert_eq!(usage.peak_memory_bytes, Some(888));
        assert_eq!(usage.peak_disk_bytes, Some(333));
        assert_eq!(usage.cpu_time, Some(Duration::from_millis(12)));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn wait_for_ready_file(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(content) = std::fs::read_to_string(path)
                && !content.trim().is_empty()
            {
                return Ok(content);
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for {}", path.display()).into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(target_os = "linux")]
    fn write_ready_atomic(path: &Path, body: &str) -> std::io::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, body)?;
        std::fs::rename(tmp, path)
    }

    #[cfg(target_os = "linux")]
    #[ignore = "child helper for PSS and new-session memory accounting tests"]
    #[test]
    fn sampling_payload_helper() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(mode) = std::env::var("GANG_SAMPLING_HELPER_MODE") else {
            return Ok(());
        };
        let ready = PathBuf::from(
            std::env::var_os("GANG_SAMPLING_READY").ok_or("missing GANG_SAMPLING_READY")?,
        );
        if mode == "new-session-wrapper" {
            let mut child = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "worker_sampling::tests::sampling_payload_helper",
                    "--nocapture",
                ])
                .env("GANG_SAMPLING_HELPER_MODE", "new-session-child")
                .env("GANG_SAMPLING_READY", &ready)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            let status = child.wait()?;
            assert!(status.success());
            return Ok(());
        }
        if mode == "new-session-child" {
            rustix::process::setsid().expect("setsid must succeed");
            let pid = rustix::process::getpid();
            assert_eq!(rustix::process::getsid(None), Ok(pid));
            let mut buf = vec![0x3C_u8; 20 * 1024 * 1024];
            for chunk in buf.chunks_mut(4096) {
                chunk[0] = 0x5A;
            }
            std::hint::black_box(&buf);
            write_ready_atomic(
                &ready,
                &format!("{} {}\n", pid.as_raw_nonzero(), pid.as_raw_nonzero()),
            )?;
            std::thread::sleep(Duration::from_secs(30));
            return Ok(());
        }
        if mode == "shared-parent" {
            let child_ready = ready.with_extension("child");
            let exe = std::env::current_exe()?;
            let mut child = std::process::Command::new(exe)
                .args([
                    "--ignored",
                    "--exact",
                    "worker_sampling::tests::sampling_payload_helper",
                    "--nocapture",
                ])
                .env("GANG_SAMPLING_HELPER_MODE", "shared-child")
                .env("GANG_SAMPLING_READY", &child_ready)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            let child_pid = wait_for_ready_file(&child_ready)?;
            write_ready_atomic(
                &ready,
                &format!("{} {}\n", std::process::id(), child_pid.trim()),
            )?;
            std::thread::sleep(Duration::from_secs(30));
            let _ = child.kill();
            let _ = child.wait();
            return Ok(());
        }
        if mode == "shared-child" {
            write_ready_atomic(&ready, &format!("{}\n", std::process::id()))?;
            std::thread::sleep(Duration::from_secs(30));
            return Ok(());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn read_process_tree_bytes_uses_pss_without_double_counting_shared_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::CommandExt as _;
        use std::process::{Command, Stdio};

        let dir = tempfile::tempdir()?;
        let ready = dir.path().join("shared.ready");
        let exe = std::env::current_exe()?;
        let mut child = Command::new(exe)
            .args([
                "--ignored",
                "--exact",
                "worker_sampling::tests::sampling_payload_helper",
                "--nocapture",
            ])
            .env("GANG_SAMPLING_HELPER_MODE", "shared-parent")
            .env("GANG_SAMPLING_READY", &ready)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;

        let content = wait_for_ready_file(&ready);
        let sample = content.as_ref().ok().and_then(|text| {
            let mut parts = text.split_whitespace();
            let parent: u32 = parts.next()?.parse().ok()?;
            let sub: u32 = parts.next()?.parse().ok()?;
            let tree_bytes = read_process_tree_bytes(parent, Some(parent))?;
            let rss = |pid: u32| {
                std::fs::read_to_string(format!("/proc/{pid}/status"))
                    .ok()
                    .and_then(|s| parse_memory_kib(&s, "VmRSS:"))
            };
            let summed_rss = rss(parent)?.saturating_add(rss(sub)?);
            let pss_readable = std::fs::read_to_string(format!("/proc/{parent}/smaps_rollup"))
                .ok()
                .and_then(|s| parse_memory_kib(&s, "Pss:"))
                .is_some();
            Some((tree_bytes, summed_rss, pss_readable))
        });

        let pgid = i32::try_from(child.id())?;
        if let Some(pid) = rustix::process::Pid::from_raw(pgid) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
        let _ = child.wait();

        let _ = content?;
        let (tree_bytes, summed_rss, pss_readable) =
            sample.ok_or("expected memory measurement for parent+child tree")?;
        assert!(tree_bytes > 0);
        if pss_readable {
            assert!(
                tree_bytes < summed_rss,
                "PSS ({tree_bytes}) must stay strictly below summed VmRSS ({summed_rss}) for shared mappings"
            );
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn read_process_tree_bytes_measures_new_session_sandbox_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::CommandExt as _;
        use std::process::{Command, Stdio};

        let dir = tempfile::tempdir()?;
        let ready = dir.path().join("session.ready");
        let exe = std::env::current_exe()?;
        let mut child = Command::new(exe)
            .args([
                "--ignored",
                "--exact",
                "worker_sampling::tests::sampling_payload_helper",
                "--nocapture",
            ])
            .env("GANG_SAMPLING_HELPER_MODE", "new-session-wrapper")
            .env("GANG_SAMPLING_READY", &ready)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;

        let wrapper_pgid = child.id();
        let ready_result = wait_for_ready_file(&ready);
        let parsed = ready_result.as_ref().ok().and_then(|text| {
            let mut parts = text.split_whitespace();
            Some((
                parts.next()?.parse::<u32>().ok()?,
                parts.next()?.parse::<u32>().ok()?,
            ))
        });
        let measured = parsed.and_then(|(pid, _)| read_process_tree_bytes(wrapper_pgid, Some(pid)));
        if let Some((pid, _)) =
            parsed.and_then(|(pid, sid)| i32::try_from(pid).ok().map(|pid| (pid, sid)))
            && let Some(pid) = rustix::process::Pid::from_raw(pid)
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
        }
        if parsed.is_none() {
            let _ = child.kill();
        }
        let _ = child.wait();
        let _ = ready_result?;
        let (sandbox_pid, sandbox_session) = parsed.ok_or("invalid sandbox pid and sid")?;
        assert_eq!(sandbox_session, sandbox_pid);
        assert_ne!(sandbox_session, wrapper_pgid);
        let bytes = measured.ok_or("expected measurement for new-session sandbox payload")?;
        assert!(
            bytes >= 16 * 1024 * 1024,
            "expected >= 16 MiB for new-session payload, got {bytes} bytes"
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unavailable_pss_does_not_fall_back_to_rss() -> Result<(), Box<dyn std::error::Error>> {
        let proc = tempfile::tempdir()?;
        let process = proc.path().join("42");
        std::fs::create_dir(&process)?;
        std::fs::write(process.join("stat"), "42 (payload) S 1 42 42 0")?;
        std::fs::write(process.join("status"), "VmRSS: 999999 kB\n")?;
        std::fs::write(process.join("smaps_rollup"), "Rss: 999999 kB\n")?;
        assert_eq!(read_process_tree_pss_from(proc.path(), 42, None), None);
        Ok(())
    }
}
