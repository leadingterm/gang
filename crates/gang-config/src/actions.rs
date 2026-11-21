//! Action scheduling, history, and resource policy.

use std::num::{NonZeroU32, NonZeroUsize};

use gang_types::{
    BoundedDuration, EnvironmentName, HeadroomPercent, NANOS_PER_SECOND, NonZeroBytes,
    NonZeroDuration,
};
use serde::{Deserialize, Serialize};

use crate::{ColdStartTuning, DiskTuning, MemoryTuning};

const NANOS_PER_DAY: u64 = 86_400 * NANOS_PER_SECOND;
/// A positive whole number of days.
pub type HistoryWindow = BoundedDuration<NANOS_PER_DAY, 1, { u64::MAX / NANOS_PER_DAY }>;
/// A positive whole number of seconds.
pub type WholeSeconds = BoundedDuration<NANOS_PER_SECOND, 1, { u64::MAX / NANOS_PER_SECOND }>;

/// Policy shared by action scheduling and worker enforcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ActionsConfig {
    /// Server-owned action-cache environment version. Default: `default/1` (existing warm cache).
    /// Bump only when action-visible software changes, never for security-only image rebuilds.
    pub environment: EnvironmentName,
    /// Percentage added to measured peaks for learning and resource escalation. Default: 20.
    pub reservation_headroom_percent: HeadroomPercent,
    /// Multiplier for the worker's physical CPU reservation capacity. Default: 2.
    pub cpu_oversubscription: NonZeroU32,
    /// History retention, in whole days, at least one day. Default: `7d`.
    pub history_window: HistoryWindow,
    /// Maximum target/mnemonic duration-cache keys, at least one. Default: 65536.
    pub history_keys: NonZeroUsize,
    /// Resource sketch memory budget in bytes, nonzero. Default: `512MiB`.
    pub history_memory_budget: NonZeroBytes,
    /// History flush interval, nonzero whole milliseconds. Default: `30s`.
    pub history_flush_interval: NonZeroDuration,
    /// Completed-job retention, in whole seconds, at least one second. Default: `60s`.
    pub retain_completed_for: WholeSeconds,
    /// Generic failure retries, at least one; resource requeues do not consume these. Default: 3.
    pub max_job_retries: NonZeroU32,
    /// Worker-loss retries, at least one. Default: 10.
    pub max_worker_loss_retries: NonZeroU32,
    /// Action execution timeout in whole seconds, at least one second. Default: `20m`.
    pub timeout: WholeSeconds,
    /// Grace before forced termination, nonzero whole milliseconds. Default: `5s`.
    pub kill_grace: NonZeroDuration,
    /// Memory policy; each setting uses its documented default when omitted.
    pub memory: MemoryTuning,
    /// Disk policy; each setting uses its documented default when omitted.
    pub disk: DiskTuning,
    /// Initial reservation for unmeasured actions; enabled by default.
    pub cold_start: ColdStartTuning,
}
impl Default for ActionsConfig {
    fn default() -> Self {
        Self {
            environment: EnvironmentName::default(),
            reservation_headroom_percent: HeadroomPercent::from_percent(20),
            cpu_oversubscription: NonZeroU32::MIN.saturating_add(1),
            history_window: HistoryWindow::from_units(7),
            history_keys: NonZeroUsize::MIN.saturating_add(65_535),
            history_memory_budget: NonZeroBytes::mib(512),
            history_flush_interval: NonZeroDuration::from_secs(30),
            retain_completed_for: WholeSeconds::from_units(60),
            max_job_retries: NonZeroU32::MIN.saturating_add(2),
            max_worker_loss_retries: NonZeroU32::MIN.saturating_add(9),
            timeout: WholeSeconds::from_units(1200),
            kill_grace: NonZeroDuration::from_secs(5),
            memory: MemoryTuning::default(),
            disk: DiskTuning::default(),
            cold_start: ColdStartTuning::default(),
        }
    }
}
