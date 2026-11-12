//! Enforcement of reservations: whether a worker holds an action to the memory and disk it
//! reserved, how a killed action is escalated, and the reservation of an action nothing has
//! measured yet.

use std::fmt;
use std::num::NonZeroU32;

use gang_types::HeadroomPercent;
use serde::{Deserialize, Serialize};

use crate::whole_kib::WholeKib;

/// Percent above the reservation an action may reach before it is killed.
const DEFAULT_MEMORY_HEADROOM_PERCENT: HeadroomPercent = HeadroomPercent::from_percent(20);
/// The most requeues with a larger reservation before the action fails.
const DEFAULT_ESCALATION_MAX_STEPS: u32 = 8;
/// Percent above its reservation an action's own files may reach before it is killed.
const DEFAULT_DISK_HEADROOM_PERCENT: HeadroomPercent = HeadroomPercent::from_percent(20);
/// Cores an unmeasured action reserves.
const DEFAULT_COLD_START_CPU_COUNT: NonZeroU32 = NonZeroU32::MIN;
/// Memory an unmeasured action reserves: 4 GiB.
const DEFAULT_COLD_START_MEMORY: WholeKib = WholeKib::from_kib(4 * 1024 * 1024);
/// Disk an unmeasured action reserves: 4 GiB.
const DEFAULT_COLD_START_DISK: WholeKib = WholeKib::from_kib(4 * 1024 * 1024);

gang_types::string_enum! {
    /// How a worker holds an action to the memory it reserved.
    pub enum MemoryEnforcement("memory enforcement") {
        /// Measure only: nothing is killed and nothing is requeued.
        None = "none",
        /// Kill the action's process group once it exceeds its reservation plus headroom, and requeue
        /// it with a larger reservation.
        Soft = "soft",
    }
}

impl Default for MemoryEnforcement {
    /// Uses soft enforcement by default.
    fn default() -> Self {
        Self::Soft
    }
}

/// Memory enforcement of reservations. Every field has a default, so an empty section is valid.
///
/// With `enforcement = "none"` nothing is killed and nothing is requeued, whatever the other
/// fields say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MemoryTuning {
    /// What a worker does with an action that exceeds its memory reservation. Default: `soft`.
    pub enforcement: MemoryEnforcement,
    /// Percent above its reservation an action may reach before `NativeLink` kills it. Default: 20.
    pub headroom_percent: HeadroomPercent,
    /// A killed action is requeued with its reservation scaled to this percent, so 200 doubles
    /// it and 150 adds half. Must be greater than 100. Default: 200.
    pub escalation_percent: EscalationPercent,
    /// The most requeues with a larger reservation before the action fails. 0 lets the largest
    /// worker's memory alone bound them. Default: 8.
    pub escalation_max_steps: u32,
}

impl Default for MemoryTuning {
    fn default() -> Self {
        Self {
            enforcement: MemoryEnforcement::default(),
            headroom_percent: DEFAULT_MEMORY_HEADROOM_PERCENT,
            escalation_percent: EscalationPercent::DEFAULT,
            escalation_max_steps: DEFAULT_ESCALATION_MAX_STEPS,
        }
    }
}

/// The percent a killed action's reservation is scaled to when it is requeued. Always greater
/// than 100, so every requeue reserves more than the one before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct EscalationPercent(u32);

impl EscalationPercent {
    /// The default: 200, which doubles the reservation.
    pub const DEFAULT: Self = Self(200);

    /// Builds an escalation percent from `percent`.
    ///
    /// # Errors
    ///
    /// Returns [`EscalationError`] unless `percent` is greater than 100.
    pub fn try_new(percent: u32) -> Result<Self, EscalationError> {
        if percent > 100 {
            Ok(Self(percent))
        } else {
            Err(EscalationError)
        }
    }

    /// The percent, so 200 doubles a reservation.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for EscalationPercent {
    type Error = EscalationError;

    fn try_from(percent: u32) -> Result<Self, Self::Error> {
        Self::try_new(percent)
    }
}

impl From<EscalationPercent> for u32 {
    fn from(percent: EscalationPercent) -> Self {
        percent.0
    }
}

/// An escalation percent that is not greater than 100.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EscalationError;

impl fmt::Display for EscalationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("must be greater than 100")
    }
}

impl std::error::Error for EscalationError {}

gang_types::string_enum! {
    /// How a worker holds an action to the disk it reserved.
    pub enum DiskEnforcement("disk enforcement") {
        /// Measure only: nothing is refused and nothing is killed.
        None = "none",
        /// Refuse an action whose disk reservation exceeds the free space under the work directory,
        /// less what the actions already admitted reserved.
        Guard = "guard",
        /// Guard, and kill an action whose own files pass its reservation plus headroom. Sampled every
        /// ten seconds on Linux only; elsewhere `soft` acts as `guard`.
        Soft = "soft",
    }
}

impl Default for DiskEnforcement {
    /// Uses soft enforcement by default.
    fn default() -> Self {
        Self::Soft
    }
}

/// Disk enforcement of reservations. Every field has a default, so an empty section is valid.
///
/// With `enforcement = "none"` nothing is refused and nothing is killed, whatever the other fields
/// say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DiskTuning {
    /// What a worker does with an action whose disk reservation the free space cannot hold, or
    /// whose own files outgrow it. Default: `soft`.
    pub enforcement: DiskEnforcement,
    /// Percent above its reservation an action's own files may reach before it is killed.
    /// Default: 20.
    pub headroom_percent: HeadroomPercent,
}

impl Default for DiskTuning {
    fn default() -> Self {
        Self {
            enforcement: DiskEnforcement::default(),
            headroom_percent: DEFAULT_DISK_HEADROOM_PERCENT,
        }
    }
}

/// The reservation an action gets when the scheduler has measured nothing for its kind and the
/// client reserved nothing.
///
/// Until its kind is measured, a cold action reserves this much of its worker, so a worker runs
/// fewer such actions at once until they are learned. Memory and disk enforcement hold the action
/// to it: a memory kill requeues the action with a larger reservation, and a disk kill fails the
/// action, after which the measured peak raises the reservation of its next run.
///
/// With `enabled = false`, unmeasured actions reserve nothing and so are not enforced.
///
/// The three values are always written together: the scheduler keeps a dimension's learned
/// reservation only when the cold start names it. Every value is always valid, even when the
/// cold start is disabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ColdStartTuning {
    /// Whether an unmeasured action gets the reservation below. Default: true.
    pub enabled: bool,
    /// Cores an unmeasured action reserves, on the scale the workers advertise. Must be at least
    /// 1. Default: 1.
    pub cpu_count: NonZeroU32,
    /// Memory an unmeasured action reserves. Must be a whole number of KiB, at least 1KiB.
    /// Default: 4 GiB.
    pub memory: WholeKib,
    /// Disk an unmeasured action reserves. Must be a whole number of KiB, at least 1KiB.
    /// Default: 4 GiB.
    pub disk: WholeKib,
}

impl Default for ColdStartTuning {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_count: DEFAULT_COLD_START_CPU_COUNT,
            memory: DEFAULT_COLD_START_MEMORY,
            disk: DEFAULT_COLD_START_DISK,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforcement_modes_keep_their_wire_names() {
        for (mode, name) in [
            (MemoryEnforcement::None, "none"),
            (MemoryEnforcement::Soft, "soft"),
        ] {
            let json = format!("\"{name}\"");
            assert_eq!(serde_json::to_string(&mode).expect("serialize mode"), json);
            assert_eq!(
                serde_json::from_str::<MemoryEnforcement>(&json).expect("parse mode"),
                mode
            );
        }
        for (mode, name) in [
            (DiskEnforcement::None, "none"),
            (DiskEnforcement::Guard, "guard"),
            (DiskEnforcement::Soft, "soft"),
        ] {
            let json = format!("\"{name}\"");
            assert_eq!(serde_json::to_string(&mode).expect("serialize mode"), json);
            assert_eq!(
                serde_json::from_str::<DiskEnforcement>(&json).expect("parse mode"),
                mode
            );
        }
    }

    #[test]
    fn defaults_match_documented_values() {
        let memory = MemoryTuning::default();
        assert_eq!(memory.escalation_percent, EscalationPercent::DEFAULT);
        assert_eq!(memory.escalation_percent.get(), 200);
        assert_eq!(memory.headroom_percent.get(), 20);
        assert_eq!(memory.escalation_max_steps, 8);

        let cold = ColdStartTuning::default();
        assert!(cold.enabled);
        assert_eq!(cold.cpu_count.get(), 1);
        assert_eq!(cold.memory.as_kib(), 4 * 1024 * 1024);
        assert_eq!(cold.disk.as_kib(), 4 * 1024 * 1024);
    }

    #[test]
    fn escalation_percent_must_be_greater_than_100() {
        assert!(EscalationPercent::try_new(100).is_err());
        assert_eq!(EscalationPercent::try_new(101).unwrap().get(), 101);
        assert!(serde_json::from_str::<MemoryTuning>(r#"{"escalation_percent": 100}"#).is_err());
        let memory: MemoryTuning = serde_json::from_str(r#"{"escalation_percent": 101}"#).unwrap();
        assert_eq!(memory.escalation_percent.get(), 101);
    }

    #[test]
    fn cold_start_sizes_are_whole_kib_at_least_one_kib() {
        assert!(serde_json::from_str::<ColdStartTuning>(r#"{"memory": "512B"}"#).is_err());
        assert!(serde_json::from_str::<ColdStartTuning>(r#"{"disk": "512B"}"#).is_err());
        let cold: ColdStartTuning =
            serde_json::from_str(r#"{"memory": "4GiB", "disk": "4GiB"}"#).unwrap();
        assert_eq!(cold.memory.as_kib(), 4 * 1024 * 1024);
        assert_eq!(cold.disk.as_kib(), 4 * 1024 * 1024);
    }

    #[test]
    fn cold_start_cpu_count_must_be_non_zero() {
        assert!(serde_json::from_str::<ColdStartTuning>(r#"{"cpu_count": 0}"#).is_err());
    }
}
