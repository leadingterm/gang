//! Typed action resource reservations and enforcement.

use std::fmt;
use std::num::NonZeroU32;

use gang_types::{BoundedBytes, HeadroomPercent, NonZeroBytes, NonZeroDuration, Percent};
use serde::{Deserialize, Serialize};

/// A positive whole number of KiB.
pub type WholeKib = BoundedBytes<1024, 1, { u64::MAX / 1024 }>;
/// A memory reservation of at least 4 MiB, in whole KiB.
pub type MinReservation = BoundedBytes<1024, { 4 * 1024 }, { u64::MAX / 1024 }>;
/// A disk reservation of at least 1 MiB, in whole KiB.
pub type DiskMinReservation = BoundedBytes<1024, 1024, { u64::MAX / 1024 }>;

gang_types::string_enum! {
    /// How worker memory is enforced.
    pub enum MemoryEnforcement("memory enforcement") default Pressure {
        /// No userspace enforcement.
        None = "none",
        /// Kill under sustained pool pressure.
        Pressure = "pressure",
        /// Enforce a per-action userspace ceiling.
        Soft = "soft",
    }
}

gang_types::string_enum! {
    /// How worker disk is enforced.
    pub enum DiskEnforcement("disk enforcement") default Pressure {
        /// No enforcement or admission guard.
        None = "none",
        /// Admission guard only.
        Guard = "guard",
        /// Kill under sustained filesystem pressure.
        Pressure = "pressure",
        /// Enforce a per-action userspace ceiling.
        Soft = "soft",
    }
}

/// A scaling percentage strictly greater than 100.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct EscalationPercent(u32);

impl EscalationPercent {
    /// The default doubling percentage.
    pub const DEFAULT: Self = Self(200);
    /// Constructs a percentage greater than 100.
    ///
    /// # Errors
    /// Returns an error if the percentage is not greater than 100.
    pub fn try_new(value: u32) -> Result<Self, EscalationError> {
        if value > 100 {
            Ok(Self(value))
        } else {
            Err(EscalationError)
        }
    }
    /// Returns the percentage.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}
impl TryFrom<u32> for EscalationPercent {
    type Error = EscalationError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}
impl From<EscalationPercent> for u32 {
    fn from(value: EscalationPercent) -> Self {
        value.0
    }
}
/// An escalation percentage not greater than 100.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EscalationError;
impl fmt::Display for EscalationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("must be greater than 100")
    }
}
impl std::error::Error for EscalationError {}

/// Memory reservation and enforcement settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MemoryTuning {
    /// `pressure`, `soft`, or `none`. Default: `pressure`.
    pub enforcement: MemoryEnforcement,
    /// Minimum nonzero action reservation, whole KiB and at least 4 MiB; must fit every fleet.
    /// Default: `64MiB`.
    pub min_reservation: MinReservation,
    /// Bytes withheld outside the pool in every mode; nonzero and below every fleet's memory.
    /// Also the pressure shortage watermark: pool available bytes must stay below it
    /// alongside sustained stalls for a pressure kill. Default: `1GiB`.
    pub reserve: NonZeroBytes,
    /// Pressure-mode interval stall percentage, 1 through 100. Default: 10.
    pub pressure_stall_percent: Percent,
    /// Pressure-mode polling interval, nonzero whole milliseconds. Default: `250ms`.
    pub pressure_poll_interval: NonZeroDuration,
    /// Sustained pressure before a pressure-mode kill, nonzero whole milliseconds. Default: `5s`.
    pub pressure_kill_after: NonZeroDuration,
    /// Action memory sampling interval, nonzero whole milliseconds. Default: `250ms`.
    pub sample_interval: NonZeroDuration,
    /// Memory-kill reservation multiplier in percent, greater than 100. Default: 200.
    pub escalation_percent: EscalationPercent,
    /// Maximum memory resource requeues, at least one. Default: 8.
    pub max_steps: NonZeroU32,
    /// Percentage above reservation for the soft-mode ceiling. Default: 20.
    pub headroom_percent: HeadroomPercent,
    /// Consecutive soft-mode ceiling breaches before a kill, at least one. Default: 2.
    pub soft_samples: NonZeroU32,
    /// Soft-mode immediate-kill percentage of the ceiling, greater than 100. Default: 200.
    pub soft_spike_percent: EscalationPercent,
}
impl Default for MemoryTuning {
    fn default() -> Self {
        Self {
            enforcement: MemoryEnforcement::Pressure,
            min_reservation: MinReservation::from_units(64 * 1024),
            reserve: NonZeroBytes::gib(1),
            pressure_stall_percent: Percent::from_percent(10),
            pressure_poll_interval: NonZeroDuration::from_millis(250),
            pressure_kill_after: NonZeroDuration::from_secs(5),
            sample_interval: NonZeroDuration::from_millis(250),
            escalation_percent: EscalationPercent::DEFAULT,
            max_steps: NonZeroU32::MIN.saturating_add(7),
            headroom_percent: HeadroomPercent::from_percent(20),
            soft_samples: NonZeroU32::MIN.saturating_add(1),
            soft_spike_percent: EscalationPercent::DEFAULT,
        }
    }
}

/// Disk reservation and enforcement settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DiskTuning {
    /// `pressure`, `soft`, `guard`, or `none`. Default: `pressure`.
    pub enforcement: DiskEnforcement,
    /// Minimum nonzero reservation, whole KiB and at least 1 MiB; must fit every fleet.
    /// Default: `1GiB`.
    pub min_reservation: DiskMinReservation,
    /// Pressure-mode free-space percentage threshold, 1 through 100. Default: 5.
    pub pressure_free_percent: Percent,
    /// Pressure-mode polling interval, nonzero whole milliseconds. Default: `2s`.
    pub pressure_poll_interval: NonZeroDuration,
    /// Sustained pressure before a pressure-mode kill, nonzero whole milliseconds. Default: `5s`.
    pub pressure_kill_after: NonZeroDuration,
    /// Action disk sampling interval, nonzero whole milliseconds. Default: `10s`.
    pub sample_interval: NonZeroDuration,
    /// Disk-kill reservation multiplier in percent, greater than 100. Default: 200.
    pub escalation_percent: EscalationPercent,
    /// Maximum disk resource requeues, at least one. Default: 8.
    pub max_steps: NonZeroU32,
    /// Percentage above reservation for the soft-mode ceiling. Default: 20.
    pub headroom_percent: HeadroomPercent,
    /// Consecutive soft-mode ceiling breaches before a kill, at least one. Default: 1.
    pub soft_samples: NonZeroU32,
}
impl Default for DiskTuning {
    fn default() -> Self {
        Self {
            enforcement: DiskEnforcement::Pressure,
            min_reservation: DiskMinReservation::from_units(1024 * 1024),
            pressure_free_percent: Percent::from_percent(5),
            pressure_poll_interval: NonZeroDuration::from_secs(2),
            pressure_kill_after: NonZeroDuration::from_secs(5),
            sample_interval: NonZeroDuration::from_secs(10),
            escalation_percent: EscalationPercent::DEFAULT,
            max_steps: NonZeroU32::MIN.saturating_add(7),
            headroom_percent: HeadroomPercent::from_percent(20),
            soft_samples: NonZeroU32::MIN,
        }
    }
}

/// Initial reservation before an action family has been measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ColdStartTuning {
    /// Whether to reserve resources for unmeasured actions. Default: true.
    pub enabled: bool,
    /// Reserved whole CPUs, at least one. Default: 1.
    pub cpu_count: NonZeroU32,
    /// Reserved memory, nonzero whole KiB. Default: `4GiB`.
    pub memory: WholeKib,
    /// Reserved disk, nonzero whole KiB. Default: `4GiB`.
    pub disk: WholeKib,
}
impl Default for ColdStartTuning {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_count: NonZeroU32::MIN,
            memory: WholeKib::from_units(4 * 1024 * 1024),
            disk: WholeKib::from_units(4 * 1024 * 1024),
        }
    }
}
