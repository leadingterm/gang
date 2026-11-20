//! Rate-based scaling policy and its bounded scalar settings.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{BoundedDuration, NANOS_PER_MILLI, NonZeroDuration};
use serde::{Deserialize, Serialize};

use crate::errors::{PaybackPercentError, SourceLimitError, WaterlinePercentError};

/// A positive whole-millisecond interval no longer than one hour.
pub type ScalingInterval = BoundedDuration<NANOS_PER_MILLI, 1, 3_600_000>;

/// The payback horizon as a percentage of readiness plus the fleet's idle timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct PaybackPercent(u16);

impl PaybackPercent {
    /// Constructs a percentage from 1 through 1000 inclusive.
    ///
    /// # Errors
    /// Returns an error when the percentage is outside that range.
    pub fn try_new(value: u16) -> Result<Self, PaybackPercentError> {
        if (1..=1000).contains(&value) {
            Ok(Self(value))
        } else {
            Err(PaybackPercentError::OutOfRange(value))
        }
    }

    /// Returns the percentage.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl Default for PaybackPercent {
    fn default() -> Self {
        Self(50)
    }
}

impl TryFrom<u16> for PaybackPercent {
    type Error = PaybackPercentError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<PaybackPercent> for u16 {
    fn from(value: PaybackPercent) -> Self {
        value.0
    }
}

/// The bound on distinct retained sources, excluding reserved unattributed state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "usize", into = "usize")]
pub struct SourceLimit(NonZeroUsize);

impl SourceLimit {
    /// Constructs a limit from 1 through 65536 inclusive.
    ///
    /// # Errors
    /// Returns an error when the limit is outside that range.
    pub fn try_new(value: usize) -> Result<Self, SourceLimitError> {
        let Some(value) = NonZeroUsize::new(value).filter(|value| value.get() <= 65_536) else {
            return Err(SourceLimitError::OutOfRange(value));
        };
        Ok(Self(value))
    }

    /// Returns the source count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for SourceLimit {
    fn default() -> Self {
        Self(NonZeroUsize::MIN.saturating_add(1023))
    }
}

impl TryFrom<usize> for SourceLimit {
    type Error = SourceLimitError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<SourceLimit> for usize {
    fn from(value: SourceLimit) -> Self {
        value.get()
    }
}

impl From<SourceLimit> for NonZeroUsize {
    fn from(value: SourceLimit) -> Self {
        value.0
    }
}

/// The shared N−1 capacity waterline and completion-refill tolerance, in percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct WaterlinePercent(u8);

impl WaterlinePercent {
    /// Constructs a percentage from 1 through 99 inclusive.
    ///
    /// # Errors
    /// Returns an error when the percentage is outside that range.
    pub fn try_new(value: u8) -> Result<Self, WaterlinePercentError> {
        if (1..=99).contains(&value) {
            Ok(Self(value))
        } else {
            Err(WaterlinePercentError::OutOfRange(value))
        }
    }

    /// Returns the percentage.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl Default for WaterlinePercent {
    fn default() -> Self {
        Self(70)
    }
}

impl TryFrom<u8> for WaterlinePercent {
    type Error = WaterlinePercentError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<WaterlinePercent> for u8 {
    fn from(value: WaterlinePercent) -> Self {
        value.0
    }
}

/// The `[scaling]` policy. Every omitted setting uses its documented default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScalingConfig {
    /// Payback horizon as a percentage of readiness plus idle timeout. Default: 50.
    pub payback_percent: PaybackPercent,
    /// Load-estimator half-life, whole milliseconds from 1ms through 1h. Default: `60s`.
    pub load_half_life: ScalingInterval,
    /// Observed quiet that closes a source activity period, 1ms through 1h. Default: `60s`.
    pub source_idle: ScalingInterval,
    /// Maximum distinct retained sources, from 1 through 65536. Default: 1024.
    pub max_sources: SourceLimit,
    /// Last-resort action duration, positive whole milliseconds. Default: `10s`.
    pub unknown_action_duration: NonZeroDuration,
    /// Cold readiness prior, positive whole milliseconds. Default: `90s`.
    /// Clamped per fleet by [`Self::effective_readiness_prior`], never rejected against boot grace.
    pub readiness_prior: NonZeroDuration,
    /// N−1 waterline and refill tolerance, from 1 through 99 percent. Default: 70.
    /// Lowering it also loosens the completion-refill test.
    pub scale_in_waterline_percent: WaterlinePercent,
    /// Continuous N−1 feasibility before cordoning. Default: `5m`; at least `load_half_life`.
    pub scale_in_dwell: NonZeroDuration,
    /// Concurrent physical-instance cordon budget per fleet. Default: 1.
    /// Zero disables rate-based cordons, not idle-timeout drains.
    pub max_cordons: u16,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            payback_percent: PaybackPercent::default(),
            load_half_life: ScalingInterval::from_units(60_000),
            source_idle: ScalingInterval::from_units(60_000),
            max_sources: SourceLimit::default(),
            unknown_action_duration: NonZeroDuration::from_secs(10),
            readiness_prior: NonZeroDuration::from_secs(90),
            scale_in_waterline_percent: WaterlinePercent::default(),
            scale_in_dwell: NonZeroDuration::from_mins(5),
            max_cordons: 1,
        }
    }
}

impl ScalingConfig {
    /// Returns this fleet's cold readiness prior: the smaller of the configured prior and
    /// its boot grace. A zero boot grace yields zero; measured readiness is unaffected.
    #[must_use]
    pub fn effective_readiness_prior(&self, boot_grace: Duration) -> Duration {
        self.readiness_prior.get().min(boot_grace)
    }
}
