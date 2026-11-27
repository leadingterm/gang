//! Linux `cgroup` v2 `cpu.weight` values and priority-tier mapping.

use std::fmt;
use std::num::NonZeroU16;
use std::str::FromStr;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Minimum valid `cgroup` v2 `cpu.weight`.
const MIN_WEIGHT: u16 = 1;

/// Maximum valid `cgroup` v2 `cpu.weight`.
const MAX_WEIGHT: u16 = 10_000;

/// Error returned when a value is outside the `cgroup` v2 `cpu.weight` range (`1..=10_000`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CpuWeightError {
    /// The input string is not a valid unsigned integer.
    #[error("cpu weight `{input}` is not a valid integer")]
    InvalidSyntax {
        /// The rejected input text.
        input: String,
    },
    /// The weight lies outside `1..=10_000`.
    #[error("cpu weight {value} is out of range, expected 1..=10000")]
    OutOfRange {
        /// The rejected weight value.
        value: u32,
    },
}

/// Validated Linux `cgroup` v2 `cpu.weight` in `1..=10_000`.
///
/// ```
/// use gang_types::CpuWeight;
///
/// assert_eq!(CpuWeight::from_priority(200), CpuWeight::GATE);
/// assert_eq!(CpuWeight::from_priority(100), CpuWeight::INTERACTIVE);
/// assert_eq!(CpuWeight::from_priority(0), CpuWeight::BACKGROUND);
/// assert_eq!(CpuWeight::GATE.get(), 1000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CpuWeight(NonZeroU16);

impl CpuWeight {
    /// Minimum permitted weight (`1`).
    pub const MIN: Self = Self::from_const(MIN_WEIGHT);

    /// Maximum permitted weight (`10_000`).
    pub const MAX: Self = Self::from_const(MAX_WEIGHT);

    /// Weight assigned to gate-tier actions (`priority >= 200`).
    pub const GATE: Self = Self::from_const(1_000);

    /// Weight assigned to interactive-tier actions (`priority` in `100..=199`).
    pub const INTERACTIVE: Self = Self::from_const(100);

    /// Weight assigned to background-tier actions (`priority < 100`).
    pub const BACKGROUND: Self = Self::from_const(20);

    /// Weight assigned to the worker cgroup sibling (`1000`) to hold a 1:1 split with actions.
    pub const WORKER: Self = Self::from_const(1_000);

    const fn from_const(value: u16) -> Self {
        assert!(
            value >= MIN_WEIGHT && value <= MAX_WEIGHT,
            "cgroup v2 cpu.weight must be in 1..=10_000"
        );
        match NonZeroU16::new(value) {
            Some(weight) => Self(weight),
            None => Self(NonZeroU16::MIN),
        }
    }

    /// Validates that `value` lies within the `cgroup` v2 `cpu.weight` range `1..=10_000`.
    ///
    /// # Errors
    ///
    /// Returns [`CpuWeightError::OutOfRange`] if `value == 0` or `value > 10_000`.
    pub const fn try_new(value: u32) -> Result<Self, CpuWeightError> {
        if value < MIN_WEIGHT as u32 || value > MAX_WEIGHT as u32 {
            return Err(CpuWeightError::OutOfRange { value });
        }
        #[allow(clippy::cast_possible_truncation)]
        match NonZeroU16::new(value as u16) {
            Some(weight) => Ok(Self(weight)),
            None => Err(CpuWeightError::OutOfRange { value }),
        }
    }

    /// Maps an `REAPI` `ExecutionPolicy.priority` onto its per-core `CpuWeight` `QoS` tier.
    ///
    /// - `priority >= 200` -> [`CpuWeight::GATE`] (`1000`)
    /// - `100..=199` -> [`CpuWeight::INTERACTIVE`] (`100`)
    /// - `< 100` (including `0` and negatives) -> [`CpuWeight::BACKGROUND`] (`20`)
    #[must_use]
    pub const fn from_priority(priority: i32) -> Self {
        if priority >= 200 {
            Self::GATE
        } else if priority >= 100 {
            Self::INTERACTIVE
        } else {
            Self::BACKGROUND
        }
    }

    /// Maps an `REAPI` `ExecutionPolicy.priority` and reserved CPU millicores onto a core-scaled
    /// `cgroup` v2 `cpu.weight`:
    /// `clamp(tier_per_core * max(1, ceil(reserved_millicores / 1000)), 1, 10_000)`.
    ///
    /// Single-core (or sub-core / zero) reservations preserve the exact `1000 / 100 / 20` tier
    /// weights, while multi-core reservations scale proportionally so multi-threaded actions are
    /// not starved by many single-threaded cgroups under oversubscription.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn for_priority_and_millicores(priority: i32, reserved_millicores: u64) -> Self {
        let per_core = Self::from_priority(priority).get() as u64;
        let cores = if reserved_millicores == 0 {
            1
        } else {
            reserved_millicores.div_ceil(1_000)
        };
        let scaled = per_core.saturating_mul(cores);
        let clamped = if scaled > 10_000 {
            10_000_u16
        } else if scaled == 0 {
            1_u16
        } else {
            scaled as u16
        };
        match NonZeroU16::new(clamped) {
            Some(weight) => Self(weight),
            None => Self::MIN,
        }
    }

    /// Returns the weight as a `u32` in `1..=10_000`.
    #[must_use]
    #[allow(clippy::cast_lossless)]
    pub const fn get(&self) -> u32 {
        self.0.get() as u32
    }
}

impl fmt::Display for CpuWeight {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for CpuWeight {
    type Err = CpuWeightError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(CpuWeightError::InvalidSyntax {
                input: input.to_owned(),
            });
        }
        let value = input.bytes().fold(0_u32, |accumulated, byte| {
            accumulated
                .saturating_mul(10)
                .saturating_add(u32::from(byte - b'0'))
        });
        Self::try_new(value)
    }
}

impl TryFrom<u32> for CpuWeight {
    type Error = CpuWeightError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<CpuWeight> for u32 {
    fn from(weight: CpuWeight) -> Self {
        weight.get()
    }
}

impl Serialize for CpuWeight {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.get())
    }
}

impl<'de> Deserialize<'de> for CpuWeight {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(CpuWeightVisitor)
    }
}

struct CpuWeightVisitor;

impl Visitor<'_> for CpuWeightVisitor {
    type Value = CpuWeight;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a cpu.weight integer from 1 to 10000")
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        u32::try_from(value)
            .ok()
            .and_then(|raw| CpuWeight::try_new(raw).ok())
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(value), &self))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        u32::try_from(value)
            .ok()
            .and_then(|raw| CpuWeight::try_new(raw).ok())
            .ok_or_else(|| E::invalid_value(Unexpected::Signed(value), &self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        cpu_weight: CpuWeight,
    }

    #[test]
    fn accepts_bounds_and_rejects_outside_range() {
        assert_eq!(
            CpuWeight::try_new(0),
            Err(CpuWeightError::OutOfRange { value: 0 })
        );
        assert_eq!(
            CpuWeight::try_new(10_001),
            Err(CpuWeightError::OutOfRange { value: 10_001 })
        );
        assert_eq!(
            CpuWeight::try_new(u32::MAX),
            Err(CpuWeightError::OutOfRange { value: u32::MAX })
        );

        assert_eq!(CpuWeight::try_new(1), Ok(CpuWeight::MIN));
        assert_eq!(CpuWeight::try_new(10_000), Ok(CpuWeight::MAX));
        assert_eq!(CpuWeight::MIN.get(), 1);
        assert_eq!(CpuWeight::MAX.get(), 10_000);
    }

    #[test]
    fn constants_match_spec() {
        assert_eq!(CpuWeight::GATE.get(), 1_000);
        assert_eq!(CpuWeight::INTERACTIVE.get(), 100);
        assert_eq!(CpuWeight::BACKGROUND.get(), 20);
        assert_eq!(CpuWeight::WORKER.get(), 1_000);
        assert!(CpuWeight::BACKGROUND < CpuWeight::INTERACTIVE);
        assert!(CpuWeight::INTERACTIVE < CpuWeight::GATE);
    }

    #[test]
    fn priority_mapping_covers_all_tier_boundaries() {
        assert_eq!(CpuWeight::from_priority(i32::MAX), CpuWeight::GATE);
        assert_eq!(CpuWeight::from_priority(201), CpuWeight::GATE);
        assert_eq!(CpuWeight::from_priority(200), CpuWeight::GATE);

        assert_eq!(CpuWeight::from_priority(199), CpuWeight::INTERACTIVE);
        assert_eq!(CpuWeight::from_priority(100), CpuWeight::INTERACTIVE);

        assert_eq!(CpuWeight::from_priority(99), CpuWeight::BACKGROUND);
        assert_eq!(CpuWeight::from_priority(0), CpuWeight::BACKGROUND);
        assert_eq!(CpuWeight::from_priority(-1), CpuWeight::BACKGROUND);
        assert_eq!(CpuWeight::from_priority(i32::MIN), CpuWeight::BACKGROUND);

        assert_eq!(
            CpuWeight::for_priority_and_millicores(200, 0),
            CpuWeight::GATE
        );
        assert_eq!(
            CpuWeight::for_priority_and_millicores(200, 1_000),
            CpuWeight::GATE
        );
        assert_eq!(
            CpuWeight::for_priority_and_millicores(200, 1_500).get(),
            2_000
        );
        assert_eq!(
            CpuWeight::for_priority_and_millicores(200, 12_000),
            CpuWeight::MAX
        );
        assert_eq!(
            CpuWeight::for_priority_and_millicores(100, 12_000).get(),
            1_200
        );
        assert_eq!(CpuWeight::for_priority_and_millicores(0, 12_000).get(), 240);
    }

    #[test]
    fn display_and_from_str_round_trip() {
        assert_eq!(CpuWeight::GATE.to_string(), "1000");
        assert_eq!("1000".parse::<CpuWeight>(), Ok(CpuWeight::GATE));
        assert_eq!("1".parse::<CpuWeight>(), Ok(CpuWeight::MIN));
        assert_eq!("10000".parse::<CpuWeight>(), Ok(CpuWeight::MAX));
        assert_eq!(
            "0".parse::<CpuWeight>(),
            Err(CpuWeightError::OutOfRange { value: 0 })
        );
        assert_eq!(
            "10001".parse::<CpuWeight>(),
            Err(CpuWeightError::OutOfRange { value: 10_001 })
        );
        assert!(matches!(
            "".parse::<CpuWeight>(),
            Err(CpuWeightError::InvalidSyntax { .. })
        ));
        assert!(matches!(
            "-5".parse::<CpuWeight>(),
            Err(CpuWeightError::InvalidSyntax { .. })
        ));
    }

    #[test]
    fn serde_round_trips_json_and_toml() {
        let holder = Holder {
            cpu_weight: CpuWeight::INTERACTIVE,
        };
        let json = serde_json::to_string(&holder).expect("serialize json");
        assert_eq!(json, r#"{"cpu_weight":100}"#);
        assert_eq!(
            serde_json::from_str::<Holder>(&json).expect("deserialize json"),
            holder
        );

        let toml_text = toml::to_string(&holder).expect("serialize toml");
        assert_eq!(toml_text, "cpu_weight = 100\n");
        assert_eq!(
            toml::from_str::<Holder>(&toml_text).expect("deserialize toml"),
            holder
        );

        assert!(serde_json::from_str::<CpuWeight>("0").is_err());
        assert!(serde_json::from_str::<CpuWeight>("10001").is_err());
        assert!(serde_json::from_str::<CpuWeight>("-10").is_err());
        assert!(serde_json::from_str::<CpuWeight>("\"100\"").is_err());
    }
}
