//! Whole-unit durations constrained to a fixed inclusive range.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::{QuantityError, duration};

/// The number of nanoseconds in one millisecond.
pub const NANOS_PER_MILLI: u64 = 1_000_000;
/// The number of nanoseconds in one second.
pub const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// A duration that is a whole number of `UNIT_NANOS` units within `MIN_UNITS..=MAX_UNITS`.
///
/// Text uses [`duration`], whose precision is milliseconds. Units finer than a millisecond
/// therefore cannot always round-trip through text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedDuration<const UNIT_NANOS: u64, const MIN_UNITS: u64, const MAX_UNITS: u64>(u64);

impl<const UNIT_NANOS: u64, const MIN_UNITS: u64, const MAX_UNITS: u64>
    BoundedDuration<UNIT_NANOS, MIN_UNITS, MAX_UNITS>
{
    const VALID: () = {
        assert!(UNIT_NANOS > 0, "unit must be nonzero");
        assert!(MIN_UNITS >= 1 && MIN_UNITS <= MAX_UNITS, "invalid bounds");
        assert!(
            MAX_UNITS <= u64::MAX / UNIT_NANOS,
            "maximum duration overflows"
        );
    };

    /// The shortest permitted duration.
    pub const MIN: Self = Self::from_units(MIN_UNITS);
    /// The longest permitted duration.
    pub const MAX: Self = Self::from_units(MAX_UNITS);

    /// Constructs a duration from a whole number of units.
    ///
    /// # Panics
    ///
    /// Panics if `units` is outside the bounds, or the type parameters are invalid.
    #[must_use]
    pub const fn from_units(units: u64) -> Self {
        let () = Self::VALID;
        assert!(
            units >= MIN_UNITS && units <= MAX_UNITS,
            "duration outside bounds"
        );
        Self(units)
    }

    /// Validates a duration against the unit and bounds.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration is not a whole number of units or is outside the bounds.
    pub fn try_new(value: Duration) -> Result<Self, BoundedDurationError> {
        let () = Self::VALID;
        if !value.as_nanos().is_multiple_of(u128::from(UNIT_NANOS)) {
            return Err(BoundedDurationError::NotWhole {
                unit: Duration::from_nanos(UNIT_NANOS),
            });
        }
        let units = u64::try_from(value.as_nanos() / u128::from(UNIT_NANOS)).map_err(|_| {
            BoundedDurationError::TooLong {
                max: Self::MAX.get(),
            }
        })?;
        if units < MIN_UNITS {
            return Err(BoundedDurationError::TooShort {
                min: Self::MIN.get(),
            });
        }
        if units > MAX_UNITS {
            return Err(BoundedDurationError::TooLong {
                max: Self::MAX.get(),
            });
        }
        Ok(Self(units))
    }

    /// Returns the underlying duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        Duration::from_nanos(self.0 * UNIT_NANOS)
    }

    /// Returns the duration in whole units.
    #[must_use]
    pub const fn as_units(self) -> u64 {
        self.0
    }
}

/// A workload token lifetime in whole seconds from one minute to one hour inclusive.
pub type TokenTtl = BoundedDuration<NANOS_PER_SECOND, 60, 3600>;

/// The absolute lifetime of a browser session: whole seconds from 1 minute to 30 days.
pub type SessionMaxAge = BoundedDuration<NANOS_PER_SECOND, 60, 2_592_000>;

/// The lifetime of a device authorization (ADR-37): whole seconds from one to thirty minutes.
pub type DeviceCodeTtl = BoundedDuration<NANOS_PER_SECOND, 60, 1800>;

/// The device authorization polling interval (ADR-37): whole seconds from one second to one hour.
pub type DevicePollInterval = BoundedDuration<NANOS_PER_SECOND, 1, 3600>;

/// Errors from parsing or validating a bounded duration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BoundedDurationError {
    /// The duration is not a whole number of units.
    #[error("must be a whole number of {unit}", unit = duration::display(*.unit))]
    NotWhole {
        /// The required unit.
        unit: Duration,
    },
    /// The duration is below the minimum.
    #[error("must be at least {min}", min = duration::display(*.min))]
    TooShort {
        /// The minimum duration.
        min: Duration,
    },
    /// The duration exceeds the maximum.
    #[error("must be at most {max}", max = duration::display(*.max))]
    TooLong {
        /// The maximum duration.
        max: Duration,
    },
    /// The duration text could not be parsed.
    #[error(transparent)]
    Text(#[from] QuantityError),
}

impl<const U: u64, const MIN: u64, const MAX: u64> FromStr for BoundedDuration<U, MIN, MAX> {
    type Err = BoundedDurationError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::try_new(duration::parse(input)?)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> fmt::Display for BoundedDuration<U, MIN, MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&duration::display(self.get()), f)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> TryFrom<Duration>
    for BoundedDuration<U, MIN, MAX>
{
    type Error = BoundedDurationError;
    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> From<BoundedDuration<U, MIN, MAX>> for Duration {
    fn from(value: BoundedDuration<U, MIN, MAX>) -> Self {
        value.get()
    }
}

impl_parsed_serde!(
    [const UNIT_NANOS: u64, const MIN_UNITS: u64, const MAX_UNITS: u64]
    BoundedDuration<UNIT_NANOS, MIN_UNITS, MAX_UNITS>,
    "a bounded duration"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_and_whole_units_are_enforced() {
        assert!(matches!(
            TokenTtl::try_new(Duration::from_secs(59)),
            Err(BoundedDurationError::TooShort { .. })
        ));
        assert_eq!(TokenTtl::from_units(60), TokenTtl::MIN);
        assert_eq!(
            TokenTtl::try_new(Duration::from_secs(60)),
            Ok(TokenTtl::MIN)
        );
        assert_eq!(TokenTtl::from_units(3600), TokenTtl::MAX);
        assert_eq!(
            TokenTtl::try_new(Duration::from_secs(3600)),
            Ok(TokenTtl::MAX)
        );
        assert!(matches!(
            TokenTtl::try_new(Duration::from_secs(3601)),
            Err(BoundedDurationError::TooLong { .. })
        ));
        type MillisBound = BoundedDuration<NANOS_PER_MILLI, 1, 1000>;
        assert!(matches!(
            MillisBound::try_new(Duration::from_secs(u64::MAX)),
            Err(BoundedDurationError::TooLong { .. })
        ));
        assert!(matches!(
            TokenTtl::try_new(Duration::from_millis(60_001)),
            Err(BoundedDurationError::NotWhole { .. })
        ));
        assert_eq!(TokenTtl::MIN.as_units(), 60);
    }

    #[test]
    #[should_panic(expected = "duration outside bounds")]
    fn from_units_rejects_below_minimum() {
        let _ = TokenTtl::from_units(59);
    }

    #[test]
    #[should_panic(expected = "duration outside bounds")]
    fn from_units_rejects_above_maximum() {
        let _ = TokenTtl::from_units(3601);
    }

    #[test]
    fn text_and_serde_round_trip_and_reject_invalid_text() -> Result<(), Box<dyn std::error::Error>>
    {
        let value: TokenTtl = "10m".parse()?;
        assert_eq!(value.to_string(), "10m");
        let json = serde_json::to_string(&value)?;
        assert_eq!(serde_json::from_str::<TokenTtl>(&json)?, value);
        assert!(serde_json::from_str::<TokenTtl>("\"30s\"").is_err());
        assert!(serde_json::from_str::<TokenTtl>("\"invalid\"").is_err());
        Ok(())
    }

    #[test]
    fn millisecond_units_work() {
        type MillisBound = BoundedDuration<NANOS_PER_MILLI, 1, 1000>;
        assert_eq!(
            MillisBound::from_units(500).get(),
            Duration::from_millis(500)
        );
        assert_eq!(
            MillisBound::try_new(Duration::from_millis(1000)),
            Ok(MillisBound::MAX)
        );
        assert!(matches!(
            MillisBound::try_new(Duration::from_nanos(1)),
            Err(BoundedDurationError::NotWhole { .. })
        ));
    }
}
