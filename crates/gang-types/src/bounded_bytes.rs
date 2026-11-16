//! Whole-unit byte sizes constrained to a fixed inclusive range.

use std::fmt;
use std::str::FromStr;

use crate::{Bytes, QuantityError};

/// A size that is a whole number of `UNIT_BYTES` units within `MIN_UNITS..=MAX_UNITS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedBytes<const UNIT_BYTES: u64, const MIN_UNITS: u64, const MAX_UNITS: u64>(u64);

impl<const UNIT_BYTES: u64, const MIN_UNITS: u64, const MAX_UNITS: u64>
    BoundedBytes<UNIT_BYTES, MIN_UNITS, MAX_UNITS>
{
    const VALID: () = {
        assert!(UNIT_BYTES > 0, "unit must be nonzero");
        assert!(MIN_UNITS >= 1 && MIN_UNITS <= MAX_UNITS, "invalid bounds");
        assert!(MAX_UNITS <= u64::MAX / UNIT_BYTES, "maximum size overflows");
    };

    /// The smallest permitted size.
    pub const MIN: Self = Self::from_units(MIN_UNITS);
    /// The largest permitted size.
    pub const MAX: Self = Self::from_units(MAX_UNITS);

    /// Constructs a size from a whole number of units.
    ///
    /// # Panics
    ///
    /// Panics if `units` is outside the bounds, or the type parameters are invalid.
    #[must_use]
    pub const fn from_units(units: u64) -> Self {
        let () = Self::VALID;
        assert!(
            units >= MIN_UNITS && units <= MAX_UNITS,
            "size outside bounds"
        );
        Self(units)
    }

    /// Validates a size against the unit and bounds.
    ///
    /// # Errors
    ///
    /// Returns an error if the size is not a whole number of units or is outside the bounds.
    pub fn try_new(value: Bytes) -> Result<Self, BoundedBytesError> {
        let () = Self::VALID;
        if !value.get().is_multiple_of(UNIT_BYTES) {
            return Err(BoundedBytesError::NotWhole {
                unit: Bytes::new(UNIT_BYTES),
            });
        }
        let units = value.get() / UNIT_BYTES;
        if units < MIN_UNITS {
            return Err(BoundedBytesError::TooSmall {
                min: Self::MIN.get(),
            });
        }
        if units > MAX_UNITS {
            return Err(BoundedBytesError::TooLarge {
                max: Self::MAX.get(),
            });
        }
        Ok(Self(units))
    }

    /// Returns the underlying size in bytes.
    #[must_use]
    pub const fn get(self) -> Bytes {
        Bytes::new(self.0 * UNIT_BYTES)
    }

    /// Returns the size in whole units.
    #[must_use]
    pub const fn as_units(self) -> u64 {
        self.0
    }
}

/// Errors from parsing or validating a bounded size.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BoundedBytesError {
    /// The size is not a whole number of units.
    #[error("must be a whole number of {unit}")]
    NotWhole {
        /// The required unit.
        unit: Bytes,
    },
    /// The size is below the minimum.
    #[error("must be at least {min}")]
    TooSmall {
        /// The minimum size.
        min: Bytes,
    },
    /// The size exceeds the maximum.
    #[error("must be at most {max}")]
    TooLarge {
        /// The maximum size.
        max: Bytes,
    },
    /// The size text could not be parsed.
    #[error(transparent)]
    Invalid(#[from] QuantityError),
}

impl<const U: u64, const MIN: u64, const MAX: u64> FromStr for BoundedBytes<U, MIN, MAX> {
    type Err = BoundedBytesError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::try_new(input.parse()?)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> fmt::Display for BoundedBytes<U, MIN, MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.get(), f)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> TryFrom<Bytes> for BoundedBytes<U, MIN, MAX> {
    type Error = BoundedBytesError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl<const U: u64, const MIN: u64, const MAX: u64> From<BoundedBytes<U, MIN, MAX>> for Bytes {
    fn from(value: BoundedBytes<U, MIN, MAX>) -> Self {
        value.get()
    }
}

impl_parsed_serde!(
    [const UNIT_BYTES: u64, const MIN_UNITS: u64, const MAX_UNITS: u64]
    BoundedBytes<UNIT_BYTES, MIN_UNITS, MAX_UNITS>,
    "a bounded size"
);

#[cfg(test)]
mod tests {
    use super::*;

    type Kib = BoundedBytes<1024, 1, 4>;
    type Window = BoundedBytes<1, 65_535, 2_147_483_647>;

    #[test]
    fn bounds_and_whole_units_are_enforced() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!("1KiB".parse::<Kib>()?.as_units(), 1);
        assert_eq!("4KiB".parse::<Kib>()?.as_units(), 4);
        assert!(matches!(
            "1500B".parse::<Kib>(),
            Err(BoundedBytesError::NotWhole { unit }) if unit == Bytes::new(1024)
        ));
        assert!(matches!(
            "5KiB".parse::<Kib>(),
            Err(BoundedBytesError::TooLarge { max }) if max == Bytes::new(4096)
        ));
        assert!(matches!(
            "0B".parse::<Kib>(),
            Err(BoundedBytesError::TooSmall { min }) if min == Bytes::new(1024)
        ));
        assert!(matches!(
            "65534B".parse::<Window>(),
            Err(BoundedBytesError::TooSmall { min }) if min == Bytes::new(65_535)
        ));
        assert_eq!(
            "2147483647B".parse::<Window>()?.get(),
            Bytes::new(2_147_483_647)
        );
        assert!(matches!(
            "2GiB".parse::<Window>(),
            Err(BoundedBytesError::TooLarge { max }) if max == Bytes::new(2_147_483_647)
        ));
        Ok(())
    }

    #[test]
    #[should_panic(expected = "size outside bounds")]
    fn from_units_rejects_below_minimum() {
        let _ = Kib::from_units(0);
    }

    #[test]
    #[should_panic(expected = "size outside bounds")]
    fn from_units_rejects_above_maximum() {
        let _ = Kib::from_units(5);
    }

    #[test]
    fn text_and_serde_round_trip_and_reject_invalid_text() -> Result<(), Box<dyn std::error::Error>>
    {
        let value = Kib::from_units(4);
        assert_eq!(value.to_string(), Bytes::new(4096).to_string());
        let json = serde_json::to_string(&value)?;
        assert_eq!(serde_json::from_str::<Kib>(&json)?, value);
        assert!(matches!(
            "x".parse::<Kib>(),
            Err(BoundedBytesError::Invalid(_))
        ));
        assert!(serde_json::from_str::<Kib>("\"x\"").is_err());
        assert!(serde_json::from_str::<Kib>("\"5KiB\"").is_err());
        Ok(())
    }
}
