//! Whole percentages from 1 to 100, such as `50` in a configuration file.

use std::fmt;
use std::num::NonZeroU8;
use std::str::FromStr;
use std::time::Duration;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Bytes;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// A whole percentage in the range 1 to 100 inclusive.
///
/// Parses from plain decimal digits such as `50`, with no `%` sign, whitespace or fraction.
/// Serializes as an integer, so it reads naturally as a TOML or JSON number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Percent(NonZeroU8);

impl Percent {
    /// Creates a percentage, returning `None` unless `value` is from 1 to 100.
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        match NonZeroU8::new(value) {
            Some(nonzero) if value <= 100 => Some(Self(nonzero)),
            _ => None,
        }
    }

    /// Creates a percentage from a constant or default value.
    ///
    /// # Panics
    ///
    /// Panics if `value` is not from 1 to 100. Use [`Percent::new`] for untrusted input.
    #[must_use]
    pub const fn from_percent(value: u8) -> Self {
        assert!(value >= 1 && value <= 100, "a percentage from 1 to 100");
        // The assert above makes the fallback unreachable.
        match NonZeroU8::new(value) {
            Some(value) => Self(value),
            None => Self(NonZeroU8::MIN),
        }
    }

    /// Returns the percentage as a number from 1 to 100.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0.get()
    }

    /// Returns this percentage of `whole`, rounded down.
    ///
    /// The product is computed in `u128`, so this never overflows.
    #[must_use]
    pub fn of_u64(self, whole: u64) -> u64 {
        let scaled = u128::from(whole) * u128::from(self.0.get()) / 100;
        u64::try_from(scaled).unwrap_or(u64::MAX)
    }

    /// Returns this percentage of `size`, rounded down.
    #[must_use]
    pub fn of_bytes(self, size: Bytes) -> Bytes {
        Bytes::new(self.of_u64(size.get()))
    }

    /// Returns this percentage of `duration`, rounded down to whole nanoseconds.
    ///
    /// The result saturates at [`Duration::MAX`].
    #[must_use]
    pub fn of_duration(self, duration: Duration) -> Duration {
        let nanos = duration.as_nanos() * u128::from(self.0.get()) / 100;
        match (
            u64::try_from(nanos / NANOS_PER_SECOND),
            u32::try_from(nanos % NANOS_PER_SECOND),
        ) {
            (Ok(seconds), Ok(subsec)) => Duration::new(seconds, subsec),
            _ => Duration::MAX,
        }
    }
}

/// An error parsing a [`Percent`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PercentError {
    /// The input is empty or contains something other than decimal digits.
    #[error("percentage `{input}` is not a whole number")]
    NotANumber {
        /// The rejected input.
        input: String,
    },
    /// The value is zero or greater than 100. Values too large for `u64` report `u64::MAX`.
    #[error("percentage {value} is out of range, expected 1 to 100")]
    OutOfRange {
        /// The parsed value.
        value: u64,
    },
}

impl FromStr for Percent {
    type Err = PercentError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PercentError::NotANumber {
                input: input.to_owned(),
            });
        }
        let value = input.bytes().fold(0_u64, |accumulated, byte| {
            accumulated
                .saturating_mul(10)
                .saturating_add(u64::from(byte - b'0'))
        });
        u8::try_from(value)
            .ok()
            .and_then(Self::new)
            .ok_or(PercentError::OutOfRange { value })
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Percent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.get())
    }
}

impl<'de> Deserialize<'de> for Percent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PercentVisitor)
    }
}

struct PercentVisitor;

impl Visitor<'_> for PercentVisitor {
    type Value = Percent;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a percentage from 1 to 100")
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        u8::try_from(value)
            .ok()
            .and_then(Percent::new)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(value), &self))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        u8::try_from(value)
            .ok()
            .and_then(Percent::new)
            .ok_or_else(|| E::invalid_value(Unexpected::Signed(value), &self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        percent: Percent,
    }

    fn percent(value: u8) -> Percent {
        Percent::new(value).expect("test percentage is in range")
    }

    #[test]
    fn accepts_only_one_to_one_hundred() {
        assert_eq!(Percent::new(0), None);
        assert_eq!(Percent::new(1).map(Percent::get), Some(1));
        assert_eq!(Percent::new(100).map(Percent::get), Some(100));
        assert_eq!(Percent::new(101), None);
        assert_eq!(Percent::new(u8::MAX), None);
    }

    #[test]
    #[should_panic(expected = "a percentage from 1 to 100")]
    fn from_percent_panics_on_zero() {
        let _ = Percent::from_percent(0);
    }

    #[test]
    #[should_panic(expected = "a percentage from 1 to 100")]
    fn from_percent_panics_above_hundred() {
        let _ = Percent::from_percent(101);
    }

    #[test]
    fn from_percent_matches_new_in_range() {
        assert_eq!(Percent::from_percent(100), percent(100));
        assert_eq!(Percent::from_percent(1), percent(1));
    }

    #[test]
    fn of_u64_rounds_down() {
        assert_eq!(percent(25).of_u64(7), 1);
        assert_eq!(percent(50).of_u64(7), 3);
        assert_eq!(percent(100).of_u64(7), 7);
        assert_eq!(percent(1).of_u64(99), 0);
        assert_eq!(percent(1).of_u64(100), 1);
        assert_eq!(percent(33).of_u64(0), 0);
    }

    #[test]
    fn of_u64_does_not_overflow() {
        assert_eq!(percent(100).of_u64(u64::MAX), u64::MAX);
        assert_eq!(percent(99).of_u64(u64::MAX), 18_262_276_632_972_456_098);
        assert_eq!(percent(1).of_u64(u64::MAX), u64::MAX / 100);
    }

    #[test]
    fn of_bytes_rounds_down() {
        let size = Bytes::new(29 * (1 << 30));
        assert_eq!(percent(50).of_bytes(size), Bytes::new(29 * (1 << 29)));
        assert_eq!(percent(10).of_bytes(Bytes::new(15)), Bytes::new(1));
        assert_eq!(
            percent(100).of_bytes(Bytes::new(u64::MAX)),
            Bytes::new(u64::MAX)
        );
    }

    #[test]
    fn of_duration_rounds_down_in_nanoseconds() {
        assert_eq!(
            percent(25).of_duration(Duration::from_secs(10)),
            Duration::from_millis(2_500)
        );
        assert_eq!(
            percent(50).of_duration(Duration::from_nanos(7)),
            Duration::from_nanos(3)
        );
        assert_eq!(
            percent(100).of_duration(Duration::from_nanos(7)),
            Duration::from_nanos(7)
        );
        assert_eq!(
            percent(1).of_duration(Duration::from_nanos(99)),
            Duration::ZERO
        );
    }

    #[test]
    fn of_duration_saturates_at_max() {
        assert_eq!(percent(100).of_duration(Duration::MAX), Duration::MAX);
        assert_eq!(
            percent(50).of_duration(Duration::MAX).as_nanos(),
            Duration::MAX.as_nanos() / 2
        );
    }

    #[test]
    fn parses_plain_digits() {
        assert_eq!("1".parse::<Percent>(), Ok(percent(1)));
        assert_eq!("050".parse::<Percent>(), Ok(percent(50)));
        assert_eq!("100".parse::<Percent>(), Ok(percent(100)));
    }

    #[test]
    fn rejects_non_numbers() {
        for input in ["", "%", "50%", " 50", "+5", "-5", "5.0", "fifty"] {
            assert!(
                matches!(
                    input.parse::<Percent>(),
                    Err(PercentError::NotANumber { .. })
                ),
                "{input:?} should be rejected as not a number"
            );
        }
    }

    #[test]
    fn rejects_out_of_range_values() {
        assert_eq!(
            "0".parse::<Percent>(),
            Err(PercentError::OutOfRange { value: 0 })
        );
        assert_eq!(
            "101".parse::<Percent>(),
            Err(PercentError::OutOfRange { value: 101 })
        );
        assert_eq!(
            "99999999999999999999999".parse::<Percent>(),
            Err(PercentError::OutOfRange { value: u64::MAX })
        );
    }

    #[test]
    fn display_round_trips_through_from_str() {
        for value in 1..=100 {
            let original = percent(value);
            let text = original.to_string();
            assert_eq!(text, value.to_string());
            assert_eq!(text.parse::<Percent>(), Ok(original));
        }
    }

    #[test]
    fn serializes_as_integer() {
        assert_eq!(
            serde_json::to_string(&percent(50)).expect("serializes"),
            "50"
        );
    }

    #[test]
    fn json_round_trips() {
        let json = serde_json::to_string(&percent(73)).expect("serializes");
        let parsed: Percent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(parsed, percent(73));
    }

    #[test]
    fn toml_field_parses() {
        let holder: Holder = toml::from_str("percent = 50").expect("parses");
        assert_eq!(
            holder,
            Holder {
                percent: percent(50)
            }
        );
        let holder: Holder = toml::from_str("percent = 100").expect("parses");
        assert_eq!(holder.percent.get(), 100);
    }

    #[test]
    fn toml_field_rejects_zero_and_above_hundred() {
        assert!(toml::from_str::<Holder>("percent = 0").is_err());
        assert!(toml::from_str::<Holder>("percent = 101").is_err());
        assert!(toml::from_str::<Holder>("percent = -1").is_err());
        assert!(toml::from_str::<Holder>("percent = \"50\"").is_err());
    }
}
