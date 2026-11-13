//! Durations that are guaranteed to be greater than zero, for settings where zero is meaningless.
//!
//! Text forms are the same as [`duration::parse`](crate::duration::parse), so `10m` and `500ms`
//! are accepted, while `0s` is rejected with [`NonZeroDurationError::Zero`]. Durations below a
//! millisecond are rejected with [`NonZeroDurationError::NotWholeMillis`].

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;

use crate::QuantityError;
use crate::bounded_duration::BoundedDuration;
use crate::duration;

/// A duration that is strictly greater than zero, a whole number of milliseconds, and at most
/// `u64::MAX` milliseconds.
///
/// Parses from `<digits><unit>` using the [`duration`] text format. Construction admits exactly
/// the durations that text can express, so [`Display`](fmt::Display) and `FromStr` round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroDuration(Duration);

/// The longest duration the text form expresses: `u64::MAX` milliseconds.
const MAX: Duration = Duration::from_millis(u64::MAX);

impl NonZeroDuration {
    /// Wraps `duration`, returning an error unless it is greater than zero, a whole number of
    /// milliseconds, and at most `u64::MAX` milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`NonZeroDurationError::Zero`], [`NonZeroDurationError::NotWholeMillis`] or
    /// [`NonZeroDurationError::TooLong`] for a duration that breaks one of those rules.
    pub const fn try_new(duration: Duration) -> Result<Self, NonZeroDurationError> {
        if duration.is_zero() {
            Err(NonZeroDurationError::Zero)
        } else if !duration.subsec_nanos().is_multiple_of(1_000_000) {
            Err(NonZeroDurationError::NotWholeMillis(duration))
        } else if MAX.checked_sub(duration).is_none() {
            Err(NonZeroDurationError::TooLong(duration))
        } else {
            Ok(Self(duration))
        }
    }

    /// Returns the duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }

    /// The duration in milliseconds, exact by construction.
    #[must_use]
    pub fn as_millis(self) -> u64 {
        self.0.as_secs() * 1000 + u64::from(self.0.subsec_millis())
    }

    /// Creates a duration of `millis` milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if `millis` is zero or longer than `u64::MAX` milliseconds. In a `const` context
    /// this is a compile error.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self::from_literal(Duration::from_millis(millis))
    }

    /// Creates a duration of `secs` seconds.
    ///
    /// # Panics
    ///
    /// Panics if `secs` is zero or longer than `u64::MAX` milliseconds. In a `const` context
    /// this is a compile error.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self::from_literal(Duration::from_secs(secs))
    }

    /// Creates a duration of `mins` minutes.
    ///
    /// # Panics
    ///
    /// Panics if `mins` is zero or longer than `u64::MAX` milliseconds. In a `const` context
    /// this is a compile error.
    #[must_use]
    pub const fn from_mins(mins: u64) -> Self {
        Self::from_literal(Duration::from_mins(mins))
    }

    /// Creates a duration of `hours` hours.
    ///
    /// # Panics
    ///
    /// Panics if `hours` is zero or longer than `u64::MAX` milliseconds. In a `const` context
    /// this is a compile error.
    #[must_use]
    pub const fn from_hours(hours: u64) -> Self {
        Self::from_literal(Duration::from_hours(hours))
    }

    /// Wraps a duration built from a whole number of milliseconds or a larger unit. The checks
    /// are spelled out rather than taken from [`Self::try_new`], because a `const` caller cannot
    /// drop its error.
    const fn from_literal(duration: Duration) -> Self {
        assert!(!duration.is_zero(), "duration must be greater than zero");
        assert!(
            MAX.checked_sub(duration).is_some(),
            "duration is longer than u64::MAX milliseconds"
        );
        Self(duration)
    }
}

impl From<NonZeroDuration> for Duration {
    fn from(value: NonZeroDuration) -> Self {
        value.0
    }
}

/// A bounded duration of at least one whole-millisecond unit is always a valid [`NonZeroDuration`];
/// a bound that could hold zero, a fraction of a millisecond, or more than `u64::MAX` milliseconds
/// fails to compile at the conversion.
impl<const UNIT_NANOS: u64, const MIN_UNITS: u64, const MAX_UNITS: u64>
    From<BoundedDuration<UNIT_NANOS, MIN_UNITS, MAX_UNITS>> for NonZeroDuration
{
    fn from(value: BoundedDuration<UNIT_NANOS, MIN_UNITS, MAX_UNITS>) -> Self {
        const {
            assert!(MIN_UNITS >= 1, "the bound admits zero");
            assert!(
                UNIT_NANOS.is_multiple_of(1_000_000),
                "the unit is not whole milliseconds"
            );
            assert!(
                (UNIT_NANOS / 1_000_000).checked_mul(MAX_UNITS).is_some(),
                "the bound exceeds u64::MAX milliseconds"
            );
        }
        Self(value.get())
    }
}

/// Errors from parsing a [`NonZeroDuration`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NonZeroDurationError {
    /// The duration is zero.
    #[error("duration must be greater than zero")]
    Zero,
    /// The duration has a part finer than a millisecond.
    #[error("duration {0:?} is not a whole number of milliseconds")]
    NotWholeMillis(Duration),
    /// The duration is longer than `u64::MAX` milliseconds.
    #[error("duration {0:?} is longer than {max} milliseconds", max = u64::MAX)]
    TooLong(Duration),
    /// The text is not a valid duration.
    #[error(transparent)]
    Invalid(#[from] QuantityError),
}

impl FromStr for NonZeroDuration {
    type Err = NonZeroDurationError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::try_new(duration::parse(input)?)
    }
}

impl fmt::Display for NonZeroDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&duration::display(self.0), f)
    }
}

impl_parsed_serde!(
    NonZeroDuration,
    "a duration greater than zero, such as \"10m\""
);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn bounded_whole_seconds_convert_to_nonzero_duration() {
        let duration = BoundedDuration::<1_000_000_000, 1, 10>::from_units(3);
        assert_eq!(
            NonZeroDuration::from(duration),
            NonZeroDuration::from_secs(3)
        );
    }

    #[test]
    fn json_deserializes_borrowed_and_owned_strings() {
        let expected = NonZeroDuration::from_mins(10);
        assert_eq!(
            serde_json::from_str::<NonZeroDuration>("\"10m\"").expect("valid duration"),
            expected
        );
        assert_eq!(
            serde_json::from_value::<NonZeroDuration>(serde_json::Value::String("10m".to_owned()))
                .expect("valid duration"),
            expected
        );
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        timeout: NonZeroDuration,
        #[serde(default)]
        grace: Option<NonZeroDuration>,
    }

    #[test]
    fn parses_nonzero_durations() {
        assert_eq!(
            "10m".parse::<NonZeroDuration>(),
            Ok(NonZeroDuration::from_mins(10))
        );
        assert_eq!(
            "500ms".parse::<NonZeroDuration>(),
            Ok(NonZeroDuration::from_millis(500))
        );
    }

    #[test]
    fn rejects_zero_durations() {
        assert_eq!(
            "0s".parse::<NonZeroDuration>(),
            Err(NonZeroDurationError::Zero)
        );
        assert_eq!(
            "0ms".parse::<NonZeroDuration>(),
            Err(NonZeroDurationError::Zero)
        );
    }

    #[test]
    fn rejects_invalid_text() {
        assert!(matches!(
            "10x".parse::<NonZeroDuration>(),
            Err(NonZeroDurationError::Invalid(
                QuantityError::UnknownUnit { .. }
            ))
        ));
        assert!(matches!(
            "".parse::<NonZeroDuration>(),
            Err(NonZeroDurationError::Invalid(QuantityError::Empty { .. }))
        ));
    }

    #[test]
    fn try_new_accepts_only_what_the_text_form_expresses() {
        assert_eq!(
            NonZeroDuration::try_new(Duration::ZERO),
            Err(NonZeroDurationError::Zero)
        );
        assert!(matches!(
            NonZeroDuration::try_new(Duration::from_micros(500)),
            Err(NonZeroDurationError::NotWholeMillis(_))
        ));
        assert!(matches!(
            NonZeroDuration::try_new(Duration::from_micros(1500)),
            Err(NonZeroDurationError::NotWholeMillis(_))
        ));
        assert!(matches!(
            NonZeroDuration::try_new(Duration::from_secs(u64::MAX)),
            Err(NonZeroDurationError::TooLong(_))
        ));
        assert_eq!(
            NonZeroDuration::try_new(Duration::from_millis(u64::MAX)).map(NonZeroDuration::get),
            Ok(Duration::from_millis(u64::MAX))
        );
        assert_eq!(
            NonZeroDuration::try_new(Duration::from_secs(1)).map(NonZeroDuration::get),
            Ok(Duration::from_secs(1))
        );
    }

    #[test]
    fn every_constructed_duration_round_trips_through_text() {
        let values = [
            NonZeroDuration::from_millis(1),
            NonZeroDuration::from_millis(999),
            NonZeroDuration::from_millis(1001),
            NonZeroDuration::from_secs(90),
            NonZeroDuration::from_hours(168),
            NonZeroDuration::from_millis(u64::MAX),
        ];
        for value in values {
            assert_eq!(value.to_string().parse::<NonZeroDuration>(), Ok(value));
        }
    }

    #[test]
    fn as_millis_is_exact() {
        assert_eq!(NonZeroDuration::from_millis(1500).as_millis(), 1500);
        assert_eq!(NonZeroDuration::from_millis(u64::MAX).as_millis(), u64::MAX);
    }

    #[test]
    #[should_panic(expected = "duration must be greater than zero")]
    fn literal_constructor_panics_on_zero() {
        let _ = NonZeroDuration::from_millis(0);
    }

    #[test]
    #[should_panic(expected = "longer than")]
    fn literal_constructor_panics_beyond_the_text_range() {
        let _ = NonZeroDuration::from_secs(u64::MAX);
    }

    #[test]
    fn converts_into_duration() {
        let duration: Duration = NonZeroDuration::from_mins(2).into();
        assert_eq!(duration, Duration::from_mins(2));
    }

    #[test]
    fn constructors_build_expected_durations() {
        assert_eq!(
            NonZeroDuration::from_secs(90).get(),
            Duration::from_secs(90)
        );
        assert_eq!(
            NonZeroDuration::from_hours(2).get(),
            Duration::from_hours(2)
        );
    }

    #[test]
    fn display_round_trips() {
        let value = NonZeroDuration::from_mins(10);
        assert_eq!(value.to_string(), "10m");
        assert_eq!(value.to_string().parse::<NonZeroDuration>(), Ok(value));
    }

    #[test]
    fn serializes_as_string() {
        let json = serde_json::to_string(&NonZeroDuration::from_mins(10))
            .expect("serializing a duration string cannot fail");
        assert_eq!(json, "\"10m\"");
        let back: NonZeroDuration =
            serde_json::from_str(&json).expect("the serialized form must parse back");
        assert_eq!(back, NonZeroDuration::from_mins(10));
    }

    #[test]
    fn toml_absent_option_is_none() {
        let holder: Holder = toml::from_str(r#"timeout = "10m""#).expect("valid toml");
        assert_eq!(
            holder,
            Holder {
                timeout: NonZeroDuration::from_mins(10),
                grace: None,
            }
        );
    }

    #[test]
    fn toml_present_option_parses() {
        let holder: Holder =
            toml::from_str("timeout = \"10m\"\ngrace = \"500ms\"\n").expect("valid toml");
        assert_eq!(holder.grace, Some(NonZeroDuration::from_millis(500)));
    }

    #[test]
    fn toml_rejects_zero_option() {
        assert!(toml::from_str::<Holder>("timeout = \"10m\"\ngrace = \"0s\"\n").is_err());
    }

    #[test]
    fn toml_rejects_non_string_timeout() {
        assert!(toml::from_str::<Holder>("timeout = 10\n").is_err());
    }
}
