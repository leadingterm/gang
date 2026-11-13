//! Time as the deploy decisions see it: `now` is always an input, in whole seconds since the Unix
//! epoch; the manifest writes UTC times as `YYYY-MM-DDTHH:MM:SSZ`.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::Duration;

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::errors::{DeployError, Expected, Result};
use crate::newtype::decimal;

/// The manifest's whole-second UTC timestamp format.
const FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");

/// An instant in whole seconds since the Unix epoch, as `date -u +%s` prints it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixTime(i64);

impl UnixTime {
    /// The instant `seconds` after the Unix epoch.
    #[must_use]
    pub const fn from_seconds(seconds: i64) -> Self {
        Self(seconds)
    }

    /// Seconds since the Unix epoch.
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0
    }

    /// The instant `max_age` later, or `None` when it does not fit.
    #[must_use]
    pub fn checked_add(self, max_age: MaxAge) -> Option<Self> {
        self.0.checked_add(max_age.as_secs()).map(Self)
    }
}

impl FromStr for UnixTime {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        let seconds = decimal(text)?;
        i64::try_from(seconds)
            .map(Self)
            .map_err(|_| DeployError::Invalid {
                text: text.to_owned(),
                expected: Expected::Number,
            })
    }
}

impl fmt::Display for UnixTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The longest time a stored plan stays usable: a positive whole number of seconds, at most `u32::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaxAge(NonZeroU32);

impl MaxAge {
    /// A maximum age of `seconds`.
    #[must_use]
    pub const fn from_seconds(seconds: NonZeroU32) -> Self {
        Self(seconds)
    }

    /// The maximum age in seconds.
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0.get() as i64
    }
}

impl TryFrom<Duration> for MaxAge {
    type Error = DeployError;

    fn try_from(duration: Duration) -> Result<Self> {
        if duration.subsec_nanos() != 0 {
            return Err(DeployError::MaxAge(duration));
        }
        u32::try_from(duration.as_secs())
            .ok()
            .and_then(NonZeroU32::new)
            .map(Self)
            .ok_or(DeployError::MaxAge(duration))
    }
}

impl fmt::Display for MaxAge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A whole-second UTC time as the manifest writes it: `YYYY-MM-DDTHH:MM:SSZ`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(OffsetDateTime);

impl Timestamp {
    /// The time `instant`, when its year has four digits.
    ///
    /// # Errors
    ///
    /// [`DeployError::TimeRange`] when the year of `instant` is outside 0 to 9999.
    pub fn from_unix(instant: UnixTime) -> Result<Self> {
        let out_of_range = || DeployError::TimeRange {
            seconds: instant.as_secs(),
        };
        let time =
            OffsetDateTime::from_unix_timestamp(instant.as_secs()).map_err(|_| out_of_range())?;
        if (0..=9999).contains(&time.year()) {
            Ok(Self(time))
        } else {
            Err(out_of_range())
        }
    }

    /// The time in seconds since the Unix epoch.
    #[must_use]
    pub fn unix(self) -> UnixTime {
        UnixTime(self.0.unix_timestamp())
    }
}

impl FromStr for Timestamp {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        PrimitiveDateTime::parse(text, FORMAT)
            .ok()
            .filter(|time| text.len() == 20 && (0..=9999).contains(&time.year()))
            .map(PrimitiveDateTime::assume_utc)
            .map(Self)
            .ok_or_else(|| DeployError::Invalid {
                text: text.to_owned(),
                expected: Expected::Timestamp,
            })
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = self.0.format(FORMAT).map_err(|_| fmt::Error)?;
        f.write_str(&formatted)
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <String as serde::Deserialize>::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The age at `now` of an object last modified at the RFC 3339 time `last_modified`, in whole
/// seconds: the fraction of `last_modified` is dropped, and any UTC offset is honoured.
///
/// # Errors
///
/// [`DeployError::ObjectLastModified`] when `last_modified` is not an RFC 3339 time.
/// [`DeployError::TimeRange`] when the age cannot be represented in whole seconds.
pub fn object_age(last_modified: &str, now: UnixTime) -> Result<i64> {
    let modified = OffsetDateTime::parse(last_modified, &Rfc3339)
        .map_err(|_| DeployError::ObjectLastModified)?;
    now.as_secs()
        .checked_sub(modified.unix_timestamp())
        .ok_or(DeployError::TimeRange {
            seconds: now.as_secs(),
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::errors::DeployError;

    use super::{MaxAge, Timestamp, UnixTime, object_age};

    /// 2026-01-02T00:00:00Z.
    const JAN_2: UnixTime = UnixTime::from_seconds(1_767_312_000);

    #[test]
    fn object_age_of_an_object_modified_now_is_0() {
        assert_eq!(object_age("2026-01-02T00:00:00Z", JAN_2).unwrap(), 0);
    }

    #[test]
    fn object_age_of_an_object_modified_a_day_earlier_is_86400() {
        assert_eq!(object_age("2026-01-01T00:00:00Z", JAN_2).unwrap(), 86_400);
    }

    #[test]
    fn object_age_rejects_an_invalid_timestamp() {
        let error = object_age("not-a-time", JAN_2).unwrap_err();
        assert_eq!(error.to_string(), "plan object has no usable LastModified");
    }

    #[test]
    fn object_age_rejects_an_age_that_overflows() {
        assert!(matches!(
            object_age("0000-01-01T00:00:00Z", UnixTime::from_seconds(i64::MAX)),
            Err(crate::errors::DeployError::TimeRange { .. })
        ));
    }

    #[test]
    fn object_age_accepts_plus_00_00_in_place_of_z() {
        assert_eq!(
            object_age("2026-01-01T00:00:00+00:00", JAN_2).unwrap(),
            86_400
        );
    }

    #[test]
    fn object_age_drops_fractional_seconds() {
        assert_eq!(object_age("2026-01-01T23:59:59.500Z", JAN_2).unwrap(), 1);
    }

    #[test]
    fn object_age_converts_another_offset_to_utc() {
        assert_eq!(
            object_age("2026-01-01T01:00:00+01:00", JAN_2).unwrap(),
            86_400
        );
    }

    #[test]
    fn timestamp_round_trips_the_manifest_form() {
        let timestamp: Timestamp = "2026-01-02T00:00:00Z".parse().unwrap();
        assert_eq!(timestamp.unix(), JAN_2);
        assert_eq!(timestamp.to_string(), "2026-01-02T00:00:00Z");
        assert_eq!(Timestamp::from_unix(JAN_2).unwrap(), timestamp);
    }

    #[test]
    fn timestamp_rejects_offsets_fractions_and_impossible_dates() {
        for text in [
            "2026-01-02T00:00:00+00:00",
            "2026-01-02T00:00:00.5Z",
            "2026-13-02T00:00:00Z",
            "2026-02-30T00:00:00Z",
            "2026-01-02 00:00:00Z",
            "",
        ] {
            let error = text.parse::<Timestamp>().unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("`{text}` is not a YYYY-MM-DDTHH:MM:SSZ timestamp")
            );
        }
    }

    #[test]
    fn max_age_accepts_positive_whole_seconds_within_u32() {
        let max_age = MaxAge::try_from(Duration::from_secs(86_400)).expect("positive max age");
        assert_eq!(max_age.as_secs(), 86_400);
        assert!(MaxAge::try_from(Duration::from_secs(u64::from(u32::MAX))).is_ok());
    }

    #[test]
    fn max_age_rejects_zero_fractional_seconds_and_overflow() {
        for duration in [
            Duration::ZERO,
            Duration::from_millis(1_500),
            Duration::from_secs(u64::from(u32::MAX) + 1),
        ] {
            assert!(matches!(
                MaxAge::try_from(duration),
                Err(DeployError::MaxAge(_))
            ));
        }
    }

    #[test]
    fn unix_time_parses_digits_only() {
        assert_eq!("1767312000".parse::<UnixTime>().unwrap(), JAN_2);
        assert!("-1".parse::<UnixTime>().is_err());
    }
}
