use std::fmt;
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH as SYSTEM_UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::Rfc3339Error;

/// The largest millisecond count whose RFC 3339 form has a four-digit year.
const MAX_RFC3339_MILLIS: u64 = 253_402_300_799_999;

/// The RFC 3339 text of `MAX_RFC3339_MILLIS`.
const MAX_RFC3339_TEXT: &str = "9999-12-31T23:59:59.999Z";

/// A wall-clock instant, stored as milliseconds since the Unix epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The Unix epoch, `1970-01-01T00:00:00Z`.
    pub const UNIX_EPOCH: Self = Self(0);

    /// The last instant with a four-digit year, `9999-12-31T23:59:59.999Z`.
    /// Later timestamps are written clamped to it.
    pub const END_OF_TIME: Self = Self(MAX_RFC3339_MILLIS);

    /// Creates a timestamp from milliseconds since the Unix epoch.
    #[must_use]
    pub const fn from_unix_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns the milliseconds elapsed since the Unix epoch.
    #[must_use]
    pub const fn as_unix_millis(self) -> u64 {
        self.0
    }

    /// Returns the duration from `earlier` to `self`, or zero when `earlier` is later.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }

    /// Formats the timestamp as UTC RFC 3339 with exactly three fractional digits,
    /// such as `2023-11-14T22:13:20.123Z`.
    ///
    /// Equivalent to `to_string`; instants after `END_OF_TIME` saturate to it.
    #[must_use]
    pub fn to_rfc3339(self) -> String {
        self.to_string()
    }

    /// Formats the timestamp as UTC RFC 3339 truncated to whole seconds, such as
    /// `2023-11-14T22:13:20Z`.
    #[must_use]
    pub fn to_rfc3339_seconds(self) -> String {
        let text = self.to_string();
        match text.split_once('.') {
            Some((whole, _)) => format!("{whole}Z"),
            None => text,
        }
    }

    /// Parses an RFC 3339 timestamp with any UTC offset and any number of fractional
    /// digits. Sub-millisecond precision is truncated.
    ///
    /// # Errors
    ///
    /// Returns [`Rfc3339Error::Malformed`] when `text` is not an RFC 3339 timestamp, and
    /// [`Rfc3339Error::BeforeEpoch`] when the instant precedes the Unix epoch.
    pub fn from_rfc3339(text: &str) -> Result<Self, Rfc3339Error> {
        let instant = OffsetDateTime::parse(text, &Rfc3339)?;
        let millis = instant.unix_timestamp_nanos().div_euclid(1_000_000);
        u64::try_from(millis)
            .map(Self)
            .map_err(|_| Rfc3339Error::BeforeEpoch)
    }
}

impl fmt::Display for Timestamp {
    /// Writes the RFC 3339 text with three fractional digits, without allocating.
    /// Instants after `END_OF_TIME` are written as `END_OF_TIME`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.0.min(MAX_RFC3339_MILLIS);
        let Ok(instant) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        else {
            return f.write_str(MAX_RFC3339_TEXT);
        };
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            instant.year(),
            u8::from(instant.month()),
            instant.day(),
            instant.hour(),
            instant.minute(),
            instant.second(),
            instant.millisecond(),
        )
    }
}

/// Converts a duration to whole milliseconds, saturating at `u64::MAX`.
#[must_use]
pub fn millis_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl From<SystemTime> for Timestamp {
    /// Converts a host time; a time before the Unix epoch reads as the epoch.
    fn from(time: SystemTime) -> Self {
        Self(time.duration_since(SYSTEM_UNIX_EPOCH).map_or(0, millis_of))
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    /// Advances by whole milliseconds of `rhs`, saturating at `u64::MAX`.
    fn add(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(millis_of(rhs)))
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    /// Moves back by whole milliseconds of `rhs`, saturating at the Unix epoch.
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0.saturating_sub(millis_of(rhs)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_saturates_at_maximum() {
        let near_max = Timestamp::from_unix_millis(u64::MAX - 1);
        let result = near_max + Duration::from_millis(10);
        assert_eq!(result.as_unix_millis(), u64::MAX);
    }

    #[test]
    fn sub_saturates_at_epoch() {
        let result = Timestamp::UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(result, Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn system_time_before_the_epoch_clamps_to_the_epoch() {
        use std::time::UNIX_EPOCH;

        let before = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(Timestamp::from(before), Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn system_time_after_the_epoch_keeps_its_milliseconds() {
        use std::time::UNIX_EPOCH;

        let after = UNIX_EPOCH + Duration::from_millis(1_500);
        assert_eq!(Timestamp::from(after).as_unix_millis(), 1_500);
    }

    #[test]
    fn add_truncates_sub_millisecond_part() {
        let start = Timestamp::from_unix_millis(1_000);
        assert_eq!(
            (start + Duration::from_micros(1_999)).as_unix_millis(),
            1_001
        );
    }

    #[test]
    fn sub_truncates_sub_millisecond_part() {
        let start = Timestamp::from_unix_millis(1_000);
        assert_eq!((start - Duration::from_micros(1_999)).as_unix_millis(), 999);
    }

    #[test]
    fn duration_since_earlier_is_difference() {
        let earlier = Timestamp::from_unix_millis(1_000);
        let later = Timestamp::from_unix_millis(1_750);
        assert_eq!(
            later.saturating_duration_since(earlier),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn duration_since_later_is_zero() {
        let earlier = Timestamp::from_unix_millis(1_000);
        let later = Timestamp::from_unix_millis(1_750);
        assert_eq!(earlier.saturating_duration_since(later), Duration::ZERO);
    }

    #[test]
    fn serializes_as_bare_unix_millis() {
        let timestamp = Timestamp::from_unix_millis(1_500);
        let json = serde_json::to_string(&timestamp).expect("timestamp serializes");
        assert_eq!(json, "1500");
        let parsed: Timestamp = serde_json::from_str(&json).expect("timestamp deserializes");
        assert_eq!(parsed, timestamp);
    }

    #[test]
    fn epoch_formats_as_rfc3339() {
        assert_eq!(
            Timestamp::UNIX_EPOCH.to_rfc3339(),
            "1970-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn formats_milliseconds_as_three_fractional_digits() {
        let timestamp = Timestamp::from_unix_millis(1_700_000_000_123);
        assert_eq!(timestamp.to_rfc3339(), "2023-11-14T22:13:20.123Z");
    }

    #[test]
    fn parses_utc_rfc3339() {
        let parsed = Timestamp::from_rfc3339("2026-01-14T09:19:30.000Z").expect("parses");
        assert_eq!(parsed, Timestamp::from_unix_millis(1_768_382_370_000));
    }

    #[test]
    fn parses_offset_to_utc() {
        let parsed = Timestamp::from_rfc3339("2026-01-14T11:19:30.5+02:00").expect("parses");
        let utc = Timestamp::from_rfc3339("2026-01-14T09:19:30.500Z").expect("parses");
        assert_eq!(parsed, utc);
    }

    #[test]
    fn truncates_fractional_digits_beyond_milliseconds() {
        let parsed = Timestamp::from_rfc3339("2026-01-14T09:19:30.123999Z").expect("parses");
        assert_eq!(parsed.as_unix_millis() % 1_000, 123);
    }

    #[test]
    fn rejects_malformed_text() {
        assert!(matches!(
            Timestamp::from_rfc3339("yesterday"),
            Err(Rfc3339Error::Malformed(_))
        ));
        assert!(matches!(
            Timestamp::from_rfc3339("2026-01-14"),
            Err(Rfc3339Error::Malformed(_))
        ));
    }

    #[test]
    fn rejects_instants_before_epoch() {
        assert!(matches!(
            Timestamp::from_rfc3339("1969-12-31T23:59:59.999Z"),
            Err(Rfc3339Error::BeforeEpoch)
        ));
    }

    #[test]
    fn end_of_time_formats_as_last_four_digit_instant() {
        assert_eq!(
            Timestamp::END_OF_TIME.to_string(),
            "9999-12-31T23:59:59.999Z"
        );
        assert_eq!(
            Timestamp::END_OF_TIME,
            Timestamp::from_rfc3339("9999-12-31T23:59:59.999Z").expect("parses")
        );
    }

    #[test]
    fn instants_after_end_of_time_clamp_to_it() {
        let beyond = Timestamp::from_unix_millis(Timestamp::END_OF_TIME.as_unix_millis() + 1);
        assert_eq!(beyond.to_string(), "9999-12-31T23:59:59.999Z");
    }

    #[test]
    fn seconds_truncate_the_fraction() {
        assert_eq!(
            Timestamp::from_unix_millis(1_700_000_000_000).to_rfc3339_seconds(),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            Timestamp::from_unix_millis(1_700_000_000_999).to_rfc3339_seconds(),
            "2023-11-14T22:13:20Z"
        );
    }

    #[test]
    fn rfc3339_round_trips() {
        for millis in [
            0,
            1,
            999,
            1_700_000_000_123,
            1_768_382_370_500,
            MAX_RFC3339_MILLIS,
        ] {
            let timestamp = Timestamp::from_unix_millis(millis);
            let text = timestamp.to_rfc3339();
            assert_eq!(Timestamp::from_rfc3339(&text).expect("parses"), timestamp);
        }
    }
}
