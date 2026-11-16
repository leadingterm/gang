//! Weekly minimum capacity schedules and their validated wall-clock fields.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use gang_clock::Timestamp;
use serde::{Deserialize, Serialize};

/// A day of the week, written `mon` to `sun`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Weekday {
    /// Monday.
    Mon,
    /// Tuesday.
    Tue,
    /// Wednesday.
    Wed,
    /// Thursday.
    Thu,
    /// Friday.
    Fri,
    /// Saturday.
    Sat,
    /// Sunday.
    Sun,
}

impl Weekday {
    /// Every day, in week order.
    pub const ALL: [Self; 7] = [
        Self::Mon,
        Self::Tue,
        Self::Wed,
        Self::Thu,
        Self::Fri,
        Self::Sat,
        Self::Sun,
    ];
}

impl fmt::Display for Weekday {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mon => "mon",
            Self::Tue => "tue",
            Self::Wed => "wed",
            Self::Thu => "thu",
            Self::Fri => "fri",
            Self::Sat => "sat",
            Self::Sun => "sun",
        })
    }
}

/// A non-empty set of weekdays, written as a list without repeats and written back in week order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "Vec<Weekday>", into = "Vec<Weekday>")]
pub struct Weekdays(u8);

/// An invalid list of weekdays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WeekdaysError {
    /// No days were listed.
    #[error("days must name at least one day")]
    Empty,
    /// A day was listed more than once.
    #[error("day `{0}` is listed twice")]
    Repeated(Weekday),
}

impl TryFrom<Vec<Weekday>> for Weekdays {
    type Error = WeekdaysError;

    fn try_from(days: Vec<Weekday>) -> Result<Self, Self::Error> {
        let mut bits = 0u8;
        for day in days {
            let bit = 1 << day as u8;
            if bits & bit != 0 {
                return Err(WeekdaysError::Repeated(day));
            }
            bits |= bit;
        }
        if bits == 0 {
            return Err(WeekdaysError::Empty);
        }
        Ok(Self(bits))
    }
}

impl From<Weekdays> for Vec<Weekday> {
    fn from(days: Weekdays) -> Self {
        Weekday::ALL
            .into_iter()
            .filter(|day| days.0 & (1 << *day as u8) != 0)
            .collect()
    }
}

/// A local wall-clock time of day in whole minutes, written `"HH:MM"` from `"00:00"` to `"24:00"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct ClockTime(u16);

/// A time that is not a valid whole-minute wall-clock value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("expected HH:MM from 00:00 to 24:00, got `{0}`")]
pub struct ClockTimeError(String);

impl FromStr for ClockTime {
    type Err = ClockTimeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        if let [h1, h2, b':', m1, m2] = bytes
            && h1.is_ascii_digit()
            && h2.is_ascii_digit()
            && m1.is_ascii_digit()
            && m2.is_ascii_digit()
        {
            let hour = u16::from(h1 - b'0') * 10 + u16::from(h2 - b'0');
            let minute = u16::from(m1 - b'0') * 10 + u16::from(m2 - b'0');
            if minute < 60 && (hour < 24 || (hour == 24 && minute == 0)) {
                return Ok(Self(hour * 60 + minute));
            }
        }
        Err(ClockTimeError(value.to_owned()))
    }
}

impl fmt::Display for ClockTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}:{:02}", self.0 / 60, self.0 % 60)
    }
}

impl TryFrom<String> for ClockTime {
    type Error = ClockTimeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ClockTime> for String {
    fn from(value: ClockTime) -> Self {
        value.to_string()
    }
}

/// An IANA time zone resolved against the tz database bundled in the binary; `name` is the
/// database's canonical spelling, whatever case was written.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct IanaTimeZone {
    name: String,
    zone: jiff::tz::TimeZone,
}

impl IanaTimeZone {
    /// The canonical IANA name, as the bundled database spells it.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

fn bundled() -> &'static jiff::tz::TimeZoneDatabase {
    static BUNDLED: std::sync::OnceLock<jiff::tz::TimeZoneDatabase> = std::sync::OnceLock::new();
    BUNDLED.get_or_init(jiff::tz::TimeZoneDatabase::bundled)
}

impl PartialEq for IanaTimeZone {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for IanaTimeZone {}

/// A time zone name absent from the bundled database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown time zone `{0}`")]
pub struct UnknownTimeZone(String);

impl FromStr for IanaTimeZone {
    type Err = UnknownTimeZone;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let zone = bundled()
            .get(name)
            .map_err(|_| UnknownTimeZone(name.to_owned()))?;
        let canonical = zone
            .iana_name()
            .ok_or_else(|| UnknownTimeZone(name.to_owned()))?
            .to_owned();
        Ok(Self {
            name: canonical,
            zone,
        })
    }
}

impl TryFrom<String> for IanaTimeZone {
    type Error = UnknownTimeZone;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<IanaTimeZone> for String {
    fn from(value: IanaTimeZone) -> Self {
        value.name
    }
}

/// One weekly window during which a fleet keeps at least `instances` instances.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "RawScheduledMinimum", into = "RawScheduledMinimum")]
pub struct ScheduledMinimum {
    instances: NonZeroU32,
    days: Weekdays,
    start: ClockTime,
    end: ClockTime,
    time_zone: IanaTimeZone,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RawScheduledMinimum {
    instances: NonZeroU32,
    days: Weekdays,
    start: ClockTime,
    end: ClockTime,
    time_zone: IanaTimeZone,
}

/// A window whose start is not before its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("start {start} is not before end {end}")]
pub struct WindowError {
    /// The start time.
    pub start: ClockTime,
    /// The end time.
    pub end: ClockTime,
}

impl ScheduledMinimum {
    /// Creates a window whose start is before its end.
    ///
    /// # Errors
    /// Returns [`WindowError`] if the start is not before the end.
    pub fn try_new(
        instances: NonZeroU32,
        days: Weekdays,
        start: ClockTime,
        end: ClockTime,
        time_zone: IanaTimeZone,
    ) -> Result<Self, WindowError> {
        if start >= end {
            return Err(WindowError { start, end });
        }
        Ok(Self {
            instances,
            days,
            start,
            end,
            time_zone,
        })
    }

    /// The minimum number of instances during this window.
    #[must_use]
    pub const fn instances(&self) -> NonZeroU32 {
        self.instances
    }

    /// Whether the local day and time fall in this window.
    pub(crate) fn in_force(&self, wall: Timestamp) -> bool {
        let instant = jiff::Timestamp::from_millisecond(
            i64::try_from(wall.as_unix_millis()).unwrap_or(i64::MAX),
        )
        .unwrap_or(jiff::Timestamp::MAX);
        let local = self.time_zone.zone.to_datetime(instant);
        let day = local.weekday().to_monday_zero_offset();
        let minute = i32::from(local.hour()) * 60 + i32::from(local.minute());
        self.days.0 & (1 << day) != 0
            && i32::from(self.start.0) <= minute
            && minute < i32::from(self.end.0)
    }
}

impl TryFrom<RawScheduledMinimum> for ScheduledMinimum {
    type Error = WindowError;

    fn try_from(raw: RawScheduledMinimum) -> Result<Self, Self::Error> {
        Self::try_new(raw.instances, raw.days, raw.start, raw.end, raw.time_zone)
    }
}

impl From<ScheduledMinimum> for RawScheduledMinimum {
    fn from(value: ScheduledMinimum) -> Self {
        Self {
            instances: value.instances,
            days: value.days,
            start: value.start,
            end: value.end,
            time_zone: value.time_zone,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_time_round_trips_and_rejects_invalid_values() {
        for value in ["00:00", "08:00", "23:59", "24:00"] {
            assert_eq!(
                value.parse::<ClockTime>().expect("valid time").to_string(),
                value
            );
        }
        for value in ["8:00", "08:0", "24:01", "25:00", "08:60", "0800", ""] {
            assert!(value.parse::<ClockTime>().is_err(), "{value}");
        }
    }

    #[test]
    fn weekdays_require_unique_nonempty_days_and_write_in_week_order() {
        assert_eq!(Weekdays::try_from(vec![]), Err(WeekdaysError::Empty));
        assert_eq!(
            Weekdays::try_from(vec![Weekday::Mon, Weekday::Mon]),
            Err(WeekdaysError::Repeated(Weekday::Mon))
        );
        let days = Weekdays::try_from(vec![Weekday::Fri, Weekday::Mon]).expect("valid days");
        assert_eq!(Vec::<Weekday>::from(days), vec![Weekday::Mon, Weekday::Fri]);
    }

    #[test]
    fn timezone_names_are_resolved() {
        assert!("Europe/Paris".parse::<IanaTimeZone>().is_ok());
        assert!("UTC".parse::<IanaTimeZone>().is_ok());
        assert!("Europe/Pariss".parse::<IanaTimeZone>().is_err());
    }

    #[test]
    fn timezone_names_are_canonicalized() {
        let lower = "europe/paris".parse::<IanaTimeZone>().expect("valid zone");
        let canonical = "Europe/Paris".parse::<IanaTimeZone>().expect("valid zone");
        assert_eq!(lower, canonical);
        assert_eq!(String::from(lower), "Europe/Paris");
    }

    #[test]
    fn windows_require_start_before_end() {
        let days = Weekdays::try_from(vec![Weekday::Mon]).expect("valid days");
        let zone = "UTC".parse::<IanaTimeZone>().expect("valid zone");
        for (start, end) in [("08:00", "08:00"), ("19:00", "08:00")] {
            assert!(
                ScheduledMinimum::try_new(
                    NonZeroU32::MIN,
                    days,
                    start.parse().expect("valid start"),
                    end.parse().expect("valid end"),
                    zone.clone()
                )
                .is_err()
            );
        }
    }
}
