//! Conversion from Gang timestamps to X.509 validity times at whole-second precision.
//!
//! X.509 times cannot carry milliseconds, so every conversion truncates. The never-expires
//! instant is `Timestamp::END_OF_TIME`, whose final milliseconds truncate to
//! `9999-12-31T23:59:59Z`.

use std::time::{Duration, UNIX_EPOCH};

use gang_clock::Timestamp;
use x509_cert::time::Time;

use crate::errors::MtlsError;

/// Converts a timestamp to an X.509 time at whole-second precision.
///
/// Times up to 2049 are `UTCTime`; later times are `GeneralizedTime`.
///
/// # Errors
///
/// Returns [`MtlsError::TimeRange`] when the timestamp is outside the representable range.
pub(crate) fn x509_time(timestamp: Timestamp) -> Result<Time, MtlsError> {
    unix_time(Duration::from_secs(unix_seconds(timestamp))).ok_or(MtlsError::TimeRange(timestamp))
}

/// Returns the Unix epoch as an X.509 time.
pub(crate) fn epoch() -> Result<Time, MtlsError> {
    x509_time(Timestamp::UNIX_EPOCH)
}

/// Returns `9999-12-31T23:59:59Z`, used as the expiry of the self-signed CA certificate.
pub(crate) fn never_expires() -> Result<Time, MtlsError> {
    x509_time(Timestamp::END_OF_TIME)
}

/// Converts a duration since the Unix epoch to an X.509 time, or `None` when out of range.
fn unix_time(since_epoch: Duration) -> Option<Time> {
    Time::try_from(UNIX_EPOCH.checked_add(since_epoch)?).ok()
}

/// Returns whole seconds since the Unix epoch, truncating milliseconds.
pub(crate) fn unix_seconds(timestamp: Timestamp) -> u64 {
    timestamp.as_unix_millis() / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    const MILLIS_PER_SECOND: u64 = 1000;
    /// 2050-01-01T00:00:00Z.
    const YEAR_2050_MILLIS: u64 = 2_524_608_000 * MILLIS_PER_SECOND;

    #[test]
    fn milliseconds_are_truncated_to_whole_seconds() {
        let with_millis = Timestamp::from_unix_millis(1_500);
        let whole = Timestamp::from_unix_millis(1_000);

        assert_eq!(unix_seconds(with_millis), 1);
        assert_eq!(
            x509_time(with_millis).expect("in range"),
            x509_time(whole).expect("in range")
        );
    }

    #[test]
    fn epoch_is_the_unix_epoch() {
        assert_eq!(
            epoch()
                .expect("the unix epoch is representable")
                .to_string(),
            "1970-01-01T00:00:00Z"
        );
    }

    #[test]
    fn never_expires_is_a_generalized_time() {
        let expiry = never_expires().expect("never expires is representable");
        assert!(matches!(expiry, Time::GeneralTime(_)));
    }

    #[test]
    fn never_expires_truncates_to_the_last_whole_second() {
        assert_eq!(
            never_expires().expect("never expires is representable"),
            x509_time(Timestamp::from_unix_millis(253_402_300_799_000)).expect("in range")
        );
    }

    #[test]
    fn pre_2050_timestamp_is_utc_time() {
        let year_2000 = Timestamp::from_unix_millis(946_684_800 * MILLIS_PER_SECOND);

        assert!(matches!(
            x509_time(year_2000).expect("in range"),
            Time::UtcTime(_)
        ));
    }

    #[test]
    fn post_2049_timestamp_becomes_generalized_time() {
        let year_2050 = Timestamp::from_unix_millis(YEAR_2050_MILLIS);

        assert!(matches!(
            x509_time(year_2050).expect("in range"),
            Time::GeneralTime(_)
        ));
    }
}
