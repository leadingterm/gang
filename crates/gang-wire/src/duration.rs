//! Google `Duration` JSON strings: seconds with an optional fraction and an `s` suffix, such as
//! `600s` or `0.250s`.
//!
//! Serialization writes whole seconds as `{secs}s`. Otherwise it writes the shortest exact
//! fraction of 3, 6 or 9 digits. Parsing is strict: ASCII digits only, no sign, whitespace or
//! exponent, at most nine fractional digits, and the `s` suffix is required.
//!
//! The config syntax (`10m`) in `gang_types::duration` is unrelated and is not accepted here.
//!
//! ```text
//! #[serde(with = "gang_wire::duration")]
//! lease: Duration,
//!
//! #[serde(default, skip_serializing_if = "Option::is_none")]
//! #[serde(with = "gang_wire::duration::option")]
//! timeout: Option<Duration>,
//! ```

use std::fmt;
use std::time::Duration;

use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};

use crate::WireDurationError;

const NANOS_PER_MILLI: u32 = 1_000_000;
const NANOS_PER_MICRO: u32 = 1_000;

/// Returns a value that displays a Google `Duration` string without allocating.
///
/// Whole seconds become `{secs}s`. Otherwise the fraction uses 3 digits when it is exact to the
/// millisecond, 6 digits when exact to the microsecond, and 9 digits otherwise.
#[must_use]
pub fn display(duration: Duration) -> impl fmt::Display + Copy {
    Formatted(duration)
}

/// Display adapter that writes a [`Duration`] in the form used by [`display`].
#[derive(Clone, Copy)]
struct Formatted(Duration);

impl fmt::Display for Formatted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0.as_secs();
        let nanos = self.0.subsec_nanos();
        if nanos == 0 {
            write!(f, "{secs}s")
        } else if nanos.is_multiple_of(NANOS_PER_MILLI) {
            write!(f, "{secs}.{:03}s", nanos / NANOS_PER_MILLI)
        } else if nanos.is_multiple_of(NANOS_PER_MICRO) {
            write!(f, "{secs}.{:06}s", nanos / NANOS_PER_MICRO)
        } else {
            write!(f, "{secs}.{nanos:09}s")
        }
    }
}

/// Parses a Google `Duration` string strictly.
///
/// # Errors
///
/// Returns [`WireDurationError::Malformed`] when the text is not ASCII, lacks the `s` suffix,
/// has an empty or signed integer part, has an empty or more than nine fractional digits, or
/// contains any other character. Returns [`WireDurationError::OutOfRange`] when the whole seconds
/// overflow `u64`.
pub fn parse(text: &str) -> Result<Duration, WireDurationError> {
    if !text.is_ascii() {
        return Err(WireDurationError::Malformed);
    }
    let Some(body) = text.strip_suffix('s') else {
        return Err(WireDurationError::Malformed);
    };
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (body, None),
    };
    if !is_digits(whole) {
        return Err(WireDurationError::Malformed);
    }
    let secs = whole
        .parse::<u64>()
        .map_err(|_| WireDurationError::OutOfRange)?;
    let nanos = match fraction {
        None => 0,
        Some(digits) => {
            if digits.len() > 9 || !is_digits(digits) {
                return Err(WireDurationError::Malformed);
            }
            // Right-pad the fraction to nine digits; a short fraction is exact, not rounded.
            let mut nanos = 0_u32;
            for index in 0..9 {
                nanos *= 10;
                if let Some(digit) = digits.as_bytes().get(index) {
                    nanos += u32::from(*digit - b'0');
                }
            }
            nanos
        }
    };
    Ok(Duration::new(secs, nanos))
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Serializes a [`Duration`] as a Google `Duration` string, for use with `#[serde(with)]`.
///
/// # Errors
///
/// Returns the serializer's error when the string cannot be written.
pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&Formatted(*duration))
}

/// Deserializes a [`Duration`] from a Google `Duration` string, for use with `#[serde(with)]`.
///
/// # Errors
///
/// Returns the deserializer's error when the input is not a string, or the error message of
/// [`parse`] when the string is not a valid duration.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    struct DurationVisitor;

    impl Visitor<'_> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a Google duration string")
        }

        fn visit_str<E: de::Error>(self, text: &str) -> Result<Duration, E> {
            parse(text).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(DurationVisitor)
}

gang_types::serde_option_module!(::std::time::Duration, "null or a Google duration string");

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[test]
    fn option_serializes_and_deserializes_some_and_none() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Nullable {
            #[serde(with = "crate::duration::option")]
            value: Option<Duration>,
        }

        for (value, json) in [
            (Some(Duration::from_millis(250)), json!({"value": "0.250s"})),
            (None, json!({"value": null})),
        ] {
            let original = Nullable { value };
            assert_eq!(serde_json::to_value(&original).expect("serializes"), json);
            assert_eq!(
                serde_json::from_value::<Nullable>(json).expect("deserializes"),
                original
            );
        }
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Lease {
        #[serde(with = "crate::duration")]
        ttl: Duration,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Timeouts {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::duration::option"
        )]
        timeout: Option<Duration>,
    }

    #[test]
    fn display_writes_canonical_strings() {
        assert_eq!(display(Duration::from_secs(0)).to_string(), "0s");
        assert_eq!(display(Duration::from_mins(10)).to_string(), "600s");
        assert_eq!(display(Duration::from_millis(250)).to_string(), "0.250s");
        assert_eq!(display(Duration::from_millis(1500)).to_string(), "1.500s");
        assert_eq!(display(Duration::from_micros(1)).to_string(), "0.000001s");
        assert_eq!(display(Duration::from_nanos(1)).to_string(), "0.000000001s");
        assert_eq!(
            display(Duration::from_secs(u64::MAX)).to_string(),
            "18446744073709551615s"
        );
    }

    #[test]
    fn parse_accepts_canonical_and_short_fractions() {
        assert_eq!(parse("600s"), Ok(Duration::from_mins(10)));
        assert_eq!(parse("0s"), Ok(Duration::ZERO));
        assert_eq!(parse("0.250s"), Ok(Duration::from_millis(250)));
        assert_eq!(parse("0.25s"), Ok(Duration::from_millis(250)));
        assert_eq!(parse("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse("0.000000001s"), Ok(Duration::from_nanos(1)));
    }

    #[test]
    fn parse_rejects_malformed_text() {
        for text in [
            ".5s",
            "-1s",
            "+1s",
            "1 s",
            " 1s",
            "1s ",
            "1.s",
            "1.1234567891s",
            "1e3s",
            "s",
            "",
            "1",
            "1ms",
            "1.2.3s",
            "١s",
        ] {
            assert_eq!(parse(text), Err(WireDurationError::Malformed), "{text:?}");
        }
    }

    #[test]
    fn parse_reports_overflowing_seconds_as_out_of_range() {
        assert_eq!(
            parse("99999999999999999999s"),
            Err(WireDurationError::OutOfRange)
        );
        assert_eq!(
            parse("18446744073709551615s"),
            Ok(Duration::from_secs(u64::MAX))
        );
    }

    #[test]
    fn format_and_parse_round_trip() {
        for duration in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_micros(1),
            Duration::from_millis(250),
            Duration::from_millis(1500),
            Duration::new(7, 123_456_789),
            Duration::new(u64::MAX, 999_999_999),
        ] {
            assert_eq!(
                parse(&display(duration).to_string()),
                Ok(duration),
                "{duration:?}"
            );
        }
    }

    #[test]
    fn serde_round_trips_through_string() {
        let lease = Lease {
            ttl: Duration::from_millis(250),
        };
        let value = serde_json::to_value(&lease).expect("lease serializes");
        assert_eq!(value, json!({"ttl": "0.250s"}));
        let back: Lease = serde_json::from_value(value).expect("lease parses");
        assert_eq!(back, lease);
        let edge = serde_json::to_value(Lease {
            ttl: Duration::from_nanos(1),
        })
        .expect("lease serializes");
        assert_eq!(edge, json!({"ttl": "0.000000001s"}));
    }

    #[test]
    fn serde_rejects_non_string_and_malformed_input() {
        assert!(serde_json::from_value::<Lease>(json!({"ttl": 600})).is_err());
        assert!(serde_json::from_value::<Lease>(json!({"ttl": "10m"})).is_err());
    }

    #[test]
    fn option_round_trips_some() {
        let timeouts = Timeouts {
            timeout: Some(Duration::from_mins(10)),
        };
        let value = serde_json::to_value(&timeouts).expect("timeouts serialize");
        assert_eq!(value, json!({"timeout": "600s"}));
        let back: Timeouts = serde_json::from_value(value).expect("timeouts parse");
        assert_eq!(back, timeouts);
    }

    #[test]
    fn option_omits_absent_and_reads_null_as_none() {
        let timeouts = Timeouts { timeout: None };
        assert_eq!(
            serde_json::to_value(&timeouts).expect("timeouts serialize"),
            json!({})
        );
        let absent: Timeouts = serde_json::from_value(json!({})).expect("absent parses");
        assert_eq!(absent, timeouts);
        let null: Timeouts = serde_json::from_value(json!({"timeout": null})).expect("null parses");
        assert_eq!(null, timeouts);
    }

    #[test]
    fn option_rejects_malformed_string() {
        assert!(serde_json::from_value::<Timeouts>(json!({"timeout": "1 s"})).is_err());
    }
}
