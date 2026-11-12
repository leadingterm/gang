//! Compact durations such as `500ms`, `10m` and `7d`, for configuration files.
//!
//! Use `#[serde(with = "gang_types::duration")]` on a [`Duration`](std::time::Duration) field.

use std::fmt;
use std::time::Duration;

use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};

use crate::QuantityError;
use crate::quantity::Units;

/// Units accepted by [`parse`] and written by [`display`], largest first.
const UNITS: Units = Units {
    kind: "duration",
    table: &[
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ],
    zero_unit: "s",
};

/// Parses a duration of the form `<digits><unit>`.
///
/// Units are `ms`, `s`, `m`, `h` and `d` (86400 seconds). Whitespace, fractions and
/// compound forms such as `1h30m` are rejected.
///
/// # Errors
///
/// Returns a [`QuantityError`] for empty input, a missing or malformed number, a missing or
/// unknown unit, or a value that overflows.
pub fn parse(input: &str) -> Result<Duration, QuantityError> {
    UNITS.parse(input).map(Duration::from_millis)
}

/// Returns a value that writes a duration using the largest unit that divides it exactly.
///
/// The text is written straight into the formatter, so no allocation is needed. Sub-millisecond
/// precision is truncated toward zero, because the format has no unit finer than `ms`. Every
/// duration that [`parse`] can produce is written losslessly. A zero duration displays as `0s`.
#[must_use]
pub fn display(value: Duration) -> impl fmt::Display + Copy {
    Formatted(value)
}

/// Writes a duration in the format of [`display`].
#[derive(Clone, Copy)]
struct Formatted(Duration);

impl fmt::Display for Formatted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        UNITS.write(formatter, self.0.as_millis())
    }
}

/// Serializes a duration as a string accepted by [`parse`].
///
/// # Errors
///
/// Returns the serializer's error if writing the string fails.
pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&Formatted(*value))
}

/// Deserializes a duration from a string accepted by [`parse`].
///
/// # Errors
///
/// Returns a deserialization error if the input is not a string or does not parse.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    deserializer.deserialize_str(DurationVisitor)
}

struct DurationVisitor;

impl Visitor<'_> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a duration such as \"10m\"")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        parse(value).map_err(E::custom)
    }
}

crate::serde_option_module!(::std::time::Duration, "null or a duration such as \"10m\"");

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn option_serializes_and_deserializes_some_and_none() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Optional {
            #[serde(with = "crate::duration::option")]
            value: Option<Duration>,
        }

        for (value, json) in [
            (
                Some(Duration::from_mins(10)),
                serde_json::json!({"value": "10m"}),
            ),
            (None, serde_json::json!({"value": null})),
        ] {
            let original = Optional { value };
            assert_eq!(serde_json::to_value(&original).expect("serializes"), json);
            assert_eq!(
                serde_json::from_value::<Optional>(json).expect("deserializes"),
                original
            );
        }
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        #[serde(with = "crate::duration")]
        timeout: Duration,
    }

    #[test]
    fn parses_valid_durations() {
        assert_eq!(parse("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse("10m"), Ok(Duration::from_mins(10)));
        assert_eq!(parse("7d"), Ok(Duration::from_hours(168)));
        assert_eq!(parse("2m"), Ok(Duration::from_mins(2)));
        assert_eq!(parse("0s"), Ok(Duration::ZERO));
    }

    #[test]
    fn rejects_malformed_durations() {
        assert!(matches!(parse(""), Err(QuantityError::Empty { .. })));
        assert!(matches!(
            parse("ms"),
            Err(QuantityError::MissingNumber { .. })
        ));
        assert!(matches!(
            parse("10"),
            Err(QuantityError::MissingUnit { .. })
        ));
        assert!(matches!(
            parse("10x"),
            Err(QuantityError::UnknownUnit { .. })
        ));
        assert!(matches!(
            parse("1h30m"),
            Err(QuantityError::UnknownUnit { .. })
        ));
        assert!(matches!(
            parse("1.5s"),
            Err(QuantityError::InvalidNumber { .. })
        ));
        assert!(matches!(
            parse(" 5s"),
            Err(QuantityError::InvalidNumber { .. })
        ));
        assert!(matches!(
            parse("99999999999999999999d"),
            Err(QuantityError::Overflow { .. })
        ));
        assert!(matches!(
            parse("18446744073709551615d"),
            Err(QuantityError::Overflow { .. })
        ));
    }

    #[test]
    fn unknown_unit_is_rejected() {
        assert!(matches!(
            parse("10x"),
            Err(QuantityError::UnknownUnit { .. })
        ));
    }

    #[test]
    fn units_are_case_sensitive() {
        assert!(matches!(
            parse("10M"),
            Err(QuantityError::UnknownUnit { unit, .. }) if unit == "M"
        ));
        assert!(matches!(
            parse("5MS"),
            Err(QuantityError::UnknownUnit { unit, .. }) if unit == "MS"
        ));
    }

    #[test]
    fn format_and_parse_round_trip() {
        let cases = [
            (Duration::ZERO, "0s"),
            (Duration::from_millis(500), "500ms"),
            (Duration::from_millis(1_500), "1500ms"),
            (Duration::from_secs(90), "90s"),
            (Duration::from_mins(2), "2m"),
            (Duration::from_hours(1), "1h"),
            (Duration::from_hours(168), "7d"),
        ];
        for (value, text) in cases {
            assert_eq!(display(value).to_string(), text);
            assert_eq!(parse(text), Ok(value));
        }
    }

    #[test]
    fn format_truncates_sub_millisecond_precision() {
        assert_eq!(display(Duration::from_micros(1_999)).to_string(), "1ms");
        assert_eq!(display(Duration::from_micros(400)).to_string(), "0s");
    }

    #[test]
    fn serde_round_trips_through_toml() {
        let holder = Holder {
            timeout: Duration::from_mins(10),
        };
        let text = toml::to_string(&holder).unwrap();
        assert_eq!(text.trim(), "timeout = \"10m\"");
        assert_eq!(toml::from_str::<Holder>(&text).unwrap(), holder);
    }

    #[test]
    fn serde_rejects_integers() {
        assert!(toml::from_str::<Holder>("timeout = 600").is_err());
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OptionHolder {
        #[serde(
            with = "crate::duration::option",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        timeout: Option<Duration>,
    }

    #[test]
    fn option_adapter_reads_present_value() {
        let holder: OptionHolder = toml::from_str("timeout = \"10m\"").expect("valid toml");
        assert_eq!(holder.timeout, Some(Duration::from_mins(10)));
    }

    #[test]
    fn option_adapter_treats_absent_field_as_none() {
        let holder: OptionHolder = toml::from_str("").expect("valid toml");
        assert_eq!(holder.timeout, None);
    }

    #[test]
    fn option_adapter_skips_none_when_serializing() {
        let text = toml::to_string(&OptionHolder { timeout: None }).expect("serializes");
        assert_eq!(text.trim(), "");
        let holder = OptionHolder {
            timeout: Some(Duration::from_secs(90)),
        };
        let text = toml::to_string(&holder).expect("serializes");
        assert_eq!(text.trim(), "timeout = \"90s\"");
        assert_eq!(
            toml::from_str::<OptionHolder>(&text).expect("valid toml"),
            holder
        );
    }

    #[test]
    fn option_adapter_accepts_json_null_and_round_trips() {
        let holder: OptionHolder = serde_json::from_str(r#"{"timeout":null}"#).expect("valid json");
        assert_eq!(holder.timeout, None);
        let holder = OptionHolder {
            timeout: Some(Duration::from_hours(1)),
        };
        let json = serde_json::to_string(&holder).expect("serializes");
        assert_eq!(json, r#"{"timeout":"1h"}"#);
        assert_eq!(
            serde_json::from_str::<OptionHolder>(&json).expect("valid json"),
            holder
        );
    }

    #[test]
    fn option_adapter_rejects_invalid_values() {
        assert!(toml::from_str::<OptionHolder>("timeout = \"10x\"").is_err());
        assert!(toml::from_str::<OptionHolder>("timeout = 600").is_err());
    }

    struct UnitValue;

    impl<'de> Deserializer<'de> for UnitValue {
        type Error = serde::de::value::Error;

        fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            visitor.visit_unit()
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map
            struct enum identifier ignored_any
        }
    }

    #[test]
    fn option_adapter_maps_unit_value_to_none() {
        let value = option::deserialize(UnitValue).expect("unit is valid");
        assert_eq!(value, None);
    }

    #[test]
    fn option_adapter_rejects_non_string_values() {
        let deserializer = serde::de::value::BoolDeserializer::<serde::de::value::Error>::new(true);
        assert!(option::deserialize(deserializer).is_err());
    }
}
