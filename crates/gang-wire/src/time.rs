//! RFC 3339 serde for `gang_clock::Timestamp` fields (`*Time`).
//!
//! Writes are canonical (`YYYY-MM-DDTHH:MM:SS.mmmZ`); reads accept any RFC 3339 offset and
//! normalize it to UTC. Required fields use the module directly, optional fields use
//! [`option`]:
//!
//! ```text
//! #[serde(with = "gang_wire::time")]
//! pub create_time: Timestamp,
//!
//! #[serde(default, skip_serializing_if = "Option::is_none", with = "gang_wire::time::option")]
//! pub end_time: Option<Timestamp>,
//! ```

use gang_clock::Timestamp;
use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};

/// Serializes a timestamp as canonical RFC 3339 text.
///
/// # Errors
///
pub fn serialize<S: Serializer>(timestamp: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(timestamp)
}

/// Deserializes a timestamp from RFC 3339 text with any offset, normalized to UTC.
///
/// # Errors
///
/// Returns an error when the input is not a string or not a valid RFC 3339 timestamp.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
    struct TimestampVisitor;

    impl Visitor<'_> for TimestampVisitor {
        type Value = Timestamp;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an RFC 3339 timestamp")
        }

        fn visit_str<E: de::Error>(self, text: &str) -> Result<Timestamp, E> {
            Timestamp::from_rfc3339(text).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(TimestampVisitor)
}

gang_types::serde_option_module!(::gang_clock::Timestamp, "null or an RFC 3339 timestamp");

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[test]
    fn option_serializes_and_deserializes_some_and_none() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Nullable {
            #[serde(with = "crate::time::option")]
            value: Option<Timestamp>,
        }

        for (value, json) in [
            (Some(create_timestamp()), json!({"value": CREATE_TEXT})),
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

    /// 2026-01-14T09:19:30.000Z.
    const CREATE_MILLIS: u64 = 1_768_382_370_000;
    const CREATE_TEXT: &str = "2026-01-14T09:19:30.000Z";

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Required {
        #[serde(with = "crate::time")]
        create_time: Timestamp,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Optional {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::time::option"
        )]
        end_time: Option<Timestamp>,
    }

    fn create_timestamp() -> Timestamp {
        Timestamp::from_unix_millis(CREATE_MILLIS)
    }

    #[test]
    fn required_writes_canonical_rfc3339() {
        let value = Required {
            create_time: create_timestamp(),
        };
        assert_eq!(
            serde_json::to_value(&value).expect("serializes"),
            json!({"createTime": CREATE_TEXT})
        );
    }

    #[test]
    fn required_reads_offset_and_normalizes_to_utc() {
        let value: Required =
            serde_json::from_value(json!({"createTime": "2026-01-14T11:19:30+02:00"}))
                .expect("offset parses");
        assert_eq!(value.create_time, create_timestamp());
        assert_eq!(
            serde_json::to_value(&value).expect("serializes"),
            json!({"createTime": CREATE_TEXT})
        );
    }

    #[test]
    fn required_rejects_number() {
        let value = json!({"createTime": CREATE_MILLIS});
        assert!(serde_json::from_value::<Required>(value).is_err());
    }

    #[test]
    fn required_rejects_malformed_string() {
        let value = json!({"createTime": "yesterday"});
        assert!(serde_json::from_value::<Required>(value).is_err());
    }

    #[test]
    fn optional_absent_reads_as_none() {
        let value: Optional = serde_json::from_value(json!({})).expect("absent parses");
        assert_eq!(value, Optional { end_time: None });
    }

    #[test]
    fn optional_null_reads_as_none() {
        let value: Optional =
            serde_json::from_value(json!({"endTime": null})).expect("null parses");
        assert_eq!(value, Optional { end_time: None });
    }

    #[test]
    fn optional_none_is_omitted_on_write() {
        let value = Optional { end_time: None };
        assert_eq!(serde_json::to_value(&value).expect("serializes"), json!({}));
    }

    #[test]
    fn optional_present_round_trips() {
        let value = Optional {
            end_time: Some(create_timestamp()),
        };
        let written = serde_json::to_value(&value).expect("serializes");
        assert_eq!(written, json!({"endTime": CREATE_TEXT}));
        let round_trip: Optional = serde_json::from_value(written).expect("round-trips");
        assert_eq!(round_trip, value);
    }

    #[test]
    fn optional_rejects_number_and_malformed_string() {
        assert!(serde_json::from_value::<Optional>(json!({"endTime": 1})).is_err());
        assert!(serde_json::from_value::<Optional>(json!({"endTime": "not a time"})).is_err());
    }
}
