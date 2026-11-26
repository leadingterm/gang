//! Identifiers for execution attempts and `REAPI` operations.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read as _;

/// Longest allowed attempt identifier, well within `NAME_MAX` (255).
const ATTEMPT_ID_MAX_LENGTH: usize = 128;

/// Longest allowed `REAPI` operation name, in characters.
const OPERATION_NAME_MAX_LENGTH: usize = 1024;

/// Errors from validating or generating an [`AttemptId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttemptIdError {
    /// The attempt identifier was empty.
    #[error("attempt id is empty")]
    Empty,
    /// The attempt identifier exceeds the maximum permitted length.
    #[error("attempt id is longer than {max} characters")]
    TooLong {
        /// Maximum permitted length in characters.
        max: usize,
    },
    /// The attempt identifier begins with `.`, which is reserved for special entries.
    #[error("attempt id must not start with '.'")]
    LeadingDot,
    /// The attempt identifier contains a character disallowed in a single path segment.
    #[error("invalid character '{character}' in attempt id")]
    InvalidCharacter {
        /// The first disallowed character.
        character: char,
    },
    /// Reading system randomness for a fresh identifier failed.
    #[error("failed to read system entropy for attempt id: {message}")]
    Entropy {
        /// Underlying OS error description.
        message: String,
    },
}

fn validate_attempt_id(value: &str) -> Result<(), AttemptIdError> {
    if value.is_empty() {
        return Err(AttemptIdError::Empty);
    }
    if let Some(character) = value.chars().find(|character| {
        !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    }) {
        return Err(AttemptIdError::InvalidCharacter { character });
    }
    if value.starts_with('.') {
        return Err(AttemptIdError::LeadingDot);
    }
    if value.len() > ATTEMPT_ID_MAX_LENGTH {
        return Err(AttemptIdError::TooLong {
            max: ATTEMPT_ID_MAX_LENGTH,
        });
    }
    Ok(())
}

crate::validated_string!(
    /// Unique identifier for a single dispatched attempt of an operation.
    ///
    /// Fresh identifiers generated with [`AttemptId::generate`] are canonical lowercase `UUIDv4`
    /// strings. Parsed identifiers are validated as non-empty, safe single filename segments
    /// (`A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, never starting with `.` and at most 128 characters) so
    /// an attempt directory under `<work>/<attempt_id>`, `<work>/.graveyard/<attempt_id>`, or
    /// `.side_channels/<attempt_id>` cannot escape or collide with reserved directories.
    pub AttemptId,
    validate_attempt_id,
    AttemptIdError
);

impl AttemptId {
    /// Generates a fresh `UUIDv4` attempt identifier using OS entropy (`/dev/urandom`).
    ///
    /// # Errors
    ///
    /// Returns [`AttemptIdError::Entropy`] if OS randomness cannot be read.
    pub fn generate() -> Result<Self, AttemptIdError> {
        let mut bytes = [0_u8; 16];
        let mut file = File::open("/dev/urandom").map_err(|error| AttemptIdError::Entropy {
            message: error.to_string(),
        })?;
        file.read_exact(&mut bytes)
            .map_err(|error| AttemptIdError::Entropy {
                message: error.to_string(),
            })?;
        Ok(Self::from_random_bytes(bytes))
    }

    /// Formats 16 random bytes as a canonical lowercase `UUIDv4` [`AttemptId`].
    ///
    /// Sets the RFC 4122 version (`4`) and variant (`10xx`) bits automatically.
    #[must_use]
    pub fn from_random_bytes(mut bytes: [u8; 16]) -> Self {
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;

        let mut text = String::with_capacity(36);
        let _ = write!(
            text,
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u16::from_be_bytes([bytes[4], bytes[5]]),
            u16::from_be_bytes([bytes[6], bytes[7]]),
            u16::from_be_bytes([bytes[8], bytes[9]]),
            u64::from_be_bytes([
                0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            ]),
        );
        Self(Cow::Owned(text))
    }
}

/// Errors from validating an [`OperationName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperationNameError {
    /// The operation name was empty.
    #[error("operation name is empty")]
    Empty,
    /// The operation name exceeds the maximum permitted length.
    #[error("operation name is longer than {max} characters")]
    TooLong {
        /// Maximum permitted length in characters.
        max: usize,
    },
    /// The operation name contains a control or whitespace character.
    #[error("invalid character '{character}' in operation name")]
    InvalidCharacter {
        /// The first disallowed character.
        character: char,
    },
}

fn validate_operation_name(value: &str) -> Result<(), OperationNameError> {
    if value.is_empty() {
        return Err(OperationNameError::Empty);
    }
    if let Some(character) = value
        .chars()
        .find(|character| character.is_control() || character.is_whitespace())
    {
        return Err(OperationNameError::InvalidCharacter { character });
    }
    if value.chars().count() > OPERATION_NAME_MAX_LENGTH {
        return Err(OperationNameError::TooLong {
            max: OPERATION_NAME_MAX_LENGTH,
        });
    }
    Ok(())
}

crate::validated_string!(
    /// Validated non-empty `REAPI` `google.longrunning.Operation` resource name.
    ///
    /// Permits `/` for resource paths such as `operations/<id>` or `<instance>/operations/<id>`,
    /// while rejecting empty strings, control characters, whitespace, and names over 1024
    /// characters.
    pub OperationName,
    validate_operation_name,
    OperationNameError
);

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn is_uuid_v4_text(text: &str) -> bool {
        text.len() == 36
            && text.bytes().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                14 => byte == b'4',
                19 => matches!(byte, b'8' | b'9' | b'a' | b'b'),
                _ => byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'),
            })
    }

    #[test]
    fn generate_produces_unique_valid_uuid_v4_attempt_ids() {
        let first = AttemptId::generate().expect("entropy available");
        let second = AttemptId::generate().expect("entropy available");
        assert_ne!(first, second);
        assert!(is_uuid_v4_text(first.as_str()), "{first}");
        assert!(is_uuid_v4_text(second.as_str()), "{second}");
        assert_eq!(first.as_str().parse::<AttemptId>(), Ok(first.clone()));
    }

    #[test]
    fn from_random_bytes_sets_rfc4122_v4_bits() {
        let all_zeros = AttemptId::from_random_bytes([0_u8; 16]);
        assert_eq!(all_zeros.as_str(), "00000000-0000-4000-8000-000000000000");
        assert!(is_uuid_v4_text(all_zeros.as_str()));

        let all_ones = AttemptId::from_random_bytes([0xff_u8; 16]);
        assert_eq!(all_ones.as_str(), "ffffffff-ffff-4fff-bfff-ffffffffffff");
        assert!(is_uuid_v4_text(all_ones.as_str()));
    }

    #[test]
    fn attempt_id_validates_single_path_segment_and_round_trips() {
        for valid in [
            "550e8400-e29b-41d4-a716-446655440000",
            "attempt-1",
            "att_01.part",
            &"a".repeat(ATTEMPT_ID_MAX_LENGTH),
        ] {
            let id = AttemptId::try_new(valid).expect("valid attempt id");
            assert_eq!(id.as_str(), valid);
            assert_eq!(id.to_string(), valid);
            let json = serde_json::to_string(&id).expect("serialize");
            assert_eq!(
                serde_json::from_str::<AttemptId>(&json).expect("deserialize"),
                id
            );
        }

        assert_eq!(AttemptId::try_new(""), Err(AttemptIdError::Empty));
        for leading_dot in [".", "..", ".side_channels", ".graveyard", ".hidden"] {
            assert_eq!(
                AttemptId::try_new(leading_dot),
                Err(AttemptIdError::LeadingDot),
                "{leading_dot}"
            );
        }
        for bad_char in ["a/b", "a\\b", "../escape", "a b", "a\nb", "a\0b", "a:b"] {
            assert!(
                matches!(
                    AttemptId::try_new(bad_char),
                    Err(AttemptIdError::InvalidCharacter { .. })
                ),
                "{bad_char:?}"
            );
        }
        assert_eq!(
            AttemptId::try_new("a".repeat(ATTEMPT_ID_MAX_LENGTH + 1)),
            Err(AttemptIdError::TooLong {
                max: ATTEMPT_ID_MAX_LENGTH,
            })
        );
    }

    #[test]
    fn operation_name_validates_and_supports_ordering_and_serde() {
        for valid in [
            "operations/550e8400-e29b-41d4-a716-446655440000",
            "default/operations/op-1",
            "op_1:v2",
            &"x".repeat(OPERATION_NAME_MAX_LENGTH),
        ] {
            let name = OperationName::try_new(valid).expect("valid operation name");
            assert_eq!(name.as_str(), valid);
            assert_eq!(name.to_string(), valid);
            assert_eq!(valid.parse::<OperationName>(), Ok(name.clone()));
            let json = serde_json::to_string(&name).expect("serialize");
            assert_eq!(
                serde_json::from_str::<OperationName>(&json).expect("deserialize"),
                name
            );
        }

        assert_eq!(OperationName::try_new(""), Err(OperationNameError::Empty));
        for bad in ["op 1", "op\t1", "op\n1", "op\0"] {
            assert!(
                matches!(
                    OperationName::try_new(bad),
                    Err(OperationNameError::InvalidCharacter { .. })
                ),
                "{bad:?}"
            );
        }
        assert_eq!(
            OperationName::try_new("x".repeat(OPERATION_NAME_MAX_LENGTH + 1)),
            Err(OperationNameError::TooLong {
                max: OPERATION_NAME_MAX_LENGTH,
            })
        );

        let mut set = BTreeSet::new();
        set.insert(OperationName::try_new("operations/b").expect("valid"));
        set.insert(OperationName::try_new("operations/a").expect("valid"));
        assert!(set.contains("operations/a"));
        assert!(!set.contains("operations/c"));
    }
}
