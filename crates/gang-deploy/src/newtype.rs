//! Declarations of the text and number newtypes: each validates on `FromStr` and on
//! deserialization, prints as its text, and serializes as a JSON string or number.

/// Declares a validated text newtype. `$check` is a `fn(&str) -> Result<()>` that accepts the
/// text or names why it is rejected.
macro_rules! text_newtype {
    ($(#[$meta:meta])* $name:ident, $check:path) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// The text of the value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::DeployError;

            fn from_str(text: &str) -> $crate::errors::Result<Self> {
                $check(text)?;
                Ok(Self(text.to_owned()))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                $check(&text).map_err(serde::de::Error::custom)?;
                Ok(Self(text))
            }
        }
    };
}

/// Declares 1-based GitHub ids: digits only, never 0, serialized as JSON numbers.
macro_rules! number_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(std::num::NonZeroU64);

        impl $name {
            /// The number.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl From<std::num::NonZeroU64> for $name {
            fn from(number: std::num::NonZeroU64) -> Self {
                Self(number)
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::DeployError;

            fn from_str(text: &str) -> $crate::errors::Result<Self> {
                $crate::newtype::positive(text).map(Self)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

pub(crate) use {number_newtype, text_newtype};

use std::num::NonZeroU64;

use crate::errors::{DeployError, Expected, Result};

/// Parses `text` as a decimal number of digits only: no sign, no blanks, not empty.
pub(crate) fn decimal(text: &str) -> Result<u64> {
    let invalid = || DeployError::Invalid {
        text: text.to_owned(),
        expected: Expected::Number,
    };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    text.parse().map_err(|_| invalid())
}

/// Parses a positive decimal number of digits only.
pub(crate) fn positive(text: &str) -> Result<NonZeroU64> {
    NonZeroU64::new(decimal(text)?).ok_or_else(|| DeployError::Invalid {
        text: text.to_owned(),
        expected: Expected::PositiveNumber,
    })
}

/// Accepts `text` when it is exactly `length` lowercase hex characters.
pub(crate) fn lower_hex(text: &str, length: usize, expected: Expected) -> Result<()> {
    let valid = text.len() == length
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(DeployError::Invalid {
            text: text.to_owned(),
            expected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::decimal;

    #[test]
    fn decimal_accepts_digits_and_zero() {
        assert_eq!(decimal("42").unwrap(), 42);
        assert_eq!(decimal("0").unwrap(), 0);
    }

    #[test]
    fn positive_rejects_zero() {
        assert!(matches!(
            super::positive("0"),
            Err(crate::errors::DeployError::Invalid {
                expected: crate::errors::Expected::PositiveNumber,
                ..
            })
        ));
    }

    #[test]
    fn decimal_rejects_letters_signs_blanks_and_empty_text() {
        for text in ["4x", "", "-1", "+1", " 1", "1 "] {
            let error = decimal(text).unwrap_err();
            assert_eq!(error.to_string(), format!("`{text}` is not numeric"));
        }
    }
}
