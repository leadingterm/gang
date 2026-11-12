//! Byte sizes that must be greater than zero, such as memory limits.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use crate::{Bytes, QuantityError};

/// A size in bytes that is never zero.
///
/// Parses and displays exactly like [`Bytes`], but rejects `0B`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroBytes(NonZeroU64);

impl NonZeroBytes {
    /// Creates a size from `size`, returning `None` if it is zero.
    #[must_use]
    pub const fn new(size: Bytes) -> Option<Self> {
        match NonZeroU64::new(size.get()) {
            Some(count) => Some(Self(count)),
            None => None,
        }
    }

    /// Creates a size of `count` kibibytes, saturating at `u64::MAX` bytes.
    ///
    /// Intended for constants and defaults.
    ///
    /// # Panics
    ///
    /// Panics if `count` is zero.
    #[must_use]
    pub const fn kib(count: u64) -> Self {
        Self::scaled(count, 1 << 10)
    }

    /// Creates a size of `count` mebibytes, saturating at `u64::MAX` bytes.
    ///
    /// Intended for constants and defaults.
    ///
    /// # Panics
    ///
    /// Panics if `count` is zero.
    #[must_use]
    pub const fn mib(count: u64) -> Self {
        Self::scaled(count, 1 << 20)
    }

    /// Creates a size of `count` gibibytes, saturating at `u64::MAX` bytes.
    ///
    /// Intended for constants and defaults.
    ///
    /// # Panics
    ///
    /// Panics if `count` is zero.
    #[must_use]
    pub const fn gib(count: u64) -> Self {
        Self::scaled(count, 1 << 30)
    }

    /// Creates a size of `count` tebibytes, saturating at `u64::MAX` bytes.
    ///
    /// Intended for constants and defaults.
    ///
    /// # Panics
    ///
    /// Panics if `count` is zero.
    #[must_use]
    pub const fn tib(count: u64) -> Self {
        Self::scaled(count, 1 << 40)
    }

    /// Returns the size.
    #[must_use]
    pub const fn get(self) -> Bytes {
        Bytes::new(self.0.get())
    }

    /// Returns the size in bytes.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0.get()
    }

    /// Returns the non-zero size in bytes.
    #[must_use]
    pub const fn as_nonzero_u64(self) -> NonZeroU64 {
        self.0
    }

    const fn scaled(count: u64, factor: u64) -> Self {
        assert!(count != 0, "size must be greater than zero");
        match NonZeroU64::new(count.saturating_mul(factor)) {
            Some(value) => Self(value),
            // Unreachable: both operands are non-zero, so their saturated product is too.
            None => Self(NonZeroU64::MIN),
        }
    }
}

impl From<NonZeroBytes> for Bytes {
    fn from(size: NonZeroBytes) -> Self {
        size.get()
    }
}

/// Errors from parsing a [`NonZeroBytes`] size.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NonZeroBytesError {
    /// The size is zero.
    #[error("size must be greater than zero")]
    Zero,
    /// The text is not a valid size.
    #[error(transparent)]
    Invalid(#[from] QuantityError),
}

impl FromStr for NonZeroBytes {
    type Err = NonZeroBytesError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let size: Bytes = input.parse()?;
        Self::new(size).ok_or(NonZeroBytesError::Zero)
    }
}

impl fmt::Display for NonZeroBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.get(), f)
    }
}

impl_parsed_serde!(NonZeroBytes, "a non-zero size such as \"29GiB\"");

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn json_deserializes_borrowed_and_owned_strings() {
        let expected = NonZeroBytes::gib(29);
        assert_eq!(
            serde_json::from_str::<NonZeroBytes>("\"29GiB\"").expect("valid size"),
            expected
        );
        assert_eq!(
            serde_json::from_value::<NonZeroBytes>(serde_json::Value::String("29GiB".to_owned()))
                .expect("valid size"),
            expected
        );
    }

    #[test]
    fn returns_nonzero_byte_count() {
        const COUNT: NonZeroU64 = NonZeroBytes::kib(1).as_nonzero_u64();
        assert_eq!(COUNT.get(), 1024);
    }

    const GIB: u64 = 1 << 30;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        size: NonZeroBytes,
    }

    const DEFAULT_LIMIT: NonZeroBytes = NonZeroBytes::gib(29);

    #[test]
    fn parses_non_zero_sizes() {
        let size: NonZeroBytes = "29GiB".parse().expect("valid size");
        assert_eq!(size.as_u64(), 29 * GIB);
        assert_eq!(size.get(), Bytes::new(29 * GIB));
        assert_eq!(Bytes::from(size), Bytes::new(29 * GIB));
    }

    #[test]
    fn rejects_zero_and_malformed_sizes() {
        assert_eq!("0B".parse::<NonZeroBytes>(), Err(NonZeroBytesError::Zero));
        assert_eq!("0GiB".parse::<NonZeroBytes>(), Err(NonZeroBytesError::Zero));
        assert!(matches!(
            "29GB".parse::<NonZeroBytes>(),
            Err(NonZeroBytesError::Invalid(
                QuantityError::UnknownUnit { .. }
            ))
        ));
        assert_eq!(NonZeroBytes::new(Bytes::new(0)), None);
    }

    #[test]
    fn displays_like_bytes() {
        let size = NonZeroBytes::gib(29);
        assert_eq!(size.to_string(), "29GiB");
        assert_eq!(size.to_string().parse::<NonZeroBytes>(), Ok(size));
    }

    #[test]
    fn toml_round_trips_as_string() {
        let holder = Holder {
            size: NonZeroBytes::gib(29),
        };
        let text = toml::to_string(&holder).expect("serializes");
        assert_eq!(text.trim(), "size = \"29GiB\"");
        assert_eq!(
            toml::from_str::<Holder>(&text).expect("deserializes"),
            holder
        );
        assert!(toml::from_str::<Holder>("size = \"0B\"").is_err());
    }

    #[test]
    fn literal_constructors_work_in_const_context() {
        assert_eq!(DEFAULT_LIMIT.as_u64(), 29 * GIB);
        assert_eq!(NonZeroBytes::kib(1).as_u64(), 1 << 10);
        assert_eq!(NonZeroBytes::mib(1).as_u64(), 1 << 20);
        assert_eq!(NonZeroBytes::tib(1).as_u64(), 1 << 40);
        assert_eq!(NonZeroBytes::tib(u64::MAX).as_u64(), u64::MAX);
    }

    #[test]
    fn rejects_non_string_values() {
        assert!(toml::from_str::<Holder>("size = 29").is_err());
    }
}
