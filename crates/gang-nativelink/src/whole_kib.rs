//! Whole-kibibyte settings used by `NativeLink`.
//!
//! Values are parsed as Gang byte sizes, then checked to be whole non-zero KiB.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

use gang_types::{Bytes, NonZeroBytes};
use serde::{Deserialize, Serialize};

/// Bytes in one kibibyte.
const KIB: u64 = 1 << 10;

/// A value that does not fit the unit of a whole-number setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WholeError {
    /// The value is not a whole number of KiB, or is less than one KiB.
    Kib,
}

impl WholeError {
    /// Describes the value a setting requires, for use in configuration errors.
    #[must_use]
    pub const fn requirement(self) -> &'static str {
        match self {
            Self::Kib => "must be a whole number of KiB, at least 1KiB",
        }
    }
}

impl fmt::Display for WholeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.requirement())
    }
}

impl Error for WholeError {}

/// A size that is a whole, non-zero number of KiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroBytes", into = "NonZeroBytes")]
pub struct WholeKib(NonZeroU64);

impl WholeKib {
    /// Creates a size of `kib` kibibytes.
    ///
    /// # Panics
    ///
    /// Panics if `kib` is zero. In a `const` context this is a compile error.
    #[must_use]
    pub const fn from_kib(kib: u64) -> Self {
        assert!(kib != 0, "a whole-KiB setting is at least one KiB");
        match NonZeroU64::new(kib) {
            Some(value) => Self(value),
            None => Self(NonZeroU64::MIN),
        }
    }

    /// Returns the number of KiB.
    #[must_use]
    pub const fn as_kib(self) -> u64 {
        self.0.get()
    }

    /// Returns the size in bytes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn get(self) -> Bytes {
        Bytes::kib(self.0.get())
    }
}

impl TryFrom<NonZeroBytes> for WholeKib {
    type Error = WholeError;

    fn try_from(size: NonZeroBytes) -> Result<Self, Self::Error> {
        let bytes = size.as_u64();
        if bytes < KIB || !bytes.is_multiple_of(KIB) {
            return Err(WholeError::Kib);
        }
        NonZeroU64::new(bytes / KIB)
            .map(Self)
            .ok_or(WholeError::Kib)
    }
}

impl From<WholeKib> for NonZeroBytes {
    fn from(value: WholeKib) -> Self {
        Self::kib(value.as_kib())
    }
}

#[cfg(test)]
mod tests {
    use gang_types::NonZeroBytes;

    use super::{WholeError, WholeKib};

    fn size(text: &str) -> NonZeroBytes {
        serde_json::from_value(serde_json::Value::String(text.to_owned())).expect("valid size text")
    }

    #[test]
    fn kib_accepts_whole_kib_of_at_least_one() {
        let parsed = WholeKib::try_from(size("4GiB")).expect("whole KiB");
        assert_eq!(parsed.as_kib(), 4 * 1024 * 1024);
        assert_eq!(parsed.get().get(), 4 << 30);

        assert_eq!(WholeKib::try_from(size("1500B")), Err(WholeError::Kib));
        assert_eq!(WholeKib::try_from(size("512B")), Err(WholeError::Kib));
        assert_eq!(WholeKib::try_from(size("1KiB")), Ok(WholeKib::from_kib(1)));
    }

    #[test]
    fn kib_round_trip_through_json() {
        let value = WholeKib::from_kib(4 * 1024 * 1024);
        let text = serde_json::to_string(&value).expect("serializes");
        assert_eq!(text, "\"4GiB\"");
        let back: WholeKib = serde_json::from_str(&text).expect("deserializes");
        assert_eq!(back, value);
        assert!(serde_json::from_str::<WholeKib>("\"512B\"").is_err());
    }
}
