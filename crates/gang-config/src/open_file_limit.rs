//! Validated process open-file limit for Gang.

use serde::{Deserialize, Serialize};

/// Minimum limit, leaving a margin above the zero-permit range after reservation and rounding.
const MIN_OPEN_FILES: usize = 8;
/// Maximum supported process open-file limit.
const MAX_OPEN_FILES: usize = 1 << 30;
/// Default process open-file limit.
const DEFAULT_OPEN_FILES: usize = 24_576;

/// An open-file limit that leaves permits after the file semaphore's reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "usize", into = "usize")]
pub struct OpenFileLimit(usize);

impl OpenFileLimit {
    /// Returns the validated limit.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for OpenFileLimit {
    fn default() -> Self {
        Self(DEFAULT_OPEN_FILES)
    }
}

/// Errors from validating an open-file limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OpenFileLimitError {
    /// The limit is outside the supported range.
    #[error(
        "open file limit must be between {} and {}",
        MIN_OPEN_FILES,
        MAX_OPEN_FILES
    )]
    OutOfRange,
}

impl TryFrom<usize> for OpenFileLimit {
    type Error = OpenFileLimitError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if (MIN_OPEN_FILES..=MAX_OPEN_FILES).contains(&value) {
            Ok(Self(value))
        } else {
            Err(OpenFileLimitError::OutOfRange)
        }
    }
}

impl From<OpenFileLimit> for usize {
    fn from(value: OpenFileLimit) -> Self {
        value.get()
    }
}

#[cfg(test)]
mod tests {
    use super::OpenFileLimit;

    #[test]
    fn validates_inclusive_bounds() {
        assert!(OpenFileLimit::try_from(7).is_err());
        assert_eq!(OpenFileLimit::try_from(8).unwrap().get(), 8);
        assert_eq!(OpenFileLimit::try_from(1 << 30).unwrap().get(), 1 << 30);
        assert!(OpenFileLimit::try_from((1 << 30) + 1).is_err());
        assert_eq!(OpenFileLimit::default().get(), 24_576);
    }
}
