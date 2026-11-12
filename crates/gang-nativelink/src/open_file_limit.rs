//! Validated process open-file limit for `NativeLink`.

use serde::{Deserialize, Serialize};

/// Keeps a margin above the zero-permit range: `NativeLink` reserves 20% and rounds,
/// leaving its file semaphore with no permits below five.
const MIN_OPEN_FILES: usize = 8;
/// Maximum supported process open-file limit.
const MAX_OPEN_FILES: usize = 1 << 30;
/// `NativeLink`'s own default process open-file limit.
const DEFAULT_OPEN_FILES: usize = 24_576;

/// An open-file limit that leaves permits for `NativeLink`'s file semaphore.
///
/// `NativeLink` reserves 20% of the limit; at least eight provides a margin above
/// the point where rounding leaves its semaphore with no permits.
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
