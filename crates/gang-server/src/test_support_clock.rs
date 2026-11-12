//! A test clock shared between the code under test and the test body.

use std::sync::Arc;

use gang_clock::{TestClock, Timestamp};

/// The fixed instant every test clock starts at: 2026-01-01T00:00:00Z.
pub const START_UNIX_MILLIS: u64 = 1_767_225_600_000;

/// A test clock at the fixed test instant `START_UNIX_MILLIS`, shared between the code under test
/// and the test body.
#[must_use]
pub fn start() -> Arc<TestClock> {
    at(START_UNIX_MILLIS)
}

/// A test clock reading `unix_millis` milliseconds after the Unix epoch, shared between the code
/// under test and the test body.
#[must_use]
pub fn at(unix_millis: u64) -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(unix_millis)))
}
