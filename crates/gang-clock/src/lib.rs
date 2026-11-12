//! Injectable wall-clock and monotonic time for Gang components.
//!
//! Components read time and wait only through the [`Clock`] trait, so tests can drive
//! time deterministically with [`TestClock`].
//! Use [`Clock::timeout`] to bound a future by monotonic time.
//!
//! ```
//! use gang_clock::{Clock, TestClock, Timestamp};
//! use std::time::Duration;
//!
//! let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
//! clock.advance(Duration::from_millis(500));
//! assert_eq!(clock.wall().as_unix_millis(), 1_500);
//! ```

mod clock;
mod errors;
mod interval;
mod poll;
mod sleep;
mod system_clock;
mod test_clock;
mod tick;
mod timeout;
mod timers;
mod timestamp;

pub use clock::Clock;
pub use errors::{Elapsed, Rfc3339Error};
pub use interval::Interval;
pub use poll::Poll;
pub use sleep::Sleep;
pub use system_clock::SystemClock;
pub use test_clock::TestClock;
pub use tick::Tick;
pub use timeout::Timeout;
pub use timestamp::{Timestamp, millis_of};
