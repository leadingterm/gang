//! Certificate validity time, read from the injected clock rather than the host.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Clock;
use rustls::time_provider::TimeProvider;
use rustls_pki_types::UnixTime;

/// Supplies rustls with the wall time of an injected [`Clock`], so every certificate validity
/// check follows that clock.
struct ClockTimeProvider {
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for ClockTimeProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClockTimeProvider").finish_non_exhaustive()
    }
}

impl TimeProvider for ClockTimeProvider {
    fn current_time(&self) -> Option<UnixTime> {
        let millis = self.clock.wall().as_unix_millis();
        Some(UnixTime::since_unix_epoch(Duration::from_millis(millis)))
    }
}

/// The rustls time source that reads `clock`.
pub(crate) fn time_provider(clock: Arc<dyn Clock>) -> Arc<dyn TimeProvider> {
    Arc::new(ClockTimeProvider { clock })
}

#[cfg(test)]
mod tests {
    use gang_clock::{TestClock, Timestamp};

    use super::*;

    /// A provider reading a clock that starts at `millis` after the Unix epoch.
    fn provider_at(millis: u64) -> (Arc<TestClock>, Arc<dyn TimeProvider>) {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(millis)));
        let provider = time_provider(clock.clone());
        (clock, provider)
    }

    #[test]
    fn current_time_truncates_the_clock_to_whole_seconds() {
        let seconds = |millis| provider_at(millis).1.current_time().map(|t| t.as_secs());
        assert_eq!(seconds(0), Some(0));
        assert_eq!(seconds(999), Some(0));
        assert_eq!(seconds(1_000), Some(1));
        assert_eq!(seconds(1_999), Some(1));
    }

    #[test]
    fn current_time_at_the_latest_representable_timestamp() {
        let (_clock, provider) = provider_at(Timestamp::END_OF_TIME.as_unix_millis());
        let expected = Timestamp::END_OF_TIME.as_unix_millis() / 1_000;
        assert_eq!(provider.current_time().map(|t| t.as_secs()), Some(expected));
    }

    #[test]
    fn current_time_follows_the_clock_as_it_advances() {
        let (clock, provider) = provider_at(999);
        assert_eq!(provider.current_time().map(|t| t.as_secs()), Some(0));
        clock.advance(Duration::from_millis(1));
        assert_eq!(provider.current_time().map(|t| t.as_secs()), Some(1));
    }
}
