//! Shared timing and refusal policy for cooperative controller handover.

use std::time::Duration;

use crate::health::health_check;

/// Default interval between leadership renewals.
pub const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(1);
/// Default minimum remaining validity before a leadership-authorized mutation.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(1);
/// Marks a refusal made before the request reached its handler.
pub const REQUEST_NOT_STARTED_HEADER: &str = "gang-request-not-started";

/// Retry budget of the minimum-supported CLI, shipped before cooperative handover.
pub const MINIMUM_CLI_RETRY_BUDGET: Duration = Duration::from_secs(21);
/// Initial exponential delay of that CLI.
pub const MINIMUM_CLI_INITIAL_DELAY: Duration = Duration::from_millis(100);
/// Maximum exponential delay of that CLI.
pub const MINIMUM_CLI_MAX_DELAY: Duration = Duration::from_secs(5);

/// Latest retry attempt made by the minimum-supported CLI. It stops rather than sleeping
/// when its next delay is greater than or equal to the remaining retry budget.
#[must_use]
pub const fn minimum_cli_last_attempt() -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut delay = MINIMUM_CLI_INITIAL_DELAY;
    while delay.as_nanos() < MINIMUM_CLI_RETRY_BUDGET.saturating_sub(elapsed).as_nanos() {
        elapsed = elapsed.saturating_add(delay);
        let doubled = delay.saturating_mul(2);
        delay = if doubled.as_nanos() < MINIMUM_CLI_MAX_DELAY.as_nanos() {
            doubled
        } else {
            MINIMUM_CLI_MAX_DELAY
        };
    }
    elapsed
}

/// Bounds the learned-history snapshot taken before releasing leadership.
#[must_use]
pub const fn snapshot_timeout(renew_interval: Duration, call_timeout: Duration) -> Duration {
    if renew_interval.as_nanos() < call_timeout.as_nanos() {
        renew_interval
    } else {
        call_timeout
    }
}

/// Bounds healthy observation, candidate observation, and four handover calls.
#[must_use]
pub const fn handover_window(renew_interval: Duration, call_timeout: Duration) -> Duration {
    health_check()
        .healthy_window()
        .saturating_add(renew_interval)
        .saturating_add(call_timeout.saturating_mul(4))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_retry_and_default_handover_have_a_call_cushion() {
        assert_eq!(minimum_cli_last_attempt(), Duration::from_millis(16_300));
        let planned = handover_window(DEFAULT_RENEW_INTERVAL, DEFAULT_CALL_TIMEOUT);
        assert_eq!(planned, Duration::from_secs(15));
        assert!(planned + DEFAULT_CALL_TIMEOUT < minimum_cli_last_attempt());
    }

    #[test]
    fn window_uses_all_four_calls_and_saturates() {
        assert_eq!(
            handover_window(Duration::from_millis(500), Duration::from_millis(250)),
            Duration::from_millis(11_500)
        );
        assert_eq!(handover_window(Duration::MAX, Duration::MAX), Duration::MAX);
        assert_eq!(
            snapshot_timeout(Duration::from_secs(2), Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }
}
