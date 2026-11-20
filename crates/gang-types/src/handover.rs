//! Shared timing and refusal policy for cooperative controller handover.

use std::time::Duration;

use crate::health::{CLIENT_RETRY_MAX_DELAY, health_check};

/// Default interval between leadership renewals.
pub const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(3);
/// Default minimum remaining validity before a leadership-authorized mutation.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(2);
/// Allowance for the successor to promote after a cooperative yield.
pub const PROMOTION_ALLOWANCE: Duration = Duration::from_secs(1);
/// Retry allowance included in the handover window.
pub const RETRY_MARGIN: Duration = CLIENT_RETRY_MAX_DELAY;
/// Marks a refusal made before the request reached its handler.
pub const REQUEST_NOT_STARTED_HEADER: &str = "gang-request-not-started";

/// Bounds the learned-history snapshot taken before releasing leadership.
#[must_use]
pub const fn snapshot_timeout(renew_interval: Duration, call_timeout: Duration) -> Duration {
    if renew_interval.as_nanos() < call_timeout.as_nanos() {
        renew_interval
    } else {
        call_timeout
    }
}

/// The load-balancer warmup covering readiness, snapshot, yield, promotion and retries.
#[must_use]
pub const fn handover_window(renew_interval: Duration, call_timeout: Duration) -> Duration {
    health_check()
        .healthy_window()
        .saturating_add(renew_interval)
        .saturating_add(snapshot_timeout(renew_interval, call_timeout))
        .saturating_add(PROMOTION_ALLOWANCE)
        .saturating_add(RETRY_MARGIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_window_tracks_policy() {
        assert_eq!(
            handover_window(DEFAULT_RENEW_INTERVAL, DEFAULT_CALL_TIMEOUT),
            Duration::from_secs(21)
        );
        assert_eq!(
            handover_window(Duration::from_secs(7), DEFAULT_CALL_TIMEOUT),
            health_check().healthy_window()
                + Duration::from_secs(7)
                + DEFAULT_CALL_TIMEOUT
                + PROMOTION_ALLOWANCE
                + RETRY_MARGIN
        );
    }

    #[test]
    fn snapshot_selects_the_smaller_bound() {
        assert_eq!(
            snapshot_timeout(DEFAULT_RENEW_INTERVAL, DEFAULT_CALL_TIMEOUT),
            DEFAULT_CALL_TIMEOUT
        );
        assert_eq!(
            snapshot_timeout(Duration::from_secs(1), DEFAULT_CALL_TIMEOUT),
            Duration::from_secs(1)
        );
        assert_eq!(
            snapshot_timeout(DEFAULT_CALL_TIMEOUT, DEFAULT_CALL_TIMEOUT),
            DEFAULT_CALL_TIMEOUT
        );
        assert_eq!(
            snapshot_timeout(Duration::ZERO, DEFAULT_CALL_TIMEOUT),
            Duration::ZERO
        );
    }

    #[test]
    fn custom_window_preserves_subsecond_snapshot_bounds() {
        let renew_interval = Duration::from_millis(750);
        let call_timeout = Duration::from_millis(1250);
        assert_eq!(
            snapshot_timeout(renew_interval, call_timeout),
            renew_interval
        );
        assert_eq!(
            snapshot_timeout(call_timeout, renew_interval),
            renew_interval
        );
        assert_eq!(
            handover_window(renew_interval, call_timeout),
            health_check().healthy_window()
                + renew_interval
                + renew_interval
                + PROMOTION_ALLOWANCE
                + RETRY_MARGIN
        );
    }

    #[test]
    fn window_saturates_instead_of_overflowing() {
        assert_eq!(
            snapshot_timeout(Duration::MAX, DEFAULT_CALL_TIMEOUT),
            DEFAULT_CALL_TIMEOUT
        );
        assert_eq!(
            snapshot_timeout(Duration::MAX, Duration::MAX),
            Duration::MAX
        );
        assert_eq!(handover_window(Duration::MAX, Duration::MAX), Duration::MAX);
        let half_max = Duration::MAX / 2;
        assert_eq!(handover_window(half_max, half_max), Duration::MAX);
        assert_eq!(
            handover_window(Duration::MAX, DEFAULT_CALL_TIMEOUT),
            Duration::MAX
        );
    }
}
