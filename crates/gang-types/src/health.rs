//! Load-balancer health checks and client keepalive: timing facts that decide how fast clients
//! leave a frozen controller. Rust owns the definition; Terraform reads its rendered JSON.

use std::num::NonZeroU32;
use std::time::Duration;

/// The health check every load-balancer target group runs against a controller's health listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheck {
    serve_path: &'static str,
    leader_path: &'static str,
    interval: Duration,
    timeout: Duration,
    threshold: NonZeroU32,
}

impl HealthCheck {
    /// The path the serve frontend checks: 200 while the controller serves.
    #[must_use]
    pub const fn serve_path(&self) -> &'static str {
        self.serve_path
    }

    /// The path the execute and workers frontends check: 200 only on the leader.
    #[must_use]
    pub const fn leader_path(&self) -> &'static str {
        self.leader_path
    }

    /// Time between checks of one target.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.interval
    }

    /// A check that does not answer within this fails.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Consecutive results needed to change a target's health (healthy and unhealthy alike).
    #[must_use]
    pub const fn threshold(&self) -> NonZeroU32 {
        self.threshold
    }

    /// Longest time from a target starting to answer 200 until it is healthy: threshold × interval.
    #[must_use]
    pub const fn healthy_window(&self) -> Duration {
        self.interval.saturating_mul(self.threshold.get())
    }
}

/// The only definition: Terraform reads its JSON rendering, kept equal by a test.
const HEALTH_CHECK: HealthCheck = HealthCheck {
    serve_path: "/serve",
    leader_path: "/leader",
    interval: Duration::from_secs(5),
    timeout: Duration::from_secs(2),
    threshold: NonZeroU32::MIN.saturating_add(1),
};

/// The health check shared with the Terraform deployment.
#[must_use]
pub const fn health_check() -> &'static HealthCheck {
    &HEALTH_CHECK
}

/// Bazel's `--grpc_keepalive_time`: a ping after this much read silence while a call is pending
/// (gRPC Java's minimum is 10 s).
pub const CLIENT_KEEPALIVE_TIME: Duration = Duration::from_secs(10);
/// Bazel's `--grpc_keepalive_timeout`: an unanswered ping drops the connection after this long. It
/// must outlast a leader takeover plus the new leader's healthy window (gang-config checks it).
pub const CLIENT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bazel's `--remote_retry_max_delay`: the longest wait between remote retries during a controller
/// hand-off or crash take-over.
pub const CLIENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
/// Time for a resubmitted call to reach the new leader's scheduler and be published.
const REROUTE_PUBLICATION_MARGIN: Duration = Duration::from_secs(5);
/// How long after a controller is lost the demand its clients held may take to reach the new
/// leader: the keepalive ping that finds it gone, the ping's timeout, the longest retry delay,
/// and the publication margin. Before then a new leader cannot tell an idle worker from one whose
/// demand is still rerouting.
pub const CLIENT_REROUTE_BOUND: Duration = CLIENT_KEEPALIVE_TIME
    .saturating_add(CLIENT_KEEPALIVE_TIMEOUT)
    .saturating_add(CLIENT_RETRY_MAX_DELAY)
    .saturating_add(REROUTE_PUBLICATION_MARGIN);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_health_check_values() {
        let check = health_check();
        assert_eq!(check.serve_path(), "/serve");
        assert_eq!(check.leader_path(), "/leader");
        assert_eq!(check.interval(), Duration::from_secs(5));
        assert_eq!(check.timeout(), Duration::from_secs(2));
        assert_eq!(check.threshold().get(), 2);
        assert_eq!(check.healthy_window(), Duration::from_secs(10));
    }

    #[test]
    fn keepalive_timeout_outlasts_default_takeover_and_healthy_window() {
        let ttl = Duration::from_secs(10);
        let skew = Duration::from_secs(2);
        let renew = Duration::from_secs(3);
        assert!(CLIENT_KEEPALIVE_TIMEOUT > ttl + skew + renew + health_check().healthy_window());
    }

    #[test]
    fn keepalive_time_meets_grpc_java_minimum() {
        assert!(CLIENT_KEEPALIVE_TIME >= Duration::from_secs(10));
    }

    #[test]
    fn terraform_health_check_json_renders_the_constants() {
        let actual: serde_json::Value = serde_json::from_str(include_str!(
            "../../../infra/terraform/modules/load-balancer/health-check.json"
        ))
        .expect("health-check.json is valid JSON");
        let check = health_check();
        let expected = serde_json::json!({
            "serve_path": check.serve_path(),
            "leader_path": check.leader_path(),
            "interval_seconds": check.interval().as_secs(),
            "timeout_seconds": check.timeout().as_secs(),
            "threshold": check.threshold().get(),
        });
        assert_eq!(
            actual,
            expected,
            "health-check.json must render these constants:\n{}",
            serde_json::to_string_pretty(&expected).expect("health check JSON can be rendered")
        );
    }
}
