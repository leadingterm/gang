//! The `[worker]` section: settings of `gang worker` that are not plugin choices.

use std::time::Duration;

use gang_types::{NonZeroBytes, NonZeroDuration, Percent};
use serde::{Deserialize, Serialize};

/// The default share of the scratch volume given to the fast CAS: 50 percent.
const DEFAULT_FAST_CAS_PERCENT: Percent = Percent::from_percent(50);

/// The default point in a certificate's lifetime at which it is refreshed: 75 percent.
const DEFAULT_CERTIFICATE_REFRESH_PERCENT: Percent = Percent::from_percent(75);

/// The default lead time of a preemption stop: 10 seconds.
const DEFAULT_PREEMPTION_MARGIN: Duration = Duration::from_secs(10);

/// The default interval at which the worker polls the node lifecycle port: 5 seconds.
const DEFAULT_LIFECYCLE_POLL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(5);

/// The default wait between failed attempts to obtain the worker's next certificate: 10
/// seconds.
const DEFAULT_ENROLLMENT_RETRY_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The default time `gang worker` keeps retrying a rate-limited first enrollment: 5 minutes.
const DEFAULT_ENROLLMENT_DEADLINE: NonZeroDuration = NonZeroDuration::from_mins(5);

/// The default timeout for one enrollment request: 10 seconds.
const DEFAULT_ENROLLMENT_REQUEST_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The default maximum enrollment response body size: 1 MiB.
const DEFAULT_ENROLLMENT_MAX_RESPONSE_SIZE: NonZeroBytes = NonZeroBytes::mib(1);

/// Settings of `gang worker`. Every field has a default, so the section may be omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerConfig {
    /// The share of the scratch volume's capacity given to the worker's fast CAS as its
    /// eviction budget, in percent from 1 to 100. Defaults to 50.
    pub fast_cas_percent: Percent,
    /// The worker refreshes its certificate once this percent of its lifetime has elapsed,
    /// in percent from 1 to 100. Defaults to 75.
    pub certificate_refresh_percent: Percent,
    /// How long before a preemption deadline the worker stops. The embedded worker ends and
    /// the scheduler requeues its in-flight actions. Defaults to 10 seconds.
    #[serde(with = "gang_types::duration")]
    pub preemption_margin: Duration,
    /// How often `gang worker` polls the node lifecycle port (`NodeLifecyclePort::preemption_notice`
    /// and `NodeLifecyclePort::parked`). Defaults to 5 seconds.
    pub lifecycle_poll_interval: NonZeroDuration,
    /// How long `gang worker` waits between failed attempts to obtain its next certificate.
    /// Defaults to 10 seconds.
    pub enrollment_retry_interval: NonZeroDuration,
    /// How long `gang worker` keeps retrying a rate-limited first enrollment before it gives
    /// up. Defaults to 5 minutes.
    pub enrollment_deadline: NonZeroDuration,
    /// The total time allowed for one enrollment request, including reading the response.
    /// Defaults to 10 seconds.
    pub enrollment_request_timeout: NonZeroDuration,
    /// The largest enrollment response body accepted. Defaults to 1 MiB.
    pub enrollment_max_response_size: NonZeroBytes,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            fast_cas_percent: DEFAULT_FAST_CAS_PERCENT,
            certificate_refresh_percent: DEFAULT_CERTIFICATE_REFRESH_PERCENT,
            preemption_margin: DEFAULT_PREEMPTION_MARGIN,
            lifecycle_poll_interval: DEFAULT_LIFECYCLE_POLL_INTERVAL,
            enrollment_retry_interval: DEFAULT_ENROLLMENT_RETRY_INTERVAL,
            enrollment_deadline: DEFAULT_ENROLLMENT_DEADLINE,
            enrollment_request_timeout: DEFAULT_ENROLLMENT_REQUEST_TIMEOUT,
            enrollment_max_response_size: DEFAULT_ENROLLMENT_MAX_RESPONSE_SIZE,
        }
    }
}
