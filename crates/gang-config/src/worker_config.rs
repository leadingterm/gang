//! The `[worker]` section: settings of `gang worker` that are not plugin choices.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{Bytes, NonZeroBytes, NonZeroDuration, Percent};
use serde::{Deserialize, Serialize};

use crate::{OpenFileLimit, WholeSeconds};

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

/// The default number of concurrent blob downloads on a worker: 64.
const DEFAULT_DOWNLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// The default maximum individual blob size eligible for batched CAS transfers: 1 MiB.
const DEFAULT_BATCH_BLOB_MAX: NonZeroBytes = NonZeroBytes::mib(1);

/// The upper bound on the default `hash_concurrency` (`min(vCPUs, 4)`).
const MAX_DEFAULT_HASH_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(4).expect("4 is non-zero");

/// The default minimum blob size eligible for compression on upload: 4 KiB.
const DEFAULT_COMPRESS_MIN_SIZE: Bytes = Bytes::kib(4);

/// The default in-memory directory cache budget on a worker: 64 MiB.
const DEFAULT_DIRECTORY_CACHE_BUDGET: NonZeroBytes = NonZeroBytes::mib(64);

/// The default minimum file count for an input tree to enter the worker tree cache: 512.
const DEFAULT_TREE_CACHE_MIN_FILES: NonZeroUsize = NonZeroUsize::new(512).expect("512 is non-zero");

/// The default maximum number of cached input trees on a worker: 256.
const DEFAULT_TREE_CACHE_MAX_TREES: NonZeroUsize = NonZeroUsize::new(256).expect("256 is non-zero");

/// The default share of the tree cache budget pinned for hot trees: 50 percent.
const DEFAULT_TREE_PIN_PERCENT: Percent = Percent::from_percent(50);

/// The default number of background graveyard deletion workers: 2.
const DEFAULT_GRAVEYARD_WORKERS: NonZeroUsize = NonZeroUsize::new(2).expect("2 is non-zero");

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
    /// Process open-file limit. Defaults to 24,576.
    pub max_open_files: OpenFileLimit,
    /// Longest time the outputs of one action may take to upload. Defaults to 10 minutes.
    pub upload_timeout: WholeSeconds,
    /// Longest time the inputs of one action may take to download. Defaults to 10 minutes.
    pub download_timeout: WholeSeconds,
    /// Longest time to connect to the controller and for each request. The worker's scheduler
    /// keepalive interval is half this value; it must be below twice `server.worker_timeout`.
    /// Defaults to 5 seconds.
    pub connect_timeout: WholeSeconds,
    /// Time to wait for the last action directory to be cleaned up. Defaults to 30 seconds.
    pub cleanup_wait: WholeSeconds,
    /// Interval of HTTP/2 keepalive pings from the worker. Defaults to 30 seconds.
    pub keepalive_interval: WholeSeconds,
    /// Maximum concurrent blob downloads on the worker. Defaults to 64.
    pub download_concurrency: NonZeroUsize,
    /// Maximum individual blob size eligible for batched CAS transfers. Defaults to 1 MiB.
    pub batch_blob_max: NonZeroBytes,
    /// Maximum concurrent CPU-bound upload hashing and compression tasks. Defaults to unset,
    /// which resolves to `min(vCPUs, 4)` via [`Self::effective_hash_concurrency`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_concurrency: Option<NonZeroUsize>,
    /// Minimum blob size eligible for compression on upload. Defaults to 4 KiB.
    pub compress_min_size: Bytes,
    /// In-memory decoded directory cache budget. Defaults to 64 MiB.
    pub directory_cache_budget: NonZeroBytes,
    /// Minimum file count for an input tree to enter the worker tree cache. Defaults to 512.
    pub tree_cache_min_files: NonZeroUsize,
    /// Maximum number of cached input trees on the worker. Defaults to 256.
    pub tree_cache_max_trees: NonZeroUsize,
    /// Percentage (1–100) of cached trees pinned against eviction. Defaults to 50.
    pub tree_pin_percent: Percent,
    /// Number of background graveyard deletion workers. Defaults to 2.
    pub graveyard_workers: NonZeroUsize,
}

impl WorkerConfig {
    /// The maximum time for an attempt, including download, execution and upload.
    #[must_use]
    pub fn attempt_budget(&self, action_timeout: WholeSeconds) -> Duration {
        action_timeout
            .get()
            .saturating_add(self.download_timeout.get())
            .saturating_add(self.upload_timeout.get())
    }

    /// Maximum concurrent CPU-bound upload hashing and compression tasks: the explicit
    /// `hash_concurrency`, else `min(vCPUs, 4)` from host parallelism (falling back to `1` if
    /// parallelism cannot be queried).
    #[must_use]
    pub fn effective_hash_concurrency(&self) -> NonZeroUsize {
        let available = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        self.hash_concurrency_for(available)
    }

    /// Maximum concurrent CPU-bound upload hashing and compression tasks for a host with
    /// `vcpus` logical CPUs: the explicit `hash_concurrency`, else `min(vcpus, 4)`.
    #[must_use]
    pub fn hash_concurrency_for(&self, vcpus: NonZeroUsize) -> NonZeroUsize {
        self.hash_concurrency
            .unwrap_or_else(|| vcpus.min(MAX_DEFAULT_HASH_CONCURRENCY))
    }
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
            max_open_files: OpenFileLimit::default(),
            upload_timeout: WholeSeconds::from_units(10 * 60),
            download_timeout: WholeSeconds::from_units(10 * 60),
            connect_timeout: WholeSeconds::from_units(5),
            cleanup_wait: WholeSeconds::from_units(30),
            keepalive_interval: WholeSeconds::from_units(30),
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
            batch_blob_max: DEFAULT_BATCH_BLOB_MAX,
            hash_concurrency: None,
            compress_min_size: DEFAULT_COMPRESS_MIN_SIZE,
            directory_cache_budget: DEFAULT_DIRECTORY_CACHE_BUDGET,
            tree_cache_min_files: DEFAULT_TREE_CACHE_MIN_FILES,
            tree_cache_max_trees: DEFAULT_TREE_CACHE_MAX_TREES,
            tree_pin_percent: DEFAULT_TREE_PIN_PERCENT,
            graveyard_workers: DEFAULT_GRAVEYARD_WORKERS,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use gang_types::{Bytes, NonZeroBytes, Percent};

    use super::WorkerConfig;
    use crate::WholeSeconds;

    #[test]
    fn attempt_budget_includes_both_transfers() {
        let worker = WorkerConfig {
            download_timeout: WholeSeconds::from_units(12),
            upload_timeout: WholeSeconds::from_units(17),
            ..WorkerConfig::default()
        };
        assert_eq!(
            worker.attempt_budget(WholeSeconds::from_units(31)),
            std::time::Duration::from_mins(1)
        );
    }

    #[test]
    fn worker_connection_settings_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let config: WorkerConfig = toml::from_str(
            "max_open_files = 8192\nupload_timeout = '11s'\ndownload_timeout = '12s'\nconnect_timeout = '3s'\ncleanup_wait = '14s'\nkeepalive_interval = '15s'",
        )?;
        assert_eq!(config.max_open_files.get(), 8192);
        assert_eq!(config.upload_timeout, WholeSeconds::from_units(11));
        assert_eq!(config.download_timeout, WholeSeconds::from_units(12));
        assert_eq!(config.connect_timeout, WholeSeconds::from_units(3));
        assert_eq!(config.cleanup_wait, WholeSeconds::from_units(14));
        assert_eq!(config.keepalive_interval, WholeSeconds::from_units(15));
        assert_eq!(
            toml::from_str::<WorkerConfig>(&toml::to_string(&config)?)?,
            config
        );
        Ok(())
    }

    #[test]
    fn worker_connection_settings_reject_invalid_values() {
        assert!(toml::from_str::<WorkerConfig>("connect_timeout = '0s'").is_err());
        assert!(toml::from_str::<WorkerConfig>("upload_timeout = '1500ms'").is_err());
        assert!(toml::from_str::<WorkerConfig>("max_open_files = 7").is_err());
        assert!(toml::from_str::<WorkerConfig>("unknown = 1").is_err());
    }

    #[test]
    fn native_worker_tuning_defaults_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let default: WorkerConfig = toml::from_str("")?;
        assert_eq!(default.download_concurrency.get(), 64);
        assert_eq!(default.batch_blob_max, NonZeroBytes::mib(1));
        assert_eq!(default.hash_concurrency, None);
        assert_eq!(default.compress_min_size, Bytes::kib(4));
        assert_eq!(default.directory_cache_budget, NonZeroBytes::mib(64));
        assert_eq!(default.tree_cache_min_files.get(), 512);
        assert_eq!(default.tree_cache_max_trees.get(), 256);
        assert_eq!(default.tree_pin_percent, Percent::from_percent(50));
        assert_eq!(default.graveyard_workers.get(), 2);

        let custom: WorkerConfig = toml::from_str(
            "\
download_concurrency = 16
batch_blob_max = '512KiB'
hash_concurrency = 6
compress_min_size = '0B'
directory_cache_budget = '128MiB'
tree_cache_min_files = 1024
tree_cache_max_trees = 128
tree_pin_percent = 25
graveyard_workers = 4
",
        )?;
        assert_eq!(custom.download_concurrency.get(), 16);
        assert_eq!(custom.batch_blob_max, NonZeroBytes::kib(512));
        assert_eq!(
            custom.hash_concurrency,
            Some(NonZeroUsize::new(6).expect("6 is non-zero"))
        );
        assert_eq!(custom.compress_min_size, Bytes::new(0));
        assert_eq!(custom.directory_cache_budget, NonZeroBytes::mib(128));
        assert_eq!(custom.tree_cache_min_files.get(), 1024);
        assert_eq!(custom.tree_cache_max_trees.get(), 128);
        assert_eq!(custom.tree_pin_percent, Percent::from_percent(25));
        assert_eq!(custom.graveyard_workers.get(), 4);
        assert_eq!(
            toml::from_str::<WorkerConfig>(&toml::to_string(&custom)?)?,
            custom
        );
        Ok(())
    }

    #[test]
    fn hash_concurrency_defaults_to_min_vcpus_four() {
        let default = WorkerConfig::default();
        assert_eq!(
            default
                .hash_concurrency_for(NonZeroUsize::new(1).expect("1 is non-zero"))
                .get(),
            1
        );
        assert_eq!(
            default
                .hash_concurrency_for(NonZeroUsize::new(2).expect("2 is non-zero"))
                .get(),
            2
        );
        assert_eq!(
            default
                .hash_concurrency_for(NonZeroUsize::new(4).expect("4 is non-zero"))
                .get(),
            4
        );
        assert_eq!(
            default
                .hash_concurrency_for(NonZeroUsize::new(16).expect("16 is non-zero"))
                .get(),
            4
        );
        let effective = default.effective_hash_concurrency().get();
        assert!((1..=4).contains(&effective), "{effective}");

        let explicit = WorkerConfig {
            hash_concurrency: Some(NonZeroUsize::new(8).expect("8 is non-zero")),
            ..WorkerConfig::default()
        };
        assert_eq!(
            explicit
                .hash_concurrency_for(NonZeroUsize::new(2).expect("2 is non-zero"))
                .get(),
            8
        );
        assert_eq!(explicit.effective_hash_concurrency().get(), 8);
    }

    #[test]
    fn native_worker_tuning_rejects_invalid_values() {
        for input in [
            "download_concurrency = 0",
            "batch_blob_max = '0B'",
            "hash_concurrency = 0",
            "directory_cache_budget = '0B'",
            "tree_cache_min_files = 0",
            "tree_cache_max_trees = 0",
            "tree_pin_percent = 0",
            "tree_pin_percent = 101",
            "graveyard_workers = 0",
        ] {
            assert!(
                toml::from_str::<WorkerConfig>(input).is_err(),
                "accepted {input}"
            );
        }
    }
}
