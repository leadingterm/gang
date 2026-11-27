//! Configuration for the native execution [`Engine`](crate::Engine) (`§9`).

use std::num::NonZeroUsize;

use gang_config::{
    DEFAULT_WORKER_TIMEOUT, Http2Config, OpenFileLimit, ServerConfig, WholeSeconds, WorkerConfig,
};
use gang_types::{Bytes, NonZeroBytes, NonZeroDuration, Url};

use crate::actor::{
    DEFAULT_ASSIGN_INLINE_MAX, DEFAULT_SCHEDULER_BATCH, DEFAULT_SCHEDULER_QUEUE_DEPTH,
    DEFAULT_WORKER_OUTBOUND_DEPTH,
};

/// Default timeout before an unacknowledged `Assign` triggers worker-loss retry (`10s`).
const DEFAULT_DISPATCH_ACK_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// Default maximum non-fitting queued entries skipped per worker-driven dispatch scan (`256`).
const DEFAULT_DISPATCH_SCAN_LIMIT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(255);

/// Default total bytes of CAS blobs allowed in flight simultaneously (`1 GiB`).
const DEFAULT_MAX_INFLIGHT_BLOB_BYTES: NonZeroBytes = NonZeroBytes::gib(1);

/// Default `ByteStream.Read` response chunk size (`64 KiB`).
const DEFAULT_READ_CHUNK: NonZeroBytes = NonZeroBytes::kib(64);

/// Errors validating or resolving [`EngineConfig`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, gang_spi::PortFailure)]
pub enum EngineConfigError {
    /// Only `grpc` and `grpcs` schemes can be dialed by workers.
    #[error("{endpoint} must use grpc or grpcs, not `{scheme}`")]
    #[failure(config)]
    EndpointScheme {
        /// The configuration field holding the endpoint.
        endpoint: &'static str,
        /// The unsupported scheme found on the URL.
        scheme: String,
    },
    /// An endpoint without a host cannot be dialed.
    #[error("{0} has no host")]
    #[failure(config)]
    EndpointHost(&'static str),
    /// `worker_endpoint` was omitted when constructing an [`Engine`](crate::Engine).
    #[error(
        "worker_endpoint is required for gang worker; set worker_endpoint or --worker-endpoint"
    )]
    #[failure(config)]
    WorkerEndpointUnresolved,
    /// The worker endpoint scheme does not match the worker's TLS configuration.
    #[error("worker_endpoint must use {expected}")]
    #[failure(config)]
    WorkerEndpointTls {
        /// Expected URL scheme (`grpc` or `grpcs`).
        expected: &'static str,
    },
}

/// Role-specific settings needed to run the native execution [`Engine`](crate::Engine) (`§9`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// The URL workers dial to reach the controller, resolved before construction.
    pub worker_endpoint: Option<Url>,
    /// The process open-file limit.
    pub max_open_files: OpenFileLimit,
    /// How long a worker may be silent before the controller drops it.
    pub worker_timeout: WholeSeconds,
    /// Capacity of each scheduler actor inbox (`control_rx`, `worker_rx`, `client_rx`).
    pub scheduler_queue_depth: NonZeroUsize,
    /// Maximum worker or client messages drained per scheduler actor iteration.
    pub scheduler_batch: NonZeroUsize,
    /// Per-worker outbound channel capacity.
    pub worker_outbound_depth: NonZeroUsize,
    /// Timeout before an unacknowledged `Assign` triggers worker-loss retry.
    pub dispatch_ack_timeout: NonZeroDuration,
    /// Maximum non-fitting queued jobs skipped per worker-driven dispatch scan.
    pub dispatch_scan_limit: NonZeroUsize,
    /// Maximum combined `Action` + `Command` bytes inlined in `worker.v1::Assign`.
    pub assign_inline_max: Bytes,
    /// Total bytes of CAS blobs allowed in flight simultaneously across listeners.
    pub max_inflight_blob_bytes: NonZeroBytes,
    /// Target chunk size emitted per `ByteStream.Read` response frame.
    pub read_chunk: NonZeroBytes,
    /// Controller HTTP/2 limits and timeouts.
    pub http2: Http2Config,
    /// Control API deadline, populated for controller-role configs.
    pub request_timeout: Option<NonZeroDuration>,
    /// Worker connection and execution settings.
    pub worker: WorkerConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            worker_endpoint: None,
            max_open_files: OpenFileLimit::default(),
            worker_timeout: DEFAULT_WORKER_TIMEOUT,
            scheduler_queue_depth: DEFAULT_SCHEDULER_QUEUE_DEPTH,
            scheduler_batch: DEFAULT_SCHEDULER_BATCH,
            worker_outbound_depth: DEFAULT_WORKER_OUTBOUND_DEPTH,
            dispatch_ack_timeout: DEFAULT_DISPATCH_ACK_TIMEOUT,
            dispatch_scan_limit: DEFAULT_DISPATCH_SCAN_LIMIT,
            assign_inline_max: DEFAULT_ASSIGN_INLINE_MAX,
            max_inflight_blob_bytes: DEFAULT_MAX_INFLIGHT_BLOB_BYTES,
            read_chunk: DEFAULT_READ_CHUNK,
            http2: Http2Config::default(),
            request_timeout: None,
            worker: WorkerConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Constructs controller settings from the resolved `worker_endpoint` and `[server]` config.
    #[must_use]
    pub fn for_controller(worker_endpoint: Url, server: &ServerConfig) -> Self {
        Self {
            worker_endpoint: Some(worker_endpoint),
            max_open_files: server.max_open_files,
            worker_timeout: server.worker_timeout,
            scheduler_queue_depth: server.scheduler_queue_depth,
            scheduler_batch: server.scheduler_batch,
            worker_outbound_depth: server.worker_outbound_depth,
            dispatch_ack_timeout: server.dispatch_ack_timeout,
            dispatch_scan_limit: server.dispatch_scan_limit,
            assign_inline_max: server.assign_inline_max,
            max_inflight_blob_bytes: server.max_inflight_blob_bytes,
            read_chunk: server.read_chunk,
            http2: server.http2.clone(),
            request_timeout: Some(server.request_timeout),
            worker: WorkerConfig::default(),
        }
    }

    /// Constructs worker settings from the optional `worker_endpoint` and `[worker]` config.
    #[must_use]
    pub fn for_worker(worker_endpoint: Option<Url>, worker: &WorkerConfig) -> Self {
        Self {
            worker_endpoint,
            max_open_files: worker.max_open_files,
            worker: worker.clone(),
            ..Self::default()
        }
    }

    /// Validates configured endpoints (`grpc` or `grpcs` scheme and non-empty host when present).
    ///
    /// # Errors
    ///
    /// Returns [`EngineConfigError::EndpointScheme`] or [`EngineConfigError::EndpointHost`] if
    /// `worker_endpoint` is set to an invalid dial target.
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if let Some(url) = &self.worker_endpoint {
            if !matches!(url.scheme(), "grpc" | "grpcs") {
                return Err(EngineConfigError::EndpointScheme {
                    endpoint: "worker_endpoint",
                    scheme: url.scheme().to_owned(),
                });
            }
            if url.host_str().is_none_or(str::is_empty) {
                return Err(EngineConfigError::EndpointHost("worker_endpoint"));
            }
        }
        Ok(())
    }

    /// Validates the config and extracts the required `worker_endpoint`.
    pub(crate) fn resolve_worker_endpoint(&self) -> Result<Url, EngineConfigError> {
        self.validate()?;
        self.worker_endpoint
            .clone()
            .ok_or(EngineConfigError::WorkerEndpointUnresolved)
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::PortError;
    use gang_testkit::TestResult;

    use super::*;

    #[test]
    fn defaults_match_domain_defaults() {
        let config = EngineConfig::default();
        assert!(config.worker_endpoint.is_none());
        assert_eq!(config.max_open_files, OpenFileLimit::default());
        assert_eq!(config.worker_timeout, WholeSeconds::from_units(10));
        assert_eq!(config.scheduler_queue_depth.get(), 4096);
        assert_eq!(config.scheduler_batch.get(), 64);
        assert_eq!(config.worker_outbound_depth.get(), 256);
        assert_eq!(config.dispatch_ack_timeout, NonZeroDuration::from_secs(10));
        assert_eq!(config.dispatch_scan_limit.get(), 256);
        assert_eq!(config.assign_inline_max, Bytes::kib(64));
        assert_eq!(config.max_inflight_blob_bytes, NonZeroBytes::gib(1));
        assert_eq!(config.read_chunk, NonZeroBytes::kib(64));
        assert_eq!(config.http2, Http2Config::default());
        assert!(config.request_timeout.is_none());
        assert_eq!(config.worker, WorkerConfig::default());
    }

    #[test]
    fn controller_copies_controller_and_native_server_settings() -> TestResult {
        let mut server = gang_testkit::STANDARD_GANG_TOML
            .parse::<gang_config::GangConfig<gang_testkit::OpaquePlugins>>()?
            .server;
        server.transport = gang_config::Transport::Plaintext;
        server.max_open_files = OpenFileLimit::try_from(8192)?;
        server.worker_timeout = WholeSeconds::from_units(17);
        server.scheduler_queue_depth = NonZeroUsize::new(8192).ok_or("nonzero")?;
        server.scheduler_batch = NonZeroUsize::new(128).ok_or("nonzero")?;
        server.worker_outbound_depth = NonZeroUsize::new(512).ok_or("nonzero")?;
        server.dispatch_ack_timeout = NonZeroDuration::from_secs(25);
        server.dispatch_scan_limit = NonZeroUsize::new(1024).ok_or("nonzero")?;
        server.assign_inline_max = Bytes::new(0);
        server.max_inflight_blob_bytes = NonZeroBytes::gib(2);
        server.read_chunk = NonZeroBytes::kib(128);
        server.request_timeout = NonZeroDuration::from_secs(7);
        server.http2.max_message_size = NonZeroBytes::mib(8);

        let endpoint: Url = "grpc://localhost:9002".parse()?;
        let config = EngineConfig::for_controller(endpoint.clone(), &server);
        assert_eq!(config.worker_endpoint, Some(endpoint));
        assert_eq!(config.max_open_files, server.max_open_files);
        assert_eq!(config.worker_timeout, server.worker_timeout);
        assert_eq!(config.scheduler_queue_depth, server.scheduler_queue_depth);
        assert_eq!(config.scheduler_batch, server.scheduler_batch);
        assert_eq!(config.worker_outbound_depth, server.worker_outbound_depth);
        assert_eq!(config.dispatch_ack_timeout, server.dispatch_ack_timeout);
        assert_eq!(config.dispatch_scan_limit, server.dispatch_scan_limit);
        assert_eq!(config.assign_inline_max, server.assign_inline_max);
        assert_eq!(
            config.max_inflight_blob_bytes,
            server.max_inflight_blob_bytes
        );
        assert_eq!(config.read_chunk, server.read_chunk);
        assert_eq!(config.http2, server.http2);
        assert_eq!(config.request_timeout, Some(server.request_timeout));
        assert_eq!(config.worker, WorkerConfig::default());
        Ok(())
    }

    #[test]
    fn worker_copies_worker_settings_and_defaults_controller_settings() -> TestResult {
        let worker = WorkerConfig {
            max_open_files: OpenFileLimit::try_from(8192)?,
            connect_timeout: WholeSeconds::from_units(7),
            ..WorkerConfig::default()
        };
        let endpoint: Url = "grpcs://localhost:9002".parse()?;
        let config = EngineConfig::for_worker(Some(endpoint.clone()), &worker);
        assert_eq!(config.worker_endpoint, Some(endpoint));
        assert_eq!(config.max_open_files, worker.max_open_files);
        assert_eq!(config.worker, worker);
        assert_eq!(config.worker_timeout, WholeSeconds::from_units(10));
        assert_eq!(config.http2, Http2Config::default());
        assert!(config.request_timeout.is_none());
        assert!(
            EngineConfig::for_worker(None, &worker)
                .worker_endpoint
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn validation_checks_endpoint_scheme_and_host() -> TestResult {
        for endpoint in ["grpc://localhost:9002", "grpcs://localhost:9002"] {
            let config =
                EngineConfig::for_worker(Some(endpoint.parse()?), &WorkerConfig::default());
            config.validate()?;
            assert_eq!(config.resolve_worker_endpoint()?.as_str(), endpoint);
        }

        let bad_scheme = EngineConfig {
            worker_endpoint: Some("https://localhost:9002".parse()?),
            ..EngineConfig::default()
        };
        assert!(matches!(
            bad_scheme.validate(),
            Err(EngineConfigError::EndpointScheme { .. })
        ));
        assert!(matches!(
            PortError::from(bad_scheme.validate().err().ok_or("expected error")?),
            PortError::Config(_)
        ));

        let no_host = EngineConfig {
            worker_endpoint: Some("grpc:///worker".parse()?),
            ..EngineConfig::default()
        };
        assert!(matches!(
            no_host.validate(),
            Err(EngineConfigError::EndpointHost(_))
        ));

        let unresolved = EngineConfig::default();
        unresolved.validate()?;
        assert!(matches!(
            unresolved.resolve_worker_endpoint(),
            Err(EngineConfigError::WorkerEndpointUnresolved)
        ));
        Ok(())
    }
}
