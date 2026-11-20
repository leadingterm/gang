//! Internal configuration of the embedded scheduler adapter.

use gang_config::{
    DEFAULT_WORKER_TIMEOUT, Http2Config, OpenFileLimit, ServerConfig, WholeSeconds, WorkerConfig,
};
use gang_types::{NonZeroDuration, Url};

use crate::errors::NativeLinkConfigError;

/// Role-specific settings needed by the embedded scheduler adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLinkConfig {
    /// The URL workers dial to reach the controller, resolved before construction.
    pub worker_endpoint: Option<Url>,
    /// The process open-file limit.
    pub max_open_files: OpenFileLimit,
    /// How long a worker may be silent before the controller drops it.
    pub worker_timeout: WholeSeconds,
    /// Worker connection and execution settings.
    pub worker: WorkerConfig,
    /// Controller HTTP/2 settings.
    pub http2: Http2Config,
    /// Control API deadline, available only for controller-role adapters.
    pub request_timeout: Option<NonZeroDuration>,
}

impl Default for NativeLinkConfig {
    fn default() -> Self {
        Self {
            worker_endpoint: None,
            max_open_files: OpenFileLimit::default(),
            worker_timeout: DEFAULT_WORKER_TIMEOUT,
            worker: WorkerConfig::default(),
            http2: Http2Config::default(),
            request_timeout: None,
        }
    }
}

impl NativeLinkConfig {
    /// Constructs controller settings from the resolved endpoint and server configuration.
    #[must_use]
    pub fn for_controller(worker_endpoint: Url, server: &ServerConfig) -> Self {
        Self {
            worker_endpoint: Some(worker_endpoint),
            max_open_files: server.max_open_files,
            worker_timeout: server.worker_timeout,
            worker: WorkerConfig::default(),
            http2: server.http2.clone(),
            request_timeout: Some(server.request_timeout),
        }
    }

    /// Constructs worker settings from the endpoint and worker configuration.
    #[must_use]
    pub fn for_worker(worker_endpoint: Option<Url>, worker: &WorkerConfig) -> Self {
        Self {
            worker_endpoint,
            max_open_files: worker.max_open_files,
            worker: worker.clone(),
            ..Self::default()
        }
    }

    /// Consumes the resolved worker endpoint during adapter construction.
    pub(crate) fn take_worker_endpoint(&mut self) -> Result<Url, NativeLinkConfigError> {
        self.worker_endpoint
            .take()
            .ok_or(NativeLinkConfigError::WorkerEndpointUnresolved)
    }

    /// Checks an endpoint, including a CLI override supplied by the host.
    pub(crate) fn validate(&self) -> Result<(), NativeLinkConfigError> {
        if let Some(url) = &self.worker_endpoint {
            if !matches!(url.scheme(), "grpc" | "grpcs") {
                return Err(NativeLinkConfigError::EndpointScheme {
                    endpoint: "worker_endpoint",
                    scheme: url.scheme().to_owned(),
                });
            }
            if url.host_str().is_none_or(str::is_empty) {
                return Err(NativeLinkConfigError::EndpointHost("worker_endpoint"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_domain_defaults() {
        let config = NativeLinkConfig::default();
        assert!(config.worker_endpoint.is_none());
        assert!(config.request_timeout.is_none());
        assert_eq!(config.max_open_files, OpenFileLimit::default());
        assert_eq!(config.worker_timeout, WholeSeconds::from_units(10));
        assert_eq!(config.worker, WorkerConfig::default());
        assert_eq!(config.http2, Http2Config::default());
    }

    #[test]
    fn controller_copies_only_controller_settings() {
        let mut server: ServerConfig = serde_json::from_value(serde_json::json!({
            "transport": "plaintext"
        }))
        .expect("valid server config");
        server.max_open_files = OpenFileLimit::try_from(8192).expect("valid limit");
        server.worker_timeout = WholeSeconds::from_units(17);
        server.request_timeout = NonZeroDuration::from_secs(7);
        server.http2.max_message_size = gang_types::NonZeroBytes::mib(8);
        let endpoint: Url = "grpc://localhost:9002".parse().expect("valid URL");
        let config = NativeLinkConfig::for_controller(endpoint.clone(), &server);
        assert_eq!(config.worker_endpoint, Some(endpoint));
        assert_eq!(config.max_open_files, server.max_open_files);
        assert_eq!(config.worker_timeout, server.worker_timeout);
        assert_eq!(config.http2, server.http2);
        assert_eq!(config.request_timeout, Some(server.request_timeout));
        assert_eq!(config.worker, WorkerConfig::default());
    }

    #[test]
    fn worker_copies_worker_settings_and_defaults_controller_settings() {
        let worker = WorkerConfig {
            max_open_files: OpenFileLimit::try_from(8192).expect("valid limit"),
            connect_timeout: WholeSeconds::from_units(7),
            ..WorkerConfig::default()
        };
        let endpoint: Url = "grpcs://localhost:9002".parse().expect("valid URL");
        let config = NativeLinkConfig::for_worker(Some(endpoint.clone()), &worker);
        assert_eq!(config.worker_endpoint, Some(endpoint));
        assert_eq!(config.max_open_files, worker.max_open_files);
        assert_eq!(config.worker, worker);
        assert_eq!(config.worker_timeout, WholeSeconds::from_units(10));
        assert_eq!(config.http2, Http2Config::default());
        assert!(config.request_timeout.is_none());
        assert!(
            NativeLinkConfig::for_worker(None, &worker)
                .worker_endpoint
                .is_none()
        );
    }

    #[test]
    fn validation_checks_endpoint_scheme_and_host() {
        for endpoint in ["grpc://localhost:9002", "grpcs://localhost:9002"] {
            let config = NativeLinkConfig::for_worker(
                Some(endpoint.parse().expect("valid URL")),
                &WorkerConfig::default(),
            );
            assert!(config.validate().is_ok());
        }
        let mut config = NativeLinkConfig {
            worker_endpoint: Some("https://localhost:9002".parse().expect("valid URL")),
            ..NativeLinkConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(NativeLinkConfigError::EndpointScheme { .. })
        ));
        config.worker_endpoint = Some("grpc:///worker".parse().expect("valid URL"));
        assert!(matches!(
            config.validate(),
            Err(NativeLinkConfigError::EndpointHost(_))
        ));
    }
}
