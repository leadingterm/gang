//! Error types of the embedded `NativeLink` and their mapping onto `PortError`.

use std::path::PathBuf;

/// Plugin configuration that cannot be used to run `NativeLink`.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub(crate) enum NativeLinkConfigError {
    /// Only plain and TLS gRPC endpoints can be dialed by Bazel and workers.
    #[error("{endpoint} must use grpc or grpcs, not `{scheme}`")]
    #[failure(config)]
    EndpointScheme {
        /// The configuration field that holds the endpoint.
        endpoint: &'static str,
        /// The scheme the endpoint actually uses.
        scheme: String,
    },
    /// An endpoint without a host cannot be dialed.
    #[error("{0} has no host")]
    #[failure(config)]
    EndpointHost(&'static str),
    /// `worker_endpoint` was omitted and the server has not derived it from a worker listener.
    #[error("worker_endpoint is not resolved: the server must derive it from its worker listener")]
    #[failure(config)]
    WorkerEndpointUnresolved,
    /// The worker endpoint's scheme does not match whether TLS is configured.
    #[error("worker_endpoint must use {expected}")]
    #[failure(config)]
    WorkerEndpointTls {
        /// The scheme the worker endpoint must use.
        expected: &'static str,
    },
    /// A duration that `NativeLink` cannot take as configured.
    #[error("{field} {requirement}")]
    #[failure(config)]
    Duration {
        /// The configuration field that holds the duration.
        field: &'static str,
        /// What the field must be.
        requirement: &'static str,
    },
    /// The scheduler settings are not valid JSON for `NativeLink`'s configuration.
    #[error("invalid scheduler settings")]
    #[failure(config)]
    Settings(#[source] serde_json::Error),
    /// A path must be UTF-8 to be written into `NativeLink`'s configuration.
    #[error("path {0:?} is not valid UTF-8")]
    #[failure(config)]
    NotUtf8(PathBuf),
    /// A store specification is not a JSON object with exactly one store variant.
    #[error("invalid store spec: {0}")]
    #[failure(config)]
    StoreSpec(&'static str),
    /// The cold start reserves more than any fleet accepts, so an unmeasured action could never run.
    #[error(
        "cold_start reserves {cpu_count} cpus, {memory_kb} KiB of memory and {disk_kb} KiB of disk, which no fleet accepts: lower [scheduler.nativelink.cold_start] or enlarge a fleet"
    )]
    #[failure(config)]
    ColdStartFitsNoFleet {
        /// The cores the cold start reserves.
        cpu_count: u32,
        /// The memory the cold start reserves, in KiB.
        memory_kb: u64,
        /// The disk the cold start reserves, in KiB.
        disk_kb: u64,
    },
}

/// Failures of the embedded `NativeLink` controller or worker.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub(crate) enum NativeLinkEmbedError {
    /// The plugin configuration cannot be used to run `NativeLink`.
    #[error(transparent)]
    #[failure(config)]
    Config(#[from] NativeLinkConfigError),
    /// A TLS file of the worker cannot be read.
    #[error("cannot read the worker tls file {}", path.display())]
    #[failure(config)]
    WorkerTlsFile {
        /// The file.
        path: PathBuf,
        /// The read error.
        #[source]
        source: std::io::Error,
    },
    /// A TLS file of the worker is not a PEM document of its kind.
    #[error("the worker tls file {} is malformed", path.display())]
    #[failure(config)]
    WorkerTlsPem {
        /// The file.
        path: PathBuf,
        /// The parsing error.
        #[source]
        source: gang_types::PemError,
    },
    /// The worker's TLS client cannot be built from its files.
    #[error("the worker tls client cannot be built")]
    #[failure(config)]
    WorkerTls(#[source] gang_tls::TransportError),
    /// The controller refused the worker's certificate as revoked.
    #[error("the controller refused the worker certificate")]
    CertificateRevoked(#[source] gang_tls::CertificateRevoked),
    /// `NativeLink` rejected a component it was asked to build or run.
    #[error("nativelink {context}")]
    NativeLink {
        /// The component that failed.
        context: &'static str,
        /// The `NativeLink` error.
        #[source]
        source: nativelink_error::Error,
    },
    /// The controller serves at most once per process.
    #[error("the controller is already serving")]
    #[failure(config)]
    AlreadyServing,
}

impl NativeLinkEmbedError {
    /// Builds a mapper that wraps a `NativeLink` error with the component `context` that failed.
    pub(crate) fn native(context: &'static str) -> impl FnOnce(nativelink_error::Error) -> Self {
        move |source| Self::NativeLink { context, source }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use gang_spi::PortError;
    use nativelink_error::{Code, make_err};

    use super::*;

    #[test]
    fn config_errors_map_to_config() {
        let error = NativeLinkConfigError::EndpointHost("worker_endpoint");
        assert!(matches!(PortError::from(error), PortError::Config(_)));
    }

    #[test]
    fn invalid_config_inputs_map_to_config() {
        let settings = serde_json::from_str::<serde_json::Value>("{").err();
        let Some(settings) = settings else {
            panic!("malformed json must fail to parse");
        };
        assert!(matches!(
            PortError::from(NativeLinkConfigError::Settings(settings)),
            PortError::Config(_)
        ));
        assert!(matches!(
            PortError::from(NativeLinkConfigError::NotUtf8(PathBuf::from("/tmp/x"))),
            PortError::Config(_)
        ));
        assert!(matches!(
            PortError::from(NativeLinkConfigError::WorkerEndpointTls { expected: "grpcs" }),
            PortError::Config(_)
        ));
        assert!(matches!(
            PortError::from(NativeLinkConfigError::StoreSpec("empty object")),
            PortError::Config(_)
        ));
    }

    #[test]
    fn embed_config_and_already_serving_map_to_config() {
        assert!(matches!(
            PortError::from(NativeLinkEmbedError::Config(
                NativeLinkConfigError::EndpointHost("worker_endpoint")
            )),
            PortError::Config(_)
        ));
        assert!(matches!(
            PortError::from(NativeLinkEmbedError::AlreadyServing),
            PortError::Config(_)
        ));
    }

    #[test]
    fn nativelink_failures_map_to_backend() {
        let native = NativeLinkEmbedError::NativeLink {
            context: "worker",
            source: make_err!(Code::Internal, "worker stopped"),
        };
        assert!(matches!(PortError::from(native), PortError::Backend(_)));
    }

    #[test]
    fn native_wraps_the_context_and_source() -> Result<(), Box<dyn std::error::Error>> {
        let error =
            NativeLinkEmbedError::native("worker init")(make_err!(Code::Internal, "store down"));
        assert!(matches!(
            error,
            NativeLinkEmbedError::NativeLink {
                context: "worker init",
                ..
            }
        ));
        let source = error.source().ok_or("NativeLink must keep its source")?;
        assert!(source.downcast_ref::<nativelink_error::Error>().is_some());
        Ok(())
    }

    #[test]
    fn duration_errors_map_to_config() {
        let config = NativeLinkConfigError::Duration {
            field: "scheduler.worker_timeout",
            requirement: "must be a whole number of seconds, at least 1s",
        };
        assert!(matches!(PortError::from(config), PortError::Config(_)));

        let embed = NativeLinkEmbedError::Config(NativeLinkConfigError::Duration {
            field: "client_action_timeout",
            requirement: "must be a whole number of seconds, at least 1s",
        });
        assert!(matches!(PortError::from(embed), PortError::Config(_)));
    }

    #[test]
    fn cold_start_fitting_no_fleet_is_a_config_error() {
        let config = NativeLinkConfigError::ColdStartFitsNoFleet {
            cpu_count: 1,
            memory_kb: 67_108_864,
            disk_kb: 4_194_304,
        };
        assert!(matches!(PortError::from(config), PortError::Config(_)));
    }
}
