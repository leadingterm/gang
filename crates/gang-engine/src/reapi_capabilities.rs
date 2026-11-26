//! `REAPI v2` `Capabilities` gRPC service.
//!
//! Advertises `REAPI v2.0–2.3` capabilities: `SHA256` digest function,
//! `max_batch_total_size_bytes`, `ZSTD` + `IDENTITY` compressors,
//! `ActionCacheUpdateCapabilities { update_enabled: true }`,
//! `SymlinkAbsolutePathStrategy::ALLOWED`, and `ExecutionCapabilities` with priority range
//! `[i32::MIN, i32::MAX]`.

use gang_reapi::build::bazel::remote::execution::v2::capabilities_server::{
    Capabilities, CapabilitiesServer,
};
use gang_reapi::build::bazel::remote::execution::v2::priority_capabilities::PriorityRange;
use gang_reapi::build::bazel::remote::execution::v2::{
    ActionCacheUpdateCapabilities, CacheCapabilities, ExecutionCapabilities,
    GetCapabilitiesRequest, PriorityCapabilities, ServerCapabilities, compressor, digest_function,
    symlink_absolute_path_strategy,
};
use gang_reapi::build::bazel::semver::SemVer;
use gang_types::NonZeroBytes;
use tonic::{Request, Response, Status};

const DEFAULT_MAX_BATCH_BYTES: NonZeroBytes = NonZeroBytes::mib(4);

/// Configuration for [`CapabilitiesService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilitiesConfig {
    /// Maximum combined blob size advertised for `BatchReadBlobs` / `BatchUpdateBlobs`.
    /// Set to `[server.http2] max_message_size`.
    pub max_batch_total_size_bytes: NonZeroBytes,
    /// Whether remote execution is enabled (`ExecutionCapabilities::exec_enabled`).
    pub exec_enabled: bool,
}

impl Default for CapabilitiesConfig {
    fn default() -> Self {
        Self {
            max_batch_total_size_bytes: DEFAULT_MAX_BATCH_BYTES,
            exec_enabled: true,
        }
    }
}

/// `REAPI v2` `Capabilities` gRPC server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapabilitiesService {
    config: CapabilitiesConfig,
}

impl CapabilitiesService {
    /// Creates a [`CapabilitiesService`] from `config`.
    #[must_use]
    pub const fn new(config: CapabilitiesConfig) -> Self {
        Self { config }
    }

    /// Wraps this service in a tonic [`CapabilitiesServer`].
    #[must_use]
    pub fn into_server(self) -> CapabilitiesServer<Self> {
        CapabilitiesServer::new(self)
    }

    /// Builds the [`ServerCapabilities`] response advertised by `GetCapabilities`.
    #[must_use]
    pub fn capabilities(&self) -> ServerCapabilities {
        let sha256 = digest_function::Value::Sha256 as i32;
        let full_priority_range = PriorityRange {
            min_priority: i32::MIN,
            max_priority: i32::MAX,
        };
        let max_batch_total_size_bytes =
            i64::try_from(self.config.max_batch_total_size_bytes.as_u64()).unwrap_or(i64::MAX);
        let compressors = vec![
            compressor::Value::Identity as i32,
            compressor::Value::Zstd as i32,
        ];

        ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![sha256],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                cache_priority_capabilities: Some(PriorityCapabilities {
                    priorities: vec![full_priority_range],
                }),
                max_batch_total_size_bytes,
                symlink_absolute_path_strategy: symlink_absolute_path_strategy::Value::Allowed
                    as i32,
                supported_compressors: compressors.clone(),
                supported_batch_update_compressors: compressors,
                max_cas_blob_size_bytes: 0,
                split_blob_support: false,
                splice_blob_support: false,
                fast_cdc_2020_params: None,
                rep_max_cdc_params: None,
            }),
            execution_capabilities: Some(ExecutionCapabilities {
                digest_function: sha256,
                exec_enabled: self.config.exec_enabled,
                execution_priority_capabilities: Some(PriorityCapabilities {
                    priorities: vec![full_priority_range],
                }),
                supported_node_properties: Vec::new(),
                digest_functions: vec![sha256],
            }),
            deprecated_api_version: None,
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: String::new(),
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 3,
                patch: 0,
                prerelease: String::new(),
            }),
        }
    }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesService {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        Ok(Response::new(self.capabilities()))
    }
}

#[cfg(test)]
mod tests {
    use gang_reapi::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
    use gang_reapi::build::bazel::remote::execution::v2::{
        GetCapabilitiesRequest, compressor, digest_function, symlink_absolute_path_strategy,
    };
    use gang_reapi::build::bazel::semver::SemVer;
    use gang_types::NonZeroBytes;
    use tonic::Request;

    use super::{CapabilitiesConfig, CapabilitiesService};

    #[tokio::test]
    async fn advertises_expected_reapi_v2_capabilities() {
        let svc = CapabilitiesService::new(CapabilitiesConfig {
            max_batch_total_size_bytes: NonZeroBytes::mib(8),
            exec_enabled: true,
        });

        let caps = svc
            .get_capabilities(Request::new(GetCapabilitiesRequest {
                instance_name: String::new(),
            }))
            .await
            .expect("get_capabilities")
            .into_inner();

        assert_eq!(
            caps.low_api_version,
            Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: String::new(),
            })
        );
        assert_eq!(
            caps.high_api_version,
            Some(SemVer {
                major: 2,
                minor: 3,
                patch: 0,
                prerelease: String::new(),
            })
        );

        let cache = caps.cache_capabilities.expect("cache capabilities");
        assert_eq!(
            cache.digest_functions,
            vec![digest_function::Value::Sha256 as i32]
        );
        assert_eq!(cache.max_batch_total_size_bytes, 8 * 1024 * 1024);
        assert_eq!(
            cache.symlink_absolute_path_strategy,
            symlink_absolute_path_strategy::Value::Allowed as i32
        );
        assert!(
            cache
                .action_cache_update_capabilities
                .expect("ac update")
                .update_enabled
        );
        assert_eq!(
            cache.supported_compressors,
            vec![
                compressor::Value::Identity as i32,
                compressor::Value::Zstd as i32
            ]
        );
        assert_eq!(
            cache.supported_batch_update_compressors,
            vec![
                compressor::Value::Identity as i32,
                compressor::Value::Zstd as i32
            ]
        );

        let exec = caps.execution_capabilities.expect("execution capabilities");
        assert_eq!(exec.digest_function, digest_function::Value::Sha256 as i32);
        assert_eq!(
            exec.digest_functions,
            vec![digest_function::Value::Sha256 as i32]
        );
        assert!(exec.exec_enabled);
        assert!(exec.supported_node_properties.is_empty());
        let range = exec
            .execution_priority_capabilities
            .expect("priority caps")
            .priorities;
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].min_priority, i32::MIN);
        assert_eq!(range[0].max_priority, i32::MAX);
    }

    #[tokio::test]
    async fn honors_exec_enabled_flag() {
        let svc = CapabilitiesService::new(CapabilitiesConfig {
            max_batch_total_size_bytes: NonZeroBytes::mib(4),
            exec_enabled: false,
        });
        let caps = svc
            .get_capabilities(Request::new(GetCapabilitiesRequest {
                instance_name: "cache-only".to_owned(),
            }))
            .await
            .expect("get_capabilities")
            .into_inner();
        assert!(
            !caps
                .execution_capabilities
                .expect("execution caps")
                .exec_enabled
        );
    }
}
