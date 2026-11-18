//! The cache stores on S3: the content-addressed store and the action cache, both described to the
//! build engine as cloud object stores under one bucket and key prefix.
//!
//! Workers open the stores themselves with their own credentials (the instance role), so the
//! controller never proxies blob traffic. Bucket lifecycle expiry should exceed
//! `consider_expired_after`, so that an object is never deleted while the engine still treats it
//! as existing.

use gang_spi::{CacheStorePort, Plugin, PluginContext, PortError, RenderedStoreSpec, StoreAccess};
use serde_json::json;

use crate::config::S3CacheConfig;

/// Key prefix of the content-addressed store, appended to the configured `key_prefix`.
const CONTENT_PREFIX: &str = "cas/";

/// Key prefix of the action cache, appended to the configured `key_prefix`.
const ACTION_PREFIX: &str = "ac/";

/// The `NativeLink` store kind that reads and writes S3 objects.
const STORE_KEY: &str = "experimental_cloud_object_store";

/// The `NativeLink` provider name for S3.
const PROVIDER: &str = "aws";

/// The cache stores on S3, opened by the workers directly.
pub struct S3Cache {
    config: S3CacheConfig,
}

impl Plugin for S3Cache {
    type Config = S3CacheConfig;

    fn build(config: S3CacheConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self { config })
    }
}

impl CacheStorePort for S3Cache {
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(store_spec(&self.config, CONTENT_PREFIX))
    }

    fn action_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(store_spec(&self.config, ACTION_PREFIX))
    }

    /// The workers open the S3 stores with their own credentials.
    fn worker_access(&self) -> StoreAccess {
        StoreAccess::Direct
    }
}

/// Describes one store under the configured key prefix, namespaced by `prefix`.
fn store_spec(config: &S3CacheConfig, prefix: &str) -> RenderedStoreSpec {
    let mut object = json!({
        "provider": PROVIDER,
        "region": config.region.as_str(),
        "bucket": config.bucket.as_str(),
        "force_path_style": config.force_path_style,
        "key_prefix": format!("{}{prefix}", config.key_prefix.as_str()),
        "retry": {
            "max_retries": config.tuning.max_retries,
            "delay": config.tuning.retry_delay.get().as_secs_f64(),
            "jitter": f64::from(config.tuning.retry_jitter_percent.get()) / 100.0,
        },
        "multipart_max_concurrent_uploads": config.tuning.multipart_max_concurrent_uploads.get(),
        "max_retry_buffer_per_request": config.tuning.retry_buffer_per_request.get(),
    });
    if let Some(endpoint) = &config.endpoint {
        object["endpoint"] = json!(endpoint.as_str());
        // NativeLink otherwise configures its S3 HTTP connector for HTTPS only.
        if endpoint.scheme() == "http" {
            object["insecure_allow_http"] = json!(true);
        }
    }
    if let Some(expiry) = &config.tuning.consider_expired_after {
        object["consider_expired_after_s"] = json!(expiry.as_secs());
    }
    RenderedStoreSpec(json!({ STORE_KEY: object }))
}

#[cfg(test)]
mod tests {
    use nativelink_config::stores::StoreSpec;
    use serde_json::Value;

    use super::*;
    use crate::test_support::context;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn base_config() -> Value {
        json!({ "region": "us-east-1", "bucket": "example-bucket" })
    }

    fn object(spec: &RenderedStoreSpec) -> &Value {
        &spec.0[STORE_KEY]
    }

    #[test]
    fn both_stores_deserialize_into_nativelink_store_specs() -> TestResult {
        let cache = S3Cache::build(serde_json::from_value(base_config())?, &context())?;

        serde_json::from_value::<StoreSpec>(cache.content_store()?.0)?;
        serde_json::from_value::<StoreSpec>(cache.action_store()?.0)?;
        Ok(())
    }

    #[test]
    fn endpoint_and_path_style_render_for_both_stores() -> TestResult {
        for (endpoint, path_style, allow_http) in [
            (None, false, false),
            (None, true, false),
            (Some("http://127.0.0.1:9000/"), false, true),
            (Some("http://127.0.0.1:9000/"), true, true),
            (Some("https://s3.example.com/"), false, false),
            (Some("https://s3.example.com/"), true, false),
        ] {
            let mut value = base_config();
            if let Some(endpoint) = endpoint {
                value["endpoint"] = json!(endpoint);
            }
            value["force_path_style"] = json!(path_style);
            let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;
            for spec in [cache.content_store()?, cache.action_store()?] {
                assert_eq!(
                    object(&spec).get("endpoint").and_then(Value::as_str),
                    endpoint
                );
                assert_eq!(object(&spec)["force_path_style"], json!(path_style));
                assert_eq!(
                    object(&spec)
                        .get("insecure_allow_http")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    allow_http
                );
                let parsed = serde_json::from_value::<StoreSpec>(spec.0)?;
                let StoreSpec::ExperimentalCloudObjectStore(
                    nativelink_config::stores::ExperimentalCloudObjectSpec::Aws(aws),
                ) = parsed
                else {
                    return Err("expected an AWS cloud object store".into());
                };
                assert_eq!(aws.endpoint.as_deref(), endpoint);
                assert_eq!(aws.force_path_style, path_style);
                assert_eq!(aws.common.insecure_allow_http, allow_http);
            }
        }
        Ok(())
    }

    #[test]
    fn content_and_action_keys_differ_under_the_configured_prefix() -> TestResult {
        let mut value = base_config();
        value["key_prefix"] = json!("gang/");
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let content = cache.content_store()?;
        let action = cache.action_store()?;

        assert_eq!(object(&content)["key_prefix"].as_str(), Some("gang/cas/"));
        assert_eq!(object(&action)["key_prefix"].as_str(), Some("gang/ac/"));
        Ok(())
    }

    #[test]
    fn retry_delay_and_jitter_render_as_seconds_and_fraction() -> TestResult {
        let mut value = base_config();
        value["tuning"]["retry_delay"] = json!("300ms");
        value["tuning"]["retry_jitter_percent"] = json!(50);
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let spec = cache.content_store()?;

        assert_eq!(object(&spec)["retry"]["delay"], json!(0.3));
        assert_eq!(object(&spec)["retry"]["jitter"], json!(0.5));
        Ok(())
    }

    #[test]
    fn expiry_is_absent_by_default_and_rendered_in_seconds_when_set() -> TestResult {
        let default = S3Cache::build(serde_json::from_value(base_config())?, &context())?;
        assert!(
            object(&default.content_store()?)
                .get("consider_expired_after_s")
                .is_none()
        );

        let mut value = base_config();
        value["tuning"]["consider_expired_after"] = json!("6d");
        let expiring = S3Cache::build(serde_json::from_value(value)?, &context())?;
        assert_eq!(
            object(&expiring.action_store()?)["consider_expired_after_s"].as_u64(),
            Some(518_400)
        );
        Ok(())
    }

    #[test]
    fn max_expiry_renders_and_deserializes_into_nativelink() -> TestResult {
        let mut value = base_config();
        value["tuning"]["consider_expired_after"] = json!("4294967295s");
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let spec = cache.content_store()?;

        assert_eq!(
            object(&spec)["consider_expired_after_s"].as_u64(),
            Some(4_294_967_295)
        );
        serde_json::from_value::<StoreSpec>(spec.0)?;
        Ok(())
    }

    #[test]
    fn empty_bucket_is_rejected_at_deserialization() {
        let mut value = base_config();
        value["bucket"] = json!("");
        assert!(serde_json::from_value::<S3CacheConfig>(value).is_err());
    }
}
