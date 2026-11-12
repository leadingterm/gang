//! Shared AWS SDK configuration.
//!
//! Every AWS client in this crate is built from one explicit HTTP client backed by ring. The SDK's
//! default client would link a second TLS crypto provider (aws-lc) next to the rest of the binary,
//! so the TLS provider is pinned here instead.

use crate::client_settings::ClientSettings;
use crate::names::Region;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;

/// HTTP client using rustls with the ring provider and the platform trust store.
pub(crate) fn http_client() -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https()
}

/// SDK config loader with `region`, the timeouts and retries from `client`, and the shared HTTP
/// client.
pub(crate) fn loader(region: &Region, client: &ClientSettings) -> aws_config::ConfigLoader {
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.as_str().to_owned()))
        .http_client(http_client())
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(client.connect_timeout.get())
                .operation_timeout(client.request_timeout.get())
                .build(),
        )
        .retry_config(
            RetryConfig::standard()
                .with_max_attempts(client.max_attempts.get())
                .with_initial_backoff(client.retry_initial_backoff.get())
                .with_max_backoff(client.retry_max_backoff.get()),
        )
}

/// Loads the SDK config for `region` with the settings in `client`.
pub(crate) async fn load(region: &Region, client: &ClientSettings) -> SdkConfig {
    loader(region, client).load().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{AwsError, sdk_error};
    use crate::fake_aws::{FakeAws, FakeFailure};
    use aws_sdk_autoscaling::Client;
    use gang_types::NonZeroDuration;
    use std::error::Error;
    use std::num::NonZeroU32;

    type TestResult = Result<(), Box<dyn Error>>;

    // Real time, not paused: the 1 ms backoffs are real but tiny, and the verdict never depends on elapsed time.
    #[tokio::test]
    async fn retries_stop_at_max_attempts() -> TestResult {
        let fake = FakeAws::start().await?;
        fake.state().failures.insert(
            "DescribeAutoScalingGroups",
            FakeFailure {
                status: 500,
                code: "InternalFailure",
                message: "injected",
            },
        );
        let settings = ClientSettings {
            request_timeout: NonZeroDuration::from_hours(24),
            connect_timeout: NonZeroDuration::from_hours(24),
            max_attempts: NonZeroU32::new(3).ok_or("zero attempts")?,
            retry_initial_backoff: NonZeroDuration::from_millis(1),
            retry_max_backoff: NonZeroDuration::from_millis(1),
        };
        let sdk = fake.sdk_config_with(&settings).await;

        let error = Client::new(&sdk)
            .describe_auto_scaling_groups()
            .send()
            .await
            .err()
            .ok_or("describe unexpectedly succeeded")?;
        let received = fake.state().requests.len();
        assert_eq!(received, 3);

        let error = sdk_error("DescribeAutoScalingGroups", error);
        assert!(matches!(error, AwsError::Transient { .. }));
        Ok(())
    }
}
