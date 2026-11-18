//! Shared AWS SDK configuration.
//!
//! Every AWS client using this loader is built from one explicit HTTP client backed by ring. The SDK's
//! default client would link a second TLS crypto provider (aws-lc) next to the rest of the binary,
//! so the TLS provider is pinned here instead.

use crate::{ClientSettings, Region};
use aws_config::{BehaviorVersion, SdkConfig};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;

/// HTTP client using rustls with the ring provider and the platform trust store.
#[must_use]
pub fn http_client() -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https()
}

/// SDK config loader with `region`, the timeouts and retries from `client`, and the shared HTTP
/// client.
#[must_use]
pub fn loader(region: &Region, client: &ClientSettings) -> aws_config::ConfigLoader {
    let mut timeout = TimeoutConfig::builder()
        .connect_timeout(client.connect_timeout.get())
        .operation_timeout(client.request_timeout.get());
    if client.max_attempts.get() > 1 {
        timeout = timeout.operation_attempt_timeout(client.request_timeout.get() / 2);
    }
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.as_str().to_owned()))
        .http_client(http_client())
        .timeout_config(timeout.build())
        .retry_config(
            RetryConfig::standard()
                .with_max_attempts(client.max_attempts.get())
                .with_initial_backoff(client.retry_initial_backoff.get())
                .with_max_backoff(client.retry_max_backoff.get()),
        )
}

/// Loads the SDK config for `region` with the settings in `client`.
pub async fn load(region: &Region, client: &ClientSettings) -> SdkConfig {
    loader(region, client).load().await
}
