use std::sync::Arc;

use gang_clock::{Clock, SystemClock};
use gang_spi::{Plugin, PluginContext, PortError};
use gang_types::Audience;

use crate::claims::Policy;
use crate::config::OidcConfig;
use crate::discovery::Discovery;
use crate::errors::OidcError;
use crate::http::Http;
use crate::keys::KeyCache;
use crate::refresh_cache::RefreshCache;

/// An `OpenID` Connect identity provider, built from its configuration.
///
/// Construction performs no network I/O: discovery and key fetches happen on first use.
pub struct Provider {
    config: OidcConfig,
    http: Http,
    clock: Arc<dyn Clock>,
    discovery: RefreshCache<Discovery>,
    keys: KeyCache,
}

impl Provider {
    /// Builds the provider with the given clock.
    ///
    /// # Errors
    ///
    /// Returns a [`PortError`] when the HTTP client cannot be built.
    pub fn new(config: OidcConfig, clock: Arc<dyn Clock>) -> Result<Self, PortError> {
        select_crypto_provider();
        let http = Http::new(&config, Arc::clone(&clock))?;
        let keys = KeyCache::new(config.cache.key_ttl, config.cache.key_refresh_interval);
        let discovery = RefreshCache::new(
            config.cache.discovery_ttl,
            config.cache.discovery_retry_interval,
            || OidcError::DiscoveryUnavailable,
        );
        Ok(Self {
            config,
            http,
            clock,
            discovery,
            keys,
        })
    }

    pub(crate) fn config(&self) -> &OidcConfig {
        &self.config
    }

    pub(crate) fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub(crate) fn http(&self) -> &Http {
        &self.http
    }

    pub(crate) fn keys(&self) -> &KeyCache {
        &self.keys
    }

    pub(crate) async fn discovery(&self) -> Result<Arc<Discovery>, OidcError> {
        self.discovery
            .get(self.clock(), || async {
                let mut audiences = vec![self.config.audience.as_str()];
                audiences.extend(
                    self.config
                        .additional_audiences
                        .iter()
                        .map(Audience::as_str),
                );
                Discovery::fetch(&self.http, self.config.issuer_url.as_url(), &audiences).await
            })
            .await
    }

    pub(crate) fn policy(&self) -> Policy<'_> {
        Policy {
            principal_claim: &self.config.principal_claim,
            required_claims: &self.config.required_claims,
        }
    }
}

impl Plugin for Provider {
    type Config = OidcConfig;

    fn build(config: OidcConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Self::new(config, Arc::new(SystemClock::new()))
    }
}

/// `jsonwebtoken` cannot choose between its backends when another crate in the build enables the
/// second one (the `NativeLink` crates enable `aws_lc_rs`), so Gang installs the pure Rust one once
/// per process; installing twice is harmless.
fn select_crypto_provider() {
    match jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default() {
        Ok(()) | Err(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gang_clock::TestClock;

    use super::{OidcConfig, Provider};

    fn config(issuer: &str) -> OidcConfig {
        serde_json::from_value(serde_json::json!({
            "issuer_url": issuer,
            "audience": "alpha",
            "principal_claim": "sub"
        }))
        .expect("test configuration is valid")
    }

    fn accepts(config: OidcConfig) {
        assert!(
            Provider::new(config, Arc::new(TestClock::default())).is_ok(),
            "configuration should be accepted"
        );
    }

    #[test]
    fn construction_performs_no_network_io() {
        // The issuer name does not resolve; construction must still succeed without a lookup.
        accepts(config("https://idp.example.invalid"));
    }
}
