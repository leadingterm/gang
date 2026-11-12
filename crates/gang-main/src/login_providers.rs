//! The identity providers of the `gang` binary as the client sees them: each login section a
//! server publishes is built into the composed [`IdentityProvider`].

use std::path::PathBuf;

use gang_client::LoginProviders;
use gang_spi::{PluginContext, PortError};

use crate::plugins::{IdentityProvider, IdentityProviderConfig};

/// The identity providers compiled into this binary.
pub(crate) struct BuiltInProviders {
    context_path: PathBuf,
}

impl BuiltInProviders {
    /// Creates the provider set; `context_path` is the file the plugin context names.
    pub(crate) fn new(context_path: PathBuf) -> Self {
        Self { context_path }
    }
}

impl LoginProviders for BuiltInProviders {
    type Config = IdentityProviderConfig;
    type Provider = IdentityProvider;

    fn build(&self, config: IdentityProviderConfig) -> Result<IdentityProvider, PortError> {
        IdentityProvider::build(
            config,
            &PluginContext {
                config_path: &self.context_path,
            },
        )
        .map_err(|error| error.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OIDC_SECTION: &str = r#"
[oidc]
issuer_url = "https://issuer.example.com"
audience = "gang"
principal_claim = "sub"
"#;

    #[test]
    fn an_oidc_section_builds_the_provider() {
        let config = toml::from_str::<IdentityProviderConfig>(OIDC_SECTION)
            .expect("the oidc section is valid toml for the identity provider config");
        BuiltInProviders::new(PathBuf::from("/etc/gang/gang.toml"))
            .build(config)
            .unwrap_or_else(|error| panic!("a valid oidc section must build: {error}"));
    }

    #[test]
    fn a_plain_http_issuer_is_rejected_when_the_section_is_parsed() {
        let section =
            OIDC_SECTION.replace("https://issuer.example.com", "http://issuer.example.com");
        let Err(error) = toml::from_str::<IdentityProviderConfig>(&section) else {
            panic!("a plain-http, non-loopback issuer must be rejected when parsed");
        };
        assert!(error.to_string().contains("issuer url must use https"));
    }
}
