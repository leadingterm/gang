//! The `none` plugin: every request is the anonymous identity, and no credential is issued.

use std::sync::Arc;

use gang_spi::{
    Authenticated, AuthenticationHost, AuthenticationPort, Capabilities, ClientCertificates,
    CredentialLifecyclePort, Evidence, Plugin, PluginContext, PortError, Scheme,
};
use gang_types::{AuthenticationSettings, Identity, Principal, ProviderName};
use serde::{Deserialize, Serialize};

/// The `none` section of the configuration. It has no settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NoneConfig {}

impl AuthenticationSettings for NoneConfig {
    fn requires_tls(&self) -> bool {
        false
    }
}

/// Admits every caller as [`anonymous`]. It issues no credentials and revokes none.
#[derive(Debug)]
pub struct Anonymous {
    /// The identity every request is attributed to, shared rather than rebuilt per request.
    identity: Arc<Identity>,
}

/// The identity of a caller on a surface with no authentication.
#[must_use]
pub fn anonymous() -> Identity {
    Identity::new(ProviderName::ANONYMOUS, Principal::ANONYMOUS)
}

impl Plugin for Anonymous {
    type Config = NoneConfig;

    fn build(_config: NoneConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self {
            identity: Arc::new(anonymous()),
        })
    }
}

impl AuthenticationPort for Anonymous {
    fn scheme(&self) -> Scheme {
        Scheme::None
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            certificates: false,
            tokens: false,
            sessions: false,
        }
    }

    fn client_certificates(&self) -> ClientCertificates {
        ClientCertificates::Never
    }

    async fn authenticate<H: AuthenticationHost>(
        &self,
        _host: &H,
        _evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        Ok(Some(Authenticated {
            identity: Arc::clone(&self.identity),
            valid_until: None,
        }))
    }
}

impl CredentialLifecyclePort for Anonymous {}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use gang_spi::{Plugin, PluginContext};
    use gang_types::AuthenticationSettings;

    use super::{Anonymous, NoneConfig, anonymous};

    #[test]
    fn config_parses_from_an_empty_object() {
        let config: NoneConfig = serde_json::from_str("{}").expect("an empty section is valid");

        assert_eq!(config, NoneConfig::default());
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let parsed = serde_json::from_str::<NoneConfig>(r#"{"unexpected":true}"#);

        assert!(parsed.is_err(), "unknown fields are rejected");
    }

    #[test]
    fn anonymous_identity_displays_as_none_anonymous() {
        let identity = anonymous();

        assert_eq!(identity.to_string(), "none:anonymous");
    }

    #[test]
    fn build_shares_the_anonymous_identity() {
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };

        let built = Anonymous::build(NoneConfig::default(), &context)
            .expect("the none plugin builds from an empty section");

        let expected = anonymous();

        assert_eq!(*built.identity, expected);
    }

    #[test]
    fn none_names_no_provider_and_requires_no_tls() {
        let config = NoneConfig::default();
        assert!(!config.requires_tls());
        assert!(config.providers().is_empty());
    }
}
