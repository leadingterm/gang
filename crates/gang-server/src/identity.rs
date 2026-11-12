//! Identity providers as the server sees them: the verifier for their proofs, and the plugin
//! section clients use to log in.

use std::collections::BTreeMap;
use std::sync::Arc;

use gang_spi::{LoginPort, LoginSupport, ProofVerifierPort};
use gang_types::ProviderName;

/// One configured identity provider.
pub struct Provider<V> {
    /// Verifies the proofs presented under this provider.
    pub verifier: Arc<V>,
    /// The provider's plugin section, shown to clients in `Server.authentication.token.login`.
    /// Present only for providers with an interactive login.
    login: Option<serde_json::Value>,
}

impl<V: ProofVerifierPort + LoginPort> Provider<V> {
    /// Builds a provider from its verifier and its serialized plugin configuration.
    ///
    /// `plugin_section` is what `Server.authentication.token.login` shows to clients so they can
    /// start an interactive login. It is kept only when `verifier.login()` reports an
    /// interactive login.
    #[must_use]
    pub fn new(verifier: V, plugin_section: serde_json::Value) -> Self {
        let login = match verifier.login() {
            LoginSupport::Interactive => Some(plugin_section),
            LoginSupport::Unsupported => None,
        };
        Self {
            verifier: Arc::new(verifier),
            login,
        }
    }
}

impl<V> Provider<V> {
    /// The plugin section clients see in `Server.authentication.token.login`, `None` when the
    /// provider has no interactive login.
    #[must_use]
    pub fn login(&self) -> Option<&serde_json::Value> {
        self.login.as_ref()
    }
}

/// The configured identity providers, keyed by name.
pub type Providers<V> = BTreeMap<ProviderName, Provider<V>>;
