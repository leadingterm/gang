//! The seam between the generic client and the binary's identity providers.

use gang_spi::{LoginPort, PortError};
use serde::de::DeserializeOwned;

/// How the binary turns the login section a server publishes into a provider.
///
/// The binary implements this over its generated identity-provider configuration, building
/// each provider with `IdentityProvider::build(config, &context)`.
pub trait LoginProviders: Send + Sync {
    /// The binary's generated identity-provider configuration type.
    type Config: DeserializeOwned + Send + Sync;
    /// The provider that one configuration builds.
    type Provider: LoginPort;

    /// Builds the provider for one configuration. Fails when the configuration is unusable.
    ///
    /// # Errors
    ///
    /// Returns the backend's own error when it rejects the configuration, and
    /// [`PortError::Config`] when the section is unusable.
    fn build(&self, config: Self::Config) -> Result<Self::Provider, PortError>;
}
