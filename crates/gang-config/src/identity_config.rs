//! The `[identity]` section: the identity providers that prove who a caller is.

use std::collections::BTreeMap;

use gang_types::ProviderName;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The identity providers. Each entry is keyed by the operator's name for it and holds one
/// provider plugin's section, as `[identity.providers.<name>.<plugin>]`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    bound(serialize = "Provider: Serialize", deserialize = "")
)]
pub struct IdentityConfig<Provider: DeserializeOwned> {
    /// The configured providers, keyed by the operator's name for them. Defaults to none, so the
    /// table may be omitted, as the plaintext configuration with the `none` plugins does.
    #[serde(default)]
    pub providers: BTreeMap<ProviderName, Provider>,
}

impl<Provider: DeserializeOwned> Default for IdentityConfig<Provider> {
    /// No identity providers. The `none` plugins need none, and a plugin that names a provider
    /// is rejected by the configuration's authentication-provider check.
    fn default() -> Self {
        Self {
            providers: BTreeMap::new(),
        }
    }
}
