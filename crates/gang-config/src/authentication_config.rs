//! The `[authentication]` section: one plugin for each surface.

use serde::{Deserialize, Serialize};

/// The authentication plugins, one per surface. Each surface selects exactly one plugin from its
/// own domain: `[authentication.clients.<plugin>]` or `[authentication.workers.<plugin>]`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationConfig<C, W> {
    /// The plugin that authenticates people and CI jobs calling the public listener.
    pub clients: C,
    /// The plugin that authenticates workers calling the worker listener.
    pub workers: W,
}
