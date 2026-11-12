//! Shared helpers for local plugin unit tests.

use std::path::Path;

use gang_spi::PluginContext;

/// Returns a plugin context with a deterministic configuration path.
pub(crate) fn plugin_context() -> PluginContext<'static> {
    PluginContext {
        config_path: Path::new("gang.toml"),
    }
}
