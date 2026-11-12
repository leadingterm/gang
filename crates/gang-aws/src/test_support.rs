//! Shared fixtures for AWS plugin tests.

use std::path::Path;

use gang_spi::PluginContext;

/// A plugin context with a fixed configuration path.
pub(crate) fn context() -> PluginContext<'static> {
    PluginContext {
        config_path: Path::new("gang.toml"),
    }
}
