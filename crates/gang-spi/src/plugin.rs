//! The contract between the composition root and a backend.

use std::fmt::Debug;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::PortError;

/// What the host tells every plugin when it is built.
#[derive(Debug, Clone, Copy)]
pub struct PluginContext<'a> {
    /// The configuration file the process was started with: `gang.toml`, or for `gang exec`,
    /// its sandbox file.
    pub config_path: &'a Path,
}

/// A backend selected in `gang.toml` by its section key and composed into the binary at compile
/// time by `compose!`. There is no runtime registry.
pub trait Plugin: Sized + Send + Sync + 'static {
    /// The plugin's section, deserialized strictly (`deny_unknown_fields`).
    type Config: DeserializeOwned + Serialize + Debug + Send + Sync;

    /// Builds the plugin from its section.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the section is unusable, or the backend's own error.
    fn build(config: Self::Config, context: &PluginContext<'_>) -> Result<Self, PortError>;
}

/// A sandbox plugin section that names the programs the sandbox runs.
///
/// The host rewrites the paths in place before the section is written to the sandbox file, so a
/// relative program name is replaced by the absolute path the worker resolves through its own
/// `PATH`.
pub trait SandboxPrograms {
    /// Calls `visit` on every program path the section names, in order, and stops at the first error.
    ///
    /// # Errors
    ///
    /// The first error `visit` returns.
    fn try_for_each_program<E>(
        &mut self,
        visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E>;
}
