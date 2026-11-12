//! The no-op sandbox: runs actions directly on the host, with a per-action `TMPDIR`.

use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, Plugin, PluginContext,
    PortError, SandboxedAction,
};
use gang_types::AbsolutePathBuf;
use tempfile::TempDir;
use tokio::process::Child;

use crate::command::command;
use crate::config::NoopConfig;
use crate::errors::NoopError;

/// Runs actions directly on the host without isolation. Each action gets its own `TMPDIR`,
/// removed when the action ends.
#[derive(Debug)]
pub struct Noop {
    /// The directory under which each action's `TMPDIR` is created, or the system temporary
    /// directory when `None`.
    scratch_dir: Option<AbsolutePathBuf>,
}

impl Plugin for Noop {
    type Config = NoopConfig;

    fn build(config: NoopConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self {
            scratch_dir: config.scratch_dir,
        })
    }
}

impl ActionIsolationPort for Noop {
    /// Spawns the action in the calling process's own process group, so a signal to that group
    /// reaches it. Spawning is synchronous, so this returns once the action is running. Dropping
    /// the [`SandboxedAction`] kills the action's process.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the temporary directory cannot be created or the command cannot be
    /// started.
    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("gang-action-");
        let temporary_directory = match &self.scratch_dir {
            Some(scratch) => builder.tempdir_in(scratch),
            None => builder.tempdir(),
        }
        .map_err(NoopError::TemporaryDirectory)?;
        let mut process = tokio::process::Command::from(command(spec, temporary_directory.path()));
        process.kill_on_drop(true);
        let child = process.spawn().map_err(NoopError::Spawn)?;
        Ok(SandboxedAction::new(wait_for_end(
            child,
            temporary_directory,
        )))
    }
}

/// Waits for the started action to end. The temporary directory is removed when the future
/// ends. Dropping the future before the action ends kills the action's process.
async fn wait_for_end(
    mut child: Child,
    _temporary_directory: TempDir,
) -> Result<ActionExecutionOutcome, PortError> {
    let status = child.wait().await.map_err(NoopError::Wait)?;
    ActionExecutionOutcome::try_from(status)
}
