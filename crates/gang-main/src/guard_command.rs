//! The guardian entrypoint: reap scopes after the action entrypoint that started it exits.

use std::path::Path;
use std::process::ExitCode;

use gang_config::SandboxFile;
use gang_spi::PluginContext;

use crate::errors::Result;
use crate::plugins::{Limits, Plugins};

/// Loads the worker's limits plugin and reaps the creator's scopes once the kernel reports that
/// the creator, this process's parent, has exited.
pub(crate) fn run(entrypoint: &Path, creator: u32) -> Result<ExitCode> {
    if !entrypoint.is_absolute() {
        return Err(gang_exec::ExecError::EntrypointNotAbsolute {
            entrypoint: entrypoint.to_path_buf(),
        }
        .into());
    }
    let path = SandboxFile::<Plugins>::beside(entrypoint);
    let file = gang_exec::read_sandbox_file::<Plugins>(&path)?;
    if let Some(config) = file.limits {
        let exited = gang_os::ParentExit::watch(creator).map_err(gang_exec::ExecError::Watch)?;
        let context = PluginContext { config_path: &path };
        let limits = Limits::build(config, &context)
            .map_err(|error| gang_exec::ExecError::Limits(error.source))?;
        crate::runtime::block_on(async {
            gang_exec::guard(&limits, creator, exited.exited()).await
        })?;
    }
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;

    use gang_config::SandboxFile;

    use super::run;
    use crate::plugins::Plugins;

    /// A guardian with no configured limits has nothing to reap and never waits on stdin.
    #[test]
    fn no_limits_exits_without_waiting() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let entrypoint = dir.path().join("gang-guard");
        std::fs::write(
            SandboxFile::<Plugins>::beside(&entrypoint),
            "work_directory = \"/tmp\"\nselected = \"noop\"\n\n[sandbox.noop]\n",
        )?;
        assert_eq!(run(&entrypoint, 42)?, ExitCode::SUCCESS);
        Ok(())
    }
}
