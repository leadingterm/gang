//! The `gang exec` handler: runs one action in its configured sandbox.
//!
//! Nothing but the action's exit status is printed; the action owns stdout and stderr. Any
//! failure before the action exits (an empty command, a bad sandbox file, a plugin that cannot
//! be built) is returned as an error, which dispatch prints to stderr and turns into exit
//! status 1. This handler installs no logging and starts no runtime of its own: the executor
//! does both, so the action runs with the process exactly as the worker started it.

use std::ffi::OsString;
use std::path::Path;
use std::process::ExitCode;

use crate::errors::Result;
use crate::plugins::{Limits, Plugins, Sandbox};

/// Runs `argv` in the sandbox described beside `entrypoint` and returns the action's exit status.
pub(crate) fn run(entrypoint: &Path, argv: &[OsString]) -> Result<ExitCode> {
    Ok(gang_exec::run::<Plugins, _, _>(
        entrypoint,
        argv,
        |key, plugin, context| Sandbox::build(key, plugin, context).map_err(|error| error.source),
        |config, context| Limits::build(config, context).map_err(|error| error.source),
    )?)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::Path;

    use gang_exec::ExecError;

    use super::run;
    use crate::errors::MainError;

    #[test]
    fn empty_argv_is_rejected_before_any_file_is_read() {
        let result = run(Path::new("/nonexistent/gang-exec"), &[]);
        assert!(matches!(result, Err(MainError::Exec(ExecError::NoCommand))));
    }

    #[test]
    fn relative_entrypoint_is_rejected_before_any_file_is_read() {
        let argv = [OsString::from("true")];
        let result = run(Path::new("gang-exec"), &argv);
        assert!(matches!(
            result,
            Err(MainError::Exec(ExecError::EntrypointNotAbsolute { .. }))
        ));
    }
}
