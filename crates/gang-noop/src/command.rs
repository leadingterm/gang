//! Builds the host command that runs one action.

use std::path::Path;
use std::process::Command;

use gang_spi::ActionExecutionSpec;

/// Builds the command for `spec` with its complete environment, working directory, and a
/// `TMPDIR` pointing at `temporary_directory`.
/// The action directory is not used: without isolation every path is already visible.
///
/// The command is `spec.launcher` followed by `spec.command`. With a launcher (limits), the launcher
/// starts the action inside its limits scope. The command stays in the process group of its
/// parent, whether or not there is a launcher. A signal sent to that group, as `NativeLink` does
/// to the `gang exec` process that hosts the action, reaches every descendant of the action as
/// well. `TMPDIR` is set after the given environment and therefore overrides any `TMPDIR` the
/// action supplied, keeping its temporary files in the per-action directory.
pub(crate) fn command(spec: &ActionExecutionSpec<'_>, temporary_directory: &Path) -> Command {
    let mut command = match spec.launcher.split_first() {
        Some((program, args)) => {
            let mut command = Command::new(program);
            command.args(args).arg(spec.command.program());
            command
        }
        None => Command::new(spec.command.program()),
    };
    command
        .args(spec.command.args())
        .env_clear()
        .envs(spec.environment.iter().map(|(name, value)| (name, value)))
        .env("TMPDIR", temporary_directory)
        .current_dir(spec.working_directory);
    command
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::path::Path;

    use gang_spi::{ActionCommand, ActionExecutionSpec, NetworkRequest};

    use super::command;

    #[test]
    fn the_launcher_precedes_the_action() -> Result<(), Box<dyn std::error::Error>> {
        let argv = [OsString::from("/bin/echo"), OsString::from("hi")];
        let launcher = [
            OsString::from("/usr/bin/gang-join"),
            OsString::from("/scope"),
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &launcher,
            environment: &[],
            action_directory: Path::new("/work"),
            working_directory: Path::new("/work"),
            network: NetworkRequest::NotRequested,
        };

        let command = command(&spec, Path::new("/tmp/action"));

        assert_eq!(command.get_program(), launcher[0].as_os_str());
        let args: Vec<&OsStr> = command.get_args().collect();
        let expected: Vec<&OsStr> = launcher[1..]
            .iter()
            .chain(&argv)
            .map(OsString::as_os_str)
            .collect();
        assert_eq!(args, expected);
        Ok(())
    }
}
