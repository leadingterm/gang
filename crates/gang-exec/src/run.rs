//! The `gang exec` entry point: from the process invocation to the action's exit status.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, ExitCode, Stdio};

use gang_config::{PluginSet, SandboxFile};
use gang_spi::{
    ActionCommand, ActionExecutionSpec, ActionIsolationPort, ActionLimitsPort, GUARD_LINK_NAME,
    PluginContext, PortError,
};

use crate::action_directory::ActionDirectory;
use crate::errors::ExecError;
use crate::execute::{Action, execute, exit_status};
use crate::properties::ActionProperties;
use crate::terminations;

/// The process `gang exec` was started with: what it runs and where.
#[derive(Debug, Clone, Copy)]
pub struct Invocation<'a> {
    /// The absolute path of the `gang-exec` link the process was started through.
    pub entrypoint: &'a Path,
    /// The command and its arguments; the first is the program.
    pub argv: &'a [OsString],
    /// The environment the process was started with. The action runs with it unchanged.
    pub environment: &'a [(OsString, OsString)],
    /// The working directory the process was started in.
    pub working_directory: &'a Path,
}

/// Runs the action `argv` as `gang exec` does and returns its exit status.
///
/// Takes the entrypoint, the arguments, the environment and the working directory from the
/// process, and runs them with [`run_with`].
///
/// `NativeLink` signals action process groups and eventually kills `gang exec` itself by PID.
/// When limits are configured, a guardian in the worker's group reaps scopes after it exits; it
/// starts with an empty environment and no standard streams, and waits for this process's exit
/// as the kernel reports it.
///
/// # Errors
///
/// [`ExecError`] for any failure before or while running the action.
pub fn run<P, S, L>(
    entrypoint: &Path,
    argv: &[OsString],
    build_sandbox: impl FnOnce(P::SandboxKey, P::Sandbox, &PluginContext<'_>) -> Result<S, PortError>,
    build_limits: impl FnOnce(P::Limits, &PluginContext<'_>) -> Result<L, PortError>,
) -> Result<ExitCode, ExecError>
where
    P: PluginSet,
    S: ActionIsolationPort,
    L: ActionLimitsPort,
{
    let environment: Vec<(OsString, OsString)> = std::env::vars_os().collect();
    let working_directory = std::env::current_dir().map_err(ExecError::WorkingDirectory)?;
    run_inner::<P, S, L>(
        &Invocation {
            entrypoint,
            argv,
            environment: &environment,
            working_directory: &working_directory,
        },
        build_sandbox,
        build_limits,
        gang_os::join_parent_group,
        || {
            let group = gang_os::parent_process_group().map_err(ExecError::Guard)?;
            let executable = std::env::current_exe().map_err(ExecError::Guard)?;
            // The guardian outlives this process: it must not read the action's untrusted
            // environment, nor hold open the standard error the worker captures as the action's.
            // It is this process's child and learns of its exit from the kernel alone.
            Command::new(executable)
                .arg0(entrypoint.with_file_name(GUARD_LINK_NAME))
                .arg(std::process::id().to_string())
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(group)
                .spawn()
                .map(drop)
                .map_err(ExecError::Guard)
        },
    )
}

/// Runs the action in `invocation` as `gang exec` does and returns its exit status.
///
/// Steps, in order: reject an empty command; reject a relative entrypoint; read the sandbox file
/// beside the entrypoint; read the action properties from the environment; resolve the action
/// directory and the working directory inside it under the work directory the file records,
/// rejecting a working directory outside it; the sandbox gets both, so it can expose the whole
/// action directory; build the sandbox and, when the file has limits, the limits plugin; then run
/// the action on a dedicated current-thread runtime. Hard-link breaking happens inside that
/// runtime, before the sandbox starts. The single runtime thread is deliberate: nothing else runs
/// before the sandbox, so per-action startup stays minimal.
///
/// Termination signals are handled for the whole run on that runtime. `leave_action_group` is
/// chosen by the caller and runs once an action that shares the process group of `gang exec`
/// exists; the `gang exec` entrypoint uses it to move into the worker's process group, outside the
/// action's signalled process groups. An action in its own process group leaves `gang exec` in the
/// original group, where it receives each termination and relays it.
///
/// Nothing the worker controls is read from the environment, because the action can override
/// the environment it is given.
///
/// # Errors
///
/// [`ExecError`] for any failure before or while running the action; a sandbox that runs the
/// action reports its exit status through the returned [`ExitCode`], not as an error.
pub fn run_with<P, S, L>(
    invocation: &Invocation<'_>,
    build_sandbox: impl FnOnce(P::SandboxKey, P::Sandbox, &PluginContext<'_>) -> Result<S, PortError>,
    build_limits: impl FnOnce(P::Limits, &PluginContext<'_>) -> Result<L, PortError>,
    leave_action_group: impl FnOnce() -> io::Result<()>,
) -> Result<ExitCode, ExecError>
where
    P: PluginSet,
    S: ActionIsolationPort,
    L: ActionLimitsPort,
{
    run_inner::<P, S, L>(
        invocation,
        build_sandbox,
        build_limits,
        leave_action_group,
        || Ok(()),
    )
}

/// Runs an invocation, starting its guardian after the limits plugin is built when configured.
fn run_inner<P, S, L>(
    invocation: &Invocation<'_>,
    build_sandbox: impl FnOnce(P::SandboxKey, P::Sandbox, &PluginContext<'_>) -> Result<S, PortError>,
    build_limits: impl FnOnce(P::Limits, &PluginContext<'_>) -> Result<L, PortError>,
    leave_action_group: impl FnOnce() -> io::Result<()>,
    start_guardian: impl FnOnce() -> Result<(), ExecError>,
) -> Result<ExitCode, ExecError>
where
    P: PluginSet,
    S: ActionIsolationPort,
    L: ActionLimitsPort,
{
    let command = ActionCommand::from_argv(invocation.argv).ok_or(ExecError::NoCommand)?;
    if !invocation.entrypoint.is_absolute() {
        return Err(ExecError::EntrypointNotAbsolute {
            entrypoint: invocation.entrypoint.to_path_buf(),
        });
    }
    let path = SandboxFile::<P>::beside(invocation.entrypoint);
    let file = read_sandbox_file::<P>(&path)?;
    let properties = ActionProperties::from_environment(invocation.environment)?;
    let directory = ActionDirectory::try_new(&file.work_directory, invocation.working_directory)?;
    let context = PluginContext { config_path: &path };

    let sandbox =
        build_sandbox(file.selected, file.sandbox, &context).map_err(ExecError::Sandbox)?;
    let limits = file
        .limits
        .map(|config| build_limits(config, &context))
        .transpose()
        .map_err(ExecError::Limits)?;
    if limits.is_some() {
        start_guardian()?;
    }

    let action = Action {
        spec: ActionExecutionSpec {
            command,
            environment: invocation.environment,
            action_directory: &directory.path,
            working_directory: &directory.working_directory,
            network: properties.network,
            launcher: &[],
        },
        limits: properties.limits,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ExecError::Runtime)?;
    let outcome = runtime.block_on(async {
        let mut terminations = terminations::listen().map_err(ExecError::Signals)?;
        execute(
            &sandbox,
            limits.as_ref(),
            file.execution.input_links,
            &action,
            &mut terminations,
            leave_action_group,
        )
        .await
    })?;
    Ok(ExitCode::from(exit_status(outcome)))
}

/// Reads and parses the sandbox file at `path`.
///
/// # Errors
///
/// Returns [`ExecError`] if the file cannot be read or parsed.
pub fn read_sandbox_file<P: PluginSet>(path: &Path) -> Result<SandboxFile<P>, ExecError> {
    let text = fs::read_to_string(path).map_err(|source| ExecError::ReadSandboxFile {
        path: path.to_path_buf(),
        source,
    })?;
    text.parse::<SandboxFile<P>>()
        .map_err(|error| ExecError::SandboxFile {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })
}
