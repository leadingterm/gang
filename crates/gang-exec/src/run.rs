//! The `gang exec` entry point: from the process invocation to the action's exit status.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use gang_config::{PluginSet, SandboxFile};
use gang_spi::{
    ActionCommand, ActionExecutionSpec, ActionIsolationPort, ActionLimitsPort, GUARD_LINK_NAME,
    NetworkAccess, ReadOnlyTree, SIDE_CHANNEL_FILE_ENV,
};
use gang_spi::{PluginContext, PortError};
use gang_types::{Capability, NetworkRequest};

use crate::action_directory::ActionDirectory;
use crate::errors::ExecError;
use crate::execute::{Action, ExecutionHooks, execute, exit_status};
use crate::properties::ActionProperties;
use crate::side_channel;
use crate::terminations;

/// The process `gang exec` was started with: what it runs and where.
#[derive(Debug, Clone, Copy)]
pub struct Invocation<'a> {
    /// The absolute path of the `gang-exec` link the process was started through.
    pub entrypoint: &'a Path,
    /// The command and its arguments; the first is the program.
    pub argv: &'a [OsString],
    /// The environment the process was started with; the side-channel path is withheld.
    pub environment: &'a [(OsString, OsString)],
    /// The working directory the process was started in.
    pub working_directory: &'a Path,
}

/// Runs the action `argv` as `gang exec` does and returns its exit status.
///
/// Takes the entrypoint, the arguments, the environment and the working directory from the
/// process, and runs them with [`run_with`].
///
/// The worker signals action process groups and eventually kills `gang exec` itself by PID.
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
        |action_dir, guardian_pid| {
            gang_os::DescendantTracker::start(action_dir, guardian_pid).map(Some)
        },
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
                .map(|child| Some(child.id()))
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
/// The side-channel path is withheld from the sandbox and validated against the protected
/// per-action directory beneath the canonical work root before creating a new report.
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
        |_, _| Ok(None),
        || Ok(None),
    )
}

fn resolve_network<'a>(
    request: NetworkRequest,
    grants: &std::collections::BTreeSet<Capability>,
    egress: Option<&'a gang_types::PublicEgressPolicy>,
    controller_endpoints: &'a [gang_types::Url],
) -> Result<NetworkAccess<'a>, ExecError> {
    match request {
        NetworkRequest::NotRequested => Ok(NetworkAccess::Offline),
        NetworkRequest::Requested => {
            if !grants.contains(&Capability::Network) {
                return Err(ExecError::NetworkNotGranted);
            }
            let policy = egress.ok_or(ExecError::NetworkPolicyMissing)?;
            if controller_endpoints.is_empty() {
                return Err(ExecError::NetworkEndpointsMissing);
            }
            Ok(NetworkAccess::PublicEgress {
                policy,
                controller_endpoints,
            })
        }
    }
}

/// Runs an invocation, starting its guardian after the limits plugin is built when configured.
fn run_inner<P, S, L>(
    invocation: &Invocation<'_>,
    build_sandbox: impl FnOnce(P::SandboxKey, P::Sandbox, &PluginContext<'_>) -> Result<S, PortError>,
    build_limits: impl FnOnce(P::Limits, &PluginContext<'_>) -> Result<L, PortError>,
    leave_action_group: impl FnOnce() -> io::Result<()>,
    start_tracker: impl FnOnce(&Path, Option<u32>) -> io::Result<Option<gang_os::DescendantTracker>>,
    start_guardian: impl FnOnce() -> Result<Option<u32>, ExecError>,
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
    let network = resolve_network(
        properties.network,
        &file.actions.grants,
        file.execution.egress.as_ref(),
        &file.controller_endpoints,
    )?;
    let directory = ActionDirectory::try_new(&file.work_directory, invocation.working_directory)?;
    let side_channel_inputs = side_channel::read_inputs(invocation.environment, &directory.path)?;
    let context = PluginContext {
        config_path: Path::new(&path),
    };

    let sandbox =
        build_sandbox(file.selected, file.sandbox, &context).map_err(ExecError::Sandbox)?;
    let limits = file
        .limits
        .map(|config| build_limits(config, &context))
        .transpose()
        .map_err(ExecError::Limits)?;
    let guardian_pid = limits.is_some().then(start_guardian).transpose()?.flatten();
    let mut tracker =
        start_tracker(&directory.path, guardian_pid).map_err(ExecError::CleanupActionGroup)?;

    let environment: Vec<_> = invocation
        .environment
        .iter()
        .filter(|(name, _)| name != SIDE_CHANNEL_FILE_ENV)
        .cloned()
        .collect();

    let read_only_trees: Vec<ReadOnlyTree<'_>> = side_channel_inputs
        .read_only_trees
        .iter()
        .map(|tree| ReadOnlyTree {
            source: &tree.source,
            target: &tree.target,
        })
        .collect();
    let ephemeral_dirs: Vec<&Path> = side_channel_inputs
        .ephemeral_dirs
        .iter()
        .map(PathBuf::as_path)
        .collect();
    let mut action_limits = properties.limits;
    action_limits.cpu_weight = side_channel_inputs.cpu_weight;

    let action = Action {
        spec: ActionExecutionSpec {
            command,
            read_only_trees: &read_only_trees,
            ephemeral_dirs: &ephemeral_dirs,
            environment: &environment,
            action_directory: &directory.path,
            working_directory: &directory.working_directory,
            network,
            memory_cgroup: None,
            launcher: &[],
        },
        limits: action_limits,
    };
    run_action(
        &sandbox,
        limits.as_ref(),
        file.execution.input_links,
        &action,
        invocation.environment,
        leave_action_group,
        tracker.as_mut(),
    )
}

fn run_action<S: ActionIsolationPort, L: ActionLimitsPort>(
    sandbox: &S,
    limits: Option<&L>,
    input_links: gang_config::InputLinks,
    action: &Action<'_>,
    environment: &[(OsString, OsString)],
    leave_action_group: impl FnOnce() -> io::Result<()>,
    tracker: Option<&mut gang_os::DescendantTracker>,
) -> Result<ExitCode, ExecError> {
    let action_dir = action.spec.action_directory;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ExecError::Runtime)?;
    let execution = runtime.block_on(async {
        let mut terminations = terminations::listen().map_err(ExecError::Signals)?;
        execute(
            sandbox,
            limits,
            input_links,
            action,
            &mut terminations,
            ExecutionHooks {
                publish_cgroup: |cgroup| {
                    side_channel::publish_cgroup(environment, action_dir, cgroup);
                },
                publish_pid: |pid| {
                    side_channel::publish_pid(environment, action_dir, pid);
                },
                leave_action_group,
                cleanup_action_group: || {
                    if let Some(tracker) = tracker {
                        let raw = i32::try_from(std::process::id())
                            .map_err(|_| io::Error::other("invalid action process group"))?;
                        let group = rustix::process::Pid::from_raw(raw)
                            .ok_or_else(|| io::Error::other("invalid action process group"))?;
                        tracker.drain(group, std::time::Duration::from_secs(10))?;
                    }
                    Ok(())
                },
            },
        )
        .await
    });
    // Cancelled DNS lookups may still occupy the blocking pool; do not wait for them on exit.
    runtime.shutdown_background();
    match execution {
        Ok(execution) => {
            side_channel::report(environment, action_dir, execution.usage);
            Ok(ExitCode::from(exit_status(execution.outcome)))
        }
        Err(failure) => {
            side_channel::report(environment, action_dir, failure.usage);
            Err(failure.error)
        }
    }
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
