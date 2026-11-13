//! Runs one action in its sandbox, inside its limits scope, and maps the outcome to an exit status.

use std::io;

use gang_config::InputLinks;
use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionLimits,
    ActionLimitsPort, ActionScope, ActionUsage, SandboxedAction, Termination,
};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{ExecError, break_hard_links};

/// One action as `gang exec` receives it: what to run and the limits it was granted.
#[derive(Debug, Clone, Copy)]
pub struct Action<'a> {
    /// The command, environment, working directory and network request of the action.
    pub spec: ActionExecutionSpec<'a>,
    /// The resources the action reserves; enforced only when a limits port is given.
    pub limits: ActionLimits,
}

/// Runs `action` in `sandbox`.
///
/// The steps, in order, are: break the hard links under the action directory when `input_links` is
/// `break`; enter a limits scope when `limits` is given, which creates and limits the scope while
/// `gang exec` stays outside it; start the action through the scope's launcher, so it runs inside the scope
/// from its first instruction; call `leave_action_group` once an action that shares the process
/// group of `gang exec` exists (the caller moves `gang exec` out of that group; an action in its
/// own group, which the sandbox relays to, leaves `gang exec` where it is); wait for the action,
/// while each termination is relayed to the action (`SandboxedAction::relay`); leave the scope.
/// A termination pending before the action starts prevents it from being started.
///
/// `gang exec` never enters the scope, so an out-of-memory kill of the scope never reaches it.
/// The scope is left whenever it was entered, even when the sandbox failed. A sandbox or limits
/// error from the run takes precedence over a failure to leave the scope.
///
/// # Errors
///
/// - [`ExecError::LinkBreak`] when a hard link cannot be broken; the sandbox does not start.
/// - [`ExecError::Limits`] when the scope cannot be entered; the sandbox does not start. It is
///   also returned when the action succeeded but the scope cannot be left.
/// - [`ExecError::Sandbox`] when the sandbox cannot start the action or the action fails.
/// - [`ExecError::LeaveActionGroup`] when `leave_action_group` fails; the action is dropped.
pub async fn execute<S: ActionIsolationPort, L: ActionLimitsPort>(
    sandbox: &S,
    limits: Option<&L>,
    input_links: InputLinks,
    action: &Action<'_>,
    terminations: &mut UnboundedReceiver<Termination>,
    leave_action_group: impl FnOnce() -> io::Result<()>,
) -> Result<ActionExecutionOutcome, ExecError> {
    if matches!(input_links, InputLinks::Break) {
        break_hard_links(action.spec.action_directory)?;
    }
    let Some(port) = limits else {
        if let Ok(termination) = terminations.try_recv() {
            return Ok(ActionExecutionOutcome::Signaled(termination.signal()));
        }
        let mut running = sandbox
            .start(&action.spec)
            .await
            .map_err(ExecError::Sandbox)?;
        leave_if_shared(&running, leave_action_group)?;
        return wait_relaying(&mut running, terminations).await;
    };
    let scope = port
        .enter(&action.limits)
        .await
        .map_err(ExecError::Limits)?;
    let spec = ActionExecutionSpec {
        launcher: scope.launcher(),
        ..action.spec
    };
    let outcome = if let Ok(termination) = terminations.try_recv() {
        Ok(ActionExecutionOutcome::Signaled(termination.signal()))
    } else {
        match sandbox.start(&spec).await {
            Ok(mut running) => match leave_if_shared(&running, leave_action_group) {
                Ok(()) => wait_relaying(&mut running, terminations).await,
                Err(error) => Err(error),
            },
            Err(error) => Err(ExecError::Sandbox(error)),
        }
    };
    let usage = port.leave(scope).await;
    match (outcome, usage) {
        (Ok(outcome), Ok(usage)) => {
            log_usage(&usage);
            Ok(outcome)
        }
        (Ok(_), Err(error)) => Err(ExecError::Limits(error)),
        (Err(error), Ok(usage)) => {
            log_usage(&usage);
            Err(error)
        }
        (Err(error), Err(leave)) => {
            tracing::warn!(error = %gang_types::error_chain(&leave), "cannot leave the action's limits scope");
            Err(error)
        }
    }
}

/// Waits for `running` to end, passing each termination that `terminations` receives on to the
/// action. The wait is kept across relays, so it resumes where it left off.
async fn wait_relaying(
    running: &mut SandboxedAction,
    terminations: &mut UnboundedReceiver<Termination>,
) -> Result<ActionExecutionOutcome, ExecError> {
    loop {
        tokio::select! {
            outcome = running.wait() => return outcome.map_err(ExecError::Sandbox),
            Some(termination) = terminations.recv() => {
                running.relay(termination).map_err(ExecError::Sandbox)?;
            }
        }
    }
}

/// Moves `gang exec` out of the action's process group when the action shares it, so the group
/// kill at the end of the kill grace spares `gang exec`. An action in its own process group keeps
/// `gang exec` in the original group, which `NativeLink` signals, so every termination reaches
/// `gang exec` and is relayed, including one that arrives before the sandbox has started the
/// action.
fn leave_if_shared(
    running: &SandboxedAction,
    leave_action_group: impl FnOnce() -> io::Result<()>,
) -> Result<(), ExecError> {
    if running.shares_host_group() {
        leave_action_group().map_err(ExecError::LeaveActionGroup)
    } else {
        Ok(())
    }
}

/// Maps an action's outcome to the exit status `gang exec` reports.
///
/// An exit code is kept as is. A signal `n` is reported as `128 + n`, the shell convention. Only
/// the low 8 bits survive the operating system's exit status, so the result is those bits, which
/// is what the process exit truncates to anyway.
#[must_use]
pub fn exit_status(outcome: ActionExecutionOutcome) -> u8 {
    match outcome {
        ActionExecutionOutcome::Exited(code) => code.to_le_bytes()[0],
        ActionExecutionOutcome::Signaled(signal) => signal.saturating_add(128).to_le_bytes()[0],
    }
}

/// Records what the limits scope measured for the action.
fn log_usage(usage: &ActionUsage) {
    tracing::info!(
        peak_memory = ?usage.peak_memory,
        cpu_time = ?usage.cpu_time,
        "action finished"
    );
}
