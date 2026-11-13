//! Isolation: starting one action in a sandbox and waiting for how it ended.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::Path;
use std::pin::Pin;

use crate::{PortError, port};

/// The file name of the `gang-exec` entrypoint link used to run an action.
pub const EXEC_LINK_NAME: &str = "gang-exec";

/// The environment variable the worker configuration exports the
/// [`gang_types::property::NETWORK`] value to, read by `gang exec`. Unset means network access
/// is not requested.
pub const NETWORK_ENV: &str = "GANG_NETWORK";

/// The multicall name used to reset the signal mask before starting a sandboxed action.
pub const RESET_LINK_NAME: &str = "gang-reset";

/// A sandbox for actions.
#[port]
pub trait ActionIsolationPort: Send + Sync {
    /// Starts the action with the caller's standard streams and returns as soon as it is running.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the sandbox cannot start the action.
    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError>;
}

/// A termination request the process hosting an action received: `SIGTERM` or `SIGINT`.
///
/// `NativeLink` sends one to the host's process group when an action times out or is cancelled,
/// and kills the group once its kill grace has passed. The host stays alive for that grace and
/// passes the request on with [`SandboxedAction::relay`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// `SIGTERM`.
    Terminate,
    /// `SIGINT`.
    Interrupt,
}

impl Termination {
    /// The signal number, the same on every Unix the worker runs on.
    #[must_use]
    pub const fn signal(self) -> i32 {
        match self {
            Self::Terminate => 15,
            Self::Interrupt => 2,
        }
    }
}

/// Delivers a [`Termination`] to an action outside its host's process group.
type Relay = Box<dyn Fn(Termination) -> Result<(), PortError> + Send + Sync>;

/// An action a sandbox has started. Waiting reports how it ended; dropping it without waiting
/// kills the action.
#[must_use]
pub struct SandboxedAction {
    wait: Pin<Box<dyn Future<Output = Result<ActionExecutionOutcome, PortError>> + Send>>,
    /// `None` when the action shares its host's process group.
    relay: Option<Relay>,
}

impl SandboxedAction {
    /// Wraps the wait for an already spawned action that shares its host's process group, so a
    /// signal sent to that group reaches it directly. The future owns the process handle (spawned
    /// with kill-on-drop), so dropping the `SandboxedAction` ends the action; it does not borrow
    /// the sandbox or the spec.
    pub fn new(
        wait: impl Future<Output = Result<ActionExecutionOutcome, PortError>> + Send + 'static,
    ) -> Self {
        Self {
            wait: Box::pin(wait),
            relay: None,
        }
    }

    /// Wraps the wait for an already spawned action outside its host's process group, as
    /// [`Self::new`] does; `relay` delivers each [`Termination`] the host receives to it.
    pub fn relayed(
        wait: impl Future<Output = Result<ActionExecutionOutcome, PortError>> + Send + 'static,
        relay: impl Fn(Termination) -> Result<(), PortError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            wait: Box::pin(wait),
            relay: Some(Box::new(relay)),
        }
    }

    /// Passes `termination` on to the action. An action that shares its host's process group
    /// received the signal with its host, so nothing is sent to it again.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the sandbox cannot deliver the signal.
    pub fn relay(&self, termination: Termination) -> Result<(), PortError> {
        self.relay
            .as_ref()
            .map_or(Ok(()), |relay| relay(termination))
    }

    /// Whether the action shares its host's process group, so a signal sent to that group reaches it
    /// directly: `true` for [`Self::new`], `false` for [`Self::relayed`].
    #[must_use]
    pub fn shares_host_group(&self) -> bool {
        self.relay.is_none()
    }

    /// Waits for the action to end. Dropping the returned future keeps the action running, so
    /// the wait can be raced against termination requests and resumed; it must not be resumed
    /// once it has completed.
    ///
    /// # Errors
    ///
    /// [`PortError`] when waiting fails or the exit status is unusable.
    pub async fn wait(&mut self) -> Result<ActionExecutionOutcome, PortError> {
        self.wait.as_mut().await
    }
}

impl fmt::Debug for SandboxedAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxedAction")
            .finish_non_exhaustive()
    }
}

/// A command to run: a program and its arguments, never empty.
#[derive(Debug, Clone, Copy)]
pub struct ActionCommand<'a> {
    program: &'a OsStr,
    args: &'a [OsString],
}

impl<'a> ActionCommand<'a> {
    /// The command named by `argv`, or `None` when it is empty.
    #[must_use]
    pub fn from_argv(argv: &'a [OsString]) -> Option<Self> {
        let (program, arguments) = argv.split_first()?;
        Some(Self {
            program,
            args: arguments,
        })
    }

    /// The program.
    #[must_use]
    pub const fn program(&self) -> &'a OsStr {
        self.program
    }

    /// The arguments after the program.
    #[must_use]
    pub const fn args(&self) -> &'a [OsString] {
        self.args
    }
}

/// An action to run.
#[derive(Debug, Clone, Copy)]
pub struct ActionExecutionSpec<'a> {
    /// The command to run.
    pub command: ActionCommand<'a>,
    /// The complete environment of the command.
    pub environment: &'a [(OsString, OsString)],
    /// The directory that holds everything of this one action: its inputs, its outputs and the
    /// working directory. The only writable location besides temporary space; a sandbox that
    /// isolates exposes all of it, so inputs outside the working directory resolve.
    pub action_directory: &'a Path,
    /// The directory the command runs in, inside `action_directory`.
    pub working_directory: &'a Path,
    /// Whether the action asked for network access.
    pub network: NetworkRequest,
    /// The command prefix the sandbox starts its own first process through: empty without a
    /// limits plugin; otherwise [`crate::ActionLimitsPort::launcher`], which starts that process
    /// inside the action's limits scope. The sandbox runs `launcher` followed by the command it
    /// would run on its own.
    pub launcher: &'a [OsString],
}

gang_types::string_enum! {
    /// Whether the action asked for network access. A sandbox grants it only if its own
    /// configuration permits.
    pub enum NetworkRequest("network request") {
        /// The action needs no network access.
        NotRequested = "not-requested",
        /// The action asked for network access.
        Requested = "requested",
    }
}

/// How an action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum ActionExecutionOutcome {
    /// The command exited with this status.
    Exited(i32),
    /// The command was killed by this signal.
    Signaled(i32),
}

#[cfg(unix)]
impl TryFrom<std::process::ExitStatus> for ActionExecutionOutcome {
    type Error = PortError;

    fn try_from(status: std::process::ExitStatus) -> Result<Self, Self::Error> {
        use std::os::unix::process::ExitStatusExt;

        if let Some(code) = status.code() {
            Ok(Self::Exited(code))
        } else if let Some(signal) = status.signal() {
            Ok(Self::Signaled(signal))
        } else {
            Err(PortError::backend(std::io::Error::other(
                "process status has neither exit code nor signal",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::ffi::{OsStr, OsString};
    use std::sync::Arc;

    use gang_testkit::must_ready as ready;
    use parking_lot::Mutex;

    use super::{
        ActionCommand, ActionExecutionOutcome, NetworkRequest, PortError, SandboxedAction,
        Termination,
    };

    #[test]
    fn command_requires_a_program_and_preserves_arguments() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(ActionCommand::from_argv(&[]).is_none());

        let argv = [OsString::from("program"), OsString::from("argument")];
        let command = ActionCommand::from_argv(&argv).ok_or("empty argv")?;
        assert_eq!(command.program(), OsStr::new("program"));
        assert_eq!(command.args(), &argv[1..]);

        let command = ActionCommand::from_argv(&argv[..1]).ok_or("empty argv")?;
        assert_eq!(command.program(), OsStr::new("program"));
        assert!(command.args().is_empty());
        Ok(())
    }

    #[test]
    fn network_request_round_trips_through_its_text() {
        for request in [NetworkRequest::NotRequested, NetworkRequest::Requested] {
            let text = request.to_string();

            assert_eq!(text.parse::<NetworkRequest>().ok(), Some(request));
        }
    }

    #[test]
    fn network_request_text_is_the_accepted_value() {
        assert_eq!(NetworkRequest::NotRequested.as_str(), "not-requested");
        assert_eq!(NetworkRequest::Requested.to_string(), "requested");
    }

    #[test]
    fn unknown_network_request_lists_the_accepted_names() {
        let error = "networked"
            .parse::<NetworkRequest>()
            .expect_err("unknown text parses");

        assert_eq!(
            error.to_string(),
            "unknown network request `networked`, expected one of not-requested, requested"
        );
    }

    #[test]
    fn sandboxed_action_waits_to_its_outcome() -> Result<(), Box<dyn Error>> {
        let mut running = SandboxedAction::new(async { Ok(ActionExecutionOutcome::Exited(3)) });

        assert_eq!(ready(running.wait())?, ActionExecutionOutcome::Exited(3));
        Ok(())
    }

    #[test]
    fn sandboxed_action_passes_wait_errors_through() {
        let mut running = SandboxedAction::new(async { Err(PortError::config("wait failed")) });

        assert!(matches!(
            ready(running.wait()),
            Err(PortError::Config(cause)) if cause.to_string() == "wait failed"
        ));
    }

    #[test]
    fn a_shared_group_action_relays_nothing() {
        let running = SandboxedAction::new(async { Ok(ActionExecutionOutcome::Exited(0)) });

        assert!(running.relay(Termination::Terminate).is_ok());
    }

    #[test]
    fn a_relayed_action_passes_each_termination_to_its_relay() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let relay_seen = Arc::clone(&seen);
        let running = SandboxedAction::relayed(
            async { Ok(ActionExecutionOutcome::Exited(0)) },
            move |termination| {
                relay_seen.lock().push(termination);
                Ok(())
            },
        );

        assert!(running.relay(Termination::Terminate).is_ok());
        assert!(running.relay(Termination::Interrupt).is_ok());
        assert_eq!(
            *seen.lock(),
            [Termination::Terminate, Termination::Interrupt]
        );
    }

    #[test]
    fn a_relay_error_is_returned() {
        let running =
            SandboxedAction::relayed(async { Ok(ActionExecutionOutcome::Exited(0)) }, |_| {
                Err(PortError::config("relay failed"))
            });

        assert!(matches!(
            running.relay(Termination::Interrupt),
            Err(PortError::Config(cause)) if cause.to_string() == "relay failed"
        ));
    }

    #[test]
    fn termination_signals_are_sigterm_and_sigint() {
        assert_eq!(Termination::Terminate.signal(), 15);
        assert_eq!(Termination::Interrupt.signal(), 2);
    }
}

#[cfg(all(test, unix))]
mod unix_tests {
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    use super::ActionExecutionOutcome;

    #[test]
    fn exit_code_converts_to_exited() {
        let status = ExitStatus::from_raw(3 << 8);

        assert_eq!(
            ActionExecutionOutcome::try_from(status).expect("unix status converts"),
            ActionExecutionOutcome::Exited(3)
        );
    }

    #[test]
    fn killing_signal_converts_to_signaled() {
        let status = ExitStatus::from_raw(9);

        assert_eq!(
            ActionExecutionOutcome::try_from(status).expect("unix status converts"),
            ActionExecutionOutcome::Signaled(9)
        );
    }
}
