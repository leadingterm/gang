//! A scripted sandbox double: it logs starts and waits, records every spec it was started with, and
//! ends each action with a scripted outcome.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, NetworkRequest, PortError,
    SandboxedAction,
};
use parking_lot::Mutex;

use crate::CallLog;

/// The spec of one action a [`ScriptedSandbox`] was started with, owned so a test can keep it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedAction {
    /// The command and its arguments.
    pub argv: Vec<OsString>,
    /// The complete environment of the command.
    pub environment: Vec<(OsString, OsString)>,
    /// The directory the command runs in.
    pub working_directory: PathBuf,
    /// Whether the action asked for network access.
    pub network: NetworkRequest,
    /// The command prefix the action was started through: empty when it started without a limits scope.
    pub launcher: Vec<OsString>,
}

/// How a [`ScriptedSandbox`] starts its actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    /// The action starts and ends with this outcome.
    Exits(ActionExecutionOutcome),
    /// Starting fails.
    Fails,
}

/// A sandbox double that logs `sandbox.start` and `sandbox.wait`, records every spec it was started
/// with, and ends each started action with a scripted outcome. Clones share what they were asked.
#[derive(Clone)]
pub struct ScriptedSandbox {
    log: CallLog,
    start: Start,
    failing_wait: bool,
    started: Arc<Mutex<Vec<StartedAction>>>,
}

impl ScriptedSandbox {
    /// Creates a sandbox whose every action starts and resolves to `Ok(outcome)` when waited for.
    #[must_use]
    pub fn exiting(log: &CallLog, outcome: ActionExecutionOutcome) -> Self {
        Self::with_start(log, Start::Exits(outcome))
    }

    /// Creates a sandbox whose every start fails with a backend error.
    #[must_use]
    pub fn failing_to_start(log: &CallLog) -> Self {
        Self::with_start(log, Start::Fails)
    }

    /// Makes every started action resolve to a backend error when waited for.
    #[must_use]
    pub fn failing_to_wait(mut self) -> Self {
        self.failing_wait = true;
        self
    }

    /// Every spec the sandbox was started with, in the order the starts were made, including starts
    /// that failed.
    #[must_use]
    pub fn started(&self) -> Vec<StartedAction> {
        self.started.lock().clone()
    }

    fn with_start(log: &CallLog, start: Start) -> Self {
        Self {
            log: log.clone(),
            start,
            failing_wait: false,
            started: Arc::default(),
        }
    }
}

impl ActionIsolationPort for ScriptedSandbox {
    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        self.log.record("sandbox.start");
        self.started.lock().push(StartedAction {
            argv: std::iter::once(spec.command.program().to_os_string())
                .chain(spec.command.args().iter().cloned())
                .collect(),
            environment: spec.environment.to_vec(),
            working_directory: spec.working_directory.to_path_buf(),
            network: spec.network,
            launcher: spec.launcher.to_vec(),
        });
        let outcome = match self.start {
            Start::Exits(outcome) => outcome,
            Start::Fails => return Err(PortError::backend("sandbox start failed")),
        };
        let log = self.log.clone();
        let failing_wait = self.failing_wait;
        Ok(SandboxedAction::new(async move {
            log.record("sandbox.wait");
            if failing_wait {
                return Err(PortError::backend("sandbox wait failed"));
            }
            Ok(outcome)
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    use gang_spi::{
        ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort,
        NetworkRequest, PortError, SandboxedAction,
    };

    use super::{ScriptedSandbox, StartedAction};
    use crate::{CallLog, TestResult, must_ready as ready};

    fn sample_argv() -> Vec<OsString> {
        vec![
            OsString::from("sh"),
            OsString::from("-c"),
            OsString::from("true"),
        ]
    }

    fn sample_environment() -> Vec<(OsString, OsString)> {
        vec![(OsString::from("PATH"), OsString::from("/bin"))]
    }

    /// Starts one action with the sample spec: `sh -c true` in `/work`, network requested.
    fn start_action(
        sandbox: &ScriptedSandbox,
    ) -> Result<SandboxedAction, Box<dyn std::error::Error>> {
        Ok(ready(sandbox.start(&ActionExecutionSpec {
            command: ActionCommand::from_argv(&sample_argv()).ok_or("empty argv")?,
            environment: &sample_environment(),
            working_directory: Path::new("/work"),
            network: NetworkRequest::Requested,
            launcher: &[],
        }))?)
    }

    #[test]
    fn exiting_sandbox_starts_then_resolves_to_its_outcome() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::exiting(&log, ActionExecutionOutcome::Exited(3));

        let mut action = start_action(&sandbox)?;
        assert_eq!(log.calls(), ["sandbox.start"]);

        assert_eq!(ready(action.wait())?, ActionExecutionOutcome::Exited(3));
        assert_eq!(log.calls(), ["sandbox.start", "sandbox.wait"]);
        Ok(())
    }

    #[test]
    fn failing_start_logs_the_start_and_errors_with_backend() {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::failing_to_start(&log);

        assert!(matches!(
            start_action(&sandbox)
                .err()
                .and_then(|error| error.downcast::<PortError>().ok()),
            Some(error) if matches!(*error, PortError::Backend(_))
        ));
        assert_eq!(log.calls(), ["sandbox.start"]);
    }

    #[test]
    fn failing_wait_resolves_the_action_to_a_backend_error() -> TestResult {
        let log = CallLog::default();
        let sandbox =
            ScriptedSandbox::exiting(&log, ActionExecutionOutcome::Exited(0)).failing_to_wait();

        let mut action = start_action(&sandbox)?;

        assert!(matches!(ready(action.wait()), Err(PortError::Backend(_))));
        assert_eq!(log.calls(), ["sandbox.start", "sandbox.wait"]);
        Ok(())
    }

    #[test]
    fn started_records_every_spec_including_failed_starts() {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::failing_to_start(&log);

        assert!(start_action(&sandbox).is_err());

        assert_eq!(
            sandbox.started(),
            [StartedAction {
                argv: sample_argv(),
                environment: sample_environment(),
                working_directory: PathBuf::from("/work"),
                network: NetworkRequest::Requested,
                launcher: Vec::new(),
            }]
        );
    }
}
