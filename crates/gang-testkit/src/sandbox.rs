//! A scripted sandbox double: it logs starts and waits, records every spec it was started with, and
//! ends each action with a scripted outcome.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, NetworkAccess, PortError,
    SandboxedAction,
};
use gang_types::ActionEnvironment;
use parking_lot::Mutex;

use crate::CallLog;

/// The spec of one action a [`ScriptedSandbox`] was started with, owned so a test can keep it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedAction {
    /// The command and its arguments.
    pub argv: Vec<OsString>,
    /// The complete environment of the command.
    pub environment: Vec<(OsString, OsString)>,
    /// The directory that holds everything of the action; the working directory is inside it.
    pub action_directory: PathBuf,
    /// The directory the command runs in.
    pub working_directory: PathBuf,
    /// Read-only input trees passed to the sandbox as `(source, target)` pairs.
    pub read_only_trees: Vec<(PathBuf, PathBuf)>,
    /// Action-local ephemeral scratch directories passed to the sandbox.
    pub ephemeral_dirs: Vec<PathBuf>,
    /// The trusted egress policy and controller endpoints, or none for offline actions.
    pub network: Option<(gang_types::PublicEgressPolicy, Box<[gang_types::Url]>)>,
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
    environment: ActionEnvironment,
    log: CallLog,
    start: Start,
    failing_wait: bool,
    started: Arc<Mutex<Vec<StartedAction>>>,
}

impl ScriptedSandbox {
    /// Creates a sandbox whose actions resolve to `Ok(outcome)` when waited for.
    ///
    /// # Errors
    ///
    /// Returns an error if the test host identity is invalid.
    pub fn try_exiting(
        log: &CallLog,
        outcome: ActionExecutionOutcome,
    ) -> Result<Self, gang_types::ActionEnvironmentError> {
        Self::try_with_start(log, Start::Exits(outcome))
    }

    /// Creates a sandbox whose every start fails with a backend error.
    ///
    /// # Errors
    ///
    /// Returns an error if the test host identity is invalid.
    pub fn try_failing_to_start(log: &CallLog) -> Result<Self, gang_types::ActionEnvironmentError> {
        Self::try_with_start(log, Start::Fails)
    }

    /// Sets the environment reported by the sandbox.
    #[must_use]
    pub fn with_environment(mut self, environment: ActionEnvironment) -> Self {
        self.environment = environment;
        self
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

    fn try_with_start(
        log: &CallLog,
        start: Start,
    ) -> Result<Self, gang_types::ActionEnvironmentError> {
        Ok(Self {
            environment: "host:test".parse()?,
            log: log.clone(),
            start,
            failing_wait: false,
            started: Arc::default(),
        })
    }
}

impl ActionIsolationPort for ScriptedSandbox {
    fn environment(&self) -> &ActionEnvironment {
        &self.environment
    }

    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        self.log.record("sandbox.start");
        self.started.lock().push(StartedAction {
            argv: std::iter::once(spec.command.program().to_os_string())
                .chain(spec.command.args().iter().cloned())
                .collect(),
            environment: spec.environment.to_vec(),
            action_directory: spec.action_directory.to_path_buf(),
            working_directory: spec.working_directory.to_path_buf(),
            read_only_trees: spec
                .read_only_trees
                .iter()
                .map(|tree| (tree.source.to_path_buf(), tree.target.to_path_buf()))
                .collect(),
            ephemeral_dirs: spec
                .ephemeral_dirs
                .iter()
                .map(|path| path.to_path_buf())
                .collect(),
            network: match spec.network {
                NetworkAccess::Offline => None,
                NetworkAccess::PublicEgress {
                    policy,
                    controller_endpoints,
                } => Some((policy.clone(), controller_endpoints.into())),
            },
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
        NetworkAccess, PortError, SandboxedAction,
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

    /// Starts one action with the sample spec: `sh -c true` in `/work`, offline.
    fn start_action(
        sandbox: &ScriptedSandbox,
    ) -> Result<SandboxedAction, Box<dyn std::error::Error>> {
        Ok(ready(sandbox.start(&ActionExecutionSpec {
            command: ActionCommand::from_argv(&sample_argv()).ok_or("empty argv")?,
            environment: &sample_environment(),
            action_directory: Path::new("/work"),
            working_directory: Path::new("/work"),
            read_only_trees: &[],
            ephemeral_dirs: &[],
            network: NetworkAccess::Offline,
            memory_cgroup: None,
            launcher: &[],
        }))?)
    }

    #[test]
    fn reports_configured_environment() -> TestResult {
        let identity = "a".repeat(64).parse()?;
        let sandbox =
            ScriptedSandbox::try_exiting(&CallLog::default(), ActionExecutionOutcome::Exited(0))?
                .with_environment(identity);
        assert_eq!(sandbox.environment().as_str(), "a".repeat(64));
        Ok(())
    }

    #[test]
    fn exiting_sandbox_starts_then_resolves_to_its_outcome() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(3))?;

        let mut action = start_action(&sandbox)?;
        assert_eq!(log.calls(), ["sandbox.start"]);

        assert_eq!(ready(action.wait())?, ActionExecutionOutcome::Exited(3));
        assert_eq!(log.calls(), ["sandbox.start", "sandbox.wait"]);
        Ok(())
    }

    #[test]
    fn failing_start_logs_the_start_and_errors_with_backend() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;

        assert!(matches!(
            start_action(&sandbox)
                .err()
                .and_then(|error| error.downcast::<PortError>().ok()),
            Some(error) if matches!(*error, PortError::Backend(_))
        ));
        assert_eq!(log.calls(), ["sandbox.start"]);
        Ok(())
    }

    #[test]
    fn failing_wait_resolves_the_action_to_a_backend_error() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?
            .failing_to_wait();

        let mut action = start_action(&sandbox)?;

        assert!(matches!(ready(action.wait()), Err(PortError::Backend(_))));
        assert_eq!(log.calls(), ["sandbox.start", "sandbox.wait"]);
        Ok(())
    }

    #[test]
    fn records_public_egress_policy_and_controller_endpoints() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
        let policy = gang_types::PublicEgressPolicy {
            protected_networks: vec!["10.0.0.0/8".parse()?].into_boxed_slice(),
            dns_servers: vec!["1.1.1.1".parse()?].into_boxed_slice(),
            setup_timeout: gang_types::NonZeroDuration::from_secs(10),
        };
        let endpoints = vec!["https://controller.example:443".parse()?].into_boxed_slice();
        let argv = sample_argv();
        let environment = sample_environment();
        let action = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            environment: &environment,
            action_directory: Path::new("/work"),
            working_directory: Path::new("/work"),
            read_only_trees: &[],
            ephemeral_dirs: &[],
            network: NetworkAccess::PublicEgress {
                policy: &policy,
                controller_endpoints: &endpoints,
            },
            memory_cgroup: None,
            launcher: &[],
        };
        let _action = ready(sandbox.start(&action))?;
        assert_eq!(sandbox.started()[0].network, Some((policy, endpoints)));
        Ok(())
    }

    #[test]
    fn started_records_every_spec_including_failed_starts() -> TestResult {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;

        assert!(start_action(&sandbox).is_err());

        assert_eq!(
            sandbox.started(),
            [StartedAction {
                argv: sample_argv(),
                environment: sample_environment(),
                action_directory: PathBuf::from("/work"),
                working_directory: PathBuf::from("/work"),
                read_only_trees: Vec::new(),
                ephemeral_dirs: vec![],
                network: None,
                launcher: Vec::new(),
            }]
        );
        Ok(())
    }
}
