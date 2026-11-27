//! Behaviour of running one action: the order of sandbox and limits calls, error precedence,
//! hard-link breaking, and the exit status of an outcome.

use std::error::Error;
use std::ffi::OsString;
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::{Stream, StreamExt};
use gang_config::InputLinks;
use gang_exec::{Action, ExecError, ExecFailure, ExecutionHooks, execute, exit_status};
use gang_spi::{
    ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionLimits,
    ActionLimitsPort, ActionScope, NetworkAccess, PortError, SandboxedAction, Termination,
};
use gang_testkit::{CallLog, LimitsStep, RecordingLimits, ScriptedSandbox};
use gang_types::{Bytes, NonZeroBytes};
use parking_lot::Mutex;
use tokio::sync::Barrier;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A sandbox that records the link counts of the working directory each action starts in, then
/// starts the action on `inner`.
struct LinkCounting {
    inner: ScriptedSandbox,
    counts: Mutex<Vec<u64>>,
}

impl LinkCounting {
    /// Wraps `inner`, with no link counts recorded yet.
    fn new(inner: ScriptedSandbox) -> Self {
        Self {
            inner,
            counts: Mutex::new(Vec::new()),
        }
    }
}

impl ActionIsolationPort for LinkCounting {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        self.inner.environment()
    }

    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        *self.counts.lock() = link_counts(spec.working_directory);
        self.inner.start(spec).await
    }
}

/// The link counts of the regular files directly inside `directory`, sorted.
fn link_counts(directory: &Path) -> Vec<u64> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut counts: Vec<u64> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|metadata| metadata.nlink())
        .collect();
    counts.sort_unstable();
    counts
}

fn limits() -> ActionLimits {
    ActionLimits {
        cpu_weight: None,
        cpu_count: NonZeroU32::new(2),
        memory: Some(NonZeroBytes::kib(1)),
    }
}

fn action<'a>(
    argv: &'a [OsString],
    action_directory: &'a Path,
    working_directory: &'a Path,
) -> Result<Action<'a>, Box<dyn Error>> {
    Ok(Action {
        spec: ActionExecutionSpec {
            read_only_trees: &[],
            ephemeral_dirs: &[],
            command: ActionCommand::from_argv(argv).ok_or("empty argv")?,
            environment: &[],
            action_directory,
            working_directory,
            network: NetworkAccess::Offline,
            memory_cgroup: None,
            launcher: &[],
        },
        limits: limits(),
    })
}

fn termination_channel() -> (
    UnboundedSender<Termination>,
    impl Stream<Item = io::Result<Termination>> + Unpin,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (sender, UnboundedReceiverStream::new(receiver).map(Ok))
}

/// A channel that never receives a termination: its sender is dropped.
fn no_terminations() -> impl Stream<Item = io::Result<Termination>> + Unpin {
    let (sender, receiver) = termination_channel();
    drop(sender);
    receiver
}

/// A sandbox whose action is terminated while it waits: `start` sends `Interrupt` on `sender`,
/// and the action ends once `relay` has received it. Every relayed termination is recorded.
struct Relaying {
    environment: gang_types::ActionEnvironment,
    sender: UnboundedSender<Termination>,
    relayed: Arc<Mutex<Vec<Termination>>>,
}

impl ActionIsolationPort for Relaying {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        &self.environment
    }

    async fn start(&self, _spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        self.sender
            .send(Termination::Interrupt)
            .map_err(PortError::backend)?;
        let (done, finished) = oneshot::channel();
        let done = Mutex::new(Some(done));
        let relayed = Arc::clone(&self.relayed);
        Ok(SandboxedAction::relayed(
            async move {
                finished.await.map_err(PortError::backend)?;
                Ok(ActionExecutionOutcome::Exited(0))
            },
            move |termination| {
                relayed.lock().push(termination);
                if let Some(done) = done.lock().take() {
                    let _ = done.send(());
                }
                Ok(())
            },
        ))
    }
}

/// Records when a started action is dropped, as a kill-on-drop action would be.
struct DropAction(CallLog);

impl Drop for DropAction {
    fn drop(&mut self) {
        self.0.record("sandbox.drop");
    }
}

/// An action whose lifetime is visible even if its wait is never polled.
struct DropSandbox(CallLog, gang_types::ActionEnvironment);

impl ActionIsolationPort for DropSandbox {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        &self.1
    }

    async fn start(&self, _spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        self.0.record("sandbox.start");
        let guard = DropAction(self.0.clone());
        Ok(SandboxedAction::new(async move {
            let _guard = guard;
            Ok(ActionExecutionOutcome::Exited(0))
        }))
    }
}

/// Holds startup at a barrier and records cancellation before a payload could be released.
struct PendingStartup {
    environment: gang_types::ActionEnvironment,
    log: CallLog,
    started: Arc<Barrier>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl ActionIsolationPort for PendingStartup {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        &self.environment
    }

    async fn start(&self, _spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        self.log.record("sandbox.start");
        let _guard = DropAction(self.log.clone());
        let release = self
            .release
            .lock()
            .take()
            .ok_or_else(|| PortError::backend("startup is called once"))?;
        self.started.wait().await;
        release.await.map_err(PortError::backend)?;
        self.log.record("payload");
        Ok(SandboxedAction::new(async {
            Ok(ActionExecutionOutcome::Exited(0))
        }))
    }
}

/// The number of hard links to the file at `path`.
fn link_count(path: &Path) -> Result<u64, Box<dyn Error>> {
    Ok(std::fs::metadata(path)?.nlink())
}

/// Creates `root/partner` and `root/action/inputs/shared`, hard-linked to each other, and the
/// working directory `root/action/work`. Returns the action directory `root/action` and the
/// working directory.
fn linked_inputs(root: &Path) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    let partner = root.join("partner");
    std::fs::write(&partner, b"shared")?;
    let inputs = root.join("action/inputs");
    std::fs::create_dir_all(&inputs)?;
    std::fs::hard_link(&partner, inputs.join("shared"))?;
    let working_directory = root.join("action/work");
    std::fs::create_dir(&working_directory)?;
    Ok((root.join("action"), working_directory))
}

#[test]
fn exit_status_maps_exit_codes_and_signals() {
    assert_eq!(exit_status(ActionExecutionOutcome::Exited(0)), 0);
    assert_eq!(exit_status(ActionExecutionOutcome::Exited(3)), 3);
    assert_eq!(exit_status(ActionExecutionOutcome::Exited(256 + 1)), 1);
    assert_eq!(exit_status(ActionExecutionOutcome::Signaled(9)), 137);
    assert_eq!(exit_status(ActionExecutionOutcome::Signaled(15)), 143);
}

#[tokio::test]
async fn enters_starts_waits_and_leaves_in_order() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(3))?;
    let limits_port = RecordingLimits::new(&log).with_oom_kills(2);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || {
                log.record("leave_action_group");
                Ok(())
            },
            cleanup_action_group: || {
                log.record("cleanup_group");
                Ok(())
            },
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(3));
    assert_eq!(outcome.usage.map(|usage| usage.oom_kills), Some(2));
    assert_eq!(
        log.calls(),
        [
            "limits.enter",
            "sandbox.start",
            "leave_action_group",
            "sandbox.wait",
            "cleanup_group",
            "limits.leave"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn without_limits_neither_enters_nor_leaves() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Signaled(9))?;
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute::<ScriptedSandbox, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || {
                log.record("leave_action_group");
                Ok(())
            },
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Signaled(9));
    assert_eq!(outcome.usage, None);
    assert_eq!(
        log.calls(),
        ["sandbox.start", "leave_action_group", "sandbox.wait"]
    );
    Ok(())
}

/// A failed post-start transition drops the action before leaving any entered scope.
#[tokio::test]
async fn leave_action_group_failure_drops_action_and_leaves_scope() -> Result<(), Box<dyn Error>> {
    for with_limits in [false, true] {
        for leave_fails in [false, true] {
            let log = CallLog::default();
            let sandbox = DropSandbox(log.clone(), "host:test".parse()?);
            let limits_port = if leave_fails {
                RecordingLimits::new(&log).failing(LimitsStep::Leave)
            } else {
                RecordingLimits::new(&log)
            };
            let argv = [OsString::from("cmd")];
            let directory = tempfile::tempdir()?;
            let action = action(&argv, directory.path(), directory.path())?;
            let result = execute(
                &sandbox,
                with_limits.then_some(&limits_port),
                InputLinks::Keep,
                &action,
                &mut no_terminations(),
                ExecutionHooks {
                    publish_cgroup: |_| {},
                    publish_pid: |_| {},
                    leave_action_group: || {
                        log.record("leave_action_group");
                        Err(io::Error::other("transition failed"))
                    },
                    cleanup_action_group: || Ok(()),
                },
            )
            .await;
            let failure = result.expect_err("group transition must fail");
            assert!(matches!(failure.error, ExecError::LeaveActionGroup(_)));
            assert_eq!(failure.usage.is_some(), with_limits && !leave_fails);
            if with_limits {
                assert_eq!(
                    log.calls(),
                    [
                        "limits.enter",
                        "sandbox.start",
                        "leave_action_group",
                        "sandbox.drop",
                        "limits.leave"
                    ]
                );
            } else {
                assert_eq!(
                    log.calls(),
                    ["sandbox.start", "leave_action_group", "sandbox.drop"]
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn pending_termination_prevents_start_with_and_without_limits() -> Result<(), Box<dyn Error>>
{
    for termination in [Termination::Terminate, Termination::Interrupt] {
        for with_limits in [false, true] {
            let log = CallLog::default();
            let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
            let limits_port = RecordingLimits::new(&log);
            let argv = [OsString::from("cmd")];
            let directory = tempfile::tempdir()?;
            let action = action(&argv, directory.path(), directory.path())?;
            let (sender, mut receiver) = termination_channel();
            sender.send(termination)?;

            let outcome = execute(
                &sandbox,
                with_limits.then_some(&limits_port),
                InputLinks::Keep,
                &action,
                &mut receiver,
                ExecutionHooks {
                    publish_cgroup: |_| {},
                    publish_pid: |_| {},
                    leave_action_group: || {
                        log.record("leave_action_group");
                        Ok(())
                    },
                    cleanup_action_group: || Ok(()),
                },
            )
            .await?;

            assert_eq!(
                outcome.outcome,
                ActionExecutionOutcome::Signaled(termination.signal())
            );
            assert_eq!(outcome.usage.is_some(), with_limits);
            if with_limits {
                assert_eq!(log.calls(), ["limits.enter", "limits.leave"]);
            } else {
                assert!(log.calls().is_empty());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn termination_during_startup_drops_start_and_leaves_scope() -> Result<(), Box<dyn Error>> {
    for with_limits in [false, true] {
        let log = CallLog::default();
        let started = Arc::new(Barrier::new(2));
        let (release, released) = oneshot::channel();
        let sandbox = PendingStartup {
            environment: "host:test".parse()?,
            log: log.clone(),
            started: Arc::clone(&started),
            release: Mutex::new(Some(released)),
        };
        let limits_port = RecordingLimits::new(&log).with_oom_kills(2);
        let argv = [OsString::from("cmd")];
        let directory = tempfile::tempdir()?;
        let action = action(&argv, directory.path(), directory.path())?;
        let (sender, mut terminations) = termination_channel();

        let (outcome, sent) = tokio::join!(
            execute(
                &sandbox,
                with_limits.then_some(&limits_port),
                InputLinks::Keep,
                &action,
                &mut terminations,
                ExecutionHooks {
                    publish_cgroup: |_| {},
                    publish_pid: |_| {},
                    leave_action_group: || {
                        log.record("leave_action_group");
                        Ok(())
                    },
                    cleanup_action_group: || Ok(()),
                },
            ),
            async {
                started.wait().await;
                sender.send(Termination::Terminate)?;
                let _ = release.send(());
                Ok::<_, mpsc::error::SendError<Termination>>(())
            }
        );
        sent?;
        let outcome = outcome?;
        assert_eq!(
            outcome.outcome,
            ActionExecutionOutcome::Signaled(Termination::Terminate.signal())
        );
        assert_eq!(
            outcome.usage.map(|usage| usage.oom_kills),
            with_limits.then_some(2)
        );
        if with_limits {
            assert_eq!(
                log.calls(),
                [
                    "limits.enter",
                    "sandbox.start",
                    "sandbox.drop",
                    "limits.leave"
                ]
            );
        } else {
            assert_eq!(log.calls(), ["sandbox.start", "sandbox.drop"]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn signal_error_during_startup_drops_start_and_preserves_usage() -> Result<(), Box<dyn Error>>
{
    for with_limits in [false, true] {
        let log = CallLog::default();
        let started = Arc::new(Barrier::new(2));
        let (release, released) = oneshot::channel();
        let sandbox = PendingStartup {
            environment: "host:test".parse()?,
            log: log.clone(),
            started: Arc::clone(&started),
            release: Mutex::new(Some(released)),
        };
        let limits_port = RecordingLimits::new(&log).with_oom_kills(2);
        let argv = [OsString::from("cmd")];
        let directory = tempfile::tempdir()?;
        let action = action(&argv, directory.path(), directory.path())?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut terminations = UnboundedReceiverStream::new(receiver);

        let (outcome, sent) = tokio::join!(
            execute(
                &sandbox,
                with_limits.then_some(&limits_port),
                InputLinks::Keep,
                &action,
                &mut terminations,
                ExecutionHooks {
                    publish_cgroup: |_| {},
                    publish_pid: |_| {},
                    leave_action_group: || {
                        log.record("leave_action_group");
                        Ok(())
                    },
                    cleanup_action_group: || Ok(()),
                },
            ),
            async {
                started.wait().await;
                sender.send(Err(io::Error::other("signal read failed")))?;
                let _ = release.send(());
                Ok::<_, mpsc::error::SendError<io::Result<Termination>>>(())
            }
        );
        sent?;
        let failure = outcome.expect_err("signal stream must fail");
        assert!(matches!(failure.error, ExecError::Signals(_)));
        assert_eq!(
            failure.usage.map(|usage| usage.oom_kills),
            with_limits.then_some(2)
        );
        if with_limits {
            assert_eq!(
                log.calls(),
                [
                    "limits.enter",
                    "sandbox.start",
                    "sandbox.drop",
                    "limits.leave"
                ]
            );
        } else {
            assert_eq!(log.calls(), ["sandbox.start", "sandbox.drop"]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn signal_stream_error_prevents_start_and_leaves_scope() -> Result<(), Box<dyn Error>> {
    for with_limits in [false, true] {
        let log = CallLog::default();
        let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
        let limits_port = RecordingLimits::new(&log);
        let argv = [OsString::from("cmd")];
        let directory = tempfile::tempdir()?;
        let action = action(&argv, directory.path(), directory.path())?;
        let mut terminations = futures::stream::iter([Err(io::Error::other("signal read failed"))]);

        let result = execute(
            &sandbox,
            with_limits.then_some(&limits_port),
            InputLinks::Keep,
            &action,
            &mut terminations,
            ExecutionHooks {
                publish_cgroup: |_| {},
                publish_pid: |_| {},
                leave_action_group: || {
                    log.record("leave_action_group");
                    Ok(())
                },
                cleanup_action_group: || Ok(()),
            },
        )
        .await;

        let failure = result.expect_err("signal stream must fail");
        assert!(matches!(failure.error, ExecError::Signals(_)));
        assert_eq!(failure.usage.is_some(), with_limits);
        if with_limits {
            assert_eq!(log.calls(), ["limits.enter", "limits.leave"]);
        } else {
            assert!(log.calls().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_action_in_its_own_group_keeps_gang_exec_in_the_action_group()
-> Result<(), Box<dyn Error>> {
    for with_limits in [false, true] {
        let log = CallLog::default();
        let (sender, mut terminations) = termination_channel();
        let relayed = Arc::new(Mutex::new(Vec::new()));
        let sandbox = Relaying {
            environment: "host:test".parse()?,
            sender,
            relayed: Arc::clone(&relayed),
        };
        let limits_port = RecordingLimits::new(&log);
        let argv = [OsString::from("cmd")];
        let directory = tempfile::tempdir()?;
        let action = action(&argv, directory.path(), directory.path())?;

        let outcome = execute(
            &sandbox,
            with_limits.then_some(&limits_port),
            InputLinks::Keep,
            &action,
            &mut terminations,
            ExecutionHooks {
                publish_cgroup: |_| {},
                publish_pid: |_| {},
                leave_action_group: || {
                    log.record("leave_action_group");
                    Ok(())
                },
                cleanup_action_group: || Ok(()),
            },
        )
        .await?;

        assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));
        assert_eq!(*relayed.lock(), vec![Termination::Interrupt]);
        if with_limits {
            assert_eq!(log.calls(), ["limits.enter", "limits.leave"]);
        } else {
            assert!(log.calls().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn leaves_the_scope_when_the_sandbox_fails_to_start() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;
    let limits_port = RecordingLimits::new(&log);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let result = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    let failure = result.expect_err("sandbox start must fail");
    assert!(matches!(failure.error, ExecError::Sandbox(_)));
    let usage = failure.usage.ok_or("missing scope usage")?;
    assert_eq!(usage.peak_memory.map(Bytes::get), Some(4096));
    assert_eq!(
        log.calls(),
        ["limits.enter", "sandbox.start", "limits.leave"]
    );
    Ok(())
}

#[tokio::test]
async fn a_failing_wait_while_limits_are_active_is_a_sandbox_error_and_the_scope_is_still_left()
-> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox =
        ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?.failing_to_wait();
    let limits_port = RecordingLimits::new(&log);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let result = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ExecFailure {
            error: ExecError::Sandbox(_),
            usage: Some(_)
        })
    ));
    assert_eq!(
        log.calls(),
        [
            "limits.enter",
            "sandbox.start",
            "sandbox.wait",
            "limits.leave"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn leave_failure_after_success_is_a_limits_error() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let limits_port = RecordingLimits::new(&log).failing(LimitsStep::Leave);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let result = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ExecFailure {
            error: ExecError::Limits(_),
            usage: None
        })
    ));
    assert_eq!(
        log.calls(),
        [
            "limits.enter",
            "sandbox.start",
            "sandbox.wait",
            "limits.leave"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn sandbox_error_wins_over_leave_error() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;
    let limits_port = RecordingLimits::new(&log).failing(LimitsStep::Leave);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let result = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ExecFailure {
            error: ExecError::Sandbox(_),
            usage: None
        })
    ));
    Ok(())
}

#[tokio::test]
async fn enter_failure_never_starts_the_sandbox() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let limits_port = RecordingLimits::new(&log).failing(LimitsStep::Enter);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let result = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ExecFailure {
            error: ExecError::Limits(_),
            usage: None
        })
    ));
    assert_eq!(log.calls(), ["limits.enter"]);
    Ok(())
}

#[tokio::test]
async fn break_leaves_one_link_per_file_before_the_sandbox_starts() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = LinkCounting::new(ScriptedSandbox::try_exiting(
        &log,
        ActionExecutionOutcome::Exited(0),
    )?);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first");
    std::fs::write(&first, b"shared")?;
    std::fs::hard_link(&first, directory.path().join("second"))?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute::<LinkCounting, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Break,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));
    assert_eq!(*sandbox.counts.lock(), vec![1, 1]);
    Ok(())
}

#[tokio::test]
async fn breaks_links_under_the_action_directory_outside_the_working_directory()
-> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let argv = [OsString::from("cmd")];
    let root = tempfile::tempdir()?;
    let (action_directory, working_directory) = linked_inputs(root.path())?;
    let action = action(&argv, &action_directory, &working_directory)?;

    let outcome = execute::<ScriptedSandbox, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Break,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));
    assert_eq!(link_count(&action_directory.join("inputs/shared"))?, 1);
    assert_eq!(link_count(&root.path().join("partner"))?, 1);
    Ok(())
}

#[tokio::test]
async fn keep_leaves_hard_links_in_place() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = LinkCounting::new(ScriptedSandbox::try_exiting(
        &log,
        ActionExecutionOutcome::Exited(0),
    )?);
    let argv = [OsString::from("cmd")];
    let root = tempfile::tempdir()?;
    let (action_directory, working_directory) = linked_inputs(root.path())?;
    let first = working_directory.join("first");
    std::fs::write(&first, b"shared")?;
    std::fs::hard_link(&first, working_directory.join("second"))?;
    let action = action(&argv, &action_directory, &working_directory)?;

    let outcome = execute::<LinkCounting, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;
    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));

    assert_eq!(*sandbox.counts.lock(), vec![2, 2]);
    assert_eq!(link_count(&action_directory.join("inputs/shared"))?, 2);
    assert_eq!(link_count(&root.path().join("partner"))?, 2);
    Ok(())
}

#[tokio::test]
async fn break_failure_never_starts_the_sandbox() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let missing = directory.path().join("missing");
    let action = action(&argv, &missing, &missing)?;

    let result = execute::<ScriptedSandbox, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Break,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ExecFailure {
            error: ExecError::LinkBreak(_),
            usage: None
        })
    ));
    assert!(log.calls().is_empty());
    Ok(())
}

/// A termination received while the action runs is relayed to the action, and the outcome is the
/// action's own.
#[tokio::test]
async fn a_termination_while_waiting_is_relayed_and_the_action_outcome_is_reported()
-> Result<(), Box<dyn Error>> {
    let (sender, mut terminations) = termination_channel();
    let relayed = Arc::new(Mutex::new(Vec::new()));
    let sandbox = Relaying {
        environment: "host:test".parse()?,
        sender,
        relayed: Arc::clone(&relayed),
    };
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute::<Relaying, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Keep,
        &action,
        &mut terminations,
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));
    assert_eq!(*relayed.lock(), vec![Termination::Interrupt]);
    Ok(())
}

/// The same relay with limits: the scope is still left after the action ends.
#[tokio::test]
async fn a_relayed_termination_still_leaves_the_limits_scope() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let (sender, mut terminations) = termination_channel();
    let relayed = Arc::new(Mutex::new(Vec::new()));
    let sandbox = Relaying {
        environment: "host:test".parse()?,
        sender,
        relayed: Arc::clone(&relayed),
    };
    let limits_port = RecordingLimits::new(&log);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut terminations,
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;

    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));
    assert_eq!(*relayed.lock(), vec![Termination::Interrupt]);
    assert_eq!(log.calls(), ["limits.enter", "limits.leave"]);
    Ok(())
}

#[tokio::test]
async fn the_action_starts_through_the_scope_launcher() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let limits_port = RecordingLimits::new(&log);
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;
    // `enter` hands out the scope's launcher; every scope of the double has the same one.
    let scope = limits_port.enter(&action.limits).await?;

    let outcome = execute(
        &sandbox,
        Some(&limits_port),
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;
    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));

    assert_eq!(sandbox.started()[0].launcher, scope.launcher().to_vec());
    Ok(())
}

#[tokio::test]
async fn without_limits_the_action_starts_without_a_launcher() -> Result<(), Box<dyn Error>> {
    let log = CallLog::default();
    let sandbox = ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
    let argv = [OsString::from("cmd")];
    let directory = tempfile::tempdir()?;
    let action = action(&argv, directory.path(), directory.path())?;

    let outcome = execute::<ScriptedSandbox, RecordingLimits, _, _, _, _>(
        &sandbox,
        None,
        InputLinks::Keep,
        &action,
        &mut no_terminations(),
        ExecutionHooks {
            publish_cgroup: |_| {},
            publish_pid: |_| {},
            leave_action_group: || Ok(()),
            cleanup_action_group: || Ok(()),
        },
    )
    .await?;
    assert_eq!(outcome.outcome, ActionExecutionOutcome::Exited(0));

    assert!(sandbox.started()[0].launcher.is_empty());
    Ok(())
}
