//! Child processes of the harness and the process table, which the harness reads to find and stop
//! the workers of a stack it started.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant};

use gang_clock::{Clock, SystemClock};
use gang_types::NonZeroBytes;
use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

use crate::errors::{E2eError, Result};

/// Maximum captured size of each command output stream.
const OUTPUT_LIMIT: NonZeroBytes = NonZeroBytes::mib(64);

/// How a finished command ended and what it printed, decoded lossily.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Ran {
    /// The exit code; `None` when a signal ended the command.
    pub code: Option<i32>,
    /// The standard output, decoded lossily.
    pub stdout: String,
    /// The standard error, decoded lossily.
    pub stderr: String,
}

impl Ran {
    /// Whether the command exited with status zero.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.code == Some(0)
    }

    /// Standard output followed by standard error, separated by a newline when needed.
    #[must_use]
    pub fn combined(&self) -> String {
        let mut output = self.stdout.clone();
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(&self.stderr);
        output
    }
}

/// Runs a command in its own process group with scrubbed environment and bounded output.
///
/// Feeds `stdin` when given. A non-zero exit is returned normally.
///
/// # Errors
///
/// Returns an error on process or stream failure, or a timeout after killing the process group.
pub fn run(command: Command, stdin: Option<&[u8]>, deadline: Duration) -> Result<Ran> {
    run_inner(command, stdin, deadline, None)
}

/// Like [`run`], but gives up as soon as `abort` yields a reason: the command's process group
/// is killed and the reason becomes [`E2eError::System`].
///
/// # Errors
///
/// Returns the errors of [`run`], or [`E2eError::System`] with the abort reason.
pub fn run_until(
    command: Command,
    stdin: Option<&[u8]>,
    deadline: Duration,
    abort: tokio::sync::oneshot::Receiver<String>,
) -> Result<Ran> {
    run_inner(command, stdin, deadline, Some(abort))
}

/// Runs a command with an optional abort reason.
fn run_inner(
    mut command: Command,
    stdin: Option<&[u8]>,
    deadline: Duration,
    abort: Option<tokio::sync::oneshot::Receiver<String>>,
) -> Result<Ran> {
    scrub_environment(&mut command);
    let program = command.get_program().to_string_lossy().into_owned();
    let mut command = tokio::process::Command::from(command);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(E2eError::io("building the command runtime"))?;
    let output = runtime.block_on(async {
        let clock = SystemClock::new();
        let completion = Clock::timeout(
            &clock,
            deadline,
            gang_os::run_bounded(&mut command, stdin, OUTPUT_LIMIT),
        );
        tokio::select! {
            biased;
            output = completion => output
                .map_err(|_| E2eError::timeout(format!("{program} to finish"), deadline))?
                .map_err(E2eError::system_from(format!("running {program}"))),
            reason = async {
                match abort {
                    Some(receiver) => match receiver.await {
                        Ok(reason) => reason,
                        Err(_) => std::future::pending().await,
                    },
                    None => std::future::pending().await,
                }
            } => Err(E2eError::system(reason)),
        }
    })?;
    Ok(Ran {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// A child process the harness started and must not leave behind.
///
/// Its stderr goes to a log file, its stdout is copied to the same log and read line by line, its
/// stdin is closed, and it is killed when dropped. Stdout is what the harness waits on: a line the
/// process prints is an event the harness can block on, and the end of its output is an event
/// too, since the output closes when the process exits.
#[derive(Debug)]
pub struct Supervised {
    pid: u32,
    /// The process, until it has been terminated.
    child: Option<Child>,
    announcements: Receiver<String>,
}

impl Supervised {
    /// Starts `command` with stdin closed, stderr appended to `log`, and stdout copied to `log` and
    /// read line by line. `log` is created when missing. The inherited `BUILD_`, `TEST_` and
    /// `RUNFILES` variables are removed first.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the log cannot be opened or the process cannot be started.
    pub fn spawn(command: &mut Command, log: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .map_err(E2eError::io(format!("opening log {}", log.display())))?;
        let stderr = file
            .try_clone()
            .map_err(E2eError::io(format!("duplicating log {}", log.display())))?;
        scrub_environment(command);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr));
        let program = command.get_program().to_string_lossy().into_owned();
        let mut child = command
            .spawn()
            .map_err(E2eError::io(format!("spawning {program}")))?;
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(E2eError::system(format!(
                "the output of {program} is not piped"
            )));
        };
        let (sender, announcements) = mpsc::channel();
        if let Err(error) = thread::Builder::new()
            .name("e2e-child-output".to_owned())
            .spawn(move || pump_stdout(stdout, file, &sender))
        {
            let _ = child.kill();
            let _ = child.wait();
            return Err(E2eError::io("starting the output reader")(error));
        }
        Ok(Self {
            pid: child.id(),
            child: Some(child),
            announcements,
        })
    }

    /// Blocks until the process prints a line that `ends`, and returns every line it printed up to
    /// and including that line, in order. Returns `None` once the process's output closes without
    /// such a line, which happens when it exits. Lines are also copied to the log.
    ///
    /// Kills the process if it does not announce within `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::Timeout`] on expiry, or an I/O error if the process cannot be killed.
    pub fn await_announcements(
        &mut self,
        timeout: Duration,
        mut ends: impl FnMut(&str) -> bool,
    ) -> Result<Option<Vec<String>>> {
        let start = Instant::now();
        let mut lines = Vec::new();
        loop {
            if start.elapsed() >= timeout {
                self.kill()?;
                return Err(E2eError::timeout(
                    "the gang server to announce its listeners",
                    timeout,
                ));
            }
            match self
                .announcements
                .recv_timeout(timeout.saturating_sub(start.elapsed()))
            {
                Ok(line) => {
                    let finished = ends(&line);
                    lines.push(line);
                    if finished {
                        return Ok(Some(lines));
                    }
                }
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
                Err(RecvTimeoutError::Timeout) => {
                    self.kill()?;
                    return Err(E2eError::timeout(
                        "the gang server to announce its listeners",
                        timeout,
                    ));
                }
            }
        }
    }

    /// The operating-system id of the process.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Whether the process has exited, without waiting for it.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the status cannot be read.
    pub fn has_exited(&mut self) -> Result<bool> {
        let Some(child) = self.child.as_mut() else {
            return Ok(true);
        };
        child
            .try_wait()
            .map(|status| status.is_some())
            .map_err(E2eError::io("polling a child process"))
    }

    /// Kills the process with SIGKILL and reaps it. A process that already exited is not an error.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the process cannot be killed or reaped.
    pub fn kill(&mut self) -> Result<()> {
        if self.has_exited()? {
            return Ok(());
        }
        let pid = self.pid();
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        child
            .kill()
            .map_err(E2eError::io(format!("killing process {pid}")))?;
        child
            .wait()
            .map(|_| ())
            .map_err(E2eError::io(format!("waiting for process {pid}")))
    }

    /// Asks the process to stop with SIGTERM and waits at most `timeout` for it to exit, reaping it.
    /// A process that already exited returns its status at once.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when it is still running after `timeout`; it is then killed and reaped.
    /// [`E2eError::Io`] when it cannot be signalled or waited for.
    pub fn terminate(&mut self, timeout: Duration) -> Result<ExitStatus> {
        let pid = self.pid;
        let already_terminated =
            || E2eError::system(format!("process {pid} was already terminated"));
        let child = self.child.as_mut().ok_or_else(already_terminated)?;
        if let Some(status) = child
            .try_wait()
            .map_err(E2eError::io("polling a child process"))?
        {
            self.child = None;
            return Ok(status);
        }
        gang_os::terminate_process(pid)
            .map_err(E2eError::io(format!("terminating process {pid}")))?;
        let result = wait_within(child, timeout, "the process to exit after SIGTERM");
        self.child = None;
        result
    }
}

impl Drop for Supervised {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

/// Waits without reaping in a helper thread, keeping the child owned here until it is safe to
/// either reap it or kill it. `waitid` with `NOWAIT` is available on both Linux and macOS.
fn wait_within(child: &mut Child, timeout: Duration, condition: &str) -> Result<ExitStatus> {
    let id = child.id();
    let Some(pid) = i32::try_from(id).ok().and_then(Pid::from_raw) else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(E2eError::system(format!("invalid child process id {id}")));
    };
    let (sender, receiver) = mpsc::channel();
    if let Err(error) = thread::Builder::new()
        .name("e2e-child-exit".to_owned())
        .spawn(move || {
            let result = loop {
                match waitid(
                    WaitId::Pid(pid),
                    WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
                ) {
                    Err(rustix::io::Errno::INTR) => {}
                    result => break result,
                }
            };
            // Darwin's siginfo is not Send; only the observation result crosses the channel.
            let _ = sender.send(result.map(|_| ()));
        })
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(E2eError::io("starting the exit waiter")(error));
    }
    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => child
            .wait()
            .map_err(E2eError::io(format!("waiting for process {id}"))),
        Ok(Err(error)) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(E2eError::io(format!("waiting for process {id}"))(
                error.into(),
            ))
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(E2eError::system(format!(
                "the exit waiter of process {id} stopped"
            )))
        }
        Err(RecvTimeoutError::Timeout) => {
            let killed = child.kill();
            let reaped = child.wait();
            killed.map_err(E2eError::io(format!("killing process {id}")))?;
            reaped.map_err(E2eError::io(format!("waiting for process {id}")))?;
            Err(E2eError::timeout(condition, timeout))
        }
    }
}

/// Copies `stdout` to `log` line by line and forwards each line to `lines`. Copying ends when the
/// stream closes, which happens when the process exits. Forwarding stops silently once the
/// receiver is gone, so that the log stays complete.
fn pump_stdout(stdout: ChildStdout, mut log: File, lines: &Sender<String>) {
    let mut reader = BufReader::new(stdout);
    let mut raw = Vec::new();
    loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        if log.write_all(&raw).is_err() {
            return;
        }
        let line = String::from_utf8_lossy(&raw).trim_end().to_owned();
        let _ = lines.send(line);
    }
}

/// One process in the process table, with its command-line arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessEntry {
    /// The process id.
    pub pid: u32,
    /// The command line, program first.
    pub args: Vec<String>,
}

/// The current process table, including the available command-line arguments.
#[must_use]
pub fn snapshot() -> Vec<ProcessEntry> {
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing()
            .without_tasks()
            .with_cmd(UpdateKind::Always),
    );
    system
        .processes()
        .iter()
        .map(|(pid, process)| ProcessEntry {
            pid: pid.as_u32(),
            args: process
                .cmd()
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect(),
        })
        .collect()
}

/// The entries that are workers of the stack whose configuration is `config`: the second argument
/// is `worker`, and some argument is the config path or `--config=<path>`.
#[must_use]
pub fn workers_of<'a>(entries: &'a [ProcessEntry], config: &Path) -> Vec<&'a ProcessEntry> {
    let config_path = config.to_string_lossy().into_owned();
    let config_flag = format!("--config={config_path}");
    entries
        .iter()
        .filter(|entry| {
            entry.args.get(1).is_some_and(|arg| arg == "worker")
                && entry
                    .args
                    .iter()
                    .any(|arg| *arg == config_path || *arg == config_flag)
        })
        .collect()
}

/// Sends SIGTERM to every worker of the stack whose configuration is `config`. A worker that has
/// already exited is not an error.
///
/// # Errors
///
/// [`E2eError::Io`] when a signal cannot be sent.
pub fn terminate_workers(config: &Path) -> Result<()> {
    let entries = snapshot();
    for worker in workers_of(&entries, config) {
        gang_os::terminate_process(worker.pid)
            .map_err(E2eError::io(format!("signalling worker {}", worker.pid)))?;
    }
    Ok(())
}

/// Removes from `command` every inherited variable that leaks from an outer `bazel run`: names
/// starting with `BUILD_` or `TEST_`, names containing `RUNFILES`, and the exact name `BAZELRC`,
/// the list of rc files Bazel reads from the environment.
pub fn scrub_environment(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if is_leaked(&name) {
            command.env_remove(name);
        }
    }
}

/// Whether an inherited variable named `name` is one `bazel run` leaks into the environment, or
/// the `BAZELRC` variable that lists the rc files Bazel reads.
fn is_leaked(name: &OsString) -> bool {
    let name = name.to_string_lossy();
    name.starts_with("BUILD_")
        || name.starts_with("TEST_")
        || name.contains("RUNFILES")
        || name == "BAZELRC"
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::ChildTest;
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;

    const MARKER: &str = "GANG_E2E_DEADLINE_HELPER";
    const STREAMS_CHILD: ChildTest =
        ChildTest::new("process::tests::writes_to_both_streams_when_asked");
    const CHILD_HELPER: ChildTest = ChildTest::new("process::tests::child_helper");

    /// Marks the child that writes to both standard streams.
    const STREAMS_CHILD_MARKER: &str = "GANG_E2E_STREAMS_CHILD";

    #[test]
    fn run_keeps_standard_output_apart_from_standard_error() {
        let mut command = STREAMS_CHILD
            .command()
            .expect("the test binary path is known");
        command.env(STREAMS_CHILD_MARKER, "1");
        let captured = run(command, None, crate::settings::DEFAULT_COMMAND_TIMEOUT)
            .expect("the test binary can be re-run");
        assert!(
            captured.succeeded(),
            "the child run failed:\n{}",
            captured.combined()
        );
        assert!(
            STREAMS_CHILD.completed_in(&captured.stdout),
            "{:?}",
            captured.stdout
        );
        assert!(captured.stdout.contains("out\n"), "{:?}", captured.stdout);
        assert!(
            !captured.stdout.lines().any(|line| line == "err"),
            "{:?}",
            captured.stdout
        );
        assert!(
            captured.combined().contains("out\n") && captured.combined().contains("err\n"),
            "{:?}",
            captured.combined()
        );
    }

    #[ignore = "child process helper of the process tests"]
    #[test]
    fn writes_to_both_streams_when_asked() {
        if std::env::var_os(STREAMS_CHILD_MARKER).is_none() {
            return;
        }
        println!("out");
        eprintln!("err");
        STREAMS_CHILD.complete();
    }

    #[test]
    fn combined_separates_unterminated_stdout_from_stderr() {
        let output = Ran {
            code: Some(1),
            stdout: "out".to_owned(),
            stderr: "err".to_owned(),
        };
        assert!(!output.succeeded());
        assert_eq!(output.combined(), "out\nerr");
    }

    fn helper(mode: &str) -> Command {
        let mut command = CHILD_HELPER
            .command()
            .expect("the test binary path is known");
        command.env(MARKER, mode);
        command
    }

    #[ignore = "child process helper of the process tests"]
    #[test]
    fn child_helper() {
        match std::env::var(MARKER).as_deref() {
            Ok("output") => {
                println!("captured output");
                CHILD_HELPER.complete();
            }
            Ok("stdin") => {
                println!("ready");
                let mut bytes = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut bytes)
                    .expect("reading stdin");
            }
            Ok("park") => {
                println!("ready");
                loop {
                    std::thread::park();
                }
            }
            _ => {}
        }
    }

    #[test]
    fn run_captures_exited_child() {
        let output = run(helper("output"), None, Duration::from_secs(30)).expect("child exits");
        assert!(output.succeeded());
        assert!(output.stdout.contains("captured output"));
        assert!(
            CHILD_HELPER.completed_in(&output.stdout),
            "{:?}",
            output.stdout
        );
    }

    #[test]
    fn run_kills_a_blocked_child() {
        let error = run(helper("park"), None, Duration::ZERO).expect_err("child exceeds deadline");
        assert!(matches!(error, E2eError::Timeout { .. }));
    }

    #[test]
    fn wait_within_kills_child_waiting_on_open_stdin() {
        let mut child = helper("stdin")
            .stdin(Stdio::piped())
            .spawn()
            .expect("child starts");
        let error = wait_within(&mut child, Duration::ZERO, "child").expect_err("child times out");
        assert!(matches!(error, E2eError::Timeout { .. }));
    }
    #[test]
    fn announcements_timeout_kills_the_child() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let mut child =
            Supervised::spawn(&mut helper("park"), &dir.path().join("log")).expect("child starts");
        let error = child
            .await_announcements(Duration::ZERO, |_| false)
            .expect_err("deadline expires");
        assert!(matches!(error, E2eError::Timeout { .. }));
        assert!(child.has_exited().expect("child status"));
    }

    #[test]
    fn terminate_returns_sigterm_status() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let mut child =
            Supervised::spawn(&mut helper("park"), &dir.path().join("log")).expect("child starts");
        child
            .await_announcements(Duration::from_secs(30), |line| line.ends_with("ready"))
            .expect("announcement")
            .expect("child remains alive");
        let status = child
            .terminate(Duration::from_secs(30))
            .expect("child terminates");
        assert_eq!(
            status.signal(),
            Some(rustix::process::Signal::TERM.as_raw())
        );
    }

    #[test]
    fn terminate_kills_child_with_sigterm_blocked() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let blocked = gang_os::block_terminations().expect("block sigterm");
        let child = Supervised::spawn(&mut helper("park"), &dir.path().join("log"));
        drop(blocked);
        let mut child = child.expect("child starts");
        child
            .await_announcements(Duration::from_secs(30), |line| line.ends_with("ready"))
            .expect("announcement")
            .expect("child remains alive");
        let error = child
            .terminate(Duration::ZERO)
            .expect_err("child ignores sigterm");
        assert!(matches!(error, E2eError::Timeout { .. }));
    }
}
