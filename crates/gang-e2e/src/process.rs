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

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

use crate::errors::{E2eError, Result};

/// Maximum captured size of each command output stream.
pub(crate) const OUTPUT_LIMIT: NonZeroBytes = NonZeroBytes::mib(64);

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

    /// Waits for the child without signalling it, killing it if the deadline expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the child cannot be waited for or does not exit by the deadline.
    pub fn wait_within(&mut self, timeout: Duration, condition: &str) -> Result<ExitStatus> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| E2eError::system("process was already terminated"))?;
        let result = match child
            .try_wait()
            .map_err(E2eError::io("reading child status"))?
        {
            Some(status) => Ok(status),
            None => wait_within(child, timeout, condition),
        };
        self.child = None;
        result
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

    /// Requests that a running process exit without waiting for it.
    pub(crate) fn request_exit(&mut self) -> Result<()> {
        let pid = self.pid;
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| E2eError::system(format!("process {pid} was already terminated")))?;
        if child
            .try_wait()
            .map_err(E2eError::io("polling a child process"))?
            .is_none()
        {
            gang_os::terminate_process(pid)
                .map_err(E2eError::io(format!("terminating process {pid}")))?;
        }
        Ok(())
    }

    /// Asks the process to stop with SIGTERM and waits at most `timeout` for it to exit, reaping it.
    /// A process that already exited returns its status at once.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when it is still running after `timeout`; it is then killed and reaped.
    /// [`E2eError::Io`] when it cannot be signalled or waited for.
    pub fn terminate(&mut self, timeout: Duration) -> Result<ExitStatus> {
        self.request_exit()?;
        self.wait_within(timeout, "the process to exit after SIGTERM")
    }
}

impl Drop for Supervised {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

/// A stable local host running a controller with systemd's `Restart=always` policy.
///
/// The monitor owns restarts, independently of observations. Explicit stop/kill/drop suppresses
/// restart before signalling. The original PID and its status are never replaced by a new child.
pub(crate) struct ControllerHost {
    state: Arc<Mutex<HostState>>,
    changed: Arc<Condvar>,
    monitor: Option<thread::JoinHandle<()>>,
}

struct HostState {
    child: Supervised,
    identity: Arc<str>,
    original_pid: u32,
    original_status: Option<ExitStatus>,
    enabled: bool,
    error: Option<String>,
}

impl ControllerHost {
    pub(crate) fn start(
        child: Supervised,
        identity: Arc<str>,
        mut command: Command,
        log: std::path::PathBuf,
        timeout: Duration,
        ready: fn(&mut Supervised, &Path, usize, Duration) -> Result<Arc<str>>,
    ) -> Result<Self> {
        let original_pid = child.pid();
        let state = Arc::new(Mutex::new(HostState {
            child,
            identity,
            original_pid,
            original_status: None,
            enabled: true,
            error: None,
        }));
        let changed = Arc::new(Condvar::new());
        let shared = Arc::clone(&state);
        let wake = Arc::clone(&changed);
        let monitor = thread::Builder::new()
            .name("e2e-controller-host".into())
            .spawn(move || {
                loop {
                    let pid = {
                        let state = shared.lock();
                        if !state.enabled {
                            break;
                        }
                        state.child.pid()
                    };
                    // Observe the OS exit event without reaping. Explicit stop disables restart
                    // before signalling/reaping; it joins this monitor only after the child stops.
                    let observed = (|| -> Result<()> {
                        let pid = i32::try_from(pid)
                            .ok()
                            .and_then(Pid::from_raw)
                            .ok_or_else(|| E2eError::system("invalid supervised controller PID"))?;
                        loop {
                            match waitid(
                                WaitId::Pid(pid),
                                WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
                            ) {
                                Err(rustix::io::Errno::INTR) => {}
                                result => {
                                    return result.map(|_| ()).map_err(|error| {
                                        E2eError::io("observing controller exit")(
                                            std::io::Error::from(error),
                                        )
                                    });
                                }
                            }
                        }
                    })();
                    let mut state = shared.lock();
                    if !state.enabled {
                        break;
                    }
                    let outcome = (|| -> Result<()> {
                        if let Err(error) = observed {
                            // A concurrent observation may have reaped through Child::try_wait;
                            // std retains that exact status. It must not suppress host restart.
                            if !state.child.has_exited()? {
                                return Err(error);
                            }
                        }
                        let status = state
                            .child
                            .wait_within(Duration::ZERO, "exited controller")?;
                        if state.original_status.is_none() {
                            state.original_status = Some(status);
                        }
                        wake.notify_all();
                        let offset = std::fs::metadata(&log)
                            .map_err(E2eError::io("measuring restarted controller log"))?
                            .len();
                        let offset = usize::try_from(offset)
                            .map_err(E2eError::system_from("measuring restarted controller log"))?;
                        state.child = Supervised::spawn(&mut command, &log)?;
                        match ready(&mut state.child, &log, offset, timeout) {
                            Ok(identity) => {
                                state.identity = identity;
                                state.error = None;
                            }
                            Err(error) => {
                                if !state.child.has_exited()? {
                                    return Err(error);
                                }
                                // A rejected/repeatedly failing child still has Restart=always.
                                // Keep its startup error visible until a replacement prepares.
                                state.error = Some(gang_types::error_chain(&error).to_string());
                            }
                        }
                        wake.notify_all();
                        Ok(())
                    })();
                    if let Err(error) = outcome {
                        state.error = Some(gang_types::error_chain(&error).to_string());
                        state.enabled = false;
                        wake.notify_all();
                        break;
                    }
                }
            })
            .map_err(E2eError::io("starting controller host supervisor"))?;
        Ok(Self {
            state,
            changed,
            monitor: Some(monitor),
        })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.state.lock().child.pid()
    }

    pub(crate) fn identity(&self) -> Arc<str> {
        Arc::clone(&self.state.lock().identity)
    }

    pub(crate) fn wait_restarted(&self, timeout: Duration) -> Result<(u32, Arc<str>)> {
        let start = Instant::now();
        let mut state = self.state.lock();
        loop {
            if let Some(error) = &state.error {
                return Err(E2eError::system(format!(
                    "controller host supervisor: {error}"
                )));
            }
            if state.child.pid() != state.original_pid && !state.child.has_exited()? {
                return Ok((state.child.pid(), Arc::clone(&state.identity)));
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    "host to restart its original controller",
                    timeout,
                ));
            }
            self.changed.wait_for(&mut state, remaining);
        }
    }

    pub(crate) fn has_exited(&mut self) -> Result<bool> {
        let mut state = self.state.lock();
        if let Some(error) = &state.error {
            return Err(E2eError::system(format!(
                "controller host supervisor: {error}"
            )));
        }
        state.child.has_exited()
    }

    /// Suppresses restart before explicit stop or a fault aimed at the original process.
    pub(crate) fn suppress_restart(&mut self) {
        self.state.lock().enabled = false;
        self.changed.notify_all();
    }

    fn join_monitor(&mut self) -> Result<()> {
        if let Some(monitor) = self.monitor.take() {
            monitor
                .join()
                .map_err(|_| E2eError::system("controller host supervisor panicked"))?;
        }
        Ok(())
    }

    pub(crate) fn request_exit(&mut self) -> Result<()> {
        self.suppress_restart();
        let mut state = self.state.lock();
        if state.child.has_exited()? {
            return Ok(());
        }
        state.child.request_exit()
    }

    pub(crate) fn kill(&mut self) -> Result<()> {
        self.suppress_restart();
        let mut state = self.state.lock();
        state.child.kill()?;
        let status = state
            .child
            .wait_within(Duration::ZERO, "killed controller")?;
        state.original_status.get_or_insert(status);
        drop(state);
        self.join_monitor()
    }

    pub(crate) fn terminate(&mut self, timeout: Duration) -> Result<ExitStatus> {
        self.request_exit()?;
        self.wait_stopped(timeout)
    }

    pub(crate) fn wait_stopped(&mut self, timeout: Duration) -> Result<ExitStatus> {
        self.suppress_restart();
        let mut state = self.state.lock();
        if state.child.child.is_some() {
            let status = state
                .child
                .wait_within(timeout, "stopped host controller")?;
            state.original_status.get_or_insert(status);
        }
        let status = state
            .original_status
            .ok_or_else(|| E2eError::system("host has no original exit status"))?;
        drop(state);
        self.join_monitor()?;
        Ok(status)
    }

    /// Waits for the original child, not the replacement that the monitor may already have started.
    pub(crate) fn wait_within(&mut self, timeout: Duration, condition: &str) -> Result<ExitStatus> {
        let start = Instant::now();
        let mut state = self.state.lock();
        loop {
            if let Some(status) = state.original_status {
                return Ok(status);
            }
            if !state.enabled {
                let status = state.child.wait_within(timeout, condition)?;
                state.original_status = Some(status);
                return Ok(status);
            }
            if let Some(error) = &state.error {
                return Err(E2eError::system(error.clone()));
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!("{condition} (original pid {})", state.original_pid),
                    timeout,
                ));
            }
            self.changed.wait_for(&mut state, remaining);
        }
    }
}

impl Drop for ControllerHost {
    fn drop(&mut self) {
        self.suppress_restart();
        let _ = self.state.lock().child.kill();
        let _ = self.join_monitor();
    }
}

/// Waits without reaping in a helper thread, keeping the child owned here until it is safe to
/// either reap it or kill it. `waitid` with `NOWAIT` is available on both Linux and macOS.
pub(crate) fn wait_within(
    child: &mut Child,
    timeout: Duration,
    condition: &str,
) -> Result<ExitStatus> {
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

/// Returns the exact command-line arguments of `pid`, or `None` if it no longer exists.
#[must_use]
pub fn arguments(pid: u32) -> Option<Vec<OsString>> {
    let mut system = System::new();
    let pid = sysinfo::Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .without_tasks()
            .with_cmd(UpdateKind::Always),
    );
    system.process(pid).map(|process| process.cmd().to_vec())
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

/// Workers and held launch wrappers for the stack whose configuration is `config`.
#[must_use]
pub fn workers_of<'a>(entries: &'a [ProcessEntry], config: &Path) -> Vec<&'a ProcessEntry> {
    let config_path = config.to_string_lossy().into_owned();
    let config_flag = format!("--config={config_path}");
    entries
        .iter()
        .filter(|entry| {
            let args = if entry
                .args
                .get(1)
                .is_some_and(|arg| arg == "--gang-e2e-launch-gate")
            {
                entry.args.get(4..).unwrap_or_default()
            } else {
                &entry.args
            };
            args.get(1).is_some_and(|arg| arg == "worker")
                && args
                    .iter()
                    .any(|arg| *arg == config_path || *arg == config_flag)
        })
        .collect()
}

/// Waits for a process to exit without requiring it to be our child.
pub(crate) fn await_gone(pid: u32, timeout: Duration) -> Result<bool> {
    match ExitHandle::open(pid)? {
        Some(handle) => handle.wait(timeout),
        None => Ok(true),
    }
}

#[cfg(target_os = "linux")]
type ExitFd = std::os::fd::OwnedFd;
#[cfg(target_os = "macos")]
type ExitFd = gang_os::ProcessExitQueue;

struct ExitHandle {
    fd: ExitFd,
    pid: Pid,
}

impl ExitHandle {
    fn open(pid: u32) -> Result<Option<Self>> {
        let raw = i32::try_from(pid)
            .map_err(|_| E2eError::system(format!("invalid process id {pid}")))?;
        let target = Pid::from_raw(raw)
            .ok_or_else(|| E2eError::system(format!("invalid process id {pid}")))?;
        match open_exit_handle(target) {
            Ok(fd) => Ok(Some(Self { fd, pid: target })),
            Err(rustix::io::Errno::SRCH) => Ok(None),
            Err(error) => Err(E2eError::system(format!(
                "observing process {pid}: {error}"
            ))),
        }
    }

    fn signal(&self, signal: rustix::process::Signal) -> Result<()> {
        #[cfg(target_os = "linux")]
        let result = rustix::process::pidfd_send_signal(&self.fd, signal);
        #[cfg(target_os = "macos")]
        let result = rustix::process::kill_process(self.pid, signal);
        match result {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(error) => Err(E2eError::system(format!(
                "signalling process {}: {error}",
                self.pid
            ))),
        }
    }

    fn wait(&self, timeout: Duration) -> Result<bool> {
        wait_exit_handle(&self.fd, timeout)
            .map_err(|error| E2eError::system(format!("waiting for process {}: {error}", self.pid)))
    }
}

#[cfg(target_os = "linux")]
fn open_exit_handle(pid: Pid) -> rustix::io::Result<std::os::fd::OwnedFd> {
    rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())
}

#[cfg(target_os = "linux")]
fn wait_exit_handle(fd: &std::os::fd::OwnedFd, timeout: Duration) -> rustix::io::Result<bool> {
    use rustix::event::{PollFd, PollFlags, Timespec, poll};

    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(rustix::io::Errno::INVAL)?;
    let mut fds = [PollFd::new(fd, PollFlags::IN)];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = Timespec {
            tv_sec: i64::try_from(remaining.as_secs()).map_err(|_| rustix::io::Errno::INVAL)?,
            tv_nsec: remaining.subsec_nanos().into(),
        };
        match poll(&mut fds, Some(&timeout)) {
            Err(rustix::io::Errno::INTR) => {}
            result => return result.map(|ready| ready != 0),
        }
    }
}

#[cfg(target_os = "macos")]
fn open_exit_handle(pid: Pid) -> rustix::io::Result<ExitFd> {
    gang_os::ProcessExitQueue::new(pid)
}

#[cfg(target_os = "macos")]
fn wait_exit_handle(fd: &ExitFd, timeout: Duration) -> rustix::io::Result<bool> {
    fd.wait(timeout)
}

/// Terminates the stack's workers and waits for their exit, killing unresponsive workers.
///
/// # Errors
///
/// Returns an error if signalling or observing a worker fails, or a killed worker does not exit.
pub fn terminate_workers(config: &Path) -> Result<()> {
    use rustix::process::Signal;

    let entries = snapshot();
    let workers = workers_of(&entries, config);
    let mut targets = Vec::with_capacity(workers.len());
    for worker in workers {
        let Some(handle) = ExitHandle::open(worker.pid)? else {
            continue;
        };
        handle.signal(Signal::TERM)?;
        targets.push((worker.pid, handle));
    }
    let timeout = crate::settings::DEFAULT_COMMAND_TIMEOUT;
    for (pid, handle) in targets {
        if !handle.wait(timeout)? {
            handle.signal(Signal::KILL)?;
            if !handle.wait(timeout)? {
                return Err(E2eError::timeout(format!("worker {pid} to exit"), timeout));
            }
        }
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
    fn terminating_workers_waits_for_their_sigterm_handler() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let config = dir.path().join("config.toml");
        std::fs::write(
            dir.path().join("worker"),
            "trap 'exit 0' TERM\nprintf 'ready\\n'\nread -r value\n",
        )?;
        // The worker argv contract requires a real shell process with `worker` as argv[1].
        let bash = std::fs::canonicalize(
            std::env::var_os("GANG_E2E_TEST_BASH").ok_or("Bazel supplies pinned Bash")?,
        )?;
        let mut child = Command::new(bash)
            .current_dir(dir.path())
            .arg("worker")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().ok_or("worker stdout missing")?;
        let mut reader = BufReader::new(stdout);
        let mut ready = String::new();
        reader.read_line(&mut ready)?;
        assert_eq!(ready, "ready\n");
        terminate_workers(&config)?;
        assert!(
            child.try_wait()?.is_some(),
            "worker still runs after termination"
        );
        Ok(())
    }

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
            Ok("host") => {
                let path = std::env::var_os("GANG_E2E_HOST_SOCKET").expect("host control socket");
                let mut control =
                    std::os::unix::net::UnixStream::connect(path).expect("connecting host control");
                println!("ready");
                let mut exit = [0];
                control
                    .read_exact(&mut exit)
                    .expect("host exit instruction");
                if exit[0] != 0 {
                    std::process::exit(i32::from(exit[0]));
                }
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

    fn host_ready(
        child: &mut Supervised,
        _log: &Path,
        _offset: usize,
        timeout: Duration,
    ) -> Result<Arc<str>> {
        child
            .await_announcements(timeout, |line| line.ends_with("ready"))?
            .ok_or_else(|| E2eError::system("host child exited before ready"))?;
        Ok(Arc::from(child.pid().to_string()))
    }

    #[test]
    fn host_restarts_without_observation_and_keeps_original_exit_evidence()
    -> gang_testkit::TestResult {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        for code in [0, 17] {
            let root = tempfile::tempdir()?;
            // Unix socket paths are short; Bazel's test temporary directory may exceed SUN_LEN.
            let sockets = tempfile::Builder::new()
                .prefix("gang-sock")
                .tempdir_in("/tmp")?;
            let path = sockets.path().join("control.sock");
            let listener = runtime.block_on(async { tokio::net::UnixListener::bind(&path) })?;
            let log = root.path().join("log");
            let mut command = helper("host");
            command.env("GANG_E2E_HOST_SOCKET", &path);
            let mut child = Supervised::spawn(&mut command, &log)?;
            let identity = host_ready(&mut child, &log, 0, Duration::from_secs(30))?;
            let original_pid = child.pid();
            let mut host = ControllerHost::start(
                child,
                Arc::clone(&identity),
                command,
                log,
                Duration::from_secs(30),
                host_ready,
            )?;
            let accept = || {
                runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(30), listener.accept()).await
                })
            };
            let (mut first, _) = accept()??;
            runtime.block_on(async {
                tokio::io::AsyncWriteExt::write_all(&mut first, &[code]).await
            })?;
            // No host observation has run since the exit instruction: this accept can only be
            // satisfied by the independent monitor spawning the replacement.
            let (_replacement, _) = accept()??;
            let (pid, holder) = host.wait_restarted(Duration::from_secs(30))?;
            assert_ne!(pid, original_pid);
            assert_ne!(holder, identity);
            let original = host.wait_within(Duration::from_secs(30), "original exit")?;
            assert_eq!(original.code(), Some(i32::from(code)));
            host.kill()?;
            assert!(!host.state.lock().enabled);
            assert!(host.monitor.is_none());
            assert_eq!(
                host.wait_within(Duration::ZERO, "original evidence")?,
                original
            );
        }
        Ok(())
    }

    #[test]
    fn explicit_host_stop_suppresses_restart_and_preserves_kill_status() -> gang_testkit::TestResult
    {
        let root = tempfile::tempdir()?;
        let log = root.path().join("log");
        let mut command = helper("park");
        let mut child = Supervised::spawn(&mut command, &log)?;
        let identity = host_ready(&mut child, &log, 0, Duration::from_secs(30))?;
        let mut host = ControllerHost::start(
            child,
            identity,
            command,
            log,
            Duration::from_secs(30),
            host_ready,
        )?;
        let pid = host.pid();
        host.kill()?;
        assert_eq!(host.pid(), pid);
        assert!(host.monitor.is_none());
        assert_eq!(
            host.wait_within(Duration::ZERO, "killed original")?
                .signal(),
            Some(rustix::process::Signal::KILL.as_raw()),
        );
        Ok(())
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
    fn supervised_wait_reaps_without_signalling() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let mut child = Supervised::spawn(&mut helper("output"), &dir.path().join("log"))?;
        let status = child.wait_within(Duration::from_secs(30), "child to exit")?;
        assert!(status.success());
        Ok(())
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
