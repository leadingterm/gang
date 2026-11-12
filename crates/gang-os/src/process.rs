//! Process-group signalling, bounded child runs, and logged spawning.

use std::fs::OpenOptions;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process, kill_process_group};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::fs::PRIVATE_FILE;
use crate::{PathIo, RunError, SpawnError};

/// A child process group that is killed when the guard drops, unless disarmed.
///
/// The group id equals its leader's pid, so the child must have been spawned with its own group
/// via `process_group(0)`. Disarm once the leader is reaped, because the id may be reused.
#[derive(Debug)]
#[must_use]
pub struct ProcessGroup {
    leader: Pid,
}

impl ProcessGroup {
    /// Returns the group led by a running `child`, or `None` once the child has been reaped.
    pub fn of(child: &tokio::process::Child) -> Option<Self> {
        child.id().and_then(Self::from_pid)
    }

    /// Returns the group led by the process `pid`, or `None` if `pid` cannot name a process.
    #[must_use]
    pub fn from_pid(pid: u32) -> Option<Self> {
        pid_of(pid).map(|leader| Self { leader })
    }

    /// Sends `SIGTERM` to every process in the group.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal cannot be delivered. A group that no longer exists is not
    /// an error.
    pub fn terminate(&self) -> io::Result<()> {
        tolerate_gone(kill_process_group(self.leader, Signal::TERM))
    }

    /// Sends `SIGKILL` to every process in the group.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal cannot be delivered. A group that no longer exists is not
    /// an error.
    pub fn kill(&self) -> io::Result<()> {
        tolerate_gone(kill_process_group(self.leader, Signal::KILL))
    }

    /// Consumes the guard without signalling the group when it would otherwise drop.
    pub fn disarm(self) {
        std::mem::forget(self);
    }
}

/// The exit of this process's parent, as the kernel reports it.
///
/// On Linux it is observed through a pidfd, which becomes readable only once the parent has
/// exited: it is a zombie or already reaped. The parent closing its descriptors, or anything it
/// does before it exits, never completes the wait.
#[derive(Debug)]
#[must_use]
pub struct ParentExit {
    /// The pidfd of the parent; `None` when the parent had already exited at [`Self::watch`].
    pidfd: Option<OwnedFd>,
}

impl ParentExit {
    /// Watches `parent`, the process that started this one.
    ///
    /// The pidfd is opened before the parent is checked: a process that is still this process's
    /// parent once the pidfd is open is the process the pidfd names, never a later process that
    /// reused its PID. When `parent` no longer exists, or is no longer this process's parent
    /// because it exited and this process was reparented, its exit has already happened.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] when `parent` cannot name a process, another
    /// [`io::Error`] when the pidfd cannot be opened, and [`io::ErrorKind::Unsupported`] on
    /// platforms without pidfds.
    #[cfg(target_os = "linux")]
    pub fn watch(parent: u32) -> io::Result<Self> {
        let pid = pid_of(parent)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not a process id"))?;
        let pidfd = match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) {
            Ok(pidfd) => pidfd,
            Err(Errno::SRCH) => return Ok(Self { pidfd: None }),
            Err(errno) => return Err(errno.into()),
        };
        Ok(Self {
            pidfd: (rustix::process::getppid() == Some(pid)).then_some(pidfd),
        })
    }

    /// Watches `parent`; pidfds exist only on Linux.
    ///
    /// # Errors
    ///
    /// Always [`io::ErrorKind::Unsupported`].
    #[cfg(not(target_os = "linux"))]
    pub fn watch(_parent: u32) -> io::Result<Self> {
        Err(io::ErrorKind::Unsupported.into())
    }

    /// Completes once the parent has exited. Must be awaited inside a tokio runtime; the wait
    /// occupies one blocking thread.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the pidfd cannot be polled or the blocking wait does not complete.
    pub async fn exited(self) -> io::Result<()> {
        let Some(pidfd) = self.pidfd else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || wait_readable(&pidfd))
            .await
            .map_err(io::Error::other)?
    }
}

/// Blocks until `fd` is readable; an interrupted wait is resumed.
fn wait_readable(fd: &OwnedFd) -> io::Result<()> {
    let mut fds = [rustix::event::PollFd::new(fd, rustix::event::PollFlags::IN)];
    loop {
        match rustix::event::poll(&mut fds, None) {
            Ok(_) => return Ok(()),
            Err(Errno::INTR) => {}
            Err(errno) => return Err(errno.into()),
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = kill_process_group(self.leader, Signal::KILL);
    }
}

/// Returns the process group ID of the calling process's parent.
///
/// # Errors
///
/// Returns an error if the process has no parent or `getpgid(2)` fails.
pub fn parent_process_group() -> io::Result<i32> {
    let parent =
        rustix::process::getppid().ok_or_else(|| io::Error::other("the process has no parent"))?;
    Ok(rustix::process::getpgid(Some(parent))?
        .as_raw_nonzero()
        .get())
}

/// Moves the calling process into its parent's process group.
///
/// `gang exec` calls it once the action's first process exists: `NativeLink` signals the action's
/// process groups but never the worker's own, so `gang exec` no longer receives the action's `SIGTERM`.
///
/// # Errors
///
/// Returns an error if the process has no parent or if `getpgid(2)` or `setpgid(2)` fails.
pub fn join_parent_group() -> io::Result<()> {
    let group = Pid::from_raw(parent_process_group()?)
        .ok_or_else(|| io::Error::other("invalid parent process group"))?;
    rustix::process::setpgid(None, Some(group))?;
    Ok(())
}

/// Sends `SIGTERM` to the single process `pid`, not to its group.
///
/// A pid that cannot name a process (`0` or above `i32::MAX`) is a no-op. It must never reach
/// `kill(2)`, where `0` would address the caller's own group.
///
/// # Errors
///
/// Returns an error if the signal cannot be delivered. A process that no longer exists is not an
/// error.
pub fn terminate_process(pid: u32) -> io::Result<()> {
    match pid_of(pid) {
        Some(pid) => tolerate_gone(kill_process(pid, Signal::TERM)),
        None => Ok(()),
    }
}

/// What a child run to completion by [`run_bounded`] produced.
#[derive(Debug)]
pub struct RunOutput {
    /// The child's exit status, including non-zero statuses.
    pub status: std::process::ExitStatus,
    /// The complete standard output.
    pub stdout: Vec<u8>,
    /// The first `limit` bytes of standard error.
    pub stderr: Vec<u8>,
}

/// Runs a child in its own process group with bounded captured output.
///
/// The runner replaces the command's standard streams. It writes `stdin`, if supplied, then
/// closes the pipe; otherwise standard input is null. A broken pipe during input is accepted.
/// Standard output exceeding `limit` fails the run. Standard error is truncated to `limit`
/// while the remainder is drained. Input, both output streams and waiting run concurrently.
/// A non-zero exit status is returned normally.
///
/// Dropping this future kills the child's entire process group. To impose a timeout, compose
/// it at the call site as `clock.timeout(duration, run_bounded(..)).await` using
/// `gang_clock::Clock::timeout`.
///
/// # Errors
///
/// Returns [`RunError`] if spawning, I/O or waiting fails, or standard output exceeds the limit.
pub async fn run_bounded(
    command: &mut tokio::process::Command,
    stdin: Option<&[u8]>,
    limit: gang_types::NonZeroBytes,
) -> Result<RunOutput, RunError> {
    let program = PathBuf::from(command.as_std().get_program());
    command
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    let mut child = command.spawn().map_err(|source| RunError::Spawn {
        program: program.clone(),
        source,
    })?;
    let group = ProcessGroup::of(&child);
    let input = child.stdin.take();
    let io_error = |source| RunError::Io {
        program: program.clone(),
        source,
    };
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        return Err(io_error(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "the child's output is not piped",
        )));
    };
    let write_input = async {
        if let (Some(mut pipe), Some(bytes)) = (input, stdin)
            && let Err(source) = pipe.write_all(bytes).await
            && source.kind() != io::ErrorKind::BrokenPipe
        {
            return Err(io_error(source));
        }
        Ok(())
    };
    let read_stdout = async {
        let mut output = Vec::new();
        stdout
            .take(limit.as_u64().saturating_add(1))
            .read_to_end(&mut output)
            .await
            .map_err(io_error)?;
        if u64::try_from(output.len()).unwrap_or(u64::MAX) > limit.as_u64() {
            return Err(RunError::StdoutTooLarge {
                program: program.clone(),
                limit,
            });
        }
        Ok(output)
    };
    let read_stderr = async {
        let mut output = Vec::new();
        let mut bounded = stderr.take(limit.as_u64());
        bounded.read_to_end(&mut output).await.map_err(io_error)?;
        tokio::io::copy(&mut bounded.into_inner(), &mut tokio::io::sink())
            .await
            .map_err(io_error)?;
        Ok(output)
    };
    let wait = async { child.wait().await.map_err(io_error) };
    let ((), stdout, stderr, status) =
        tokio::try_join!(write_input, read_stdout, read_stderr, wait)?;
    if let Some(group) = group {
        group.disarm();
    }
    Ok(RunOutput {
        status,
        stdout,
        stderr,
    })
}

/// Starts `command` in its own process group, with its output appended to `log`.
///
/// The log is appended to, never truncated. It is created with mode 0600 only when missing.
/// Callers that need a fresh log truncate it first, for example with
/// `fs::write_atomic(log, b"", PRIVATE_FILE)`. Stdout and stderr share one open file description.
/// `stdin` is whatever the caller passes: `Stdio::null()`, or a [`crate::FileLock`] converted with
/// `Stdio::from`, which keeps the lock held as long as the child lives.
///
/// # Errors
///
/// Returns [`SpawnError::Log`] if the log cannot be opened or duplicated, and
/// [`SpawnError::Spawn`] if the program cannot be started.
pub fn spawn_logged(command: &mut Command, stdin: Stdio, log: &Path) -> Result<Child, SpawnError> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(PRIVATE_FILE)
        .open(log)
        .map_err(PathIo::at("open log file", log))?;
    let stderr = file.try_clone().map_err(PathIo::at("open log file", log))?;
    // The log is a file: a forced colour request of the caller must not put escape codes in it.
    // Applied after any env the caller configured, so it also wins over them.
    // Own process group: the child survives the caller's terminal and receives only the caller's
    // signals.
    command
        .env_remove("CLICOLOR_FORCE")
        .stdin(stdin)
        .stdout(file)
        .stderr(stderr)
        .process_group(0)
        .spawn()
        .map_err(|source| SpawnError::Spawn {
            program: PathBuf::from(command.get_program()),
            source,
        })
}

fn pid_of(pid: u32) -> Option<Pid> {
    i32::try_from(pid).ok().and_then(Pid::from_raw)
}

fn tolerate_gone(result: Result<(), Errno>) -> io::Result<()> {
    match result {
        Err(errno) if errno == Errno::SRCH => Ok(()),
        other => other.map_err(io::Error::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::ChildTest;
    use std::error::Error;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::os::unix::process::ExitStatusExt;

    #[cfg(target_os = "linux")]
    const EXITING_PARENT: ChildTest = ChildTest::new("process::tests::exiting_parent");
    #[cfg(target_os = "linux")]
    const WATCHING_CHILD: ChildTest = ChildTest::new("process::tests::watching_child");
    const BOUNDED_CHILD: ChildTest = ChildTest::new("process::tests::bounded_child");
    const CHILD_BLOCKS_ON_STDIN: ChildTest =
        ChildTest::new("process::tests::child_blocks_on_stdin");
    const CHILD_JOINS_PARENT_GROUP: ChildTest =
        ChildTest::new("process::tests::child_joins_parent_group");
    const CHILD_REPORTS: ChildTest = ChildTest::new("process::tests::child_reports");
    const CHILD_LEADS_A_GROUP_OF_TWO: ChildTest =
        ChildTest::new("process::tests::child_leads_a_group_of_two");

    #[cfg(target_os = "linux")]
    const PARENT_EXIT_HELPER: &str = "PARENT_EXIT_HELPER";
    #[cfg(target_os = "linux")]
    const PARENT_EXIT_PARENT: &str = "PARENT_EXIT_PARENT";

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_process_that_is_not_the_parent_has_already_exited() -> Result<(), Box<dyn Error>> {
        let exit = ParentExit::watch(std::process::id())?;
        assert!(matches!(
            gang_testkit::ready_now(exit.exited()),
            Some(Ok(()))
        ));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_parent_exit_is_reported_once_the_kernel_has_ended_it() -> Result<(), Box<dyn Error>>
    {
        use std::time::Duration;
        use tokio::io::AsyncBufReadExt;

        let mut child = {
            let _spawn = gang_testkit::fork_gate::spawner();
            tokio::process::Command::from(EXITING_PARENT.command()?)
                .env(PARENT_EXIT_HELPER, "parent")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?
        };
        let stdout = child.stdout.take().ok_or("missing child stdout")?;
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await? {
                if line == "watching" {
                    return Ok::<(), io::Error>(());
                }
            }
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing watching",
            ))
        })
        .await??;
        child.start_kill()?;
        let line = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await? {
                if line.starts_with("parent-exited ") {
                    return Ok::<String, io::Error>(line);
                }
            }
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing parent exit",
            ))
        })
        .await??;
        assert_eq!(line, "parent-exited Z");
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await? {
                if WATCHING_CHILD.completed_in(&line) {
                    return Ok::<(), io::Error>(());
                }
            }
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing watching child completion",
            ))
        })
        .await??;
        child.wait().await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[ignore = "child process helper of the parent exit test"]
    #[test]
    fn exiting_parent() -> Result<(), Box<dyn Error>> {
        if std::env::var(PARENT_EXIT_HELPER).ok().as_deref() != Some("parent") {
            return Ok(());
        }
        let _child = WATCHING_CHILD
            .command()?
            .env(PARENT_EXIT_HELPER, "child")
            .env(PARENT_EXIT_PARENT, std::process::id().to_string())
            .stdout(Stdio::inherit())
            .stdin(Stdio::null())
            .spawn()?;
        io::stdin().read_exact(&mut [0; 1])?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[ignore = "child process helper of the parent exit test"]
    #[test]
    fn watching_child() -> Result<(), Box<dyn Error>> {
        if std::env::var(PARENT_EXIT_HELPER).ok().as_deref() != Some("child") {
            return Ok(());
        }
        let parent: u32 = std::env::var(PARENT_EXIT_PARENT)?.parse()?;
        let exit = ParentExit::watch(parent)?;
        println!();
        println!("watching");
        io::stdout().flush()?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(exit.exited())?;
        match std::fs::read_to_string(format!("/proc/{parent}/stat")) {
            Ok(stat) => {
                let state = stat
                    .rsplit_once(')')
                    .and_then(|(_, rest)| rest.chars().find(|c| !c.is_whitespace()))
                    .ok_or("missing process state")?;
                println!("parent-exited {state}");
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => println!("parent-exited gone"),
            Err(err) => return Err(err.into()),
        }
        io::stdout().flush()?;
        WATCHING_CHILD.complete();
        Ok(())
    }

    const CHILD_ENV: &str = "GANG_OS_PROCESS_CHILD";

    /// The line [`child_leads_a_group_of_two`] prints once both members run.
    const READY: &str = "group-ready";

    fn child_mode(mode: &str) -> bool {
        std::env::var(CHILD_ENV).ok().as_deref() == Some(mode)
    }

    const SOCKET_ENV: &str = "GANG_OS_PROCESS_SOCKET";

    #[ignore = "child process helper of the bounded runner tests"]
    #[test]
    fn bounded_child() -> Result<(), Box<dyn Error>> {
        let mode = std::env::var(CHILD_ENV)?;
        match mode.as_str() {
            "echo" => {
                io::copy(&mut io::stdin(), &mut io::stdout())?;
                eprintln!("bounded-echo");
            }
            "empty" => {
                let count = io::copy(&mut io::stdin(), &mut io::sink())?;
                eprintln!("bounded-input={count}");
            }
            "size" => {
                let count: usize = std::env::var("GANG_OS_PROCESS_SIZE")?.parse()?;
                eprintln!("bounded-size");
                io::stdout().write_all(&vec![b'x'; count])?;
                io::stdout().flush()?;
            }
            "stderr" => {
                io::stderr().write_all(&vec![b'e'; 3 * 131_072])?;
            }
            "exit" => {
                eprintln!("bounded-exit");
                std::process::exit(23);
            }
            "leader" => {
                eprintln!(
                    "bounded-leader={}",
                    rustix::process::getpgrp() == rustix::process::getpid()
                );
            }
            "socket-leader" => {
                let mut socket =
                    UnixStream::connect(std::env::var_os(SOCKET_ENV).ok_or("missing socket")?)?;
                writeln!(socket, "leader-ready")?;
                let mut member = BOUNDED_CHILD
                    .command()?
                    .env(CHILD_ENV, "socket-member")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                let _stdin = member.stdin.take();
                member.wait()?;
                drop(socket);
            }
            "socket-member" => {
                let mut socket =
                    UnixStream::connect(std::env::var_os(SOCKET_ENV).ok_or("missing socket")?)?;
                writeln!(socket, "member-ready")?;
                io::copy(&mut io::stdin(), &mut io::sink())?;
                drop(socket);
            }
            _ => return Err("unexpected helper mode".into()),
        }
        BOUNDED_CHILD.complete();
        Ok(())
    }

    fn bounded_command(mode: &str) -> Result<tokio::process::Command, Box<dyn Error>> {
        let mut command = tokio::process::Command::from(BOUNDED_CHILD.command()?);
        command.env(CHILD_ENV, mode);
        Ok(command)
    }

    /// Runs `command` through [`run_bounded`] on its own runtime while holding the fork gate, so
    /// its fork never copies a lock test's descriptor. The gate is a blocking lock, so it is held
    /// outside any await.
    fn gated_run_bounded(
        command: &mut tokio::process::Command,
        stdin: Option<&[u8]>,
        limit: gang_types::NonZeroBytes,
    ) -> Result<Result<RunOutput, RunError>, Box<dyn Error>> {
        let _spawn = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(runtime.block_on(run_bounded(command, stdin, limit)))
    }

    fn byte_limit(bytes: u64) -> Result<gang_types::NonZeroBytes, Box<dyn Error>> {
        gang_types::NonZeroBytes::new(gang_types::Bytes::new(bytes))
            .ok_or_else(|| "zero limit".into())
    }

    #[test]
    fn bounded_run_delivers_input_and_captures_both_streams() -> Result<(), Box<dyn Error>> {
        let output = gated_run_bounded(
            &mut bounded_command("echo")?,
            Some(b"input-marker"),
            byte_limit(4096)?,
        )??;
        assert!(output.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&output.stdout)?));
        assert!(
            output
                .stdout
                .windows(b"input-marker".len())
                .any(|w| w == b"input-marker")
        );
        assert!(
            output
                .stderr
                .windows(b"bounded-echo".len())
                .any(|w| w == b"bounded-echo")
        );
        Ok(())
    }

    #[test]
    fn bounded_run_null_input_is_eof() -> Result<(), Box<dyn Error>> {
        let output = gated_run_bounded(&mut bounded_command("empty")?, None, byte_limit(4096)?)??;
        assert!(output.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&output.stdout)?));
        assert!(
            output
                .stderr
                .windows(b"bounded-input=0".len())
                .any(|w| w == b"bounded-input=0")
        );
        Ok(())
    }

    #[test]
    fn bounded_run_enforces_stdout_boundary() -> Result<(), Box<dyn Error>> {
        let mut baseline = bounded_command("size")?;
        baseline.env("GANG_OS_PROCESS_SIZE", "0");
        let baseline = gated_run_bounded(&mut baseline, None, byte_limit(4096)?)??;
        assert!(baseline.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&baseline.stdout)?));
        assert!(
            baseline
                .stderr
                .windows(b"bounded-size".len())
                .any(|w| w == b"bounded-size")
        );
        let limit = byte_limit(u64::try_from(baseline.stdout.len())? + 16)?;
        let mut exact = bounded_command("size")?;
        exact.env("GANG_OS_PROCESS_SIZE", "16");
        let output = gated_run_bounded(&mut exact, None, limit)??;
        assert!(output.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&output.stdout)?));
        assert_eq!(u64::try_from(output.stdout.len())?, limit.as_u64());
        assert!(
            output
                .stderr
                .windows(b"bounded-size".len())
                .any(|w| w == b"bounded-size")
        );
        let mut over = bounded_command("size")?;
        over.env("GANG_OS_PROCESS_SIZE", "17");
        assert!(
            matches!(gated_run_bounded(&mut over, None, limit)?, Err(RunError::StdoutTooLarge { limit: actual, .. }) if actual == limit)
        );
        Ok(())
    }

    #[test]
    fn bounded_run_drains_truncated_stderr() -> Result<(), Box<dyn Error>> {
        let output =
            gated_run_bounded(&mut bounded_command("stderr")?, None, byte_limit(131_072)?)??;
        assert!(output.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&output.stdout)?));
        assert_eq!(output.stderr, vec![b'e'; 131_072]);
        Ok(())
    }

    #[test]
    fn bounded_run_returns_nonzero_status() -> Result<(), Box<dyn Error>> {
        let output = gated_run_bounded(&mut bounded_command("exit")?, None, byte_limit(4096)?)??;
        assert_eq!(output.status.code(), Some(23));
        assert!(
            output
                .stderr
                .windows(b"bounded-exit".len())
                .any(|w| w == b"bounded-exit")
        );
        Ok(())
    }

    #[test]
    fn bounded_run_starts_group_leader() -> Result<(), Box<dyn Error>> {
        let output = gated_run_bounded(&mut bounded_command("leader")?, None, byte_limit(4096)?)??;
        assert!(output.status.success());
        assert!(BOUNDED_CHILD.completed_in(std::str::from_utf8(&output.stdout)?));
        assert!(
            output
                .stderr
                .windows(b"bounded-leader=true".len())
                .any(|w| w == b"bounded-leader=true")
        );
        Ok(())
    }

    #[test]
    fn bounded_run_reports_missing_program() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("missing");
        let error = gated_run_bounded(
            &mut tokio::process::Command::new(&path),
            None,
            byte_limit(1)?,
        )?;
        assert!(
            matches!(error, Err(RunError::Spawn { program, source }) if program == path && source.kind() == io::ErrorKind::NotFound)
        );
        Ok(())
    }

    #[test]
    fn dropping_bounded_run_kills_the_whole_group() -> Result<(), Box<dyn Error>> {
        let _spawn = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("group.sock");
            let listener = UnixListener::bind(&path)?;
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let reader = tokio::task::spawn_blocking(move || -> io::Result<()> {
                let (first, _) = listener.accept()?;
                let (second, _) = listener.accept()?;
                let mut first = BufReader::new(first);
                let mut second = BufReader::new(second);
                let mut first_line = String::new();
                let mut second_line = String::new();
                first.read_line(&mut first_line)?;
                second.read_line(&mut second_line)?;
                if first_line != "leader-ready\n" || second_line != "member-ready\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "both group members must signal readiness",
                    ));
                }
                let _ = ready_tx.send(());
                io::copy(&mut first, &mut io::sink())?;
                io::copy(&mut second, &mut io::sink())?;
                Ok(())
            });
            let mut command = bounded_command("socket-leader")?;
            command.env(SOCKET_ENV, path);
            let mut run = Box::pin(run_bounded(&mut command, None, byte_limit(4096)?));
            tokio::select! {
                result = &mut run => return Err(format!("runner ended before helper was ready: {result:?}").into()),
                ready = ready_rx => ready?,
            }
            drop(run);
            reader.await??;
            Ok(())
        })
    }

    #[ignore = "child process helper of the process tests"]
    #[test]
    fn child_blocks_on_stdin() {
        if child_mode("block") {
            let _ = io::copy(&mut io::stdin(), &mut io::sink());
            CHILD_BLOCKS_ON_STDIN.complete();
        }
    }

    #[ignore = "child process helper of the process group join test"]
    #[test]
    fn child_joins_parent_group() -> Result<(), Box<dyn Error>> {
        if !child_mode("join-parent-group") {
            return Ok(());
        }
        println!(
            "pgrp-before={}",
            rustix::process::getpgrp().as_raw_nonzero().get()
        );
        join_parent_group()?;
        println!(
            "pgrp-after={}",
            rustix::process::getpgrp().as_raw_nonzero().get()
        );
        CHILD_JOINS_PARENT_GROUP.complete();
        Ok(())
    }

    #[test]
    fn joins_parent_group_from_own_group() -> Result<(), Box<dyn Error>> {
        let mut command = CHILD_JOINS_PARENT_GROUP.command()?;
        command
            .env(CHILD_ENV, "join-parent-group")
            .stdout(Stdio::piped())
            .process_group(0);
        let child = {
            let _guard = gang_testkit::fork_gate::spawner();
            command.spawn()?
        };
        let output = child.wait_with_output()?;
        CHILD_JOINS_PARENT_GROUP.assert_completed(&output);
        let text = std::str::from_utf8(&output.stdout)?;
        let before: i32 = text
            .split_whitespace()
            .find_map(|word| word.strip_prefix("pgrp-before="))
            .ok_or("missing pgrp-before")?
            .parse()?;
        let after: i32 = text
            .split_whitespace()
            .find_map(|word| word.strip_prefix("pgrp-after="))
            .ok_or("missing pgrp-after")?
            .parse()?;
        let parent_group = rustix::process::getpgrp().as_raw_nonzero().get();
        assert_ne!(before, parent_group);
        assert_eq!(after, parent_group);
        Ok(())
    }

    #[ignore = "child process helper of the process tests"]
    #[test]
    fn child_reports() -> Result<(), Box<dyn Error>> {
        if !child_mode("report") {
            return Ok(());
        }
        let n = io::copy(&mut io::stdin(), &mut io::sink())?;
        println!("stdin_bytes={n}");
        eprintln!("stderr_marker");
        println!("clicolor_force={:?}", std::env::var_os("CLICOLOR_FORCE"));
        println!(
            "leader={}",
            rustix::process::getpgrp() == rustix::process::getpid()
        );
        CHILD_REPORTS.complete();
        Ok(())
    }

    /// Starts a second member of its own process group that blocks on a pipe this process never
    /// writes, so both hold standard output until they are killed, then prints [`READY`] and waits.
    #[ignore = "child process helper of the process tests"]
    #[test]
    fn child_leads_a_group_of_two() -> Result<(), Box<dyn Error>> {
        if !child_mode("group") {
            return Ok(());
        }
        let mut member = CHILD_BLOCKS_ON_STDIN
            .command()?
            .env(CHILD_ENV, "block")
            .stdin(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        // Held open, so the member blocks until it is killed; `wait` would otherwise close it.
        let _pipe = member.stdin.take();
        println!("{READY}");
        member.wait()?;
        Ok(())
    }

    fn blocking_command() -> Result<Command, Box<dyn Error>> {
        let mut command = CHILD_BLOCKS_ON_STDIN.command()?;
        command
            .env(CHILD_ENV, "block")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0);
        Ok(command)
    }

    fn spawn_blocked() -> Result<Child, Box<dyn Error>> {
        let mut command = blocking_command()?;
        let _guard = gang_testkit::fork_gate::spawner();
        Ok(command.spawn()?)
    }

    fn leader_of(child: &Child) -> Result<ProcessGroup, Box<dyn Error>> {
        ProcessGroup::from_pid(child.id()).ok_or_else(|| "child has no pid".into())
    }

    #[test]
    fn terminate_kills_group_child_with_sigterm() -> Result<(), Box<dyn Error>> {
        let mut child = spawn_blocked()?;
        let group = leader_of(&child)?;
        group.terminate()?;
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(Signal::TERM.as_raw()));
        group.disarm();
        Ok(())
    }

    #[test]
    fn kill_kills_group_child_with_sigkill() -> Result<(), Box<dyn Error>> {
        let mut child = spawn_blocked()?;
        let group = leader_of(&child)?;
        group.kill()?;
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(Signal::KILL.as_raw()));
        group.disarm();
        Ok(())
    }

    #[test]
    fn dropping_armed_group_kills_child() -> Result<(), Box<dyn Error>> {
        let mut child = spawn_blocked()?;
        drop(leader_of(&child)?);
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(Signal::KILL.as_raw()));
        Ok(())
    }

    #[test]
    fn disarmed_group_survives_drop() -> Result<(), Box<dyn Error>> {
        let mut child = spawn_blocked()?;
        leader_of(&child)?.disarm();
        assert!(child.try_wait()?.is_none());
        drop(child.stdin.take());
        let output = child.wait_with_output()?;
        CHILD_BLOCKS_ON_STDIN.assert_completed(&output);
        Ok(())
    }

    #[test]
    fn from_pid_rejects_zero_and_overflow() {
        assert!(ProcessGroup::from_pid(0).is_none());
        assert!(ProcessGroup::from_pid(u32::MAX).is_none());
    }

    #[test]
    fn killing_the_group_kills_every_member() -> Result<(), Box<dyn Error>> {
        let mut command = CHILD_LEADS_A_GROUP_OF_TWO.command()?;
        command
            .env(CHILD_ENV, "group")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = {
            let _guard = gang_testkit::fork_gate::spawner();
            command.spawn()?
        };
        let group = leader_of(&child)?;
        let stdout = child.stdout.take().ok_or("child has no stdout")?;
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                return Err("the group leader exited before it was ready".into());
            }
            if line.trim_end() == READY {
                break;
            }
        }
        group.kill()?;
        // EOF arrives only once every holder of the pipe, the leader and the second member,
        // is dead.
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest)?;
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(Signal::KILL.as_raw()));
        group.disarm();
        Ok(())
    }

    #[test]
    fn tolerate_gone_forgives_only_a_missing_process() {
        assert!(tolerate_gone(Ok(())).is_ok());
        assert!(tolerate_gone(Err(Errno::SRCH)).is_ok());
        let err = tolerate_gone(Err(Errno::PERM)).expect_err("EPERM must stay an error");
        assert_eq!(err.raw_os_error(), Some(Errno::PERM.raw_os_error()));
    }

    #[test]
    fn terminate_process_ignores_pids_that_cannot_name_a_process() -> Result<(), Box<dyn Error>> {
        terminate_process(0)?;
        terminate_process(u32::MAX)?;
        Ok(())
    }

    #[test]
    fn terminate_process_ends_child_with_sigterm() -> Result<(), Box<dyn Error>> {
        let mut child = spawn_blocked()?;
        terminate_process(child.id())?;
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(Signal::TERM.as_raw()));
        Ok(())
    }

    #[tokio::test]
    async fn of_tokio_child_is_killed_and_none_after_reap() -> Result<(), Box<dyn Error>> {
        let command = blocking_command()?;
        let mut child = {
            let _guard = gang_testkit::fork_gate::spawner();
            tokio::process::Command::from(command).spawn()?
        };
        let group = ProcessGroup::of(&child).ok_or("child has no pid")?;
        group.kill()?;
        let status = child.wait().await?;
        assert_eq!(status.signal(), Some(Signal::KILL.as_raw()));
        assert!(ProcessGroup::of(&child).is_none());
        group.disarm();
        Ok(())
    }

    #[test]
    fn spawn_logged_appends_and_overrides_child_environment() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let log = dir.path().join("child.log");
        std::fs::write(&log, "old\n")?;
        let mut command = CHILD_REPORTS.command()?;
        command.env(CHILD_ENV, "report").env("CLICOLOR_FORCE", "1");
        let mut child = {
            let _guard = gang_testkit::fork_gate::spawner();
            spawn_logged(&mut command, Stdio::null(), &log)?
        };
        let status = child.wait()?;
        assert!(status.success());

        let text = std::fs::read_to_string(&log)?;
        assert!(CHILD_REPORTS.completed_in(&text), "{text:?}");
        assert!(text.starts_with("old\n"), "log was truncated: {text:?}");
        assert!(text.contains("stdin_bytes=0"), "{text:?}");
        assert!(text.contains("stderr_marker"), "{text:?}");
        assert!(text.contains("clicolor_force=None"), "{text:?}");
        assert!(text.contains("leader=true"), "{text:?}");
        Ok(())
    }

    #[test]
    fn spawn_logged_creates_missing_log_with_private_mode() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let log = dir.path().join("child.log");
        let mut command = CHILD_REPORTS.command()?;
        command.env(CHILD_ENV, "absent");
        let mut child = {
            let _guard = gang_testkit::fork_gate::spawner();
            spawn_logged(&mut command, Stdio::null(), &log)?
        };
        child.wait()?;
        let mode = std::fs::metadata(&log)?.permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        Ok(())
    }

    #[test]
    fn spawn_logged_reports_missing_log_directory() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let log = dir.path().join("missing").join("child.log");
        let mut command = Command::new(std::env::current_exe()?);
        let result = {
            let _guard = gang_testkit::fork_gate::spawner();
            spawn_logged(&mut command, Stdio::null(), &log)
        };
        let Err(err) = result else {
            return Err("spawn unexpectedly succeeded".into());
        };
        let SpawnError::Log(io_err) = &err else {
            return Err("expected a log error".into());
        };
        assert!(io_err.is_not_found());
        assert_eq!(err.to_string(), io_err.to_string());
        Ok(())
    }

    #[test]
    fn spawn_logged_reports_missing_program() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let log = dir.path().join("child.log");
        let mut command = Command::new("/nonexistent/gang-os-test");
        let result = {
            let _guard = gang_testkit::fork_gate::spawner();
            spawn_logged(&mut command, Stdio::null(), &log)
        };
        let Err(err) = result else {
            return Err("spawn unexpectedly succeeded".into());
        };
        assert_eq!(err.to_string(), "cannot spawn /nonexistent/gang-os-test");
        let SpawnError::Spawn { source, .. } = &err else {
            return Err("expected a spawn error".into());
        };
        assert_eq!(source.kind(), io::ErrorKind::NotFound);
        Ok(())
    }
}
