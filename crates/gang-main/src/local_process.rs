//! Process control and server launch behind traits, so the local up and down logic runs against fakes in tests.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use gang_clock::Poll;
use gang_os::fs::{PRIVATE_FILE, write_atomic};
use gang_os::{FileLock, PathIo, spawn_logged, terminate_process};
use gang_types::NonZeroDuration;

use crate::errors::LocalCommandError;

/// Subcommand that runs the gang server.
const SERVER_SUBCOMMAND: &str = "server";
/// Flag that names the configuration file for the server subcommand.
const CONFIG_FLAG: &str = "--config";

/// A bounded wait: at most `timeout`, polled every `poll_interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Wait {
    /// Longest time to wait.
    pub(crate) timeout: NonZeroDuration,
    /// Time between polls.
    pub(crate) poll_interval: NonZeroDuration,
}

impl Wait {
    /// Runs `probe`, pausing `poll_interval` between runs, until it finds a value or `timeout` has passed. The probe runs once after each of the `Poll::attempts` pauses, so it runs one more time than it pauses. `Ok(None)` means the time ran out.
    pub(crate) fn until<T, E>(
        &self,
        processes: &impl ProcessControl,
        mut probe: impl FnMut() -> Result<Option<T>, E>,
    ) -> Result<Option<T>, E> {
        let attempts = Poll::new(self.timeout.get(), self.poll_interval).attempts();
        for _ in 0..attempts {
            if let Some(found) = probe()? {
                return Ok(Some(found));
            }
            processes.pause(self.poll_interval.get());
        }
        probe()
    }
}

/// Termination and pauses between polls.
pub(crate) trait ProcessControl {
    /// Asks the process `pid` to terminate; a process that is already gone is not an error.
    fn terminate(&self, pid: u32) -> Result<(), LocalCommandError>;
    /// Waits `interval` before the next poll.
    fn pause(&self, interval: Duration);
}

/// A server process started by a [`ServerLauncher`].
pub(crate) trait LaunchedServer {
    /// The process id of the server.
    fn pid(&self) -> u32;
    /// Whether the server has exited; reaps it when it has, so an exited server is not left as a zombie.
    fn has_exited(&mut self) -> Result<bool, LocalCommandError>;
    /// Forcibly stops the server and reaps it; a server that already exited is not an error.
    fn kill(&mut self) -> Result<(), LocalCommandError>;
}

/// Starts the gang server as a child process that keeps the server lock.
pub(crate) trait ServerLauncher {
    /// The handle to the server this launcher starts.
    type Handle: LaunchedServer;
    /// Starts the server on `config`, writing its output to `log` (truncated first). `lock` is the held server lock; the server inherits a duplicate of it as stdin, so the lock lives as long as the server.
    fn launch(
        &self,
        config: &Path,
        log: &Path,
        lock: &FileLock,
    ) -> Result<Self::Handle, LocalCommandError>;
}

/// Process control backed by the operating system.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LocalStackProcesses;

impl ProcessControl for LocalStackProcesses {
    fn terminate(&self, pid: u32) -> Result<(), LocalCommandError> {
        terminate_process(pid).map_err(|source| LocalCommandError::Signal { pid, source })
    }

    fn pause(&self, interval: Duration) {
        std::thread::sleep(interval);
    }
}

/// Launches a server program with fixed arguments followed by the configuration path.
#[derive(Debug, Clone)]
pub(crate) struct SystemLauncher {
    /// The executable to run.
    program: PathBuf,
    /// Arguments placed before the configuration path.
    arguments: Vec<OsString>,
}

impl SystemLauncher {
    /// Launches the running `gang` executable as `gang server --config <config>`.
    pub(crate) fn gang() -> Result<Self, LocalCommandError> {
        let executable = std::env::current_exe().map_err(LocalCommandError::Executable)?;
        Ok(Self::new(executable, [SERVER_SUBCOMMAND, CONFIG_FLAG]))
    }

    /// Launches `program` with `arguments` followed by the configuration path.
    pub(crate) fn new(
        program: impl Into<PathBuf>,
        arguments: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        Self {
            program: program.into(),
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }
}

impl ServerLauncher for SystemLauncher {
    type Handle = SystemServer;

    fn launch(
        &self,
        config: &Path,
        log: &Path,
        lock: &FileLock,
    ) -> Result<SystemServer, LocalCommandError> {
        // `spawn_logged` appends, so the log is emptied here for the announcements of this run alone.
        write_atomic(log, b"", PRIVATE_FILE)?;
        let mut command = Command::new(&self.program);
        command.args(&self.arguments).arg(config);
        let child = spawn_logged(
            &mut command,
            lock.to_stdio().map_err(LocalCommandError::Stdin)?,
            log,
        )?;
        Ok(SystemServer {
            child,
            log: log.to_path_buf(),
        })
    }
}

/// A server child process started by [`SystemLauncher`].
#[derive(Debug)]
pub(crate) struct SystemServer {
    /// The child process.
    child: Child,
    /// The server log, named in I/O errors.
    log: PathBuf,
}

impl SystemServer {
    /// An I/O failure on the child, reported against the server log.
    fn failure(&self, action: &'static str, source: io::Error) -> LocalCommandError {
        LocalCommandError::Io(PathIo::new(action, &self.log, source))
    }
}

impl LaunchedServer for SystemServer {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn has_exited(&mut self) -> Result<bool, LocalCommandError> {
        match self.child.try_wait() {
            Ok(status) => Ok(status.is_some()),
            Err(source) => Err(self.failure("wait for", source)),
        }
    }

    fn kill(&mut self) -> Result<(), LocalCommandError> {
        match self.child.kill() {
            Ok(()) => {}
            // The child was already reaped, so there is nothing left to kill.
            Err(source) if source.kind() == io::ErrorKind::InvalidInput => {}
            Err(source) => return Err(self.failure("kill", source)),
        }
        self.child
            .wait()
            .map(|_| ())
            .map_err(|source| self.failure("wait for", source))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn wait(timeout: NonZeroDuration, poll_interval: NonZeroDuration) -> Wait {
        Wait {
            timeout,
            poll_interval,
        }
    }

    /// Runs a probe that never finds anything and returns the probe runs and pauses.
    fn misses(wait: Wait) -> (u32, u32) {
        let processes = CountingPauses::default();
        let mut runs = 0;
        let found: Result<Option<()>, &str> = wait.until(&processes, || {
            runs += 1;
            Ok(None)
        });
        assert_eq!(found, Ok(None));
        (runs, processes.pauses.get())
    }

    #[test]
    fn thirty_seconds_at_100ms_probes_301_times_with_300_pauses() {
        let counted = misses(wait(
            NonZeroDuration::from_secs(30),
            NonZeroDuration::from_millis(100),
        ));
        assert_eq!(counted, (301, 300));
    }

    #[test]
    fn two_hundred_fifty_ms_at_100ms_probes_four_times_with_three_pauses() {
        let counted = misses(wait(
            NonZeroDuration::from_millis(250),
            NonZeroDuration::from_millis(100),
        ));
        assert_eq!(counted, (4, 3));
    }

    #[test]
    fn one_ms_at_100ms_probes_twice_with_one_pause() {
        let counted = misses(wait(
            NonZeroDuration::from_millis(1),
            NonZeroDuration::from_millis(100),
        ));
        assert_eq!(counted, (2, 1));
    }

    #[test]
    fn poll_interval_longer_than_timeout_probes_twice_with_one_pause() {
        let counted = misses(wait(
            NonZeroDuration::from_millis(1),
            NonZeroDuration::from_secs(30),
        ));
        assert_eq!(counted, (2, 1));
    }

    /// Kills and reaps the launched server when dropped, so a failed assertion cannot leak the child or its lock.
    struct Reaped(SystemServer);

    impl Drop for Reaped {
        fn drop(&mut self) {
            let _ = self.0.kill();
        }
    }

    #[test]
    fn launched_server_keeps_the_lock_until_it_is_killed() {
        let _spawn = gang_testkit::fork_gate::spawner();
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("server.lock");
        let log = dir.path().join("server.log");
        let lock = FileLock::try_exclusive(&path)
            .expect("the lock can be taken")
            .expect("the lock is free");
        // The server is a Bazel-built probe that parks until killed: no host program runs.
        let probe = std::env::current_dir()
            .expect("the working directory")
            .join(
                std::env::var_os("FAKE_LAUNCHER")
                    .expect("FAKE_LAUNCHER must be set by the Bazel test_env"),
            );
        let mut server = Reaped(
            SystemLauncher::new(&probe, ["worker"])
                .launch(Path::new("config"), &log, &lock)
                .expect("the probe starts"),
        );
        drop(lock);
        assert!(FileLock::is_held(&path).expect("the lock can be probed"));
        server.0.kill().expect("the server is killed");
        assert!(!FileLock::is_held(&path).expect("the lock can be probed"));
        assert!(server.0.has_exited().expect("the child can be polled"));
    }

    /// Counts pauses and never terminates anything.
    #[derive(Default)]
    struct CountingPauses {
        pauses: Cell<u32>,
    }

    impl ProcessControl for CountingPauses {
        fn terminate(&self, _pid: u32) -> Result<(), LocalCommandError> {
            Ok(())
        }

        fn pause(&self, _interval: Duration) {
            self.pauses.set(self.pauses.get() + 1);
        }
    }

    #[test]
    fn until_returns_the_first_found_value_after_two_pauses() {
        let processes = CountingPauses::default();
        let mut runs = 0;
        let found: Result<Option<u32>, &str> = wait(
            NonZeroDuration::from_secs(10),
            NonZeroDuration::from_secs(1),
        )
        .until(&processes, || {
            runs += 1;
            Ok((runs == 3).then_some(runs))
        });
        assert_eq!(found, Ok(Some(3)));
        assert_eq!(runs, 3);
        assert_eq!(processes.pauses.get(), 2);
    }

    #[test]
    fn until_runs_polls_plus_one_times_and_pauses_polls_times_on_timeout() {
        let processes = CountingPauses::default();
        let mut runs = 0;
        let found: Result<Option<()>, &str> = wait(
            NonZeroDuration::from_secs(3),
            NonZeroDuration::from_secs(1),
        )
        .until(&processes, || {
            runs += 1;
            Ok(None)
        });
        assert_eq!(found, Ok(None));
        assert_eq!(runs, 4);
        assert_eq!(processes.pauses.get(), 3);
    }

    #[test]
    fn until_returns_a_probe_error_without_pausing() {
        let processes = CountingPauses::default();
        let found = wait(NonZeroDuration::from_secs(3), NonZeroDuration::from_secs(1))
            .until(&processes, || Err::<Option<()>, _>("probe failed"));
        assert_eq!(found, Err("probe failed"));
        assert_eq!(processes.pauses.get(), 0);
    }
}
