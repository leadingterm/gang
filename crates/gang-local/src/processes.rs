use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use gang_os::{FileLock, PathIo, ProcessGroup, spawn_logged};
use gang_types::Url;
use parking_lot::Mutex;

use crate::errors::LocalError;
use crate::worker_identity::WorkerIdentity;

/// Environment variable that names the instance a launched worker process is.
pub const INSTANCE_ENV: &str = "GANG_LOCAL_INSTANCE";

/// Environment variable that carries the secret a launched worker presents to the mock IMDS.
pub const INSTANCE_SECRET_ENV: &str = "GANG_LOCAL_INSTANCE_SECRET";

/// The server's resolved endpoints, handed to every worker it launches as `--endpoint` (public) and
/// `--worker-endpoint` (worker-facing), so workers bound to ephemeral ports still reach the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerEndpoints {
    /// The server's public endpoint URL.
    pub public: Url,
    /// The worker-facing endpoint URL.
    pub worker: Url,
}

/// A fully resolved worker launch: program, arguments, environment, lifetime lock and log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerCommand {
    /// Executable to run; the running `gang` binary for workers.
    pub program: PathBuf,
    /// Arguments passed after the program name.
    pub arguments: Vec<OsString>,
    /// Environment variables set on top of the inherited environment.
    pub environment: Vec<(OsString, OsString)>,
    /// Lock file whose exclusive `flock` marks the instance alive; it becomes the worker's standard input.
    pub lifetime_lock: PathBuf,
    /// File that receives both stdout and stderr, opened in append mode.
    pub log: PathBuf,
}

impl WorkerCommand {
    /// `<program> worker --config <config_path>` with `INSTANCE_ENV=<instance>` and
    /// `INSTANCE_SECRET_ENV=<secret>`, the lifetime lock as standard input and the output
    /// appended to `log`. A non-empty `launcher` runs its first element instead of
    /// `program`, with the rest of the launcher, then `program`, `worker` and the config flags as arguments.
    /// The launcher must exec those arguments in place, so the lock and the signals reach the worker.
    /// When `endpoints` is set, `--endpoint` and `--worker-endpoint` follow the config flag.
    #[must_use]
    pub(crate) fn gang_worker(
        program: &Path,
        config_path: &Path,
        worker: &WorkerIdentity,
        launcher: &[OsString],
        lifetime_lock: PathBuf,
        log: PathBuf,
        endpoints: Option<&WorkerEndpoints>,
    ) -> Self {
        let mut arguments = Vec::new();
        let executable = match launcher.split_first() {
            Some((launcher_program, launcher_arguments)) => {
                arguments.extend_from_slice(launcher_arguments);
                arguments.push(program.as_os_str().to_owned());
                PathBuf::from(launcher_program)
            }
            None => program.to_path_buf(),
        };
        arguments.extend([
            OsString::from("worker"),
            OsString::from("--config"),
            config_path.as_os_str().to_owned(),
        ]);
        if let Some(endpoints) = endpoints {
            arguments.extend([
                OsString::from("--endpoint"),
                OsString::from(endpoints.public.as_str()),
                OsString::from("--worker-endpoint"),
                OsString::from(endpoints.worker.as_str()),
            ]);
        }
        Self {
            program: executable,
            arguments,
            environment: vec![
                (
                    OsString::from(INSTANCE_ENV),
                    OsString::from(worker.instance.as_str()),
                ),
                (
                    OsString::from(INSTANCE_SECRET_ENV),
                    OsString::from(worker.secret.expose()),
                ),
            ],
            lifetime_lock,
            log,
        }
    }
}

/// Operating-system process control used by the fleet; abstracted so tests can fake it.
pub(crate) trait Processes: Send + Sync {
    /// Starts the worker with `command.lifetime_lock` locked and passed as its standard input (see `FileLock`);
    /// returns the pid of its process group leader.
    fn spawn(&self, command: &WorkerCommand) -> Result<u32, LocalError>;
    /// The cgroup the server runs in, or `None` when it is unknown.
    fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError>;
    /// Kills and removes a dead worker's cgroup, returning busy while processes remain.
    fn reap_cgroup(&self, dir: &Path) -> Result<gang_os::cgroup::Removal, LocalError>;
    /// SIGTERM to the process group led by `pid`; a process group that is already gone counts as success.
    fn terminate(&self, pid: u32) -> Result<(), LocalError>;
    /// SIGKILL to the process group led by `pid`; a group that is already gone counts as success.
    fn kill(&self, pid: u32) -> Result<(), LocalError>;
    /// Reaps children that have exited so they do not linger as zombies.
    fn reap(&self);
}

/// Real process control; keeps spawned children so that they are reaped once they exit.
pub(crate) struct WorkerProcesses {
    /// Children spawned by this server, keyed by pid, kept so exited ones can be waited on.
    children: Mutex<BTreeMap<u32, Child>>,
}

impl WorkerProcesses {
    /// Creates an empty process table.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            children: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Default for WorkerProcesses {
    fn default() -> Self {
        Self::new()
    }
}

/// Takes the lifetime lock of a worker about to start. A lock that another process holds is an error,
/// because the worker it would mark alive does not exist yet.
pub(crate) fn acquire_lifetime_lock(path: &Path) -> Result<FileLock, LocalError> {
    FileLock::try_exclusive(path)
        .map_err(LocalError::Io)?
        .ok_or_else(|| {
            LocalError::Io(PathIo::new(
                "lock worker lifetime",
                path,
                io::Error::from(io::ErrorKind::WouldBlock),
            ))
        })
}

/// Sends a signal to the process group led by `pid` through `send`. A group that is already gone
/// counts as success, and so does a pid that cannot name a process.
fn signal_group(pid: u32, send: fn(&ProcessGroup) -> io::Result<()>) -> Result<(), LocalError> {
    let Some(group) = ProcessGroup::from_pid(pid) else {
        return Ok(());
    };
    let result = send(&group);
    // Dropping a group kills it, which would turn SIGTERM into SIGKILL.
    group.disarm();
    result.map_err(|source| LocalError::Signal { pid, source })
}

impl Processes for WorkerProcesses {
    fn spawn(&self, command: &WorkerCommand) -> Result<u32, LocalError> {
        // The lock becomes the worker's standard input, so the worker and its children keep it held until they exit.
        let lock = acquire_lifetime_lock(&command.lifetime_lock)?;
        let mut worker = Command::new(&command.program);
        worker
            .args(&command.arguments)
            .envs(command.environment.iter().map(|(key, value)| (key, value)));
        let child = spawn_logged(&mut worker, Stdio::from(lock), &command.log)?;
        let pid = child.id();
        self.children.lock().insert(pid, child);
        Ok(pid)
    }

    fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
        Ok(gang_os::cgroup::of_process(std::process::id())?)
    }

    fn reap_cgroup(&self, dir: &Path) -> Result<gang_os::cgroup::Removal, LocalError> {
        gang_os::cgroup::kill_and_remove(dir).map_err(LocalError::Io)
    }

    fn terminate(&self, pid: u32) -> Result<(), LocalError> {
        signal_group(pid, ProcessGroup::terminate)
    }

    fn kill(&self, pid: u32) -> Result<(), LocalError> {
        signal_group(pid, ProcessGroup::kill)
    }

    fn reap(&self) {
        self.children
            .lock()
            .retain(|_, child| matches!(child.try_wait(), Ok(None)));
    }
}

/// The end of the log from its last `fail` record, trimmed, if it has one.
#[must_use]
pub(crate) fn failure_record(log: &str) -> Option<&str> {
    let mut offset = 0;
    let mut last_failure = None;
    for line in log.split_inclusive('\n') {
        if line.split_whitespace().next() == Some("fail") {
            last_failure = Some(offset);
        }
        offset += line.len();
    }
    last_failure
        .and_then(|start| log.get(start..))
        .map(str::trim)
}

/// The failure record, or the last non-empty line when there is none.
#[must_use]
pub(crate) fn exit_reason(log: &str) -> Option<&str> {
    failure_record(log).or_else(|| {
        log.lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failure_record_requires_a_fail_line() {
        assert_eq!(failure_record("INFO starting\nINFO stopped\n"), None);
        assert_eq!(
            failure_record("INFO starting\n fail  broken\n fix  retry\n"),
            Some("fail  broken\n fix  retry")
        );
    }

    #[test]
    fn exit_reason_returns_the_failure_record_and_its_continuation() {
        let log = "INFO starting worker\n  fail  gang worker: fleet `f` declares disk_per_instance 20GiB, but this worker's scratch volume leaves its actions 17GiB\n   fix  lower disk_per_instance\n";
        assert_eq!(
            exit_reason(log),
            Some(
                "fail  gang worker: fleet `f` declares disk_per_instance 20GiB, but this worker's scratch volume leaves its actions 17GiB\n   fix  lower disk_per_instance"
            )
        );
    }

    #[test]
    fn exit_reason_uses_the_last_failure_record() {
        assert_eq!(
            exit_reason("fail  first\nfix  first fix\n  fail  second\n  fix  second fix\n"),
            Some("fail  second\n  fix  second fix")
        );
    }

    #[test]
    fn exit_reason_uses_the_last_nonempty_line_without_a_record() {
        assert_eq!(
            exit_reason("INFO starting\n  INFO stopped  \n \n"),
            Some("INFO stopped")
        );
    }

    #[test]
    fn exit_reason_is_none_for_empty_logs() {
        assert_eq!(exit_reason(""), None);
        assert_eq!(exit_reason("\n  \n"), None);
    }

    #[test]
    fn exit_reason_does_not_treat_other_words_as_failure_records() {
        assert_eq!(
            exit_reason("INFO request failed\nINFO stopped\n"),
            Some("INFO stopped")
        );
    }

    /// The identity the launch tests pass to `gang_worker`.
    fn test_worker() -> Result<WorkerIdentity, Box<dyn std::error::Error>> {
        use crate::worker_identity::InstanceSecret;
        use gang_types::InstanceId;

        Ok(WorkerIdentity {
            instance: InstanceId::try_new("local-1".to_owned())?,
            secret: InstanceSecret::new("secret-1"),
        })
    }

    #[test]
    fn gang_worker_builds_the_documented_command() -> Result<(), Box<dyn std::error::Error>> {
        let worker = test_worker()?;
        let command = WorkerCommand::gang_worker(
            Path::new("/opt/gang"),
            Path::new("/etc/gang.toml"),
            &worker,
            &[],
            PathBuf::from("/state/instances/local-1/alive.lock"),
            PathBuf::from("/state/instances/local-1/worker.log"),
            None,
        );

        assert_eq!(command.program, PathBuf::from("/opt/gang"));
        assert_eq!(
            command.arguments,
            vec![
                OsString::from("worker"),
                OsString::from("--config"),
                OsString::from("/etc/gang.toml"),
            ]
        );
        assert!(
            command
                .environment
                .contains(&(OsString::from(INSTANCE_ENV), OsString::from("local-1"),)),
            "environment was {:?}",
            command.environment
        );
        assert!(
            command.environment.contains(&(
                OsString::from(INSTANCE_SECRET_ENV),
                OsString::from("secret-1")
            )),
            "environment was {:?}",
            command.environment
        );
        assert_eq!(
            command.lifetime_lock,
            PathBuf::from("/state/instances/local-1/alive.lock")
        );
        assert_eq!(
            command.log,
            PathBuf::from("/state/instances/local-1/worker.log")
        );
        Ok(())
    }

    #[test]
    fn gang_worker_prefixes_the_launcher_ahead_of_the_worker_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let worker = test_worker()?;
        let launcher = [
            OsString::from("systemd-run"),
            OsString::from("--user"),
            OsString::from("--"),
        ];
        let command = WorkerCommand::gang_worker(
            Path::new("/opt/gang"),
            Path::new("/etc/gang.toml"),
            &worker,
            &launcher,
            PathBuf::from("/state/instances/local-1/alive.lock"),
            PathBuf::from("/state/instances/local-1/worker.log"),
            None,
        );

        assert_eq!(command.program, PathBuf::from("systemd-run"));
        assert_eq!(
            command.arguments,
            vec![
                OsString::from("--user"),
                OsString::from("--"),
                OsString::from("/opt/gang"),
                OsString::from("worker"),
                OsString::from("--config"),
                OsString::from("/etc/gang.toml"),
            ]
        );
        assert_eq!(
            command.environment,
            vec![
                (OsString::from(INSTANCE_ENV), OsString::from("local-1")),
                (
                    OsString::from(INSTANCE_SECRET_ENV),
                    OsString::from("secret-1")
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn gang_worker_passes_the_resolved_server_endpoints_after_the_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let worker = test_worker()?;
        let endpoints = WorkerEndpoints {
            public: Url::parse("http://127.0.0.1:41000")?,
            worker: Url::parse("http://127.0.0.1:41001")?,
        };
        let command = WorkerCommand::gang_worker(
            Path::new("/opt/gang"),
            Path::new("/etc/gang.toml"),
            &worker,
            &[],
            PathBuf::from("/state/instances/local-1/alive.lock"),
            PathBuf::from("/state/instances/local-1/worker.log"),
            Some(&endpoints),
        );

        assert_eq!(
            command.arguments,
            vec![
                OsString::from("worker"),
                OsString::from("--config"),
                OsString::from("/etc/gang.toml"),
                OsString::from("--endpoint"),
                OsString::from("http://127.0.0.1:41000/"),
                OsString::from("--worker-endpoint"),
                OsString::from("http://127.0.0.1:41001/"),
            ]
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_launcher_that_execs_hands_the_lock_and_the_signal_to_the_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::ExitStatusExt;

        let _gate = gang_testkit::fork_gate::spawner();

        let dir = tempfile::tempdir()?;
        let probe = std::env::current_dir()?.join(
            std::env::var_os("FAKE_LAUNCHER")
                .ok_or("FAKE_LAUNCHER must be set by the Bazel test_env")?,
        );
        // The launcher and the worker are a Bazel-built probe: no host program runs.
        let lock = dir.path().join("alive.lock");
        let worker = test_worker()?;
        let launcher = [probe.clone().into_os_string()];
        let command = WorkerCommand::gang_worker(
            &probe,
            Path::new("gang.toml"),
            &worker,
            &launcher,
            lock.clone(),
            dir.path().join("worker.log"),
            None,
        );

        let processes = WorkerProcesses::new();
        let pid = processes.spawn(&command)?;
        // Collect every outcome before returning so that the child is always signalled and waited.
        let held_while_running = FileLock::is_held(&lock);
        let terminated = processes.terminate(pid);
        let removed = processes.children.lock().remove(&pid);
        let exited = match removed {
            Some(mut child) => child.wait().map_err(|source| source.to_string()),
            None => Err("the spawned child is missing from the process table".to_owned()),
        };
        let held_after_exit = FileLock::is_held(&lock);

        terminated?;
        assert!(
            held_while_running?,
            "the worker must hold the lock after the launcher execs"
        );
        let status = exited?;
        assert!(
            !held_after_exit?,
            "the lock must be released once the worker has exited"
        );
        assert_eq!(
            status.signal(),
            Some(rustix::process::Signal::TERM.as_raw())
        );
        Ok(())
    }

    const KILL_TARGET: gang_testkit::ChildTest =
        gang_testkit::ChildTest::new("processes::tests::kill_target_child");

    /// Environment variable that makes `kill_target_child` block until it is killed.
    const KILL_TARGET_ENV: &str = "GANG_LOCAL_KILL_TARGET";

    /// A worker that re-runs `kill_target_child` of this test binary with `KILL_TARGET_ENV` set.
    fn kill_target_command(dir: &Path) -> Result<WorkerCommand, std::io::Error> {
        Ok(WorkerCommand {
            program: std::env::current_exe()?,
            arguments: KILL_TARGET
                .arguments()
                .into_iter()
                .map(OsString::from)
                .collect(),
            environment: vec![(OsString::from(KILL_TARGET_ENV), OsString::from("1"))],
            lifetime_lock: dir.join("alive.lock"),
            log: dir.join("worker.log"),
        })
    }

    /// The worker that `kill_sends_sigkill_to_the_worker_group` and the group-gone test spawn.
    /// Unless `KILL_TARGET_ENV` is set it returns at once; otherwise it parks its thread until a
    /// signal kills it; it never exits by itself.
    #[ignore = "child process helper of the processes tests"]
    #[test]
    fn kill_target_child() {
        if std::env::var_os(KILL_TARGET_ENV).is_none() {
            return;
        }
        loop {
            std::thread::park();
        }
    }

    #[cfg(unix)]
    #[test]
    fn kill_sends_sigkill_to_the_worker_group() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::ExitStatusExt;
        let _gate = gang_testkit::fork_gate::spawner();

        let dir = tempfile::tempdir()?;
        let command = kill_target_command(dir.path())?;

        let processes = WorkerProcesses::new();
        let pid = processes.spawn(&command)?;
        // Collect every outcome before returning so that the child is always waited on.
        let killed = processes.kill(pid);
        let removed = processes.children.lock().remove(&pid);
        let exited = match removed {
            Some(mut child) => child.wait().map_err(|source| source.to_string()),
            None => Err("the spawned child is missing from the process table".to_owned()),
        };

        killed?;
        assert_eq!(
            exited?.signal(),
            Some(rustix::process::Signal::KILL.as_raw())
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn terminate_and_kill_of_pid_zero_are_ok_and_the_process_survives()
    -> Result<(), Box<dyn std::error::Error>> {
        let processes = WorkerProcesses::new();
        // Pid 0 names the caller's own process group; it must be refused before reaching kill(2).
        processes.terminate(0)?;
        processes.kill(0)?;
        // Reaching this line with the test runner alive shows that neither signal hit this group.
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn signalling_a_group_that_is_gone_is_ok() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::ExitStatusExt;
        let _gate = gang_testkit::fork_gate::spawner();

        let dir = tempfile::tempdir()?;
        let command = kill_target_command(dir.path())?;

        let processes = WorkerProcesses::new();
        let pid = processes.spawn(&command)?;
        let killed = processes.kill(pid);
        let removed = processes.children.lock().remove(&pid);
        let exited = match removed {
            Some(mut child) => child.wait().map_err(|source| source.to_string()),
            None => Err("the spawned child is missing from the process table".to_owned()),
        };

        killed?;
        assert_eq!(
            exited?.signal(),
            Some(rustix::process::Signal::KILL.as_raw())
        );
        processes.kill(pid)?;
        processes.terminate(pid)?;
        Ok(())
    }
}
