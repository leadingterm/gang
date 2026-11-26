use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use gang_os::{FileLock, PathIo, ProcessGroup, SpawnError, spawn_logged_piped};
use gang_types::Url;
use parking_lot::Mutex;

use crate::errors::LocalError;
use crate::layout::StateLayout;
use crate::records::{Mutation, RecordStore};
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
    /// Gang executable that commits the record before executing the final command.
    pub bootstrap: PathBuf,
    /// Initial instance record to commit from the bootstrap.
    pub record: PathBuf,
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
    /// The launcher must place each worker in a new private cgroup or scope owned exclusively by that
    /// worker, never an existing or shared scope, and exec those arguments in place, so the lock and
    /// signals reach the worker. On hosts without cgroups, cgroup reaping is inapplicable.
    /// When `endpoints` is set, `--endpoint` and `--worker-endpoint` follow the config flag.
    #[must_use]
    pub(crate) fn gang_worker(
        program: &Path,
        config_path: &Path,
        worker: &WorkerIdentity,
        launcher: &[OsString],
        layout: &StateLayout,
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
            bootstrap: program.to_path_buf(),
            record: layout.record_path(&worker.instance),
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
            lifetime_lock: layout.lock_path(&worker.instance),
            log: layout.log_path(&worker.instance),
        }
    }
}

/// Operating-system process control used by the fleet; abstracted so tests can fake it.
pub(crate) trait Processes: Send + Sync {
    /// Starts the worker with its lifetime lock as standard input and returns its process group pid
    /// only after that pid is committed to the instance record. A failed commit never starts the
    /// final worker.
    fn spawn(&self, mutation: &Mutation, command: &WorkerCommand) -> Result<u32, LocalError>;
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

/// The process table as the fleet changes it.
pub(crate) struct FleetProcesses<P>(P);

impl<P: Processes> FleetProcesses<P> {
    /// Wraps the underlying process table.
    pub(crate) fn new(processes: P) -> Self {
        Self(processes)
    }

    /// Starts a worker if the deadline has not passed.
    pub(crate) fn spawn(
        &self,
        mutation: &Mutation,
        command: &WorkerCommand,
    ) -> Result<u32, LocalError> {
        mutation.check()?;
        self.0.spawn(mutation, command)
    }

    /// Returns the server's cgroup.
    pub(crate) fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
        self.0.server_cgroup()
    }

    /// Reaps a worker's cgroup if the deadline has not passed.
    pub(crate) fn reap_cgroup(
        &self,
        mutation: &Mutation,
        dir: &Path,
    ) -> Result<gang_os::cgroup::Removal, LocalError> {
        mutation.check()?;
        self.0.reap_cgroup(dir)
    }

    /// Signals a worker if the deadline has not passed.
    pub(crate) fn terminate(&self, mutation: &Mutation, pid: u32) -> Result<(), LocalError> {
        mutation.check()?;
        self.0.terminate(pid)
    }

    /// Kills a worker if the deadline has not passed.
    pub(crate) fn kill(&self, mutation: &Mutation, pid: u32) -> Result<(), LocalError> {
        mutation.check()?;
        self.0.kill(pid)
    }

    /// Reaps exited children.
    pub(crate) fn reap(&self) {
        self.0.reap();
    }
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

    fn adopt_bootstrap(
        &self,
        deadline: &gang_spi::Deadline,
        mut child: Child,
        readiness: Option<OwnedFd>,
    ) -> Result<u32, LocalError> {
        let pid = child.id();
        let group = ProcessGroup::from_pid(pid);
        // The readiness pipe stays open until the bootstrap is killed and reaped below: closing it
        // first lets the bootstrap fail on a broken pipe and log that failure before it dies.
        let mut stdout = readiness;
        let mut reader = None;
        let readiness = (|| {
            let handle = tokio::runtime::Handle::try_current()
                .map_err(|error| LocalError::Bootstrap(io::Error::other(error)))?;
            handle.block_on(async {
                let stdout = stdout.take().ok_or_else(|| {
                    LocalError::Bootstrap(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "worker bootstrap stdout missing",
                    ))
                })?;
                let reader = &*reader
                    .insert(gang_os::PipeReader::new(stdout).map_err(LocalError::Bootstrap)?);
                tokio::select! {
                    biased;
                    () = deadline.invalidated() => deadline.check().map_err(LocalError::from),
                    result = read_readiness(reader) => {
                        deadline.check()?;
                        result.map_err(LocalError::Bootstrap)
                    }
                }
            })
        })();
        if let Err(error) = readiness {
            if let Some(group) = group {
                if let Err(cleanup) = group.kill() {
                    tracing::warn!(pid, error = %cleanup, "failed to kill worker bootstrap group");
                }
                group.disarm();
            } else if let Err(cleanup) = child.kill() {
                tracing::warn!(pid, error = %cleanup, "failed to kill worker bootstrap");
            }
            if let Err(cleanup) = child.wait() {
                tracing::warn!(pid, error = %cleanup, "failed to reap worker bootstrap");
            }
            return Err(error);
        }
        self.children.lock().insert(pid, child);
        if let Some(group) = group {
            group.disarm();
        }
        Ok(pid)
    }
}

/// Reads the bootstrap's readiness byte, then its exec receipt.
async fn read_readiness(reader: &gang_os::PipeReader) -> io::Result<()> {
    let mut byte = [0; 1];
    if reader.read(&mut byte).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "missing worker readiness byte",
        ));
    }
    if byte != *b"R" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid worker readiness byte",
        ));
    }
    read_exec_receipt(reader).await
}

async fn read_exec_receipt(reader: &gang_os::PipeReader) -> io::Result<()> {
    let mut tag = [0; 1];
    if reader.read(&mut tag).await? == 0 {
        return Ok(());
    }
    if tag != *b"E" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid exec receipt",
        ));
    }
    let mut bytes = [0; 4];
    let mut filled = 0;
    while filled < bytes.len() {
        let count = reader.read(&mut bytes[filled..]).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated exec receipt",
            ));
        }
        filled += count;
    }
    let errno = i32::from_le_bytes(bytes);
    Err(if errno == 0 {
        io::Error::other("worker launcher exec failed")
    } else {
        io::Error::from_raw_os_error(errno)
    })
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

/// Internal child bootstrap, not a user-facing command.
///
/// Standard input holds the controller mutation lock, standard output is the parent's readiness
/// pipe, and standard error is the worker log. The bootstrap acquires the lifetime lock and commits
/// its pid while holding both locks. It replaces stdin with the lifetime lock before acknowledging
/// readiness, then execs the worker with that stdin inherited.
///
/// # Errors
///
/// Returns an error if the record cannot be committed, readiness cannot be signalled, or exec fails.
pub fn bootstrap_worker(
    record: &Path,
    lifetime_lock: &Path,
    program: &OsStr,
    arguments: &[OsString],
) -> Result<std::convert::Infallible, LocalError> {
    let lock = acquire_lifetime_lock(lifetime_lock)?;
    RecordStore::commit_pid(record, std::process::id())?;
    // Retain the receipt pipe across stdout redirection; successful exec closes this duplicate.
    let stdout = io::stdout()
        .as_fd()
        .try_clone_to_owned()
        .map_err(LocalError::Bootstrap)?;
    let mut readiness = std::fs::File::from(stdout);
    exec_committed_worker(lock, &mut readiness, program, arguments)
}

fn exec_committed_worker(
    lifetime: FileLock,
    readiness: &mut impl Write,
    program: &OsStr,
    arguments: &[OsString],
) -> Result<std::convert::Infallible, LocalError> {
    let stderr = io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map_err(LocalError::Bootstrap)?;
    lifetime.replace_stdin().map_err(LocalError::Bootstrap)?;
    drop(lifetime);
    readiness.write_all(b"R").map_err(LocalError::Bootstrap)?;
    readiness.flush().map_err(LocalError::Bootstrap)?;
    let source = Command::new(program)
        .args(arguments)
        .stdin(Stdio::inherit())
        .stdout(Stdio::from(stderr))
        .exec();
    let mut receipt = [0; 5];
    receipt[0] = b'E';
    receipt[1..].copy_from_slice(&source.raw_os_error().unwrap_or(0).to_le_bytes());
    readiness
        .write_all(&receipt)
        .map_err(LocalError::Bootstrap)?;
    Err(LocalError::Spawn(SpawnError::Spawn {
        program: PathBuf::from(program),
        source,
    }))
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
    fn spawn(&self, mutation: &Mutation, command: &WorkerCommand) -> Result<u32, LocalError> {
        mutation.check()?;
        let stdin = mutation.bootstrap_stdio()?;
        let mut worker = Command::new(&command.bootstrap);
        worker
            .arg("__local-worker")
            .arg("--record")
            .arg(&command.record)
            .arg("--lifetime-lock")
            .arg(&command.lifetime_lock)
            .arg("--")
            .arg(&command.program)
            .args(&command.arguments)
            .envs(command.environment.iter().map(|(key, value)| (key, value)));
        let mut child = spawn_logged_piped(&mut worker, stdin, &command.log)?;
        let readiness = child.stdout.take().map(OwnedFd::from);
        self.adopt_bootstrap(mutation.deadline(), child, readiness)
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
    use gang_testkit::fence::DeadlineScope;
    use std::io::Read;
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn exec_receipt_reports_protocol_outcomes() -> io::Result<()> {
        // ENOENT is 2 on the supported Unix platforms.
        let cases: &[(&[u8], Option<io::ErrorKind>)] = &[
            (b"", None),
            (b"E\x02\x00\x00\x00", Some(io::ErrorKind::NotFound)),
            (b"E\x00\x00\x00\x00", Some(io::ErrorKind::Other)),
            (b"E\x02\x00", Some(io::ErrorKind::UnexpectedEof)),
            (b"X", Some(io::ErrorKind::InvalidData)),
        ];
        for (bytes, expected) in cases {
            let (read, mut write) = std::io::pipe()?;
            write.write_all(bytes)?;
            drop(write);
            let reader = gang_os::PipeReader::new(read.into())?;
            let result = read_exec_receipt(&reader).await;
            assert_eq!(result.err().map(|error| error.kind()), *expected);
        }
        Ok(())
    }
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

    fn initialized_layout(
        dir: &Path,
        worker: &WorkerIdentity,
    ) -> Result<StateLayout, Box<dyn std::error::Error>> {
        use crate::records::InstanceRecord;
        use gang_clock::Timestamp;
        use gang_types::{FleetName, Isa};

        let layout = StateLayout::new(dir.join("state"));
        layout.prepare()?;
        let scope = DeadlineScope::try_new()?;
        let store = RecordStore::new(layout.clone());
        let mutation = store.mutation(scope.deadline())?;
        store.write(
            &mutation,
            &InstanceRecord {
                id: worker.instance.clone(),
                fleet: FleetName::try_new("workers-x86")?,
                isa: Isa::X86_64,
                pid: None,
                launch_cgroup: None,
                launched_at: Timestamp::from_unix_millis(1_000),
                exited_at: None,
                terminating_since: None,
                secret: worker.secret.clone(),
            },
        )?;
        Ok(layout)
    }

    fn fake_launcher() -> Result<PathBuf, Box<dyn std::error::Error>> {
        Ok(std::env::current_dir()?.join(
            std::env::var_os("FAKE_LAUNCHER")
                .ok_or("FAKE_LAUNCHER must be set by the Bazel test_env")?,
        ))
    }
    async fn spawn_test_worker(
        processes: Arc<WorkerProcesses>,
        layout: StateLayout,
        deadline: gang_spi::Deadline,
        command: WorkerCommand,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        Ok(tokio::task::spawn_blocking(move || {
            let mutation = RecordStore::new(layout).mutation(&deadline)?;
            processes.spawn(&mutation, &command)
        })
        .await??)
    }

    const COMMITTED_BOOTSTRAP: gang_testkit::ChildTest =
        gang_testkit::ChildTest::new("processes::tests::committed_bootstrap_child");

    struct PauseAfterReady {
        readiness: std::os::unix::net::UnixStream,
        release: std::os::unix::net::UnixStream,
    }

    impl Write for PauseAfterReady {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.readiness.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.readiness.flush()?;
            self.release.write_all(b"F")?;
            self.release.flush()?;
            let mut byte = [0];
            self.release.read_exact(&mut byte)?;
            if byte != *b"G" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid post-readiness release byte",
                ));
            }
            Ok(())
        }
    }

    #[test]
    #[ignore = "child process helper of the processes tests"]
    fn committed_bootstrap_child() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::net::UnixStream;
        let record = PathBuf::from(
            std::env::var_os("GANG_TEST_COMMITTED_RECORD").ok_or("record path missing")?,
        );
        let lock_path =
            PathBuf::from(std::env::var_os("GANG_TEST_COMMITTED_LOCK").ok_or("lock path missing")?);
        let gate_path = std::env::var_os("GANG_TEST_COMMITTED_GATE").ok_or("gate path missing")?;
        let ready_path =
            std::env::var_os("GANG_TEST_COMMITTED_READY").ok_or("readiness path missing")?;
        let program = std::env::var_os("GANG_TEST_COMMITTED_PROGRAM").ok_or("program missing")?;
        let lifetime = acquire_lifetime_lock(&lock_path)?;
        RecordStore::commit_pid(&record, std::process::id())?;
        let mut gate = UnixStream::connect(gate_path)?;
        gate.write_all(&std::process::id().to_ne_bytes())?;
        let mut ready = UnixStream::connect(ready_path)?;
        if std::env::var_os("GANG_TEST_PAUSE_AFTER_READY").is_some() {
            let mut writer = PauseAfterReady {
                readiness: ready,
                release: gate,
            };
            match exec_committed_worker(
                lifetime,
                &mut writer,
                &program,
                &[OsString::from("worker")],
            ) {
                Ok(never) => match never {},
                Err(error) => Err(error.into()),
            }
        } else {
            let mut byte = [0];
            gate.read_exact(&mut byte)?;
            if byte != *b"G" {
                return Err("invalid bootstrap release byte".into());
            }
            match exec_committed_worker(lifetime, &mut ready, &program, &[OsString::from("worker")])
            {
                Ok(never) => match never {},
                Err(error) => Err(error.into()),
            }
        }
    }

    fn committed_child_command(
        layout: &StateLayout,
        worker: &WorkerIdentity,
        gate: &Path,
        ready: &Path,
        witness: &Path,
        pause_after_ready: bool,
    ) -> Result<Command, Box<dyn std::error::Error>> {
        let mut command = COMMITTED_BOOTSTRAP.command()?;
        command
            .env(
                "GANG_TEST_COMMITTED_RECORD",
                layout.record_path(&worker.instance),
            )
            .env(
                "GANG_TEST_COMMITTED_LOCK",
                layout.lock_path(&worker.instance),
            )
            .env("GANG_TEST_COMMITTED_GATE", gate)
            .env("GANG_TEST_COMMITTED_READY", ready)
            .env("GANG_TEST_COMMITTED_PROGRAM", fake_launcher()?)
            .env("GANG_TEST_WORKER_WITNESS", witness);
        if pause_after_ready {
            command.env("GANG_TEST_PAUSE_AFTER_READY", "1");
        }
        Ok(command)
    }

    type AdoptionReport = Result<(Result<u32, LocalError>, bool), String>;

    struct CleanupReport {
        empty: bool,
        killed: Result<(), String>,
        waited: Result<(), String>,
    }

    fn committed_owner(
        layout: StateLayout,
        deadline: &gang_spi::Deadline,
        mut command: Command,
        log: &Path,
        readiness_rx: &std::sync::mpsc::Receiver<(OwnedFd, tokio::sync::oneshot::Sender<()>)>,
        report_tx: tokio::sync::oneshot::Sender<AdoptionReport>,
        cleanup_rx: &std::sync::mpsc::Receiver<()>,
    ) -> CleanupReport {
        let processes = WorkerProcesses::new();
        let result = (|| {
            let mutation = RecordStore::new(layout)
                .mutation(deadline)
                .map_err(|e| e.to_string())?;
            let stdin = mutation.bootstrap_stdio().map_err(|e| e.to_string())?;
            let child =
                gang_os::spawn_logged(&mut command, stdin, log).map_err(|e| e.to_string())?;
            drop(command);
            let readiness = readiness_rx.recv().ok();
            let work = mutation.into_work();
            let readiness = readiness.map(|(fd, released)| {
                let _ = released.send(());
                fd
            });
            let result = processes.adopt_bootstrap(work.deadline(), child, readiness);
            drop(work);
            let adopted = !processes.children.lock().is_empty();
            Ok((result, adopted))
        })();
        let pid = result
            .as_ref()
            .ok()
            .and_then(|(result, _)| result.as_ref().ok())
            .copied();
        let _ = report_tx.send(result);
        let _ = cleanup_rx.recv();
        let (killed, waited) = if let Some(pid) = pid {
            let killed = processes.kill(pid).map_err(|e| e.to_string());
            let waited = match processes.children.lock().remove(&pid) {
                Some(mut child) => child.wait().map(|_| ()).map_err(|e| e.to_string()),
                None => Err("spawned child missing from process table".to_owned()),
            };
            (killed, waited)
        } else {
            (Ok(()), Ok(()))
        };
        let empty = processes.children.lock().is_empty();
        CleanupReport {
            empty,
            killed,
            waited,
        }
    }

    #[derive(Clone, Copy)]
    enum CommittedCase {
        Drop,
        Expire,
        AfterReady,
        DropAfterReady,
        ExpireAfterReady,
        ExecFailure,
    }

    impl CommittedCase {
        fn after_ready(self) -> bool {
            matches!(
                self,
                Self::AfterReady | Self::DropAfterReady | Self::ExpireAfterReady
            )
        }
    }

    async fn accept_test_socket(
        listener: &tokio::net::UnixListener,
    ) -> Result<std::os::unix::net::UnixStream, Box<dyn std::error::Error>> {
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept()).await??;
        let stream = stream.into_std()?;
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
        Ok(stream)
    }

    async fn committed_case_inner(case: CommittedCase) -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Duration;
        let dir = tempfile::tempdir()?;
        let worker = test_worker()?;
        let layout = initialized_layout(dir.path(), &worker)?;
        // Unix socket paths are short; Bazel's test temporary directory may exceed SUN_LEN.
        let sockets = tempfile::Builder::new()
            .prefix("gang-sock")
            .tempdir_in("/tmp")?;
        let gate_path = sockets.path().join("commit-gate.sock");
        let ready_path = sockets.path().join("commit-ready.sock");
        let witness_path = sockets.path().join("commit-worker.sock");
        let gate_listener = tokio::net::UnixListener::bind(&gate_path)?;
        let ready_listener = tokio::net::UnixListener::bind(&ready_path)?;
        let witness = std::os::unix::net::UnixListener::bind(&witness_path)?;
        witness.set_nonblocking(true)?;
        let scope = DeadlineScope::try_new()?;
        let clock = scope.clock().clone();
        let after_ready = case.after_ready();
        let mut command = committed_child_command(
            &layout,
            &worker,
            &gate_path,
            &ready_path,
            &witness_path,
            after_ready,
        )?;
        if matches!(case, CommittedCase::ExecFailure) {
            command.env(
                "GANG_TEST_COMMITTED_PROGRAM",
                dir.path().join("missing-worker-launcher"),
            );
        }
        let (readiness_tx, readiness_rx) = std::sync::mpsc::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let (report_tx, mut report_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
        let owner_layout = layout.clone();
        let deadline = scope.deadline().clone();
        let log = layout.log_path(&worker.instance);
        let owner = tokio::task::spawn_blocking(move || {
            committed_owner(
                owner_layout,
                &deadline,
                command,
                &log,
                &readiness_rx,
                report_tx,
                &cleanup_rx,
            )
        });
        let observed = async {
            let mut gate = accept_test_socket(&gate_listener).await?;
            let mut pid_bytes = [0; 4];
            gate.read_exact(&mut pid_bytes)?;
            let pid = u32::from_ne_bytes(pid_bytes);
            let ready = accept_test_socket(&ready_listener).await?;
            let mut record = RecordStore::new(layout.clone()).read(&worker.instance)?;
            let mut held = FileLock::is_held(&layout.lock_path(&worker.instance))?;
            readiness_tx.send((ready.into(), released_tx))?;
            let mut fresh_lock = None;
            let mut no_worker = None;
            if after_ready {
                let mut byte = [0];
                gate.read_exact(&mut byte)?;
                released_rx.await?;
                record = RecordStore::new(layout.clone()).read(&worker.instance)?;
                held = FileLock::is_held(&layout.lock_path(&worker.instance))?;
                let fresh = DeadlineScope::try_new()?;
                let store = RecordStore::new(layout.clone());
                let mutation = store.mutation(fresh.deadline());
                fresh_lock = Some(mutation.is_ok() && byte == *b"F");
                drop(mutation);
                no_worker = Some(matches!(witness.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
            }
            match case {
                CommittedCase::Drop | CommittedCase::DropAfterReady => drop(scope),
                CommittedCase::Expire | CommittedCase::ExpireAfterReady => clock.advance(Duration::from_secs(1)),
                CommittedCase::AfterReady | CommittedCase::ExecFailure => gate.write_all(b"G")?,
            }
            let (without_rescue, report) = if let Ok(report) = tokio::time::timeout(Duration::from_secs(5), &mut report_rx).await {
                (true, report?)
            } else {
                let _ = gate.write_all(b"G");
                drop(gate);
                return Ok::<_, Box<dyn std::error::Error>>((false, report_rx.await?, pid, record, held, fresh_lock, no_worker, None));
            };
            Ok((without_rescue, report, pid, record, held, fresh_lock, no_worker, Some(gate)))
        }.await;
        drop(readiness_tx);
        // Disconnecting the readiness channel makes the owner clean up a child on setup failure.
        let _ = cleanup_tx.send(());
        let CleanupReport {
            empty,
            killed,
            waited,
        } = owner.await?;
        killed.map_err(|e| format!("bootstrap cleanup kill failed: {e}"))?;
        waited.map_err(|e| format!("bootstrap cleanup wait failed: {e}"))?;
        assert!(empty, "bootstrap owner retained a child");
        verify_committed_observations(case, layout, &worker, &witness, observed?)
    }

    type CommittedObservation = (
        bool,
        AdoptionReport,
        u32,
        Option<crate::records::InstanceRecord>,
        bool,
        Option<bool>,
        Option<bool>,
        Option<std::os::unix::net::UnixStream>,
    );

    fn verify_committed_observations(
        case: CommittedCase,
        layout: StateLayout,
        worker: &WorkerIdentity,
        witness: &std::os::unix::net::UnixListener,
        observed: CommittedObservation,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let after_ready = case.after_ready();
        let (without_rescue, report, pid, record, held, fresh_lock, no_worker, _gate) = observed;
        let (result, adopted) = report.map_err(|e| format!("bootstrap setup failed: {e}"))?;
        assert!(!FileLock::is_held(&layout.lock_path(&worker.instance))?);
        assert!(held, "lifetime lock was not held at the committed gate");
        assert!(record.is_some_and(|record| record.pid == Some(pid)));
        assert!(without_rescue, "bootstrap needed watchdog rescue");
        if after_ready {
            assert_eq!(
                fresh_lock,
                Some(true),
                "mutation lock remained held after readiness"
            );
            assert_eq!(no_worker, Some(true), "worker exec occurred before release");
        }
        if matches!(case, CommittedCase::AfterReady) {
            assert_eq!(result?, pid);
            assert!(adopted);
        } else if matches!(case, CommittedCase::ExecFailure) {
            assert!(
                matches!(
                    &result,
                    Err(LocalError::Spawn(SpawnError::Spawn { source, .. })
                        | LocalError::Bootstrap(source))
                        if source.kind() == io::ErrorKind::NotFound
                ),
                "unexpected spawn result: {result:?}"
            );
            assert!(!adopted);
            assert!(
                matches!(witness.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
            );
        } else {
            assert!(
                matches!(result, Err(LocalError::LeaseLapsed(_))),
                "unexpected spawn result: {result:?}"
            );
            assert!(!adopted);
            assert!(
                matches!(witness.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
            );
            assert!(
                RecordStore::new(layout.clone())
                    .read(&worker.instance)?
                    .is_some_and(|record| record.pid == Some(pid))
            );
            let fresh = DeadlineScope::try_new()?;
            let store = RecordStore::new(layout);
            assert!(store.mutation(fresh.deadline()).is_ok());
        }
        Ok(())
    }

    fn committed_case(case: CommittedCase) -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::spawner();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(committed_case_inner(case))
    }

    #[test]
    fn dropping_call_cancels_committed_bootstrap_before_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        committed_case(CommittedCase::Drop)
    }

    #[test]
    fn deadline_expiry_cancels_committed_bootstrap_before_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        committed_case(CommittedCase::Expire)
    }

    #[test]
    fn ready_bootstrap_releases_mutation_lock_before_exec() -> Result<(), Box<dyn std::error::Error>>
    {
        committed_case(CommittedCase::AfterReady)
    }

    #[test]
    fn dropping_call_cancels_committed_bootstrap_after_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        committed_case(CommittedCase::DropAfterReady)
    }

    #[test]
    fn deadline_expiry_cancels_committed_bootstrap_after_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        committed_case(CommittedCase::ExpireAfterReady)
    }

    #[test]
    fn missing_launcher_is_reported_before_bootstrap_is_adopted()
    -> Result<(), Box<dyn std::error::Error>> {
        committed_case(CommittedCase::ExecFailure)
    }

    #[cfg(unix)]
    fn cancelled_bootstrap_case(drop_call: bool) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::net::UnixListener;
        use std::time::Duration;

        let _gate = gang_testkit::fork_gate::spawner();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(async {
                let dir = tempfile::tempdir()?;
                let probe = fake_launcher()?;
                let worker = test_worker()?;
                let layout = initialized_layout(dir.path(), &worker)?;
                // Unix socket paths are short; Bazel's test temporary directory may exceed SUN_LEN.
                let sockets = tempfile::Builder::new()
                    .prefix("gang-sock")
                    .tempdir_in("/tmp")?;
                let gate_path = sockets.path().join("bootstrap.sock");
                let listener = UnixListener::bind(&gate_path)?;
                let mut command = WorkerCommand::gang_worker(
                    &probe,
                    Path::new("gang.toml"),
                    &worker,
                    &[],
                    &layout,
                    None,
                );
                command.environment.push((
                    OsString::from("GANG_TEST_BOOTSTRAP_GATE"),
                    gate_path.into_os_string(),
                ));

                let scope = DeadlineScope::try_new()?;
                let clock = scope.clock().clone();
                let deadline = scope.deadline().clone();
                let owner_layout = layout.clone();

                let (completion_tx, mut completion_rx) = tokio::sync::mpsc::unbounded_channel();
                let owner = tokio::task::spawn_blocking(move || {
                    let processes = FleetProcesses::new(WorkerProcesses::new());
                    let result = RecordStore::new(owner_layout)
                        .mutation(&deadline)
                        .and_then(|mutation| processes.spawn(&mutation, &command));
                    let adopted = !processes.0.children.lock().is_empty();
                    let cleanup = if let Ok(pid) = result.as_ref() {
                        let killed = processes.0.kill(*pid).map_err(|error| error.to_string());
                        let removed = processes.0.children.lock().remove(pid);
                        let waited = match removed {
                            Some(mut child) => {
                                child.wait().map(|_| ()).map_err(|error| error.to_string())
                            }
                            None => Err("spawned child missing from process table".to_owned()),
                        };
                        (killed, waited)
                    } else {
                        (Ok(()), Ok(()))
                    };
                    let children_empty = processes.0.children.lock().is_empty();
                    let _ = completion_tx.send((result, adopted, children_empty, cleanup));
                });

                let (mut gate, _) = listener.accept()?;
                let mut pid_bytes = [0; 4];
                gate.read_exact(&mut pid_bytes)?;
                let pid = u32::from_ne_bytes(pid_bytes);
                let record = RecordStore::new(layout.clone()).read(&worker.instance);
                if drop_call {
                    drop(scope);
                } else {
                    clock.advance(Duration::from_secs(1));
                }

                let (completed_without_rescue, report) = if let Ok(report) =
                    tokio::time::timeout(Duration::from_secs(5), completion_rx.recv()).await
                {
                    (true, report)
                } else {
                    // Closing the gate also unblocks the launcher if the rescue write fails.
                    let _ = gate.write_all(b"G");
                    drop(gate);
                    (false, completion_rx.recv().await)
                };
                owner.await?;
                let (result, adopted, children_empty, (killed, waited)) =
                    report.ok_or("bootstrap owner did not report")?;
                killed.map_err(|error| format!("bootstrap cleanup kill failed: {error}"))?;
                waited.map_err(|error| format!("bootstrap cleanup wait failed: {error}"))?;
                assert!(children_empty, "bootstrap owner retained a child");
                assert!(!FileLock::is_held(&layout.lock_path(&worker.instance))?);
                assert_ne!(pid, 0, "bootstrap gate did not identify the child");
                assert!(
                    record?.is_some_and(|record| record.pid.is_none()),
                    "bootstrap published before the gate"
                );
                assert!(
                    completed_without_rescue,
                    "bootstrap did not stop before watchdog rescue"
                );
                assert!(
                    matches!(result, Err(LocalError::LeaseLapsed(_))),
                    "unexpected spawn result: {result:?}"
                );
                assert!(!adopted, "cancelled bootstrap was adopted");
                Ok(())
            })
    }

    #[cfg(unix)]
    #[test]
    fn dropping_call_cancels_bootstrap() -> Result<(), Box<dyn std::error::Error>> {
        cancelled_bootstrap_case(true)
    }

    #[cfg(unix)]
    #[test]
    fn deadline_expiry_cancels_bootstrap() -> Result<(), Box<dyn std::error::Error>> {
        cancelled_bootstrap_case(false)
    }

    #[cfg(unix)]
    #[test]
    fn a_launcher_that_execs_hands_the_lock_and_the_signal_to_the_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::ExitStatusExt;

        let _gate = gang_testkit::fork_gate::spawner();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(async {
                let dir = tempfile::tempdir()?;
                let probe = fake_launcher()?;
                // The launcher and the worker are a Bazel-built probe: no host program runs.
                let worker = test_worker()?;
                let layout = initialized_layout(dir.path(), &worker)?;
                let lock = layout.lock_path(&worker.instance);
                let launcher = [probe.clone().into_os_string()];
                // Unix socket paths are short; Bazel's test temporary directory may exceed SUN_LEN.
                let sockets = tempfile::Builder::new()
                    .prefix("gang-sock")
                    .tempdir_in("/tmp")?;
                let witness_path = sockets.path().join("worker.sock");
                let witness = std::os::unix::net::UnixListener::bind(&witness_path)?;
                let mut command = WorkerCommand::gang_worker(
                    &probe,
                    Path::new("gang.toml"),
                    &worker,
                    &launcher,
                    &layout,
                    None,
                );
                command.environment.push((
                    OsString::from("GANG_TEST_WORKER_WITNESS"),
                    witness_path.into_os_string(),
                ));

                let scope = DeadlineScope::try_new()?;
                let processes = Arc::new(WorkerProcesses::new());
                let pid = spawn_test_worker(
                    processes.clone(),
                    layout.clone(),
                    scope.deadline().clone(),
                    command,
                )
                .await?;
                let (mut worker_witness, _) = witness.accept()?;
                let mut ready = [0];
                worker_witness.read_exact(&mut ready)?;
                let mutation_released = RecordStore::new(layout.clone()).mutation(scope.deadline());
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
                assert_eq!(ready, [1]);
                assert!(
                    mutation_released.is_ok(),
                    "the worker retained the mutation lock"
                );
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
            })
    }

    const KILL_TARGET: gang_testkit::ChildTest =
        gang_testkit::ChildTest::new("processes::tests::kill_target_child");

    /// Environment variable that makes `kill_target_child` block until it is killed.
    const KILL_TARGET_ENV: &str = "GANG_LOCAL_KILL_TARGET";

    /// A worker that re-runs `kill_target_child` of this test binary with `KILL_TARGET_ENV` set.
    fn kill_target_command(dir: &Path) -> Result<WorkerCommand, Box<dyn std::error::Error>> {
        let worker = test_worker()?;
        let layout = initialized_layout(dir, &worker)?;
        Ok(WorkerCommand {
            bootstrap: fake_launcher()?,
            record: layout.record_path(&worker.instance),
            program: std::env::current_exe()?,
            arguments: KILL_TARGET
                .arguments()
                .into_iter()
                .map(OsString::from)
                .collect(),
            environment: vec![(OsString::from(KILL_TARGET_ENV), OsString::from("1"))],
            lifetime_lock: layout.lock_path(&worker.instance),
            log: layout.log_path(&worker.instance),
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
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(async {
                let dir = tempfile::tempdir()?;
                let command = kill_target_command(dir.path())?;

                let scope = DeadlineScope::try_new()?;
                let processes = Arc::new(WorkerProcesses::new());
                let layout = StateLayout::new(dir.path().join("state"));
                let pid =
                    spawn_test_worker(processes.clone(), layout, scope.deadline().clone(), command)
                        .await?;
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
            })
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
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(async {
                let dir = tempfile::tempdir()?;
                let command = kill_target_command(dir.path())?;

                let scope = DeadlineScope::try_new()?;
                let processes = Arc::new(WorkerProcesses::new());
                let layout = StateLayout::new(dir.path().join("state"));
                let pid =
                    spawn_test_worker(processes.clone(), layout, scope.deadline().clone(), command)
                        .await?;
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
            })
    }
}
