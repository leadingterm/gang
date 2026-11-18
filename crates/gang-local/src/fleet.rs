//! The local fleet. Instances are worker processes; the record files and the
//! desired-capacity files persist across server restarts. Spawn failures are kept
//! in memory until a launch succeeds or the request is met.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::OnceLock;

use gang_clock::Clock;
use gang_spi::{Deadline, FleetInstance, FleetSupply, LaunchFailure};
use gang_types::{Fleet, FleetName, InstanceId, InstanceLifecycle, NonZeroDuration};
use parking_lot::Mutex;

use crate::LocalFleetOptions;
use crate::errors::LocalError;
use crate::layout::StateLayout;
use crate::processes::{
    FleetProcesses, Processes, WorkerCommand, WorkerEndpoints, exit_reason, failure_record,
};
use crate::records::{InstanceRecord, Mutation, RecordStore};
use crate::worker_identity::{InstanceSecret, WorkerIdentity};

/// What a launched worker runs: the program, the config file it is started with, and the launcher argv prefix.
#[derive(Debug, Clone)]
pub(crate) struct LaunchSpec {
    /// Binary that answers `worker --config <path>`.
    pub program: PathBuf,
    /// Config file every worker is started with.
    pub config_path: PathBuf,
    /// Argv prefix that wraps the worker command; empty runs the worker directly.
    pub launcher: Vec<OsString>,
}

/// Local fleet operations over the record store and a process table.
pub(crate) struct LocalFleet<P: Processes, C: Clock> {
    records: RecordStore,
    layout: StateLayout,
    launch: LaunchSpec,
    /// The server endpoints every worker launched after attach is started with; set at most once.
    endpoints: OnceLock<WorkerEndpoints>,
    processes: FleetProcesses<P>,
    clock: C,
    /// How long a worker may live after its launch and still count as an early exit when it dies.
    relaunch_backoff: NonZeroDuration,
    /// How long a worker may keep running after SIGTERM before its process group is killed.
    terminate_grace: NonZeroDuration,
    /// The most recent failed spawn for each fleet.
    launch_failures: Mutex<HashMap<FleetName, String>>,
}

impl<P: Processes, C: Clock> LocalFleet<P, C> {
    /// Creates a fleet over the state directory described by `layout`.
    #[must_use]
    pub(crate) fn new(
        layout: StateLayout,
        launch: LaunchSpec,
        processes: P,
        clock: C,
        relaunch_backoff: NonZeroDuration,
        terminate_grace: NonZeroDuration,
    ) -> Self {
        Self {
            records: RecordStore::new(layout.clone()),
            layout,
            launch,
            endpoints: OnceLock::new(),
            processes: FleetProcesses::new(processes),
            clock,
            relaunch_backoff,
            terminate_grace,
            launch_failures: Mutex::new(HashMap::new()),
        }
    }

    /// Records the endpoints for launched workers; equal retries are accepted.
    ///
    /// # Errors
    ///
    /// Returns [`LocalError::AlreadyAttached`] when different endpoints were recorded.
    pub(crate) fn set_server_endpoints(
        &self,
        endpoints: WorkerEndpoints,
    ) -> Result<(), LocalError> {
        match self.endpoints.set(endpoints) {
            Ok(()) => Ok(()),
            Err(endpoints) if self.endpoints.get() == Some(&endpoints) => Ok(()),
            Err(_) => Err(LocalError::AlreadyAttached),
        }
    }

    /// Overwrites `into` with the desired capacity and the live instances of `fleet`.
    ///
    /// Reads only; dead records are omitted and retired by convergence.
    /// The `instances` buffer is cleared and refilled, never replaced; the
    /// order of instances and exits is unspecified.
    pub(crate) fn supply(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), LocalError> {
        into.warm = 0;
        into.abandoned = 0;
        into.instances.clear();
        into.exits.clear();
        for record in self.records.records()? {
            let record = record?;
            if record.fleet == *fleet.name
                && gang_os::FileLock::is_held(&self.layout.lock_path(&record.id))?
            {
                into.instances.push(FleetInstance {
                    lifecycle: if record.is_terminating() {
                        InstanceLifecycle::Terminating
                    } else {
                        InstanceLifecycle::InService
                    },
                    id: record.id.clone(),
                    launched_at: record.launched_at,
                    release_first: false,
                });
            }
        }
        into.desired = self.records.desired(fleet.name)?;
        into.launch_failure =
            self.launch_failures
                .lock()
                .get(fleet.name)
                .map(|reason| LaunchFailure {
                    reason: reason.clone(),
                });
        self.records.fill_exits(fleet.name, &mut into.exits)?;
        Ok(())
    }

    /// Retires dead workers, kills those past their termination grace, and launches up to the
    /// stored desired count.
    pub(crate) fn converge(
        &self,
        deadline: &Deadline,
        fleet: Fleet<'_, LocalFleetOptions>,
    ) -> Result<(), LocalError> {
        let mutation = self.records.mutation(deadline)?;
        self.processes.reap();
        self.reconcile(&mutation, fleet, None)
    }

    /// Persists the requested capacity, clamped to the serving count, and
    /// launches missing workers. Lowering it never stops a worker; scale-in is
    /// done by [`LocalFleet::terminate`].
    pub(crate) fn set_desired_capacity(
        &self,
        deadline: &Deadline,
        fleet: Fleet<'_, LocalFleetOptions>,
        desired: u32,
    ) -> Result<(), LocalError> {
        let mutation = self.records.mutation(deadline)?;
        self.processes.reap();
        self.reconcile(&mutation, fleet, Some(desired))
    }

    /// Drains one instance: marks it terminating and signals its process, or removes it outright
    /// if its lifetime lock is no longer held. A dead instance whose cgroup is still busy stays
    /// marked terminating until a later pass retires it. Desired capacity drops by one unless the
    /// instance was already terminating. An instance with no record, or whose record belongs to
    /// another fleet, is already gone from `fleet` and is left alone.
    pub(crate) fn terminate(
        &self,
        deadline: &Deadline,
        fleet: Fleet<'_, LocalFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), LocalError> {
        let mutation = self.records.mutation(deadline)?;
        self.records.recover_release(&mutation, fleet.name)?;
        let record = match self.records.read(instance)? {
            Some(record) if record.fleet == *fleet.name => record,
            _ => return Ok(()),
        };
        let already_terminating = record.is_terminating();
        let record = InstanceRecord {
            terminating_since: Some(
                record
                    .terminating_since
                    .unwrap_or_else(|| self.clock.wall()),
            ),
            ..record
        };
        if !already_terminating {
            self.records.release(&mutation, &record)?;
        }
        if gang_os::FileLock::is_held(&self.layout.lock_path(&record.id))? {
            if let Some(pid) = record.pid {
                self.processes.terminate(&mutation, pid)?;
            }
            self.records.write(&mutation, &record)?;
        } else if !self.retire(&mutation, &record)? {
            self.records.write(&mutation, &record)?;
        }
        Ok(())
    }

    /// Acknowledges a durably recorded exit, including already acknowledged exits.
    pub(crate) fn acknowledge_exit(
        &self,
        deadline: &Deadline,
        fleet: Fleet<'_, LocalFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), LocalError> {
        let mutation = self.records.mutation(deadline)?;
        self.records
            .acknowledge_exit(&mutation, fleet.name, instance)
    }

    /// The record store backing this fleet, shared with the plugin's IMDS and preemption paths.
    #[must_use]
    pub(crate) fn records(&self) -> &RecordStore {
        &self.records
    }

    /// Removes a dead worker's cgroup and record, or leaves both for a later pass while the
    /// cgroup is busy. On a host without cgroup v2 the server has no cgroup and no worker records
    /// one, so only the record is removed. Otherwise the cgroup is the one the worker recorded once
    /// it was identified: the scope its launcher created, never a leaf inside it. A cgroup that is
    /// or holds the original launcher's or current caller's cgroup is never reaped. A worker that
    /// recorded none died before it was identified; actions start only after registration, which
    /// follows the recording, so it ran none and there is nothing to reap.
    fn retire(&self, mutation: &Mutation, record: &InstanceRecord) -> Result<bool, LocalError> {
        if let Some(caller) = self.processes.server_cgroup()? {
            match self.records.worker_cgroup(&record.id)? {
                None => tracing::warn!(
                    instance = %record.id,
                    "the worker died before it was identified; it ran no action"
                ),
                Some(dir) => {
                    if let Some(launch) = &record.launch_cgroup {
                        if !launch.starts_with(&dir)
                            && !caller.starts_with(&dir)
                            && matches!(
                                self.processes.reap_cgroup(mutation, &dir)?,
                                gang_os::cgroup::Removal::Busy
                            )
                        {
                            return Ok(false);
                        }
                    } else {
                        tracing::warn!(
                            instance = %record.id,
                            "worker cgroup ownership is unknown without the original launch cgroup; skipping reap"
                        );
                    }
                }
            }
        }
        self.records.remove(mutation, &record.id)?;
        Ok(true)
    }

    /// Logs an early exit and reports its failure record when one exists.
    fn report_exit(
        &self,
        mutation: &Mutation,
        fleet: Fleet<'_, LocalFleetOptions>,
        record: &InstanceRecord,
    ) -> Result<(), LocalError> {
        let path = self.layout.log_path(&record.id);
        let bytes = std::fs::read(&path);
        let log = bytes.as_ref().map(|bytes| String::from_utf8_lossy(bytes));
        let reason = match &log {
            Ok(log) => exit_reason(log).map_or_else(
                || Cow::Owned(format!("its log {} is empty", path.display())),
                Cow::Borrowed,
            ),
            Err(error) => Cow::Owned(format!("its log {} is unreadable: {error}", path.display())),
        };
        tracing::warn!(
            instance = %record.id,
            fleet = %fleet.name,
            reason = %reason,
            "a worker exited within its relaunch backoff of its launch; its fleet relaunches it once the backoff has passed"
        );
        if let Ok(log) = &log
            && let Some(failure) = failure_record(log)
        {
            self.records
                .record_exit(mutation, fleet.name, &record.id, failure)?;
        }
        Ok(())
    }

    /// Reaps the dead records of `fleet`, then launches workers until the live,
    /// non-terminating count reaches the persisted desired capacity. A worker that
    /// died within `relaunch_backoff` of its launch is noted as an early exit, and
    /// no replacement is launched until that backoff has passed since the exit. A launch
    /// that never committed its pid started no worker, so it is retired without an early exit.
    /// A terminating worker that still holds its lock is sent SIGTERM on every pass within
    /// `terminate_grace` of its release, so a release committed before its signal still
    /// reaches the worker; once the grace has passed it is killed on every pass until it is gone.
    fn reconcile(
        &self,
        mutation: &Mutation,
        fleet: Fleet<'_, LocalFleetOptions>,
        requested: Option<u32>,
    ) -> Result<(), LocalError> {
        self.records.recover_release(mutation, fleet.name)?;
        let now = self.clock.wall();
        let mut serving: u32 = 0;
        let mut pending_retirement = false;
        for record in self.records.records()? {
            let mut record = record?;
            if record.fleet != *fleet.name {
                continue;
            }
            if !gang_os::FileLock::is_held(&self.layout.lock_path(&record.id))? {
                let exited_at = if let Some(exited_at) = record.exited_at {
                    exited_at
                } else {
                    record.exited_at = Some(now);
                    self.records.write(mutation, &record)?;
                    now
                };
                let early_exit = record.pid.is_some()
                    && !record.is_terminating()
                    && exited_at.saturating_duration_since(record.launched_at)
                        < self.relaunch_backoff.get();
                if early_exit {
                    self.records
                        .note_early_exit(mutation, fleet.name, exited_at)?;
                    self.report_exit(mutation, fleet, &record)?;
                }
                if !self.retire(mutation, &record)? {
                    pending_retirement = true;
                }
                continue;
            }
            match record.terminating_since {
                Some(since) => {
                    if let Some(pid) = record.pid {
                        if now.saturating_duration_since(since) >= self.terminate_grace.get() {
                            self.processes.kill(mutation, pid)?;
                        } else {
                            self.processes.terminate(mutation, pid)?;
                        }
                    }
                }
                None => serving = serving.saturating_add(1),
            }
        }
        let desired = if let Some(requested) = requested {
            let desired = requested.max(serving);
            self.records.set_desired(mutation, fleet.name, desired)?;
            desired
        } else {
            self.records.desired(fleet.name)?
        };
        if serving >= desired {
            self.launch_failures.lock().remove(fleet.name);
            return Ok(());
        }
        if pending_retirement {
            return Ok(());
        }
        let backing_off = self
            .records
            .early_exit(fleet.name)?
            .is_some_and(|at| now.saturating_duration_since(at) < self.relaunch_backoff.get());
        if backing_off {
            return Ok(());
        }
        for _ in serving..desired {
            if self.spawn_one(mutation, fleet)?.is_none() {
                break;
            }
            self.launch_failures.lock().remove(fleet.name);
        }
        Ok(())
    }

    /// Writes the worker's record before starting it, so mock IMDS can authorize its first request.
    /// The child commits its pid while holding the lifetime lock before spawn returns. A failed
    /// spawn removes the record and records a launch failure.
    fn spawn_one(
        &self,
        mutation: &Mutation,
        fleet: Fleet<'_, LocalFleetOptions>,
    ) -> Result<Option<InstanceRecord>, LocalError> {
        let id = self.records.next_instance(mutation)?;
        let dir = self.layout.instance_dir(&id);
        mutation.check()?;
        gang_os::fs::ensure_private_dir(&dir)?;
        let worker = WorkerIdentity {
            instance: id,
            secret: InstanceSecret::generate()?,
        };
        let command = WorkerCommand::gang_worker(
            &self.launch.program,
            &self.launch.config_path,
            &worker,
            &self.launch.launcher,
            &self.layout,
            self.endpoints.get(),
        );
        let WorkerIdentity { instance, secret } = worker;
        let mut record = InstanceRecord {
            id: instance,
            fleet: fleet.name.clone(),
            isa: fleet.isa,
            pid: None,
            launch_cgroup: self.processes.server_cgroup()?,
            launched_at: self.clock.wall(),
            exited_at: None,
            terminating_since: None,
            secret,
        };
        self.records.write(mutation, &record)?;
        let pid = match self.processes.spawn(mutation, &command) {
            Ok(pid) => pid,
            Err(error @ LocalError::LeaseLapsed(_)) => return Err(error),
            Err(error) => {
                match self.records.remove(mutation, &record.id) {
                    Err(lapsed @ LocalError::LeaseLapsed(_)) => return Err(lapsed),
                    Err(cleanup) => tracing::warn!(
                        instance = %record.id,
                        error = %cleanup,
                        "failed to remove the record of a failed launch"
                    ),
                    Ok(()) => {}
                }
                self.launch_failures
                    .lock()
                    .insert(fleet.name.clone(), error.to_string());
                return Ok(None);
            }
        };
        record.pid = Some(pid);
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::ffi::OsString;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_clock::{Clock, TestClock, Timestamp};
    use gang_os::FileLock;
    use gang_spi::{FleetInstance, FleetSupply, InstanceExit};
    use gang_types::{
        Bytes, Fleet, FleetName, InstanceId, InstanceLifecycle, Isa, NonZeroDuration, Resources,
        Url,
    };
    use parking_lot::Mutex;
    use tempfile::TempDir;
    use tracing_subscriber::fmt::MakeWriter;

    use super::{LaunchSpec, LocalFleet};
    use crate::LocalFleetOptions;
    use crate::errors::LocalError;
    use crate::layout::StateLayout;
    use crate::processes::{
        FleetProcesses, INSTANCE_ENV, INSTANCE_SECRET_ENV, Processes, WorkerCommand,
        WorkerEndpoints, acquire_lifetime_lock,
    };
    use crate::records::{Mutation, RecordStore};

    /// The backoff the tests run with: a worker that dies sooner than this after launch is an early exit.
    const RELAUNCH_BACKOFF: NonZeroDuration = NonZeroDuration::from_secs(30);

    /// How long the tests' terminating workers may run after SIGTERM before they are killed.
    const TERMINATE_GRACE: NonZeroDuration = NonZeroDuration::from_secs(10);

    /// A live authority scope for one operation.
    fn deadline() -> gang_testkit::fence::DeadlineScope {
        gang_testkit::fence::DeadlineScope::try_new().expect("create a live test deadline")
    }

    /// An in-memory tracing sink shared with the test.
    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedBuffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    type SpawnProbe = Box<dyn Fn(&WorkerCommand) + Send + Sync>;
    type Hook = Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>;

    /// A process table shared by clones, so a restarted fleet sees the same processes.
    /// Each fake worker holds its lifetime lock the way a real worker group does,
    /// so [`FakeProcesses::exit`] (which drops the lock) is what makes it dead.
    #[derive(Clone)]
    struct FakeProcesses {
        locks: Arc<Mutex<BTreeMap<u32, FileLock>>>,
        next_pid: Arc<Mutex<u32>>,
        terminated: Arc<Mutex<Vec<u32>>>,
        killed: Arc<Mutex<Vec<u32>>>,
        commands: Arc<Mutex<Vec<WorkerCommand>>>,
        spawn_probe: Arc<Mutex<Option<SpawnProbe>>>,
        after_reap: Hook,
        after_terminate: Hook,
        fail_next_spawn: Arc<Mutex<bool>>,
        obstruct_record: Arc<Mutex<Option<PathBuf>>>,
        /// The cgroup the server runs in.
        server: Arc<Mutex<Option<PathBuf>>>,
        /// Scripted cgroup removal outcomes.
        removals: Arc<Mutex<VecDeque<gang_os::cgroup::Removal>>>,
        /// Cgroups passed to the removal operation.
        reaped_cgroups: Arc<Mutex<Vec<PathBuf>>>,
    }

    impl FakeProcesses {
        fn new() -> Self {
            Self {
                locks: Arc::new(Mutex::new(BTreeMap::new())),
                next_pid: Arc::new(Mutex::new(100)),
                terminated: Arc::new(Mutex::new(Vec::new())),
                killed: Arc::new(Mutex::new(Vec::new())),
                commands: Arc::new(Mutex::new(Vec::new())),
                spawn_probe: Arc::new(Mutex::new(None)),
                after_reap: Arc::new(Mutex::new(None)),
                after_terminate: Arc::new(Mutex::new(None)),
                fail_next_spawn: Arc::new(Mutex::new(false)),
                obstruct_record: Arc::new(Mutex::new(None)),
                server: Arc::new(Mutex::new(Some(PathBuf::from("/fake/cgroup/server")))),
                removals: Arc::new(Mutex::new(VecDeque::new())),
                reaped_cgroups: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Ends the fake worker: dropping its lock file releases the lifetime lock.
        fn exit(&self, pid: u32) {
            self.locks.lock().remove(&pid);
        }

        fn launched(&self) -> usize {
            self.commands.lock().len()
        }
    }

    impl Processes for FakeProcesses {
        fn spawn(&self, mutation: &Mutation, command: &WorkerCommand) -> Result<u32, LocalError> {
            if let Some(probe) = &*self.spawn_probe.lock() {
                probe(command);
            }
            // Like the real table, a permission that lapses during the spawn commits no pid.
            mutation.check()?;
            if std::mem::take(&mut *self.fail_next_spawn.lock()) {
                return Err(LocalError::Io(gang_os::PathIo::new(
                    "spawn worker",
                    &command.program,
                    std::io::Error::other("spawn refused"),
                )));
            }
            if let Some(path) = self.obstruct_record.lock().take() {
                std::fs::remove_file(&path).map_err(|source| {
                    LocalError::Io(gang_os::PathIo::new("remove record", &path, source))
                })?;
                std::fs::create_dir(&path).map_err(|source| {
                    LocalError::Io(gang_os::PathIo::new("obstruct record", &path, source))
                })?;
            }
            let lock = acquire_lifetime_lock(&command.lifetime_lock)?;
            let mut next = self.next_pid.lock();
            let pid = *next;
            *next += 1;
            RecordStore::commit_pid(&command.record, pid)?;
            self.locks.lock().insert(pid, lock);
            self.commands.lock().push(command.clone());
            Ok(pid)
        }

        fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
            Ok(self.server.lock().clone())
        }

        fn reap_cgroup(&self, dir: &Path) -> Result<gang_os::cgroup::Removal, LocalError> {
            self.reaped_cgroups.lock().push(dir.to_path_buf());
            let removal = self
                .removals
                .lock()
                .pop_front()
                .unwrap_or(gang_os::cgroup::Removal::Removed);
            if let Some(hook) = &*self.after_reap.lock() {
                hook();
            }
            Ok(removal)
        }

        fn terminate(&self, pid: u32) -> Result<(), LocalError> {
            self.terminated.lock().push(pid);
            if let Some(hook) = &*self.after_terminate.lock() {
                hook();
            }
            Ok(())
        }

        fn kill(&self, pid: u32) -> Result<(), LocalError> {
            self.killed.lock().push(pid);
            Ok(())
        }

        fn reap(&self) {}
    }

    /// Counts calls reaching the underlying process table.
    struct CountingProcesses(Arc<AtomicUsize>);

    impl Processes for CountingProcesses {
        fn spawn(&self, _: &Mutation, _: &WorkerCommand) -> Result<u32, LocalError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }

        fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
            Ok(None)
        }

        fn reap_cgroup(&self, _: &Path) -> Result<gang_os::cgroup::Removal, LocalError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(gang_os::cgroup::Removal::Removed)
        }

        fn terminate(&self, _: u32) -> Result<(), LocalError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn kill(&self, _: u32) -> Result<(), LocalError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn reap(&self) {}
    }

    #[test]
    fn an_expired_permission_reaches_no_process() {
        let calls = Arc::new(AtomicUsize::new(0));
        let processes = FleetProcesses::new(CountingProcesses(calls.clone()));
        let dir = TempDir::new().unwrap();
        let store = RecordStore::new(layout_in(dir.path()));
        let scope = deadline();
        let mutation = store.mutation(scope.deadline()).unwrap();
        scope.clock().advance(Duration::from_secs(1));
        let command = WorkerCommand {
            program: PathBuf::from("gang"),
            bootstrap: PathBuf::from("gang"),
            record: PathBuf::from("record"),
            arguments: Vec::new(),
            environment: Vec::new(),
            lifetime_lock: PathBuf::from("lock"),
            log: PathBuf::from("log"),
        };

        assert!(matches!(
            processes.spawn(&mutation, &command),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            processes.terminate(&mutation, 1),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            processes.kill(&mutation, 1),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            processes.reap_cgroup(&mutation, Path::new("/fake/cgroup")),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// A subscriber that counts the warnings emitted while it is the thread's default.
    #[derive(Clone, Default)]
    struct Warnings(Arc<AtomicUsize>);

    impl Warnings {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl tracing::Subscriber for Warnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn id(name: &str) -> InstanceId {
        InstanceId::try_new(name.to_owned()).unwrap()
    }

    fn fleet_name(name: &str) -> FleetName {
        FleetName::try_new(name.to_owned()).unwrap()
    }

    /// A view of the fleet `name` running `isa`, with the test instance size and no options.
    fn fleet_view(name: &FleetName, isa: Isa) -> Fleet<'_, LocalFleetOptions> {
        Fleet {
            name,
            isa,
            instance: Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(100 << 30),
            },
            options: &LocalFleetOptions {},
        }
    }

    fn launch_spec() -> LaunchSpec {
        LaunchSpec {
            program: PathBuf::from("gang"),
            config_path: PathBuf::from("gang.toml"),
            launcher: Vec::new(),
        }
    }

    fn layout_in(dir: &Path) -> StateLayout {
        let layout = StateLayout::new(dir.join("state"));
        layout.prepare().unwrap();
        layout
    }

    fn new_manager<'a>(
        layout: &StateLayout,
        processes: FakeProcesses,
        clock: &'a TestClock,
    ) -> LocalFleet<FakeProcesses, &'a TestClock> {
        LocalFleet::new(
            layout.clone(),
            launch_spec(),
            processes,
            clock,
            RELAUNCH_BACKOFF,
            TERMINATE_GRACE,
        )
    }

    fn supply_of<P: Processes, C: gang_clock::Clock>(
        manager: &LocalFleet<P, C>,
        fleet: Fleet<'_, LocalFleetOptions>,
    ) -> FleetSupply {
        let mut supply = FleetSupply::default();
        manager.supply(fleet, &mut supply).unwrap();
        supply.instances.sort_by(|a, b| a.id.cmp(&b.id));
        supply
    }

    fn in_service(name: &str, launched_at: Timestamp) -> FleetInstance {
        FleetInstance {
            id: id(name),
            lifecycle: InstanceLifecycle::InService,
            launched_at,
            release_first: false,
        }
    }

    fn clock() -> TestClock {
        TestClock::new(Timestamp::from_unix_millis(1_000))
    }

    #[cfg(unix)]
    const SPAWN_COMMIT_PARENT: gang_testkit::ChildTest =
        gang_testkit::ChildTest::new("fleet::tests::spawn_commit_parent_child");

    #[cfg(unix)]
    struct PausedSpawnProcesses {
        real: crate::processes::WorkerProcesses,
        ready: Mutex<std::os::unix::net::UnixStream>,
    }

    #[cfg(unix)]
    impl Processes for PausedSpawnProcesses {
        fn spawn(&self, mutation: &Mutation, command: &WorkerCommand) -> Result<u32, LocalError> {
            use std::io::{Read, Write};

            let pid = self.real.spawn(mutation, command)?;
            let mut ready = self.ready.lock();
            ready.write_all(&pid.to_ne_bytes()).unwrap();
            let mut resume = [0];
            ready.read_exact(&mut resume).unwrap();
            Ok(pid)
        }

        fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
            self.real.server_cgroup()
        }

        fn reap_cgroup(&self, dir: &Path) -> Result<gang_os::cgroup::Removal, LocalError> {
            self.real.reap_cgroup(dir)
        }

        fn terminate(&self, pid: u32) -> Result<(), LocalError> {
            self.real.terminate(pid)
        }

        fn kill(&self, pid: u32) -> Result<(), LocalError> {
            self.real.kill(pid)
        }

        fn reap(&self) {
            self.real.reap();
        }
    }

    #[cfg(unix)]
    fn real_probe_launch_spec() -> LaunchSpec {
        LaunchSpec {
            program: std::env::current_dir().unwrap().join(
                std::env::var_os("FAKE_LAUNCHER")
                    .expect("FAKE_LAUNCHER must be set by the Bazel test_env"),
            ),
            ..launch_spec()
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "child process helper for the spawn commit crash regression"]
    fn spawn_commit_parent_child() {
        let Some(root) = std::env::var_os("GANG_TEST_SPAWN_ROOT") else {
            return;
        };
        let socket = std::env::var_os("GANG_TEST_PARENT_READY").unwrap();
        let ready = std::os::unix::net::UnixStream::connect(socket).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let layout = StateLayout::new(PathBuf::from(root));
        let manager = LocalFleet::new(
            layout,
            real_probe_launch_spec(),
            PausedSpawnProcesses {
                real: crate::processes::WorkerProcesses::new(),
                ready: Mutex::new(ready),
            },
            clock(),
            RELAUNCH_BACKOFF,
            TERMINATE_GRACE,
        );
        let name = fleet_name("local");
        manager
            .set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 1)
            .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spawned_worker_remains_terminable_after_parent_dies_before_pid_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::AsyncReadExt;
        use tokio::net::UnixListener;
        use tokio::time::{Instant, timeout, timeout_at};

        let _gate = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let dir = TempDir::new()?;
            let layout = layout_in(dir.path());
            let parent_path = dir.path().join("parent.sock");
            let witness_path = dir.path().join("worker.sock");
            let parent_listener = UnixListener::bind(&parent_path)?;
            let witness_listener = UnixListener::bind(&witness_path)?;
            let mut parent = SPAWN_COMMIT_PARENT
                .command()?
                .env("GANG_TEST_SPAWN_ROOT", dir.path().join("state"))
                .env("GANG_TEST_PARENT_READY", &parent_path)
                .env("GANG_TEST_WORKER_WITNESS", &witness_path)
                .spawn()?;

            let mut worker_group = None;
            let ready = timeout_at(Instant::now() + Duration::from_secs(20), async {
                let (mut parent_ready, _) = parent_listener.accept().await?;
                let mut pid = [0; 4];
                parent_ready.read_exact(&mut pid).await?;
                worker_group = gang_os::ProcessGroup::from_pid(u32::from_ne_bytes(pid));
                if worker_group.is_none() {
                    return Err(io::Error::other("invalid spawned worker pid"));
                }
                let (mut witness, _) = witness_listener.accept().await?;
                let mut byte = [0];
                witness.read_exact(&mut byte).await?;
                if byte != [1] {
                    return Err(io::Error::other("invalid worker readiness byte"));
                }
                Ok::<_, io::Error>((parent_ready, witness))
            })
            .await;

            // Reap only the child test runner, never the worker (which it orphaned).
            let parent_killed = parent.kill();
            let parent_waited = parent.wait();
            let mut outcome = None;
            if let Ok(Ok((parent_ready, mut witness))) = ready {
                drop(parent_ready);
                let manager = LocalFleet::new(
                    layout,
                    real_probe_launch_spec(),
                    crate::processes::WorkerProcesses::new(),
                    clock(),
                    RELAUNCH_BACKOFF,
                    TERMINATE_GRACE,
                );
                let name = fleet_name("local");
                let terminated =
                    manager.terminate(deadline().deadline(), fleet_view(&name, Isa::X86_64), &id("local-1"));
                let mut byte = [0];
                let eof = timeout(Duration::from_secs(1), witness.read(&mut byte)).await;
                drop(witness);
                outcome = Some((terminated, eof));
            }
            let cleanup = worker_group.and_then(|group| {
                if matches!(&outcome, Some((_, Ok(Ok(0))))) {
                    group.disarm();
                    None
                } else {
                    let result = group.kill();
                    group.disarm();
                    Some(result)
                }
            });
            drop(parent_listener);
            drop(witness_listener);
            drop(parent);
            parent_killed?;
            parent_waited?;
            if let Some(cleanup) = cleanup {
                cleanup?;
            }
            let (terminated, eof) =
                outcome.ok_or("parent or worker did not become ready in 20 seconds")?;
            assert!(
                terminated.is_ok() && matches!(eof, Ok(Ok(0))),
                "restarted provisioner could not terminate spawned worker: terminate={terminated:?}, witness={eof:?}"
            );
            Ok(())
        })
    }

    #[cfg(unix)]
    #[test]
    fn a_missing_launcher_under_a_live_lease_is_only_a_launch_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let _entered = runtime.enter();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let manager = LocalFleet::new(
            layout,
            LaunchSpec {
                launcher: vec![dir.path().join("missing-launcher").into_os_string()],
                ..real_probe_launch_spec()
            },
            crate::processes::WorkerProcesses::new(),
            clock(),
            RELAUNCH_BACKOFF,
            TERMINATE_GRACE,
        );
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);

        manager.set_desired_capacity(deadline().deadline(), fleet, 1)?;
        manager.converge(deadline().deadline(), fleet)?;

        let supply = supply_of(&manager, fleet);
        assert!(supply.launch_failure.is_some());
        assert!(supply.instances.is_empty());
        assert!(supply.exits.is_empty());
        assert_eq!(manager.records().early_exit(&name)?, None);
        assert!(manager.records().read(&id("local-1"))?.is_none());
        assert!(manager.records().read(&id("local-2"))?.is_none());
        Ok(())
    }

    #[test]
    fn scale_out_launches_the_missing_instances() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();

        let supply = supply_of(&manager, fleet);
        let launched_at = clock.wall();
        assert_eq!(
            supply,
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                launch_failure: None,
                instances: vec![
                    in_service("local-1", launched_at),
                    in_service("local-2", launched_at),
                ],
            }
        );
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn spawned_workers_run_behind_the_configured_launcher() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let launch = LaunchSpec {
            launcher: vec![OsString::from("env"), OsString::from("GANG_TEST=1")],
            ..launch_spec()
        };
        let manager = LocalFleet::new(
            layout.clone(),
            launch,
            processes.clone(),
            &clock,
            RELAUNCH_BACKOFF,
            TERMINATE_GRACE,
        );
        let name = fleet_name("local");

        manager
            .set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 1)
            .unwrap();

        let commands = processes.commands.lock().clone();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].program, PathBuf::from("env"));
        assert_eq!(
            commands[0].arguments,
            vec![
                OsString::from("GANG_TEST=1"),
                OsString::from("gang"),
                OsString::from("worker"),
                OsString::from("--config"),
                OsString::from("gang.toml"),
            ]
        );
    }

    #[test]
    fn workers_launched_with_server_endpoints_get_them_as_flags() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        manager
            .set_server_endpoints(WorkerEndpoints {
                public: Url::parse("https://gang.example:8443").unwrap(),
                worker: Url::parse("http://127.0.0.1:41001").unwrap(),
            })
            .unwrap();
        let name = fleet_name("local");

        manager
            .set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 1)
            .unwrap();

        let commands = processes.commands.lock().clone();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].arguments,
            vec![
                OsString::from("worker"),
                OsString::from("--config"),
                OsString::from("gang.toml"),
                OsString::from("--endpoint"),
                OsString::from("https://gang.example:8443/"),
                OsString::from("--worker-endpoint"),
                OsString::from("http://127.0.0.1:41001/"),
            ]
        );
    }

    #[test]
    fn server_endpoints_accept_equal_retries() -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let clock = clock();
        let manager = new_manager(&layout, FakeProcesses::new(), &clock);
        let endpoints = WorkerEndpoints {
            public: Url::parse("https://gang.example:8443")?,
            worker: Url::parse("http://127.0.0.1:41001")?,
        };
        manager.set_server_endpoints(endpoints.clone())?;
        manager.set_server_endpoints(endpoints.clone())?;
        let other = WorkerEndpoints {
            public: endpoints.public.clone(),
            worker: Url::parse("http://127.0.0.1:41002")?,
        };
        assert!(matches!(
            manager.set_server_endpoints(other),
            Err(LocalError::AlreadyAttached)
        ));
        Ok(())
    }

    #[test]
    fn set_desired_capacity_is_idempotent() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();

        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn set_desired_capacity_withdraws_unborn_without_stopping_workers() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        *processes.fail_next_spawn.lock() = true;
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 4)
            .unwrap();
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        assert_eq!(manager.records().desired(&name).unwrap(), 1);
        assert!(processes.terminated.lock().is_empty());
        assert_eq!(processes.launched(), 1);
        assert_eq!(supply_of(&manager, fleet).instances.len(), 1);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        assert_eq!(processes.launched(), 2);
        assert_eq!(supply_of(&manager, fleet).desired, 2);
        assert!(processes.terminated.lock().is_empty());
    }

    #[test]
    fn a_restarted_fleet_sees_the_same_instances() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        let before = {
            let manager = new_manager(&layout, processes.clone(), &clock);
            manager
                .set_desired_capacity(deadline().deadline(), fleet, 2)
                .unwrap();
            supply_of(&manager, fleet)
        };

        let restarted = new_manager(&layout, processes.clone(), &clock);

        assert_eq!(supply_of(&restarted, fleet), before);
        restarted
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn dead_processes_are_reaped_and_replaced() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        clock.advance(Duration::from_secs(31));
        let dead = id("local-1");
        let dead_pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();

        processes.exit(dead_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        let ids: Vec<&InstanceId> = supply.instances.iter().map(|i| &i.id).collect();
        assert_eq!(supply.desired, 2);
        assert_eq!(ids, [&id("local-2"), &id("local-3")]);
        assert!(!layout.instance_dir(&dead).exists());
        assert_eq!(processes.launched(), 3);

        assert_eq!(supply_of(&manager, fleet), supply);
        assert_eq!(processes.launched(), 3);
    }

    #[test]
    fn a_lapse_while_a_dead_worker_is_reaped_changes_nothing_more() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/w1"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);
        let lapse_scope = deadline();
        let lapsing = lapse_scope.deadline();
        let lapse_clock = Arc::clone(lapse_scope.clock());
        *processes.after_reap.lock() = Some(Box::new(move || {
            lapse_clock.advance(Duration::from_secs(1));
        }));

        assert!(manager.set_desired_capacity(lapsing, fleet, 3).is_err());
        assert!(
            manager.records().read(&dead).unwrap().is_some(),
            "the dead record was removed after the permission lapsed"
        );
        assert_eq!(
            manager.records().desired(&name).unwrap(),
            1,
            "the desired count was written after the permission lapsed"
        );
        assert_eq!(processes.launched(), 1);
    }

    #[test]
    fn a_lapse_while_an_early_exit_is_reaped_preserves_recorded_exit() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/w1"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);
        let lapse_scope = deadline();
        let lapsing = lapse_scope.deadline();
        let lapse_clock = Arc::clone(lapse_scope.clock());
        let store = RecordStore::new(layout.clone());
        let callback_name = name.clone();
        let callback_dead = dead.clone();
        let snapshot = Arc::new(Mutex::new(None));
        let callback_snapshot = Arc::clone(&snapshot);
        *processes.after_reap.lock() = Some(Box::new(move || {
            *callback_snapshot.lock() = Some((
                store.early_exit(&callback_name).unwrap(),
                store.read(&callback_dead).unwrap(),
                store.desired(&callback_name).unwrap(),
            ));
            lapse_clock.advance(Duration::from_secs(1));
        }));

        assert!(manager.converge(lapsing, fleet).is_err());
        let (early_exit, recorded, desired) = snapshot.lock().take().unwrap();
        assert_eq!(early_exit, Some(clock.wall()));
        let recorded = recorded.unwrap();
        let after = manager.records().read(&dead).unwrap().unwrap();
        assert_eq!(manager.records().early_exit(&name).unwrap(), early_exit);
        assert_eq!(manager.records().desired(&name).unwrap(), desired);
        assert_eq!(after.id, recorded.id);
        assert_eq!(after.fleet, recorded.fleet);
        assert_eq!(after.isa, recorded.isa);
        assert_eq!(after.pid, recorded.pid);
        assert_eq!(after.launch_cgroup, recorded.launch_cgroup);
        assert_eq!(after.launched_at, recorded.launched_at);
        assert_eq!(after.exited_at, recorded.exited_at);
        assert_eq!(after.terminating_since, recorded.terminating_since);
        assert!(after.secret.matches(recorded.secret.expose()));
    }

    #[test]
    fn a_lapse_while_a_worker_is_signalled_preserves_committed_release() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");
        let lapse_scope = deadline();
        let lapsing = lapse_scope.deadline();
        let lapse_clock = Arc::clone(lapse_scope.clock());
        let store = RecordStore::new(layout.clone());
        let callback_name = name.clone();
        let callback_target = target.clone();
        let snapshot = Arc::new(Mutex::new(None));
        let callback_snapshot = Arc::clone(&snapshot);
        *processes.after_terminate.lock() = Some(Box::new(move || {
            *callback_snapshot.lock() = Some((
                store.read(&callback_target).unwrap(),
                store.desired(&callback_name).unwrap(),
            ));
            lapse_clock.advance(Duration::from_secs(1));
        }));

        assert!(manager.terminate(lapsing, fleet, &target).is_err());
        let (recorded, desired) = snapshot.lock().take().unwrap();
        let recorded = recorded.unwrap();
        assert!(recorded.is_terminating());
        assert_eq!(desired, 1);
        let after = manager.records().read(&target).unwrap().unwrap();
        assert_eq!(manager.records().desired(&name).unwrap(), desired);
        assert_eq!(after.id, recorded.id);
        assert_eq!(after.fleet, recorded.fleet);
        assert_eq!(after.isa, recorded.isa);
        assert_eq!(after.pid, recorded.pid);
        assert_eq!(after.launch_cgroup, recorded.launch_cgroup);
        assert_eq!(after.launched_at, recorded.launched_at);
        assert_eq!(after.exited_at, recorded.exited_at);
        assert_eq!(after.terminating_since, recorded.terminating_since);
        assert!(after.secret.matches(recorded.secret.expose()));
    }

    #[test]
    fn a_launch_lapsed_before_its_pid_commit_is_replaced_at_once_without_an_early_exit()
    -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        let lapse_scope = deadline();
        let lapse_clock = Arc::clone(lapse_scope.clock());
        *processes.spawn_probe.lock() = Some(Box::new(move |_: &WorkerCommand| {
            lapse_clock.advance(Duration::from_secs(1));
        }));
        assert!(matches!(
            manager.set_desired_capacity(lapse_scope.deadline(), fleet, 1),
            Err(LocalError::LeaseLapsed(_))
        ));
        *processes.spawn_probe.lock() = None;
        let interrupted = id("local-1");
        assert!(
            manager
                .records()
                .read(&interrupted)?
                .is_some_and(|record| record.pid.is_none())
        );
        // The interrupted bootstrap left a failure line in the instance log.
        std::fs::write(
            layout.log_path(&interrupted),
            "  fail  worker bootstrap interrupted\n",
        )?;

        let successor = new_manager(&layout, processes.clone(), &clock);
        successor.converge(deadline().deadline(), fleet)?;

        let supply = supply_of(&successor, fleet);
        assert_eq!(supply.instances, [in_service("local-2", clock.wall())]);
        assert!(supply.exits.is_empty());
        assert_eq!(successor.records().early_exit(&name)?, None);
        assert!(successor.records().read(&interrupted)?.is_none());
        Ok(())
    }

    #[test]
    fn a_release_committed_before_its_sigterm_is_signalled_by_the_next_pass_within_the_grace()
    -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(deadline().deadline(), fleet, 2)?;
        let target = id("local-1");
        let record = manager
            .records()
            .read(&target)?
            .ok_or("no record for local-1")?;
        let target_pid = record.pid.ok_or("worker has no pid")?;
        // The durable release `terminate` commits, then the lease lapses before the SIGTERM.
        let lapse_scope = deadline();
        let mutation = manager.records().mutation(lapse_scope.deadline())?;
        manager.records().release(
            &mutation,
            &crate::records::InstanceRecord {
                terminating_since: Some(clock.wall()),
                ..record
            },
        )?;
        lapse_scope.clock().advance(Duration::from_secs(1));
        drop(mutation);
        assert!(processes.terminated.lock().is_empty());

        let successor = new_manager(&layout, processes.clone(), &clock);
        clock.advance(
            TERMINATE_GRACE
                .get()
                .saturating_sub(Duration::from_millis(1)),
        );
        successor.converge(deadline().deadline(), fleet)?;

        assert_eq!(processes.terminated.lock().as_slice(), [target_pid]);
        assert!(processes.killed.lock().is_empty());
        assert_eq!(successor.records().desired(&name)?, 1);
        assert_eq!(processes.launched(), 2);
        Ok(())
    }
    #[test]
    fn a_dead_worker_is_reaped_by_the_scope_it_recorded() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let scope = PathBuf::from("/fake/cgroup/run-1.scope");
        manager
            .records()
            .write_worker_cgroup(&dead, &scope)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert_eq!(*processes.reaped_cgroups.lock(), vec![scope]);
    }

    #[test]
    fn a_dead_worker_is_not_replaced_while_its_recorded_scope_stays_populated() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        processes.removals.lock().extend([
            gang_os::cgroup::Removal::Busy,
            gang_os::cgroup::Removal::Removed,
        ]);
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let scope = PathBuf::from("/fake/cgroup/run-1.scope");
        manager
            .records()
            .write_worker_cgroup(&dead, &scope)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let first = supply_of(&manager, fleet);
        assert!(first.instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert!(manager.records().read(&dead).unwrap().is_some());

        manager.converge(deadline().deadline(), fleet).unwrap();
        let second = supply_of(&manager, fleet);
        assert_eq!(second.instances.len(), 1);
        assert_eq!(second.instances[0].id, id("local-2"));
        assert_eq!(processes.launched(), 2);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert_eq!(*processes.reaped_cgroups.lock(), vec![scope.clone(), scope]);
    }

    #[test]
    fn a_worker_that_died_before_recording_its_cgroup_is_replaced_without_a_reap() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = LocalFleet::new(
            layout.clone(),
            LaunchSpec {
                launcher: vec![OsString::from("launcher")],
                ..launch_spec()
            },
            processes.clone(),
            &clock,
            RELAUNCH_BACKOFF,
            TERMINATE_GRACE,
        );
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let warnings = Warnings::default();
        let supply = gang_testkit::with_subscriber(warnings.clone(), || {
            manager.converge(deadline().deadline(), fleet).unwrap();
            supply_of(&manager, fleet)
        });
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert_eq!(processes.launched(), 2);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(processes.reaped_cgroups.lock().is_empty());
        assert_eq!(warnings.count(), 1);
    }

    #[test]
    fn a_dead_worker_on_a_host_without_cgroups_is_replaced_without_a_warning() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        *processes.server.lock() = None;
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let warnings = Warnings::default();
        let supply = gang_testkit::with_subscriber(warnings.clone(), || {
            manager.converge(deadline().deadline(), fleet).unwrap();
            supply_of(&manager, fleet)
        });

        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(processes.reaped_cgroups.lock().is_empty());
        assert_eq!(warnings.count(), 0);
    }

    #[test]
    fn a_recorded_cgroup_that_is_the_servers_is_never_reaped() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/server"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let warnings = Warnings::default();
        let supply = gang_testkit::with_subscriber(warnings.clone(), || {
            manager.converge(deadline().deadline(), fleet).unwrap();
            supply_of(&manager, fleet)
        });
        assert_eq!(warnings.count(), 0);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert_eq!(processes.launched(), 2);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(processes.reaped_cgroups.lock().is_empty());
    }

    #[test]
    fn a_later_caller_never_reaps_the_launchers_shared_cgroup() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/server"))
            .unwrap();
        let record = manager.records().read(&dead).unwrap().unwrap();
        assert_eq!(
            record.launch_cgroup.as_deref(),
            Some(Path::new("/fake/cgroup/server"))
        );
        let pid = record.pid.unwrap();
        clock.advance(Duration::from_secs(31));
        *processes.server.lock() = Some(PathBuf::from("/other/session"));
        drop(manager);
        let manager = new_manager(&layout, processes.clone(), &clock);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &dead)
            .unwrap();
        assert_eq!(
            manager
                .records()
                .read(&dead)
                .unwrap()
                .unwrap()
                .launch_cgroup,
            record.launch_cgroup
        );
        assert_eq!(processes.terminated.lock().as_slice(), [pid]);
        processes.exit(pid);
        let warnings = Warnings::default();
        gang_testkit::with_subscriber(warnings.clone(), || {
            manager
                .set_desired_capacity(deadline().deadline(), fleet, 0)
                .unwrap();
        });
        assert_eq!(warnings.count(), 0);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(supply_of(&manager, fleet).instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert!(processes.reaped_cgroups.lock().is_empty());
    }

    #[test]
    fn a_later_caller_never_reaps_its_own_cgroup_or_an_ancestor() {
        let _gate = gang_testkit::fork_gate::lock_user();
        for scope in ["/other/session/sub", "/other/session"] {
            let dir = TempDir::new().unwrap();
            let layout = layout_in(dir.path());
            let processes = FakeProcesses::new();
            let clock = clock();
            let manager = new_manager(&layout, processes.clone(), &clock);
            let name = fleet_name("local");
            let fleet = fleet_view(&name, Isa::X86_64);
            manager
                .set_desired_capacity(deadline().deadline(), fleet, 1)
                .unwrap();
            let dead = id("local-1");
            manager
                .records()
                .write_worker_cgroup(&dead, Path::new(scope))
                .unwrap();
            let record = manager.records().read(&dead).unwrap().unwrap();
            assert_eq!(
                record.launch_cgroup.as_deref(),
                Some(Path::new("/fake/cgroup/server"))
            );
            let pid = record.pid.unwrap();
            clock.advance(Duration::from_secs(31));
            processes.exit(pid);
            *processes.server.lock() = Some(PathBuf::from("/other/session/sub"));
            drop(manager);
            let manager = new_manager(&layout, processes.clone(), &clock);

            manager
                .set_desired_capacity(deadline().deadline(), fleet, 0)
                .unwrap();
            assert!(manager.records().read(&dead).unwrap().is_none());
            assert!(supply_of(&manager, fleet).instances.is_empty());
            assert!(processes.reaped_cgroups.lock().is_empty());
        }
    }

    #[test]
    fn a_later_caller_never_reaps_an_ancestor_of_the_launchers_cgroup() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);
        *processes.server.lock() = Some(PathBuf::from("/other/session"));
        drop(manager);
        let manager = new_manager(&layout, processes.clone(), &clock);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(supply_of(&manager, fleet).instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert!(processes.reaped_cgroups.lock().is_empty());
    }

    #[test]
    fn a_later_caller_reaps_a_private_worker_cgroup() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let scope = PathBuf::from("/fake/cgroup/worker.scope");
        manager
            .records()
            .write_worker_cgroup(&dead, &scope)
            .unwrap();
        let record = manager.records().read(&dead).unwrap().unwrap();
        assert_eq!(
            record.launch_cgroup.as_deref(),
            Some(Path::new("/fake/cgroup/server"))
        );
        let pid = record.pid.unwrap();
        clock.advance(Duration::from_secs(31));
        *processes.server.lock() = Some(PathBuf::from("/other/session"));
        drop(manager);
        let manager = new_manager(&layout, processes.clone(), &clock);

        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &dead)
            .unwrap();
        assert_eq!(
            manager
                .records()
                .read(&dead)
                .unwrap()
                .unwrap()
                .launch_cgroup,
            record.launch_cgroup
        );
        assert_eq!(processes.terminated.lock().as_slice(), [pid]);
        processes.exit(pid);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(supply_of(&manager, fleet).instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert_eq!(*processes.reaped_cgroups.lock(), vec![scope]);
    }

    #[test]
    fn a_later_caller_never_reaps_a_legacy_record_without_launch_ownership() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/worker.scope"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        let path = layout.record_path(&dead);
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record.as_object_mut().unwrap().remove("launch_cgroup");
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);
        *processes.server.lock() = Some(PathBuf::from("/other/session"));
        drop(manager);
        let manager = new_manager(&layout, processes.clone(), &clock);

        let warnings = Warnings::default();
        gang_testkit::with_subscriber(warnings.clone(), || {
            manager
                .set_desired_capacity(deadline().deadline(), fleet, 0)
                .unwrap();
        });
        assert_eq!(warnings.count(), 1);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(supply_of(&manager, fleet).instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert!(processes.reaped_cgroups.lock().is_empty());
    }

    #[test]
    fn a_recorded_cgroup_above_the_server_is_never_reaped() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert_eq!(processes.launched(), 2);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert!(processes.reaped_cgroups.lock().is_empty());
    }

    #[test]
    fn terminating_a_dead_worker_retries_a_busy_cgroup() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let cgroup = PathBuf::from("/fake/cgroup/w1");
        processes.removals.lock().extend([
            gang_os::cgroup::Removal::Busy,
            gang_os::cgroup::Removal::Removed,
        ]);
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, &cgroup)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);

        manager
            .terminate(deadline().deadline(), fleet, &dead)
            .unwrap();
        assert!(manager.records().read(&dead).unwrap().is_some());
        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(supply_of(&manager, fleet).desired, 0);
        assert!(manager.records().read(&dead).unwrap().is_none());
        assert_eq!(
            *processes.reaped_cgroups.lock(),
            vec![cgroup.clone(), cgroup]
        );
        assert_eq!(processes.launched(), 1);
    }

    #[test]
    fn a_dead_worker_with_a_busy_cgroup_stays_terminating_until_it_is_retired() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let cgroup = PathBuf::from("/fake/cgroup/w1");
        processes.removals.lock().extend([
            gang_os::cgroup::Removal::Busy,
            gang_os::cgroup::Removal::Busy,
            gang_os::cgroup::Removal::Removed,
        ]);
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, &cgroup)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);

        manager
            .terminate(deadline().deadline(), fleet, &dead)
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &dead)
            .unwrap();

        assert!(
            manager
                .records()
                .read(&dead)
                .unwrap()
                .unwrap()
                .is_terminating()
        );
        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.desired, 1);
        assert_eq!(supply.instances, vec![in_service("local-2", clock.wall())]);
        assert_eq!(manager.records().early_exit(&name).unwrap(), None);
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn terminate_marks_then_reaps_and_lowers_desired() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();

        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();

        assert_eq!(processes.terminated.lock().as_slice(), [target_pid]);
        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![
                    FleetInstance {
                        id: target.clone(),
                        lifecycle: InstanceLifecycle::Terminating,
                        launched_at,
                        release_first: false,
                    },
                    in_service("local-2", launched_at),
                ],
            }
        );

        processes.exit(target_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![in_service("local-2", launched_at)],
            }
        );
    }

    #[test]
    fn terminating_a_missing_instance_is_a_no_op() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");

        manager
            .terminate(
                deadline().deadline(),
                fleet_view(&name, Isa::X86_64),
                &id("local-9"),
            )
            .unwrap();

        assert!(processes.terminated.lock().is_empty());
    }

    #[test]
    fn terminating_twice_succeeds_and_lowers_desired_once() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");

        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();

        assert_eq!(supply_of(&manager, fleet).desired, 1);
    }

    #[test]
    fn retrying_termination_after_desired_read_failure_commits_release_across_restart() {
        let _gate = gang_testkit::fork_gate::lock_user();
        for worker_exited in [false, true] {
            let dir = TempDir::new().unwrap();
            let layout = layout_in(dir.path());
            let processes = FakeProcesses::new();
            let clock = clock();
            let manager = new_manager(&layout, processes.clone(), &clock);
            let name = fleet_name("local");
            let fleet = fleet_view(&name, Isa::X86_64);
            manager
                .set_desired_capacity(deadline().deadline(), fleet, 1)
                .unwrap();
            let target = id("local-1");
            let pid = manager
                .records()
                .read(&target)
                .unwrap()
                .unwrap()
                .pid
                .unwrap();
            if worker_exited {
                processes.exit(pid);
            }

            std::fs::write(layout.desired_path(&name), "invalid decimal").unwrap();
            assert!(
                manager
                    .terminate(deadline().deadline(), fleet, &target)
                    .is_err()
            );

            manager
                .records()
                .set_desired(
                    &manager.records().mutation(deadline().deadline()).unwrap(),
                    &name,
                    1,
                )
                .unwrap();
            drop(manager);
            let restarted = new_manager(&layout, processes.clone(), &clock);
            restarted
                .terminate(deadline().deadline(), fleet, &target)
                .unwrap();
            clock.advance(Duration::from_secs(31));
            restarted.converge(deadline().deadline(), fleet).unwrap();
            let supply = supply_of(&restarted, fleet);
            assert_eq!(supply.desired, 0, "worker_exited={worker_exited}");
            assert_eq!(processes.launched(), 1, "worker_exited={worker_exited}");
            if worker_exited {
                assert!(supply.instances.is_empty());
            } else {
                processes.exit(pid);
                restarted.converge(deadline().deadline(), fleet).unwrap();
                let supply = supply_of(&restarted, fleet);
                assert_eq!(supply.desired, 0);
                assert!(supply.instances.is_empty());
                assert_eq!(processes.launched(), 1);
            }
        }
    }

    #[test]
    fn fleets_are_per_name() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let small_name = fleet_name("local-small");
        let big_name = fleet_name("local-big");
        let small = fleet_view(&small_name, Isa::X86_64);
        let big = fleet_view(&big_name, Isa::X86_64);

        manager
            .set_desired_capacity(deadline().deadline(), small, 1)
            .unwrap();
        manager
            .set_desired_capacity(deadline().deadline(), big, 2)
            .unwrap();

        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, small),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![in_service("local-1", launched_at)],
            }
        );
        assert_eq!(
            supply_of(&manager, big),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                launch_failure: None,
                instances: vec![
                    in_service("local-2", launched_at),
                    in_service("local-3", launched_at),
                ],
            }
        );

        let small_before = supply_of(&manager, small);
        let big_before = supply_of(&manager, big);
        manager
            .terminate(deadline().deadline(), big, &id("local-1"))
            .unwrap();

        assert!(processes.terminated.lock().is_empty());
        assert_eq!(supply_of(&manager, small), small_before);
        assert_eq!(supply_of(&manager, big), big_before);

        let target_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), small, &id("local-1"))
            .unwrap();

        assert_eq!(processes.terminated.lock().as_slice(), [target_pid]);
        assert_eq!(
            supply_of(&manager, small),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 0,
                launch_failure: None,
                instances: vec![FleetInstance {
                    id: id("local-1"),
                    lifecycle: InstanceLifecycle::Terminating,
                    launched_at,
                    release_first: false,
                }],
            }
        );
        assert_eq!(
            supply_of(&manager, big),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                launch_failure: None,
                instances: vec![
                    in_service("local-2", launched_at),
                    in_service("local-3", launched_at),
                ],
            }
        );
    }

    #[test]
    fn fleets_with_different_isas_are_independent() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let x86_name = fleet_name("local-x86");
        let arm_name = fleet_name("local-arm");
        let x86 = fleet_view(&x86_name, Isa::X86_64);
        let arm = fleet_view(&arm_name, Isa::Aarch64);

        manager
            .set_desired_capacity(deadline().deadline(), x86, 1)
            .unwrap();
        manager
            .set_desired_capacity(deadline().deadline(), arm, 2)
            .unwrap();

        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, x86),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![in_service("local-1", launched_at)],
            }
        );
        assert_eq!(
            supply_of(&manager, arm),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                launch_failure: None,
                instances: vec![
                    in_service("local-2", launched_at),
                    in_service("local-3", launched_at),
                ],
            }
        );
        let x86_before = supply_of(&manager, x86);
        let arm_before = supply_of(&manager, arm);
        manager
            .terminate(deadline().deadline(), arm, &id("local-1"))
            .unwrap();

        assert!(processes.terminated.lock().is_empty());
        assert_eq!(supply_of(&manager, x86), x86_before);
        assert_eq!(supply_of(&manager, arm), arm_before);
    }

    #[test]
    fn the_instance_record_with_its_secret_exists_when_the_worker_starts() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let store = crate::records::RecordStore::new(layout.clone());
        let observed = Arc::new(Mutex::new(false));
        let observed_at_spawn = observed.clone();
        *processes.spawn_probe.lock() = Some(Box::new(move |command| {
            let instance = command
                .environment
                .iter()
                .find(|(name, _)| name == INSTANCE_ENV)
                .unwrap()
                .1
                .to_str()
                .unwrap();
            let secret = command
                .environment
                .iter()
                .find(|(name, _)| name == INSTANCE_SECRET_ENV)
                .unwrap()
                .1
                .to_str()
                .unwrap();
            let record = store.read(&id(instance)).unwrap().unwrap();
            *observed_at_spawn.lock() = record.pid.is_none() && record.secret.matches(secret);
        }));
        let clock = clock();
        let manager = new_manager(&layout, processes, &clock);
        let name = fleet_name("local");

        manager
            .set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 1)
            .unwrap();

        assert!(*observed.lock());
    }

    #[test]
    fn launch_command_names_the_instance() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");

        manager
            .set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 1)
            .unwrap();

        let launched = processes.commands.lock();
        let command = launched.first().unwrap();
        let instance = id("local-1");
        assert!(
            command
                .environment
                .contains(&(OsString::from(INSTANCE_ENV), OsString::from("local-1")))
        );
        let recorded = manager.records().read(&instance).unwrap().unwrap();
        assert!(command.environment.contains(&(
            OsString::from(INSTANCE_SECRET_ENV),
            OsString::from(recorded.secret.expose())
        )));
        assert_eq!(command.log, layout.log_path(&instance));
        assert!(!command.log.starts_with(layout.instance_dir(&instance)));
        assert_eq!(command.lifetime_lock, layout.lock_path(&instance));
    }

    #[test]
    fn each_instance_gets_its_own_secret() -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");

        manager.set_desired_capacity(deadline().deadline(), fleet_view(&name, Isa::X86_64), 2)?;

        let first = manager
            .records()
            .read(&id("local-1"))?
            .ok_or("no record for local-1")?;
        let second = manager
            .records()
            .read(&id("local-2"))?
            .ok_or("no record for local-2")?;
        assert!(!first.secret.matches(second.secret.expose()));
        Ok(())
    }

    #[test]
    fn converge_replaces_a_dead_worker_without_another_set_desired_capacity() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        clock.advance(Duration::from_secs(31));
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        processes.exit(dead_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);

        assert_eq!(supply.desired, 2);
        let ids: Vec<&InstanceId> = supply.instances.iter().map(|i| &i.id).collect();
        assert_eq!(ids, [&id("local-2"), &id("local-3")]);
        assert_eq!(processes.launched(), 3);
    }

    #[test]
    fn terminating_instances_are_not_replaced_by_converge() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();

        let launched_at = clock.wall();
        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![
                    FleetInstance {
                        id: target.clone(),
                        lifecycle: InstanceLifecycle::Terminating,
                        launched_at,
                        release_first: false,
                    },
                    in_service("local-2", launched_at),
                ],
            }
        );
        assert_eq!(processes.launched(), 2);

        processes.exit(target_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                launch_failure: None,
                instances: vec![in_service("local-2", launched_at)],
            }
        );
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn an_early_exit_logs_the_worker_failure_record() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(
            layout.log_path(&dead),
            "INFO starting worker\n  fail  gang worker: the scratch volume is too small\n",
        )
        .unwrap();
        processes.exit(pid);

        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .finish();
        let supply = gang_testkit::with_subscriber(subscriber, || {
            manager.converge(deadline().deadline(), fleet).unwrap();
            supply_of(&manager, fleet)
        });
        assert_eq!(
            supply.exits,
            vec![InstanceExit {
                instance: dead.clone(),
                failure: "fail  gang worker: the scratch volume is too small".to_owned(),
            }]
        );
        let output = String::from_utf8_lossy(&buffer.0.lock()).into_owned();
        assert!(
            output.contains("fail  gang worker: the scratch volume is too small"),
            "{output}"
        );
        assert!(output.contains(&dead.to_string()), "{output}");
    }

    #[test]
    fn early_exit_survives_supply_and_fleet_reconstruction() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(
            layout.log_path(&dead),
            "INFO starting worker\n  fail  gang worker: the scratch volume is too small\n",
        )
        .unwrap();
        processes.exit(pid);

        let expected = vec![InstanceExit {
            instance: dead.clone(),
            failure: "fail  gang worker: the scratch volume is too small".to_owned(),
        }];
        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(supply_of(&manager, fleet).exits, expected);
        drop(manager);

        let restarted = new_manager(&layout, processes.clone(), &clock);
        assert_eq!(supply_of(&restarted, fleet).exits, expected);
        restarted
            .acknowledge_exit(deadline().deadline(), fleet, &dead)
            .unwrap();
        drop(restarted);
        let reconstructed = new_manager(&layout, processes, &clock);
        assert!(supply_of(&reconstructed, fleet).exits.is_empty());
    }

    #[test]
    fn an_early_exit_without_a_failure_record_is_not_reported() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(layout.log_path(&dead), "INFO worker stopped\n").unwrap();
        processes.exit(pid);
        manager.converge(deadline().deadline(), fleet).unwrap();
        assert!(supply_of(&manager, fleet).exits.is_empty());
    }

    #[test]
    fn an_exit_seen_during_capacity_change_is_reported_by_the_next_supply() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(layout.log_path(&dead), "fail  worker failed\n").unwrap();
        processes.exit(pid);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        assert_eq!(
            supply_of(&manager, fleet).exits,
            vec![InstanceExit {
                instance: dead.clone(),
                failure: "fail  worker failed".to_owned(),
            }]
        );
        manager
            .acknowledge_exit(deadline().deadline(), fleet, &dead)
            .unwrap();
        assert!(supply_of(&manager, fleet).exits.is_empty());
    }

    #[test]
    fn exit_record_failure_does_not_retire_the_worker() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(layout.log_path(&dead), "fail  worker failed\n").unwrap();
        std::fs::create_dir_all(layout.exit_path(&name, &dead)).unwrap();
        processes.exit(pid);

        assert!(manager.converge(deadline().deadline(), fleet).is_err());
        assert!(manager.records().read(&dead).unwrap().is_some());

        drop(manager);
        clock.advance(Duration::from_secs(31));
        let manager = new_manager(&layout, processes.clone(), &clock);

        std::fs::remove_dir(layout.exit_path(&name, &dead)).unwrap();
        manager.converge(deadline().deadline(), fleet).unwrap();
        assert_eq!(
            supply_of(&manager, fleet).exits,
            vec![InstanceExit {
                instance: dead,
                failure: "fail  worker failed".to_owned(),
            }]
        );
    }

    #[test]
    fn an_instance_that_dies_at_start_is_not_relaunched_within_the_backoff() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();

        processes.exit(dead_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.desired, 1);
        assert_eq!(supply.instances.len(), 0);
        assert_eq!(processes.launched(), 1);
        assert_eq!(
            manager.records().early_exit(&name).unwrap(),
            Some(clock.wall())
        );
    }

    #[test]
    fn it_is_relaunched_once_the_backoff_has_passed() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        processes.exit(dead_pid);
        manager.converge(deadline().deadline(), fleet).unwrap();
        supply_of(&manager, fleet);
        assert_eq!(processes.launched(), 1);

        clock.advance(RELAUNCH_BACKOFF.get());

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        let ids: Vec<&InstanceId> = supply.instances.iter().map(|i| &i.id).collect();
        assert_eq!(ids, [&id("local-2")]);
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn an_instance_that_lived_longer_than_the_backoff_is_replaced_at_once() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        clock.advance(RELAUNCH_BACKOFF.get() + Duration::from_secs(1));

        processes.exit(dead_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        let ids: Vec<&InstanceId> = supply.instances.iter().map(|i| &i.id).collect();
        assert_eq!(ids, [&id("local-2")]);
        assert_eq!(processes.launched(), 2);
        assert_eq!(manager.records().early_exit(&name).unwrap(), None);
    }

    #[test]
    fn a_terminating_instance_that_exits_is_not_an_early_exit() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();

        processes.exit(target_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.desired, 0);
        assert_eq!(supply.instances.len(), 0);
        assert_eq!(manager.records().early_exit(&name).unwrap(), None);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn the_log_of_a_reaped_instance_is_kept() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let instance = id("local-1");
        let dead_pid = manager
            .records()
            .read(&instance)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        let log = layout.log_path(&instance);
        std::fs::write(&log, "worker output").unwrap();

        processes.exit(dead_pid);
        manager.converge(deadline().deadline(), fleet).unwrap();
        supply_of(&manager, fleet);

        assert!(!layout.instance_dir(&instance).exists());
        assert_eq!(std::fs::read_to_string(&log).unwrap(), "worker output");
    }

    #[test]
    fn a_terminating_worker_is_killed_once_the_grace_has_passed() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();

        clock.advance(
            TERMINATE_GRACE
                .get()
                .saturating_sub(Duration::from_millis(1)),
        );
        manager.converge(deadline().deadline(), fleet).unwrap();
        supply_of(&manager, fleet);
        assert!(processes.killed.lock().is_empty());

        clock.advance(Duration::from_millis(1));
        manager.converge(deadline().deadline(), fleet).unwrap();
        supply_of(&manager, fleet);
        assert_eq!(processes.killed.lock().as_slice(), [target_pid]);
    }

    #[test]
    fn a_terminating_worker_that_exited_is_reaped_and_never_killed() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        clock.advance(TERMINATE_GRACE.get() + Duration::from_secs(1));

        processes.exit(target_pid);

        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        assert!(supply.instances.is_empty());
        assert!(processes.killed.lock().is_empty());
        assert!(manager.records().read(&target).unwrap().is_none());
    }

    #[test]
    fn terminating_again_does_not_restart_the_grace() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 2)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        clock.advance(TERMINATE_GRACE.get() / 2);
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        clock.advance(TERMINATE_GRACE.get() / 2);

        manager.converge(deadline().deadline(), fleet).unwrap();
        supply_of(&manager, fleet);

        assert_eq!(processes.killed.lock().as_slice(), [target_pid]);
    }

    #[test]
    fn failed_spawn_is_reported_until_a_successful_spawn() -> Result<(), Box<dyn std::error::Error>>
    {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        let instance = id("local-1");
        *processes.fail_next_spawn.lock() = true;
        manager.set_desired_capacity(deadline().deadline(), fleet, 1)?;
        assert!(!layout.instance_dir(&instance).exists());
        assert!(manager.records().read(&instance)?.is_none());

        *processes.fail_next_spawn.lock() = true;
        manager.converge(deadline().deadline(), fleet).unwrap();
        let supply = supply_of(&manager, fleet);
        let expected = LocalError::Io(gang_os::PathIo::new(
            "spawn worker",
            &launch_spec().program,
            std::io::Error::other("spawn refused"),
        ))
        .to_string();
        assert_eq!(
            supply.launch_failure.as_ref().map(|f| f.reason.as_str()),
            Some(expected.as_str())
        );
        assert!(supply.instances.is_empty());
        let other_name = fleet_name("other");
        assert!(
            supply_of(&manager, fleet_view(&other_name, Isa::X86_64))
                .launch_failure
                .is_none()
        );

        manager.converge(deadline().deadline(), fleet).unwrap();
        let recovered = supply_of(&manager, fleet);
        assert!(recovered.launch_failure.is_none());
        assert_eq!(recovered.instances.len(), 1);
        Ok(())
    }

    #[test]
    fn withdrawing_failed_request_to_zero_clears_failure() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        *processes.fail_next_spawn.lock() = true;
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 0)
            .unwrap();
        assert_eq!(manager.records().desired(&name).unwrap(), 0);
        let supply = supply_of(&manager, fleet);
        assert!(supply.instances.is_empty());
        assert!(supply.launch_failure.is_none());
        assert_eq!(processes.launched(), 0);
        assert!(processes.terminated.lock().is_empty());
    }

    #[test]
    fn a_failed_pid_publication_records_a_launch_failure() -> Result<(), Box<dyn std::error::Error>>
    {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        *processes.obstruct_record.lock() = Some(layout.record_path(&id("local-1")));

        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(deadline().deadline(), fleet, 1)?;
        assert!(manager.launch_failures.lock().contains_key(&name));
        assert!(manager.records().read(&id("local-1"))?.is_none());
        assert_eq!(processes.launched(), 0);
        assert!(processes.locks.lock().is_empty());
        assert!(processes.terminated.lock().is_empty());
        gang_os::fs::ensure_private_dir(&layout.instance_dir(&id("local-1")))?;
        drop(acquire_lifetime_lock(&layout.lock_path(&id("local-1")))?);

        // Convergence retries the launch; keep that attempt from serving a worker.
        *processes.fail_next_spawn.lock() = true;
        manager.converge(deadline().deadline(), fleet)?;
        let supply = supply_of(&manager, fleet);
        assert!(supply.launch_failure.is_some());
        assert!(supply.instances.is_empty());
        Ok(())
    }

    #[test]
    fn a_worker_still_holding_its_lock_is_killed_again_on_every_pass_after_the_grace()
    -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(deadline().deadline(), fleet, 1)?;
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)?
            .ok_or("no record for local-1")?
            .pid
            .ok_or("worker has no pid")?;
        manager.terminate(deadline().deadline(), fleet, &target)?;

        clock.advance(TERMINATE_GRACE.get());
        manager.converge(deadline().deadline(), fleet)?;
        supply_of(&manager, fleet);
        manager.converge(deadline().deadline(), fleet)?;
        supply_of(&manager, fleet);

        assert_eq!(processes.killed.lock().as_slice(), [target_pid, target_pid]);
        Ok(())
    }

    /// Another controller raised the shared desired count; reading the supply launches nothing.
    #[test]
    fn supply_launches_nothing_when_the_shared_desired_count_rises() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        manager
            .records()
            .set_desired(
                &manager.records().mutation(deadline().deadline()).unwrap(),
                &name,
                2,
            )
            .unwrap();

        supply_of(&manager, fleet);

        assert_eq!(
            processes.launched(),
            1,
            "reading the supply launched a worker"
        );
    }

    /// Reading the supply kills no terminating worker, even past its grace.
    #[test]
    fn supply_kills_no_worker_past_its_grace() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let target = id("local-1");
        let _target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        clock.advance(TERMINATE_GRACE.get() + Duration::from_secs(1));

        supply_of(&manager, fleet);

        assert!(
            processes.killed.lock().is_empty(),
            "reading the supply killed a worker"
        );
    }

    /// Reading the supply leaves a dead worker's record for the fleet's owner to retire.
    #[test]
    fn supply_retires_no_dead_worker() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        processes.exit(target_pid);

        supply_of(&manager, fleet);

        assert!(
            manager.records().read(&target).unwrap().is_some(),
            "reading the supply retired a dead worker"
        );
        assert_eq!(
            processes.launched(),
            1,
            "reading the supply relaunched a worker"
        );
    }

    #[test]
    fn converge_launches_up_to_the_shared_desired_count() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        manager
            .records()
            .set_desired(
                &manager.records().mutation(deadline().deadline()).unwrap(),
                &name,
                2,
            )
            .unwrap();

        manager.converge(deadline().deadline(), fleet).unwrap();

        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn an_expired_permission_launches_nothing() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        let expired_scope = deadline();
        expired_scope.clock().advance(Duration::from_secs(1));
        let expired = expired_scope.deadline();

        assert!(matches!(
            manager.set_desired_capacity(expired, fleet, 1),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert_eq!(processes.launched(), 0);
    }

    #[test]
    fn an_expired_permission_signals_nothing() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        let target = id("local-1");
        manager
            .set_desired_capacity(deadline().deadline(), fleet, 1)
            .unwrap();
        let expired_scope = deadline();
        expired_scope.clock().advance(Duration::from_secs(1));
        let expired = expired_scope.deadline();

        assert!(matches!(
            manager.terminate(expired, fleet, &target),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(processes.terminated.lock().is_empty());
        assert!(
            !manager
                .records()
                .read(&target)
                .unwrap()
                .unwrap()
                .is_terminating()
        );

        manager
            .terminate(deadline().deadline(), fleet, &target)
            .unwrap();
        clock.advance(TERMINATE_GRACE.get() + Duration::from_secs(1));
        assert!(matches!(
            manager.converge(expired, fleet),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(processes.killed.lock().is_empty());
    }
}
