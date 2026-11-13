//! The local fleet. Instances are worker processes; the record files and the
//! desired-capacity files are the only state, so a restarted server sees the
//! same fleet.

use std::borrow::Cow;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::OnceLock;

use gang_clock::Clock;
use gang_spi::{FleetInstance, FleetSupply, InstanceExit};
use gang_types::{Fleet, FleetName, InstanceId, InstanceLifecycle, NonZeroDuration};
use parking_lot::Mutex;

use crate::LocalFleetOptions;
use crate::errors::LocalError;
use crate::layout::StateLayout;
use crate::processes::{Processes, WorkerCommand, WorkerEndpoints, exit_reason, failure_record};
use crate::records::{InstanceRecord, RecordStore};
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
    processes: P,
    clock: C,
    /// How long a worker may live after its launch and still count as an early exit when it dies.
    relaunch_backoff: NonZeroDuration,
    /// How long a worker may keep running after SIGTERM before its process group is killed.
    terminate_grace: NonZeroDuration,
    /// Early exits not yet reported, with their fleet; `supply` hands a fleet's exits out.
    pending_exits: Mutex<Vec<(FleetName, InstanceExit)>>,
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
            processes,
            clock,
            relaunch_backoff,
            terminate_grace,
            pending_exits: Mutex::new(Vec::new()),
        }
    }

    /// Records the server endpoints for launched workers once; a second call is rejected because
    /// the provisioner attaches once per server process.
    ///
    /// # Errors
    ///
    /// Returns [`LocalError::AlreadyAttached`] when endpoints were already recorded.
    pub(crate) fn set_server_endpoints(
        &self,
        endpoints: WorkerEndpoints,
    ) -> Result<(), LocalError> {
        self.endpoints
            .set(endpoints)
            .map_err(|_| LocalError::AlreadyAttached)
    }

    /// Overwrites `into` with the desired capacity and the live instances of `fleet`.
    ///
    /// Dead records of `fleet` are reaped and replaced first, so the reported fleet
    /// already holds the persisted desired capacity. The `instances` buffer is
    /// cleared and refilled, never replaced; the order of instances and exits is unspecified.
    pub(crate) fn supply(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), LocalError> {
        self.processes.reap();
        into.warm = 0;
        into.instances.clear();
        into.exits.clear();
        self.converge(fleet, &mut |record: &InstanceRecord| {
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
        })?;
        into.desired = self.records.desired(fleet.name)?;
        let mut pending = self.pending_exits.lock();
        let mut index = 0;
        while index < pending.len() {
            if pending[index].0 == *fleet.name {
                into.exits.push(pending.swap_remove(index).1);
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    /// Persists the larger of `desired` and the persisted desired capacity, then
    /// launches workers until the live, non-terminating count reaches it. Lowering
    /// `desired` never lowers it and never stops a worker; scale-in is done by
    /// [`LocalFleet::terminate`].
    pub(crate) fn set_desired_capacity(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        desired: u32,
    ) -> Result<(), LocalError> {
        self.processes.reap();
        if desired > self.records.desired(fleet.name)? {
            self.records.set_desired(fleet.name, desired)?;
        }
        self.converge(fleet, &mut |_: &InstanceRecord| {})
    }

    /// Drains one instance: marks it terminating and signals its process, or removes it outright
    /// if its lifetime lock is no longer held. A dead instance whose cgroup is still busy stays
    /// marked terminating until a later pass retires it. Desired capacity drops by one unless the
    /// instance was already terminating. An instance with no record, or whose record belongs to
    /// another fleet, is already gone from `fleet` and is left alone.
    pub(crate) fn terminate(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), LocalError> {
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
        if gang_os::FileLock::is_held(&self.layout.lock_path(&record.id))? {
            if let Some(pid) = record.pid {
                self.processes.terminate(pid)?;
            }
            self.records.write(&record)?;
        } else if !self.retire(&record)? {
            self.records.write(&record)?;
        }
        if !already_terminating {
            let desired = self.records.desired(fleet.name)?;
            self.records
                .set_desired(fleet.name, desired.saturating_sub(1))?;
        }
        Ok(())
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
    /// or holds the server's is never reaped. A worker that recorded none died before it was
    /// identified; actions start only after registration, which follows the recording, so it ran
    /// none and there is nothing to reap.
    fn retire(&self, record: &InstanceRecord) -> Result<bool, LocalError> {
        if let Some(server) = self.processes.server_cgroup()? {
            match self.records.worker_cgroup(&record.id)? {
                None => tracing::warn!(
                    instance = %record.id,
                    "the worker died before it was identified; it ran no action"
                ),
                Some(dir) => {
                    if !server.starts_with(&dir)
                        && matches!(
                            self.processes.reap_cgroup(&dir)?,
                            gang_os::cgroup::Removal::Busy
                        )
                    {
                        return Ok(false);
                    }
                }
            }
        }
        self.records.remove(&record.id)?;
        Ok(true)
    }

    /// Logs an early exit and reports its failure record when one exists.
    fn report_exit(&self, fleet: Fleet<'_, LocalFleetOptions>, record: &InstanceRecord) {
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
            self.pending_exits.lock().push((
                fleet.name.clone(),
                InstanceExit {
                    instance: record.id.clone(),
                    failure: failure.to_owned(),
                },
            ));
        }
    }

    /// Reaps the dead records of `fleet`, then launches workers until the live,
    /// non-terminating count reaches the persisted desired capacity. A worker that
    /// died within `relaunch_backoff` of its launch is noted as an early exit, and
    /// no replacement is launched until that backoff has passed since the exit.
    /// A terminating worker that still holds its lock once `terminate_grace` has passed since its
    /// SIGTERM is killed; this repeats every pass until the worker is gone.
    ///
    /// `on_instance` is called once for each record of `fleet` that survives the pass
    /// (terminating or serving) and once for each worker this pass launches.
    fn converge(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        on_instance: &mut impl FnMut(&InstanceRecord),
    ) -> Result<(), LocalError> {
        let now = self.clock.wall();
        let mut serving: u32 = 0;
        let mut pending_retirement = false;
        for record in self.records.records()? {
            let record = record?;
            if record.fleet != *fleet.name {
                continue;
            }
            if !gang_os::FileLock::is_held(&self.layout.lock_path(&record.id))? {
                if self.retire(&record)? {
                    let early_exit = !record.is_terminating()
                        && now.saturating_duration_since(record.launched_at)
                            < self.relaunch_backoff.get();
                    if early_exit {
                        self.records.note_early_exit(fleet.name, now)?;
                        self.report_exit(fleet, &record);
                    }
                } else {
                    pending_retirement = true;
                }
                continue;
            }
            match record.terminating_since {
                Some(since) => {
                    if now.saturating_duration_since(since) >= self.terminate_grace.get()
                        && let Some(pid) = record.pid
                    {
                        self.processes.kill(pid)?;
                    }
                }
                None => serving = serving.saturating_add(1),
            }
            on_instance(&record);
        }
        if pending_retirement {
            return Ok(());
        }
        let desired = self.records.desired(fleet.name)?;
        if serving >= desired {
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
            let record = self.spawn_one(fleet)?;
            on_instance(&record);
        }
        Ok(())
    }

    /// Writes the worker's record before starting it, so mock IMDS can authorize its first request,
    /// then records its pid. The process spawn takes the worker's lifetime lock. A failed spawn
    /// removes the record; a failed pid update signals the new process.
    fn spawn_one(&self, fleet: Fleet<'_, LocalFleetOptions>) -> Result<InstanceRecord, LocalError> {
        let id = self.records.next_instance()?;
        let dir = self.layout.instance_dir(&id);
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
            self.layout.lock_path(&worker.instance),
            self.layout.log_path(&worker.instance),
            self.endpoints.get(),
        );
        let WorkerIdentity { instance, secret } = worker;
        let mut record = InstanceRecord {
            id: instance,
            fleet: fleet.name.clone(),
            isa: fleet.isa,
            pid: None,
            launched_at: self.clock.wall(),
            terminating_since: None,
            secret,
        };
        self.records.write(&record)?;
        let pid = match self.processes.spawn(&command) {
            Ok(pid) => pid,
            Err(error) => {
                let _ = self.records.remove(&record.id);
                return Err(error);
            }
        };
        record.pid = Some(pid);
        if let Err(error) = self.records.write(&record) {
            let _ = self.processes.terminate(pid);
            return Err(error);
        }
        Ok(record)
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
        INSTANCE_ENV, INSTANCE_SECRET_ENV, Processes, WorkerCommand, WorkerEndpoints,
        acquire_lifetime_lock,
    };

    /// The backoff the tests run with: a worker that dies sooner than this after launch is an early exit.
    const RELAUNCH_BACKOFF: NonZeroDuration = NonZeroDuration::from_secs(30);

    /// How long the tests' terminating workers may run after SIGTERM before they are killed.
    const TERMINATE_GRACE: NonZeroDuration = NonZeroDuration::from_secs(10);

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
        fn spawn(&self, command: &WorkerCommand) -> Result<u32, LocalError> {
            if let Some(probe) = &*self.spawn_probe.lock() {
                probe(command);
            }
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
            self.locks.lock().insert(pid, lock);
            self.commands.lock().push(command.clone());
            Ok(pid)
        }

        fn server_cgroup(&self) -> Result<Option<PathBuf>, LocalError> {
            Ok(self.server.lock().clone())
        }

        fn reap_cgroup(&self, dir: &Path) -> Result<gang_os::cgroup::Removal, LocalError> {
            self.reaped_cgroups.lock().push(dir.to_path_buf());
            Ok(self
                .removals
                .lock()
                .pop_front()
                .unwrap_or(gang_os::cgroup::Removal::Removed))
        }

        fn terminate(&self, pid: u32) -> Result<(), LocalError> {
            self.terminated.lock().push(pid);
            Ok(())
        }

        fn kill(&self, pid: u32) -> Result<(), LocalError> {
            self.killed.lock().push(pid);
            Ok(())
        }

        fn reap(&self) {}
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

        manager.set_desired_capacity(fleet, 2).unwrap();

        let supply = supply_of(&manager, fleet);
        let launched_at = clock.wall();
        assert_eq!(
            supply,
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 2,
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
            .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
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
            .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
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
    fn server_endpoints_are_recorded_once() -> Result<(), Box<dyn std::error::Error>> {
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
        assert!(matches!(
            manager.set_server_endpoints(endpoints.clone()),
            Err(LocalError::AlreadyAttached)
        ));
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

        manager.set_desired_capacity(fleet, 2).unwrap();
        manager.set_desired_capacity(fleet, 2).unwrap();

        assert_eq!(processes.launched(), 2);
    }

    #[test]
    fn set_desired_capacity_never_lowers_the_desired_capacity() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);

        manager.set_desired_capacity(fleet, 3).unwrap();
        manager.set_desired_capacity(fleet, 1).unwrap();

        assert!(processes.terminated.lock().is_empty());
        assert_eq!(processes.launched(), 3);
        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.desired, 3);
        assert_eq!(supply.instances.len(), 3);
        assert!(
            supply
                .instances
                .iter()
                .all(|i| i.lifecycle == InstanceLifecycle::InService)
        );
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
            manager.set_desired_capacity(fleet, 2).unwrap();
            supply_of(&manager, fleet)
        };

        let restarted = new_manager(&layout, processes.clone(), &clock);

        assert_eq!(supply_of(&restarted, fleet), before);
        restarted.set_desired_capacity(fleet, 2).unwrap();
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
        manager.set_desired_capacity(fleet, 2).unwrap();
        clock.advance(Duration::from_secs(31));
        let dead = id("local-1");
        let dead_pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();

        processes.exit(dead_pid);

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
    fn a_dead_worker_is_reaped_by_the_scope_it_recorded() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let scope = PathBuf::from("/fake/cgroup/run-1.scope");
        manager
            .records()
            .write_worker_cgroup(&dead, &scope)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let scope = PathBuf::from("/fake/cgroup/run-1.scope");
        manager
            .records()
            .write_worker_cgroup(&dead, &scope)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let first = supply_of(&manager, fleet);
        assert!(first.instances.is_empty());
        assert_eq!(processes.launched(), 1);
        assert!(manager.records().read(&dead).unwrap().is_some());

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let warnings = Warnings::default();
        let supply = gang_testkit::with_subscriber(warnings.clone(), || supply_of(&manager, fleet));
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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let warnings = Warnings::default();
        let supply = gang_testkit::with_subscriber(warnings.clone(), || supply_of(&manager, fleet));

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup/server"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, id("local-2"));
        assert_eq!(processes.launched(), 2);
        assert!(manager.records().read(&dead).unwrap().is_none());
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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, Path::new("/fake/cgroup"))
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        clock.advance(Duration::from_secs(31));
        processes.exit(pid);

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, &cgroup)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);

        manager.terminate(fleet, &dead).unwrap();
        assert!(manager.records().read(&dead).unwrap().is_some());
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
        manager.set_desired_capacity(fleet, 2).unwrap();
        let dead = id("local-1");
        manager
            .records()
            .write_worker_cgroup(&dead, &cgroup)
            .unwrap();
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        processes.exit(pid);

        manager.terminate(fleet, &dead).unwrap();
        manager.terminate(fleet, &dead).unwrap();

        assert!(
            manager
                .records()
                .read(&dead)
                .unwrap()
                .unwrap()
                .is_terminating()
        );
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
        manager.set_desired_capacity(fleet, 2).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();

        manager.terminate(fleet, &target).unwrap();

        assert_eq!(processes.terminated.lock().as_slice(), [target_pid]);
        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
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

        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
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
            .terminate(fleet_view(&name, Isa::X86_64), &id("local-9"))
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
        manager.set_desired_capacity(fleet, 2).unwrap();
        let target = id("local-1");

        manager.terminate(fleet, &target).unwrap();
        manager.terminate(fleet, &target).unwrap();

        assert_eq!(supply_of(&manager, fleet).desired, 1);
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

        manager.set_desired_capacity(small, 1).unwrap();
        manager.set_desired_capacity(big, 2).unwrap();

        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, small),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                instances: vec![in_service("local-1", launched_at)],
            }
        );
        assert_eq!(
            supply_of(&manager, big),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                instances: vec![
                    in_service("local-2", launched_at),
                    in_service("local-3", launched_at),
                ],
            }
        );

        let small_before = supply_of(&manager, small);
        let big_before = supply_of(&manager, big);
        manager.terminate(big, &id("local-1")).unwrap();

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
        manager.terminate(small, &id("local-1")).unwrap();

        assert_eq!(processes.terminated.lock().as_slice(), [target_pid]);
        assert_eq!(
            supply_of(&manager, small),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 0,
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
                warm: 0,
                exits: Vec::new(),
                desired: 2,
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

        manager.set_desired_capacity(x86, 1).unwrap();
        manager.set_desired_capacity(arm, 2).unwrap();

        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, x86),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                instances: vec![in_service("local-1", launched_at)],
            }
        );
        assert_eq!(
            supply_of(&manager, arm),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 2,
                instances: vec![
                    in_service("local-2", launched_at),
                    in_service("local-3", launched_at),
                ],
            }
        );
        let x86_before = supply_of(&manager, x86);
        let arm_before = supply_of(&manager, arm);
        manager.terminate(arm, &id("local-1")).unwrap();

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
            .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
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
            .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
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

        manager.set_desired_capacity(fleet_view(&name, Isa::X86_64), 2)?;

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
    fn supply_replaces_a_dead_worker_without_another_set_desired_capacity() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(fleet, 2).unwrap();
        clock.advance(Duration::from_secs(31));
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        processes.exit(dead_pid);

        let supply = supply_of(&manager, fleet);

        assert_eq!(supply.desired, 2);
        let ids: Vec<&InstanceId> = supply.instances.iter().map(|i| &i.id).collect();
        assert_eq!(ids, [&id("local-2"), &id("local-3")]);
        assert_eq!(processes.launched(), 3);
    }

    #[test]
    fn terminating_instances_are_not_replaced_by_supply() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(fleet, 2).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager.terminate(fleet, &target).unwrap();

        let launched_at = clock.wall();
        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
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

        assert_eq!(
            supply_of(&manager, fleet),
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
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
        manager.set_desired_capacity(fleet, 1).unwrap();
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
        let supply = gang_testkit::with_subscriber(subscriber, || supply_of(&manager, fleet));
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
    fn an_early_exit_without_a_failure_record_is_not_reported() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new().unwrap();
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let fleet = fleet_view(&name, Isa::X86_64);
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(layout.log_path(&dead), "INFO worker stopped\n").unwrap();
        processes.exit(pid);
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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead = id("local-1");
        let pid = manager.records().read(&dead).unwrap().unwrap().pid.unwrap();
        std::fs::write(layout.log_path(&dead), "fail  worker failed\n").unwrap();
        processes.exit(pid);
        manager.set_desired_capacity(fleet, 1).unwrap();
        assert_eq!(
            supply_of(&manager, fleet).exits,
            vec![InstanceExit {
                instance: dead,
                failure: "fail  worker failed".to_owned(),
            }]
        );
        assert!(supply_of(&manager, fleet).exits.is_empty());
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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();

        processes.exit(dead_pid);

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        processes.exit(dead_pid);
        supply_of(&manager, fleet);
        assert_eq!(processes.launched(), 1);

        clock.advance(RELAUNCH_BACKOFF.get());

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let dead_pid = manager
            .records()
            .read(&id("local-1"))
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        clock.advance(RELAUNCH_BACKOFF.get() + Duration::from_secs(1));

        processes.exit(dead_pid);

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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager.terminate(fleet, &target).unwrap();

        processes.exit(target_pid);

        let supply = supply_of(&manager, fleet);
        assert_eq!(supply.desired, 0);
        assert_eq!(supply.instances.len(), 0);
        assert_eq!(manager.records().early_exit(&name).unwrap(), None);
        manager.set_desired_capacity(fleet, 1).unwrap();
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
        manager.set_desired_capacity(fleet, 1).unwrap();
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
        manager.set_desired_capacity(fleet, 2).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager.terminate(fleet, &target).unwrap();

        clock.advance(
            TERMINATE_GRACE
                .get()
                .saturating_sub(Duration::from_millis(1)),
        );
        supply_of(&manager, fleet);
        assert!(processes.killed.lock().is_empty());

        clock.advance(Duration::from_millis(1));
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
        manager.set_desired_capacity(fleet, 1).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager.terminate(fleet, &target).unwrap();
        clock.advance(TERMINATE_GRACE.get() + Duration::from_secs(1));

        processes.exit(target_pid);

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
        manager.set_desired_capacity(fleet, 2).unwrap();
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)
            .unwrap()
            .unwrap()
            .pid
            .unwrap();
        manager.terminate(fleet, &target).unwrap();
        clock.advance(TERMINATE_GRACE.get() / 2);
        manager.terminate(fleet, &target).unwrap();
        clock.advance(TERMINATE_GRACE.get() / 2);

        supply_of(&manager, fleet);

        assert_eq!(processes.killed.lock().as_slice(), [target_pid]);
    }

    #[test]
    fn a_failed_spawn_leaves_no_instance_directory() -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        let instance = id("local-1");
        *processes.fail_next_spawn.lock() = true;

        assert!(
            manager
                .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
                .is_err()
        );

        assert!(!layout.instance_dir(&instance).exists());
        assert!(manager.records().read(&instance)?.is_none());
        Ok(())
    }

    #[test]
    fn a_failed_record_write_terminates_the_spawned_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = TempDir::new()?;
        let layout = layout_in(dir.path());
        let processes = FakeProcesses::new();
        let clock = clock();
        let manager = new_manager(&layout, processes.clone(), &clock);
        let name = fleet_name("local");
        *processes.obstruct_record.lock() = Some(layout.record_path(&id("local-1")));

        assert!(
            manager
                .set_desired_capacity(fleet_view(&name, Isa::X86_64), 1)
                .is_err()
        );

        let spawned: Vec<u32> = processes.locks.lock().keys().copied().collect();
        assert_eq!(spawned.len(), 1);
        assert_eq!(processes.terminated.lock().as_slice(), spawned.as_slice());
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
        manager.set_desired_capacity(fleet, 1)?;
        let target = id("local-1");
        let target_pid = manager
            .records()
            .read(&target)?
            .ok_or("no record for local-1")?
            .pid
            .ok_or("worker has no pid")?;
        manager.terminate(fleet, &target)?;

        clock.advance(TERMINATE_GRACE.get());
        supply_of(&manager, fleet);
        supply_of(&manager, fleet);

        assert_eq!(processes.killed.lock().as_slice(), [target_pid, target_pid]);
        Ok(())
    }
}
