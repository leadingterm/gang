//! `gang local down`: stops the gang server recorded in the server directory and terminates its workers.

use std::path::Path;
use std::process::ExitCode;

use gang_config::GangConfig;
use gang_os::FileLock;
use gang_spi::{FleetSupply, PluginContext, ProvisionerPort};
use gang_types::Fleet;

use crate::cli::LocalDownArgs;
use crate::config_file::LoadedConfig;
use crate::errors::{self, LocalCommandError};
use crate::local_process::{LocalStackProcesses, ProcessControl, Wait};
use crate::local_record::ServerDirectory;
use crate::output::{Line, Output, Record};
use crate::plugins::{Plugins, Provisioner};
use crate::runtime;

/// What `stop_server` found and did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// No server holds the lock; any stale record was removed and nothing was signalled.
    NotRunning,
    /// The recorded server released its lock; `pid` is the process that was recorded.
    Stopped {
        /// Process id from the server record.
        pid: u32,
    },
}

/// Terminates the server that holds the server lock, waits for the lock to be released, and
/// removes its record. Liveness is the lock, never a pid probe.
///
/// The returned `FileLock` is the server lock, acquired by the caller. While it lives, no server
/// can start in this state directory, so callers hold it until they have finished acting on the
/// stopped server (such as terminating its workers).
pub(crate) fn stop_server(
    directory: &ServerDirectory,
    wait: &Wait,
    processes: &impl ProcessControl,
    out: &Output,
) -> Result<(Stopped, FileLock), LocalCommandError> {
    directory.create()?;
    match directory.acquire_lock() {
        Ok(lock) => {
            directory.remove()?;
            Ok((Stopped::NotRunning, lock))
        }
        Err(LocalCommandError::AlreadyRunning { .. }) => {
            stop_running(directory, wait, processes, out)
        }
        Err(error) => Err(error),
    }
}

/// Terminates the recorded process of the running server, then polls until the lock can be
/// acquired. Acquiring the lock means the server is gone and no other server has started.
fn stop_running(
    directory: &ServerDirectory,
    wait: &Wait,
    processes: &impl ProcessControl,
    out: &Output,
) -> Result<(Stopped, FileLock), LocalCommandError> {
    let Some(record) = directory.read()? else {
        return Err(LocalCommandError::UnrecordedServer {
            lock: directory.lock_path(),
        });
    };
    let pid = record.pid;
    // Held until the lock is acquired or an error returns: dropping it clears the wait line.
    let _progress = out.progress(&format!("stopping the gang server pid {pid}"));
    processes.terminate(pid)?;
    let acquired = wait.until(processes, || match directory.acquire_lock() {
        Ok(lock) => Ok(Some(lock)),
        Err(LocalCommandError::AlreadyRunning { .. }) => Ok(None),
        Err(error) => Err(error),
    })?;
    match acquired {
        Some(lock) => {
            directory.remove()?;
            Ok((Stopped::Stopped { pid }, lock))
        }
        None => Err(LocalCommandError::StopTimedOut {
            pid,
            timeout: wait.timeout.get(),
        }),
    }
}

/// The record that reports what happened to the server.
fn stopped_record(outcome: Stopped) -> Record {
    Record::ok(match outcome {
        Stopped::NotRunning => Line::new().plain("no gang server was running"),
        Stopped::Stopped { pid } => Line::new()
            .plain("stopped the gang server ")
            .muted(format!("· pid {pid}")),
    })
}

/// The record that reports how many workers were terminated.
fn terminated_record(workers: usize) -> Record {
    let noun = if workers == 1 { "worker" } else { "workers" };
    Record::ok(Line::new().plain(format!("terminated {workers} {noun}")))
}

/// Prints what `gang local down` did: the server outcome, then the terminated workers.
fn present(out: &Output, outcome: Stopped, workers: usize) -> errors::Result<()> {
    out.result(&stopped_record(outcome))?;
    out.result(&terminated_record(workers))
}

/// `gang local down`: stops the gang server, terminates every worker instance, and reports both.
pub(crate) fn run(args: LocalDownArgs, out: &Output) -> errors::Result<ExitCode> {
    let (LoadedConfig { path, config, .. }, directory) = LoadedConfig::local(args.config)?;
    let wait = Wait {
        timeout: args.stop_timeout,
        poll_interval: args.poll_interval,
    };
    let (outcome, lock) = stop_server(&directory, &wait, &LocalStackProcesses, out)?;
    let workers = terminate_workers(config, &path)?;
    present(out, outcome, workers)?;
    drop(lock);
    Ok(ExitCode::SUCCESS)
}

/// Terminates every worker instance the configured fleets name; returns how many were terminated.
fn terminate_workers(config: GangConfig<Plugins>, config_path: &Path) -> errors::Result<usize> {
    let GangConfig {
        provisioner,
        fleets,
        ..
    } = config;
    let provisioner = Provisioner::build(provisioner, &PluginContext { config_path })?;
    runtime::block_on(terminate_all(
        &provisioner,
        fleets.iter().map(|fleet| fleet.fleet()),
    ))
}

/// Terminates each instance the provisioner reports for every fleet; returns how many were terminated.
async fn terminate_all<'a, P: ProvisionerPort + 'a>(
    provisioner: &P,
    fleets: impl IntoIterator<Item = Fleet<'a, P::FleetOptions>>,
) -> Result<usize, LocalCommandError> {
    let mut terminated = 0;
    for fleet in fleets {
        terminated += terminate_fleet(provisioner, fleet).await?;
    }
    Ok(terminated)
}

/// Terminates every instance of `fleet` and returns how many there were.
///
/// The desired capacity falls to zero before the instances are listed, so the provisioner
/// does not launch replacements for the instances it terminates.
async fn terminate_fleet<P: ProvisionerPort>(
    provisioner: &P,
    fleet: Fleet<'_, P::FleetOptions>,
) -> Result<usize, LocalCommandError> {
    provisioner
        .set_desired_capacity(fleet, 0)
        .await
        .map_err(LocalCommandError::Provisioner)?;
    let mut supply = FleetSupply::default();
    provisioner
        .supply(fleet, &mut supply)
        .await
        .map_err(LocalCommandError::Provisioner)?;
    for instance in &supply.instances {
        provisioner
            .terminate(fleet, &instance.id)
            .await
            .map_err(LocalCommandError::Provisioner)?;
    }
    Ok(supply.instances.len())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_spi::{FleetInstance, PortError};
    use gang_types::{
        Bytes, FleetName, InstanceId, InstanceLifecycle, Isa, NonZeroDuration, Resources,
    };
    use parking_lot::Mutex;
    use tempfile::TempDir;

    use super::*;
    use crate::local_record::ServerRecord;

    const SERVER_PID: u32 = 4242;
    const STALE_PID: u32 = 99;

    /// Process control whose lock release is scripted. `terminate` records the pid and fails
    /// with a signal error when `fail_terminate` is set. The lock held by the fake is dropped on
    /// the `release_after`-th pause, so the lock is released only after that many pauses; `None`
    /// keeps it held forever.
    struct FakeProcesses {
        lock: Mutex<Option<FileLock>>,
        release_after: Option<u32>,
        fail_terminate: bool,
        pauses: Cell<u32>,
        terminated: Mutex<Vec<u32>>,
    }

    impl FakeProcesses {
        fn new(lock: Option<FileLock>, release_after: Option<u32>) -> Self {
            Self {
                lock: Mutex::new(lock),
                release_after,
                fail_terminate: false,
                pauses: Cell::new(0),
                terminated: Mutex::new(Vec::new()),
            }
        }

        /// Makes every `terminate` call fail as if the signal were refused.
        fn failing_terminate(mut self) -> Self {
            self.fail_terminate = true;
            self
        }
    }

    impl ProcessControl for FakeProcesses {
        fn terminate(&self, pid: u32) -> Result<(), LocalCommandError> {
            self.terminated.lock().push(pid);
            if self.fail_terminate {
                return Err(LocalCommandError::Signal {
                    pid,
                    source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                });
            }
            Ok(())
        }

        fn pause(&self, _interval: Duration) {
            let pauses = self.pauses.get() + 1;
            self.pauses.set(pauses);
            if Some(pauses) == self.release_after {
                let released = self.lock.lock().take();
                drop(released);
            }
        }
    }

    /// Provisioner that records each port call in order, naming the fleet in the call text. Each
    /// fleet reports only the instances it was given; `failing_capacity` makes every capacity
    /// change fail.
    struct RecordingProvisioner {
        supplies: Vec<(&'static str, Vec<InstanceId>)>,
        failing_capacity: bool,
        calls: Mutex<Vec<String>>,
    }

    impl RecordingProvisioner {
        /// A provisioner whose `local-workers` fleet reports `instances`.
        fn new(instances: &[&str]) -> Self {
            Self::with_fleets(&[("local-workers", instances)])
        }

        /// A provisioner whose named fleets report the given instances.
        fn with_fleets(fleets: &[(&'static str, &[&str])]) -> Self {
            Self {
                supplies: fleets
                    .iter()
                    .map(|(name, instances)| {
                        let instances: Vec<InstanceId> = instances
                            .iter()
                            .map(|id| InstanceId::try_new(*id).expect("valid instance id"))
                            .collect();
                        (*name, instances)
                    })
                    .collect(),
                failing_capacity: false,
                calls: Mutex::new(Vec::new()),
            }
        }

        /// Makes every capacity change fail.
        fn failing_capacity(mut self) -> Self {
            self.failing_capacity = true;
            self
        }

        fn instances_of(&self, fleet: &str) -> &[InstanceId] {
            self.supplies
                .iter()
                .find(|(name, _)| *name == fleet)
                .map_or(&[], |(_, instances)| instances.as_slice())
        }

        fn record(&self, call: String) {
            self.calls.lock().push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().clone()
        }
    }

    impl ProvisionerPort for RecordingProvisioner {
        type FleetOptions = ();

        async fn supply(
            &self,
            fleet: Fleet<'_, ()>,
            into: &mut FleetSupply,
        ) -> Result<(), PortError> {
            self.record(format!("supply({})", fleet.name.as_str()));
            into.exits.clear();
            into.instances = self
                .instances_of(fleet.name.as_str())
                .iter()
                .map(|id| FleetInstance {
                    id: id.clone(),
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: Timestamp::UNIX_EPOCH,
                    release_first: false,
                })
                .collect();
            Ok(())
        }

        async fn set_desired_capacity(
            &self,
            fleet: Fleet<'_, ()>,
            desired: u32,
        ) -> Result<(), PortError> {
            self.record(format!(
                "set_desired_capacity({}, {desired})",
                fleet.name.as_str()
            ));
            if self.failing_capacity {
                return Err(PortError::unavailable("capacity change refused"));
            }
            Ok(())
        }

        async fn terminate(
            &self,
            fleet: Fleet<'_, ()>,
            instance: &InstanceId,
        ) -> Result<(), PortError> {
            self.record(format!(
                "terminate({}, {})",
                fleet.name.as_str(),
                instance.as_str()
            ));
            Ok(())
        }

        fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
            gang_types::WorkerLifetime::LongLived
        }
    }

    /// The fleet name the tests build their [`Fleet`] from.
    fn fleet_name() -> FleetName {
        FleetName::try_new("local-workers").expect("valid fleet name")
    }

    /// A fleet named `name` with the test sizing and no options.
    fn fleet(name: &FleetName) -> Fleet<'_, ()> {
        Fleet {
            name,
            isa: Isa::X86_64,
            instance: Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(20 << 30),
            },
            options: &(),
        }
    }

    fn wait(timeout_secs: u64) -> Wait {
        Wait {
            timeout: NonZeroDuration::from_secs(timeout_secs),
            poll_interval: NonZeroDuration::from_secs(1),
        }
    }

    fn directory() -> (TempDir, ServerDirectory) {
        let state = TempDir::new().expect("create state dir");
        let directory = ServerDirectory::new(&state.path().join("state"));
        directory.create().expect("create server directory");
        (state, directory)
    }

    fn record(pid: u32) -> ServerRecord {
        ServerRecord {
            pid,
            log: PathBuf::from("server.log"),
            config: PathBuf::from("gang.toml"),
        }
    }

    /// Whether a live server holds the server lock. Probes by acquiring the lock; a successful
    /// acquire drops its file immediately, so the probe leaves the lock free.
    fn held(directory: &ServerDirectory) -> bool {
        match directory.acquire_lock() {
            Ok(lock) => {
                drop(lock);
                false
            }
            Err(LocalCommandError::AlreadyRunning { .. }) => true,
            Err(error) => panic!("probe server lock: {error:?}"),
        }
    }

    #[test]
    fn no_lock_and_no_record_reports_not_running_and_holds_the_lock() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        let processes = FakeProcesses::new(None, None);

        let (outcome, lock) =
            stop_server(&directory, &wait(10), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::NotRunning);
        assert!(processes.terminated.lock().is_empty());
        assert!(held(&directory));
        drop(lock);
        assert!(!held(&directory));
    }

    #[test]
    fn not_running_lock_blocks_a_second_acquire_until_dropped() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        let processes = FakeProcesses::new(None, None);

        let (outcome, lock) =
            stop_server(&directory, &wait(10), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::NotRunning);
        assert!(matches!(
            directory.acquire_lock(),
            Err(LocalCommandError::AlreadyRunning { .. })
        ));
        drop(lock);
        let _lock = directory.acquire_lock().expect("lock is free once dropped");
    }

    #[test]
    fn stale_record_without_lock_is_removed_and_not_signalled() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(STALE_PID)).expect("seed record");
        let processes = FakeProcesses::new(None, None);

        let (outcome, lock) =
            stop_server(&directory, &wait(10), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::NotRunning);
        assert!(processes.terminated.lock().is_empty());
        assert!(directory.read().expect("read record").is_none());
        assert!(held(&directory));
        drop(lock);
        assert!(!held(&directory));
    }

    #[test]
    fn lock_without_record_is_an_unrecorded_server() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        let _lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(None, None);
        let lock_path = directory.lock_path();

        let result = stop_server(&directory, &wait(10), &processes, &Output::captured().0);

        assert!(matches!(
            result,
            Err(LocalCommandError::UnrecordedServer { ref lock }) if *lock == lock_path
        ));
        assert!(processes.terminated.lock().is_empty());
    }

    #[test]
    fn lock_released_after_two_pauses_stops_the_recorded_server() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), Some(2));

        let (outcome, lock) =
            stop_server(&directory, &wait(10), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::Stopped { pid: SERVER_PID });
        assert_eq!(*processes.terminated.lock(), vec![SERVER_PID]);
        assert_eq!(processes.pauses.get(), 2);
        assert!(directory.read().expect("read record").is_none());
        assert!(held(&directory));
        drop(lock);
        assert!(!held(&directory));
    }

    #[test]
    fn lock_never_released_times_out_and_keeps_record() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), None);

        let result = stop_server(&directory, &wait(3), &processes, &Output::captured().0);

        assert!(matches!(
            result,
            Err(LocalCommandError::StopTimedOut { pid: SERVER_PID, timeout })
                if timeout == Duration::from_secs(3)
        ));
        assert_eq!(*processes.terminated.lock(), vec![SERVER_PID]);
        let kept = directory.read().expect("read record").expect("record kept");
        assert_eq!(kept.pid, SERVER_PID);
    }

    #[test]
    fn stop_creates_an_absent_state_directory_and_holds_the_lock() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = TempDir::new().expect("create temp dir");
        let directory = ServerDirectory::new(&temp.path().join("fresh"));
        let processes = FakeProcesses::new(None, None);

        let (outcome, lock) =
            stop_server(&directory, &wait(10), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::NotRunning);
        assert!(matches!(
            directory.acquire_lock(),
            Err(LocalCommandError::AlreadyRunning { .. })
        ));
        drop(lock);
        assert!(!held(&directory));
    }

    #[test]
    fn lock_released_on_the_first_pause_stops_with_one_poll() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), Some(1));

        let (outcome, lock) =
            stop_server(&directory, &wait(1), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::Stopped { pid: SERVER_PID });
        assert_eq!(processes.pauses.get(), 1);
        assert!(directory.read().expect("read record").is_none());
        drop(lock);
    }

    #[test]
    fn lock_released_on_the_last_pause_is_caught_by_the_deadline_attempt() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), Some(3));

        let (outcome, lock) =
            stop_server(&directory, &wait(3), &processes, &Output::captured().0).expect("stop");

        assert_eq!(outcome, Stopped::Stopped { pid: SERVER_PID });
        assert_eq!(processes.pauses.get(), 3);
        assert!(directory.read().expect("read record").is_none());
        drop(lock);
    }

    #[test]
    fn lock_released_after_the_poll_budget_times_out_and_keeps_record() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), Some(4));

        let result = stop_server(&directory, &wait(3), &processes, &Output::captured().0);

        assert!(matches!(
            result,
            Err(LocalCommandError::StopTimedOut { pid: SERVER_PID, timeout })
                if timeout == Duration::from_secs(3)
        ));
        assert_eq!(processes.pauses.get(), 3);
        let kept = directory.read().expect("read record").expect("record kept");
        assert_eq!(kept.pid, SERVER_PID);
    }

    /// What `present` prints for `outcome` and `workers`, piped.
    fn printed(outcome: Stopped, workers: usize) -> (String, String) {
        let (out, stdout, stderr) = Output::captured();
        present(&out, outcome, workers).expect("present");
        (stdout.text(), stderr.text())
    }

    #[test]
    fn stopped_server_and_workers_print_two_ok_records() {
        let (stdout, stderr) = printed(Stopped::Stopped { pid: SERVER_PID }, 2);

        assert_eq!(
            stdout,
            "    ok  stopped the gang server · pid 4242\n    ok  terminated 2 workers\n"
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn no_server_and_no_workers_print_two_ok_records() {
        let (stdout, _) = printed(Stopped::NotRunning, 0);

        assert_eq!(
            stdout,
            "    ok  no gang server was running\n    ok  terminated 0 workers\n"
        );
    }

    #[test]
    fn one_worker_is_singular() {
        let (stdout, _) = printed(Stopped::NotRunning, 1);

        assert_eq!(
            stdout,
            "    ok  no gang server was running\n    ok  terminated 1 worker\n"
        );
    }

    #[test]
    fn workers_drop_desired_capacity_before_supply_and_terminate_each_instance() {
        let provisioner = RecordingProvisioner::new(&["local-1", "local-2"]);
        let name = fleet_name();
        let terminated = runtime::block_on(terminate_fleet(&provisioner, fleet(&name)))
            .expect("fleet terminates");

        assert_eq!(terminated, 2);
        assert_eq!(
            provisioner.calls(),
            [
                "set_desired_capacity(local-workers, 0)",
                "supply(local-workers)",
                "terminate(local-workers, local-1)",
                "terminate(local-workers, local-2)",
            ]
        );
    }

    #[test]
    fn failed_capacity_change_lists_and_terminates_nothing() {
        let provisioner = RecordingProvisioner::new(&["local-1", "local-2"]).failing_capacity();
        let name = fleet_name();

        let result = runtime::block_on(terminate_fleet(&provisioner, fleet(&name)));

        assert!(matches!(
            result,
            Err(crate::errors::MainError::Local(
                LocalCommandError::Provisioner(_)
            ))
        ));
        assert_eq!(
            provisioner.calls(),
            ["set_desired_capacity(local-workers, 0)"]
        );
    }

    #[test]
    fn terminate_all_drops_each_fleets_capacity_before_its_instances() {
        let provisioner = RecordingProvisioner::with_fleets(&[
            ("alpha", &["alpha-1", "alpha-2"]),
            ("beta", &["beta-1"]),
        ]);
        let alpha = FleetName::try_new("alpha").expect("valid fleet name");
        let beta = FleetName::try_new("beta").expect("valid fleet name");

        let terminated =
            runtime::block_on(terminate_all(&provisioner, [fleet(&alpha), fleet(&beta)]))
                .expect("fleets terminate");

        assert_eq!(terminated, 3);
        assert_eq!(
            provisioner.calls(),
            [
                "set_desired_capacity(alpha, 0)",
                "supply(alpha)",
                "terminate(alpha, alpha-1)",
                "terminate(alpha, alpha-2)",
                "set_desired_capacity(beta, 0)",
                "supply(beta)",
                "terminate(beta, beta-1)",
            ]
        );
    }

    #[test]
    fn failed_signal_propagates_and_keeps_the_record_without_polling() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let (_state, directory) = directory();
        directory.write(&record(SERVER_PID)).expect("seed record");
        let lock = directory.acquire_lock().expect("hold server lock");
        let processes = FakeProcesses::new(Some(lock), Some(1)).failing_terminate();

        let result = stop_server(&directory, &wait(10), &processes, &Output::captured().0);

        assert!(matches!(
            result,
            Err(LocalCommandError::Signal {
                pid: SERVER_PID,
                ..
            })
        ));
        assert_eq!(*processes.terminated.lock(), vec![SERVER_PID]);
        assert_eq!(processes.pauses.get(), 0);
        let kept = directory.read().expect("read record").expect("record kept");
        assert_eq!(kept.pid, SERVER_PID);
    }

    #[test]
    fn a_state_directory_other_users_can_open_is_refused_and_left_unchanged() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let state = TempDir::new().expect("create state dir");
        fs::set_permissions(state.path(), fs::Permissions::from_mode(0o755)).expect("chmod");
        let directory = ServerDirectory::new(state.path());
        let processes = FakeProcesses::new(None, None);

        let result = stop_server(&directory, &wait(1), &processes, &Output::captured().0);

        assert!(matches!(
            result,
            Err(LocalCommandError::StateDirNotPrivate { mode: 0o755, .. })
        ));
        let mode = fs::metadata(state.path())
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755);
    }
}
