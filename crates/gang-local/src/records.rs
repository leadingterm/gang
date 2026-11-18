//! Controller mutations hold a shared filesystem lock and check their authority before publication.
//! Child and worker-owned records use their respective inherited and lifetime locks.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;

use gang_clock::Timestamp;
use gang_os::fs::{PRIVATE_FILE, ensure_private_dir, read_optional, read_to_string_optional};
use gang_os::{FileLock, PathIo};
use gang_spi::Deadline;
use gang_types::{FleetName, InstanceId, Isa};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::errors::LocalError;
use crate::layout::StateLayout;
use crate::worker_identity::InstanceSecret;

/// The durable facts about one launched worker, stored as `instance.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstanceRecord {
    /// Instance name; also the name of the record's directory.
    pub id: InstanceId,
    /// The fleet the worker belongs to.
    pub fleet: FleetName,
    /// Architecture the worker was launched for.
    pub isa: Isa,
    /// Process id committed by the child bootstrap before it starts the worker; `None` until then.
    /// The initial record makes the instance's secret available before the worker queries IMDS.
    pub pid: Option<u32>,
    /// Original launch process cgroup for durable ownership checks; `None` if unknown or without v2.
    #[serde(default)]
    pub launch_cgroup: Option<PathBuf>,
    /// Wall-clock time the worker was launched.
    pub launched_at: Timestamp,
    /// First observed lifetime lock release, retained until retirement.
    #[serde(default)]
    pub exited_at: Option<Timestamp>,
    /// Wall-clock time termination was requested (the SIGTERM), or `None` while the worker is not terminating.
    /// Such a worker no longer counts toward desired capacity.
    pub terminating_since: Option<Timestamp>,
    /// Secret the worker presents to the mock IMDS; only this record and the worker's environment hold it.
    pub secret: InstanceSecret,
}

impl InstanceRecord {
    /// Whether termination was requested for this worker.
    #[must_use]
    pub(crate) fn is_terminating(&self) -> bool {
        self.terminating_since.is_some()
    }
}

#[derive(Serialize, Deserialize)]
struct ReleaseIntent {
    record: InstanceRecord,
    desired: u32,
}

/// Reads and writes instance records, desired capacity, early-exit markers, and the id counter on disk.
///
/// Every call goes to disk, so a restarted process sees exactly what the previous one left.
/// The store creates instance directories and fleet exit directories lazily;
/// [`StateLayout::prepare`] creates the shared ones.
#[derive(Debug, Clone)]
pub(crate) struct RecordStore {
    layout: StateLayout,
}

/// Serializes controller mutations across stores and retains registered work.
pub(crate) struct Mutation {
    // Drop the lock before announcing that registered work has finished.
    lock: FileLock,
    work: gang_spi::Work,
}

impl Mutation {
    /// Releases the filesystem mutation lock while retaining registered work
    /// until the returned `Work` is dropped.
    #[cfg(test)]
    pub(crate) fn into_work(self) -> gang_spi::Work {
        let Self { lock, work } = self;
        drop(lock);
        work
    }

    pub(crate) fn check(&self) -> Result<(), LocalError> {
        self.work.deadline().check()?;
        Ok(())
    }

    pub(crate) fn deadline(&self) -> &Deadline {
        self.work.deadline()
    }

    pub(crate) fn bootstrap_stdio(&self) -> Result<Stdio, LocalError> {
        self.lock.to_stdio().map_err(LocalError::Bootstrap)
    }
}

impl RecordStore {
    /// Creates a store over the directories named by `layout`.
    #[must_use]
    pub(crate) fn new(layout: StateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn mutation(&self, deadline: &Deadline) -> Result<Mutation, LocalError> {
        let work = deadline.begin_work()?;
        let lock_path = self.layout.mutation_lock_path();
        let directory = ensure_private_dir(&self.layout.fleets_dir())?;
        let lock = directory.try_lock("mutation.lock")?.ok_or_else(|| {
            LocalError::Io(PathIo::new(
                "lock",
                &lock_path,
                io::ErrorKind::WouldBlock.into(),
            ))
        })?;
        work.deadline().check()?;
        Ok(Mutation { lock, work })
    }

    /// Atomically replaces the instance's record, creating its directory when needed.
    /// Refused once the deadline has passed.
    pub(crate) fn write(
        &self,
        mutation: &Mutation,
        record: &InstanceRecord,
    ) -> Result<(), LocalError> {
        mutation.check()?;
        let dir = self.layout.instance_dir(&record.id);
        let directory = ensure_private_dir(&dir)?;
        let path = self.layout.record_path(&record.id);
        let name = path
            .file_name()
            .ok_or_else(|| PathIo::new("write", &path, io::ErrorKind::InvalidInput.into()))?;
        directory.write_atomic_checked::<LocalError>(
            name,
            &to_json(&path, record)?,
            PRIVATE_FILE,
            || mutation.check(),
        )?;
        Ok(())
    }

    /// Commits the bootstrap process identity to an existing, uncommitted record.
    pub(crate) fn commit_pid(path: &Path, pid: u32) -> Result<(), LocalError> {
        let mut record = read_json::<InstanceRecord>(path)?
            .ok_or_else(|| PathIo::new("read", path, io::ErrorKind::NotFound.into()))?;
        if record.pid.is_some() {
            return Err(PathIo::new(
                "commit worker pid",
                path,
                io::Error::new(io::ErrorKind::InvalidData, "worker pid already committed"),
            )
            .into());
        }
        record.pid = Some(pid);
        write_private(path, &to_json(path, &record)?)
    }

    /// Returns the instance's record, or `None` when no record exists.
    pub(crate) fn read(&self, id: &InstanceId) -> Result<Option<InstanceRecord>, LocalError> {
        read_json(&self.layout.record_path(id))
    }

    /// Records the cgroup the instance's worker runs in. The worker writes it; it is removed with
    /// the instance directory.
    pub(crate) fn write_worker_cgroup(
        &self,
        id: &InstanceId,
        cgroup: &Path,
    ) -> Result<(), LocalError> {
        let path = self.layout.worker_cgroup_path(id);
        write_private(&path, cgroup.as_os_str().as_bytes())?;
        Ok(())
    }

    /// The cgroup the instance's worker recorded, or `None` when it recorded none.
    pub(crate) fn worker_cgroup(&self, id: &InstanceId) -> Result<Option<PathBuf>, LocalError> {
        Ok(read_optional(&self.layout.worker_cgroup_path(id))?
            .map(|bytes| PathBuf::from(OsString::from_vec(bytes))))
    }

    /// Lazily yields every record, in no particular order.
    ///
    /// Directories without a record, and directories not named as an instance id, are skipped.
    /// The directory is read on demand, so no list of records is ever built.
    pub(crate) fn records(
        &self,
    ) -> Result<impl Iterator<Item = Result<InstanceRecord, LocalError>>, LocalError> {
        let dir = self.layout.instances_dir();
        let entries = match fs::read_dir(&dir).map_err(PathIo::at("read directory", &dir)) {
            Ok(entries) => Some(entries),
            Err(error) if error.is_not_found() => None,
            Err(error) => return Err(LocalError::Io(error)),
        };
        let store = self.clone();
        Ok(entries
            .into_iter()
            .flatten()
            .filter_map(move |entry| record_of_entry(&store, &dir, entry)))
    }

    /// Removes the instance's whole directory, scratch included. Missing directories are fine.
    /// Refused once the deadline has passed.
    pub(crate) fn remove(&self, mutation: &Mutation, id: &InstanceId) -> Result<(), LocalError> {
        mutation.check()?;
        gang_os::fs::remove_dir_optional(&self.layout.instance_dir(id)).map_err(LocalError::Io)
    }

    /// Returns the persisted desired capacity for `fleet`; zero until it is first set.
    pub(crate) fn desired(&self, fleet: &FleetName) -> Result<u32, LocalError> {
        let path = self.layout.desired_path(fleet);
        match read_to_string_optional(&path)? {
            Some(text) => parse_count(&path, &text),
            None => Ok(0),
        }
    }

    /// Persists the desired capacity for `fleet`.
    /// Refused once the deadline has passed.
    pub(crate) fn set_desired(
        &self,
        mutation: &Mutation,
        fleet: &FleetName,
        desired: u32,
    ) -> Result<(), LocalError> {
        mutation.check()?;
        let path = self.layout.desired_path(fleet);
        write_private_checked(mutation, &path, desired.to_string().as_bytes())?;
        Ok(())
    }

    /// Durably records and applies a release before the worker is signalled or retired.
    pub(crate) fn release(
        &self,
        mutation: &Mutation,
        record: &InstanceRecord,
    ) -> Result<(), LocalError> {
        let desired = self.desired(&record.fleet)?.saturating_sub(1);
        let intent = ReleaseIntent {
            record: record.clone(),
            desired,
        };
        let path = self
            .layout
            .desired_path(&record.fleet)
            .with_extension("release.json");
        mutation.check()?;
        write_private_checked(mutation, &path, &to_json(&path, &intent)?)?;
        self.apply_release(mutation, &path, &intent)
    }

    /// Completes any interrupted release for this fleet before inspecting its state.
    pub(crate) fn recover_release(
        &self,
        mutation: &Mutation,
        fleet: &FleetName,
    ) -> Result<(), LocalError> {
        let path = self
            .layout
            .desired_path(fleet)
            .with_extension("release.json");
        if let Some(intent) = read_json::<ReleaseIntent>(&path)? {
            self.apply_release(mutation, &path, &intent)?;
        }
        Ok(())
    }

    fn apply_release(
        &self,
        mutation: &Mutation,
        path: &Path,
        intent: &ReleaseIntent,
    ) -> Result<(), LocalError> {
        self.write(mutation, &intent.record)?;
        self.set_desired(mutation, &intent.record.fleet, intent.desired)?;
        mutation.check()?;
        let name = path
            .file_name()
            .ok_or_else(|| PathIo::new("remove", path, io::ErrorKind::InvalidInput.into()))?;
        ensure_private_dir(&self.layout.fleets_dir())?.remove_optional(name)?;
        Ok(())
    }

    /// Persists the time of the latest early exit of a `fleet` worker, as decimal Unix
    /// milliseconds.
    /// Refused once the deadline has passed.
    pub(crate) fn note_early_exit(
        &self,
        mutation: &Mutation,
        fleet: &FleetName,
        at: Timestamp,
    ) -> Result<(), LocalError> {
        mutation.check()?;
        let path = self.layout.early_exit_path(fleet);
        write_private_checked(mutation, &path, at.as_unix_millis().to_string().as_bytes())?;
        Ok(())
    }

    /// Returns when a `fleet` worker last exited within its relaunch backoff, or `None` when none
    /// has.
    pub(crate) fn early_exit(&self, fleet: &FleetName) -> Result<Option<Timestamp>, LocalError> {
        let path = self.layout.early_exit_path(fleet);
        match read_to_string_optional(&path)? {
            Some(text) => Ok(Some(Timestamp::from_unix_millis(parse_count(
                &path, &text,
            )?))),
            None => Ok(None),
        }
    }

    /// Persists an exit before retiring its instance.
    pub(crate) fn record_exit(
        &self,
        mutation: &Mutation,
        fleet: &FleetName,
        instance: &InstanceId,
        failure: &str,
    ) -> Result<(), LocalError> {
        mutation.check()?;
        write_private_checked(
            mutation,
            &self.layout.exit_path(fleet, instance),
            failure.as_bytes(),
        )
    }

    /// Reuses `into` for the unacknowledged exits whose instances have been retired.
    pub(crate) fn fill_exits(
        &self,
        fleet: &FleetName,
        into: &mut Vec<gang_spi::InstanceExit>,
    ) -> Result<(), LocalError> {
        into.clear();
        let dir = self.layout.exits_dir(fleet);
        let entries = match fs::read_dir(&dir).map_err(PathIo::at("read directory", &dir)) {
            Ok(entries) => entries,
            Err(error) if error.is_not_found() => return Ok(()),
            Err(error) => return Err(LocalError::Io(error)),
        };
        for entry in entries {
            let entry = entry.map_err(PathIo::at("read directory", &dir))?;
            let path = entry.path();
            if !entry
                .file_type()
                .map_err(PathIo::at("read file type", &path))?
                .is_file()
                || path.extension().and_then(|extension| extension.to_str()) != Some("exit")
            {
                continue;
            }
            let Some(instance) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| InstanceId::try_new(stem).ok())
            else {
                continue;
            };
            if let Some(failure) = read_to_string_optional(&path)?
                && self.read(&instance)?.is_none()
            {
                into.push(gang_spi::InstanceExit { instance, failure });
            }
        }
        Ok(())
    }

    /// Removes the persisted exit after it has been audited.
    pub(crate) fn acknowledge_exit(
        &self,
        mutation: &Mutation,
        fleet: &FleetName,
        instance: &InstanceId,
    ) -> Result<(), LocalError> {
        mutation.check()?;
        gang_os::fs::remove_optional(&self.layout.exit_path(fleet, instance))
            .map_err(LocalError::Io)
    }

    /// Hands out the next `local-<n>` id; the persisted counter starts at 1 and survives restarts.
    /// Refused once the deadline has passed.
    pub(crate) fn next_instance(&self, mutation: &Mutation) -> Result<InstanceId, LocalError> {
        mutation.check()?;
        let path = self.layout.counter_path();
        let current: u64 = match read_to_string_optional(&path)? {
            Some(text) => parse_count(&path, &text)?,
            None => 1,
        };
        mutation.check()?;
        let next = current.checked_add(1).ok_or_else(|| {
            LocalError::Io(PathIo::new(
                "advance counter",
                &path,
                io::Error::new(io::ErrorKind::InvalidData, "instance counter is exhausted"),
            ))
        })?;
        write_private_checked(mutation, &path, next.to_string().as_bytes())?;
        let id = InstanceId::try_new(format!("local-{current}"))?;
        Ok(id)
    }

    /// Records a simulated Spot interruption for an existing instance.
    ///
    /// Fails with `UnknownInstance` when the instance has no record.
    #[cfg(test)]
    pub(crate) fn write_preemption(
        &self,
        id: &InstanceId,
        terminate_at: Timestamp,
    ) -> Result<(), LocalError> {
        if self.read(id)?.is_none() {
            return Err(LocalError::UnknownInstance(id.clone()));
        }
        let path = self.layout.preemption_path(id);
        write_private(&path, &to_json(&path, &terminate_at)?)?;
        Ok(())
    }

    /// Returns the recorded termination time, or `None` when no interruption was recorded.
    pub(crate) fn read_preemption(&self, id: &InstanceId) -> Result<Option<Timestamp>, LocalError> {
        read_json(&self.layout.preemption_path(id))
    }
}

/// Writes a record through its verified containing directory.
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), LocalError> {
    let parent = path
        .parent()
        .ok_or_else(|| PathIo::new("write", path, io::ErrorKind::InvalidInput.into()))?;
    let name = path
        .file_name()
        .ok_or_else(|| PathIo::new("write", path, io::ErrorKind::InvalidInput.into()))?;
    ensure_private_dir(parent)?.write_atomic(name, bytes, PRIVATE_FILE)?;
    Ok(())
}

fn write_private_checked(mutation: &Mutation, path: &Path, bytes: &[u8]) -> Result<(), LocalError> {
    let parent = path
        .parent()
        .ok_or_else(|| PathIo::new("write", path, io::ErrorKind::InvalidInput.into()))?;
    let name = path
        .file_name()
        .ok_or_else(|| PathIo::new("write", path, io::ErrorKind::InvalidInput.into()))?;
    ensure_private_dir(parent)?.write_atomic_checked::<LocalError>(
        name,
        bytes,
        PRIVATE_FILE,
        || mutation.check(),
    )
}

/// Reads the record behind one `instances_dir` entry. `None` skips the entry: it is not a directory,
/// its name is not an instance id, or it holds no record.
fn record_of_entry(
    store: &RecordStore,
    dir: &Path,
    entry: io::Result<fs::DirEntry>,
) -> Option<Result<InstanceRecord, LocalError>> {
    let entry = match entry {
        Ok(entry) => entry,
        Err(source) => {
            return Some(Err(LocalError::Io(PathIo::new(
                "read directory",
                dir,
                source,
            ))));
        }
    };
    let entry_path = entry.path();
    let is_dir = match entry.file_type() {
        Ok(file_type) => file_type.is_dir(),
        Err(source) => {
            return Some(Err(LocalError::Io(PathIo::new(
                "read",
                &entry_path,
                source,
            ))));
        }
    };
    if !is_dir {
        return None;
    }
    let id = InstanceId::try_new(entry.file_name().into_string().ok()?).ok()?;
    store.read(&id).transpose()
}

fn parse_count<T: FromStr>(path: &Path, text: &str) -> Result<T, LocalError> {
    let trimmed = text.trim();
    trimmed.parse().map_err(|_| {
        LocalError::Io(PathIo::new(
            "parse count",
            path,
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("`{trimmed}` is not a decimal count"),
            ),
        ))
    })
}

fn to_json<T: Serialize>(path: &Path, value: &T) -> Result<Vec<u8>, LocalError> {
    serde_json::to_vec(value).map_err(|source| LocalError::State {
        path: path.to_path_buf(),
        source,
    })
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, LocalError> {
    let Some(bytes) = read_optional(path)? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| LocalError::State {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Lays out the shared directories in a fresh `state` child of the temporary `root`, as the
    /// provisioner does at startup. The child is created `0700`; `root` itself is never configured.
    fn layout_over(root: &Path) -> Result<StateLayout, LocalError> {
        let layout = StateLayout::new(root.join("state"));
        layout.prepare()?;
        Ok(layout)
    }

    fn store_over(root: &Path) -> Result<RecordStore, LocalError> {
        Ok(RecordStore::new(layout_over(root)?))
    }

    /// Writes `record` the way the fleet does.
    fn write_record(store: &RecordStore, record: &InstanceRecord) -> TestResult {
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        store.write(&mutation, record)?;
        Ok(())
    }

    fn all_records(store: &RecordStore) -> Result<Vec<InstanceRecord>, LocalError> {
        store.records()?.collect()
    }

    /// Compares all durable fields without requiring equality on the secret.
    fn assert_record_eq(actual: &InstanceRecord, expected: &InstanceRecord) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.fleet, expected.fleet);
        assert_eq!(actual.isa, expected.isa);
        assert_eq!(actual.pid, expected.pid);
        assert_eq!(actual.launch_cgroup, expected.launch_cgroup);
        assert_eq!(actual.launched_at, expected.launched_at);
        assert_eq!(actual.exited_at, expected.exited_at);
        assert_eq!(actual.terminating_since, expected.terminating_since);
        assert!(actual.secret.matches(expected.secret.expose()));
    }

    fn record(id: &str, launched_at_ms: u64) -> Result<InstanceRecord, Box<dyn std::error::Error>> {
        Ok(InstanceRecord {
            id: InstanceId::try_new(id)?,
            fleet: FleetName::try_new("workers-x86")?,
            isa: Isa::X86_64,
            pid: Some(4242),
            launch_cgroup: None,
            launched_at: Timestamp::from_unix_millis(launched_at_ms),
            exited_at: None,
            terminating_since: None,
            secret: InstanceSecret::new("secret"),
        })
    }

    #[test]
    fn an_expired_permission_writes_nothing() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let valid = store.mutation(scope.deadline())?;
        let original = record("local-1", 1_000)?;
        let mut changed = original.clone();
        changed.pid = Some(9999);
        store.write(&valid, &original)?;
        store.set_desired(&valid, &original.fleet, 2)?;
        let previous_exit = Timestamp::from_unix_millis(5_000);
        store.note_early_exit(&valid, &original.fleet, previous_exit)?;
        assert_eq!(store.next_instance(&valid)?.as_str(), "local-1");
        scope.clock().advance(std::time::Duration::from_secs(1));

        assert!(matches!(
            store.write(&valid, &changed),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            store.remove(&valid, &original.id),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            store.set_desired(&valid, &original.fleet, 3),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            store.note_early_exit(&valid, &original.fleet, Timestamp::from_unix_millis(6_000)),
            Err(LocalError::LeaseLapsed(_))
        ));
        assert!(matches!(
            store.next_instance(&valid),
            Err(LocalError::LeaseLapsed(_))
        ));

        assert_record_eq(
            &store.read(&original.id)?.ok_or("missing record")?,
            &original,
        );
        assert_eq!(store.desired(&original.fleet)?, 2);
        assert_eq!(store.early_exit(&original.fleet)?, Some(previous_exit));
        drop(valid);
        let next_scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let next = store.mutation(next_scope.deadline())?;
        assert_eq!(store.next_instance(&next)?.as_str(), "local-2");
        Ok(())
    }

    #[tokio::test]
    async fn counter_read_crossing_deadline_cannot_publish() -> TestResult {
        use std::io::Write as _;
        use std::time::Duration;

        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let path = store.layout.counter_path();
        gang_os::fs::create_private_fifo(&path)?;

        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let deadline = scope.deadline().clone();
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || {
            let mutation = worker_store.mutation(&deadline)?;
            worker_store.next_instance(&mutation)
        });

        // Opening the writer rendezvous with the actual counter read.
        let mut writer = std::fs::OpenOptions::new().write(true).open(&path)?;
        std::fs::remove_file(&path)?;
        std::fs::write(&path, b"100")?;

        scope.clock().advance(Duration::from_secs(1));
        writer.write_all(b"1")?;
        drop(writer); // EOF releases the blocked read.
        let result = worker.join().expect("counter reader panicked");

        assert!(matches!(result, Err(LocalError::LeaseLapsed(_))));
        assert_eq!(std::fs::read_to_string(&path)?, "100");
        Ok(())
    }

    #[test]
    fn write_then_read_and_list_round_trip() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let mut written = record("local-1", 1_000)?;
        written.exited_at = Some(Timestamp::from_unix_millis(1_500));

        write_record(&store, &written)?;

        assert_record_eq(&store.read(&written.id)?.ok_or("missing record")?, &written);
        let listed = all_records(&store)?;
        assert_eq!(listed.len(), 1);
        assert_record_eq(&listed[0], &written);
        Ok(())
    }

    #[test]
    fn a_record_without_a_pid_round_trips() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let mut written = record("local-1", 1_000)?;
        written.pid = None;

        write_record(&store, &written)?;

        assert_record_eq(&store.read(&written.id)?.ok_or("missing record")?, &written);
        Ok(())
    }

    #[test]
    fn launch_cgroup_survives_write_and_pid_commit() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let mut written = record("local-1", 1_000)?;
        written.pid = None;
        written.launch_cgroup = Some(PathBuf::from("/sys/fs/cgroup/controller.scope"));
        write_record(&store, &written)?;
        assert_record_eq(&store.read(&written.id)?.ok_or("missing record")?, &written);

        RecordStore::commit_pid(&store.layout.record_path(&written.id), 4242)?;
        written.pid = Some(4242);
        assert_record_eq(&store.read(&written.id)?.ok_or("missing record")?, &written);
        Ok(())
    }

    #[test]
    fn legacy_and_null_launch_cgroup_are_unknown() -> TestResult {
        let written = record("local-1", 1_000)?;
        let mut json = serde_json::to_value(&written)?;
        let object = json.as_object_mut().ok_or("record is not an object")?;
        object.remove("launch_cgroup");
        let legacy: InstanceRecord = serde_json::from_value(json.clone())?;
        assert_eq!(legacy.launch_cgroup, None);

        json["launch_cgroup"] = serde_json::Value::Null;
        let explicit_null: InstanceRecord = serde_json::from_value(json)?;
        assert_eq!(explicit_null.launch_cgroup, None);
        Ok(())
    }

    #[test]
    fn a_recorded_worker_cgroup_reads_back_and_is_absent_until_written() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;
        let id = &written.id;

        assert_eq!(store.worker_cgroup(id)?, None);
        let cgroup = PathBuf::from("/fake/cgroup/run-1.scope");
        store.write_worker_cgroup(id, &cgroup)?;
        assert_eq!(store.worker_cgroup(id)?, Some(cgroup));
        Ok(())
    }

    #[test]
    fn secret_round_trips_through_the_record_file() -> TestResult {
        let dir = tempfile::tempdir()?;
        let written = record("local-1", 1_000)?;
        write_record(&store_over(dir.path())?, &written)?;

        let read = store_over(dir.path())?
            .read(&written.id)?
            .ok_or("the record was not written")?;

        assert!(read.secret.matches("secret"));
        Ok(())
    }

    #[test]
    fn terminating_since_survives_a_restart() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mut terminating = record("local-1", 1_000)?;
        terminating.terminating_since = Some(Timestamp::from_unix_millis(5_000));
        write_record(&store_over(dir.path())?, &terminating)?;

        let restarted = store_over(dir.path())?.read(&terminating.id)?;

        let found = restarted.ok_or("missing record")?;
        assert_record_eq(&found, &terminating);
        assert!(found.is_terminating());
        Ok(())
    }

    #[test]
    fn remove_deletes_the_scratch_too() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let layout = layout_over(dir.path())?;
        let store = RecordStore::new(layout.clone());
        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;
        let scratch = layout.scratch_dir(&written.id);
        fs::create_dir_all(&scratch)?;
        fs::write(scratch.join("blob"), b"data")?;

        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        store.remove(&mutation, &written.id)?;

        assert!(!layout.instance_dir(&written.id).exists());
        assert!(store.read(&written.id)?.is_none());
        store.remove(&mutation, &written.id)?;
        Ok(())
    }

    /// Restores owner access to the listed directories, parents first, so a failed removal leaves
    /// nothing the temporary directories cannot delete.
    struct OwnerAccess(Vec<PathBuf>);

    impl Drop for OwnerAccess {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            for path in &self.0 {
                if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
                    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
                }
            }
        }
    }

    /// A worker's scratch with owned read-only and sealed directories and a link to an external
    /// read-only directory is removed on Linux. Apple cannot pin a sealed directory, so removal
    /// fails closed there, naming it and leaving its mode and contents untouched. The linked
    /// directory is never followed on either.
    #[test]
    fn remove_clears_a_read_only_scratch_without_touching_a_linked_directory() -> TestResult {
        use std::os::unix::fs::PermissionsExt;
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        let layout = layout_over(dir.path())?;
        let store = RecordStore::new(layout.clone());
        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;
        let external = outside.path().join("external");
        let scratch = layout.scratch_dir(&written.id);
        let read_only = scratch.join("action").join("read-only");
        let sealed = read_only.join("sealed");
        let _access = OwnerAccess(vec![
            external.clone(),
            scratch.clone(),
            read_only.clone(),
            sealed.clone(),
        ]);
        fs::create_dir(&external)?;
        fs::write(external.join("data"), b"external data")?;
        fs::create_dir_all(&sealed)?;
        fs::write(read_only.join("file"), b"stale")?;
        fs::write(sealed.join("file"), b"stale")?;
        std::os::unix::fs::symlink(&external, read_only.join("external"))?;
        fs::set_permissions(&external, fs::Permissions::from_mode(0o555))?;
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o000))?;
        fs::set_permissions(&read_only, fs::Permissions::from_mode(0o555))?;
        fs::set_permissions(&scratch, fs::Permissions::from_mode(0o555))?;

        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        let removed = store.remove(&mutation, &written.id);

        if cfg!(target_os = "linux") {
            removed?;
            assert_eq!(
                fs::symlink_metadata(layout.instance_dir(&written.id))
                    .map_err(|error| error.kind())
                    .err(),
                Some(io::ErrorKind::NotFound)
            );
            assert!(store.read(&written.id)?.is_none());
        } else {
            let error = match removed {
                Err(LocalError::Io(error)) => error,
                other => {
                    return Err(
                        format!("a sealed directory cannot be pinned on Apple: {other:?}").into(),
                    );
                }
            };
            assert_eq!(error.path, sealed);
            assert_eq!(error.source.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(
                fs::symlink_metadata(&sealed)?.permissions().mode() & 0o777,
                0o000
            );
            fs::set_permissions(&sealed, fs::Permissions::from_mode(0o700))?;
            assert_eq!(fs::read(sealed.join("file"))?, b"stale");
        }
        assert_eq!(fs::metadata(&external)?.permissions().mode() & 0o777, 0o555);
        assert_eq!(fs::read(external.join("data"))?, b"external data");
        assert_eq!(fs::read_dir(&external)?.count(), 1);
        Ok(())
    }

    #[test]
    fn exits_survive_retirement_and_restart_until_acknowledged() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let written = record("local-1", 1_000)?;
        let mut exits = Vec::new();
        store.fill_exits(&written.fleet, &mut exits)?;
        assert!(exits.is_empty());

        write_record(&store, &written)?;
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        store.record_exit(&mutation, &written.fleet, &written.id, "worker failed")?;
        store.fill_exits(&written.fleet, &mut exits)?;
        assert!(
            exits.is_empty(),
            "an instance still being retired must not be acknowledged"
        );
        store.remove(&mutation, &written.id)?;

        let restarted = store_over(dir.path())?;
        restarted.fill_exits(&written.fleet, &mut exits)?;
        assert_eq!(
            exits,
            vec![gang_spi::InstanceExit {
                instance: written.id.clone(),
                failure: "worker failed".to_owned(),
            }]
        );
        restarted.fill_exits(&written.fleet, &mut exits)?;
        assert_eq!(exits.len(), 1);
        restarted.acknowledge_exit(&mutation, &written.fleet, &written.id)?;
        restarted.acknowledge_exit(&mutation, &written.fleet, &written.id)?;
        restarted.fill_exits(&written.fleet, &mut exits)?;
        assert!(exits.is_empty());
        Ok(())
    }

    #[test]
    fn desired_defaults_to_zero_and_persists() -> TestResult {
        let dir = tempfile::tempdir()?;
        let x86 = FleetName::try_new("workers-x86")?;
        let arm = FleetName::try_new("workers-arm")?;
        assert_eq!(store_over(dir.path())?.desired(&x86)?, 0);

        let store = store_over(dir.path())?;
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        store.set_desired(&mutation, &x86, 3)?;

        assert_eq!(store_over(dir.path())?.desired(&x86)?, 3);
        assert_eq!(store_over(dir.path())?.desired(&arm)?, 0);
        Ok(())
    }

    #[test]
    fn release_recovery_replays_partial_writes_once() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        for (record_written, desired_written) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let dir = tempfile::tempdir()?;
            let store = store_over(dir.path())?;
            let initial = record("local-1", 1_000)?;
            write_record(&store, &initial)?;
            let scope = gang_testkit::fence::DeadlineScope::try_new()?;
            let mutation = store.mutation(scope.deadline())?;
            store.set_desired(&mutation, &initial.fleet, 1)?;
            let intent = ReleaseIntent {
                record: InstanceRecord {
                    terminating_since: Some(Timestamp::from_unix_millis(2_000)),
                    ..initial
                },
                desired: 0,
            };
            let path = store
                .layout
                .desired_path(&intent.record.fleet)
                .with_extension("release.json");
            write_private(&path, &to_json(&path, &intent)?)?;
            if record_written {
                store.write(&mutation, &intent.record)?;
            }
            if desired_written {
                store.set_desired(&mutation, &intent.record.fleet, 0)?;
            }

            let restarted = store_over(dir.path())?;
            restarted.recover_release(&mutation, &intent.record.fleet)?;
            restarted.recover_release(&mutation, &intent.record.fleet)?;
            assert_eq!(restarted.desired(&intent.record.fleet)?, 0);
            assert_record_eq(
                &restarted.read(&intent.record.id)?.ok_or("missing record")?,
                &intent.record,
            );
            restarted.set_desired(&mutation, &intent.record.fleet, 3)?;
            restarted.recover_release(&mutation, &intent.record.fleet)?;
            assert_eq!(restarted.desired(&intent.record.fleet)?, 3);
        }
        Ok(())
    }

    #[test]
    fn next_instance_yields_sequential_ids_across_restarts() -> TestResult {
        let dir = tempfile::tempdir()?;

        let store = store_over(dir.path())?;
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        let first = store.next_instance(&mutation)?;
        let second = store_over(dir.path())?.next_instance(&mutation)?;

        assert_eq!(first.as_str(), "local-1");
        assert_eq!(second.as_str(), "local-2");
        Ok(())
    }

    #[test]
    fn stores_on_same_root_serialize_mutations() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let first = store_over(dir.path())?;
        let second = store_over(dir.path())?;
        let first_scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let second_scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let guard = first.mutation(first_scope.deadline())?;
        assert!(matches!(
            second.mutation(second_scope.deadline()),
            Err(LocalError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(guard);
        let _second_guard = second.mutation(second_scope.deadline())?;
        Ok(())
    }

    #[test]
    fn expired_guard_retains_mutation_lock_until_dropped() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let first = store_over(dir.path())?;
        let second = store_over(dir.path())?;
        let first_scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let second_scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let guard = first.mutation(first_scope.deadline())?;
        first_scope
            .clock()
            .advance(std::time::Duration::from_secs(1));
        assert!(matches!(guard.check(), Err(LocalError::LeaseLapsed(_))));
        assert!(matches!(
            second.mutation(second_scope.deadline()),
            Err(LocalError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(guard);
        let _second_guard = second.mutation(second_scope.deadline())?;
        Ok(())
    }

    #[test]
    fn preemption_for_unknown_instance_is_unknown_instance() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let id = InstanceId::try_new("local-9")?;

        let error = store
            .write_preemption(&id, Timestamp::from_unix_millis(5))
            .expect_err("no record exists for the instance");

        assert!(matches!(&error, LocalError::UnknownInstance(found) if found == &id));
        Ok(())
    }

    #[test]
    fn preemption_is_absent_then_round_trips() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;
        assert_eq!(store.read_preemption(&written.id)?, None);

        store.write_preemption(&written.id, Timestamp::from_unix_millis(9_000))?;

        assert_eq!(
            store.read_preemption(&written.id)?,
            Some(Timestamp::from_unix_millis(9_000))
        );
        Ok(())
    }

    #[test]
    fn records_ignores_a_directory_without_a_record() -> TestResult {
        let dir = tempfile::tempdir()?;
        let layout = layout_over(dir.path())?;
        let store = RecordStore::new(layout.clone());
        fs::create_dir_all(layout.instance_dir(&InstanceId::try_new("local-7")?))?;
        assert_eq!(all_records(&store)?.len(), 0);

        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;

        let listed = all_records(&store)?;
        assert_eq!(listed.len(), 1);
        assert_record_eq(&listed[0], &written);
        Ok(())
    }

    #[test]
    fn records_yields_every_record_in_any_order() -> TestResult {
        let _gate = gang_testkit::fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        write_record(&store, &record("local-10", 1_000)?)?;
        write_record(&store, &record("local-1", 2_000)?)?;
        write_record(&store, &record("local-2", 1_000)?)?;

        let mut ids = Vec::new();
        for found in store.records()? {
            ids.push(found?.id.as_str().to_owned());
        }
        ids.sort();

        assert_eq!(ids, ["local-1", "local-10", "local-2"]);
        Ok(())
    }

    #[test]
    fn early_exit_marker_survives_a_restart() -> TestResult {
        let dir = tempfile::tempdir()?;
        let x86 = FleetName::try_new("workers-x86")?;
        let arm = FleetName::try_new("workers-arm")?;
        assert_eq!(store_over(dir.path())?.early_exit(&x86)?, None);

        let store = store_over(dir.path())?;
        let scope = gang_testkit::fence::DeadlineScope::try_new()?;
        let mutation = store.mutation(scope.deadline())?;
        store.note_early_exit(&mutation, &x86, Timestamp::from_unix_millis(7_000))?;

        assert_eq!(
            store_over(dir.path())?.early_exit(&x86)?,
            Some(Timestamp::from_unix_millis(7_000))
        );
        assert_eq!(store_over(dir.path())?.early_exit(&arm)?, None);
        Ok(())
    }
}
