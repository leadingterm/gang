use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use gang_clock::Timestamp;
use gang_os::PathIo;
use gang_os::fs::{
    PRIVATE_FILE, ensure_private_dir, read_optional, read_to_string_optional, write_atomic,
};
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
    /// Process id of the worker, the handle used to stop it; `None` from the moment the record is
    /// written until the worker's process has started, so the mock IMDS knows the instance's secret
    /// before the worker can ask for its identity.
    pub pid: Option<u32>,
    /// Wall-clock time the worker was launched.
    pub launched_at: Timestamp,
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

/// Reads and writes instance records, desired capacity, early-exit markers, and the id counter on disk.
///
/// Every call goes to disk, so a restarted process sees exactly what the previous one left.
/// The store creates no directories except each instance directory it writes a record into:
/// [`StateLayout::prepare`] creates the shared ones.
#[derive(Debug, Clone)]
pub(crate) struct RecordStore {
    layout: StateLayout,
}

impl RecordStore {
    /// Creates a store over the directories named by `layout`.
    #[must_use]
    pub(crate) fn new(layout: StateLayout) -> Self {
        Self { layout }
    }

    /// Atomically replaces the instance's record, creating its directory when needed.
    pub(crate) fn write(&self, record: &InstanceRecord) -> Result<(), LocalError> {
        let dir = self.layout.instance_dir(&record.id);
        ensure_private_dir(&dir)?;
        let path = self.layout.record_path(&record.id);
        write_atomic(&path, &to_json(&path, record)?, PRIVATE_FILE)?;
        Ok(())
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
        write_atomic(&path, cgroup.as_os_str().as_bytes(), PRIVATE_FILE)?;
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
    pub(crate) fn remove(&self, id: &InstanceId) -> Result<(), LocalError> {
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
    pub(crate) fn set_desired(&self, fleet: &FleetName, desired: u32) -> Result<(), LocalError> {
        let path = self.layout.desired_path(fleet);
        write_atomic(&path, desired.to_string().as_bytes(), PRIVATE_FILE)?;
        Ok(())
    }

    /// Persists the time of the latest early exit of a `fleet` worker, as decimal Unix
    /// milliseconds.
    pub(crate) fn note_early_exit(
        &self,
        fleet: &FleetName,
        at: Timestamp,
    ) -> Result<(), LocalError> {
        let path = self.layout.early_exit_path(fleet);
        write_atomic(
            &path,
            at.as_unix_millis().to_string().as_bytes(),
            PRIVATE_FILE,
        )?;
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

    /// Hands out the next `local-<n>` id; the persisted counter starts at 1 and survives restarts.
    pub(crate) fn next_instance(&self) -> Result<InstanceId, LocalError> {
        let path = self.layout.counter_path();
        let current: u64 = match read_to_string_optional(&path)? {
            Some(text) => parse_count(&path, &text)?,
            None => 1,
        };
        let next = current.checked_add(1).ok_or_else(|| {
            LocalError::Io(PathIo::new(
                "advance counter",
                &path,
                io::Error::new(io::ErrorKind::InvalidData, "instance counter is exhausted"),
            ))
        })?;
        write_atomic(&path, next.to_string().as_bytes(), PRIVATE_FILE)?;
        let id = InstanceId::try_new(format!("local-{current}"))?;
        Ok(id)
    }

    /// Records a simulated Spot interruption for an existing instance.
    ///
    /// Fails with `UnknownInstance` when the instance has no record.
    pub(crate) fn write_preemption(
        &self,
        id: &InstanceId,
        terminate_at: Timestamp,
    ) -> Result<(), LocalError> {
        if self.read(id)?.is_none() {
            return Err(LocalError::UnknownInstance(id.clone()));
        }
        let path = self.layout.preemption_path(id);
        write_atomic(&path, &to_json(&path, &terminate_at)?, PRIVATE_FILE)?;
        Ok(())
    }

    /// Returns the recorded termination time, or `None` when no interruption was recorded.
    pub(crate) fn read_preemption(&self, id: &InstanceId) -> Result<Option<Timestamp>, LocalError> {
        read_json(&self.layout.preemption_path(id))
    }
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
        store.write(record)?;
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
        assert_eq!(actual.launched_at, expected.launched_at);
        assert_eq!(actual.terminating_since, expected.terminating_since);
        assert!(actual.secret.matches(expected.secret.expose()));
    }

    fn record(id: &str, launched_at_ms: u64) -> Result<InstanceRecord, Box<dyn std::error::Error>> {
        Ok(InstanceRecord {
            id: InstanceId::try_new(id)?,
            fleet: FleetName::try_new("workers-x86")?,
            isa: Isa::X86_64,
            pid: Some(4242),
            launched_at: Timestamp::from_unix_millis(launched_at_ms),
            terminating_since: None,
            secret: InstanceSecret::new("secret"),
        })
    }

    #[test]
    fn write_then_read_and_list_round_trip() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = store_over(dir.path())?;
        let written = record("local-1", 1_000)?;

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

        store.write(&written)?;

        assert_record_eq(&store.read(&written.id)?.ok_or("missing record")?, &written);
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
        let dir = tempfile::tempdir()?;
        let layout = layout_over(dir.path())?;
        let store = RecordStore::new(layout.clone());
        let written = record("local-1", 1_000)?;
        write_record(&store, &written)?;
        let scratch = layout.scratch_dir(&written.id);
        fs::create_dir_all(&scratch)?;
        fs::write(scratch.join("blob"), b"data")?;

        store.remove(&written.id)?;

        assert!(!layout.instance_dir(&written.id).exists());
        assert!(store.read(&written.id)?.is_none());
        store.remove(&written.id)?;
        Ok(())
    }

    #[test]
    fn desired_defaults_to_zero_and_persists() -> TestResult {
        let dir = tempfile::tempdir()?;
        let x86 = FleetName::try_new("workers-x86")?;
        let arm = FleetName::try_new("workers-arm")?;
        assert_eq!(store_over(dir.path())?.desired(&x86)?, 0);

        store_over(dir.path())?.set_desired(&x86, 3)?;

        assert_eq!(store_over(dir.path())?.desired(&x86)?, 3);
        assert_eq!(store_over(dir.path())?.desired(&arm)?, 0);
        Ok(())
    }

    #[test]
    fn next_instance_yields_sequential_ids_across_restarts() -> TestResult {
        let dir = tempfile::tempdir()?;

        let first = store_over(dir.path())?.next_instance()?;
        let second = store_over(dir.path())?.next_instance()?;

        assert_eq!(first.as_str(), "local-1");
        assert_eq!(second.as_str(), "local-2");
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

        store_over(dir.path())?.note_early_exit(&x86, Timestamp::from_unix_millis(7_000))?;

        assert_eq!(
            store_over(dir.path())?.early_exit(&x86)?,
            Some(Timestamp::from_unix_millis(7_000))
        );
        assert_eq!(store_over(dir.path())?.early_exit(&arm)?, None);
        Ok(())
    }
}
