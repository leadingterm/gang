//! Persistent record of the local gang server: its pid, log file and config, kept under the state directory.

use std::path::{Path, PathBuf};

use gang_os::FileLock;
use gang_os::fs::{
    PRIVATE_FILE, PrivateDirError, ensure_private_dir, read_optional, remove_optional,
    require_private_dir, write_atomic,
};
use serde::{Deserialize, Serialize};

use crate::errors::LocalCommandError;

/// Name of the server directory under the state directory.
const SERVER_DIR: &str = "server";
/// Name of the record file inside the server directory.
const RECORD_FILE: &str = "record.json";
/// Name of the configuration copy inside the server directory.
const CONFIG_FILE: &str = "gang.toml";
/// Name of the server log file inside the server directory.
const LOG_FILE: &str = "server.log";
/// Name of the server lifetime lock file inside the server directory.
const LOCK_FILE: &str = "server.lock";

/// What `gang local up` persists about the running server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerRecord {
    /// Process id of the server.
    pub(crate) pid: u32,
    /// Log file the server writes its stdout and stderr to.
    pub(crate) log: PathBuf,
    /// Configuration file the server was started with.
    pub(crate) config: PathBuf,
}

/// The server directory under a state directory, holding the record and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerDirectory {
    /// The state directory from the `[provisioner.local]` section, which the user names.
    state_dir: PathBuf,
    /// `server` directory inside the state directory.
    server_dir: PathBuf,
}

impl ServerDirectory {
    /// Locates the server directory under `state_dir`; nothing is created or read.
    #[must_use]
    pub(crate) fn new(state_dir: &Path) -> Self {
        Self {
            state_dir: state_dir.to_path_buf(),
            server_dir: state_dir.join(SERVER_DIR),
        }
    }

    /// Path of the record file.
    #[must_use]
    pub(crate) fn record_path(&self) -> PathBuf {
        self.server_dir.join(RECORD_FILE)
    }

    /// Path of the server log file.
    #[must_use]
    pub(crate) fn log_path(&self) -> PathBuf {
        self.server_dir.join(LOG_FILE)
    }

    /// Path of the configuration copy the server is started with.
    #[must_use]
    pub(crate) fn config_path(&self) -> PathBuf {
        self.server_dir.join(CONFIG_FILE)
    }

    /// Path of the server lifetime lock, held exclusively by a running server.
    #[must_use]
    pub(crate) fn lock_path(&self) -> PathBuf {
        self.server_dir.join(LOCK_FILE)
    }

    /// Creates the state directory with mode 0700 when it is missing, refusing an existing one
    /// that is not private, then creates or tightens the server directory, which gang owns.
    pub(crate) fn create(&self) -> Result<(), LocalCommandError> {
        require_private_dir(&self.state_dir).map_err(|error| match error {
            PrivateDirError::NotPrivate { path, mode } => {
                LocalCommandError::StateDirNotPrivate { path, mode }
            }
            PrivateDirError::Io(error) => error.into(),
        })?;
        ensure_private_dir(&self.server_dir)?;
        Ok(())
    }

    /// Reads the record; `Ok(None)` when no record exists.
    pub(crate) fn read(&self) -> Result<Option<ServerRecord>, LocalCommandError> {
        let path = self.record_path();
        let Some(bytes) = read_optional(&path)? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| LocalCommandError::Record { path, source })
    }

    /// Atomically replaces the record with its JSON encoding.
    pub(crate) fn write(&self, record: &ServerRecord) -> Result<(), LocalCommandError> {
        let path = self.record_path();
        let bytes = serde_json::to_vec(record).map_err(|source| LocalCommandError::Record {
            path: path.clone(),
            source,
        })?;
        write_atomic(&path, &bytes, PRIVATE_FILE)?;
        Ok(())
    }

    /// Atomically replaces the configuration copy the server is started with.
    pub(crate) fn write_config(&self, text: &str) -> Result<(), LocalCommandError> {
        write_atomic(&self.config_path(), text.as_bytes(), PRIVATE_FILE)?;
        Ok(())
    }

    /// Removes the record; removing an absent record is not an error.
    pub(crate) fn remove(&self) -> Result<(), LocalCommandError> {
        remove_optional(&self.record_path())?;
        Ok(())
    }

    /// Takes the exclusive server lifetime lock; the server directory must already exist (see `create`). The returned lock is held while it stays open.
    pub(crate) fn acquire_lock(&self) -> Result<FileLock, LocalCommandError> {
        let path = self.lock_path();
        FileLock::try_exclusive(&path)?.ok_or(LocalCommandError::AlreadyRunning { lock: path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).expect("metadata").permissions().mode() & 0o777
    }

    fn sample_record(state: &Path, pid: u32) -> ServerRecord {
        ServerRecord {
            pid,
            log: state.join("server").join(LOG_FILE),
            config: state.join("gang.toml"),
        }
    }

    #[test]
    fn log_path_is_server_log_under_the_server_directory() {
        let state = Path::new("/var/lib/gang-state");
        let directory = ServerDirectory::new(state);
        assert_eq!(
            directory.log_path(),
            state.join("server").join("server.log")
        );
    }

    #[test]
    fn record_path_is_record_json_under_the_server_directory() {
        let state = Path::new("/var/lib/gang-state");
        let directory = ServerDirectory::new(state);
        assert_eq!(
            directory.record_path(),
            state.join("server").join("record.json")
        );
    }

    #[test]
    fn create_makes_the_server_directory_with_mode_0700() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        assert!(state.join("server").is_dir());
        assert_eq!(mode(&state.join("server")), 0o700);
    }

    #[test]
    fn create_makes_an_absent_state_directory_with_mode_0700() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        assert!(state.is_dir());
        assert_eq!(mode(&state), 0o700);
    }

    #[test]
    fn create_tightens_an_existing_server_directory_to_mode_0700() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        fs::create_dir(&state).expect("create state dir");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).expect("chmod state");
        let server = state.join("server");
        fs::create_dir(&server).expect("create server dir");
        fs::set_permissions(&server, fs::Permissions::from_mode(0o755)).expect("chmod");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        assert_eq!(mode(&server), 0o700);
    }

    #[test]
    fn create_refuses_a_state_directory_other_users_can_open_and_leaves_it_unchanged() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        fs::create_dir(&state).expect("create state dir");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).expect("chmod");
        let directory = ServerDirectory::new(&state);
        match directory.create() {
            Err(LocalCommandError::StateDirNotPrivate { path, mode }) => {
                assert_eq!(path, state);
                assert_eq!(mode, 0o755);
            }
            other => panic!("expected a state directory refusal, got {other:?}"),
        }
        assert_eq!(mode(&state), 0o755);
        assert!(!state.join("server").exists());
    }

    #[test]
    fn create_accepts_an_existing_private_state_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        fs::create_dir(&state).expect("create state dir");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).expect("chmod");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        assert_eq!(mode(&state.join("server")), 0o700);
    }

    #[test]
    fn read_of_an_absent_record_is_none() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        assert_eq!(directory.read().expect("read"), None);
    }

    #[test]
    fn write_then_read_round_trips_the_record() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        let record = sample_record(&state, 4242);
        directory.write(&record).expect("write");
        assert_eq!(directory.read().expect("read"), Some(record));
    }

    #[test]
    fn write_replaces_an_older_record() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        directory
            .write(&sample_record(&state, 1))
            .expect("first write");
        let newer = sample_record(&state, 2);
        directory.write(&newer).expect("second write");
        assert_eq!(directory.read().expect("read"), Some(newer));
    }

    #[test]
    fn record_file_has_mode_0600() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        directory.write(&sample_record(&state, 7)).expect("write");
        assert_eq!(mode(&directory.record_path()), PRIVATE_FILE);
    }

    #[test]
    fn config_path_is_gang_toml_under_the_server_directory() {
        let state = Path::new("/var/lib/gang-state");
        let directory = ServerDirectory::new(state);
        assert_eq!(
            directory.config_path(),
            state.join("server").join("gang.toml")
        );
    }

    #[test]
    fn write_config_then_read_back_equals_the_text() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        let text = "[server]\nca_pin = \"sha256:00\"\n";
        directory.write_config(text).expect("write config");
        assert_eq!(
            fs::read_to_string(directory.config_path()).expect("read config"),
            text
        );
    }

    #[test]
    fn config_file_has_mode_0600() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        directory.write_config("[server]\n").expect("write config");
        assert_eq!(mode(&directory.config_path()), PRIVATE_FILE);
    }

    #[test]
    fn write_config_replaces_an_older_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        directory.write_config("old = true\n").expect("first write");
        directory
            .write_config("new = true\n")
            .expect("second write");
        assert_eq!(
            fs::read_to_string(directory.config_path()).expect("read config"),
            "new = true\n"
        );
    }

    #[test]
    fn invalid_json_record_is_a_record_error_naming_the_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        fs::write(directory.record_path(), b"{ not json").expect("write garbage");
        match directory.read() {
            Err(LocalCommandError::Record { path, .. }) => {
                assert_eq!(path, directory.record_path());
            }
            other => panic!("expected a record error, got {other:?}"),
        }
    }

    #[test]
    fn record_with_unknown_fields_is_a_record_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        fs::write(
            directory.record_path(),
            br#"{"pid":1,"log":"/tmp/x.log","config":"/tmp/x.toml","extra":true}"#,
        )
        .expect("write record");
        assert!(matches!(
            directory.read(),
            Err(LocalCommandError::Record { .. })
        ));
    }

    #[test]
    fn remove_twice_is_ok() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = temp.path().join("state");
        let directory = ServerDirectory::new(&state);
        directory.create().expect("create");
        directory.write(&sample_record(&state, 9)).expect("write");
        directory.remove().expect("first remove");
        directory.remove().expect("second remove");
        assert_eq!(directory.read().expect("read"), None);
    }

    #[test]
    fn lock_path_is_server_lock_under_the_server_directory() {
        let state = Path::new("/var/lib/gang-state");
        let directory = ServerDirectory::new(state);
        assert_eq!(
            directory.lock_path(),
            state.join("server").join("server.lock")
        );
    }

    #[test]
    fn acquire_lock_then_held_is_true() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        let _lock = directory.acquire_lock().expect("acquire");
        assert!(FileLock::is_held(&directory.lock_path()).expect("is held"));
    }

    #[test]
    fn second_acquire_while_the_first_is_held_is_already_running() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        let _first = directory.acquire_lock().expect("first acquire");
        match directory.acquire_lock() {
            Err(LocalCommandError::AlreadyRunning { lock }) => {
                assert_eq!(lock, directory.lock_path());
            }
            other => panic!("expected already running, got {other:?}"),
        }
    }

    #[test]
    fn dropping_the_lock_file_frees_the_lock() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        let lock = directory.acquire_lock().expect("first acquire");
        drop(lock);
        assert!(!FileLock::is_held(&directory.lock_path()).expect("is held"));
        let _again = directory.acquire_lock().expect("second acquire");
    }

    #[test]
    fn held_without_a_lock_file_is_false() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        directory.create().expect("create");
        assert!(!FileLock::is_held(&directory.lock_path()).expect("is held"));
    }

    #[test]
    fn acquire_lock_does_not_create_the_server_directory() {
        let _gate = gang_testkit::fork_gate::lock_user();
        let temp = tempfile::tempdir().expect("tempdir");
        let directory = ServerDirectory::new(&temp.path().join("state"));
        assert!(matches!(
            directory.acquire_lock(),
            Err(LocalCommandError::Io(_))
        ));
        assert!(!temp.path().join("state").join("server").exists());
    }
}
