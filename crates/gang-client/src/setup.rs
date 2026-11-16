//! Planning and applying the workspace files owned by `gang setup`.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use gang_clock::Timestamp;
use gang_os::PathIo;
use rustix::fs::{Mode, OFlags};

use crate::ClientError;

/// Directory containing generated Gang workspace files.
pub const GANG_DIR: &str = ".gang";
/// Name of the generated Bazel rc file.
pub const GANG_RC: &str = "bazelrc";
/// Name of the generated CA certificate.
pub const GANG_CA: &str = "ca.pem";
/// The workspace path of the generated Bazel rc, as the import names it.
const IMPORT_PATH: &str = "%workspace%/.gang/bazelrc";
/// Markers recognized as Bazel workspace roots.
const WORKSPACE_MARKERS: &[&str] = &["MODULE.bazel", "REPO.bazel", "WORKSPACE", "WORKSPACE.bazel"];
/// The workspace Bazel rc file.
const BAZELRC: &str = ".bazelrc";
/// The generated directory's ignore file.
const GITIGNORE: &str = ".gitignore";
/// Comment introducing the import.
const IMPORT_COMMENT: &str = "# Gang's Bazel flags, written by `gang setup`.";

/// Finds the nearest Bazel workspace at or above `start`.
///
/// # Errors
/// Returns an error when no workspace marker exists at or above `start`.
pub fn workspace_root(start: &Path) -> Result<PathBuf, ClientError> {
    for dir in start.ancestors() {
        if WORKSPACE_MARKERS
            .iter()
            .any(|marker| dir.join(marker).is_file())
        {
            return Ok(dir.to_path_buf());
        }
    }
    Err(ClientError::NoWorkspace {
        start: start.to_path_buf(),
    })
}

/// An exact before-and-after file change, optionally preserving the original in a backup.
#[derive(Clone, Debug)]
pub struct Change {
    /// The destination file.
    pub path: PathBuf,
    /// Original bytes, or none when the file did not exist.
    pub before: Option<Vec<u8>>,
    /// Exact bytes to write.
    pub after: Vec<u8>,
    /// Destination for a create-new backup of the original bytes.
    pub backup: Option<PathBuf>,
}

impl Change {
    /// Whether the destination already contains the planned bytes.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.before.as_deref() == Some(self.after.as_slice())
    }

    /// Creates the backup first and atomically replaces the destination, retaining its mode.
    ///
    /// # Errors
    /// Returns an error if the destination or backup cannot be inspected or written,
    /// or if a backup is requested without original bytes.
    pub fn apply(&self) -> Result<(), ClientError> {
        if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(PathIo::at("create directory", parent))?;
        }
        let changed = || ClientError::FileChanged {
            path: self.path.clone(),
        };
        let mode = if let Some(before) = &self.before {
            let fd = match rustix::fs::open(
                &self.path,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(rustix::io::Errno::NOENT) => return Err(changed()),
                Err(rustix::io::Errno::LOOP) => {
                    let error = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "is a symbolic link; gang setup edits only regular files",
                    );
                    return Err(PathIo::new("open", &self.path, error).into());
                }
                Err(error) => {
                    return Err(PathIo::new("open", &self.path, io::Error::from(error)).into());
                }
            };
            let stat = rustix::fs::fstat(&fd)
                .map_err(|error| PathIo::new("inspect", &self.path, io::Error::from(error)))?;
            let mut file = fs::File::from(fd);
            let mode = Mode::from_raw_mode(stat.st_mode);
            let mut current = Vec::new();
            file.read_to_end(&mut current)
                .map_err(PathIo::at("read", &self.path))?;
            if current != *before {
                return Err(changed());
            }
            mode
        } else {
            match fs::symlink_metadata(&self.path) {
                Ok(_) => return Err(changed()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => gang_os::fs::PUBLIC_FILE,
                Err(error) => return Err(PathIo::new("inspect", &self.path, error).into()),
            }
        };
        if let Some(backup) = &self.backup {
            let before = self.before.as_ref().ok_or_else(|| {
                PathIo::new(
                    "create backup",
                    backup,
                    io::Error::new(io::ErrorKind::InvalidInput, "no original file to back up"),
                )
            })?;
            let mut file = rustix::fs::open(
                backup,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
                mode & gang_os::fs::PRIVATE_FILE,
            )
            .map(fs::File::from)
            .map_err(|error| PathIo::new("create backup", backup, io::Error::from(error)))?;
            file.write_all(before)
                .map_err(PathIo::at("write backup", backup))?;
            file.sync_all().map_err(PathIo::at("sync backup", backup))?;
            let parent = backup
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let dir = rustix::fs::open(
                parent,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| {
                PathIo::new("open backup directory", parent, io::Error::from(error))
            })?;
            fs::File::from(dir)
                .sync_all()
                .map_err(PathIo::at("sync backup directory", parent))?;
        }
        gang_os::fs::write_atomic(&self.path, &self.after, mode)?;
        Ok(())
    }
}

/// Reads a destination without treating an absent file as an error.
fn read_before(path: &Path) -> Result<Option<Vec<u8>>, ClientError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PathIo::new("read", path, error).into()),
    }
}

/// Plans changed Gang-owned files, omitting files already equal to their desired content.
///
/// # Errors
/// Returns an error if an existing destination cannot be read.
pub fn gang_files(
    root: &Path,
    rc_text: &str,
    ca_pem: Option<&str>,
) -> Result<Vec<Change>, ClientError> {
    let dir = root.join(GANG_DIR);
    let mut files = vec![
        (GANG_RC, rc_text.as_bytes()),
        (GITIGNORE, b"*\n".as_slice()),
    ];
    if let Some(ca) = ca_pem {
        files.push((GANG_CA, ca.as_bytes()));
    }
    let mut changes = Vec::new();
    for (name, bytes) in files {
        let path = dir.join(name);
        let change = Change {
            before: read_before(&path)?,
            path,
            after: bytes.to_vec(),
            backup: None,
        };
        if !change.is_noop() {
            changes.push(change);
        }
    }
    Ok(changes)
}

/// Plans the workspace import, preserving all existing bytes and its first line ending.
///
/// # Errors
/// Returns an error if the existing workspace rc file cannot be read.
pub fn import_change(root: &Path) -> Result<Option<Change>, ClientError> {
    let path = root.join(BAZELRC);
    let before = read_before(&path)?;
    if before.as_ref().is_some_and(|bytes| {
        crate::rc_file::RcFile::parse(bytes).entries.iter().any(|entry| {
            matches!(entry.tokens.as_slice(), [command, path]
                if (command.value.as_slice() == b"try-import" || command.value.as_slice() == b"import")
                    && path.value.as_slice() == IMPORT_PATH.as_bytes())
        })
    }) {
        return Ok(None);
    }
    let mut after = before.clone().unwrap_or_default();
    let ending = if after
        .iter()
        .position(|b| *b == b'\n')
        .is_some_and(|index| index > 0 && after[index - 1] == b'\r')
    {
        "\r\n"
    } else {
        "\n"
    };
    if !after.is_empty() && !after.ends_with(b"\n") {
        after.extend_from_slice(ending.as_bytes());
    }
    after.extend_from_slice(IMPORT_COMMENT.as_bytes());
    after.extend_from_slice(ending.as_bytes());
    after.extend_from_slice(b"try-import ");
    after.extend_from_slice(IMPORT_PATH.as_bytes());
    after.extend_from_slice(ending.as_bytes());
    Ok(Some(Change {
        path,
        before,
        after,
        backup: None,
    }))
}

/// Constructs a UTC timestamped backup path without discarding non-UTF-8 filenames.
#[must_use]
pub fn backup_path(path: &Path, now: Timestamp) -> PathBuf {
    let timestamp: String = now
        .to_string()
        .chars()
        .take(19)
        .filter(|c| *c != '-' && *c != ':')
        .collect();
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".gang-backup-{timestamp}Z"));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn roots_find_each_marker_and_reject_missing() {
        for marker in WORKSPACE_MARKERS {
            let temp = tempfile::tempdir().expect("temp dir");
            let nested = temp.path().join("a/b");
            fs::create_dir_all(&nested).expect("nested dir");
            assert!(matches!(
                workspace_root(&nested),
                Err(ClientError::NoWorkspace { .. })
            ));
            fs::write(temp.path().join(marker), "").expect("marker");
            assert_eq!(workspace_root(&nested).expect("root"), temp.path());
        }
    }

    #[test]
    fn plans_owned_files_and_omits_unchanged() {
        let temp = tempfile::tempdir().expect("temp dir");
        let rc = format!(
            "build --tls_certificate={}\n",
            temp.path().join(GANG_DIR).join(GANG_CA).display()
        );
        let changes = gang_files(temp.path(), &rc, Some("cert\n")).expect("plan");
        assert_eq!(changes.len(), 3);
        assert!(
            changes
                .iter()
                .any(|c| c.path.ends_with(GANG_RC) && c.after == rc.as_bytes())
        );
        assert!(
            changes
                .iter()
                .any(|c| c.path.ends_with(GANG_CA) && c.after == b"cert\n")
        );
        assert!(
            changes
                .iter()
                .any(|c| c.path.ends_with(GITIGNORE) && c.after == b"*\n")
        );
        for change in &changes {
            change.apply().expect("apply");
        }
        assert!(
            gang_files(temp.path(), &rc, Some("cert\n"))
                .expect("plan")
                .is_empty()
        );
    }

    #[test]
    fn import_preserves_existing_bytes_and_line_endings() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join(BAZELRC);
        for (before, separator, ending) in [
            (None, "", "\n"),
            (Some("build --foo\n"), "", "\n"),
            (Some("build --foo"), "\n", "\n"),
            (Some("build --foo\r\nother"), "\r\n", "\r\n"),
        ] {
            if let Some(text) = before {
                fs::write(&path, text).expect("write");
            } else if path.exists() {
                fs::remove_file(&path).expect("remove");
            }
            let change = import_change(temp.path()).expect("plan").expect("change");
            let expected = format!(
                "{}{separator}{IMPORT_COMMENT}{ending}try-import {IMPORT_PATH}{ending}",
                before.unwrap_or("")
            );
            assert_eq!(change.after, expected.as_bytes());
            change.apply().expect("apply");
            assert!(import_change(temp.path()).expect("plan").is_none());
        }
    }

    #[test]
    fn apply_preserves_mode_and_never_overwrites_backup() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("file");
        fs::write(&path, b"old").expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).expect("chmod");
        let backup = backup_path(&path, Timestamp::UNIX_EPOCH);
        assert!(backup.ends_with("file.gang-backup-19700101T000000Z"));
        let change = Change {
            path: path.clone(),
            before: Some(b"old".to_vec()),
            after: b"new".to_vec(),
            backup: Some(backup.clone()),
        };
        change.apply().expect("apply");
        assert_eq!(fs::read(&path).expect("read"), b"new");
        assert_eq!(fs::read(&backup).expect("read backup"), b"old");
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o640
        );
        assert!(change.apply().is_err());
        assert_eq!(fs::read(&backup).expect("read backup"), b"old");
    }

    /// Existing imports are recognized using Bazel's rc tokenization.
    #[test]
    fn import_recognizes_existing_tokenized_imports() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join(BAZELRC);
        for line in [
            "try-import %workspace%/.gang/bazelrc # note\n",
            "try-import   %workspace%/.gang/bazelrc\n",
            "import %workspace%/.gang/bazelrc\n",
        ] {
            fs::write(&path, line).expect("write");
            assert!(
                import_change(temp.path()).expect("plan").is_none(),
                "existing import must not be duplicated: {line:?}"
            );
        }
    }

    /// Backups cannot grant permissions absent from the original or expose group/other bits.
    #[test]
    fn backup_permissions_are_private() {
        let temp = tempfile::tempdir().expect("temp dir");
        for mode in [0o640, 0o644] {
            let path = temp.path().join(format!("file-{mode:o}"));
            let backup = temp.path().join(format!("backup-{mode:o}"));
            fs::write(&path, b"old").expect("write");
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("chmod");
            Change {
                path,
                before: Some(b"old".to_vec()),
                after: b"new".to_vec(),
                backup: Some(backup.clone()),
            }
            .apply()
            .expect("apply");
            let actual = fs::metadata(&backup)
                .expect("backup metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                actual & !(mode & 0o600),
                0,
                "backup of {mode:04o} original must not grant group or other permissions"
            );
        }
    }

    /// A changed destination must remain untouched without producing a stale backup.
    #[test]
    fn apply_rejects_file_changed_since_preview() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("file");
        let backup = temp.path().join("backup");
        fs::write(&path, b"changed").expect("write");
        let change = Change {
            path: path.clone(),
            before: Some(b"old".to_vec()),
            after: b"new".to_vec(),
            backup: Some(backup.clone()),
        };
        let result = change.apply();
        assert!(
            result.is_err(),
            "apply must reject a file changed since preview"
        );
        assert!(
            matches!(result, Err(ClientError::FileChanged { path: changed }) if changed == path)
        );
        assert_eq!(fs::read(&path).expect("read"), b"changed");
        assert!(!backup.exists(), "a stale preview must not create a backup");
    }

    /// A file that appeared after preview, including a dangling symlink, is not overwritten.
    #[test]
    fn apply_rejects_new_destination_and_symlink() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("file");
        let change = Change {
            path: path.clone(),
            before: None,
            after: b"new".to_vec(),
            backup: None,
        };
        fs::write(&path, b"appeared").expect("write");
        assert!(
            matches!(change.apply(), Err(ClientError::FileChanged { .. })),
            "apply must reject a newly created file"
        );
        fs::remove_file(&path).expect("remove");
        std::os::unix::fs::symlink(temp.path().join("missing"), &path).expect("symlink");
        assert!(
            matches!(change.apply(), Err(ClientError::FileChanged { .. })),
            "apply must reject a newly created symlink"
        );
        let existing = Change {
            before: Some(b"old".to_vec()),
            ..change
        };
        let refused = existing.apply();
        assert!(
            matches!(&refused, Err(ClientError::Io(error)) if error.source.to_string().contains("symbolic link")),
            "apply must refuse a symlink in place of the previewed file: {refused:?}"
        );
    }
}
