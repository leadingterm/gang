//! Private files and directories, atomic writes, optional reads and filesystem space.
//!
//! Everything here works on paths and surfaces failures as [`PathIo`], which names the action
//! and the path that failed.

use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, DirBuilder, File, Permissions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use gang_types::Bytes;
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use rustix::process::Uid;
use tempfile::NamedTempFile;

use crate::PathIo;

/// Mode of files created by this module: owner read and write only.
pub const PRIVATE_FILE: u32 = 0o600;

/// Mode of directories created or tightened by this module: owner access only.
pub const PRIVATE_DIR: u32 = 0o700;

/// Creates `path` and any missing parents as private directories, then checks the directory.
///
/// This is for directories Gang owns outright, such as those inside its own state or cache
/// roots. Use [`require_private_dir`] for directories the user names.
///
/// Every directory this call creates is [`PRIVATE_DIR`]. An existing `path` is tightened to
/// [`PRIVATE_DIR`] when its permissions differ; its contents are left untouched. A symbolic link
/// at `path`, or a directory owned by another user, is refused. Existing parent directories are
/// not changed.
///
/// # Errors
///
/// Returns an error when `path` cannot be created (including when it exists as a regular file),
/// when it is a symbolic link or is not owned by the effective user, or when its permissions
/// cannot be set.
pub fn ensure_private_dir(path: &Path) -> Result<(), PathIo> {
    ensure_private_dir_as(path, rustix::process::geteuid())
}

/// Checks that `path` is a private directory, creating it when it is missing.
///
/// This is for directories the user names, in configuration or on the command line. A missing
/// `path` and any missing parents are created as [`PRIVATE_DIR`]. An existing directory is never
/// changed, because tightening a directory the user chose would change permissions they set.
/// A symbolic link, or a directory owned by another user, is refused. Use
/// [`ensure_private_dir`] for directories Gang owns.
///
/// # Errors
///
/// Returns [`PrivateDirError::NotPrivate`] when `path` exists with a mode other than
/// [`PRIVATE_DIR`]. Returns [`PrivateDirError::Io`] when `path` cannot be created, is a symbolic
/// link, or is not owned by the effective user.
pub fn require_private_dir(path: &Path) -> Result<(), PrivateDirError> {
    require_private_dir_as(path, rustix::process::geteuid())
}

/// Why [`require_private_dir`] refused a directory.
#[derive(Debug, thiserror::Error)]
pub enum PrivateDirError {
    /// The directory could not be created, opened or inspected, or it is a symbolic link or is
    /// not owned by the current user.
    #[error(transparent)]
    Io(#[from] PathIo),
    /// The directory exists with a mode other than [`PRIVATE_DIR`]. It was left unchanged.
    #[error("{} has mode {mode:04o}; a private directory must be 0700", .path.display())]
    NotPrivate {
        /// The directory that was refused.
        path: PathBuf,
        /// Its permission bits, as `mode & 0o7777`.
        mode: u32,
    },
}

/// The body of [`ensure_private_dir`], checking ownership against `owner`.
fn ensure_private_dir_as(path: &Path, owner: Uid) -> Result<(), PathIo> {
    DirBuilder::new()
        .recursive(true)
        .mode(PRIVATE_DIR)
        .create(path)
        .map_err(PathIo::at("create directory", path))?;
    let (directory, metadata) = open_owned_dir(path, owner)?;
    tighten_open_dir(&directory, &metadata, path)
}

/// The body of [`require_private_dir`], checking ownership against `owner`.
fn require_private_dir_as(path: &Path, owner: Uid) -> Result<(), PrivateDirError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        DirBuilder::new()
            .recursive(true)
            .mode(PRIVATE_DIR)
            .create(parent)
            .map_err(PathIo::at("create directory", parent))?;
    }
    match DirBuilder::new().mode(PRIVATE_DIR).create(path) {
        Ok(()) => {
            let (directory, metadata) = open_owned_dir(path, owner)?;
            tighten_open_dir(&directory, &metadata, path)?;
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let (_directory, metadata) = open_owned_dir(path, owner)?;
            let mode = metadata.mode() & 0o7777;
            if mode != PRIVATE_DIR {
                return Err(PrivateDirError::NotPrivate {
                    path: path.to_path_buf(),
                    mode,
                });
            }
            Ok(())
        }
        Err(err) => Err(PathIo::new("create directory", path, err).into()),
    }
}

/// Opens `path` as a directory without following a symbolic link; `owner` must own it.
///
/// The owner check runs on the open descriptor, so it applies to the directory that is later
/// changed. Refusals are [`io::ErrorKind::PermissionDenied`] errors with the action
/// `use directory`.
fn open_owned_dir(path: &Path, owner: Uid) -> Result<(File, fs::Metadata), PathIo> {
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|errno| {
        // A symbolic link fails with ELOOP, or with ENOTDIR on Linux because of O_DIRECTORY.
        let link = (errno == Errno::LOOP || errno == Errno::NOTDIR)
            && fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink());
        if link {
            refused(path, "is a symbolic link")
        } else {
            PathIo::new("open directory", path, errno.into())
        }
    })?;
    let directory = File::from(fd);
    let metadata = directory
        .metadata()
        .map_err(PathIo::at("inspect directory", path))?;
    if metadata.uid() != owner.as_raw() {
        return Err(refused(path, "not owned by the current user"));
    }
    Ok((directory, metadata))
}

/// Sets an opened directory to [`PRIVATE_DIR`] unless it already has that mode.
fn tighten_open_dir(directory: &File, metadata: &fs::Metadata, path: &Path) -> Result<(), PathIo> {
    if metadata.mode() & 0o7777 != PRIVATE_DIR {
        directory
            .set_permissions(Permissions::from_mode(PRIVATE_DIR))
            .map_err(PathIo::at("set permissions of directory", path))?;
    }
    Ok(())
}

/// Builds a refusal for `path` that names the action `use directory`.
fn refused(path: &Path, reason: &'static str) -> PathIo {
    PathIo::new(
        "use directory",
        path,
        io::Error::new(io::ErrorKind::PermissionDenied, reason),
    )
}

/// Replaces the file at `path` with `contents`, created with permissions `mode`.
///
/// The bytes are written to a temporary file in the same directory, synced, and renamed over
/// `path`, so readers see either the old file or the complete new one. The temporary file is
/// removed on failure. The file is fsynced; the parent directory is not.
///
/// # Errors
///
/// Returns an error when the temporary file cannot be created in the parent directory, when its
/// permissions cannot be set, when the contents cannot be written or synced, or when the rename
/// fails. Every error names `path`.
pub fn write_atomic(path: &Path, contents: &[u8], mode: u32) -> Result<(), PathIo> {
    let file = write_temp_file(path, contents, mode)?;
    file.persist(path)
        .map_err(|persist| PathIo::new("replace", path, persist.error))?;
    Ok(())
}

/// Writes `contents` to a new temporary file in the parent of `path`, with permissions `mode`,
/// and syncs it. The temporary file is removed when the returned handle is dropped.
///
/// # Errors
///
/// Returns an error when the temporary file cannot be created in the parent directory, when its
/// permissions cannot be set, or when the contents cannot be written or synced. Every error names
/// `path`.
fn write_temp_file(path: &Path, contents: &[u8], mode: u32) -> Result<NamedTempFile, PathIo> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let mut file =
        NamedTempFile::new_in(parent).map_err(PathIo::at("create temporary file for", path))?;
    file.as_file()
        .set_permissions(Permissions::from_mode(mode))
        .map_err(PathIo::at("set permissions of", path))?;
    file.write_all(contents)
        .map_err(PathIo::at("write", path))?;
    file.as_file()
        .sync_all()
        .map_err(PathIo::at("sync", path))?;
    Ok(file)
}

/// Publishes `contents` at `path` only if nothing is there yet, created with permissions `mode`.
///
/// The bytes go to a temporary file in the same directory, are synced, and are then hard-linked
/// to `path`. The link never overwrites, so the first creator wins and any other caller must
/// re-read the winner's file. This is atomic across processes. The temporary file is removed in
/// every outcome. The file is fsynced; the parent directory is neither fsynced nor created.
///
/// Returns `Ok(true)` when this call published the file and `Ok(false)` when a file already
/// existed at `path`; in that case nothing is replaced.
///
/// # Errors
///
/// Returns an error when the temporary file cannot be created in the parent directory, when its
/// permissions cannot be set, when the contents cannot be written or synced, or when publishing
/// fails for any reason other than `path` already existing. Every error names `path`.
pub fn write_new_atomic(path: &Path, contents: &[u8], mode: u32) -> Result<bool, PathIo> {
    let file = write_temp_file(path, contents, mode)?;
    match file.persist_noclobber(path) {
        Ok(_) => Ok(true),
        Err(persist) if persist.error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(persist) => Err(PathIo::new("publish", path, persist.error)),
    }
}

/// Reads the file at `path`, or returns `None` when it does not exist.
///
/// # Errors
///
/// Returns an error for any read failure other than the file being absent.
pub fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, PathIo> {
    absent_as_none(fs::read(path), "read", path)
}

/// Reads the UTF-8 file at `path`, or returns `None` when it does not exist.
///
/// # Errors
///
/// Returns an error for any read failure other than the file being absent, including contents
/// that are not valid UTF-8 (kind [`io::ErrorKind::InvalidData`]).
pub fn read_to_string_optional(path: &Path) -> Result<Option<String>, PathIo> {
    absent_as_none(fs::read_to_string(path), "read", path)
}

/// Removes the file at `path`; a file that does not exist is not an error.
///
/// # Errors
///
/// Returns an error for any removal failure other than the file being absent, such as when
/// `path` is a directory.
pub fn remove_optional(path: &Path) -> Result<(), PathIo> {
    absent_as_none(fs::remove_file(path), "remove", path).map(|_| ())
}

/// The size of a filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    /// Total size of the filesystem's data blocks.
    pub total: Bytes,
    /// Size of the blocks available to an unprivileged user.
    pub available: Bytes,
}

/// Returns the size of the filesystem that holds `path`.
///
/// `available` counts only the blocks an unprivileged user may use, so it can be smaller than
/// the free space that the superuser can see.
///
/// # Errors
///
/// Returns an error when the filesystem cannot be queried, such as when `path` does not exist.
pub fn space(path: &Path) -> Result<Space, PathIo> {
    let stat =
        rustix::fs::statvfs(path).map_err(|errno| PathIo::new("statvfs", path, errno.into()))?;
    Ok(Space {
        total: Bytes::new(stat.f_blocks.saturating_mul(stat.f_frsize)),
        available: Bytes::new(stat.f_bavail.saturating_mul(stat.f_frsize)),
    })
}

/// Size of the units `st_blocks` counts in: 512 bytes on every Unix.
const BLOCK_BYTES: u64 = 512;

/// Returns the bytes allocated to the regular files under `path`.
///
/// Each file counts its allocated blocks once, even when it has multiple hard links under `path`,
/// so a sparse file counts only the blocks it holds. Symbolic links are never followed and never
/// counted. A `path` that does not exist holds no files and is `0` bytes.
///
/// # Errors
///
/// Returns a [`PathIo`] naming the path that cannot be read.
pub fn usage(path: &Path) -> Result<Bytes, PathIo> {
    match absent_as_none(fs::symlink_metadata(path), "inspect", path)? {
        Some(metadata) => Ok(Bytes::new(allocated_under(path, &metadata)?)),
        None => Ok(Bytes::new(0)),
    }
}

/// Sums the allocated bytes under `path`, whose own metadata is `metadata`, without following
/// symbolic links.
fn allocated_under(path: &Path, metadata: &fs::Metadata) -> Result<u64, PathIo> {
    if metadata.is_file() {
        return Ok(metadata.blocks().saturating_mul(BLOCK_BYTES));
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut directories = vec![path.to_path_buf()];
    let mut seen = HashSet::new();
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(PathIo::at("read directory", &directory))? {
            let entry = entry.map_err(PathIo::at("read directory", &directory))?;
            let file_type = entry
                .file_type()
                .map_err(|err| PathIo::new("inspect", &entry.path(), err))?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                let metadata = entry
                    .metadata()
                    .map_err(|err| PathIo::new("inspect", &entry.path(), err))?;
                if metadata.nlink() <= 1 || seen.insert((metadata.dev(), metadata.ino())) {
                    total = total.saturating_add(metadata.blocks().saturating_mul(BLOCK_BYTES));
                }
            }
        }
    }
    Ok(total)
}

/// Maps a missing file to `None` and any other failure to a [`PathIo`] named `action`.
fn absent_as_none<T>(
    result: io::Result<T>,
    action: &'static str,
    path: &Path,
) -> Result<Option<T>, PathIo> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(PathIo::new(action, path, source)),
    }
}

/// Creates a FIFO at `path` with mode 0600 (owner read and write; the umask can only clear bits).
///
/// # Errors
///
/// Returns [`PathIo`] when it cannot be created, for example because `path` exists.
pub fn create_private_fifo(path: &Path) -> Result<(), PathIo> {
    let c = CString::new(path.as_os_str().as_bytes()).map_err(|error| {
        PathIo::new(
            "create fifo",
            path,
            io::Error::new(io::ErrorKind::InvalidInput, error),
        )
    })?;
    crate::sys::mkfifo(&c, 0o600).map_err(PathIo::at("create fifo", path))
}

/// Removes `path` and its contents; a missing directory is not an error.
///
/// # Errors
///
/// Returns an error for any removal failure other than the directory being absent.
pub fn remove_dir_optional(path: &Path) -> Result<(), PathIo> {
    absent_as_none(fs::remove_dir_all(path), "remove directory", path).map(|_| ())
}

/// Atomically replaces the file or symlink at `path` with a symlink to `target`.
///
/// A sibling named `.<file name>.gang-link-<pid>` is created and renamed over `path`.
/// An existing staging entry is not removed or overwritten. A staging link created by this
/// call is removed on rename failure.
///
/// # Errors
///
/// Returns an error if `path` has no file name, the staging link cannot be created, or the
/// rename fails. Every error names `path`.
pub fn replace_symlink(target: &Path, path: &Path) -> Result<(), PathIo> {
    let name = path.file_name().ok_or_else(|| {
        PathIo::new(
            "create symlink",
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"),
        )
    })?;
    let mut temporary_name = std::ffi::OsString::from(".");
    temporary_name.push(name);
    temporary_name.push(format!(".gang-link-{}", std::process::id()));
    let temporary = path.with_file_name(temporary_name);
    std::os::unix::fs::symlink(target, &temporary).map_err(PathIo::at("create symlink", path))?;
    let result = fs::rename(&temporary, path).map_err(PathIo::at("rename file", path));
    if result.is_err() {
        // Preserve the rename error if cleanup also fails.
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::fs::Permissions;
    use std::fs::{self, DirBuilder};
    use std::io;
    use std::io::ErrorKind;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    use std::path::Path;

    use gang_testkit::TestResult;

    use super::*;

    fn mode_of(path: &Path) -> io::Result<u32> {
        Ok(fs::metadata(path)?.permissions().mode() & 0o777)
    }

    #[test]
    fn ensure_private_dir_creates_nested_directories_as_private() -> TestResult {
        let root = tempfile::tempdir()?;
        let outer = root.path().join("outer");
        let inner = outer.join("inner");

        ensure_private_dir(&inner)?;

        assert_eq!(mode_of(&outer)?, 0o700);
        assert_eq!(mode_of(&inner)?, 0o700);
        Ok(())
    }

    #[test]
    fn ensure_private_dir_tightens_an_existing_directory_and_keeps_its_contents() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("existing");
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(0o755))?;
        fs::write(dir.join("keep"), b"kept")?;

        ensure_private_dir(&dir)?;

        assert_eq!(mode_of(&dir)?, 0o700);
        assert_eq!(fs::read(dir.join("keep"))?, b"kept");
        Ok(())
    }

    #[test]
    fn ensure_private_dir_rejects_an_existing_regular_file() -> TestResult {
        let root = tempfile::tempdir()?;
        let file = root.path().join("file");
        fs::write(&file, b"not a directory")?;

        let err = ensure_private_dir(&file).expect_err("a regular file is not a directory");

        assert_eq!(err.action, "create directory");
        Ok(())
    }

    #[test]
    fn ensure_private_dir_refuses_a_directory_owned_by_another_user() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("child");
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(0o755))?;

        let err =
            ensure_private_dir_as(&dir, other_uid()).expect_err("another user owns the directory");

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(err.action, "use directory");
        assert_eq!(mode_of(&dir)?, 0o755);
        Ok(())
    }

    #[test]
    fn ensure_private_dir_refuses_a_symlink_to_an_owned_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        let target = root.path().join("target");
        fs::create_dir(&target)?;
        fs::set_permissions(&target, Permissions::from_mode(0o755))?;
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link)?;

        let err = ensure_private_dir(&link).expect_err("a symbolic link is refused");

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(err.action, "use directory");
        assert_eq!(mode_of(&target)?, 0o755);
        Ok(())
    }

    #[test]
    fn require_private_dir_creates_a_missing_directory_0700() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("a").join("b");

        require_private_dir(&path)?;

        assert_eq!(mode_of(&path)?, 0o700);
        Ok(())
    }

    #[test]
    fn require_private_dir_accepts_an_existing_0700_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("private");
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(PRIVATE_DIR))?;

        require_private_dir(&dir)?;

        assert_eq!(mode_of(&dir)?, PRIVATE_DIR);
        Ok(())
    }

    #[test]
    fn require_private_dir_refuses_a_0755_directory_unchanged() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("open");
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(0o755))?;

        let err = require_private_dir(&dir).expect_err("a 0755 directory is not private");

        assert!(
            matches!(&err, PrivateDirError::NotPrivate { path, mode: 0o755 } if path == &dir),
            "{err}"
        );
        assert_eq!(mode_of(&dir)?, 0o755);
        Ok(())
    }

    #[test]
    fn require_private_dir_refuses_a_foreign_owner() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("private");
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(PRIVATE_DIR))?;

        let err = require_private_dir_as(&dir, other_uid()).expect_err("another user owns it");

        assert!(
            matches!(&err, PrivateDirError::Io(io_err) if io_err.action == "use directory"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn require_private_dir_refuses_a_symlink() -> TestResult {
        let root = tempfile::tempdir()?;
        let target = root.path().join("target");
        fs::create_dir(&target)?;
        fs::set_permissions(&target, Permissions::from_mode(PRIVATE_DIR))?;
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link)?;

        let err = require_private_dir(&link).expect_err("a symbolic link is refused");

        assert!(
            matches!(&err, PrivateDirError::Io(io_err) if io_err.action == "use directory"),
            "{err}"
        );
        Ok(())
    }

    fn other_uid() -> Uid {
        Uid::from_raw(rustix::process::geteuid().as_raw().wrapping_add(1))
    }

    #[test]
    fn existing_parents_are_left_unchanged() -> TestResult {
        let root = tempfile::tempdir()?;
        let parent = root.path().join("parent");
        fs::create_dir(&parent)?;
        fs::set_permissions(&parent, Permissions::from_mode(0o755))?;

        ensure_private_dir(&parent.join("child"))?;

        assert_eq!(mode_of(&parent)?, 0o755);
        assert_eq!(mode_of(&parent.join("child"))?, PRIVATE_DIR);
        Ok(())
    }

    #[test]
    fn write_atomic_creates_a_file_with_mode_0600() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("token");

        write_atomic(&path, b"secret", PRIVATE_FILE)?;

        assert_eq!(mode_of(&path)?, 0o600);
        assert_eq!(fs::read(&path)?, b"secret");
        Ok(())
    }

    #[test]
    fn write_atomic_replaces_existing_content() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("state");
        write_atomic(&path, b"old and longer", PRIVATE_FILE)?;

        write_atomic(&path, b"new", PRIVATE_FILE)?;

        assert_eq!(fs::read(&path)?, b"new");
        Ok(())
    }

    #[test]
    fn write_atomic_replaces_a_0644_file_with_a_0600_one() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("state");
        write_atomic(&path, b"old", 0o644)?;

        write_atomic(&path, b"new", PRIVATE_FILE)?;

        assert_eq!(mode_of(&path)?, 0o600);
        assert_eq!(fs::read(&path)?, b"new");
        Ok(())
    }

    #[test]
    fn write_atomic_leaves_exactly_one_directory_entry() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("state");

        write_atomic(&path, b"one", PRIVATE_FILE)?;
        write_atomic(&path, b"two", PRIVATE_FILE)?;

        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn write_atomic_fails_without_a_temporary_file_when_the_parent_is_missing() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("missing").join("state");

        let err = write_atomic(&path, b"data", PRIVATE_FILE).expect_err("parent does not exist");

        assert!(err.is_not_found());
        assert_eq!(err.action, "create temporary file for");
        assert_eq!(err.path, path);
        Ok(())
    }

    #[test]
    fn write_atomic_failing_at_the_rename_reports_replace_and_leaves_no_temporary() -> TestResult {
        let root = tempfile::tempdir()?;
        let target = root.path().join("target");
        fs::create_dir(&target)?;
        fs::write(target.join("inside"), b"occupied")?;

        let err = write_atomic(&target, b"x", PRIVATE_FILE)
            .expect_err("a non-empty directory cannot be replaced by a file");

        assert_eq!(err.action, "replace");
        assert_eq!(err.path, target);
        let mut names = fs::read_dir(root.path())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<io::Result<Vec<String>>>()?;
        names.sort();
        assert_eq!(names, ["target"]);
        Ok(())
    }

    #[test]
    fn write_new_atomic_creates_a_file_with_mode_0600_and_returns_true() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("token");

        assert!(write_new_atomic(&path, b"secret", PRIVATE_FILE)?);

        assert_eq!(mode_of(&path)?, 0o600);
        assert_eq!(fs::read(&path)?, b"secret");
        Ok(())
    }

    #[test]
    fn write_new_atomic_creates_a_file_with_mode_0644() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("public");

        assert!(write_new_atomic(&path, b"open", 0o644)?);

        assert_eq!(mode_of(&path)?, 0o644);
        assert_eq!(fs::read(&path)?, b"open");
        Ok(())
    }

    #[test]
    fn write_new_atomic_keeps_the_first_file_and_returns_false_for_the_second() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("state");
        assert!(write_new_atomic(&path, b"first", PRIVATE_FILE)?);

        assert!(!write_new_atomic(&path, b"second", PRIVATE_FILE)?);

        assert_eq!(fs::read(&path)?, b"first");
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn write_new_atomic_leaves_no_temporary_file_when_the_path_exists() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("state");
        fs::write(&path, b"existing")?;

        assert!(!write_new_atomic(&path, b"new", PRIVATE_FILE)?);

        assert_eq!(fs::read(&path)?, b"existing");
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn write_new_atomic_fails_without_a_temporary_file_when_the_parent_is_missing() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("missing").join("state");

        let err =
            write_new_atomic(&path, b"data", PRIVATE_FILE).expect_err("parent does not exist");

        assert!(err.is_not_found());
        assert_eq!(err.action, "create temporary file for");
        assert_eq!(err.path, path);
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn read_optional_is_none_for_a_missing_file_and_some_for_a_present_one() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("file");
        assert_eq!(read_optional(&path)?, None);

        fs::write(&path, b"data")?;

        assert_eq!(read_optional(&path)?.as_deref(), Some(&b"data"[..]));
        Ok(())
    }

    #[test]
    fn read_to_string_optional_reads_text_and_reports_invalid_utf8() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("text");
        assert_eq!(read_to_string_optional(&path)?, None);

        fs::write(&path, "héllo")?;
        assert_eq!(read_to_string_optional(&path)?.as_deref(), Some("héllo"));

        fs::write(&path, b"\xff\xfe")?;
        let err = read_to_string_optional(&path).expect_err("contents are not UTF-8");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        Ok(())
    }

    #[test]
    fn remove_optional_ignores_a_missing_file_and_removes_an_existing_one() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("file");
        remove_optional(&path)?;

        fs::write(&path, b"data")?;
        remove_optional(&path)?;

        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn remove_optional_fails_for_a_directory_and_is_not_not_found() -> TestResult {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("dir");
        DirBuilder::new().mode(PRIVATE_DIR).create(&dir)?;

        let err = remove_optional(&dir).expect_err("a directory is not a file");

        assert!(!err.is_not_found());
        assert_eq!(err.action, "remove");
        Ok(())
    }

    #[test]
    fn space_reports_the_filesystem_of_a_directory() -> TestResult {
        let root = tempfile::tempdir()?;

        let size = space(root.path())?;

        assert!(size.total.get() > 0);
        assert!(size.available <= size.total);
        Ok(())
    }

    #[test]
    fn space_of_a_missing_path_is_not_found_from_statvfs() -> TestResult {
        let root = tempfile::tempdir()?;

        let err = space(&root.path().join("missing")).expect_err("path does not exist");

        assert!(err.is_not_found());
        assert_eq!(err.action, "statvfs");
        Ok(())
    }

    #[test]
    fn usage_of_an_empty_directory_is_zero() -> TestResult {
        let root = tempfile::tempdir()?;

        assert_eq!(usage(root.path())?, Bytes::new(0));
        Ok(())
    }

    #[test]
    fn usage_of_a_missing_directory_is_zero() -> TestResult {
        let root = tempfile::tempdir()?;

        assert_eq!(usage(&root.path().join("missing"))?, Bytes::new(0));
        Ok(())
    }

    #[test]
    fn usage_counts_the_allocated_bytes_of_nested_files() -> TestResult {
        let root = tempfile::tempdir()?;
        let nested = root.path().join("a").join("b");
        fs::create_dir_all(&nested)?;
        let top = root.path().join("top");
        let deep = nested.join("deep");
        fs::write(&top, vec![1_u8; 5_000])?;
        fs::write(&deep, vec![2_u8; 70_000])?;

        let total = usage(root.path())?.get();
        let allocated = [&top, &deep]
            .into_iter()
            .map(|file| fs::symlink_metadata(file).map(|m| m.blocks().saturating_mul(512)))
            .sum::<io::Result<u64>>()?;

        assert!(total >= 75_000);
        assert_eq!(total, allocated);
        Ok(())
    }

    #[test]
    fn usage_counts_hard_links_in_different_directories_once() -> TestResult {
        let root = tempfile::tempdir()?;
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::create_dir(&first)?;
        fs::create_dir(&second)?;
        let file = first.join("file");
        fs::write(&file, vec![1_u8; 16_384])?;
        let single_link_usage = usage(root.path())?;
        assert!(single_link_usage.get() > 0);

        fs::hard_link(&file, second.join("link"))?;

        assert_eq!(usage(root.path())?, single_link_usage);
        Ok(())
    }

    #[test]
    fn usage_does_not_count_a_symlink_to_a_file_outside() -> TestResult {
        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        let target = outside.path().join("large");
        fs::write(&target, vec![3_u8; 4 * 1024 * 1024])?;
        std::os::unix::fs::symlink(&target, root.path().join("link"))?;

        assert_eq!(usage(root.path())?, Bytes::new(0));
        Ok(())
    }

    #[test]
    fn remove_dir_optional_accepts_a_missing_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        remove_dir_optional(&root.path().join("missing"))?;
        Ok(())
    }

    #[test]
    fn private_fifo_has_private_mode_and_round_trips_bytes() -> TestResult {
        use std::io::{Read, Write};
        use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};

        let root = tempfile::tempdir()?;
        let path = root.path().join("fifo");
        create_private_fifo(&path)?;
        let metadata = fs::metadata(&path)?;
        assert!(metadata.file_type().is_fifo());
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode & 0o077, 0);
        let baseline = root.path().join("mode-baseline");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&baseline)?;
        assert_eq!(mode & 0o600, fs::metadata(baseline)?.mode() & 0o600);
        assert_eq!(mode & !0o600, 0);

        // An unusually restrictive umask may have removed the owner's access.
        fs::set_permissions(&path, Permissions::from_mode(0o600))?;
        let reader_path = path.clone();
        let reader = std::thread::spawn(move || -> io::Result<[u8; 5]> {
            let mut fifo = File::open(reader_path)?;
            let mut bytes = [0; 5];
            fifo.read_exact(&mut bytes)?;
            Ok(bytes)
        });
        let mut writer = fs::OpenOptions::new().write(true).open(&path)?;
        writer.write_all(b"hello")?;
        let bytes = reader.join().expect("FIFO reader thread must not panic")?;
        assert_eq!(&bytes, b"hello");
        Ok(())
    }

    #[test]
    fn replace_symlink_replaces_an_existing_link() -> TestResult {
        let root = tempfile::tempdir()?;
        let link = root.path().join("entry");
        std::os::unix::fs::symlink("old", &link)?;

        replace_symlink(Path::new("new"), &link)?;

        assert_eq!(fs::read_link(&link)?, Path::new("new"));
        Ok(())
    }

    #[test]
    fn private_fifo_rejects_existing_path_and_names_it() -> TestResult {
        let root = tempfile::tempdir()?;
        let path = root.path().join("existing");
        fs::write(&path, b"existing")?;

        let error = create_private_fifo(&path).expect_err("existing path cannot be created");
        assert_eq!(error.action, "create fifo");
        assert_eq!(error.path, path);
        assert!(error.to_string().contains(&path.display().to_string()));
        Ok(())
    }

    #[test]
    fn replace_symlink_preserves_a_sibling_with_the_tmp_extension() -> TestResult {
        let root = tempfile::tempdir()?;
        let link = root.path().join("entry.exe");
        let sibling = root.path().join("entry.tmp");
        fs::write(&sibling, b"keep")?;

        replace_symlink(Path::new("target"), &link)?;

        assert_eq!(fs::read_link(&link)?, Path::new("target"));
        assert_eq!(fs::read(&sibling)?, b"keep");
        Ok(())
    }

    #[test]
    fn replace_symlink_does_not_remove_an_existing_staging_file() -> TestResult {
        let root = tempfile::tempdir()?;
        let link = root.path().join("entry");
        let staging = root
            .path()
            .join(format!(".entry.gang-link-{}", std::process::id()));
        fs::write(&staging, b"keep")?;

        assert!(replace_symlink(Path::new("target"), &link).is_err());
        assert_eq!(fs::read(&staging)?, b"keep");
        Ok(())
    }

    #[test]
    fn private_fifo_rejects_nul_in_path() -> TestResult {
        use std::ffi::OsStr;

        let path = Path::new(OsStr::from_bytes(b"fifo\0name"));
        let error = create_private_fifo(path).expect_err("NUL cannot occur in a filesystem path");
        assert_eq!(error.path, path);
        assert_eq!(error.source.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }
}
