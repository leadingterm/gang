//! Private files and directories, atomic writes, optional reads and filesystem space.
//!
//! Filesystem failures are surfaced as [`PathIo`], naming the action and path. Private directory
//! capabilities keep the verified descriptor open for subsequent file operations.

use std::collections::{HashSet, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

use gang_types::Bytes;
/// Permission bits, as every mode-taking function of this module expects them.
pub use rustix::fs::Mode;
use rustix::fs::{OFlags, Stat};
use rustix::process::Uid;

use crate::PathIo;

/// Mode of files created by this module: owner read and write only.
pub const PRIVATE_FILE: Mode = Mode::RUSR.union(Mode::WUSR);

/// Mode of files everyone may read: owner read and write, everyone else read only.
pub const PUBLIC_FILE: Mode = PRIVATE_FILE.union(Mode::RGRP).union(Mode::ROTH);

/// Mode of directories created or tightened by this module: owner access only.
pub const PRIVATE_DIR: Mode = Mode::RWXU;

/// The write bits a private directory and a trusted parent must not grant.
const SHARED_WRITE: Mode = Mode::WGRP.union(Mode::WOTH);

/// Creates `path` and any missing parents as private directories, then checks the directory.
///
/// This is for directories Gang owns outright, such as those inside its own state or cache
/// roots. Use [`require_private_dir`] for directories the user names.
///
/// Every directory this call creates is [`PRIVATE_DIR`]. An existing `path` writable by group or
/// others is refused; any other existing `path` is tightened to [`PRIVATE_DIR`] when its
/// permissions differ, leaving its contents untouched. The path is walked without following links
/// implicitly. Every entered directory or link must be owned by the user or the filesystem root's
/// owner. A writable parent must be sticky and its entry owned by the user or the root's owner.
/// `..` is resolved relative to the opened directory. Existing parents are not changed.
///
/// # Errors
///
/// Returns an error when `path` cannot be created (including when it exists as a regular file),
/// when a directory or link on it is owned by another user or sits in a directory others can
/// write, when the final directory is not owned by the effective user or is writable by group or
/// others, or when permissions cannot be set. A sticky writable parent with a trusted entry is
/// allowed.
pub fn ensure_private_dir(path: &Path) -> Result<PrivateDir, PathIo> {
    ensure_private_dir_as(path, rustix::process::geteuid(), Path::new("/"))
}

/// Opens a private directory using the same walk and rules as [`ensure_private_dir`], but creates
/// and changes nothing. An existing directory writable by group or others is refused.
///
/// Returns `Ok(None)` if `path` or a parent is missing.
///
/// # Errors
///
/// Returns an error if the walk fails for a reason other than absence.
pub fn open_private_dir(path: &Path) -> Result<Option<PrivateDir>, PathIo> {
    open_private_dir_as(path, rustix::process::geteuid(), Path::new("/"))
}

/// Checks that `path` is a private directory, creating it when it is missing.
///
/// This is for directories the user names, in configuration or on the command line. A missing
/// `path` and any missing parents are created as [`PRIVATE_DIR`]. An existing directory is never
/// changed, because tightening a directory the user chose would change permissions they set.
/// The same ownership and traversal rules as [`ensure_private_dir`] apply.
///
/// # Errors
///
/// Returns [`PrivateDirError::NotPrivate`] when `path` exists with a mode other than
/// [`PRIVATE_DIR`]. Returns [`PrivateDirError::Io`] when `path` cannot be created, a directory or
/// link on it is owned by another user or sits in a directory others can write, or the final
/// directory is not owned by the effective user. A sticky writable parent with a trusted entry is
/// allowed.
pub fn require_private_dir(path: &Path) -> Result<PrivateDir, PrivateDirError> {
    require_private_dir_as(path, rustix::process::geteuid(), Path::new("/"))
}

/// Why [`require_private_dir`] refused a directory.
#[derive(Debug, thiserror::Error)]
pub enum PrivateDirError {
    /// The directory could not be created, opened or inspected, or its path was unsafe.
    #[error(transparent)]
    Io(#[from] PathIo),
    /// The directory exists with a mode other than [`PRIVATE_DIR`]. It was left unchanged.
    #[error("{} has mode {mode:04o}; a private directory must be 0700", .path.display())]
    NotPrivate {
        /// The directory that was refused.
        path: PathBuf,
        /// Its permission bits.
        mode: Mode,
    },
}

/// The body of [`ensure_private_dir`], checking ownership against `owner`.
///
/// `trusted` bounds the checks; directories outside it are not inspected.
fn ensure_private_dir_as(path: &Path, owner: Uid, trusted: &Path) -> Result<PrivateDir, PathIo> {
    open_private_dir_walk(path, owner, trusted, true)
}

/// Opens an existing private directory using the same checks as [`ensure_private_dir_as`].
fn open_private_dir_as(
    path: &Path,
    owner: Uid,
    trusted: &Path,
) -> Result<Option<PrivateDir>, PathIo> {
    match open_private_dir_walk(path, owner, trusted, false) {
        Ok(directory) => Ok(Some(directory)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Walks to a private directory, optionally creating missing components and tightening the leaf.
fn open_private_dir_walk(
    path: &Path,
    owner: Uid,
    trusted: &Path,
    create: bool,
) -> Result<PrivateDir, PathIo> {
    let opened = open_owned_dir(path, owner, trusted, create)?;
    if Mode::from_raw_mode(opened.stat.st_mode).intersects(SHARED_WRITE) {
        return Err(refused(path, "is writable by group or others"));
    }
    if create {
        tighten_open_dir(&opened.directory, &opened.stat, path)?;
    }
    Ok(PrivateDir {
        directory: opened.directory,
        path: path.to_path_buf(),
    })
}

/// The body of [`require_private_dir`], checking ownership against `owner`.
///
/// `trusted` bounds the checks; directories outside it are not inspected.
fn require_private_dir_as(
    path: &Path,
    owner: Uid,
    trusted: &Path,
) -> Result<PrivateDir, PrivateDirError> {
    let opened = open_owned_dir(path, owner, trusted, true)?;
    if opened.created {
        tighten_open_dir(&opened.directory, &opened.stat, path)?;
        return Ok(PrivateDir {
            directory: opened.directory,
            path: path.to_path_buf(),
        });
    }
    let mode = Mode::from_raw_mode(opened.stat.st_mode);
    if mode != PRIVATE_DIR {
        return Err(PrivateDirError::NotPrivate {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(PrivateDir {
        directory: opened.directory,
        path: path.to_path_buf(),
    })
}

/// The most symbolic links one walk follows, as Linux's `MAXSYMLINKS`.
const MAX_LINKS: usize = 40;

/// A directory opened by [`open_owned_dir`].
struct OpenedDir {
    /// The open directory.
    directory: File,
    /// Its status, read from the descriptor.
    stat: Stat,
    /// Whether this call created it.
    created: bool,
}

/// One directory of a walk, held open.
struct Step {
    /// The open directory.
    directory: File,
    /// Its status, read from the descriptor.
    stat: Stat,
    /// Its physical path, which names it in refusals.
    path: PathBuf,
}

/// Opens `path` as a directory that `owner` owns, optionally creating missing components as
/// [`PRIVATE_DIR`].
///
/// The walk starts at `/` and opens one component at a time relative to the directory before it,
/// never following a link implicitly, so every check is made on the directory that is then used.
/// Every parent and entered entry is checked before descent, link resolution, or creation.
/// Refusals are permission errors naming the offending directory.
///
/// `trusted` bounds the checks: directories outside it are not inspected. Production passes the
/// filesystem root.
fn open_owned_dir(
    path: &Path,
    owner: Uid,
    trusted: &Path,
    create: bool,
) -> Result<OpenedDir, PathIo> {
    let absolute = std::path::absolute(path).map_err(PathIo::at("resolve directory", path))?;
    let root = Path::new("/");
    let root_step = open_step(None, root, root)?;
    let root_owner = root_step.stat.st_uid;
    let mut current = Step {
        directory: root_step
            .directory
            .try_clone()
            .map_err(PathIo::at("open directory", root))?,
        stat: root_step.stat,
        path: root.to_path_buf(),
    };
    let mut pending = components(&absolute);
    let mut links = 0;
    let mut created = false;
    while let Some(name) = pending.pop_front() {
        let last = pending.is_empty();
        let entry = if name == ".." {
            current.path.parent().unwrap_or(root).to_path_buf()
        } else {
            current.path.join(&name)
        };
        let stat = if let Some(stat) = lstat_at(&current.directory, &name, &entry)? {
            stat
        } else {
            check_trusted(&current, owner.as_raw(), owner, root_owner, trusted)?;
            if !create {
                return Err(PathIo::new(
                    "open directory",
                    &entry,
                    io::ErrorKind::NotFound.into(),
                ));
            }
            match rustix::fs::mkdirat(&current.directory, name.as_os_str(), PRIVATE_DIR) {
                Ok(()) => created = last,
                Err(rustix::io::Errno::EXIST) => {}
                Err(errno) => return Err(PathIo::new("create directory", &entry, errno.into())),
            }
            lstat_at(&current.directory, &name, &entry)?.ok_or_else(|| {
                PathIo::new("create directory", &entry, io::ErrorKind::NotFound.into())
            })?
        };
        if entry.starts_with(trusted) && !trusted_owner(stat.st_uid, owner.as_raw(), root_owner) {
            return Err(refused(&entry, "is owned by another user"));
        }
        check_trusted(&current, stat.st_uid, owner, root_owner, trusted)?;
        let file_type = rustix::fs::FileType::from_raw_mode(stat.st_mode);
        if file_type == rustix::fs::FileType::Symlink {
            links += 1;
            if links > MAX_LINKS {
                return Err(PathIo::new(
                    "resolve directory",
                    path,
                    rustix::io::Errno::LOOP.into(),
                ));
            }
            let target = rustix::fs::readlinkat(&current.directory, name.as_os_str(), Vec::new())
                .map_err(|errno| PathIo::new("resolve directory", &entry, errno.into()))?;
            let target = PathBuf::from(OsStr::from_bytes(target.as_bytes()));
            if target.is_absolute() {
                current = Step {
                    directory: root_step
                        .directory
                        .try_clone()
                        .map_err(PathIo::at("open directory", root))?,
                    stat: root_step.stat,
                    path: root.to_path_buf(),
                };
            }
            for component in components(&target).into_iter().rev() {
                pending.push_front(component);
            }
            continue;
        }
        if file_type != rustix::fs::FileType::Directory {
            return Err(not_a_directory(&entry, create));
        }
        let step = open_step(Some(&current.directory), Path::new(&name), &entry)?;
        let opened = rustix::fs::fstat(&step.directory)
            .map_err(|errno| PathIo::new("inspect directory", &entry, errno.into()))?;
        if (opened.st_dev, opened.st_ino) != (stat.st_dev, stat.st_ino) {
            return Err(refused(&entry, "changed while it was being checked"));
        }
        current = step;
    }
    if current.stat.st_uid != owner.as_raw() {
        return Err(refused(path, "not owned by the current user"));
    }
    Ok(OpenedDir {
        directory: current.directory,
        stat: current.stat,
        created,
    })
}

/// The names a walk takes for `path`: normal components and `..`; `/` and `.` are implied.
fn components(path: &Path) -> VecDeque<OsString> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name.to_os_string()),
            Component::ParentDir => Some(OsString::from("..")),
            Component::RootDir | Component::CurDir | Component::Prefix(_) => None,
        })
        .collect()
}

/// Opens the directory `name` relative to `parent` (or the absolute `name` without one), without
/// following a link, and reads its status from the descriptor.
fn open_step(parent: Option<&File>, name: &Path, path: &Path) -> Result<Step, PathIo> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = match parent {
        Some(parent) => rustix::fs::openat(parent, name, flags, Mode::empty()),
        None => rustix::fs::open(name, flags, Mode::empty()),
    }
    .map_err(|errno| PathIo::new("open directory", path, errno.into()))?;
    let stat = rustix::fs::fstat(&fd)
        .map_err(|errno| PathIo::new("inspect directory", path, errno.into()))?;
    Ok(Step {
        directory: File::from(fd),
        stat,
        path: path.to_path_buf(),
    })
}

/// The metadata of the entry `name` in `directory`, without following a link; `None` when it is
/// missing.
fn lstat_at(directory: &File, name: &OsStr, path: &Path) -> Result<Option<Stat>, PathIo> {
    match rustix::fs::statat(directory, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(errno) => Err(PathIo::new("inspect directory", path, errno.into())),
    }
}

/// Whoever owns the filesystem root already controls everything beneath it. On real hosts that is
/// uid 0, while in a user namespace it is whatever the kernel shows owning `/`.
fn trusted_owner(uid: u32, user: u32, root_owner: u32) -> bool {
    uid == user || uid == root_owner
}

/// Whether a parent and its entered entry satisfy the ownership and writable-parent rules.
fn trusted_parent(
    parent_owner: u32,
    mode: Mode,
    entry_owner: u32,
    user: u32,
    root_owner: u32,
) -> bool {
    trusted_owner(parent_owner, user, root_owner)
        && (!mode.intersects(SHARED_WRITE)
            || (mode.contains(Mode::SVTX) && trusted_owner(entry_owner, user, root_owner)))
}

/// Refuses an untrusted parent; directories outside `trusted` are not inspected.
fn check_trusted(
    step: &Step,
    entry_owner: u32,
    owner: Uid,
    root_owner: u32,
    trusted: &Path,
) -> Result<(), PathIo> {
    if !step.path.starts_with(trusted) {
        return Ok(());
    }
    if !trusted_owner(step.stat.st_uid, owner.as_raw(), root_owner) {
        return Err(refused(&step.path, "is owned by another user"));
    }
    if !trusted_parent(
        step.stat.st_uid,
        Mode::from_raw_mode(step.stat.st_mode),
        entry_owner,
        owner.as_raw(),
        root_owner,
    ) {
        return Err(refused(&step.path, "is writable by group or others"));
    }
    Ok(())
}

/// Sets an opened directory to [`PRIVATE_DIR`] unless it already has that mode.
fn tighten_open_dir(directory: &File, stat: &Stat, path: &Path) -> Result<(), PathIo> {
    if Mode::from_raw_mode(stat.st_mode) != PRIVATE_DIR {
        rustix::fs::fchmod(directory, PRIVATE_DIR)
            .map_err(|errno| PathIo::new("set permissions of directory", path, errno.into()))?;
    }
    Ok(())
}

/// The error for a non-directory entry: a failed creation, or ENOTDIR for an open that creates
/// nothing.
fn not_a_directory(entry: &Path, create: bool) -> PathIo {
    if create {
        PathIo::new(
            "create directory",
            entry,
            io::ErrorKind::AlreadyExists.into(),
        )
    } else {
        PathIo::new("open directory", entry, rustix::io::Errno::NOTDIR.into())
    }
}

/// Builds a refusal for `path` that names the action `use directory`.
fn refused(path: &Path, reason: &'static str) -> PathIo {
    PathIo::new(
        "use directory",
        path,
        io::Error::new(io::ErrorKind::PermissionDenied, reason),
    )
}

/// An opened, verified private directory. Writes remain anchored to it if its path is renamed.
#[derive(Debug)]
pub struct PrivateDir {
    /// Verified directory descriptor.
    directory: File,
    /// Original path, used only in errors.
    path: PathBuf,
}

impl PrivateDir {
    /// Atomically replaces a single file in this directory and syncs the directory.
    ///
    /// # Errors
    /// Returns an error for an invalid name or a failed filesystem operation.
    pub fn write_atomic(
        &self,
        name: impl AsRef<OsStr>,
        contents: &[u8],
        mode: Mode,
    ) -> Result<(), PathIo> {
        self.publish(name.as_ref(), contents, mode, false)
            .map(|_| ())
    }

    /// Atomically publishes a file only if absent, returning whether this call won.
    ///
    /// # Errors
    /// Returns an error for an invalid name or a failed filesystem operation.
    pub fn write_new_atomic(
        &self,
        name: impl AsRef<OsStr>,
        contents: &[u8],
        mode: Mode,
    ) -> Result<bool, PathIo> {
        self.publish(name.as_ref(), contents, mode, true)
    }

    /// Reads a regular file in this directory without following links, or returns `None` if absent.
    ///
    /// # Errors
    /// Returns an error for an invalid name, a non-regular file, or a failed read.
    pub fn read_optional(&self, name: impl AsRef<OsStr>) -> Result<Option<Vec<u8>>, PathIo> {
        let name = name.as_ref();
        let path = self.file_path(name, "read")?;
        let fd = match rustix::fs::openat(
            &self.directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(PathIo::new("read", &path, error.into())),
        };
        let stat =
            rustix::fs::fstat(&fd).map_err(|error| PathIo::new("read", &path, error.into()))?;
        if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile {
            return Err(PathIo::new(
                "read",
                &path,
                io::ErrorKind::InvalidInput.into(),
            ));
        }
        let mut bytes = Vec::new();
        File::from(fd)
            .read_to_end(&mut bytes)
            .map_err(PathIo::at("read", &path))?;
        Ok(Some(bytes))
    }

    /// Removes a file in this directory, succeeding if it is absent.
    ///
    /// # Errors
    /// Returns an error for an invalid name or a failed removal.
    pub fn remove_optional(&self, name: impl AsRef<OsStr>) -> Result<(), PathIo> {
        let name = name.as_ref();
        let path = self.file_path(name, "remove")?;
        match rustix::fs::unlinkat(&self.directory, name, rustix::fs::AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => Ok(()),
            Err(error) => Err(PathIo::new("remove", &path, error.into())),
        }
    }

    /// Takes an exclusive lock on a file in this directory, blocking until it is free.
    /// The file is created with mode 0600 when missing; links and non-regular files are refused.
    ///
    /// # Errors
    /// Returns an error for an invalid name, failed open or inspection, non-regular file, or
    /// failed lock operation.
    pub fn lock(&self, name: impl AsRef<OsStr>) -> Result<crate::FileLock, PathIo> {
        let name = name.as_ref();
        let path = self.file_path(name, "open lock file")?;
        let fd = rustix::fs::openat(
            &self.directory,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            PRIVATE_FILE,
        )
        .map_err(|error| PathIo::new("open lock file", &path, error.into()))?;
        let stat = rustix::fs::fstat(&fd)
            .map_err(|error| PathIo::new("open lock file", &path, error.into()))?;
        if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile {
            return Err(PathIo::new(
                "open lock file",
                &path,
                io::ErrorKind::InvalidInput.into(),
            ));
        }
        crate::FileLock::exclusive_blocking_on(File::from(fd), &path)
    }

    /// Validates a single file name and constructs its diagnostic path.
    fn file_path(&self, name: &OsStr, action: &'static str) -> Result<PathBuf, PathIo> {
        let path = self.path.join(name);
        let mut components = Path::new(name).components();
        if !matches!(
            (components.next(), components.next()),
            (Some(Component::Normal(_)), None)
        ) || name.as_bytes().contains(&b'/')
        {
            return Err(PathIo::new(
                action,
                &path,
                io::ErrorKind::InvalidInput.into(),
            ));
        }
        Ok(path)
    }

    /// Writes a temporary file and publishes it relative to the verified descriptor.
    fn publish(
        &self,
        name: &OsStr,
        contents: &[u8],
        mode: Mode,
        new: bool,
    ) -> Result<bool, PathIo> {
        let path = self.file_path(name, "write")?;
        publish_at(&self.directory, name, &path, contents, mode, new)
    }
}

/// Publishes bytes relative to an opened directory, naming `error_path` on failure.
fn publish_at(
    directory: &File,
    name: &OsStr,
    error_path: &Path,
    contents: &[u8],
    mode: Mode,
    new: bool,
) -> Result<bool, PathIo> {
    static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let n = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temporary = format!(".gang-{}-{n}", std::process::id());
        if OsStr::new(&temporary) == name {
            continue;
        }
        let flags =
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        match rustix::fs::openat(directory, temporary.as_str(), flags, mode) {
            Ok(fd) => break (temporary, File::from(fd)),
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => {
                return Err(PathIo::new(
                    "create temporary file for",
                    error_path,
                    error.into(),
                ));
            }
        }
    };
    let mut renamed = false;
    let result = (|| {
        rustix::fs::fchmod(&file, mode)
            .map_err(|error| PathIo::new("set permissions of", error_path, error.into()))?;
        file.write_all(contents)
            .map_err(PathIo::at("write", error_path))?;
        file.sync_all().map_err(PathIo::at("sync", error_path))?;
        if new {
            match rustix::fs::linkat(
                directory,
                temporary.as_str(),
                directory,
                name,
                rustix::fs::AtFlags::empty(),
            ) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => return Ok(false),
                Err(error) => return Err(PathIo::new("publish", error_path, error.into())),
            }
        } else {
            rustix::fs::renameat(directory, temporary.as_str(), directory, name)
                .map_err(|error| PathIo::new("replace", error_path, error.into()))?;
            renamed = true;
        }
        Ok(true)
    })();
    if !renamed {
        let _ = rustix::fs::unlinkat(directory, temporary.as_str(), rustix::fs::AtFlags::empty());
    }
    if matches!(result, Ok(true)) {
        directory
            .sync_all()
            .map_err(PathIo::at("sync directory", error_path))?;
    }
    result
}

/// Replaces the file at `path` with `contents`, created with permissions `mode`.
///
/// The bytes are written to a temporary file in the same directory, synced, and renamed over
/// `path`, so readers see either the old file or the complete new one. The temporary file is
/// removed on failure. The file and its parent directory are fsynced.
///
/// # Errors
///
/// Returns an error when the temporary file cannot be created in the parent directory, when its
/// permissions cannot be set, when the contents cannot be written or synced, or when the rename
/// fails. Every error names `path`.
pub fn write_atomic(path: &Path, contents: &[u8], mode: Mode) -> Result<(), PathIo> {
    let (directory, name) = open_publish_parent(path)?;
    publish_at(&directory, name, path, contents, mode, false).map(|_| ())
}

/// Opens a path's parent for the path-based write helpers.
fn open_publish_parent(path: &Path) -> Result<(File, &OsStr), PathIo> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        PathIo::new(
            "create temporary file for",
            path,
            io::ErrorKind::InvalidInput.into(),
        )
    })?;
    let fd = rustix::fs::open(
        parent,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| PathIo::new("create temporary file for", path, error.into()))?;
    Ok((File::from(fd), name))
}

/// Publishes `contents` at `path` only if nothing is there yet, created with permissions `mode`.
///
/// The bytes go to a temporary file in the same directory, are synced, and are then hard-linked
/// to `path`. The link never overwrites, so the first creator wins and any other caller must
/// re-read the winner's file. This is atomic across processes. The temporary file is removed in
/// every outcome. The file and parent directory are fsynced; the parent is not created.
///
/// Returns `Ok(true)` when this call published the file and `Ok(false)` when a file already
/// existed at `path`; in that case nothing is replaced.
///
/// # Errors
///
/// Returns an error when the temporary file cannot be created in the parent directory, when its
/// permissions cannot be set, when the contents cannot be written or synced, or when publishing
/// fails for any reason other than `path` already existing. Every error names `path`.
pub fn write_new_atomic(path: &Path, contents: &[u8], mode: Mode) -> Result<bool, PathIo> {
    let (directory, name) = open_publish_parent(path)?;
    publish_at(&directory, name, path, contents, mode, true)
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
    crate::sys::mkfifo(&c, PRIVATE_FILE).map_err(PathIo::at("create fifo", path))
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

    /// Writable parents are refused without attributing the refusal to a link.
    #[test]
    fn refusals_do_not_blame_a_missing_link() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let shared = trusted.join("shared");
        fs::create_dir(&shared)?;
        fs::set_permissions(&shared, Permissions::from_mode(0o777))?;
        let error = ensure_private_dir_as(
            &shared.join("private"),
            rustix::process::geteuid(),
            &trusted,
        )
        .expect_err("a writable parent must be refused");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.path, shared);
        assert!(
            !error.source.to_string().contains("symbolic link"),
            "a refusal must not blame a link that does not exist: {}",
            error.source
        );
        Ok(())
    }

    /// Opening a missing private directory does not create any components.
    #[test]
    fn open_private_dir_creates_nothing() -> TestResult {
        let root = tempfile::tempdir()?;
        let parent = root.path().join("a");
        assert!(
            open_private_dir(&parent.join("b"))?.is_none(),
            "a missing private directory must return None"
        );
        assert!(!parent.exists(), "opening must not create a missing parent");
        Ok(())
    }

    /// Opening an existing private directory refuses a writable parent.
    #[test]
    fn open_private_dir_refuses_a_writable_parent() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let shared = trusted.join("shared");
        let private = shared.join("gang");
        fs::create_dir(&shared)?;
        fs::create_dir(&private)?;
        fs::set_permissions(&private, Permissions::from_mode(0o700))?;
        fs::set_permissions(&shared, Permissions::from_mode(0o777))?;
        let error = open_private_dir_as(&private, rustix::process::geteuid(), &trusted)
            .expect_err("a writable parent must be refused");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.path, shared);
        Ok(())
    }

    /// Opening a writable leaf refuses it without changing its permissions.
    #[test]
    fn open_private_dir_refuses_a_writable_leaf() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let leaf = trusted.join("gang");
        fs::create_dir(&leaf)?;
        fs::set_permissions(&leaf, Permissions::from_mode(0o777))?;
        let error = open_private_dir_as(&leaf, rustix::process::geteuid(), &trusted)
            .expect_err("a writable leaf must be refused");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.path, leaf);
        assert_eq!(mode_of(&leaf)?, 0o777, "a refused leaf must not be changed");
        Ok(())
    }

    /// Ensuring a writable leaf refuses it without changing its permissions.
    #[test]
    fn ensure_private_dir_refuses_a_writable_leaf() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let leaf = trusted.join("gang");
        fs::create_dir(&leaf)?;
        fs::set_permissions(&leaf, Permissions::from_mode(0o775))?;
        let error = ensure_private_dir_as(&leaf, rustix::process::geteuid(), &trusted)
            .expect_err("a writable leaf must be refused");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.path, leaf);
        assert_eq!(mode_of(&leaf)?, 0o775, "a refused leaf must not be changed");
        Ok(())
    }

    /// Opening a non-writable leaf leaves its existing mode unchanged.
    #[test]
    fn open_private_dir_leaves_the_mode_unchanged() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let leaf = trusted.join("gang");
        fs::create_dir(&leaf)?;
        fs::set_permissions(&leaf, Permissions::from_mode(0o755))?;
        open_private_dir_as(&leaf, rustix::process::geteuid(), &trusted)?
            .expect("existing directory");
        assert_eq!(
            mode_of(&leaf)?,
            0o755,
            "opening must not change the directory"
        );
        Ok(())
    }

    /// Opening a file as a private directory reports a non-directory error.
    #[test]
    fn open_private_dir_reports_a_file_as_not_a_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        fs::write(trusted.join("gang"), b"x")?;
        let error =
            open_private_dir_as(&trusted.join("gang"), rustix::process::geteuid(), &trusted)
                .expect_err("a file is not a directory");
        assert_eq!(
            (error.action, error.source.raw_os_error()),
            (
                "open directory",
                Some(rustix::io::Errno::NOTDIR.raw_os_error())
            )
        );
        Ok(())
    }

    /// Reads through a capability do not follow links.
    #[test]
    fn private_dir_read_refuses_a_symbolic_link() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        let outside = root.path().join("outside");
        fs::write(&outside, b"secret")?;
        std::os::unix::fs::symlink(&outside, private.join("link"))?;
        let error = cap
            .read_optional("link")
            .expect_err("a link inside the directory must be refused");
        assert_eq!(error.path, private.join("link"));
        assert_eq!(
            error.source.raw_os_error(),
            Some(rustix::io::Errno::LOOP.raw_os_error())
        );
        Ok(())
    }

    /// Reads reject a FIFO without waiting for a writer.
    #[test]
    fn private_dir_read_refuses_a_fifo_without_blocking() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        create_private_fifo(&private.join("fifo"))?;
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(cap.read_optional("fifo").map(|_| ()));
        });
        let error = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("reading a FIFO must not block")
            .expect_err("a FIFO must be refused");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.path, private.join("fifo"));
        Ok(())
    }

    /// Locking a FIFO fails without waiting for a reader.
    #[test]
    fn private_dir_lock_refuses_a_fifo_without_blocking() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        create_private_fifo(&private.join("lock"))?;
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(cap.lock("lock").map(drop));
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("locking a FIFO must not block")
            .expect_err("a FIFO lock file must be refused");
        Ok(())
    }

    /// Locking a FIFO with a reader still refuses the non-regular file.
    #[test]
    fn private_dir_lock_refuses_a_fifo_with_a_reader() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        let path = private.join("lock");
        create_private_fifo(&path)?;
        let _reader = rustix::fs::open(
            &path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let error = cap
            .lock("lock")
            .expect_err("a FIFO lock file must be refused");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.action, "open lock file");
        assert_eq!(error.path, path);
        Ok(())
    }

    /// Removing a file through a capability also succeeds when it is already absent.
    #[test]
    fn private_dir_remove_optional_removes_and_ignores_absent() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        fs::write(private.join("file"), b"data")?;
        cap.remove_optional("file")?;
        assert!(!private.join("file").exists(), "the file must be removed");
        cap.remove_optional("file")?;
        Ok(())
    }

    /// Lock files cannot be opened through symbolic links.
    #[test]
    fn private_dir_lock_refuses_a_symbolic_link() -> TestResult {
        let root = tempfile::tempdir()?;
        let private = root.path().join("private");
        let cap = ensure_private_dir(&private)?;
        let outside = root.path().join("elsewhere");
        std::os::unix::fs::symlink(&outside, private.join("lock"))?;
        let error = cap.lock("lock").expect_err("a lock link must be refused");
        assert_eq!(error.path, private.join("lock"));
        assert!(!outside.exists(), "locking must not create the link target");
        Ok(())
    }

    /// Path-based writes preserve a bare relative path in errors.
    #[test]
    fn write_atomic_names_a_bare_relative_path_in_errors() -> TestResult {
        let dir = tempfile::Builder::new().tempdir_in(".")?;
        let name = PathBuf::from(dir.path().file_name().expect("name"));
        fs::write(dir.path().join("child"), b"data")?;
        let error = write_atomic(&name, b"x", PRIVATE_FILE)
            .expect_err("replacing a non-empty directory fails");
        assert_eq!(error.path, name, "errors must name the given path");
        Ok(())
    }

    #[test]
    fn trusted_owner_uses_the_observed_root_owner() {
        assert!(!trusted_owner(65534, 1000, 0));
        assert!(trusted_owner(65534, 1000, 65534));
        assert!(trusted_owner(1000, 1000, 65534));
        assert!(!trusted_owner(0, 1000, 65534));
    }

    #[test]
    fn trusted_parent_enforces_sticky_and_entry_ownership() {
        let sticky = Mode::from_bits_retain(0o1777);
        assert!(trusted_parent(65534, sticky, 1000, 1000, 65534));
        assert!(trusted_parent(65534, sticky, 65534, 1000, 65534));
        assert!(!trusted_parent(65534, sticky, 2000, 1000, 65534));
        assert!(!trusted_parent(
            65534,
            Mode::from_bits_retain(0o0775),
            1000,
            1000,
            65534
        ));
        assert!(!trusted_parent(
            2000,
            Mode::from_bits_retain(0o0755),
            1000,
            1000,
            65534
        ));
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

        let err = ensure_private_dir_as(&dir, other_uid(), Path::new("/"))
            .expect_err("another user owns the directory");

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(err.action, "use directory");
        assert_eq!(mode_of(&dir)?, 0o755);
        Ok(())
    }

    #[test]
    fn ensure_private_dir_follows_a_safe_link_and_tightens_the_target() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let me = rustix::process::geteuid();
        let target = trusted.join("target");
        let home = trusted.join("home");
        fs::create_dir(&target)?;
        fs::create_dir(&home)?;
        let link = home.join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        fs::set_permissions(&target, Permissions::from_mode(0o755))?;
        fs::set_permissions(&home, Permissions::from_mode(0o700))?;

        ensure_private_dir_as(&link, me, &trusted)?;

        assert_eq!(mode_of(&target)?, 0o700);
        assert!(fs::symlink_metadata(&link)?.file_type().is_symlink());
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
        fs::set_permissions(&dir, Permissions::from_mode(0o700))?;

        require_private_dir(&dir)?;

        assert_eq!(mode_of(&dir)?, 0o700);
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
            matches!(&err, PrivateDirError::NotPrivate { path, mode }
                if path == &dir && *mode == Mode::from_bits_retain(0o755)),
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
        fs::set_permissions(&dir, Permissions::from_mode(0o700))?;

        let err = require_private_dir_as(&dir, other_uid(), Path::new("/"))
            .expect_err("another user owns it");

        assert!(
            matches!(&err, PrivateDirError::Io(io_err) if io_err.action == "use directory"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn require_private_dir_accepts_a_link_in_a_private_home_to_a_private_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let me = rustix::process::geteuid();
        let target = trusted.join("target");
        let home = trusted.join("home");
        fs::create_dir(&target)?;
        fs::create_dir(&home)?;
        let link = home.join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        fs::set_permissions(&target, Permissions::from_mode(0o700))?;
        fs::set_permissions(&home, Permissions::from_mode(0o700))?;

        require_private_dir_as(&link, me, &trusted)?;
        Ok(())
    }

    #[test]
    fn require_private_dir_refuses_a_link_into_a_group_writable_directory() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let me = rustix::process::geteuid();
        let shared = trusted.join("shared");
        let target = shared.join("target");
        fs::create_dir(&shared)?;
        fs::create_dir(&target)?;
        let link = trusted.join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        fs::set_permissions(&target, Permissions::from_mode(0o700))?;
        fs::set_permissions(&shared, Permissions::from_mode(0o775))?;

        let err = require_private_dir_as(&link, me, &trusted)
            .expect_err("a group-writable ancestor is unsafe");
        match err {
            PrivateDirError::Io(e) => {
                assert_eq!(e.action, "use directory");
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
                assert_eq!(e.path, trusted.join("shared"));
            }
            other @ PrivateDirError::NotPrivate { .. } => panic!("expected io error, got {other}"),
        }
        Ok(())
    }

    #[test]
    fn require_private_dir_accepts_a_link_into_a_sticky_directory_entry_it_owns() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let me = rustix::process::geteuid();
        let sticky = trusted.join("sticky");
        let target = sticky.join("target");
        fs::create_dir(&sticky)?;
        fs::create_dir(&target)?;
        let link = trusted.join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        fs::set_permissions(&target, Permissions::from_mode(0o700))?;
        fs::set_permissions(&sticky, Permissions::from_mode(0o1777))?;

        require_private_dir_as(&link, me, &trusted)?;
        Ok(())
    }

    #[test]
    fn require_private_dir_refuses_a_link_whose_ancestor_another_user_owns() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let target = trusted.join("target");
        fs::create_dir(&target)?;
        let link = trusted.join("link");
        std::os::unix::fs::symlink(&target, &link)?;
        fs::set_permissions(&target, Permissions::from_mode(0o700))?;

        let err = require_private_dir_as(&link, other_uid(), &trusted)
            .expect_err("another user owns the ancestor");
        match err {
            PrivateDirError::Io(e) => {
                assert_eq!(e.path, trusted);
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
            }
            other @ PrivateDirError::NotPrivate { .. } => panic!("expected io error, got {other}"),
        }
        Ok(())
    }

    #[test]
    fn a_plain_path_under_a_group_writable_parent_is_refused() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let open = trusted.join("open");
        fs::create_dir(&open)?;
        fs::set_permissions(&open, Permissions::from_mode(0o777))?;

        let err =
            require_private_dir_as(&open.join("private"), rustix::process::geteuid(), &trusted)
                .expect_err("writable parent must be refused");
        match err {
            PrivateDirError::Io(error) => {
                assert_eq!(error.kind(), ErrorKind::PermissionDenied);
                assert_eq!(error.path, open);
            }
            other @ PrivateDirError::NotPrivate { .. } => panic!("expected io error, got {other}"),
        }
        Ok(())
    }

    /// The attack: someone who can write to a shared directory plants a link there pointing at a
    /// directory of the user's. Every directory on the target's own path is the user's, so only
    /// the directory holding the link shows the danger.
    #[test]
    fn a_link_planted_in_a_world_writable_directory_is_refused() -> TestResult {
        let root = tempfile::tempdir()?;
        let target = root.path().join("home").join("dir");
        fs::create_dir_all(&target)?;
        let shared = root.path().join("shared");
        fs::create_dir(&shared)?;
        fs::set_permissions(&shared, Permissions::from_mode(0o777))?;
        let link = shared.join("evil");
        std::os::unix::fs::symlink(&target, &link)?;

        for result in [
            ensure_private_dir(&link),
            require_private_dir(&link).map_err(|error| match error {
                PrivateDirError::Io(error) => error,
                other @ PrivateDirError::NotPrivate { .. } => {
                    PathIo::new("use directory", &link, io::Error::other(other.to_string()))
                }
            }),
        ] {
            let error = result.expect_err(
                "a link planted in a world-writable directory must be refused, not followed",
            );
            assert_eq!(error.path, fs::canonicalize(&shared)?);
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        }
        Ok(())
    }

    /// Nothing is created through a refused link.
    #[test]
    fn nothing_is_created_through_a_refused_link() -> TestResult {
        let root = tempfile::tempdir()?;
        let trusted = fs::canonicalize(root.path())?;
        let me = rustix::process::geteuid();
        let elsewhere = trusted.join("elsewhere");
        fs::create_dir(&elsewhere)?;
        let shared = trusted.join("shared");
        fs::create_dir(&shared)?;
        fs::set_permissions(&shared, Permissions::from_mode(0o777))?;
        std::os::unix::fs::symlink(&elsewhere, shared.join("evil"))?;

        let result =
            ensure_private_dir_as(&shared.join("evil").join("new").join("dir"), me, &trusted);

        assert!(
            !elsewhere.join("new").exists(),
            "no directory may be created through a link in a world-writable directory"
        );
        assert!(result.is_err());
        Ok(())
    }

    /// Parent components are resolved on the open directory, even without links.
    #[test]
    fn a_non_canonical_path_without_links_is_accepted() -> TestResult {
        let root = tempfile::tempdir()?;
        let open = root.path().join("open");
        fs::create_dir_all(open.join("x"))?;
        fs::set_permissions(&open, Permissions::from_mode(0o755))?;

        require_private_dir(&open.join("x").join("..").join("private"))?;
        assert_eq!(mode_of(&open.join("private"))?, 0o700);
        Ok(())
    }

    /// A link in a root-owned directory to a directory the user owns, the shape of macOS's `/tmp`
    /// and `/var`, is followed when every directory around it is trusted.
    #[test]
    fn a_link_from_a_private_directory_through_a_sticky_one_is_followed() -> TestResult {
        let root = tempfile::tempdir()?;
        let sticky = root.path().join("private").join("tmp");
        fs::create_dir_all(&sticky)?;
        fs::set_permissions(&sticky, Permissions::from_mode(0o1777))?;
        std::os::unix::fs::symlink("private/tmp", root.path().join("tmp"))?;

        ensure_private_dir(&root.path().join("tmp").join("gang").join("state"))?;

        assert_eq!(mode_of(&sticky.join("gang").join("state"))?, 0o700);
        Ok(())
    }

    fn other_uid() -> Uid {
        Uid::from_raw(rustix::process::geteuid().as_raw().wrapping_add(1))
    }

    #[test]
    fn foreign_intermediate_is_refused_before_creation() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let sticky = fs::canonicalize(root.path())?.join("sticky");
        fs::create_dir(&sticky)?;
        fs::set_permissions(&sticky, Permissions::from_mode(0o1777))?;
        let foreign = sticky.join("foreign");
        fs::create_dir(&foreign)?;
        let child = foreign.join("new");
        let error =
            ensure_private_dir_as(&child, other_uid(), &foreign).expect_err("foreign owner");
        assert!(!child.exists(), "foreign intermediate permitted creation");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.path, foreign);
        Ok(())
    }

    #[test]
    fn parent_component_does_not_erase_foreign_traversal() -> TestResult {
        if rustix::process::geteuid().as_raw() == 0 {
            return Ok(());
        }
        let root = tempfile::tempdir()?;
        let sticky = fs::canonicalize(root.path())?.join("sticky");
        fs::create_dir(&sticky)?;
        fs::set_permissions(&sticky, Permissions::from_mode(0o1777))?;
        let foreign = sticky.join("foreign");
        fs::create_dir(&foreign)?;
        let mine = sticky.join("mine");
        fs::create_dir(&mine)?;
        fs::create_dir(mine.join("target"))?;
        std::os::unix::fs::symlink("target", mine.join("link"))?;
        let path = foreign.join("..").join("mine").join("link");
        let error = ensure_private_dir_as(&path, other_uid(), &foreign).expect_err("foreign owner");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.path == foreign, "foreign traversal was not refused");
        Ok(())
    }

    #[test]
    fn private_dir_write_survives_parent_replacement() -> TestResult {
        let root = tempfile::tempdir()?;
        let parent = root.path().join("parent");
        let directory = parent.join("private");
        let cap = ensure_private_dir(&directory)?;
        let moved = root.path().join("moved");
        fs::rename(&parent, &moved)?;
        fs::create_dir_all(&directory)?;
        cap.write_atomic("token", b"secret", PRIVATE_FILE)?;
        assert_eq!(fs::read(moved.join("private/token"))?, b"secret");
        assert!(!directory.join("token").exists());
        Ok(())
    }

    #[test]
    fn private_dir_write_refuses_separator() -> TestResult {
        let root = tempfile::tempdir()?;
        let cap = ensure_private_dir(&root.path().join("private"))?;
        let error = cap
            .write_atomic("nested/token", b"secret", PRIVATE_FILE)
            .expect_err("a name containing a separator is invalid");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.path, root.path().join("private/nested/token"));
        Ok(())
    }

    #[test]
    fn existing_parents_are_left_unchanged() -> TestResult {
        let root = tempfile::tempdir()?;
        let parent = root.path().join("parent");
        fs::create_dir(&parent)?;
        fs::set_permissions(&parent, Permissions::from_mode(0o755))?;

        ensure_private_dir(&parent.join("child"))?;

        assert_eq!(mode_of(&parent)?, 0o755);
        assert_eq!(mode_of(&parent.join("child"))?, 0o700);
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
        write_atomic(&path, b"old", PUBLIC_FILE)?;

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

        assert!(write_new_atomic(&path, b"open", PUBLIC_FILE)?);

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
        DirBuilder::new().mode(0o700).create(&dir)?;

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
    fn private_fifo_rejects_nul_in_path() {
        use std::ffi::OsStr;

        let path = Path::new(OsStr::from_bytes(b"fifo\0name"));
        let error = create_private_fifo(path).expect_err("NUL cannot occur in a filesystem path");
        assert_eq!(error.path, path);
        assert_eq!(error.source.kind(), io::ErrorKind::InvalidInput);
    }
}
