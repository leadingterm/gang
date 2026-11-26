//! On-disk sharded CAS layout, atomic verified insertion, startup scan, and link/clone staging.

use std::fs::{self, File, FileTimes};
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gang_types::Digest;
use rustix::fs::{AtFlags, Mode, OFlags, fchmod, openat, renameat, unlinkat};
use sha2::{Digest as _, Sha256};

#[cfg(target_os = "linux")]
use rustix::fs::{CWD, ioctl_ficlone, linkat};
#[cfg(target_os = "macos")]
use rustix::fs::{CloneFlags, fclonefileat};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use rustix::io::Errno;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd as _;
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicBool;

use crate::lru::{BlobMode, CacheKey};

/// Unix timestamp for `2000-01-01T00:00:00Z` applied to every stored CAS file.
pub const DETERMINISTIC_MTIME_SECS: u64 = 946_684_800;

/// Subdirectory inside the CAS root holding temporary staging files.
pub const TMP_DIR_NAME: &str = "tmp";

/// Legacy temporary directory name also cleaned during startup scan.
const DOT_TMP_DIR_NAME: &str = ".tmp";

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(target_os = "linux")]
static TMPFILE_SUPPORTED: AtomicBool = AtomicBool::new(true);

/// Returns the deterministic [`SystemTime`] (`2000-01-01T00:00:00Z`).
#[must_use]
pub fn deterministic_mtime() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(DETERMINISTIC_MTIME_SECS)
}

/// Returns the two-character lowercase hexadecimal shard directory name for `digest`.
#[must_use]
pub fn shard_dir_name(digest: &Digest) -> String {
    format!("{:02x}", digest.hash()[0])
}

/// Returns the filename (`<64-hex>-<size>` or `<64-hex>-<size>.x`) for `key`.
#[must_use]
pub fn entry_file_name(key: CacheKey) -> String {
    let hex = key.digest.hex();
    let size = key.digest.size();
    match key.mode {
        BlobMode::Plain => format!("{hex}-{size}"),
        BlobMode::Executable => format!("{hex}-{size}.x"),
    }
}

/// Returns the full path (`<root>/<hh>/<hex>-<size>[.x]`) for `key`.
#[must_use]
pub fn blob_path(root: &Path, key: CacheKey) -> PathBuf {
    root.join(shard_dir_name(&key.digest))
        .join(entry_file_name(key))
}

/// Parses a CAS entry filename into `(Digest, BlobMode)`, rejecting malformed names and
/// [`Digest::EMPTY`].
#[must_use]
pub fn parse_entry_name(name: &str) -> Option<(Digest, BlobMode)> {
    let (stem, mode) = match name.strip_suffix(".x") {
        Some(stem) => (stem, BlobMode::Executable),
        None => (name, BlobMode::Plain),
    };
    let (hex_part, size_part) = stem.split_once('-')?;
    if hex_part.len() != 64
        || !hex_part
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    if size_part.is_empty() || !size_part.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let size = size_part.parse::<u64>().ok()?;
    let digest = Digest::from_hex_and_size(hex_part, size).ok()?;
    if digest.is_empty() {
        return None;
    }
    Some((digest, mode))
}

/// Returns the POSIX permission [`Mode`] for `mode` (`0444` plain, `0555` executable).
#[must_use]
pub const fn file_mode(mode: BlobMode) -> Mode {
    let read_all = Mode::RUSR.union(Mode::RGRP).union(Mode::ROTH);
    match mode {
        BlobMode::Plain => read_all,
        BlobMode::Executable => read_all
            .union(Mode::XUSR)
            .union(Mode::XGRP)
            .union(Mode::XOTH),
    }
}

/// Ensures the shard directory `<root>/<hh>` exists and returns its path.
///
/// # Errors
///
/// Returns any filesystem error creating the shard directory.
pub fn ensure_shard_dir(root: &Path, digest: &Digest) -> io::Result<PathBuf> {
    let shard = root.join(shard_dir_name(digest));
    fs::create_dir_all(&shard)?;
    Ok(shard)
}

/// Applies deterministic `2000-01-01T00:00:00Z` timestamps and immutable permissions to `file`.
///
/// # Errors
///
/// Returns any OS error updating timestamps or permission bits.
pub fn normalize_file(file: &File, mode: BlobMode) -> io::Result<()> {
    let stamp = deterministic_mtime();
    let times = FileTimes::new().set_accessed(stamp).set_modified(stamp);
    file.set_times(times)?;
    fchmod(file, file_mode(mode))?;
    Ok(())
}

/// An entry discovered during startup scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScannedEntry {
    /// Entry key.
    pub key: CacheKey,
    /// Verified on-disk byte size matching `key.digest.size()`.
    pub size: u64,
    /// Monotonic LRU ordering stamp derived from `atime` (or `ctime` on `noatime` mounts).
    pub stamp: u64,
}

/// Result of scanning the CAS directory tree at startup.
#[derive(Debug, Default)]
pub struct ScanReport {
    /// Valid entries discovered on disk.
    pub entries: Vec<ScannedEntry>,
    /// Total valid bytes across `entries`.
    pub total_bytes: u64,
    /// Number of stale temp, malformed, or size-mismatched files removed.
    pub removed_corrupt_or_stale: u64,
}

/// Scans `<root>` in parallel across shard directories, removing stale temp files, malformed
/// names, and size-mismatched files, and returns valid entries for LRU reconstruction.
///
/// # Errors
///
/// Returns an error if `root` cannot be read or `<root>/tmp` cannot be prepared.
pub fn scan_cas_root(root: &Path) -> io::Result<ScanReport> {
    fs::create_dir_all(root)?;
    let mut removed = 0_u64;

    for temp_name in [TMP_DIR_NAME, DOT_TMP_DIR_NAME] {
        let temp_path = root.join(temp_name);
        if temp_path.exists() {
            fs::remove_dir_all(&temp_path)?;
            removed = removed.saturating_add(1);
        }
    }
    fs::create_dir_all(root.join(TMP_DIR_NAME))?;

    let mut shard_dirs = Vec::new();
    for entry_res in fs::read_dir(root)? {
        let entry = entry_res?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            remove_unknown_path(&entry.path())?;
            removed = removed.saturating_add(1);
            continue;
        };
        if name == TMP_DIR_NAME {
            continue;
        }
        let is_valid_shard = name.len() == 2
            && name
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            && entry.file_type()?.is_dir();
        if !is_valid_shard {
            remove_unknown_path(&entry.path())?;
            removed = removed.saturating_add(1);
            continue;
        }
        shard_dirs.push((name.to_owned(), entry.path()));
    }

    if shard_dirs.is_empty() {
        return Ok(ScanReport {
            entries: Vec::new(),
            total_bytes: 0,
            removed_corrupt_or_stale: removed,
        });
    }

    let worker_count = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(shard_dirs.len())
        .clamp(1, 16);
    let chunk_size = shard_dirs.len().div_ceil(worker_count);

    let shard_results: Vec<io::Result<ScanReport>> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for chunk in shard_dirs.chunks(chunk_size) {
            handles.push(scope.spawn(|| scan_shard_batch(chunk)));
        }
        let mut out = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.join() {
                Ok(res) => out.push(res),
                Err(_) => out.push(Err(io::Error::other("startup shard scan worker panicked"))),
            }
        }
        out
    });

    let mut report = ScanReport {
        entries: Vec::new(),
        total_bytes: 0,
        removed_corrupt_or_stale: removed,
    };
    for partial in shard_results {
        let partial = partial?;
        report.entries.extend(partial.entries);
        report.total_bytes = report.total_bytes.saturating_add(partial.total_bytes);
        report.removed_corrupt_or_stale = report
            .removed_corrupt_or_stale
            .saturating_add(partial.removed_corrupt_or_stale);
    }
    Ok(report)
}

fn scan_shard_batch(shards: &[(String, PathBuf)]) -> io::Result<ScanReport> {
    let mut report = ScanReport::default();
    for (shard_name, shard_path) in shards {
        for entry_res in fs::read_dir(shard_path)? {
            let entry = entry_res?;
            let path = entry.path();
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if !meta.file_type().is_file() {
                remove_unknown_path(&path)?;
                report.removed_corrupt_or_stale = report.removed_corrupt_or_stale.saturating_add(1);
                continue;
            }
            let Some((digest, mode)) = entry
                .file_name()
                .to_str()
                .and_then(parse_entry_name)
                .filter(|(digest, _)| shard_dir_name(digest) == *shard_name)
            else {
                fs::remove_file(&path)?;
                report.removed_corrupt_or_stale = report.removed_corrupt_or_stale.saturating_add(1);
                continue;
            };

            if meta.len() != digest.size() {
                fs::remove_file(&path)?;
                report.removed_corrupt_or_stale = report.removed_corrupt_or_stale.saturating_add(1);
                continue;
            }

            let stamp = metadata_lru_stamp(&meta);
            report.entries.push(ScannedEntry {
                key: CacheKey::new(digest, mode),
                size: digest.size(),
                stamp,
            });
            report.total_bytes = report.total_bytes.saturating_add(digest.size());
        }
    }
    Ok(report)
}

fn metadata_lru_stamp(meta: &fs::Metadata) -> u64 {
    let atime_secs = u64::try_from(meta.atime()).unwrap_or(0);
    let atime_nsec = u64::try_from(meta.atime_nsec()).unwrap_or(0);
    // `mtime` is normalized to `DETERMINISTIC_MTIME_SECS`. If `atime` still equals it, the mount
    // is likely `noatime` or the file has not been read since creation; fall back to `ctime`.
    let (secs, nsecs) = if atime_secs == DETERMINISTIC_MTIME_SECS && atime_nsec == 0 {
        (
            u64::try_from(meta.ctime()).unwrap_or(atime_secs),
            u64::try_from(meta.ctime_nsec()).unwrap_or(0),
        )
    } else {
        (atime_secs, atime_nsec)
    };
    secs.saturating_mul(1_000_000_000).saturating_add(nsecs)
}

fn remove_unknown_path(path: &Path) -> io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

/// RAII guard removing a temporary file unless [`TempFileGuard::persist`] is called.
struct TempFileGuard {
    dir: rustix::fd::OwnedFd,
    name: String,
    persisted: bool,
}

impl TempFileGuard {
    fn persist(mut self) {
        self.persisted = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = unlinkat(&self.dir, self.name.as_str(), AtFlags::empty());
        }
    }
}

/// Outcome of streaming and verifying a blob into [`VerifiedWriter`].
pub enum VerifiedWriteError {
    /// The stream's byte length or SHA-256 hash did not match the expected digest.
    DigestMismatch,
    /// Underlying filesystem I/O failure.
    Io(io::Error),
}

impl From<io::Error> for VerifiedWriteError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Streaming SHA-256 verifying writer targeting either `DiskCas` (`O_TMPFILE` / `<cas>/tmp`) or a
/// direct destination path (bypass path).
pub struct VerifiedWriter {
    file: File,
    hasher: Sha256,
    written: u64,
    expected: Digest,
    mode: BlobMode,
    target: WriterTarget,
}

enum WriterTarget {
    #[cfg(target_os = "linux")]
    CasUnnamed {
        shard_dir: rustix::fd::OwnedFd,
        dest_name: String,
    },
    CasNamed {
        tmp_dir: rustix::fd::OwnedFd,
        tmp_name: String,
        shard_dir: rustix::fd::OwnedFd,
        dest_name: String,
        guard: TempFileGuard,
    },
    DirectPath {
        parent_dir: rustix::fd::OwnedFd,
        tmp_name: String,
        dest_name: String,
        guard: TempFileGuard,
    },
}

impl VerifiedWriter {
    /// Opens a staged writer inside `<root>/<hh>/` (Linux `O_TMPFILE` first, `<root>/tmp/`
    /// fallback).
    ///
    /// # Errors
    ///
    /// Returns any filesystem error creating the temporary staging file.
    pub fn open_cas(root: &Path, key: CacheKey) -> io::Result<Self> {
        let shard_path = ensure_shard_dir(root, &key.digest)?;
        let shard_dir = open_dir(&shard_path)?;
        let dest_name = entry_file_name(key);

        #[cfg(target_os = "linux")]
        if TMPFILE_SUPPORTED.load(Ordering::Relaxed) {
            match openat(
                &shard_dir,
                c".",
                OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(fd) => {
                    return Ok(Self {
                        file: File::from(fd),
                        hasher: Sha256::new(),
                        written: 0,
                        expected: key.digest,
                        mode: key.mode,
                        target: WriterTarget::CasUnnamed {
                            shard_dir,
                            dest_name,
                        },
                    });
                }
                Err(Errno::OPNOTSUPP | Errno::ISDIR) => {
                    TMPFILE_SUPPORTED.store(false, Ordering::Relaxed);
                }
                Err(error) => return Err(error.into()),
            }
        }

        let tmp_root = root.join(TMP_DIR_NAME);
        fs::create_dir_all(&tmp_root)?;
        let tmp_dir = open_dir(&tmp_root)?;
        let tmp_name = unique_temp_name("cas");
        let (file, guard) = create_exclusive_temp(&tmp_dir, &tmp_name)?;
        Ok(Self {
            file,
            hasher: Sha256::new(),
            written: 0,
            expected: key.digest,
            mode: key.mode,
            target: WriterTarget::CasNamed {
                tmp_dir,
                tmp_name,
                shard_dir,
                dest_name,
                guard,
            },
        })
    }

    /// Opens a staged writer directly in the parent directory of `dest` (for the bypass path).
    ///
    /// # Errors
    ///
    /// Returns an error if `dest` has no filename/parent or the temp file cannot be created.
    pub fn open_direct(dest: &Path, expected: Digest, mode: BlobMode) -> io::Result<Self> {
        let parent = dest.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct destination path has no parent directory",
            )
        })?;
        let dest_name = dest
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "direct destination filename is missing or non-UTF-8",
                )
            })?
            .to_owned();
        fs::create_dir_all(parent)?;
        let parent_dir = open_dir(parent)?;
        let tmp_name = unique_temp_name("bypass");
        let (file, guard) = create_exclusive_temp(&parent_dir, &tmp_name)?;
        Ok(Self {
            file,
            hasher: Sha256::new(),
            written: 0,
            expected,
            mode,
            target: WriterTarget::DirectPath {
                parent_dir,
                tmp_name,
                dest_name,
                guard,
            },
        })
    }

    /// Feeds `chunk` into the SHA-256 hasher and writes it to the underlying temp file.
    ///
    /// Fails immediately with [`VerifiedWriteError::DigestMismatch`] if total written bytes would
    /// exceed `expected.size()`.
    ///
    /// # Errors
    ///
    /// Returns [`VerifiedWriteError::DigestMismatch`] on size overflow or [`VerifiedWriteError::Io`]
    /// on write failure.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), VerifiedWriteError> {
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let next_written = self
            .written
            .checked_add(chunk_len)
            .ok_or(VerifiedWriteError::DigestMismatch)?;
        if next_written > self.expected.size() {
            return Err(VerifiedWriteError::DigestMismatch);
        }
        self.hasher.update(chunk);
        self.file.write_all(chunk)?;
        self.written = next_written;
        Ok(())
    }

    /// Verifies exact byte length and SHA-256 digest, normalizes mtime and mode, and publishes
    /// the file atomically.
    ///
    /// # Errors
    ///
    /// Returns [`VerifiedWriteError::DigestMismatch`] if length or hash differs (leaving nothing
    /// visible), or [`VerifiedWriteError::Io`] on filesystem failure.
    pub fn commit(self) -> Result<(), VerifiedWriteError> {
        let Self {
            file,
            hasher,
            written,
            expected,
            mode,
            target,
        } = self;

        if written != expected.size() {
            return Err(VerifiedWriteError::DigestMismatch);
        }
        let actual_hash: [u8; 32] = hasher.finalize().into();
        if actual_hash != *expected.hash() {
            return Err(VerifiedWriteError::DigestMismatch);
        }

        normalize_file(&file, mode)?;
        publish_target(file, target)?;
        Ok(())
    }
}

fn publish_target(file: File, target: WriterTarget) -> io::Result<()> {
    match target {
        #[cfg(target_os = "linux")]
        WriterTarget::CasUnnamed {
            shard_dir,
            dest_name,
        } => {
            let proc_path = format!("/proc/self/fd/{}", file.as_raw_fd());
            match linkat(
                CWD,
                proc_path.as_str(),
                &shard_dir,
                dest_name.as_str(),
                AtFlags::SYMLINK_FOLLOW,
            ) {
                Ok(()) | Err(Errno::EXIST) => Ok(()),
                Err(Errno::NOENT) => {
                    TMPFILE_SUPPORTED.store(false, Ordering::Relaxed);
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "/proc/self/fd entry unavailable for O_TMPFILE linkat",
                    ))
                }
                Err(error) => Err(error.into()),
            }
        }
        WriterTarget::CasNamed {
            tmp_dir,
            tmp_name,
            shard_dir,
            dest_name,
            guard,
        } => {
            drop(file);
            // Prefer non-replacing link when supported so an existing hardlinked inode stays intact;
            // fall back to atomic rename when linkat is unavailable or cross-device.
            #[cfg(target_os = "linux")]
            match linkat(
                &tmp_dir,
                tmp_name.as_str(),
                &shard_dir,
                dest_name.as_str(),
                AtFlags::empty(),
            ) {
                Ok(()) | Err(Errno::EXIST) => {
                    drop(guard);
                    return Ok(());
                }
                Err(Errno::PERM | Errno::OPNOTSUPP | Errno::XDEV) => {}
                Err(error) => return Err(error.into()),
            }

            renameat(&tmp_dir, tmp_name.as_str(), &shard_dir, dest_name.as_str())?;
            guard.persist();
            Ok(())
        }
        WriterTarget::DirectPath {
            parent_dir,
            tmp_name,
            dest_name,
            guard,
        } => {
            drop(file);
            renameat(
                &parent_dir,
                tmp_name.as_str(),
                &parent_dir,
                dest_name.as_str(),
            )?;
            guard.persist();
            Ok(())
        }
    }
}

/// Hard-links `src` to `dest` (`§6.3`), falling back to a `CoW` reflink on `EMLINK` (Linux
/// `ioctl_ficlone` / macOS `fclonefileat`), and finally to a metadata-preserving copy when
/// hard-linking or reflinking is unsupported (`EXDEV`, `EMLINK` without `CoW`).
///
/// # Errors
///
/// Returns any filesystem error creating `dest`.
pub fn hardlink_or_reflink_file(src: &Path, dest: &Path, mode: BlobMode) -> io::Result<()> {
    match fs::hard_link(src, dest) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            match fs::hard_link(src, dest) {
                Ok(()) => return Ok(()),
                Err(retry_err) if is_hardlink_fallback_errno(&retry_err) => {}
                Err(retry_err) => return Err(retry_err),
            }
        }
        Err(error) if is_hardlink_fallback_errno(&error) => {}
        Err(error) => return Err(error),
    }

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }

    #[cfg(target_os = "linux")]
    {
        if try_linux_ficlone(src, dest, mode)? {
            return Ok(());
        }
    }

    #[cfg(target_os = "macos")]
    {
        if try_macos_clonefile(src, dest, mode)? {
            return Ok(());
        }
    }

    copy_with_metadata(src, dest, mode)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn is_hardlink_fallback_errno(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code)
            if code == Errno::MLINK.raw_os_error()
                || code == Errno::XDEV.raw_os_error()
                || code == Errno::OPNOTSUPP.raw_os_error()
                || code == Errno::PERM.raw_os_error()
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn is_hardlink_fallback_errno(_error: &io::Error) -> bool {
    false
}

/// Creates an empty file at `dest` with `mode` and deterministic `mtime = 2000-01-01T00:00:00Z`.
///
/// # Errors
///
/// Returns any filesystem error creating or normalizing `dest`.
pub fn create_empty_file(dest: &Path, mode: BlobMode) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    normalize_file(&file, mode)?;
    Ok(())
}

/// Copies `src` to a new file `dest` with `mode` and deterministic `mtime = 2000-01-01T00:00:00Z`.
///
/// # Errors
///
/// Returns any filesystem error reading `src` or writing `dest`.
pub fn copy_with_metadata(src: &Path, dest: &Path, mode: BlobMode) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let parent = dest.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination path has no parent directory",
        )
    })?;
    let dest_name = dest
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination filename is missing or non-UTF-8",
            )
        })?;
    let parent_dir = open_dir(parent)?;
    let tmp_name = unique_temp_name("copy");
    let (mut dest_file, guard) = create_exclusive_temp(&parent_dir, &tmp_name)?;
    let mut src_file = File::open(src)?;
    io::copy(&mut src_file, &mut dest_file)?;
    normalize_file(&dest_file, mode)?;
    drop(dest_file);
    renameat(&parent_dir, tmp_name.as_str(), &parent_dir, dest_name)?;
    guard.persist();
    Ok(())
}

#[cfg(target_os = "linux")]
fn try_linux_ficlone(src: &Path, dest: &Path, mode: BlobMode) -> io::Result<bool> {
    let src_file = File::open(src)?;
    let parent = dest.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination path has no parent directory",
        )
    })?;
    let dest_name = dest
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination filename is missing or non-UTF-8",
            )
        })?;
    let parent_dir = open_dir(parent)?;
    let dest_file = File::from(openat(
        &parent_dir,
        dest_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    if ioctl_ficlone(&dest_file, &src_file).is_ok() {
        normalize_file(&dest_file, mode)?;
        return Ok(true);
    }
    drop(dest_file);
    let _ = unlinkat(&parent_dir, dest_name, AtFlags::empty());
    Ok(false)
}

#[cfg(target_os = "macos")]
fn try_macos_clonefile(src: &Path, dest: &Path, mode: BlobMode) -> io::Result<bool> {
    let src_file = File::open(src)?;
    let parent = dest.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination path has no parent directory",
        )
    })?;
    let dest_name = dest
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination filename is missing or non-UTF-8",
            )
        })?;
    let parent_dir = open_dir(parent)?;
    match fclonefileat(&src_file, &parent_dir, dest_name, CloneFlags::empty()) {
        Ok(()) => {
            if let Ok(fd) = openat(
                &parent_dir,
                dest_name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                let cloned = File::from(fd);
                normalize_file(&cloned, mode)?;
            }
            Ok(true)
        }
        Err(Errno::NOTSUP | Errno::XDEV | Errno::ACCESS) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn open_dir(path: &Path) -> io::Result<rustix::fd::OwnedFd> {
    Ok(openat(
        rustix::fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?)
}

fn create_exclusive_temp(
    dir: &rustix::fd::OwnedFd,
    name: &str,
) -> io::Result<(File, TempFileGuard)> {
    let fd = openat(
        dir,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?;
    let dir_dup = rustix::io::fcntl_dupfd_cloexec(dir, 0)?;
    let guard = TempFileGuard {
        dir: dir_dup,
        name: name.to_owned(),
        persisted: false,
    };
    Ok((File::from(fd), guard))
}

fn unique_temp_name(prefix: &str) -> String {
    let seq = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    format!(".{prefix}-{}-{seq}", std::process::id())
}

#[cfg(test)]
mod tests {
    use gang_types::Digest;

    use super::{entry_file_name, parse_entry_name, shard_dir_name};
    use crate::lru::BlobMode;

    #[test]
    fn parses_plain_and_executable_names_and_rejects_invalid() {
        let digest = Digest::from_bytes(b"layout-test");
        let plain = entry_file_name(crate::lru::CacheKey::new(digest, BlobMode::Plain));
        let exec = entry_file_name(crate::lru::CacheKey::new(digest, BlobMode::Executable));

        assert_eq!(parse_entry_name(&plain), Some((digest, BlobMode::Plain)));
        assert_eq!(
            parse_entry_name(&exec),
            Some((digest, BlobMode::Executable))
        );
        assert_eq!(shard_dir_name(&digest), digest.hex()[..2]);

        let empty_name = format!("{}-0", Digest::EMPTY.hex());
        assert_eq!(parse_entry_name(&empty_name), None);
        assert_eq!(parse_entry_name("not-a-valid-entry"), None);
        assert_eq!(
            parse_entry_name(&format!(
                "{}-{}",
                digest.hex().to_ascii_uppercase(),
                digest.size()
            )),
            None
        );
    }

    #[test]
    fn temp_file_guard_persist_closes_dup_fd_without_leaking() -> std::io::Result<()> {
        use std::os::fd::AsRawFd as _;

        let dir = tempfile::tempdir()?;
        let root = super::open_dir(dir.path())?;
        let probe_before = super::open_dir(dir.path())?;
        let baseline_fd = probe_before.as_raw_fd();
        drop(probe_before);

        for i in 0..256_u32 {
            let name = format!("tmp-{i}");
            let (file, guard) = super::create_exclusive_temp(&root, &name)?;
            drop(file);
            guard.persist();
        }

        let probe_after = super::open_dir(dir.path())?;
        let after_fd = probe_after.as_raw_fd();
        drop(probe_after);

        assert!(
            after_fd < baseline_fd + 64,
            "TempFileGuard::persist leaked file descriptors: baseline={baseline_fd}, after={after_fd}"
        );
        Ok(())
    }
}
