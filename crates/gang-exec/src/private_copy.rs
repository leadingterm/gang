//! Replacing a hard-linked file with a private copy of its contents.
//!
//! The copy is built under a temporary name in the directory of the link and
//! then renamed over the link, so the link is never observed missing or
//! half-written. On Linux the copy is first made as an unnamed `O_TMPFILE` file
//! and given its name with `linkat`, which never replaces an existing entry;
//! when the fast path is unavailable, the copy is made under its name instead.
//! On macOS the copy is first attempted as a clone. Every name this module
//! creates is recorded in a [`TempEntry`] that removes it again if the
//! replacement fails, and never one that the call did not create.

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::fd::BorrowedFd;

use rustix::fs::{AtFlags, Mode, OFlags, fchmod, openat, renameat, unlinkat};

#[cfg(target_os = "linux")]
use rustix::fs::{CWD, ioctl_ficlone, linkat};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use rustix::io::Errno;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "macos")]
use rustix::fs::{CloneFlags, fclonefileat};

/// Whether the Linux unnamed-file fast path has not yet been found unavailable.
#[cfg(target_os = "linux")]
static TMPFILE_SUPPORTED: AtomicBool = AtomicBool::new(true);

/// Replaces the regular file `name` in `directory` with a private copy of itself.
///
/// The copy is built as `temp_name` in `directory`, renamed over `name`, and
/// carries `mode`. A failed step removes only the entry this call created.
pub(crate) fn replace(
    directory: BorrowedFd<'_>,
    name: &CStr,
    mode: Mode,
    temp_name: &str,
) -> io::Result<()> {
    let source = File::from(openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let entry = private_copy(directory, &source, mode, temp_name)?;
    renameat(directory, temp_name, directory, name)?;
    entry.persist();
    Ok(())
}

/// A directory entry created by this call, removed on drop unless persisted.
#[must_use]
struct TempEntry<'a> {
    directory: BorrowedFd<'a>,
    name: &'a str,
}

impl<'a> TempEntry<'a> {
    /// Records `name`, which the caller has just created in `directory`.
    fn created(directory: BorrowedFd<'a>, name: &'a str) -> Self {
        Self { directory, name }
    }

    /// Keeps the entry, whose name the caller has already moved away.
    fn persist(self) {
        std::mem::forget(self);
    }
}

impl Drop for TempEntry<'_> {
    fn drop(&mut self) {
        let _ = unlinkat(self.directory, self.name, AtFlags::empty());
    }
}

/// Creates the private copy of `source` as `temp_name` in `directory`.
///
/// An unnamed or cloned copy is tried first where the platform has one, then a named copy.
fn private_copy<'a>(
    directory: BorrowedFd<'a>,
    source: &File,
    mode: Mode,
    temp_name: &'a str,
) -> io::Result<TempEntry<'a>> {
    match cloned_copy(directory, source, mode, temp_name)? {
        Some(entry) => Ok(entry),
        None => named_copy(directory, source, mode, temp_name),
    }
}

/// Makes an unnamed copy of `source` and gives it `temp_name`.
///
/// Returns `None` when `O_TMPFILE` is unsupported or `/proc/self/fd` is absent.
/// `EOPNOTSUPP` (also `ENOTSUP`) and `EISDIR` from `openat` indicate that
/// the filesystem or kernel does not support `O_TMPFILE`. `ENOENT` from
/// `linkat` permits fallback only when the `/proc` source path is absent.
/// Other failures are returned without falling back.
#[cfg(target_os = "linux")]
fn cloned_copy<'a>(
    directory: BorrowedFd<'a>,
    source: &File,
    mode: Mode,
    temp_name: &'a str,
) -> io::Result<Option<TempEntry<'a>>> {
    if !TMPFILE_SUPPORTED.load(Ordering::Relaxed) {
        return Ok(None);
    }
    let dest = match openat(
        directory,
        c".",
        OFlags::TMPFILE | OFlags::WRONLY | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    ) {
        Ok(fd) => File::from(fd),
        // Linux reports OPNOTSUPP (also ENOTSUP) or ISDIR when O_TMPFILE
        // is unsupported by the filesystem or kernel.
        Err(Errno::OPNOTSUPP | Errno::ISDIR) => {
            TMPFILE_SUPPORTED.store(false, Ordering::Relaxed);
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let proc_path = format!("/proc/self/fd/{}", dest.as_raw_fd());
    match linkat(
        CWD,
        proc_path.as_str(),
        directory,
        temp_name,
        AtFlags::SYMLINK_FOLLOW,
    ) {
        Ok(()) => {
            let entry = TempEntry::created(directory, temp_name);
            fill(source, &dest, mode)?;
            Ok(Some(entry))
        }
        Err(Errno::NOENT)
            if matches!(
                rustix::fs::statat(CWD, proc_path.as_str(), AtFlags::SYMLINK_NOFOLLOW),
                Err(Errno::NOENT)
            ) =>
        {
            // Without /proc/self/fd the unnamed file cannot be linked by this path.
            TMPFILE_SUPPORTED.store(false, Ordering::Relaxed);
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Clones `source` to `temp_name` where the filesystem supports it.
///
/// Returns `None`, leaving nothing behind, when the filesystem cannot clone
/// (macOS: ENOTSUP, EXDEV). Any other failure is returned.
#[cfg(target_os = "macos")]
fn cloned_copy<'a>(
    directory: BorrowedFd<'a>,
    source: &File,
    _mode: Mode,
    temp_name: &'a str,
) -> io::Result<Option<TempEntry<'a>>> {
    // A clone carries the source's permission bits, so no `fchmod` is needed.
    match fclonefileat(source, directory, temp_name, CloneFlags::empty()) {
        Ok(()) => Ok(Some(TempEntry::created(directory, temp_name))),
        Err(error) if error == Errno::NOTSUP || error == Errno::XDEV => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Creates `temp_name` exclusively in `directory` and fills it with a copy of `source`.
fn named_copy<'a>(
    directory: BorrowedFd<'a>,
    source: &File,
    mode: Mode,
    temp_name: &'a str,
) -> io::Result<TempEntry<'a>> {
    let dest = File::from(openat(
        directory,
        temp_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let entry = TempEntry::created(directory, temp_name);
    fill(source, &dest, mode)?;
    Ok(entry)
}

/// Writes the contents of `source` into the empty `dest`, then applies `mode`.
///
/// On Linux the contents are reflinked when the filesystem allows it, and copied otherwise.
fn fill(mut source: &File, mut dest: &File, mode: Mode) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if ioctl_ficlone(dest, source).is_ok() {
        return Ok(fchmod(dest, mode)?);
    }
    io::copy(&mut source, &mut dest)?;
    Ok(fchmod(dest, mode)?)
}
