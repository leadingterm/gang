//! Breaking hard links in an action's work directory.
//!
//! `NativeLink` hard-links content-addressed inputs into the work directory. An
//! action that rewrites such a file in place would also rewrite the cached
//! blob, poisoning the cache. Each linked regular file is therefore replaced
//! by a private copy (a reflink where the filesystem supports one) before the
//! action runs. All traversal is fd-relative and never follows symlinks. A copy
//! is built under the name `.gang-exec-<pid>` (this process's id) in the
//! directory of its link, and an existing entry with that name is never removed.
//! Links are replaced only after their directory has been fully listed, so no
//! entry is renamed while it is being read.

use std::path::Path;

use rustix::fs::{AtFlags, CWD, Dir, FileType, Mode, OFlags, openat, statat};

use crate::errors::LinkBreakError;
use crate::private_copy;

/// Replaces every hard-linked regular file under `root` with a private copy.
///
/// Symbolic links are never followed, and files with a single link are left
/// untouched. Each replacement keeps the original permission bits.
///
/// Returns the number of files replaced.
///
/// # Errors
///
/// Returns [`LinkBreakError`] naming the path that could not be opened, read,
/// copied, or renamed. The directory tree may be partially processed on error.
pub fn break_hard_links(root: &Path) -> Result<usize, LinkBreakError> {
    let fd = openat(
        CWD,
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| LinkBreakError {
        path: root.to_owned(),
        source: source.into(),
    })?;
    let directory = Dir::new(fd).map_err(|source| LinkBreakError {
        path: root.to_owned(),
        source: source.into(),
    })?;
    let temp_name = format!(".gang-exec-{}", std::process::id());
    break_in(directory, &temp_name).map_err(|error| LinkBreakError {
        path: root.join(error.path),
        source: error.source,
    })
}

/// Breaks hard links in `directory`, recursing into subdirectories.
///
/// Subdirectories are walked as they are listed. Hard-linked regular files are
/// collected while the directory is read, then each is re-checked and replaced
/// once the directory has been fully listed. Private copies are built as
/// `temp_name` in the directory of each link. Error paths are relative to
/// `directory`.
fn break_in(mut directory: Dir, temp_name: &str) -> Result<usize, LinkBreakError> {
    let mut linked = Vec::new();
    let mut replaced = 0;
    while let Some(entry) = directory.read() {
        let entry = entry.map_err(|source| LinkBreakError::at(c".", source))?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        let fd = directory
            .fd()
            .map_err(|source| LinkBreakError::at(c".", source))?;
        let stat = statat(fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|source| LinkBreakError::at(name, source))?;
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => {
                let child = openat(
                    fd,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|source| LinkBreakError::at(name, source))?;
                let child = Dir::new(child).map_err(|source| LinkBreakError::at(name, source))?;
                replaced += break_in(child, temp_name).map_err(|error| error.within(name))?;
            }
            FileType::RegularFile if stat.st_nlink > 1 => linked.push(entry),
            _ => {}
        }
    }
    let fd = directory
        .fd()
        .map_err(|source| LinkBreakError::at(c".", source))?;
    for entry in &linked {
        let name = entry.file_name();
        let stat = statat(fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|source| LinkBreakError::at(name, source))?;
        if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile && stat.st_nlink > 1 {
            private_copy::replace(fd, name, Mode::from_raw_mode(stat.st_mode), temp_name)
                .map_err(|source| LinkBreakError::at(name, source))?;
            replaced += 1;
        }
    }
    Ok(replaced)
}
