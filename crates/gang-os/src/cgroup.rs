//! cgroup v2 trees: which cgroup a process is in, and killing and removing a tree.
//!
//! Real kill and removal behavior is exercised by the Linux Gate 2 scenarios because it needs a
//! delegated cgroup v2 tree.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::PathIo;

/// Root of the unified cgroup v2 hierarchy.
const UNIFIED_ROOT: &str = "/sys/fs/cgroup";

/// The outcome of [`kill_and_remove`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Removal {
    /// The tree is gone (or never existed).
    Removed,
    /// A directory of the tree still holds processes: they were sent SIGKILL and the kernel has
    /// not finished reaping them. Call again later.
    Busy,
}

/// The cgroup v2 directory process `pid` is in: the `0::<path>` line of `/proc/<pid>/cgroup`
/// joined under `/sys/fs/cgroup`. `None` when the process is gone or the host has no unified
/// hierarchy line (including hosts without `/proc`).
///
/// # Errors
///
/// Returns an error when the process cgroup file cannot be read for a reason other than absence.
pub fn of_process(pid: u32) -> Result<Option<PathBuf>, PathIo> {
    let path = PathBuf::from(format!("/proc/{pid}/cgroup"));
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(PathIo::new("read process cgroup", &path, error)),
    };
    Ok(unified_path(&text).map(|rel| Path::new(UNIFIED_ROOT).join(rel.trim_start_matches('/'))))
}

/// Finds the unified hierarchy path in the contents of a process cgroup file.
fn unified_path(text: &str) -> Option<&str> {
    text.lines().find_map(|line| line.strip_prefix("0::"))
}

/// Kills every process of the cgroup tree at `dir` by writing `1` to its `cgroup.kill`, then
/// removes the tree's directories, deepest first. A missing tree counts as removed.
///
/// On kernels without `cgroup.kill`, removal is still attempted, but processes are not killed.
///
/// # Errors
///
/// Returns an error if killing processes, enumerating the tree, or removing a directory fails,
/// except when the relevant path is missing or removal reports a busy directory.
pub fn kill_and_remove(dir: &Path) -> Result<Removal, PathIo> {
    let kill = dir.join("cgroup.kill");
    match fs::OpenOptions::new().write(true).open(&kill) {
        Ok(mut file) => file
            .write_all(b"1")
            .map_err(PathIo::at("kill cgroup", &kill))?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(PathIo::new("kill cgroup", &kill, error)),
    }
    remove_tree(dir)
}

/// Removes child directories before their parent, stopping when a directory is busy.
fn remove_tree(dir: &Path) -> Result<Removal, PathIo> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Removal::Removed),
        Err(error) => return Err(PathIo::new("read cgroup", dir, error)),
    };
    for entry in entries {
        let entry = entry.map_err(PathIo::at("read cgroup", dir))?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(PathIo::new("read cgroup", &path, error)),
        };
        if file_type.is_dir() && remove_tree(&path)? == Removal::Busy {
            return Ok(Removal::Busy);
        }
    }
    match fs::remove_dir(dir) {
        Ok(()) => Ok(Removal::Removed),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Removal::Removed),
        Err(error) if error.kind() == io::ErrorKind::ResourceBusy => Ok(Removal::Busy),
        Err(error) => Err(PathIo::new("remove cgroup", dir, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{Removal, kill_and_remove, of_process, unified_path};

    #[test]
    fn parses_unified_hierarchy() {
        assert_eq!(
            unified_path("0::/user.slice/a.scope\n"),
            Some("/user.slice/a.scope")
        );
        assert_eq!(unified_path("1:name=systemd:/x\n"), None);
        assert_eq!(unified_path(""), None);
    }

    #[test]
    fn missing_tree_is_removed() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        assert_eq!(
            kill_and_remove(&dir.path().join("missing"))?,
            Removal::Removed
        );
        Ok(())
    }

    #[test]
    fn missing_process_has_no_cgroup() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(of_process(u32::MAX)?, None);
        Ok(())
    }
}
