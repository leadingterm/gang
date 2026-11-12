//! The seam between the plugin and the kernel's cgroup v2 filesystem.
//!
//! Everything that touches the cgroup v2 filesystem or `/proc/self/cgroup` goes through
//! [`Cgroupfs`], so the controller logic can be tested against an in-memory fake.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::parse;

/// Procfs file listing the cgroup membership of the calling process.
pub(crate) const MEMBERSHIP_FILE: &str = "/proc/self/cgroup";

/// Procfs directory holding one entry per live process.
pub(crate) const PROC_ROOT: &str = "/proc";

/// Interface file listing the controllers the cgroup offers.
pub(crate) const CONTROLLERS: &str = "cgroup.controllers";
/// Interface file that reports whether the cgroup is populated.
pub(crate) const EVENTS: &str = "cgroup.events";
/// Interface file that enables controllers for the children of the cgroup.
pub(crate) const SUBTREE_CONTROL: &str = "cgroup.subtree_control";
/// Interface file that moves a process into the cgroup when written.
pub(crate) const PROCS: &str = "cgroup.procs";
/// Interface file that kills every process in the cgroup when written `1`.
pub(crate) const KILL: &str = "cgroup.kill";
/// Interface file holding the memory limit of the cgroup.
pub(crate) const MEMORY_MAX: &str = "memory.max";
/// Interface file holding the swap limit of the cgroup.
pub(crate) const MEMORY_SWAP_MAX: &str = "memory.swap.max";
/// Interface file that makes the OOM killer act on the whole cgroup.
pub(crate) const MEMORY_OOM_GROUP: &str = "memory.oom.group";
/// Interface file holding the peak memory use of the cgroup.
pub(crate) const MEMORY_PEAK: &str = "memory.peak";
/// Interface file holding the CPU bandwidth limit of the cgroup.
pub(crate) const CPU_MAX: &str = "cpu.max";
/// Interface file holding the CPU statistics of the cgroup.
pub(crate) const CPU_STAT: &str = "cpu.stat";
/// Interface file holding the process limit of the cgroup.
pub(crate) const PIDS_MAX: &str = "pids.max";

/// Systemd marker on cgroup directories delegated to unprivileged units.
#[cfg(target_os = "linux")]
const DELEGATE_XATTR: &str = "user.delegate";

/// Systemd marker on cgroup directories delegated to privileged units.
#[cfg(target_os = "linux")]
const TRUSTED_DELEGATE_XATTR: &str = "trusted.delegate";

/// Access to a cgroup v2 hierarchy.
///
/// Every cgroup path is relative to the cgroup root; the empty path denotes the
/// root itself.
pub(crate) trait Cgroupfs: Send + Sync {
    /// The cgroup the calling process is in.
    fn own_cgroup(&self) -> io::Result<PathBuf>;

    /// Where `cgroup` is on the host filesystem: the directory that a process outside this
    /// adapter, such as the `gang-join` trampoline, opens.
    fn absolute(&self, cgroup: &Path) -> PathBuf;

    /// Creates one cgroup directory. `AlreadyExists` is returned unchanged.
    fn create(&self, cgroup: &Path) -> io::Result<()>;

    /// Removes one cgroup directory. The kernel answers `ErrorKind::ResourceBusy`
    /// while the cgroup still has processes or child cgroups.
    fn remove(&self, cgroup: &Path) -> io::Result<()>;

    /// Reads the interface file `file` of `cgroup` as text.
    fn read(&self, cgroup: &Path, file: &str) -> io::Result<String>;

    /// Writes `value` to an existing interface file of `cgroup`. Never creates
    /// the file and never truncates a regular file.
    fn write(&self, cgroup: &Path, file: &str, value: &str) -> io::Result<()>;

    /// The direct child cgroups of `cgroup`, as full relative paths, in no
    /// particular order.
    fn children(&self, cgroup: &Path) -> io::Result<Vec<PathBuf>>;

    /// Whether the interface file exists in `cgroup`.
    fn has_file(&self, cgroup: &Path, file: &str) -> bool;

    /// Whether `cgroup` is delegated to the worker: systemd's `user.delegate` or `trusted.delegate` marker on the cgroup directory is `1`.
    fn delegated(&self, cgroup: &Path) -> io::Result<bool>;

    /// The start time of process `pid` in clock ticks since boot: field 22 of
    /// `/proc/<pid>/stat`. `None` when the process is a zombie, does not exist, or its stat cannot be read.
    fn process_start(&self, pid: u32) -> Option<u64>;
}

/// The real cgroup v2 filesystem, rooted at the configured cgroup root.
#[derive(Debug)]
pub(crate) struct Kernel {
    /// The mount point of the cgroup v2 filesystem.
    root: PathBuf,
}

impl Kernel {
    /// A cgroup v2 filesystem mounted at `root`.
    #[must_use]
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

/// Whether the extended attribute `name` of `directory` is set to `1`. An
/// attribute the filesystem or the caller cannot see counts as unset.
#[cfg(target_os = "linux")]
fn marker_set(directory: &Path, name: &str) -> io::Result<bool> {
    let mut value = [0_u8; 8];
    match rustix::fs::getxattr(directory, name, &mut value[..]) {
        Ok(length) => Ok(length > 0 && value.first() == Some(&b'1')),
        Err(errno)
            if [
                rustix::io::Errno::NODATA,
                rustix::io::Errno::PERM,
                rustix::io::Errno::ACCESS,
                rustix::io::Errno::OPNOTSUPP,
            ]
            .contains(&errno) =>
        {
            Ok(false)
        }
        Err(errno) => Err(errno.into()),
    }
}

/// Lists direct child directories of `abs` as paths relative to the cgroup root at `rel`.
pub(crate) fn child_directories(abs: &Path, rel: &Path) -> io::Result<Vec<PathBuf>> {
    let mut children = Vec::new();
    for entry in fs::read_dir(abs)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            children.push(rel.join(entry.file_name()));
        }
    }
    Ok(children)
}

impl Cgroupfs for Kernel {
    fn own_cgroup(&self) -> io::Result<PathBuf> {
        let membership = fs::read_to_string(MEMBERSHIP_FILE)?;
        parse::own_cgroup(&membership)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
    }

    fn absolute(&self, cgroup: &Path) -> PathBuf {
        self.root.join(cgroup)
    }

    fn create(&self, cgroup: &Path) -> io::Result<()> {
        fs::create_dir(self.absolute(cgroup))
    }

    fn remove(&self, cgroup: &Path) -> io::Result<()> {
        fs::remove_dir(self.absolute(cgroup))
    }

    fn read(&self, cgroup: &Path, file: &str) -> io::Result<String> {
        fs::read_to_string(self.absolute(cgroup).join(file))
    }

    fn write(&self, cgroup: &Path, file: &str, value: &str) -> io::Result<()> {
        let mut handle = OpenOptions::new()
            .write(true)
            .open(self.absolute(cgroup).join(file))?;
        handle.write_all(value.as_bytes())
    }

    fn children(&self, cgroup: &Path) -> io::Result<Vec<PathBuf>> {
        child_directories(&self.absolute(cgroup), cgroup)
    }

    fn has_file(&self, cgroup: &Path, file: &str) -> bool {
        self.absolute(cgroup).join(file).exists()
    }

    #[cfg(target_os = "linux")]
    fn delegated(&self, cgroup: &Path) -> io::Result<bool> {
        let directory = self.absolute(cgroup);
        for name in [DELEGATE_XATTR, TRUSTED_DELEGATE_XATTR] {
            if marker_set(&directory, name)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(not(target_os = "linux"))]
    fn delegated(&self, _cgroup: &Path) -> io::Result<bool> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn process_start(&self, pid: u32) -> Option<u64> {
        let path = Path::new(PROC_ROOT).join(pid.to_string()).join("stat");
        let stat = fs::read_to_string(path).ok()?;
        parse::process_start(&stat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A kernel adapter rooted at a fresh temporary directory, plus the guard keeping it alive.
    fn fixture() -> io::Result<(tempfile::TempDir, Kernel)> {
        let root = tempfile::tempdir()?;
        let kernel = Kernel::new(root.path().to_path_buf());
        Ok((root, kernel))
    }

    #[test]
    fn create_makes_directory_and_rejects_duplicate() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        assert!(root.path().join("group").is_dir());
        let error = kernel
            .create(Path::new("group"))
            .expect_err("second create must fail");
        assert!(matches!(error.kind(), io::ErrorKind::AlreadyExists));
        Ok(())
    }

    #[test]
    fn children_lists_only_directories_relative_to_cgroup() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("parent"))?;
        fs::create_dir(root.path().join("parent").join("child"))?;
        fs::write(root.path().join("parent").join("regular"), "")?;
        let children = kernel.children(Path::new("parent"))?;
        assert_eq!(children, vec![PathBuf::from("parent").join("child")]);
        Ok(())
    }

    #[test]
    fn write_updates_existing_interface_file() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        fs::write(root.path().join("group").join(MEMORY_MAX), "")?;
        kernel.write(Path::new("group"), MEMORY_MAX, "1024")?;
        assert_eq!(kernel.read(Path::new("group"), MEMORY_MAX)?, "1024");
        Ok(())
    }

    #[test]
    fn write_does_not_create_missing_file() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        let error = kernel
            .write(Path::new("group"), MEMORY_MAX, "1024")
            .expect_err("write to a missing file must fail");
        assert!(matches!(error.kind(), io::ErrorKind::NotFound));
        assert!(!root.path().join("group").join(MEMORY_MAX).exists());
        assert!(!kernel.has_file(Path::new("group"), MEMORY_MAX));
        Ok(())
    }

    #[test]
    fn write_does_not_truncate_existing_content() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        fs::write(root.path().join("group").join(MEMORY_MAX), "abcdef")?;
        kernel.write(Path::new("group"), MEMORY_MAX, "xy")?;
        assert_eq!(kernel.read(Path::new("group"), MEMORY_MAX)?, "xycdef");
        Ok(())
    }

    #[test]
    fn read_returns_file_text() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        fs::write(root.path().join("group").join(CONTROLLERS), "cpu memory\n")?;
        assert_eq!(
            kernel.read(Path::new("group"), CONTROLLERS)?,
            "cpu memory\n"
        );
        Ok(())
    }

    #[test]
    fn remove_deletes_empty_directory() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        kernel.remove(Path::new("group"))?;
        assert!(!root.path().join("group").exists());
        Ok(())
    }

    #[test]
    fn remove_reports_error_for_non_empty_directory() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        kernel.create(Path::new("group/child"))?;
        assert!(kernel.remove(Path::new("group")).is_err());
        assert!(root.path().join("group").is_dir());
        Ok(())
    }

    #[test]
    fn has_file_reports_presence() -> io::Result<()> {
        let (root, kernel) = fixture()?;
        kernel.create(Path::new("group"))?;
        fs::write(root.path().join("group").join(PROCS), "")?;
        assert!(kernel.has_file(Path::new("group"), PROCS));
        assert!(!kernel.has_file(Path::new("group"), KILL));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_start_reports_running_process() {
        let (_root, kernel) = fixture().expect("tempdir");
        assert!(kernel.process_start(std::process::id()).is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_start_reports_none_for_impossible_pid() {
        let (_root, kernel) = fixture().expect("tempdir");
        assert_eq!(kernel.process_start(u32::MAX), None);
    }
}
