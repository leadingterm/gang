//! Scratch volume adoption. A root service prepares the instance's scratch volume before the
//! worker starts; this module only adopts it. Adoption is read-only: it verifies that the mount
//! point is a mount, that it holds xfs with reflink support, and reports the space available on
//! it. The worker runs unprivileged, so it never formats or mounts anything.

use std::io;
use std::path::{Path, PathBuf};

use gang_os::PathIo;
use gang_spi::MountedScratchVolume;
use gang_types::Bytes;

use crate::errors::ScratchError;

/// Filesystem magic reported by `statfs` for xfs (`XFS_SUPER_MAGIC`).
const XFS_SUPER_MAGIC: i64 = 0x5846_5342;

/// Read-only host probes. Every method is synchronous and touches no state.
pub(crate) trait Machine: Send + Sync + 'static {
    /// Returns the device number the path lives on.
    fn device(&self, path: &Path) -> io::Result<u64>;

    /// Returns the filesystem holding the path.
    fn filesystem(&self, path: &Path) -> io::Result<FilesystemKind>;

    /// Reports whether files in `directory` can share extents by reflink.
    fn can_reflink(&self, directory: &Path) -> io::Result<bool>;

    /// Returns the space available to an unprivileged user on the filesystem holding the path.
    fn capacity(&self, path: &Path) -> Result<Bytes, PathIo>;
}

/// The filesystem families the scratch volume may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemKind {
    /// An xfs filesystem.
    Xfs,
    /// Any other filesystem.
    Other,
}

/// Adopts the scratch volume mounted at `mount_point`.
pub(crate) struct Scratch<M: Machine> {
    mount_point: PathBuf,
    machine: M,
}

impl<M: Machine> Scratch<M> {
    /// Creates an adopter for the volume at `mount_point`, probed through `machine`.
    pub(crate) fn new(mount_point: PathBuf, machine: M) -> Self {
        Self {
            mount_point,
            machine,
        }
    }

    /// Verifies the volume at the mount point and returns it with the space available on it. Calling
    /// it again returns an equal volume.
    ///
    /// # Errors
    ///
    /// [`ScratchError::NotMounted`] when the mount point shares a device with its parent,
    /// [`ScratchError::NotXfs`] or [`ScratchError::NoReflink`] when the volume lacks the
    /// required filesystem features, [`ScratchError::Probe`] when a host probe fails, and
    /// [`ScratchError::Space`] when the space available cannot be read.
    pub(crate) fn adopt(&self) -> Result<MountedScratchVolume, ScratchError> {
        let mount_point = self.mount_point.as_path();
        // A root mount point has no parent; comparing it with itself reports it as unmounted.
        let parent = mount_point.parent().unwrap_or(mount_point);
        let mount_device = probe(mount_point, self.machine.device(mount_point))?;
        let parent_device = probe(parent, self.machine.device(parent))?;
        if mount_device == parent_device {
            return Err(ScratchError::NotMounted {
                path: self.mount_point.clone(),
            });
        }
        let filesystem = probe(mount_point, self.machine.filesystem(mount_point))?;
        if filesystem != FilesystemKind::Xfs {
            return Err(ScratchError::NotXfs {
                path: self.mount_point.clone(),
            });
        }
        if !probe(mount_point, self.machine.can_reflink(mount_point))? {
            return Err(ScratchError::NoReflink {
                path: self.mount_point.clone(),
            });
        }
        let capacity = self.machine.capacity(mount_point)?;
        Ok(MountedScratchVolume {
            path: self.mount_point.clone(),
            capacity,
        })
    }
}

/// Attaches the probed path to an io failure.
fn probe<T>(path: &Path, result: io::Result<T>) -> Result<T, ScratchError> {
    result.map_err(|source| ScratchError::Probe {
        path: path.to_path_buf(),
        source,
    })
}

/// The real host, probed through the kernel.
pub(crate) struct SystemMachine;

impl Machine for SystemMachine {
    fn device(&self, path: &Path) -> io::Result<u64> {
        use std::os::unix::fs::MetadataExt;

        Ok(std::fs::metadata(path)?.dev())
    }

    fn filesystem(&self, path: &Path) -> io::Result<FilesystemKind> {
        let magic = fs_magic(path)?;
        Ok(if magic == XFS_SUPER_MAGIC {
            FilesystemKind::Xfs
        } else {
            FilesystemKind::Other
        })
    }

    fn can_reflink(&self, directory: &Path) -> io::Result<bool> {
        reflinks(directory)
    }

    fn capacity(&self, path: &Path) -> Result<Bytes, PathIo> {
        gang_os::fs::space(path).map(|space| space.available)
    }
}

/// Returns the `statfs` filesystem magic of `path`.
#[cfg(target_os = "linux")]
fn fs_magic(path: &Path) -> io::Result<i64> {
    Ok(rustix::fs::statfs(path)?.f_type as i64)
}

/// Returns the `statfs` filesystem magic of `path`; statfs magics are Linux-only.
#[cfg(not(target_os = "linux"))]
fn fs_magic(_path: &Path) -> io::Result<i64> {
    Err(io::ErrorKind::Unsupported.into())
}

/// Tests whether `directory` supports reflinks by cloning a probe file into another unnamed
/// file on the same filesystem. Both files vanish when closed.
#[cfg(target_os = "linux")]
fn reflinks(directory: &Path) -> io::Result<bool> {
    use std::fs::File;
    use std::io::Write;

    use rustix::fs::{Mode, OFlags};
    use rustix::io::Errno;

    const PROBE_BYTES: usize = 4096;

    let flags = OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC;
    let mode = Mode::from_raw_mode(0o600);
    let mut source = File::from(rustix::fs::open(directory, flags, mode)?);
    let destination = rustix::fs::open(directory, flags, mode)?;
    source.write_all(&[0u8; PROBE_BYTES])?;
    match rustix::fs::ioctl_ficlone(&destination, &source) {
        Ok(()) => Ok(true),
        Err(Errno::OPNOTSUPP) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Tests reflink support; the probe relies on Linux-only ioctls.
#[cfg(not(target_os = "linux"))]
fn reflinks(_directory: &Path) -> io::Result<bool> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::error::Error;
    use std::io::ErrorKind;

    const MOUNT: &str = "/srv/scratch";
    const PARENT: &str = "/srv";
    const VOLUME_BYTES: u64 = 4096;

    /// A machine with fixed answers. `failure` makes every probe fail with that kind.
    #[derive(Debug)]
    struct FakeMachine {
        devices: HashMap<PathBuf, u64>,
        filesystem: FilesystemKind,
        reflinks: bool,
        capacity: u64,
        failure: Option<ErrorKind>,
    }

    impl FakeMachine {
        /// A mount point on its own device holding `filesystem`.
        fn mounted(filesystem: FilesystemKind, reflinks: bool) -> Self {
            let devices = HashMap::from([(PathBuf::from(PARENT), 1), (PathBuf::from(MOUNT), 2)]);
            Self {
                devices,
                filesystem,
                reflinks,
                capacity: VOLUME_BYTES,
                failure: None,
            }
        }

        fn check(&self) -> io::Result<()> {
            match self.failure {
                Some(kind) => Err(io::Error::from(kind)),
                None => Ok(()),
            }
        }
    }

    impl Machine for FakeMachine {
        fn device(&self, path: &Path) -> io::Result<u64> {
            self.check()?;
            self.devices
                .get(path)
                .copied()
                .ok_or_else(|| io::Error::from(ErrorKind::NotFound))
        }

        fn filesystem(&self, _path: &Path) -> io::Result<FilesystemKind> {
            self.check()?;
            Ok(self.filesystem)
        }

        fn can_reflink(&self, _directory: &Path) -> io::Result<bool> {
            self.check()?;
            Ok(self.reflinks)
        }

        fn capacity(&self, path: &Path) -> Result<Bytes, PathIo> {
            self.check().map_err(PathIo::at("statvfs", path))?;
            Ok(Bytes::new(self.capacity))
        }
    }

    fn adopt(machine: FakeMachine) -> Result<MountedScratchVolume, ScratchError> {
        Scratch::new(PathBuf::from(MOUNT), machine).adopt()
    }

    #[test]
    fn unmounted_path_is_rejected() {
        let mut machine = FakeMachine::mounted(FilesystemKind::Xfs, true);
        machine.devices.insert(PathBuf::from(MOUNT), 1);
        assert!(matches!(
            adopt(machine),
            Err(ScratchError::NotMounted { path }) if path == Path::new(MOUNT)
        ));
    }

    #[test]
    fn non_xfs_mount_is_rejected() {
        let machine = FakeMachine::mounted(FilesystemKind::Other, true);
        assert!(matches!(
            adopt(machine),
            Err(ScratchError::NotXfs { path }) if path == Path::new(MOUNT)
        ));
    }

    #[test]
    fn xfs_without_reflink_is_rejected() {
        let machine = FakeMachine::mounted(FilesystemKind::Xfs, false);
        assert!(matches!(
            adopt(machine),
            Err(ScratchError::NoReflink { path }) if path == Path::new(MOUNT)
        ));
    }

    #[test]
    fn probe_failure_is_reported_with_its_path() -> Result<(), Box<dyn Error>> {
        let mut machine = FakeMachine::mounted(FilesystemKind::Xfs, true);
        machine.failure = Some(ErrorKind::PermissionDenied);
        let Err(ScratchError::Probe { path, source }) = adopt(machine) else {
            return Err("adoption did not report a probe failure".into());
        };
        assert_eq!(path, Path::new(MOUNT));
        assert_eq!(source.kind(), ErrorKind::PermissionDenied);
        Ok(())
    }

    #[test]
    fn good_volume_reports_path_and_capacity() -> Result<(), Box<dyn Error>> {
        let volume = adopt(FakeMachine::mounted(FilesystemKind::Xfs, true))?;
        assert_eq!(
            volume,
            MountedScratchVolume {
                path: PathBuf::from(MOUNT),
                capacity: Bytes::new(VOLUME_BYTES),
            }
        );
        Ok(())
    }

    #[test]
    fn adopting_twice_returns_equal_volumes() -> Result<(), Box<dyn Error>> {
        let scratch = Scratch::new(
            PathBuf::from(MOUNT),
            FakeMachine::mounted(FilesystemKind::Xfs, true),
        );
        let first = scratch.adopt()?;
        let second = scratch.adopt()?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn system_machine_probes_a_directory() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let machine = SystemMachine;
        machine.device(dir.path())?;
        assert!(machine.capacity(dir.path())?.get() > 0);
        Ok(())
    }

    #[test]
    fn system_machine_capacity_stays_within_total() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let space = gang_os::fs::space(dir.path())?;
        assert!(space.available.get() <= space.total.get());
        assert!(SystemMachine.capacity(dir.path())?.get() > 0);
        Ok(())
    }

    #[test]
    fn system_machine_probes_fail_for_missing_path() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let missing = dir.path().join("absent");
        let machine = SystemMachine;
        assert!(machine.device(&missing).is_err());
        assert!(machine.filesystem(&missing).is_err());
        assert!(machine.can_reflink(&missing).is_err());
        Ok(())
    }
}
