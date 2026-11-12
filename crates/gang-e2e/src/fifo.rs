//! Private named pipes shared by the hold signal and execution journal.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use rustix::event::{Nsecs, PollFd, PollFlags, Timespec, poll};
use rustix::io::Errno;

use crate::{E2eError, Result};

/// Creates the named pipe at `path` readable and writable by its owner only, subject to the
/// process umask, and opens it for reading and writing so neither side's open blocks and a reader
/// never sees end of file. `what` names the pipe in errors.
pub(crate) fn create_private(path: &Path, what: &str) -> Result<File> {
    gang_os::fs::create_private_fifo(path).map_err(|error| {
        E2eError::io(format!("creating {what} {}", path.display()))(error.source)
    })?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(E2eError::io(format!("opening {what} {}", path.display())))
}

/// Waits for a pipe to become readable, bounding interruptions by the same timeout.
pub(crate) fn await_readable(file: &File, timeout: Duration, condition: &str) -> Result<()> {
    let start = Instant::now();
    loop {
        let remaining = timeout.saturating_sub(start.elapsed());
        let timespec = Timespec {
            tv_sec: i64::try_from(remaining.as_secs()).unwrap_or(i64::MAX),
            tv_nsec: Nsecs::from(remaining.subsec_nanos()),
        };
        let mut fds = [PollFd::new(file, PollFlags::IN)];
        match poll(&mut fds, Some(&timespec)) {
            Ok(0) => {
                return Err(E2eError::timeout(condition, timeout));
            }
            Ok(_)
                if fds[0]
                    .revents()
                    .intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL) =>
            {
                return Err(E2eError::system(format!(
                    "polling for {condition} reported an error"
                )));
            }
            Ok(_) if fds[0].revents().contains(PollFlags::IN) => return Ok(()),
            Ok(_) => {}
            Err(error) if error == Errno::INTR => {}
            Err(error) => {
                return Err(io::Error::from(error))
                    .map_err(E2eError::io(format!("polling for {condition}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};

    use super::create_private;
    use crate::E2eError;

    #[test]
    fn creates_a_private_fifo() -> crate::Result<()> {
        let directory =
            tempfile::tempdir().map_err(E2eError::io("creating a temporary directory"))?;
        let path = directory.path().join("pipe");
        let _file = create_private(&path, "the pipe")?;
        let metadata = std::fs::metadata(&path).map_err(E2eError::io("reading pipe metadata"))?;
        assert!(metadata.file_type().is_fifo());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        Ok(())
    }

    #[test]
    fn existing_path_is_an_io_error() -> crate::Result<()> {
        let directory =
            tempfile::tempdir().map_err(E2eError::io("creating a temporary directory"))?;
        let path = directory.path().join("pipe");
        let _file = create_private(&path, "the pipe")?;
        let error = create_private(&path, "the pipe").expect_err("the pipe already exists");
        assert!(
            matches!(error, E2eError::Io { source, .. } if source.kind() == io::ErrorKind::AlreadyExists)
        );
        Ok(())
    }

    #[test]
    fn missing_directory_is_an_io_error() -> crate::Result<()> {
        let directory =
            tempfile::tempdir().map_err(E2eError::io("creating a temporary directory"))?;
        let path = directory.path().join("absent").join("pipe");
        let error = create_private(&path, "the pipe").expect_err("the directory does not exist");
        assert!(
            matches!(error, E2eError::Io { source, .. } if source.kind() == io::ErrorKind::NotFound)
        );
        Ok(())
    }
}
