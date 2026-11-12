//! The rendezvous between the harness and the hold probe: a named pipe in the signal directory.
//!
//! The hold action runs in a sandbox whose only host mounts are its working directory, the
//! configuration's read-only binds, and an empty `tmp_path`, so a file it creates is invisible to
//! the harness. A named pipe is visible through a bind. The harness keeps the pipe open for reading
//! and writing, so opening it never blocks. Once the hold action's orphan runs, it opens the pipe
//! for writing and writes a byte, which ends [`Signal::wait`]. When the hold build ends without
//! the orphan ever running, the harness itself calls [`Signal::notify`], so a wait always ends.
//! The harness bounds its wait with a deadline, so a hold action that never signals fails its
//! check instead of hanging the run.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::Duration;

use crate::{E2eError, Result, fifo};

/// The byte written by the hold probe once its orphan runs.
pub const STARTED: u8 = b's';

/// The byte written when the build ends without waiting for its orphan.
const ABANDONED: u8 = b'a';

/// The shell command the hold probe's orphan runs once it exists. `printf` is a shell builtin,
/// not an external program; it writes one byte to the quoted signal pipe path.
pub const SIGNAL_COMMAND: &str = "printf s >\"$E2E_SIGNAL\"";

/// The outcome of waiting for the hold probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signalled {
    /// The orphan runs.
    Started,
    /// The build ended before its orphan signalled.
    Abandoned,
}

/// The harness's end of the named pipe the hold probe signals through.
///
/// The pipe is open for reading and writing: the open never blocks, and the write end the harness
/// holds keeps a reader from seeing end of file before a byte arrives.
#[derive(Debug)]
pub struct Signal {
    file: File,
}

impl Signal {
    /// Creates the named pipe at `path` with owner read and write permissions, subject to the
    /// process umask, and opens it for reading and writing.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the pipe cannot be created or opened.
    pub fn create(path: &Path) -> Result<Self> {
        let file = fifo::create_private(path, "the signal")?;
        Ok(Self { file })
    }

    /// Blocks until the hold probe signals, for at most `timeout`: until a byte written by its
    /// orphan, or by [`Signal::notify`], can be read.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when no byte arrives within `timeout`, [`E2eError::Io`] when the pipe
    /// cannot be polled or read, or [`E2eError::System`] for an unexpected byte.
    pub fn wait(&self, timeout: Duration) -> Result<Signalled> {
        fifo::await_readable(
            &self.file,
            timeout,
            "the hold action to signal that its orphan runs",
        )
        .map_err(|error| match error {
            E2eError::Io { source, .. } => E2eError::io("polling the signal")(source),
            E2eError::System { .. } => E2eError::system("the signal pipe reported an error"),
            other => other,
        })?;
        let mut byte = [0_u8; 1];
        loop {
            match (&self.file).read(&mut byte) {
                Ok(_) => {
                    return match byte[0] {
                        STARTED => Ok(Signalled::Started),
                        ABANDONED => Ok(Signalled::Abandoned),
                        _ => Err(E2eError::system("unexpected byte on the signal pipe")),
                    };
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error).map_err(E2eError::io("reading the signal")),
            }
        }
    }

    /// Signals the pipe, so that a pending or future [`Signal::wait`] returns.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the pipe cannot be written.
    pub fn notify(&self) -> Result<()> {
        (&self.file)
            .write_all(&[ABANDONED])
            .map_err(E2eError::io("writing the signal"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{self, Write};
    use std::thread;
    use std::time::Duration;

    use super::{SIGNAL_COMMAND, STARTED, Signal, Signalled};
    use crate::E2eError;

    /// Bounds the waits in these tests. It only matters when a test is broken: a wait that should
    /// succeed returns as soon as its byte arrives.
    const WAIT: Duration = Duration::from_secs(30);

    fn directory() -> crate::Result<tempfile::TempDir> {
        tempfile::tempdir().map_err(E2eError::io("creating a temporary directory"))
    }

    #[test]
    fn a_writer_that_opens_the_fifo_releases_wait() -> crate::Result<()> {
        let directory = directory()?;
        let path = directory.path().join("started");
        let signal = Signal::create(&path)?;
        let writer_path = path.clone();
        let writer = thread::spawn(move || -> io::Result<()> {
            let mut fifo = OpenOptions::new().write(true).open(writer_path)?;
            fifo.write_all(&[STARTED])
        });
        assert_eq!(signal.wait(WAIT)?, Signalled::Started);
        writer
            .join()
            .expect("the signal writer does not panic")
            .map_err(E2eError::io("writing the signal"))?;
        assert!(matches!(
            signal.wait(Duration::ZERO),
            Err(E2eError::Timeout { .. })
        ));
        Ok(())
    }

    #[test]
    fn notify_releases_wait() -> crate::Result<()> {
        let directory = directory()?;
        let path = directory.path().join("started");
        let signal = Signal::create(&path)?;
        signal.notify()?;
        assert_eq!(signal.wait(WAIT)?, Signalled::Abandoned);
        Ok(())
    }

    #[test]
    fn signal_command_writes_the_started_byte() {
        assert_eq!(
            SIGNAL_COMMAND,
            format!("printf {} >\"$E2E_SIGNAL\"", char::from(STARTED))
        );
    }

    #[test]
    fn a_writer_that_writes_nothing_times_out() -> crate::Result<()> {
        let directory = directory()?;
        let path = directory.path().join("started");
        let signal = Signal::create(&path)?;
        let error = signal
            .wait(Duration::ZERO)
            .expect_err("no byte was written, so the wait cannot succeed");
        assert!(
            matches!(error, E2eError::Timeout { .. }),
            "{}",
            gang_types::error_chain(&error)
        );
        Ok(())
    }

    #[test]
    fn an_unknown_byte_is_an_error() -> crate::Result<()> {
        let directory = directory()?;
        let path = directory.path().join("started");
        let signal = Signal::create(&path)?;
        let mut writer = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(E2eError::io("opening the signal"))?;
        writer
            .write_all(b"?")
            .map_err(E2eError::io("writing the signal"))?;
        let error = signal.wait(WAIT).expect_err("an unknown byte is rejected");
        assert!(
            matches!(error, E2eError::System { context, .. } if context == "unexpected byte on the signal pipe")
        );
        Ok(())
    }
}
