//! The journal the execution probes write to: a named pipe beside the hold signal, which a bwrap action can
//! write through its read-only bind. Lines are shorter than `PIPE_BUF`, so each arrives whole.

use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::{
    errors::{E2eError, Result},
    fifo,
};

pub(crate) struct Journal {
    reader: Mutex<BufReader<File>>,
    writer: Mutex<File>,
    path: PathBuf,
    timeout: Duration,
}

impl Journal {
    /// Creates the named pipe and bounds each subsequent line read by `timeout`.
    pub(crate) fn create(path: &Path, timeout: Duration) -> Result<Self> {
        let file = fifo::create_private(path, "the journal")?;
        let writer = file
            .try_clone()
            .map_err(E2eError::io("duplicating the journal"))?;
        Ok(Self {
            reader: Mutex::new(BufReader::new(file)),
            writer: Mutex::new(writer),
            path: path.to_path_buf(),
            timeout,
        })
    }

    /// The path of the pipe.
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the next line without its trailing newline, within the configured timeout.
    pub(crate) fn read_line(&self) -> Result<String> {
        let mut reader = self.reader.lock();
        let start = Instant::now();
        let mut line = String::new();
        loop {
            if !reader.buffer().contains(&b'\n') {
                fifo::await_readable(
                    reader.get_ref(),
                    self.timeout.saturating_sub(start.elapsed()),
                    "the journal to provide a line",
                )
                .map_err(|error| match error {
                    E2eError::Timeout { condition, .. } => {
                        E2eError::timeout(condition, self.timeout)
                    }
                    other => other,
                })?;
            }
            match reader.read_line(&mut line) {
                Ok(0) => return Err(E2eError::system("the journal closed")),
                Ok(_) => {
                    if line.ends_with('\n') {
                        line.pop();
                    }
                    return Ok(line);
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error).map_err(E2eError::io("reading the journal")),
            }
        }
    }

    /// Writes `line` and a newline in one write.
    pub(crate) fn write_line(&self, line: &str) -> Result<()> {
        let mut writer = self.writer.lock();
        writer
            .write_all(format!("{line}\n").as_bytes())
            .map_err(E2eError::io("writing the journal"))
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::Journal;
    use crate::errors::E2eError;

    #[test]
    fn read_line_without_a_line_times_out() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal =
            Journal::create(&directory.path().join("journal"), Duration::ZERO).expect("journal");
        assert!(matches!(journal.read_line(), Err(E2eError::Timeout { .. })));
    }

    #[test]
    fn written_line_is_read_without_newline() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal =
            Journal::create(&directory.path().join("journal"), Duration::ZERO).expect("journal");
        journal.write_line("first").expect("write");
        assert_eq!(journal.read_line().expect("read"), "first");
    }

    #[test]
    fn lines_are_read_in_order() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal =
            Journal::create(&directory.path().join("journal"), Duration::ZERO).expect("journal");
        journal.write_line("first").expect("write first");
        journal.write_line("second").expect("write second");
        assert_eq!(journal.read_line().expect("read first"), "first");
        assert_eq!(journal.read_line().expect("read second"), "second");
    }
}
