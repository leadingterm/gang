//! The journal the execution probes write to: a named pipe hard-linked into each action.
//! Lines are shorter than `PIPE_BUF`, so each arrives whole.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::{
    errors::{E2eError, Result},
    fifo,
};

#[derive(Debug)]
pub(crate) struct Attempt {
    pub(crate) worker: u32,
    pub(crate) start: u64,
    pub(crate) end: Option<u64>,
    ended_before: Option<u64>,
    contended: bool,
}

pub(crate) type Attempts = BTreeMap<(String, u32), Attempt>;

pub(crate) fn record_attempt(line: &str, run: &str, attempts: &mut Attempts) -> Result<()> {
    let mut words = line.split_whitespace();
    if words.next() != Some(run) || words.next() != Some("loss") {
        return Err(E2eError::system(format!("unexpected journal line: {line}")));
    }
    let kind = words
        .next()
        .ok_or_else(|| E2eError::system("journal event lacks kind"))?;
    let action = words
        .next()
        .ok_or_else(|| E2eError::system("journal event lacks action"))?;
    let worker = words
        .next()
        .ok_or_else(|| E2eError::system("journal event lacks worker"))?
        .parse::<u32>()
        .map_err(E2eError::system_from("parsing worker pid"))?;
    let pid = words
        .next()
        .ok_or_else(|| E2eError::system("journal event lacks attempt"))?
        .parse::<u32>()
        .map_err(E2eError::system_from("parsing attempt pid"))?;
    let time = words
        .next()
        .ok_or_else(|| E2eError::system("journal event lacks timestamp"))?
        .parse::<u64>()
        .map_err(E2eError::system_from("parsing attempt timestamp"))?;
    if worker == 0 || pid == 0 {
        return Err(E2eError::system("invalid journal pid"));
    }
    let contended = match kind {
        "start" => match words.next() {
            Some("exclusive") => false,
            Some("contended") => true,
            _ => return Err(E2eError::system("invalid attempt witness")),
        },
        "end" => false,
        _ => return Err(E2eError::system("unknown journal event")),
    };
    if words.next().is_some() {
        return Err(E2eError::system("extra journal fields"));
    }
    let key = (action.to_owned(), pid);
    if kind == "start" {
        if attempts.contains_key(&key) {
            return Err(E2eError::system("duplicate attempt start"));
        }
        if !contended {
            for ((previous_action, _), previous) in attempts.iter_mut() {
                if previous_action == action
                    && previous.end.is_none()
                    && previous.ended_before.is_none()
                    && previous.start <= time
                {
                    previous.ended_before = Some(time);
                }
            }
        }
        attempts.insert(
            key,
            Attempt {
                worker,
                start: time,
                end: None,
                ended_before: None,
                contended,
            },
        );
    } else {
        let attempt = attempts
            .get_mut(&key)
            .ok_or_else(|| E2eError::system("attempt ended without starting"))?;
        if attempt.worker != worker || time < attempt.start || attempt.end.is_some() {
            return Err(E2eError::system("invalid attempt end"));
        }
        attempt.end = Some(time);
    }
    Ok(())
}

pub(crate) fn overlapping_attempts(attempts: &Attempts) -> bool {
    attempts.values().any(|attempt| attempt.contended)
        || attempts.iter().enumerate().any(|(i, ((action, _), a))| {
            attempts.iter().skip(i + 1).any(|((other_action, _), b)| {
                action == other_action
                    && a.start < b.end.or(b.ended_before).unwrap_or(u64::MAX)
                    && b.start < a.end.or(a.ended_before).unwrap_or(u64::MAX)
            })
        })
}

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

    /// Precreates action witnesses without replacing an existing lock file.
    pub(crate) fn prepare_attempts<'a>(
        &self,
        run: &str,
        actions: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        for action in actions {
            let path = self.path.with_file_name(format!("{run}-{action}.lock"));
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .map_err(E2eError::io("creating attempt witness"))?;
        }
        Ok(())
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

    use super::{Attempts, Journal, overlapping_attempts, record_attempt};
    use crate::errors::E2eError;

    fn records(lines: &[&str]) -> Attempts {
        let mut attempts = Attempts::new();
        for line in lines {
            record_attempt(line, "run", &mut attempts).expect("valid journal event");
        }
        attempts
    }

    #[test]
    fn contended_retry_remains_overlap_when_read_later() {
        let attempts = records(&[
            "run loss start action 1 11 10 exclusive",
            "run loss start action 2 12 20 contended",
            "run loss end action 1 11 20",
            "run loss end action 2 12 40",
        ]);
        assert!(overlapping_attempts(&attempts));
    }

    #[test]
    fn exclusive_retry_bounds_missing_end() {
        let attempts = records(&[
            "run loss start action 1 11 10 exclusive",
            "run loss start action 2 12 20 exclusive",
            "run loss end action 2 12 30",
        ]);
        assert!(!overlapping_attempts(&attempts));
    }

    #[test]
    fn recorded_intervals_overlap_even_with_exclusive_witnesses() {
        let attempts = records(&[
            "run loss start action 1 11 10 exclusive",
            "run loss end action 1 11 30",
            "run loss start action 2 12 20 exclusive",
            "run loss end action 2 12 40",
        ]);
        assert!(overlapping_attempts(&attempts));
    }

    #[test]
    fn independent_actions_and_touching_intervals_do_not_overlap() {
        let attempts = records(&[
            "run loss start action 1 11 10 exclusive",
            "run loss end action 1 11 20",
            "run loss start action 2 12 20 exclusive",
            "run loss start other 3 13 15 exclusive",
        ]);
        assert!(!overlapping_attempts(&attempts));
    }

    #[test]
    fn rejects_invalid_journal_lines() {
        for line in [
            "other loss start action 1 11 10 exclusive",
            "run wrong start action 1 11 10 exclusive",
            "run loss start action 1 11 10",
            "run loss start action 1 11 10 unknown",
            "run loss start action 1 11 10 exclusive extra",
            "run loss end action 1 11 10 exclusive",
            "run loss start action 0 11 10 exclusive",
            "run loss start action 1 0 10 exclusive",
            "run loss start action x 11 10 exclusive",
            "run loss start action 1 11 bad exclusive",
            "run loss end action 1 11 10",
        ] {
            assert!(
                record_attempt(line, "run", &mut Attempts::new()).is_err(),
                "{line}"
            );
        }
    }

    #[test]
    fn rejects_invalid_transitions_without_mutating_attempts() {
        let mut attempts = records(&["run loss start action 1 11 10 exclusive"]);
        for line in [
            "run loss start action 1 11 20 exclusive",
            "run loss end action 2 11 20",
            "run loss end action 1 11 9",
        ] {
            assert!(
                record_attempt(line, "run", &mut attempts).is_err(),
                "{line}"
            );
        }
        assert_eq!(
            attempts.values().next().expect("attempt").ended_before,
            None
        );
        record_attempt("run loss end action 1 11 20", "run", &mut attempts).expect("end");
        assert!(record_attempt("run loss end action 1 11 30", "run", &mut attempts).is_err());
        assert_eq!(attempts.values().next().expect("attempt").end, Some(20));
    }

    #[test]
    fn witnesses_are_precreated_without_truncating_existing_files() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let journal =
            Journal::create(&directory.path().join("journal"), Duration::ZERO).expect("journal");
        journal
            .prepare_attempts("run", ["action"])
            .expect("prepare");
        let witness = directory.path().join("run-action.lock");
        std::fs::write(&witness, b"preserve").expect("write witness");
        assert!(journal.prepare_attempts("run", ["action"]).is_err());
        assert_eq!(std::fs::read(witness).expect("read witness"), b"preserve");
    }

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
