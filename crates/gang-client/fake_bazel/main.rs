//! A stand-in for Bazel in this crate's unit tests.
//!
//! Protocol:
//! - `--fake_events_from=<fixture>` copies the fixture to the path supplied through
//!   `--build_event_json_file=<events>`; together with `--fake_record_events_path=<seen>`, it also
//!   writes the events path text to `<seen>` and exits 0.
//! - `--fake_grandchild_lock=<path>` starts this executable with `--fake_block=<path>`, waits for
//!   it, and exits 0.
//! - `--fake_block=<path>` takes an exclusive lock on `<path>.tmp`, renames it to `<path>` so the
//!   lock is held once `<path>` exists, and blocks until killed.
//! - Anything else writes an error to stderr and exits 2.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Prints `message` to standard error and exits with status 2.
fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

/// Returns the path given as `--<name>=<path>`, if any.
fn flag(arguments: &[OsString], name: &str) -> Option<PathBuf> {
    let prefix = format!("--{name}=");
    arguments
        .iter()
        .find_map(|argument| argument.to_str()?.strip_prefix(&prefix).map(PathBuf::from))
}

/// Starts the fake Bazel binary.
fn main() {
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();

    if let Some(path) = flag(&arguments, "fake_block") {
        let temporary = PathBuf::from(format!("{}.tmp", path.display()));
        let _lock = gang_os::FileLock::try_exclusive(&temporary)
            .unwrap_or_else(|error| fail(format!("taking the grandchild lock: {error}")))
            .unwrap_or_else(|| fail("the grandchild lock is already held"));
        fs::rename(&temporary, &path)
            .unwrap_or_else(|error| fail(format!("publishing the grandchild lock: {error}")));
        loop {
            std::thread::park();
        }
    }

    if let Some(path) = flag(&arguments, "fake_grandchild_lock") {
        let mut argument = OsString::from("--fake_block=");
        argument.push(path);
        let mut child = Command::new(std::env::current_exe().unwrap_or_else(|error| fail(error)))
            .arg(argument)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|error| fail(format!("starting fake child: {error}")));
        let status = child
            .wait()
            .unwrap_or_else(|error| fail(format!("waiting for fake child: {error}")));
        if !status.success() {
            fail("fake child exited unsuccessfully");
        }
        return;
    }

    if let Some(fixture) = flag(&arguments, "fake_events_from") {
        let events = flag(&arguments, "build_event_json_file")
            .unwrap_or_else(|| fail("missing --build_event_json_file argument"));
        let seen = flag(&arguments, "fake_record_events_path")
            .unwrap_or_else(|| fail("missing --fake_record_events_path argument"));
        fs::copy(&fixture, &events)
            .unwrap_or_else(|error| fail(format!("copying events fixture: {error}")));
        let events_text = events
            .to_str()
            .unwrap_or_else(|| fail("events path is not UTF-8"));
        fs::write(seen, events_text)
            .unwrap_or_else(|error| fail(format!("writing seen path: {error}")));
        return;
    }

    fail("unsupported fake Bazel arguments");
}
