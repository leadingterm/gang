//! Shared runfile and child-process helpers for integration tests.

use std::error::Error;
use std::path::PathBuf;
use std::process::{Child, Output};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process};

/// Resolves a runfile path supplied by the Bazel test environment.
///
/// # Errors
///
/// Returns an error if the variable is missing or the current directory cannot be read.
pub fn required_path(var: &str) -> Result<PathBuf, Box<dyn Error>> {
    let raw =
        std::env::var_os(var).ok_or_else(|| format!("{var} must be set by the Bazel test_env"))?;
    Ok(std::env::current_dir()?.join(raw))
}

/// Waits for a child and collects its output without polling.
///
/// On timeout, the waiter thread reaps the child after it exits. If
/// `kill_on_timeout` is false, the caller must clean up the child.
///
/// # Errors
///
/// Returns an error if the wait fails, times out, or the waiter thread stops early.
pub fn wait_within(
    child: Child,
    limit: Duration,
    kill_on_timeout: bool,
) -> Result<Output, Box<dyn Error>> {
    let pid = child.id();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        // The receiver may have gone away after a timeout.
        let _ = tx.send(child.wait_with_output());
    });
    match rx.recv_timeout(limit) {
        Ok(output) => Ok(output?),
        Err(RecvTimeoutError::Timeout) => {
            if kill_on_timeout
                && let Ok(raw) = i32::try_from(pid)
                && let Some(pid) = Pid::from_raw(raw)
            {
                // The child may already have exited; report the timeout either way.
                let _ = kill_process(pid, Signal::KILL);
            }
            Err(format!("process {pid} timed out after {limit:?}").into())
        }
        Err(RecvTimeoutError::Disconnected) => {
            Err("the waiting thread ended without an exit status".into())
        }
    }
}
