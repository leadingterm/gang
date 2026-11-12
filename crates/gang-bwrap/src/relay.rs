//! Relays terminations from `gang exec` into a running sandbox.
//!
//! `bwrap` shares the action's original group and blocks termination signals. Bubblewrap calls
//! `setsid()` in the sandbox's first process before it forks the action, so the action and its
//! descendants share that process's group. Before the action exists, a relayed termination kills
//! the launcher instead: a blocked termination would not stop it, but `--die-with-parent` does.

use std::io;
use std::path::Path;

use gang_spi::Termination;
use rustix::process::{Pid, Signal, kill_process, kill_process_group};

use crate::errors::BwrapError;

/// The procfs directory holding one directory per process.
const PROC_ROOT: &str = "/proc";

/// The processes a termination is relayed to: `bwrap` itself and the sandbox's first process,
/// whose process group holds the action and its descendants.
pub(crate) struct Relay {
    /// The `bwrap` process `gang exec` spawned.
    launcher: Pid,
    /// The sandbox's first process, which bubblewrap made a session and process group leader.
    sandbox: Pid,
}

impl Relay {
    /// Relays to `launcher`, the spawned `bwrap`, and to `sandbox`, the sandbox's first process.
    #[must_use]
    pub(crate) fn new(launcher: Pid, sandbox: Pid) -> Self {
        Self { launcher, sandbox }
    }

    /// Sends `termination` to the action's process group once it exists, and kills `bwrap`
    /// before that. A target that is already gone is not an error.
    ///
    /// # Errors
    ///
    /// [`BwrapError::Relay`] when the sandbox state cannot be read or the signal cannot be sent.
    pub(crate) fn relay(&self, termination: Termination) -> Result<(), BwrapError> {
        let signal = match termination {
            Termination::Terminate => Signal::TERM,
            Termination::Interrupt => Signal::INT,
        };
        if action_started(self.sandbox).map_err(BwrapError::Relay)? {
            ignore_gone(kill_process_group(self.sandbox, signal))
        } else {
            ignore_gone(kill_process(self.launcher, Signal::KILL))
        }
    }
}

/// Whether the sandbox's first process has forked the action: its `task/<pid>/children` lists a
/// pid. A first process that is gone has no child.
fn action_started(sandbox: Pid) -> io::Result<bool> {
    let process = sandbox.as_raw_pid().to_string();
    let children = Path::new(PROC_ROOT)
        .join(&process)
        .join("task")
        .join(&process)
        .join("children");
    match std::fs::read_to_string(children) {
        Ok(listed) => Ok(!listed.trim().is_empty()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Treats a target that is already gone as delivered.
fn ignore_gone(result: rustix::io::Result<()>) -> Result<(), BwrapError> {
    match result {
        Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
        Err(errno) => Err(BwrapError::Relay(io::Error::from(errno))),
    }
}
