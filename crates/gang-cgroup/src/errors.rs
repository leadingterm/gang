//! Errors of the cgroup plugin, mapped onto the port error contract.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Why the cgroup plugin cannot build or run an action.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum CgroupError {
    /// A cgroup interface file or directory could not be read, written, created or removed.
    #[error("{}", path.display())]
    Io {
        /// The path that failed.
        path: PathBuf,
        /// The underlying OS error.
        #[source]
        source: io::Error,
    },
    /// The start time of a process could not be read or parsed from procfs, so a scope cannot be
    /// named after it.
    #[error("cannot read the start time of process {pid}")]
    ProcessStart {
        /// The process whose start time is unknown.
        pid: u32,
    },
    /// The worker process runs in the root cgroup, so there is no delegated subtree to use.
    #[error("the process is in the root cgroup, not in a delegated subtree")]
    NoDelegatedSubtree,
    /// The worker leaf cgroup has not been prepared, so actions cannot be placed in it.
    #[error("cgroup {} is not the worker leaf; prepare has not run", cgroup.display())]
    NotPrepared {
        /// The cgroup that was expected to be the worker leaf.
        cgroup: PathBuf,
    },
    /// The cgroup does not enable a controller that the limits need.
    #[error("cgroup {} lacks the {controller} controller", cgroup.display())]
    MissingController {
        /// The cgroup missing the controller.
        cgroup: PathBuf,
        /// The controller name, as the kernel spells it.
        controller: &'static str,
    },
    /// A kernel interface file did not have the expected format.
    #[error("cannot parse {what}: {content:?}")]
    Malformed {
        /// The file that was parsed.
        what: &'static str,
        /// The content that could not be parsed.
        content: String,
    },
    /// The scope still had processes after the removal timeout.
    #[error("cgroup {} still has processes after {timeout:?}", cgroup.display())]
    #[failure(unavailable)]
    StillPopulated {
        /// The scope that could not be removed.
        cgroup: PathBuf,
        /// How long removal was attempted.
        timeout: Duration,
    },
    /// The subtree is not delegated to the worker, so it cannot host scopes.
    #[error("cgroup {} is not delegated to the worker", cgroup.display())]
    NotDelegated {
        /// The cgroup that is not delegated.
        cgroup: PathBuf,
    },
    /// The kernel lacks an interface file that the plugin relies on.
    #[error("the kernel provides no {file}")]
    MissingKernelFeature {
        /// The interface file that is absent.
        file: &'static str,
    },
    /// The command could not join its action scope, so it was not started.
    #[error("cannot join the action scope {}", scope.display())]
    Join {
        /// The scope directory the command could not join.
        scope: PathBuf,
        /// The underlying OS error.
        #[source]
        source: io::Error,
    },
    /// `gang-join` was given no scope directory or no command.
    #[error("gang-join needs a scope directory and a command")]
    JoinUsage,
    /// The command joined its action scope but could not be executed.
    #[error("cannot execute {} in the action scope", program.display())]
    Exec {
        /// The program that could not be executed.
        program: PathBuf,
        /// The underlying OS error.
        #[source]
        source: io::Error,
    },
}

impl CgroupError {
    /// Turns an [`io::Error`] on `path` into a [`CgroupError::Io`], for use with `map_err`.
    pub(crate) fn io(path: &Path) -> impl FnOnce(io::Error) -> Self {
        let path = path.to_path_buf();
        move |source| Self::Io { path, source }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::path::PathBuf;

    use super::CgroupError;

    #[test]
    fn io_message_leaves_the_cause_to_the_source_chain() {
        let error = CgroupError::Io {
            path: PathBuf::from("/cgroup/events"),
            source: io::Error::other("underlying failure"),
        };
        assert_eq!(error.to_string(), "/cgroup/events");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("underlying failure")
        );
    }
}
