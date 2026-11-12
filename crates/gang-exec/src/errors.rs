//! Typed errors for `gang exec`.

use std::ffi::{CStr, OsStr};
use std::io;
use std::num::ParseIntError;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use gang_spi::PortError;
use gang_types::UnknownVariantError;

/// Why `gang exec` could not run its action.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// No command was given after the multicall name.
    #[error("no command to run")]
    NoCommand,
    /// The entrypoint is not an absolute path. A relative path would resolve against the
    /// working directory, which the action controls.
    #[error("entrypoint {} is not an absolute path", entrypoint.display())]
    EntrypointNotAbsolute {
        /// The entrypoint as it was given.
        entrypoint: PathBuf,
    },
    /// The sandbox file beside the entrypoint cannot be read.
    #[error("cannot read sandbox file {}", path.display())]
    ReadSandboxFile {
        /// The path that was read.
        path: PathBuf,
        /// The I/O failure.
        #[source]
        source: io::Error,
    },
    /// The sandbox file was read but is not a valid configuration.
    #[error("invalid sandbox file {}: {reason}", path.display())]
    SandboxFile {
        /// The path that was parsed.
        path: PathBuf,
        /// Why parsing failed.
        reason: String,
    },
    /// An action property in the environment is malformed.
    #[error("invalid {name}")]
    InvalidProperty {
        /// The environment variable name.
        name: &'static str,
        /// Why the value is invalid.
        #[source]
        source: PropertyError,
    },
    /// The current working directory cannot be determined.
    #[error("cannot determine the working directory")]
    WorkingDirectory(#[source] io::Error),
    /// The work directory cannot be resolved.
    #[error("cannot resolve the work directory {}", path.display())]
    WorkDirectory {
        /// The work directory as the sandbox file records it.
        path: PathBuf,
        /// The I/O failure.
        #[source]
        source: io::Error,
    },
    /// The working directory is not strictly inside the work directory.
    #[error(
        "working directory {} is not inside the work directory {}",
        working_directory.display(),
        work_directory.display()
    )]
    OutsideWorkDirectory {
        /// The canonical working directory.
        working_directory: PathBuf,
        /// The canonical work directory.
        work_directory: PathBuf,
    },
    /// The async runtime could not be started.
    #[error("cannot start the runtime")]
    Runtime(#[source] io::Error),
    /// The termination signal handlers cannot be installed.
    #[error("cannot listen for termination signals")]
    Signals(#[source] io::Error),
    /// The executor could not leave the action's process group after starting it.
    #[error("cannot leave the action's process group")]
    LeaveActionGroup(#[source] io::Error),
    /// The scope guardian could not be started.
    #[error("cannot start the scope guardian")]
    Guard(#[source] io::Error),
    /// The scope guardian could not observe the exit of the `gang exec` it watches.
    #[error("cannot watch the exit of the action entrypoint")]
    Watch(#[source] io::Error),
    /// The scope guardian could not remove abandoned action scopes.
    #[error("cannot remove the scopes of exited actions")]
    Reap(#[source] PortError),
    /// Breaking hard links under the action directory failed.
    #[error(transparent)]
    LinkBreak(#[from] LinkBreakError),
    /// The sandbox could not be built or could not run the action.
    #[error("cannot run the action in the sandbox")]
    Sandbox(#[source] PortError),
    /// The limits plugin failed to enter or leave the action scope.
    #[error("cannot apply the action limits")]
    Limits(#[source] PortError),
}

/// Why an action property in the environment is malformed.
#[derive(Debug, thiserror::Error)]
pub enum PropertyError {
    /// The value is not valid unicode.
    #[error("is not valid unicode")]
    NotUnicode,
    /// The network request is not one the sandbox understands.
    #[error(transparent)]
    Network(UnknownVariantError),
    /// The value is not a non-negative integer.
    #[error(transparent)]
    Number(ParseIntError),
    /// The value does not fit the target type.
    #[error("is too large")]
    TooLarge,
}

/// A hard link could not be broken.
#[derive(Debug, thiserror::Error)]
#[error("cannot break the hard link {}", path.display())]
pub struct LinkBreakError {
    /// The path of the file whose link could not be broken.
    pub path: PathBuf,
    /// The I/O failure.
    #[source]
    pub source: io::Error,
}

impl LinkBreakError {
    /// Builds an error for the entry `name`, relative to the directory being walked.
    pub(crate) fn at(name: &CStr, source: impl Into<io::Error>) -> Self {
        Self {
            path: PathBuf::from(OsStr::from_bytes(name.to_bytes())),
            source: source.into(),
        }
    }

    /// Prefixes the reported path with the directory `parent` it was found in.
    pub(crate) fn within(self, parent: &CStr) -> Self {
        let path = Path::new(OsStr::from_bytes(parent.to_bytes())).join(&self.path);
        Self {
            path,
            source: self.source,
        }
    }
}
