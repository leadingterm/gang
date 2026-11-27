//! Errors of the bubblewrap plugin, mapped onto the port error contract.

use std::io;
use std::path::PathBuf;

/// Why the bubblewrap plugin cannot build or run an action.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub(crate) enum BwrapError {
    /// The `bwrap` program is not absolute, so `gang worker` did not resolve it before the sandbox
    /// file was written.
    #[error(
        "bwrap `program` {} is not absolute: gang worker resolves names through its PATH before it writes the sandbox file",
        .0.display()
    )]
    #[failure(config)]
    RelativeProgram(PathBuf),
    /// Public egress was requested without configured helpers.
    #[error("public egress helpers are not configured")]
    #[failure(backend)]
    NetworkNotConfigured,
    /// Public egress setup failed.
    #[error("cannot set up public egress")]
    #[failure(backend)]
    NetworkSetup(#[source] io::Error),
    /// Public egress setup did not finish before its deadline.
    #[error("public egress setup exceeded {0}")]
    #[failure(backend)]
    NetworkTimeout(gang_types::NonZeroDuration),
    /// The running gang executable could not be located.
    #[error("cannot locate the gang executable")]
    #[failure(backend)]
    Executable(#[source] io::Error),
    /// The spawning thread's signal mask could not be changed.
    #[error("cannot change the signal mask for bwrap")]
    #[failure(backend)]
    SignalMask(#[source] io::Error),
    /// The `bwrap` binary could not be spawned.
    #[error("cannot run bwrap")]
    #[failure(backend)]
    Run(#[source] io::Error),
    /// The started `bwrap` process could not be waited on.
    #[error("cannot wait for bwrap")]
    #[failure(backend)]
    Wait(#[source] io::Error),
    /// The pipe `bwrap` reports the sandbox on could not be created or prepared.
    #[error("cannot create the pipe bwrap reports the sandbox on")]
    #[failure(backend)]
    InfoPipe(#[source] io::Error),
    /// The rootfs envelope or tree cannot be read.
    #[error("cannot read action rootfs {}", .0.display())]
    #[failure(config)]
    Root(PathBuf, #[source] io::Error),
    /// The rootfs identity is absent, malformed, or not a rootfs digest.
    #[error("invalid action rootfs identity in {}", .0.display())]
    #[failure(config)]
    RootIdentity(PathBuf),
    /// An entry cannot be represented safely in the sandbox.
    #[error("invalid or reserved action rootfs entry {}", .0.display())]
    #[failure(config)]
    RootEntry(PathBuf),
    /// The action directory would obscure a protected mount.
    #[error("action directory {} overlaps a protected sandbox path", .0.display())]
    #[failure(config)]
    ActionDirectory(PathBuf),
    /// Too many read-only input trees were requested for a single action.
    #[error("too many read-only input trees ({count}); at most {max} are supported")]
    #[failure(config)]
    TooManyReadOnlyTrees {
        /// Number of read-only input trees supplied.
        count: usize,
        /// Maximum permitted count.
        max: usize,
    },
    /// A read-only input tree entry has an invalid source or target path.
    #[error("invalid read-only input tree {} -> {}", host_path.display(), target.display())]
    #[failure(config)]
    InvalidReadOnlyTree {
        /// Host path to the read-only tree.
        host_path: PathBuf,
        /// Target path inside the action directory.
        target: PathBuf,
    },
    /// Too many action-local scratch mounts were requested.
    #[error("too many ephemeral directories ({count}); at most {max} are supported")]
    #[failure(config)]
    TooManyEphemeralDirs {
        /// Number of scratch directories supplied.
        count: usize,
        /// Maximum permitted count.
        max: usize,
    },
    /// A scratch mount escapes the action or obscures another mount or the working directory.
    #[error("invalid ephemeral directory {}", path.display())]
    #[failure(config)]
    InvalidEphemeralDir {
        /// Rejected scratch directory.
        path: PathBuf,
    },
    /// The sandbox report could not be read from the pipe.
    #[error("cannot read the sandbox bwrap reported")]
    #[failure(backend)]
    Info(#[source] io::Error),
    /// The sandbox report is not the JSON object `bwrap` writes.
    #[error("bwrap reported the sandbox in an unexpected format")]
    #[failure(backend)]
    InfoFormat(#[source] serde_json::Error),
    /// A termination could not be delivered to the sandbox.
    #[error("cannot relay a termination into the sandbox")]
    #[failure(backend)]
    Relay(#[source] io::Error),
}
