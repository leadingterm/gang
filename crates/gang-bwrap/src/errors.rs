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
    /// The running gang executable could not be located.
    #[error("cannot locate the gang executable")]
    Executable(#[source] io::Error),
    /// The spawning thread's signal mask could not be changed.
    #[error("cannot change the signal mask for bwrap")]
    SignalMask(#[source] io::Error),
    /// The `bwrap` binary could not be spawned.
    #[error("cannot run bwrap")]
    Run(#[source] io::Error),
    /// The started `bwrap` process could not be waited on.
    #[error("cannot wait for bwrap")]
    Wait(#[source] io::Error),
    /// The pipe `bwrap` reports the sandbox on could not be created or prepared.
    #[error("cannot create the pipe bwrap reports the sandbox on")]
    InfoPipe(#[source] io::Error),
    /// The sandbox report could not be read from the pipe.
    #[error("cannot read the sandbox bwrap reported")]
    Info(#[source] io::Error),
    /// The sandbox report is not the JSON object `bwrap` writes.
    #[error("bwrap reported the sandbox in an unexpected format")]
    InfoFormat(#[source] serde_json::Error),
    /// A termination could not be delivered to the sandbox.
    #[error("cannot relay a termination into the sandbox")]
    Relay(#[source] io::Error),
}
