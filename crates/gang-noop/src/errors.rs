//! Errors of running an action without isolation.

use std::io;

/// Why an action could not be run.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub(crate) enum NoopError {
    /// The per-action TMPDIR could not be created.
    #[error("cannot create the temporary directory")]
    TemporaryDirectory(#[source] io::Error),
    /// The command could not be started.
    #[error("cannot start the command")]
    Spawn(#[source] io::Error),
    /// Waiting for the command failed.
    #[error("cannot wait for the command")]
    Wait(#[source] io::Error),
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use super::NoopError;

    #[test]
    fn messages_leave_the_cause_to_the_source_chain() {
        for error in [
            NoopError::TemporaryDirectory(io::Error::other("underlying failure")),
            NoopError::Spawn(io::Error::other("underlying failure")),
            NoopError::Wait(io::Error::other("underlying failure")),
        ] {
            assert!(!error.to_string().contains("underlying failure"));
            assert_eq!(
                error.source().map(ToString::to_string).as_deref(),
                Some("underlying failure")
            );
        }
    }
}
