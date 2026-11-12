//! Error types shared by the filesystem, lock, and process modules.

use std::io;
use std::path::{Path, PathBuf};

/// An I/O failure on a path: what was attempted, where, and why.
///
/// This is the single `Io { action, path, source }` shape used across the workspace.
#[derive(Debug, thiserror::Error)]
#[error("cannot {action} {}", .path.display())]
pub struct PathIo {
    /// The operation that failed, such as `"read"` or `"open log file"`.
    pub action: &'static str,
    /// The path the operation was applied to.
    pub path: PathBuf,
    /// The underlying I/O error.
    #[source]
    pub source: io::Error,
}

impl PathIo {
    /// Builds a [`PathIo`] for `action` on `path`, copying `path` into the error.
    #[must_use]
    pub fn new(action: &'static str, path: &Path, source: io::Error) -> Self {
        Self {
            action,
            path: path.to_path_buf(),
            source,
        }
    }

    /// Returns a `map_err` adapter that builds a [`PathIo`] for `action` on `path`.
    ///
    /// Use as `std::fs::read(p).map_err(PathIo::at("read file", p))`. The path is copied only
    /// when the adapter is called with an error.
    pub fn at(action: &'static str, path: &Path) -> impl FnOnce(io::Error) -> Self {
        move |source| Self::new(action, path, source)
    }

    /// Returns the kind of the underlying I/O error.
    #[must_use]
    pub fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }

    /// Returns `true` when the underlying I/O error is [`io::ErrorKind::NotFound`].
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.kind() == io::ErrorKind::NotFound
    }
}

/// Why `spawn_logged` failed.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// The log file could not be opened or duplicated.
    #[error(transparent)]
    Log(#[from] PathIo),
    /// The program could not be started.
    #[error("cannot spawn {}", .program.display())]
    Spawn {
        /// The program that was asked to start.
        program: PathBuf,
        /// The underlying I/O error from the failed spawn.
        #[source]
        source: io::Error,
    },
}

/// Why running a child process failed.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The program could not be started.
    #[error("cannot start {}: {source}", .program.display())]
    Spawn {
        /// The program that was asked to start.
        program: PathBuf,
        /// The underlying spawn error.
        #[source]
        source: io::Error,
    },
    /// Writing, reading or waiting on the child failed.
    #[error("running {}: {source}", .program.display())]
    Io {
        /// The program being run.
        program: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// The child wrote more than the limit to standard output.
    #[error("{} wrote more than {limit} to standard output", .program.display())]
    StdoutTooLarge {
        /// The program being run.
        program: PathBuf,
        /// The maximum permitted standard output size.
        limit: gang_types::NonZeroBytes,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::{self, ErrorKind};
    use std::path::Path;

    use gang_testkit::TestResult;

    use super::PathIo;

    fn gone() -> io::Error {
        io::Error::new(ErrorKind::NotFound, "gone")
    }

    #[test]
    fn path_io_displays_action_and_path() {
        let err = PathIo::new("read", Path::new("/x/y"), gone());

        assert_eq!(err.to_string(), "cannot read /x/y");
    }

    #[test]
    fn path_io_source_is_the_io_error() -> TestResult {
        let err = PathIo::new("read", Path::new("/x/y"), gone());

        let source = err.source().ok_or("missing source")?;

        assert_eq!(source.to_string(), "gone");
        Ok(())
    }

    #[test]
    fn path_io_reports_kind_and_not_found() {
        let missing = PathIo::new("read", Path::new("/x/y"), gone());
        let denied = PathIo::new(
            "read",
            Path::new("/x/y"),
            io::Error::from(ErrorKind::PermissionDenied),
        );

        assert_eq!(missing.kind(), ErrorKind::NotFound);
        assert!(missing.is_not_found());
        assert_eq!(denied.kind(), ErrorKind::PermissionDenied);
        assert!(!denied.is_not_found());
    }
}
