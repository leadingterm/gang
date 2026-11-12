//! Typed errors for the `tofu` contract runner.

use std::io;
use std::path::PathBuf;
use std::process::ExitStatus;

use crate::contract::Step;

/// Why a `tofu` contract run failed.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The root directory is missing from the staged sources.
    #[error("root not found: {}", root.display())]
    MissingRoot {
        /// The root, relative to the sources directory.
        root: PathBuf,
    },
    /// The test file is missing from the root.
    #[error("test file not found: {}", path.display())]
    MissingTestFile {
        /// The test file, relative to the root.
        path: PathBuf,
    },
    /// A file or directory could not be copied into the work directory.
    #[error("cannot stage {}", path.display())]
    Stage {
        /// The copy failure.
        #[source]
        source: io::Error,
        /// The source path that could not be copied.
        path: PathBuf,
    },
    /// The `tofu` binary could not be started.
    #[error("cannot run opentofu {step}")]
    Spawn {
        /// The step that failed to start.
        step: Step,
        /// The spawn failure.
        #[source]
        source: io::Error,
    },
    /// The `tofu` step exited unsuccessfully.
    #[error("opentofu {step} failed with {status}")]
    Failed {
        /// The step that failed.
        step: Step,
        /// The exit status of the step.
        status: ExitStatus,
    },
    /// A checked step wrote to stderr, which the step does not allow.
    #[error("opentofu {step} wrote to stderr")]
    Stderr {
        /// The checked step that wrote to stderr.
        step: Step,
    },
    /// A required environment variable is not set.
    #[error("missing environment variable {name}")]
    Environment {
        /// The name of the missing variable.
        name: &'static str,
    },
}
