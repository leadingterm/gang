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
    /// A JSON-mode test emitted a warning or error diagnostic.
    #[error("opentofu test emitted a {severity} diagnostic")]
    Diagnostic {
        /// Diagnostic severity.
        severity: String,
    },
    /// The explicit test expectations file could not be read.
    #[error("cannot read test expectations {}", path.display())]
    ReadExpectations {
        /// The requested file.
        path: PathBuf,
        /// The read failure.
        #[source]
        source: io::Error,
    },
    /// The expectations JSON has the wrong format.
    #[error("invalid test expectations JSON")]
    ParseExpectations {
        /// The parse failure.
        #[source]
        source: serde_json::Error,
    },
    /// The expectations must identify at least one run and resource.
    #[error("empty plan expectations for {run}")]
    EmptyExpectations {
        /// The run with no resources, or a marker for the entire file.
        run: String,
    },
    /// The machine-readable test output is malformed.
    #[error("invalid OpenTofu test JSON at line {line}: {reason}")]
    InvalidPlanOutput {
        /// One-based output line number.
        line: usize,
        /// Description without plan values.
        reason: &'static str,
    },
    /// A required run or resource did not appear exactly once.
    #[error("expected exactly one plan for run {run}, address {address}; found {count}")]
    PlanCount {
        /// The requested test run.
        run: String,
        /// The requested resource, or a marker for the run itself.
        address: String,
        /// Number found.
        count: usize,
    },
    /// A resource's actions differed from the contract.
    #[error(
        "unexpected actions for run {run}, address {address}: expected {expected:?}, got {actual:?}"
    )]
    PlanActions {
        /// The test run.
        run: String,
        /// The resource address.
        address: String,
        /// Expected action sequence.
        expected: Vec<String>,
        /// Observed action sequence.
        actual: Vec<String>,
    },
    /// An SSM parameter deletion would violate the immutable configuration contract.
    #[error(
        "forbidden SSM parameter deletion in run {run}, address {address}, actions {actions:?}"
    )]
    ParameterDelete {
        /// The test run.
        run: String,
        /// The resource address.
        address: String,
        /// Observed action sequence.
        actions: Vec<String>,
    },
    /// An expectation or machine-readable test result violated the contract.
    #[error("invalid test evidence: {reason}")]
    Evidence {
        /// A value-free description of the violated evidence requirement.
        reason: String,
    },
    /// A required environment variable is not set.
    #[error("missing environment variable {name}")]
    Environment {
        /// The name of the missing variable.
        name: &'static str,
    },
}
