//! The `opentofu-runner` binary: runs one `tofu` contract described by the environment.
//!
//! Bazel starts the binary in the runfiles workspace. It reads `OPENTOFU_TOFU` and
//! `OPENTOFU_PROVIDER_MIRROR` (made absolute against the current directory), and
//! `OPENTOFU_ROOT` (relative to the sources directory) and `OPENTOFU_TEST_FILE` (relative to the
//! root). The current directory holds the `infra/` tree. The work directory is a fresh directory
//! under `TEST_TMPDIR`, removed when the run ends.

use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::process::ExitCode;

use opentofu_runner::{Contract, RunError};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report(&*error);
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let cwd = env::current_dir()?;
    let contract = Contract {
        tofu: cwd.join(required("OPENTOFU_TOFU")?),
        provider_mirror: cwd.join(required("OPENTOFU_PROVIDER_MIRROR")?),
        sources: cwd.clone(),
        root: required("OPENTOFU_ROOT")?.into(),
        test_file: required("OPENTOFU_TEST_FILE")?.into(),
    };

    let work = tempfile::TempDir::new_in(required("TEST_TMPDIR")?)?;
    let result = contract.run(work.path());
    let cleanup = work.close();
    result?;
    cleanup?;
    Ok(())
}

/// Returns the value of the environment variable `name`.
fn required(name: &'static str) -> Result<OsString, RunError> {
    env::var_os(name).ok_or(RunError::Environment { name })
}

/// Prints `error` and its chain of causes to stderr.
fn report(error: &dyn Error) {
    eprint!("opentofu-runner: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprint!(": {cause}");
        source = cause.source();
    }
    eprintln!();
}
