//! The `opentofu-runner` binary: runs one `tofu` contract described by the environment.
//!
//! Bazel starts the binary in the runfiles workspace. It reads `OPENTOFU_TOFU` and
//! `OPENTOFU_PROVIDER_MIRROR` (made absolute against the current directory), and
//! `OPENTOFU_ROOT` (relative to the sources directory) and `OPENTOFU_TEST_FILE` (relative to the
//! root). The current directory holds the `infra/` tree. `OPENTOFU_TEST_EXPECTATIONS`, when set,
//! names a JSON runfile resolved against that directory. The work directory is a fresh directory
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
    let args: Vec<_> = env::args_os().skip(1).collect();
    if !args.is_empty() {
        if args.len() != 11
            || args[0] != "--upgrade-fixture"
            || args[1] != "--source-root"
            || args[3] != "--test-file"
            || args[5] != "--tofu"
            || args[7] != "--provider-mirror"
            || args[9] != "--output"
        {
            return Err("expected --upgrade-fixture --source-root ROOT --test-file FILE --tofu TF --provider-mirror MIRROR --output OUT".into());
        }
        return opentofu_runner::upgrade_fixture(
            args[2].as_os_str().as_ref(),
            args[4].as_os_str().as_ref(),
            args[6].as_os_str().as_ref(),
            args[8].as_os_str().as_ref(),
            args[10].as_os_str().as_ref(),
        );
    }
    let cwd = env::current_dir()?;
    let contract = Contract {
        tofu: cwd.join(required("OPENTOFU_TOFU")?),
        provider_mirror: cwd.join(required("OPENTOFU_PROVIDER_MIRROR")?),
        sources: cwd.clone(),
        root: required("OPENTOFU_ROOT")?.into(),
        test_file: required("OPENTOFU_TEST_FILE")?.into(),
        test_expectations: env::var_os("OPENTOFU_TEST_EXPECTATIONS").map(|path| cwd.join(path)),
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
