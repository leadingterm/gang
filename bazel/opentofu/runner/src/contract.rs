//! One `tofu` contract: stage the sources into a work directory, then run the four steps.

use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::errors::RunError;

/// One `tofu` contract: a root under `sources` and the test file to run in it.
pub struct Contract {
    /// The `tofu` binary. Must be absolute, because the steps run in the staged root.
    pub tofu: PathBuf,
    /// Directory of pre-downloaded providers, passed to `tofu init` as `-plugin-dir`.
    pub provider_mirror: PathBuf,
    /// Directory holding `infra/` and optionally `configs/`.
    pub sources: PathBuf,
    /// Root directory, relative to `sources`.
    pub root: PathBuf,
    /// Test file, relative to the root.
    pub test_file: PathBuf,
}

/// A step of a contract run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// `tofu fmt -check`, which must succeed silently.
    Fmt,
    /// `tofu init`, which runs without a backend and against the provider mirror.
    Init,
    /// `tofu validate`, which must succeed silently.
    Validate,
    /// `tofu test` for the test file.
    Test,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fmt => "fmt",
            Self::Init => "init",
            Self::Validate => "validate",
            Self::Test => "test",
        })
    }
}

impl Contract {
    /// Stages `sources` into `work` and runs the four steps in order.
    ///
    /// `work` must exist and be writable. The staged copy of the root lives at
    /// `work/<root>`, so the steps never write into `sources`.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::Stage`] when the sources cannot be copied, [`RunError::MissingRoot`]
    /// or [`RunError::MissingTestFile`] when the staged tree lacks them, and the step errors
    /// when a step cannot start, exits unsuccessfully or writes to stderr where it must not.
    pub fn run(&self, work: &Path) -> Result<(), RunError> {
        stage(&self.sources, work)?;

        let root = work.join(&self.root);
        if !root.is_dir() {
            return Err(RunError::MissingRoot {
                root: self.root.clone(),
            });
        }
        if !root.join(&self.test_file).is_file() {
            return Err(RunError::MissingTestFile {
                path: self.test_file.clone(),
            });
        }

        let mut fmt = self.command(&root);
        fmt.args(["fmt", "-check", "-recursive", "-no-color"]);
        checked(Step::Fmt, fmt)?;

        let mut init = self.command(&root);
        init.args(["init", "-backend=false", "-input=false", "-no-color"]);
        let mut plugin_dir = OsString::from("-plugin-dir=");
        plugin_dir.push(&self.provider_mirror);
        init.arg(plugin_dir);
        if root.join(".terraform.lock.hcl").is_file() {
            init.arg("-lockfile=readonly");
        }
        unchecked(Step::Init, init)?;

        let mut validate = self.command(&root);
        validate.args(["validate", "-no-color"]);
        checked(Step::Validate, validate)?;

        let mut test = self.command(&root);
        let mut filter = OsString::from("-filter=");
        filter.push(&self.test_file);
        test.args(["test", "-no-color"]).arg(filter);
        unchecked(Step::Test, test)
    }

    /// Returns a `tofu` command in `root` with the automation environment set.
    fn command(&self, root: &Path) -> Command {
        let mut command = Command::new(&self.tofu);
        command
            .current_dir(root)
            .env("TF_IN_AUTOMATION", "1")
            .env("TF_INPUT", "0")
            .env("CHECKPOINT_DISABLE", "1");
        command
    }
}

/// Copies `sources/infra` into `work/infra`, and `sources/configs` beside it when present.
///
/// Symlinks are followed, so the copy holds regular files only.
fn stage(sources: &Path, work: &Path) -> Result<(), RunError> {
    copy_dir(&sources.join("infra"), &work.join("infra"))?;
    let configs = sources.join("configs");
    if configs.is_dir() {
        copy_dir(&configs, &work.join("configs"))?;
    }
    Ok(())
}

/// Recursively copies the directory `from` to the new directory `to`, following symlinks.
fn copy_dir(from: &Path, to: &Path) -> Result<(), RunError> {
    let stage_error = |source: io::Error, path: &Path| RunError::Stage {
        source,
        path: path.to_path_buf(),
    };

    fs::create_dir(to).map_err(|source| stage_error(source, to))?;
    let entries = fs::read_dir(from).map_err(|source| stage_error(source, from))?;
    for entry in entries {
        let entry = entry.map_err(|source| stage_error(source, from))?;
        let source_path = entry.path();
        let target = to.join(entry.file_name());
        // `metadata` follows symlinks: runfiles entries are symlinks into the build outputs.
        let metadata =
            fs::metadata(&source_path).map_err(|source| stage_error(source, &source_path))?;
        if metadata.is_dir() {
            copy_dir(&source_path, &target)?;
        } else {
            fs::copy(&source_path, &target).map_err(|source| stage_error(source, &source_path))?;
        }
    }
    Ok(())
}

/// Runs a step whose exit status is checked; it must also write nothing to stderr.
///
/// The captured stderr goes to this process's stderr when the step fails.
fn checked(step: Step, mut command: Command) -> Result<(), RunError> {
    command.stdout(Stdio::inherit()).stderr(Stdio::piped());
    let output = command
        .output()
        .map_err(|source| RunError::Spawn { step, source })?;
    if !output.status.success() {
        write_stderr(&output.stderr);
        return Err(RunError::Failed {
            step,
            status: output.status,
        });
    }
    if !output.stderr.is_empty() {
        write_stderr(&output.stderr);
        return Err(RunError::Stderr { step });
    }
    Ok(())
}

/// Runs a step whose output passes through and whose exit status is the only verdict.
fn unchecked(step: Step, mut command: Command) -> Result<(), RunError> {
    let status = command
        .status()
        .map_err(|source| RunError::Spawn { step, source })?;
    if status.success() {
        Ok(())
    } else {
        Err(RunError::Failed { step, status })
    }
}

/// Writes captured child stderr to this process's stderr, best effort: the step error that
/// follows is the failure the caller reports.
fn write_stderr(bytes: &[u8]) {
    let _ = io::stderr().write_all(bytes);
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;

    /// Builds a contract for the fixture `root`, using the `OPENTOFU_TOFU` and
    /// `OPENTOFU_PROVIDER_MIRROR` environment variables, which may be relative to the current directory.
    fn contract(root: &str) -> Contract {
        let cwd = env::current_dir().expect("current directory");
        let tofu =
            env::var_os("OPENTOFU_TOFU").expect("OPENTOFU_TOFU is set by the test environment");
        let mirror = env::var_os("OPENTOFU_PROVIDER_MIRROR")
            .expect("OPENTOFU_PROVIDER_MIRROR is set by the test environment");
        Contract {
            tofu: cwd.join(tofu),
            provider_mirror: cwd.join(mirror),
            sources: cwd.join(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"),
            root: PathBuf::from(root),
            test_file: PathBuf::from("tests/greeting.tftest.hcl"),
        }
    }

    fn run(root: &str) -> Result<(), RunError> {
        let work = tempfile::tempdir().expect("work directory");
        contract(root).run(work.path())
    }

    #[test]
    fn passing_root_runs_every_step() {
        let result = run("infra/passing");
        assert!(result.is_ok(), "got {result:?}");
    }

    #[test]
    fn misformatted_root_fails_fmt() {
        let result = run("infra/misformatted");
        assert!(
            matches!(
                result,
                Err(RunError::Failed {
                    step: Step::Fmt,
                    ..
                })
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn failing_test_fails_test() {
        let result = run("infra/failing");
        assert!(
            matches!(
                result,
                Err(RunError::Failed {
                    step: Step::Test,
                    ..
                })
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn missing_root_is_reported() {
        let result = run("infra/does-not-exist");
        assert!(
            matches!(result, Err(RunError::MissingRoot { .. })),
            "got {result:?}"
        );
    }
}
