//! The `local` binary: runs the end-to-end harness against a `gang` binary and prints the verdicts.
//!
//! Exit status: 0 when every check passes or is skipped, 1 when a check fails, 2 when the
//! invocation cannot run (bad arguments, unreadable files).

use std::env;
use std::ffi::OsString;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use gang_e2e::{
    ActionTools, Gate, Invocation, Scenario, Settings, anchored, run, unknown_scenarios,
};

/// Relative to the workspace: the base `gang.toml` the variants are applied to.
const DEFAULT_CONFIG: &str = "configs/gang.local.toml";

/// Relative to the workspace: the sample workspace the builds run in.
const DEFAULT_SAMPLE: &str = "e2e/sample";

/// Relative to the workspace: the directory of the checked-in gang.toml files.
const DEFAULT_CONFIGS: &str = "configs";

/// Runs the Local Mode end-to-end harness against a `gang` binary.
///
/// Run it through Bazel: `bazel run //crates/gang-e2e:local -- --gate 1`.
#[derive(Debug, Parser)]
struct Cli {
    /// The gang binary under test. Defaults to the one Bazel builds beside the harness.
    #[arg(long, value_name = "PATH")]
    gang: Option<PathBuf>,

    /// The Bazel binary to drive. Defaults to the pinned one Bazel provides beside the harness.
    #[arg(long, value_name = "PATH")]
    bazel: Option<PathBuf>,

    /// The gate to run: `1` or `one`, or `2` or `two`, which includes the earlier gate.
    #[arg(long, value_name = "GATE", default_value = "1")]
    gate: Gate,

    /// A scenario to run, by name; repeat for several. Omit to run every scenario of the gate.
    #[arg(long = "scenario", value_name = "NAME")]
    scenarios: Vec<Scenario>,

    /// Run only the scenarios this gate adds to the earlier gates.
    #[arg(long, conflicts_with = "scenarios")]
    additions: bool,

    /// The base `gang.toml` the variants are applied to. Defaults to a fixture of the workspace.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// The sample workspace the builds run in. Defaults to `e2e/sample` of the workspace.
    #[arg(long, value_name = "PATH")]
    sample: Option<PathBuf>,

    /// The directory of the checked-in gang.toml files. Defaults to `configs` of the workspace.
    #[arg(long, value_name = "DIR")]
    configs: Option<PathBuf>,

    /// A TOML file of harness settings. Every setting it omits takes its default.
    #[arg(long, value_name = "PATH")]
    settings: Option<PathBuf>,

    /// How many independent scenarios run at once; overrides the settings.
    #[arg(long, value_name = "N")]
    parallel: Option<NonZeroUsize>,
}

impl Cli {
    /// Resolves the paths and reads the files this invocation needs.
    ///
    /// # Errors
    ///
    /// A required binary or workspace default is missing, or a directory or file cannot be read
    /// or parsed.
    fn into_invocation(self) -> Result<(Gate, Invocation), String> {
        let directory = invocation_directory()?;
        let current = env::current_dir()
            .map_err(|error| format!("cannot read the current directory: {error}"))?;
        let gang = resolve_binary(self.gang, &directory, &current, "GANG_E2E_GANG", "--gang")?;
        let bazel = resolve_binary(
            self.bazel,
            &directory,
            &current,
            "GANG_E2E_BAZEL",
            "--bazel",
        )?;
        let workspace = non_empty_path(env::var_os("BUILD_WORKSPACE_DIRECTORY"));
        let workspace = workspace.as_deref();
        let config = resolve(
            self.config.map(|path| anchored(&path, &directory)),
            workspace,
            DEFAULT_CONFIG,
            "--config",
        )?;
        let sample = resolve(
            self.sample.map(|path| anchored(&path, &directory)),
            workspace,
            DEFAULT_SAMPLE,
            "--sample",
        )?;
        let configs = resolve(
            self.configs.map(|path| anchored(&path, &directory)),
            workspace,
            DEFAULT_CONFIGS,
            "--configs",
        )?;
        let mut settings = match &self.settings {
            Some(path) => read_settings(&anchored(path, &directory))?,
            None => Settings::default(),
        };
        if let Some(parallel) = self.parallel {
            settings.parallel_scenarios = parallel;
        }
        let action_tools = ActionTools {
            shell: settings
                .shell
                .clone()
                .or_else(|| env::var_os("GANG_E2E_ACTION_SHELL").map(PathBuf::from))
                .ok_or_else(|| "the sample's action shell is unset: run the harness inside `nix develop`, or set `shell` in the settings".to_owned())?,
            path: settings
                .action_path
                .clone()
                .or_else(|| env::var("GANG_E2E_ACTION_PATH").ok())
                .ok_or_else(|| "the sample's action PATH is unset: run the harness inside `nix develop`, or set `action_path` in the settings".to_owned())?,
            bwrap: settings
                .bwrap
                .clone()
                .or_else(|| env::var_os("GANG_E2E_BWRAP").map(PathBuf::from)),
        };
        let invocation = Invocation {
            gang,
            bazel,
            base_config: read_file(&config)?,
            sample,
            configs,
            settings,
            action_tools,
            scenarios: if self.additions {
                self.gate.additions().collect()
            } else {
                self.scenarios
            },
        };
        Ok((self.gate, invocation))
    }
}

fn main() -> ExitCode {
    // `gang login --token-command` re-executes this binary to print a workload proof.
    let arguments: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "proof-command")
    {
        return gang_e2e::proof_command(&arguments[2..]);
    }
    // `gang doctor --bazel` runs this binary as its Bazel in the cli scenario.
    if gang_e2e::fake_bazel_requested() {
        return gang_e2e::fake_bazel(env::args_os().skip(1));
    }
    let cli = Cli::parse();
    let rejected = unknown_scenarios(cli.gate, &cli.scenarios);
    if !rejected.is_empty() {
        Cli::command()
            .error(ErrorKind::InvalidValue, rejection(cli.gate, &rejected))
            .exit();
    }
    let (gate, invocation) = match cli.into_invocation() {
        Ok(parts) => parts,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::from(2);
        }
    };
    let report = run(&invocation, gate);
    println!("{report}");
    if report.checks.is_empty() {
        eprintln!("error: the run reached no checks, so nothing was verified");
        return ExitCode::FAILURE;
    }
    if report.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// The explicit binary flag, or a Bazel runfile relative to the process's starting directory.
fn resolve_binary(
    flag: Option<PathBuf>,
    directory: &Path,
    current: &Path,
    variable: &str,
    flag_name: &str,
) -> Result<PathBuf, String> {
    if let Some(path) = flag {
        return Ok(anchored(&path, directory));
    }
    env::var_os(variable)
        .map(|path| current.join(path))
        .ok_or_else(|| format!("{flag_name} is required outside bazel run: {variable} is unset"))
}

/// The explicit flag, or `relative` under the workspace.
fn resolve(
    flag: Option<PathBuf>,
    workspace: Option<&Path>,
    relative: &str,
    flag_name: &str,
) -> Result<PathBuf, String> {
    match (flag, workspace) {
        (Some(path), _) => Ok(path),
        (None, Some(directory)) => Ok(directory.join(relative)),
        (None, None) => Err(format!(
            "{flag_name} is required outside `bazel run`: BUILD_WORKSPACE_DIRECTORY is unset"
        )),
    }
}

/// Converts a non-empty environment value into a path.
fn non_empty_path(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

/// The directory relative paths are anchored to: where the user ran `bazel run`, else the
/// current directory.
fn invocation_directory() -> Result<PathBuf, String> {
    match non_empty_path(env::var_os("BUILD_WORKING_DIRECTORY")) {
        Some(directory) => Ok(directory),
        None => env::current_dir()
            .map_err(|error| format!("cannot read the current directory: {error}")),
    }
}

/// The usage error for scenarios that the selected gate does not run.
fn rejection(gate: Gate, rejected: &[Scenario]) -> String {
    format!(
        "{} not part of this gate; valid scenarios for it: {}",
        names(rejected),
        names(&gate.scenarios().collect::<Vec<_>>())
    )
}

fn names(scenarios: &[Scenario]) -> String {
    scenarios
        .iter()
        .map(|scenario| scenario.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn read_file(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|error| format!("cannot read {}: {error}", path.display()))
}

fn read_settings(path: &Path) -> Result<Settings, String> {
    let text = read_file(path)?;
    toml::from_str(&text).map_err(|error| format!("invalid settings {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use super::non_empty_path;

    #[test]
    fn empty_environment_paths_are_unset() {
        assert_eq!(non_empty_path(None), None);
        assert_eq!(non_empty_path(Some(OsString::new())), None);
        assert_eq!(
            non_empty_path(Some(OsString::from("workspace"))),
            Some(PathBuf::from("workspace")),
        );
    }
}
