//! The `local` binary: runs the end-to-end harness against a `gang` binary and prints the verdicts.
//!
//! Exit status: 0 when every check passes or is skipped, 1 when a check fails, 2 when the
//! invocation cannot run (bad arguments, unreadable files). A harness killed by a signal, or a
//! reaper stopped by SIGTERM, SIGINT, SIGHUP or SIGQUIT, exits with 128 plus the signal number.

use std::env;
use std::ffi::OsString;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use gang_e2e::{
    ActionTools, DeploymentInputs, Gate, Invocation, Scenario, Settings, anchored, run,
    unknown_scenarios,
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

    /// The pinned `PostgreSQL` archive. Defaults to the one Bazel provides.
    #[arg(long, value_name = "PATH")]
    postgres: Option<PathBuf>,

    /// The gate to run: `1` or `one`, or `2` or `two`, which includes the earlier gate.
    #[arg(long, value_name = "GATE", default_value = "1")]
    gate: Gate,

    /// A scenario to run, by name; repeat for several. Omit to run every scenario of the gate.
    #[arg(long = "scenario", value_name = "NAME")]
    scenarios: Vec<Scenario>,

    /// Run only the scenarios this gate adds to the earlier gates.
    #[arg(long, conflicts_with = "scenarios")]
    additions: bool,

    /// Run one deterministically balanced shard of the selected scenarios, numbered from 1.
    #[arg(long, value_name = "K/N")]
    shard: Option<Shard>,

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
        let scenarios = self.selected_scenarios()?;
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
        let postgres = resolve_binary(
            self.postgres,
            &directory,
            &current,
            "GANG_E2E_POSTGRES",
            "--postgres",
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
            action_bin: settings
                .action_bin
                .clone()
                .or_else(|| env::var_os("GANG_E2E_ACTION_BIN").map(PathBuf::from))
                .ok_or_else(|| "the sample's action tools directory is unset: run the harness inside `nix develop`, or set `action_bin` in the settings".to_owned())?,
            bwrap: settings
                .bwrap
                .clone()
                .or_else(|| env::var_os("GANG_E2E_BWRAP").map(PathBuf::from)),
            glibc_tool: env::var_os("GANG_E2E_GLIBC_TOOL").map(|path| current.join(path)),
            glibc_loader: ActionTools::host_glibc_loader(),
        };
        if settings.chromium.is_none() {
            settings.chromium = env::var_os("GANG_E2E_CHROMIUM").map(PathBuf::from);
        }
        let invocation = Invocation {
            inputs: DeploymentInputs {
                gang,
                bazel,
                postgres,
                base_config: read_file(&config)?,
                sample,
                configs,
                settings,
                action_tools,
            },
            scenarios,
        };
        Ok((self.gate, invocation))
    }

    fn selected_scenarios(&self) -> Result<Vec<Scenario>, String> {
        let selected: Vec<_> = if self.additions {
            self.gate.additions().collect()
        } else if self.scenarios.is_empty() {
            self.gate.scenarios().collect()
        } else {
            self.gate
                .scenarios()
                .filter(|scenario| self.scenarios.contains(scenario))
                .collect()
        };
        match self.shard {
            Some(shard) => {
                let selected = partition(&selected, shard);
                if selected.is_empty() {
                    return Err("the selected shard has no scenarios".to_owned());
                }
                Ok(selected)
            }
            None => Ok(selected),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Shard {
    index: usize,
    count: usize,
}

impl FromStr for Shard {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || format!("invalid shard `{text}`; expected K/N with 1 <= K <= N");
        let (index, count) = text.split_once('/').ok_or_else(invalid)?;
        let index: usize = index.parse().map_err(|_| invalid())?;
        let count: usize = count.parse().map_err(|_| invalid())?;
        if index == 0 || index > count {
            return Err(invalid());
        }
        Ok(Self {
            index: index - 1,
            count,
        })
    }
}

/// Assign the heaviest scenarios first to the lightest shard, breaking ties by scenario order
/// and shard number. Return the chosen shard in execution order.
fn partition(scenarios: &[Scenario], shard: Shard) -> Vec<Scenario> {
    let mut ordered: Vec<_> = scenarios.iter().copied().enumerate().collect();
    ordered.sort_by_key(|(_, scenario)| std::cmp::Reverse(scenario.weight()));
    let mut loads = vec![0_u64; shard.count.min(scenarios.len())];
    let mut assigned = Vec::new();
    for (position, scenario) in ordered {
        let Some((index, load)) = loads.iter_mut().enumerate().min_by_key(|(_, load)| **load)
        else {
            break;
        };
        *load += scenario.weight();
        if index == shard.index {
            assigned.push((position, scenario));
        }
    }
    assigned.sort_by_key(|(position, _)| *position);
    assigned.into_iter().map(|(_, scenario)| scenario).collect()
}

fn main() -> ExitCode {
    // `gang login --token-command` re-executes this binary to print a workload proof.
    let arguments: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "--gang-e2e-launch-gate")
    {
        return gang_e2e::run_launch_gate(&arguments[2..]);
    }
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
    gang_e2e::reaped(harness)
}

/// Parses the command line, runs the selected scenarios and prints the verdicts.
fn harness() -> ExitCode {
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

/// The explicit path flag, or a Bazel runfile relative to the process's starting directory.
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

    use super::{Gate, Shard, non_empty_path, partition};

    #[test]
    fn shards_are_valid_deterministic_and_cover_the_selection() {
        for invalid in ["", "0/1", "2/1", "1/0", "1", "1/2/3", "-1/2"] {
            assert!(invalid.parse::<Shard>().is_err(), "{invalid}");
        }
        for selected in [
            Gate::One.scenarios().collect::<Vec<_>>(),
            Gate::Two.scenarios().collect(),
            Gate::Two.additions().collect(),
        ] {
            for count in [1, 2] {
                let shards: Vec<_> = (1..=count)
                    .map(|index| partition(&selected, format!("{index}/{count}").parse().unwrap()))
                    .collect();
                for scenario in &selected {
                    assert_eq!(
                        shards
                            .iter()
                            .filter(|shard| shard.contains(scenario))
                            .count(),
                        1
                    );
                }
                for (index, shard) in shards.iter().enumerate() {
                    assert_eq!(
                        *shard,
                        partition(&selected, format!("{}/{count}", index + 1).parse().unwrap())
                    );
                }
            }
        }
    }

    #[test]
    fn gate_two_shards_fit_the_measured_budget() {
        let path = std::env::var_os("GANG_E2E_GATE_SCRIPT")
            .map_or_else(|| PathBuf::from("../../tools/e2e-gate.sh"), PathBuf::from);
        let script = std::fs::read_to_string(path).expect("gate script is available");
        let budget: u64 = script
            .lines()
            .find_map(|line| line.strip_prefix("budget="))
            .expect("gate script declares budget")
            .parse()
            .expect("numeric budget");
        let selected: Vec<_> = Gate::Two.additions().collect();
        for index in 1..=2 {
            let shard = partition(&selected, format!("{index}/2").parse().unwrap());
            // run_bounded starts scenarios in selected order on the next available slot.
            let mut slots = vec![0_u64; gang_e2e::Settings::default().parallel_scenarios.get()];
            for scenario in shard {
                let slot = slots.iter_mut().min().expect("parallelism is nonzero");
                *slot += scenario.weight();
            }
            let makespan = slots.into_iter().max().expect("parallelism is nonzero");
            assert!(
                2 * makespan <= budget,
                "shard {index}: estimated {makespan}s exceeds half the {budget}s budget"
            );
        }
    }

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
