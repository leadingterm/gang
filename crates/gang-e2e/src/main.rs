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
    ActionTools, DeploymentInputs, EgressTools, Gate, Invocation, ReleaseInput, Scenario, Settings,
    UpgradeInputs, UpgradeProcedure, anchored, run, unknown_scenarios,
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

    /// Previous release executable.
    #[arg(long, value_name = "PATH")]
    previous_gang: Option<PathBuf>,
    /// Previous release deployment fixture.
    #[arg(long, value_name = "PATH")]
    previous_fixture: Option<PathBuf>,
    /// Candidate deployment fixture.
    #[arg(long, value_name = "PATH")]
    candidate_fixture: Option<PathBuf>,

    /// Upgrade procedure; overrides settings (default: pre-stop).
    #[arg(long, value_enum)]
    upgrade_procedure: Option<UpgradeProcedure>,

    /// The gate to run: `1` or `2`, which includes the earlier gate.
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

    /// Pinned rootless network implementation.
    #[arg(long, value_name = "PATH")]
    slirp4netns: Option<PathBuf>,
    /// Pinned nftables program.
    #[arg(long, value_name = "PATH")]
    nft: Option<PathBuf>,
    /// Pinned namespace entry program.
    #[arg(long, value_name = "PATH")]
    nsenter: Option<PathBuf>,
    /// Pinned network configuration program.
    #[arg(long, value_name = "PATH")]
    ip: Option<PathBuf>,
    /// Pinned namespace creation program.
    #[arg(long, value_name = "PATH")]
    unshare: Option<PathBuf>,
    /// Repo-built action egress probe.
    #[arg(long, value_name = "PATH")]
    egress_probe: Option<PathBuf>,
    /// Repo-built fixture peer.
    #[arg(long, value_name = "PATH")]
    egress_peer: Option<PathBuf>,
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
        let upgrade = self.resolve_upgrade(&scenarios, &directory, &current)?;
        let mut settings = match &self.settings {
            Some(path) => read_settings(&anchored(path, &directory))?,
            None => Settings::default(),
        };
        self.apply_settings_overrides(&mut settings);
        let egress = self.resolve_egress_tools(&directory, &current);
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
            glibc_tool: env::var_os("GANG_E2E_GLIBC_TOOL").map(|path| current.join(path)),
            action_rootfs: settings
                .action_rootfs
                .clone()
                .or_else(|| env::var_os("GANG_E2E_ACTION_ROOTFS").map(PathBuf::from)),
            egress,
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
                upgrade,
            },
            scenarios,
        };
        Ok((self.gate, invocation))
    }

    fn apply_settings_overrides(&self, settings: &mut Settings) {
        if let Some(parallel) = self.parallel {
            settings.parallel_scenarios = parallel;
        }
        if let Some(procedure) = self.upgrade_procedure {
            settings.upgrade_procedure = procedure;
        }
    }

    fn resolve_upgrade(
        &self,
        scenarios: &[Scenario],
        directory: &Path,
        current: &Path,
    ) -> Result<Option<UpgradeInputs>, String> {
        if !cfg!(target_os = "linux")
            || !scenarios.iter().any(|scenario| {
                matches!(
                    scenario,
                    Scenario::RollingUpgrade
                        | Scenario::RollingUpgradeAutomatic
                        | Scenario::ConfigRefetchRestart
                )
            })
        {
            return Ok(None);
        }
        let resolve = |path: &Option<PathBuf>, variable, flag| {
            resolve_binary(path.clone(), directory, current, variable, flag)
        };
        Ok(Some(UpgradeInputs {
            previous: ReleaseInput {
                binary: resolve(
                    &self.previous_gang,
                    "GANG_E2E_PREVIOUS_GANG",
                    "--previous-gang",
                )?,
                fixture: resolve(
                    &self.previous_fixture,
                    "GANG_E2E_PREVIOUS_FIXTURE",
                    "--previous-fixture",
                )?,
            },
            candidate_fixture: resolve(
                &self.candidate_fixture,
                "GANG_E2E_CANDIDATE_FIXTURE",
                "--candidate-fixture",
            )?,
        }))
    }

    fn resolve_egress_tools(&self, directory: &Path, current: &Path) -> Option<EgressTools> {
        let optional_tool = |flag: &Option<PathBuf>, variable: &str| {
            flag.as_ref()
                .map(|path| anchored(path, directory))
                .or_else(|| non_empty_path(env::var_os(variable)).map(|path| current.join(path)))
        };
        match (
            optional_tool(&self.slirp4netns, "GANG_E2E_SLIRP4NETNS"),
            optional_tool(&self.nft, "GANG_E2E_NFT"),
            optional_tool(&self.nsenter, "GANG_E2E_NSENTER"),
            optional_tool(&self.ip, "GANG_E2E_IP"),
            optional_tool(&self.unshare, "GANG_E2E_UNSHARE"),
            optional_tool(&self.egress_probe, "GANG_E2E_EGRESS_PROBE"),
            optional_tool(&self.egress_peer, "GANG_E2E_EGRESS_PEER"),
        ) {
            (
                Some(slirp4netns),
                Some(nft),
                Some(nsenter),
                Some(ip),
                Some(unshare),
                Some(probe),
                Some(peer),
            ) => Some(EgressTools {
                slirp4netns,
                nft,
                nsenter,
                ip,
                unshare,
                probe,
                peer,
                policy: None,
                targets: None,
                repository_cache: None,
            }),
            _ => None,
        }
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
    if let Some(status) = gang_e2e::egress_fixture::private_main(&arguments[1..]) {
        return status;
    }
    if let Some(status) = gang_e2e::egress_lifecycle::private_main(&arguments[1..]) {
        return status;
    }
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "--gang-e2e-action-environment-probe")
    {
        return gang_e2e::action_environment_probe(&arguments[2..]);
    }
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

    use clap::Parser;
    use gang_e2e::{Scenario, Settings, UpgradeProcedure};

    use super::{Cli, Gate, Shard, non_empty_path, partition};

    #[test]
    fn upgrade_procedure_cli_overrides_settings_only_when_present() {
        let plain = Cli::try_parse_from(["local"]).expect("default CLI");
        let mut settings: Settings =
            toml::from_str("upgrade_procedure = 'instance-refresh'").expect("settings");
        plain.apply_settings_overrides(&mut settings);
        assert_eq!(
            settings.upgrade_procedure,
            UpgradeProcedure::InstanceRefresh
        );

        let cli = Cli::try_parse_from(["local", "--upgrade-procedure", "pre-stop"])
            .expect("pre-stop CLI");
        cli.apply_settings_overrides(&mut settings);
        assert_eq!(settings.upgrade_procedure, UpgradeProcedure::PreStop);

        let cli = Cli::try_parse_from(["local", "--upgrade-procedure", "instance-refresh"])
            .expect("instance-refresh CLI");
        cli.apply_settings_overrides(&mut settings);
        assert_eq!(
            settings.upgrade_procedure,
            UpgradeProcedure::InstanceRefresh
        );
        assert!(Cli::try_parse_from(["local", "--upgrade-procedure", "invalid"]).is_err());
    }

    #[test]
    fn upgrade_selection_and_release_flags_are_explicit() {
        let cli = Cli::try_parse_from([
            "local",
            "--gate",
            "2",
            "--scenario",
            "config-refetch-restart",
            "--previous-gang",
            "previous",
            "--previous-fixture",
            "previous.json",
            "--candidate-fixture",
            "candidate.json",
        ])
        .expect("upgrade arguments parse");
        assert_eq!(
            cli.selected_scenarios().unwrap(),
            vec![Scenario::ConfigRefetchRestart]
        );
        assert_eq!(cli.previous_gang, Some(PathBuf::from("previous")));
        assert_eq!(cli.previous_fixture, Some(PathBuf::from("previous.json")));
        assert_eq!(cli.candidate_fixture, Some(PathBuf::from("candidate.json")));
        let selected = [
            Scenario::RollingUpgrade,
            Scenario::RollingUpgradeAutomatic,
            Scenario::ConfigRefetchRestart,
        ];
        let shards: Vec<_> = (1..=2)
            .map(|index| partition(&selected, format!("{index}/2").parse().unwrap()))
            .collect();
        for scenario in selected {
            assert_eq!(
                shards
                    .iter()
                    .filter(|shard| shard.contains(&scenario))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn shards_are_valid_deterministic_and_cover_the_selection() {
        for invalid in ["", "0/1", "2/1", "1/0", "0/0", "1", "1/2/3", "-1/2"] {
            assert!(invalid.parse::<Shard>().is_err(), "{invalid}");
        }
        for selected in [
            Gate::One.scenarios().collect::<Vec<_>>(),
            Gate::Two.scenarios().collect(),
            Gate::Two.additions().collect(),
        ] {
            for count in [1, 2, 8] {
                let shards: Vec<_> = (1..=count)
                    .map(|index| partition(&selected, format!("{index}/{count}").parse().unwrap()))
                    .collect();
                assert!(shards.iter().all(|shard| !shard.is_empty()));
                assert_eq!(shards.iter().map(Vec::len).sum::<usize>(), selected.len());
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
                    assert!(shard.iter().all(|scenario| selected.contains(scenario)));
                    let execution_order: Vec<_> = selected
                        .iter()
                        .copied()
                        .filter(|scenario| shard.contains(scenario))
                        .collect();
                    assert_eq!(*shard, execution_order);
                    assert_eq!(
                        *shard,
                        partition(&selected, format!("{}/{count}", index + 1).parse().unwrap())
                    );
                }
            }
        }
    }

    #[test]
    fn ci_gate_selections_retain_the_complete_suite_exactly_once() {
        use std::collections::BTreeSet;

        use gang_e2e::Scenario;

        let complete: BTreeSet<_> = Scenario::ALL.into_iter().collect();
        let matrices: [(Gate, &[&str]); 2] = [
            (Gate::One, &["local", "--gate", "1"]),
            (Gate::Two, &["local", "--gate", "2", "--additions"]),
        ];
        let mut matrix_selections = Vec::new();
        for (gate, arguments) in matrices {
            let expected: BTreeSet<_> = Scenario::ALL
                .into_iter()
                .filter(|scenario| scenario.gate() == gate)
                .collect();
            let selected: Vec<_> = (1..=8)
                .flat_map(|index| {
                    let shard = format!("{index}/8");
                    Cli::try_parse_from(
                        arguments.iter().copied().chain(["--shard", shard.as_str()]),
                    )
                    .unwrap()
                    .selected_scenarios()
                    .unwrap()
                })
                .collect();
            let unique: BTreeSet<_> = selected.iter().copied().collect();
            assert_eq!(
                selected.len(),
                unique.len(),
                "{gate:?} matrix repeats a scenario"
            );
            assert_eq!(unique, expected, "{gate:?} matrix coverage");
            matrix_selections.push(unique);
        }
        assert!(matrix_selections[0].is_disjoint(&matrix_selections[1]));
        assert_eq!(
            matrix_selections
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>(),
            complete
        );
        let all: Vec<_> = Gate::Two.scenarios().collect();
        assert_eq!(all.len(), complete.len());
        assert_eq!(all.into_iter().collect::<BTreeSet<_>>(), complete);
    }

    #[test]
    fn excess_shards_are_empty_and_rejected_at_selection() {
        for selected in [
            Gate::One.scenarios().collect::<Vec<_>>(),
            Gate::Two.additions().collect(),
        ] {
            let count = selected.len() + 1;
            let populated: Vec<_> = (1..count)
                .flat_map(|index| {
                    partition(
                        &selected,
                        Shard {
                            index: index - 1,
                            count,
                        },
                    )
                })
                .collect();
            assert_eq!(populated.len(), selected.len());
            for scenario in &selected {
                assert_eq!(populated.iter().filter(|item| *item == scenario).count(), 1);
            }
            let empty = Shard {
                index: count - 1,
                count,
            };
            assert!(partition(&selected, empty).is_empty());
        }
        let populated = Cli::try_parse_from([
            "local",
            "--gate",
            "1",
            "--scenario",
            "lifecycle",
            "--shard",
            "1/2",
        ])
        .unwrap();
        assert_eq!(populated.selected_scenarios().unwrap().len(), 1);
        let empty = Cli::try_parse_from([
            "local",
            "--gate",
            "1",
            "--scenario",
            "lifecycle",
            "--shard",
            "2/2",
        ])
        .unwrap();
        assert_eq!(
            empty.selected_scenarios().unwrap_err(),
            "the selected shard has no scenarios"
        );
    }

    #[test]
    fn gate_two_ci_shards_fit_the_weighted_launcher_budget() {
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
        for index in 1..=8 {
            let shard = partition(&selected, format!("{index}/8").parse().unwrap());
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
