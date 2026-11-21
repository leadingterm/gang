//! Tunables, resolved deployment inputs, and tools used by sample actions.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::errors::{E2eError, Result};

/// Binary and rendered deployment fixture for one release.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseInput {
    /// Release executable.
    pub binary: PathBuf,
    /// Rendered deployment fixture.
    pub fixture: PathBuf,
}

/// Inputs needed by the mixed-version upgrade scenarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeInputs {
    /// Release upgraded or restarted by the positive mixed-version scenarios.
    pub previous: ReleaseInput,
    /// Rendered fixture for the candidate executable.
    pub candidate_fixture: PathBuf,
}

/// The resolved inputs used to create harness deployments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentInputs {
    /// The `gang` binary under test.
    pub gang: PathBuf,
    /// The Bazel binary the harness drives.
    pub bazel: PathBuf,
    /// The pinned `PostgreSQL` archive used by cluster scenarios.
    pub postgres: PathBuf,
    /// The base `gang.toml` the variants are applied to.
    pub base_config: String,
    /// The sample workspace the builds run in.
    pub sample: PathBuf,
    /// The directory of the checked-in gang.toml files.
    pub configs: PathBuf,
    /// The tunables of the harness.
    pub settings: Settings,
    /// The resolved tools used by the sample's actions.
    pub action_tools: ActionTools,
    /// Release artifacts needed by mixed-version scenarios, when selected.
    pub upgrade: Option<UpgradeInputs>,
}

/// Default for [`Settings::handshake_timeout`].
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default for [`Settings::startup_timeout`]: two minutes.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_mins(2);

/// Default for [`Settings::command_timeout`]: two minutes.
pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_mins(2);

/// Default for [`Settings::build_timeout`]: thirty minutes.
pub const DEFAULT_BUILD_TIMEOUT: Duration = Duration::from_mins(30);

/// Default for [`Settings::history_timeout`]: three minutes.
pub const DEFAULT_HISTORY_TIMEOUT: Duration = Duration::from_mins(3);

/// Default for [`Settings::hold_timeout`]: three minutes.
pub const DEFAULT_HOLD_TIMEOUT: Duration = Duration::from_mins(3);

/// Default gap bound, derived from the production NLB/lease handover protocol.
pub const DEFAULT_UPGRADE_GAP_LIMIT: Duration = gang_types::handover::handover_window(
    gang_types::handover::DEFAULT_RENEW_INTERVAL,
    gang_types::handover::DEFAULT_CALL_TIMEOUT,
);
/// Default interval between upgrade availability probes.
pub const DEFAULT_UPGRADE_PROBE_INTERVAL: Duration = Duration::from_millis(100);
/// Default timeout of an individual upgrade availability probe.
pub const DEFAULT_UPGRADE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Default for [`Settings::token_lifetime`]: ten minutes.
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_mins(10);

/// Default for [`Settings::stderr_tail_lines`].
pub const DEFAULT_STDERR_TAIL_LINES: usize = 30;

/// Default for [`Settings::log_tail_bytes`].
pub const DEFAULT_LOG_TAIL_BYTES: usize = 4096;

/// Default for [`Settings::parallel_scenarios`]: four.
pub const DEFAULT_PARALLEL_SCENARIOS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);

/// Default for [`Settings::worker_launcher`]: starts a worker in a scope delegated to the user,
/// through the host's `systemd-run` resolved from PATH, the client of the host's systemd user manager.
/// `--slice-inherit` (systemd 254+) keeps workers in the harness's slice, so its host-imposed
/// bounds apply to them instead of placing them in the user manager's `app.slice`.
pub const DEFAULT_WORKER_LAUNCHER: &[&str] = &[
    "systemd-run",
    "--user",
    "--scope",
    "--slice-inherit",
    "--collect",
    "--property=Delegate=yes",
    "--",
];

/// How the harness drives Bazel, waits, and shapes the environment of actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Settings {
    /// Extra Bazel startup options, placed before the command.
    pub bazel_startup_flags: Vec<String>,
    /// Extra Bazel options for every `build`, placed after the harness's own.
    pub bazel_flags: Vec<String>,
    /// The shell of the sample's actions. Defaults to `GANG_E2E_ACTION_SHELL`, which the flake's devShell sets.
    #[serde(default)]
    pub shell: Option<PathBuf>,
    /// The bubblewrap binary. Defaults to `GANG_E2E_BWRAP`, which the Linux devShell sets.
    #[serde(default)]
    pub bwrap: Option<PathBuf>,
    /// The headless Chromium binary. Defaults to `GANG_E2E_CHROMIUM`, which the Linux devShell sets.
    #[serde(default)]
    pub chromium: Option<PathBuf>,
    /// The PATH of the sample's actions. Defaults to `GANG_E2E_ACTION_PATH`, which the flake's devShell sets.
    #[serde(default)]
    pub action_path: Option<String>,
    /// Rootfs envelope used by bubblewrap workers, supplied by `GANG_E2E_ACTION_ROOTFS`.
    #[serde(default)]
    pub action_rootfs: Option<PathBuf>,
    /// How long connection setup, TLS, and the first HTTP/2 frame may take.
    #[serde(with = "gang_types::duration")]
    pub handshake_timeout: Duration,
    /// How long servers may take to start, or provider-owned workers to restart and enroll.
    #[serde(with = "gang_types::duration")]
    pub startup_timeout: Duration,
    /// How long one gang command may run.
    #[serde(with = "gang_types::duration")]
    pub command_timeout: Duration,
    /// How long one Bazel invocation may run.
    #[serde(with = "gang_types::duration")]
    pub build_timeout: Duration,
    /// How long a scenario waits for the scaling history to satisfy a condition, such as a fleet
    /// draining, before the wait becomes a failed check.
    #[serde(with = "gang_types::duration")]
    pub history_timeout: Duration,
    /// How long the harness waits for the hold action to signal that its orphan runs.
    #[serde(with = "gang_types::duration")]
    pub hold_timeout: Duration,
    /// Maximum permitted availability gap during an upgrade.
    #[serde(with = "gang_types::duration")]
    pub upgrade_gap_limit: Duration,
    /// Interval between upgrade availability probes.
    #[serde(with = "gang_types::duration")]
    pub upgrade_probe_interval: Duration,
    /// Timeout for one upgrade availability probe.
    #[serde(with = "gang_types::duration")]
    pub upgrade_probe_timeout: Duration,
    /// The lifetime of the tokens the in-process issuer mints.
    #[serde(with = "gang_types::duration")]
    pub token_lifetime: Duration,
    /// The directory under which each scenario creates its scratch directory; the system
    /// temporary directory when absent.
    pub scratch_root: Option<PathBuf>,
    /// The argv prefix `[provisioner.local] worker_launcher` that starts each Local Mode worker
    /// in its own delegated cgroup subtree when per-action limits are on. Its first word is the
    /// program.
    pub worker_launcher: Vec<String>,
    /// The cgroup v2 hierarchy under which the cgroups of the actions are created.
    pub cgroup_root: PathBuf,
    /// How many trailing lines of a process's stderr an error message quotes.
    pub stderr_tail_lines: usize,
    /// How many trailing bytes of the server log an error message quotes.
    pub log_tail_bytes: usize,
    /// How many independent scenarios run at once.
    pub parallel_scenarios: NonZeroUsize,
    /// Minimum resident victim bytes before inducing pressure. Defaults to 160 MiB.
    pub pressure_victim_minimum: gang_types::NonZeroBytes,
    /// Minimum resident neighbour bytes before inducing pressure. Defaults to 352 MiB.
    pub pressure_neighbour_minimum: gang_types::NonZeroBytes,
    /// Delay between pressure observations. Defaults to 50 ms.
    #[serde(with = "gang_types::duration")]
    pub pressure_observe_interval: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bazel_startup_flags: Vec::new(),
            bazel_flags: Vec::new(),
            shell: None,
            bwrap: None,
            chromium: None,
            action_path: None,
            action_rootfs: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            build_timeout: DEFAULT_BUILD_TIMEOUT,
            history_timeout: DEFAULT_HISTORY_TIMEOUT,
            hold_timeout: DEFAULT_HOLD_TIMEOUT,
            upgrade_gap_limit: DEFAULT_UPGRADE_GAP_LIMIT,
            upgrade_probe_interval: DEFAULT_UPGRADE_PROBE_INTERVAL,
            upgrade_probe_timeout: DEFAULT_UPGRADE_PROBE_TIMEOUT,
            token_lifetime: DEFAULT_TOKEN_LIFETIME,
            scratch_root: None,
            worker_launcher: DEFAULT_WORKER_LAUNCHER
                .iter()
                .map(|word| (*word).to_owned())
                .collect(),
            cgroup_root: gang_cgroup::CgroupConfig::default().cgroup_root.into(),
            stderr_tail_lines: DEFAULT_STDERR_TAIL_LINES,
            log_tail_bytes: DEFAULT_LOG_TAIL_BYTES,
            parallel_scenarios: DEFAULT_PARALLEL_SCENARIOS,
            pressure_victim_minimum: gang_types::NonZeroBytes::mib(160),
            pressure_neighbour_minimum: gang_types::NonZeroBytes::mib(352),
            pressure_observe_interval: Duration::from_millis(50),
        }
    }
}

impl Settings {
    /// The headless Chromium binary of the browser checks.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::System`] when it is unset.
    pub fn chromium_program(&self) -> Result<&Path> {
        self.chromium.as_deref().ok_or_else(|| E2eError::system(
            "the Chromium binary is unset: run the harness inside `nix develop`, or set `chromium` in the settings",
        ))
    }
}

/// The resolved shell and PATH used by the sample's actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionTools {
    /// The shell Bazel uses to run each action.
    pub shell: PathBuf,
    /// The PATH provided to each action.
    pub path: String,
    /// The bubblewrap binary a bwrap-sandboxed worker runs (`[sandbox.bwrap] program`);
    /// `None` where the platform has no bwrap.
    pub bwrap: Option<PathBuf>,
    /// A dynamically linked glibc program, as the toolchains Bazel downloads are, that the sample's
    /// `glibc_tool` probe runs; `None` where the platform has no bwrap.
    pub glibc_tool: Option<PathBuf>,
    /// Rootfs envelope used by bubblewrap workers.
    pub action_rootfs: Option<PathBuf>,
}

impl ActionTools {
    /// The bubblewrap binary bwrap-sandboxed workers run.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::System`] when the binary is unset.
    pub fn bwrap_program(&self) -> Result<&Path> {
        self.bwrap.as_deref().ok_or_else(|| {
            E2eError::system(
                "the bwrap binary is unset: run the harness inside `nix develop`, or set `bwrap` in the settings",
            )
        })
    }

    /// The dynamically linked glibc program of the glibc-toolchain scenario.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::System`] when the program is unset.
    pub fn glibc_tool_program(&self) -> Result<&Path> {
        self.glibc_tool.as_deref().ok_or_else(|| {
            E2eError::system(
                "the glibc tool is unset: run the harness with `bazel run //crates/gang-e2e:local`, which sets GANG_E2E_GLIBC_TOOL",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_inputs_round_trip_upgrade() {
        let release = |name: &str| ReleaseInput {
            binary: format!("{name}-binary").into(),
            fixture: format!("{name}-fixture").into(),
        };
        let inputs = DeploymentInputs {
            gang: "gang".into(),
            bazel: "bazel".into(),
            postgres: "postgres".into(),
            base_config: "base config".to_owned(),
            sample: "sample".into(),
            configs: "configs".into(),
            settings: Settings::default(),
            action_tools: ActionTools {
                shell: "shell".into(),
                path: "action-path".to_owned(),
                bwrap: Some("bwrap".into()),
                glibc_tool: Some("glibc".into()),
                action_rootfs: Some("rootfs".into()),
            },
            upgrade: Some(UpgradeInputs {
                previous: release("previous"),
                candidate_fixture: "candidate-fixture".into(),
            }),
        };
        let bytes = serde_json::to_vec(&inputs).expect("deployment inputs serialize");
        let decoded: DeploymentInputs =
            serde_json::from_slice(&bytes).expect("deployment inputs deserialize");
        assert_eq!(decoded.settings, inputs.settings);
        assert_eq!(decoded.action_tools, inputs.action_tools);
        let upgrade = decoded.upgrade.expect("upgrade inputs survive transport");
        assert_eq!(upgrade.previous.binary, PathBuf::from("previous-binary"));
        assert_eq!(upgrade.previous.fixture, PathBuf::from("previous-fixture"));
        assert_eq!(
            upgrade.candidate_fixture,
            PathBuf::from("candidate-fixture")
        );
    }

    #[test]
    fn chromium_defaults_to_unset_and_accepts_override() {
        let defaults: Settings = toml::from_str("").expect("default settings parse");
        assert_eq!(defaults.chromium, None);
        let settings: Settings =
            toml::from_str("chromium = '/bin/chromium'").expect("chromium override parses");
        assert_eq!(settings.chromium, Some(PathBuf::from("/bin/chromium")));
    }

    #[test]
    fn cgroup_root_defaults_to_cgroup_config_and_accepts_override() {
        let defaults: Settings = toml::from_str("").expect("default settings parse");
        assert_eq!(
            defaults.cgroup_root,
            PathBuf::from(gang_cgroup::CgroupConfig::default().cgroup_root)
        );

        let settings: Settings =
            toml::from_str("cgroup_root = '/tmp/cgroup'").expect("cgroup root override parses");
        assert_eq!(settings.cgroup_root, PathBuf::from("/tmp/cgroup"));
    }

    #[test]
    fn upgrade_durations_default_and_accept_overrides() {
        let defaults: Settings = toml::from_str("").expect("default settings parse");
        assert!(toml::from_str::<Settings>("upgrade_procedure = 'instance-refresh'").is_err());
        assert!(toml::from_str::<Settings>("upgrade_warmup_window = '25s'").is_err());
        assert!(toml::from_str::<Settings>("upgrade_drain_window = '12s'").is_err());
        assert_eq!(defaults.upgrade_gap_limit, Duration::from_secs(15));
        assert_eq!(defaults.upgrade_probe_interval, Duration::from_millis(100));
        assert_eq!(defaults.upgrade_probe_timeout, Duration::from_secs(1));

        let settings: Settings = toml::from_str(
            "upgrade_gap_limit = '20s'\nupgrade_probe_interval = '200ms'\nupgrade_probe_timeout = '2s'",
        )
        .expect("upgrade duration overrides parse");
        assert_eq!(settings.upgrade_gap_limit, Duration::from_secs(20));
        assert_eq!(settings.upgrade_probe_interval, Duration::from_millis(200));
        assert_eq!(settings.upgrade_probe_timeout, Duration::from_secs(2));
    }

    #[test]
    fn process_deadlines_have_defaults_and_accept_duration_overrides() {
        let defaults: Settings = toml::from_str("").expect("default settings parse");
        assert_eq!(defaults.startup_timeout, DEFAULT_STARTUP_TIMEOUT);
        assert_eq!(defaults.command_timeout, DEFAULT_COMMAND_TIMEOUT);
        assert_eq!(defaults.build_timeout, DEFAULT_BUILD_TIMEOUT);

        let settings: Settings =
            toml::from_str("startup_timeout = '3s'\ncommand_timeout = '4s'\nbuild_timeout = '5s'")
                .expect("deadline overrides parse");
        assert_eq!(settings.startup_timeout, Duration::from_secs(3));
        assert_eq!(settings.command_timeout, Duration::from_secs(4));
        assert_eq!(settings.build_timeout, Duration::from_secs(5));
    }
}
