//! Tunables of the harness and the resolved tools used by sample actions.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::errors::{E2eError, Result};

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

/// Default for [`Settings::token_lifetime`]: ten minutes.
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_mins(10);

/// Default for [`Settings::remote_retries`].
pub const DEFAULT_REMOTE_RETRIES: u32 = 10;

/// Default for [`Settings::stderr_tail_lines`].
pub const DEFAULT_STDERR_TAIL_LINES: usize = 30;

/// Default for [`Settings::log_tail_bytes`].
pub const DEFAULT_LOG_TAIL_BYTES: usize = 4096;

/// Default for [`Settings::parallel_scenarios`]: four.
pub const DEFAULT_PARALLEL_SCENARIOS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);

/// Default for [`Settings::worker_launcher`]: starts a worker in a scope delegated to the user,
/// through the host's `systemd-run` resolved from PATH, the client of the host's systemd user manager.
pub const DEFAULT_WORKER_LAUNCHER: &[&str] = &[
    "systemd-run",
    "--user",
    "--scope",
    "--collect",
    "--property=Delegate=yes",
    "--",
];

/// How the harness drives Bazel, waits, and shapes the environment of actions.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    /// The PATH of the sample's actions. Defaults to `GANG_E2E_ACTION_PATH`, which the flake's devShell sets.
    #[serde(default)]
    pub action_path: Option<String>,
    /// How long connection setup, TLS, and the first HTTP/2 frame may take.
    #[serde(with = "gang_types::duration")]
    pub handshake_timeout: Duration,
    /// How long the gang server may take to announce its listeners.
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
    /// The lifetime of the tokens the in-process issuer mints.
    #[serde(with = "gang_types::duration")]
    pub token_lifetime: Duration,
    /// How often Bazel retries a failed remote call (`--remote_retries`).
    pub remote_retries: u32,
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
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bazel_startup_flags: Vec::new(),
            bazel_flags: Vec::new(),
            shell: None,
            bwrap: None,
            action_path: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            build_timeout: DEFAULT_BUILD_TIMEOUT,
            history_timeout: DEFAULT_HISTORY_TIMEOUT,
            hold_timeout: DEFAULT_HOLD_TIMEOUT,
            token_lifetime: DEFAULT_TOKEN_LIFETIME,
            remote_retries: DEFAULT_REMOTE_RETRIES,
            scratch_root: None,
            worker_launcher: DEFAULT_WORKER_LAUNCHER
                .iter()
                .map(|word| (*word).to_owned())
                .collect(),
            cgroup_root: gang_cgroup::CgroupConfig::default().cgroup_root.into(),
            stderr_tail_lines: DEFAULT_STDERR_TAIL_LINES,
            log_tail_bytes: DEFAULT_LOG_TAIL_BYTES,
            parallel_scenarios: DEFAULT_PARALLEL_SCENARIOS,
        }
    }
}

/// The resolved shell and PATH used by the sample's actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionTools {
    /// The shell Bazel uses to run each action.
    pub shell: PathBuf,
    /// The PATH provided to each action.
    pub path: String,
    /// The bubblewrap binary a bwrap-sandboxed worker runs (`[sandbox.bwrap] program`);
    /// `None` where the platform has no bwrap.
    pub bwrap: Option<PathBuf>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
