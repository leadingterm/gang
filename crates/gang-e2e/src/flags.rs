//! The argument vectors of the Bazel invocations that drive the remote build farm.
//!
//! Every function here is pure: it maps the harness's settings and the way a build reaches
//! its executor to the arguments of one `bazel` command.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::settings::{ActionTools, Settings};

/// How a build reaches the remote executor.
///
/// Both variants run Bazel with the rc file that `gang bazelrc` rendered and ignore the user's
/// home rc and the workspace rc files; that rc file names the executor, cache, and ISA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Remote {
    /// Without a credential helper: Bazel sends no credentials. The executor, cache, and ISA come
    /// from the rc file that `gang bazelrc` rendered, or from a copy of it without its helper.
    Plain {
        /// The rc file: `gang bazelrc`'s rendering, or a copy of it without its credential helper.
        bazelrc: PathBuf,
        /// The isolated home directory used by the Bazel client.
        home: PathBuf,
    },
    /// TLS to the deployment's endpoint with bearer tokens minted by the `gang` credential
    /// helper. The executor, cache, and certificate authority come from the Bazel rc file that
    /// `gang bazelrc` rendered; workspace rc files other than that one are ignored.
    Gang {
        /// The host of the deployment's endpoint, which the credential helper serves.
        host: String,
        /// The Bazel rc file rendered by `gang bazelrc`.
        bazelrc: PathBuf,
        /// The `gang` binary Bazel runs as its credential helper.
        helper: PathBuf,
        /// The client home the credential helper reads its session from; Bazel's child process
        /// gets it as `HOME` and `XDG_CONFIG_HOME`.
        home: PathBuf,
    },
}

impl Remote {
    /// The Bazel rc file rendered by `gang bazelrc` for this deployment.
    #[must_use]
    pub fn bazelrc(&self) -> &Path {
        match self {
            Self::Plain { bazelrc, .. } | Self::Gang { bazelrc, .. } => bazelrc,
        }
    }
}

/// The arguments that precede the command: the output root, the rc files that must not leak
/// into the build, the generated rc file of the deployment, and the settings' extra startup
/// options.
#[must_use]
pub fn startup_args(settings: &Settings, output_user_root: &Path, remote: &Remote) -> Vec<String> {
    let mut args = vec![
        format!("--output_user_root={}", output_user_root.display()),
        "--nohome_rc".to_owned(),
        "--nosystem_rc".to_owned(),
        "--noworkspace_rc".to_owned(),
        format!("--bazelrc={}", remote.bazelrc().display()),
    ];
    args.extend(settings.bazel_startup_flags.iter().cloned());
    args
}

/// The arguments of one `build` of `targets` through the remote executor.
///
/// Every build carries `--verbose_failures`, so Bazel prints the full cause chain of a failed
/// remote call (the TLS alert, for instance) rather than only its top-level message.
///
/// Every build carries `--keep_going`, so one failed action does not cancel the other actions of
/// the same invocation, such as a neighbour that shares the worker.
///
/// The probe rule declares its action environment in the staged sample configuration.
#[must_use]
pub fn build_args(
    settings: &Settings,
    action_tools: &ActionTools,
    remote: &Remote,
    targets: &[String],
) -> Vec<String> {
    let mut args: Vec<String> = [
        "build",
        "--color=no",
        "--curses=no",
        "--noshow_progress",
        "--verbose_failures",
        "--keep_going",
        "--disk_cache=",
        "--spawn_strategy=remote",
        "--noremote_local_fallback",
        "--remote_download_outputs=all",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    args.push(format!("--remote_retries={}", settings.remote_retries));
    args.push(format!(
        "--shell_executable={}",
        action_tools.shell.display()
    ));
    args.push("--incompatible_strict_action_env".to_owned());

    if let Remote::Gang { host, helper, .. } = remote {
        args.push(format!("--credential_helper={host}={}", helper.display()));
    }

    args.extend(settings.bazel_flags.iter().cloned());
    args.extend(targets.iter().cloned());
    args
}

/// The file every action receives as `E2E_ATTEMPTS`: a counter the action appends one line to
/// on each attempt. It sits beside the signal, in the scenario's own signal directory.
#[must_use]
pub fn attempts_path(signal: &Path) -> PathBuf {
    signal.with_file_name("attempts")
}

/// The arguments of `bazel clean`.
#[must_use]
pub fn clean_args() -> Vec<String> {
    vec!["clean".to_owned()]
}

/// The `PATH` of a Bazel client using a credential helper: its directory first, followed by
/// the pinned action tool directories, never the host's inherited `PATH`.
#[must_use]
pub fn helper_path(helper: &Path, pinned: &str) -> OsString {
    let mut entries: Vec<PathBuf> = helper.parent().map(Path::to_path_buf).into_iter().collect();
    if !pinned.is_empty() {
        entries.extend(std::env::split_paths(pinned));
    }
    std::env::join_paths(entries).unwrap_or_else(|_| OsString::from(pinned))
}
