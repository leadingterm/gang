//! Runs the Bazel launcher against the remote executor and reports what it did.

use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::sync::oneshot;

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::flags::{self, Remote};
use crate::gang_cli::client_home;
use crate::process::{self, Ran, scrub_environment};
use crate::settings::{ActionTools, Settings};
use crate::system::Probe;

/// One Bazel workspace and the output root its server runs under.
#[derive(Debug, Clone)]
pub struct Bazel {
    /// The Bazel binary to run.
    program: PathBuf,
    settings: Settings,
    action_tools: ActionTools,
    workspace: PathBuf,
    output_user_root: PathBuf,
}

impl Bazel {
    /// A Bazel driver for `workspace`, whose server keeps its state under `output_user_root`.
    #[must_use]
    pub const fn new(
        program: PathBuf,
        settings: Settings,
        action_tools: ActionTools,
        workspace: PathBuf,
        output_user_root: PathBuf,
    ) -> Self {
        Self {
            program,
            settings,
            action_tools,
            workspace,
            output_user_root,
        }
    }
    /// The pinned Bazel executable used by this driver.
    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Builds `probes` through `remote`, as `run` when there is one, and reports the exit code,
    /// the output, and the process summary. A failing build is an outcome, not an error.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::Io`] when the launcher cannot be started or read,
    /// [`E2eError::Timeout`] when it exceeds the build deadline, or [`E2eError::System`] with an
    /// abort reason.
    pub fn build_until(
        &self,
        remote: &Remote,
        probes: &[Probe],
        run: Option<&Run>,
        abort: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        let command = self.invocation(
            remote,
            flags::build_args(&self.settings, &self.action_tools, remote, probes, run),
        );
        let output = process::run_until(command, None, self.settings.build_timeout, abort)?;
        Ok(BuildOutcome::new(output.code, output.combined()))
    }

    /// Cleans the workspace's outputs through `remote`'s server.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::Io`] when the launcher cannot be started or read,
    /// [`E2eError::Timeout`] on expiry, and [`E2eError::Command`] if `clean` exits unsuccessfully.
    pub fn clean(&self, remote: &Remote) -> Result<()> {
        let command = self.invocation(remote, flags::clean_args());
        let output = self.execute(command)?;
        if output.succeeded() {
            return Ok(());
        }
        Err(E2eError::Command {
            program: self.program.display().to_string(),
            outcome: output.code.map_or_else(
                || "terminated by a signal".to_owned(),
                |code| format!("exited with status {code}"),
            ),
            output: output.combined(),
        })
    }

    /// Asks the Bazel server to stop. Best effort: failures are ignored.
    pub fn shutdown(&self, remote: &Remote) {
        let command = self.invocation(remote, vec!["shutdown".to_owned()]);
        let _ = self.execute(command);
    }

    fn invocation(&self, remote: &Remote, arguments: Vec<String>) -> Command {
        // The pinned Bazel from the harness's runfiles (MODULE.bazel), never one found on PATH.
        let mut command = Command::new(&self.program);
        command
            .current_dir(&self.workspace)
            .args(flags::startup_args(
                &self.settings,
                &self.output_user_root,
                remote,
            ))
            .args(arguments);
        scrub_environment(&mut command);
        match remote {
            Remote::Plain { home, .. } => {
                client_home(&mut command, home);
                command.env("PATH", &self.action_tools.path);
            }
            Remote::Gang { home, helper, .. } => {
                client_home(&mut command, home);
                command.env("PATH", flags::helper_path(helper, &self.action_tools.path));
            }
        }
        command
    }

    fn execute(&self, command: Command) -> Result<Ran> {
        process::run(command, None, self.settings.build_timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    #[test]
    fn every_invocation_uses_the_remote_home_and_pinned_path() {
        let bazel = Bazel::new(
            PathBuf::from("/tools/bazel"),
            Settings::default(),
            ActionTools {
                shell: PathBuf::from("/tools/sh"),
                path: "/pinned/bin:/pinned/other".to_owned(),
                action_bin: PathBuf::from("/nix/store/ddd-action-tools/bin"),
                bwrap: None,
                glibc_tool: None,
                glibc_loader: Vec::new(),
            },
            PathBuf::from("/workspace"),
            PathBuf::from("/output"),
        );
        let remotes = [
            (
                Remote::Plain {
                    bazelrc: PathBuf::from("/plain/rc"),
                    home: PathBuf::from("/plain/home"),
                },
                "/plain/home",
                "/pinned/bin:/pinned/other",
            ),
            (
                Remote::Gang {
                    host: "example.test".to_owned(),
                    bazelrc: PathBuf::from("/gang/rc"),
                    helper: PathBuf::from("/helper/bin/gang"),
                    home: PathBuf::from("/gang/home"),
                },
                "/gang/home",
                "/helper/bin:/pinned/bin:/pinned/other",
            ),
        ];
        for (remote, home, path) in remotes {
            for args in [
                vec!["build".to_owned()],
                flags::clean_args(),
                vec!["shutdown".to_owned()],
            ] {
                let command = bazel.invocation(&remote, args);
                let env: Vec<_> = command.get_envs().collect();
                assert!(env.contains(&(OsStr::new("HOME"), Some(OsStr::new(home)))));
                assert!(env.contains(&(OsStr::new("XDG_CONFIG_HOME"), Some(OsStr::new(home)))));
                assert!(env.contains(&(OsStr::new("RUNNER_TEMP"), None)));
                assert!(env.contains(&(OsStr::new("PATH"), Some(OsStr::new(path)))));
            }
        }
    }
}
