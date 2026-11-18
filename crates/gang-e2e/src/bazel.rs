//! Runs the Bazel launcher against the remote executor and reports what it did.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process};
use tokio::sync::oneshot;

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::flags::{self, Remote};
use crate::gang_cli::client_home;
use crate::process::{self, Ran, scrub_environment};
use crate::settings::{ActionTools, Settings};
use crate::system::Probe;

/// Where a Bazel server records its process id, relative to its output base.
const SERVER_PID_FILE: &str = "server/server.pid.txt";

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

    pub(crate) fn with_output_user_root(&self, root: PathBuf) -> Self {
        let mut driver = self.clone();
        driver.output_user_root = root;
        driver
    }

    /// The same driver with one more Bazel build flag.
    #[must_use]
    pub(crate) fn with_flag(&self, flag: impl Into<String>) -> Self {
        let mut driver = self.clone();
        driver.settings.bazel_flags.push(flag.into());
        driver
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
        self.build_command(command, Some(abort))
    }

    /// Builds using Bazel's normal rc discovery in the supplied workspace and home.
    ///
    /// # Errors
    ///
    /// Returns an error if Bazel cannot be run or exceeds the build timeout.
    pub(crate) fn build_with_user_rc(
        &self,
        workspace: &Path,
        home: &Path,
        probes: &[Probe],
        run: Option<&Run>,
    ) -> Result<BuildOutcome> {
        let remote = Remote::Plain {
            bazelrc: PathBuf::new(),
            home: home.to_path_buf(),
        };
        let mut command = Command::new(&self.program);
        command
            .current_dir(workspace)
            .args(flags::user_rc_startup_args(
                &self.settings,
                &self.output_user_root,
            ))
            .args(flags::build_args(
                &self.settings,
                &self.action_tools,
                &remote,
                probes,
                run,
            ))
            .env("PATH", &self.action_tools.path);
        scrub_environment(&mut command);
        client_home(&mut command, home);
        self.build_command(command, None)
    }

    /// Runs a build command with an optional early-abort signal.
    fn build_command(
        &self,
        command: Command,
        abort: Option<oneshot::Receiver<String>>,
    ) -> Result<BuildOutcome> {
        let output = match abort {
            Some(abort) => process::run_until(command, None, self.settings.build_timeout, abort)?,
            None => process::run(command, None, self.settings.build_timeout)?,
        };
        Ok(BuildOutcome::new(output.code, output.combined()))
    }

    /// The pinned tool path used by Bazel clients.
    pub(crate) fn action_path(&self) -> &str {
        &self.action_tools.path
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

    /// Kills every Bazel server under the output root with `SIGKILL`, abandoning the command each
    /// still runs, as the death of the client's host would. `bazel shutdown` is no substitute: it
    /// waits for the running command, and a build whose remote action no worker can take never
    /// ends. A server that is already gone is not an error, and a recorded pid whose process
    /// does not carry the exact output base argument (Bazel leaves the pid file behind; the pid
    /// may be reused) is left alone.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the output root or a pid file cannot be read or a server cannot be
    /// signalled; [`E2eError::System`] when a pid file holds no process id.
    pub fn kill_servers(&self) -> Result<()> {
        let bases = match fs::read_dir(&self.output_user_root) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            bases => bases.map_err(E2eError::io("reading the Bazel output root"))?,
        };
        for base in bases {
            let output_base = base
                .map_err(E2eError::io("reading the Bazel output root"))?
                .path();
            let pid_file = output_base.join(SERVER_PID_FILE);
            let text = match fs::read_to_string(&pid_file) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                text => text.map_err(E2eError::io(format!("reading {}", pid_file.display())))?,
            };
            let raw: i32 = text.trim().parse().map_err(|_| {
                E2eError::system(format!("{} holds no process id", pid_file.display()))
            })?;
            let pid = Pid::from_raw(raw).ok_or_else(|| {
                E2eError::system(format!("{} holds no process id", pid_file.display()))
            })?;
            let unsigned_pid = u32::try_from(raw).map_err(|_| {
                E2eError::system(format!("{} holds no process id", pid_file.display()))
            })?;
            if !process::arguments(unsigned_pid).is_some_and(|args| serves(&args, &output_base)) {
                continue;
            }
            match kill_process(pid, Signal::KILL) {
                Ok(()) | Err(Errno::SRCH) => {}
                Err(errno) => {
                    return Err(E2eError::io(format!("killing the Bazel server {raw}"))(
                        io::Error::from(errno),
                    ));
                }
            }
        }
        Ok(())
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

/// Whether a process's exact arguments identify the specified Bazel output base.
fn serves(arguments: &[OsString], output_base: &Path) -> bool {
    arguments.iter().any(|argument| {
        argument
            .as_bytes()
            .strip_prefix(b"--output_base=")
            .is_some_and(|value| value == output_base.as_os_str().as_bytes())
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    fn bazel(output_user_root: PathBuf) -> Bazel {
        Bazel::new(
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
            output_user_root,
        )
    }

    #[test]
    fn every_invocation_uses_the_remote_home_and_pinned_path() {
        let bazel = bazel(PathBuf::from("/output"));
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

    #[test]
    fn a_pid_is_killed_only_when_it_serves_the_output_base() {
        let base = Path::new("/tmp/x/base");
        assert!(
            serves(&["bazel".into(), "--output_base=/tmp/x/base".into()], base),
            "the exact output base argument must match"
        );
        assert!(
            !serves(&["bazel".into(), "--output_base=/tmp/x/base2".into()], base),
            "a longer output base must not match"
        );
        assert!(
            !serves(&["--output_base=/tmp/x/".into(), "base".into()], base),
            "a flag split across arguments must not match"
        );

        let spaced = Path::new("/tmp/x/my base");
        assert!(
            serves(&["--output_base=/tmp/x/my base".into()], spaced),
            "a single output base argument containing a space must match"
        );
        assert!(
            !serves(&["--output_base=/tmp/x/my".into(), "base".into()], spaced),
            "a space-separated output base across arguments must not match"
        );
    }

    #[test]
    fn a_gone_pid_serves_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let mut child = Command::new("true").spawn()?;
        let pid = child.id();
        child.wait()?;
        assert!(
            process::arguments(pid).is_none(),
            "a reaped process must have no arguments"
        );
        Ok(())
    }

    #[test]
    fn kill_servers_kills_the_server_each_output_base_records()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::process::ExitStatusExt;

        let root = tempfile::tempdir()?;
        let output_root = root
            .path()
            .join(format!("dir with spaces {}", "x".repeat(200)));
        let base = output_root.join("0123456789abcdef");
        let mut server = Command::new("sh")
            .args([
                "-c",
                "read -r line",
                &format!("--output_base={}", base.display()),
            ])
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        fs::create_dir_all(base.join("server"))?;
        fs::write(base.join(SERVER_PID_FILE), server.id().to_string())?;
        fs::create_dir_all(output_root.join("install"))?;
        bazel(output_root).kill_servers()?;
        drop(server.stdin.take());
        assert_eq!(
            server.wait()?.signal(),
            Some(Signal::KILL.as_raw()),
            "the server with a long, space-containing output base must be killed"
        );
        Ok(())
    }

    #[test]
    fn kill_servers_without_an_output_root_kills_nothing() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        bazel(root.path().join("absent")).kill_servers()?;
        Ok(())
    }
}
