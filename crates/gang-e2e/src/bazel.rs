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

    /// Records JSON build events, all action results, and remote RPCs at the supplied paths.
    #[must_use]
    pub(crate) fn with_build_logs(&self, bep: &Path, rpc: &Path) -> Self {
        let mut driver = self.clone();
        driver.settings.bazel_flags.extend([
            format!("--build_event_json_file={}", bep.display()),
            "--build_event_publish_all_actions".to_owned(),
            format!("--remote_grpc_log={}", rpc.display()),
        ]);
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
    /// Runs the rootfs test without accepting cached action or test results.
    pub(crate) fn rootfs_test(&self, remote: &Remote, bep: &Path) -> Result<BuildOutcome> {
        let mut args = flags::build_args(&self.settings, &self.action_tools, remote, &[], None);
        "test".clone_into(&mut args[0]);
        args.extend([
            "--noremote_accept_cached".to_owned(),
            "--cache_test_results=no".to_owned(),
            format!("--build_event_json_file={}", bep.display()),
            "--build_event_publish_all_actions".to_owned(),
            "//:rootfs_sh_test".to_owned(),
        ]);
        self.build_command(self.invocation(remote, args), None)
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
        mut command: Command,
        abort: Option<oneshot::Receiver<String>>,
    ) -> Result<BuildOutcome> {
        self.offline_repositories(&mut command);
        let output = match abort {
            Some(abort) => process::run_until(command, None, self.settings.build_timeout, abort)?,
            None => process::run(command, None, self.settings.build_timeout)?,
        };
        Ok(BuildOutcome::new(output.code, output.combined()))
    }

    fn offline_repositories(&self, command: &mut Command) {
        if let Some(cache) = self
            .action_tools
            .egress
            .as_ref()
            .and_then(|tools| tools.repository_cache.as_deref())
        {
            command
                .arg(path_flag("--repository_cache=", cache))
                .arg("--lockfile_mode=error");
        }
    }

    /// Resolves the staged sample online before its isolated fixture starts.
    ///
    /// # Errors
    ///
    /// Returns an error if resolution fails, exceeds its deadline, or server cleanup fails.
    #[cfg(target_os = "linux")]
    pub(crate) fn prefetch(&self, cache: &Path) -> Result<()> {
        let command = self.prefetch_command(&prefetch_args(&self.output_user_root, cache));
        let output = process::run(command, None, self.settings.build_timeout);
        let shutdown = self.prefetch_command(&[
            path_flag("--output_user_root=", &self.output_user_root),
            "--nohome_rc".into(),
            "--nosystem_rc".into(),
            "shutdown".into(),
        ]);
        let _ = process::run(shutdown, None, self.settings.command_timeout);
        let cleanup = self.kill_servers();
        let output = output?;
        if !output.succeeded() {
            return Err(E2eError::Command {
                program: self.program.display().to_string(),
                outcome: format!("prefetch exited with {:?}", output.code),
                output: output.combined(),
            });
        }
        cleanup
    }

    #[cfg(target_os = "linux")]
    fn prefetch_command(&self, arguments: &[OsString]) -> Command {
        let mut command = Command::new(&self.program);
        command
            .current_dir(&self.workspace)
            .args(arguments)
            .env("PATH", &self.action_tools.path);
        scrub_environment(&mut command);
        client_home(&mut command, &self.workspace);
        command
    }

    /// The pinned tool path used by Bazel clients.
    pub(crate) fn action_path(&self) -> &str {
        &self.action_tools.path
    }

    /// Starts the server with `info output_base`, without loading targets or running actions.
    /// The caller owns shutdown, including when preparation fails after starting a server.
    pub(crate) fn prepare(&self, remote: &Remote) -> Result<()> {
        let output = self.execute(self.prepare_command(remote))?;
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

    fn prepare_command(&self, remote: &Remote) -> Command {
        self.invocation(remote, vec!["info".to_owned(), "output_base".to_owned()])
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

/// The online analysis command; its explicit mode overrides the sample rc's disabled lockfile.
#[cfg(any(target_os = "linux", test))]
fn prefetch_args(output_user_root: &Path, cache: &Path) -> [OsString; 8] {
    [
        path_flag("--output_user_root=", output_user_root),
        "--nohome_rc".into(),
        "--nosystem_rc".into(),
        "build".into(),
        "--nobuild".into(),
        "//...".into(),
        path_flag("--repository_cache=", cache),
        "--lockfile_mode=update".into(),
    ]
}

fn path_flag(prefix: &str, path: &Path) -> OsString {
    let mut flag = OsString::from(prefix);
    flag.push(path);
    flag
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
                bwrap: None,
                glibc_tool: None,
                action_rootfs: None,
                egress: None,
            },
            PathBuf::from("/workspace"),
            output_user_root,
        )
    }

    #[test]
    fn build_logs_preserve_the_driver_and_record_both_builds() {
        let mut bazel = bazel(PathBuf::from("/old output"));
        bazel.program = PathBuf::from("/old/bin/bazel");
        bazel.workspace = PathBuf::from("/old workspace");
        bazel.settings.build_timeout = std::time::Duration::from_secs(37);
        bazel
            .settings
            .bazel_startup_flags
            .push("--max_idle_secs=29".to_owned());
        bazel
            .settings
            .bazel_flags
            .push("--remote_timeout=37".to_owned());
        let remote = Remote::Gang {
            host: "example.test".to_owned(),
            bazelrc: PathBuf::from("/old client/bazelrc"),
            helper: PathBuf::from("/old client/bin/gang"),
            home: PathBuf::from("/old client/home"),
        };
        let probes = [Probe::ControllerLoss0];
        let original = bazel.invocation(
            &remote,
            flags::build_args(&bazel.settings, &bazel.action_tools, &remote, &probes, None),
        );
        let original_args: Vec<_> = original.get_args().map(OsString::from).collect();
        let insertion = original_args
            .iter()
            .position(|arg| arg == "--remote_timeout=37")
            .expect("the configured build flag is present")
            + 1;
        for name in ["initial", "post"] {
            let bep = PathBuf::from(format!("/build logs/upgrade-{name}.bep"));
            let rpc = PathBuf::from(format!("/build logs/upgrade-{name}.rpc"));
            let driver = bazel.with_build_logs(&bep, &rpc);
            let command = driver.invocation(
                &remote,
                flags::build_args(
                    &driver.settings,
                    &driver.action_tools,
                    &remote,
                    &probes,
                    None,
                ),
            );
            let mut expected = original_args.clone();
            expected.splice(
                insertion..insertion,
                [
                    OsString::from(format!("--build_event_json_file={}", bep.display())),
                    OsString::from("--build_event_publish_all_actions"),
                    OsString::from(format!("--remote_grpc_log={}", rpc.display())),
                ],
            );
            assert_eq!(command.get_args().collect::<Vec<_>>(), expected);
            assert_eq!(command.get_program(), original.get_program());
            assert_eq!(command.get_current_dir(), original.get_current_dir());
            assert!(command.get_envs().eq(original.get_envs()));
            assert_eq!(driver.settings.build_timeout, bazel.settings.build_timeout);
        }
        assert_eq!(bazel.settings.bazel_flags, ["--remote_timeout=37"]);
    }

    #[test]
    fn fixture_builds_override_lockfile_mode_with_the_prefetched_cache() {
        let mut bazel = bazel(PathBuf::from("/output"));
        bazel.action_tools.egress = Some(crate::settings::EgressTools {
            slirp4netns: "/tools/slirp4netns".into(),
            nft: "/tools/nft".into(),
            nsenter: "/tools/nsenter".into(),
            ip: "/tools/ip".into(),
            unshare: "/tools/unshare".into(),
            probe: "/tools/probe".into(),
            peer: "/tools/peer".into(),
            policy: None,
            targets: None,
            repository_cache: Some("/repository cache".into()),
        });
        let mut command = Command::new("/tools/bazel");
        command.args(["build", "--lockfile_mode=off", "//:probe"]);
        bazel.offline_repositories(&mut command);
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            [
                "build",
                "--lockfile_mode=off",
                "//:probe",
                "--repository_cache=/repository cache",
                "--lockfile_mode=error",
            ]
        );
        if let Some(tools) = &mut bazel.action_tools.egress {
            tools.repository_cache = None;
        }
        let mut command = Command::new("/tools/bazel");
        command.args(["build", "--lockfile_mode=off", "//:probe"]);
        bazel.offline_repositories(&mut command);
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["build", "--lockfile_mode=off", "//:probe"]
        );
    }

    #[test]
    fn ordinary_build_arguments_remain_unchanged() {
        let bazel = bazel(PathBuf::from("/output"));
        let mut command = Command::new("/tools/bazel");
        command.args(["build", "--lockfile_mode=off", "//:probe"]);
        bazel.offline_repositories(&mut command);
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["build", "--lockfile_mode=off", "//:probe"]
        );
    }

    #[test]
    fn prefetch_arguments_analyze_every_sample_target_and_update_the_lockfile() {
        assert_eq!(
            prefetch_args(
                Path::new("/prefetch output"),
                Path::new("/repository cache")
            ),
            [
                "--output_user_root=/prefetch output",
                "--nohome_rc",
                "--nosystem_rc",
                "build",
                "--nobuild",
                "//...",
                "--repository_cache=/repository cache",
                "--lockfile_mode=update",
            ]
            .map(OsString::from)
        );
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
    fn preparation_starts_only_the_server_in_the_selected_output_root() {
        let driver =
            bazel(PathBuf::from("/output")).with_output_user_root(PathBuf::from("/demand-output"));
        let remote = Remote::Plain {
            bazelrc: PathBuf::from("/plain/rc"),
            home: PathBuf::from("/plain/home"),
        };
        let command = driver.prepare_command(&remote);
        let arguments: Vec<_> = command.get_args().collect();
        assert_eq!(command.get_program(), OsStr::new("/tools/bazel"));
        assert_eq!(command.get_current_dir(), Some(Path::new("/workspace")));
        assert_eq!(
            arguments,
            [
                "--output_user_root=/demand-output",
                "--nohome_rc",
                "--nosystem_rc",
                "--noworkspace_rc",
                "--bazelrc=/plain/rc",
                "info",
                "output_base",
            ]
            .map(OsStr::new),
        );
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
        use std::time::Duration;

        let root = tempfile::tempdir()?;
        let output_root = root
            .path()
            .join(format!("dir with spaces {}", "x".repeat(200)));
        let base = output_root.join("0123456789abcdef");
        // Pinned Bash supplies a real, non-exiting process with an arbitrary Bazel-style argv.
        let shell = std::fs::canonicalize(
            std::env::var_os("GANG_E2E_TEST_BASH").ok_or("Bazel supplies pinned Bash")?,
        )?;
        let mut server = process::Supervised::spawn(
            Command::new(shell).args([
                "-c",
                "printf 'ready\\n'; kill -STOP $$",
                &format!("--output_base={}", base.display()),
            ]),
            &root.path().join("server.log"),
        )?;
        // Spawn can return before argv is visible. Publish the fixture's pid only after its
        // readiness event makes startup ordering deterministic.
        assert!(
            server
                .await_announcements(Duration::from_secs(30), |line| line == "ready")?
                .is_some(),
            "the server must announce readiness before its pid is recorded"
        );
        fs::create_dir_all(base.join("server"))?;
        fs::write(base.join(SERVER_PID_FILE), server.pid().to_string())?;
        fs::create_dir_all(output_root.join("install"))?;
        bazel(output_root).kill_servers()?;
        assert_eq!(
            server
                .wait_within(Duration::from_secs(30), "the Bazel server to be killed")?
                .signal(),
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
