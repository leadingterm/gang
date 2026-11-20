//! The `bwrap` command line of one action.

use std::ffi::OsStr;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::process::Command;

use crate::bwrap::Bwrap;
use gang_spi::ActionExecutionSpec;

/// The bound multicall executable inside the sandbox.
const RESET_PATH: &str = "/gang-reset";

impl Bwrap {
    /// Builds the `bwrap` invocation that runs `spec`.
    ///
    /// The sandbox root contains the immutable rootfs entries and the action directory;
    /// the content store and other actions' directories are never mapped.
    /// The action directory is bound read-write, so every input resolves,
    /// including those outside the working directory (`../input`, relative symlinks); the working
    /// directory inside it becomes the current directory. The host network is never shared.
    /// The `bwrap` process itself starts with an empty environment. For gated actions the
    /// trusted reset process also starts empty; the environment is delivered after setup.
    ///
    /// `info_fd` is the descriptor `bwrap` writes its `--info-fd` report to. It is passed through
    /// unchanged, so the caller keeps it open in the spawned process; `bwrap` writes the report
    /// and closes it.
    ///
    /// With limits, the launcher starts `bwrap` itself, so the whole sandbox, `bwrap` included,
    /// starts inside the limits scope.
    #[must_use]
    pub(crate) fn command(
        &self,
        spec: &ActionExecutionSpec<'_>,
        info_fd: RawFd,
        network: Option<(BorrowedFd<'_>, BorrowedFd<'_>)>,
    ) -> Command {
        let action_directory = spec.action_directory.as_os_str();
        let mut command = match spec.launcher.split_first() {
            Some((launcher, launcher_arguments)) => {
                let mut command = Command::new(launcher);
                command.args(launcher_arguments).arg(&self.program);
                command
            }
            None => Command::new(&self.program),
        };
        command.env_clear();
        command.args(self.prefix.iter());
        command.arg("--info-fd").arg(info_fd.to_string());
        command.args([
            OsStr::new("--bind"),
            action_directory,
            action_directory,
            OsStr::new("--chdir"),
            spec.working_directory.as_os_str(),
        ]);
        if let Some((_gate, resolver)) = network {
            command
                .arg("--ro-bind-data")
                .arg(resolver.as_raw_fd().to_string())
                .arg("/etc/resolv.conf");
        } else {
            for (name, value) in spec.environment {
                command.arg("--setenv").arg(name).arg(value);
            }
        }
        command
            .arg("--ro-bind")
            .arg(&self.executable)
            .arg(RESET_PATH)
            .arg("--remount-ro")
            .arg("/")
            .arg("--")
            .arg(RESET_PATH);
        if let Some((gate, _resolver)) = network {
            command
                .arg("--execution-gate-fd")
                .arg(gate.as_raw_fd().to_string());
        }
        command
            .arg("--")
            .arg(spec.command.program())
            .args(spec.command.args());
        command
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    #[cfg(target_os = "linux")]
    use std::os::fd::{AsFd, AsRawFd};
    use std::os::fd::{BorrowedFd, RawFd};
    use std::path::Path;
    use std::process::Command;

    #[cfg(target_os = "linux")]
    use gang_spi::ActionIsolationPort;
    use gang_spi::{ActionCommand, ActionExecutionSpec, NetworkAccess, Plugin, PluginContext};

    use crate::Bwrap;
    use crate::BwrapConfig;
    use crate::bwrap::tests::config;

    /// The descriptor the tests report the sandbox on; the command line only names it.
    const INFO_FD: RawFd = 7;

    fn bwrap() -> Bwrap {
        let directory = tempfile::tempdir().expect("fixture");
        Bwrap::build(
            config(directory.path()),
            &PluginContext {
                config_path: Path::new("sandbox.toml"),
            },
        )
        .expect("fixture builds")
    }

    fn command(
        bwrap: &Bwrap,
        action_directory: &Path,
        working_directory: &Path,
        network: NetworkAccess<'_>,
        setup: Option<(BorrowedFd<'_>, BorrowedFd<'_>)>,
    ) -> Result<Command, Box<dyn std::error::Error>> {
        let argv = [
            OsString::from("/bin/sh"),
            OsString::from("-c"),
            OsString::from("echo hi there"),
        ];
        let environment = [
            (OsString::from("A"), OsString::from("1")),
            (OsString::from("B"), OsString::from("two words")),
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &environment,
            action_directory,
            working_directory,
            network,
        };
        Ok(bwrap.command(&spec, INFO_FD, setup))
    }

    fn args(command: &Command) -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn position(args: &[String], needle: &str) -> usize {
        args.iter()
            .position(|arg| arg == needle)
            .unwrap_or_else(|| panic!("`{needle}` is in the command line: {args:?}"))
    }

    #[test]
    fn reset_path_matches_multicall_name() {
        assert_eq!(
            Path::new(super::RESET_PATH).file_name(),
            Some(OsStr::new(gang_spi::RESET_LINK_NAME))
        );
    }

    #[test]
    fn root_entries_and_mount_order_are_explicit() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().expect("fixture");
        let config = config(directory.path());
        symlink("usr/bin", config.root.join("root/bin")).expect("fixture symlink");
        let bwrap = Bwrap::build(
            config,
            &PluginContext {
                config_path: Path::new("/"),
            },
        )
        .expect("build");
        let command = command(
            &bwrap,
            Path::new("/tmp/action"),
            Path::new("/tmp/action"),
            NetworkAccess::Offline,
            None,
        )
        .expect("command");
        let args = args(&command);
        assert!(
            args.windows(3)
                .any(|a| a == ["--symlink", "usr/bin", "/bin"])
        );
        assert!(
            args.windows(3)
                .any(|a| a[0] == "--ro-bind" && a[2] == "/usr")
        );
        assert!(args.windows(2).any(|a| a == ["--uid", "1000"]));
        assert!(args.windows(2).any(|a| a == ["--gid", "1000"]));
        assert!(args.windows(3).any(|a| a == ["--setenv", "B", "two words"]));
        assert!(position(&args, "/usr") < position(&args, "--proc"));
        assert!(position(&args, "--tmpfs") < position(&args, "--bind"));
        assert!(position(&args, "--bind") < position(&args, "--remount-ro"));
        assert!(position(&args, "--remount-ro") < position(&args, "--"));
        assert!(!args.iter().any(|a| a == "--ro-bind-data"));
    }

    #[test]
    fn rootfs_identity_files_are_not_replaced() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let config = config(directory.path());
        std::fs::create_dir(config.root.join("root/etc"))?;
        for file in ["passwd", "group", "nsswitch.conf"] {
            std::fs::write(config.root.join("root/etc").join(file), file)?;
        }
        let bwrap = Bwrap::build(
            config,
            &PluginContext {
                config_path: Path::new("/"),
            },
        )?;
        let args = args(&command(
            &bwrap,
            Path::new("/work"),
            Path::new("/work"),
            NetworkAccess::Offline,
            None,
        )?);
        assert!(
            args.windows(3)
                .any(|a| a[0] == "--ro-bind" && a[2] == "/etc")
        );
        assert!(!args.iter().any(|a| a == "--ro-bind-data"));
        Ok(())
    }

    #[test]
    fn full_command_line_for_unshared_network() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let bwrap = Bwrap::build(
            config(directory.path()),
            &PluginContext {
                config_path: Path::new("/"),
            },
        )?;
        let command = command(
            &bwrap,
            Path::new("/work/a"),
            Path::new("/work/a/sub"),
            NetworkAccess::Offline,
            None,
        )?;
        assert_eq!(command.get_program(), "/usr/bin/bwrap");
        let root = std::fs::canonicalize(directory.path().join("envelope/root/usr"))?;
        assert_eq!(
            args(&command),
            [
                "--unshare-all",
                "--unshare-user",
                "--uid",
                "1000",
                "--gid",
                "1000",
                "--die-with-parent",
                "--new-session",
                "--clearenv",
                "--ro-bind",
                &root.to_string_lossy(),
                "/usr",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
                "/tmp",
                "--info-fd",
                "7",
                "--bind",
                "/work/a",
                "/work/a",
                "--chdir",
                "/work/a/sub",
                "--setenv",
                "A",
                "1",
                "--setenv",
                "B",
                "two words",
                "--ro-bind",
                &bwrap.executable.to_string_lossy(),
                "/gang-reset",
                "--remount-ro",
                "/",
                "--",
                "/gang-reset",
                "--",
                "/bin/sh",
                "-c",
                "echo hi there",
            ]
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn action_policy_never_shares_host_network() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap();
        let (_parent, gate) = gang_os::execution_gate_pair()?;
        let resolver = tempfile::tempfile()?;
        let gate_fd = gate.as_raw_fd().to_string();
        let resolver_fd = resolver.as_raw_fd().to_string();
        let policy: gang_types::PublicEgressPolicy =
            toml::from_str("protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"1.1.1.1\"]")?;
        let args = args(&command(
            &bwrap,
            Path::new("/work/a"),
            Path::new("/work/a"),
            NetworkAccess::PublicEgress {
                policy: &policy,
                controller_endpoints: &[],
            },
            Some((gate.as_fd(), resolver.as_fd())),
        )?);
        assert!(args.iter().any(|arg| arg == "--unshare-all"), "{args:?}");
        assert!(!args.iter().any(|arg| arg == "--share-net"), "{args:?}");
        assert!(!args.iter().any(|arg| arg == "--setenv"), "{args:?}");
        let resolver = position(&args, "/etc/resolv.conf");
        assert_eq!(
            &args[resolver - 2..=resolver],
            [
                "--ro-bind-data".into(),
                resolver_fd,
                "/etc/resolv.conf".into()
            ]
        );
        let gate = position(&args, "--execution-gate-fd");
        assert_eq!(
            &args[gate + 1..gate + 4],
            [gate_fd, "--".into(), "/bin/sh".into()]
        );
        Ok(())
    }

    #[test]
    fn tmpfs_tmp_precedes_action_directory_bind() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap();

        let args = args(&command(
            &bwrap,
            Path::new("/tmp/work"),
            Path::new("/tmp/work"),
            NetworkAccess::Offline,
            None,
        )?);

        let tmpfs = position(&args, "--tmpfs");
        assert_eq!(args[tmpfs + 1], "/tmp");
        assert!(tmpfs < position(&args, "--bind"), "{args:?}");
        Ok(())
    }

    #[test]
    fn uses_configured_program() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: "/custom/bwrap".into(),
                ..config(directory.path())
            },
            &PluginContext {
                config_path: Path::new("sandbox.toml"),
            },
        )?;
        let command = command(
            &bwrap,
            Path::new("/work/a"),
            Path::new("/work/a"),
            NetworkAccess::Offline,
            None,
        )?;
        assert_eq!(command.get_program(), "/custom/bwrap");
        Ok(())
    }

    #[test]
    fn the_launcher_wraps_bwrap() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap();
        let argv = [OsString::from("/bin/true")];
        let launcher = [
            OsString::from("/usr/bin/gang-join"),
            OsString::from("/scope"),
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &launcher,
            environment: &[],
            action_directory: Path::new("/work/a"),
            working_directory: Path::new("/work/a"),
            network: NetworkAccess::Offline,
        };

        let command = bwrap.command(&spec, INFO_FD, None);

        assert_eq!(command.get_program(), launcher[0].as_os_str());
        let arguments: Vec<&OsStr> = command.get_args().collect();
        assert_eq!(
            &arguments[..2],
            &[launcher[1].as_os_str(), OsStr::new("/usr/bin/bwrap")]
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bwrap_process_starts_with_an_empty_environment()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = crate::bwrap::tests::SPAWN.lock().await;
        assert!(
            std::env::vars_os().next().is_some(),
            "the test process has at least one variable"
        );

        let fake =
            std::env::var_os("FAKE_BWRAP").ok_or("FAKE_BWRAP must be set by the Bazel test_env")?;
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let output = directory.path().join("environment.txt");
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: std::env::current_dir()?.join(fake),
                ..config(directory.path())
            },
            &PluginContext {
                config_path: Path::new("sandbox.toml"),
            },
        )
        .expect("bwrap plugin builds from its config");
        let argv = [
            OsString::from("environment"),
            output.clone().into_os_string(),
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
        };
        // The bwrap stand-in is a Bazel-built binary: no host program runs.
        let outcome = bwrap.start(&spec).await?.wait().await?;
        assert_eq!(outcome, gang_spi::ActionExecutionOutcome::Exited(0));

        let recorded =
            std::fs::read_to_string(&output).expect("the stand-in wrote its environment");
        assert!(
            recorded.is_empty(),
            "the bwrap process environment is not empty: {recorded}"
        );
        Ok(())
    }
}
