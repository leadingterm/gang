//! The `bwrap` command line of one action.

use std::ffi::OsStr;
use std::os::fd::RawFd;
use std::process::Command;

use gang_spi::{ActionExecutionSpec, NetworkRequest};

use crate::bwrap::Bwrap;

/// The bound multicall executable inside the sandbox.
const RESET_PATH: &str = "/gang-reset";

impl Bwrap {
    /// Builds the `bwrap` invocation that runs `spec`.
    ///
    /// The sandbox root starts empty, so the only host paths visible inside it are the prefix's
    /// read-only binds and the working directory; the content store is never mapped. The working
    /// directory is bound read-write and becomes the current directory. `--share-net` must follow
    /// `--unshare-all`, which the prefix contains, so it re-enables the network only when both the
    /// plugin permits it and the action requests it. The `bwrap` process itself starts with an
    /// empty environment, so a host variable such as `LD_PRELOAD` never reaches it; inside the
    /// sandbox the environment is replaced wholesale with `--clearenv` and `--setenv`, so nothing
    /// is inherited.
    ///
    /// `info_fd` is the descriptor `bwrap` writes its `--info-fd` report to. It is passed through
    /// unchanged, so the caller keeps it open in the spawned process; `bwrap` writes the report
    /// and closes it.
    ///
    /// With limits, the launcher starts `bwrap` itself, so the whole sandbox, `bwrap` included,
    /// starts inside the limits scope.
    #[must_use]
    pub(crate) fn command(&self, spec: &ActionExecutionSpec<'_>, info_fd: RawFd) -> Command {
        let working_directory = spec.working_directory.as_os_str();
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
            working_directory,
            working_directory,
            OsStr::new("--chdir"),
            working_directory,
        ]);
        if self.permit_network && spec.network == NetworkRequest::Requested {
            command.arg("--share-net");
        }
        for (name, value) in spec.environment {
            command.arg("--setenv").arg(name).arg(value);
        }
        command
            .arg("--ro-bind")
            .arg(&self.executable)
            .arg(RESET_PATH)
            .arg("--")
            .arg(RESET_PATH)
            .arg(spec.command.program())
            .args(spec.command.args());
        command
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::os::fd::RawFd;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use gang_spi::{ActionCommand, ActionExecutionSpec, NetworkRequest, Plugin, PluginContext};
    use gang_types::AbsolutePathBuf;

    use crate::{Bwrap, BwrapConfig};

    const READONLY: [&str; 2] = ["/nix/store", "/usr"];

    /// The descriptor the tests report the sandbox on; the command line only names it.
    const INFO_FD: RawFd = 7;

    fn abs(path: &str) -> AbsolutePathBuf {
        AbsolutePathBuf::try_new(path).expect("absolute")
    }

    fn bwrap(permit_network_property: bool) -> Bwrap {
        Bwrap::build(
            BwrapConfig {
                readonly_bind_paths: READONLY.iter().copied().map(abs).collect(),
                permit_network_property,
                program: PathBuf::from("/usr/bin/bwrap"),
                tmp_path: abs("/tmp"),
            },
            &PluginContext {
                config_path: Path::new("sandbox.toml"),
            },
        )
        .expect("bwrap plugin builds from its config")
    }

    fn command(
        bwrap: &Bwrap,
        working_directory: &Path,
        network: NetworkRequest,
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
            working_directory,
            network,
        };
        Ok(bwrap.command(&spec, INFO_FD))
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
    fn full_command_line_for_unshared_network() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap(false);

        let command = command(&bwrap, Path::new("/work/a"), NetworkRequest::NotRequested)?;

        assert_eq!(command.get_program(), "/usr/bin/bwrap");
        assert_eq!(
            args(&command),
            [
                "--unshare-all",
                "--die-with-parent",
                "--new-session",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
                "/tmp",
                "--clearenv",
                "--ro-bind",
                "/nix/store",
                "/nix/store",
                "--ro-bind",
                "/usr",
                "/usr",
                "--info-fd",
                "7",
                "--bind",
                "/work/a",
                "/work/a",
                "--chdir",
                "/work/a",
                "--setenv",
                "A",
                "1",
                "--setenv",
                "B",
                "two words",
                "--ro-bind",
                &bwrap.executable.to_string_lossy(),
                "/gang-reset",
                "--",
                "/gang-reset",
                "/bin/sh",
                "-c",
                "echo hi there",
            ]
        );
        Ok(())
    }

    #[test]
    fn share_net_only_when_permitted_and_requested() -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            (false, NetworkRequest::NotRequested, false),
            (false, NetworkRequest::Requested, false),
            (true, NetworkRequest::NotRequested, false),
            (true, NetworkRequest::Requested, true),
        ];

        for (permit, request, expected) in cases {
            let bwrap = bwrap(permit);
            let args = args(&command(&bwrap, Path::new("/work/a"), request)?);

            let occurrences = args.iter().filter(|arg| *arg == "--share-net").count();
            assert_eq!(
                occurrences,
                usize::from(expected),
                "permit={permit}, request={request:?}: {args:?}"
            );
            if expected {
                let share = position(&args, "--share-net");
                assert!(position(&args, "--unshare-all") < share, "{args:?}");
                assert!(share < position(&args, "--"), "{args:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn tmpfs_tmp_precedes_working_directory_bind() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap(false);

        let args = args(&command(
            &bwrap,
            Path::new("/tmp/work"),
            NetworkRequest::NotRequested,
        )?);

        let tmpfs = position(&args, "--tmpfs");
        assert_eq!(args[tmpfs + 1], "/tmp");
        assert!(tmpfs < position(&args, "--bind"), "{args:?}");
        Ok(())
    }

    #[test]
    fn uses_configured_program_and_tmp_path() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = Bwrap::build(
            BwrapConfig {
                readonly_bind_paths: Vec::new(),
                permit_network_property: false,
                program: PathBuf::from("/custom/bwrap"),
                tmp_path: abs("/scratch"),
            },
            &PluginContext {
                config_path: Path::new("sandbox.toml"),
            },
        )
        .expect("bwrap plugin builds from its config");

        let command = command(&bwrap, Path::new("/work/a"), NetworkRequest::NotRequested)?;

        assert_eq!(command.get_program(), "/custom/bwrap");
        let args = args(&command);
        let tmpfs = position(&args, "--tmpfs");
        assert_eq!(args[tmpfs + 1], "/scratch");
        assert!(tmpfs < position(&args, "--bind"), "{args:?}");
        Ok(())
    }

    #[test]
    fn the_launcher_wraps_bwrap() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap(false);
        let argv = [OsString::from("/bin/true")];
        let launcher = [
            OsString::from("/usr/bin/gang-join"),
            OsString::from("/scope"),
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &launcher,
            environment: &[],
            working_directory: Path::new("/work/a"),
            network: NetworkRequest::NotRequested,
        };

        let command = bwrap.command(&spec, INFO_FD);

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
                readonly_bind_paths: Vec::new(),
                permit_network_property: false,
                program: std::env::current_dir()?.join(fake),
                tmp_path: abs("/tmp"),
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
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
        };
        // The bwrap stand-in is a Bazel-built binary: no host program runs.
        let status = bwrap
            .command(&spec, INFO_FD)
            .status()
            .expect("the bwrap stand-in runs");
        assert!(status.success(), "the bwrap stand-in exits successfully");

        let recorded =
            std::fs::read_to_string(&output).expect("the stand-in wrote its environment");
        assert!(
            recorded.is_empty(),
            "the bwrap process environment is not empty: {recorded}"
        );
        Ok(())
    }
}
