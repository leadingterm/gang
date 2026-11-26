//! The `bwrap` command line of one action.

use std::ffi::OsStr;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::path::{Component, Path};
use std::process::Command;

use crate::bwrap::Bwrap;
use crate::errors::BwrapError;
use gang_spi::{ActionExecutionSpec, MAX_RO_BINDS};

/// The bound multicall executable inside the sandbox.
const RESET_PATH: &str = "/gang-reset";

fn has_parent_dir(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

/// Validates `spec.read_only_trees`: at most [`MAX_RO_BINDS`] entries, each with an absolute
/// source without `..` and a target strictly inside `spec.action_directory` without `..`.
pub(crate) fn validate_read_only_trees(spec: &ActionExecutionSpec<'_>) -> Result<(), BwrapError> {
    if spec.read_only_trees.len() > MAX_RO_BINDS {
        return Err(BwrapError::TooManyReadOnlyTrees {
            count: spec.read_only_trees.len(),
            max: MAX_RO_BINDS,
        });
    }
    for tree in spec.read_only_trees {
        let valid_source = tree.source.is_absolute() && !has_parent_dir(tree.source);
        let valid_target = tree.target.is_absolute()
            && !has_parent_dir(tree.target)
            && tree.target != spec.action_directory
            && tree.target.starts_with(spec.action_directory);
        if !valid_source || !valid_target {
            return Err(BwrapError::InvalidReadOnlyTree {
                host_path: tree.source.to_path_buf(),
                target: tree.target.to_path_buf(),
            });
        }
    }
    Ok(())
}

impl Bwrap {
    /// Builds the `bwrap` invocation that runs `spec`.
    ///
    /// The sandbox root contains the immutable rootfs entries and the action directory;
    /// the content store and other actions' directories are never mapped.
    /// The action directory is bound read-write, so every input resolves,
    /// including those outside the working directory (`../input`, relative symlinks); the working
    /// directory inside it becomes the current directory. Read-only input trees are bound
    /// read-only after the work bind. Granted actions share only pasta's
    /// network namespace. The outer processes start with an empty environment.
    ///
    /// `info_fd` is the descriptor `bwrap` writes its `--info-fd` report to. It is passed through
    /// unchanged, so the caller keeps it open in the spawned process; `bwrap` writes the report
    /// and closes it.
    ///
    /// With limits, the launcher starts `bwrap` itself, so the whole sandbox, `bwrap` included,
    /// starts inside the limits scope.
    ///
    /// # Errors
    ///
    /// Returns [`BwrapError::TooManyReadOnlyTrees`] if `spec.read_only_trees` exceeds
    /// [`MAX_RO_BINDS`], or [`BwrapError::InvalidReadOnlyTree`] if an entry has an invalid path.
    pub(crate) fn command(
        &self,
        spec: &ActionExecutionSpec<'_>,
        info_fd: RawFd,
        network: Option<BorrowedFd<'_>>,
    ) -> Result<Command, BwrapError> {
        validate_read_only_trees(spec)?;
        let action_directory = spec.action_directory.as_os_str();
        let outer = network
            .and(self.egress.as_ref())
            .map_or(&self.program, |egress| &egress.program);
        let mut command = match spec.launcher.split_first() {
            Some((launcher, launcher_arguments)) => {
                let mut command = Command::new(launcher);
                command.args(launcher_arguments).arg(outer);
                command
            }
            None => Command::new(outer),
        };
        command.env_clear();
        if network.is_some() {
            command.args([
                "--foreground",
                "--config-net",
                "--no-map-gw",
                "--dns",
                "none",
                "--search",
                "none",
                "-t",
                "none",
                "-u",
                "none",
                "-T",
                "none",
                "-U",
                "none",
                "--no-pidns",
                "--pass-fds",
            ]);
            command.arg(&self.program);
        }
        command.args(self.prefix.iter());
        if network.is_some() {
            command.arg("--share-net");
        }
        command.arg("--info-fd").arg(info_fd.to_string());
        command.args([
            OsStr::new("--bind"),
            action_directory,
            action_directory,
            OsStr::new("--chdir"),
            spec.working_directory.as_os_str(),
        ]);
        for tree in spec.read_only_trees {
            command.arg("--ro-bind").arg(tree.source).arg(tree.target);
        }
        if let Some(resolver) = network {
            command
                .arg("--ro-bind-data")
                .arg(resolver.as_raw_fd().to_string())
                .arg("/etc/resolv.conf");
        }
        for (name, value) in spec.environment {
            command.arg("--setenv").arg(name).arg(value);
        }
        command
            .arg("--ro-bind")
            .arg(&self.executable)
            .arg(RESET_PATH)
            .arg("--remount-ro")
            .arg("/")
            .arg("--")
            .arg(RESET_PATH);
        command
            .arg("--")
            .arg(spec.command.program())
            .args(spec.command.args());
        Ok(command)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    #[cfg(target_os = "linux")]
    use std::os::fd::{AsFd, AsRawFd};
    use std::os::fd::{BorrowedFd, RawFd};
    use std::path::{Path, PathBuf};
    use std::process::Command;

    #[cfg(target_os = "linux")]
    use gang_spi::ActionIsolationPort;
    use gang_spi::{
        ActionCommand, ActionExecutionSpec, MAX_RO_BINDS, NetworkAccess, Plugin, PluginContext,
        ReadOnlyTree,
    };

    use crate::Bwrap;
    use crate::BwrapConfig;
    use crate::bwrap::tests::config;
    use crate::errors::BwrapError;

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
        setup: Option<BorrowedFd<'_>>,
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
            read_only_trees: &[],
            memory_cgroup: None,
            network,
        };
        Ok(bwrap.command(&spec, INFO_FD, setup)?)
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
    fn action_policy_uses_pasta_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let mut bwrap = bwrap();
        bwrap.egress = Some(crate::EgressPrograms {
            program: "/usr/bin/pasta".into(),
        });
        let resolver = tempfile::tempfile()?;
        let resolver_fd = resolver.as_raw_fd().to_string();
        let policy: gang_types::PublicEgressPolicy =
            toml::from_str("protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"1.1.1.1\"]")?;
        let command = command(
            &bwrap,
            Path::new("/work/a"),
            Path::new("/work/a"),
            NetworkAccess::PublicEgress {
                policy: &policy,
                controller_endpoints: &[],
            },
            Some(resolver.as_fd()),
        )?;
        assert_eq!(command.get_program(), "/usr/bin/pasta");
        let args = args(&command);
        assert_eq!(
            &args[..18],
            [
                "--foreground",
                "--config-net",
                "--no-map-gw",
                "--dns",
                "none",
                "--search",
                "none",
                "-t",
                "none",
                "-u",
                "none",
                "-T",
                "none",
                "-U",
                "none",
                "--no-pidns",
                "--pass-fds",
                "/usr/bin/bwrap",
            ]
        );
        assert!(
            position(&args, "--unshare-all") < position(&args, "--share-net"),
            "{args:?}"
        );
        assert!(args.iter().any(|arg| arg == "--setenv"), "{args:?}");
        let resolver = position(&args, "/etc/resolv.conf");
        assert_eq!(
            &args[resolver - 2..=resolver],
            [
                "--ro-bind-data".into(),
                resolver_fd,
                "/etc/resolv.conf".into()
            ]
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
            read_only_trees: &[],
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let command = bwrap.command(&spec, INFO_FD, None)?;

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
            read_only_trees: &[],
            network: NetworkAccess::Offline,
            memory_cgroup: None,
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

    #[test]
    fn emits_ro_bind_for_read_only_trees_after_work_bind() -> Result<(), Box<dyn std::error::Error>>
    {
        let bwrap = bwrap();
        let argv = [OsString::from("/bin/true")];
        let trees = [
            ReadOnlyTree {
                source: Path::new("/fast-cas/trees/aaa"),
                target: Path::new("/work/a/work/external/toolchain"),
            },
            ReadOnlyTree {
                source: Path::new("/fast-cas/trees/bbb"),
                target: Path::new("/work/a/work/external/sdk"),
            },
        ];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: Path::new("/work/a"),
            working_directory: Path::new("/work/a/work"),
            read_only_trees: &trees,
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let command_args = args(&bwrap.command(&spec, INFO_FD, None)?);
        let work_bind = position(&command_args, "--bind");
        let first_ro = command_args
            .windows(3)
            .position(|window| {
                window
                    == [
                        "--ro-bind",
                        "/fast-cas/trees/aaa",
                        "/work/a/work/external/toolchain",
                    ]
            })
            .ok_or("missing first --ro-bind")?;
        let second_ro = command_args
            .windows(3)
            .position(|window| {
                window
                    == [
                        "--ro-bind",
                        "/fast-cas/trees/bbb",
                        "/work/a/work/external/sdk",
                    ]
            })
            .ok_or("missing second --ro-bind")?;
        let remount = position(&command_args, "--remount-ro");

        assert!(work_bind < first_ro && first_ro < second_ro && second_ro < remount);
        Ok(())
    }

    #[test]
    fn accepts_max_ro_binds_and_rejects_above_limit() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap();
        let argv = [OsString::from("/bin/true")];
        let pairs: Vec<_> = (0..=MAX_RO_BINDS)
            .map(|index| {
                (
                    PathBuf::from(format!("/fast-cas/trees/{index}")),
                    PathBuf::from(format!("/work/a/work/tree-{index}")),
                )
            })
            .collect();
        let refs: Vec<_> = pairs
            .iter()
            .map(|(source, target)| ReadOnlyTree { source, target })
            .collect();

        let at_limit = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: Path::new("/work/a"),
            working_directory: Path::new("/work/a/work"),
            read_only_trees: &refs[..MAX_RO_BINDS],
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };
        assert!(bwrap.command(&at_limit, INFO_FD, None).is_ok());

        let over_limit = ActionExecutionSpec {
            read_only_trees: &refs,
            ..at_limit
        };
        let error = bwrap
            .command(&over_limit, INFO_FD, None)
            .expect_err("more than MAX_RO_BINDS must be rejected");
        assert!(matches!(
            error,
            BwrapError::TooManyReadOnlyTrees {
                count,
                max: MAX_RO_BINDS,
            } if count == MAX_RO_BINDS + 1
        ));
        Ok(())
    }

    #[test]
    fn rejects_invalid_read_only_tree_paths() -> Result<(), Box<dyn std::error::Error>> {
        let bwrap = bwrap();
        let argv = [OsString::from("/bin/true")];
        let bad_cases = [
            ("/fast-cas/tree", "/work/a"),
            ("/fast-cas/tree", "/work/ab/sub"),
            ("/fast-cas/tree", "/etc/passwd"),
            ("/fast-cas/tree", "work/sub"),
            ("/fast-cas/tree", "/work/a/../b"),
            ("relative/tree", "/work/a/sub"),
            ("/fast-cas/../escape", "/work/a/sub"),
        ];
        for (source, target) in bad_cases {
            let trees = [ReadOnlyTree {
                source: Path::new(source),
                target: Path::new(target),
            }];
            let spec = ActionExecutionSpec {
                command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
                launcher: &[],
                environment: &[],
                action_directory: Path::new("/work/a"),
                working_directory: Path::new("/work/a"),
                read_only_trees: &trees,
                network: NetworkAccess::Offline,
                memory_cgroup: None,
            };
            assert!(
                matches!(
                    bwrap.command(&spec, INFO_FD, None),
                    Err(BwrapError::InvalidReadOnlyTree { .. })
                ),
                "expected InvalidReadOnlyTree for ({source}, {target})"
            );
        }
        Ok(())
    }
}
