use std::ffi::OsString;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::{Component, Path, PathBuf};

use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, NetworkAccess, Plugin,
    PluginContext, PortError, SandboxedAction,
};
use gang_types::ActionEnvironment;
use rustix::process::Pid;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::errors::BwrapError;
use crate::relay::Relay;
use crate::{BwrapConfig, EgressPrograms};

const RESERVED: [&str; 4] = ["/proc", "/dev", "/tmp", "/gang-reset"];

/// The most bytes read from bwrap's info report; the report is one small JSON object, well under this.
const INFO_LIMIT: u64 = 4096;

/// Runs actions under bubblewrap.
#[derive(Debug)]
pub struct Bwrap {
    /// The absolute `bwrap` program. A relative name is resolved by `gang worker` through the
    /// worker's `PATH` when it renders the sandbox file, never by `gang exec`; `build` rejects one
    /// that is still relative.
    pub(crate) program: PathBuf,
    /// The gang binary gang exec runs as; its `gang-reset` multicall link starts the action inside the sandbox.
    pub(crate) executable: PathBuf,
    /// The `bwrap` arguments that precede the action's command.
    pub(crate) prefix: Box<[OsString]>,
    /// Identity of the immutable rootfs selected at worker preparation.
    pub(crate) environment: ActionEnvironment,
    /// Absolute destinations of the immutable top-level entries.
    pub(crate) entries: Box<[PathBuf]>,
    /// Programs for isolated public egress, when configured.
    pub(crate) egress: Option<EgressPrograms>,
}

/// A spawned bubblewrap and the process identifiers used for signal relay.
pub(crate) struct StartedBwrap {
    pub child: tokio::process::Child,
    pub launcher: Option<Pid>,
    pub sandbox: Option<Pid>,
    info: Option<std::io::PipeReader>,
}

impl StartedBwrap {
    /// Reads bubblewrap's report while retaining ownership of its child.
    pub(crate) async fn read_info(&mut self) -> Result<(), BwrapError> {
        if let Some(reader) = self.info.take() {
            self.sandbox = sandbox_pid(reader).await?;
        }
        Ok(())
    }
}

impl Plugin for Bwrap {
    type Config = BwrapConfig;

    fn build(config: Self::Config, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let BwrapConfig {
            root,
            egress,
            program,
        } = config;

        if !program.is_absolute() {
            return Err(BwrapError::RelativeProgram(program).into());
        }

        let envelope = std::fs::canonicalize(root.as_path())
            .map_err(|error| BwrapError::Root(root.as_path().to_path_buf(), error))?;
        let identity_path = envelope.join("identity");
        let mut identity = std::fs::read_to_string(&identity_path)
            .map_err(|error| BwrapError::Root(identity_path.clone(), error))?;
        if identity.pop() != Some('\n') {
            return Err(BwrapError::RootIdentity(identity_path).into());
        }
        let environment = ActionEnvironment::try_from(identity)
            .ok()
            .filter(ActionEnvironment::is_rootfs)
            .ok_or(BwrapError::RootIdentity(identity_path))?;
        let tree = envelope.join("root");
        let listing =
            std::fs::read_dir(&tree).map_err(|error| BwrapError::Root(tree.clone(), error))?;
        let mut roots = Vec::new();
        for item in listing {
            let item = item.map_err(|error| BwrapError::Root(tree.clone(), error))?;
            roots.push(item);
        }
        roots.sort_by_key(std::fs::DirEntry::file_name);
        let mut prefix: Vec<OsString> = [
            "--unshare-all",
            "--unshare-user",
            "--uid",
            "1000",
            "--gid",
            "1000",
            "--die-with-parent",
            "--new-session",
            "--clearenv",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let mut entries = Vec::with_capacity(roots.len());
        for item in roots {
            let source = item.path();
            let destination = Path::new("/").join(item.file_name());
            if RESERVED.iter().any(|path| destination == Path::new(path)) {
                return Err(BwrapError::RootEntry(destination).into());
            }
            let kind = item
                .file_type()
                .map_err(|error| BwrapError::Root(source.clone(), error))?;
            if kind.is_dir() {
                prefix.push("--ro-bind".into());
                prefix.push(source.into_os_string());
                prefix.push(destination.as_os_str().to_owned());
            } else if kind.is_symlink() {
                let target = std::fs::read_link(&source)
                    .map_err(|error| BwrapError::Root(source.clone(), error))?;
                if target.is_absolute()
                    || target
                        .components()
                        .any(|c| matches!(c, Component::ParentDir))
                {
                    return Err(BwrapError::RootEntry(source).into());
                }
                prefix.push("--symlink".into());
                prefix.push(target.into_os_string());
                prefix.push(destination.as_os_str().to_owned());
            } else {
                return Err(BwrapError::RootEntry(source).into());
            }
            entries.push(destination);
        }
        prefix.extend(
            ["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
                .into_iter()
                .map(OsString::from),
        );
        validate_egress_paths(egress.as_ref())?;

        Ok(Self {
            program,
            executable: std::env::current_exe().map_err(BwrapError::Executable)?,
            prefix: prefix.into_boxed_slice(),
            environment,
            entries: entries.into_boxed_slice(),
            egress,
        })
    }
}

fn validate_egress_paths(egress: Option<&EgressPrograms>) -> Result<(), PortError> {
    if let Some(helpers) = egress
        && !helpers.program.is_absolute()
    {
        return Err(BwrapError::NetworkSetup(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "egress helper path {} is not absolute",
                helpers.program.display()
            ),
        ))
        .into());
    }
    Ok(())
}

/// Starts the action under `bwrap`; the returned `SandboxedAction` waits for it.
///
/// Bubblewrap reports an action killed by signal `n` as exit status `128 + n`, so that death is
/// reported as `Exited(128 + n)`; every other exit code passes through unchanged. A `bwrap` setup
/// failure happens before the action starts and is reported as its own exit status (exit 1), not
/// as an error from this port. `Signaled(n)` means `bwrap` itself was killed: a termination
/// relayed before the action exists ends `bwrap` with `SIGKILL`, reported as `Signaled(9)`.
///
/// For offline actions, `bwrap` remains in the action's original process group with termination
/// signals blocked. With public egress, the launcher, pasta, and outer `bwrap` instead run in the
/// worker's process group: pasta resets its signal mask and handles termination signals itself.
/// The caller stays in the original group and relays terminations to the action's own group once
/// it exists, or kills the launcher before. The sandbox's action resets its mask.
///
/// With limits, `bwrap` is started through the scope's launcher, so the sandbox and every process
/// in it run inside the scope.
impl Bwrap {
    pub(crate) fn spawn(
        &self,
        spec: &ActionExecutionSpec<'_>,
        network: Option<BorrowedFd<'_>>,
    ) -> Result<StartedBwrap, BwrapError> {
        let (reader, writer) = std::io::pipe().map_err(BwrapError::InfoPipe)?;
        if let Some(resolver) = network {
            rustix::io::fcntl_setfd(resolver, rustix::io::FdFlags::empty())
                .map_err(|errno| BwrapError::NetworkSetup(io::Error::from(errno)))?;
        }
        let mut command =
            tokio::process::Command::from(self.command(spec, writer.as_raw_fd(), network));
        command.kill_on_drop(true);
        if network.is_some() {
            command
                .process_group(gang_os::parent_process_group().map_err(BwrapError::NetworkSetup)?);
        }
        let blocked = gang_os::block_terminations().map_err(BwrapError::SignalMask)?;
        // These descriptors are inherited by bwrap (and by gang-join when a launcher is used).
        rustix::io::fcntl_setfd(&writer, rustix::io::FdFlags::empty())
            .map_err(|errno| BwrapError::InfoPipe(io::Error::from(errno)))?;
        let spawned = command.spawn();
        drop(blocked);
        drop(writer);
        let child = spawned.map_err(BwrapError::Run)?;
        let launcher = child.id().and_then(pid);
        Ok(StartedBwrap {
            child,
            launcher,
            sandbox: None,
            info: Some(reader),
        })
    }
}

impl ActionIsolationPort for Bwrap {
    fn environment(&self) -> &ActionEnvironment {
        &self.environment
    }

    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let action = spec.action_directory;
        if !action.is_absolute()
            || action
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            || self
                .entries
                .iter()
                .any(|entry| action.starts_with(entry) || entry.starts_with(action))
            || RESERVED.iter().any(|reserved| {
                let reserved = Path::new(reserved);
                action == reserved
                    || reserved.starts_with(action)
                    || (reserved != Path::new("/tmp") && action.starts_with(reserved))
            })
        {
            return Err(BwrapError::ActionDirectory(action.to_path_buf()).into());
        }
        let setup = async {
            let resolver: Option<std::fs::File> = match spec.network {
                NetworkAccess::Offline => None,
                NetworkAccess::PublicEgress {
                    policy,
                    controller_endpoints,
                } => {
                    if self.egress.is_none() {
                        return Err(BwrapError::NetworkNotConfigured.into());
                    }
                    #[cfg(target_os = "linux")]
                    {
                        let cgroup = spec.memory_cgroup.ok_or_else(|| {
                            BwrapError::NetworkSetup(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "public egress requires an action memory cgroup",
                            ))
                        })?;
                        crate::host_egress::verify_host_egress(
                            policy,
                            controller_endpoints,
                            cgroup,
                        )
                        .await
                        .map_err(BwrapError::NetworkSetup)?;
                        Some(resolver_file(policy).map_err(BwrapError::NetworkSetup)?)
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        let _ = (policy, controller_endpoints);
                        return Err(BwrapError::NetworkSetup(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "public egress requires Linux",
                        ))
                        .into());
                    }
                }
            };
            let mut started = self.spawn(spec, resolver.as_ref().map(AsFd::as_fd))?;
            drop(resolver);
            started.read_info().await?;
            Ok::<_, PortError>(started)
        };
        let started = match spec.network {
            NetworkAccess::PublicEgress { policy, .. } => {
                tokio::time::timeout(policy.setup_timeout.get(), setup)
                    .await
                    .map_err(|_| BwrapError::NetworkTimeout(policy.setup_timeout))??
            }
            NetworkAccess::Offline => setup.await?,
        };
        let StartedBwrap {
            mut child,
            launcher,
            sandbox,
            ..
        } = started;
        let wait = async move {
            let status = child.wait().await.map_err(BwrapError::Wait)?;
            ActionExecutionOutcome::try_from(status)
        };
        match (launcher, sandbox) {
            (Some(launcher), Some(sandbox)) => {
                let relay = Relay::new(launcher, sandbox);
                Ok(SandboxedAction::relayed(wait, move |termination| {
                    relay.relay(termination).map_err(PortError::from)
                }))
            }
            _ => Ok(SandboxedAction::new(wait)),
        }
    }
}

#[cfg(target_os = "linux")]
fn resolver_file(policy: &gang_types::PublicEgressPolicy) -> io::Result<std::fs::File> {
    use rustix::fs::{MemfdFlags, SealFlags, fcntl_add_seals, memfd_create};
    use std::io::{Seek, SeekFrom, Write};
    let fd = memfd_create(
        "gang-resolver",
        MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING,
    )?;
    let mut file = std::fs::File::from(fd);
    for address in &policy.dns_servers {
        writeln!(file, "nameserver {address}")?;
    }
    file.seek(SeekFrom::Start(0))?;
    fcntl_add_seals(
        &file,
        SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL,
    )?;
    Ok(file)
}

/// The report `bwrap` writes on its `--info-fd`; fields it does not name are ignored.
#[derive(Deserialize)]
struct Info {
    /// The host pid of the sandbox's first process, which bubblewrap makes a process group leader.
    #[serde(rename = "child-pid")]
    child_pid: u32,
}

/// Converts a reported process ID to a platform PID if it is representable and nonzero.
fn pid(raw: u32) -> Option<Pid> {
    i32::try_from(raw).ok().and_then(Pid::from_raw)
}

/// Reads the sandbox's first process from `bwrap`'s report, or `None` when `bwrap` failed before
/// it created the sandbox and so wrote nothing. `bwrap` writes one JSON object, and reading stops
/// at the end of that object or after `INFO_LIMIT` bytes. An incomplete object at either point is
/// an error.
///
/// # Errors
///
/// [`BwrapError::InfoPipe`] when the pipe cannot be registered for reading, [`BwrapError::Info`]
/// when a read fails or the report names an invalid pid, and [`BwrapError::InfoFormat`] when the
/// report is not the JSON object `bwrap` writes or ends before it is complete.
async fn sandbox_pid(reader: std::io::PipeReader) -> Result<Option<Pid>, BwrapError> {
    let mut receiver = tokio::net::unix::pipe::Receiver::from_owned_fd(OwnedFd::from(reader))
        .map_err(BwrapError::InfoPipe)?
        .take(INFO_LIMIT);
    let mut report = Vec::new();
    loop {
        let read = receiver
            .read_buf(&mut report)
            .await
            .map_err(BwrapError::Info)?;
        match serde_json::Deserializer::from_slice(&report)
            .into_iter::<Info>()
            .next()
        {
            Some(Ok(info)) => {
                return pid(info.child_pid).map(Some).ok_or_else(|| {
                    BwrapError::Info(io::Error::other("bwrap reported an invalid pid"))
                });
            }
            Some(Err(error)) if error.is_eof() && read != 0 => {}
            Some(Err(error)) => return Err(BwrapError::InfoFormat(error)),
            None if read != 0 => {}
            None if report.is_empty() => return Ok(None),
            None => {
                return Err(BwrapError::InfoFormat(
                    <serde_json::Error as serde::de::Error>::custom(
                        "bwrap's report holds no JSON object",
                    ),
                ));
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::ffi::OsString;
    #[cfg(target_os = "linux")]
    use std::fs::File;
    #[cfg(target_os = "linux")]
    use std::io::{BufRead, BufReader, Read};
    use std::path::{Path, PathBuf};
    #[cfg(target_os = "linux")]
    use std::time::Duration;

    use gang_spi::{
        ActionCommand, ActionExecutionSpec, ActionIsolationPort, NetworkAccess, Plugin,
        PluginContext, PortError,
    };
    #[cfg(target_os = "linux")]
    use gang_spi::{ActionExecutionOutcome, Termination};
    use gang_types::AbsolutePathBuf;
    #[cfg(target_os = "linux")]
    use rustix::process::Signal;
    #[cfg(target_os = "linux")]
    use tokio::time::timeout;

    use super::Bwrap;
    use crate::BwrapConfig;
    #[cfg(target_os = "linux")]
    use crate::errors::BwrapError;

    /// Bwrap's info write end is inherited by any child spawned while it is open, so these tests
    /// start children one at a time.
    pub(crate) static SPAWN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn context() -> PluginContext<'static> {
        PluginContext {
            config_path: Path::new("/"),
        }
    }

    pub(crate) fn config(directory: &Path) -> BwrapConfig {
        let envelope = directory.join("envelope");
        std::fs::create_dir_all(envelope.join("root/usr")).expect("create fixture root");
        std::fs::write(envelope.join("identity"), format!("{}\n", "a".repeat(64)))
            .expect("write fixture identity");
        BwrapConfig {
            root: AbsolutePathBuf::try_new(envelope).expect("fixture path is absolute"),
            egress: None,
            program: PathBuf::from("/usr/bin/bwrap"),
        }
    }

    #[test]
    fn rejects_missing_and_invalid_root_identity() {
        let directory = tempfile::tempdir().expect("fixture");
        let config = config(directory.path());
        let identity = config.root.join("identity");
        std::fs::remove_file(&identity).expect("remove identity");
        assert!(Bwrap::build(config.clone(), &context()).is_err());
        for value in [
            "",
            "host:local\n",
            "INVALID\n",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            std::fs::write(&identity, value).expect("write identity");
            assert!(
                Bwrap::build(config.clone(), &context()).is_err(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn rejects_reserved_or_unsupported_root_entries() {
        let directory = tempfile::tempdir().expect("fixture");
        let config = config(directory.path());
        let tree = config.root.join("root");
        for name in ["proc", "dev", "tmp", "gang-reset"] {
            std::fs::create_dir(tree.join(name)).expect("create reserved entry");
            assert!(Bwrap::build(config.clone(), &context()).is_err(), "{name}");
            std::fs::remove_dir(tree.join(name)).expect("remove entry");
        }
        std::fs::write(tree.join("file"), "not a directory").expect("create file");
        assert!(Bwrap::build(config.clone(), &context()).is_err());
        std::fs::remove_file(tree.join("file")).expect("remove file");
        std::os::unix::fs::symlink("/etc", tree.join("escape")).expect("create symlink");
        assert!(Bwrap::build(config, &context()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn action_directory_must_not_cover_protected_mounts()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let mut config = config(directory.path());
        config.program = fake_bwrap()?;
        let sandbox = Bwrap::build(config, &context())?;
        let argv = [OsString::from("exit"), OsString::from("0")];
        for path in [
            "/",
            "/tmp",
            "/usr",
            "/usr/bin",
            "/proc/self",
            "/dev/null",
            "/gang-reset",
        ] {
            let spec = ActionExecutionSpec {
                command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
                environment: &[],
                action_directory: Path::new(path),
                working_directory: Path::new(path),
                network: NetworkAccess::Offline,
                memory_cgroup: None,
                launcher: &[],
            };
            assert!(sandbox.start(&spec).await.is_err(), "{path}");
        }
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            environment: &[],
            action_directory: Path::new("/tmp/action"),
            working_directory: Path::new("/tmp/action"),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
            launcher: &[],
        };
        assert_eq!(
            sandbox.start(&spec).await?.wait().await?,
            ActionExecutionOutcome::Exited(0)
        );
        Ok(())
    }

    /// The bound on every blocking read and wait in these tests.
    #[cfg(target_os = "linux")]
    const LIMIT: Duration = Duration::from_mins(1);

    /// Returns the `fake_bwrap` test program that Bazel builds and exposes through `FAKE_BWRAP`.
    #[cfg(target_os = "linux")]
    fn fake_bwrap() -> Result<PathBuf, Box<dyn std::error::Error>> {
        let raw =
            std::env::var_os("FAKE_BWRAP").ok_or("FAKE_BWRAP must be set by the Bazel test_env")?;
        Ok(std::env::current_dir()?.join(raw))
    }

    /// Creates a FIFO at `path`, the channel the test program signals through.
    #[cfg(target_os = "linux")]
    fn make_fifo(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        rustix::fs::mknodat(
            rustix::fs::CWD,
            path,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            0,
        )?;
        Ok(())
    }

    /// Opens the record FIFO for reading, blocking until the test program opens it for writing.
    #[cfg(target_os = "linux")]
    async fn open_record(path: PathBuf) -> Result<BufReader<File>, Box<dyn std::error::Error>> {
        let file =
            timeout(LIMIT, tokio::task::spawn_blocking(move || File::open(path))).await???;
        Ok(BufReader::new(file))
    }

    /// Reads one line of the record on a blocking thread, bounded by `LIMIT`.
    #[cfg(target_os = "linux")]
    async fn next_line(
        mut record: BufReader<File>,
    ) -> Result<(BufReader<File>, String), Box<dyn std::error::Error>> {
        let read = timeout(
            LIMIT,
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                record.read_line(&mut line)?;
                Ok::<_, std::io::Error>((record, line))
            }),
        )
        .await???;
        Ok(read)
    }

    #[test]
    fn rejects_a_program_that_is_not_absolute() {
        let directory = tempfile::tempdir().expect("temporary directory");
        for program in ["bwrap", "./bwrap"] {
            let config = BwrapConfig {
                program: PathBuf::from(program),
                ..config(directory.path())
            };

            assert!(
                matches!(Bwrap::build(config, &context()), Err(PortError::Config(_))),
                "program {program:?} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_relative_egress_program() {
        let directory = tempfile::tempdir().expect("fixture");
        let config = BwrapConfig {
            egress: Some(crate::EgressPrograms {
                program: "pasta".into(),
            }),
            ..config(directory.path())
        };
        let error = Bwrap::build(config, &context()).expect_err("relative helper rejected");
        assert!(error.to_string().contains("egress"), "{error}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolver_contains_only_configured_servers_and_is_sealed()
    -> Result<(), Box<dyn std::error::Error>> {
        use rustix::fs::{SealFlags, fcntl_get_seals};
        use std::io::{Read, Write};
        let policy: gang_types::PublicEgressPolicy = toml::from_str(
            "protected_networks = ['10.0.0.0/8']\ndns_servers = ['1.1.1.1', '2606:4700:4700::1111']",
        )?;
        let mut file = super::resolver_file(&policy)?;
        assert!(
            fcntl_get_seals(&file)?
                .contains(SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL)
        );
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        assert_eq!(
            contents,
            "nameserver 1.1.1.1\nnameserver 2606:4700:4700::1111\n"
        );
        assert!(file.write_all(b"unexpected").is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn granted_action_requires_cgroup_before_spawn() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let config = BwrapConfig {
            egress: Some(crate::EgressPrograms {
                program: "/no/such/pasta".into(),
            }),
            ..config(directory.path())
        };
        let sandbox = Bwrap::build(config, &context())?;
        let argv = [OsString::from("/bin/true")];
        let policy: gang_types::PublicEgressPolicy =
            toml::from_str("protected_networks = ['10.0.0.0/8']\ndns_servers = ['1.1.1.1']")?;
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::PublicEgress {
                policy: &policy,
                controller_endpoints: &[],
            },
            memory_cgroup: None,
            launcher: &[],
        };
        let Err(error) = sandbox.start(&spec).await else {
            return Err("granted action without cgroup unexpectedly started".into());
        };
        assert!(error.to_string().contains("public egress"));
        Ok(())
    }

    /// The launcher inherits blocked signals and stays in the caller's group.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn launcher_inherits_mask_and_group() -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let record = directory.path().join("mask");
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("mask"), record.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };
        let mut action = bwrap.start(&spec).await?;
        assert!(matches!(
            action.wait().await?,
            ActionExecutionOutcome::Exited(0)
        ));
        let text = std::fs::read_to_string(record)?;
        let mut lines = text.lines();
        let mask = u64::from_str_radix(
            lines
                .next()
                .ok_or("missing mask")?
                .split_whitespace()
                .nth(1)
                .ok_or("missing mask value")?,
            16,
        )?;
        assert_ne!(mask & (1 << (u32::try_from(Signal::TERM.as_raw())? - 1)), 0);
        assert_ne!(mask & (1 << (u32::try_from(Signal::INT.as_raw())? - 1)), 0);
        assert_eq!(
            lines.next().ok_or("missing group")?,
            lines.next().ok_or("missing parent group")?
        );
        Ok(())
    }

    /// The fake `bwrap` reports through `/proc/self/fd`, so this test runs only on Linux.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_command_that_exits_with_a_status_reports_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let program = fake_bwrap()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("exit"), OsString::from("3")];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let outcome = bwrap.start(&spec).await?.wait().await?;

        assert_eq!(outcome, ActionExecutionOutcome::Exited(3));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn repeated_and_concurrent_starts_work() -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("exit"), OsString::from("0")];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };
        for _ in 0..2 {
            let (first, second) = timeout(LIMIT, async {
                tokio::join!(bwrap.start(&spec), bwrap.start(&spec))
            })
            .await?;
            let (mut first, mut second) = (first?, second?);
            let (first, second) =
                timeout(LIMIT, async { tokio::join!(first.wait(), second.wait()) }).await?;
            assert_eq!(first?, ActionExecutionOutcome::Exited(0));
            assert_eq!(second?, ActionExecutionOutcome::Exited(0));
        }
        Ok(())
    }

    /// Starts the fake with a report and checks that the report is rejected as malformed.
    #[cfg(target_os = "linux")]
    async fn assert_bad_report(payload: &str) -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("report"), OsString::from(payload)];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };
        let result = bwrap.start(&spec).await;
        assert!(matches!(
            result,
            Err(PortError::Backend(error))
                if matches!(error.downcast_ref::<BwrapError>(), Some(BwrapError::InfoFormat(_)))
        ));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn negative_child_pid_is_a_format_error() -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        assert_bad_report("{\"child-pid\": -5}").await
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn oversized_incomplete_report_is_a_format_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        assert_bad_report(&" ".repeat(5000)).await
    }

    #[tokio::test]
    async fn a_missing_program_is_a_backend_failure() -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let program = PathBuf::from("/nonexistent/bwrap");
        let bwrap = Bwrap::build(
            BwrapConfig {
                program,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("/bin/true")];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let result = bwrap.start(&spec).await;

        assert!(matches!(result, Err(PortError::Backend(_))));
        Ok(())
    }

    /// Bubblewrap is Linux-only, so this kill-on-drop proof runs only where the sandbox runs.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dropping_the_running_action_kills_it() -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let fifo = directory.path().join("started");
        make_fifo(&fifo)?;
        let program = fake_bwrap()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("hold"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let running = bwrap.start(&spec).await?;
        let opener = fifo.clone();
        let mut reader = timeout(
            LIMIT,
            tokio::task::spawn_blocking(move || File::open(opener)),
        )
        .await???;
        drop(running);
        let read = timeout(
            LIMIT,
            tokio::task::spawn_blocking(move || {
                let mut byte = [0_u8; 1];
                reader.read(&mut byte)
            }),
        )
        .await???;

        assert_eq!(read, 0);
        Ok(())
    }

    /// Relays a termination to the action once the sandbox runs it. The action records the
    /// termination itself and bwrap is not signalled, so bwrap exits with the action's status.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_termination_reaches_the_action_once_the_sandbox_runs_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let fifo = directory.path().join("record");
        make_fifo(&fifo)?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("sandbox"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let mut running = bwrap.start(&spec).await?;
        let record = open_record(fifo).await?;
        let (record, line) = next_line(record).await?;
        assert_eq!(line, "ready\n");
        running.relay(Termination::Terminate)?;
        let (_record, line) = next_line(record).await?;
        assert_eq!(line, "terminated\n");
        let outcome = timeout(LIMIT, running.wait()).await??;
        assert_eq!(outcome, ActionExecutionOutcome::Exited(0));
        Ok(())
    }

    /// A termination that arrives before the action exists goes to bwrap, which the signal ends.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_termination_before_the_action_exists_ends_bwrap()
    -> Result<(), Box<dyn std::error::Error>> {
        let _spawn = SPAWN.lock().await;
        let directory = tempfile::tempdir()?;
        let fifo = directory.path().join("record");
        make_fifo(&fifo)?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config(directory.path())
            },
            &context(),
        )?;
        let argv = [OsString::from("starting"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            action_directory: directory.path(),
            working_directory: directory.path(),
            network: NetworkAccess::Offline,
            memory_cgroup: None,
        };

        let mut running = bwrap.start(&spec).await?;
        let record = open_record(fifo).await?;
        let (_record, line) = next_line(record).await?;
        assert_eq!(line, "idle\n");
        running.relay(Termination::Terminate)?;
        let outcome = timeout(LIMIT, running.wait()).await??;
        assert_eq!(outcome, ActionExecutionOutcome::Signaled(9));
        Ok(())
    }
}
