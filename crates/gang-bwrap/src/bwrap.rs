use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::PathBuf;

use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, Plugin, PluginContext,
    PortError, SandboxedAction,
};
use rustix::process::Pid;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::BwrapConfig;
use crate::errors::BwrapError;
use crate::relay::Relay;

/// The arguments every sandbox gets before its tmpfs, which the configured tmp path fills in.
const FIXED_ARGUMENTS: [&str; 7] = [
    "--unshare-all",
    "--die-with-parent",
    "--new-session",
    "--proc",
    "/proc",
    "--dev",
    "/dev",
];

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
    /// Whether the configuration permits the action to share the host network.
    pub(crate) permit_network: bool,
}

impl Plugin for Bwrap {
    type Config = BwrapConfig;

    fn build(config: Self::Config, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let BwrapConfig {
            readonly_bind_paths,
            permit_network_property,
            program,
            tmp_path,
        } = config;

        if !program.is_absolute() {
            return Err(BwrapError::RelativeProgram(program).into());
        }

        let mut prefix =
            Vec::with_capacity(FIXED_ARGUMENTS.len() + 3 + 3 * readonly_bind_paths.len());
        prefix.extend(FIXED_ARGUMENTS.iter().copied().map(OsString::from));
        prefix.push(OsString::from("--tmpfs"));
        prefix.push(tmp_path.into_path_buf().into_os_string());
        prefix.push(OsString::from("--clearenv"));
        for path in readonly_bind_paths {
            let source = path.into_path_buf().into_os_string();
            prefix.push(OsString::from("--ro-bind"));
            prefix.push(source.clone());
            prefix.push(source);
        }

        Ok(Self {
            program,
            executable: std::env::current_exe().map_err(BwrapError::Executable)?,
            prefix: prefix.into_boxed_slice(),
            permit_network: permit_network_property,
        })
    }
}

/// Starts the action under `bwrap`; the returned `SandboxedAction` waits for it.
///
/// Bubblewrap reports an action killed by signal `n` as exit status `128 + n`, so that death is
/// reported as `Exited(128 + n)`; every other exit code passes through unchanged. A `bwrap` setup
/// failure happens before the action starts and is reported as its own exit status (exit 1), not
/// as an error from this port. `Signaled(n)` means `bwrap` itself was killed: a termination
/// relayed before the action exists ends `bwrap` with `SIGKILL`, reported as `Signaled(9)`.
///
/// `bwrap` remains in the action's original process group with termination signals blocked, and
/// the action runs in a process group of its own, so the returned action is relayed: its caller
/// stays in the original group, receives the terminations sent to it, and relays them to the
/// action's group once the action exists, or kills `bwrap` before. The sandbox's action resets its
/// mask.
///
/// With limits, `bwrap` is started through the scope's launcher, so the sandbox and every process
/// in it run inside the scope.
impl ActionIsolationPort for Bwrap {
    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let (reader, writer) = std::io::pipe().map_err(BwrapError::InfoPipe)?;
        // bwrap inherits the write end under the same number, so it must not be close-on-exec.
        // `gang exec` spawns nothing else while the descriptor is open; tests serialise spawns.
        rustix::io::fcntl_setfd(&writer, rustix::io::FdFlags::empty())
            .map_err(|errno| BwrapError::InfoPipe(io::Error::from(errno)))?;
        let mut command = tokio::process::Command::from(self.command(spec, writer.as_raw_fd()));
        command.kill_on_drop(true);
        // NativeLink signals every member group. Bubblewrap has no termination handler, so
        // block these signals across spawn; gang-reset clears the inherited mask for the action.
        let blocked = gang_os::block_terminations().map_err(BwrapError::SignalMask)?;
        let spawned = command.spawn();
        drop(blocked);
        let mut child = spawned.map_err(BwrapError::Run)?;
        drop(writer);
        let launcher = child.id().and_then(pid);
        let sandbox = sandbox_pid(reader).await?;
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
        ActionCommand, ActionExecutionSpec, ActionIsolationPort, NetworkRequest, Plugin,
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

    fn config() -> BwrapConfig {
        BwrapConfig {
            readonly_bind_paths: Vec::new(),
            permit_network_property: false,
            program: PathBuf::from("/usr/bin/bwrap"),
            tmp_path: AbsolutePathBuf::try_new("/tmp").expect("`/tmp` is absolute"),
        }
    }

    /// The bound on every blocking read and wait in these tests.
    #[cfg(target_os = "linux")]
    const LIMIT: Duration = Duration::from_secs(60);

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
        for program in ["bwrap", "./bwrap"] {
            let config = BwrapConfig {
                program: PathBuf::from(program),
                ..config()
            };

            assert!(
                matches!(Bwrap::build(config, &context()), Err(PortError::Config(_))),
                "program {program:?} must be rejected"
            );
        }
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("mask"), record.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("exit"), OsString::from("3")];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
        };

        let outcome = bwrap.start(&spec).await?.wait().await?;

        assert_eq!(outcome, ActionExecutionOutcome::Exited(3));
        Ok(())
    }

    /// Starts the fake with a report and checks that the report is rejected as malformed.
    #[cfg(target_os = "linux")]
    async fn assert_bad_report(payload: &str) -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let bwrap = Bwrap::build(
            BwrapConfig {
                program: fake_bwrap()?,
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("report"), OsString::from(payload)];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("/bin/true")];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("hold"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("sandbox"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
                ..config()
            },
            &context(),
        )?;
        let argv = [OsString::from("starting"), fifo.clone().into_os_string()];
        let spec = ActionExecutionSpec {
            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
            launcher: &[],
            environment: &[],
            working_directory: directory.path(),
            network: NetworkRequest::NotRequested,
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
