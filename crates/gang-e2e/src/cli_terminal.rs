//! The real terminal of the cli scenario: the `gang` binary under test run with a deadline in a
//! placement of its own, on configurations derived from the checked-in `configs/gang.local.toml`.
//! Also the deadline runner the checked-in-configs scenario starts servers with.

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use gang_config::Document;
use gang_main::plugins::{Plugins, SandboxKey};
use gang_os::ProcessGroup;
use gang_types::Url;

use crate::Ran;
use crate::checked_in::retarget;
use crate::cli::{Call, Colour, Local, Refused, Terminal};
use crate::errors::{E2eError, Result};
use crate::fake_bazel::FAKE_BAZEL_ENV;
use crate::mirror::follow;
use crate::observer::{tail, worker_logs};
use crate::placement::Placement;
use crate::process;
use crate::sse::{self, EventStream as _};
use crate::state_api::StateApi;
use crate::suite::Invocation;
use crate::system::Sandbox;

/// How a command run by [`run_until`] finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Finished {
    /// It printed a standard output line that ended the wait: its lines through that one. Its
    /// process group was killed and it was reaped.
    Printed(Vec<String>),
    /// It exited first.
    Exited(Ran),
}

/// Runs `command` until it prints a standard output line that `ends`, exits, or `deadline` passes.
/// The inherited build environment is scrubbed, stdin is closed, stdout and stderr are piped, and
/// the command leads a process group of its own.
///
/// # Errors
///
/// [`E2eError::Io`] when it cannot be started; [`E2eError::Timeout`] once `deadline` passed, after
/// its process group was killed.
pub(crate) fn run_until(
    command: &mut Command,
    deadline: Duration,
    mut ends: impl FnMut(&str) -> bool,
) -> Result<Finished> {
    process::scrub_environment(command);
    let program = command.get_program().to_string_lossy().into_owned();
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
        .map_err(E2eError::io(format!("starting {program}")))?;
    let pid = child.id();
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        kill_group(pid);
        let _ = child.wait();
        return Err(E2eError::system("the command's output pipes are missing"));
    };

    let (line_tx, line_rx) = mpsc::channel::<Option<String>>();
    thread::Builder::new()
        .name("e2e-cli-stdout".to_owned())
        .spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut raw = Vec::new();
            loop {
                raw.clear();
                match reader.read_until(b'\n', &mut raw) {
                    Ok(0) | Err(_) => {
                        let _ = line_tx.send(None);
                        return;
                    }
                    Ok(_) => {
                        let line = String::from_utf8_lossy(&raw)
                            .trim_end_matches(['\n', '\r'])
                            .to_owned();
                        if line_tx.send(Some(line)).is_err() {
                            return;
                        }
                    }
                }
            }
        })
        .map_err(E2eError::io("starting the stdout reader"))?;

    let (error_tx, error_rx) = mpsc::channel::<String>();
    thread::Builder::new()
        .name("e2e-cli-stderr".to_owned())
        .spawn(move || {
            let mut stderr = stderr;
            let mut bytes = Vec::new();
            let _ = stderr.read_to_end(&mut bytes);
            let _ = error_tx.send(String::from_utf8_lossy(&bytes).into_owned());
        })
        .map_err(E2eError::io("starting the stderr reader"))?;

    let end = Instant::now() + deadline;
    let mut lines = Vec::new();
    loop {
        match line_rx.recv_timeout(end.saturating_duration_since(Instant::now())) {
            Ok(Some(line)) => {
                let done = ends(&line);
                lines.push(line);
                if done {
                    kill_group(pid);
                    let _ = child.wait();
                    return Ok(Finished::Printed(lines));
                }
            }
            Ok(None) | Err(RecvTimeoutError::Disconnected) => {
                let status = child
                    .wait()
                    .map_err(E2eError::io(format!("waiting for {program}")))?;
                let remaining = end.saturating_duration_since(Instant::now());
                let stderr = match error_rx.recv_timeout(remaining) {
                    Ok(stderr) => stderr,
                    Err(RecvTimeoutError::Timeout) => {
                        kill_group(pid);
                        return Err(timed_out(&program, deadline));
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(E2eError::system("the stderr reader ended without a result"));
                    }
                };
                // Each line ends with a newline, which is how `lines.join("\n")` plus a trailing
                // newline reads back.
                let stdout = lines.iter().fold(String::new(), |mut text, line| {
                    text.push_str(line);
                    text.push('\n');
                    text
                });
                return Ok(Finished::Exited(Ran {
                    code: status.code(),
                    stdout,
                    stderr,
                }));
            }
            Err(RecvTimeoutError::Timeout) => {
                kill_group(pid);
                let _ = child.wait();
                return Err(timed_out(&program, deadline));
            }
        }
    }
}

/// Kills the process group led by `pid`. A group already gone is fine: the command may have
/// exited on its own between the check that started the kill and the signal.
fn kill_group(pid: u32) {
    if let Some(group) = ProcessGroup::from_pid(pid) {
        let _ = group.kill();
    }
}

/// The timeout of a command run by [`run_until`] that did not finish within `deadline`.
fn timed_out(program: &str, deadline: Duration) -> E2eError {
    E2eError::timeout(format!("{program} to finish"), deadline)
}

/// The checked-in configuration both local deployments derive from.
const LOCAL_CONFIG: &str = "gang.local.toml";

/// The cli scenario's terminal: the `gang` binary under test, its placement, and the
/// configurations each [`Call`] names.
pub(crate) struct LocalTerminal {
    gang: PathBuf,
    placement: Placement,
    pinned: PathBuf,
    insecure: PathBuf,
    unparsable: PathBuf,
    public_listen: PathBuf,
    deadline: Duration,
    /// How long connecting to the state stream and its response headers may take.
    handshake: Duration,
    /// How many trailing bytes of the worker logs a failure reason quotes.
    log_tail_bytes: usize,
}

impl LocalTerminal {
    /// Creates the placement of the cli scenario and writes its four configurations into it: the
    /// pinned and the public-listen configurations derived from the checked-in `gang.local.toml`,
    /// the insecure one with its first fleet keeping one worker, and an unparsable one.
    ///
    /// # Errors
    ///
    /// When the placement cannot be created, the checked-in configuration cannot be read or
    /// parsed, or the configurations cannot be rendered or written.
    pub(crate) fn create(invocation: &Invocation) -> Result<Self> {
        let placement = Placement::create(invocation.settings.scratch_root.as_deref())?;
        let path = invocation.configs.join(LOCAL_CONFIG);
        let text = fs::read_to_string(&path)
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        let parse = || {
            Document::<Plugins>::parse(&text)
                .map_err(E2eError::parse("the checked-in gang.local.toml"))
        };

        let pinned_text = retarget(parse()?, &placement)?
            .to_toml()
            .map_err(E2eError::system_from("rendering the pinned configuration"))?;
        let bwrap = if Sandbox::native() == Sandbox::Bwrap {
            Some(invocation.action_tools.bwrap_program()?)
        } else {
            None
        };
        let insecure_text = kept_worker(&pinned_text, bwrap)?;
        let mut open = retarget(parse()?, &placement)?;
        open.listen = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
        let public_text = open.to_toml().map_err(E2eError::system_from(
            "rendering the public-listen configuration",
        ))?;
        let unparsable_text = "listen = \"not an address\"\n";

        let root = placement.root();
        let pinned = write_under(root, "pinned.toml", &pinned_text)?;
        let insecure = write_under(root, "insecure.toml", &insecure_text)?;
        let public_listen = write_under(root, "public-listen.toml", &public_text)?;
        let unparsable = write_under(root, "unparsable.toml", unparsable_text)?;

        Ok(Self {
            gang: invocation.gang.clone(),
            placement,
            pinned,
            insecure,
            unparsable,
            public_listen,
            deadline: invocation.settings.history_timeout,
            handshake: invocation.settings.handshake_timeout,
            log_tail_bytes: invocation.settings.log_tail_bytes,
        })
    }

    /// The configuration of the deployment `local`.
    fn config(&self, local: Local) -> &Path {
        match local {
            Local::Pinned => &self.pinned,
            Local::Insecure => &self.insecure,
        }
    }

    /// A `gang` command in the placement, with its home set to the placement's home and the colour
    /// variables removed.
    fn command(&self) -> Command {
        let home = self.placement.home();
        let mut command = Command::new(&self.gang);
        command
            .current_dir(self.placement.root())
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &home)
            .env_remove("NO_COLOR")
            .env_remove("CLICOLOR")
            .env_remove("CLICOLOR_FORCE")
            .env_remove("TERM");
        command
    }
}

/// `text` with its first fleet keeping one worker under the native sandbox.
fn kept_worker(text: &str, bwrap: Option<&Path>) -> Result<String> {
    let mut document =
        Document::<Plugins>::parse(text).map_err(E2eError::parse("the pinned configuration"))?;
    let first = document
        .fleets
        .first_mut()
        .ok_or_else(|| E2eError::system("the pinned configuration has no [[fleets]] entry"))?;
    first.min_instances = 1;
    first.sandbox = match Sandbox::native() {
        Sandbox::Bwrap => SandboxKey::Bwrap,
        Sandbox::Noop => SandboxKey::Noop,
    };
    if let Some(program) = bwrap {
        let sandbox = document.sandbox.bwrap.as_mut().ok_or_else(|| {
            E2eError::system("the pinned configuration has no [sandbox.bwrap] section")
        })?;
        sandbox.program = program.to_path_buf();
    }
    document.to_toml().map_err(E2eError::system_from(
        "rendering the insecure configuration",
    ))
}

/// Writes `text` to `name` under `root` and returns the path written.
fn write_under(root: &Path, name: &str, text: &str) -> Result<PathBuf> {
    let path = root.join(name);
    fs::write(&path, text).map_err(E2eError::io(format!("writing {}", path.display())))?;
    Ok(path)
}

/// Adds `-q` before the subcommand when `quiet`.
fn quiet_if(command: &mut Command, quiet: bool) {
    if quiet {
        command.arg("-q");
    }
}

impl Terminal for LocalTerminal {
    fn run(&self, call: &Call) -> Result<Ran> {
        let mut command = self.command();
        match call {
            Call::RevokeWithoutTarget => {
                command.arg("revoke");
            }
            Call::UnknownSubcommand => {
                command.arg("bogus");
            }
            Call::DoctorWithoutServer { quiet } => {
                quiet_if(&mut command, *quiet);
                command.arg("doctor");
            }
            Call::Server(refused) => {
                let config = match refused {
                    Refused::Unparsable => &self.unparsable,
                    Refused::PublicListenWithoutEndpoint => &self.public_listen,
                };
                command.args(["server", "--config"]).arg(config);
            }
            Call::Up(local) => {
                command
                    .args(["local", "up", "--config"])
                    .arg(self.config(*local));
                if *local == Local::Insecure {
                    command.arg("--insecure");
                }
            }
            Call::Down {
                local,
                quiet,
                colour,
            } => {
                quiet_if(&mut command, *quiet);
                command
                    .args(["local", "down", "--config"])
                    .arg(self.config(*local));
                match colour {
                    Colour::Plain => {}
                    Colour::Forced => {
                        command.env("CLICOLOR_FORCE", "1");
                    }
                    Colour::ForcedNoColor => {
                        command.env("CLICOLOR_FORCE", "1").env("NO_COLOR", "1");
                    }
                }
            }
            Call::Bazelrc { server } => {
                command.args(["bazelrc", "--server"]).arg(server.as_str());
            }
            Call::Doctor { server, executor } => {
                let bazel =
                    std::env::current_exe().map_err(E2eError::io("locating the harness binary"))?;
                command
                    .args(["doctor", "--server"])
                    .arg(server.as_str())
                    .arg("--bazel")
                    .arg(bazel)
                    .arg("--")
                    .arg(format!("--remote_executor={executor}"))
                    .env(FAKE_BAZEL_ENV, "1");
            }
            Call::Worker {
                endpoint,
                worker_endpoint,
            } => {
                command.args(["worker", "--config"]).arg(&self.pinned);
                if let Some(value) = endpoint {
                    command.args(["--endpoint", value.as_str()]);
                }
                if let Some(value) = worker_endpoint {
                    command.args(["--worker-endpoint", value.as_str()]);
                }
            }
        }
        process::run(command, None, self.deadline)
    }

    fn server_log(&self, path: &Path) -> Result<String> {
        fs::read_to_string(path).map_err(E2eError::io(format!("reading {}", path.display())))
    }

    fn worker_connects(&self, public: &Url) -> Result<bool> {
        let mut stream = StateApi::anonymous(public, None, self.handshake)?.open_stream()?;
        if stream.status() != sse::STREAM_OK {
            return Err(E2eError::system(format!(
                "the state stream answered {}",
                stream.status()
            )));
        }
        match sse::await_frames(&mut stream, self.deadline, &mut |frames| {
            follow(frames)
                .state
                .is_some_and(|state| !state.workers.is_empty())
        }) {
            Ok(_) => Ok(true),
            Err(E2eError::Timeout { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn worker_log_tail(&self) -> Result<String> {
        Ok(tail(
            worker_logs(&self.placement.state_dir())?.as_bytes(),
            self.log_tail_bytes,
        ))
    }

    fn worker_commands(&self) -> Result<Vec<Vec<String>>> {
        Ok(worker_commands_for(
            &process::snapshot(),
            &self.placement.state_dir(),
        ))
    }
}

/// Commands for workers launched from the local server's generated configuration.
fn worker_commands_for(entries: &[process::ProcessEntry], state_dir: &Path) -> Vec<Vec<String>> {
    let config = state_dir.join("server").join("gang.toml");
    process::workers_of(entries, &config)
        .into_iter()
        .map(|entry| entry.args.clone())
        .collect()
}

/// Stops whatever server and workers this scenario left running; failures are ignored, this runs
/// on the way out whatever the outcome.
impl Drop for LocalTerminal {
    fn drop(&mut self) {
        for local in [Local::Insecure, Local::Pinned] {
            let _ = self.run(&Call::Down {
                local,
                quiet: true,
                colour: Colour::Plain,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::process::ProcessEntry;

    use super::worker_commands_for;

    #[test]
    fn worker_commands_require_the_exact_generated_config_path() {
        let entries = [
            ProcessEntry {
                pid: 1,
                args: [
                    "/bin/gang",
                    "worker",
                    "--config",
                    "/tmp/run/state/server/gang.toml",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            },
            ProcessEntry {
                pid: 2,
                args: [
                    "/bin/gang",
                    "worker",
                    "--config",
                    "/tmp/run-other/state/server/gang.toml",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            },
        ];
        let selected = worker_commands_for(&entries, Path::new("/tmp/run/state"));
        assert_eq!(selected, vec![entries[0].args.clone()]);
    }
}
