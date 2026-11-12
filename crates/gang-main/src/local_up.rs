//! `gang local up`: starts the gang server and waits until it announces its CA pin, or, under
//! `--insecure`, its plaintext endpoint.

use std::fmt::Display;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use gang_config::{Document, Fleets, GangConfig, ServerTransport};
use gang_os::FileLock;
use gang_spi::PluginContext;
use gang_types::announcement::{
    CA_PIN, ListenerRole, PLAINTEXT, last_ca_pin, last_listening, last_plaintext,
};
use gang_types::{CaPin, SchedulerSettings, Url};

use crate::cli::LocalUpArgs;
use crate::config_file::LoadedConfig;
use crate::errors::{self, LocalCommandError};
use crate::local_pin::{require_insecure_loopback, with_ca_pin, with_plaintext};
use crate::local_process::{
    LaunchedServer, LocalStackProcesses, ProcessControl, ServerLauncher, SystemLauncher, Wait,
};
use crate::local_record::{ServerDirectory, ServerRecord};
use crate::mark;
use crate::output::{Line, Mark, Output, Record, elapsed};
use crate::plugins::{Plugins, SignerConfig};
use crate::runtime;
use crate::signer_pin::configured_ca_pin;
use crate::workspace;

/// How the local server is secured: the one decision its configuration copy, its startup wait and its report derive from.
pub(crate) enum Security {
    /// A TLS server: clients pin its certificate authority; the announced pin must equal this derived one.
    Pinned(CaPin),
    /// A plaintext server that anyone on this machine can use.
    Insecure,
}

impl Security {
    /// The transport the server speaks.
    fn transport(&self) -> ServerTransport {
        match self {
            Self::Pinned(_) => ServerTransport::Tls,
            Self::Insecure => ServerTransport::Plaintext,
        }
    }

    /// The name of the announcement the server prints when it is ready under this security.
    fn awaited(&self) -> &'static str {
        match self {
            Self::Pinned(_) => CA_PIN,
            Self::Insecure => PLAINTEXT,
        }
    }
}

/// Starts the server with `config` and records it once its log carries the announcement `security` waits for.
///
/// The launch truncates the log, so only an announcement the server prints itself can count. Any failure
/// after the launch stops and reaps the server before the record is removed; the primary error
/// is returned even when that cleanup fails.
pub(crate) fn start_server<L: ServerLauncher>(
    directory: &ServerDirectory,
    config: &Path,
    security: &Security,
    wait: &Wait,
    launcher: &L,
    processes: &impl ProcessControl,
    lock: &FileLock,
) -> Result<ServerRecord, LocalCommandError> {
    let log = directory.log_path();
    let mut server = launcher.launch(config, &log, lock)?;
    let record = ServerRecord {
        pid: server.pid(),
        log: log.clone(),
        config: config.to_path_buf(),
    };
    let outcome = directory
        .write(&record)
        .and_then(|()| await_announcement(security, wait, processes, &mut server, &log));
    match outcome {
        Ok(()) => Ok(record),
        Err(error) => {
            abort(&mut server, wait, processes);
            // Best effort: the record may never have been written, and a failed removal must not
            // hide the error that caused the abort.
            let _ = directory.remove();
            Err(error)
        }
    }
}

/// Polls the log until it carries the announcement `security` waits for, checking the child's liveness between polls.
fn await_announcement<H: LaunchedServer>(
    security: &Security,
    wait: &Wait,
    processes: &impl ProcessControl,
    server: &mut H,
    log: &Path,
) -> Result<(), LocalCommandError> {
    wait.until(processes, || {
        if announced(&read_log(log)?, security)? {
            return Ok(Some(()));
        }
        if server.has_exited()? {
            return Err(LocalCommandError::ServerExited {
                log: log.to_path_buf(),
            });
        }
        Ok(None)
    })?
    .ok_or_else(|| LocalCommandError::StartupTimedOut {
        awaited: security.awaited(),
        timeout: wait.timeout.get(),
        log: log.to_path_buf(),
    })
}

/// Whether `log` carries the announcement `security` waits for. A pin that differs from the derived one is an error.
fn announced(log: &str, security: &Security) -> Result<bool, LocalCommandError> {
    match security {
        Security::Pinned(expected) => match last_ca_pin(log) {
            Some(announced) if announced == *expected => Ok(true),
            Some(announced) => Err(LocalCommandError::PinMismatch {
                derived: *expected,
                announced,
            }),
            None => Ok(false),
        },
        Security::Insecure => Ok(last_plaintext(log).is_some()),
    }
}

/// The text of the log, or nothing while the log does not exist yet.
fn read_log(log: &Path) -> Result<String, LocalCommandError> {
    Ok(gang_os::fs::read_to_string_optional(log)?.unwrap_or_default())
}

/// Stops a launched server that did not start cleanly: terminate it, wait for it to exit, and
/// kill it if it is still running. A server that already exited is left alone, since its pid may
/// have been reused. Every step is best effort.
fn abort<H: LaunchedServer>(server: &mut H, wait: &Wait, processes: &impl ProcessControl) {
    if exited(server) {
        return;
    }
    let _ = processes.terminate(server.pid());
    let stopped = wait.until(processes, || {
        Ok::<_, LocalCommandError>(exited(server).then_some(()))
    });
    if matches!(stopped, Ok(None)) {
        let _ = server.kill();
    }
}

/// Whether the server has exited; an error from the check counts as still running.
fn exited<H: LaunchedServer>(server: &mut H) -> bool {
    matches!(server.has_exited(), Ok(true))
}

/// The address the server announced for `role` before its startup line.
fn listener_address(
    log_text: &str,
    role: ListenerRole,
    log: &Path,
) -> Result<SocketAddr, LocalCommandError> {
    last_listening(log_text, role).ok_or_else(|| LocalCommandError::MissingListener {
        role,
        log: log.to_path_buf(),
    })
}

/// The warning `gang local up --insecure` prints: both listeners accept unauthenticated connections.
const INSECURE_WARNING: &str =
    "no TLS and no authentication: anyone on this machine can use the farm";

/// The dashboard page under the public `endpoint`, which may or may not end with a slash, when the
/// dashboard is `enabled`.
fn dashboard_url(enabled: bool, endpoint: &Url) -> Option<String> {
    enabled.then(|| format!("{}/ui/", endpoint.as_str().trim_end_matches('/')))
}

/// `path` relative to `base` when it lies below it, otherwise as it is.
fn shown_path(path: &Path, base: Option<&Path>) -> String {
    base.and_then(|base| path.strip_prefix(base).ok())
        .unwrap_or(path)
        .display()
        .to_string()
}

/// One configured fleet as the banner shows it.
struct FleetCapacity {
    /// The fleet's name.
    name: String,
    /// The most workers the fleet ever runs.
    max_instances: u32,
}

/// The banner lines: the command and its version, the configuration and a capacity tape per fleet.
fn banner(config: &str, fleets: &[FleetCapacity]) -> Vec<Line> {
    let mut lines = vec![
        Line::new()
            .strong("gang local up")
            .muted(format!("  {}", env!("CARGO_PKG_VERSION"))),
        Line::new().muted(config),
        Line::new(),
    ];
    lines.extend(fleets.iter().map(|fleet| {
        let cells = usize::try_from(fleet.max_instances).unwrap_or(usize::MAX);
        Line::new()
            .plain(format!("{}  ", fleet.name))
            .muted(format!(
                "{}  0 of {}",
                mark::tape(cells, 0),
                fleet.max_instances
            ))
    }));
    lines
}

/// Everything `gang local up` reports about the server it started.
struct Started {
    /// Process id of the server.
    pid: u32,
    /// How long the server took to announce itself.
    took: Duration,
    /// Address clients reach the public listener at.
    public: Url,
    /// Address workers reach the worker listener at.
    worker: Url,
    /// How the server is secured.
    security: Security,
    /// The dashboard page, when the dashboard is enabled.
    dashboard: Option<String>,
    /// The server's log file.
    log: PathBuf,
}

/// An endpoint as shown to the user: without its trailing slash.
fn displayed(endpoint: &Url) -> &str {
    endpoint
        .as_str()
        .strip_suffix('/')
        .unwrap_or(endpoint.as_str())
}

impl Started {
    /// The `gang bazelrc` command that points Bazel at the server.
    fn bazelrc_command(&self) -> String {
        let server = displayed(&self.public);
        match &self.security {
            Security::Pinned(ca_pin) => {
                format!("gang bazelrc --server {server} --ca-pin {ca_pin} > .bazelrc")
            }
            Security::Insecure => format!("gang bazelrc --server {server} > .bazelrc"),
        }
    }
}

/// Prints the warning, the outcome, the endpoints and the next step of a started server.
fn present(out: &Output, started: &Started) -> errors::Result<()> {
    if matches!(started.security, Security::Insecure) {
        out.step(&Record::warn(INSECURE_WARNING));
    }
    out.result(&Record::ok(
        Line::new().plain("started the gang server ").muted(format!(
            "in {} · pid {}",
            elapsed(started.took),
            started.pid
        )),
    ))?;
    out.gap()?;
    let log = started.log.display();
    let public = displayed(&started.public);
    let worker = displayed(&started.worker);
    let mut fields: Vec<(&str, &dyn Display)> = vec![
        ("public", &public as &dyn Display),
        ("worker", &worker as &dyn Display),
    ];
    match &started.security {
        Security::Pinned(ca_pin) => fields.push(("ca-pin", ca_pin as &dyn Display)),
        Security::Insecure => {
            if let Some(url) = &started.dashboard {
                fields.push(("ui", url as &dyn Display));
            }
        }
    }
    fields.push(("log", &log as &dyn Display));
    out.fields(&fields)?;
    out.next(&started.bazelrc_command());
    Ok(())
}

/// The addresses the configuration copy of a server is rendered from.
struct Endpoints<'a> {
    /// The configured public endpoint.
    public: Option<&'a Url>,
    /// The configured worker endpoint.
    worker: Option<&'a Url>,
}

/// The configuration copy the server reads, how `gang local up` waits for the server and reports it, and how
/// the server is secured. An insecure server needs no signer; a TLS server's pin derives from `signer`, built
/// from `config_path`.
fn prepare(
    insecure: bool,
    signer: SignerConfig,
    config_path: &Path,
    endpoints: &Endpoints<'_>,
    document: Document<Plugins>,
) -> errors::Result<(String, Security, Option<Url>, Option<Url>)> {
    if insecure {
        let (rendered, public, worker) =
            with_plaintext(document, endpoints.public, endpoints.worker)?;
        return Ok((rendered, Security::Insecure, public, worker));
    }
    let context = PluginContext { config_path };
    let derived = runtime::block_on(configured_ca_pin(signer, &context))?;
    Ok((
        with_ca_pin(document, derived)?,
        Security::Pinned(derived),
        endpoints.public.cloned(),
        endpoints.worker.cloned(),
    ))
}

/// Prints the banner: the command, the configuration at `config_path` and the capacity of every fleet.
fn show_banner(out: &Output, config_path: &Path, fleets: &Fleets<Plugins>) {
    let fleets: Vec<FleetCapacity> = fleets
        .iter()
        .map(|fleet| FleetCapacity {
            name: fleet.policy.name.to_string(),
            max_instances: fleet.policy.instances.max().get(),
        })
        .collect();
    out.decoration(
        Some(Mark::Full),
        &banner(
            &shown_path(
                config_path,
                workspace::working_directory(|key| std::env::var_os(key)).as_deref(),
            ),
            &fleets,
        ),
    );
    // One blank row separates the banner from the wait line and the records.
    out.decoration(None, &[Line::new()]);
}

/// Refuses before anything is written or started: `--insecure` needs loopback addresses, and a
/// plaintext configuration needs `--insecure`.
fn check_security(insecure: bool, config: &GangConfig<Plugins>) -> Result<(), LocalCommandError> {
    if insecure {
        return require_insecure_loopback(
            config.listen,
            config.worker_listen,
            config.endpoint.as_ref(),
            &config.scheduler,
        );
    }
    match config.server.transport.kind() {
        ServerTransport::Tls => Ok(()),
        ServerTransport::Plaintext => Err(LocalCommandError::PlaintextNeedsInsecure),
    }
}

/// `gang local up`: starts the gang server and prints how to reach it.
pub(crate) fn run(args: LocalUpArgs, out: &Output) -> errors::Result<ExitCode> {
    let (loaded, document, directory) = LoadedConfig::local_document(args.config)?;
    check_security(args.insecure, &loaded.config)?;
    directory.create()?;
    // Refuse before deriving the pin or rewriting the configuration copy a live server reads.
    let lock = directory.acquire_lock()?;
    let (rendered, security, public, worker) = prepare(
        args.insecure,
        loaded.config.signer,
        &loaded.path,
        &Endpoints {
            public: loaded.config.endpoint.as_ref(),
            worker: loaded.config.scheduler.worker_endpoint(),
        },
        document,
    )?;
    let transport = security.transport();
    directory.write_config(&rendered)?;
    show_banner(out, &loaded.path, &loaded.config.fleets);
    let wait = Wait {
        timeout: args.startup_timeout,
        poll_interval: args.poll_interval,
    };
    let launcher = SystemLauncher::gang()?;
    let progress = out.progress("starting the gang server");
    let record = start_server(
        &directory,
        &directory.config_path(),
        &security,
        &wait,
        &launcher,
        &LocalStackProcesses,
        &lock,
    )?;
    let took = progress.finish();
    let log_text = read_log(&record.log)?;
    let public_address = listener_address(&log_text, ListenerRole::Public, &record.log)?;
    let public = match public {
        Some(endpoint) => endpoint,
        None => transport.public_url(public_address)?,
    };
    let worker_address = listener_address(&log_text, ListenerRole::Worker, &record.log)?;
    let worker = match worker {
        Some(endpoint) => endpoint,
        None => transport.worker_url(worker_address)?,
    };
    let dashboard = if matches!(security, Security::Insecure) {
        dashboard_url(loaded.config.dashboard.enabled, &public)
    } else {
        None
    };
    present(
        out,
        &Started {
            pid: record.pid,
            took,
            public,
            worker,
            security,
            dashboard,
            log: record.log,
        },
    )?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;
    use crate::config_file::tests::VALID_LOCAL_CONFIG;
    use crate::output::text_of;
    use crate::palette::{Palette, strip_ansi as without_escapes};
    use crate::signer_pin::file_signer;
    use gang_config::{CaPinSetting, GangConfig, Transport};
    use gang_os::PathIo;
    use gang_types::NonZeroDuration;
    use gang_types::announcement::Announcement;
    use parking_lot::Mutex;

    /// Pid the fake launcher reports.
    const LAUNCH_PID: u32 = 4242;
    /// Configuration path handed to the launcher; the fakes never read it.
    const CONFIG: &str = "/unused/gang.toml";
    /// Digest byte of the expected pin.
    const PIN_BYTE: u8 = 0xab;
    /// Digest byte of a second pin, different from [`PIN_BYTE`].
    const OTHER_BYTE: u8 = 0xcd;
    /// A `ca-pin:` line whose digest is not hex.
    const MALFORMED_PIN_LINE: &str = "ca-pin: sha256:not-hex";

    fn pin() -> CaPin {
        CaPin::new([PIN_BYTE; 32])
    }

    fn other_pin() -> CaPin {
        CaPin::new([OTHER_BYTE; 32])
    }

    /// A wait of 50ms polled every 10ms: five polls, with fakes that never sleep.
    fn quick_wait() -> Wait {
        Wait {
            timeout: NonZeroDuration::from_millis(50),
            poll_interval: NonZeroDuration::from_millis(10),
        }
    }

    /// A state directory with its server directory created; the temporary root is kept alive by the caller.
    fn sandbox() -> (TempDir, ServerDirectory) {
        let root = TempDir::new().expect("temp dir");
        let directory = ServerDirectory::new(&root.path().join("state"));
        directory.create().expect("state directory");
        (root, directory)
    }

    /// Replaces the log with `text`, as a freshly launched server's output would.
    fn write_log(log: &Path, text: &str) -> Result<(), LocalCommandError> {
        fs::write(log, text).map_err(PathIo::at("write", log))?;
        Ok(())
    }

    /// Launcher that writes `log_text` to the log, as a server would print it, and returns a fake handle.
    struct FakeLauncher {
        log_text: String,
        exit_after_checks: u32,
        has_exited_error: bool,
        signals: Arc<Signals>,
    }

    impl ServerLauncher for FakeLauncher {
        type Handle = FakeHandle;

        fn launch(
            &self,
            _config: &Path,
            log: &Path,
            _lock: &FileLock,
        ) -> Result<FakeHandle, LocalCommandError> {
            write_log(log, &self.log_text)?;
            Ok(FakeHandle {
                exit_after_checks: self.exit_after_checks,
                has_exited_error: self.has_exited_error,
                signals: Arc::clone(&self.signals),
                checks: 0,
            })
        }
    }

    /// A launched fake server that reports exited from its `exit_after_checks + 1`-th check on.
    struct FakeHandle {
        exit_after_checks: u32,
        has_exited_error: bool,
        signals: Arc<Signals>,
        checks: u32,
    }

    impl LaunchedServer for FakeHandle {
        fn pid(&self) -> u32 {
            LAUNCH_PID
        }

        fn has_exited(&mut self) -> Result<bool, LocalCommandError> {
            if self.has_exited_error {
                let source =
                    std::io::Error::new(std::io::ErrorKind::PermissionDenied, "exit check refused");
                return Err(LocalCommandError::Io(PathIo::at(
                    "check",
                    Path::new(CONFIG),
                )(source)));
            }
            self.checks += 1;
            Ok(self.checks > self.exit_after_checks)
        }

        fn kill(&mut self) -> Result<(), LocalCommandError> {
            self.signals.kills.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Signals the fakes have delivered, shared by the process control and the handles it stops.
    #[derive(Default)]
    struct Signals {
        terminated: Mutex<Vec<u32>>,
        kills: AtomicU32,
    }

    /// Process control that records terminations and counts pauses. After `after` pauses it appends
    /// `text` to the log, as a server announcing its pin would. It never sleeps.
    struct FakeProcesses {
        log: PathBuf,
        announce: Option<(u32, String)>,
        pauses: Cell<u32>,
        signals: Arc<Signals>,
    }

    impl FakeProcesses {
        /// Processes that never announce anything.
        fn silent(log: &Path) -> Self {
            Self {
                log: log.to_path_buf(),
                announce: None,
                pauses: Cell::new(0),
                signals: Arc::new(Signals::default()),
            }
        }

        /// Processes that append `text` to the log after `after` pauses.
        fn announcing(log: &Path, after: u32, text: String) -> Self {
            Self {
                announce: Some((after, text)),
                ..Self::silent(log)
            }
        }

        /// The pids terminated so far, in order.
        fn terminated(&self) -> Vec<u32> {
            self.signals.terminated.lock().clone()
        }
    }

    impl ProcessControl for FakeProcesses {
        fn terminate(&self, pid: u32) -> Result<(), LocalCommandError> {
            self.signals.terminated.lock().push(pid);
            Ok(())
        }

        fn pause(&self, _interval: Duration) {
            let pauses = self.pauses.get() + 1;
            self.pauses.set(pauses);
            match &self.announce {
                Some((after, text)) if *after == pauses => {
                    OpenOptions::new()
                        .append(true)
                        .open(&self.log)
                        .expect("log opens for append")
                        .write_all(text.as_bytes())
                        .expect("announcement appends");
                }
                _ => {}
            }
        }
    }

    /// The signal that makes a [`SignalHandle`] report exited.
    #[derive(Clone, Copy)]
    enum ExitsOn {
        Terminate,
        Kill,
    }

    /// Launcher whose server writes `log_text` and exits only once it receives `exits_on`.
    struct SignalLauncher {
        log_text: String,
        exits_on: ExitsOn,
        signals: Arc<Signals>,
    }

    impl ServerLauncher for SignalLauncher {
        type Handle = SignalHandle;

        fn launch(
            &self,
            _config: &Path,
            log: &Path,
            _lock: &FileLock,
        ) -> Result<SignalHandle, LocalCommandError> {
            write_log(log, &self.log_text)?;
            Ok(SignalHandle {
                exits_on: self.exits_on,
                signals: Arc::clone(&self.signals),
            })
        }
    }

    /// A launched fake server whose exit depends on the signals delivered to it.
    struct SignalHandle {
        exits_on: ExitsOn,
        signals: Arc<Signals>,
    }

    impl LaunchedServer for SignalHandle {
        fn pid(&self) -> u32 {
            LAUNCH_PID
        }

        fn has_exited(&mut self) -> Result<bool, LocalCommandError> {
            Ok(match self.exits_on {
                ExitsOn::Terminate => !self.signals.terminated.lock().is_empty(),
                ExitsOn::Kill => self.signals.kills.load(Ordering::SeqCst) > 0,
            })
        }

        fn kill(&mut self) -> Result<(), LocalCommandError> {
            self.signals.kills.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn success_returns_and_records_the_server() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: format!("{}\n", Announcement::CaPin(pin())),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let record = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect("a server announcing the expected pin starts");

        let expected = ServerRecord {
            pid: LAUNCH_PID,
            log: directory.log_path(),
            config: PathBuf::from(CONFIG),
        };
        assert_eq!(record, expected);
        assert_eq!(
            directory.read().expect("record reads"),
            Some(expected),
            "the record is written"
        );
        assert!(processes.terminated().is_empty());
    }

    #[test]
    fn pin_announced_after_three_pauses_is_found() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: String::new(),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::announcing(
            &directory.log_path(),
            3,
            format!("{}\n", Announcement::CaPin(pin())),
        );

        start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect("the pin appears on the fourth poll");

        assert_eq!(processes.pauses.get(), 3);
    }

    #[test]
    fn announced_pin_different_from_expected_is_a_mismatch() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: format!("{}\n", Announcement::CaPin(other_pin())),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a different announced pin is refused");

        match error {
            LocalCommandError::PinMismatch { derived, announced } => {
                assert_eq!(derived, pin());
                assert_eq!(announced, other_pin());
            }
            other => panic!("expected a pin mismatch, got {other:?}"),
        }
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn no_announcement_within_the_budget_times_out_and_terminates() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: String::new(),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a server that never announces times out");

        assert!(matches!(
            error,
            LocalCommandError::StartupTimedOut {
                awaited: "ca-pin",
                ..
            }
        ));
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn child_exiting_first_is_server_exited_and_not_terminated() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: String::new(),
            exit_after_checks: 0,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a server that exits before announcing fails");

        assert!(matches!(error, LocalCommandError::ServerExited { .. }));
        assert!(
            processes.terminated().is_empty(),
            "an exited server is not signalled"
        );
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn stale_log_pin_does_not_count_when_the_child_exits() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let log = directory.log_path();
        // The previous run's pin is still in the log when the launch happens.
        fs::write(&log, format!("{}\n", Announcement::CaPin(pin()))).expect("stale log");
        // The launch replaces the log with the child's own output, which is empty, and the child
        // has already exited.
        let launcher = FakeLauncher {
            log_text: String::new(),
            exit_after_checks: 0,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&log);

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a server that exits must not succeed on a stale pin");

        assert!(matches!(error, LocalCommandError::ServerExited { .. }));
        assert_eq!(directory.read().expect("record reads"), None);
        let text = fs::read_to_string(&log).expect("log reads");
        assert!(
            last_ca_pin(&text).is_none(),
            "the launch replaced the stale pin"
        );
        assert!(processes.terminated().is_empty());
    }

    #[test]
    fn malformed_pin_line_is_ignored() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: format!("{MALFORMED_PIN_LINE}\n"),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::announcing(
            &directory.log_path(),
            2,
            format!("{}\n", Announcement::CaPin(pin())),
        );

        start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect("the malformed line is skipped and the valid pin is found");

        assert_eq!(processes.pauses.get(), 2);
    }

    #[test]
    fn record_write_failure_terminates_the_launched_server() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        // A directory where the record file must go makes the atomic rename fail.
        fs::create_dir(directory.record_path()).expect("blocking directory");
        let launcher = FakeLauncher {
            log_text: format!("{}\n", Announcement::CaPin(pin())),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("an unwritable record fails the start");

        assert!(matches!(error, LocalCommandError::Io(_)));
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
    }

    #[test]
    fn server_ignoring_terminate_is_killed_and_leaves_no_record() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let processes = FakeProcesses::silent(&directory.log_path());
        // Ignores SIGTERM: it exits only once killed.
        let launcher = SignalLauncher {
            log_text: String::new(),
            exits_on: ExitsOn::Kill,
            signals: Arc::clone(&processes.signals),
        };

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a server that never announces times out");

        assert!(matches!(
            error,
            LocalCommandError::StartupTimedOut {
                awaited: "ca-pin",
                ..
            }
        ));
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
        assert_eq!(
            processes.signals.kills.load(Ordering::SeqCst),
            1,
            "the server is killed once SIGTERM is ignored"
        );
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn mismatched_server_exiting_on_terminate_leaves_no_record() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let processes = FakeProcesses::silent(&directory.log_path());
        let launcher = SignalLauncher {
            log_text: format!("{}\n", Announcement::CaPin(other_pin())),
            exits_on: ExitsOn::Terminate,
            signals: Arc::clone(&processes.signals),
        };

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a different announced pin is refused");

        assert!(matches!(error, LocalCommandError::PinMismatch { .. }));
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
        assert_eq!(
            processes.signals.kills.load(Ordering::SeqCst),
            0,
            "a server that exits on SIGTERM is not killed"
        );
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn abort_terminates_then_kills_when_the_exit_check_errors() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: String::new(),
            exit_after_checks: u32::MAX,
            has_exited_error: true,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Pinned(pin()),
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a failed exit check aborts the start");

        match error {
            LocalCommandError::Io(source) => assert_eq!(
                source.kind(),
                std::io::ErrorKind::PermissionDenied,
                "the exit-check error is the primary error"
            ),
            other => panic!("expected the exit-check error, got {other:?}"),
        }
        assert_eq!(processes.terminated(), vec![LAUNCH_PID]);
        assert_eq!(launcher.signals.kills.load(Ordering::SeqCst), 1);
        assert_eq!(directory.read().expect("record reads"), None);
    }

    #[test]
    fn security_names_its_transport() {
        assert_eq!(Security::Pinned(pin()).transport(), ServerTransport::Tls);
        assert_eq!(Security::Insecure.transport(), ServerTransport::Plaintext);
    }

    #[test]
    fn a_missing_listener_announcement_is_named() {
        let log = Path::new(CONFIG);
        let without = format!("{}\n", Announcement::CaPin(pin()));
        let error = listener_address(&without, ListenerRole::Worker, log)
            .expect_err("a log without the worker line has no worker address");
        assert!(matches!(
            error,
            LocalCommandError::MissingListener {
                role: ListenerRole::Worker,
                ..
            }
        ));

        let with = format!(
            "listening: worker 127.0.0.1:9090\n{}\n",
            Announcement::CaPin(pin())
        );
        let address = listener_address(&with, ListenerRole::Worker, log)
            .expect("the announced worker address is found");
        assert_eq!(address, SocketAddr::from(([127, 0, 0, 1], 9090)));
    }

    /// A `plaintext:` line announcing the endpoint of a server started with `--insecure`.
    const PLAINTEXT_LINE: &str = "plaintext: grpc://127.0.0.1:9443";

    /// The facts of a started TLS server.
    fn tls_server() -> Started {
        Started {
            pid: 48213,
            took: Duration::from_millis(2400),
            public: "https://127.0.0.1:50051".parse().expect("test public url"),
            worker: "grpcs://127.0.0.1:9090".parse().expect("test worker url"),
            security: Security::Pinned(pin()),
            dashboard: None,
            log: PathBuf::from("/tmp/gang-local/server/server.log"),
        }
    }

    /// The facts of a started plaintext server whose dashboard address is `dashboard`.
    fn insecure_server(dashboard: Option<&str>) -> Started {
        Started {
            pid: 48213,
            took: Duration::from_millis(1100),
            public: "http://127.0.0.1:50051".parse().expect("test public url"),
            worker: "grpc://127.0.0.1:9090".parse().expect("test worker url"),
            security: Security::Insecure,
            dashboard: dashboard.map(str::to_owned),
            log: PathBuf::from("/tmp/gang-local/server/server.log"),
        }
    }

    /// What `present` writes to standard output and standard error for `started`, piped.
    fn printed(started: &Started) -> (String, String) {
        let (out, stdout, stderr) = Output::captured();
        present(&out, started).expect("present");
        (stdout.text(), stderr.text())
    }

    #[test]
    fn a_tls_server_prints_the_outcome_the_endpoints_and_the_pin() {
        let (stdout, stderr) = printed(&tls_server());

        let ca_pin = format!("ca-pin  {}", pin());
        assert_eq!(
            stdout,
            text_of(&[
                "    ok  started the gang server in 2.4s · pid 48213",
                "",
                "public  https://127.0.0.1:50051",
                "worker  grpcs://127.0.0.1:9090",
                &ca_pin,
                "   log  /tmp/gang-local/server/server.log",
            ])
        );
        assert_eq!(stderr, "");
    }

    #[test]
    fn an_insecure_server_warns_and_lists_the_dashboard_before_the_log() {
        let (stdout, stderr) = printed(&insecure_server(Some("http://127.0.0.1:50051/ui/")));

        assert_eq!(
            stderr,
            text_of(&[
                "  warn  no TLS and no authentication: anyone on this machine can use the farm"
            ])
        );
        assert_eq!(
            stdout,
            text_of(&[
                "    ok  started the gang server in 1.1s · pid 48213",
                "",
                "public  http://127.0.0.1:50051",
                "worker  grpc://127.0.0.1:9090",
                "    ui  http://127.0.0.1:50051/ui/",
                "   log  /tmp/gang-local/server/server.log",
            ])
        );
    }

    #[test]
    fn an_insecure_server_without_a_dashboard_lists_no_ui() {
        let (stdout, _) = printed(&insecure_server(None));

        assert!(!stdout.contains("    ui  "));
        assert!(stdout.contains("worker  grpc://127.0.0.1:9090\n   log  "));
    }

    #[test]
    fn the_next_command_carries_the_pin_only_for_tls() {
        assert_eq!(
            tls_server().bazelrc_command(),
            format!(
                "gang bazelrc --server https://127.0.0.1:50051 --ca-pin {} > .bazelrc",
                pin()
            )
        );
        assert_eq!(
            insecure_server(None).bazelrc_command(),
            "gang bazelrc --server http://127.0.0.1:50051 > .bazelrc"
        );
    }

    #[test]
    fn endpoints_are_shown_without_a_trailing_slash() {
        let mut started = insecure_server(None);
        started.public = "http://127.0.0.1:50051/".parse().expect("test url");
        started.worker = "grpc://127.0.0.1:9090/".parse().expect("test url");

        let (stdout, _) = printed(&started);

        assert!(stdout.contains("public  http://127.0.0.1:50051\n"));
        assert!(stdout.contains("worker  grpc://127.0.0.1:9090\n"));
        assert_eq!(
            started.bazelrc_command(),
            "gang bazelrc --server http://127.0.0.1:50051 > .bazelrc"
        );
    }

    /// The fleets of the checked-in local configuration.
    fn one_fleet() -> Vec<FleetCapacity> {
        vec![FleetCapacity {
            name: "local-worker-x86-64".to_owned(),
            max_instances: 4,
        }]
    }

    #[test]
    fn the_banner_names_the_command_the_configuration_and_a_tape_per_fleet() {
        let lines = banner("configs/gang.local.toml", &one_fleet());

        let text: Vec<String> = lines.iter().map(Line::text).collect();
        assert_eq!(
            text,
            [
                format!("gang local up  {}", env!("CARGO_PKG_VERSION")),
                "configs/gang.local.toml".to_owned(),
                String::new(),
                format!("local-worker-x86-64  {}  0 of 4", mark::tape(4, 0)),
            ]
        );
    }

    #[test]
    fn the_banner_lists_every_fleet_on_its_own_line() {
        let fleets = [
            FleetCapacity {
                name: "small".to_owned(),
                max_instances: 2,
            },
            FleetCapacity {
                name: "large".to_owned(),
                max_instances: 8,
            },
        ];

        let text: Vec<String> = banner("gang.toml", &fleets)
            .iter()
            .map(Line::text)
            .collect();

        assert_eq!(text.len(), 5);
        assert_eq!(text[3], format!("small  {}  0 of 2", mark::tape(2, 0)));
        assert_eq!(text[4], format!("large  {}  0 of 8", mark::tape(8, 0)));
    }

    /// The column of `needle` in `line`, counted in characters.
    fn column_of(line: &str, needle: &str) -> usize {
        let byte = line.find(needle).expect("needle in line");
        line[..byte].chars().count()
    }

    #[test]
    fn the_styled_banner_puts_the_mark_left_of_the_text_rows() {
        let (out, _stdout, stderr) = Output::captured_styled(Palette::Ansi, 80);

        out.decoration(
            Some(Mark::Full),
            &banner("configs/gang.local.toml", &one_fleet()),
        );

        let screen = without_escapes(&stderr.text());
        let rows: Vec<&str> = screen.lines().collect();
        assert!(rows.len() >= 4, "the mark has at least four rows");
        let text_column = column_of(rows[0], "gang local up");
        assert!(text_column > 0, "the mark precedes the command");
        assert_eq!(column_of(rows[1], "configs/gang.local.toml"), text_column);
        assert_eq!(column_of(rows[3], "local-worker-x86-64"), text_column);
        assert!(rows[3].ends_with("0 of 4"));
    }

    #[test]
    fn a_path_below_the_base_is_shown_relative() {
        assert_eq!(
            shown_path(
                Path::new("/work/configs/gang.local.toml"),
                Some(Path::new("/work"))
            ),
            "configs/gang.local.toml"
        );
    }

    #[test]
    fn a_path_outside_the_base_or_without_a_base_is_shown_whole() {
        assert_eq!(
            shown_path(Path::new("/etc/gang.toml"), Some(Path::new("/work"))),
            "/etc/gang.toml"
        );
        assert_eq!(
            shown_path(Path::new("/etc/gang.toml"), None),
            "/etc/gang.toml"
        );
    }

    #[test]
    fn plaintext_startup_completes_on_the_plaintext_line() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: format!("{PLAINTEXT_LINE}\n"),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let record = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Insecure,
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect("a plaintext server announcing its endpoint starts");

        assert_eq!(record.pid, LAUNCH_PID);
        assert!(processes.terminated().is_empty());
    }

    #[test]
    fn plaintext_startup_ignores_a_ca_pin_line() {
        let (_root, directory) = sandbox();
        let _gate = gang_testkit::fork_gate::lock_user();
        let lock = directory.acquire_lock().expect("server lock");
        let launcher = FakeLauncher {
            log_text: format!("{}\n", Announcement::CaPin(pin())),
            exit_after_checks: u32::MAX,
            has_exited_error: false,
            signals: Arc::default(),
        };
        let processes = FakeProcesses::silent(&directory.log_path());

        let error = start_server(
            &directory,
            Path::new(CONFIG),
            &Security::Insecure,
            &quick_wait(),
            &launcher,
            &processes,
            &lock,
        )
        .expect_err("a TLS server does not satisfy a plaintext startup");

        assert!(matches!(
            error,
            LocalCommandError::StartupTimedOut {
                awaited: "plaintext",
                ..
            }
        ));
        assert!(
            error
                .to_string()
                .contains("printed no plaintext announcement")
        );
    }

    #[test]
    fn the_dashboard_url_points_at_the_ui_under_the_endpoint() {
        assert_eq!(
            dashboard_url(true, &Url::parse("http://127.0.0.1:9443/").expect("url")),
            Some("http://127.0.0.1:9443/ui/".to_owned())
        );
        assert_eq!(
            dashboard_url(true, &Url::parse("http://127.0.0.1:9443").expect("url")),
            Some("http://127.0.0.1:9443/ui/".to_owned())
        );
    }

    #[test]
    fn a_disabled_dashboard_has_no_url() {
        assert_eq!(
            dashboard_url(false, &Url::parse("http://127.0.0.1:9443/").expect("url")),
            None
        );
    }

    /// The valid local configuration rewritten to serve plaintext, with workers that need no TLS.
    fn plaintext_configuration() -> GangConfig<Plugins> {
        let text = VALID_LOCAL_CONFIG
            .replace("[server]\n", "[server]\ntransport = \"plaintext\"\n")
            .replace(
                "[authentication.workers.mtls]\n\
                 provider = \"local-workers\"\n\
                 certificate_ttl = \"1d\"\n\
                 max_cached_certificates = 4096\n",
                "[authentication.workers.none]\n",
            );
        assert!(text.contains("transport = \"plaintext\""));
        assert!(!text.contains("mtls"));
        text.parse::<GangConfig<Plugins>>()
            .expect("the plaintext configuration is valid")
    }

    #[test]
    fn plaintext_configuration_without_insecure_is_refused() {
        let config = plaintext_configuration();
        assert!(matches!(
            check_security(false, &config),
            Err(LocalCommandError::PlaintextNeedsInsecure)
        ));
    }

    #[test]
    fn plaintext_configuration_with_insecure_passes_the_loopback_check() {
        let config = plaintext_configuration();
        check_security(true, &config).expect("a loopback plaintext configuration passes");
    }

    #[test]
    fn tls_configuration_without_insecure_is_accepted() {
        let config = VALID_LOCAL_CONFIG
            .parse::<GangConfig<Plugins>>()
            .expect("the valid local configuration parses");
        check_security(false, &config).expect("a TLS configuration passes");
    }

    /// The valid local configuration parsed into the document `prepare` takes.
    fn valid_document() -> Document<Plugins> {
        Document::<Plugins>::parse(VALID_LOCAL_CONFIG)
            .expect("the valid local configuration parses")
    }

    #[test]
    fn a_tls_prepare_pins_the_copy_to_the_derived_ca() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("gang.toml");
        let endpoint: Url = "https://127.0.0.1:9443"
            .parse()
            .expect("test endpoint is a url");
        let (rendered, security, public, worker) = prepare(
            false,
            file_signer(dir.path()),
            &config_path,
            &Endpoints {
                public: Some(&endpoint),
                worker: None,
            },
            valid_document(),
        )
        .expect("a TLS copy prepares");

        let Security::Pinned(pin) = security else {
            panic!("a TLS prepare is pinned");
        };
        let config: GangConfig<Plugins> = rendered
            .parse()
            .expect("the rendered copy is a valid configuration");
        assert_eq!(config.server.ca_pin, Some(CaPinSetting::Pinned(pin)));
        assert!(matches!(config.server.transport, Transport::Tls { .. }));
        assert_eq!(public.as_ref(), Some(&endpoint));
        assert_eq!(worker, None);
    }

    #[test]
    fn an_insecure_prepare_is_plaintext_without_a_pin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("gang.toml");
        let endpoint = Url::parse("https://127.0.0.1:443/").expect("test endpoint is a url");
        let worker_endpoint =
            Url::parse("grpcs://127.0.0.1:9444/").expect("test worker endpoint is a url");
        let (rendered, security, public, worker) = prepare(
            true,
            file_signer(dir.path()),
            &config_path,
            &Endpoints {
                public: Some(&endpoint),
                worker: Some(&worker_endpoint),
            },
            valid_document(),
        )
        .expect("a plaintext copy prepares");

        assert!(matches!(security, Security::Insecure));
        let config: GangConfig<Plugins> = rendered
            .parse()
            .expect("the rendered copy is a valid configuration");
        assert!(matches!(config.server.transport, Transport::Plaintext));
        assert_eq!(config.server.ca_pin, None);
        let [role] = config.server.roles.as_slice() else {
            panic!("the insecure copy has exactly one role");
        };
        assert_eq!(role.name.as_str(), "administrator");
        assert_eq!(
            config.endpoint.as_ref().map(Url::scheme),
            Some("http"),
            "the public endpoint is plaintext"
        );
        assert_eq!(public, config.endpoint);
        assert_eq!(
            public.as_ref().map(Url::as_str),
            Some("http://127.0.0.1:443/")
        );
        assert_eq!(worker.as_ref(), config.scheduler.worker_endpoint());
        assert_eq!(
            worker.as_ref().map(Url::as_str),
            Some("grpc://127.0.0.1:9444/")
        );
    }

    #[test]
    fn an_insecure_prepare_derives_no_pin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("gang.toml");
        let key_file = dir.path().join("signer").join("ca.key");

        let (_rendered, security, public, worker) = prepare(
            true,
            file_signer(dir.path()),
            &config_path,
            &Endpoints {
                public: None,
                worker: None,
            },
            valid_document(),
        )
        .expect("a plaintext copy prepares");

        assert!(matches!(security, Security::Insecure));
        assert_eq!(public, None);
        assert_eq!(worker, None);
        assert!(!key_file.exists(), "the signer's key is never created");
        assert!(
            !key_file.parent().is_some_and(Path::exists),
            "the signer's directory is never created"
        );
    }
}
