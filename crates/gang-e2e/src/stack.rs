//! The real deployment: one `gang` server, its workers, and the Bazel workspace that uses them.
//!
//! [`Stack::create`] writes the configuration, provisions the signer key, stages the sample, and
//! prepares the commands that drive the deployment.
//! The server is started by [`System::start_server`], and everything the stack started is torn
//! down when it is dropped.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock as _, SystemClock};
use gang_types::announcement::{Announcement, ListenerRole, last_ca_pin};
use gang_types::{CaPin, Url};
use gang_wire::routes::LIST_EVENTS;
use gang_wire::{Event, Frame, State};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

use crate::administrator::Administrator;
use crate::bazel::Bazel;
use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::event_log::{EventFollower, describe_early_exit, early_exit};
use crate::flags::Remote;
use crate::gang_cli::GangCli;
use crate::host::Host;
use crate::issuer::{TestIssuer, TokenSpec};
use crate::placement::Placement;
use crate::process::{self, ProcessEntry, Supervised};
use crate::report::ProbeReport;
use crate::sample::{self, SampleParameters};
use crate::secured;
use crate::session::{self, SessionCookie};
use crate::settings::Settings;
use crate::signer_key;
use crate::sse::{self, EventStream as _, STREAM_OK};
use crate::state_api::StateApi;
use crate::suite::Invocation;
use crate::system::{
    HOLD_CPU_COUNT, HOLD_MEMORY_KB, LARGE_FLEET, Limits, Probe, Sandbox, ScopeFacts, System,
    Variant,
};
use crate::template::{ConfigTemplate, Transport};

/// A running `gang` deployment and the Bazel workspace that builds against it.
pub struct Stack<'a> {
    placement: Placement,
    settings: Settings,
    bazel: Bazel,
    cli: GangCli,
    binary: PathBuf,
    config_file: PathBuf,
    /// The public endpoint the running server announced; each start replaces it.
    endpoint: Mutex<Option<Url>>,
    /// The worker listener the running server announced; each start replaces it.
    worker: Mutex<Option<SocketAddr>>,
    transport: Transport,
    tmp_path: PathBuf,
    /// The named pipe the hold probe opens for writing once its orphan runs: each action sees its
    /// path as `E2E_SIGNAL`.
    signal_path: PathBuf,
    /// The harness's end of the pipe at `signal_path`, which the hold action signals through.
    signal: crate::hold::Signal,
    salt: String,
    issuer: &'a TestIssuer,
    /// The CA pin of a mutual-TLS deployment; `None` for a plaintext one.
    ca_pin: Option<CaPin>,
    /// The key file the server signs certificates and sessions with, as provisioned for a
    /// mutual-TLS deployment; `None` for a plaintext one.
    signer_key: Option<PathBuf>,
    /// Whether the developer is enrolled; a restarted server keeps the enrollment.
    developer: Mutex<bool>,
    host: Host,
    server: Mutex<Option<Supervised>>,
    /// The administrator of the running server, shared by every read of the scaling history and
    /// every revocation. A start replaces the endpoint it dials, so each start and kill drops it.
    administrator: Mutex<Option<Arc<Administrator<'a>>>>,
    /// The HTTP client of the running server's API, built on first use once the CA pin is known.
    /// Its pooled connections reach one server process, so each start and kill drops it.
    pub(crate) server_http: Mutex<Option<Arc<crate::http::Http>>>,
}

impl<'a> Stack<'a> {
    /// Writes the configuration of `variant` over `transport` into a fresh placement, provisions
    /// the signer key of a mutual-TLS deployment, stages the sample into its workspace, and
    /// prepares the commands that drive the deployment. Client identities are issued by `issuer`.
    /// Nothing is started.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::Template`] when the base configuration cannot be adapted,
    /// [`E2eError::Io`] when a file or directory cannot be written, and the errors of
    /// [`Placement::create`], [`sample::stage`], and [`signer_key::provision`].
    pub fn create(
        invocation: &Invocation,
        variant: &Variant,
        transport: Transport,
        issuer: &'a TestIssuer,
    ) -> Result<Self> {
        let settings = &invocation.settings;
        let placement = Placement::create(settings.scratch_root.as_deref())?;

        let salt = sample::salt()?;
        let signal_dir = placement.root().join(format!("signal-{salt}"));
        gang_os::fs::ensure_private_dir(&signal_dir)
            .map_err(E2eError::system_from("creating the signal directory"))?;
        let signal_path = signal_dir.join("started");
        let signal = crate::hold::Signal::create(&signal_path)?;

        let template = ConfigTemplate::parse(&invocation.base_config)?
            .place(&placement, transport)?
            .vary(variant, &settings.worker_launcher, &invocation.action_tools)?
            .with_signal_dir(&signal_dir)?;
        let (template, ca_pin, signer_key) = match transport {
            Transport::Tls => {
                let template = template.with_identity(issuer.url(), variant.identity)?;
                let signer_key = template.signer_key_file()?;
                let ca_pin = signer_key::provision(&signer_key)?;
                (
                    template.with_ca_pin(&ca_pin)?,
                    Some(ca_pin),
                    Some(signer_key),
                )
            }
            Transport::Plain => (template, None, None),
        };

        let isa = template.default_isa()?;
        let tmp_path = template.tmp_path()?;
        let large_memory_kb = template.large_memory_kb()?;
        let rendered = template.render()?;
        let config_file = placement.config_file();
        fs::write(&config_file, rendered)
            .map_err(E2eError::io(format!("writing {}", config_file.display())))?;

        let parameters = SampleParameters {
            salt: salt.clone(),
            action_path: invocation.action_tools.path.clone(),
            signal: signal_path.clone(),
            attempts: crate::flags::attempts_path(&signal_path),
            isa,
            memory_kb: HOLD_MEMORY_KB,
            cpu_count: HOLD_CPU_COUNT,
            large_fleet: LARGE_FLEET.to_owned(),
            large_memory_kb,
            requested_sandbox: Sandbox::Noop,
        };
        sample::stage(&invocation.sample, &placement.workspace(), &parameters)?;

        if transport == Transport::Tls {
            for directory in [
                placement.home().join("admin"),
                placement.home().join("admin-workspace"),
            ] {
                fs::create_dir_all(&directory)
                    .map_err(E2eError::io(format!("creating {}", directory.display())))?;
            }
        }

        Ok(Self {
            cli: GangCli::new(
                invocation.gang.clone(),
                config_file.clone(),
                placement.home(),
            ),
            bazel: Bazel::new(
                invocation.bazel.clone(),
                settings.clone(),
                invocation.action_tools.clone(),
                placement.workspace(),
                placement.bazel_root(),
            ),
            binary: invocation.gang.clone(),
            config_file,
            endpoint: Mutex::new(None),
            worker: Mutex::new(None),
            transport,
            tmp_path,
            signal_path,
            signal,
            salt,
            issuer,
            ca_pin,
            signer_key,
            developer: Mutex::new(false),
            host: Host::observe(),
            settings: settings.clone(),
            server: Mutex::new(None),
            administrator: Mutex::new(None),
            server_http: Mutex::new(None),
            placement,
        })
    }

    /// The placement the deployment keeps its files in.
    #[must_use]
    pub(crate) const fn placement(&self) -> &Placement {
        &self.placement
    }
    /// The pinned Bazel executable used by this stack.
    #[must_use]
    pub fn bazel_program(&self) -> &Path {
        self.bazel.program()
    }

    /// The path of the signal pipe the hold action writes to, as actions see it in `E2E_SIGNAL`.
    #[must_use]
    pub fn signal_path(&self) -> &Path {
        &self.signal_path
    }

    /// The server's signing key file.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] for a plaintext deployment, which has no signer key.
    pub fn signer_key(&self) -> Result<&Path> {
        self.signer_key
            .as_deref()
            .ok_or_else(|| E2eError::system("a plaintext stack has no signer key"))
    }

    /// The worker listener the running server announced.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the server has not announced its worker listener.
    pub fn worker_endpoint(&self) -> Result<SocketAddr> {
        self.worker.lock().as_ref().copied().ok_or_else(|| {
            E2eError::system("the gang server has not announced its worker listener")
        })
    }

    /// The settings the deployment was created with.
    #[must_use]
    pub const fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The public URL Bazel and `gang login` dial: the endpoint the running server announced.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when no server has announced an endpoint yet.
    pub(crate) fn endpoint(&self) -> Result<Url> {
        self.endpoint
            .lock()
            .clone()
            .ok_or_else(|| E2eError::system("the gang server has not announced its endpoint"))
    }

    /// How a build reaches the executor of the running server.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when no server has announced an endpoint yet, or it has no host.
    fn remote(&self) -> Result<Remote> {
        let endpoint = self.endpoint()?;
        Ok(match self.transport {
            Transport::Tls => Remote::Gang {
                host: endpoint
                    .host_str()
                    .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?
                    .to_owned(),
                bazelrc: self.placement.home().join(secured::BAZEL_RC_FILE),
                home: self.placement.home(),
                helper: self.binary.clone(),
            },
            Transport::Plain => Remote::Plain {
                bazelrc: self.placement.home().join(secured::BAZEL_RC_FILE),
                home: self.placement.home(),
            },
        })
    }

    /// The `gang` binary under test.
    #[must_use]
    pub(crate) fn binary(&self) -> &Path {
        &self.binary
    }

    /// The administrator of the running deployment, which dials the endpoint it announced. It is
    /// created once per server lifetime, so that its login is made once and kept.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] for a plaintext deployment, which has no administrator, and when no
    /// server has announced an endpoint yet.
    pub(crate) fn administrator(&self) -> Result<Arc<Administrator<'a>>> {
        if self.transport != Transport::Tls {
            return Err(E2eError::system(
                "a plaintext deployment has no administrator",
            ));
        }
        let mut slot = self.administrator.lock();
        if let Some(administrator) = slot.as_ref() {
            return Ok(Arc::clone(administrator));
        }
        let administrator = Arc::new(Administrator::new(
            GangCli::new(
                self.binary.clone(),
                self.config_file.clone(),
                self.placement.home().join("admin"),
            ),
            self.issuer,
            self.endpoint()?,
            self.placement.home().join("admin-workspace"),
            self.settings.command_timeout,
        ));
        *slot = Some(Arc::clone(&administrator));
        Ok(administrator)
    }

    /// Stops the running server with SIGTERM, waiting at most `timeout` for it to exit.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when no server is running; otherwise the errors of
    /// [`Supervised::terminate`].
    pub fn terminate_server(&self, timeout: Duration) -> Result<ExitStatus> {
        *self.administrator.lock() = None;
        *self.server_http.lock() = None;
        *self.endpoint.lock() = None;
        *self.worker.lock() = None;
        let child = self.server.lock().take();
        match child {
            Some(mut child) => child.terminate(timeout),
            None => Err(E2eError::system("the gang server is not running")),
        }
    }

    /// The CA pin this deployment was provisioned with; the server must announce the same pin.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] for a plaintext deployment, which has no certificate authority.
    pub(crate) fn ca_pin(&self) -> Result<CaPin> {
        self.ca_pin
            .ok_or_else(|| E2eError::system("a plaintext deployment has no certificate authority"))
    }

    /// The session cookie of `principal` (`provider:principal`) for the running server, signed
    /// with the key the deployment was provisioned with and valid for the token lifetime.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] for a plaintext deployment, which has no sessions, when no server has
    /// announced an endpoint yet, and when the signing key cannot be used; [`E2eError::Io`] when
    /// the key file cannot be read.
    pub(crate) fn session_cookie(&self, principal: &str) -> Result<SessionCookie> {
        let key_file = self
            .signer_key
            .as_deref()
            .ok_or_else(|| E2eError::system("a plaintext deployment has no sessions to mint"))?;
        session::session_cookie(
            key_file,
            &self.endpoint()?,
            principal,
            SystemClock::new().wall(),
            self.settings.token_lifetime,
        )
    }

    /// The state API as the deployment's own administrator reads it: with no credential on a
    /// plaintext deployment, whose anonymous caller is its administrator, and with the
    /// administrator's bearer token otherwise.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when no server has announced an endpoint yet, the administrator
    /// cannot log in or a client cannot be built.
    fn state_api(&self) -> Result<StateApi> {
        let endpoint = self.endpoint()?;
        match self.transport {
            Transport::Plain => {
                StateApi::anonymous(&endpoint, None, self.settings.handshake_timeout)
            }
            Transport::Tls => {
                let credentials = self.administrator()?.credentials(&self.ca_pin()?)?;
                StateApi::with_bearer(&endpoint, &credentials, self.settings.handshake_timeout)
            }
        }
    }

    /// Builds `probes` through `remote`, returning how the build ended.
    /// A follower accelerates failure on a newly recorded early exit; its own errors do not
    /// change the build result.
    ///
    /// # Errors
    ///
    /// When the event follower cannot be opened or Bazel cannot be run.
    pub(crate) fn run_build(&self, remote: &Remote, probes: &[Probe]) -> Result<BuildOutcome> {
        let labels: Vec<String> = probes.iter().map(|probe| probe.label()).collect();
        let mut follower = match self.transport {
            Transport::Tls => self
                .administrator()?
                .event_follower(&self.ca_pin()?, self.settings.handshake_timeout)?,
            Transport::Plain => {
                EventFollower::open_with(&self.endpoint()?, self.settings.handshake_timeout)?
            }
        };
        let baseline = follower.newest_seq();
        let stop = Notify::new();
        let (sender, receiver) = oneshot::channel();
        std::thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                if let Ok(Some(events)) = follower.wait_for_or_stop(
                    |events| early_exit(events, baseline).is_some(),
                    self.settings.build_timeout,
                    &stop,
                ) && let Some(event) = early_exit(events, baseline)
                {
                    let _ = sender.send(describe_early_exit(event));
                }
            });
            let result = self.bazel.build_until(remote, &labels, receiver);
            stop.notify_one();
            // The follower is an accelerator, not a verdict, even if its thread fails.
            let _ = watcher.join();
            result
        })
    }

    /// Cleans the Bazel output for `remote`, so that the next build reaches it again.
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    pub(crate) fn run_clean(&self, remote: &Remote) -> Result<()> {
        self.bazel.clean(remote)
    }

    /// The argument that marks the orphan process of this stack's hold action.
    fn orphan_marker(&self) -> String {
        format!("e2e-orphan-{}", self.salt)
    }

    /// Logs the developer of this deployment in through the real client, once, with a workload
    /// token minted by the in-process issuer. A restarted server keeps that login.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the token cannot be minted or written, or the login fails.
    fn enroll_developer(&self, pin: CaPin) -> Result<()> {
        let mut enrolled = self.developer.lock();
        if *enrolled {
            return Ok(());
        }
        let token = self
            .issuer
            .mint(&TokenSpec::admitted("developer"))
            .map_err(E2eError::system_from("minting the developer token"))?;
        let token_file = self.placement.home().join("developer.token");
        secured::write_secret(&token_file, token.as_str())?;
        let captured = process::run(
            self.cli
                .login_workload(&self.endpoint()?, &pin, &token_file),
            None,
            self.settings.command_timeout,
        )?;
        if !captured.succeeded() {
            return Err(E2eError::system(format!(
                "enrolling the developer failed ({}): {}",
                captured
                    .code
                    .map_or_else(|| "a signal".to_owned(), |code| format!("status {code}")),
                captured.combined()
            )));
        }
        *enrolled = true;
        Ok(())
    }
}

/// Whether `line` is the startup line of a server: its CA pin, or its plaintext endpoint.
pub(crate) fn is_announcement(line: &str) -> bool {
    matches!(
        line.trim().parse::<Announcement>(),
        Ok(Announcement::CaPin(_) | Announcement::Plaintext(_)),
    )
}

/// The listeners a server announced before its startup line: the addresses it bound.
#[derive(Debug)]
struct Listeners {
    /// The address of the `public` listener, which Bazel and `gang login` dial.
    public: SocketAddr,
    /// The address of the `worker` listener, which workers dial.
    worker: SocketAddr,
}

impl Listeners {
    /// The endpoint of the `public` listener: its address under the transport's endpoint scheme.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the endpoint is not a URL.
    fn endpoint(&self, transport: Transport) -> Result<Url> {
        let public = self.public;
        Url::parse(&format!("{}://{public}", transport.endpoint_scheme())).map_err(|error| {
            E2eError::system_from(format!("the announced endpoint {public} is not a URL"))(error)
        })
    }
}

/// The listeners of a server, derived from the lines it prints before its startup line. A
/// `worker` listener is required; an `imds` listener is optional and ignored.
///
/// # Errors
///
/// [`E2eError::System`] when a listener line is malformed or repeats a role, or when `public` or
/// `worker` is missing.
fn announced_listeners(lines: &[String]) -> Result<Listeners> {
    let mut public = None;
    let mut worker = None;
    for line in lines {
        let announcement = match line.trim().parse::<Announcement>() {
            Ok(announcement) => announcement,
            Err(error) if line.trim().starts_with("listening") => {
                return Err(E2eError::system_from(format!(
                    "malformed listener announcement \"{line}\""
                ))(error));
            }
            Err(_) => continue,
        };
        let Announcement::Listening(role, address) = announcement else {
            continue;
        };
        let slot = match role {
            ListenerRole::Public => &mut public,
            ListenerRole::Worker => &mut worker,
            ListenerRole::Imds => continue,
        };
        if slot.replace(address).is_some() {
            return Err(E2eError::system(format!(
                "the server announced its {role} listener twice"
            )));
        }
    }
    let worker = worker
        .ok_or_else(|| E2eError::system("the server did not announce its worker listener"))?;
    let public = public
        .ok_or_else(|| E2eError::system("the server did not announce its public listener"))?;
    Ok(Listeners { public, worker })
}

/// The tail of the server log, as lossy text. A log that cannot be read is described instead.
fn log_tail(path: &Path, limit: usize) -> String {
    match fs::read(path) {
        Ok(bytes) => {
            let start = bytes.len().saturating_sub(limit);
            let tail = bytes.get(start..).unwrap_or_default();
            String::from_utf8_lossy(tail).into_owned()
        }
        Err(error) => format!("unreadable ({error})"),
    }
}

/// Whether one of the arguments of `entry` contains `marker`.
fn carries(entry: &ProcessEntry, marker: &str) -> bool {
    entry.args.iter().any(|arg| arg.contains(marker))
}

/// The trimmed contents of the file at `path`.
fn read_trimmed(path: &Path) -> Result<String> {
    fs::read_to_string(path)
        .map(|text| text.trim().to_owned())
        .map_err(E2eError::io(format!("reading {}", path.display())))
}

impl System for Stack<'_> {
    fn start_server(&self) -> Result<()> {
        // The administrator and the API client reach the server a previous start announced,
        // so both are dropped.
        *self.administrator.lock() = None;
        *self.server_http.lock() = None;
        let mut slot = self.server.lock();
        if slot.is_some() {
            return Err(E2eError::system("the gang server is already running"));
        }
        let log = self.placement.server_log();
        let mut command = self.cli.server();
        let mut child = Supervised::spawn(&mut command, &log)?;
        // The server prints the addresses it bound, then its startup line: its CA pin, or its
        // plaintext endpoint. That line is the event that the deployment accepts connections.
        // The wait ends with it, or when the server's output closes, which happens when it exits.
        let Some(lines) =
            child.await_announcements(self.settings.startup_timeout, is_announcement)?
        else {
            return Err(E2eError::system(format!(
                "the gang server exited before announcing itself; log tail: {}",
                log_tail(&log, self.settings.log_tail_bytes)
            )));
        };
        let listeners = announced_listeners(&lines)?;
        for (announced, port) in [
            (listeners.public, self.placement.public_port()),
            (listeners.worker, self.placement.worker_port()),
        ] {
            let reserved = SocketAddr::from(([127, 0, 0, 1], port));
            if announced != reserved {
                return Err(E2eError::system(format!(
                    "the server bound {announced} instead of the reserved {reserved}"
                )));
            }
        }
        let endpoint = listeners.endpoint(self.transport)?;
        let announced = lines.join("\n");
        let pinned = self
            .ca_pin
            .is_none_or(|pin| last_ca_pin(&announced) == Some(pin));
        if !pinned {
            return Err(E2eError::system(format!(
                "the gang server did not announce the provisioned ca-pin; it announced \"{announced}\"; log tail: {}",
                log_tail(&log, self.settings.log_tail_bytes)
            )));
        }
        *slot = Some(child);
        drop(slot);
        *self.worker.lock() = Some(listeners.worker);
        *self.endpoint.lock() = Some(endpoint);
        secured::render_deployment_bazelrc(
            &self.binary,
            &self.config_file,
            &self.endpoint()?,
            self.ca_pin,
            &self.placement.workspace(),
            &self.placement.home(),
            self.settings.command_timeout,
        )?;
        if let Some(pin) = self.ca_pin {
            self.enroll_developer(pin)?;
        }
        Ok(())
    }

    fn kill_server(&self) -> Result<()> {
        *self.administrator.lock() = None;
        *self.server_http.lock() = None;
        *self.endpoint.lock() = None;
        *self.worker.lock() = None;
        let child = self.server.lock().take();
        match child {
            Some(mut child) => child.kill(),
            None => Err(E2eError::system("the gang server is not running")),
        }
    }

    /// The pid of the stored server, or `None` when there is none or it has exited. A status
    /// that cannot be read counts as not running.
    fn server_pid(&self) -> Option<u32> {
        let mut slot = self.server.lock();
        let child = slot.as_mut()?;
        match child.has_exited() {
            Ok(false) => Some(child.pid()),
            _ => None,
        }
    }

    fn worker_count(&self) -> Result<usize> {
        let entries = process::snapshot();
        Ok(process::workers_of(&entries, &self.config_file).len())
    }

    fn scaling_events(&self) -> Result<Vec<Event>> {
        let follower = self
            .administrator()?
            .event_follower(&self.ca_pin()?, self.settings.handshake_timeout)?;
        Ok(follower.events().to_vec())
    }

    fn anonymous_admin_status(&self) -> Result<u16> {
        if self.endpoint()?.scheme() != "http" {
            return Err(E2eError::system(
                "an anonymous admin status is observed only on a plaintext deployment",
            ));
        }
        StateApi::anonymous(&self.endpoint()?, None, self.settings.handshake_timeout)?
            .status(LIST_EVENTS.template)
    }

    fn live_state(&self) -> Result<State> {
        self.state_api()?.state()
    }

    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>> {
        let mut stream = self.state_api()?.open_stream()?;
        if stream.status() != STREAM_OK {
            return Err(E2eError::system(format!(
                "the state stream answered {}, not {STREAM_OK}",
                stream.status()
            )));
        }
        sse::await_frames(&mut stream, self.settings.history_timeout, done)
    }

    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome> {
        self.run_build(&self.remote()?, probes)
    }

    fn clean(&self) -> Result<()> {
        self.bazel.clean(&self.remote()?)
    }

    fn report(&self, probe: Probe) -> Result<ProbeReport> {
        ProbeReport::from_file(&self.placement.workspace().join(probe.report_path()))
    }

    fn host(&self) -> &Host {
        &self.host
    }

    fn tmp_path(&self) -> &Path {
        &self.tmp_path
    }

    fn stderr_tail_lines(&self) -> usize {
        self.settings.stderr_tail_lines
    }

    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>> {
        let mut follower = self
            .administrator()?
            .event_follower(&self.ca_pin()?, self.settings.handshake_timeout)?;
        follower
            .wait_for(done, self.settings.history_timeout)
            .map(<[Event]>::to_vec)
    }
}

impl Limits for Stack<'_> {
    fn await_hold(&self) -> Result<crate::Signalled> {
        self.signal.wait(self.settings.hold_timeout)
    }

    fn abandon_hold(&self) -> Result<()> {
        self.signal.notify()
    }

    fn running_scope(&self) -> Result<Option<ScopeFacts>> {
        let marker = self.orphan_marker();
        let entries = process::snapshot();
        let Some(orphan) = entries.iter().find(|entry| carries(entry, &marker)) else {
            return Ok(None);
        };
        let pid = orphan.pid;
        // A process that vanishes between these reads has no scope left to observe.
        let Ok(cgroup) = fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
            return Ok(None);
        };
        let Ok(working_directory) = fs::read_link(format!("/proc/{pid}/cwd")) else {
            return Ok(None);
        };
        let path = cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .ok_or_else(|| E2eError::system(format!("process {pid} has no unified cgroup")))?;
        let directory = self.settings.cgroup_root.join(path.trim_start_matches('/'));
        Ok(Some(ScopeFacts {
            memory_max: read_trimmed(&directory.join("memory.max"))?,
            oom_group: read_trimmed(&directory.join("memory.oom.group"))?,
            cpu_max: read_trimmed(&directory.join("cpu.max"))?,
            pids_max: read_trimmed(&directory.join("pids.max"))?,
            directory,
            working_directory,
        }))
    }

    fn release(&self, scope: &ScopeFacts) -> Result<()> {
        let release = scope.working_directory.join("release");
        fs::write(&release, "").map_err(E2eError::io(format!("writing {}", release.display())))
    }

    fn orphan_alive(&self) -> Result<bool> {
        let marker = self.orphan_marker();
        Ok(process::snapshot()
            .iter()
            .any(|entry| carries(entry, &marker)))
    }

    fn scope_exists(&self, scope: &ScopeFacts) -> Result<bool> {
        Ok(scope.directory.exists())
    }

    fn attempts(&self) -> Result<usize> {
        let path = crate::flags::attempts_path(&self.signal_path);
        match std::fs::read_to_string(&path) {
            Ok(text) => Ok(text.lines().count()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(E2eError::Io {
                operation: format!("reading the attempts {}", path.display()),
                source: error,
            }),
        }
    }
}

impl Drop for Stack<'_> {
    /// Kills the server, terminates the workers of this configuration, and stops the Bazel
    /// server. Failures are ignored: this runs on the way out of a scenario, whatever its outcome.
    fn drop(&mut self) {
        let child = self.server.lock().take();
        if let Some(mut child) = child {
            let _ = child.kill();
        }
        let _ = process::terminate_workers(&self.config_file);
        if let Ok(remote) = self.remote() {
            self.bazel.shutdown(&remote);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announcement(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|line| (*line).to_owned()).collect()
    }

    #[test]
    fn endpoint_uses_the_public_listener_and_the_transport_scheme() {
        let lines = announcement(&[
            "listening: public 127.0.0.1:43211",
            "listening: worker 127.0.0.1:43212",
            "listening: imds 127.0.0.1:43213",
            "ca-pin: announced",
        ]);
        let listeners = announced_listeners(&lines).expect("the announcement is complete");
        let tls = listeners
            .endpoint(Transport::Tls)
            .expect("the endpoint is a URL");
        assert_eq!(tls.as_str(), "https://127.0.0.1:43211/");
        let plain = listeners
            .endpoint(Transport::Plain)
            .expect("the endpoint is a URL");
        assert_eq!(plain.as_str(), "http://127.0.0.1:43211/");
    }

    #[test]
    fn a_missing_worker_listener_is_an_error() {
        let lines = announcement(&["listening: public 127.0.0.1:43211", "plaintext: 127.0.0.1"]);
        let error = announced_listeners(&lines).expect_err("the worker listener is required");
        assert!(matches!(&error, E2eError::System { context, .. } if context.contains("worker")));
    }

    #[test]
    fn a_missing_public_listener_is_an_error() {
        let lines = announcement(&["listening: worker 127.0.0.1:43212", "plaintext: 127.0.0.1"]);
        let error = announced_listeners(&lines).expect_err("the public listener is required");
        assert!(matches!(&error, E2eError::System { context, .. } if context.contains("public")));
    }
}
