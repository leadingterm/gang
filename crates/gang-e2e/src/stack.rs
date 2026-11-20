//! The real deployment: one `gang` server, its workers, and the Bazel workspace that uses them.
//!
//! [`Stack::create`] writes the configuration, provisions the signer key, stages the sample, and
//! prepares the commands that drive the deployment.
//! The server is started by [`System::start_server`], and everything the stack started is torn
//! down when it is dropped.

use std::cell::Cell;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gang_clock::{Clock as _, SystemClock};
use gang_tls::{HttpProtocol, HttpTransport, ServerTrust};
use gang_types::announcement::{Announcement, ListenerRole, last_ca_pin};
use gang_types::{CaPin, Url};
use gang_wire::routes::LIST_EVENTS;
use gang_wire::{Event, Frame, State};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

use crate::administrator::Administrator;
use crate::bazel::Bazel;
use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::event_log::{EventFollower, describe_early_exit, early_exit};
use crate::execution::Run;
use crate::flags::Remote;
use crate::gang_cli::GangCli;
use crate::held_build::Cancel;
use crate::host::Host;
use crate::http::Http;
use crate::identity::Credentials;
use crate::issuer::{TestIssuer, TokenSpec};
use crate::placement::Placement;
use crate::process::{self, ProcessEntry, Supervised};
use crate::report::ProbeReport;
use crate::sample::{self, SampleParameters};
use crate::secured;
use crate::session::{self, SessionCookie};
use crate::settings::{DeploymentInputs, Settings};
use crate::signer_key;
use crate::sse::{self, EventStream as _, STREAM_OK};
use crate::state_api::StateApi;
use crate::system::{
    AttemptRecord, HOLD_CPU_COUNT, HOLD_MEMORY_KB, LARGE_FLEET, Limits, PressureObservation, Probe,
    Sandbox, ScopeFacts, System, Variant,
};
use crate::template::{ConfigTemplate, Transport};

pub(crate) const DEMAND_ROOT: &str = "demand-bazel";
pub(crate) const FOLLOWER_ROOT: &str = "follower-bazel";

thread_local! {
    /// Placements of the scenario running on this thread, kept alive until its verdict is known;
    /// `None` outside [`with_failure_evidence`].
    static EVIDENCE_PLACEMENTS: Cell<Option<Vec<(Placement, usize)>>> = const { Cell::new(None) };
}

/// Runs a scenario and prints evidence for every placement before removing it on failure.
pub(crate) fn with_failure_evidence(run: impl FnOnce() -> Vec<Check>) -> Vec<Check> {
    EVIDENCE_PLACEMENTS.set(Some(Vec::new()));
    let checks = run();
    let placements = EVIDENCE_PLACEMENTS.take();
    if checks.iter().any(Check::is_failure) {
        for (placement, limit) in placements.into_iter().flatten() {
            println!("{}", failure_evidence(&placement, limit));
        }
    }
    checks
}

/// Formats the server and local worker log tails for a placement.
fn failure_evidence(placement: &Placement, limit: usize) -> String {
    let server_log = placement.server_log();
    format!(
        "server log tail {}:\n{}\nlocal worker log tails:\n{}",
        server_log.display(),
        log_tail(&server_log, limit),
        local_evidence(&placement.state_dir().join("logs"), false, limit)
    )
}

/// Creates the private signal directory and its hold-probe FIFO.
fn signal_in(directory: &Path) -> Result<(PathBuf, crate::hold::Signal)> {
    gang_os::fs::ensure_private_dir(directory)
        .map_err(E2eError::system_from("creating the signal directory"))?;
    for name in ["attempts", "go", "churn"] {
        let path = directory.join(name);
        fs::write(&path, b"").map_err(E2eError::io(format!("creating {}", path.display())))?;
    }
    let path = directory.join("started");
    let signal = crate::hold::Signal::create(&path)?;
    Ok((path, signal))
}

pub(crate) fn sample_parameters(
    invocation: &DeploymentInputs,
    variant: &Variant,
    salt: &str,
    signal_path: &Path,
    isa: gang_types::Isa,
    large_memory_kb: u64,
) -> SampleParameters {
    SampleParameters {
        salt: salt.to_owned(),
        action_path: if variant.sandbox == Sandbox::Bwrap {
            "/bin:/usr/bin".to_owned()
        } else {
            invocation.action_tools.path.clone()
        },
        action_shell: if variant.sandbox == Sandbox::Bwrap {
            PathBuf::from("/bin/bash")
        } else {
            invocation.action_tools.shell.clone()
        },
        action_ipc: variant.sandbox == Sandbox::Bwrap,
        host_action_path: invocation.action_tools.path.clone(),
        host_action_shell: invocation.action_tools.shell.clone(),
        signal: signal_path.to_path_buf(),
        attempts: crate::flags::attempts_path(signal_path),
        isa,
        memory_kb: HOLD_MEMORY_KB,
        cpu_count: HOLD_CPU_COUNT,
        large_fleet: LARGE_FLEET.to_owned(),
        large_memory_kb,
        requested_sandbox: Sandbox::Noop,
        glibc_tool: invocation.action_tools.glibc_tool.clone(),
        egress_probe: invocation
            .action_tools
            .egress
            .as_ref()
            .map(|tools| tools.probe.clone()),
        egress_targets: invocation
            .action_tools
            .egress
            .as_ref()
            .and_then(|tools| tools.targets.clone()),
    }
}

#[cfg(target_os = "linux")]
fn start_action_ipc(
    placement: &Placement,
    sandbox: Sandbox,
    signal_dir: &Path,
    salt: &str,
) -> Result<Option<Arc<crate::action_ipc::ActionIpc>>> {
    if sandbox != Sandbox::Bwrap {
        return Ok(None);
    }
    let instances = placement.state_dir().join("instances");
    gang_os::fs::ensure_private_dir(&instances)
        .map_err(E2eError::system_from("creating instance directory"))?;
    Ok(Some(Arc::new(crate::action_ipc::ActionIpc::start(
        &instances, signal_dir, salt,
    )?)))
}

fn prepare_admin_directories(placement: &Placement, transport: Transport) -> Result<()> {
    if transport == Transport::Tls {
        for directory in [
            placement.home().join("admin"),
            placement.home().join("admin-workspace"),
        ] {
            fs::create_dir_all(&directory)
                .map_err(E2eError::io(format!("creating {}", directory.display())))?;
        }
    }
    Ok(())
}

fn configure_identity(
    template: ConfigTemplate,
    transport: Transport,
    issuer: &TestIssuer,
    variant: &Variant,
) -> Result<(ConfigTemplate, Option<CaPin>, Option<PathBuf>)> {
    match transport {
        Transport::Tls => {
            let template = template.with_identity(issuer.url(), variant.identity)?;
            let signer_key = template.signer_key_file()?;
            let ca_pin = signer_key::provision(&signer_key)?;
            Ok((
                template.with_ca_pin(&ca_pin)?,
                Some(ca_pin),
                Some(signer_key),
            ))
        }
        Transport::Plain => Ok((template, None, None)),
    }
}

#[cfg(target_os = "linux")]
fn populated(scope: &Path) -> Result<bool> {
    let events = scope.join("cgroup.events");
    let text = match fs::read_to_string(&events) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(E2eError::io(format!("reading {}", events.display()))(error)),
    };
    match text
        .lines()
        .find_map(|line| line.strip_prefix("populated "))
    {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err(E2eError::system(format!(
            "invalid populated state in {}",
            events.display()
        ))),
    }
}

#[cfg(target_os = "linux")]
fn process_exited(pid: u32) -> Result<bool> {
    let path = format!("/proc/{pid}/status");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(E2eError::io(format!("reading {path}"))(error)),
    };
    let state = text
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| E2eError::system(format!("missing process state in {path}")))?;
    Ok(state == "Z" || state == "X")
}

/// A running `gang` deployment and the Bazel workspace that builds against it.
pub struct Stack<'a> {
    placement: Placement,
    settings: Settings,
    /// Sustained memory pressure duration configured for this scenario.
    #[cfg(target_os = "linux")]
    pressure_kill_after: Option<Duration>,
    /// Parameters used to stage another copy of this deployment's sample.
    sample_parameters: SampleParameters,
    /// The sample workspace this deployment was staged from.
    sample_source: PathBuf,
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
    #[cfg(target_os = "linux")]
    action_ipc: Option<Arc<crate::action_ipc::ActionIpc>>,
    /// The launch gate path and its held writer, when enabled for this scenario.
    launch_gate: Option<(PathBuf, Mutex<Option<fs::File>>)>,
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
    /// The HTTP client of the running server's API, built on first use after its announcement.
    /// Its pooled connections reach one server process, so each start and kill drops it.
    server_http: Mutex<Option<Arc<Http>>>,
}

/// Owns a separate Bazel server from preparation through its last build or early return.
pub(crate) struct AdditionalBuild {
    driver: Bazel,
    remote: Remote,
    #[cfg(target_os = "linux")]
    action_ipc: Option<Arc<crate::action_ipc::ActionIpc>>,
}

impl AdditionalBuild {
    pub(crate) fn build_until(
        self,
        probes: &[Probe],
        run: &Run,
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        #[cfg(target_os = "linux")]
        if let Some(ipc) = &self.action_ipc {
            ipc.check()?;
        }
        let result = self
            .driver
            .build_until(&self.remote, probes, Some(run), stop);
        #[cfg(target_os = "linux")]
        if let Some(ipc) = &self.action_ipc {
            ipc.check()?;
        }
        // A failed or cancelled build can leave its server wedged.
        if result.is_ok() {
            self.driver.shutdown(&self.remote);
        }
        result
    }
}

impl Drop for AdditionalBuild {
    fn drop(&mut self) {
        // Also covers preparation failure, an unused prepared server, and failed shutdown.
        if let Err(error) = self.driver.kill_servers() {
            eprintln!("failed to kill the additional build's Bazel server: {error}");
        }
    }
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
        invocation: &DeploymentInputs,
        variant: &Variant,
        transport: Transport,
        issuer: &'a TestIssuer,
    ) -> Result<Self> {
        let settings = &invocation.settings;
        let placement = Placement::create(settings.scratch_root.as_deref())?;
        if let Some(mut placements) = EVIDENCE_PLACEMENTS.take() {
            placements.push((placement.clone(), settings.log_tail_bytes));
            EVIDENCE_PLACEMENTS.set(Some(placements));
        }

        let salt = sample::salt()?;
        let signal_dir = placement.root().join(format!("signal-{salt}"));
        let (signal_path, signal) = signal_in(&signal_dir)?;
        #[cfg(target_os = "linux")]
        let action_ipc = start_action_ipc(&placement, variant.sandbox, &signal_dir, &salt)?;

        let gate_path = variant
            .control
            .holdable_launches
            .then(|| signal_dir.join("launch-gate"));
        let template = ConfigTemplate::parse(&invocation.base_config)?
            .place(&placement, transport)?
            .vary(variant, &settings.worker_launcher, &invocation.action_tools)?
            .with_launch_gate(gate_path.as_deref(), settings.history_timeout)?;
        let launch_gate = gate_path.map(|path| (path, Mutex::new(None)));
        let (template, ca_pin, signer_key) =
            configure_identity(template, transport, issuer, variant)?;

        let isa = template.default_isa()?;
        let tmp_path = PathBuf::from("/tmp");
        let large_memory_kb = template.large_memory_kb()?;
        let rendered = template.render()?;
        let config_file = placement.config_file();
        fs::write(&config_file, rendered)
            .map_err(E2eError::io(format!("writing {}", config_file.display())))?;

        let parameters = sample_parameters(
            invocation,
            variant,
            &salt,
            &signal_path,
            isa,
            large_memory_kb,
        );
        sample::stage(&invocation.sample, &placement.workspace(), &parameters)?;

        prepare_admin_directories(&placement, transport)?;
        let mut bazel_tools = invocation.action_tools.clone();
        bazel_tools.shell.clone_from(&parameters.action_shell);

        Ok(Self {
            cli: GangCli::new(
                invocation.gang.clone(),
                config_file.clone(),
                placement.home(),
            ),
            bazel: Bazel::new(
                invocation.bazel.clone(),
                settings.clone(),
                bazel_tools,
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
            #[cfg(target_os = "linux")]
            action_ipc,
            launch_gate,
            salt,
            issuer,
            ca_pin,
            signer_key,
            developer: Mutex::new(false),
            host: Host::observe(),
            settings: settings.clone(),
            #[cfg(target_os = "linux")]
            pressure_kill_after: variant
                .execution
                .resource_policy
                .map(|policy| policy.memory.pressure_kill_after.get()),
            sample_parameters: parameters,
            sample_source: invocation.sample.clone(),
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

    /// The selected client binary, configuration and original credential home.
    pub(crate) const fn gang_cli(&self) -> &GangCli {
        &self.cli
    }

    pub(crate) fn use_client_config(&mut self, config: PathBuf) {
        self.cli = GangCli::new(self.binary.clone(), config.clone(), self.placement.home());
        self.config_file = config;
    }
    /// Holds launches by keeping a private FIFO open until release.
    pub(crate) fn hold_worker_launches(&self) -> Result<()> {
        let (path, held) = self
            .launch_gate
            .as_ref()
            .ok_or_else(|| E2eError::system("worker launch gate is not enabled"))?;
        let mut held = held.lock();
        if held.is_some() {
            return Err(E2eError::system("worker launches are already held"));
        }
        let pending = path.with_extension("pending");
        let file = crate::fifo::create_private(&pending, "the launch gate")?;
        if let Err(error) = fs::rename(&pending, path) {
            let _ = fs::remove_file(&pending);
            return Err(E2eError::io(format!("publishing {}", path.display()))(
                error,
            ));
        }
        *held = Some(file);
        Ok(())
    }

    /// Unlinks the gate before closing its writer, broadcasting EOF to every waiting launcher.
    pub(crate) fn release_worker_launches(&self) -> Result<()> {
        let (path, held) = self
            .launch_gate
            .as_ref()
            .ok_or_else(|| E2eError::system("worker launch gate is not enabled"))?;
        let mut held = held.lock();
        if held.is_some() {
            fs::remove_file(path).map_err(E2eError::io(format!("removing {}", path.display())))?;
            held.take();
        }
        Ok(())
    }

    /// Stages another copy of the sample this deployment was staged from, with the same parameters.
    pub(crate) fn stage_sample(&self, destination: &Path) -> Result<()> {
        sample::stage(&self.sample_source, destination, &self.sample_parameters)
    }

    /// The Bazel driver associated with this deployment.
    pub(crate) const fn bazel(&self) -> &Bazel {
        &self.bazel
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

    /// How a build made as the deployment's administrator reaches the executor: the rc file is
    /// rendered into the administrator's home.
    pub(crate) fn administrator_remote(&self, credentials: &Credentials) -> Result<Remote> {
        let endpoint = self.endpoint()?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?
            .to_owned();
        let bazelrc = secured::render_deployment_bazelrc(
            &self.binary,
            &self.config_file,
            &endpoint,
            self.ca_pin,
            &self.placement.workspace(),
            &credentials.home,
            self.settings.command_timeout,
        )?;
        Ok(Remote::Gang {
            host,
            bazelrc,
            home: credentials.home.clone(),
            helper: self.binary.clone(),
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

    /// Reuses the running server's plaintext or pinned TLS client across startup and API calls.
    pub(crate) fn server_client(&self) -> Result<Arc<Http>> {
        let mut slot = self.server_http.lock();
        if let Some(client) = slot.as_ref() {
            return Ok(Arc::clone(client));
        }
        let transport = match self.ca_pin.as_ref() {
            Some(pin) => HttpTransport::Tls {
                trust: ServerTrust::Pin(pin),
            },
            None => HttpTransport::Plain,
        };
        let http = Arc::new(Http::try_new(
            transport,
            HttpProtocol::Http2,
            self.settings.command_timeout,
        )?);
        *slot = Some(Arc::clone(&http));
        Ok(http)
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
    /// Connects the existing client machinery to a separately supervised deployment.
    pub(crate) fn connect_external(&self, endpoint: Url) -> Result<()> {
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

    /// Runs a build until it finishes or the caller sends an abort reason.
    pub(crate) fn build_until_stopped(
        &self,
        probes: &[Probe],
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        self.bazel.build_until(&self.remote()?, probes, None, stop)
    }

    /// Kills the Bazel server after its launcher was aborted, abandoning the build it still runs.
    pub(crate) fn kill_bazel(&self) -> Result<()> {
        self.bazel.kill_servers()
    }

    /// Kills the Bazel server for the stack or a distinct concurrent-build output root.
    pub(crate) fn kill_bazel_in(&self, root: Option<&str>) -> Result<()> {
        match root {
            None => self.bazel.kill_servers(),
            Some(root) => self
                .bazel
                .with_output_user_root(self.placement.root().join(root))
                .kill_servers(),
        }
    }

    /// Waits for the state reconstructed from the stream to satisfy the condition.
    pub(crate) fn wait_live_state(
        &self,
        timeout: Duration,
        mut done: impl FnMut(&State) -> bool,
    ) -> Result<State> {
        let mut stream = self.state_api()?.open_stream()?;
        if stream.status() != STREAM_OK {
            return Err(E2eError::system(format!(
                "the state stream answered {}",
                stream.status()
            )));
        }
        sse::await_state(&mut stream, timeout, &mut done)
    }

    /// A concurrent build needs its own Bazel server; one output base serializes invocations.
    pub(crate) fn build_additional(
        &self,
        probes: &[Probe],
        run: &Run,
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        self.build_additional_in(probes, run, DEMAND_ROOT, stop)
    }

    /// Runs another concurrent build in a distinct Bazel output root.
    pub(crate) fn build_additional_in(
        &self,
        probes: &[Probe],
        run: &Run,
        output_root: &str,
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        self.additional_build_in(output_root)?
            .build_until(probes, run, stop)
    }

    /// Starts only the stack's Bazel server; the stack retains its cleanup ownership.
    pub(crate) fn prepare_bazel(&self) -> Result<()> {
        self.bazel.prepare(&self.remote()?)
    }

    /// Starts only the separate Bazel server, without loading or executing probe actions.
    pub(crate) fn prepare_additional_in(&self, output_root: &str) -> Result<AdditionalBuild> {
        let build = self.additional_build_in(output_root)?;
        build.driver.prepare(&build.remote)?;
        Ok(build)
    }

    fn additional_build_in(&self, output_root: &str) -> Result<AdditionalBuild> {
        Ok(AdditionalBuild {
            remote: self.remote()?,
            driver: self
                .bazel
                .with_output_user_root(self.placement.root().join(output_root)),
            #[cfg(target_os = "linux")]
            action_ipc: self.action_ipc.clone(),
        })
    }

    /// Runs the rootfs test and requires its uncached remote test result in the BEP.
    pub(crate) fn rootfs_test(&self) -> Result<Check> {
        let bep = self.placement.root().join("rootfs-test.bep");
        let outcome = self.bazel.rootfs_test(&self.remote()?, &bep)?;
        #[cfg(target_os = "linux")]
        if let Some(ipc) = &self.action_ipc {
            ipc.check()?;
        }
        let events = fs::read_to_string(&bep).map_err(E2eError::io("reading rootfs test BEP"))?;
        let mut passed_remote = false;
        for line in events.lines() {
            let event: serde_json::Value = serde_json::from_str(line)
                .map_err(E2eError::system_from("decoding rootfs test BEP"))?;
            if event
                .pointer("/id/testResult/label")
                .and_then(serde_json::Value::as_str)
                == Some("//:rootfs_sh_test")
                && event
                    .pointer("/testResult/status")
                    .and_then(serde_json::Value::as_str)
                    == Some("PASSED")
                && event
                    .pointer("/testResult/executionInfo/strategy")
                    .and_then(serde_json::Value::as_str)
                    == Some("remote")
                && event
                    .pointer("/testResult/cachedLocally")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
                && event
                    .pointer("/testResult/executionInfo/cachedRemotely")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                passed_remote = true;
            }
        }
        Ok(Check::expect(
            "rootfs Bazel test executes remotely",
            outcome.succeeded() && outcome.processes.remote() > 0 && passed_remote,
            || {
                format!(
                    "rootfs test lacked an uncached remote passing BEP result: {}",
                    outcome.exit_summary()
                )
            },
        ))
    }

    /// Builds `probes` through `remote`, as `run` when there is one, returning how the build ended.
    /// A follower accelerates failure on a newly recorded early exit; its own errors do not
    /// change the build result.
    ///
    /// # Errors
    ///
    /// When the event follower cannot be opened or Bazel cannot be run.
    pub(crate) fn run_build(
        &self,
        remote: &Remote,
        probes: &[Probe],
        run: Option<&Run>,
    ) -> Result<BuildOutcome> {
        let (cancel, stop) = Cancel::pair();
        self.run_build_until(remote, probes, run, &cancel, stop)
    }

    /// Runs a build with caller-owned cancellation and early-exit observation.
    pub(crate) fn run_build_until(
        &self,
        remote: &Remote,
        probes: &[Probe],
        run: Option<&Run>,
        cancel: &Cancel,
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        #[cfg(target_os = "linux")]
        if let Some(ipc) = &self.action_ipc {
            ipc.check()?;
        }
        let mut follower = match self.transport {
            Transport::Tls => self
                .administrator()?
                .event_follower(&self.ca_pin()?, self.settings.handshake_timeout)?,
            Transport::Plain => {
                EventFollower::open_with(&self.endpoint()?, self.settings.handshake_timeout)?
            }
        };
        let baseline = follower.newest_seq();
        let watcher_stop = Notify::new();
        let result = std::thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                if let Ok(Some(events)) = follower.wait_for_or_stop(
                    |events| early_exit(events, baseline).is_some(),
                    self.settings.build_timeout,
                    &watcher_stop,
                ) && let Some(event) = early_exit(events, baseline)
                {
                    cancel.fire(&describe_early_exit(event));
                }
            });
            let result = self.bazel.build_until(remote, probes, run, stop);
            watcher_stop.notify_one();
            // The follower is an accelerator, not a verdict, even if its thread fails.
            let _ = watcher.join();
            result
        });
        #[cfg(target_os = "linux")]
        if let Some(ipc) = &self.action_ipc {
            ipc.check()?;
        }
        result
    }

    /// Runs a named build with caller-owned cancellation.
    pub(crate) fn build_run_until(
        &self,
        probes: &[Probe],
        run: &Run,
        cancel: &Cancel,
        stop: oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        self.run_build_until(&self.remote()?, probes, Some(run), cancel, stop)
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
    #[cfg(target_os = "linux")]
    fn pressure_number(&self, path: &Path) -> Result<u64> {
        let value = match read_trimmed(path) {
            Ok(value) => value,
            Err(E2eError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                let logs = local_evidence(
                    &self.placement.state_dir().join("logs"),
                    false,
                    self.settings.log_tail_bytes,
                );
                return Err(E2eError::system(format!(
                    "pressure action scope vanished before the observation at {}; worker logs:\n{logs}",
                    path.display()
                )));
            }
            Err(error) => return Err(error),
        };
        value
            .parse()
            .map_err(|_| E2eError::system(format!("invalid number in {}", path.display())))
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
            ListenerRole::Health | ListenerRole::Imds => continue,
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

/// Reads instance records with secrets redacted or per-worker log tails, reporting failures.
pub(crate) fn local_evidence(directory: &Path, records: bool, limit: usize) -> String {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => return format!("{}: {error}", directory.display()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = if records {
                    entry.path().join("instance.json")
                } else {
                    entry.path()
                };
                if records || path.extension().is_some_and(|extension| extension == "log") {
                    paths.push(path);
                }
            }
            Err(error) => return format!("{}: {error}", directory.display()),
        }
    }
    paths.sort();
    if paths.is_empty() {
        return format!("{}: no entries", directory.display());
    }
    paths
        .into_iter()
        .map(|path| {
            let content = match fs::read(&path) {
                Ok(bytes) if records => redacted_record(&bytes),
                Ok(bytes) => crate::observer::tail(&bytes, limit),
                Err(error) => format!("unreadable: {error}"),
            };
            format!("{}:\n{content}", path.display())
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redacted_record(bytes: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(serde_json::Value::Object(mut record)) => {
            if let Some(secret) = record.get_mut("secret") {
                *secret = serde_json::Value::String("<redacted>".into());
            }
            serde_json::Value::Object(record).to_string()
        }
        Ok(_) => "unreadable: instance record is not a JSON object".to_owned(),
        Err(_) => "unreadable: invalid instance record JSON".to_owned(),
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
        let started = Instant::now();
        let remaining_startup = || {
            self.settings
                .startup_timeout
                .checked_sub(started.elapsed())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| {
                    E2eError::timeout(
                        "the server to start and admit client traffic",
                        self.settings.startup_timeout,
                    )
                })
        };
        let mut child = Supervised::spawn(&mut command, &log)?;
        // The CA pin or plaintext endpoint announces the warm launch milestone, not promotion.
        // Record the child and its listeners before waiting for client traffic admission.
        // The announcement wait ends when stdout closes if the server exits first.
        let Some(lines) = child.await_announcements(remaining_startup()?, is_announcement)? else {
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
        let client = self.server_client()?;
        client.wait_for_client_traffic(&self.endpoint()?, remaining_startup()?)?;
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
        self.run_build(&self.remote()?, probes, None)
    }

    fn build_run(&self, probes: &[Probe], run: &Run) -> Result<BuildOutcome> {
        self.run_build(&self.remote()?, probes, Some(run))
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

    fn cancel_hold(&self) -> Result<()> {
        self.kill_bazel()
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

    fn attempt_records(&self) -> Result<Vec<AttemptRecord>> {
        let path = crate::flags::attempts_path(&self.signal_path);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(E2eError::Io {
                    operation: format!("reading the attempts {}", path.display()),
                    source,
                });
            }
        };
        text.lines()
            .map(|line| {
                let mut fields = line.split_whitespace();
                let (Some(action), Some(memory), None) =
                    (fields.next(), fields.next(), fields.next())
                else {
                    return Err(E2eError::system(format!("invalid attempt record: {line}")));
                };
                let memory_kb = memory.parse().map_err(|_| {
                    E2eError::system(format!("invalid attempt reservation: {line}"))
                })?;
                Ok(AttemptRecord {
                    action: action.to_owned(),
                    memory_kb,
                })
            })
            .collect()
    }

    #[cfg(target_os = "linux")]
    fn induce_pressure(&self, victim_pid: u32, neighbour_pid: u32) -> Result<PressureObservation> {
        use std::io::Write as _;
        use std::time::Instant;

        let scope = |pid: u32| -> Result<PathBuf> {
            let path = format!("/proc/{pid}/cgroup");
            let text =
                fs::read_to_string(&path).map_err(E2eError::io(format!("reading {path}")))?;
            let relative = text
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .ok_or_else(|| E2eError::system(format!("process {pid} has no unified cgroup")))?;
            Ok(self
                .settings
                .cgroup_root
                .join(relative.trim_start_matches('/')))
        };
        let victim = scope(victim_pid)?;
        let neighbour = scope(neighbour_pid)?;
        let pool = victim
            .parent()
            .ok_or_else(|| E2eError::system("victim has no pool"))?;
        if neighbour.parent() != Some(pool) {
            return Err(E2eError::system(
                "pressure probes are not in the same action pool",
            ));
        }
        let settle = self
            .pressure_kill_after
            .ok_or_else(|| E2eError::system("pressure scenario has no resource policy"))?
            .saturating_mul(3);
        let hold_timeout = self.settings.hold_timeout.max(settle);
        let deadline = Instant::now() + hold_timeout;
        let mut previous_peak = None;
        let mut stable_peak = None;
        while Instant::now() < deadline {
            let held = self.pressure_number(&victim.join("memory.current"))?
                >= self.settings.pressure_victim_minimum.as_u64()
                && self.pressure_number(&neighbour.join("memory.current"))?
                    >= self.settings.pressure_neighbour_minimum.as_u64();
            let peak = self.pressure_number(&victim.join("memory.peak"))?;
            if held && previous_peak == Some(peak) {
                stable_peak = Some(peak);
                break;
            }
            previous_peak = held.then_some(peak);
            std::thread::sleep(self.settings.pressure_observe_interval);
        }
        let peak = stable_peak.ok_or_else(|| {
            E2eError::system("pressure probes did not reach stable held allocations")
        })?;
        let high = pool.join("memory.high");
        let original = read_trimmed(&high)?;
        let victim_current = self.pressure_number(&victim.join("memory.current"))?;
        let neighbour_current = self.pressure_number(&neighbour.join("memory.current"))?;
        let high_bytes = neighbour_current
            .checked_add(victim_current / 2)
            .ok_or_else(|| E2eError::system("pressure threshold exceeds u64"))?;
        let fd = rustix::fs::open(
            &high,
            rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::NONBLOCK,
            rustix::fs::Mode::empty(),
        )
        .map_err(E2eError::system_from("opening memory.high"))?;
        let mut file = std::fs::File::from(fd);
        write!(file, "{high_bytes}").map_err(E2eError::io("lowering memory.high"))?;
        let churn = self.signal_path.with_file_name("churn");
        if let Err(error) = fs::write(&churn, b"1") {
            fs::write(&high, &original).map_err(E2eError::io("restoring memory.high"))?;
            return Err(E2eError::io(format!("creating {}", churn.display()))(error));
        }
        let deadline = Instant::now() + hold_timeout;
        let victim_killed = (|| -> Result<bool> {
            loop {
                if process_exited(victim_pid)? {
                    return Ok(true);
                }
                if Instant::now() >= deadline {
                    return Ok(false);
                }
                std::thread::sleep(self.settings.pressure_observe_interval);
            }
        })();
        let restored = fs::write(&high, original).map_err(E2eError::io("restoring memory.high"));
        let removed =
            fs::write(&churn, b"").map_err(E2eError::io(format!("clearing {}", churn.display())));
        restored?;
        removed?;
        let victim_exited = victim_killed?;
        while victim_exited && populated(&victim)? && Instant::now() < deadline {
            std::thread::sleep(self.settings.pressure_observe_interval);
        }
        let victim_killed = victim_exited && !populated(&victim)?;
        std::thread::sleep(settle);
        let observed = PressureObservation {
            victim_peak_kb: peak.div_ceil(1024),
            victim_killed,
            neighbour_alive: populated(&neighbour)?,
        };
        Ok(observed)
    }

    #[cfg(not(target_os = "linux"))]
    fn induce_pressure(
        &self,
        _victim_pid: u32,
        _neighbour_pid: u32,
    ) -> Result<PressureObservation> {
        Err(E2eError::system("memory pressure requires Linux cgroups"))
    }

    #[cfg(target_os = "linux")]
    fn await_pool_oom(&self, victim_pid: u32, neighbour_pid: u32) -> Result<u64> {
        let pool = [victim_pid, neighbour_pid]
            .into_iter()
            .find_map(|pid| {
                let text = fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
                let relative = text.lines().find_map(|line| line.strip_prefix("0::"))?;
                let scope = self
                    .settings
                    .cgroup_root
                    .join(relative.trim_start_matches('/'));
                scope.parent().map(Path::to_path_buf)
            })
            .ok_or_else(|| E2eError::system("neither pool OOM probe has a cgroup"))?;
        let events = pool.join("memory.events");
        let deadline = std::time::Instant::now() + self.settings.hold_timeout;
        loop {
            let text = fs::read_to_string(&events)
                .map_err(E2eError::io(format!("reading {}", events.display())))?;
            let value = text
                .lines()
                .find_map(|line| line.strip_prefix("oom_kill "))
                .ok_or_else(|| E2eError::system("pool memory.events has no oom_kill count"))?;
            let count = value
                .parse::<u64>()
                .map_err(E2eError::system_from("parsing pool oom_kill count"))?;
            if count > 0 || std::time::Instant::now() >= deadline {
                return Ok(count);
            }
            std::thread::sleep(self.settings.pressure_observe_interval);
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn await_pool_oom(&self, _victim_pid: u32, _neighbour_pid: u32) -> Result<u64> {
        Err(E2eError::system(
            "pool OOM observation requires Linux cgroups",
        ))
    }
}

impl Drop for Stack<'_> {
    /// Kills the server, terminates workers, and stops the Bazel server.
    fn drop(&mut self) {
        let child = self.server.lock().take();
        let server_stopped = if let Some(mut child) = child {
            match child.kill() {
                Ok(()) => true,
                Err(error) => {
                    eprintln!("server cleanup for {}: {error}", self.config_file.display());
                    false
                }
            }
        } else {
            true
        };
        let workers_stopped = match process::terminate_workers(&self.config_file) {
            Ok(()) => true,
            Err(error) => {
                eprintln!("worker cleanup for {}: {error}", self.config_file.display());
                false
            }
        };
        if server_stopped
            && workers_stopped
            && self.launch_gate.is_some()
            && let Err(error) = self.release_worker_launches()
        {
            eprintln!(
                "launch gate cleanup for {}: {error}",
                self.config_file.display()
            );
        }
        if let Ok(remote) = self.remote() {
            self.bazel.shutdown(&remote);
        }
        #[cfg(target_os = "linux")]
        if let Some(ipc) = self.action_ipc.take()
            && let Ok(ipc) = Arc::try_unwrap(ipc)
            && let Err(error) = ipc.finish()
        {
            eprintln!(
                "action IPC cleanup for {}: {error}",
                self.config_file.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_evidence_contains_server_and_worker_log_tails() {
        let placement = Placement::create(None).expect("placement");
        let logs = placement.state_dir().join("logs");
        fs::create_dir_all(&logs).expect("worker log directory");
        fs::write(placement.server_log(), "prefix server-tail").expect("server log");
        fs::write(logs.join("one.log"), "prefix worker-one").expect("first worker log");
        fs::write(logs.join("two.log"), "prefix worker-two").expect("second worker log");

        let evidence = failure_evidence(&placement, 10);
        assert!(evidence.contains(&format!(
            "server log tail {}:",
            placement.server_log().display()
        )));
        assert!(evidence.contains("erver-tail"));
        assert!(evidence.contains("local worker log tails:"));
        assert!(evidence.contains(&format!("{}:", logs.join("one.log").display())));
        assert!(evidence.contains(&format!("{}:", logs.join("two.log").display())));
        assert!(evidence.contains("worker-one"));
        assert!(evidence.contains("worker-two"));
        assert!(!evidence.contains("prefix"));
    }

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
