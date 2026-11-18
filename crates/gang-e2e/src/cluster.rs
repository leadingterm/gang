//! A set of controllers sharing durable state behind loopback load balancers.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gang_clock::SystemClock;
use gang_testkit::{FakeDynamoDb, PostgresServer, SUPERUSER};
use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, ServerTrust};
use gang_types::{CaPin, Url};
use gang_wire::{Event, Frame, State};
use parking_lot::Mutex;
use rustix::process::{Pid, Signal, kill_process};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::balancer::{Balancer, Frontend, Probe as HealthProbe, Target};
use crate::build_output::BuildOutcome;
use crate::check::{Check, Outcome};
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::fake_s3::FakeS3;
use crate::gang_cli::GangCli;
use crate::host::Host;
use crate::issuer::TestIssuer;
use crate::ports;
use crate::process::{self, Supervised};
use crate::report::ProbeReport;
use crate::settings::DeploymentInputs;
use crate::stack::{Stack, is_announcement, local_evidence};
use crate::system::{Probe, System, Variant};
use crate::template::Transport;

const TABLE: &str = "gang-e2e";
const BUCKET: &str = "gang-e2e";
const REGION: &str = "us-east-1";
const DUMMY_CREDENTIAL: &str = "dummy";
const DISABLE_INSTANCE_METADATA: &str = "true";
const DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(3);
const CAPABILITIES_PATH: &str = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";
const DIAGNOSTIC_S3_KEYS: usize = 20;
const DIAGNOSTIC_S3_REQUESTS: usize = 30;

/// Sends one bounded HTTP/2 request over the deployment's pinned TLS transport.
fn probe_http(client: &crate::http::Http, address: SocketAddr, path: &str) -> Result<String> {
    let url = Url::parse(&format!("https://{address}{path}"))
        .map_err(E2eError::system_from("forming diagnostic url"))?;
    let reply = client.send(HttpRequest {
        method: http::Method::GET,
        url: &url,
        bearer: None,
        headers: &[],
        body: None,
    })?;
    if path == gang_wire::routes::GET_SERVER.template {
        let document: serde_json::Value = serde_json::from_slice(&reply.body)
            .map_err(E2eError::system_from("decoding server information"))?;
        Ok(format!(
            "HTTP/2 status={} remote_executor={} remote_cache={}",
            reply.status,
            document
                .get("remoteExecutor")
                .map_or_else(|| "<missing>".to_owned(), ToString::to_string),
            document
                .get("remoteCache")
                .map_or_else(|| "<missing>".to_owned(), ToString::to_string),
        ))
    } else {
        Ok(format!(
            "HTTP/2 GET status={} grpc-status={:?}",
            reply.status,
            reply.headers.get("grpc-status")
        ))
    }
}

/// State shared between the synchronous partition control and the forwarding task.
#[derive(Default)]
struct LinkState {
    cut: bool,
    flows: Vec<AbortHandle>,
}

/// The database engine backing a cluster.
#[derive(Clone, Copy)]
pub enum Engine {
    /// The in-process `DynamoDB` implementation.
    DynamoDb,
    /// A real `PostgreSQL` server.
    Postgres,
}

impl Engine {
    /// The engine's label in scenario verdicts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DynamoDb => "dynamodb",
            Self::Postgres => "postgres",
        }
    }
}

/// The shared database and its outage controls.
pub enum ClusterDatabase {
    /// An in-process `DynamoDB` implementation.
    DynamoDb(FakeDynamoDb),
    /// An isolated `PostgreSQL` server and database.
    Postgres {
        /// The server process.
        server: PostgresServer,
        /// The database shared by controllers.
        database: String,
        /// Whether the server and its children are paused.
        paused: Mutex<bool>,
    },
}

impl ClusterDatabase {
    /// Makes the database unavailable or restores it.
    ///
    /// # Errors
    /// Returns an error if the `PostgreSQL` processes cannot be signalled.
    pub fn set_unavailable(&self, unavailable: bool) -> Result<()> {
        match self {
            Self::DynamoDb(database) => database.set_unavailable(unavailable),
            Self::Postgres { server, paused, .. } => {
                let mut state = paused.lock();
                if unavailable {
                    server.pause()
                } else {
                    server.resume()
                }
                .map_err(E2eError::system_from("signalling postgresql"))?;
                *state = unavailable;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum LinkTarget {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

/// A controller-local TCP link to the shared database.
struct DatabaseLink {
    address: SocketAddr,
    state: Arc<Mutex<LinkState>>,
    task: JoinHandle<()>,
}

impl DatabaseLink {
    async fn start(target: LinkTarget) -> Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .map_err(E2eError::io("binding database link"))?;
        let address = listener
            .local_addr()
            .map_err(E2eError::io("reading database link address"))?;
        let state = Arc::new(Mutex::new(LinkState::default()));
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut flows = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut incoming, _)) = accepted else { break };
                        let mut state = shared.lock();
                        if state.cut {
                            continue;
                        }
                        state.flows.retain(|flow| !flow.is_finished());
                        let target = target.clone();
                        let flow = flows.spawn(async move {
                            match target {
                                LinkTarget::Tcp(address) => {
                                    if let Ok(mut outgoing) = TcpStream::connect(address).await {
                                        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                                    }
                                }
                                LinkTarget::Unix(path) => {
                                    if let Ok(mut outgoing) = UnixStream::connect(path).await {
                                        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                                    }
                                }
                            }
                        });
                        state.flows.push(flow);
                    }
                    Some(_) = flows.join_next(), if !flows.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            state,
            task,
        })
    }

    fn set_cut(&self, cut: bool) {
        let mut state = self.state.lock();
        state.cut = cut;
        if cut {
            for flow in state.flows.drain(..) {
                flow.abort();
            }
        }
    }
}

impl Drop for DatabaseLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Controller {
    public: SocketAddr,
    direct_endpoint: Url,
    execute: SocketAddr,
    config: PathBuf,
    child: Option<Supervised>,
    exit_status: Option<ExitStatus>,
    database_link: DatabaseLink,
    paused: bool,
}

pub(crate) struct Retiring {
    index: usize,
    child: Supervised,
}

/// Controllers with shared database, S3, local provisioner state and signing key.
pub struct Cluster<'a> {
    stack: Stack<'a>,
    controllers: Mutex<Vec<Controller>>,
    balancer: Balancer,
    database: ClusterDatabase,
    s3: FakeS3,
    runtime: tokio::runtime::Runtime,
}

impl<'a> Cluster<'a> {
    /// Starts `controllers` processes and waits for an elected leader.
    ///
    /// # Errors
    /// Returns an error if a fake, controller, or client cannot start before its deadline.
    pub fn start(
        invocation: &DeploymentInputs,
        variant: &Variant,
        issuer: &'a TestIssuer,
        controllers: usize,
        engine: Engine,
    ) -> Result<Self> {
        if controllers == 0 {
            return Err(E2eError::system("a cluster needs at least one controller"));
        }
        let stack = Stack::create(invocation, variant, Transport::Tls, issuer)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(E2eError::io("starting the cluster runtime"))?;
        let (balancer, database, s3) = runtime.block_on(async {
            let balancer =
                Balancer::start(Arc::new(SystemClock::new()), HealthProbe::default()).await?;
            let database = match engine {
                Engine::DynamoDb => ClusterDatabase::DynamoDb(
                    FakeDynamoDb::start()
                        .await
                        .map_err(E2eError::system_from("starting fake dynamodb"))?,
                ),
                Engine::Postgres => {
                    let server = PostgresServer::start(&invocation.postgres)
                        .map_err(E2eError::system_from("starting postgresql"))?;
                    let database = server
                        .create_database()
                        .await
                        .map_err(E2eError::system_from("creating postgresql database"))?;
                    ClusterDatabase::Postgres {
                        server,
                        database,
                        paused: Mutex::new(false),
                    }
                }
            };
            let s3 = FakeS3::start().await?;
            Ok::<_, E2eError>((balancer, database, s3))
        })?;
        let cluster = Self {
            stack,
            controllers: Mutex::new(Vec::new()),
            balancer,
            database,
            s3,
            runtime,
        };
        let startup = (|| -> Result<()> {
            for _ in 0..controllers {
                cluster.start_controller()?;
            }
            cluster.wait_for_leader()?;
            cluster.wait_for_serve()?;
            cluster
                .stack
                .connect_external(cluster.frontend_url(Frontend::Serve, "https")?)
        })();
        let _ = cluster.diagnose(startup.map(|()| Vec::new()))?;
        Ok(cluster)
    }

    fn frontend_url(&self, frontend: Frontend, scheme: &str) -> Result<Url> {
        Url::parse(&format!("{scheme}://{}", self.balancer.address(frontend)))
            .map_err(E2eError::system_from("forming balancer endpoint"))
    }

    fn start_database_link(&self) -> Result<DatabaseLink> {
        let target = match &self.database {
            ClusterDatabase::DynamoDb(database) => {
                let port = database
                    .endpoint()
                    .port()
                    .ok_or_else(|| E2eError::system("fake database endpoint has no port"))?;
                LinkTarget::Tcp(SocketAddr::from(([127, 0, 0, 1], port)))
            }
            ClusterDatabase::Postgres { server, .. } => LinkTarget::Unix(
                server
                    .socket_dir()
                    .join(format!(".s.PGSQL.{}", server.port())),
            ),
        };
        self.runtime.block_on(DatabaseLink::start(target))
    }

    fn wait_for_serve(&self) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        loop {
            let revision = self.balancer.revision();
            if !self.balancer.healthy(Frontend::Serve).is_empty() {
                return Ok(());
            }
            if !self
                .balancer
                .wait_change(revision, timeout.saturating_sub(start.elapsed()))
            {
                return Err(E2eError::timeout(
                    format!("a healthy public controller\n{}", self.diagnostics()),
                    timeout,
                ));
            }
        }
    }

    /// The unpaused controller whose `/leader` health check succeeds, if any.
    #[must_use]
    pub fn leader(&self) -> Option<usize> {
        let healthy = self.balancer.healthy(Frontend::Execute);
        self.controllers
            .lock()
            .iter()
            .position(|controller| !controller.paused && healthy.contains(&controller.execute))
    }

    /// All unpaused controllers whose `/leader` health checks succeed, in controller order.
    #[must_use]
    pub fn leaders(&self) -> Vec<usize> {
        let healthy = self.balancer.healthy(Frontend::Execute);
        self.controllers
            .lock()
            .iter()
            .enumerate()
            .filter_map(|(index, controller)| {
                (!controller.paused && healthy.contains(&controller.execute)).then_some(index)
            })
            .collect()
    }

    /// Waits for a healthy `/leader` check within the startup deadline.
    ///
    /// # Errors
    /// Returns a timeout if no leader becomes healthy.
    pub fn wait_for_leader(&self) -> Result<usize> {
        self.wait_for_leader_except(None)
    }

    pub(crate) fn wait_for_leader_except(&self, excluded: Option<usize>) -> Result<usize> {
        self.wait_for_leader_matching(self.stack.settings().startup_timeout, |leader| {
            Some(leader) != excluded
        })
    }

    pub(crate) fn wait_for_leader_matching(
        &self,
        timeout: Duration,
        mut matches: impl FnMut(usize) -> bool,
    ) -> Result<usize> {
        let start = Instant::now();
        loop {
            let revision = self.balancer.revision();
            if let Some(leader) = self.leaders().into_iter().find(|leader| matches(*leader)) {
                return Ok(leader);
            }
            if !self
                .balancer
                .wait_change(revision, timeout.saturating_sub(start.elapsed()))
            {
                return Err(E2eError::timeout(
                    format!("a healthy leader\n{}", self.diagnostics()),
                    timeout,
                ));
            }
        }
    }

    pub(crate) fn wait_for_single_leader(&self, expected: usize) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        loop {
            let revision = self.balancer.revision();
            if self.leaders() == [expected] {
                return Ok(());
            }
            if !self
                .balancer
                .wait_change(revision, timeout.saturating_sub(start.elapsed()))
            {
                return Err(E2eError::timeout("a single healthy leader", timeout));
            }
        }
    }

    /// Adds a controller with fresh, fixed listener ports.
    ///
    /// # Errors
    /// Returns an error if ports, configuration, or the child cannot be created.
    pub fn start_controller(&self) -> Result<usize> {
        let mut controllers = self.controllers.lock();
        let index = controllers.len();
        let addr =
            || -> Result<SocketAddr> { Ok(SocketAddr::from(([127, 0, 0, 1], ports::reserve()?))) };
        let (public, execute, worker, health) = (addr()?, addr()?, addr()?, addr()?);
        let direct_endpoint = Url::parse(&format!("https://{public}"))
            .map_err(E2eError::system_from("forming controller endpoint"))?;
        let database_link = self.start_database_link()?;
        let config = self
            .stack
            .placement()
            .root()
            .join(format!("controller-{index}.toml"));
        self.write_controller_config(&config, public, execute, worker, health, &database_link)?;
        let cli = GangCli::new(
            self.stack.binary().to_path_buf(),
            config.clone(),
            self.stack.placement().home(),
        );
        let mut command = cli.server();
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("AWS_") {
                command.env_remove(key);
            }
        }
        // The harness host has no instance metadata.
        command
            .env("AWS_ACCESS_KEY_ID", DUMMY_CREDENTIAL)
            .env("AWS_SECRET_ACCESS_KEY", DUMMY_CREDENTIAL)
            .env("AWS_REGION", REGION)
            .env("AWS_EC2_METADATA_DISABLED", DISABLE_INSTANCE_METADATA);
        let log = self
            .stack
            .placement()
            .root()
            .join(format!("controller-{index}.log"));
        let mut child = Supervised::spawn(&mut command, &log)?;
        for (frontend, address) in [
            (Frontend::Serve, public),
            (Frontend::Execute, execute),
            (Frontend::Workers, worker),
        ] {
            self.balancer.register(
                frontend,
                Target {
                    traffic: address,
                    health,
                },
            );
        }
        let failure = match child
            .await_announcements(self.stack.settings().startup_timeout, is_announcement)
        {
            Ok(Some(_)) => None,
            Ok(None) => Some("exited before announcing".to_owned()),
            Err(error) => Some(gang_types::error_chain(&error).to_string()),
        };
        if let Some(outcome) = failure {
            for (frontend, address) in [
                (Frontend::Serve, public),
                (Frontend::Execute, execute),
                (Frontend::Workers, worker),
            ] {
                self.balancer.deregister(frontend, address);
            }
            let tail = match fs::read(&log) {
                Ok(bytes) => crate::observer::tail(&bytes, self.stack.settings().log_tail_bytes),
                Err(error) => format!("unreadable: {error}"),
            };
            let state = match child.has_exited() {
                Ok(false) => format!("alive pid={}", child.pid()),
                Ok(true) => match child.terminate(Duration::ZERO) {
                    Ok(status) => format!("exited status={status}"),
                    Err(error) => format!("exited; status unavailable: {error}"),
                },
                Err(error) => format!("status unavailable: {error}"),
            };
            drop(controllers);
            return Err(E2eError::system(format!(
                "controller {index} {outcome}; {state}; log tail {}:\n{tail}\n{}",
                log.display(),
                self.diagnostics()
            )));
        }
        controllers.push(Controller {
            public,
            direct_endpoint,
            execute,
            config,
            child: Some(child),
            exit_status: None,
            database_link,
            paused: false,
        });
        Ok(index)
    }

    fn write_controller_config(
        &self,
        config: &Path,
        public: SocketAddr,
        execute: SocketAddr,
        worker: SocketAddr,
        health: SocketAddr,
        database_link: &DatabaseLink,
    ) -> Result<()> {
        let base = fs::read_to_string(self.stack.placement().config_file())
            .map_err(E2eError::io("reading cluster configuration"))?;
        let mut doc: toml::Value =
            toml::from_str(&base).map_err(crate::template::template_error)?;
        let root = doc
            .as_table_mut()
            .ok_or_else(|| E2eError::system("configuration is not a table"))?;
        root.insert("listen".into(), toml::Value::String(public.to_string()));
        root.insert(
            "execution_listen".into(),
            toml::Value::String(execute.to_string()),
        );
        root.insert(
            "worker_listen".into(),
            toml::Value::String(worker.to_string()),
        );
        root.insert(
            "health_listen".into(),
            toml::Value::String(health.to_string()),
        );
        root.insert(
            "endpoint".into(),
            toml::Value::String(self.frontend_url(Frontend::Serve, "https")?.to_string()),
        );
        root.insert(
            "execution_endpoint".into(),
            toml::Value::String(self.frontend_url(Frontend::Execute, "https")?.to_string()),
        );
        let scheduler = doc
            .get_mut("scheduler")
            .and_then(|value| value.get_mut("nativelink"))
            .and_then(toml::Value::as_table_mut)
            .ok_or_else(|| E2eError::system("missing nativelink scheduler"))?;
        scheduler.insert(
            "worker_endpoint".into(),
            toml::Value::String(self.frontend_url(Frontend::Workers, "grpcs")?.to_string()),
        );
        let database_config = match &self.database {
            ClusterDatabase::DynamoDb(_) => format!(
                "[dynamodb]\ntable = \"{TABLE}\"\nregion = \"{REGION}\"\nendpoint = \"http://{}\"\n",
                database_link.address
            ),
            ClusterDatabase::Postgres { database, .. } => format!(
                "[postgres]\nhost = \"127.0.0.1\"\nport = {}\ndatabase = \"{database}\"\nuser = \"{SUPERUSER}\"\ntls = \"disable\"\n",
                database_link.address.port()
            ),
        };
        doc["database"] =
            toml::from_str(&database_config).map_err(crate::template::template_error)?;
        doc["cache"] = toml::from_str(&format!("[s3]\nregion = \"{REGION}\"\nbucket = \"{BUCKET}\"\nforce_path_style = true\nendpoint = \"{}\"\n", self.s3.endpoint())).map_err(crate::template::template_error)?;
        let rendered = toml::to_string(&doc).map_err(crate::template::template_error)?;
        fs::write(config, rendered)
            .map_err(E2eError::io(format!("writing {}", config.display())))?;
        Ok(())
    }

    /// Kills a controller with SIGKILL.
    ///
    /// # Errors
    /// Returns an error for an unknown controller or a failed signal.
    pub fn kill(&self, index: usize) -> Result<()> {
        let mut child = self.take_controller(index)?;
        child.kill()?;
        let status = child.terminate(Duration::ZERO)?;
        self.remember_exit(index, status);
        Ok(())
    }

    /// Transfers a controller's process handle without bypassing balancer health checks.
    fn take_controller(&self, index: usize) -> Result<Supervised> {
        let mut controllers = self.controllers.lock();
        let controller = controllers
            .get_mut(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        let child = controller
            .child
            .take()
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        Ok(child)
    }

    fn remember_exit(&self, index: usize, status: ExitStatus) {
        if let Some(controller) = self.controllers.lock().get_mut(index) {
            controller.exit_status = Some(status);
        }
    }

    /// Requests graceful handoff without waiting for the controller to exit.
    pub(crate) fn begin_retire(&self, index: usize) -> Result<Retiring> {
        let mut child = self.take_controller(index)?;
        child
            .request_exit()
            .map_err(|error| self.timeout_evidence(error))?;
        Ok(Retiring { index, child })
    }

    /// Waits for a controller whose graceful handoff has been requested.
    pub(crate) fn finish_retire(&self, mut retiring: Retiring) -> Result<ExitStatus> {
        let status = retiring
            .child
            .wait_within(
                self.stack.settings().startup_timeout,
                "the process to exit after SIGTERM",
            )
            .map_err(|error| self.timeout_evidence(error))?;
        self.remember_exit(retiring.index, status);
        Ok(status)
    }

    /// Requests graceful handoff and waits for the process to exit.
    ///
    /// # Errors
    /// Returns an error for an unknown controller, a signal failure, or an expired deadline.
    pub fn retire(&self, index: usize) -> Result<ExitStatus> {
        self.finish_retire(self.begin_retire(index)?)
    }
    /// Waits for a controller to exit on its own, within the startup deadline.
    ///
    /// A controller that does not exit before the deadline is killed when its handle is dropped.
    ///
    /// # Errors
    /// Returns an error for an unknown controller, a failed process observation, or a timeout.
    pub fn wait_exit(&self, index: usize) -> Result<ExitStatus> {
        if let Some(status) = self
            .controllers
            .lock()
            .get(index)
            .and_then(|controller| controller.exit_status)
        {
            return Ok(status);
        }
        let mut child = self.take_controller(index)?;
        let timeout = self.stack.settings().startup_timeout;
        let condition = format!("controller {index} (pid {}) to exit", child.pid());
        let status = child
            .wait_within(timeout, &condition)
            .map_err(|error| self.timeout_evidence(error))?;
        self.remember_exit(index, status);
        Ok(status)
    }

    /// Reads the captured standard output and standard error of a controller.
    ///
    /// # Errors
    /// Returns an error for an unknown controller or if the log cannot be read.
    pub fn log(&self, index: usize) -> Result<String> {
        {
            let controllers = self.controllers.lock();
            if controllers.get(index).is_none() {
                return Err(E2eError::system("unknown controller"));
            }
        }
        let path = self
            .stack
            .placement()
            .root()
            .join(format!("controller-{index}.log"));
        let bytes = fs::read(&path).map_err(E2eError::io(format!("reading {}", path.display())))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn signal(&self, index: usize, signal: Signal, paused: bool) -> Result<()> {
        let mut controllers = self.controllers.lock();
        let controller = controllers
            .get_mut(index)
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        let pid = controller
            .child
            .as_ref()
            .map(Supervised::pid)
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        let raw = i32::try_from(pid).map_err(E2eError::system_from("converting controller pid"))?;
        let target =
            Pid::from_raw(raw).ok_or_else(|| E2eError::system("invalid controller pid"))?;
        kill_process(target, signal)
            .map_err(|error| E2eError::io("signalling controller")(std::io::Error::from(error)))?;
        controller.paused = paused;
        drop(controllers);
        self.balancer.notify_change();
        Ok(())
    }

    /// Stops a controller without terminating it.
    ///
    /// # Errors
    /// Returns an error if the controller cannot be signalled.
    pub fn pause(&self, index: usize) -> Result<()> {
        self.signal(index, Signal::STOP, true)
    }

    /// Resumes a stopped controller.
    ///
    /// # Errors
    /// Returns an error if the controller cannot be signalled.
    pub fn resume(&self, index: usize) -> Result<()> {
        self.signal(index, Signal::CONT, false)
    }

    /// The public listener of a controller, bypassing the balancer.
    ///
    /// # Panics
    /// Panics if `index` does not identify a controller started by this cluster.
    #[must_use]
    pub fn direct_endpoint(&self, index: usize) -> Url {
        self.controllers.lock()[index].direct_endpoint.clone()
    }

    /// Stops routing new execution connections to a controller, preserving existing streams.
    pub(crate) fn unroute_execution(&self, index: usize) {
        let execute = self.controllers.lock()[index].execute;
        self.balancer.deregister(Frontend::Execute, execute);
    }

    /// Stops routing new public connections to a controller, preserving existing streams.
    pub(crate) fn unroute_public(&self, index: usize) {
        let public = self.controllers.lock()[index].public;
        self.balancer.deregister(Frontend::Serve, public);
    }

    /// The controllers whose public listener is a healthy `Frontend::Serve` target, in controller order.
    #[must_use]
    pub(crate) fn serving(&self) -> Vec<usize> {
        let healthy = self.balancer.healthy(Frontend::Serve);
        self.controllers
            .lock()
            .iter()
            .enumerate()
            .filter_map(|(i, c)| healthy.contains(&c.public).then_some(i))
            .collect()
    }

    /// Waits within the startup deadline until `serving()` is exactly `expected` (ascending).
    pub(crate) fn wait_for_serving(&self, expected: &[usize]) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        loop {
            let revision = self.balancer.revision();
            if self.serving() == expected {
                return Ok(());
            }
            if !self
                .balancer
                .wait_change(revision, timeout.saturating_sub(start.elapsed()))
            {
                return Err(E2eError::timeout(
                    format!(
                        "public traffic served only by controllers {expected:?}\n{}",
                        self.diagnostics()
                    ),
                    timeout,
                ));
            }
        }
    }

    /// Cuts or restores one controller's database link.
    ///
    /// Cutting drops established connections and rejects subsequent connections until restored.
    ///
    /// # Panics
    /// Panics if `index` does not identify a controller started by this cluster.
    pub fn cut_database(&self, index: usize, cut: bool) {
        self.controllers.lock()[index].database_link.set_cut(cut);
    }

    /// The shared database, including its outage controls.
    #[must_use]
    pub const fn database(&self) -> &ClusterDatabase {
        &self.database
    }

    /// Attaches a deployment snapshot to scenario errors and failed checks.
    pub(crate) fn diagnose(&self, result: Result<Vec<Check>>) -> Result<Vec<Check>> {
        match result {
            Err(error) => {
                let error = self.timeout_evidence(error);
                if matches!(&error, E2eError::Timeout { .. }) {
                    Err(error)
                } else {
                    let message = gang_types::error_chain(&error).to_string();
                    if message.contains("cluster evidence:") {
                        Err(error)
                    } else {
                        Err(E2eError::system(format!(
                            "{message}\n{}",
                            self.diagnostics()
                        )))
                    }
                }
            }
            Ok(mut checks) => {
                if checks.iter().any(Check::is_failure) {
                    let evidence = self.diagnostics();
                    for check in &mut checks {
                        if let Outcome::Failed(reason) = &mut check.outcome {
                            reason.push('\n');
                            reason.push_str(&evidence);
                        }
                    }
                }
                Ok(checks)
            }
        }
    }

    fn timeout_evidence(&self, error: E2eError) -> E2eError {
        match error {
            E2eError::Timeout { condition, waited } if !condition.contains("cluster evidence:") => {
                E2eError::timeout(format!("{condition}\n{}", self.diagnostics()), waited)
            }
            other => other,
        }
    }

    /// Performs a bounded, pinned TLS handshake and reports its negotiated ALPN.
    fn probe_execution_tls(&self, address: SocketAddr, pin: &CaPin) -> Result<String> {
        let config = gang_tls::client_config(
            ServerTrust::Pin(pin),
            None,
            &[crate::tls_client::ALPN_H2],
            Arc::new(SystemClock::new()),
        )
        .map_err(E2eError::system_from("configuring diagnostic tls"))?;
        let name = rustls_pki_types::ServerName::try_from(address.ip().to_string())
            .map_err(E2eError::system_from("forming diagnostic tls server name"))?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        self.runtime.block_on(crate::tls_client::within(
            DIAGNOSTIC_TIMEOUT,
            format!("tls handshake to {address}"),
            async move {
                let tcp = TcpStream::connect(address)
                    .await
                    .map_err(E2eError::io(format!("connecting to {address}")))?;
                let stream = connector
                    .connect(name, tcp)
                    .await
                    .map_err(E2eError::system_from(format!("handshaking with {address}")))?;
                let alpn = stream.get_ref().1.alpn_protocol().map_or_else(
                    || "<none>".to_owned(),
                    |value| String::from_utf8_lossy(value).into_owned(),
                );
                Ok(format!("TLS handshake succeeded, ALPN={alpn}"))
            },
        ))
    }

    /// Probes each public and execution endpoint and reports the build's remote rc settings.
    fn active_probes(&self) -> String {
        let rc = self
            .stack
            .placement()
            .home()
            .join(crate::secured::BAZEL_RC_FILE);
        let rc_text = match fs::read_to_string(&rc) {
            Ok(text) => {
                let lines: Vec<_> = text
                    .lines()
                    .filter(|line| {
                        line.contains("--remote_executor") || line.contains("--remote_cache")
                    })
                    .collect();
                if lines.is_empty() {
                    "<no executor/cache lines>".to_owned()
                } else {
                    lines.join("\n")
                }
            }
            Err(error) => format!("unreadable: {error}"),
        };
        let mut lines = vec![format!("Bazel rc {}:\n{rc_text}", rc.display())];
        let pin = match self.stack.ca_pin() {
            Ok(pin) => pin,
            Err(error) => {
                lines.push(format!(
                    "active probes unavailable: {}",
                    gang_types::error_chain(&error)
                ));
                return lines.join("\n");
            }
        };
        let client = crate::http::Http::try_new(
            HttpTransport::Tls {
                trust: ServerTrust::Pin(&pin),
            },
            HttpProtocol::Http2,
            DIAGNOSTIC_TIMEOUT,
        );
        let controllers: Vec<_> = self
            .controllers
            .lock()
            .iter()
            .enumerate()
            .map(|(index, controller)| (index, controller.public, controller.execute))
            .collect();
        let mut public: Vec<_> = controllers
            .iter()
            .map(|(index, address, _)| (format!("controller {index} public"), *address))
            .collect();
        let mut execution: Vec<_> = controllers
            .iter()
            .map(|(index, _, address)| (format!("controller {index} execution"), *address))
            .collect();
        public.push((
            "balancer serve".to_owned(),
            self.balancer.address(Frontend::Serve),
        ));
        execution.push((
            "balancer execute".to_owned(),
            self.balancer.address(Frontend::Execute),
        ));
        for (name, address) in public {
            let result = match &client {
                Ok(client) => probe_http(client, address, gang_wire::routes::GET_SERVER.template)
                    .unwrap_or_else(|error| format!("error: {}", gang_types::error_chain(&error))),
                Err(error) => format!("client error: {}", gang_types::error_chain(error)),
            };
            lines.push(format!("{name} {address} GET /v1/server: {result}"));
        }
        for (name, address) in execution {
            let tls = self
                .probe_execution_tls(address, &pin)
                .unwrap_or_else(|error| format!("error: {}", gang_types::error_chain(&error)));
            let request = match &client {
                Ok(client) => probe_http(client, address, CAPABILITIES_PATH)
                    .unwrap_or_else(|error| format!("error: {}", gang_types::error_chain(&error))),
                Err(error) => format!("client error: {}", gang_types::error_chain(error)),
            };
            lines.push(format!("{name} {address}: {tls}; {request}"));
        }
        lines.join("\n")
    }

    /// Captures controller, balancer, worker and database evidence before the cluster is dropped.
    fn diagnostics(&self) -> String {
        let limit = self.stack.settings().log_tail_bytes;
        let leaders = self.leaders();
        let database = match &self.database {
            ClusterDatabase::DynamoDb(database) => {
                format!("fake dynamodb requests={}", database.requests())
            }
            ClusterDatabase::Postgres { paused, .. } => {
                format!("postgresql paused={}", *paused.lock())
            }
        };
        let mut lines = vec![format!(
            "cluster evidence: leaders={leaders:?}; healthy serve={:?}, execute={:?}, workers={:?}; {database}",
            self.balancer.healthy(Frontend::Serve),
            self.balancer.healthy(Frontend::Execute),
            self.balancer.healthy(Frontend::Workers),
        )];
        lines.push(self.s3_evidence());
        {
            let mut controllers = self.controllers.lock();
            for (index, controller) in controllers.iter_mut().enumerate() {
                let state = if let Some(child) = controller.child.as_mut() {
                    let pid = child.pid();
                    match child.has_exited() {
                        Ok(false) => format!("alive pid={pid}, paused={}", controller.paused),
                        Ok(true) => match child.terminate(Duration::ZERO) {
                            Ok(status) => {
                                controller.exit_status = Some(status);
                                format!("exited pid={pid}, status={status}")
                            }
                            Err(error) => format!("exited pid={pid}, status unavailable: {error}"),
                        },
                        Err(error) => format!("pid={pid}, status unavailable: {error}"),
                    }
                } else if let Some(status) = controller.exit_status {
                    format!("exited status={status}")
                } else {
                    "not running or process handle in use; exit status unavailable".to_owned()
                };
                let path = self
                    .stack
                    .placement()
                    .root()
                    .join(format!("controller-{index}.log"));
                let log = match fs::read(&path) {
                    Ok(bytes) => crate::observer::tail(&bytes, limit),
                    Err(error) => format!("unreadable: {error}"),
                };
                lines.push(format!(
                    "controller {index}: {state}; log tail {}:\n{log}",
                    path.display()
                ));
            }
        }
        let state = self.stack.placement().state_dir();
        lines.push(format!(
            "local instance records:\n{}",
            local_evidence(&state.join("instances"), true, limit)
        ));
        lines.push(format!(
            "local worker log tails:\n{}",
            local_evidence(&state.join("logs"), false, limit)
        ));
        lines.push(self.active_probes());
        lines.join("\n")
    }

    fn s3_evidence(&self) -> String {
        let keys = self
            .s3
            .keys()
            .into_iter()
            .take(DIAGNOSTIC_S3_KEYS)
            .map(|(key, size)| format!("{key}: {size} bytes"))
            .collect::<Vec<_>>()
            .join("\n");
        let requests = self.s3.requests();
        let start = requests.len().saturating_sub(DIAGNOSTIC_S3_REQUESTS);
        let recent = requests.get(start..).unwrap_or_default().join("\n");
        format!(
            "fake S3 objects={}; first keys:\n{keys}\nlast requests:\n{recent}",
            self.s3.objects()
        )
    }

    pub(crate) const fn stack(&self) -> &Stack<'a> {
        &self.stack
    }
}

impl System for Cluster<'_> {
    fn start_server(&self) -> Result<()> {
        self.start_controller().map(|_| ())
    }
    fn kill_server(&self) -> Result<()> {
        self.kill(self.leader().ok_or_else(|| E2eError::system("no leader"))?)
    }
    fn server_pid(&self) -> Option<u32> {
        let index = self.leader()?;
        let mut controllers = self.controllers.lock();
        let child = controllers.get_mut(index)?.child.as_mut()?;
        if child.has_exited().ok()? {
            None
        } else {
            Some(child.pid())
        }
    }
    fn worker_count(&self) -> Result<usize> {
        let entries = process::snapshot();
        Ok(self
            .controllers
            .lock()
            .iter()
            .map(|controller| process::workers_of(&entries, &controller.config).len())
            .sum())
    }
    fn scaling_events(&self) -> Result<Vec<Event>> {
        self.stack.scaling_events()
    }
    fn anonymous_admin_status(&self) -> Result<u16> {
        self.stack.anonymous_admin_status()
    }
    fn live_state(&self) -> Result<State> {
        self.stack.live_state()
    }
    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>> {
        self.stack.await_stream(done)
    }
    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome> {
        self.stack.build(probes)
    }
    fn build_run(&self, probes: &[Probe], run: &Run) -> Result<BuildOutcome> {
        self.stack.build_run(probes, run)
    }
    fn clean(&self) -> Result<()> {
        self.stack.clean()
    }
    fn report(&self, probe: Probe) -> Result<ProbeReport> {
        self.stack.report(probe)
    }
    fn host(&self) -> &Host {
        self.stack.host()
    }
    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>> {
        self.stack.await_history(done)
    }
    fn tmp_path(&self) -> &Path {
        self.stack.tmp_path()
    }
    fn stderr_tail_lines(&self) -> usize {
        self.stack.stderr_tail_lines()
    }
}

impl Drop for Cluster<'_> {
    fn drop(&mut self) {
        for controller in self.controllers.get_mut().iter_mut() {
            if let Some(mut child) = controller.child.take() {
                let _ = child.kill();
            }
            if let Err(error) = process::terminate_workers(&controller.config) {
                eprintln!(
                    "worker cleanup for {}: {error}",
                    controller.config.display()
                );
            }
        }
        // Keep the asynchronous fakes alive until after all child processes have stopped.
        let _ = &self.runtime;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_testkit::TestResult;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream, UnixListener};
    use tokio::time::timeout;

    use super::{DatabaseLink, LinkTarget, local_evidence};

    const DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn local_evidence_includes_records_and_individual_bounded_logs() -> TestResult {
        let root = tempfile::tempdir()?;
        let instances = root.path().join("instances");
        let logs = root.path().join("logs");
        std::fs::create_dir_all(instances.join("worker-1"))?;
        std::fs::create_dir_all(&logs)?;
        std::fs::write(instances.join("worker-1/instance.json"), b"{\"pid\":1}")?;
        std::fs::write(logs.join("worker-1.log"), b"prefix-tail")?;
        std::fs::write(logs.join("worker-2.log"), b"other-last")?;
        let records = local_evidence(&instances, true, 4);
        assert!(records.contains("{\"pid\":1}"));
        let tails = local_evidence(&logs, false, 4);
        assert!(tails.contains("worker-1.log:\ntail"));
        assert!(tails.contains("worker-2.log:\nlast"));
        assert!(!tails.contains("prefix"));
        Ok(())
    }

    #[test]
    fn local_evidence_redacts_instance_secrets() -> TestResult {
        let root = tempfile::tempdir()?;
        let instances = root.path().join("instances");
        let worker = instances.join("worker");
        std::fs::create_dir_all(&worker)?;
        let record = worker.join("instance.json");
        let sentinel = "obviously-synthetic-secret-sentinel";
        std::fs::write(&record, format!(r#"{{"pid":4242,"secret":"{sentinel}"}}"#))?;

        let output = local_evidence(&instances, true, 4);
        assert!(!output.contains(sentinel));
        assert!(output.contains("<redacted>"));
        assert!(output.contains("pid"));
        assert!(output.contains("4242"));

        for raw in [
            format!(r#"{{"secret":"{sentinel}""#),
            format!(r#""{sentinel}""#),
        ] {
            std::fs::write(&record, &raw)?;
            let output = local_evidence(&instances, true, 4);
            assert!(!output.contains(sentinel));
            assert!(!output.contains(&raw));
            let indication = output.to_lowercase();
            assert!(indication.contains("unreadable") || indication.contains("error"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn database_link_cuts_existing_and_new_connections_then_recovers() -> TestResult {
        let backend = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let link = DatabaseLink::start(LinkTarget::Tcp(backend.local_addr()?)).await?;
        let mut client = TcpStream::connect(link.address).await?;
        let (mut server, _) = timeout(DEADLINE, backend.accept()).await??;
        client.write_all(b"a").await?;
        let mut byte = [0; 1];
        timeout(DEADLINE, server.read_exact(&mut byte)).await??;
        assert_eq!(byte, *b"a");

        link.set_cut(true);
        assert!(matches!(
            timeout(DEADLINE, client.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ));
        let mut rejected = TcpStream::connect(link.address).await?;
        assert!(matches!(
            timeout(DEADLINE, rejected.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ));

        link.set_cut(false);
        let mut restored = TcpStream::connect(link.address).await?;
        let (mut server, _) = timeout(DEADLINE, backend.accept()).await??;
        restored.write_all(b"b").await?;
        timeout(DEADLINE, server.read_exact(&mut byte)).await??;
        assert_eq!(byte, *b"b");
        Ok(())
    }

    #[tokio::test]
    async fn unix_database_link_cuts_existing_and_new_connections_then_recovers() -> TestResult {
        let directory = tempfile::Builder::new()
            .prefix("gang-link")
            .tempdir_in("/tmp")?;
        let path = directory.path().join("socket");
        let backend = UnixListener::bind(&path)?;
        let link = DatabaseLink::start(LinkTarget::Unix(path)).await?;
        let mut client = TcpStream::connect(link.address).await?;
        let (mut server, _) = timeout(DEADLINE, backend.accept()).await??;
        client.write_all(b"a").await?;
        let mut byte = [0; 1];
        timeout(DEADLINE, server.read_exact(&mut byte)).await??;
        assert_eq!(byte, *b"a");

        link.set_cut(true);
        assert!(matches!(
            timeout(DEADLINE, client.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ));
        let mut rejected = TcpStream::connect(link.address).await?;
        assert!(matches!(
            timeout(DEADLINE, rejected.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ));

        link.set_cut(false);
        let mut restored = TcpStream::connect(link.address).await?;
        let (mut server, _) = timeout(DEADLINE, backend.accept()).await??;
        restored.write_all(b"b").await?;
        timeout(DEADLINE, server.read_exact(&mut byte)).await??;
        assert_eq!(byte, *b"b");
        Ok(())
    }
}
