//! A set of controllers sharing durable state behind loopback load balancers.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use gang_clock::SystemClock;
use gang_testkit::{FakeDynamoDb, PostgresServer, SUPERUSER};
use gang_tls::ServerTrust;
use gang_types::{CaPin, Url};
use gang_wire::{Event, Frame, State};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use rustix::process::{Pid, Signal, kill_process};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::balancer::{Balancer, Frontend, Policy, Probe as HealthProbe, Target};
use crate::build_output::BuildOutcome;
use crate::check::{Check, Outcome};
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::fake_s3::FakeS3;
use crate::gang_cli::GangCli;
use crate::host::Host;
use crate::issuer::TestIssuer;
use crate::ports;
use crate::process::{self, ControllerHost, Supervised};
use crate::report::ProbeReport;
use crate::settings::DeploymentInputs;
use crate::stack::{Stack, is_announcement, local_evidence};
use crate::system::{Probe, System, Variant};
use crate::template::Transport;
use crate::upgrade_fixture::{FrontendPlan, ReleaseFixture};
use crate::upgrade_probe::DirectTarget;

const TABLE: &str = "gang-e2e";
const BUCKET: &str = "gang-e2e";
const REGION: &str = "us-east-1";
const DUMMY_CREDENTIAL: &str = "dummy";
const DISABLE_INSTANCE_METADATA: &str = "true";
const DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(3);
const CAPABILITIES_PATH: &str = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";
const DIAGNOSTIC_S3_KEYS: usize = 20;
const DIAGNOSTIC_S3_REQUESTS: usize = 30;
const LEASE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LEADING_HEADER: &str = "gang-leading";

#[derive(Clone, Copy, Debug)]
struct LeadershipHealth {
    leading: bool,
}

async fn probe_health(
    address: SocketAddr,
    policy: &Policy,
    path: &str,
    timeout: Duration,
) -> Option<(StatusCode, bool)> {
    tokio::time::timeout(policy.timeout.min(timeout), async {
        let mut stream = TcpStream::connect(address).await.ok()?;
        if policy.proxy_protocol_v2 {
            stream
                .write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0")
                .await
                .ok()?;
        }
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake::<_, Body>(TokioIo::new(stream))
                .await
                .ok()?;
        let mut connections = JoinSet::new();
        connections.spawn(connection);
        let request = Request::get(path)
            .header(hyper::header::HOST, address.to_string())
            .body(Body::empty())
            .ok()?;
        let response = sender.send_request(request).await.ok()?;
        let values = response.headers().get_all(LEADING_HEADER);
        let mut values = values.iter();
        let leading = values
            .next()
            .is_some_and(|value| value.as_bytes() == b"true")
            && values.next().is_none();
        Some((response.status(), leading))
    })
    .await
    .ok()
    .flatten()
}

async fn probe_leadership(
    address: SocketAddr,
    policy: &Policy,
    timeout: Duration,
) -> Option<LeadershipHealth> {
    let (status, leading) = probe_health(address, policy, &policy.path, timeout).await?;
    Some(LeadershipHealth {
        leading: status == StatusCode::OK && leading,
    })
}

fn observed_leaders(observations: &[Option<LeadershipHealth>]) -> impl Iterator<Item = usize> + '_ {
    observations
        .iter()
        .enumerate()
        .filter_map(|(index, health)| health.is_some_and(|health| health.leading).then_some(index))
}

fn initial_leader(observations: &[Option<LeadershipHealth>]) -> Option<usize> {
    if observations.iter().any(Option::is_none) {
        return None;
    }
    let mut leaders = observed_leaders(observations);
    let leader = leaders.next()?;
    if leaders.next().is_some() || Some(leader) != observations.len().checked_sub(1) {
        return None;
    }
    Some(leader)
}

/// State shared between the synchronous partition control and the forwarding task.
#[derive(Default)]
struct LinkState {
    cut: bool,
    stalled: bool,
    stall_postgres_history: bool,
    stalled_connections: usize,
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
                        if state.stalled {
                            state.stalled_connections += 1;
                            let flow = flows.spawn(async move {
                                std::future::pending::<()>().await;
                                drop(incoming);
                            });
                            state.flows.push(flow);
                            continue;
                        }
                        let stall_postgres_history = state.stall_postgres_history;
                        let shared = Arc::clone(&shared);
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
                                        if stall_postgres_history {
                                            let (read, mut write) = incoming.split();
                                            let (mut response, request) = outgoing.split();
                                            tokio::select! {
                                                _ = forward_postgres_preparation(read, request, &shared) => {}
                                                _ = tokio::io::copy(&mut response, &mut write) => {}
                                            }
                                        } else {
                                            let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                                        }
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

// PostgreSQL's initial authentication reads fail fast on timeout. Let those and schema setup
// finish, then hold the read-only history import, which retries without requesting takeover.
async fn forward_postgres_preparation(
    mut incoming: impl AsyncRead + Unpin,
    mut outgoing: impl AsyncWrite + Unpin,
    state: &Mutex<LinkState>,
) -> std::io::Result<()> {
    const HISTORY_QUERY: &[u8] = b"SELECT id, body, version, expires_at FROM expiring_records";
    let mut startup = true;
    let mut header = [0; 5];
    let mut payload = Vec::new();
    loop {
        let prefix = usize::from(!startup);
        let header = &mut header[..4 + prefix];
        incoming.read_exact(header).await?;
        let length = u32::from_be_bytes([
            header[prefix],
            header[prefix + 1],
            header[prefix + 2],
            header[prefix + 3],
        ]);
        let length = length.checked_sub(4).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid postgres frame length",
            )
        })?;
        payload.resize(length as usize, 0);
        incoming.read_exact(&mut payload).await?;
        if !startup
            && matches!(header[0], b'P' | b'Q')
            && payload
                .windows(HISTORY_QUERY.len())
                .any(|part| part == HISTORY_QUERY)
        {
            state.lock().stalled_connections += 1;
            std::future::pending::<()>().await;
        }
        outgoing.write_all(header).await?;
        outgoing.write_all(&payload).await?;
        startup = false;
    }
}

impl Drop for DatabaseLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Copy)]
struct ControllerAddresses {
    public: SocketAddr,
    worker: SocketAddr,
    health: SocketAddr,
}

struct ReleaseServices<'a> {
    public: &'a Url,
    worker: &'a Url,
    database: SocketAddr,
    s3: &'a str,
}

struct Controller {
    public: SocketAddr,
    public_proxy_protocol_v2: bool,
    direct_endpoint: Url,
    worker: SocketAddr,
    health: SocketAddr,
    health_policy: Arc<Policy>,
    identity: Arc<str>,
    config: PathBuf,
    child: Option<ControllerHost>,
    exit_status: Option<ExitStatus>,
    database_link: DatabaseLink,
    paused: bool,
}

pub(crate) struct Retiring {
    index: usize,
    started: Instant,
}

/// Controllers with shared database, S3, local provisioner state and signing key.
pub struct Cluster<'a> {
    stack: Stack<'a>,
    controllers: Mutex<Vec<Controller>>,
    balancer: Balancer,
    database: ClusterDatabase,
    s3: FakeS3,
    runtime: tokio::runtime::Runtime,
    release_evidence: Mutex<Vec<String>>,
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
            release_evidence: Mutex::new(Vec::new()),
            runtime,
        };
        let startup = (|| -> Result<()> {
            for _ in 0..controllers {
                cluster.start_controller()?;
            }
            cluster.wait_for_initial_leader()?;
            cluster
                .stack
                .connect_external(cluster.frontend_url(Frontend::Serve, "https")?)
        })();
        let _ = cluster.diagnose(startup.map(|()| Vec::new()))?;
        Ok(cluster)
    }

    pub(crate) fn start_release(
        invocation: &DeploymentInputs,
        variant: &Variant,
        issuer: &'a TestIssuer,
        binary: &Path,
        fixture: &ReleaseFixture,
    ) -> Result<Self> {
        let mut inputs = invocation.clone();
        inputs.gang = binary.to_path_buf();
        let stack = Stack::create(&inputs, variant, Transport::Tls, issuer)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(E2eError::io("starting the cluster runtime"))?;
        let (balancer, database, s3) = runtime.block_on(async {
            let balancer =
                Balancer::start(Arc::new(SystemClock::new()), HealthProbe::default()).await?;
            let database = FakeDynamoDb::start()
                .await
                .map_err(E2eError::system_from("starting fake dynamodb"))?;
            Ok::<_, E2eError>((
                balancer,
                ClusterDatabase::DynamoDb(database),
                FakeS3::start().await?,
            ))
        })?;
        let mut cluster = Self {
            stack,
            controllers: Mutex::new(Vec::new()),
            balancer,
            database,
            s3,
            runtime,
            release_evidence: Mutex::new(Vec::new()),
        };
        cluster.set_release_policies(fixture)?;
        let index = cluster.start_controller_release(binary, fixture)?;
        let config = cluster.controllers.lock()[index].config.clone();
        cluster.stack.use_client_config(config);
        cluster.wait_for_leader()?;
        cluster.wait_for_controller_serve(index)?;
        cluster.wait_for_controller_health(index)?;
        cluster.wait_for_serve()?;
        cluster
            .stack
            .connect_external(cluster.frontend_url(Frontend::Serve, "https")?)?;
        Ok(cluster)
    }

    fn set_release_policies(&self, fixture: &ReleaseFixture) -> Result<()> {
        let plan = |frontend: &crate::upgrade_fixture::FrontendPlan| {
            format!(
                "port={} target={} health={:?} proxy_protocol_v2={} preserve_client_ip={:?}",
                frontend.port,
                frontend.target_port,
                frontend.health,
                frontend.proxy_protocol_v2,
                frontend.preserve_client_ip
            )
        };
        self.release_evidence.lock().push(format!(
            "public={}; workers={}; controllers={}, health={:?}, min={}%, max={}%",
            plan(&fixture.public),
            plan(&fixture.workers),
            fixture.controller.instances,
            fixture.controller.health_check_type,
            fixture.controller.min_healthy_percentage,
            fixture.controller.max_healthy_percentage
        ));
        for (frontend, plan) in [
            (Frontend::Serve, &fixture.public),
            (Frontend::Workers, &fixture.workers),
        ] {
            self.balancer.set_policy(frontend, release_policy(plan)?);
        }
        Ok(())
    }

    pub(crate) fn apply_release(&self, fixture: &ReleaseFixture) -> Result<()> {
        self.set_release_policies(fixture)
    }

    pub(crate) fn wait_for_controller_serve(&self, index: usize) -> Result<()> {
        self.wait_for_controller_serve_within(index, self.stack.settings().startup_timeout)
    }

    pub(crate) fn wait_for_controller_serve_within(
        &self,
        index: usize,
        _timeout: Duration,
    ) -> Result<()> {
        let mut controllers = self.controllers.lock();
        let controller = controllers
            .get_mut(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        let child = controller
            .child
            .as_mut()
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        if child.has_exited()? {
            return Err(E2eError::system(format!(
                "controller {index} exited during startup"
            )));
        }
        Ok(())
    }

    /// Waits for the same candidate process to report completed preparation.
    pub(crate) fn wait_for_controller_prepared_within(
        &self,
        index: usize,
        timeout: Duration,
    ) -> Result<()> {
        let started = Instant::now();
        let pid = self.controller_pid(index)?;
        let (address, policy) = {
            let controllers = self.controllers.lock();
            let controller = controllers
                .get(index)
                .ok_or_else(|| E2eError::system("unknown controller"))?;
            (controller.health, Arc::clone(&controller.health_policy))
        };
        loop {
            if self.controller_pid(index)? != pid {
                return Err(E2eError::system(
                    "candidate controller process changed during preparation",
                ));
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!("controller {index} preparation"),
                    timeout,
                ));
            }
            let response =
                self.runtime
                    .block_on(probe_health(address, &policy, "/prepared", remaining));
            if self.controller_pid(index)? != pid {
                return Err(E2eError::system(
                    "candidate controller process changed during preparation",
                ));
            }
            if started.elapsed() >= timeout {
                return Err(E2eError::timeout(
                    format!("controller {index} preparation"),
                    timeout,
                ));
            }
            if response.is_some_and(|(status, _)| status == StatusCode::OK) {
                return Ok(());
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
        }
    }

    pub(crate) fn deregister_controller(&self, index: usize) -> Result<()> {
        let controllers = self.controllers.lock();
        let controller = controllers
            .get(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        self.balancer.deregister(Frontend::Serve, controller.public);
        self.balancer
            .deregister(Frontend::Workers, controller.worker);
        Ok(())
    }

    pub(crate) fn wait_for_controller_leader_within(
        &self,
        index: usize,
        timeout: Duration,
    ) -> Result<()> {
        let identity = self.controller_identity(index)?;
        let start = Instant::now();
        loop {
            let lease = self.observe_leader_lease()?;
            let now = unix_millis()?;
            if lease
                .as_ref()
                .is_some_and(|lease| lease.is_held_by(&identity, now))
            {
                self.release_evidence.lock().push(format!(
                    "controller {index} identity={identity} owns lease {lease:?}"
                ));
                return Ok(());
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!(
                        "controller {index} identity={identity} to own lease; observed {lease:?}"
                    ),
                    timeout,
                ));
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(remaining));
        }
    }

    pub(crate) fn controller_is_leader(&self, index: usize) -> Result<bool> {
        let identity = self.controller_identity(index)?;
        let lease = self.observe_leader_lease()?;
        let now = unix_millis()?;
        Ok(lease.is_some_and(|lease| lease.is_held_by(&identity, now)))
    }

    /// The identity emitted by this process before its completed startup announcement.
    pub(crate) fn controller_identity(&self, index: usize) -> Result<Arc<str>> {
        self.controllers
            .lock()
            .get(index)
            .map(|controller| {
                controller.child.as_ref().map_or_else(
                    || Arc::clone(&controller.identity),
                    ControllerHost::identity,
                )
            })
            .ok_or_else(|| E2eError::system("unknown controller"))
    }

    fn observe_leader_lease(&self) -> Result<Option<LeaderLease>> {
        match &self.database {
            ClusterDatabase::DynamoDb(database) => {
                let item = database.item(TABLE, "lease", "leader");
                decode_leader_lease(item.as_ref())
            }
            ClusterDatabase::Postgres { server, database, .. } => self.runtime.block_on(
                crate::tls_client::within(DIAGNOSTIC_TIMEOUT, "observing postgresql leader lease", async {
                    let (client, connection) = tokio_postgres::Config::new()
                        .host_path(server.socket_dir())
                        .port(server.port())
                        .user(SUPERUSER)
                        .dbname(database)
                        .connect(tokio_postgres::NoTls)
                        .await
                        .map_err(E2eError::system_from("connecting to observe postgresql lease"))?;
                    let mut drivers = JoinSet::new();
                    drivers.spawn(connection);
                    let row = client.query_opt(
                        "SELECT holder, epoch, floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_ms FROM leases WHERE name = $1",
                        &[&"leader"],
                    ).await.map_err(E2eError::system_from("reading postgresql leader lease"))?;
                    drop(client);
                    drivers.join_next().await
                        .ok_or_else(|| E2eError::system("postgresql lease connection driver missing"))?
                        .map_err(E2eError::system_from("joining postgresql lease connection"))?
                        .map_err(E2eError::system_from("driving postgresql lease connection"))?;
                    row.map(|row| {
                        let holder = row.try_get("holder")
                            .map_err(E2eError::system_from("decoding postgresql lease holder"))?;
                        let epoch: i64 = row.try_get("epoch")
                            .map_err(E2eError::system_from("decoding postgresql lease epoch"))?;
                        let expiry: i64 = row.try_get("expires_at_ms")
                            .map_err(E2eError::system_from("decoding postgresql lease expiry"))?;
                        LeaderLease::from_postgres(holder, epoch, expiry)
                    }).transpose()
                }),
            ),
        }
    }

    pub(crate) fn wait_for_controller_health(&self, index: usize) -> Result<()> {
        self.wait_for_controller_health_within(index, self.stack.settings().startup_timeout)
    }

    pub(crate) fn controller_is_healthy(&self, index: usize) -> Result<bool> {
        let controllers = self.controllers.lock();
        let controller = controllers
            .get(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        Ok(self
            .balancer
            .healthy(Frontend::Serve)
            .contains(&controller.public)
            && self
                .balancer
                .healthy(Frontend::Workers)
                .contains(&controller.worker))
    }

    pub(crate) fn wait_for_controller_health_within(
        &self,
        index: usize,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let revision = self.balancer.revision();
            if self.controller_is_healthy(index)? {
                return Ok(());
            }
            if !self
                .balancer
                .wait_change(revision, timeout.saturating_sub(start.elapsed()))
            {
                return Err(E2eError::timeout(
                    format!("controller {index} target groups healthy"),
                    timeout,
                ));
            }
        }
    }

    pub(crate) fn observe_restarted_standby(
        &self,
        old: usize,
        successor: usize,
        original_pid: u32,
    ) -> Result<()> {
        // Cooperative yield keeps refusal listeners alive until actual shutdown. Exercise the
        // host's Restart=always policy with SIGTERM, without disabling its supervisor.
        if self.controller_pid(old)? != original_pid {
            return Err(E2eError::system(
                "outgoing controller changed before SIGTERM",
            ));
        }
        self.signal(old, Signal::TERM, false)?;
        let (pid, _) = {
            let controllers = self.controllers.lock();
            let child = controllers
                .get(old)
                .and_then(|controller| controller.child.as_ref())
                .ok_or_else(|| E2eError::system("original controller host is not running"))?;
            child.wait_restarted(self.stack.settings().startup_timeout)?
        };
        if pid == original_pid {
            return Err(E2eError::system("original controller did not restart"));
        }
        self.wait_for_serving(&[successor])?;
        let start = Instant::now();
        let window = gang_types::health::health_check().healthy_window();
        loop {
            if self.controller_pid(old)? != pid
                || !self.controller_is_leader(successor)?
                || self.controller_is_leader(old)?
                || !self.only_healthy_controller(successor)?
            {
                return Err(E2eError::system(
                    "restarted older controller displaced the successor",
                ));
            }
            if start.elapsed() >= window {
                return Ok(());
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(window.saturating_sub(start.elapsed())));
        }
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

    /// The first unpaused controller reporting active, fenced leadership.
    #[must_use]
    pub fn leader(&self) -> Option<usize> {
        observed_leaders(&self.observe_leadership(Duration::MAX)).next()
    }

    /// All unpaused controllers reporting active, fenced leadership, in controller order.
    ///
    /// Probes each controller directly, including deregistered controllers.
    #[must_use]
    pub fn leaders(&self) -> Vec<usize> {
        observed_leaders(&self.observe_leadership(Duration::MAX)).collect()
    }

    fn observe_leadership(&self, timeout: Duration) -> Vec<Option<LeadershipHealth>> {
        let controllers = self.controllers.lock();
        let mut observations = vec![None; controllers.len()];
        let probes: Vec<_> = controllers
            .iter()
            .enumerate()
            .filter(|(_, controller)| !controller.paused && controller.child.is_some())
            .map(|(index, controller)| {
                (
                    index,
                    controller.health,
                    Arc::clone(&controller.health_policy),
                    controller.child.as_ref().map(ControllerHost::identity),
                )
            })
            .collect();
        drop(controllers);
        let lease = self.observe_leader_lease().ok().flatten();
        let now = unix_millis().ok();
        self.runtime.block_on(async {
            let mut checks = JoinSet::new();
            for (index, address, policy, identity) in probes {
                checks.spawn(async move {
                    (
                        index,
                        identity,
                        probe_leadership(address, &policy, timeout).await,
                    )
                });
            }
            while let Some(result) = checks.join_next().await {
                if let Ok((index, identity, Some(_))) = result {
                    let leading =
                        lease.as_ref().zip(identity.as_ref()).zip(now).is_some_and(
                            |((lease, identity), now)| lease.is_held_by(identity, now),
                        );
                    observations[index] = Some(LeadershipHealth { leading });
                }
            }
        });
        observations
    }

    fn wait_for_initial_leader(&self) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        loop {
            let observations = self.observe_leadership(timeout.saturating_sub(start.elapsed()));
            if let Some(index) = initial_leader(&observations)
                && self.only_healthy_controller(index)?
            {
                return Ok(());
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!(
                        "initial single leader and healthy routes; observations={observations:?}\n{}",
                        self.diagnostics()
                    ),
                    timeout,
                ));
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(remaining));
        }
    }

    fn only_healthy_controller(&self, index: usize) -> Result<bool> {
        let controllers = self.controllers.lock();
        let controller = controllers
            .get(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        Ok(
            self.balancer.healthy(Frontend::Serve) == [controller.public]
                && self.balancer.healthy(Frontend::Workers) == [controller.worker],
        )
    }

    /// Waits for directly observed active, fenced leadership within the startup deadline.
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
            let observations = self.observe_leadership(timeout.saturating_sub(start.elapsed()));
            if let Some(leader) = observed_leaders(&observations).find(|leader| matches(*leader)) {
                return Ok(leader);
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!("a healthy leader\n{}", self.diagnostics()),
                    timeout,
                ));
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(remaining));
        }
    }

    pub(crate) fn wait_for_single_leader(&self, expected: usize) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        loop {
            let observations = self.observe_leadership(timeout.saturating_sub(start.elapsed()));
            let mut leaders = observed_leaders(&observations);
            if leaders.next() == Some(expected) && leaders.next().is_none() {
                return Ok(());
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!(
                        "a single healthy leader {expected}; observations={observations:?}\n{}",
                        self.diagnostics()
                    ),
                    timeout,
                ));
            }
            std::thread::sleep(LEASE_POLL_INTERVAL.min(remaining));
        }
    }

    /// Adds a controller with fresh, fixed listener ports.
    ///
    /// # Errors
    /// Returns an error if ports, configuration, or the child cannot be created.
    pub fn start_controller(&self) -> Result<usize> {
        self.start_controller_inner(
            self.stack.binary(),
            None,
            self.stack.settings().startup_timeout,
        )
    }

    /// Launches a real candidate with its database link stalled, and collects it without promotion.
    pub(crate) fn observe_unprepared_candidate(
        &self,
        leader: usize,
        mut work_is_held: impl FnMut() -> Result<bool>,
    ) -> Result<()> {
        let index = self.controllers.lock().len();
        let addr =
            || -> Result<SocketAddr> { Ok(SocketAddr::from(([127, 0, 0, 1], ports::reserve()?))) };
        let (public, worker, health) = (addr()?, addr()?, addr()?);
        let database_link = self.start_database_link()?;
        {
            let mut state = database_link.state.lock();
            match &self.database {
                ClusterDatabase::DynamoDb(_) => state.stalled = true,
                ClusterDatabase::Postgres { .. } => state.stall_postgres_history = true,
            }
        }
        let config = self
            .stack
            .placement()
            .root()
            .join("controller-unprepared.toml");
        self.write_controller_config(&config, public, worker, health, &database_link)?;
        for (frontend, traffic) in [(Frontend::Serve, public), (Frontend::Workers, worker)] {
            self.balancer.register(frontend, Target { traffic, health });
        }
        let result = (|| {
            let (mut child, _) = self.spawn_controller(index, self.stack.binary(), &config)?;
            let start = Instant::now();
            let window = gang_types::health::health_check().healthy_window();
            loop {
                if child.has_exited()? {
                    return Err(self.controller_start_error(
                        index,
                        &mut child,
                        "exited during deliberately stalled preparation",
                        &self.controller_log_path(index),
                        false,
                    ));
                }
                if !work_is_held()?
                    || !self.controller_is_leader(leader)?
                    || !self.controller_is_healthy(leader)?
                    || self.balancer.healthy(Frontend::Serve).contains(&public)
                    || self.balancer.healthy(Frontend::Workers).contains(&worker)
                {
                    return Err(E2eError::system(
                        "unprepared candidate or original held leader violated the handover contract",
                    ));
                }
                if start.elapsed() >= window {
                    break;
                }
                std::thread::sleep(LEASE_POLL_INTERVAL.min(window.saturating_sub(start.elapsed())));
            }
            if database_link.state.lock().stalled_connections == 0 {
                return Err(E2eError::system(
                    "candidate did not reach the stalled database",
                ));
            }
            child.kill()?;
            Ok(())
        })();
        self.balancer.deregister(Frontend::Serve, public);
        self.balancer.deregister(Frontend::Workers, worker);
        result
    }

    pub(crate) fn start_controller_release(
        &self,
        binary: &Path,
        fixture: &ReleaseFixture,
    ) -> Result<usize> {
        self.start_controller_release_within(binary, fixture, self.stack.settings().startup_timeout)
    }

    pub(crate) fn start_controller_release_within(
        &self,
        binary: &Path,
        fixture: &ReleaseFixture,
        timeout: Duration,
    ) -> Result<usize> {
        self.start_controller_inner(binary, Some(fixture), timeout)
    }

    fn start_controller_inner(
        &self,
        binary: &Path,
        fixture: Option<&ReleaseFixture>,
        timeout: Duration,
    ) -> Result<usize> {
        let mut controllers = self.controllers.lock();
        let index = controllers.len();
        let addr =
            || -> Result<SocketAddr> { Ok(SocketAddr::from(([127, 0, 0, 1], ports::reserve()?))) };
        let (public, worker, health) = (addr()?, addr()?, addr()?);
        let direct_endpoint = Url::parse(&format!("https://{public}"))
            .map_err(E2eError::system_from("forming controller endpoint"))?;
        let database_link = self.start_database_link()?;
        let health_policy = controller_health_policy(fixture)?;
        let config = self
            .stack
            .placement()
            .root()
            .join(format!("controller-{index}.toml"));
        if let Some(fixture) = fixture {
            self.write_release_config(
                &config,
                ControllerAddresses {
                    public,
                    worker,
                    health,
                },
                &database_link,
                fixture,
            )?;
        } else {
            self.write_controller_config(&config, public, worker, health, &database_link)?;
        }
        let public_proxy_protocol_v2 = controller_public_proxy_protocol(&config)?;
        let log = self.controller_log_path(index);
        for (frontend, address) in [(Frontend::Serve, public), (Frontend::Workers, worker)] {
            self.balancer.register(
                frontend,
                Target {
                    traffic: address,
                    health,
                },
            );
        }
        for (frontend, address) in [(Frontend::Serve, public), (Frontend::Workers, worker)] {
            if !self.balancer.registered(frontend, address)
                || self.balancer.healthy(frontend).contains(&address)
            {
                return Err(E2eError::system(
                    "new target must be registered and unhealthy before launch",
                ));
            }
        }
        self.release_evidence.lock().push(format!(
            "controller {index}: targets registered and initially unhealthy before process launch; health path={}",
            health_policy.path,
        ));
        let (mut child, log_start) =
            self.spawn_controller(index, binary, &config)
                .inspect_err(|_| {
                    self.balancer.deregister(Frontend::Serve, public);
                    self.balancer.deregister(Frontend::Workers, worker);
                })?;
        let startup = await_controller_start(&mut child, &log, log_start, timeout);
        let identity = match startup {
            Ok(identity) => identity,
            Err(error) => {
                for (frontend, address) in [(Frontend::Serve, public), (Frontend::Workers, worker)]
                {
                    self.balancer.deregister(frontend, address);
                }
                drop(controllers);
                return Err(self.controller_start_error(
                    index,
                    &mut child,
                    &gang_types::error_chain(&error).to_string(),
                    &log,
                    fixture.is_some(),
                ));
            }
        };
        let child = ControllerHost::start(
            child,
            Arc::clone(&identity),
            self.controller_command(binary, &config),
            log,
            timeout,
            await_controller_start,
        )?;
        controllers.push(Controller {
            public,
            public_proxy_protocol_v2,
            direct_endpoint,
            identity,
            worker,
            health,
            health_policy: Arc::new(health_policy),
            config,
            child: Some(child),
            exit_status: None,
            database_link,
            paused: false,
        });
        Ok(index)
    }

    fn controller_start_error(
        &self,
        index: usize,
        child: &mut Supervised,
        outcome: &str,
        log: &Path,
        release: bool,
    ) -> E2eError {
        let tail = match fs::read(log) {
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
        if release {
            return E2eError::system(format!(
                "controller {index} {outcome}; {state}; log tail {}:\n{tail}",
                log.display()
            ));
        }
        E2eError::system(format!(
            "controller {index} {outcome}; {state}; log tail {}:\n{tail}\n{}",
            log.display(),
            self.diagnostics()
        ))
    }

    fn controller_log_path(&self, index: usize) -> PathBuf {
        self.stack
            .placement()
            .root()
            .join(format!("controller-{index}.log"))
    }

    fn spawn_controller(
        &self,
        index: usize,
        binary: &Path,
        config: &Path,
    ) -> Result<(Supervised, usize)> {
        let log = self.controller_log_path(index);
        let log_start = match fs::metadata(&log) {
            Ok(metadata) => usize::try_from(metadata.len())
                .map_err(E2eError::system_from("measuring controller startup log"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(E2eError::io("measuring controller startup log")(error)),
        };
        let mut command = self.controller_command(binary, config);
        Ok((Supervised::spawn(&mut command, &log)?, log_start))
    }

    fn controller_command(&self, binary: &Path, config: &Path) -> Command {
        let cli = GangCli::new(
            binary.to_path_buf(),
            config.to_path_buf(),
            self.stack.placement().home(),
        );
        let mut command = cli.server();
        configure_fake_aws(&mut command);
        command.env("GANG_LOG", "info");
        command
    }

    fn write_release_config(
        &self,
        config: &Path,
        addresses: ControllerAddresses,
        database_link: &DatabaseLink,
        fixture: &ReleaseFixture,
    ) -> Result<()> {
        let rendered = self.render_release_config(addresses, database_link, fixture)?;
        fs::write(config, rendered).map_err(E2eError::io(format!("writing {}", config.display())))
    }

    fn render_release_config(
        &self,
        addresses: ControllerAddresses,
        database_link: &DatabaseLink,
        fixture: &ReleaseFixture,
    ) -> Result<String> {
        let mut doc: toml::Value =
            toml::from_str(&fixture.gang_toml).map_err(crate::template::template_error)?;
        let local: toml::Value =
            toml::from_str(&fixture.local_config).map_err(crate::template::template_error)?;
        let staged = fs::read_to_string(self.stack.placement().config_file())
            .map_err(E2eError::io("reading staged local configuration"))?;
        let staged: toml::Value =
            toml::from_str(&staged).map_err(crate::template::template_error)?;
        adapt_release_config(
            &mut doc,
            &local,
            &staged,
            addresses,
            &ReleaseServices {
                public: &self.frontend_url(Frontend::Serve, "https")?,
                worker: &self.frontend_url(Frontend::Workers, "grpcs")?,
                database: database_link.address,
                s3: self.s3.endpoint().as_str(),
            },
        )?;
        toml::to_string(&doc).map_err(crate::template::template_error)
    }

    /// The saved configuration used by this controller and its Local workers.
    pub(crate) fn release_config_path(&self, index: usize) -> Result<PathBuf> {
        self.controllers
            .lock()
            .get(index)
            .map(|controller| controller.config.clone())
            .ok_or_else(|| E2eError::system("unknown controller"))
    }

    /// Renders a publication for this instance's existing local topology without installing it.
    pub(crate) fn render_release_config_for(
        &self,
        index: usize,
        fixture: &ReleaseFixture,
    ) -> Result<String> {
        let controllers = self.controllers.lock();
        let controller = controllers
            .get(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        self.render_release_config(
            ControllerAddresses {
                public: controller.public,
                worker: controller.worker,
                health: controller.health,
            },
            &controller.database_link,
            fixture,
        )
    }

    pub(crate) fn controller_pid(&self, index: usize) -> Result<u32> {
        let mut controllers = self.controllers.lock();
        let child = controllers
            .get_mut(index)
            .and_then(|controller| controller.child.as_mut())
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        if child.has_exited()? {
            return Err(E2eError::system("controller has exited"));
        }
        Ok(child.pid())
    }

    /// Disables automatic replacement without signalling or relinquishing the original child.
    pub(crate) fn suppress_controller_restart(&self, index: usize) -> Result<u32> {
        let mut controllers = self.controllers.lock();
        let child = controllers
            .get_mut(index)
            .and_then(|controller| controller.child.as_mut())
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        child.suppress_restart();
        if child.has_exited()? {
            return Err(E2eError::system(
                "controller exited before restart suppression",
            ));
        }
        Ok(child.pid())
    }

    /// Requires a response from the original process's health listener after it yields.
    pub(crate) fn observe_yielded_controller(&self, index: usize, pid: u32) -> Result<()> {
        if self.controller_pid(index)? != pid {
            return Err(E2eError::system(
                "outgoing controller PID changed before SIGTERM",
            ));
        }
        let (address, policy) = {
            let controllers = self.controllers.lock();
            let controller = controllers
                .get(index)
                .ok_or_else(|| E2eError::system("unknown controller"))?;
            (controller.health, Arc::clone(&controller.health_policy))
        };
        let health = self.runtime.block_on(probe_leadership(
            address,
            &policy,
            self.stack.settings().handshake_timeout,
        ));
        if health.is_none_or(|health| health.leading) {
            return Err(E2eError::system(format!(
                "original outgoing controller health did not answer nonleading: {health:?}"
            )));
        }
        Ok(())
    }

    /// Reboots the pinned process from fetched bytes, without re-rendering or changing topology.
    pub(crate) fn restart_release_controller(
        &self,
        index: usize,
        binary: &Path,
        fetched: &str,
    ) -> Result<()> {
        let timeout = self.stack.settings().startup_timeout;
        let start = Instant::now();
        let startup = (|| {
            let mut controllers = self.controllers.lock();
            let controller = controllers
                .get_mut(index)
                .ok_or_else(|| E2eError::system("unknown controller"))?;
            let child = controller
                .child
                .as_mut()
                .ok_or_else(|| E2eError::system("controller is not running"))?;
            let status = child.terminate(timeout.saturating_sub(start.elapsed()))?;
            controller.exit_status = Some(status);
            controller.child = None;
            install_fetched_config(&controller.config, fetched)?;
            let (mut child, log_start) =
                self.spawn_controller(index, binary, &controller.config)?;
            let identity = match await_controller_start(
                &mut child,
                &self.controller_log_path(index),
                log_start,
                timeout.saturating_sub(start.elapsed()),
            ) {
                Ok(identity) => identity,
                Err(error) => {
                    // An explicit configuration restart begins a new observation. Preserve that
                    // attempted child's rejection, not the successful stop of the prior child.
                    child.kill()?;
                    controller.exit_status =
                        Some(child.wait_within(Duration::ZERO, "failed configuration restart")?);
                    return Err(error);
                }
            };
            controller.child = Some(ControllerHost::start(
                child,
                Arc::clone(&identity),
                self.controller_command(binary, &controller.config),
                self.controller_log_path(index),
                timeout.saturating_sub(start.elapsed()),
                await_controller_start,
            )?);
            controller.identity = identity;
            controller.exit_status = None;
            controller.paused = false;
            Ok::<(), E2eError>(())
        })();
        startup.map_err(|error| {
            let log = self.log(index).map_or_else(
                |error| format!("unreadable: {error}"),
                |log| crate::observer::tail(log.as_bytes(), self.stack.settings().log_tail_bytes),
            );
            E2eError::system(format!(
                "controller {index} restart failed: {}; log tail:\n{log}",
                gang_types::error_chain(&error)
            ))
        })?;
        self.wait_for_controller_leader_within(index, timeout.saturating_sub(start.elapsed()))?;
        self.wait_for_controller_health_within(index, timeout.saturating_sub(start.elapsed()))
    }

    /// Installs the fetched bytes and leaves replacement to the running Local provisioner.
    pub(crate) fn restart_release_worker(&self, index: usize, fetched: &str) -> Result<()> {
        let config = self.release_config_path(index)?;
        install_fetched_config(&config, fetched)?;
        process::terminate_workers(&config)
    }

    fn write_controller_config(
        &self,
        config: &Path,
        public: SocketAddr,
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
    fn take_controller(&self, index: usize) -> Result<ControllerHost> {
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
        let started = Instant::now();
        {
            let mut controllers = self.controllers.lock();
            let child = controllers
                .get_mut(index)
                .and_then(|controller| controller.child.as_mut())
                .ok_or_else(|| E2eError::system("controller is not running"))?;
            child.request_exit()?;
        }
        Ok(Retiring { index, started })
    }

    /// Waits for a controller whose graceful handoff has been requested.
    pub(crate) fn finish_retire(&self, retiring: &Retiring) -> Result<ExitStatus> {
        self.finish_retire_within(retiring, self.stack.settings().startup_timeout)
    }

    pub(crate) fn finish_retire_within(
        &self,
        retiring: &Retiring,
        budget: Duration,
    ) -> Result<ExitStatus> {
        let remaining = budget.saturating_sub(retiring.started.elapsed());
        if remaining.is_zero() {
            return Err(E2eError::timeout("controller graceful stop", budget));
        }
        let mut child = self.take_controller(retiring.index)?;
        let status = child
            .wait_stopped(remaining)
            .map_err(|error| self.timeout_evidence(error))?;
        self.remember_exit(retiring.index, status);
        Ok(status)
    }

    /// Requests graceful handoff and waits for the process to exit.
    ///
    /// # Errors
    /// Returns an error for an unknown controller, a signal failure, or an expired deadline.
    pub fn retire(&self, index: usize) -> Result<ExitStatus> {
        self.finish_retire(&self.begin_retire(index)?)
    }
    /// Waits for the original controller child to exit, without stopping its host supervisor.
    ///
    /// A successful drain may already have produced a fresh passive process when this returns.
    ///
    /// # Errors
    /// Returns an error for an unknown controller, failed observation, or timeout.
    pub fn wait_exit(&self, index: usize) -> Result<ExitStatus> {
        let timeout = self.stack.settings().startup_timeout;
        let mut controllers = self.controllers.lock();
        let controller = controllers
            .get_mut(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        if let Some(status) = controller.exit_status {
            return Ok(status);
        }
        let child = controller
            .child
            .as_mut()
            .ok_or_else(|| E2eError::system("controller is not running"))?;
        child.wait_within(timeout, &format!("original controller {index} to exit"))
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
            .map(ControllerHost::pid)
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
        if let Some(child) = self
            .controllers
            .lock()
            .get_mut(index)
            .and_then(|c| c.child.as_mut())
        {
            // This deliberate zombie fault observes the original process after resumption,
            // not a fresh, legitimately contending host process.
            child.suppress_restart();
        }
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

    /// Observes one original controller's live public socket and listener framing.
    pub(crate) fn controller_public_target(&self, index: usize) -> Result<DirectTarget> {
        let mut controllers = self.controllers.lock();
        let controller = controllers
            .get_mut(index)
            .ok_or_else(|| E2eError::system("unknown controller public target"))?;
        let child = controller
            .child
            .as_mut()
            .ok_or_else(|| E2eError::system("controller public target is not running"))?;
        if child.has_exited()? {
            return Err(E2eError::system("controller public target has exited"));
        }
        Ok(DirectTarget {
            address: controller.public,
            proxy_protocol_v2: controller.public_proxy_protocol_v2,
        })
    }

    pub(crate) fn controller_targets_registered(&self, index: usize) -> Result<bool> {
        let controllers = self.controllers.lock();
        let controller = controllers
            .get(index)
            .ok_or_else(|| E2eError::system("unknown controller"))?;
        Ok(self.balancer.registered(Frontend::Serve, controller.public)
            && self
                .balancer
                .registered(Frontend::Workers, controller.worker))
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
        let mut controllers = self.controllers.lock();
        let controller = &mut controllers[index];
        if cut && let Some(child) = controller.child.as_mut() {
            // Preserve the original fenced process's exit and mutation witnesses during this
            // deliberate partition. Healing does not silently introduce a fresh contender.
            child.suppress_restart();
        }
        controller.database_link.set_cut(cut);
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
    fn probe_execution_tls(&self, target: DirectTarget, pin: &CaPin) -> Result<String> {
        let address = target.address;
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
                let mut tcp = TcpStream::connect(address)
                    .await
                    .map_err(E2eError::io(format!("connecting to {address}")))?;
                if target.proxy_protocol_v2 {
                    let source = tcp
                        .local_addr()
                        .map_err(E2eError::io("reading diagnostic PROXY source"))?;
                    let (SocketAddr::V4(source), SocketAddr::V4(destination)) = (source, address)
                    else {
                        return Err(E2eError::system("diagnostic PROXY framing requires IPv4"));
                    };
                    tcp.write_all(&crate::balancer::proxy_v2_tcp_header(source, destination))
                        .await
                        .map_err(E2eError::io("writing diagnostic PROXY header"))?;
                }
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

    /// Probes each controller public endpoint and reports the build's remote rc settings.
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
        let client = self
            .stack
            .administrator()
            .and_then(|administrator| administrator.credentials(&pin))
            .and_then(|credentials| {
                self.frontend_url(Frontend::Serve, "https")
                    .map(|endpoint| (credentials, endpoint))
            });
        let controllers: Vec<_> = self
            .controllers
            .lock()
            .iter()
            .enumerate()
            .map(|(index, controller)| {
                (
                    index,
                    DirectTarget {
                        address: controller.public,
                        proxy_protocol_v2: controller.public_proxy_protocol_v2,
                    },
                )
            })
            .collect();
        let mut public: Vec<_> = controllers
            .iter()
            .map(|(index, target)| (format!("controller {index} public"), *target))
            .collect();
        public.push((
            "balancer serve".to_owned(),
            DirectTarget {
                address: self.balancer.address(Frontend::Serve),
                proxy_protocol_v2: false,
            },
        ));
        for (name, target) in public {
            let address = target.address;
            let probe = |path| match &client {
                Ok((credentials, endpoint)) => crate::upgrade_probe::probe_public(
                    endpoint,
                    credentials,
                    target,
                    path,
                    DIAGNOSTIC_TIMEOUT,
                )
                .unwrap_or_else(|error| format!("error: {}", gang_types::error_chain(&error))),
                Err(_) => "authenticated diagnostic client unavailable".to_owned(),
            };
            let result = probe(gang_wire::routes::GET_SERVER.template);
            lines.push(format!("{name} {address} GET /v1/server: {result}"));
            let tls = self
                .probe_execution_tls(target, &pin)
                .unwrap_or_else(|error| format!("error: {}", gang_types::error_chain(&error)));
            let request = probe(CAPABILITIES_PATH);
            lines.push(format!("{name} {address}: {tls}; {request}"));
        }
        lines.join("\n")
    }

    /// Captures controller, balancer, worker and database evidence before the cluster is dropped.
    fn diagnostics(&self) -> String {
        let limit = self.stack.settings().log_tail_bytes;
        let observations = self.observe_leadership(Duration::MAX);
        let leaders: Vec<_> = observed_leaders(&observations).collect();
        let database = match &self.database {
            ClusterDatabase::DynamoDb(database) => {
                format!(
                    "fake dynamodb requests={}; leader lease={:?}",
                    database.requests(),
                    database.item(TABLE, "lease", "leader"),
                )
            }
            ClusterDatabase::Postgres { paused, .. } => {
                format!("postgresql paused={}", *paused.lock())
            }
        };
        let mut lines = vec![format!(
            "cluster evidence: leaders={leaders:?}; direct health={observations:?}; healthy serve={:?}, workers={:?}; {database}",
            self.balancer.healthy(Frontend::Serve),
            self.balancer.healthy(Frontend::Workers),
        )];
        lines.push(self.s3_evidence());
        let releases = self.release_evidence.lock();
        if !releases.is_empty() {
            lines.push(format!("release topology:\n{}", releases.join("\n")));
        }
        drop(releases);
        {
            let mut controllers = self.controllers.lock();
            for (index, controller) in controllers.iter_mut().enumerate() {
                let state = if let Some(child) = controller.child.as_mut() {
                    let pid = child.pid();
                    match child.has_exited() {
                        Ok(false) => format!("alive pid={pid}, paused={}", controller.paused),
                        Ok(true) => format!("current child exited pid={pid}"),
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
                    "controller {index} identity={}: {state}; log tail {}:\n{log}",
                    controller.identity,
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

fn controller_public_proxy_protocol(config: &Path) -> Result<bool> {
    let source = fs::read_to_string(config)
        .map_err(E2eError::io("reading original controller public policy"))?;
    let doc: toml::Value = toml::from_str(&source).map_err(crate::template::template_error)?;
    let server = doc
        .get("server")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| E2eError::system("original controller has no server policy"))?;
    match server.get("proxy_protocol") {
        None => Ok(false),
        Some(toml::Value::Table(_)) => Ok(true),
        Some(_) => Err(E2eError::system("server.proxy_protocol is not a table")),
    }
}

fn controller_health_policy(fixture: Option<&ReleaseFixture>) -> Result<Policy> {
    if let Some(fixture) = fixture {
        return release_policy(&fixture.workers);
    }
    let probe = HealthProbe::default();
    Ok(Policy {
        path: Frontend::Workers.path().to_owned(),
        interval: probe.interval,
        timeout: probe.timeout,
        healthy_threshold: probe.threshold.get(),
        unhealthy_threshold: probe.threshold.get(),
        proxy_protocol_v2: false,
    })
}

fn install_fetched_config(config: &Path, fetched: &str) -> Result<()> {
    use std::io::Write;

    let parent = config
        .parent()
        .ok_or_else(|| E2eError::system("configuration has no parent directory"))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)
        .map_err(E2eError::io("staging fetched configuration"))?;
    staged
        .write_all(fetched.as_bytes())
        .map_err(E2eError::io("writing fetched configuration"))?;
    staged
        .persist(config)
        .map_err(|error| E2eError::io("installing fetched configuration")(error.error))?;
    Ok(())
}

fn await_controller_start(
    child: &mut Supervised,
    log: &Path,
    log_start: usize,
    timeout: Duration,
) -> Result<Arc<str>> {
    child
        .await_announcements(timeout, is_announcement)?
        .ok_or_else(|| E2eError::system("exited before announcing"))?;
    let log =
        fs::read_to_string(log).map_err(E2eError::io("reading controller startup identity"))?;
    let startup = log
        .get(log_start..)
        .ok_or_else(|| E2eError::system("controller startup log was truncated"))?;
    startup_identity(startup).map(Arc::from)
}

fn startup_identity(log: &str) -> Result<&str> {
    let mut identity = None;
    for line in log.lines() {
        let Some((prefix, fields)) =
            line.split_once("gang_main::server_command: starting controller")
        else {
            continue;
        };
        if prefix.split_whitespace().last() != Some("INFO") {
            continue;
        }
        let mut fields = fields.split_whitespace();
        let id = fields
            .next()
            .and_then(|field| field.strip_prefix("controller="))
            .filter(|id| {
                id.len() == 32
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .ok_or_else(|| E2eError::system("malformed starting controller identity"))?;
        if fields.next().is_some() || identity.replace(id).is_some() {
            return Err(E2eError::system("ambiguous starting controller identity"));
        }
    }
    identity.ok_or_else(|| E2eError::system("missing starting controller INFO identity"))
}

#[derive(Debug, PartialEq, Eq)]
struct LeaderLease {
    holder: Option<String>,
    epoch: u64,
    expires_at: u64,
}

impl LeaderLease {
    fn is_held_by(&self, identity: &str, now: u64) -> bool {
        self.epoch > 0 && self.holder.as_deref() == Some(identity) && self.expires_at > now
    }

    fn from_postgres(holder: Option<String>, epoch: i64, expires_at: i64) -> Result<Self> {
        if holder.as_deref() == Some("") {
            return Err(E2eError::system("empty postgresql lease holder"));
        }
        Ok(Self {
            holder,
            epoch: u64::try_from(epoch)
                .map_err(E2eError::system_from("negative postgresql lease epoch"))?,
            expires_at: u64::try_from(expires_at)
                .map_err(E2eError::system_from("negative postgresql lease expiry"))?,
        })
    }
}

fn decode_leader_lease(
    item: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<LeaderLease>> {
    let Some(item) = item else { return Ok(None) };
    let number = |name: &str| -> Result<u64> {
        item.get(name)
            .and_then(serde_json::Value::as_object)
            .filter(|attribute| attribute.len() == 1)
            .and_then(|value| value.get("N"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                E2eError::system(format!("malformed leader lease {name}.N; item={item:?}"))
            })
    };
    let holder = item
        .get("holder")
        .map(|value| {
            value
                .as_object()
                .filter(|attribute| attribute.len() == 1)
                .and_then(|attribute| attribute.get("S"))
                .and_then(serde_json::Value::as_str)
                .filter(|holder| !holder.is_empty())
                .ok_or_else(|| {
                    E2eError::system(format!("malformed leader lease holder.S; item={item:?}"))
                })
        })
        .transpose()?;
    Ok(Some(LeaderLease {
        holder: holder.map(str::to_owned),
        epoch: number("epoch")?,
        expires_at: number("expires_at")?,
    }))
}

fn unix_millis() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(E2eError::system_from("reading lease observation time"))?;
    u64::try_from(now.as_millis())
        .map_err(E2eError::system_from("representing lease observation time"))
}

fn configure_fake_aws(command: &mut std::process::Command) {
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
}

fn release_policy(frontend: &FrontendPlan) -> Result<Policy> {
    let health = &frontend.health;
    if health.interval_seconds == 0
        || health.timeout_seconds == 0
        || health.healthy_threshold == 0
        || health.unhealthy_threshold == 0
    {
        return Err(E2eError::system("invalid release health policy"));
    }
    Ok(Policy {
        path: health.path.clone(),
        interval: Duration::from_secs(health.interval_seconds),
        timeout: Duration::from_secs(health.timeout_seconds),
        healthy_threshold: health.healthy_threshold,
        unhealthy_threshold: health.unhealthy_threshold,
        proxy_protocol_v2: frontend.proxy_protocol_v2,
    })
}

fn copy_key(target: &mut toml::Value, source: &toml::Value, key: &str) -> Result<()> {
    let value = source
        .get(key)
        .cloned()
        .ok_or_else(|| E2eError::system(format!("missing local {key}")))?;
    target
        .as_table_mut()
        .ok_or_else(|| E2eError::system("configuration is not a table"))?
        .insert(key.to_owned(), value);
    Ok(())
}

fn copy_local_fields(
    target: &mut toml::Value,
    source: &toml::Value,
    path: &[&str],
    fields: &[&str],
) -> Result<()> {
    let mut to = target;
    let mut from = source;
    for part in path {
        to = to
            .get_mut(*part)
            .ok_or_else(|| E2eError::system(format!("missing local {part}")))?;
        from = from
            .get(*part)
            .ok_or_else(|| E2eError::system(format!("missing staged {part}")))?;
    }
    for field in fields {
        if let Some(value) = from.get(*field) {
            to.as_table_mut()
                .ok_or_else(|| E2eError::system("local section is not a table"))?
                .insert((*field).to_owned(), value.clone());
        }
    }
    Ok(())
}

fn adapt_release_config(
    doc: &mut toml::Value,
    local: &toml::Value,
    staged: &toml::Value,
    addresses: ControllerAddresses,
    services: &ReleaseServices<'_>,
) -> Result<()> {
    adapt_local_plugins(doc, local, staged)?;
    let root = doc
        .as_table_mut()
        .ok_or_else(|| E2eError::system("configuration is not a table"))?;
    adapt_addresses(root, addresses, services);
    adapt_fleet(root, local, staged)?;
    adapt_server(root, local, staged)?;
    adapt_backends(root, services)
}

fn adapt_local_plugins(
    doc: &mut toml::Value,
    local: &toml::Value,
    staged: &toml::Value,
) -> Result<()> {
    for key in [
        "provisioner",
        "signer",
        "sandbox",
        "authentication",
        "identity",
    ] {
        copy_key(doc, local, key)?;
    }
    if staged.get("limits").is_some() {
        copy_key(doc, staged, "limits")?;
    } else {
        doc.as_table_mut()
            .ok_or_else(|| E2eError::system("configuration is not a table"))?
            .remove("limits");
    }
    copy_local_fields(
        doc,
        staged,
        &["provisioner", "local"],
        &["state_dir", "mock_imds_listen", "worker_launcher"],
    )?;
    copy_local_fields(doc, staged, &["signer", "file"], &["key_file"])?;
    copy_local_fields(doc, staged, &["sandbox", "bwrap"], &["program"])?;
    copy_local_fields(doc, staged, &["sandbox", "noop"], &["scratch_dir"])?;
    if local["sandbox"]["bwrap"].get("root").is_some() {
        copy_local_fields(doc, staged, &["sandbox", "bwrap"], &["root"])?;
    }
    if local["sandbox"]["bwrap"].get("egress").is_some() {
        copy_local_fields(doc, staged, &["sandbox", "bwrap"], &["egress"])?;
    }
    if local["sandbox"]["noop"].get("environment").is_some() {
        copy_local_fields(doc, staged, &["sandbox", "noop"], &["environment"])?;
    }
    copy_local_fields(
        doc,
        staged,
        &["identity", "providers", "local-workers", "oidc"],
        &["issuer_url"],
    )?;
    copy_local_fields(
        doc,
        staged,
        &["authentication", "clients", "token"],
        &["providers"],
    )?;
    let provider = staged
        .get("identity")
        .and_then(|v| v.get("providers"))
        .and_then(|v| v.get("e2e"))
        .and_then(|v| v.get("oidc"))
        .and_then(toml::Value::as_table)
        .ok_or_else(|| E2eError::system("missing staged test issuer"))?;
    let mut oidc = toml::map::Map::new();
    for key in [
        "issuer_url",
        "audience",
        "principal_claim",
        "required_claims",
    ] {
        if let Some(value) = provider.get(key) {
            oidc.insert(key.into(), value.clone());
        }
    }
    let mut plugin = toml::map::Map::new();
    plugin.insert("oidc".into(), toml::Value::Table(oidc));
    doc.get_mut("identity")
        .and_then(|v| v.get_mut("providers"))
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| E2eError::system("missing local identity providers"))?
        .insert("e2e".into(), toml::Value::Table(plugin));
    Ok(())
}

fn adapt_addresses(
    root: &mut toml::map::Map<String, toml::Value>,
    addresses: ControllerAddresses,
    services: &ReleaseServices<'_>,
) {
    for (key, value) in [
        ("listen", addresses.public.to_string()),
        ("worker_listen", addresses.worker.to_string()),
        ("health_listen", addresses.health.to_string()),
        ("endpoint", services.public.to_string()),
    ] {
        root.insert(key.into(), toml::Value::String(value));
    }
}

fn adapt_fleet(
    root: &mut toml::map::Map<String, toml::Value>,
    local: &toml::Value,
    staged: &toml::Value,
) -> Result<()> {
    let fleets = root
        .get_mut("fleets")
        .and_then(toml::Value::as_array_mut)
        .ok_or_else(|| E2eError::system("missing local fleets"))?;
    fleets.truncate(1);
    let fleet = fleets
        .first_mut()
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| E2eError::system("missing local fleet"))?;
    match local
        .get("fleets")
        .and_then(toml::Value::as_array)
        .and_then(|fleets| fleets.first())
        .and_then(|fleet| fleet.get("options"))
    {
        Some(options) => {
            fleet.insert("options".into(), options.clone());
        }
        None => {
            fleet.remove("options");
        }
    }
    fleet.insert("min_instances".into(), toml::Value::Integer(1));
    if let Some(staged_fleet) = staged
        .get("fleets")
        .and_then(toml::Value::as_array)
        .and_then(|fleets| fleets.first())
    {
        for key in ["disk_per_instance", "sandbox"] {
            if let Some(value) = staged_fleet.get(key) {
                fleet.insert(key.into(), value.clone());
            }
        }
        if local
            .get("fleets")
            .and_then(toml::Value::as_array)
            .and_then(|fleets| fleets.first())
            .and_then(|fleet| fleet.get("action_environment"))
            .is_some()
            && let Some(value) = staged_fleet.get("action_environment")
        {
            fleet.insert("action_environment".into(), value.clone());
        }
    }
    Ok(())
}

fn adapt_server(
    root: &mut toml::map::Map<String, toml::Value>,
    local: &toml::Value,
    staged: &toml::Value,
) -> Result<()> {
    let server = root
        .get_mut("server")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| E2eError::system("missing server"))?;
    let staged_server = staged
        .get("server")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| E2eError::system("missing local server"))?;
    for key in ["ca_pin", "transport", "endpoint_trust"] {
        if (server.contains_key(key) || local.get("server").and_then(|v| v.get(key)).is_some())
            && let Some(value) = staged_server.get(key)
        {
            server.insert(key.into(), value.clone());
        }
    }
    if let Some(proxy) = server.get_mut("proxy_protocol") {
        proxy
            .as_table_mut()
            .ok_or_else(|| E2eError::system("server.proxy_protocol is not a table"))?
            .insert(
                "trusted_cidrs".into(),
                toml::Value::Array(vec![toml::Value::String("127.0.0.1/32".into())]),
            );
    }
    let roles: toml::Value = toml::from_str(
        "[[roles]]\nname = 'administrator'\npermissions = ['*']\nprincipals = [{ provider = 'e2e', principal = 'administrator' }]\n"
    ).map_err(crate::template::template_error)?;
    server.insert("roles".into(), roles["roles"].clone());
    Ok(())
}

fn adapt_backends(
    root: &mut toml::map::Map<String, toml::Value>,
    services: &ReleaseServices<'_>,
) -> Result<()> {
    root.insert(
        "worker_endpoint".into(),
        toml::Value::String(services.worker.to_string()),
    );
    root.insert(
        "database".into(),
        toml::from_str(&format!(
            "[dynamodb]\ntable = \"{TABLE}\"\nregion = \"{REGION}\"\nendpoint = \"http://{}\"\n",
            services.database
        ))
        .map_err(crate::template::template_error)?,
    );
    root.insert("cache".into(), toml::from_str(&format!(
        "[s3]\nregion = \"{REGION}\"\nbucket = \"{BUCKET}\"\nforce_path_style = true\nendpoint = \"{}\"\n", services.s3
    )).map_err(crate::template::template_error)?);
    Ok(())
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
    fn build_until_stopped(
        &self,
        probes: &[Probe],
        stop: tokio::sync::oneshot::Receiver<String>,
    ) -> Result<BuildOutcome> {
        self.stack.build_until_stopped(probes, stop)
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
        let controllers = self.controllers.get_mut();
        // Stop every supervisor before stopping any child or worker. Otherwise another active
        // controller could replace workers while cleanup is walking the shared local topology.
        for controller in controllers.iter_mut() {
            if let Some(child) = controller.child.as_mut() {
                child.suppress_restart();
            }
        }
        for controller in controllers.iter_mut() {
            if let Some(mut child) = controller.child.take() {
                let _ = child.kill();
            }
        }
        for controller in controllers.iter() {
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
    use std::fs;
    use std::time::Duration;

    use gang_testkit::TestResult;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream, UnixListener};
    use tokio::time::timeout;

    use super::{DatabaseLink, LinkTarget, local_evidence};

    const DEADLINE: Duration = Duration::from_secs(5);

    const CONTROLLER_ID: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn local_adaptation_preserves_the_selected_generation_maximum() -> TestResult {
        let local: toml::Value = toml::from_str("[[fleets]]")?;
        for maximum in [1, 2] {
            let mut config: toml::Value =
                toml::from_str(&format!("[[fleets]]\nmax_instances = {maximum}"))?;
            super::adapt_fleet(config.as_table_mut().expect("root table"), &local, &local)?;
            assert_eq!(
                config["fleets"][0]["max_instances"].as_integer(),
                Some(maximum)
            );
        }
        Ok(())
    }

    fn leadership_policy(path: &str) -> crate::balancer::Policy {
        let probe = crate::balancer::Probe::default();
        crate::balancer::Policy {
            path: path.into(),
            interval: probe.interval,
            timeout: DEADLINE,
            healthy_threshold: probe.threshold.get(),
            unhealthy_threshold: probe.threshold.get(),
            proxy_protocol_v2: false,
        }
    }

    async fn answer_health(
        listener: &TcpListener,
        path: &str,
        response: &[u8],
        proxy_protocol_v2: bool,
    ) -> TestResult {
        let (mut stream, _) = timeout(DEADLINE, listener.accept()).await??;
        if proxy_protocol_v2 {
            let mut header = [0; 16];
            timeout(DEADLINE, stream.read_exact(&mut header)).await??;
            assert_eq!(&header, b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0");
        }
        let mut request = Vec::new();
        timeout(DEADLINE, async {
            let mut buffer = [0; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await?;
                if count == 0 || request.len() + count > 4096 {
                    return Err(std::io::Error::other("missing bounded HTTP request"));
                }
                request.extend_from_slice(&buffer[..count]);
            }
            Ok::<_, std::io::Error>(())
        })
        .await??;
        assert!(request.starts_with(format!("GET {path} HTTP/1.1\r\n").as_bytes()));
        timeout(DEADLINE, stream.write_all(response)).await??;
        Ok(())
    }

    #[tokio::test]
    async fn leadership_health_requires_active_header_and_success_status() -> TestResult {
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let address = listener.local_addr()?;
        let policy = leadership_policy("/ready");
        for (response, leading) in [
            (&b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading: false\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading: true\r\ncontent-length: 0\r\n\r\n"[..], true),
            (&b"HTTP/1.1 503 Unavailable\r\ngang-leading: false\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 503 Unavailable\r\ngang-leading: true\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading: invalid\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading:\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading: TRUE\r\ncontent-length: 0\r\n\r\n"[..], false),
            (&b"HTTP/1.1 200 OK\r\ngang-leading: true\r\ngang-leading: false\r\ncontent-length: 0\r\n\r\n"[..], false),
        ] {
            let (health, served) = tokio::join!(
                super::probe_leadership(address, &policy, DEADLINE),
                answer_health(&listener, "/ready", response, false),
            );
            served?;
            let health = health.ok_or("missing health response")?;
            assert_eq!(health.leading, leading);
        }
        Ok(())
    }

    #[tokio::test]
    async fn leadership_probe_honors_rendered_proxy_health_transport() -> TestResult {
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let address = listener.local_addr()?;
        let mut policy = leadership_policy("/rendered-ready");
        policy.proxy_protocol_v2 = true;
        let (health, served) = tokio::join!(
            super::probe_leadership(address, &policy, DEADLINE),
            answer_health(
                &listener,
                "/rendered-ready",
                b"HTTP/1.1 200 OK\r\ngang-leading: true\r\ncontent-length: 0\r\n\r\n",
                true,
            ),
        );
        served?;
        assert!(health.ok_or("missing health response")?.leading);
        Ok(())
    }

    #[tokio::test]
    async fn prepared_probe_uses_explicit_path_and_local_proxy_framing() -> TestResult {
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let address = listener.local_addr()?;
        let mut policy = leadership_policy("/ready");
        policy.proxy_protocol_v2 = true;
        for (response, status) in [
            (
                &b"HTTP/1.1 503 Unavailable\r\ncontent-length: 0\r\n\r\n"[..],
                hyper::StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                &b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"[..],
                hyper::StatusCode::OK,
            ),
        ] {
            let (health, served) = tokio::join!(
                super::probe_health(address, &policy, "/prepared", DEADLINE),
                answer_health(&listener, "/prepared", response, true),
            );
            served?;
            assert_eq!(health, Some((status, false)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn leadership_changes_are_visible_without_balancer_revision_or_membership() -> TestResult
    {
        let clock = std::sync::Arc::new(gang_clock::TestClock::default());
        let balancer =
            crate::balancer::Balancer::start(clock, crate::balancer::Probe::default()).await?;
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let address = listener.local_addr()?;
        let policy = leadership_policy("/ready");
        let revision = balancer.revision();
        // The controller is intentionally absent from every target group.
        for leading in [false, true, false] {
            let response =
                format!("HTTP/1.1 200 OK\r\ngang-leading: {leading}\r\ncontent-length: 0\r\n\r\n");
            let (health, served) = tokio::join!(
                super::probe_leadership(address, &policy, DEADLINE),
                answer_health(&listener, "/ready", response.as_bytes(), false),
            );
            served?;
            let observations = [health];
            assert_eq!(
                super::observed_leaders(&observations).next(),
                leading.then_some(0)
            );
            assert_eq!(balancer.revision(), revision);
            assert!(
                balancer
                    .healthy(crate::balancer::Frontend::Workers)
                    .is_empty()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_health_source_is_never_leadership() -> TestResult {
        let listener = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let address = listener.local_addr()?;
        let policy = leadership_policy("/leader");
        let (health, served) = tokio::join!(
            super::probe_leadership(address, &policy, DEADLINE),
            answer_health(&listener, "/leader", b"not HTTP\r\n\r\n", false),
        );
        served?;
        assert!(health.is_none());
        drop(listener);
        assert!(
            super::probe_leadership(address, &policy, DEADLINE)
                .await
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn initial_newest_selection_does_not_pin_later_leadership() {
        let warm = Some(super::LeadershipHealth { leading: false });
        let active = Some(super::LeadershipHealth { leading: true });
        assert_eq!(super::initial_leader(&[active, warm, warm]), None);
        assert_eq!(super::initial_leader(&[warm, active, warm]), None);
        assert_eq!(super::initial_leader(&[warm, warm, active]), Some(2));
        assert_eq!(super::initial_leader(&[warm, active, active]), None);
        assert_eq!(super::initial_leader(&[None, warm, active]), None);
        assert_eq!(super::initial_leader(&[]), None);
        assert_eq!(super::initial_leader(&[active]), Some(0));
        // Later failover observation never invokes the startup-only newest selection.
        assert_eq!(
            super::observed_leaders(&[active, warm, warm]).collect::<Vec<_>>(),
            [0]
        );
    }

    #[test]
    fn startup_identity_reads_the_published_and_current_info_event() -> TestResult {
        // All producers use tracing::info!(%controller, "starting controller").
        let log = format!(
            "2026-11-19T01:02:03.000000Z  INFO gang_main::server_command: starting controller controller={CONTROLLER_ID}\n\
             2026-11-19T01:02:04.000000Z  INFO gang_server::leadership: lease held holder=fedcba9876543210fedcba9876543210\n\
             listening health 127.0.0.1:1234\nca-pin: sha256:00\n"
        );
        assert_eq!(super::startup_identity(&log)?, CONTROLLER_ID);
        Ok(())
    }

    #[test]
    fn startup_identity_rejects_missing_malformed_and_ambiguous_events() {
        let event = format!(
            "2026-11-19T01:02:03Z  INFO gang_main::server_command: starting controller controller={CONTROLLER_ID}\n"
        );
        for log in [
            String::new(),
            event.replace(" INFO ", " DEBUG "),
            event.replace("controller=", "other="),
            event.replace(CONTROLLER_ID, "abc"),
            event.replace(CONTROLLER_ID, "controller-0123456789abcdef0123456789abcdef"),
            event.replace(CONTROLLER_ID, "0123456789ABCDEF0123456789ABCDEF"),
            format!("{event}{event}"),
            format!(
                "{event}{}",
                event.replace(CONTROLLER_ID, "fedcba9876543210fedcba9876543210")
            ),
        ] {
            assert!(super::startup_identity(&log).is_err(), "{log}");
        }
    }

    #[test]
    fn lease_ownership_requires_matching_unexpired_holder_not_warm_startup() -> TestResult {
        let startup = format!(
            "2026-11-19T01:02:03Z  INFO gang_main::server_command: starting controller controller={CONTROLLER_ID}\n"
        );
        let identity = super::startup_identity(&startup)?;
        let mut item = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
            serde_json::json!({
                "holder": {"S": CONTROLLER_ID}, "epoch": {"N": "7"},
                "expires_at": {"N": "1001"}
            }),
        )?;
        let lease = super::decode_leader_lease(Some(&item))?.ok_or("missing lease")?;
        let postgres = super::LeaderLease::from_postgres(Some(identity.to_owned()), 7, 1001)?;
        assert_eq!(lease, postgres);
        assert!(super::LeaderLease::from_postgres(Some(identity.to_owned()), -1, 1001).is_err());
        assert!(super::LeaderLease::from_postgres(Some(identity.to_owned()), 7, -1).is_err());
        assert!(super::LeaderLease::from_postgres(Some(String::new()), 7, 1001).is_err());
        assert!(
            !super::LeaderLease::from_postgres(Some(identity.to_owned()), 0, 1001)?
                .is_held_by(identity, 1000)
        );
        assert!(lease.is_held_by(identity, 1000));
        assert!(!lease.is_held_by("another-warm-controller", 1000));
        assert!(!lease.is_held_by(identity, 1001));
        assert!(!lease.is_held_by(identity, 1002));
        assert!(format!("{lease:?}").contains("epoch: 7"));
        item.remove("holder");
        let released = super::decode_leader_lease(Some(&item))?.ok_or("missing released lease")?;
        assert_eq!(released, super::LeaderLease::from_postgres(None, 7, 1001)?);
        assert!(!released.is_held_by(identity, 1000));
        assert_eq!(released.epoch, 7);
        assert_eq!(released.expires_at, 1001);
        assert!(super::decode_leader_lease(None)?.is_none());
        Ok(())
    }

    #[test]
    fn malformed_lease_attributes_are_errors_even_after_release() -> TestResult {
        let valid = serde_json::json!({
            "holder": {"S": CONTROLLER_ID}, "epoch": {"N": "7"},
            "expires_at": {"N": "1001"}
        });
        for (name, value) in [
            ("holder", serde_json::json!({"N": "1"})),
            ("holder", serde_json::json!({"S": ""})),
            ("holder", serde_json::json!({"S": CONTROLLER_ID, "N": "1"})),
            ("epoch", serde_json::json!({"S": "7"})),
            ("epoch", serde_json::json!({"N": "-1"})),
            ("epoch", serde_json::json!({"N": "7", "S": "7"})),
            ("expires_at", serde_json::json!({"N": "bad"})),
            (
                "expires_at",
                serde_json::json!({"N": "18446744073709551616"}),
            ),
        ] {
            let mut item = valid.as_object().ok_or("missing object")?.clone();
            item.insert(name.into(), value);
            assert!(super::decode_leader_lease(Some(&item)).is_err(), "{item:?}");
        }
        for name in ["epoch", "expires_at"] {
            let mut item = valid.as_object().ok_or("missing object")?.clone();
            item.remove("holder");
            item.remove(name);
            assert!(super::decode_leader_lease(Some(&item)).is_err(), "{item:?}");
        }
        Ok(())
    }

    #[test]
    fn direct_target_policy_uses_original_public_listener_not_health_or_nlb() -> TestResult {
        let root = tempfile::tempdir()?;
        let config = root.path().join("controller.toml");
        for source in [
            "[server.proxy_protocol]\ntrusted_cidrs = ['127.0.0.1/32']\n",
            "[server.proxy_protocol]\npublic = 'accept'\nworker = 'require'\nhealth = 'off'\ntrusted_cidrs = ['127.0.0.1/32']\n",
        ] {
            fs::write(&config, source)?;
            assert!(super::controller_public_proxy_protocol(&config)?);
        }
        fs::write(&config, "[server]\n")?;
        assert!(!super::controller_public_proxy_protocol(&config)?);
        for invalid in ["", "[server]\nproxy_protocol = 'invalid'\n"] {
            fs::write(&config, invalid)?;
            assert!(super::controller_public_proxy_protocol(&config).is_err());
        }
        Ok(())
    }

    #[test]
    fn release_policy_uses_actual_proxy_flag_and_health_path() -> TestResult {
        for proxy_protocol_v2 in [false, true] {
            let frontend = crate::upgrade_fixture::FrontendPlan {
                port: 443,
                target_port: 8443,
                proxy_protocol_v2,
                preserve_client_ip: Some(false),
                health: crate::upgrade_fixture::HealthPlan {
                    path: "/rendered-health".into(),
                    interval_seconds: 5,
                    timeout_seconds: 2,
                    healthy_threshold: 2,
                    unhealthy_threshold: 2,
                },
            };
            let policy = super::release_policy(&frontend)?;
            assert_eq!(policy.proxy_protocol_v2, proxy_protocol_v2);
            assert_eq!(policy.path, "/rendered-health");
        }
        Ok(())
    }

    #[test]
    fn server_adaptation_changes_only_rendered_proxy_trust() -> TestResult {
        let local: toml::Value = toml::from_str("[server]\ntransport = 'tls'\n")?;
        for source in [
            "[server]\n",
            "[server.proxy_protocol]\npublic = 'accept'\nworker = 'require'\nhealth = 'off'\ntrusted_cidrs = ['10.0.1.2/32', '10.0.2.3/32']\nheader_timeout = '7s'\nmax_pending_headers = 17\nmax_header_size = '4KiB'\n",
        ] {
            let source = format!("{source}[server.leadership]\ncall_timeout = '7s'\n");
            let mut doc: toml::Value = toml::from_str(&source)?;
            let leadership = doc["server"]["leadership"].clone();
            let before = doc["server"].get("proxy_protocol").cloned();
            super::adapt_server(doc.as_table_mut().ok_or("missing root")?, &local, &local)?;
            assert_eq!(doc["server"]["leadership"], leadership);
            if let Some(mut expected) = before {
                expected["trusted_cidrs"] =
                    toml::Value::Array(vec![toml::Value::String("127.0.0.1/32".into())]);
                assert_eq!(doc["server"]["proxy_protocol"], expected);
            } else {
                assert!(doc["server"].get("proxy_protocol").is_none());
            }
        }
        Ok(())
    }

    fn old_local_schema(local: &mut toml::Value) -> TestResult {
        local["fleets"][0]
            .as_table_mut()
            .ok_or("missing fleet")?
            .remove("action_environment");
        local["sandbox"]["noop"]
            .as_table_mut()
            .ok_or("missing noop")?
            .remove("environment");
        let bwrap = local["sandbox"]["bwrap"]
            .as_table_mut()
            .ok_or("missing bwrap")?;
        bwrap.remove("root");
        bwrap.insert("readonly_bind_paths".into(), toml::Value::Array(Vec::new()));
        Ok(())
    }

    fn assert_environment_schema(doc: &toml::Value, modern: bool) {
        let fleet = &doc["fleets"][0];
        assert_eq!(fleet.get("action_environment").is_some(), modern);
        assert_eq!(doc["sandbox"]["noop"].get("environment").is_some(), modern);
        assert_eq!(doc["sandbox"]["bwrap"].get("root").is_some(), modern);
        if modern {
            assert_eq!(fleet["action_environment"].as_str(), Some("host:e2e"));
            assert_eq!(
                doc["sandbox"]["noop"]["environment"].as_str(),
                Some("host:e2e")
            );
            assert_eq!(
                doc["sandbox"]["bwrap"]["root"].as_str(),
                Some("/staged-root")
            );
        } else {
            assert!(doc["sandbox"]["bwrap"].get("readonly_bind_paths").is_some());
        }
    }

    #[test]
    fn release_topology_preserves_combined_frontend() -> TestResult {
        let local: toml::Value = toml::from_str(
            r#"
[[fleets]]
name = "local"
action_environment = "host:e2e"
[provisioner.local]
state_dir = "/tmp/local"
[signer.file]
key_file = "/tmp/key"
[sandbox.noop]
environment = "host:e2e"
[sandbox.bwrap]
root = "/old-root"
[identity.providers.local-workers.oidc]
issuer_url = "http://127.0.0.1:1"
[authentication.clients.token]
providers = ["local-workers"]
"#,
        )?;
        let mut staged = local.clone();
        staged["sandbox"]["bwrap"]["root"] = toml::Value::String("/staged-root".into());
        let identity = staged
            .get_mut("identity")
            .and_then(|v| v.get_mut("providers"))
            .and_then(toml::Value::as_table_mut)
            .ok_or("missing providers")?;
        identity.insert(
            "e2e".into(),
            toml::from_str("[oidc]\nissuer_url = 'http://127.0.0.1:2'\naudience = 'gang'\n")?,
        );
        staged
            .as_table_mut()
            .ok_or("missing table")?
            .insert("server".into(), toml::from_str("transport = 'tls'")?);
        let address = "127.0.0.1:1234".parse()?;
        let public = gang_types::Url::parse("https://127.0.0.1:1234")?;
        let worker = gang_types::Url::parse("grpcs://127.0.0.1:1234")?;
        for modern in [false, true] {
            let mut local = local.clone();
            if !modern {
                old_local_schema(&mut local)?;
            }
            let source = "worker_endpoint = 'grpcs://old:1'\n[server]\n[server.http2]\n";
            let mut doc: toml::Value = toml::from_str(&format!(
                "{source}\n[[fleets]]\nname = 'production'\nvcpus_per_instance = 2\n[fleets.options]\ncapacity = 'spot'\n[limits.cgroup]\n"
            ))?;
            super::adapt_release_config(
                &mut doc,
                &local,
                &staged,
                super::ControllerAddresses {
                    public: address,
                    worker: address,
                    health: address,
                },
                &super::ReleaseServices {
                    public: &public,
                    worker: &worker,
                    database: address,
                    s3: "http://127.0.0.1:1",
                },
            )?;
            let fleet = &doc["fleets"][0];
            assert_eq!(fleet["vcpus_per_instance"].as_integer(), Some(2));
            assert_environment_schema(&doc, modern);
            assert!(fleet.get("options").is_none());
            assert!(doc.get("limits").is_none());
            assert_eq!(doc["worker_endpoint"].as_str(), Some(worker.as_str()));
            assert!(doc.get("scheduler").is_none());
        }
        Ok(())
    }

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
    async fn postgres_stall_allows_bootstrap_but_withholds_fragmented_history_queries() -> TestResult
    {
        let frame = |tag, body: &[u8]| {
            let mut bytes = vec![tag];
            bytes.extend_from_slice(&u32::try_from(body.len() + 4)?.to_be_bytes());
            bytes.extend_from_slice(body);
            Ok::<_, Box<dyn std::error::Error>>(bytes)
        };
        for tag in *b"PQ" {
            let (mut client, incoming) = tokio::io::duplex(4096);
            let (outgoing, mut backend) = tokio::io::duplex(4096);
            let state = parking_lot::Mutex::new(super::LinkState::default());
            let mut forwarding = Box::pin(super::forward_postgres_preparation(
                incoming, outgoing, &state,
            ));
            let startup = b"\0\0\0\x08\0\x03\0\0";
            let authentication = frame(b'P', b"\0SELECT revoked_at FROM principals\0\0\0")?;
            let schema = frame(b'Q', b"CREATE TABLE expiring_records (id bytea)\0")?;
            let bootstrap: Vec<_> = startup
                .iter()
                .chain(&authentication)
                .chain(&schema)
                .copied()
                .collect();
            client.write_all(&bootstrap).await?;
            assert!(futures::poll!(forwarding.as_mut()).is_pending());
            let mut received = vec![0; bootstrap.len()];
            timeout(DEADLINE, backend.read_exact(&mut received)).await??;
            assert_eq!(received, bootstrap);
            assert_eq!(state.lock().stalled_connections, 0);

            let query =
                b"SELECT id, body, version, expires_at FROM expiring_records WHERE kind = $1";
            let mut body = Vec::new();
            if tag == b'P' {
                body.extend_from_slice(b"statement\0");
            }
            body.extend_from_slice(query);
            body.push(0);
            if tag == b'P' {
                body.extend_from_slice(&0u16.to_be_bytes());
            }
            let history = frame(tag, &body)?;
            let split = history.len() / 2;
            client.write_all(&history[..split]).await?;
            assert!(futures::poll!(forwarding.as_mut()).is_pending());
            assert_eq!(state.lock().stalled_connections, 0);
            client.write_all(&history[split..]).await?;
            assert!(futures::poll!(forwarding.as_mut()).is_pending());
            assert_eq!(state.lock().stalled_connections, 1);
            let mut byte = [0];
            {
                let read = backend.read(&mut byte);
                tokio::pin!(read);
                assert!(futures::poll!(read.as_mut()).is_pending());
            }
            drop(forwarding);
            assert_eq!(timeout(DEADLINE, backend.read(&mut byte)).await??, 0);
        }
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
