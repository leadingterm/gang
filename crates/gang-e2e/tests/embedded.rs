//! End-to-end tests of the embedded controller and worker, in one process over loopback.

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use gang_nativelink::{NativeLink, NativeLinkConfig};
use gang_spi::{
    Accepted, Admitted, ApiRequestBody, ApiResponseBody, ApiService, Authenticated, ClientTls,
    ControllerConnections, ControllerReady, ControllerSpec, GateDecision, Listening,
    ObservationSink, Peer, PortError, Rejection, RenderedStoreSpec, RequestGate, RequestScope,
    SchedulerPort, SchedulerSnapshot, StoreAccess, StoreKind, WorkerSpec, WorkerState, WorkerTls,
    full_body,
};
use gang_testkit::{ChildTest, ExtendedKeyUsagePurpose, TestAuthority, TestResult};
use gang_tls::{
    AcceptorSettings, Alpn, CertificateRevoked, ClientAuth, IncomingSettings, RevocationSet,
    TlsAcceptor,
};
use gang_types::{
    Bytes, CertSerial, FleetCatalog, FleetName, FleetProperties, FleetSet, Identity, InstanceId,
    Isa, NonZeroDuration, Principal, ProviderName, Resources, WorkerLifetime,
};
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, BatchUpdateBlobsRequest, Command, Digest, Directory, ExecuteRequest,
    ExecuteResponse, FileNode, FindMissingBlobsRequest, GetActionResultRequest,
    GetCapabilitiesRequest, Platform, ServerCapabilities, batch_update_blobs_request, command,
    platform,
};
#[cfg(unix)]
use nativelink_proto::build::bazel::remote::execution::v2::{BatchReadBlobsRequest, Tree};
#[cfg(target_os = "linux")]
use nativelink_proto::build::bazel::remote::execution::v2::{
    ExecuteOperationMetadata, execution_stage,
};
use nativelink_proto::google::longrunning::Operation;
use nativelink_proto::google::longrunning::operation::Result as OperationResult;
use parking_lot::Mutex;
use prost::Message as _;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt;
#[cfg(target_os = "linux")]
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::Code;
use tonic::codec::Streaming;
use tonic::transport::{Channel, Endpoint};

/// The timeout the controller applies to client actions.
const CLIENT_ACTION_TIMEOUT: Duration = Duration::from_mins(1);
/// The instance the test worker runs on.
const WORKER_INSTANCE: &str = "local-1";
/// Virtual CPUs the test worker registers.
const WORKER_VCPUS: u32 = 4;
/// Bytes in one gibibyte.
const GIB: u64 = 1 << 30;
/// Memory the test worker registers.
const WORKER_MEMORY: u64 = 8 * GIB;
/// Disk every test fleet declares.
const FLEET_DISK: u64 = 100 * GIB;
/// The fast cache budget of the test worker.
const FAST_CAS_BUDGET: u64 = GIB;
const ENTRYPOINT_HELPER: ChildTest = ChildTest::new("entrypoint_helper");
/// The environment variable that makes `entrypoint_helper` do its work.
const ENTRYPOINT_ENV: &str = "GANG_TEST_ENTRYPOINT";
/// The address at which an action waits for permission to continue.
const HOLD_ENV: &str = "GANG_TEST_HOLD";
/// The address held by both an action's entrypoint and its child.
const GROUP_HOLD_ENV: &str = "GANG_TEST_GROUP_HOLD";
/// The address for a helper that leaves its action group after starting a held child.
#[cfg(target_os = "linux")]
const GROUP_LEAVE_ENV: &str = "GANG_TEST_GROUP_LEAVE";
/// The address for the default-memory-floor helper to report its reservation.
#[cfg(target_os = "linux")]
const MEMORY_ENV: &str = "GANG_TEST_MEMORY";
/// Makes the memory output contain a symlink whose target cannot be encoded in REAPI.
#[cfg(target_os = "linux")]
const MEMORY_INVALID_OUTPUT_ENV: &str = "GANG_TEST_MEMORY_INVALID_OUTPUT";
/// The output directory used by the default-memory-floor helper.
#[cfg(target_os = "linux")]
const MEMORY_OUTPUT: &str = "memory-output";
/// Enables the output-directory symlink fixture.
#[cfg(unix)]
const SYMLINK_OUTPUT_ENV: &str = "GANG_TEST_SYMLINK_OUTPUT";
/// The output directory containing the symlink fixture.
#[cfg(unix)]
const SYMLINK_OUTPUT: &str = "symlink-output";
/// The input file of the action, materialised in its working directory.
const INPUT_NAME: &str = "input.txt";
/// The output file of the action, written by `entrypoint_helper`.
const OUTPUT_NAME: &str = "out.txt";
/// The content of the input file.
const HELLO: &[u8] = b"hello";
/// The name of the test worker's fleet.
const DEFAULT_FLEET: &str = "gang-x86";
/// The sandbox of every test fleet.
const SANDBOX: &str = "bwrap";
/// The small fleet of the scaling test.
const SMALL_FLEET: &str = "gang-small";
/// Virtual CPUs of the small fleet.
const SMALL_VCPUS: u32 = 2;
/// Memory of the small fleet.
const SMALL_MEMORY: u64 = 4 * GIB;
/// The large fleet of the scaling test.
const LARGE_FLEET: &str = "gang-large";
/// Virtual CPUs of the large fleet.
const LARGE_VCPUS: u32 = 8;
/// Memory of the large fleet.
const LARGE_MEMORY: u64 = 16 * GIB;

/// The configuration of a controller whose worker endpoint dials the given loopback port, with
/// defaults for everything else.
fn config(worker_port: u16) -> Result<NativeLinkConfig, Box<dyn Error>> {
    let server = serde_json::from_value(json!({ "transport": "plaintext" }))?;
    Ok(NativeLinkConfig::for_controller(
        format!("grpc://127.0.0.1:{worker_port}").parse()?,
        &server,
    ))
}

/// Builds the scheduler adapter from its configuration.
fn build_plugin(config: NativeLinkConfig) -> Result<NativeLink, Box<dyn Error>> {
    Ok(NativeLink::try_new(config)?)
}

/// The fleet of the test worker: x86-64 with the size the test worker registers.
fn default_fleet() -> Result<FleetProperties, Box<dyn Error>> {
    fleet(DEFAULT_FLEET, Isa::X86_64, WORKER_VCPUS, WORKER_MEMORY)
}

/// A fleet named `name` with the given ISA and size, in the test sandbox and with no options.
fn fleet(name: &str, isa: Isa, vcpus: u32, memory: u64) -> Result<FleetProperties, Box<dyn Error>> {
    fleet_with_disk(name, isa, vcpus, memory, FLEET_DISK)
}

/// A fleet with an explicitly declared disk capacity.
fn fleet_with_disk(
    name: &str,
    isa: Isa,
    vcpus: u32,
    memory: u64,
    disk: u64,
) -> Result<FleetProperties, Box<dyn Error>> {
    Ok(FleetProperties::new(
        &FleetName::try_new(name)?,
        isa,
        &SANDBOX,
        &"host:test".parse()?,
        Resources {
            vcpus,
            memory: Bytes::new(memory),
            disk: Bytes::new(disk),
        },
        &gang_types::FleetActionPolicy::default(),
        [],
    ))
}

/// The connections accepted on `listener`, as a stream. The `n`th connection carries `peers[n]`,
/// and the default peer once `peers` runs out. The stream ends when accepting fails.
fn accepted(listener: TcpListener, peers: Vec<Peer>) -> BoxStream<'static, Accepted> {
    let peers = VecDeque::from(peers);
    stream::unfold((listener, peers), |(listener, mut peers)| async move {
        let (socket, _) = listener.accept().await.ok()?;
        let accepted = Accepted {
            connection: Box::new(socket),
            peer: peers.pop_front().unwrap_or_default(),
        };
        Some((accepted, (listener, peers)))
    })
    .boxed()
}

/// Sends on `serving` when the controller first polls the stream, which it does once it serves; the stream yields what `connections` yields.
fn announcing(
    connections: BoxStream<'static, Accepted>,
    serving: oneshot::Sender<()>,
) -> BoxStream<'static, Accepted> {
    stream::once(async move {
        let _ = serving.send(());
    })
    .filter_map(|()| std::future::ready(None))
    .chain(connections)
    .boxed()
}

/// Connection streams that never yield a connection, behind gates that admit every request.
fn no_connections() -> ControllerConnections<TestGate, TestGate, TestApi> {
    let listening = || Listening {
        connections: stream::empty::<Accepted>().boxed(),
        gate: TestGate::allow(),
    };
    ControllerConnections {
        public: listening(),
        worker: listening(),
        api: TestApi,
    }
}

fn caller() -> Result<Authenticated, PortError> {
    Ok(Authenticated {
        identity: Arc::new(Identity::new(
            ProviderName::try_new("test").map_err(PortError::unavailable)?,
            Principal::try_new("tester").map_err(PortError::unavailable)?,
        )),
        valid_until: None,
    })
}

#[cfg(target_os = "linux")]
type PlatformCase<'a> = (
    Option<&'a [(&'a str, &'a str)]>,
    &'a [(&'a str, &'a str)],
    &'a str,
    &'a str,
);

/// A gate that refuses every request with `denial`, or admits every request when there is none.
/// It records the peer of each request it sees.
#[derive(Clone)]
struct TestGate {
    denial: Option<Rejection>,
    peers: Arc<Mutex<Vec<Peer>>>,
}

impl TestGate {
    /// A gate that admits every request.
    fn allow() -> Self {
        Self {
            denial: None,
            peers: Arc::default(),
        }
    }

    /// A gate that refuses every request with `rejection`.
    fn deny(rejection: Rejection) -> Self {
        Self {
            denial: Some(rejection),
            ..Self::allow()
        }
    }

    /// The peers of the requests seen so far, in arrival order.
    fn seen(&self) -> Vec<Peer> {
        self.peers.lock().clone()
    }
}

impl RequestGate for TestGate {
    async fn admit(
        &self,
        _scope: RequestScope,
        peer: &Peer,
        _parts: &mut axum::http::request::Parts,
    ) -> Result<GateDecision, PortError> {
        self.peers.lock().push(peer.clone());
        self.denial.map_or_else(
            || caller().map(|caller| GateDecision::Admit(Admitted::Authenticated(caller))),
            |rejection| Err(PortError::refused(rejection)),
        )
    }
}

/// The Gang API of these tests: it answers every request with `api`.
#[derive(Clone)]
struct TestApi;

impl ApiService for TestApi {
    async fn call(&self, _request: Request<ApiRequestBody>) -> Response<ApiResponseBody> {
        Response::new(full_body(axum::body::Bytes::from_static(b"api")))
    }
}

/// The largest body a test reads from the Gang API.
const BODY_LIMIT: usize = 1 << 20;

/// Sends `GET path` over HTTP/2 to the listener on `port`, and returns the status and body.
async fn http_get(
    port: u16,
    path: &str,
) -> Result<(StatusCode, axum::body::Bytes), Box<dyn Error>> {
    let socket = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    let (mut sender, connection) =
        http2::handshake(TokioExecutor::new(), TokioIo::new(socket)).await?;
    tokio::spawn(connection);
    let request = Request::get(format!("http://127.0.0.1:{port}{path}")).body(Body::empty())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    let body = to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
    Ok((status, body))
}

/// Sends `GET path` over HTTP/1.1 to the listener on `port`, and returns the status and body.
async fn http1_get(
    port: u16,
    path: &str,
) -> Result<(StatusCode, axum::body::Bytes), Box<dyn Error>> {
    let socket = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    let (mut sender, connection) = http1::handshake(TokioIo::new(socket)).await?;
    tokio::spawn(connection);
    let request = Request::get(format!("http://127.0.0.1:{port}{path}")).body(Body::empty())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    let body = to_bytes(Body::new(response.into_body()), BODY_LIMIT).await?;
    Ok((status, body))
}

/// The fleet and execution policy owned by one controller fixture.
struct ControllerPolicy {
    fleets: FleetCatalog,
    actions: Arc<gang_config::ActionsConfig>,
    attempt_budget: Duration,
}

/// Starts the controller in a task. The task builds its own specs from owned stores and serves
/// the connections accepted on its listeners, recording into `observations`. The public listener
/// applies `public_gate` and labels its connections with `public_peers`. The readiness receiver
/// resolves when built; the serving receiver resolves once it polls its public connections.
fn spawn_controller(
    plugin: Arc<NativeLink>,
    policy: ControllerPolicy,
    listeners: (TcpListener, TcpListener),
    public_gate: TestGate,
    public_peers: Vec<Peer>,
    observations: ObservationSink,
) -> (
    JoinHandle<Result<(), PortError>>,
    oneshot::Receiver<()>,
    oneshot::Receiver<()>,
) {
    let (serving_tx, serving_rx) = oneshot::channel();
    let (ready, readiness) = ControllerReady::channel();
    let (public, worker) = listeners;
    let controller = tokio::spawn(async move {
        let ControllerPolicy {
            fleets,
            actions,
            attempt_budget,
        } = policy;
        let content = RenderedStoreSpec(json!({"memory": {}}));
        let action = RenderedStoreSpec(json!({"memory": {}}));
        let spec = ControllerSpec {
            actions,
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            control_acquire_timeout: NonZeroDuration::from_secs(1),
            clock: Arc::new(gang_clock::SystemClock::new()),
            content_store: &content,
            action_store: &action,
            fleets: &fleets,
            max_workers: 8,
            observations: &observations,
            client_action_timeout: CLIENT_ACTION_TIMEOUT,
            attempt_budget,
        };
        let connections = ControllerConnections {
            public: Listening {
                connections: announcing(accepted(public, public_peers), serving_tx),
                gate: public_gate,
            },
            worker: Listening {
                connections: accepted(worker, Vec::new()),
                gate: TestGate::allow(),
            },
            api: TestApi,
        };
        plugin.run_controller(&spec, connections, ready).await
    });
    (controller, serving_rx, readiness)
}

/// Directories and paths that one worker uses.
#[derive(Clone)]
struct WorkerPaths {
    work: PathBuf,
    fast: PathBuf,
}

impl WorkerPaths {
    /// The paths of a worker named `name` under `root`.
    fn new(root: &Path, name: &str) -> Self {
        let base = root.join(name);
        Self {
            work: base.join("work"),
            fast: base.join("fast"),
        }
    }

    /// Prepare the private layout normally created by the worker host.
    fn prepare(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.work)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(self.work.join(".side_channels"))?;
        }
        #[cfg(not(unix))]
        std::fs::create_dir(self.work.join(".side_channels"))?;
        std::fs::create_dir_all(&self.fast)
    }
}

/// Starts a worker in a task. The task builds its own spec from owned data. The worker runs
/// `entrypoint` for every action.
fn spawn_worker(
    plugin: Arc<NativeLink>,
    actions: Arc<gang_config::ActionsConfig>,
    instance: InstanceId,
    paths: WorkerPaths,
    entrypoint: PathBuf,
    properties: FleetProperties,
    stop: oneshot::Receiver<()>,
) -> JoinHandle<Result<(), PortError>> {
    tokio::spawn(async move {
        let content = RenderedStoreSpec(json!({"memory": {}}));
        let action = RenderedStoreSpec(json!({"memory": {}}));
        let spec = WorkerSpec {
            actions,
            memory_pool: None,
            instance,
            properties,
            lifetime: WorkerLifetime::LongLived,
            work_directory: paths.work,
            fast_cas_directory: paths.fast,
            fast_cas_budget: Bytes::new(FAST_CAS_BUDGET),
            content_store: content,
            action_store: action,
            store_access: StoreAccess::ViaController,
            entrypoint,
        };
        plugin
            .run_worker(&spec, WorkerTls::Plain, async {
                let _ = stop.await;
            })
            .await
    })
}

/// A worker task and its graceful stop signal.
struct RunningWorker {
    /// Requests shutdown of the worker.
    stop: oneshot::Sender<()>,
    /// Runs the worker to completion.
    task: JoinHandle<Result<(), PortError>>,
}

/// A serving controller, and the workers started against it. Dropping it stops everything.
struct Harness {
    plugin: Arc<NativeLink>,
    actions: Arc<gang_config::ActionsConfig>,
    controller: JoinHandle<Result<(), PortError>>,
    workers: Vec<RunningWorker>,
    public_port: u16,
    worker_port: u16,
    /// The controller's observation sink, whose traffic counters the tests read.
    observations: ObservationSink,
}

impl Harness {
    /// Starts a controller over the default fleet, and waits until it serves.
    async fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with(vec![default_fleet()?], TestGate::allow(), Vec::new()).await
    }

    /// Binds the two loopback listeners, starts a controller over `fleets` on them, and waits
    /// until it serves. The public listener gates its requests with `public_gate` and labels its
    /// connections with `public_peers`, in accept order.
    async fn start_with(
        fleets: Vec<FleetProperties>,
        public_gate: TestGate,
        public_peers: Vec<Peer>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_configured(
            fleets,
            public_gate,
            public_peers,
            gang_config::ActionsConfig::default(),
            gang_config::WorkerConfig::default(),
        )
        .await
    }

    /// Starts the controller and workers with the supplied action policy and worker settings.
    async fn start_configured(
        fleets: Vec<FleetProperties>,
        public_gate: TestGate,
        public_peers: Vec<Peer>,
        actions: gang_config::ActionsConfig,
        worker_config: gang_config::WorkerConfig,
    ) -> Result<Self, Box<dyn Error>> {
        let public = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let worker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let public_port = public.local_addr()?.port();
        let worker_port = worker.local_addr()?.port();
        let actions = Arc::new(actions);
        let attempt_budget = worker_config.attempt_budget(actions.timeout);
        let mut config = config(worker_port)?;
        config.worker = worker_config;
        let plugin = Arc::new(build_plugin(config)?);
        let fleets = FleetCatalog::try_new(fleets)?;
        let (observations, _receiver) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let (mut controller, serving, readiness) = spawn_controller(
            Arc::clone(&plugin),
            ControllerPolicy {
                fleets,
                actions: Arc::clone(&actions),
                attempt_budget,
            },
            (public, worker),
            public_gate,
            public_peers,
            observations.clone(),
        );
        readiness
            .await
            .map_err(|_| "controller stopped before signalling readiness")?;
        tokio::select! {
            stopped = &mut controller => Err(Box::<dyn Error>::from(format!(
                "controller stopped before serving: {stopped:?}"
            ))),
            serving = serving => serving.map_err(Box::<dyn Error>::from),
        }?;
        Ok(Self {
            plugin,
            actions,
            controller,
            workers: Vec::new(),
            public_port,
            worker_port,
            observations,
        })
    }

    /// Starts a worker on this controller's worker endpoint. The worker runs this test binary
    /// as its entrypoint.
    fn add_worker(
        &mut self,
        paths: WorkerPaths,
        properties: FleetProperties,
    ) -> Result<(), Box<dyn Error>> {
        paths.prepare()?;
        let instance = InstanceId::try_new(WORKER_INSTANCE)?;
        let entrypoint = std::env::current_exe()?;
        let (stop, receiver) = oneshot::channel();
        let handle = spawn_worker(
            Arc::clone(&self.plugin),
            Arc::clone(&self.actions),
            instance,
            paths,
            entrypoint,
            properties,
            receiver,
        );
        self.workers.push(RunningWorker { stop, task: handle });
        Ok(())
    }
}

impl Drop for Harness {
    /// Requests worker shutdown and aborts the worker and controller tasks.
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            let _ = worker.stop.send(());
            worker.task.abort();
        }
        self.controller.abort();
    }
}

/// Runs a small action on a worker of the default fleet, and returns once it completes. A worker
/// takes actions only after it has registered with the scheduler, and an action completes only
/// after a worker took it, so the registration is in the snapshot once this returns.
async fn await_registration(harness: &Harness) -> Result<(), Box<dyn Error>> {
    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("ISA", "x86-64")])?;
    upload_blobs(&channel, action.blobs).await?;
    run_action(&channel, &action.digest).await?;
    Ok(())
}

/// Reads the scheduler snapshot of the plugin.
async fn read_snapshot(harness: &Harness) -> Result<SchedulerSnapshot, Box<dyn Error>> {
    let mut snapshot = SchedulerSnapshot::default();
    harness.plugin.snapshot(&mut snapshot).await?;
    Ok(snapshot)
}

/// The only worker of `snapshot`.
fn sole_worker(snapshot: &SchedulerSnapshot) -> Result<&WorkerState, Box<dyn Error>> {
    match snapshot.workers.as_slice() {
        [worker] => Ok(worker),
        workers => Err(format!("expected one worker, found {}", workers.len()).into()),
    }
}

/// Whether the capabilities advertise remote execution.
fn advertises_execution(capabilities: &ServerCapabilities) -> bool {
    capabilities
        .execution_capabilities
        .as_ref()
        .is_some_and(|execution| execution.exec_enabled)
}

/// A channel to the public listener on `port`.
async fn public_channel(port: u16) -> Result<Channel, Box<dyn Error>> {
    Ok(Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect()
        .await?)
}

/// A channel to the worker listener on `port`.
async fn worker_channel(port: u16) -> Result<Channel, Box<dyn Error>> {
    Ok(Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect()
        .await?)
}

/// Dials the public listener and asks for its capabilities.
async fn plain_capabilities(port: u16) -> Result<ServerCapabilities, Box<dyn Error>> {
    let mut client = CapabilitiesClient::new(public_channel(port).await?);
    Ok(client
        .get_capabilities(GetCapabilitiesRequest {
            instance_name: String::new(),
        })
        .await?
        .into_inner())
}

/// The controller answers capability queries over gRPC on its public listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_serves_capabilities_over_grpc() -> TestResult {
    let harness = Harness::start().await?;
    let capabilities = plain_capabilities(harness.public_port).await?;
    assert!(advertises_execution(&capabilities));
    Ok(())
}

/// A worker whose certificate the controller refuses as revoked ends with that cause before it reconnects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_whose_certificate_is_revoked_ends_instead_of_reconnecting() -> TestResult {
    let ca = TestAuthority::new("test ca")?;
    let server = ca.issue(
        vec!["127.0.0.1".to_owned()],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        [1; 16],
        "server",
    )?;
    let worker = ca.issue(
        Vec::new(),
        vec![ExtendedKeyUsagePurpose::ClientAuth],
        [2; 16],
        "worker",
    )?;
    let scratch = tempfile::tempdir()?;
    let certificate = scratch.path().join("worker.pem");
    let private_key = scratch.path().join("worker-key.pem");
    let server_ca = scratch.path().join("ca.pem");
    std::fs::write(&certificate, worker.pem.as_str())?;
    std::fs::write(&private_key, worker.key_pem.expose())?;
    std::fs::write(&server_ca, ca.pem.as_str())?;

    let revocations = RevocationSet::default();
    revocations.revoke(worker.serial.clone());
    let acceptor = TlsAcceptor::try_new(&AcceptorSettings {
        certificate: &server.pem,
        private_key: &server.key_pem,
        client_ca: &ca.pem,
        revocations: &revocations,
        handshake_timeout: NonZeroDuration::from_secs(10),
        client_auth: ClientAuth::Required,
        alpn: Alpn::H2,
        clock: Arc::new(gang_clock::SystemClock::default()),
    })?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let _incoming = acceptor.incoming(
        listener,
        IncomingSettings {
            max_pending_handshakes: NonZeroUsize::new(8).expect("eight is not zero"),
            accept_error_backoff: NonZeroDuration::from_millis(100),
            refusal_linger: NonZeroDuration::from_secs(2),
        },
        |_| {},
    );

    let mut config = config(port)?;
    config.worker_endpoint = Some(format!("grpcs://127.0.0.1:{port}").parse()?);
    let plugin = build_plugin(config)?;
    let paths = WorkerPaths::new(scratch.path(), "revoked");
    paths.prepare()?;
    let spec = WorkerSpec {
        actions: Arc::new(gang_config::ActionsConfig::default()),
        memory_pool: None,
        instance: InstanceId::try_new(WORKER_INSTANCE)?,
        properties: default_fleet()?,
        lifetime: WorkerLifetime::LongLived,
        work_directory: paths.work,
        fast_cas_directory: paths.fast,
        fast_cas_budget: Bytes::new(FAST_CAS_BUDGET),
        content_store: RenderedStoreSpec(json!({"memory": {}})),
        action_store: RenderedStoreSpec(json!({"memory": {}})),
        store_access: StoreAccess::Direct,
        entrypoint: std::env::current_exe()?,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        plugin.run_worker(
            &spec,
            WorkerTls::Mutual(ClientTls {
                certificate: &certificate,
                private_key: &private_key,
                server_ca: &server_ca,
            }),
            std::future::pending(),
        ),
    )
    .await
    .expect("a revoked worker terminates after receiving the TLS alert");
    let error = result.expect_err("a revoked worker ends with an error instead of reconnecting");
    assert!(
        std::iter::successors(Some(&error as &(dyn Error + 'static)), |&e| e.source())
            .any(<dyn Error>::is::<CertificateRevoked>),
        "the worker's error names the revoked certificate"
    );
    Ok(())
}

/// The snapshot is empty until a worker connects, then lists that worker with its registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_is_empty_then_shows_a_registered_worker() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut harness = Harness::start().await?;
    let mut snapshot = SchedulerSnapshot::default();
    harness.plugin.snapshot(&mut snapshot).await?;
    assert!(snapshot.workers.is_empty());
    assert!(snapshot.queued.is_empty());

    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
    await_registration(&harness).await?;
    let snapshot = read_snapshot(&harness).await?;
    let worker = sole_worker(&snapshot)?;

    let instance = InstanceId::try_new(WORKER_INSTANCE)?;
    assert!(worker.id.as_str().starts_with(&instance.worker_name()));
    assert_eq!(worker.fleet, FleetName::try_new(DEFAULT_FLEET)?);
    assert_eq!(worker.max_inflight_actions, None);
    assert!(!worker.draining);
    Ok(())
}

/// A physical controller drain publishes its intent as well as the connection flag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_a_worker_is_visible_in_the_snapshot() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut harness = Harness::start().await?;
    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
    await_registration(&harness).await?;
    let fleet = FleetName::try_new(DEFAULT_FLEET)?;
    let instance = InstanceId::try_new(WORKER_INSTANCE)?;

    harness.plugin.set_draining(&fleet, &instance, true).await?;
    let snapshot = read_snapshot(&harness).await?;
    assert!(sole_worker(&snapshot)?.draining);
    assert_eq!(snapshot.cordons.len(), 1);
    assert_eq!(*snapshot.cordons[0].instance, instance);

    harness
        .plugin
        .set_draining(&fleet, &instance, false)
        .await?;
    let snapshot = read_snapshot(&harness).await?;
    assert!(!sole_worker(&snapshot)?.draining);
    assert!(snapshot.cordons.is_empty());
    Ok(())
}

/// A physical intent with no connection succeeds and is visible until cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_an_unconnected_instance_records_intent() -> TestResult {
    let harness = Harness::start().await?;
    let fleet = FleetName::try_new(DEFAULT_FLEET)?;
    let instance = InstanceId::try_new("local-9")?;
    harness.plugin.set_draining(&fleet, &instance, true).await?;
    let snapshot = read_snapshot(&harness).await?;
    assert!(snapshot.workers.is_empty());
    assert_eq!(snapshot.cordons.len(), 1);
    assert_eq!(*snapshot.cordons[0].instance, instance);
    harness
        .plugin
        .set_draining(&fleet, &instance, false)
        .await?;
    assert!(read_snapshot(&harness).await?.cordons.is_empty());
    Ok(())
}

/// The snapshot is unavailable before the controller serves. Nothing is listening on the
/// endpoints of this plugin, and nothing dials them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_before_serving_is_unavailable() -> TestResult {
    let plugin = build_plugin(config(0)?)?;
    let mut snapshot = SchedulerSnapshot::default();
    let result = plugin.snapshot(&mut snapshot).await;
    assert!(matches!(result, Err(PortError::Unavailable(_))));
    Ok(())
}

/// A second controller on the same plugin is refused while the first one serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_controller_is_refused() -> TestResult {
    let harness = Harness::start().await?;
    let content = RenderedStoreSpec(json!({"memory": {}}));
    let action = RenderedStoreSpec(json!({"memory": {}}));
    let fleets = FleetCatalog::try_new(vec![default_fleet()?])?;
    let (observations, _receiver) = gang_spi::ObservationSink::channel(
        std::num::NonZeroUsize::MIN,
        std::num::NonZeroUsize::MIN,
    );
    let spec = ControllerSpec {
        actions: Arc::new(gang_config::ActionsConfig::default()),
        scaling: Arc::new(gang_config::ScalingConfig::default()),
        control_acquire_timeout: NonZeroDuration::from_secs(1),
        clock: Arc::new(gang_clock::SystemClock::new()),
        content_store: &content,
        action_store: &action,
        fleets: &fleets,
        max_workers: 8,
        observations: &observations,
        client_action_timeout: CLIENT_ACTION_TIMEOUT,
        attempt_budget: gang_config::WorkerConfig::default()
            .attempt_budget(gang_config::ActionsConfig::default().timeout),
    };
    let (ready, readiness) = ControllerReady::channel();
    let result = harness
        .plugin
        .run_controller(&spec, no_connections(), ready)
        .await;
    assert!(matches!(result, Err(PortError::Config(_))));
    assert!(readiness.await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_original_controller_drains_and_fresh_adapters_republish() -> TestResult {
    let content = RenderedStoreSpec(json!({"memory": {}}));
    let action = RenderedStoreSpec(json!({"memory": {}}));
    let fleets = FleetCatalog::try_new(vec![default_fleet()?])?;
    let (observations, _receiver) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let spec = ControllerSpec {
        actions: Arc::new(gang_config::ActionsConfig::default()),
        scaling: Arc::new(gang_config::ScalingConfig::default()),
        control_acquire_timeout: NonZeroDuration::from_secs(1),
        clock: Arc::new(gang_clock::SystemClock::new()),
        content_store: &content,
        action_store: &action,
        fleets: &fleets,
        max_workers: 8,
        observations: &observations,
        client_action_timeout: CLIENT_ACTION_TIMEOUT,
        attempt_budget: gang_config::WorkerConfig::default()
            .attempt_budget(gang_config::ActionsConfig::default().timeout),
    };
    for _ in 0..2 {
        // A fresh adapter models a fresh host process, never another term on the old engine.
        let plugin = build_plugin(config(0)?)?;
        let (public_tx, public_rx) = futures::channel::mpsc::unbounded::<Accepted>();
        let (worker_tx, worker_rx) = futures::channel::mpsc::unbounded::<Accepted>();
        let connections = ControllerConnections {
            public: Listening {
                connections: public_rx.boxed(),
                gate: TestGate::allow(),
            },
            worker: Listening {
                connections: worker_rx.boxed(),
                gate: TestGate::allow(),
            },
            api: TestApi,
        };
        let (ready, readiness) = ControllerReady::channel();
        let run = plugin.run_controller(&spec, connections, ready);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => return Err(format!("controller stopped early: {result:?}").into()),
            result = readiness => result?,
        }
        let mut snapshot = SchedulerSnapshot::default();
        plugin.snapshot(&mut snapshot).await?;
        assert!(snapshot.workers.is_empty());
        assert!(snapshot.queued.is_empty());
        assert!(snapshot.executing.is_empty());
        assert_controller_reuse_rejected(&plugin, &spec).await?;
        let mut drain = gang_spi::ControllerDrain::default();
        plugin.hand_off(&mut drain).await?;
        assert!(
            drain.complete,
            "the empty original engine is drained before intake ends"
        );
        drop((public_tx, worker_tx));
        run.await?;
        assert!(matches!(
            plugin.snapshot(&mut SchedulerSnapshot::default()).await,
            Err(PortError::Unavailable(_))
        ));
        assert_controller_reuse_rejected(&plugin, &spec).await?;
    }
    Ok(())
}

async fn assert_controller_reuse_rejected(
    plugin: &NativeLink,
    spec: &ControllerSpec<'_>,
) -> TestResult {
    let (ready, readiness) = ControllerReady::channel();
    let Err(error) = plugin.run_controller(spec, no_connections(), ready).await else {
        return Err("an adapter serves exactly one original controller".into());
    };
    assert!(matches!(&error, PortError::Config(_)));
    assert!(
        error
            .to_string()
            .contains("the controller is already serving"),
        "reuse has an unambiguous error: {error:?}"
    );
    assert!(
        readiness.await.is_err(),
        "a rejected second run never republishes readiness"
    );
    Ok(())
}

/// Dropping the controller future stops serving: the snapshot becomes unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_controller_future_stops_serving() -> TestResult {
    let mut harness = Harness::start().await?;
    harness.controller.abort();
    let joined = (&mut harness.controller).await;
    assert!(joined.is_err_and(|error| error.is_cancelled()));

    let mut snapshot = SchedulerSnapshot::default();
    let result = harness.plugin.snapshot(&mut snapshot).await;
    assert!(matches!(result, Err(PortError::Unavailable(_))));
    Ok(())
}

/// The helper action of the worker. Ignored by the ordinary run, it returns at once unless
/// `ENTRYPOINT_ENV` is set; when it is, it copies the input file into the output file in the
/// working directory. The hold environment variables keep one or two processes running.
#[ignore = "child process helper of the embedded tests"]
#[test]
fn entrypoint_helper() -> TestResult {
    if std::env::var_os(ENTRYPOINT_ENV).is_none() {
        return Ok(());
    }
    for (expected, actual) in [
        ("GANG_TEST_EXPECT_NETWORK", "GANG_NETWORK"),
        ("GANG_TEST_EXPECT_CPU", "GANG_CPU_COUNT"),
        ("GANG_TEST_EXPECT_MEMORY", "GANG_MEMORY_KB"),
    ] {
        if let Ok(value) = std::env::var(expected) {
            let observed = std::env::var(actual).ok();
            assert_eq!(
                observed.as_deref(),
                (value != "<absent>").then_some(value.as_str()),
                "{actual}",
            );
        }
    }
    if std::env::var_os("GANG_TEST_EXPECT_SIDE_CHANNEL").is_some() {
        assert_ne!(std::env::var("GANG_SIDE_CHANNEL_FILE")?, "spoofed");
    }
    #[cfg(unix)]
    if std::env::var_os(SYMLINK_OUTPUT_ENV).is_some() {
        create_symlink_output()?;
    }
    #[cfg(target_os = "linux")]
    if std::env::var_os(MEMORY_ENV).is_some()
        || std::env::var_os(MEMORY_INVALID_OUTPUT_ENV).is_some()
    {
        use std::os::unix::ffi::OsStringExt as _;
        use std::os::unix::fs::symlink;

        std::fs::create_dir_all(MEMORY_OUTPUT)?;
        let link = Path::new(MEMORY_OUTPUT).join("input-link");
        let target = if std::env::var_os(MEMORY_INVALID_OUTPUT_ENV).is_some() {
            PathBuf::from(std::ffi::OsString::from_vec(vec![0xff]))
        } else {
            std::env::current_dir()?.join(INPUT_NAME)
        };
        symlink(target, link)?;
    }
    #[cfg(target_os = "linux")]
    if let Some(address) = std::env::var_os(MEMORY_ENV) {
        use std::io::Write as _;

        let mib = if std::env::var_os(MEMORY_INVALID_OUTPUT_ENV).is_some() {
            96
        } else {
            50
        };
        let memory = vec![1_u8; mib * 1024 * 1024];
        std::hint::black_box(&memory);
        let mut stream = std::net::TcpStream::connect(address.to_string_lossy().as_ref())?;
        writeln!(stream, "{}", std::env::var("GANG_MEMORY_KB")?)?;
        std::io::copy(&mut stream, &mut std::io::sink())?;
        std::hint::black_box(&memory);
        std::fs::remove_file(Path::new(MEMORY_OUTPUT).join("input-link"))?;
    }
    #[cfg(target_os = "linux")]
    if let Some(address) = std::env::var_os(GROUP_LEAVE_ENV) {
        use std::io::Write as _;
        use std::os::unix::process::ExitStatusExt as _;

        let mut child = ENTRYPOINT_HELPER
            .command()?
            .env_remove(GROUP_LEAVE_ENV)
            .env(HOLD_ENV, &address)
            .spawn()?;
        gang_os::join_parent_group()?;
        // This connection marks the group transition; the child may connect before or after it.
        std::net::TcpStream::connect(address.to_string_lossy().as_ref())?.write_all(b"r")?;
        if child.wait()?.signal().is_none() {
            return Err("held child did not end by a signal".into());
        }
        std::net::TcpStream::connect(address.to_string_lossy().as_ref())?.write_all(b"k")?;
        return Ok(());
    }
    if let Some(address) = std::env::var_os(GROUP_HOLD_ENV) {
        ENTRYPOINT_HELPER
            .command()?
            .env_remove(GROUP_HOLD_ENV)
            .env(HOLD_ENV, &address)
            .spawn()?;
        let mut stream = std::net::TcpStream::connect(address.to_string_lossy().as_ref())?;
        std::io::copy(&mut stream, &mut std::io::sink())?;
    }
    if let Some(address) = std::env::var_os(HOLD_ENV) {
        let mut stream = std::net::TcpStream::connect(address.to_string_lossy().as_ref())?;
        std::io::copy(&mut stream, &mut std::io::sink())?;
    }
    let input = std::fs::read(INPUT_NAME)?;
    std::fs::write(OUTPUT_NAME, input)?;
    #[cfg(target_os = "linux")]
    if std::env::var_os(MEMORY_INVALID_OUTPUT_ENV).as_deref() == Some(std::ffi::OsStr::new("7")) {
        std::process::exit(7);
    }
    Ok(())
}

/// Creates links without resolving their targets, including dangling and external targets.
#[cfg(unix)]
fn create_symlink_output() -> TestResult {
    use std::os::unix::fs::symlink;

    let root = Path::new(SYMLINK_OUTPUT);
    let nested = root.join("nested");
    std::fs::create_dir_all(&nested)?;
    std::fs::copy(INPUT_NAME, root.join("payload"))?;
    let absolute_root = std::env::current_dir()?.join(root);
    for (name, target) in [
        ("absolute-internal", absolute_root.join("payload")),
        ("absolute-dangling", absolute_root.join("missing")),
        ("relative", PathBuf::from("../payload")),
        ("absolute-external", PathBuf::from("/outside-action-root")),
        ("absolute-lexical", absolute_root.join("nested/../payload")),
    ] {
        symlink(target, nested.join(name))?;
    }
    Ok(())
}

/// The REAPI digest of `bytes`.
fn digest_of(bytes: &[u8]) -> Result<Digest, Box<dyn Error>> {
    let digest = Sha256::digest(bytes);
    let mut hash = String::with_capacity(64);
    for byte in &digest {
        write!(hash, "{byte:02x}")?;
    }
    Ok(Digest {
        hash,
        size_bytes: i64::try_from(bytes.len())?,
    })
}

/// A hand-built REAPI action, and every blob it refers to.
struct TestAction {
    digest: Digest,
    blobs: Vec<(Digest, Vec<u8>)>,
}

/// Builds the action that runs `entrypoint_helper` over an input file holding `HELLO`, with the
/// given platform properties.
fn build_action(platform_properties: &[(&str, &str)]) -> Result<TestAction, Box<dyn Error>> {
    build_action_with(platform_properties, &[])
}

/// Builds an action with additional environment variables.
fn build_action_with(
    platform_properties: &[(&str, &str)],
    env: &[(&str, &str)],
) -> Result<TestAction, Box<dyn Error>> {
    build_action_platforms(Some(platform_properties), platform_properties, env)
}

fn build_action_platforms(
    action_properties: Option<&[(&str, &str)]>,
    command_properties: &[(&str, &str)],
    env: &[(&str, &str)],
) -> Result<TestAction, Box<dyn Error>> {
    let input = HELLO.to_vec();
    let input_digest = digest_of(&input)?;
    let root = Directory {
        files: vec![FileNode {
            name: INPUT_NAME.into(),
            digest: Some(input_digest.clone()),
            is_executable: false,
            ..Default::default()
        }],
        ..Default::default()
    };
    let root_bytes = root.encode_to_vec();
    let root_digest = digest_of(&root_bytes)?;
    let platform = |properties: &[(&str, &str)]| Platform {
        properties: properties
            .iter()
            .copied()
            .chain(std::iter::once((
                gang_types::property::ACTION_ENVIRONMENT,
                "host:test",
            )))
            .map(|(name, value)| platform::Property {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
    };
    let command = Command {
        arguments: ENTRYPOINT_HELPER
            .arguments()
            .into_iter()
            .map(String::from)
            .collect(),
        environment_variables: std::iter::once(command::EnvironmentVariable {
            name: ENTRYPOINT_ENV.into(),
            value: "1".into(),
        })
        .chain(
            env.iter()
                .map(|(name, value)| command::EnvironmentVariable {
                    name: (*name).into(),
                    value: (*value).into(),
                }),
        )
        .collect(),
        output_files: vec![OUTPUT_NAME.into()],
        #[cfg(unix)]
        output_directories: {
            let mut directories = Vec::new();
            #[cfg(target_os = "linux")]
            if env
                .iter()
                .any(|(name, _)| *name == MEMORY_ENV || *name == MEMORY_INVALID_OUTPUT_ENV)
            {
                directories.push(MEMORY_OUTPUT.into());
            }
            if env.iter().any(|(name, _)| *name == SYMLINK_OUTPUT_ENV) {
                directories.push(SYMLINK_OUTPUT.into());
            }
            directories
        },
        platform: Some(platform(command_properties)),
        ..Default::default()
    };
    let command_bytes = command.encode_to_vec();
    let command_digest = digest_of(&command_bytes)?;
    let action = Action {
        command_digest: Some(command_digest.clone()),
        input_root_digest: Some(root_digest.clone()),
        platform: action_properties.map(|properties| {
            if properties.is_empty() {
                Platform::default()
            } else {
                platform(properties)
            }
        }),
        ..Default::default()
    };
    let action_bytes = action.encode_to_vec();
    let action_digest = digest_of(&action_bytes)?;
    Ok(TestAction {
        digest: action_digest.clone(),
        blobs: vec![
            (input_digest, input),
            (root_digest, root_bytes),
            (command_digest, command_bytes),
            (action_digest, action_bytes),
        ],
    })
}

#[test]
fn empty_action_platform_preserves_command_fallback() -> TestResult {
    let fixture = build_action_platforms(Some(&[]), &[("cpu_count", "3")], &[])?;
    let action_bytes = fixture
        .blobs
        .iter()
        .find(|(digest, _)| *digest == fixture.digest)
        .map(|(_, bytes)| bytes)
        .ok_or("missing action blob")?;
    let action = Action::decode(action_bytes.as_slice())?;
    assert!(
        action
            .platform
            .ok_or("missing action platform")?
            .properties
            .is_empty()
    );
    let command_digest = action.command_digest.ok_or("missing command digest")?;
    let command_bytes = fixture
        .blobs
        .iter()
        .find(|(digest, _)| *digest == command_digest)
        .map(|(_, bytes)| bytes)
        .ok_or("missing command blob")?;
    let command = Command::decode(command_bytes.as_slice())?;
    let properties = command
        .platform
        .ok_or("missing command platform")?
        .properties;
    assert!(properties.iter().any(|property| {
        property.name == gang_types::property::ACTION_ENVIRONMENT && property.value == "host:test"
    }));
    assert!(
        properties
            .iter()
            .any(|property| { property.name == "cpu_count" && property.value == "3" })
    );
    Ok(())
}

/// Uploads `blobs` through the client listener's content-addressable storage.
async fn upload_blobs(
    channel: &Channel,
    blobs: Vec<(Digest, Vec<u8>)>,
) -> Result<(), Box<dyn Error>> {
    let requests: Vec<_> = blobs
        .into_iter()
        .map(|(digest, data)| batch_update_blobs_request::Request {
            digest: Some(digest),
            data: data.into(),
            ..Default::default()
        })
        .collect();
    let response = ContentAddressableStorageClient::new(channel.clone())
        .batch_update_blobs(BatchUpdateBlobsRequest {
            instance_name: String::new(),
            requests,
            ..Default::default()
        })
        .await?
        .into_inner();
    if response
        .responses
        .iter()
        .any(|blob| blob.status.as_ref().is_some_and(|status| status.code != 0))
    {
        return Err("the controller rejected an uploaded blob".into());
    }
    Ok(())
}

/// Reads the operation stream until the action is done, and returns its response.
async fn await_response(
    mut operations: Streaming<Operation>,
) -> Result<ExecuteResponse, Box<dyn Error>> {
    while let Some(operation) = operations.message().await? {
        if !operation.done {
            continue;
        }
        return match operation.result {
            Some(OperationResult::Response(response)) => {
                Ok(ExecuteResponse::decode(response.value.as_ref())?)
            }
            Some(OperationResult::Error(status)) => {
                Err(format!("the action failed: {}", status.message).into())
            }
            None => Err("the operation is done without a result".into()),
        };
    }
    Err("the operation stream ended before the action was done".into())
}

/// Reads until the action goes from executing to queued `refusals` times, or errors if it completes first.
#[cfg(target_os = "linux")]
async fn await_refusals(
    operations: &mut Streaming<Operation>,
    refusals: usize,
) -> Result<(), Box<dyn Error>> {
    let mut previous = None;
    let mut observed = 0;
    while observed < refusals {
        let operation = operations
            .message()
            .await?
            .ok_or("second action's stream ended before two refusals")?;
        if operation.done {
            return Err("second action completed before two disk-guard refusals".into());
        }
        let metadata = operation.metadata.ok_or("operation has no metadata")?;
        let stage = ExecuteOperationMetadata::decode(metadata.value.as_ref())?.stage;
        if previous == Some(execution_stage::Value::Executing as i32)
            && stage == execution_stage::Value::Queued as i32
        {
            observed += 1;
        }
        previous = Some(stage);
    }
    Ok(())
}

/// Executes the action with the cache lookup skipped, and returns the response once it is done.
async fn run_action(channel: &Channel, action: &Digest) -> Result<ExecuteResponse, Box<dyn Error>> {
    let request = ExecuteRequest {
        instance_name: String::new(),
        skip_cache_lookup: true,
        action_digest: Some(action.clone()),
        ..Default::default()
    };
    let operations = ExecutionClient::new(channel.clone())
        .execute(request)
        .await?
        .into_inner();
    await_response(operations).await
}

/// Reads the action result cached for `action` through the public listener's action cache.
async fn cached_result(channel: &Channel, action: &Digest) -> Result<ActionResult, Box<dyn Error>> {
    let request = GetActionResultRequest {
        instance_name: String::new(),
        action_digest: Some(action.clone()),
        ..Default::default()
    };
    Ok(ActionCacheClient::new(channel.clone())
        .get_action_result(request)
        .await?
        .into_inner())
}

/// The output file `OUTPUT_NAME` of `result`, or an error when it is not listed.
fn output_digest(result: &ActionResult) -> Result<Option<&Digest>, Box<dyn Error>> {
    let output = result
        .output_files
        .iter()
        .find(|file| file.path == OUTPUT_NAME)
        .ok_or("the result does not list out.txt")?;
    Ok(output.digest.as_ref())
}

/// An action runs on the worker with this test binary as its entrypoint, and its result is cached.
///
/// This covers scheduling, input fetch through the worker listener's gRPC stores, execution
/// through the entrypoint, the upload of the result to the action cache, and the cache traffic
/// and store usage the controller reports: client calls only, never the worker's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_action_runs_and_its_result_is_cached() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut harness = Harness::start().await?;
    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;

    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("ISA", "x86-64")])?;
    upload_blobs(&channel, action.blobs).await?;
    let uploaded = harness.observations.traffic();
    assert!(uploaded.uploaded > 0, "the client's blob upload is counted");

    let response = run_action(&channel, &action.digest).await?;
    // The worker fetched inputs and uploaded the result over the unobserved worker listener, and
    // `Execute` carries no counted bytes.
    assert_eq!(harness.observations.traffic(), uploaded);
    let result = response
        .result
        .ok_or("the response carries no action result")?;
    assert_eq!(result.exit_code, 0);
    let expected = digest_of(HELLO)?;
    assert_eq!(output_digest(&result)?, Some(&expected));

    // The scheduler does no cache lookup, so a second `Execute` would run the action again.
    // The result is checked through the action cache instead, where the worker uploaded it.
    let cached = cached_result(&channel, &action.digest).await?;
    assert_eq!(cached.exit_code, 0);
    assert_eq!(output_digest(&cached)?, Some(&expected));
    let looked_up = harness.observations.traffic();
    assert_eq!((looked_up.lookups, looked_up.hits), (1, 1));
    assert!(looked_up.downloaded > uploaded.downloaded);

    // Both memory stores hold what the client and the worker uploaded.
    let snapshot = read_snapshot(&harness).await?;
    let stores: Vec<StoreKind> = snapshot.stores.iter().map(|row| row.store).collect();
    assert_eq!(stores, [StoreKind::Content, StoreKind::Action]);
    assert!(snapshot.stores.iter().all(|row| row.used > 0));

    // The worker uploaded the output blob to the shared CAS, so the client finds it present.
    let output = output_digest(&cached)?
        .cloned()
        .ok_or("the cached result does not list out.txt's digest")?;
    let missing = ContentAddressableStorageClient::new(channel.clone())
        .find_missing_blobs(FindMissingBlobsRequest {
            instance_name: String::new(),
            blob_digests: vec![output],
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(missing.missing_blob_digests.len(), 0);
    Ok(())
}

/// Reads one blob through the client listener.
#[cfg(unix)]
async fn read_blob(channel: &Channel, digest: Digest) -> Result<Vec<u8>, Box<dyn Error>> {
    let response = ContentAddressableStorageClient::new(channel.clone())
        .batch_read_blobs(BatchReadBlobsRequest {
            instance_name: String::new(),
            digests: vec![digest],
            ..Default::default()
        })
        .await?
        .into_inner();
    let blob = response
        .responses
        .into_iter()
        .next()
        .ok_or("missing blob response")?;
    let status = blob.status.ok_or("missing blob status")?;
    assert_eq!(status.code, 0, "blob read failed: {status:?}");
    Ok(blob.data.to_vec())
}

/// Verifies the serialized tree and reads the copied payload from CAS.
#[cfg(unix)]
async fn assert_symlink_tree(channel: &Channel, tree_digest: Digest) -> TestResult {
    let tree = Tree::decode(read_blob(channel, tree_digest).await?.as_slice())?;
    let root = tree.root.ok_or("tree has no root")?;
    assert_eq!(root.files.len(), 1);
    assert_eq!(root.files[0].name, "payload");
    let payload = root.files[0]
        .digest
        .clone()
        .ok_or("payload has no digest")?;
    assert_eq!(payload, digest_of(HELLO)?);
    assert_eq!(read_blob(channel, payload).await?, HELLO);
    assert!(root.symlinks.is_empty());
    assert_eq!(root.directories.len(), 1);
    assert_eq!(root.directories[0].name, "nested");
    let nested_digest = root.directories[0]
        .digest
        .as_ref()
        .ok_or("nested has no digest")?;
    let nested = tree
        .children
        .iter()
        .find(|child| {
            digest_of(&child.encode_to_vec()).is_ok_and(|digest| &digest == nested_digest)
        })
        .ok_or("the tree does not hold the nested directory")?;
    assert!(nested.files.is_empty());
    assert!(nested.directories.is_empty());
    let targets: std::collections::BTreeMap<_, _> = nested
        .symlinks
        .iter()
        .map(|link| (link.name.as_str(), link.target.as_str()))
        .collect();
    assert_eq!(nested.symlinks.len(), 5);
    assert_eq!(
        targets,
        [
            ("absolute-dangling", "../missing"),
            ("absolute-external", "/outside-action-root"),
            ("absolute-internal", "../payload"),
            ("absolute-lexical", "../payload"),
            ("relative", "../payload"),
        ]
        .into_iter()
        .collect()
    );
    Ok(())
}

#[cfg(unix)]
async fn assert_absolute_internal_output_symlinks_are_rebased(paths: WorkerPaths) -> TestResult {
    let flow: Result<TestResult, _> = tokio::time::timeout(Duration::from_mins(2), async {
        let mut harness = Harness::start().await?;
        harness.add_worker(paths, default_fleet()?)?;
        let channel = public_channel(harness.public_port).await?;
        let action = build_action_with(&[("ISA", "x86-64")], &[(SYMLINK_OUTPUT_ENV, "1")])?;
        upload_blobs(&channel, action.blobs).await?;
        let response = run_action(&channel, &action.digest).await?;
        let status = response.status.ok_or("the response carries no status")?;
        assert_eq!(status.code, 0, "action failed: {status:?}");
        let result = response
            .result
            .ok_or("the response carries no action result")?;
        assert_eq!(result.exit_code, 0);
        assert_eq!(output_digest(&result)?, Some(&digest_of(HELLO)?));
        assert_eq!(result.output_directories.len(), 1);
        let output = &result.output_directories[0];
        assert_eq!(output.path, SYMLINK_OUTPUT);
        let tree_digest = output
            .tree_digest
            .clone()
            .ok_or("output has no tree digest")?;
        assert_symlink_tree(&channel, tree_digest).await
    })
    .await;
    flow.map_err(|_| "timed out waiting for the symlink output action")?
}

/// Internal absolute links are rebased relative to their containing output directory.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_internal_output_symlinks_are_rebased_from_their_parent() -> TestResult {
    let scratch = tempfile::tempdir()?;
    assert_absolute_internal_output_symlinks_are_rebased(WorkerPaths::new(scratch.path(), "worker"))
        .await
}

/// A symlinked work root does not prevent internal absolute links from being rebased.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_internal_output_symlinks_are_rebased_through_a_symlinked_work_root() -> TestResult
{
    let scratch = tempfile::tempdir()?;
    let physical = scratch.path().join("physical");
    std::fs::create_dir(&physical)?;
    let alias = scratch.path().join("alias");
    std::os::unix::fs::symlink(physical.canonicalize()?, &alias)?;
    assert_absolute_internal_output_symlinks_are_rebased(WorkerPaths::new(&alias, "worker")).await
}

/// A worker-marked action still runs the ordinary entrypoint and produces its output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_worker_marked_action_runs_the_entrypoint() -> TestResult {
    let flow: Result<TestResult, _> = tokio::time::timeout(Duration::from_mins(2), async {
        let scratch = tempfile::tempdir()?;
        let mut harness = Harness::start().await?;
        harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
        let channel = public_channel(harness.public_port).await?;
        let action = build_action(&[("ISA", "x86-64"), ("supports-workers", "1")])?;
        upload_blobs(&channel, action.blobs).await?;

        let response = run_action(&channel, &action.digest).await?;
        let result = response
            .result
            .ok_or("the response carries no action result")?;
        assert_eq!(result.exit_code, 0);
        assert_eq!(output_digest(&result)?, Some(&digest_of(HELLO)?));
        Ok(())
    })
    .await;
    flow.map_err(|_| "timed out waiting for the worker-marked action")?
}

/// Stopping a worker kills the complete action process group and removes its directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_a_worker_ends_every_process_of_a_running_action() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let paths = WorkerPaths::new(scratch.path(), "worker");
    let mut harness = Harness::start().await?;
    harness.add_worker(paths.clone(), default_fleet()?)?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?.to_string();
    let action = build_action_with(&[("ISA", "x86-64")], &[(GROUP_HOLD_ENV, &address)])?;
    let channel = public_channel(harness.public_port).await?;
    upload_blobs(&channel, action.blobs).await?;
    let _operations = ExecutionClient::new(channel)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(action.digest),
            ..Default::default()
        })
        .await?
        .into_inner();
    let (mut first, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept()).await??;
    let (mut second, _) =
        tokio::time::timeout(Duration::from_secs(20), listener.accept()).await??;
    let worker = harness.workers.pop().ok_or("missing worker")?;
    worker
        .stop
        .send(())
        .map_err(|()| "worker stopped before shutdown")?;
    worker.task.await??;
    for stream in [&mut first, &mut second] {
        let mut byte = [0];
        match stream.read(&mut byte).await {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => return Err(format!("action process still sent data: {result:?}").into()),
        }
    }
    let entries = std::fs::read_dir(&paths.work)?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(entries.len(), 1, "only the side-channel directory remains");
    assert_eq!(entries[0].file_name(), ".side_channels");
    assert_eq!(std::fs::read_dir(entries[0].path())?.count(), 0);
    Ok(())
}

/// `NativeLink` excludes its worker's process group when signalling action members.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nativelink_kill_never_reaches_the_workers_process_group() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let paths = WorkerPaths::new(scratch.path(), "worker");
    let mut harness = Harness::start().await?;
    harness.add_worker(paths, default_fleet()?)?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?.to_string();
    let action = build_action_with(&[("ISA", "x86-64")], &[(GROUP_LEAVE_ENV, &address)])?;
    let channel = public_channel(harness.public_port).await?;
    upload_blobs(&channel, action.blobs).await?;
    let _operations = ExecutionClient::new(channel)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(action.digest),
            ..Default::default()
        })
        .await?
        .into_inner();
    // Both the child's hold connection and the helper's transition acknowledgement must arrive.
    let (_first, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept()).await??;
    let (_second, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept()).await??;
    let worker = harness.workers.pop().ok_or("missing worker")?;
    worker
        .stop
        .send(())
        .map_err(|()| "worker stopped before shutdown")?;
    let (mut result, _) =
        tokio::time::timeout(Duration::from_secs(20), listener.accept()).await??;
    let mut byte = [0];
    result.read_exact(&mut byte).await?;
    assert_eq!(&byte, b"k", "helper did not report a signalled child");
    worker.task.await??;
    Ok(())
}

/// A client-requested 26,864 KiB reservation is raised to the default memory floor before dispatch.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subfloor_request_runs_with_the_default_memory_floor() -> TestResult {
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir()?;
        let mut harness = Harness::start().await?;
        harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?.to_string();
        let action = build_action_with(
            &[("ISA", "x86-64"), ("memory_kb", "26864")],
            &[(MEMORY_ENV, &address)],
        )?;
        let channel = public_channel(harness.public_port).await?;
        upload_blobs(&channel, action.blobs).await?;
        let execution = run_action(&channel, &action.digest);
        tokio::pin!(execution);

        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            response = &mut execution => {
                response?;
                return Err("action completed before the memory helper connected".into());
            }
        };
        let mut stream = BufReader::new(stream);
        let mut reservation = String::new();
        stream.read_line(&mut reservation).await?;
        assert_eq!(reservation.trim(), "65536");
        drop(stream);

        let response = execution.await?;
        let result = response
            .result
            .ok_or("the response carries no action result")?;
        assert_eq!(result.exit_code, 0);
        assert_eq!(output_digest(&result)?, Some(&digest_of(HELLO)?));
        Ok::<(), Box<dyn Error>>(())
    })
    .await?
}

/// An upload error after a memory kill must not hide the kill from the retry scheduler.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_kill_survives_an_output_upload_error() -> TestResult {
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir()?;
        let mut harness = Harness::start_configured(
            vec![default_fleet()?],
            TestGate::allow(),
            Vec::new(),
            gang_config::ActionsConfig {
                memory: gang_config::MemoryTuning {
                    enforcement: gang_config::MemoryEnforcement::Soft,
                    ..Default::default()
                },
                ..Default::default()
            },
            gang_config::WorkerConfig::default(),
        )
        .await?;
        harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?.to_string();
        let action = build_action_with(
            &[("ISA", "x86-64"), ("memory_kb", "65536")],
            &[(MEMORY_ENV, &address), (MEMORY_INVALID_OUTPUT_ENV, "1")],
        )?;
        let channel = public_channel(harness.public_port).await?;
        upload_blobs(&channel, action.blobs).await?;
        let execution = run_action(&channel, &action.digest);
        tokio::pin!(execution);

        let (first, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            response = &mut execution => {
                response?;
                return Err("action completed before the first helper connected".into());
            }
        };
        let mut first = BufReader::new(first);
        let mut reservation = String::new();
        tokio::select! {
            read = first.read_line(&mut reservation) => { read?; }
            response = &mut execution => {
                response?;
                return Err("action completed before the first reservation".into());
            }
        }
        assert_eq!(reservation.trim(), "65536");
        let mut byte = [0];
        let closed = tokio::select! {
            read = first.read(&mut byte) => read,
            response = &mut execution => {
                response?;
                return Err("action completed before the first helper was killed".into());
            }
        };
        match closed {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => {
                return Err(
                    format!("first helper did not close its connection: {result:?}").into(),
                );
            }
        }

        let (second, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            response = &mut execution => {
                response?;
                return Err("action completed before the retry connected".into());
            }
        };
        let mut second = BufReader::new(second);
        reservation.clear();
        tokio::select! {
            read = second.read_line(&mut reservation) => { read?; }
            response = &mut execution => {
                response?;
                return Err("action completed before the retry reservation".into());
            }
        }
        assert_eq!(reservation.trim(), "131072");
        drop(second);

        let response = execution.await?;
        let result = response
            .result
            .ok_or("the response carries no action result")?;
        assert_eq!(result.exit_code, 0);
        assert_eq!(output_digest(&result)?, Some(&digest_of(HELLO)?));
        Ok::<(), Box<dyn Error>>(())
    })
    .await?
}

/// Upload failures from normally completed actions remain errors.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_output_upload_error_is_not_a_successful_action() -> TestResult {
    tokio::time::timeout(Duration::from_secs(30), async {
        let scratch = tempfile::tempdir()?;
        let mut harness = Harness::start_configured(
            vec![default_fleet()?],
            TestGate::allow(),
            Vec::new(),
            gang_config::ActionsConfig {
                max_job_retries: NonZeroU32::MIN,
                ..Default::default()
            },
            gang_config::WorkerConfig::default(),
        )
        .await?;
        harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
        let channel = public_channel(harness.public_port).await?;
        for exit in ["0", "7"] {
            let action =
                build_action_with(&[("ISA", "x86-64")], &[(MEMORY_INVALID_OUTPUT_ENV, exit)])?;
            upload_blobs(&channel, action.blobs).await?;
            let response = run_action(&channel, &action.digest).await?;
            let status = response.status.ok_or("upload failure has no status")?;
            assert_ne!(status.code, 0, "exit {exit}: upload failure returned OK");
            let message = status.message;
            assert!(
                message.contains("Error while uploading results")
                    || message.contains("Could not convert") && message.contains("RelativePath"),
                "exit {exit}: expected an upload or path conversion error, got {message}"
            );
            assert!(
                response.result.as_ref().is_none_or(|result| {
                    !result
                        .output_files
                        .iter()
                        .any(|file| file.path == OUTPUT_NAME)
                }),
                "exit {exit}: upload failure returned the output file"
            );
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await?
}

/// Disk-guard backpressure requeues an action without exhausting its retry allowance.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_disk_guard_refusal_requeues_without_spending_an_attempt() -> TestResult {
    let mut waiting_for = "test setup";
    let flow: Result<TestResult, _> = tokio::time::timeout(Duration::from_mins(2), async {
        let scratch = tempfile::tempdir()?;
        let paths = WorkerPaths::new(scratch.path(), "worker");
        std::fs::create_dir_all(&paths.work)?;
        std::fs::create_dir_all(&paths.fast)?;
        let work_dir = paths.work.to_str().ok_or("worker path is not UTF-8")?;
        let free_kb = nativelink_worker::running_actions_manager::free_disk_kb(work_dir)
            .ok_or("could not read free worker disk space")?;
        let reserve_kb = free_kb / 5 * 3;
        assert!(
            reserve_kb > free_kb / 2,
            "insufficient free disk for the test"
        );
        let fleet_disk = free_kb
            .checked_mul(2)
            .and_then(|kb| kb.checked_mul(1024))
            .and_then(|bytes| bytes.checked_add(GIB))
            .ok_or("fleet disk capacity overflow")?;
        let fleet = fleet_with_disk(
            DEFAULT_FLEET,
            Isa::X86_64,
            WORKER_VCPUS,
            WORKER_MEMORY,
            fleet_disk,
        )?;
        let mut harness = Harness::start_configured(
            vec![fleet.clone()],
            TestGate::allow(),
            Vec::new(),
            gang_config::ActionsConfig {
                disk: gang_config::DiskTuning {
                    enforcement: gang_config::DiskEnforcement::Guard,
                    ..Default::default()
                },
                max_job_retries: NonZeroU32::MIN,
                ..Default::default()
            },
            gang_config::WorkerConfig {
                keepalive_interval: gang_config::WholeSeconds::from_units(1),
                connect_timeout: gang_config::WholeSeconds::from_units(2),
                ..Default::default()
            },
        )
        .await?;
        harness.add_worker(paths, fleet)?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?.to_string();
        let disk = reserve_kb.to_string();
        let held = build_action_with(&[("disk_kb", &disk)], &[(HOLD_ENV, &address)])?;
        let second = build_action(&[("disk_kb", &disk)])?;
        let channel = public_channel(harness.public_port).await?;
        upload_blobs(&channel, held.blobs).await?;
        upload_blobs(&channel, second.blobs).await?;

        let first_channel = channel.clone();
        let first = tokio::spawn(async move {
            run_action(&first_channel, &held.digest)
                .await
                .map_err(|error| error.to_string())
        });
        waiting_for = "the first action to connect to the hold listener";
        let (release_hold, _) = listener.accept().await?;

        waiting_for = "the second action to be requeued twice by the disk guard";
        let mut operations = ExecutionClient::new(channel)
            .execute(ExecuteRequest {
                instance_name: String::new(),
                skip_cache_lookup: true,
                action_digest: Some(second.digest),
                ..Default::default()
            })
            .await?
            .into_inner();
        await_refusals(&mut operations, 2).await?;
        drop(release_hold);

        waiting_for = "the second action to complete after releasing the disk reservation";
        let response = await_response(operations).await?;
        assert_eq!(
            response
                .result
                .ok_or("second action has no result after refusals")?
                .exit_code,
            0,
            "second action must succeed after repeated disk-guard refusals"
        );
        waiting_for = "the first action to complete after its hold was released";
        let response = first.await??;
        assert_eq!(
            response
                .result
                .ok_or("held first action has no result")?
                .exit_code,
            0,
            "held first action must succeed after release"
        );
        Ok(())
    })
    .await;
    flow.map_err(|_| format!("timed out waiting for {waiting_for}"))?
}

/// An action exceeding every fleet's disk declaration is rejected without waiting for a worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_action_no_fleet_accepts_is_refused_when_submitted() -> TestResult {
    let harness = Harness::start().await?;
    let channel = public_channel(harness.public_port).await?;
    let disk_kb = (FLEET_DISK / 1024 + 1).to_string();
    let action = build_action(&[("disk_kb", &disk_kb)])?;
    upload_blobs(&channel, action.blobs).await?;

    let status = ExecutionClient::new(channel)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(action.digest),
            ..Default::default()
        })
        .await
        .expect_err("an unplaceable action must be refused");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("no fleet accepts this action"));
    assert!(status.message().contains("disk_kb"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_grant_is_required_and_unknown_properties_are_refused() -> TestResult {
    let harness = Harness::start().await?;
    let channel = public_channel(harness.public_port).await?;
    for (key, value) in [("network", "requested"), ("unknown-property", "value")] {
        let action = build_action(&[(key, value)])?;
        upload_blobs(&channel, action.blobs).await?;
        let status = ExecutionClient::new(channel.clone())
            .execute(ExecuteRequest {
                skip_cache_lookup: true,
                action_digest: Some(action.digest),
                ..Default::default()
            })
            .await
            .expect_err("ungranted or unknown property must be refused");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains(key), "{}", status.message());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_request_runs_on_granted_fleet() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut policy = gang_types::FleetActionPolicy::default();
    policy.grants.insert(gang_types::Capability::Network);
    let fleet = FleetProperties::new(
        &FleetName::try_new(DEFAULT_FLEET)?,
        Isa::X86_64,
        &SANDBOX,
        &"host:test".parse()?,
        Resources {
            vcpus: WORKER_VCPUS,
            memory: Bytes::new(WORKER_MEMORY),
            disk: Bytes::new(FLEET_DISK),
        },
        &policy,
        [],
    );
    let mut harness =
        Harness::start_with(vec![fleet.clone()], TestGate::allow(), Vec::new()).await?;
    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), fleet)?;
    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("network", "requested")])?;
    upload_blobs(&channel, action.blobs).await?;
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        run_action(&channel, &action.digest),
    )
    .await??;
    assert_eq!(response.result.ok_or("missing result")?.exit_code, 0);
    Ok(())
}

#[cfg(target_os = "linux")]
fn spoofed_platform_cases() -> [PlatformCase<'static>; 4] {
    [
        (
            Some(&[
                ("cpu_count", "2"),
                ("memory_kb", "16384"),
                ("network", "requested"),
            ]),
            &[("cpu_count", "3")],
            "2",
            "requested",
        ),
        (
            None,
            &[
                ("cpu_count", "3"),
                ("memory_kb", "16384"),
                ("network", "not-requested"),
            ],
            "3",
            "<absent>",
        ),
        (
            Some(&[]),
            &[("cpu_count", "3"), ("memory_kb", "16384")],
            "3",
            "<absent>",
        ),
        (
            Some(&[
                ("cpu_count", "2"),
                ("memory_kb", "16384"),
                ("network", "requested"),
                ("gang_action_environment", "identity-roots-v1"),
            ]),
            &[],
            "2",
            "requested",
        ),
    ]
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_environment_and_platform_authority_survive_client_spoofing() -> TestResult {
    for actions in [
        gang_config::ActionsConfig::default(),
        gang_config::ActionsConfig {
            memory: gang_config::MemoryTuning {
                enforcement: gang_config::MemoryEnforcement::Soft,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        let scratch = tempfile::tempdir()?;
        let mut policy = gang_types::FleetActionPolicy::default();
        policy.grants.insert(gang_types::Capability::Network);
        let fleet = FleetProperties::new(
            &FleetName::try_new(DEFAULT_FLEET)?,
            Isa::X86_64,
            &SANDBOX,
            &"host:test".parse()?,
            Resources {
                vcpus: WORKER_VCPUS,
                memory: Bytes::new(WORKER_MEMORY),
                disk: Bytes::new(FLEET_DISK),
            },
            &policy,
            [],
        );
        let mut harness = Harness::start_configured(
            vec![fleet.clone()],
            TestGate::allow(),
            Vec::new(),
            actions,
            gang_config::WorkerConfig::default(),
        )
        .await?;
        harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), fleet)?;
        let channel = public_channel(harness.public_port).await?;
        // The 16 MiB request is raised to the configured minimum before dispatch.
        let memory = harness
            .actions
            .memory
            .min_reservation
            .as_units()
            .to_string();
        for (action_platform, command_platform, cpu, network) in spoofed_platform_cases() {
            let env = [
                ("GANG_NETWORK", "spoofed"),
                ("GANG_CPU_COUNT", "spoofed"),
                ("GANG_MEMORY_KB", "spoofed"),
                ("GANG_TEST_EXPECT_MEMORY", memory.as_str()),
                ("GANG_SIDE_CHANNEL_FILE", "spoofed"),
                ("GANG_TEST_EXPECT_NETWORK", network),
                ("GANG_TEST_EXPECT_CPU", cpu),
                ("GANG_TEST_EXPECT_SIDE_CHANNEL", "1"),
            ];
            let action = build_action_platforms(action_platform, command_platform, &env)?;
            upload_blobs(&channel, action.blobs).await?;
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                run_action(&channel, &action.digest),
            )
            .await??;
            assert_eq!(
                result.result.ok_or("missing result")?.exit_code,
                0,
                "action platform {action_platform:?}, command platform {command_platform:?}",
            );
        }
    }
    Ok(())
}

/// A queued action is a candidate of exactly the fleets that fit it, and a worker of one of those
/// fleets registers under that fleet's name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_action_is_queued_for_the_fleets_that_fit_it() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let small = fleet(SMALL_FLEET, Isa::X86_64, SMALL_VCPUS, SMALL_MEMORY)?;
    let large = fleet(LARGE_FLEET, Isa::X86_64, LARGE_VCPUS, LARGE_MEMORY)?;
    let large_name = FleetName::try_new(LARGE_FLEET)?;
    let mut harness =
        Harness::start_with(vec![small, large.clone()], TestGate::allow(), Vec::new()).await?;

    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("cpu_count", "4")])?;
    upload_blobs(&channel, action.blobs).await?;
    let mut operations = ExecutionClient::new(channel)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(action.digest),
            ..Default::default()
        })
        .await?
        .into_inner();
    // The first operation is the action's state once the scheduler has queued it, so the
    // snapshot lists the action after it.
    let first = operations
        .message()
        .await?
        .ok_or("the operation stream ended before the action was queued")?;
    assert!(!first.done);
    // Only the large fleet has the four vCPUs the action asks for.
    let snapshot = read_snapshot(&harness).await?;
    let queued = snapshot
        .queued
        .first()
        .ok_or("the snapshot lists no queued action")?;
    assert_eq!(queued.candidates, FleetSet::try_from_indices([1])?);

    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), large)?;
    // The action completes on the large fleet's worker, so that worker has registered.
    await_response(operations).await?;
    let snapshot = read_snapshot(&harness).await?;
    assert_eq!(sole_worker(&snapshot)?.fleet, large_name);
    Ok(())
}

/// The controller applies its configured minimum to a smaller requested reservation, end to end
/// from the configuration to the queued action.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reservation_below_the_minimum_is_queued_at_the_minimum() -> TestResult {
    let harness = Harness::start_configured(
        vec![default_fleet()?],
        TestGate::allow(),
        Vec::new(),
        gang_config::ActionsConfig {
            memory: gang_config::MemoryTuning {
                min_reservation: gang_config::MinReservation::from_units(16 * 1024),
                ..Default::default()
            },
            ..Default::default()
        },
        gang_config::WorkerConfig::default(),
    )
    .await?;
    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("memory_kb", "252")])?;
    upload_blobs(&channel, action.blobs).await?;
    let mut operations = ExecutionClient::new(channel)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(action.digest),
            ..Default::default()
        })
        .await?
        .into_inner();
    let first = operations
        .message()
        .await?
        .ok_or("the operation stream ended before the action was queued")?;
    assert!(!first.done);
    let snapshot = read_snapshot(&harness).await?;
    let queued = snapshot
        .queued
        .first()
        .ok_or("the snapshot lists no queued action")?;
    assert_eq!(queued.resources.memory, Bytes::mib(16));
    Ok(())
}

/// The Gang API answers on the public listener only. The worker router has no API fallback; tonic
/// answers its unmatched paths with an empty gRPC UNIMPLEMENTED response, so the API body is absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gang_api_is_served_on_the_public_listener_only() -> TestResult {
    let harness = Harness::start().await?;
    let (status, body) = http_get(harness.public_port, "/v1/server-info").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"api");
    let (status, body) = http_get(harness.worker_port, "/v1/server-info").await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.is_empty(),
        "the worker listener must not serve the Gang API"
    );
    Ok(())
}

/// The public listener negotiates HTTP/1.1 as well as HTTP/2, and the Gang API answers HTTP/1.1
/// requests there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gang_api_answers_http1_requests_on_the_public_listener() -> TestResult {
    let harness = Harness::start().await?;
    let (status, body) = http1_get(harness.public_port, "/v1/server-info").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"api");
    Ok(())
}

/// The worker listener serves HTTP/2 only, so an HTTP/1.1 request to it fails or gets no
/// successful response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_worker_listener_does_not_answer_http1_requests() -> TestResult {
    let harness = Harness::start().await?;
    let result = http1_get(harness.worker_port, "/v1/server-info").await;
    assert!(
        !matches!(result, Ok((status, _)) if status.is_success()),
        "the worker listener must not answer HTTP/1.1 requests successfully"
    );
    Ok(())
}

/// The worker listener does not serve `Execute`: a call to it fails as unimplemented, and no action
/// is queued or completed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_worker_listener_does_not_serve_execute() -> TestResult {
    let harness = Harness::start().await?;
    let status = ExecutionClient::new(worker_channel(harness.worker_port).await?)
        .execute(ExecuteRequest {
            instance_name: String::new(),
            ..Default::default()
        })
        .await
        .err()
        .ok_or("the worker listener served Execute")?;
    assert_eq!(status.code(), Code::Unimplemented);

    let snapshot = read_snapshot(&harness).await?;
    assert!(snapshot.queued.is_empty());
    assert_eq!(snapshot.completed, 0);
    Ok(())
}

/// A gate that refuses a request fails the call with the status its rejection maps to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusing_gate_fails_grpc_calls_on_the_public_listener() -> TestResult {
    let refusals = [
        (Rejection::InvalidProof, Code::Unauthenticated),
        (Rejection::PolicyDenied, Code::PermissionDenied),
        (Rejection::PrincipalRevoked, Code::PermissionDenied),
    ];
    for (rejection, code) in refusals {
        let harness = Harness::start_with(
            vec![default_fleet()?],
            TestGate::deny(rejection),
            Vec::new(),
        )
        .await?;
        let status = CapabilitiesClient::new(public_channel(harness.public_port).await?)
            .get_capabilities(GetCapabilitiesRequest {
                instance_name: String::new(),
            })
            .await
            .err()
            .ok_or("the refused call succeeded")?;
        assert_eq!(status.code(), code);
    }
    Ok(())
}

/// The gate sees the peer of the connection each request arrived on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gate_sees_the_peer_of_each_connection() -> TestResult {
    let first = Peer {
        certificate: Some(CertSerial::try_new("01")?),
        ..Peer::default()
    };
    let second = Peer {
        certificate: Some(CertSerial::try_new("02")?),
        ..Peer::default()
    };
    let gate = TestGate::allow();
    let harness = Harness::start_with(
        vec![default_fleet()?],
        gate.clone(),
        vec![first.clone(), second.clone()],
    )
    .await?;
    // Each call opens its own connection and completes before the next one starts, so the
    // connections are accepted, and their peers assigned, in this order.
    plain_capabilities(harness.public_port).await?;
    plain_capabilities(harness.public_port).await?;
    assert_eq!(gate.seen(), vec![first, second]);
    Ok(())
}

struct WorkerDeployment;

impl gang_server::Stack for WorkerDeployment {
    type Signer = gang_testkit::TestSigner;
    type Clock = gang_clock::TestClock;
    type Repository = gang_testkit::FakeRepository;
    type Identity = UnusedWorkerIdentity;
    type Clients = WorkerAnonymous;
    type Workers = WorkerAnonymous;
}

impl gang_server::Deployment for WorkerDeployment {
    type Provisioner = gang_testkit::FakeProvisioner;
    type Cache = gang_testkit::FakeCache;
    type Scheduler = Arc<NativeLink>;
    type Listener = WorkerMemoryListener;
    type Plugins = gang_testkit::TestPlugins;
}

struct UnusedWorkerIdentity;

impl gang_spi::ProofVerifierPort for UnusedWorkerIdentity {
    async fn verify(&self, _: &gang_types::Proof) -> Result<Principal, PortError> {
        Err(PortError::Unsupported {
            operation: "verify",
        })
    }
}

impl gang_spi::LoginPort for UnusedWorkerIdentity {
    fn login(&self) -> gang_spi::LoginSupport {
        gang_spi::LoginSupport::Unsupported
    }
}

struct WorkerAnonymous(Arc<Identity>);

impl gang_spi::AuthenticationPort for WorkerAnonymous {
    fn scheme(&self) -> gang_spi::Scheme {
        gang_spi::Scheme::None
    }

    fn capabilities(&self) -> gang_spi::Capabilities {
        gang_spi::Capabilities::default()
    }

    fn client_certificates(&self) -> gang_spi::ClientCertificates {
        gang_spi::ClientCertificates::Never
    }

    async fn authenticate<H: gang_spi::AuthenticationHost>(
        &self,
        _: &H,
        _: &gang_spi::Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        Ok(Some(Authenticated {
            identity: Arc::clone(&self.0),
            valid_until: None,
        }))
    }
}

impl gang_spi::CredentialLifecyclePort for WorkerAnonymous {}

fn worker_server_settings(
    port: u16,
    worker: &gang_config::WorkerConfig,
) -> Result<gang_server::ServerSettings, Box<dyn Error>> {
    let actions = Arc::new(gang_config::ActionsConfig {
        kill_grace: NonZeroDuration::from_secs(7),
        ..gang_config::ActionsConfig::default()
    });
    let attempt_budget = worker.attempt_budget(actions.timeout);
    Ok(gang_server::ServerSettings {
        actions,
        scaling: Arc::new(gang_config::ScalingConfig::default()),
        proxy_protocol: None,
        leadership: gang_config::LeadershipConfig::default(),
        attempt_budget,
        revocation_refresh_interval: NonZeroDuration::from_secs(1),
        poll_interval: NonZeroDuration::from_secs(1),
        client_action_timeout: NonZeroDuration::from_mins(1),
        default_isa: Isa::X86_64,
        endpoint: gang_server::PublicEndpoint::try_new(
            format!("http://127.0.0.1:{port}").parse()?,
        )?,
        transport: gang_server::TransportSettings::Plaintext,
        roles: Vec::new(),
        limits: gang_server::RequestLimits {
            max_request_size: 64 * 1024,
            request_timeout: NonZeroDuration::from_secs(30),
        },
        rate_limit: gang_config::RateLimitConfig::default(),
        principal_seen_interval: NonZeroDuration::from_mins(1),
        max_tracked_principals: NonZeroUsize::new(10_000).ok_or("nonzero principal limit")?,
        accept_error_backoff: NonZeroDuration::from_millis(100),
        health_max_connections: NonZeroUsize::new(64).ok_or("nonzero health limit")?,
        health_max_header_size: 8 * 1024,
        dashboard: gang_config::DashboardConfig::default(),
        builds: gang_config::BuildsConfig::default(),
        login: None,
        provisioner: "test".into(),
    })
}

struct WorkerMemoryListener {
    dials: tokio::sync::mpsc::UnboundedReceiver<tokio::io::DuplexStream>,
    accepted: u32,
}

impl gang_tls::Listener for WorkerMemoryListener {
    type Io = tokio::io::DuplexStream;
    type Addr = u32;

    async fn accept(&mut self) -> std::io::Result<(Self::Io, Self::Addr)> {
        match self.dials.recv().await {
            Some(stream) => {
                self.accepted += 1;
                Ok((stream, self.accepted))
            }
            None => std::future::pending().await,
        }
    }
}

#[derive(Clone)]
struct WorkerMemoryDialer(tokio::sync::mpsc::UnboundedSender<tokio::io::DuplexStream>);

impl WorkerMemoryDialer {
    fn dial(&self) -> tokio::io::DuplexStream {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let _ = self.0.send(server);
        client
    }
}

fn worker_memory_listener() -> (WorkerMemoryListener, WorkerMemoryDialer) {
    let (sender, dials) = tokio::sync::mpsc::unbounded_channel();
    (
        WorkerMemoryListener { dials, accepted: 0 },
        WorkerMemoryDialer(sender),
    )
}

struct WorkerServer {
    plugin: Arc<NativeLink>,
    repository: gang_testkit::FakeRepository,
    clock: Arc<gang_clock::TestClock>,
    renew_interval: Duration,
    properties: FleetProperties,
    worker_config: gang_config::WorkerConfig,
    public: WorkerMemoryDialer,
    worker: WorkerMemoryDialer,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), gang_server::ServerError>>,
}

fn worker_fleets() -> Result<gang_config::Fleets<gang_testkit::TestPlugins>, Box<dyn Error>> {
    Ok(gang_config::Fleets::try_new(vec![
        gang_config::FleetMember {
            policy: gang_config::FleetPolicy {
                name: FleetName::try_new(DEFAULT_FLEET)?,
                isa: Isa::X86_64,
                action_environment: "host:test".parse()?,
                instances: gang_config::InstanceBounds::up_to(
                    NonZeroU32::new(4).ok_or("nonzero instance limit")?,
                ),
                vcpus_per_instance: NonZeroU32::new(4).ok_or("nonzero vCPU count")?,
                memory_per_instance: gang_types::NonZeroBytes::gib(8),
                disk_per_instance: gang_types::NonZeroBytes::gib(100),
                idle_timeout: Duration::from_secs(15),
                max_drain_wait: Duration::from_mins(1),
                boot_grace: Duration::from_secs(30),
                actions: gang_types::FleetActionPolicy::default(),
            },
            sandbox: gang_testkit::SandboxKey::Noop,
            options: gang_testkit::Opaque::default(),
        },
    ])?)
}

impl WorkerServer {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let (public_listener, public) = worker_memory_listener();
        let (worker_listener, worker) = worker_memory_listener();
        let fleets = worker_fleets()?;
        let properties = fleets.catalog().fleets()[0].clone();
        let mut native_config = config(9001)?;
        native_config.worker.connect_timeout = gang_config::WholeSeconds::from_units(2);
        let worker_config = native_config.worker.clone();
        let plugin = Arc::new(build_plugin(native_config)?);
        let provisioner = gang_testkit::FakeProvisioner::default();
        let convergences = gang_testkit::CallCount::new();
        let observed = convergences.clone();
        provisioner.on_converge(move || {
            observed.bump();
            Ok(())
        });
        let repository = gang_testkit::FakeRepository::default();
        let clock = Arc::new(gang_clock::TestClock::new(
            gang_clock::Timestamp::from_unix_millis(1_767_225_600_000),
        ));
        let identity = Arc::new(Identity::new(ProviderName::ANONYMOUS, Principal::ANONYMOUS));
        let parts = gang_server::Parts::<WorkerDeployment> {
            provisioner,
            fleets,
            cache: gang_testkit::FakeCache,
            signer: gang_testkit::TestSigner::authority()?,
            scheduler: Arc::clone(&plugin),
            repository: repository.clone(),
            providers: std::collections::BTreeMap::default(),
            clock: Arc::clone(&clock),
            listeners: gang_server::Listeners {
                public: public_listener,
                worker: worker_listener,
            },
            controller: gang_spi::ControllerId::new("worker-drain-controller"),
            clients: WorkerAnonymous(Arc::clone(&identity)),
            workers: WorkerAnonymous(identity),
        };
        let settings = worker_server_settings(9000, &worker_config)?;
        let renew_interval = settings.leadership.renew_interval.get();
        let server = gang_server::Server::prepare(parts, settings).await?;
        let (stop, stopped) = oneshot::channel();
        let (ready, prepared) = ControllerReady::channel();
        let task = tokio::spawn(server.run(
            async {
                let _ = stopped.await;
            },
            ready,
        ));
        let fixture = Self {
            plugin,
            repository,
            clock,
            renew_interval,
            properties,
            worker_config,
            public,
            worker,
            stop: Some(stop),
            task,
        };
        prepared.await?;
        convergences.reached(1).await;
        Ok(fixture)
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    async fn channel(&self) -> Result<Channel, Box<dyn Error>> {
        let dialer = self.public.clone();
        Ok(Endpoint::from_static("http://127.0.0.1:9000")
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let io = dialer.dial();
                async move { Ok::<_, std::io::Error>(TokioIo::new(io)) }
            }))
            .await?)
    }
}

impl Drop for WorkerServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

use nativelink_proto::com::github::trace_machina::nativelink::remote_execution as worker_wire;

struct WorkerTask<T>(JoinHandle<T>);

impl<T> Drop for WorkerTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct WorkerConnection {
    sender: h2::client::SendRequest<bytes::Bytes>,
    driver: WorkerTask<Result<(), h2::Error>>,
    upstream: WorkerTask<()>,
    downstream: WorkerTask<()>,
}

struct WorkerStream {
    send: h2::SendStream<bytes::Bytes>,
    recv: h2::RecvStream,
    buffered: bytes::BytesMut,
}

impl WorkerConnection {
    async fn connect(dialer: &WorkerMemoryDialer) -> Result<Self, Box<dyn Error>> {
        let server = dialer.dial();
        let (client, proxy) = tokio::io::duplex(64 * 1024);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let (mut proxy_read, mut proxy_write) = tokio::io::split(proxy);
        let upstream = WorkerTask(tokio::spawn(async move {
            let _ = tokio::io::copy(&mut proxy_read, &mut server_write).await;
        }));
        let downstream = WorkerTask(tokio::spawn(async move {
            let _ = tokio::io::copy(&mut server_read, &mut proxy_write).await;
            std::future::pending::<()>().await;
            drop(proxy_write);
        }));
        let (sender, connection) = h2::client::handshake(client).await?;
        Ok(Self {
            sender,
            driver: WorkerTask(tokio::spawn(connection)),
            upstream,
            downstream,
        })
    }

    async fn request(
        &mut self,
        registration: worker_wire::ConnectWorkerRequest,
        finish_request: bool,
    ) -> Result<(http::Response<h2::RecvStream>, h2::SendStream<bytes::Bytes>), Box<dyn Error>>
    {
        self.sender.clone().ready().await?;
        let request = Request::post("http://127.0.0.1:9001/com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())?;
        let (response, mut send) = self.sender.send_request(request, false)?;
        send_worker_message(
            &mut send,
            worker_wire::update_for_scheduler::Update::ConnectWorkerRequest(registration),
        )?;
        if finish_request {
            send.send_data(bytes::Bytes::new(), true)?;
        }
        Ok((response.await?, send))
    }

    async fn finish_witness(&mut self) {
        self.upstream.0.abort();
        self.downstream.0.abort();
        let _ = (&mut self.upstream.0).await;
        let _ = (&mut self.downstream.0).await;
    }
}

fn send_worker_message(
    stream: &mut h2::SendStream<bytes::Bytes>,
    update: worker_wire::update_for_scheduler::Update,
) -> Result<(), Box<dyn Error>> {
    use bytes::BufMut as _;
    let message = worker_wire::UpdateForScheduler {
        update: Some(update),
    };
    let mut frame = bytes::BytesMut::with_capacity(5 + message.encoded_len());
    frame.put_u8(0);
    frame.put_u32(u32::try_from(message.encoded_len())?);
    message.encode(&mut frame)?;
    stream.send_data(frame.freeze(), false)?;
    Ok(())
}

impl WorkerStream {
    fn send(
        &mut self,
        update: worker_wire::update_for_scheduler::Update,
    ) -> Result<(), Box<dyn Error>> {
        send_worker_message(&mut self.send, update)
    }

    async fn next(
        &mut self,
    ) -> Result<Option<worker_wire::update_for_worker::Update>, Box<dyn Error>> {
        use bytes::Buf as _;
        loop {
            if self.buffered.len() >= 5 {
                if self.buffered[0] != 0 {
                    return Err("compressed worker response".into());
                }
                let length = u32::from_be_bytes(self.buffered[1..5].try_into()?) as usize;
                if self.buffered.len() >= 5 + length {
                    self.buffered.advance(5);
                    let frame = self.buffered.split_to(length);
                    return Ok(Some(
                        worker_wire::UpdateForWorker::decode(frame.freeze())?
                            .update
                            .ok_or("empty worker response")?,
                    ));
                }
            }
            if let Some(data) = self.recv.data().await {
                let data = data?;
                self.recv.flow_control().release_capacity(data.len())?;
                self.buffered.extend_from_slice(&data);
            } else {
                if !self.buffered.is_empty() {
                    return Err("truncated worker response".into());
                }
                let trailers = self
                    .recv
                    .trailers()
                    .await?
                    .ok_or("missing worker trailers")?;
                if trailers
                    .get("grpc-status")
                    .and_then(|value| value.to_str().ok())
                    != Some("0")
                {
                    return Err(format!("worker stream ended with {trailers:?}").into());
                }
                return Ok(None);
            }
        }
    }

    async fn update(&mut self) -> Result<worker_wire::update_for_worker::Update, Box<dyn Error>> {
        loop {
            match self
                .next()
                .await?
                .ok_or("worker stream ended unexpectedly")?
            {
                worker_wire::update_for_worker::Update::KeepAlive(()) => {}
                update => return Ok(update),
            }
        }
    }

    async fn disconnect(mut self) -> TestResult {
        assert!(matches!(
            self.update().await?,
            worker_wire::update_for_worker::Update::Disconnect(())
        ));
        self.send.send_data(bytes::Bytes::new(), true)?;
        assert!(self.next().await?.is_none());
        Ok(())
    }
}

fn worker_registration(
    properties: &FleetProperties,
    instance: &str,
    max: u64,
) -> Result<worker_wire::ConnectWorkerRequest, Box<dyn Error>> {
    // Manual workers need the release metadata normally added by NativeLink's worker config.
    Ok(worker_wire::ConnectWorkerRequest {
        properties: properties
            .worker_properties()
            .chain([(
                gang_types::property::GANG_VERSION,
                env!("CARGO_PKG_VERSION"),
            )])
            .map(|(name, value)| platform::Property {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
        worker_id_prefix: InstanceId::try_new(instance)?.worker_name(),
        max_inflight_tasks: max,
        admits_when_idle: true,
        action_ceilings: HashMap::default(),
    })
}

async fn register_worker(
    connection: &mut WorkerConnection,
    registration: worker_wire::ConnectWorkerRequest,
) -> Result<(WorkerStream, String), Box<dyn Error>> {
    let (response, send) = connection.request(registration, false).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = WorkerStream {
        send,
        recv: response.into_body(),
        buffered: bytes::BytesMut::new(),
    };
    let worker_wire::update_for_worker::Update::ConnectionResult(result) = stream.update().await?
    else {
        return Err("first worker response was not ConnectionResult".into());
    };
    assert!(result.dispatch_ack);
    Ok((stream, result.worker_id))
}

async fn submit_wire_action(
    channel: &Channel,
    action: TestAction,
) -> Result<(Digest, Streaming<Operation>), Box<dyn Error>> {
    upload_blobs(channel, action.blobs).await?;
    let digest = action.digest;
    let stream = ExecutionClient::new(channel.clone())
        .execute(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: true,
            action_digest: Some(digest.clone()),
            ..Default::default()
        })
        .await?
        .into_inner();
    Ok((digest, stream))
}

async fn wire_dispatch(
    stream: &mut WorkerStream,
    digest: &Digest,
) -> Result<worker_wire::StartExecute, Box<dyn Error>> {
    let worker_wire::update_for_worker::Update::StartAction(action) = stream.update().await? else {
        return Err("expected StartAction".into());
    };
    assert_eq!(
        action
            .execute_request
            .as_ref()
            .and_then(|request| request.action_digest.as_ref()),
        Some(digest)
    );
    Ok(action)
}

fn wire_accept(stream: &mut WorkerStream, operation: &str) -> TestResult {
    stream.send(worker_wire::update_for_scheduler::Update::ExecuteAccepted(
        worker_wire::ExecuteAccepted {
            operation_id: operation.into(),
        },
    ))
}

async fn wire_kill_barrier(stream: &mut WorkerStream, operation: &str) -> TestResult {
    wire_accept(stream, operation)?;
    let worker_wire::update_for_worker::Update::KillOperationRequest(kill) =
        stream.update().await?
    else {
        return Err("expected KillOperationRequest".into());
    };
    assert_eq!(kill.operation_id, operation);
    Ok(())
}

fn wire_complete(stream: &mut WorkerStream, action: &worker_wire::StartExecute) -> TestResult {
    use nativelink_util::action_messages::{
        ActionResult as NativeActionResult, ActionStage, ExecutionMetadata,
    };

    let response: ExecuteResponse = ActionStage::Completed(NativeActionResult {
        exit_code: 0,
        execution_metadata: ExecutionMetadata {
            worker: action.worker_id.clone(),
            ..Default::default()
        },
        ..Default::default()
    })
    .into();
    stream.send(worker_wire::update_for_scheduler::Update::ExecuteComplete(
        worker_wire::ExecuteComplete {
            operation_id: action.operation_id.clone(),
        },
    ))?;
    stream.send(worker_wire::update_for_scheduler::Update::ExecuteResult(
        worker_wire::ExecuteResult {
            instance_name: action
                .execute_request
                .as_ref()
                .ok_or("dispatch missing execute request")?
                .instance_name
                .clone(),
            operation_id: action.operation_id.clone(),
            result: Some(worker_wire::execute_result::Result::ExecuteResponse(
                response,
            )),
            resource_usage: None,
        },
    ))
}

async fn wire_snapshot(
    server: &WorkerServer,
    worker_id: &str,
    paused: bool,
    draining: bool,
    completed: u64,
) -> TestResult {
    let mut snapshot = SchedulerSnapshot::default();
    server.plugin.snapshot(&mut snapshot).await?;
    let index = snapshot
        .workers
        .iter()
        .position(|worker| worker.id.to_string() == worker_id)
        .ok_or("held worker missing from snapshot")?;
    let worker = &snapshot.workers[index];
    assert_eq!(worker.running_actions, 1);
    assert_eq!(worker.paused, paused);
    assert_eq!(worker.draining, draining);
    assert_eq!(snapshot.completed, completed);
    assert!(
        snapshot
            .executing
            .iter()
            .any(|action| action.worker == index)
    );
    assert!(!server.task.is_finished());
    Ok(())
}

#[derive(Clone, Copy)]
enum DrainCause {
    Voluntary,
    Shutdown,
}

struct HeldWorker {
    connection: WorkerConnection,
    stream: Option<WorkerStream>,
    response: Option<Streaming<Operation>>,
    id: String,
    action: worker_wire::StartExecute,
}

impl HeldWorker {
    async fn register(
        server: &WorkerServer,
        channel: &Channel,
        instance: &str,
        max: u64,
        action_tag: &str,
        waiting_for: &mut &'static str,
        dispatch_stage: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        let mut connection = WorkerConnection::connect(&server.worker).await?;
        let (mut stream, id) = register_worker(
            &mut connection,
            worker_registration(&server.properties, instance, max)?,
        )
        .await?;
        *waiting_for = dispatch_stage;
        let action = build_action_with(&[("ISA", "x86-64")], &[("WIRE_ACTION", action_tag)])?;
        let (digest, response) = submit_wire_action(channel, action).await?;
        let action = wire_dispatch(&mut stream, &digest).await?;
        wire_accept(&mut stream, &action.operation_id)?;
        Ok(Self {
            connection,
            stream: Some(stream),
            response: Some(response),
            id,
            action,
        })
    }

    fn stream(&mut self) -> Result<&mut WorkerStream, Box<dyn Error>> {
        self.stream
            .as_mut()
            .ok_or_else(|| "worker stream already completed".into())
    }

    async fn complete(&mut self) -> TestResult {
        let mut stream = self
            .stream
            .take()
            .ok_or("worker stream already completed")?;
        wire_complete(&mut stream, &self.action)?;
        let result = await_response(
            self.response
                .take()
                .ok_or("worker response already consumed")?,
        )
        .await?;
        assert_eq!(result.status.as_ref().map(|status| status.code), Some(0));
        assert_eq!(
            result.result.ok_or("held action missing result")?.exit_code,
            0
        );
        stream.disconnect().await
    }
}

struct PressureProbe {
    operation: String,
    response: Streaming<Operation>,
}

impl PressureProbe {
    async fn pause(
        server: &WorkerServer,
        channel: &Channel,
        worker: &mut HeldWorker,
    ) -> Result<Self, Box<dyn Error>> {
        let action = build_action_with(
            &[("ISA", "x86-64"), ("memory_kb", "1024")],
            &[("WIRE_ACTION", "pressure")],
        )?;
        let (digest, response) = submit_wire_action(channel, action).await?;
        let stream = worker.stream()?;
        let pressure = wire_dispatch(stream, &digest).await?;
        let memory = pressure
            .platform
            .as_ref()
            .ok_or("pressure dispatch missing platform")?
            .properties
            .iter()
            .find(|property| property.name == gang_types::property::MEMORY_KB)
            .ok_or("pressure dispatch missing memory reservation")?
            .value
            .parse()?;
        stream.send(worker_wire::update_for_scheduler::Update::ExecuteDeclined(
            worker_wire::ExecuteDeclined {
                operation_id: pressure.operation_id.clone(),
                reason: worker_wire::execute_declined::Reason::Load as i32,
                detail: String::new(),
                needed_kb: memory,
                free_kb: 0,
            },
        ))?;
        wire_kill_barrier(stream, &pressure.operation_id).await?;
        wire_snapshot(server, &worker.id, true, false, 0).await?;
        Ok(Self {
            operation: pressure.operation_id,
            response,
        })
    }

    async fn retryable_handoff(&mut self) -> TestResult {
        loop {
            match self.response.message().await {
                Ok(Some(operation)) => {
                    assert!(!operation.done, "declined action must not complete");
                }
                Ok(None) => return Err("declined action ended without retryable status".into()),
                Err(status) => {
                    assert_eq!(status.code(), Code::Unavailable);
                    return Ok(());
                }
            }
        }
    }
}

async fn trigger_worker_drain(server: &mut WorkerServer, cause: DrainCause) -> TestResult {
    use gang_clock::Clock as _;
    use gang_spi::LeaderLeasePort as _;
    match cause {
        DrainCause::Voluntary => {
            let candidate = server
                .repository
                .register_candidate(
                    &gang_spi::ControllerId::new("worker-drain-successor"),
                    &gang_spi::ControllerHostIdentity::Persistent(InstanceId::try_new(
                        "worker-drain-successor-host",
                    )?),
                )
                .await?;
            let settings = worker_server_settings(9000, &server.worker_config)?;
            let now = server.clock.wall();
            let outcome = server
                .repository
                .acquire(
                    &candidate,
                    now,
                    gang_clock::Timestamp::UNIX_EPOCH,
                    now + settings.leadership.ttl.get(),
                    gang_spi::Takeover::Request,
                )
                .await?;
            match outcome {
                gang_spi::Acquisition::Contending(lease) => assert_eq!(
                    lease.holder,
                    gang_spi::ControllerId::new("worker-drain-controller")
                ),
                other => return Err(format!("expected contending lease, got {other:?}").into()),
            }
            server
                .clock
                .advance(settings.leadership.renew_interval.get());
        }
        DrainCause::Shutdown => server.stop(),
    }
    Ok(())
}

async fn refuse_worker_reconnect(server: &WorkerServer, worker: &mut HeldWorker) -> TestResult {
    tokio::time::timeout(Duration::from_secs(1), async {
        let (response, _send) = worker
            .connection
            .request(
                worker_registration(&server.properties, "wire-worker-1", 1)?,
                true,
            )
            .await?;
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok()),
            Some("14")
        );
        assert_eq!(
            response
                .headers()
                .get(gang_types::handover::REQUEST_NOT_STARTED_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let mut body = response.into_body();
        while let Some(data) = body.data().await {
            let data = data?;
            body.flow_control().release_capacity(data.len())?;
            assert!(data.is_empty(), "refused registration sent worker messages");
        }
        let _ = body.trailers().await?;
        (&mut worker.connection.driver.0).await??;
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .map_err(|_| "timed out waiting for reconnect refusal and GOAWAY")??;
    worker.connection.finish_witness().await;
    Ok(())
}

async fn observe_worker_keepalives(
    server: &WorkerServer,
    worker: &mut HeldWorker,
    pressure: &PressureProbe,
) -> TestResult {
    for _ in 0..3 {
        server.clock.advance(Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        let operation = worker.action.operation_id.clone();
        let stream = worker.stream()?;
        stream.send(worker_wire::update_for_scheduler::Update::KeepAliveRequest(
            worker_wire::KeepAliveRequest {
                load: Some(worker_wire::WorkerLoad {
                    free_memory_kb: server
                        .properties
                        .limit(gang_types::property::MEMORY_KB)
                        .ok_or("missing worker memory")?,
                }),
                running_operations: Some(worker_wire::RunningOperations {
                    operation_ids: vec![operation],
                }),
                cache_stats: None,
            },
        ))?;
        wire_kill_barrier(stream, &pressure.operation).await?;
        wire_snapshot(server, &worker.id, false, true, 1).await?;
    }
    Ok(())
}

async fn worker_wire_drain(cause: DrainCause) -> TestResult {
    let mut waiting_for = "server promotion";
    let flow: Result<TestResult, _> = tokio::time::timeout(Duration::from_mins(2), async {
        let mut server = WorkerServer::start().await?;
        let channel = server.channel().await?;
        waiting_for = "W1 registration";
        let mut w1 = HeldWorker::register(
            &server,
            &channel,
            "wire-worker-1",
            1,
            "one",
            &mut waiting_for,
            "H1 dispatch",
        )
        .await?;
        waiting_for = "W2 registration";
        let mut w2 = HeldWorker::register(
            &server,
            &channel,
            "wire-worker-2",
            2,
            "two",
            &mut waiting_for,
            "H2 dispatch",
        )
        .await?;
        waiting_for = "pressure dispatch and pause barrier";
        let mut pressure = PressureProbe::pause(&server, &channel, &mut w2).await?;
        waiting_for = "leader drain initiation";
        trigger_worker_drain(&mut server, cause).await?;
        waiting_for = "H1 success and disconnect";
        w1.complete().await?;
        wire_snapshot(&server, &w2.id, true, true, 1).await?;
        let mut snapshot = SchedulerSnapshot::default();
        server.plugin.snapshot(&mut snapshot).await?;
        assert!(
            !snapshot
                .workers
                .iter()
                .any(|worker| worker.id.to_string() == w1.id)
        );
        waiting_for = "declined pressure action retryable handoff";
        pressure.retryable_handoff().await?;
        waiting_for = "same-H2 reconnect refusal and GOAWAY";
        refuse_worker_reconnect(&server, &mut w1).await?;
        wire_snapshot(&server, &w2.id, true, true, 1).await?;
        waiting_for = "three keepalive receipt barriers";
        observe_worker_keepalives(&server, &mut w2, &pressure).await?;
        waiting_for = "H2 success and disconnect";
        w2.complete().await?;
        if matches!(cause, DrainCause::Voluntary) {
            server.stop();
        }
        waiting_for = "server shutdown after the original engine completes";
        // Runtime drain refreshes use the injected owner clock, not Tokio's clock.
        // Keep driving that existing cadence until the real server ends its feeds.
        let finish = async {
            loop {
                server.clock.advance(server.renew_interval);
                tokio::time::advance(server.renew_interval).await;
            }
        };
        tokio::select! {
            stopped = &mut server.task => stopped??,
            () = finish => unreachable!("the owner clock driver runs until server shutdown"),
        }
        waiting_for = "W2 in-band GOAWAY after server shutdown";
        (&mut w2.connection.driver.0).await??;
        w2.connection.finish_witness().await;
        Ok(())
    })
    .await;
    match flow {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("while waiting for {waiting_for}: {error}").into()),
        Err(_) => Err(format!("timed out waiting for {waiting_for}").into()),
    }
}

#[tokio::test(start_paused = true)]
async fn voluntary_drain_keeps_admitted_worker_bidi_alive() -> TestResult {
    worker_wire_drain(DrainCause::Voluntary).await
}

#[tokio::test(start_paused = true)]
async fn shutdown_drain_keeps_admitted_worker_bidi_alive() -> TestResult {
    worker_wire_drain(DrainCause::Shutdown).await
}
