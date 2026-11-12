//! End-to-end tests of the embedded controller and worker, in one process over loopback.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use gang_nativelink::{NativeLink, NativeLinkConfig};
use gang_spi::{
    Accepted, ApiRequestBody, ApiResponseBody, ApiService, ControllerConnections, ControllerReady,
    ControllerSpec, Listening, Peer, Plugin, PluginContext, PortError, Rejection,
    RenderedStoreSpec, RequestGate, SchedulerPort, SchedulerSnapshot, Slots, StoreAccess, Subject,
    WorkerSpec, WorkerState, WorkerTls, full_body,
};
use gang_testkit::{ChildTest, TestResult};
use gang_types::{
    Bytes, CertSerial, FleetCatalog, FleetName, FleetProperties, FleetSet, InstanceId, Isa,
    Resources, WorkerId,
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
/// Actions the test worker accepts at once.
const MAX_INFLIGHT: NonZeroU32 = NonZeroU32::new(2).expect("two is not zero");
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
/// A well-formed worker id that no worker of these tests has.
const UNKNOWN_WORKER: &str = "local-9-6c5e4f3a-9b1d-4e2a-8f7c-0d3b2a1e9f58";
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
fn config(worker_port: u16) -> Result<NativeLinkConfig, serde_json::Error> {
    config_with(worker_port, json!({}))
}

/// The default controller configuration with additional top-level settings.
fn config_with(
    worker_port: u16,
    extra: serde_json::Value,
) -> Result<NativeLinkConfig, serde_json::Error> {
    let mut value = json!({
        "worker_endpoint": format!("grpc://127.0.0.1:{worker_port}"),
        "scheduler_name": "MAIN_SCHEDULER",
    });
    if let (Some(base), serde_json::Value::Object(extra)) = (value.as_object_mut(), extra) {
        base.extend(extra);
    }
    serde_json::from_value(value)
}

/// Builds the plugin from its configuration, as the host does.
fn build_plugin(config: NativeLinkConfig) -> Result<NativeLink, Box<dyn Error>> {
    Ok(NativeLink::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )?)
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
        Resources {
            vcpus,
            memory: Bytes::new(memory),
            disk: Bytes::new(disk),
        },
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
        peer: &Peer,
        _parts: &mut axum::http::request::Parts,
    ) -> Result<(), PortError> {
        self.peers.lock().push(peer.clone());
        self.denial
            .map_or(Ok(()), |rejection| Err(PortError::refused(rejection)))
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

/// Starts the controller in a task. The task builds its own specs from owned stores and serves
/// the connections accepted on its listeners. The public listener applies `public_gate` and
/// labels its connections with `public_peers`. The readiness receiver resolves when built; the
/// serving receiver resolves once it polls its public connections.
fn spawn_controller(
    plugin: Arc<NativeLink>,
    fleets: FleetCatalog,
    public: TcpListener,
    worker: TcpListener,
    public_gate: TestGate,
    public_peers: Vec<Peer>,
) -> (
    JoinHandle<Result<(), PortError>>,
    oneshot::Receiver<()>,
    oneshot::Receiver<()>,
) {
    let (serving_tx, serving_rx) = oneshot::channel();
    let (ready, readiness) = ControllerReady::channel();
    let controller = tokio::spawn(async move {
        let content = RenderedStoreSpec(json!({"memory": {}}));
        let action = RenderedStoreSpec(json!({"memory": {}}));
        let spec = ControllerSpec {
            content_store: &content,
            action_store: &action,
            fleets: &fleets,
            client_action_timeout: CLIENT_ACTION_TIMEOUT,
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
}

/// Starts a worker in a task. The task builds its own spec from owned data. The worker runs
/// `entrypoint` for every action.
fn spawn_worker(
    plugin: Arc<NativeLink>,
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
            instance,
            properties,
            slots: Slots::LongLived(MAX_INFLIGHT),
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
    controller: JoinHandle<Result<(), PortError>>,
    workers: Vec<RunningWorker>,
    public_port: u16,
    worker_port: u16,
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
        Self::start_configured(fleets, public_gate, public_peers, json!({})).await
    }

    /// Starts the controller with extra top-level configuration settings.
    async fn start_configured(
        fleets: Vec<FleetProperties>,
        public_gate: TestGate,
        public_peers: Vec<Peer>,
        extra: serde_json::Value,
    ) -> Result<Self, Box<dyn Error>> {
        let public = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let worker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let public_port = public.local_addr()?.port();
        let worker_port = worker.local_addr()?.port();
        let plugin = Arc::new(build_plugin(config_with(worker_port, extra)?)?);
        let fleets = FleetCatalog::try_new(fleets)?;
        let (mut controller, serving, readiness) = spawn_controller(
            Arc::clone(&plugin),
            fleets,
            public,
            worker,
            public_gate,
            public_peers,
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
            controller,
            workers: Vec::new(),
            public_port,
            worker_port,
        })
    }

    /// Starts a worker on this controller's worker endpoint. The worker runs this test binary
    /// as its entrypoint.
    fn add_worker(
        &mut self,
        paths: WorkerPaths,
        properties: FleetProperties,
    ) -> Result<(), Box<dyn Error>> {
        std::fs::create_dir_all(&paths.work)?;
        std::fs::create_dir_all(&paths.fast)?;
        let instance = InstanceId::try_new(WORKER_INSTANCE)?;
        let entrypoint = std::env::current_exe()?;
        let (stop, receiver) = oneshot::channel();
        let handle = spawn_worker(
            Arc::clone(&self.plugin),
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
    assert_eq!(worker.max_inflight_actions, Some(MAX_INFLIGHT));
    assert!(!worker.draining);
    Ok(())
}

/// Draining a worker shows in the snapshot, and resuming it clears the flag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_a_worker_is_visible_in_the_snapshot() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut harness = Harness::start().await?;
    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;
    await_registration(&harness).await?;
    let worker = sole_worker(&read_snapshot(&harness).await?)?.id.clone();

    harness.plugin.set_draining(&worker, true).await?;
    assert!(sole_worker(&read_snapshot(&harness).await?)?.draining);

    harness.plugin.set_draining(&worker, false).await?;
    assert!(!sole_worker(&read_snapshot(&harness).await?)?.draining);
    Ok(())
}

/// Draining a worker that is not connected is `NotFound`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_an_unknown_worker_is_not_found() -> TestResult {
    let harness = Harness::start().await?;
    let unknown = WorkerId::try_new(UNKNOWN_WORKER)?;
    let result = harness.plugin.set_draining(&unknown, true).await;
    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Worker(_)))
    ));
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
    let spec = ControllerSpec {
        content_store: &content,
        action_store: &action,
        fleets: &fleets,
        client_action_timeout: CLIENT_ACTION_TIMEOUT,
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

/// A cold start that fits no fleet is rejected before the controller is ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cold_start_no_fleet_accepts_is_refused_before_ready() -> TestResult {
    let config = serde_json::from_value(json!({
        "worker_endpoint": "grpc://127.0.0.1:0",
        "scheduler_name": "MAIN_SCHEDULER",
        "cold_start": { "memory": "64GiB" },
    }))?;
    let plugin = build_plugin(config)?;
    let content = RenderedStoreSpec(json!({"memory": {}}));
    let action = RenderedStoreSpec(json!({"memory": {}}));
    let fleets = FleetCatalog::try_new(vec![default_fleet()?])?;
    let spec = ControllerSpec {
        content_store: &content,
        action_store: &action,
        fleets: &fleets,
        client_action_timeout: CLIENT_ACTION_TIMEOUT,
    };
    let (ready, readiness) = ControllerReady::channel();
    let result = plugin.run_controller(&spec, no_connections(), ready).await;
    assert!(matches!(
        result,
        Err(PortError::Config(error)) if error.to_string().contains("which no fleet accepts")
    ));
    assert!(readiness.await.is_err());
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
    let platform = Platform {
        properties: platform_properties
            .iter()
            .map(|(name, value)| platform::Property {
                name: (*name).into(),
                value: (*value).into(),
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
        platform: Some(platform.clone()),
        ..Default::default()
    };
    let command_bytes = command.encode_to_vec();
    let command_digest = digest_of(&command_bytes)?;
    let action = Action {
        command_digest: Some(command_digest.clone()),
        input_root_digest: Some(root_digest.clone()),
        platform: Some(platform),
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
/// through the entrypoint, and the upload of the result to the action cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_action_runs_and_its_result_is_cached() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let mut harness = Harness::start().await?;
    harness.add_worker(WorkerPaths::new(scratch.path(), "worker"), default_fleet()?)?;

    let channel = public_channel(harness.public_port).await?;
    let action = build_action(&[("ISA", "x86-64")])?;
    upload_blobs(&channel, action.blobs).await?;

    let response = run_action(&channel, &action.digest).await?;
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
    let (mut first, _) = listener.accept().await?;
    let (mut second, _) = listener.accept().await?;
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
    for entry in std::fs::read_dir(&paths.work)? {
        assert!(
            !entry?.file_type()?.is_dir(),
            "action directory remains after shutdown"
        );
    }
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
    let (_first, _) = listener.accept().await?;
    let (_second, _) = listener.accept().await?;
    let worker = harness.workers.pop().ok_or("missing worker")?;
    worker
        .stop
        .send(())
        .map_err(|()| "worker stopped before shutdown")?;
    let (mut result, _) = listener.accept().await?;
    let mut byte = [0];
    result.read_exact(&mut byte).await?;
    assert_eq!(&byte, b"k", "helper did not report a signalled child");
    worker.task.await??;
    Ok(())
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
            json!({
                "disk": {"enforcement": "guard"},
                "scheduler": {"max_job_retries": 1},
                "worker": {"keepalive_interval": "1s", "connect_timeout": "2s"},
            }),
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
    assert!(
        status
            .message()
            .contains("no fleet accepts the platform properties of this action")
    );
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
