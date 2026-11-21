//! Tests for action stream hand-off.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};
use nativelink_util::origin_event::OriginMetadata;

use crate::handoff::{HandOff, HandOffActions};

struct Fake(ActionStage, ActionStage);

fn state(stage: ActionStage) -> Arc<ActionState> {
    Arc::new(ActionState {
        stage,
        last_transition_timestamp: UNIX_EPOCH,
        client_operation_id: OperationId::String("operation".to_owned()),
        action_digest: DigestInfo::new([1; 32], 1),
    })
}

struct Row(ActionStage, ActionStage);

#[async_trait]
impl ActionStateResult for Row {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        Ok((state(self.0.clone()), None))
    }
    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        if matches!(self.0, ActionStage::Executing) {
            self.0 = self.1.clone();
            return Ok((state(self.0.clone()), None));
        }
        std::future::pending().await
    }
    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        Err(make_err!(Code::Internal, "not used"))
    }
}

impl MetricsComponent for Fake {
    fn publish(
        &self,
        _: MetricKind,
        _: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}
impl RootMetricsComponent for Fake {}

#[async_trait]
impl ClientStateManager for Fake {
    async fn add_action(
        &self,
        _: OperationId,
        _: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        Ok(Box::new(Row(self.0.clone(), self.1.clone())))
    }
    async fn filter_operations(
        &self,
        _: OperationFilter,
    ) -> Result<ActionStateResultStream<'_>, Error> {
        Ok(Box::pin(futures::stream::once(async move {
            Box::new(Row(self.0.clone(), self.1.clone())) as Box<dyn ActionStateResult>
        })))
    }
}
#[async_trait]
impl KnownPlatformPropertyProvider for Fake {
    async fn get_known_properties(&self, _: &str) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }
}

fn info() -> Arc<ActionInfo> {
    let digest = DigestInfo::new([1; 32], 1);
    Arc::new(ActionInfo {
        command_digest: digest,
        input_root_digest: digest,
        timeout: Duration::from_secs(1),
        platform_properties: std::collections::HashMap::default(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            execution_scope: None,
            instance_name: String::new(),
            digest_function: DigestHasherFunc::Sha256,
            digest,
        }),
    })
}

#[tokio::test]
async fn queued_execute_and_wait_streams_are_rejected() -> gang_testkit::TestResult {
    use futures::StreamExt;
    let handoff = HandOff::new();
    let actions = HandOffActions::new(
        Arc::new(Fake(ActionStage::Queued, ActionStage::Queued)),
        handoff.clone(),
    );
    let mut execute = actions
        .add_action(OperationId::String("op".into()), info())
        .await
        .expect("add");
    let mut wait = actions
        .filter_operations(OperationFilter::default())
        .await
        .expect("filter")
        .next()
        .await
        .expect("row");
    let mut changed = Box::pin(execute.changed());
    assert!(futures::poll!(changed.as_mut()).is_pending());
    handoff.begin();
    assert_eq!(
        changed.await.expect_err("queued execute rejected").code,
        Code::Unavailable
    );
    assert_eq!(
        wait.changed().await.expect_err("queued wait rejected").code,
        Code::Unavailable
    );
    assert!(
        matches!(actions.add_action(OperationId::String("op".into()), info()).await, Err(error) if error.code == Code::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn executing_stream_continues_after_handoff() -> gang_testkit::TestResult {
    let handoff = HandOff::new();
    let actions = HandOffActions::new(
        Arc::new(Fake(
            ActionStage::Executing,
            ActionStage::Completed(ActionResult::default()),
        )),
        handoff.clone(),
    );
    let mut result = actions
        .add_action(OperationId::String("op".into()), info())
        .await
        .expect("add");
    handoff.begin();
    let (state, _) = result.changed().await.expect("completion forwarded");
    assert!(matches!(state.stage, ActionStage::Completed(_)));
    Ok(())
}

#[tokio::test]
async fn requeued_executing_action_is_unavailable_after_handoff() -> gang_testkit::TestResult {
    let handoff = HandOff::new();
    let actions = HandOffActions::new(
        Arc::new(Fake(ActionStage::Executing, ActionStage::Queued)),
        handoff.clone(),
    );
    let mut result = actions
        .add_action(OperationId::String("op".into()), info())
        .await?;
    handoff.begin();
    let error = result
        .changed()
        .await
        .expect_err("requeued action must be unavailable");
    assert_eq!(error.code, Code::Unavailable);
    Ok(())
}

#[derive(Clone)]
struct Allow;

impl gang_spi::RequestGate for Allow {
    fn admit(
        &self,
        _: gang_spi::RequestScope,
        _: &gang_spi::Peer,
        _: &mut http::request::Parts,
    ) -> impl Future<Output = Result<gang_spi::GateDecision, gang_spi::PortError>> + Send {
        std::future::ready(Ok(gang_spi::GateDecision::Admit(
            gang_spi::Admitted::Authenticated(gang_spi::Authenticated {
                identity: Arc::new(gang_types::Identity::new(
                    gang_types::ProviderName::try_new("test").expect("valid provider"),
                    gang_types::Principal::try_new("tester").expect("valid principal"),
                )),
                valid_until: None,
            }),
        )))
    }
}

#[derive(Clone)]
struct Api;

impl gang_spi::ApiService for Api {
    fn call(
        &self,
        _: http::Request<gang_spi::ApiRequestBody>,
    ) -> impl Future<Output = http::Response<gang_spi::ApiResponseBody>> + Send {
        std::future::ready(http::Response::new(gang_spi::full_body("api")))
    }
}

fn listener() -> (
    tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
    gang_spi::Listening<Allow>,
) {
    use futures::StreamExt;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let connections = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|accepted| (accepted, receiver))
    })
    .boxed();
    (
        sender,
        gang_spi::Listening {
            connections,
            gate: Allow,
        },
    )
}

async fn client(
    feed: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
) -> gang_testkit::TestResult<hyper::client::conn::http2::SendRequest<axum::body::Body>> {
    let (server, client) = tokio::io::duplex(64 * 1024);
    feed.send(gang_spi::Accepted {
        connection: Box::new(server),
        peer: gang_spi::Peer::default(),
    })
    .map_err(|_| "listener closed")?;
    let (sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(client),
    )
    .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}

async fn grpc_route(
    client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
    path: &str,
) -> gang_testkit::TestResult {
    client.ready().await?;
    let request = http::Request::post(format!("http://localhost/{path}"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("te", "trailers")
        .body(axum::body::Body::empty())?;
    let response = client.send_request(request).await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.headers()[http::header::CONTENT_TYPE],
        "application/grpc"
    );
    Ok(())
}

#[tokio::test]
async fn public_listener_serves_execution_and_waits_for_worker_stream() -> gang_testkit::TestResult
{
    use gang_spi::{ControllerConnections, ControllerReady, ControllerSpec, RenderedStoreSpec};
    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, Isa, Resources};

    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let (ready, waiting) = ControllerReady::channel();
    let task = tokio::spawn(async move {
        let config = crate::config::NativeLinkConfig::default();
        let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("test").expect("valid fleet name"),
            Isa::X86_64,
            &"docker",
            Some(&"host:test".parse().expect("valid environment")),
            Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(16 << 30),
            },
            &gang_types::FleetActionPolicy::default(),
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid fleet");
        let store = RenderedStoreSpec(serde_json::json!({"memory": {}}));
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: std::sync::Arc::new(gang_config::ActionsConfig::default()),
            scaling: std::sync::Arc::new(gang_config::ScalingConfig::default()),
            clock: std::sync::Arc::new(gang_clock::SystemClock::new()),
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            max_workers: 8,
            client_action_timeout: Duration::from_mins(10),
            observations: &observations,
        };
        let (handles, _) = tokio::sync::watch::channel(None);
        let histories = parking_lot::Mutex::new(None);
        crate::controller::run(
            &config,
            &spec,
            ControllerConnections {
                public,
                worker,
                api: Api,
            },
            ready,
            crate::controller::ControllerState {
                handles: &handles,
                histories: &histories,
                resources: &std::sync::OnceLock::new(),
                shared: &crate::controller::SharedServices::default(),
            },
        )
        .await
    });
    waiting.await?;
    let mut public_client = client(&public_feed).await?;
    for path in [
        "build.bazel.remote.execution.v2.Execution/Execute",
        "google.longrunning.Operations/GetOperation",
        "build.bazel.remote.execution.v2.ActionCache/GetActionResult",
        "build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
        "google.bytestream.ByteStream/Read",
        "build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
    ] {
        grpc_route(&mut public_client, path).await?;
    }
    public_client.ready().await?;
    let api_response = public_client
        .send_request(
            http::Request::get("http://localhost/v1/test").body(axum::body::Body::empty())?,
        )
        .await?;
    assert_eq!(api_response.status(), http::StatusCode::OK);
    assert!(
        api_response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .is_none()
    );
    drop(api_response);
    drop(worker_feed);
    // Ending the worker stream must not retire the public listener.
    grpc_route(
        &mut public_client,
        "build.bazel.remote.execution.v2.Execution/Execute",
    )
    .await?;
    drop(public_feed);
    drop(public_client);
    task.await??;
    Ok(())
}

#[derive(Clone)]
struct HeldApi {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
}

impl gang_spi::ApiService for HeldApi {
    async fn call(
        &self,
        _: http::Request<gang_spi::ApiRequestBody>,
    ) -> http::Response<gang_spi::ApiResponseBody> {
        let released = self.release.notified();
        let _ = self.entered.send(());
        released.await;
        http::Response::new(gang_spi::full_body("completed"))
    }
}

/// A supplied gate and handler that the refusal transport must never invoke.
#[derive(Clone, Default)]
struct Untouched(Arc<std::sync::atomic::AtomicUsize>);

impl gang_spi::RequestGate for Untouched {
    async fn admit(
        &self,
        _: gang_spi::RequestScope,
        _: &gang_spi::Peer,
        _: &mut http::request::Parts,
    ) -> Result<gang_spi::GateDecision, gang_spi::PortError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(gang_spi::PortError::backend("unexpected authentication"))
    }
}

impl gang_spi::ApiService for Untouched {
    async fn call(
        &self,
        _: http::Request<gang_spi::ApiRequestBody>,
    ) -> http::Response<gang_spi::ApiResponseBody> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        http::Response::new(gang_spi::full_body("unexpected handler"))
    }
}

fn refusal_inputs(
    untouched: &Untouched,
) -> (
    [tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>; 2],
    gang_spi::ControllerConnections<Untouched, Untouched, Untouched>,
) {
    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    (
        [public_feed, worker_feed],
        gang_spi::ControllerConnections {
            public: gang_spi::Listening {
                connections: public.connections,
                gate: untouched.clone(),
            },
            worker: gang_spi::Listening {
                connections: worker.connections,
                gate: untouched.clone(),
            },
            api: untouched.clone(),
        },
    )
}

fn test_plugin(server: serde_json::Value) -> gang_testkit::TestResult<crate::NativeLink> {
    let server = serde_json::from_value(server)?;
    Ok(crate::NativeLink::try_new(
        crate::NativeLinkConfig::for_controller("grpc://127.0.0.1:9100".parse()?, &server),
    )?)
}

async fn run_held_controller(
    plugin: Arc<crate::NativeLink>,
    connections: gang_spi::ControllerConnections<Allow, Allow, HeldApi>,
    ready: gang_spi::ControllerReady,
) -> Result<(), gang_spi::PortError> {
    use gang_spi::SchedulerPort;
    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, Isa, Resources};
    let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
        &FleetName::try_new("test").expect("valid fleet name"),
        Isa::X86_64,
        &"docker",
        Some(&"host:test".parse().expect("valid environment")),
        Resources {
            vcpus: 4,
            memory: Bytes::new(8 << 30),
            disk: Bytes::new(16 << 30),
        },
        &gang_types::FleetActionPolicy::default(),
        Vec::<(&'static str, String)>::new(),
    )])
    .expect("valid test fleet");
    let store = gang_spi::RenderedStoreSpec(serde_json::json!({"memory": {}}));
    let (observations, _receiver) = gang_spi::ObservationSink::channel(
        std::num::NonZeroUsize::MIN,
        std::num::NonZeroUsize::MIN,
    );
    let spec = gang_spi::ControllerSpec {
        actions: Arc::new(gang_config::ActionsConfig::default()),
        scaling: Arc::new(gang_config::ScalingConfig::default()),
        clock: Arc::new(gang_clock::SystemClock::new()),
        content_store: &store,
        action_store: &store,
        fleets: &fleets,
        max_workers: 8,
        client_action_timeout: Duration::from_mins(10),
        observations: &observations,
    };
    plugin.run_controller(&spec, connections, ready).await
}

fn observe_intake_end(
    mut listening: gang_spi::Listening<Allow>,
) -> (
    gang_spi::Listening<Allow>,
    tokio::sync::oneshot::Receiver<()>,
) {
    use futures::StreamExt;
    let (ended, waiting) = tokio::sync::oneshot::channel();
    listening.connections = listening
        .connections
        .chain(
            futures::stream::once(async move {
                let _ = ended.send(());
                None
            })
            .filter_map(std::future::ready),
        )
        .boxed();
    (listening, waiting)
}

async fn assert_wire_refusal(
    feed: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
    path: &str,
) -> gang_testkit::TestResult {
    use axum::body::Body;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let (server, client) = tokio::io::duplex(64 * 1024);
    feed.send(gang_spi::Accepted {
        connection: Box::new(server),
        peer: gang_spi::Peer::default(),
    })
    .map_err(|_| "refusal listener closed")?;
    let request = http::Request::post(format!("http://localhost/{path}"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(Body::empty())?;
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client)).await?;
    tokio::spawn(connection);
    let response = sender.send_request(request).await?;
    assert_eq!(
        response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
        "true",
    );
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.headers()["grpc-status"], "14");
    let body = axum::body::to_bytes(Body::new(response.into_body()), 1024).await?;
    assert!(body.is_empty());
    Ok(())
}

async fn assert_leader_refusals(
    public: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
    worker: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
) -> gang_testkit::TestResult {
    assert_wire_refusal(public, "build.bazel.remote.execution.v2.Execution/Execute").await?;
    assert_wire_refusal(public, "google.longrunning.Operations/GetOperation").await?;
    assert_wire_refusal(
        worker,
        "com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
    )
    .await?;
    Ok(())
}

async fn assert_shared_api_and_capabilities(
    public_feed: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
) -> gang_testkit::TestResult {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        GetCapabilitiesRequest, ServerCapabilities,
    };
    use prost::Message;

    let mut shared_client = client(public_feed).await?;
    let response = shared_client
        .send_request(
            http::Request::get("http://localhost/v1/info").body(axum::body::Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(axum::body::Body::new(response.into_body()), 1024).await?;
    assert_eq!(body.as_ref(), b"api");
    let body = grpc_message(
        &mut shared_client,
        "build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
        GetCapabilitiesRequest::default(),
    )
    .await?;
    let capabilities = ServerCapabilities::decode(&body[5..])?;
    assert!(capabilities.cache_capabilities.is_some());
    Ok(())
}

#[tokio::test]
async fn fresh_wire_refusals_coexist_with_a_held_controller_drain() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, ControllerReady, SchedulerPort, SchedulerSnapshot};
    tokio::time::timeout(Duration::from_secs(10), async {
        let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
        let (public_feed, public) = listener();
        let (worker_feed, worker) = listener();
        let (public, public_ended) = observe_intake_end(public);
        let (worker, worker_ended) = observe_intake_end(worker);
        let (entered, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let (ready, waiting) = ControllerReady::channel();
        let controller = tokio::spawn(run_held_controller(
            Arc::clone(&plugin),
            ControllerConnections {
                public,
                worker,
                api: HeldApi {
                    entered,
                    release: Arc::clone(&release),
                },
            },
            ready,
        ));
        waiting.await?;
        let mut old_client = client(&public_feed).await?;
        let admitted = tokio::spawn(async move {
            old_client
                .send_request(
                    http::Request::post("http://localhost/held")
                        .body(axum::body::Body::empty())
                        .expect("valid request"),
                )
                .await
        });
        entered_rx
            .recv()
            .await
            .ok_or("old API request did not enter")?;
        drop((public_feed, worker_feed));
        public_ended.await?;
        worker_ended.await?;
        assert!(
            !controller.is_finished(),
            "the admitted request holds the old drain"
        );
        let (public_feed, public) = listener();
        let (worker_feed, worker) = listener();
        let connections = ControllerConnections {
            public,
            worker,
            api: Api,
        };
        let refusing = {
            let plugin = Arc::clone(&plugin);
            tokio::spawn(async move { plugin.serve_standby(connections).await })
        };
        assert_wire_refusal(
            &public_feed,
            "build.bazel.remote.execution.v2.Execution/Execute",
        )
        .await?;
        assert_wire_refusal(&public_feed, "google.longrunning.Operations/GetOperation").await?;
        assert_wire_refusal(
            &worker_feed,
            "com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
        )
        .await?;
        assert_shared_api_and_capabilities(&public_feed).await?;
        assert!(
            !controller.is_finished(),
            "refusals must not cancel the admitted request"
        );
        plugin.snapshot(&mut SchedulerSnapshot::default()).await?;
        drop((public_feed, worker_feed));
        refusing.await??;
        assert!(
            !controller.is_finished(),
            "the refusal transport does not own the controller"
        );
        release.notify_one();
        let response = admitted.await??;
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = axum::body::to_bytes(axum::body::Body::new(response.into_body()), 1024).await?;
        assert_eq!(body.as_ref(), b"completed");
        controller.await??;
        assert!(matches!(
            plugin.snapshot(&mut SchedulerSnapshot::default()).await,
            Err(gang_spi::PortError::Unavailable(_)),
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await?
}

#[tokio::test]
async fn refusal_serving_requires_controller_settings() -> gang_testkit::TestResult {
    use gang_spi::SchedulerPort;
    let config = crate::NativeLinkConfig::for_worker(
        Some("grpc://127.0.0.1:9100".parse()?),
        &gang_config::WorkerConfig::default(),
    );
    let plugin = crate::NativeLink::try_new(config)?;
    let untouched = Untouched::default();
    let (_feeds, connections) = refusal_inputs(&untouched);
    assert!(matches!(
        plugin.serve_standby(connections).await,
        Err(gang_spi::PortError::Config(_))
    ));
    assert_eq!(untouched.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn standby_before_initial_preparation_is_unavailable() -> gang_testkit::TestResult {
    use gang_spi::SchedulerPort;
    let plugin = test_plugin(serde_json::json!({"transport": "plaintext"}))?;
    let untouched = Untouched::default();
    let (_feeds, connections) = refusal_inputs(&untouched);
    assert!(matches!(
        plugin.serve_standby(connections).await,
        Err(gang_spi::PortError::Unavailable(_))
    ));
    assert_eq!(untouched.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    Ok(())
}

async fn prepare(plugin: Arc<crate::NativeLink>) -> gang_testkit::TestResult {
    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let (entered, _) = tokio::sync::mpsc::unbounded_channel();
    let (ready, waiting) = gang_spi::ControllerReady::channel();
    let task = tokio::spawn(run_held_controller(
        plugin,
        gang_spi::ControllerConnections {
            public,
            worker,
            api: HeldApi {
                entered,
                release: Arc::new(tokio::sync::Notify::new()),
            },
        },
        ready,
    ));
    waiting.await?;
    drop((public_feed, worker_feed));
    task.await??;
    Ok(())
}

async fn assert_cache_roundtrip(
    client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
) -> gang_testkit::TestResult {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        ActionResult, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
        BatchUpdateBlobsResponse, Digest, GetActionResultRequest, UpdateActionResultRequest,
        batch_update_blobs_request,
    };
    use prost::Message;

    let digest = Digest {
        hash: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_owned(),
        size_bytes: 5,
    };
    let update = BatchUpdateBlobsRequest {
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(digest.clone()),
            data: "hello".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let body = grpc_message(
        client,
        "build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
        update,
    )
    .await?;
    let response = BatchUpdateBlobsResponse::decode(&body[5..])?;
    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0]
            .status
            .as_ref()
            .map(|status| status.code),
        Some(0)
    );
    let body = grpc_message(
        client,
        "build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
        BatchReadBlobsRequest {
            digests: vec![digest.clone()],
            ..Default::default()
        },
    )
    .await?;
    let response = BatchReadBlobsResponse::decode(&body[5..])?;
    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0]
            .status
            .as_ref()
            .map(|status| status.code),
        Some(0)
    );
    assert_eq!(response.responses[0].data.as_ref(), b"hello");

    let result = ActionResult {
        exit_code: 42,
        ..Default::default()
    };
    let body = grpc_message(
        client,
        "build.bazel.remote.execution.v2.ActionCache/UpdateActionResult",
        UpdateActionResultRequest {
            action_digest: Some(digest.clone()),
            action_result: Some(result.clone()),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(ActionResult::decode(&body[5..])?, result);
    let body = grpc_message(
        client,
        "build.bazel.remote.execution.v2.ActionCache/GetActionResult",
        GetActionResultRequest {
            action_digest: Some(digest),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(ActionResult::decode(&body[5..])?, result);
    Ok(())
}

#[tokio::test]
async fn standby_serves_real_public_action_and_content_cache() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, SchedulerPort};

    tokio::time::timeout(Duration::from_secs(10), async {
        let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
        prepare(Arc::clone(&plugin)).await?;
        let (public_feed, public) = listener();
        let (worker_feed, worker) = listener();
        let serving = {
            let plugin = Arc::clone(&plugin);
            tokio::spawn(async move {
                plugin
                    .serve_standby(ControllerConnections {
                        public,
                        worker,
                        api: Api,
                    })
                    .await
            })
        };
        let mut client = client(&public_feed).await?;
        assert_cache_roundtrip(&mut client).await?;
        drop((client, public_feed, worker_feed));
        serving.await??;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await?
}

#[tokio::test]
async fn standby_worker_connections_use_worker_capacity() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, SchedulerPort};
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Digest, FindMissingBlobsRequest, FindMissingBlobsResponse,
    };
    use prost::Message;
    let plugin = Arc::new(test_plugin(serde_json::json!({
        "transport": "plaintext",
        "http2": { "max_connections": 1 }
    }))?);
    prepare(Arc::clone(&plugin)).await?;
    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let serving = {
        let plugin = Arc::clone(&plugin);
        tokio::spawn(async move {
            plugin
                .serve_standby(ControllerConnections {
                    public,
                    worker,
                    api: Api,
                })
                .await
        })
    };
    let missing = Digest {
        hash: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_owned(),
        size_bytes: 5,
    };
    let request = || FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![missing.clone()],
        ..Default::default()
    };
    let mut first = client(&worker_feed).await?;
    let body = grpc_message(
        &mut first,
        "build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
        request(),
    )
    .await?;
    let response = FindMissingBlobsResponse::decode(&body[5..])?;
    assert_eq!(response.missing_blob_digests, vec![missing.clone()]);
    let mut second = tokio::time::timeout(Duration::from_secs(10), client(&worker_feed)).await??;
    let body = tokio::time::timeout(
        Duration::from_secs(10),
        grpc_message(
            &mut second,
            "build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
            request(),
        ),
    )
    .await??;
    let response = FindMissingBlobsResponse::decode(&body[5..])?;
    assert_eq!(response.missing_blob_digests, vec![missing]);
    drop((first, second, public_feed, worker_feed));
    serving.await??;
    Ok(())
}

async fn grpc_message<M: prost::Message>(
    client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
    path: &str,
    message: M,
) -> gang_testkit::TestResult<axum::body::Bytes> {
    let (headers, body) = grpc_exchange(client, path, message).await?;
    assert!(!headers.contains_key(gang_types::handover::REQUEST_NOT_STARTED_HEADER));
    assert!(!body.trailers().is_some_and(|trailers| {
        trailers.contains_key(gang_types::handover::REQUEST_NOT_STARTED_HEADER)
    }));
    let status = headers.get("grpc-status").or_else(|| {
        body.trailers()
            .and_then(|trailers| trailers.get("grpc-status"))
    });
    assert_eq!(status.and_then(|value| value.to_str().ok()), Some("0"));
    Ok(body.to_bytes())
}

async fn grpc_exchange<M: prost::Message>(
    client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
    path: &str,
    message: M,
) -> gang_testkit::TestResult<(
    http::HeaderMap,
    http_body_util::Collected<axum::body::Bytes>,
)> {
    use http_body_util::BodyExt;
    let payload = message.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
    frame.extend_from_slice(&payload);
    let response = client
        .send_request(
            http::Request::post(format!("http://localhost/{path}"))
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .body(axum::body::Body::from(frame))?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    let (parts, body) = response.into_parts();
    Ok((parts.headers, body.collect().await?))
}

async fn resume_upload(
    standby_client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
    resource: &str,
) -> gang_testkit::TestResult {
    use nativelink_proto::google::bytestream::{
        QueryWriteStatusRequest, QueryWriteStatusResponse, WriteRequest,
    };
    use prost::Message;

    let query = || QueryWriteStatusRequest {
        resource_name: resource.to_owned(),
    };
    let body = grpc_message(
        standby_client,
        "google.bytestream.ByteStream/QueryWriteStatus",
        query(),
    )
    .await?;
    let status = QueryWriteStatusResponse::decode(&body[5..])?;
    assert_eq!(status.committed_size, 2);
    assert!(!status.complete);
    let body = grpc_message(
        standby_client,
        "google.bytestream.ByteStream/Write",
        WriteRequest {
            resource_name: resource.to_owned(),
            write_offset: 2,
            finish_write: true,
            data: "llo".into(),
        },
    )
    .await?;
    let result = nativelink_proto::google::bytestream::WriteResponse::decode(&body[5..])?;
    assert_eq!(result.committed_size, 5);
    let body = grpc_message(
        standby_client,
        "google.bytestream.ByteStream/Read",
        nativelink_proto::google::bytestream::ReadRequest {
            resource_name:
                "blobs/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5"
                    .to_owned(),
            read_offset: 0,
            read_limit: 0,
        },
    )
    .await?;
    let read = nativelink_proto::google::bytestream::ReadResponse::decode(&body[5..])?;
    assert_eq!(read.data.as_ref(), b"hello");
    Ok(())
}

#[tokio::test]
async fn upload_resumes_across_standby_and_later_controller_term() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, ControllerReady, SchedulerPort};
    use nativelink_proto::google::bytestream::{
        QueryWriteStatusRequest, QueryWriteStatusResponse, WriteRequest,
    };
    use prost::Message;

    let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let (entered, _) = tokio::sync::mpsc::unbounded_channel();
    let (ready, waiting) = ControllerReady::channel();
    let first = tokio::spawn(run_held_controller(
        Arc::clone(&plugin),
        ControllerConnections {
            public,
            worker,
            api: HeldApi {
                entered,
                release: Arc::new(tokio::sync::Notify::new()),
            },
        },
        ready,
    ));
    waiting.await?;
    let resource = "uploads/0123456789abcdef0123456789abcdef/blobs/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5";
    let mut first_client = client(&public_feed).await?;
    let _ = grpc_exchange(
        &mut first_client,
        "google.bytestream.ByteStream/Write",
        WriteRequest {
            resource_name: resource.to_owned(),
            write_offset: 0,
            finish_write: false,
            data: "he".into(),
        },
    )
    .await?;
    drop((first_client, public_feed, worker_feed));
    first.await??;

    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let standby = {
        let plugin = Arc::clone(&plugin);
        tokio::spawn(async move {
            plugin
                .serve_standby(ControllerConnections {
                    public,
                    worker,
                    api: Api,
                })
                .await
        })
    };
    let mut standby_client = client(&public_feed).await?;
    resume_upload(&mut standby_client, resource).await?;
    drop(standby_client);
    let standby_public_feed = public_feed;
    let standby_worker_feed = worker_feed;

    let (public_feed, public) = listener();
    let (worker_feed, worker) = listener();
    let (entered, _) = tokio::sync::mpsc::unbounded_channel();
    let (ready, waiting) = ControllerReady::channel();
    let second = tokio::spawn(run_held_controller(
        Arc::clone(&plugin),
        ControllerConnections {
            public,
            worker,
            api: HeldApi {
                entered,
                release: Arc::new(tokio::sync::Notify::new()),
            },
        },
        ready,
    ));
    waiting.await?;
    assert_leader_refusals(&standby_public_feed, &standby_worker_feed).await?;
    drop((standby_public_feed, standby_worker_feed));
    standby.await??;
    let mut second_client = client(&public_feed).await?;
    let body = grpc_message(
        &mut second_client,
        "google.bytestream.ByteStream/QueryWriteStatus",
        QueryWriteStatusRequest {
            resource_name: resource.to_owned(),
        },
    )
    .await?;
    let status = QueryWriteStatusResponse::decode(&body[5..])?;
    assert_eq!(status.committed_size, 5);
    assert!(status.complete);
    drop((second_client, public_feed, worker_feed));
    second.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn both_refusal_flushes_share_the_original_bounded_deadline() -> gang_testkit::TestResult {
    use gang_spi::{SchedulerPort, SchedulerSnapshot};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let plugin = Arc::new(test_plugin(serde_json::json!({
        "transport": "plaintext",
        "request_timeout": "5s",
        "http2": { "shutdown_grace": "30s", "unproven_idle_timeout": "60s" }
    }))?);
    prepare(Arc::clone(&plugin)).await?;
    let untouched = Untouched::default();
    let (feeds, connections) = refusal_inputs(&untouched);
    let mut peers = Vec::new();
    for feed in &feeds {
        let (server, mut peer) = tokio::io::duplex(64 * 1024);
        peer.write_all(b"PRI * HTTP/2.0\r\n").await?;
        feed.send(gang_spi::Accepted {
            connection: Box::new(server),
            peer: gang_spi::Peer::default(),
        })
        .map_err(|_| "refusal listener closed")?;
        peers.push(peer);
    }
    let refusing = plugin.serve_standby(connections);
    tokio::pin!(refusing);
    assert!(futures::poll!(&mut refusing).is_pending());
    let stopped_at = tokio::time::Instant::now();
    drop(feeds);
    match futures::poll!(&mut refusing) {
        std::task::Poll::Ready(result) => result?,
        std::task::Poll::Pending => {
            tokio::time::timeout(Duration::from_secs(30), refusing).await??;
        }
    }
    assert!(
        stopped_at.elapsed() <= Duration::from_secs(5),
        "flushes must run concurrently"
    );
    for mut peer in peers {
        peer.read_to_end(&mut Vec::new()).await?;
    }
    assert!(matches!(
        plugin.snapshot(&mut SchedulerSnapshot::default()).await,
        Err(gang_spi::PortError::Unavailable(_)),
    ));
    assert_eq!(untouched.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    Ok(())
}
