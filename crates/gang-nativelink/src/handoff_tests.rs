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
        memory_reservation: None,
        command_digest: digest,
        input_root_digest: digest,
        timeout: Duration::from_secs(1),
        platform_properties: std::collections::HashMap::default(),
        pre_fit_reservation: None,
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
struct Allow {
    leading: Arc<std::sync::atomic::AtomicBool>,
    worker_store: Arc<std::sync::atomic::AtomicBool>,
    scopes: Arc<parking_lot::Mutex<Vec<gang_spi::RequestScope>>>,
}

impl Default for Allow {
    fn default() -> Self {
        Self {
            leading: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            worker_store: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            scopes: Arc::default(),
        }
    }
}

impl gang_spi::RequestGate for Allow {
    async fn admit(
        &self,
        scope: gang_spi::RequestScope,
        _: &gang_spi::Peer,
        parts: &mut http::request::Parts,
    ) -> Result<gang_spi::GateDecision, gang_spi::PortError> {
        self.scopes.lock().push(scope);
        if parts.headers.contains_key("x-revoked") {
            return Err(gang_spi::PortError::refused(
                gang_spi::Rejection::PrincipalRevoked,
            ));
        }
        let permitted = match scope {
            gang_spi::RequestScope::Controller => &self.leading,
            gang_spi::RequestScope::WorkerStore => &self.worker_store,
        };
        if !permitted.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(gang_spi::GateDecision::NotLeading);
        }
        Ok(gang_spi::GateDecision::Admit(
            gang_spi::Admitted::Authenticated(gang_spi::Authenticated {
                identity: Arc::new(gang_types::Identity::new(
                    gang_types::ProviderName::try_new("test").expect("valid provider"),
                    gang_types::Principal::try_new("tester").expect("valid principal"),
                )),
                valid_until: None,
            }),
        ))
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
            gate: Allow::default(),
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
    use gang_spi::{ControllerConnections, ControllerDrain, ControllerReady, SchedulerPort};

    let (public_feed, public) = listener();
    let gate = public.gate.clone();
    let (worker_feed, worker) = listener();
    let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
    let (ready, waiting) = ControllerReady::channel();
    let task = tokio::spawn(run_held_controller(
        Arc::clone(&plugin),
        ControllerConnections {
            public,
            worker,
            api: Api,
        },
        ready,
    ));
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
    {
        let scopes = gate.scopes.lock();
        assert_eq!(scopes.len(), 7);
        assert!(
            scopes
                .iter()
                .all(|scope| *scope == gang_spi::RequestScope::Controller)
        );
    }
    drop(api_response);
    drop(worker_feed);
    // Ending the worker stream must not retire the public listener.
    grpc_route(
        &mut public_client,
        "build.bazel.remote.execution.v2.Execution/Execute",
    )
    .await?;
    plugin.hand_off(&mut ControllerDrain::default()).await?;
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

fn test_plugin(server: serde_json::Value) -> gang_testkit::TestResult<crate::NativeLink> {
    let server = serde_json::from_value(server)?;
    Ok(crate::NativeLink::try_new(
        crate::NativeLinkConfig::for_controller("grpc://127.0.0.1:9100".parse()?, &server),
    )?)
}

async fn run_held_controller<A: gang_spi::ApiService>(
    plugin: Arc<crate::NativeLink>,
    connections: gang_spi::ControllerConnections<Allow, Allow, A>,
    ready: gang_spi::ControllerReady,
) -> Result<(), gang_spi::PortError> {
    use gang_spi::SchedulerPort;
    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, Isa, Resources};
    let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
        &FleetName::try_new("test").expect("valid fleet name"),
        Isa::X86_64,
        &"docker",
        &"host:test".parse().expect("valid environment"),
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
        control_acquire_timeout: gang_types::NonZeroDuration::from_secs(1),
        content_store: &store,
        action_store: &store,
        fleets: &fleets,
        max_workers: 8,
        client_action_timeout: Duration::from_mins(10),
        attempt_budget: gang_config::WorkerConfig::default()
            .attempt_budget(gang_config::ActionsConfig::default().timeout),
        observations: &observations,
    };
    plugin.run_controller(&spec, connections, ready).await
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

async fn assert_http_refusal(
    feed: &tokio::sync::mpsc::UnboundedSender<gang_spi::Accepted>,
) -> gang_testkit::TestResult {
    let mut client = client(feed).await?;
    let response = client
        .send_request(
            http::Request::get("http://localhost/v1/info").body(axum::body::Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
        "true"
    );
    let body = axum::body::to_bytes(axum::body::Body::new(response.into_body()), 1024).await?;
    assert!(!body.is_empty());
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

#[tokio::test]
async fn fresh_wire_refusals_coexist_with_a_held_controller_drain() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, ControllerDrain, ControllerReady, SchedulerPort};
    use std::sync::atomic::Ordering;

    let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
    let (public_feed, mut public) = listener();
    let (worker_feed, mut worker) = listener();
    let gate = Allow::default();
    gate.leading.store(false, Ordering::SeqCst);
    gate.worker_store.store(false, Ordering::SeqCst);
    public.gate = gate.clone();
    worker.gate = gate.clone();
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
    assert_leader_refusals(&public_feed, &worker_feed).await?;
    assert_http_refusal(&public_feed).await?;
    assert_wire_refusal(
        &public_feed,
        "build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
    )
    .await?;
    assert_wire_refusal(&worker_feed, "google.bytestream.ByteStream/Write").await?;
    gate.leading.store(true, Ordering::SeqCst);
    gate.worker_store.store(true, Ordering::SeqCst);
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
    gate.leading.store(false, Ordering::SeqCst);
    let mut drain = ControllerDrain::default();
    plugin.hand_off(&mut drain).await?;
    assert!(!drain.complete, "the admitted response is still held");
    assert_leader_refusals(&public_feed, &worker_feed).await?;
    assert_wire_refusal(
        &public_feed,
        "build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
    )
    .await?;
    assert_http_refusal(&public_feed).await?;
    assert_wire_refusal(
        &public_feed,
        "build.bazel.remote.execution.v2.ActionCache/GetActionResult",
    )
    .await?;
    assert_wire_refusal(&public_feed, "google.bytestream.ByteStream/Read").await?;
    release.notify_one();
    let response = admitted.await??;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(axum::body::Body::new(response.into_body()), 1024).await?;
    assert_eq!(body.as_ref(), b"completed");
    plugin.hand_off(&mut drain).await?;
    assert!(drain.complete);
    assert_leader_refusals(&public_feed, &worker_feed).await?;
    assert!(
        !controller.is_finished(),
        "completed drain retains the original listeners"
    );
    drop((public_feed, worker_feed));
    controller.await??;
    Ok(())
}

#[tokio::test]
async fn handoff_before_initial_preparation_is_unavailable() -> gang_testkit::TestResult {
    use gang_spi::{ControllerDrain, SchedulerPort};
    let plugin = test_plugin(serde_json::json!({"transport": "plaintext"}))?;
    assert!(matches!(
        plugin.hand_off(&mut ControllerDrain::default()).await,
        Err(gang_spi::PortError::Unavailable(_))
    ));
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
async fn worker_store_remains_authenticated_during_controller_drain() -> gang_testkit::TestResult {
    use gang_spi::{
        ControllerConnections, ControllerDrain, ControllerReady, RequestScope, SchedulerPort,
    };
    use std::sync::atomic::Ordering;

    let plugin = Arc::new(test_plugin(serde_json::json!({
        "transport": "plaintext",
        "http2": { "max_connections": 1 }
    }))?);
    let (public_feed, mut public) = listener();
    let (worker_feed, mut worker) = listener();
    let gate = Allow::default();
    public.gate = gate.clone();
    worker.gate = gate.clone();
    let (entered, _) = tokio::sync::mpsc::unbounded_channel();
    let (ready, waiting) = ControllerReady::channel();
    let serving = tokio::spawn(run_held_controller(
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
    let mut public_client = client(&public_feed).await?;
    assert_cache_roundtrip(&mut public_client).await?;
    drop(public_client);
    let mut worker_client = client(&worker_feed).await?;
    assert_cache_roundtrip(&mut worker_client).await?;
    gate.leading.store(false, Ordering::SeqCst);
    let mut drain = ControllerDrain::default();
    plugin.hand_off(&mut drain).await?;
    assert_cache_roundtrip(&mut worker_client).await?;
    let mut second_worker_client = client(&worker_feed).await?;
    assert_cache_roundtrip(&mut second_worker_client).await?;
    let response = worker_client
        .send_request(
            http::Request::post(
                "http://localhost/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
            )
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("x-revoked", "true")
            .body(axum::body::Body::empty())?,
        )
        .await?;
    assert_eq!(response.headers()["grpc-status"], "7");
    assert!(
        !response
            .headers()
            .contains_key(gang_types::handover::REQUEST_NOT_STARTED_HEADER)
    );
    assert_wire_refusal(
        &worker_feed,
        "com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
    )
    .await?;
    {
        let scopes = gate.scopes.lock();
        assert!(scopes.contains(&RequestScope::Controller));
        assert!(scopes.contains(&RequestScope::WorkerStore));
    }
    drop((
        worker_client,
        second_worker_client,
        public_feed,
        worker_feed,
    ));
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
    worker_client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
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
        worker_client,
        "google.bytestream.ByteStream/QueryWriteStatus",
        query(),
    )
    .await?;
    let status = QueryWriteStatusResponse::decode(&body[5..])?;
    assert_eq!(status.committed_size, 2);
    assert!(!status.complete);
    let body = grpc_message(
        worker_client,
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
        worker_client,
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
async fn worker_upload_resumes_on_original_controller_after_handoff() -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, ControllerDrain, ControllerReady, SchedulerPort};
    use nativelink_proto::google::bytestream::WriteRequest;
    use std::sync::atomic::Ordering;

    let plugin = Arc::new(test_plugin(serde_json::json!({"transport": "plaintext"}))?);
    let (public_feed, mut public) = listener();
    let (worker_feed, mut worker) = listener();
    let gate = Allow::default();
    public.gate = gate.clone();
    worker.gate = gate.clone();
    let (entered, _) = tokio::sync::mpsc::unbounded_channel();
    let (ready, waiting) = ControllerReady::channel();
    let controller = tokio::spawn(run_held_controller(
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
    let mut worker_client = client(&worker_feed).await?;
    let _ = grpc_exchange(
        &mut worker_client,
        "google.bytestream.ByteStream/Write",
        WriteRequest {
            resource_name: resource.to_owned(),
            write_offset: 0,
            finish_write: false,
            data: "he".into(),
        },
    )
    .await?;
    gate.leading.store(false, Ordering::SeqCst);
    let mut drain = ControllerDrain::default();
    plugin.hand_off(&mut drain).await?;
    assert_wire_refusal(&public_feed, "google.bytestream.ByteStream/Write").await?;
    assert_wire_refusal(
        &worker_feed,
        "com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
    )
    .await?;
    resume_upload(&mut worker_client, resource).await?;
    plugin.hand_off(&mut drain).await?;
    assert!(drain.complete);
    drop((worker_client, public_feed, worker_feed));
    controller.await??;
    Ok(())
}
