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
        _: &gang_spi::Peer,
        _: &mut http::request::Parts,
    ) -> impl Future<Output = Result<gang_spi::Admitted, gang_spi::PortError>> + Send {
        std::future::ready(Ok(gang_spi::Admitted::Authenticated(
            gang_spi::Authenticated {
                identity: Arc::new(gang_types::Identity::new(
                    gang_types::ProviderName::try_new("test").expect("valid provider"),
                    gang_types::Principal::try_new("tester").expect("valid principal"),
                )),
                valid_until: None,
            },
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

async fn execute(
    client: &mut hyper::client::conn::http2::SendRequest<axum::body::Body>,
) -> gang_testkit::TestResult<http::StatusCode> {
    client.ready().await?;
    let request =
        http::Request::post("http://localhost/build.bazel.remote.execution.v2.Execution/Execute")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .body(axum::body::Body::empty())?;
    Ok(client.send_request(request).await?.status())
}

async fn routing_case(dedicated: bool) -> gang_testkit::TestResult {
    use gang_spi::{ControllerConnections, ControllerReady, ControllerSpec, RenderedStoreSpec};
    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, Isa, Resources};

    let (public_feed, public) = listener();
    let (execution_feed, execution) = listener();
    let (worker_feed, worker) = listener();
    let (ready, waiting) = ControllerReady::channel();
    let task = tokio::spawn(async move {
        let config: crate::config::NativeLinkConfig =
            serde_json::from_value(serde_json::json!({"scheduler_name": "MAIN"}))
                .expect("valid test configuration");
        let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("test").expect("valid fleet name"),
            Isa::X86_64,
            &"docker",
            Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(16 << 30),
            },
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid fleet");
        let store = RenderedStoreSpec(serde_json::json!({"memory": {}}));
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            max_workers: 8,
            client_action_timeout: Duration::from_mins(10),
            observations: &observations,
        };
        let (handles, _) = tokio::sync::watch::channel(None);
        crate::controller::run(
            &config,
            &spec,
            ControllerConnections {
                public,
                execution: dedicated.then_some(execution),
                worker,
                api: Api,
            },
            ready,
            &handles,
        )
        .await
    });
    waiting.await?;
    let mut public_client = client(&public_feed).await?;
    let public_status = execute(&mut public_client).await?;
    if dedicated {
        assert_eq!(public_status, http::StatusCode::NOT_FOUND);
        let mut execution_client = client(&execution_feed).await?;
        assert_ne!(
            execute(&mut execution_client).await?,
            http::StatusCode::NOT_FOUND
        );
        drop(public_feed);
        drop(worker_feed);
        // Both ended streams must not retire the remaining execution listener.
        assert_ne!(
            execute(&mut execution_client).await?,
            http::StatusCode::NOT_FOUND
        );
        drop(execution_feed);
    } else {
        assert_ne!(public_status, http::StatusCode::NOT_FOUND);
        drop(public_feed);
        drop(execution_feed);
        drop(worker_feed);
    }
    task.await??;
    Ok(())
}

#[tokio::test]
async fn dedicated_execution_listener_and_all_stream_handoff() -> gang_testkit::TestResult {
    routing_case(true).await
}

#[tokio::test]
async fn public_listener_serves_execution_without_dedicated_listener() -> gang_testkit::TestResult {
    routing_case(false).await
}
