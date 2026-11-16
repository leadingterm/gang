//! Runs the controller: the stores, the scheduler, and the client and worker services, each
//! served on the connections the caller hands over.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::body::Body;
use http::{Request, Response};
use http_body_util::BodyExt;
use nativelink_config::cas_server::SchedulerConfig;
use nativelink_config::schedulers::{ExperimentalSimpleSchedulerBackend, SchedulerSpec};
use nativelink_error::{Code, make_err};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::historical_resource_scheduler::{
    HistoricalResourceScheduler, replica_id, snapshot_key,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::ac_server::AcServer;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::store_manager::StoreManager;
use tokio::sync::watch;
use tonic::service::Routes;
use tower::{Service, service_fn};

use gang_spi::{
    ApiRequestBody, ApiService, BoxedError, ControllerConnections, ControllerReady, ControllerSpec,
    RequestGate,
};
use gang_types::FleetCatalog;

use crate::config::NativeLinkConfig;
use crate::counting::CountingWorkerScheduler;
use crate::errors::NativeLinkEmbedError;
use crate::fitted::FittedActions;
use crate::globals;
use crate::placeable::{AdmittedFleets, PlaceableActions};
use crate::serve::{Protocols, serve};
use crate::settings;
use crate::stores::build_stores;

/// What the running controller hands to the plugin: the scheduler handles it queries.
pub(crate) struct Handles {
    /// Connected workers and their assignments.
    pub worker_scheduler: Arc<dyn WorkerScheduler>,
    /// Queued actions and the platform properties they require.
    pub action_scheduler: Arc<dyn KnownPlatformPropertyProvider>,
    /// Every fleet's properties, to match queued actions against.
    pub fleets: FleetCatalog,
    /// Actions that reached a terminal result on a worker since the controller started.
    pub completed: Arc<AtomicU64>,
}

/// Resets the published handles to `None` when the controller stops, however it stops.
struct Serving<'a>(&'a watch::Sender<Option<Arc<Handles>>>);

impl Drop for Serving<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|slot| *slot = None);
    }
}

/// Builds the controller from `config` and `spec`, publishes its [`Handles`] on `handles`, and
/// serves the public connections and the worker connections until both streams have ended or
/// the returned future is dropped. The public listener also answers the Gang API as the fallback
/// of its router, behind its gate.
/// It signals `ready` once the controller is built, before it serves; a run that fails first
/// drops `ready` unsent.
pub(crate) async fn run<P: RequestGate, W: RequestGate, A: ApiService>(
    config: &NativeLinkConfig,
    spec: &ControllerSpec<'_>,
    connections: ControllerConnections<P, W, A>,
    ready: ControllerReady,
    handles: &watch::Sender<Option<Arc<Handles>>>,
) -> Result<(), NativeLinkEmbedError> {
    if handles.borrow().is_some() {
        return Err(NativeLinkEmbedError::AlreadyServing);
    }
    globals::init(config.max_open_files);

    // Checks that need only the configuration run before any store is built.
    let scheduler = settings::scheduler(config, spec)?;
    let client_services = settings::client_services(config);
    let worker_services = settings::worker_services(config);
    let store_manager = build_stores(settings::controller_stores(spec)?).await?;

    let (action_scheduler, worker_scheduler) = scheduler_stack(&scheduler, spec.fleets)?;
    let completed = Arc::new(AtomicU64::new(0));
    let worker_scheduler: Arc<dyn WorkerScheduler> = Arc::new(CountingWorkerScheduler::new(
        worker_scheduler,
        Arc::clone(&completed),
    ));
    let actions = HashMap::from([(scheduler.name.clone(), Arc::clone(&action_scheduler))]);
    let workers = HashMap::from([(scheduler.name.clone(), Arc::clone(&worker_scheduler))]);

    let shared = Shared {
        store_manager: &store_manager,
        actions: &actions,
        workers: &workers,
        max_decoding_message_size: config.http2.max_message_size.get().as_usize(),
    };
    let client_router = client_routes(&client_services, &shared).await?;
    let worker_router = worker_routes(&worker_services, &shared)?;

    let published = Arc::new(Handles {
        worker_scheduler,
        action_scheduler,
        fleets: spec.fleets.clone(),
        completed,
    });
    let claimed = handles.send_if_modified(|slot| {
        if slot.is_some() {
            return false;
        }
        *slot = Some(published);
        true
    });
    if !claimed {
        return Err(NativeLinkEmbedError::AlreadyServing);
    }
    let _serving = Serving(handles);
    ready.signal();

    let ControllerConnections {
        public,
        worker,
        api,
    } = connections;
    let public_router = with_request_metadata(client_router).fallback_service(api_service(api));
    futures::future::try_join(
        serve(public, public_router, &config.http2, Protocols::Auto),
        serve(worker, worker_router, &config.http2, Protocols::H2),
    )
    .await?;
    Ok(())
}

/// The action side and the worker side of the scheduler.
type SchedulerHandles = (
    Arc<dyn KnownPlatformPropertyProvider>,
    Arc<dyn WorkerScheduler>,
);

/// Builds the action stack from the client inward: `PlaceableActions`,
/// `HistoricalResourceScheduler`, `FittedActions`, then `SimpleScheduler`. Client refusal
/// stays outside, while learned-reservation fitting happens inside the historical layer.
fn scheduler_stack(
    scheduler: &SchedulerConfig,
    fleets: &FleetCatalog,
) -> Result<SchedulerHandles, NativeLinkEmbedError> {
    let SchedulerSpec::HistoricalResource(historical_spec) = &scheduler.spec else {
        return Err(NativeLinkEmbedError::native("scheduler creation failed")(
            make_err!(Code::Internal, "expected a historical resource scheduler"),
        ));
    };
    let SchedulerSpec::Simple(simple) = historical_spec.scheduler.as_ref() else {
        return Err(NativeLinkEmbedError::native("scheduler creation failed")(
            make_err!(Code::Internal, "expected a nested simple scheduler"),
        ));
    };
    if !matches!(
        simple.experimental_backend,
        None | Some(ExperimentalSimpleSchedulerBackend::Memory)
    ) {
        return Err(NativeLinkEmbedError::native("scheduler creation failed")(
            make_err!(Code::Internal, "expected the memory scheduler backend"),
        ));
    }
    HistoricalResourceScheduler::validate(historical_spec)
        .map_err(NativeLinkEmbedError::native("scheduler creation failed"))?;
    let sketch = Arc::new(HistoricalResourceScheduler::sketch_from_spec(
        historical_spec,
    ));
    let notify = Arc::new(tokio::sync::Notify::new());
    let db =
        memory_awaited_action_db_factory(simple.retain_completed_for_s, &notify, SystemTime::now);
    let (simple_actions, worker_scheduler) =
        SimpleScheduler::new_with_resource_sketch(simple, db, notify, None, Some(sketch.clone()));
    let admitted = AdmittedFleets::default();
    let fitted = Arc::new(FittedActions::new(
        simple_actions,
        fleets.clone(),
        admitted.clone(),
    ));
    let historical = Arc::new(HistoricalResourceScheduler::new(
        historical_spec,
        fitted,
        sketch.clone(),
    ));
    if historical_spec.learn_from_workers {
        HistoricalResourceScheduler::spawn_snapshot_writer(
            sketch,
            None,
            Duration::from_secs(historical_spec.snapshot_interval_s),
            snapshot_key(&replica_id(&historical_spec.replica_id)),
        );
    }
    let action_scheduler: Arc<dyn KnownPlatformPropertyProvider> =
        Arc::new(PlaceableActions::new(historical, fleets.clone(), admitted));
    Ok((action_scheduler, worker_scheduler))
}

/// Decodes Bazel's `RequestMetadata` header into the request context so actions carry their target
/// and mnemonic and the scheduler learns per target.
fn with_request_metadata(router: Router) -> Router {
    router.layer(nativelink_util::telemetry::OtlpLayer::new(false))
}

/// The Gang API as the controller's fallback service: an infallible axum service that maps the
/// request body to the SPI body type and the response body back to axum's.
fn api_service<A: ApiService>(
    api: A,
) -> impl Service<Request<Body>, Response = Response<Body>, Error = Infallible, Future: Send>
+ Clone
+ Send
+ Sync
+ 'static {
    service_fn(move |request: Request<Body>| {
        let api = api.clone();
        async move {
            let (parts, body) = request.into_parts();
            let body: ApiRequestBody = body
                .map_err(|error| -> BoxedError { Box::new(error) })
                .boxed_unsync();
            let response = api.call(Request::from_parts(parts, body)).await;
            let (parts, body) = response.into_parts();
            Ok::<_, Infallible>(Response::from_parts(parts, Body::new(body)))
        }
    })
}

/// The inputs every service set is built from.
struct Shared<'a> {
    /// The stores the services read and write.
    store_manager: &'a StoreManager,
    /// The action schedulers, keyed by scheduler name.
    actions: &'a HashMap<String, Arc<dyn KnownPlatformPropertyProvider>>,
    /// The worker schedulers, keyed by scheduler name.
    workers: &'a HashMap<String, Arc<dyn WorkerScheduler>>,
    /// The largest gRPC message each service decodes.
    max_decoding_message_size: usize,
}

/// The gRPC services of the client listener, served as an axum router.
async fn client_routes(
    services: &settings::ClientServices,
    shared: &Shared<'_>,
) -> Result<Router, NativeLinkEmbedError> {
    let limit = shared.max_decoding_message_size;
    let compression =
        RemoteCacheCompressionInstances::from_capabilities_configs(&services.capabilities);
    let ac = AcServer::new(&services.ac, shared.store_manager)
        .map_err(NativeLinkEmbedError::native("action cache service"))?;
    let cas = CasServer::new(&services.cas, shared.store_manager, &compression)
        .map_err(NativeLinkEmbedError::native("cas service"))?;
    let execution = ExecutionServer::new(&services.execution, shared.actions, shared.store_manager)
        .map_err(NativeLinkEmbedError::native("execution service"))?;
    let bytestream =
        ByteStreamServer::new(&services.bytestream, shared.store_manager, &compression)
            .map_err(NativeLinkEmbedError::native("bytestream service"))?;
    let capabilities = CapabilitiesServer::new(
        &services.capabilities,
        shared.actions,
        &compression,
        &services.cas,
    )
    .await
    .map_err(NativeLinkEmbedError::native("capabilities service"))?;
    let routes = Routes::builder()
        .routes()
        .add_service(ac.into_service().max_decoding_message_size(limit))
        .add_service(cas.into_service().max_decoding_message_size(limit))
        .add_service(
            execution
                .clone()
                .into_service()
                .max_decoding_message_size(limit),
        )
        .add_service(
            execution
                .into_operations_service()
                .max_decoding_message_size(limit),
        )
        .add_service(bytestream.into_service().max_decoding_message_size(limit))
        .add_service(capabilities.into_service().max_decoding_message_size(limit));
    Ok(routes.into_axum_router())
}

/// The gRPC services of the worker listener, served as an axum router.
fn worker_routes(
    services: &settings::WorkerServices,
    shared: &Shared<'_>,
) -> Result<Router, NativeLinkEmbedError> {
    let limit = shared.max_decoding_message_size;
    let compression = RemoteCacheCompressionInstances::from_capabilities_configs(&[]);
    let ac = AcServer::new(&services.ac, shared.store_manager)
        .map_err(NativeLinkEmbedError::native("action cache service"))?;
    let cas = CasServer::new(&services.cas, shared.store_manager, &compression)
        .map_err(NativeLinkEmbedError::native("cas service"))?;
    let bytestream =
        ByteStreamServer::new(&services.bytestream, shared.store_manager, &compression)
            .map_err(NativeLinkEmbedError::native("bytestream service"))?;
    let worker_api = WorkerApiServer::new(&services.worker_api, shared.workers)
        .map_err(NativeLinkEmbedError::native("worker api service"))?;
    let routes = Routes::builder()
        .routes()
        .add_service(ac.into_service().max_decoding_message_size(limit))
        .add_service(cas.into_service().max_decoding_message_size(limit))
        .add_service(bytestream.into_service().max_decoding_message_size(limit))
        .add_service(worker_api.into_service().max_decoding_message_size(limit));
    Ok(routes.into_axum_router())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::UNIX_EPOCH;

    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD_NO_PAD;
    use futures::StreamExt;
    use gang_spi::{ApiResponseBody, Peer, PortError, RenderedStoreSpec, full_body};
    use gang_testkit::TestResult;
    use gang_types::{
        Bytes, FleetName, FleetProperties, Identity, Isa, Principal, ProviderName, Resources,
        property,
    };
    use http::StatusCode;
    use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
    use nativelink_util::action_messages::{
        ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::operation_state_manager::OperationFilter;
    use nativelink_util::origin_event::{BAZEL_METADATA_KEY, request_metadata_from_baggage};
    use opentelemetry::baggage::BaggageExt;
    use prost::Message;
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;
    use crate::gate::admit;

    /// Puts a fixed identity in the request extensions, as the gate does for a verified caller.
    #[derive(Clone)]
    struct Identify;

    impl RequestGate for Identify {
        fn admit(
            &self,
            _peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<(), PortError>> + Send {
            parts.extensions.insert(alice());
            std::future::ready(Ok(()))
        }
    }

    fn alice() -> Identity {
        Identity::new(
            ProviderName::try_new("local").expect("valid provider"),
            Principal::try_new("alice").expect("valid principal"),
        )
    }

    /// Answers every request the router does not route with its path and the identity the gate
    /// put in the request extensions.
    #[derive(Clone)]
    struct Echo;

    impl ApiService for Echo {
        fn call(
            &self,
            request: Request<ApiRequestBody>,
        ) -> impl Future<Output = Response<ApiResponseBody>> + Send {
            let identity = request
                .extensions()
                .get::<Identity>()
                .map_or_else(|| "anonymous".to_owned(), ToString::to_string);
            let answer = format!("{} {identity}", request.uri().path());
            std::future::ready(Response::new(full_body(answer)))
        }
    }

    /// Sends `path` through a router shaped like the public listener's: one REAPI route, the Gang
    /// API as the fallback, and the identity gate over both. Returns the status and the body.
    async fn fetch(path: &str) -> Result<(StatusCode, String), Box<dyn Error>> {
        let router = Router::new()
            .route("/reapi.Service/Method", get(|| async { "reapi" }))
            .fallback_service(api_service(Echo))
            .layer(from_fn_with_state(Identify, admit::<Identify>));
        let response = router
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        Ok((status, String::from_utf8(body.to_vec())?))
    }

    /// Fitting outside the historical layer would leave memory at 30 GiB. Losing the operation
    /// ID handoff would fit against all fleets: B's CPU reduction (8000/12000) is smaller than
    /// A's memory reduction (22/30), so CPU would become 4000 instead of remaining 12000.
    #[tokio::test]
    async fn scheduler_stack_fits_reservations_to_the_admitted_fleet() {
        let fleets = FleetCatalog::try_new(
            [("fleet-a", 16, 8_u64), ("fleet-b", 4, 32)]
                .into_iter()
                .map(|(name, vcpus, memory_gib)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).expect("valid fleet name"),
                        Isa::X86_64,
                        &"docker",
                        Resources {
                            vcpus,
                            memory: Bytes::new(memory_gib << 30),
                            disk: Bytes::new(100_u64 << 30),
                        },
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("valid fleet catalog");
        let config: NativeLinkConfig = serde_json::from_value(json!({
            "scheduler_name": "MAIN_SCHEDULER",
            "cold_start": { "memory": "30GiB" },
        }))
        .expect("valid controller config");
        let store = RenderedStoreSpec(json!({ "memory": {} }));
        let spec = ControllerSpec {
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            client_action_timeout: Duration::from_secs(600),
        };
        let mut scheduler = settings::scheduler(&config, &spec).expect("valid scheduler config");
        let SchedulerSpec::HistoricalResource(historical) = &mut scheduler.spec else {
            panic!("expected historical scheduler");
        };
        historical.learn_from_workers = false;
        let (actions, _workers) = scheduler_stack(&scheduler, &fleets).expect("scheduler stack");
        let operation = OperationId::String("stack-wiring".to_owned());
        let digest = DigestInfo::new([1; 32], 1);
        let info = Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_secs(60),
            platform_properties: [(property::CPU_MILLICORES.to_owned(), "12000".to_owned())]
                .into_iter()
                .collect(),
            priority: 0,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH,
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest,
            }),
        });
        actions
            .add_action(operation.clone(), info)
            .await
            .expect("client request admits fleet A");
        let mut results = actions
            .filter_operations(OperationFilter {
                client_operation_id: Some(operation),
                ..Default::default()
            })
            .await
            .expect("filter submitted operation");
        let result = results.next().await.expect("submitted operation is queued");
        let (info, _) = result.as_action_info().await.expect("queued action info");
        assert_eq!(
            info.platform_properties[property::CPU_MILLICORES],
            "12000",
            "scheduler stack must hand the admitted fleet to fitting by operation ID"
        );
        assert_eq!(
            info.platform_properties[property::MEMORY_KB],
            "8388608",
            "scheduler stack must fit after historical cold-start memory is applied"
        );
    }

    /// Confirms the client router puts Bazel request metadata in the scheduler's context.
    #[tokio::test]
    async fn request_metadata_reaches_the_scheduler_context() -> TestResult {
        /// Returns the action identity from the current request's baggage.
        async fn handler() -> String {
            opentelemetry::Context::current()
                .baggage()
                .get(BAZEL_METADATA_KEY)
                .and_then(|value| request_metadata_from_baggage(value.as_str()).ok())
                .map_or_else(
                    || "none".to_owned(),
                    |metadata| format!("{} {}", metadata.target_id, metadata.action_mnemonic),
                )
        }

        let metadata = RequestMetadata {
            target_id: "//pkg:lib".to_owned(),
            action_mnemonic: "Rustc".to_owned(),
            ..Default::default()
        };
        let router =
            with_request_metadata(Router::new().route("/probe", axum::routing::post(handler)));
        let response = router
            .oneshot(
                Request::post("/probe")
                    .header(
                        "build.bazel.remote.execution.v2.requestmetadata-bin",
                        BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec()),
                    )
                    .body(Body::empty())?,
            )
            .await?;
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        assert_eq!(body.as_ref(), b"//pkg:lib Rustc");
        Ok(())
    }

    #[tokio::test]
    async fn the_api_answers_unknown_paths_with_the_gates_identity() -> TestResult {
        let (status, body) = fetch("/unknown").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "/unknown local:alice");
        Ok(())
    }

    #[tokio::test]
    async fn the_api_answers_v1_paths_with_the_gates_identity() -> TestResult {
        let (status, body) = fetch("/v1/status").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "/v1/status local:alice");
        Ok(())
    }

    #[tokio::test]
    async fn reapi_routes_are_served_before_the_api() -> TestResult {
        let (status, body) = fetch("/reapi.Service/Method").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "reapi");
        Ok(())
    }
}
