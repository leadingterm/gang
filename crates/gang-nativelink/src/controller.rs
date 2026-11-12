//! Runs the controller: the stores, the scheduler, and the client and worker services, each
//! served on the connections the caller hands over.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use axum::Router;
use axum::body::Body;
use http::{Request, Response};
use http_body_util::BodyExt;
use nativelink_error::{Code, make_err};
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
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
use crate::globals;
use crate::placeable::PlaceableActions;
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

    let (action_scheduler, worker_scheduler, _) =
        scheduler_factory(&scheduler.spec, &store_manager, None)
            .await
            .map_err(NativeLinkEmbedError::native("scheduler creation failed"))?;
    let (Some(action_scheduler), Some(worker_scheduler)) = (action_scheduler, worker_scheduler)
    else {
        return Err(NativeLinkEmbedError::native("scheduler creation failed")(
            make_err!(Code::Internal, "the scheduler has no action or worker side"),
        ));
    };
    let action_scheduler: Arc<dyn KnownPlatformPropertyProvider> =
        Arc::new(PlaceableActions::new(action_scheduler, spec.fleets.clone()));
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
    let public_router = client_router.fallback_service(api_service(api));
    futures::future::try_join(
        serve(public, public_router, &config.http2, Protocols::Auto),
        serve(worker, worker_router, &config.http2, Protocols::H2),
    )
    .await?;
    Ok(())
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

    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use gang_spi::{ApiResponseBody, Peer, PortError, full_body};
    use gang_testkit::TestResult;
    use gang_types::{Identity, Principal, ProviderName};
    use http::StatusCode;
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
