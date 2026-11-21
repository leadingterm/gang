//! Runs the controller: the stores, the scheduler, and the client and worker services, each
//! served on the connections the caller hands over.

use std::collections::HashMap;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use axum::Router;
use axum::body::Body;
use http::{Request, Response};
use http_body_util::BodyExt;
use nativelink_config::cas_server::SchedulerConfig;
use nativelink_config::schedulers::{
    ExperimentalSimpleSchedulerBackend, SchedulerSpec, SimpleSpec,
};
use nativelink_error::{Code, make_err};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::historical_resource_scheduler::HistoricalResourceScheduler;
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::resource_sketch::ResourceSketch;
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
use parking_lot::Mutex;
use tokio::sync::{Notify, OnceCell, watch};
use tonic::service::Routes;
use tower::{Service, service_fn};

use gang_clock::{Clock, SystemClock};
use gang_config::Http2Config;
use gang_spi::{
    ApiRequestBody, ApiService, BoxedError, ControllerConnections, ControllerReady, ControllerSpec,
    RequestGate,
};
use gang_types::{FleetCatalog, SaturatingInto};

use crate::config::NativeLinkConfig;
use crate::durations::ActionDurations;
use crate::errors::NativeLinkEmbedError;
use crate::fitted::FittedActions;
use crate::globals;
use crate::handoff::{HandOff, HandOffActions};
use crate::history::Histories;
use crate::observer::BuildObserverLayer;
use crate::observing::ObservingWorkerScheduler;
use crate::placeable::{AdmittedFleets, PlaceableActions};
use crate::serve::{Listener, ScopedRoutes, ServeShutdown, serve};
use crate::settings;
use crate::settings::WORKER_CONNECTIONS;
use crate::source_rates::SourceRates;
use crate::store_usage::StoreUsage;
use crate::stores::build_stores;

/// What the running controller hands to the plugin: the scheduler handles it queries.
pub(crate) struct Handles {
    /// Connected workers and their assignments.
    pub worker_scheduler: Arc<ObservingWorkerScheduler>,
    /// Queued actions and the platform properties they require.
    pub action_scheduler: Arc<dyn KnownPlatformPropertyProvider>,
    /// Every fleet's properties, to match queued actions against.
    pub fleets: Arc<FleetCatalog>,
    /// Actions that reached a terminal result on a worker since the controller started.
    pub completed: Arc<AtomicU64>,
    /// Learned worker slot durations.
    pub durations: Arc<Mutex<ActionDurations>>,
    /// Server clock shared by completion receipts, duration writers and snapshot readers.
    pub clock: Arc<dyn Clock>,
    pub rates: Arc<SourceRates>,
    pub scratch: tokio::sync::Mutex<crate::snapshot::Scratch>,
    /// The controller stores' usage, read through one reused metrics collector.
    pub stores: StoreUsage,
}

/// Borrowed view of process-owned controller state.
pub(crate) struct ControllerState<'a> {
    pub(crate) handles: &'a watch::Sender<Option<Arc<Handles>>>,
    pub(crate) histories: &'a Mutex<Option<Arc<Histories>>>,
    pub(crate) resources: &'a OnceLock<Arc<ResourceSketch>>,
    pub(crate) shared: &'a SharedServices,
}

/// Resets the published handles to `None` when the controller stops, however it stops.
struct Serving<'a>(&'a watch::Sender<Option<Arc<Handles>>>);

impl Drop for Serving<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|slot| *slot = None);
    }
}

/// Process-lived stores and shared handlers, reused across controller terms and standby.
/// The first term creates stores before its scheduler, then builds shared routes from copied
/// capabilities metadata. The cached routes must not retain a term's scheduler.
#[derive(Default)]
pub(crate) struct SharedServices {
    /// Initialized before the first term's scheduler.
    stores: OnceCell<Arc<StoreManager>>,
    /// Initialized after the first scheduler supplies capabilities metadata.
    routes: OnceCell<SharedRoutes>,
}

struct SharedRoutes {
    public: Router,
    worker: Router,
    standby_public: Router,
    standby_worker: Router,
    limits: ConnectionLimits,
    observations: gang_spi::ObservationSink,
}

impl SharedServices {
    async fn stores(
        &self,
        spec: &ControllerSpec<'_>,
    ) -> Result<&Arc<StoreManager>, NativeLinkEmbedError> {
        self.stores
            .get_or_try_init(|| async { build_stores(settings::controller_stores(spec)?).await })
            .await
    }

    async fn prepared_routes(
        &self,
        config: &NativeLinkConfig,
        spec: &ControllerSpec<'_>,
        shared: &Shared<'_>,
        services: (&settings::ClientServices, &settings::WorkerServices),
        standby: (Router, Router),
    ) -> Result<&SharedRoutes, NativeLinkEmbedError> {
        self.routes
            .get_or_try_init(|| async {
                Ok::<_, NativeLinkEmbedError>(SharedRoutes {
                    public: with_request_metadata(
                        client_routes(services.0, shared).await?,
                        spec.observations.clone(),
                    ),
                    worker: worker_routes(services.1, shared)?,
                    standby_public: with_request_metadata(standby.0, spec.observations.clone()),
                    standby_worker: standby.1,
                    limits: connection_limits(&config.http2, spec.max_workers),
                    observations: spec.observations.clone(),
                })
            })
            .await
    }

    pub(crate) fn observations(&self) -> Option<gang_spi::ObservationSink> {
        self.routes.get().map(|routes| routes.observations.clone())
    }

    pub(crate) fn standby<A: ApiService>(
        &self,
        api: A,
    ) -> Option<(ScopedRoutes, ScopedRoutes, NonZeroUsize, NonZeroUsize)> {
        let routes = self.routes.get()?;
        Some((
            ScopedRoutes {
                shared: routes.public.clone().fallback_service(api_service(api)),
                leader: routes.standby_public.clone(),
            },
            ScopedRoutes {
                shared: routes.worker.clone(),
                leader: routes.standby_worker.clone(),
            },
            routes.limits.public,
            routes.limits.worker,
        ))
    }
}

/// Builds the controller and serves its public and worker connections.
/// After all listener streams end, it drains the controller within the configured grace.
/// It signals `ready` once the controller is built, before it serves; a run that fails first
/// drops `ready` unsent.
pub(crate) async fn run<P: RequestGate, W: RequestGate, A: ApiService>(
    config: &NativeLinkConfig,
    spec: &ControllerSpec<'_>,
    connections: ControllerConnections<P, W, A>,
    ready: ControllerReady,
    ControllerState {
        handles,
        histories,
        resources,
        shared: shared_services,
    }: ControllerState<'_>,
) -> Result<(), NativeLinkEmbedError> {
    if handles.borrow().is_some() {
        return Err(NativeLinkEmbedError::AlreadyServing);
    }
    globals::init(config.max_open_files);

    // Checks that need only the configuration run before any store is built.
    let scheduler = settings::scheduler(config, spec)?;
    let client_services = settings::client_services(config);
    let worker_services = settings::worker_services(config);
    let store_manager = shared_services.stores(spec).await?;

    let SchedulerStack {
        actions: action_scheduler,
        workers: worker_scheduler,
        resource,
    } = scheduler_stack(&scheduler, spec.fleets, &spec.actions, resources)?;
    let handoff = HandOff::new();
    let action_scheduler: Arc<dyn KnownPlatformPropertyProvider> =
        Arc::new(HandOffActions::new(action_scheduler, handoff.clone()));
    let completed = Arc::new(AtomicU64::new(0));
    let durations = retained_durations(histories, resource, &spec.actions);
    let clock = Arc::clone(&spec.clock);
    let fleets = Arc::new(spec.fleets.clone());
    let rates = Arc::new(
        SourceRates::new(&spec.scaling, fleets.len(), Arc::clone(&clock))
            .map_err(NativeLinkEmbedError::native("source rates"))?,
    );
    let draining = Arc::new(ObservingWorkerScheduler::new(
        worker_scheduler,
        Arc::clone(&action_scheduler)
            as Arc<dyn nativelink_util::operation_state_manager::ClientStateManager>,
        Arc::clone(&completed),
        spec.observations.clone(),
        handoff.clone(),
        Arc::clone(&durations),
        Arc::clone(&clock),
        Arc::clone(&fleets),
        Arc::clone(&rates),
    ));
    let worker_scheduler: Arc<dyn WorkerScheduler> = draining.clone();
    let actions = HashMap::from([(scheduler.name.clone(), Arc::clone(&action_scheduler))]);
    let workers = HashMap::from([(scheduler.name.clone(), Arc::clone(&worker_scheduler))]);

    let shared = Shared {
        store_manager,
        actions: &actions,
        workers: &workers,
        max_decoding_message_size: config.http2.max_message_size.get().as_usize(),
    };
    let (client_leader, standby_public) = client_leader_routes(&client_services, &shared)?;
    let (worker_leader, standby_worker) = worker_leader_routes(&worker_services, &shared)?;
    let routes = shared_services
        .prepared_routes(
            config,
            spec,
            &shared,
            (&client_services, &worker_services),
            (standby_public, standby_worker),
        )
        .await?;

    let published = Arc::new(Handles {
        worker_scheduler: draining.clone(),
        action_scheduler,
        fleets,
        completed,
        durations,
        clock,
        rates,
        scratch: tokio::sync::Mutex::new(crate::snapshot::Scratch::default()),
        stores: StoreUsage::new(Arc::clone(store_manager)),
    });
    publish(handles, published)?;
    let _serving = Serving(handles);
    ready.signal();

    serve_connections(
        config,
        connections,
        ClientAndWorkerRoutes {
            public: ScopedRoutes {
                shared: routes.public.clone(),
                leader: with_request_metadata(client_leader, spec.observations.clone()),
            },
            worker: ScopedRoutes {
                shared: routes.worker.clone(),
                leader: worker_leader,
            },
        },
        routes.limits,
        &draining,
        handoff,
        spec.observations.clone(),
    )
    .await
}

fn retained_durations(
    histories: &Mutex<Option<Arc<Histories>>>,
    resource: Arc<ResourceSketch>,
    actions: &gang_config::ActionsConfig,
) -> Arc<Mutex<ActionDurations>> {
    let history = Arc::clone(histories.lock().get_or_insert_with(|| {
        Arc::new(Histories::new(
            resource,
            Arc::new(Mutex::new(ActionDurations::new(
                actions.history_keys,
                actions.history_window,
            ))),
        ))
    }));
    Arc::clone(&history.durations)
}

fn publish(
    handles: &watch::Sender<Option<Arc<Handles>>>,
    published: Arc<Handles>,
) -> Result<(), NativeLinkEmbedError> {
    let claimed = handles.send_if_modified(|slot| {
        if slot.is_some() {
            return false;
        }
        *slot = Some(published);
        true
    });
    if claimed {
        Ok(())
    } else {
        Err(NativeLinkEmbedError::AlreadyServing)
    }
}

/// Routers built for the client and worker listener roles.
struct ClientAndWorkerRoutes {
    public: ScopedRoutes,
    worker: ScopedRoutes,
}

/// Serves all listeners, then drains the scheduler after the last connection stream ends.
async fn serve_connections<P: RequestGate, W: RequestGate, A: ApiService>(
    config: &NativeLinkConfig,
    connections: ControllerConnections<P, W, A>,
    routes: ClientAndWorkerRoutes,
    limits: ConnectionLimits,
    draining: &ObservingWorkerScheduler,
    handoff: HandOff,
    observations: gang_spi::ObservationSink,
) -> Result<(), NativeLinkEmbedError> {
    let ClientAndWorkerRoutes {
        public: client_router,
        worker: worker_router,
    } = routes;
    let ControllerConnections {
        public,
        worker,
        api,
    } = connections;
    let public_router = ScopedRoutes {
        shared: client_router.shared.fallback_service(api_service(api)),
        leader: client_router.leader,
    };
    let (ended, mut endings) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = ServeShutdown {
        ended,
        handoff,
        mode: crate::serve::ServeMode::Controller,
        observations: Some(observations),
    };
    let clock = SystemClock::new();
    let serving = async {
        tokio::try_join!(
            serve(
                public,
                public_router,
                &config.http2,
                Listener::Public,
                limits.public,
                clock,
                shutdown.clone()
            ),
            serve(
                worker,
                worker_router,
                &config.http2,
                Listener::Worker,
                limits.worker,
                clock,
                shutdown
            ),
        )?;
        Ok::<(), NativeLinkEmbedError>(())
    };
    tokio::pin!(serving);
    for _ in 0..2 {
        tokio::select! {
            result = &mut serving => return result,
            _ = endings.recv() => {}
        }
    }
    let drained = clock
        .timeout(config.http2.shutdown_grace, async {
            draining.begin_hand_off().await;
            serving.await
        })
        .await;
    if let Ok(result) = drained {
        result?;
    } else {
        tracing::warn!("controller hand-off exceeded the shutdown grace");
    }
    Ok(())
}

/// Worker generations that overlap on one instance: the one stopping (certificate renewal,
/// termination, or a dead socket not yet noticed) and the one replacing it.
const WORKER_GENERATIONS: u64 = 2;

/// How many connections each listener serves at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConnectionLimits {
    /// The public listener's limit, as configured.
    public: NonZeroUsize,
    /// The worker listener's limit: every connection of two generations of every worker the
    /// fleets can run, and at least one.
    worker: NonZeroUsize,
}

/// The connection limits of the listeners under `tuning`, for fleets of at most `max_workers`.
fn connection_limits(tuning: &Http2Config, max_workers: u64) -> ConnectionLimits {
    let worker: usize = max_workers
        .saturating_mul(WORKER_GENERATIONS)
        .saturating_mul(WORKER_CONNECTIONS)
        .saturating_into();
    ConnectionLimits {
        public: tuning.max_connections,
        worker: NonZeroUsize::new(worker).unwrap_or(NonZeroUsize::MIN),
    }
}

/// The action side and the worker side of the scheduler.
type SchedulerHandles = (
    Arc<dyn KnownPlatformPropertyProvider>,
    Arc<dyn WorkerScheduler>,
);

struct SchedulerStack {
    actions: Arc<dyn KnownPlatformPropertyProvider>,
    workers: Arc<dyn WorkerScheduler>,
    resource: Arc<ResourceSketch>,
}

/// The action stack over `NativeLink`'s simple scheduler.
fn scheduler_stack(
    scheduler: &SchedulerConfig,
    fleets: &FleetCatalog,
    actions: &gang_config::ActionsConfig,
    resources: &OnceLock<Arc<ResourceSketch>>,
) -> Result<SchedulerStack, NativeLinkEmbedError> {
    let mut resource = None;
    let (actions, workers) = action_stack(
        scheduler,
        fleets,
        actions,
        resources,
        |simple, notify, sketch| {
            resource = Some(Arc::clone(&sketch));
            let db = memory_awaited_action_db_factory(
                simple.retain_completed_for_s,
                &notify,
                SystemTime::now,
            );
            SimpleScheduler::new_with_resource_sketch(simple, db, notify, None, Some(sketch))
        },
    )?;
    let resource = resource.ok_or_else(|| {
        NativeLinkEmbedError::native("scheduler creation failed")(make_err!(
            Code::Internal,
            "resource sketch was not constructed"
        ))
    })?;
    Ok(SchedulerStack {
        actions,
        workers,
        resource,
    })
}

/// Builds the action stack from the client inward: `PlaceableActions`,
/// `HistoricalResourceScheduler`, `FittedActions`, then `SimpleScheduler`. Client refusal
/// stays outside, while learned-reservation fitting happens inside the historical layer,
/// over the simple scheduler `simple_scheduler` builds from the nested spec, the scheduler's
/// change notification and the sketch the historical layer learns from.
fn action_stack(
    scheduler: &SchedulerConfig,
    fleets: &FleetCatalog,
    actions: &gang_config::ActionsConfig,
    resources: &OnceLock<Arc<ResourceSketch>>,
    simple_scheduler: impl FnOnce(
        &SimpleSpec,
        Arc<Notify>,
        Arc<ResourceSketch>,
    ) -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>),
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
    let sketch = resources.get_or_init(|| {
        let sketch = Arc::new(HistoricalResourceScheduler::sketch_from_spec(
            historical_spec,
        ));
        // Maintenance settles half-window histograms and expires stale entries. Gang's
        // learned-history port owns persistence, so no native store or replica key is used.
        HistoricalResourceScheduler::spawn_snapshot_writer(
            Arc::clone(&sketch),
            None,
            actions.history_flush_interval.get(),
            String::new(),
        );
        sketch
    });
    let notify = Arc::new(Notify::new());
    let (simple_actions, worker_scheduler) = simple_scheduler(simple, notify, Arc::clone(sketch));
    let admitted = AdmittedFleets::default();
    let fitted = Arc::new(FittedActions::new(
        simple_actions,
        fleets.clone(),
        admitted.clone(),
        actions.memory.min_reservation,
        actions.disk.min_reservation,
    ));
    let historical = Arc::new(HistoricalResourceScheduler::new(
        historical_spec,
        fitted,
        Arc::clone(sketch),
    ));
    let action_scheduler: Arc<dyn KnownPlatformPropertyProvider> =
        Arc::new(PlaceableActions::new(historical, fleets.clone(), admitted));
    Ok((action_scheduler, worker_scheduler))
}

/// Observes the router's calls into `sink`, and decodes Bazel's `RequestMetadata` header into the
/// request context so actions carry their target and mnemonic and the scheduler learns per target.
fn with_request_metadata(router: Router, sink: gang_spi::ObservationSink) -> Router {
    router
        .layer(BuildObserverLayer::new(sink))
        .layer(nativelink_util::telemetry::OtlpLayer::new(false))
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
    let routes = Routes::builder()
        .routes()
        .add_service(ac.into_service().max_decoding_message_size(limit))
        .add_service(cas.into_service().max_decoding_message_size(limit))
        .add_service(bytestream.into_service().max_decoding_message_size(limit));
    Ok(routes.into_axum_router())
}

fn client_leader_routes(
    services: &settings::ClientServices,
    shared: &Shared<'_>,
) -> Result<(Router, Router), NativeLinkEmbedError> {
    let limit = shared.max_decoding_message_size;
    let execution = ExecutionServer::new(&services.execution, shared.actions, shared.store_manager)
        .map_err(NativeLinkEmbedError::native("execution service"))?;
    let exec = execution
        .clone()
        .into_service()
        .max_decoding_message_size(limit);
    let ops = execution
        .into_operations_service()
        .max_decoding_message_size(limit);
    let standby = Routes::from(Router::new())
        .add_service(unavailable_like(&exec))
        .add_service(unavailable_like(&ops));
    let live = Routes::from(Router::new())
        .add_service(exec)
        .add_service(ops);
    Ok((live.into_axum_router(), standby.into_axum_router()))
}

fn worker_leader_routes(
    services: &settings::WorkerServices,
    shared: &Shared<'_>,
) -> Result<(Router, Router), NativeLinkEmbedError> {
    let limit = shared.max_decoding_message_size;
    let worker_api = WorkerApiServer::new(&services.worker_api, shared.workers)
        .map_err(NativeLinkEmbedError::native("worker api service"))?
        .into_service()
        .max_decoding_message_size(limit);
    let standby = Routes::from(Router::new()).add_service(unavailable_like(&worker_api));
    let live = Routes::from(Router::new()).add_service(worker_api);
    Ok((live.into_axum_router(), standby.into_axum_router()))
}

/// Registers the live service's name without retaining the service or its scheduler.
struct Unavailable<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Clone for Unavailable<T> {
    fn clone(&self) -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T: tonic::server::NamedService> tonic::server::NamedService for Unavailable<T> {
    const NAME: &'static str = T::NAME;
}

impl<T> Service<Request<tonic::body::Body>> for Unavailable<T> {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Request<tonic::body::Body>) -> Self::Future {
        std::future::ready(Ok(tonic::Status::unavailable("not leading").into_http()))
    }
}

fn unavailable_like<T: tonic::server::NamedService>(_: &T) -> Unavailable<T> {
    Unavailable(std::marker::PhantomData)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::error::Error;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD_NO_PAD;
    use futures::StreamExt;
    use gang_spi::{
        Admitted, ApiResponseBody, Authenticated, GateDecision, Peer, PortError, RenderedStoreSpec,
        RequestScope, full_body,
    };
    use gang_testkit::TestResult;
    use gang_types::{
        Bytes, FleetName, FleetProperties, Identity, Isa, Principal, ProviderName, Resources,
        property,
    };
    use http::StatusCode;
    use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        ActionResourceUsage, Reservation, ResourceOutcome, StartExecute, UpdateForWorker,
        update_for_worker,
    };
    use nativelink_scheduler::resource_sketch::{Observation, ResourceSketch, SketchKey};
    use nativelink_scheduler::worker::Worker;
    use nativelink_util::action_messages::{
        ActionInfo, ActionResult, ActionStage, ActionUniqueKey, ActionUniqueQualifier, OperationId,
        WorkerId,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::instant_wrapper::InstantWrapper;
    use nativelink_util::operation_state_manager::{OperationFilter, UpdateOperationType};
    use nativelink_util::origin_event::{BAZEL_METADATA_KEY, request_metadata_from_baggage};
    use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
    use opentelemetry::baggage::BaggageExt;
    use prost::Message;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    use super::*;
    use crate::gate::{GateState, admit};

    #[test]
    fn the_public_limit_is_configured_and_the_worker_limit_follows_the_fleets() {
        let tuning = Http2Config {
            max_connections: NonZeroUsize::MIN.saturating_add(6),
            ..Http2Config::default()
        };
        let limits = connection_limits(&tuning, 2);
        assert_eq!((limits.public.get(), limits.worker.get()), (7, 12));
        assert_eq!(connection_limits(&tuning, 0).worker, NonZeroUsize::MIN);
        assert_eq!(
            connection_limits(&tuning, u64::MAX).worker.get(),
            usize::MAX
        );
    }

    /// The deployment module's default `client_action_timeout` in
    /// `infra/terraform/modules/deployment/variables.tf`.
    const CLIENT_ACTION_TIMEOUT: Duration = Duration::from_mins(2);
    /// How long the connected client waits on the test clock.
    const WAIT: Duration = Duration::from_hours(48);
    /// Clock increments shorter than the client-action timeout.
    const STEP: Duration = Duration::from_mins(1);
    /// Scheduler turns that let the client's keepalive reach the action database on the
    /// current-thread runtime; these are not a wait on time.
    const KEEPALIVE_YIELDS: usize = 16;

    thread_local! {
        /// The stack's clock, moved only by the test on its current-thread runtime.
        static NOW: Cell<Duration> = const { Cell::new(Duration::from_secs(1_000_000)) };
    }

    /// Moves the stack's clock forward.
    fn advance(by: Duration) {
        NOW.set(NOW.get() + by);
    }

    /// An instant on the stack's test-controlled clock.
    #[derive(Debug, Clone, Copy)]
    struct TestInstant(Duration);

    impl InstantWrapper for TestInstant {
        fn from_secs(secs: u64) -> Self {
            Self(Duration::from_secs(secs))
        }

        fn unix_timestamp(&self) -> u64 {
            self.0.as_secs()
        }

        fn now(&self) -> SystemTime {
            UNIX_EPOCH + self.0
        }

        fn elapsed(&self) -> Duration {
            NOW.get().saturating_sub(self.0)
        }

        async fn sleep(self, duration: Duration) {
            while self.elapsed() < duration {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Returns the current test-controlled instant.
    fn test_now() -> TestInstant {
        TestInstant(NOW.get())
    }

    /// A fleet with the resources used by the queued-action test.
    fn fleet(name: &str, isa: Isa) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).expect("valid fleet name"),
            isa,
            &"docker",
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(100 << 30),
            },
            &gang_types::FleetActionPolicy::default(),
            Vec::<(&'static str, String)>::new(),
        )
    }

    /// Two fleets with incompatible instruction sets.
    fn x86_and_arm_fleets() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet("x86", Isa::X86_64), fleet("arm", Isa::Aarch64)])
            .expect("valid fleet catalog")
    }

    /// Scheduler handles used by the test-controlled stack.
    struct TestStack {
        /// The outer action scheduler.
        actions: Arc<dyn KnownPlatformPropertyProvider>,
        /// The worker scheduler.
        workers: Arc<dyn WorkerScheduler>,
        /// The simple scheduler, used to drive matching.
        simple: Arc<SimpleScheduler>,
        /// The sketch the historical layer learns from.
        sketch: Arc<ResourceSketch>,
    }

    /// The default scheduler configuration used by the test-controlled stack.
    fn default_config() -> NativeLinkConfig {
        NativeLinkConfig::default()
    }

    /// Builds the rendered stack on the test clock with the deployment's default client timeout.
    fn stack_on_test_clock(
        fleets: &FleetCatalog,
        config: &NativeLinkConfig,
    ) -> TestResult<TestStack> {
        Ok(stack_on_test_clock_with_matching_gate(fleets, config, None)?.0)
    }

    /// Optionally parks the background matcher after its first empty pass.
    fn stack_on_test_clock_with_matching_gate(
        fleets: &FleetCatalog,
        config: &NativeLinkConfig,
        ready: Option<mpsc::Sender<()>>,
    ) -> TestResult<(TestStack, Arc<tokio::sync::Notify>)> {
        stack_with_actions(
            fleets,
            config,
            ready,
            &Arc::new(gang_config::ActionsConfig::default()),
        )
    }

    fn stack_with_actions(
        fleets: &FleetCatalog,
        config: &NativeLinkConfig,
        ready: Option<mpsc::Sender<()>>,
        actions: &Arc<gang_config::ActionsConfig>,
    ) -> TestResult<(TestStack, Arc<tokio::sync::Notify>)> {
        stack_with_resources(fleets, config, ready, actions, &OnceLock::new())
    }

    fn stack_with_resources(
        fleets: &FleetCatalog,
        config: &NativeLinkConfig,
        ready: Option<mpsc::Sender<()>>,
        actions: &Arc<gang_config::ActionsConfig>,
        resources: &OnceLock<Arc<ResourceSketch>>,
    ) -> TestResult<(TestStack, Arc<tokio::sync::Notify>)> {
        let memory = RenderedStoreSpec(json!({ "memory": {} }));
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let rendered = settings::scheduler(
            config,
            &ControllerSpec {
                actions: Arc::clone(actions),
                scaling: Arc::new(gang_config::ScalingConfig::default()),
                clock: Arc::new(SystemClock::new()),
                content_store: &memory,
                action_store: &memory,
                fleets,
                max_workers: 8,
                observations: &observations,
                client_action_timeout: CLIENT_ACTION_TIMEOUT,
            },
        )?;
        let mut simple_slot = None;
        let mut sketch_slot = None;
        let mut notify_slot = None;
        let (actions, workers) = action_stack(
            &rendered,
            fleets,
            actions,
            resources,
            |simple, notify, sketch| {
                notify_slot = Some(Arc::clone(&notify));
                let db = memory_awaited_action_db_factory(
                    simple.retain_completed_for_s,
                    &notify,
                    test_now,
                );
                let (scheduler, workers) = SimpleScheduler::new_with_callback(
                    simple,
                    db,
                    move || {
                        let ready = ready.clone();
                        async move {
                            if let Some(ready) = ready {
                                ready
                                    .send(())
                                    .await
                                    .expect("matching gate receiver is alive");
                                std::future::pending::<()>().await;
                            }
                        }
                    },
                    notify,
                    test_now,
                    None,
                );
                simple_slot = Some(Arc::clone(&scheduler));
                sketch_slot = Some(Arc::clone(&sketch));
                (scheduler, workers)
            },
        )?;
        Ok((
            TestStack {
                actions,
                workers,
                simple: simple_slot.expect("simple scheduler was constructed"),
                sketch: sketch_slot.expect("resource sketch was provided"),
            },
            notify_slot.expect("matching notification was provided"),
        ))
    }

    /// Connects an Arm worker and keeps its update receiver alive for the caller.
    async fn connect_arm_worker(
        workers: &dyn WorkerScheduler,
        id: &WorkerId,
    ) -> TestResult<mpsc::Receiver<UpdateForWorker>> {
        connect_arm_worker_with_limit(workers, id, 1).await
    }

    /// Connects an Arm worker with an explicit concurrency limit.
    async fn connect_arm_worker_with_limit(
        workers: &dyn WorkerScheduler,
        id: &WorkerId,
        max_inflight_tasks: u64,
    ) -> TestResult<mpsc::Receiver<UpdateForWorker>> {
        connect_arm_worker_with_environment(workers, id, max_inflight_tasks, "host:test").await
    }

    async fn connect_arm_worker_with_environment(
        workers: &dyn WorkerScheduler,
        id: &WorkerId,
        max_inflight_tasks: u64,
        environment: &str,
    ) -> TestResult<mpsc::Receiver<UpdateForWorker>> {
        let (tx, rx) = mpsc::channel(64);
        let properties = PlatformProperties::new(HashMap::from([
            (
                property::FLEET.to_owned(),
                PlatformPropertyValue::Exact("arm".to_owned()),
            ),
            (
                property::ISA.to_owned(),
                PlatformPropertyValue::Exact(Isa::Aarch64.to_string()),
            ),
            (
                property::SANDBOX.to_owned(),
                PlatformPropertyValue::Exact("docker".to_owned()),
            ),
            (
                property::ACTION_ENVIRONMENT.to_owned(),
                PlatformPropertyValue::Exact(environment.to_owned()),
            ),
            (
                property::CPU_COUNT.to_owned(),
                PlatformPropertyValue::Minimum(4),
            ),
            (
                property::CPU_MILLICORES.to_owned(),
                PlatformPropertyValue::Minimum(4_000),
            ),
            (
                property::MEMORY_KB.to_owned(),
                PlatformPropertyValue::Minimum(8 * 1024 * 1024),
            ),
            (
                property::DISK_KB.to_owned(),
                PlatformPropertyValue::Minimum(100 * 1024 * 1024),
            ),
        ]));
        workers
            .add_worker(Worker::new(
                id.clone(),
                properties,
                tx,
                NOW.get().as_secs(),
                max_inflight_tasks,
            ))
            .await?;
        Ok(rx)
    }

    /// An action requiring the given instruction set and optional memory reservation.
    fn action(isa: Isa, memory_kb: Option<&str>) -> Arc<ActionInfo> {
        let digest = DigestInfo::new([1; 32], 1);
        let mut platform_properties = HashMap::from([
            (property::ISA.to_owned(), isa.to_string()),
            (
                property::ACTION_ENVIRONMENT.to_owned(),
                "host:test".to_owned(),
            ),
        ]);
        if let Some(memory_kb) = memory_kb {
            platform_properties.insert(property::MEMORY_KB.to_owned(), memory_kb.to_owned());
        }
        Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties,
            priority: 0,
            load_timestamp: test_now().now(),
            insert_timestamp: test_now().now(),
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest,
            }),
        })
    }

    /// An action accepted by the x86 fleet, but not by an Arm worker.
    fn x86_action() -> Arc<ActionInfo> {
        action(Isa::X86_64, None)
    }

    /// Reads the next worker dispatch without waiting on wall-clock time.
    async fn next_dispatch(rx: &mut mpsc::Receiver<UpdateForWorker>) -> (StartExecute, u64) {
        let mut last_message = None;
        for _ in 0..KEEPALIVE_YIELDS {
            match rx.try_recv() {
                Ok(message) => match message.update {
                    Some(update_for_worker::Update::StartAction(start)) => {
                        let memory_kb = start
                            .platform
                            .as_ref()
                            .and_then(|platform| {
                                platform
                                    .properties
                                    .iter()
                                    .find(|property| property.name == "memory_kb")
                                    .and_then(|property| property.value.parse().ok())
                            })
                            .expect("the dispatch carries memory_kb");
                        return (start, memory_kb);
                    }
                    update => last_message = Some(update),
                },
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    panic!("worker update channel closed before dispatch");
                }
            }
            tokio::task::yield_now().await;
        }
        panic!("expected a StartAction, last update: {last_message:?}");
    }

    /// Reads the current stage without driving the client's subscription.
    async fn stage_of(
        actions: &dyn KnownPlatformPropertyProvider,
        client: &OperationId,
    ) -> TestResult<Option<ActionStage>> {
        let mut results = actions
            .filter_operations(OperationFilter {
                client_operation_id: Some(client.clone()),
                ..Default::default()
            })
            .await?;
        match results.next().await {
            Some(result) => Ok(Some(result.as_state().await?.0.stage.clone())),
            None => Ok(None),
        }
    }

    async fn assert_available_cpu(stack: &TestStack, expected: &str) {
        let snapshot = stack.workers.worker_snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].running_actions, 1);
        assert_eq!(
            snapshot[0]
                .available_platform_properties
                .get(property::CPU_MILLICORES)
                .map(String::as_str),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn dispatch_ignores_worker_image_identity() -> TestResult {
        NOW.set(Duration::from_secs(1_000_000));
        let fleets = x86_and_arm_fleets();
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        let (stack, notify) =
            stack_on_test_clock_with_matching_gate(&fleets, &default_config(), Some(ready_tx))?;
        notify.notify_one();
        ready_rx
            .recv()
            .await
            .expect("background matcher reached the gate");
        let old_id = WorkerId("old-worker".to_owned());
        let mut old =
            connect_arm_worker_with_environment(stack.workers.as_ref(), &old_id, 1, "host:old")
                .await?;
        old.try_recv().expect("connection acknowledgement");
        let client = OperationId::from("identity-client");
        let _subscription = stack
            .actions
            .add_action(client.clone(), action(Isa::Aarch64, Some("252")))
            .await?;
        stack.simple.do_try_match_for_test().await?;
        let _ = next_dispatch(&mut old).await;
        assert_eq!(
            stage_of(stack.actions.as_ref(), &client).await?,
            Some(ActionStage::Executing)
        );
        Ok(())
    }

    /// A whole-CPU request must debit the same ledger as a millicore request.
    #[tokio::test]
    async fn whole_cpu_requests_reserve_millicores_until_completion() -> TestResult {
        NOW.set(Duration::from_secs(1_000_000));
        let fleets = x86_and_arm_fleets();
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        let (stack, notify) =
            stack_on_test_clock_with_matching_gate(&fleets, &default_config(), Some(ready_tx))?;
        notify.notify_one();
        ready_rx
            .recv()
            .await
            .expect("background matcher reached the gate");

        let worker_id = WorkerId("arm-worker".to_owned());
        let mut rx = connect_arm_worker_with_limit(stack.workers.as_ref(), &worker_id, 4).await?;
        assert!(matches!(
            rx.try_recv()
                .expect("worker connection acknowledgement")
                .update,
            Some(update_for_worker::Update::ConnectionResult(_))
        ));

        let mut first = action(Isa::Aarch64, None);
        Arc::get_mut(&mut first)
            .expect("action is uniquely owned")
            .platform_properties
            .insert(property::CPU_COUNT.to_owned(), "4".to_owned());
        let first_client = OperationId::from("whole-cpu-client");
        let _first_subscription = stack.actions.add_action(first_client, first).await?;
        stack.simple.do_try_match_for_test().await?;
        let first_start = match rx.try_recv().expect("whole-CPU action dispatch").update {
            Some(update_for_worker::Update::StartAction(start)) => start,
            update => panic!("expected StartAction, got {update:?}"),
        };
        assert_available_cpu(&stack, "0").await;

        let mut second = action(Isa::Aarch64, None);
        let second_info = Arc::get_mut(&mut second).expect("action is uniquely owned");
        let digest = DigestInfo::new([2; 32], 1);
        second_info.command_digest = digest;
        second_info.input_root_digest = digest;
        second_info.unique_qualifier = ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            execution_scope: None,
            instance_name: String::new(),
            digest_function: DigestHasherFunc::Sha256,
            digest,
        });
        second_info
            .platform_properties
            .insert(property::CPU_MILLICORES.to_owned(), "1000".to_owned());
        let second_client = OperationId::from("millicore-client");
        let _second_subscription = stack
            .actions
            .add_action(second_client.clone(), second)
            .await?;
        stack.simple.do_try_match_for_test().await?;
        assert_eq!(
            stage_of(stack.actions.as_ref(), &second_client).await?,
            Some(ActionStage::Queued)
        );
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_available_cpu(&stack, "0").await;

        let operation_id = OperationId::from(first_start.operation_id.as_str());
        stack
            .workers
            .update_action(
                &worker_id,
                &operation_id,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default(),
                )),
            )
            .await?;
        stack.simple.do_try_match_for_test().await?;
        assert!(matches!(
            rx.try_recv().expect("millicore action dispatch").update,
            Some(update_for_worker::Update::StartAction(_))
        ));
        assert_available_cpu(&stack, "3000").await;
        let properties = &first_start
            .platform
            .as_ref()
            .expect("dispatch platform")
            .properties;
        assert!(
            properties
                .iter()
                .any(|p| p.name == property::CPU_COUNT && p.value == "4")
        );
        assert!(
            properties
                .iter()
                .any(|p| p.name == property::CPU_MILLICORES && p.value == "4000"),
            "a whole-CPU request dispatches with its canonical millicore reservation"
        );
        Ok(())
    }

    /// A live subscription refreshes the client keepalive at the deployment's default timeout,
    /// so a placeable action stays queued even with no worker or only an incompatible worker.
    /// A Gang deadline derived from that timeout would fail this test. Once the subscription
    /// stops, `NativeLink` drops the action after the client-action timeout.
    #[tokio::test]
    async fn a_placeable_action_that_cannot_launch_stays_queued_while_its_client_waits()
    -> TestResult {
        for (case, arm_worker) in [("no worker", false), ("arm worker", true)] {
            NOW.set(Duration::from_secs(1_000_000));
            let fleets = x86_and_arm_fleets();
            let stack = stack_on_test_clock(&fleets, &default_config())?;
            let worker_id = WorkerId("arm-worker".to_owned());
            let _rx = if arm_worker {
                Some(connect_arm_worker(stack.workers.as_ref(), &worker_id).await?)
            } else {
                None
            };
            let client = OperationId::from("client");
            let mut result = stack
                .actions
                .add_action(client.clone(), x86_action())
                .await?;
            let handle = tokio::spawn(async move {
                while let Ok((state, _)) = result.changed().await {
                    if state.stage != ActionStage::Queued {
                        break;
                    }
                }
            });
            for minute in 1..=WAIT.as_secs() / STEP.as_secs() {
                advance(STEP);
                for _ in 0..KEEPALIVE_YIELDS {
                    tokio::task::yield_now().await;
                }
                if arm_worker {
                    stack
                        .workers
                        .worker_keep_alive_received(&worker_id, NOW.get().as_secs(), None)
                        .await?;
                }
                stack.simple.do_try_match_for_test().await?;
                assert_eq!(
                    stage_of(stack.actions.as_ref(), &client).await?,
                    Some(ActionStage::Queued),
                    "{case}, elapsed {minute} minutes"
                );
            }
            handle.abort();
            for _ in 0..(CLIENT_ACTION_TIMEOUT + STEP).as_secs() / STEP.as_secs() {
                advance(STEP);
                for _ in 0..KEEPALIVE_YIELDS {
                    tokio::task::yield_now().await;
                }
                if arm_worker {
                    stack
                        .workers
                        .worker_keep_alive_received(&worker_id, NOW.get().as_secs(), None)
                        .await?;
                }
                stack.simple.do_try_match_for_test().await?;
            }
            assert_ne!(
                stage_of(stack.actions.as_ref(), &client).await?,
                Some(ActionStage::Queued),
                "{case}, client gone"
            );
        }
        Ok(())
    }

    /// A request below the configured minimum escalates after a memory kill.
    async fn reservation_escalates(environment: &str) -> TestResult {
        NOW.set(Duration::from_secs(1_000_000));
        let config = default_config();
        let actions = Arc::new(serde_json::from_value(json!({
            "reservation_headroom_percent": 13,
            "memory": { "min_reservation": "128MiB", "escalation_percent": 175, "max_steps": 4 }
        }))?);
        let fleets = x86_and_arm_fleets();
        let stack = stack_with_actions(&fleets, &config, None, &actions)?.0;
        let worker_id = WorkerId("arm-worker".to_owned());
        let mut rx =
            connect_arm_worker_with_environment(stack.workers.as_ref(), &worker_id, 1, environment)
                .await?;
        let connected = rx
            .try_recv()
            .expect("the worker receives a connection result");
        assert!(matches!(
            connected.update,
            Some(update_for_worker::Update::ConnectionResult(_))
        ));
        let mut request = action(Isa::Aarch64, Some("252"));
        Arc::make_mut(&mut request).platform_properties.insert(
            property::ACTION_ENVIRONMENT.to_owned(),
            environment.to_owned(),
        );

        let _subscription = stack
            .actions
            .add_action(OperationId::from("client"), request)
            .await?;
        stack.simple.do_try_match_for_test().await?;
        let (start, memory_kb) = next_dispatch(&mut rx).await;
        assert_eq!(memory_kb, 131_072, "the minimum reaches the worker");
        let operation_id = OperationId::from(start.operation_id.as_str());
        stack
            .workers
            .record_action_resource_usage(
                &worker_id,
                &operation_id,
                ActionResourceUsage {
                    peak_memory_kb: memory_kb + memory_kb / 5,
                    sampled: true,
                    outcome: ResourceOutcome::KilledMemory.into(),
                    enforced: true,
                    reserved: Some(Reservation {
                        memory_kb,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        stack
            .workers
            .update_action(
                &worker_id,
                &operation_id,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(ActionResult {
                    exit_code: 9,
                    error: Some(make_err!(
                        Code::FailedPrecondition,
                        "action exceeded its memory reservation"
                    )),
                    ..Default::default()
                })),
            )
            .await?;
        for _ in 0..KEEPALIVE_YIELDS {
            tokio::task::yield_now().await;
        }
        stack.simple.do_try_match_for_test().await?;
        let (retry, memory_kb) = next_dispatch(&mut rx).await;
        assert_eq!(retry.operation_id, start.operation_id);
        assert_eq!(
            memory_kb, 229_376,
            "the configured multiplier applies to the raised reservation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reservation_escalates_on_launch_environment() -> TestResult {
        reservation_escalates("host:test").await
    }

    #[tokio::test]
    async fn reservation_escalates_on_live_old_environment() -> TestResult {
        reservation_escalates("host:old").await
    }

    /// A configured minimum raises a learned reservation.
    #[tokio::test]
    async fn the_configured_floor_raises_a_too_tight_learned_reservation() -> TestResult {
        NOW.set(Duration::from_secs(1_000_000));
        let config = NativeLinkConfig::default();
        let fleets = x86_and_arm_fleets();
        let actions = Arc::new(serde_json::from_value(json!({
            "memory": { "min_reservation": "96MiB" }
        }))?);
        let stack = stack_with_actions(&fleets, &config, None, &actions)?.0;
        let worker_id = WorkerId("arm-worker".to_owned());
        let mut rx = connect_arm_worker(stack.workers.as_ref(), &worker_id).await?;
        let connected = rx
            .try_recv()
            .expect("the worker receives a connection result");
        assert!(matches!(
            connected.update,
            Some(update_for_worker::Update::ConnectionResult(_))
        ));

        let action = action(Isa::Aarch64, None);
        let key = SketchKey::for_attempt(None, None, &action.command_digest.to_string());
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let observation = Observation {
            peak_memory_kb: 22_387,
            now_s,
            ..Default::default()
        };
        // A successful command's first sighting is probation; the second records one sample.
        assert!(!stack.sketch.observe(key.clone(), &observation));
        assert!(stack.sketch.observe(key.clone(), &observation));
        let entry = stack
            .sketch
            .lookup(&key, now_s)
            .expect("sample was learned");
        assert_eq!(stack.sketch.shape(&entry).memory_kb, 26_865);

        let _subscription = stack
            .actions
            .add_action(OperationId::from("client"), action)
            .await?;
        stack.simple.do_try_match_for_test().await?;
        let (_, memory_kb) = next_dispatch(&mut rx).await;
        assert_eq!(
            memory_kb, 98_304,
            "the learned reservation is raised to the configured small-action floor"
        );
        Ok(())
    }

    fn seed_controller_history(history: &Histories, now: gang_clock::Timestamp) {
        let now_s = now.as_unix_millis() / 1000;
        let key = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let observation = Observation {
            peak_memory_kb: 22_387,
            now_s,
            ..Default::default()
        };
        // Only anonymous command keys need probation. A named target records
        // immediately and also trains the mnemonic fallback.
        assert!(history.resource.observe(key.clone(), &observation));
        assert_eq!(
            history
                .resource
                .lookup(&key, now_s)
                .expect("target learned")
                .samples,
            1,
        );
        let mnemonic = SketchKey::for_attempt(None, Some("compile"), "");
        assert_eq!(
            history
                .resource
                .lookup(&mnemonic, now_s)
                .expect("mnemonic fallback learned")
                .samples,
            1,
        );
        history
            .durations
            .lock()
            .record("target", "compile", 800, now);
    }

    #[tokio::test]
    async fn controller_terms_retain_resource_and_duration_history() -> TestResult {
        let config = default_config();
        let fleets = x86_and_arm_fleets();
        let histories = Mutex::new(None);
        let resources = OnceLock::new();
        let shared = SharedServices::default();
        let (handles, _) = watch::channel(None);
        let store = RenderedStoreSpec(json!({"memory": {}}));
        let (observations, _receiver) =
            gang_spi::ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(SystemClock::new()),
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            max_workers: 8,
            observations: &observations,
            client_action_timeout: CLIENT_ACTION_TIMEOUT,
        };
        let mut previous = None;
        let mut expected = Vec::new();
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let now = gang_clock::Timestamp::from_unix_millis(now_s * 1000);
        for _ in 0..2 {
            let connections = ControllerConnections {
                public: gang_spi::Listening {
                    connections: futures::stream::empty().boxed(),
                    gate: Identify,
                },
                worker: gang_spi::Listening {
                    connections: futures::stream::empty().boxed(),
                    gate: Identify,
                },
                api: Echo,
            };
            let (ready, waiting) = ControllerReady::channel();
            run(
                &config,
                &spec,
                connections,
                ready,
                ControllerState {
                    handles: &handles,
                    histories: &histories,
                    resources: &resources,
                    shared: &shared,
                },
            )
            .await?;
            waiting.await?;
            assert!(handles.borrow().is_none(), "drain clears serving handles");
            let history = histories
                .lock()
                .clone()
                .expect("learned history is retained");
            assert!(Arc::ptr_eq(
                &history.resource,
                resources.get().expect("resource sketch was initialized"),
            ));
            if let Some(previous) = &previous {
                assert!(Arc::ptr_eq(previous, &history));
                let mut actual = Vec::new();
                history.snapshot(now, &mut actual)?;
                assert_eq!(actual, expected, "both learned models survive a term");
            } else {
                seed_controller_history(&history, now);
                history.snapshot(now, &mut expected)?;
                assert!(
                    expected
                        .iter()
                        .any(|record| { record.key.kind == gang_spi::RecordKind::ResourceHistory })
                );
                assert!(
                    expected
                        .iter()
                        .any(|record| { record.key.kind == gang_spi::RecordKind::DurationHistory })
                );
                previous = Some(history);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn learned_reservation_survives_a_new_scheduler_term() -> TestResult {
        NOW.set(Duration::from_secs(1_000_000));
        let config = default_config();
        let fleets = x86_and_arm_fleets();
        let actions = Arc::new(gang_config::ActionsConfig::default());
        let resources = OnceLock::new();
        let first = stack_with_resources(&fleets, &config, None, &actions, &resources)?.0;
        let action = action(Isa::Aarch64, None);
        let key = SketchKey::for_attempt(None, None, &action.command_digest.to_string());
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let observation = Observation {
            peak_memory_kb: 22_387,
            now_s,
            ..Default::default()
        };
        assert!(!first.sketch.observe(key.clone(), &observation));
        assert!(first.sketch.observe(key.clone(), &observation));
        drop(first);
        let second = stack_with_resources(&fleets, &config, None, &actions, &resources)?.0;
        let entry = second
            .sketch
            .lookup(&key, now_s)
            .expect("reservation survived");
        assert_eq!(second.sketch.shape(&entry).memory_kb, 26_865);
        Ok(())
    }

    #[tokio::test]
    async fn restored_history_shapes_queued_actions_in_the_real_stack() -> TestResult {
        async fn queued_reservations(
            stack: &TestStack,
            action: Arc<ActionInfo>,
        ) -> TestResult<Vec<Option<String>>> {
            let operation = OperationId::from("history-restart");
            let _subscription = stack.actions.add_action(operation.clone(), action).await?;
            let mut results = stack
                .actions
                .filter_operations(OperationFilter {
                    client_operation_id: Some(operation),
                    ..Default::default()
                })
                .await?;
            let queued = results
                .next()
                .await
                .expect("action remains queued without a worker");
            let (info, _) = queued.as_action_info().await?;
            Ok([
                property::MEMORY_KB,
                property::DISK_KB,
                property::CPU_MILLICORES,
            ]
            .iter()
            .map(|key| info.platform_properties.get(*key).cloned())
            .collect())
        }

        NOW.set(Duration::from_secs(1_000_000));
        let config = default_config();
        let fleets = x86_and_arm_fleets();
        let source = stack_on_test_clock(&fleets, &config)?;
        let request = action(Isa::Aarch64, None);
        let key = SketchKey::for_attempt(None, None, &request.command_digest.to_string());
        // HistoricalResourceScheduler reads the system wall clock; use the same clock for
        // the observation while keeping the scheduler's test clock deterministic.
        let now_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let observation = Observation {
            now_s,
            peak_memory_kb: 200_000,
            peak_disk_kb: 500_000,
            cpu_millicores: Some(1_200),
            ..Default::default()
        };
        assert!(!source.sketch.observe(key.clone(), &observation));
        assert!(source.sketch.observe(key, &observation));
        let durations = || {
            Arc::new(Mutex::new(ActionDurations::new(
                NonZeroUsize::MIN,
                gang_config::HistoryWindow::from_units(1),
            )))
        };
        let history = Histories::new(Arc::clone(&source.sketch), durations());
        let now = gang_clock::Timestamp::from_unix_millis(now_s * 1000);
        let mut records = Vec::new();
        history.snapshot(now, &mut records)?;
        assert!(!records.is_empty());

        let restored = stack_on_test_clock(&fleets, &config)?;
        Histories::new(Arc::clone(&restored.sketch), durations()).restore(now, &records)?;
        let cold = stack_on_test_clock(&fleets, &config)?;
        let learned = queued_reservations(&source, Arc::clone(&request)).await?;
        let restarted = queued_reservations(&restored, Arc::clone(&request)).await?;
        let cold_reservations = queued_reservations(&cold, request).await?;
        assert_eq!(restarted, learned);
        assert_ne!(
            restarted, cold_reservations,
            "restored admission must not use cold defaults"
        );
        assert!(restarted.iter().all(Option::is_some));
        Ok(())
    }

    /// Admits a fixed identity as the verified caller.
    #[derive(Clone)]
    struct Identify;

    impl RequestGate for Identify {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(Ok(GateDecision::Admit(Admitted::Authenticated(
                Authenticated {
                    identity: Arc::new(alice()),
                    valid_until: None,
                },
            ))))
        }
    }

    fn alice() -> Identity {
        Identity::new(
            ProviderName::try_new("local").expect("valid provider"),
            Principal::try_new("alice").expect("valid principal"),
        )
    }

    /// Answers every request the router does not route with its path and admitted identity.
    #[derive(Clone)]
    struct Echo;

    impl ApiService for Echo {
        fn call(
            &self,
            request: Request<ApiRequestBody>,
        ) -> impl Future<Output = Response<ApiResponseBody>> + Send {
            let identity = request.extensions().get::<Authenticated>().map_or_else(
                || "anonymous".to_owned(),
                |caller| caller.identity.to_string(),
            );
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
            .layer(from_fn_with_state(
                GateState {
                    gate: Identify,
                    scope: RequestScope::Shared,
                    force_not_leading: false,
                },
                admit::<Identify>,
            ));
        let response = router
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        Ok((status, String::from_utf8(body.to_vec())?))
    }

    fn capacity_test_fleets() -> FleetCatalog {
        FleetCatalog::try_new(
            [("fleet-a", 16, 8_u64), ("fleet-b", 4, 32)]
                .into_iter()
                .map(|(name, vcpus, memory_gib)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).expect("valid fleet name"),
                        Isa::X86_64,
                        &"docker",
                        &"host:test".parse().expect("valid environment"),
                        Resources {
                            vcpus,
                            memory: Bytes::new(memory_gib << 30),
                            disk: Bytes::new(100_u64 << 30),
                        },
                        &gang_types::FleetActionPolicy::default(),
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("valid fleet catalog")
    }

    /// The cold-start memory exceeds the only fleet eligible for the requested CPU. Fitting
    /// clamps memory to that fleet and reserves all its CPU.
    #[tokio::test]
    async fn scheduler_stack_clamps_to_eligible_capacity_and_reserves_whole_cpu() {
        let fleets = capacity_test_fleets();
        let config = default_config();
        let actions = Arc::new(
            serde_json::from_value(json!({
                "cold_start": { "memory": "30GiB" }
            }))
            .expect("valid action policy"),
        );
        let store = RenderedStoreSpec(json!({ "memory": {} }));
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::clone(&actions),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(SystemClock::new()),
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            max_workers: 8,
            observations: &observations,
            client_action_timeout: Duration::from_mins(10),
        };
        let mut scheduler = settings::scheduler(&config, &spec).expect("valid scheduler config");
        let SchedulerSpec::HistoricalResource(historical) = &mut scheduler.spec else {
            panic!("expected historical scheduler");
        };
        historical.learn_from_workers = false;
        let SchedulerStack { actions, .. } =
            scheduler_stack(&scheduler, &fleets, &actions, &OnceLock::new())
                .expect("scheduler stack");
        let operation = OperationId::String("stack-wiring".to_owned());
        let digest = DigestInfo::new([1; 32], 1);
        let info = Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: [
                (property::CPU_MILLICORES.to_owned(), "12000".to_owned()),
                (
                    property::ACTION_ENVIRONMENT.to_owned(),
                    "host:test".to_owned(),
                ),
            ]
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
            "16000",
            "memory clamped to fleet A's capacity reserves its whole CPU"
        );
        assert_eq!(
            info.platform_properties[property::MEMORY_KB],
            "8388608",
            "scheduler stack must fit after historical cold-start memory is applied"
        );
    }

    /// An action used to check the operation's origin.
    fn origin_test_action() -> Arc<ActionInfo> {
        let digest = DigestInfo::new([2; 32], 2);
        Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: HashMap::from([(
                property::ACTION_ENVIRONMENT.to_owned(),
                "host:test".to_owned(),
            )]),
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
    async fn operation_origin_identity_is_the_gates_principal() {
        let fleets =
            FleetCatalog::try_new(vec![fleet("fleet-a", Isa::X86_64)]).expect("valid catalog");
        let config = NativeLinkConfig::default();
        let store = RenderedStoreSpec(json!({ "memory": {} }));
        let (sink, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(SystemClock::new()),
            content_store: &store,
            action_store: &store,
            fleets: &fleets,
            max_workers: 8,
            observations: &sink,
            client_action_timeout: Duration::from_mins(10),
        };
        let mut scheduler = settings::scheduler(&config, &spec).expect("scheduler");
        let SchedulerSpec::HistoricalResource(historical) = &mut scheduler.spec else {
            panic!("historical scheduler")
        };
        historical.learn_from_workers = false;
        let SchedulerStack { actions, .. } =
            scheduler_stack(&scheduler, &fleets, &spec.actions, &OnceLock::new()).expect("stack");
        let info = origin_test_action();
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            ..Default::default()
        };
        let handler_actions = Arc::clone(&actions);
        let router = with_request_metadata(
            Router::new().route(
                "/build.bazel.remote.execution.v2.Execution/Execute",
                axum::routing::post(move || {
                    let actions = Arc::clone(&handler_actions);
                    let info = Arc::clone(&info);
                    async move {
                        actions
                            .add_action(OperationId::String("client".to_owned()), info)
                            .await
                            .expect("add");
                    }
                }),
            ),
            sink,
        )
        .layer(axum::Extension(gang_spi::Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        }));
        router
            .oneshot(
                Request::post("/build.bazel.remote.execution.v2.Execution/Execute")
                    .header(
                        "build.bazel.remote.execution.v2.requestmetadata-bin",
                        BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec()),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router");
        let mut all = actions
            .filter_operations(OperationFilter::default())
            .await
            .expect("list");
        let (state, _) = all
            .next()
            .await
            .expect("queued")
            .as_state()
            .await
            .expect("state");
        let mut found = actions
            .filter_operations(OperationFilter {
                operation_id: Some(state.client_operation_id.clone()),
                ..Default::default()
            })
            .await
            .expect("filter");
        let (_, origin) = found
            .next()
            .await
            .expect("operation")
            .as_action_info()
            .await
            .expect("info");
        let origin = origin.expect("origin");
        assert_eq!(origin.identity, alice().to_string());
        assert_eq!(origin.bazel_metadata, Some(metadata));
    }
    #[tokio::test]
    async fn deduplicated_operation_keeps_first_creator_source_after_creator_disconnects()
    -> TestResult {
        use opentelemetry::context::FutureExt as _;
        let fleets = FleetCatalog::try_new(vec![fleet("fleet-a", Isa::X86_64)])?;
        let stack = stack_on_test_clock(&fleets, &default_config())?;
        let action = origin_test_action();
        let context = |identity: &'static str| {
            opentelemetry::Context::new().with_baggage([opentelemetry::KeyValue::new(
                crate::observer::ENDUSER_ID,
                identity,
            )])
        };
        let mut creator = Some(
            stack
                .actions
                .add_action(
                    OperationId::String("creator-client-id".to_owned()),
                    Arc::clone(&action),
                )
                .with_context(context("local:alice"))
                .await?,
        );
        let joiner = stack
            .actions
            .add_action(OperationId::String("joiner-client-id".to_owned()), action)
            .with_context(context("local:bob"))
            .await?;
        let clock = Arc::new(gang_clock::TestClock::new(
            gang_clock::Timestamp::from_unix_millis(100_000),
        ));
        let rates = SourceRates::new(&gang_config::ScalingConfig::default(), fleets.len(), clock)?;
        for disconnected in [false, true] {
            let mut listing = stack
                .actions
                .filter_operations(OperationFilter::default())
                .await?;
            let row = listing.next().await.ok_or("retained operation")?;
            let (state, origin) = row.as_state().await?;
            assert_ne!(
                state.client_operation_id,
                OperationId::String("joiner-client-id".to_owned())
            );
            assert_eq!(
                rates.source(origin.as_ref()),
                gang_types::SourceId::from_identity_str("local:alice")
            );
            assert!(listing.next().await.is_none(), "joiner adds no operation");
            if !disconnected {
                // Dropping the client subscription must not substitute the remaining joiner's identity.
                let (_, origin) = joiner.as_action_info().await?;
                assert_eq!(
                    rates.source(origin.as_ref()),
                    gang_types::SourceId::from_identity_str("local:alice")
                );
                drop(creator.take());
            }
        }
        Ok(())
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
        let router = with_request_metadata(
            Router::new().route("/probe", axum::routing::post(handler)),
            gang_spi::ObservationSink::channel(
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::MIN,
            )
            .0,
        );
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

    fn observer_router(sink: gang_spi::ObservationSink) -> Router {
        let auth = gang_spi::Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        };
        with_request_metadata(
            Router::new().route(
                "/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
                axum::routing::post(|| async { ([("grpc-status", "5")], "") }),
            ),
            sink,
        )
        .layer(axum::Extension(auth))
    }

    #[tokio::test]
    async fn not_found_is_a_miss_and_ok_a_hit_for_the_callers_build() -> TestResult {
        let (sink, mut observations) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::new(4).expect("nonzero"),
            std::num::NonZeroUsize::MIN,
        );
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            target_id: "//pkg:lib".to_owned(),
            action_mnemonic: "Rustc".to_owned(),
            ..Default::default()
        };
        let header = BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec());
        let miss = observer_router(sink.clone());
        let auth = gang_spi::Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        };
        let hit = with_request_metadata(
            Router::new().route(
                "/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
                axum::routing::post(|| async { "" }),
            ),
            sink,
        )
        .layer(axum::Extension(auth));
        for router in [miss, hit] {
            router
                .oneshot(
                    Request::post("/build.bazel.remote.execution.v2.ActionCache/GetActionResult")
                        .header(
                            "build.bazel.remote.execution.v2.requestmetadata-bin",
                            &header,
                        )
                        .body(Body::empty())?,
                )
                .await?;
        }
        for outcome in [gang_spi::CallOutcome::NotFound, gang_spi::CallOutcome::Ok] {
            let Some(observation) = observations
                .next()
                .await
                .and_then(gang_spi::Delivery::into_observation)
            else {
                panic!("expected observation")
            };
            let gang_spi::Observation::Call(call) = observation else {
                panic!("expected call")
            };
            assert_eq!(call.outcome, outcome);
            assert_eq!(call.rpc, gang_spi::Rpc::GetActionResult);
            assert_eq!(*call.origin.build.principal, alice());
            assert_eq!(call.origin.build.invocation.as_str(), "invocation");
            assert_eq!(
                call.origin
                    .target
                    .as_ref()
                    .map(gang_types::TargetLabel::as_str),
                Some("//pkg:lib")
            );
            assert_eq!(
                call.origin
                    .mnemonic
                    .as_ref()
                    .map(gang_types::Mnemonic::as_str),
                Some("Rustc")
            );
        }
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 2,
                hits: 1,
                uploaded: 0,
                downloaded: 0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_gates_principal_replaces_a_client_enduser_id() -> TestResult {
        async fn handler() -> String {
            let context = opentelemetry::Context::current();
            let baggage = context.baggage();
            let identity = baggage
                .get(crate::observer::ENDUSER_ID)
                .map_or_else(String::new, |v| v.as_str().to_owned());
            let metadata = baggage
                .get(BAZEL_METADATA_KEY)
                .and_then(|value| request_metadata_from_baggage(value.as_str()).ok())
                .expect("request metadata reaches the handler");
            format!(
                "{identity} {} {} {}",
                metadata.tool_invocation_id, metadata.target_id, metadata.action_mnemonic
            )
        }
        nativelink_util::telemetry::init_propagator();
        let (sink, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let auth = gang_spi::Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        };
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            target_id: "//pkg:lib".to_owned(),
            action_mnemonic: "Rustc".to_owned(),
            ..Default::default()
        };
        let router = with_request_metadata(
            Router::new().route(
                "/build.bazel.remote.execution.v2.Execution/Execute",
                axum::routing::post(handler),
            ),
            sink,
        )
        .layer(axum::Extension(auth));
        let response = router
            .oneshot(
                Request::post("/build.bazel.remote.execution.v2.Execution/Execute")
                    .header("baggage", "enduser.id=mallory")
                    .header(
                        "build.bazel.remote.execution.v2.requestmetadata-bin",
                        BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec()),
                    )
                    .body(Body::empty())?,
            )
            .await?;
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        assert_eq!(
            body.as_ref(),
            format!("{} invocation //pkg:lib Rustc", alice()).as_bytes()
        );
        Ok(())
    }

    #[tokio::test]
    async fn only_successful_post_grpc_responses_are_hits() -> TestResult {
        let (sink, mut observations) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::new(4).expect("nonzero"),
            std::num::NonZeroUsize::MIN,
        );
        let path = "/build.bazel.remote.execution.v2.ActionCache/GetActionResult";
        let auth = gang_spi::Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        };
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            ..Default::default()
        };
        let header = BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec());
        let router = with_request_metadata(
            Router::new().route(
                path,
                axum::routing::get(|| async { "" })
                    .post(|| async { StatusCode::METHOD_NOT_ALLOWED }),
            ),
            sink.clone(),
        )
        .layer(axum::Extension(auth.clone()));
        for request in [Request::get(path), Request::post(path)] {
            router
                .clone()
                .oneshot(
                    request
                        .header(
                            "build.bazel.remote.execution.v2.requestmetadata-bin",
                            &header,
                        )
                        .body(Body::empty())?,
                )
                .await?;
        }
        let ok = with_request_metadata(
            Router::new().route(
                path,
                axum::routing::post(|| async { ([("grpc-status", "0")], "") }),
            ),
            sink,
        )
        .layer(axum::Extension(auth));
        ok.oneshot(
            Request::post(path)
                .header(
                    "build.bazel.remote.execution.v2.requestmetadata-bin",
                    &header,
                )
                .body(Body::empty())?,
        )
        .await?;
        drop(router);
        for expected in [gang_spi::CallOutcome::Error, gang_spi::CallOutcome::Ok] {
            let Some(observation) = observations
                .next()
                .await
                .and_then(gang_spi::Delivery::into_observation)
            else {
                panic!("expected observation")
            };
            let gang_spi::Observation::Call(call) = observation else {
                panic!("expected call")
            };
            assert_eq!(call.outcome, expected);
        }
        assert!(observations.next().await.is_none());
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 2,
                hits: 1,
                uploaded: 0,
                downloaded: 0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn anonymous_observed_calls_strip_client_identity() -> TestResult {
        async fn handler() -> String {
            let context = opentelemetry::Context::current();
            let baggage = context.baggage();
            format!(
                "{} {}",
                baggage
                    .get(crate::observer::ENDUSER_ID)
                    .map_or("none", |v| v.as_str()),
                baggage.get("other").map_or("none", |v| v.as_str())
            )
        }
        nativelink_util::telemetry::init_propagator();
        let (sink, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let path = "/build.bazel.remote.execution.v2.Execution/Execute";
        let router = with_request_metadata(
            Router::new().route(path, axum::routing::post(handler)),
            sink,
        );
        let response = router
            .oneshot(
                Request::post(path)
                    .header("baggage", "enduser.id=mallory,other=kept")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await?
                .as_ref(),
            b"none kept"
        );
        Ok(())
    }

    const GET_ACTION_RESULT: &str = "/build.bazel.remote.execution.v2.ActionCache/GetActionResult";
    const BYTE_STREAM_READ: &str = "/google.bytestream.ByteStream/Read";
    const EXECUTE: &str = "/build.bazel.remote.execution.v2.Execution/Execute";
    /// The gRPC header carrying Bazel's `RequestMetadata`.
    const METADATA_HEADER: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

    /// One frame of a test body.
    type TestFrame = Result<hyper::body::Frame<bytes::Bytes>, Infallible>;
    type FrameSender = futures::channel::mpsc::UnboundedSender<TestFrame>;

    /// A body yielding the frames sent on the returned sender; it ends when the sender is dropped.
    fn streamed() -> (FrameSender, Body) {
        let (frames, stream) = futures::channel::mpsc::unbounded();
        (frames, Body::new(http_body_util::StreamBody::new(stream)))
    }

    /// Sends one data frame holding `data`.
    fn send_data(frames: &FrameSender, data: &'static [u8]) -> TestResult {
        frames.unbounded_send(Ok(hyper::body::Frame::data(bytes::Bytes::from_static(
            data,
        ))))?;
        Ok(())
    }

    /// Sends the trailers ending a gRPC response with `status`.
    fn send_status(frames: &FrameSender, status: &'static str) -> TestResult {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static(status));
        frames.unbounded_send(Ok(hyper::body::Frame::trailers(trailers)))?;
        Ok(())
    }

    /// Observes POST `path` for alice with a handler that reads the whole request and then
    /// answers with `response`.
    fn streaming_router(path: &str, sink: gang_spi::ObservationSink, response: Body) -> Router {
        let response = Arc::new(parking_lot::Mutex::new(Some(response)));
        with_request_metadata(
            Router::new().route(
                path,
                axum::routing::post(move |request: Body| {
                    let response = response.lock().take().unwrap_or_default();
                    async move {
                        axum::body::to_bytes(request, usize::MAX)
                            .await
                            .expect("the request is read");
                        Response::new(response)
                    }
                }),
            ),
            sink,
        )
        .layer(axum::Extension(Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        }))
    }

    /// A POST to `path` carrying `body` and the metadata of the build `invocation`.
    fn attributed(path: &str, body: Body) -> Result<Request<Body>, http::Error> {
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            ..Default::default()
        };
        Request::post(path)
            .header(
                METADATA_HEADER,
                BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec()),
            )
            .body(body)
    }

    fn observation_channel() -> (gang_spi::ObservationSink, gang_spi::Observations) {
        gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::new(4).expect("nonzero"),
            std::num::NonZeroUsize::MIN,
        )
    }

    /// The next observation, which must be a call.
    async fn next_call(observations: &mut gang_spi::Observations) -> gang_spi::Call {
        match observations
            .next()
            .await
            .and_then(gang_spi::Delivery::into_observation)
        {
            Some(gang_spi::Observation::Call(call)) => call,
            other => panic!("expected a call, got {other:?}"),
        }
    }

    /// Whether no observation is ready to read.
    fn nothing_recorded(observations: &mut gang_spi::Observations) -> bool {
        !matches!(
            futures::FutureExt::now_or_never(observations.next()),
            Some(Some(_))
        )
    }

    /// U1. Simulated flaws: counting only the first frame; recording at the response head.
    #[tokio::test]
    async fn a_byte_stream_call_counts_every_frame_and_records_once_at_its_end() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (upload, request) = streamed();
        send_data(&upload, b"abc")?;
        send_data(&upload, b"defg")?;
        drop(upload);
        let (download, response) = streamed();
        let mut body = streaming_router(BYTE_STREAM_READ, sink, response)
            .oneshot(attributed(BYTE_STREAM_READ, request)?)
            .await?
            .into_body();
        send_data(&download, b"12345")?;
        assert!(body.frame().await.transpose()?.is_some());
        assert!(nothing_recorded(&mut observations));
        assert_eq!(observations.traffic(), gang_spi::Traffic::default());

        send_data(&download, b"678")?;
        send_status(&download, "0")?;
        drop(download);
        while body.frame().await.transpose()?.is_some() {}
        let call = next_call(&mut observations).await;
        assert_eq!(call.rpc, gang_spi::Rpc::Cas);
        assert_eq!(call.outcome, gang_spi::CallOutcome::Ok);
        assert_eq!(
            call.transferred,
            gang_spi::Transferred {
                uploaded: 7,
                downloaded: 8,
            }
        );
        drop(body);
        assert!(nothing_recorded(&mut observations));
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 0,
                hits: 0,
                uploaded: 7,
                downloaded: 8,
            }
        );
        Ok(())
    }

    /// U2. Simulated flaw: recording only at the end of the stream, so a dropped call is lost.
    #[tokio::test]
    async fn a_response_dropped_mid_stream_records_what_crossed_once() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (download, response) = streamed();
        let mut body = streaming_router(BYTE_STREAM_READ, sink, response)
            .oneshot(attributed(BYTE_STREAM_READ, Body::from("abc"))?)
            .await?
            .into_body();
        send_data(&download, b"12345")?;
        assert!(body.frame().await.transpose()?.is_some());
        drop(body);
        let call = next_call(&mut observations).await;
        assert_eq!(call.outcome, gang_spi::CallOutcome::Error);
        assert_eq!(
            call.transferred,
            gang_spi::Transferred {
                uploaded: 3,
                downloaded: 5,
            }
        );
        assert!(nothing_recorded(&mut observations));
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 0,
                hits: 0,
                uploaded: 3,
                downloaded: 5,
            }
        );
        Ok(())
    }

    /// U3. Simulated flaw: classifying the call from the response head only.
    #[tokio::test]
    async fn a_trailer_status_overrides_the_head_of_a_lookup() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (download, response) = streamed();
        let body = streaming_router(GET_ACTION_RESULT, sink, response)
            .oneshot(attributed(GET_ACTION_RESULT, Body::empty())?)
            .await?
            .into_body();
        send_status(&download, "5")?;
        drop(download);
        axum::body::to_bytes(body, usize::MAX).await?;
        let call = next_call(&mut observations).await;
        assert_eq!(call.rpc, gang_spi::Rpc::GetActionResult);
        assert_eq!(call.outcome, gang_spi::CallOutcome::NotFound);
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 1,
                hits: 0,
                uploaded: 0,
                downloaded: 0,
            }
        );
        Ok(())
    }

    /// Simulated flaw: a cancelled call keeps the outcome of its OK head and counts a hit.
    #[tokio::test]
    async fn a_lookup_cancelled_before_its_end_is_an_error_without_a_hit() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (download, response) = streamed();
        let mut body = streaming_router(GET_ACTION_RESULT, sink, response)
            .oneshot(attributed(GET_ACTION_RESULT, Body::empty())?)
            .await?
            .into_body();
        send_data(&download, b"12")?;
        assert!(body.frame().await.transpose()?.is_some());
        drop(body);
        let call = next_call(&mut observations).await;
        assert_eq!(call.outcome, gang_spi::CallOutcome::Error);
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 1,
                hits: 0,
                uploaded: 0,
                downloaded: 2,
            }
        );
        Ok(())
    }

    /// U4. Simulated flaw: wrapping `Execute` like a cache call, which delays its record until
    /// the action's stream ends and counts its bytes.
    #[tokio::test]
    async fn execute_is_recorded_at_its_head_while_its_stream_runs() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (download, response) = streamed();
        let mut body = streaming_router(EXECUTE, sink, response)
            .oneshot(attributed(EXECUTE, Body::from("abc"))?)
            .await?
            .into_body();
        let Some(Some(delivery)) = futures::FutureExt::now_or_never(observations.next()) else {
            panic!("Execute is recorded at its response head")
        };
        let Some(gang_spi::Observation::Call(call)) = delivery.into_observation() else {
            panic!("expected a call")
        };
        assert_eq!(call.rpc, gang_spi::Rpc::Execute);
        assert_eq!(call.outcome, gang_spi::CallOutcome::Ok);
        assert_eq!(call.transferred, gang_spi::Transferred::default());
        send_data(&download, b"12345")?;
        assert!(body.frame().await.transpose()?.is_some());
        drop(body);
        assert!(nothing_recorded(&mut observations));
        assert_eq!(observations.traffic(), gang_spi::Traffic::default());
        Ok(())
    }

    /// U5. Simulated flaw: counting farm traffic only for attributed calls.
    #[tokio::test]
    async fn calls_without_build_metadata_count_traffic_and_record_no_call() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let router = with_request_metadata(
            Router::new().route(
                GET_ACTION_RESULT,
                axum::routing::post(|request: Body| async move {
                    axum::body::to_bytes(request, usize::MAX)
                        .await
                        .expect("the request is read");
                    ([("grpc-status", "0")], "wxyz")
                }),
            ),
            sink,
        )
        .layer(axum::Extension(Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        }));
        let empty = BASE64_STANDARD_NO_PAD.encode(RequestMetadata::default().encode_to_vec());
        for header in [None, Some(empty)] {
            let mut request = Request::post(GET_ACTION_RESULT);
            if let Some(header) = header {
                request = request.header(METADATA_HEADER, header);
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::from("abc"))?)
                .await?;
            axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        }
        drop(router);
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 2,
                hits: 2,
                uploaded: 6,
                downloaded: 8,
            }
        );
        assert!(observations.next().await.is_none());
        Ok(())
    }

    const BYTE_STREAM_WRITE: &str = "/google.bytestream.ByteStream/Write";

    /// Observes POST `path` for alice with a handler that reads `frames` request frames, reports
    /// that on `reading`, and then never answers.
    fn stalled_router(
        path: &str,
        sink: gang_spi::ObservationSink,
        frames: usize,
        reading: tokio::sync::oneshot::Sender<()>,
    ) -> Router {
        let reading = Arc::new(parking_lot::Mutex::new(Some(reading)));
        with_request_metadata(
            Router::new().route(
                path,
                axum::routing::post(move |mut request: Body| {
                    let reading = reading.lock().take();
                    async move {
                        for _ in 0..frames {
                            request
                                .frame()
                                .await
                                .expect("a request frame")
                                .expect("a readable frame");
                        }
                        reading
                            .expect("one request")
                            .send(())
                            .expect("the test waits");
                        std::future::pending::<Response<Body>>().await
                    }
                }),
            ),
            sink,
        )
        .layer(axum::Extension(Authenticated {
            identity: Arc::new(alice()),
            valid_until: None,
        }))
    }

    /// Polls `call` until its handler reports `read`, then cancels it before its response head.
    async fn cancel_before_head<F: Future>(
        call: F,
        read: tokio::sync::oneshot::Receiver<()>,
    ) -> TestResult {
        tokio::pin!(call);
        tokio::select! {
            biased;
            _ = &mut call => panic!("the stalled call has no response head"),
            read = read => read?,
        }
        Ok(())
    }

    /// Simulated flaw: creating the call's record only once its response head arrives, so an
    /// upload cancelled before the head is lost.
    #[tokio::test]
    async fn an_upload_cancelled_before_its_head_records_what_crossed() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (upload, request) = streamed();
        send_data(&upload, b"abc")?;
        send_data(&upload, b"defg")?;
        let (reading, read) = tokio::sync::oneshot::channel();
        let call = stalled_router(BYTE_STREAM_WRITE, sink, 2, reading)
            .oneshot(attributed(BYTE_STREAM_WRITE, request)?);
        cancel_before_head(call, read).await?;
        let call = next_call(&mut observations).await;
        assert_eq!(call.rpc, gang_spi::Rpc::Cas);
        assert_eq!(call.outcome, gang_spi::CallOutcome::Error);
        assert_eq!(
            call.transferred,
            gang_spi::Transferred {
                uploaded: 7,
                downloaded: 0,
            }
        );
        assert!(nothing_recorded(&mut observations));
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 0,
                hits: 0,
                uploaded: 7,
                downloaded: 0,
            }
        );
        drop(upload);
        Ok(())
    }

    /// Simulated flaw: a lookup cancelled before its head is neither recorded nor counted.
    #[tokio::test]
    async fn a_lookup_cancelled_before_its_head_is_an_error_without_a_hit() -> TestResult {
        let (sink, mut observations) = observation_channel();
        let (reading, read) = tokio::sync::oneshot::channel();
        let call = stalled_router(GET_ACTION_RESULT, sink, 0, reading)
            .oneshot(attributed(GET_ACTION_RESULT, Body::empty())?);
        cancel_before_head(call, read).await?;
        let call = next_call(&mut observations).await;
        assert_eq!(call.rpc, gang_spi::Rpc::GetActionResult);
        assert_eq!(call.outcome, gang_spi::CallOutcome::Error);
        assert!(nothing_recorded(&mut observations));
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 1,
                hits: 0,
                uploaded: 0,
                downloaded: 0,
            }
        );
        Ok(())
    }

    /// Simulated flaw: an inner service error skips the farm count.
    #[tokio::test]
    async fn a_lookup_failing_before_its_head_counts_a_lookup_without_a_hit() -> TestResult {
        let (sink, observations) = observation_channel();
        let failing =
            service_fn(|_: Request<Body>| async { Err::<Response<Body>, _>("the service failed") });
        let result = tower::Layer::layer(&BuildObserverLayer::new(sink), failing)
            .oneshot(Request::post(GET_ACTION_RESULT).body(Body::empty())?)
            .await;
        assert!(result.is_err());
        assert_eq!(
            observations.traffic(),
            gang_spi::Traffic {
                lookups: 1,
                hits: 0,
                uploaded: 0,
                downloaded: 0,
            }
        );
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
