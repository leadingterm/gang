//! Inherent native execution [`Engine`] controller and worker entrypoints (`§6.1`, `§6.2`).
//!
//! [`Engine`] wires together the single-owner [`Scheduler`] actor (`controller` + `actor`),
//! REAPI v2 and `leadingterm.gang.worker.v1` gRPC services, the `serve` HTTP/1.1 + HTTP/2
//! connection loop with `RequestGate` admission, `BuildObserverLayer`, `HandOff`, term-spanning
//! [`LearnedSlot`] persistence, and the native [`run_worker`] runtime with `gang-tls` dialing.

use std::convert::Infallible;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use gang_clock::{Clock, SystemClock, Tick, Timestamp};
use gang_config::{ActionsConfig, OpenFileLimit, ScalingConfig};
use gang_scheduler::{Learned, PolicyError, RatesError, Scheduler, SchedulerPolicy};
use gang_spi::{
    ActionStore, ActionsView, ApiRequestBody, ApiService, BoxedError, ContentStore,
    ControllerConnections, ControllerDrain, ControllerEndpoints, ControllerReady, ControllerSpec,
    ControllerStores, EngineStore, FleetSupply, LearnedHistory, ListingLimits, ObservationSink,
    PortError, RequestGate, SchedulerPort, SchedulerSnapshot, WorkerSpec, WorkerStores, WorkerTls,
};
use gang_tls::{Alpn, ClientIdentity, ConnectorSettings, Dialer, ServerTrust, TlsConnector};
use gang_types::{
    CertificatePem, FleetCatalog, FleetName, InstanceId, NonZeroDuration, PrivateKeyPem,
    SaturatingInto, Url,
};
use http::{Request, Response};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::service::Routes;
use tonic::transport::Endpoint;
use tower::{Service, service_fn};

use crate::actor::{ActorConfig, ControllerRuntime};
use crate::config::{EngineConfig, EngineConfigError};
use crate::controller::{ControllerHandle, LearnedSlot, spawn_controller};
use crate::handoff::HandOff;
use crate::observer::BuildObserverLayer;
use crate::reapi_ac::ActionCacheService;
use crate::reapi_bytestream::{ByteStreamConfig, ByteStreamService};
use crate::reapi_capabilities::{CapabilitiesConfig, CapabilitiesService};
use crate::reapi_cas::{CasConfig, CasService, InflightBlobBudget};
use crate::reapi_execution::{ExecutionService, ExecutionServiceConfig};
use crate::serve::{Listener, ScopedRoutes, ServeShutdown, serve};
use crate::worker::{DEFAULT_WORKER_MAX_MESSAGE_SIZE, WorkerRuntimeSpec};
use crate::worker_service::{WorkerGrpcService, WorkerServiceConfig};
use crate::worker_session::{ConnectError, ConnectedChannel, SessionConnector};

/// Overlapping worker generations per physical instance (e.g. during certificate renewal).
const WORKER_GENERATIONS: u64 = 2;

/// Maximum concurrent connections one worker opens to the controller's worker listener.
const WORKER_CONNECTIONS: u64 = 3;

/// Timeout waiting for an HTTP/2 keepalive PING acknowledgement on worker connections.
const WORKER_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Poll interval used when waiting for draining workers to finish after listener streams end.
const HANDOFF_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Inputs required to run a controller term on [`Engine`], mirroring `gang_spi::ControllerSpec`
/// with typed [`ControllerStores`] (`§3.1`, `§6.1`).
#[derive(Clone)]
pub struct EngineControllerSpec<'a, C, A> {
    /// Shared action scheduling and enforcement policy.
    pub actions: Arc<ActionsConfig>,
    /// Shared rate estimation and scaling policy.
    pub scaling: Arc<ScalingConfig>,
    /// Injected controller clock shared with timers, observations, and snapshots.
    pub clock: Arc<dyn Clock>,
    /// Bound on control-plane enqueueing before returning [`PortError::Unavailable`].
    pub control_acquire_timeout: NonZeroDuration,
    /// Typed content and action stores served by the controller.
    pub stores: ControllerStores<C, A>,
    /// Configured fleets in preference order.
    pub fleets: &'a FleetCatalog,
    /// Maximum aggregate workers across all configured fleets.
    pub max_workers: u64,
    /// Timeout before a zero-watcher operation is dropped or cancelled.
    pub client_action_timeout: Duration,
    /// Maximum elapsed time for one attempt, including input and output transfers.
    pub attempt_budget: Duration,
    /// Build and traffic observation sink.
    pub observations: &'a ObservationSink,
}

impl<C, A> std::fmt::Debug for EngineControllerSpec<'_, C, A>
where
    C: std::fmt::Debug,
    A: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngineControllerSpec")
            .field("actions", &self.actions)
            .field("scaling", &self.scaling)
            .field("control_acquire_timeout", &self.control_acquire_timeout)
            .field("stores", &self.stores)
            .field("fleets", &self.fleets)
            .field("max_workers", &self.max_workers)
            .field("client_action_timeout", &self.client_action_timeout)
            .field("attempt_budget", &self.attempt_budget)
            .field("observations", &self.observations)
            .finish_non_exhaustive()
    }
}

/// Errors produced directly by [`Engine`] construction, controller setup, or worker TLS dialing.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum EngineError {
    /// Invalid engine configuration or endpoint mismatch.
    #[error(transparent)]
    #[failure(config)]
    Config(#[from] EngineConfigError),
    /// A controller term is already active on this [`Engine`].
    #[error("the controller is already serving")]
    #[failure(config)]
    AlreadyServing,
    /// The controller is not currently serving.
    #[error("the controller is not serving")]
    NotServing,
    /// Scheduler policy or fleet catalog failed validation.
    #[error("invalid scheduler policy: {0}")]
    #[failure(config)]
    Policy(#[from] PolicyError),
    /// Scaling rate table sizing exceeded memory bounds.
    #[error("invalid scaling rate configuration: {0}")]
    #[failure(config)]
    Rates(#[from] RatesError),
    /// A worker TLS certificate or key file could not be read.
    #[error("cannot read the worker tls file {}", path.display())]
    #[failure(config)]
    WorkerTlsFile {
        /// Path to the unreadable TLS file.
        path: PathBuf,
        /// Underlying OS I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A worker TLS file was not valid PEM of its expected kind.
    #[error("the worker tls file {} is malformed", path.display())]
    #[failure(config)]
    WorkerTlsPem {
        /// Path to the malformed PEM file.
        path: PathBuf,
        /// PEM parsing error.
        #[source]
        source: gang_types::PemError,
    },
    /// The worker TLS client connector could not be constructed.
    #[error("the worker tls client cannot be built")]
    #[failure(config)]
    WorkerTls(#[source] gang_tls::TransportError),
    /// Worker controller endpoint URI could not be converted for `tonic`.
    #[error("invalid worker endpoint URI: {0}")]
    #[failure(config)]
    EndpointUri(String),
    /// Reading OS entropy for per-term `attempt_seed` failed.
    #[error("failed to read OS entropy: {0}")]
    Entropy(String),
}

/// `gang-tls`-backed [`SessionConnector`] dialing a controller's worker endpoint (`§6.2`).
#[derive(Clone)]
pub struct TlsSessionConnector {
    endpoint: Endpoint,
    dialer: Dialer,
    has_tls: bool,
}

impl std::fmt::Debug for TlsSessionConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsSessionConnector")
            .field("uri", &self.endpoint.uri())
            .field("has_tls", &self.has_tls)
            .finish_non_exhaustive()
    }
}

impl TlsSessionConnector {
    /// Creates a connector from a configured `tonic` [`Endpoint`] and `gang-tls` [`Dialer`].
    #[must_use]
    pub const fn new(endpoint: Endpoint, dialer: Dialer, has_tls: bool) -> Self {
        Self {
            endpoint,
            dialer,
            has_tls,
        }
    }

    /// Builds a [`TlsSessionConnector`] from `worker_endpoint`, [`WorkerTls`], and connection
    /// timeouts.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] if the endpoint scheme mismatches `tls`, lacks a host, or any TLS
    /// file cannot be read or parsed.
    pub fn from_tls(
        worker_endpoint: &Url,
        tls: WorkerTls<'_>,
        connect_timeout: NonZeroDuration,
        keepalive_interval: Duration,
    ) -> Result<Self, EngineError> {
        if tls.scheme() != worker_endpoint.scheme() {
            return Err(EngineError::Config(EngineConfigError::WorkerEndpointTls {
                expected: tls.scheme(),
            }));
        }
        let has_tls = !matches!(tls, WorkerTls::Plain);
        let dialer = build_dialer(tls, connect_timeout)?.with_tcp_keepalive(keepalive_interval);
        let endpoint = build_controller_endpoint(
            worker_endpoint,
            has_tls,
            connect_timeout.get(),
            keepalive_interval,
        )?;
        Ok(Self::new(endpoint, dialer, has_tls))
    }
}

impl SessionConnector for TlsSessionConnector {
    async fn connect(&mut self) -> Result<ConnectedChannel, ConnectError> {
        let channel = self
            .endpoint
            .clone()
            .connect_with_connector(self.dialer.clone())
            .await
            .map_err(|err| ConnectError::from_transport(Box::new(err)))?;
        let revoked = self.has_tls.then(|| self.dialer.revoked().clone());
        Ok(ConnectedChannel { channel, revoked })
    }
}

type StoreUsageFn = Arc<dyn Fn(&mut Vec<EngineStore>) + Send + Sync>;

#[derive(Clone)]
struct LiveController {
    handle: ControllerHandle,
    handoff: HandOff,
    store_usage: StoreUsageFn,
}

/// Native execution engine coordinating controller terms, gRPC/HTTP serving, and worker sessions
/// (`§6.1`, `§6.2`).
pub struct Engine {
    config: EngineConfig,
    worker_endpoint: Url,
    learned: LearnedSlot,
    live: Arc<Mutex<Option<LiveController>>>,
    active: Arc<AtomicBool>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("worker_endpoint", &self.worker_endpoint)
            .field("learned", &self.learned)
            .field("active", &self.active.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// RAII guard releasing the single-controller-term claim, clearing live state, and aborting an
/// unjoined actor task if [`Engine::run_controller`] exits early or its future is dropped.
struct ControllerTermGuard {
    live: Arc<Mutex<Option<LiveController>>>,
    actor_task: Option<JoinHandle<()>>,
}

impl ControllerTermGuard {
    fn finish_actor(&mut self) -> Option<JoinHandle<()>> {
        *self.live.lock() = None;
        self.actor_task.take()
    }
}

impl Drop for ControllerTermGuard {
    fn drop(&mut self) {
        *self.live.lock() = None;
        if let Some(task) = self.actor_task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConnectionLimits {
    public: NonZeroUsize,
    worker: NonZeroUsize,
}

fn compute_connection_limits(
    max_public_connections: NonZeroUsize,
    max_workers: u64,
) -> ConnectionLimits {
    let worker_limit: usize = max_workers
        .saturating_mul(WORKER_GENERATIONS)
        .saturating_mul(WORKER_CONNECTIONS)
        .saturating_into();
    ConnectionLimits {
        public: max_public_connections,
        worker: NonZeroUsize::new(worker_limit).unwrap_or(NonZeroUsize::MIN),
    }
}

impl Engine {
    /// Validates `config` and constructs an [`Engine`] without starting a controller or worker.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Config`] if `worker_endpoint` is absent, lacks a host, or uses a
    /// scheme other than `grpc` / `grpcs`.
    pub fn try_new(config: EngineConfig) -> Result<Self, PortError> {
        let worker_endpoint = config.resolve_worker_endpoint().map_err(PortError::from)?;
        Ok(Self {
            config,
            worker_endpoint,
            learned: LearnedSlot::empty(),
            live: Arc::new(Mutex::new(None)),
            active: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Returns the dialable worker listener endpoint configured for this engine.
    #[must_use]
    pub fn endpoints(&self) -> ControllerEndpoints {
        ControllerEndpoints {
            worker: self.worker_endpoint.clone(),
        }
    }

    /// Returns the synchronous maximum encoded byte size of one [`LearnedHistory`] record (`§6.1`).
    ///
    /// Available from controller readiness through drain and after [`Self::run_controller`] exits.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] before the controller initializes learned state.
    pub fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        self.learned.learned_history_byte_limit()
    }

    /// Snapshots live or post-exit retained learned histories into `into`, reusing its buffer.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained.
    pub async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        self.learned.learned_history(now, into).await
    }

    /// Restores compatible learned histories into live or retained learned state.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained, or
    /// [`PortError::Backend`] if restoration fails.
    pub async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        self.learned.restore_learned_history(now, records).await
    }

    /// Reads the connected workers, queued/executing demand, and store usage into `into`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] when the controller is not serving or control acquisition
    /// times out.
    pub async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        let live = self.live_controller()?;
        live.handle.snapshot(into).await?;
        (live.store_usage)(&mut into.stores);
        Ok(())
    }

    /// Lists running and queued actions bounded by `limits` into `into`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] when the controller is not serving or control acquisition
    /// times out.
    pub async fn actions(
        &self,
        limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        let live = self.live_controller()?;
        live.handle.actions(limits, into).await
    }

    /// Sets or clears persistent physical drain intent for `(fleet, instance)`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] when the controller is not serving or control acquisition
    /// times out, or [`PortError::Backend`] if `fleet` is unknown.
    pub async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError> {
        let live = self.live_controller()?;
        live.handle.set_draining(fleet, instance, draining).await
    }

    /// Retains physical drain intents seen in `live`, allowing `grace` of absence.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] when the controller is not serving or control acquisition
    /// times out.
    pub async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError> {
        let controller = self.live_controller()?;
        controller
            .handle
            .retain_drains_from_supply(now, grace, live)
            .await
    }

    /// Begins cooperative handoff (`GOAWAY` on pre-handoff public connections, worker drain) and
    /// refreshes `into`, releasing worker-store listeners once no worker retains attempts and all
    /// pre-handoff public streams are complete.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] when the controller is not serving or control acquisition
    /// times out, or [`PortError::Backend`] if drain projection fails.
    pub async fn hand_off(&self, into: &mut ControllerDrain) -> Result<(), PortError> {
        let live = self.live_controller()?;
        live.handoff.begin();
        live.handle
            .hand_off(live.handoff.public_complete(), into)
            .await?;
        if into.workers.is_empty() && into.complete {
            live.handoff.release_workers();
        }
        Ok(())
    }

    /// Builds and runs one controller term, serving the supplied public and worker connections
    /// until both connection streams end and drain completes.
    ///
    /// Signals `ready` after the controller actor and routes are constructed and live state is
    /// published, before serving connections. On exit, cooperatively shuts down the actor and
    /// joins its task so [`LearnedSlot`] holds `Retained` state before returning. Dropping the
    /// returned future immediately clears live state, aborts the actor task, and closes all
    /// connections.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Config`] if another controller term is already active or the
    /// policy/catalog is invalid, or [`PortError`] if serving fails.
    pub async fn run_controller<
        C: ContentStore,
        A: ActionStore,
        P: RequestGate,
        W: RequestGate,
        Api: ApiService,
    >(
        &self,
        spec: &EngineControllerSpec<'_, C, A>,
        connections: ControllerConnections<P, W, Api>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(PortError::config(EngineError::AlreadyServing));
        }

        let mut term_guard = ControllerTermGuard {
            live: Arc::clone(&self.live),
            actor_task: None,
        };

        raise_open_file_limit(self.config.max_open_files);

        let learned = match self.learned.take_retained() {
            Some(retained) => retained,
            None => Learned::new(&spec.actions, &spec.scaling, spec.fleets.len())
                .map_err(|err| PortError::from(EngineError::Rates(err)))?,
        };
        let policy = build_scheduler_policy(&self.config, spec)?;
        let scheduler = Scheduler::new(policy, spec.fleets, learned)
            .map_err(|err| PortError::from(EngineError::Policy(err)))?;

        let handoff = HandOff::new();
        let actor_config = ActorConfig {
            scheduler_queue_depth: self.config.scheduler_queue_depth,
            scheduler_batch: self.config.scheduler_batch,
            worker_outbound_depth: self.config.worker_outbound_depth,
            assign_inline_max: self.config.assign_inline_max,
        };
        let content = Arc::clone(&spec.stores.content);
        let runtime = ControllerRuntime {
            observations: Some(spec.observations.clone()),
            handoff: Some(handoff.clone()),
            control_acquire_timeout: Some(spec.control_acquire_timeout),
            invalidate_existence: Some(Arc::new(move |digests| {
                content.invalidate_existence(digests);
            })),
        };
        let (handle, actor_task) = spawn_controller(
            scheduler,
            Arc::clone(&spec.clock),
            actor_config,
            self.learned.clone(),
            runtime,
        );
        term_guard.actor_task = Some(actor_task);

        let (public_routes, worker_routes, bytestream_shutdowns) =
            build_controller_routes(&self.config, spec, &handle, &handoff, connections.api);
        let limits = compute_connection_limits(self.config.http2.max_connections, spec.max_workers);
        let store_usage = make_store_usage_fn(&spec.stores);

        *self.live.lock() = Some(LiveController {
            handle: handle.clone(),
            handoff: handoff.clone(),
            store_usage,
        });
        ready.signal();

        let serve_result = serve_controller(
            &self.config,
            connections.public,
            connections.worker,
            public_routes,
            worker_routes,
            limits,
            Arc::clone(&spec.clock),
            spec.observations.clone(),
            &handle,
            &handoff,
        )
        .await;

        let _ = handle.shutdown().await;
        if let Some(task) = term_guard.finish_actor() {
            let _ = task.await;
        }
        for bytestream in bytestream_shutdowns {
            bytestream.shutdown().await;
        }
        spec.stores.content.shutdown().await;
        serve_result
    }

    /// Connects a native worker to [`Self::endpoints`] using `tls` and runs it until `stop`
    /// completes, a single-use worker finishes, or a fatal revocation/auth error occurs.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Config`] if `tls.scheme()` does not match the configured endpoint or
    /// TLS files cannot be loaded, or [`PortError`] if worker setup or session fails fatally.
    pub async fn run_worker<C: ContentStore, A: ActionStore>(
        &self,
        spec: &WorkerRuntimeSpec,
        stores: WorkerStores<C, A>,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError> {
        raise_open_file_limit(self.config.max_open_files);
        let connect_timeout = NonZeroDuration::from(spec.worker.connect_timeout);
        let connector = TlsSessionConnector::from_tls(
            &self.worker_endpoint,
            tls,
            connect_timeout,
            spec.worker.keepalive_interval.get(),
        )
        .map_err(PortError::from)?;
        self.run_worker_with_connector(spec, stores, connector, SystemClock::new(), stop)
            .await
    }

    /// Runs a native worker with an explicit [`SessionConnector`] and [`Clock`] (supporting
    /// in-memory `tokio::io::duplex` channels and deterministic clocks in tests).
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] mapped from [`WorkerError`](crate::WorkerError).
    pub async fn run_worker_with_connector<Conn, C, A>(
        &self,
        spec: &WorkerRuntimeSpec,
        stores: WorkerStores<C, A>,
        connector: Conn,
        clock: impl Clock + Clone + 'static,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError>
    where
        Conn: SessionConnector + 'static,
        C: ContentStore,
        A: ActionStore,
    {
        raise_open_file_limit(self.config.max_open_files);
        let direct_content = match &stores {
            WorkerStores::Direct { content, .. } => Some(Arc::clone(content)),
            WorkerStores::ViaController => None,
        };
        let result = crate::worker::run_worker(spec, stores, connector, &clock, stop)
            .await
            .map_err(PortError::from);
        if let Some(content) = direct_content {
            content.shutdown().await;
        }
        result
    }

    fn live_controller(&self) -> Result<LiveController, PortError> {
        self.live
            .lock()
            .clone()
            .ok_or_else(|| PortError::unavailable(EngineError::NotServing))
    }
}

impl SchedulerPort for Engine {
    fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        Self::learned_history_byte_limit(self)
    }

    async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        Self::learned_history(self, now, into).await
    }

    async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        Self::restore_learned_history(self, now, records).await
    }

    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        Self::snapshot(self, into).await
    }

    async fn actions(
        &self,
        limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        Self::actions(self, limits, into).await
    }

    async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError> {
        Self::set_draining(self, fleet, instance, draining).await
    }

    async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError> {
        Self::retain_drains(self, now, grace, live).await
    }

    fn endpoints(&self) -> ControllerEndpoints {
        Self::endpoints(self)
    }

    async fn run_controller<
        C: ContentStore,
        Ac: ActionStore,
        P: RequestGate,
        W: RequestGate,
        A: ApiService,
    >(
        &self,
        spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        stores: ControllerStores<C, Ac>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        let engine_spec = EngineControllerSpec {
            actions: Arc::clone(&spec.actions),
            scaling: Arc::clone(&spec.scaling),
            clock: Arc::clone(&spec.clock),
            control_acquire_timeout: spec.control_acquire_timeout,
            stores,
            fleets: spec.fleets,
            max_workers: spec.max_workers,
            client_action_timeout: spec.client_action_timeout,
            attempt_budget: spec.attempt_budget,
            observations: spec.observations,
        };
        Self::run_controller(self, &engine_spec, connections, ready).await
    }

    async fn hand_off(&self, into: &mut ControllerDrain) -> Result<(), PortError> {
        Self::hand_off(self, into).await
    }

    async fn run_worker<C: ContentStore, Ac: ActionStore>(
        &self,
        spec: &WorkerSpec,
        stores: WorkerStores<C, Ac>,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError> {
        let runtime_spec = WorkerRuntimeSpec {
            actions: Arc::clone(&spec.actions),
            worker: self.config.worker.clone(),
            memory_pool: spec.memory_pool.clone(),
            instance: spec.instance.clone(),
            properties: spec.properties.clone(),
            lifetime: spec.lifetime,
            work_directory: spec.work_directory.clone(),
            fast_cas_directory: spec.fast_cas_directory.clone(),
            fast_cas_budget: spec.fast_cas_budget,
            trees_directory: spec.trees_directory.clone(),
            entrypoint: spec.entrypoint.clone(),
            instance_name: String::new(),
            gang_version: env!("CARGO_PKG_VERSION").to_owned(),
            max_message_size: self
                .config
                .http2
                .max_message_size
                .max(DEFAULT_WORKER_MAX_MESSAGE_SIZE),
            remote_compression: gang_cas::Compression::Zstd,
        };
        Self::run_worker(self, &runtime_spec, stores, tls, stop).await
    }
}

fn build_scheduler_policy<C, A>(
    config: &EngineConfig,
    spec: &EngineControllerSpec<'_, C, A>,
) -> Result<SchedulerPolicy, PortError> {
    let mut seed_bytes = [0_u8; 8];
    getrandom::fill(&mut seed_bytes)
        .map_err(|err| PortError::from(EngineError::Entropy(err.to_string())))?;
    Ok(SchedulerPolicy {
        actions: (*spec.actions).clone(),
        scaling: *spec.scaling,
        expected_gang_version: env!("CARGO_PKG_VERSION").to_owned(),
        dispatch_ack_timeout: config.dispatch_ack_timeout.get(),
        dispatch_scan_limit: config.dispatch_scan_limit.get(),
        worker_timeout: config.worker_timeout.get(),
        client_action_timeout: spec.client_action_timeout,
        attempt_budget: spec.attempt_budget,
        attempt_seed: u64::from_be_bytes(seed_bytes),
        ..SchedulerPolicy::default()
    })
}

fn make_store_usage_fn<C: ContentStore, A: ActionStore>(
    stores: &ControllerStores<C, A>,
) -> StoreUsageFn {
    let content = Arc::clone(&stores.content);
    let actions = Arc::clone(&stores.actions);
    Arc::new(move |into: &mut Vec<EngineStore>| {
        into.clear();
        if let Some(usage) = content.usage() {
            into.push(usage);
        }
        if let Some(usage) = actions.usage() {
            into.push(usage);
        }
    })
}

fn shared_cache_routes<C: ContentStore, A: ActionStore>(
    stores: &ControllerStores<C, A>,
    budget: &InflightBlobBudget,
    cas_config: CasConfig,
    bytestream_config: ByteStreamConfig,
    limit: usize,
) -> (Routes, crate::reapi_bytestream::ByteStreamShutdown) {
    let action_cache =
        ActionCacheService::new(Arc::clone(&stores.actions), Arc::clone(&stores.content))
            .into_server()
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit);
    let cas = CasService::new(Arc::clone(&stores.content), budget.clone(), cas_config)
        .into_server()
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit);
    let bytestream_service = ByteStreamService::new(
        Arc::clone(&stores.content),
        budget.clone(),
        bytestream_config,
    );
    let bytestream_shutdown = bytestream_service.shutdown_handle();
    let bytestream = bytestream_service
        .into_server()
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit);
    let routes = Routes::builder()
        .routes()
        .add_service(action_cache)
        .add_service(cas)
        .add_service(bytestream);
    (routes, bytestream_shutdown)
}

fn build_controller_routes<C: ContentStore, A: ActionStore, Api: ApiService>(
    config: &EngineConfig,
    spec: &EngineControllerSpec<'_, C, A>,
    handle: &ControllerHandle,
    handoff: &HandOff,
    api: Api,
) -> (
    ScopedRoutes,
    ScopedRoutes,
    Vec<crate::reapi_bytestream::ByteStreamShutdown>,
) {
    let limit = config.http2.max_message_size.get().as_usize();
    let budget = InflightBlobBudget::new(config.max_inflight_blob_bytes);
    let cas_config = CasConfig {
        max_batch_total_size_bytes: config.http2.max_message_size,
        ..CasConfig::default()
    };
    let bytestream_config = ByteStreamConfig {
        read_chunk: config.read_chunk,
        ..ByteStreamConfig::default()
    };
    let capabilities_config = CapabilitiesConfig {
        max_batch_total_size_bytes: config.http2.max_message_size,
        exec_enabled: true,
    };

    let capabilities = CapabilitiesService::new(capabilities_config)
        .into_server()
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit);
    let (public_cache_routes, public_bs_shutdown) =
        shared_cache_routes(&spec.stores, &budget, cas_config, bytestream_config, limit);
    let public_shared = public_cache_routes
        .add_service(capabilities)
        .into_axum_router()
        .layer(BuildObserverLayer::new(spec.observations.clone()))
        .fallback_service(api_service(api));

    let exec_svc = ExecutionService::new(
        handle.clone(),
        Arc::clone(&spec.stores.content),
        Arc::clone(&spec.stores.actions),
        ExecutionServiceConfig::default(),
    )
    .with_clock(Arc::clone(&spec.clock))
    .with_handoff(handoff.clone());
    let ops_svc = exec_svc.operations_service();
    let public_leader = Routes::from(Router::new())
        .add_service(
            exec_svc
                .into_server()
                .max_decoding_message_size(limit)
                .max_encoding_message_size(limit),
        )
        .add_service(
            ops_svc
                .into_server()
                .max_decoding_message_size(limit)
                .max_encoding_message_size(limit),
        )
        .into_axum_router()
        .layer(BuildObserverLayer::new(spec.observations.clone()));

    let (worker_cache_routes, worker_bs_shutdown) =
        shared_cache_routes(&spec.stores, &budget, cas_config, bytestream_config, limit);
    let worker_shared = worker_cache_routes.into_axum_router();
    let worker_svc = WorkerGrpcService::with_clock(
        handle.clone(),
        WorkerServiceConfig::from_worker_timeout(NonZeroDuration::from(config.worker_timeout)),
        Arc::clone(&spec.clock),
    )
    .into_server()
    .max_decoding_message_size(limit)
    .max_encoding_message_size(limit);
    let legacy_svc = crate::legacy_worker::LegacyWorkerService::new(
        handle.clone(),
        WorkerServiceConfig::from_worker_timeout(NonZeroDuration::from(config.worker_timeout)),
        Arc::clone(&spec.clock),
        limit,
    );
    let worker_leader = Routes::from(Router::new())
        .add_service(worker_svc)
        .add_service(legacy_svc)
        .into_axum_router();

    (
        ScopedRoutes {
            shared: public_shared,
            leader: public_leader,
        },
        ScopedRoutes {
            shared: worker_shared,
            leader: worker_leader,
        },
        vec![public_bs_shutdown, worker_bs_shutdown],
    )
}

#[allow(clippy::too_many_arguments)]
async fn serve_controller<P: RequestGate, W: RequestGate>(
    config: &EngineConfig,
    public: gang_spi::Listening<P>,
    worker: gang_spi::Listening<W>,
    public_routes: ScopedRoutes,
    worker_routes: ScopedRoutes,
    limits: ConnectionLimits,
    clock: Arc<dyn Clock>,
    observations: ObservationSink,
    handle: &ControllerHandle,
    handoff: &HandOff,
) -> Result<(), PortError> {
    let (ended, mut endings) = mpsc::unbounded_channel();
    let shutdown = ServeShutdown {
        ended,
        handoff: handoff.clone(),
        observations: Some(observations),
    };
    let serving = async {
        tokio::try_join!(
            serve(
                public,
                public_routes,
                &config.http2,
                Listener::Public,
                limits.public,
                Arc::clone(&clock),
                shutdown.clone(),
            ),
            serve(
                worker,
                worker_routes,
                &config.http2,
                Listener::Worker,
                limits.worker,
                clock,
                shutdown,
            ),
        )?;
        Ok::<(), PortError>(())
    };
    tokio::pin!(serving);

    for _ in 0..2 {
        tokio::select! {
            result = &mut serving => return result,
            _ = endings.recv() => {}
        }
    }

    handoff.begin();
    let mut drain = ControllerDrain::default();
    loop {
        match handle.hand_off(handoff.public_complete(), &mut drain).await {
            Ok(()) if !drain.workers.is_empty() => {}
            Ok(()) | Err(_) => {
                handoff.release_workers();
                break;
            }
        }
        tokio::select! {
            result = &mut serving => return result,
            () = tokio::time::sleep(HANDOFF_DRAIN_POLL_INTERVAL) => {}
        }
    }
    serving.await
}

/// Adapts a [`gang_spi::ApiService`] into an infallible `axum` fallback service.
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

/// Safely raises the process open-file soft limit up to `limit` (clamped to the hard limit).
fn raise_open_file_limit(limit: OpenFileLimit) {
    let target = u64::try_from(limit.get()).unwrap_or(u64::MAX);
    let mut rlimit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let current = rlimit.current.unwrap_or(0);
    if current >= target {
        return;
    }
    let capped = rlimit.maximum.map_or(target, |max| target.min(max));
    if capped > current {
        rlimit.current = Some(capped);
        if let Err(err) = rustix::process::setrlimit(rustix::process::Resource::Nofile, rlimit) {
            tracing::warn!(
                error = %err,
                current,
                target = capped,
                "cannot raise process open-file soft limit"
            );
        }
    }
}

fn build_dialer(
    tls: WorkerTls<'_>,
    handshake_timeout: NonZeroDuration,
) -> Result<Dialer, EngineError> {
    let (ca, certificate, private_key) = match tls {
        WorkerTls::Plain => return Ok(Dialer::new(None)),
        WorkerTls::ServerOnly { server_ca } => {
            (server_ca.map(read_certificate).transpose()?, None, None)
        }
        WorkerTls::Mutual(files) => (
            Some(read_certificate(files.server_ca)?),
            Some(read_certificate(files.certificate)?),
            Some(read_private_key(files.private_key)?),
        ),
    };
    let trust = ca.as_ref().map_or(ServerTrust::System, ServerTrust::Ca);
    let identity =
        certificate
            .as_ref()
            .zip(private_key.as_ref())
            .map(|(certificate, private_key)| ClientIdentity {
                certificate,
                private_key,
            });
    let connector = TlsConnector::try_new(&ConnectorSettings {
        trust,
        identity,
        handshake_timeout,
        clock: Arc::new(SystemClock::default()),
        alpn: Alpn::H2,
    })
    .map_err(EngineError::WorkerTls)?;
    Ok(Dialer::new(Some(connector)))
}

fn read_certificate(path: &Path) -> Result<CertificatePem, EngineError> {
    let text = std::fs::read_to_string(path).map_err(|source| EngineError::WorkerTlsFile {
        path: path.to_owned(),
        source,
    })?;
    CertificatePem::try_new(text).map_err(|source| EngineError::WorkerTlsPem {
        path: path.to_owned(),
        source,
    })
}

fn read_private_key(path: &Path) -> Result<PrivateKeyPem, EngineError> {
    let text = std::fs::read_to_string(path).map_err(|source| EngineError::WorkerTlsFile {
        path: path.to_owned(),
        source,
    })?;
    PrivateKeyPem::try_new(text).map_err(|source| EngineError::WorkerTlsPem {
        path: path.to_owned(),
        source,
    })
}

fn build_controller_endpoint(
    endpoint: &Url,
    has_tls: bool,
    connect_timeout: Duration,
    keepalive_interval: Duration,
) -> Result<Endpoint, EngineError> {
    let host = endpoint
        .host_str()
        .ok_or(EngineConfigError::EndpointHost("worker_endpoint"))?;
    let port = endpoint.port().unwrap_or(if has_tls { 443 } else { 80 });
    let authority = format!("{host}:{port}");
    let transport = Endpoint::from_shared(format!("http://{authority}"))
        .map_err(|err| EngineError::EndpointUri(err.to_string()))?;
    let transport = if has_tls {
        let origin = format!("https://{authority}")
            .parse()
            .map_err(|err: http::uri::InvalidUri| EngineError::EndpointUri(err.to_string()))?;
        transport.origin(origin)
    } else {
        transport
    };
    Ok(transport
        .connect_timeout(connect_timeout)
        .http2_keep_alive_interval(keepalive_interval)
        .keep_alive_timeout(WORKER_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::{pending, ready};
    use std::sync::atomic::AtomicU64;

    use bytes::Bytes;
    use futures::{StreamExt as _, stream};
    use gang_cas::Compression;
    use gang_clock::SystemClock;
    use gang_config::{ActionsConfig, ScalingConfig, WholeSeconds, WorkerConfig};
    use gang_reapi::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
    use gang_reapi::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
    use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
    use gang_reapi::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
    use gang_reapi::build::bazel::remote::execution::v2::{
        Action, ActionResult, Command, ExecuteRequest, ExecuteResponse, GetActionResultRequest,
        GetCapabilitiesRequest, compressor, digest_function,
    };
    use gang_reapi::google::bytestream::WriteRequest;
    use gang_reapi::google::bytestream::byte_stream_client::ByteStreamClient;
    use gang_reapi::google::longrunning::operation;
    use gang_reapi::to_proto;
    use gang_spi::{
        Accepted, Admitted, ApiResponseBody, Authenticated, BlobStream, ByteRange, ClientTls,
        GateDecision, Listening, ObservationSink, Peer, RequestScope, StoreKind, full_body,
    };
    use gang_testkit::{ChildTest, MemoryActionStore, MemoryContentStore, TestResult};
    use gang_types::{
        Bytes as GangBytes, Digest, FleetActionPolicy, FleetCatalog, FleetName, FleetProperties,
        Identity, InstanceId, Isa, NonZeroBytes, NonZeroDuration, Principal, ProviderName,
        Resources, WorkerLifetime,
    };
    use hyper::StatusCode;
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;
    use prost::Message as _;
    use tempfile::TempDir;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use super::*;
    use crate::worker::DEFAULT_WORKER_MAX_MESSAGE_SIZE;

    const ENGINE_ROUNDTRIP_CHILD: ChildTest =
        ChildTest::new("engine::tests::engine_roundtrip_child");

    #[ignore = "child test helper executing inside Engine roundtrip"]
    #[test]
    fn engine_roundtrip_child() -> Result<(), Box<dyn std::error::Error>> {
        let input = fs::read("input.txt")?;
        let mut output = b"processed:".to_vec();
        output.extend_from_slice(&input);
        fs::write("output.txt", &output)?;
        println!("engine-child-ok");
        ENGINE_ROUNDTRIP_CHILD.complete();
        Ok(())
    }

    fn run_gated<F>(future: F) -> TestResult
    where
        F: Future<Output = TestResult>,
    {
        let _spawn = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(future)
    }

    fn worker_config_with_endpoint(endpoint: &str) -> TestResult<EngineConfig> {
        Ok(EngineConfig::for_worker(
            Some(endpoint.parse()?),
            &WorkerConfig::default(),
        ))
    }

    fn test_catalog() -> TestResult<(FleetCatalog, FleetProperties)> {
        let env = "host:test".parse()?;
        let props = FleetProperties::new(
            &FleetName::try_new("fleet-a")?,
            Isa::X86_64,
            &"none",
            &env,
            Resources {
                vcpus: 4,
                memory: GangBytes::gib(8),
                disk: GangBytes::gib(32),
            },
            &FleetActionPolicy::default(),
            [],
        );
        let catalog = FleetCatalog::try_new(vec![props.clone()])?;
        Ok((catalog, props))
    }

    #[derive(Clone)]
    struct AllowAllGate;

    impl RequestGate for AllowAllGate {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            let auth = Authenticated {
                identity: Arc::new(Identity::new(
                    ProviderName::try_new("test").expect("provider"),
                    Principal::try_new("tester").expect("principal"),
                )),
                valid_until: None,
            };
            ready(Ok(GateDecision::Admit(Admitted::Authenticated(auth))))
        }
    }

    #[derive(Clone)]
    struct FallbackApi;

    impl ApiService for FallbackApi {
        fn call(
            &self,
            request: Request<ApiRequestBody>,
        ) -> impl Future<Output = Response<ApiResponseBody>> + Send {
            let path = request.uri().path().to_owned();
            async move {
                if path == "/v1/ping" {
                    Response::new(full_body("pong"))
                } else {
                    let mut resp = Response::new(full_body("not found"));
                    *resp.status_mut() = StatusCode::NOT_FOUND;
                    resp
                }
            }
        }
    }

    #[derive(Debug)]
    struct UsageContentStore {
        inner: MemoryContentStore,
        used: AtomicU64,
        budget: u64,
        shutdown_count: AtomicU64,
    }

    impl UsageContentStore {
        fn new(budget: u64) -> Self {
            Self {
                inner: MemoryContentStore::new(),
                used: AtomicU64::new(0),
                budget,
                shutdown_count: AtomicU64::new(0),
            }
        }
    }

    impl ContentStore for UsageContentStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.inner.find_missing(digests, missing).await
        }

        async fn read(
            &self,
            digest: Digest,
            range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            self.inner.read(digest, range).await
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            let size = body.size;
            self.inner.write(digest, body).await?;
            self.used.fetch_add(size, Ordering::Relaxed);
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            Some(EngineStore {
                store: StoreKind::Content,
                used: self.used.load(Ordering::Relaxed),
                budget: self.budget,
                evicted: 0,
            })
        }

        async fn shutdown(&self) {
            self.shutdown_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[derive(Debug)]
    struct UsageActionStore {
        inner: MemoryActionStore,
        used: AtomicU64,
        budget: u64,
    }

    impl UsageActionStore {
        fn new(budget: u64) -> Self {
            Self {
                inner: MemoryActionStore::new(),
                used: AtomicU64::new(0),
                budget,
            }
        }
    }

    impl ActionStore for UsageActionStore {
        async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
            self.inner.get(action).await
        }

        async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
            let len = u64::try_from(result.len()).unwrap_or(u64::MAX);
            self.inner.put(action, result).await?;
            self.used.fetch_add(len, Ordering::Relaxed);
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            Some(EngineStore {
                store: StoreKind::Action,
                used: self.used.load(Ordering::Relaxed),
                budget: self.budget,
                evicted: 0,
            })
        }
    }

    fn tcp_listening(
        listener: TcpListener,
        stop: oneshot::Receiver<()>,
    ) -> Listening<AllowAllGate> {
        let connections = stream::unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await.ok()?;
            Some((
                Accepted {
                    connection: Box::new(stream),
                    peer: Peer::default(),
                },
                listener,
            ))
        })
        .take_until(async move {
            let _ = stop.await;
        })
        .boxed();
        Listening {
            connections,
            gate: AllowAllGate,
        }
    }

    #[test]
    fn endpoints_and_try_new_validation() -> TestResult {
        let engine = Engine::try_new(worker_config_with_endpoint("grpc://127.0.0.1:9100")?)?;
        let expected: Url = "grpc://127.0.0.1:9100".parse()?;
        assert_eq!(engine.endpoints().worker, expected);

        assert!(matches!(
            Engine::try_new(EngineConfig::default()),
            Err(PortError::Config(_))
        ));

        let bad_scheme = EngineConfig {
            worker_endpoint: Some("http://127.0.0.1:9100".parse()?),
            ..EngineConfig::default()
        };
        assert!(matches!(
            Engine::try_new(bad_scheme),
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn queries_and_control_are_unavailable_until_controller_serves() -> TestResult {
        let engine = Engine::try_new(worker_config_with_endpoint("grpc://127.0.0.1:9100")?)?;

        assert!(matches!(
            engine.learned_history_byte_limit(),
            Err(PortError::Unavailable(_))
        ));
        let mut records = Vec::new();
        assert!(matches!(
            engine
                .learned_history(Timestamp::from_unix_millis(1_000), &mut records)
                .await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            engine
                .restore_learned_history(Timestamp::from_unix_millis(1_000), &[])
                .await,
            Err(PortError::Unavailable(_))
        ));

        let mut snapshot = SchedulerSnapshot::default();
        assert!(matches!(
            engine.snapshot(&mut snapshot).await,
            Err(PortError::Unavailable(_))
        ));

        let mut actions = ActionsView::default();
        assert!(matches!(
            engine
                .actions(
                    ListingLimits {
                        running: 4,
                        queued: 4
                    },
                    &mut actions
                )
                .await,
            Err(PortError::Unavailable(_))
        ));

        let fleet = FleetName::try_new("fleet-a")?;
        let instance = InstanceId::try_new("i-0")?;
        assert!(matches!(
            engine.set_draining(&fleet, &instance, true).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            engine
                .retain_drains(Tick::ORIGIN, Duration::from_secs(10), &[])
                .await,
            Err(PortError::Unavailable(_))
        ));

        let mut drain = ControllerDrain::default();
        assert!(matches!(
            engine.hand_off(&mut drain).await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn run_worker_validates_tls_scheme_and_pem_files() -> TestResult {
        let tmp = TempDir::new()?;
        let root = tmp.path();
        let (_, props) = test_catalog()?;
        let spec = WorkerRuntimeSpec {
            actions: Arc::new(ActionsConfig::default()),
            worker: WorkerConfig::default(),
            memory_pool: None,
            instance: InstanceId::try_new("i-tls")?,
            properties: props,
            lifetime: WorkerLifetime::LongLived,
            work_directory: root.join("work"),
            fast_cas_directory: root.join("cas"),
            fast_cas_budget: GangBytes::mib(16),
            trees_directory: None,
            entrypoint: std::env::current_exe()?,
            instance_name: String::new(),
            gang_version: env!("CARGO_PKG_VERSION").to_owned(),
            max_message_size: DEFAULT_WORKER_MAX_MESSAGE_SIZE,
            remote_compression: Compression::Identity,
        };
        let stores: WorkerStores<MemoryContentStore, MemoryActionStore> =
            WorkerStores::ViaController;

        // `grpc://` endpoint rejects TLS.
        let plain_engine = Engine::try_new(worker_config_with_endpoint("grpc://127.0.0.1:9100")?)?;
        let err = plain_engine
            .run_worker(
                &spec,
                stores.clone(),
                WorkerTls::ServerOnly { server_ca: None },
                pending(),
            )
            .await
            .expect_err("scheme mismatch must fail");
        assert!(matches!(err, PortError::Config(_)));

        // `grpcs://` endpoint rejects Plain.
        let tls_engine = Engine::try_new(worker_config_with_endpoint("grpcs://127.0.0.1:9100")?)?;
        let err = tls_engine
            .run_worker(&spec, stores.clone(), WorkerTls::Plain, pending())
            .await
            .expect_err("scheme mismatch must fail");
        assert!(matches!(err, PortError::Config(_)));

        // Missing TLS file returns PortError::Config.
        let missing_ca = root.join("missing-ca.pem");
        let err = tls_engine
            .run_worker(
                &spec,
                stores.clone(),
                WorkerTls::ServerOnly {
                    server_ca: Some(&missing_ca),
                },
                pending(),
            )
            .await
            .expect_err("missing ca file must fail");
        assert!(matches!(err, PortError::Config(_)));

        // Malformed PEM file returns PortError::Config.
        let bad_pem = root.join("bad.pem");
        fs::write(&bad_pem, "not a pem")?;
        let err = tls_engine
            .run_worker(
                &spec,
                stores,
                WorkerTls::Mutual(ClientTls {
                    certificate: &bad_pem,
                    private_key: &bad_pem,
                    server_ca: &bad_pem,
                }),
                pending(),
            )
            .await
            .expect_err("malformed pem must fail");
        assert!(matches!(err, PortError::Config(_)));

        // IPv6 grpcs endpoint builds connector cleanly without error.
        let ipv6_url: Url = "grpcs://[::1]:9100".parse()?;
        let connector = TlsSessionConnector::from_tls(
            &ipv6_url,
            WorkerTls::ServerOnly { server_ca: None },
            NonZeroDuration::from_secs(2),
            Duration::from_secs(10),
        )?;
        assert_eq!(connector.endpoint.uri().scheme_str(), Some("http"));
        assert_eq!(
            connector
                .endpoint
                .uri()
                .authority()
                .map(http::uri::Authority::as_str),
            Some("[::1]:9100")
        );
        Ok(())
    }

    #[tokio::test]
    async fn connection_limits_match_server_and_fleet_bounds() {
        let limits = compute_connection_limits(NonZeroUsize::MIN.saturating_add(6), 2);
        assert_eq!((limits.public.get(), limits.worker.get()), (7, 12));
        assert_eq!(
            compute_connection_limits(NonZeroUsize::MIN, 0).worker,
            NonZeroUsize::MIN
        );
        assert_eq!(
            compute_connection_limits(NonZeroUsize::MIN, u64::MAX)
                .worker
                .get(),
            usize::MAX
        );
    }

    #[tokio::test]
    async fn concurrent_run_controller_is_rejected_and_drop_clears_live_state() -> TestResult {
        let engine = Arc::new(Engine::try_new(worker_config_with_endpoint(
            "grpc://127.0.0.1:9100",
        )?)?);
        let (catalog, _) = test_catalog()?;
        let (sink, _) =
            ObservationSink::channel(NonZeroUsize::new(8).ok_or("nonzero")?, NonZeroUsize::MIN);
        let content = Arc::new(MemoryContentStore::new());
        let actions = Arc::new(MemoryActionStore::new());
        let spec = EngineControllerSpec {
            actions: Arc::new(ActionsConfig::default()),
            scaling: Arc::new(ScalingConfig::default()),
            clock: Arc::new(SystemClock::new()),
            control_acquire_timeout: NonZeroDuration::from_secs(1),
            stores: ControllerStores { content, actions },
            fleets: &catalog,
            max_workers: 4,
            client_action_timeout: Duration::from_secs(30),
            attempt_budget: Duration::from_mins(2),
            observations: &sink,
        };

        let (ready, ready_rx) = ControllerReady::channel();
        let engine_clone = Arc::clone(&engine);
        let catalog_clone = catalog.clone();
        let sink_clone = sink.clone();
        let ctrl_task = tokio::spawn(async move {
            let local_spec = EngineControllerSpec {
                actions: Arc::new(ActionsConfig::default()),
                scaling: Arc::new(ScalingConfig::default()),
                clock: Arc::new(SystemClock::new()),
                control_acquire_timeout: NonZeroDuration::from_secs(1),
                stores: ControllerStores {
                    content: Arc::new(MemoryContentStore::new()),
                    actions: Arc::new(MemoryActionStore::new()),
                },
                fleets: &catalog_clone,
                max_workers: 4,
                client_action_timeout: Duration::from_secs(30),
                attempt_budget: Duration::from_mins(2),
                observations: &sink_clone,
            };
            let conns = ControllerConnections {
                public: Listening {
                    connections: stream::pending().boxed(),
                    gate: AllowAllGate,
                },
                worker: Listening {
                    connections: stream::pending().boxed(),
                    gate: AllowAllGate,
                },
                api: FallbackApi,
            };
            Engine::run_controller(&engine_clone, &local_spec, conns, ready).await
        });

        ready_rx.await?;
        assert!(engine.learned_history_byte_limit().is_ok());

        // Second concurrent run_controller must fail with PortError::Config and leave ready unsent.
        let (second_ready, second_ready_rx) = ControllerReady::channel();
        let second_conns = ControllerConnections {
            public: Listening {
                connections: stream::pending().boxed(),
                gate: AllowAllGate,
            },
            worker: Listening {
                connections: stream::pending().boxed(),
                gate: AllowAllGate,
            },
            api: FallbackApi,
        };
        let second = Engine::run_controller(&engine, &spec, second_conns, second_ready).await;
        assert!(matches!(second, Err(PortError::Config(_))));
        assert!(second_ready_rx.await.is_err());

        // Aborting/dropping the running controller clears live state cleanly.
        ctrl_task.abort();
        let _ = ctrl_task.await;
        let mut snap = SchedulerSnapshot::default();
        assert!(matches!(
            engine.snapshot(&mut snap).await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }

    async fn assert_fallback_api(public_addr: std::net::SocketAddr) -> TestResult {
        let stream = TcpStream::connect(public_addr).await?;
        let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let http_resp = sender
            .send_request(
                http::Request::get("/v1/ping").body(http_body_util::Empty::<Bytes>::new())?,
            )
            .await?;
        assert_eq!(http_resp.status(), StatusCode::OK);
        let body = http_resp.into_body().collect().await?.to_bytes();
        assert_eq!(&body[..], b"pong");
        Ok(())
    }

    async fn assert_capabilities(channel: tonic::transport::Channel) -> TestResult {
        let mut caps_client = CapabilitiesClient::new(channel);
        let caps = caps_client
            .get_capabilities(GetCapabilitiesRequest {
                instance_name: String::new(),
            })
            .await?
            .into_inner();
        let cache_caps = caps.cache_capabilities.ok_or("missing cache caps")?;
        assert_eq!(
            cache_caps.digest_functions,
            vec![digest_function::Value::Sha256 as i32]
        );
        assert!(
            cache_caps
                .supported_compressors
                .contains(&(compressor::Value::Zstd as i32))
        );
        let exec_caps = caps
            .execution_capabilities
            .ok_or("missing execution caps")?;
        assert!(exec_caps.exec_enabled);
        let prio_range = exec_caps
            .execution_priority_capabilities
            .ok_or("missing priority caps")?
            .priorities;
        assert_eq!(prio_range[0].min_priority, i32::MIN);
        assert_eq!(prio_range[0].max_priority, i32::MAX);
        Ok(())
    }

    async fn seed_action_via_bytestream(channel: tonic::transport::Channel) -> TestResult<Digest> {
        let mut bs_client = ByteStreamClient::new(channel.clone());
        let input_bytes = Bytes::from_static(b"hello-engine");
        let input_digest = Digest::from_bytes(&input_bytes);
        upload_via_bytestream(&mut bs_client, input_digest, input_bytes).await?;

        let root_dir = gang_reapi::build::bazel::remote::execution::v2::Directory {
            files: vec![gang_reapi::build::bazel::remote::execution::v2::FileNode {
                name: "input.txt".to_owned(),
                digest: Some(to_proto(&input_digest)?),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let dir_bytes = Bytes::from(root_dir.encode_to_vec());
        let dir_digest = Digest::from_bytes(&dir_bytes);
        upload_via_bytestream(&mut bs_client, dir_digest, dir_bytes).await?;

        let cmd = Command {
            arguments: ENGINE_ROUNDTRIP_CHILD
                .arguments()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            output_paths: vec!["output.txt".to_owned()],
            ..Default::default()
        };
        let cmd_bytes = Bytes::from(cmd.encode_to_vec());
        let cmd_digest = Digest::from_bytes(&cmd_bytes);
        upload_via_bytestream(&mut bs_client, cmd_digest, cmd_bytes).await?;

        let action = Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&dir_digest)?),
            ..Default::default()
        };
        let action_bytes = Bytes::from(action.encode_to_vec());
        let action_digest = Digest::from_bytes(&action_bytes);
        upload_via_bytestream(&mut bs_client, action_digest, action_bytes).await?;

        let mut storage_client = ContentAddressableStorageClient::new(channel);
        let missing = storage_client
            .find_missing_blobs(
                gang_reapi::build::bazel::remote::execution::v2::FindMissingBlobsRequest {
                    instance_name: String::new(),
                    blob_digests: vec![to_proto(&action_digest)?, to_proto(&cmd_digest)?],
                    digest_function: digest_function::Value::Sha256 as i32,
                },
            )
            .await?
            .into_inner();
        assert!(missing.missing_blob_digests.is_empty());
        Ok(action_digest)
    }

    async fn execute_and_verify_action(
        channel: tonic::transport::Channel,
        action_digest: Digest,
    ) -> TestResult {
        let mut exec_client = ExecutionClient::new(channel.clone());
        let metadata = gang_reapi::build::bazel::remote::execution::v2::RequestMetadata {
            tool_invocation_id: "inv-roundtrip".to_owned(),
            target_id: "//app:roundtrip".to_owned(),
            action_mnemonic: "Roundtrip".to_owned(),
            ..Default::default()
        };
        let mut request = tonic::Request::new(ExecuteRequest {
            instance_name: String::new(),
            skip_cache_lookup: false,
            action_digest: Some(to_proto(&action_digest)?),
            execution_policy: None,
            results_cache_policy: None,
            digest_function: digest_function::Value::Sha256 as i32,
            inline_stdout: true,
            inline_stderr: true,
            inline_output_files: vec!["output.txt".to_owned()],
        });
        request.metadata_mut().insert_bin(
            "build.bazel.remote.execution.v2.requestmetadata-bin",
            tonic::metadata::MetadataValue::from_bytes(&metadata.encode_to_vec()),
        );
        let mut op_stream = exec_client.execute(request).await?.into_inner();

        let mut completed_result: Option<ActionResult> = None;
        while let Some(op) = op_stream.message().await? {
            if op.done {
                let Some(operation::Result::Response(any)) = op.result else {
                    return Err("expected ExecuteResponse on done operation".into());
                };
                let exec_resp = ExecuteResponse::decode(any.value.as_slice())?;
                completed_result = exec_resp.result;
                break;
            }
        }
        let result = completed_result.ok_or("missing ActionResult")?;
        assert_eq!(result.exit_code, 0);
        let stdout_digest: Digest = gang_reapi::from_proto(
            result
                .stdout_digest
                .as_ref()
                .ok_or("missing stdout_digest")?,
        )?;
        let mut bs_client = ByteStreamClient::new(channel.clone());
        let stdout_bytes = read_via_bytestream(&mut bs_client, stdout_digest).await?;
        let stdout_text = String::from_utf8(stdout_bytes)?;
        assert!(stdout_text.contains("engine-child-ok"), "{stdout_text}");
        assert_eq!(result.output_files.len(), 1);
        assert_eq!(result.output_files[0].path, "output.txt");
        let out_digest: Digest = gang_reapi::from_proto(
            result.output_files[0]
                .digest
                .as_ref()
                .ok_or("missing output file digest")?,
        )?;
        let out_bytes = read_via_bytestream(&mut bs_client, out_digest).await?;
        assert_eq!(out_bytes.as_slice(), b"processed:hello-engine");

        let mut ac_client = ActionCacheClient::new(channel);
        let cached = ac_client
            .get_action_result(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(to_proto(&action_digest)?),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await?
            .into_inner();
        assert_eq!(cached.exit_code, 0);
        Ok(())
    }

    async fn read_via_bytestream(
        client: &mut ByteStreamClient<tonic::transport::Channel>,
        digest: Digest,
    ) -> TestResult<Vec<u8>> {
        let mut stream = client
            .read(gang_reapi::google::bytestream::ReadRequest {
                resource_name: format!("blobs/{}/{}", digest.hex(), digest.size()),
                read_offset: 0,
                read_limit: 0,
            })
            .await?
            .into_inner();
        let mut data = Vec::new();
        while let Some(chunk) = stream.message().await? {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }

    fn build_roundtrip_worker_spec(
        root: &Path,
        props: FleetProperties,
    ) -> TestResult<WorkerRuntimeSpec> {
        Ok(WorkerRuntimeSpec {
            actions: Arc::new(ActionsConfig::default()),
            worker: WorkerConfig {
                connect_timeout: WholeSeconds::from_units(2),
                graveyard_workers: NonZeroUsize::new(2).ok_or("nonzero")?,
                ..WorkerConfig::default()
            },
            memory_pool: None,
            instance: InstanceId::try_new("inst-d6")?,
            properties: props,
            lifetime: WorkerLifetime::LongLived,
            work_directory: root.join("work"),
            fast_cas_directory: root.join("cas"),
            fast_cas_budget: GangBytes::mib(32),
            trees_directory: None,
            entrypoint: std::env::current_exe()?,
            instance_name: String::new(),
            gang_version: env!("CARGO_PKG_VERSION").to_owned(),
            max_message_size: DEFAULT_WORKER_MAX_MESSAGE_SIZE,
            remote_compression: Compression::Identity,
        })
    }

    async fn handoff_and_verify_retained(
        controller_engine: &Engine,
        byte_limit_during_serve: NonZeroUsize,
        stop_worker_tx: oneshot::Sender<()>,
        worker_task: JoinHandle<Result<(), PortError>>,
        public_stop_tx: oneshot::Sender<()>,
        worker_stop_tx: oneshot::Sender<()>,
        controller_task: JoinHandle<Result<(), PortError>>,
    ) -> TestResult {
        let mut drain = ControllerDrain::default();
        loop {
            controller_engine.hand_off(&mut drain).await?;
            if drain.complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(drain.workers.is_empty());
        assert!(drain.complete);

        let _ = stop_worker_tx.send(());
        worker_task.await??;
        let _ = public_stop_tx.send(());
        let _ = worker_stop_tx.send(());
        controller_task.await??;

        assert_eq!(
            controller_engine.learned_history_byte_limit()?,
            byte_limit_during_serve
        );
        let now = SystemClock::new().wall();
        let mut history = Vec::new();
        controller_engine.learned_history(now, &mut history).await?;
        assert!(
            !history.is_empty(),
            "retained learned history must survive controller exit"
        );
        controller_engine
            .restore_learned_history(now, &history)
            .await?;
        let mut snap_after = SchedulerSnapshot::default();
        assert!(matches!(
            controller_engine.snapshot(&mut snap_after).await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }

    async fn assert_legacy_route(address: std::net::SocketAddr) -> TestResult {
        let channel = Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        let mut legacy = tonic::client::Grpc::new(channel);
        legacy.ready().await?;
        let path = http::uri::PathAndQuery::from_static(
            "/com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
        );
        let response: Result<tonic::Response<tonic::Streaming<()>>, _> = legacy
            .streaming(
                tonic::Request::new(futures::stream::empty::<()>()),
                path,
                tonic_prost::ProstCodec::default(),
            )
            .await;
        assert_eq!(
            response.expect_err("empty registration").code(),
            tonic::Code::InvalidArgument
        );
        Ok(())
    }

    #[test]
    fn full_controller_worker_reapi_roundtrip_and_post_drain_learned_history() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let (catalog, props) = test_catalog()?;

            let public_listener = TcpListener::bind("127.0.0.1:0").await?;
            let public_addr = public_listener.local_addr()?;
            let worker_listener = TcpListener::bind("127.0.0.1:0").await?;
            let worker_addr = worker_listener.local_addr()?;

            let worker_endpoint: Url = format!("grpc://{worker_addr}").parse()?;
            let mut server_cfg = gang_testkit::STANDARD_GANG_TOML
                .parse::<gang_config::GangConfig<gang_testkit::OpaquePlugins>>()?
                .server;
            server_cfg.transport = gang_config::Transport::Plaintext;
            server_cfg.worker_timeout = WholeSeconds::from_units(10);
            server_cfg.http2.max_message_size = NonZeroBytes::mib(8);

            let controller_engine = Arc::new(Engine::try_new(EngineConfig::for_controller(
                worker_endpoint.clone(),
                &server_cfg,
            ))?);

            let content = Arc::new(UsageContentStore::new(64 * 1024 * 1024));
            let content_observer = Arc::clone(&content);
            let actions = Arc::new(UsageActionStore::new(16 * 1024 * 1024));
            let (sink, _observations) = ObservationSink::channel(
                NonZeroUsize::new(32).ok_or("nonzero")?,
                NonZeroUsize::MIN,
            );

            let (public_stop_tx, public_stop_rx) = oneshot::channel::<()>();
            let (worker_stop_tx, worker_stop_rx) = oneshot::channel::<()>();
            let (ready, ready_rx) = ControllerReady::channel();

            let ctrl_engine = Arc::clone(&controller_engine);
            let ctrl_catalog = catalog.clone();
            let controller_task = tokio::spawn(async move {
                let spec = EngineControllerSpec {
                    actions: Arc::new(ActionsConfig::default()),
                    scaling: Arc::new(ScalingConfig::default()),
                    clock: Arc::new(SystemClock::new()),
                    control_acquire_timeout: NonZeroDuration::from_secs(2),
                    stores: ControllerStores { content, actions },
                    fleets: &ctrl_catalog,
                    max_workers: 4,
                    client_action_timeout: Duration::from_secs(30),
                    attempt_budget: Duration::from_mins(2),
                    observations: &sink,
                };
                let connections = ControllerConnections {
                    public: tcp_listening(public_listener, public_stop_rx),
                    worker: tcp_listening(worker_listener, worker_stop_rx),
                    api: FallbackApi,
                };
                Engine::run_controller(&ctrl_engine, &spec, connections, ready).await
            });

            ready_rx.await?;
            assert_legacy_route(worker_addr).await?;
            let byte_limit_during_serve = controller_engine.learned_history_byte_limit()?;
            assert_fallback_api(public_addr).await?;

            let public_channel = Endpoint::from_shared(format!("http://{public_addr}"))?
                .connect()
                .await?;
            assert_capabilities(public_channel.clone()).await?;
            let action_digest = seed_action_via_bytestream(public_channel.clone()).await?;

            let worker_engine = Engine::try_new(EngineConfig::for_worker(
                Some(worker_endpoint),
                &WorkerConfig::default(),
            ))?;
            let worker_spec = build_roundtrip_worker_spec(&root, props)?;
            let (stop_worker_tx, stop_worker_rx) = oneshot::channel::<()>();
            let worker_task = tokio::spawn(async move {
                worker_engine
                    .run_worker(
                        &worker_spec,
                        WorkerStores::<MemoryContentStore, MemoryActionStore>::ViaController,
                        WorkerTls::Plain,
                        async move {
                            let _ = stop_worker_rx.await;
                        },
                    )
                    .await
            });

            execute_and_verify_action(public_channel.clone(), action_digest).await?;
            drop(public_channel);

            let mut snapshot = SchedulerSnapshot::default();
            controller_engine.snapshot(&mut snapshot).await?;
            assert_eq!(snapshot.completed, 1);
            assert_eq!(snapshot.workers.len(), 1);
            assert_eq!(snapshot.stores.len(), 2);
            assert!(snapshot.stores[0].used > 0);
            assert!(snapshot.stores[1].used > 0);

            handoff_and_verify_retained(
                &controller_engine,
                byte_limit_during_serve,
                stop_worker_tx,
                worker_task,
                public_stop_tx,
                worker_stop_tx,
                controller_task,
            )
            .await?;
            assert_eq!(content_observer.shutdown_count.load(Ordering::Acquire), 1);
            Ok(())
        })
    }

    async fn upload_via_bytestream(
        client: &mut ByteStreamClient<tonic::transport::Channel>,
        digest: Digest,
        data: Bytes,
    ) -> TestResult {
        let resource_name = format!(
            "uploads/test-upload/blobs/{}/{}",
            digest.hex(),
            digest.size()
        );
        let req = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: true,
            data: data.clone(),
        };
        let resp = client.write(stream::once(ready(req))).await?.into_inner();
        assert_eq!(
            u64::try_from(resp.committed_size)?,
            u64::try_from(data.len())?
        );
        Ok(())
    }
}
