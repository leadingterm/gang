//! The server: prepares the authentication core, the gates and the control API, then runs the
//! scheduler's controller and the control loop until shutdown.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{Map, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use gang_clock::Clock;
use gang_config::{DashboardConfig, EndpointTrust, Fleets, LeadershipConfig};
use gang_mtls::{ControllerKey, ProofError, SubjectName};
use gang_spi::{
    Accepted, AuthenticationPort, CacheStorePort, CertificateSignerPort, ClientCertificates,
    ControllerConnections, ControllerId, ControllerReady, ControllerSpec, CredentialLifecyclePort,
    Lease, Listening, ObservationSink, Observations, PortError, ProvisionerPort, RevocationEpoch,
    SchedulerPort, Scheme, ServerEndpoints, StateRepositoryPort,
};
use gang_tls::{
    Alpn, ClientAuth, HttpClient, HttpProtocol, HttpSettings, HttpTransport, RevocationSet,
    ServerTrust,
};
use gang_types::{CaPin, Isa, NonZeroBytes, NonZeroDuration, Url};
use tokio::sync::oneshot;

use crate::api::{Api, ApiParts};
use crate::authorizer::Authorizer;
use crate::builds::BuildsCell;
use crate::builds_forward::{Forwarding, ProofSource};
use crate::client_address::TrustedProxies;
use crate::composite::PublicService;
use crate::control::ControlLoop;
use crate::core::Core;
use crate::dashboard::Dashboard;
use crate::deployment::{Binder, Deployment, LeaderListeners, Listeners, Parts, Stack};
use crate::endpoint::{EndpointRole, ExecutionEndpoint, PublicEndpoint, WorkerEndpoint};
use crate::errors::ServerError;
use crate::gate::Gate;
use crate::health::{HealthLimits, Readiness, serve_health};
use crate::host::Host;
use crate::info::{advertise_device_login, advertise_execution, wire_server};
use crate::ingest::Ingest;
use crate::leader_routes::LeaderRoutes;
use crate::leadership::{Leadership, Leading};
use crate::listeners::{Accepting, TlsPolicy};
use crate::login::SessionRoutes;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::revocation_refresh::{purge_expired_records, refresh_revocations};
use crate::security::{Security, TlsIdentity};
use crate::settings::{LoginSettings, ServerSettings, TransportSettings};

impl From<&ServerSettings> for HealthLimits {
    fn from(settings: &ServerSettings) -> Self {
        Self {
            max_connections: settings.health_max_connections,
            max_header_size: settings.health_max_header_size,
            accept_error_backoff: settings.accept_error_backoff,
        }
    }
}

/// Builds the advertised resource from the prepared host and client authentication, selecting the
/// dedicated execution endpoint when present.
fn server_info<K: Stack>(
    core: &Core<K>,
    isa: Isa,
    trust: EndpointTrust,
    execution: Option<&ExecutionEndpoint>,
) -> Result<gang_wire::Server<serde_json::Value>, ServerError> {
    let mut info = wire_server(
        &core.host.endpoint,
        isa,
        trust,
        core.host.security.certificate(),
        core.clients.scheme(),
        &core.host.providers,
    )?;
    if let Some(execution) = execution {
        advertise_execution(&mut info, execution.remote());
    }
    Ok(info)
}
/// Prepares the advertised server and its revoked certificates.
async fn initial_server_info<K: Stack>(
    core: &Core<K>,
    isa: Isa,
    trust: EndpointTrust,
    execution: Option<&ExecutionEndpoint>,
) -> Result<(gang_wire::Server<serde_json::Value>, RevocationSet), ServerError> {
    let info = server_info(core, isa, trust, execution)?;
    let certificates = revoked_certificates(core).await?;
    Ok((info, certificates))
}

async fn optional_health<L: gang_tls::Listener, C: Clock>(
    listener: Option<L>,
    readiness: Readiness,
    clock: &C,
    limits: HealthLimits,
) -> Infallible {
    match listener {
        Some(listener) => serve_health(listener, readiness, clock, limits).await,
        None => std::future::pending().await,
    }
}

/// Builds and drives all controller-wide background tasks until one terminates.
async fn run_background<K: Stack, L: gang_tls::Listener>(
    core: &Core<K>,
    revocations: (&RevocationSet, RevocationEpoch, NonZeroDuration),
    builds: &BuildsCell,
    observations: (Observations, Option<Forwarding<LeaderRoutes>>),
    leading: &Leading,
    health: Option<L>,
    health_settings: (Readiness, HealthLimits),
) -> Infallible {
    let (revoked_certificates, revocation_epoch, revocation_refresh_interval) = revocations;
    let (observations, forwarding) = observations;
    let (readiness, health_limits) = health_settings;
    let refresh = refresh_revocations(
        core,
        revoked_certificates,
        revocation_epoch,
        revocation_refresh_interval,
    );
    let purge = purge_expired_records(core, revocation_refresh_interval);
    let record = builds.record_or_forward(
        observations,
        &*core.host.clock,
        &*core.host.signer,
        leading,
        forwarding,
    );
    let health = optional_health(health, readiness, &*core.host.clock, health_limits);
    tokio::select! {
        never = refresh => never,
        never = record => never,
        never = purge => never,
        never = health => never,
    }
}

fn relay_trust(
    endpoint_trust: EndpointTrust,
    ca_pinned: bool,
    certificate: Option<&gang_types::CertificatePem>,
) -> ServerTrust<'_> {
    match certificate {
        Some(ca) if endpoint_trust == EndpointTrust::Ca || ca_pinned => ServerTrust::Ca(ca),
        _ => ServerTrust::System,
    }
}

/// Builds the follower's HTTP transport for the dedicated execution endpoint.
fn leader_routes<K: Stack>(
    execution: Option<&ExecutionEndpoint>,
    core: &Core<K>,
    trust: (EndpointTrust, bool),
    leading: &Leading,
    timeout: NonZeroDuration,
) -> Result<Option<LeaderRoutes>, ServerError> {
    execution
        .map(|execution| {
            let certificate = core.host.security.certificate();
            let transport = if execution.url().scheme() == "http" {
                HttpTransport::Plain
            } else {
                let trust = relay_trust(trust.0, trust.1, certificate.as_ref());
                HttpTransport::Tls { trust }
            };
            let upstream = HttpClient::try_new(&HttpSettings {
                transport,
                protocol: HttpProtocol::Negotiated,
                timeout,
                max_body: NonZeroBytes::kib(1),
                clock: core.host.clock.clone(),
            })
            .map_err(ServerError::LeaderClient)?;
            Ok(LeaderRoutes::new(
                leading.clone(),
                upstream,
                execution.url().clone(),
            ))
        })
        .transpose()
}

/// Shared signal ending all listener streams during hand-off.
type Stop = Shared<Map<oneshot::Receiver<()>, fn(Result<(), oneshot::error::RecvError>)>>;
/// The connection stream handed to the controller.
type Connections = BoxStream<'static, Accepted>;
/// The controller's typed set of listening streams.
type ControllerListening<D> = ControllerConnections<
    Gate<D, <D as Stack>::Clients>,
    Gate<D, <D as Stack>::Workers>,
    PublicService<D>,
>;

/// Treat a sent or dropped stop signal alike.
fn stop_fired(_: Result<(), oneshot::error::RecvError>) {}

fn stop_channel() -> (oneshot::Sender<()>, Stop) {
    let (sender, stopped) = oneshot::channel::<()>();
    let stop = stopped
        .map(stop_fired as fn(Result<(), oneshot::error::RecvError>))
        .shared();
    (sender, stop)
}

/// Channels the leader fills once its listeners are bound.
struct LeadChannels {
    execution: oneshot::Sender<Connections>,
    worker: oneshot::Sender<Connections>,
    execution_ended: oneshot::Sender<()>,
    worker_ended: oneshot::Sender<()>,
}

/// Notifications of listeners ending unexpectedly.
struct ListenerEnds {
    public: oneshot::Receiver<()>,
    execution: oneshot::Receiver<()>,
    worker: oneshot::Receiver<()>,
}

/// Builds the controller streams and channels for leader-only streams.
fn controller_connections<D: Deployment>(
    public: Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
    public_gate: Gate<D, D::Clients>,
    worker_gate: Gate<D, D::Workers>,
    api: PublicService<D>,
    dedicated: bool,
    stop: Stop,
) -> (ControllerListening<D>, LeadChannels, ListenerEnds) {
    let (public_ended, public_end) = oneshot::channel();
    let (execution_ended, execution_end) = oneshot::channel();
    let (worker_ended, worker_end) = oneshot::channel();
    let (execution_tx, execution_rx) = oneshot::channel();
    let (worker_tx, worker_rx) = oneshot::channel();
    let execution = dedicated.then(|| Listening {
        connections: deferred_connections(execution_rx, stop.clone()),
        gate: public_gate.clone().executing(),
    });
    let connections = ControllerConnections {
        public: Listening {
            connections: public
                .into_stream(public_ended)
                .take_until(stop.clone())
                .boxed(),
            gate: public_gate,
        },
        execution,
        worker: Listening {
            connections: deferred_connections(worker_rx, stop),
            gate: worker_gate,
        },
        api,
    };
    (
        connections,
        LeadChannels {
            execution: execution_tx,
            worker: worker_tx,
            execution_ended,
            worker_ended,
        },
        ListenerEnds {
            public: public_end,
            execution: execution_end,
            worker: worker_end,
        },
    )
}

/// A leader-only stream remains empty if its sender is dropped before binding.
fn deferred_connections(receiver: oneshot::Receiver<Connections>, stop: Stop) -> Connections {
    futures::stream::once(async move {
        receiver
            .await
            .unwrap_or_else(|_| futures::stream::empty().boxed())
    })
    .flatten()
    .take_until(stop)
    .boxed()
}

/// The shared ports and settings the lead phase borrows.
struct LeadContext<'a, D: Deployment> {
    core: &'a Core<D>,
    provisioner: &'a D::Provisioner,
    fleets: &'a Fleets<D::Plugins>,
    scheduler: &'a D::Scheduler,
    worker_endpoint: &'a WorkerEndpoint,
    dashboard: Option<&'a Dashboard>,
    builds: &'a BuildsCell,
    leading: &'a Leading,
    readiness: &'a Readiness,
    leadership: &'a Leadership<'a, D::Repository, D::Clock>,
    revoked_certificates: &'a RevocationSet,
    backoff: NonZeroDuration,
    poll_interval: NonZeroDuration,
    sidecar_retry_interval: NonZeroDuration,
}

/// Acquires and holds the lease while the leader's work is running.
async fn run_leader<D: Deployment>(
    context: LeadContext<'_, D>,
    listeners: LeaderListeners<D::Binder>,
    channels: LeadChannels,
    initial_lease: Option<Lease>,
    (retiring, stop): (impl Future<Output = ()>, Stop),
) -> Result<(), ServerError> {
    let mut lease = if let Some(lease) = initial_lease {
        lease
    } else {
        tokio::select! {
            biased;
            () = retiring => return Ok(()),
            () = stop.clone() => return Ok(()),
            lease = context.leadership.acquire() => lease,
        }
    };
    context.leading.set();
    let result = tokio::select! {
        biased;
        () = stop.clone() => Ok(()),
        () = context.leadership.hold(&mut lease) => Err(ServerError::LeadershipLost),
        result = lead_work(&context, listeners, channels, stop.clone()) => result,
    };
    if !matches!(&result, Err(ServerError::LeadershipLost)) {
        context.leadership.release(&lease).await;
    }
    result
}

/// Binds the leader listeners and runs its control loop, sidecar and dashboard work.
async fn lead_work<D: Deployment>(
    context: &LeadContext<'_, D>,
    listeners: LeaderListeners<D::Binder>,
    channels: LeadChannels,
    stop: Stop,
) -> Result<(), ServerError> {
    if let Some(dashboard) = context.dashboard
        && let Err(error) = dashboard.reload_from(&context.core.host.repository).await
    {
        tracing::warn!(%error, "dashboard history reload failed during promotion");
    }
    let worker = match listeners {
        LeaderListeners::OnPublic { worker } => worker,
        LeaderListeners::Dedicated {
            execution,
            worker,
            binder,
        } => {
            let listener = binder
                .bind(execution)
                .await
                .map_err(|source| ServerError::Bind {
                    endpoint: "execution",
                    source,
                })?;
            let listener = open_listener::<D>(
                listener,
                EndpointRole::Execution,
                context.core,
                context.revoked_certificates,
                context.backoff,
            )
            .await?;
            let _ = channels
                .execution
                .send(listener.into_stream(channels.execution_ended));
            binder
                .bind(worker)
                .await
                .map_err(|source| ServerError::Bind {
                    endpoint: "worker",
                    source,
                })?
        }
    };
    let worker = open_listener::<D>(
        worker,
        EndpointRole::Worker,
        context.core,
        context.revoked_certificates,
        context.backoff,
    )
    .await?;
    let _ = channels
        .worker
        .send(worker.into_stream(channels.worker_ended));
    context.readiness.lead();
    let sidecar = retry_sidecar(context);
    let clock = &context.core.host.clock;
    let control = ControlLoop::new(
        context.fleets,
        context.poll_interval,
        context.provisioner,
        context.scheduler,
        &context.core.host.repository,
        clock,
        context.leadership.fence().clone(),
    );
    let control = match context.dashboard {
        Some(dashboard) => control.with_publisher(dashboard.publisher(context.builds.clone())),
        None => control,
    };
    tokio::select! {
        biased;
        () = stop => Ok(()),
        never = sidecar => match never {},
        never = control.run() => match never {},
        never = dashboard_tasks(context.dashboard, context.scheduler, clock,
            &context.core.host.revoked_principals) => match never {},
    }
}

/// Reattaches an unavailable or stopped provisioner sidecar while leadership is held.
async fn retry_sidecar<D: Deployment>(context: &LeadContext<'_, D>) -> Infallible {
    let endpoints = ServerEndpoints {
        public: context.core.host.endpoint.url().clone(),
        worker: context.worker_endpoint.url().clone(),
    };
    loop {
        let error = match context.provisioner.attach(&endpoints).await {
            Ok(Some(sidecar)) => sidecar.serve().await,
            Ok(None) => return std::future::pending().await,
            Err(error) => error,
        };
        tracing::warn!(error = ?error, "provisioner sidecar failed; retrying attachment");
        context
            .core
            .host
            .clock
            .sleep(context.sidecar_retry_interval.get())
            .await;
    }
}

/// Waits for leadership before serving the combined public listener, or stops on shutdown.
/// `background` (every controller's work, leading or not) runs meanwhile.
async fn initial_lease<D: Deployment>(
    listeners: &LeaderListeners<D::Binder>,
    leadership: &Leadership<'_, D::Repository, D::Clock>,
    shutdown: impl Future<Output = ()>,
    background: Pin<&mut impl Future<Output = Infallible>>,
) -> Option<Option<Lease>> {
    if matches!(listeners, LeaderListeners::Dedicated { .. }) {
        return Some(None);
    }
    tokio::select! {
        biased;
        () = shutdown => None,
        lease = leadership.acquire() => Some(Some(lease)),
        never = background => match never {},
    }
}

/// Opens the public listener, releasing an already-acquired lease if opening fails.
async fn open_public<D: Deployment>(
    public: D::Listener,
    core: &Core<D>,
    certificates: &RevocationSet,
    backoff: NonZeroDuration,
    lease: Option<&Lease>,
    leadership: &Leadership<'_, D::Repository, D::Clock>,
) -> Result<Accepting<D::Listener, Arc<D::Signer>, D::Clock>, ServerError> {
    match open_listener::<D>(public, EndpointRole::Public, core, certificates, backoff).await {
        Ok(public) => Ok(public),
        Err(error) => {
            if let Some(lease) = lease {
                leadership.release(lease).await;
            }
            Err(error)
        }
    }
}

/// Validates the dedicated endpoint against the listener shape and transport.
fn execution_endpoint(
    dedicated: bool,
    endpoint: Option<Url>,
    transport: &TransportSettings,
) -> Result<Option<ExecutionEndpoint>, ServerError> {
    if dedicated != endpoint.is_some() {
        return Err(ServerError::ExecutionEndpoint);
    }
    endpoint
        .map(|url| ExecutionEndpoint::try_new(url, matches!(transport, TransportSettings::Tls(_))))
        .transpose()
}

/// Reads the epoch before the initial principal snapshot, so concurrent changes are refreshed.
async fn initial_revocations<K: Stack>(
    repository: &K::Repository,
) -> Result<(RevocationEpoch, RevokedPrincipals), ServerError> {
    let epoch = repository
        .revocation_epoch()
        .await
        .map_err(ServerError::Revocations)?;
    let principals = load_revoked_principals::<K>(repository).await?;
    Ok((epoch, principals))
}

/// Drives the controller until shutdown or failure, then hands off and drains, running
/// `background` throughout.
async fn drive_controller(
    shutdown: impl Future<Output = ()>,
    stop: impl FnOnce(),
    dashboard: Option<&Dashboard>,
    mut controller: Pin<&mut impl Future<Output = Result<(), PortError>>>,
    mut leader: Pin<&mut impl Future<Output = Result<(), ServerError>>>,
    ends: ListenerEnds,
    mut background: Pin<&mut impl Future<Output = Infallible>>,
) -> Result<(), ServerError> {
    let mut public_stopped = Box::pin(listener_stopped("public", ends.public));
    let mut execution_stopped = Box::pin(listener_stopped("execution", ends.execution));
    let mut worker_stopped = Box::pin(listener_stopped("worker", ends.worker));
    let mut leader_finished = false;
    let mut controller_finished = false;
    let result = tokio::select! {
        biased;
        () = shutdown => Ok(()),
        result = &mut leader => {
            leader_finished = true;
            result
        },
        error = &mut public_stopped => Err(error),
        error = &mut execution_stopped => Err(error),
        error = &mut worker_stopped => Err(error),
        result = &mut controller => {
            controller_finished = true;
            match result {
                Ok(()) => Err(ServerError::ControllerEnded),
                Err(error) => Err(ServerError::Controller(error)),
            }
        },
        never = &mut background => match never {},
    };
    stop();
    if let Some(dashboard) = dashboard {
        dashboard.shutdown();
    }
    let handoff = async {
        let drain = async {
            if controller_finished {
                Ok(())
            } else {
                controller.await.map_err(ServerError::Controller)
            }
        };
        let release = async {
            if !leader_finished {
                let _ = leader.await;
            }
        };
        let (drained, ()) = tokio::join!(biased; drain, release);
        drained
    };
    let drained = tokio::select! {
        result = handoff => result,
        never = &mut background => match never {},
    };
    result.and(drained)
}

/// The composed server: the controller's scheduler and provisioner, the control loop, the gates that
/// admit requests on each listener, and the Gang API they hand requests to.
pub struct Server<D: Deployment> {
    core: Arc<Core<D>>,
    provisioner: D::Provisioner,
    fleets: Fleets<D::Plugins>,
    cache: D::Cache,
    scheduler: D::Scheduler,
    listeners: Listeners<D::Binder>,
    health: Option<D::Listener>,
    public: PublicService<D>,
    public_gate: Gate<D, D::Clients>,
    worker_gate: Gate<D, D::Workers>,
    revoked_certificates: RevocationSet,
    worker_endpoint: WorkerEndpoint,
    controller: ControllerId,
    leadership: LeadershipConfig,
    leading: Leading,
    readiness: Readiness,
    revocation_epoch: RevocationEpoch,
    revocation_refresh_interval: NonZeroDuration,
    poll_interval: NonZeroDuration,
    client_action_timeout: NonZeroDuration,
    accept_error_backoff: NonZeroDuration,
    health_limits: HealthLimits,
    dashboard: Option<Dashboard>,
    builds: BuildsCell,
    observation_sink: ObservationSink,
    observations: Observations,
    forwarding: Option<Forwarding<LeaderRoutes>>,
}

impl<D: Deployment> Server<D> {
    /// Prepares the server from its ports and settings: checks the worker URL's scheme,
    /// establishes the certificate authority when TLS is on, loads the revoked principals and
    /// certificates, and builds the core, the two gates and the Gang API. Nothing listens until
    /// [`Server::run`].
    ///
    /// # Errors
    ///
    /// Returns an error when the worker URL has the wrong scheme, a TLS listener has no usable
    /// host name, the certificate authority cannot be established, the revoked principals or
    /// certificates cannot be loaded from the ports, the server information or controller proof
    /// key cannot be built, or the request size limit cannot support observation forwarding.
    pub fn prepare(
        parts: Parts<D>,
        settings: ServerSettings,
    ) -> impl Future<Output = Result<Self, ServerError>> {
        Self::prepare_with(parts, settings, leader_routes)
    }

    async fn prepare_with(
        parts: Parts<D>,
        mut settings: ServerSettings,
        build_routes: impl FnOnce(
            Option<&ExecutionEndpoint>,
            &Core<D>,
            (EndpointTrust, bool),
            &Leading,
            NonZeroDuration,
        ) -> Result<Option<LeaderRoutes>, ServerError>,
    ) -> Result<Self, ServerError> {
        let execution_endpoint = execution_endpoint(
            matches!(&parts.listeners.leader, LeaderListeners::Dedicated { .. }),
            settings.execution_endpoint.take(),
            &settings.transport,
        )?;
        let endpoint_trust = (
            settings.transport.endpoint_trust(),
            matches!(&settings.transport, TransportSettings::Tls(tls) if tls.ca_pinned),
        );
        Self::assemble(
            parts,
            settings,
            execution_endpoint,
            endpoint_trust,
            build_routes,
        )
        .await
    }

    /// Establishes the transport and composes the server after endpoint validation.
    async fn assemble(
        parts: Parts<D>,
        settings: ServerSettings,
        execution_endpoint: Option<ExecutionEndpoint>,
        endpoint_trust: (EndpointTrust, bool),
        build_routes: impl FnOnce(
            Option<&ExecutionEndpoint>,
            &Core<D>,
            (EndpointTrust, bool),
            &Leading,
            NonZeroDuration,
        ) -> Result<Option<LeaderRoutes>, ServerError>,
    ) -> Result<Self, ServerError> {
        let health_limits = HealthLimits::from(&settings);
        let (worker_endpoint, mut security, signer) = prepare_transport::<D>(
            settings.transport,
            parts.scheduler.endpoints().worker,
            &settings.endpoint,
            parts.signer,
            &parts.clock,
        )
        .await?;
        security.set_execution(execution_endpoint.as_ref())?;
        let (revocation_epoch, revoked_principals) =
            initial_revocations::<D>(&parts.repository).await?;
        let core = Core::new(
            Host {
                repository: parts.repository,
                signer,
                security,
                providers: parts.providers,
                clock: parts.clock,
                endpoint: settings.endpoint,
                revoked_principals,
                sightings: principal_sightings(
                    settings.principal_seen_interval,
                    settings.max_tracked_principals,
                ),
            },
            parts.clients,
            parts.workers,
        );
        let execution = execution_endpoint.as_ref();
        let initial = initial_server_info(&core, settings.default_isa, endpoint_trust.0, execution);
        let (info, revoked_certificates) = initial.await?;
        let leading = Leading::default();
        let (builds, observation_sink, observations) = BuildsCell::channel(&settings.builds);
        let dashboard = start_dashboard(
            settings.dashboard,
            settings.provisioner,
            settings.poll_interval.get(),
            &core.host,
        )
        .await?;
        let leader_routes = build_routes(
            execution,
            &core,
            endpoint_trust,
            &leading,
            settings.limits.request_timeout,
        )?;
        let (ingest, forwarding) = build_forwarding(
            (
                &settings.leadership,
                &settings.builds,
                settings.limits.max_request_size,
            ),
            &parts.controller,
            leader_routes.as_ref(),
            &observation_sink,
            &leading,
            &core.host,
        )
        .await?;
        let public = public_service(
            &core,
            (info, &revoked_certificates),
            (settings.roles, settings.limits, &settings.rate_limit),
            (dashboard.as_ref(), settings.login),
            (leader_routes, ingest),
            &builds,
        );
        Ok(Self {
            provisioner: parts.provisioner,
            fleets: parts.fleets,
            cache: parts.cache,
            scheduler: parts.scheduler,
            listeners: parts.listeners,
            health: None,
            public,
            public_gate: public_gate(&core, &settings.rate_limit),
            worker_gate: Gate::worker(Arc::clone(&core)),
            core,
            revoked_certificates,
            worker_endpoint,
            controller: parts.controller,
            leadership: settings.leadership,
            leading,
            readiness: Readiness::default(),
            revocation_epoch,
            revocation_refresh_interval: settings.revocation_refresh_interval,
            poll_interval: settings.poll_interval,
            client_action_timeout: settings.client_action_timeout,
            accept_error_backoff: settings.accept_error_backoff,
            health_limits,
            dashboard,
            builds,
            observation_sink,
            observations,
            forwarding,
        })
    }

    /// Adds an optional plaintext listener for load-balancer health checks.
    #[must_use]
    pub fn with_health(mut self, health: Option<D::Listener>) -> Self {
        self.health = health;
        self
    }

    /// The pin of the certificate authority the server issues certificates under; `None` when the
    /// transport is plaintext.
    #[must_use]
    pub fn ca_pin(&self) -> Option<CaPin> {
        self.core.host.security.ca_pin()
    }

    fn warn_startup(&self) {
        let core = &*self.core;
        warn_unauthenticated(
            !core.host.security.is_tls(),
            core.host.endpoint.url(),
            self.worker_endpoint.url(),
            core.clients.scheme(),
            core.workers.scheme(),
        );
    }

    /// Runs the server until `shutdown` completes or a component fails. On shutdown, each
    /// viewer is told the server is shutting down, then the connections are drained. Dropping
    /// the returned future stops every connection and task the server started.
    /// `ready` is signalled once the embedded controller is built and serves; a run that fails
    /// first drops it unsent.
    ///
    /// Under plaintext, and for every surface whose authentication plugin presents
    /// [`Scheme::None`], a warning is logged once at start.
    ///
    /// # Errors
    ///
    /// Returns an error when the stores cannot be described, a TLS listener cannot be secured,
    /// or the controller fails or returns without being asked to stop.
    /// A listener that stops accepting connections before shutdown ends the run with
    /// [`ServerError::ListenerEnded`].
    pub async fn run(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
        ready: ControllerReady,
    ) -> Result<(), ServerError> {
        let core = &*self.core;
        self.warn_startup();
        let content_store = self.cache.content_store().map_err(ServerError::Cache)?;
        let action_store = self.cache.action_store().map_err(ServerError::Cache)?;
        let spec = ControllerSpec {
            content_store: &content_store,
            action_store: &action_store,
            fleets: self.fleets.catalog(),
            max_workers: self.fleets.max_instances(),
            client_action_timeout: self.client_action_timeout.get(),
            observations: &self.observation_sink,
        };
        let shutdown = shutdown.shared();
        let (stop_sender, stop) = stop_channel();
        let sidecar_retry_interval = self.leadership.renew_interval;
        let leadership = Leadership::new(
            &core.host.repository,
            &core.host.clock,
            &self.controller,
            self.leadership,
        );
        // Every controller refreshes revocations and drains its admitted observations.
        let mut background = Box::pin(run_background(
            core,
            (
                &self.revoked_certificates,
                self.revocation_epoch,
                self.revocation_refresh_interval,
            ),
            &self.builds,
            (self.observations, self.forwarding),
            &self.leading,
            self.health,
            (self.readiness.clone(), self.health_limits),
        ));
        let Listeners { public, leader } = self.listeners;
        let Some(lease) =
            initial_lease::<D>(&leader, &leadership, shutdown.clone(), background.as_mut()).await
        else {
            return Ok(());
        };
        let public = open_public::<D>(
            public,
            core,
            &self.revoked_certificates,
            self.accept_error_backoff,
            lease.as_ref(),
            &leadership,
        )
        .await?;
        self.readiness.serve();
        let (connections, channels, ends) = controller_connections::<D>(
            public,
            self.public_gate,
            self.worker_gate,
            self.public,
            matches!(&leader, LeaderListeners::Dedicated { .. }),
            stop.clone(),
        );
        let dashboard = self.dashboard.as_ref();
        let scheduler = &self.scheduler;
        let mut controller = Box::pin(scheduler.run_controller(&spec, connections, ready));
        let context = LeadContext {
            core,
            provisioner: &self.provisioner,
            fleets: &self.fleets,
            scheduler,
            worker_endpoint: &self.worker_endpoint,
            dashboard,
            builds: &self.builds,
            leading: &self.leading,
            readiness: &self.readiness,
            leadership: &leadership,
            revoked_certificates: &self.revoked_certificates,
            backoff: self.accept_error_backoff,
            poll_interval: self.poll_interval,
            sidecar_retry_interval,
        };
        let mut leader_task = Box::pin(run_leader(
            context,
            leader,
            channels,
            lease,
            (shutdown.clone(), stop),
        ));
        drive_controller(
            shutdown,
            || {
                let _ = stop_sender.send(());
                self.readiness.stop();
            },
            dashboard,
            controller.as_mut(),
            leader_task.as_mut(),
            ends,
            background.as_mut(),
        )
        .await
    }
}

async fn build_forwarding<K: Stack>(
    settings: (&LeadershipConfig, &gang_config::BuildsConfig, usize),
    controller: &ControllerId,
    routes: Option<&LeaderRoutes>,
    sink: &ObservationSink,
    leading: &Leading,
    host: &Host<K>,
) -> Result<(Option<Ingest>, Option<Forwarding<LeaderRoutes>>), ServerError> {
    let Some(uplink) = routes else {
        return Ok((None, None));
    };
    let public_key = host
        .signer
        .public_key()
        .await
        .map_err(ProofError::Sign)
        .map_err(ServerError::ControllerProofKey)?;
    let key = ControllerKey::from_spki(&public_key).map_err(ServerError::ControllerProofKey)?;
    let issuer = host.endpoint.url().origin().ascii_serialization();
    let (leadership, builds, max_request_size) = settings;
    let ingest = Ingest::new(
        sink.clone(),
        leading.clone(),
        key,
        issuer.clone(),
        leadership.clock_skew,
        leadership.ttl,
        builds.max_live.get(),
    );
    let forwarding = Forwarding::try_new(
        uplink.clone(),
        ProofSource::new(controller.clone(), issuer, leadership.ttl),
        leadership.renew_interval.get(),
        builds.max_live,
        max_request_size,
    )
    .map_err(ServerError::Forwarding)?;
    Ok((Some(ingest), Some(forwarding)))
}

/// Configures bounded tracking of principal sightings.
fn principal_sightings(
    interval: NonZeroDuration,
    max_tracked: std::num::NonZeroUsize,
) -> PrincipalSightings {
    PrincipalSightings::new(interval.get(), max_tracked)
}

/// Builds the public gate with the configured trusted proxies.
fn public_gate<K: Stack>(
    core: &Arc<Core<K>>,
    rate_limit: &gang_config::RateLimitConfig,
) -> Gate<K, K::Clients> {
    let proxies = Arc::new(TrustedProxies::new(rate_limit.trusted_proxies.clone()));
    Gate::public(Arc::clone(core), proxies)
}

/// Validates the worker endpoint and establishes the listener security policy.
async fn prepare_transport<K: Stack>(
    transport: TransportSettings,
    worker_url: Url,
    endpoint: &PublicEndpoint,
    signer: K::Signer,
    clock: &Arc<K::Clock>,
) -> Result<(WorkerEndpoint, Security<K>, Arc<K::Signer>), ServerError> {
    let tls = matches!(transport, TransportSettings::Tls(_));
    let worker_endpoint = WorkerEndpoint::try_new(worker_url, tls)?;
    let signer = Arc::new(signer);
    let security = Security::<K>::establish(
        transport,
        endpoint,
        &worker_endpoint,
        Arc::clone(&signer),
        Arc::clone(clock),
    )
    .await?;
    Ok((worker_endpoint, security, signer))
}

/// Builds the public API and mounts browser routes only when the dashboard is enabled.
fn public_service<K: Stack>(
    core: &Arc<Core<K>>,
    initial: (gang_wire::Server<serde_json::Value>, &RevocationSet),
    settings: (
        Vec<gang_config::RoleConfig>,
        crate::settings::RequestLimits,
        &gang_config::RateLimitConfig,
    ),
    browser: (Option<&Dashboard>, Option<LoginSettings>),
    routing: (Option<LeaderRoutes>, Option<Ingest>),
    builds: &BuildsCell,
) -> PublicService<K> {
    let (info, revoked_certificates) = initial;
    let (roles, limits, rate_limit) = settings;
    let (dashboard, login) = browser;
    let (leader_routes, ingest) = routing;
    let mut parts = ApiParts {
        core: Arc::clone(core),
        revocations: Revocations::new(Arc::clone(core), revoked_certificates.clone()),
        info,
        authorizer: Authorizer::new(roles),
        limits,
        dashboard: dashboard.map(Dashboard::api),
        leader_routes,
        ingest,
        builds: builds.clone(),
        limiter: Arc::new(RateLimiter::new(rate_limit, Arc::clone(&core.host.clock))),
        login_provider: None,
        device: None,
    };
    let ui = parts.dashboard.as_ref().map(|_| {
        SessionRoutes::new(
            Arc::clone(&parts.core),
            login,
            Arc::clone(&parts.limiter),
            parts.limits.max_request_size,
        )
    });
    parts.login_provider = ui.as_ref().and_then(SessionRoutes::login_provider).cloned();
    parts.device = ui.as_ref().and_then(SessionRoutes::device);
    if let Some(device) = &parts.device {
        advertise_device_login(&mut parts.info, &device.provider);
    }
    PublicService {
        api: Api::new(parts),
        ui,
    }
}

/// Resolves with [`ServerError::ListenerEnded`] once the `endpoint` listener's connection stream
/// has ended; never when the stream is dropped without ending, as on shutdown.
async fn listener_stopped(endpoint: &'static str, ended: oneshot::Receiver<()>) -> ServerError {
    if ended.await.is_err() {
        std::future::pending::<()>().await;
    }
    ServerError::ListenerEnded { endpoint }
}

/// Starts the dashboard when it is enabled; a disabled dashboard builds nothing.
async fn start_dashboard<K: Stack>(
    config: DashboardConfig,
    provisioner: String,
    poll_interval: Duration,
    host: &Host<K>,
) -> Result<Option<Dashboard>, ServerError> {
    if !config.enabled {
        return Ok(None);
    }
    Dashboard::start(
        config,
        provisioner,
        poll_interval,
        host.revoked_principals.epoch(),
        &host.clock,
        &host.repository,
    )
    .await
    .map(Some)
}

/// The dashboard's background work: the actions refresher and the principals forwarder. Never
/// completes; without a dashboard it waits forever.
async fn dashboard_tasks<S: SchedulerPort, C: Clock>(
    dashboard: Option<&Dashboard>,
    scheduler: &S,
    clock: &C,
    revoked_principals: &RevokedPrincipals,
) -> Infallible {
    let Some(dashboard) = dashboard else {
        return std::future::pending().await;
    };
    tokio::select! {
        never = dashboard.refresher(scheduler, clock).run() => never,
        never = dashboard.forward_principals(revoked_principals) => never,
    }
}

/// The principals the repository has revoked, which the host refuses.
async fn load_revoked_principals<K: Stack>(
    repository: &K::Repository,
) -> Result<RevokedPrincipals, ServerError> {
    let revoked = repository
        .revoked_principals()
        .await
        .map_err(ServerError::Revocations)?;
    Ok(RevokedPrincipals::from_identities(
        revoked.into_iter().map(|principal| principal.identity),
    ))
}

/// The serials of the certificates the worker plugin reports revoked; none when it issues no
/// certificates. Only workers present certificates: a clients plugin that presents itself as
/// mTLS is refused at startup ([`ServerError::ClientScheme`]).
async fn revoked_certificates<K: Stack>(core: &Core<K>) -> Result<RevocationSet, ServerError> {
    let serials = if core.workers.capabilities().certificates {
        core.workers
            .revoked_serials(&core.host)
            .await
            .map_err(ServerError::Revocations)?
    } else {
        Vec::new()
    };
    Ok(RevocationSet::from_serials(serials))
}

/// The listeners that accept unauthenticated connections, as the startup warning names them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnauthenticatedListeners<'a> {
    /// Both endpoints, public first, when the server serves without TLS.
    plaintext: Option<(&'a Url, &'a Url)>,
    /// The surfaces whose authentication is [`Scheme::None`].
    surfaces: Vec<EndpointRole>,
}

/// Decides what the startup warning reports: whether the server serves without TLS, and which
/// surfaces have their authentication disabled.
fn unauthenticated_listeners<'a>(
    plaintext: bool,
    endpoint: &'a Url,
    worker_endpoint: &'a Url,
    public: Scheme,
    worker: Scheme,
) -> UnauthenticatedListeners<'a> {
    let surfaces = [
        (EndpointRole::Public, public),
        (EndpointRole::Worker, worker),
    ]
    .into_iter()
    .filter(|(_, scheme)| *scheme == Scheme::None)
    .map(|(role, _)| role)
    .collect();
    UnauthenticatedListeners {
        plaintext: plaintext.then_some((endpoint, worker_endpoint)),
        surfaces,
    }
}

/// Logs a warning when the server serves without TLS, and once for each surface whose
/// authentication is disabled.
fn warn_unauthenticated(
    plaintext: bool,
    endpoint: &Url,
    worker_endpoint: &Url,
    public: Scheme,
    worker: Scheme,
) {
    let listeners = unauthenticated_listeners(plaintext, endpoint, worker_endpoint, public, worker);
    if let Some((public, worker)) = listeners.plaintext {
        tracing::warn!(
            public = %public,
            worker = %worker,
            "serving without TLS: the listeners accept unauthenticated connections"
        );
    }
    for role in listeners.surfaces {
        tracing::warn!(surface = %role, "authentication is disabled on this surface");
    }
}

/// Opens one listener under the policy for its endpoint.
async fn open_listener<D: Deployment>(
    listener: D::Listener,
    role: EndpointRole,
    core: &Core<D>,
    revoked_certificates: &RevocationSet,
    accept_error_backoff: NonZeroDuration,
) -> Result<Accepting<D::Listener, Arc<D::Signer>, D::Clock>, ServerError> {
    let Security::Tls(tls) = &core.host.security else {
        return Ok(Accepting::plain(
            listener,
            accept_error_backoff,
            Arc::clone(&core.host.clock),
        ));
    };
    let (name, certificates, alpn) = match role {
        EndpointRole::Public | EndpointRole::Execution => (
            &tls.names.public,
            core.clients.client_certificates(),
            Alpn::H2AndHttp1,
        ),
        EndpointRole::Worker => (
            &tls.names.worker,
            core.workers.client_certificates(),
            Alpn::H2,
        ),
    };
    let mut policy = policy(
        tls,
        name,
        certificates,
        alpn,
        revoked_certificates,
        accept_error_backoff,
    );
    if role == EndpointRole::Execution
        && let Some(execution) = &tls.names.execution
        && !policy.names.contains(execution)
    {
        policy.names.push(execution.clone());
    }
    Accepting::secured(
        listener,
        Arc::clone(&tls.authority),
        policy,
        Arc::clone(&core.host.clock),
    )
    .await
}

/// The TLS policy of one listener: it presents a certificate named `name`, and asks its clients
/// for `certificates` and offers `alpn`.
fn policy<K: Stack>(
    tls: &TlsIdentity<K>,
    name: &SubjectName,
    certificates: ClientCertificates,
    alpn: Alpn,
    revoked_certificates: &RevocationSet,
    accept_error_backoff: NonZeroDuration,
) -> TlsPolicy {
    TlsPolicy {
        names: vec![name.clone()],
        client_auth: client_auth(certificates),
        revoked_certificates: revoked_certificates.clone(),
        certificate_ttl: tls.settings.certificate_ttl,
        reissue_at_percent: tls.settings.reissue_at_percent,
        alpn,
        handshake: tls.settings.handshake,
        accept_error_backoff,
    }
}

/// The client authentication a listener asks of its clients at the TLS layer.
fn client_auth(certificates: ClientCertificates) -> ClientAuth {
    match certificates {
        ClientCertificates::Never => ClientAuth::None,
        ClientCertificates::Optional => ClientAuth::Optional,
        ClientCertificates::Required => ClientAuth::Required,
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::Scheme;
    use gang_types::Url;

    use super::{UnauthenticatedListeners, unauthenticated_listeners};
    use crate::endpoint::EndpointRole;

    struct SignerStack<S>(std::marker::PhantomData<S>);

    impl<S: gang_spi::CertificateSignerPort + 'static> super::Stack for SignerStack<S> {
        type Signer = S;
        type Clock = <crate::test_support_deployment::PlainDeployment as super::Stack>::Clock;
        type Repository =
            <crate::test_support_deployment::PlainDeployment as super::Stack>::Repository;
        type Identity = <crate::test_support_deployment::PlainDeployment as super::Stack>::Identity;
        type Clients = <crate::test_support_deployment::PlainDeployment as super::Stack>::Clients;
        type Workers = <crate::test_support_deployment::PlainDeployment as super::Stack>::Workers;
    }

    #[derive(Debug)]
    struct MalformedKeySigner;

    impl gang_spi::CertificateSignerPort for MalformedKeySigner {
        fn algorithm(&self) -> gang_spi::SignatureAlgorithm {
            gang_spi::SignatureAlgorithm::EcdsaP256Sha256
        }

        async fn public_key(&self) -> Result<Vec<u8>, gang_spi::PortError> {
            Ok(b"not spki".to_vec())
        }

        async fn sign(&self, _message: &[u8]) -> Result<Vec<u8>, gang_spi::PortError> {
            Err(gang_spi::PortError::unavailable("not used"))
        }
    }

    #[tokio::test]
    async fn forwarding_key_errors_fail_preparation() -> gang_testkit::TestResult {
        let settings = crate::test_support::plain_settings();
        let leading = super::Leading::default();
        let client = gang_tls::HttpClient::try_new(&gang_tls::HttpSettings {
            transport: gang_tls::HttpTransport::Plain,
            protocol: gang_tls::HttpProtocol::Negotiated,
            timeout: gang_types::NonZeroDuration::from_secs(5),
            max_body: gang_types::NonZeroBytes::kib(1),
            clock: std::sync::Arc::new(gang_clock::SystemClock::default()),
        })?;
        let routes = super::LeaderRoutes::new(
            leading.clone(),
            client,
            Url::parse("http://gang.example.test:31001/")?,
        );
        let (sink, _observations) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let controller = gang_spi::ControllerId::new("test");
        let (parts, _, _) = crate::test_support::plain_parts();
        let host = super::Host::<SignerStack<gang_testkit::FailingSigner>> {
            repository: parts.repository,
            signer: std::sync::Arc::new(gang_testkit::FailingSigner),
            security: super::Security::Plaintext,
            providers: parts.providers,
            clock: parts.clock,
            endpoint: crate::test_support_deployment::endpoint("http://gang.example.test:31001/"),
            revoked_principals: super::RevokedPrincipals::new(),
            sightings: super::principal_sightings(
                settings.principal_seen_interval,
                settings.max_tracked_principals,
            ),
        };
        let args = (
            &settings.leadership,
            &settings.builds,
            settings.limits.max_request_size,
        );
        let absent =
            super::build_forwarding(args, &controller, None, &sink, &leading, &host).await?;
        assert!(absent.0.is_none() && absent.1.is_none());
        let failing =
            super::build_forwarding(args, &controller, Some(&routes), &sink, &leading, &host).await;
        assert!(matches!(
            failing,
            Err(super::ServerError::ControllerProofKey(
                gang_mtls::ProofError::Sign(_)
            ))
        ));
        let malformed_host = super::Host::<SignerStack<MalformedKeySigner>> {
            repository: host.repository,
            signer: std::sync::Arc::new(MalformedKeySigner),
            security: super::Security::Plaintext,
            providers: host.providers,
            clock: host.clock,
            endpoint: host.endpoint,
            revoked_principals: host.revoked_principals,
            sightings: host.sightings,
        };
        let malformed = super::build_forwarding(
            args,
            &controller,
            Some(&routes),
            &sink,
            &leading,
            &malformed_host,
        )
        .await;
        assert!(matches!(
            malformed,
            Err(super::ServerError::ControllerProofKey(
                gang_mtls::ProofError::Key(_)
            ))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn dedicated_plaintext_prepares_controller_key_before_serving() -> gang_testkit::TestResult
    {
        let (mut parts, _, _) = crate::test_support::plain_parts();
        parts.listeners.leader = super::LeaderListeners::Dedicated {
            execution: "127.0.0.1:31001".parse()?,
            worker: "127.0.0.1:31002".parse()?,
            binder: crate::test_support_deployment::MemoryBinder::default(),
        };
        let mut settings = crate::test_support::plain_settings();
        settings.execution_endpoint = Some(Url::parse("http://gang.example.test:31001/")?);
        let server = super::Server::prepare(parts, settings).await?;
        assert_eq!(server.core.host.signer.public_key_reads(), 1);

        let (parts, _, _) = crate::test_support::plain_parts();
        let server = super::Server::prepare(parts, crate::test_support::plain_settings()).await?;
        assert_eq!(server.core.host.signer.public_key_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn prepare_passes_ca_pin_to_leader_routes() -> gang_testkit::TestResult {
        for pinned in [false, true] {
            let (mut parts, _, _) = crate::test_support::tls_parts();
            parts.listeners.leader = super::LeaderListeners::Dedicated {
                execution: "127.0.0.1:31001".parse()?,
                worker: "127.0.0.1:31002".parse()?,
                binder: crate::test_support_deployment::MemoryBinder::default(),
            };
            let mut settings = crate::test_support::tls_settings();
            settings.execution_endpoint = Some(Url::parse("https://gang.example.test:31001/")?);
            let super::TransportSettings::Tls(tls) = &mut settings.transport else {
                panic!("test settings use TLS");
            };
            tls.endpoint_trust = gang_config::EndpointTrust::System;
            tls.ca_pinned = pinned;
            let observed = std::cell::Cell::new(None);
            let _server = super::Server::prepare_with(
                parts,
                settings,
                |execution, core, trust, leading, timeout| {
                    observed.set(Some(trust));
                    super::leader_routes(execution, core, trust, leading, timeout)
                },
            )
            .await?;
            assert_eq!(
                observed.get(),
                Some((gang_config::EndpointTrust::System, pinned))
            );
        }
        Ok(())
    }

    #[test]
    fn relay_trust_uses_pinned_ca_when_public_endpoint_uses_system_roots() {
        let ca = gang_types::CertificatePem::try_new(
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----",
        )
        .expect("certificate PEM is valid");
        assert!(matches!(
            super::relay_trust(gang_config::EndpointTrust::System, true, Some(&ca)),
            gang_tls::ServerTrust::Ca(_)
        ));
    }

    #[tokio::test]
    async fn shutdown_polls_controller_while_release_is_hung() -> gang_testkit::TestResult {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let readiness = super::Readiness::default();
        readiness.serve();
        readiness.lead();
        let (public, public_end) = tokio::sync::oneshot::channel();
        let (execution, execution_end) = tokio::sync::oneshot::channel();
        let (worker, worker_end) = tokio::sync::oneshot::channel();
        let mut controller = Box::pin(async move {
            let _ = stopped.await;
            drop(listener);
            Ok(())
        });
        let mut leader = Box::pin(std::future::pending());
        let mut background = Box::pin(std::future::pending());
        let ends = super::ListenerEnds {
            public: public_end,
            execution: execution_end,
            worker: worker_end,
        };
        let mut drive = Box::pin(super::drive_controller(
            std::future::ready(()),
            || {
                let _ = stop.send(());
                readiness.stop();
            },
            None,
            controller.as_mut(),
            leader.as_mut(),
            ends,
            background.as_mut(),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        assert_eq!(
            readiness.status(gang_types::health::health_check().leader_path()),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(
            std::net::TcpStream::connect(address).is_err(),
            "listener remains open during release"
        );
        drop((public, execution, worker));
        Ok(())
    }

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url parses")
    }

    #[test]
    fn tls_with_token_clients_and_mtls_workers_warns_about_nothing() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::Token, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: Vec::new(),
            }
        );
    }

    #[test]
    fn plaintext_with_none_on_both_surfaces_lists_both_listeners_with_their_addresses() {
        let public = url("http://gang.example.com:8080");
        let worker = url("grpc://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(true, &public, &worker, Scheme::None, Scheme::None),
            UnauthenticatedListeners {
                plaintext: Some((&public, &worker)),
                surfaces: vec![EndpointRole::Public, EndpointRole::Worker],
            }
        );
    }

    #[test]
    fn tls_with_none_on_clients_lists_only_the_public_listener() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::None, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: vec![EndpointRole::Public],
            }
        );
    }

    #[test]
    fn tls_with_none_on_workers_lists_only_the_worker_listener() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::Token, Scheme::None),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: vec![EndpointRole::Worker],
            }
        );
    }

    #[test]
    fn plaintext_with_token_warns_only_about_the_transport() {
        let public = url("http://gang.example.com:8080");
        let worker = url("grpc://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(true, &public, &worker, Scheme::Token, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: Some((&public, &worker)),
                surfaces: Vec::new(),
            }
        );
    }
}
