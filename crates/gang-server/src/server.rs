//! The server: prepares the authentication core, the gates and the control API, then runs the
//! scheduler's controller and the control loop until shutdown.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::{BoxFuture, Map, Shared};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use gang_clock::Clock;
use gang_config::{DashboardConfig, EndpointTrust, Fleets, LeadershipConfig};
use gang_mtls::SubjectName;
use gang_reconciler::{PassTime, SourceProfiles};
use gang_spi::{
    AuthenticationPort, CacheStorePort, ClientCertificates, ControllerConnections,
    ControllerHostIdentity, ControllerHostPort, ControllerId, ControllerReady, ControllerSpec,
    CredentialLifecyclePort, HostMilestone, Lease, LeaseCandidate, Listening, ObservationSink,
    Observations, PortError, ProvisionerPort, RevocationEpoch, SchedulerPort, SchedulerSnapshot,
    Scheme, ServerEndpoints, StateRepositoryPort,
};
use gang_state::CacheBaseline;
use gang_tls::{Alpn, ClientAuth, RevocationSet};
use gang_types::{CaPin, Isa, NonZeroDuration, Url};
use tokio::sync::oneshot;

use crate::api::{Api, ApiParts};
use crate::authorizer::Authorizer;
use crate::builds::BuildsCell;
use crate::client_address::TrustedProxies;
use crate::composite::PublicService;
use crate::control::{ControlLoop, ControlSettings};
use crate::core::Core;
use crate::dashboard::Dashboard;
use crate::deployment::{Deployment, Listeners, Parts, Stack};
use crate::endpoint::{EndpointRole, PublicEndpoint, WorkerEndpoint};
use crate::errors::ServerError;
use crate::gate::Gate;
use crate::health::{HealthLimits, Readiness, serve_health};
use crate::history::{HistoryError, LearnedHistoryStore};
use crate::host::Host;
use crate::info::{advertise_device_login, wire_server};
use crate::leadership::{Leadership, Leading, LeaseEnd};
use crate::listeners::{Accepting, TlsPolicy};
use crate::login::SessionRoutes;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::revocation_refresh::{
    purge_expired_records, refresh_revocations, synchronize_revocations,
};
use crate::security::{Security, TlsIdentity};
use crate::settings::{LoginSettings, ServerSettings, TransportSettings};

impl From<&ServerSettings> for HealthLimits {
    fn from(settings: &ServerSettings) -> Self {
        Self {
            max_connections: settings.health_max_connections,
            max_header_size: settings.health_max_header_size,
            accept_error_backoff: settings.accept_error_backoff,
            proxy_protocol: settings.proxy_protocol.clone(),
        }
    }
}

/// Builds the advertised resource from the public endpoint.
fn server_info<K: Stack>(
    core: &Core<K>,
    isa: Isa,
    trust: EndpointTrust,
) -> Result<gang_wire::Server<serde_json::Value>, ServerError> {
    wire_server(
        &core.host.endpoint,
        isa,
        trust,
        core.host.security.certificate(),
        core.clients.scheme(),
        &core.host.providers,
    )
}
/// Prepares the advertised server and its revoked certificates.
async fn initial_server_info<K: Stack>(
    core: &Core<K>,
    isa: Isa,
    trust: EndpointTrust,
) -> Result<(gang_wire::Server<serde_json::Value>, RevocationSet), ServerError> {
    let info = server_info(core, isa, trust)?;
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
    observations: Observations,
    health: Option<L>,
    health_settings: (Readiness, HealthLimits),
) -> Infallible {
    let (revoked_certificates, revocation_epoch, revocation_refresh_interval) = revocations;
    let (readiness, health_limits) = health_settings;
    let refresh = refresh_revocations(
        core,
        revoked_certificates,
        revocation_epoch,
        revocation_refresh_interval,
    );
    let purge = purge_expired_records(core, revocation_refresh_interval);
    let record = builds.record(observations, &*core.host.clock);
    let health = optional_health(health, readiness, &*core.host.clock, health_limits);
    tokio::select! {
        never = refresh => never,
        never = record => never,
        never = purge => never,
        never = health => never,
    }
}

/// Shared signal ending all listener streams during hand-off.
type Stop = Shared<Map<oneshot::Receiver<()>, fn(Result<(), oneshot::error::RecvError>)>>;
/// The controller's typed set of listening streams.
type ControllerListening<D> = ControllerConnections<
    Gate<D, <D as Stack>::Clients>,
    Gate<D, <D as Stack>::Workers>,
    PublicService<D>,
>;

type StandbyServices<D> = (
    Gate<D, <D as Stack>::Clients>,
    Gate<D, <D as Stack>::Workers>,
    PublicService<D>,
);
type StandbyServiceRefs<'a, D> = (
    &'a Gate<D, <D as Stack>::Clients>,
    &'a Gate<D, <D as Stack>::Workers>,
    &'a PublicService<D>,
);

/// Treat a sent or dropped stop signal alike.
fn stop_fired(_: Result<(), oneshot::error::RecvError>) {}

fn stop_channel() -> (oneshot::Sender<()>, Stop) {
    let (sender, stopped) = oneshot::channel::<()>();
    let stop = stopped
        .map(stop_fired as fn(Result<(), oneshot::error::RecvError>))
        .shared();
    (sender, stop)
}

/// Notifications of listeners ending unexpectedly.
struct ListenerEnds {
    public: Option<oneshot::Receiver<()>>,
    worker: Option<oneshot::Receiver<()>>,
}

/// Signals used to coordinate listener failure, drain and leader handoff.
struct ControllerSignals<R> {
    listeners: ListenerEnds,
    drained: oneshot::Sender<()>,
    failed: oneshot::Receiver<ServerError>,
    standby: R,
}

/// Signals shared by one leader term and its controller driver.
struct LeaderSignals<'a> {
    prepared: Shared<BoxFuture<'static, bool>>,
    yielded: oneshot::Sender<()>,
    drained: oneshot::Receiver<()>,
    failed: oneshot::Sender<ServerError>,
    voluntary: &'a AtomicBool,
}

type StandbyCompletion<'a> = BoxFuture<'a, Result<(), ServerError>>;
type PendingStandbys<'a> = FuturesUnordered<StandbyCompletion<'a>>;

struct ActiveStandby<'a> {
    stop: Option<oneshot::Sender<()>>,
    listeners: ListenerEnds,
    completion: StandbyCompletion<'a>,
}

enum StandbyEvent {
    Complete(Result<(), ServerError>),
    ListenerEnded(ServerError),
}

impl ActiveStandby<'_> {
    async fn event(&mut self) -> StandbyEvent {
        tokio::select! {
            biased;
            error = listener_stopped("public", &mut self.listeners.public), if self.stop.is_some() =>
                StandbyEvent::ListenerEnded(error),
            error = listener_stopped("worker", &mut self.listeners.worker), if self.stop.is_some() =>
                StandbyEvent::ListenerEnded(error),
            result = &mut self.completion => StandbyEvent::Complete(match result {
                Ok(()) if self.stop.is_some() =>
                    Err(self.listeners.ended().unwrap_or(ServerError::ControllerEnded)),
                result => result,
            }),
        }
    }
}

struct Maintenance<'p, 's, B> {
    background: Pin<&'p mut B>,
    pending: &'p mut PendingStandbys<'s>,
}

impl<B: Future<Output = Infallible>> Maintenance<'_, '_, B> {
    async fn event(&mut self) -> Option<ServerError> {
        tokio::select! {
            result = self.pending.next(), if !self.pending.is_empty() =>
                result.and_then(Result::err),
            never = self.background.as_mut() => match never {},
        }
    }

    async fn failure(&mut self) -> ServerError {
        loop {
            if let Some(error) = self.event().await {
                return error;
            }
        }
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        let mut result = Ok(());
        while !self.pending.is_empty() {
            if let Some(error) = self.event().await {
                result = result.and(Err(error));
            }
        }
        result
    }
}

struct TermExit<'a> {
    end: TermEnd,
    standby: Option<StandbyCompletion<'a>>,
}

impl<'a> TermExit<'a> {
    fn queue_standby(self, pending: &mut PendingStandbys<'a>) -> TermEnd {
        if let Some(completion) = self.standby {
            pending.push(completion);
        }
        self.end
    }
}

struct TermConnections<D: Deployment, R> {
    controller: ControllerListening<D>,
    listeners: ListenerEnds,
    standby: R,
}

/// The secured public and worker listeners.
struct OpenListeners<D: Deployment> {
    public: Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
    worker: Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
}

/// Opens the public listener before the worker listener.
async fn open_listeners<D: Deployment>(
    listeners: Listeners<D::Listener>,
    core: &Core<D>,
    revoked: &RevocationSet,
    backoff: NonZeroDuration,
) -> Result<OpenListeners<D>, ServerError> {
    let Listeners { public, worker } = listeners;
    let public = open_listener::<D>(public, EndpointRole::Public, core, revoked, backoff).await?;
    let worker = open_listener::<D>(worker, EndpointRole::Worker, core, revoked, backoff).await?;
    Ok(OpenListeners { public, worker })
}

/// Builds both controller streams before acquiring leadership.
fn controller_connections<D: Deployment>(
    public: Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
    worker: Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
    public_gate: Gate<D, D::Clients>,
    worker_gate: Gate<D, D::Workers>,
    api: PublicService<D>,
    stop: Stop,
) -> (ControllerListening<D>, ListenerEnds) {
    let (public_ended, public_end) = oneshot::channel();
    let (worker_ended, worker_end) = oneshot::channel();
    let connections = ControllerConnections {
        public: Listening {
            connections: public
                .into_stream(public_ended)
                .take_until(stop.clone())
                .boxed(),
            gate: public_gate,
        },
        worker: Listening {
            connections: worker.into_stream(worker_ended).take_until(stop).boxed(),
            gate: worker_gate,
        },
        api,
    };
    (connections, ListenerEnds::new(public_end, worker_end))
}

/// The shared ports and settings the lead phase borrows.
struct LeadContext<'a, D: Deployment> {
    core: &'a Core<D>,
    revoked_certificates: &'a RevocationSet,
    provisioner: &'a D::Provisioner,
    fleets: &'a Fleets<D::Plugins>,
    scheduler: &'a D::Scheduler,
    worker_endpoint: &'a WorkerEndpoint,
    dashboard: Option<&'a Dashboard>,
    control_settings: ControlSettings,
    builds: &'a BuildsCell,
    sink: &'a ObservationSink,
    cache: &'a D::Cache,
    leading: &'a Leading,
    readiness: &'a Readiness,
    leadership: &'a Leadership<'a, D::Repository, D::Clock>,
    actions: &'a gang_config::ActionsConfig,
    scaling: &'a Arc<gang_config::ScalingConfig>,
    sidecar_retry_interval: NonZeroDuration,
    milestone_timeout: Duration,
}

fn controller_leadership<'a, D: Deployment>(
    core: &'a Core<D>,
    controller: &'a ControllerId,
    settings: LeadershipConfig,
) -> Leadership<'a, D::Repository, D::Clock> {
    Leadership::new(
        &core.host.repository,
        &core.host.clock,
        controller,
        settings,
    )
}

fn start_standby<D: Deployment>(
    scheduler: &D::Scheduler,
    listeners: OpenListeners<D>,
    (public_gate, worker_gate, api): StandbyServices<D>,
) -> ActiveStandby<'_> {
    let (stop_sender, stop) = stop_channel();
    let (connections, ends) = controller_connections::<D>(
        listeners.public,
        listeners.worker,
        public_gate,
        worker_gate,
        api,
        stop,
    );
    ActiveStandby {
        stop: Some(stop_sender),
        listeners: ends,
        completion: async move {
            scheduler
                .serve_standby(connections)
                .await
                .map_err(ServerError::Controller)
        }
        .boxed(),
    }
}

fn term_connections<'s, D: Deployment>(
    scheduler: &'s D::Scheduler,
    OpenListeners { public, worker }: &'s OpenListeners<D>,
    (public_gate, worker_gate, api): StandbyServiceRefs<'s, D>,
    stop: Stop,
) -> TermConnections<D, impl FnOnce() -> Option<ActiveStandby<'s>>> {
    let (controller, listeners) = controller_connections::<D>(
        public.clone(),
        worker.clone(),
        public_gate.clone(),
        worker_gate.clone(),
        api.clone(),
        stop,
    );
    TermConnections {
        controller,
        listeners,
        standby: move || {
            Some(start_standby::<D>(
                scheduler,
                OpenListeners {
                    public: public.clone(),
                    worker: worker.clone(),
                },
                (public_gate.clone(), worker_gate.clone(), api.clone()),
            ))
        },
    }
}

/// Waits for the controller to serve, then holds leadership through restoration and drain.
async fn run_leader<D: Deployment>(
    context: LeadContext<'_, D>,
    signals: LeaderSignals<'_>,
    ready: &mut Option<ControllerReady>,
    candidate: &mut Option<LeaseCandidate>,
    routability_delay: Duration,
    (retiring, stop): (impl Future<Output = ()> + Clone, Stop),
) -> Result<(), ServerError> {
    let LeaderSignals {
        prepared,
        yielded,
        drained,
        failed,
        voluntary,
    } = signals;
    tokio::pin!(retiring);
    tokio::select! {
        biased;
        () = &mut retiring => return Ok(()),
        () = stop.clone() => return Ok(()),
        prepared = prepared => if !prepared { return Err(ServerError::ControllerEnded); },
    }
    let candidate = match candidate {
        Some(candidate) => candidate,
        vacant @ None => {
            let host = tokio::select! {
                biased;
                () = &mut retiring => return Ok(()),
                () = stop.clone() => return Ok(()),
                result = host_identity(
                    context.provisioner,
                    &*context.core.host.clock,
                    context.sidecar_retry_interval.get(),
                ) => result.map_err(ServerError::HostIdentity)?,
            };
            let registered = tokio::select! {
                biased;
                () = &mut retiring => return Ok(()),
                () = stop.clone() => return Ok(()),
                result = context.leadership.register_candidate(&host) => {
                    result.map_err(ServerError::CandidateRegistration)?
                }
            };
            vacant.insert(registered)
        }
    };
    context.readiness.serve();
    if ready.is_some() {
        tokio::select! {
            biased;
            () = &mut retiring => return Ok(()),
            () = stop.clone() => return Ok(()),
            result = report_serving(
                context.provisioner,
                &*context.core.host.clock,
                context.sidecar_retry_interval.get(),
                context.milestone_timeout,
            ) => result.map_err(ServerError::ServingMilestone)?,
        }
        if let Some(ready) = ready.take() {
            ready.signal();
        }
    }
    let lease = tokio::select! {
        biased;
        () = &mut retiring => return Ok(()),
        () = stop.clone() => return Ok(()),
        lease = context.leadership.acquire(candidate, context.readiness, routability_delay) => lease,
    };
    hold_leader(context, (yielded, drained, failed, voluntary), lease, stop).await
}

async fn hold_leader<D: Deployment>(
    context: LeadContext<'_, D>,
    (yielded, drained, failed, voluntary): (
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
        oneshot::Sender<ServerError>,
        &AtomicBool,
    ),
    mut lease: Lease,
    stop: Stop,
) -> Result<(), ServerError> {
    let mut yielded = Some(yielded);
    // The identity is stable across renewals; the repository checks its stored expiry.
    let authority = lease.clone();
    let profiles = SourceProfiles::new(context.scaling, context.actions.history_window);
    let result = {
        let (requested, mut request) = oneshot::channel();
        let hold = hold_leader_lease(&context, &mut lease, requested);
        tokio::pin!(hold);
        let restored = tokio::select! {
            biased;
            () = &mut hold => return Err(ServerError::LeadershipLost),
            () = stop.clone() => None,
            Ok(()) = &mut request => {
                finish_yield(context.readiness, &mut yielded, stop.clone()).await;
                None
            }
            result = restore_history(&context, &authority, &profiles) => Some(result?),
        };
        if let Some((mut store, mut records)) = restored {
            let mut requested = {
                let work = lead_work(&context, &profiles, stop.clone());
                let periodic = flush_history_periodically(
                    &context,
                    &authority,
                    &profiles,
                    &mut store,
                    &mut records,
                );
                tokio::pin!(work, periodic);
                tokio::select! {
                    biased;
                    () = &mut hold => return Err(ServerError::LeadershipLost),
                    () = stop.clone() => false,
                    Ok(()) = &mut request => true,
                    never = &mut periodic => match never {},
                    result = &mut work => {
                        if let Err(error) = result {
                            let _ = failed.send(error);
                        }
                        false
                    }
                }
            };
            if !requested && !voluntary.load(Ordering::Acquire) {
                requested = drain_and_flush(
                    &context,
                    &authority,
                    &profiles,
                    (&mut store, &mut records),
                    hold.as_mut(),
                    &mut request,
                    drained,
                )
                .await?;
            }
            if requested {
                let snapshot = context.core.host.clock.timeout(
                    context.leadership.snapshot_timeout(),
                    flush_history(&context, &authority, &profiles, &mut store, &mut records),
                );
                tokio::select! {
                    biased;
                    () = &mut hold => return Err(ServerError::LeadershipLost),
                    result = snapshot => match result {
                        Ok(Err(error)) => tracing::warn!(%error, "voluntary learned history snapshot failed"),
                        Err(error) => tracing::warn!(%error, "voluntary learned history snapshot timed out"),
                        Ok(Ok(())) => {}
                    }
                }
                finish_yield(context.readiness, &mut yielded, stop.clone()).await;
            }
        } else if !voluntary.load(Ordering::Acquire) {
            tokio::select! {
                biased;
                () = &mut hold => return Err(ServerError::LeadershipLost),
                Ok(()) = &mut request => {
                    finish_yield(context.readiness, &mut yielded, stop.clone()).await;
                }
                _ = drained => {}
            }
        }
        Ok(())
    };
    context.leadership.release(&lease).await;
    result
}

/// Keeps shutdown durability unless a successor requests the lease before completion.
async fn drain_and_flush<D: Deployment>(
    context: &LeadContext<'_, D>,
    authority: &Lease,
    profiles: &SourceProfiles,
    (store, records): (&mut LearnedHistoryStore, &mut Vec<gang_spi::LearnedHistory>),
    mut hold: Pin<&mut impl Future<Output = ()>>,
    request: &mut oneshot::Receiver<()>,
    drained: oneshot::Receiver<()>,
) -> Result<bool, ServerError> {
    tokio::select! {
        biased;
        () = &mut hold => return Err(ServerError::LeadershipLost),
        Ok(()) = &mut *request => return Ok(true),
        _ = drained => {}
    }
    let final_flush = context.core.host.clock.timeout(
        context.actions.history_flush_interval.get(),
        flush_history(context, authority, profiles, store, records),
    );
    tokio::select! {
        biased;
        () = &mut hold => Err(ServerError::LeadershipLost),
        Ok(()) = request => Ok(true),
        result = final_flush => {
            match result {
                Ok(Err(HistoryError::Fenced)) => tracing::warn!("final learned history flush was fenced"),
                Ok(Err(error)) => tracing::warn!(%error, "final learned history flush failed"),
                Err(error) => tracing::warn!(%error, "final learned history flush timed out"),
                Ok(Ok(())) => {}
            }
            Ok(false)
        }
    }
}

async fn hold_leader_lease<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &mut Lease,
    requested: oneshot::Sender<()>,
) {
    let mut requested = Some(requested);
    loop {
        match context.leadership.hold(lease).await {
            LeaseEnd::Lost => return,
            LeaseEnd::YieldRequested => {
                if let Some(requested) = requested.take() {
                    let _ = requested.send(());
                }
            }
        }
    }
}

async fn finish_yield(
    readiness: &Readiness,
    yielded: &mut Option<oneshot::Sender<()>>,
    stop: Stop,
) {
    readiness.stop();
    if let Some(yielded) = yielded.take() {
        let _ = yielded.send(());
    }
    stop.await;
}

/// Retries restoration without admitting work, including while the engine starts.
async fn restore_history<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &Lease,
    profiles: &SourceProfiles,
) -> Result<(LearnedHistoryStore, Vec<gang_spi::LearnedHistory>), ServerError> {
    let clock = &context.core.host.clock;
    let limit = loop {
        match context.scheduler.learned_history_byte_limit() {
            Ok(limit) => break history_byte_limit(limit),
            Err(error) => {
                tracing::warn!(%error, "learned history source unavailable; retrying before admission");
            }
        }
        clock.sleep(context.sidecar_retry_interval.get()).await;
    };
    // The store's batch sequence belongs to the whole lease term, including failed loads.
    let mut store = LearnedHistoryStore::new(limit);
    let mut records = Vec::new();
    loop {
        if !context.leadership.fence().holds() {
            clock.sleep(context.leadership.renew_interval()).await;
            continue;
        }
        let attempt = async {
            store
                .load(
                    &context.core.host.repository,
                    lease,
                    clock.wall(),
                    &mut records,
                )
                .await?;
            let time = PassTime::read(clock);
            context
                .scheduler
                .restore_learned_history(time.wall, &records)
                .await
                .map_err(HistoryError::Storage)?;
            profiles.restore(time, &records);
            Ok::<(), HistoryError>(())
        }
        .await;
        match attempt {
            Ok(()) => return Ok((store, records)),
            Err(HistoryError::Fenced) => {
                tracing::warn!("learned history restore was fenced; retrying before admission");
            }
            Err(error) => {
                tracing::warn!(%error, "learned history restore failed; retrying before admission");
            }
        }
        clock.sleep(context.leadership.renew_interval()).await;
    }
}

fn history_byte_limit(scheduler: std::num::NonZeroUsize) -> std::num::NonZeroUsize {
    scheduler.max(SourceProfiles::record_byte_limit())
}

async fn flush_history<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &Lease,
    profiles: &SourceProfiles,
    store: &mut LearnedHistoryStore,
    records: &mut Vec<gang_spi::LearnedHistory>,
) -> Result<(), HistoryError> {
    let clock = &context.core.host.clock;
    let now = clock.wall();
    context
        .scheduler
        .learned_history(now, records)
        .await
        .map_err(HistoryError::Storage)?;
    profiles
        .snapshot(PassTime::read(clock), records)
        .map_err(HistoryError::Storage)?;
    store
        .flush(&context.core.host.repository, lease, now, records)
        .await
}

async fn flush_history_periodically<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &Lease,
    profiles: &SourceProfiles,
    store: &mut LearnedHistoryStore,
    records: &mut Vec<gang_spi::LearnedHistory>,
) -> Infallible {
    let clock = &context.core.host.clock;
    let interval = context.actions.history_flush_interval.get();
    let mut due = clock.monotonic() + interval;
    loop {
        clock.sleep_until(due).await;
        let started = clock.monotonic();
        if !context.leadership.fence().holds() {
            due = started + context.leadership.renew_interval();
            continue;
        }
        // Anchor before I/O; an overrun catches up once, then re-anchors at that start.
        due = started + interval;
        match flush_history(context, lease, profiles, store, records).await {
            Ok(()) => {}
            Err(HistoryError::Fenced) => {
                tracing::warn!("learned history flush was fenced; retrying after renewal");
                due = clock.monotonic() + context.leadership.renew_interval();
            }
            Err(error) => {
                tracing::warn!(%error, "learned history flush failed; retaining pending changes");
            }
        }
    }
}

async fn host_identity<H: ControllerHostPort, C: Clock>(
    host: &H,
    clock: &C,
    interval: Duration,
) -> Result<ControllerHostIdentity, PortError> {
    loop {
        let started = clock.monotonic();
        match clock.timeout(interval, host.incarnation()).await {
            Ok(Ok(identity)) => return Ok(identity),
            Ok(Err(error @ PortError::Unavailable(_))) => {
                tracing::warn!(%error, "cannot identify controller host");
            }
            Ok(Err(error)) => return Err(error),
            Err(error) => tracing::warn!(%error, "timed out identifying controller host"),
        }
        clock.sleep_until(started + interval).await;
    }
}

async fn report_serving<H: ControllerHostPort, C: Clock>(
    host: &H,
    clock: &C,
    interval: Duration,
    bound: Duration,
) -> Result<(), PortError> {
    clock
        .timeout(bound, async {
            loop {
                match host.reached(HostMilestone::Serving).await {
                    Ok(()) => return Ok(()),
                    Err(error @ PortError::Unavailable(_)) => {
                        tracing::warn!(%error, "failed to report serving milestone; retrying");
                    }
                    Err(error) => return Err(error),
                }
                clock.sleep(interval).await;
            }
        })
        .await
        .map_err(PortError::unavailable)?
}

/// Starts the term and runs the leader's work after taking the cache baseline.
async fn lead_work<D: Deployment>(
    context: &LeadContext<'_, D>,
    profiles: &SourceProfiles,
    stop: Stop,
) -> Result<(), ServerError> {
    let baseline = match context.dashboard {
        Some(_) => Some(term_baseline(context).await),
        None => None,
    };
    synchronize_revocations(context.core, context.revoked_certificates)
        .await
        .map_err(ServerError::Revocations)?;
    context.leading.set(context.core.host.clock.wall());
    if let Some(dashboard) = context.dashboard
        && let Err(error) = dashboard.reload_from(&context.core.host.repository).await
    {
        tracing::warn!(%error, "dashboard history reload failed during promotion");
    }

    context.readiness.lead();
    let sidecar = retry_sidecar(context);
    let clock = &context.core.host.clock;
    let control = ControlLoop::new(
        context.fleets,
        context.control_settings,
        Arc::clone(context.scaling),
        profiles.clone(),
        context.actions.cpu_oversubscription,
        &crate::control::ControlPorts {
            provisioner: context.provisioner,
            scheduler: context.scheduler,
            repository: &context.core.host.repository,
            clock,
        },
        context.leadership.fence().clone(),
    );
    let control = match context.dashboard.zip(baseline) {
        Some((dashboard, baseline)) => control.with_publisher(dashboard.publisher(
            context.builds.clone(),
            context.sink.clone(),
            baseline,
        )),
        None => control,
    };
    tokio::select! {
        biased;
        () = stop => Ok(()),
        never = sidecar => match never {},
        never = control.run() => match never {},
        never = dashboard_tasks(context) => match never {},
    }
}

/// The cache traffic and engine evictions counted before the term, which no sample may hold.
/// The engine is asked within one poll interval; when it cannot answer, the baseline is left to
/// its first successful snapshot.
async fn term_baseline<D: Deployment>(context: &LeadContext<'_, D>) -> CacheBaseline {
    let mut snapshot = SchedulerSnapshot::default();
    let read = context.core.host.clock.timeout(
        context.control_settings.poll_interval.get(),
        context.scheduler.snapshot(&mut snapshot),
    );
    let evicted = match read.await {
        Ok(Ok(())) => Some(
            snapshot
                .stores
                .iter()
                .fold(0_u64, |sum, store| sum.saturating_add(store.evicted)),
        ),
        Ok(Err(error @ PortError::Unavailable(_))) => {
            tracing::debug!(
                %error,
                "engine not serving at the term start; evictions baseline later"
            );
            None
        }
        Ok(Err(error)) => {
            tracing::warn!(
                %error,
                "cannot read the engine at the term start; evictions baseline later"
            );
            None
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "engine read timed out at the term start; evictions baseline later"
            );
            None
        }
    };
    CacheBaseline {
        traffic: context.sink.traffic(),
        evicted,
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

/// Whether the process stopped or should begin another controller term.
enum TermEnd {
    Stopped,
    Yielded,
}

/// Drives the controller until shutdown or failure, then hands off and drains, running
/// `background` throughout.
async fn drive_controller<'s, C: Clock>(
    (shutdown, yielded): (impl Future<Output = ()>, oneshot::Receiver<()>),
    stop: impl FnOnce(bool),
    dashboard: Option<&Dashboard>,
    (mut controller, mut leader): (
        Pin<&mut impl Future<Output = Result<(), PortError>>>,
        Pin<&mut impl Future<Output = Result<(), ServerError>>>,
    ),
    signals: ControllerSignals<impl FnOnce() -> Option<ActiveStandby<'s>>>,
    mut maintenance: Maintenance<'_, 's, impl Future<Output = Infallible>>,
    (clock, handoff_timeout): (&C, Duration),
) -> Result<TermExit<'s>, ServerError> {
    let ControllerSignals {
        listeners: mut ends,
        drained,
        failed,
        standby,
    } = signals;
    let mut public_stopped = Box::pin(listener_stopped("public", &mut ends.public));
    let mut worker_stopped = Box::pin(listener_stopped("worker", &mut ends.worker));
    let failure = async {
        match failed.await {
            Ok(error) => error,
            Err(_) => std::future::pending().await,
        }
    };
    tokio::pin!(failure, shutdown);
    let mut handoff = Handoff {
        controller_finished: false,
        leader_finished: false,
        drained: Some(drained),
        standby: None,
        voluntary: false,
        result: Ok(()),
    };
    handoff.result = tokio::select! {
        biased;
        () = &mut shutdown => Ok(()),
        Ok(()) = yielded => { handoff.voluntary = true; Ok(()) },
        error = &mut public_stopped => Err(error),
        error = &mut worker_stopped => Err(error),
        result = &mut controller => {
            handoff.controller_finished = true;
            result.map_err(ServerError::Controller).and(Err(ServerError::ControllerEnded))
        },
        result = &mut leader => { handoff.leader_finished = true; result },
        error = &mut failure => Err(error),
        error = maintenance.failure() => Err(error),
    };
    let deadline = handoff
        .voluntary
        .then(|| clock.monotonic() + handoff_timeout);
    stop(handoff.voluntary);
    if let Some(dashboard) = dashboard {
        dashboard.shutdown();
    }
    if !handoff.controller_finished {
        let polled =
            std::future::poll_fn(|cx| std::task::Poll::Ready(controller.as_mut().poll(cx))).await;
        if let std::task::Poll::Ready(result) = polled {
            handoff.controller_done(result).await;
        } else {
            handoff.standby = standby();
        }
    }
    if handoff.controller_finished {
        handoff.signal_drained();
    }
    let drain = drain_controller(
        (controller, leader),
        handoff,
        &mut maintenance,
        shutdown.as_mut(),
    );
    if let Some(deadline) = deadline {
        clock
            .timeout(deadline.saturating_duration_since(clock.monotonic()), drain)
            .await
            .map_err(ServerError::HandoffTimedOut)?
    } else {
        drain.await
    }
}

struct Handoff<'a> {
    controller_finished: bool,
    leader_finished: bool,
    drained: Option<oneshot::Sender<()>>,
    standby: Option<ActiveStandby<'a>>,
    voluntary: bool,
    result: Result<(), ServerError>,
}

impl Handoff<'_> {
    fn record(&mut self, result: Result<(), ServerError>) {
        if self.result.is_ok() {
            self.result = result;
        }
    }

    fn signal_drained(&mut self) {
        if let Some(drained) = self.drained.take() {
            let _ = drained.send(());
        }
    }

    async fn controller_done(&mut self, result: Result<(), PortError>) {
        self.controller_finished = true;
        self.record(result.map_err(ServerError::Controller));
        if let Some(result) = stop_standby(&mut self.standby).await {
            self.record(result);
        }
        self.signal_drained();
    }
}

async fn stop_standby(active: &mut Option<ActiveStandby<'_>>) -> Option<Result<(), ServerError>> {
    let standby = active.as_mut()?;
    let ended = if standby.stop.is_some() {
        standby.listeners.ended()
    } else {
        None
    };
    if let Some(stop) = standby.stop.take() {
        let _ = stop.send(());
    }
    let polled =
        std::future::poll_fn(|cx| std::task::Poll::Ready(standby.completion.as_mut().poll(cx)))
            .await;
    match polled {
        std::task::Poll::Ready(result) => {
            active.take();
            Some(ended.map_or(result, Err))
        }
        std::task::Poll::Pending => ended.map(Err),
    }
}

impl ListenerEnds {
    fn new(public: oneshot::Receiver<()>, worker: oneshot::Receiver<()>) -> Self {
        Self {
            public: Some(public),
            worker: Some(worker),
        }
    }

    fn ended(&mut self) -> Option<ServerError> {
        for (endpoint, ended) in [("public", &mut self.public), ("worker", &mut self.worker)] {
            let Some(receiver) = ended.as_mut() else {
                continue;
            };
            match receiver.try_recv() {
                Ok(()) => {
                    *ended = None;
                    return Some(ServerError::ListenerEnded { endpoint });
                }
                Err(oneshot::error::TryRecvError::Closed) => *ended = None,
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }
        None
    }
}

async fn standby_event(active: &mut Option<ActiveStandby<'_>>) -> StandbyEvent {
    match active {
        Some(standby) => standby.event().await,
        None => std::future::pending().await,
    }
}

async fn drain_controller<'s>(
    (mut controller, mut leader): (
        Pin<&mut impl Future<Output = Result<(), PortError>>>,
        Pin<&mut impl Future<Output = Result<(), ServerError>>>,
    ),
    mut handoff: Handoff<'s>,
    maintenance: &mut Maintenance<'_, 's, impl Future<Output = Infallible>>,
    mut shutdown: Pin<&mut impl Future<Output = ()>>,
) -> Result<TermExit<'s>, ServerError> {
    loop {
        if handoff.controller_finished && handoff.leader_finished {
            if handoff.voluntary && handoff.result.is_ok() {
                return Ok(TermExit {
                    end: TermEnd::Yielded,
                    standby: handoff.standby.take().map(|standby| standby.completion),
                });
            }
            if handoff.standby.is_none() && maintenance.pending.is_empty() {
                return handoff.result.map(|()| TermExit {
                    end: TermEnd::Stopped,
                    standby: None,
                });
            }
        }
        tokio::select! {
            biased;
            () = &mut shutdown, if handoff.voluntary => handoff.voluntary = false,
            result = &mut controller, if !handoff.controller_finished =>
                handoff.controller_done(result).await,
            result = &mut leader, if !handoff.leader_finished => {
                handoff.leader_finished = true;
                handoff.record(result);
            },
            event = standby_event(&mut handoff.standby) => match event {
                StandbyEvent::Complete(result) => {
                    handoff.standby.take();
                    handoff.record(result);
                }
                StandbyEvent::ListenerEnded(error) => {
                    handoff.record(Err(error));
                    if let Some(result) = stop_standby(&mut handoff.standby).await {
                        handoff.record(result);
                    }
                }
            },
            error = maintenance.event() => {
                if let Some(error) = error { handoff.record(Err(error)); }
            }
        }
    }
}

fn observe_prepared(
    prepared: oneshot::Receiver<()>,
    observed: Arc<AtomicBool>,
) -> Shared<BoxFuture<'static, bool>> {
    async move {
        let ready = prepared.await.is_ok();
        if ready {
            observed.store(true, Ordering::Release);
        }
        ready
    }
    .boxed()
    .shared()
}

async fn drive_term<'s, D: Deployment>(
    context: LeadContext<'_, D>,
    spec: &ControllerSpec<'_>,
    connections: TermConnections<D, impl FnOnce() -> Option<ActiveStandby<'s>>>,
    (ready, candidate, observed_prepared): (
        &mut Option<ControllerReady>,
        &mut Option<LeaseCandidate>,
        &Arc<AtomicBool>,
    ),
    (shutdown, stop, stop_sender): (impl Future<Output = ()> + Clone, Stop, oneshot::Sender<()>),
    maintenance: Maintenance<'_, 's, impl Future<Output = Infallible>>,
    (routability_delay, handoff_timeout): (Duration, Duration),
) -> Result<TermExit<'s>, ServerError> {
    context
        .readiness
        .begin_term(context.leadership.fence().clone());
    let TermConnections {
        controller: connections,
        listeners: ends,
        standby,
    } = connections;
    let readiness = context.readiness;
    let dashboard = context.dashboard;
    let clock = &*context.core.host.clock;
    let (controller_ready, prepared) = ControllerReady::channel();
    let prepared = observe_prepared(prepared, Arc::clone(observed_prepared));
    let preparation = prepared.clone();
    let readiness_seen = Arc::clone(observed_prepared);
    let standby = move || {
        let _ = preparation.now_or_never();
        if readiness_seen.load(Ordering::Acquire) {
            standby()
        } else {
            None
        }
    };
    let mut controller = Box::pin(context.scheduler.run_controller(
        spec,
        connections,
        controller_ready,
    ));
    let (yielded, yielding) = oneshot::channel();
    let (drained_tx, drained_rx) = oneshot::channel();
    let (failed_tx, failed_rx) = oneshot::channel();
    let voluntary = AtomicBool::new(false);
    let mut leader = Box::pin(run_leader(
        context,
        LeaderSignals {
            prepared,
            yielded,
            drained: drained_rx,
            failed: failed_tx,
            voluntary: &voluntary,
        },
        ready,
        candidate,
        routability_delay,
        (shutdown.clone(), stop),
    ));
    drive_controller(
        (shutdown, yielding),
        |yielding| {
            voluntary.store(yielding, Ordering::Release);
            readiness.stop();
            let _ = stop_sender.send(());
        },
        dashboard,
        (controller.as_mut(), leader.as_mut()),
        ControllerSignals {
            listeners: ends,
            drained: drained_tx,
            failed: failed_rx,
            standby,
        },
        maintenance,
        (clock, handoff_timeout),
    )
    .await
}

/// The composed server: the controller's scheduler and provisioner, the control loop, the gates that
/// admit requests on each listener, and the Gang API they hand requests to.
pub struct Server<D: Deployment> {
    core: Arc<Core<D>>,
    provisioner: D::Provisioner,
    fleets: Fleets<D::Plugins>,
    cache: D::Cache,
    scheduler: D::Scheduler,
    listeners: Listeners<D::Listener>,
    health: Option<D::Listener>,
    public: PublicService<D>,
    public_gate: Gate<D, D::Clients>,
    worker_gate: Gate<D, D::Workers>,
    revoked_certificates: RevocationSet,
    worker_endpoint: WorkerEndpoint,
    controller: ControllerId,
    leadership: LeadershipConfig,
    readiness: Readiness,
    revocation_epoch: RevocationEpoch,
    revocation_refresh_interval: NonZeroDuration,
    control_settings: ControlSettings,
    client_action_timeout: NonZeroDuration,
    actions: Arc<gang_config::ActionsConfig>,
    scaling: Arc<gang_config::ScalingConfig>,
    accept_error_backoff: NonZeroDuration,
    standby_routability_delay: Duration,
    handoff_timeout: Duration,
    health_limits: HealthLimits,
    dashboard: Option<Dashboard>,
    builds: BuildsCell,
    observation_sink: ObservationSink,
    observations: Observations,
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
    /// certificates cannot be loaded from the ports, or the server information cannot be built.
    pub async fn prepare(parts: Parts<D>, settings: ServerSettings) -> Result<Self, ServerError> {
        let endpoint_trust = settings.transport.endpoint_trust();
        let health_limits = HealthLimits::from(&settings);
        let (worker_endpoint, security, signer) = prepare_transport::<D>(
            settings.transport,
            parts.scheduler.endpoints().worker,
            &settings.endpoint,
            parts.signer,
            &parts.clock,
        )
        .await?;
        let (revocation_epoch, revoked_principals) =
            initial_revocations::<D>(&parts.repository).await?;
        let core = Core::new(
            Host {
                proxy_protocol: settings.proxy_protocol,
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
        let initial = initial_server_info(&core, settings.default_isa, endpoint_trust);
        let (info, revoked_certificates) = initial.await?;
        let readiness = Readiness::default();
        let (builds, observation_sink, observations) = BuildsCell::channel(&settings.builds);
        let (dashboard, control_settings) = start_observability(
            settings.dashboard,
            settings.provisioner,
            settings.poll_interval,
            &core.host,
        )
        .await?;
        let public = public_service(
            &core,
            (info, &revoked_certificates),
            (settings.roles, settings.limits, &settings.rate_limit),
            (dashboard.as_ref(), settings.login),
            readiness.clone(),
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
            public_gate: public_gate(&core, &settings.rate_limit).with_readiness(readiness.clone()),
            worker_gate: Gate::worker(Arc::clone(&core)).with_readiness(readiness.clone()),
            core,
            revoked_certificates,
            worker_endpoint,
            controller: parts.controller,
            leadership: settings.leadership,
            readiness,
            revocation_epoch,
            revocation_refresh_interval: settings.revocation_refresh_interval,
            control_settings,
            client_action_timeout: settings.client_action_timeout,
            actions: settings.actions,
            scaling: settings.scaling,
            accept_error_backoff: settings.accept_error_backoff,
            standby_routability_delay: settings.standby_routability_delay,
            handoff_timeout: settings.handoff_timeout,
            health_limits,
            dashboard,
            builds,
            observation_sink,
            observations,
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

    fn cache_stores(
        &self,
    ) -> Result<(gang_spi::RenderedStoreSpec, gang_spi::RenderedStoreSpec), ServerError> {
        let content = self.cache.content_store().map_err(ServerError::Cache)?;
        let action = self
            .cache
            .action_store(&self.actions.environment)
            .map_err(ServerError::Cache)?;
        Ok((content, action))
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
        let (content_store, action_store) = self.cache_stores()?;
        let spec = ControllerSpec {
            content_store: &content_store,
            action_store: &action_store,
            fleets: self.fleets.catalog(),
            max_workers: self.fleets.max_instances(),
            client_action_timeout: self.client_action_timeout.get(),
            control_acquire_timeout: self.control_settings.poll_interval,
            actions: Arc::clone(&self.actions),
            scaling: Arc::clone(&self.scaling),
            clock: core.host.clock.clone(),
            observations: &self.observation_sink,
        };
        let shutdown = shutdown.shared();
        let readiness = self.readiness;
        // Every controller refreshes revocations and drains its admitted observations.
        let mut background = Box::pin(run_background(
            core,
            (
                &self.revoked_certificates,
                self.revocation_epoch,
                self.revocation_refresh_interval,
            ),
            &self.builds,
            self.observations,
            self.health,
            (readiness.clone(), self.health_limits),
        ));
        let listeners = open_listeners::<D>(
            self.listeners,
            core,
            &self.revoked_certificates,
            self.accept_error_backoff,
        )
        .await?;
        let mut candidate = None;
        let mut ready = Some(ready);
        let observed_prepared = Arc::new(AtomicBool::new(false));
        let mut pending = PendingStandbys::new();
        loop {
            if shutdown.clone().now_or_never().is_some() {
                Maintenance {
                    background: background.as_mut(),
                    pending: &mut pending,
                }
                .finish()
                .await?;
                return Ok(());
            }
            let leadership = controller_leadership(core, &self.controller, self.leadership.clone());
            let leading = Leading::default();
            let (stop_sender, stop) = stop_channel();
            let connections = term_connections::<D>(
                &self.scheduler,
                &listeners,
                (&self.public_gate, &self.worker_gate, &self.public),
                stop.clone(),
            );
            let context = LeadContext {
                core,
                revoked_certificates: &self.revoked_certificates,
                provisioner: &self.provisioner,
                fleets: &self.fleets,
                scheduler: &self.scheduler,
                worker_endpoint: &self.worker_endpoint,
                dashboard: self.dashboard.as_ref(),
                builds: &self.builds,
                sink: &self.observation_sink,
                cache: &self.cache,
                leading: &leading,
                readiness: &readiness,
                leadership: &leadership,
                control_settings: self.control_settings,
                actions: &self.actions,
                scaling: &self.scaling,
                sidecar_retry_interval: self.leadership.renew_interval,
                milestone_timeout: self.leadership.ttl.get(),
            };
            let term_end = drive_term(
                context,
                &spec,
                connections,
                (&mut ready, &mut candidate, &observed_prepared),
                (shutdown.clone(), stop, stop_sender),
                Maintenance {
                    background: background.as_mut(),
                    pending: &mut pending,
                },
                (self.standby_routability_delay, self.handoff_timeout),
            )
            .await?;
            if matches!(term_end.queue_standby(&mut pending), TermEnd::Stopped) {
                return Ok(());
            }
        }
    }
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
    readiness: Readiness,
    builds: &BuildsCell,
) -> PublicService<K> {
    let (info, revoked_certificates) = initial;
    let (roles, limits, rate_limit) = settings;
    let (dashboard, login) = browser;
    let mut parts = ApiParts {
        readiness,
        core: Arc::clone(core),
        revocations: Revocations::new(Arc::clone(core), revoked_certificates.clone()),
        info,
        authorizer: Authorizer::new(roles),
        limits,
        dashboard: dashboard.map(Dashboard::api),
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
async fn listener_stopped(
    endpoint: &'static str,
    ended: &mut Option<oneshot::Receiver<()>>,
) -> ServerError {
    if let Some(receiver) = ended {
        let notified = receiver.await.is_ok();
        // Retire consumed receivers before waiting again, including across select cancellation.
        *ended = None;
        if notified {
            return ServerError::ListenerEnded { endpoint };
        }
    }
    std::future::pending().await
}

/// Starts the optional dashboard and retains the configured control-loop settings.
async fn start_observability<K: Stack>(
    config: DashboardConfig,
    provisioner: String,
    poll_interval: NonZeroDuration,
    host: &Host<K>,
) -> Result<(Option<Dashboard>, ControlSettings), ServerError> {
    let settings = ControlSettings::new(poll_interval, &config);
    if !config.enabled {
        return Ok((None, settings));
    }
    Dashboard::start(
        config,
        provisioner,
        poll_interval.get(),
        host.revoked_principals.epoch(),
        &host.clock,
        &host.repository,
    )
    .await
    .map(|dashboard| (Some(dashboard), settings))
}

/// The dashboard's background work: the actions refresher, the principals forwarder and the cache
/// usage sampler. Never completes; without a dashboard it waits forever.
async fn dashboard_tasks<D: Deployment>(context: &LeadContext<'_, D>) -> Infallible {
    let Some(dashboard) = context.dashboard else {
        return std::future::pending().await;
    };
    let clock = &*context.core.host.clock;
    tokio::select! {
        never = dashboard.refresher(context.scheduler, clock).run() => never,
        never = dashboard.forward_principals(&context.core.host.revoked_principals) => never,
        never = dashboard.sample_usage(context.cache, clock, context.leading) => never,
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
    let config = &core.host.proxy_protocol;
    let Security::Tls(tls) = &core.host.security else {
        return Ok(Accepting::plain(
            listener,
            accept_error_backoff,
            Arc::clone(&core.host.clock),
            config.clone(),
        ));
    };
    let (name, certificates, alpn) = match role {
        EndpointRole::Public => (
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
    let policy = policy(
        tls,
        name,
        certificates,
        alpn,
        revoked_certificates,
        accept_error_backoff,
        config.clone(),
    );
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
    proxy_protocol: Option<Arc<gang_config::ProxyProtocolConfig>>,
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
        proxy_protocol,
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
    use std::pin::Pin;

    use super::{UnauthenticatedListeners, unauthenticated_listeners};
    use crate::endpoint::EndpointRole;

    fn standby_driver_signals<R>(
        standby: R,
    ) -> (
        super::ControllerSignals<R>,
        [tokio::sync::oneshot::Sender<()>; 2],
    ) {
        let (public, public_end) = tokio::sync::oneshot::channel();
        let (worker, worker_end) = tokio::sync::oneshot::channel();
        let (drained, _drained) = tokio::sync::oneshot::channel();
        let (_failed, failed) = tokio::sync::oneshot::channel();
        (
            super::ControllerSignals {
                listeners: super::ListenerEnds::new(public_end, worker_end),
                drained,
                failed,
                standby,
            },
            [public, worker],
        )
    }

    #[tokio::test]
    async fn ready_on_final_controller_poll_is_observed_before_standby() {
        use futures::FutureExt;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let readiness_seen = Arc::new(AtomicBool::new(false));
        let (ready, receiver) = tokio::sync::oneshot::channel();
        let preparation = super::observe_prepared(receiver, Arc::clone(&readiness_seen));
        let mut ready = Some(ready);
        let mut controller = Box::pin(std::future::poll_fn(move |_| {
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
            }
            std::task::Poll::Pending::<Result<(), gang_spi::PortError>>
        }));
        let mut leader = Box::pin(std::future::pending());
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let started = std::cell::Cell::new(false);
        let (signals, _ends) = standby_driver_signals(|| {
            let _ = preparation.now_or_never();
            started.set(readiness_seen.load(Ordering::Acquire));
            None
        });
        let (_yielded, yielding) = tokio::sync::oneshot::channel();
        let clock = crate::test_support_clock::start();
        let mut drive = Box::pin(super::drive_controller(
            (std::future::ready(()), yielding),
            |_| {},
            None,
            (controller.as_mut(), leader.as_mut()),
            signals,
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        assert!(started.get());
    }

    #[tokio::test]
    async fn observed_preparation_remains_sticky_after_unsent_later_term() {
        use futures::FutureExt;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let observed = Arc::new(AtomicBool::new(false));
        let (ready, receiver) = tokio::sync::oneshot::channel();
        ready.send(()).expect("receiver retained");
        assert_eq!(
            super::observe_prepared(receiver, Arc::clone(&observed)).now_or_never(),
            Some(true)
        );
        let (ready, receiver) = tokio::sync::oneshot::channel();
        drop(ready);
        assert_eq!(
            super::observe_prepared(receiver, Arc::clone(&observed)).now_or_never(),
            Some(false)
        );
        assert!(observed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn controller_first_post_stop_poll_skips_unneeded_refuser() -> gang_testkit::TestResult {
        let polls = std::cell::Cell::new(0);
        let refusals = std::cell::Cell::new(0);
        let mut controller = Box::pin(std::future::poll_fn(|_| {
            polls.set(polls.get() + 1);
            std::task::Poll::Ready(Ok(()))
        }));
        let mut leader = Box::pin(std::future::ready(Ok(())));
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let (signals, _ends) = standby_driver_signals(|| {
            refusals.set(refusals.get() + 1);
            None
        });
        let (_yielded, yielding) = tokio::sync::oneshot::channel();
        let clock = crate::test_support_clock::start();
        let exit = super::drive_controller(
            (std::future::ready(()), yielding),
            |_| {},
            None,
            (controller.as_mut(), leader.as_mut()),
            signals,
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        )
        .await?;
        assert!(matches!(exit.end, super::TermEnd::Stopped));
        assert!(exit.standby.is_none());
        assert_eq!(polls.get(), 1);
        assert_eq!(refusals.get(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn late_queued_refusal_failure_stops_the_current_term() -> gang_testkit::TestResult {
        let (stop, stopped) = super::stop_channel();
        let controller_stop = stopped.clone();
        let mut controller = Box::pin(async {
            controller_stop.await;
            Ok(())
        });
        let mut leader = Box::pin(async {
            stopped.await;
            Ok(())
        });
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let (fail, failed) = tokio::sync::oneshot::channel::<()>();
        pending.push(Box::pin(async {
            let _ = failed.await;
            Err(super::ServerError::Controller(
                gang_spi::PortError::backend("late refusal"),
            ))
        }));
        let (signals, _ends) = standby_driver_signals(|| None);
        let (_yielded, yielding) = tokio::sync::oneshot::channel();
        let clock = crate::test_support_clock::start();
        let mut drive = Box::pin(super::drive_controller(
            (std::future::pending(), yielding),
            |_| {
                let _ = stop.send(());
            },
            None,
            (controller.as_mut(), leader.as_mut()),
            signals,
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        fail.send(()).map_err(|()| "queued refusal was cancelled")?;
        assert!(matches!(
            drive.await,
            Err(super::ServerError::Controller(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn refusal_listener_end_is_fatal_for_both_roles() -> gang_testkit::TestResult {
        for public in [true, false] {
            assert_refusal_listener_end(public).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn ready_refusal_preserves_listener_end_classification() -> gang_testkit::TestResult {
        for public in [true, false] {
            let (public_end, public_stopped) = tokio::sync::oneshot::channel();
            let (worker_end, worker_stopped) = tokio::sync::oneshot::channel();
            let (stop, _stopped) = tokio::sync::oneshot::channel();
            let mut active = super::ActiveStandby {
                stop: Some(stop),
                listeners: super::ListenerEnds::new(public_stopped, worker_stopped),
                completion: Box::pin(std::future::ready(Ok(()))),
            };
            let retained = if public {
                public_end
                    .send(())
                    .map_err(|()| "public listener monitor missing")?;
                worker_end
            } else {
                worker_end
                    .send(())
                    .map_err(|()| "worker listener monitor missing")?;
                public_end
            };
            let expected = if public { "public" } else { "worker" };
            assert!(matches!(
                active.event().await,
                super::StandbyEvent::ListenerEnded(super::ServerError::ListenerEnded { endpoint })
                    if endpoint == expected
            ));
            let mut active = Some(active);
            assert!(matches!(
                super::stop_standby(&mut active).await,
                Some(Ok(()))
            ));
            assert!(active.is_none());
            drop(retained);
        }
        Ok(())
    }

    #[tokio::test]
    async fn completion_poll_preserves_listener_end_and_backend_error() -> gang_testkit::TestResult
    {
        for public in [true, false] {
            for backend_error in [false, true] {
                assert_completion_poll_notification(public, backend_error).await?;
            }
        }
        Ok(())
    }

    async fn assert_completion_poll_notification(
        public: bool,
        backend_error: bool,
    ) -> gang_testkit::TestResult {
        let (public_end, public_stopped) = tokio::sync::oneshot::channel();
        let (worker_end, worker_stopped) = tokio::sync::oneshot::channel();
        let (notify, retained) = if public {
            (public_end, worker_end)
        } else {
            (worker_end, public_end)
        };
        let (stop, _stopped) = tokio::sync::oneshot::channel();
        let active = super::ActiveStandby {
            stop: Some(stop),
            listeners: super::ListenerEnds::new(public_stopped, worker_stopped),
            completion: Box::pin(async move {
                let _ = notify.send(());
                if backend_error {
                    Err(super::ServerError::Controller(
                        gang_spi::PortError::backend("same-poll refusal failure"),
                    ))
                } else {
                    Ok(())
                }
            }),
        };
        let handoff = super::Handoff {
            controller_finished: false,
            leader_finished: true,
            drained: None,
            standby: Some(active),
            voluntary: false,
            result: Ok(()),
        };
        let (finish, finished) = tokio::sync::oneshot::channel();
        let mut controller = std::pin::pin!(async {
            let _ = finished.await;
            Ok(())
        });
        let mut leader = std::pin::pin!(std::future::pending());
        let mut background = std::pin::pin!(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let mut maintenance = super::Maintenance {
            background: background.as_mut(),
            pending: &mut pending,
        };
        let mut shutdown = std::pin::pin!(std::future::pending());
        let mut drain = std::pin::pin!(super::drain_controller(
            (controller.as_mut(), leader.as_mut()),
            handoff,
            &mut maintenance,
            shutdown.as_mut(),
        ));
        assert!(futures::poll!(drain.as_mut()).is_pending());
        finish.send(()).map_err(|()| "old drain was cancelled")?;
        let expected = if public { "public" } else { "worker" };
        let Err(error) = drain.await else {
            return Err("refusal completion did not fail the drain".into());
        };
        if backend_error {
            assert!(matches!(
                error,
                super::ServerError::Controller(gang_spi::PortError::Backend(cause))
                    if cause.to_string() == "same-poll refusal failure"
            ));
        } else {
            assert!(
                matches!(error, super::ServerError::ListenerEnded { endpoint } if endpoint == expected)
            );
        }
        drop(retained);
        Ok(())
    }

    #[tokio::test]
    async fn dropped_refusal_senders_preserve_async_backend_error() -> gang_testkit::TestResult {
        for dropped in [[true, false], [false, true], [true, true]] {
            assert_dropped_refusal_senders(dropped).await?;
        }
        Ok(())
    }

    async fn assert_dropped_refusal_senders(dropped: [bool; 2]) -> gang_testkit::TestResult {
        let (public_end, public_stopped) = tokio::sync::oneshot::channel();
        let (worker_end, worker_stopped) = tokio::sync::oneshot::channel();
        let (public_end, retained_public) = if dropped[0] {
            (Some(public_end), None)
        } else {
            (None, Some(public_end))
        };
        let (worker_end, retained_worker) = if dropped[1] {
            (Some(worker_end), None)
        } else {
            (None, Some(worker_end))
        };
        let (fail, failed) = tokio::sync::oneshot::channel::<()>();
        let (stop, _stopped) = tokio::sync::oneshot::channel();
        let mut active = Some(super::ActiveStandby {
            stop: Some(stop),
            listeners: super::ListenerEnds::new(public_stopped, worker_stopped),
            completion: Box::pin(async move {
                drop((public_end, worker_end));
                let _ = failed.await;
                Err(super::ServerError::Controller(
                    gang_spi::PortError::backend("asynchronous refusal failure"),
                ))
            }),
        });
        // Each cancelled event must retain the retired-receiver state for the next selection.
        assert!(futures::poll!(std::pin::pin!(super::standby_event(&mut active))).is_pending());
        assert!(
            active
                .as_mut()
                .is_some_and(|active| active.listeners.ended().is_none())
        );
        assert!(futures::poll!(std::pin::pin!(super::standby_event(&mut active))).is_pending());
        assert!(futures::poll!(std::pin::pin!(super::standby_event(&mut active))).is_pending());
        assert!(super::stop_standby(&mut active).await.is_none());
        fail.send(())
            .map_err(|()| "refusal failure was cancelled")?;
        let super::StandbyEvent::Complete(Err(error)) = super::standby_event(&mut active).await
        else {
            return Err("dropped senders masked the refusal backend error".into());
        };
        assert!(matches!(
            error,
            super::ServerError::Controller(gang_spi::PortError::Backend(cause))
                if cause.to_string() == "asynchronous refusal failure"
        ));
        drop((retained_public, retained_worker));
        Ok(())
    }

    async fn assert_refusal_listener_end(public: bool) -> gang_testkit::TestResult {
        let (finish, finished) = tokio::sync::oneshot::channel();
        let mut controller = Box::pin(async {
            let _ = finished.await;
            Ok(())
        });
        let mut leader = Box::pin(std::future::ready(Ok(())));
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let (public_end, public_stopped) = tokio::sync::oneshot::channel();
        let (worker_end, worker_stopped) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let active = super::ActiveStandby {
            stop: Some(stop),
            listeners: super::ListenerEnds::new(public_stopped, worker_stopped),
            completion: Box::pin(async {
                let _ = stopped.await;
                Ok(())
            }),
        };
        let (signals, _ends) = standby_driver_signals(|| Some(active));
        let (yielded, yielding) = tokio::sync::oneshot::channel();
        yielded.send(()).map_err(|()| "yield receiver missing")?;
        let clock = crate::test_support_clock::start();
        let mut drive = Box::pin(super::drive_controller(
            (std::future::pending(), yielding),
            |_| {},
            None,
            (controller.as_mut(), leader.as_mut()),
            signals,
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        let retained = if public {
            public_end
                .send(())
                .map_err(|()| "public listener monitor missing")?;
            worker_end
        } else {
            worker_end
                .send(())
                .map_err(|()| "worker listener monitor missing")?;
            public_end
        };
        assert!(
            futures::poll!(drive.as_mut()).is_pending(),
            "fatal refusal still drains old streams"
        );
        finish.send(()).map_err(|()| "old drain was cancelled")?;
        let expected = if public { "public" } else { "worker" };
        assert!(
            matches!(drive.await, Err(super::ServerError::ListenerEnded { endpoint }) if endpoint == expected)
        );
        drop(retained);
        Ok(())
    }

    #[tokio::test]
    async fn queued_refusal_deadline_is_anchored_before_next_term() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use std::sync::atomic::{AtomicU64, Ordering};
        let clock = crate::test_support_clock::start();
        let anchored = AtomicU64::new(0);
        let (finish, finished) = tokio::sync::oneshot::channel();
        let mut controller = Box::pin(async {
            let _ = finished.await;
            Ok(())
        });
        let mut leader = Box::pin(std::future::ready(Ok(())));
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let (public, public_end) = tokio::sync::oneshot::channel();
        let (worker, worker_end) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let active = super::ActiveStandby {
            stop: Some(stop),
            listeners: super::ListenerEnds::new(public_end, worker_end),
            completion: Box::pin(async {
                let _ = stopped.await;
                anchored.store(clock.wall().as_unix_millis(), Ordering::SeqCst);
                clock.sleep(std::time::Duration::from_secs(2)).await;
                Ok(())
            }),
        };
        let (signals, _ends) = standby_driver_signals(|| Some(active));
        let (yielded, yielding) = tokio::sync::oneshot::channel();
        yielded.send(()).map_err(|()| "yield receiver missing")?;
        let mut drive = Box::pin(super::drive_controller(
            (std::future::pending(), yielding),
            |_| {},
            None,
            (controller.as_mut(), leader.as_mut()),
            signals,
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        finish.send(()).map_err(|()| "old drain was cancelled")?;
        let exit = drive.await?;
        assert!(matches!(exit.end, super::TermEnd::Yielded));
        assert_eq!(
            anchored.load(Ordering::SeqCst),
            clock.wall().as_unix_millis()
        );
        let mut completion = exit.standby.ok_or("pending denial was not retained")?;
        clock.advance(std::time::Duration::from_secs(2));
        assert!(matches!(
            futures::poll!(completion.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        drop((public, worker));
        Ok(())
    }

    fn passive_candidate(holder: &gang_spi::ControllerId) -> gang_spi::LeaseCandidate {
        gang_spi::LeaseCandidate {
            holder: holder.clone(),
            priority: gang_spi::CandidatePriority::registered(
                gang_spi::CandidateGeneration::default(),
            ),
        }
    }

    struct IdentityHost {
        calls: std::sync::atomic::AtomicUsize,
        mode: u8,
    }

    impl gang_spi::ControllerHostPort for IdentityHost {
        async fn incarnation(
            &self,
        ) -> Result<gang_spi::ControllerHostIdentity, gang_spi::PortError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.mode {
                0 if call == 0 => Err(gang_spi::PortError::unavailable("temporary")),
                1 => Err(gang_spi::PortError::backend("permanent")),
                2 => std::future::pending().await,
                _ => Ok(gang_spi::ControllerHostIdentity::Persistent(
                    gang_types::InstanceId::try_new("host").map_err(gang_spi::PortError::config)?,
                )),
            }
        }

        async fn retiring(&self) -> Result<(), gang_spi::PortError> {
            std::future::pending().await
        }

        async fn reached(&self, _: gang_spi::HostMilestone) -> Result<(), gang_spi::PortError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn host_identity_retries_transient_and_returns_permanent_failures()
    -> gang_testkit::TestResult {
        let clock = gang_clock::TestClock::default();
        let interval = std::time::Duration::from_secs(3);
        let host = IdentityHost {
            calls: std::sync::atomic::AtomicUsize::new(0),
            mode: 0,
        };
        let mut identity = Box::pin(super::host_identity(&host, &clock, interval));
        assert!(futures::poll!(identity.as_mut()).is_pending());
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        clock.advance(interval);
        assert!(matches!(
            futures::poll!(identity.as_mut()),
            std::task::Poll::Ready(Ok(gang_spi::ControllerHostIdentity::Persistent(id)))
                if id == gang_types::InstanceId::try_new("host")?
        ));
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        let host = IdentityHost {
            calls: std::sync::atomic::AtomicUsize::new(0),
            mode: 1,
        };
        assert!(matches!(
            super::host_identity(&host, &clock, interval).await,
            Err(gang_spi::PortError::Backend(_))
        ));
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn host_identity_bounds_hung_attempts_and_can_be_cancelled() {
        let clock = gang_clock::TestClock::default();
        let interval = std::time::Duration::from_secs(3);
        let host = IdentityHost {
            calls: std::sync::atomic::AtomicUsize::new(0),
            mode: 2,
        };
        {
            let mut identity = Box::pin(super::host_identity(&host, &clock, interval));
            assert!(futures::poll!(identity.as_mut()).is_pending());
            clock.advance(interval);
            assert!(futures::poll!(identity.as_mut()).is_pending());
            assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
        clock.advance(interval);
        assert_eq!(host.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn persistent_old_host_restart_cannot_reverse_handoff() -> gang_testkit::TestResult {
        old_host_restart_cannot_reverse_handoff(
            gang_testkit::FakeProvisioner::default(),
            gang_testkit::FakeProvisioner::default(),
        )
        .await
    }

    #[tokio::test]
    async fn launched_old_host_restart_cannot_reverse_handoff() -> gang_testkit::TestResult {
        let host = |at, id| -> gang_testkit::TestResult<_> {
            Ok(gang_testkit::FakeProvisioner::default().with_host_identity(
                gang_spi::ControllerHostIdentity::Launched {
                    at: gang_clock::Timestamp::from_unix_millis(at),
                    instance: gang_types::InstanceId::try_new(id)?,
                },
            ))
        };
        old_host_restart_cannot_reverse_handoff(host(1, "old")?, host(2, "new")?).await
    }

    async fn old_host_restart_cannot_reverse_handoff(
        old_host: gang_testkit::FakeProvisioner,
        new_host: gang_testkit::FakeProvisioner,
    ) -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_spi::{ControllerId, ControllerReady};

        let (mut first, _dials, old) = plain_parts();
        first.provisioner = old_host.clone();
        first.controller = ControllerId::new("old-process");
        let (mut second, _dials, replacement) = plain_parts();
        second.provisioner = new_host;
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("replacement-process");
        let settings = plain_settings();
        let interval = settings.leadership.renew_interval.get();
        let first = super::Server::prepare(first, settings).await?;
        let first_state = first.readiness.clone();
        let second = super::Server::prepare(second, plain_settings()).await?;
        let second_state = second.readiness.clone();
        let (stop_first, stopped_first) = tokio::sync::oneshot::channel::<()>();
        let (stop_second, stopped_second) = tokio::sync::oneshot::channel::<()>();
        let (ready_first, mut first_ready) = ControllerReady::channel();
        let (ready_second, mut second_ready) = ControllerReady::channel();
        let mut first = Box::pin(first.run(
            async {
                let _ = stopped_first.await;
            },
            ready_first,
        ));
        assert!(futures::poll!(first.as_mut()).is_pending());
        first_ready.try_recv()?;
        assert!(first_state.is_leading());
        let mut second = Box::pin(second.run(
            async {
                let _ = stopped_second.await;
            },
            ready_second,
        ));
        assert!(futures::poll!(second.as_mut()).is_pending());
        second_ready.try_recv()?;
        assert!(second_state.ready());
        assert!(!second_state.is_leading());
        old.clock.advance(interval);
        let terms = old.scheduler.controller_runs();
        tokio::select! {
            result = first.as_mut() => {
                result?;
                return Err("old process exited instead of yielding".into());
            }
            () = terms.reached(2) => {}
        }
        assert!(!first_state.ready());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(second_state.is_leading());
        stop_first
            .send(())
            .map_err(|()| "old process stopped early")?;
        first.await?;

        assert_restarted_host_stays_passive(
            old_host,
            &old,
            &replacement,
            &second_state,
            &mut second,
            interval,
        )
        .await?;
        stop_second
            .send(())
            .map_err(|()| "replacement stopped early")?;
        second.await?;
        Ok(())
    }

    async fn assert_restarted_host_stays_passive(
        old_host: gang_testkit::FakeProvisioner,
        old: &crate::test_support_deployment::Handles,
        replacement: &crate::test_support_deployment::Handles,
        second_state: &crate::health::Readiness,
        second: &mut std::pin::Pin<Box<impl Future<Output = Result<(), super::ServerError>>>>,
        interval: std::time::Duration,
    ) -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_spi::{ControllerId, ControllerReady};
        let (mut restarted, _dials, restart) = plain_parts();
        restarted.provisioner = old_host;
        restarted.repository = old.repository.clone();
        restarted.clock = old.clock.clone();
        restarted.controller = ControllerId::new("restarted-old-process");
        let restarted = super::Server::prepare(restarted, plain_settings()).await?;
        let restarted_state = restarted.readiness.clone();
        let (stop_restart, stopped_restart) = tokio::sync::oneshot::channel::<()>();
        let (ready_restart, mut restart_ready) = ControllerReady::channel();
        let mut restarted = Box::pin(restarted.run(
            async {
                let _ = stopped_restart.await;
            },
            ready_restart,
        ));
        assert!(futures::poll!(restarted.as_mut()).is_pending());
        restart_ready.try_recv()?;
        assert!(!restarted_state.ready());
        for _ in 0..4 {
            old.clock.advance(interval);
            assert!(futures::poll!(restarted.as_mut()).is_pending());
            assert!(
                !restarted_state.ready(),
                "old host cannot become an active contender"
            );
            assert!(!restarted_state.is_leading());
            let snapshots = replacement.scheduler.snapshots().get();
            assert!(futures::poll!(second.as_mut()).is_pending());
            assert!(
                second_state.is_leading(),
                "replacement must not yield to an old host restart"
            );
            assert!(replacement.scheduler.snapshots().get() > snapshots);
            assert_eq!(replacement.scheduler.controller_runs().get(), 1);
            assert_eq!(restart.scheduler.snapshots().get(), 0);
        }
        stop_restart
            .send(())
            .map_err(|()| "restarted process stopped early")?;
        restarted.await?;
        Ok(())
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
        let (worker, worker_end) = tokio::sync::oneshot::channel();
        let mut controller = Box::pin(async move {
            let _ = stopped.await;
            drop(listener);
            Ok(())
        });
        let mut leader = Box::pin(std::future::pending());
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let ends = super::ListenerEnds::new(public_end, worker_end);
        let (drained, _drained_rx) = tokio::sync::oneshot::channel();
        let (_failed_tx, failed) = tokio::sync::oneshot::channel();
        let (_yielded, yielding) = tokio::sync::oneshot::channel();
        let clock = crate::test_support_clock::start();
        let mut drive = Box::pin(super::drive_controller(
            (std::future::ready(()), yielding),
            |_| {
                let _ = stop.send(());
                readiness.stop();
            },
            None,
            (controller.as_mut(), leader.as_mut()),
            super::ControllerSignals {
                listeners: ends,
                drained,
                failed,
                standby: || None,
            },
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        assert_eq!(
            readiness.status(gang_types::health::health_check().ready_path()),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(
            std::net::TcpStream::connect(address).is_err(),
            "listener remains open during release"
        );
        drop((public, worker));
        Ok(())
    }

    #[tokio::test]
    async fn handoff_waits_for_drain_and_reports_lost_lease() {
        let (public, public_end) = tokio::sync::oneshot::channel();
        let (worker, worker_end) = tokio::sync::oneshot::channel();
        let (drained, drained_rx) = tokio::sync::oneshot::channel();
        let (_failed_tx, failed) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let mut controller = Box::pin(async move {
            let _ = finished.await;
            Ok(())
        });
        let mut leader = Box::pin(async move {
            let _ = drained_rx.await;
            Err(super::ServerError::LeadershipLost)
        });
        let mut background = Box::pin(std::future::pending());
        let mut pending = super::PendingStandbys::new();
        let ends = super::ListenerEnds::new(public_end, worker_end);
        let (_yielded, yielding) = tokio::sync::oneshot::channel();
        let clock = crate::test_support_clock::start();
        let mut drive = Box::pin(super::drive_controller(
            (std::future::ready(()), yielding),
            |_| {},
            None,
            (controller.as_mut(), leader.as_mut()),
            super::ControllerSignals {
                listeners: ends,
                drained,
                failed,
                standby: || None,
            },
            super::Maintenance {
                background: background.as_mut(),
                pending: &mut pending,
            },
            (&*clock, std::time::Duration::from_secs(10)),
        ));
        assert!(futures::poll!(drive.as_mut()).is_pending());
        let _ = finish.send(());
        assert!(matches!(
            drive.await,
            Err(super::ServerError::LeadershipLost)
        ));
        drop((public, worker));
    }

    #[tokio::test]
    async fn learned_history_interval_and_final_drain_flush() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            ControllerId, ControllerReady, ExpiringRecordPort, LearnedHistory, RecordId, RecordKey,
            RecordKind, SchedulerPort,
        };
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let release_drain = handles.scheduler.hold_drain();
        let drains = handles.scheduler.drains();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = std::sync::Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(3),
            ..gang_config::ActionsConfig::default()
        });
        let server = super::Server::prepare(parts, settings).await?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut run = Box::pin(server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ));
        assert!(futures::poll!(run.as_mut()).is_pending());

        let now = handles.clock.wall();
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([42; 32]),
        };
        let mut history = LearnedHistory {
            key,
            body: vec![1],
            expires_at: now + Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(now, &[history.clone()])
            .await?;
        handles.clock.advance(Duration::from_millis(2_999));
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert!(
            handles
                .repository
                .get(&key, handles.clock.wall())
                .await?
                .is_none()
        );

        handles.clock.advance(Duration::from_millis(1));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let first = handles
            .repository
            .get(&key, handles.clock.wall())
            .await?
            .expect("interval flush writes history");
        handles.clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let unchanged = handles
            .repository
            .get(&key, handles.clock.wall())
            .await?
            .expect("history remains live");
        assert_eq!(
            first.version, unchanged.version,
            "unchanged history must not be rewritten"
        );

        let _ = stop.send(());
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(drains.get(), 1);
        history.body = vec![2];
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), &[history.clone()])
            .await?;
        let contender = ControllerId::new("history-contender");
        assert_history_held_during_drain(&handles, key, first.version, &contender).await?;

        let _ = release_drain.send(());
        assert!(matches!(
            futures::poll!(run.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_released_history(&handles, history, first.version, &contender).await?;
        Ok(())
    }

    #[test]
    fn learned_history_budget_is_the_largest_record_not_the_sum() {
        let profiles = gang_reconciler::SourceProfiles::record_byte_limit();
        assert_eq!(
            super::history_byte_limit(std::num::NonZeroUsize::MIN),
            profiles
        );
        let scheduler = profiles.saturating_add(1);
        assert_eq!(super::history_byte_limit(scheduler), scheduler);
    }

    fn source_profile_checkpoint(
        policies: &[gang_config::FleetPolicy],
        settings: &crate::settings::ServerSettings,
        clock: &std::sync::Arc<gang_clock::TestClock>,
    ) -> gang_testkit::TestResult<Vec<gang_spi::LearnedHistory>> {
        use gang_reconciler::{
            FleetPass, HostedSnapshot, PassTime, Plan, Reconciler, SourceProfiles,
        };
        use gang_spi::SchedulerSnapshot;
        use std::time::Duration;

        let profiles = SourceProfiles::new(&settings.scaling, settings.actions.history_window);
        let mut model = Reconciler::new(
            policies,
            std::sync::Arc::clone(&settings.scaling),
            profiles.clone(),
            settings.actions.cpu_oversubscription,
        );
        let mut fleets: Vec<_> = policies.iter().cloned().map(FleetPass::new).collect();
        let mut queued = crate::test_support::queued_demand(gang_types::FleetSet::prefix(1)?);
        queued.source = Some("0123456789abcdef".parse()?);
        let mut snapshot = SchedulerSnapshot {
            queued: vec![queued],
            ..Default::default()
        };
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let mut plan = Plan::new();
        model.reconcile(&mut fleets, &hosted, PassTime::read(clock), &mut plan);
        clock.advance(Duration::from_secs(30));
        model.reconcile(&mut fleets, &hosted, PassTime::read(clock), &mut plan);
        let mut seed = Vec::new();
        profiles.snapshot(PassTime::read(clock), &mut seed)?;
        Ok(seed)
    }

    #[tokio::test]
    async fn leader_restores_source_profiles_and_keeps_them_through_final_flush()
    -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_reconciler::{PassTime, SourceProfiles};
        use gang_spi::{Acquisition, ControllerId, ControllerReady, LeaderLeasePort, Takeover};
        use std::sync::Arc;
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_hours(1),
            ..gang_config::ActionsConfig::default()
        });
        let seed = source_profile_checkpoint(parts.fleets.policies(), &settings, &handles.clock)?;
        assert_eq!(
            seed.len(),
            1,
            "first-ever activity must persist its checkpoint"
        );
        assert_eq!(seed[0].key.kind, gang_spi::RecordKind::SourceProfile);
        let now = handles.clock.wall();
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(
                &passive_candidate(&ControllerId::new("profile-seed")),
                now,
                now,
                now + Duration::from_mins(1),
                Takeover::Observe,
            )
            .await?
        else {
            panic!("fresh repository grants the seed lease");
        };
        let mut store =
            super::LearnedHistoryStore::new(super::history_byte_limit(std::num::NonZeroUsize::MIN));
        store.flush(&handles.repository, &lease, now, &seed).await?;
        handles.repository.release(&lease).await?;

        let expected = SourceProfiles::new(&settings.scaling, settings.actions.history_window);
        expected.restore(PassTime::read(&handles.clock), &seed);
        let server = super::Server::prepare(parts, settings).await?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut run = Box::pin(server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ));
        assert!(futures::poll!(run.as_mut()).is_pending());
        handles.clock.advance(Duration::from_mins(1));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let mut expected_records = Vec::new();
        expected.snapshot(PassTime::read(&handles.clock), &mut expected_records)?;
        assert_ne!(
            seed[0].body, expected_records[0].body,
            "the expired checkpoint settles once"
        );
        let _ = stop.send(());
        assert!(matches!(
            futures::poll!(run.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));

        let now = handles.clock.wall();
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(
                &passive_candidate(&ControllerId::new("profile-inspector")),
                now,
                now,
                now + Duration::from_mins(1),
                Takeover::Observe,
            )
            .await?
        else {
            panic!("server releases its lease after the final flush");
        };
        let mut store =
            super::LearnedHistoryStore::new(super::history_byte_limit(std::num::NonZeroUsize::MIN));
        let mut actual = Vec::new();
        store
            .load(&handles.repository, &lease, now, &mut actual)
            .await?;
        assert_eq!(
            actual, expected_records,
            "final flush appends restored profiles after the scheduler clears its records"
        );
        handles.repository.release(&lease).await?;
        Ok(())
    }

    #[tokio::test]
    async fn voluntary_yield_flushes_snapshot_without_waiting_for_drain() -> gang_testkit::TestResult
    {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ControllerHostIdentity, ControllerId, ControllerReady, LeaderLeasePort,
            LearnedHistory, RecordId, RecordKey, RecordKind, SchedulerPort, Takeover,
        };

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let release_drain = handles.scheduler.hold_drain();
        let drains = handles.scheduler.drains();
        let settings = crate::test_support::plain_settings();
        let renew = settings.leadership.renew_interval.get();
        let ttl = settings.leadership.ttl.get();
        let snapshot_bound =
            gang_types::handover::snapshot_timeout(renew, settings.leadership.call_timeout.get());
        let server = super::Server::prepare(parts, settings).await?;
        let readiness = server.readiness.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut run = Box::pin(server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([47; 32]),
        };
        let history = LearnedHistory {
            key,
            body: vec![1],
            expires_at: handles.clock.wall() + std::time::Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), std::slice::from_ref(&history))
            .await?;
        let (snapshot_entered, release_snapshot) = handles.repository.hold_next_record_apply();
        let successor = handles
            .repository
            .register_candidate(
                &ControllerId::new("yield-successor"),
                &ControllerHostIdentity::Persistent(gang_types::InstanceId::try_new("yield-host")?),
            )
            .await?;
        let now = handles.clock.wall();
        assert!(matches!(
            handles
                .repository
                .acquire(&successor, now, now, now + ttl, Takeover::Request)
                .await?,
            Acquisition::Contending(_)
        ));
        handles.clock.advance(renew);
        tokio::select! {
            result = run.as_mut() => {
                result?;
                return Err("yield exited before snapshot".into());
            }
            result = snapshot_entered => result?,
        }
        assert!(readiness.is_leading(), "snapshot precedes admission stop");
        assert_eq!(drains.get(), 0);
        handles
            .clock
            .advance(snapshot_bound.saturating_sub(std::time::Duration::from_millis(1)));
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert!(readiness.is_leading());
        assert_eq!(drains.get(), 0);
        handles.clock.advance(std::time::Duration::from_millis(1));
        tokio::select! {
            result = run.as_mut() => {
                result?;
                return Err("yield exited before drain".into());
            }
            () = drains.reached(1) => {}
        }
        assert!(!readiness.ready());
        assert!(
            release_snapshot.send(()).is_err(),
            "snapshot acknowledgement is bounded"
        );
        assert_yield_snapshot_survives_late_drain(
            &handles,
            &successor,
            ttl,
            history,
            release_drain,
            run.as_mut(),
        )
        .await?;
        stop.send(()).map_err(|()| "old process stopped early")?;
        run.await?;
        Ok(())
    }

    #[tokio::test]
    async fn newer_host_preempts_sigterm_drain_without_restarting_old_process()
    -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ControllerId, ControllerReady, LeaderLeasePort, LearnedHistory, RecordId,
            RecordKey, RecordKind, SchedulerPort, Takeover,
        };

        let (first, dials, old) = plain_parts();
        let mut refused = old.scheduler.refusal_connections();
        let (mut second, _dials, replacement) = plain_parts();
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("sigterm-successor");
        let release_drain = old.scheduler.hold_drain();
        let settings = plain_settings();
        let renew = settings.leadership.renew_interval.get();
        let ttl = settings.leadership.ttl.get();
        let bound =
            gang_types::handover::handover_window(renew, settings.leadership.call_timeout.get());
        let first = super::Server::prepare(first, settings).await?;
        let second = super::Server::prepare(second, plain_settings()).await?;
        let second_state = second.readiness.clone();
        let (stop_first, stopped_first) = super::stop_channel();
        let (stop_second, stopped_second) = super::stop_channel();
        let mut first = Box::pin(first.run(stopped_first, ControllerReady::channel().0));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let now = old.clock.wall();
        let history = LearnedHistory {
            key: RecordKey {
                kind: RecordKind::ResourceHistory,
                id: RecordId::new([48; 32]),
            },
            body: vec![1],
            expires_at: now + std::time::Duration::from_mins(2),
        };
        old.scheduler
            .restore_learned_history(now, std::slice::from_ref(&history))
            .await?;
        stop_first
            .send(())
            .map_err(|()| "old process stopped early")?;
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert_eq!(old.scheduler.drains().get(), 1);
        assert!(matches!(
            old.repository
                .acquire(
                    &passive_candidate(&ControllerId::new("drain-observer")),
                    now,
                    now,
                    now + ttl,
                    Takeover::Observe,
                )
                .await?,
            Acquisition::Held(_)
        ));
        let mut second = Box::pin(second.run(stopped_second, ControllerReady::channel().0));
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(second_state.ready());
        assert!(!second_state.is_leading());
        let started = old.clock.wall();
        old.clock.advance(renew);
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(
            second_state.is_leading(),
            "SIGTERM drain must not block the successor"
        );
        assert!(old.clock.wall().saturating_duration_since(started) <= bound);
        assert_eq!(old.scheduler.drains().get(), 1);
        assert_eq!(old.scheduler.controller_runs().get(), 1);
        assert_refusal_intake(&old.scheduler, &dials, &mut refused, first.as_mut()).await?;
        assert_sigterm_snapshot_durable(&old, &replacement, history, release_drain, first.as_mut())
            .await?;
        stop_second
            .send(())
            .map_err(|()| "successor stopped early")?;
        second.await?;
        Ok(())
    }

    async fn assert_refusal_intake(
        scheduler: &gang_testkit::FakeScheduler,
        dials: &crate::test_support_deployment::Dials,
        refused: &mut tokio::sync::mpsc::UnboundedReceiver<gang_testkit::Received>,
        mut run: Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult {
        assert_eq!(scheduler.refusal_runs().get(), 1);
        let _public = dials.public.dial();
        let _worker = dials.worker.dial();
        assert!(futures::poll!(run.as_mut()).is_pending());
        let first = refused.try_recv()?;
        let second = refused.try_recv()?;
        assert!(matches!(
            (first.side, second.side),
            (gang_testkit::Side::Public, gang_testkit::Side::Worker)
                | (gang_testkit::Side::Worker, gang_testkit::Side::Public)
        ));
        assert_eq!(scheduler.controller_runs().get(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn prepared_first_term_keeps_standby_available_during_later_preparation()
    -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_spi::{ControllerId, ControllerReady};
        let (first, _dials, old) = plain_parts();
        let (mut second, _dials, new) = plain_parts();
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("sticky-successor");
        let release_first_drain = old.scheduler.hold_drain();
        let renew = plain_settings().leadership.renew_interval.get();
        let first = super::Server::prepare(first, plain_settings()).await?;
        let second = super::Server::prepare(second, plain_settings()).await?;
        let (stop_first, stopped_first) = super::stop_channel();
        let (stop_second, stopped_second) = super::stop_channel();
        let mut first = Box::pin(first.run(stopped_first, ControllerReady::channel().0));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let mut second = Box::pin(second.run(stopped_second, ControllerReady::channel().0));
        assert!(futures::poll!(second.as_mut()).is_pending());
        old.clock.advance(renew);
        let drains = old.scheduler.drains();
        tokio::select! {
            result = first.as_mut() => { result?; return Err("first term exited before drain".into()); },
            () = drains.reached(1) => {}
        }
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(new.scheduler.controller_runs().get(), 1);
        let _release_second_ready = old.scheduler.hold_ready();
        let release_second_drain = old.scheduler.hold_drain();
        release_first_drain
            .send(())
            .map_err(|()| "first drain missing")?;
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert_eq!(old.scheduler.controller_runs().get(), 2);
        stop_first
            .send(())
            .map_err(|()| "old process stopped early")?;
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert_eq!(old.scheduler.drains().get(), 2);
        assert_eq!(old.scheduler.refusal_runs().get(), 2);
        assert!(old.scheduler.standby_services().await.is_some());
        release_second_drain
            .send(())
            .map_err(|()| "second drain missing")?;
        first.await?;
        stop_second
            .send(())
            .map_err(|()| "successor stopped early")?;
        second.await?;
        Ok(())
    }

    #[tokio::test]
    async fn stopped_refusal_flush_does_not_block_next_term_and_retirement_waits()
    -> gang_testkit::TestResult {
        assert_pending_refusal_continuity(false).await
    }

    #[tokio::test]
    async fn late_queued_refusal_failure_after_new_term_is_fatal() -> gang_testkit::TestResult {
        assert_pending_refusal_continuity(true).await
    }

    async fn assert_pending_refusal_continuity(fail: bool) -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_spi::{ControllerId, ControllerReady};
        let (first, dials, old) = plain_parts();
        let (mut second, _dials, new) = plain_parts();
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("queued-successor");
        let release_drain = old.scheduler.hold_drain();
        let release_refusal = old.scheduler.hold_refusal_drain();
        let renew = plain_settings().leadership.renew_interval.get();
        let first = super::Server::prepare(first, plain_settings()).await?;
        let second = super::Server::prepare(second, plain_settings()).await?;
        let second_state = second.readiness.clone();
        let (stop_first, stopped_first) = super::stop_channel();
        let (stop_second, stopped_second) = super::stop_channel();
        let mut first = Box::pin(first.run(stopped_first, ControllerReady::channel().0));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let mut second = Box::pin(second.run(stopped_second, ControllerReady::channel().0));
        assert!(futures::poll!(second.as_mut()).is_pending());
        old.clock.advance(renew);
        let drains = old.scheduler.drains();
        tokio::select! {
            result = first.as_mut() => { result?; return Err("old process exited before drain".into()); },
            () = drains.reached(1) => {}
        }
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(second_state.is_leading());
        assert_eq!(new.scheduler.controller_runs().get(), 1);
        assert_eq!(old.scheduler.refusal_runs().get(), 1);
        assert_next_term_intake(&old.scheduler, &dials, release_drain, first.as_mut()).await?;
        finish_pending_refusal(&old, stop_first, release_refusal, first.as_mut(), fail).await?;
        stop_second
            .send(())
            .map_err(|()| "successor stopped early")?;
        second.await?;
        Ok(())
    }

    async fn finish_pending_refusal(
        old: &crate::test_support_deployment::Handles,
        stop: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Sender<()>,
        mut run: Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
        fail: bool,
    ) -> gang_testkit::TestResult {
        if fail {
            old.scheduler.fail_refusals(true);
            release
                .send(())
                .map_err(|()| "queued denial was cancelled")?;
            assert!(matches!(run.await, Err(super::ServerError::Controller(_))));
        } else {
            stop.send(()).map_err(|()| "old process stopped early")?;
            assert!(
                futures::poll!(run.as_mut()).is_pending(),
                "retirement must finish queued denials"
            );
            assert_eq!(old.scheduler.controller_runs().get(), 2);
            release
                .send(())
                .map_err(|()| "queued denial was cancelled")?;
            run.await?;
        }
        Ok(())
    }

    async fn assert_next_term_intake(
        scheduler: &gang_testkit::FakeScheduler,
        dials: &crate::test_support_deployment::Dials,
        release_drain: tokio::sync::oneshot::Sender<()>,
        mut run: Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult {
        let mut received = scheduler.connections();
        release_drain
            .send(())
            .map_err(|()| "old controller was not draining")?;
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(scheduler.controller_runs().get(), 2);
        assert_eq!(scheduler.refusal_runs().get(), 1);
        let _public = dials.public.dial();
        let _worker = dials.worker.dial();
        assert!(futures::poll!(run.as_mut()).is_pending());
        let first = received.try_recv()?;
        let second = received.try_recv()?;
        assert!(matches!(
            (first.side, second.side),
            (gang_testkit::Side::Public, gang_testkit::Side::Worker)
                | (gang_testkit::Side::Worker, gang_testkit::Side::Public)
        ));
        Ok(())
    }

    async fn assert_sigterm_snapshot_durable(
        old: &crate::test_support_deployment::Handles,
        replacement: &crate::test_support_deployment::Handles,
        history: gang_spi::LearnedHistory,
        release_drain: tokio::sync::oneshot::Sender<()>,
        first: std::pin::Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{ExpiringRecordPort, SchedulerPort};
        let persisted = old
            .repository
            .get(&history.key, old.clock.wall())
            .await?
            .ok_or("yield snapshot missing")?;
        let mut restored = Vec::new();
        replacement
            .scheduler
            .learned_history(old.clock.wall(), &mut restored)
            .await?;
        assert_eq!(restored, vec![history.clone()]);
        assert_stopped_term_keeps_snapshot(old, history, persisted.version, release_drain, first)
            .await
    }

    async fn assert_stopped_term_keeps_snapshot(
        old: &crate::test_support_deployment::Handles,
        mut history: gang_spi::LearnedHistory,
        version: gang_spi::RecordVersion,
        release_drain: tokio::sync::oneshot::Sender<()>,
        mut first: std::pin::Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{ExpiringRecordPort, SchedulerPort};
        history.body = vec![2];
        let key = history.key;
        old.scheduler
            .restore_learned_history(old.clock.wall(), &[history])
            .await?;
        assert!(futures::poll!(first.as_mut()).is_pending());
        release_drain
            .send(())
            .map_err(|()| "old controller stopped before drain release")?;
        assert!(matches!(
            futures::poll!(first.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(
            old.scheduler.controller_runs().get(),
            1,
            "shutdown must not create another term"
        );
        assert_eq!(
            old.repository
                .get(&key, old.clock.wall())
                .await?
                .ok_or("snapshot missing")?
                .version,
            version
        );
        Ok(())
    }

    async fn assert_yield_snapshot_survives_late_drain(
        handles: &crate::test_support_deployment::Handles,
        successor: &gang_spi::LeaseCandidate,
        ttl: std::time::Duration,
        history: gang_spi::LearnedHistory,
        release_drain: tokio::sync::oneshot::Sender<()>,
        mut run: std::pin::Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ExpiringRecordPort, LeaderLeasePort, LearnedHistory, SchedulerPort,
            Takeover,
        };
        let key = history.key;
        let now = handles.clock.wall();
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(successor, now, now, now + ttl, Takeover::Request)
            .await?
        else {
            return Err("voluntary lease release waited for admitted drain".into());
        };
        let persisted = handles
            .repository
            .get(&key, now)
            .await?
            .ok_or("yield snapshot missing")?;
        let mut store = super::LearnedHistoryStore::new(
            std::num::NonZeroUsize::new(1024).ok_or("history bound must be nonzero")?,
        );
        let mut loaded = Vec::new();
        store
            .load(&handles.repository, &lease, now, &mut loaded)
            .await?;
        assert_eq!(loaded, vec![history.clone()]);
        handles
            .scheduler
            .restore_learned_history(
                now,
                &[LearnedHistory {
                    body: vec![2],
                    ..history
                }],
            )
            .await?;
        release_drain
            .send(())
            .map_err(|()| "old controller stopped before drain release")?;
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(
            handles
                .repository
                .get(&key, now)
                .await?
                .ok_or("persisted snapshot missing")?
                .version,
            persisted.version,
            "late old-term measurements cannot write after lease release"
        );
        Ok(())
    }

    async fn assert_history_held_during_drain(
        handles: &crate::test_support_deployment::Handles,
        key: gang_spi::RecordKey,
        first_version: gang_spi::RecordVersion,
        contender: &gang_spi::ControllerId,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{Acquisition, ExpiringRecordPort, LeaderLeasePort};
        use std::time::Duration;

        let now = handles.clock.wall();
        assert!(matches!(
            handles
                .repository
                .acquire(
                    &passive_candidate(contender),
                    now,
                    gang_clock::Timestamp::from_unix_millis(0),
                    now + Duration::from_mins(1),
                    gang_spi::Takeover::Observe,
                )
                .await?,
            Acquisition::Held(_)
        ));
        assert_eq!(
            handles
                .repository
                .get(&key, now)
                .await?
                .expect("pre-drain snapshot")
                .version,
            first_version
        );
        Ok(())
    }

    async fn assert_released_history(
        handles: &crate::test_support_deployment::Handles,
        history: gang_spi::LearnedHistory,
        first_version: gang_spi::RecordVersion,
        contender: &gang_spi::ControllerId,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{Acquisition, ExpiringRecordPort, LeaderLeasePort};
        use std::num::NonZeroUsize;
        use std::time::Duration;

        let now = handles.clock.wall();
        let final_head = handles
            .repository
            .get(&history.key, now)
            .await?
            .expect("final history head");
        assert_ne!(final_head.version, first_version);
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(
                &passive_candidate(contender),
                now,
                gang_clock::Timestamp::from_unix_millis(0),
                now + Duration::from_mins(1),
                gang_spi::Takeover::Observe,
            )
            .await?
        else {
            panic!("lease was not released after final flush");
        };
        let mut store =
            super::LearnedHistoryStore::new(NonZeroUsize::new(1024).expect("nonzero bound"));
        let mut loaded = Vec::new();
        store
            .load(&handles.repository, &lease, now, &mut loaded)
            .await?;
        assert_eq!(loaded, vec![history]);
        Ok(())
    }

    #[tokio::test]
    async fn slow_flush_does_not_extend_the_next_interval() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            ControllerReady, ExpiringRecordPort, LearnedHistory, RecordId, RecordKey, RecordKind,
            SchedulerPort,
        };
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = std::sync::Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(3),
            ..gang_config::ActionsConfig::default()
        });
        let server = super::Server::prepare(parts, settings).await?;
        let mut run = Box::pin(server.run(std::future::pending(), ControllerReady::channel().0));
        assert!(futures::poll!(run.as_mut()).is_pending());

        let now = handles.clock.wall();
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([46; 32]),
        };
        let mut history = LearnedHistory {
            key,
            body: vec![1],
            expires_at: now + Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(now, &[history.clone()])
            .await?;
        let (committed, release) = handles.repository.hold_next_record_apply();
        handles.clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(run.as_mut()).is_pending());
        committed.await.expect("first interval committed");
        let first = handles
            .repository
            .get(&key, handles.clock.wall())
            .await?
            .expect("first head");
        history.body = vec![2];
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), &[history])
            .await?;
        handles.clock.advance(Duration::from_secs(2));
        let _ = release.send(());
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(
            handles
                .repository
                .get(&key, handles.clock.wall())
                .await?
                .expect("first head")
                .version,
            first.version
        );

        handles.clock.advance(Duration::from_secs(1));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let second = handles
            .repository
            .get(&key, handles.clock.wall())
            .await?
            .expect("second head");
        assert_ne!(
            second.version, first.version,
            "the second flush starts at six seconds, not eight"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restoration_retry_keeps_the_term_batch_sequence() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ApplyRecords, ControllerId, ControllerReady, ExpiringRecordPort,
            LeaderLeasePort, LearnedHistory, RecordBatchSeq, RecordId, RecordKey, RecordKind,
            RecordVersion, SchedulerPort,
        };
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = std::sync::Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(3),
            ..gang_config::ActionsConfig::default()
        });
        let retry = settings.leadership.renew_interval.get();
        let server = super::Server::prepare(parts, settings).await?;
        handles.repository.fail_records(true);
        let mut run = Box::pin(server.run(std::future::pending(), ControllerReady::channel().0));
        assert!(futures::poll!(run.as_mut()).is_pending());
        handles.repository.fail_records(false);
        handles.clock.advance(retry);
        assert!(futures::poll!(run.as_mut()).is_pending());

        let now = handles.clock.wall();
        let key = RecordKey {
            kind: RecordKind::DurationHistory,
            id: RecordId::new([43; 32]),
        };
        let history = LearnedHistory {
            key,
            body: vec![7],
            expires_at: now + Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(now, &[history])
            .await?;
        handles.clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let now = handles.clock.wall();
        assert!(
            handles.repository.get(&key, now).await?.is_some(),
            "retry followed by flush"
        );
        let Acquisition::Held(lease) = handles
            .repository
            .acquire(
                &passive_candidate(&ControllerId::new("sequence-observer")),
                now,
                gang_clock::Timestamp::from_unix_millis(0),
                now + Duration::from_mins(1),
                gang_spi::Takeover::Observe,
            )
            .await?
        else {
            panic!("server should hold the lease");
        };
        assert_eq!(
            handles
                .repository
                .apply(
                    &lease,
                    RecordBatchSeq::new(3),
                    RecordVersion::new(1),
                    now,
                    &[]
                )
                .await?,
            ApplyRecords::Fenced,
            "failed load must consume a sequence number",
        );
        Ok(())
    }

    #[tokio::test]
    async fn lapsed_renewal_keeps_serving_and_flushes_after_recovery() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ControllerId, ControllerReady, ExpiringRecordPort, LeaderLeasePort,
            LearnedHistory, RecordId, RecordKey, RecordKind, SchedulerPort,
        };
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = std::sync::Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(3),
            ..gang_config::ActionsConfig::default()
        });
        let ttl = settings.leadership.ttl.get();
        let renew = settings.leadership.renew_interval.get();
        let server = super::Server::prepare(parts, settings).await?;
        let snapshots = handles.scheduler.snapshots();
        let mut run = Box::pin(server.run(std::future::pending(), ControllerReady::channel().0));
        assert!(futures::poll!(run.as_mut()).is_pending());
        let contender = ControllerId::new("lapse-observer");
        let zero = gang_clock::Timestamp::from_unix_millis(0);
        let now = handles.clock.wall();
        let Acquisition::Held(before) = handles
            .repository
            .acquire(
                &passive_candidate(&contender),
                now,
                zero,
                now + ttl,
                gang_spi::Takeover::Observe,
            )
            .await?
        else {
            panic!("server should hold the lease");
        };
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([45; 32]),
        };
        let history = LearnedHistory {
            key,
            body: vec![3],
            expires_at: now + ttl + Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(now, &[history])
            .await?;
        let passes = snapshots.get();
        handles.repository.fail_lease(true);
        handles.clock.advance(ttl + Duration::from_secs(1));
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert!(
            snapshots.get() > passes,
            "the controller keeps scheduling while renewal fails"
        );
        assert!(
            handles
                .repository
                .get(&key, handles.clock.wall())
                .await?
                .is_none(),
            "lapsed authority must not write history"
        );

        handles.repository.fail_lease(false);
        handles.clock.advance(renew);
        assert!(futures::poll!(run.as_mut()).is_pending());
        let Acquisition::Held(after) = handles
            .repository
            .acquire(
                &passive_candidate(&contender),
                handles.clock.wall(),
                zero,
                handles.clock.wall() + ttl,
                gang_spi::Takeover::Observe,
            )
            .await?
        else {
            panic!("server should still hold the lease");
        };
        assert_eq!(after.epoch, before.epoch);
        assert!(
            handles
                .repository
                .get(&key, handles.clock.wall())
                .await?
                .is_some(),
            "pending history flushes after renewal"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_periodic_commit_is_reconciled_before_final_flush() -> gang_testkit::TestResult
    {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ControllerId, ControllerReady, ExpiringRecordPort, LeaderLeasePort,
            LearnedHistory, RecordId, RecordKey, RecordKind, SchedulerPort,
        };
        use std::num::NonZeroUsize;
        use std::time::Duration;

        let (parts, _dials, handles) = crate::test_support::plain_parts();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = std::sync::Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(3),
            ..gang_config::ActionsConfig::default()
        });
        let server = super::Server::prepare(parts, settings).await?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut run = Box::pin(server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ));
        assert!(futures::poll!(run.as_mut()).is_pending());

        let now = handles.clock.wall();
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([44; 32]),
        };
        let mut history = LearnedHistory {
            key,
            body: vec![1],
            expires_at: now + Duration::from_mins(2),
        };
        handles
            .scheduler
            .restore_learned_history(now, &[history.clone()])
            .await?;
        let (entered, release) = handles.repository.hold_next_record_apply();
        handles.clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(run.as_mut()).is_pending());
        entered
            .await
            .expect("periodic apply committed before acknowledgement");
        assert!(
            handles
                .repository
                .get(&key, handles.clock.wall())
                .await?
                .is_some()
        );

        history.body = vec![2];
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), &[history.clone()])
            .await?;
        let _ = stop.send(());
        assert!(matches!(
            futures::poll!(run.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(
            release.send(()).is_err(),
            "periodic acknowledgement must have been cancelled"
        );

        let contender = ControllerId::new("history-successor");
        let now = handles.clock.wall();
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(
                &passive_candidate(&contender),
                now,
                gang_clock::Timestamp::from_unix_millis(0),
                now + Duration::from_mins(1),
                gang_spi::Takeover::Observe,
            )
            .await?
        else {
            panic!("lease was not released after final flush");
        };
        let mut store =
            super::LearnedHistoryStore::new(NonZeroUsize::new(1024).expect("nonzero bound"));
        let mut loaded = Vec::new();
        store
            .load(&handles.repository, &lease, now, &mut loaded)
            .await?;
        assert_eq!(loaded, vec![history]);
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
