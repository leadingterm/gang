//! The server: prepares the authentication core, the gates and the control API, then runs the
//! scheduler's controller and the control loop until shutdown.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::{Map, Shared};
use futures::{FutureExt, StreamExt};
use gang_clock::Clock;
use gang_config::{DashboardConfig, EndpointTrust, Fleets, LeadershipConfig};
use gang_mtls::SubjectName;
use gang_reconciler::{PassTime, SourceProfiles};
use gang_spi::{
    AuthenticationPort, CacheStorePort, ClientCertificates, ControllerConnections, ControllerDrain,
    ControllerHostIdentity, ControllerHostPort, ControllerHostState, ControllerId, ControllerReady,
    ControllerSpec, CredentialLifecyclePort, HostMilestone, Lease, Listening, ObservationSink,
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

impl ListenerEnds {
    fn new(public: oneshot::Receiver<()>, worker: oneshot::Receiver<()>) -> Self {
        Self {
            public: Some(public),
            worker: Some(worker),
        }
    }
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

/// Prepares once, promotes at most once, then freezes and releases authority before long drain.
async fn run_leader<D: Deployment>(
    context: &LeadContext<'_, D>,
    prepared: impl Future<Output = bool>,
    ready: ControllerReady,
    shutdown: impl Future<Output = ()> + Clone,
    outgoing: &mut Option<crate::protection::Outgoing>,
) -> Result<(), ServerError> {
    tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        prepared = prepared => if !prepared { return Err(ServerError::ControllerEnded); },
    }
    context.readiness.serve();
    let profiles = SourceProfiles::new(context.scaling, context.actions.history_window);
    let mut history = tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        result = prepare_history(context, &profiles) => result?,
    };
    let host = tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        result = host_identity(context.provisioner, &*context.core.host.clock, context.sidecar_retry_interval.get()) => result.map_err(ServerError::HostIdentity)?,
    };
    let state = tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        result = report_prepared(context.provisioner, &*context.core.host.clock,
            context.sidecar_retry_interval.get()) => result.map_err(ServerError::PreparedMilestone)?,
    };
    if state == ControllerHostState::Retiring {
        return Ok(());
    }
    let candidate = tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        result = context.leadership.register_candidate(&host) => result.map_err(ServerError::CandidateRegistration)?,
    };
    ready.signal();
    let lease = tokio::select! {
        biased;
        () = shutdown.clone() => return Ok(()),
        result = context.leadership.acquire(&candidate, context.provisioner) => match result.map_err(ServerError::HostIdentity)? {
            Some(lease) => lease,
            None => return Ok(()),
        },
    };
    let mut renewing = lease.clone();
    let result = activate_and_serve(
        context,
        &lease,
        &mut renewing,
        &profiles,
        &mut history,
        shutdown,
    )
    .await;
    if matches!(&result, Err(ServerError::LeadershipLost)) {
        context.readiness.stop();
    }
    freeze_and_release(
        context,
        &lease,
        &mut renewing,
        &profiles,
        Some(&mut history),
        outgoing,
    )
    .await?;
    result
}

/// Applies prepared state under authority and shares one deadline with final promotion.
async fn activate_and_serve<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &Lease,
    renewing: &mut Lease,
    profiles: &SourceProfiles,
    history: &mut (LearnedHistoryStore, Vec<gang_spi::LearnedHistory>),
    shutdown: impl Future<Output = ()> + Clone,
) -> Result<(), ServerError> {
    let (store, records) = history;
    let hold = context.leadership.hold(renewing);
    tokio::pin!(hold);
    let promotion_deadline =
        context.core.host.clock.monotonic() + context.leadership.call_timeout();
    let activated = tokio::select! {
        biased;
        end = &mut hold => match end { LeaseEnd::Lost => Err(ServerError::LeadershipLost), LeaseEnd::YieldRequested => Ok(false) },
        () = shutdown.clone() => Ok(false),
        result = context.core.host.clock.timeout_at(promotion_deadline, store.activate(
            &context.core.host.repository, lease, context.core.host.clock.wall())) =>
            result.map_err(ServerError::HandoffTimedOut).and_then(|result| result.map(|()| true)
                .map_err(|error| match error {
                    HistoryError::Fenced => ServerError::LeadershipLost,
                    error => ServerError::Controller(PortError::backend(error)),
                })),
    };
    match activated {
        Ok(true) => {
            let work = lead_work(context, profiles, shutdown.clone(), promotion_deadline);
            let periodic = flush_history_periodically(context, lease, profiles, store, records);
            tokio::pin!(work, periodic);
            tokio::select! {
                biased;
                end = &mut hold => match end { LeaseEnd::Lost => Err(ServerError::LeadershipLost), LeaseEnd::YieldRequested => Ok(()) },
                () = shutdown.clone() => Ok(()),
                never = &mut periodic => match never {},
                result = &mut work => result,
            }
        }
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Stops mutation producers before publishing the frozen worker set and freeing authority.
async fn freeze_and_release<D: Deployment>(
    context: &LeadContext<'_, D>,
    lease: &Lease,
    renewing: &mut Lease,
    profiles: &SourceProfiles,
    history: Option<&mut (LearnedHistoryStore, Vec<gang_spi::LearnedHistory>)>,
    outgoing: &mut Option<crate::protection::Outgoing>,
) -> Result<(), ServerError> {
    let keep_authority = async {
        loop {
            if context.leadership.hold(renewing).await == LeaseEnd::Lost {
                return;
            }
        }
    };
    let preparation = async {
        // History preparation is still on the prepared old host, before visible freeze.
        if let Some((store, records)) = history {
            match context
                .core
                .host
                .clock
                .timeout(
                    context.leadership.call_timeout(),
                    flush_history(context, lease, profiles, store, records),
                )
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "handoff learned history snapshot failed"),
                Err(error) => tracing::warn!(%error, "handoff learned history snapshot timed out"),
            }
        }
        context.readiness.stop();
        let mut drain = ControllerDrain::default();
        loop {
            let prepare = async {
                context.scheduler.hand_off(&mut drain).await?;
                if outgoing.is_none() {
                    *outgoing = Some(crate::protection::Outgoing::new(&lease.holder, &drain)?);
                }
                if let Some(protection) = outgoing {
                    protection
                        .install(
                            &context.core.host.repository,
                            context.core.host.clock.wall(),
                            context.leadership.protection_ttl(),
                        )
                        .await?;
                }
                Ok::<(), PortError>(())
            };
            match context
                .core
                .host
                .clock
                .timeout(context.leadership.call_timeout(), prepare)
                .await
            {
                Ok(Ok(())) => return,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "handoff protection unavailable; retaining authority and retrying")
                }
                Err(error) => {
                    tracing::warn!(%error, "handoff preparation timed out; retaining authority and retrying")
                }
            }
            context
                .core
                .host
                .clock
                .sleep(context.leadership.renew_interval())
                .await;
        }
    };
    tokio::pin!(keep_authority, preparation);
    tokio::select! {
        () = &mut keep_authority => return Err(ServerError::LeadershipLost),
        () = &mut preparation => {}
    }
    context.leadership.release(lease).await;
    Ok(())
}

/// Completes read-only import before launch completion or a visible takeover request.
async fn prepare_history<D: Deployment>(
    context: &LeadContext<'_, D>,
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
    // No authority or body baseline is retained: only per-key imported fingerprints.
    let mut store = LearnedHistoryStore::new(limit);
    let mut records = Vec::new();
    loop {
        let attempt = async {
            store
                .prepare(&context.core.host.repository, clock.wall(), &mut records)
                .await?;
            let time = PassTime::read(clock);
            context
                .scheduler
                .restore_learned_history(time.wall, &records)
                .await
                .map_err(HistoryError::Storage)?;
            profiles.restore(time, &records);
            context
                .scheduler
                .learned_history(time.wall, &mut records)
                .await
                .map_err(HistoryError::Storage)?;
            profiles
                .snapshot(time, &mut records)
                .map_err(HistoryError::Storage)?;
            store.prepared(&records);
            records.clear();
            Ok::<(), HistoryError>(())
        }
        .await;
        match attempt {
            Ok(()) => break,
            Err(error) => {
                tracing::warn!(%error, "learned history restore failed; retrying before admission");
            }
        }
        clock.sleep(context.leadership.renew_interval()).await;
    }
    synchronize_revocations(context.core, context.revoked_certificates)
        .await
        .map_err(ServerError::Revocations)?;
    if let Some(dashboard) = context.dashboard
        && let Err(error) = dashboard.reload_from(&context.core.host.repository).await
    {
        tracing::warn!(%error, "dashboard history reload failed during preparation");
    }
    Ok((store, records))
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

/// Passive registration can take longer than the handover window. No takeover is
/// requested until it completes; shutdown/retirement cancels the caller's future.
async fn report_prepared<H: ControllerHostPort, C: Clock>(
    host: &H,
    clock: &C,
    interval: Duration,
) -> Result<ControllerHostState, PortError> {
    loop {
        let attempt = async {
            let state = host.state().await?;
            if state == ControllerHostState::Retiring {
                return Ok(state);
            }
            host.reached(HostMilestone::Prepared).await?;
            Ok(ControllerHostState::Active)
        }
        .await;
        match attempt {
            Ok(state) => return Ok(state),
            Err(error @ PortError::Unavailable(_)) => {
                tracing::warn!(%error, "cannot prepare controller host; retrying before takeover");
            }
            Err(error) => return Err(error),
        }
        clock.sleep(interval).await;
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
    stop: impl Future<Output = ()>,
    promotion_deadline: gang_clock::Tick,
) -> Result<(), ServerError> {
    let baseline = context
        .core
        .host
        .clock
        .timeout(
            promotion_deadline.saturating_duration_since(context.core.host.clock.monotonic()),
            async {
                if context
                    .provisioner
                    .state()
                    .await
                    .map_err(ServerError::HostIdentity)?
                    == ControllerHostState::Retiring
                {
                    return Err(ServerError::HostIdentity(PortError::unavailable(
                        "controller host is retiring",
                    )));
                }
                let baseline = match context.dashboard {
                    Some(_) => Some(term_baseline(context).await),
                    None => None,
                };
                context.leading.set(context.core.host.clock.wall());
                context.readiness.lead();
                report_serving(
                    context.provisioner,
                    &*context.core.host.clock,
                    context.sidecar_retry_interval.get(),
                    promotion_deadline
                        .saturating_duration_since(context.core.host.clock.monotonic()),
                )
                .await
                .map_err(ServerError::ServingMilestone)?;
                Ok::<_, ServerError>(baseline)
            },
        )
        .await
        .map_err(ServerError::HandoffTimedOut)??;
    let sidecar = retry_sidecar(context);
    let clock = &context.core.host.clock;
    let control = ControlLoop::new(
        context.fleets,
        context.control_settings,
        Arc::clone(context.scaling),
        profiles.clone(),
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

async fn observe_prepared(prepared: oneshot::Receiver<()>, observed: Arc<AtomicBool>) -> bool {
    let ready = prepared.await.is_ok();
    if ready {
        observed.store(true, Ordering::Release);
    }
    ready
}

async fn drain_original<D: Deployment>(
    context: &LeadContext<'_, D>,
    outgoing: &mut Option<crate::protection::Outgoing>,
) -> Result<(), ServerError> {
    let clock = &context.core.host.clock;
    let mut drain = ControllerDrain::default();
    let mut incoming = ControllerDrain::default();
    if let Some(protection) = outgoing.as_ref() {
        protection.snapshot(&mut drain);
    }
    loop {
        // Poll the engine and renew the last confirmed frozen set concurrently. A failed or
        // cancelled snapshot must never erase protection for an attempt still owned by the old engine.
        let observation = clock.timeout(
            context.leadership.call_timeout(),
            context.scheduler.hand_off(&mut incoming),
        );
        let protection = clock.timeout(context.leadership.call_timeout(), async {
            if let Some(protection) = outgoing.as_mut() {
                protection
                    .refresh(
                        &context.core.host.repository,
                        &drain,
                        clock.wall(),
                        context.leadership.protection_ttl(),
                    )
                    .await?;
            }
            Ok::<(), PortError>(())
        });
        let heartbeat = clock.timeout(
            context.leadership.call_timeout(),
            context.provisioner.heartbeat(),
        );
        let (observed, protected, heartbeat) = tokio::join!(observation, protection, heartbeat);
        match observed {
            Ok(Ok(())) => {
                std::mem::swap(&mut drain, &mut incoming);
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "cannot observe original drain; retaining confirmed worker protection")
            }
            Err(error) => tracing::warn!(%error, "original drain observation timed out"),
        }
        match protected {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "cannot renew old-worker protection"),
            Err(error) => tracing::warn!(%error, "old-worker protection renewal timed out"),
        }
        match heartbeat {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "cannot heartbeat controller retirement"),
            Err(error) => tracing::warn!(%error, "controller heartbeat timed out"),
        }
        if drain.complete {
            if let Some(protection) = outgoing.as_mut() {
                match clock
                    .timeout(
                        context.leadership.call_timeout(),
                        protection.refresh(
                            &context.core.host.repository,
                            &drain,
                            clock.wall(),
                            context.leadership.protection_ttl(),
                        ),
                    )
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "cannot remove completed old-worker protection")
                    }
                    Err(error) => {
                        tracing::warn!(%error, "completed old-worker protection removal timed out")
                    }
                }
            }
            return Ok(());
        }
        clock.sleep(context.leadership.renew_interval()).await;
    }
}

/// Routing queries and completion may span lifecycle transitions; heartbeat while awaiting
/// the platform and retry an unavailable response without pretending that it succeeded.
async fn finish_host<D: Deployment>(
    context: &LeadContext<'_, D>,
    milestone: HostMilestone,
) -> Result<(), ServerError> {
    loop {
        let reached = context.provisioner.reached(milestone);
        tokio::pin!(reached);
        let outcome = loop {
            tokio::select! {
                outcome = &mut reached => break outcome,
                () = context.core.host.clock.sleep(context.leadership.renew_interval()) => {
                    if let Err(error) = context.provisioner.heartbeat().await {
                        tracing::warn!(%error, "cannot heartbeat controller retirement");
                    }
                }
            }
        };
        match outcome {
            Ok(()) => return Ok(()),
            Err(error @ PortError::Unavailable(_)) => {
                tracing::warn!(%error, ?milestone, "controller milestone unavailable; retrying");
                context
                    .core
                    .host
                    .clock
                    .sleep(context.leadership.renew_interval())
                    .await;
            }
            Err(error) => return Err(ServerError::HostIdentity(error)),
        }
    }
}

/// Drives one original engine for the full process lifetime, including passive refusal,
/// authority transfer, old work completion and actual platform routing-away.
async fn drive_original<D: Deployment>(
    context: &LeadContext<'_, D>,
    spec: &ControllerSpec<'_>,
    (connections, mut ends, stop_sender): (
        ControllerListening<D>,
        ListenerEnds,
        oneshot::Sender<()>,
    ),
    ready: ControllerReady,
    shutdown: impl Future<Output = ()> + Clone,
    mut background: Pin<&mut impl Future<Output = Infallible>>,
) -> Result<(), ServerError> {
    let (controller_ready, prepared) = ControllerReady::channel();
    let observed = Arc::new(AtomicBool::new(false));
    let prepared = observe_prepared(prepared, Arc::clone(&observed));
    let mut controller = Box::pin(context.scheduler.run_controller(
        spec,
        connections,
        controller_ready,
    ));
    let mut outgoing = None;
    let result = {
        let leader = run_leader(context, prepared, ready, shutdown, &mut outgoing);
        tokio::pin!(leader);
        tokio::select! {
            biased;
            result = &mut controller => return result.map_err(ServerError::Controller).and(Err(ServerError::ControllerEnded)),
            error = listener_stopped("public", &mut ends.public) => return Err(error),
            error = listener_stopped("worker", &mut ends.worker) => return Err(error),
            result = &mut leader => result,
            never = &mut background => match never {},
        }
    };
    context.readiness.stop();
    if let Some(dashboard) = context.dashboard {
        dashboard.shutdown();
    }
    if observed.load(Ordering::Acquire) {
        let drain = async {
            drain_original(context, &mut outgoing).await?;
            finish_host(context, HostMilestone::Unrouted).await
        };
        tokio::pin!(drain);
        tokio::select! {
            result = &mut controller => return result.map_err(ServerError::Controller).and(Err(ServerError::ControllerEnded)),
            result = &mut drain => result?,
            never = &mut background => match never {},
        }
        let _ = stop_sender.send(());
        tokio::select! {
            result = &mut controller => result.map_err(ServerError::Controller)?,
            never = &mut background => match never {},
        }
    }
    finish_host(context, HostMilestone::HandedOff).await?;
    result
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
    attempt_budget: Duration,
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
            attempt_budget: settings.attempt_budget,
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
            attempt_budget: self.attempt_budget,
            actions: Arc::clone(&self.actions),
            scaling: Arc::clone(&self.scaling),
            clock: core.host.clock.clone(),
            observations: &self.observation_sink,
        };
        let retirement_error = parking_lot::Mutex::new(None);
        let provisioner = &self.provisioner;
        let shutdown = async {
            tokio::select! {
                () = shutdown => {}
                result = provisioner.retiring() => if let Err(error) = result {
                    *retirement_error.lock() = Some(error);
                }
            }
        }
        .shared();
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
        let leadership = controller_leadership(core, &self.controller, self.leadership.clone());
        readiness.begin_term(leadership.fence().clone());
        let leading = Leading::default();
        let (stop_sender, stop) = stop_channel();
        let (connections, ends) = controller_connections::<D>(
            listeners.public,
            listeners.worker,
            self.public_gate,
            self.worker_gate,
            self.public,
            stop,
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
        };
        let result = drive_original(
            &context,
            &spec,
            (connections, ends, stop_sender),
            ready,
            shutdown,
            background.as_mut(),
        )
        .await;
        if let Some(error) = retirement_error.lock().take() {
            return Err(ServerError::HostIdentity(error));
        }
        result
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
        async fn state(&self) -> Result<gang_spi::ControllerHostState, gang_spi::PortError> {
            Ok(gang_spi::ControllerHostState::Active)
        }
        async fn drain_limit(&self) -> Result<Option<std::time::Duration>, gang_spi::PortError> {
            Ok(None)
        }
        async fn heartbeat(&self) -> Result<(), gang_spi::PortError> {
            Ok(())
        }

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
        assert!(!second_state.ready());
        assert!(!second_state.is_leading());
        old.clock.advance(interval);
        first.await?;
        drop(stop_first);
        assert_eq!(old.scheduler.controller_runs().get(), 1);
        assert!(!first_state.ready());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(second_state.is_leading());

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
    async fn slow_large_preparation_precedes_launch_completion_and_takeover()
    -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{ControllerHostIdentity, ControllerId, ControllerReady, RecordKind};
        use std::time::Duration;
        let (mut first, _dials, old) = crate::test_support::plain_parts();
        let (mut second, _dials, replacement) = crate::test_support::plain_parts();
        first.provisioner =
            first
                .provisioner
                .with_host_identity(ControllerHostIdentity::Launched {
                    at: old.clock.wall(),
                    instance: gang_types::InstanceId::try_from("local-old")?,
                });
        second.provisioner =
            second
                .provisioner
                .with_host_identity(ControllerHostIdentity::Launched {
                    at: old.clock.wall() + Duration::from_secs(1),
                    instance: gang_types::InstanceId::try_from("local-new")?,
                });
        let host = second.provisioner.clone();
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("prepared-successor");
        let release_drain = old.scheduler.hold_drain();
        let mut settings = crate::test_support::plain_settings();
        settings.actions = Arc::new(gang_config::ActionsConfig {
            history_flush_interval: gang_types::NonZeroDuration::from_secs(1),
            ..gang_config::ActionsConfig::default()
        });
        let renew = settings.leadership.renew_interval.get();
        let bound =
            gang_types::handover::handover_window(renew, settings.leadership.call_timeout.get());
        let first = super::Server::prepare(first, settings.clone()).await?;
        let second = super::Server::prepare(second, settings).await?;
        let old_state = first.readiness.clone();
        let new_state = second.readiness.clone();
        let mut first = Box::pin(first.run(std::future::pending(), ControllerReady::channel().0));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let expected = seed_large_live_history(&old, first.as_mut()).await?;
        let (mut entered, release) = old
            .repository
            .hold_next_record_page(RecordKind::ResourceHistory);
        let (stop, stopped) = super::stop_channel();
        let (ready, mut fully_prepared) = ControllerReady::channel();
        let mut second = Box::pin(second.run(stopped, ready));
        assert!(futures::poll!(second.as_mut()).is_pending());
        entered.try_recv()?;
        // Even a successful read delayed beyond the entire planned-switch budget is
        // preparation, not a reason to deregister the still-serving original controller.
        assert_passive_preparation(
            (&old, &host),
            (&old_state, &new_state),
            (first.as_mut(), second.as_mut()),
            bound,
        )
        .await;
        assert_eq!(
            host.prepared_attempts().get(),
            0,
            "history import precedes routing preparation"
        );
        assert!(matches!(
            fully_prepared.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        let release_prepared = host.hold_prepared();
        release
            .send(())
            .map_err(|()| "preparation read was cancelled")?;
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(host.prepared_attempts().get(), 1);
        assert_passive_preparation(
            (&old, &host),
            (&old_state, &new_state),
            (first.as_mut(), second.as_mut()),
            bound,
        )
        .await;
        assert!(matches!(
            fully_prepared.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release_prepared
            .send(())
            .map_err(|()| "routing preparation was cancelled")?;
        assert!(futures::poll!(second.as_mut()).is_pending());
        fully_prepared.try_recv()?;
        assert_eq!(host.milestones(), vec![gang_spi::HostMilestone::Prepared]);
        assert_prepared_activation(
            (&old, &replacement.scheduler, &host),
            &new_state,
            (first.as_mut(), second.as_mut()),
            &expected,
            (stop, release_drain),
            renew,
        )
        .await?;
        assert_eq!(old.scheduler.controller_runs().get(), 1);
        assert_eq!(replacement.scheduler.controller_runs().get(), 1);
        Ok(())
    }

    async fn assert_passive_preparation(
        handles: (
            &crate::test_support_deployment::Handles,
            &gang_testkit::FakeProvisioner,
        ),
        states: (&crate::health::Readiness, &crate::health::Readiness),
        runs: (
            Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
            Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
        ),
        bound: std::time::Duration,
    ) {
        let (old, host) = handles;
        let (old_state, new_state) = states;
        let (mut first, mut second) = runs;
        for _ in 0..=bound.as_secs() {
            old.clock.advance(std::time::Duration::from_secs(1));
            assert!(futures::poll!(first.as_mut()).is_pending());
            assert!(futures::poll!(second.as_mut()).is_pending());
            assert!(old_state.ready());
            assert!(!new_state.ready());
            assert_eq!(host.serving_attempts().get(), 0);
            assert!(host.milestones().is_empty());
            assert_eq!(old.scheduler.drains().get(), 0, "no early takeover request");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn passive_host_preparation_cancels_on_shutdown_or_retirement() -> gang_testkit::TestResult
    {
        for retiring in [false, true] {
            let (parts, _dials, handles) = crate::test_support::plain_parts();
            let host = parts.provisioner.clone();
            let release = host.hold_prepared();
            let server =
                super::Server::prepare(parts, crate::test_support::plain_settings()).await?;
            let readiness = server.readiness.clone();
            let (ready, prepared) = gang_spi::ControllerReady::channel();
            let (stop, stopped) = super::stop_channel();
            let mut run = Box::pin(server.run(stopped, ready));
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert_eq!(host.prepared_attempts().get(), 1);
            assert!(!readiness.ready());
            if retiring {
                host.retire();
            } else {
                stop.send(()).map_err(|()| "server stopped early")?;
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), run).await??;
            assert!(
                prepared.await.is_err(),
                "an incomplete Prepared stage never signals readiness"
            );
            assert!(
                release.send(()).is_err(),
                "the host registration future must be cancelled"
            );
            assert!(!host.milestones().iter().any(|milestone| matches!(
                milestone,
                gang_spi::HostMilestone::Prepared | gang_spi::HostMilestone::Serving
            )));
            assert_eq!(
                handles.scheduler.snapshots().get(),
                0,
                "leader work never starts"
            );
        }
        Ok(())
    }

    async fn assert_prepared_activation(
        handles: (
            &crate::test_support_deployment::Handles,
            &gang_testkit::FakeScheduler,
            &gang_testkit::FakeProvisioner,
        ),
        readiness: &crate::health::Readiness,
        runs: (
            Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
            Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
        ),
        expected: &[gang_spi::LearnedHistory],
        stops: (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Sender<()>,
        ),
        renew: std::time::Duration,
    ) -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{RecordKind, SchedulerPort};
        let (old, replacement, host) = handles;
        let (mut first, mut second) = runs;
        let (stop, release_drain) = stops;
        let (mut reread, release_reread) = old
            .repository
            .hold_next_record_page(RecordKind::ResourceHistory);
        old.clock.advance(renew);
        let release_serving = host.hold_serving();
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(
            readiness.ready(),
            "post-acquire activation must not reload the full history"
        );
        assert!(matches!(
            reread.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(host.serving_attempts().get(), 1);
        assert_eq!(
            host.milestones(),
            vec![gang_spi::HostMilestone::Prepared],
            "the launch hook is held after leader health becomes ready"
        );
        release_serving
            .send(())
            .map_err(|()| "launch completion hold disappeared")?;
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(
            host.milestones(),
            vec![
                gang_spi::HostMilestone::Prepared,
                gang_spi::HostMilestone::Serving
            ]
        );
        let mut restored = Vec::new();
        replacement
            .learned_history(old.clock.wall(), &mut restored)
            .await?;
        assert_eq!(
            restored.as_slice(),
            expected,
            "preparation restores every chunk and page"
        );
        release_reread
            .send(())
            .map_err(|()| "canonical refresh gate was cancelled")?;
        stop.send(()).map_err(|()| "successor stopped early")?;
        second.await?;
        release_drain
            .send(())
            .map_err(|()| "old response hold disappeared")?;
        old.clock.advance(renew);
        first.await?;
        Ok(())
    }

    async fn seed_large_live_history(
        handles: &crate::test_support_deployment::Handles,
        mut run: Pin<&mut impl Future<Output = Result<(), super::ServerError>>>,
    ) -> gang_testkit::TestResult<Vec<gang_spi::LearnedHistory>> {
        use gang_clock::Clock;
        use gang_spi::{LearnedHistory, RecordId, RecordKey, RecordKind, SchedulerPort};
        use std::time::Duration;
        let mut records = Vec::with_capacity(gang_spi::MAX_RECORD_CHANGES + 1);
        for id in 0..=gang_spi::MAX_RECORD_CHANGES {
            let id = u8::try_from(id)?;
            records.push(LearnedHistory {
                key: RecordKey {
                    kind: RecordKind::ResourceHistory,
                    id: RecordId::new([id; 32]),
                },
                body: vec![id; if id == 0 { 410 * 1024 } else { 8192 }],
                expires_at: handles.clock.wall() + Duration::from_hours(1),
            });
        }
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), &records)
            .await?;
        handles.clock.advance(Duration::from_secs(1));
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert!(handles.repository.stored_records() > gang_spi::MAX_RECORD_CHANGES);
        Ok(records)
    }

    #[tokio::test]
    async fn learned_history_interval_and_pre_release_snapshot() -> gang_testkit::TestResult {
        use gang_clock::Clock;
        use gang_spi::{
            Acquisition, ControllerId, ControllerReady, ExpiringRecordPort, LeaderLeasePort,
            LearnedHistory, RecordId, RecordKey, RecordKind, SchedulerPort, Takeover,
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

        history.body = vec![2];
        handles
            .scheduler
            .restore_learned_history(handles.clock.wall(), &[history.clone()])
            .await?;
        let _ = stop.send(());
        assert!(futures::poll!(run.as_mut()).is_pending());
        assert_eq!(drains.get(), 1);
        let now = handles.clock.wall();
        let snapshot = handles
            .repository
            .get(&key, now)
            .await?
            .ok_or("handoff snapshot missing")?;
        let Acquisition::Acquired(lease) = handles
            .repository
            .acquire(
                &passive_candidate(&ControllerId::new("history-successor")),
                now,
                now,
                now + Duration::from_mins(1),
                Takeover::Observe,
            )
            .await?
        else {
            return Err("shutdown kept the lease during response drain".into());
        };
        let mut store =
            super::LearnedHistoryStore::new(super::history_byte_limit(std::num::NonZeroUsize::MIN));
        let mut loaded = Vec::new();
        store
            .load(&handles.repository, &lease, now, &mut loaded)
            .await?;
        assert_eq!(loaded, vec![history.clone()]);
        history.body = vec![3];
        handles
            .scheduler
            .restore_learned_history(now, &[history])
            .await?;
        let _ = release_drain.send(());
        handles.clock.advance(Duration::from_secs(1));
        run.await?;
        assert_eq!(
            handles
                .repository
                .get(&key, now)
                .await?
                .ok_or("snapshot disappeared")?
                .version,
            snapshot.version,
            "late measurements cannot write after authority release"
        );
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
        let snapshot_bound = settings.leadership.call_timeout.get();
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
        drop(stop);
        Ok(())
    }

    #[tokio::test]
    async fn newer_host_preempts_sigterm_drain_without_restarting_old_process()
    -> gang_testkit::TestResult {
        use crate::test_support::{plain_parts, plain_settings};
        use gang_clock::Clock;
        use gang_spi::{
            ControllerId, ControllerReady, LearnedHistory, RecordId, RecordKey, RecordKind,
            SchedulerPort,
        };

        let (first, dials, old) = plain_parts();
        let mut refused = old.scheduler.connections();
        let (mut second, _dials, replacement) = plain_parts();
        second.repository = first.repository.clone();
        second.clock = first.clock.clone();
        second.controller = ControllerId::new("sigterm-successor");
        let release_drain = old.scheduler.hold_drain();
        let settings = plain_settings();
        let renew = settings.leadership.renew_interval.get();
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
        let mut second = Box::pin(second.run(stopped_second, ControllerReady::channel().0));
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(second_state.is_leading());
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
        old.clock.advance(std::time::Duration::from_secs(1));
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
        handles.clock.advance(std::time::Duration::from_secs(1));
        run.await?;
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
    async fn readonly_preparation_retry_preserves_activation_and_flush_sequences()
    -> gang_testkit::TestResult {
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
            "activation, reconciliation and flush must use distinct ordered batches",
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
