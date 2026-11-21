//! The fake scheduler: a controller that serves the connections it is handed, with each
//! listener's gate applied to its requests, and a worker side that records every spec it is
//! asked to run and behaves as scripted.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use gang_clock::{Tick, Timestamp};
use gang_spi::{
    ActionsView, Admitted, ApiRequestBody, ApiResponseBody, ApiService, BoxedConnection,
    ControllerConnections, ControllerEndpoints, ControllerReady, ControllerSpec, FleetSupply,
    GateDecision, LeaderUnavailable, LearnedHistory, Listening, ListingLimits, Peer, PortError,
    RecordKind, Rejection, RequestGate, RequestScope, SchedulerPort, SchedulerSnapshot, WorkerSpec,
    WorkerTls, full_body,
};
use gang_types::{Bytes, FleetName, FleetProperties, InstanceId, WorkerLifetime};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};
use url::Url;

/// The listener side of the controller a connection arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// The public listener: Bazel and `gang` clients.
    Public,
    /// The worker listener: workers.
    Worker,
}

/// A connection the fake controller received, with the listener it arrived on.
pub struct Received {
    /// The listener the connection arrived on.
    pub side: Side,
    /// The connection; dropping it closes the stream.
    pub connection: BoxedConnection,
    /// What the transport verified about the connection.
    pub peer: Peer,
}

/// The services a controller applies to requests, behind a trait object so that [`Controller`]
/// is not generic over the listeners' gates and the Gang API.
trait Services: Send + Sync {
    /// Applies the public listener's gate to a request.
    fn admit_public<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>>;

    /// Applies the worker listener's gate to a request.
    fn admit_worker<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>>;

    /// Answers a request with the Gang API.
    fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> BoxFuture<'static, http::Response<ApiResponseBody>>;
}

/// The listener gates and the Gang API of one controller.
struct Gates<P, W, A> {
    /// The public listener's gate.
    public: P,
    /// The worker listener's gate.
    worker: W,
    /// The Gang API.
    api: A,
    /// Whether leader requests are permanently refused by this transport.
    standby: bool,
}

impl<P: RequestGate, W: RequestGate, A: ApiService> Services for Gates<P, W, A> {
    fn admit_public<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>> {
        if self.standby && matches!(scope, RequestScope::Leader) {
            return Box::pin(async { Ok(GateDecision::NotLeading) });
        }
        Box::pin(self.public.admit(scope, peer, parts))
    }

    fn admit_worker<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>> {
        if self.standby && matches!(scope, RequestScope::Leader) {
            return Box::pin(async { Ok(GateDecision::NotLeading) });
        }
        Box::pin(self.worker.admit(scope, peer, parts))
    }

    fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> BoxFuture<'static, http::Response<ApiResponseBody>> {
        Box::pin(admit_and_call(
            self.public.clone(),
            self.api.clone(),
            request,
        ))
    }
}

async fn admit_and_call<P: RequestGate, A: ApiService>(
    gate: P,
    api: A,
    request: http::Request<ApiRequestBody>,
) -> http::Response<ApiResponseBody> {
    let (mut parts, body) = request.into_parts();
    match gate
        .admit(RequestScope::Shared, &Peer::default(), &mut parts)
        .await
    {
        Ok(GateDecision::Admit(admitted)) => {
            if let Admitted::Authenticated(authenticated) = admitted {
                parts.extensions.insert(authenticated);
            }
            api.call(http::Request::from_parts(parts, body)).await
        }
        Ok(GateDecision::NotLeading) => {
            let mut response = http::Response::new(full_body(bytes::Bytes::new()));
            *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
            response.headers_mut().insert(
                gang_types::handover::REQUEST_NOT_STARTED_HEADER,
                http::HeaderValue::from_static("true"),
            );
            response.extensions_mut().insert(LeaderUnavailable);
            response
        }
        Err(error) => refusal(&error),
    }
}

/// The answer the controller gives a request its public gate refuses.
fn refusal(error: &PortError) -> http::Response<ApiResponseBody> {
    let status = match error {
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        } => http::StatusCode::UNAUTHORIZED,
        PortError::Rejected {
            reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
            ..
        } => http::StatusCode::FORBIDDEN,
        PortError::Unavailable(_) => http::StatusCode::SERVICE_UNAVAILABLE,
        _ => http::StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = http::Response::new(full_body(bytes::Bytes::new()));
    *response.status_mut() = status;
    response
}

/// The controller a [`FakeScheduler`] runs: the gates and the Gang API it was started with.
#[derive(Clone)]
pub struct Controller {
    /// Action policy passed to the controller.
    pub actions: Arc<gang_config::ActionsConfig>,
    /// The gates and the Gang API.
    services: Arc<dyn Services>,
    observations: gang_spi::ObservationSink,
}

impl Controller {
    /// Sends an observation through the controller's bounded channel.
    pub fn observe(&self, observation: gang_spi::Observation) {
        self.observations.record(observation);
    }

    /// Simulates traffic through the controller's observation sink.
    pub fn count(&self, delta: gang_spi::Traffic) {
        self.observations.count(delta);
    }

    /// Applies the public listener's gate to a request, as the controller does before any
    /// public service sees it.
    ///
    /// # Errors
    ///
    /// Whatever the public gate rejects the request with.
    pub async fn admit_public(
        &self,
        scope: RequestScope,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<GateDecision, PortError> {
        self.services.admit_public(scope, peer, parts).await
    }

    /// Applies the worker listener's gate to a request, as the controller does before any worker
    /// service sees it.
    ///
    /// # Errors
    ///
    /// Whatever the worker gate rejects the request with.
    pub async fn admit_worker(
        &self,
        scope: RequestScope,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<GateDecision, PortError> {
        self.services.admit_worker(scope, peer, parts).await
    }

    /// Answers a request admitted by the public gate, passing its authenticated caller to the
    /// Gang API in the request extensions. Refusals are answered with their status and no body.
    pub async fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> http::Response<ApiResponseBody> {
        self.services.call_api(request).await
    }
}

/// The real services retained by a standby transport, with leader requests always refused.
#[derive(Clone)]
pub struct StandbyServices {
    services: Arc<dyn Services>,
}

impl StandbyServices {
    /// Applies the public gate, refusing leader scope independently of that gate.
    ///
    /// # Errors
    ///
    /// Returns an error from the public gate for a shared request.
    pub async fn admit_public(
        &self,
        scope: RequestScope,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<GateDecision, PortError> {
        self.services.admit_public(scope, peer, parts).await
    }

    /// Applies the worker gate, refusing leader scope independently of that gate.
    ///
    /// # Errors
    ///
    /// Returns an error from the worker gate for a shared request.
    pub async fn admit_worker(
        &self,
        scope: RequestScope,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<GateDecision, PortError> {
        self.services.admit_worker(scope, peer, parts).await
    }

    /// Runs the supplied API after shared public admission.
    pub async fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> http::Response<ApiResponseBody> {
        self.services.call_api(request).await
    }
}

/// How the fake scheduler's worker behaves once it has been recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behaviour {
    /// Runs until the stop signal completes.
    Pending,
    /// Returns `Ok(())` at once.
    Finish,
    /// Returns a backend failure at once.
    Fail,
}

/// The owned transport credentials passed to a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedTls {
    /// No TLS.
    Plain,
    /// Server authentication with an optional custom CA.
    ServerOnly(Option<PathBuf>),
    /// Mutual authentication with certificate, private key and server CA paths.
    Mutual {
        /// The client certificate.
        certificate: PathBuf,
        /// The client private key.
        private_key: PathBuf,
        /// The server CA certificate.
        server_ca: PathBuf,
    },
}

/// The fields of a [`WorkerSpec`] the scheduler received, owned so they outlive the call.
#[derive(Debug, Clone)]
pub struct RecordedSpec {
    /// Action policy passed to the worker.
    pub actions: Arc<gang_config::ActionsConfig>,
    /// Prepared action memory pool.
    pub memory_pool: Option<gang_spi::ActionMemoryPool>,
    /// The instance the worker runs on.
    pub instance: InstanceId,
    /// The properties the worker registers with the scheduler.
    pub properties: FleetProperties,
    /// Whether the worker serves one action or remains available for more.
    pub lifetime: WorkerLifetime,
    /// The directory actions are built in.
    pub work_directory: PathBuf,
    /// The fast content-addressed store directory.
    pub fast_cas_directory: PathBuf,
    /// The fast store's eviction budget.
    pub fast_cas_budget: Bytes,
    /// The transport and credential paths used by the worker.
    pub tls: RecordedTls,
    /// The number of pending workers already stopped when this worker started.
    pub stops_before_start: usize,
    /// How the worker reaches the stores.
    pub store_access: gang_spi::StoreAccess,
    /// The `gang-exec` entrypoint.
    pub entrypoint: PathBuf,
}

/// The scripted result of a refusal run after its intake and response drain finish.
#[derive(Default)]
enum RefusalOutcome {
    #[default]
    Success,
    Failure,
}

/// Which operations of a [`FakeScheduler`] fail.
#[derive(Default)]
struct Failing {
    /// Snapshot queries.
    snapshot: bool,
    /// Actions queries.
    actions: bool,
    /// `run_controller`, which fails at once.
    controller: bool,
    /// `serve_standby`, checked after intake and any held response drain finish.
    refusals: RefusalOutcome,
}

type DrainRetention = (Tick, Duration, Vec<(FleetName, InstanceId)>);

/// The state a [`FakeScheduler`] shares between its clones.
struct SchedulerState {
    /// The worker endpoint `endpoints` reports.
    endpoint: Url,
    /// The snapshot `snapshot` reports.
    snapshot: SchedulerSnapshot,
    /// Release of the next held snapshot.
    snapshot_hold: Option<oneshot::Receiver<()>>,
    /// The actions `actions` reports.
    actions: ActionsView,
    /// Live learned histories in recency order.
    history: Vec<LearnedHistory>,
    /// The operations that fail.
    failing: Failing,
    /// Whether `run_controller` returns at once without serving the connections it was handed.
    finish_controller: bool,
    /// Where `run_controller` forwards every connection it receives.
    connections: Option<mpsc::UnboundedSender<Received>>,
    /// Where `serve_standby` forwards every connection it receives.
    refusal_connections: Option<mpsc::UnboundedSender<Received>>,
    /// How the next worker behaves.
    behaviour: Behaviour,
    /// Every spec `run_worker` has been asked to run.
    specs: Vec<RecordedSpec>,
    /// The `set_draining` calls that succeeded, in order, with their flags.
    draining_calls: Vec<(FleetName, InstanceId, bool)>,
    /// Complete provider census requests, in call order.
    retention_calls: Vec<DrainRetention>,
    /// Released by the test; while held, `run_controller` waits on it after its connections end.
    drain_hold: Option<oneshot::Receiver<()>>,
    /// Released by the test after the next refusal run's two connection streams end.
    refusal_drain_hold: Option<oneshot::Receiver<()>>,
    /// Released by the test before the next controller signals readiness.
    ready_hold: Option<oneshot::Receiver<()>>,
}

/// Counts calls made to a fake, so that a test can wait until a number of them has been made.
#[derive(Clone)]
pub struct CallCount(Arc<watch::Sender<usize>>);

impl Default for CallCount {
    fn default() -> Self {
        Self::new()
    }
}

impl CallCount {
    /// Creates a counter with no calls recorded.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(watch::channel(0).0))
    }

    /// Counts one call.
    pub fn bump(&self) {
        self.0.send_modify(|count| *count += 1);
    }

    /// The calls counted so far.
    #[must_use]
    pub fn get(&self) -> usize {
        *self.0.borrow()
    }

    /// Completes once at least `calls` calls have been counted.
    ///
    /// A fake counts a call before it returns, and the code under test runs without yielding
    /// until its next wait. A test that resumes here therefore finds that code parked.
    pub async fn reached(&self, calls: usize) {
        // The sender lives as long as this counter, so the wait cannot fail.
        let _ = self.0.subscribe().wait_for(|count| *count >= calls).await;
    }
}

/// Buffers preparation-time connections, forwards them after readiness, and drains on EOF.
async fn serve_controller_connections(
    mut merged: impl futures::Stream<Item = Received> + Unpin,
    sender: Option<mpsc::UnboundedSender<Received>>,
    ready: ControllerReady,
    ready_hold: Option<oneshot::Receiver<()>>,
    drain_hold: Option<oneshot::Receiver<()>>,
    drains: &CallCount,
) {
    let mut pending = Vec::new();
    if let Some(mut hold) = ready_hold {
        loop {
            tokio::select! {
                biased;
                _ = &mut hold => break,
                received = merged.next() => {
                    if let Some(received) = received {
                        pending.push(received);
                    } else {
                        drop(ready);
                        drop(hold);
                        drop(merged);
                        drop(pending);
                        if let Some(hold) = drain_hold {
                            drains.bump();
                            let _ = hold.await;
                        }
                        return;
                    }
                },
            }
        }
    }
    ready.signal();
    if let Some(sender) = &sender {
        for received in pending {
            let _ = sender.send(received);
        }
    }
    while let Some(received) = merged.next().await {
        if let Some(sender) = &sender {
            let _ = sender.send(received);
        }
    }
    drop(merged);
    if let Some(hold) = drain_hold {
        drains.bump();
        let _ = hold.await;
    }
}

/// Removes the published controller on normal return and cancellation.
struct PublishedController<'a>(&'a watch::Sender<Option<Controller>>);

impl Drop for PublishedController<'_> {
    fn drop(&mut self) {
        self.0.send_replace(None);
    }
}

/// Removes only this run's standby services, including on cancellation.
struct PublishedStandby<'a> {
    sender: &'a watch::Sender<Option<StandbyServices>>,
    services: Arc<dyn Services>,
}

impl Drop for PublishedStandby<'_> {
    fn drop(&mut self) {
        self.sender.send_if_modified(|slot| {
            if slot
                .as_ref()
                .is_some_and(|standby| Arc::ptr_eq(&standby.services, &self.services))
            {
                *slot = None;
                true
            } else {
                false
            }
        });
    }
}

/// A scheduler that serves as both sides of the scheduler port. Its controller reports a
/// configured snapshot, forwards connections, and records physical drain requests without
/// changing the configured observations. Its worker records specs and behaves as scripted.
///
/// Clones share state.
#[derive(Clone)]
pub struct FakeScheduler {
    /// The state shared by every clone.
    state: Arc<Mutex<SchedulerState>>,
    /// The controller `run_controller` publishes; `controller` waits for it.
    published: Arc<watch::Sender<Option<Controller>>>,
    /// The services of the independently running standby transport.
    standby: Arc<watch::Sender<Option<StandbyServices>>>,
    /// Successfully published controller terms.
    controller_runs: CallCount,
    /// Calls to `run_worker`, counted once each call's spec is recorded.
    worker_starts: CallCount,
    /// Pending workers that returned after their stop signal completed.
    worker_stops: CallCount,
    /// Calls to `actions`, counted before each answers.
    actions_calls: CallCount,
    /// Calls to `snapshot`, counted before each answers.
    snapshots: CallCount,
    /// Controller runs that finished their connections and wait on a drain hold.
    drains: CallCount,
    /// Refusal transports started independently of normal controller publication.
    refusal_runs: CallCount,
}

impl FakeScheduler {
    /// Creates a scheduler whose worker endpoint uses the `grpcs` scheme, as a worker with TLS
    /// dials.
    ///
    /// # Errors
    ///
    /// [`url::ParseError`] if the endpoint cannot be parsed; it always can.
    pub fn new() -> Result<Self, url::ParseError> {
        Self::with_scheme("grpcs")
    }

    /// Creates a scheduler whose worker endpoint uses the plain `grpc` scheme, which is what a
    /// worker without TLS dials.
    ///
    /// # Errors
    ///
    /// [`url::ParseError`] if the endpoint cannot be parsed; it always can.
    pub fn plain() -> Result<Self, url::ParseError> {
        Self::with_scheme("grpc")
    }

    fn with_scheme(scheme: &str) -> Result<Self, url::ParseError> {
        let endpoint = Url::parse(&format!("{scheme}://scheduler.example.test:9002"))?;
        Ok(Self {
            state: Arc::new(Mutex::new(SchedulerState {
                endpoint,
                snapshot: SchedulerSnapshot::default(),
                snapshot_hold: None,
                actions: ActionsView::default(),
                history: Vec::new(),
                failing: Failing::default(),
                finish_controller: false,
                connections: None,
                refusal_connections: None,
                behaviour: Behaviour::Pending,
                specs: Vec::new(),
                draining_calls: Vec::new(),
                retention_calls: Vec::new(),
                drain_hold: None,
                refusal_drain_hold: None,
                ready_hold: None,
            })),
            published: Arc::new(watch::channel::<Option<Controller>>(None).0),
            standby: Arc::new(watch::channel::<Option<StandbyServices>>(None).0),
            controller_runs: CallCount::new(),
            worker_starts: CallCount::new(),
            worker_stops: CallCount::new(),
            actions_calls: CallCount::new(),
            snapshots: CallCount::new(),
            drains: CallCount::new(),
            refusal_runs: CallCount::new(),
        })
    }

    /// Changes the shared worker endpoint's scheme without resetting recorded state.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Config`] if the scheme cannot be applied to the endpoint.
    pub fn set_worker_scheme(&self, scheme: &str) -> Result<(), PortError> {
        self.state
            .lock()
            .endpoint
            .set_scheme(scheme)
            .map_err(|()| PortError::config(io::Error::other("invalid worker endpoint scheme")))
    }

    /// Sets the snapshot, sorting workers, completion cells and physical cordons once.
    pub fn set_snapshot(&self, mut snapshot: SchedulerSnapshot) {
        snapshot
            .source_rates
            .sort_unstable_by_key(|row| (row.source, row.preferred_fleet));
        snapshot
            .source_rates
            .dedup_by_key(|row| (row.source, row.preferred_fleet));
        snapshot.cordons.sort_unstable_by(|a, b| {
            (a.fleet, a.instance.as_ref()).cmp(&(b.fleet, b.instance.as_ref()))
        });
        snapshot
            .cordons
            .dedup_by(|a, b| a.fleet == b.fleet && a.instance == b.instance);
        if snapshot.executing.is_empty() {
            snapshot.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        } else {
            let mut workers: Vec<_> = snapshot.workers.drain(..).enumerate().collect();
            workers.sort_unstable_by(|a, b| a.1.id.cmp(&b.1.id));
            let mut sorted_index = vec![0; workers.len()];
            for (new, (old, worker)) in workers.into_iter().enumerate() {
                sorted_index[old] = new;
                snapshot.workers.push(worker);
            }
            for execution in &mut snapshot.executing {
                if let Some(&index) = sorted_index.get(execution.worker) {
                    execution.worker = index;
                }
            }
        }
        self.state.lock().snapshot = snapshot;
    }

    /// Pauses the next snapshot until the returned sender sends or is dropped.
    #[must_use]
    pub fn hold_snapshot(&self) -> oneshot::Sender<()> {
        let (sender, receiver) = oneshot::channel();
        self.state.lock().snapshot_hold = Some(receiver);
        sender
    }

    /// Makes snapshot queries fail while `fail` is true.
    pub fn fail_snapshot(&self, fail: bool) {
        self.state.lock().failing.snapshot = fail;
    }

    /// Makes `run_controller` fail at once without signalling readiness while `fail` is true.
    pub fn fail_controller(&self, fail: bool) {
        self.state.lock().failing.controller = fail;
    }

    /// Makes `run_controller` signal readiness and return `Ok(())` at once while `finish` is
    /// true, without serving the connections it was handed.
    pub fn finish_controller(&self, finish: bool) {
        self.state.lock().finish_controller = finish;
    }

    /// Sets the actions `actions` reports.
    pub fn set_actions(&self, actions: ActionsView) {
        self.state.lock().actions = actions;
    }

    /// Makes actions queries fail while `fail` is true.
    pub fn fail_actions(&self, fail: bool) {
        self.state.lock().failing.actions = fail;
    }

    /// Counts the calls to `actions`.
    #[must_use]
    pub fn actions_calls(&self) -> CallCount {
        self.actions_calls.clone()
    }

    /// Counts the calls to `snapshot`.
    #[must_use]
    pub fn snapshots(&self) -> CallCount {
        self.snapshots.clone()
    }

    /// Counts the controller runs that finished their connections and wait on a drain hold.
    #[must_use]
    pub fn drains(&self) -> CallCount {
        self.drains.clone()
    }

    /// Counts controller terms at their atomic publication.
    #[must_use]
    pub fn controller_runs(&self) -> CallCount {
        self.controller_runs.clone()
    }

    /// Holds the next controller run open after its connections end, as a controller draining
    /// its connections, until the returned sender is used or dropped.
    #[must_use]
    pub fn hold_drain(&self) -> oneshot::Sender<()> {
        let (release, hold) = oneshot::channel();
        self.state.lock().drain_hold = Some(hold);
        release
    }

    /// Counts refusal transports when they start consuming connections.
    #[must_use]
    pub fn refusal_runs(&self) -> CallCount {
        self.refusal_runs.clone()
    }

    /// Holds the next refusal run after both intake streams end until released or dropped.
    #[must_use]
    pub fn hold_refusal_drain(&self) -> oneshot::Sender<()> {
        let (release, hold) = oneshot::channel();
        self.state.lock().refusal_drain_hold = Some(hold);
        release
    }

    /// Makes refusal runs fail at completion while `fail` is true, including runs already held.
    pub fn fail_refusals(&self, fail: bool) {
        self.state.lock().failing.refusals = if fail {
            RefusalOutcome::Failure
        } else {
            RefusalOutcome::Success
        };
    }

    /// The channel the next refusal run forwards connections to, separate from the controller.
    /// Call before that refusal run; the channel closes when the run returns.
    #[must_use]
    pub fn refusal_connections(&self) -> mpsc::UnboundedReceiver<Received> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state.lock().refusal_connections = Some(sender);
        receiver
    }

    /// Holds the next controller before it signals readiness until released or dropped.
    #[must_use]
    pub fn hold_ready(&self) -> oneshot::Sender<()> {
        let (release, hold) = oneshot::channel();
        self.state.lock().ready_hold = Some(hold);
        release
    }

    /// The channel `run_controller` forwards every connection to. Call it once, before
    /// `run_controller`; the channel closes when `run_controller` returns.
    #[must_use]
    pub fn connections(&self) -> mpsc::UnboundedReceiver<Received> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state.lock().connections = Some(sender);
        receiver
    }

    /// The controller `run_controller` published, waiting until `run_controller` has started.
    ///
    /// Returns `None` only if the scheduler's sender were dropped while this waits, which cannot
    /// happen while `&self` exists.
    pub async fn controller(&self) -> Option<Controller> {
        let mut published = self.published.subscribe();
        let ready = published.wait_for(Option::is_some).await.ok()?;
        (*ready).clone()
    }

    /// Waits for a standby transport and returns its supplied services.
    ///
    /// Returns `None` only if the scheduler's sender is dropped while waiting.
    pub async fn standby_services(&self) -> Option<StandbyServices> {
        let mut standby = self.standby.subscribe();
        let ready = standby.wait_for(Option::is_some).await.ok()?;
        (*ready).clone()
    }

    /// Sets how the next worker behaves.
    pub fn set_behaviour(&self, behaviour: Behaviour) {
        self.state.lock().behaviour = behaviour;
    }

    /// The specs recorded so far, one per `run_worker` call.
    #[must_use]
    pub fn specs(&self) -> Vec<RecordedSpec> {
        self.state.lock().specs.clone()
    }

    /// The `run_worker` calls made so far, counted once each call's spec is recorded.
    #[must_use]
    pub fn worker_starts(&self) -> CallCount {
        self.worker_starts.clone()
    }

    /// Counts pending workers that stopped after their stop signal completed.
    #[must_use]
    pub fn worker_stops(&self) -> CallCount {
        self.worker_stops.clone()
    }

    /// Successful physical drain requests in call order, with their requested flags.
    #[must_use]
    pub fn draining_calls(&self) -> Vec<(FleetName, InstanceId, bool)> {
        self.state.lock().draining_calls.clone()
    }

    /// Complete census retention requests in call order, including the supplied clock and grace.
    #[must_use]
    pub fn retain_drains_calls(&self) -> Vec<DrainRetention> {
        self.state.lock().retention_calls.clone()
    }
}

impl SchedulerPort for FakeScheduler {
    fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        Ok(NonZeroUsize::MAX)
    }

    async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        into.clear();
        into.extend(
            self.state
                .lock()
                .history
                .iter()
                .filter(|record| record.expires_at > now)
                .cloned(),
        );
        Ok(())
    }

    async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        state.history.clear();
        state.history.extend(
            records
                .iter()
                .filter(|record| {
                    record.expires_at > now
                        && matches!(
                            record.key.kind,
                            RecordKind::ResourceHistory | RecordKind::DurationHistory
                        )
                })
                .cloned(),
        );
        Ok(())
    }

    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        let hold = self.state.lock().snapshot_hold.take();
        self.snapshots.bump();
        if let Some(hold) = hold {
            let _ = hold.await;
        }
        let state = self.state.lock();
        if state.failing.snapshot {
            return Err(PortError::unavailable("scheduler is unavailable"));
        }
        into.completed = state.snapshot.completed;
        into.source_rates.clone_from(&state.snapshot.source_rates);
        into.source_rates_at = state.snapshot.source_rates_at;
        into.cordons.clone_from(&state.snapshot.cordons);
        into.stores.clone_from(&state.snapshot.stores);
        into.workers.clone_from(&state.snapshot.workers);
        into.queued.clone_from(&state.snapshot.queued);
        into.executing.clone_from(&state.snapshot.executing);
        Ok(())
    }

    async fn actions(
        &self,
        _limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        self.actions_calls.bump();
        let state = self.state.lock();
        if state.failing.actions {
            return Err(PortError::unavailable("scheduler is unavailable"));
        }
        into.clone_from(&state.actions);
        Ok(())
    }

    async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError> {
        self.state
            .lock()
            .draining_calls
            .push((fleet.clone(), instance.clone(), draining));
        Ok(())
    }

    async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError> {
        let census = live
            .iter()
            .flat_map(|(fleet, supply)| {
                supply
                    .instances
                    .iter()
                    .map(move |instance| ((*fleet).clone(), instance.id.clone()))
            })
            .collect();
        self.state.lock().retention_calls.push((now, grace, census));
        Ok(())
    }

    fn endpoints(&self) -> ControllerEndpoints {
        ControllerEndpoints {
            worker: self.state.lock().endpoint.clone(),
        }
    }

    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        if self.published.borrow().is_some() {
            return Err(PortError::config(io::Error::other(
                "controller is already serving",
            )));
        }
        let (fail, finish, sender, drain_hold, ready_hold) = {
            let mut state = self.state.lock();
            (
                state.failing.controller,
                state.finish_controller,
                state.connections.take(),
                state.drain_hold.take(),
                state.ready_hold.take(),
            )
        };
        if fail {
            return Err(PortError::backend(io::Error::other("controller failed")));
        }
        if finish {
            ready.signal();
            return Ok(());
        }
        let ControllerConnections {
            public,
            worker,
            api,
        } = connections;
        let Listening {
            connections: public_connections,
            gate: public_gate,
        } = public;
        let Listening {
            connections: worker_connections,
            gate: worker_gate,
        } = worker;
        let controller = Controller {
            actions: Arc::clone(&spec.actions),
            services: Arc::new(Gates {
                public: public_gate,
                worker: worker_gate,
                api,
                standby: false,
            }),
            observations: spec.observations.clone(),
        };
        let claimed = self.published.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            *slot = Some(controller);
            self.controller_runs.bump();
            true
        });
        if !claimed {
            return Err(PortError::config(io::Error::other(
                "controller is already serving",
            )));
        }
        let _published = PublishedController(&self.published);
        let public_side = public_connections.map(|accepted| Received {
            side: Side::Public,
            connection: accepted.connection,
            peer: accepted.peer,
        });
        let worker_side = worker_connections.map(|accepted| Received {
            side: Side::Worker,
            connection: accepted.connection,
            peer: accepted.peer,
        });
        let merged = futures::stream::select(public_side, worker_side);
        serve_controller_connections(merged, sender, ready, ready_hold, drain_hold, &self.drains)
            .await;
        Ok(())
    }

    async fn serve_standby<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        connections: ControllerConnections<P, W, A>,
    ) -> Result<(), PortError> {
        let (sender, drain_hold) = {
            let mut state = self.state.lock();
            (
                state.refusal_connections.take(),
                state.refusal_drain_hold.take(),
            )
        };
        let ControllerConnections {
            public,
            worker,
            api,
        } = connections;
        let Listening {
            connections: public_connections,
            gate: public_gate,
        } = public;
        let Listening {
            connections: worker_connections,
            gate: worker_gate,
        } = worker;
        let services: Arc<dyn Services> = Arc::new(Gates {
            public: public_gate,
            worker: worker_gate,
            api,
            standby: true,
        });
        self.standby.send_replace(Some(StandbyServices {
            services: Arc::clone(&services),
        }));
        let _standby = PublishedStandby {
            sender: &self.standby,
            services,
        };
        self.refusal_runs.bump();
        let public_side = public_connections.map(|accepted| Received {
            side: Side::Public,
            connection: accepted.connection,
            peer: accepted.peer,
        });
        let worker_side = worker_connections.map(|accepted| Received {
            side: Side::Worker,
            connection: accepted.connection,
            peer: accepted.peer,
        });
        let mut merged = futures::stream::select(public_side, worker_side);
        while let Some(received) = merged.next().await {
            if let Some(sender) = &sender {
                let _ = sender.send(received);
            }
        }
        drop(merged);
        if let Some(hold) = drain_hold {
            let _ = hold.await;
        }
        if matches!(self.state.lock().failing.refusals, RefusalOutcome::Failure) {
            return Err(PortError::backend(io::Error::other(
                "refusal transport failed",
            )));
        }
        Ok(())
    }

    async fn run_worker(
        &self,
        spec: &WorkerSpec,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError> {
        let behaviour = {
            let mut state = self.state.lock();
            if tls.scheme() != state.endpoint.scheme() {
                return Err(PortError::config(io::Error::other(
                    "worker transport scheme does not match scheduler endpoint",
                )));
            }
            state.specs.push(RecordedSpec {
                actions: Arc::clone(&spec.actions),
                memory_pool: spec.memory_pool.clone(),
                instance: spec.instance.clone(),
                properties: spec.properties.clone(),
                lifetime: spec.lifetime,
                work_directory: spec.work_directory.clone(),
                fast_cas_directory: spec.fast_cas_directory.clone(),
                fast_cas_budget: spec.fast_cas_budget,
                tls: match tls {
                    WorkerTls::Plain => RecordedTls::Plain,
                    WorkerTls::ServerOnly { server_ca } => {
                        RecordedTls::ServerOnly(server_ca.map(Path::to_path_buf))
                    }
                    WorkerTls::Mutual(tls) => RecordedTls::Mutual {
                        certificate: tls.certificate.to_path_buf(),
                        private_key: tls.private_key.to_path_buf(),
                        server_ca: tls.server_ca.to_path_buf(),
                    },
                },
                stops_before_start: self.worker_stops.get(),
                store_access: spec.store_access,
                entrypoint: spec.entrypoint.clone(),
            });
            state.behaviour
        };
        self.worker_starts.bump();
        match behaviour {
            Behaviour::Pending => {
                stop.await;
                self.worker_stops.bump();
                Ok(())
            }
            Behaviour::Finish => Ok(()),
            Behaviour::Fail => Err(PortError::backend(io::Error::other("scheduler broke"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::FutureExt;
    use gang_spi::{Authenticated, EngineStore, StoreKind};
    use gang_types::{FleetName, Identity, Isa, Principal, ProviderName, Resources};

    use super::*;
    use crate::TestResult;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[tokio::test]
    async fn learned_history_roundtrip_preserves_order_and_expiry() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let now = Timestamp::from_unix_millis(10);
        let records = vec![
            LearnedHistory {
                key: gang_spi::RecordKey {
                    kind: RecordKind::ResourceHistory,
                    id: gang_spi::RecordId::new([1; 32]),
                },
                body: vec![1],
                expires_at: Timestamp::from_unix_millis(20),
            },
            LearnedHistory {
                key: gang_spi::RecordKey {
                    kind: RecordKind::DurationHistory,
                    id: gang_spi::RecordId::new([2; 32]),
                },
                body: vec![2],
                expires_at: Timestamp::from_unix_millis(30),
            },
            LearnedHistory {
                key: gang_spi::RecordKey {
                    kind: RecordKind::ResourceHistory,
                    id: gang_spi::RecordId::new([3; 32]),
                },
                body: vec![3],
                expires_at: now,
            },
        ];
        scheduler.restore_learned_history(now, &records).await?;
        let mut out = vec![records[2].clone()];
        scheduler.learned_history(now, &mut out).await?;
        assert_eq!(out, records[..2]);
        scheduler
            .learned_history(Timestamp::from_unix_millis(20), &mut out)
            .await?;
        assert_eq!(out, records[1..2]);
        scheduler.restore_learned_history(now, &[]).await?;
        scheduler.learned_history(now, &mut out).await?;
        assert!(out.is_empty());
        Ok(())
    }

    /// The owned inputs of a [`WorkerSpec`], so a test can borrow one.
    struct WorkerFixture {
        instance: InstanceId,
        properties: FleetProperties,
        store: gang_spi::RenderedStoreSpec,
    }

    impl WorkerFixture {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let name = FleetName::try_new("example-fleet")?;
            Ok(Self {
                instance: InstanceId::try_new("i-test")?,
                properties: FleetProperties::new(
                    &name,
                    Isa::Aarch64,
                    &"noop",
                    &"host:test".parse()?,
                    Resources {
                        vcpus: 1,
                        memory: Bytes::new(GIB),
                        disk: Bytes::new(GIB),
                    },
                    &gang_types::FleetActionPolicy::default(),
                    std::iter::empty::<(&'static str, String)>(),
                ),
                store: gang_spi::RenderedStoreSpec(serde_json::Value::Null),
            })
        }

        fn spec(&self) -> WorkerSpec {
            WorkerSpec {
                actions: Arc::new(gang_config::ActionsConfig::default()),
                memory_pool: None,
                instance: self.instance.clone(),
                properties: self.properties.clone(),
                lifetime: WorkerLifetime::LongLived,
                work_directory: PathBuf::from("/work"),
                fast_cas_directory: PathBuf::from("/cas"),
                fast_cas_budget: Bytes::new(GIB),
                content_store: self.store.clone(),
                action_store: self.store.clone(),
                store_access: gang_spi::StoreAccess::Direct,
                entrypoint: PathBuf::from("/usr/bin/gang"),
            }
        }
    }

    #[tokio::test]
    async fn snapshot_reports_the_configured_snapshot_and_fails_while_switched() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        scheduler.set_snapshot(SchedulerSnapshot {
            completed: 3,
            stores: vec![EngineStore {
                store: StoreKind::Action,
                used: 3,
                budget: 7,
                evicted: 11,
            }],
            ..SchedulerSnapshot::default()
        });
        let mut into = SchedulerSnapshot::default();
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.completed, 3);
        assert_eq!(
            into.stores,
            vec![EngineStore {
                store: StoreKind::Action,
                used: 3,
                budget: 7,
                evicted: 11,
            }]
        );
        scheduler.set_snapshot(SchedulerSnapshot::default());
        scheduler.snapshot(&mut into).await?;
        assert!(into.stores.is_empty());

        scheduler.set_snapshot(SchedulerSnapshot {
            completed: 3,
            ..SchedulerSnapshot::default()
        });

        scheduler.fail_snapshot(true);
        let failed = scheduler.snapshot(&mut into).await;
        assert!(matches!(failed, Err(PortError::Unavailable(_))));

        scheduler.fail_snapshot(false);
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.completed, 3);
        Ok(())
    }

    fn snapshot_rate_fixture(clock: &gang_clock::TestClock) -> TestResult<SchedulerSnapshot> {
        use gang_clock::Clock;
        use gang_spi::{CompletionRates, InstanceCordon, NonNegativeFinite, ResourceRates};
        use gang_types::SourceId;

        let created_at = clock.monotonic();
        clock.advance(Duration::from_secs(1));
        let at = clock.monotonic();
        clock.advance(Duration::from_secs(1));
        let source = SourceId::from_identity_str("example:alice").expect("valid identity");
        let row = |source, preferred_fleet| CompletionRates {
            source,
            preferred_fleet,
            generation: 7,
            created_at,
            at,
            completions: NonNegativeFinite::try_new(0.5).expect("finite"),
            timed_completions: NonNegativeFinite::try_new(0.25).expect("finite"),
            work: ResourceRates::default(),
        };
        let cordons = [(0, "a"), (0, "z"), (1, "a")]
            .into_iter()
            .map(|(fleet, name)| {
                Ok(InstanceCordon {
                    fleet,
                    instance: Arc::new(InstanceId::try_new(name)?),
                })
            })
            .collect::<TestResult<Vec<_>>>()?;
        Ok(SchedulerSnapshot {
            source_rates: vec![row(None, 0), row(None, 1), row(Some(source), 0)],
            source_rates_at: clock.monotonic(),
            cordons,
            ..SchedulerSnapshot::default()
        })
    }

    fn snapshot_worker(name: &str) -> TestResult<gang_spi::WorkerState> {
        Ok(gang_spi::WorkerState {
            id: crate::worker_id(name)?,
            fleet: FleetName::try_new("example-fleet")?,
            environment: Some("host:test".parse()?),
            running_actions: 1,
            max_inflight_actions: None,
            capacity: None,
            reserved: None,
            draining: false,
            paused: false,
            last_seen: Timestamp::UNIX_EPOCH,
        })
    }

    fn snapshot_demand_fixture()
    -> TestResult<(gang_spi::QueuedDemand, Vec<gang_spi::ExecutingDemand>)> {
        use gang_spi::{ExecutingDemand, QueuedDemand};
        use gang_types::{ActionResources, FleetSet};

        let queued = QueuedDemand {
            source: None,
            candidates: FleetSet::prefix(1)?,
            resources: ActionResources::default(),
            queued_since: Timestamp::UNIX_EPOCH,
            priority: 0,
            expected: Some(Duration::from_secs(7)),
        };
        let executions = [(0, 10, Some(Duration::from_secs(3))), (1, 20, None)]
            .into_iter()
            .map(|(worker, started, expected)| ExecutingDemand {
                source: None,
                candidates: queued.candidates,
                resources: queued.resources,
                worker,
                started: Timestamp::from_unix_millis(started),
                expected,
            })
            .collect();
        Ok((queued, executions))
    }

    fn snapshot_capacities(snapshot: &SchedulerSnapshot) -> [usize; 5] {
        [
            snapshot.workers.capacity(),
            snapshot.queued.capacity(),
            snapshot.executing.capacity(),
            snapshot.source_rates.capacity(),
            snapshot.cordons.capacity(),
        ]
    }

    #[tokio::test]
    async fn snapshot_sorts_workers_remaps_executions_and_reuses_buffers() -> TestResult {
        use gang_clock::{Clock, TestClock};
        use gang_spi::InstanceCordon;

        let scheduler = FakeScheduler::new()?;
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let frame = snapshot_rate_fixture(&clock)?;
        let rates = frame.source_rates;
        let rates_at = frame.source_rates_at;
        let cordons = frame.cordons;
        let (queued, executions) = snapshot_demand_fixture()?;
        scheduler.set_snapshot(SchedulerSnapshot {
            workers: vec![snapshot_worker("z")?, snapshot_worker("a")?],
            queued: vec![queued.clone()],
            executing: executions.clone(),
            source_rates: rates.iter().rev().copied().collect(),
            source_rates_at: rates_at,
            cordons: cordons.iter().rev().cloned().collect(),
            ..SchedulerSnapshot::default()
        });
        let mut into = SchedulerSnapshot {
            workers: Vec::with_capacity(8),
            queued: Vec::with_capacity(8),
            executing: Vec::with_capacity(8),
            source_rates: Vec::with_capacity(8),
            cordons: Vec::with_capacity(8),
            ..SchedulerSnapshot::default()
        };
        into.workers.push(snapshot_worker("stale")?);
        into.queued.push(queued.clone());
        into.executing.push(executions[0].clone());
        let mut stale = rates[0];
        stale.preferred_fleet = 9;
        into.source_rates.push(stale);
        into.cordons.push(InstanceCordon {
            fleet: 9,
            instance: Arc::new(InstanceId::try_new("stale")?),
        });
        let capacities = snapshot_capacities(&into);
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.workers[0].id, crate::worker_id("a")?);
        assert_eq!(into.workers[1].id, crate::worker_id("z")?);
        let mut expected_executions = executions.clone();
        expected_executions[0].worker = 1;
        expected_executions[1].worker = 0;
        assert_eq!(into.executing, expected_executions);
        assert_eq!(into.source_rates, rates);
        assert_eq!(into.source_rates_at, rates_at);
        assert_eq!(into.cordons, cordons);
        assert!(Arc::ptr_eq(&into.cordons[0].instance, &cordons[0].instance));
        scheduler.snapshot(&mut into).await?;
        assert_eq!(
            into.source_rates, rates,
            "a repeated fill must not append rows"
        );
        assert_eq!(
            into.cordons, cordons,
            "a repeated fill must not append intents"
        );
        assert_eq!(into.queued, vec![queued.clone()]);
        assert_eq!(snapshot_capacities(&into), capacities);
        scheduler.set_snapshot(SchedulerSnapshot::default());
        scheduler.snapshot(&mut into).await?;
        assert!(into.workers.is_empty() && into.queued.is_empty() && into.executing.is_empty());
        assert!(into.source_rates.is_empty() && into.cordons.is_empty());
        assert_eq!(into.source_rates_at, Tick::ORIGIN);
        let mut expected = executions[1].clone();
        expected.worker = 0;
        scheduler.set_snapshot(SchedulerSnapshot {
            workers: vec![snapshot_worker("a")?],
            queued: vec![queued.clone()],
            executing: vec![expected.clone()],
            source_rates: vec![rates[2]],
            source_rates_at: clock.monotonic(),
            cordons: vec![cordons[1].clone()],
            ..SchedulerSnapshot::default()
        });
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.workers[0].id, crate::worker_id("a")?);
        assert_eq!(into.queued, vec![queued]);
        assert_eq!(into.executing, vec![expected]);
        assert_eq!(into.source_rates, vec![rates[2]]);
        assert_eq!(into.source_rates_at, clock.monotonic());
        assert_eq!(into.cordons, vec![cordons[1].clone()]);
        assert_eq!(snapshot_capacities(&into), capacities);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_hold_releases_on_send_or_drop_and_only_holds_once() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let mut into = SchedulerSnapshot::default();
        let release = scheduler.hold_snapshot();
        {
            let snapshot = scheduler.snapshot(&mut into);
            tokio::pin!(snapshot);
            assert!(futures::poll!(&mut snapshot).is_pending());
            scheduler.set_snapshot(SchedulerSnapshot {
                completed: 7,
                ..SchedulerSnapshot::default()
            });
            release.send(()).expect("snapshot is waiting");
            snapshot.await?;
        }
        assert_eq!(into.completed, 7);
        scheduler.snapshot(&mut into).await?;
        let release = scheduler.hold_snapshot();
        {
            let snapshot = scheduler.snapshot(&mut into);
            tokio::pin!(snapshot);
            assert!(futures::poll!(&mut snapshot).is_pending());
            scheduler.fail_snapshot(true);
            drop(release);
            assert!(matches!(snapshot.await, Err(PortError::Unavailable(_))));
        }
        Ok(())
    }

    #[tokio::test]
    async fn actions_fills_the_view_and_counts_calls_even_when_failing() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let limits = ListingLimits {
            running: 2,
            queued: 2,
        };
        let mut view = ActionsView::default();
        scheduler.actions(limits, &mut view).await?;
        assert_eq!(view, ActionsView::default());
        assert_eq!(scheduler.actions_calls().get(), 1);

        scheduler.fail_actions(true);
        assert!(scheduler.actions(limits, &mut view).await.is_err());
        assert_eq!(scheduler.actions_calls().get(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn physical_drain_requests_succeed_without_connected_workers() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let fleet = FleetName::try_new("example")?;
        let instance = InstanceId::try_new("worker-1")?;
        scheduler.set_draining(&fleet, &instance, true).await?;
        let mut snapshot = SchedulerSnapshot::default();
        scheduler.snapshot(&mut snapshot).await?;
        assert!(snapshot.workers.is_empty() && snapshot.cordons.is_empty());
        assert_eq!(scheduler.draining_calls(), vec![(fleet, instance, true)]);
        Ok(())
    }

    #[tokio::test]
    async fn set_draining_records_each_physical_call_with_its_flag() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let fleet = FleetName::try_new("example")?;
        let other_fleet = FleetName::try_new("other")?;
        let first = InstanceId::try_new("worker-1")?;
        let second = InstanceId::try_new("worker-2")?;
        scheduler.set_draining(&fleet, &first, true).await?;
        scheduler.set_draining(&other_fleet, &second, true).await?;
        scheduler.set_draining(&fleet, &first, false).await?;
        assert_eq!(
            scheduler.draining_calls(),
            vec![
                (fleet.clone(), first.clone(), true),
                (other_fleet, second, true),
                (fleet, first, false)
            ]
        );
        Ok(())
    }

    fn require_send<T: Send>(value: T) -> T {
        value
    }

    #[tokio::test]
    async fn retain_drains_records_the_complete_borrowed_census() -> TestResult {
        use gang_clock::{Clock, TestClock};

        let scheduler = FakeScheduler::new()?;
        let clock = TestClock::new(Timestamp::UNIX_EPOCH);
        let fleet = FleetName::try_new("example")?;
        let first = InstanceId::try_new("worker-1")?;
        let second = InstanceId::try_new("worker-2")?;
        let third = InstanceId::try_new("worker-3")?;
        let supply = FleetSupply {
            instances: [
                (first.clone(), gang_types::InstanceLifecycle::Pending),
                (second.clone(), gang_types::InstanceLifecycle::InService),
                (third.clone(), gang_types::InstanceLifecycle::Terminating),
            ]
            .into_iter()
            .map(|(id, lifecycle)| gang_spi::FleetInstance {
                id,
                lifecycle,
                launched_at: clock.wall(),
                release_first: false,
            })
            .collect(),
            ..FleetSupply::default()
        };
        let grace = Duration::from_secs(30);
        let live = [(&fleet, &supply)];
        require_send(scheduler.retain_drains(clock.monotonic(), grace, &live)).await?;
        clock.advance(Duration::from_secs(1));
        scheduler
            .retain_drains(clock.monotonic(), grace, &[])
            .await?;
        assert_eq!(
            scheduler.retain_drains_calls(),
            vec![
                (
                    Tick::ORIGIN,
                    grace,
                    vec![
                        (fleet.clone(), first),
                        (fleet.clone(), second),
                        (fleet, third)
                    ]
                ),
                (clock.monotonic(), grace, vec![])
            ]
        );
        Ok(())
    }

    #[test]
    fn refusal_answers_each_rejection_with_its_status() {
        let cases = [
            (
                PortError::refused(Rejection::InvalidProof),
                http::StatusCode::UNAUTHORIZED,
            ),
            (
                PortError::refused(Rejection::PolicyDenied),
                http::StatusCode::FORBIDDEN,
            ),
            (
                PortError::refused(Rejection::PrincipalRevoked),
                http::StatusCode::FORBIDDEN,
            ),
            (
                PortError::unavailable("scheduler is unavailable"),
                http::StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                PortError::backend(io::Error::other("scheduler broke")),
                http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (error, status) in cases {
            assert_eq!(refusal(&error).status(), status);
        }
    }

    #[test]
    fn reached_completes_once_a_worker_start_is_counted() -> TestResult {
        let scheduler = FakeScheduler::plain()?;
        let starts = scheduler.worker_starts();
        assert_eq!(starts.get(), 0);
        assert!(starts.reached(1).now_or_never().is_none());

        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        scheduler.set_behaviour(Behaviour::Finish);
        assert!(matches!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::pending())
                .now_or_never(),
            Some(Ok(()))
        ));
        assert_eq!(starts.get(), 1);
        assert!(starts.reached(1).now_or_never().is_some());
        Ok(())
    }

    #[test]
    fn finish_returns_ok_and_records_one_spec() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::plain()?;
        scheduler.set_behaviour(Behaviour::Finish);
        assert!(matches!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::pending())
                .now_or_never(),
            Some(Ok(()))
        ));

        let recorded = scheduler.specs();
        assert_eq!(recorded.len(), 1);
        let first = recorded.first().ok_or("one spec is recorded")?;
        assert_eq!(first.instance, fixture.instance);
        assert_eq!(first.lifetime, WorkerLifetime::LongLived);
        assert_eq!(first.work_directory, Path::new("/work"));
        assert_eq!(first.tls, RecordedTls::Plain);
        Ok(())
    }

    #[test]
    fn fail_returns_a_backend_error() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::plain()?;
        scheduler.set_behaviour(Behaviour::Fail);
        assert!(matches!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::pending())
                .now_or_never(),
            Some(Err(PortError::Backend(_)))
        ));
        Ok(())
    }

    #[test]
    fn pending_stays_pending_until_stopped() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::plain()?;
        scheduler.set_behaviour(Behaviour::Pending);
        assert!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::pending())
                .now_or_never()
                .is_none()
        );
        assert_eq!(scheduler.specs().len(), 1);
        Ok(())
    }

    #[test]
    fn pending_stops_when_signalled() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::plain()?;
        assert!(matches!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::ready(()))
                .now_or_never(),
            Some(Ok(()))
        ));
        assert_eq!(scheduler.worker_starts().get(), 1);
        assert_eq!(scheduler.worker_stops().get(), 1);
        Ok(())
    }

    #[test]
    fn mismatched_scheme_does_not_start_worker() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::new()?;
        assert!(matches!(
            scheduler
                .run_worker(&spec, WorkerTls::Plain, std::future::ready(()))
                .now_or_never(),
            Some(Err(PortError::Config(_)))
        ));
        assert!(scheduler.specs().is_empty());
        assert_eq!(scheduler.worker_starts().get(), 0);
        assert_eq!(scheduler.worker_stops().get(), 0);
        Ok(())
    }

    #[test]
    fn records_tls_paths() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let spec = fixture.spec();
        let scheduler = FakeScheduler::new()?;
        scheduler.set_behaviour(Behaviour::Finish);
        let ca = Path::new("/ca");
        let certificate = Path::new("/cert");
        let private_key = Path::new("/key");
        for tls in [
            WorkerTls::ServerOnly { server_ca: None },
            WorkerTls::ServerOnly {
                server_ca: Some(ca),
            },
            WorkerTls::Mutual(gang_spi::ClientTls {
                certificate,
                private_key,
                server_ca: ca,
            }),
        ] {
            assert!(matches!(
                scheduler
                    .run_worker(&spec, tls, std::future::pending())
                    .now_or_never(),
                Some(Ok(()))
            ));
        }
        let recorded = scheduler.specs();
        let tls: Vec<_> = recorded.into_iter().map(|spec| spec.tls).collect();
        assert_eq!(
            tls,
            [
                RecordedTls::ServerOnly(None),
                RecordedTls::ServerOnly(Some(ca.to_path_buf())),
                RecordedTls::Mutual {
                    certificate: certificate.to_path_buf(),
                    private_key: private_key.to_path_buf(),
                    server_ca: ca.to_path_buf(),
                },
            ]
        );
        Ok(())
    }

    /// A request gate that admits every request.
    #[derive(Clone)]
    struct AdmitAll;

    impl RequestGate for AdmitAll {
        async fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> Result<GateDecision, PortError> {
            Ok(GateDecision::Admit(Admitted::Authenticated(
                Authenticated {
                    identity: Arc::new(Identity::new(
                        ProviderName::try_new("test").expect("valid provider"),
                        Principal::try_new("tester").expect("valid principal"),
                    )),
                    valid_until: None,
                },
            )))
        }
    }

    /// A Gang API that answers every request with an empty body.
    #[derive(Clone)]
    struct EmptyApi;

    impl ApiService for EmptyApi {
        async fn call(
            &self,
            _request: http::Request<ApiRequestBody>,
        ) -> http::Response<ApiResponseBody> {
            http::Response::new(full_body(""))
        }
    }

    /// A request gate that admits like [`AdmitAll`] while open and refuses as unavailable while
    /// closed.
    #[derive(Clone)]
    struct SwitchGate(bool);

    impl RequestGate for SwitchGate {
        async fn admit(
            &self,
            scope: RequestScope,
            peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> Result<GateDecision, PortError> {
            if self.0 {
                AdmitAll.admit(scope, peer, parts).await
            } else {
                Err(PortError::unavailable("gate is closed"))
            }
        }
    }

    #[derive(Clone)]
    struct CountingGate {
        calls: CallCount,
        not_leading: bool,
    }

    impl RequestGate for CountingGate {
        async fn admit(
            &self,
            scope: RequestScope,
            peer: &Peer,
            parts: &mut http::request::Parts,
        ) -> Result<GateDecision, PortError> {
            assert!(matches!(scope, RequestScope::Shared));
            self.calls.bump();
            if self.not_leading {
                Ok(GateDecision::NotLeading)
            } else {
                AdmitAll.admit(scope, peer, parts).await
            }
        }
    }

    /// A Gang API that returns the admitted identity.
    #[derive(Clone)]
    struct CallerApi;

    impl ApiService for CallerApi {
        async fn call(
            &self,
            request: http::Request<ApiRequestBody>,
        ) -> http::Response<ApiResponseBody> {
            let identity = request.extensions().get::<Authenticated>().map_or_else(
                || "anonymous".to_owned(),
                |caller| caller.identity.to_string(),
            );
            let mut response = http::Response::new(full_body(""));
            response.headers_mut().insert(
                "x-caller",
                http::HeaderValue::from_str(&identity).expect("an identity is a header value"),
            );
            response
        }
    }

    async fn assert_controller_gates(controller: &Controller) -> TestResult {
        let public = controller.call_api(http::Request::new(full_body(""))).await;
        assert_eq!(public.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let (mut parts, ()) = http::Request::new(()).into_parts();
        assert!(
            controller
                .admit_public(RequestScope::Shared, &Peer::default(), &mut parts)
                .await
                .is_err()
        );
        assert!(
            controller
                .admit_worker(RequestScope::Shared, &Peer::default(), &mut parts)
                .await
                .is_ok()
        );
        Ok(())
    }

    fn assert_cancellation_unpublishes(scheduler: &FakeScheduler, spec: &ControllerSpec<'_>) {
        {
            let connections = ControllerConnections {
                public: Listening {
                    connections: futures::stream::pending().boxed(),
                    gate: AdmitAll,
                },
                worker: Listening {
                    connections: futures::stream::pending().boxed(),
                    gate: AdmitAll,
                },
                api: EmptyApi,
            };
            let (ready, _) = ControllerReady::channel();
            let mut run = Box::pin(scheduler.run_controller(spec, connections, ready));
            assert!(run.as_mut().now_or_never().is_none());
            assert!(scheduler.published.borrow().is_some());
        }
        assert!(scheduler.published.borrow().is_none());
    }

    #[tokio::test]
    async fn refusals_consume_both_intakes_before_drain_and_read_failure_at_completion()
    -> TestResult {
        let scheduler = FakeScheduler::new()?;
        scheduler.fail_controller(true);
        scheduler.finish_controller(true);
        for last in 0..2 {
            scheduler.fail_refusals(last == 0);
            let release = scheduler.hold_refusal_drain();
            let mut received = scheduler.refusal_connections();
            let (public_tx, public_rx) = futures::channel::mpsc::unbounded();
            let (worker_tx, worker_rx) = futures::channel::mpsc::unbounded();
            let mut senders = [Some(public_tx), Some(worker_tx)];
            let connections = ControllerConnections {
                public: Listening {
                    connections: public_rx.boxed(),
                    gate: AdmitAll,
                },
                worker: Listening {
                    connections: worker_rx.boxed(),
                    gate: AdmitAll,
                },
                api: EmptyApi,
            };
            let run = scheduler.serve_standby(connections);
            futures::pin_mut!(run);
            assert!(run.as_mut().now_or_never().is_none());
            assert_eq!(scheduler.refusal_runs().get(), last + 1);
            for (index, side) in [Side::Public, Side::Worker].into_iter().enumerate() {
                let (connection, _peer) = tokio::io::duplex(64);
                senders[index]
                    .as_ref()
                    .ok_or("sender missing")?
                    .unbounded_send(gang_spi::Accepted {
                        connection: Box::new(connection),
                        peer: Peer::default(),
                    })?;
                assert!(run.as_mut().now_or_never().is_none());
                assert_eq!(received.try_recv()?.side, side);
            }
            drop(senders[1 - last].take());
            assert!(run.as_mut().now_or_never().is_none());
            let (connection, _peer) = tokio::io::duplex(64);
            senders[last]
                .as_ref()
                .ok_or("last sender missing")?
                .unbounded_send(gang_spi::Accepted {
                    connection: Box::new(connection),
                    peer: Peer::default(),
                })?;
            assert!(run.as_mut().now_or_never().is_none());
            assert_eq!(
                received.try_recv()?.side,
                [Side::Public, Side::Worker][last]
            );
            drop(senders[last].take());
            assert!(run.as_mut().now_or_never().is_none());
            assert!(scheduler.published.borrow().is_none());
            assert_eq!(scheduler.controller_runs().get(), 0);
            assert_eq!(scheduler.drains().get(), 0);
            assert_eq!(scheduler.snapshots().get(), 0);
            scheduler.fail_refusals(last == 1);
            drop(release);
            let outcome = run.as_mut().now_or_never();
            if last == 0 {
                assert!(matches!(outcome, Some(Ok(()))));
            } else {
                assert!(matches!(outcome, Some(Err(PortError::Backend(_)))));
            }
            assert!(matches!(received.recv().now_or_never(), Some(None)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn refusals_do_not_publish_or_replace_a_draining_controller() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let stores = gang_spi::RenderedStoreSpec(serde_json::Value::Null);
        let catalog = gang_types::FleetCatalog::try_new(vec![fixture.properties.clone()])?;
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(gang_clock::TestClock::new(Timestamp::UNIX_EPOCH)),
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            max_workers: 8,
            client_action_timeout: std::time::Duration::from_mins(1),
            observations: &observations,
        };
        let scheduler = FakeScheduler::new()?;
        let release = scheduler.hold_drain();
        let empty = || ControllerConnections {
            public: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            worker: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            api: EmptyApi,
        };
        let (ready, mut readiness) = ControllerReady::channel();
        let controller = scheduler.run_controller(&spec, empty(), ready);
        futures::pin_mut!(controller);
        assert!(controller.as_mut().now_or_never().is_none());
        assert!(readiness.try_recv().is_ok());
        assert_eq!(scheduler.drains().get(), 1);
        let public_calls = CallCount::new();
        let worker_calls = CallCount::new();
        let standby_connections = ControllerConnections {
            public: Listening {
                connections: futures::stream::pending().boxed(),
                gate: CountingGate {
                    calls: public_calls.clone(),
                    not_leading: false,
                },
            },
            worker: Listening {
                connections: futures::stream::pending().boxed(),
                gate: CountingGate {
                    calls: worker_calls.clone(),
                    not_leading: false,
                },
            },
            api: CallerApi,
        };
        let standby = scheduler.serve_standby(standby_connections);
        futures::pin_mut!(standby);
        assert!(standby.as_mut().now_or_never().is_none());
        let services = scheduler
            .standby_services()
            .await
            .ok_or("standby missing")?;
        let response = services.call_api(http::Request::new(full_body(""))).await;
        assert_eq!(response.headers()["x-caller"], "test:tester");
        let (mut parts, ()) = http::Request::new(()).into_parts();
        assert!(matches!(
            services
                .admit_public(RequestScope::Shared, &Peer::default(), &mut parts)
                .await?,
            GateDecision::Admit(_)
        ));
        assert!(matches!(
            services
                .admit_worker(RequestScope::Shared, &Peer::default(), &mut parts)
                .await?,
            GateDecision::Admit(_)
        ));
        assert!(matches!(
            services
                .admit_public(RequestScope::Leader, &Peer::default(), &mut parts)
                .await?,
            GateDecision::NotLeading
        ));
        assert!(matches!(
            services
                .admit_worker(RequestScope::Leader, &Peer::default(), &mut parts)
                .await?,
            GateDecision::NotLeading
        ));
        assert_eq!(public_calls.get(), 2);
        assert_eq!(worker_calls.get(), 1);
        assert!(scheduler.published.borrow().is_some());
        assert_eq!(scheduler.controller_runs().get(), 1);
        assert_eq!(scheduler.refusal_runs().get(), 1);
        assert!(controller.as_mut().now_or_never().is_none());
        drop(release);
        controller.await?;
        assert!(scheduler.published.borrow().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn controller_serves_all_listeners_until_each_stream_ends() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let stores = gang_spi::RenderedStoreSpec(serde_json::Value::Null);
        let catalog = gang_types::FleetCatalog::try_new(vec![fixture.properties.clone()])?;
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(gang_clock::TestClock::new(Timestamp::UNIX_EPOCH)),
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            max_workers: 8,
            client_action_timeout: std::time::Duration::from_mins(1),
            observations: &observations,
        };
        let scheduler = FakeScheduler::new()?;
        for last in 0..2 {
            let mut received = scheduler.connections();
            let (public_tx, public_rx) = futures::channel::mpsc::unbounded();
            let (worker_tx, worker_rx) = futures::channel::mpsc::unbounded();
            let mut senders = [Some(public_tx), Some(worker_tx)];
            let connections = ControllerConnections {
                public: Listening {
                    connections: public_rx.boxed(),
                    gate: SwitchGate(false),
                },
                worker: Listening {
                    connections: worker_rx.boxed(),
                    gate: AdmitAll,
                },
                api: EmptyApi,
            };
            let (ready, mut readiness) = ControllerReady::channel();
            let run = scheduler.run_controller(&spec, connections, ready);
            futures::pin_mut!(run);
            assert!(run.as_mut().now_or_never().is_none());
            assert!(readiness.try_recv().is_ok());
            assert_eq!(scheduler.controller_runs().get(), last + 1);
            let (other_ready, _) = ControllerReady::channel();
            let other = ControllerConnections {
                public: Listening {
                    connections: futures::stream::pending().boxed(),
                    gate: AdmitAll,
                },
                worker: Listening {
                    connections: futures::stream::pending().boxed(),
                    gate: AdmitAll,
                },
                api: EmptyApi,
            };
            assert!(matches!(
                scheduler.run_controller(&spec, other, other_ready).await,
                Err(PortError::Config(_))
            ));

            let controller = scheduler
                .controller()
                .await
                .ok_or("controller not published")?;
            assert_controller_gates(&controller).await?;

            for (sender, side) in senders.iter().zip([Side::Public, Side::Worker]) {
                let (connection, _other) = tokio::io::duplex(64);
                let accepted = gang_spi::Accepted {
                    connection: Box::new(connection),
                    peer: Peer::default(),
                };
                assert!(
                    sender
                        .as_ref()
                        .ok_or("sender missing")?
                        .unbounded_send(accepted)
                        .is_ok()
                );
                assert!(run.as_mut().now_or_never().is_none());
                assert_eq!(
                    received.recv().await.ok_or("connection missing")?.side,
                    side
                );
            }
            for (index, sender) in senders.iter_mut().enumerate() {
                if index != last {
                    drop(sender.take());
                    assert!(run.as_mut().now_or_never().is_none());
                }
            }
            drop(senders[last].take());
            assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
            assert!(scheduler.published.borrow().is_none());
            assert!(received.recv().await.is_none());
        }
        assert_readiness_hold_cases(&scheduler, &spec).await?;
        assert_readiness_cancellation_drain(&scheduler, &spec).await;
        assert_cancellation_unpublishes(&scheduler, &spec);
        Ok(())
    }

    async fn assert_readiness_hold_cases(
        scheduler: &FakeScheduler,
        spec: &ControllerSpec<'_>,
    ) -> TestResult {
        for (cancel, simultaneous) in [(true, false), (false, false), (false, true)] {
            let release = scheduler.hold_ready();
            let mut received = scheduler.connections();
            let (public_tx, public_rx) = futures::channel::mpsc::unbounded();
            let (worker_tx, worker_rx) = futures::channel::mpsc::unbounded();
            let connections = ControllerConnections {
                public: Listening {
                    connections: public_rx.boxed(),
                    gate: AdmitAll,
                },
                worker: Listening {
                    connections: worker_rx.boxed(),
                    gate: AdmitAll,
                },
                api: EmptyApi,
            };
            let (ready, mut readiness) = ControllerReady::channel();
            let run = scheduler.run_controller(spec, connections, ready);
            futures::pin_mut!(run);
            assert!(run.as_mut().now_or_never().is_none());
            let (connection, _other) = tokio::io::duplex(64);
            public_tx.unbounded_send(gang_spi::Accepted {
                connection: Box::new(connection),
                peer: Peer::default(),
            })?;
            assert!(run.as_mut().now_or_never().is_none());
            assert!(received.try_recv().is_err());
            if cancel {
                drop(public_tx);
                assert!(run.as_mut().now_or_never().is_none());
                drop(worker_tx);
                assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
                assert!(readiness.await.is_err());
                assert!(release.send(()).is_err());
                assert!(scheduler.published.borrow().is_none());
            } else {
                release.send(()).expect("readiness hold is active");
                if simultaneous {
                    drop(public_tx);
                    drop(worker_tx);
                    assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
                } else {
                    assert!(run.as_mut().now_or_never().is_none());
                    drop(public_tx);
                    drop(worker_tx);
                    assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
                }
                assert!(readiness.try_recv().is_ok());
                assert_eq!(received.try_recv()?.side, Side::Public);
            }
        }
        Ok(())
    }

    async fn assert_readiness_cancellation_drain(
        scheduler: &FakeScheduler,
        spec: &ControllerSpec<'_>,
    ) {
        let ready_release = scheduler.hold_ready();
        let drain_release = scheduler.hold_drain();
        let connections = ControllerConnections {
            public: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            worker: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            api: EmptyApi,
        };
        let (ready, readiness) = ControllerReady::channel();
        let run = scheduler.run_controller(spec, connections, ready);
        futures::pin_mut!(run);
        let previous_drains = scheduler.drains().get();
        assert!(run.as_mut().now_or_never().is_none());
        assert_eq!(scheduler.drains().get(), previous_drains + 1);
        assert!(readiness.await.is_err());
        assert!(ready_release.send(()).is_err());
        assert!(scheduler.published.borrow().is_some());
        drop(drain_release);
        assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
        assert!(scheduler.published.borrow().is_none());
    }

    #[tokio::test]
    async fn the_api_receives_the_caller_its_gate_admitted() {
        let gates = Gates {
            public: AdmitAll,
            worker: AdmitAll,
            api: CallerApi,
            standby: false,
        };
        let response = gates.call_api(http::Request::new(full_body(""))).await;
        assert_eq!(response.headers()["x-caller"], "test:tester");
    }

    #[tokio::test]
    async fn api_not_leading_refuses_before_calling_api() {
        let calls = CallCount::new();
        let gates = Gates {
            public: CountingGate {
                calls: calls.clone(),
                not_leading: true,
            },
            worker: AdmitAll,
            api: CallerApi,
            standby: false,
        };
        let response = gates.call_api(http::Request::new(full_body(""))).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
            "true"
        );
        assert!(!response.headers().contains_key("x-caller"));
        assert!(response.extensions().get::<LeaderUnavailable>().is_some());
    }

    #[test]
    fn finish_controller_makes_run_controller_return_at_once() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let stores = gang_spi::RenderedStoreSpec(serde_json::Value::Null);
        let catalog = gang_types::FleetCatalog::try_new(vec![fixture.properties.clone()])?;
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(gang_clock::TestClock::new(Timestamp::UNIX_EPOCH)),
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            max_workers: 8,
            client_action_timeout: std::time::Duration::from_mins(1),
            observations: &observations,
        };
        let scheduler = FakeScheduler::new()?;
        let mut received = scheduler.connections();
        scheduler.finish_controller(true);

        let connections = ControllerConnections {
            public: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            worker: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            api: EmptyApi,
        };
        let (ready, mut readiness) = ControllerReady::channel();
        let outcome = scheduler
            .run_controller(&spec, connections, ready)
            .now_or_never();
        assert!(matches!(outcome, Some(Ok(()))));
        assert!(readiness.try_recv().is_ok());
        // The connections channel closed without a connection being forwarded.
        assert!(matches!(received.recv().now_or_never(), Some(None)));
        Ok(())
    }

    #[test]
    fn a_failing_controller_drops_its_ready_signal_unsent() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let stores = gang_spi::RenderedStoreSpec(serde_json::Value::Null);
        let catalog = gang_types::FleetCatalog::try_new(vec![fixture.properties.clone()])?;
        let (observations, _receiver) = gang_spi::ObservationSink::channel(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::MIN,
        );
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            scaling: Arc::new(gang_config::ScalingConfig::default()),
            clock: Arc::new(gang_clock::TestClock::new(Timestamp::UNIX_EPOCH)),
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            max_workers: 8,
            client_action_timeout: std::time::Duration::from_mins(1),
            observations: &observations,
        };
        let scheduler = FakeScheduler::new()?;
        scheduler.fail_controller(true);
        let connections = ControllerConnections {
            public: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            worker: Listening {
                connections: futures::stream::empty().boxed(),
                gate: AdmitAll,
            },
            api: EmptyApi,
        };
        let (ready, mut readiness) = ControllerReady::channel();
        let outcome = scheduler
            .run_controller(&spec, connections, ready)
            .now_or_never();
        assert!(matches!(outcome, Some(Err(_))));
        assert!(readiness.try_recv().is_err());
        Ok(())
    }
}
