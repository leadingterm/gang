//! The fake scheduler: a controller that serves the connections it is handed, with each
//! listener's gate applied to its requests, and a worker side that records every spec it is
//! asked to run and behaves as scripted.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use futures::future::{BoxFuture, Shared};
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::{
    ActionsView, Admitted, ApiRequestBody, ApiResponseBody, ApiService, BoxedConnection,
    ControllerConnections, ControllerDrain, ControllerEndpoints, ControllerReady, ControllerSpec,
    DrainingWorker, FleetSupply,
    GateDecision, LeaderUnavailable, LearnedHistory, Listening, ListingLimits, Peer, PortError,
    RecordKind, Rejection, RequestGate, RequestScope, SchedulerPort, SchedulerSnapshot, Slots,
    WorkerSpec, WorkerTls, full_body,
};
use gang_types::{Bytes, FleetName, FleetProperties, InstanceId};
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
    /// Whether handoff has frozen new controller requests.
    draining: Arc<AtomicBool>,
}

impl<P: RequestGate, W: RequestGate, A: ApiService> Services for Gates<P, W, A> {
    fn admit_public<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>> {
        Box::pin(async move {
            let decision = self.public.admit(scope, peer, parts).await?;
            Ok(if self.draining.load(Ordering::Acquire) && scope == RequestScope::Controller {
                GateDecision::NotLeading
            } else {
                decision
            })
        })
    }

    fn admit_worker<'a>(
        &'a self,
        scope: RequestScope,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<GateDecision, PortError>> {
        Box::pin(async move {
            let decision = self.worker.admit(scope, peer, parts).await?;
            Ok(if self.draining.load(Ordering::Acquire) && scope == RequestScope::Controller {
                GateDecision::NotLeading
            } else {
                decision
            })
        })
    }

    fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> BoxFuture<'static, http::Response<ApiResponseBody>> {
        Box::pin(admit_and_call(
            self.public.clone(),
            self.api.clone(),
            Arc::clone(&self.draining),
            request,
        ))
    }
}

async fn admit_and_call<P: RequestGate, A: ApiService>(
    gate: P,
    api: A,
    draining: Arc<AtomicBool>,
    request: http::Request<ApiRequestBody>,
) -> http::Response<ApiResponseBody> {
    let (mut parts, body) = request.into_parts();
    match gate
        .admit(RequestScope::Controller, &Peer::default(), &mut parts)
        .await
    {
        Ok(GateDecision::Admit(admitted)) if !draining.load(Ordering::Acquire) => {
            if let Admitted::Authenticated(authenticated) = admitted {
                parts.extensions.insert(authenticated);
            }
            api.call(http::Request::from_parts(parts, body)).await
        }
        Ok(GateDecision::NotLeading | GateDecision::Admit(_)) => {
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
    /// How many actions the worker takes at once and whether it serves one only.
    pub slots: Slots,
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

/// Which operations of a [`FakeScheduler`] fail.
#[derive(Default)]
struct Failing {
    /// Snapshot queries.
    snapshot: bool,
    /// Actions queries.
    actions: bool,
    /// `run_controller`, which fails at once.
    controller: bool,
}

type DrainRetention = (Tick, Duration, Vec<(FleetName, InstanceId)>);

/// The state a [`FakeScheduler`] shares between its clones.
struct SchedulerState {
    /// The worker endpoint `endpoints` reports.
    endpoint: Url,
    /// The snapshot `snapshot` reports.
    snapshot: SchedulerSnapshot,
    /// Explicit dispatch-stamped maximum attempt deadlines by physical worker.
    attempt_deadlines: Vec<DrainingWorker>,
    /// Controller clock used to evaluate dispatch-stamped deadlines.
    clock: Option<Arc<dyn Clock>>,
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
    /// How the next worker behaves.
    behaviour: Behaviour,
    /// Every spec `run_worker` has been asked to run.
    specs: Vec<RecordedSpec>,
    /// The `set_draining` calls that succeeded, in order, with their flags.
    draining_calls: Vec<(FleetName, InstanceId, bool)>,
    /// Complete provider census requests, in call order.
    retention_calls: Vec<DrainRetention>,
    /// Scripted response hold, shared between handoff observation and controller shutdown.
    drain_hold: Option<Shared<BoxFuture<'static, ()>>>,
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
    drain_hold: Option<Shared<BoxFuture<'static, ()>>>,
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
    /// Shared admission freeze for the currently published controller.
    draining: Arc<AtomicBool>,
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
                attempt_deadlines: Vec::new(),
                clock: None,
                snapshot_hold: None,
                actions: ActionsView::default(),
                history: Vec::new(),
                failing: Failing::default(),
                finish_controller: false,
                connections: None,
                behaviour: Behaviour::Pending,
                specs: Vec::new(),
                draining_calls: Vec::new(),
                retention_calls: Vec::new(),
                drain_hold: None,
                ready_hold: None,
            })),
            published: Arc::new(watch::channel::<Option<Controller>>(None).0),
            draining: Arc::new(AtomicBool::new(false)),
            controller_runs: CallCount::new(),
            worker_starts: CallCount::new(),
            worker_stops: CallCount::new(),
            actions_calls: CallCount::new(),
            snapshots: CallCount::new(),
            drains: CallCount::new(),
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

    /// Replaces the authoritative dispatch-stamped maximum deadline of each physical worker.
    ///
    /// Tests must supply a stamp for every worker with outstanding attempts. The fake never
    /// infers dispatch time or a deadline from an action's execution start time.
    pub fn set_attempt_deadlines(&self, workers: Vec<DrainingWorker>) {
        self.state.lock().attempt_deadlines = workers;
    }

    /// Sets the clock for direct port tests; a controller run supplies its own clock.
    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        self.state.lock().clock = Some(clock);
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

    /// Counts first handoff calls, once per controller term.
    #[must_use]
    pub fn drains(&self) -> CallCount {
        self.drains.clone()
    }

    /// Counts controller terms at their atomic publication.
    #[must_use]
    pub fn controller_runs(&self) -> CallCount {
        self.controller_runs.clone()
    }

    /// Holds a controller response until the returned sender is used or dropped. The response
    /// may finish while the controller's refusal listeners remain open.
    #[must_use]
    pub fn hold_drain(&self) -> oneshot::Sender<()> {
        let (release, hold) = oneshot::channel();
        self.state.lock().drain_hold = Some(async move { let _ = hold.await; }.boxed().shared());
        release
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
            self.draining.store(false, Ordering::Release);
            state.clock = Some(Arc::clone(&spec.clock));
            (
                state.failing.controller,
                state.finish_controller,
                state.connections.take(),
                state.drain_hold.clone(),
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
                draining: Arc::clone(&self.draining),
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
        serve_controller_connections(merged, sender, ready, ready_hold, drain_hold)
            .await;
        self.state.lock().drain_hold = None;
        Ok(())
    }

    async fn hand_off(&self, into: &mut ControllerDrain) -> Result<(), PortError> {
        if !self.draining.swap(true, Ordering::AcqRel) {
            self.drains.bump();
        }
        let mut state = self.state.lock();
        into.workers.clear();
        let has_work = !state.snapshot.executing.is_empty()
            || !state.actions.running.is_empty()
            || state.snapshot.workers.iter().any(|worker| worker.running_actions != 0);
        let now = if has_work {
            state.clock.as_ref().ok_or_else(|| {
                PortError::config("handoff with attempts requires a controller clock")
            })?.wall()
        } else {
            Timestamp::UNIX_EPOCH
        };
        for action in &state.snapshot.executing {
            let worker = state.snapshot.workers.get(action.worker).ok_or_else(|| {
                PortError::config("executing action references an unknown worker")
            })?;
            let instance = worker.id.instance().ok_or_else(|| {
                PortError::config("executing worker has no physical instance identity")
            })?;
            let stamp = state.attempt_deadlines.iter().find(|stamp| {
                stamp.fleet == worker.fleet && stamp.instance == instance
            }).ok_or_else(|| PortError::config("executing worker has no dispatch-stamped attempt deadline"))?;
            if stamp.deadline > now && !into.workers.iter().any(|entry| {
                entry.fleet == stamp.fleet && entry.instance == stamp.instance
            }) {
                into.workers.push(stamp.clone());
            }
        }
        for worker in &state.snapshot.workers {
            if worker.running_actions == 0 {
                continue;
            }
            let instance = worker.id.instance().ok_or_else(|| {
                PortError::config("running worker has no physical instance identity")
            })?;
            let stamp = state.attempt_deadlines.iter().find(|stamp| {
                stamp.fleet == worker.fleet && stamp.instance == instance
            }).ok_or_else(|| PortError::config("running worker has no dispatch-stamped attempt deadline"))?;
            if stamp.deadline > now && !into.workers.iter().any(|entry| {
                entry.fleet == stamp.fleet && entry.instance == stamp.instance
            }) {
                into.workers.push(stamp.clone());
            }
        }
        for action in &state.actions.running {
            let worker = state.snapshot.workers.iter().find(|worker| worker.id == action.worker)
                .ok_or_else(|| PortError::config("running action references an unknown worker"))?;
            let instance = worker.id.instance().ok_or_else(|| {
                PortError::config("running worker has no physical instance identity")
            })?;
            let stamp = state.attempt_deadlines.iter().find(|stamp| {
                stamp.fleet == worker.fleet && stamp.instance == instance
            }).ok_or_else(|| PortError::config("running worker has no dispatch-stamped attempt deadline"))?;
            if stamp.deadline > now && !into.workers.iter().any(|entry| {
                entry.fleet == stamp.fleet && entry.instance == stamp.instance
            }) {
                into.workers.push(stamp.clone());
            }
        }
        into.workers.sort_unstable_by(|left, right| {
            (&left.fleet, &left.instance).cmp(&(&right.fleet, &right.instance))
        });
        let held = !into.workers.is_empty();
        for worker in &mut state.snapshot.workers {
            worker.draining = true;
        }
        let response_held = state.drain_hold.as_ref()
            .is_some_and(|hold| hold.clone().now_or_never().is_none());
        into.complete = !held && !response_held;
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
                slots: spec.slots,
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
    use std::num::NonZeroU32;
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
                slots: Slots::LongLived(NonZeroU32::MIN.saturating_add(3)),
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

    #[tokio::test]
    async fn handoff_uses_dispatch_stamps_even_after_delayed_execution() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let clock = Arc::new(gang_clock::TestClock::new(Timestamp::UNIX_EPOCH));
        scheduler.set_clock(clock.clone());
        let (_, mut executions) = snapshot_demand_fixture()?;
        executions[0].started = Timestamp::from_unix_millis(12_000);
        executions[1].started = Timestamp::from_unix_millis(14_000);
        let mut workers = vec![snapshot_worker("b")?, snapshot_worker("a")?];
        workers[0].fleet = FleetName::try_new("a-fleet")?;
        workers[1].fleet = FleetName::try_new("z-fleet")?;
        let stamps = workers.iter().enumerate().rev().map(|(index, worker)| {
            Ok(DrainingWorker {
                fleet: worker.fleet.clone(),
                instance: worker.id.instance().ok_or("worker instance missing")?,
                deadline: Timestamp::from_unix_millis(15_000 + index as u64),
            })
        }).collect::<TestResult<Vec<_>>>()?;
        scheduler.set_snapshot(SchedulerSnapshot {
            workers,
            executing: executions,
            ..SchedulerSnapshot::default()
        });
        let mut drain = ControllerDrain::default();
        assert!(matches!(
            scheduler.hand_off(&mut drain).await,
            Err(PortError::Config(_))
        ));
        scheduler.set_attempt_deadlines(stamps);
        scheduler.hand_off(&mut drain).await?;
        assert!(!drain.complete);
        assert_eq!(drain.workers.len(), 2);
        assert_eq!(drain.workers[0].fleet, FleetName::try_new("a-fleet")?);
        assert_eq!(drain.workers[1].fleet, FleetName::try_new("z-fleet")?);
        assert_eq!(drain.workers[0].deadline, Timestamp::from_unix_millis(15_000));
        assert_eq!(drain.workers[1].deadline, Timestamp::from_unix_millis(15_001));
        clock.advance(Duration::from_millis(15_000));
        scheduler.hand_off(&mut drain).await?;
        assert!(!drain.complete);
        assert_eq!(drain.workers.len(), 1);
        assert_eq!(drain.workers[0].deadline, Timestamp::from_unix_millis(15_001));
        clock.advance(Duration::from_millis(1));
        scheduler.hand_off(&mut drain).await?;
        assert!(drain.complete);
        assert!(drain.workers.is_empty());
        Ok(())
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
        assert_eq!(
            first.slots,
            Slots::LongLived(NonZeroU32::MIN.saturating_add(3))
        );
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
            assert!(matches!(scope, RequestScope::Controller));
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
                .admit_public(RequestScope::Controller, &Peer::default(), &mut parts)
                .await
                .is_err()
        );
        assert!(
            controller
                .admit_worker(RequestScope::Controller, &Peer::default(), &mut parts)
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
            attempt_budget: gang_config::WorkerConfig::default().attempt_budget(gang_config::ActionsConfig::default().timeout),
            observations: &observations,
        };
        {
            let scheduler = FakeScheduler::new()?;
            let release = scheduler.hold_drain();
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
            let (ready, mut readiness) = ControllerReady::channel();
            let run = scheduler.run_controller(&spec, connections, ready);
            futures::pin_mut!(run);
            assert!(run.as_mut().now_or_never().is_none());
            assert!(readiness.try_recv().is_ok());
            let mut drain = ControllerDrain::default();
            scheduler.hand_off(&mut drain).await?;
            assert_eq!(scheduler.drains().get(), 1);
            assert!(!drain.complete);
            release.send(()).map_err(|()| "held response disappeared")?;
            scheduler.hand_off(&mut drain).await?;
            assert_eq!(scheduler.drains().get(), 1);
            assert!(drain.complete);
            assert!(run.as_mut().now_or_never().is_none());
            assert!(scheduler.published.borrow().is_some());
        }
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
        let mut drain = ControllerDrain::default();
        scheduler.hand_off(&mut drain).await.expect("handoff succeeds");
        assert!(!drain.complete);
        assert_eq!(scheduler.drains().get(), previous_drains + 1);
        assert!(readiness.await.is_err());
        assert!(ready_release.send(()).is_err());
        assert!(scheduler.published.borrow().is_some());
        drop(drain_release);
        assert!(matches!(run.as_mut().now_or_never(), Some(Ok(()))));
        scheduler.hand_off(&mut drain).await.expect("handoff refresh succeeds");
        assert!(drain.complete);
        assert!(scheduler.published.borrow().is_none());
    }

    #[tokio::test]
    async fn the_api_receives_the_caller_its_gate_admitted() {
        let gates = Gates {
            public: AdmitAll,
            worker: AdmitAll,
            api: CallerApi,
            draining: Arc::new(AtomicBool::new(false)),
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
            draining: Arc::new(AtomicBool::new(false)),
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
            attempt_budget: gang_config::WorkerConfig::default().attempt_budget(gang_config::ActionsConfig::default().timeout),
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
            attempt_budget: gang_config::WorkerConfig::default().attempt_budget(gang_config::ActionsConfig::default().timeout),
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
