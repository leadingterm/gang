//! The fake scheduler: a controller that serves the connections it is handed, with each
//! listener's gate applied to its requests, and a worker side that records every spec it is
//! asked to run and behaves as scripted.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use gang_spi::{
    ActionsView, ApiRequestBody, ApiResponseBody, ApiService, BoxedConnection,
    ControllerConnections, ControllerEndpoints, ControllerReady, ControllerSpec, Listening,
    ListingLimits, Peer, PortError, Rejection, RequestGate, SchedulerPort, SchedulerSnapshot,
    Slots, Subject, WorkerSpec, WorkerTls, full_body,
};
use gang_types::{Bytes, FleetProperties, InstanceId, WorkerId};
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
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<(), PortError>>;

    /// Applies the worker listener's gate to a request.
    fn admit_worker<'a>(
        &'a self,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<(), PortError>>;

    /// Answers a request with the Gang API.
    fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> BoxFuture<'static, http::Response<ApiResponseBody>>;
}

/// The gates of both listeners and the Gang API of one controller.
struct Gates<P, W, A> {
    /// The public listener's gate.
    public: P,
    /// The worker listener's gate.
    worker: W,
    /// The Gang API.
    api: A,
}

impl<P: RequestGate, W: RequestGate, A: ApiService> Services for Gates<P, W, A> {
    fn admit_public<'a>(
        &'a self,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<(), PortError>> {
        Box::pin(self.public.admit(peer, parts))
    }

    fn admit_worker<'a>(
        &'a self,
        peer: &'a Peer,
        parts: &'a mut http::request::Parts,
    ) -> BoxFuture<'a, Result<(), PortError>> {
        Box::pin(self.worker.admit(peer, parts))
    }

    fn call_api(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> BoxFuture<'static, http::Response<ApiResponseBody>> {
        let api = self.api.clone();
        let gate = self.public.clone();
        Box::pin(async move {
            let (mut parts, body) = request.into_parts();
            match gate.admit(&Peer::default(), &mut parts).await {
                Ok(()) => api.call(http::Request::from_parts(parts, body)).await,
                Err(error) => refusal(&error),
            }
        })
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
    /// The gates and the Gang API.
    services: Arc<dyn Services>,
}

impl Controller {
    /// Applies the public listener's gate to a request, as the controller does before any
    /// public service sees it.
    ///
    /// # Errors
    ///
    /// Whatever the public gate rejects the request with.
    pub async fn admit_public(
        &self,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<(), PortError> {
        self.services.admit_public(peer, parts).await
    }

    /// Applies the worker listener's gate to a request, as the controller does before any worker
    /// service sees it.
    ///
    /// # Errors
    ///
    /// Whatever the worker gate rejects the request with.
    pub async fn admit_worker(
        &self,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> Result<(), PortError> {
        self.services.admit_worker(peer, parts).await
    }

    /// Answers a request with the Gang API after the public listener's gate admitted it; a
    /// request the gate refuses is answered with the refusal's status and no body.
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

/// The state a [`FakeScheduler`] shares between its clones.
struct SchedulerState {
    /// The worker endpoint `endpoints` reports.
    endpoint: Url,
    /// The snapshot `snapshot` reports.
    snapshot: SchedulerSnapshot,
    /// The actions `actions` reports.
    actions: ActionsView,
    /// Workers that are no longer connected; drain requests for them report not found.
    vanished: Vec<WorkerId>,
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
    draining_calls: Vec<(WorkerId, bool)>,
    /// Released by the test; while held, `run_controller` waits on it after its connections end.
    drain_hold: Option<oneshot::Receiver<()>>,
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

/// A scheduler that serves as both sides of the scheduler port. Its controller reports a
/// configured snapshot, forwards the connections it receives, and refuses drain requests for
/// vanished workers. Its worker records every spec it is asked to run and behaves as scripted.
///
/// Clones share state.
#[derive(Clone)]
pub struct FakeScheduler {
    /// The state shared by every clone.
    state: Arc<Mutex<SchedulerState>>,
    /// The controller `run_controller` publishes; `controller` waits for it.
    published: Arc<watch::Sender<Option<Controller>>>,
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
                actions: ActionsView::default(),
                vanished: Vec::new(),
                failing: Failing::default(),
                finish_controller: false,
                connections: None,
                behaviour: Behaviour::Pending,
                specs: Vec::new(),
                draining_calls: Vec::new(),
                drain_hold: None,
            })),
            published: Arc::new(watch::channel::<Option<Controller>>(None).0),
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

    /// Sets the snapshot returned by [`SchedulerPort::snapshot`], sorting workers by id.
    pub fn set_snapshot(&self, mut snapshot: SchedulerSnapshot) {
        snapshot.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        self.state.lock().snapshot = snapshot;
    }

    /// Makes snapshot queries fail while `fail` is true.
    pub fn fail_snapshot(&self, fail: bool) {
        self.state.lock().failing.snapshot = fail;
    }

    /// Removes `worker` from the scheduler: later drain requests for it report not found.
    pub fn vanish(&self, worker: WorkerId) {
        self.state.lock().vanished.push(worker);
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

    /// Holds the next controller run open after its connections end, as a controller draining
    /// its connections, until the returned sender is used or dropped.
    #[must_use]
    pub fn hold_drain(&self) -> oneshot::Sender<()> {
        let (release, hold) = oneshot::channel();
        self.state.lock().drain_hold = Some(hold);
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

    /// The `set_draining` calls that succeeded, in the order they were made, each with its
    /// flag: `true` starts draining the worker and `false` cancels the drain. A call the fake
    /// refuses for a vanished worker is not listed.
    #[must_use]
    pub fn draining_calls(&self) -> Vec<(WorkerId, bool)> {
        self.state.lock().draining_calls.clone()
    }
}

impl SchedulerPort for FakeScheduler {
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        self.snapshots.bump();
        let state = self.state.lock();
        if state.failing.snapshot {
            return Err(PortError::unavailable("scheduler is unavailable"));
        }
        *into = state.snapshot.clone();
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

    async fn set_draining(&self, worker: &WorkerId, draining: bool) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.vanished.contains(worker) {
            return Err(PortError::NotFound(Subject::Worker(worker.clone())));
        }
        state.draining_calls.push((worker.clone(), draining));
        Ok(())
    }

    fn endpoints(&self) -> ControllerEndpoints {
        ControllerEndpoints {
            worker: self.state.lock().endpoint.clone(),
        }
    }

    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        _spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        let (fail, finish, sender, drain_hold) = {
            let mut state = self.state.lock();
            (
                state.failing.controller,
                state.finish_controller,
                state.connections.take(),
                state.drain_hold.take(),
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
            services: Arc::new(Gates {
                public: public_gate,
                worker: worker_gate,
                api,
            }),
        };
        self.published.send_replace(Some(controller));
        ready.signal();

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
        if let Some(hold) = drain_hold {
            self.drains.bump();
            let _ = hold.await;
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
    use gang_types::{FleetName, Isa, Resources};

    use super::*;
    use crate::TestResult;

    const GIB: u64 = 1024 * 1024 * 1024;

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
                    Resources {
                        vcpus: 1,
                        memory: Bytes::new(GIB),
                        disk: Bytes::new(GIB),
                    },
                    std::iter::empty::<(&'static str, String)>(),
                ),
                store: gang_spi::RenderedStoreSpec(serde_json::Value::Null),
            })
        }

        fn spec(&self) -> WorkerSpec {
            WorkerSpec {
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
            ..SchedulerSnapshot::default()
        });
        let mut into = SchedulerSnapshot::default();
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.completed, 3);

        scheduler.fail_snapshot(true);
        let failed = scheduler.snapshot(&mut into).await;
        assert!(matches!(failed, Err(PortError::Unavailable(_))));

        scheduler.fail_snapshot(false);
        scheduler.snapshot(&mut into).await?;
        assert_eq!(into.completed, 3);
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
    async fn set_draining_refuses_only_vanished_workers() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let worker = WorkerId::try_new("worker-1")?;
        scheduler.set_draining(&worker, true).await?;

        scheduler.vanish(worker.clone());
        let refused = scheduler.set_draining(&worker, true).await;
        assert!(matches!(
            refused,
            Err(PortError::NotFound(Subject::Worker(id))) if id == worker
        ));
        Ok(())
    }

    #[tokio::test]
    async fn set_draining_records_each_call_with_its_flag() -> TestResult {
        let scheduler = FakeScheduler::new()?;
        let first = WorkerId::try_new("worker-1")?;
        let second = WorkerId::try_new("worker-2")?;
        scheduler.set_draining(&first, true).await?;
        scheduler.set_draining(&second, true).await?;
        scheduler.set_draining(&first, false).await?;
        let expected = vec![
            (first.clone(), true),
            (second.clone(), true),
            (first.clone(), false),
        ];
        assert_eq!(scheduler.draining_calls(), expected);

        scheduler.vanish(second.clone());
        let refused = scheduler.set_draining(&second, false).await;
        assert!(matches!(
            refused,
            Err(PortError::NotFound(Subject::Worker(id))) if id == second
        ));
        assert_eq!(scheduler.draining_calls(), expected);
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
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> Result<(), PortError> {
            Ok(())
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

    #[test]
    fn finish_controller_makes_run_controller_return_at_once() -> TestResult {
        let fixture = WorkerFixture::new()?;
        let stores = gang_spi::RenderedStoreSpec(serde_json::Value::Null);
        let catalog = gang_types::FleetCatalog::try_new(vec![fixture.properties.clone()])?;
        let spec = ControllerSpec {
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            client_action_timeout: std::time::Duration::from_mins(1),
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
        let spec = ControllerSpec {
            content_store: &stores,
            action_store: &stores,
            fleets: &catalog,
            client_action_timeout: std::time::Duration::from_mins(1),
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
