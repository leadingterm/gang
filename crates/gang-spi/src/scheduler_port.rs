//! Scheduler: the embedded remote execution scheduler, its workers, and what it holds.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ::bytes::Bytes as BodyBytes;
use futures::stream::BoxStream;
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

use gang_clock::Timestamp;
use gang_types::{
    Bytes, FleetCatalog, FleetName, FleetProperties, FleetSet, InstanceId, Resources, Url, WorkerId,
};

use crate::{BoxedError, Peer, PortError, RenderedStoreSpec, port};

/// The remote execution scheduler Gang scales workers for. The scheduler runs inside the `gang`
/// process: `gang server` runs the controller and `gang worker` runs a worker, each through the
/// future a method returns.
#[port]
pub trait SchedulerPort: Send + Sync {
    /// Overwrites `into` with the connected workers and the queued actions. A worker Gang does
    /// not manage is left out. Workers have unique ids and are sorted by `WorkerId` ascending.
    /// The buffers of `into` are reused. On `Err`, `into` is unspecified; callers skip the pass.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError`] when the
    /// scheduler cannot be queried.
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError>;

    /// Overwrites `into` with the actions the scheduler holds: the executing ones, ordered by
    /// operation, and the pending ones, highest priority first, then oldest. Each list is cut
    /// to `limits`; the `truncated_*` counts of `into` say how many entries were left out. The
    /// buffers of `into` are reused. On `Err`, `into` is unspecified.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError`] when the
    /// scheduler cannot be queried.
    async fn actions(&self, limits: ListingLimits, into: &mut ActionsView)
    -> Result<(), PortError>;

    /// Stops (`true`) or resumes (`false`) assigning new actions to a worker.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Worker`](crate::Subject::Worker) when the worker
    /// is not connected; [`PortError::Unavailable`] while the controller is not serving.
    async fn set_draining(&self, worker: &WorkerId, draining: bool) -> Result<(), PortError>;

    /// The URL the controller's worker listener is dialed on.
    fn endpoints(&self) -> ControllerEndpoints;

    /// Builds the controller's stores, scheduler, and services, and serves the connections it is
    /// handed on `connections`: the client services and `api` on every public connection, which
    /// may speak HTTP/1.1 or HTTP/2 (auto-detected), and the worker services on every worker
    /// connection, which speak HTTP/2 only. Each request first passes its listener's gate. It
    /// runs until both streams have ended or the returned future is dropped; dropping it closes
    /// every connection it serves. Listening, TLS, and authentication belong to the caller.
    /// It sends `ready` once the controller is built and before it serves the first connection;
    /// a run that fails before that drops `ready` unsent.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid controller;
    /// [`PortError`] when a store cannot be started or the controller fails.
    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError>;

    /// Builds one worker, connects it to the controller's worker listener, and runs it until
    /// `stop` completes, the worker fails, or a [`Slots::SingleUse`] worker has finished its one
    /// action and uploaded the result.
    ///
    /// Once `stop` completes the worker takes no new action, ends every action it runs (the
    /// action's whole process group) and removes their directories, then returns `Ok(())`, so
    /// the next worker may use the same `work_directory` at once. Dropping the returned future
    /// instead abandons the running actions; callers stop a worker through `stop`.
    ///
    /// `tls` is how the worker secures its connection to the worker endpoint of
    /// [`SchedulerPort::endpoints`]: [`WorkerTls::scheme`] must be that endpoint's URL scheme.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid worker, including a `tls` whose
    /// scheme is not the endpoint's; [`PortError`] when the worker cannot be started or fails.
    async fn run_worker(
        &self,
        spec: &WorkerSpec,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError>;
}

/// What the scheduler holds at one moment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchedulerSnapshot {
    /// Actions that reached a terminal result on a worker since the controller started. Monotonic.
    pub completed: u64,
    /// The connected workers, with unique ids sorted by `WorkerId` ascending.
    pub workers: Vec<WorkerState>,
    /// The queued actions, highest priority first, then oldest.
    pub queued: Vec<QueuedDemand>,
}

/// A connected worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerState {
    /// The scheduler's id for the worker; it starts with the id of the instance that hosts it.
    pub id: WorkerId,
    /// The fleet the worker registered.
    pub fleet: FleetName,
    /// Actions it is running.
    pub running_actions: u32,
    /// Actions it accepts at once; `None` when the worker accepts any number.
    pub max_inflight_actions: Option<NonZeroU32>,
    /// Whether the scheduler has stopped assigning it new actions.
    pub draining: bool,
    /// Whether the scheduler has paused the worker. Not the same as draining, which a drain
    /// request sets.
    pub paused: bool,
    /// When the worker last spoke to the scheduler.
    pub last_seen: Timestamp,
}

/// An action waiting for a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedDemand {
    /// The fleets that accept the action, as indices into [`ControllerSpec::fleets`], in
    /// preference order; empty when none accepts it.
    pub candidates: FleetSet,
    /// The resources the action requests, each dimension zero when the action does not request it.
    pub resources: Resources,
    /// When it was queued.
    pub queued_since: Timestamp,
    /// Scheduling priority; higher runs first.
    pub priority: i32,
}

/// Limits on how many actions [`SchedulerPort::actions`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListingLimits {
    /// The most executing actions to return.
    pub running: usize,
    /// The most pending actions to return.
    pub queued: usize,
}

/// The actions the scheduler holds, as [`SchedulerPort::actions`] reports them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActionsView {
    /// The running actions, ordered by operation.
    pub running: Vec<RunningActionView>,
    /// The queued actions, highest priority first, then oldest.
    pub queued: Vec<QueuedActionView>,
    /// The running actions left out by [`ListingLimits::running`].
    pub truncated_running: u64,
    /// The queued actions left out by [`ListingLimits::queued`].
    pub truncated_queued: u64,
}

/// The part of an action that a running and a queued action share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionInfo {
    /// The operation name the scheduler assigned.
    pub op: String,
    /// The action digest, `<hash>/<size>`.
    pub digest: String,
    /// The Bazel request target; empty when the request carries none.
    pub target: String,
    /// The Bazel request mnemonic; empty when the request carries none.
    pub mnemonic: String,
    /// Scheduling priority; higher runs first.
    pub priority: i32,
}

/// A running action reported by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningActionView {
    /// The metadata the action shares with a queued action.
    pub info: ActionInfo,
    /// The worker executing the action.
    pub worker: WorkerId,
    /// When the worker started the action.
    pub started: Timestamp,
}

/// An action waiting for a worker, with the metadata [`SchedulerPort::actions`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedActionView {
    /// The metadata the action shares with a running action.
    pub info: ActionInfo,
    /// When it was queued.
    pub queued_since: Timestamp,
    /// The fleets that accept the action; empty when none accepts it.
    pub candidates: FleetSet,
}

/// A byte stream the controller serves one HTTP/2 connection on: an accepted socket, a TLS
/// stream, or an in-memory pipe.
pub trait Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Connection for T {}

/// One boxed [`Connection`].
pub type BoxedConnection = Box<dyn Connection>;

/// An accepted connection and what the transport verified about it.
pub struct Accepted {
    /// The connection to serve.
    pub connection: BoxedConnection,
    /// What the transport verified about the connection, such as the client certificate.
    pub peer: Peer,
}

/// The authentication gate of a listener. The controller applies it to every request before any
/// service sees it, so the scheduler stays free of TLS, tokens and revocation.
pub trait RequestGate: Clone + Send + Sync + 'static {
    /// Decides whether a request may proceed. The gate may add values to `parts.extensions`,
    /// such as the caller's identity, for the services behind it.
    ///
    /// # Errors
    ///
    /// [`PortError::Rejected`] with [`Rejection::InvalidProof`](crate::Rejection::InvalidProof)
    /// when the caller is not authenticated, with
    /// [`Rejection::PolicyDenied`](crate::Rejection::PolicyDenied) when it is not allowed, with
    /// [`Rejection::PrincipalRevoked`](crate::Rejection::PrincipalRevoked) when the caller's
    /// principal is revoked;
    /// [`PortError::Unavailable`] when a dependency of the gate is down.
    fn admit(
        &self,
        peer: &Peer,
        parts: &mut http::request::Parts,
    ) -> impl Future<Output = Result<(), PortError>> + Send;
}

/// The request body of an [`ApiService`].
pub type ApiRequestBody = http_body_util::combinators::UnsyncBoxBody<BodyBytes, BoxedError>;

/// The response body of an [`ApiService`]: boxed so an answer can be a stream.
pub type ApiResponseBody = http_body_util::combinators::UnsyncBoxBody<BodyBytes, BoxedError>;

/// A complete in-memory body.
#[must_use]
pub fn full_body(bytes: impl Into<BodyBytes>) -> ApiResponseBody {
    http_body_util::Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// The Gang API (`/v1/*`) that the controller serves beside its own services, for every request
/// no controller service handles. It runs after the listener's gate and finds the caller's
/// identity in the request extensions.
pub trait ApiService: Clone + Send + Sync + 'static {
    /// Answers one request.
    fn call(
        &self,
        request: http::Request<ApiRequestBody>,
    ) -> impl Future<Output = http::Response<ApiResponseBody>> + Send;
}

/// One listener's connections and the gate applied to its requests.
pub struct Listening<G> {
    /// The connections the caller accepted. A stream ending means no more connections.
    pub connections: BoxStream<'static, Accepted>,
    /// The gate every request on these connections passes.
    pub gate: G,
}

/// What the controller serves: the public listener, which carries the client services and the
/// Gang API, and the private worker listener. Each listener has its own gate type, so the two
/// gates may differ.
pub struct ControllerConnections<P, W, A> {
    /// The public listener: Bazel and `gang` clients. Requests no client service handles go to
    /// `api`.
    pub public: Listening<P>,
    /// The worker listener: workers.
    pub worker: Listening<W>,
    /// The Gang API.
    pub api: A,
}

/// The signal [`SchedulerPort::run_controller`] sends once its controller is built and about to
/// serve, so every startup check of the controller has passed. Dropped unsent, it tells the
/// waiter that the controller never became ready.
#[derive(Debug)]
pub struct ControllerReady(oneshot::Sender<()>);

impl ControllerReady {
    /// A signal and the receiver that resolves once the signal is sent, or fails once it is
    /// dropped unsent.
    #[must_use]
    pub fn channel() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (Self(sender), receiver)
    }

    /// Tells the waiter that the controller is ready.
    pub fn signal(self) {
        // A waiter that has gone needs no signal.
        let _ = self.0.send(());
    }
}

/// How a worker secures its connection to the controller's worker listener.
#[derive(Debug, Clone, Copy)]
pub enum WorkerTls<'a> {
    /// No TLS: the worker endpoint's scheme is `grpc`.
    Plain,
    /// TLS that authenticates the server only; the worker presents no certificate.
    ServerOnly {
        /// The CA that the controller's certificate must chain to; `None` trusts the system
        /// roots, as for a TLS-terminating proxy with a public certificate.
        server_ca: Option<&'a Path>,
    },
    /// Mutual TLS: the worker also presents its own certificate.
    Mutual(ClientTls<'a>),
}

impl WorkerTls<'_> {
    /// The URL scheme of a worker endpoint reached this way: `grpc` for [`WorkerTls::Plain`],
    /// `grpcs` otherwise.
    #[must_use]
    pub const fn scheme(&self) -> &'static str {
        match self {
            Self::Plain => "grpc",
            Self::ServerOnly { .. } | Self::Mutual(_) => "grpcs",
        }
    }
}

/// The certificate material a worker presents on a mutual TLS connection to the controller.
#[derive(Debug, Clone, Copy)]
pub struct ClientTls<'a> {
    /// The worker's certificate chain.
    pub certificate: &'a Path,
    /// The worker's private key.
    pub private_key: &'a Path,
    /// The CA that the controller's certificate must chain to.
    pub server_ca: &'a Path,
}

/// Inputs of the controller.
#[derive(Debug, Clone, Copy)]
pub struct ControllerSpec<'a> {
    /// The content-addressable store.
    pub content_store: &'a RenderedStoreSpec,
    /// The action store.
    pub action_store: &'a RenderedStoreSpec,
    /// Every fleet's properties in preference order: what the scheduler declares and how
    /// queued actions are matched.
    pub fleets: &'a FleetCatalog,
    /// The timeout the controller applies to client actions.
    pub client_action_timeout: Duration,
}

/// The dialable URL of the controller's worker listener, with its host name, as the plugin's
/// configuration names it (`worker_endpoint`). Workers dial it. `gang-mtls` takes the host name
/// for the worker server certificate's subject alternative name from it. The caller listens
/// where it leads and secures the connections it hands to the controller; a `grpcs` scheme says
/// the listener uses TLS. The public endpoint that clients dial is the caller's own setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerEndpoints {
    /// The URL that workers dial.
    pub worker: Url,
}

/// How a worker reaches the stores of [`ControllerSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreAccess {
    /// The workers open the store themselves, as with an object store that every node can reach.
    Direct,
    /// Only the controller can open the store, as with a local directory; workers reach it
    /// through the controller's worker listener.
    ViaController,
}

/// How many actions a worker takes at once, and whether it serves one action only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slots {
    /// The worker serves one action, then finishes.
    SingleUse,
    /// The worker serves this many actions at once.
    LongLived(NonZeroU32),
}

impl Slots {
    /// The number of actions the worker takes at once: one for [`Slots::SingleUse`].
    #[must_use]
    pub const fn count(self) -> NonZeroU32 {
        match self {
            Self::SingleUse => NonZeroU32::MIN,
            Self::LongLived(count) => count,
        }
    }

    /// Whether the worker serves one action and then finishes.
    #[must_use]
    pub const fn is_single_use(self) -> bool {
        matches!(self, Self::SingleUse)
    }
}

/// Inputs of one worker.
#[derive(Debug, Clone)]
pub struct WorkerSpec {
    /// The instance the worker runs on; the plugin names the worker with
    /// [`InstanceId::worker_name`].
    pub instance: InstanceId,
    /// The properties the worker registers: its fleet's.
    pub properties: FleetProperties,
    /// Whether it serves many actions or exactly one, and how many at once.
    pub slots: Slots,
    /// Where actions are built: one directory per worker. The worker empties it at start.
    pub work_directory: PathBuf,
    /// The worker-local fast content-addressable cache directory, under the mounted scratch
    /// volume; one per worker.
    pub fast_cas_directory: PathBuf,
    /// The most the fast cache may hold before it evicts; derived from the scratch volume's
    /// capacity.
    pub fast_cas_budget: Bytes,
    /// The content-addressable store.
    pub content_store: RenderedStoreSpec,
    /// The action store.
    pub action_store: RenderedStoreSpec,
    /// How the worker reaches the two stores above.
    pub store_access: StoreAccess,
    /// The `gang-exec` link to the `gang` binary, which runs as the worker's action
    /// entrypoint (`gang exec`). The sandbox file `gang exec` reads is its sibling
    /// (`gang_config::SandboxFile::beside`); the directory is the worker's, never writable by
    /// actions.
    pub entrypoint: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use http_body_util::BodyExt;

    use super::{BoxedError, Slots, full_body};

    #[test]
    fn full_body_collects_to_its_bytes() -> Result<(), BoxedError> {
        let mut future = pin!(full_body("answer").collect());
        let mut task_context = Context::from_waker(Waker::noop());
        let Poll::Ready(collected) = future.as_mut().poll(&mut task_context) else {
            return Err("a full body is ready on its first poll".into());
        };
        assert_eq!(collected?.to_bytes(), "answer");
        Ok(())
    }

    #[test]
    fn slots_count_is_one_for_single_use() {
        assert_eq!(Slots::SingleUse.count(), NonZeroU32::MIN);
    }

    #[test]
    fn slots_count_is_the_long_lived_slot_count() -> Result<(), BoxedError> {
        let four = NonZeroU32::new(4).ok_or("four is non-zero")?;
        assert_eq!(Slots::LongLived(four).count(), four);
        Ok(())
    }

    #[test]
    fn only_single_use_is_single_use() -> Result<(), BoxedError> {
        let four = NonZeroU32::new(4).ok_or("four is non-zero")?;
        assert!(Slots::SingleUse.is_single_use());
        assert!(!Slots::LongLived(NonZeroU32::MIN).is_single_use());
        assert!(!Slots::LongLived(four).is_single_use());
        Ok(())
    }
}
