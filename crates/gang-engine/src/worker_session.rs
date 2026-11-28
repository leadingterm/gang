//! Worker bidirectional `gRPC` session client over `leadingterm.gang.worker.v1.WorkerService/Connect`.
//!
//! Manages one session per worker generation: establishes the transport channel through a
//! caller-supplied connector (supporting TLS/mTLS reload on reconnect), performs the
//! `Register` -> `Registered` handshake, sends periodic `KeepAlive` frames with store and memory
//! telemetry, forwards inbound controller messages (`Assign`, `Kill`, `SetDraining`, `Rehome`),
//! sends outbound worker messages (`Accept`, `Decline`, `Executed`, `Completed`, `GoingAway`),
//! reconnects with exponential backoff and jitter capped at `connect_timeout`, handles `Rehome`
//! immediately on a fresh session, and stops without reconnecting on revocation or
//! unauthenticated refusals.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::time::Duration;

use futures::stream;
use gang_clock::Clock;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_reapi::leadingterm::gang::worker::v1::controller_message::Kind as ControllerKind;
use gang_reapi::leadingterm::gang::worker::v1::worker_message::Kind as WorkerKind;
use gang_reapi::leadingterm::gang::worker::v1::worker_service_client::WorkerServiceClient;
use gang_spi::{BoxedError, PortFailure, WorkerCacheStats};
use gang_tls::{CertificateRevoked, RevokedAlert, is_certificate_refused};
use gang_types::{
    AttemptId, AttemptIdError, InstanceId, NameError, NonZeroBytes, NonZeroDuration, WorkerId,
};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Default bounded capacity for the inbound [`SessionEvent`] channel.
const DEFAULT_EVENT_BUFFER: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Default bounded capacity for the per-session outbound [`proto::WorkerMessage`] channel.
const DEFAULT_OUTBOUND_BUFFER: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Default initial reconnect backoff before exponential doubling.
const DEFAULT_INITIAL_BACKOFF: NonZeroDuration = NonZeroDuration::from_millis(100);

/// Maximum wait when flushing a final `GoingAway` frame before tearing down the stream.
const GOING_AWAY_FLUSH_CAP: Duration = Duration::from_millis(250);

/// Grace period after `END_STREAM` for HTTP/2 to flush `GoingAway` before dropping `Streaming`.
const GOING_AWAY_WIRE_GRACE: Duration = Duration::from_millis(50);

/// Minimum non-zero `KeepAlive` cadence when `connect_timeout` is tiny.
const MIN_KEEPALIVE: Duration = Duration::from_millis(1);

/// Configuration for [`run_session`].
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Initial `Register` payload sent as the first frame of every session generation.
    pub register: proto::Register,
    /// Timeout for dialing a channel and completing the `Register` -> `Registered` handshake,
    /// and the cap on reconnect backoff.
    pub connect_timeout: NonZeroDuration,
    /// Fallback `KeepAlive` cadence used when `Registered.keepalive_interval` is absent or zero.
    pub keepalive_interval: NonZeroDuration,
    /// Initial reconnect delay before exponential growth with jitter.
    pub initial_backoff: NonZeroDuration,
    /// Maximum encoded and decoded `gRPC` message size for `WorkerServiceClient`.
    pub max_message_size: NonZeroBytes,
    /// Bounded buffer size for inbound [`SessionEvent`] delivery.
    pub event_buffer: NonZeroUsize,
    /// Bounded buffer size for outbound [`proto::WorkerMessage`] delivery.
    pub outbound_buffer: NonZeroUsize,
}

impl SessionConfig {
    /// Builds a [`SessionConfig`] with default `KeepAlive` (`connect_timeout / 2`), backoff, and
    /// channel capacities.
    #[must_use]
    pub fn new(
        register: proto::Register,
        connect_timeout: NonZeroDuration,
        max_message_size: NonZeroBytes,
    ) -> Self {
        let half = (connect_timeout.get() / 2).max(MIN_KEEPALIVE);
        let keepalive_interval = NonZeroDuration::try_new(half).unwrap_or(connect_timeout);
        Self {
            register,
            connect_timeout,
            keepalive_interval,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_message_size,
            event_buffer: DEFAULT_EVENT_BUFFER,
            outbound_buffer: DEFAULT_OUTBOUND_BUFFER,
        }
    }
}

/// An established transport channel to the controller and its optional TLS revocation watch.
#[derive(Clone)]
pub struct ConnectedChannel {
    /// The `tonic` transport channel wrapped by [`WorkerServiceClient`].
    pub channel: Channel,
    /// Optional live TLS revocation signal raised if the controller sends `certificate_revoked`.
    pub revoked: Option<RevokedAlert>,
}

/// Why dialing a fresh controller channel failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The controller refused this worker's certificate or identity permanently; do not reconnect.
    #[error("controller refused worker credentials")]
    Revoked(#[source] BoxedError),
    /// A transient network, DNS, or transport error; reconnect after backoff.
    #[error("transient connection failure: {0}")]
    Transient(#[source] BoxedError),
}

impl ConnectError {
    /// Classifies a transport error as [`Self::Revoked`] when a refused certificate appears in its
    /// error chain, and [`Self::Transient`] otherwise.
    #[must_use]
    pub fn from_transport(error: BoxedError) -> Self {
        if is_certificate_refused(error.as_ref()) {
            Self::Revoked(error)
        } else {
            Self::Transient(error)
        }
    }
}

/// Rebuilds and connects a transport channel to the controller for each worker generation.
///
/// Implementations may reload TLS/mTLS certificates and trust roots on every call so rotated
/// credentials take effect on the next reconnect or rehome.
pub trait SessionConnector: Send {
    /// Opens a fresh [`ConnectedChannel`] to the controller.
    fn connect(&mut self) -> impl Future<Output = Result<ConnectedChannel, ConnectError>> + Send;
}

impl<F, Fut> SessionConnector for F
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<ConnectedChannel, ConnectError>> + Send,
{
    fn connect(&mut self) -> impl Future<Output = Result<ConnectedChannel, ConnectError>> + Send {
        (self)()
    }
}

/// Samples periodic store and memory capacity telemetry for `KeepAlive` messages.
pub trait TelemetrySource: Send + Sync {
    /// Invalidates telemetry from the previous worker session.
    fn start_session(&self) {}
    /// Returns current store measurements and optional observed post-reserve memory capacity.
    fn sample(&self) -> (WorkerCacheStats, Option<u64>);
    /// Returns the latest available CPU sample.
    fn cpu_load(&self) -> Option<proto::WorkerLoad> {
        None
    }
    /// Returns material changes in active attempt usage.
    fn inflight(&self) -> Vec<proto::AttemptUsageUpdate> {
        Vec::new()
    }
}

impl<F> TelemetrySource for F
where
    F: Fn() -> (WorkerCacheStats, Option<u64>) + Send + Sync,
{
    fn sample(&self) -> (WorkerCacheStats, Option<u64>) {
        (self)()
    }
}

/// Errors returned when sending an outbound message on [`SessionOutbound`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionSendError {
    /// The session stream has already closed or rehomed.
    #[error("worker session outbound stream is closed")]
    Closed,
}

/// Outbound sender bound to a single connected session generation.
#[derive(Debug, Clone)]
pub struct SessionOutbound {
    sender: mpsc::Sender<proto::WorkerMessage>,
}

impl SessionOutbound {
    const fn new(sender: mpsc::Sender<proto::WorkerMessage>) -> Self {
        Self { sender }
    }

    /// Sends an `Accept` frame for `attempt`.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn accept(&self, attempt: impl Into<String>) -> Result<(), SessionSendError> {
        self.send(WorkerKind::Accept(proto::Accept {
            attempt: attempt.into(),
        }))
        .await
    }

    /// Sends a `Decline` frame for `attempt` with `reason` and current `free_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn decline(
        &self,
        attempt: impl Into<String>,
        reason: proto::DeclineReason,
        free_bytes: u64,
    ) -> Result<(), SessionSendError> {
        self.send(WorkerKind::Decline(proto::Decline {
            attempt: attempt.into(),
            reason: decline_reason_i32(reason),
            free_bytes,
        }))
        .await
    }

    /// Sends an `Executed` frame reporting process exit and resource usage.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn executed(&self, executed: proto::Executed) -> Result<(), SessionSendError> {
        self.send(WorkerKind::Executed(executed)).await
    }

    /// Sends a `Completed` frame reporting upload and `ActionCache` publication outcome.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn completed(&self, completed: proto::Completed) -> Result<(), SessionSendError> {
        self.send(WorkerKind::Completed(completed)).await
    }

    /// Sends a `GoingAway` frame notifying the controller not to send new assignments.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn going_away(&self) -> Result<(), SessionSendError> {
        self.send(WorkerKind::GoingAway(proto::GoingAway {})).await
    }

    /// Sends an arbitrary [`WorkerKind`] frame on this session stream.
    ///
    /// # Errors
    ///
    /// Returns [`SessionSendError::Closed`] if this session generation has ended.
    pub async fn send(&self, kind: WorkerKind) -> Result<(), SessionSendError> {
        self.sender
            .send(proto::WorkerMessage { kind: Some(kind) })
            .await
            .map_err(|_| SessionSendError::Closed)
    }
}

/// Inbound lifecycle and controller events emitted by [`run_session`].
#[derive(Debug)]
pub enum SessionEvent {
    /// A new session generation completed `Register` -> `Registered`.
    Registered {
        /// Controller-minted worker identifier for this session.
        worker_id: WorkerId,
        /// Fresh local generation identifier generated for this connection attempt.
        generation_id: WorkerId,
        /// Negotiated `KeepAlive` cadence for this session.
        keepalive_interval: Duration,
        /// Outbound sender bound to this session generation.
        outbound: SessionOutbound,
    },
    /// The controller assigned an action attempt to execute.
    Assign(Box<proto::Assign>),
    /// The controller requested termination of an in-flight attempt.
    Kill(proto::Kill),
    /// The controller updated this worker's draining flag.
    SetDraining(bool),
    /// The controller ended this session with `Rehome`; a fresh session will be dialed.
    Rehome,
    /// The active session ended before reconnect or shutdown.
    Disconnected {
        /// Diagnostic reason if the session ended with an error.
        reason: Option<String>,
        /// Sent only after all attempts from this generation have been cancelled and drained.
        ack: oneshot::Sender<()>,
    },
}

/// Fatal errors that terminate [`run_session`] without reconnecting.
#[derive(Debug, thiserror::Error, PortFailure)]
pub enum SessionError {
    /// The configured `Register.instance` is not a valid [`InstanceId`].
    #[error("invalid worker instance id")]
    #[failure(config)]
    InvalidInstance(#[source] NameError),
    /// System entropy could not be read to mint a fresh session [`WorkerId`].
    #[error("cannot generate fresh worker id")]
    Entropy(#[source] AttemptIdError),
    /// The controller sent a TLS `certificate_revoked` alert.
    #[error("the controller refused the worker certificate")]
    CertificateRevoked(#[source] CertificateRevoked),
    /// The connector reported permanently refused credentials.
    #[error("worker credentials were refused")]
    Revoked(#[source] BoxedError),
    /// The controller rejected the worker session with `PermissionDenied` or `Unauthenticated`.
    #[error("controller rejected worker session: {0}")]
    Rejected(#[source] Status),
    /// The caller dropped the [`SessionEvent`] receiver before `stop` completed.
    #[error("worker session event receiver closed")]
    EventsClosed,
}

/// Generates a fresh `{instance}-{uuidv4}` [`WorkerId`] for a new session generation.
///
/// # Errors
///
/// Returns [`SessionError::Entropy`] if OS randomness cannot be read, or
/// [`SessionError::InvalidInstance`] if the formatted identifier is invalid.
pub fn generate_worker_id(instance: &InstanceId) -> Result<WorkerId, SessionError> {
    let uuid = AttemptId::generate().map_err(SessionError::Entropy)?;
    WorkerId::try_new(format!("{}{uuid}", instance.worker_name()))
        .map_err(SessionError::InvalidInstance)
}

/// Computes exponential reconnect backoff with equal jitter in `[ceiling / 2, ceiling]`, capped
/// at `cap`.
#[must_use]
pub fn backoff_delay(attempt: u32, initial: Duration, cap: Duration, entropy: u64) -> Duration {
    let cap = cap.max(Duration::from_millis(1));
    let initial = initial.min(cap);
    let shift = attempt.min(30);
    let factor = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
    let ceiling = initial.saturating_mul(factor).min(cap);
    let half = ceiling / 2;
    let span = ceiling.saturating_sub(half);
    let span_nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
    let jitter = if span_nanos == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(entropy % (span_nanos + 1))
    };
    half.saturating_add(jitter).min(cap)
}

/// Converts [`WorkerCacheStats`] to its `worker.v1` protobuf representation.
#[must_use]
pub const fn cache_stats_to_proto(stats: WorkerCacheStats) -> proto::CacheStats {
    proto::CacheStats {
        local_read_requests: stats.local_read_requests,
        local_read_bytes: stats.local_read_bytes,
        slow_read_requests: stats.slow_read_requests,
        slow_read_bytes: stats.slow_read_bytes,
        evictions: stats.evictions,
        evicted_bytes: stats.evicted_bytes,
        size_bytes: stats.size_bytes,
        budget_bytes: stats.budget_bytes,
    }
}

/// Runs the worker session reconnect and streaming loop until `stop` completes or a fatal error
/// (such as certificate/principal revocation) occurs.
///
/// Each connection generation invokes `connector` to build a fresh channel, generates a fresh
/// [`WorkerId`], sends `Register`, awaits `Registered`, and runs periodic `KeepAlive` and message
/// forwarding until disconnect, `Rehome`, `stop`, or revocation.
///
/// # Errors
///
/// Returns [`SessionError::InvalidInstance`] if `config.register.instance` is invalid,
/// [`SessionError::CertificateRevoked`] / [`SessionError::Revoked`] / [`SessionError::Rejected`]
/// when the controller refuses credentials (never reconnecting), [`SessionError::Entropy`] if a
/// fresh [`WorkerId`] cannot be generated, or [`SessionError::EventsClosed`] if `events` closes.
pub async fn run_session<C, T, K>(
    config: &SessionConfig,
    connector: &mut C,
    telemetry: &T,
    clock: &K,
    events: mpsc::Sender<SessionEvent>,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), SessionError>
where
    C: SessionConnector,
    T: TelemetrySource,
    K: Clock,
{
    let instance =
        InstanceId::try_new(&config.register.instance).map_err(SessionError::InvalidInstance)?;
    let mut stop = pin!(stop);
    let mut attempt: u32 = 0;

    loop {
        if futures::poll!(stop.as_mut()).is_ready() {
            return Ok(());
        }

        let generation_id = generate_worker_id(&instance)?;
        let Some(connected) = dial_with_timeout(config, connector, clock, stop.as_mut()).await?
        else {
            if let Some(outcome) = sleep_backoff(config, attempt, clock, stop.as_mut()).await {
                return outcome;
            }
            attempt = attempt.saturating_add(1);
            continue;
        };

        let outcome = run_one_session(
            config,
            connected,
            &generation_id,
            telemetry,
            clock,
            &events,
            stop.as_mut(),
        )
        .await?;

        match outcome {
            LoopControl::Stopped => return Ok(()),
            LoopControl::Rehome => {
                attempt = 0;
            }
            LoopControl::Reconnect | LoopControl::HandshakeFailed => {
                if let Some(outcome) = sleep_backoff(config, attempt, clock, stop.as_mut()).await {
                    return outcome;
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

enum LoopControl {
    Stopped,
    Rehome,
    Reconnect,
    HandshakeFailed,
}

async fn dial_with_timeout<C: SessionConnector, K: Clock>(
    config: &SessionConfig,
    connector: &mut C,
    clock: &K,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> Result<Option<ConnectedChannel>, SessionError> {
    tokio::select! {
        biased;
        () = &mut stop => Ok(None),
        dialed = clock.timeout(config.connect_timeout.get(), connector.connect()) => {
            match dialed {
                Ok(Ok(connected)) => Ok(Some(connected)),
                Ok(Err(ConnectError::Revoked(error))) => Err(SessionError::Revoked(error)),
                Ok(Err(ConnectError::Transient(error))) => {
                    tracing::warn!(error = %gang_types::error_chain(error.as_ref()), "worker dial failed");
                    Ok(None)
                }
                Err(_elapsed) => {
                    tracing::warn!(timeout = ?config.connect_timeout.get(), "worker dial timed out");
                    Ok(None)
                }
            }
        }
    }
}

async fn run_one_session<T: TelemetrySource, K: Clock>(
    config: &SessionConfig,
    connected: ConnectedChannel,
    generation_id: &WorkerId,
    telemetry: &T,
    clock: &K,
    events: &mpsc::Sender<SessionEvent>,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> Result<LoopControl, SessionError> {
    let (out_tx, close_tx, flushed_rx, outbound_stream) = open_outbound(config);
    let max_size = config.max_message_size.get().as_usize();
    let mut client = WorkerServiceClient::new(connected.channel)
        .max_decoding_message_size(max_size)
        .max_encoding_message_size(max_size);

    let handshake = clock.timeout(
        config.connect_timeout.get(),
        connect_and_register(
            &mut client,
            outbound_stream,
            config.keepalive_interval.get(),
        ),
    );
    let (mut inbound, worker_id, keepalive_interval) = tokio::select! {
        biased;
        revoked = wait_revoked(connected.revoked.as_ref()) => {
            return Err(SessionError::CertificateRevoked(revoked));
        }
        () = &mut stop => return Ok(LoopControl::Stopped),
        result = handshake => match result {
            Ok(Ok(registered)) => registered,
            Ok(Err(HandshakeError::Terminal(err))) => return Err(err),
            Ok(Err(HandshakeError::Transient(reason))) => {
                tracing::warn!(%generation_id, %reason, "worker registration failed");
                return Ok(LoopControl::HandshakeFailed);
            }
            Err(_elapsed) => {
                tracing::warn!(%generation_id, timeout = ?config.connect_timeout.get(), "worker registration timed out");
                return Ok(LoopControl::HandshakeFailed);
            }
        }
    };

    telemetry.start_session();
    if events
        .send(SessionEvent::Registered {
            worker_id: worker_id.clone(),
            generation_id: generation_id.clone(),
            keepalive_interval,
            outbound: SessionOutbound::new(out_tx.clone()),
        })
        .await
        .is_err()
    {
        finish_outbound(
            out_tx,
            close_tx,
            flushed_rx,
            &mut inbound,
            true,
            clock,
            config.connect_timeout.get(),
        )
        .await;
        return Err(SessionError::EventsClosed);
    }

    let active_outcome = drive_active_session(
        &worker_id,
        keepalive_interval,
        connected.revoked.as_ref(),
        &mut inbound,
        &out_tx,
        telemetry,
        clock,
        events,
        stop.as_mut(),
    )
    .await;

    finalize_active_session(
        active_outcome,
        out_tx,
        close_tx,
        flushed_rx,
        inbound,
        clock,
        config.connect_timeout.get(),
        events,
        stop.as_mut(),
    )
    .await
}

fn open_outbound(
    config: &SessionConfig,
) -> (
    mpsc::Sender<proto::WorkerMessage>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
    impl futures::Stream<Item = proto::WorkerMessage> + Send + 'static,
) {
    let (out_tx, out_rx) = mpsc::channel::<proto::WorkerMessage>(config.outbound_buffer.get());
    let (close_tx, close_rx) = oneshot::channel::<()>();
    let (flushed_tx, flushed_rx) = oneshot::channel::<()>();
    let _ = out_tx.try_send(proto::WorkerMessage {
        kind: Some(WorkerKind::Register(config.register.clone())),
    });
    let outbound_stream = stream::unfold(
        (out_rx, Some(close_rx), Some(flushed_tx)),
        |(mut rx, mut close_rx, mut flushed)| async move {
            loop {
                if let Some(ref mut close) = close_rx {
                    tokio::select! {
                        biased;
                        msg = rx.recv() => {
                            if let Some(msg) = msg {
                                return Some((msg, (rx, close_rx, flushed)));
                            }
                            if let Some(done) = flushed.take() {
                                let _ = done.send(());
                            }
                            return None;
                        }
                        _ = close => {
                            rx.close();
                            close_rx = None;
                        }
                    }
                } else if let Some(msg) = rx.recv().await {
                    return Some((msg, (rx, None, flushed)));
                } else {
                    if let Some(done) = flushed.take() {
                        let _ = done.send(());
                    }
                    return None;
                }
            }
        },
    );
    (out_tx, close_tx, flushed_rx, outbound_stream)
}

#[allow(clippy::too_many_arguments)]
async fn finalize_active_session<K: Clock>(
    active_outcome: ActiveOutcome,
    out_tx: mpsc::Sender<proto::WorkerMessage>,
    close_tx: oneshot::Sender<()>,
    flushed_rx: oneshot::Receiver<()>,
    mut inbound: tonic::Streaming<proto::ControllerMessage>,
    clock: &K,
    connect_timeout: Duration,
    events: &mpsc::Sender<SessionEvent>,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> Result<LoopControl, SessionError> {
    match active_outcome {
        ActiveOutcome::Fatal(err) => {
            let _ = close_tx.send(());
            drop(out_tx);
            Err(err)
        }
        ActiveOutcome::Stopped => {
            finish_outbound(
                out_tx,
                close_tx,
                flushed_rx,
                &mut inbound,
                true,
                clock,
                connect_timeout,
            )
            .await;
            Ok(LoopControl::Stopped)
        }
        ActiveOutcome::Rehome => {
            finish_outbound(
                out_tx,
                close_tx,
                flushed_rx,
                &mut inbound,
                false,
                clock,
                connect_timeout,
            )
            .await;
            drop(inbound);
            if !wait_for_attempt_drain(events, None, stop.as_mut()).await? {
                return Ok(LoopControl::Stopped);
            }
            Ok(LoopControl::Rehome)
        }
        ActiveOutcome::Disconnected(reason) => {
            let _ = close_tx.send(());
            drop(out_tx);
            let _ = tokio::time::timeout(GOING_AWAY_WIRE_GRACE, flushed_rx).await;
            drop(inbound);
            if !wait_for_attempt_drain(events, reason, stop.as_mut()).await? {
                return Ok(LoopControl::Stopped);
            }
            Ok(LoopControl::Reconnect)
        }
    }
}

async fn wait_for_attempt_drain(
    events: &mpsc::Sender<SessionEvent>,
    reason: Option<String>,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> Result<bool, SessionError> {
    let (ack, drained) = oneshot::channel();
    tokio::select! {
        biased;
        () = &mut stop => return Ok(false),
        sent = events.send(SessionEvent::Disconnected { reason, ack }) => sent.map_err(|_| SessionError::EventsClosed)?,
    }
    tokio::select! {
        biased;
        () = &mut stop => Ok(false),
        result = drained => result.map(|()| true).map_err(|_| SessionError::EventsClosed),
    }
}

enum HandshakeError {
    Terminal(SessionError),
    Transient(String),
}

async fn connect_and_register(
    client: &mut WorkerServiceClient<Channel>,
    outbound_stream: impl futures::Stream<Item = proto::WorkerMessage> + Send + 'static,
    default_keepalive: Duration,
) -> Result<
    (
        tonic::Streaming<proto::ControllerMessage>,
        WorkerId,
        Duration,
    ),
    HandshakeError,
> {
    let response = client
        .connect(outbound_stream)
        .await
        .map_err(classify_status)?;
    let mut inbound = response.into_inner();
    let first = inbound.message().await.map_err(classify_status)?;
    let Some(proto::ControllerMessage {
        kind: Some(ControllerKind::Registered(registered)),
    }) = first
    else {
        return Err(HandshakeError::Transient(
            "expected Registered as first controller frame".to_owned(),
        ));
    };
    let worker_id = WorkerId::try_new(&registered.worker_id)
        .map_err(|err| HandshakeError::Transient(format!("invalid worker_id: {err}")))?;
    let keepalive = effective_keepalive(registered.keepalive_interval.as_ref(), default_keepalive);
    Ok((inbound, worker_id, keepalive))
}

fn classify_status(status: Status) -> HandshakeError {
    if is_terminal_status(&status) {
        HandshakeError::Terminal(SessionError::Rejected(status))
    } else {
        HandshakeError::Transient(status.to_string())
    }
}

enum ActiveOutcome {
    Stopped,
    Rehome,
    Disconnected(Option<String>),
    Fatal(SessionError),
}

#[allow(clippy::too_many_arguments)]
async fn drive_active_session<T: TelemetrySource, K: Clock>(
    worker_id: &WorkerId,
    keepalive_interval: Duration,
    revoked: Option<&RevokedAlert>,
    inbound: &mut tonic::Streaming<proto::ControllerMessage>,
    out_tx: &mpsc::Sender<proto::WorkerMessage>,
    telemetry: &T,
    clock: &K,
    events: &mpsc::Sender<SessionEvent>,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> ActiveOutcome {
    let mut next_keepalive = pin!(clock.sleep(keepalive_interval));

    loop {
        tokio::select! {
            biased;
            cert_revoked = wait_revoked(revoked) => {
                return ActiveOutcome::Fatal(SessionError::CertificateRevoked(cert_revoked));
            }
            () = &mut stop => {
                return ActiveOutcome::Stopped;
            }
            () = &mut next_keepalive => {
                let (cache, memory_capacity) = telemetry.sample();
                let msg = proto::WorkerMessage {
                    kind: Some(WorkerKind::KeepAlive(proto::KeepAlive {
                        cache: Some(cache_stats_to_proto(cache)),
                        memory_capacity,
                        load: telemetry.cpu_load(),
                        inflight: telemetry.inflight(),
                    })),
                };
                if out_tx.send(msg).await.is_err() {
                    return ActiveOutcome::Disconnected(Some("outbound stream closed".to_owned()));
                }
                next_keepalive.set(clock.sleep(keepalive_interval));
            }
            frame = inbound.message() => match frame {
                Ok(Some(proto::ControllerMessage { kind: Some(kind) })) => {
                    if let Some(outcome) = handle_controller_kind(worker_id, kind, events).await {
                        return outcome;
                    }
                }
                Ok(Some(proto::ControllerMessage { kind: None })) => {
                    tracing::warn!(%worker_id, "ignoring empty ControllerMessage frame");
                }
                Ok(None) => return ActiveOutcome::Disconnected(None),
                Err(status) => {
                    if is_terminal_status(&status) {
                        return ActiveOutcome::Fatal(SessionError::Rejected(status));
                    }
                    return ActiveOutcome::Disconnected(Some(status.to_string()));
                }
            }
        }
    }
}

async fn handle_controller_kind(
    worker_id: &WorkerId,
    kind: ControllerKind,
    events: &mpsc::Sender<SessionEvent>,
) -> Option<ActiveOutcome> {
    let event = match kind {
        ControllerKind::Assign(assign) => SessionEvent::Assign(Box::new(assign)),
        ControllerKind::Kill(kill) => SessionEvent::Kill(kill),
        ControllerKind::SetDraining(msg) => SessionEvent::SetDraining(msg.draining),
        ControllerKind::Rehome(_) => {
            return Some(match events.send(SessionEvent::Rehome).await {
                Ok(()) => ActiveOutcome::Rehome,
                Err(_) => ActiveOutcome::Fatal(SessionError::EventsClosed),
            });
        }
        ControllerKind::Registered(_) => {
            tracing::warn!(
                %worker_id,
                "ignoring unexpected Registered frame on active session"
            );
            return None;
        }
    };
    if events.send(event).await.is_err() {
        return Some(ActiveOutcome::Fatal(SessionError::EventsClosed));
    }
    None
}

async fn finish_outbound<K: Clock>(
    out_tx: mpsc::Sender<proto::WorkerMessage>,
    close_tx: oneshot::Sender<()>,
    flushed_rx: oneshot::Receiver<()>,
    inbound: &mut tonic::Streaming<proto::ControllerMessage>,
    send_going_away: bool,
    clock: &K,
    connect_timeout: Duration,
) {
    if send_going_away {
        let _ = out_tx
            .send(proto::WorkerMessage {
                kind: Some(WorkerKind::GoingAway(proto::GoingAway {})),
            })
            .await;
    }
    let _ = close_tx.send(());
    drop(out_tx);
    let bound = connect_timeout.min(GOING_AWAY_FLUSH_CAP);
    let _ = clock.timeout(bound, flushed_rx).await;
    if send_going_away {
        let _ = tokio::time::timeout(GOING_AWAY_WIRE_GRACE, async {
            while matches!(inbound.message().await, Ok(Some(_))) {}
        })
        .await;
    }
}

async fn sleep_backoff<K: Clock>(
    config: &SessionConfig,
    attempt: u32,
    clock: &K,
    mut stop: Pin<&mut impl Future<Output = ()>>,
) -> Option<Result<(), SessionError>> {
    let delay = backoff_delay(
        attempt,
        config.initial_backoff.get(),
        config.connect_timeout.get(),
        random_entropy(),
    );
    tokio::select! {
        biased;
        () = &mut stop => Some(Ok(())),
        () = clock.sleep(delay) => None,
    }
}

async fn wait_revoked(revoked: Option<&RevokedAlert>) -> CertificateRevoked {
    match revoked {
        Some(alert) => alert.raised().await,
        None => std::future::pending().await,
    }
}

fn is_terminal_status(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::PermissionDenied | Code::Unauthenticated
    )
}

fn effective_keepalive(server: Option<&prost_types::Duration>, default: Duration) -> Duration {
    let parsed = server.and_then(|d| {
        if d.seconds < 0 || d.nanos < 0 {
            return None;
        }
        let secs = u64::try_from(d.seconds).ok()?;
        let nanos = u32::try_from(d.nanos).ok()?;
        let duration = Duration::new(secs, nanos);
        (!duration.is_zero()).then_some(duration)
    });
    parsed.unwrap_or(default).max(MIN_KEEPALIVE)
}

const fn decline_reason_i32(reason: proto::DeclineReason) -> i32 {
    match reason {
        proto::DeclineReason::Unspecified => 0,
        proto::DeclineReason::DiskShortage => 1,
        proto::DeclineReason::Draining => 2,
        proto::DeclineReason::CpuPressure => 3,
    }
}

fn random_entropy() -> u64 {
    let mut bytes = [0_u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        return 0;
    }
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use gang_clock::{SystemClock, TestClock, Timestamp};
    use gang_reapi::build::bazel::remote::execution::v2::{ActionResult, Digest};
    use gang_reapi::google::rpc::Status as RpcStatus;
    use gang_reapi::leadingterm::gang::worker::v1::worker_service_server::{
        WorkerService, WorkerServiceServer,
    };
    use gang_spi::PortError;
    use gang_testkit::TestResult;
    use gang_types::Bytes;
    use parking_lot::Mutex;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tonic::transport::{Endpoint, Server};

    use super::*;

    struct MockSession {
        inbound_rx: mpsc::Receiver<proto::WorkerMessage>,
        outbound_tx: mpsc::Sender<Result<proto::ControllerMessage, Status>>,
    }

    #[derive(Clone)]
    struct MockController {
        sessions: mpsc::Sender<MockSession>,
        reject_with: Arc<Mutex<Option<Status>>>,
    }

    impl MockController {
        fn new() -> (Self, mpsc::Receiver<MockSession>) {
            let (sessions, rx) = mpsc::channel(16);
            (
                Self {
                    sessions,
                    reject_with: Arc::new(Mutex::new(None)),
                },
                rx,
            )
        }

        fn set_reject(&self, status: Option<Status>) {
            *self.reject_with.lock() = status;
        }
    }

    type MockStream = Pin<
        Box<dyn futures::Stream<Item = Result<proto::ControllerMessage, Status>> + Send + 'static>,
    >;

    #[tonic::async_trait]
    impl WorkerService for MockController {
        type ConnectStream = MockStream;

        async fn connect(
            &self,
            request: tonic::Request<tonic::Streaming<proto::WorkerMessage>>,
        ) -> Result<tonic::Response<Self::ConnectStream>, Status> {
            if let Some(status) = self.reject_with.lock().clone() {
                return Err(status);
            }
            let mut incoming = request.into_inner();
            let (in_tx, in_rx) = mpsc::channel(32);
            tokio::spawn(async move {
                while let Ok(Some(msg)) = incoming.message().await {
                    if in_tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
            let (out_tx, out_rx) = mpsc::channel(32);
            self.sessions
                .send(MockSession {
                    inbound_rx: in_rx,
                    outbound_tx: out_tx,
                })
                .await
                .map_err(|_| Status::unavailable("mock controller closed"))?;
            let out_stream = stream::unfold(out_rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            Ok(tonic::Response::new(Box::pin(out_stream)))
        }
    }

    async fn spawn_mock_server(
        controller: MockController,
    ) -> TestResult<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = stream::unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await.ok()?;
            Some((Ok::<_, std::io::Error>(stream), listener))
        });
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(WorkerServiceServer::new(controller))
                .serve_with_incoming(incoming)
                .await;
        });
        Ok((addr, handle))
    }

    fn test_register() -> proto::Register {
        proto::Register {
            instance: "inst-1".to_owned(),
            fleet: "fleet-a".to_owned(),
            gang_version: "0.3.9".to_owned(),
            properties: HashMap::from([("arch".to_owned(), "x86_64".to_owned())]),
            capacity: Some(proto::Resources {
                cpu_millicores: 4000,
                memory_bytes: 8 << 30,
                disk_bytes: 64 << 30,
            }),
            ceilings: Some(proto::Resources {
                cpu_millicores: 4000,
                memory_bytes: 8 << 30,
                disk_bytes: 64 << 30,
            }),
            max_inflight: 4,
            single_use: false,
            environment: Some(proto::ActionEnvironment {
                identity: "host:test".to_owned(),
            }),
        }
    }

    fn test_config(connect_timeout: NonZeroDuration) -> SessionConfig {
        SessionConfig::new(
            test_register(),
            connect_timeout,
            NonZeroBytes::new(Bytes::mib(4)).expect("4 MiB is non-zero"),
        )
    }

    fn endpoint_connector(
        addr: std::net::SocketAddr,
        calls: Arc<AtomicUsize>,
    ) -> impl SessionConnector {
        let uri = format!("http://{addr}");
        move || {
            let uri = uri.clone();
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let endpoint = Endpoint::from_shared(uri)
                    .map_err(|err| ConnectError::Transient(Box::new(err)))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|err| ConnectError::from_transport(Box::new(err)))?;
                Ok(ConnectedChannel {
                    channel,
                    revoked: None,
                })
            }
        }
    }

    fn registered_msg(worker_id: &str, keepalive: Duration) -> proto::ControllerMessage {
        proto::ControllerMessage {
            kind: Some(ControllerKind::Registered(proto::Registered {
                worker_id: worker_id.to_owned(),
                keepalive_interval: Some(prost_types::Duration {
                    seconds: i64::try_from(keepalive.as_secs()).expect("seconds"),
                    nanos: i32::try_from(keepalive.subsec_nanos()).expect("nanos"),
                }),
            })),
        }
    }

    fn sample_assign(attempt: &str) -> proto::Assign {
        proto::Assign {
            attempt: attempt.to_owned(),
            operation: "operations/op-1".to_owned(),
            action_digest: Some(Digest {
                hash: "aa".repeat(32),
                size_bytes: 128,
            }),
            action: None,
            command: None,
            platform: HashMap::new(),
            reservation: Some(proto::Resources {
                cpu_millicores: 1000,
                memory_bytes: 512 << 20,
                disk_bytes: 1 << 30,
            }),
            attempt_deadline: None,
            timeout: None,
            target: "//pkg:target".to_owned(),
            mnemonic: "Rustc".to_owned(),
            skip_cache_lookup: false,
            do_not_cache: false,
            cpu_weight: 100,
            assign_seq: 1,
        }
    }

    #[test]
    fn backoff_delay_grows_exponentially_with_jitter_and_caps_at_connect_timeout() {
        let initial = Duration::from_millis(100);
        let cap = Duration::from_secs(5);
        for attempt in 0..12 {
            let expected_ceil = initial.saturating_mul(1_u32 << attempt.min(10)).min(cap);
            let min = backoff_delay(attempt, initial, cap, 0);
            let max = backoff_delay(attempt, initial, cap, u64::MAX);
            assert_eq!(min, expected_ceil / 2, "attempt {attempt}");
            assert!(
                max <= expected_ceil && max >= expected_ceil / 2,
                "attempt {attempt}"
            );
            assert!(max <= cap, "attempt {attempt} exceeded cap");
        }
        // High attempt numbers saturate without overflow.
        let saturated = backoff_delay(100, initial, cap, 123_456_789);
        assert!(saturated <= cap && saturated >= cap / 2);
    }

    #[tokio::test]
    async fn register_and_periodic_keepalive_reports_stats_and_stops_with_going_away() -> TestResult
    {
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let dial_calls = Arc::new(AtomicUsize::new(0));
        let mut connector = endpoint_connector(addr, Arc::clone(&dial_calls));
        let config = test_config(NonZeroDuration::from_secs(2));
        let stats = WorkerCacheStats {
            local_read_requests: 12,
            local_read_bytes: 4096,
            slow_read_requests: 3,
            slow_read_bytes: 1024,
            evictions: 1,
            evicted_bytes: 512,
            size_bytes: 8192,
            budget_bytes: 65536,
        };
        let telemetry = move || (stats, Some(4_000_000_000_u64));
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let clock = SystemClock::new();

        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });

        let mut session = sessions.recv().await.ok_or("missing session")?;
        let first = session.inbound_rx.recv().await.ok_or("missing Register")?;
        let Some(WorkerKind::Register(reg)) = first.kind else {
            return Err("first message must be Register".into());
        };
        assert_eq!(reg.instance, "inst-1");
        assert_eq!(reg.fleet, "fleet-a");

        session
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000001",
                Duration::from_millis(25),
            )))
            .await?;

        let Some(SessionEvent::Registered {
            worker_id,
            generation_id,
            keepalive_interval,
            ..
        }) = events_rx.recv().await
        else {
            return Err("expected Registered event".into());
        };
        assert_eq!(
            worker_id.as_str(),
            "inst-1-00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(generation_id.instance_name(), Some("inst-1"));
        assert_eq!(keepalive_interval, Duration::from_millis(25));

        let ka = tokio::time::timeout(Duration::from_secs(2), session.inbound_rx.recv())
            .await?
            .ok_or("missing KeepAlive")?;
        let Some(WorkerKind::KeepAlive(ka)) = ka.kind else {
            return Err("expected KeepAlive".into());
        };
        assert_eq!(ka.cache, Some(cache_stats_to_proto(stats)));
        assert_eq!(ka.memory_capacity, Some(4_000_000_000));

        let _ = stop_tx.send(());
        task.await??;
        let mut saw_going_away = false;
        while let Some(msg) = session.inbound_rx.recv().await {
            if matches!(msg.kind, Some(WorkerKind::GoingAway(_))) {
                saw_going_away = true;
                break;
            }
        }
        assert!(saw_going_away, "stop must send GoingAway");
        assert_eq!(dial_calls.load(Ordering::SeqCst), 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn assign_accept_executed_completed_and_decline_roundtrip() -> TestResult {
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let dial_calls = Arc::new(AtomicUsize::new(0));
        let mut connector = endpoint_connector(addr, dial_calls);
        let config = test_config(NonZeroDuration::from_secs(5));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let clock = SystemClock::new();

        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });

        let mut session = sessions.recv().await.ok_or("missing session")?;
        let _ = session.inbound_rx.recv().await.ok_or("missing Register")?;
        session
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000002",
                Duration::from_secs(30),
            )))
            .await?;

        let Some(SessionEvent::Registered { outbound, .. }) = events_rx.recv().await else {
            return Err("expected Registered".into());
        };

        // Roundtrip 1: Assign -> Accept -> Executed -> Completed
        session
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::Assign(sample_assign("attempt-1"))),
            }))
            .await?;
        let Some(SessionEvent::Assign(assign)) = events_rx.recv().await else {
            return Err("expected Assign".into());
        };
        assert_eq!(assign.attempt, "attempt-1");
        outbound.accept("attempt-1").await?;
        outbound
            .executed(proto::Executed {
                attempt: "attempt-1".to_owned(),
                exit_code: 0,
                status: Some(RpcStatus::default()),
                ..Default::default()
            })
            .await?;
        outbound
            .completed(proto::Completed {
                attempt: "attempt-1".to_owned(),
                outcome: Some(proto::completed::Outcome::Result(ActionResult {
                    exit_code: 0,
                    ..Default::default()
                })),
            })
            .await?;

        assert!(matches!(
            session.inbound_rx.recv().await.and_then(|m| m.kind),
            Some(WorkerKind::Accept(a)) if a.attempt == "attempt-1"
        ));
        assert!(matches!(
            session.inbound_rx.recv().await.and_then(|m| m.kind),
            Some(WorkerKind::Executed(e)) if e.attempt == "attempt-1" && e.exit_code == 0
        ));
        assert!(matches!(
            session.inbound_rx.recv().await.and_then(|m| m.kind),
            Some(WorkerKind::Completed(c)) if c.attempt == "attempt-1"
        ));

        // Roundtrip 2: Assign -> Decline
        session
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::Assign(sample_assign("attempt-2"))),
            }))
            .await?;
        let Some(SessionEvent::Assign(assign2)) = events_rx.recv().await else {
            return Err("expected second Assign".into());
        };
        assert_eq!(assign2.attempt, "attempt-2");
        outbound
            .decline("attempt-2", proto::DeclineReason::DiskShortage, 1234)
            .await?;
        assert!(matches!(
            session.inbound_rx.recv().await.and_then(|m| m.kind),
            Some(WorkerKind::Decline(d))
                if d.attempt == "attempt-2"
                    && d.reason == decline_reason_i32(proto::DeclineReason::DiskShortage)
                    && d.free_bytes == 1234
        ));

        let _ = stop_tx.send(());
        task.await??;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn kill_and_set_draining_are_forwarded_in_order() -> TestResult {
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let mut connector = endpoint_connector(addr, Arc::new(AtomicUsize::new(0)));
        let config = test_config(NonZeroDuration::from_secs(5));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let clock = SystemClock::new();

        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });

        let mut session = sessions.recv().await.ok_or("missing session")?;
        let _ = session.inbound_rx.recv().await;
        session
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000003",
                Duration::from_secs(30),
            )))
            .await?;
        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::Registered { .. })
        ));

        session
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::SetDraining(proto::SetDraining {
                    draining: true,
                })),
            }))
            .await?;
        session
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::Kill(proto::Kill {
                    attempt: "attempt-9".to_owned(),
                    reason: 1,
                })),
            }))
            .await?;
        session
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::SetDraining(proto::SetDraining {
                    draining: false,
                })),
            }))
            .await?;

        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::SetDraining(true))
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::Kill(k)) if k.attempt == "attempt-9" && k.reason == 1
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::SetDraining(false))
        ));

        let _ = stop_tx.send(());
        task.await??;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn rehome_opens_fresh_session_with_fresh_worker_id() -> TestResult {
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let dial_calls = Arc::new(AtomicUsize::new(0));
        let mut connector = endpoint_connector(addr, Arc::clone(&dial_calls));
        let config = test_config(NonZeroDuration::from_secs(5));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let clock = SystemClock::new();

        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });

        let mut first = sessions.recv().await.ok_or("missing session 1")?;
        let _ = first.inbound_rx.recv().await;
        first
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000010",
                Duration::from_secs(30),
            )))
            .await?;
        let Some(SessionEvent::Registered {
            worker_id: wid1,
            generation_id: gen1,
            outbound: outbound1,
            ..
        }) = events_rx.recv().await
        else {
            return Err("expected first Registered".into());
        };

        first
            .outbound_tx
            .send(Ok(proto::ControllerMessage {
                kind: Some(ControllerKind::Rehome(proto::Rehome {})),
            }))
            .await?;
        assert!(matches!(events_rx.recv().await, Some(SessionEvent::Rehome)));
        let Some(SessionEvent::Disconnected { reason: None, ack }) = events_rx.recv().await else {
            return Err("expected disconnect drain barrier".into());
        };
        assert!(
            sessions.try_recv().is_err(),
            "session replaced before drain acknowledgement"
        );
        let _ = ack.send(());

        let mut second = sessions.recv().await.ok_or("missing session 2")?;
        let reg2 = second
            .inbound_rx
            .recv()
            .await
            .ok_or("missing second Register")?;
        assert!(matches!(reg2.kind, Some(WorkerKind::Register(_))));
        second
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000011",
                Duration::from_secs(30),
            )))
            .await?;
        let Some(SessionEvent::Registered {
            worker_id: wid2,
            generation_id: gen2,
            outbound: outbound2,
            ..
        }) = events_rx.recv().await
        else {
            return Err("expected second Registered".into());
        };
        assert_ne!(wid1, wid2);
        assert_ne!(gen1, gen2);
        assert_eq!(dial_calls.load(Ordering::SeqCst), 2);
        assert!(outbound1.accept("stale").await.is_err());
        outbound2.accept("fresh").await?;
        assert!(matches!(
            second.inbound_rx.recv().await.and_then(|m| m.kind),
            Some(WorkerKind::Accept(a)) if a.attempt == "fresh"
        ));

        let _ = stop_tx.send(());
        task.await??;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn reconnects_using_test_clock_backoff_after_transient_failures() -> TestResult {
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempted = Arc::new(Notify::new());
        let uri = format!("http://{addr}");
        let attempts_ref = Arc::clone(&attempts);
        let attempted_ref = Arc::clone(&attempted);
        let mut connector = move || {
            let uri = uri.clone();
            let attempts = Arc::clone(&attempts_ref);
            let attempted = Arc::clone(&attempted_ref);
            async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                attempted.notify_one();
                if n < 3 {
                    return Err(ConnectError::Transient(Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "controller starting",
                    ))));
                }
                let endpoint = Endpoint::from_shared(uri)
                    .map_err(|err| ConnectError::Transient(Box::new(err)))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|err| ConnectError::from_transport(Box::new(err)))?;
                Ok(ConnectedChannel {
                    channel,
                    revoked: None,
                })
            }
        };

        let config = test_config(NonZeroDuration::from_secs(2));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let clock = Arc::new(TestClock::new(Timestamp::UNIX_EPOCH));
        let run_clock = Arc::clone(&clock);

        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &run_clock,
                events_tx,
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });

        // Attempt 1 fails -> backoff sleep -> advance clock -> Attempt 2 fails -> advance -> Attempt 3 connects.
        for expected in 1..=2 {
            while attempts.load(Ordering::SeqCst) < expected {
                attempted.notified().await;
            }
            tokio::task::yield_now().await;
            clock.advance(Duration::from_secs(2));
        }

        let mut session = sessions
            .recv()
            .await
            .ok_or("missing session after backoff")?;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let _ = session.inbound_rx.recv().await;
        session
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000020",
                Duration::from_secs(30),
            )))
            .await?;
        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::Registered { .. })
        ));

        let _ = stop_tx.send(());
        task.await??;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn stops_without_reconnect_on_permission_denied_unauthenticated_and_connect_revoked()
    -> TestResult {
        for status in [
            Status::permission_denied("principal revoked"),
            Status::unauthenticated("bad certificate"),
        ] {
            let (controller, _sessions) = MockController::new();
            controller.set_reject(Some(status));
            let (addr, server) = spawn_mock_server(controller).await?;
            let dial_calls = Arc::new(AtomicUsize::new(0));
            let mut connector = endpoint_connector(addr, Arc::clone(&dial_calls));
            let config = test_config(NonZeroDuration::from_secs(2));
            let telemetry = || (WorkerCacheStats::default(), None);
            let (events_tx, _events_rx) = mpsc::channel(config.event_buffer.get());
            let clock = SystemClock::new();

            let err = run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                std::future::pending::<()>(),
            )
            .await
            .expect_err("revoked/unauthenticated must terminate");
            assert!(matches!(err, SessionError::Rejected(_)));
            let port_err: PortError = err.into();
            assert!(matches!(port_err, PortError::Backend(_)));
            assert_eq!(dial_calls.load(Ordering::SeqCst), 1);
            server.abort();
        }

        // Connector-level refusal (e.g. TLS certificate refused) stops without reconnecting.
        let dial_calls = Arc::new(AtomicUsize::new(0));
        let calls_ref = Arc::clone(&dial_calls);
        let mut connector = move || {
            let calls = Arc::clone(&calls_ref);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ConnectError::Revoked(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "certificate refused",
                ))))
            }
        };
        let config = test_config(NonZeroDuration::from_secs(2));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, _events_rx) = mpsc::channel(config.event_buffer.get());
        let clock = SystemClock::new();
        let err = run_session(
            &config,
            &mut connector,
            &telemetry,
            &clock,
            events_tx,
            std::future::pending::<()>(),
        )
        .await
        .expect_err("connect revocation must terminate");
        assert!(matches!(err, SessionError::Revoked(_)));
        assert_eq!(dial_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    struct AlertReader;
    impl tokio::io::AsyncRead for AlertReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                rustls::Error::AlertReceived(rustls::AlertDescription::CertificateRevoked),
            )))
        }
    }
    impl tokio::io::AsyncWrite for AlertReader {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn stops_without_reconnect_when_tls_revoked_alert_fires_mid_session() -> TestResult {
        use tokio::io::AsyncWriteExt;

        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller).await?;
        let alert = RevokedAlert::default();
        let alert_for_connector = alert.clone();
        let dial_calls = Arc::new(AtomicUsize::new(0));
        let calls_ref = Arc::clone(&dial_calls);
        let uri = format!("http://{addr}");
        let mut connector = move || {
            let uri = uri.clone();
            let calls = Arc::clone(&calls_ref);
            let alert = alert_for_connector.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let endpoint = Endpoint::from_shared(uri)
                    .map_err(|err| ConnectError::Transient(Box::new(err)))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|err| ConnectError::from_transport(Box::new(err)))?;
                Ok(ConnectedChannel {
                    channel,
                    revoked: Some(alert),
                })
            }
        };

        let config = test_config(NonZeroDuration::from_secs(5));
        let telemetry = || (WorkerCacheStats::default(), None);
        let (events_tx, mut events_rx) = mpsc::channel(config.event_buffer.get());
        let clock = SystemClock::new();
        let task = tokio::spawn(async move {
            run_session(
                &config,
                &mut connector,
                &telemetry,
                &clock,
                events_tx,
                std::future::pending::<()>(),
            )
            .await
        });

        let session = sessions.recv().await.ok_or("missing session")?;
        session
            .outbound_tx
            .send(Ok(registered_msg(
                "inst-1-00000000-0000-4000-8000-000000000030",
                Duration::from_secs(30),
            )))
            .await?;
        assert!(matches!(
            events_rx.recv().await,
            Some(SessionEvent::Registered { .. })
        ));

        let mut watched = alert.watch(AlertReader);
        let _ = tokio::io::AsyncReadExt::read(&mut watched, &mut [0_u8; 1]).await;
        let _ = watched.shutdown().await;

        let err = task
            .await?
            .expect_err("TLS revoked alert must terminate session");
        assert!(matches!(err, SessionError::CertificateRevoked(_)));
        let port_err: PortError = err.into();
        assert!(matches!(port_err, PortError::Backend(_)));
        assert_eq!(dial_calls.load(Ordering::SeqCst), 1);
        server.abort();
        Ok(())
    }
}
