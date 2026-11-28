//! Inherent native worker runtime (`§6.2`–`§6.5`).
//!
//! Composes [`DiskCas`], optional [`TreeCache`], [`Tiered`] (`Direct` or `ViaController`
//! [`RemoteCas`]), [`DirectoryCache`], [`Graveyard`], [`WorkerSampler`], [`start_orphan_reaper`],
//! and [`run_session`] into a complete action execution loop ready for `SchedulerPort::run_worker`
//! wiring.

use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use gang_cas::{
    BlockingScope, Compression, DiskCas, RemoteCas, RemoteCasConfig, Tiered, TreeCache,
    TreeCacheConfig,
};
use gang_clock::{Clock, Timestamp};
use gang_config::ActionsConfig;
use gang_config::WorkerConfig;
use gang_reapi::from_proto;
use gang_reapi::google::rpc as rpc_proto;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_spi::EngineStore;
use gang_spi::{
    ActionMemoryPool, ActionStore, BlobStream, ByteRange, ContentStore, PortError, WorkerStores,
};
use gang_types::{
    AttemptId, CpuWeight, Digest, FleetProperties, InstanceId, WorkerId, WorkerLifetime, property,
};
use gang_types::{NonZeroBytes, NonZeroDuration};
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tonic::transport::Channel;

use crate::worker_graveyard::{Graveyard, GraveyardError, sweep_work_root};
use crate::worker_runner::{
    DEFAULT_MAX_OUTPUT_BYTES, ReadOnlyBind, RunOutcome, RunSpec, RunnerError, SideChannelInputs,
    declared_output_paths, run_attempt, start_orphan_reaper,
};
use crate::worker_sampling::{
    GraveyardPending, KillTrigger, KillWaiter, SamplingConfig, SamplingRegistry, SystemProbe,
    WorkerSampler, kill_channel,
};
use crate::worker_session::{
    ConnectError, ConnectedChannel, SessionConfig, SessionConnector, SessionError, SessionEvent,
    SessionOutbound, TelemetrySource, run_session,
};
use crate::worker_staging::{
    DirectoryCache, RemoteTreeSource, StageConfig, StagedAttempt, StagingError,
    stage_attempt_in_worker,
};
use crate::worker_upload::{
    CapturedOutput as UploadCapturedOutput, ExecutionTiming, UploadConfig, UploadError,
    UploadInput, upload_outputs_with_scope,
};

/// Default maximum `gRPC` frame size for worker session messages (16 `MiB`).
pub const DEFAULT_WORKER_MAX_MESSAGE_SIZE: NonZeroBytes = NonZeroBytes::mib(16);

/// Runtime inputs describing one native worker (`§6.2`–`§6.5`).
#[derive(Debug, Clone)]
pub struct WorkerRuntimeSpec {
    /// Shared action scheduling and enforcement policy.
    pub actions: Arc<ActionsConfig>,
    /// Worker-local tuning (concurrency, batch limits, cache budgets, graveyard pool size).
    pub worker: WorkerConfig,
    /// Action memory pool `cgroup`, when kernel limits are active.
    pub memory_pool: Option<ActionMemoryPool>,
    /// Physical instance hosting this worker.
    pub instance: InstanceId,
    /// Admitted fleet properties registered with the controller.
    pub properties: FleetProperties,
    /// Long-lived or single-use worker lifetime.
    pub lifetime: WorkerLifetime,
    /// Scratch directory for attempt workspaces (`<work>`), emptied at start and stop.
    pub work_directory: PathBuf,
    /// Local fast CAS directory (`<scratch>/cas`).
    pub fast_cas_directory: PathBuf,
    /// Eviction budget in bytes for [`DiskCas`].
    pub fast_cas_budget: gang_types::Bytes,
    /// Optional immutable input tree cache directory; active iff `Some`.
    pub trees_directory: Option<PathBuf>,
    /// Absolute path to the action entrypoint (`gang-exec`).
    pub entrypoint: PathBuf,
    /// REAPI `instance_name` sent on controller CAS, `ActionCache`, and `GetTree` requests.
    pub instance_name: String,
    /// Gang build version string reported in `Register.gang_version`.
    pub gang_version: String,
    /// Maximum encoded and decoded `gRPC` message size on the worker session stream.
    pub max_message_size: NonZeroBytes,
    /// Transport compression used by [`RemoteCas`] in [`WorkerStores::ViaController`] mode.
    pub remote_compression: Compression,
}

/// Fatal errors returned by [`run_worker`].
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum WorkerError {
    /// Worker specification or configuration was invalid.
    #[error("invalid worker configuration: {0}")]
    #[failure(config)]
    Config(String),
    /// Controller session failed fatally (for example certificate or principal revocation).
    #[error(transparent)]
    #[failure(transparent)]
    Session(#[from] SessionError),
    /// Preparing, retiring into, or draining `.graveyard` or `<work>` failed.
    #[error(transparent)]
    #[failure(backend)]
    Graveyard(#[from] GraveyardError),
    /// Opening or operating [`DiskCas`] or [`TreeCache`] failed.
    #[error("local cache failure: {0}")]
    #[failure(transparent)]
    Cache(#[from] PortError),
}

#[derive(Debug)]
enum WorkerContentStore<C> {
    Direct(Arc<C>),
    Controller(ControllerCas),
}

#[derive(Debug)]
enum WorkerActionStore<A> {
    Direct(Arc<A>),
    Controller(ControllerCas),
}

#[derive(Debug, Clone)]
struct ControllerCas {
    channel: Arc<Mutex<Option<Channel>>>,
    config: RemoteCasConfig,
}

impl ControllerCas {
    fn client(&self) -> Result<RemoteCas, PortError> {
        let channel = self.channel.lock().clone().ok_or_else(|| {
            PortError::unavailable(std::io::Error::other("worker session is not connected"))
        })?;
        Ok(RemoteCas::with_config(channel, self.config.clone()))
    }
}

impl<C: ContentStore> ContentStore for WorkerContentStore<C> {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        match self {
            Self::Direct(store) => store.find_missing(digests, missing).await,
            Self::Controller(ctrl) => ctrl.client()?.find_missing(digests, missing).await,
        }
    }

    async fn read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        match self {
            Self::Direct(store) => store.read(digest, range).await,
            Self::Controller(ctrl) => ctrl.client()?.read(digest, range).await,
        }
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        match self {
            Self::Direct(store) => store.write(digest, body).await,
            Self::Controller(ctrl) => ctrl.client()?.write(digest, body).await,
        }
    }

    fn usage(&self) -> Option<EngineStore> {
        match self {
            Self::Direct(store) => store.usage(),
            Self::Controller(_) => None,
        }
    }
}

impl<A: ActionStore> ActionStore for WorkerActionStore<A> {
    async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
        match self {
            Self::Direct(store) => store.get(action).await,
            Self::Controller(ctrl) => ctrl.client()?.get(action).await,
        }
    }

    async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
        match self {
            Self::Direct(store) => store.put(action, result).await,
            Self::Controller(ctrl) => ctrl.client()?.put(action, result).await,
        }
    }

    fn usage(&self) -> Option<EngineStore> {
        match self {
            Self::Direct(store) => store.usage(),
            Self::Controller(_) => None,
        }
    }
}

struct ChannelCapturingConnector<Conn> {
    inner: Conn,
    shared: Arc<Mutex<Option<Channel>>>,
}

impl<Conn: SessionConnector> SessionConnector for ChannelCapturingConnector<Conn> {
    async fn connect(&mut self) -> Result<ConnectedChannel, ConnectError> {
        let connected = self.inner.connect().await?;
        *self.shared.lock() = Some(connected.channel.clone());
        Ok(connected)
    }
}

struct WorkerTelemetry {
    disk_cas: DiskCas,
    memory_capacity: Option<u64>,
    sampling: SamplingRegistry,
}

impl TelemetrySource for WorkerTelemetry {
    fn start_session(&self) {
        self.sampling.start_session();
    }

    fn sample(&self) -> (gang_spi::WorkerCacheStats, Option<u64>) {
        (self.disk_cas.worker_cache_stats(), self.memory_capacity)
    }

    fn cpu_load(&self) -> Option<proto::WorkerLoad> {
        self.sampling.cpu_load()
    }

    fn inflight(&self) -> Vec<proto::AttemptUsageUpdate> {
        self.sampling.inflight_updates()
    }
}

struct DiskReservationTracker {
    admitted_bytes: AtomicU64,
}

impl DiskReservationTracker {
    const fn new() -> Self {
        Self {
            admitted_bytes: AtomicU64::new(0),
        }
    }

    fn try_admit(&self, work_dir: &Path, requested_bytes: u64) -> Result<DiskReservation<'_>, u64> {
        let free_bytes = gang_os::fs::space(work_dir).map_or(0, |space| space.available.get());
        if requested_bytes == 0 {
            return Ok(DiskReservation {
                tracker: None,
                bytes: 0,
            });
        }
        let admitted = self.admitted_bytes.load(Ordering::Acquire);
        if free_bytes.saturating_sub(admitted) < requested_bytes {
            return Err(free_bytes);
        }
        let prev = self
            .admitted_bytes
            .fetch_add(requested_bytes, Ordering::AcqRel);
        if free_bytes.saturating_sub(prev) < requested_bytes {
            self.admitted_bytes
                .fetch_sub(requested_bytes, Ordering::AcqRel);
            return Err(free_bytes);
        }
        Ok(DiskReservation {
            tracker: Some(self),
            bytes: requested_bytes,
        })
    }
}

struct DiskReservation<'a> {
    tracker: Option<&'a DiskReservationTracker>,
    bytes: u64,
}

struct OwnedDiskReservation {
    tracker: Arc<DiskReservationTracker>,
    bytes: u64,
}

impl OwnedDiskReservation {
    fn release(&mut self) {
        if self.bytes > 0 {
            self.tracker
                .admitted_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            self.bytes = 0;
        }
    }
}

impl Drop for OwnedDiskReservation {
    fn drop(&mut self) {
        self.release();
    }
}

impl Drop for DiskReservation<'_> {
    fn drop(&mut self) {
        if let Some(tracker) = self.tracker
            && self.bytes > 0
        {
            tracker
                .admitted_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

fn into_owned_reservation(
    guard: DiskReservation<'_>,
    tracker: &Arc<DiskReservationTracker>,
) -> OwnedDiskReservation {
    let bytes = guard.bytes;
    // Transfer ownership atomically by disarming the borrowed guard without touching the counter.
    std::mem::forget(guard);
    OwnedDiskReservation {
        tracker: Arc::clone(tracker),
        bytes,
    }
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct WorkerRuntime<C, A, K> {
    spec: WorkerRuntimeSpec,
    clock: K,
    tiered: Tiered<DiskCas, WorkerContentStore<C>>,
    action_store: WorkerActionStore<A>,
    tree_cache: Option<TreeCache>,
    dir_cache: DirectoryCache,
    graveyard: Graveyard,
    sampling: SamplingRegistry,
    disk_reservations: Arc<DiskReservationTracker>,
    shared_channel: Arc<Mutex<Option<Channel>>>,
    via_controller: bool,
    active_kills: Arc<Mutex<HashMap<String, KillTrigger>>>,
}

struct ActiveAttemptGuard {
    attempt: String,
    active_kills: Arc<Mutex<HashMap<String, KillTrigger>>>,
}

impl Drop for ActiveAttemptGuard {
    fn drop(&mut self) {
        self.active_kills.lock().remove(&self.attempt);
    }
}

struct RetireOnDrop<'a> {
    graveyard: &'a Graveyard,
    attempt_dir: Option<PathBuf>,
    walk_bytes: u64,
    cas_shared_bytes: u64,
}

impl RetireOnDrop<'_> {
    fn retire_now(mut self) -> Result<(), GraveyardError> {
        if let Some(dir) = self.attempt_dir.take() {
            self.graveyard
                .retire_attempt(&dir, self.walk_bytes, self.cas_shared_bytes)?;
        }
        Ok(())
    }
}

impl Drop for RetireOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(dir) = self.attempt_dir.take() {
            let _ = self
                .graveyard
                .retire_attempt(&dir, self.walk_bytes, self.cas_shared_bytes);
        }
    }
}

/// Runs a native worker against a controller session until `stop` completes, a single-use worker
/// finishes its attempt, or a fatal session error (such as revocation) occurs.
///
/// # Errors
///
/// Returns [`WorkerError`] if configuration is invalid, `<work>` / `.graveyard` preparation or
/// drainage fails, [`DiskCas`] / [`TreeCache`] initialization fails, or [`run_session`] fails
/// fatally.
pub async fn run_worker<Conn, C, A, K>(
    spec: &WorkerRuntimeSpec,
    stores: WorkerStores<C, A>,
    connector: Conn,
    clock: &K,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), WorkerError>
where
    Conn: SessionConnector + 'static,
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    start_orphan_reaper();
    let prepared = prepare_worker_runtime(spec, stores, clock).await?;
    let mut wrapped_connector = ChannelCapturingConnector {
        inner: connector,
        shared: Arc::clone(&prepared.runtime.shared_channel),
    };

    let sampler_stop = Arc::new(Notify::new());
    let sampler_stop_rx = Arc::clone(&sampler_stop);
    let sampler_clock = clock.clone();
    let sampler = prepared.sampler;
    let sampler_task = AbortOnDrop(tokio::spawn(async move {
        sampler
            .run(&sampler_clock, sampler_stop_rx.notified())
            .await;
    }));

    let session_config = build_session_config(spec);
    let (events_tx, events_rx) = mpsc::channel(session_config.event_buffer.get());
    let session_stop = Arc::new(Notify::new());
    let session_stop_rx = Arc::clone(&session_stop);
    let session_clock = clock.clone();
    let telemetry = prepared.telemetry;
    let session_task = AbortOnDrop(tokio::spawn(async move {
        run_session(
            &session_config,
            &mut wrapped_connector,
            &telemetry,
            &session_clock,
            events_tx,
            session_stop_rx.notified(),
        )
        .await
    }));

    let result = drive_worker_loop(
        &prepared.runtime,
        events_rx,
        &session_stop,
        session_task,
        &sampler_stop,
        sampler_task,
        stop,
    )
    .await;

    drain_worker_caches(&prepared.runtime).await;
    result
}

async fn drain_worker_caches<C, A, K>(runtime: &WorkerRuntime<C, A, K>)
where
    C: ContentStore,
{
    // Signal both owners before joining: a tree build may be awaiting a detached CAS fetch.
    if let Some(trees) = &runtime.tree_cache {
        trees.cancel();
    }
    runtime.tiered.cancel_inflight();
    if let Some(trees) = &runtime.tree_cache {
        trees.shutdown().await;
    }
    // Direct stores are shared across certificate generations and remain owned by the caller.
    runtime.tiered.shutdown_inflight().await;
    runtime.graveyard.drain().await;
}

struct PreparedWorker<C, A, K> {
    runtime: Arc<WorkerRuntime<C, A, K>>,
    sampler: WorkerSampler<SystemProbe>,
    telemetry: WorkerTelemetry,
}

async fn prepare_worker_runtime<C, A, K>(
    spec: &WorkerRuntimeSpec,
    stores: WorkerStores<C, A>,
    clock: &K,
) -> Result<PreparedWorker<C, A, K>, WorkerError>
where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    let work_dir = spec.work_directory.clone();
    tokio::task::spawn_blocking(move || sweep_work_root(&work_dir))
        .await
        .map_err(|err| WorkerError::Config(format!("work cleanup task failed: {err}")))??;

    let graveyard_pending = Arc::new(GraveyardPending::new());
    let sampling = SamplingRegistry::new(spec.actions.cpu, spec.actions.memory, spec.actions.disk);
    let graveyard = Graveyard::open(
        &spec.work_directory,
        spec.worker.graveyard_workers,
        Arc::clone(&graveyard_pending),
        Some(sampling.clone()),
    )
    .await?;

    let disk_cas = DiskCas::open(&spec.fast_cas_directory, spec.fast_cas_budget.get()).await?;
    let tree_cache = open_optional_tree_cache(spec, &graveyard, &disk_cas).await?;

    let shared_channel = Arc::new(Mutex::new(None));
    let remote_config = RemoteCasConfig {
        instance_name: spec.instance_name.clone(),
        compression: spec.remote_compression,
        max_batch_bytes: spec.worker.batch_blob_max.get().as_usize(),
        ..RemoteCasConfig::default()
    };
    let (slow_content, action_store, via_controller) = match stores {
        WorkerStores::Direct { content, actions } => (
            WorkerContentStore::Direct(content),
            WorkerActionStore::Direct(actions),
            false,
        ),
        WorkerStores::ViaController => {
            let ctrl = ControllerCas {
                channel: Arc::clone(&shared_channel),
                config: remote_config,
            };
            (
                WorkerContentStore::Controller(ctrl.clone()),
                WorkerActionStore::Controller(ctrl),
                true,
            )
        }
    };

    let disk_per_instance_bytes = spec
        .properties
        .capacity(property::DISK_KB)
        .map_or(0, |kb| kb.saturating_mul(1024));
    let memory_capacity = spec.memory_pool.as_ref().map(|pool| pool.capacity.as_u64());

    let sampler = WorkerSampler::with_registry(
        SamplingConfig {
            cpu: spec.actions.cpu,
            physical_cpu_millicores: spec
                .properties
                .capacity(property::CPU_MILLICORES)
                .unwrap_or(0),
            memory: spec.actions.memory,
            disk: spec.actions.disk,
            memory_pool: spec.memory_pool.as_ref().map(|pool| pool.cgroup.clone()),
            work_directory: spec.work_directory.clone(),
            disk_per_instance_bytes,
        },
        sampling.clone(),
        SystemProbe::new(Arc::clone(&graveyard_pending)),
    );

    let telemetry = WorkerTelemetry {
        disk_cas: disk_cas.clone(),
        memory_capacity,
        sampling: sampling.clone(),
    };

    let runtime = Arc::new(WorkerRuntime {
        spec: spec.clone(),
        clock: clock.clone(),
        tiered: Tiered::new(disk_cas, slow_content),
        action_store,
        tree_cache,
        dir_cache: DirectoryCache::new(spec.worker.directory_cache_budget),
        graveyard,
        sampling,
        disk_reservations: Arc::new(DiskReservationTracker::new()),
        shared_channel,
        via_controller,
        active_kills: Arc::new(Mutex::new(HashMap::new())),
    });

    Ok(PreparedWorker {
        runtime,
        sampler,
        telemetry,
    })
}

async fn open_optional_tree_cache(
    spec: &WorkerRuntimeSpec,
    graveyard: &Graveyard,
    disk_cas: &DiskCas,
) -> Result<Option<TreeCache>, WorkerError> {
    let Some(trees_dir) = &spec.trees_directory else {
        return Ok(None);
    };
    let hook_graveyard = graveyard.clone();
    let config = TreeCacheConfig {
        min_files: spec.worker.tree_cache_min_files.get(),
        max_trees: spec.worker.tree_cache_max_trees.get(),
        tree_pin_percent: spec.worker.tree_pin_percent,
    };
    let cache = TreeCache::open_with_evict_hook(
        trees_dir,
        Some(graveyard.dir().to_path_buf()),
        disk_cas.clone(),
        config,
        move |evicted| hook_graveyard.retire_evicted_tree(evicted),
    )
    .await?;
    Ok(Some(cache))
}

#[allow(clippy::too_many_lines)]
async fn drive_worker_loop<C, A, K>(
    runtime: &Arc<WorkerRuntime<C, A, K>>,
    mut events_rx: mpsc::Receiver<SessionEvent>,
    session_stop: &Notify,
    mut session_task: AbortOnDrop<Result<(), SessionError>>,
    sampler_stop: &Notify,
    mut sampler_task: AbortOnDrop<()>,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), WorkerError>
where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    let mut stop = pin!(stop);
    let mut attempt_tasks = JoinSet::new();
    let mut current_worker_id: Option<WorkerId> = None;
    let mut current_outbound: Option<SessionOutbound> = None;
    let mut controller_draining = false;
    let mut single_use_claimed = false;
    let single_use = matches!(runtime.spec.lifetime, WorkerLifetime::SingleUse);
    let mut fatal_error: Option<WorkerError> = None;
    let mut session_finished = false;

    loop {
        if single_use && single_use_claimed && attempt_tasks.is_empty() {
            break;
        }

        tokio::select! {
            biased;
            () = &mut stop => {
                if let Some(outbound) = &current_outbound {
                    let _ = outbound.going_away().await;
                }
                cancel_all_attempts(&runtime.active_kills);
                break;
            }
            joined = &mut session_task.0 => {
                session_finished = true;
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        fatal_error = Some(WorkerError::Session(err));
                    }
                    Err(join_err) => {
                        fatal_error = Some(WorkerError::Config(format!(
                            "worker session task failed: {join_err}"
                        )));
                    }
                }
                cancel_all_attempts(&runtime.active_kills);
                break;
            }
            Some(_) = attempt_tasks.join_next(), if !attempt_tasks.is_empty() => {}
            event = events_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    SessionEvent::Registered {
                        worker_id,
                        outbound,
                        ..
                    } => {
                        current_worker_id = Some(worker_id);
                        current_outbound = Some(outbound);
                        controller_draining = false;
                    }
                    SessionEvent::SetDraining(draining) => {
                        controller_draining = draining;
                    }
                    SessionEvent::Kill(kill) => {
                        let reason = proto::KillReason::try_from(kill.reason)
                            .unwrap_or(proto::KillReason::Unspecified);
                        if let Some(trigger) = runtime.active_kills.lock().get(&kill.attempt).cloned() {
                            let _ = trigger.kill(reason);
                        }
                    }
                    SessionEvent::Rehome => {
                        current_outbound = None;
                        while !attempt_tasks.is_empty() {
                            tokio::select! {
                                biased;
                                () = &mut stop => {
                                    cancel_all_attempts(&runtime.active_kills);
                                    drain_attempts(&mut attempt_tasks).await;
                                    session_stop.notify_one();
                                    let _ = (&mut session_task.0).await;
                                    sampler_stop.notify_one();
                                    let _ = (&mut sampler_task.0).await;
                                    return Ok(());
                                }
                                Some(_) = attempt_tasks.join_next() => {}
                            }
                        }
                    }
                    SessionEvent::Disconnected { ack, .. } => {
                        current_outbound = None;
                        current_worker_id = None;
                        cancel_all_attempts(&runtime.active_kills);
                        drain_attempts(&mut attempt_tasks).await;
                        let _ = ack.send(());
                    }
                    SessionEvent::Assign(assign) => {
                        let Some(outbound) = current_outbound.clone() else {
                            continue;
                        };
                        let Some(worker_id) = current_worker_id.clone() else {
                            continue;
                        };
                        let draining = controller_draining || (single_use && single_use_claimed);
                        if draining {
                            let free = free_work_bytes(&runtime.spec.work_directory);
                            let _ = outbound
                                .decline(&assign.attempt, proto::DeclineReason::Draining, free)
                                .await;
                            continue;
                        }
                        let saturated = runtime.sampling.cpu_load().is_some_and(|load| load.cpu_saturated);
                        let has_running = !runtime.active_kills.lock().is_empty();
                        if saturated && has_running {
                            let free = free_work_bytes(&runtime.spec.work_directory);
                            let _ = outbound
                                .decline(&assign.attempt, proto::DeclineReason::CpuPressure, free)
                                .await;
                            continue;
                        }
                        let requested_disk = assign.reservation.as_ref().map_or(0, |r| r.disk_bytes);
                        let reservation = match runtime
                            .disk_reservations
                            .try_admit(&runtime.spec.work_directory, requested_disk)
                        {
                            Ok(guard) => guard,
                            Err(free) => {
                                let _ = outbound
                                    .decline(
                                        &assign.attempt,
                                        proto::DeclineReason::DiskShortage,
                                        free,
                                    )
                                    .await;
                                continue;
                            }
                        };
                        if outbound.accept(&assign.attempt).await.is_err() {
                            drop(reservation);
                            continue;
                        }
                        if single_use {
                            single_use_claimed = true;
                        }
                        let owned_reservation =
                            into_owned_reservation(reservation, &runtime.disk_reservations);
                        let (kill_trigger, kill_waiter) = kill_channel();
                        runtime.active_kills.lock().insert(assign.attempt.clone(), kill_trigger.clone());
                        runtime.sampling.admit_assignment(assign.assign_seq);
                        let active_guard = ActiveAttemptGuard { attempt: assign.attempt.clone(), active_kills: Arc::clone(&runtime.active_kills) };
                        let rt = Arc::clone(runtime);
                        attempt_tasks.spawn(async move {
                            let _active_guard = active_guard;
                            execute_assigned_attempt(
                                rt,
                                *assign,
                                worker_id,
                                outbound,
                                owned_reservation,
                                kill_trigger,
                                kill_waiter,
                            )
                            .await;
                        });
                    }
                }
            }
        }
    }

    drain_attempts(&mut attempt_tasks).await;
    if !session_finished {
        session_stop.notify_one();
        match (&mut session_task.0).await {
            Ok(Err(err)) if fatal_error.is_none() => {
                fatal_error = Some(WorkerError::Session(err));
            }
            Err(join_err) if fatal_error.is_none() => {
                fatal_error = Some(WorkerError::Config(format!(
                    "worker session task failed: {join_err}"
                )));
            }
            _ => {}
        }
    }
    sampler_stop.notify_one();
    let _ = (&mut sampler_task.0).await;

    if let Some(err) = fatal_error {
        return Err(err);
    }
    Ok(())
}

async fn drain_attempts(tasks: &mut JoinSet<()>) {
    while tasks.join_next().await.is_some() {}
}

fn cancel_all_attempts(active_kills: &Mutex<HashMap<String, KillTrigger>>) {
    let triggers: Vec<KillTrigger> = active_kills.lock().values().cloned().collect();
    for trigger in triggers {
        let _ = trigger.kill(proto::KillReason::Cancelled);
    }
}

async fn execute_assigned_attempt<C, A, K>(
    runtime: Arc<WorkerRuntime<C, A, K>>,
    assign: proto::Assign,
    worker_id: WorkerId,
    outbound: SessionOutbound,
    mut reservation: OwnedDiskReservation,
    kill_trigger: KillTrigger,
    kill_waiter: KillWaiter,
) where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    let worker_start = runtime.clock.wall();
    let attempt_id = match AttemptId::try_from(assign.attempt.as_str()) {
        Ok(id) => id,
        Err(error) => {
            let status = rpc_status(rpc_proto::Code::InvalidArgument, error.to_string());
            send_attempt_status(&outbound, &assign.attempt, status).await;
            return;
        }
    };

    let input_fetch_start = runtime.clock.wall();
    let mut staged = match stage_assigned_attempt(&runtime, &assign, &kill_waiter).await {
        Ok(staged) => staged,
        Err(status) => {
            let _ = runtime.graveyard.retire_attempt(
                &runtime.spec.work_directory.join(attempt_id.as_str()),
                0,
                0,
            );
            reservation.release();
            send_attempt_status(&outbound, attempt_id.as_str(), status).await;
            return;
        }
    };
    let input_fetch_completed = runtime.clock.wall();

    let attempt_dir = staged.attempt_dir().to_path_buf();
    let mut retire_guard = RetireOnDrop {
        graveyard: &runtime.graveyard,
        attempt_dir: Some(attempt_dir),
        walk_bytes: 0,
        cas_shared_bytes: 0,
    };

    let run_spec = build_run_spec(&runtime.spec, &assign, &attempt_id, &staged);
    let run_outcome = match run_attempt(
        &run_spec,
        &runtime.clock,
        Some(&runtime.sampling),
        kill_waiter.clone(),
        kill_trigger,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            staged.release_for_upload();
            let _ = retire_guard.retire_now();
            reservation.release();
            let status = runner_error_status(&error);
            send_attempt_status(&outbound, attempt_id.as_str(), status).await;
            return;
        }
    };

    // Release TreeLeases and input CasPins immediately on child exit BEFORE upload (`§6.3`).
    staged.release_for_upload();
    retire_guard.walk_bytes = run_outcome.usage.peak_disk_bytes.unwrap_or(0);

    // Emit `ConnectRequest::Executed` immediately before upload (`§6.4`).
    let executed_proto = run_outcome.to_executed_proto(&attempt_id);
    if outbound.executed(executed_proto.clone()).await.is_err() {
        let _ = retire_guard.retire_now();
        reservation.release();
        return;
    }

    let outcome = finalize_executed_attempt(
        &runtime,
        &assign,
        &attempt_id,
        &staged,
        run_outcome,
        executed_proto.status,
        worker_id,
        worker_start,
        input_fetch_start,
        input_fetch_completed,
        &mut retire_guard,
        &kill_waiter,
    )
    .await;

    let _ = retire_guard.retire_now();
    reservation.release();
    let _ = outbound.completed(outcome).await;
}

async fn send_attempt_status(outbound: &SessionOutbound, attempt: &str, status: rpc_proto::Status) {
    let _ = outbound
        .completed(completed_status(attempt.to_owned(), status))
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn finalize_executed_attempt<C, A, K>(
    runtime: &WorkerRuntime<C, A, K>,
    assign: &proto::Assign,
    attempt_id: &AttemptId,
    staged: &StagedAttempt,
    run_outcome: RunOutcome,
    executed_status: Option<rpc_proto::Status>,
    worker_id: WorkerId,
    worker_start: Timestamp,
    input_fetch_start: Timestamp,
    input_fetch_completed: Timestamp,
    retire_guard: &mut RetireOnDrop<'_>,
    kill_waiter: &KillWaiter,
) -> proto::Completed
where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    if let Some(reason) = kill_waiter.reason() {
        return completed_status(attempt_id.as_str().to_owned(), attempt_kill_status(reason));
    }
    if let Some(kill_reason) = run_outcome.kill {
        let status = executed_status.unwrap_or_else(|| {
            rpc_status(
                rpc_proto::Code::Aborted,
                format!("attempt terminated ({kill_reason:?})"),
            )
        });
        return completed_status(attempt_id.as_str().to_owned(), status);
    }
    match resolve_action_digest(assign) {
        Ok(action_digest) => {
            finish_upload_and_build_completed(
                runtime,
                assign,
                attempt_id,
                action_digest,
                staged,
                run_outcome,
                worker_id,
                worker_start,
                input_fetch_start,
                input_fetch_completed,
                retire_guard,
                kill_waiter,
            )
            .await
        }
        Err(status) => completed_status(attempt_id.as_str().to_owned(), status),
    }
}

async fn stage_assigned_attempt<C, A, K>(
    runtime: &WorkerRuntime<C, A, K>,
    assign: &proto::Assign,
    kill_waiter: &KillWaiter,
) -> Result<StagedAttempt, rpc_proto::Status>
where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    let remote_tree = if runtime.via_controller {
        runtime
            .shared_channel
            .lock()
            .clone()
            .map(|channel| RemoteTreeSource {
                channel,
                instance_name: runtime.spec.instance_name.clone(),
                max_message_size: runtime.spec.max_message_size,
            })
    } else {
        None
    };

    let stage_config = StageConfig::from_worker_config(&runtime.spec.worker);
    let blocking = BlockingScope::new();
    let staged_result = tokio::select! {
        biased;
        reason = kill_waiter.wait() => {
            blocking.cancel_and_join().await;
            return Err(attempt_kill_status(reason));
        }
        result = runtime.clock.timeout(
            runtime.spec.worker.download_timeout.get(),
            Box::pin(stage_attempt_in_worker(
                assign,
                &runtime.spec.work_directory,
                &runtime.tiered,
                runtime.tree_cache.as_ref(),
                &runtime.dir_cache,
                remote_tree.as_ref(),
                &stage_config,
                &blocking,
            )),
        ) => result,
    };

    match staged_result {
        Ok(Ok(staged)) => Ok(staged),
        Ok(Err(error)) => {
            blocking.cancel_and_join().await;
            Err(staging_error_status(&error))
        }
        Err(_elapsed) => {
            blocking.cancel_and_join().await;
            Err(rpc_status(
                rpc_proto::Code::DeadlineExceeded,
                "input staging timed out",
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_upload_and_build_completed<C, A, K>(
    runtime: &WorkerRuntime<C, A, K>,
    assign: &proto::Assign,
    attempt_id: &AttemptId,
    action_digest: Digest,
    staged: &StagedAttempt,
    run_outcome: RunOutcome,
    worker_id: WorkerId,
    worker_start: Timestamp,
    input_fetch_start: Timestamp,
    input_fetch_completed: Timestamp,
    retire_guard: &mut RetireOnDrop<'_>,
    kill_waiter: &KillWaiter,
) -> proto::Completed
where
    C: ContentStore,
    A: ActionStore,
    K: Clock + Clone + 'static,
{
    let upload_config = UploadConfig::from_worker_config(&runtime.spec.worker);
    let upload_input = UploadInput {
        action_digest,
        do_not_cache: assign.do_not_cache,
        exit_code: run_outcome.exit_code,
        command: staged.command(),
        work_dir: staged.work_dir(),
        working_dir: staged.working_dir(),
        stdout: Some(captured_for_upload(run_outcome.stdout)),
        stderr: Some(captured_for_upload(run_outcome.stderr)),
        timing: ExecutionTiming {
            worker: worker_id.as_str().to_owned(),
            queued: None,
            worker_start: Some(worker_start),
            input_fetch_start: Some(input_fetch_start),
            input_fetch_completed: Some(input_fetch_completed),
            execution_start: Some(run_outcome.started),
            execution_completed: Some(run_outcome.finished),
        },
    };

    let blocking = BlockingScope::new();
    let upload_res = tokio::select! {
        biased;
        reason = kill_waiter.wait() => {
            blocking.cancel_and_join().await;
            return completed_status(attempt_id.as_str().to_owned(), attempt_kill_status(reason));
        }
        result = runtime.clock.timeout(
            runtime.spec.worker.upload_timeout.get(),
            Box::pin(upload_outputs_with_scope(
                upload_input,
                &runtime.tiered,
                Some(&runtime.action_store),
                &runtime.clock,
                &upload_config,
                &blocking,
            )),
        ) => result,
    };

    match upload_res {
        Ok(Ok(outcome)) => {
            retire_guard.cas_shared_bytes = outcome.cas_shared_bytes;
            proto::Completed {
                attempt: attempt_id.as_str().to_owned(),
                outcome: Some(proto::completed::Outcome::Result(outcome.action_result)),
            }
        }
        Ok(Err(error)) => {
            blocking.cancel_and_join().await;
            completed_status(attempt_id.as_str().to_owned(), upload_error_status(&error))
        }
        Err(_elapsed) => {
            blocking.cancel_and_join().await;
            completed_status(
                attempt_id.as_str().to_owned(),
                rpc_status(rpc_proto::Code::DeadlineExceeded, "output upload timed out"),
            )
        }
    }
}

fn attempt_kill_status(reason: proto::KillReason) -> rpc_proto::Status {
    let (code, message) = match reason {
        proto::KillReason::Cancelled => (rpc_proto::Code::Cancelled, "attempt cancelled"),
        proto::KillReason::DeadlineExceeded => (
            rpc_proto::Code::DeadlineExceeded,
            "attempt deadline exceeded",
        ),
        proto::KillReason::Timeout => (
            rpc_proto::Code::DeadlineExceeded,
            "action execution timed out",
        ),
        proto::KillReason::Memory => (
            rpc_proto::Code::ResourceExhausted,
            "action killed due to memory limit or pressure",
        ),
        proto::KillReason::DiskPressure => (
            rpc_proto::Code::ResourceExhausted,
            "action killed due to worker disk pressure",
        ),
        proto::KillReason::DiskLimit => (
            rpc_proto::Code::ResourceExhausted,
            "action exceeded soft disk ceiling",
        ),
        proto::KillReason::Unspecified => (rpc_proto::Code::Aborted, "attempt terminated"),
    };
    rpc_status(code, message)
}

fn captured_for_upload(output: crate::worker_runner::CapturedOutput) -> UploadCapturedOutput {
    match output {
        crate::worker_runner::CapturedOutput::File(path) => UploadCapturedOutput::File(path),
        crate::worker_runner::CapturedOutput::Bytes(bytes) => {
            UploadCapturedOutput::Bytes(Bytes::from(bytes))
        }
    }
}

fn build_session_config(spec: &WorkerRuntimeSpec) -> SessionConfig {
    let cpu_physical = spec
        .properties
        .capacity(property::CPU_MILLICORES)
        .unwrap_or(0);
    let cpu_millicores = cpu_physical;
    let memory_bytes = spec.memory_pool.as_ref().map_or_else(
        || {
            spec.properties
                .capacity(property::MEMORY_KB)
                .unwrap_or(0)
                .saturating_mul(1024)
        },
        |pool| pool.capacity.as_u64(),
    );
    let disk_bytes = spec
        .properties
        .capacity(property::DISK_KB)
        .unwrap_or(0)
        .saturating_mul(1024);

    let ceilings = &spec.properties.actions().resources;
    let ceiling_cpu = ceilings.cpu_count.map_or(cpu_physical, |count| {
        u64::from(count.get()).saturating_mul(property::MILLICORES_PER_CORE)
    });
    let ceiling_mem = ceilings.memory.map_or(memory_bytes, NonZeroBytes::as_u64);
    let ceiling_disk = ceilings.disk.map_or(disk_bytes, NonZeroBytes::as_u64);

    let properties: HashMap<String, String> = spec
        .properties
        .worker_properties()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let environment = spec
        .properties
        .get(property::ACTION_ENVIRONMENT)
        .map(|identity| proto::ActionEnvironment {
            identity: identity.to_owned(),
        });
    let single_use = matches!(spec.lifetime, WorkerLifetime::SingleUse);

    let register = proto::Register {
        instance: spec.instance.as_str().to_owned(),
        fleet: spec.properties.name().to_owned(),
        gang_version: spec.gang_version.clone(),
        properties,
        capacity: Some(proto::Resources {
            cpu_millicores,
            memory_bytes,
            disk_bytes,
        }),
        ceilings: Some(proto::Resources {
            cpu_millicores: ceiling_cpu,
            memory_bytes: ceiling_mem,
            disk_bytes: ceiling_disk,
        }),
        max_inflight: u32::from(single_use),
        single_use,
        environment,
    };

    let connect_timeout = NonZeroDuration::try_new(spec.worker.connect_timeout.get())
        .unwrap_or_else(|_| NonZeroDuration::from_secs(5));
    SessionConfig::new(register, connect_timeout, spec.max_message_size)
}

fn build_run_spec(
    spec: &WorkerRuntimeSpec,
    assign: &proto::Assign,
    attempt_id: &AttemptId,
    staged: &StagedAttempt,
) -> RunSpec {
    let command = staged.command();
    let argv = command.arguments.iter().map(OsString::from).collect();
    let environment = command
        .environment_variables
        .iter()
        .map(|var| (OsString::from(&var.name), OsString::from(&var.value)))
        .collect();
    let cpu_weight = CpuWeight::try_from(assign.cpu_weight).ok();
    let read_only_trees = staged
        .read_only_trees()
        .iter()
        .map(|tree| ReadOnlyBind {
            source: tree.source.clone(),
            target: tree.relative_target.clone(),
        })
        .collect();
    let (reserved_memory_bytes, reserved_disk_bytes) = assign
        .reservation
        .as_ref()
        .map_or((0, 0), |res| (res.memory_bytes, res.disk_bytes));
    let timeout = Some(effective_action_timeout(
        assign.timeout.as_ref(),
        spec.actions.timeout.get(),
    ));
    let attempt_deadline = assign
        .attempt_deadline
        .as_ref()
        .and_then(proto_timestamp_to_wall);

    RunSpec {
        attempt: attempt_id.clone(),
        entrypoint: spec.entrypoint.clone(),
        argv,
        work_directory: spec.work_directory.clone(),
        working_directory: PathBuf::from(&command.working_directory),
        environment,
        output_paths: declared_output_paths(command),
        platform: assign.platform.clone(),
        side_channel: SideChannelInputs {
            cpu_weight,
            read_only_trees,
        },
        reserved_memory_bytes,
        reserved_cpu_millicores: assign
            .reservation
            .as_ref()
            .map_or(0, |res| res.cpu_millicores),
        reserved_disk_bytes,
        timeout,
        attempt_deadline,
        kill_grace: spec.actions.kill_grace,
        max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
    }
}

fn effective_action_timeout(
    assign_timeout: Option<&prost_types::Duration>,
    max_timeout: Duration,
) -> Duration {
    assign_timeout
        .and_then(proto_duration_to_std)
        .filter(|d| !d.is_zero())
        .map_or(max_timeout, |d| d.min(max_timeout))
}

fn resolve_action_digest(assign: &proto::Assign) -> Result<Digest, rpc_proto::Status> {
    if let Some(proto_digest) = &assign.action_digest {
        return from_proto(proto_digest).map_err(|err| {
            rpc_status(
                rpc_proto::Code::InvalidArgument,
                format!("invalid action_digest: {err}"),
            )
        });
    }
    if let Some(action_bytes) = &assign.action {
        return Ok(Digest::from_bytes(action_bytes));
    }
    Err(rpc_status(
        rpc_proto::Code::InvalidArgument,
        "missing action_digest",
    ))
}

fn proto_duration_to_std(duration: &prost_types::Duration) -> Option<Duration> {
    if duration.seconds < 0 || duration.nanos < 0 {
        return None;
    }
    let secs = u64::try_from(duration.seconds).ok()?;
    let nanos = u32::try_from(duration.nanos).ok()?;
    Some(Duration::new(secs, nanos))
}

fn proto_timestamp_to_wall(timestamp: &prost_types::Timestamp) -> Option<Timestamp> {
    if timestamp.seconds < 0 || timestamp.nanos < 0 {
        return None;
    }
    let secs_ms = i128::from(timestamp.seconds).saturating_mul(1_000);
    let nanos_ms = i128::from(timestamp.nanos) / 1_000_000;
    let total_ms = u64::try_from(secs_ms.saturating_add(nanos_ms)).ok()?;
    Some(Timestamp::from_unix_millis(total_ms))
}

fn free_work_bytes(work_dir: &Path) -> u64 {
    gang_os::fs::space(work_dir).map_or(0, |space| space.available.get())
}

fn completed_status(attempt: String, status: rpc_proto::Status) -> proto::Completed {
    proto::Completed {
        attempt,
        outcome: Some(proto::completed::Outcome::Status(status)),
    }
}

fn rpc_status(code: rpc_proto::Code, message: impl Into<String>) -> rpc_proto::Status {
    rpc_proto::Status {
        code: code as i32,
        message: message.into(),
        details: Vec::new(),
    }
}

fn staging_error_status(error: &StagingError) -> rpc_proto::Status {
    let code = match error {
        StagingError::InvalidAttemptId(_)
        | StagingError::MissingField { .. }
        | StagingError::InvalidDigest { .. }
        | StagingError::MalformedProto { .. }
        | StagingError::InvalidPath { .. }
        | StagingError::InvalidTree { .. } => rpc_proto::Code::InvalidArgument,
        StagingError::MissingBlob { .. } => rpc_proto::Code::FailedPrecondition,
        StagingError::Cancelled => rpc_proto::Code::Cancelled,
        StagingError::Io { .. } | StagingError::Store(_) => rpc_proto::Code::Internal,
    };
    rpc_status(code, gang_types::error_chain(error).to_string())
}

fn runner_error_status(error: &RunnerError) -> rpc_proto::Status {
    let code = match error {
        RunnerError::TooManyReadOnlyTrees { .. }
        | RunnerError::InvalidReadOnlyTree { .. }
        | RunnerError::InvalidWorkingDirectory { .. } => rpc_proto::Code::InvalidArgument,
        RunnerError::Io(_) | RunnerError::Spawn { .. } | RunnerError::Wait { .. } => {
            rpc_proto::Code::Internal
        }
    };
    rpc_status(code, gang_types::error_chain(error).to_string())
}

fn upload_error_status(error: &UploadError) -> rpc_proto::Status {
    let code = match error {
        UploadError::InvalidPath { .. }
        | UploadError::InvalidSymlinkTarget { .. }
        | UploadError::DigestConversion(_) => rpc_proto::Code::InvalidArgument,
        UploadError::OutputKindMismatch { .. } => rpc_proto::Code::FailedPrecondition,
        UploadError::Cancelled => rpc_proto::Code::Cancelled,
        UploadError::Io { .. } | UploadError::Store(_) => rpc_proto::Code::Internal,
    };
    rpc_status(code, gang_types::error_chain(error).to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream;
    use gang_clock::SystemClock;
    use gang_config::ActionsConfig;
    use gang_config::WorkerConfig;
    use gang_reapi::build::bazel::remote::execution::v2::action_cache_server::ActionCacheServer;
    use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer;
    use gang_reapi::build::bazel::remote::execution::v2::{self as reapi};
    use gang_reapi::from_proto;
    use gang_reapi::google::bytestream::byte_stream_server::ByteStreamServer;
    use gang_reapi::leadingterm::gang::worker::v1 as proto;
    use gang_reapi::leadingterm::gang::worker::v1::controller_message::Kind as ControllerKind;
    use gang_reapi::leadingterm::gang::worker::v1::worker_message::Kind as WorkerKind;
    use gang_reapi::leadingterm::gang::worker::v1::worker_service_server::{
        WorkerService, WorkerServiceServer,
    };
    use gang_reapi::to_proto;
    use gang_testkit::{ChildTest, MemoryActionStore, MemoryContentStore, TestResult};
    use gang_types::Bytes as GangBytes;
    use gang_types::{
        ActionEnvironment, Digest, FleetActionPolicy, FleetName, FleetProperties, InstanceId, Isa,
        Resources, WorkerLifetime,
    };
    use prost::Message as _;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tonic::Status;
    use tonic::transport::{Endpoint, Server};

    use super::*;
    use crate::{
        ActionCacheService, ByteStreamConfig, ByteStreamService, CasConfig, CasService,
        InflightBlobBudget,
    };

    const WRITE_OUTPUT_CHILD: ChildTest = ChildTest::new("worker::tests::write_output_child");
    const SLEEP_FOREVER_CHILD: ChildTest = ChildTest::new("worker::tests::sleep_forever_child");

    #[ignore = "child test helper producing an output file and stdout"]
    #[test]
    fn write_output_child() -> Result<(), Box<dyn std::error::Error>> {
        fs::write("result.txt", b"hello-from-worker")?;
        println!("child-stdout-line");
        if let Ok(count) = std::env::var("STDERR_BYTES") {
            use std::io::Write as _;
            let count: usize = count.parse()?;
            std::io::stderr().lock().write_all(&vec![b'x'; count])?;
        }
        WRITE_OUTPUT_CHILD.complete();
        Ok(())
    }

    #[ignore = "child test helper sleeping until killed"]
    #[test]
    fn sleep_forever_child() -> Result<(), Box<dyn std::error::Error>> {
        if let Ok(marker) = std::env::var("STARTED_MARKER_FILE") {
            fs::write(marker, b"started")?;
        }
        if let Ok(address) = std::env::var("STARTED_SOCKET_ADDR") {
            use std::io::Write as _;
            std::net::TcpStream::connect(address)?.write_all(&std::process::id().to_le_bytes())?;
        }
        std::thread::sleep(Duration::from_mins(1));
        SLEEP_FOREVER_CHILD.complete();
        Ok(())
    }

    struct MockSession {
        inbound_rx: mpsc::Receiver<proto::WorkerMessage>,
        outbound_tx: mpsc::Sender<Result<proto::ControllerMessage, Status>>,
    }

    #[derive(Clone)]
    struct MockController {
        sessions: mpsc::Sender<MockSession>,
    }

    impl MockController {
        fn new() -> (Self, mpsc::Receiver<MockSession>) {
            let (sessions, rx) = mpsc::channel(8);
            (Self { sessions }, rx)
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
            let mut incoming = request.into_inner();
            let (in_tx, in_rx) = mpsc::channel(64);
            tokio::spawn(async move {
                while let Ok(Some(msg)) = incoming.message().await {
                    if in_tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
            let (out_tx, out_rx) = mpsc::channel(64);
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
        cas_stores: Option<(Arc<MemoryContentStore>, Arc<MemoryActionStore>)>,
    ) -> TestResult<(std::net::SocketAddr, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = stream::unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await.ok()?;
            Some((Ok::<_, std::io::Error>(stream), listener))
        });

        let handle = tokio::spawn(async move {
            let builder = Server::builder().add_service(WorkerServiceServer::new(controller));
            if let Some((content, ac)) = cas_stores {
                let budget = InflightBlobBudget::new(NonZeroBytes::mib(64));
                let cas =
                    CasService::new(Arc::clone(&content), budget.clone(), CasConfig::default());
                let bytestream =
                    ByteStreamService::new(content, budget, ByteStreamConfig::default());
                let action_cache = ActionCacheService::new(ac);
                let _ = builder
                    .add_service(ContentAddressableStorageServer::new(cas))
                    .add_service(ByteStreamServer::new(bytestream))
                    .add_service(ActionCacheServer::new(action_cache))
                    .serve_with_incoming(incoming)
                    .await;
            } else {
                let _ = builder.serve_with_incoming(incoming).await;
            }
        });
        Ok((addr, handle))
    }

    fn endpoint_connector(addr: std::net::SocketAddr) -> impl SessionConnector {
        let uri = format!("http://{addr}");
        move || {
            let uri = uri.clone();
            async move {
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

    fn test_spec(
        root: &Path,
        lifetime: WorkerLifetime,
        with_trees: bool,
    ) -> TestResult<WorkerRuntimeSpec> {
        let fleet = FleetName::try_new("test-fleet")?;
        let env: ActionEnvironment = "host:test".parse()?;
        let properties = FleetProperties::new(
            &fleet,
            Isa::X86_64,
            &"none",
            &env,
            Resources {
                vcpus: 4,
                memory: GangBytes::gib(8),
                disk: GangBytes::gib(16),
            },
            &FleetActionPolicy::default(),
            [],
        );
        let worker = WorkerConfig {
            connect_timeout: gang_config::WholeSeconds::from_units(2),
            graveyard_workers: NonZeroUsize::new(2).expect("2 is non-zero"),
            ..WorkerConfig::default()
        };

        Ok(WorkerRuntimeSpec {
            actions: Arc::new(ActionsConfig::default()),
            worker,
            memory_pool: None,
            instance: InstanceId::try_new("inst-e5")?,
            properties,
            lifetime,
            work_directory: root.join("work"),
            fast_cas_directory: root.join("cas"),
            fast_cas_budget: GangBytes::mib(32),
            trees_directory: with_trees.then(|| root.join("trees")),
            entrypoint: std::env::current_exe()?,
            instance_name: String::new(),
            gang_version: "0.3.9".to_owned(),
            max_message_size: DEFAULT_WORKER_MAX_MESSAGE_SIZE,
            remote_compression: Compression::Identity,
        })
    }

    #[test]
    fn worker_registers_physical_cpu_capacity() -> TestResult {
        let spec = test_spec(Path::new("/tmp"), WorkerLifetime::LongLived, false)?;
        let config = build_session_config(&spec);
        assert_eq!(
            config.register.capacity.expect("capacity").cpu_millicores,
            4_000
        );
        Ok(())
    }

    fn registered_frame(worker_id: &str) -> proto::ControllerMessage {
        proto::ControllerMessage {
            kind: Some(ControllerKind::Registered(proto::Registered {
                worker_id: worker_id.to_owned(),
                keepalive_interval: Some(prost_types::Duration {
                    seconds: 30,
                    nanos: 0,
                }),
            })),
        }
    }

    async fn seed_action_in_store(
        store: &MemoryContentStore,
        child: ChildTest,
        extra_env: Vec<reapi::command::EnvironmentVariable>,
        outputs: Vec<String>,
        input_files: Vec<(String, Bytes)>,
    ) -> TestResult<(Digest, Bytes, Bytes)> {
        let mut dir_files = Vec::new();
        for (name, content) in input_files {
            let digest = Digest::from_bytes(&content);
            store.write(digest, BlobStream::from_bytes(content)).await?;
            dir_files.push(reapi::FileNode {
                name,
                digest: Some(to_proto(&digest)?),
                is_executable: false,
                node_properties: None,
            });
        }
        dir_files.sort_by(|a, b| a.name.cmp(&b.name));
        let root_dir = reapi::Directory {
            files: dir_files,
            ..Default::default()
        };
        let root_bytes = Bytes::from(root_dir.encode_to_vec());
        let root_digest = Digest::from_bytes(&root_bytes);
        store
            .write(root_digest, BlobStream::from_bytes(root_bytes))
            .await?;

        let command = reapi::Command {
            arguments: child.arguments().into_iter().map(str::to_owned).collect(),
            environment_variables: extra_env,
            output_paths: outputs,
            ..Default::default()
        };
        let cmd_bytes = Bytes::from(command.encode_to_vec());
        let cmd_digest = Digest::from_bytes(&cmd_bytes);
        store
            .write(cmd_digest, BlobStream::from_bytes(cmd_bytes.clone()))
            .await?;

        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_digest)?),
            ..Default::default()
        };
        let action_bytes = Bytes::from(action.encode_to_vec());
        let action_digest = Digest::from_bytes(&action_bytes);
        store
            .write(action_digest, BlobStream::from_bytes(action_bytes.clone()))
            .await?;
        Ok((action_digest, action_bytes, cmd_bytes))
    }

    fn assign_frame(
        attempt: &str,
        action_digest: Digest,
        action_bytes: Option<Bytes>,
        command_bytes: Option<Bytes>,
        disk_bytes: u64,
    ) -> TestResult<proto::ControllerMessage> {
        Ok(proto::ControllerMessage {
            kind: Some(ControllerKind::Assign(proto::Assign {
                attempt: attempt.to_owned(),
                operation: format!("operations/{attempt}"),
                action_digest: Some(to_proto(&action_digest)?),
                action: action_bytes,
                command: command_bytes,
                platform: HashMap::new(),
                reservation: Some(proto::Resources {
                    cpu_millicores: 1000,
                    memory_bytes: 64 << 20,
                    disk_bytes,
                }),
                attempt_deadline: None,
                timeout: Some(prost_types::Duration {
                    seconds: 15,
                    nanos: 0,
                }),
                target: "//pkg:test".to_owned(),
                mnemonic: "Test".to_owned(),
                skip_cache_lookup: false,
                do_not_cache: false,
                cpu_weight: 100,
                assign_seq: 1,
            })),
        })
    }

    async fn next_non_keepalive(
        rx: &mut mpsc::Receiver<proto::WorkerMessage>,
    ) -> TestResult<WorkerKind> {
        while let Some(msg) = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await? {
            if let Some(kind) = msg.kind
                && !matches!(kind, WorkerKind::KeepAlive(_))
            {
                return Ok(kind);
            }
        }
        Err("worker inbound stream ended unexpectedly".into())
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

    fn spawn_direct_worker<C: ContentStore>(
        spec: WorkerRuntimeSpec,
        content: Arc<C>,
        actions: Arc<MemoryActionStore>,
        addr: std::net::SocketAddr,
        stop_rx: oneshot::Receiver<()>,
    ) -> JoinHandle<Result<(), WorkerError>> {
        tokio::spawn(async move {
            run_worker(
                &spec,
                WorkerStores::Direct { content, actions },
                endpoint_connector(addr),
                &SystemClock::new(),
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        })
    }

    async fn expect_register_and_ack(
        sessions: &mut mpsc::Receiver<MockSession>,
        worker_id: &str,
    ) -> TestResult<(MockSession, proto::Register)> {
        let mut session = tokio::time::timeout(Duration::from_secs(5), sessions.recv())
            .await?
            .ok_or("missing session")?;
        let WorkerKind::Register(reg) = next_non_keepalive(&mut session.inbound_rx).await? else {
            return Err("expected Register".into());
        };
        session
            .outbound_tx
            .send(Ok(registered_frame(worker_id)))
            .await?;
        Ok((session, reg))
    }

    async fn expect_accept_executed_completed(
        rx: &mut mpsc::Receiver<proto::WorkerMessage>,
        attempt_id: &str,
    ) -> TestResult<reapi::ActionResult> {
        let WorkerKind::Accept(accept) = next_non_keepalive(rx).await? else {
            return Err("expected Accept".into());
        };
        assert_eq!(accept.attempt, attempt_id);

        let WorkerKind::Executed(executed) = next_non_keepalive(rx).await? else {
            return Err("expected Executed before Completed".into());
        };
        assert_eq!(executed.attempt, attempt_id);
        assert_eq!(executed.exit_code, 0);
        assert!(executed.kill.is_none());

        let WorkerKind::Completed(completed) = next_non_keepalive(rx).await? else {
            return Err("expected Completed".into());
        };
        assert_eq!(completed.attempt, attempt_id);
        let Some(proto::completed::Outcome::Result(action_result)) = completed.outcome else {
            return Err("expected ActionResult".into());
        };
        Ok(action_result)
    }

    async fn assert_output_and_cached(
        content: &MemoryContentStore,
        actions: &MemoryActionStore,
        action_digest: Digest,
        action_result: &reapi::ActionResult,
    ) -> TestResult {
        let out_digest = from_proto(
            action_result.output_files[0]
                .digest
                .as_ref()
                .ok_or("missing output digest")?,
        )?;
        assert_eq!(
            content.get_bytes(out_digest),
            Some(Bytes::from_static(b"hello-from-worker"))
        );
        let cached = actions
            .get(action_digest)
            .await?
            .ok_or("expected ActionResult in MemoryActionStore")?;
        assert_eq!(reapi::ActionResult::decode(cached)?, *action_result);
        Ok(())
    }

    #[derive(Debug, Default)]
    struct BlockedOutputStore {
        inner: MemoryContentStore,
        entered: Notify,
        cancelled: Notify,
    }

    struct UploadCancellation<'a>(&'a Notify);

    impl Drop for UploadCancellation<'_> {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    impl ContentStore for BlockedOutputStore {
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
            if digest != Digest::from_bytes(b"hello-from-worker") {
                return self.inner.write(digest, body).await;
            }
            let _cancel = UploadCancellation(&self.cancelled);
            self.entered.notify_one();
            std::future::pending::<()>().await;
            drop(body);
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            None
        }
    }

    struct UploadAttempt {
        spec: WorkerRuntimeSpec,
        content: Arc<BlockedOutputStore>,
        actions: Arc<MemoryActionStore>,
        sessions: mpsc::Receiver<MockSession>,
        session: MockSession,
        worker: JoinHandle<Result<(), WorkerError>>,
        server: JoinHandle<()>,
        stop: oneshot::Sender<()>,
        attempt: &'static str,
    }

    async fn start_blocked_upload(root: &Path) -> TestResult<UploadAttempt> {
        let spec = test_spec(root, WorkerLifetime::LongLived, false)?;
        let content = Arc::new(BlockedOutputStore::default());
        let actions = Arc::new(MemoryActionStore::new());
        let (digest, action, command) = seed_action_in_store(
            &content.inner,
            WRITE_OUTPUT_CHILD,
            Vec::new(),
            vec!["result.txt".to_owned()],
            Vec::new(),
        )
        .await?;
        let (controller, mut sessions) = MockController::new();
        let (addr, server) = spawn_mock_server(controller, None).await?;
        let (stop, stop_rx) = oneshot::channel();
        let worker = spawn_direct_worker(
            spec.clone(),
            Arc::clone(&content),
            Arc::clone(&actions),
            addr,
            stop_rx,
        );
        let (mut session, _) = expect_register_and_ack(
            &mut sessions,
            "inst-e5-00000000-0000-4000-8000-000000000024",
        )
        .await?;
        let attempt = "00000000-0000-4000-8000-000000000890";
        session
            .outbound_tx
            .send(Ok(assign_frame(
                attempt,
                digest,
                Some(action),
                Some(command),
                128,
            )?))
            .await?;
        assert!(matches!(
            next_non_keepalive(&mut session.inbound_rx).await?,
            WorkerKind::Accept(_)
        ));
        let WorkerKind::Executed(executed) = next_non_keepalive(&mut session.inbound_rx).await?
        else {
            return Err("expected Executed before blocked upload".into());
        };
        assert_eq!(executed.exit_code, 0);
        tokio::time::timeout(Duration::from_secs(5), content.entered.notified()).await?;
        assert!(spec.work_directory.join(attempt).exists());
        Ok(UploadAttempt {
            spec,
            content,
            actions,
            sessions,
            session,
            worker,
            server,
            stop,
            attempt,
        })
    }

    #[tokio::test]
    async fn already_killed_attempt_does_not_start_staging() -> TestResult {
        let tmp = TempDir::new()?;
        let spec = test_spec(
            &tmp.path().canonicalize()?,
            WorkerLifetime::LongLived,
            false,
        )?;
        let prepared = prepare_worker_runtime(
            &spec,
            WorkerStores::Direct {
                content: Arc::new(MemoryContentStore::new()),
                actions: Arc::new(MemoryActionStore::new()),
            },
            &SystemClock::new(),
        )
        .await?;
        let assign = proto::Assign {
            attempt: "00000000-0000-4000-8000-000000000891".to_owned(),
            ..Default::default()
        };
        for (reason, code) in [
            (proto::KillReason::Cancelled, rpc_proto::Code::Cancelled),
            (
                proto::KillReason::DeadlineExceeded,
                rpc_proto::Code::DeadlineExceeded,
            ),
        ] {
            let (trigger, waiter) = kill_channel();
            assert!(trigger.kill(reason));
            let status = stage_assigned_attempt(&prepared.runtime, &assign, &waiter)
                .await
                .expect_err("already killed attempt started staging");
            assert_eq!(status.code, code as i32);
            assert!(!spec.work_directory.join(&assign.attempt).exists());
        }
        prepared.runtime.graveyard.drain().await;
        Ok(())
    }

    async fn prepare_gated_tree_worker<C: ContentStore>(
        spec: &WorkerRuntimeSpec,
        stores: WorkerStores<C, MemoryActionStore>,
        entered: Arc<Notify>,
        release: oneshot::Receiver<()>,
    ) -> TestResult<PreparedWorker<C, MemoryActionStore, SystemClock>> {
        let mut prepared = prepare_worker_runtime(spec, stores, &SystemClock::new()).await?;
        let runtime = Arc::get_mut(&mut prepared.runtime).ok_or("runtime already shared")?;
        let config = *runtime.tree_cache.as_ref().ok_or("tree cache")?.config();
        let graveyard = runtime.graveyard.clone();
        let release = Mutex::new(Some(release));
        runtime.tree_cache = Some(
            TreeCache::open_with_evict_hook(
                spec.trees_directory.as_ref().ok_or("trees directory")?,
                Some(graveyard.dir().to_path_buf()),
                runtime.tiered.disk().clone(),
                config,
                move |evicted| {
                    let gate = release.lock().take();
                    if let Some(gate) = gate {
                        entered.notify_one();
                        tokio::task::block_in_place(|| {
                            let _ = gate.blocking_recv();
                        });
                    }
                    graveyard.retire_evicted_tree(evicted);
                },
            )
            .await?,
        );
        Ok(prepared)
    }

    fn single_file_directory(
        name: &str,
        digest: Digest,
    ) -> TestResult<(Digest, HashMap<Digest, reapi::Directory>)> {
        let directory = reapi::Directory {
            files: vec![reapi::FileNode {
                name: name.to_owned(),
                digest: Some(to_proto(&digest)?),
                ..Default::default()
            }],
            ..Default::default()
        };
        let root = Digest::from_bytes(&directory.encode_to_vec());
        Ok((root, HashMap::from([(root, directory)])))
    }

    #[derive(Debug)]
    struct StalledContentStore {
        contents: MemoryContentStore,
        digest: Digest,
        stalled: Arc<Notify>,
    }

    impl ContentStore for StalledContentStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.contents.find_missing(digests, missing).await
        }

        async fn read(
            &self,
            digest: Digest,
            range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            use futures::StreamExt as _;

            let Some(body) = self.contents.read(digest, range).await? else {
                return Ok(None);
            };
            if digest != self.digest {
                return Ok(Some(body));
            }
            let stalled = Arc::clone(&self.stalled);
            let chunks = body.chunks.chain(stream::poll_fn(move |_| {
                stalled.notify_one();
                std::task::Poll::Pending
            }));
            Ok(Some(BlobStream {
                size: body.size,
                chunks: Box::pin(chunks),
            }))
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            self.contents.write(digest, body).await
        }

        fn usage(&self) -> Option<EngineStore> {
            None
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_shutdown_cancels_stalled_stream_and_joins_filesystem_work() -> TestResult {
        tokio::time::timeout(Duration::from_secs(10), async {
            let tmp = TempDir::new()?;
            let mut spec = test_spec(&tmp.path().canonicalize()?, WorkerLifetime::LongLived, true)?;
            spec.worker.tree_cache_min_files = NonZeroUsize::MIN;
            spec.worker.tree_cache_max_trees = NonZeroUsize::MIN;
            let payload = Bytes::from_static(b"never-finishing-worker-stream");
            let digest = Digest::from_bytes(&payload);
            let stalled = Arc::new(Notify::new());
            let content = Arc::new(StalledContentStore {
                contents: MemoryContentStore::new(),
                digest,
                stalled: Arc::clone(&stalled),
            });
            content
                .write(digest, BlobStream::from_bytes(payload))
                .await?;
            let stores = WorkerStores::Direct {
                content,
                actions: Arc::new(MemoryActionStore::new()),
            };
            let entered = Arc::new(Notify::new());
            let (release, gate) = oneshot::channel();
            let prepared =
                prepare_gated_tree_worker(&spec, stores, Arc::clone(&entered), gate).await?;
            let disk = prepared.runtime.tiered.disk();
            let resident = Bytes::from_static(b"resident-worker-tree");
            let resident_digest = Digest::from_bytes(&resident);
            disk.write(resident_digest, BlobStream::from_bytes(resident))
                .await?;
            let trees = prepared.runtime.tree_cache.as_ref().ok_or("tree cache")?;
            let (old_root, old_dirs) = single_file_directory("old", resident_digest)?;
            drop(
                trees
                    .get_or_materialize(old_root, &old_dirs)
                    .await?
                    .ok_or("old tree")?,
            );

            // Hold an eviction's tracked filesystem task before graveyard dispatch.
            let (new_root, new_dirs) = single_file_directory("new", resident_digest)?;
            let mut building = Box::pin(trees.get_or_materialize(new_root, &new_dirs));
            assert!(futures::poll!(building.as_mut()).is_pending());
            entered.notified().await;
            drop(building);

            // A second tree owns a CAS fetch whose stream never reaches EOF.
            let (stalled_root, stalled_dirs) = single_file_directory("stalled", digest)?;
            let mut staging = Box::pin(trees.get_or_materialize_from(
                stalled_root,
                &stalled_dirs,
                &prepared.runtime.tiered,
            ));
            assert!(futures::poll!(staging.as_mut()).is_pending());
            stalled.notified().await;
            drop(staging);

            let mut shutdown = Box::pin(drain_worker_caches(&prepared.runtime));
            let pending = futures::poll!(shutdown.as_mut()).is_pending();
            let released = release.send(());
            assert!(
                pending,
                "worker released ownership of unfinished filesystem work"
            );
            released.map_err(|()| "eviction gate closed")?;
            shutdown.await;

            assert!(disk.read(digest, ByteRange::FULL).await?.is_none());
            assert!(disk.pin_attempt(digest, false).is_none());
            assert!(!trees.contains(stalled_root));
            assert_eq!(
                fs::read_dir(disk.root().join(gang_cas::TMP_DIR_NAME))?.count(),
                0
            );
            assert_eq!(fs::read_dir(trees.root().join(".tmp"))?.count(), 0);
            assert_eq!(prepared.runtime.graveyard.in_flight(), 0);
            assert_eq!(prepared.runtime.graveyard.failures(), 0);
            assert_eq!(fs::read_dir(prepared.runtime.graveyard.dir())?.count(), 0);
            Ok(())
        })
        .await?
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewal_waits_for_cancelled_tree_build_and_its_graveyard_work() -> TestResult {
        tokio::time::timeout(Duration::from_secs(10), async {
            let tmp = TempDir::new()?;
            let mut spec = test_spec(&tmp.path().canonicalize()?, WorkerLifetime::LongLived, true)?;
            spec.worker.tree_cache_min_files = NonZeroUsize::MIN;
            spec.worker.tree_cache_max_trees = NonZeroUsize::MIN;
            let stores = WorkerStores::Direct {
                content: Arc::new(MemoryContentStore::new()),
                actions: Arc::new(MemoryActionStore::new()),
            };
            let entered = Arc::new(Notify::new());
            let (release_tx, release_rx) = oneshot::channel();
            let prepared =
                prepare_gated_tree_worker(&spec, stores.clone(), Arc::clone(&entered), release_rx)
                    .await?;
            let payload = Bytes::from_static(b"renewal-tree-payload");
            let digest = Digest::from_bytes(&payload);
            prepared
                .runtime
                .tiered
                .disk()
                .write(digest, BlobStream::from_bytes(payload.clone()))
                .await?;
            let trees = prepared.runtime.tree_cache.as_ref().ok_or("tree cache")?;
            let (old_root, old_dirs) = single_file_directory("old", digest)?;
            drop(
                trees
                    .get_or_materialize(old_root, &old_dirs)
                    .await?
                    .ok_or("old tree")?,
            );
            let (new_root, new_dirs) = single_file_directory("new", digest)?;
            let mut staging = Box::pin(trees.get_or_materialize(new_root, &new_dirs));
            assert!(futures::poll!(staging.as_mut()).is_pending());
            entered.notified().await;
            drop(staging);

            // Eviction has detached the old tree, but its cleanup is not queued until release.
            assert_eq!(prepared.runtime.graveyard.in_flight(), 0);
            assert_eq!(fs::read_dir(prepared.runtime.graveyard.dir())?.count(), 1);
            let mut shutdown = Box::pin(drain_worker_caches(&prepared.runtime));
            assert!(
                futures::poll!(shutdown.as_mut()).is_pending(),
                "generation ended while its cancelled staging still owned a tree build"
            );
            release_tx.send(()).map_err(|()| "tree gate closed")?;
            shutdown.await;
            assert!(
                !trees.contains(new_root),
                "cancelled tree build published after shutdown"
            );
            assert_eq!(prepared.runtime.graveyard.failures(), 0);
            assert_eq!(prepared.runtime.graveyard.in_flight(), 0);
            assert_eq!(fs::read_dir(prepared.runtime.graveyard.dir())?.count(), 0);
            drop(prepared);

            let next = prepare_worker_runtime(&spec, stores, &SystemClock::new()).await?;
            let reopened = next.runtime.tree_cache.as_ref().ok_or("new tree cache")?;
            assert!(reopened.is_empty());
            assert_eq!(fs::read_dir(next.runtime.graveyard.dir())?.count(), 0);
            assert_eq!(next.runtime.graveyard.failures(), 0);
            let blob = next
                .runtime
                .tiered
                .read(digest, ByteRange::FULL)
                .await?
                .ok_or("reopened blob")?;
            assert_eq!(blob.collect().await?, payload);
            let tree = reopened
                .get_or_materialize(new_root, &new_dirs)
                .await?
                .ok_or("reopened tree")?;
            assert_eq!(fs::read(tree.path().join("new"))?, payload.as_ref());
            drop(tree);
            drain_worker_caches(&next.runtime).await;
            drop(next);
            gang_os::fs::remove_dir_optional(tmp.path())?;
            Ok(())
        })
        .await?
    }

    #[test]
    fn deadline_kill_cancels_upload_without_publishing_and_retires_attempt() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let mut upload = start_blocked_upload(&tmp.path().canonicalize()?).await?;
            upload
                .session
                .outbound_tx
                .send(Ok(proto::ControllerMessage {
                    kind: Some(ControllerKind::Kill(proto::Kill {
                        attempt: upload.attempt.to_owned(),
                        reason: proto::KillReason::DeadlineExceeded as i32,
                    })),
                }))
                .await?;
            let WorkerKind::Completed(completed) = tokio::time::timeout(
                Duration::from_secs(5),
                next_non_keepalive(&mut upload.session.inbound_rx),
            )
            .await??
            else {
                return Err("expected Completed after upload cancellation".into());
            };
            assert_eq!(completed.attempt, upload.attempt);
            let Some(proto::completed::Outcome::Status(status)) = completed.outcome else {
                return Err("cancelled upload published an ActionResult".into());
            };
            assert_eq!(status.code, rpc_proto::Code::DeadlineExceeded as i32);
            tokio::time::timeout(Duration::from_secs(5), upload.content.cancelled.notified())
                .await?;
            assert!(!upload.spec.work_directory.join(upload.attempt).exists());
            assert_eq!(upload.actions.put_calls(), 0);
            let _ = upload.stop.send(());
            tokio::time::timeout(Duration::from_secs(5), upload.worker).await???;
            upload.server.abort();
            Ok(())
        })
    }

    #[test]
    fn disconnect_cancels_upload_before_replacement_session_acknowledges() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let mut upload = start_blocked_upload(&tmp.path().canonicalize()?).await?;
            drop(upload.session);
            let (replacement, _) = tokio::time::timeout(
                Duration::from_secs(5),
                expect_register_and_ack(
                    &mut upload.sessions,
                    "inst-e5-00000000-0000-4000-8000-000000000025",
                ),
            )
            .await??;
            tokio::time::timeout(Duration::from_secs(5), upload.content.cancelled.notified())
                .await?;
            assert!(!upload.spec.work_directory.join(upload.attempt).exists());
            assert_eq!(upload.actions.put_calls(), 0);
            let _ = upload.stop.send(());
            tokio::time::timeout(Duration::from_secs(5), upload.worker).await???;
            drop(replacement);
            upload.server.abort();
            Ok(())
        })
    }

    #[test]
    fn direct_end_to_end_assign_accept_executed_before_completed_and_graveyard() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            fs::create_dir_all(root.join("work/.graveyard/old"))?;
            fs::write(root.join("work/stale-file"), b"old")?;

            let spec = test_spec(&root, WorkerLifetime::LongLived, true)?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let (action_digest, action_bytes, cmd_bytes) = seed_action_in_store(
                &content,
                WRITE_OUTPUT_CHILD,
                Vec::new(),
                vec!["result.txt".to_owned()],
                vec![("input.txt".to_owned(), Bytes::from_static(b"in"))],
            )
            .await?;

            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            let worker_handle = spawn_direct_worker(
                spec.clone(),
                Arc::clone(&content),
                Arc::clone(&actions),
                addr,
                stop_rx,
            );

            let (mut session, reg) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000001",
            )
            .await?;
            assert_eq!(reg.instance, "inst-e5");
            assert!(!reg.single_use);
            assert!(!root.join("work/stale-file").exists());
            assert!(!root.join("work/.graveyard/old").exists());

            let attempt_id = "00000000-0000-4000-8000-000000000101";
            session
                .outbound_tx
                .send(Ok(assign_frame(
                    attempt_id,
                    action_digest,
                    Some(action_bytes),
                    Some(cmd_bytes),
                    1024,
                )?))
                .await?;

            let action_result =
                expect_accept_executed_completed(&mut session.inbound_rx, attempt_id).await?;
            assert_output_and_cached(&content, &actions, action_digest, &action_result).await?;
            assert!(!spec.work_directory.join(attempt_id).exists());

            let _ = stop_tx.send(());
            worker_handle.await??;
            assert!(
                !spec
                    .work_directory
                    .join(".graveyard")
                    .join(attempt_id)
                    .exists()
            );
            assert!(matches!(
                next_non_keepalive(&mut session.inbound_rx).await?,
                WorkerKind::GoingAway(_)
            ));
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn spools_and_uploads_complete_output_beyond_old_truncation_limit() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let spec = test_spec(&root, WorkerLifetime::LongLived, false)?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let length = 17 * 1024 * 1024 + 7;
            let (digest, action, command) = seed_action_in_store(
                &content,
                WRITE_OUTPUT_CHILD,
                vec![reapi::command::EnvironmentVariable {
                    name: "STDERR_BYTES".into(),
                    value: length.to_string(),
                }],
                vec!["result.txt".into()],
                Vec::new(),
            )
            .await?;
            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel();
            let worker = spawn_direct_worker(
                spec.clone(),
                Arc::clone(&content),
                Arc::clone(&actions),
                addr,
                stop_rx,
            );
            let (mut session, _) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000021",
            )
            .await?;
            let attempt = "00000000-0000-4000-8000-000000000888";
            session
                .outbound_tx
                .send(Ok(assign_frame(
                    attempt,
                    digest,
                    Some(action),
                    Some(command),
                    32 * 1024 * 1024,
                )?))
                .await?;
            let result = expect_accept_executed_completed(&mut session.inbound_rx, attempt).await?;
            let stderr = from_proto(
                result
                    .stderr_digest
                    .as_ref()
                    .ok_or("missing stderr digest")?,
            )?;
            let expected = Bytes::from(vec![b'x'; length]);
            assert_eq!(stderr, Digest::from_bytes(&expected));
            assert_eq!(content.get_bytes(stderr), Some(expected));
            assert_output_and_cached(&content, &actions, digest, &result).await?;
            assert!(
                !spec.work_directory.join(attempt).exists(),
                "attempt spool was not retired"
            );
            let _ = stop_tx.send(());
            worker.await??;
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn disconnect_drains_old_process_before_replacement_registers() -> TestResult {
        run_gated(async {
            use tokio::io::AsyncReadExt as _;
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let spec = test_spec(&root, WorkerLifetime::LongLived, false)?;
            let readiness = TcpListener::bind("127.0.0.1:0").await?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let (digest, action, command) = seed_action_in_store(
                &content,
                SLEEP_FOREVER_CHILD,
                vec![reapi::command::EnvironmentVariable {
                    name: "STARTED_SOCKET_ADDR".into(),
                    value: readiness.local_addr()?.to_string(),
                }],
                Vec::new(),
                Vec::new(),
            )
            .await?;
            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel();
            let worker = spawn_direct_worker(spec.clone(), content, actions, addr, stop_rx);
            let (mut first, _) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000022",
            )
            .await?;
            let attempt = "00000000-0000-4000-8000-000000000889";
            first
                .outbound_tx
                .send(Ok(assign_frame(
                    attempt,
                    digest,
                    Some(action),
                    Some(command),
                    128,
                )?))
                .await?;
            assert!(matches!(
                next_non_keepalive(&mut first.inbound_rx).await?,
                WorkerKind::Accept(_)
            ));
            let (mut ready, _) = readiness.accept().await?;
            let mut pid_bytes = [0; 4];
            ready.read_exact(&mut pid_bytes).await?;
            let pid = rustix::process::Pid::from_raw(i32::try_from(u32::from_le_bytes(pid_bytes))?)
                .ok_or("invalid child pid")?;
            drop(first);
            let (second, _) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000023",
            )
            .await?;
            assert_eq!(
                rustix::process::test_kill_process(pid),
                Err(rustix::io::Errno::SRCH),
                "old process survived replacement registration"
            );
            assert!(
                !spec.work_directory.join(attempt).exists(),
                "old attempt directory survived replacement registration"
            );
            let _ = stop_tx.send(());
            worker.await??;
            drop(second);
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn via_controller_uploads_outputs_and_caches_action_result() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let spec = test_spec(&root, WorkerLifetime::SingleUse, false)?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let (action_digest, _, _) = seed_action_in_store(
                &content,
                WRITE_OUTPUT_CHILD,
                Vec::new(),
                vec!["result.txt".to_owned()],
                vec![("in.txt".to_owned(), Bytes::from_static(b"data"))],
            )
            .await?;

            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(
                controller,
                Some((Arc::clone(&content), Arc::clone(&actions))),
            )
            .await?;

            let worker_handle = tokio::spawn({
                let spec = spec.clone();
                async move {
                    run_worker::<_, MemoryContentStore, MemoryActionStore, SystemClock>(
                        &spec,
                        WorkerStores::ViaController,
                        endpoint_connector(addr),
                        &SystemClock::new(),
                        std::future::pending(),
                    )
                    .await
                }
            });

            let (mut session, reg) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000002",
            )
            .await?;
            assert!(reg.single_use);
            assert_eq!(reg.max_inflight, 1);

            let attempt_1 = "00000000-0000-4000-8000-000000000201";
            session
                .outbound_tx
                .send(Ok(assign_frame(attempt_1, action_digest, None, None, 512)?))
                .await?;
            assert!(matches!(
                next_non_keepalive(&mut session.inbound_rx).await?,
                WorkerKind::Accept(_)
            ));

            let attempt_2 = "00000000-0000-4000-8000-000000000202";
            session
                .outbound_tx
                .send(Ok(assign_frame(attempt_2, action_digest, None, None, 512)?))
                .await?;

            let (mut saw_decline, mut saw_executed, mut saw_completed) = (false, false, false);
            for _ in 0..3 {
                match next_non_keepalive(&mut session.inbound_rx).await? {
                    WorkerKind::Decline(decline) => {
                        assert_eq!(decline.attempt, attempt_2);
                        assert_eq!(decline.reason, proto::DeclineReason::Draining as i32);
                        saw_decline = true;
                    }
                    WorkerKind::Executed(executed) => {
                        assert_eq!(executed.attempt, attempt_1);
                        assert_eq!(executed.exit_code, 0);
                        saw_executed = true;
                    }
                    WorkerKind::Completed(completed) => {
                        assert!(saw_executed, "Executed must arrive before Completed");
                        assert_eq!(completed.attempt, attempt_1);
                        let Some(proto::completed::Outcome::Result(res)) = completed.outcome else {
                            return Err("expected ActionResult".into());
                        };
                        assert_output_and_cached(&content, &actions, action_digest, &res).await?;
                        saw_completed = true;
                    }
                    other => return Err(format!("unexpected frame: {other:?}").into()),
                }
            }
            assert!(saw_decline && saw_executed && saw_completed);

            worker_handle.await??;
            assert!(!spec.work_directory.join(attempt_1).exists());
            assert!(
                !spec
                    .work_directory
                    .join(".graveyard")
                    .join(attempt_1)
                    .exists()
            );
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn draining_and_disk_shortage_decline_cleanly() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let spec = test_spec(&root, WorkerLifetime::LongLived, false)?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let (action_digest, action_bytes, cmd_bytes) = seed_action_in_store(
                &content,
                WRITE_OUTPUT_CHILD,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .await?;

            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            let worker_handle = spawn_direct_worker(spec, content, actions, addr, stop_rx);

            let mut session = sessions.recv().await.ok_or("missing session")?;
            let _ = next_non_keepalive(&mut session.inbound_rx).await?;
            session
                .outbound_tx
                .send(Ok(registered_frame(
                    "inst-e5-00000000-0000-4000-8000-000000000003",
                )))
                .await?;

            session
                .outbound_tx
                .send(Ok(proto::ControllerMessage {
                    kind: Some(ControllerKind::SetDraining(proto::SetDraining {
                        draining: true,
                    })),
                }))
                .await?;
            let declined = "00000000-0000-4000-8000-000000000301";
            session
                .outbound_tx
                .send(Ok(assign_frame(
                    declined,
                    action_digest,
                    Some(action_bytes.clone()),
                    Some(cmd_bytes.clone()),
                    128,
                )?))
                .await?;
            let WorkerKind::Decline(decline) = next_non_keepalive(&mut session.inbound_rx).await?
            else {
                return Err("expected Decline(Draining)".into());
            };
            assert_eq!(decline.attempt, declined);
            assert_eq!(decline.reason, proto::DeclineReason::Draining as i32);

            session
                .outbound_tx
                .send(Ok(proto::ControllerMessage {
                    kind: Some(ControllerKind::SetDraining(proto::SetDraining {
                        draining: false,
                    })),
                }))
                .await?;
            let disk_declined = "00000000-0000-4000-8000-000000000302";
            session
                .outbound_tx
                .send(Ok(assign_frame(
                    disk_declined,
                    action_digest,
                    Some(action_bytes),
                    Some(cmd_bytes),
                    u64::MAX,
                )?))
                .await?;
            let WorkerKind::Decline(disk_decline) =
                next_non_keepalive(&mut session.inbound_rx).await?
            else {
                return Err("expected Decline(DiskShortage)".into());
            };
            assert_eq!(disk_decline.attempt, disk_declined);
            assert_eq!(
                disk_decline.reason,
                proto::DeclineReason::DiskShortage as i32
            );

            let _ = stop_tx.send(());
            worker_handle.await??;
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn kill_terminates_running_attempt_and_retires_dir() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let mut spec = test_spec(&root, WorkerLifetime::LongLived, false)?;
            spec.actions = Arc::new(ActionsConfig {
                kill_grace: NonZeroDuration::from_millis(80),
                ..ActionsConfig::default()
            });
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let started_marker = root.join("child-started");
            let (action_digest, action_bytes, cmd_bytes) = seed_action_in_store(
                &content,
                SLEEP_FOREVER_CHILD,
                vec![reapi::command::EnvironmentVariable {
                    name: "STARTED_MARKER_FILE".to_owned(),
                    value: started_marker.to_string_lossy().into_owned(),
                }],
                Vec::new(),
                Vec::new(),
            )
            .await?;

            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            let worker_handle = spawn_direct_worker(spec.clone(), content, actions, addr, stop_rx);

            let mut session = sessions.recv().await.ok_or("missing session")?;
            let _ = next_non_keepalive(&mut session.inbound_rx).await?;
            session
                .outbound_tx
                .send(Ok(registered_frame(
                    "inst-e5-00000000-0000-4000-8000-000000000004",
                )))
                .await?;

            let kill_attempt = "00000000-0000-4000-8000-000000000303";
            session
                .outbound_tx
                .send(Ok(assign_frame(
                    kill_attempt,
                    action_digest,
                    Some(action_bytes),
                    Some(cmd_bytes),
                    128,
                )?))
                .await?;
            assert!(matches!(
                next_non_keepalive(&mut session.inbound_rx).await?,
                WorkerKind::Accept(_)
            ));
            for _ in 0..200 {
                if started_marker.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(started_marker.exists(), "sleeping child never started");

            session
                .outbound_tx
                .send(Ok(proto::ControllerMessage {
                    kind: Some(ControllerKind::Kill(proto::Kill {
                        attempt: kill_attempt.to_owned(),
                        reason: proto::KillReason::Cancelled as i32,
                    })),
                }))
                .await?;

            let WorkerKind::Executed(executed) =
                next_non_keepalive(&mut session.inbound_rx).await?
            else {
                return Err("expected Executed after Kill".into());
            };
            assert_eq!(executed.attempt, kill_attempt);
            assert_eq!(
                executed.kill.as_ref().map(|k| k.reason),
                Some(proto::KillReason::Cancelled as i32)
            );

            let WorkerKind::Completed(completed) =
                next_non_keepalive(&mut session.inbound_rx).await?
            else {
                return Err("expected Completed after Kill".into());
            };
            assert_eq!(completed.attempt, kill_attempt);
            assert!(matches!(
                completed.outcome,
                Some(proto::completed::Outcome::Status(_))
            ));
            assert!(!spec.work_directory.join(kill_attempt).exists());

            let _ = stop_tx.send(());
            worker_handle.await??;
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn rehome_waits_for_active_attempts_and_opens_new_session() -> TestResult {
        run_gated(async {
            let tmp = TempDir::new()?;
            let root = tmp.path().canonicalize()?;
            let spec = test_spec(&root, WorkerLifetime::LongLived, false)?;
            let content = Arc::new(MemoryContentStore::new());
            let actions = Arc::new(MemoryActionStore::new());
            let (action_digest, action_bytes, cmd_bytes) = seed_action_in_store(
                &content,
                WRITE_OUTPUT_CHILD,
                Vec::new(),
                vec!["result.txt".to_owned()],
                Vec::new(),
            )
            .await?;

            let (controller, mut sessions) = MockController::new();
            let (addr, server) = spawn_mock_server(controller, None).await?;
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            let connect_count = Arc::new(AtomicUsize::new(0));
            let uri = format!("http://{addr}");
            let connector = {
                let connect_count = Arc::clone(&connect_count);
                move || {
                    let uri = uri.clone();
                    let count = Arc::clone(&connect_count);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
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
            };

            let worker_handle = tokio::spawn(async move {
                run_worker(
                    &spec,
                    WorkerStores::Direct { content, actions },
                    connector,
                    &SystemClock::new(),
                    async move {
                        let _ = stop_rx.await;
                    },
                )
                .await
            });

            let (first_session, _) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000010",
            )
            .await?;
            first_session
                .outbound_tx
                .send(Ok(proto::ControllerMessage {
                    kind: Some(ControllerKind::Rehome(proto::Rehome {})),
                }))
                .await?;

            let (mut second_session, _) = expect_register_and_ack(
                &mut sessions,
                "inst-e5-00000000-0000-4000-8000-000000000011",
            )
            .await?;
            assert!(connect_count.load(Ordering::SeqCst) >= 2);
            let attempt = "00000000-0000-4000-8000-000000000401";
            second_session
                .outbound_tx
                .send(Ok(assign_frame(
                    attempt,
                    action_digest,
                    Some(action_bytes),
                    Some(cmd_bytes),
                    256,
                )?))
                .await?;

            assert!(matches!(
                next_non_keepalive(&mut second_session.inbound_rx).await?,
                WorkerKind::Accept(_)
            ));
            assert!(matches!(
                next_non_keepalive(&mut second_session.inbound_rx).await?,
                WorkerKind::Executed(_)
            ));
            assert!(matches!(
                next_non_keepalive(&mut second_session.inbound_rx).await?,
                WorkerKind::Completed(_)
            ));

            let _ = stop_tx.send(());
            worker_handle.await??;
            server.abort();
            Ok(())
        })
    }

    #[test]
    fn effective_action_timeout_honors_client_below_cap_and_clamps_above() {
        let cap = Duration::from_hours(1);
        assert_eq!(effective_action_timeout(None, cap), cap);
        assert_eq!(
            effective_action_timeout(
                Some(&prost_types::Duration {
                    seconds: 0,
                    nanos: 0
                }),
                cap
            ),
            cap
        );
        assert_eq!(
            effective_action_timeout(
                Some(&prost_types::Duration {
                    seconds: 300,
                    nanos: 0
                }),
                cap
            ),
            Duration::from_mins(5)
        );
        assert_eq!(
            effective_action_timeout(
                Some(&prost_types::Duration {
                    seconds: 7200,
                    nanos: 0
                }),
                cap
            ),
            cap
        );
    }
}
