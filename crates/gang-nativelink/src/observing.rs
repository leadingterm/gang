//! A worker scheduler that counts and observes completed actions.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::{
    ControllerDrain, DrainingWorker, Executed, ExecutionTimes, FleetSupply, InstanceCordon,
    Observation, ObservationSink, Phase, PortError,
};
use gang_types::{FleetCatalog, FleetName, Identity, InstanceId};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ActionResourceUsage, CacheStats, WorkerLoad,
};
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::{Worker, WorkerTimestamp};
use nativelink_scheduler::worker_scheduler::{
    ResourceCapacitySnapshot, WorkerScheduler, WorkerSummary,
};
use nativelink_util::action_messages::{
    ActionInfo as NativeActionInfo, ActionResult, ActionStage, ExecutionMetadata, OperationId,
    WorkerId,
};
use nativelink_util::operation_state_manager::UpdateOperationType;
use nativelink_util::operation_state_manager::{
    ClientStateManager, OperationFilter, OperationStageFlags,
};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::shutdown_guard::ShutdownGuard;
use parking_lot::Mutex;
use tokio::sync::{RwLock, watch};

use crate::acquisition::Acquisition;
use crate::actions::digest_text;
use crate::capacity::RuntimeCatalog;
use crate::cordons::InstanceCordons;
use crate::durations::ActionDurations;
use crate::handoff::HandOff;
use crate::observer::origin;
use crate::source_rates::{self, SourceRates};

/// Forwards every call to the inner worker scheduler and counts the actions that reach a
/// terminal result on a worker. Only a successful `Completed` update counts: errors, keep-alives,
/// declines, disconnects and results served from the cache do not. Once the hand-off begins, it
/// refuses new and undrained workers and releases each worker when it falls idle.
pub(crate) struct ObservingWorkerScheduler {
    inner: Arc<dyn WorkerScheduler>,
    operations: Arc<dyn ClientStateManager>,
    completed: Arc<AtomicU64>,
    sink: ObservationSink,
    handoff: HandOff,
    durations: Arc<Mutex<ActionDurations>>,
    clock: Arc<dyn Clock>,
    fleets: Arc<RuntimeCatalog>,
    rates: Arc<SourceRates>,
    cordons: InstanceCordons,
    retiring: RwLock<bool>,
}

impl ObservingWorkerScheduler {
    /// Wraps the worker scheduler, counting and observing completed actions.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: Arc<dyn WorkerScheduler>,
        operations: Arc<dyn ClientStateManager>,
        completed: Arc<AtomicU64>,
        sink: ObservationSink,
        handoff: HandOff,
        durations: Arc<Mutex<ActionDurations>>,
        clock: Arc<dyn Clock>,
        fleets: &Arc<FleetCatalog>,
        runtime: Arc<RuntimeCatalog>,
        rates: Arc<SourceRates>,
    ) -> Self {
        Self {
            inner,
            operations,
            completed,
            sink,
            handoff,
            durations,
            clock,
            cordons: InstanceCordons::new(Arc::clone(fleets)),
            fleets: runtime,
            rates,
            retiring: RwLock::new(false),
        }
    }

    /// A physical controller command, separate from worker-owned self-drain flags.
    pub(crate) async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
        acquisition: &Acquisition<'_>,
    ) -> Result<(), PortError> {
        let retiring = acquisition.wait(self.retiring.read()).await?;
        if *retiring && !draining {
            return Err(PortError::backend(make_err!(
                Code::Unavailable,
                "the controller is handing off"
            )));
        }
        self.cordons
            .set_draining(&*self.inner, fleet, instance, draining, acquisition)
            .await
    }

    pub(crate) async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
        acquisition: &Acquisition<'_>,
    ) -> Result<(), PortError> {
        self.cordons.retain(now, grace, live, acquisition).await
    }

    pub(crate) async fn cordons_into(&self, into: &mut Vec<InstanceCordon>) {
        self.cordons.snapshot_into(into).await;
    }

    async fn forget_if_disconnected(&self, worker: &WorkerId, removed: bool) {
        if removed
            || !self
                .inner
                .worker_snapshot()
                .await
                .iter()
                .any(|row| row.id == worker.0)
        {
            self.cordons.disconnected(worker).await;
        }
    }

    /// Stops worker admission, freezes every worker, then announces the irreversible handoff.
    pub(crate) async fn begin_hand_off(&self) -> Result<(), PortError> {
        let mut retiring = self.retiring.write().await;
        *retiring = true;
        if !self.handoff.is_begun() {
            for worker in self.inner.worker_snapshot().await {
                self.inner
                    .set_drain_worker(&WorkerId::from(worker.id), true)
                    .await
                    .map_err(PortError::backend)?;
            }
            self.handoff.begin();
        }
        drop(retiring);
        Ok(())
    }

    pub(crate) async fn refresh_drain(&self, into: &mut ControllerDrain) -> Result<(), PortError> {
        let now = self.clock.wall();
        self.inner
            .expire_attempts(std::time::UNIX_EPOCH + Duration::from_millis(now.as_unix_millis()))
            .await
            .map_err(PortError::backend)?;
        into.workers.clear();
        let mut attempts = false;
        for worker in self.inner.worker_snapshot().await {
            if worker.running_actions == 0 {
                let id = WorkerId::from(worker.id);
                self.inner
                    .remove_worker(&id)
                    .await
                    .map_err(PortError::backend)?;
                self.cordons.disconnected(&id).await;
                continue;
            }
            let Some(deadline) = worker.latest_attempt_deadline else {
                return Err(PortError::backend(
                    "owned worker attempt has no controller dispatch deadline",
                ));
            };
            if Timestamp::from(deadline) <= now {
                let id = WorkerId::from(worker.id);
                self.inner
                    .remove_worker(&id)
                    .await
                    .map_err(PortError::backend)?;
                self.cordons.disconnected(&id).await;
                continue;
            }
            attempts = true;
            let Some(instance) = gang_types::WorkerId::instance_name_from_str(&worker.id)
                .and_then(|name| InstanceId::try_new(name).ok())
            else {
                continue;
            };
            let Some(fleet) = worker
                .platform_properties
                .get(gang_types::property::FLEET)
                .and_then(|name| FleetName::try_new(name).ok())
            else {
                continue;
            };
            let deadline = Timestamp::from(deadline);
            into.workers.push(DrainingWorker {
                fleet,
                instance,
                deadline,
            });
        }
        into.workers
            .sort_unstable_by(|a, b| (&a.fleet, &a.instance).cmp(&(&b.fleet, &b.instance)));
        into.workers.dedup_by(|later, earlier| {
            if later.fleet != earlier.fleet || later.instance != earlier.instance {
                return false;
            }
            earlier.deadline = earlier.deadline.max(later.deadline);
            true
        });
        into.complete = !attempts && self.handoff.public_complete();
        self.signal_if_released().await;
        Ok(())
    }

    async fn remove_if_retiring_and_idle(&self, worker_id: &WorkerId) {
        let retiring = *self.retiring.read().await;
        if retiring
            && self
                .inner
                .worker_snapshot()
                .await
                .into_iter()
                .any(|worker| worker.id == worker_id.0 && worker.running_actions == 0)
        {
            if let Err(error) = self.inner.remove_worker(worker_id).await {
                tracing::warn!(?error, worker = %worker_id.0, "could not remove idle worker during hand-off");
            } else {
                self.cordons.disconnected(worker_id).await;
            }
        }
        self.signal_if_released().await;
    }

    async fn signal_if_released(&self) {
        let retiring = *self.retiring.read().await;
        if retiring && self.inner.worker_snapshot().await.is_empty() {
            self.handoff.release_workers();
        }
    }

    /// Builds an observation from the retained operation and its final result.
    fn executed(
        worker_id: &WorkerId,
        info: &NativeActionInfo,
        metadata: Option<&OriginMetadata>,
        result: &ActionResult,
    ) -> Option<Executed> {
        let OriginMetadata {
            identity,
            bazel_metadata,
        } = metadata?;
        let principal = Arc::new(identity.parse::<Identity>().ok()?);
        Some(Executed {
            origin: origin(principal, bazel_metadata.as_ref()?)?,
            digest: digest_text(info.digest()),
            worker: gang_types::WorkerId::try_new(&worker_id.0).ok(),
            exit_code: result.exit_code,
            error: result.error.is_some(),
            times: execution_times(&result.execution_metadata),
        })
    }
}

/// Converts the worker's phase timestamps into Gang's clock representation.
fn execution_times(metadata: &ExecutionMetadata) -> ExecutionTimes {
    ExecutionTimes {
        queued: Phase::reported(
            Timestamp::from(metadata.queued_timestamp),
            Timestamp::from(metadata.worker_start_timestamp),
        ),
        input_fetch: Phase::reported(
            Timestamp::from(metadata.input_fetch_start_timestamp),
            Timestamp::from(metadata.input_fetch_completed_timestamp),
        ),
        executing: Phase::reported(
            Timestamp::from(metadata.execution_start_timestamp),
            Timestamp::from(metadata.execution_completed_timestamp),
        ),
        output_upload: Phase::reported(
            Timestamp::from(metadata.output_upload_start_timestamp),
            Timestamp::from(metadata.output_upload_completed_timestamp),
        ),
        whole: Phase::reported(
            Timestamp::from(metadata.queued_timestamp),
            Timestamp::from(metadata.worker_completed_timestamp),
        ),
    }
}

impl MetricsComponent for ObservingWorkerScheduler {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MetricsComponent::publish(&*self.inner, kind, field_metadata)
    }
}

impl RootMetricsComponent for ObservingWorkerScheduler {}

// Fails clippy if a defaulted `WorkerScheduler` method is not forwarded to `inner`.
#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl WorkerScheduler for ObservingWorkerScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.inner.get_platform_property_manager()
    }

    fn resource_capacity_updates(&self) -> Option<watch::Receiver<Arc<ResourceCapacitySnapshot>>> {
        self.inner.resource_capacity_updates()
    }

    async fn add_worker(&self, mut worker: Worker) -> Result<(), Error> {
        let retiring = self.retiring.read().await;
        if *retiring {
            return Err(make_err!(
                Code::Unavailable,
                "the controller is handing off"
            ));
        }
        let _admission = self.cordons.admission(&*self.inner, &mut worker).await?;
        self.inner.add_worker(worker).await
    }

    async fn worker_dispatch_accepted(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
    ) -> Result<(), Error> {
        self.inner
            .worker_dispatch_accepted(worker_id, operation_id)
            .await
    }

    async fn worker_dispatch_declined(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        reason: String,
        needs_kb: Option<u64>,
    ) -> Result<(), Error> {
        let result = self
            .inner
            .worker_dispatch_declined(worker_id, operation_id, reason, needs_kb)
            .await;
        if result.is_ok() {
            self.remove_if_retiring_and_idle(worker_id).await;
        }
        result
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        let terminal = matches!(
            update,
            UpdateOperationType::UpdateWithActionStage(
                ActionStage::Completed(_) | ActionStage::CompletedFromCache(_)
            ) | UpdateOperationType::UpdateWithError(_)
                | UpdateOperationType::UpdateWithDisconnect
                | UpdateOperationType::UpdateWithDecline(_)
        );
        let sent = match &update {
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)) => {
                Some((result.exit_code, result.execution_metadata.clone()))
            }
            _ => None,
        };
        let operation = if sent.is_some() {
            match self
                .operations
                .filter_operations(OperationFilter {
                    operation_id: Some(operation_id.clone()),
                    stages: OperationStageFlags::Any.difference(OperationStageFlags::Completed),
                    ..Default::default()
                })
                .await
            {
                Ok(mut operations) => operations.next().await,
                Err(_) => None,
            }
        } else {
            None
        };
        let result = self
            .inner
            .update_action(worker_id, operation_id, update)
            .await;
        if result.is_ok()
            && let Some(operation) = operation
            && let Ok((state, origin)) = operation.as_state().await
            && let ActionStage::Completed(completed) = &state.stage
            && let Some((exit_code, metadata)) = sent
            && completed.exit_code == exit_code
            && completed.execution_metadata == metadata
        {
            self.completed.fetch_add(1, Ordering::Relaxed);
            let action = operation.as_action_info().await.ok();
            let fleets = self.fleets.current()?;
            if let Some((info, _)) = &action
                && let Some(preferred) = crate::snapshot::accepting(info, &fleets).iter().next()
            {
                let source = self.rates.source(origin.as_ref());
                self.rates.record(
                    source,
                    preferred,
                    crate::snapshot::resources(&info.platform_properties),
                    source_rates::timing(&completed.execution_metadata),
                );
            }
            if let Some(metadata) = origin.and_then(|origin| origin.bazel_metadata)
                && !metadata.action_mnemonic.is_empty()
                && completed.execution_metadata.worker_start_timestamp
                    != std::time::SystemTime::UNIX_EPOCH
                && completed.execution_metadata.worker_completed_timestamp
                    != std::time::SystemTime::UNIX_EPOCH
            {
                let elapsed = completed
                    .execution_metadata
                    .worker_completed_timestamp
                    .duration_since(completed.execution_metadata.worker_start_timestamp)
                    .unwrap_or_default();
                let slot_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                self.durations.lock().record(
                    &metadata.target_id,
                    &metadata.action_mnemonic,
                    slot_ms,
                    self.clock.wall(),
                );
            }
            if let Some((info, metadata)) = &action
                && let Some(executed) =
                    Self::executed(worker_id, info, metadata.as_ref(), completed)
            {
                self.sink.record(Observation::Executed(executed));
            }
        }
        if terminal && result.is_ok() {
            self.remove_if_retiring_and_idle(worker_id).await;
        }
        result
    }

    async fn record_action_resource_usage(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        resource_usage: ActionResourceUsage,
    ) -> Result<(), Error> {
        self.inner
            .record_action_resource_usage(worker_id, operation_id, resource_usage)
            .await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
        load: Option<WorkerLoad>,
        cache_stats: Option<CacheStats>,
    ) -> Result<(), Error> {
        self.inner
            .worker_keep_alive_received(worker_id, timestamp, load, cache_stats)
            .await
    }

    async fn worker_running_operations_reported(
        &self,
        worker_id: &WorkerId,
        operation_ids: Vec<OperationId>,
    ) -> Result<(), Error> {
        let result = self
            .inner
            .worker_running_operations_reported(worker_id, operation_ids)
            .await;
        if result.is_ok() {
            self.remove_if_retiring_and_idle(worker_id).await;
        }
        result
    }

    async fn worker_liveness_refreshed(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.inner
            .worker_liveness_refreshed(worker_id, timestamp)
            .await
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let result = self.inner.remove_worker(worker_id).await;
        self.forget_if_disconnected(worker_id, result.is_ok()).await;
        self.signal_if_released().await;
        result
    }

    async fn worker_disconnected(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let result = self.inner.worker_disconnected(worker_id).await;
        self.forget_if_disconnected(worker_id, result.is_ok()).await;
        self.signal_if_released().await;
        result
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        self.inner.shutdown(shutdown_guard).await;
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let result = self.inner.remove_timedout_workers(now_timestamp).await;
        self.signal_if_released().await;
        result
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        let retiring = self.retiring.read().await;
        if *retiring && !is_draining {
            return Err(make_err!(
                Code::Unavailable,
                "the controller is handing off"
            ));
        }
        self.cordons
            .set_worker_draining(&*self.inner, worker_id, is_draining)
            .await
    }

    async fn expire_attempts(&self, now: std::time::SystemTime) -> Result<(), Error> {
        self.inner.expire_attempts(now).await
    }

    async fn worker_snapshot(&self) -> Vec<WorkerSummary> {
        self.inner.worker_snapshot().await
    }

    async fn kill_revoked_operations(&self) -> Result<(), Error> {
        self.inner.kill_revoked_operations().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, SystemTime};

    use gang_clock::TestClock;
    use nativelink_error::{Code, make_err};
    use nativelink_proto::build::bazel::remote::execution::v2::ActionResult as ProtoActionResult;
    use nativelink_util::action_messages::{
        ActionInfo, ActionResult, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::operation_state_manager::{ActionStateResult, ActionStateResultStream};

    struct NoOperations;
    impl MetricsComponent for NoOperations {
        fn publish(
            &self,
            _: MetricKind,
            _: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }
    #[async_trait]
    impl ClientStateManager for NoOperations {
        async fn add_action(
            &self,
            _: OperationId,
            _: Arc<ActionInfo>,
        ) -> Result<Box<dyn ActionStateResult>, Error> {
            Err(make_err!(Code::Internal, "not implemented"))
        }
        async fn filter_operations(
            &self,
            _: OperationFilter,
        ) -> Result<ActionStateResultStream<'_>, Error> {
            Err(make_err!(Code::Internal, "read failed"))
        }
    }

    struct Operation {
        info: Arc<ActionInfo>,
        stage: Arc<parking_lot::Mutex<Option<ActionStage>>>,
        origin: Option<OriginMetadata>,
    }
    #[async_trait]
    impl ActionStateResult for Operation {
        async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            let stage = self
                .stage
                .lock()
                .clone()
                .ok_or_else(|| make_err!(Code::Internal, "not forwarded"))?;
            Ok((
                Arc::new(ActionState {
                    stage,
                    last_transition_timestamp: SystemTime::UNIX_EPOCH,
                    client_operation_id: OperationId::String("op-1".to_owned()),
                    action_digest: self.info.digest(),
                }),
                self.origin.clone(),
            ))
        }
        async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Err(make_err!(Code::Internal, "not implemented"))
        }
        async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
            Ok((Arc::clone(&self.info), self.origin.clone()))
        }
    }
    struct Operations {
        info: Arc<ActionInfo>,
        stage: Arc<parking_lot::Mutex<Option<ActionStage>>>,
        origin: Option<OriginMetadata>,
    }
    impl MetricsComponent for Operations {
        fn publish(
            &self,
            _: MetricKind,
            _: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }
    #[async_trait]
    impl ClientStateManager for Operations {
        async fn add_action(
            &self,
            _: OperationId,
            _: Arc<ActionInfo>,
        ) -> Result<Box<dyn ActionStateResult>, Error> {
            Err(make_err!(Code::Internal, "not implemented"))
        }
        async fn filter_operations(
            &self,
            filter: OperationFilter,
        ) -> Result<ActionStateResultStream<'_>, Error> {
            assert_eq!(
                filter.operation_id,
                Some(OperationId::String("op-1".to_owned()))
            );
            assert_eq!(
                filter.stages,
                OperationStageFlags::Any.difference(OperationStageFlags::Completed)
            );
            if matches!(*self.stage.lock(), Some(ActionStage::Completed(_))) {
                return Ok(Box::pin(futures::stream::empty()));
            }
            assert!(
                self.stage.lock().is_none(),
                "operation was removed after forwarding"
            );
            let row = Box::new(Operation {
                info: Arc::clone(&self.info),
                stage: Arc::clone(&self.stage),
                origin: self.origin.clone(),
            }) as Box<dyn ActionStateResult>;
            Ok(Box::pin(futures::stream::iter([row])))
        }
    }

    fn test_origin(identity: &str) -> OriginMetadata {
        OriginMetadata {
            identity: identity.to_owned(),
            bazel_metadata: Some(
                nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata {
                    tool_invocation_id: "build".to_owned(),
                    target_id: "//pkg:target".to_owned(),
                    action_mnemonic: "Compile".to_owned(),
                    ..Default::default()
                },
            ),
        }
    }

    fn test_durations() -> Arc<Mutex<ActionDurations>> {
        Arc::new(Mutex::new(ActionDurations::new(
            std::num::NonZeroUsize::MIN.saturating_add(9),
            gang_config::HistoryWindow::from_units(7),
        )))
    }

    fn test_clock() -> Arc<dyn Clock> {
        Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000_000_000)))
    }
    fn test_fleets() -> Arc<FleetCatalog> {
        Arc::new(crate::snapshot::tests::catalog())
    }

    fn test_rates() -> Arc<SourceRates> {
        Arc::new(
            SourceRates::new(&gang_config::ScalingConfig::default(), 2, test_clock())
                .expect("bounded test table"),
        )
    }

    fn test_action_info() -> Arc<ActionInfo> {
        let digest = DigestInfo::new([3; 32], 30);
        Arc::new(ActionInfo {
            memory_reservation: None,
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: HashMap::from([(
                gang_types::property::ACTION_ENVIRONMENT.to_owned(),
                "host:test".to_owned(),
            )]),
            pre_fit_reservation: None,
            priority: 0,
            load_timestamp: SystemTime::UNIX_EPOCH,
            insert_timestamp: SystemTime::UNIX_EPOCH,
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest,
            }),
        })
    }

    /// A completed action with reported execution phase timestamps.
    fn completed_result(t: SystemTime) -> ActionResult {
        ActionResult {
            exit_code: 7,
            execution_metadata: ExecutionMetadata {
                queued_timestamp: t,
                worker_start_timestamp: t + Duration::from_secs(4),
                input_fetch_start_timestamp: t + Duration::from_secs(5),
                input_fetch_completed_timestamp: t + Duration::from_secs(6),
                execution_start_timestamp: t + Duration::from_secs(7),
                execution_completed_timestamp: t + Duration::from_secs(8),
                output_upload_start_timestamp: t + Duration::from_secs(9),
                output_upload_completed_timestamp: t + Duration::from_secs(10),
                worker_completed_timestamp: t + Duration::from_secs(11),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Completion cases as (`fail`, `read_fails`, `requeue`).
    const COMPLETION_CASES: [(bool, bool, bool); 4] = [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ];

    #[tokio::test]
    async fn a_completed_update_records_its_execution() {
        let info = test_action_info();
        let digest = info.digest();
        let worker = WorkerId("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b".to_owned());
        let operation = OperationId::String("op-1".to_owned());
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let result = completed_result(t);
        for (fail, read_fails, requeue) in COMPLETION_CASES {
            let (sink, mut receiver) =
                ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN);
            let completed = Arc::new(AtomicU64::new(0));
            let inner = Arc::new(RecordingScheduler::new(fail));
            inner.requeue.store(requeue, Ordering::Relaxed);
            let operations: Arc<dyn ClientStateManager> = if read_fails {
                Arc::new(NoOperations)
            } else {
                Arc::new(Operations {
                    info: Arc::clone(&info),
                    stage: Arc::clone(&inner.stage),
                    origin: Some(test_origin("local:alice")),
                })
            };
            let scheduler = ObservingWorkerScheduler::new(
                inner,
                operations,
                Arc::clone(&completed),
                sink,
                HandOff::new(),
                test_durations(),
                test_clock(),
                &test_fleets(),
                crate::capacity::test_catalog((*test_fleets()).clone()),
                test_rates(),
            );
            let forwarded = scheduler
                .update_action(
                    &worker,
                    &operation,
                    UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                        result.clone(),
                    )),
                )
                .await;
            assert_eq!(forwarded.is_err(), fail);
            assert_eq!(
                completed.load(Ordering::Relaxed),
                u64::from(!fail && !read_fails && !requeue)
            );
            let mut moments = Vec::new();
            scheduler.rates.snapshot_into(&mut moments);
            assert_eq!(moments.len(), usize::from(!fail && !read_fails && !requeue));
            drop(scheduler);
            let observation = receiver.next().await;
            if fail || read_fails || requeue {
                assert!(observation.is_none());
                continue;
            }
            let observation = observation
                .and_then(gang_spi::Delivery::into_observation)
                .expect("expected observation");
            let Observation::Executed(executed) = observation else {
                panic!("expected execution")
            };
            assert_eq!(
                executed.digest,
                format!("{}/{}", digest.packed_hash(), digest.size_bytes())
            );
            assert_eq!(
                executed.worker.as_ref().map(gang_types::WorkerId::as_str),
                Some(worker.0.as_str())
            );
            assert_eq!(executed.exit_code, 7);
            assert!(!executed.error);
            assert_execution_times(&executed.times, t);
            assert!(receiver.next().await.is_none());
        }
    }

    fn assert_execution_times(times: &ExecutionTimes, t: SystemTime) {
        assert_eq!(
            times.queued,
            Phase::reported(
                Timestamp::from(t),
                Timestamp::from(t + Duration::from_secs(4))
            )
        );
        assert_eq!(
            times.input_fetch,
            Phase::reported(
                Timestamp::from(t + Duration::from_secs(5)),
                Timestamp::from(t + Duration::from_secs(6))
            )
        );
        assert_eq!(
            times.executing,
            Phase::reported(
                Timestamp::from(t + Duration::from_secs(7)),
                Timestamp::from(t + Duration::from_secs(8))
            )
        );
        assert_eq!(
            times.output_upload,
            Phase::reported(
                Timestamp::from(t + Duration::from_secs(9)),
                Timestamp::from(t + Duration::from_secs(10))
            )
        );
        assert_eq!(
            times.whole,
            Phase::reported(
                Timestamp::from(t),
                Timestamp::from(t + Duration::from_secs(11))
            )
        );
    }

    #[tokio::test]
    async fn ignored_completion_never_counts_a_different_result() {
        let worker = WorkerId("w-1".to_owned());
        let operation = OperationId::String("op-1".to_owned());
        let sent = ActionResult {
            exit_code: 7,
            ..Default::default()
        };
        let other = ActionResult {
            exit_code: 8,
            ..Default::default()
        };
        let mut different_metadata = sent.clone();
        different_metadata.execution_metadata.worker = "other-worker".to_owned();
        for (already_completed, other) in [
            (true, other.clone()),
            (false, other),
            (false, different_metadata),
        ] {
            let inner = Arc::new(RecordingScheduler::new(false));
            if already_completed {
                *inner.stage.lock() = Some(ActionStage::Completed(other.clone()));
            } else {
                *inner.replacement.lock() = Some(other.clone());
            }
            let (sink, mut observations) =
                ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN);
            let completed = Arc::new(AtomicU64::new(0));
            let scheduler = ObservingWorkerScheduler::new(
                Arc::clone(&inner) as Arc<dyn WorkerScheduler>,
                Arc::new(Operations {
                    info: test_action_info(),
                    stage: Arc::clone(&inner.stage),
                    origin: Some(test_origin("local:alice")),
                }),
                Arc::clone(&completed),
                sink,
                HandOff::new(),
                test_durations(),
                test_clock(),
                &test_fleets(),
                crate::capacity::test_catalog((*test_fleets()).clone()),
                test_rates(),
            );
            scheduler
                .update_action(
                    &worker,
                    &operation,
                    UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                        sent.clone(),
                    )),
                )
                .await
                .expect("ignored update forwards");
            assert_eq!(completed.load(Ordering::Relaxed), 0);
            let mut moments = Vec::new();
            scheduler.rates.snapshot_into(&mut moments);
            assert!(moments.is_empty());
            drop(scheduler);
            assert!(observations.next().await.is_none());
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn duration_history_only_records_verified_worker_completions() {
        let worker = WorkerId("w-1".to_owned());
        let operation = OperationId::String("op-1".to_owned());
        let now = Timestamp::from_unix_millis(1_000_000_000);
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        for case in 0..17 {
            let inner = Arc::new(RecordingScheduler::new(case == 7));
            let mut origin = Some(test_origin(if case == 1 { "" } else { "local:alice" }));
            if case == 2 {
                origin = None;
            } else if case == 3 {
                origin.as_mut().expect("origin").bazel_metadata = None;
            } else if case == 4 {
                origin
                    .as_mut()
                    .expect("origin")
                    .bazel_metadata
                    .as_mut()
                    .expect("metadata")
                    .action_mnemonic
                    .clear();
            }
            let mut result = completed_result(t);
            if case == 5 {
                result.execution_metadata.worker_start_timestamp = SystemTime::UNIX_EPOCH;
            } else if case == 6 {
                result.execution_metadata.worker_completed_timestamp = SystemTime::UNIX_EPOCH;
            } else if case == 11 {
                result.execution_metadata.worker_completed_timestamp = t;
            }
            if case == 14 {
                result.execution_metadata.worker_completed_timestamp =
                    result.execution_metadata.worker_start_timestamp;
            }
            if case == 8 || case == 12 {
                let mut replacement = result.clone();
                if case == 8 {
                    replacement.exit_code += 1;
                } else {
                    replacement.execution_metadata.worker = "different".to_owned();
                }
                *inner.replacement.lock() = Some(replacement);
            }
            let durations = test_durations();
            let mut info = test_action_info();
            let properties = &mut Arc::make_mut(&mut info).platform_properties;
            properties.insert(
                gang_types::property::CPU_MILLICORES.to_owned(),
                "2000".to_owned(),
            );
            properties.insert(gang_types::property::MEMORY_KB.to_owned(), "4".to_owned());
            properties.insert(gang_types::property::DISK_KB.to_owned(), "8".to_owned());
            if case == 15 {
                properties.insert(
                    gang_types::property::FLEET.to_owned(),
                    "unknown-fleet".to_owned(),
                );
            }
            if case == 16 {
                properties.insert(
                    gang_types::property::ACTION_ENVIRONMENT.to_owned(),
                    "host:old".to_owned(),
                );
            }
            let clock = Arc::new(TestClock::new(now));
            clock.advance(Duration::from_hours(4));
            let rates = Arc::new(
                SourceRates::new(&gang_config::ScalingConfig::default(), 2, clock.clone())
                    .expect("bounded table"),
            );
            let operations: Arc<dyn ClientStateManager> = if case == 13 {
                Arc::new(NoOperations)
            } else {
                Arc::new(Operations {
                    info,
                    stage: Arc::clone(&inner.stage),
                    origin,
                })
            };
            let scheduler = ObservingWorkerScheduler::new(
                Arc::clone(&inner) as Arc<dyn WorkerScheduler>,
                operations,
                Arc::new(AtomicU64::new(0)),
                ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN)
                    .0,
                HandOff::new(),
                Arc::clone(&durations),
                clock.clone(),
                &test_fleets(),
                crate::capacity::test_catalog((*test_fleets()).clone()),
                Arc::clone(&rates),
            );
            let duplicate = result.clone();
            let update = match case {
                9 => UpdateOperationType::UpdateWithActionStage(ActionStage::CompletedFromCache(
                    ProtoActionResult::default(),
                )),
                10 => UpdateOperationType::UpdateWithError(make_err!(Code::Internal, "failed")),
                _ => UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)),
            };
            let forwarded = scheduler.update_action(&worker, &operation, update).await;
            assert_eq!(forwarded.is_err(), case == 7);
            let mut moments = Vec::new();
            scheduler.rates.snapshot_into(&mut moments);
            let verified = matches!(case, 0..=6 | 11 | 14 | 16);
            assert_eq!(moments.len(), usize::from(verified), "case {case}");
            if verified {
                let row = moments[0];
                let kappa = std::f64::consts::LN_2 / 60.0;
                assert_eq!(
                    row.completions.get().to_bits(),
                    kappa.to_bits(),
                    "case {case}"
                );
                assert_eq!(row.at, clock.monotonic(), "case {case}");
                assert_eq!(row.source.is_some(), !matches!(case, 1 | 2), "case {case}");
                let timed = !matches!(case, 5 | 6 | 11);
                assert_eq!(
                    row.timed_completions.get().to_bits(),
                    (if timed { kappa } else { 0.0 }).to_bits(),
                    "case {case}"
                );
                let seconds = if timed && case != 14 { 7.0 } else { 0.0 };
                assert_eq!(
                    row.work.actions.get().to_bits(),
                    (kappa * seconds).to_bits(),
                    "case {case}"
                );
                assert_eq!(
                    row.work.cpu_millicores.get().to_bits(),
                    (kappa * seconds * 2000.0).to_bits(),
                    "case {case}"
                );
                assert_eq!(
                    row.work.memory.get().to_bits(),
                    (kappa * seconds * 4096.0).to_bits(),
                    "case {case}"
                );
                assert_eq!(
                    row.work.disk.get().to_bits(),
                    (kappa * seconds * 8192.0).to_bits(),
                    "case {case}"
                );
                scheduler
                    .update_action(
                        &worker,
                        &operation,
                        UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                            duplicate,
                        )),
                    )
                    .await
                    .expect("duplicate forwards");
                rates.snapshot_into(&mut moments);
                assert_eq!(moments, [row], "duplicate must not add moments");
            }
            let expected = match case {
                0 | 1 | 15 | 16 => Some(Duration::from_secs(7)),
                11 | 14 => Some(Duration::ZERO),
                _ => None,
            };
            assert_eq!(
                durations
                    .lock()
                    .expected("//pkg:target", "Compile", clock.wall()),
                expected,
                "case {case}"
            );
        }
    }

    #[test]
    fn unset_execution_timestamps_do_not_report_phases() {
        let times = execution_times(&ExecutionMetadata::default());
        assert_eq!(times.queued, None);
        assert_eq!(times.input_fetch, None);
        assert_eq!(times.executing, None);
        assert_eq!(times.output_upload, None);
        assert_eq!(times.whole, None);
    }

    use super::*;

    /// Fails `update_action` when asked to, and accepts every other call. With `workers` set, it
    /// serves them as its snapshot, releases a slot per completion and records removals and drains.
    struct RecordingScheduler {
        fail_updates: bool,
        block_first_worker: AtomicBool,
        published: AtomicBool,
        stage: Arc<parking_lot::Mutex<Option<ActionStage>>>,
        requeue: AtomicBool,
        replacement: parking_lot::Mutex<Option<ActionResult>>,
        manager: PlatformPropertyManager,
        workers: parking_lot::Mutex<Option<Vec<WorkerSummary>>>,
        events: parking_lot::Mutex<Vec<String>>,
        admitted_flags: parking_lot::Mutex<Vec<bool>>,
    }

    impl RecordingScheduler {
        fn new(fail_updates: bool) -> Self {
            Self {
                fail_updates,
                block_first_worker: AtomicBool::new(false),
                published: AtomicBool::new(false),
                stage: Arc::new(parking_lot::Mutex::new(None)),
                requeue: AtomicBool::new(false),
                replacement: parking_lot::Mutex::new(None),
                manager: PlatformPropertyManager::new(HashMap::new()),
                workers: parking_lot::Mutex::new(None),
                events: parking_lot::Mutex::new(Vec::new()),
                admitted_flags: parking_lot::Mutex::new(Vec::new()),
            }
        }
    }

    impl MetricsComponent for RecordingScheduler {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            self.published.store(true, Ordering::Relaxed);
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    impl RootMetricsComponent for RecordingScheduler {}

    #[async_trait]
    impl WorkerScheduler for RecordingScheduler {
        fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
            &self.manager
        }

        fn resource_capacity_updates(
            &self,
        ) -> Option<watch::Receiver<Arc<ResourceCapacitySnapshot>>> {
            None
        }

        async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
            self.admitted_flags.lock().push(worker.is_draining);
            Ok(())
        }

        async fn worker_dispatch_accepted(
            &self,
            _worker_id: &WorkerId,
            _operation_id: &OperationId,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_dispatch_declined(
            &self,
            worker_id: &WorkerId,
            _operation_id: &OperationId,
            _reason: String,
            _needs_kb: Option<u64>,
        ) -> Result<(), Error> {
            if let Some(workers) = self.workers.lock().as_mut() {
                let worker = workers
                    .iter_mut()
                    .find(|worker| worker.id == worker_id.0)
                    .ok_or_else(|| make_err!(Code::NotFound, "worker missing"))?;
                worker.running_actions = worker.running_actions.saturating_sub(1);
            }
            Ok(())
        }

        async fn update_action(
            &self,
            worker_id: &WorkerId,
            _operation_id: &OperationId,
            update: UpdateOperationType,
        ) -> Result<(), Error> {
            if self.block_first_worker.load(Ordering::Relaxed) && worker_id.0 == "first" {
                std::future::pending::<()>().await;
            }
            if self.fail_updates {
                return Err(make_err!(Code::Internal, "update rejected"));
            }
            if let UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)) =
                update
            {
                if let Some(workers) = self.workers.lock().as_mut()
                    && let Some(worker) = workers.iter_mut().find(|worker| worker.id == worker_id.0)
                {
                    worker.running_actions = worker.running_actions.saturating_sub(1);
                }
                let mut stage = self.stage.lock();
                if !matches!(*stage, Some(ActionStage::Completed(_))) {
                    *stage = Some(if self.requeue.load(Ordering::Relaxed) {
                        ActionStage::Queued
                    } else {
                        ActionStage::Completed(self.replacement.lock().clone().unwrap_or(result))
                    });
                }
            }
            Ok(())
        }

        async fn record_action_resource_usage(
            &self,
            _worker_id: &WorkerId,
            _operation_id: &OperationId,
            _resource_usage: ActionResourceUsage,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_keep_alive_received(
            &self,
            _worker_id: &WorkerId,
            _timestamp: WorkerTimestamp,
            _load: Option<WorkerLoad>,
            _cache_stats: Option<CacheStats>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_running_operations_reported(
            &self,
            worker_id: &WorkerId,
            operation_ids: Vec<OperationId>,
        ) -> Result<(), Error> {
            if let Some(workers) = self.workers.lock().as_mut() {
                let worker = workers
                    .iter_mut()
                    .find(|worker| worker.id == worker_id.0)
                    .ok_or_else(|| make_err!(Code::NotFound, "worker missing"))?;
                if operation_ids.is_empty() {
                    worker.running_actions = 0;
                }
            }
            Ok(())
        }

        async fn worker_liveness_refreshed(
            &self,
            _worker_id: &WorkerId,
            _timestamp: WorkerTimestamp,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error> {
            self.events.lock().push(format!("remove:{}", worker_id.0));
            if let Some(workers) = self.workers.lock().as_mut() {
                workers.retain(|worker| worker.id != worker_id.0);
            }
            Ok(())
        }

        async fn worker_disconnected(&self, _worker_id: &WorkerId) -> Result<(), Error> {
            Ok(())
        }

        async fn shutdown(&self, _shutdown_guard: ShutdownGuard) {}

        async fn remove_timedout_workers(
            &self,
            _now_timestamp: WorkerTimestamp,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn set_drain_worker(
            &self,
            worker_id: &WorkerId,
            is_draining: bool,
        ) -> Result<(), Error> {
            self.events
                .lock()
                .push(format!("drain:{}:{is_draining}", worker_id.0));
            if let Some(workers) = self.workers.lock().as_mut()
                && let Some(worker) = workers.iter_mut().find(|worker| worker.id == worker_id.0)
            {
                worker.is_draining = is_draining;
            }
            Ok(())
        }
        async fn expire_attempts(&self, now: SystemTime) -> Result<(), Error> {
            if let Some(workers) = self.workers.lock().as_mut() {
                for worker in workers {
                    if worker.running_actions != 0
                        && worker
                            .latest_attempt_deadline
                            .is_some_and(|stamp| stamp <= now)
                    {
                        worker.running_actions = 0;
                        worker.latest_attempt_deadline = None;
                        *self.stage.lock() = Some(ActionStage::Completed(ActionResult {
                            error: Some(make_err!(
                                Code::DeadlineExceeded,
                                "attempt reached controller dispatch deadline"
                            )),
                            ..ActionResult::default()
                        }));
                    }
                }
            }
            Ok(())
        }

        async fn worker_snapshot(&self) -> Vec<WorkerSummary> {
            if let Some(workers) = self.workers.lock().as_ref() {
                return workers.clone();
            }
            vec![WorkerSummary {
                id: "snap".to_owned(),
                running_actions: 1,
                max_inflight_tasks: 2,
                is_paused: false,
                is_draining: false,
                last_update_timestamp: 3,
                platform_properties: HashMap::new(),
                available_platform_properties: HashMap::new(),
                free_memory_kb: None,
                cache: None,
                latest_attempt_deadline: Some(
                    SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_060),
                ),
            }]
        }

        async fn kill_revoked_operations(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn wrap(
        fail_updates: bool,
    ) -> (
        Arc<RecordingScheduler>,
        ObservingWorkerScheduler,
        Arc<AtomicU64>,
    ) {
        let inner = Arc::new(RecordingScheduler::new(fail_updates));
        let completed = Arc::new(AtomicU64::new(0));
        let counting = ObservingWorkerScheduler::new(
            Arc::clone(&inner) as Arc<dyn WorkerScheduler>,
            Arc::new(Operations {
                info: test_action_info(),
                stage: Arc::clone(&inner.stage),
                origin: Some(test_origin("local:alice")),
            }),
            Arc::clone(&completed),
            ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN).0,
            HandOff::new(),
            test_durations(),
            test_clock(),
            &test_fleets(),
            crate::capacity::test_catalog((*test_fleets()).clone()),
            test_rates(),
        );
        (inner, counting, completed)
    }
    #[tokio::test]
    async fn retiring_lock_timeout_is_unavailable_and_retryable() {
        let (_, scheduler, _) = wrap(false);
        let clock = gang_clock::TestClock::default();
        let bound = gang_types::NonZeroDuration::from_secs(1);
        let fleet = FleetName::try_new("fleet-a").expect("fleet");
        let instance = InstanceId::try_new("local-1").expect("instance");
        let held = scheduler.retiring.write().await;
        let acquisition = Acquisition::new(&clock, bound);
        let command = scheduler.set_draining(&fleet, &instance, true, &acquisition);
        tokio::pin!(command);
        assert!(futures::poll!(command.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(matches!(command.await, Err(PortError::Unavailable(_))));
        drop(held);
        let mut cordons = Vec::new();
        scheduler.cordons_into(&mut cordons).await;
        assert!(cordons.is_empty());
        let acquisition = Acquisition::new(&clock, bound);
        scheduler
            .set_draining(&fleet, &instance, true, &acquisition)
            .await
            .expect("retry");
        scheduler.cordons_into(&mut cordons).await;
        assert_eq!(cordons.len(), 1);
    }

    #[tokio::test]
    async fn worker_self_drain_and_controller_cordon_keep_separate_owners() {
        let (inner, scheduler, _) = wrap(false);
        let old = WorkerId("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b".to_owned());
        let new = WorkerId("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6c".to_owned());
        let mut summary = inner.worker_snapshot().await.remove(0);
        summary.id.clone_from(&old.0);
        summary
            .platform_properties
            .insert(gang_types::property::FLEET.to_owned(), "fleet-a".to_owned());
        summary.platform_properties.insert(
            gang_types::property::GANG_VERSION.to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
        );
        let mut healthy = summary.clone();
        healthy.id.clone_from(&new.0);
        *inner.workers.lock() = Some(vec![summary, healthy]);
        let fleet = FleetName::try_new("fleet-a").expect("fleet");
        let instance = InstanceId::try_new("local-1").expect("instance");
        let mut cordons = Vec::new();
        scheduler
            .set_drain_worker(&old, true)
            .await
            .expect("self drain");
        scheduler.cordons_into(&mut cordons).await;
        assert!(cordons.is_empty());
        scheduler
            .set_draining(
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await
            .expect("controller cordon");
        scheduler.cordons_into(&mut cordons).await;
        assert_eq!(cordons.len(), 1);
        scheduler
            .set_drain_worker(&new, false)
            .await
            .expect("self resume cannot override controller");
        assert!(
            inner
                .worker_snapshot()
                .await
                .iter()
                .all(|worker| worker.is_draining)
        );
        scheduler
            .set_draining(
                &fleet,
                &instance,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await
            .expect("controller cancel");
        let workers = inner.worker_snapshot().await;
        assert!(workers[0].is_draining);
        assert!(!workers[1].is_draining);
        scheduler.cordons_into(&mut cordons).await;
        assert!(cordons.is_empty());
    }

    #[tokio::test]
    async fn worker_admission_inherits_physical_intent_before_forwarding() {
        let (inner, scheduler, _) = wrap(false);
        *inner.workers.lock() = Some(Vec::new());
        let fleet = FleetName::try_new("fleet-a").expect("fleet");
        let instance = InstanceId::try_new("local-1").expect("instance");
        scheduler
            .set_draining(
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await
            .expect("unconnected cordon");
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            WorkerId("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b".to_owned()),
            nativelink_util::platform_properties::PlatformProperties {
                properties: HashMap::from([
                    (
                        gang_types::property::FLEET.to_owned(),
                        nativelink_util::platform_properties::PlatformPropertyValue::Exact(
                            "fleet-a".to_owned(),
                        ),
                    ),
                    (
                        gang_types::property::GANG_VERSION.to_owned(),
                        nativelink_util::platform_properties::PlatformPropertyValue::Ignore(
                            env!("CARGO_PKG_VERSION").to_owned(),
                        ),
                    ),
                ]),
            },
            sender,
            0,
            0,
        );
        scheduler.add_worker(worker).await.expect("admission");
        assert_eq!(*inner.admitted_flags.lock(), [true]);
    }

    #[tokio::test]
    async fn blocked_worker_update_does_not_block_other_workers() -> gang_testkit::TestResult {
        let (inner, counting, _) = wrap(false);
        inner.block_first_worker.store(true, Ordering::Relaxed);
        let first = WorkerId("first".into());
        let second = WorkerId("second".into());
        let operation = OperationId::String("op-1".into());
        let mut blocked = std::pin::pin!(counting.update_action(
            &first,
            &operation,
            UpdateOperationType::KeepAlive
        ));
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        let mut independent = std::pin::pin!(counting.update_action(
            &second,
            &operation,
            UpdateOperationType::KeepAlive
        ));
        assert!(matches!(
            futures::poll!(independent.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn handoff_releases_worker_after_decline_or_reconciliation() -> gang_testkit::TestResult {
        for declined in [true, false] {
            let (inner, counting, _) = wrap(false);
            let mut worker = inner.worker_snapshot().await.remove(0);
            worker.id = "busy".to_owned();
            worker.running_actions = 1;
            *inner.workers.lock() = Some(vec![worker]);
            counting.begin_hand_off().await?;
            let mut released = std::pin::pin!(counting.handoff.workers_released());
            assert!(futures::poll!(released.as_mut()).is_pending());
            let id = WorkerId("busy".into());
            if declined {
                counting
                    .worker_dispatch_declined(
                        &id,
                        &OperationId::String("op-1".into()),
                        "declined".into(),
                        None,
                    )
                    .await?;
            } else {
                counting
                    .worker_running_operations_reported(&id, Vec::new())
                    .await?;
            }
            assert!(
                inner.worker_snapshot().await.is_empty(),
                "idle worker was not released"
            );
            assert!(futures::poll!(released.as_mut()).is_ready());
        }
        Ok(())
    }

    #[tokio::test]
    async fn handoff_drains_every_worker_before_removing_idle_and_releases_busy_at_completion() {
        let (inner, counting, _) = wrap(false);
        let mut idle = inner.worker_snapshot().await.remove(0);
        idle.id = "idle".to_owned();
        idle.running_actions = 0;
        let mut busy = idle.clone();
        busy.id = "busy".to_owned();
        busy.running_actions = 1;
        *inner.workers.lock() = Some(vec![idle, busy]);

        counting
            .begin_hand_off()
            .await
            .expect("handoff freezes dispatch");
        counting
            .refresh_drain(&mut ControllerDrain::default())
            .await
            .expect("handoff snapshot");
        assert!(counting.handoff.is_begun());
        assert_eq!(
            *inner.events.lock(),
            vec!["drain:idle:true", "drain:busy:true", "remove:idle"]
        );

        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            WorkerId("new".into()),
            nativelink_util::platform_properties::PlatformProperties::default(),
            sender,
            0,
            0,
        );
        assert!(
            matches!(counting.add_worker(worker).await, Err(error) if error.code == Code::Unavailable)
        );
        counting
            .update_action(
                &WorkerId("busy".into()),
                &OperationId::String("op-1".into()),
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default(),
                )),
            )
            .await
            .expect("completion forwarded");
        assert_eq!(
            *inner.events.lock(),
            vec![
                "drain:idle:true",
                "drain:busy:true",
                "remove:idle",
                "remove:busy"
            ]
        );
    }

    #[tokio::test]
    async fn only_completed_results_count() {
        let (_inner, counting, completed) = wrap(false);
        let worker = WorkerId("w-1".to_owned());
        let operation = OperationId::String("op-1".to_owned());

        let not_terminal = [
            UpdateOperationType::KeepAlive,
            UpdateOperationType::ExecutionComplete,
            UpdateOperationType::UpdateWithDisconnect,
            UpdateOperationType::UpdateWithError(make_err!(Code::Internal, "failed")),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Executing),
            UpdateOperationType::UpdateWithActionStage(ActionStage::CompletedFromCache(
                ProtoActionResult::default(),
            )),
        ];
        for update in not_terminal {
            counting
                .update_action(&worker, &operation, update)
                .await
                .expect("update forwards");
        }
        assert_eq!(completed.load(Ordering::Relaxed), 0);

        counting
            .update_action(
                &worker,
                &operation,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default(),
                )),
            )
            .await
            .expect("completion forwards");
        assert_eq!(completed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn failed_completion_does_not_count() {
        let (_inner, counting, completed) = wrap(true);
        let worker = WorkerId("w-1".to_owned());
        let operation = OperationId::String("op-1".to_owned());

        let result = counting
            .update_action(
                &worker,
                &operation,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default(),
                )),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(completed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn publishing_metrics_is_forwarded_to_the_inner_scheduler() {
        let (inner, counting, _completed) = wrap(false);

        let result = MetricsComponent::publish(
            &counting,
            MetricKind::Component,
            MetricFieldData {
                name: "x".into(),
                help: "".into(),
                group: "".into(),
            },
        );

        assert!(result.is_ok());
        assert!(inner.published.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn drain_uses_owned_stamp_and_waits_for_original_response_not_fresh_refusals()
    -> gang_testkit::TestResult {
        let (inner, mut counting, _) = wrap(false);
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000_000_000)));
        counting.clock = clock.clone();
        let instance = InstanceId::try_new("old-worker")?;
        let mut worker = inner.worker_snapshot().await.remove(0);
        worker.id = gang_testkit::worker_on(&instance)?.to_string();
        worker
            .platform_properties
            .insert(gang_types::property::FLEET.to_owned(), "fleet-a".to_owned());
        worker.latest_attempt_deadline = Some(
            SystemTime::UNIX_EPOCH
                + Duration::from_millis(clock.wall().as_unix_millis())
                + Duration::from_mins(6),
        );
        *inner.workers.lock() = Some(vec![worker]);
        let old_connection = Arc::new(crate::standing::Standing::default());
        assert!(old_connection.admit());
        let old_response = old_connection.begin();
        counting.handoff.public_connection(&old_connection);
        counting.begin_hand_off().await?;
        let retry = old_connection.retry_requested();
        tokio::pin!(retry);
        assert!(
            futures::poll!(retry.as_mut()).is_ready(),
            "admitted public connection receives GOAWAY"
        );
        let fresh_refusal = Arc::new(crate::standing::Standing::default());
        let _fresh_response = fresh_refusal.begin();
        counting.handoff.public_connection(&fresh_refusal);
        let mut drain = ControllerDrain {
            workers: Vec::with_capacity(4),
            complete: false,
        };
        counting.refresh_drain(&mut drain).await?;
        let buffer = drain.workers.as_ptr();
        assert_eq!(drain.workers.len(), 1);
        assert_eq!(drain.workers[0].instance, instance);
        assert_eq!(
            drain.workers[0].deadline,
            clock.wall() + Duration::from_mins(6)
        );
        assert!(!drain.complete);
        clock.advance(Duration::from_secs(301));
        counting.begin_hand_off().await?;
        counting.refresh_drain(&mut drain).await?;
        assert_eq!(
            drain.workers.as_ptr(),
            buffer,
            "handoff refresh reuses the caller buffer"
        );
        assert_eq!(
            drain.workers.len(),
            1,
            "no independent 300-second action kill"
        );
        assert!(!drain.complete);
        clock.advance(Duration::from_secs(59));
        counting.refresh_drain(&mut drain).await?;
        assert!(drain.workers.is_empty());
        assert!(
            !drain.complete,
            "terminal attempts still wait for the original response body"
        );
        assert!(
            matches!(&*inner.stage.lock(), Some(ActionStage::Completed(result))
            if result.error.as_ref().is_some_and(|error| error.code == Code::DeadlineExceeded))
        );
        assert!(
            !inner.requeue.load(Ordering::Acquire),
            "stamp expiry is terminal, never a generic-error requeue"
        );
        drop(old_response);
        counting.refresh_drain(&mut drain).await?;
        assert!(
            drain.complete,
            "fresh refusal traffic cannot retain the old engine"
        );
        Ok(())
    }
}
