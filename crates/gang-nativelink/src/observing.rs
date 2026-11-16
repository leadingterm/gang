//! A worker scheduler that counts and observes completed actions.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::StreamExt;
use gang_clock::Timestamp;
use gang_spi::{Executed, ExecutionTimes, Observation, ObservationSink, Phase};
use gang_types::Identity;
use nativelink_error::Error;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ActionResourceUsage, WorkerLoad,
};
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::{Worker, WorkerTimestamp};
use nativelink_scheduler::worker_scheduler::{WorkerScheduler, WorkerSummary};
use nativelink_util::action_messages::{
    ActionResult, ActionStage, ExecutionMetadata, OperationId, WorkerId,
};
use nativelink_util::operation_state_manager::UpdateOperationType;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, OperationFilter, OperationStageFlags,
};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::shutdown_guard::ShutdownGuard;

use crate::actions::digest_text;
use crate::observer::origin;

/// Forwards every call to the inner worker scheduler and counts the actions that reach a
/// terminal result on a worker. Only a successful `Completed` update counts: errors, keep-alives,
/// declines, disconnects and results served from the cache do not.
pub(crate) struct ObservingWorkerScheduler {
    inner: Arc<dyn WorkerScheduler>,
    operations: Arc<dyn ClientStateManager>,
    completed: Arc<AtomicU64>,
    sink: ObservationSink,
}

impl ObservingWorkerScheduler {
    /// Wraps the worker scheduler, counting and observing completed actions.
    pub(crate) fn new(
        inner: Arc<dyn WorkerScheduler>,
        operations: Arc<dyn ClientStateManager>,
        completed: Arc<AtomicU64>,
        sink: ObservationSink,
    ) -> Self {
        Self {
            inner,
            operations,
            completed,
            sink,
        }
    }

    /// Builds an observation from the retained operation and its final result.
    async fn executed(
        &self,
        worker_id: &WorkerId,
        operation: &dyn ActionStateResult,
        result: &ActionResult,
    ) -> Option<Executed> {
        let (info, metadata) = operation.as_action_info().await.ok()?;
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

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
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
        self.inner
            .worker_dispatch_declined(worker_id, operation_id, reason, needs_kb)
            .await
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
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
            && let Ok((state, _)) = operation.as_state().await
            && let ActionStage::Completed(completed) = &state.stage
            && let Some((exit_code, metadata)) = sent
            && completed.exit_code == exit_code
            && completed.execution_metadata == metadata
        {
            self.completed.fetch_add(1, Ordering::Relaxed);
            if let Some(executed) = self.executed(worker_id, &*operation, completed).await {
                self.sink.record(Observation::Executed(executed));
            }
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
    ) -> Result<(), Error> {
        self.inner
            .worker_keep_alive_received(worker_id, timestamp, load)
            .await
    }

    async fn worker_running_operations_reported(
        &self,
        worker_id: &WorkerId,
        operation_ids: Vec<OperationId>,
    ) -> Result<(), Error> {
        self.inner
            .worker_running_operations_reported(worker_id, operation_ids)
            .await
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
        self.inner.remove_worker(worker_id).await
    }

    async fn worker_disconnected(&self, worker_id: &WorkerId) -> Result<(), Error> {
        self.inner.worker_disconnected(worker_id).await
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        self.inner.shutdown(shutdown_guard).await;
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.inner.remove_timedout_workers(now_timestamp).await
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        self.inner.set_drain_worker(worker_id, is_draining).await
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
                None,
            ))
        }
        async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Err(make_err!(Code::Internal, "not implemented"))
        }
        async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
            Ok((
                Arc::clone(&self.info),
                Some(OriginMetadata {
                    identity: "local:alice".to_owned(),
                    bazel_metadata: Some(
                        nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata {
                            tool_invocation_id: "build".to_owned(),
                            ..Default::default()
                        },
                    ),
                }),
            ))
        }
    }
    struct Operations {
        info: Arc<ActionInfo>,
        stage: Arc<parking_lot::Mutex<Option<ActionStage>>>,
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
            }) as Box<dyn ActionStateResult>;
            Ok(Box::pin(futures::stream::iter([row])))
        }
    }

    fn test_action_info() -> Arc<ActionInfo> {
        let digest = DigestInfo::new([3; 32], 30);
        Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: HashMap::new(),
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
                })
            };
            let scheduler =
                ObservingWorkerScheduler::new(inner, operations, Arc::clone(&completed), sink);
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
            drop(scheduler);
            let observation = receiver.next().await;
            if fail || read_fails || requeue {
                assert!(observation.is_none());
                continue;
            }
            let Some(gang_spi::Delivery::Observation(observation)) = observation else {
                panic!("expected observation")
            };
            let Observation::Executed(executed) = *observation else {
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
            assert_eq!(
                executed.times.queued,
                Phase::reported(
                    Timestamp::from(t),
                    Timestamp::from(t + Duration::from_secs(4))
                )
            );
            assert_eq!(
                executed.times.input_fetch,
                Phase::reported(
                    Timestamp::from(t + Duration::from_secs(5)),
                    Timestamp::from(t + Duration::from_secs(6))
                )
            );
            assert_eq!(
                executed.times.executing,
                Phase::reported(
                    Timestamp::from(t + Duration::from_secs(7)),
                    Timestamp::from(t + Duration::from_secs(8))
                )
            );
            assert_eq!(
                executed.times.output_upload,
                Phase::reported(
                    Timestamp::from(t + Duration::from_secs(9)),
                    Timestamp::from(t + Duration::from_secs(10))
                )
            );
            assert_eq!(
                executed.times.whole,
                Phase::reported(
                    Timestamp::from(t),
                    Timestamp::from(t + Duration::from_secs(11))
                )
            );
            assert!(receiver.next().await.is_none());
        }
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
                }),
                Arc::clone(&completed),
                sink,
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
            drop(scheduler);
            assert!(observations.next().await.is_none());
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

    /// Fails `update_action` when asked to, and accepts every other call.
    struct RecordingScheduler {
        fail_updates: bool,
        published: AtomicBool,
        stage: Arc<parking_lot::Mutex<Option<ActionStage>>>,
        requeue: AtomicBool,
        replacement: parking_lot::Mutex<Option<ActionResult>>,
        manager: PlatformPropertyManager,
    }

    impl RecordingScheduler {
        fn new(fail_updates: bool) -> Self {
            Self {
                fail_updates,
                published: AtomicBool::new(false),
                stage: Arc::new(parking_lot::Mutex::new(None)),
                requeue: AtomicBool::new(false),
                replacement: parking_lot::Mutex::new(None),
                manager: PlatformPropertyManager::new(HashMap::new()),
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

        async fn add_worker(&self, _worker: Worker) -> Result<(), Error> {
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
            _worker_id: &WorkerId,
            _operation_id: &OperationId,
            _reason: String,
            _needs_kb: Option<u64>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn update_action(
            &self,
            _worker_id: &WorkerId,
            _operation_id: &OperationId,
            update: UpdateOperationType,
        ) -> Result<(), Error> {
            if self.fail_updates {
                return Err(make_err!(Code::Internal, "update rejected"));
            }
            if let UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)) =
                update
            {
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
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_running_operations_reported(
            &self,
            _worker_id: &WorkerId,
            _operation_ids: Vec<OperationId>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_liveness_refreshed(
            &self,
            _worker_id: &WorkerId,
            _timestamp: WorkerTimestamp,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn remove_worker(&self, _worker_id: &WorkerId) -> Result<(), Error> {
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
            _worker_id: &WorkerId,
            _is_draining: bool,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn worker_snapshot(&self) -> Vec<WorkerSummary> {
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
            }),
            Arc::clone(&completed),
            ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN).0,
        );
        (inner, counting, completed)
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
}
