//! A worker scheduler that counts the actions workers finish, for the scheduler snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
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
use nativelink_util::action_messages::{ActionStage, OperationId, WorkerId};
use nativelink_util::operation_state_manager::UpdateOperationType;
use nativelink_util::shutdown_guard::ShutdownGuard;

/// Forwards every call to the inner worker scheduler and counts the actions that reach a
/// terminal result on a worker. Only a successful `Completed` update counts: errors, keep-alives,
/// declines, disconnects and results served from the cache do not.
pub(crate) struct CountingWorkerScheduler {
    inner: Arc<dyn WorkerScheduler>,
    completed: Arc<AtomicU64>,
}

impl CountingWorkerScheduler {
    /// Wraps `inner`, adding one to `completed` for every action it completes on a worker.
    pub(crate) fn new(inner: Arc<dyn WorkerScheduler>, completed: Arc<AtomicU64>) -> Self {
        Self { inner, completed }
    }
}

impl MetricsComponent for CountingWorkerScheduler {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MetricsComponent::publish(&*self.inner, kind, field_metadata)
    }
}

impl RootMetricsComponent for CountingWorkerScheduler {}

// Fails clippy if a defaulted `WorkerScheduler` method is not forwarded to `inner`.
#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl WorkerScheduler for CountingWorkerScheduler {
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
        let terminal = matches!(
            update,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(_))
        );
        let result = self
            .inner
            .update_action(worker_id, operation_id, update)
            .await;
        if terminal && result.is_ok() {
            self.completed.fetch_add(1, Ordering::Relaxed);
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

    use nativelink_error::{Code, make_err};
    use nativelink_proto::build::bazel::remote::execution::v2::ActionResult as ProtoActionResult;
    use nativelink_util::action_messages::ActionResult;

    use super::*;

    /// Fails `update_action` when asked to, and accepts every other call.
    struct RecordingScheduler {
        fail_updates: bool,
        published: AtomicBool,
        manager: PlatformPropertyManager,
    }

    impl RecordingScheduler {
        fn new(fail_updates: bool) -> Self {
            Self {
                fail_updates,
                published: AtomicBool::new(false),
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
            _update: UpdateOperationType,
        ) -> Result<(), Error> {
            if self.fail_updates {
                return Err(make_err!(Code::Internal, "update rejected"));
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
        CountingWorkerScheduler,
        Arc<AtomicU64>,
    ) {
        let inner = Arc::new(RecordingScheduler::new(fail_updates));
        let completed = Arc::new(AtomicU64::new(0));
        let counting = CountingWorkerScheduler::new(
            Arc::clone(&inner) as Arc<dyn WorkerScheduler>,
            Arc::clone(&completed),
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
