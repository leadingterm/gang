//! Stops admission and releases queued client streams during a controller hand-off.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt;
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{ActionInfo, ActionStage, ActionState, OperationId};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};
use nativelink_util::origin_event::OriginMetadata;
use tokio::sync::watch;

/// Persistent, cloneable hand-off notifications.
#[derive(Clone)]
pub(crate) struct HandOff {
    begun: watch::Sender<bool>,
    workers_released: watch::Sender<bool>,
}

impl HandOff {
    pub(crate) fn new() -> Self {
        Self {
            begun: watch::channel(false).0,
            workers_released: watch::channel(false).0,
        }
    }

    pub(crate) fn begin(&self) {
        self.begun.send_replace(true);
    }

    pub(crate) fn is_begun(&self) -> bool {
        *self.begun.borrow()
    }

    pub(crate) async fn begun(&self) {
        let mut receiver = self.begun.subscribe();
        let _ = receiver.wait_for(|begun| *begun).await;
    }

    pub(crate) fn release_workers(&self) {
        self.workers_released.send_replace(true);
    }

    pub(crate) async fn workers_released(&self) {
        let mut receiver = self.workers_released.subscribe();
        let _ = receiver.wait_for(|released| *released).await;
    }
}

fn unavailable() -> Error {
    make_err!(
        Code::Unavailable,
        "the controller is handing off; submit the action again"
    )
}

/// Refuses new work and wraps both `Execute` and `WaitExecution` results.
pub(crate) struct HandOffActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    handoff: HandOff,
}

impl HandOffActions {
    pub(crate) fn new(inner: Arc<dyn KnownPlatformPropertyProvider>, handoff: HandOff) -> Self {
        Self { inner, handoff }
    }
}

impl MetricsComponent for HandOffActions {
    fn publish(
        &self,
        kind: MetricKind,
        metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MetricsComponent::publish(&*self.inner, kind, metadata)
    }
}

impl RootMetricsComponent for HandOffActions {
    fn publish(
        &self,
        kind: MetricKind,
        metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        RootMetricsComponent::publish(&*self.inner, kind, metadata)
    }
}

#[async_trait]
impl ClientStateManager for HandOffActions {
    async fn add_action(
        &self,
        id: OperationId,
        info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        if self.handoff.is_begun() {
            return Err(unavailable());
        }
        let inner = self.inner.add_action(id, info).await?;
        Ok(Box::new(HandOffResult {
            inner,
            handoff: self.handoff.clone(),
        }))
    }

    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'_>, Error> {
        let stream = self.inner.filter_operations(filter).await?;
        let handoff = self.handoff.clone();
        Ok(Box::pin(stream.map(move |inner| {
            Box::new(HandOffResult {
                inner,
                handoff: handoff.clone(),
            }) as Box<dyn ActionStateResult>
        })))
    }
}

#[async_trait]
impl KnownPlatformPropertyProvider for HandOffActions {
    async fn get_known_properties(&self, instance_name: &str) -> Result<Vec<String>, Error> {
        self.inner.get_known_properties(instance_name).await
    }
}

struct HandOffResult {
    inner: Box<dyn ActionStateResult>,
    handoff: HandOff,
}

#[async_trait]
impl ActionStateResult for HandOffResult {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        self.inner.as_state().await
    }

    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        let result = tokio::select! {
            biased;
            () = self.handoff.begun() => {
                let (state, metadata) = self.inner.as_state().await?;
                match &state.stage {
                    ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => {
                        return Ok((state, metadata));
                    }
                    ActionStage::Executing => self.inner.changed().await,
                    _ => return Err(unavailable()),
                }
            }
            result = self.inner.changed() => result,
        }?;
        if self.handoff.is_begun()
            && !matches!(
                result.0.stage,
                ActionStage::Executing
                    | ActionStage::Completed(_)
                    | ActionStage::CompletedFromCache(_)
            )
        {
            return Err(unavailable());
        }
        Ok(result)
    }

    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        self.inner.as_action_info().await
    }

    async fn client_last_seen(&self) -> Result<Option<SystemTime>, Error> {
        self.inner.client_last_seen().await
    }
}
