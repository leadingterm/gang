//! Rejects actions no declared fleet can run before they enter the scheduler.
//!
//! Gang scales from zero, so `NativeLink`'s worker-based unsatisfiable check cannot see fleets
//! whose workers have not connected yet.

use std::sync::Arc;

use async_trait::async_trait;
use gang_types::FleetCatalog;
use nativelink_error::{Error, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{ActionInfo, OperationId};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};

/// Forwards scheduler calls while refusing actions that cannot run on any declared fleet.
pub(crate) struct PlaceableActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    fleets: FleetCatalog,
}

impl PlaceableActions {
    /// Wraps the action scheduler with the fleet catalog used for placement.
    pub(crate) fn new(inner: Arc<dyn KnownPlatformPropertyProvider>, fleets: FleetCatalog) -> Self {
        Self { inner, fleets }
    }
}

#[deny(clippy::missing_trait_methods)]
impl MetricsComponent for PlaceableActions {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MetricsComponent::publish(&*self.inner, kind, field_metadata)
    }
}

#[deny(clippy::missing_trait_methods)]
impl RootMetricsComponent for PlaceableActions {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        RootMetricsComponent::publish(&*self.inner, kind, field_metadata)
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ClientStateManager for PlaceableActions {
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        if self
            .fleets
            .accepting(|key| action_info.platform_properties.get(key).map(String::as_str))
            .is_empty()
        {
            let mut properties: Vec<_> = action_info.platform_properties.iter().collect();
            properties.sort_unstable_by_key(|(key, _)| *key);
            let properties = properties
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(make_input_err!(
                "no fleet accepts the platform properties of this action ({properties}): change its exec_properties or declare a fleet that accepts them"
            ));
        }
        self.inner
            .add_action(client_operation_id, action_info)
            .await
    }

    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        self.inner.filter_operations(filter).await
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl KnownPlatformPropertyProvider for PlaceableActions {
    async fn get_known_properties(&self, instance_name: &str) -> Result<Vec<String>, Error> {
        self.inner.get_known_properties(instance_name).await
    }
}
