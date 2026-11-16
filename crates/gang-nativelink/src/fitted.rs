//! Fits learned reservations to a fleet that accepted the original client request.
//! It also raises a non-zero memory reservation below the configured minimum to it; a `memory_kb`
//! of 0 means not requested and stays 0.

use std::sync::Arc;

use async_trait::async_trait;
use gang_types::{FleetCatalog, property};
use nativelink_error::Error;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{ActionInfo, OperationId};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};

use crate::enforcement::MinReservation;
use crate::placeable::{AdmittedFleets, refused};

/// Fits reservations after historical learning and before the simple scheduler.
pub(crate) struct FittedActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    fleets: FleetCatalog,
    admitted: AdmittedFleets,
    min_reservation: MinReservation,
}

impl FittedActions {
    pub(crate) fn new(
        inner: Arc<dyn KnownPlatformPropertyProvider>,
        fleets: FleetCatalog,
        admitted: AdmittedFleets,
        min_reservation: MinReservation,
    ) -> Self {
        Self {
            inner,
            fleets,
            admitted,
            min_reservation,
        }
    }
}

#[deny(clippy::missing_trait_methods)]
impl MetricsComponent for FittedActions {
    fn publish(
        &self,
        kind: MetricKind,
        metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MetricsComponent::publish(&*self.inner, kind, metadata)
    }
}

#[deny(clippy::missing_trait_methods)]
impl RootMetricsComponent for FittedActions {
    fn publish(
        &self,
        kind: MetricKind,
        metadata: MetricFieldData<'_>,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        RootMetricsComponent::publish(&*self.inner, kind, metadata)
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ClientStateManager for FittedActions {
    async fn add_action(
        &self,
        operation: OperationId,
        mut action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        let admitted = self
            .admitted
            .take(&operation)
            .unwrap_or_else(|| self.fleets.accepting(|_| None));
        let min_kb = self.min_reservation.as_units();
        if let Some(reserved_kb) = action_info
            .platform_properties
            .get(property::MEMORY_KB)
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&value| value > 0 && value < min_kb)
        {
            tracing::debug!(operation_id = %operation, reserved_kb, min_kb, "raising a memory reservation to the minimum");
            Arc::make_mut(&mut action_info)
                .platform_properties
                .insert(property::MEMORY_KB.to_owned(), min_kb.to_string());
        }
        let properties = |key: &str| action_info.platform_properties.get(key).map(String::as_str);
        let Some(fit) = self.fleets.fit(admitted, properties) else {
            return Err(refused(&self.fleets, properties));
        };
        if !fit.lowered.is_empty() {
            // A learned reservation comes from an action that already ran on a worker of a
            // fleet, so the largest admitted fleet's size is enough. Only a client value above
            // every fleet is an error, and the outer layer refuses it. Memory escalation happens
            // below this layer, inside NativeLink, bounded by the largest connected worker.
            tracing::info!(operation_id = %operation, fleet = fit.fleet, keys = ?fit.lowered, "fitting action reservation to fleet");
            let properties = &mut Arc::make_mut(&mut action_info).platform_properties;
            for (key, limit) in fit.lowered {
                properties.insert(key.to_owned(), limit.to_string());
            }
        }
        self.inner.add_action(operation, action_info).await
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
impl KnownPlatformPropertyProvider for FittedActions {
    async fn get_known_properties(&self, instance_name: &str) -> Result<Vec<String>, Error> {
        self.inner.get_known_properties(instance_name).await
    }
}
