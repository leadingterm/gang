//! Fits learned reservations to a fleet that accepted the original client request.
//! Positive memory and disk reservations are raised to their configured minima; zero stays zero.

use std::sync::Arc;

use async_trait::async_trait;
use gang_config::{DiskMinReservation, MinReservation};
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

use crate::placeable::AdmittedFleets;

/// Fits reservations after historical learning and before the simple scheduler.
pub(crate) struct FittedActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    fleets: FleetCatalog,
    admitted: AdmittedFleets,
    min_reservation: MinReservation,
    disk_min_reservation: DiskMinReservation,
}

impl FittedActions {
    pub(crate) fn new(
        inner: Arc<dyn KnownPlatformPropertyProvider>,
        fleets: FleetCatalog,
        admitted: AdmittedFleets,
        min_reservation: MinReservation,
        disk_min_reservation: DiskMinReservation,
    ) -> Self {
        Self {
            inner,
            fleets,
            admitted,
            min_reservation,
            disk_min_reservation,
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
        let (admitted, live_catalog) = self
            .admitted
            .take(&operation)
            .unwrap_or_else(|| (self.fleets.accepting_base(|_| None), None));
        let fleets = live_catalog.as_ref().unwrap_or(&self.fleets);
        for (key, min_kb) in [
            (property::MEMORY_KB, self.min_reservation.as_units()),
            (property::DISK_KB, self.disk_min_reservation.as_units()),
        ] {
            if let Some(reserved_kb) = action_info
                .platform_properties
                .get(key)
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|&value| value > 0 && value < min_kb)
            {
                tracing::debug!(operation_id = %operation, key, reserved_kb, min_kb, "raising a reservation to the minimum");
                Arc::make_mut(&mut action_info)
                    .platform_properties
                    .insert(key.to_owned(), min_kb.to_string());
            }
        }
        // Match the other requested properties before choosing the largest eligible capacity.
        // This avoids selecting a smaller fleet merely because it is earlier in preference order.
        for key in [property::MEMORY_KB, property::DISK_KB] {
            let Some(reserved) = action_info
                .platform_properties
                .get(key)
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            let eligible = admitted
                & fleets
                    .admit(
                        action_info
                            .platform_properties
                            .iter()
                            .filter(|(other, _)| other.as_str() != key)
                            .map(|(other, value)| (other.as_str(), value.as_str())),
                    )
                    .unwrap_or_default();
            let largest = eligible
                .iter()
                .filter_map(|index| fleets.fleets().get(index))
                .filter_map(|fleet| fleet.limit(key).map(|capacity| (capacity, fleet)))
                .max_by_key(|(capacity, _)| *capacity);
            if let Some((capacity, fleet)) = largest
                && reserved > capacity
            {
                let whole_cpu = (key == property::MEMORY_KB
                    && fleet.capacity(key) == Some(capacity))
                .then(|| fleet.limit(property::CPU_MILLICORES))
                .flatten();
                let properties = &mut Arc::make_mut(&mut action_info).platform_properties;
                properties.insert(key.to_owned(), capacity.to_string());
                if let Some(whole_cpu) = whole_cpu {
                    properties.insert(property::CPU_MILLICORES.to_owned(), whole_cpu.to_string());
                }
            }
        }
        let properties = |key: &str| action_info.platform_properties.get(key).map(String::as_str);
        if let Some(fit) = fleets.fit(admitted, properties)
            && !fit.lowered.is_empty()
        {
            tracing::info!(operation_id = %operation, fleet = fit.fleet, keys = ?fit.lowered, "fitting action reservation to fleet");
            let whole_cpu = fleets.fleets().get(fit.fleet).and_then(|fleet| {
                fit.lowered
                    .iter()
                    .any(|(key, limit)| {
                        *key == property::MEMORY_KB && fleet.capacity(key) == Some(*limit)
                    })
                    .then(|| fleet.limit(property::CPU_MILLICORES))
                    .flatten()
            });
            let properties = &mut Arc::make_mut(&mut action_info).platform_properties;
            for (key, limit) in fit.lowered {
                properties.insert(key.to_owned(), limit.to_string());
            }
            if let Some(whole_cpu) = whole_cpu {
                properties.insert(property::CPU_MILLICORES.to_owned(), whole_cpu.to_string());
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
