//! Fits learned reservations to a fleet that accepted the original client request.
//! Positive memory and disk reservations are raised to their configured minima; zero stays zero.
//! Client minimums and trusted historical peaks remain hard floors; fleets below them are
//! excluded before fitting. Desired memory is captured before static clamps; ordinary CPU
//! fitting finishes before `NativeLink` performs authoritative physical-memory fitting.

use std::sync::Arc;

use async_trait::async_trait;
use gang_config::{DiskMinReservation, MinReservation};
use gang_types::{Bytes, FleetCatalog, property};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{
    ActionInfo, ActionReservation, MemoryReservation, OperationId,
};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};

use crate::capacity::RuntimeCatalog;
use crate::placeable::{AdmittedAction, AdmittedFleets, normalized_cpu_reservation};

/// Fits reservations after historical learning and before the simple scheduler.
pub(crate) struct FittedActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    fleets: Arc<RuntimeCatalog>,
    admitted: AdmittedFleets,
    min_reservation: MinReservation,
    disk_min_reservation: DiskMinReservation,
}

fn cpu_reservation(info: &ActionInfo) -> Option<u64> {
    normalized_cpu_reservation(&info.platform_properties).or_else(|| {
        info.platform_properties
            .get(property::CPU_MILLICORES)
            .and_then(|value| value.parse().ok())
    })
}

impl FittedActions {
    pub(crate) fn new(
        inner: Arc<dyn KnownPlatformPropertyProvider>,
        fleets: Arc<RuntimeCatalog>,
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

    fn current_admission(
        &self,
        operation: &OperationId,
        fleets: &FleetCatalog,
        trusted_minimum_kb: Option<u64>,
    ) -> Result<AdmittedAction, Error> {
        let mut admission = self
            .admitted
            .take(operation)
            .unwrap_or_else(|| AdmittedAction {
                fleets: fleets.accepting(|_| None),
                minimum_kb: None,
            });
        // Both the client floor and a measured historical peak are hard constraints.
        admission.minimum_kb = admission.minimum_kb.max(trusted_minimum_kb);
        if let Some(minimum) = admission.minimum_kb {
            for index in admission.fleets {
                if fleets
                    .fleets()
                    .get(index)
                    .and_then(|fleet| fleet.limit(property::MEMORY_KB))
                    .is_none_or(|limit| limit < minimum)
                {
                    admission.fleets.remove(index);
                }
            }
            if admission.fleets.is_empty() {
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "no admitted fleet can satisfy memory floor {minimum} KiB"
                ));
            }
        }
        Ok(admission)
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
        let fleets = self.fleets.current()?;
        let trusted_minimum_kb = action_info
            .memory_reservation
            .as_ref()
            .and_then(|reservation| reservation.minimum_kb);
        let AdmittedAction {
            fleets: admitted,
            minimum_kb,
        } = self.current_admission(&operation, &fleets, trusted_minimum_kb)?;
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
        let desired_kb = action_info
            .platform_properties
            .get(property::MEMORY_KB)
            .and_then(|value| value.parse::<u64>().ok());
        let pre_fit_reservation = ActionReservation {
            cpu_millicores: cpu_reservation(&action_info).unwrap_or_default(),
            memory_bytes: desired_kb
                .unwrap_or_default()
                .saturating_mul(Bytes::kib(1).get()),
        };
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
                .filter_map(|fleet| fleet.limit(key))
                .max();
            if let Some(capacity) = largest
                && reserved > capacity
            {
                Arc::make_mut(&mut action_info)
                    .platform_properties
                    .insert(key.to_owned(), capacity.to_string());
            }
        }
        let properties = |key: &str| action_info.platform_properties.get(key).map(String::as_str);
        if let Some(fit) = fleets.fit(admitted, properties)
            && !fit.lowered.is_empty()
        {
            tracing::info!(operation_id = %operation, fleet = fit.fleet, keys = ?fit.lowered, "fitting action reservation to fleet");
            let properties = &mut Arc::make_mut(&mut action_info).platform_properties;
            for (key, limit) in fit.lowered {
                properties.insert(key.to_owned(), limit.to_string());
            }
        }
        let cpu_before_memory_fit = cpu_reservation(&action_info);
        // Keep ordinary CPU learning/fitting, but let NativeLink alone promote CPU when
        // fitted memory fills the observed physical worker (including exact equality).
        // Preserve only the trusted floor from historical learning; recompute the rest.
        let info = Arc::make_mut(&mut action_info);
        info.pre_fit_reservation = Some(pre_fit_reservation);
        info.memory_reservation = Some(MemoryReservation {
            minimum_kb,
            desired_kb,
            cpu_before_memory_fit,
            fitted_revision: None,
        });
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
