//! Rejects actions no declared fleet can run before they enter the scheduler.
//!
//! Gang scales from zero, so `NativeLink`'s worker-based unsatisfiable check cannot see fleets
//! whose workers have not connected yet.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gang_types::{FleetCatalog, FleetSet, Refusal, property};
use nativelink_error::{Error, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::action_messages::{ActionInfo, OperationId};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};
use parking_lot::Mutex;

/// The client-accepted fleets for calls passing through the historical scheduler.
#[derive(Clone, Default)]
pub(crate) struct AdmittedFleets(Arc<Mutex<HashMap<OperationId, FleetSet>>>);

/// Removes an admission if the scheduler has not already taken it.
#[must_use]
pub(crate) struct Admission<'a> {
    fleets: &'a AdmittedFleets,
    operation: OperationId,
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        let _ = self.fleets.take(&self.operation);
    }
}

impl AdmittedFleets {
    /// Inserts an admission that is removed when its guard is dropped.
    pub(crate) fn insert(&self, operation: OperationId, fleets: FleetSet) -> Admission<'_> {
        self.0.lock().insert(operation.clone(), fleets);
        Admission {
            fleets: self,
            operation,
        }
    }

    pub(crate) fn take(&self, operation: &OperationId) -> Option<FleetSet> {
        self.0.lock().remove(operation)
    }
}

/// The error a client gets when no declared fleet accepts its action, naming what was refused.
pub(crate) fn refused<'r>(
    fleets: &FleetCatalog,
    request: impl Fn(&str) -> Option<&'r str>,
) -> Error {
    let refusal = fleets.refusal(request).unwrap_or(Refusal::Combination);
    make_input_err!(
        "no fleet accepts this action: {refusal}; change its exec_properties or declare a fleet that accepts them"
    )
}

/// Computes the millicore reservation needed for an explicit core count, if it must increase.
pub(crate) fn normalized_cpu_reservation(properties: &HashMap<String, String>) -> Option<u64> {
    let count = properties.get(property::CPU_COUNT)?.parse::<u64>().ok()?;
    if count == 0 {
        return None;
    }
    let required = count.saturating_mul(property::MILLICORES_PER_CORE);
    let existing = properties
        .get(property::CPU_MILLICORES)
        .and_then(|value| value.parse::<u64>().ok());
    existing
        .is_none_or(|millicores| millicores < required)
        .then_some(required)
}

/// Forwards scheduler calls while refusing actions that cannot run on any declared fleet.
pub(crate) struct PlaceableActions {
    inner: Arc<dyn KnownPlatformPropertyProvider>,
    fleets: FleetCatalog,
    admitted: AdmittedFleets,
}

impl PlaceableActions {
    /// Wraps the action scheduler with the fleet catalog used for placement.
    pub(crate) fn new(
        inner: Arc<dyn KnownPlatformPropertyProvider>,
        fleets: FleetCatalog,
        admitted: AdmittedFleets,
    ) -> Self {
        Self {
            inner,
            fleets,
            admitted,
        }
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
        mut action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        let properties = |key: &str| action_info.platform_properties.get(key).map(String::as_str);
        let admitted = self.fleets.accepting(properties);
        if admitted.is_empty() {
            return Err(refused(&self.fleets, properties));
        }
        if let Some(millicores) = normalized_cpu_reservation(&action_info.platform_properties) {
            Arc::make_mut(&mut action_info)
                .platform_properties
                .insert(property::CPU_MILLICORES.to_owned(), millicores.to_string());
        }
        let _admission = self.admitted.insert(client_operation_id.clone(), admitted);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use gang_types::{Bytes, FleetName, FleetProperties, Isa, Resources, property};
    use nativelink_error::Code;
    use nativelink_util::action_messages::{ActionState, ActionUniqueKey, ActionUniqueQualifier};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::origin_event::OriginMetadata;
    use parking_lot::Mutex;

    use super::*;
    use crate::enforcement::{MemoryTuning, MinReservation};
    use crate::fitted::FittedActions;

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<Arc<ActionInfo>>>,
        fail: AtomicBool,
        observed_admitted: Mutex<Option<AdmittedFleets>>,
        saw_admitted: AtomicBool,
    }

    struct ResultRow;

    #[async_trait]
    impl ActionStateResult for ResultRow {
        async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Err(make_input_err!("not used"))
        }
        async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Err(make_input_err!("not used"))
        }
        async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
            Err(make_input_err!("not used"))
        }
    }

    impl MetricsComponent for Fake {
        fn publish(
            &self,
            _: MetricKind,
            _: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    impl RootMetricsComponent for Fake {
        fn publish(
            &self,
            _: MetricKind,
            _: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl ClientStateManager for Fake {
        async fn add_action(
            &self,
            operation: OperationId,
            info: Arc<ActionInfo>,
        ) -> Result<Box<dyn ActionStateResult>, Error> {
            if let Some(admitted) = self.observed_admitted.lock().as_ref() {
                let map = admitted.0.lock();
                self.saw_admitted
                    .store(map.contains_key(&operation), Ordering::Relaxed);
            }
            self.calls.lock().push(info);
            if self.fail.load(Ordering::Relaxed) {
                Err(make_input_err!("fake failure"))
            } else {
                Ok(Box::new(ResultRow))
            }
        }
        async fn filter_operations(
            &self,
            _: OperationFilter,
        ) -> Result<ActionStateResultStream<'_>, Error> {
            Err(make_input_err!("not used"))
        }
    }

    #[async_trait]
    impl KnownPlatformPropertyProvider for Fake {
        async fn get_known_properties(&self, _: &str) -> Result<Vec<String>, Error> {
            Ok(Vec::new())
        }
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("fleet-a").expect("valid fleet"),
            Isa::X86_64,
            &"docker",
            Resources {
                vcpus: 15,
                memory: Bytes::new(1024),
                disk: Bytes::new(1024),
            },
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid catalog")
    }

    fn info(key: &str, value: &str) -> Arc<ActionInfo> {
        let digest = DigestInfo::new([1; 32], 1);
        Arc::new(ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: [(key.to_owned(), value.to_owned())].into_iter().collect(),
            priority: 0,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH,
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest,
            }),
        })
    }

    fn operation() -> OperationId {
        OperationId::String("op".to_owned())
    }

    #[test]
    fn an_abandoned_admission_is_removed() {
        let admitted = AdmittedFleets::default();
        let fleets = catalog();
        {
            let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
            assert!(admitted.take(&operation()).is_some());
        }
        {
            let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
        }
        assert!(admitted.take(&operation()).is_none());
    }

    #[tokio::test]
    async fn refuses_client_request_above_every_fleet() {
        let fake = Arc::new(Fake::default());
        let wrapper = PlaceableActions::new(fake.clone(), catalog(), AdmittedFleets::default());
        let Err(error) = wrapper
            .add_action(operation(), info(property::CPU_COUNT, "99"))
            .await
        else {
            panic!("unplaceable request accepted");
        };
        assert_eq!(error.code, Code::InvalidArgument);
        let message = error.to_string();
        assert!(message.contains("cpu_count") && message.contains("99") && message.contains("15"));
        assert!(fake.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn fits_learned_reservations_but_preserves_values_within_fleet() {
        let fake = Arc::new(Fake::default());
        let fleets = catalog();
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets.clone(),
            admitted.clone(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        for (value, expected) in [("871000", "15000"), ("12000", "12000")] {
            let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
            fitted
                .add_action(operation(), info(property::CPU_MILLICORES, value))
                .await
                .expect("fitted");
            assert_eq!(
                fake.calls
                    .lock()
                    .last()
                    .expect("recorded")
                    .platform_properties[property::CPU_MILLICORES],
                expected
            );
        }
        let outer = PlaceableActions::new(Arc::new(fitted), fleets, admitted);
        outer
            .add_action(operation(), info(property::CPU_MILLICORES, "14000"))
            .await
            .expect("accepted");
        assert_eq!(
            fake.calls
                .lock()
                .last()
                .expect("recorded")
                .platform_properties[property::CPU_MILLICORES],
            "14000"
        );
    }

    #[tokio::test]
    async fn oversized_reservation_fits_the_largest_of_two_fleets() {
        let fake = Arc::new(Fake::default());
        let fleets = FleetCatalog::try_new(
            [("small", 4, 8_u64), ("large", 16, 32)]
                .into_iter()
                .map(|(name, vcpus, gib)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).expect("valid fleet"),
                        Isa::X86_64,
                        &"docker",
                        Resources {
                            vcpus,
                            memory: Bytes::new(gib << 30),
                            disk: Bytes::new(100_u64 << 30),
                        },
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("valid catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets.clone(),
            admitted.clone(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "35232154"))
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls
                .lock()
                .last()
                .expect("recorded")
                .platform_properties[property::MEMORY_KB],
            "33554432"
        );
    }

    fn large_memory_catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("fleet-a").expect("valid fleet"),
            Isa::X86_64,
            &"docker",
            Resources {
                vcpus: 15,
                memory: Bytes::gib(64),
                disk: Bytes::gib(100),
            },
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid catalog")
    }

    #[tokio::test]
    async fn fitted_raises_a_tiny_learned_reservation_to_the_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
            AdmittedFleets::default(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "252"))
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
            "65536"
        );
    }

    #[tokio::test]
    async fn fitted_uses_a_custom_memory_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
            AdmittedFleets::default(),
            MinReservation::from_units(4096),
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        for (value, expected) in [("252", "4096"), ("26864", "26864")] {
            fitted
                .add_action(operation(), info(property::MEMORY_KB, value))
                .await
                .expect("fitted");
            assert_eq!(
                fake.calls
                    .lock()
                    .last()
                    .expect("recorded")
                    .platform_properties[property::MEMORY_KB],
                expected
            );
        }
    }

    #[tokio::test]
    async fn fitted_keeps_a_reservation_above_the_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
            AdmittedFleets::default(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "35232154"))
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
            "35232154"
        );
    }

    #[tokio::test]
    async fn fitted_leaves_an_action_without_memory_kb_alone() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
            AdmittedFleets::default(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        fitted
            .add_action(operation(), info(property::CPU_COUNT, "1"))
            .await
            .expect("fitted");
        assert!(
            !fake.calls.lock()[0]
                .platform_properties
                .contains_key(property::MEMORY_KB)
        );
    }

    #[tokio::test]
    async fn fitted_leaves_a_zero_memory_kb_alone() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
            AdmittedFleets::default(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "0"))
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
            "0"
        );
    }

    #[tokio::test]
    async fn fitted_disk_floor_preserves_zero_and_larger_reservations() {
        let fake = Arc::new(Fake::default());
        let fleets = large_memory_catalog();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets,
            AdmittedFleets::default(),
            MemoryTuning::default().min_reservation,
            crate::enforcement::DiskTuning::default().min_reservation,
        );
        for (value, expected) in [("1627", "1048576"), ("0", "0"), ("2097152", "2097152")] {
            let mut action = info(property::DISK_KB, value);
            Arc::make_mut(&mut action)
                .platform_properties
                .insert(property::MEMORY_KB.to_owned(), "252".to_owned());
            fitted
                .add_action(operation(), action)
                .await
                .expect("fitted");
            let calls = fake.calls.lock();
            let properties = &calls.last().expect("recorded").platform_properties;
            assert_eq!(properties[property::DISK_KB], expected);
            assert_eq!(properties[property::MEMORY_KB], "65536");
        }
    }

    #[tokio::test]
    async fn admitted_entry_removed_after_success_and_failure() {
        let fake = Arc::new(Fake::default());
        let admitted = AdmittedFleets::default();
        *fake.observed_admitted.lock() = Some(admitted.clone());
        let outer = PlaceableActions::new(fake.clone(), catalog(), admitted.clone());
        for fail in [false, true] {
            fake.fail.store(fail, Ordering::Relaxed);
            assert_eq!(
                outer
                    .add_action(operation(), info(property::CPU_COUNT, "1"))
                    .await
                    .is_err(),
                fail
            );
            assert!(fake.saw_admitted.swap(false, Ordering::Relaxed));
            assert!(admitted.take(&operation()).is_none());
        }
    }
}
