//! Rejects actions no declared fleet can run before they enter the scheduler.
//!
//! Gang scales from zero, so `NativeLink`'s worker-based unsatisfiable check cannot see fleets
//! whose workers have not connected yet.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gang_types::{FleetSet, Refusal, property};
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

use crate::capacity::RuntimeCatalog;

type AdmissionMap = HashMap<OperationId, AdmittedAction>;

/// Original client intent retained across historical resource learning.
pub(crate) struct AdmittedAction {
    pub(crate) fleets: FleetSet,
    pub(crate) minimum_kb: Option<u64>,
}

/// Client-accepted fleets and the original memory floor, carried through historical learning.
#[derive(Clone, Default)]
pub(crate) struct AdmittedFleets(Arc<Mutex<AdmissionMap>>);

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
    pub(crate) fn insert(
        &self,
        operation: OperationId,
        fleets: FleetSet,
        minimum_kb: Option<u64>,
    ) -> Admission<'_> {
        self.0
            .lock()
            .insert(operation.clone(), AdmittedAction { fleets, minimum_kb });
        Admission {
            fleets: self,
            operation,
        }
    }

    pub(crate) fn take(&self, operation: &OperationId) -> Option<AdmittedAction> {
        self.0.lock().remove(operation)
    }
}

/// The error a client gets when no declared fleet accepts its action, naming what was refused.
pub(crate) fn refused(refusal: &Refusal) -> Error {
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
    fleets: Arc<RuntimeCatalog>,
    admitted: AdmittedFleets,
}

impl PlaceableActions {
    /// Wraps the action scheduler with the fleet catalog used for placement.
    pub(crate) fn new(
        inner: Arc<dyn KnownPlatformPropertyProvider>,
        fleets: Arc<RuntimeCatalog>,
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
        if action_info
            .platform_properties
            .get(property::NETWORK)
            .is_some_and(|value| value.is_empty() || value == "not-requested")
        {
            Arc::make_mut(&mut action_info)
                .platform_properties
                .remove(property::NETWORK);
        }
        let fleets = self.fleets.current()?;
        let admitted = fleets
            .admit(
                action_info
                    .platform_properties
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .map_err(|refusal| refused(&refusal))?;
        let minimum_kb = action_info
            .platform_properties
            .get(property::MEMORY_KB)
            .and_then(|value| value.parse().ok());
        let _admission = self
            .admitted
            .insert(client_operation_id.clone(), admitted, minimum_kb);
        if let Some(millicores) = normalized_cpu_reservation(&action_info.platform_properties) {
            Arc::make_mut(&mut action_info)
                .platform_properties
                .insert(property::CPU_MILLICORES.to_owned(), millicores.to_string());
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, Isa, Resources, property};
    use nativelink_error::Code;
    use nativelink_util::action_messages::{ActionState, ActionUniqueKey, ActionUniqueQualifier};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::origin_event::OriginMetadata;
    use parking_lot::Mutex;

    use super::*;
    use crate::fitted::FittedActions;
    use gang_config::{DiskMinReservation, MinReservation};

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
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 15,
                memory: Bytes::new(1024),
                disk: Bytes::new(1024),
            },
            &gang_types::FleetActionPolicy::default(),
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid catalog")
    }

    fn info(key: &str, value: &str) -> Arc<ActionInfo> {
        let digest = DigestInfo::new([1; 32], 1);
        Arc::new(ActionInfo {
            memory_reservation: None,
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
            platform_properties: [
                (
                    property::ACTION_ENVIRONMENT.to_owned(),
                    "host:test".to_owned(),
                ),
                (key.to_owned(), value.to_owned()),
            ]
            .into_iter()
            .collect(),
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
            let _admission = admitted.insert(
                operation(),
                fleets
                    .admit(std::iter::empty())
                    .expect("baseline admission"),
                None,
            );
            assert!(admitted.take(&operation()).is_some());
        }
        {
            let _admission = admitted.insert(
                operation(),
                fleets
                    .admit(std::iter::empty())
                    .expect("baseline admission"),
                None,
            );
        }
        assert!(admitted.take(&operation()).is_none());
    }

    #[tokio::test]
    async fn refuses_client_request_above_every_fleet() {
        let fake = Arc::new(Fake::default());
        let wrapper = PlaceableActions::new(
            fake.clone(),
            crate::test_catalog(catalog()),
            AdmittedFleets::default(),
        );
        let Err(error) = wrapper
            .add_action(operation(), info(property::CPU_COUNT, "99"))
            .await
        else {
            panic!("unplaceable request accepted");
        };
        assert_eq!(error.code, Code::InvalidArgument);
        let message = error.to_string();
        assert!(
            message.contains("cpu_count") && message.contains("99") && message.contains("fleet-a")
        );
        assert!(fake.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn clients_need_no_environment_and_old_labels_are_ignored() {
        let fake = Arc::new(Fake::default());
        let wrapper = PlaceableActions::new(
            fake.clone(),
            crate::test_catalog(catalog()),
            AdmittedFleets::default(),
        );
        for label in [None, Some(""), Some("legacy"), Some("host:old")] {
            let mut request = info(property::ISA, "x86-64");
            let properties = &mut Arc::make_mut(&mut request).platform_properties;
            properties.remove(property::ACTION_ENVIRONMENT);
            if let Some(value) = label {
                properties.insert(property::ACTION_ENVIRONMENT.to_owned(), value.to_owned());
            }
            wrapper
                .add_action(operation(), request)
                .await
                .expect("environment-free admission");
        }
        assert_eq!(fake.calls.lock().len(), 4);
    }

    #[tokio::test]
    async fn fits_learned_reservations_but_preserves_values_within_fleet() {
        let fake = Arc::new(Fake::default());
        let fleets = catalog();
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        for (value, expected) in [("871000", "15000"), ("12000", "12000")] {
            let _admission = admitted.insert(
                operation(),
                fleets
                    .admit(std::iter::empty())
                    .expect("baseline admission"),
                None,
            );
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
            assert_eq!(
                fake.calls
                    .lock()
                    .last()
                    .unwrap()
                    .memory_reservation
                    .as_ref()
                    .unwrap()
                    .cpu_before_memory_fit,
                Some(expected.parse::<u64>().unwrap()),
            );
        }
        let outer = PlaceableActions::new(Arc::new(fitted), crate::test_catalog(fleets), admitted);
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
                        &"host:test".parse().expect("valid environment"),
                        Resources {
                            vcpus,
                            memory: Bytes::new(gib << 30),
                            disk: Bytes::new(100_u64 << 30),
                        },
                        &gang_types::FleetActionPolicy::default(),
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("valid catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(
            operation(),
            fleets
                .admit(std::iter::empty())
                .expect("baseline admission"),
            None,
        );
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
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 15,
                memory: Bytes::gib(64),
                disk: Bytes::gib(100),
            },
            &gang_types::FleetActionPolicy::default(),
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("valid catalog")
    }

    fn measured_catalog() -> (
        tokio::sync::watch::Sender<
            Arc<nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot>,
        >,
        Arc<RuntimeCatalog>,
    ) {
        let mut fleets = large_memory_catalog().fleets().to_vec();
        fleets[0].set_memory_capacity(gang_types::NonZeroBytes::gib(63));
        let (sender, receiver) = tokio::sync::watch::channel(Arc::new(
            nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot::new(Vec::new()),
        ));
        let runtime = Arc::new(RuntimeCatalog::new(
            FleetCatalog::try_new(fleets).unwrap(),
            receiver,
        ));
        (sender, runtime)
    }

    fn observe_memory(
        sender: &tokio::sync::watch::Sender<
            Arc<nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot>,
        >,
        memory_kb: u64,
    ) {
        let mut row = nativelink_config::schedulers::ResourceCapacity::default();
        row.exact
            .insert(property::FLEET.to_owned(), "fleet-a".to_owned());
        row.minimum
            .insert(property::MEMORY_KB.to_owned(), memory_kb);
        sender
            .send(Arc::new(
                nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot::new(vec![row]),
            ))
            .unwrap();
    }

    #[tokio::test]
    async fn measured_fit_keeps_original_floor_and_unclamped_desired_memory() {
        let fake = Arc::new(Fake::default());
        let (sender, runtime) = measured_catalog();
        observe_memory(&sender, 62_229 * 1024);
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            Arc::clone(&runtime),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(
            operation(),
            runtime.current().unwrap().accepting(|_| None),
            Some(2 * 1024 * 1024),
        );
        let mut learned = info(property::MEMORY_KB, "73400320");
        Arc::make_mut(&mut learned)
            .platform_properties
            .insert(property::CPU_MILLICORES.to_owned(), "1500".to_owned());
        Arc::make_mut(&mut learned).memory_reservation =
            Some(nativelink_util::action_messages::MemoryReservation {
                minimum_kb: Some(u64::MAX),
                desired_kb: Some(1),
                cpu_before_memory_fit: Some(u64::MAX),
                fitted_revision: None,
            });
        fitted
            .add_action(operation(), learned)
            .await
            .expect("learned fit");
        let calls = fake.calls.lock();
        let action = &calls[0];
        assert_eq!(
            action.platform_properties[property::MEMORY_KB],
            (62_229 * 1024).to_string()
        );
        assert_eq!(action.platform_properties[property::CPU_MILLICORES], "1500");
        let reservation = action.memory_reservation.as_ref().unwrap();
        assert_eq!(reservation.minimum_kb, Some(2 * 1024 * 1024));
        assert_eq!(reservation.desired_kb, Some(73_400_320));
        assert_eq!(reservation.cpu_before_memory_fit, Some(1500));
        assert_eq!(reservation.fitted_revision, None);
    }

    #[tokio::test]
    async fn observed_shrink_never_clamps_an_explicit_admission_floor() {
        let fake = Arc::new(Fake::default());
        let (sender, runtime) = measured_catalog();
        let admitted = AdmittedFleets::default();
        let mask = runtime.current().unwrap().accepting(|_| None);
        let floor = 63 * 1024 * 1024;
        let _admission = admitted.insert(operation(), mask, Some(floor));
        observe_memory(&sender, 62_229 * 1024);
        let fitted = FittedActions::new(
            fake.clone(),
            Arc::clone(&runtime),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        assert!(
            fitted
                .add_action(operation(), info(property::MEMORY_KB, &floor.to_string()))
                .await
                .is_err()
        );
        assert!(fake.calls.lock().is_empty());
        let placeable = PlaceableActions::new(fake.clone(), runtime, admitted);
        assert!(
            placeable
                .add_action(operation(), info(property::MEMORY_KB, &floor.to_string()))
                .await
                .is_err()
        );
        assert!(fake.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn physical_memory_equality_preserves_cpu_for_authoritative_native_fit() {
        let fake = Arc::new(Fake::default());
        let (sender, runtime) = measured_catalog();
        let memory = 62_229 * 1024;
        observe_memory(&sender, memory);
        let fitted = FittedActions::new(
            fake.clone(),
            runtime,
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let mut action = info(property::MEMORY_KB, &memory.to_string());
        Arc::make_mut(&mut action)
            .platform_properties
            .insert(property::CPU_MILLICORES.to_owned(), "1000".to_owned());
        fitted
            .add_action(operation(), action)
            .await
            .expect("equal physical fit");
        let calls = fake.calls.lock();
        assert_eq!(
            calls[0].platform_properties[property::CPU_MILLICORES],
            "1000"
        );
        assert_eq!(
            calls[0]
                .memory_reservation
                .as_ref()
                .unwrap()
                .cpu_before_memory_fit,
            Some(1000)
        );
    }

    #[tokio::test]
    async fn learned_peak_clamps_memory_and_delegates_whole_cpu_to_native() {
        let fake = Arc::new(Fake::default());
        let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("eight-gib").expect("fleet name"),
            Isa::X86_64,
            &"docker",
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 6,
                memory: Bytes::gib(8),
                disk: Bytes::gib(20),
            },
            &gang_types::FleetActionPolicy::default(),
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            gang_config::DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(
            operation(),
            fleets
                .admit(std::iter::empty())
                .expect("baseline admission"),
            None,
        );
        // ceil(7 GiB in KiB * 120 / 100), as supplied by historical learning.
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "8808039"))
            .await
            .expect("accepted");
        let calls = fake.calls.lock();
        let properties = &calls[0].platform_properties;
        assert_eq!(properties[property::MEMORY_KB], "8388608");
        assert!(!properties.contains_key(property::CPU_MILLICORES));
        assert_eq!(
            calls[0]
                .memory_reservation
                .as_ref()
                .unwrap()
                .cpu_before_memory_fit,
            None
        );
    }

    #[tokio::test]
    async fn lower_action_memory_ceiling_does_not_reserve_whole_worker_cpu() {
        let fake = Arc::new(Fake::default());
        let mut policy = gang_types::FleetActionPolicy::default();
        policy.resources.memory = Some(gang_types::NonZeroBytes::gib(4));
        let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("limited").expect("fleet name"),
            Isa::X86_64,
            &"docker",
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 6,
                memory: Bytes::gib(8),
                disk: Bytes::gib(20),
            },
            &policy,
            [],
        )])
        .expect("catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(operation(), fleets.admit([]).expect("baseline"), None);
        let mut action = info(property::MEMORY_KB, "6000000");
        Arc::make_mut(&mut action)
            .platform_properties
            .insert(property::CPU_MILLICORES.to_owned(), "1000".to_owned());
        fitted
            .add_action(operation(), action)
            .await
            .expect("fitted");
        let calls = fake.calls.lock();
        assert_eq!(calls[0].platform_properties[property::MEMORY_KB], "4194304");
        assert_eq!(
            calls[0].platform_properties[property::CPU_MILLICORES],
            "1000"
        );
    }

    #[tokio::test]
    async fn admission_rejects_unknown_and_normalizes_only_baseline_network() {
        let fake = Arc::new(Fake::default());
        let outer = PlaceableActions::new(
            fake.clone(),
            crate::test_catalog(catalog()),
            AdmittedFleets::default(),
        );
        assert!(
            outer
                .add_action(operation(), info("unknown-property", "value"))
                .await
                .is_err()
        );
        assert!(
            outer
                .add_action(operation(), info(property::NETWORK, "requested"))
                .await
                .is_err()
        );
        for baseline in ["", "not-requested"] {
            let action = info(property::NETWORK, baseline);
            let digest = action.unique_qualifier.clone();
            outer
                .add_action(operation(), action)
                .await
                .expect("baseline admitted");
            let calls = fake.calls.lock();
            let observed = calls.last().expect("forwarded");
            assert!(!observed.platform_properties.contains_key(property::NETWORK));
            assert_eq!(observed.unique_qualifier, digest);
        }
    }

    #[tokio::test]
    async fn learned_seventy_gib_reservation_clamps_to_sixty_four_gib() {
        let fake = Arc::new(Fake::default());
        let fleets = large_memory_catalog();
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            gang_config::DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(
            operation(),
            fleets
                .admit(std::iter::empty())
                .expect("baseline admission"),
            None,
        );
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "73400320"))
            .await
            .expect("accepted");
        let calls = fake.calls.lock();
        let properties = &calls[0].platform_properties;
        assert_eq!(properties[property::MEMORY_KB], "67108864");
        assert!(!properties.contains_key(property::CPU_MILLICORES));
    }

    #[tokio::test]
    async fn memory_clamp_uses_largest_fleet_eligible_for_other_properties() {
        let fake = Arc::new(Fake::default());
        let fleets = FleetCatalog::try_new(
            [("large", 8, 32, 10), ("eligible", 4, 8, 30)]
                .into_iter()
                .map(|(name, vcpus, memory, disk)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).expect("fleet name"),
                        Isa::X86_64,
                        &"docker",
                        &"host:test".parse().expect("valid environment"),
                        Resources {
                            vcpus,
                            memory: Bytes::gib(memory),
                            disk: Bytes::gib(disk),
                        },
                        &gang_types::FleetActionPolicy::default(),
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(fleets.clone()),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(
            operation(),
            fleets
                .admit(std::iter::empty())
                .expect("baseline admission"),
            None,
        );
        let mut action = info(property::MEMORY_KB, "40000000");
        Arc::make_mut(&mut action).platform_properties.insert(
            property::DISK_KB.to_owned(),
            (20_u64 * 1024 * 1024).to_string(),
        );
        fitted
            .add_action(operation(), action)
            .await
            .expect("accepted");
        let calls = fake.calls.lock();
        let properties = &calls[0].platform_properties;
        assert_eq!(properties[property::MEMORY_KB], "8388608");
        assert!(!properties.contains_key(property::CPU_MILLICORES));
        assert_eq!(properties[property::DISK_KB], "20971520");
    }

    #[tokio::test]
    async fn fitted_raises_a_tiny_learned_reservation_to_the_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(large_memory_catalog()),
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "252"))
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
            "8192"
        );
    }

    #[tokio::test]
    async fn fitted_uses_a_custom_memory_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            crate::test_catalog(large_memory_catalog()),
            AdmittedFleets::default(),
            MinReservation::from_units(4096),
            DiskMinReservation::from_units(2048),
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
            crate::test_catalog(large_memory_catalog()),
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
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
            crate::test_catalog(large_memory_catalog()),
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
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
            crate::test_catalog(large_memory_catalog()),
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
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
            crate::test_catalog(fleets),
            AdmittedFleets::default(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        for (value, expected) in [("1627", "2048"), ("0", "0"), ("2097152", "2097152")] {
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
            assert_eq!(properties[property::MEMORY_KB], "8192");
        }
    }

    #[tokio::test]
    async fn admitted_entry_removed_after_success_and_failure() {
        let fake = Arc::new(Fake::default());
        let admitted = AdmittedFleets::default();
        *fake.observed_admitted.lock() = Some(admitted.clone());
        let outer = PlaceableActions::new(
            fake.clone(),
            crate::test_catalog(catalog()),
            admitted.clone(),
        );
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
