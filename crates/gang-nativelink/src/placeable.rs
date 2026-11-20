//! Rejects actions no declared fleet can run before they enter the scheduler.
//!
//! Gang scales from zero, so `NativeLink`'s worker-based unsatisfiable check cannot see fleets
//! whose workers have not connected yet.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gang_clock::{Clock, Tick};
use gang_types::{
    ActionEnvironment, Bytes, FleetCatalog, FleetName, FleetProperties, FleetSet, Isa, MatchKind,
    Refusal, Resources, property,
};
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, RootMetricsComponent,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::worker_scheduler::{WorkerScheduler, WorkerSummary};
use nativelink_util::action_messages::{ActionInfo, OperationId};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
};
use parking_lot::Mutex;

type AdmissionMap = HashMap<OperationId, (FleetSet, Option<FleetCatalog>)>;

/// The client-accepted fleets for calls passing through the historical scheduler.
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
    pub(crate) fn insert(&self, operation: OperationId, fleets: FleetSet) -> Admission<'_> {
        self.insert_with_catalog(operation, fleets, None)
    }

    fn insert_with_catalog(
        &self,
        operation: OperationId,
        fleets: FleetSet,
        catalog: Option<FleetCatalog>,
    ) -> Admission<'_> {
        self.0.lock().insert(operation.clone(), (fleets, catalog));
        Admission {
            fleets: self,
            operation,
        }
    }

    pub(crate) fn take(&self, operation: &OperationId) -> Option<(FleetSet, Option<FleetCatalog>)> {
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
    workers: Option<Arc<dyn WorkerScheduler>>,
    reconnect: Option<(Arc<dyn Clock>, Tick)>,
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
            workers: None,
            reconnect: None,
        }
    }

    pub(crate) fn with_workers(
        mut self,
        workers: Arc<dyn WorkerScheduler>,
        clock: Arc<dyn Clock>,
        timeout: Duration,
    ) -> Self {
        let deadline = clock.monotonic() + timeout;
        self.workers = Some(workers);
        self.reconnect = Some((clock, deadline));
        self
    }
}

type WorkerProfile = (u64, u64, u64);
type SelectedWorker<'a> = (&'a WorkerSummary, Resources, WorkerProfile);

/// Reads the registered capacity, rather than the current launch image.
fn worker_resources(worker: &WorkerSummary) -> Option<(Resources, WorkerProfile)> {
    let p = &worker.platform_properties;
    let cpu = p.get(property::CPU_MILLICORES)?.parse::<u64>().ok()?;
    let memory = p.get(property::MEMORY_KB)?.parse::<u64>().ok()?;
    let disk = p.get(property::DISK_KB)?.parse::<u64>().ok()?;
    Some((
        Resources {
            vcpus: u32::try_from(cpu / property::MILLICORES_PER_CORE).ok()?,
            memory: Bytes::new(memory.saturating_mul(1024)),
            disk: Bytes::new(disk.saturating_mul(1024)),
        },
        (memory, disk, cpu),
    ))
}

fn worker_properties(
    worker: &WorkerSummary,
    base: &FleetProperties,
    resources: Resources,
) -> Option<FleetProperties> {
    let p = &worker.platform_properties;
    let name = FleetName::try_new(p.get(property::FLEET)?.as_str()).ok()?;
    let isa: Isa = p.get(property::ISA)?.parse().ok()?;
    let environment: ActionEnvironment = p.get(property::ACTION_ENVIRONMENT)?.parse().ok()?;
    let options = base
        .options()
        .filter_map(|(key, _)| p.get(key).map(|value| (key, value.clone())));
    Some(FleetProperties::new(
        &name,
        isa,
        p.get(property::SANDBOX)?,
        &environment,
        resources,
        options,
    ))
}

fn worker_accepts<'a>(
    worker: &WorkerSummary,
    fleets: &FleetCatalog,
    request: impl Fn(&str) -> Option<&'a str>,
) -> bool {
    fleets.declared().iter().all(|&(key, kind)| {
        let Some(requested) = request(key) else {
            return true;
        };
        if key == property::CPU_COUNT {
            return worker
                .platform_properties
                .get(property::CPU_MILLICORES)
                .and_then(|actual| {
                    Some((actual.parse::<u64>().ok()?, requested.parse::<u64>().ok()?))
                })
                .is_some_and(|(actual, count)| {
                    actual >= count.saturating_mul(property::MILLICORES_PER_CORE)
                });
        }
        match kind {
            MatchKind::Ignored => true,
            MatchKind::Exact => worker
                .platform_properties
                .get(key)
                .is_some_and(|actual| actual == requested),
            MatchKind::Minimum => worker
                .platform_properties
                .get(key)
                .and_then(|actual| {
                    Some((actual.parse::<u64>().ok()?, requested.parse::<u64>().ok()?))
                })
                .is_some_and(|(actual, requested)| actual >= requested),
        }
    })
}

/// Selects one actual compatible worker per configured fleet, in fleet preference order.
fn live_catalog<'a>(
    workers: &[WorkerSummary],
    fleets: &FleetCatalog,
    base: FleetSet,
    environment: &ActionEnvironment,
    request: impl Fn(&str) -> Option<&'a str> + Copy,
) -> Option<FleetCatalog> {
    let mut selected: Vec<Option<SelectedWorker<'_>>> = vec![None; fleets.len()];
    for worker in workers {
        let p = &worker.platform_properties;
        if worker.is_draining
            || p.get(property::ISA)
                .and_then(|value| value.parse::<Isa>().ok())
                .is_none()
            || p.get(property::SANDBOX).is_none()
            || p.get(property::ACTION_ENVIRONMENT).map(String::as_str) != Some(environment.as_str())
            || !worker_accepts(worker, fleets, request)
        {
            continue;
        }
        let Some(index) = p.get(property::FLEET).and_then(|name| {
            base.iter()
                .find(|&index| fleets.fleets()[index].name() == name.as_str())
        }) else {
            continue;
        };
        let Some((resources, profile)) = worker_resources(worker) else {
            continue;
        };
        // FleetProperties rounds CPU down to whole cores and saturates byte conversion.
        // Apply the same minimum matching as a one-row catalog before selecting a worker.
        let limits = [
            (property::CPU_COUNT, u64::from(resources.vcpus)),
            (
                property::CPU_MILLICORES,
                u64::from(resources.vcpus) * property::MILLICORES_PER_CORE,
            ),
            (property::MEMORY_KB, resources.memory.get() / 1024),
            (property::DISK_KB, resources.disk.get() / 1024),
        ];
        if limits.iter().any(|(key, limit)| {
            request(key)
                .is_some_and(|value| value.parse::<u64>().map_or(true, |value| value > *limit))
        }) {
            continue;
        }
        if selected[index]
            .as_ref()
            .is_none_or(|(_, _, old)| profile > *old)
        {
            selected[index] = Some((worker, resources, profile));
        }
    }
    let properties: Vec<_> = selected
        .into_iter()
        .enumerate()
        .filter_map(|(index, selected)| {
            let (worker, resources, _) = selected?;
            worker_properties(worker, &fleets.fleets()[index], resources)
        })
        .collect();
    if properties.is_empty() {
        None
    } else {
        // There is at most one entry per configured fleet, which already fits the catalog bound.
        FleetCatalog::try_new(properties).ok()
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
        let served = || {
            self.fleets
                .fleets()
                .iter()
                .filter_map(|fleet| fleet.get(property::ACTION_ENVIRONMENT))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let Some(environment) = properties(property::ACTION_ENVIRONMENT)
            .and_then(|value| value.parse::<ActionEnvironment>().ok())
        else {
            return Err(make_input_err!(
                "missing or invalid action environment; run gang setup; served identities: {}",
                served()
            ));
        };
        let admitted = self.fleets.accepting(properties);
        let mut live = None;
        if admitted.is_empty() {
            let base = self.fleets.accepting_base(properties);
            if base.is_empty() {
                return Err(refused(&self.fleets, |key| {
                    if key == property::ACTION_ENVIRONMENT {
                        None
                    } else {
                        properties(key)
                    }
                }));
            }
            if let Some(workers) = &self.workers {
                live = live_catalog(
                    &workers.worker_snapshot().await,
                    &self.fleets,
                    base,
                    &environment,
                    properties,
                )
                .filter(|catalog| !catalog.accepting(properties).is_empty());
            }
            if live.is_none() {
                if self
                    .reconnect
                    .as_ref()
                    .is_some_and(|(clock, deadline)| clock.monotonic() < *deadline)
                {
                    return Err(make_err!(
                        Code::Unavailable,
                        "action environment not yet served while workers reconnect; retry; served identities: {}",
                        served()
                    ));
                }
                return Err(make_input_err!(
                    "action environment is not served; run gang setup; served identities: {}",
                    served()
                ));
            }
        }
        let _admission = if let Some(catalog) = live {
            let mask = catalog.accepting(properties);
            self.admitted
                .insert_with_catalog(client_operation_id.clone(), mask, Some(catalog))
        } else {
            self.admitted.insert(client_operation_id.clone(), admitted)
        };
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

    use gang_types::{Bytes, FleetName, FleetProperties, Isa, Resources, property};
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
    async fn invalid_identity_is_refused_with_setup_guidance() {
        let fake = Arc::new(Fake::default());
        let wrapper = PlaceableActions::new(fake.clone(), catalog(), AdmittedFleets::default());
        for value in ["", "legacy", "host:"] {
            let error = wrapper
                .add_action(operation(), info(property::ACTION_ENVIRONMENT, value))
                .await
                .err()
                .expect("invalid identity");
            assert_eq!(error.code, Code::InvalidArgument);
            let message = error.to_string();
            assert!(message.contains("gang setup"));
            assert!(message.contains("host:test"));
        }
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
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
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
    async fn live_worker_capacity_bounds_learned_reservations() {
        let fake = Arc::new(Fake::default());
        let fleets = catalog();
        let worker = WorkerSummary {
            id: "worker".to_owned(),
            running_actions: 0,
            max_inflight_tasks: 1,
            is_paused: false,
            is_draining: false,
            last_update_timestamp: 0,
            platform_properties: [
                (property::FLEET, "fleet-a"),
                (property::ISA, "x86-64"),
                (property::SANDBOX, "docker"),
                (property::ACTION_ENVIRONMENT, "host:old"),
                (property::CPU_COUNT, "4"),
                (property::CPU_MILLICORES, "4000"),
                (property::MEMORY_KB, "8192"),
                (property::DISK_KB, "4096"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
            available_platform_properties: HashMap::new(),
            free_memory_kb: None,
        };
        let live = live_catalog(
            &[worker],
            &fleets,
            fleets.accepting(|_| None),
            &"host:old".parse().expect("valid environment"),
            |_| None,
        )
        .expect("valid worker");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets,
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission =
            admitted.insert_with_catalog(operation(), live.accepting(|_| None), Some(live));
        let mut request = info(property::MEMORY_KB, "50000");
        Arc::make_mut(&mut request).platform_properties.insert(
            property::ACTION_ENVIRONMENT.to_owned(),
            "host:old".to_owned(),
        );
        fitted
            .add_action(operation(), request)
            .await
            .expect("fitted");
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
            "8192"
        );
        assert_eq!(
            fake.calls.lock()[0].platform_properties[property::CPU_MILLICORES],
            "4000"
        );
    }

    fn live_test_worker(name: &str, memory_gib: u64, disk_gib: u64, cores: u64) -> WorkerSummary {
        WorkerSummary {
            id: name.to_owned(),
            running_actions: 0,
            max_inflight_tasks: 1,
            is_paused: false,
            is_draining: false,
            last_update_timestamp: 0,
            platform_properties: [
                (property::FLEET, name.to_owned()),
                (property::ISA, "x86-64".to_owned()),
                (property::SANDBOX, "docker".to_owned()),
                (property::ACTION_ENVIRONMENT, "host:old".to_owned()),
                (property::CPU_COUNT, cores.to_string()),
                (property::CPU_MILLICORES, (cores * 1000).to_string()),
                (property::MEMORY_KB, (memory_gib << 20).to_string()),
                (property::DISK_KB, (disk_gib << 20).to_string()),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
            available_platform_properties: HashMap::new(),
            free_memory_kb: None,
        }
    }

    fn live_test_workers() -> Vec<WorkerSummary> {
        let mut draining = live_test_worker("small", 128, 100, 32);
        draining.is_draining = true;
        let mut malformed = live_test_worker("small", 128, 100, 32);
        malformed
            .platform_properties
            .insert(property::ISA.to_owned(), "bad".to_owned());
        let mut malformed_capacity = live_test_worker("small", 128, 100, 32);
        malformed_capacity
            .platform_properties
            .insert(property::MEMORY_KB.to_owned(), "bad".to_owned());
        let mut malformed_environment = live_test_worker("small", 128, 100, 32);
        malformed_environment
            .platform_properties
            .insert(property::ACTION_ENVIRONMENT.to_owned(), "host:".to_owned());
        let mut incompatible = live_test_worker("small", 128, 100, 32);
        incompatible
            .platform_properties
            .insert(property::SANDBOX.to_owned(), "other".to_owned());
        let mut wrong_environment = live_test_worker("small", 128, 100, 32);
        wrong_environment.platform_properties.insert(
            property::ACTION_ENVIRONMENT.to_owned(),
            "host:other".to_owned(),
        );
        vec![
            live_test_worker("small", 8, 100, 8),
            live_test_worker("large", 8, 100, 8),
            live_test_worker("large", 64, 20, 32),
            live_test_worker("large", 64, 30, 3),
            live_test_worker("large", 64, 30, 4),
            draining,
            malformed_capacity,
            malformed_environment,
            malformed,
            incompatible,
            wrong_environment,
        ]
    }

    #[tokio::test]
    async fn live_catalog_keeps_all_fleets_and_real_largest_worker_profiles() {
        let fleets = FleetCatalog::try_new(
            [("small", 8_u64), ("large", 64)]
                .into_iter()
                .map(|(name, memory)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).expect("fleet name"),
                        Isa::X86_64,
                        &"docker",
                        &"host:new".parse().expect("environment"),
                        Resources {
                            vcpus: 16,
                            memory: Bytes::gib(memory),
                            disk: Bytes::gib(100),
                        },
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("catalog");
        let mut workers = live_test_workers();
        let environment: ActionEnvironment = "host:old".parse().expect("environment");
        let mut client = info(property::SANDBOX, "docker");
        Arc::make_mut(&mut client).platform_properties.insert(
            property::ACTION_ENVIRONMENT.to_owned(),
            "host:old".to_owned(),
        );
        let request = |key: &str| client.platform_properties.get(key).map(String::as_str);
        let base = fleets.accepting_base(request);
        let mut previous = None;
        for _ in 0..2 {
            let live =
                live_catalog(&workers, &fleets, base, &environment, request).expect("live catalog");
            assert_eq!(
                live.fleets()
                    .iter()
                    .map(FleetProperties::name)
                    .collect::<Vec<_>>(),
                ["small", "large"]
            );
            assert_eq!(live.fleets()[0].limit(property::MEMORY_KB), Some(8 << 20));
            assert_eq!(live.fleets()[1].limit(property::MEMORY_KB), Some(64 << 20));
            assert_eq!(live.fleets()[1].limit(property::DISK_KB), Some(30 << 20));
            assert_eq!(live.fleets()[1].limit(property::CPU_MILLICORES), Some(4000));
            if let Some(previous) = &previous {
                assert_eq!(previous, &live);
            }
            previous = Some(live.clone());

            let fake = Arc::new(Fake::default());
            let admitted = AdmittedFleets::default();
            let fitted = FittedActions::new(
                fake.clone(),
                fleets.clone(),
                admitted.clone(),
                MinReservation::from_units(8192),
                DiskMinReservation::from_units(2048),
            );
            let _admission =
                admitted.insert_with_catalog(operation(), live.accepting(request), Some(live));
            let mut learned = client.clone();
            Arc::make_mut(&mut learned)
                .platform_properties
                .insert(property::MEMORY_KB.to_owned(), (32_u64 << 20).to_string());
            fitted
                .add_action(operation(), learned)
                .await
                .expect("fitted");
            assert_eq!(
                fake.calls.lock()[0].platform_properties[property::MEMORY_KB],
                (32_u64 << 20).to_string()
            );
            workers.reverse();
        }
        assert!(
            live_catalog(
                &[live_test_worker("unknown", 64, 30, 4)],
                &fleets,
                base,
                &environment,
                request,
            )
            .is_none()
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
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
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
            &"host:test".parse().expect("valid environment"),
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
    async fn learned_peak_with_headroom_clamps_to_eight_gib_and_reserves_whole_cpu() {
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
            Vec::<(&'static str, String)>::new(),
        )])
        .expect("catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets.clone(),
            admitted.clone(),
            MinReservation::from_units(8192),
            gang_config::DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
        // ceil(7 GiB in KiB * 120 / 100), as supplied by historical learning.
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "8808039"))
            .await
            .expect("accepted");
        let calls = fake.calls.lock();
        let properties = &calls[0].platform_properties;
        assert_eq!(properties[property::MEMORY_KB], "8388608");
        assert_eq!(properties[property::CPU_MILLICORES], "6000");
    }

    #[tokio::test]
    async fn stale_seventy_gib_floor_clamps_to_sixty_four_gib_and_runs() {
        let fake = Arc::new(Fake::default());
        let fleets = large_memory_catalog();
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets.clone(),
            admitted.clone(),
            MinReservation::from_units(8192),
            gang_config::DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
        fitted
            .add_action(operation(), info(property::MEMORY_KB, "73400320"))
            .await
            .expect("accepted");
        let calls = fake.calls.lock();
        let properties = &calls[0].platform_properties;
        assert_eq!(properties[property::MEMORY_KB], "67108864");
        assert_eq!(properties[property::CPU_MILLICORES], "15000");
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
                        Vec::<(&'static str, String)>::new(),
                    )
                })
                .collect(),
        )
        .expect("catalog");
        let admitted = AdmittedFleets::default();
        let fitted = FittedActions::new(
            fake.clone(),
            fleets.clone(),
            admitted.clone(),
            MinReservation::from_units(8192),
            DiskMinReservation::from_units(2048),
        );
        let _admission = admitted.insert(operation(), fleets.accepting(|_| None));
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
        assert_eq!(properties[property::CPU_MILLICORES], "4000");
        assert_eq!(properties[property::DISK_KB], "20971520");
    }

    #[tokio::test]
    async fn fitted_raises_a_tiny_learned_reservation_to_the_minimum() {
        let fake = Arc::new(Fake::default());
        let fitted = FittedActions::new(
            fake.clone(),
            large_memory_catalog(),
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
            large_memory_catalog(),
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
            large_memory_catalog(),
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
            large_memory_catalog(),
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
            large_memory_catalog(),
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
            fleets,
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
