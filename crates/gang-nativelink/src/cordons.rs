//! Controller-owned physical cordons and independent connection self-drains.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Tick;
use gang_spi::{FleetSupply, InstanceCordon, PortError};
use gang_types::{FleetCatalog, FleetName, InstanceId, WorkerId, property};
use nativelink_error::{Code, Error, make_err};
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::{WorkerScheduler, WorkerSummary};
use nativelink_util::action_messages::WorkerId as NativeWorkerId;
use nativelink_util::platform_properties::PlatformPropertyValue;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::acquisition::Acquisition;

struct Entry {
    id: Arc<InstanceId>,
    present: bool,
    absent_since: Option<Tick>,
    replacing: bool,
}

struct State {
    fleets: Vec<Vec<Entry>>,
    self_drains: BTreeSet<String>,
}

impl State {
    fn owns(&self, fleet: usize, instance: &str) -> bool {
        self.fleets.get(fleet).is_some_and(|entries| {
            entries
                .binary_search_by(|entry| entry.id.as_str().cmp(instance))
                .is_ok()
        })
    }
}

/// Keeps physical intent stable until the caller has finished `inner.add_worker`.
#[must_use = "hold this guard through the inner worker admission"]
pub(crate) struct Admission<'a> {
    _state: RwLockReadGuard<'a, State>,
}

/// The registry belongs to one live worker engine, not a reconciler lease.
pub(crate) struct InstanceCordons {
    catalog: Arc<FleetCatalog>,
    state: RwLock<State>,
}

impl InstanceCordons {
    /// Starts an empty registry; native drain flags are not controller ownership.
    pub(crate) fn new(fleets: Arc<FleetCatalog>) -> Self {
        let entries = (0..fleets.len()).map(|_| Vec::new()).collect();
        Self {
            catalog: fleets,
            state: RwLock::new(State {
                fleets: entries,
                self_drains: BTreeSet::new(),
            }),
        }
    }

    fn fleet_index(&self, name: &str) -> Option<usize> {
        self.catalog
            .fleets()
            .iter()
            .position(|fleet| fleet.name() == name)
    }

    fn controller_drain(&self, state: &State, fleet: &str, worker: &str) -> bool {
        let Some(fleet) = self.fleet_index(fleet) else {
            return false;
        };
        WorkerId::instance_name_from_str(worker).is_some_and(|instance| state.owns(fleet, instance))
    }

    /// Applies controller intent before admission, holding it stable across the inner await.
    /// A pre-set flag is a self-drain, recorded before OR-ing the controller flag.
    pub(crate) async fn admission<'a>(
        &'a self,
        inner: &dyn WorkerScheduler,
        worker: &mut Worker,
    ) -> Result<Admission<'a>, Error> {
        let fleet = worker.total_platform_properties.properties.get(property::FLEET)
            .and_then(|value| match value {
                PlatformPropertyValue::Exact(fleet) => self.fleet_index(fleet),
                _ => None,
            });
        let stale = worker.total_platform_properties.properties.get(property::GANG_VERSION)
            .is_none_or(|value| value.as_str().as_ref() != env!("CARGO_PKG_VERSION"));
        let instance = stale.then(|| WorkerId::instance_name_from_str(&worker.id.0)
            .and_then(|name| InstanceId::try_new(name).ok())).flatten();
        let read = self.state.read().await;
        let state = if read.self_drains.is_empty() && !worker.is_draining
            && !(stale && fleet.is_some() && instance.is_some()) {
            read
        } else {
            drop(read);
            let mut write = self.state.write().await;
            if !write.self_drains.is_empty() {
                // Sample only AFTER taking the write lock. Self-intent changes and other
                // admissions cannot race the sample/sweep, including timeout removals.
                let connected = inner.worker_snapshot().await;
                write
                    .self_drains
                    .retain(|id| connected.iter().any(|worker| worker.id == *id));
            }
            if worker.is_draining {
                write.self_drains.insert(worker.id.0.clone());
            }
            if let (true, Some(fleet), Some(instance)) = (stale, fleet, instance) {
                let entries = &mut write.fleets[fleet];
                match entries.binary_search_by(|entry| entry.id.as_str().cmp(instance.as_str())) {
                    Ok(position) => entries[position].replacing = true,
                    Err(position) => entries.insert(position, Entry {
                        id: Arc::new(instance.clone()),
                        present: false,
                        absent_since: None,
                        replacing: true,
                    }),
                }
                let workers = inner.worker_snapshot().await;
                Self::apply_drain(inner, &write, fleet, &instance, true, &self.catalog, workers).await?;
            }
            RwLockWriteGuard::downgrade(write)
        };
        if let Some(PlatformPropertyValue::Exact(fleet)) = worker
            .total_platform_properties
            .properties
            .get(property::FLEET)
        {
            worker.is_draining |= self.controller_drain(&state, fleet, &worker.id.0);
        }
        Ok(Admission { _state: state })
    }

    /// Changes one physical controller intent and all of its current connections atomically
    /// with respect to admission. A failed update retains the intent for repair/retry.
    pub(crate) async fn set_draining(
        &self,
        inner: &dyn WorkerScheduler,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
        acquisition: &Acquisition<'_>,
    ) -> Result<(), PortError> {
        let fleet_index = self.fleet_index(fleet.as_str()).ok_or_else(|| {
            PortError::config(make_err!(Code::InvalidArgument, "unknown fleet {fleet}"))
        })?;
        let mut state = acquisition.wait(self.state.write()).await?;
        let position = state.fleets[fleet_index]
            .binary_search_by(|entry| entry.id.as_str().cmp(instance.as_str()));
        if !draining && position.is_err() {
            return Ok(());
        }
        let workers = acquisition.wait(inner.worker_snapshot()).await?;
        let position = match (position, draining) {
            (Ok(position), _) => position,
            (Err(_), false) => return Ok(()),
            (Err(position), true) => {
                state.fleets[fleet_index].insert(
                    position,
                    Entry {
                        id: Arc::new(instance.clone()),
                        present: false,
                        absent_since: None,
                        replacing: false,
                    },
                );
                position
            }
        };
        if draining && workers.iter().any(|worker| {
            worker.platform_properties.get(property::FLEET).map(String::as_str) == Some(fleet.as_str())
                && WorkerId::instance_name_from_str(&worker.id) == Some(instance.as_str())
                && worker.platform_properties.get(property::GANG_VERSION).map(String::as_str)
                    != Some(env!("CARGO_PKG_VERSION"))
        }) {
            state.fleets[fleet_index][position].replacing = true;
        }
        let draining = draining || state.fleets[fleet_index][position].replacing;
        Self::apply_drain(inner, &state, fleet_index, instance, draining, &self.catalog, workers)
            .await
            .map_err(PortError::backend)?;
        if !draining {
            state.fleets[fleet_index].remove(position);
        }
        Ok(())
    }

    async fn apply_drain(
        inner: &dyn WorkerScheduler,
        state: &State,
        fleet_index: usize,
        instance: &InstanceId,
        draining: bool,
        catalog: &FleetCatalog,
        workers: Vec<WorkerSummary>,
    ) -> Result<(), Error> {
        let fleet = catalog.fleets()[fleet_index].name();
        let mut failure = None;
        for worker in workers {
            if worker
                .platform_properties
                .get(property::FLEET)
                .map(String::as_str)
                != Some(fleet)
                || WorkerId::instance_name_from_str(&worker.id) != Some(instance.as_str())
            {
                continue;
            }
            let effective = draining || state.self_drains.contains(worker.id.as_str());
            if worker.is_draining == effective {
                continue;
            }
            let id = NativeWorkerId(worker.id);
            if let Err(error) = inner.set_drain_worker(&id, effective).await {
                // NativeLink's missing-worker error has no reliable code. Only a fresh
                // connected-worker lookup may classify this failure as a disappearance.
                if inner
                    .worker_snapshot()
                    .await
                    .iter()
                    .any(|worker| worker.id == id.0)
                {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Updates only the connection's self intent; a controller cordon always wins.
    /// Failed uncertain updates preserve their original error and any observed self-drain.
    pub(crate) async fn set_worker_draining(
        &self,
        inner: &dyn WorkerScheduler,
        worker: &NativeWorkerId,
        draining: bool,
    ) -> Result<(), Error> {
        let mut state = self.state.write().await;
        let connected = inner.worker_snapshot().await;
        let current = connected.iter().find(|summary| summary.id == worker.0);
        let controller = current.is_some_and(|summary| {
            summary
                .platform_properties
                .get(property::FLEET)
                .is_some_and(|fleet| self.controller_drain(&state, fleet, &summary.id))
        });
        let result = inner.set_drain_worker(worker, draining || controller).await;
        match &result {
            Ok(()) if draining && current.is_some() => {
                state.self_drains.insert(worker.0.clone());
            }
            Ok(()) if !draining => {
                state.self_drains.remove(worker.0.as_str());
            }
            Err(_)
                if draining
                    && inner
                        .worker_snapshot()
                        .await
                        .iter()
                        .any(|summary| summary.id == worker.0 && summary.is_draining) =>
            {
                state.self_drains.insert(worker.0.clone());
            }
            _ => {}
        }
        result
    }

    /// Forgets only this connection's self intent, never the physical controller intent.
    pub(crate) async fn disconnected(&self, worker: &NativeWorkerId) {
        self.state
            .write()
            .await
            .self_drains
            .remove(worker.0.as_str());
    }

    /// Marks a complete good provider census, then synchronously sweeps absent physical keys.
    pub(crate) async fn retain(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
        acquisition: &Acquisition<'_>,
    ) -> Result<(), PortError> {
        let mut state = acquisition.wait(self.state.write()).await?;
        for entries in &mut state.fleets {
            for entry in entries {
                entry.present = false;
            }
        }
        for (fleet, supply) in live {
            let Some(fleet) = self.fleet_index(fleet.as_str()) else {
                continue;
            };
            let entries = &mut state.fleets[fleet];
            if entries.is_empty() {
                continue;
            }
            for instance in &supply.instances {
                if let Ok(index) =
                    entries.binary_search_by(|entry| entry.id.as_str().cmp(instance.id.as_str()))
                {
                    entries[index].present = true;
                }
            }
        }
        for entries in &mut state.fleets {
            entries.retain_mut(|entry| {
                if entry.present {
                    entry.absent_since = None;
                    return true;
                }
                let absent = *entry.absent_since.get_or_insert(now);
                now < absent || now.saturating_duration_since(absent) < grace
            });
        }
        Ok(())
    }

    /// Copies sorted unique controller intents, reusing the destination vector and Arc slots.
    pub(crate) async fn snapshot_into(&self, into: &mut Vec<InstanceCordon>) {
        let state = self.state.read().await;
        let mut index = 0;
        for (fleet, entries) in state.fleets.iter().enumerate() {
            for entry in entries {
                if let Some(slot) = into.get_mut(index) {
                    slot.fleet = fleet;
                    Arc::clone_from(&mut slot.instance, &entry.id);
                    slot.replacing = entry.replacing;
                } else {
                    into.push(InstanceCordon {
                        fleet,
                        instance: Arc::clone(&entry.id),
                        replacing: entry.replacing,
                    });
                }
                index += 1;
            }
        }
        into.truncate(index);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::task::Context;

    use async_trait::async_trait;
    use gang_spi::FleetInstance;
    use gang_types::{Bytes, FleetProperties, InstanceLifecycle, Isa, Resources};
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
        RootMetricsComponent,
    };
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        CacheStats, WorkerLoad,
    };
    use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
    use nativelink_scheduler::worker::WorkerTimestamp;
    use nativelink_scheduler::worker_scheduler::WorkerSummary;
    use nativelink_util::action_messages::OperationId;
    use nativelink_util::operation_state_manager::UpdateOperationType;
    use nativelink_util::platform_properties::PlatformProperties;
    use nativelink_util::shutdown_guard::ShutdownGuard;
    use parking_lot::Mutex;
    use tokio::sync::Notify;

    use super::*;

    type TestResult = gang_testkit::TestResult;
    fn supplied(instance: &InstanceId, lifecycle: InstanceLifecycle) -> FleetSupply {
        FleetSupply {
            instances: vec![FleetInstance {
                id: instance.clone(),
                lifecycle,
                launched_at: gang_clock::Timestamp::UNIX_EPOCH,
                release_first: false,
            }],
            ..Default::default()
        }
    }

    #[derive(Clone, Copy)]
    enum Failure {
        Connected,
        Disappeared,
        Applied,
    }

    #[derive(Default)]
    struct Gate {
        entered: Notify,
        release: Notify,
    }

    #[derive(Default)]
    struct Workers {
        connected: Vec<WorkerSummary>,
        failure: Option<(String, Failure)>,
        update_gate: Option<Arc<Gate>>,
        snapshot_gate: Option<Arc<Gate>>,
    }

    struct Scheduler {
        manager: PlatformPropertyManager,
        workers: Mutex<Workers>,
    }

    impl Scheduler {
        fn new() -> Self {
            Self {
                manager: PlatformPropertyManager::new(HashMap::new()),
                workers: Mutex::new(Workers::default()),
            }
        }

        fn draining(&self, id: &NativeWorkerId) -> Option<bool> {
            self.workers
                .lock()
                .connected
                .iter()
                .find(|worker| worker.id == id.0)
                .map(|worker| worker.is_draining)
        }

        fn fail(&self, id: &NativeWorkerId, failure: Failure) {
            self.workers.lock().failure = Some((id.0.clone(), failure));
        }
    }

    impl MetricsComponent for Scheduler {
        fn publish(
            &self,
            _: MetricKind,
            _: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    impl RootMetricsComponent for Scheduler {}

    #[async_trait]
    impl WorkerScheduler for Scheduler {
        fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
            &self.manager
        }

        fn resource_capacity_updates(
            &self,
        ) -> Option<
            tokio::sync::watch::Receiver<
                Arc<nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot>,
            >,
        > {
            None
        }

        async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
            let properties = worker
                .total_platform_properties
                .properties
                .into_iter()
                .map(|(key, value)| (key, value.as_str().into_owned()))
                .collect();
            self.workers.lock().connected.push(WorkerSummary {
                id: worker.id.0,
                running_actions: 0,
                max_inflight_tasks: worker.max_inflight_tasks,
                is_paused: worker.is_paused,
                is_draining: worker.is_draining,
                last_update_timestamp: worker.last_update_timestamp,
                platform_properties: properties,
                available_platform_properties: HashMap::new(),
                free_memory_kb: None,
                cache: None,
            });
            Ok(())
        }

        async fn remove_worker(&self, id: &NativeWorkerId) -> Result<(), Error> {
            self.workers
                .lock()
                .connected
                .retain(|worker| worker.id != id.0);
            Ok(())
        }

        async fn worker_disconnected(&self, id: &NativeWorkerId) -> Result<(), Error> {
            self.remove_worker(id).await
        }

        async fn worker_snapshot(&self) -> Vec<WorkerSummary> {
            let gate = self.workers.lock().snapshot_gate.take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            self.workers.lock().connected.clone()
        }

        async fn set_drain_worker(&self, id: &NativeWorkerId, draining: bool) -> Result<(), Error> {
            let gate = self.workers.lock().update_gate.take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            let mut workers = self.workers.lock();
            let failure = if workers
                .failure
                .as_ref()
                .is_some_and(|(key, _)| key == &id.0)
            {
                workers.failure.take().map(|(_, failure)| failure)
            } else {
                None
            };
            if matches!(failure, Some(Failure::Disappeared)) {
                workers.connected.retain(|worker| worker.id != id.0);
            }
            let worker = workers
                .connected
                .iter_mut()
                .find(|worker| worker.id == id.0)
                .ok_or_else(|| make_err!(Code::Internal, "worker disappeared"))?;
            if !matches!(failure, Some(Failure::Connected)) {
                worker.is_draining = draining;
            }
            if failure.is_some() {
                return Err(make_err!(Code::Internal, "connected update rejected"));
            }
            Ok(())
        }

        async fn worker_dispatch_accepted(
            &self,
            _: &NativeWorkerId,
            _: &OperationId,
        ) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "dispatch not supported by cordon fixture"
            ))
        }

        async fn worker_dispatch_declined(
            &self,
            _: &NativeWorkerId,
            _: &OperationId,
            _: String,
            _: Option<u64>,
        ) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "dispatch not supported by cordon fixture"
            ))
        }

        async fn update_action(
            &self,
            _: &NativeWorkerId,
            _: &OperationId,
            _: UpdateOperationType,
        ) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "actions not supported by cordon fixture"
            ))
        }

        async fn worker_keep_alive_received(
            &self,
            _: &NativeWorkerId,
            _: WorkerTimestamp,
            _: Option<WorkerLoad>,
            _: Option<CacheStats>,
        ) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "keepalive not supported by cordon fixture"
            ))
        }

        async fn worker_liveness_refreshed(
            &self,
            _: &NativeWorkerId,
            _: WorkerTimestamp,
        ) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "liveness not supported by cordon fixture"
            ))
        }

        async fn shutdown(&self, _: ShutdownGuard) {
            self.workers.lock().connected.clear();
        }

        async fn remove_timedout_workers(&self, _: WorkerTimestamp) -> Result<(), Error> {
            self.workers.lock().connected.clear();
            Ok(())
        }

        async fn kill_revoked_operations(&self) -> Result<(), Error> {
            Err(make_err!(
                Code::Unimplemented,
                "actions not supported by cordon fixture"
            ))
        }
    }

    fn catalog() -> gang_testkit::TestResult<Arc<FleetCatalog>> {
        let mut fleets = Vec::new();
        for name in ["alpha", "beta"] {
            fleets.push(FleetProperties::new(
                &FleetName::try_new(name)?,
                Isa::Aarch64,
                &"bwrap",
                &"host:test".parse().expect("valid environment"),
                Resources {
                    vcpus: 4,
                    memory: Bytes::new(1 << 30),
                    disk: Bytes::new(1 << 30),
                },
                &gang_types::FleetActionPolicy::default(),
                std::iter::empty(),
            ));
        }
        Ok(Arc::new(FleetCatalog::try_new(fleets)?))
    }

    fn id(instance: &str, generation: u8) -> NativeWorkerId {
        NativeWorkerId(format!(
            "{instance}-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a{generation:02x}"
        ))
    }

    fn worker(fleet: PlatformPropertyValue, id: NativeWorkerId) -> Worker {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        Worker::new(
            id,
            PlatformProperties::new(HashMap::from([
                (property::FLEET.to_owned(), fleet),
                (property::GANG_VERSION.to_owned(), PlatformPropertyValue::Exact(env!("CARGO_PKG_VERSION").to_owned())),
            ])),
            sender,
            0,
            16,
        )
    }

    async fn admit(
        cordons: &InstanceCordons,
        inner: &Scheduler,
        fleet: &str,
        id: NativeWorkerId,
    ) -> Result<(), Error> {
        let mut worker = worker(PlatformPropertyValue::Exact(fleet.to_owned()), id);
        let _admission = cordons.admission(inner, &mut worker).await?;
        inner.add_worker(worker).await
    }


    #[tokio::test]
    async fn stale_and_missing_versions_replace_the_whole_instance() -> TestResult {
        for version in [Some("old"), None] {
            let cordons = InstanceCordons::new(catalog()?);
            let inner = Scheduler::new();
            let first = id("host", 1);
            admit(&cordons, &inner, "alpha", first.clone()).await?;
            assert_eq!(inner.draining(&first), Some(false));
            let other = id("host", 4);
            admit(&cordons, &inner, "alpha", other.clone()).await?;
            inner.workers.lock().connected[0].running_actions = 2;
            let mut stale = worker(PlatformPropertyValue::Exact("alpha".to_owned()), id("host", 2));
            if let Some(version) = version {
                stale.total_platform_properties.properties.insert(
                    property::GANG_VERSION.to_owned(), PlatformPropertyValue::Ignore(version.to_owned()));
            } else {
                stale.total_platform_properties.properties.remove(property::GANG_VERSION);
            }
            let admission = cordons.admission(&inner, &mut stale).await?;
            assert!(stale.is_draining);
            inner.add_worker(stale).await?;
            drop(admission);
            assert_eq!(inner.draining(&first), Some(true));
            assert_eq!(inner.draining(&other), Some(true));
            assert_eq!(inner.workers.lock().connected[0].running_actions, 2);
            let fleet = FleetName::try_new("alpha")?;
            let instance = InstanceId::try_new("host")?;
            assert!(snapshot(&cordons).await[0].replacing);
            cordons.set_draining(&inner, &fleet, &instance, false, crate::acquisition::test_acquisition()).await?;
            inner.remove_worker(&first).await?;
            let reconnect = id("host", 3);
            admit(&cordons, &inner, "alpha", reconnect.clone()).await?;
            assert_eq!(inner.draining(&reconnect), Some(true));
            assert!(snapshot(&cordons).await[0].replacing);
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_intent_survives_failed_drain_and_can_be_repaired() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let first = id("host", 1);
        admit(&cordons, &inner, "alpha", first.clone()).await?;
        inner.fail(&first, Failure::Connected);
        let mut stale = worker(PlatformPropertyValue::Exact("alpha".to_owned()), id("host", 2));
        stale.total_platform_properties.properties.remove(property::GANG_VERSION);
        assert!(cordons.admission(&inner, &mut stale).await.is_err());
        assert!(snapshot(&cordons).await[0].replacing);
        cordons.set_draining(&inner, &FleetName::try_new("alpha")?, &InstanceId::try_new("host")?, false, crate::acquisition::test_acquisition()).await?;
        assert_eq!(inner.draining(&first), Some(true));
        assert!(snapshot(&cordons).await[0].replacing);
        Ok(())
    }

    #[tokio::test]
    async fn existing_stale_generation_upgrades_ordinary_drain_to_replacement() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let first = id("host", 1);
        let second = id("host", 2);
        admit(&cordons, &inner, "alpha", first.clone()).await?;
        let mut old = worker(PlatformPropertyValue::Exact("alpha".to_owned()), second.clone());
        old.total_platform_properties.properties.remove(property::GANG_VERSION);
        inner.add_worker(old).await?;
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        cordons.set_draining(&inner, &fleet, &instance, true, crate::acquisition::test_acquisition()).await?;
        assert!(snapshot(&cordons).await[0].replacing);
        assert_eq!(inner.draining(&first), Some(true));
        assert_eq!(inner.draining(&second), Some(true));
        cordons.set_draining(&inner, &fleet, &instance, false, crate::acquisition::test_acquisition()).await?;
        assert_eq!(inner.draining(&first), Some(true));
        assert_eq!(inner.draining(&second), Some(true));
        Ok(())
    }
    async fn snapshot(cordons: &InstanceCordons) -> Vec<InstanceCordon> {
        let mut result = Vec::new();
        cordons.snapshot_into(&mut result).await;
        result
    }
    #[tokio::test]
    async fn acquisition_timeouts_preserve_intent_and_retry() -> TestResult {
        let clock = gang_clock::TestClock::default();
        let bound = gang_types::NonZeroDuration::from_secs(1);
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let held = cordons.state.write().await;
        let acquisition = Acquisition::new(&clock, bound);
        let command = cordons.set_draining(&inner, &fleet, &instance, true, &acquisition);
        tokio::pin!(command);
        assert!(futures::poll!(command.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(matches!(command.await, Err(PortError::Unavailable(_))));
        drop(held);
        assert!(snapshot(&cordons).await.is_empty());

        let gate = Arc::new(Gate::default());
        inner.workers.lock().snapshot_gate = Some(Arc::clone(&gate));
        let acquisition = Acquisition::new(&clock, bound);
        let command = cordons.set_draining(&inner, &fleet, &instance, true, &acquisition);
        tokio::pin!(command);
        assert!(futures::poll!(command.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(matches!(command.await, Err(PortError::Unavailable(_))));
        assert!(snapshot(&cordons).await.is_empty());
        let acquisition = Acquisition::new(&clock, bound);
        cordons
            .set_draining(&inner, &fleet, &instance, true, &acquisition)
            .await?;
        assert_eq!(snapshot(&cordons).await.len(), 1);

        let held = cordons.state.read().await;
        let acquisition = Acquisition::new(&clock, bound);
        let retain = cordons.retain(Tick::ORIGIN, Duration::ZERO, &[], &acquisition);
        tokio::pin!(retain);
        assert!(futures::poll!(retain.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(matches!(retain.await, Err(PortError::Unavailable(_))));
        drop(held);
        assert_eq!(snapshot(&cordons).await.len(), 1);
        let acquisition = Acquisition::new(&clock, bound);
        cordons
            .retain(Tick::ORIGIN, Duration::ZERO, &[], &acquisition)
            .await?;
        assert!(snapshot(&cordons).await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn started_native_update_is_not_cancelled_at_deadline() -> TestResult {
        let clock = gang_clock::TestClock::default();
        let bound = gang_types::NonZeroDuration::from_secs(1);
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let worker = id("host", 1);
        admit(&cordons, &inner, "alpha", worker.clone()).await?;
        let gate = Arc::new(Gate::default());
        inner.workers.lock().update_gate = Some(Arc::clone(&gate));
        let acquisition = Acquisition::new(&clock, bound);
        let command = cordons.set_draining(&inner, &fleet, &instance, true, &acquisition);
        tokio::pin!(command);
        assert!(futures::poll!(command.as_mut()).is_pending());
        clock.advance(bound.get());
        assert!(futures::poll!(command.as_mut()).is_pending());
        gate.release.notify_one();
        command.await?;
        assert_eq!(inner.draining(&worker), Some(true));
        assert_eq!(snapshot(&cordons).await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn physical_cordon_covers_generations_and_survives_last_disconnect() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let old = id("host", 1);
        let new = id("host", 2);
        let other = id("other", 1);
        let beta = id("host", 3);
        for (fleet, worker) in [
            ("alpha", old.clone()),
            ("alpha", new.clone()),
            ("alpha", other.clone()),
            ("beta", beta.clone()),
        ] {
            admit(&cordons, &inner, fleet, worker).await?;
        }
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(inner.draining(&old), Some(true));
        assert_eq!(inner.draining(&new), Some(true));
        assert_eq!(inner.draining(&other), Some(false));
        assert_eq!(inner.draining(&beta), Some(false));
        for worker in [&old, &new] {
            inner.worker_disconnected(worker).await?;
            cordons.disconnected(worker).await;
        }
        let reconnect = id("host", 4);
        admit(&cordons, &inner, "alpha", reconnect.clone()).await?;
        assert_eq!(inner.draining(&reconnect), Some(true));
        assert_eq!(snapshot(&cordons).await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_physical_intent_gates_later_admission() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        cordons
            .set_draining(
                &inner,
                &FleetName::try_new("alpha")?,
                &InstanceId::try_new("host")?,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let later = id("host", 1);
        admit(&cordons, &inner, "alpha", later.clone()).await?;
        assert_eq!(inner.draining(&later), Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn admission_waits_for_cancel_and_cancel_waits_for_admission() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let old = id("host", 1);
        admit(&cordons, &inner, "alpha", old.clone()).await?;
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let gate = Arc::new(Gate::default());
        inner.workers.lock().update_gate = Some(Arc::clone(&gate));
        let cancel = cordons.set_draining(
            &inner,
            &fleet,
            &instance,
            false,
            crate::acquisition::test_acquisition(),
        );
        tokio::pin!(cancel);
        assert!(futures::poll!(cancel.as_mut()).is_pending());
        let entered = gate.entered.notified();
        tokio::pin!(entered);
        assert!(futures::poll!(entered.as_mut()).is_ready());
        let incoming = id("host", 2);
        let registration = admit(&cordons, &inner, "alpha", incoming.clone());
        tokio::pin!(registration);
        assert!(futures::poll!(registration.as_mut()).is_pending());
        gate.release.notify_one();
        cancel.await?;
        registration.await?;
        assert_eq!(inner.draining(&old), Some(false));
        assert_eq!(inner.draining(&incoming), Some(false));

        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let incoming = id("host", 3);
        let mut worker = worker(
            PlatformPropertyValue::Exact("alpha".to_owned()),
            incoming.clone(),
        );
        let admission = cordons.admission(&inner, &mut worker).await?;
        assert!(worker.is_draining);
        let cancel = cordons.set_draining(
            &inner,
            &fleet,
            &instance,
            false,
            crate::acquisition::test_acquisition(),
        );
        tokio::pin!(cancel);
        assert!(futures::poll!(cancel.as_mut()).is_pending());
        inner.add_worker(worker).await?;
        drop(admission);
        cancel.await?;
        assert_eq!(inner.draining(&incoming), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn self_drain_is_connection_scoped_and_coownership_preserves_old_generation() -> TestResult
    {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let old = id("host", 1);
        let new = id("host", 2);
        admit(&cordons, &inner, "alpha", old.clone()).await?;
        cordons.set_worker_draining(&inner, &old, true).await?;
        assert!(snapshot(&cordons).await.is_empty());
        admit(&cordons, &inner, "alpha", new.clone()).await?;
        assert_eq!(inner.draining(&new), Some(false));
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons.set_worker_draining(&inner, &new, false).await?;
        assert_eq!(inner.draining(&new), Some(true));
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(inner.draining(&old), Some(true));
        assert_eq!(inner.draining(&new), Some(false));
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(inner.draining(&old), Some(true));
        cordons.set_worker_draining(&inner, &old, false).await?;
        assert_eq!(inner.draining(&old), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn connected_failures_retain_canonical_intent_and_retry_converges() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let first = id("host", 1);
        let second = id("host", 2);
        admit(&cordons, &inner, "alpha", first.clone()).await?;
        admit(&cordons, &inner, "alpha", second.clone()).await?;
        inner.fail(&second, Failure::Connected);
        assert!(matches!(
            cordons
                .set_draining(
                    &inner,
                    &fleet,
                    &instance,
                    true,
                    crate::acquisition::test_acquisition()
                )
                .await,
            Err(PortError::Backend(_))
        ));
        assert_eq!(inner.draining(&first), Some(true));
        assert_eq!(inner.draining(&second), Some(false));
        assert_eq!(snapshot(&cordons).await.len(), 1);
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        inner.fail(&second, Failure::Connected);
        assert!(
            cordons
                .set_draining(
                    &inner,
                    &fleet,
                    &instance,
                    false,
                    crate::acquisition::test_acquisition()
                )
                .await
                .is_err()
        );
        assert_eq!(snapshot(&cordons).await.len(), 1);
        let late = id("host", 3);
        admit(&cordons, &inner, "alpha", late.clone()).await?;
        assert_eq!(inner.draining(&late), Some(true));
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert!(snapshot(&cordons).await.is_empty());
        for worker in [&first, &second, &late] {
            assert_eq!(inner.draining(worker), Some(false));
        }
        Ok(())
    }

    #[tokio::test]
    async fn freshly_confirmed_disappearance_is_benign_for_set_and_cancel() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        for draining in [true, false] {
            let worker = id("host", u8::from(draining));
            admit(&cordons, &inner, "alpha", worker.clone()).await?;
            inner.fail(&worker, Failure::Disappeared);
            cordons
                .set_draining(
                    &inner,
                    &fleet,
                    &instance,
                    draining,
                    crate::acquisition::test_acquisition(),
                )
                .await?;
            assert_eq!(inner.draining(&worker), None);
            assert_eq!(snapshot(&cordons).await.len(), usize::from(draining));
        }
        Ok(())
    }

    #[tokio::test]
    async fn registry_survives_consumer_replacement_but_never_adopts_native_flags() -> TestResult {
        let fleets = catalog()?;
        let engine_registry = Arc::new(InstanceCordons::new(Arc::clone(&fleets)));
        let inner = Scheduler::new();
        let worker = id("host", 1);
        admit(&engine_registry, &inner, "alpha", worker.clone()).await?;
        {
            let consumer = Arc::clone(&engine_registry);
            consumer
                .set_draining(
                    &inner,
                    &FleetName::try_new("alpha")?,
                    &InstanceId::try_new("host")?,
                    true,
                    crate::acquisition::test_acquisition(),
                )
                .await?;
        }
        let replacement = Arc::clone(&engine_registry);
        assert_eq!(snapshot(&replacement).await.len(), 1);
        let fresh = InstanceCordons::new(fleets);
        assert!(snapshot(&fresh).await.is_empty());
        assert_eq!(inner.draining(&worker), Some(true));
        let reconnect = id("host", 2);
        admit(&fresh, &inner, "alpha", reconnect.clone()).await?;
        assert_eq!(inner.draining(&reconnect), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn census_grace_reappearance_and_fleet_identity_are_independent() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let alpha = FleetName::try_new("alpha")?;
        let beta = FleetName::try_new("beta")?;
        let instance = InstanceId::try_new("host")?;
        let grace = Duration::from_secs(10);
        for fleet in [&alpha, &beta] {
            cordons
                .set_draining(
                    &inner,
                    fleet,
                    &instance,
                    true,
                    crate::acquisition::test_acquisition(),
                )
                .await?;
        }
        let beta_supply = supplied(&instance, InstanceLifecycle::Terminating);
        let live = [(&beta, &beta_supply)];
        cordons
            .retain(
                Tick::ORIGIN,
                grace,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::ORIGIN + Duration::from_secs(9),
                grace,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(snapshot(&cordons).await.len(), 2);
        let alpha_supply = supplied(&instance, InstanceLifecycle::Pending);
        let old_generation = supplied(&instance, InstanceLifecycle::InService);
        let all_generations = [
            (&alpha, &alpha_supply),
            (&beta, &beta_supply),
            (&alpha, &old_generation),
        ];
        cordons
            .retain(
                Tick::ORIGIN + grace,
                grace,
                &all_generations,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::ORIGIN + Duration::from_secs(11),
                grace,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        // Reissuing an existing physical intent must not restart its absence timer.
        cordons
            .set_draining(
                &inner,
                &alpha,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::ORIGIN + Duration::from_secs(20),
                grace,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(snapshot(&cordons).await.len(), 2);
        cordons
            .retain(
                Tick::ORIGIN + Duration::from_secs(21),
                grace,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let remaining = snapshot(&cordons).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].fleet, 1);
        Ok(())
    }
    #[tokio::test]
    async fn census_retains_all_lifecycles_without_warmed_allocations() -> TestResult {
        fn assert_send<T: Send>(_: &T) {}

        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let pending = InstanceId::try_new("pending")?;
        let serving = InstanceId::try_new("serving")?;
        let terminating = InstanceId::try_new("terminating")?;
        for instance in [&pending, &serving, &terminating] {
            cordons
                .set_draining(
                    &inner,
                    &fleet,
                    instance,
                    true,
                    crate::acquisition::test_acquisition(),
                )
                .await?;
        }
        let mut supply = supplied(&pending, InstanceLifecycle::Pending);
        supply
            .instances
            .extend(supplied(&serving, InstanceLifecycle::InService).instances);
        supply
            .instances
            .extend(supplied(&terminating, InstanceLifecycle::Terminating).instances);
        let live = [(&fleet, &supply)];
        cordons
            .retain(
                Tick::ORIGIN,
                Duration::ZERO,
                &live,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let retention = cordons.retain(
            Tick::from_elapsed(Duration::MAX),
            Duration::ZERO,
            &live,
            crate::acquisition::test_acquisition(),
        );
        assert_send(&retention);
        tokio::pin!(retention);
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let measured = allocation_counter::measure(|| {
            assert!(retention.as_mut().poll(&mut context).is_ready());
        });
        assert_eq!(measured.count_total, 0);
        assert_eq!(snapshot(&cordons).await.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn retention_handles_maximum_ticks_zero_grace_and_backward_clock() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let maximum = Tick::from_elapsed(Duration::MAX);
        cordons
            .retain(
                maximum,
                Duration::MAX,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::ORIGIN,
                Duration::ZERO,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(snapshot(&cordons).await.len(), 1);
        cordons
            .retain(
                maximum,
                Duration::ZERO,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert!(snapshot(&cordons).await.is_empty());
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::ORIGIN,
                Duration::MAX,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .retain(
                Tick::from_elapsed(Duration::MAX.saturating_sub(Duration::from_nanos(1))),
                Duration::MAX,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(snapshot(&cordons).await.len(), 1);
        cordons
            .retain(
                maximum,
                Duration::MAX,
                &[],
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert!(snapshot(&cordons).await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn self_markers_prune_timeout_disconnect_and_missing_rpc_ids() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        for generation in 0..8 {
            let worker = id("host", generation);
            admit(&cordons, &inner, "alpha", worker.clone()).await?;
            cordons.set_worker_draining(&inner, &worker, true).await?;
            assert_eq!(cordons.state.read().await.self_drains.len(), 1);
            inner.remove_timedout_workers(0).await?;
        }
        let missing = id("missing", 1);
        assert!(
            cordons
                .set_worker_draining(&inner, &missing, true)
                .await
                .is_err()
        );
        assert!(
            !cordons
                .state
                .read()
                .await
                .self_drains
                .contains(missing.0.as_str())
        );
        let current = id("host", 9);
        admit(&cordons, &inner, "alpha", current.clone()).await?;
        assert!(cordons.state.read().await.self_drains.is_empty());
        assert_eq!(inner.draining(&current), Some(false));
        cordons.set_worker_draining(&inner, &current, true).await?;
        inner.remove_worker(&current).await?;
        cordons.disconnected(&current).await;
        assert!(cordons.state.read().await.self_drains.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn admission_cleanup_samples_under_lock_before_self_intent_can_change() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let old = id("host", 1);
        admit(&cordons, &inner, "alpha", old.clone()).await?;
        cordons.set_worker_draining(&inner, &old, true).await?;
        let gate = Arc::new(Gate::default());
        inner.workers.lock().snapshot_gate = Some(Arc::clone(&gate));
        let registration = admit(&cordons, &inner, "alpha", id("host", 2));
        tokio::pin!(registration);
        assert!(futures::poll!(registration.as_mut()).is_pending());
        let entered = gate.entered.notified();
        tokio::pin!(entered);
        assert!(futures::poll!(entered.as_mut()).is_ready());
        let cancel_self = cordons.set_worker_draining(&inner, &old, false);
        tokio::pin!(cancel_self);
        assert!(futures::poll!(cancel_self.as_mut()).is_pending());
        gate.release.notify_one();
        registration.await?;
        cancel_self.await?;
        assert!(cordons.state.read().await.self_drains.is_empty());
        assert_eq!(inner.draining(&old), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn preset_and_uncertain_self_drains_survive_physical_cancel() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        let fleet = FleetName::try_new("alpha")?;
        let instance = InstanceId::try_new("host")?;
        let preset = id("host", 1);
        let uncertain = id("host", 2);
        let mut worker = worker(
            PlatformPropertyValue::Exact("alpha".to_owned()),
            preset.clone(),
        );
        worker.is_draining = true;
        let admission = cordons.admission(&inner, &mut worker).await?;
        inner.add_worker(worker).await?;
        drop(admission);
        admit(&cordons, &inner, "alpha", uncertain.clone()).await?;
        inner.fail(&uncertain, Failure::Applied);
        let error = cordons.set_worker_draining(&inner, &uncertain, true).await;
        assert!(matches!(error, Err(error) if error.code == Code::Internal));
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons
            .set_draining(
                &inner,
                &fleet,
                &instance,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        assert_eq!(inner.draining(&preset), Some(true));
        assert_eq!(inner.draining(&uncertain), Some(true));
        assert!(snapshot(&cordons).await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn unmanaged_or_invalid_raw_identity_does_not_inherit_controller_intent() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        cordons
            .set_draining(
                &inner,
                &FleetName::try_new("alpha")?,
                &InstanceId::try_new("host")?,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        for (fleet, id) in [
            (
                PlatformPropertyValue::Exact("unmanaged".to_owned()),
                id("host", 1),
            ),
            (PlatformPropertyValue::Minimum(0), id("host", 2)),
            (
                PlatformPropertyValue::Exact("alpha".to_owned()),
                NativeWorkerId("host-not-a-uuid".to_owned()),
            ),
            (
                PlatformPropertyValue::Exact("alpha".to_owned()),
                NativeWorkerId(format!("host-{}", "x".repeat(36))),
            ),
        ] {
            let mut worker = worker(fleet, id);
            let _admission = cordons.admission(&inner, &mut worker).await?;
            assert!(!worker.is_draining);
        }
        Ok(())
    }

    #[tokio::test]
    async fn warmed_snapshot_is_sorted_unique_and_reuses_arc_identity_without_allocations()
    -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        for (fleet, instance) in [
            ("beta", "z"),
            ("alpha", "z"),
            ("alpha", "a"),
            ("alpha", "z"),
        ] {
            cordons
                .set_draining(
                    &inner,
                    &FleetName::try_new(fleet)?,
                    &InstanceId::try_new(instance)?,
                    true,
                    crate::acquisition::test_acquisition(),
                )
                .await?;
        }
        let mut output = snapshot(&cordons).await;
        assert_eq!(
            output
                .iter()
                .map(|entry| (entry.fleet, entry.instance.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "a"), (0, "z"), (1, "z")]
        );
        let identity = output
            .iter()
            .map(|entry| Arc::clone(&entry.instance))
            .collect::<Vec<_>>();
        let capacity = output.capacity();
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let allocations = allocation_counter::measure(|| {
            for _ in 0..100 {
                let fill = cordons.snapshot_into(&mut output);
                tokio::pin!(fill);
                assert!(fill.as_mut().poll(&mut context).is_ready());
            }
        });
        assert_eq!(allocations.count_total, 0);
        assert_eq!(output.capacity(), capacity);
        assert!(
            output
                .iter()
                .zip(&identity)
                .all(|(entry, id)| Arc::ptr_eq(&entry.instance, id))
        );
        cordons
            .set_draining(
                &inner,
                &FleetName::try_new("alpha")?,
                &InstanceId::try_new("a")?,
                false,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        cordons.snapshot_into(&mut output).await;
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].instance.as_str(), "z");
        assert_eq!(output.capacity(), capacity);
        Ok(())
    }

    #[tokio::test]
    async fn warmed_admission_uses_borrowed_identity_without_allocations() -> TestResult {
        let cordons = InstanceCordons::new(catalog()?);
        let inner = Scheduler::new();
        cordons
            .set_draining(
                &inner,
                &FleetName::try_new("alpha")?,
                &InstanceId::try_new("host")?,
                true,
                crate::acquisition::test_acquisition(),
            )
            .await?;
        let mut incoming = worker(
            PlatformPropertyValue::Exact("alpha".to_owned()),
            id("host", 1),
        );
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let allocations = allocation_counter::measure(|| {
            for _ in 0..100 {
                incoming.is_draining = false;
                let admission = cordons.admission(&inner, &mut incoming);
                tokio::pin!(admission);
                assert!(admission.as_mut().poll(&mut context).is_ready());
            }
        });
        assert!(incoming.is_draining);
        assert_eq!(allocations.count_total, 0);
        Ok(())
    }
}
