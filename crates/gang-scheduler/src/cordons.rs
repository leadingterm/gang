//! Controller-owned physical instance cordons, census retention, and stale-build replacement.
//!
//! Physical drain and cordon intents are keyed by `(fleet, instance)` so they apply across all
//! overlapping worker renewal generations and persist across worker disconnects until explicitly
//! cleared or swept by [`CordonRegistry::retain_drains`] after `grace` absence from a complete
//! provider [`Census`]. Stale-build connections (`!worker.is_current_build()`) permanently upgrade
//! the physical instance to `replacing = true`, cordoning all generations of that instance so
//! clearing an ordinary drain cannot un-cordon an outdated host.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Tick;
use gang_spi::{FleetSupply, InstanceCordon};
use gang_types::{FleetCatalog, FleetCountError, FleetName, InstanceId, MAX_FLEETS};
use thiserror::Error;

use crate::workers::{WorkerKey, WorkerRegistry};

/// Compact provider census: `(fleet, instance)` pairs, sorted and unique.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Census(Vec<(FleetName, Arc<InstanceId>)>);

impl Census {
    /// Builds a sorted, deduplicated [`Census`] from arbitrary `(fleet, instance)` pairs.
    #[must_use]
    pub fn new(mut entries: Vec<(FleetName, Arc<InstanceId>)>) -> Self {
        Self::normalize(&mut entries);
        Self(entries)
    }

    /// Builds a [`Census`] from borrowed provider supply snapshots across all fleets.
    ///
    /// Every instance lifecycle (`Pending`, `InService`, `Terminating`) present in each
    /// [`FleetSupply::instances`] is retained.
    #[must_use]
    pub fn from_supply(live: &[(&FleetName, &FleetSupply)]) -> Self {
        let mut census = Self::default();
        census.rebuild_from_supply(live);
        census
    }

    /// Replaces the census entries in place from `live`, reusing the backing allocation.
    pub fn rebuild_from_supply(&mut self, live: &[(&FleetName, &FleetSupply)]) {
        self.0.clear();
        for (fleet, supply) in live {
            for instance in &supply.instances {
                self.0
                    .push(((*fleet).clone(), Arc::new(instance.id.clone())));
            }
        }
        Self::normalize(&mut self.0);
    }

    /// Returns `true` when `(fleet, instance)` is present in this census.
    #[must_use]
    pub fn contains(&self, fleet: &FleetName, instance: &InstanceId) -> bool {
        self.contains_name(fleet.as_str(), instance)
    }

    /// Returns `true` when `(fleet, instance)` is present in this census by fleet name slice.
    #[must_use]
    pub fn contains_name(&self, fleet: &str, instance: &InstanceId) -> bool {
        self.0
            .binary_search_by(|(entry_fleet, entry_instance)| {
                entry_fleet
                    .as_str()
                    .cmp(fleet)
                    .then_with(|| entry_instance.as_ref().cmp(instance))
            })
            .is_ok()
    }

    /// Returns the sorted, deduplicated slice of `(fleet, instance)` pairs.
    #[must_use]
    pub fn as_slice(&self) -> &[(FleetName, Arc<InstanceId>)] {
        &self.0
    }

    /// Iterates over all `(fleet, instance)` pairs in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&FleetName, &Arc<InstanceId>)> {
        self.0.iter().map(|(fleet, instance)| (fleet, instance))
    }

    /// Returns the number of unique `(fleet, instance)` pairs in the census.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when the census contains no instances.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn normalize(entries: &mut Vec<(FleetName, Arc<InstanceId>)>) {
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        entries.dedup_by(|later, earlier| later.0 == earlier.0 && later.1 == earlier.1);
    }
}

/// Errors returned by [`CordonRegistry`] operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CordonError {
    /// The named fleet does not exist in the configured [`FleetCatalog`].
    #[error("unknown fleet `{fleet}`")]
    UnknownFleet {
        /// The unrecognized fleet name.
        fleet: FleetName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CordonEntry {
    id: Arc<InstanceId>,
    present: bool,
    absent_since: Option<Tick>,
    replacing: bool,
}

/// Controller-owned registry of physical instance drains and stale-build replacement cordons.
#[derive(Debug, Clone, Default)]
pub struct CordonRegistry {
    fleets: Vec<Vec<CordonEntry>>,
    fleet_names: Vec<String>,
    fleet_lookup: HashMap<String, usize>,
}

impl CordonRegistry {
    /// Creates an empty cordon registry sized for the fleets in `catalog`.
    ///
    /// # Errors
    ///
    /// Returns [`FleetCountError`] if `catalog.len() > MAX_FLEETS`.
    pub fn from_catalog(catalog: &FleetCatalog) -> Result<Self, FleetCountError> {
        let count = catalog.len();
        if count > MAX_FLEETS {
            return Err(FleetCountError { count });
        }
        let mut fleet_names = Vec::with_capacity(count);
        let mut fleet_lookup = HashMap::with_capacity(count);
        for (index, fleet) in catalog.fleets().iter().enumerate() {
            let name = fleet.name().to_owned();
            fleet_lookup.insert(name.clone(), index);
            fleet_names.push(name);
        }
        Ok(Self {
            fleets: vec![Vec::new(); count],
            fleet_names,
            fleet_lookup,
        })
    }

    /// Returns the number of configured fleets tracked by this registry.
    #[must_use]
    pub const fn fleet_count(&self) -> usize {
        self.fleets.len()
    }

    /// Returns the total number of active physical cordon/drain entries across all fleets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fleets.iter().map(Vec::len).sum()
    }

    /// Returns `true` when no physical instances are cordoned or drained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fleets.iter().all(Vec::is_empty)
    }

    /// Returns `true` when `(fleet, instance)` currently has an active controller intent.
    #[must_use]
    pub fn contains(&self, fleet: usize, instance: &InstanceId) -> bool {
        self.find_entry(fleet, instance).is_some()
    }

    /// Returns `true` when `(fleet, instance)` is marked for permanent stale-build replacement.
    #[must_use]
    pub fn is_replacing(&self, fleet: usize, instance: &InstanceId) -> bool {
        self.find_entry(fleet, instance)
            .is_some_and(|entry| entry.replacing)
    }

    /// Returns the tick when `(fleet, instance)` was first observed absent from a census, if any.
    #[must_use]
    pub fn absent_since(&self, fleet: usize, instance: &InstanceId) -> Option<Tick> {
        self.find_entry(fleet, instance)
            .and_then(|entry| entry.absent_since)
    }

    /// Applies or clears operator/reconciler drain intent for `(fleet, instance)` and synchronizes
    /// all connected worker generations in `workers`.
    ///
    /// - When `draining == true`, the physical intent persists across worker disconnects and
    ///   reconnects. Reissuing `draining = true` preserves any existing `absent_since` timer.
    /// - If any connected worker generation of `(fleet, instance)` is running a stale build
    ///   (`!worker.is_current_build()`), the entry is upgraded to `replacing = true` and cordoned.
    /// - When `draining == false`, ordinary drain intent is removed, but a `replacing = true`
    ///   cordon cannot be cleared and keeps the instance cordoned and draining.
    ///
    /// Returns the connected [`WorkerKey`]s whose drain/cordon state applies.
    ///
    /// # Errors
    ///
    /// Returns [`CordonError::UnknownFleet`] if `fleet` is not in the catalog.
    pub fn set_draining(
        &mut self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
        workers: &mut WorkerRegistry,
    ) -> Result<Vec<WorkerKey>, CordonError> {
        let Some(&fleet_index) = self.fleet_lookup.get(fleet.as_str()) else {
            return Err(CordonError::UnknownFleet {
                fleet: fleet.clone(),
            });
        };

        let position = self.search(fleet_index, instance);
        if !draining && position.is_err() {
            // Ensure any uncordoned `InstanceState::draining` on `workers` stays cleared without
            // allocating if nothing was marked.
            if workers
                .instance_state(fleet_index, instance)
                .is_some_and(|state| state.draining)
            {
                let instance_arc = Arc::new(instance.clone());
                return Ok(workers.set_instance_draining(fleet_index, &instance_arc, false));
            }
            return Ok(Vec::new());
        }

        let (instance_arc, effective_draining, effective_replacing) = if draining {
            let has_stale = Self::has_stale_generation(workers, fleet_index, instance);
            let Some(entries) = self.fleets.get_mut(fleet_index) else {
                return Err(CordonError::UnknownFleet {
                    fleet: fleet.clone(),
                });
            };
            let idx = match position {
                Ok(idx) => idx,
                Err(insert_at) => {
                    entries.insert(
                        insert_at,
                        CordonEntry {
                            id: Arc::new(instance.clone()),
                            present: false,
                            absent_since: None,
                            replacing: false,
                        },
                    );
                    insert_at
                }
            };
            let entry = &mut entries[idx];
            if has_stale {
                entry.replacing = true;
            }
            (Arc::clone(&entry.id), true, entry.replacing)
        } else {
            let Ok(idx) = position else {
                return Ok(Vec::new());
            };
            let Some(entries) = self.fleets.get_mut(fleet_index) else {
                return Err(CordonError::UnknownFleet {
                    fleet: fleet.clone(),
                });
            };
            let entry = &entries[idx];
            let instance_arc = Arc::clone(&entry.id);
            let replacing = entry.replacing;
            if !replacing {
                entries.remove(idx);
            }
            (instance_arc, replacing, replacing)
        };

        if effective_replacing {
            workers.set_instance_cordon(fleet_index, &instance_arc, true);
        } else {
            workers.set_instance_cordon(fleet_index, &instance_arc, false);
        }
        Ok(workers.set_instance_draining(fleet_index, &instance_arc, effective_draining))
    }

    /// Synchronizes physical cordon state when `worker_key` has just registered in `workers`.
    ///
    /// - If the newly registered worker is running a stale build (`!worker.is_current_build()`),
    ///   marks its physical `(fleet, instance)` with `replacing = true` and drains/cordons **all**
    ///   connected generations of that `(fleet, instance)` (including current-build peers).
    /// - Otherwise, if `(fleet, instance)` already has an active controller cordon/drain in this
    ///   registry, ensures `workers` reflects it on all connected generations.
    ///
    /// Returns all connected [`WorkerKey`]s on the physical instance whose drain/cordon flags were
    /// applied.
    pub fn on_worker_registered(
        &mut self,
        worker_key: WorkerKey,
        workers: &mut WorkerRegistry,
    ) -> Vec<WorkerKey> {
        let Some((fleet, instance, current_build)) = workers.get(worker_key).map(|worker| {
            (
                worker.fleet,
                Arc::clone(&worker.instance),
                worker.is_current_build(),
            )
        }) else {
            return Vec::new();
        };

        if !current_build {
            return self.mark_replacing(fleet, &instance, workers);
        }

        let Some(entry) = self.find_entry(fleet, &instance) else {
            return Vec::new();
        };
        let replacing = entry.replacing;
        let instance_arc = Arc::clone(&entry.id);
        if replacing {
            workers.set_instance_cordon(fleet, &instance_arc, true);
        }
        workers.set_instance_draining(fleet, &instance_arc, true)
    }

    /// Scans all connected workers in `workers` for stale builds (`!worker.is_current_build()`)
    /// and permanently cordons their physical `(fleet, instance)` pairs across all generations.
    ///
    /// Returns the unique [`WorkerKey`]s (in ascending `WorkerId` order) of all connected
    /// generations belonging to newly or actively replaced stale-build instances.
    pub fn stale_build_drains(&mut self, workers: &mut WorkerRegistry) -> Vec<WorkerKey> {
        let stale_instances: Vec<(usize, Arc<InstanceId>)> = workers
            .workers_sorted()
            .filter(|(_, worker)| !worker.is_current_build())
            .map(|(_, worker)| (worker.fleet, Arc::clone(&worker.instance)))
            .collect();

        let mut affected = Vec::new();
        for (fleet, instance) in stale_instances {
            for key in self.mark_replacing(fleet, &instance, workers) {
                if !affected.contains(&key) {
                    affected.push(key);
                }
            }
        }
        affected
    }

    /// Retains physical intents present in `census`, allowing `grace` of continuous absence before
    /// pruning.
    ///
    /// Reappearance in `census` resets `absent_since` to `None`. Backward clock steps (`now <
    /// absent_since`) retain the entry rather than pruning prematurely.
    ///
    /// When an absent intent exceeds `grace`, it is pruned from this registry and its physical
    /// `INSTANCE` / `CORDON` bits are cleared on `workers`, leaving worker-scoped intents (such as
    /// `GOING_AWAY`, `PAUSED`, or `CONTROLLER`) on active workers untouched.
    ///
    /// Returns any connected [`WorkerKey`]s whose physical `INSTANCE` or `CORDON` flag was cleared.
    pub fn retain_drains(
        &mut self,
        now: Tick,
        grace: Duration,
        census: &Census,
        workers: &mut WorkerRegistry,
    ) -> Vec<WorkerKey> {
        let mut cleared_workers = Vec::new();
        for (fleet_index, entries) in self.fleets.iter_mut().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let Some(fleet_name) = self.fleet_names.get(fleet_index) else {
                continue;
            };
            for entry in entries.iter_mut() {
                entry.present = census.contains_name(fleet_name, &entry.id);
            }
            entries.retain_mut(|entry| {
                if entry.present {
                    entry.absent_since = None;
                    return true;
                }
                let absent = *entry.absent_since.get_or_insert(now);
                let keep = now < absent || now.saturating_duration_since(absent) < grace;
                if !keep && workers.instance_state(fleet_index, &entry.id).is_some() {
                    for key in workers.set_instance_cordon(fleet_index, &entry.id, false) {
                        if !cleared_workers.contains(&key) {
                            cleared_workers.push(key);
                        }
                    }
                    for key in workers.set_instance_draining(fleet_index, &entry.id, false) {
                        if !cleared_workers.contains(&key) {
                            cleared_workers.push(key);
                        }
                    }
                }
                keep
            });
        }
        cleared_workers
    }

    /// Copies sorted unique controller-owned intents into `into`, reusing `into`'s capacity and
    /// `Arc` slots.
    pub fn snapshot_into(&self, into: &mut Vec<InstanceCordon>) {
        let mut index = 0;
        for (fleet, entries) in self.fleets.iter().enumerate() {
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

    fn mark_replacing(
        &mut self,
        fleet: usize,
        instance: &Arc<InstanceId>,
        workers: &mut WorkerRegistry,
    ) -> Vec<WorkerKey> {
        let position = self.search(fleet, instance);
        let Some(entries) = self.fleets.get_mut(fleet) else {
            return Vec::new();
        };
        let idx = match position {
            Ok(idx) => {
                entries[idx].replacing = true;
                idx
            }
            Err(insert_at) => {
                entries.insert(
                    insert_at,
                    CordonEntry {
                        id: Arc::clone(instance),
                        present: false,
                        absent_since: None,
                        replacing: true,
                    },
                );
                insert_at
            }
        };
        let instance_arc = Arc::clone(&entries[idx].id);
        workers.set_instance_cordon(fleet, &instance_arc, true);
        workers.set_instance_draining(fleet, &instance_arc, true)
    }

    fn has_stale_generation(workers: &WorkerRegistry, fleet: usize, instance: &InstanceId) -> bool {
        workers
            .instance_state(fleet, instance)
            .is_some_and(|state| {
                state.generations.iter().any(|&key| {
                    workers
                        .get(key)
                        .is_some_and(|worker| !worker.is_current_build())
                })
            })
    }

    fn search(&self, fleet: usize, instance: &InstanceId) -> Result<usize, usize> {
        let Some(entries) = self.fleets.get(fleet) else {
            return Err(0);
        };
        entries.binary_search_by(|entry| entry.id.as_ref().cmp(instance))
    }

    fn find_entry(&self, fleet: usize, instance: &InstanceId) -> Option<&CordonEntry> {
        let idx = self.search(fleet, instance).ok()?;
        self.fleets.get(fleet)?.get(idx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gang_clock::Timestamp;
    use gang_spi::FleetInstance;
    use gang_types::{
        ActionResources, Bytes, FleetActionPolicy, FleetProperties, InstanceLifecycle, Isa,
        Resources,
    };

    use super::*;
    use crate::operations::Now;
    use crate::workers::WorkerRegistration;

    const CURRENT_VERSION: &str = "0.3.9";

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn fleet_props(name: &str) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(16),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet_props("alpha"), fleet_props("beta")]).expect("catalog")
    }

    fn register_worker(
        workers: &mut WorkerRegistry,
        fleet: &str,
        instance: &str,
        seed: u8,
        version: &str,
    ) -> WorkerKey {
        workers
            .register(
                now(1),
                CURRENT_VERSION,
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
                    fleet: FleetName::try_new(fleet).expect("fleet"),
                    gang_version: version.to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 16_000,
                        memory: Bytes::gib(16),
                        disk: Bytes::gib(100),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 8_000,
                        memory: Bytes::gib(16),
                        disk: Bytes::gib(100),
                    },
                    max_inflight: None,
                    single_use: false,
                    environment: None,
                    uuid_bytes: [seed; 16],
                },
            )
            .expect("register worker")
    }

    fn supply_with(instances: &[(&str, InstanceLifecycle)]) -> FleetSupply {
        FleetSupply {
            instances: instances
                .iter()
                .map(|(id, lifecycle)| FleetInstance {
                    id: InstanceId::try_new(*id).expect("instance"),
                    lifecycle: *lifecycle,
                    launched_at: Timestamp::UNIX_EPOCH,
                    release_first: false,
                })
                .collect(),
            ..Default::default()
        }
    }

    fn snapshot(cordons: &CordonRegistry) -> Vec<InstanceCordon> {
        let mut out = Vec::new();
        cordons.snapshot_into(&mut out);
        out
    }

    #[test]
    fn cordon_uncordon_covers_generations_and_persists_across_disconnect_reconnect() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let host = InstanceId::try_new("i-host").expect("host");

        let gen1 = register_worker(&mut workers, "alpha", "i-host", 1, CURRENT_VERSION);
        let gen2 = register_worker(&mut workers, "alpha", "i-host", 2, CURRENT_VERSION);
        let peer_other_instance =
            register_worker(&mut workers, "alpha", "i-other", 3, CURRENT_VERSION);
        let peer_other_fleet = register_worker(&mut workers, "beta", "i-host", 4, CURRENT_VERSION);

        let affected = cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("set draining");
        assert_eq!(affected, vec![gen1, gen2]);
        assert!(workers.get(gen1).expect("gen1").drain.is_draining());
        assert!(workers.get(gen2).expect("gen2").drain.is_draining());
        assert!(
            !workers
                .get(peer_other_instance)
                .expect("other")
                .drain
                .blocks_assignment()
        );
        assert!(
            !workers
                .get(peer_other_fleet)
                .expect("beta")
                .drain
                .blocks_assignment()
        );
        assert_eq!(snapshot(&cordons).len(), 1);
        assert!(!snapshot(&cordons)[0].replacing);

        // Disconnect all generations; intent persists and applies to future reconnect.
        workers.disconnect(gen1);
        workers.disconnect(gen2);
        assert_eq!(workers.worker_count(), 2);
        assert!(cordons.contains(0, &host));

        let gen3 = register_worker(&mut workers, "alpha", "i-host", 5, CURRENT_VERSION);
        assert!(
            cordons
                .on_worker_registered(gen3, &mut workers)
                .contains(&gen3)
        );
        assert!(workers.get(gen3).expect("gen3").drain.is_draining());
        assert!(!workers.get(gen3).expect("gen3").is_available_for_dispatch());

        // Clearing ordinary drain lifts drain on connected generations and drops the entry.
        let cleared = cordons
            .set_draining(&alpha, &host, false, &mut workers)
            .expect("clear draining");
        assert_eq!(cleared, vec![gen3]);
        assert!(!workers.get(gen3).expect("gen3").drain.blocks_assignment());
        assert!(workers.get(gen3).expect("gen3").is_available_for_dispatch());
        assert!(cordons.is_empty());
    }

    #[test]
    fn pre_existing_drain_intent_before_any_worker_gates_later_registration() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let host = InstanceId::try_new("i-cold").expect("host");

        let affected = cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("set draining");
        assert!(affected.is_empty());
        assert_eq!(cordons.len(), 1);

        let w = register_worker(&mut workers, "alpha", "i-cold", 1, CURRENT_VERSION);
        cordons.on_worker_registered(w, &mut workers);
        assert!(workers.get(w).expect("w").drain.is_draining());
        assert!(!workers.get(w).expect("w").is_available_for_dispatch());
    }

    #[test]
    fn unknown_fleet_is_rejected() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let missing = FleetName::try_new("gamma").expect("gamma");
        let host = InstanceId::try_new("i-1").expect("host");
        assert_eq!(
            cordons.set_draining(&missing, &host, true, &mut workers),
            Err(CordonError::UnknownFleet { fleet: missing })
        );
    }

    #[test]
    fn stale_build_replaces_whole_instance_and_survives_uncordon_and_reconnect() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let host = InstanceId::try_new("i-host").expect("host");

        let cur1 = register_worker(&mut workers, "alpha", "i-host", 1, CURRENT_VERSION);
        let cur2 = register_worker(&mut workers, "alpha", "i-host", 2, CURRENT_VERSION);
        let other = register_worker(&mut workers, "alpha", "i-other", 3, CURRENT_VERSION);

        let stale = register_worker(&mut workers, "alpha", "i-host", 4, "0.3.8");
        let affected = cordons.on_worker_registered(stale, &mut workers);
        assert_eq!(affected, vec![cur1, cur2, stale]);

        for key in [cur1, cur2, stale] {
            let w = workers.get(key).expect("worker");
            assert!(w.drain.is_cordoned());
            assert!(w.drain.is_draining());
            assert!(!w.is_available_for_dispatch());
        }
        assert!(!workers.get(other).expect("other").drain.blocks_assignment());
        assert!(cordons.is_replacing(0, &host));
        assert!(snapshot(&cordons)[0].replacing);

        // Uncordoning via set_draining(..., false) must not clear a replacement cordon.
        let still_drained = cordons
            .set_draining(&alpha, &host, false, &mut workers)
            .expect("uncordon attempt");
        assert_eq!(still_drained, vec![cur1, cur2, stale]);
        assert!(cordons.is_replacing(0, &host));
        assert!(workers.get(cur1).expect("cur1").drain.is_cordoned());
        assert!(workers.get(cur1).expect("cur1").drain.is_draining());

        // Even after disconnecting stale and reconnecting with current build, cordon stays.
        workers.disconnect(stale);
        workers.disconnect(cur1);
        let reconnect = register_worker(&mut workers, "alpha", "i-host", 5, CURRENT_VERSION);
        cordons.on_worker_registered(reconnect, &mut workers);
        assert!(
            workers
                .get(reconnect)
                .expect("reconnect")
                .drain
                .is_cordoned()
        );
        assert!(
            workers
                .get(reconnect)
                .expect("reconnect")
                .drain
                .is_draining()
        );
        assert!(cordons.is_replacing(0, &host));
    }

    #[test]
    fn stale_build_drains_sweep_and_ordinary_drain_upgrade() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let host = InstanceId::try_new("i-host").expect("host");
        let host2 = InstanceId::try_new("i-host2").expect("host2");

        let cur = register_worker(&mut workers, "alpha", "i-host", 1, CURRENT_VERSION);
        let stale = register_worker(&mut workers, "alpha", "i-host", 2, "0.3.7");

        // Ordinary drain upgrades to `replacing` when a stale generation is already connected.
        cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("set draining");
        assert!(cordons.is_replacing(0, &host));
        cordons
            .set_draining(&alpha, &host, false, &mut workers)
            .expect("cannot clear replacement");
        assert!(workers.get(cur).expect("cur").drain.is_cordoned());
        assert!(workers.get(stale).expect("stale").drain.is_cordoned());

        // Whole-registry sweep catches stale workers registered prior to sweep.
        let stale2 = register_worker(&mut workers, "alpha", "i-host2", 3, "0.3.6");
        let cur2 = register_worker(&mut workers, "alpha", "i-host2", 4, CURRENT_VERSION);
        let swept = cordons.stale_build_drains(&mut workers);
        assert_eq!(swept, vec![cur, stale, stale2, cur2]);
        assert!(cordons.is_replacing(0, &host2));
        assert!(workers.get(stale2).expect("stale2").drain.is_cordoned());
        assert!(workers.get(cur2).expect("cur2").drain.is_cordoned());
    }

    #[test]
    fn retain_drains_respects_grace_reappearance_fleets_and_active_worker_self_intents() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let beta = FleetName::try_new("beta").expect("beta");
        let host = InstanceId::try_new("i-host").expect("host");
        let grace = Duration::from_secs(10);

        // Active worker with self-drain (`GoingAway`) and paused state on alpha/i-host.
        let active = register_worker(&mut workers, "alpha", "i-host", 1, CURRENT_VERSION);
        assert!(workers.set_going_away(active));
        assert!(workers.set_paused(active, true));

        cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("alpha drain");
        cordons
            .set_draining(&beta, &host, true, &mut workers)
            .expect("beta drain");

        let beta_supply = supply_with(&[("i-host", InstanceLifecycle::Terminating)]);
        let only_beta = Census::from_supply(&[(&beta, &beta_supply)]);

        // Alpha absent at t=0, t=9 (< 10s grace) -> still retained.
        assert!(
            cordons
                .retain_drains(Tick::ORIGIN, grace, &only_beta, &mut workers)
                .is_empty()
        );
        assert!(
            cordons
                .retain_drains(
                    Tick::ORIGIN + Duration::from_secs(9),
                    grace,
                    &only_beta,
                    &mut workers
                )
                .is_empty()
        );
        assert_eq!(cordons.len(), 2);

        // Alpha reappears in Pending lifecycle at t=10 -> resets absence timer!
        let alpha_pending = supply_with(&[("i-host", InstanceLifecycle::Pending)]);
        let both = Census::from_supply(&[(&alpha, &alpha_pending), (&beta, &beta_supply)]);
        cordons.retain_drains(Tick::ORIGIN + grace, grace, &both, &mut workers);
        assert_eq!(cordons.absent_since(0, &host), None);

        // Alpha disappears again at t=11; reissuing set_draining at t=15 must not reset timer.
        cordons.retain_drains(
            Tick::ORIGIN + Duration::from_secs(11),
            grace,
            &only_beta,
            &mut workers,
        );
        assert_eq!(
            cordons.absent_since(0, &host),
            Some(Tick::ORIGIN + Duration::from_secs(11))
        );
        cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("reissue");
        assert_eq!(
            cordons.absent_since(0, &host),
            Some(Tick::ORIGIN + Duration::from_secs(11))
        );

        // At t=20 (elapsed 9s < 10s), still retained.
        cordons.retain_drains(
            Tick::ORIGIN + Duration::from_secs(20),
            grace,
            &only_beta,
            &mut workers,
        );
        assert_eq!(cordons.len(), 2);

        // At t=21 (elapsed 10s >= 10s), alpha is pruned while beta remains.
        let cleared = cordons.retain_drains(
            Tick::ORIGIN + Duration::from_secs(21),
            grace,
            &only_beta,
            &mut workers,
        );
        assert_eq!(cleared, vec![active]);
        assert!(!cordons.contains(0, &host));
        assert!(cordons.contains(1, &host));

        // Active worker's self-drain (`GoingAway`) and `paused` intents were NOT dropped!
        let w = workers.get(active).expect("active");
        assert!(w.drain.is_going_away());
        assert!(w.drain.is_draining());
        assert!(w.drain.is_paused());
    }

    #[test]
    fn retain_drains_handles_max_ticks_zero_grace_and_backward_clock() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let host = InstanceId::try_new("i-host").expect("host");
        let empty = Census::default();
        let maximum = Tick::from_elapsed(Duration::MAX);

        cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("drain");
        cordons.retain_drains(maximum, Duration::MAX, &empty, &mut workers);
        // Backward clock retains rather than pruning.
        cordons.retain_drains(Tick::ORIGIN, Duration::ZERO, &empty, &mut workers);
        assert_eq!(cordons.len(), 1);
        cordons.retain_drains(maximum, Duration::ZERO, &empty, &mut workers);
        assert!(cordons.is_empty());

        cordons
            .set_draining(&alpha, &host, true, &mut workers)
            .expect("drain again");
        cordons.retain_drains(Tick::ORIGIN, Duration::MAX, &empty, &mut workers);
        cordons.retain_drains(
            Tick::from_elapsed(Duration::MAX.saturating_sub(Duration::from_nanos(1))),
            Duration::MAX,
            &empty,
            &mut workers,
        );
        assert_eq!(cordons.len(), 1);
        cordons.retain_drains(maximum, Duration::MAX, &empty, &mut workers);
        assert!(cordons.is_empty());
    }

    #[test]
    fn snapshot_into_reuses_buffer_in_sorted_fleet_instance_order() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let beta = FleetName::try_new("beta").expect("beta");
        let h2 = InstanceId::try_new("i-2").expect("h2");
        let h1 = InstanceId::try_new("i-1").expect("h1");

        cordons
            .set_draining(&beta, &h1, true, &mut workers)
            .expect("beta h1");
        cordons
            .set_draining(&alpha, &h2, true, &mut workers)
            .expect("alpha h2");
        cordons
            .set_draining(&alpha, &h1, true, &mut workers)
            .expect("alpha h1");

        let mut buf = Vec::new();
        cordons.snapshot_into(&mut buf);
        assert_eq!(buf.len(), 3);
        assert_eq!((buf[0].fleet, buf[0].instance.as_str()), (0, "i-1"));
        assert_eq!((buf[1].fleet, buf[1].instance.as_str()), (0, "i-2"));
        assert_eq!((buf[2].fleet, buf[2].instance.as_str()), (1, "i-1"));

        cordons
            .set_draining(&alpha, &h2, false, &mut workers)
            .expect("clear h2");
        cordons.snapshot_into(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!((buf[0].fleet, buf[0].instance.as_str()), (0, "i-1"));
        assert_eq!((buf[1].fleet, buf[1].instance.as_str()), (1, "i-1"));
    }
}
