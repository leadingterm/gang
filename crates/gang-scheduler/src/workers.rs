//! Slab-indexed worker registry, physical instance generation tracking, and `WorkerId` minting.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::Arc;

use gang_spi::WorkerCacheStats;
use gang_types::ActionEnvironment;
use gang_types::ActionResources;
use gang_types::AttemptId;
use gang_types::AttemptIdError;
use gang_types::Bytes;
use gang_types::FleetCatalog;
use gang_types::FleetCountError;
use gang_types::FleetName;
use gang_types::InstanceId;
use gang_types::MAX_FLEETS;
use gang_types::NameError;
use gang_types::WorkerId;
use thiserror::Error;

use crate::ledger::{LedgerError, WorkerLedger};
use crate::operations::{Now, Slab};

/// Generation-checked `u32` handle for a registered worker in [`WorkerRegistry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerKey {
    index: u32,
    generation: u32,
}

impl WorkerKey {
    /// Returns the slab slot index.
    #[must_use]
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Returns the slot generation counter.
    #[must_use]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

/// Compact bitset of drain, cordon, controller-handoff, and pause sources on a [`Worker`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainSources(u8);

impl DrainSources {
    const GOING_AWAY: u8 = 1 << 0;
    const INSTANCE: u8 = 1 << 1;
    const CORDON: u8 = 1 << 2;
    const CONTROLLER: u8 = 1 << 3;
    const PAUSED: u8 = 1 << 4;

    /// Returns `true` when any drain source (`going_away`, `instance`, or `controller`) is active.
    #[must_use]
    pub const fn is_draining(self) -> bool {
        (self.0 & (Self::GOING_AWAY | Self::INSTANCE | Self::CONTROLLER)) != 0
    }

    /// Returns `true` when the worker's physical instance is cordoned.
    #[must_use]
    pub const fn is_cordoned(self) -> bool {
        (self.0 & Self::CORDON) != 0
    }

    /// Returns `true` when the worker is paused.
    #[must_use]
    pub const fn is_paused(self) -> bool {
        (self.0 & Self::PAUSED) != 0
    }

    /// Returns `true` when the worker sent `GoingAway`.
    #[must_use]
    pub const fn is_going_away(self) -> bool {
        (self.0 & Self::GOING_AWAY) != 0
    }

    /// Returns `true` when any drain, cordon, or pause flag blocks assignment.
    #[must_use]
    pub const fn blocks_assignment(self) -> bool {
        self.0 != 0
    }

    const fn set(&mut self, bit: u8, active: bool) {
        if active {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }
}

/// Lifecycle flags on a [`Worker`] generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WorkerFlags(u8);

impl WorkerFlags {
    const CURRENT_BUILD: u8 = 1 << 0;
    const SINGLE_USE: u8 = 1 << 1;
    const SINGLE_USE_EXHAUSTED: u8 = 1 << 2;
    const DISK_HOLD: u8 = 1 << 3;

    const fn get(self, bit: u8) -> bool {
        (self.0 & bit) != 0
    }

    const fn set(&mut self, bit: u8, value: bool) {
        if value {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }
}

/// Errors returned when minting a random [`WorkerId`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkerIdError {
    /// Reading OS randomness failed.
    #[error(transparent)]
    Entropy(#[from] AttemptIdError),
    /// The resulting worker identifier failed validation.
    #[error(transparent)]
    Name(#[from] NameError),
}

/// Mints a deterministic [`WorkerId`] formatted as `{instance}-{uuid}` via
/// [`InstanceId::worker_name`].
///
/// # Errors
///
/// Returns [`NameError`] if the combined identifier is invalid.
pub fn mint_worker_id(
    instance: &InstanceId,
    random_bytes: [u8; 16],
) -> Result<WorkerId, NameError> {
    let uuid = AttemptId::from_random_bytes(random_bytes);
    WorkerId::try_new(format!("{}{uuid}", instance.worker_name()))
}

/// Mints a random [`WorkerId`] formatted as `{instance}-{uuid}` using OS entropy.
///
/// # Errors
///
/// Returns [`WorkerIdError`] if randomness cannot be read or the identifier is invalid.
pub fn mint_random_worker_id(instance: &InstanceId) -> Result<WorkerId, WorkerIdError> {
    let uuid = AttemptId::generate()?;
    Ok(WorkerId::try_new(format!(
        "{}{uuid}",
        instance.worker_name()
    ))?)
}

/// Registration payload for a newly connected worker generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRegistration {
    /// Physical instance hosting this worker process.
    pub instance: Arc<InstanceId>,
    /// Fleet name the worker belongs to.
    pub fleet: FleetName,
    /// Worker Gang package version string.
    pub gang_version: String,
    /// Registered worker properties (`FleetProperties::worker_properties()`).
    pub properties: BTreeMap<String, String>,
    /// Registered ledger capacity (oversubscribed CPU, post-reserve memory, action disk).
    pub capacity: ActionResources,
    /// Effective per-action resource ceilings enforced by the worker's fleet.
    pub ceilings: ActionResources,
    /// Maximum concurrent attempts accepted by this worker (`None` means unlimited).
    pub max_inflight: Option<NonZeroU32>,
    /// Whether this worker serves at most a single action and then exits.
    pub single_use: bool,
    /// Operational action environment reported by this worker, if valid.
    pub environment: Option<ActionEnvironment>,
    /// Deterministic 16-byte UUID seed for minting the worker's [`WorkerId`].
    pub uuid_bytes: [u8; 16],
}

/// Errors returned by [`WorkerRegistry::register`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistrationError {
    /// The worker named a fleet missing from the catalog.
    #[error("unknown fleet `{fleet}`")]
    UnknownFleet {
        /// The unrecognized fleet name.
        fleet: FleetName,
    },
    /// Minting the [`WorkerId`] from the instance name failed.
    #[error("invalid worker identifier: {0}")]
    InvalidWorkerId(#[from] NameError),
    /// The generated [`WorkerId`] collided with an active worker.
    #[error("duplicate worker id `{id}`")]
    DuplicateWorkerId {
        /// The colliding worker identifier.
        id: WorkerId,
    },
    /// The worker slab reached `u32::MAX` entries.
    #[error("worker slab capacity exhausted")]
    SlabFull,
}

/// A connected worker generation tracked by [`WorkerRegistry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    /// Controller-minted identifier starting with `{instance}-`.
    pub id: WorkerId,
    /// Catalog index of the worker's fleet.
    pub fleet: usize,
    /// Name of the worker's fleet.
    pub fleet_name: FleetName,
    /// Physical instance hosting this worker generation.
    pub instance: Arc<InstanceId>,
    /// Monotonic generation sequence within the physical instance.
    pub instance_generation_seq: u64,
    /// Reported Gang package version string.
    pub gang_version: String,
    /// Registered worker properties.
    pub properties: BTreeMap<String, String>,
    /// Operational action environment reported by this worker.
    pub environment: Option<ActionEnvironment>,
    /// Effective per-action resource ceilings.
    pub ceilings: ActionResources,
    /// Capacity and reservation conservation ledger.
    pub ledger: WorkerLedger,
    /// Currently active (`Offered` or `Running`) attempts on this worker.
    pub inflight_attempts: u32,
    /// Currently active gate-tier (`priority >= 200`) attempts on this worker.
    pub gate_running: u32,
    /// Maximum concurrent attempts allowed (`None` = unlimited).
    pub max_inflight: Option<NonZeroU32>,
    /// Active drain, cordon, controller, and pause sources.
    pub drain: DrainSources,
    /// Monotonic sequence stamped on each assignment for LRU tie-breaking.
    pub last_assigned_seq: u64,
    /// Last monotonic/wall timestamp when this worker spoke to the scheduler.
    pub last_seen: Now,
    /// Timestamp when this worker generation registered.
    pub registered_at: Now,
    /// Latest store counters reported in `KeepAlive`.
    pub cache_stats: Option<WorkerCacheStats>,
    flags: WorkerFlags,
}

impl Worker {
    /// Returns `true` when the worker's `gang_version` matches the controller's build.
    #[must_use]
    pub const fn is_current_build(&self) -> bool {
        self.flags.get(WorkerFlags::CURRENT_BUILD)
    }

    /// Returns `true` when this worker is configured for single-use execution.
    #[must_use]
    pub const fn is_single_use(&self) -> bool {
        self.flags.get(WorkerFlags::SINGLE_USE)
    }

    /// Returns `true` when a single-use worker has already executed or retired its single action.
    #[must_use]
    pub const fn is_single_use_exhausted(&self) -> bool {
        self.flags.get(WorkerFlags::SINGLE_USE_EXHAUSTED)
    }

    /// Returns `true` when a `Decline(DiskShortage)` hold awaits the next `KeepAlive`.
    #[must_use]
    pub const fn has_disk_hold(&self) -> bool {
        self.flags.get(WorkerFlags::DISK_HOLD)
    }

    /// Returns `true` when the worker has an available concurrent attempt slot.
    #[must_use]
    pub const fn has_inflight_slot(&self) -> bool {
        if self.is_single_use() && (self.is_single_use_exhausted() || self.inflight_attempts > 0) {
            return false;
        }
        match self.max_inflight {
            Some(max) => self.inflight_attempts < max.get(),
            None => true,
        }
    }

    /// Returns `true` when the worker's flags, build match, disk hold, and slot limit allow
    /// scheduling new work onto it (before checking an operation's fleet/resource fit).
    #[must_use]
    pub const fn is_available_for_dispatch(&self) -> bool {
        !self.drain.blocks_assignment()
            && self.is_current_build()
            && !self.has_disk_hold()
            && self.has_inflight_slot()
    }
}

/// Physical instance state shared across overlapping worker renewal generations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceState {
    /// Currently connected worker generations for this `(fleet, instance)`.
    pub generations: Vec<WorkerKey>,
    /// Persistent operator/reconciler drain intent for this physical instance.
    pub draining: bool,
    /// Persistent cordon state (e.g. stale-build replacement) for this physical instance.
    pub cordoned: bool,
    /// Latest authoritative ledger CPU capacity fact (in millicores) reported by this instance.
    pub cpu_capacity_millicores: Option<u64>,
    /// Latest authoritative physical/post-reserve memory capacity fact reported by this instance.
    pub memory_capacity: Option<Bytes>,
    /// Latest authoritative physical action disk capacity fact reported by this instance.
    pub disk_capacity: Option<Bytes>,
    capacity_fact_seq: u64,
    next_generation_seq: u64,
}

impl Default for InstanceState {
    fn default() -> Self {
        Self {
            generations: Vec::new(),
            draining: false,
            cordoned: false,
            cpu_capacity_millicores: None,
            memory_capacity: None,
            disk_capacity: None,
            capacity_fact_seq: 0,
            next_generation_seq: 1,
        }
    }
}

/// Slab-indexed registry of connected worker generations and physical instance state.
#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    slab: Slab<Worker>,
    by_id: BTreeMap<WorkerId, WorkerKey>,
    by_fleet: Vec<Vec<WorkerKey>>,
    instances: BTreeMap<(usize, Arc<InstanceId>), InstanceState>,
    fleet_lookup: HashMap<String, usize>,
    next_assign_seq: u64,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self {
            slab: Slab::new(),
            by_id: BTreeMap::new(),
            by_fleet: vec![Vec::new(); MAX_FLEETS],
            instances: BTreeMap::new(),
            fleet_lookup: HashMap::new(),
            next_assign_seq: 1,
        }
    }
}

impl WorkerRegistry {
    /// Creates an empty registry matching the fleets in `catalog`.
    ///
    /// # Errors
    ///
    /// Returns [`FleetCountError`] if `catalog.len() > MAX_FLEETS`.
    pub fn from_catalog(catalog: &FleetCatalog) -> Result<Self, FleetCountError> {
        let count = catalog.len();
        if count > MAX_FLEETS {
            return Err(FleetCountError { count });
        }
        let mut fleet_lookup = HashMap::with_capacity(count);
        for (index, fleet) in catalog.fleets().iter().enumerate() {
            fleet_lookup.insert(fleet.name().to_owned(), index);
        }
        Ok(Self {
            slab: Slab::new(),
            by_id: BTreeMap::new(),
            by_fleet: vec![Vec::new(); count],
            instances: BTreeMap::new(),
            fleet_lookup,
            next_assign_seq: 1,
        })
    }

    /// Returns the number of connected worker generations.
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.slab.len()
    }

    /// Returns the number of tracked physical instances (including retained drain/cordon intents).
    #[must_use]
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Borrows a connected worker by its generation-checked [`WorkerKey`].
    #[must_use]
    pub fn get(&self, key: WorkerKey) -> Option<&Worker> {
        self.slab.get(key.index, key.generation)
    }

    /// Mutably borrows a connected worker by its generation-checked [`WorkerKey`].
    pub fn get_mut(&mut self, key: WorkerKey) -> Option<&mut Worker> {
        self.slab.get_mut(key.index, key.generation)
    }

    /// Resolves a [`WorkerId`] to its [`WorkerKey`].
    #[must_use]
    pub fn lookup_id(&self, id: &WorkerId) -> Option<WorkerKey> {
        self.by_id.get(id).copied()
    }

    /// Returns the [`WorkerKey`]s of currently connected workers in `fleet`.
    #[must_use]
    pub fn fleet_workers(&self, fleet: usize) -> &[WorkerKey] {
        self.by_fleet.get(fleet).map_or(&[], Vec::as_slice)
    }

    /// Borrows the [`InstanceState`] for `(fleet, instance)`, if present.
    #[must_use]
    pub fn instance_state(&self, fleet: usize, instance: &InstanceId) -> Option<&InstanceState> {
        self.instances
            .iter()
            .find(|((idx, id), _)| *idx == fleet && id.as_ref() == instance)
            .map(|(_, state)| state)
    }

    /// Iterates over all connected workers in ascending [`WorkerId`] order.
    pub fn workers_sorted(&self) -> impl Iterator<Item = (WorkerKey, &Worker)> {
        self.by_id
            .values()
            .filter_map(|&key| self.get(key).map(|worker| (key, worker)))
    }

    /// Registers a new worker generation, deduplicating physical instance state by `(fleet,
    /// instance)`.
    ///
    /// # Errors
    ///
    /// Returns [`RegistrationError`] when the fleet is unknown, [`WorkerId`] minting fails or
    /// collides, or the worker slab is full.
    pub fn register(
        &mut self,
        now: Now,
        expected_gang_version: &str,
        controller_draining: bool,
        registration: WorkerRegistration,
    ) -> Result<WorkerKey, RegistrationError> {
        let Some(&fleet) = self.fleet_lookup.get(registration.fleet.as_str()) else {
            return Err(RegistrationError::UnknownFleet {
                fleet: registration.fleet,
            });
        };

        let id = mint_worker_id(&registration.instance, registration.uuid_bytes)?;
        if self.by_id.contains_key(&id) {
            return Err(RegistrationError::DuplicateWorkerId { id });
        }

        let instance_key = (fleet, Arc::clone(&registration.instance));
        let instance_entry = self.instances.entry(instance_key).or_default();
        let instance_generation_seq = instance_entry.next_generation_seq;
        instance_entry.next_generation_seq = instance_entry.next_generation_seq.saturating_add(1);
        if registration.gang_version == expected_gang_version {
            instance_entry.cpu_capacity_millicores = Some(registration.capacity.cpu_millicores);
            instance_entry.memory_capacity = Some(registration.capacity.memory);
            instance_entry.disk_capacity = Some(registration.capacity.disk);
            instance_entry.capacity_fact_seq = instance_generation_seq;
        }

        let mut drain = DrainSources::default();
        drain.set(DrainSources::INSTANCE, instance_entry.draining);
        drain.set(DrainSources::CORDON, instance_entry.cordoned);
        drain.set(DrainSources::CONTROLLER, controller_draining);

        let mut flags = WorkerFlags::default();
        flags.set(
            WorkerFlags::CURRENT_BUILD,
            registration.gang_version == expected_gang_version,
        );
        flags.set(WorkerFlags::SINGLE_USE, registration.single_use);

        let worker = Worker {
            id: id.clone(),
            fleet,
            fleet_name: registration.fleet,
            instance: registration.instance,
            instance_generation_seq,
            gang_version: registration.gang_version,
            properties: registration.properties,
            environment: registration.environment,
            ceilings: registration.ceilings,
            ledger: WorkerLedger::new(registration.capacity),
            inflight_attempts: 0,
            gate_running: 0,
            max_inflight: registration.max_inflight,
            drain,
            last_assigned_seq: 0,
            last_seen: now,
            registered_at: now,
            cache_stats: None,
            flags,
        };

        let Some((index, generation)) = self.slab.insert(worker) else {
            return Err(RegistrationError::SlabFull);
        };
        let key = WorkerKey { index, generation };
        self.by_id.insert(id, key);
        if let Some(fleet_list) = self.by_fleet.get_mut(fleet) {
            fleet_list.push(key);
        }
        instance_entry.generations.push(key);
        Ok(key)
    }

    /// Disconnects and removes a worker generation while preserving persistent instance drain/cordon
    /// intents.
    pub fn disconnect(&mut self, key: WorkerKey) -> Option<Worker> {
        let worker = self.slab.remove(key.index, key.generation)?;
        if self.by_id.get(&worker.id) == Some(&key) {
            self.by_id.remove(&worker.id);
        }
        if let Some(fleet_list) = self.by_fleet.get_mut(worker.fleet) {
            fleet_list.retain(|&entry| entry != key);
        }
        let instance_key = (worker.fleet, Arc::clone(&worker.instance));
        let remove_instance = if let Some(state) = self.instances.get_mut(&instance_key) {
            state.generations.retain(|&entry| entry != key);
            state.generations.is_empty() && !state.draining && !state.cordoned
        } else {
            false
        };
        if remove_instance {
            self.instances.remove(&instance_key);
        }
        Some(worker)
    }

    /// Updates `last_seen`, clears any `DiskShortage` hold, refreshes cache stats, and applies
    /// authoritative memory capacity facts when sent by the latest generation.
    ///
    /// Returns `Some(cleared_disk_hold)` if `key` is live, where `cleared_disk_hold` is `true`
    /// when a prior `DiskShortage` hold was lifted.
    pub fn on_keepalive(
        &mut self,
        key: WorkerKey,
        now: Now,
        cache: Option<WorkerCacheStats>,
        memory_capacity: Option<Bytes>,
    ) -> Option<bool> {
        let (fleet, instance, gen_seq, had_hold, current_build) = {
            let worker = self.get_mut(key)?;
            worker.last_seen = now;
            let had_hold = worker.has_disk_hold();
            worker.flags.set(WorkerFlags::DISK_HOLD, false);
            if let Some(stats) = cache {
                worker.cache_stats = Some(stats);
            }
            (
                worker.fleet,
                Arc::clone(&worker.instance),
                worker.instance_generation_seq,
                had_hold,
                worker.is_current_build(),
            )
        };

        if let Some(memory) = memory_capacity.filter(|_| current_build) {
            let instance_key = (fleet, instance);
            let generations_to_update = if let Some(state) = self.instances.get_mut(&instance_key) {
                if gen_seq >= state.capacity_fact_seq {
                    state.memory_capacity = Some(memory);
                    state.capacity_fact_seq = gen_seq;
                    Some(state.generations.clone())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(generations) = generations_to_update {
                for gen_key in generations {
                    if let Some(worker) = self.get_mut(gen_key) {
                        worker.ledger.update_memory_capacity(memory);
                    }
                }
            }
        }
        Some(had_hold)
    }

    /// Marks `key` as holding on disk (`Decline(DiskShortage)`) until its next `KeepAlive`.
    pub fn set_disk_hold(&mut self, key: WorkerKey) -> bool {
        let Some(worker) = self.get_mut(key) else {
            return false;
        };
        worker.flags.set(WorkerFlags::DISK_HOLD, true);
        true
    }

    /// Marks `key` as `GoingAway` (no new assignments).
    pub fn set_going_away(&mut self, key: WorkerKey) -> bool {
        let Some(worker) = self.get_mut(key) else {
            return false;
        };
        worker.drain.set(DrainSources::GOING_AWAY, true);
        true
    }

    /// Updates controller-wide draining state on `key`.
    pub fn set_controller_draining(&mut self, key: WorkerKey, draining: bool) -> bool {
        let Some(worker) = self.get_mut(key) else {
            return false;
        };
        worker.drain.set(DrainSources::CONTROLLER, draining);
        true
    }

    /// Updates the paused state on `key`.
    pub fn set_paused(&mut self, key: WorkerKey, paused: bool) -> bool {
        let Some(worker) = self.get_mut(key) else {
            return false;
        };
        worker.drain.set(DrainSources::PAUSED, paused);
        true
    }

    /// Applies persistent drain intent to `(fleet, instance)` and all connected generations.
    ///
    /// Returns the keys of currently connected generations whose drain state applies.
    pub fn set_instance_draining(
        &mut self,
        fleet: usize,
        instance: &Arc<InstanceId>,
        draining: bool,
    ) -> Vec<WorkerKey> {
        let key = (fleet, Arc::clone(instance));
        let generations = if draining {
            let state = self.instances.entry(key).or_default();
            state.draining = true;
            state.generations.clone()
        } else if let Some(state) = self.instances.get_mut(&key) {
            state.draining = false;
            let gens = state.generations.clone();
            if state.generations.is_empty() && !state.cordoned {
                self.instances.remove(&key);
            }
            gens
        } else {
            Vec::new()
        };

        for &worker_key in &generations {
            if let Some(worker) = self.get_mut(worker_key) {
                worker.drain.set(DrainSources::INSTANCE, draining);
            }
        }
        generations
    }

    /// Applies persistent cordon state to `(fleet, instance)` and all connected generations.
    ///
    /// Returns the keys of currently connected generations whose cordon state applies.
    pub fn set_instance_cordon(
        &mut self,
        fleet: usize,
        instance: &Arc<InstanceId>,
        cordoned: bool,
    ) -> Vec<WorkerKey> {
        let key = (fleet, Arc::clone(instance));
        let generations = if cordoned {
            let state = self.instances.entry(key).or_default();
            state.cordoned = true;
            state.generations.clone()
        } else if let Some(state) = self.instances.get_mut(&key) {
            state.cordoned = false;
            let gens = state.generations.clone();
            if state.generations.is_empty() && !state.draining {
                self.instances.remove(&key);
            }
            gens
        } else {
            Vec::new()
        };

        for &worker_key in &generations {
            if let Some(worker) = self.get_mut(worker_key) {
                worker.drain.set(DrainSources::CORDON, cordoned);
            }
        }
        generations
    }

    /// Commits a placement assignment onto `key`: reserves ledger capacity, increments inflight
    /// and optional gate counters, and stamps `last_assigned_seq`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if `key` lacks sufficient capacity or is missing.
    pub fn commit_assignment(
        &mut self,
        key: WorkerKey,
        reservation: ActionResources,
        is_gate: bool,
    ) -> Result<(), LedgerError> {
        let seq = self.next_assign_seq;
        let Some(worker) = self.get_mut(key) else {
            return Err(LedgerError::InsufficientCapacity { resource: "worker" });
        };
        worker.ledger.try_reserve(reservation)?;
        worker.inflight_attempts = worker.inflight_attempts.saturating_add(1);
        if is_gate {
            worker.gate_running = worker.gate_running.saturating_add(1);
        }
        worker.last_assigned_seq = seq;
        self.next_assign_seq = self.next_assign_seq.saturating_add(1);
        Ok(())
    }

    /// Updates worker counters and ledger when an attempt reports `Executed` (CPU and memory
    /// released, disk kept until `Completed`).
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if releasing compute capacity underflows.
    pub fn on_attempt_executed(
        &mut self,
        key: WorkerKey,
        reservation: ActionResources,
        is_gate: bool,
    ) -> Result<(), LedgerError> {
        let Some(worker) = self.get_mut(key) else {
            return Err(LedgerError::Underflow { resource: "worker" });
        };
        worker.ledger.release_compute(reservation)?;
        worker.inflight_attempts = worker.inflight_attempts.saturating_sub(1);
        if is_gate {
            worker.gate_running = worker.gate_running.saturating_sub(1);
        }
        if worker.is_single_use() {
            worker.flags.set(WorkerFlags::SINGLE_USE_EXHAUSTED, true);
        }
        Ok(())
    }

    /// Releases the disk reservation when a previously `Executed` attempt reports `Completed`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if releasing disk underflows.
    pub fn on_attempt_completed(&mut self, key: WorkerKey, disk: Bytes) -> Result<(), LedgerError> {
        let Some(worker) = self.get_mut(key) else {
            return Err(LedgerError::Underflow { resource: "worker" });
        };
        worker.ledger.release_disk(disk)
    }

    /// Releases all reservations and inflight counters for an attempt terminated prior to
    /// `Executed` (e.g. `Decline` or worker loss).
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if releasing reservations underflows.
    pub fn on_attempt_aborted(
        &mut self,
        key: WorkerKey,
        reservation: ActionResources,
        is_gate: bool,
    ) -> Result<(), LedgerError> {
        let Some(worker) = self.get_mut(key) else {
            return Err(LedgerError::Underflow { resource: "worker" });
        };
        worker.ledger.release_all(reservation)?;
        worker.inflight_attempts = worker.inflight_attempts.saturating_sub(1);
        if is_gate {
            worker.gate_running = worker.gate_running.saturating_sub(1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::FleetActionPolicy;
    use gang_types::FleetProperties;
    use gang_types::Isa;
    use gang_types::Resources;

    use super::*;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn catalog() -> FleetCatalog {
        let props = FleetProperties::new(
            &FleetName::try_new("build-x86").expect("fleet"),
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
        );
        FleetCatalog::try_new(vec![props]).expect("catalog")
    }

    fn registration(instance: &str, seed: u8, mem_gib: u64) -> WorkerRegistration {
        WorkerRegistration {
            instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
            fleet: FleetName::try_new("build-x86").expect("fleet"),
            gang_version: "0.3.9".to_owned(),
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores: 16_000,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(100),
            },
            ceilings: ActionResources {
                cpu_millicores: 8_000,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(100),
            },
            max_inflight: None,
            single_use: false,
            environment: None,
            uuid_bytes: [seed; 16],
        }
    }

    #[test]
    fn worker_id_minting_round_trips_instance_prefix() {
        let instance = InstanceId::try_new("i-0abc123").expect("instance");
        let id = mint_worker_id(&instance, [0x42; 16]).expect("worker id");
        assert!(id.as_str().starts_with(&instance.worker_name()));
        assert_eq!(id.instance().as_ref(), Some(&instance));
        assert!(instance.hosts(&id));

        let random = mint_random_worker_id(&instance).expect("random worker id");
        assert!(random.as_str().starts_with(&instance.worker_name()));
        assert_eq!(random.instance().as_ref(), Some(&instance));
        assert_ne!(id, random);
    }

    #[test]
    fn overlapping_generations_dedup_into_one_instance_state() {
        let cat = catalog();
        let mut reg = WorkerRegistry::from_catalog(&cat).expect("registry");

        let gen1 = reg
            .register(now(1), "0.3.9", false, registration("i-host1", 1, 16))
            .expect("gen1");
        let gen2 = reg
            .register(now(2), "0.3.9", false, registration("i-host1", 2, 15))
            .expect("gen2");

        assert_eq!(reg.worker_count(), 2);
        assert_eq!(reg.instance_count(), 1);

        let inst = InstanceId::try_new("i-host1").expect("instance");
        let state = reg.instance_state(0, &inst).expect("instance state");
        assert_eq!(state.generations, vec![gen1, gen2]);
        assert_eq!(state.memory_capacity, Some(Bytes::gib(15)));

        // Stale gen1 keepalive cannot overwrite newer gen2 capacity fact, while gen2 can.
        assert_eq!(
            reg.on_keepalive(gen1, now(3), None, Some(Bytes::gib(12))),
            Some(false)
        );
        assert_eq!(
            reg.instance_state(0, &inst).expect("state").memory_capacity,
            Some(Bytes::gib(15))
        );
        assert_eq!(
            reg.on_keepalive(gen2, now(4), None, Some(Bytes::gib(14))),
            Some(false)
        );
        assert_eq!(
            reg.instance_state(0, &inst).expect("state").memory_capacity,
            Some(Bytes::gib(14))
        );
        assert_eq!(
            reg.get(gen1).expect("gen1").ledger.capacity().memory,
            Bytes::gib(14)
        );
        assert_eq!(
            reg.get(gen2).expect("gen2").ledger.capacity().memory,
            Bytes::gib(14)
        );

        // Drain intent applies across both generations and survives disconnect until cleared.
        let inst_arc = Arc::new(inst);
        let affected = reg.set_instance_draining(0, &inst_arc, true);
        assert_eq!(affected, vec![gen1, gen2]);
        assert!(reg.get(gen1).expect("gen1").drain.is_draining());
        assert!(reg.get(gen2).expect("gen2").drain.is_draining());

        reg.disconnect(gen1);
        reg.disconnect(gen2);
        assert_eq!(reg.worker_count(), 0);
        // Persistent drain intent remains until cleared.
        assert_eq!(reg.instance_count(), 1);

        let gen3 = reg
            .register(now(5), "0.3.9", false, registration("i-host1", 3, 15))
            .expect("gen3");
        assert!(reg.get(gen3).expect("gen3").drain.is_draining());
    }

    #[test]
    fn single_use_and_attempt_lifecycle_bookkeeping() {
        let cat = catalog();
        let mut reg = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut single = registration("i-single", 7, 16);
        single.single_use = true;
        let w = reg
            .register(now(1), "0.3.9", false, single)
            .expect("register");

        let req = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(5),
        };
        assert!(reg.get(w).expect("w").is_available_for_dispatch());
        reg.commit_assignment(w, req, true).expect("assign");
        assert!(!reg.get(w).expect("w").has_inflight_slot());
        assert_eq!(reg.get(w).expect("w").gate_running, 1);

        reg.on_attempt_executed(w, req, true).expect("executed");
        assert_eq!(reg.get(w).expect("w").gate_running, 0);
        assert!(reg.get(w).expect("w").is_single_use_exhausted());
        assert!(!reg.get(w).expect("w").is_available_for_dispatch());

        reg.on_attempt_completed(w, req.disk).expect("completed");
        assert_eq!(
            reg.get(w).expect("w").ledger.reserved(),
            ActionResources::default()
        );
    }
}
