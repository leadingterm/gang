//! Observed physical fleet capacity facts and effective per-action resource ceilings (`§5.3`).
//!
//! Configured fleet estimates in [`FleetCatalog`] are conservatively lowered by local connected
//! worker memory reports and optional peer-local facts, never rising above the configured
//! estimate and keeping configured per-action ceilings independent.

use std::num::NonZeroU32;

use gang_spi::FleetMemoryCapacity;
use gang_types::{
    Bytes, FleetCatalog, FleetCountError, FleetProperties, MAX_FLEETS, NonZeroBytes, property,
};
use thiserror::Error;

use crate::workers::WorkerRegistry;

/// Errors updating observed fleet capacities.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CapacityError {
    /// A capacity observation named a fleet index outside the catalog.
    #[error("fleet index {fleet} out of range (catalog has {count} fleets)")]
    UnknownFleet {
        /// Out-of-bounds fleet index.
        fleet: usize,
        /// Configured catalog fleet count.
        count: usize,
    },
    /// A capacity observation cannot be represented as whole `KiB` (`>= 1 KiB`).
    #[error("observed memory capacity {bytes} for fleet {fleet} is below 1 KiB")]
    BelowOneKib {
        /// Fleet index of the observation.
        fleet: usize,
        /// Observed memory capacity in bytes.
        bytes: Bytes,
    },
    /// Rebuilding the derived [`FleetCatalog`] exceeded the maximum fleet count.
    #[error("too many fleets ({count}) for catalog")]
    TooManyFleets {
        /// Number of fleets provided.
        count: usize,
    },
}

impl From<FleetCountError> for CapacityError {
    fn from(err: FleetCountError) -> Self {
        Self::TooManyFleets { count: err.count }
    }
}

/// Physical capacities and effective per-action ceilings for one fleet at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FleetCapacity {
    /// Catalog index of the fleet.
    pub fleet: usize,
    /// Physical `vCPU` count per instance.
    pub physical_vcpus: u32,
    /// Physical per-instance CPU in millicores (`physical_vcpus * 1000`).
    pub physical_cpu_millicores: u64,
    /// Aggregate oversubscribed CPU ledger capacity (`physical_cpu_millicores * cpu_oversubscription`).
    pub ledger_cpu_millicores: u64,
    /// Effective per-action CPU ceiling in millicores.
    pub cpu_ceiling_millicores: u64,
    /// Current observed post-reserve memory capacity in `KiB`.
    pub physical_memory_kb: u64,
    /// Effective per-action memory ceiling in `KiB`.
    pub memory_ceiling_kb: u64,
    /// Configured physical action disk capacity per instance in `KiB`.
    pub physical_disk_kb: u64,
    /// Effective per-action disk ceiling in `KiB`.
    pub disk_ceiling_kb: u64,
}

impl FleetCapacity {
    /// Returns `true` when the fleet's memory ceiling is strictly below physical memory capacity.
    #[must_use]
    pub const fn memory_limited_by_policy(self) -> bool {
        self.memory_ceiling_kb < self.physical_memory_kb
    }

    /// Returns `true` when the fleet's disk ceiling is strictly below physical disk capacity.
    #[must_use]
    pub const fn disk_limited_by_policy(self) -> bool {
        self.disk_ceiling_kb < self.physical_disk_kb
    }
}

/// Maintains the current [`FleetCatalog`] derived from configured estimates and live observed
/// physical memory capacity facts.
#[derive(Debug, Clone)]
pub struct CapacityCatalog {
    estimates: FleetCatalog,
    current: FleetCatalog,
    cpu_oversubscription: NonZeroU32,
    estimate_memory_kb: Vec<u64>,
    local_memory_kb: Vec<Option<u64>>,
    peer_memory_kb: Vec<Option<u64>>,
    effective_memory_kb: Vec<u64>,
    revision: u64,
}

impl CapacityCatalog {
    /// Creates a capacity catalog initialized with the configured prelaunch `estimates`.
    #[must_use]
    pub fn new(estimates: FleetCatalog, cpu_oversubscription: NonZeroU32) -> Self {
        let count = estimates.len();
        let estimate_memory_kb: Vec<u64> = estimates
            .fleets()
            .iter()
            .map(|fleet| fleet.capacity(property::MEMORY_KB).unwrap_or(0))
            .collect();
        let effective_memory_kb = estimate_memory_kb.clone();
        Self {
            current: estimates.clone(),
            estimates,
            cpu_oversubscription,
            estimate_memory_kb,
            local_memory_kb: vec![None; count],
            peer_memory_kb: vec![None; count],
            effective_memory_kb,
            revision: 0,
        }
    }

    /// Returns the immutable prelaunch estimate catalog.
    #[must_use]
    pub const fn estimates(&self) -> &FleetCatalog {
        &self.estimates
    }

    /// Returns the current catalog reflecting observed capacity facts.
    #[must_use]
    pub const fn current(&self) -> &FleetCatalog {
        &self.current
    }

    /// Returns the configured aggregate CPU oversubscription factor.
    #[must_use]
    pub const fn cpu_oversubscription(&self) -> NonZeroU32 {
        self.cpu_oversubscription
    }

    /// Monotonic revision incremented whenever [`Self::current`] changes.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the current physical capacities and effective ceilings for `fleet`.
    #[must_use]
    pub fn fleet(&self, fleet: usize) -> Option<FleetCapacity> {
        let props = self.current.fleets().get(fleet)?;
        let physical_vcpus = props
            .capacity(property::CPU_COUNT)
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0);
        let physical_cpu_millicores = props.capacity(property::CPU_MILLICORES).unwrap_or(0);
        let ledger_cpu_millicores =
            physical_cpu_millicores.saturating_mul(u64::from(self.cpu_oversubscription.get()));
        let cpu_ceiling_millicores = props
            .limit(property::CPU_MILLICORES)
            .unwrap_or(physical_cpu_millicores);
        let physical_memory_kb = props.capacity(property::MEMORY_KB).unwrap_or(0);
        let memory_ceiling_kb = props
            .limit(property::MEMORY_KB)
            .unwrap_or(physical_memory_kb);
        let physical_disk_kb = props.capacity(property::DISK_KB).unwrap_or(0);
        let disk_ceiling_kb = props.limit(property::DISK_KB).unwrap_or(physical_disk_kb);
        Some(FleetCapacity {
            fleet,
            physical_vcpus,
            physical_cpu_millicores,
            ledger_cpu_millicores,
            cpu_ceiling_millicores,
            physical_memory_kb,
            memory_ceiling_kb,
            physical_disk_kb,
            disk_ceiling_kb,
        })
    }

    /// Recomputes local observed memory capacities from the currently connected workers in
    /// `workers`, returning `true` when [`Self::current`] changed.
    pub fn refresh_local(&mut self, workers: &WorkerRegistry) -> bool {
        let mut observed = [None::<u64>; MAX_FLEETS];
        for (_, worker) in workers.workers_sorted() {
            if worker.fleet >= self.estimates.len() {
                continue;
            }
            let bytes = workers
                .instance_state(worker.fleet, &worker.instance)
                .and_then(|state| state.memory_capacity)
                .unwrap_or_else(|| worker.ledger.capacity().memory);
            let kib = bytes.get() / 1024;
            if kib == 0 {
                continue;
            }
            observed[worker.fleet] = Some(match observed[worker.fleet] {
                Some(prev) => prev.min(kib),
                None => kib,
            });
        }

        let mut changed = false;
        for (fleet, slot) in self.local_memory_kb.iter_mut().enumerate() {
            let next = observed[fleet];
            if *slot != next {
                *slot = next;
                changed = true;
            }
        }
        if !changed {
            return false;
        }
        self.rebuild().unwrap_or(false)
    }

    /// Replaces the local observed memory capacities from an explicit set of fleet observations,
    /// combining multiple observations for the same fleet by minimum.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityError`] if any observation names an unknown fleet or is smaller than
    /// `1 KiB`.
    pub fn set_local_observations(
        &mut self,
        observations: impl IntoIterator<Item = FleetMemoryCapacity>,
    ) -> Result<bool, CapacityError> {
        let next = self.collect_observations(observations)?;
        if self.local_memory_kb == next {
            return Ok(false);
        }
        self.local_memory_kb = next;
        self.rebuild()
    }

    /// Replaces the peer-local observed memory capacities from peer beacons, combining multiple
    /// observations for the same fleet by minimum.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityError`] if any observation names an unknown fleet or is smaller than
    /// `1 KiB`.
    pub fn set_peer_observations(
        &mut self,
        observations: impl IntoIterator<Item = FleetMemoryCapacity>,
    ) -> Result<bool, CapacityError> {
        let next = self.collect_observations(observations)?;
        if self.peer_memory_kb == next {
            return Ok(false);
        }
        self.peer_memory_kb = next;
        self.rebuild()
    }

    /// Clears all peer-local memory observations, returning `true` if [`Self::current`] changed.
    pub fn clear_peer_observations(&mut self) -> bool {
        if self.peer_memory_kb.iter().all(Option::is_none) {
            return false;
        }
        self.peer_memory_kb.fill(None);
        self.rebuild().unwrap_or(false)
    }

    /// Populates `into` with local-only observed memory capacities (or the prelaunch estimate when
    /// a fleet currently has no local observations), reusing `into`'s buffer.
    pub fn local_fleet_memory(&self, into: &mut Vec<FleetMemoryCapacity>) {
        into.clear();
        for (fleet, (&estimate_kb, &local_kb)) in self
            .estimate_memory_kb
            .iter()
            .zip(&self.local_memory_kb)
            .enumerate()
        {
            let kb = local_kb.map_or(estimate_kb, |obs| estimate_kb.min(obs));
            if let Some(capacity) = NonZeroBytes::new(Bytes::kib(kb)) {
                into.push(FleetMemoryCapacity { fleet, capacity });
            }
        }
    }

    /// Populates `into` with the current conservative usable memory per fleet (combining estimate,
    /// local observations, and peer-local observations), reusing `into`'s buffer.
    pub fn fleet_memory(&self, into: &mut Vec<FleetMemoryCapacity>) {
        into.clear();
        for (fleet, &kb) in self.effective_memory_kb.iter().enumerate() {
            if let Some(capacity) = NonZeroBytes::new(Bytes::kib(kb)) {
                into.push(FleetMemoryCapacity { fleet, capacity });
            }
        }
    }

    fn collect_observations(
        &self,
        observations: impl IntoIterator<Item = FleetMemoryCapacity>,
    ) -> Result<Vec<Option<u64>>, CapacityError> {
        let count = self.estimates.len();
        let mut next = vec![None::<u64>; count];
        for obs in observations {
            if obs.fleet >= count {
                return Err(CapacityError::UnknownFleet {
                    fleet: obs.fleet,
                    count,
                });
            }
            let kib = obs.capacity.as_u64() / 1024;
            if kib == 0 {
                return Err(CapacityError::BelowOneKib {
                    fleet: obs.fleet,
                    bytes: obs.capacity.get(),
                });
            }
            next[obs.fleet] = Some(match next[obs.fleet] {
                Some(prev) => prev.min(kib),
                None => kib,
            });
        }
        Ok(next)
    }

    fn rebuild(&mut self) -> Result<bool, CapacityError> {
        let mut new_effective = Vec::with_capacity(self.estimates.len());
        let mut changed = false;
        for (fleet, &estimate_kb) in self.estimate_memory_kb.iter().enumerate() {
            let mut kb = estimate_kb;
            if let Some(local_kb) = self.local_memory_kb[fleet] {
                kb = kb.min(local_kb);
            }
            if let Some(peer_kb) = self.peer_memory_kb[fleet] {
                kb = kb.min(peer_kb);
            }
            if kb != self.effective_memory_kb[fleet] {
                changed = true;
            }
            new_effective.push(kb);
        }
        if !changed {
            return Ok(false);
        }

        let mut fleets: Vec<FleetProperties> = self.estimates.fleets().to_vec();
        for (fleet, &kb) in fleets.iter_mut().zip(&new_effective) {
            if let Some(bytes) = NonZeroBytes::new(Bytes::kib(kb)) {
                fleet.set_memory_capacity(bytes);
            }
        }
        self.current = FleetCatalog::try_new(fleets)?;
        self.effective_memory_kb = new_effective;
        self.revision = self.revision.saturating_add(1);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::{ActionResources, FleetActionPolicy, FleetName, InstanceId, Isa, Resources};

    use super::*;
    use crate::operations::Now;
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn fleet_props(name: &str, mem_gib: u64, ceiling_gib: Option<u64>) -> FleetProperties {
        let mut actions = FleetActionPolicy::default();
        actions.resources.memory = ceiling_gib.map(NonZeroBytes::gib);
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 16,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(100),
            },
            &actions,
            std::iter::empty(),
        )
    }

    fn two_fleets() -> FleetCatalog {
        FleetCatalog::try_new(vec![
            fleet_props("alpha", 63, Some(4)),
            fleet_props("beta", 32, None),
        ])
        .expect("catalog")
    }

    fn register_worker(
        reg: &mut WorkerRegistry,
        fleet: &str,
        instance: &str,
        seed: u8,
        mem: Bytes,
    ) -> crate::workers::WorkerKey {
        reg.register(
            now(1),
            "0.3.9",
            false,
            WorkerRegistration {
                instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
                fleet: FleetName::try_new(fleet).expect("fleet"),
                gang_version: "0.3.9".to_owned(),
                properties: BTreeMap::new(),
                capacity: ActionResources {
                    cpu_millicores: 32_000,
                    memory: mem,
                    disk: Bytes::gib(100),
                },
                ceilings: ActionResources {
                    cpu_millicores: 16_000,
                    memory: mem,
                    disk: Bytes::gib(100),
                },
                max_inflight: None,
                single_use: false,
                environment: None,
                uuid_bytes: [seed; 16],
            },
        )
        .expect("register")
    }

    #[test]
    fn local_and_peer_observations_lower_and_recover_within_estimate_keeping_ceilings() {
        let mut cap = CapacityCatalog::new(two_fleets(), NonZeroU32::new(2).expect("nonzero"));
        assert_eq!(cap.revision(), 0);
        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_memory_kb, 63 * 1024 * 1024);
        assert_eq!(alpha.memory_ceiling_kb, 4 * 1024 * 1024);
        assert!(alpha.memory_limited_by_policy());
        assert_eq!(alpha.ledger_cpu_millicores, 32_000);
        assert_eq!(alpha.cpu_ceiling_millicores, 16_000);

        let mut workers = WorkerRegistry::from_catalog(cap.estimates()).expect("workers");
        let w1 = register_worker(&mut workers, "alpha", "i-1", 1, Bytes::mib(62_229));
        assert!(cap.refresh_local(&workers));
        assert_eq!(cap.revision(), 1);
        assert!(!cap.refresh_local(&workers));
        assert_eq!(cap.revision(), 1);

        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_memory_kb, 62_229 * 1024);
        // Policy ceiling remains 4 GiB independent of observed physical memory.
        assert_eq!(alpha.memory_ceiling_kb, 4 * 1024 * 1024);

        // Peer lowers beta below its estimate; local_fleet_memory omits peer facts.
        assert!(
            cap.set_peer_observations([FleetMemoryCapacity {
                fleet: 1,
                capacity: NonZeroBytes::gib(24),
            }])
            .expect("peer obs")
        );
        assert_eq!(
            cap.fleet(1).expect("beta").physical_memory_kb,
            24 * 1024 * 1024
        );
        let mut local_only = Vec::new();
        cap.local_fleet_memory(&mut local_only);
        assert_eq!(local_only[1].capacity, NonZeroBytes::gib(32));

        // Disconnecting w1 restores alpha to its configured estimate.
        workers.disconnect(w1);
        assert!(cap.refresh_local(&workers));
        assert_eq!(
            cap.fleet(0).expect("alpha").physical_memory_kb,
            63 * 1024 * 1024
        );

        // Clearing peer facts restores beta; observations above estimate never exceed estimate.
        assert!(cap.clear_peer_observations());
        assert_eq!(
            cap.fleet(1).expect("beta").physical_memory_kb,
            32 * 1024 * 1024
        );
        assert!(
            !cap.set_peer_observations([FleetMemoryCapacity {
                fleet: 0,
                capacity: NonZeroBytes::gib(128),
            }])
            .expect("above estimate")
        );
        assert_eq!(
            cap.fleet(0).expect("alpha").physical_memory_kb,
            63 * 1024 * 1024
        );
    }

    #[test]
    fn rejects_unknown_fleet_and_sub_kib_observations() {
        let mut cap = CapacityCatalog::new(two_fleets(), NonZeroU32::new(2).expect("nonzero"));
        assert!(matches!(
            cap.set_local_observations([FleetMemoryCapacity {
                fleet: 9,
                capacity: NonZeroBytes::gib(1),
            }]),
            Err(CapacityError::UnknownFleet { fleet: 9, count: 2 })
        ));
        assert!(matches!(
            cap.set_peer_observations([FleetMemoryCapacity {
                fleet: 0,
                capacity: NonZeroBytes::new(Bytes::new(512)).expect("nonzero"),
            }]),
            Err(CapacityError::BelowOneKib { fleet: 0, .. })
        ));
    }
}
