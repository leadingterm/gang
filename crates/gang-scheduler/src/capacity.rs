//! Observed physical fleet capacity facts and effective per-action resource ceilings (`§5.3`).
//!
//! Configured fleet estimates in [`FleetCatalog`] are conservatively lowered by local connected
//! worker resource reports and optional peer-local facts, never rising above the configured
//! estimate and keeping configured per-action ceilings independent.

use gang_spi::FleetResourceCapacity;
use gang_types::Bytes;
use gang_types::FleetCatalog;
use gang_types::FleetCountError;
use gang_types::FleetProperties;
use gang_types::MAX_FLEETS;
use gang_types::Resources;
use gang_types::property;
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
    /// A capacity observation has no physical CPU or less than one KiB of disk.
    #[error("invalid CPU or disk capacity for fleet {fleet}")]
    InvalidResources {
        /// Fleet index of the observation.
        fleet: usize,
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
    /// Physical CPU ledger capacity in millicores.
    pub ledger_cpu_millicores: u64,
    /// Effective per-action CPU ceiling in millicores.
    pub cpu_ceiling_millicores: u64,
    /// Current observed post-reserve memory capacity in `KiB`.
    pub physical_memory_kb: u64,
    /// Effective per-action memory ceiling in `KiB`.
    pub memory_ceiling_kb: u64,
    /// Observed physical action disk capacity per instance in `KiB`.
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
/// physical resource capacity facts.
#[derive(Debug, Clone)]
pub struct CapacityCatalog {
    estimates: FleetCatalog,
    current: FleetCatalog,
    estimates_by_fleet: Vec<Resources>,
    local: Vec<Option<Resources>>,
    peer: Vec<Option<Resources>>,
    effective: Vec<Resources>,
    revision: u64,
}

impl CapacityCatalog {
    /// Creates a capacity catalog initialized with the configured prelaunch `estimates`.
    #[must_use]
    pub fn new(estimates: FleetCatalog) -> Self {
        let count = estimates.len();
        let estimates_by_fleet: Vec<Resources> =
            estimates.fleets().iter().map(physical_resources).collect();
        let effective = estimates_by_fleet.clone();
        Self {
            current: estimates.clone(),
            estimates,
            estimates_by_fleet,
            local: vec![None; count],
            peer: vec![None; count],
            effective,
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
        let ledger_cpu_millicores = physical_cpu_millicores;
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

    /// Recomputes physical capacity from connected workers, returning whether the catalog changed.
    pub fn refresh_local(&mut self, workers: &WorkerRegistry) -> bool {
        let mut observed = [None::<Resources>; MAX_FLEETS];
        let millicores_per_physical_vcpu = property::MILLICORES_PER_CORE;
        for (_, worker) in workers.workers_sorted() {
            if !worker.is_current_build() || worker.fleet >= self.estimates.len() {
                continue;
            }
            let capacity = worker.ledger.capacity();
            let instance_state = workers.instance_state(worker.fleet, &worker.instance);
            let cpu_millicores = instance_state
                .and_then(|state| state.cpu_capacity_millicores)
                .unwrap_or(capacity.cpu_millicores);
            let memory = instance_state
                .and_then(|state| state.memory_capacity)
                .unwrap_or(capacity.memory);
            let disk = instance_state
                .and_then(|state| state.disk_capacity)
                .unwrap_or(capacity.disk);
            let vcpus =
                u32::try_from(cpu_millicores / millicores_per_physical_vcpu).unwrap_or(u32::MAX);
            let actual = normalize(Resources {
                vcpus,
                memory,
                disk,
            });
            if actual.vcpus == 0 || actual.memory.get() < 1024 || actual.disk.get() < 1024 {
                continue;
            }
            let slot = &mut observed[worker.fleet];
            *slot = Some(slot.map_or(actual, |previous| minimum(previous, actual)));
        }
        if self.local.as_slice() == &observed[..self.local.len()] {
            return false;
        }
        let count = self.local.len();
        self.local.copy_from_slice(&observed[..count]);
        self.rebuild().unwrap_or(false)
    }

    /// Replaces local physical observations, combining duplicate fleets by dimension minimum.
    ///
    /// # Errors
    /// Returns an error for an unknown fleet or an invalid resource capacity.
    pub fn set_local_observations(
        &mut self,
        observations: impl IntoIterator<Item = FleetResourceCapacity>,
    ) -> Result<bool, CapacityError> {
        let next = self.collect_observations(observations)?;
        if self.local == next {
            return Ok(false);
        }
        self.local = next;
        self.rebuild()
    }

    /// Replaces peer physical observations, combining duplicate fleets by dimension minimum.
    ///
    /// # Errors
    /// Returns an error for an unknown fleet or an invalid resource capacity.
    pub fn set_peer_observations(
        &mut self,
        observations: impl IntoIterator<Item = FleetResourceCapacity>,
    ) -> Result<bool, CapacityError> {
        let next = self.collect_observations(observations)?;
        if self.peer == next {
            return Ok(false);
        }
        self.peer = next;
        self.rebuild()
    }

    /// Clears peer observations, returning whether the effective catalog changed.
    pub fn clear_peer_observations(&mut self) -> bool {
        if self.peer.iter().all(Option::is_none) {
            return false;
        }
        self.peer.fill(None);
        self.rebuild().unwrap_or(false)
    }

    /// Writes estimate-bounded local physical capacities, excluding peer observations.
    pub fn local_fleet_capacity(&self, into: &mut Vec<FleetResourceCapacity>) {
        into.clear();
        for (fleet, (&estimate, &local)) in
            self.estimates_by_fleet.iter().zip(&self.local).enumerate()
        {
            into.push(FleetResourceCapacity {
                fleet,
                capacity: local.map_or(estimate, |actual| minimum(estimate, actual)),
            });
        }
    }

    /// Writes estimate-bounded physical capacities including local and peer observations.
    pub fn fleet_capacity(&self, into: &mut Vec<FleetResourceCapacity>) {
        into.clear();
        into.extend(
            self.effective
                .iter()
                .copied()
                .enumerate()
                .map(|(fleet, capacity)| FleetResourceCapacity { fleet, capacity }),
        );
    }

    fn collect_observations(
        &self,
        observations: impl IntoIterator<Item = FleetResourceCapacity>,
    ) -> Result<Vec<Option<Resources>>, CapacityError> {
        let count = self.estimates.len();
        let mut next = vec![None::<Resources>; count];
        for obs in observations {
            if obs.fleet >= count {
                return Err(CapacityError::UnknownFleet {
                    fleet: obs.fleet,
                    count,
                });
            }
            if obs.capacity.memory.get() < 1024 {
                return Err(CapacityError::BelowOneKib {
                    fleet: obs.fleet,
                    bytes: obs.capacity.memory,
                });
            }
            if obs.capacity.vcpus == 0 || obs.capacity.disk.get() < 1024 {
                return Err(CapacityError::InvalidResources { fleet: obs.fleet });
            }
            let actual = normalize(obs.capacity);
            let slot = &mut next[obs.fleet];
            *slot = Some(slot.map_or(actual, |previous| minimum(previous, actual)));
        }
        Ok(next)
    }

    fn rebuild(&mut self) -> Result<bool, CapacityError> {
        let mut effective = Vec::with_capacity(self.estimates.len());
        for (index, &estimate) in self.estimates_by_fleet.iter().enumerate() {
            let local = self.local[index].map_or(estimate, |value| minimum(estimate, value));
            effective.push(self.peer[index].map_or(local, |value| minimum(local, value)));
        }
        if effective == self.effective {
            return Ok(false);
        }
        let mut fleets = self.estimates.fleets().to_vec();
        for (fleet, &capacity) in fleets.iter_mut().zip(&effective) {
            fleet.set_capacity(capacity);
        }
        self.current = FleetCatalog::try_new(fleets)?;
        self.effective = effective;
        self.revision = self.revision.saturating_add(1);
        Ok(true)
    }
}

fn physical_resources(fleet: &FleetProperties) -> Resources {
    Resources {
        vcpus: fleet
            .capacity(property::CPU_COUNT)
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0),
        memory: Bytes::new(
            fleet
                .capacity(property::MEMORY_KB)
                .unwrap_or(0)
                .saturating_mul(1024),
        ),
        disk: Bytes::new(
            fleet
                .capacity(property::DISK_KB)
                .unwrap_or(0)
                .saturating_mul(1024),
        ),
    }
}

fn normalize(mut capacity: Resources) -> Resources {
    capacity.memory = Bytes::new(capacity.memory.get() / 1024 * 1024);
    capacity.disk = Bytes::new(capacity.disk.get() / 1024 * 1024);
    capacity
}

fn minimum(left: Resources, right: Resources) -> Resources {
    Resources {
        vcpus: left.vcpus.min(right.vcpus),
        memory: left.memory.min(right.memory),
        disk: left.disk.min(right.disk),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::ActionResources;
    use gang_types::FleetActionPolicy;
    use gang_types::FleetName;
    use gang_types::InstanceId;
    use gang_types::Isa;
    use gang_types::NonZeroBytes;

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
                    cpu_millicores: 16_000,
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
        let mut cap = CapacityCatalog::new(two_fleets());
        assert_eq!(cap.revision(), 0);
        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_memory_kb, 63 * 1024 * 1024);
        assert_eq!(alpha.memory_ceiling_kb, 4 * 1024 * 1024);
        assert!(alpha.memory_limited_by_policy());
        assert_eq!(alpha.ledger_cpu_millicores, 16_000);
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

        // Peer lowers beta below its estimate; local_fleet_capacity omits peer facts.
        assert!(
            cap.set_peer_observations([FleetResourceCapacity {
                fleet: 1,
                capacity: Resources {
                    vcpus: 16,
                    memory: Bytes::gib(24),
                    disk: Bytes::gib(100)
                },
            }])
            .expect("peer obs")
        );
        assert_eq!(
            cap.fleet(1).expect("beta").physical_memory_kb,
            24 * 1024 * 1024
        );
        let mut local_only = Vec::new();
        cap.local_fleet_capacity(&mut local_only);
        assert_eq!(local_only[1].capacity.memory, Bytes::gib(32));

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
            !cap.set_peer_observations([FleetResourceCapacity {
                fleet: 0,
                capacity: Resources {
                    vcpus: 16,
                    memory: Bytes::gib(128),
                    disk: Bytes::gib(100)
                },
            }])
            .expect("above estimate")
        );
        assert_eq!(
            cap.fleet(0).expect("alpha").physical_memory_kb,
            63 * 1024 * 1024
        );
    }

    #[test]
    fn stale_generation_does_not_change_fleet_capacity() {
        let mut cap = CapacityCatalog::new(two_fleets());
        let mut workers = WorkerRegistry::from_catalog(cap.estimates()).expect("workers");
        register_worker(&mut workers, "alpha", "i-1", 1, Bytes::gib(40));
        assert!(cap.refresh_local(&workers));
        let mut before = Vec::new();
        cap.fleet_capacity(&mut before);
        workers
            .register(
                now(2),
                "0.3.9",
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new("i-1").expect("instance")),
                    fleet: FleetName::try_new("alpha").expect("fleet"),
                    gang_version: "legacy".to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 2_000,
                        memory: Bytes::mib(1),
                        disk: Bytes::mib(1),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 2_000,
                        memory: Bytes::mib(1),
                        disk: Bytes::mib(1),
                    },
                    max_inflight: None,
                    single_use: false,
                    environment: None,
                    uuid_bytes: [2; 16],
                },
            )
            .expect("stale registration");
        cap.refresh_local(&workers);
        let mut after = Vec::new();
        cap.fleet_capacity(&mut after);
        assert_eq!(before, after);
    }

    #[test]
    fn rejects_unknown_fleet_and_sub_kib_observations() {
        let mut cap = CapacityCatalog::new(two_fleets());
        assert!(matches!(
            cap.set_local_observations([FleetResourceCapacity {
                fleet: 9,
                capacity: Resources {
                    vcpus: 16,
                    memory: Bytes::gib(1),
                    disk: Bytes::gib(100)
                },
            }]),
            Err(CapacityError::UnknownFleet { fleet: 9, count: 2 })
        ));
        assert!(matches!(
            cap.set_peer_observations([FleetResourceCapacity {
                fleet: 0,
                capacity: Resources {
                    vcpus: 16,
                    memory: Bytes::new(512),
                    disk: Bytes::gib(100)
                },
            }]),
            Err(CapacityError::BelowOneKib { fleet: 0, .. })
        ));
    }
    #[test]
    fn all_dimensions_use_minimum_and_recover_after_peer_disconnect() {
        let mut catalog = CapacityCatalog::new(two_fleets());
        let entry = |vcpus, memory, disk| FleetResourceCapacity {
            fleet: 0,
            capacity: Resources {
                vcpus,
                memory: Bytes::gib(memory),
                disk: Bytes::gib(disk),
            },
        };
        assert!(
            catalog
                .set_local_observations([entry(12, 48, 80)])
                .expect("local")
        );
        assert!(
            catalog
                .set_peer_observations([entry(8, 60, 50), entry(10, 40, 70)])
                .expect("peer")
        );
        let mut observed = Vec::new();
        catalog.fleet_capacity(&mut observed);
        assert_eq!(
            observed[0].capacity,
            Resources {
                vcpus: 8,
                memory: Bytes::gib(40),
                disk: Bytes::gib(50)
            }
        );
        let current = catalog.fleet(0).expect("fleet");
        assert_eq!(
            (
                current.physical_vcpus,
                current.physical_memory_kb,
                current.physical_disk_kb
            ),
            (8, 40 * 1024 * 1024, 50 * 1024 * 1024)
        );
        assert_eq!(current.memory_ceiling_kb, 4 * 1024 * 1024);
        assert!(catalog.clear_peer_observations());
        catalog.fleet_capacity(&mut observed);
        assert_eq!(
            observed[0].capacity,
            Resources {
                vcpus: 12,
                memory: Bytes::gib(48),
                disk: Bytes::gib(80)
            }
        );
        assert!(catalog.set_local_observations([]).expect("clear local"));
        catalog.fleet_capacity(&mut observed);
        assert_eq!(
            observed[0].capacity,
            Resources {
                vcpus: 16,
                memory: Bytes::gib(63),
                disk: Bytes::gib(100)
            }
        );
    }

    #[test]
    fn refresh_local_derives_cpu_and_disk_from_registered_capacity_and_preserves_generation_order()
    {
        let mut cap = CapacityCatalog::new(two_fleets());
        let mut workers = WorkerRegistry::from_catalog(cap.estimates()).expect("workers");

        // Generation 1 registers 8 vCPUs, 40 GiB memory, and 50 GiB disk.
        let gen1 = workers
            .register(
                now(1),
                "0.3.9",
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new("i-1").expect("instance")),
                    fleet: FleetName::try_new("alpha").expect("fleet"),
                    gang_version: "0.3.9".to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 8_000,
                        memory: Bytes::gib(40),
                        disk: Bytes::gib(50),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 8_000,
                        memory: Bytes::gib(40),
                        disk: Bytes::gib(50),
                    },
                    max_inflight: None,
                    single_use: false,
                    environment: None,
                    uuid_bytes: [1; 16],
                },
            )
            .expect("gen1");
        assert!(cap.refresh_local(&workers));
        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_vcpus, 8);
        assert_eq!(alpha.physical_cpu_millicores, 8_000);
        assert_eq!(alpha.ledger_cpu_millicores, 8_000);
        assert_eq!(alpha.physical_memory_kb, 40 * 1024 * 1024);
        assert_eq!(alpha.physical_disk_kb, 50 * 1024 * 1024);

        // Overlapping newer generation 2 on the same instance supersedes gen 1's CPU, memory, and disk.
        let gen2 = workers
            .register(
                now(2),
                "0.3.9",
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new("i-1").expect("instance")),
                    fleet: FleetName::try_new("alpha").expect("fleet"),
                    gang_version: "0.3.9".to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 12_000,
                        memory: Bytes::gib(48),
                        disk: Bytes::gib(80),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 12_000,
                        memory: Bytes::gib(48),
                        disk: Bytes::gib(80),
                    },
                    max_inflight: None,
                    single_use: false,
                    environment: None,
                    uuid_bytes: [2; 16],
                },
            )
            .expect("gen2");
        assert!(cap.refresh_local(&workers));
        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_vcpus, 12);
        assert_eq!(alpha.physical_memory_kb, 48 * 1024 * 1024);
        assert_eq!(alpha.physical_disk_kb, 80 * 1024 * 1024);

        // Disconnecting both generations restores configured estimates.
        workers.disconnect(gen1);
        workers.disconnect(gen2);
        assert!(cap.refresh_local(&workers));
        let alpha = cap.fleet(0).expect("alpha");
        assert_eq!(alpha.physical_vcpus, 16);
        assert_eq!(alpha.physical_memory_kb, 63 * 1024 * 1024);
        assert_eq!(alpha.physical_disk_kb, 100 * 1024 * 1024);
    }
}
