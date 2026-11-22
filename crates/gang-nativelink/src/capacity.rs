//! Cached fleet properties updated from observed physical worker capacities.

use std::sync::Arc;

use gang_types::{Bytes, FleetCatalog, NonZeroBytes, Resources, property};
use nativelink_error::{Code, Error, make_err};
use nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot;
use parking_lot::RwLock;
use tokio::sync::watch;

/// Compiles a fleet catalog only when the scheduler publishes a new capacity snapshot.
pub(crate) struct RuntimeCatalog {
    receiver: watch::Receiver<Arc<ResourceCapacitySnapshot>>,
    estimates: Arc<FleetCatalog>,
    cache: RwLock<(Option<Arc<ResourceCapacitySnapshot>>, Arc<FleetCatalog>)>,
}

impl RuntimeCatalog {
    pub(crate) fn new(
        estimates: FleetCatalog,
        receiver: watch::Receiver<Arc<ResourceCapacitySnapshot>>,
    ) -> Self {
        let estimates = Arc::new(estimates);
        Self {
            receiver,
            cache: RwLock::new((None, Arc::clone(&estimates))),
            estimates,
        }
    }

    pub(crate) fn estimates(&self) -> &FleetCatalog {
        &self.estimates
    }

    pub(crate) fn current(&self) -> Result<Arc<FleetCatalog>, Error> {
        // Drop the watch guard before taking the cache lock: publishers must not be
        // blocked by a reader waiting for another catalog rebuild.
        let source = Arc::clone(&self.receiver.borrow());
        {
            let cache = self.cache.read();
            if cache
                .0
                .as_ref()
                .is_some_and(|cached| Arc::ptr_eq(&source, cached))
            {
                return Ok(Arc::clone(&cache.1));
            }
        }

        let mut cache = self.cache.write();
        // Another caller may have rebuilt while we waited. Read the watch again
        // under the write lock so an older observation cannot replace a newer one.
        let source = Arc::clone(&self.receiver.borrow());
        if cache
            .0
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(&source, cached))
        {
            return Ok(Arc::clone(&cache.1));
        }

        let mut fleets = self.estimates.fleets().to_vec();
        for fleet in &mut fleets {
            let mut minimum: Option<Resources> = None;
            for row in source.capacities.iter().filter(|row| {
                row.exact
                    .get(property::FLEET)
                    .is_some_and(|name| fleet.name() == name)
                    && row
                        .exact
                        .iter()
                        .all(|(key, value)| fleet.get(key) == Some(value.as_str()))
            }) {
                let read = |key| {
                    row.physical_capacity(key).ok_or_else(|| {
                        make_err!(
                            Code::Internal,
                            "missing {key} capacity for fleet {}",
                            fleet.name()
                        )
                    })
                };
                let cpu = read(property::CPU_MILLICORES)?;
                let bytes = |key| {
                    read(key)?
                        .checked_mul(1024)
                        .and_then(|value| NonZeroBytes::new(Bytes::new(value)))
                        .map(NonZeroBytes::get)
                        .ok_or_else(|| {
                            make_err!(
                                Code::Internal,
                                "invalid physical {key} capacity for fleet {}",
                                fleet.name()
                            )
                        })
                };
                let vcpus = u32::try_from(cpu / 1000)
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| {
                        make_err!(
                            Code::Internal,
                            "invalid physical CPU capacity for fleet {}: {cpu}",
                            fleet.name()
                        )
                    })?;
                let memory = bytes(property::MEMORY_KB)?;
                let disk = bytes(property::DISK_KB)?;
                minimum = Some(match minimum {
                    Some(previous) => Resources {
                        vcpus: previous.vcpus.min(vcpus),
                        memory: previous.memory.min(memory),
                        disk: previous.disk.min(disk),
                    },
                    None => Resources {
                        vcpus,
                        memory,
                        disk,
                    },
                });
            }
            if let Some(resources) = minimum {
                fleet.set_capacity(resources);
            }
        }
        let catalog = FleetCatalog::try_new(fleets)
            .map_err(|error| make_err!(Code::Internal, "rebuilding fleet catalog: {error}"))?;
        let catalog = Arc::new(catalog);
        *cache = (Some(source), Arc::clone(&catalog));
        Ok(catalog)
    }
}

#[cfg(test)]
pub(crate) fn test_catalog(fleets: FleetCatalog) -> Arc<RuntimeCatalog> {
    let (_sender, receiver) = watch::channel(Arc::new(ResourceCapacitySnapshot::new(Vec::new())));
    Arc::new(RuntimeCatalog::new(fleets, receiver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::{FleetActionPolicy, FleetName, FleetProperties, Isa, Resources};
    use nativelink_config::schedulers::ResourceCapacity;

    fn fleet(name: &str) -> FleetProperties {
        let mut actions = FleetActionPolicy::default();
        actions.resources.memory = Some(NonZeroBytes::gib(4));
        FleetProperties::new(
            &FleetName::try_new(name).unwrap(),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            Resources {
                vcpus: 16,
                memory: Bytes::gib(63),
                disk: Bytes::gib(100),
            },
            &actions,
            [("capacity", "spot".to_owned())],
        )
    }

    fn row(name: &str, memory_kb: u64) -> ResourceCapacity {
        let mut row = ResourceCapacity::default();
        row.exact
            .insert(property::FLEET.to_owned(), name.to_owned());
        row.minimum
            .insert(property::MEMORY_KB.to_owned(), memory_kb);
        row.minimum
            .insert(property::CPU_MILLICORES.to_owned(), 32_000);
        row.pool_multipliers.insert(
            property::CPU_MILLICORES.to_owned(),
            std::num::NonZeroU64::new(2).unwrap(),
        );
        row.minimum
            .insert(property::DISK_KB.to_owned(), 100 * 1024 * 1024);
        row
    }

    fn snapshot(rows: Vec<ResourceCapacity>) -> Arc<ResourceCapacitySnapshot> {
        Arc::new(ResourceCapacitySnapshot::new(rows))
    }

    #[test]
    fn applies_observation_already_present_at_construction() {
        let (_sender, receiver) = watch::channel(snapshot(vec![row("alpha", 62_229 * 1024)]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![fleet("alpha")]).unwrap(),
            receiver,
        );
        let catalog = runtime.current().unwrap();
        assert_eq!(
            catalog.fleets()[0].capacity(property::MEMORY_KB),
            Some(62_229 * 1024)
        );
        assert!(Arc::ptr_eq(&catalog, &runtime.current().unwrap()));
    }

    #[test]
    fn catalog_uses_row_physical_capacity_not_aggregate_or_action_ceiling() {
        let mut observed = row("alpha", 80 * 1024 * 1024);
        observed
            .minimum
            .insert(property::CPU_MILLICORES.to_owned(), 96_000);
        observed.pool_multipliers.insert(
            property::CPU_MILLICORES.to_owned(),
            std::num::NonZeroU64::new(3).unwrap(),
        );
        observed
            .action_ceilings
            .insert(property::CPU_MILLICORES.to_owned(), 16_000);
        observed
            .minimum
            .insert(property::DISK_KB.to_owned(), 120 * 1024 * 1024);
        let (_sender, receiver) = watch::channel(snapshot(vec![observed]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![fleet("alpha")]).unwrap(),
            receiver,
        );
        let catalog = runtime.current().unwrap();
        let actual = &catalog.fleets()[0];
        assert_eq!(actual.capacity(property::CPU_COUNT), Some(32));
        assert_eq!(actual.capacity(property::MEMORY_KB), Some(80 * 1024 * 1024));
        assert_eq!(actual.capacity(property::DISK_KB), Some(120 * 1024 * 1024));
    }

    #[test]
    fn current_observations_restore_actual_capacity_and_reuse_catalog() {
        let original = fleet("alpha");
        let (sender, receiver) = watch::channel(snapshot(vec![]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![original.clone()]).unwrap(),
            receiver,
        );
        let initial = runtime.current().unwrap();
        assert!(Arc::ptr_eq(&initial, &runtime.current().unwrap()));
        assert_eq!(
            initial.fleets()[0].capacity(property::MEMORY_KB),
            Some(63 * 1024 * 1024)
        );

        sender
            .send(snapshot(vec![row("alpha", 62_229 * 1024)]))
            .unwrap();
        let measured = runtime.current().unwrap();
        let actual = &measured.fleets()[0];
        assert_eq!(actual.capacity(property::MEMORY_KB), Some(62_229 * 1024));
        assert_eq!(actual.limit(property::MEMORY_KB), Some(4 * 1024 * 1024));
        assert_eq!(
            actual.capacity(property::CPU_COUNT),
            original.capacity(property::CPU_COUNT)
        );
        assert_eq!(
            actual.limit(property::CPU_MILLICORES),
            original.limit(property::CPU_MILLICORES)
        );
        assert_eq!(
            actual.capacity(property::DISK_KB),
            original.capacity(property::DISK_KB)
        );
        assert_eq!(actual.actions(), original.actions());
        assert_eq!(
            actual.options().collect::<Vec<_>>(),
            original.options().collect::<Vec<_>>()
        );
        assert!(Arc::ptr_eq(&measured, &runtime.current().unwrap()));

        sender
            .send(snapshot(vec![row("alpha", 62_500 * 1024)]))
            .unwrap();
        let raised = runtime.current().unwrap();
        assert_eq!(
            raised.fleets()[0].capacity(property::MEMORY_KB),
            Some(62_500 * 1024)
        );
        assert!(Arc::ptr_eq(&raised, &runtime.current().unwrap()));

        sender
            .send(snapshot(vec![row("alpha", 64 * 1024 * 1024)]))
            .unwrap();
        let later = runtime.current().unwrap();
        assert_eq!(
            later.fleets()[0].capacity(property::MEMORY_KB),
            Some(64 * 1024 * 1024)
        );
        assert_eq!(
            later.fleets()[0].limit(property::MEMORY_KB),
            Some(4 * 1024 * 1024)
        );
        assert!(Arc::ptr_eq(&later, &runtime.current().unwrap()));
    }

    #[test]
    fn matches_fleet_identity_and_combines_rows_by_minimum() {
        let (sender, receiver) = watch::channel(snapshot(vec![]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![fleet("alpha"), fleet("beta")]).unwrap(),
            receiver,
        );
        let mut unnamed = row("alpha", 1024);
        unnamed.exact.clear();
        let mut wrong_selector = row("alpha", 1024);
        wrong_selector
            .exact
            .insert("capacity".to_owned(), "on-demand".to_owned());
        sender
            .send(snapshot(vec![
                row("alpha", 30 * 1024 * 1024),
                row("alpha", 40 * 1024 * 1024),
                row("unknown", 1),
                wrong_selector,
                unnamed,
            ]))
            .unwrap();
        let catalog = runtime.current().unwrap();
        assert_eq!(
            catalog.fleets()[0].capacity(property::MEMORY_KB),
            Some(30 * 1024 * 1024)
        );
        assert_eq!(
            catalog.fleets()[1].capacity(property::MEMORY_KB),
            Some(63 * 1024 * 1024)
        );
        sender
            .send(snapshot(vec![row("beta", 50 * 1024 * 1024)]))
            .unwrap();
        let catalog = runtime.current().unwrap();
        assert_eq!(
            catalog.fleets()[0].capacity(property::MEMORY_KB),
            Some(63 * 1024 * 1024)
        );
        assert_eq!(
            catalog.fleets()[1].capacity(property::MEMORY_KB),
            Some(50 * 1024 * 1024)
        );
    }

    #[test]
    fn all_dimensions_follow_observed_minimum_and_recover() {
        let (sender, receiver) = watch::channel(snapshot(vec![]));
        let original = fleet("alpha");
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![original.clone()]).unwrap(),
            receiver,
        );
        for (ledger_key, catalog_key, estimate, lower, higher, highest, divisor) in [
            (
                property::CPU_MILLICORES,
                property::CPU_COUNT,
                16,
                8_000,
                40_000,
                48_000,
                2_000,
            ),
            (
                property::MEMORY_KB,
                property::MEMORY_KB,
                63 * 1024 * 1024,
                32 * 1024 * 1024,
                80 * 1024 * 1024,
                90 * 1024 * 1024,
                1,
            ),
            (
                property::DISK_KB,
                property::DISK_KB,
                100 * 1024 * 1024,
                50 * 1024 * 1024,
                120 * 1024 * 1024,
                140 * 1024 * 1024,
                1,
            ),
        ] {
            assert_eq!(
                runtime.current().unwrap().fleets()[0].capacity(catalog_key),
                Some(estimate)
            );
            let mut make_row = |value| {
                let mut result = row("alpha", 63 * 1024 * 1024);
                result.minimum.insert(ledger_key.to_owned(), value);
                result
            };
            for (values, expected) in [
                (vec![higher, highest], higher / divisor),
                (vec![higher, lower], lower / divisor),
                (vec![higher], higher / divisor),
            ] {
                sender
                    .send(snapshot(values.into_iter().map(&mut make_row).collect()))
                    .unwrap();
                let current = runtime.current().unwrap();
                let actual = &current.fleets()[0];
                assert_eq!(actual.capacity(catalog_key), Some(expected));
                assert_eq!(
                    actual.action_ceilings().collect::<Vec<_>>(),
                    original.action_ceilings().collect::<Vec<_>>()
                );
            }
            sender.send(snapshot(vec![])).unwrap();
            assert_eq!(
                runtime.current().unwrap().fleets()[0].capacity(catalog_key),
                Some(estimate)
            );
        }
    }

    #[test]
    fn rejects_invalid_matched_capacities_without_caching() {
        let (sender, receiver) = watch::channel(snapshot(vec![]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![fleet("alpha")]).unwrap(),
            receiver,
        );
        for key in [
            property::CPU_MILLICORES,
            property::MEMORY_KB,
            property::DISK_KB,
        ] {
            for value in [None, Some(0), Some(u64::MAX)] {
                let mut invalid = row("alpha", 1024);
                if let Some(value) = value {
                    invalid.minimum.insert(key.to_owned(), value);
                } else {
                    invalid.minimum.remove(key);
                }
                sender.send(snapshot(vec![invalid])).unwrap();
                assert!(matches!(runtime.current(), Err(error) if error.code == Code::Internal));
                assert!(matches!(runtime.current(), Err(error) if error.code == Code::Internal));
            }
        }
        sender.send(snapshot(vec![row("alpha", 1024)])).unwrap();
        assert_eq!(
            runtime.current().unwrap().fleets()[0].capacity(property::MEMORY_KB),
            Some(1024)
        );
    }
}
