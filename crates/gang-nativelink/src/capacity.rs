//! Cached fleet properties lowered by observed physical worker capacities.

use std::sync::Arc;

use gang_types::{Bytes, FleetCatalog, NonZeroBytes, property};
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
        for row in source.capacities.iter() {
            let Some(name) = row.exact.get(property::FLEET) else {
                continue;
            };
            let Some(fleet) = fleets.iter_mut().find(|fleet| fleet.name() == name) else {
                continue;
            };
            let observed = row
                .minimum
                .get(property::MEMORY_KB)
                .copied()
                .ok_or_else(|| {
                    make_err!(
                        Code::Internal,
                        "missing physical memory capacity for fleet {name}"
                    )
                })?;
            // Validate the provider value itself, even if our existing minimum is lower.
            observed
                .checked_mul(1024)
                .and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes)))
                .ok_or_else(|| {
                    make_err!(
                        Code::Internal,
                        "invalid physical memory capacity for fleet {name}: {observed} KiB"
                    )
                })?;
            let previous = fleet.capacity(property::MEMORY_KB).ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "missing cached physical memory capacity for fleet {name}"
                )
            })?;
            let bytes = previous
                .min(observed)
                .checked_mul(1024)
                .and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes)))
                .ok_or_else(|| {
                    make_err!(
                        Code::Internal,
                        "invalid cached physical memory capacity for fleet {name}"
                    )
                })?;
            fleet.set_memory_capacity(bytes);
        }
        let catalog = FleetCatalog::try_new(fleets)
            .map_err(|error| make_err!(Code::Internal, "rebuilding fleet catalog: {error}"))?;
        let catalog = Arc::new(catalog);
        *cache = (Some(source), Arc::clone(&catalog));
        Ok(catalog)
    }
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
    fn current_observations_restore_bounded_estimate_and_reuse_catalog() {
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
            Some(63 * 1024 * 1024)
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
        sender
            .send(snapshot(vec![
                row("alpha", 30 * 1024 * 1024),
                row("alpha", 40 * 1024 * 1024),
                row("unknown", 1),
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
    fn rejects_missing_zero_and_overflowing_matched_memory_without_caching() {
        let (sender, receiver) = watch::channel(snapshot(vec![]));
        let runtime = RuntimeCatalog::new(
            FleetCatalog::try_new(vec![fleet("alpha")]).unwrap(),
            receiver,
        );
        let mut missing = row("alpha", 1);
        missing.minimum.clear();
        for invalid in [missing, row("alpha", 0), row("alpha", u64::MAX)] {
            sender.send(snapshot(vec![invalid])).unwrap();
            assert!(matches!(runtime.current(), Err(error) if error.code == Code::Internal));
            assert!(matches!(runtime.current(), Err(error) if error.code == Code::Internal));
        }
        sender.send(snapshot(vec![row("alpha", 1024)])).unwrap();
        assert_eq!(
            runtime.current().unwrap().fleets()[0].capacity(property::MEMORY_KB),
            Some(1024)
        );
    }
}
