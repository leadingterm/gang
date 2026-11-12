//! Which worker runs on which instance: the rule the reconciler and state builder share.

use gang_spi::{SchedulerSnapshot, WorkerState};
use gang_types::{FleetName, InstanceId};

/// A scheduler snapshot with its workers indexed by the instance that hosts each. The
/// snapshot can only be replaced through [`HostedSnapshot::replace`], which re-indexes it, so
/// the index always answers for the snapshot it is stored with.
#[derive(Debug, Default)]
pub struct HostedSnapshot {
    snapshot: SchedulerSnapshot,
    by_instance: Vec<usize>,
}

impl HostedSnapshot {
    /// Swaps `snapshot` in and re-indexes its workers; `snapshot` receives the previous one, so
    /// both buffers keep their capacity. A worker whose id names no instance is not indexed.
    pub fn replace(&mut self, snapshot: &mut SchedulerSnapshot) {
        std::mem::swap(&mut self.snapshot, snapshot);
        let workers = &self.snapshot.workers;
        self.by_instance.clear();
        self.by_instance
            .extend((0..workers.len()).filter(|&i| workers[i].id.instance_name().is_some()));
        self.by_instance.sort_unstable_by(|&a, &b| {
            key(&workers[a])
                .cmp(&key(&workers[b]))
                .then_with(|| workers[a].id.cmp(&workers[b].id))
        });
    }

    /// The snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &SchedulerSnapshot {
        &self.snapshot
    }

    /// The indices into `snapshot().workers` of the workers of `fleet` that run on the instance
    /// `id`, in worker id order. A worker registered in another fleet does not host the
    /// instance. Allocates nothing.
    pub fn workers_on<'a>(
        &'a self,
        fleet: &'a FleetName,
        id: &'a InstanceId,
    ) -> impl Iterator<Item = usize> + 'a {
        let probe = (Some(id.as_str()), fleet);
        let workers = &self.snapshot.workers;
        let start = self
            .by_instance
            .partition_point(|&i| key(&workers[i]) < probe);
        self.by_instance
            .iter()
            .skip(start)
            .copied()
            .take_while(move |&i| key(&workers[i]) == probe)
    }
}

/// The sort key of `worker`: its instance name, then its fleet.
fn key(worker: &WorkerState) -> (Option<&str>, &FleetName) {
    (worker.id.instance_name(), &worker.fleet)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::num::NonZeroU32;

    use gang_clock::Timestamp;
    use gang_spi::{SchedulerSnapshot, WorkerState};
    use gang_testkit::worker_id;
    use gang_types::{FleetName, InstanceId, WorkerId};

    use super::HostedSnapshot;

    /// An additional worker on `instance`, sorting after the shared test worker.
    fn renewed_worker(instance: &InstanceId, sequence: u8) -> Result<WorkerId, Box<dyn Error>> {
        Ok(WorkerId::try_new(format!(
            "{}6c5e4f3a-9b1d-4e2a-8f7c-00000000000{sequence}",
            instance.worker_name()
        ))?)
    }

    fn worker(id: WorkerId, fleet: &str) -> Result<WorkerState, Box<dyn Error>> {
        Ok(WorkerState {
            id,
            fleet: FleetName::try_new(fleet)?,
            running_actions: 0,
            max_inflight_actions: NonZeroU32::new(4),
            draining: false,
            paused: false,
            last_seen: Timestamp::default(),
        })
    }

    fn hosted(workers: Vec<WorkerState>) -> HostedSnapshot {
        let mut snapshot = SchedulerSnapshot {
            workers,
            ..SchedulerSnapshot::default()
        };
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        hosted
    }

    #[test]
    fn prefix_ids_find_their_own_worker() -> Result<(), Box<dyn Error>> {
        let hosted = hosted(vec![
            worker(worker_id("local-1")?, "x86-fleet")?,
            worker(worker_id("local-10")?, "x86-fleet")?,
        ]);
        let fleet = FleetName::try_new("x86-fleet")?;
        assert_eq!(
            hosted
                .workers_on(&fleet, &InstanceId::try_new("local-1")?)
                .next(),
            Some(0)
        );
        assert_eq!(
            hosted
                .workers_on(&fleet, &InstanceId::try_new("local-10")?)
                .next(),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn only_the_worker_of_the_probed_fleet_hosts_the_instance() -> Result<(), Box<dyn Error>> {
        let hosted = hosted(vec![worker(worker_id("i-0abc123")?, "b-fleet")?]);
        let instance = InstanceId::try_new("i-0abc123")?;
        assert_eq!(
            hosted
                .workers_on(&FleetName::try_new("b-fleet")?, &instance)
                .next(),
            Some(0)
        );
        assert_eq!(
            hosted
                .workers_on(&FleetName::try_new("a-fleet")?, &instance)
                .next(),
            None
        );
        Ok(())
    }

    #[test]
    fn worker_without_an_instance_is_not_indexed() -> Result<(), Box<dyn Error>> {
        let hosted = hosted(vec![worker(
            WorkerId::try_new("not-an-instance-id")?,
            "x86-fleet",
        )?]);
        assert_eq!(
            hosted
                .workers_on(
                    &FleetName::try_new("x86-fleet")?,
                    &InstanceId::try_new("not-an-instance-id")?,
                )
                .next(),
            None
        );
        Ok(())
    }

    #[test]
    fn workers_on_lists_every_worker_of_the_fleet_on_the_instance() -> Result<(), Box<dyn Error>> {
        let hosted = hosted(vec![
            worker(worker_id("i-0abc123")?, "x86-fleet")?,
            worker(
                renewed_worker(&InstanceId::try_new("i-0abc123")?, 2)?,
                "x86-fleet",
            )?,
            worker(
                renewed_worker(&InstanceId::try_new("i-0abc123")?, 3)?,
                "arm-fleet",
            )?,
            worker(worker_id("i-0abc124")?, "x86-fleet")?,
        ]);
        let fleet = FleetName::try_new("x86-fleet")?;
        let instance = InstanceId::try_new("i-0abc123")?;
        let on: Vec<usize> = hosted.workers_on(&fleet, &instance).collect();
        assert_eq!(on, [0, 1]);
        Ok(())
    }

    #[test]
    fn replace_swaps_and_reindexes() -> Result<(), Box<dyn Error>> {
        let mut hosted = hosted(vec![worker(worker_id("local-1")?, "x86-fleet")?]);
        let mut incoming = SchedulerSnapshot {
            workers: vec![worker(worker_id("local-2")?, "x86-fleet")?],
            ..SchedulerSnapshot::default()
        };
        hosted.replace(&mut incoming);
        let fleet = FleetName::try_new("x86-fleet")?;
        assert_eq!(incoming.workers[0].id.instance_name(), Some("local-1"));
        assert_eq!(
            hosted
                .workers_on(&fleet, &InstanceId::try_new("local-1")?)
                .next(),
            None
        );
        assert_eq!(
            hosted
                .workers_on(&fleet, &InstanceId::try_new("local-2")?)
                .next(),
            Some(0)
        );
        Ok(())
    }
}
