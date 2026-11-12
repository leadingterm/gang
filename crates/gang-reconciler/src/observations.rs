//! In-process memory of when each worker and cloud instance was first seen in
//! its current state.
//!
//! Nothing here persists across restarts; a restarted reconciler begins every
//! timer from zero.

use std::collections::HashMap;
use std::hash::Hash;

use gang_clock::Tick;
use gang_spi::WorkerState;
use gang_types::{InstanceId, WorkerId};

use crate::activity::Activity;

/// What a worker is doing and since when: the tick at which it first showed its current activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Seen {
    /// What the worker is doing.
    pub(crate) activity: Activity,
    /// The tick at which the worker first showed `activity`.
    pub(crate) since: Tick,
}

/// What is known about one cloud instance across passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct InstanceObservation {
    /// When first seen in service without a worker, or `None` while served or not in service.
    pub(crate) unserved_since: Option<Tick>,
    /// When termination of the instance was last ordered, if it has been.
    pub(crate) ordered_at: Option<Tick>,
}

/// A table whose values are each tagged with the pass that last touched them; the entries a pass did not touch are swept at its end.
#[derive(Debug)]
struct Ledger<K, V> {
    entries: HashMap<K, (V, u64)>,
}

impl<K, V> Default for Ledger<K, V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq + Clone, V> Ledger<K, V> {
    /// Runs `update` on the value of `key`, inserting `init()` first when it is absent, and tags the entry with `epoch`. The key is cloned only on insert.
    fn touch<R>(
        &mut self,
        key: &K,
        epoch: u64,
        init: impl FnOnce() -> V,
        update: impl FnOnce(&mut V) -> R,
    ) -> R {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.1 = epoch;
            return update(&mut entry.0);
        }
        let mut value = init();
        let result = update(&mut value);
        self.entries.insert(key.clone(), (value, epoch));
        result
    }

    /// The value of `key`, without touching its tag.
    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries.get_mut(key).map(|entry| &mut entry.0)
    }

    /// Drops every entry not tagged with `epoch`.
    fn sweep(&mut self, epoch: u64) {
        self.entries.retain(|_, (_, tag)| *tag == epoch);
    }
}

/// The stored state of every worker and instance, each tagged with the pass that last observed it.
#[derive(Debug)]
pub(crate) struct Observations {
    epoch: u64,
    workers: Ledger<WorkerId, Seen>,
    instances: Vec<Ledger<InstanceId, InstanceObservation>>,
}

impl Observations {
    /// Creates an observation ledger for each fleet.
    pub(crate) fn new(fleets: usize) -> Self {
        Self {
            epoch: 0,
            workers: Ledger::default(),
            instances: (0..fleets).map(|_| Ledger::default()).collect(),
        }
    }

    /// Starts a new pass; every worker and instance must be observed again before the pass ends.
    pub(crate) fn begin_pass(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Records the activity of `worker` at `now` and returns it.
    ///
    /// A worker keeps the tick at which it first showed its current activity.
    pub(crate) fn observe_worker(&mut self, worker: &WorkerState, now: Tick) -> Seen {
        let activity = Activity::from(worker);
        self.workers.touch(
            &worker.id,
            self.epoch,
            || Seen {
                activity,
                since: now,
            },
            |seen| {
                if seen.activity != activity {
                    *seen = Seen {
                        activity,
                        since: now,
                    };
                }
                *seen
            },
        )
    }

    /// Records whether the instance is in service without a worker at `now`.
    ///
    /// An unserved instance keeps the tick at which it was first seen unserved.
    pub(crate) fn observe_instance(
        &mut self,
        fleet: usize,
        id: &InstanceId,
        unserved: bool,
        now: Tick,
    ) -> InstanceObservation {
        self.instances[fleet].touch(id, self.epoch, InstanceObservation::default, |held| {
            held.unserved_since = if unserved {
                held.unserved_since.or(Some(now))
            } else {
                None
            };
            *held
        })
    }

    /// Records that termination of the instance was ordered at `now`.
    pub(crate) fn order_termination(&mut self, fleet: usize, id: &InstanceId, now: Tick) {
        self.instances[fleet].touch(id, self.epoch, InstanceObservation::default, |held| {
            held.ordered_at = Some(now);
        });
    }

    /// Clears the termination ordered for `id`, so it is ordered again on the next pass instead
    /// of waiting out `boot_grace`. Used when the caller could not apply the order, so the
    /// instance is not still in flight.
    ///
    /// An instance with no entry has no outstanding order: nothing is created and no epoch is
    /// touched.
    pub(crate) fn forget_order(&mut self, fleet: usize, id: &InstanceId) {
        if let Some(held) = self
            .instances
            .get_mut(fleet)
            .and_then(|ledger| ledger.get_mut(id))
        {
            held.ordered_at = None;
        }
    }

    /// Ends a pass, forgetting every worker and instance that was not observed during it.
    pub(crate) fn finish_pass(&mut self) {
        self.workers.sweep(self.epoch);
        for instances in &mut self.instances {
            instances.sweep(self.epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use std::num::NonZeroU32;

    use gang_clock::{Tick, Timestamp};
    use gang_spi::WorkerState;
    use gang_testkit::{TestResult, worker_id};
    use gang_types::{FleetName, InstanceId};

    use super::{Observations, Seen};
    use crate::activity::Activity;

    fn at(secs: u64) -> Tick {
        Tick::from_elapsed(Duration::from_secs(secs))
    }

    fn instance() -> TestResult<InstanceId> {
        Ok(InstanceId::try_new("i-0abc123")?)
    }

    fn worker(running_actions: u32, draining: bool) -> TestResult<WorkerState> {
        Ok(WorkerState {
            id: worker_id("i-0abc123")?,
            fleet: FleetName::try_new("aarch64-fleet")?,
            running_actions,
            max_inflight_actions: NonZeroU32::new(4),
            paused: false,
            draining,
            last_seen: Timestamp::default(),
        })
    }

    #[test]
    fn idle_and_draining_keep_their_first_tick_across_passes() -> TestResult {
        let mut observations = Observations::new(1);

        observations.begin_pass();
        let first = observations.observe_worker(&worker(0, false)?, at(1));
        observations.finish_pass();
        assert_eq!(
            first,
            Seen {
                activity: Activity::Idle,
                since: at(1)
            }
        );

        observations.begin_pass();
        let second = observations.observe_worker(&worker(0, false)?, at(5));
        observations.finish_pass();
        assert_eq!(
            second,
            Seen {
                activity: Activity::Idle,
                since: at(1)
            }
        );

        observations.begin_pass();
        let draining = observations.observe_worker(&worker(0, true)?, at(6));
        observations.finish_pass();
        assert_eq!(
            draining,
            Seen {
                activity: Activity::Draining,
                since: at(6)
            }
        );

        observations.begin_pass();
        let still_draining = observations.observe_worker(&worker(0, true)?, at(9));
        observations.finish_pass();
        assert_eq!(
            still_draining,
            Seen {
                activity: Activity::Draining,
                since: at(6)
            }
        );
        Ok(())
    }

    #[test]
    fn busy_clears_idle_and_later_idle_restarts() -> TestResult {
        let mut observations = Observations::new(1);

        observations.begin_pass();
        observations.observe_worker(&worker(0, false)?, at(1));
        observations.finish_pass();

        observations.begin_pass();
        let busy = observations.observe_worker(&worker(2, false)?, at(2));
        observations.finish_pass();
        assert_eq!(busy.activity, Activity::Busy);

        observations.begin_pass();
        let idle_again = observations.observe_worker(&worker(0, false)?, at(3));
        observations.finish_pass();
        assert_eq!(
            idle_again,
            Seen {
                activity: Activity::Idle,
                since: at(3)
            }
        );
        Ok(())
    }

    #[test]
    fn draining_supersedes_idle_and_busy() -> TestResult {
        let mut observations = Observations::new(1);

        observations.begin_pass();
        let idle = observations.observe_worker(&worker(0, false)?, at(1));
        observations.finish_pass();
        assert_eq!(
            idle,
            Seen {
                activity: Activity::Idle,
                since: at(1)
            }
        );

        observations.begin_pass();
        let draining = observations.observe_worker(&worker(0, true)?, at(2));
        observations.finish_pass();
        assert_eq!(
            draining,
            Seen {
                activity: Activity::Draining,
                since: at(2)
            }
        );

        observations.begin_pass();
        let draining_busy = observations.observe_worker(&worker(2, true)?, at(3));
        observations.finish_pass();
        assert_eq!(
            draining_busy,
            Seen {
                activity: Activity::Draining,
                since: at(2)
            }
        );
        Ok(())
    }

    #[test]
    fn unserved_instance_keeps_first_tick_until_served() -> TestResult {
        let mut observations = Observations::new(1);
        let id = instance()?;

        observations.begin_pass();
        let first = observations.observe_instance(0, &id, true, at(1));
        observations.finish_pass();
        assert_eq!(first.unserved_since, Some(at(1)));

        observations.begin_pass();
        let second = observations.observe_instance(0, &id, true, at(4));
        observations.finish_pass();
        assert_eq!(second.unserved_since, Some(at(1)));

        observations.begin_pass();
        let connected = observations.observe_instance(0, &id, false, at(5));
        observations.finish_pass();
        assert_eq!(connected.unserved_since, None);

        observations.begin_pass();
        let unconnected_again = observations.observe_instance(0, &id, true, at(7));
        observations.finish_pass();
        assert_eq!(unconnected_again.unserved_since, Some(at(7)));
        Ok(())
    }

    #[test]
    fn ordered_at_survives_passes_while_instance_is_listed() -> TestResult {
        let mut observations = Observations::new(1);
        let id = instance()?;

        observations.begin_pass();
        observations.observe_instance(0, &id, true, at(1));
        observations.order_termination(0, &id, at(2));
        observations.finish_pass();

        observations.begin_pass();
        let seen = observations.observe_instance(0, &id, true, at(3));
        observations.finish_pass();
        assert_eq!(seen.ordered_at, Some(at(2)));
        assert_eq!(seen.unserved_since, Some(at(1)));
        Ok(())
    }

    #[test]
    fn shared_ids_have_independent_orders_and_sweeps() -> TestResult {
        let mut observations = Observations::new(2);
        let id = instance()?;
        observations.begin_pass();
        observations.observe_instance(0, &id, true, at(1));
        observations.observe_instance(1, &id, true, at(2));
        observations.order_termination(0, &id, at(3));
        observations.order_termination(1, &id, at(4));
        observations.forget_order(0, &id);
        observations.finish_pass();

        observations.begin_pass();
        let first = observations.observe_instance(0, &id, true, at(5));
        let second = observations.observe_instance(1, &id, true, at(5));
        assert_eq!(first.ordered_at, None);
        assert_eq!(second.ordered_at, Some(at(4)));
        assert_eq!(first.unserved_since, Some(at(1)));
        assert_eq!(second.unserved_since, Some(at(2)));
        observations.finish_pass();

        observations.begin_pass();
        observations.observe_instance(0, &id, true, at(6));
        observations.finish_pass();
        observations.begin_pass();
        let second = observations.observe_instance(1, &id, true, at(7));
        assert_eq!(second.unserved_since, Some(at(7)));
        assert_eq!(second.ordered_at, None);
        Ok(())
    }

    #[test]
    fn unobserved_entries_are_forgotten_and_timers_restart() -> TestResult {
        let mut observations = Observations::new(1);
        let id = instance()?;

        observations.begin_pass();
        observations.observe_worker(&worker(0, false)?, at(1));
        observations.observe_instance(0, &id, true, at(1));
        observations.order_termination(0, &id, at(1));
        observations.finish_pass();

        observations.begin_pass();
        observations.finish_pass();

        observations.begin_pass();
        let seen_worker = observations.observe_worker(&worker(0, false)?, at(10));
        let seen = observations.observe_instance(0, &id, true, at(10));
        observations.finish_pass();
        assert_eq!(
            seen_worker,
            Seen {
                activity: Activity::Idle,
                since: at(10)
            }
        );
        assert_eq!(seen.unserved_since, Some(at(10)));
        assert_eq!(seen.ordered_at, None);
        Ok(())
    }
}
