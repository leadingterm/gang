//! Cooperative controller leadership handoff state machine (`ADR-59`).
//!
//! When leadership yields to a prepared successor:
//! 1. Dispatch, new worker registration, and uncordon commands freeze immediately.
//! 2. All currently connected workers are marked `controller_draining`.
//! 3. [`ControllerHandOff::refresh_drain`] partitions connected workers into:
//!    - [`DrainingWorker`] entries in [`ControllerDrain::workers`] (unique and sorted by
//!      `(fleet, instance)` ascending, retaining the maximum active attempt deadline per physical
//!      instance), and
//!    - [`RehomeDirective`] entries for workers that are idle or whose latest attempt deadline has
//!      reached or passed `now`, so the controller shell can end their sessions with `Rehome`.

use gang_clock::Timestamp;
use gang_spi::{ControllerDrain, DrainingWorker};
use gang_types::WorkerId;
use thiserror::Error;

use crate::workers::{WorkerKey, WorkerRegistry};

/// Reason an outgoing controller closes a worker session with `Rehome` during handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RehomeReason {
    /// All controller-origin attempts on this worker have reached terminal completion.
    Idle,
    /// The latest dispatch-stamped deadline for this worker's remaining attempts has elapsed.
    DeadlineExpired,
}

/// Directive to terminate a connected worker's stream with `Rehome` during handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RehomeDirective {
    /// Connected worker handle in [`WorkerRegistry`].
    pub worker: WorkerKey,
    /// Why the outgoing controller is releasing this worker session.
    pub reason: RehomeReason,
}

/// Errors returned when computing a [`ControllerDrain`] snapshot during handoff.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandOffError {
    /// A worker reported active inflight attempts with no corresponding dispatch deadline.
    #[error("owned worker `{worker}` has inflight attempts without a dispatch deadline")]
    MissingAttemptDeadline {
        /// Identifier of the worker missing a dispatch deadline.
        worker: WorkerId,
    },
}

/// Combined directives emitted by [`ControllerHandOff::hand_off`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandOffEffects {
    /// Connected workers newly marked `controller_draining` when handoff started.
    pub drain_workers: Vec<WorkerKey>,
    /// Idle or deadline-expired connected workers to release with `Rehome`.
    pub rehome: Vec<RehomeDirective>,
}

/// Pure controller handoff coordinator (`ADR-59`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControllerHandOff {
    active: bool,
}

impl ControllerHandOff {
    /// Creates an inactive handoff coordinator.
    #[must_use]
    pub const fn new() -> Self {
        Self { active: false }
    }

    /// Returns `true` once cooperative handoff has begun.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Returns `true` while normal placement/dispatch is permitted (false once handoff begins).
    #[must_use]
    pub const fn allows_dispatch(&self) -> bool {
        !self.active
    }

    /// Returns `true` while new worker registrations are permitted (false once handoff begins).
    #[must_use]
    pub const fn allows_registration(&self) -> bool {
        !self.active
    }

    /// Returns `true` while clearing physical drains (`set_draining(..., false)`) is permitted.
    #[must_use]
    pub const fn allows_uncordon(&self) -> bool {
        !self.active
    }

    /// Freezes dispatch and registration and marks every connected worker `controller_draining`.
    ///
    /// Idempotent: returns the connected [`WorkerKey`]s (in ascending `WorkerId` order) on the
    /// initial transition into handoff, and an empty list on subsequent calls.
    pub fn begin(&mut self, workers: &mut WorkerRegistry) -> Vec<WorkerKey> {
        let keys: Vec<WorkerKey> = workers.workers_sorted().map(|(key, _)| key).collect();
        for &key in &keys {
            workers.set_controller_draining(key, true);
        }
        if self.active {
            Vec::new()
        } else {
            self.active = true;
            keys
        }
    }

    /// Begins handoff (if not already active) and populates `into` and `effects`.
    ///
    /// Reuses `effects.drain_workers`, `effects.rehome`, and `into.workers`.
    ///
    /// # Errors
    ///
    /// Returns [`HandOffError::MissingAttemptDeadline`] if a worker with `inflight_attempts > 0`
    /// produces `None` from `latest_attempt_deadline`.
    pub fn hand_off(
        &mut self,
        now: Timestamp,
        workers: &mut WorkerRegistry,
        public_complete: bool,
        latest_attempt_deadline: impl FnMut(WorkerKey) -> Option<Timestamp>,
        into: &mut ControllerDrain,
        effects: &mut HandOffEffects,
    ) -> Result<(), HandOffError> {
        effects.drain_workers = self.begin(workers);
        self.refresh_drain(
            now,
            workers,
            public_complete,
            latest_attempt_deadline,
            into,
            &mut effects.rehome,
        )
    }

    /// Refreshes `into` (`ControllerDrain`) and `rehome` from the current `workers` state without
    /// mutating `workers`.
    ///
    /// `latest_attempt_deadline` must return the latest dispatch-stamped deadline among all
    /// non-terminal attempts still owned by `WorkerKey` (including `Executed` attempts awaiting
    /// `Completed`), or `None` when the worker owns zero non-terminal attempts.
    ///
    /// # Errors
    ///
    /// Returns [`HandOffError::MissingAttemptDeadline`] if a worker has `inflight_attempts > 0`
    /// while `latest_attempt_deadline` returns `None`.
    pub fn refresh_drain(
        &self,
        now: Timestamp,
        workers: &WorkerRegistry,
        public_complete: bool,
        mut latest_attempt_deadline: impl FnMut(WorkerKey) -> Option<Timestamp>,
        into: &mut ControllerDrain,
        rehome: &mut Vec<RehomeDirective>,
    ) -> Result<(), HandOffError> {
        into.workers.clear();
        rehome.clear();

        let mut has_live_attempts = false;
        for (key, worker) in workers.workers_sorted() {
            let Some(deadline) = latest_attempt_deadline(key) else {
                if worker.inflight_attempts > 0 {
                    return Err(HandOffError::MissingAttemptDeadline {
                        worker: worker.id.clone(),
                    });
                }
                if self.active {
                    rehome.push(RehomeDirective {
                        worker: key,
                        reason: RehomeReason::Idle,
                    });
                }
                continue;
            };

            if deadline <= now {
                if self.active {
                    rehome.push(RehomeDirective {
                        worker: key,
                        reason: RehomeReason::DeadlineExpired,
                    });
                }
                continue;
            }

            has_live_attempts = true;
            into.workers.push(DrainingWorker {
                fleet: worker.fleet_name.clone(),
                instance: worker.instance.as_ref().clone(),
                deadline,
            });
        }

        into.workers.sort_unstable_by(|left, right| {
            (&left.fleet, &left.instance).cmp(&(&right.fleet, &right.instance))
        });
        into.workers.dedup_by(|later, earlier| {
            if later.fleet != earlier.fleet || later.instance != earlier.instance {
                return false;
            }
            earlier.deadline = earlier.deadline.max(later.deadline);
            true
        });

        into.complete = !has_live_attempts && public_complete;
        Ok(())
    }

    /// Returns `Some(RehomeDirective)` when handoff is active and `key` is connected with zero
    /// non-terminal attempts.
    #[must_use]
    pub fn check_idle_rehome(
        &self,
        key: WorkerKey,
        workers: &WorkerRegistry,
        has_non_terminal_attempts: bool,
    ) -> Option<RehomeDirective> {
        if !self.active || has_non_terminal_attempts {
            return None;
        }
        let worker = workers.get(key)?;
        (worker.inflight_attempts == 0).then_some(RehomeDirective {
            worker: key,
            reason: RehomeReason::Idle,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::Tick;
    use gang_types::{
        ActionResources, Bytes, FleetActionPolicy, FleetCatalog, FleetName, FleetProperties,
        InstanceId, Isa, Resources,
    };

    use super::*;
    use crate::operations::Now;
    use crate::workers::WorkerRegistration;

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
    ) -> WorkerKey {
        workers
            .register(
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

    fn assign_one(workers: &mut WorkerRegistry, key: WorkerKey) {
        workers
            .commit_assignment(
                key,
                ActionResources {
                    cpu_millicores: 1_000,
                    memory: Bytes::gib(1),
                    disk: Bytes::gib(1),
                },
                false,
            )
            .expect("assign");
    }

    #[test]
    fn handoff_freezes_admission_marks_controller_drain_and_builds_sorted_deduped_drain_set() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut handoff = ControllerHandOff::new();

        assert!(handoff.allows_dispatch());
        assert!(handoff.allows_registration());
        assert!(handoff.allows_uncordon());

        let beta_idle = register_worker(&mut workers, "beta", "i-idle", 1);
        let beta_gen1 = register_worker(&mut workers, "beta", "i-busy", 2);
        let beta_gen2 = register_worker(&mut workers, "beta", "i-busy", 3);
        let alpha_busy = register_worker(&mut workers, "alpha", "i-alpha", 4);
        let alpha_expired = register_worker(&mut workers, "alpha", "i-expired", 5);

        assign_one(&mut workers, beta_gen1);
        assign_one(&mut workers, beta_gen2);
        assign_one(&mut workers, alpha_busy);
        assign_one(&mut workers, alpha_expired);

        let deadlines = HashMap::from([
            (beta_gen1, Timestamp::from_unix_millis(50_000)),
            (beta_gen2, Timestamp::from_unix_millis(90_000)),
            (alpha_busy, Timestamp::from_unix_millis(60_000)),
            (alpha_expired, Timestamp::from_unix_millis(10_000)),
        ]);

        let mut drain = ControllerDrain::default();
        let mut effects = HandOffEffects::default();
        handoff
            .hand_off(
                Timestamp::from_unix_millis(10_000),
                &mut workers,
                true,
                |key| deadlines.get(&key).copied(),
                &mut drain,
                &mut effects,
            )
            .expect("handoff succeeds");

        assert!(handoff.is_active());
        assert!(!handoff.allows_dispatch());
        assert!(!handoff.allows_registration());
        assert!(!handoff.allows_uncordon());

        // Every connected worker is marked controller-draining.
        for key in [beta_idle, beta_gen1, beta_gen2, alpha_busy, alpha_expired] {
            let w = workers.get(key).expect("worker");
            assert!(w.drain.is_draining());
            assert!(!w.is_available_for_dispatch());
        }
        assert_eq!(effects.drain_workers.len(), 5);

        // Idle and expired workers receive Rehome directives.
        assert_eq!(effects.rehome.len(), 2);
        assert!(effects.rehome.contains(&RehomeDirective {
            worker: beta_idle,
            reason: RehomeReason::Idle,
        }));
        assert!(effects.rehome.contains(&RehomeDirective {
            worker: alpha_expired,
            reason: RehomeReason::DeadlineExpired,
        }));

        // ControllerDrain is sorted by `(fleet, instance)` ascending and dedups beta/i-busy by max deadline.
        assert!(!drain.complete);
        assert_eq!(
            drain.workers,
            vec![
                DrainingWorker {
                    fleet: FleetName::try_new("alpha").expect("alpha"),
                    instance: InstanceId::try_new("i-alpha").expect("i-alpha"),
                    deadline: Timestamp::from_unix_millis(60_000),
                },
                DrainingWorker {
                    fleet: FleetName::try_new("beta").expect("beta"),
                    instance: InstanceId::try_new("i-busy").expect("i-busy"),
                    deadline: Timestamp::from_unix_millis(90_000),
                },
            ]
        );

        // Re-calling hand_off is idempotent on `drain_workers`.
        handoff
            .hand_off(
                Timestamp::from_unix_millis(10_000),
                &mut workers,
                true,
                |key| deadlines.get(&key).copied(),
                &mut drain,
                &mut effects,
            )
            .expect("second handoff");
        assert!(effects.drain_workers.is_empty());
        assert_eq!(drain.workers.len(), 2);
    }

    #[test]
    fn complete_requires_both_no_live_attempts_and_public_streams_complete() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut handoff = ControllerHandOff::new();
        let busy = register_worker(&mut workers, "alpha", "i-1", 1);
        assign_one(&mut workers, busy);

        let mut deadlines = HashMap::from([(busy, Timestamp::from_unix_millis(30_000))]);
        let mut drain = ControllerDrain::default();
        let mut effects = HandOffEffects::default();

        handoff
            .hand_off(
                Timestamp::from_unix_millis(5_000),
                &mut workers,
                false,
                |key| deadlines.get(&key).copied(),
                &mut drain,
                &mut effects,
            )
            .expect("handoff");
        assert!(!drain.complete);

        // Attempt finishes (`Executed` -> `Completed`), but public streams are still open.
        workers
            .on_attempt_executed(
                busy,
                ActionResources {
                    cpu_millicores: 1_000,
                    memory: Bytes::gib(1),
                    disk: Bytes::gib(1),
                },
                false,
            )
            .expect("executed");
        deadlines.remove(&busy);

        assert_eq!(
            handoff.check_idle_rehome(busy, &workers, true),
            None,
            "Executed attempt awaiting Completed must not rehome early"
        );
        assert_eq!(
            handoff.check_idle_rehome(busy, &workers, false),
            Some(RehomeDirective {
                worker: busy,
                reason: RehomeReason::Idle,
            })
        );

        handoff
            .refresh_drain(
                Timestamp::from_unix_millis(6_000),
                &workers,
                false,
                |key| deadlines.get(&key).copied(),
                &mut drain,
                &mut effects.rehome,
            )
            .expect("refresh while public busy");
        assert!(drain.workers.is_empty());
        assert!(!drain.complete);

        // Once public streams finish as well, `complete` becomes true.
        handoff
            .refresh_drain(
                Timestamp::from_unix_millis(6_000),
                &workers,
                true,
                |key| deadlines.get(&key).copied(),
                &mut drain,
                &mut effects.rehome,
            )
            .expect("refresh complete");
        assert!(drain.complete);
    }

    #[test]
    fn missing_deadline_for_inflight_worker_errors_explicitly() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut handoff = ControllerHandOff::new();
        let w = register_worker(&mut workers, "alpha", "i-1", 1);
        assign_one(&mut workers, w);

        let mut drain = ControllerDrain::default();
        let mut effects = HandOffEffects::default();
        let err = handoff
            .hand_off(
                Timestamp::from_unix_millis(5_000),
                &mut workers,
                true,
                |_| None,
                &mut drain,
                &mut effects,
            )
            .expect_err("missing deadline must error");

        assert!(matches!(err, HandOffError::MissingAttemptDeadline { .. }));
    }
}
