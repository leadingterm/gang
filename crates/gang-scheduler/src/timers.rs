//! Deterministic deadline queue and `on_timer(now)` dispatcher (`§5.2`, `§5.5`).
//!
//! Tracks dispatch ack deadlines, dispatch-stamped attempt deadlines, worker keepalive timeouts,
//! and client-less operation expirations using monotonic [`Tick`]s without reading any OS clock.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Tick;

use crate::attempts::{
    AckTimeoutOutcome, AttemptDeadlineOutcome, Attempts, KillDirective, PublishEvent,
};
use crate::operations::{Now, OperationKey, OperationStage, Operations};
use crate::queue::OperationQueue;
use crate::workers::{WorkerKey, WorkerRegistry};

/// Category of scheduled scheduler deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimerKind {
    /// Worker failed to send `Accept` or `Decline` before `dispatch_ack_timeout`.
    AckTimeout,
    /// Execution or post-`Executed` completion deadline elapsed.
    AttemptDeadline,
    /// Worker exceeded `worker_timeout` without a `KeepAlive` or message.
    WorkerTimeout,
    /// Non-terminal operation had zero watchers for `client_action_timeout`.
    ClientLessTimeout,
}

/// Typed key identifying the entity armed in [`TimerQueue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimerSubject {
    /// Dispatch ack deadline for an attempt.
    Ack(crate::attempts::AttemptKey),
    /// Execution or completion deadline for an attempt.
    AttemptDeadline(crate::attempts::AttemptKey),
    /// Silence/keepalive timeout for a connected worker.
    Worker(WorkerKey),
    /// Client-less expiry for a zero-watcher operation.
    ClientLess(OperationKey),
}

impl TimerSubject {
    const fn kind(self) -> TimerKind {
        match self {
            Self::Ack(_) => TimerKind::AckTimeout,
            Self::AttemptDeadline(_) => TimerKind::AttemptDeadline,
            Self::Worker(_) => TimerKind::WorkerTimeout,
            Self::ClientLess(_) => TimerKind::ClientLessTimeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TimerEntry {
    deadline: Tick,
    subject: TimerSubject,
    seq: u64,
}

/// A deadline that reached or passed `now` in [`TimerQueue::drain_due`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DueTimer {
    /// Monotonic tick at which the timer was scheduled to expire.
    pub deadline: Tick,
    /// Target entity and timer category.
    pub subject: TimerSubject,
}

/// Ordered, generation-safe deadline queue supporting O(1) `next_deadline()` queries (`§5.2`).
#[derive(Debug, Clone, Default)]
pub struct TimerQueue {
    ordered: BTreeSet<TimerEntry>,
    active: HashMap<TimerSubject, (Tick, u64)>,
    next_seq: u64,
}

impl TimerQueue {
    /// Creates an empty timer queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of armed timers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.active.len()
    }

    /// Returns `true` when no timers are armed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Returns the earliest armed deadline, or `None` when empty.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Tick> {
        self.ordered.first().map(|entry| entry.deadline)
    }

    /// Arms or replaces the timer for `subject` at `deadline`.
    pub fn arm(&mut self, subject: TimerSubject, deadline: Tick) {
        if let Some((old_deadline, old_seq)) = self.active.remove(&subject) {
            self.ordered.remove(&TimerEntry {
                deadline: old_deadline,
                subject,
                seq: old_seq,
            });
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.active.insert(subject, (deadline, seq));
        self.ordered.insert(TimerEntry {
            deadline,
            subject,
            seq,
        });
    }

    /// Disarms any active timer for `subject`, returning `true` if one was armed.
    pub fn disarm(&mut self, subject: TimerSubject) -> bool {
        let Some((deadline, seq)) = self.active.remove(&subject) else {
            return false;
        };
        self.ordered.remove(&TimerEntry {
            deadline,
            subject,
            seq,
        });
        true
    }

    /// Arms both `Ack` and `AttemptDeadline` timers for a newly dispatched attempt.
    pub fn arm_attempt(
        &mut self,
        attempt_key: crate::attempts::AttemptKey,
        ack_deadline: Tick,
        attempt_deadline: Tick,
    ) {
        self.arm(TimerSubject::Ack(attempt_key), ack_deadline);
        self.arm(TimerSubject::AttemptDeadline(attempt_key), attempt_deadline);
    }

    /// Disarms both `Ack` and `AttemptDeadline` timers for `attempt_key`.
    pub fn disarm_attempt(&mut self, attempt_key: crate::attempts::AttemptKey) {
        self.disarm(TimerSubject::Ack(attempt_key));
        self.disarm(TimerSubject::AttemptDeadline(attempt_key));
    }

    /// Pops the earliest timer whose deadline is `<= now`, or `None` when none are due.
    pub fn pop_due(&mut self, now: Tick) -> Option<DueTimer> {
        let first = *self.ordered.first()?;
        if first.deadline > now {
            return None;
        }
        self.ordered.remove(&first);
        self.active.remove(&first.subject);
        Some(DueTimer {
            deadline: first.deadline,
            subject: first.subject,
        })
    }

    /// Drains all timers with `deadline <= now` in chronological order into `out` (clearing `out`
    /// first).
    pub fn drain_due(&mut self, now: Tick, out: &mut Vec<DueTimer>) {
        out.clear();
        while let Some(due) = self.pop_due(now) {
            out.push(due);
        }
    }

    /// Returns the kind of the earliest armed timer, if any (primarily for diagnostics/tests).
    #[must_use]
    pub fn next_kind(&self) -> Option<TimerKind> {
        self.ordered.first().map(|entry| entry.subject.kind())
    }
}

/// Side effects produced by [`on_timer`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerEffects {
    /// Worker kills to transmit (`AckTimeout`, `DeadlineExceeded`, or `Cancelled`).
    pub kills: Vec<KillDirective>,
    /// Workers disconnected due to `worker_timeout`.
    pub timed_out_workers: Vec<WorkerKey>,
    /// Operations requeued due to ack timeout or worker timeout.
    pub requeued_operations: Vec<OperationKey>,
    /// Queued operations dropped due to `client_action_timeout`.
    pub dropped_operations: Vec<OperationKey>,
    /// Terminal or stage-transition events to publish to operation watchers.
    pub publish: Vec<PublishEvent>,
}

/// Fires all timers in `timers` due at `now`, updating `attempts`, `operations`, `queue`, and
/// `workers` deterministically (`§5.5`, `§5.10`).
#[allow(
    clippy::too_many_arguments,
    reason = "pure timer sweep explicitly receives all affected scheduler state tables"
)]
pub fn on_timer(
    now: Now,
    max_worker_loss_retries: NonZeroU32,
    client_action_timeout: Duration,
    timers: &mut TimerQueue,
    attempts: &mut Attempts,
    operations: &mut Operations,
    queue: &mut OperationQueue,
    workers: &mut WorkerRegistry,
) -> TimerEffects {
    let mut effects = TimerEffects::default();
    let mut due = Vec::new();
    timers.drain_due(now.tick, &mut due);

    for item in due {
        match item.subject {
            TimerSubject::Ack(attempt_key) => {
                if let AckTimeoutOutcome::TimedOut {
                    operation,
                    kill,
                    requeued,
                    publish,
                    ..
                } = attempts.on_ack_timeout(
                    attempt_key,
                    max_worker_loss_retries,
                    operations,
                    queue,
                    workers,
                ) {
                    timers.disarm(TimerSubject::AttemptDeadline(attempt_key));
                    effects.kills.push(kill);
                    if requeued {
                        effects.requeued_operations.push(operation);
                    }
                    if let Some(event) = publish {
                        effects.publish.push(event);
                    }
                }
            }
            TimerSubject::AttemptDeadline(attempt_key) => {
                if let AttemptDeadlineOutcome::Expired { kill, publish, .. } =
                    attempts.on_attempt_deadline(attempt_key, operations, queue, workers)
                {
                    timers.disarm(TimerSubject::Ack(attempt_key));
                    effects.kills.push(kill);
                    if let Some(publish) = publish {
                        effects.publish.push(publish);
                    }
                }
            }
            TimerSubject::Worker(worker_key) => {
                if workers.disconnect(worker_key).is_some() {
                    effects.timed_out_workers.push(worker_key);
                    let loss = attempts.on_worker_lost(
                        worker_key,
                        max_worker_loss_retries,
                        operations,
                        queue,
                    );
                    for removed in loss.removed_attempts {
                        timers.disarm_attempt(removed);
                    }
                    effects.requeued_operations.extend(loss.requeued);
                    effects.publish.extend(loss.failed);
                }
            }
            TimerSubject::ClientLess(op_key) => {
                if let Some(expired) = operations.try_expire_client_less(
                    op_key,
                    now.tick,
                    client_action_timeout,
                    queue,
                ) {
                    match expired.previous_stage {
                        OperationStage::Queued => {
                            effects.dropped_operations.push(expired.key);
                            effects.publish.push(PublishEvent::Cancelled {
                                operation: expired.key,
                            });
                        }
                        OperationStage::Running => {
                            if expired.needs_kill
                                && let Some(kill) = attempts.mark_cancelled_kill(expired.key)
                            {
                                timers.disarm(TimerSubject::Ack(kill.attempt_key));
                                effects.kills.push(kill);
                            }
                            effects.publish.push(PublishEvent::Cancelled {
                                operation: expired.key,
                            });
                        }
                        OperationStage::Completed | OperationStage::Cancelled => {}
                    }
                }
            }
        }
    }

    effects
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use gang_types::{
        ActionResources, AttemptId, Bytes, Digest, FleetActionPolicy, FleetCatalog, FleetName,
        FleetProperties, FleetSet, InstanceId, Isa, OperationName, Resources,
    };

    use super::*;
    use crate::attempts::{KillReason, TerminalFailure};
    use crate::operations::{AdmitOutcome, NewOperation, canonical_platform_key};
    use crate::placement::place_operation;
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: gang_clock::Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![FleetProperties::new(
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
        )])
        .expect("catalog")
    }

    fn register_worker(reg: &mut WorkerRegistry, instance: &str, seed: u8) -> WorkerKey {
        reg.register(
            now(1),
            "0.3.9",
            false,
            WorkerRegistration {
                instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
                fleet: FleetName::try_new("build-x86").expect("fleet"),
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
        .expect("register")
    }

    fn admit_op(
        ops: &mut Operations,
        queue: &mut OperationQueue,
        name: &str,
        watchers: u32,
    ) -> OperationKey {
        let AdmitOutcome::Inserted { key, .. } = ops.admit(
            NewOperation {
                name: OperationName::try_new(name).expect("name"),
                action_digest: Digest::from_bytes(name.as_bytes()),
                platform_key: canonical_platform_key([("ISA", "x86-64")]),
                skip_cache_lookup: false,
                do_not_cache: false,
                source: None,
                build: None,
                target: None,
                mnemonic: None,
                candidates: FleetSet::try_from_indices([0]).expect("candidates"),
                priority: 100,
                has_explicit_cpu_floor: false,
                reservation: ActionResources {
                    cpu_millicores: 2_000,
                    memory: Bytes::gib(2),
                    disk: Bytes::gib(1),
                },
                learned_reservation: None,
                initial_watchers: watchers,
            },
            now(1),
            queue,
        ) else {
            panic!("insert");
        };
        key
    }

    #[test]
    fn ack_timeout_and_attempt_deadline_wakeups() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let w = register_worker(&mut workers, "i-1", 1);
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();
        let mut timers = TimerQueue::new();
        let max_losses = NonZeroU32::new(3).expect("3");
        let client_timeout = Duration::from_mins(1);

        assert_eq!(timers.next_deadline(), None);

        // Ack timeout fires at t=11s for unacked attempt dispatched at t=1s.
        let op_ack = admit_op(&mut ops, &mut queue, "operations/ack", 1);
        let a1 = place_operation(op_ack, &mut ops, &mut queue, &mut workers).expect("place");
        let k1 = attempts
            .dispatch(
                a1,
                AttemptId::from_random_bytes([1; 16]),
                now(1),
                Duration::from_secs(10),
                Duration::from_mins(5),
            )
            .expect("dispatch");
        timers.arm_attempt(
            k1,
            now(1).tick + Duration::from_secs(10),
            now(1).tick + Duration::from_mins(5),
        );
        assert_eq!(timers.next_deadline(), Some(now(11).tick));
        assert_eq!(timers.next_kind(), Some(TimerKind::AckTimeout));

        let e10 = on_timer(
            now(10),
            max_losses,
            client_timeout,
            &mut timers,
            &mut attempts,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert!(e10.kills.is_empty());

        let e11 = on_timer(
            now(11),
            max_losses,
            client_timeout,
            &mut timers,
            &mut attempts,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert_eq!(e11.kills.len(), 1);
        assert_eq!(e11.kills[0].reason, KillReason::AckTimeout);
        assert_eq!(e11.requeued_operations, vec![op_ack]);
        assert_eq!(ops.get(op_ack).expect("op").stage, OperationStage::Queued);

        // Dispatch op_ack again, accept it (disarming ack), and let attempt_deadline expire.
        let a2 = place_operation(op_ack, &mut ops, &mut queue, &mut workers).expect("place2");
        let aid2 = AttemptId::from_random_bytes([2; 16]);
        let k2 = attempts
            .dispatch(
                a2,
                aid2.clone(),
                now(12),
                Duration::from_secs(10),
                Duration::from_secs(50),
            )
            .expect("d2");
        timers.arm_attempt(k2, now(22).tick, now(62).tick);
        attempts.on_accept(&aid2, now(13), &ops);
        timers.disarm(TimerSubject::Ack(k2));

        let e62 = on_timer(
            now(62),
            max_losses,
            client_timeout,
            &mut timers,
            &mut attempts,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert_eq!(e62.kills.len(), 1);
        assert_eq!(e62.kills[0].reason, KillReason::DeadlineExceeded);
        assert!(matches!(
            e62.publish.as_slice(),
            [PublishEvent::Failed {
                failure: TerminalFailure::DeadlineExceeded { .. },
                ..
            }]
        ));
        // Reservation remains on worker until Executed or worker loss.
        assert_ne!(
            workers.get(w).expect("w").ledger.reserved(),
            ActionResources::default()
        );
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn worker_timeout_and_client_less_wakeups() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let w = register_worker(&mut workers, "i-1", 1);
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();
        let mut timers = TimerQueue::new();
        let max_losses = NonZeroU32::new(3).expect("3");
        let client_timeout = Duration::from_mins(1);

        timers.arm(TimerSubject::Worker(w), now(70).tick);
        let op_cl = admit_op(&mut ops, &mut queue, "operations/cl", 1);
        let a3 = place_operation(op_cl, &mut ops, &mut queue, &mut workers).expect("place3");
        let k3 = attempts
            .dispatch(
                a3,
                AttemptId::from_random_bytes([3; 16]),
                now(63),
                Duration::from_secs(10),
                Duration::from_mins(5),
            )
            .expect("d3");
        timers.arm_attempt(k3, now(73).tick, now(363).tick);
        ops.unwatch(op_cl, now(64).tick);
        timers.arm(
            TimerSubject::ClientLess(op_cl),
            now(64).tick + client_timeout,
        );

        let e70 = on_timer(
            now(70),
            max_losses,
            client_timeout,
            &mut timers,
            &mut attempts,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert_eq!(e70.timed_out_workers, vec![w]);
        assert_eq!(e70.requeued_operations, vec![op_cl]);
        assert_eq!(workers.worker_count(), 0);
        assert_eq!(ops.get(op_cl).expect("op_cl").stage, OperationStage::Queued);

        let e124 = on_timer(
            now(124),
            max_losses,
            client_timeout,
            &mut timers,
            &mut attempts,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert_eq!(e124.dropped_operations, vec![op_cl]);
        assert!(ops.get(op_cl).is_none());
        assert_eq!(timers.next_deadline(), None);
    }
}
