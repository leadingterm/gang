//! Attempt lifecycle state machine, worker-loss retry accounting, and ledger conservation
//! (`§5.5`).
//!
//! Invariants enforced across all transitions:
//! - Every admitted operation reaches a terminal stage (`Completed` or `Cancelled`) at most once.
//! - A worker's reserved CPU and memory equal the sum over its `Offered` and `Running` attempts;
//!   its reserved disk equals the sum over all non-`Completed` attempts (`Offered`, `Running`, and
//!   `Executed` awaiting `Completed`).

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::ActionsConfig;
use gang_types::{ActionResources, AttemptId, Bytes, Digest, OperationName};

use crate::admission::ActionProfile;
use crate::capacity::CapacityCatalog;
use crate::durations::ActionDurations;
use crate::escalation::{
    EscalationDecision, EscalationFailure, EscalationInput, KillCause, KillResource, classify_kill,
    evaluate_escalation,
};
use crate::fitting;
use crate::operations::{Now, OperationKey, OperationStage, Operations, Slab};
use crate::placement::PlacedAssignment;
use crate::queue::OperationQueue;
use crate::rates::{SourceRates, execution_timing};
use crate::sketch::{Observation, ResourceSketch, SketchKey};
use crate::workers::{WorkerKey, WorkerRegistry};

/// Generation-checked `u32` handle for an [`Attempt`] stored in [`Attempts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptKey {
    index: u32,
    generation: u32,
}

impl AttemptKey {
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

/// Lifecycle phase of a dispatched [`Attempt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttemptPhase {
    /// Dispatched to a worker via `Assign`, awaiting `Accept` or `Decline` before `ack_deadline`.
    Offered,
    /// Accepted by the worker and actively executing.
    Running,
    /// Execution finished on the worker; CPU and memory released, disk held until `Completed`.
    Executed,
}

/// Reason a worker declined an offered attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeclineReason {
    /// Worker lacked sufficient disk space; places worker in `disk_hold` until its next keepalive.
    DiskShortage,
    /// Worker is draining or shutting down.
    Draining,
    /// Worker is saturated by CPU pressure.
    CpuPressure,
}

/// Reason the controller asks a worker to kill an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KillReason {
    /// Worker failed to acknowledge `Assign` before `ack_deadline` (best-effort cleanup kill).
    AckTimeout,
    /// Dispatch-stamped `attempt_deadline` elapsed.
    DeadlineExceeded,
    /// Client cancelled the operation or dropped all watchers for `client_action_timeout`.
    Cancelled,
}

/// Directive to send `Kill` to a worker for a specific attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillDirective {
    /// Target worker holding the attempt.
    pub worker: WorkerKey,
    /// Scheduler attempt key.
    pub attempt_key: AttemptKey,
    /// Wire `AttemptId` sent to the worker.
    pub attempt_id: AttemptId,
    /// Why the attempt is being killed.
    pub reason: KillReason,
}

/// Terminal failure summary for an operation that will not be retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalFailure {
    /// The action references CAS objects that must be uploaded again.
    MissingBlobs {
        /// Operation name.
        operation: OperationName,
        /// Absent blobs.
        digests: Vec<Digest>,
        /// Worker diagnostic.
        message: String,
    },
    /// Dispatch-stamped `attempt_deadline` passed without execution finishing.
    DeadlineExceeded {
        /// Operation name.
        operation: OperationName,
    },
    /// Resource kill could not escalate further.
    Escalation(EscalationFailure),
    /// Generic non-resource failures exhausted `max_job_retries`.
    GenericRetriesExhausted {
        /// Operation name.
        operation: OperationName,
        /// Number of generic failures recorded.
        attempts: u32,
        /// Last worker failure message.
        message: String,
    },
    /// Worker disconnect / ack-timeout losses exhausted `max_worker_loss_retries`.
    WorkerLossExhausted {
        /// Operation name.
        operation: OperationName,
        /// Number of worker losses recorded.
        losses: u32,
    },
}

/// State publication event emitted when an operation transitions stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishEvent {
    /// Worker accepted the attempt and execution is in progress.
    Executing {
        /// Running operation key.
        operation: OperationKey,
        /// Active attempt identifier.
        attempt_id: AttemptId,
        /// Executing worker key.
        worker: WorkerKey,
    },
    /// Operation finished successfully.
    Completed {
        /// Completed operation key.
        operation: OperationKey,
    },
    /// Operation reached a terminal failure.
    Failed {
        /// Failed operation key.
        operation: OperationKey,
        /// Terminal failure cause.
        failure: TerminalFailure,
    },
    /// Operation was cancelled explicitly or by client-less timeout.
    Cancelled {
        /// Cancelled operation key.
        operation: OperationKey,
    },
}

/// Resource and timing measurements reported by a worker in `Executed`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AttemptUsage {
    /// Peak memory usage in bytes, if measured.
    pub peak_memory: Option<Bytes>,
    /// Peak private disk usage in bytes, if measured.
    pub peak_disk: Option<Bytes>,
    /// Cumulative CPU time consumed by the action's processes.
    pub cpu_time: Duration,
    /// Peak smoothed interval CPU burn in millicores, if sampled.
    pub peak_window_cpu_millicores: Option<u64>,
    /// Wall-clock execution duration on the worker.
    pub wall_time: Duration,
    /// Kernel `cgroup` `oom_kill` counter observed on the action's scope.
    pub oom_kills: u64,
}

/// Worker `Executed` event payload for [`Attempts::on_executed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedReport<'a> {
    /// Wire `AttemptId` reported by the worker.
    pub attempt_id: &'a AttemptId,
    /// Current scheduler clock reading.
    pub now: Now,
    /// Measured CPU, memory, disk, and OOM counters.
    pub usage: AttemptUsage,
    /// Worker-classified resource kill, if any.
    pub kill: Option<KillCause>,
    /// Side-channel `failure: memory` signal (applied only if no kill was already classified).
    pub side_channel_memory_failure: bool,
    /// The worker stopped execution because the action or attempt deadline elapsed.
    pub deadline_exceeded: bool,
    /// Admission profile of the operation (`command_digest`, original `floors`).
    pub profile: &'a ActionProfile,
}

/// Worker `Completed` status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionStatus {
    /// Action execution and output upload succeeded (`REAPI` status `OK`).
    Ok,
    /// Required CAS blobs are absent; retrying the worker cannot repair them.
    MissingBlobs {
        /// Absent blobs.
        digests: Vec<Digest>,
        /// Worker diagnostic.
        message: String,
    },
    /// Non-OK completion not classified as a resource kill.
    Failed {
        /// Failure diagnostic message.
        message: String,
    },
}

/// Worker `Completed` event payload for [`Attempts::on_completed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedReport<'a> {
    /// Wire `AttemptId` reported by the worker.
    pub attempt_id: &'a AttemptId,
    /// Current scheduler clock reading.
    pub now: Now,
    /// Execution completion status.
    pub status: CompletionStatus,
    /// Worker-reported `(execution_started_at, execution_completed_at)` timestamps for duration and
    /// rate observation (`None` when timestamps were omitted or invalid).
    pub execution_timestamps: Option<(Timestamp, Timestamp)>,
}

/// A single dispatched execution attempt tracked by [`Attempts`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    /// Unique `UUIDv4` attempt identifier sent to the worker.
    pub id: AttemptId,
    /// Operation being executed by this attempt.
    pub operation: OperationKey,
    /// Worker holding this attempt's reservations.
    pub worker: WorkerKey,
    /// Resource reservation held on `worker`'s ledger for this attempt.
    pub reservation: ActionResources,
    /// Whether this attempt counted against `worker.gate_running` at dispatch.
    pub is_gate: bool,
    cpu_reservation: AttemptCpuReservation,
    /// Current attempt phase.
    pub phase: AttemptPhase,
    /// Monotonic and wall-clock time when the attempt was dispatched.
    pub dispatched_at: Now,
    /// Monotonic deadline for receiving `Accept`/`Decline`.
    pub ack_deadline: Tick,
    /// Monotonic deadline for execution, replaced by the completion deadline at `Executed`.
    pub attempt_deadline: Tick,
    /// Wall-clock deadline for the current phase, used by controller handoff projections.
    pub deadline_wall: Timestamp,
    /// Timestamp when the worker acknowledged the attempt, if accepted.
    pub accepted_at: Option<Now>,
    /// `true` while this attempt still owns the operation's lifecycle outcome.
    owns_operation: bool,
    /// Whether the phase deadline already emitted its kill directive.
    deadline_expired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AttemptCpuReservation {
    guaranteed: bool,
    assign_seq: u64,
}

/// Live lower-bound measurements for an active attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptUsageUpdate {
    /// Attempt identifier.
    pub attempt_id: AttemptId,
    /// Observed CPU burn in millicores.
    pub cpu_millicores: u64,
    /// Observed working-set memory.
    pub memory: Bytes,
}

impl Attempt {
    /// Returns `true` while this attempt still owns the operation's lifecycle outcome.
    #[must_use]
    pub const fn owns_operation(&self) -> bool {
        self.owns_operation
    }
}

/// Result of [`Attempts::on_accept`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// Attempt transitioned from `Offered` to `Running`.
    Accepted {
        /// Key of the accepted attempt.
        attempt_key: AttemptKey,
        /// `Executing` event when the operation is still active and owned by this attempt.
        publish: Option<PublishEvent>,
    },
    /// Attempt was unknown or no longer in `Offered` phase.
    Ignored,
}

/// Result of [`Attempts::on_decline`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclineOutcome {
    /// Attempt declined and the operation was requeued at its original `seq` with no retry spent.
    Requeued {
        /// Attempt key removed.
        attempt_key: AttemptKey,
        /// Operation requeued for placement.
        operation: OperationKey,
        /// Declining worker.
        worker: WorkerKey,
        /// Decline reason.
        reason: DeclineReason,
        /// `true` when `disk_hold` was set on the worker (`DeclineReason::DiskShortage`).
        disk_hold_set: bool,
    },
    /// Attempt declined after the operation had already reached a terminal stage.
    DroppedTerminal {
        /// Attempt key removed.
        attempt_key: AttemptKey,
        /// Terminal operation key.
        operation: OperationKey,
        /// Declining worker.
        worker: WorkerKey,
        /// Decline reason.
        reason: DeclineReason,
        /// `true` when `disk_hold` was set on the worker.
        disk_hold_set: bool,
    },
    /// Attempt was unknown or no longer `Offered`.
    Ignored,
}

/// Result of [`Attempts::on_executed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutedOutcome {
    /// Compute (CPU and memory) released; disk remains reserved while awaiting `Completed`.
    AwaitingCompletion {
        /// Executed attempt key.
        attempt_key: AttemptKey,
        /// Operation key.
        operation: OperationKey,
        /// Worker holding disk until `Completed`.
        worker: WorkerKey,
    },
    /// Resource kill triggered escalation and requeued the operation; old attempt still holds disk
    /// on `worker` until `Completed`.
    EscalatedRequeue {
        /// Old attempt key awaiting `Completed` for disk release.
        attempt_key: AttemptKey,
        /// Requeued operation key.
        operation: OperationKey,
        /// Old worker key.
        worker: WorkerKey,
        /// Classified kill cause.
        cause: KillCause,
        /// Escalated reservation for the next attempt.
        next_reservation: ActionResources,
    },
    /// The operation terminated while the attempt retains disk until completion.
    Failed {
        /// Executed attempt key still awaiting `Completed` for disk release.
        attempt_key: AttemptKey,
        /// Operation key.
        operation: OperationKey,
        /// Worker holding disk until `Completed`.
        worker: WorkerKey,
        /// Terminal failure event to publish once.
        publish: PublishEvent,
    },
    /// Operation was already terminal (e.g. `DeadlineExceeded` or `Cancelled`); only worker CPU and
    /// memory were released.
    ReleasedComputeOnly {
        /// Attempt key.
        attempt_key: AttemptKey,
        /// Operation key.
        operation: OperationKey,
        /// Worker key.
        worker: WorkerKey,
    },
    /// Attempt was unknown or already `Executed`.
    Ignored,
}

/// Result of [`Attempts::on_completed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletedOutcome {
    /// Attempt finished successfully, disk released, observations recorded, and terminal event
    /// emitted.
    Completed {
        /// Removed attempt key.
        attempt_key: AttemptKey,
        /// Completed operation key.
        operation: OperationKey,
        /// Worker whose disk was released.
        worker: WorkerKey,
        /// Terminal completion event.
        publish: PublishEvent,
    },
    /// Non-OK completion retried under `max_job_retries` at original `seq`.
    GenericRetry {
        /// Removed attempt key.
        attempt_key: AttemptKey,
        /// Requeued operation key.
        operation: OperationKey,
        /// Worker whose disk was released.
        worker: WorkerKey,
        /// Updated generic retry count.
        retries_used: u32,
    },
    /// Non-OK completion exhausted `max_job_retries` and terminated the operation.
    Failed {
        /// Removed attempt key.
        attempt_key: AttemptKey,
        /// Failed operation key.
        operation: OperationKey,
        /// Worker whose disk was released.
        worker: WorkerKey,
        /// Terminal failure event.
        publish: PublishEvent,
    },
    /// Attempt belonged to an already-escalated or already-terminal operation; only disk was
    /// released.
    ReleasedDiskOnly {
        /// Removed attempt key.
        attempt_key: AttemptKey,
        /// Operation key.
        operation: OperationKey,
        /// Worker whose disk was released.
        worker: WorkerKey,
    },
    /// Attempt was unknown.
    Ignored,
}

/// Result of [`Attempts::on_ack_timeout`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckTimeoutOutcome {
    /// Ack timed out while still `Offered`; attempt removed, reservation released, best-effort
    /// `Kill` emitted, and operation either requeued or terminated via worker-loss budget.
    TimedOut {
        /// Removed attempt key.
        attempt_key: AttemptKey,
        /// Operation key.
        operation: OperationKey,
        /// Unresponsive worker key.
        worker: WorkerKey,
        /// Best-effort kill directive for the worker.
        kill: KillDirective,
        /// `true` when the operation was requeued for retry.
        requeued: bool,
        /// Terminal failure event if worker-loss retries were exhausted.
        publish: Option<PublishEvent>,
    },
    /// Attempt already accepted, executed, or removed.
    Stale,
}

/// Result of [`Attempts::on_attempt_deadline`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptDeadlineOutcome {
    /// Execution or completion deadline expired. Execution reservations remain until `Executed`
    /// or worker loss; disk remains reserved until `Completed` or worker loss.
    Expired {
        /// Attempt key.
        attempt_key: AttemptKey,
        /// Operation key (possibly already terminal or owned by a newer attempt).
        operation: OperationKey,
        /// Worker running the expired attempt.
        worker: WorkerKey,
        /// Kill directive to send to the worker.
        kill: KillDirective,
        /// Terminal `DeadlineExceeded` event, only when the expired attempt owns the operation.
        publish: Option<PublishEvent>,
    },
    /// Attempt is unknown, already expired, or detached before execution finished.
    Stale,
}

/// Combined effects of [`Attempts::on_worker_lost`] when a worker disconnects or times out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerLossOutcome {
    /// Attempts removed from the lost worker (for disarming timers).
    pub removed_attempts: Vec<AttemptKey>,
    /// Operations requeued at original `seq` with promoted priority/weight under
    /// `max_worker_loss_retries`.
    pub requeued: Vec<OperationKey>,
    /// Terminal failure events for operations that exhausted `max_worker_loss_retries`.
    pub failed: Vec<PublishEvent>,
}

/// Result of [`Attempts::on_operation_cancelled`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOutcome {
    /// Stage before cancellation (`None` if missing or already terminal).
    pub previous_stage: Option<OperationStage>,
    /// `Kill(Cancelled)` directive if an active `Offered` or `Running` attempt was running.
    pub kill: Option<KillDirective>,
    /// `PublishEvent::Cancelled` if cancellation transitioned a non-terminal operation.
    pub publish: Option<PublishEvent>,
}

/// Slab-indexed registry of active and post-execution attempts (`§5.5`).
#[derive(Debug, Clone, Default)]
pub struct Attempts {
    slab: Slab<Attempt>,
    by_id: HashMap<AttemptId, AttemptKey>,
    active_by_op: HashMap<OperationKey, AttemptKey>,
    by_worker: HashMap<WorkerKey, Vec<AttemptKey>>,
    completed_count: u64,
}

impl Attempts {
    pub(crate) fn ratchet_inflight(
        &mut self,
        worker_key: WorkerKey,
        update: &AttemptUsageUpdate,
        workers: &mut WorkerRegistry,
    ) -> Option<OperationKey> {
        let key = self.lookup_id(&update.attempt_id)?;
        let attempt = self.slab.get_mut(key.index, key.generation)?;
        if attempt.worker != worker_key || attempt.phase == AttemptPhase::Executed {
            return None;
        }
        let worker = workers.get_mut(worker_key)?;
        let cpu = update
            .cpu_millicores
            .min(worker.ceilings.cpu_millicores)
            .max(attempt.reservation.cpu_millicores);
        let memory = update
            .memory
            .min(worker.ceilings.memory)
            .max(attempt.reservation.memory);
        let cpu_delta = cpu - attempt.reservation.cpu_millicores;
        let memory_delta = memory.saturating_sub(attempt.reservation.memory);
        if cpu_delta == 0 && memory_delta.get() == 0 {
            return None;
        }
        worker
            .ledger
            .ratchet_compute(cpu_delta, memory_delta)
            .ok()?;
        if attempt.cpu_reservation.guaranteed {
            worker.guaranteed_cpu_millicores =
                worker.guaranteed_cpu_millicores.saturating_add(cpu_delta);
        }
        if let Some((_, pending)) = worker
            .pending_cpu
            .iter_mut()
            .find(|(seq, _)| *seq == attempt.cpu_reservation.assign_seq)
        {
            *pending = pending.saturating_add(cpu_delta);
            worker.dispatched_since_load_cpu =
                worker.dispatched_since_load_cpu.saturating_add(cpu_delta);
        }
        attempt.reservation.cpu_millicores = cpu;
        attempt.reservation.memory = memory;
        Some(attempt.operation)
    }
    /// Creates an empty attempt registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of tracked non-`Completed` attempts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slab.len()
    }

    /// Returns `true` when no non-`Completed` attempts exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    /// Monotonic count of operations that finished with [`CompletionStatus::Ok`] on a worker.
    #[must_use]
    pub const fn completed_count(&self) -> u64 {
        self.completed_count
    }

    /// Borrows an attempt by its generation-checked [`AttemptKey`].
    #[must_use]
    pub fn get(&self, key: AttemptKey) -> Option<&Attempt> {
        self.slab.get(key.index, key.generation)
    }

    /// Resolves a wire [`AttemptId`] to its [`AttemptKey`].
    #[must_use]
    pub fn lookup_id(&self, id: &AttemptId) -> Option<AttemptKey> {
        self.by_id.get(id).copied()
    }

    /// Returns the active (`Offered` or `Running`) attempt for `op`, if any.
    #[must_use]
    pub fn active_for_operation(&self, op: OperationKey) -> Option<AttemptKey> {
        self.active_by_op.get(&op).copied()
    }

    /// Iterates over all tracked non-`Completed` attempts (`Offered`, `Running`, and `Executed`).
    pub fn iter(&self) -> impl Iterator<Item = (AttemptKey, &Attempt)> {
        self.slab
            .iter()
            .map(|(index, generation, attempt)| (AttemptKey { index, generation }, attempt))
    }

    /// Iterates over `(OperationKey, AttemptKey)` pairs for operations with an active attempt.
    pub fn active_operations(&self) -> impl Iterator<Item = (OperationKey, AttemptKey)> + '_ {
        self.active_by_op
            .iter()
            .map(|(&op, &attempt)| (op, attempt))
    }

    /// Returns all non-`Completed` attempts (`Offered`, `Running`, `Executed`) owned by `worker`.
    #[must_use]
    pub fn worker_attempts(&self, worker: WorkerKey) -> &[AttemptKey] {
        self.by_worker.get(&worker).map_or(&[], Vec::as_slice)
    }

    /// Returns the latest current-phase wall-clock deadline among all non-`Completed` attempts
    /// owned by `worker` (for `ControllerHandOff::refresh_drain`).
    #[must_use]
    pub fn latest_attempt_deadline(&self, worker: WorkerKey) -> Option<Timestamp> {
        self.worker_attempts(worker)
            .iter()
            .filter_map(|&key| self.get(key))
            .map(|attempt| attempt.deadline_wall)
            .max()
    }

    /// Records a newly placed assignment as an [`AttemptPhase::Offered`] attempt with
    /// dispatch-stamped `ack_deadline` and `attempt_deadline`.
    pub fn dispatch(
        &mut self,
        assignment: PlacedAssignment,
        attempt_id: AttemptId,
        now: Now,
        ack_timeout: Duration,
        attempt_budget: Duration,
    ) -> Option<AttemptKey> {
        if self.by_id.contains_key(&attempt_id) {
            return None;
        }
        let attempt = Attempt {
            id: attempt_id.clone(),
            operation: assignment.operation,
            worker: assignment.worker,
            reservation: assignment.reservation,
            is_gate: crate::qos::is_gate(assignment.priority),
            cpu_reservation: AttemptCpuReservation {
                guaranteed: assignment.guaranteed,
                assign_seq: assignment.assign_seq,
            },
            phase: AttemptPhase::Offered,
            dispatched_at: now,
            ack_deadline: now.tick + ack_timeout,
            attempt_deadline: now.tick + attempt_budget,
            deadline_wall: now.wall + attempt_budget,
            accepted_at: None,
            owns_operation: true,
            deadline_expired: false,
        };
        let (index, generation) = self.slab.insert(attempt)?;
        let key = AttemptKey { index, generation };
        self.by_id.insert(attempt_id, key);
        self.active_by_op.insert(assignment.operation, key);
        self.by_worker
            .entry(assignment.worker)
            .or_default()
            .push(key);
        Some(key)
    }

    /// Handles worker `Accept` for an `Offered` attempt before `ack_deadline`.
    pub fn on_accept(
        &mut self,
        attempt_id: &AttemptId,
        now: Now,
        operations: &Operations,
    ) -> AcceptOutcome {
        let Some(key) = self.lookup_id(attempt_id) else {
            return AcceptOutcome::Ignored;
        };
        let Some(attempt) = self.slab.get_mut(key.index, key.generation) else {
            return AcceptOutcome::Ignored;
        };
        if attempt.phase != AttemptPhase::Offered {
            return AcceptOutcome::Ignored;
        }
        attempt.phase = AttemptPhase::Running;
        attempt.accepted_at = Some(now);

        let publish = if attempt.owns_operation
            && operations
                .get(attempt.operation)
                .is_some_and(|op| op.stage == OperationStage::Running)
        {
            Some(PublishEvent::Executing {
                operation: attempt.operation,
                attempt_id: attempt.id.clone(),
                worker: attempt.worker,
            })
        } else {
            None
        };

        AcceptOutcome::Accepted {
            attempt_key: key,
            publish,
        }
    }

    /// Handles worker `Decline` (`DiskShortage` or `Draining`) for an `Offered` attempt.
    ///
    /// Releases all reservations on the worker, applies `disk_hold` on `DiskShortage`, and requeues
    /// the operation at its original `seq` without consuming any retry budget.
    pub fn on_decline(
        &mut self,
        attempt_id: &AttemptId,
        reason: DeclineReason,
        operations: &mut Operations,
        queue: &mut OperationQueue,
        workers: &mut WorkerRegistry,
    ) -> DeclineOutcome {
        let Some(key) = self.lookup_id(attempt_id) else {
            return DeclineOutcome::Ignored;
        };
        let Some(phase) = self.get(key).map(|a| a.phase) else {
            return DeclineOutcome::Ignored;
        };
        if phase != AttemptPhase::Offered {
            return DeclineOutcome::Ignored;
        }
        let Some(attempt) = self.remove_attempt(key) else {
            return DeclineOutcome::Ignored;
        };

        let _ = workers.on_attempt_aborted(
            attempt.worker,
            attempt.reservation,
            attempt.is_gate,
            attempt.cpu_reservation.guaranteed,
            attempt.cpu_reservation.assign_seq,
        );
        if reason == DeclineReason::CpuPressure
            && let Some(worker) = workers.get_mut(attempt.worker)
        {
            worker.cpu_pressure_hold = true;
        }
        let disk_hold_set = if reason == DeclineReason::DiskShortage {
            workers.set_disk_hold(attempt.worker)
        } else {
            false
        };

        let requeued = attempt.owns_operation && operations.requeue(attempt.operation, queue);
        if requeued {
            DeclineOutcome::Requeued {
                attempt_key: key,
                operation: attempt.operation,
                worker: attempt.worker,
                reason,
                disk_hold_set,
            }
        } else {
            DeclineOutcome::DroppedTerminal {
                attempt_key: key,
                operation: attempt.operation,
                worker: attempt.worker,
                reason,
                disk_hold_set,
            }
        }
    }

    /// Handles worker `Executed` report: releases CPU and memory while retaining disk until
    /// `Completed`, records a sketch sample, and evaluates kill classification / escalation (`§5.7`).
    #[allow(
        clippy::too_many_arguments,
        reason = "pure state transition requires explicit domain registries and policy"
    )]
    pub fn on_executed(
        &mut self,
        report: &ExecutedReport<'_>,
        actions: &ActionsConfig,
        capacity: &CapacityCatalog,
        sketch: &mut ResourceSketch,
        operations: &mut Operations,
        queue: &mut OperationQueue,
        workers: &mut WorkerRegistry,
    ) -> ExecutedOutcome {
        let Some(key) = self.lookup_id(report.attempt_id) else {
            return ExecutedOutcome::Ignored;
        };
        let Some((worker, operation, reservation, is_gate, guaranteed, assign_seq, owns_operation)) =
            self.mark_phase_executed(key)
        else {
            return ExecutedOutcome::Ignored;
        };

        let _ = workers.on_attempt_executed(worker, reservation, is_gate, guaranteed, assign_seq);

        let is_active_op = owns_operation
            && operations
                .get(operation)
                .is_some_and(|op| op.stage == OperationStage::Running);
        if !is_active_op {
            self.detach_operation(key, operation);
            return ExecutedOutcome::ReleasedComputeOnly {
                attempt_key: key,
                operation,
                worker,
            };
        }

        let Some((sketch_key, action_name, candidates, retry_counts)) =
            operation_exec_snapshot(operations, operation, report.profile.history_key.as_ref())
        else {
            self.detach_operation(key, operation);
            return ExecutedOutcome::ReleasedComputeOnly {
                attempt_key: key,
                operation,
                worker,
            };
        };

        let classified_kill = classify_kill(
            report.kill,
            report.usage.oom_kills,
            report.side_channel_memory_failure,
        );
        let peak_memory_kb = usage_kib(report.usage.peak_memory);
        let peak_disk_kb = usage_kib(report.usage.peak_disk);

        let memory_ceiling_kb = fitting::largest_eligible_memory_kb(
            capacity.current(),
            candidates,
            report.profile.floors,
        )
        .unwrap_or(0);
        record_executed_observation(
            sketch,
            sketch_key,
            report,
            reservation,
            classified_kill,
            memory_ceiling_kb,
        );

        if report.deadline_exceeded {
            self.detach_operation(key, operation);
            return finish_worker_deadline(key, operation, worker, operations, queue);
        }

        let Some(cause) = classified_kill else {
            return ExecutedOutcome::AwaitingCompletion {
                attempt_key: key,
                operation,
                worker,
            };
        };

        self.detach_operation(key, operation);
        let measured_peak_kb = match cause.resource() {
            KillResource::Memory => peak_memory_kb,
            KillResource::Disk => peak_disk_kb,
        };
        let input = EscalationInput {
            action: &action_name,
            cause,
            measured_peak_kb,
            current_reservation: reservation,
            cpu_before_memory_fit: report.profile.cpu_before_memory_fit,
            floors: report.profile.floors,
            candidates,
            retry_counts,
        };
        apply_escalation_decision(
            key,
            operation,
            worker,
            cause,
            evaluate_escalation(&input, actions, capacity),
            operations,
            queue,
        )
    }

    fn mark_phase_executed(
        &mut self,
        key: AttemptKey,
    ) -> Option<(
        WorkerKey,
        OperationKey,
        ActionResources,
        bool,
        bool,
        u64,
        bool,
    )> {
        let attempt = self.slab.get_mut(key.index, key.generation)?;
        if attempt.phase == AttemptPhase::Executed {
            return None;
        }
        attempt.phase = AttemptPhase::Executed;
        Some((
            attempt.worker,
            attempt.operation,
            attempt.reservation,
            attempt.is_gate,
            attempt.cpu_reservation.guaranteed,
            attempt.cpu_reservation.assign_seq,
            attempt.owns_operation,
        ))
    }

    fn detach_operation(&mut self, key: AttemptKey, operation: OperationKey) {
        if let Some(attempt) = self.slab.get_mut(key.index, key.generation) {
            attempt.owns_operation = false;
        }
        if self.active_by_op.get(&operation) == Some(&key) {
            self.active_by_op.remove(&operation);
        }
    }

    /// Handles worker `Completed` report: releases disk on the worker ledger, updates completion
    /// counter, duration/rate estimators, and applies generic `max_job_retries` on non-OK status.
    #[allow(
        clippy::too_many_arguments,
        reason = "pure completion requires explicit domain registries and estimators"
    )]
    pub fn on_completed(
        &mut self,
        report: CompletedReport<'_>,
        actions: &ActionsConfig,
        operations: &mut Operations,
        queue: &mut OperationQueue,
        workers: &mut WorkerRegistry,
        durations: &mut ActionDurations,
        rates: &mut SourceRates,
    ) -> CompletedOutcome {
        let Some(key) = self.lookup_id(report.attempt_id) else {
            return CompletedOutcome::Ignored;
        };
        let Some(attempt) = self.remove_attempt(key) else {
            return CompletedOutcome::Ignored;
        };

        if attempt.phase == AttemptPhase::Executed {
            let _ = workers.on_attempt_completed(attempt.worker, attempt.reservation.disk);
        } else {
            let _ = workers.on_attempt_aborted(
                attempt.worker,
                attempt.reservation,
                attempt.is_gate,
                attempt.cpu_reservation.guaranteed,
                attempt.cpu_reservation.assign_seq,
            );
        }

        let is_active_op = attempt.owns_operation
            && operations
                .get(attempt.operation)
                .is_some_and(|op| op.stage == OperationStage::Running);
        if !is_active_op {
            return CompletedOutcome::ReleasedDiskOnly {
                attempt_key: key,
                operation: attempt.operation,
                worker: attempt.worker,
            };
        }

        match report.status {
            CompletionStatus::MissingBlobs { digests, message } => {
                let Some(operation) = operations.get(attempt.operation).map(|op| op.name.clone())
                else {
                    return CompletedOutcome::ReleasedDiskOnly {
                        attempt_key: key,
                        operation: attempt.operation,
                        worker: attempt.worker,
                    };
                };
                operations.complete(attempt.operation, queue);
                CompletedOutcome::Failed {
                    attempt_key: key,
                    operation: attempt.operation,
                    worker: attempt.worker,
                    publish: PublishEvent::Failed {
                        operation: attempt.operation,
                        failure: TerminalFailure::MissingBlobs {
                            operation,
                            digests,
                            message,
                        },
                    },
                }
            }
            CompletionStatus::Ok => {
                self.completed_count = self.completed_count.saturating_add(1);
                record_successful_completion(
                    &attempt,
                    report.now,
                    report.execution_timestamps,
                    operations,
                    durations,
                    rates,
                );
                operations.complete(attempt.operation, queue);
                CompletedOutcome::Completed {
                    attempt_key: key,
                    operation: attempt.operation,
                    worker: attempt.worker,
                    publish: PublishEvent::Completed {
                        operation: attempt.operation,
                    },
                }
            }
            CompletionStatus::Failed { message } => {
                handle_generic_failure(key, &attempt, message, actions, operations, queue)
            }
        }
    }

    /// Expires an unacknowledged `Offered` attempt on `ack_deadline`, emitting a best-effort
    /// `Kill` and applying the `max_worker_loss_retries` budget (`§5.5`).
    pub fn on_ack_timeout(
        &mut self,
        attempt_key: AttemptKey,
        max_worker_loss_retries: NonZeroU32,
        operations: &mut Operations,
        queue: &mut OperationQueue,
        workers: &mut WorkerRegistry,
    ) -> AckTimeoutOutcome {
        let Some(phase) = self.get(attempt_key).map(|a| a.phase) else {
            return AckTimeoutOutcome::Stale;
        };
        if phase != AttemptPhase::Offered {
            return AckTimeoutOutcome::Stale;
        }
        let Some(attempt) = self.remove_attempt(attempt_key) else {
            return AckTimeoutOutcome::Stale;
        };

        let _ = workers.on_attempt_aborted(
            attempt.worker,
            attempt.reservation,
            attempt.is_gate,
            attempt.cpu_reservation.guaranteed,
            attempt.cpu_reservation.assign_seq,
        );
        let kill = KillDirective {
            worker: attempt.worker,
            attempt_key,
            attempt_id: attempt.id,
            reason: KillReason::AckTimeout,
        };

        let (requeued, publish) = if attempt.owns_operation {
            apply_worker_loss(
                attempt.operation,
                max_worker_loss_retries,
                operations,
                queue,
            )
        } else {
            (false, None)
        };

        AckTimeoutOutcome::TimedOut {
            attempt_key,
            operation: attempt.operation,
            worker: attempt.worker,
            kill,
            requeued,
            publish,
        }
    }

    /// Bounds the remaining attempt budget by a post-`Executed` completion timeout.
    /// Duplicate `Executed` messages must not call this method or extend the deadline.
    pub(crate) fn start_completion_timeout(
        &mut self,
        key: AttemptKey,
        now: Now,
        timeout: Duration,
    ) -> Option<Tick> {
        let attempt = self.slab.get_mut(key.index, key.generation)?;
        if attempt.phase != AttemptPhase::Executed || attempt.deadline_expired {
            return None;
        }
        let completion_deadline = now.tick + timeout;
        if completion_deadline < attempt.attempt_deadline {
            attempt.attempt_deadline = completion_deadline;
            attempt.deadline_wall = now.wall + timeout;
        }
        Some(attempt.attempt_deadline)
    }

    /// Expires the current phase deadline and emits `Kill(DeadlineExceeded)`. An owning attempt
    /// terminates its operation without retry. Reservations remain held until worker cleanup
    /// or loss, including detached uploads whose operation was cancelled or escalated (`§5.5`).
    pub fn on_attempt_deadline(
        &mut self,
        attempt_key: AttemptKey,
        operations: &mut Operations,
        queue: &mut OperationQueue,
        _workers: &mut WorkerRegistry,
    ) -> AttemptDeadlineOutcome {
        let (worker, operation, attempt_id, owns_operation) = {
            let Some(attempt) = self.slab.get_mut(attempt_key.index, attempt_key.generation) else {
                return AttemptDeadlineOutcome::Stale;
            };
            if attempt.deadline_expired
                || (!attempt.owns_operation && attempt.phase != AttemptPhase::Executed)
            {
                return AttemptDeadlineOutcome::Stale;
            }
            let owns_operation = attempt.owns_operation;
            attempt.owns_operation = false;
            attempt.deadline_expired = true;
            (
                attempt.worker,
                attempt.operation,
                attempt.id.clone(),
                owns_operation,
            )
        };
        if self.active_by_op.get(&operation) == Some(&attempt_key) {
            self.active_by_op.remove(&operation);
        }
        let publish = if owns_operation {
            operations
                .get(operation)
                .map(|op| op.name.clone())
                .and_then(|name| {
                    operations
                        .complete(operation, queue)
                        .then_some(PublishEvent::Failed {
                            operation,
                            failure: TerminalFailure::DeadlineExceeded { operation: name },
                        })
                })
        } else {
            None
        };

        AttemptDeadlineOutcome::Expired {
            attempt_key,
            operation,
            worker,
            kill: KillDirective {
                worker,
                attempt_key,
                attempt_id,
                reason: KillReason::DeadlineExceeded,
            },
            publish,
        }
    }

    /// Handles worker disconnect or `worker_timeout`: removes every attempt owned by `worker` and
    /// requeues active operations at their original `seq` (preserving promoted priority/weight)
    /// within `max_worker_loss_retries` (`§5.5`).
    pub fn on_worker_lost(
        &mut self,
        worker: WorkerKey,
        max_worker_loss_retries: NonZeroU32,
        operations: &mut Operations,
        queue: &mut OperationQueue,
    ) -> WorkerLossOutcome {
        let mut outcome = WorkerLossOutcome::default();
        let Some(keys) = self.by_worker.remove(&worker) else {
            return outcome;
        };
        for key in keys {
            let Some(attempt) = self.slab.remove(key.index, key.generation) else {
                continue;
            };
            self.by_id.remove(&attempt.id);
            if self.active_by_op.get(&attempt.operation) == Some(&key) {
                self.active_by_op.remove(&attempt.operation);
            }
            outcome.removed_attempts.push(key);
            if !attempt.owns_operation {
                continue;
            }
            let (requeued, failed) = apply_worker_loss(
                attempt.operation,
                max_worker_loss_retries,
                operations,
                queue,
            );
            if requeued {
                outcome.requeued.push(attempt.operation);
            }
            if let Some(event) = failed {
                outcome.failed.push(event);
            }
        }
        outcome
    }

    /// Cancels `op_key` immediately (`CancelOperation`): dequeues if queued, or marks `Cancelled`
    /// and emits `Kill(Cancelled)` if running while retaining worker reservations until `Executed`
    /// or worker loss (`§5.5`).
    pub fn on_operation_cancelled(
        &mut self,
        op_key: OperationKey,
        operations: &mut Operations,
        queue: &mut OperationQueue,
    ) -> CancelOutcome {
        let previous_stage = operations.cancel(op_key, queue);
        let Some(prev) = previous_stage else {
            return CancelOutcome {
                previous_stage: None,
                kill: None,
                publish: None,
            };
        };

        let kill = if prev == OperationStage::Running {
            self.mark_cancelled_kill(op_key)
        } else {
            None
        };

        CancelOutcome {
            previous_stage: Some(prev),
            kill,
            publish: Some(PublishEvent::Cancelled { operation: op_key }),
        }
    }

    /// Detaches ownership from the active attempt for `op_key` (if any) and builds a
    /// `Kill(Cancelled)` directive without modifying `operations` (used after
    /// [`Operations::try_expire_client_less`]).
    pub fn mark_cancelled_kill(&mut self, op_key: OperationKey) -> Option<KillDirective> {
        let attempt_key = self.active_by_op.remove(&op_key)?;
        let attempt = self
            .slab
            .get_mut(attempt_key.index, attempt_key.generation)?;
        attempt.owns_operation = false;
        Some(KillDirective {
            worker: attempt.worker,
            attempt_key,
            attempt_id: attempt.id.clone(),
            reason: KillReason::Cancelled,
        })
    }

    fn remove_attempt(&mut self, key: AttemptKey) -> Option<Attempt> {
        let attempt = self.slab.remove(key.index, key.generation)?;
        self.by_id.remove(&attempt.id);
        if self.active_by_op.get(&attempt.operation) == Some(&key) {
            self.active_by_op.remove(&attempt.operation);
        }
        if let Some(list) = self.by_worker.get_mut(&attempt.worker) {
            list.retain(|&entry| entry != key);
            if list.is_empty() {
                self.by_worker.remove(&attempt.worker);
            }
        }
        Some(attempt)
    }
}

fn apply_worker_loss(
    op_key: OperationKey,
    max_worker_loss_retries: NonZeroU32,
    operations: &mut Operations,
    queue: &mut OperationQueue,
) -> (bool, Option<PublishEvent>) {
    let Some(op) = operations.get_mut(op_key) else {
        return (false, None);
    };
    if op.stage != OperationStage::Running {
        return (false, None);
    }
    if op.retry_counts.worker_loss < max_worker_loss_retries.get() {
        op.retry_counts.worker_loss = op.retry_counts.worker_loss.saturating_add(1);
        operations.requeue(op_key, queue);
        (true, None)
    } else {
        let op_name = op.name.clone();
        let losses = op.retry_counts.worker_loss;
        operations.complete(op_key, queue);
        (
            false,
            Some(PublishEvent::Failed {
                operation: op_key,
                failure: TerminalFailure::WorkerLossExhausted {
                    operation: op_name,
                    losses,
                },
            }),
        )
    }
}

fn usage_kib(bytes: Option<Bytes>) -> Option<u64> {
    bytes.map(|b| b.get().div_ceil(1024)).filter(|&kb| kb > 0)
}

fn operation_exec_snapshot(
    operations: &Operations,
    operation: OperationKey,
    history_key: Option<&SketchKey>,
) -> Option<(
    SketchKey,
    OperationName,
    gang_types::FleetSet,
    crate::operations::RetryCounts,
)> {
    let op = operations.get(operation)?;
    Some((
        history_key.cloned().unwrap_or_else(|| {
            SketchKey::for_attempt(
                op.target.as_ref().map(gang_types::TargetLabel::as_str),
                op.mnemonic.as_ref().map(gang_types::Mnemonic::as_str),
                "",
            )
        }),
        op.name.clone(),
        op.candidates,
        op.retry_counts,
    ))
}

fn finish_worker_deadline(
    attempt_key: AttemptKey,
    operation: OperationKey,
    worker: WorkerKey,
    operations: &mut Operations,
    queue: &mut OperationQueue,
) -> ExecutedOutcome {
    let Some(name) = operations.get(operation).map(|op| op.name.clone()) else {
        return ExecutedOutcome::ReleasedComputeOnly {
            attempt_key,
            operation,
            worker,
        };
    };
    operations.complete(operation, queue);
    ExecutedOutcome::Failed {
        attempt_key,
        operation,
        worker,
        publish: PublishEvent::Failed {
            operation,
            failure: TerminalFailure::DeadlineExceeded { operation: name },
        },
    }
}

fn record_executed_observation(
    sketch: &mut ResourceSketch,
    sketch_key: SketchKey,
    report: &ExecutedReport<'_>,
    reservation: ActionResources,
    classified_kill: Option<KillCause>,
    memory_ceiling_kb: u64,
) {
    let peak_memory_kb =
        usage_kib(report.usage.peak_memory).map(|peak| peak.min(memory_ceiling_kb));
    let peak_disk_kb = usage_kib(report.usage.peak_disk);
    let reserved_mem_kb = reservation
        .memory
        .get()
        .div_ceil(1024)
        .min(memory_ceiling_kb);
    let reserved_disk_kb = reservation.disk.get().div_ceil(1024);
    let killed_peak_memory_kb = classified_kill
        .filter(|k| k.resource() == KillResource::Memory)
        .map(|_| peak_memory_kb.unwrap_or(reserved_mem_kb));
    let killed_peak_disk_kb = classified_kill
        .filter(|k| k.resource() == KillResource::Disk)
        .map(|_| peak_disk_kb.unwrap_or(reserved_disk_kb));

    sketch.observe_with_memory_ceiling(
        sketch_key,
        &Observation {
            peak_memory_kb: peak_memory_kb.unwrap_or(0),
            cpu_millicores: average_cpu_millicores(report.usage.cpu_time, report.usage.wall_time),
            peak_disk_kb: peak_disk_kb.unwrap_or(0),
            killed_peak_memory_kb,
            killed_peak_disk_kb,
            killed: classified_kill.is_some(),
            now_s: report.now.wall.as_unix_millis() / 1000,
        },
        memory_ceiling_kb,
    );
}

fn apply_escalation_decision(
    attempt_key: AttemptKey,
    operation: OperationKey,
    worker: WorkerKey,
    cause: KillCause,
    decision: EscalationDecision,
    operations: &mut Operations,
    queue: &mut OperationQueue,
) -> ExecutedOutcome {
    match decision {
        EscalationDecision::Retry {
            next_reservation,
            resource,
            ..
        } => {
            if let Some(op) = operations.get_mut(operation) {
                op.reservation = next_reservation;
                op.cpu_weight =
                    crate::qos::weight_for_reservation(op.priority, op.reservation.cpu_millicores);
                match resource {
                    KillResource::Memory => {
                        op.retry_counts.memory_steps =
                            op.retry_counts.memory_steps.saturating_add(1);
                    }
                    KillResource::Disk => {
                        op.retry_counts.disk_steps = op.retry_counts.disk_steps.saturating_add(1);
                    }
                }
            }
            operations.requeue(operation, queue);
            ExecutedOutcome::EscalatedRequeue {
                attempt_key,
                operation,
                worker,
                cause,
                next_reservation,
            }
        }
        EscalationDecision::Fail(failure) => {
            operations.complete(operation, queue);
            ExecutedOutcome::Failed {
                attempt_key,
                operation,
                worker,
                publish: PublishEvent::Failed {
                    operation,
                    failure: TerminalFailure::Escalation(failure),
                },
            }
        }
    }
}

fn record_successful_completion(
    attempt: &Attempt,
    now: Now,
    execution_timestamps: Option<(Timestamp, Timestamp)>,
    operations: &Operations,
    durations: &mut ActionDurations,
    rates: &mut SourceRates,
) {
    let timing = execution_timestamps.and_then(|(start, end)| execution_timing(start, end));
    let Some(op) = operations.get(attempt.operation) else {
        return;
    };
    let target = op
        .target
        .as_ref()
        .map_or("", gang_types::TargetLabel::as_str);
    let mnemonic = op
        .mnemonic
        .as_ref()
        .map_or("", gang_types::Mnemonic::as_str);
    if let (Some(duration), Some((_, completed_at))) = (timing, execution_timestamps) {
        let slot_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        durations.record(target, mnemonic, slot_ms, completed_at);
    }
    let preferred_fleet = op.candidates.iter().next().unwrap_or(0);
    rates.record(
        op.source,
        preferred_fleet,
        attempt.reservation,
        timing,
        now.tick,
    );
}

fn handle_generic_failure(
    attempt_key: AttemptKey,
    attempt: &Attempt,
    message: String,
    actions: &ActionsConfig,
    operations: &mut Operations,
    queue: &mut OperationQueue,
) -> CompletedOutcome {
    let Some(op) = operations.get_mut(attempt.operation) else {
        return CompletedOutcome::ReleasedDiskOnly {
            attempt_key,
            operation: attempt.operation,
            worker: attempt.worker,
        };
    };
    if op.retry_counts.generic < actions.max_job_retries.get() {
        op.retry_counts.generic = op.retry_counts.generic.saturating_add(1);
        let retries_used = op.retry_counts.generic;
        operations.requeue(attempt.operation, queue);
        CompletedOutcome::GenericRetry {
            attempt_key,
            operation: attempt.operation,
            worker: attempt.worker,
            retries_used,
        }
    } else {
        let op_name = op.name.clone();
        let attempts = op.retry_counts.generic;
        operations.complete(attempt.operation, queue);
        CompletedOutcome::Failed {
            attempt_key,
            operation: attempt.operation,
            worker: attempt.worker,
            publish: PublishEvent::Failed {
                operation: attempt.operation,
                failure: TerminalFailure::GenericRetriesExhausted {
                    operation: op_name,
                    attempts,
                    message,
                },
            },
        }
    }
}

fn average_cpu_millicores(cpu_time: Duration, wall_time: Duration) -> Option<u64> {
    if wall_time.is_zero() {
        return None;
    }
    let millicores = cpu_time
        .as_millis()
        .saturating_mul(1_000)
        .checked_div(wall_time.as_millis())
        .or_else(|| {
            cpu_time
                .as_nanos()
                .saturating_mul(1_000)
                .checked_div(wall_time.as_nanos())
        })?;
    Some(u64::try_from(millicores).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use gang_config::HistoryWindow;
    use gang_types::{
        CpuWeight, Digest, FleetActionPolicy, FleetCatalog, FleetName, FleetProperties, InstanceId,
        Isa, Mnemonic, Percent, Resources, SourceId, TargetLabel,
    };

    use super::*;
    use crate::fitting::ClientFloors;
    use crate::operations::{AdmitOutcome, NewOperation, canonical_platform_key};
    use crate::placement::place_operation;
    use crate::sketch::{Budget, Policy};
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
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

    fn admit_test_op(
        ops: &mut Operations,
        queue: &mut OperationQueue,
        name: &str,
        priority: i32,
        reservation: ActionResources,
    ) -> (OperationKey, ActionProfile) {
        let AdmitOutcome::Inserted { key, .. } = ops.admit(
            NewOperation {
                name: OperationName::try_new(name).expect("name"),
                action_digest: Digest::from_bytes(name.as_bytes()),
                platform_key: canonical_platform_key([("ISA", "x86-64")]),
                skip_cache_lookup: false,
                do_not_cache: false,
                source: SourceId::from_identity_str("local:alice"),
                build: None,
                target: Some(TargetLabel::try_new("//pkg:target").expect("target")),
                mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
                candidates: gang_types::FleetSet::try_from_indices([0]).expect("candidates"),
                priority,
                has_explicit_cpu_floor: false,
                reservation,
                learned_reservation: None,
                initial_watchers: 1,
            },
            now(1),
            queue,
        ) else {
            panic!("insert");
        };
        let profile = ActionProfile {
            history_key: Some(SketchKey::for_attempt(
                Some("//pkg:target"),
                Some("Rustc"),
                "cmd",
            )),
            floors: ClientFloors::default(),
            desired_reservation: reservation,
            cpu_before_memory_fit: reservation.cpu_millicores,
            fitted_fleet: 0,
            capacity_revision: 0,
        };
        (key, profile)
    }

    fn sketch() -> ResourceSketch {
        ResourceSketch::with_shards(
            Policy {
                headroom_percent: 20,
                cpu_reservation_percentile: Percent::from_percent(50),
                classes: Vec::new(),
                reserve_cpu: true,
                reserve_memory: true,
                reserve_disk: true,
            },
            Budget {
                target: 8,
                mnemonic: 8,
                command: 8,
                probation: 8,
            },
            Duration::from_hours(24),
            1,
        )
    }

    #[test]
    fn decline_disk_shortage_sets_hold_and_requeues_without_spending_retry() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let w = register_worker(&mut workers, "i-1", 1);
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();

        let res = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(8),
        };
        let (op, _) = admit_test_op(&mut ops, &mut queue, "operations/op1", 100, res);
        let assigned = place_operation(op, &mut ops, &mut queue, &mut workers).expect("place");
        let aid = AttemptId::from_random_bytes([1; 16]);
        attempts
            .dispatch(
                assigned,
                aid.clone(),
                now(2),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("dispatch");

        assert_eq!(workers.get(w).expect("w").ledger.reserved(), res);

        let decline = attempts.on_decline(
            &aid,
            DeclineReason::DiskShortage,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert!(matches!(
            decline,
            DeclineOutcome::Requeued {
                disk_hold_set: true,
                ..
            }
        ));
        assert!(workers.get(w).expect("w").has_disk_hold());
        assert_eq!(
            workers.get(w).expect("w").ledger.reserved(),
            ActionResources::default()
        );
        assert_eq!(ops.get(op).expect("op").stage, OperationStage::Queued);
        assert_eq!(
            ops.get(op).expect("op").retry_counts,
            crate::operations::RetryCounts::default()
        );
        // Worker cannot be placed while disk_hold is active; keepalive clears it.
        assert!(place_operation(op, &mut ops, &mut queue, &mut workers).is_none());
        assert_eq!(workers.on_keepalive(w, now(5), None, None), Some(true));
        assert!(place_operation(op, &mut ops, &mut queue, &mut workers).is_some());
    }

    fn normal_usage() -> AttemptUsage {
        AttemptUsage {
            peak_window_cpu_millicores: None,
            peak_memory: Some(Bytes::gib(2)),
            peak_disk: Some(Bytes::gib(1)),
            cpu_time: Duration::from_secs(12),
            wall_time: Duration::from_secs(6),
            oom_kills: 0,
        }
    }

    fn estimators() -> (ActionDurations, SourceRates) {
        let durations = ActionDurations::new(
            std::num::NonZeroUsize::new(16).expect("16"),
            HistoryWindow::from_units(1),
        );
        let rates = SourceRates::new(&gang_config::ScalingConfig::default(), 1).expect("rates");
        (durations, rates)
    }

    struct TestEnv {
        cap: CapacityCatalog,
        workers: WorkerRegistry,
        worker: WorkerKey,
        ops: Operations,
        queue: OperationQueue,
        attempts: Attempts,
        sk: ResourceSketch,
        actions: ActionsConfig,
        durations: ActionDurations,
        rates: SourceRates,
    }

    fn setup_env() -> TestEnv {
        let cat = catalog();
        let cap = CapacityCatalog::new(cat.clone());
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let worker = register_worker(&mut workers, "i-1", 2);
        let (durations, rates) = estimators();
        TestEnv {
            cap,
            workers,
            worker,
            ops: Operations::new(),
            queue: OperationQueue::new(1).expect("queue"),
            attempts: Attempts::new(),
            sk: sketch(),
            actions: ActionsConfig::default(),
            durations,
            rates,
        }
    }

    #[test]
    fn missing_blobs_finish_without_retry_and_release_resources() {
        let mut env = setup_env();
        let resources = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(8),
        };
        let (op, _) = admit_test_op(
            &mut env.ops,
            &mut env.queue,
            "operations/missing",
            100,
            resources,
        );
        let assigned =
            place_operation(op, &mut env.ops, &mut env.queue, &mut env.workers).expect("place");
        let id = AttemptId::from_random_bytes([43; 16]);
        env.attempts
            .dispatch(
                assigned,
                id.clone(),
                now(2),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("dispatch");
        env.attempts.on_accept(&id, now(3), &env.ops);
        let digest = Digest::from_bytes(b"missing");
        let outcome = env.attempts.on_completed(
            CompletedReport {
                attempt_id: &id,
                now: now(4),
                status: CompletionStatus::MissingBlobs {
                    digests: vec![digest],
                    message: "missing".to_owned(),
                },
                execution_timestamps: None,
            },
            &env.actions,
            &mut env.ops,
            &mut env.queue,
            &mut env.workers,
            &mut env.durations,
            &mut env.rates,
        );
        assert!(
            matches!(outcome, CompletedOutcome::Failed { publish: PublishEvent::Failed { failure: TerminalFailure::MissingBlobs { digests, .. }, .. }, .. } if digests == vec![digest])
        );
        assert_eq!(env.ops.get(op).expect("operation").retry_counts.generic, 0);
        assert_eq!(
            env.workers
                .get(env.worker)
                .expect("worker")
                .ledger
                .reserved(),
            ActionResources::default()
        );
    }

    #[test]
    fn worker_deadline_finishes_without_retry() {
        let mut env = setup_env();
        let resources = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(8),
        };
        let (op, profile) = admit_test_op(
            &mut env.ops,
            &mut env.queue,
            "operations/timeout",
            100,
            resources,
        );
        let assigned =
            place_operation(op, &mut env.ops, &mut env.queue, &mut env.workers).expect("place");
        let id = AttemptId::from_random_bytes([42; 16]);
        env.attempts
            .dispatch(
                assigned,
                id.clone(),
                now(2),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("dispatch");
        env.attempts.on_accept(&id, now(3), &env.ops);
        let outcome = env.attempts.on_executed(
            &ExecutedReport {
                attempt_id: &id,
                now: now(4),
                usage: normal_usage(),
                kill: None,
                side_channel_memory_failure: false,
                deadline_exceeded: true,
                profile: &profile,
            },
            &env.actions,
            &env.cap,
            &mut env.sk,
            &mut env.ops,
            &mut env.queue,
            &mut env.workers,
        );
        assert!(matches!(
            outcome,
            ExecutedOutcome::Failed {
                publish: PublishEvent::Failed {
                    failure: TerminalFailure::DeadlineExceeded { .. },
                    ..
                },
                ..
            }
        ));
        assert_eq!(env.ops.get(op).expect("operation").retry_counts.generic, 0);
        assert_eq!(
            env.workers
                .get(env.worker)
                .expect("worker")
                .ledger
                .reserved()
                .disk,
            resources.disk
        );
        let completed = env.attempts.on_completed(
            CompletedReport {
                attempt_id: &id,
                now: now(5),
                status: CompletionStatus::Failed {
                    message: "timed out".to_owned(),
                },
                execution_timestamps: None,
            },
            &env.actions,
            &mut env.ops,
            &mut env.queue,
            &mut env.workers,
            &mut env.durations,
            &mut env.rates,
        );
        assert!(matches!(
            completed,
            CompletedOutcome::ReleasedDiskOnly { .. }
        ));
        assert_eq!(
            env.workers
                .get(env.worker)
                .expect("worker")
                .ledger
                .reserved(),
            ActionResources::default()
        );
    }

    #[test]
    fn executed_releases_compute_early_while_holding_disk_until_completed() {
        let mut env = setup_env();
        let res = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(8),
        };
        let (op, profile) = admit_test_op(&mut env.ops, &mut env.queue, "operations/op1", 100, res);
        let assigned =
            place_operation(op, &mut env.ops, &mut env.queue, &mut env.workers).expect("place");
        let aid = AttemptId::from_random_bytes([2; 16]);
        let akey = env
            .attempts
            .dispatch(
                assigned,
                aid.clone(),
                now(2),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("dispatch");

        assert!(matches!(
            env.attempts.on_accept(&aid, now(3), &env.ops),
            AcceptOutcome::Accepted {
                publish: Some(PublishEvent::Executing { .. }),
                ..
            }
        ));
        assert_eq!(
            env.attempts.latest_attempt_deadline(env.worker),
            Some(Timestamp::from_unix_millis(602_000))
        );

        let executed = env.attempts.on_executed(
            &ExecutedReport {
                attempt_id: &aid,
                now: now(10),
                usage: normal_usage(),
                kill: None,
                side_channel_memory_failure: false,
                deadline_exceeded: false,
                profile: &profile,
            },
            &env.actions,
            &env.cap,
            &mut env.sk,
            &mut env.ops,
            &mut env.queue,
            &mut env.workers,
        );
        assert!(matches!(
            executed,
            ExecutedOutcome::AwaitingCompletion { attempt_key, .. } if attempt_key == akey
        ));
        assert_eq!(
            env.workers.get(env.worker).expect("w").ledger.reserved(),
            ActionResources {
                cpu_millicores: 0,
                memory: Bytes::new(0),
                disk: Bytes::gib(8),
            }
        );
        assert_eq!(env.attempts.worker_attempts(env.worker), &[akey]);
        let key = SketchKey::for_attempt(Some("//pkg:target"), Some("Rustc"), "cmd");
        assert!(env.sk.lookup(&key, 11).is_some());

        let completed = env.attempts.on_completed(
            CompletedReport {
                attempt_id: &aid,
                now: now(12),
                status: CompletionStatus::Ok,
                execution_timestamps: Some((
                    Timestamp::from_unix_millis(3_000),
                    Timestamp::from_unix_millis(9_000),
                )),
            },
            &env.actions,
            &mut env.ops,
            &mut env.queue,
            &mut env.workers,
            &mut env.durations,
            &mut env.rates,
        );
        assert!(matches!(
            completed,
            CompletedOutcome::Completed {
                publish: PublishEvent::Completed { .. },
                ..
            }
        ));
        assert_eq!(env.attempts.completed_count(), 1);
        assert_eq!(
            env.workers.get(env.worker).expect("w").ledger.reserved(),
            ActionResources::default()
        );
        let wall = Timestamp::from_unix_millis(12_000);
        assert_eq!(
            env.durations.expected("//pkg:target", "Rustc", wall),
            Some(Duration::from_secs(6))
        );
        let mut rate_rows = Vec::new();
        env.rates.snapshot_into(&mut rate_rows);
        assert_eq!(rate_rows.len(), 1);
    }

    #[test]
    fn oom_escalation_requeues_while_old_attempt_holds_disk_until_completed() {
        let cat = catalog();
        let cap = CapacityCatalog::new(cat.clone());
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let w1 = register_worker(&mut workers, "i-1", 1);
        let w2 = register_worker(&mut workers, "i-2", 2);
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();
        let mut sk = sketch();
        let actions = ActionsConfig::default();
        let mut durations = ActionDurations::new(
            std::num::NonZeroUsize::new(16).expect("16"),
            HistoryWindow::from_units(1),
        );
        let mut rates = SourceRates::new(&gang_config::ScalingConfig::default(), 1).expect("rates");

        let res = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(4),
            disk: Bytes::gib(2),
        };
        let (op, profile) = admit_test_op(&mut ops, &mut queue, "operations/oom", 100, res);
        let a1_assign = place_operation(op, &mut ops, &mut queue, &mut workers).expect("p1");
        let w_first = a1_assign.worker;
        let aid1 = AttemptId::from_random_bytes([10; 16]);
        attempts
            .dispatch(
                a1_assign,
                aid1.clone(),
                now(1),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("d1");
        attempts.on_accept(&aid1, now(2), &ops);

        // OOM kill on Executed triggers escalation to 8 GiB and requeues op immediately while w_first
        // still holds disk until aid1 reports Completed.
        let exec1 = attempts.on_executed(
            &ExecutedReport {
                attempt_id: &aid1,
                now: now(5),
                usage: AttemptUsage {
                    peak_window_cpu_millicores: None,
                    peak_memory: Some(Bytes::gib(4)),
                    peak_disk: None,
                    cpu_time: Duration::from_secs(2),
                    wall_time: Duration::from_secs(2),
                    oom_kills: 1,
                },
                kill: None,
                side_channel_memory_failure: false,
                deadline_exceeded: false,
                profile: &profile,
            },
            &actions,
            &cap,
            &mut sk,
            &mut ops,
            &mut queue,
            &mut workers,
        );
        assert!(matches!(
            exec1,
            ExecutedOutcome::EscalatedRequeue {
                cause: KillCause::Oom,
                next_reservation,
                ..
            } if next_reservation.memory == Bytes::gib(8)
        ));
        assert_eq!(ops.get(op).expect("op").stage, OperationStage::Queued);
        assert_eq!(ops.get(op).expect("op").retry_counts.memory_steps, 1);

        // Dispatch attempt 2 before attempt 1 finishes Completed!
        let a2_assign = place_operation(op, &mut ops, &mut queue, &mut workers).expect("p2");
        let aid2 = AttemptId::from_random_bytes([11; 16]);
        attempts
            .dispatch(
                a2_assign,
                aid2.clone(),
                now(6),
                Duration::from_secs(10),
                Duration::from_mins(10),
            )
            .expect("d2");

        // Old attempt aid1 finally completes: releases disk on w_first, does NOT complete `op`.
        let c1 = attempts.on_completed(
            CompletedReport {
                attempt_id: &aid1,
                now: now(7),
                status: CompletionStatus::Failed {
                    message: "killed".to_owned(),
                },
                execution_timestamps: None,
            },
            &actions,
            &mut ops,
            &mut queue,
            &mut workers,
            &mut durations,
            &mut rates,
        );
        assert!(matches!(c1, CompletedOutcome::ReleasedDiskOnly { .. }));
        assert_eq!(ops.get(op).expect("op").stage, OperationStage::Running);
        assert_eq!(attempts.completed_count(), 0);
        assert_ne!(w1, w2);
        let _ = w_first;
    }

    #[test]
    fn worker_loss_requeues_with_promoted_priority_until_exhausted() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();

        let res = ActionResources {
            cpu_millicores: 1_000,
            memory: Bytes::gib(2),
            disk: Bytes::gib(1),
        };
        let (op, _) = admit_test_op(&mut ops, &mut queue, "operations/loss", 10, res);
        let max_losses = NonZeroU32::new(2).expect("2");

        for round in 0..3_u8 {
            let w = register_worker(&mut workers, &format!("i-{round}"), round + 1);
            let assign = place_operation(op, &mut ops, &mut queue, &mut workers).expect("place");
            let aid = AttemptId::from_random_bytes([round + 20; 16]);
            attempts
                .dispatch(
                    assign,
                    aid.clone(),
                    now(u64::from(round) + 1),
                    Duration::from_secs(10),
                    Duration::from_mins(10),
                )
                .expect("dispatch");

            if round == 0 {
                // Promote operation priority while Running; worker loss must requeue with the
                // promoted priority and original seq.
                let dup = ops.admit(
                    NewOperation {
                        name: OperationName::try_new("operations/loss-dup").expect("dup"),
                        action_digest: Digest::from_bytes(b"operations/loss"),
                        platform_key: canonical_platform_key([("ISA", "x86-64")]),
                        skip_cache_lookup: false,
                        do_not_cache: false,
                        source: None,
                        build: None,
                        target: None,
                        mnemonic: None,
                        candidates: gang_types::FleetSet::try_from_indices([0]).expect("c"),
                        priority: 200,
                        has_explicit_cpu_floor: false,
                        reservation: res,
                        learned_reservation: None,
                        initial_watchers: 1,
                    },
                    now(2),
                    &mut queue,
                );
                assert!(matches!(
                    dup,
                    AdmitOutcome::Deduplicated {
                        promoted: true,
                        cpu_weight: CpuWeight::GATE,
                        ..
                    }
                ));
            }

            workers.disconnect(w);
            let loss = attempts.on_worker_lost(w, max_losses, &mut ops, &mut queue);
            if round < 2 {
                assert_eq!(loss.requeued, vec![op]);
                assert!(loss.failed.is_empty());
                assert_eq!(ops.get(op).expect("op").stage, OperationStage::Queued);
                assert_eq!(ops.get(op).expect("op").priority, 200);
                assert_eq!(ops.get(op).expect("op").cpu_weight, CpuWeight::GATE);
            } else {
                assert!(loss.requeued.is_empty());
                assert!(matches!(
                    loss.failed.as_slice(),
                    [PublishEvent::Failed {
                        failure: TerminalFailure::WorkerLossExhausted { losses: 2, .. },
                        ..
                    }]
                ));
                assert_eq!(ops.get(op).expect("op").stage, OperationStage::Completed);
            }
        }
    }
}
