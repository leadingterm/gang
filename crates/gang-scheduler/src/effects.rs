//! Caller-owned reusable output buffer of pure directives emitted by [`Scheduler`](crate::Scheduler).

use gang_clock::{Tick, Timestamp};
use gang_types::{
    ActionResources, AttemptId, BuildId, CpuWeight, Digest, Mnemonic, OperationName, TargetLabel,
    WorkerId,
};

use crate::attempts::{AttemptKey, KillReason, PublishEvent, TerminalFailure};
use crate::handoff::RehomeReason;
use crate::operations::OperationKey;
use crate::qos::GateAudit;
use crate::workers::WorkerKey;

/// Stage transition emitted on an operation's watcher channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationUpdate {
    /// Operation is queued (newly admitted or requeued for retry).
    Queued,
    /// Worker accepted an attempt and execution is in progress.
    Executing {
        /// Active attempt identifier.
        attempt_id: AttemptId,
        /// Worker executing the attempt.
        worker: WorkerKey,
    },
    /// Operation finished successfully.
    Completed,
    /// Operation reached a terminal failure.
    Failed(TerminalFailure),
    /// Operation was cancelled explicitly or by client-less timeout.
    Cancelled,
}

impl OperationUpdate {
    /// Converts a [`PublishEvent`] into `(OperationKey, OperationUpdate)`.
    #[must_use]
    pub(crate) fn from_publish(event: PublishEvent) -> (OperationKey, Self) {
        match event {
            PublishEvent::Executing {
                operation,
                attempt_id,
                worker,
            } => (operation, Self::Executing { attempt_id, worker }),
            PublishEvent::Completed { operation } => (operation, Self::Completed),
            PublishEvent::Failed { operation, failure } => (operation, Self::Failed(failure)),
            PublishEvent::Cancelled { operation } => (operation, Self::Cancelled),
        }
    }
}

/// Completed-attempt execution summary emitted for the build observation sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedObservation {
    /// Completed operation handle.
    pub operation: OperationKey,
    /// Unique `REAPI` operation name.
    pub operation_name: OperationName,
    /// Digest of the `REAPI` `Action`.
    pub action_digest: Digest,
    /// Worker handle that executed the attempt.
    pub worker: WorkerKey,
    /// Wire `WorkerId` that executed the attempt.
    pub worker_id: WorkerId,
    /// First creator's build identifier, if present.
    pub build: Option<BuildId>,
    /// First creator's target label, if present.
    pub target: Option<TargetLabel>,
    /// First creator's mnemonic, if present.
    pub mnemonic: Option<Mnemonic>,
    /// Wall-clock time when the operation was first admitted/queued.
    pub queued_at: Timestamp,
    /// Wall-clock time when execution started (accepted or dispatched).
    pub started_at: Timestamp,
    /// Wall-clock time when `Completed` arrived.
    pub completed_at: Timestamp,
    /// Worker-reported `(execution_started_at, execution_completed_at)` timestamps, if valid.
    pub execution_timestamps: Option<(Timestamp, Timestamp)>,
}

/// Pure output directive emitted by [`Scheduler`](crate::Scheduler) for the actor shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Dispatch an execution attempt to a worker.
    AssignWorker {
        /// Target worker handle.
        worker: WorkerKey,
        /// Scheduler attempt handle.
        attempt: AttemptKey,
        /// Wire `UUIDv4` attempt identifier.
        attempt_id: AttemptId,
        /// Admitted operation handle.
        operation: OperationKey,
        /// Out-of-band `cgroup` v2 `cpu.weight` derived from operation priority.
        cpu_weight: CpuWeight,
        /// Whether the reservation is protected from reclamation.
        guaranteed: bool,
        /// Monotonic sequence of this assignment on the worker.
        assign_seq: u64,
        /// Resource reservation held for this attempt.
        reservation: ActionResources,
        /// Monotonic execution deadline (`dispatched_at.tick + attempt_budget`).
        deadline: Tick,
        /// Wall-clock execution deadline (`dispatched_at.wall + attempt_budget`).
        deadline_wall: Timestamp,
    },
    /// Ask a worker to terminate an attempt.
    KillWorker {
        /// Target worker handle.
        worker: WorkerKey,
        /// Scheduler attempt handle.
        attempt: AttemptKey,
        /// Wire `AttemptId` sent to the worker.
        attempt_id: AttemptId,
        /// Why the attempt is being killed.
        reason: KillReason,
    },
    /// Update a connected worker's draining state.
    SetWorkerDraining {
        /// Target worker handle.
        worker: WorkerKey,
        /// Effective draining state (`true` to stop new work, `false` to resume).
        draining: bool,
    },
    /// Close a worker session with `Rehome` during cooperative controller handoff.
    RehomeWorker {
        /// Target worker handle.
        worker: WorkerKey,
        /// Why the outgoing controller is releasing this worker session.
        reason: RehomeReason,
    },
    /// Drop a connected worker handle after keepalive timeout expiry.
    CloseWorker {
        /// Timed-out worker handle disconnected from the registry.
        worker: WorkerKey,
    },
    /// Publish an operation stage transition to active client watchers.
    NotifyWatchers {
        /// Target operation handle.
        operation: OperationKey,
        /// Updated operation lifecycle event.
        event: OperationUpdate,
    },
    /// Emit a structured gate-tier (`priority >= 200`) audit record.
    AuditGateTier(GateAudit),
    /// Emit a completed-attempt execution observation for the build observer sink.
    RecordObservation(CompletedObservation),
}

/// Caller-owned reusable buffer of [`Effect`] directives (`§5.1`).
///
/// Steady-state actor loops call [`Effects::clear`] before each step so the underlying `Vec`
/// retains its heap allocation across batches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Effects(Vec<Effect>);

impl Effects {
    /// Creates an empty effect buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Creates an empty effect buffer with preallocated capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    /// Clears all buffered effects while retaining allocated capacity.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Returns `true` when no effects are buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of buffered effects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Appends one effect to the buffer.
    pub fn push(&mut self, effect: Effect) {
        self.0.push(effect);
    }

    /// Iterates over buffered effects in emission order.
    pub fn iter(&self) -> std::slice::Iter<'_, Effect> {
        self.0.iter()
    }

    /// Returns the buffered effects as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[Effect] {
        &self.0
    }

    /// Drains all buffered effects in emission order, retaining allocated capacity.
    pub fn drain(&mut self) -> std::vec::Drain<'_, Effect> {
        self.0.drain(..)
    }
}

impl IntoIterator for Effects {
    type Item = Effect;
    type IntoIter = std::vec::IntoIter<Effect>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Effects {
    type Item = &'a Effect;
    type IntoIter = std::slice::Iter<'a, Effect>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
