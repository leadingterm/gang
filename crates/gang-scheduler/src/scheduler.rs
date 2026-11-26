//! Deterministic single-owner scheduling state machine (`§5.1`).
//!
//! [`Scheduler`] composes admission, `QoS`, fitting, capacity facts, priority queues, distributed
//! placement, attempt lifecycle, escalation, timers, cordons, handoff, learning, and projections
//! behind a pure `&mut self` API with caller-owned [`Effects`] output buffers.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::{ActionsConfig, ScalingConfig};
use gang_spi::{
    ActionsView, ControllerDrain, FleetMemoryCapacity, LearnedHistory, ListingLimits,
    SchedulerSnapshot, WorkerCacheStats,
};
use gang_types::{
    ActionResources, AttemptId, Bytes, FleetCatalog, FleetCountError, FleetName, InstanceId,
    OperationName,
};
use thiserror::Error;

use crate::admission::{
    ActionProfile, AdmissionError, AdmissionOutcome, AdmissionRequest, admit_action,
};
use crate::attempts::{
    AcceptOutcome, AttemptKey, AttemptUsage, Attempts, CompletedOutcome, CompletedReport,
    CompletionStatus, DeclineOutcome, DeclineReason, ExecutedOutcome, ExecutedReport, PublishEvent,
};
use crate::capacity::{CapacityCatalog, CapacityError};
use crate::cordons::{Census, CordonError, CordonRegistry};
use crate::escalation::KillCause;
use crate::fitting::ClientFloors;
use crate::handoff::{ControllerHandOff, HandOffEffects, HandOffError};
use crate::learned::Learned;
use crate::listing::fill_actions;
use crate::operations::{Now, OperationKey, Operations};
use crate::placement::{PlacedAssignment, place_operation, scan_queue_for_worker};
use crate::queue::OperationQueue;
use crate::sketch::SketchError;
use crate::snapshot::fill_snapshot;
use crate::timers::{TimerQueue, TimerSubject, on_timer as sweep_timers};
use crate::workers::{RegistrationError, WorkerKey, WorkerRegistration, WorkerRegistry};
use crate::{CompletedObservation, Effect, Effects, OperationUpdate};

/// Pure scheduling policy parameters supplied when constructing [`Scheduler`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerPolicy {
    /// Action scheduling, retry, and resource estimation policy.
    pub actions: ActionsConfig,
    /// Rate estimation and scaling policy.
    pub scaling: ScalingConfig,
    /// Expected controller Gang package version for stale-build replacement detection.
    pub expected_gang_version: String,
    /// Timeout before an unacknowledged `Assign` triggers worker-loss retry.
    pub dispatch_ack_timeout: Duration,
    /// Maximum non-fitting queued entries skipped per worker-driven dispatch scan.
    pub dispatch_scan_limit: usize,
    /// Silence timeout before a connected worker is dropped.
    pub worker_timeout: Duration,
    /// Timeout before a zero-watcher non-terminal operation is dropped or cancelled.
    pub client_action_timeout: Duration,
    /// Whole-attempt execution deadline budget (including transfers).
    pub attempt_budget: Duration,
    /// Per-term deterministic seed mixed into minted `UUIDv4` [`AttemptId`]s.
    pub attempt_seed: u64,
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self {
            actions: ActionsConfig::default(),
            scaling: ScalingConfig::default(),
            expected_gang_version: env!("CARGO_PKG_VERSION").to_owned(),
            dispatch_ack_timeout: Duration::from_secs(10),
            dispatch_scan_limit: 256,
            worker_timeout: Duration::from_secs(10),
            client_action_timeout: Duration::from_mins(10),
            attempt_budget: Duration::from_mins(20),
            attempt_seed: 0,
        }
    }
}

/// Errors constructing a [`Scheduler`] from policy, fleet catalog, and [`Learned`] state.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum PolicyError {
    /// Fleet catalog exceeded `MAX_FLEETS`.
    #[error(transparent)]
    FleetCount(#[from] FleetCountError),
    /// Supplied [`Learned`] rate table fleet count did not match `catalog.len()`.
    #[error("learned rate table has {actual} fleets, expected {expected}")]
    FleetCountMismatch {
        /// Number of fleets in `FleetCatalog`.
        expected: usize,
        /// Number of fleets in `Learned::rates`.
        actual: usize,
    },
    /// A required scheduler timeout or limit was zero.
    #[error("scheduler policy setting `{setting}` must be nonzero")]
    ZeroSetting {
        /// Name of the invalid zero setting.
        setting: &'static str,
    },
}

/// Errors returned by [`Scheduler::execute`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExecuteError {
    /// Controller leadership handoff has started and new dispatches/admissions are frozen.
    #[error("controller is handing off leadership")]
    HandingOff,
    /// Admission or registry rejection.
    #[error(transparent)]
    Admission(#[from] AdmissionError),
}

/// Errors returned by [`Scheduler::register`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegisterError {
    /// Controller leadership handoff has started and new worker registrations are frozen.
    #[error("controller is handing off leadership")]
    HandingOff,
    /// Worker registration failed validation or slab capacity.
    #[error(transparent)]
    Registration(#[from] RegistrationError),
}

/// Worker `Executed` event payload passed to [`Scheduler::on_worker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerExecuted {
    /// Wire `AttemptId` reported by the worker.
    pub attempt_id: AttemptId,
    /// Measured CPU, memory, disk, and OOM counters.
    pub usage: AttemptUsage,
    /// Worker-classified resource kill, if any.
    pub kill: Option<KillCause>,
    /// Side-channel `failure: memory` signal (applied only if no kill was already classified).
    pub side_channel_memory_failure: bool,
}

/// Worker `Completed` event payload passed to [`Scheduler::on_worker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCompleted {
    /// Wire `AttemptId` reported by the worker.
    pub attempt_id: AttemptId,
    /// Execution completion status.
    pub status: CompletionStatus,
    /// Worker-reported `(execution_started_at, execution_completed_at)` timestamps, if valid.
    pub execution_timestamps: Option<(Timestamp, Timestamp)>,
}

/// Inbound worker protocol events handled by [`Scheduler::on_worker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerEvent {
    /// Worker acknowledged an `Assign` before `ack_deadline`.
    Accept {
        /// Wire identifier of the accepted attempt.
        attempt_id: AttemptId,
    },
    /// Worker declined an offered `Assign`.
    Decline {
        /// Wire identifier of the declined attempt.
        attempt_id: AttemptId,
        /// Why the worker declined the offer.
        reason: DeclineReason,
    },
    /// Action process exited on the worker; CPU and memory are free, disk remains until `Completed`.
    Executed(WorkerExecuted),
    /// Outputs uploaded and attempt fully retired on the worker; disk is free.
    Completed(WorkerCompleted),
    /// Periodic worker heartbeat with optional cache stats and authoritative memory capacity.
    KeepAlive {
        /// Latest fast/slow store counters, if reported.
        cache: Option<WorkerCacheStats>,
        /// Latest physical/post-reserve memory capacity fact, if reported.
        memory_capacity: Option<Bytes>,
    },
    /// Worker announced graceful shutdown and must receive no new work.
    GoingAway,
}

/// Single-owner deterministic scheduler state machine (`§5.1`).
#[derive(Debug)]
pub struct Scheduler {
    policy: SchedulerPolicy,
    capacity: CapacityCatalog,
    operations: Operations,
    queue: OperationQueue,
    workers: WorkerRegistry,
    cordons: CordonRegistry,
    attempts: Attempts,
    timers: TimerQueue,
    handoff: ControllerHandOff,
    learned: Learned,
    profiles: HashMap<OperationKey, ActionProfile>,
    placement_scratch: Vec<PlacedAssignment>,
    handoff_scratch: HandOffEffects,
    next_attempt_seq: u64,
}

impl Scheduler {
    /// Constructs a new [`Scheduler`] term from `policy`, `catalog`, and `learned` (`§5.1`).
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError`] if `catalog` exceeds `MAX_FLEETS`, `learned`'s fleet count
    /// mismatches `catalog`, or any required timeout/limit setting is zero.
    pub fn new(
        policy: SchedulerPolicy,
        catalog: &FleetCatalog,
        learned: Learned,
    ) -> Result<Self, PolicyError> {
        if policy.dispatch_scan_limit == 0 {
            return Err(PolicyError::ZeroSetting {
                setting: "dispatch_scan_limit",
            });
        }
        if policy.dispatch_ack_timeout.is_zero() {
            return Err(PolicyError::ZeroSetting {
                setting: "dispatch_ack_timeout",
            });
        }
        if policy.worker_timeout.is_zero() {
            return Err(PolicyError::ZeroSetting {
                setting: "worker_timeout",
            });
        }
        if policy.client_action_timeout.is_zero() {
            return Err(PolicyError::ZeroSetting {
                setting: "client_action_timeout",
            });
        }
        if policy.attempt_budget.is_zero() {
            return Err(PolicyError::ZeroSetting {
                setting: "attempt_budget",
            });
        }
        if learned.rates().fleet_count() != catalog.len() {
            return Err(PolicyError::FleetCountMismatch {
                expected: catalog.len(),
                actual: learned.rates().fleet_count(),
            });
        }

        let queue = OperationQueue::new(catalog.len())?;
        let workers = WorkerRegistry::from_catalog(catalog)?;
        let cordons = CordonRegistry::from_catalog(catalog)?;
        let capacity = CapacityCatalog::new(catalog.clone(), policy.actions.cpu_oversubscription);

        Ok(Self {
            policy,
            capacity,
            operations: Operations::new(),
            queue,
            workers,
            cordons,
            attempts: Attempts::new(),
            timers: TimerQueue::new(),
            handoff: ControllerHandOff::new(),
            learned,
            profiles: HashMap::new(),
            placement_scratch: Vec::new(),
            handoff_scratch: HandOffEffects::default(),
            next_attempt_seq: 1,
        })
    }

    /// Returns the immutable scheduling policy.
    #[must_use]
    pub const fn policy(&self) -> &SchedulerPolicy {
        &self.policy
    }

    /// Returns the capacity catalog reflecting local and peer observations.
    #[must_use]
    pub const fn capacity(&self) -> &CapacityCatalog {
        &self.capacity
    }

    /// Returns the operation registry.
    #[must_use]
    pub const fn operations(&self) -> &Operations {
        &self.operations
    }

    /// Returns the priority operation queue.
    #[must_use]
    pub const fn queue(&self) -> &OperationQueue {
        &self.queue
    }

    /// Returns the connected worker registry.
    #[must_use]
    pub const fn workers(&self) -> &WorkerRegistry {
        &self.workers
    }

    /// Returns the physical instance cordon registry.
    #[must_use]
    pub const fn cordons(&self) -> &CordonRegistry {
        &self.cordons
    }

    /// Returns the active and post-execution attempt registry.
    #[must_use]
    pub const fn attempts(&self) -> &Attempts {
        &self.attempts
    }

    /// Returns the cooperative handoff coordinator.
    #[must_use]
    pub const fn handoff(&self) -> &ControllerHandOff {
        &self.handoff
    }

    /// Admits a client `Execute` request, deduplicating/promoting or enqueueing and attempting
    /// immediate placement (`§5.1`, `§5.3`).
    ///
    /// # Errors
    ///
    /// Returns [`ExecuteError::HandingOff`] if leadership handoff has started, or
    /// [`ExecuteError::Admission`] if no fleet accepts the request or registration fails.
    pub fn execute(
        &mut self,
        now: Now,
        request: AdmissionRequest,
        out: &mut Effects,
    ) -> Result<OperationKey, ExecuteError> {
        if !self.handoff.allows_dispatch() {
            return Err(ExecuteError::HandingOff);
        }
        let outcome = admit_action(
            request,
            now,
            &self.policy.actions,
            &self.capacity,
            self.learned.histories_mut().resource_mut(),
            &mut self.operations,
            &mut self.queue,
        )?;

        match outcome {
            AdmissionOutcome::Inserted {
                key,
                gate_audit,
                profile,
                ..
            } => {
                self.profiles.insert(key, profile);
                if let Some(audit) = gate_audit {
                    out.push(Effect::AuditGateTier(audit));
                }
                out.push(Effect::NotifyWatchers {
                    operation: key,
                    event: OperationUpdate::Queued,
                });
                if self.operations.get(key).is_some_and(|op| op.watchers == 0) {
                    self.timers.arm(
                        TimerSubject::ClientLess(key),
                        now.tick + self.policy.client_action_timeout,
                    );
                }
                self.try_place_operation(key, now, out);
                Ok(key)
            }
            AdmissionOutcome::Deduplicated {
                key, gate_audit, ..
            } => {
                if let Some(audit) = gate_audit {
                    out.push(Effect::AuditGateTier(audit));
                }
                if self.operations.get(key).is_some_and(|op| op.watchers > 0) {
                    self.timers.disarm(TimerSubject::ClientLess(key));
                }
                Ok(key)
            }
        }
    }

    /// Attaches a watcher to `operation`, disarming any pending client-less timeout.
    pub fn watch(&mut self, _now: Now, operation: &OperationName) -> Option<OperationKey> {
        let key = self.operations.watch_by_name(operation)?;
        self.timers.disarm(TimerSubject::ClientLess(key));
        Some(key)
    }

    /// Detaches a watcher from `key`, arming `client_action_timeout` if watchers drop to zero.
    pub fn unwatch(&mut self, now: Now, key: OperationKey, _out: &mut Effects) {
        let Some(result) = self.operations.unwatch(key, now.tick) else {
            return;
        };
        if result.became_client_less {
            self.timers.arm(
                TimerSubject::ClientLess(key),
                now.tick + self.policy.client_action_timeout,
            );
        }
    }

    /// Cancels `key` immediately, dequeuing if queued or emitting `Kill(Cancelled)` if running.
    pub fn cancel(&mut self, _now: Now, key: OperationKey, out: &mut Effects) {
        self.timers.disarm(TimerSubject::ClientLess(key));
        let outcome =
            self.attempts
                .on_operation_cancelled(key, &mut self.operations, &mut self.queue);
        if let Some(kill) = outcome.kill {
            self.timers.disarm_attempt(kill.attempt_key);
            out.push(Effect::KillWorker {
                worker: kill.worker,
                attempt: kill.attempt_key,
                attempt_id: kill.attempt_id,
                reason: kill.reason,
            });
        }
        if let Some(publish) = outcome.publish {
            self.push_publish(publish, out);
        }
        if outcome.previous_stage.is_some() {
            self.profiles.remove(&key);
        }
    }

    /// Registers a newly connected worker generation, synchronizes physical cordons/stale-build
    /// drains, refreshes local capacity facts, and performs a worker-driven dispatch scan.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::HandingOff`] if leadership handoff has started, or
    /// [`RegisterError::Registration`] on unknown fleet, invalid/duplicate worker id, or full slab.
    pub fn register(
        &mut self,
        now: Now,
        registration: WorkerRegistration,
        out: &mut Effects,
    ) -> Result<WorkerKey, RegisterError> {
        if !self.handoff.allows_registration() {
            return Err(RegisterError::HandingOff);
        }
        let worker_key = self.workers.register(
            now,
            &self.policy.expected_gang_version,
            self.handoff.is_active(),
            registration,
        )?;
        let affected = self
            .cordons
            .on_worker_registered(worker_key, &mut self.workers);
        self.emit_draining_updates(&affected, out);
        self.capacity.refresh_local(&self.workers);
        self.timers.arm(
            TimerSubject::Worker(worker_key),
            now.tick + self.policy.worker_timeout,
        );
        self.dispatch_for_worker(worker_key, now, out);
        Ok(worker_key)
    }

    /// Processes one inbound worker protocol event (`Accept`, `Decline`, `Executed`, `Completed`,
    /// `KeepAlive`, or `GoingAway`).
    pub fn on_worker(
        &mut self,
        now: Now,
        worker: WorkerKey,
        event: WorkerEvent,
        out: &mut Effects,
    ) {
        if self.workers.get(worker).is_none() {
            return;
        }
        self.refresh_worker_liveness(now, worker);

        match event {
            WorkerEvent::Accept { attempt_id } => {
                self.handle_accept(now, worker, &attempt_id, out);
            }
            WorkerEvent::Decline { attempt_id, reason } => {
                self.handle_decline(now, worker, &attempt_id, reason, out);
            }
            WorkerEvent::Executed(executed) => {
                self.handle_executed(now, worker, &executed, out);
            }
            WorkerEvent::Completed(completed) => {
                self.handle_completed(now, worker, completed, out);
            }
            WorkerEvent::KeepAlive {
                cache,
                memory_capacity,
            } => {
                let cleared_hold = self
                    .workers
                    .on_keepalive(worker, now, cache, memory_capacity);
                if memory_capacity.is_some() {
                    self.capacity.refresh_local(&self.workers);
                }
                if cleared_hold == Some(true) {
                    self.dispatch_for_worker(worker, now, out);
                }
            }
            WorkerEvent::GoingAway => {
                if self.workers.set_going_away(worker) {
                    out.push(Effect::SetWorkerDraining {
                        worker,
                        draining: true,
                    });
                    self.maybe_rehome_idle(worker, out);
                }
            }
        }
    }

    /// Handles worker stream disconnect: releases all attempts on `worker`, updates local capacity
    /// facts, and requeues or fails affected operations under `max_worker_loss_retries`.
    pub fn on_worker_lost(&mut self, now: Now, worker: WorkerKey, out: &mut Effects) {
        self.timers.disarm(TimerSubject::Worker(worker));
        if self.workers.disconnect(worker).is_none() {
            return;
        }
        self.capacity.refresh_local(&self.workers);
        let loss = self.attempts.on_worker_lost(
            worker,
            self.policy.actions.max_worker_loss_retries,
            &mut self.operations,
            &mut self.queue,
        );
        for attempt_key in loss.removed_attempts {
            self.timers.disarm_attempt(attempt_key);
        }
        for failed in loss.failed {
            self.cleanup_terminal_publish(&failed);
            self.push_publish(failed, out);
        }
        self.place_requeued(loss.requeued, now, out);
    }

    /// Fires all timers due at `now.tick` (ack timeouts, attempt deadlines, worker keepalive
    /// timeouts, and client-less expiries) and attempts placement for any requeued operations.
    pub fn on_timer(&mut self, now: Now, out: &mut Effects) {
        let effects = sweep_timers(
            now,
            self.policy.actions.max_worker_loss_retries,
            self.policy.client_action_timeout,
            &mut self.timers,
            &mut self.attempts,
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
        );

        let mut affected_workers = Vec::new();
        for kill in effects.kills {
            affected_workers.push(kill.worker);
            out.push(Effect::KillWorker {
                worker: kill.worker,
                attempt: kill.attempt_key,
                attempt_id: kill.attempt_id,
                reason: kill.reason,
            });
        }
        if !effects.timed_out_workers.is_empty() {
            self.capacity.refresh_local(&self.workers);
        }
        for worker in effects.timed_out_workers {
            out.push(Effect::CloseWorker { worker });
        }
        for dropped in effects.dropped_operations {
            self.profiles.remove(&dropped);
        }
        for publish in effects.publish {
            self.cleanup_terminal_publish(&publish);
            self.push_publish(publish, out);
        }
        self.place_requeued(effects.requeued_operations, now, out);
        for worker in affected_workers {
            self.maybe_rehome_idle(worker, out);
        }
    }

    /// Returns the earliest armed monotonic deadline, or `None` when idle.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Tick> {
        self.timers.next_deadline()
    }

    /// Sets or clears persistent physical drain intent for `(fleet, instance)` and synchronizes
    /// connected worker generations.
    ///
    /// Clearing drain intent (`draining == false`) is ignored once leadership handoff has started
    /// (`!handoff.allows_uncordon()`), and never clears a stale-build replacement cordon.
    ///
    /// # Errors
    ///
    /// Returns [`CordonError::UnknownFleet`] if `fleet` is not in the configured catalog.
    pub fn set_draining(
        &mut self,
        now: Now,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
        out: &mut Effects,
    ) -> Result<(), CordonError> {
        if !draining && !self.handoff.allows_uncordon() {
            return Ok(());
        }
        let affected = self
            .cordons
            .set_draining(fleet, instance, draining, &mut self.workers)?;
        self.emit_draining_updates(&affected, out);
        for worker_key in affected {
            if self
                .workers
                .get(worker_key)
                .is_some_and(|worker| !worker.drain.is_draining())
            {
                self.dispatch_for_worker(worker_key, now, out);
            }
        }
        Ok(())
    }

    /// Retains physical cordons present in `census`, pruning entries absent longer than `grace`.
    pub fn retain_drains(&mut self, now: Tick, grace: Duration, census: &Census) {
        self.cordons
            .retain_drains(now, grace, census, &mut self.workers);
    }

    /// Scans connected workers for stale builds and permanently cordons their physical instances.
    pub fn stale_build_drains(&mut self, out: &mut Effects) -> Vec<WorkerKey> {
        let affected = self.cordons.stale_build_drains(&mut self.workers);
        self.emit_draining_updates(&affected, out);
        affected
    }

    /// Begins or refreshes cooperative leadership handoff (`ADR-59`), freezing dispatch and
    /// registration, draining all connected workers, filling `into`, and emitting `RehomeWorker`
    /// for idle or deadline-expired workers.
    ///
    /// # Errors
    ///
    /// Returns [`HandOffError::MissingAttemptDeadline`] if a worker holds inflight attempts with no
    /// corresponding dispatch deadline.
    pub fn hand_off(
        &mut self,
        now: Now,
        public_complete: bool,
        into: &mut ControllerDrain,
        out: &mut Effects,
    ) -> Result<(), HandOffError> {
        self.handoff.hand_off(
            now.wall,
            &mut self.workers,
            public_complete,
            |worker| self.attempts.latest_attempt_deadline(worker),
            into,
            &mut self.handoff_scratch,
        )?;
        for &worker in &self.handoff_scratch.drain_workers {
            out.push(Effect::SetWorkerDraining {
                worker,
                draining: true,
            });
        }
        for &directive in &self.handoff_scratch.rehome {
            out.push(Effect::RehomeWorker {
                worker: directive.worker,
                reason: directive.reason,
            });
        }
        Ok(())
    }

    /// Projects current scheduler state into `into`, reusing its buffers (`§5.9`).
    pub fn snapshot(&mut self, now: Now, into: &mut SchedulerSnapshot) {
        let (durations, rates) = self.learned.durations_and_rates_mut();
        fill_snapshot(
            now,
            &self.operations,
            &self.queue,
            &self.workers,
            &self.cordons,
            &self.attempts,
            &self.capacity,
            durations,
            rates,
            into,
        );
    }

    /// Projects running and queued actions bounded by `limits` into `into`, reusing its buffers
    /// (`§5.9`).
    pub fn actions(&self, limits: ListingLimits, into: &mut ActionsView) {
        fill_actions(
            limits,
            &self.operations,
            &self.queue,
            &self.attempts,
            &self.workers,
            into,
        );
    }

    /// Returns the maximum encoded byte size of one persisted [`LearnedHistory`] record.
    #[must_use]
    pub fn learned_history_byte_limit(&self) -> NonZeroUsize {
        self.learned.byte_limit()
    }

    /// Snapshots all live learned resource and duration histories into `into` (`§5.6`).
    pub fn learned_history(&self, now: Timestamp, into: &mut Vec<LearnedHistory>) {
        self.learned.snapshot(now, into);
    }

    /// Restores compatible live learned resource and duration histories (`§5.6`).
    ///
    /// # Errors
    ///
    /// Returns [`SketchError`] if the resource sketch refuses the validated batch.
    pub fn restore_learned_history(
        &mut self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), SketchError> {
        self.learned.restore(now, records)
    }

    /// Replaces peer-local memory capacity observations, returning `true` if effective catalog
    /// capacity changed.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityError`] if any observation names an unknown fleet or is below `1 KiB`.
    pub fn set_peer_memory_observations(
        &mut self,
        observations: impl IntoIterator<Item = FleetMemoryCapacity>,
    ) -> Result<bool, CapacityError> {
        self.capacity.set_peer_observations(observations)
    }

    /// Recomputes local observed fleet memory capacities from connected workers, returning `true`
    /// if effective catalog capacity changed.
    pub fn refresh_local_memory_observations(&mut self) -> bool {
        self.capacity.refresh_local(&self.workers)
    }

    /// Returns a shared reference to the [`Learned`] state bundle.
    #[must_use]
    pub const fn learned(&self) -> &Learned {
        &self.learned
    }

    /// Consumes this [`Scheduler`] term and returns its [`Learned`] state for post-drain
    /// persistence (`§6.1`).
    #[must_use]
    pub fn into_learned(self) -> Learned {
        self.learned
    }

    fn refresh_worker_liveness(&mut self, now: Now, worker: WorkerKey) {
        if let Some(record) = self.workers.get_mut(worker) {
            record.last_seen = now;
        }
        self.timers.arm(
            TimerSubject::Worker(worker),
            now.tick + self.policy.worker_timeout,
        );
    }

    fn owns_attempt(&self, worker: WorkerKey, attempt_id: &AttemptId) -> Option<AttemptKey> {
        let key = self.attempts.lookup_id(attempt_id)?;
        let attempt = self.attempts.get(key)?;
        (attempt.worker == worker).then_some(key)
    }

    fn handle_accept(
        &mut self,
        now: Now,
        worker: WorkerKey,
        attempt_id: &AttemptId,
        out: &mut Effects,
    ) {
        if self.owns_attempt(worker, attempt_id).is_none() {
            return;
        }
        if let AcceptOutcome::Accepted {
            attempt_key,
            publish,
        } = self.attempts.on_accept(attempt_id, now, &self.operations)
        {
            self.timers.disarm(TimerSubject::Ack(attempt_key));
            if let Some(event) = publish {
                self.push_publish(event, out);
            }
        }
    }

    fn handle_decline(
        &mut self,
        now: Now,
        worker: WorkerKey,
        attempt_id: &AttemptId,
        reason: DeclineReason,
        out: &mut Effects,
    ) {
        if self.owns_attempt(worker, attempt_id).is_none() {
            return;
        }
        match self.attempts.on_decline(
            attempt_id,
            reason,
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
        ) {
            DeclineOutcome::Requeued {
                attempt_key,
                operation,
                ..
            } => {
                self.timers.disarm_attempt(attempt_key);
                out.push(Effect::NotifyWatchers {
                    operation,
                    event: OperationUpdate::Queued,
                });
                self.try_place_operation(operation, now, out);
            }
            DeclineOutcome::DroppedTerminal { attempt_key, .. } => {
                self.timers.disarm_attempt(attempt_key);
            }
            DeclineOutcome::Ignored => {}
        }
        self.maybe_rehome_idle(worker, out);
    }

    fn handle_executed(
        &mut self,
        now: Now,
        worker: WorkerKey,
        executed: &WorkerExecuted,
        out: &mut Effects,
    ) {
        let Some(attempt_key) = self.owns_attempt(worker, &executed.attempt_id) else {
            return;
        };
        let Some(op_key) = self.attempts.get(attempt_key).map(|a| a.operation) else {
            return;
        };
        let fallback = fallback_profile();
        let profile = self.profiles.get(&op_key).unwrap_or(&fallback);
        let report = ExecutedReport {
            attempt_id: &executed.attempt_id,
            now,
            usage: executed.usage,
            kill: executed.kill,
            side_channel_memory_failure: executed.side_channel_memory_failure,
            profile,
        };

        let outcome = self.attempts.on_executed(
            &report,
            &self.policy.actions,
            &self.capacity,
            self.learned.histories_mut().resource_mut(),
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
        );

        match outcome {
            ExecutedOutcome::AwaitingCompletion { attempt_key, .. }
            | ExecutedOutcome::ReleasedComputeOnly { attempt_key, .. } => {
                self.timers.disarm_attempt(attempt_key);
            }
            ExecutedOutcome::EscalatedRequeue {
                attempt_key,
                operation,
                ..
            } => {
                self.timers.disarm_attempt(attempt_key);
                out.push(Effect::NotifyWatchers {
                    operation,
                    event: OperationUpdate::Queued,
                });
                self.try_place_operation(operation, now, out);
            }
            ExecutedOutcome::EscalationFailed {
                attempt_key,
                operation,
                publish,
                ..
            } => {
                self.timers.disarm_attempt(attempt_key);
                self.timers.disarm(TimerSubject::ClientLess(operation));
                self.profiles.remove(&operation);
                self.push_publish(publish, out);
            }
            ExecutedOutcome::Ignored => return,
        }

        self.dispatch_for_worker(worker, now, out);
        self.maybe_rehome_idle(worker, out);
    }

    fn handle_completed(
        &mut self,
        now: Now,
        worker: WorkerKey,
        completed: WorkerCompleted,
        out: &mut Effects,
    ) {
        let Some(attempt_key) = self.owns_attempt(worker, &completed.attempt_id) else {
            return;
        };
        let started_at = self
            .attempts
            .get(attempt_key)
            .map_or(now.wall, |a| a.accepted_at.unwrap_or(a.dispatched_at).wall);
        let worker_id = self.workers.get(worker).map(|w| w.id.clone());

        let (durations, rates) = self.learned.durations_and_rates_mut();
        let outcome = self.attempts.on_completed(
            CompletedReport {
                attempt_id: &completed.attempt_id,
                now,
                status: completed.status,
                execution_timestamps: completed.execution_timestamps,
            },
            &self.policy.actions,
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
            durations,
            rates,
        );

        match outcome {
            CompletedOutcome::Completed {
                attempt_key,
                operation,
                worker,
                publish,
            } => {
                self.timers.disarm_attempt(attempt_key);
                self.timers.disarm(TimerSubject::ClientLess(operation));
                self.profiles.remove(&operation);
                self.push_publish(publish, out);
                if let (Some(op), Some(worker_id)) = (self.operations.get(operation), worker_id) {
                    out.push(Effect::RecordObservation(CompletedObservation {
                        operation,
                        operation_name: op.name.clone(),
                        action_digest: op.action_digest,
                        worker,
                        worker_id,
                        build: op.build,
                        target: op.target.clone(),
                        mnemonic: op.mnemonic.clone(),
                        queued_at: op.queued_since.wall,
                        started_at,
                        completed_at: now.wall,
                        execution_timestamps: completed.execution_timestamps,
                    }));
                }
            }
            CompletedOutcome::GenericRetry {
                attempt_key,
                operation,
                ..
            } => {
                self.timers.disarm_attempt(attempt_key);
                out.push(Effect::NotifyWatchers {
                    operation,
                    event: OperationUpdate::Queued,
                });
                self.try_place_operation(operation, now, out);
            }
            CompletedOutcome::Failed {
                attempt_key,
                operation,
                publish,
                ..
            } => {
                self.timers.disarm_attempt(attempt_key);
                self.timers.disarm(TimerSubject::ClientLess(operation));
                self.profiles.remove(&operation);
                self.push_publish(publish, out);
            }
            CompletedOutcome::ReleasedDiskOnly { attempt_key, .. } => {
                self.timers.disarm_attempt(attempt_key);
            }
            CompletedOutcome::Ignored => return,
        }

        self.dispatch_for_worker(worker, now, out);
        self.maybe_rehome_idle(worker, out);
    }

    fn place_requeued(&mut self, mut requeued: Vec<OperationKey>, now: Now, out: &mut Effects) {
        requeued.sort_unstable_by_key(|&op_key| {
            self.operations
                .get(op_key)
                .map_or((Reverse(i32::MIN), u64::MAX, op_key), |op| {
                    (Reverse(op.priority), op.seq, op_key)
                })
        });
        for op_key in requeued {
            out.push(Effect::NotifyWatchers {
                operation: op_key,
                event: OperationUpdate::Queued,
            });
            self.try_place_operation(op_key, now, out);
        }
    }

    fn try_place_operation(&mut self, op_key: OperationKey, now: Now, out: &mut Effects) -> bool {
        if !self.handoff.allows_dispatch() {
            return false;
        }
        let Some(assignment) = place_operation(
            op_key,
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
        ) else {
            return false;
        };
        self.commit_placed_assignment(assignment, now, out)
    }

    fn dispatch_for_worker(&mut self, worker_key: WorkerKey, now: Now, out: &mut Effects) {
        if !self.handoff.allows_dispatch() {
            return;
        }
        let mut batch = std::mem::take(&mut self.placement_scratch);
        scan_queue_for_worker(
            worker_key,
            self.policy.dispatch_scan_limit,
            &mut self.operations,
            &mut self.queue,
            &mut self.workers,
            &mut batch,
        );
        for assignment in batch.drain(..) {
            let _ = self.commit_placed_assignment(assignment, now, out);
        }
        self.placement_scratch = batch;
    }

    fn commit_placed_assignment(
        &mut self,
        assignment: PlacedAssignment,
        now: Now,
        out: &mut Effects,
    ) -> bool {
        let attempt_id = self.next_attempt_id();
        let Some(attempt_key) = self.attempts.dispatch(
            assignment,
            attempt_id.clone(),
            now,
            self.policy.dispatch_ack_timeout,
            self.policy.attempt_budget,
        ) else {
            let _ = self.workers.on_attempt_aborted(
                assignment.worker,
                assignment.reservation,
                crate::qos::is_gate(assignment.priority),
            );
            let _ = self
                .operations
                .requeue(assignment.operation, &mut self.queue);
            return false;
        };

        let Some(attempt) = self.attempts.get(attempt_key) else {
            return false;
        };
        let ack_deadline = attempt.ack_deadline;
        let attempt_deadline = attempt.attempt_deadline;
        let deadline_wall = attempt.deadline_wall;
        self.timers
            .arm_attempt(attempt_key, ack_deadline, attempt_deadline);

        out.push(Effect::AssignWorker {
            worker: assignment.worker,
            attempt: attempt_key,
            attempt_id,
            operation: assignment.operation,
            cpu_weight: assignment.cpu_weight,
            reservation: assignment.reservation,
            deadline: attempt_deadline,
            deadline_wall,
        });
        true
    }

    fn next_attempt_id(&mut self) -> AttemptId {
        let seq = self.next_attempt_seq;
        self.next_attempt_seq = self.next_attempt_seq.saturating_add(1);
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.policy.attempt_seed.to_be_bytes());
        bytes[8..].copy_from_slice(&seq.to_be_bytes());
        AttemptId::from_random_bytes(bytes)
    }

    fn emit_draining_updates(&self, affected: &[WorkerKey], out: &mut Effects) {
        for &worker_key in affected {
            if let Some(worker) = self.workers.get(worker_key) {
                out.push(Effect::SetWorkerDraining {
                    worker: worker_key,
                    draining: worker.drain.is_draining(),
                });
            }
        }
    }

    fn maybe_rehome_idle(&self, worker: WorkerKey, out: &mut Effects) {
        if !self.handoff.is_active() {
            return;
        }
        let has_non_terminal = !self.attempts.worker_attempts(worker).is_empty();
        if let Some(directive) =
            self.handoff
                .check_idle_rehome(worker, &self.workers, has_non_terminal)
        {
            out.push(Effect::RehomeWorker {
                worker: directive.worker,
                reason: directive.reason,
            });
        }
    }

    fn cleanup_terminal_publish(&mut self, publish: &PublishEvent) {
        match publish {
            PublishEvent::Failed { operation, .. }
            | PublishEvent::Cancelled { operation }
            | PublishEvent::Completed { operation } => {
                self.timers.disarm(TimerSubject::ClientLess(*operation));
                self.profiles.remove(operation);
            }
            PublishEvent::Executing { .. } => {}
        }
    }

    #[allow(
        clippy::unused_self,
        reason = "kept as a helper on Scheduler for uniform effect emission"
    )]
    fn push_publish(&self, event: PublishEvent, out: &mut Effects) {
        let (operation, update) = OperationUpdate::from_publish(event);
        out.push(Effect::NotifyWatchers {
            operation,
            event: update,
        });
    }
}

const fn fallback_profile() -> ActionProfile {
    ActionProfile {
        command_digest: String::new(),
        floors: ClientFloors {
            cpu_millicores: None,
            memory_kb: None,
            disk_kb: None,
        },
        desired_reservation: ActionResources {
            cpu_millicores: 0,
            memory: Bytes::new(0),
            disk: Bytes::new(0),
        },
        cpu_before_memory_fit: 0,
        fitted_fleet: 0,
        capacity_revision: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use gang_types::{
        BuildId, CpuWeight, Digest, FleetActionPolicy, FleetProperties, Isa, Mnemonic,
        NonZeroBytes, Resources, SourceId, TargetLabel, property,
    };

    use super::*;
    use crate::attempts::{KillReason, TerminalFailure};
    use crate::escalation::{EscalationFailureReason, KillResource};
    use crate::handoff::RehomeReason;
    use crate::operations::OperationStage;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn fleet_props(name: &str, mem_gib: u64, mem_cap_gib: Option<u64>) -> FleetProperties {
        let mut actions = FleetActionPolicy::default();
        actions.resources.memory = mem_cap_gib.map(NonZeroBytes::gib);
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(100),
            },
            &actions,
            std::iter::empty(),
        )
    }

    fn two_fleet_catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![
            fleet_props("small", 8, None),
            fleet_props("large", 32, None),
        ])
        .expect("catalog")
    }

    fn default_policy() -> SchedulerPolicy {
        let mut actions = ActionsConfig::default();
        actions.cold_start.cpu_millicores = NonZeroU32::new(4_000).expect("4000");
        actions.cold_start.memory = gang_config::WholeKib::from_units(4 * 1024 * 1024);
        actions.cold_start.disk = gang_config::WholeKib::from_units(10 * 1024 * 1024);
        SchedulerPolicy {
            actions,
            expected_gang_version: "0.3.9".to_owned(),
            ..SchedulerPolicy::default()
        }
    }

    fn make_scheduler(cat: &FleetCatalog, policy: SchedulerPolicy) -> Scheduler {
        let learned = Learned::new(&policy.actions, &policy.scaling, cat.len()).expect("learned");
        Scheduler::new(policy, cat, learned).expect("scheduler")
    }

    fn worker_reg(
        fleet: &str,
        instance: &str,
        seed: u8,
        cpu_millicores: u64,
        mem_gib: u64,
        disk_gib: u64,
    ) -> WorkerRegistration {
        WorkerRegistration {
            instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
            fleet: FleetName::try_new(fleet).expect("fleet"),
            gang_version: "0.3.9".to_owned(),
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(disk_gib),
            },
            ceilings: ActionResources {
                cpu_millicores: cpu_millicores / 2,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(disk_gib),
            },
            max_inflight: None,
            single_use: false,
            environment: Some("host:test".parse().expect("env")),
            uuid_bytes: [seed; 16],
        }
    }

    fn exec_req(name: &str, digest_seed: &[u8], priority: i32) -> AdmissionRequest {
        AdmissionRequest {
            name: OperationName::try_new(name).expect("op"),
            action_digest: Digest::from_bytes(digest_seed),
            command_digest: format!("cmd-{name}"),
            platform_properties: [(property::ISA.to_owned(), "x86-64".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: SourceId::from_identity_str("local:alice"),
            build: Some("12rsfjsg79g6h7f3".parse::<BuildId>().expect("build")),
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
            priority,
            initial_watchers: 1,
        }
    }

    fn extract_assign(effects: &Effects) -> Option<(WorkerKey, AttemptKey, AttemptId, CpuWeight)> {
        effects.iter().find_map(|e| match e {
            Effect::AssignWorker {
                worker,
                attempt,
                attempt_id,
                cpu_weight,
                ..
            } => Some((*worker, *attempt, attempt_id.clone(), *cpu_weight)),
            _ => None,
        })
    }

    fn finish_attempt_ok(
        sched: &mut Scheduler,
        worker: WorkerKey,
        attempt_id: AttemptId,
        t_start: u64,
        t_end: u64,
    ) {
        let mut out = Effects::new();
        sched.on_worker(
            now(t_start),
            worker,
            WorkerEvent::Accept {
                attempt_id: attempt_id.clone(),
            },
            &mut out,
        );
        sched.on_worker(
            now(t_end),
            worker,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: attempt_id.clone(),
                usage: AttemptUsage::default(),
                kill: None,
                side_channel_memory_failure: false,
            }),
            &mut out,
        );
        sched.on_worker(
            now(t_end + 1),
            worker,
            WorkerEvent::Completed(WorkerCompleted {
                attempt_id,
                status: CompletionStatus::Ok,
                execution_timestamps: Some((now(t_start).wall, now(t_end).wall)),
            }),
            &mut out,
        );
    }

    fn ok_usage() -> AttemptUsage {
        AttemptUsage {
            peak_memory: Some(Bytes::gib(1)),
            peak_disk: Some(Bytes::gib(1)),
            cpu_time: Duration::from_secs(2),
            wall_time: Duration::from_secs(2),
            oom_kills: 0,
        }
    }

    #[test]
    fn full_lifecycle_early_compute_release_dispatches_before_completed() {
        let cat = FleetCatalog::try_new(vec![fleet_props("small", 8, None)]).expect("catalog");
        let mut sched = make_scheduler(&cat, default_policy());
        let mut out = Effects::new();

        // Worker with 6 GiB memory: 4 GiB + 4 GiB > 6 GiB while running, so op2 waits until op1
        // reports Executed (freeing CPU+mem while keeping 10 GiB disk).
        let w = sched
            .register(
                now(1),
                worker_reg("small", "i-1", 1, 16_000, 6, 100),
                &mut out,
            )
            .expect("register");
        assert!(out.is_empty());

        let op1 = sched
            .execute(now(2), exec_req("operations/op1", b"op1", 250), &mut out)
            .expect("execute op1");
        assert!(
            out.iter()
                .any(|e| matches!(e, Effect::AuditGateTier(a) if a.priority == 250))
        );
        let (assigned_w, _, att1, weight1) = extract_assign(&out).expect("op1 assigned");
        assert_eq!(
            (assigned_w, weight1),
            (w, CpuWeight::for_priority_and_millicores(250, 4_000))
        );

        out.clear();
        let op2 = sched
            .execute(now(3), exec_req("operations/op2", b"op2", 100), &mut out)
            .expect("execute op2");
        assert_eq!(
            sched.operations().get(op2).expect("op2").stage,
            OperationStage::Queued
        );
        assert!(extract_assign(&out).is_none());

        out.clear();
        let accept = WorkerEvent::Accept {
            attempt_id: att1.clone(),
        };
        sched.on_worker(now(4), w, accept, &mut out);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::NotifyWatchers { operation, event: OperationUpdate::Executing { .. } }
                if *operation == op1
        )));

        // Executed on op1 frees compute early and dispatches op2 BEFORE Completed.
        out.clear();
        let executed = WorkerEvent::Executed(WorkerExecuted {
            attempt_id: att1.clone(),
            usage: ok_usage(),
            kill: None,
            side_channel_memory_failure: false,
        });
        sched.on_worker(now(6), w, executed, &mut out);
        let reserved = sched.workers().get(w).expect("worker").ledger.reserved();
        assert_eq!(
            (reserved.cpu_millicores, reserved.memory, reserved.disk),
            (4_000, Bytes::gib(4), Bytes::gib(20))
        );

        let (_, _, att2, weight2) = extract_assign(&out).expect("op2 dispatched on Executed");
        assert_eq!(weight2, CpuWeight::for_priority_and_millicores(100, 4_000));

        out.clear();
        let completed = WorkerEvent::Completed(WorkerCompleted {
            attempt_id: att1,
            status: CompletionStatus::Ok,
            execution_timestamps: Some((now(4).wall, now(6).wall)),
        });
        sched.on_worker(now(7), w, completed, &mut out);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::NotifyWatchers { operation, event: OperationUpdate::Completed }
                if *operation == op1
        )));
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::RecordObservation(obs) if obs.operation == op1 && obs.worker == w
        )));
        let disk_after = sched.workers().get(w).expect("w").ledger.reserved().disk;
        assert_eq!(disk_after, Bytes::gib(10));

        let mut snap = SchedulerSnapshot::default();
        sched.snapshot(now(8), &mut snap);
        assert_eq!((snap.completed, snap.source_rates.len()), (1, 1));
        assert_eq!(snap.executing[0].expected, Some(Duration::from_secs(2)));

        finish_attempt_ok(&mut sched, w, att2, 8, 9);
        assert_eq!(sched.attempts().completed_count(), 2);

        let learned = sched.into_learned();
        let mut records = Vec::new();
        learned.snapshot(now(11).wall, &mut records);
        assert!(!records.is_empty());
    }

    #[test]
    fn dedup_promotes_queued_op_before_worker_registers() {
        let cat = FleetCatalog::try_new(vec![fleet_props("small", 8, None)]).expect("catalog");
        let mut sched = make_scheduler(&cat, default_policy());
        let mut out = Effects::new();

        // Admit background opA and interactive opB while no workers exist.
        let op_a = sched
            .execute(now(1), exec_req("operations/a", b"same", 0), &mut out)
            .expect("op_a");
        let op_b = sched
            .execute(now(2), exec_req("operations/b", b"other", 100), &mut out)
            .expect("op_b");

        // Deduplicate onto op_a with gate priority 250 -> promotes op_a ahead of op_b!
        out.clear();
        let dedup = sched
            .execute(now(3), exec_req("operations/a-dup", b"same", 250), &mut out)
            .expect("dedup");
        assert_eq!(dedup, op_a);
        assert!(
            out.iter()
                .any(|e| matches!(e, Effect::AuditGateTier(a) if a.priority == 250))
        );

        let mut view = ActionsView::default();
        sched.actions(
            ListingLimits {
                running: 10,
                queued: 10,
            },
            &mut view,
        );
        assert_eq!(view.queued.len(), 2);
        assert_eq!(view.queued[0].info.op, "operations/a");
        assert_eq!(view.queued[0].info.priority, 250);
        assert_eq!(view.queued[1].info.op, "operations/b");

        // Register a 1-slot worker -> op_a dispatches first with CpuWeight::GATE!
        out.clear();
        let mut reg = worker_reg("small", "i-1", 1, 16_000, 8, 100);
        reg.max_inflight = Some(NonZeroU32::new(1).expect("1"));
        let _w = sched.register(now(4), reg, &mut out).expect("register");
        let (_, _, _, weight) = extract_assign(&out).expect("assigned");
        assert_eq!(weight, CpuWeight::for_priority_and_millicores(250, 4_000));
        assert_eq!(
            sched.operations().get(op_a).expect("op_a").stage,
            OperationStage::Running
        );
        assert_eq!(
            sched.operations().get(op_b).expect("op_b").stage,
            OperationStage::Queued
        );
    }

    #[test]
    fn oom_escalation_retries_on_larger_worker_and_fails_past_ceiling() {
        let cat = two_fleet_catalog();
        let mut sched = make_scheduler(&cat, default_policy());
        let mut out = Effects::new();

        // Register small (8 GiB) and large (32 GiB) workers.
        let w_small = sched
            .register(
                now(1),
                worker_reg("small", "i-small", 1, 16_000, 8, 100),
                &mut out,
            )
            .expect("small");
        let w_large = sched
            .register(
                now(1),
                worker_reg("large", "i-large", 2, 16_000, 32, 100),
                &mut out,
            )
            .expect("large");

        // Op fits preferred small fleet at 4 GiB cold start.
        let op = sched
            .execute(now(2), exec_req("operations/oom", b"oom", 100), &mut out)
            .expect("execute");
        let (w1, _, att1, _) = extract_assign(&out).expect("placed on small");
        assert_eq!(w1, w_small);

        // OOM kill with peak 12 GiB -> exceeds small (8 GiB), escalates to > 12 GiB -> lands on large!
        out.clear();
        sched.on_worker(
            now(3),
            w_small,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: att1,
                usage: AttemptUsage {
                    peak_memory: Some(Bytes::gib(12)),
                    oom_kills: 1,
                    ..AttemptUsage::default()
                },
                kill: None,
                side_channel_memory_failure: false,
            }),
            &mut out,
        );
        let (w2, _, att2, _) = extract_assign(&out).expect("escalated onto large");
        assert_eq!(w2, w_large);
        assert!(sched.operations().get(op).expect("op").reservation.memory > Bytes::gib(12));

        // Second OOM with peak 40 GiB > 32 GiB fleet ceiling -> terminal EscalationFailure!
        out.clear();
        sched.on_worker(
            now(4),
            w_large,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: att2,
                usage: AttemptUsage {
                    peak_memory: Some(Bytes::gib(40)),
                    oom_kills: 1,
                    ..AttemptUsage::default()
                },
                kill: None,
                side_channel_memory_failure: false,
            }),
            &mut out,
        );
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::NotifyWatchers {
                operation,
                event: OperationUpdate::Failed(TerminalFailure::Escalation(failure))
            } if *operation == op
                && failure.resource == KillResource::Memory
                && failure.reason == EscalationFailureReason::PeakExceedsCapacity
        )));
    }

    #[test]
    fn worker_loss_exhausts_retries_terminally() {
        let cat = FleetCatalog::try_new(vec![fleet_props("small", 8, None)]).expect("catalog");
        let mut policy = default_policy();
        policy.actions.max_worker_loss_retries = NonZeroU32::new(1).expect("1");
        let mut sched = make_scheduler(&cat, policy);
        let mut out = Effects::new();

        let w1 = sched
            .register(
                now(1),
                worker_reg("small", "i-1", 1, 16_000, 8, 100),
                &mut out,
            )
            .expect("w1");
        let op = sched
            .execute(now(2), exec_req("operations/loss", b"loss", 100), &mut out)
            .expect("execute");
        assert!(extract_assign(&out).is_some());

        // First loss requeues (1 retry allowed).
        out.clear();
        sched.on_worker_lost(now(3), w1, &mut out);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::NotifyWatchers {
                operation,
                event: OperationUpdate::Queued
            } if *operation == op
        )));
        assert_eq!(
            sched.operations().get(op).expect("op").stage,
            OperationStage::Queued
        );

        // Register second worker -> picks up op -> lose second worker -> exhausts budget!
        out.clear();
        let w2 = sched
            .register(
                now(4),
                worker_reg("small", "i-2", 2, 16_000, 8, 100),
                &mut out,
            )
            .expect("w2");
        assert!(extract_assign(&out).is_some());

        out.clear();
        sched.on_worker_lost(now(5), w2, &mut out);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::NotifyWatchers {
                operation,
                event: OperationUpdate::Failed(TerminalFailure::WorkerLossExhausted {
                    losses: 1,
                    ..
                })
            } if *operation == op
        )));
    }

    #[test]
    fn drain_retain_and_handoff_rehome_when_last_attempt_finishes() {
        let cat = FleetCatalog::try_new(vec![fleet_props("small", 16, None)]).expect("catalog");
        let mut sched = make_scheduler(&cat, default_policy());
        let mut out = Effects::new();

        let small = FleetName::try_new("small").expect("small");
        let i_busy = InstanceId::try_new("i-busy").expect("i-busy");
        let i_idle = InstanceId::try_new("i-idle").expect("i-idle");

        let w_busy = sched
            .register(
                now(1),
                worker_reg("small", "i-busy", 1, 16_000, 16, 100),
                &mut out,
            )
            .expect("w_busy");
        let w_idle = sched
            .register(
                now(1),
                worker_reg("small", "i-idle", 2, 16_000, 16, 100),
                &mut out,
            )
            .expect("w_idle");

        // Drain i-idle before handoff and verify retain_drains keeps it before grace and prunes
        // past grace.
        sched
            .set_draining(now(2), &small, &i_idle, true, &mut out)
            .expect("drain i-idle");
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::SetWorkerDraining {
                worker,
                draining: true
            } if *worker == w_idle
        )));
        let empty_census = Census::default();
        sched.retain_drains(now(3).tick, Duration::from_secs(10), &empty_census);
        assert!(sched.cordons().contains(0, &i_idle));
        sched.retain_drains(now(13).tick, Duration::from_secs(10), &empty_census);
        assert!(!sched.cordons().contains(0, &i_idle));

        // Keep w_idle drained so the submitted op lands deterministically on w_busy.
        sched
            .set_draining(now(14), &small, &i_idle, true, &mut out)
            .expect("drain i-idle");
        out.clear();
        sched
            .execute(now(15), exec_req("operations/run", b"run", 100), &mut out)
            .expect("execute");
        let (assigned, _, att, _) = extract_assign(&out).expect("assigned");
        assert_eq!(assigned, w_busy);

        // Start handoff -> freezes dispatch, drains workers, immediately rehomes w_idle.
        out.clear();
        let mut drain = ControllerDrain::default();
        sched
            .hand_off(now(16), true, &mut drain, &mut out)
            .expect("handoff");
        assert!(!drain.complete);
        assert_eq!(drain.workers.len(), 1);
        assert_eq!(drain.workers[0].instance, i_busy);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::RehomeWorker { worker, reason: RehomeReason::Idle } if *worker == w_idle
        )));
        assert!(
            !out.iter()
                .any(|e| matches!(e, Effect::RehomeWorker { worker, .. } if *worker == w_busy))
        );
        assert!(matches!(
            sched.execute(now(17), exec_req("operations/new", b"new", 100), &mut out),
            Err(ExecuteError::HandingOff)
        ));

        // Executed keeps disk on w_busy -> NOT rehomed until Completed finishes.
        out.clear();
        sched.on_worker(
            now(18),
            w_busy,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: att.clone(),
                usage: AttemptUsage::default(),
                kill: None,
                side_channel_memory_failure: false,
            }),
            &mut out,
        );
        assert!(
            !out.iter()
                .any(|e| matches!(e, Effect::RehomeWorker { worker, .. } if *worker == w_busy))
        );

        out.clear();
        sched.on_worker(
            now(19),
            w_busy,
            WorkerEvent::Completed(WorkerCompleted {
                attempt_id: att,
                status: CompletionStatus::Ok,
                execution_timestamps: None,
            }),
            &mut out,
        );
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::RehomeWorker { worker, reason: RehomeReason::Idle } if *worker == w_busy
        )));
    }

    #[test]
    fn timers_handle_ack_timeout_worker_timeout_and_client_less_expiry() {
        let cat = FleetCatalog::try_new(vec![fleet_props("small", 16, None)]).expect("catalog");
        let mut policy = default_policy();
        policy.dispatch_ack_timeout = Duration::from_secs(5);
        policy.worker_timeout = Duration::from_secs(10);
        policy.client_action_timeout = Duration::from_secs(15);
        let mut sched = make_scheduler(&cat, policy);
        let mut out = Effects::new();

        let w = sched
            .register(
                now(1),
                worker_reg("small", "i-1", 1, 16_000, 16, 100),
                &mut out,
            )
            .expect("register");
        assert_eq!(sched.next_deadline(), Some(now(11).tick));

        let op = sched
            .execute(now(2), exec_req("operations/ack", b"ack", 100), &mut out)
            .expect("execute");
        let (_, _, att, _) = extract_assign(&out).expect("assign");
        assert_eq!(sched.next_deadline(), Some(now(7).tick));

        // Ack timeout fires at t=7s -> kills attempt & requeues onto w.
        out.clear();
        sched.on_timer(now(7), &mut out);
        assert!(out.iter().any(|e| matches!(
            e,
            Effect::KillWorker {
                reason: KillReason::AckTimeout,
                ..
            }
        )));
        let (_, _, att2, _) = extract_assign(&out).expect("re-assigned");
        assert_ne!(att, att2);

        // Worker accepts att2 at t=8s, then client unwatches at t=9s.
        out.clear();
        sched.on_worker(
            now(8),
            w,
            WorkerEvent::Accept { attempt_id: att2 },
            &mut out,
        );
        sched.unwatch(now(9), op, &mut out);

        // Worker goes silent past t=18s -> worker_timeout closes worker & requeues op.
        out.clear();
        sched.on_timer(now(18), &mut out);
        assert!(
            out.iter()
                .any(|e| matches!(e, Effect::CloseWorker { worker } if *worker == w))
        );
        assert_eq!(
            sched.operations().get(op).expect("op").stage,
            OperationStage::Queued
        );

        // Client-less timeout expires queued op at t=24s (9s + 15s).
        out.clear();
        sched.on_timer(now(24), &mut out);
        assert!(sched.operations().get(op).is_none());
        assert!(sched.queue().is_empty());
    }
}
