//! Controller actor lifecycle handle and term-spanning [`LearnedSlot`] (`§5.10`, `§6.1`).
//!
//! [`spawn_controller`] binds a [`Scheduler`] to a single Tokio task with 3 priority-biased
//! bounded inboxes and attaches its control sender to [`LearnedSlot`]. When the actor exits,
//! [`Scheduler::into_learned`] is stored back into [`LearnedSlot`] as `Retained` before any
//! receiver drops, guaranteeing post-drain persistence flushes never lose learned state.

use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use gang_clock::{Clock, Tick, Timestamp};
use gang_scheduler::{
    Census, ExecuteError, Learned, OperationKey, RegisterError, Scheduler, WorkerEvent, WorkerKey,
    WorkerRegistration,
};
use gang_spi::{
    ActionsView, ControllerDrain, FleetResourceCapacity, FleetSupply, LearnedHistory,
    ListingLimits, PortError, SchedulerSnapshot,
};
use gang_types::{FleetName, InstanceId, NonZeroDuration, OperationName};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::acquisition::Acquisition;
use crate::actor::{
    ActorConfig, ActorInboxes, ClientMsg, CompletionPayload, ControlMsg, ControllerRuntime,
    ExecuteSpec, OperationWatch, RegisteredWorker, WorkerMsg, run_actor,
};

/// Error returned when the controller actor has stopped and can no longer accept commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("controller actor is closed")]
pub struct ControllerClosed;

/// Errors returned by [`ControllerHandle::execute`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ControllerExecuteError {
    /// Controller actor has exited.
    #[error("controller actor is closed")]
    Closed,
    /// Scheduler rejected the execution request.
    #[error(transparent)]
    Execute(#[from] ExecuteError),
}

/// Errors returned by [`ControllerHandle::register_worker`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ControllerRegisterError {
    /// Controller actor has exited.
    #[error("controller actor is closed")]
    Closed,
    /// Scheduler rejected the worker registration.
    #[error(transparent)]
    Register(#[from] RegisterError),
}

#[derive(Debug)]
enum LearnedSlotState {
    Empty,
    Retained(Box<Learned>),
    Live(mpsc::Sender<ControlMsg>),
}

/// Thread-safe holder for controller learned state across actor terms (`§6.1`).
///
/// Holds `Retained(Learned)` before start and after actor exit, and `Live(control_tx)` while a
/// controller term is serving. The internal [`parking_lot::Mutex`] is never held across `.await`:
/// readers clone `Live(tx)`, release the lock, send their request, and on send/reply failure due
/// to a concurrent actor exit, re-lock and fall back to `Retained(learned)`.
#[derive(Debug, Clone)]
pub struct LearnedSlot {
    state: Arc<Mutex<LearnedSlotState>>,
    byte_limit: Arc<OnceLock<NonZeroUsize>>,
}

impl Default for LearnedSlot {
    fn default() -> Self {
        Self::empty()
    }
}

impl LearnedSlot {
    /// Creates an empty [`LearnedSlot`] before controller readiness.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            state: Arc::new(Mutex::new(LearnedSlotState::Empty)),
            byte_limit: Arc::new(OnceLock::new()),
        }
    }

    /// Creates a [`LearnedSlot`] holding a retained [`Learned`] bundle and records its byte limit.
    #[must_use]
    pub fn new(learned: Learned) -> Self {
        let byte_limit = Arc::new(OnceLock::new());
        let _ = byte_limit.set(learned.byte_limit());
        Self {
            state: Arc::new(Mutex::new(LearnedSlotState::Retained(Box::new(learned)))),
            byte_limit,
        }
    }

    /// Attaches a live actor control sender and ensures `byte_limit` is recorded.
    pub fn attach_live(&self, control_tx: mpsc::Sender<ControlMsg>, byte_limit: NonZeroUsize) {
        let _ = self.byte_limit.set(byte_limit);
        *self.state.lock() = LearnedSlotState::Live(control_tx);
    }

    /// Stores a [`Learned`] bundle as `Retained` (replacing any live sender).
    pub fn retain(&self, learned: Learned) {
        let _ = self.byte_limit.set(learned.byte_limit());
        *self.state.lock() = LearnedSlotState::Retained(Box::new(learned));
    }

    /// Takes a currently `Retained` [`Learned`] bundle so a new controller term can own it.
    #[must_use]
    pub fn take_retained(&self) -> Option<Learned> {
        let mut guard = self.state.lock();
        if matches!(*guard, LearnedSlotState::Retained(_)) {
            match std::mem::replace(&mut *guard, LearnedSlotState::Empty) {
                LearnedSlotState::Retained(learned) => Some(*learned),
                LearnedSlotState::Empty | LearnedSlotState::Live(_) => None,
            }
        } else {
            None
        }
    }

    /// Returns the synchronous maximum encoded byte size of one [`LearnedHistory`] record (`§6.1`).
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the controller has not yet initialized learned state.
    pub fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        self.byte_limit
            .get()
            .copied()
            .ok_or_else(|| PortError::unavailable("controller learned history is not initialized"))
    }

    /// Snapshots all live learned histories into `into`, handling live-to-retained exit races
    /// cleanly without holding any lock across `.await` (`§6.1`).
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained.
    pub async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        let live_tx = {
            let guard = self.state.lock();
            match &*guard {
                LearnedSlotState::Live(tx) => tx.clone(),
                LearnedSlotState::Retained(learned) => {
                    learned.snapshot(now, into);
                    return Ok(());
                }
                LearnedSlotState::Empty => {
                    return Err(PortError::unavailable(
                        "controller learned state is not initialized",
                    ));
                }
            }
        };

        let buffer = std::mem::take(into);
        let (reply_tx, reply_rx) = oneshot::channel();
        match live_tx
            .send(ControlMsg::LearnedHistory {
                now,
                into: buffer,
                reply: reply_tx,
            })
            .await
        {
            Ok(()) => {
                if let Ok(filled) = reply_rx.await {
                    *into = filled;
                    return Ok(());
                }
            }
            Err(err) => {
                if let ControlMsg::LearnedHistory { into: returned, .. } = err.0 {
                    *into = returned;
                }
            }
        }

        // Actor exited between cloning `live_tx` and receiving the reply: re-lock and read Retained.
        let guard = self.state.lock();
        match &*guard {
            LearnedSlotState::Retained(learned) => {
                learned.snapshot(now, into);
                Ok(())
            }
            LearnedSlotState::Live(_) | LearnedSlotState::Empty => Err(PortError::unavailable(
                "controller actor exited without retaining learned state",
            )),
        }
    }

    /// Restores compatible live learned histories, handling live-to-retained exit races cleanly
    /// without holding any lock across `.await` (`§6.1`).
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained, or
    /// [`PortError::Backend`] if the sketch rejects the batch.
    pub async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        let live_tx = {
            let mut guard = self.state.lock();
            match &mut *guard {
                LearnedSlotState::Live(tx) => tx.clone(),
                LearnedSlotState::Retained(learned) => {
                    return learned.restore(now, records).map_err(PortError::backend);
                }
                LearnedSlotState::Empty => {
                    return Err(PortError::unavailable(
                        "controller learned state is not initialized",
                    ));
                }
            }
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        if live_tx
            .send(ControlMsg::RestoreLearnedHistory {
                now,
                records: records.to_vec(),
                reply: reply_tx,
            })
            .await
            .is_ok()
            && let Ok(result) = reply_rx.await
        {
            return result.map_err(PortError::backend);
        }

        // Actor exited between cloning `live_tx` and receiving the reply: re-lock and restore Retained.
        let mut guard = self.state.lock();
        match &mut *guard {
            LearnedSlotState::Retained(learned) => {
                learned.restore(now, records).map_err(PortError::backend)
            }
            LearnedSlotState::Live(_) | LearnedSlotState::Empty => Err(PortError::unavailable(
                "controller actor exited without retaining learned state",
            )),
        }
    }
}

/// Cheaply cloneable async handle to a running controller actor (`§5.10`, `§6.1`).
///
/// Routes requests to the appropriate priority lane (`control_tx` > `worker_tx` > `client_tx`)
/// and delegates learned-history queries through [`LearnedSlot`].
#[derive(Clone)]
pub struct ControllerHandle {
    control_tx: mpsc::Sender<ControlMsg>,
    worker_tx: mpsc::Sender<WorkerMsg>,
    client_tx: mpsc::Sender<ClientMsg>,
    learned: LearnedSlot,
    clock: Arc<dyn Clock>,
    control_acquire_timeout: Option<NonZeroDuration>,
}

impl std::fmt::Debug for ControllerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControllerHandle")
            .field("learned", &self.learned)
            .field("control_acquire_timeout", &self.control_acquire_timeout)
            .finish_non_exhaustive()
    }
}

impl ControllerHandle {
    /// Returns a reference to the shared [`LearnedSlot`].
    #[must_use]
    pub const fn learned_slot(&self) -> &LearnedSlot {
        &self.learned
    }

    async fn reserve_control(&self) -> Result<mpsc::Permit<'_, ControlMsg>, PortError> {
        if let Some(bound) = self.control_acquire_timeout {
            let acquisition = Acquisition::new(self.clock.as_ref(), bound);
            acquisition
                .wait(self.control_tx.reserve())
                .await?
                .map_err(|_| PortError::unavailable(ControllerClosed))
        } else {
            self.control_tx
                .reserve()
                .await
                .map_err(|_| PortError::unavailable(ControllerClosed))
        }
    }

    // ---- Client-plane methods (WP D4: reapi_execution.rs) ----

    /// Admits or deduplicates a client `Execute` request and returns an [`OperationWatch`].
    ///
    /// # Errors
    ///
    /// Returns [`ControllerExecuteError::Closed`] if the actor has stopped, or
    /// [`ControllerExecuteError::Execute`] if admission was refused or handoff is active.
    pub async fn execute(
        &self,
        spec: ExecuteSpec,
    ) -> Result<OperationWatch, ControllerExecuteError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.client_tx
            .send(ClientMsg::Execute {
                spec: Box::new(spec),
                reply: reply_tx,
            })
            .await
            .map_err(|_| ControllerExecuteError::Closed)?;
        reply_rx
            .await
            .map_err(|_| ControllerExecuteError::Closed)?
            .map_err(ControllerExecuteError::Execute)
    }

    /// Attaches a watcher to an existing operation by canonical name.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn watch(
        &self,
        operation: OperationName,
    ) -> Result<Option<OperationWatch>, ControllerClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.client_tx
            .send(ClientMsg::Watch {
                operation,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ControllerClosed)?;
        reply_rx.await.map_err(|_| ControllerClosed)
    }

    /// Explicitly detaches one watcher from `operation`.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn unwatch(&self, operation: OperationKey) -> Result<(), ControllerClosed> {
        self.client_tx
            .send(ClientMsg::Unwatch { operation })
            .await
            .map_err(|_| ControllerClosed)
    }

    /// Cancels `operation` immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn cancel(&self, operation: OperationKey) -> Result<(), ControllerClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.client_tx
            .send(ClientMsg::Cancel {
                operation,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ControllerClosed)?;
        reply_rx.await.map_err(|_| ControllerClosed)
    }

    // ---- Worker-plane methods (WP D5: worker_service.rs) ----

    /// Registers a newly connected worker generation and binds its bounded outbound channel.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerRegisterError::Closed`] if the actor has stopped, or
    /// [`ControllerRegisterError::Register`] on unknown fleet, invalid ID, or active handoff.
    pub async fn register_worker(
        &self,
        registration: WorkerRegistration,
    ) -> Result<RegisteredWorker, ControllerRegisterError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.worker_tx
            .send(WorkerMsg::Register {
                registration: Box::new(registration),
                reply: reply_tx,
            })
            .await
            .map_err(|_| ControllerRegisterError::Closed)?;
        reply_rx
            .await
            .map_err(|_| ControllerRegisterError::Closed)?
            .map_err(ControllerRegisterError::Register)
    }

    /// Delivers one inbound worker protocol event (`Accept`, `Decline`, `Executed`, `Completed`,
    /// `KeepAlive`, or `GoingAway`).
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn worker_event(
        &self,
        worker: WorkerKey,
        event: WorkerEvent,
    ) -> Result<(), ControllerClosed> {
        self.worker_tx
            .send(WorkerMsg::Event {
                worker,
                event,
                completion: None,
            })
            .await
            .map_err(|_| ControllerClosed)
    }

    /// Delivers one inbound worker protocol event alongside a shell-side [`CompletionPayload`].
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn worker_event_with_completion(
        &self,
        worker: WorkerKey,
        event: WorkerEvent,
        completion: CompletionPayload,
    ) -> Result<(), ControllerClosed> {
        self.worker_tx
            .send(WorkerMsg::Event {
                worker,
                event,
                completion: Some(Box::new(completion)),
            })
            .await
            .map_err(|_| ControllerClosed)
    }

    /// Reports that `worker` disconnected.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has stopped.
    pub async fn worker_lost(&self, worker: WorkerKey) -> Result<(), ControllerClosed> {
        self.worker_tx
            .send(WorkerMsg::Lost { worker })
            .await
            .map_err(|_| ControllerClosed)
    }

    // ---- Control-plane methods (WP D6: engine.rs / SchedulerPort) ----

    /// Projects current scheduler state into `into`, reusing its buffers.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let buffer = std::mem::take(into);
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::Snapshot {
            into: buffer,
            reply: reply_tx,
        });
        *into = reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?;
        Ok(())
    }

    /// Projects running and queued actions bounded by `limits` into `into`, reusing its buffers.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn actions(
        &self,
        limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let buffer = std::mem::take(into);
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::Actions {
            limits,
            into: buffer,
            reply: reply_tx,
        });
        *into = reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?;
        Ok(())
    }

    /// Sets or clears persistent physical drain intent for `(fleet, instance)`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out, or
    /// [`PortError::Backend`] if `fleet` is unknown.
    pub async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::SetDraining {
            fleet: fleet.clone(),
            instance: instance.clone(),
            draining,
            reply: reply_tx,
        });
        reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?
            .map_err(PortError::backend)
    }

    /// Retains physical cordons present in `census`, pruning entries absent longer than `grace`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        census: Census,
    ) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::RetainDrains {
            now,
            grace,
            census,
            reply: reply_tx,
        });
        reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))
    }

    /// Builds a compact [`Census`] from borrowed provider supply snapshots and calls
    /// [`Self::retain_drains`].
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn retain_drains_from_supply(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError> {
        let census = Census::from_supply(live);
        self.retain_drains(now, grace, census).await
    }

    /// Scans connected workers for stale builds and permanently cordons their physical instances.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn stale_build_drains(&self) -> Result<Vec<WorkerKey>, PortError> {
        let permit = self.reserve_control().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::StaleBuildDrains { reply: reply_tx });
        reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))
    }

    /// Begins or refreshes cooperative leadership handoff, filling `into`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out, or
    /// [`PortError::Backend`] if drain computation fails.
    pub async fn hand_off(
        &self,
        public_complete: bool,
        into: &mut ControllerDrain,
    ) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let buffer = std::mem::take(into);
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::HandOff {
            public_complete,
            into: buffer,
            reply: reply_tx,
        });
        *into = reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?
            .map_err(PortError::backend)?;
        Ok(())
    }

    /// Refreshes drain progress into `into` without activating handoff if inactive.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out, or
    /// [`PortError::Backend`] if drain computation fails.
    pub async fn refresh_drain(
        &self,
        public_complete: bool,
        into: &mut ControllerDrain,
    ) -> Result<(), PortError> {
        let permit = self.reserve_control().await?;
        let buffer = std::mem::take(into);
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::RefreshDrain {
            public_complete,
            into: buffer,
            reply: reply_tx,
        });
        *into = reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?
            .map_err(PortError::backend)?;
        Ok(())
    }

    /// Replaces peer-local physical capacity observations.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out, or
    /// [`PortError::Backend`] if an observation is invalid.
    pub async fn set_peer_capacity_observations(
        &self,
        observations: Vec<FleetResourceCapacity>,
    ) -> Result<bool, PortError> {
        let permit = self.reserve_control().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::SetPeerCapacity {
            observations,
            reply: reply_tx,
        });
        reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))?
            .map_err(PortError::backend)
    }

    /// Recomputes local observed fleet physical capacities from connected workers.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if the actor has stopped or enqueueing times out.
    pub async fn refresh_local_capacity_observations(&self) -> Result<bool, PortError> {
        let permit = self.reserve_control().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        permit.send(ControlMsg::RefreshLocalCapacity { reply: reply_tx });
        reply_rx
            .await
            .map_err(|_| PortError::unavailable(ControllerClosed))
    }

    /// Returns the synchronous maximum encoded byte size of one [`LearnedHistory`] record.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if learned state has not been initialized.
    pub fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        self.learned.learned_history_byte_limit()
    }

    /// Snapshots all live learned histories via [`LearnedSlot`] (with post-exit fallback).
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained.
    pub async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        self.learned.learned_history(now, into).await
    }

    /// Restores compatible live learned histories via [`LearnedSlot`] (with post-exit fallback).
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Unavailable`] if no learned state is live or retained, or
    /// [`PortError::Backend`] if restoration fails.
    pub async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        self.learned.restore_learned_history(now, records).await
    }

    /// Cooperatively stops the actor, ensuring [`Scheduler::into_learned`] is stored in
    /// [`LearnedSlot`] before returning.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerClosed`] if the actor has already exited.
    pub async fn shutdown(&self) -> Result<(), ControllerClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control_tx
            .send(ControlMsg::Shutdown { reply: reply_tx })
            .await
            .map_err(|_| ControllerClosed)?;
        reply_rx.await.map_err(|_| ControllerClosed)
    }

    /// Exposes the raw `control_tx` channel for white-box bias tests.
    #[cfg(test)]
    pub(crate) fn control_sender(&self) -> mpsc::Sender<ControlMsg> {
        self.control_tx.clone()
    }

    /// Exposes the raw `worker_tx` channel for white-box bias tests.
    #[cfg(test)]
    pub(crate) fn worker_sender(&self) -> mpsc::Sender<WorkerMsg> {
        self.worker_tx.clone()
    }

    /// Exposes the raw `client_tx` channel for white-box bias tests.
    #[cfg(test)]
    pub(crate) fn client_sender(&self) -> mpsc::Sender<ClientMsg> {
        self.client_tx.clone()
    }
}

/// Spawns the single-owner controller actor on the current Tokio runtime (`§5.10`, `§6.1`).
///
/// Attaches the actor's `control_tx` and [`Scheduler::learned_history_byte_limit`] into
/// `learned_slot` before returning. Call [`ControllerHandle::shutdown`] to terminate the actor
/// cooperatively and retain its [`Learned`] bundle in `learned_slot`.
#[must_use]
pub fn spawn_controller<C>(
    scheduler: Scheduler,
    clock: C,
    config: ActorConfig,
    learned_slot: LearnedSlot,
    runtime: ControllerRuntime,
) -> (ControllerHandle, JoinHandle<()>)
where
    C: Clock + Send + Sync + 'static,
{
    let depth = config.scheduler_queue_depth.get();
    let (control_tx, control_rx) = mpsc::channel(depth);
    let (worker_tx, worker_rx) = mpsc::channel(depth);
    let (client_tx, client_rx) = mpsc::channel(depth);

    let byte_limit = scheduler.learned_history_byte_limit();
    learned_slot.attach_live(control_tx.clone(), byte_limit);

    let clock = Arc::new(clock);
    let clock_for_handle = Arc::clone(&clock);
    let dyn_clock: Arc<dyn Clock> = clock_for_handle;
    let control_acquire_timeout = runtime.control_acquire_timeout;

    let handle = ControllerHandle {
        control_tx,
        worker_tx,
        client_tx: client_tx.clone(),
        learned: learned_slot.clone(),
        clock: dyn_clock,
        control_acquire_timeout,
    };

    let inboxes = ActorInboxes {
        control_rx,
        worker_rx,
        client_rx,
        client_tx,
    };

    let task = tokio::spawn(run_actor(
        scheduler,
        clock,
        config,
        learned_slot,
        runtime,
        inboxes,
    ));

    (handle, task)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes as RawBytes;
    use gang_clock::{TestClock, Timestamp};
    use gang_config::{ActionsConfig, WholeKib};
    use gang_reapi::build::bazel::remote::execution::v2::ActionResult;
    use gang_reapi::leadingterm::gang::worker::v1 as proto;
    use gang_scheduler::{
        AdmissionRequest, AttemptUsage, CompletionStatus, Learned, OperationUpdate, Scheduler,
        SchedulerPolicy, TerminalFailure, WorkerCompleted, WorkerEvent, WorkerExecuted,
        WorkerRegistration,
    };
    use gang_spi::{Observation, ObservationSink, Origin, SchedulerSnapshot};
    use gang_types::{
        ActionResources, AttemptId, BuildId, BuildKey, Bytes, CpuWeight, Digest, FleetActionPolicy,
        FleetCatalog, FleetName, FleetProperties, Identity, InstanceId, InvocationId, Isa,
        Mnemonic, OperationName, Principal, ProviderName, Resources, SourceId, TargetLabel,
        property,
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::actor::{
        ActorConfig, ClientMsg, CompletionPayload, ControlMsg, ControllerRuntime, ExecuteSpec,
        InlineActionPayload, WorkerMsg, WorkerOutbound, select_inline_payload,
    };

    fn fleet_props(name: &str, mem_gib: u64) -> FleetProperties {
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
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )
    }

    fn single_fleet_catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet_props("small", 8)]).expect("catalog")
    }

    fn test_policy() -> SchedulerPolicy {
        let mut actions = ActionsConfig::default();
        actions.cold_start.cpu_millicores = NonZeroU32::new(2_000).expect("2000");
        actions.cold_start.memory = WholeKib::from_units(2 * 1024 * 1024);
        actions.cold_start.disk = WholeKib::from_units(4 * 1024 * 1024);
        SchedulerPolicy {
            actions,
            expected_gang_version: "0.3.9".to_owned(),
            dispatch_ack_timeout: Duration::from_secs(5),
            worker_timeout: Duration::from_secs(10),
            client_action_timeout: Duration::from_secs(15),
            attempt_budget: Duration::from_secs(30),
            ..SchedulerPolicy::default()
        }
    }

    fn make_scheduler(policy: SchedulerPolicy) -> Scheduler {
        let catalog = single_fleet_catalog();
        let learned =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
        Scheduler::new(policy, &catalog, learned).expect("scheduler")
    }

    fn worker_reg(instance: &str, seed: u8) -> WorkerRegistration {
        WorkerRegistration {
            instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
            fleet: FleetName::try_new("small").expect("fleet"),
            gang_version: "0.3.9".to_owned(),
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores: 16_000,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            ceilings: ActionResources {
                cpu_millicores: 8_000,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
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
            source: SourceId::from_identity_str("github-actions:alice"),
            build: Some("12rsfjsg79g6h7f3".parse::<BuildId>().expect("build")),
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
            priority,
            initial_watchers: 1,
        }
    }

    fn test_identity() -> Arc<Identity> {
        Arc::new(Identity::new(
            ProviderName::try_new("github-actions").expect("provider"),
            Principal::try_new("alice").expect("principal"),
        ))
    }

    fn test_origin(identity: Arc<Identity>) -> Origin {
        Origin {
            build: BuildKey {
                principal: identity,
                invocation: InvocationId::try_new("inv-1").expect("inv"),
            },
            correlated: None,
            tool: None,
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
        }
    }

    #[test]
    fn assign_inline_payload_is_all_or_nothing_at_limit() {
        let pair = InlineActionPayload {
            action: RawBytes::from_static(b"1234"),
            command: RawBytes::from_static(b"5678"),
        };
        let (a, c) = select_inline_payload(Some(&pair), Bytes::new(8));
        assert_eq!(a.as_deref(), Some(&b"1234"[..]));
        assert_eq!(c.as_deref(), Some(&b"5678"[..]));

        let (a, c) = select_inline_payload(Some(&pair), Bytes::new(7));
        assert!(a.is_none());
        assert!(c.is_none());

        let (a, c) = select_inline_payload(None, Bytes::new(64));
        assert!(a.is_none());
        assert!(c.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn three_lane_bias_prioritizes_control_and_worker_over_flooded_client_inbox() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let config = ActorConfig {
            scheduler_queue_depth: NonZeroUsize::new(64).expect("64"),
            scheduler_batch: NonZeroUsize::new(2).expect("2"),
            worker_outbound_depth: NonZeroUsize::new(32).expect("32"),
            assign_inline_max: Bytes::kib(64),
        };
        let (handle, task) = spawn_controller(
            make_scheduler(test_policy()),
            Arc::clone(&clock),
            config,
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );

        // Register a worker and dispatch one initial action so we have a live attempt to Accept.
        let mut worker = handle
            .register_worker(worker_reg("i-bias", 1))
            .await
            .expect("register");
        let mut watch = handle
            .execute(ExecuteSpec::new(exec_req(
                "operations/first",
                b"first",
                100,
            )))
            .await
            .expect("execute first");
        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected initial Assign");
        };
        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt id");

        // Flood client_rx to full capacity (64 messages) BEFORE yielding to the single-threaded
        // actor task, then enqueue a worker Accept and a control Snapshot while client_rx is full.
        let client_tx = handle.client_sender();
        let worker_tx = handle.worker_sender();
        let control_tx = handle.control_sender();

        let mut client_replies = Vec::with_capacity(64);
        for i in 0..64_u32 {
            let (reply_tx, reply_rx) = oneshot::channel();
            let mut req = exec_req(&format!("operations/flood-{i}"), &i.to_le_bytes(), 100);
            req.skip_cache_lookup = true;
            client_tx
                .try_send(ClientMsg::Execute {
                    spec: Box::new(ExecuteSpec::new(req)),
                    reply: reply_tx,
                })
                .expect("fill client_rx to capacity");
            client_replies.push(reply_rx);
        }
        // Prove client_rx is genuinely full.
        assert_eq!(client_tx.capacity(), 0);

        // Enqueue worker Accept and control Snapshot while client_rx is saturated.
        worker_tx
            .try_send(WorkerMsg::Event {
                worker: worker.key,
                event: WorkerEvent::Accept { attempt_id },
                completion: None,
            })
            .expect("enqueue worker accept");
        let (snap_tx, snap_rx) = oneshot::channel();
        control_tx
            .try_send(ControlMsg::Snapshot {
                into: SchedulerSnapshot::default(),
                reply: snap_tx,
            })
            .expect("enqueue snapshot");

        // Control snapshot MUST run BEFORE any of the 64 flooded client_rx Execute messages!
        let snap = snap_rx.await.expect("snapshot reply");
        assert_eq!(
            snap.queued.len(),
            0,
            "control_rx must preempt all 64 flooded client_rx messages"
        );
        assert_eq!(snap.executing.len(), 1);

        // Worker Accept MUST also run ahead of the client flood (delivering Executing update).
        let state = watch.changed().await.expect("executing update");
        assert!(matches!(state.update, OperationUpdate::Executing { .. }));

        // Confirm flooded client messages are eventually drained too.
        for reply in client_replies {
            let _ = reply.await.expect("flood reply").expect("admitted");
        }

        handle.shutdown().await.expect("shutdown");
        task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_worker_outbound_channel_evicts_stalled_worker_and_requeues_attempt() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let config = ActorConfig {
            worker_outbound_depth: NonZeroUsize::new(1).expect("1"),
            ..ActorConfig::default()
        };
        let (handle, task) = spawn_controller(
            make_scheduler(test_policy()),
            Arc::clone(&clock),
            config,
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );

        let mut stalled = handle
            .register_worker(worker_reg("i-stalled", 1))
            .await
            .expect("register stalled");

        // First Execute fills stalled worker's depth-1 outbound channel (we intentionally do NOT
        // read from `stalled.outbound`).
        let watch1 = handle
            .execute(ExecuteSpec::new(exec_req("operations/op1", b"op1", 100)))
            .await
            .expect("op1");
        // Second Execute tries to Assign onto `stalled`, hits `TrySendError::Full`, MUST NOT block
        // the actor, evicts `stalled` via `on_worker_lost`, and requeues both attempts!
        let watch2 = handle
            .execute(ExecuteSpec::new(exec_req("operations/op2", b"op2", 100)))
            .await
            .expect("op2");

        // Snapshot proves the actor remained responsive, evicted `stalled`, and requeued both ops.
        let mut snap = SchedulerSnapshot::default();
        handle.snapshot(&mut snap).await.expect("snapshot");
        assert!(snap.workers.is_empty(), "stalled worker must be evicted");
        assert_eq!(snap.queued.len(), 2, "both operations must be requeued");

        // Stalled worker's channel had 1 buffered Assign followed by EOF (sender dropped on eviction).
        assert!(matches!(
            stalled.outbound.recv().await,
            Some(WorkerOutbound::Assign(_))
        ));
        assert!(
            stalled.outbound.recv().await.is_none(),
            "evicted worker outbound channel must close"
        );

        // Register a healthy worker with max_inflight = 1 (matching depth 1) -> requeued attempt 1
        // dispatches immediately while attempt 2 stays queued.
        let mut healthy_reg = worker_reg("i-healthy", 2);
        healthy_reg.max_inflight = Some(NonZeroU32::new(1).expect("1"));
        let mut healthy = handle
            .register_worker(healthy_reg)
            .await
            .expect("register healthy");
        let a1 = healthy.outbound.recv().await.expect("requeued assign 1");
        assert!(matches!(a1, WorkerOutbound::Assign(_)));
        handle
            .snapshot(&mut snap)
            .await
            .expect("snapshot after healthy");
        assert_eq!(snap.workers.len(), 1);
        assert_eq!(snap.executing.len(), 1);
        assert_eq!(snap.queued.len(), 1);

        // Both watchers still observe Queued (no spurious failure).
        assert_eq!(watch1.current().update, OperationUpdate::Queued);
        assert_eq!(watch2.current().update, OperationUpdate::Queued);
        let _ = (watch1.close().await, watch2.close().await);

        handle.shutdown().await.expect("shutdown");
        task.await.expect("actor joined");
    }

    async fn complete_attempt_ok(
        handle: &ControllerHandle,
        clock: &TestClock,
        worker: WorkerKey,
        attempt_id: AttemptId,
        watch: &mut OperationWatch,
        exec_secs: u64,
        completion: Option<CompletionPayload>,
    ) -> crate::actor::OperationWatchState {
        let exec_start = clock.wall();
        handle
            .worker_event(
                worker,
                WorkerEvent::Accept {
                    attempt_id: attempt_id.clone(),
                },
            )
            .await
            .expect("accept");
        let executing = watch.changed().await.expect("executing update");
        assert!(matches!(
            executing.update,
            OperationUpdate::Executing {
                attempt_id: ref id,
                worker: w,
            } if id == &attempt_id && w == worker
        ));

        clock.advance(Duration::from_secs(exec_secs));
        let exec_end = clock.wall();
        handle
            .worker_event(
                worker,
                WorkerEvent::Executed(WorkerExecuted {
                    attempt_id: attempt_id.clone(),
                    usage: AttemptUsage {
                        peak_window_cpu_millicores: None,
                        peak_memory: Some(Bytes::mib(256)),
                        peak_disk: Some(Bytes::mib(32)),
                        cpu_time: Duration::from_secs(exec_secs),
                        wall_time: Duration::from_secs(exec_secs),
                        oom_kills: 0,
                    },
                    kill: None,
                    side_channel_memory_failure: false,
                    deadline_exceeded: false,
                }),
            )
            .await
            .expect("executed");
        clock.advance(Duration::from_secs(1));
        let event = WorkerEvent::Completed(WorkerCompleted {
            attempt_id,
            status: CompletionStatus::Ok,
            execution_timestamps: Some((exec_start, exec_end)),
        });
        match completion {
            Some(payload) => handle
                .worker_event_with_completion(worker, event, payload)
                .await
                .expect("completed"),
            None => handle.worker_event(worker, event).await.expect("completed"),
        }
        watch.changed().await.expect("completed update")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn learned_slot_serves_live_retained_and_exit_race_fallback() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(10_000)));
        let policy = test_policy();
        let catalog = single_fleet_catalog();
        let learned =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
        let expected_limit = learned.byte_limit();
        let slot = LearnedSlot::new(learned);
        assert_eq!(
            slot.learned_history_byte_limit().expect("pre-start limit"),
            expected_limit
        );

        let mut records = Vec::new();
        slot.learned_history(clock.wall(), &mut records)
            .await
            .expect("retained read");
        assert!(records.is_empty());

        let sched = Scheduler::new(
            policy,
            &catalog,
            slot.take_retained().expect("take retained"),
        )
        .expect("scheduler");
        let (handle, task) = spawn_controller(
            sched,
            Arc::clone(&clock),
            ActorConfig::default(),
            slot.clone(),
            ControllerRuntime::new(),
        );

        let mut worker = handle
            .register_worker(worker_reg("i-learn", 1))
            .await
            .expect("register");
        let mut watch = handle
            .execute(ExecuteSpec::new(exec_req(
                "operations/learn",
                b"learn",
                100,
            )))
            .await
            .expect("execute");
        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt id");
        let done =
            complete_attempt_ok(&handle, &clock, worker.key, attempt_id, &mut watch, 2, None).await;
        assert_eq!(done.update, OperationUpdate::Completed);

        slot.learned_history(clock.wall(), &mut records)
            .await
            .expect("live read");
        assert!(
            !records.is_empty(),
            "live learned history must be populated"
        );
        let live_records = records.clone();

        // Exit-race test: queue Shutdown ahead of LearnedHistory so `LearnedSlot::learned_history`
        // clones `Live(tx)` right before Shutdown exits the actor and drops `control_rx`.
        let control_tx = handle.control_sender();
        let (shut_tx, shut_rx) = oneshot::channel();
        control_tx
            .try_send(ControlMsg::Shutdown { reply: shut_tx })
            .expect("queue shutdown");

        let mut race_records = Vec::new();
        let (race_res, shut_res) = tokio::join!(
            slot.learned_history(clock.wall(), &mut race_records),
            shut_rx
        );
        shut_res.expect("shutdown acknowledged");
        race_res.expect("exit-race fallback must succeed");
        assert_eq!(race_records, live_records);
        task.await.expect("actor joined");

        // Post-exit Retained read and restore both continue working after the actor has exited.
        let mut post_records = Vec::new();
        slot.learned_history(clock.wall(), &mut post_records)
            .await
            .expect("post-exit retained read");
        assert_eq!(post_records, live_records);
        slot.restore_learned_history(clock.wall(), &post_records)
            .await
            .expect("post-exit restore");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clock_drives_ack_timeout_attempt_deadline_and_client_less_expiry() {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000)));
        let mut policy = test_policy();
        policy.actions.max_worker_loss_retries = NonZeroU32::new(1).expect("1");
        policy.dispatch_ack_timeout = Duration::from_secs(5);
        policy.attempt_budget = Duration::from_secs(20);
        policy.client_action_timeout = Duration::from_secs(8);
        policy.worker_timeout = Duration::from_mins(1);

        let (handle, task) = spawn_controller(
            make_scheduler(policy),
            Arc::clone(&clock),
            ActorConfig::default(),
            LearnedSlot::empty(),
            ControllerRuntime::new(),
        );

        let mut worker = handle
            .register_worker(worker_reg("i-timers", 1))
            .await
            .expect("register");

        // 1. Ack timeout: dispatch op_ack and never send Accept.
        let mut watch_ack = handle
            .execute(ExecuteSpec::new(exec_req("operations/ack", b"ack", 100)))
            .await
            .expect("exec ack");
        let Some(WorkerOutbound::Assign(first_assign)) = worker.outbound.recv().await else {
            panic!("expected Assign for op_ack");
        };

        // Advance past dispatch_ack_timeout (5s): actor sends Kill(Cancelled) and retries once.
        clock.advance(Duration::from_secs(5));
        let kill = worker.outbound.recv().await.expect("ack timeout kill");
        assert!(matches!(
            kill,
            WorkerOutbound::Kill(proto::Kill { attempt, reason })
                if attempt == first_assign.attempt && reason == proto::KillReason::Cancelled as i32
        ));
        let Some(WorkerOutbound::Assign(retry_assign)) = worker.outbound.recv().await else {
            panic!("expected retry Assign after ack timeout");
        };
        assert_ne!(retry_assign.attempt, first_assign.attempt);

        // 2. Attempt deadline: Accept the retry, then advance past attempt_budget (20s).
        let retry_id = retry_assign.attempt.parse().expect("retry id");
        handle
            .worker_event(
                worker.key,
                WorkerEvent::Accept {
                    attempt_id: retry_id,
                },
            )
            .await
            .expect("accept retry");
        let executing = watch_ack.changed().await.expect("executing");
        assert!(matches!(
            executing.update,
            OperationUpdate::Executing { .. }
        ));

        clock.advance(Duration::from_secs(20));
        let deadline_kill = worker.outbound.recv().await.expect("deadline kill");
        assert!(matches!(
            deadline_kill,
            WorkerOutbound::Kill(proto::Kill { attempt, reason })
                if attempt == retry_assign.attempt
                    && reason == proto::KillReason::DeadlineExceeded as i32
        ));
        let failed = watch_ack.changed().await.expect("deadline failed");
        assert!(matches!(
            failed.update,
            OperationUpdate::Failed(TerminalFailure::DeadlineExceeded { .. })
        ));

        // 3. Client-less expiry: execute op_orphan, accept it, then drop its OperationWatch!
        let watch_orphan = handle
            .execute(ExecuteSpec::new(exec_req(
                "operations/orphan",
                b"orphan",
                100,
            )))
            .await
            .expect("exec orphan");
        let Some(WorkerOutbound::Assign(orphan_assign)) = worker.outbound.recv().await else {
            panic!("expected Assign for orphan");
        };
        let orphan_id = orphan_assign.attempt.parse().expect("orphan id");
        handle
            .worker_event(
                worker.key,
                WorkerEvent::Accept {
                    attempt_id: orphan_id,
                },
            )
            .await
            .expect("accept orphan");

        // Dropping OperationWatch automatically sends Unwatch -> arms client_action_timeout (8s).
        drop(watch_orphan);
        // Yield once via a snapshot so Unwatch is guaranteed processed before advancing clock.
        let mut snap = SchedulerSnapshot::default();
        handle.snapshot(&mut snap).await.expect("sync unwatch");

        clock.advance(Duration::from_secs(8));
        let orphan_kill = worker.outbound.recv().await.expect("client-less kill");
        assert!(matches!(
            orphan_kill,
            WorkerOutbound::Kill(proto::Kill { attempt, reason })
                if attempt == orphan_assign.attempt
                    && reason == proto::KillReason::Cancelled as i32
        ));

        handle.shutdown().await.expect("shutdown");
        task.await.expect("actor joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gate_tier_end_to_end_assign_accept_executed_completed_delivers_stream_and_observation()
    {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(5_000)));
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::new(8).expect("8"), NonZeroUsize::MIN);
        let runtime = ControllerRuntime {
            observations: Some(sink),
            handoff: None,
            control_acquire_timeout: None,
            invalidate_existence: None,
        };
        let (handle, task) = spawn_controller(
            make_scheduler(test_policy()),
            Arc::clone(&clock),
            ActorConfig::default(),
            LearnedSlot::empty(),
            runtime,
        );

        let mut worker = handle
            .register_worker(worker_reg("i-e2e", 7))
            .await
            .expect("register");

        let caller = test_identity();
        let origin = test_origin(Arc::clone(&caller));
        let spec = ExecuteSpec {
            request: exec_req("operations/gate-e2e", b"gate-e2e", 250),
            caller: Some(caller),
            origin: Some(origin),
            inline: Some(InlineActionPayload {
                action: RawBytes::from_static(b"action-proto"),
                command: RawBytes::from_static(b"command-proto"),
            }),
            timeout: Some(Duration::from_secs(42)),
        };

        let mut watch = handle.execute(spec).await.expect("execute");
        assert_eq!(watch.name().as_str(), "operations/gate-e2e");
        assert_eq!(watch.current().update, OperationUpdate::Queued);

        // Verify Assign frame carries inline pair, cpu_weight == GATE, and keeps platform clean.
        let Some(WorkerOutbound::Assign(assign)) = worker.outbound.recv().await else {
            panic!("expected Assign");
        };
        assert_eq!(assign.operation, "operations/gate-e2e");
        assert_eq!(
            assign.cpu_weight,
            CpuWeight::for_priority_and_millicores(250, 2_000).get()
        );
        assert_eq!(assign.action.as_deref(), Some(&b"action-proto"[..]));
        assert_eq!(assign.command.as_deref(), Some(&b"command-proto"[..]));
        assert_eq!(assign.timeout.as_ref().map(|d| d.seconds), Some(42));
        assert!(
            !assign.platform.contains_key("cpu_weight"),
            "cpu_weight must never enter Assign.platform"
        );

        let attempt_id: AttemptId = assign.attempt.parse().expect("attempt id");
        let action_result = ActionResult {
            exit_code: 0,
            stdout_raw: RawBytes::from_static(b"ok"),
            ..ActionResult::default()
        };
        let completed = complete_attempt_ok(
            &handle,
            &clock,
            worker.key,
            attempt_id,
            &mut watch,
            3,
            Some(CompletionPayload {
                result: Some(action_result.clone()),
                status: None,
                exit_code: 0,
                error: false,
            }),
        )
        .await;
        assert_eq!(completed.update, OperationUpdate::Completed);
        assert_eq!(completed.result, Some(action_result));
        assert!(completed.is_terminal());

        let delivery = observations.next().await.expect("recorded observation");
        let Some(Observation::Executed(obs)) = delivery.into_observation() else {
            panic!("expected Executed observation");
        };
        assert_eq!(obs.worker, Some(worker.worker_id));
        assert_eq!(obs.exit_code, 0);
        assert!(!obs.error);
        assert_eq!(
            obs.times.executing.map(|p| p.duration()),
            Some(Duration::from_secs(3))
        );

        handle.shutdown().await.expect("shutdown");
        task.await.expect("actor joined");
    }
}
