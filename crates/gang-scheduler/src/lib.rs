//! Pure scheduling domain for Gang's native execution engine.
//!
//! Owns the deterministic state machines for queue admission, `QoS` mapping, resource fitting,
//! worker capacity ledger, placement, learned resource/duration estimation, escalation, cordons,
//! attempt tracking, pressure policy, and snapshot/activity projections for both controller and
//! worker roles.
//!
//! This crate uses only `std`, lower-tier workspace contracts, `serde`, and `thiserror`, with no
//! Tokio runtime, I/O, or clock reads.

mod cordons;
mod handoff;
mod ledger;
mod operations;
mod placement;
mod qos;
mod queue;
mod workers;

pub use cordons::{Census, CordonError, CordonRegistry};
pub use handoff::{ControllerHandOff, HandOffEffects, HandOffError, RehomeDirective, RehomeReason};
pub use ledger::{LedgerError, WorkerLedger, cmp_reserved_cpu_fraction, ledger_capacity};
pub use operations::{
    AdmitError, AdmitOutcome, ClientLessExpiry, DedupKey, NewOperation, Now, Operation,
    OperationKey, OperationStage, Operations, RetryCounts, UnwatchResult, canonical_platform_key,
};
pub use placement::{
    PlacedAssignment, compare_workers, is_eligible, place_operation, scan_queue_for_worker,
    select_worker_for_operation,
};
pub use qos::{
    GATE_PRIORITY_THRESHOLD, GateAudit, INTERACTIVE_PRIORITY_THRESHOLD, PriorityTier, classify,
    gate_audit, is_gate, weight,
};
pub use queue::{OperationQueue, QueueKey};
pub use workers::{
    DrainSources, InstanceState, RegistrationError, Worker, WorkerIdError, WorkerKey,
    WorkerRegistration, WorkerRegistry, mint_random_worker_id, mint_worker_id,
};
