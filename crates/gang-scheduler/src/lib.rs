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
mod durations;
mod handoff;
mod history;
mod ledger;
mod operations;
mod placement;
mod pressure;
mod qos;
mod queue;
mod rates;
mod sketch;
mod workers;

pub use cordons::{Census, CordonError, CordonRegistry};
pub use durations::ActionDurations;
pub use handoff::{ControllerHandOff, HandOffEffects, HandOffError, RehomeDirective, RehomeReason};
pub use history::{Histories, history_id, resource_key};
pub use ledger::{LedgerError, WorkerLedger, cmp_reserved_cpu_fraction, ledger_capacity};
pub use operations::{
    AdmitError, AdmitOutcome, ClientLessExpiry, DedupKey, NewOperation, Now, Operation,
    OperationKey, OperationStage, Operations, RetryCounts, UnwatchResult, canonical_platform_key,
};
pub use placement::{
    PlacedAssignment, compare_workers, is_eligible, place_operation, scan_queue_for_worker,
    select_worker_for_operation,
};
pub use pressure::{
    DISK_EMERGENCY_FLOOR_PERCENT, DiskPressureMonitor, DiskSample, GRAVEYARD_CREDIT_CAP_PERCENT,
    MemoryPressureMonitor, PressureCandidate, QuiescenceGate, SoftCeilingMonitor, SoftVerdict,
    disk_free_percent, disk_under_pressure, effective_free_bytes, graveyard_credit,
    parse_cgroup_limit, parse_psi_total_us, pick_pressure_victim, pool_available_bytes,
    pool_shortage, reclaimable_bytes, soft_ceiling,
};
pub use qos::{
    GATE_PRIORITY_THRESHOLD, GateAudit, INTERACTIVE_PRIORITY_THRESHOLD, PriorityTier, classify,
    gate_audit, is_gate, weight,
};
pub use queue::{OperationQueue, QueueKey};
pub use rates::{RatesError, SourceRates, execution_timing};
pub use sketch::{
    Budget, DEFAULT_HEADROOM_PERCENT, DEFAULT_MEMORY_BUDGET_BYTES, DEFAULT_WINDOW_DAYS,
    ENTRY_BYTES, Entry, Family, HIST_BUCKETS, Half, HintRow, Hist, Observation,
    PROBATION_ENTRY_BYTES, Policy, ResourceSketch, SAMPLER_GENERATION, SHARDS, Shaped, SizeClass,
    SketchError, SketchKey, Snapshot, Stats,
};
pub use workers::{
    DrainSources, InstanceState, RegistrationError, Worker, WorkerIdError, WorkerKey,
    WorkerRegistration, WorkerRegistry, mint_random_worker_id, mint_worker_id,
};
