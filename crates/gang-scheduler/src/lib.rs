//! Pure scheduling domain for Gang's native execution engine.
//!
//! Owns the deterministic state machines for queue admission, `QoS` mapping, resource fitting,
//! worker capacity ledger, placement, learned resource/duration estimation, escalation, cordons,
//! attempt tracking, pressure policy, and snapshot/activity projections for both controller and
//! worker roles.
//!
//! This crate uses only `std`, lower-tier workspace contracts, `serde`, and `thiserror`, with no
//! Tokio runtime, I/O, or clock reads.
