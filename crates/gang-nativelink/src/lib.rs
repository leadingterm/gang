//! `NativeLink` scheduler plugin for Gang.
//!
//! `NativeLink` runs inside the `gang` process as a library. `gang server` runs the controller
//! (scheduler, content and action stores, and gRPC services over the connections it is handed)
//! and `gang worker` runs a worker that executes actions and reports to the controller over gRPC.
//!
//! # Limits
//!
//! One controller and one worker at a time per process; `run_worker` may be called again after it
//! returns. The controller's resource sketch snapshot task outlives its call. On Linux the worker
//! makes the process a child subreaper with one zombie reaper for the process's lifetime.
//! `globals::init` is process-wide: the first call decides the digest hash function and raises the
//! open-file soft limit for the whole process.
//!
//! ```
//! use gang_types::SchedulerName;
//!
//! assert!(SchedulerName::try_new("MAIN_SCHEDULER").is_ok());
//! ```

#![recursion_limit = "256"]

mod actions;
mod config;
mod controller;
mod counting;
mod enforcement;
#[cfg(test)]
mod equivalence;
mod errors;
mod gate;
mod globals;
mod open_file_limit;
mod placeable;
mod plugin;
mod properties;
mod serve;
mod settings;
mod snapshot;
mod stores;
mod whole_kib;
mod worker;

pub use config::{Http2Tuning, NativeLinkConfig, SchedulerTuning, WorkerTuning};
pub use enforcement::{
    ColdStartTuning, DiskEnforcement, DiskTuning, EscalationError, EscalationPercent,
    MemoryEnforcement, MemoryTuning,
};
pub use open_file_limit::{OpenFileLimit, OpenFileLimitError};
pub use plugin::NativeLink;
pub use whole_kib::{WholeError, WholeKib};
