//! Embedded `NativeLink` scheduler adapter for Gang.
//!
//! `NativeLink` runs inside the `gang` process as a library. `gang server` runs the controller
//! (scheduler, content and action stores, and gRPC services over the connections it is handed)
//! and `gang worker` runs a worker that executes actions and reports to the controller over gRPC.
//!
//! # Limits
//!
//! One controller and one worker at a time per adapter; either may run again after returning.
//! Learned resource reservations and durations survive controller terms and remain available
//! after drain for the final persistence flush. One resource sketch maintenance task outlives
//! each term. On Linux the worker makes the process a child subreaper with one zombie reaper
//! for the process's lifetime.
//! `globals::init` is process-wide: the first call decides the digest hash function and raises the
//! open-file soft limit for the whole process.
//! During controller drain, a separate refusal transport can finish retryable HTTP/gRPC
//! responses without publishing another controller or invoking authentication or API handlers.
//!
//! ```
//! use gang_config::WorkerConfig;
//! use gang_nativelink::NativeLinkConfig;
//!
//! let endpoint = "grpc://localhost:9002".parse().expect("valid URL");
//! let settings = NativeLinkConfig::for_worker(Some(endpoint), &WorkerConfig::default());
//! assert!(settings.worker_endpoint.is_some());
//! ```

#![recursion_limit = "256"]

mod actions;
mod config;
mod controller;
mod cordons;
mod durations;
#[cfg(test)]
mod equivalence;
mod errors;
mod fitted;
mod gate;
mod globals;
mod handoff;
#[cfg(test)]
mod handoff_tests;
mod history;
#[cfg(feature = "test-support")]
pub mod history_fixture;
mod observer;
mod observing;
mod placeable;
mod plugin;
mod properties;
mod refusals;
mod serve;
mod settings;
mod snapshot;
mod source_rates;
mod standing;
mod store_usage;
mod stores;
mod worker;

pub use config::NativeLinkConfig;
pub use plugin::NativeLink;
