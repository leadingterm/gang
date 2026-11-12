//! Worker-side pieces of `gang worker`.
//!
//! The crate runs one worker node: it mounts the scratch volume, lays out the
//! work, cache and identity directories, enrolls for a worker certificate when mutual TLS
//! is enabled, and runs the scheduler's worker loop. Credentials are refreshed
//! on a fraction of their lifetime, and the node stops when the provider signals
//! preemption. Everything provider-specific arrives through the ports in
//! [`WorkerDeployment`]; the crate itself has no provider dependencies.
//!
//! Cache budgets and credential refresh points are fractions of the volume
//! capacity and the certificate lifetime, set by the `fast_cas_percent` and
//! `certificate_refresh_percent` fields of `WorkerConfig`.

mod credentials;
mod deployment;
mod errors;
mod layout;
mod programs;
mod transport;
mod worker;

pub use credentials::{Credentials, EnrollError, Enroller, RateLimit};
pub use deployment::{WorkerDeployment, WorkerParts, WorkerSettings};
pub use errors::WorkerError;
pub use layout::{IdentityFiles, Layout};
pub use transport::{ServerCaError, ServerCaSource, Transport};
pub use worker::run;
