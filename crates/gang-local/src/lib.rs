//! Local plugins for gang, serving every domain on a single machine.
//!
//! The crate provides three plugins, one per domain, each with its own configuration section:
//!
//! - [`Local`] (`[provisioner.local]`) is the `provisioner` domain: it runs the worker fleet as
//!   processes launched from the current executable, keeps their records and desired capacity
//!   in a state directory, serves scratch volumes and workload credentials, and runs a mock
//!   IMDS that workers query in place of a cloud metadata service.
//! - [`FilesystemCache`] (`[cache.filesystem]`) is the `cache` domain: a `NativeLink` filesystem
//!   store under one directory.
//! - [`FileSigner`] (`[signer.file]`) is the `signer` domain: a certificate authority key
//!   kept in one PKCS#8 file.
//!
//! Building a plugin validates its configuration only. Each role opens the resources it uses
//! on first use, so a plugin builds on every node that runs `gang`. The worker environment
//! variable is exported as a constant:
//!
//! ```
//! use gang_local::INSTANCE_ENV;
//! assert_eq!(INSTANCE_ENV, "GANG_LOCAL_INSTANCE");
//! ```

mod config;
mod errors;
mod file_signer;
mod filesystem_cache;
mod fleet;
mod imds;
mod imds_client;
mod jwt;
mod key_file;
mod layout;
mod plugin;
mod processes;
mod records;
mod scratch;
#[cfg(test)]
mod test_support;
mod worker_identity;

pub use config::{LocalConfig, LocalFleetOptions, LoopbackAddr, WorkerLauncher};
pub use errors::LocalError;
pub use file_signer::{FileSigner, FileSignerConfig};
pub use filesystem_cache::{FilesystemCache, FilesystemCacheConfig};
pub use plugin::Local;
pub use processes::{INSTANCE_ENV, INSTANCE_SECRET_ENV};
