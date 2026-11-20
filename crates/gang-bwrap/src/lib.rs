//! The `[sandbox.bwrap]` plugin: runs actions under bubblewrap.
//!
//! Each action runs in fresh namespaces with no host-network sharing.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use gang_bwrap::BwrapConfig;
//! use gang_types::AbsolutePathBuf;
//! use std::path::PathBuf;
//!
//! let config = BwrapConfig {
//!     root: AbsolutePathBuf::try_new("/etc/gang/action-rootfs")?,
//!     egress: None,
//!     program: PathBuf::from("/usr/bin/bwrap"),
//! };
//! assert!(config.egress.is_none());
//! # Ok(()) }
//! ```

mod bwrap;
mod command_line;
mod config;
#[cfg(target_os = "linux")]
mod egress;
#[cfg(target_os = "linux")]
mod egress_policy;
mod errors;
mod relay;

pub use bwrap::Bwrap;
pub use config::{BwrapConfig, EgressPrograms};
