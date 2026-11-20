//! The `[sandbox.bwrap]` plugin: runs actions under bubblewrap.
//!
//! Each action runs in fresh namespaces with no view of the CAS, and without network unless the
//! plugin permits it and the action requests it.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use gang_bwrap::BwrapConfig;
//! use gang_types::AbsolutePathBuf;
//! use std::path::PathBuf;
//!
//! let config = BwrapConfig {
//!     root: AbsolutePathBuf::try_new("/etc/gang/action-rootfs")?,
//!     permit_network_property: false,
//!     program: PathBuf::from("/usr/bin/bwrap"),
//! };
//! assert!(!config.permit_network_property);
//! # Ok(()) }
//! ```

mod bwrap;
mod command_line;
mod config;
mod errors;
mod relay;

pub use bwrap::Bwrap;
pub use config::BwrapConfig;
