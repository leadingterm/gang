//! The `[sandbox.bwrap]` plugin: runs actions under bubblewrap.
//!
//! Each action runs in fresh namespaces with no view of the CAS, and without network unless the
//! plugin permits it and the action requests it.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use gang_bwrap::{Bwrap, BwrapConfig};
//! use gang_spi::{Plugin, PluginContext};
//! use gang_types::AbsolutePathBuf;
//! use std::path::{Path, PathBuf};
//!
//! let config = BwrapConfig {
//!     readonly_bind_paths: vec![AbsolutePathBuf::try_new("/usr")?],
//!     readonly_bind_mounts: std::collections::BTreeMap::new(),
//!     permit_network_property: false,
//!     program: PathBuf::from("/usr/bin/bwrap"),
//!     tmp_path: AbsolutePathBuf::try_new("/tmp")?,
//! };
//! let context = PluginContext { config_path: Path::new("sandbox.toml") };
//! assert!(Bwrap::build(config, &context).is_ok());
//! # Ok(()) }
//! ```

mod bwrap;
mod command_line;
mod config;
mod errors;
mod relay;

pub use bwrap::Bwrap;
pub use config::BwrapConfig;
