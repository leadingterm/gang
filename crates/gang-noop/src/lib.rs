//! The `[sandbox.noop]` plugin: runs actions directly on the host with an isolated TMPDIR.
//!
//! ```
//! use gang_noop::{Noop, NoopConfig};
//! use gang_spi::{Plugin, PluginContext};
//! use std::path::Path;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! let config = NoopConfig { environment: "host:example".parse()?, scratch_dir: None };
//! let context = PluginContext { config_path: Path::new("sandbox.toml") };
//! assert!(Noop::build(config, &context).is_ok());
//! # Ok(())
//! # }
//! ```

mod command;
mod config;
mod errors;
mod noop;

pub use config::NoopConfig;
pub use noop::Noop;
