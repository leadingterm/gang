//! The `[sandbox.noop]` plugin: runs actions directly on the host with an isolated TMPDIR.
//!
//! ```
//! use gang_noop::{Noop, NoopConfig};
//! use gang_spi::{Plugin, PluginContext};
//! use std::path::Path;
//!
//! let config = NoopConfig { scratch_dir: None };
//! let context = PluginContext { config_path: Path::new("sandbox.toml") };
//! assert!(Noop::build(config, &context).is_ok());
//! ```

mod command;
mod config;
mod errors;
mod noop;

pub use config::NoopConfig;
pub use noop::Noop;
