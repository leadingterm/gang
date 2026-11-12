//! The `[limits.cgroup]` plugin: per-action cgroup v2 scopes (memory, CPU, pids) under the worker's
//! delegated subtree. Linux only.
//!
//! ```
//! use gang_cgroup::{Cgroup, CgroupConfig};
//! use gang_spi::{Plugin, PluginContext};
//! use std::path::Path;
//!
//! let context = PluginContext { config_path: Path::new("gang.toml") };
//! assert!(Cgroup::build(CgroupConfig::default(), &context).is_ok());
//! ```

mod cgroup;
mod config;
mod controller;
#[cfg(test)]
mod controller_tests;
mod errors;
mod fs;
mod join;
mod limits;
mod parse;
#[cfg(test)]
mod test_tree;

pub use cgroup::Cgroup;
pub use config::{CgroupConfig, CpuPeriod, CpuPeriodError, Delegation, OomGroup};
pub use controller::CgroupScope;
pub use errors::CgroupError;
pub use join::join;
