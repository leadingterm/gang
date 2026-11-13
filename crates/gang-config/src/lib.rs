//! The typed `gang.toml`: everything that is the same for every plugin set, generic over the
//! plugin configuration types `gang_spi::compose!` generates.
//!
//! ```
//! use std::fmt;
//! use std::path::PathBuf;
//!
//! use gang_config::{
//!     ConfigError, FleetOptionsError, FleetOptionsTable, GangConfig,
//!     PluginSet, SandboxSelection,
//! };
//! use gang_types::AuthenticationSettings;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! #[serde(rename_all = "snake_case")]
//! enum Backend {
//!     Local {},
//! }
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
//! enum SandboxKey {
//!     #[serde(rename = "noop")]
//!     Noop,
//! }
//!
//! impl fmt::Display for SandboxKey {
//!     fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
//!         match self {
//!             Self::Noop => formatter.write_str("noop"),
//!         }
//!     }
//! }
//!
//! #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! struct Sandboxes {
//!     noop: Option<toml::Table>,
//! }
//!
//! impl SandboxSelection for Sandboxes {
//!     type Key = SandboxKey;
//!
//!     fn is_configured(&self, key: SandboxKey) -> bool {
//!         match key {
//!             SandboxKey::Noop => self.noop.is_some(),
//!         }
//!     }

//!     fn try_for_each_program<E>(
//!         &mut self,
//!         _key: SandboxKey,
//!         _visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
//!     ) -> Result<(), E> {
//!         Ok(())
//!     }
//! }
//!
//! impl AuthenticationSettings for Backend {}
//!
//! impl gang_types::SchedulerSettings for Backend {}
//!
//! #[derive(Debug)]
//! struct Plugins;
//!
//! impl PluginSet for Plugins {
//!     type Provisioner = Backend;
//!     type Cache = Backend;
//!     type Signer = Backend;
//!     type Scheduler = Backend;
//!     type Database = Backend;
//!     type SandboxKey = SandboxKey;
//!     type Sandbox = Sandboxes;
//!     type IdentityProvider = Backend;
//!     type ClientAuthentication = Backend;
//!     type WorkerAuthentication = Backend;
//!     type Limits = Backend;
//!     type FleetOptions = ();
//!
//!     fn fleet_options(_: &Backend, _: FleetOptionsTable) -> Result<(), FleetOptionsError> {
//!         Ok(())
//!     }
//!
//!     fn option_properties(_: &()) -> Vec<(&'static str, String)> {
//!         Vec::new()
//!     }
//! }
//!
//! let rejected = "poll_interval = \"1s\"".parse::<GangConfig<Plugins>>();
//! assert!(matches!(rejected, Err(ConfigError::Gang(_))));
//! ```

mod authentication_config;
mod ca_pin_setting;
mod client_secret;
mod dashboard_config;
mod endpoint;
mod errors;
mod execution_config;
mod fleet_policy;
mod fleets;
mod gang_config;
mod identity_config;
mod plugin_set;
mod rate_limit_config;
mod role_config;
mod sandbox_file;
mod server_config;
mod worker_config;

pub use authentication_config::AuthenticationConfig;
pub use ca_pin_setting::CaPinSetting;
pub use client_secret::{ClientSecretSource, SecretFileError};
pub use dashboard_config::{DashboardConfig, DashboardLogin};
pub use endpoint::ListenerUrlError;
pub use errors::{CaPinSettingError, ConfigError};
pub use execution_config::{ExecutionConfig, InputLinks};
pub use fleet_policy::{FleetPolicy, InstanceBounds, InstanceBoundsError};
pub use fleets::{FleetConfig, FleetMember, Fleets, RawFleet};
pub use gang_config::{Document, GangConfig};
pub use identity_config::IdentityConfig;
pub use plugin_set::{
    FleetOptionsError, FleetOptionsTable, PluginSet, SandboxSelection, validated_fleet_options,
};
pub use rate_limit_config::{
    BucketConfig, Ipv6PrefixLength, Ipv6PrefixLengthError, RateLimitConfig,
};
pub use role_config::{RoleConfig, RolePrincipal};
pub use sandbox_file::{SANDBOX_FILE_NAME, SandboxFile, SandboxFileError};
pub use server_config::{EndpointTrust, ServerConfig, ServerTransport, Transport};
pub use worker_config::WorkerConfig;
