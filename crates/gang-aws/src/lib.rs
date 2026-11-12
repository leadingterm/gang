//! AWS plugins for Gang: EC2 Spot worker fleets, an S3 build cache, and a KMS certificate signer.
//!
//! The crate provides three plugins, each selected by its table in `gang.toml`:
//!
//! - [`Aws`] is the provisioner, configured by `[provisioner.aws]` ([`AwsConfig`]). It runs the
//!   worker fleet as an Auto Scaling group and serves the node lifecycle, scratch volume and
//!   workload credential ports.
//! - [`S3Cache`] is the cache, configured by `[cache.s3]` ([`S3CacheConfig`]).
//! - [`KmsSigner`] is the certificate signer, configured by `[signer.kms]`
//!   ([`KmsSignerConfig`]).
//!
//! A configuration is validated when it is deserialized; a value that parses is valid, and
//! building a plugin checks nothing more. AWS clients are opened on first use, so a node that
//! never calls a port makes no AWS request. Credentials always come
//! from the SDK default provider chain, usually the instance role; they are never read from
//! configuration.
//!
//! Configuration is deserialized strictly: unknown keys are rejected, and every optional key has
//! a default. A valid provisioner section needs only a region and a scratch mount point:
//!
//! ```
//! use gang_aws::AwsConfig;
//! use serde_json::json;
//!
//! let config: AwsConfig = serde_json::from_value(json!({
//!     "region": "eu-north-1",
//!     "scratch_mount_point": "/mnt/scratch",
//! }))?;
//! assert_eq!(config.region.as_str(), "eu-north-1");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod cache;
mod client_settings;
mod config;
mod credential;
mod errors;
mod fallback;
mod fleet;
mod group;
mod lifecycle;
mod names;
mod plugin;
mod scratch;
mod sdk;
mod signer;

#[cfg(test)]
mod fake_aws;

#[cfg(test)]
mod fallback_scenarios;

#[cfg(test)]
mod fleet_harness;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod warm_scenarios;

pub use cache::S3Cache;
pub use client_settings::{ClientSettings, ClientSettingsError};
pub use config::{
    AwsConfig, AwsFleetOptions, Capacity, CooldownPolicy, ExpiryAge, KmsSignerConfig,
    RetrySpotAfter, S3CacheConfig, S3Tuning, SpotFallback, WorkloadTokenAlgorithm,
};
pub use errors::{AwsError, ScratchError};
pub use names::{AwsNameError, BucketName, FleetTagKey, KeyId, KeyPrefix, MountPoint, Region};
pub use plugin::Aws;
pub use signer::KmsSigner;
