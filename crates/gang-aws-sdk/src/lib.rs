//! Shared AWS SDK configuration and error classification for Gang adapters.
//!
//! ```
//! use gang_aws_sdk::Region;
//! let region = Region::try_new("eu-north-1")?;
//! assert_eq!(region.as_str(), "eu-north-1");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod client_settings;
mod errors;
mod region;
mod sdk;

pub use client_settings::{ClientSettings, ClientSettingsError};
pub use errors::{CallError, Retryability, call_error, classify};
pub use region::{Region, RegionError};
pub use sdk::{http_client, load, loader};
