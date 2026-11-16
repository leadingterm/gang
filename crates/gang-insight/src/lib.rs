//! In-memory build observations and bounded summaries.
//!
//! This crate is pure: it performs no I/O and reads no clocks.
//!
//! ```
//! use gang_insight::BuildFilter;
//! let filter: BuildFilter = "".parse().unwrap();
//! assert_eq!(filter, BuildFilter::default());
//! ```

mod builds;
mod filter;
mod record;
mod timeline;

pub use builds::Builds;
pub use filter::{BuildFilter, BuildFilterError};
