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
mod errors;
mod filter;
mod forward;
mod ingest;
mod record;
mod timeline;

pub use builds::Builds;
pub use errors::{IngestError, OutboxError};
pub use filter::{BuildFilter, BuildFilterError};
pub use forward::{Dispatch, Outbox, Outcome};
pub use ingest::{Ingested, decode};
