//! Live state of the fleet for the admin dashboard: the builder that derives a `State` from one
//! scheduling pass, the diff and apply pair that keeps a client's copy in step, and the
//! server-sent-event frames that carry them.
//!
//! This crate is pure: it performs no I/O and runs no tasks.
//!
//! ```
//! use gang_clock::Timestamp;
//! use gang_wire::{Frame, Tick};
//!
//! let tick = Frame::Tick(Tick { seq: 1, time: Timestamp::from_unix_millis(1) });
//! let bytes = gang_state::sse::encode(&tick).unwrap();
//! assert!(bytes.starts_with(b"event: tick\n"));
//! let frames = gang_state::sse::Decoder::new().push(&bytes).unwrap();
//! assert_eq!(frames, [tick]);
//! ```

mod apply;
mod builder;
mod diff;
mod errors;
mod events;
mod inputs;
pub mod sse;

pub use apply::apply;
pub use builder::Builder;
pub use diff::diff;
pub use errors::{ApplyError, DecodeError, EncodeError};
pub use inputs::{ActionsSample, BuilderConfig, Inputs, PassStatus};
