//! Generated `REAPI v2`, Google RPC/ByteStream/LongRunning, and Gang Worker v1 protobuf and gRPC
//! bindings, plus checked conversions for [`gang_types::Digest`].

pub mod build;
mod digest;
pub mod google;
pub mod leadingterm;

pub use digest::{DigestConversionError, from_proto, to_proto};
