//! Generated `REAPI v2`, Google RPC/ByteStream/LongRunning, and Gang Worker v1 protobuf and gRPC
//! bindings, plus checked conversions for [`gang_types::Digest`].

pub mod build;
mod digest;
pub mod google;
pub mod leadingterm;

pub use digest::{
    DigestConversionError, PRECONDITION_FAILURE_TYPE_URL, extract_missing_blobs, from_proto,
    missing_blobs_precondition_failure, missing_blobs_rpc_status, to_proto,
};
