//! Controller-backed REAPI v2 remote CAS and action cache client.
//!
//! [`RemoteCas`] implements [`ContentStore`] and [`ActionStore`] over a [`tonic::transport::Channel`]
//! using `gang-reapi`'s generated `ContentAddressableStorageClient`, `ByteStreamClient`, and
//! `ActionCacheClient`.

use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_reapi::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use gang_reapi::build::bazel::remote::execution::v2::batch_update_blobs_request;
use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use gang_reapi::build::bazel::remote::execution::v2::{
    ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, FindMissingBlobsRequest,
    GetActionResultRequest, UpdateActionResultRequest, compressor, digest_function,
};
use gang_reapi::google::bytestream::byte_stream_client::ByteStreamClient;
use gang_reapi::google::bytestream::{ReadRequest, WriteRequest};
use gang_reapi::google::rpc::{Code as RpcCode, Status as RpcStatus};
use gang_reapi::{from_proto, to_proto};
use gang_spi::{
    ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection,
};
use gang_types::Digest;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tonic::Code;
use tonic::transport::Channel;

use crate::codec::{
    Compression, DEFAULT_ZSTD_LEVEL, compress_zstd, compress_zstd_stream, decompress_zstd_bounded,
    decompress_zstd_stream, verify_identity_stream,
};
use crate::resource::ResourceName;

const DEFAULT_MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_WRITE_CHUNK_BYTES: usize = 64 * 1024;

static UPLOAD_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Configuration for [`RemoteCas`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCasConfig {
    /// REAPI `instance_name` sent on CAS, `ByteStream`, and `ActionCache` requests.
    pub instance_name: String,
    /// Transport compression used for `ByteStream` and batch transfers.
    pub compression: Compression,
    /// Maximum blob byte size eligible for `BatchReadBlobs` / `BatchUpdateBlobs`.
    /// Larger blobs use `ByteStream`.
    pub max_batch_bytes: usize,
    /// Target chunk size in bytes for `ByteStream::Write` requests.
    pub write_chunk_bytes: usize,
}

impl Default for RemoteCasConfig {
    fn default() -> Self {
        Self {
            instance_name: String::new(),
            compression: Compression::Identity,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            write_chunk_bytes: DEFAULT_WRITE_CHUNK_BYTES,
        }
    }
}

/// REAPI v2 gRPC client implementing [`ContentStore`] and [`ActionStore`].
#[derive(Debug, Clone)]
pub struct RemoteCas {
    channel: Channel,
    config: RemoteCasConfig,
}

impl RemoteCas {
    /// Creates a [`RemoteCas`] client over `channel` with default configuration.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self::with_config(channel, RemoteCasConfig::default())
    }

    /// Creates a [`RemoteCas`] client over `channel` with explicit `config`.
    #[must_use]
    pub const fn with_config(channel: Channel, config: RemoteCasConfig) -> Self {
        Self { channel, config }
    }

    /// Returns the configured REAPI `instance_name`.
    #[must_use]
    pub fn instance_name(&self) -> &str {
        &self.config.instance_name
    }

    /// Returns the client configuration.
    #[must_use]
    pub const fn config(&self) -> &RemoteCasConfig {
        &self.config
    }

    async fn batch_read(&self, digest: Digest) -> Result<Option<BlobStream>, PortError> {
        let mut client = ContentAddressableStorageClient::new(self.channel.clone());
        let acceptable_compressors = match self.config.compression {
            Compression::Identity => vec![compressor::Value::Identity as i32],
            Compression::Zstd => vec![
                compressor::Value::Zstd as i32,
                compressor::Value::Identity as i32,
            ],
        };
        let proto_digest = to_proto(&digest)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;

        let response = client
            .batch_read_blobs(BatchReadBlobsRequest {
                instance_name: self.config.instance_name.clone(),
                digests: vec![proto_digest],
                acceptable_compressors,
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await
            .map_err(map_grpc_status)?
            .into_inner();

        let Some(item) = response.responses.into_iter().next() else {
            return Err(PortError::backend(
                "BatchReadBlobs returned an empty response list",
            ));
        };

        if let Some(status) = item.status
            && status.code != RpcCode::Ok as i32
        {
            if status.code == RpcCode::NotFound as i32 {
                return Ok(None);
            }
            return Err(map_rpc_status(&status));
        }

        let verified = match compressor::Value::try_from(item.compressor) {
            Ok(compressor::Value::Zstd) => decompress_zstd_bounded(digest, &item.data)?,
            Ok(compressor::Value::Identity) => {
                let actual_size = u64::try_from(item.data.len()).unwrap_or(u64::MAX);
                if actual_size != digest.size() || Digest::from_bytes(&item.data) != digest {
                    return Err(PortError::refused(Rejection::DigestMismatch));
                }
                item.data
            }
            _ => {
                return Err(PortError::backend(format!(
                    "server returned unsupported compressor id {}",
                    item.compressor
                )));
            }
        };

        Ok(Some(BlobStream::from_bytes(verified)))
    }

    async fn bytestream_read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        let is_full = range == ByteRange::FULL;
        let compression = if is_full {
            self.config.compression
        } else {
            Compression::Identity
        };

        let resource = ResourceName::blob(&self.config.instance_name, compression, digest)?;
        let read_offset =
            i64::try_from(range.offset).map_err(|_| PortError::refused(Rejection::OutOfRange))?;
        let read_limit = match range.limit {
            Some(limit) => i64::try_from(limit.get()).unwrap_or(i64::MAX),
            None => 0,
        };

        let mut client = ByteStreamClient::new(self.channel.clone());
        let response = match client
            .read(ReadRequest {
                resource_name: resource.to_string(),
                read_offset,
                read_limit,
            })
            .await
        {
            Ok(resp) => resp,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) if status.code() == Code::OutOfRange => {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            Err(status) => return Err(map_grpc_status(status)),
        };

        let mut grpc_stream = response.into_inner();
        let first_chunk = match grpc_stream.message().await {
            Ok(Some(frame)) => Some(frame.data),
            Ok(None) => None,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) if status.code() == Code::OutOfRange => {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            Err(status) => return Err(map_grpc_status(status)),
        };

        if range.offset > digest.size() {
            return Err(PortError::refused(Rejection::OutOfRange));
        }
        let available = digest.size() - range.offset;
        let expected_size = match range.limit {
            Some(limit) => available.min(limit.get()),
            None => available,
        };

        let raw_chunks = combine_first_and_rest(first_chunk, grpc_stream);

        if is_full {
            let stream = match compression {
                Compression::Zstd => decompress_zstd_stream(digest, raw_chunks),
                Compression::Identity => verify_identity_stream(digest, raw_chunks),
            };
            return Ok(Some(stream));
        }

        Ok(Some(bounded_range_stream(expected_size, raw_chunks)))
    }

    async fn batch_write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        let payload = body.collect().await?;
        let actual_size = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        if actual_size != digest.size() || Digest::from_bytes(&payload) != digest {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }

        let (data, comp_value) = match self.config.compression {
            Compression::Identity => (payload, compressor::Value::Identity as i32),
            Compression::Zstd => (
                compress_zstd(&payload, DEFAULT_ZSTD_LEVEL)?,
                compressor::Value::Zstd as i32,
            ),
        };
        let proto_digest = to_proto(&digest)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;

        let mut client = ContentAddressableStorageClient::new(self.channel.clone());
        let response = client
            .batch_update_blobs(BatchUpdateBlobsRequest {
                instance_name: self.config.instance_name.clone(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(proto_digest),
                    data,
                    compressor: comp_value,
                }],
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await
            .map_err(map_grpc_status)?
            .into_inner();

        let Some(item) = response.responses.into_iter().next() else {
            return Err(PortError::backend(
                "BatchUpdateBlobs returned an empty response list",
            ));
        };
        if let Some(status) = item.status
            && status.code != RpcCode::Ok as i32
        {
            return Err(map_rpc_status(&status));
        }
        Ok(())
    }

    async fn bytestream_write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        let upload_id = next_upload_uuid();
        let resource = ResourceName::upload(
            &self.config.instance_name,
            upload_id,
            self.config.compression,
            digest,
        )?;
        let resource_name = resource.to_string();
        let chunk_size = self.config.write_chunk_bytes.max(1);

        let verified_chunks = verifying_upload_stream(digest, body.chunks);
        let wire_chunks: BoxStream<'static, Result<Bytes, PortError>> =
            match self.config.compression {
                Compression::Identity => verified_chunks,
                Compression::Zstd => compress_zstd_stream(verified_chunks, DEFAULT_ZSTD_LEVEL),
            };

        let (tx, rx) = tokio::sync::mpsc::channel::<WriteRequest>(4);
        let writer_handle = tokio::spawn(drive_write_requests(
            resource_name,
            chunk_size,
            wire_chunks,
            tx,
        ));

        let mut client = ByteStreamClient::new(self.channel.clone());
        let request_stream =
            futures::stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|req| (req, rx)) },
            );

        let rpc_result = client.write(request_stream).await;
        let stream_result = writer_handle
            .await
            .map_err(|err| PortError::backend(format!("ByteStream write task failed: {err}")))?;

        // Prioritize local stream verification failure (e.g., DigestMismatch) over any transport
        // cancellation caused by closing the request stream before `finish_write: true`.
        stream_result?;
        let write_response = rpc_result.map_err(map_grpc_status)?.into_inner();
        if self.config.compression == Compression::Identity {
            let expected = i64::try_from(digest.size())
                .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
            if write_response.committed_size != expected {
                return Err(PortError::backend(format!(
                    "ByteStream::Write committed {} bytes, expected {}",
                    write_response.committed_size, expected
                )));
            }
        }
        Ok(())
    }
}

impl ContentStore for RemoteCas {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        missing.clear();
        let mut blob_digests = Vec::new();
        for digest in digests {
            if !digest.is_empty() {
                blob_digests.push(
                    to_proto(digest)
                        .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?,
                );
            }
        }
        if blob_digests.is_empty() {
            return Ok(());
        }

        let mut client = ContentAddressableStorageClient::new(self.channel.clone());
        let response = client
            .find_missing_blobs(FindMissingBlobsRequest {
                instance_name: self.config.instance_name.clone(),
                blob_digests,
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await
            .map_err(map_grpc_status)?
            .into_inner();

        let mut missing_set = HashSet::with_capacity(response.missing_blob_digests.len());
        for proto in &response.missing_blob_digests {
            let parsed = from_proto(proto)
                .map_err(|err| PortError::backend(format!("invalid digest from CAS: {err}")))?;
            if !parsed.is_empty() {
                missing_set.insert(parsed);
            }
        }

        for &digest in digests {
            if !digest.is_empty() && missing_set.contains(&digest) {
                missing.push(digest);
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        if digest.is_empty() {
            if range.offset > 0 {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            return Ok(Some(BlobStream::empty()));
        }

        let max_batch = u64::try_from(self.config.max_batch_bytes).unwrap_or(u64::MAX);
        if range == ByteRange::FULL && self.config.max_batch_bytes > 0 && digest.size() <= max_batch
        {
            return self.batch_read(digest).await;
        }
        self.bytestream_read(digest, range).await
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }

        if digest.is_empty() {
            let bytes = body.collect().await?;
            if !bytes.is_empty() {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            return Ok(());
        }

        let max_batch = u64::try_from(self.config.max_batch_bytes).unwrap_or(u64::MAX);
        if self.config.max_batch_bytes > 0 && digest.size() <= max_batch {
            return self.batch_write(digest, body).await;
        }
        self.bytestream_write(digest, body).await
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }
}

impl ActionStore for RemoteCas {
    async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
        let action_digest = to_proto(&action)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
        let mut client = ActionCacheClient::new(self.channel.clone());
        let response = match client
            .get_action_result(GetActionResultRequest {
                instance_name: self.config.instance_name.clone(),
                action_digest: Some(action_digest),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(map_grpc_status(status)),
        };

        Ok(Some(Bytes::from(response.encode_to_vec())))
    }

    async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
        let action_digest = to_proto(&action)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
        let action_result = ActionResult::decode(result)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;

        let mut client = ActionCacheClient::new(self.channel.clone());
        client
            .update_action_result(UpdateActionResultRequest {
                instance_name: self.config.instance_name.clone(),
                action_digest: Some(action_digest),
                action_result: Some(action_result),
                results_cache_policy: None,
                digest_function: digest_function::Value::Sha256 as i32,
            })
            .await
            .map_err(map_grpc_status)?;
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }
}

fn combine_first_and_rest(
    first: Option<Bytes>,
    rest: tonic::Streaming<gang_reapi::google::bytestream::ReadResponse>,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    let head = stream::iter(first.into_iter().map(Ok));
    let tail = stream::unfold(rest, |mut grpc_stream| async move {
        match grpc_stream.message().await {
            Ok(Some(frame)) => Some((Ok(frame.data), grpc_stream)),
            Ok(None) => None,
            Err(status) => Some((Err(map_grpc_status(status)), grpc_stream)),
        }
    });
    Box::pin(head.chain(tail))
}

fn bounded_range_stream(
    expected_size: u64,
    input: BoxStream<'static, Result<Bytes, PortError>>,
) -> BlobStream {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        expected: u64,
        received: u64,
        done: bool,
    }

    let state = State {
        input,
        expected: expected_size,
        received: 0,
        done: false,
    };

    let chunks = Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let remaining = state.expected.saturating_sub(state.received);
            if remaining == 0 {
                state.done = true;
                return Some((
                    Err(PortError::backend(
                        "ByteStream::Read returned more bytes than requested range",
                    )),
                    state,
                ));
            }
            let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            if chunk_len > remaining {
                state.done = true;
                return Some((
                    Err(PortError::backend(
                        "ByteStream::Read returned more bytes than requested range",
                    )),
                    state,
                ));
            }
            state.received = state.received.saturating_add(chunk_len);
            return Some((Ok(chunk), state));
        }

        state.done = true;
        if state.received != state.expected {
            return Some((
                Err(PortError::backend(format!(
                    "ByteStream::Read ended early: got {} bytes, expected {}",
                    state.received, state.expected
                ))),
                state,
            ));
        }
        None
    }));

    BlobStream {
        size: expected_size,
        chunks,
    }
}

fn verifying_upload_stream(
    digest: Digest,
    input: BoxStream<'static, Result<Bytes, PortError>>,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        hasher: Sha256,
        seen: u64,
        expected: Digest,
        done: bool,
    }

    let state = State {
        input,
        hasher: Sha256::new(),
        seen: 0,
        expected: digest,
        done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            let next_seen = state.seen.saturating_add(len);
            if next_seen > state.expected.size() {
                state.done = true;
                return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
            }
            state.seen = next_seen;
            state.hasher.update(&chunk);
            return Some((Ok(chunk), state));
        }

        state.done = true;
        if state.seen != state.expected.size() {
            return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
        }
        let actual_hash: [u8; 32] = state.hasher.finalize_reset().into();
        if &actual_hash != state.expected.hash() {
            return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
        }
        None
    }))
}

async fn drive_write_requests(
    resource_name: String,
    chunk_size: usize,
    mut wire_chunks: BoxStream<'static, Result<Bytes, PortError>>,
    tx: tokio::sync::mpsc::Sender<WriteRequest>,
) -> Result<(), PortError> {
    let mut write_offset: i64 = 0;
    let mut sent_first = false;

    while let Some(next) = wire_chunks.next().await {
        let chunk = next?;
        if chunk.is_empty() {
            continue;
        }
        let mut offset = 0_usize;
        while offset < chunk.len() {
            let end = (offset + chunk_size).min(chunk.len());
            let slice = chunk.slice(offset..end);
            let slice_len = i64::try_from(slice.len()).unwrap_or(i64::MAX);
            let req = WriteRequest {
                resource_name: if sent_first {
                    String::new()
                } else {
                    resource_name.clone()
                },
                write_offset,
                finish_write: false,
                data: slice,
            };
            sent_first = true;
            write_offset = write_offset.saturating_add(slice_len);
            offset = end;
            if tx.send(req).await.is_err() {
                return Ok(());
            }
        }
    }

    // Only finalize after the entire upload stream verified successfully.
    let final_req = WriteRequest {
        resource_name: if sent_first {
            String::new()
        } else {
            resource_name
        },
        write_offset,
        finish_write: true,
        data: Bytes::new(),
    };
    let _ = tx.send(final_req).await;
    Ok(())
}

fn next_upload_uuid() -> String {
    let seq = UPLOAD_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let hi = RandomState::new().hash_one(seq);
    let lo = RandomState::new().hash_one(seq ^ 0x9e37_79b9_7f4a_7c15);
    let time_lo = u32::try_from(hi >> 32).unwrap_or(0);
    let time_mid = u16::try_from((hi >> 16) & 0xffff).unwrap_or(0);
    let time_hi = u16::try_from(hi & 0x0fff).unwrap_or(0) | 0x4000;
    let clock_seq = u16::try_from((lo >> 48) & 0x3fff).unwrap_or(0) | 0x8000;
    let node = lo & 0xffff_ffff_ffff;
    format!("{time_lo:08x}-{time_mid:04x}-{time_hi:04x}-{clock_seq:04x}-{node:012x}")
}

fn map_grpc_status(status: tonic::Status) -> PortError {
    match status.code() {
        Code::OutOfRange => PortError::rejected(Rejection::OutOfRange, status),
        Code::InvalidArgument => PortError::rejected(Rejection::MalformedRequest, status),
        Code::PermissionDenied | Code::Unauthenticated => {
            PortError::rejected(Rejection::PolicyDenied, status)
        }
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Cancelled
        | Code::Unknown => PortError::unavailable(status),
        Code::Unimplemented => PortError::Unsupported { operation: "reapi" },
        _ => PortError::backend(status),
    }
}

fn map_rpc_status(status: &RpcStatus) -> PortError {
    let code = RpcCode::try_from(status.code).unwrap_or(RpcCode::Unknown);
    let message = status.message.clone();
    match code {
        RpcCode::OutOfRange => PortError::rejected(Rejection::OutOfRange, message),
        RpcCode::InvalidArgument => PortError::rejected(Rejection::MalformedRequest, message),
        RpcCode::PermissionDenied | RpcCode::Unauthenticated => {
            PortError::rejected(Rejection::PolicyDenied, message)
        }
        RpcCode::Unavailable
        | RpcCode::DeadlineExceeded
        | RpcCode::ResourceExhausted
        | RpcCode::Aborted
        | RpcCode::Cancelled
        | RpcCode::Unknown => PortError::unavailable(message),
        RpcCode::Unimplemented => PortError::Unsupported { operation: "reapi" },
        _ => PortError::backend(message),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::{Bytes, BytesMut};
    use futures::stream::{self, BoxStream};
    use gang_reapi::build::bazel::remote::execution::v2::action_cache_server::{
        ActionCache, ActionCacheServer,
    };
    use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_server::{
        ContentAddressableStorage, ContentAddressableStorageServer,
    };
    use gang_reapi::build::bazel::remote::execution::v2::{
        ActionResult, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
        BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse,
        GetActionResultRequest, GetChunkMappingRequest, GetChunkMappingResponse, GetTreeRequest,
        GetTreeResponse, RegisterChunkMappingRequest, RegisterChunkMappingResponse,
        SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse,
        UpdateActionResultRequest, batch_read_blobs_response, batch_update_blobs_response,
        compressor,
    };
    use gang_reapi::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
    use gang_reapi::google::bytestream::{
        QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
        WriteResponse,
    };
    use gang_reapi::google::rpc::{Code as RpcCode, Status as RpcStatus};
    use gang_reapi::{from_proto, to_proto};
    use gang_spi::{ActionStore, BlobStream, ByteRange, ContentStore, PortError, Rejection};
    use gang_types::Digest;
    use prost::Message as _;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Channel, Endpoint, Server};
    use tonic::{Request, Response, Status, Streaming};

    use crate::codec::{Compression, DEFAULT_ZSTD_LEVEL, compress_zstd, decompress_zstd_bounded};
    use crate::resource::ResourceName;

    use super::{RemoteCas, RemoteCasConfig};

    #[derive(Default)]
    struct MockState {
        blobs: Mutex<HashMap<Digest, Bytes>>,
        actions: Mutex<HashMap<Digest, ActionResult>>,
        rpc_calls: AtomicUsize,
        corrupt_next_read: Mutex<Option<Bytes>>,
    }

    #[derive(Clone)]
    struct MockReapi {
        state: Arc<MockState>,
    }

    #[tonic::async_trait]
    impl ContentAddressableStorage for MockReapi {
        type GetTreeStream = BoxStream<'static, Result<GetTreeResponse, Status>>;
        type GetChunkMappingStream = BoxStream<'static, Result<GetChunkMappingResponse, Status>>;

        async fn find_missing_blobs(
            &self,
            request: Request<FindMissingBlobsRequest>,
        ) -> Result<Response<FindMissingBlobsResponse>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let blobs = self.state.blobs.lock().await;
            let mut missing_blob_digests = Vec::new();
            for proto in req.blob_digests {
                let digest =
                    from_proto(&proto).map_err(|err| Status::invalid_argument(err.to_string()))?;
                if !digest.is_empty() && !blobs.contains_key(&digest) {
                    missing_blob_digests.push(proto);
                }
            }
            Ok(Response::new(FindMissingBlobsResponse {
                missing_blob_digests,
            }))
        }

        async fn batch_update_blobs(
            &self,
            request: Request<BatchUpdateBlobsRequest>,
        ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let mut blobs = self.state.blobs.lock().await;
            let mut responses = Vec::with_capacity(req.requests.len());

            for item in req.requests {
                let proto = item
                    .digest
                    .clone()
                    .ok_or_else(|| Status::invalid_argument("missing digest"))?;
                let digest =
                    from_proto(&proto).map_err(|err| Status::invalid_argument(err.to_string()))?;
                let raw = match compressor::Value::try_from(item.compressor) {
                    Ok(compressor::Value::Identity) => item.data,
                    Ok(compressor::Value::Zstd) => decompress_zstd_bounded(digest, &item.data)
                        .map_err(|err| Status::invalid_argument(err.to_string()))?,
                    _ => return Err(Status::invalid_argument("unsupported compressor")),
                };
                let status = if Digest::from_bytes(&raw) == digest {
                    blobs.insert(digest, raw);
                    RpcStatus {
                        code: RpcCode::Ok as i32,
                        message: String::new(),
                        details: Vec::new(),
                    }
                } else {
                    RpcStatus {
                        code: RpcCode::InvalidArgument as i32,
                        message: "digest mismatch".to_owned(),
                        details: Vec::new(),
                    }
                };
                responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto),
                    status: Some(status),
                });
            }

            Ok(Response::new(BatchUpdateBlobsResponse { responses }))
        }

        async fn batch_read_blobs(
            &self,
            request: Request<BatchReadBlobsRequest>,
        ) -> Result<Response<BatchReadBlobsResponse>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let blobs = self.state.blobs.lock().await;
            let corrupt = self.state.corrupt_next_read.lock().await.take();
            let wants_zstd = req
                .acceptable_compressors
                .contains(&(compressor::Value::Zstd as i32));

            let mut responses = Vec::with_capacity(req.digests.len());
            for proto in req.digests {
                let digest =
                    from_proto(&proto).map_err(|err| Status::invalid_argument(err.to_string()))?;
                if let Some(corrupt_payload) = corrupt.clone() {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(proto),
                        data: corrupt_payload,
                        compressor: if wants_zstd {
                            compressor::Value::Zstd as i32
                        } else {
                            compressor::Value::Identity as i32
                        },
                        status: Some(RpcStatus {
                            code: RpcCode::Ok as i32,
                            message: String::new(),
                            details: Vec::new(),
                        }),
                    });
                    continue;
                }
                if let Some(data) = blobs.get(&digest) {
                    let (wire_data, comp) = if wants_zstd {
                        (
                            compress_zstd(data, DEFAULT_ZSTD_LEVEL)
                                .map_err(|err| Status::internal(err.to_string()))?,
                            compressor::Value::Zstd as i32,
                        )
                    } else {
                        (data.clone(), compressor::Value::Identity as i32)
                    };
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(proto),
                        data: wire_data,
                        compressor: comp,
                        status: Some(RpcStatus {
                            code: RpcCode::Ok as i32,
                            message: String::new(),
                            details: Vec::new(),
                        }),
                    });
                } else {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(proto),
                        data: Bytes::new(),
                        compressor: compressor::Value::Identity as i32,
                        status: Some(RpcStatus {
                            code: RpcCode::NotFound as i32,
                            message: "not found".to_owned(),
                            details: Vec::new(),
                        }),
                    });
                }
            }

            Ok(Response::new(BatchReadBlobsResponse { responses }))
        }

        async fn get_tree(
            &self,
            _request: Request<GetTreeRequest>,
        ) -> Result<Response<Self::GetTreeStream>, Status> {
            Err(Status::unimplemented("get_tree not used"))
        }

        async fn split_blob(
            &self,
            _request: Request<SplitBlobRequest>,
        ) -> Result<Response<SplitBlobResponse>, Status> {
            Err(Status::unimplemented("split_blob not used"))
        }

        async fn get_chunk_mapping(
            &self,
            _request: Request<GetChunkMappingRequest>,
        ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
            Err(Status::unimplemented("get_chunk_mapping not used"))
        }

        async fn splice_blob(
            &self,
            _request: Request<SpliceBlobRequest>,
        ) -> Result<Response<SpliceBlobResponse>, Status> {
            Err(Status::unimplemented("splice_blob not used"))
        }

        async fn register_chunk_mapping(
            &self,
            _request: Request<Streaming<RegisterChunkMappingRequest>>,
        ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
            Err(Status::unimplemented("register_chunk_mapping not used"))
        }
    }

    #[tonic::async_trait]
    impl ByteStream for MockReapi {
        type ReadStream = BoxStream<'static, Result<ReadResponse, Status>>;

        async fn read(
            &self,
            request: Request<ReadRequest>,
        ) -> Result<Response<Self::ReadStream>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let parsed = ResourceName::parse(&req.resource_name)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

            if let Some(corrupt) = self.state.corrupt_next_read.lock().await.take() {
                return Ok(Response::new(Box::pin(stream::once(std::future::ready(
                    Ok(ReadResponse { data: corrupt }),
                )))));
            }

            let blobs = self.state.blobs.lock().await;
            let data = blobs
                .get(&parsed.digest())
                .cloned()
                .ok_or_else(|| Status::not_found("blob missing"))?;

            if req.read_offset < 0 || req.read_limit < 0 {
                return Err(Status::invalid_argument("negative offset or limit"));
            }
            let total = data.len();
            let offset = usize::try_from(req.read_offset)
                .map_err(|_| Status::out_of_range("offset overflow"))?;
            if offset > total {
                return Err(Status::out_of_range("offset exceeds blob size"));
            }

            let wire_bytes = match parsed.compression() {
                Compression::Zstd => {
                    if req.read_limit != 0 {
                        return Err(Status::invalid_argument(
                            "read_limit must be 0 for compressed reads",
                        ));
                    }
                    compress_zstd(&data, DEFAULT_ZSTD_LEVEL)
                        .map_err(|err| Status::internal(err.to_string()))?
                }
                Compression::Identity => {
                    let available = total - offset;
                    let limit = if req.read_limit == 0 {
                        available
                    } else {
                        available.min(usize::try_from(req.read_limit).unwrap_or(usize::MAX))
                    };
                    data.slice(offset..offset + limit)
                }
            };

            let frames: Vec<Result<ReadResponse, Status>> = wire_bytes
                .chunks(1024)
                .map(|c| {
                    Ok(ReadResponse {
                        data: Bytes::copy_from_slice(c),
                    })
                })
                .collect();
            Ok(Response::new(Box::pin(stream::iter(frames))))
        }

        async fn write(
            &self,
            request: Request<Streaming<WriteRequest>>,
        ) -> Result<Response<WriteResponse>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let mut stream = request.into_inner();
            let mut resource: Option<ResourceName> = None;
            let mut buffer = BytesMut::new();
            let mut expected_offset: i64 = 0;
            let mut finished = false;

            while let Some(msg) = stream.message().await? {
                if resource.is_none() {
                    resource = Some(
                        ResourceName::parse(&msg.resource_name)
                            .map_err(|err| Status::invalid_argument(err.to_string()))?,
                    );
                }
                if msg.write_offset != expected_offset {
                    return Err(Status::invalid_argument("offset mismatch"));
                }
                expected_offset += i64::try_from(msg.data.len()).unwrap_or(i64::MAX);
                buffer.extend_from_slice(&msg.data);
                if msg.finish_write {
                    finished = true;
                    break;
                }
            }

            if !finished {
                return Err(Status::invalid_argument("write stream did not finish"));
            }
            let parsed = resource.ok_or_else(|| Status::invalid_argument("empty write"))?;
            let wire_bytes = buffer.freeze();
            let raw = match parsed.compression() {
                Compression::Identity => wire_bytes,
                Compression::Zstd => decompress_zstd_bounded(parsed.digest(), &wire_bytes)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            };
            if Digest::from_bytes(&raw) != parsed.digest() {
                return Err(Status::invalid_argument("digest mismatch"));
            }
            self.state.blobs.lock().await.insert(parsed.digest(), raw);
            let committed_size = if parsed.compression() == Compression::Zstd {
                -1
            } else {
                i64::try_from(parsed.digest().size()).unwrap_or(i64::MAX)
            };
            Ok(Response::new(WriteResponse { committed_size }))
        }

        async fn query_write_status(
            &self,
            _request: Request<QueryWriteStatusRequest>,
        ) -> Result<Response<QueryWriteStatusResponse>, Status> {
            Err(Status::unimplemented("query_write_status not used"))
        }
    }

    #[tonic::async_trait]
    impl ActionCache for MockReapi {
        async fn get_action_result(
            &self,
            request: Request<GetActionResultRequest>,
        ) -> Result<Response<ActionResult>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let proto = req
                .action_digest
                .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;
            let digest =
                from_proto(&proto).map_err(|err| Status::invalid_argument(err.to_string()))?;
            let actions = self.state.actions.lock().await;
            actions
                .get(&digest)
                .cloned()
                .map(Response::new)
                .ok_or_else(|| Status::not_found("action result missing"))
        }

        async fn update_action_result(
            &self,
            request: Request<UpdateActionResultRequest>,
        ) -> Result<Response<ActionResult>, Status> {
            self.state.rpc_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let proto = req
                .action_digest
                .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;
            let digest =
                from_proto(&proto).map_err(|err| Status::invalid_argument(err.to_string()))?;
            let result = req
                .action_result
                .ok_or_else(|| Status::invalid_argument("missing action_result"))?;
            self.state
                .actions
                .lock()
                .await
                .insert(digest, result.clone());
            Ok(Response::new(result))
        }
    }

    async fn spawn_mock_server() -> (Channel, Arc<MockState>, tokio::task::JoinHandle<()>) {
        let state = Arc::new(MockState::default());
        let service = MockReapi {
            state: Arc::clone(&state),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let incoming = TcpIncoming::from(listener);

        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(ContentAddressableStorageServer::new(service.clone()))
                .add_service(ByteStreamServer::new(service.clone()))
                .add_service(ActionCacheServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });

        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");

        (channel, state, handle)
    }

    fn assert_rejected<T: std::fmt::Debug>(result: &Result<T, PortError>, expected: Rejection) {
        assert!(
            matches!(result, Err(PortError::Rejected { reason, .. }) if *reason == expected),
            "expected Rejection::{expected:?}, got {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_digest_short_circuits_without_rpc() {
        let (channel, state, handle) = spawn_mock_server().await;
        let cas = RemoteCas::new(channel);

        let mut missing = vec![Digest::from_bytes(b"stale")];
        cas.find_missing(&[Digest::EMPTY], &mut missing)
            .await
            .expect("find_missing empty");
        assert!(missing.is_empty());

        let stream = cas
            .read(Digest::EMPTY, ByteRange::FULL)
            .await
            .expect("read empty")
            .expect("empty is present");
        assert_eq!(stream.size, 0);
        assert!(stream.collect().await.expect("collect empty").is_empty());

        assert_rejected(
            &cas.read(
                Digest::EMPTY,
                ByteRange {
                    offset: 1,
                    limit: None,
                },
            )
            .await,
            Rejection::OutOfRange,
        );

        cas.write(Digest::EMPTY, BlobStream::empty())
            .await
            .expect("write empty");
        assert_rejected(
            &cas.write(
                Digest::EMPTY,
                BlobStream::from_bytes(Bytes::from_static(b"nonempty")),
            )
            .await,
            Rejection::DigestMismatch,
        );

        assert_eq!(
            state.rpc_calls.load(Ordering::SeqCst),
            0,
            "Digest::EMPTY must not emit RPCs"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn batch_and_bytestream_roundtrip_with_identity_and_zstd() {
        let (channel, _state, handle) = spawn_mock_server().await;

        for compression in [Compression::Identity, Compression::Zstd] {
            for max_batch_bytes in [0_usize, 1024 * 1024] {
                let cas = RemoteCas::with_config(
                    channel.clone(),
                    RemoteCasConfig {
                        instance_name: "prod/cluster".to_owned(),
                        compression,
                        max_batch_bytes,
                        write_chunk_bytes: 512,
                    },
                );

                let payload = Bytes::from(vec![b'x'; 4096]);
                let digest = Digest::from_bytes(&payload);
                let missing_probe = Digest::from_bytes(b"absent-probe");

                // Split write into multiple chunks.
                let chunks: Vec<Result<Bytes, PortError>> = payload
                    .chunks(700)
                    .map(|c| Ok(Bytes::copy_from_slice(c)))
                    .collect();
                cas.write(
                    digest,
                    BlobStream {
                        size: digest.size(),
                        chunks: Box::pin(stream::iter(chunks)),
                    },
                )
                .await
                .expect("write blob");

                let mut missing = Vec::new();
                cas.find_missing(&[missing_probe, digest, Digest::EMPTY], &mut missing)
                    .await
                    .expect("find_missing");
                assert_eq!(missing, vec![missing_probe]);

                let full = cas
                    .read(digest, ByteRange::FULL)
                    .await
                    .expect("read full")
                    .expect("blob present");
                assert_eq!(full.size, digest.size());
                assert_eq!(full.collect().await.expect("collect"), payload);
            }
        }
        handle.abort();
    }

    #[tokio::test]
    async fn ranged_reads_and_edges_follow_contract() {
        let (channel, _state, handle) = spawn_mock_server().await;
        let cas = RemoteCas::with_config(
            channel,
            RemoteCasConfig {
                compression: Compression::Zstd,
                ..RemoteCasConfig::default()
            },
        );

        let payload = b"0123456789";
        let digest = Digest::from_bytes(payload);
        let absent = Digest::from_bytes(b"missing");

        assert!(
            cas.read(absent, ByteRange::FULL)
                .await
                .expect("read absent")
                .is_none()
        );

        cas.write(digest, BlobStream::from_bytes(Bytes::from_static(payload)))
            .await
            .expect("write");

        let slice = cas
            .read(
                digest,
                ByteRange {
                    offset: 2,
                    limit: NonZeroU64::new(4),
                },
            )
            .await
            .expect("read slice")
            .expect("present");
        assert_eq!(slice.size, 4);
        assert_eq!(slice.collect().await.expect("collect").as_ref(), b"2345");

        let clipped = cas
            .read(
                digest,
                ByteRange {
                    offset: 7,
                    limit: NonZeroU64::new(10),
                },
            )
            .await
            .expect("read clipped")
            .expect("present");
        assert_eq!(clipped.size, 3);
        assert_eq!(clipped.collect().await.expect("collect").as_ref(), b"789");

        let at_end = cas
            .read(
                digest,
                ByteRange {
                    offset: 10,
                    limit: None,
                },
            )
            .await
            .expect("read at end")
            .expect("present");
        assert_eq!(at_end.size, 0);
        assert!(at_end.collect().await.expect("collect").is_empty());

        assert_rejected(
            &cas.read(
                digest,
                ByteRange {
                    offset: 11,
                    limit: None,
                },
            )
            .await,
            Rejection::OutOfRange,
        );
        handle.abort();
    }

    #[tokio::test]
    async fn rejects_corrupt_writes_and_server_bombs() {
        let (channel, state, handle) = spawn_mock_server().await;
        let cas = RemoteCas::with_config(
            channel,
            RemoteCasConfig {
                compression: Compression::Zstd,
                max_batch_bytes: 0,
                ..RemoteCasConfig::default()
            },
        );

        // Corrupt client write fails locally and leaves nothing on server.
        let expected_digest = Digest::from_bytes(b"expected-data");
        assert_rejected(
            &cas.write(
                expected_digest,
                BlobStream::from_bytes(Bytes::from_static(b"wrong-content")),
            )
            .await,
            Rejection::DigestMismatch,
        );
        assert!(state.blobs.lock().await.is_empty());

        // Malicious server sends zstd bomb over ByteStream::Read -> client fails with DigestMismatch.
        let bomb =
            compress_zstd(&vec![0_u8; 64 * 1024], DEFAULT_ZSTD_LEVEL).expect("compress bomb");
        let small_digest = Digest::from_bytes(b"small");
        state
            .blobs
            .lock()
            .await
            .insert(small_digest, Bytes::from_static(b"small"));
        *state.corrupt_next_read.lock().await = Some(bomb);

        let stream = cas
            .read(small_digest, ByteRange::FULL)
            .await
            .expect("stream opens")
            .expect("exists");
        assert_rejected(&stream.collect().await, Rejection::DigestMismatch);

        handle.abort();
    }

    #[tokio::test]
    async fn action_store_roundtrips_and_rejects_invalid_protobuf() {
        let (channel, _state, handle) = spawn_mock_server().await;
        let cas = RemoteCas::new(channel);
        assert!(ContentStore::usage(&cas).is_none());
        assert!(ActionStore::usage(&cas).is_none());

        let action = Digest::from_bytes(b"action-1");
        assert_eq!(cas.get(action).await.expect("get absent"), None);

        let result = ActionResult {
            exit_code: 0,
            stdout_raw: Bytes::from_static(b"ok\n"),
            ..ActionResult::default()
        };
        let encoded = Bytes::from(result.encode_to_vec());
        cas.put(action, encoded.clone()).await.expect("put action");

        let got = cas
            .get(action)
            .await
            .expect("get action")
            .expect("action present");
        let decoded = ActionResult::decode(got).expect("decode ActionResult");
        assert_eq!(decoded, result);

        // Invalid protobuf bytes are rejected cleanly without RPC.
        let invalid = Bytes::from_static(&[0xff, 0xff, 0xff]);
        assert_rejected(&cas.put(action, invalid).await, Rejection::MalformedRequest);

        let _ = to_proto(&action);
        handle.abort();
    }
}
