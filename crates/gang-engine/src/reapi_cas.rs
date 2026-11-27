//! `REAPI v2` `ContentAddressableStorage` (`CAS`) gRPC service and shared in-flight blob budget.
//!
//! Implements `FindMissingBlobs`, `BatchUpdateBlobs`, `BatchReadBlobs`, and paginated `GetTree`
//! over an [`Arc<C>`](Arc) implementing [`gang_spi::ContentStore`], bounded by [`InflightBlobBudget`]
//! (`max_inflight_blob_bytes`). `SplitBlob`, `GetChunkMapping`, `SpliceBlob`, and
//! `RegisterChunkMapping` return `UNIMPLEMENTED`.

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;

use base64::Engine as _;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use gang_cas::{Compression, DEFAULT_ZSTD_LEVEL, compress_zstd, decompress_zstd_bounded};
use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use gang_reapi::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest as ProtoDigest, Directory, FindMissingBlobsRequest,
    FindMissingBlobsResponse, GetChunkMappingRequest, GetChunkMappingResponse, GetTreeRequest,
    GetTreeResponse, RegisterChunkMappingRequest, RegisterChunkMappingResponse, SpliceBlobRequest,
    SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse, batch_read_blobs_response,
    batch_update_blobs_response, compressor, digest_function,
};
use gang_reapi::google::rpc::{Code as RpcCode, Status as RpcStatus};
use gang_reapi::{from_proto, to_proto};
use gang_spi::{BlobStream, ByteRange, ContentStore, PortError, Rejection};
use gang_types::{Digest, NonZeroBytes};
use parking_lot::Mutex;
use prost::Message as _;
use tokio::sync::Notify;
use tonic::{Request, Response, Status, Streaming};

const DEFAULT_MAX_BATCH_BYTES: NonZeroBytes = NonZeroBytes::mib(4);
const DEFAULT_MAX_TREE_PAGE_SIZE: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1023);
const DEFAULT_MAX_TREE_DIRECTORIES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(99_999);

const TREE_TOKEN_PREFIX: &str = "v1:";
const TREE_TOKEN_BYTES: usize = 48;

#[derive(Debug)]
struct BudgetInner {
    capacity: u64,
    available: Mutex<u64>,
    notify: Notify,
}

/// Shared byte-level semaphore bounding total CAS blob bytes held in flight concurrently.
///
/// Maps to `[server] max_inflight_blob_bytes`.
#[derive(Debug, Clone)]
pub struct InflightBlobBudget {
    inner: Arc<BudgetInner>,
}

/// RAII permit reserving `bytes` from an [`InflightBlobBudget`].
///
/// Dropping the permit returns its reserved bytes to the budget and wakes waiting callers.
#[derive(Debug)]
pub struct BlobPermit {
    inner: Arc<BudgetInner>,
    bytes: u64,
}

impl BlobPermit {
    /// Returns the number of bytes reserved by this permit.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for BlobPermit {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        {
            let mut available = self.inner.available.lock();
            *available = available
                .saturating_add(self.bytes)
                .min(self.inner.capacity);
        }
        self.inner.notify.notify_waiters();
    }
}

impl InflightBlobBudget {
    /// Creates a new in-flight byte budget with capacity `max_inflight_blob_bytes`.
    #[must_use]
    pub fn new(max_inflight_blob_bytes: NonZeroBytes) -> Self {
        let capacity = max_inflight_blob_bytes.as_u64();
        Self {
            inner: Arc::new(BudgetInner {
                capacity,
                available: Mutex::new(capacity),
                notify: Notify::new(),
            }),
        }
    }

    /// Returns the total byte capacity of this budget.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Returns the currently unreserved byte capacity.
    #[must_use]
    pub fn available(&self) -> u64 {
        *self.inner.available.lock()
    }

    /// Reserves `bytes` from the budget, waiting if another in-flight operation currently holds them.
    ///
    /// # Errors
    ///
    /// Returns [`Status::resource_exhausted`] immediately if `bytes` exceeds the total budget
    /// [`capacity`](Self::capacity).
    pub async fn acquire(&self, bytes: u64) -> Result<BlobPermit, Status> {
        if bytes == 0 {
            return Ok(BlobPermit {
                inner: Arc::clone(&self.inner),
                bytes: 0,
            });
        }
        if bytes > self.inner.capacity {
            return Err(Status::resource_exhausted(format!(
                "requested {bytes} in-flight blob bytes exceeds budget capacity of {} bytes",
                self.inner.capacity
            )));
        }

        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut available = self.inner.available.lock();
                if *available >= bytes {
                    *available -= bytes;
                    return Ok(BlobPermit {
                        inner: Arc::clone(&self.inner),
                        bytes,
                    });
                }
            }

            notified.await;
        }
    }

    /// Attempts to reserve `bytes` immediately without waiting.
    ///
    /// Returns `Ok(None)` if fewer than `bytes` are currently available.
    ///
    /// # Errors
    ///
    /// Returns [`Status::resource_exhausted`] if `bytes` exceeds [`capacity`](Self::capacity).
    pub fn try_acquire(&self, bytes: u64) -> Result<Option<BlobPermit>, Status> {
        if bytes == 0 {
            return Ok(Some(BlobPermit {
                inner: Arc::clone(&self.inner),
                bytes: 0,
            }));
        }
        if bytes > self.inner.capacity {
            return Err(Status::resource_exhausted(format!(
                "requested {bytes} in-flight blob bytes exceeds budget capacity of {} bytes",
                self.inner.capacity
            )));
        }
        let mut available = self.inner.available.lock();
        if *available >= bytes {
            *available -= bytes;
            Ok(Some(BlobPermit {
                inner: Arc::clone(&self.inner),
                bytes,
            }))
        } else {
            Ok(None)
        }
    }
}

/// Configuration for [`CasService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CasConfig {
    /// Maximum batch payload or encoded `GetTreeResponse` size (the gRPC message limit).
    pub max_batch_total_size_bytes: NonZeroBytes,
    /// Maximum number of `Directory` messages emitted in a single `GetTreeResponse` page.
    pub max_tree_page_size: NonZeroUsize,
    /// Maximum number of unique directories traversed in a single `GetTree` call.
    pub max_tree_directories: NonZeroUsize,
}

impl Default for CasConfig {
    fn default() -> Self {
        Self {
            max_batch_total_size_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_tree_page_size: DEFAULT_MAX_TREE_PAGE_SIZE,
            max_tree_directories: DEFAULT_MAX_TREE_DIRECTORIES,
        }
    }
}

/// `REAPI v2` `ContentAddressableStorage` gRPC server backed by a [`ContentStore`].
#[derive(Debug, Clone)]
pub struct CasService<C> {
    store: Arc<C>,
    budget: InflightBlobBudget,
    config: CasConfig,
}

impl<C: ContentStore> CasService<C> {
    /// Creates a [`CasService`] backed by `store`, `budget`, and `config`.
    #[must_use]
    pub const fn new(store: Arc<C>, budget: InflightBlobBudget, config: CasConfig) -> Self {
        Self {
            store,
            budget,
            config,
        }
    }

    /// Creates a [`CasService`] with default [`CasConfig`].
    #[must_use]
    pub fn with_defaults(store: Arc<C>, budget: InflightBlobBudget) -> Self {
        Self::new(store, budget, CasConfig::default())
    }

    /// Wraps this service in a tonic [`ContentAddressableStorageServer`].
    #[must_use]
    pub fn into_server(self) -> ContentAddressableStorageServer<Self> {
        ContentAddressableStorageServer::new(self)
    }
}

/// Validates that `digest_function` is either `UNKNOWN` (REAPI <= v2.12 default) or `SHA256`.
pub(crate) fn validate_sha256_function(digest_function: i32) -> Result<(), Status> {
    match digest_function::Value::try_from(digest_function) {
        Ok(digest_function::Value::Unknown | digest_function::Value::Sha256) => Ok(()),
        Ok(other) => Err(Status::invalid_argument(format!(
            "unsupported digest function {other:?}; only SHA256 is supported"
        ))),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown digest function {digest_function}"
        ))),
    }
}

/// Parses an optional `REAPI v2` protobuf digest into a validated [`Digest`].
pub(crate) fn parse_proto_digest(
    proto: Option<&ProtoDigest>,
    field_name: &str,
) -> Result<Digest, Status> {
    let proto = proto.ok_or_else(|| Status::invalid_argument(format!("missing {field_name}")))?;
    from_proto(proto)
        .map_err(|err| Status::invalid_argument(format!("invalid {field_name}: {err}")))
}

/// Maps a [`PortError`] to a `tonic::Status`.
pub(crate) fn map_port_error(error: &PortError) -> Status {
    match error {
        PortError::NotFound(_) => Status::not_found(error.to_string()),
        PortError::Rejected {
            reason: Rejection::OutOfRange,
            ..
        } => Status::out_of_range(error.to_string()),
        PortError::Rejected {
            reason: Rejection::ResourceExhausted,
            ..
        } => Status::resource_exhausted(error.to_string()),
        PortError::Rejected {
            reason: Rejection::DigestMismatch | Rejection::MalformedRequest,
            ..
        } => Status::invalid_argument(error.to_string()),
        PortError::Rejected {
            reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
            ..
        } => Status::permission_denied(error.to_string()),
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        } => Status::unauthenticated(error.to_string()),
        PortError::Unsupported { .. } => Status::unimplemented(error.to_string()),
        PortError::Unavailable(_) => Status::unavailable(error.to_string()),
        PortError::Config(_) | PortError::Backend(_) => Status::internal(error.to_string()),
    }
}

/// Maps a [`PortError`] to a `google.rpc.Status` for per-item batch responses.
pub(crate) fn port_error_to_rpc_status(error: &PortError) -> RpcStatus {
    let code = match error {
        PortError::NotFound(_) => RpcCode::NotFound,
        PortError::Rejected {
            reason: Rejection::OutOfRange,
            ..
        } => RpcCode::OutOfRange,
        PortError::Rejected {
            reason: Rejection::ResourceExhausted,
            ..
        } => RpcCode::ResourceExhausted,
        PortError::Rejected {
            reason: Rejection::DigestMismatch | Rejection::MalformedRequest,
            ..
        } => RpcCode::InvalidArgument,
        PortError::Rejected {
            reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
            ..
        } => RpcCode::PermissionDenied,
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        } => RpcCode::Unauthenticated,
        PortError::Unsupported { .. } => RpcCode::Unimplemented,
        PortError::Unavailable(_) => RpcCode::Unavailable,
        PortError::Config(_) | PortError::Backend(_) => RpcCode::Internal,
    };
    RpcStatus {
        code: code as i32,
        message: error.to_string(),
        details: Vec::new(),
    }
}

fn ok_rpc_status() -> RpcStatus {
    RpcStatus {
        code: RpcCode::Ok as i32,
        message: String::new(),
        details: Vec::new(),
    }
}

fn invalid_arg_rpc_status(message: impl Into<String>) -> RpcStatus {
    RpcStatus {
        code: RpcCode::InvalidArgument as i32,
        message: message.into(),
        details: Vec::new(),
    }
}

fn not_found_rpc_status(message: impl Into<String>) -> RpcStatus {
    RpcStatus {
        code: RpcCode::NotFound as i32,
        message: message.into(),
        details: Vec::new(),
    }
}

fn parse_batch_compressor(value: i32) -> Result<Compression, String> {
    match compressor::Value::try_from(value) {
        Ok(compressor::Value::Identity) => Ok(Compression::Identity),
        Ok(compressor::Value::Zstd) => Ok(Compression::Zstd),
        Ok(other) => Err(format!("unsupported batch compressor {other:?}")),
        Err(_) => Err(format!("unknown batch compressor {value}")),
    }
}

fn decode_batch_payload(
    digest: Digest,
    compression: Compression,
    data: Bytes,
) -> Result<Bytes, PortError> {
    match compression {
        Compression::Identity => {
            let actual_size = u64::try_from(data.len()).unwrap_or(u64::MAX);
            if actual_size != digest.size() || Digest::from_bytes(&data) != digest {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            Ok(data)
        }
        Compression::Zstd => decompress_zstd_bounded(digest, &data),
    }
}

fn encode_tree_token(root: Digest, skip: u64) -> String {
    let mut raw = [0_u8; TREE_TOKEN_BYTES];
    raw[..32].copy_from_slice(root.hash());
    raw[32..40].copy_from_slice(&root.size().to_le_bytes());
    raw[40..48].copy_from_slice(&skip.to_le_bytes());
    let encoded = BASE64_URL_SAFE_NO_PAD.encode(raw);
    format!("{TREE_TOKEN_PREFIX}{encoded}")
}

fn decode_tree_token(token: &str, expected_root: Digest) -> Result<u64, Status> {
    let Some(payload) = token.strip_prefix(TREE_TOKEN_PREFIX) else {
        return Err(Status::invalid_argument("invalid GetTree page_token"));
    };
    let bytes = BASE64_URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|_| Status::invalid_argument("invalid GetTree page_token"))?;
    if bytes.len() != TREE_TOKEN_BYTES {
        return Err(Status::invalid_argument("invalid GetTree page_token"));
    }
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(&bytes[..32]);
    let mut size_buf = [0_u8; 8];
    size_buf.copy_from_slice(&bytes[32..40]);
    let mut skip_buf = [0_u8; 8];
    skip_buf.copy_from_slice(&bytes[40..48]);

    let token_root = Digest::new(hash, u64::from_le_bytes(size_buf));
    if token_root != expected_root {
        return Err(Status::invalid_argument(
            "GetTree page_token does not match root_digest",
        ));
    }
    Ok(u64::from_le_bytes(skip_buf))
}

async fn read_directory<C: ContentStore>(
    store: &C,
    digest: Digest,
) -> Result<Option<Directory>, Status> {
    if digest.is_empty() {
        return Ok(Some(Directory::default()));
    }
    let Some(stream) = store
        .read(digest, ByteRange::FULL)
        .await
        .map_err(|err| map_port_error(&err))?
    else {
        return Ok(None);
    };
    let raw = stream.collect().await.map_err(|err| map_port_error(&err))?;
    Directory::decode(raw)
        .map(Some)
        .map_err(|err| Status::data_loss(format!("corrupt Directory proto {digest}: {err}")))
}

fn directory_entry_bytes(size: u64) -> u64 {
    let size_usize = usize::try_from(size).unwrap_or(usize::MAX);
    size.saturating_add(1 + prost::length_delimiter_len(size_usize) as u64)
}

struct TreeTraversalState<C> {
    store: Arc<C>,
    budget: InflightBlobBudget,
    root_digest: Digest,
    page_size: usize,
    page_byte_limit: u64,
    page_permits: Vec<BlobPermit>,
    max_directories: usize,
    skip: u64,
    emitted_count: u64,
    queue: VecDeque<Digest>,
    visited: HashSet<Digest>,
    lookahead: Option<(Directory, BlobPermit)>,
    yielded_any: bool,
    done: bool,
}

impl<C: ContentStore> TreeTraversalState<C> {
    fn enqueue_children(&mut self, dir: &Directory) -> Result<(), Status> {
        for child in &dir.directories {
            let child_digest = parse_proto_digest(child.digest.as_ref(), "DirectoryNode.digest")?;
            if self.visited.contains(&child_digest) {
                continue;
            }
            if self.visited.len() >= self.max_directories {
                return Err(Status::resource_exhausted(format!(
                    "directory tree exceeds maximum traversal limit of {} directories",
                    self.max_directories
                )));
            }
            self.visited.insert(child_digest);
            self.queue.push_back(child_digest);
        }
        Ok(())
    }

    async fn prune_missing_directories(&mut self) -> Result<(), Status> {
        let mut missing = Vec::new();
        while let Some(&digest) = self.queue.front() {
            self.store
                .find_missing(&[digest], &mut missing)
                .await
                .map_err(|err| map_port_error(&err))?;
            if missing.is_empty() {
                break;
            }
            self.queue.pop_front();
        }
        Ok(())
    }

    async fn next_page(&mut self) -> Result<Option<GetTreeResponse>, Status> {
        // A poll for the next page acknowledges consumption of the previous page.
        self.page_permits.clear();
        if self.done {
            return Ok(None);
        }

        let token_bytes = GetTreeResponse {
            directories: Vec::new(),
            next_page_token: encode_tree_token(self.root_digest, 0),
        }
        .encoded_len() as u64;
        let mut directories = Vec::new();
        let mut page_bytes = 0_u64;
        while directories.len() < self.page_size {
            let (dir, permit) = if let Some(first) = self.lookahead.take() {
                first
            } else {
                let Some(&digest) = self.queue.front() else {
                    break;
                };
                let entry_bytes = directory_entry_bytes(digest.size());
                if !directories.is_empty()
                    && page_bytes
                        .saturating_add(entry_bytes)
                        .saturating_add(token_bytes)
                        > self.page_byte_limit
                {
                    break;
                }
                if entry_bytes > self.page_byte_limit {
                    return Err(Status::resource_exhausted(
                        "Directory exceeds GetTree page byte limit",
                    ));
                }
                let permit = if directories.is_empty() {
                    self.budget.acquire(digest.size()).await?
                } else {
                    // Never wait while holding a partial page, including when another
                    // traversal owns the rest of the shared budget.
                    let Some(permit) = self.budget.try_acquire(digest.size())? else {
                        break;
                    };
                    permit
                };
                self.queue.pop_front();
                let Some(dir) = read_directory(self.store.as_ref(), digest).await? else {
                    continue;
                };
                self.enqueue_children(&dir)?;
                (dir, permit)
            };
            if self.emitted_count < self.skip {
                self.emitted_count = self.emitted_count.saturating_add(1);
                continue;
            }
            page_bytes = page_bytes.saturating_add(directory_entry_bytes(dir.encoded_len() as u64));
            let response_bytes = page_bytes.saturating_add(if self.queue.is_empty() {
                0
            } else {
                token_bytes
            });
            if response_bytes > self.page_byte_limit {
                return Err(Status::resource_exhausted(
                    "Directory exceeds GetTree page byte limit",
                ));
            }
            self.emitted_count = self.emitted_count.saturating_add(1);
            directories.push(dir);
            self.page_permits.push(permit);
        }

        self.prune_missing_directories().await?;
        if directories.is_empty() {
            self.done = true;
            if !self.yielded_any {
                self.yielded_any = true;
                return Ok(Some(GetTreeResponse {
                    directories: Vec::new(),
                    next_page_token: String::new(),
                }));
            }
            return Ok(None);
        }

        let next_page_token = if self.queue.is_empty() {
            self.done = true;
            String::new()
        } else {
            encode_tree_token(self.root_digest, self.emitted_count)
        };

        self.yielded_any = true;
        Ok(Some(GetTreeResponse {
            directories,
            next_page_token,
        }))
    }
}

#[tonic::async_trait]
impl<C: ContentStore> ContentAddressableStorage for CasService<C> {
    type GetTreeStream = BoxStream<'static, Result<GetTreeResponse, Status>>;
    type GetChunkMappingStream = BoxStream<'static, Result<GetChunkMappingResponse, Status>>;

    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;

        let mut seen = HashSet::with_capacity(req.blob_digests.len());
        let mut query = Vec::with_capacity(req.blob_digests.len());
        for proto in &req.blob_digests {
            let digest = parse_proto_digest(Some(proto), "blob_digests")?;
            if !digest.is_empty() && seen.insert(digest) {
                query.push(digest);
            }
        }

        if query.is_empty() {
            return Ok(Response::new(FindMissingBlobsResponse {
                missing_blob_digests: Vec::new(),
            }));
        }

        let mut missing = Vec::new();
        self.store
            .find_missing(&query, &mut missing)
            .await
            .map_err(|err| map_port_error(&err))?;

        let mut missing_blob_digests = Vec::with_capacity(missing.len());
        for digest in missing {
            let proto = to_proto(&digest)
                .map_err(|err| Status::internal(format!("digest conversion failed: {err}")))?;
            missing_blob_digests.push(proto);
        }

        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;

        let max_batch = self.config.max_batch_total_size_bytes.as_u64();
        let mut total_wire_bytes = 0_u64;
        let mut total_uncompressed_bytes = 0_u64;
        let mut total_inflight_bytes = 0_u64;
        let mut parsed_items = Vec::with_capacity(req.requests.len());

        for item in req.requests {
            let proto = item
                .digest
                .clone()
                .ok_or_else(|| Status::invalid_argument("missing request digest"))?;
            let digest = parse_proto_digest(Some(&proto), "request.digest")?;
            let wire_len = u64::try_from(item.data.len()).unwrap_or(u64::MAX);
            total_wire_bytes = total_wire_bytes.saturating_add(wire_len);
            total_uncompressed_bytes = total_uncompressed_bytes.saturating_add(digest.size());
            if !digest.is_empty() {
                total_inflight_bytes =
                    total_inflight_bytes.saturating_add(wire_len.max(digest.size()));
            }
            parsed_items.push((proto, digest, item.compressor, item.data));
        }

        if total_wire_bytes > max_batch || total_uncompressed_bytes > max_batch {
            return Err(Status::invalid_argument(format!(
                "batch update size exceeds maximum of {max_batch} bytes"
            )));
        }

        let _permit = self.budget.acquire(total_inflight_bytes).await?;

        let mut responses = Vec::with_capacity(parsed_items.len());
        for (proto, digest, compressor_id, data) in parsed_items {
            let compression = match parse_batch_compressor(compressor_id) {
                Ok(comp) => comp,
                Err(message) => {
                    responses.push(batch_update_blobs_response::Response {
                        digest: Some(proto),
                        status: Some(invalid_arg_rpc_status(message)),
                    });
                    continue;
                }
            };

            let decoded = match decode_batch_payload(digest, compression, data) {
                Ok(bytes) => bytes,
                Err(err) => {
                    responses.push(batch_update_blobs_response::Response {
                        digest: Some(proto),
                        status: Some(port_error_to_rpc_status(&err)),
                    });
                    continue;
                }
            };

            if digest.is_empty() {
                responses.push(batch_update_blobs_response::Response {
                    digest: Some(proto),
                    status: Some(ok_rpc_status()),
                });
                continue;
            }

            let status = match self
                .store
                .write(digest, BlobStream::from_bytes(decoded))
                .await
            {
                Ok(()) => ok_rpc_status(),
                Err(err) => port_error_to_rpc_status(&err),
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
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;

        let use_zstd = req
            .acceptable_compressors
            .contains(&(compressor::Value::Zstd as i32));
        let max_batch = self.config.max_batch_total_size_bytes.as_u64();

        let mut total_bytes = 0_u64;
        let mut digests = Vec::with_capacity(req.digests.len());
        for proto in req.digests {
            let digest = parse_proto_digest(Some(&proto), "digests")?;
            total_bytes = total_bytes
                .checked_add(digest.size())
                .ok_or_else(|| Status::invalid_argument("batch read size overflow"))?;
            digests.push((proto, digest));
        }

        if total_bytes > max_batch {
            return Err(Status::invalid_argument(format!(
                "batch read size {total_bytes} exceeds maximum of {max_batch} bytes"
            )));
        }

        let _permit = self.budget.acquire(total_bytes).await?;
        let mut responses = Vec::with_capacity(digests.len());

        for (proto, digest) in digests {
            if digest.is_empty() {
                responses.push(batch_read_blobs_response::Response {
                    digest: Some(proto),
                    data: Bytes::new(),
                    compressor: compressor::Value::Identity as i32,
                    status: Some(ok_rpc_status()),
                });
                continue;
            }

            match self.store.read(digest, ByteRange::FULL).await {
                Ok(None) => {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(proto),
                        data: Bytes::new(),
                        compressor: compressor::Value::Identity as i32,
                        status: Some(not_found_rpc_status(format!("blob {digest} not found"))),
                    });
                }
                Ok(Some(stream)) => match stream.collect().await {
                    Ok(raw) => {
                        let (wire_data, comp_id) = if use_zstd {
                            match compress_zstd(&raw, DEFAULT_ZSTD_LEVEL) {
                                Ok(compressed) => (compressed, compressor::Value::Zstd as i32),
                                Err(err) => {
                                    responses.push(batch_read_blobs_response::Response {
                                        digest: Some(proto),
                                        data: Bytes::new(),
                                        compressor: compressor::Value::Identity as i32,
                                        status: Some(port_error_to_rpc_status(&err)),
                                    });
                                    continue;
                                }
                            }
                        } else {
                            (raw, compressor::Value::Identity as i32)
                        };
                        responses.push(batch_read_blobs_response::Response {
                            digest: Some(proto),
                            data: wire_data,
                            compressor: comp_id,
                            status: Some(ok_rpc_status()),
                        });
                    }
                    Err(err) => {
                        responses.push(batch_read_blobs_response::Response {
                            digest: Some(proto),
                            data: Bytes::new(),
                            compressor: compressor::Value::Identity as i32,
                            status: Some(port_error_to_rpc_status(&err)),
                        });
                    }
                },
                Err(err) => {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(proto),
                        data: Bytes::new(),
                        compressor: compressor::Value::Identity as i32,
                        status: Some(port_error_to_rpc_status(&err)),
                    });
                }
            }
        }

        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;

        let root_digest = parse_proto_digest(req.root_digest.as_ref(), "root_digest")?;
        if req.page_size < 0 {
            return Err(Status::invalid_argument("page_size cannot be negative"));
        }
        let max_page = self.config.max_tree_page_size.get();
        let page_size = if req.page_size == 0 {
            max_page
        } else {
            usize::try_from(req.page_size)
                .unwrap_or(max_page)
                .min(max_page)
        };

        let skip = if req.page_token.is_empty() {
            0
        } else {
            decode_tree_token(&req.page_token, root_digest)?
        };

        let page_byte_limit = self
            .budget
            .capacity()
            .min(self.config.max_batch_total_size_bytes.as_u64());
        if directory_entry_bytes(root_digest.size()) > page_byte_limit {
            return Err(Status::resource_exhausted(
                "Directory exceeds GetTree page byte limit",
            ));
        }
        let root_permit = self.budget.acquire(root_digest.size()).await?;
        let Some(root_dir) = read_directory(self.store.as_ref(), root_digest).await? else {
            return Err(Status::not_found(format!(
                "tree root directory {root_digest} not found"
            )));
        };

        let mut visited = HashSet::new();
        visited.insert(root_digest);
        let mut state = TreeTraversalState {
            store: Arc::clone(&self.store),
            budget: self.budget.clone(),
            root_digest,
            page_size,
            page_byte_limit,
            page_permits: Vec::new(),
            max_directories: self.config.max_tree_directories.get(),
            skip,
            emitted_count: 0,
            queue: VecDeque::new(),
            visited,
            lookahead: None,
            yielded_any: false,
            done: false,
        };
        state.enqueue_children(&root_dir)?;
        state.lookahead = Some((root_dir, root_permit));

        let stream = stream::unfold(state, |mut state| async move {
            match state.next_page().await {
                Ok(Some(page)) => Some((Ok(page), state)),
                Ok(None) => None,
                Err(status) => {
                    state.done = true;
                    state.page_permits.clear();
                    state.lookahead = None;
                    Some((Err(status), state))
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn split_blob(
        &self,
        _request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        Err(Status::unimplemented("SplitBlob is not supported"))
    }

    async fn get_chunk_mapping(
        &self,
        _request: Request<GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Err(Status::unimplemented("GetChunkMapping is not supported"))
    }

    async fn splice_blob(
        &self,
        _request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        Err(Status::unimplemented("SpliceBlob is not supported"))
    }

    async fn register_chunk_mapping(
        &self,
        _request: Request<Streaming<RegisterChunkMappingRequest>>,
    ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterChunkMapping is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt as _;
    use gang_cas::{DEFAULT_ZSTD_LEVEL, compress_zstd};
    use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
    use gang_reapi::build::bazel::remote::execution::v2::{
        BatchReadBlobsRequest, BatchUpdateBlobsRequest, Digest as ProtoDigest, Directory,
        DirectoryNode, FileNode, FindMissingBlobsRequest, GetTreeRequest,
        batch_update_blobs_request, compressor, digest_function,
    };
    use gang_reapi::google::rpc::Code as RpcCode;
    use gang_reapi::{from_proto, to_proto};
    use gang_spi::{BlobStream, ByteRange, ContentStore, EngineStore, PortError};
    use gang_testkit::MemoryContentStore;
    use gang_types::{Digest, NonZeroBytes};
    use parking_lot::Mutex;
    use prost::Message as _;
    use tonic::{Code, Request};

    use super::{CasConfig, CasService, InflightBlobBudget};

    fn service_with(
        store: Arc<MemoryContentStore>,
        budget_bytes: u64,
    ) -> (CasService<MemoryContentStore>, InflightBlobBudget) {
        let cap = NonZeroBytes::new(gang_types::Bytes::new(budget_bytes)).expect("nonzero budget");
        let budget = InflightBlobBudget::new(cap);
        let svc = CasService::with_defaults(store, budget.clone());
        (svc, budget)
    }

    #[tokio::test]
    async fn batch_read_reserves_the_entire_batch_before_reading() {
        let store = Arc::new(MemoryContentStore::new());
        let digest = store.insert(vec![42; 400]);
        let (svc, budget) = service_with(Arc::clone(&store), 1024);
        let held = budget.acquire(500).await.expect("reserve");
        let request = || {
            Request::new(BatchReadBlobsRequest {
                digests: vec![to_proto(&digest).expect("digest"); 2],
                digest_function: digest_function::Value::Sha256 as i32,
                ..Default::default()
            })
        };
        let first = svc.batch_read_blobs(request());
        let second = svc.batch_read_blobs(request());
        tokio::pin!(first, second);
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(store.read_calls(), 0, "no partial batch may be read");
        drop(held);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.expect("first batch").into_inner().responses.len(), 2);
        assert_eq!(
            second.expect("second batch").into_inner().responses.len(),
            2
        );
        assert_eq!(budget.available(), 1024);
    }

    #[tokio::test]
    async fn batch_read_rejects_batch_larger_than_budget() {
        let store = Arc::new(MemoryContentStore::new());
        let digest = store.insert(vec![42; 600]);
        let (svc, _) = service_with(Arc::clone(&store), 1024);
        let error = svc
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                digests: vec![to_proto(&digest).expect("digest"); 2],
                digest_function: digest_function::Value::Sha256 as i32,
                ..Default::default()
            }))
            .await
            .expect_err("batch exceeds budget");
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(store.read_calls(), 0);
    }

    #[tokio::test]
    async fn find_missing_handles_empty_digest_and_invalid_inputs() {
        let store = Arc::new(MemoryContentStore::new());
        let present = store.insert(b"present-blob".to_vec());
        let missing = Digest::from_bytes(b"missing-blob");
        let (svc, _) = service_with(Arc::clone(&store), 1024 * 1024);

        let resp = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: vec![
                    to_proto(&Digest::EMPTY).expect("empty"),
                    to_proto(&present).expect("present"),
                    to_proto(&missing).expect("missing"),
                    to_proto(&missing).expect("duplicate missing"),
                ],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("find_missing_blobs")
            .into_inner();

        assert_eq!(resp.missing_blob_digests.len(), 1);
        assert_eq!(
            from_proto(&resp.missing_blob_digests[0]).expect("valid"),
            missing
        );

        // Only Digest::EMPTY does not even hit ContentStore::find_missing.
        store.reset_calls();
        let empty_only = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: vec![to_proto(&Digest::EMPTY).expect("empty")],
                digest_function: digest_function::Value::Unknown as i32,
            }))
            .await
            .expect("empty only")
            .into_inner();
        assert!(empty_only.missing_blob_digests.is_empty());
        assert_eq!(store.find_missing_calls(), 0);

        // Invalid digest -> INVALID_ARGUMENT.
        let bad = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: vec![ProtoDigest {
                    hash: "not-hex".to_owned(),
                    size_bytes: 1,
                }],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("invalid digest rejected");
        assert_eq!(bad.code(), Code::InvalidArgument);

        // Unsupported digest function -> INVALID_ARGUMENT.
        let bad_fn = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: String::new(),
                blob_digests: Vec::new(),
                digest_function: digest_function::Value::Md5 as i32,
            }))
            .await
            .expect_err("md5 rejected");
        assert_eq!(bad_fn.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn batch_update_and_read_handle_empty_zstd_mismatches_and_misses() {
        let store = Arc::new(MemoryContentStore::new());
        let (svc, budget) = service_with(Arc::clone(&store), 1024 * 1024);

        let raw_payload = Bytes::from_static(b"batch-raw-payload");
        let raw_digest = Digest::from_bytes(&raw_payload);
        let zstd_payload = Bytes::from_static(b"batch-zstd-payload-repeated-repeated-repeated");
        let zstd_digest = Digest::from_bytes(&zstd_payload);
        let zstd_compressed =
            compress_zstd(&zstd_payload, DEFAULT_ZSTD_LEVEL).expect("compress zstd");
        let bad_digest = Digest::from_bytes(b"expected-something-else");

        let update = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![
                    batch_update_blobs_request::Request {
                        digest: Some(to_proto(&Digest::EMPTY).expect("empty")),
                        data: Bytes::new(),
                        compressor: compressor::Value::Identity as i32,
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(to_proto(&raw_digest).expect("raw")),
                        data: raw_payload.clone(),
                        compressor: compressor::Value::Identity as i32,
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(to_proto(&zstd_digest).expect("zstd")),
                        data: zstd_compressed,
                        compressor: compressor::Value::Zstd as i32,
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(to_proto(&bad_digest).expect("bad")),
                        data: Bytes::from_static(b"wrong-bytes"),
                        compressor: compressor::Value::Identity as i32,
                    },
                ],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("batch_update_blobs")
            .into_inner();

        assert_eq!(store.write_calls(), 2, "Digest::EMPTY short-circuits");
        assert_eq!(update.responses.len(), 4);
        assert_eq!(
            update.responses[0].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert_eq!(
            update.responses[1].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert_eq!(
            update.responses[2].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert_eq!(
            update.responses[3].status.as_ref().expect("status").code,
            RpcCode::InvalidArgument as i32
        );
        assert_eq!(budget.available(), budget.capacity());

        // BatchReadBlobs with Zstd accepted + missing blob + Digest::EMPTY.
        let missing = Digest::from_bytes(b"absent-blob");
        store.reset_calls();
        let read = svc
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                instance_name: String::new(),
                digests: vec![
                    to_proto(&Digest::EMPTY).expect("empty"),
                    to_proto(&raw_digest).expect("raw"),
                    to_proto(&missing).expect("missing"),
                ],
                acceptable_compressors: vec![compressor::Value::Zstd as i32],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("batch_read_blobs")
            .into_inner();

        assert_eq!(store.read_calls(), 2, "Digest::EMPTY short-circuits");
        assert_eq!(read.responses.len(), 3);
        assert_eq!(
            read.responses[0].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert!(read.responses[0].data.is_empty());

        assert_eq!(
            read.responses[1].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert_eq!(read.responses[1].compressor, compressor::Value::Zstd as i32);
        let decoded = gang_cas::decompress_zstd_bounded(raw_digest, &read.responses[1].data)
            .expect("decompress batch read");
        assert_eq!(decoded, raw_payload);

        assert_eq!(
            read.responses[2].status.as_ref().expect("status").code,
            RpcCode::NotFound as i32
        );
        assert_eq!(budget.available(), budget.capacity());
    }

    #[tokio::test]
    async fn batch_update_rejects_zstd_bomb_per_item_and_enforces_batch_limit() {
        let store = Arc::new(MemoryContentStore::new());
        let cap = NonZeroBytes::kib(4);
        let budget = InflightBlobBudget::new(cap);
        let svc = CasService::new(
            Arc::clone(&store),
            budget,
            CasConfig {
                max_batch_total_size_bytes: NonZeroBytes::new(gang_types::Bytes::new(64))
                    .expect("64B"),
                ..CasConfig::default()
            },
        );

        let actual = vec![0_u8; 4096];
        let compressed_bomb = compress_zstd(&actual, DEFAULT_ZSTD_LEVEL).expect("compress bomb");
        let claimed = Digest::new([0x22_u8; 32], 32);

        let resp = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(to_proto(&claimed).expect("claimed")),
                    data: compressed_bomb,
                    compressor: compressor::Value::Zstd as i32,
                }],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("returns per-item error")
            .into_inner();
        assert_eq!(
            resp.responses[0].status.as_ref().expect("status").code,
            RpcCode::InvalidArgument as i32
        );

        // Batch total exceeding max_batch_total_size_bytes -> top-level INVALID_ARGUMENT.
        let big = Bytes::from(vec![1_u8; 65]);
        let big_digest = Digest::from_bytes(&big);
        let err = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(to_proto(&big_digest).expect("big")),
                    data: big,
                    compressor: compressor::Value::Identity as i32,
                }],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("oversized batch rejected");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_tree_retains_bounded_pages_until_the_next_poll() {
        for message_limit in [1024, 4096] {
            let store = Arc::new(MemoryContentStore::new());
            let mut children = Vec::new();
            for index in 0..4 {
                let leaf = Directory {
                    files: vec![
                        FileNode {
                            name: format!("{index}{}", "x".repeat(220)),
                            ..Default::default()
                        },
                        FileNode {
                            name: "y".repeat(220),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                let digest = store.insert(leaf.encode_to_vec());
                children.push(DirectoryNode {
                    name: index.to_string(),
                    digest: Some(to_proto(&digest).expect("digest")),
                });
            }
            let root = store.insert(
                Directory {
                    directories: children,
                    ..Default::default()
                }
                .encode_to_vec(),
            );
            let budget = InflightBlobBudget::new(
                NonZeroBytes::new(gang_types::Bytes::new(if message_limit == 1024 {
                    4096
                } else {
                    1024
                }))
                .expect("budget"),
            );
            let service = CasService::new(
                store,
                budget.clone(),
                CasConfig {
                    max_batch_total_size_bytes: NonZeroBytes::new(gang_types::Bytes::new(
                        message_limit,
                    ))
                    .expect("limit"),
                    ..Default::default()
                },
            );
            let mut pages = service
                .get_tree(Request::new(GetTreeRequest {
                    root_digest: Some(to_proto(&root).expect("root")),
                    ..Default::default()
                }))
                .await
                .expect("tree")
                .into_inner();
            let mut count = 0;
            let mut page_count = 0;
            while let Some(page) = tokio::time::timeout(Duration::from_secs(5), pages.next())
                .await
                .expect("page must not deadlock")
            {
                let page = page.expect("page");
                assert!(
                    u64::try_from(page.encoded_len()).expect("length")
                        <= message_limit.min(budget.capacity())
                );
                let retained: usize = page
                    .directories
                    .iter()
                    .map(prost::Message::encoded_len)
                    .sum();
                assert_eq!(
                    budget.capacity() - budget.available(),
                    u64::try_from(retained).expect("retained")
                );
                count += page.directories.len();
                page_count += 1;
            }
            assert_eq!(count, 5);
            assert!(page_count > 1, "tree must split at its byte limit");
            assert_eq!(budget.available(), budget.capacity());
        }
    }

    #[tokio::test]
    async fn concurrent_get_tree_pages_yield_instead_of_holding_and_waiting() {
        let store = Arc::new(MemoryContentStore::new());
        let leaf = Directory {
            files: vec![
                FileNode {
                    name: "x".repeat(220),
                    ..Default::default()
                },
                FileNode {
                    name: "y".repeat(220),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let child = store.insert(leaf.encode_to_vec());
        let root = store.insert(
            Directory {
                directories: (0..4)
                    .map(|index| DirectoryNode {
                        name: index.to_string(),
                        digest: Some(to_proto(&child).expect("child")),
                    })
                    .collect(),
                ..Default::default()
            }
            .encode_to_vec(),
        );
        let (service, budget) = service_with(store, 1024);
        let request = || {
            Request::new(GetTreeRequest {
                root_digest: Some(to_proto(&root).expect("root")),
                ..Default::default()
            })
        };
        let mut first = service
            .get_tree(request())
            .await
            .expect("first")
            .into_inner();
        let mut second = service
            .get_tree(request())
            .await
            .expect("second")
            .into_inner();
        let (first_page, second_page) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first.next(), second.next())
        })
        .await
        .expect("partial pages must yield without waiting for each other");
        assert_eq!(
            first_page
                .expect("first page")
                .expect("page")
                .directories
                .len(),
            1
        );
        assert_eq!(
            second_page
                .expect("second page")
                .expect("page")
                .directories
                .len(),
            1
        );
        let (first_tail, second_tail) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first.collect::<Vec<_>>(), second.collect::<Vec<_>>())
        })
        .await
        .expect("both traversals finish");
        for tail in [first_tail, second_tail] {
            assert_eq!(tail.len(), 1);
            assert_eq!(
                tail[0].as_ref().expect("tail").directories,
                vec![leaf.clone()]
            );
        }
        assert_eq!(budget.available(), budget.capacity());
    }

    #[tokio::test]
    async fn get_tree_paginates_deduplicates_dag_and_handles_missing_nodes() {
        let store = Arc::new(MemoryContentStore::new());
        let shared_leaf = Directory {
            files: vec![FileNode {
                name: "shared.txt".to_owned(),
                digest: Some(to_proto(&store.insert(b"leaf".to_vec())).expect("leaf")),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let shared_digest = store.insert(shared_leaf.encode_to_vec());

        let left = Directory {
            directories: vec![DirectoryNode {
                name: "leaf".to_owned(),
                digest: Some(to_proto(&shared_digest).expect("shared")),
            }],
            ..Default::default()
        };
        let left_digest = store.insert(left.encode_to_vec());

        let right = Directory {
            directories: vec![
                DirectoryNode {
                    name: "leaf".to_owned(),
                    digest: Some(to_proto(&shared_digest).expect("shared")),
                },
                DirectoryNode {
                    name: "missing".to_owned(),
                    digest: Some(
                        to_proto(&Digest::from_bytes(b"missing-dir")).expect("missing dir"),
                    ),
                },
            ],
            ..Default::default()
        };
        let right_digest = store.insert(right.encode_to_vec());

        let root = Directory {
            directories: vec![
                DirectoryNode {
                    name: "left".to_owned(),
                    digest: Some(to_proto(&left_digest).expect("left")),
                },
                DirectoryNode {
                    name: "right".to_owned(),
                    digest: Some(to_proto(&right_digest).expect("right")),
                },
            ],
            ..Default::default()
        };
        let root_digest = store.insert(root.encode_to_vec());

        let (svc, _) = service_with(Arc::clone(&store), 1024 * 1024);

        // Stream all pages with page_size = 2: 4 unique directories -> 2 pages.
        let mut stream = svc
            .get_tree(Request::new(GetTreeRequest {
                instance_name: String::new(),
                root_digest: Some(to_proto(&root_digest).expect("root")),
                page_size: 2,
                page_token: String::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("get_tree")
            .into_inner();

        let page1 = stream.next().await.expect("page1").expect("page1 ok");
        assert_eq!(page1.directories, vec![root.clone(), left.clone()]);
        assert!(!page1.next_page_token.is_empty());

        let page2 = stream.next().await.expect("page2").expect("page2 ok");
        assert_eq!(page2.directories, vec![right.clone(), shared_leaf.clone()]);
        assert!(page2.next_page_token.is_empty());
        assert!(stream.next().await.is_none());

        // Resume from page1's token -> only page2 directories are returned.
        let mut resumed = svc
            .get_tree(Request::new(GetTreeRequest {
                instance_name: String::new(),
                root_digest: Some(to_proto(&root_digest).expect("root")),
                page_size: 2,
                page_token: page1.next_page_token,
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("resumed get_tree")
            .into_inner();
        let resumed_page = resumed
            .next()
            .await
            .expect("resumed page")
            .expect("resumed ok");
        assert_eq!(resumed_page.directories, vec![right, shared_leaf]);
        assert!(resumed_page.next_page_token.is_empty());
        assert!(resumed.next().await.is_none());

        // Missing root -> NOT_FOUND.
        let missing_root = svc
            .get_tree(Request::new(GetTreeRequest {
                instance_name: String::new(),
                root_digest: Some(to_proto(&Digest::from_bytes(b"no-such-root")).expect("root")),
                page_size: 10,
                page_token: String::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await;
        assert!(matches!(missing_root, Err(s) if s.code() == Code::NotFound));
    }

    #[derive(Debug)]
    struct CyclicDirStore {
        dirs: Mutex<HashMap<Digest, Bytes>>,
    }

    impl ContentStore for CyclicDirStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            missing.clear();
            let dirs = self.dirs.lock();
            for &d in digests {
                if !d.is_empty() && !dirs.contains_key(&d) {
                    missing.push(d);
                }
            }
            Ok(())
        }

        async fn read(
            &self,
            digest: Digest,
            _range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            Ok(self
                .dirs
                .lock()
                .get(&digest)
                .cloned()
                .map(BlobStream::from_bytes))
        }

        async fn write(&self, _digest: Digest, _body: BlobStream) -> Result<(), PortError> {
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            None
        }
    }

    #[tokio::test]
    async fn get_tree_breaks_cycles_and_respects_directory_cap() {
        let a_digest = Digest::from_bytes(b"node-a");
        let b_digest = Digest::from_bytes(b"node-b");

        let dir_a = Directory {
            directories: vec![DirectoryNode {
                name: "b".to_owned(),
                digest: Some(to_proto(&b_digest).expect("b")),
            }],
            ..Default::default()
        };
        let dir_b = Directory {
            directories: vec![DirectoryNode {
                name: "back-to-a".to_owned(),
                digest: Some(to_proto(&a_digest).expect("a")),
            }],
            ..Default::default()
        };

        let store = Arc::new(CyclicDirStore {
            dirs: Mutex::new(HashMap::from([
                (a_digest, Bytes::from(dir_a.encode_to_vec())),
                (b_digest, Bytes::from(dir_b.encode_to_vec())),
            ])),
        });
        let budget = InflightBlobBudget::new(NonZeroBytes::mib(1));
        let svc = CasService::new(
            Arc::clone(&store),
            budget,
            CasConfig {
                max_tree_directories: NonZeroUsize::new(10).expect("10"),
                ..CasConfig::default()
            },
        );

        let mut stream = svc
            .get_tree(Request::new(GetTreeRequest {
                instance_name: String::new(),
                root_digest: Some(to_proto(&a_digest).expect("a")),
                page_size: 10,
                page_token: String::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("cyclic tree terminates")
            .into_inner();

        let page = stream.next().await.expect("page").expect("ok");
        assert_eq!(page.directories, vec![dir_a, dir_b]);
        assert!(page.next_page_token.is_empty());
        assert!(stream.next().await.is_none());

        // Enforce max_tree_directories cap.
        let capped = CasService::new(
            store,
            InflightBlobBudget::new(NonZeroBytes::mib(1)),
            CasConfig {
                max_tree_directories: NonZeroUsize::new(1).expect("1"),
                ..CasConfig::default()
            },
        );
        let err = capped
            .get_tree(Request::new(GetTreeRequest {
                instance_name: String::new(),
                root_digest: Some(to_proto(&a_digest).expect("a")),
                page_size: 10,
                page_token: String::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .err()
            .expect("cap exceeded");
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn inflight_budget_blocks_until_permit_released_and_rejects_oversized() {
        let store = Arc::new(MemoryContentStore::new());
        let (svc, budget) = service_with(Arc::clone(&store), 64);

        let payload = Bytes::from(vec![0xab_u8; 40]);
        let digest = Digest::from_bytes(&payload);

        // Hold 40 bytes externally -> only 24 bytes remain, so another 40-byte BatchUpdate waits.
        let held = budget.acquire(40).await.expect("acquire 40");
        assert_eq!(budget.available(), 24);

        let svc_clone = svc.clone();
        let payload_clone = payload.clone();
        let update_task = tokio::spawn(async move {
            svc_clone
                .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                    instance_name: String::new(),
                    requests: vec![batch_update_blobs_request::Request {
                        digest: Some(to_proto(&digest).expect("digest")),
                        data: payload_clone,
                        compressor: compressor::Value::Identity as i32,
                    }],
                    digest_function: digest_function::Value::Sha256 as i32,
                }))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!update_task.is_finished(), "blocked by in-flight budget");

        drop(held);
        let resp = update_task
            .await
            .expect("join")
            .expect("batch update completes")
            .into_inner();
        assert_eq!(
            resp.responses[0].status.as_ref().expect("status").code,
            RpcCode::Ok as i32
        );
        assert_eq!(budget.available(), 64);

        // Single request exceeding total budget capacity (64B) -> RESOURCE_EXHAUSTED.
        let too_big = Bytes::from(vec![0xcd_u8; 65]);
        let too_big_digest = Digest::from_bytes(&too_big);
        let err = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(to_proto(&too_big_digest).expect("digest")),
                    data: too_big,
                    compressor: compressor::Value::Identity as i32,
                }],
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("exceeds budget capacity");
        assert_eq!(err.code(), Code::ResourceExhausted);
    }
}
