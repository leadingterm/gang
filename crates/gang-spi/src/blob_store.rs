//! Content-addressed blob and action-result store ports.

use std::fmt;
use std::num::NonZeroU64;

use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_types::Digest;

use crate::{EngineStore, PortError, port};

/// A streamed blob with its total byte length.
pub struct BlobStream {
    /// Total byte length of the stream (or of the clipped slice for a ranged read).
    pub size: u64,
    /// Ordered byte chunks making up the stream.
    pub chunks: BoxStream<'static, Result<bytes::Bytes, PortError>>,
}

impl BlobStream {
    /// Returns an empty zero-byte stream.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            size: 0,
            chunks: Box::pin(stream::empty()),
        }
    }

    /// Wraps an in-memory buffer as a [`BlobStream`].
    #[must_use]
    pub fn from_bytes(bytes: bytes::Bytes) -> Self {
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let chunks: BoxStream<'static, Result<bytes::Bytes, PortError>> = if bytes.is_empty() {
            Box::pin(stream::empty())
        } else {
            Box::pin(stream::once(std::future::ready(Ok(bytes))))
        };
        Self { size, chunks }
    }

    /// Collects all chunks of the stream into a contiguous [`bytes::Bytes`] buffer.
    ///
    /// # Errors
    ///
    /// Returns the first [`PortError`] yielded by `chunks`.
    pub async fn collect(mut self) -> Result<bytes::Bytes, PortError> {
        let capacity = usize::try_from(self.size.min(1024 * 1024)).unwrap_or(0);
        let mut buffer = bytes::BytesMut::with_capacity(capacity);
        while let Some(chunk) = self.chunks.next().await {
            buffer.extend_from_slice(&chunk?);
        }
        Ok(buffer.freeze())
    }
}

impl fmt::Debug for BlobStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlobStream")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

/// A half-open byte range (`offset .. offset + limit`) for [`ContentStore::read`].
///
/// Follows REAPI `ByteStream.Read` semantics:
/// - `offset == size` yields an empty stream.
/// - `offset > size` fails with [`PortError::Rejected`] and
///   [`Rejection::OutOfRange`](crate::Rejection::OutOfRange).
/// - `limit == None` reads from `offset` to the end of the blob.
/// - `offset + limit > size` is clipped to `size`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// Zero-based byte offset where reading begins.
    pub offset: u64,
    /// Maximum number of bytes to return, or `None` to read to the end.
    pub limit: Option<NonZeroU64>,
}

impl ByteRange {
    /// Reads an entire blob from byte 0 to the end.
    pub const FULL: Self = Self {
        offset: 0,
        limit: None,
    };
}

/// Content-addressed blob storage.
#[port]
pub trait ContentStore: fmt::Debug + Send + Sync + 'static {
    /// Discards cached presence information for blobs known to be absent.
    fn invalidate_existence(&self, digests: &[Digest]) {
        let _ = digests;
    }

    /// Clears `missing`, then pushes each digest of `digests` the store lacks, in input order.
    /// [`Digest::EMPTY`] is never missing.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] when the backend cannot query blob presence.
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError>;

    /// Streams `range` of the blob identified by `digest`, or `Ok(None)` when absent or expired.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with
    /// [`Rejection::OutOfRange`](crate::Rejection::OutOfRange) when `range.offset` exceeds the
    /// blob's size, or another [`PortError`] on backend failure.
    async fn read(&self, digest: Digest, range: ByteRange)
    -> Result<Option<BlobStream>, PortError>;

    /// Stores `body` under `digest`.
    ///
    /// The blob becomes visible only if its bytes hash to `digest`; a mismatch fails with
    /// [`PortError::Rejected`] and
    /// [`Rejection::DigestMismatch`](crate::Rejection::DigestMismatch) and leaves nothing visible.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with
    /// [`Rejection::DigestMismatch`](crate::Rejection::DigestMismatch) if the stream does not
    /// match `digest`, or another [`PortError`] on backend or stream failure.
    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError>;

    /// Usage of an engine-bounded store; `None` for provider-bounded stores (such as S3).
    fn usage(&self) -> Option<EngineStore>;

    /// Drains background cleanup tasks (such as cancelled multipart upload aborts) after callers
    /// have stopped writing and before runtime shutdown.
    async fn shutdown(&self) {}
}

/// Action-result cache storage keyed by action digest.
#[port]
pub trait ActionStore: fmt::Debug + Send + Sync + 'static {
    /// Fetches the cached action result bytes for `action`, or `Ok(None)` when absent.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] when the backend cannot be read.
    async fn get(&self, action: Digest) -> Result<Option<bytes::Bytes>, PortError>;

    /// Stores `result` bytes under `action`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] when the backend cannot write the entry.
    async fn put(&self, action: Digest, result: bytes::Bytes) -> Result<(), PortError>;

    /// Usage of an engine-bounded store; `None` for provider-bounded stores (such as S3).
    fn usage(&self) -> Option<EngineStore>;
}
