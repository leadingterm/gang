//! Reusable contract suite and in-memory doubles for [`ContentStore`] and [`ActionStore`].

use std::collections::HashMap;
use std::num::NonZeroU64;

use bytes::Bytes;
use futures::stream::{self, BoxStream};
use gang_spi::{
    ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection,
};
use gang_types::Digest;
use parking_lot::RwLock;

use crate::TestResult;

/// An in-memory [`ContentStore`] satisfying the full store contract.
#[derive(Debug, Default)]
pub struct MemoryContentStore {
    blobs: RwLock<HashMap<Digest, Bytes>>,
}

impl MemoryContentStore {
    /// Creates an empty in-memory content store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ContentStore for MemoryContentStore {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        missing.clear();
        let blobs = self.blobs.read();
        for &digest in digests {
            if !digest.is_empty() && !blobs.contains_key(&digest) {
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
        let Some(data) = self.blobs.read().get(&digest).cloned() else {
            return Ok(None);
        };
        let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
        if range.offset > total {
            return Err(PortError::refused(Rejection::OutOfRange));
        }
        if range.offset == total {
            return Ok(Some(BlobStream::empty()));
        }
        let start =
            usize::try_from(range.offset).map_err(|_| PortError::refused(Rejection::OutOfRange))?;
        let available = total - range.offset;
        let len = match range.limit {
            Some(limit) => available.min(limit.get()),
            None => available,
        };
        let len_usize = usize::try_from(len).unwrap_or(usize::MAX);
        Ok(Some(BlobStream::from_bytes(
            data.slice(start..start + len_usize),
        )))
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        let declared_size = body.size;
        let bytes = body.collect().await?;
        let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if declared_size != digest.size()
            || actual_size != digest.size()
            || Digest::from_bytes(&bytes) != digest
        {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        if !digest.is_empty() {
            self.blobs.write().insert(digest, bytes);
        }
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }
}

/// An in-memory [`ActionStore`] satisfying the action-cache contract.
#[derive(Debug, Default)]
pub struct MemoryActionStore {
    entries: RwLock<HashMap<Digest, Bytes>>,
}

impl MemoryActionStore {
    /// Creates an empty in-memory action store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ActionStore for MemoryActionStore {
    async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
        Ok(self.entries.read().get(&action).cloned())
    }

    async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
        self.entries.write().insert(action, result);
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }
}

fn assert_rejection<T: std::fmt::Debug>(
    result: Result<T, PortError>,
    expected: Rejection,
) -> TestResult {
    match result {
        Err(PortError::Rejected { reason, .. }) if reason == expected => Ok(()),
        other => Err(format!("expected Rejection::{expected:?}, got {other:?}").into()),
    }
}

fn multi_chunk_stream(chunks: &[&'static [u8]], size: u64) -> BlobStream {
    let items: Vec<Result<Bytes, PortError>> = chunks
        .iter()
        .map(|chunk| Ok(Bytes::from_static(chunk)))
        .collect();
    let stream: BoxStream<'static, Result<Bytes, PortError>> = Box::pin(stream::iter(items));
    BlobStream {
        size,
        chunks: stream,
    }
}

/// Runs the full [`ContentStore`] contract suite against `store`.
///
/// # Errors
///
/// Returns any unexpected [`PortError`] or contract assertion failure.
pub async fn content_store_contract<S: ContentStore>(store: &S) -> TestResult {
    content_empty_digest(store).await?;
    content_write_and_read_ranges(store).await?;
    content_write_verification(store).await?;
    Ok(())
}

/// Verifies [`Digest::EMPTY`] handling on `find_missing`, `read`, and `write`.
///
/// # Errors
///
/// Returns any unexpected [`PortError`] or contract assertion failure.
///
/// # Panics
///
/// Panics if [`Digest::EMPTY`] is reported missing or returns non-empty bytes.
pub async fn content_empty_digest<S: ContentStore>(store: &S) -> TestResult {
    let probe_a = Digest::from_bytes(b"contract-empty-probe-a");
    let probe_b = Digest::from_bytes(b"contract-empty-probe-b");

    let mut missing = vec![Digest::from_bytes(b"stale-entry-to-be-cleared")];
    store
        .find_missing(
            &[Digest::EMPTY, probe_a, Digest::EMPTY, probe_b],
            &mut missing,
        )
        .await?;
    assert_eq!(missing, vec![probe_a, probe_b]);

    let empty_stream = store
        .read(Digest::EMPTY, ByteRange::FULL)
        .await?
        .ok_or("Digest::EMPTY must always be readable")?;
    assert_eq!(empty_stream.size, 0);
    assert!(empty_stream.collect().await?.is_empty());

    let bounded_empty = store
        .read(
            Digest::EMPTY,
            ByteRange {
                offset: 0,
                limit: NonZeroU64::new(4),
            },
        )
        .await?
        .ok_or("Digest::EMPTY with bounded limit must be readable")?;
    assert_eq!(bounded_empty.size, 0);
    assert!(bounded_empty.collect().await?.is_empty());

    assert_rejection(
        store
            .read(
                Digest::EMPTY,
                ByteRange {
                    offset: 1,
                    limit: None,
                },
            )
            .await,
        Rejection::OutOfRange,
    )?;

    store.write(Digest::EMPTY, BlobStream::empty()).await?;
    assert_rejection(
        store
            .write(
                Digest::EMPTY,
                BlobStream::from_bytes(Bytes::from_static(b"nonempty")),
            )
            .await,
        Rejection::DigestMismatch,
    )?;
    Ok(())
}

/// Verifies `find_missing`, multi-chunk writes, and all [`ByteRange`] boundary semantics.
///
/// # Errors
///
/// Returns any unexpected [`PortError`] or contract assertion failure.
///
/// # Panics
///
/// Panics if presence ordering, full reads, or ranged read edges diverge from the contract.
pub async fn content_write_and_read_ranges<S: ContentStore>(store: &S) -> TestResult {
    let payload = b"0123456789";
    let digest = Digest::from_bytes(payload);
    let missing_first = Digest::from_bytes(b"contract-range-missing-1");
    let missing_second = Digest::from_bytes(b"contract-range-missing-2");

    assert!(store.read(digest, ByteRange::FULL).await?.is_none());

    store
        .write(digest, multi_chunk_stream(&[b"012", b"3456", b"789"], 10))
        .await?;

    let mut missing = Vec::new();
    store
        .find_missing(&[missing_first, digest, missing_second], &mut missing)
        .await?;
    assert_eq!(missing, vec![missing_first, missing_second]);

    let full = store
        .read(digest, ByteRange::FULL)
        .await?
        .ok_or("written blob must exist")?;
    assert_eq!(full.size, 10);
    assert_eq!(full.collect().await?.as_ref(), payload);

    let tail = store
        .read(
            digest,
            ByteRange {
                offset: 3,
                limit: None,
            },
        )
        .await?
        .ok_or("tail range must exist")?;
    assert_eq!(tail.size, 7);
    assert_eq!(tail.collect().await?.as_ref(), b"3456789");

    let slice = store
        .read(
            digest,
            ByteRange {
                offset: 2,
                limit: NonZeroU64::new(4),
            },
        )
        .await?
        .ok_or("interior slice must exist")?;
    assert_eq!(slice.size, 4);
    assert_eq!(slice.collect().await?.as_ref(), b"2345");

    assert_range_edges(store, digest).await
}

async fn assert_range_edges<S: ContentStore>(store: &S, digest: Digest) -> TestResult {
    // `offset + limit > size` is clipped to `size`.
    let clipped = store
        .read(
            digest,
            ByteRange {
                offset: 7,
                limit: NonZeroU64::new(10),
            },
        )
        .await?
        .ok_or("clipped range must exist")?;
    assert_eq!(clipped.size, 3);
    assert_eq!(clipped.collect().await?.as_ref(), b"789");

    // `offset == size` yields an empty stream (with `limit: None` and `limit: Some(_)`).
    let at_end_open = store
        .read(
            digest,
            ByteRange {
                offset: 10,
                limit: None,
            },
        )
        .await?
        .ok_or("offset == size must return empty stream")?;
    assert_eq!(at_end_open.size, 0);
    assert!(at_end_open.collect().await?.is_empty());

    let at_end_limited = store
        .read(
            digest,
            ByteRange {
                offset: 10,
                limit: NonZeroU64::new(5),
            },
        )
        .await?
        .ok_or("offset == size with limit must return empty stream")?;
    assert_eq!(at_end_limited.size, 0);
    assert!(at_end_limited.collect().await?.is_empty());

    // `offset > size` fails with `Rejection::OutOfRange`.
    assert_rejection(
        store
            .read(
                digest,
                ByteRange {
                    offset: 11,
                    limit: None,
                },
            )
            .await,
        Rejection::OutOfRange,
    )?;
    assert_rejection(
        store
            .read(
                digest,
                ByteRange {
                    offset: 11,
                    limit: NonZeroU64::new(1),
                },
            )
            .await,
        Rejection::OutOfRange,
    )?;

    Ok(())
}

/// Verifies that mismatched writes fail with [`Rejection::DigestMismatch`] and leave nothing
/// visible, while valid repeated writes remain idempotent.
///
/// # Errors
///
/// Returns any unexpected [`PortError`] or contract assertion failure.
///
/// # Panics
///
/// Panics if a mismatched blob becomes visible or a valid re-write fails.
pub async fn content_write_verification<S: ContentStore>(store: &S) -> TestResult {
    let expected = b"contract-verify-expected";
    let digest = Digest::from_bytes(expected);
    let wrong_same_len = b"contract-verify-mismatch";
    assert_eq!(expected.len(), wrong_same_len.len());

    // Same length, wrong hash.
    assert_rejection(
        store
            .write(
                digest,
                BlobStream::from_bytes(Bytes::from_static(wrong_same_len)),
            )
            .await,
        Rejection::DigestMismatch,
    )?;
    assert!(store.read(digest, ByteRange::FULL).await?.is_none());

    // Wrong byte length.
    let short_digest = Digest::from_bytes(b"contract-verify-wrong-len");
    assert_rejection(
        store
            .write(
                short_digest,
                BlobStream::from_bytes(Bytes::from_static(b"short")),
            )
            .await,
        Rejection::DigestMismatch,
    )?;
    let mut missing = Vec::new();
    store
        .find_missing(&[digest, short_digest], &mut missing)
        .await?;
    assert_eq!(missing, vec![digest, short_digest]);

    // Declared stream size differs from digest size even if chunk bytes match.
    let bad_size_digest = Digest::from_bytes(b"contract-verify-bad-size-field");
    let mismatched_header = BlobStream {
        size: bad_size_digest.size() + 1,
        chunks: Box::pin(stream::once(std::future::ready(Ok(Bytes::from_static(
            b"contract-verify-bad-size-field",
        ))))),
    };
    assert_rejection(
        store.write(bad_size_digest, mismatched_header).await,
        Rejection::DigestMismatch,
    )?;
    assert!(
        store
            .read(bad_size_digest, ByteRange::FULL)
            .await?
            .is_none()
    );

    // Valid write followed by idempotent duplicate write.
    let idempotent_bytes = b"contract-verify-idempotent";
    let idempotent_digest = Digest::from_bytes(idempotent_bytes);
    store
        .write(
            idempotent_digest,
            BlobStream::from_bytes(Bytes::from_static(idempotent_bytes)),
        )
        .await?;
    store
        .write(
            idempotent_digest,
            BlobStream::from_bytes(Bytes::from_static(idempotent_bytes)),
        )
        .await?;
    let stream = store
        .read(idempotent_digest, ByteRange::FULL)
        .await?
        .ok_or("idempotent blob must be readable")?;
    assert_eq!(stream.collect().await?.as_ref(), idempotent_bytes);

    Ok(())
}

/// Runs the [`ActionStore`] get/put roundtrip contract against `store`.
///
/// # Errors
///
/// Returns any unexpected [`PortError`] or contract assertion failure.
///
/// # Panics
///
/// Panics if absent entries return data or `get`/`put` values diverge.
pub async fn action_store_contract<S: ActionStore>(store: &S) -> TestResult {
    let action_a = Digest::from_bytes(b"contract-action-a");
    let action_b = Digest::from_bytes(b"contract-action-b");
    let result_a1 = Bytes::from_static(b"encoded-action-result-a1");
    let result_a2 = Bytes::from_static(b"encoded-action-result-a2");
    let result_b = Bytes::new();

    assert_eq!(store.get(action_a).await?, None);

    store.put(action_a, result_a1.clone()).await?;
    assert_eq!(store.get(action_a).await?, Some(result_a1));
    assert_eq!(store.get(action_b).await?, None);

    store.put(action_b, result_b.clone()).await?;
    assert_eq!(store.get(action_b).await?, Some(result_b));

    store.put(action_a, result_a2.clone()).await?;
    assert_eq!(store.get(action_a).await?, Some(result_a2));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_content_store_passes_contract() -> TestResult {
        let store = MemoryContentStore::new();
        content_store_contract(&store).await
    }

    #[tokio::test]
    async fn memory_action_store_passes_contract() -> TestResult {
        let store = MemoryActionStore::new();
        action_store_contract(&store).await
    }
}
