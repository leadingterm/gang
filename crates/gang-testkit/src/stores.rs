//! Reusable contract suite and in-memory doubles for [`ContentStore`] and [`ActionStore`].

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream::{self, BoxStream};
use gang_spi::{
    ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection, StoreKind,
};
use gang_types::Digest;
use parking_lot::{Mutex, RwLock};

use crate::TestResult;

type FaultFactory = Box<dyn Fn() -> PortError + Send + Sync>;

#[derive(Default)]
struct FaultSlot {
    queued: VecDeque<PortError>,
    persistent: Option<FaultFactory>,
}

impl fmt::Debug for FaultSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultSlot")
            .field("queued", &self.queued.len())
            .field("persistent", &self.persistent.is_some())
            .finish()
    }
}

impl FaultSlot {
    fn push_next(&mut self, error: PortError) {
        self.queued.push_back(error);
    }

    fn set_persistent(&mut self, make_error: FaultFactory) {
        self.persistent = Some(make_error);
    }

    fn clear(&mut self) {
        self.queued.clear();
        self.persistent = None;
    }

    fn take(&mut self) -> Result<(), PortError> {
        if let Some(error) = self.queued.pop_front() {
            return Err(error);
        }
        if let Some(make_error) = &self.persistent {
            return Err(make_error());
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ContentFaults {
    find_missing: FaultSlot,
    read: FaultSlot,
    write: FaultSlot,
}

#[derive(Debug, Default)]
struct ContentState {
    blobs: RwLock<HashMap<Digest, Bytes>>,
    usage: Mutex<Option<EngineStore>>,
    faults: Mutex<ContentFaults>,
    find_missing_calls: AtomicUsize,
    read_calls: AtomicUsize,
    write_calls: AtomicUsize,
}

/// An in-memory [`ContentStore`] satisfying the full store contract.
///
/// Clones share the same underlying blob map, usage snapshot, call counters, and injected faults.
#[derive(Debug, Clone, Default)]
pub struct MemoryContentStore {
    state: Arc<ContentState>,
}

impl MemoryContentStore {
    /// Creates an empty in-memory content store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures the initial [`EngineStore`] usage snapshot returned by [`ContentStore::usage`].
    #[must_use]
    pub fn with_usage(self, usage: EngineStore) -> Self {
        self.set_usage(Some(usage));
        self
    }

    /// Sets or clears the [`EngineStore`] usage snapshot returned by [`ContentStore::usage`].
    pub fn set_usage(&self, usage: Option<EngineStore>) {
        *self.state.usage.lock() = usage;
    }

    /// Builds an [`EngineStore`] snapshot from the store's current byte total and the supplied
    /// `budget` and `evicted` counters.
    #[must_use]
    pub fn engine_store(&self, budget: u64, evicted: u64) -> EngineStore {
        EngineStore {
            store: StoreKind::Content,
            used: self.total_bytes(),
            budget,
            evicted,
        }
    }

    /// Inserts `bytes` directly under its SHA-256 [`Digest`] and returns that digest.
    #[must_use]
    pub fn insert(&self, bytes: impl Into<Bytes>) -> Digest {
        let bytes = bytes.into();
        let digest = Digest::from_bytes(&bytes);
        if !digest.is_empty() {
            self.state.blobs.write().insert(digest, bytes);
        }
        digest
    }

    /// Returns the stored bytes for `digest`, or `Bytes::new()` for [`Digest::EMPTY`].
    #[must_use]
    pub fn get_bytes(&self, digest: Digest) -> Option<Bytes> {
        if digest.is_empty() {
            return Some(Bytes::new());
        }
        self.state.blobs.read().get(&digest).cloned()
    }

    /// Returns `true` if `digest` is present (always `true` for [`Digest::EMPTY`]).
    #[must_use]
    pub fn contains(&self, digest: Digest) -> bool {
        digest.is_empty() || self.state.blobs.read().contains_key(&digest)
    }

    /// Removes a stored blob, returning its bytes if present.
    #[must_use]
    pub fn remove(&self, digest: Digest) -> Option<Bytes> {
        self.state.blobs.write().remove(&digest)
    }

    /// Removes all stored blobs.
    pub fn clear(&self) {
        self.state.blobs.write().clear();
    }

    /// Number of non-empty blobs currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.blobs.read().len()
    }

    /// Returns `true` if no non-empty blobs are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.blobs.read().is_empty()
    }

    /// Total byte size of all stored blobs.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.state
            .blobs
            .read()
            .values()
            .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add)
    }

    /// Queues a one-shot error to return from the next [`ContentStore::find_missing`] call.
    pub fn fail_next_find_missing(&self, error: PortError) {
        self.state.faults.lock().find_missing.push_next(error);
    }

    /// Queues a one-shot error to return from the next [`ContentStore::read`] call.
    pub fn fail_next_read(&self, error: PortError) {
        self.state.faults.lock().read.push_next(error);
    }

    /// Queues a one-shot error to return from the next [`ContentStore::write`] call.
    pub fn fail_next_write(&self, error: PortError) {
        self.state.faults.lock().write.push_next(error);
    }

    /// Fails every [`ContentStore::find_missing`] call with errors built by `make_error`.
    pub fn fail_find_missing_with(
        &self,
        make_error: impl Fn() -> PortError + Send + Sync + 'static,
    ) {
        self.state
            .faults
            .lock()
            .find_missing
            .set_persistent(Box::new(make_error));
    }

    /// Fails every [`ContentStore::read`] call with errors built by `make_error`.
    pub fn fail_read_with(&self, make_error: impl Fn() -> PortError + Send + Sync + 'static) {
        self.state
            .faults
            .lock()
            .read
            .set_persistent(Box::new(make_error));
    }

    /// Fails every [`ContentStore::write`] call with errors built by `make_error`.
    pub fn fail_write_with(&self, make_error: impl Fn() -> PortError + Send + Sync + 'static) {
        self.state
            .faults
            .lock()
            .write
            .set_persistent(Box::new(make_error));
    }

    /// Clears all queued and persistent faults.
    pub fn clear_faults(&self) {
        let mut faults = self.state.faults.lock();
        faults.find_missing.clear();
        faults.read.clear();
        faults.write.clear();
    }

    /// Number of [`ContentStore::find_missing`] calls observed.
    #[must_use]
    pub fn find_missing_calls(&self) -> usize {
        self.state.find_missing_calls.load(Ordering::Relaxed)
    }

    /// Number of [`ContentStore::read`] calls observed.
    #[must_use]
    pub fn read_calls(&self) -> usize {
        self.state.read_calls.load(Ordering::Relaxed)
    }

    /// Number of [`ContentStore::write`] calls observed.
    #[must_use]
    pub fn write_calls(&self) -> usize {
        self.state.write_calls.load(Ordering::Relaxed)
    }

    /// Resets all observed call counters to zero.
    pub fn reset_calls(&self) {
        self.state.find_missing_calls.store(0, Ordering::Relaxed);
        self.state.read_calls.store(0, Ordering::Relaxed);
        self.state.write_calls.store(0, Ordering::Relaxed);
    }
}

impl ContentStore for MemoryContentStore {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        self.state
            .find_missing_calls
            .fetch_add(1, Ordering::Relaxed);
        self.state.faults.lock().find_missing.take()?;
        missing.clear();
        let blobs = self.state.blobs.read();
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
        self.state.read_calls.fetch_add(1, Ordering::Relaxed);
        self.state.faults.lock().read.take()?;
        if digest.is_empty() {
            if range.offset > 0 {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            return Ok(Some(BlobStream::empty()));
        }
        let Some(data) = self.state.blobs.read().get(&digest).cloned() else {
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
            data.slice(start..start.saturating_add(len_usize)),
        )))
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        self.state.write_calls.fetch_add(1, Ordering::Relaxed);
        self.state.faults.lock().write.take()?;
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
            self.state.blobs.write().insert(digest, bytes);
        }
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        *self.state.usage.lock()
    }
}

#[derive(Debug, Default)]
struct ActionFaults {
    get: FaultSlot,
    put: FaultSlot,
}

#[derive(Debug, Default)]
struct ActionState {
    entries: RwLock<HashMap<Digest, Bytes>>,
    usage: Mutex<Option<EngineStore>>,
    faults: Mutex<ActionFaults>,
    get_calls: AtomicUsize,
    put_calls: AtomicUsize,
}

/// An in-memory [`ActionStore`] satisfying the action-cache contract.
///
/// Clones share the same underlying entry map, usage snapshot, call counters, and injected faults.
#[derive(Debug, Clone, Default)]
pub struct MemoryActionStore {
    state: Arc<ActionState>,
}

impl MemoryActionStore {
    /// Creates an empty in-memory action store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures the initial [`EngineStore`] usage snapshot returned by [`ActionStore::usage`].
    #[must_use]
    pub fn with_usage(self, usage: EngineStore) -> Self {
        self.set_usage(Some(usage));
        self
    }

    /// Sets or clears the [`EngineStore`] usage snapshot returned by [`ActionStore::usage`].
    pub fn set_usage(&self, usage: Option<EngineStore>) {
        *self.state.usage.lock() = usage;
    }

    /// Builds an [`EngineStore`] snapshot from the store's current byte total and the supplied
    /// `budget` and `evicted` counters.
    #[must_use]
    pub fn engine_store(&self, budget: u64, evicted: u64) -> EngineStore {
        EngineStore {
            store: StoreKind::Action,
            used: self.total_bytes(),
            budget,
            evicted,
        }
    }

    /// Inserts or replaces an action result directly.
    pub fn insert(&self, action: Digest, result: impl Into<Bytes>) {
        self.state.entries.write().insert(action, result.into());
    }

    /// Returns the stored bytes for `action` without incrementing call counters or triggering faults.
    #[must_use]
    pub fn get_bytes(&self, action: Digest) -> Option<Bytes> {
        self.state.entries.read().get(&action).cloned()
    }

    /// Returns `true` if `action` has a stored entry.
    #[must_use]
    pub fn contains(&self, action: Digest) -> bool {
        self.state.entries.read().contains_key(&action)
    }

    /// Removes the stored entry for `action`, returning it if present.
    #[must_use]
    pub fn remove(&self, action: Digest) -> Option<Bytes> {
        self.state.entries.write().remove(&action)
    }

    /// Removes all stored entries.
    pub fn clear(&self) {
        self.state.entries.write().clear();
    }

    /// Number of stored action entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.entries.read().len()
    }

    /// Returns `true` if no action entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.entries.read().is_empty()
    }

    /// Total byte size of all stored action results.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.state
            .entries
            .read()
            .values()
            .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add)
    }

    /// Queues a one-shot error to return from the next [`ActionStore::get`] call.
    pub fn fail_next_get(&self, error: PortError) {
        self.state.faults.lock().get.push_next(error);
    }

    /// Queues a one-shot error to return from the next [`ActionStore::put`] call.
    pub fn fail_next_put(&self, error: PortError) {
        self.state.faults.lock().put.push_next(error);
    }

    /// Fails every [`ActionStore::get`] call with errors built by `make_error`.
    pub fn fail_get_with(&self, make_error: impl Fn() -> PortError + Send + Sync + 'static) {
        self.state
            .faults
            .lock()
            .get
            .set_persistent(Box::new(make_error));
    }

    /// Fails every [`ActionStore::put`] call with errors built by `make_error`.
    pub fn fail_put_with(&self, make_error: impl Fn() -> PortError + Send + Sync + 'static) {
        self.state
            .faults
            .lock()
            .put
            .set_persistent(Box::new(make_error));
    }

    /// Clears all queued and persistent faults.
    pub fn clear_faults(&self) {
        let mut faults = self.state.faults.lock();
        faults.get.clear();
        faults.put.clear();
    }

    /// Number of [`ActionStore::get`] calls observed.
    #[must_use]
    pub fn get_calls(&self) -> usize {
        self.state.get_calls.load(Ordering::Relaxed)
    }

    /// Number of [`ActionStore::put`] calls observed.
    #[must_use]
    pub fn put_calls(&self) -> usize {
        self.state.put_calls.load(Ordering::Relaxed)
    }

    /// Resets all observed call counters to zero.
    pub fn reset_calls(&self) {
        self.state.get_calls.store(0, Ordering::Relaxed);
        self.state.put_calls.store(0, Ordering::Relaxed);
    }
}

impl ActionStore for MemoryActionStore {
    async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
        self.state.get_calls.fetch_add(1, Ordering::Relaxed);
        self.state.faults.lock().get.take()?;
        Ok(self.state.entries.read().get(&action).cloned())
    }

    async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
        self.state.put_calls.fetch_add(1, Ordering::Relaxed);
        self.state.faults.lock().put.take()?;
        self.state.entries.write().insert(action, result);
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        *self.state.usage.lock()
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

    // Failing chunk stream leaves nothing visible.
    let broken_bytes = b"contract-verify-broken-stream";
    let broken_digest = Digest::from_bytes(broken_bytes);
    let broken_stream = BlobStream {
        size: broken_digest.size(),
        chunks: Box::pin(stream::iter([
            Ok(Bytes::from_static(&broken_bytes[..8])),
            Err(PortError::unavailable("stream aborted mid-upload")),
        ])),
    };
    assert!(matches!(
        store.write(broken_digest, broken_stream).await,
        Err(PortError::Unavailable(_))
    ));
    assert!(store.read(broken_digest, ByteRange::FULL).await?.is_none());

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

    #[tokio::test]
    async fn memory_content_store_doubles_support_seeding_usage_faults_and_cloning() -> TestResult {
        let store = MemoryContentStore::new();
        let peer = store.clone();

        let digest = store.insert(Bytes::from_static(b"seeded-blob"));
        assert!(peer.contains(digest));
        assert!(peer.contains(Digest::EMPTY));
        assert_eq!(
            peer.get_bytes(digest).as_deref(),
            Some(b"seeded-blob".as_slice())
        );
        assert_eq!(peer.len(), 1);
        assert!(!peer.is_empty());
        assert_eq!(peer.total_bytes(), 11);

        assert_eq!(store.usage(), None);
        let usage = store.engine_store(64, 4);
        assert_eq!(
            usage,
            EngineStore {
                store: StoreKind::Content,
                used: 11,
                budget: 64,
                evicted: 4,
            }
        );
        let store = store.with_usage(usage);
        assert_eq!(peer.usage(), Some(usage));

        store.fail_next_find_missing(PortError::unavailable("find down"));
        store.fail_next_read(PortError::backend("read down"));
        store.fail_next_write(PortError::unavailable("write down"));

        let mut missing = Vec::new();
        assert!(matches!(
            peer.find_missing(&[digest], &mut missing).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            peer.read(digest, ByteRange::FULL).await,
            Err(PortError::Backend(_))
        ));
        let fresh = Bytes::from_static(b"fresh-blob");
        let fresh_digest = Digest::from_bytes(&fresh);
        assert!(matches!(
            peer.write(fresh_digest, BlobStream::from_bytes(fresh.clone()))
                .await,
            Err(PortError::Unavailable(_))
        ));

        // One-shot faults are consumed; persistent faults repeat until cleared.
        peer.find_missing(&[digest], &mut missing).await?;
        assert!(missing.is_empty());
        store.fail_read_with(|| PortError::unavailable("persistent read fault"));
        assert!(matches!(
            peer.read(digest, ByteRange::FULL).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            peer.read(digest, ByteRange::FULL).await,
            Err(PortError::Unavailable(_))
        ));
        store.clear_faults();
        assert!(peer.read(digest, ByteRange::FULL).await?.is_some());

        assert_eq!(store.find_missing_calls(), 2);
        assert_eq!(store.read_calls(), 4);
        assert_eq!(store.write_calls(), 1);
        store.reset_calls();
        assert_eq!(
            (
                store.find_missing_calls(),
                store.read_calls(),
                store.write_calls()
            ),
            (0, 0, 0)
        );

        assert_eq!(
            store.remove(digest).as_deref(),
            Some(b"seeded-blob".as_slice())
        );
        assert!(!peer.contains(digest));
        assert!(peer.is_empty());
        let _ = store.insert(b"another".as_slice());
        peer.clear();
        assert_eq!(store.total_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_action_store_doubles_support_seeding_usage_faults_and_cloning() -> TestResult {
        let store = MemoryActionStore::new();
        let peer = store.clone();
        let action = Digest::from_bytes(b"action-key");

        store.insert(action, Bytes::from_static(b"result-bytes"));
        assert!(peer.contains(action));
        assert_eq!(
            peer.get_bytes(action).as_deref(),
            Some(b"result-bytes".as_slice())
        );
        assert_eq!(peer.len(), 1);
        assert!(!peer.is_empty());
        assert_eq!(peer.total_bytes(), 12);

        let usage = store.engine_store(128, 2);
        assert_eq!(
            usage,
            EngineStore {
                store: StoreKind::Action,
                used: 12,
                budget: 128,
                evicted: 2,
            }
        );
        let store = store.with_usage(usage);
        assert_eq!(peer.usage(), Some(usage));

        store.fail_next_get(PortError::unavailable("ac get down"));
        store.fail_next_put(PortError::backend("ac put down"));
        assert!(matches!(
            peer.get(action).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            peer.put(action, Bytes::from_static(b"updated")).await,
            Err(PortError::Backend(_))
        ));

        store.fail_get_with(|| PortError::unavailable("persistent ac get"));
        assert!(matches!(
            peer.get(action).await,
            Err(PortError::Unavailable(_))
        ));
        store.clear_faults();
        assert_eq!(
            peer.get(action).await?,
            Some(Bytes::from_static(b"result-bytes"))
        );
        assert_eq!(store.get_calls(), 3);
        assert_eq!(store.put_calls(), 1);
        store.reset_calls();
        assert_eq!((store.get_calls(), store.put_calls()), (0, 0));

        assert_eq!(
            store.remove(action).as_deref(),
            Some(b"result-bytes".as_slice())
        );
        assert!(peer.is_empty());
        store.insert(action, Bytes::from_static(b"x"));
        peer.clear();
        assert_eq!(store.total_bytes(), 0);
        Ok(())
    }
}
