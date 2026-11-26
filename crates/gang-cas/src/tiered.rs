//! Tiered fast/slow [`ContentStore`] composition with [`SingleFlight`] coalescing and bypass.

use std::borrow::Borrow;
use std::fmt;
use std::fs::File;
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_spi::{BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection};
use gang_types::Digest;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

use crate::disk::{DiskCas, WriteOutcome};
use crate::flight::SingleFlight;
use crate::layout::blob_path;
use crate::lru::{BlobMode, CacheKey};
use crate::pins::CasPin;

const READ_CHUNK_SIZE: usize = 64 * 1024;
const READ_CHUNK_SIZE_U64: u64 = 64 * 1024;
const TEE_CHANNEL_CAPACITY: usize = 8;

/// Outcome of staging a blob into the local fast [`DiskCas`] via [`Tiered::fetch_to_local`].
#[derive(Debug)]
pub enum FetchOutcome {
    /// [`Digest::EMPTY`] was short-circuited in memory with no CAS entry needed.
    Empty,
    /// The blob is resident in [`DiskCas`] and protected against eviction by an RAII [`CasPin`].
    Cached(CasPin),
    /// The blob could not fit in [`DiskCas`] even after evicting all unpinned entries.
    ///
    /// Callers should stream directly into the action directory via [`Tiered::fetch_direct_to`]
    /// (backed by [`DiskCas::write_verified_direct`]).
    Bypassed,
    /// The blob was absent from both [`DiskCas`] and the slow store.
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlightOutcome {
    CachedFromSlow,
    CachedFromFast,
    Bypassed,
    Missing,
}

#[derive(Debug, Clone)]
struct SharedPortError(Arc<PortError>);

impl SharedPortError {
    fn new(error: PortError) -> Self {
        Self(Arc::new(error))
    }

    fn into_port_error(self) -> PortError {
        match Arc::try_unwrap(self.0) {
            Ok(owned) => owned,
            Err(shared) => {
                let cloned = Self(shared);
                match &*cloned.0 {
                    PortError::Config(_) => PortError::config(cloned),
                    PortError::NotFound(subject) => PortError::NotFound(subject.clone()),
                    PortError::Rejected {
                        reason,
                        source: Some(_),
                    } => PortError::rejected(*reason, cloned),
                    PortError::Rejected {
                        reason,
                        source: None,
                    } => PortError::refused(*reason),
                    PortError::Unsupported { operation } => PortError::Unsupported { operation },
                    PortError::Unavailable(_) => PortError::unavailable(cloned),
                    PortError::Backend(_) => PortError::backend(cloned),
                }
            }
        }
    }
}

impl fmt::Display for SharedPortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, formatter)
    }
}

impl std::error::Error for SharedPortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Composes a fast local [`DiskCas`] with a slow [`ContentStore`], using [`SingleFlight`] to
/// deduplicate concurrent slow fetches and supporting write-through and bypass-to-action-dir.
#[derive(Debug)]
pub struct Tiered<F, S> {
    inner: Arc<TieredInner<F, S>>,
}

impl<F, S> Clone for Tiered<F, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
struct TieredInner<F, S> {
    fast: F,
    slow: S,
    flight: SingleFlight<CacheKey, Result<FlightOutcome, SharedPortError>>,
}

impl<F, S> Tiered<F, S> {
    /// Creates a [`Tiered`] store composing `fast` (`DiskCas`) over `slow` (`ContentStore`).
    #[must_use]
    pub fn new(fast: F, slow: S) -> Self {
        Self {
            inner: Arc::new(TieredInner {
                fast,
                slow,
                flight: SingleFlight::new(),
            }),
        }
    }

    /// Returns a reference to the fast store handle.
    #[must_use]
    pub fn fast(&self) -> &F {
        &self.inner.fast
    }

    /// Returns a reference to the slow store.
    #[must_use]
    pub fn slow(&self) -> &S {
        &self.inner.slow
    }
}

impl<F, S> Tiered<F, S>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    /// Returns a reference to the underlying fast [`DiskCas`].
    #[must_use]
    pub fn disk(&self) -> &DiskCas {
        self.inner.fast.borrow()
    }

    /// Ensures `(digest, executable)` is staged in [`DiskCas`] and protected by an RAII
    /// [`CasPin`], coalescing concurrent slow reads with [`SingleFlight`].
    ///
    /// - [`Digest::EMPTY`] is short-circuited in memory as [`FetchOutcome::Empty`].
    /// - Fast hits record a local read on [`DiskCas`] and return [`FetchOutcome::Cached`].
    /// - Slow misses coalesce via [`SingleFlight`]: one physical slow fetch populates [`DiskCas`]
    ///   and records a slow read, while waiting followers acquire local pins and record local
    ///   reads.
    /// - If the blob cannot fit in [`DiskCas`] even after evicting all unpinned entries, returns
    ///   [`FetchOutcome::Bypassed`] so callers can stream directly to the action directory via
    ///   [`Self::fetch_direct_to`].
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if reading from the slow store or writing/verifying into [`DiskCas`]
    /// fails.
    pub async fn fetch_to_local(
        &self,
        digest: Digest,
        executable: bool,
    ) -> Result<FetchOutcome, PortError> {
        let disk = self.disk();
        if digest.is_empty() {
            disk.stats().record_local_read(0);
            return Ok(FetchOutcome::Empty);
        }

        if let Some(pin) = disk.pin_attempt(digest, executable) {
            disk.stats().record_local_read(digest.size());
            return Ok(FetchOutcome::Cached(pin));
        }

        if let Some(outcome) = try_promote_opposite_mode(disk, digest, executable).await? {
            return Ok(outcome);
        }

        if !disk.can_admit(digest.size()) {
            return Ok(FetchOutcome::Bypassed);
        }

        let key = CacheKey::new(digest, BlobMode::from_executable(executable));
        loop {
            let (outcome, was_leader) = self.coalesce_fetch(key, executable).await?;
            match outcome {
                FlightOutcome::Missing => return Ok(FetchOutcome::Missing),
                FlightOutcome::Bypassed => return Ok(FetchOutcome::Bypassed),
                FlightOutcome::CachedFromSlow | FlightOutcome::CachedFromFast => {
                    if let Some(pin) = disk.pin_attempt(digest, executable) {
                        if !was_leader || outcome == FlightOutcome::CachedFromFast {
                            disk.stats().record_local_read(digest.size());
                        }
                        return Ok(FetchOutcome::Cached(pin));
                    }
                    if !disk.can_admit(digest.size()) {
                        return Ok(FetchOutcome::Bypassed);
                    }
                }
            }
        }
    }

    /// Streams `digest` directly from the slow store into `dest` via
    /// [`DiskCas::write_verified_direct`], bypassing [`DiskCas`] admission and eviction.
    ///
    /// Returns `Ok(true)` if written and verified, or `Ok(false)` if absent from the slow store.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with [`Rejection::DigestMismatch`] if the slow stream does
    /// not match `digest`, or [`PortError`] on slow-store or filesystem failure.
    pub async fn fetch_direct_to(
        &self,
        digest: Digest,
        dest: &Path,
        executable: bool,
    ) -> Result<bool, PortError> {
        let disk = self.disk();
        if digest.is_empty() {
            disk.write_verified_direct(Digest::EMPTY, BlobStream::empty(), dest, executable)
                .await?;
            return Ok(true);
        }

        let Some(stream) = self.inner.slow.read(digest, ByteRange::FULL).await? else {
            return Ok(false);
        };
        disk.write_verified_direct(digest, stream, dest, executable)
            .await?;
        Ok(true)
    }

    async fn coalesce_fetch(
        &self,
        key: CacheKey,
        executable: bool,
    ) -> Result<(FlightOutcome, bool), PortError> {
        let is_leader = Arc::new(AtomicBool::new(false));
        let leader_flag = Arc::clone(&is_leader);
        let inner = Arc::clone(&self.inner);

        let result = self
            .inner
            .flight
            .run_detached(key, move || async move {
                leader_flag.store(true, Ordering::Release);
                fetch_into_disk(&inner, key.digest, executable)
                    .await
                    .map_err(SharedPortError::new)
            })
            .await
            .map_err(SharedPortError::into_port_error)?;

        Ok((result, is_leader.load(Ordering::Acquire)))
    }
}

async fn try_promote_opposite_mode(
    disk: &DiskCas,
    digest: Digest,
    executable: bool,
) -> Result<Option<FetchOutcome>, PortError> {
    let Some(_other_pin) = disk.pin_attempt(digest, !executable) else {
        return Ok(None);
    };
    if !disk.can_admit(digest.size()) {
        return Ok(Some(FetchOutcome::Bypassed));
    }
    let Some(stream) = disk.read(digest, ByteRange::FULL).await? else {
        return Ok(None);
    };
    match disk.write_with_mode(digest, stream, executable).await? {
        WriteOutcome::Cached => {
            if let Some(pin) = disk.pin_attempt(digest, executable) {
                Ok(Some(FetchOutcome::Cached(pin)))
            } else {
                Ok(None)
            }
        }
        WriteOutcome::Bypassed => Ok(Some(FetchOutcome::Bypassed)),
    }
}

async fn fetch_into_disk<F, S>(
    inner: &TieredInner<F, S>,
    digest: Digest,
    executable: bool,
) -> Result<FlightOutcome, PortError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let disk = inner.fast.borrow();
    if disk.pin_attempt(digest, executable).is_some() {
        return Ok(FlightOutcome::CachedFromFast);
    }

    if let Some(_other_pin) = disk.pin_attempt(digest, !executable) {
        if !disk.can_admit(digest.size()) {
            return Ok(FlightOutcome::Bypassed);
        }
        if let Some(stream) = disk.read(digest, ByteRange::FULL).await? {
            return match disk.write_with_mode(digest, stream, executable).await? {
                WriteOutcome::Cached => Ok(FlightOutcome::CachedFromFast),
                WriteOutcome::Bypassed => Ok(FlightOutcome::Bypassed),
            };
        }
    }

    let Some(stream) = inner.slow.read(digest, ByteRange::FULL).await? else {
        return Ok(FlightOutcome::Missing);
    };

    match disk.write_with_mode(digest, stream, executable).await? {
        WriteOutcome::Cached => {
            disk.stats().record_slow_read(digest.size());
            Ok(FlightOutcome::CachedFromSlow)
        }
        WriteOutcome::Bypassed => Ok(FlightOutcome::Bypassed),
    }
}

impl<F, S> ContentStore for Tiered<F, S>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        let mut fast_missing = Vec::new();
        self.disk().find_missing(digests, &mut fast_missing).await?;
        if fast_missing.is_empty() {
            missing.clear();
            return Ok(());
        }
        self.inner.slow.find_missing(&fast_missing, missing).await
    }

    async fn read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        let disk = self.disk();
        if digest.is_empty() {
            if range.offset > 0 {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            disk.stats().record_local_read(0);
            return Ok(Some(BlobStream::empty()));
        }

        if let Some(stream) = disk.read(digest, range).await? {
            return Ok(Some(stream));
        }

        if !disk.can_admit(digest.size()) {
            let slow_stream = self.inner.slow.read(digest, range).await?;
            if let Some(stream) = &slow_stream {
                disk.stats().record_slow_read(stream.size);
            }
            return Ok(slow_stream);
        }

        let key = CacheKey::new(digest, BlobMode::Plain);
        loop {
            let (outcome, was_leader) = self.coalesce_fetch(key, false).await?;
            match outcome {
                FlightOutcome::Missing => return Ok(None),
                FlightOutcome::Bypassed => {
                    let slow_stream = self.inner.slow.read(digest, range).await?;
                    if let Some(stream) = &slow_stream {
                        disk.stats().record_slow_read(stream.size);
                    }
                    return Ok(slow_stream);
                }
                FlightOutcome::CachedFromSlow | FlightOutcome::CachedFromFast => {
                    if range.offset > digest.size() {
                        return Err(PortError::refused(Rejection::OutOfRange));
                    }
                    if was_leader && outcome == FlightOutcome::CachedFromSlow {
                        if let Some(stream) =
                            read_cached_range_without_stats(disk, digest, range).await?
                        {
                            return Ok(Some(stream));
                        }
                    } else if let Some(stream) = disk.read(digest, range).await? {
                        return Ok(Some(stream));
                    }
                    if !disk.can_admit(digest.size()) {
                        let slow_stream = self.inner.slow.read(digest, range).await?;
                        if let Some(stream) = &slow_stream {
                            disk.stats().record_slow_read(stream.size);
                        }
                        return Ok(slow_stream);
                    }
                }
            }
        }
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }

        if digest.is_empty() {
            return verify_empty_stream(body).await;
        }

        let (fast_tx, fast_rx) = mpsc::channel::<Result<Bytes, PortError>>(TEE_CHANNEL_CAPACITY);
        let (slow_tx, slow_rx) = mpsc::channel::<Result<Bytes, PortError>>(TEE_CHANNEL_CAPACITY);

        let fast_stream = BlobStream {
            size: body.size,
            chunks: channel_to_stream(fast_rx),
        };
        let slow_stream = BlobStream {
            size: body.size,
            chunks: channel_to_stream(slow_rx),
        };

        let disk = self.disk();
        let ((), fast_res, slow_res) = tokio::join!(
            pump_verified_tee(digest, body, fast_tx, slow_tx),
            disk.write_with_mode(digest, fast_stream, false),
            self.inner.slow.write(digest, slow_stream),
        );

        let fast_res = fast_res.map(|outcome| match outcome {
            WriteOutcome::Cached | WriteOutcome::Bypassed => (),
        });
        match (fast_res, slow_res) {
            (Err(err @ PortError::Rejected { .. }), _)
            | (_, Err(err @ PortError::Rejected { .. })) => Err(err),
            (fast_res, slow_res) => fast_res.and(slow_res),
        }
    }

    fn usage(&self) -> Option<EngineStore> {
        self.disk().usage()
    }

    async fn shutdown(&self) {
        self.inner.slow.shutdown().await;
    }
}

fn channel_to_stream(
    rx: mpsc::Receiver<Result<Bytes, PortError>>,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

async fn pump_verified_tee(
    digest: Digest,
    mut body: BlobStream,
    fast_tx: mpsc::Sender<Result<Bytes, PortError>>,
    slow_tx: mpsc::Sender<Result<Bytes, PortError>>,
) {
    let mut hasher = Sha256::new();
    let mut seen = 0_u64;
    let mut fast_open = true;
    let mut slow_open = true;

    while let Some(next) = body.chunks.next().await {
        let chunk = match next {
            Ok(chunk) => chunk,
            Err(err) => {
                let shared = SharedPortError::new(err);
                if fast_open {
                    let _ = fast_tx.send(Err(shared.clone().into_port_error())).await;
                }
                if slow_open {
                    let _ = slow_tx.send(Err(shared.into_port_error())).await;
                }
                return;
            }
        };

        if chunk.is_empty() {
            continue;
        }

        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let Some(next_seen) = seen.checked_add(chunk_len) else {
            send_mismatch(fast_open.then_some(&fast_tx), slow_open.then_some(&slow_tx)).await;
            return;
        };
        if next_seen > digest.size() {
            send_mismatch(fast_open.then_some(&fast_tx), slow_open.then_some(&slow_tx)).await;
            return;
        }
        seen = next_seen;
        hasher.update(&chunk);

        if fast_open && fast_tx.send(Ok(chunk.clone())).await.is_err() {
            fast_open = false;
        }
        if slow_open && slow_tx.send(Ok(chunk)).await.is_err() {
            slow_open = false;
        }
        if !fast_open && !slow_open {
            return;
        }
    }

    if seen != digest.size() {
        send_mismatch(fast_open.then_some(&fast_tx), slow_open.then_some(&slow_tx)).await;
        return;
    }
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != *digest.hash() {
        send_mismatch(fast_open.then_some(&fast_tx), slow_open.then_some(&slow_tx)).await;
    }
}

async fn send_mismatch(
    fast_tx: Option<&mpsc::Sender<Result<Bytes, PortError>>>,
    slow_tx: Option<&mpsc::Sender<Result<Bytes, PortError>>>,
) {
    if let Some(tx) = fast_tx {
        let _ = tx
            .send(Err(PortError::refused(Rejection::DigestMismatch)))
            .await;
    }
    if let Some(tx) = slow_tx {
        let _ = tx
            .send(Err(PortError::refused(Rejection::DigestMismatch)))
            .await;
    }
}

async fn verify_empty_stream(mut body: BlobStream) -> Result<(), PortError> {
    while let Some(chunk) = body.chunks.next().await {
        let chunk = chunk?;
        if !chunk.is_empty() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
    }
    Ok(())
}

async fn read_cached_range_without_stats(
    disk: &DiskCas,
    digest: Digest,
    range: ByteRange,
) -> Result<Option<BlobStream>, PortError> {
    let pin = disk
        .pin_attempt(digest, false)
        .or_else(|| disk.pin_attempt(digest, true));
    let Some(pin) = pin else {
        return Ok(None);
    };

    let total = digest.size();
    if range.offset > total {
        return Err(PortError::refused(Rejection::OutOfRange));
    }
    if range.offset == total {
        return Ok(Some(BlobStream::empty()));
    }

    let available = total - range.offset;
    let len = match range.limit {
        Some(limit) => available.min(limit.get()),
        None => available,
    };

    let path = blob_path(disk.root(), pin.key());
    let offset = range.offset;
    let opened = tokio::task::spawn_blocking(move || match File::open(&path) {
        Ok(mut file) => {
            file.seek(SeekFrom::Start(offset))?;
            Ok(Some(file))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    })
    .await
    .map_err(PortError::backend)?
    .map_err(PortError::backend)?;
    drop(pin);

    let Some(file) = opened else {
        return Ok(None);
    };

    let chunks: BoxStream<'static, Result<Bytes, PortError>> =
        Box::pin(stream::unfold(Some((file, len)), |state| async move {
            let (mut file, remaining) = state?;
            if remaining == 0 {
                return None;
            }
            let to_read =
                usize::try_from(remaining.min(READ_CHUNK_SIZE_U64)).unwrap_or(READ_CHUNK_SIZE);

            let step = tokio::task::spawn_blocking(move || {
                let mut buf = vec![0_u8; to_read];
                let read = file.read(&mut buf)?;
                buf.truncate(read);
                Ok::<_, io::Error>((file, Bytes::from(buf)))
            })
            .await;

            match step {
                Ok(Ok((file, bytes))) => {
                    if bytes.is_empty() {
                        Some((
                            Err(PortError::backend(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "cached blob truncated on disk",
                            ))),
                            None,
                        ))
                    } else {
                        let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                        let next_remaining = remaining.saturating_sub(read_len);
                        Some((Ok(bytes), Some((file, next_remaining))))
                    }
                }
                Ok(Err(error)) => Some((Err(PortError::backend(error)), None)),
                Err(join_err) => Some((Err(PortError::backend(join_err)), None)),
            }
        }));

    Ok(Some(BlobStream { size: len, chunks }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use bytes::Bytes;
    use gang_spi::{BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection};
    use gang_testkit::{TestResult, content_store_contract};
    use gang_types::Digest;
    use parking_lot::Mutex;
    use tokio::sync::Notify;

    use super::{FetchOutcome, Tiered};
    use crate::disk::DiskCas;

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(1);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Result<Self, std::io::Error> {
            let seq = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("gang-cas-tiered-test-{}-{seq}", std::process::id()));
            fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Debug, Default)]
    struct MockSlowStore {
        blobs: Mutex<HashMap<Digest, Bytes>>,
        read_calls: AtomicUsize,
        write_calls: AtomicUsize,
        find_missing_calls: AtomicUsize,
        gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
    }

    impl MockSlowStore {
        fn insert(&self, bytes: &[u8]) -> Digest {
            let digest = Digest::from_bytes(bytes);
            self.blobs
                .lock()
                .insert(digest, Bytes::copy_from_slice(bytes));
            digest
        }

        fn set_read_gate(&self, entered: Arc<Notify>, release: Arc<Notify>) {
            *self.gate.lock() = Some((entered, release));
        }

        fn clear_read_gate(&self) {
            *self.gate.lock() = None;
        }
    }

    impl ContentStore for MockSlowStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.find_missing_calls.fetch_add(1, Ordering::SeqCst);
            missing.clear();
            let map = self.blobs.lock();
            for &digest in digests {
                if !digest.is_empty() && !map.contains_key(&digest) {
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
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.lock().clone();
            if let Some((entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }

            if digest.is_empty() {
                if range.offset > 0 {
                    return Err(PortError::refused(Rejection::OutOfRange));
                }
                return Ok(Some(BlobStream::empty()));
            }

            let data = {
                let map = self.blobs.lock();
                map.get(&digest).cloned()
            };
            let Some(data) = data else {
                return Ok(None);
            };

            let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
            if range.offset > total {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            let start = usize::try_from(range.offset).unwrap_or(data.len());
            let available = total - range.offset;
            let len = match range.limit {
                Some(limit) => available.min(limit.get()),
                None => available,
            };
            let end = start.saturating_add(usize::try_from(len).unwrap_or(data.len()));
            Ok(Some(BlobStream::from_bytes(data.slice(start..end))))
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            if body.size != digest.size() {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            let bytes = body.collect().await?;
            let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if actual_size != digest.size() || Digest::from_bytes(&bytes) != digest {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            if !digest.is_empty() {
                self.blobs.lock().insert(digest, bytes);
            }
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            None
        }
    }

    #[tokio::test]
    async fn authoritative_slow_write_survives_optional_fast_bypass() -> TestResult {
        let dir = TempDir::new()?;
        let fast = DiskCas::open(dir.path(), 4).await?;
        let slow = Arc::new(MockSlowStore::default());
        let tiered = Tiered::new(fast.clone(), Arc::clone(&slow));
        let payload = Bytes::from_static(b"larger than fast capacity");
        let digest = Digest::from_bytes(&payload);
        tiered
            .write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        assert!(fast.read(digest, ByteRange::FULL).await?.is_none());
        assert_eq!(
            slow.read(digest, ByteRange::FULL)
                .await?
                .ok_or("authoritative upload")?
                .collect()
                .await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn tiered_passes_content_store_contract() -> TestResult {
        let dir = TempDir::new()?;
        let fast = DiskCas::open(dir.path(), 1024 * 1024).await?;
        let slow = Arc::new(MockSlowStore::default());
        let tiered = Tiered::new(fast, slow);
        content_store_contract(&tiered).await
    }

    #[tokio::test]
    async fn fast_hit_slow_miss_write_through_and_empty_short_circuit() -> TestResult {
        let dir = TempDir::new()?;
        let fast = DiskCas::open(dir.path(), 1024 * 1024).await?;
        let slow = Arc::new(MockSlowStore::default());
        let tiered = Tiered::new(fast.clone(), Arc::clone(&slow));

        // Empty digest short-circuits without touching slow store.
        assert!(matches!(
            tiered.fetch_to_local(Digest::EMPTY, false).await?,
            FetchOutcome::Empty
        ));
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 0);

        // Write-through populates both fast and slow stores.
        let payload = b"write-through-blob";
        let digest = Digest::from_bytes(payload);
        tiered
            .write(digest, BlobStream::from_bytes(Bytes::from_static(payload)))
            .await?;
        assert_eq!(slow.write_calls.load(Ordering::SeqCst), 1);

        // Fast hit does not hit slow store and records local read.
        let outcome = tiered.fetch_to_local(digest, false).await?;
        let FetchOutcome::Cached(pin) = outcome else {
            return Err("expected cached outcome on fast hit".into());
        };
        assert_eq!(pin.digest(), digest);
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fast.worker_cache_stats().local_read_requests, 2);
        drop(pin);

        // Fast miss pulls through from slow store and caches locally.
        let slow_only = b"slow-only-executable";
        let slow_digest = slow.insert(slow_only);
        let missing_digest = Digest::from_bytes(b"never-written");

        let mut missing = Vec::new();
        tiered
            .find_missing(
                &[Digest::EMPTY, digest, slow_digest, missing_digest],
                &mut missing,
            )
            .await?;
        assert_eq!(missing, vec![missing_digest]);
        assert_eq!(slow.find_missing_calls.load(Ordering::SeqCst), 1);

        let fetched = tiered.fetch_to_local(slow_digest, true).await?;
        let FetchOutcome::Cached(exec_pin) = fetched else {
            return Err("expected cached outcome after slow pull".into());
        };
        assert!(exec_pin.mode().is_executable());
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fast.worker_cache_stats().slow_read_requests, 1);
        assert_eq!(
            fast.worker_cache_stats().slow_read_bytes,
            slow_digest.size()
        );

        // Subsequent fetch hits fast store.
        let second = tiered.fetch_to_local(slow_digest, true).await?;
        assert!(matches!(second, FetchOutcome::Cached(_)));
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            tiered.fetch_to_local(missing_digest, false).await?,
            FetchOutcome::Missing
        ));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce_via_single_flight_and_count_follower_local_reads()
    -> TestResult {
        let dir = TempDir::new()?;
        let fast = DiskCas::open(dir.path(), 1024 * 1024).await?;
        let slow = Arc::new(MockSlowStore::default());
        let payload = b"coalesced-payload-1234567890";
        let digest = slow.insert(payload);

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        slow.set_read_gate(Arc::clone(&entered), Arc::clone(&release));

        let tiered = Tiered::new(fast.clone(), Arc::clone(&slow));

        let t0 = tokio::spawn({
            let tiered = tiered.clone();
            async move { tiered.fetch_to_local(digest, false).await }
        });
        entered.notified().await;

        let mut followers = Vec::new();
        for _ in 0..4 {
            let tiered = tiered.clone();
            followers.push(tokio::spawn(async move {
                tiered.fetch_to_local(digest, false).await
            }));
        }
        tokio::task::yield_now().await;
        slow.clear_read_gate();
        release.notify_waiters();

        assert!(matches!(t0.await??, FetchOutcome::Cached(_)));
        for handle in followers {
            assert!(matches!(handle.await??, FetchOutcome::Cached(_)));
        }

        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 1);
        let stats = fast.worker_cache_stats();
        assert_eq!(stats.slow_read_requests, 1);
        assert_eq!(stats.slow_read_bytes, digest.size());
        assert_eq!(stats.local_read_requests, 4);
        assert_eq!(stats.local_read_bytes, digest.size() * 4);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_or_pinned_budget_bypasses_and_streams_direct_to_action_dir() -> TestResult {
        let dir = TempDir::new()?;
        let small = b"pinned-ok";
        let small_digest = Digest::from_bytes(small);
        let big = b"this-oversized-binary-exceeds-fast-cas-budget";
        let big_digest = Digest::from_bytes(big);

        let fast = DiskCas::open(dir.path().join("cas"), small_digest.size() + 8).await?;
        let slow = Arc::new(MockSlowStore::default());
        slow.insert(big);

        let tiered = Tiered::new(fast.clone(), Arc::clone(&slow));
        tiered
            .write(
                small_digest,
                BlobStream::from_bytes(Bytes::from_static(small)),
            )
            .await?;
        let _pin = fast.pin_attempt(small_digest, false).ok_or("pin small")?;

        // `big_digest` cannot fit; `fetch_to_local` returns `Bypassed` without wasting a slow read.
        assert!(matches!(
            tiered.fetch_to_local(big_digest, true).await?,
            FetchOutcome::Bypassed
        ));
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 0);

        // Direct bypass streams directly into action directory with executable permissions.
        let action_bin = dir.path().join("work/attempt-1/bin/tool");
        assert!(
            tiered
                .fetch_direct_to(big_digest, &action_bin, true)
                .await?
        );
        assert_eq!(fs::read(&action_bin)?, big);
        assert_eq!(
            fs::metadata(&action_bin)?.permissions().mode() & 0o777,
            0o555
        );
        assert_eq!(slow.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fast.worker_cache_stats().slow_read_requests, 1);
        assert_eq!(fast.worker_cache_stats().slow_read_bytes, big_digest.size());

        // Fast `small_digest` remained untouched in `DiskCas`.
        assert!(fast.read(small_digest, ByteRange::FULL).await?.is_some());

        // `ContentStore::read` also falls back to slow store when bypassed.
        let read_back = tiered
            .read(big_digest, ByteRange::FULL)
            .await?
            .ok_or("oversized blob readable via slow fallback")?;
        assert_eq!(read_back.collect().await?.as_ref(), big);
        Ok(())
    }
}
