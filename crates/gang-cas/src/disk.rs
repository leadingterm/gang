//! Local `NVMe` content-addressed disk store (`DiskCas`) implementing [`ContentStore`].

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_spi::{
    BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection, StoreKind,
    WorkerCacheStats,
};
use gang_types::Digest;
use tokio::sync::mpsc;

use crate::layout::{
    self, VerifiedWriteError, VerifiedWriter, blob_path, copy_with_metadata, create_empty_file,
    hardlink_or_reflink_file,
};
use crate::lru::{BlobMode, CacheKey, InsertOutcome, PinKind, ResidencyPin, ShardedLru};
use crate::pins::CasPin;
use crate::stats::CasStats;

const READ_CHUNK_SIZE: usize = 64 * 1024;
const READ_CHUNK_SIZE_U64: u64 = 64 * 1024;
const WRITE_CHANNEL_CAPACITY: usize = 8;

static EVICT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Outcome of writing a verified blob to [`DiskCas`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The blob was admitted into `DiskCas` (or was already resident).
    Cached,
    /// Evicting every unpinned entry still could not fit the blob within `budget_bytes`.
    ///
    /// Existing warm entries were left untouched; callers needing the bytes on disk should stream
    /// directly to the action directory via [`DiskCas::write_verified_direct`].
    Bypassed,
}

enum WriteChunk {
    Data(Bytes),
    Commit,
}

/// Fast local `NVMe` content-addressed store backed by a 256-way sharded directory tree and a
/// 16-way sharded O(1) LRU index.
#[derive(Debug, Clone)]
pub struct DiskCas {
    inner: Arc<DiskCasInner>,
}

#[derive(Debug)]
struct DiskCasInner {
    root: PathBuf,
    budget_bytes: u64,
    lru: Arc<ShardedLru>,
    stats: CasStats,
}

impl DiskCas {
    /// Opens or initializes a [`DiskCas`] rooted at `root` with eviction budget `budget_bytes`.
    ///
    /// Performs a parallel startup scan over existing shard directories, cleans stale temporary
    /// and malformed files, verifies file sizes against their digest suffixes, reconstructs the
    /// sharded LRU from `atime`/`ctime`, and evicts any excess above `budget_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if `root` cannot be scanned or created.
    pub async fn open(root: impl AsRef<Path>, budget_bytes: u64) -> Result<Self, PortError> {
        let root_path = root.as_ref().to_path_buf();
        let report = tokio::task::spawn_blocking(move || {
            let scan = layout::scan_cas_root(&root_path)?;
            Ok::<_, io::Error>((root_path, scan))
        })
        .await
        .map_err(PortError::backend)?
        .map_err(PortError::backend)?;

        let (root_path, scan) = report;
        let lru = Arc::new(ShardedLru::new());
        for entry in scan.entries {
            lru.insert_with_stamp(entry.key, entry.size, entry.stamp);
        }
        lru.rebuild_startup_order();

        let stats = CasStats::new(budget_bytes);
        stats.set_size_bytes(lru.size_bytes());

        let cas = Self {
            inner: Arc::new(DiskCasInner {
                root: root_path,
                budget_bytes,
                lru,
                stats,
            }),
        };

        if cas.inner.lru.size_bytes() > budget_bytes {
            cas.evict_and_unlink_until(budget_bytes).await?;
        }
        Ok(cas)
    }

    /// Returns the filesystem root directory of this store.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Returns the configured byte budget of this store.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.inner.budget_bytes
    }

    /// Returns the atomic [`CasStats`] for this store.
    #[must_use]
    pub fn stats(&self) -> &CasStats {
        &self.inner.stats
    }

    /// Returns a [`WorkerCacheStats`] snapshot of this store's metrics.
    #[must_use]
    pub fn worker_cache_stats(&self) -> WorkerCacheStats {
        self.inner.stats.worker_cache_stats()
    }

    /// Returns distinct bytes currently held by [`PinKind::Tree`] pins.
    #[must_use]
    pub fn tree_pinned_bytes(&self) -> u64 {
        self.inner.lru.tree_pinned_bytes()
    }

    /// Returns `true` if a blob of `size` bytes can fit within `budget_bytes` after evicting
    /// unpinned entries (i.e. `size + pinned_bytes <= budget_bytes`).
    #[must_use]
    pub fn can_admit(&self, size: u64) -> bool {
        if size == 0 {
            return true;
        }
        size <= self.inner.budget_bytes
            && self.inner.lru.pinned_bytes().saturating_add(size) <= self.inner.budget_bytes
    }

    /// Acquires an RAII [`PinKind::Attempt`] pin for `(digest, executable)` if resident.
    ///
    /// [`Digest::EMPTY`] is always considered resident and returns an inert pin.
    #[must_use]
    pub fn pin_attempt(&self, digest: Digest, executable: bool) -> Option<CasPin> {
        let key = CacheKey::new(digest, BlobMode::from_executable(executable));
        if digest.is_empty() {
            return Some(CasPin::inert(key, PinKind::Attempt, 0));
        }
        self.inner
            .lru
            .pin_generation(key, PinKind::Attempt)
            .map(|generation| {
                CasPin::new(
                    Arc::clone(&self.inner.lru),
                    key,
                    PinKind::Attempt,
                    digest.size(),
                    Some(generation),
                )
            })
    }

    /// Acquires an RAII [`PinKind::Tree`] pin for `(digest, executable)` if resident and within
    /// `max_tree_pinned_bytes`.
    ///
    /// [`Digest::EMPTY`] always returns an inert pin.
    #[must_use]
    pub fn pin_tree(
        &self,
        digest: Digest,
        executable: bool,
        max_tree_pinned_bytes: u64,
    ) -> Option<CasPin> {
        let key = CacheKey::new(digest, BlobMode::from_executable(executable));
        if digest.is_empty() {
            return Some(CasPin::inert(key, PinKind::Tree, 0));
        }
        self.inner
            .lru
            .pin_tree_within_cap_generation(key, max_tree_pinned_bytes)
            .map(|generation| {
                CasPin::new(
                    Arc::clone(&self.inner.lru),
                    key,
                    PinKind::Tree,
                    digest.size(),
                    Some(generation),
                )
            })
    }

    /// Streams, verifies, and either stores `body` in `DiskCas` (for `executable` mode) or reports
    /// [`WriteOutcome::Bypassed`] when unpinned eviction cannot make room within `budget_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with [`Rejection::DigestMismatch`] if `body` does not match
    /// `digest`, or [`PortError::Backend`] on filesystem or stream failure.
    pub async fn write_with_mode(
        &self,
        digest: Digest,
        body: BlobStream,
        executable: bool,
    ) -> Result<WriteOutcome, PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        if digest.is_empty() {
            verify_empty_stream(body).await?;
            return Ok(WriteOutcome::Cached);
        }

        let mode = BlobMode::from_executable(executable);
        let key = CacheKey::new(digest, mode);

        if let Some(pin) = self.inner.lru.pin_resident(key) {
            verify_stream_only(digest, body).await?;
            if !pin.touch() {
                return Err(PortError::backend(io::Error::new(
                    io::ErrorKind::NotFound,
                    "cached blob residency changed while verifying duplicate",
                )));
            }
            return Ok(WriteOutcome::Cached);
        }

        if !self.can_admit(digest.size()) {
            verify_stream_only(digest, body).await?;
            return Ok(WriteOutcome::Bypassed);
        }

        let target_after_evict = self.inner.budget_bytes.saturating_sub(digest.size());
        if self.inner.lru.size_bytes() > target_after_evict {
            self.evict_and_unlink_until(target_after_evict).await?;
        }
        if self.inner.lru.size_bytes() > target_after_evict {
            // Concurrent pins grew while evicting; bypass rather than exceeding budget.
            verify_stream_only(digest, body).await?;
            return Ok(WriteOutcome::Bypassed);
        }

        let writer = {
            let root = self.inner.root.clone();
            tokio::task::spawn_blocking(move || VerifiedWriter::open_cas(&root, key))
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::backend)?
        };

        stream_into_verified_writer(body, writer, Some((Arc::clone(&self.inner), key))).await?;

        if self.inner.lru.size_bytes() > self.inner.budget_bytes {
            self.evict_and_unlink_until(self.inner.budget_bytes).await?;
        }

        Ok(WriteOutcome::Cached)
    }

    /// Streams `body` directly to `dest` (bypassing `DiskCas` admission and eviction), verifies
    /// SHA-256 and byte length, applies deterministic mtime and mode, and counts a slow read.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with [`Rejection::DigestMismatch`] on mismatch (leaving no
    /// file at `dest`), or [`PortError::Backend`] on filesystem or stream failure.
    pub async fn write_verified_direct(
        &self,
        digest: Digest,
        body: BlobStream,
        dest: &Path,
        executable: bool,
    ) -> Result<(), PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        let mode = BlobMode::from_executable(executable);
        if digest.is_empty() {
            verify_empty_stream(body).await?;
            let dest_owned = dest.to_path_buf();
            tokio::task::spawn_blocking(move || create_empty_file(&dest_owned, mode))
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::backend)?;
            self.inner.stats.record_slow_read(0);
            return Ok(());
        }

        let dest_owned = dest.to_path_buf();
        let writer = tokio::task::spawn_blocking(move || {
            VerifiedWriter::open_direct(&dest_owned, digest, mode)
        })
        .await
        .map_err(PortError::backend)?
        .map_err(PortError::backend)?;

        stream_into_verified_writer(body, writer, None).await?;
        self.inner.stats.record_slow_read(digest.size());
        Ok(())
    }

    /// Stages `(digest, executable)` at `dest` by reflinking (`ioctl_ficlone` on Linux /
    /// `fclonefileat` on macOS) or hard-linking from `DiskCas`.
    ///
    /// Returns `Ok(true)` if materialized, or `Ok(false)` if the blob is absent from `DiskCas`.
    /// When `executable` is `true` and only the plain variant is cached, the executable variant
    /// is materialized onto its own separate inode (`0555`) before linking `dest` so permissions
    /// are never mutated across hardlinked files.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if filesystem materialization fails.
    pub fn hardlink_or_reflink_to(
        &self,
        digest: Digest,
        executable: bool,
        dest: &Path,
    ) -> Result<bool, PortError> {
        let mode = BlobMode::from_executable(executable);
        if digest.is_empty() {
            create_empty_file(dest, mode).map_err(PortError::backend)?;
            self.inner.stats.record_local_read(0);
            return Ok(true);
        }

        let desired_key = CacheKey::new(digest, mode);
        let desired_path = blob_path(&self.inner.root, desired_key);

        if let Some(pin) = self.inner.lru.pin_resident(desired_key) {
            return self
                .finish_materialization(&pin, hardlink_or_reflink_file(&desired_path, dest, mode));
        }

        // If the opposite mode variant is resident, materialize a separate inode for `mode`.
        let other_mode = match mode {
            BlobMode::Plain => BlobMode::Executable,
            BlobMode::Executable => BlobMode::Plain,
        };
        let other_key = CacheKey::new(digest, other_mode);
        let Some(source_pin) = self.inner.lru.pin_resident(other_key) else {
            return Ok(false);
        };
        self.materialize_variant(&source_pin, desired_key, dest)
    }

    fn materialize_variant(
        &self,
        source_pin: &ResidencyPin,
        desired_key: CacheKey,
        dest: &Path,
    ) -> Result<bool, PortError> {
        let digest = desired_key.digest;
        let mode = desired_key.mode;
        let desired_path = blob_path(&self.inner.root, desired_key);
        let other_path = blob_path(&self.inner.root, source_pin.key());
        match fs::metadata(&other_path) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.remove_missing(source_pin);
                return Ok(false);
            }
            Err(error) => return Err(PortError::backend(error)),
        }

        if self.can_admit(digest.size()) {
            let target_after_evict = self.inner.budget_bytes.saturating_sub(digest.size());
            if self.inner.lru.size_bytes() > target_after_evict {
                let tombstones = self.detach_evicted_until(target_after_evict)?;
                unlink_tombstones(tombstones).map_err(PortError::backend)?;
            }
            if self.inner.lru.size_bytes() <= target_after_evict {
                let published = self
                    .inner
                    .lru
                    .publish_pinned(desired_key, digest.size(), || {
                        match copy_with_metadata(&other_path, &desired_path, mode) {
                            Ok(()) => Ok(()),
                            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
                            Err(error) => Err(error),
                        }
                    });
                let (outcome, desired_pin) = match published {
                    Ok(published) => published,
                    Err(error) => return self.finish_materialization(source_pin, Err(error)),
                };
                if let InsertOutcome::Inserted { size } = outcome {
                    self.inner.stats.add_size_bytes(size);
                }
                return self.finish_materialization(
                    &desired_pin,
                    hardlink_or_reflink_file(&desired_path, dest, mode),
                );
            }
        }

        self.finish_materialization(source_pin, copy_with_metadata(&other_path, dest, mode))
    }

    fn finish_materialization(
        &self,
        pin: &ResidencyPin,
        result: io::Result<()>,
    ) -> Result<bool, PortError> {
        match result {
            Ok(()) => {
                self.inner.stats.record_local_read(pin.key().digest.size());
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.remove_missing(pin);
                Ok(false)
            }
            Err(error) => Err(PortError::backend(error)),
        }
    }

    fn remove_missing(&self, pin: &ResidencyPin) {
        if let Some(removed_size) = pin.remove_missing() {
            self.inner.stats.sub_size_bytes(removed_size);
        }
    }

    fn open_resident(&self, pin: &ResidencyPin) -> io::Result<Option<File>> {
        match File::open(blob_path(&self.inner.root, pin.key())) {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.remove_missing(pin);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn evict_and_unlink_until(&self, target_bytes: u64) -> Result<(), PortError> {
        let cas = self.clone();
        tokio::task::spawn_blocking(move || {
            let tombstones = cas.detach_evicted_until(target_bytes)?;
            unlink_tombstones(tombstones).map_err(PortError::backend)
        })
        .await
        .map_err(PortError::backend)?
    }

    fn detach_evicted_until(&self, target_bytes: u64) -> Result<Vec<PathBuf>, PortError> {
        let mut tombstones = Vec::new();
        let result = self
            .inner
            .lru
            .try_evict_unpinned_until(target_bytes, &mut |key| {
                let sequence = EVICT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let tombstone = self
                    .inner
                    .root
                    .join(layout::TMP_DIR_NAME)
                    .join(format!("evict-{}-{sequence}", std::process::id()));
                // Publication takes the same shard lock: the old inode is detached before this key
                // can be reinserted or pinned by a replacement writer.
                match fs::rename(blob_path(&self.inner.root, key), &tombstone) {
                    Ok(()) => tombstones.push(tombstone),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(PortError::backend(error)),
                }
                self.inner.stats.record_evictions(1, key.digest.size());
                self.inner.stats.sub_size_bytes(key.digest.size());
                Ok(())
            });
        if let Err(error) = result {
            unlink_tombstones(tombstones).map_err(PortError::backend)?;
            return Err(error);
        }
        Ok(tombstones)
    }

    fn find_resident(&self, digest: Digest) -> Option<ResidencyPin> {
        let plain = CacheKey::new(digest, BlobMode::Plain);
        let exec = CacheKey::new(digest, BlobMode::Executable);
        self.inner
            .lru
            .pin_resident(plain)
            .or_else(|| self.inner.lru.pin_resident(exec))
    }
}

impl ContentStore for DiskCas {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        missing.clear();
        for &digest in digests {
            if digest.is_empty() {
                continue;
            }
            let plain = CacheKey::new(digest, BlobMode::Plain);
            let exec = CacheKey::new(digest, BlobMode::Executable);
            if !self.inner.lru.contains(plain) && !self.inner.lru.contains(exec) {
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
            self.inner.stats.record_local_read(0);
            return Ok(Some(BlobStream::empty()));
        }

        let Some(pin) = self.find_resident(digest) else {
            return Ok(None);
        };

        let cas = self.clone();
        let opened = tokio::task::spawn_blocking(move || cas.open_resident(&pin))
            .await
            .map_err(PortError::backend)?
            .map_err(PortError::backend)?;

        let Some(mut file) = opened else {
            return Ok(None);
        };

        let total = digest.size();
        if range.offset > total {
            return Err(PortError::refused(Rejection::OutOfRange));
        }
        if range.offset == total {
            self.inner.stats.record_local_read(0);
            return Ok(Some(BlobStream::empty()));
        }

        let available = total - range.offset;
        let len = match range.limit {
            Some(limit) => available.min(limit.get()),
            None => available,
        };
        self.inner.stats.record_local_read(len);

        let offset = range.offset;
        let file = tokio::task::spawn_blocking(move || {
            file.seek(SeekFrom::Start(offset))?;
            Ok::<_, io::Error>(file)
        })
        .await
        .map_err(PortError::backend)?
        .map_err(PortError::backend)?;

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

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        match self.write_with_mode(digest, body, false).await? {
            WriteOutcome::Cached => Ok(()),
            WriteOutcome::Bypassed => Err(PortError::rejected(
                Rejection::ResourceExhausted,
                "content store capacity cannot admit the verified blob",
            )),
        }
    }

    fn usage(&self) -> Option<EngineStore> {
        Some(self.inner.stats.engine_store(StoreKind::Content))
    }
}

fn unlink_tombstones(paths: Vec<PathBuf>) -> io::Result<()> {
    for path in paths {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
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

async fn verify_stream_only(digest: Digest, mut body: BlobStream) -> Result<(), PortError> {
    use sha2::{Digest as _, Sha256};

    let mut hasher = Sha256::new();
    let mut seen = 0_u64;
    while let Some(chunk) = body.chunks.next().await {
        let chunk = chunk?;
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        seen = seen
            .checked_add(chunk_len)
            .ok_or_else(|| PortError::refused(Rejection::DigestMismatch))?;
        if seen > digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        hasher.update(&chunk);
    }
    if seen != digest.size() {
        return Err(PortError::refused(Rejection::DigestMismatch));
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != *digest.hash() {
        return Err(PortError::refused(Rejection::DigestMismatch));
    }
    Ok(())
}

async fn stream_into_verified_writer(
    mut body: BlobStream,
    mut writer: VerifiedWriter,
    publication: Option<(Arc<DiskCasInner>, CacheKey)>,
) -> Result<(), PortError> {
    let (tx, mut rx) = mpsc::channel::<WriteChunk>(WRITE_CHANNEL_CAPACITY);

    let writer_task = tokio::task::spawn_blocking(move || -> Result<(), VerifiedWriteError> {
        while let Some(msg) = rx.blocking_recv() {
            match msg {
                WriteChunk::Data(bytes) => writer.write_chunk(&bytes)?,
                WriteChunk::Commit => {
                    if let Some((inner, key)) = publication {
                        let outcome = inner
                            .lru
                            .publish(key, key.digest.size(), || writer.commit())?;
                        if let InsertOutcome::Inserted { size } = outcome {
                            inner.stats.add_size_bytes(size);
                        }
                        return Ok(());
                    }
                    return writer.commit();
                }
            }
        }
        // Sender dropped before Commit (stream error or caller cancellation): drop temp without publishing.
        Ok(())
    });

    while let Some(chunk_res) = body.chunks.next().await {
        let chunk = match chunk_res {
            Ok(chunk) => chunk,
            Err(error) => {
                drop(tx);
                let _ = writer_task.await;
                return Err(error);
            }
        };
        if !chunk.is_empty() && tx.send(WriteChunk::Data(chunk)).await.is_err() {
            break;
        }
    }

    let _ = tx.send(WriteChunk::Commit).await;
    drop(tx);

    let result = writer_task.await.map_err(PortError::backend)?;
    match result {
        Ok(()) => Ok(()),
        Err(VerifiedWriteError::DigestMismatch) => {
            Err(PortError::refused(Rejection::DigestMismatch))
        }
        Err(VerifiedWriteError::Io(error)) => Err(PortError::backend(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use futures::{StreamExt as _, stream};
    use gang_spi::{BlobStream, ByteRange, ContentStore, EngineStore, StoreKind};
    use gang_testkit::{TestResult, content_store_contract};
    use gang_types::Digest;

    use super::{DiskCas, WriteOutcome};
    use crate::layout::{DETERMINISTIC_MTIME_SECS, blob_path};
    use crate::lru::{BlobMode, CacheKey};

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(1);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Result<Self, std::io::Error> {
            let seq = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("gang-cas-disk-test-{}-{seq}", std::process::id()));
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

    #[tokio::test]
    async fn disk_cas_passes_content_store_contract() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path(), 1024 * 1024).await?;
        content_store_contract(&cas).await
    }

    #[tokio::test]
    async fn authoritative_write_refuses_capacity_bypass() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path(), 4).await?;
        let payload = Bytes::from_static(b"too large");
        let digest = Digest::from_bytes(&payload);
        assert!(matches!(
            cas.write(digest, BlobStream::from_bytes(payload.clone()))
                .await,
            Err(gang_spi::PortError::Rejected {
                reason: gang_spi::Rejection::ResourceExhausted,
                ..
            })
        ));
        assert_eq!(
            cas.write_with_mode(digest, BlobStream::from_bytes(payload), false)
                .await?,
            WriteOutcome::Bypassed
        );
        Ok(())
    }

    #[tokio::test]
    async fn authoritative_write_refuses_pinned_capacity() -> TestResult {
        let dir = TempDir::new()?;
        let resident = Bytes::from_static(b"resident");
        let replacement = Bytes::from_static(b"replaced");
        let digest = Digest::from_bytes(&resident);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(resident.clone()))
            .await?;
        let _pin = cas.pin_attempt(digest, false).ok_or("resident pin")?;
        let replacement_digest = Digest::from_bytes(&replacement);
        assert!(matches!(
            cas.write(
                replacement_digest,
                BlobStream::from_bytes(replacement.clone())
            )
            .await,
            Err(gang_spi::PortError::Rejected {
                reason: gang_spi::Rejection::ResourceExhausted,
                ..
            })
        ));
        assert_eq!(
            cas.write_with_mode(
                replacement_digest,
                BlobStream::from_bytes(replacement),
                false
            )
            .await?,
            WriteOutcome::Bypassed
        );
        assert_eq!(
            cas.read(digest, ByteRange::FULL)
                .await?
                .ok_or("resident survives")?
                .collect()
                .await?,
            resident
        );
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_write_pins_residency_until_stream_verified() -> TestResult {
        let dir = TempDir::new()?;
        let payload = Bytes::from_static(b"resident");
        let digest = Digest::from_bytes(&payload);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;

        let (resume, resumed) = tokio::sync::oneshot::channel();
        let prefix = payload.slice(..4);
        let suffix = payload.slice(4..);
        let chunks = stream::once(async move { Ok(prefix) }).chain(stream::once(async move {
            resumed.await.expect("resume duplicate stream");
            Ok(suffix)
        }));
        let duplicate = cas.write(
            digest,
            BlobStream {
                size: digest.size(),
                chunks: Box::pin(chunks),
            },
        );
        tokio::pin!(duplicate);
        assert!(futures::poll!(duplicate.as_mut()).is_pending());

        cas.evict_and_unlink_until(0).await?;
        let pressure = Bytes::from_static(b"pressure");
        assert_eq!(
            cas.write_with_mode(
                Digest::from_bytes(&pressure),
                BlobStream::from_bytes(pressure),
                false,
            )
            .await?,
            WriteOutcome::Bypassed
        );
        assert_eq!(cas.inner.lru.pinned_bytes(), digest.size());
        resume.send(()).expect("duplicate stream still waiting");
        duplicate.await?;

        assert_eq!(cas.inner.lru.pinned_bytes(), 0);
        assert_eq!(
            cas.read(digest, ByteRange::FULL)
                .await?
                .ok_or("verified duplicate must remain resident")?
                .collect()
                .await?,
            payload
        );
        cas.evict_and_unlink_until(0).await?;
        assert!(cas.read(digest, ByteRange::FULL).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn stale_missing_cleanup_preserves_replacement_and_stats() -> TestResult {
        let dir = TempDir::new()?;
        let payload = Bytes::from_static(b"republished");
        let digest = Digest::from_bytes(&payload);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let observed = cas.find_resident(digest).ok_or("resident reader")?;
        fs::remove_file(blob_path(dir.path(), observed.key()))?;
        assert!(cas.open_resident(&observed)?.is_none());

        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let replacement = cas.pin_attempt(digest, false).ok_or("replacement pin")?;
        cas.remove_missing(&observed);
        let stale_result = cas.finish_materialization(
            &observed,
            Err(std::io::Error::from(std::io::ErrorKind::NotFound)),
        )?;
        assert!(!stale_result);
        drop(observed);
        assert_eq!(cas.inner.lru.pinned_bytes(), digest.size());
        assert_eq!(cas.stats().size_bytes(), digest.size());
        cas.evict_and_unlink_until(0).await?;
        assert_eq!(
            cas.read(digest, ByteRange::FULL)
                .await?
                .ok_or("replacement survives stale cleanup")?
                .collect()
                .await?,
            payload
        );
        drop(replacement);
        cas.evict_and_unlink_until(0).await?;
        assert!(cas.inner.lru.is_empty());
        assert_eq!(cas.stats().size_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn reader_residency_is_pinned_until_file_is_open() -> TestResult {
        use std::io::Read as _;

        let dir = TempDir::new()?;
        let payload = Bytes::from_static(b"resident");
        let digest = Digest::from_bytes(&payload);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let observed = cas.find_resident(digest).ok_or("resident reader")?;
        cas.evict_and_unlink_until(0).await?;
        let mut file = cas.open_resident(&observed)?.ok_or("pinned file missing")?;
        drop(observed);

        cas.evict_and_unlink_until(0).await?;
        assert_eq!(cas.inner.lru.pinned_bytes(), 0);
        assert!(cas.inner.lru.is_empty());
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        assert_eq!(bytes.as_slice(), payload.as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn eviction_cleanup_preserves_republished_pinned_blob() -> TestResult {
        let dir = TempDir::new()?;
        let payload = Bytes::from_static(b"republished");
        let digest = Digest::from_bytes(&payload);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let tombstones = cas.detach_evicted_until(0)?;
        assert_eq!(tombstones.len(), 1);
        let canonical = blob_path(dir.path(), CacheKey::new(digest, BlobMode::Plain));
        assert!(
            !canonical.exists(),
            "eviction must detach before permitting publication"
        );
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let _pin = cas
            .pin_attempt(digest, false)
            .ok_or("replacement must be pinnable")?;
        super::unlink_tombstones(tombstones)?;
        cas.evict_and_unlink_until(0).await?;
        assert_eq!(
            cas.read(digest, ByteRange::FULL)
                .await?
                .ok_or("pinned replacement missing")?
                .collect()
                .await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_eviction_detach_preserves_residency() -> TestResult {
        let dir = TempDir::new()?;
        let payload = Bytes::from_static(b"resident");
        let digest = Digest::from_bytes(&payload);
        let cas = DiskCas::open(dir.path(), digest.size()).await?;
        cas.write(digest, BlobStream::from_bytes(payload.clone()))
            .await?;
        let temporary = dir.path().join(crate::layout::TMP_DIR_NAME);
        fs::remove_dir(&temporary)?;
        fs::write(temporary, b"obstructed tombstone parent")?;
        assert!(cas.detach_evicted_until(0).is_err());
        let _pin = cas
            .pin_attempt(digest, false)
            .ok_or("failed eviction must retain index entry")?;
        assert_eq!(
            cas.read(digest, ByteRange::FULL)
                .await?
                .ok_or("resident blob")?
                .collect()
                .await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn insert_sets_deterministic_mtime_and_separate_mode_inodes() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path(), 1024 * 1024).await?;
        let payload = b"#!/bin/sh\necho hi\n";
        let digest = Digest::from_bytes(payload);

        assert_eq!(
            cas.write_with_mode(
                digest,
                BlobStream::from_bytes(Bytes::from_static(payload)),
                false
            )
            .await?,
            WriteOutcome::Cached
        );
        assert_eq!(
            cas.write_with_mode(
                digest,
                BlobStream::from_bytes(Bytes::from_static(payload)),
                true
            )
            .await?,
            WriteOutcome::Cached
        );

        let plain_path = blob_path(dir.path(), CacheKey::new(digest, BlobMode::Plain));
        let exec_path = blob_path(dir.path(), CacheKey::new(digest, BlobMode::Executable));
        let plain_meta = fs::metadata(&plain_path)?;
        let exec_meta = fs::metadata(&exec_path)?;

        assert_ne!(plain_meta.ino(), exec_meta.ino());
        assert_eq!(plain_meta.permissions().mode() & 0o777, 0o444);
        assert_eq!(exec_meta.permissions().mode() & 0o777, 0o555);
        assert_eq!(
            u64::try_from(plain_meta.mtime()).unwrap_or(0),
            DETERMINISTIC_MTIME_SECS
        );
        assert_eq!(
            u64::try_from(exec_meta.mtime()).unwrap_or(0),
            DETERMINISTIC_MTIME_SECS
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_scan_cleans_tmp_malformed_and_evicts_excess() -> TestResult {
        let dir = TempDir::new()?;
        let payload_a = b"startup-payload-aaaa";
        let payload_b = b"startup-payload-bbbb";
        let digest_a = Digest::from_bytes(payload_a);
        let digest_b = Digest::from_bytes(payload_b);

        {
            let cas = DiskCas::open(dir.path(), 1024).await?;
            cas.write(
                digest_a,
                BlobStream::from_bytes(Bytes::from_static(payload_a)),
            )
            .await?;
            cas.write(
                digest_b,
                BlobStream::from_bytes(Bytes::from_static(payload_b)),
            )
            .await?;
        }

        // Plant stale `.tmp`, `tmp/orphan`, malformed file, and size-mismatched file.
        fs::create_dir_all(dir.path().join(".tmp"))?;
        fs::write(dir.path().join(".tmp/stale"), b"leftover")?;
        fs::write(dir.path().join("tmp/orphan"), b"leftover")?;
        let bad_digest = Digest::from_bytes(b"corrupt-size");
        let bad_path = blob_path(dir.path(), CacheKey::new(bad_digest, BlobMode::Plain));
        fs::create_dir_all(bad_path.parent().ok_or("missing parent")?)?;
        fs::write(&bad_path, b"wrong")?;
        fs::create_dir_all(dir.path().join("00"))?;
        fs::write(dir.path().join("00/garbage"), b"junk")?;

        // Reopen with budget only big enough for one of the two 20-byte blobs.
        let cas = DiskCas::open(dir.path(), digest_a.size()).await?;
        assert!(!dir.path().join(".tmp").exists());
        assert!(!dir.path().join("tmp/orphan").exists());
        assert!(!bad_path.exists());
        assert!(!dir.path().join("00/garbage").exists());
        assert_eq!(
            cas.usage(),
            Some(EngineStore {
                store: StoreKind::Content,
                used: digest_a.size(),
                budget: digest_a.size(),
                evicted: digest_b.size(),
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn bypass_when_pinned_or_oversized_preserves_warm_entries() -> TestResult {
        let dir = TempDir::new()?;
        let small = b"warm-small";
        let small_digest = Digest::from_bytes(small);
        let big = b"this-blob-exceeds-remaining-budget";
        let big_digest = Digest::from_bytes(big);

        let budget = small_digest.size() + 10;
        let cas = DiskCas::open(dir.path(), budget).await?;
        cas.write(
            small_digest,
            BlobStream::from_bytes(Bytes::from_static(small)),
        )
        .await?;
        let _pin = cas.pin_attempt(small_digest, false).ok_or("missing pin")?;

        // `small_digest` is pinned (10 bytes), `big` (34 bytes) cannot fit even after eviction.
        let outcome = cas
            .write_with_mode(
                big_digest,
                BlobStream::from_bytes(Bytes::from_static(big)),
                false,
            )
            .await?;
        assert_eq!(outcome, WriteOutcome::Bypassed);
        assert!(cas.read(small_digest, ByteRange::FULL).await?.is_some());
        assert!(cas.read(big_digest, ByteRange::FULL).await?.is_none());

        // Direct bypass writes directly to destination and records slow read.
        let direct_dest = dir.path().join("action-work/big-bin");
        cas.write_verified_direct(
            big_digest,
            BlobStream::from_bytes(Bytes::from_static(big)),
            &direct_dest,
            true,
        )
        .await?;
        assert_eq!(fs::read(&direct_dest)?, big);
        assert_eq!(
            fs::metadata(&direct_dest)?.permissions().mode() & 0o777,
            0o555
        );
        assert_eq!(cas.worker_cache_stats().slow_read_requests, 1);
        assert_eq!(cas.worker_cache_stats().slow_read_bytes, big_digest.size());
        Ok(())
    }

    #[tokio::test]
    async fn hardlink_or_reflink_stages_plain_and_executable() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024 * 1024).await?;
        let payload = b"staged-content";
        let digest = Digest::from_bytes(payload);
        cas.write(digest, BlobStream::from_bytes(Bytes::from_static(payload)))
            .await?;

        let plain_dest = dir.path().join("work/plain");
        let exec_dest = dir.path().join("work/exec");
        let empty_dest = dir.path().join("work/empty");

        assert!(cas.hardlink_or_reflink_to(digest, false, &plain_dest)?);
        assert!(cas.hardlink_or_reflink_to(digest, true, &exec_dest)?);
        assert!(cas.hardlink_or_reflink_to(Digest::EMPTY, false, &empty_dest)?);

        assert_eq!(fs::read(&plain_dest)?, payload);
        assert_eq!(fs::read(&exec_dest)?, payload);
        assert!(fs::read(&empty_dest)?.is_empty());
        assert_eq!(
            fs::metadata(&plain_dest)?.permissions().mode() & 0o777,
            0o444
        );
        assert_eq!(
            fs::metadata(&exec_dest)?.permissions().mode() & 0o777,
            0o555
        );
        Ok(())
    }
}
