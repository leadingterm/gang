//! Local `NVMe` content-addressed disk store (`DiskCas`) implementing [`ContentStore`].

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_spi::EngineStore;
use gang_spi::StoreKind;
use gang_spi::WorkerCacheStats;
use gang_spi::{BlobStream, ByteRange, ContentStore, PortError, Rejection};
use gang_types::Digest;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

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

type BlockingTaskList = Vec<JoinHandle<()>>;

/// Tracks `spawn_blocking` tasks owned by an attempt phase so cancellation signals them
/// cooperatively and awaits every handle before retiring directories or acknowledging cleanup.
#[derive(Debug, Default, Clone)]
pub struct BlockingScope {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
    join: Arc<tokio::sync::Mutex<()>>,
    tasks: Arc<Mutex<BlockingTaskList>>,
}

impl BlockingScope {
    /// Creates an active, uncancelled [`BlockingScope`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Signals cooperative cancellation to every blocking task in this scope.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Returns whether cancellation has been signaled for this scope.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Waits for cancellation without losing a signal concurrent with registration.
    pub async fn cancelled(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_cancelled() {
            notified.await;
        }
    }

    /// Spawns `f` on Tokio's blocking pool, retaining its handle through task completion.
    fn spawn_tracked<F, T>(&self, f: F) -> oneshot::Receiver<T>
    where
        F: FnOnce(&AtomicBool) -> T + Send + 'static,
        T: Send + 'static,
    {
        let cancelled = Arc::clone(&self.cancelled);
        let (tx, rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || {
            let value = f(&cancelled);
            let _ = tx.send(value);
        });
        let mut guard = self.tasks.lock();
        guard.retain(|task| !task.is_finished());
        guard.push(handle);
        rx
    }

    /// Awaits the task's result, retaining its handle until it has fully exited.
    async fn finish<T>(rx: oneshot::Receiver<T>) -> io::Result<T> {
        rx.await
            .map_err(|_| io::Error::other("blocking task exited before sending result"))
    }

    /// Runs `f` on Tokio's blocking pool, retaining its join handle even if this future is dropped
    /// on cancellation so [`Self::cancel_and_join`] can guarantee completion before cleanup.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the blocking task panics or exits without producing a value.
    pub async fn run<F, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce(&AtomicBool) -> T + Send + 'static,
        T: Send + 'static,
    {
        let rx = self.spawn_tracked(f);
        Self::finish(rx).await
    }

    /// Signals cooperative cancellation and joins every blocking task spawned in this scope.
    pub async fn cancel_and_join(&self) {
        use std::future::{Future as _, poll_fn};
        use std::pin::Pin;
        use std::task::Poll;

        self.cancel();
        let _join = self.join.lock().await;
        poll_fn(|cx| {
            let mut tasks = self.tasks.lock();
            let mut index = 0;
            while index < tasks.len() {
                if Pin::new(&mut tasks[index]).poll(cx).is_ready() {
                    // Only remove a joined handle; dropping this drain future retains ownership.
                    drop(tasks.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            if tasks.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

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

#[cfg(test)]
type WriteGate = (Arc<Notify>, Arc<Mutex<Option<oneshot::Receiver<()>>>>);

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
    #[cfg(test)]
    write_gate: Mutex<Option<WriteGate>>,
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
                #[cfg(test)]
                write_gate: Mutex::new(None),
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
        self.write_with_mode_inner(digest, body, executable, None)
            .await
    }

    /// Writes a verified blob while retaining all filesystem tasks in `scope` through shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] on cancellation, digest mismatch, filesystem, or stream failure.
    pub async fn write_with_mode_scoped(
        &self,
        digest: Digest,
        body: BlobStream,
        executable: bool,
        scope: &BlockingScope,
    ) -> Result<WriteOutcome, PortError> {
        self.write_with_mode_inner(digest, body, executable, Some(scope))
            .await
    }

    async fn write_with_mode_inner(
        &self,
        digest: Digest,
        body: BlobStream,
        executable: bool,
        scope: Option<&BlockingScope>,
    ) -> Result<WriteOutcome, PortError> {
        if scope.is_some_and(BlockingScope::is_cancelled) {
            return Err(PortError::unavailable("cache write cancelled"));
        }
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        if digest.is_empty() {
            verify_empty_stream(body, scope).await?;
            return Ok(WriteOutcome::Cached);
        }

        let mode = BlobMode::from_executable(executable);
        let key = CacheKey::new(digest, mode);

        if let Some(pin) = self.inner.lru.pin_resident(key) {
            verify_stream_only(digest, body, scope).await?;
            if !pin.touch() {
                return Err(PortError::backend(io::Error::new(
                    io::ErrorKind::NotFound,
                    "cached blob residency changed while verifying duplicate",
                )));
            }
            return Ok(WriteOutcome::Cached);
        }

        if !self.can_admit(digest.size()) {
            verify_stream_only(digest, body, scope).await?;
            return Ok(WriteOutcome::Bypassed);
        }

        let target_after_evict = self.inner.budget_bytes.saturating_sub(digest.size());
        if self.inner.lru.size_bytes() > target_after_evict {
            self.evict_and_unlink_until_scoped(target_after_evict, scope)
                .await?;
        }
        if self.inner.lru.size_bytes() > target_after_evict {
            // Concurrent pins grew while evicting; bypass rather than exceeding budget.
            verify_stream_only(digest, body, scope).await?;
            return Ok(WriteOutcome::Bypassed);
        }

        let writer = {
            let root = self.inner.root.clone();
            let open = move |cancelled: &AtomicBool| {
                if cancelled.load(Ordering::Acquire) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "cache write cancelled",
                    ));
                }
                VerifiedWriter::open_cas(&root, key)
            };
            if let Some(scope) = scope {
                scope
                    .run(open)
                    .await
                    .map_err(PortError::backend)?
                    .map_err(PortError::backend)?
            } else {
                tokio::task::spawn_blocking(move || open(&AtomicBool::new(false)))
                    .await
                    .map_err(PortError::backend)?
                    .map_err(PortError::backend)?
            }
        };

        #[cfg(test)]
        let on_write = self
            .inner
            .write_gate
            .lock()
            .take()
            .map(|(entered, release)| {
                Arc::new(move || {
                    entered.notify_one();
                    if let Some(release) = release.lock().take() {
                        let _ = release.blocking_recv();
                    }
                }) as Arc<dyn Fn() + Send + Sync>
            });
        stream_into_verified_writer(
            body,
            writer,
            Some((Arc::clone(&self.inner), key)),
            scope,
            #[cfg(test)]
            on_write,
        )
        .await?;

        if self.inner.lru.size_bytes() > self.inner.budget_bytes {
            self.evict_and_unlink_until_scoped(self.inner.budget_bytes, scope)
                .await?;
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
        self.write_verified_direct_inner(
            digest,
            body,
            dest,
            executable,
            None,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Like [`Self::write_verified_direct`], routing both the opener and streaming writer blocking
    /// tasks through `scope` so cancellation stops and joins them before directory retirement.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with [`Rejection::DigestMismatch`] on mismatch, or
    /// [`PortError::Backend`] on cancellation, filesystem, or stream failure.
    pub async fn write_verified_direct_scoped(
        &self,
        digest: Digest,
        body: BlobStream,
        dest: &Path,
        executable: bool,
        scope: &BlockingScope,
    ) -> Result<(), PortError> {
        self.write_verified_direct_inner(
            digest,
            body,
            dest,
            executable,
            Some(scope),
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_verified_direct_inner(
        &self,
        digest: Digest,
        body: BlobStream,
        dest: &Path,
        executable: bool,
        scope: Option<&BlockingScope>,
        #[cfg(test)] on_open: Option<Arc<dyn Fn() + Send + Sync>>,
        #[cfg(test)] on_write: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Result<(), PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        if scope.is_some_and(BlockingScope::is_cancelled) {
            return Err(PortError::unavailable("direct write cancelled"));
        }
        let mode = BlobMode::from_executable(executable);
        if digest.is_empty() {
            verify_empty_stream(body, scope).await?;
            let dest_owned = dest.to_path_buf();
            let create = move |cancelled: &AtomicBool| -> io::Result<()> {
                #[cfg(test)]
                if let Some(hook) = on_open {
                    hook();
                }
                if cancelled.load(Ordering::Acquire) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "direct write cancelled",
                    ));
                }
                create_empty_file(&dest_owned, mode)
            };
            if let Some(scope) = scope {
                scope
                    .run(create)
                    .await
                    .map_err(PortError::backend)?
                    .map_err(PortError::backend)?;
            } else {
                tokio::task::spawn_blocking(move || create(&AtomicBool::new(false)))
                    .await
                    .map_err(PortError::backend)?
                    .map_err(PortError::backend)?;
            }
            self.inner.stats.record_slow_read(0);
            return Ok(());
        }

        let dest_owned = dest.to_path_buf();
        let open = move |cancelled: &AtomicBool| -> io::Result<VerifiedWriter> {
            #[cfg(test)]
            if let Some(hook) = on_open {
                hook();
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "direct write cancelled",
                ));
            }
            VerifiedWriter::open_direct(&dest_owned, digest, mode)
        };
        let writer = if let Some(scope) = scope {
            scope
                .run(open)
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::backend)?
        } else {
            tokio::task::spawn_blocking(move || open(&AtomicBool::new(false)))
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::backend)?
        };

        stream_into_verified_writer(
            body,
            writer,
            None,
            scope,
            #[cfg(test)]
            on_write,
        )
        .await?;
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
        self.evict_and_unlink_until_scoped(target_bytes, None).await
    }

    async fn evict_and_unlink_until_scoped(
        &self,
        target_bytes: u64,
        scope: Option<&BlockingScope>,
    ) -> Result<(), PortError> {
        let cas = self.clone();
        let evict = move |_: &AtomicBool| {
            let tombstones = cas.detach_evicted_until(target_bytes)?;
            unlink_tombstones(tombstones).map_err(PortError::backend)
        };
        if let Some(scope) = scope {
            scope.run(evict).await.map_err(PortError::backend)?
        } else {
            tokio::task::spawn_blocking(move || evict(&AtomicBool::new(false)))
                .await
                .map_err(PortError::backend)?
        }
    }

    #[cfg(test)]
    pub(crate) fn gate_next_write(&self, entered: Arc<Notify>, release: oneshot::Receiver<()>) {
        *self.inner.write_gate.lock() = Some((entered, Arc::new(Mutex::new(Some(release)))));
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
                io::Error::other("content store capacity cannot admit the verified blob"),
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

async fn next_chunk(
    body: &mut BlobStream,
    scope: Option<&BlockingScope>,
) -> Result<Option<Result<Bytes, PortError>>, PortError> {
    if let Some(scope) = scope {
        tokio::select! {
            biased;
            () = scope.cancelled() => Err(PortError::unavailable("cache write cancelled")),
            next = body.chunks.next() => Ok(next),
        }
    } else {
        Ok(body.chunks.next().await)
    }
}

async fn verify_empty_stream(
    mut body: BlobStream,
    scope: Option<&BlockingScope>,
) -> Result<(), PortError> {
    while let Some(chunk) = next_chunk(&mut body, scope).await? {
        let chunk = chunk?;
        if !chunk.is_empty() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
    }
    Ok(())
}

async fn verify_stream_only(
    digest: Digest,
    mut body: BlobStream,
    scope: Option<&BlockingScope>,
) -> Result<(), PortError> {
    use sha2::{Digest as _, Sha256};

    let mut hasher = Sha256::new();
    let mut seen = 0_u64;
    while let Some(chunk) = next_chunk(&mut body, scope).await? {
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

enum WriterHandle {
    Scoped(oneshot::Receiver<Result<(), VerifiedWriteError>>),
    Unscoped(JoinHandle<Result<(), VerifiedWriteError>>),
}

impl WriterHandle {
    async fn join(self) -> Result<(), PortError> {
        let result = match self {
            Self::Scoped(rx) => BlockingScope::finish(rx)
                .await
                .map_err(PortError::backend)?,
            Self::Unscoped(handle) => handle.await.map_err(PortError::backend)?,
        };
        match result {
            Ok(()) => Ok(()),
            Err(VerifiedWriteError::DigestMismatch) => {
                Err(PortError::refused(Rejection::DigestMismatch))
            }
            Err(VerifiedWriteError::Io(error)) => Err(PortError::backend(error)),
        }
    }
}

fn write_chunks(
    mut writer: VerifiedWriter,
    mut rx: mpsc::Receiver<WriteChunk>,
    publication: Option<(Arc<DiskCasInner>, CacheKey)>,
    cancelled: &AtomicBool,
    #[cfg(test)] mut on_write: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<(), VerifiedWriteError> {
    while let Some(msg) = rx.blocking_recv() {
        #[cfg(test)]
        if let Some(callback) = on_write.take() {
            callback();
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(VerifiedWriteError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "direct write cancelled",
            )));
        }
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
    // Losing the sender before Commit drops the temporary file without publishing.
    Ok(())
}

async fn stream_into_verified_writer(
    mut body: BlobStream,
    writer: VerifiedWriter,
    publication: Option<(Arc<DiskCasInner>, CacheKey)>,
    scope: Option<&BlockingScope>,
    #[cfg(test)] on_write: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<(), PortError> {
    let (tx, rx) = mpsc::channel::<WriteChunk>(WRITE_CHANNEL_CAPACITY);

    let run_writer = move |cancelled: &AtomicBool| {
        write_chunks(
            writer,
            rx,
            publication,
            cancelled,
            #[cfg(test)]
            on_write,
        )
    };

    let writer_handle = if let Some(scope) = scope {
        let rx = scope.spawn_tracked(run_writer);
        WriterHandle::Scoped(rx)
    } else {
        WriterHandle::Unscoped(tokio::task::spawn_blocking(move || {
            run_writer(&AtomicBool::new(false))
        }))
    };

    loop {
        let next = next_chunk(&mut body, scope).await;
        let chunk = match next {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(None) => break,
            Ok(Some(Err(error))) | Err(error) => {
                drop(body);
                drop(tx);
                let _ = writer_handle.join().await;
                return Err(error);
            }
        };
        if chunk.is_empty() {
            continue;
        }
        let send_future = tx.send(WriteChunk::Data(chunk));
        let sent = if let Some(scope) = scope {
            tokio::select! {
                biased;
                () = scope.cancelled() => false,
                result = send_future => result.is_ok(),
            }
        } else {
            send_future.await.is_ok()
        };
        if !sent {
            drop(body);
            drop(tx);
            writer_handle.join().await?;
            return Err(PortError::unavailable("cache write cancelled"));
        }
    }

    if scope.is_some_and(BlockingScope::is_cancelled) {
        drop(body);
        drop(tx);
        let _ = writer_handle.join().await;
        return Err(PortError::unavailable("cache write cancelled"));
    }

    let _ = tx.send(WriteChunk::Commit).await;
    drop(tx);

    writer_handle.join().await
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::{StreamExt as _, stream};
    use gang_spi::EngineStore;
    use gang_spi::StoreKind;
    use gang_spi::{BlobStream, ByteRange, ContentStore};
    use gang_testkit::{TestResult, content_store_contract};
    use gang_types::Digest;

    use super::{BlockingScope, DiskCas, WriteOutcome};
    use crate::layout::{DETERMINISTIC_MTIME_SECS, blob_path};
    use crate::lru::{BlobMode, CacheKey};

    struct TempDir {
        directory: tempfile::TempDir,
    }

    impl TempDir {
        fn new() -> Result<Self, std::io::Error> {
            Ok(Self {
                directory: tempfile::tempdir()?,
            })
        }

        fn path(&self) -> &Path {
            self.directory.path()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = gang_os::fs::remove_dir_optional(self.path());
        }
    }

    #[tokio::test]
    async fn scoped_verifiers_cancel_without_waiting_for_stream_eof() -> TestResult {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let dir = TempDir::new()?;
            let cas = DiskCas::open(dir.path(), 4).await?;
            let resident = Bytes::from_static(b"warm");
            let resident_digest = Digest::from_bytes(&resident);
            cas.write(resident_digest, BlobStream::from_bytes(resident.clone()))
                .await?;
            let bypassed = Digest::from_bytes(b"too large");
            for digest in [Digest::EMPTY, resident_digest, bypassed] {
                let scope = BlockingScope::new();
                let body = BlobStream {
                    size: digest.size(),
                    chunks: Box::pin(stream::pending()),
                };
                let mut write = Box::pin(cas.write_with_mode_scoped(digest, body, false, &scope));
                assert!(futures::poll!(write.as_mut()).is_pending());
                scope.cancel();
                assert!(matches!(
                    write.await,
                    Err(gang_spi::PortError::Unavailable(_))
                ));
                scope.cancel_and_join().await;
            }
            assert_eq!(
                cas.read(resident_digest, ByteRange::FULL)
                    .await?
                    .ok_or("resident")?
                    .collect()
                    .await?,
                resident
            );
            assert!(cas.read(bypassed, ByteRange::FULL).await?.is_none());
            assert_eq!(
                fs::read_dir(cas.root().join(crate::TMP_DIR_NAME))?.count(),
                0
            );
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn cancelled_drain_retains_unfinished_blocking_task() -> TestResult {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let scope = BlockingScope::new();
            let entered = Arc::new(tokio::sync::Notify::new());
            let latch = TestLatch::new();
            let _guard = LatchGuard(Arc::clone(&latch));
            let worker_latch = Arc::clone(&latch);
            let worker_entered = Arc::clone(&entered);
            let mut work = Box::pin(scope.run(move |_| {
                worker_entered.notify_one();
                worker_latch.wait();
            }));
            assert!(futures::poll!(work.as_mut()).is_pending());
            entered.notified().await;
            drop(work);
            let mut first = Box::pin(scope.cancel_and_join());
            let first_pending = futures::poll!(first.as_mut()).is_pending();
            drop(first);
            let mut second = Box::pin(scope.cancel_and_join());
            let second_pending = futures::poll!(second.as_mut()).is_pending();
            latch.release();
            second.await;
            assert!(
                first_pending,
                "first drain released an unfinished blocking task"
            );
            assert!(
                second_pending,
                "dropping a drain detached its unfinished blocking task"
            );
            Ok(())
        })
        .await?
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

    struct TestLatch {
        done: parking_lot::Mutex<bool>,
        cv: parking_lot::Condvar,
    }

    impl TestLatch {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                done: parking_lot::Mutex::new(false),
                cv: parking_lot::Condvar::new(),
            })
        }

        fn wait(&self) {
            let mut guard = self.done.lock();
            while !*guard {
                self.cv.wait(&mut guard);
            }
        }

        fn release(&self) {
            *self.done.lock() = true;
            self.cv.notify_all();
        }
    }

    struct LatchGuard(Arc<TestLatch>);

    impl Drop for LatchGuard {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[tokio::test]
    async fn direct_write_cancellation_joins_opener_before_creating_parent_dir() -> TestResult {
        use std::future::Future as _;
        use std::task::Poll;

        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 16).await?;
        let payload = Bytes::from_static(b"oversized-direct-bypass-opener-payload");
        let digest = Digest::from_bytes(&payload);
        let dest = dir.path().join("retired-attempt/work/bypass.bin");

        let entered = Arc::new(tokio::sync::Notify::new());
        let latch = TestLatch::new();
        let _guard = LatchGuard(Arc::clone(&latch));
        let hook_entered = Arc::clone(&entered);
        let hook_latch = Arc::clone(&latch);
        let on_open: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            hook_entered.notify_one();
            hook_latch.wait();
        });

        let scope = BlockingScope::new();
        let write_fut = cas.write_verified_direct_inner(
            digest,
            BlobStream::from_bytes(payload),
            &dest,
            false,
            Some(&scope),
            Some(on_open),
            None,
        );
        let outcome = tokio::select! {
            biased;
            () = entered.notified() => None,
            res = write_fut => Some(res),
        };
        assert!(outcome.is_none(), "cancellation must interrupt opener");

        let mut join_fut = std::pin::pin!(scope.cancel_and_join());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(join_fut.as_mut().poll(cx).is_pending())).await;
        latch.release();
        join_fut.await;

        assert!(
            pending,
            "cancel_and_join must remain Pending while direct-write opener is held"
        );
        assert!(
            !dir.path().join("retired-attempt").exists(),
            "cancelled opener must never recreate the retired attempt directory"
        );
        Ok(())
    }

    #[tokio::test]
    async fn direct_write_cancellation_joins_writer_before_committing_file() -> TestResult {
        use std::future::Future as _;
        use std::task::Poll;

        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 16).await?;
        let payload = Bytes::from_static(b"oversized-direct-bypass-writer-payload");
        let digest = Digest::from_bytes(&payload);
        let dest = dir.path().join("retired-attempt/work/bypass.bin");

        let entered = Arc::new(tokio::sync::Notify::new());
        let latch = TestLatch::new();
        let _guard = LatchGuard(Arc::clone(&latch));
        let hook_entered = Arc::clone(&entered);
        let hook_latch = Arc::clone(&latch);
        let on_write: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            hook_entered.notify_one();
            hook_latch.wait();
        });

        let scope = BlockingScope::new();
        let write_fut = cas.write_verified_direct_inner(
            digest,
            BlobStream::from_bytes(payload),
            &dest,
            false,
            Some(&scope),
            None,
            Some(on_write),
        );
        let outcome = tokio::select! {
            biased;
            () = entered.notified() => None,
            res = write_fut => Some(res),
        };
        assert!(outcome.is_none(), "cancellation must interrupt writer");

        let mut join_fut = std::pin::pin!(scope.cancel_and_join());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(join_fut.as_mut().poll(cx).is_pending())).await;
        latch.release();
        join_fut.await;

        assert!(
            pending,
            "cancel_and_join must remain Pending while direct-write writer is held"
        );
        assert!(
            !dest.exists(),
            "cancelled direct writer must never publish the destination file"
        );
        Ok(())
    }
}
