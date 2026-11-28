//! Asynchronous bounded worker graveyard and disk-credit accounting (`§6.5`).
//!
//! Moves completed, failed, or killed `<work>/<attempt_id>` directories into `<work>/.graveyard/`
//! with a single `rename` syscall off the critical execution/upload path, tracks exact reclaimable
//! bytes (`walk_bytes.saturating_sub(cas_shared_bytes)`) in [`GraveyardPending`], accepts evicted
//! [`TreeCache`](gang_cas::TreeCache) [`EvictedTree`] entries so `DiskCas` `tree_pins` are
//! released **only after** the unlink finishes, repairs read-only/inaccessible permissions via
//! [`gang_os::fs::remove_dir_optional`] (counting failures on macOS `0000` dirs without crediting
//! un-reclaimed bytes), sweeps remnants on startup, and provides [`Graveyard::drain`] for clean
//! shutdown.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

use gang_cas::EvictedTree;
use gang_os::fs::{PRIVATE_DIR, ensure_private_dir, remove_dir_optional};
use gang_types::AttemptId;
use parking_lot::{Condvar, Mutex};
use rustix::fs::{Mode, OFlags, fchmod, openat};
use tokio::sync::Notify;

use crate::worker_sampling::{GraveyardPending, SamplingRegistry};

/// Subdirectory under `<work>` holding retired attempt and evicted tree directories awaiting
/// asynchronous removal.
pub const GRAVEYARD_DIR: &str = ".graveyard";

/// Errors returned by [`Graveyard::open`], [`Graveyard::retire_attempt`], or startup/work sweeps.
#[derive(Debug, thiserror::Error)]
pub enum GraveyardError {
    /// Preparing `<work>/.graveyard` failed.
    #[error("failed to prepare graveyard directory `{}`: {source}", path.display())]
    Prepare {
        /// Graveyard directory path.
        path: PathBuf,
        /// Underlying OS or ownership error.
        #[source]
        source: gang_os::PathIo,
    },
    /// Sweeping pre-existing remnants on startup failed.
    #[error("failed to sweep stale graveyard entry `{}`: {source}", path.display())]
    Sweep {
        /// Path of the remnant that could not be removed.
        path: PathBuf,
        /// Underlying OS error.
        #[source]
        source: io::Error,
    },
    /// Spawning a bounded graveyard unlink worker thread failed.
    #[error("failed to spawn graveyard unlink worker thread: {0}")]
    SpawnWorker(#[source] io::Error),
    /// Moving an attempt directory into `<work>/.graveyard` failed.
    #[error("failed to move `{}` into graveyard `{}`: {source}", src.display(), dest.display())]
    Rename {
        /// Source attempt directory path.
        src: PathBuf,
        /// Destination path inside `.graveyard`.
        dest: PathBuf,
        /// Underlying OS error.
        #[source]
        source: io::Error,
    },
    /// Attempt directory had no final path component to name its graveyard entry.
    #[error("attempt path `{}` has no directory name", path.display())]
    InvalidAttemptPath {
        /// Supplied attempt directory path.
        path: PathBuf,
    },
}

enum GraveyardJob {
    Attempt {
        path: PathBuf,
        reclaimable: u64,
        attempt: Option<AttemptId>,
    },
    Tree {
        evicted: EvictedTree,
        reclaimable: u64,
    },
}

#[derive(Default)]
struct QueueState {
    jobs: VecDeque<GraveyardJob>,
    closed: bool,
}

struct PoolShared {
    pending: Arc<GraveyardPending>,
    sampling: Option<SamplingRegistry>,
    failures: AtomicU64,
    in_flight: AtomicUsize,
    drained: Notify,
    queue: Mutex<QueueState>,
    wake: Condvar,
}

struct GraveyardInner {
    graveyard_dir: PathBuf,
    collision_seq: AtomicU64,
    shared: Arc<PoolShared>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for GraveyardInner {
    fn drop(&mut self) {
        {
            let mut queue = self.shared.queue.lock();
            queue.closed = true;
            self.shared.wake.notify_all();
        }
        let mut threads = self.threads.lock();
        for handle in threads.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Asynchronous worker graveyard owning `<work>/.graveyard` and a bounded pool of unlink workers.
#[derive(Clone)]
pub struct Graveyard {
    inner: Arc<GraveyardInner>,
}

impl std::fmt::Debug for Graveyard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Graveyard")
            .field("graveyard_dir", &self.inner.graveyard_dir)
            .field("pending_bytes", &self.pending_bytes())
            .field("failures", &self.failures())
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}

impl Graveyard {
    /// Creates `<work_directory>/.graveyard` (mode `0700`), sweeps any pre-existing remnants inside
    /// it, and spawns `workers` bounded unlink worker threads.
    ///
    /// # Errors
    ///
    /// Returns [`GraveyardError::Prepare`] if `.graveyard` cannot be created or tightened,
    /// [`GraveyardError::Sweep`] if a pre-existing remnant fails to remove (failing closed on
    /// macOS `0000`/`0200` directories), or [`GraveyardError::SpawnWorker`] if worker threads
    /// cannot be spawned.
    pub async fn open(
        work_directory: impl AsRef<Path>,
        workers: NonZeroUsize,
        pending: Arc<GraveyardPending>,
        sampling: Option<SamplingRegistry>,
    ) -> Result<Self, GraveyardError> {
        let graveyard_dir = work_directory.as_ref().join(GRAVEYARD_DIR);
        let dir_for_init = graveyard_dir.clone();
        let canonical_dir =
            tokio::task::spawn_blocking(move || -> Result<PathBuf, GraveyardError> {
                let private = ensure_private_dir(&dir_for_init).map_err(|source| {
                    GraveyardError::Prepare {
                        path: dir_for_init.clone(),
                        source,
                    }
                })?;
                drop(private);
                let path =
                    fs::canonicalize(&dir_for_init).map_err(|err| GraveyardError::Prepare {
                        path: dir_for_init.clone(),
                        source: gang_os::PathIo::new(
                            "canonicalize graveyard directory",
                            &dir_for_init,
                            err,
                        ),
                    })?;
                sweep_dir_contents(&path)?;
                Ok(path)
            })
            .await
            .map_err(|err| GraveyardError::SpawnWorker(io::Error::other(err)))??;

        let shared = Arc::new(PoolShared {
            pending,
            sampling,
            failures: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            drained: Notify::new(),
            queue: Mutex::new(QueueState::default()),
            wake: Condvar::new(),
        });

        let mut threads = Vec::with_capacity(workers.get());
        for idx in 0..workers.get() {
            let worker_shared = Arc::clone(&shared);
            let handle = thread::Builder::new()
                .name(format!("gang-graveyard-{idx}"))
                .spawn(move || run_unlink_worker(&worker_shared))
                .map_err(GraveyardError::SpawnWorker)?;
            threads.push(handle);
        }

        Ok(Self {
            inner: Arc::new(GraveyardInner {
                graveyard_dir: canonical_dir,
                collision_seq: AtomicU64::new(1),
                shared,
                threads: Mutex::new(threads),
            }),
        })
    }

    /// Returns the `<work>/.graveyard` directory path.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.inner.graveyard_dir
    }

    /// Returns the shared [`GraveyardPending`] byte counter.
    #[must_use]
    pub fn pending(&self) -> &Arc<GraveyardPending> {
        &self.inner.shared.pending
    }

    /// Returns the current sum of reclaimable bytes awaiting removal in `.graveyard`.
    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.inner.shared.pending.pending_bytes()
    }

    /// Returns the cumulative count of graveyard entries whose unlink failed.
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.inner.shared.failures.load(Ordering::Relaxed)
    }

    /// Returns the number of queued or actively unlinking graveyard entries.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inner.shared.in_flight.load(Ordering::Acquire)
    }

    /// Synchronously retires `attempt_dir` (`<work>/<attempt_id>`) into `.graveyard` with one
    /// `rename` syscall, updates [`GraveyardPending`] with `walk_bytes - cas_shared_bytes`, and
    /// enqueues asynchronous unlink onto the bounded worker pool.
    ///
    /// If `attempt_dir` is already absent (`NotFound`), returns `Ok(())` without queuing work.
    ///
    /// # Errors
    ///
    /// Returns [`GraveyardError::InvalidAttemptPath`] if `attempt_dir` has no file name, or
    /// [`GraveyardError::Rename`] if renaming into `.graveyard` fails.
    pub fn retire_attempt(
        &self,
        attempt_dir: &Path,
        walk_bytes: u64,
        cas_shared_bytes: u64,
    ) -> Result<(), GraveyardError> {
        let name = attempt_dir
            .file_name()
            .ok_or_else(|| GraveyardError::InvalidAttemptPath {
                path: attempt_dir.to_path_buf(),
            })?;
        let attempt = name
            .to_str()
            .and_then(|text| AttemptId::try_from(text).ok());
        let Some(dest) = self.move_into_graveyard(attempt_dir, name)? else {
            if let (Some(sampling), Some(attempt_id)) = (&self.inner.shared.sampling, &attempt) {
                sampling.on_graveyard_processed(attempt_id);
            }
            return Ok(());
        };

        let reclaimable = self
            .inner
            .shared
            .pending
            .enqueue(walk_bytes, cas_shared_bytes);
        self.enqueue(GraveyardJob::Attempt {
            path: dest,
            reclaimable,
            attempt,
        });
        Ok(())
    }

    /// Enqueues an evicted [`TreeCache`](gang_cas::TreeCache) [`EvictedTree`] onto the bounded
    /// unlink pool.
    ///
    /// Every file in a materialized tree is shared with `DiskCas`, so `reclaimable` is `0` and
    /// `DiskCas` `tree_pins` are dropped only after [`EvictedTree::unlink_and_release`] completes.
    pub fn retire_evicted_tree(&self, evicted: EvictedTree) {
        let reclaimable = self
            .inner
            .shared
            .pending
            .enqueue(evicted.walk_bytes(), evicted.cas_shared_bytes());
        self.enqueue(GraveyardJob::Tree {
            evicted,
            reclaimable,
        });
    }

    /// Waits until every currently queued or active graveyard unlink finishes (or fails and records
    /// its failure).
    pub async fn drain(&self) {
        loop {
            let notified = self.inner.shared.drained.notified();
            if self.inner.shared.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn enqueue(&self, job: GraveyardJob) {
        self.inner.shared.in_flight.fetch_add(1, Ordering::AcqRel);
        let mut queue = self.inner.shared.queue.lock();
        queue.jobs.push_back(job);
        self.inner.shared.wake.notify_one();
    }

    fn move_into_graveyard(
        &self,
        attempt_dir: &Path,
        name: &OsStr,
    ) -> Result<Option<PathBuf>, GraveyardError> {
        let mut dest = self.inner.graveyard_dir.join(name);
        match rename_with_perm_repair(attempt_dir, &dest) {
            Ok(()) => Ok(Some(dest)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Only treat as absent when the source itself vanished, not if `.graveyard` vanished.
                if !attempt_dir.exists() && attempt_dir.symlink_metadata().is_err() {
                    return Ok(None);
                }
                Err(GraveyardError::Rename {
                    src: attempt_dir.to_path_buf(),
                    dest,
                    source: error,
                })
            }
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    || is_directory_not_empty(&error) =>
            {
                let seq = self.inner.collision_seq.fetch_add(1, Ordering::Relaxed);
                let mut unique = name.to_os_string();
                unique.push(format!("-{seq}"));
                dest = self.inner.graveyard_dir.join(unique);
                match rename_with_perm_repair(attempt_dir, &dest) {
                    Ok(()) => Ok(Some(dest)),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(err) => Err(GraveyardError::Rename {
                        src: attempt_dir.to_path_buf(),
                        dest,
                        source: err,
                    }),
                }
            }
            Err(error) => Err(GraveyardError::Rename {
                src: attempt_dir.to_path_buf(),
                dest,
                source: error,
            }),
        }
    }
}

/// Empties every entry inside `work_directory` (including `.graveyard`) on worker startup.
///
/// # Errors
///
/// Returns [`GraveyardError::Sweep`] if any entry fails to remove (failing closed on macOS
/// `0000`/`0200` directories).
pub(crate) fn sweep_work_root(work_directory: &Path) -> Result<(), GraveyardError> {
    if !work_directory.exists() && work_directory.symlink_metadata().is_err() {
        return Ok(());
    }
    sweep_dir_contents(work_directory)
}

fn sweep_dir_contents(dir: &Path) -> Result<(), GraveyardError> {
    let entries = fs::read_dir(dir).map_err(|source| GraveyardError::Sweep {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry_res in entries {
        let entry = entry_res.map_err(|source| GraveyardError::Sweep {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        remove_any_path(&path).map_err(|source| GraveyardError::Sweep { path, source })?;
    }
    Ok(())
}

fn remove_any_path(path: &Path) -> io::Result<()> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if meta.file_type().is_dir() || meta.file_type().is_symlink() {
        remove_dir_optional(path).map_err(|err| err.source)
    } else {
        fs::remove_file(path)
    }
}

fn rename_with_perm_repair(src: &Path, dest: &Path) -> io::Result<()> {
    match fs::rename(src, dest) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            // Moving a `0555` directory across parents on macOS requires owner write on `src`.
            let _ = grant_owner_write_dir(src);
            fs::rename(src, dest)
        }
        Err(error) => Err(error),
    }
}

fn grant_owner_write_dir(path: &Path) -> io::Result<()> {
    let fd = openat(
        rustix::fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    fchmod(&fd, PRIVATE_DIR)?;
    Ok(())
}

fn is_directory_not_empty(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::DirectoryNotEmpty
        || error.raw_os_error() == Some(rustix::io::Errno::NOTEMPTY.raw_os_error())
}

fn run_unlink_worker(shared: &PoolShared) {
    loop {
        let job = {
            let mut queue = shared.queue.lock();
            loop {
                if let Some(job) = queue.jobs.pop_front() {
                    break job;
                }
                if queue.closed {
                    return;
                }
                shared.wake.wait(&mut queue);
            }
        };
        process_job(shared, job);
        if shared.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            shared.drained.notify_waiters();
        }
    }
}

fn process_job(shared: &PoolShared, job: GraveyardJob) {
    match job {
        GraveyardJob::Attempt {
            path,
            reclaimable,
            attempt,
        } => {
            if let Err(error) = remove_any_path(&path) {
                shared.failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    path = %path.display(),
                    error = %gang_types::error_chain(&error),
                    "graveyard unlink failed; remnant left until next worker start"
                );
            }
            shared.pending.complete(reclaimable);
            if let (Some(sampling), Some(attempt_id)) = (&shared.sampling, &attempt) {
                sampling.on_graveyard_processed(attempt_id);
            }
        }
        GraveyardJob::Tree {
            evicted,
            reclaimable,
        } => {
            let path = evicted.path().to_path_buf();
            if let Err(error) = evicted.unlink_and_release() {
                shared.failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    path = %path.display(),
                    error = %gang_types::error_chain(&error),
                    "graveyard evicted-tree unlink failed; remnant left until next worker start"
                );
            }
            shared.pending.complete(reclaimable);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use gang_cas::{DiskCas, TreeCache, TreeCacheConfig};
    use gang_config::{DiskEnforcement, DiskTuning, MemoryTuning};
    use gang_reapi::build::bazel::remote::execution::v2::{Directory, FileNode};
    use gang_reapi::to_proto;
    use gang_spi::{BlobStream, ContentStore};
    use gang_testkit::TestResult;
    use gang_types::{AttemptId, Digest, Percent};
    use tempfile::TempDir;

    use super::*;
    use crate::worker_sampling::{
        AttemptRegistration, SampleProbe, SamplingConfig, SamplingRegistry, WorkerSampler,
        kill_channel,
    };

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("non-zero")
    }

    #[tokio::test]
    async fn instant_rename_credit_accounting_and_drain() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().canonicalize()?;
        let pending = Arc::new(GraveyardPending::new());
        let sampling = SamplingRegistry::new(
            gang_config::CpuTuning::default(),
            MemoryTuning::default(),
            DiskTuning::default(),
        );

        let graveyard = Graveyard::open(&work, nz(2), Arc::clone(&pending), Some(sampling)).await?;
        assert!(graveyard.dir().is_dir());

        let attempt_name = "00000000-0000-4000-8000-000000000001";
        let attempt_dir = work.join(attempt_name);
        fs::create_dir_all(attempt_dir.join("work/sub"))?;
        fs::write(attempt_dir.join("work/sub/file.bin"), b"payload-bytes")?;

        graveyard.retire_attempt(&attempt_dir, 10_000, 3_500)?;
        assert!(!attempt_dir.exists(), "rename must be synchronous");
        assert!(graveyard.pending_bytes() <= 6_500);

        graveyard.drain().await;
        assert_eq!(graveyard.pending_bytes(), 0);
        assert_eq!(graveyard.failures(), 0);
        assert!(!graveyard.dir().join(attempt_name).exists());
        Ok(())
    }

    #[tokio::test]
    async fn read_only_and_zero_mode_directory_handling() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().canonicalize()?;
        let pending = Arc::new(GraveyardPending::new());
        let graveyard = Graveyard::open(&work, nz(2), Arc::clone(&pending), None).await?;

        // Read-only (0555) attempt and nested directories must always be repaired and unlinked.
        let ro_attempt = work.join("00000000-0000-4000-8000-000000000010");
        let ro_nested = ro_attempt.join("work/readonly");
        fs::create_dir_all(&ro_nested)?;
        fs::write(ro_nested.join("data"), b"hello")?;
        fs::set_permissions(&ro_nested, fs::Permissions::from_mode(0o555))?;
        fs::set_permissions(&ro_attempt, fs::Permissions::from_mode(0o555))?;

        graveyard.retire_attempt(&ro_attempt, 4_096, 0)?;
        graveyard.drain().await;
        assert!(!ro_attempt.exists());
        assert!(
            !graveyard
                .dir()
                .join("00000000-0000-4000-8000-000000000010")
                .exists()
        );
        assert_eq!(graveyard.pending_bytes(), 0);
        assert_eq!(graveyard.failures(), 0);

        // Chmod 0000 subdirectory: repaired on Linux via /proc/self/fd; fails closed on macOS
        // incrementing `failures` while still decrementing `pending_bytes` and keeping the pool alive.
        let zero_attempt_name = "00000000-0000-4000-8000-000000000011";
        let zero_attempt = work.join(zero_attempt_name);
        let locked = zero_attempt.join("work/locked");
        fs::create_dir_all(&locked)?;
        fs::write(locked.join("secret"), b"abc")?;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;

        graveyard.retire_attempt(&zero_attempt, 8_192, 0)?;
        graveyard.drain().await;
        assert_eq!(graveyard.pending_bytes(), 0);

        #[cfg(target_os = "linux")]
        {
            assert_eq!(graveyard.failures(), 0);
            assert!(!graveyard.dir().join(zero_attempt_name).exists());
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(graveyard.failures(), 1);
            assert!(graveyard.dir().join(zero_attempt_name).exists());
            // Restore permissions so TempDir cleanup succeeds cleanly.
            fs::set_permissions(
                graveyard.dir().join(zero_attempt_name).join("work/locked"),
                fs::Permissions::from_mode(0o700),
            )?;
        }

        // Subsequent valid attempts continue to unlink cleanly without jamming.
        let follow_up = work.join("00000000-0000-4000-8000-000000000012");
        fs::create_dir_all(&follow_up)?;
        fs::write(follow_up.join("ok"), b"ok")?;
        graveyard.retire_attempt(&follow_up, 512, 0)?;
        graveyard.drain().await;
        assert!(
            !graveyard
                .dir()
                .join("00000000-0000-4000-8000-000000000012")
                .exists()
        );
        assert_eq!(graveyard.pending_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn startup_remnant_sweep_cleans_stale_graveyard_entries() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().canonicalize()?;
        let stale_dir = work.join(GRAVEYARD_DIR).join("stale-attempt/sub");
        fs::create_dir_all(&stale_dir)?;
        fs::write(stale_dir.join("leftover"), b"stale")?;
        fs::set_permissions(&stale_dir, fs::Permissions::from_mode(0o555))?;
        fs::write(work.join(GRAVEYARD_DIR).join("stray-file"), b"x")?;

        let pending = Arc::new(GraveyardPending::new());
        let graveyard = Graveyard::open(&work, nz(2), pending, None).await?;
        assert!(!work.join(GRAVEYARD_DIR).join("stale-attempt").exists());
        assert!(!work.join(GRAVEYARD_DIR).join("stray-file").exists());
        assert!(graveyard.dir().is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn evicted_tree_releases_cas_pins_only_after_unlink() -> TestResult {
        let tmp = TempDir::new()?;
        let root = tmp.path().canonicalize()?;
        let work = root.join("work");
        let cas_dir = root.join("cas");
        let trees_dir = root.join("trees");

        let pending = Arc::new(GraveyardPending::new());
        let graveyard = Graveyard::open(&work, nz(1), Arc::clone(&pending), None).await?;
        let cas = DiskCas::open(&cas_dir, 1024 * 1024).await?;

        let blob1 = Bytes::from_static(b"tree-one-blob-content");
        let digest1 = Digest::from_bytes(&blob1);
        cas.write(digest1, BlobStream::from_bytes(blob1)).await?;

        let blob2 = Bytes::from_static(b"tree-two-blob-content");
        let digest2 = Digest::from_bytes(&blob2);
        cas.write(digest2, BlobStream::from_bytes(blob2)).await?;

        let dir1 = Directory {
            files: vec![FileNode {
                name: "f1.txt".to_owned(),
                digest: Some(to_proto(&digest1)?),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let tree1_digest = Digest::from_bytes(&prost::Message::encode_to_vec(&dir1));

        let dir2 = Directory {
            files: vec![FileNode {
                name: "f2.txt".to_owned(),
                digest: Some(to_proto(&digest2)?),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let tree2_digest = Digest::from_bytes(&prost::Message::encode_to_vec(&dir2));

        let tree_map = HashMap::from([(tree1_digest, dir1), (tree2_digest, dir2)]);

        let graveyard_hook = graveyard.clone();
        let trees = TreeCache::open_with_evict_hook(
            &trees_dir,
            Some(graveyard.dir().to_path_buf()),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                max_trees: 1,
                tree_pin_percent: Percent::from_percent(100),
            },
            move |evicted| graveyard_hook.retire_evicted_tree(evicted),
        )
        .await?;

        let lease1 = trees
            .get_or_materialize(tree1_digest, &tree_map)
            .await?
            .ok_or("tree1 materialized")?;
        assert_eq!(cas.tree_pinned_bytes(), digest1.size());
        drop(lease1);

        // Materializing tree2 evicts tree1 into graveyard via `retire_evicted_tree`.
        let lease2 = trees
            .get_or_materialize(tree2_digest, &tree_map)
            .await?
            .ok_or("tree2 materialized")?;

        graveyard.drain().await;
        assert_eq!(graveyard.pending_bytes(), 0);
        assert_eq!(graveyard.failures(), 0);
        // Only tree2's blob remains tree-pinned; tree1's pins were released after unlink.
        assert_eq!(cas.tree_pinned_bytes(), digest2.size());
        // No `tree-*` directory should remain in `.graveyard`.
        let remaining: Vec<_> = fs::read_dir(graveyard.dir())?.collect::<Result<_, _>>()?;
        assert!(remaining.is_empty(), "graveyard not empty: {remaining:?}");
        drop(lease2);
        Ok(())
    }

    struct DummyProbe;
    impl SampleProbe for DummyProbe {
        fn read_attempt_cgroup_path(&self, _side_channel_dir: &Path) -> Option<PathBuf> {
            None
        }
        fn read_attempt_cgroup(&self, _cgroup: &Path) -> crate::AttemptCgroupSample {
            crate::AttemptCgroupSample::default()
        }
        fn read_attempt_disk(&self, _attempt_dir: &Path) -> Option<u64> {
            Some(500)
        }
        fn read_memory_pool(&self, _pool_cgroup: &Path) -> crate::MemoryPoolSample {
            crate::MemoryPoolSample::default()
        }
        fn read_disk_space(&self, _work_dir: &Path) -> Option<gang_scheduler::DiskSample> {
            Some(gang_scheduler::DiskSample {
                free_bytes: 1_000,
                total_bytes: 100_000,
                graveyard_pending_bytes: 0,
            })
        }
    }

    #[tokio::test]
    async fn notifies_quiescence_gate_when_retired_attempt_unlinks() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().canonicalize()?;
        let pending = Arc::new(GraveyardPending::new());

        let disk_tuning = DiskTuning {
            enforcement: DiskEnforcement::Pressure,
            pressure_kill_after: gang_types::NonZeroDuration::from_millis(1),
            ..DiskTuning::default()
        };
        let mut sampler = WorkerSampler::new(
            SamplingConfig {
                cpu: gang_config::CpuTuning::default(),
                physical_cpu_millicores: 4_000,
                memory: MemoryTuning::default(),
                disk: disk_tuning,
                memory_pool: None,
                work_directory: work.clone(),
                disk_per_instance_bytes: 100_000,
            },
            DummyProbe,
        );
        let registry = sampler.registry();
        let graveyard = Graveyard::open(&work, nz(2), pending, Some(registry.clone())).await?;

        let attempt = AttemptId::try_new("00000000-0000-4000-8000-000000000099")?;
        let attempt_dir = work.join(attempt.as_str());
        fs::create_dir_all(&attempt_dir)?;

        let (tx, _rx) = kill_channel();
        let guard = registry.register(AttemptRegistration {
            attempt: attempt.clone(),
            attempt_dir: attempt_dir.clone(),
            side_channel_dir: work.join(".side_channels").join(attempt.as_str()),
            reserved_cpu_millicores: 0,
            guaranteed: false,
            reserved_memory_bytes: 0,
            reserved_disk_bytes: 100,
            kill: tx,
        });
        assert_eq!(sampler.sample_disk_attempts().len(), 0);
        // Trigger disk pressure kill so quiescence waits for both victim removal and graveyard.
        let t0 = gang_clock::Tick::ORIGIN;
        assert_eq!(sampler.poll_disk_pressure(t0), None);
        let victim = sampler.poll_disk_pressure(t0 + Duration::from_millis(5));
        assert_eq!(victim, Some(attempt));
        assert!(!registry.is_quiescence_settled());

        drop(guard);
        assert!(!registry.is_quiescence_settled());

        graveyard.retire_attempt(&attempt_dir, 1_000, 0)?;
        graveyard.drain().await;
        assert!(registry.is_quiescence_settled());
        Ok(())
    }
}
