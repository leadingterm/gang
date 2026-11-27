//! Bind-only immutable directory tree cache ([`TreeCache`]) backed by [`DiskCas`] and
//! [`SingleFlight`].
//!
//! Materialized trees live under `<trees_root>/<hex>-<size>` with read-only permissions (`0555`
//! directories, `0444`/`0555` files hardlinked from [`DiskCas`]) and deterministic timestamps
//! (`2000-01-01T00:00:00Z`). No manifest file is written and no clone/link-loop fallback exists:
//! if caps cannot be satisfied without evicting leased trees, [`TreeCache::get_or_materialize`]
//! returns `Ok(None)` so the caller stages files directly.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use gang_reapi::build::bazel::remote::execution::v2::Directory;
use gang_reapi::from_proto;
use gang_spi::{ByteRange, ContentStore, PortError, Rejection};
use gang_types::Digest;
use gang_types::Percent;
use parking_lot::Mutex;
use rustix::fs::{
    AtFlags, CWD, Mode, OFlags, Timespec, Timestamps, fchmod, futimens, openat, utimensat,
};

use crate::disk::{BlockingScope, DiskCas, WriteOutcome};
use crate::flight::{FlightCancel, SingleFlight};
use crate::layout::{DETERMINISTIC_MTIME_SECS, TMP_DIR_NAME, blob_path, create_empty_file};
use crate::lru::{BlobMode, CacheKey};
use crate::pins::CasPin;
use crate::tiered::{FetchOutcome, Tiered};

const DEFAULT_MIN_FILES: usize = 512;
const DEFAULT_MAX_TREES: usize = 256;
const DEFAULT_TREE_PIN_PERCENT: u8 = 50;
const DOT_TMP_DIR_NAME: &str = ".tmp";
const MAX_TREE_DEPTH: usize = 256;
const MAX_EXPANDED_NODES: usize = 250_000;
const MAX_EXPANDED_PATH_BYTES: usize = 64 * 1024 * 1024;

static TREE_TMP_SEQ: AtomicU64 = AtomicU64::new(1);

type EvictHook = Arc<dyn Fn(EvictedTree) + Send + Sync>;

/// Configuration governing [`TreeCache`] admission and capacity limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeCacheConfig {
    /// Minimum recursive file count for a subtree to qualify for admission (default `512`).
    pub min_files: usize,
    /// Maximum number of cached trees retained simultaneously (default `256`).
    pub max_trees: usize,
    /// Percentage (`1..=100`) of [`DiskCas`] budget usable for distinct tree pins (default `50`).
    pub tree_pin_percent: Percent,
}

impl Default for TreeCacheConfig {
    fn default() -> Self {
        Self {
            min_files: DEFAULT_MIN_FILES,
            max_trees: DEFAULT_MAX_TREES,
            tree_pin_percent: Percent::from_percent(DEFAULT_TREE_PIN_PERCENT),
        }
    }
}

impl TreeCacheConfig {
    /// Computes the maximum distinct `tree_pinned_bytes` allowed for `fast_cas_budget`.
    #[must_use]
    pub fn max_tree_pinned_bytes(&self, fast_cas_budget: u64) -> u64 {
        self.tree_pin_percent.of_u64(fast_cas_budget)
    }
}

/// An unleased tree renamed out of [`TreeCache`] into `<graveyard>/tree-<uuid>`.
///
/// Holds the [`CasPin`] guards for every hardlinked [`DiskCas`] inode inside the tree so
/// `tree_pins` are released **only after** the graveyard unlink completes. Every file in a
/// materialized tree is shared with [`DiskCas`], so [`Self::reclaimable_bytes`] is `0`.
#[derive(Debug)]
pub struct EvictedTree {
    path: PathBuf,
    cas_shared_bytes: u64,
    walk_bytes: u64,
    pins: Vec<CasPin>,
}

impl EvictedTree {
    /// Returns the unique graveyard directory path (`<graveyard>/tree-<uuid>`).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the byte total shared with [`DiskCas`] inodes.
    #[must_use]
    pub const fn cas_shared_bytes(&self) -> u64 {
        self.cas_shared_bytes
    }

    /// Returns the walked byte size of the evicted tree.
    #[must_use]
    pub const fn walk_bytes(&self) -> u64 {
        self.walk_bytes
    }

    /// Returns bytes freed by unlinking this graveyard entry (`walk_bytes - cas_shared_bytes == 0`).
    #[must_use]
    pub const fn reclaimable_bytes(&self) -> u64 {
        self.walk_bytes.saturating_sub(self.cas_shared_bytes)
    }

    /// Returns the number of [`CasPin`] guards held until unlink completes.
    #[must_use]
    pub fn pin_count(&self) -> usize {
        self.pins.len()
    }

    /// Recursively unlinks the graveyard directory and releases held [`CasPin`] guards only after
    /// removal finishes.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if directory removal fails; `pins` are still released when `self`
    /// drops on return.
    pub fn unlink_and_release(self) -> Result<(), PortError> {
        let result = gang_os::fs::remove_dir_optional(&self.path).map_err(PortError::backend);
        drop(self.pins);
        result
    }
}

/// RAII lease on an immutable materialized directory tree inside [`TreeCache`].
///
/// While at least one [`TreeLease`] exists for a tree, its entry cannot be evicted. Attempts hold
/// leases until `Executed` (or drop), covering the full lifetime of read-only bind mounts.
#[derive(Debug)]
pub struct TreeLease {
    state: Arc<Mutex<TreeIndex>>,
    digest: Digest,
    path: PathBuf,
    file_count: usize,
    total_bytes: u64,
}

impl TreeLease {
    /// Returns the host directory path (`<trees_root>/<hex>-<size>`) to bind-mount read-only.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the root `Directory` [`Digest`] of this cached tree.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns the number of regular files in this materialized tree.
    #[must_use]
    pub const fn file_count(&self) -> usize {
        self.file_count
    }

    /// Returns the total logical bytes of files in this materialized tree.
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl Drop for TreeLease {
    fn drop(&mut self) {
        let mut index = self.state.lock();
        if let Some(entry) = index.entries.get_mut(&self.digest) {
            entry.leases = entry.leases.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
struct TreeEntry {
    path: PathBuf,
    file_count: usize,
    total_bytes: u64,
    pins: Vec<CasPin>,
    leases: usize,
    stamp: u64,
}

#[derive(Debug, Default)]
struct TreeIndex {
    entries: HashMap<Digest, TreeEntry>,
    lru_clock: u64,
}

impl TreeIndex {
    fn next_stamp(&mut self) -> u64 {
        self.lru_clock = self.lru_clock.saturating_add(1);
        self.lru_clock
    }

    fn oldest_unleased_digest(&self) -> Option<Digest> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.leases == 0)
            .min_by_key(|(_, entry)| entry.stamp)
            .map(|(&digest, _)| digest)
    }

    fn has_unleased(&self) -> bool {
        self.entries.values().any(|entry| entry.leases == 0)
    }
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

/// Bind-only immutable directory tree cache backed by [`DiskCas`] and [`SingleFlight`].
#[derive(Clone)]
pub struct TreeCache {
    inner: Arc<TreeCacheInner>,
}

impl fmt::Debug for TreeCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TreeCache")
            .field("root", &self.inner.root)
            .field("graveyard", &self.inner.graveyard)
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

struct TreeCacheInner {
    root: PathBuf,
    graveyard: Option<PathBuf>,
    cas: DiskCas,
    config: TreeCacheConfig,
    state: Arc<Mutex<TreeIndex>>,
    flight: SingleFlight<Digest, Result<bool, SharedPortError>>,
    scope: BlockingScope,
    evict_hook: Option<EvictHook>,
}

impl TreeCache {
    /// Opens a [`TreeCache`] rooted at `root`, cleaning up any pre-existing remnant entries from
    /// earlier processes into `graveyard` (or removing them cleanly when `graveyard` is `None`).
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if `root`, `<root>/.tmp`, or `graveyard` cannot be prepared or stale
    /// entries cannot be swept.
    pub async fn open(
        root: impl AsRef<Path>,
        graveyard: Option<PathBuf>,
        cas: DiskCas,
        config: TreeCacheConfig,
    ) -> Result<Self, PortError> {
        Self::open_inner(root, graveyard, cas, config, None).await
    }

    /// Opens a [`TreeCache`] with a custom `on_evict` sink invoked whenever an unleased tree (or
    /// startup remnant when `graveyard` is `Some`) is moved into `<graveyard>/tree-<uuid>`.
    ///
    /// Production workers pass their graveyard unlink queue here so [`EvictedTree`] is unlinked
    /// asynchronously while retaining [`DiskCas`] `tree_pins` until unlink finishes.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if `root`, `<root>/.tmp`, or `graveyard` cannot be prepared or stale
    /// entries cannot be swept.
    pub async fn open_with_evict_hook<H>(
        root: impl AsRef<Path>,
        graveyard: Option<PathBuf>,
        cas: DiskCas,
        config: TreeCacheConfig,
        on_evict: H,
    ) -> Result<Self, PortError>
    where
        H: Fn(EvictedTree) + Send + Sync + 'static,
    {
        Self::open_inner(root, graveyard, cas, config, Some(Arc::new(on_evict))).await
    }

    async fn open_inner(
        root: impl AsRef<Path>,
        graveyard: Option<PathBuf>,
        cas: DiskCas,
        config: TreeCacheConfig,
        evict_hook: Option<EvictHook>,
    ) -> Result<Self, PortError> {
        let root_path = root.as_ref().to_path_buf();
        let graveyard_clone = graveyard.clone();
        let hook_clone = evict_hook.clone();

        tokio::task::spawn_blocking(move || {
            cleanup_trees_root(&root_path, graveyard_clone.as_deref(), hook_clone.as_ref())
        })
        .await
        .map_err(PortError::backend)??;

        Ok(Self {
            inner: Arc::new(TreeCacheInner {
                root: root.as_ref().to_path_buf(),
                graveyard,
                cas,
                config,
                state: Arc::new(Mutex::new(TreeIndex::default())),
                flight: SingleFlight::new(),
                scope: BlockingScope::new(),
                evict_hook,
            }),
        })
    }

    /// Returns the `<trees_root>` directory path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Returns the configured graveyard directory, if any.
    #[must_use]
    pub fn graveyard(&self) -> Option<&Path> {
        self.inner.graveyard.as_deref()
    }

    /// Returns the [`TreeCacheConfig`].
    #[must_use]
    pub fn config(&self) -> &TreeCacheConfig {
        &self.inner.config
    }

    /// Returns the backing [`DiskCas`].
    #[must_use]
    pub fn cas(&self) -> &DiskCas {
        &self.inner.cas
    }

    /// Signals cancellation of cache-owned fetches and filesystem work.
    pub fn cancel(&self) {
        self.inner.flight.cancel();
        self.inner.scope.cancel();
    }

    /// Cancels tree builds and joins filesystem work before eviction sinks may be drained.
    ///
    /// Callers must stop submitting materializations before shutdown.
    pub async fn shutdown(&self) {
        self.cancel();
        self.inner.flight.drain().await;
        self.inner.scope.cancel_and_join().await;
    }

    /// Waits for detached tree builds, including publication and eviction cleanup dispatch.
    ///
    /// Stop admitting materializations before draining. A custom eviction sink must then drain
    /// its own cleanup queue.
    pub async fn drain(&self) {
        self.inner.flight.drain().await;
    }

    /// Returns the maximum distinct bytes allowed for `tree_pins` under [`DiskCas`].
    #[must_use]
    pub fn max_tree_pinned_bytes(&self) -> u64 {
        self.inner
            .config
            .max_tree_pinned_bytes(self.inner.cas.budget_bytes())
    }

    /// Returns the number of trees currently indexed in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.state.lock().entries.len()
    }

    /// Returns `true` when no trees are currently cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if `digest` is currently present in the cache.
    #[must_use]
    pub fn contains(&self, digest: Digest) -> bool {
        self.inner.state.lock().entries.contains_key(&digest)
    }

    /// Returns the number of cached trees that currently hold at least one active [`TreeLease`].
    #[must_use]
    pub fn leased_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .entries
            .values()
            .filter(|entry| entry.leases > 0)
            .count()
    }

    /// Returns the active [`TreeLease`] count for `digest`, or `0` if absent or unleased.
    #[must_use]
    pub fn lease_count(&self, digest: Digest) -> usize {
        self.inner
            .state
            .lock()
            .entries
            .get(&digest)
            .map_or(0, |entry| entry.leases)
    }

    /// Returns an RAII [`TreeLease`] for the immutable directory tree rooted at `root`,
    /// materializing it once via [`SingleFlight`] on first use (`admit_after = 1`).
    ///
    /// Expects all non-empty file blobs referenced by `directories` to already be present in
    /// [`DiskCas`]. Returns `Ok(None)` without falling back to clones or link-loops if:
    /// - the subtree has fewer than `tree_cache_min_files` regular files, or
    /// - `tree_cache_max_trees` or `tree_pin_percent` cannot be satisfied without evicting
    ///   currently leased (`leases > 0`) entries.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Rejected`] with [`Rejection::MalformedRequest`] if the REAPI
    /// `Directory` graph is invalid (missing directory, cycle, invalid node name, or bad digest),
    /// or [`PortError::Backend`] on missing blobs or filesystem errors.
    pub async fn get_or_materialize(
        &self,
        root: Digest,
        directories: &HashMap<Digest, Directory>,
    ) -> Result<Option<TreeLease>, PortError> {
        if let Some(lease) = self.acquire_existing_lease(root) {
            return Ok(Some(lease));
        }

        let plan = build_tree_plan(root, directories)?;
        if plan.files.len() < self.inner.config.min_files || self.inner.config.max_trees == 0 {
            return Ok(None);
        }

        self.drive_materialization(root, plan, |_, _| async { Ok(Some(Vec::new())) })
            .await
    }

    /// Like [`Self::get_or_materialize`], but fetches any file blobs missing from [`DiskCas`]
    /// through `tiered` first.
    ///
    /// Returns `Ok(None)` if any file in the subtree bypasses [`DiskCas`] (`FetchOutcome::Bypassed`)
    /// or if cache caps cannot be satisfied without evicting leased trees.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if fetching blobs, validating directories, or materializing on disk
    /// fails.
    pub async fn get_or_materialize_from<F, S>(
        &self,
        root: Digest,
        directories: &HashMap<Digest, Directory>,
        tiered: &Tiered<F, S>,
    ) -> Result<Option<TreeLease>, PortError>
    where
        F: std::borrow::Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
        S: ContentStore,
    {
        if let Some(lease) = self.acquire_existing_lease(root) {
            return Ok(Some(lease));
        }

        let plan = build_tree_plan(root, directories)?;
        if plan.files.len() < self.inner.config.min_files || self.inner.config.max_trees == 0 {
            return Ok(None);
        }

        let tiered = tiered.clone();
        let cas = self.inner.cas.clone();
        self.drive_materialization(root, plan, move |plan, cancel| {
            let tiered = tiered.clone();
            let cas = cas.clone();
            async move { fetch_tree_files(root, &plan, &cas, &tiered, &cancel).await }
        })
        .await
    }

    async fn drive_materialization<P, Fut>(
        &self,
        root: Digest,
        plan: TreePlan,
        prepare: P,
    ) -> Result<Option<TreeLease>, PortError>
    where
        P: Fn(Arc<TreePlan>, FlightCancel) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Vec<CasPin>>, PortError>> + Send + 'static,
    {
        let shared_plan = Arc::new(plan);
        let prepare = Arc::new(prepare);
        loop {
            if let Some(lease) = self.acquire_existing_lease(root) {
                return Ok(Some(lease));
            }

            let inner = Arc::clone(&self.inner);
            let plan = Arc::clone(&shared_plan);
            let prepare = Arc::clone(&prepare);
            let admitted = self
                .inner
                .flight
                .run_detached_cancellable(root, move |cancel| async move {
                    let result = async {
                        if cancel.is_cancelled() {
                            return Err(PortError::unavailable("tree build cancelled"));
                        }
                        let Some(staging_pins) = prepare(Arc::clone(&plan), cancel.clone()).await?
                        else {
                            return Ok(false);
                        };
                        let result = materialize_tree(&inner, root, &plan).await;
                        drop(staging_pins);
                        result
                    }
                    .await;
                    result.map_err(SharedPortError::new)
                })
                .await
                .map_err(SharedPortError::into_port_error)?;

            if !admitted {
                return Ok(None);
            }

            if let Some(lease) = self.acquire_existing_lease(root) {
                return Ok(Some(lease));
            }
        }
    }

    fn acquire_existing_lease(&self, root: Digest) -> Option<TreeLease> {
        let mut index = self.inner.state.lock();
        let stamp = index.next_stamp();
        let entry = index.entries.get_mut(&root)?;
        entry.leases = entry.leases.saturating_add(1);
        entry.stamp = stamp;
        Some(TreeLease {
            state: Arc::clone(&self.inner.state),
            digest: root,
            path: entry.path.clone(),
            file_count: entry.file_count,
            total_bytes: entry.total_bytes,
        })
    }
}

async fn fetch_tree_files<F, S>(
    root: Digest,
    plan: &TreePlan,
    cas: &DiskCas,
    tiered: &Tiered<F, S>,
    cancel: &FlightCancel,
) -> Result<Option<Vec<CasPin>>, PortError>
where
    F: std::borrow::Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    // Keep earlier blobs resident until the materializer has acquired tree pins.
    let mut pins = Vec::new();
    let mut seen_keys = HashSet::new();
    for file in &plan.files {
        if file.digest.is_empty() {
            continue;
        }
        let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
        if !seen_keys.insert(key) {
            continue;
        }
        if let Some(pin) = cas.pin_attempt(file.digest, file.executable) {
            pins.push(pin);
            continue;
        }
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(PortError::unavailable("tree fetch cancelled"));
            }
            result = tiered.fetch_to_local(file.digest, file.executable) => result?,
        };
        match outcome {
            FetchOutcome::Empty => {}
            FetchOutcome::Cached(pin) => pins.push(pin),
            FetchOutcome::Bypassed => return Ok(None),
            FetchOutcome::Missing => {
                return Err(PortError::backend(format!(
                    "missing blob {} for tree {}",
                    file.digest, root
                )));
            }
        }
    }
    Ok(Some(pins))
}

#[derive(Debug, Clone)]
struct PlannedFile {
    rel_path: PathBuf,
    digest: Digest,
    executable: bool,
}

#[derive(Debug, Clone)]
struct PlannedSymlink {
    rel_path: PathBuf,
    target: String,
}

#[derive(Debug, Clone)]
struct TreePlan {
    dirs: Vec<PathBuf>,
    files: Vec<PlannedFile>,
    symlinks: Vec<PlannedSymlink>,
    total_bytes: u64,
}

#[derive(Clone, Copy)]
struct Expansion {
    nodes: usize,
    path_bytes: usize,
    depth: usize,
}

fn expansion_limit() -> PortError {
    PortError::rejected(
        Rejection::MalformedRequest,
        io::Error::other("REAPI directory tree exceeds expanded node or path-byte limits"),
    )
}

// Memoize by digest before allocating per-path state. A diamond DAG is accounted for in O(V+E)
// work rather than enumerating its exponentially many materialized paths.
fn measure_expansion(
    digest: Digest,
    directories: &HashMap<Digest, Directory>,
    memo: &mut HashMap<Digest, Expansion>,
    visiting: &mut HashSet<Digest>,
) -> Result<Expansion, PortError> {
    if let Some(&expansion) = memo.get(&digest) {
        return Ok(expansion);
    }
    if visiting.len() >= MAX_TREE_DEPTH || !visiting.insert(digest) {
        return Err(PortError::rejected(
            Rejection::MalformedRequest,
            io::Error::other("REAPI directory tree is cyclic or exceeds maximum depth"),
        ));
    }
    let dir = directories.get(&digest).ok_or_else(|| {
        PortError::rejected(
            Rejection::MalformedRequest,
            io::Error::other(format!("missing Directory {digest}")),
        )
    })?;
    let mut expansion = Expansion {
        nodes: 1,
        path_bytes: 0,
        depth: 1,
    };
    for name in dir
        .files
        .iter()
        .map(|node| node.name.as_str())
        .chain(dir.symlinks.iter().map(|node| node.name.as_str()))
    {
        validate_node_name(name)?;
        expansion.nodes = expansion.nodes.checked_add(1).ok_or_else(expansion_limit)?;
        expansion.path_bytes = expansion
            .path_bytes
            .checked_add(name.len())
            .ok_or_else(expansion_limit)?;
    }
    for child in &dir.directories {
        validate_node_name(&child.name)?;
        let proto = child.digest.as_ref().ok_or_else(|| {
            PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("directory `{}` has no digest", child.name)),
            )
        })?;
        let child_digest = from_proto(proto)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
        let child_expansion = measure_expansion(child_digest, directories, memo, visiting)?;
        expansion.nodes = expansion
            .nodes
            .checked_add(child_expansion.nodes)
            .ok_or_else(expansion_limit)?;
        let prefix_bytes = child
            .name
            .len()
            .checked_add(1)
            .and_then(|prefix| prefix.checked_mul(child_expansion.nodes))
            .and_then(|bytes| bytes.checked_sub(1))
            .ok_or_else(expansion_limit)?;
        expansion.path_bytes = expansion
            .path_bytes
            .checked_add(child_expansion.path_bytes)
            .and_then(|bytes| bytes.checked_add(prefix_bytes))
            .ok_or_else(expansion_limit)?;
        expansion.depth = expansion.depth.max(child_expansion.depth + 1);
        if expansion.nodes > MAX_EXPANDED_NODES || expansion.path_bytes > MAX_EXPANDED_PATH_BYTES {
            return Err(expansion_limit());
        }
    }
    if expansion.nodes > MAX_EXPANDED_NODES || expansion.path_bytes > MAX_EXPANDED_PATH_BYTES {
        return Err(expansion_limit());
    }
    if expansion.depth > MAX_TREE_DEPTH {
        return Err(PortError::rejected(
            Rejection::MalformedRequest,
            io::Error::other("REAPI directory tree exceeds maximum depth"),
        ));
    }
    visiting.remove(&digest);
    memo.insert(digest, expansion);
    Ok(expansion)
}

fn build_tree_plan(
    root: Digest,
    directories: &HashMap<Digest, Directory>,
) -> Result<TreePlan, PortError> {
    measure_expansion(root, directories, &mut HashMap::new(), &mut HashSet::new())?;
    let mut plan = TreePlan {
        dirs: Vec::new(),
        files: Vec::new(),
        symlinks: Vec::new(),
        total_bytes: 0,
    };

    let mut visiting = HashSet::new();
    let mut stack: Vec<WalkFrame<'_>> = Vec::new();
    let root_dir = directories.get(&root).ok_or_else(|| {
        PortError::rejected(
            Rejection::MalformedRequest,
            io::Error::other(format!("missing root Directory {root}")),
        )
    })?;
    visiting.insert(root);
    stack.push(WalkFrame::enter(root, PathBuf::new(), root_dir)?);

    while let Some(frame) = stack.last_mut() {
        if let Some(next_subdir) = frame.pending_subdirs.pop_front() {
            if stack.len() >= MAX_TREE_DEPTH {
                return Err(PortError::rejected(
                    Rejection::MalformedRequest,
                    io::Error::other("REAPI directory tree exceeds maximum depth"),
                ));
            }
            if !visiting.insert(next_subdir.digest) {
                return Err(PortError::rejected(
                    Rejection::MalformedRequest,
                    io::Error::other(format!(
                        "cycle detected at Directory {}",
                        next_subdir.digest
                    )),
                ));
            }
            let dir_msg = directories.get(&next_subdir.digest).ok_or_else(|| {
                PortError::rejected(
                    Rejection::MalformedRequest,
                    io::Error::other(format!("missing child Directory {}", next_subdir.digest)),
                )
            })?;
            plan.dirs.push(next_subdir.rel_path.clone());
            stack.push(WalkFrame::enter(
                next_subdir.digest,
                next_subdir.rel_path,
                dir_msg,
            )?);
            continue;
        }

        let Some(finished) = stack.pop() else {
            break;
        };
        visiting.remove(&finished.digest);
        collect_directory_entries(&finished, &mut plan)?;
    }

    plan.dirs.sort_by_key(|path| path.components().count());
    Ok(plan)
}

fn collect_directory_entries(frame: &WalkFrame<'_>, plan: &mut TreePlan) -> Result<(), PortError> {
    let mut seen_names = HashSet::new();

    for dir_node in &frame.dir.directories {
        validate_node_name(&dir_node.name)?;
        if !seen_names.insert(dir_node.name.as_str()) {
            return Err(PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("duplicate directory entry `{}`", dir_node.name)),
            ));
        }
    }

    for file_node in &frame.dir.files {
        validate_node_name(&file_node.name)?;
        if !seen_names.insert(file_node.name.as_str()) {
            return Err(PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("duplicate file entry `{}`", file_node.name)),
            ));
        }
        let file_proto = file_node.digest.as_ref().ok_or_else(|| {
            PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("file `{}` has no digest", file_node.name)),
            )
        })?;
        let file_digest = from_proto(file_proto)
            .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
        plan.total_bytes = plan.total_bytes.saturating_add(file_digest.size());
        plan.files.push(PlannedFile {
            rel_path: frame.rel_path.join(&file_node.name),
            digest: file_digest,
            executable: file_node.is_executable,
        });
    }

    for sym_node in &frame.dir.symlinks {
        validate_node_name(&sym_node.name)?;
        if !seen_names.insert(sym_node.name.as_str()) {
            return Err(PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("duplicate symlink entry `{}`", sym_node.name)),
            ));
        }
        if sym_node.target.is_empty() || sym_node.target.contains('\0') {
            return Err(PortError::rejected(
                Rejection::MalformedRequest,
                io::Error::other(format!("invalid symlink target for `{}`", sym_node.name)),
            ));
        }
        plan.symlinks.push(PlannedSymlink {
            rel_path: frame.rel_path.join(&sym_node.name),
            target: sym_node.target.clone(),
        });
    }

    Ok(())
}

struct PendingSubdir {
    digest: Digest,
    rel_path: PathBuf,
}

struct WalkFrame<'a> {
    digest: Digest,
    rel_path: PathBuf,
    dir: &'a Directory,
    pending_subdirs: VecDeque<PendingSubdir>,
}

impl<'a> WalkFrame<'a> {
    fn enter(digest: Digest, rel_path: PathBuf, dir: &'a Directory) -> Result<Self, PortError> {
        let mut pending = VecDeque::with_capacity(dir.directories.len());
        for child in &dir.directories {
            let child_proto = child.digest.as_ref().ok_or_else(|| {
                PortError::rejected(
                    Rejection::MalformedRequest,
                    io::Error::other(format!("directory `{}` has no digest", child.name)),
                )
            })?;
            let child_digest = from_proto(child_proto)
                .map_err(|err| PortError::rejected(Rejection::MalformedRequest, err))?;
            pending.push_back(PendingSubdir {
                digest: child_digest,
                rel_path: rel_path.join(&child.name),
            });
        }
        Ok(Self {
            digest,
            rel_path,
            dir,
            pending_subdirs: pending,
        })
    }
}

fn validate_node_name(name: &str) -> Result<(), PortError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(PortError::rejected(
            Rejection::MalformedRequest,
            io::Error::other(format!("invalid REAPI node name `{name}`")),
        ));
    }
    Ok(())
}

async fn materialize_tree(
    inner: &TreeCacheInner,
    root: Digest,
    plan: &Arc<TreePlan>,
) -> Result<bool, PortError> {
    {
        let index = inner.state.lock();
        if index.entries.contains_key(&root) {
            return Ok(true);
        }
        if index.entries.len() >= inner.config.max_trees && !index.has_unleased() {
            return Ok(false);
        }
    }

    if inner.scope.is_cancelled() {
        return Err(PortError::unavailable("tree build cancelled"));
    }
    let max_tree_pinned = inner.config.max_tree_pinned_bytes(inner.cas.budget_bytes());
    let Some(pins) = acquire_tree_pins(inner, plan, max_tree_pinned).await? else {
        return Ok(false);
    };

    // Ensure max_trees room before building the directory on disk (and again before inserting).
    while needs_tree_slot(inner) {
        if !evict_one_unleased(inner).await? {
            drop(pins);
            return Ok(false);
        }
    }

    let final_path = tree_dir_path(&inner.root, root);
    let root_dir = inner.root.clone();
    let cas = inner.cas.clone();
    let plan_owned = Arc::clone(plan);
    let final_path_clone = final_path.clone();

    inner
        .scope
        .run(move |cancelled| {
            build_and_publish_tree(&root_dir, &final_path_clone, &cas, &plan_owned, cancelled)
        })
        .await
        .map_err(PortError::backend)??;

    // Re-check `max_trees` right before registering under the index lock.
    while needs_tree_slot(inner) {
        if !evict_one_unleased(inner).await? {
            inner
                .scope
                .run(move |_| gang_os::fs::remove_dir_optional(&final_path))
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::backend)?;
            drop(pins);
            return Ok(false);
        }
    }

    let mut index = inner.state.lock();
    if index.entries.contains_key(&root) {
        // Already present (race with startup or duplicate): keep existing entry.
        drop(index);
        drop(pins);
        return Ok(true);
    }
    let stamp = index.next_stamp();
    index.entries.insert(
        root,
        TreeEntry {
            path: tree_dir_path(&inner.root, root),
            file_count: plan.files.len(),
            total_bytes: plan.total_bytes,
            pins,
            leases: 0,
            stamp,
        },
    );
    Ok(true)
}

fn check_tree_cancellation(cancelled: &AtomicBool) -> Result<(), PortError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(PortError::unavailable("tree build cancelled"));
    }
    Ok(())
}

fn needs_tree_slot(inner: &TreeCacheInner) -> bool {
    let index = inner.state.lock();
    index.entries.len() >= inner.config.max_trees
}

async fn acquire_tree_pins(
    inner: &TreeCacheInner,
    plan: &TreePlan,
    max_tree_pinned_bytes: u64,
) -> Result<Option<Vec<CasPin>>, PortError> {
    let mut pins = Vec::new();
    let mut seen_keys = HashSet::new();

    for file in &plan.files {
        if file.digest.is_empty() {
            continue;
        }
        let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
        if !seen_keys.insert(key) {
            continue;
        }

        loop {
            if let Some(pin) =
                inner
                    .cas
                    .pin_tree(file.digest, file.executable, max_tree_pinned_bytes)
            {
                pins.push(pin);
                break;
            }

            // `pin_tree` failed: distinguish missing blob from `tree_pinned_bytes` cap.
            if inner
                .cas
                .pin_attempt(file.digest, file.executable)
                .is_none()
            {
                // Try promoting opposite mode in `DiskCas` if present.
                if let Some(_other) = inner.cas.pin_attempt(file.digest, !file.executable) {
                    if !inner.cas.can_admit(file.digest.size()) {
                        return Ok(None);
                    }
                    if let Some(stream) = inner.cas.read(file.digest, ByteRange::FULL).await? {
                        match inner
                            .cas
                            .write_with_mode_scoped(
                                file.digest,
                                stream,
                                file.executable,
                                &inner.scope,
                            )
                            .await?
                        {
                            WriteOutcome::Cached => continue,
                            WriteOutcome::Bypassed => return Ok(None),
                        }
                    }
                }
                return Err(PortError::backend(format!(
                    "missing DiskCas blob {} for tree materialization",
                    file.digest
                )));
            }

            // Cap reached: evict an unleased LRU tree and wait for its pins to release.
            if !evict_one_unleased(inner).await? {
                return Ok(None);
            }
        }
    }

    Ok(Some(pins))
}

async fn evict_one_unleased(inner: &TreeCacheInner) -> Result<bool, PortError> {
    let state = Arc::clone(&inner.state);
    let dest_parent = inner
        .graveyard
        .clone()
        .unwrap_or_else(|| inner.root.join(DOT_TMP_DIR_NAME));
    let detached = inner
        .scope
        .run(move |_| {
            let mut index = state.lock();
            let Some(victim) = index.oldest_unleased_digest() else {
                return Ok(None);
            };
            let Some(entry) = index.entries.get(&victim) else {
                return Ok(None);
            };
            // Keep the entry locked until its canonical path is detached. A new publication/lease
            // can then use that key while cleanup owns only this unique graveyard path.
            let path = move_to_graveyard(&entry.path, &dest_parent)?;
            Ok::<_, PortError>(index.entries.remove(&victim).map(|entry| (entry, path)))
        })
        .await
        .map_err(PortError::backend)??;
    let Some((entry, graveyard_path)) = detached else {
        return Ok(false);
    };

    let evicted = EvictedTree {
        path: graveyard_path,
        cas_shared_bytes: entry.total_bytes,
        walk_bytes: entry.total_bytes,
        pins: entry.pins,
    };

    if let Some(hook) = &inner.evict_hook {
        let hook = Arc::clone(hook);
        inner
            .scope
            .run(move |_| hook(evicted))
            .await
            .map_err(PortError::backend)?;
        return Ok(true);
    }

    inner
        .scope
        .run(move |_| evicted.unlink_and_release())
        .await
        .map_err(PortError::backend)??;

    Ok(true)
}

fn build_and_publish_tree(
    trees_root: &Path,
    final_path: &Path,
    cas: &DiskCas,
    plan: &TreePlan,
    cancelled: &AtomicBool,
) -> Result<(), PortError> {
    check_tree_cancellation(cancelled)?;
    let tmp_root = trees_root.join(DOT_TMP_DIR_NAME);
    fs::create_dir_all(&tmp_root).map_err(PortError::backend)?;
    let tmp_dir = tmp_root.join(unique_uuid_v4());
    fs::create_dir(&tmp_dir).map_err(PortError::backend)?;
    let guard = TempTreeGuard {
        path: tmp_dir.clone(),
        armed: true,
    };

    for rel_dir in &plan.dirs {
        check_tree_cancellation(cancelled)?;
        fs::create_dir_all(tmp_dir.join(rel_dir)).map_err(PortError::backend)?;
    }

    for file in &plan.files {
        check_tree_cancellation(cancelled)?;
        let dest = tmp_dir.join(&file.rel_path);
        let mode = BlobMode::from_executable(file.executable);
        if file.digest.is_empty() {
            create_empty_file(&dest, mode).map_err(PortError::backend)?;
        } else {
            let cas_blob = blob_path(cas.root(), CacheKey::new(file.digest, mode));
            match fs::hard_link(&cas_blob, &dest) {
                Ok(()) => {}
                Err(error) if is_link_fallback_errno(&error) => {
                    if !cas.hardlink_or_reflink_to(file.digest, file.executable, &dest)? {
                        return Err(PortError::backend(format!(
                            "missing DiskCas blob {} during tree link",
                            file.digest
                        )));
                    }
                }
                Err(error) => return Err(PortError::backend(error)),
            }
        }
    }

    let times = deterministic_timestamps();
    for sym in &plan.symlinks {
        check_tree_cancellation(cancelled)?;
        let dest = tmp_dir.join(&sym.rel_path);
        symlink(&sym.target, &dest).map_err(PortError::backend)?;
        if let Err(errno) = utimensat(CWD, &dest, &times, AtFlags::SYMLINK_NOFOLLOW)
            && errno != rustix::io::Errno::OPNOTSUPP
        {
            return Err(PortError::backend(io::Error::from(errno)));
        }
    }

    // Normalize subdirectories bottom-up to deterministic mtime and `0555` BEFORE moving
    // `tmp_dir` into `final_path`. Keep `tmp_dir` itself owner-writable during `rename` because
    // macOS requires write permission on a moved directory to update its `..` entry, then
    // normalize `final_path` to `0555` immediately after `rename` before publication in the index.
    let mut dirs_bottom_up = plan.dirs.clone();
    dirs_bottom_up.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for rel_dir in &dirs_bottom_up {
        normalize_dir(&tmp_dir.join(rel_dir), &times).map_err(PortError::backend)?;
    }

    check_tree_cancellation(cancelled)?;
    match fs::rename(&tmp_dir, final_path) {
        Ok(()) => {
            guard.persist();
            normalize_dir(final_path, &times).map_err(PortError::backend)?;
            Ok(())
        }
        Err(error)
            if matches!(error.raw_os_error(), Some(code) if code == rustix::io::Errno::EXIST.raw_os_error()
                || code == rustix::io::Errno::NOTEMPTY.raw_os_error())
                && fs::symlink_metadata(final_path)
                    .is_ok_and(|metadata| metadata.file_type().is_dir()) =>
        {
            Ok(())
        }
        Err(error) => Err(PortError::backend(error)),
    }
}

fn is_link_fallback_errno(error: &io::Error) -> bool {
    let raw = error.raw_os_error();
    raw == Some(rustix::io::Errno::MLINK.raw_os_error())
        || raw == Some(rustix::io::Errno::XDEV.raw_os_error())
}

fn normalize_dir(path: &Path, times: &Timestamps) -> io::Result<()> {
    let fd = openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    futimens(&fd, times)?;
    let read_exec_all = Mode::RUSR
        .union(Mode::XUSR)
        .union(Mode::RGRP)
        .union(Mode::XGRP)
        .union(Mode::ROTH)
        .union(Mode::XOTH);
    fchmod(&fd, read_exec_all)?;
    Ok(())
}

fn deterministic_timestamps() -> Timestamps {
    let secs = i64::try_from(DETERMINISTIC_MTIME_SECS).unwrap_or(0);
    let ts = Timespec {
        tv_sec: secs,
        tv_nsec: 0,
    };
    Timestamps {
        last_access: ts,
        last_modification: ts,
    }
}

fn cleanup_trees_root(
    root: &Path,
    graveyard: Option<&Path>,
    evict_hook: Option<&EvictHook>,
) -> Result<(), PortError> {
    fs::create_dir_all(root).map_err(PortError::backend)?;

    for temp_name in [DOT_TMP_DIR_NAME, TMP_DIR_NAME] {
        let temp_path = root.join(temp_name);
        if temp_path.exists() || temp_path.symlink_metadata().is_ok() {
            remove_any_path(&temp_path)?;
        }
    }

    for entry_res in fs::read_dir(root).map_err(PortError::backend)? {
        let entry = entry_res.map_err(PortError::backend)?;
        let entry_path = entry.path();
        if entry.file_name() == OsStr::new(DOT_TMP_DIR_NAME) {
            continue;
        }
        if let Some(graveyard_dir) = graveyard {
            let moved = move_to_graveyard(&entry_path, graveyard_dir)?;
            let evicted = EvictedTree {
                path: moved,
                cas_shared_bytes: 0,
                walk_bytes: 0,
                pins: Vec::new(),
            };
            if let Some(hook) = evict_hook {
                hook(evicted);
            } else {
                evicted.unlink_and_release()?;
            }
        } else {
            remove_any_path(&entry_path)?;
        }
    }

    fs::create_dir_all(root.join(DOT_TMP_DIR_NAME)).map_err(PortError::backend)?;
    Ok(())
}

fn remove_any_path(path: &Path) -> Result<(), PortError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(PortError::backend(error)),
    };
    if meta.file_type().is_dir() || meta.file_type().is_symlink() {
        gang_os::fs::remove_dir_optional(path).map_err(PortError::backend)
    } else {
        fs::remove_file(path).map_err(PortError::backend)
    }
}

fn move_to_graveyard(src_path: &Path, graveyard_dir: &Path) -> Result<PathBuf, PortError> {
    fs::create_dir_all(graveyard_dir).map_err(PortError::backend)?;
    let uuid = unique_uuid_v4();
    let dest = graveyard_dir.join(format!("tree-{uuid}"));

    if let Ok(meta) = fs::symlink_metadata(src_path)
        && meta.file_type().is_dir()
    {
        // Moving a `0555` directory across parents requires owner write on `src_path` to update
        // its `..` link on macOS.
        let _ = grant_owner_write_dir(src_path);
    }

    match fs::rename(src_path, &dest) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(PortError::backend(error)),
    }
    Ok(dest)
}

fn grant_owner_write_dir(path: &Path) -> io::Result<()> {
    let fd = openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    fchmod(&fd, Mode::RWXU)?;
    Ok(())
}

fn tree_dir_path(root: &Path, digest: Digest) -> PathBuf {
    root.join(format!("{}-{}", digest.hex(), digest.size()))
}

fn unique_uuid_v4() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        let seq = TREE_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = u64::from(std::process::id());
        bytes[..8].copy_from_slice(&pid.to_le_bytes());
        bytes[8..].copy_from_slice(&seq.to_le_bytes());
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
        u16::from_be_bytes([bytes[6], bytes[7]]),
        u16::from_be_bytes([bytes[8], bytes[9]]),
        u64::from_be_bytes([
            0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
        ]),
    )
}

struct TempTreeGuard {
    path: PathBuf,
    armed: bool,
}

impl TempTreeGuard {
    fn persist(mut self) {
        self.armed = false;
    }
}

impl Drop for TempTreeGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = gang_os::fs::remove_dir_optional(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use gang_reapi::build::bazel::remote::execution::v2::{
        Directory, DirectoryNode, FileNode, SymlinkNode,
    };
    use gang_reapi::to_proto;
    use gang_spi::BlobStream;
    use gang_testkit::TestResult;
    use gang_types::Digest;
    use gang_types::Percent;
    use parking_lot::Mutex;
    use prost::Message as _;

    use super::{EvictedTree, TreeCache, TreeCacheConfig};
    use crate::disk::DiskCas;
    use crate::layout::{DETERMINISTIC_MTIME_SECS, blob_path};
    use crate::lru::{BlobMode, CacheKey};

    fn diamond_directories(levels: usize, name_len: usize) -> (Digest, HashMap<Digest, Directory>) {
        let leaf = Directory::default();
        let mut digest = Digest::from_bytes(&leaf.encode_to_vec());
        let mut directories = HashMap::from([(digest, leaf)]);
        for _ in 0..levels {
            let directory = Directory {
                directories: ["a", "b"]
                    .into_iter()
                    .map(|name| DirectoryNode {
                        name: name.repeat(name_len),
                        digest: Some(to_proto(&digest).expect("test digest")),
                    })
                    .collect(),
                ..Directory::default()
            };
            digest = Digest::from_bytes(&directory.encode_to_vec());
            directories.insert(digest, directory);
        }
        (digest, directories)
    }

    #[test]
    fn rejects_exponential_dag_before_expanding_paths() {
        let (root, directories) = diamond_directories(30, 1);
        let error = super::build_tree_plan(root, &directories).expect_err("exponential expansion");
        assert!(matches!(
            error,
            gang_spi::PortError::Rejected {
                reason: gang_spi::Rejection::MalformedRequest,
                ..
            }
        ));
        assert!(
            std::error::Error::source(&error)
                .expect("limit explanation")
                .to_string()
                .contains("expanded node or path-byte limits")
        );
    }

    #[test]
    fn limits_path_bytes_independently_of_node_count() {
        let (root, directories) = diamond_directories(16, 255);
        assert!(super::build_tree_plan(root, &directories).is_err());
        let (root, directories) = diamond_directories(10, 1);
        let plan = super::build_tree_plan(root, &directories).expect("bounded diamond");
        assert_eq!(plan.dirs.len(), 2046);
    }

    #[test]
    fn expansion_limit_allows_twenty_thousand_file_canary() {
        let directory = Directory {
            files: (0..20_000)
                .map(|index| FileNode {
                    name: format!("file-{index}"),
                    digest: Some(to_proto(&Digest::EMPTY).expect("empty digest")),
                    ..FileNode::default()
                })
                .collect(),
            ..Directory::default()
        };
        let root = Digest::from_bytes(&directory.encode_to_vec());
        let plan =
            super::build_tree_plan(root, &HashMap::from([(root, directory)])).expect("canary fits");
        assert_eq!(plan.files.len(), 20_000);
    }

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

    async fn stage_blob(cas: &DiskCas, bytes: &[u8], executable: bool) -> TestResult<Digest> {
        let digest = Digest::from_bytes(bytes);
        cas.write_with_mode(
            digest,
            BlobStream::from_bytes(Bytes::copy_from_slice(bytes)),
            executable,
        )
        .await?;
        Ok(digest)
    }

    fn build_single_file_tree(
        name: &str,
        file_digest: Digest,
        executable: bool,
    ) -> TestResult<(Digest, HashMap<Digest, Directory>)> {
        let sub_dir = Directory {
            files: vec![FileNode {
                name: name.to_owned(),
                digest: Some(to_proto(&file_digest)?),
                is_executable: executable,
                node_properties: None,
            }],
            directories: Vec::new(),
            symlinks: vec![SymlinkNode {
                name: "link".to_owned(),
                target: name.to_owned(),
                node_properties: None,
            }],
            node_properties: None,
        };
        let sub_bytes = sub_dir.encode_to_vec();
        let sub_digest = Digest::from_bytes(&sub_bytes);

        let root_dir = Directory {
            files: Vec::new(),
            directories: vec![DirectoryNode {
                name: "sub".to_owned(),
                digest: Some(to_proto(&sub_digest)?),
            }],
            symlinks: Vec::new(),
            node_properties: None,
        };
        let root_bytes = root_dir.encode_to_vec();
        let root_digest = Digest::from_bytes(&root_bytes);

        let mut map = HashMap::new();
        map.insert(sub_digest, sub_dir);
        map.insert(root_digest, root_dir);
        Ok((root_digest, map))
    }

    fn build_mixed_tree(
        plain: Digest,
        tool: Digest,
    ) -> TestResult<(Digest, HashMap<Digest, Directory>)> {
        let sub_dir = Directory {
            files: vec![FileNode {
                name: "tool.sh".to_owned(),
                digest: Some(to_proto(&tool)?),
                is_executable: true,
                node_properties: None,
            }],
            directories: Vec::new(),
            symlinks: vec![SymlinkNode {
                name: "tool-link".to_owned(),
                target: "tool.sh".to_owned(),
                node_properties: None,
            }],
            node_properties: None,
        };
        let sub_digest = Digest::from_bytes(&sub_dir.encode_to_vec());
        let root_dir = Directory {
            files: vec![
                FileNode {
                    name: "lib.dat".to_owned(),
                    digest: Some(to_proto(&plain)?),
                    is_executable: false,
                    node_properties: None,
                },
                FileNode {
                    name: "empty.txt".to_owned(),
                    digest: Some(to_proto(&Digest::EMPTY)?),
                    is_executable: false,
                    node_properties: None,
                },
            ],
            directories: vec![DirectoryNode {
                name: "bin".to_owned(),
                digest: Some(to_proto(&sub_digest)?),
            }],
            symlinks: Vec::new(),
            node_properties: None,
        };
        let root_digest = Digest::from_bytes(&root_dir.encode_to_vec());
        let mut dirs = HashMap::new();
        dirs.insert(sub_digest, sub_dir);
        dirs.insert(root_digest, root_dir);
        Ok((root_digest, dirs))
    }

    #[tokio::test]
    async fn publication_rejects_existing_non_directory_destination() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024).await?;
        let trees = dir.path().join("trees");
        fs::create_dir_all(&trees)?;
        let destination = trees.join("canonical");
        fs::write(&destination, b"not a directory")?;
        let plan = super::TreePlan {
            dirs: Vec::new(),
            files: Vec::new(),
            symlinks: Vec::new(),
            total_bytes: 0,
        };
        assert!(
            super::build_and_publish_tree(
                &trees,
                &destination,
                &cas,
                &plan,
                &std::sync::atomic::AtomicBool::new(false),
            )
            .is_err(),
            "an unexpected rename failure must not acknowledge publication"
        );
        assert_eq!(fs::read(destination)?, b"not a directory");
        Ok(())
    }

    #[tokio::test]
    async fn failed_eviction_detach_preserves_leaseable_tree() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024).await?;
        let file = stage_blob(&cas, b"resident", false).await?;
        let (root, directories) = build_single_file_tree("file", file, false)?;
        let graveyard = dir.path().join("graveyard");
        let cache = TreeCache::open(
            dir.path().join("trees"),
            Some(graveyard.clone()),
            cas,
            TreeCacheConfig {
                min_files: 1,
                max_trees: 1,
                tree_pin_percent: Percent::from_percent(100),
            },
        )
        .await?;
        drop(
            cache
                .get_or_materialize(root, &directories)
                .await?
                .ok_or("tree")?,
        );
        fs::write(&graveyard, b"obstructed tombstone parent")?;
        assert!(super::evict_one_unleased(&cache.inner).await.is_err());
        let lease = cache
            .get_or_materialize(root, &directories)
            .await?
            .ok_or("failed eviction must retain leaseable entry")?;
        assert_eq!(fs::read(lease.path().join("sub/file"))?, b"resident");
        Ok(())
    }

    #[tokio::test]
    async fn materializes_immutable_hardlinked_tree_with_normalized_metadata() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024 * 1024).await?;
        let graveyard = dir.path().join("graveyard");
        let trees_root = dir.path().join("trees");

        let plain = stage_blob(&cas, b"plain-library-bytes", false).await?;
        let tool = stage_blob(&cas, b"#!/bin/sh\necho ok\n", true).await?;
        let (root_digest, dirs) = build_mixed_tree(plain, tool)?;

        let cache = TreeCache::open(
            &trees_root,
            Some(graveyard),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                ..TreeCacheConfig::default()
            },
        )
        .await?;

        let lease = cache
            .get_or_materialize(root_digest, &dirs)
            .await?
            .ok_or("tree should be admitted")?;

        let expected_path =
            trees_root.join(format!("{}-{}", root_digest.hex(), root_digest.size()));
        assert_eq!(lease.path(), expected_path);
        assert_eq!(lease.file_count(), 3);
        assert_eq!(lease.total_bytes(), plain.size() + tool.size());
        assert_eq!(cache.lease_count(root_digest), 1);
        assert_eq!(cas.tree_pinned_bytes(), plain.size() + tool.size());

        // No manifest file exists in `<trees_root>` or the tree directory.
        let root_entries: Vec<_> = fs::read_dir(&trees_root)?
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(!root_entries.iter().any(|name| name.contains("manifest")));

        // Files share inodes with DiskCas and carry 0444 / 0555 + deterministic mtime.
        let lib_meta = fs::metadata(lease.path().join("lib.dat"))?;
        let cas_lib_meta =
            fs::metadata(blob_path(cas.root(), CacheKey::new(plain, BlobMode::Plain)))?;
        assert_eq!(lib_meta.ino(), cas_lib_meta.ino());
        assert_eq!(lib_meta.permissions().mode() & 0o777, 0o444);
        assert_eq!(
            u64::try_from(lib_meta.mtime()).unwrap_or(0),
            DETERMINISTIC_MTIME_SECS
        );

        let tool_meta = fs::metadata(lease.path().join("bin/tool.sh"))?;
        let cas_tool_meta = fs::metadata(blob_path(
            cas.root(),
            CacheKey::new(tool, BlobMode::Executable),
        ))?;
        assert_eq!(tool_meta.ino(), cas_tool_meta.ino());
        assert_eq!(tool_meta.permissions().mode() & 0o777, 0o555);

        // Directories carry 0555 + deterministic mtime.
        let root_meta = fs::metadata(lease.path())?;
        let bin_meta = fs::metadata(lease.path().join("bin"))?;
        assert_eq!(root_meta.permissions().mode() & 0o777, 0o555);
        assert_eq!(bin_meta.permissions().mode() & 0o777, 0o555);
        assert_eq!(
            u64::try_from(bin_meta.mtime()).unwrap_or(0),
            DETERMINISTIC_MTIME_SECS
        );

        // Second lease reuses existing tree and increments lease count.
        let lease2 = cache
            .get_or_materialize(root_digest, &dirs)
            .await?
            .ok_or("warm hit")?;
        assert_eq!(cache.lease_count(root_digest), 2);
        drop(lease);
        assert_eq!(cache.lease_count(root_digest), 1);
        drop(lease2);
        assert_eq!(cache.lease_count(root_digest), 0);
        Ok(())
    }

    #[tokio::test]
    async fn below_min_files_and_leased_cap_return_none_without_evicting_leased() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024 * 1024).await?;
        let graveyard = dir.path().join("graveyard");
        let trees_root = dir.path().join("trees");

        let d1 = stage_blob(&cas, b"tree-one-blob-aaaa", false).await?;
        let d2 = stage_blob(&cas, b"tree-two-blob-bbbb", false).await?;
        let (t1, dirs1) = build_single_file_tree("a.txt", d1, false)?;
        let (t2, dirs2) = build_single_file_tree("b.txt", d2, false)?;

        // Default min_files (512) rejects small tree.
        let strict_cache = TreeCache::open(
            &trees_root,
            Some(graveyard.clone()),
            cas.clone(),
            TreeCacheConfig::default(),
        )
        .await?;
        assert!(strict_cache.get_or_materialize(t1, &dirs1).await?.is_none());

        // With `max_trees = 1`, holding a lease on `t1` prevents `t2` from evicting `t1`.
        let cache = TreeCache::open(
            &trees_root,
            Some(graveyard),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                max_trees: 1,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;

        let lease1 = cache
            .get_or_materialize(t1, &dirs1)
            .await?
            .ok_or("admit t1")?;
        assert!(cache.get_or_materialize(t2, &dirs2).await?.is_none());
        assert!(cache.contains(t1));
        assert!(!cache.contains(t2));

        // Dropping `lease1` allows `t2` to evict `t1`.
        drop(lease1);
        let lease2 = cache
            .get_or_materialize(t2, &dirs2)
            .await?
            .ok_or("admit t2 after lease drop")?;
        assert!(!cache.contains(t1));
        assert!(cache.contains(t2));
        assert_eq!(lease2.digest(), t2);
        Ok(())
    }

    #[tokio::test]
    async fn unleased_eviction_uses_unique_graveyard_names_and_retains_cas_pins_until_unlink()
    -> TestResult {
        let dir = TempDir::new()?;
        let payload1 = b"eviction-payload-1111111111";
        let payload2 = b"eviction-payload-2222222222";
        // Budget 60 bytes -> 50% tree pin cap = 30 bytes (fits 1 tree, not 2 simultaneously).
        let cas = DiskCas::open(dir.path().join("cas"), 60).await?;
        let d1 = stage_blob(&cas, payload1, false).await?;
        let d2 = stage_blob(&cas, payload2, false).await?;
        let (t1, dirs1) = build_single_file_tree("f1", d1, false)?;
        let (t2, dirs2) = build_single_file_tree("f2", d2, false)?;

        let evicted_queue: Arc<Mutex<Vec<EvictedTree>>> = Arc::new(Mutex::new(Vec::new()));
        let trees_root = dir.path().join("trees");
        let graveyard = dir.path().join("graveyard");

        let cache = TreeCache::open_with_evict_hook(
            &trees_root,
            Some(graveyard.clone()),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                max_trees: 1,
                tree_pin_percent: Percent::from_percent(100),
            },
            {
                let evicted_queue = Arc::clone(&evicted_queue);
                move |evicted| {
                    evicted_queue.lock().push(evicted);
                }
            },
        )
        .await?;

        // Admit t1, release lease, then admit t2 (evicts t1 to graveyard).
        let l1 = cache.get_or_materialize(t1, &dirs1).await?.ok_or("t1")?;
        drop(l1);
        let l2 = cache.get_or_materialize(t2, &dirs2).await?.ok_or("t2")?;
        drop(l2);

        // Re-admit t1 (evicts t2), release, then admit t2 again (evicts t1 a second time).
        let l1_again = cache
            .get_or_materialize(t1, &dirs1)
            .await?
            .ok_or("t1 again")?;
        drop(l1_again);
        let l2_again = cache
            .get_or_materialize(t2, &dirs2)
            .await?
            .ok_or("t2 again")?;

        let drained: Vec<EvictedTree> = std::mem::take(&mut *evicted_queue.lock());
        assert_eq!(drained.len(), 3);

        let mut seen_paths = std::collections::HashSet::new();
        for item in &drained {
            assert!(item.path().starts_with(&graveyard));
            let name = item
                .path()
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .ok_or("graveyard name")?;
            assert!(name.starts_with("tree-"));
            assert!(seen_paths.insert(item.path().to_path_buf()));
            assert_eq!(item.reclaimable_bytes(), 0);
            assert_eq!(item.cas_shared_bytes(), item.walk_bytes());
            assert!(item.path().exists());
        }

        // Pins for evicted trees remain active in DiskCas until graveyard unlink finishes.
        assert_eq!(cas.tree_pinned_bytes(), d1.size() + d2.size());

        for item in drained {
            let path = item.path().to_path_buf();
            item.unlink_and_release()?;
            assert!(!path.exists());
        }

        assert_eq!(fs::read(l2_again.path().join("sub/f2"))?, payload2);
        assert!(
            cache.get_or_materialize(t1, &dirs1).await?.is_none(),
            "replacement lease must still prevent eviction"
        );

        // Now only the currently resident tree `t2` retains its `CasPin`.
        assert_eq!(cas.tree_pinned_bytes(), d2.size());
        Ok(())
    }

    #[tokio::test]
    async fn tree_pin_percent_evicts_unleased_and_blocks_when_all_leased() -> TestResult {
        let dir = TempDir::new()?;
        let p1 = b"pin-cap-blob-1-aaaaaaaaaa";
        let p2 = b"pin-cap-blob-2-bbbbbbbbbb";
        let budget = 60_u64; // 50% = 30 bytes; each blob is 25 bytes.
        let cas = DiskCas::open(dir.path().join("cas"), budget).await?;
        let d1 = stage_blob(&cas, p1, false).await?;
        let d2 = stage_blob(&cas, p2, false).await?;
        let (t1, dirs1) = build_single_file_tree("f1", d1, false)?;
        let (t2, dirs2) = build_single_file_tree("f2", d2, false)?;

        let cache = TreeCache::open(
            dir.path().join("trees"),
            Some(dir.path().join("graveyard")),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                max_trees: 16,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;

        let l1 = cache.get_or_materialize(t1, &dirs1).await?.ok_or("t1")?;
        // While l1 is leased, t2 exceeds `tree_pin_percent` (50 > 30) and cannot evict l1 -> None.
        assert!(cache.get_or_materialize(t2, &dirs2).await?.is_none());

        // Drop l1 -> t2 evicts t1, waits for unlink to release t1's pin, and succeeds.
        drop(l1);
        let l2 = cache
            .get_or_materialize(t2, &dirs2)
            .await?
            .ok_or("t2 succeeds after l1 unleased")?;
        assert_eq!(lease_bytes(&l2), d2.size());
        assert_eq!(cas.tree_pinned_bytes(), d2.size());
        Ok(())
    }

    const fn lease_bytes(lease: &super::TreeLease) -> u64 {
        lease.total_bytes()
    }

    #[tokio::test]
    async fn open_moves_stale_remnants_into_graveyard_and_cleans_up() -> TestResult {
        let dir = TempDir::new()?;
        let cas = DiskCas::open(dir.path().join("cas"), 1024 * 1024).await?;
        let trees_root = dir.path().join("trees");
        let graveyard = dir.path().join("graveyard");

        // Plant a stale read-only `0555` tree remnant and a leftover `.tmp` directory.
        let stale_tree = trees_root.join("deadbeef-123");
        fs::create_dir_all(stale_tree.join("nested"))?;
        fs::write(stale_tree.join("nested/file"), b"stale")?;
        fs::set_permissions(stale_tree.join("nested"), fs::Permissions::from_mode(0o555))?;
        fs::set_permissions(&stale_tree, fs::Permissions::from_mode(0o555))?;
        fs::create_dir_all(trees_root.join(".tmp/half-built"))?;
        fs::write(trees_root.join(".tmp/half-built/f"), b"partial")?;

        let moved = Arc::new(Mutex::new(Vec::new()));
        let _cache = TreeCache::open_with_evict_hook(
            &trees_root,
            Some(graveyard.clone()),
            cas.clone(),
            TreeCacheConfig {
                min_files: 1,
                ..TreeCacheConfig::default()
            },
            {
                let moved = Arc::clone(&moved);
                move |evicted| moved.lock().push(evicted)
            },
        )
        .await?;

        assert!(!stale_tree.exists());
        assert!(!trees_root.join(".tmp/half-built").exists());
        let remnants = std::mem::take(&mut *moved.lock());
        assert_eq!(remnants.len(), 1);
        assert!(remnants[0].path().starts_with(&graveyard));
        assert!(remnants[0].path().exists());
        for item in remnants {
            item.unlink_and_release()?;
        }

        // Re-opening without an evict hook removes remnants completely.
        fs::create_dir_all(trees_root.join("another-stale"))?;
        let _cache2 = TreeCache::open(
            &trees_root,
            Some(graveyard.clone()),
            cas,
            TreeCacheConfig::default(),
        )
        .await?;
        assert!(!trees_root.join("another-stale").exists());
        assert_eq!(fs::read_dir(&graveyard)?.count(), 0);
        Ok(())
    }
}
