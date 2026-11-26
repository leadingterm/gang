//! Input staging pipeline for native worker attempts (`WP E2`).
//!
//! Resolves `Action`, `Command`, and the REAPI `Directory` Merkle tree (via controller `GetTree`
//! or breadth-first `Tiered` reads through a byte-bounded LRU [`DirectoryCache`]), selects up to
//! [`MAX_RO_BINDS`] eligible non-overlapping read-only input subtrees from [`TreeCache`], fetches
//! remaining input blobs with bounded concurrency, materializes files via
//! [`DiskCas::hardlink_or_reflink_to`] (or [`Tiered::fetch_direct_to`] on bypass), creates
//! `<work>/<attempt_id>/work/<cwd>` and `<work>/<attempt_id>/tmp`, and retains [`TreeLease`] and
//! [`CasPin`] guards inside [`StagedAttempt`] until execution completes.

use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt as _;
use gang_cas::{
    BlobMode, BlockingScope, CacheKey, CasPin, DiskCas, FetchOutcome, Tiered, TreeCache, TreeLease,
};
use gang_config::WorkerConfig;
use gang_reapi::build::bazel::remote::execution::v2 as reapi;
use gang_reapi::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use gang_reapi::from_proto;
use gang_reapi::leadingterm::gang::worker::v1 as worker_proto;
use gang_spi::{ByteRange, ContentStore, MAX_RO_BINDS, PortError};
use gang_types::{AttemptId, AttemptIdError, Digest, NonZeroBytes};
use parking_lot::Mutex;
use prost::Message as _;
use tonic::transport::Channel;

/// Maximum REAPI directory tree nesting depth accepted during staging.
const MAX_TREE_DEPTH: usize = 256;

/// Default concurrent blob downloads / directory fetches.
const DEFAULT_DOWNLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Default minimum recursive file count for read-only tree binding.
const DEFAULT_TREE_CACHE_MIN_FILES: NonZeroUsize = NonZeroUsize::new(512).expect("512 is non-zero");

/// Default byte budget for the decoded REAPI `Directory` LRU cache.
const DEFAULT_DIRECTORY_CACHE_BUDGET: NonZeroBytes = NonZeroBytes::mib(64);

/// Reserved worker subdirectories that must never be targeted by attempt or action paths.
const RESERVED_WORK_DIRS: [&str; 2] = [".graveyard", ".side_channels"];

/// Configuration governing [`stage_attempt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageConfig {
    /// Maximum concurrent blob downloads and directory fetches.
    pub download_concurrency: NonZeroUsize,
    /// Minimum recursive file count for an input subtree to be bound via [`TreeCache`].
    pub tree_cache_min_files: NonZeroUsize,
    /// Byte budget for decoded REAPI `Directory` entries in [`DirectoryCache`].
    pub directory_cache_budget: NonZeroBytes,
}

impl Default for StageConfig {
    fn default() -> Self {
        Self {
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
            tree_cache_min_files: DEFAULT_TREE_CACHE_MIN_FILES,
            directory_cache_budget: DEFAULT_DIRECTORY_CACHE_BUDGET,
        }
    }
}

impl StageConfig {
    /// Builds a [`StageConfig`] from [`WorkerConfig`].
    #[must_use]
    pub const fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            download_concurrency: config.download_concurrency,
            tree_cache_min_files: config.tree_cache_min_files,
            directory_cache_budget: config.directory_cache_budget,
        }
    }
}

/// Optional controller `ContentAddressableStorage` endpoint for `GetTree` resolution.
#[derive(Debug, Clone)]
pub struct RemoteTreeSource {
    /// Connected `gRPC` channel to the controller's worker listener.
    pub channel: Channel,
    /// REAPI `instance_name` sent on `GetTreeRequest`.
    pub instance_name: String,
}

/// A read-only input subtree bound from [`TreeCache`] into an attempt's `work` directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTree {
    /// Absolute host path to the immutable directory in [`TreeCache`].
    pub source: PathBuf,
    /// Relative target path under `<attempt_dir>` (for example `work/external/toolchain`).
    pub relative_target: PathBuf,
    /// Absolute target mountpoint path `<attempt_dir>/work/<rel_tree>`.
    pub target: PathBuf,
    /// Root `Directory` digest of the bound subtree.
    pub digest: Digest,
}

/// Materialized workspace, retained leases, and CAS pins for one action attempt.
///
/// Holds [`TreeLease`] and [`CasPin`] guards across execution so bound trees and staged blobs
/// cannot be evicted before `Executed`. Call [`Self::release_for_upload`] once action execution
/// finishes (or drop `StagedAttempt` on failure/cancellation).
#[derive(Debug)]
pub struct StagedAttempt {
    attempt_id: AttemptId,
    attempt_dir: PathBuf,
    work_dir: PathBuf,
    working_dir: PathBuf,
    tmp_dir: PathBuf,
    action: reapi::Action,
    command: reapi::Command,
    read_only_trees: Vec<BoundTree>,
    tree_leases: Vec<TreeLease>,
    blob_pins: Vec<CasPin>,
}

impl StagedAttempt {
    /// Returns the attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// Returns `<work>/<attempt_id>`.
    #[must_use]
    pub fn attempt_dir(&self) -> &Path {
        &self.attempt_dir
    }

    /// Returns `<work>/<attempt_id>/work` (the action input/output root).
    #[must_use]
    pub fn work_dir(&self) -> &Path {
        &self.work_dir
    }

    /// Returns `<work>/<attempt_id>/work/<working_directory>`.
    #[must_use]
    pub fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    /// Returns `<work>/<attempt_id>/tmp` (the action's private `TMPDIR`).
    #[must_use]
    pub fn tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    /// Returns the decoded REAPI `Action`.
    #[must_use]
    pub const fn action(&self) -> &reapi::Action {
        &self.action
    }

    /// Returns the decoded REAPI `Command`.
    #[must_use]
    pub const fn command(&self) -> &reapi::Command {
        &self.command
    }

    /// Returns the read-only input trees to bind-mount inside the sandbox.
    #[must_use]
    pub fn read_only_trees(&self) -> &[BoundTree] {
        &self.read_only_trees
    }

    /// Returns the number of active [`TreeLease`] guards held by this attempt.
    #[must_use]
    pub fn leased_tree_count(&self) -> usize {
        self.tree_leases.len()
    }

    /// Returns the number of active [`CasPin`] guards held by this attempt.
    #[must_use]
    pub fn pin_count(&self) -> usize {
        self.blob_pins.len()
    }

    /// Releases all retained [`TreeLease`] and [`CasPin`] guards after `Executed` so caches can
    /// reclaim space before output upload completes.
    pub fn release_for_upload(&mut self) {
        self.tree_leases.clear();
        self.blob_pins.clear();
    }
}

/// Errors produced while staging an attempt's inputs.
#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    /// The assigned attempt identifier is invalid or unsafe as a directory name.
    #[error("invalid attempt id: {0}")]
    InvalidAttemptId(#[from] AttemptIdError),
    /// A required field was missing from `Assign`, `Action`, or `Directory`.
    #[error("missing required field `{field}`")]
    MissingField {
        /// Name of the missing field.
        field: &'static str,
    },
    /// A REAPI digest failed validation.
    #[error("invalid digest in `{field}`: {source}")]
    InvalidDigest {
        /// Field carrying the invalid digest.
        field: &'static str,
        /// Underlying digest conversion error.
        #[source]
        source: gang_reapi::DigestConversionError,
    },
    /// A protobuf message failed to decode.
    #[error("malformed {kind} protobuf: {source}")]
    MalformedProto {
        /// Protobuf message kind (`Action`, `Command`, or `Directory`).
        kind: &'static str,
        /// Underlying prost decode error.
        #[source]
        source: prost::DecodeError,
    },
    /// A path in the command or directory tree attempted traversal or used a reserved name.
    #[error("invalid path `{path}`: {reason}")]
    InvalidPath {
        /// The rejected path text.
        path: String,
        /// Explanation of why the path was rejected.
        reason: &'static str,
    },
    /// The REAPI directory Merkle graph was invalid (missing child, cycle, depth, duplicate).
    #[error("invalid REAPI directory tree: {reason}")]
    InvalidTree {
        /// Explanation of the tree validation failure.
        reason: String,
    },
    /// A required CAS object was missing from both fast and slow stores.
    #[error("missing {kind} blob {digest}")]
    MissingBlob {
        /// Kind of missing object (`Action`, `Command`, `Directory`, `input file`).
        kind: &'static str,
        /// Missing blob digest.
        digest: Digest,
    },
    /// A filesystem operation failed while materializing the attempt directory.
    #[error("filesystem error at `{}`: {source}", path.display())]
    Io {
        /// Path associated with the I/O failure.
        path: PathBuf,
        /// Underlying OS error.
        #[source]
        source: io::Error,
    },
    /// A store or transport operation failed.
    #[error("store failure: {0}")]
    Store(#[from] PortError),
    /// Staging was cancelled cooperatively before completing.
    #[error("input staging cancelled")]
    Cancelled,
}

#[derive(Debug)]
struct CachedDir {
    dir: Arc<reapi::Directory>,
    bytes: u64,
    stamp: u64,
}

#[derive(Debug, Default)]
struct DirectoryCacheInner {
    budget: u64,
    used: u64,
    clock: u64,
    entries: HashMap<Digest, CachedDir>,
    lru: BTreeSet<(u64, Digest)>,
}

/// Byte-bounded thread-safe LRU cache for decoded REAPI `Directory` messages.
#[derive(Debug, Clone)]
pub struct DirectoryCache {
    inner: Arc<Mutex<DirectoryCacheInner>>,
}

impl DirectoryCache {
    /// Creates an empty [`DirectoryCache`] bounded to `budget` bytes.
    #[must_use]
    pub fn new(budget: NonZeroBytes) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DirectoryCacheInner {
                budget: budget.get().get(),
                ..DirectoryCacheInner::default()
            })),
        }
    }

    /// Looks up `digest`, promoting it to most-recently-used on hit.
    #[must_use]
    pub fn get(&self, digest: &Digest) -> Option<Arc<reapi::Directory>> {
        let mut inner = self.inner.lock();
        let new_stamp = inner.clock.saturating_add(1);
        let (old_stamp, dir) = {
            let entry = inner.entries.get_mut(digest)?;
            let old = entry.stamp;
            entry.stamp = new_stamp;
            (old, Arc::clone(&entry.dir))
        };
        inner.clock = new_stamp;
        inner.lru.remove(&(old_stamp, *digest));
        inner.lru.insert((new_stamp, *digest));
        Some(dir)
    }

    /// Inserts `dir` under `digest`, evicting least-recently-used entries to stay within budget.
    pub fn insert(&self, digest: Digest, dir: Arc<reapi::Directory>) {
        let byte_cost = estimate_directory_bytes(&digest, &dir);
        let mut inner = self.inner.lock();
        if byte_cost > inner.budget {
            return;
        }
        let new_stamp = inner.clock.saturating_add(1);
        if let Some(existing) = inner.entries.get_mut(&digest) {
            let old_stamp = existing.stamp;
            existing.stamp = new_stamp;
            existing.dir = dir;
            inner.clock = new_stamp;
            inner.lru.remove(&(old_stamp, digest));
            inner.lru.insert((new_stamp, digest));
            return;
        }

        while inner.used.saturating_add(byte_cost) > inner.budget {
            let Some(oldest_key) = inner.lru.pop_first() else {
                break;
            };
            if let Some(removed) = inner.entries.remove(&oldest_key.1) {
                inner.used = inner.used.saturating_sub(removed.bytes);
            }
        }

        if inner.used.saturating_add(byte_cost) <= inner.budget {
            inner.clock = inner.clock.saturating_add(1);
            let stamp = inner.clock;
            inner.used = inner.used.saturating_add(byte_cost);
            inner.entries.insert(
                digest,
                CachedDir {
                    dir,
                    bytes: byte_cost,
                    stamp,
                },
            );
            inner.lru.insert((stamp, digest));
        }
    }

    /// Returns `true` if `digest` is currently cached without updating recency.
    #[must_use]
    pub fn contains(&self, digest: &Digest) -> bool {
        self.inner.lock().entries.contains_key(digest)
    }

    /// Returns the number of directories currently cached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Returns `true` when no directories are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the currently tracked byte occupancy.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.inner.lock().used
    }

    /// Returns the configured byte budget.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.inner.lock().budget
    }
}

fn estimate_directory_bytes(digest: &Digest, dir: &reapi::Directory) -> u64 {
    let encoded = u64::try_from(dir.encoded_len()).unwrap_or(u64::MAX);
    digest.size().max(encoded).max(1)
}

struct AttemptDirGuard {
    path: PathBuf,
    armed: bool,
}

impl AttemptDirGuard {
    const fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for AttemptDirGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = gang_os::fs::remove_dir_optional(&self.path);
        }
    }
}

#[derive(Debug, Clone)]
struct TreeSubtree {
    rel_path: PathBuf,
    digest: Digest,
    file_count: usize,
}

#[derive(Debug, Clone)]
struct InputFile {
    rel_path: PathBuf,
    digest: Digest,
    executable: bool,
}

#[derive(Debug, Clone)]
struct InputSymlink {
    rel_path: PathBuf,
    target: String,
}

#[derive(Debug, Default)]
struct TreeInventory {
    subtrees: Vec<TreeSubtree>,
    dirs: Vec<PathBuf>,
    files: Vec<InputFile>,
    symlinks: Vec<InputSymlink>,
}

/// Stages an assigned action attempt into `<work_root>/<attempt_id>`.
///
/// # Errors
///
/// Returns [`StagingError`] if `assign`, `Action`, `Command`, or the REAPI `Directory` tree is
/// malformed, any path attempts traversal or targets a reserved directory, a required CAS object is
/// missing, or filesystem/store operations fail.
pub async fn stage_attempt<F, S>(
    assign: &worker_proto::Assign,
    work_root: &Path,
    tiered: &Tiered<F, S>,
    tree_cache: Option<&TreeCache>,
    dir_cache: &DirectoryCache,
    remote_tree: Option<&RemoteTreeSource>,
    config: &StageConfig,
) -> Result<StagedAttempt, StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    stage_attempt_with_scope(
        assign,
        work_root,
        tiered,
        tree_cache,
        dir_cache,
        remote_tree,
        config,
        None,
        true,
        #[cfg(test)]
        None,
    )
    .await
}

/// Worker-owned staging entry point: tracks blocking materialization in `blocking` and leaves
/// `<work_root>/<attempt_id>` retirement solely to the worker's graveyard after blocking tasks join.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage_attempt_in_worker<F, S>(
    assign: &worker_proto::Assign,
    work_root: &Path,
    tiered: &Tiered<F, S>,
    tree_cache: Option<&TreeCache>,
    dir_cache: &DirectoryCache,
    remote_tree: Option<&RemoteTreeSource>,
    config: &StageConfig,
    blocking: &BlockingScope,
) -> Result<StagedAttempt, StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    stage_attempt_with_scope(
        assign,
        work_root,
        tiered,
        tree_cache,
        dir_cache,
        remote_tree,
        config,
        Some(blocking),
        false,
        #[cfg(test)]
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn stage_attempt_with_scope<F, S>(
    assign: &worker_proto::Assign,
    work_root: &Path,
    tiered: &Tiered<F, S>,
    tree_cache: Option<&TreeCache>,
    dir_cache: &DirectoryCache,
    remote_tree: Option<&RemoteTreeSource>,
    config: &StageConfig,
    blocking: Option<&BlockingScope>,
    cleanup_dir_on_drop: bool,
    #[cfg(test)] before_materialize: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<StagedAttempt, StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let attempt_id = AttemptId::try_from(assign.attempt.as_str())?;
    let action = resolve_action(assign, tiered).await?;
    let command = resolve_command(assign, &action, tiered).await?;

    let cwd_rel = validate_relative_path(&command.working_directory, true)?;
    let output_paths_rel = collect_and_validate_outputs(&command, &cwd_rel)?;

    let (dir_map, root_digest) = resolve_input_tree(
        action.input_root_digest.as_ref(),
        tiered,
        dir_cache,
        remote_tree,
        config.download_concurrency,
    )
    .await?;

    let inventory = build_inventory(root_digest, &dir_map)?;

    let attempt_dir = work_root.join(attempt_id.as_str());
    let work_dir = attempt_dir.join("work");
    let working_dir = if cwd_rel.as_os_str().is_empty() {
        work_dir.clone()
    } else {
        work_dir.join(&cwd_rel)
    };
    let tmp_dir = attempt_dir.join("tmp");

    let guard = cleanup_dir_on_drop.then(|| AttemptDirGuard::new(attempt_dir.clone()));

    let (read_only_trees, tree_leases, bound_prefixes) = bind_read_only_trees(
        &inventory,
        &dir_map,
        &work_dir,
        &cwd_rel,
        &output_paths_rel,
        tree_cache,
        tiered,
        config.tree_cache_min_files.get(),
    )
    .await?;

    let (unbound_files, unbound_dirs, unbound_symlinks) =
        partition_unbound(inventory, &bound_prefixes);

    let (blob_pins, bypassed_keys) =
        fetch_unbound_blobs(&unbound_files, tiered, config.download_concurrency).await?;

    let output_parents = output_parent_dirs(&working_dir, &command)?;
    let mount_points: Vec<PathBuf> = read_only_trees
        .iter()
        .map(|bound| bound.target.clone())
        .collect();

    materialize_workspace(
        &work_dir,
        &working_dir,
        &tmp_dir,
        &unbound_dirs,
        &mount_points,
        &output_parents,
        &unbound_files,
        &unbound_symlinks,
        &bypassed_keys,
        tiered.disk(),
        blocking,
        #[cfg(test)]
        before_materialize,
    )
    .await?;

    if blocking.is_some_and(BlockingScope::is_cancelled) {
        return Err(StagingError::Cancelled);
    }

    stream_bypassed_files(
        &unbound_files,
        &bypassed_keys,
        &work_dir,
        tiered,
        config.download_concurrency,
        blocking,
    )
    .await?;

    if let Some(guard) = guard {
        guard.disarm();
    }

    Ok(StagedAttempt {
        attempt_id,
        attempt_dir,
        work_dir,
        working_dir,
        tmp_dir,
        action,
        command,
        read_only_trees,
        tree_leases,
        blob_pins,
    })
}

#[allow(clippy::too_many_arguments)]
async fn bind_read_only_trees<F, S>(
    inventory: &TreeInventory,
    dir_map: &HashMap<Digest, reapi::Directory>,
    work_dir: &Path,
    cwd_rel: &Path,
    output_paths_rel: &[PathBuf],
    tree_cache: Option<&TreeCache>,
    tiered: &Tiered<F, S>,
    min_files: usize,
) -> Result<(Vec<BoundTree>, Vec<TreeLease>, Vec<PathBuf>), StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let Some(cache) = tree_cache else {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    };
    let candidates = select_read_only_candidates(inventory, cwd_rel, output_paths_rel, min_files);
    let mut read_only_trees = Vec::new();
    let mut tree_leases = Vec::new();
    let mut bound_prefixes = Vec::new();
    for candidate in candidates {
        if let Some(lease) = cache
            .get_or_materialize_from(candidate.digest, dir_map, tiered)
            .await?
        {
            let relative_target = Path::new("work").join(&candidate.rel_path);
            let target = work_dir.join(&candidate.rel_path);
            read_only_trees.push(BoundTree {
                source: lease.path().to_path_buf(),
                relative_target,
                target,
                digest: candidate.digest,
            });
            tree_leases.push(lease);
            bound_prefixes.push(candidate.rel_path);
        }
    }
    Ok((read_only_trees, tree_leases, bound_prefixes))
}

fn partition_unbound(
    inventory: TreeInventory,
    bound_prefixes: &[PathBuf],
) -> (Vec<InputFile>, Vec<PathBuf>, Vec<InputSymlink>) {
    let files = inventory
        .files
        .into_iter()
        .filter(|file| !is_under_any(&file.rel_path, bound_prefixes))
        .collect();
    let dirs = inventory
        .dirs
        .into_iter()
        .filter(|dir| !is_under_any(dir, bound_prefixes))
        .collect();
    let symlinks = inventory
        .symlinks
        .into_iter()
        .filter(|sym| !is_under_any(&sym.rel_path, bound_prefixes))
        .collect();
    (files, dirs, symlinks)
}

async fn resolve_action<F, S>(
    assign: &worker_proto::Assign,
    tiered: &Tiered<F, S>,
) -> Result<reapi::Action, StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    if let Some(bytes) = &assign.action {
        return reapi::Action::decode(bytes.as_ref()).map_err(|source| {
            StagingError::MalformedProto {
                kind: "Action",
                source,
            }
        });
    }
    let proto_digest = assign
        .action_digest
        .as_ref()
        .ok_or(StagingError::MissingField {
            field: "action_digest",
        })?;
    let digest = from_proto(proto_digest).map_err(|source| StagingError::InvalidDigest {
        field: "action_digest",
        source,
    })?;
    let stream = tiered
        .read(digest, ByteRange::FULL)
        .await?
        .ok_or(StagingError::MissingBlob {
            kind: "Action",
            digest,
        })?;
    let bytes = stream.collect().await?;
    reapi::Action::decode(bytes).map_err(|source| StagingError::MalformedProto {
        kind: "Action",
        source,
    })
}

async fn resolve_command<F, S>(
    assign: &worker_proto::Assign,
    action: &reapi::Action,
    tiered: &Tiered<F, S>,
) -> Result<reapi::Command, StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    if let Some(bytes) = &assign.command {
        return reapi::Command::decode(bytes.as_ref()).map_err(|source| {
            StagingError::MalformedProto {
                kind: "Command",
                source,
            }
        });
    }
    let proto_digest = action
        .command_digest
        .as_ref()
        .ok_or(StagingError::MissingField {
            field: "command_digest",
        })?;
    let digest = from_proto(proto_digest).map_err(|source| StagingError::InvalidDigest {
        field: "command_digest",
        source,
    })?;
    let stream = tiered
        .read(digest, ByteRange::FULL)
        .await?
        .ok_or(StagingError::MissingBlob {
            kind: "Command",
            digest,
        })?;
    let bytes = stream.collect().await?;
    reapi::Command::decode(bytes).map_err(|source| StagingError::MalformedProto {
        kind: "Command",
        source,
    })
}

async fn resolve_input_tree<F, S>(
    input_root_proto: Option<&reapi::Digest>,
    tiered: &Tiered<F, S>,
    dir_cache: &DirectoryCache,
    remote_tree: Option<&RemoteTreeSource>,
    concurrency: NonZeroUsize,
) -> Result<(HashMap<Digest, reapi::Directory>, Digest), StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let root_digest = match input_root_proto {
        Some(proto) => from_proto(proto).map_err(|source| StagingError::InvalidDigest {
            field: "input_root_digest",
            source,
        })?,
        None => Digest::EMPTY,
    };

    let mut dir_map = HashMap::new();
    if root_digest.is_empty() {
        dir_map.insert(Digest::EMPTY, reapi::Directory::default());
        return Ok((dir_map, Digest::EMPTY));
    }

    if !dir_cache.contains(&root_digest)
        && let Some(remote) = remote_tree
    {
        fetch_tree_via_grpc(remote, root_digest, dir_cache, &mut dir_map).await?;
    }

    let mut frontier = vec![root_digest];
    let mut queued = HashSet::new();
    queued.insert(root_digest);

    while !frontier.is_empty() {
        let mut to_fetch = Vec::new();
        for &digest in &frontier {
            if dir_map.contains_key(&digest) {
                continue;
            }
            if digest.is_empty() {
                dir_map.insert(Digest::EMPTY, reapi::Directory::default());
                continue;
            }
            if let Some(cached) = dir_cache.get(&digest) {
                dir_map.insert(digest, (*cached).clone());
            } else {
                to_fetch.push(digest);
            }
        }

        if !to_fetch.is_empty() {
            let fetched: Vec<Result<(Digest, reapi::Directory), StagingError>> =
                futures::stream::iter(to_fetch)
                    .map(|digest| async move {
                        let stream = tiered.read(digest, ByteRange::FULL).await?.ok_or(
                            StagingError::MissingBlob {
                                kind: "Directory",
                                digest,
                            },
                        )?;
                        let bytes = stream.collect().await?;
                        let dir = reapi::Directory::decode(bytes).map_err(|source| {
                            StagingError::MalformedProto {
                                kind: "Directory",
                                source,
                            }
                        })?;
                        Ok((digest, dir))
                    })
                    .buffer_unordered(concurrency.get())
                    .collect()
                    .await;

            for item in fetched {
                let (digest, dir) = item?;
                dir_cache.insert(digest, Arc::new(dir.clone()));
                dir_map.insert(digest, dir);
            }
        }

        let mut next_frontier = Vec::new();
        for digest in frontier {
            let dir = dir_map
                .get(&digest)
                .ok_or_else(|| StagingError::InvalidTree {
                    reason: format!("resolved directory {digest} missing from map"),
                })?;
            for child in &dir.directories {
                let child_proto = child
                    .digest
                    .as_ref()
                    .ok_or(StagingError::MissingField { field: "digest" })?;
                let child_digest =
                    from_proto(child_proto).map_err(|source| StagingError::InvalidDigest {
                        field: "directory.digest",
                        source,
                    })?;
                if queued.insert(child_digest) {
                    next_frontier.push(child_digest);
                }
            }
        }
        frontier = next_frontier;
    }

    Ok((dir_map, root_digest))
}

async fn fetch_tree_via_grpc(
    remote: &RemoteTreeSource,
    root_digest: Digest,
    dir_cache: &DirectoryCache,
    dir_map: &mut HashMap<Digest, reapi::Directory>,
) -> Result<(), StagingError> {
    let proto_digest = reapi::Digest {
        hash: root_digest.hex(),
        size_bytes: i64::try_from(root_digest.size()).map_err(|_| StagingError::InvalidTree {
            reason: format!("root digest size {} overflows i64", root_digest.size()),
        })?,
    };
    let mut client = ContentAddressableStorageClient::new(remote.channel.clone());
    let mut page_token = String::new();

    loop {
        let mut stream = client
            .get_tree(reapi::GetTreeRequest {
                instance_name: remote.instance_name.clone(),
                root_digest: Some(proto_digest.clone()),
                page_size: 0,
                page_token: page_token.clone(),
                digest_function: reapi::digest_function::Value::Sha256 as i32,
            })
            .await
            .map_err(|status| PortError::backend(format!("GetTree failed: {status}")))?
            .into_inner();

        let mut next_token = String::new();
        while let Some(resp) = stream
            .message()
            .await
            .map_err(|status| PortError::backend(format!("GetTree stream failed: {status}")))?
        {
            for dir in resp.directories {
                let encoded = dir.encode_to_vec();
                let digest = Digest::from_bytes(&encoded);
                dir_cache.insert(digest, Arc::new(dir.clone()));
                dir_map.insert(digest, dir);
            }
            next_token = resp.next_page_token;
        }
        if next_token.is_empty() {
            break;
        }
        page_token = next_token;
    }

    Ok(())
}

/// Expansion budgets apply to paths, including repeated occurrences of a shared Directory.
const MAX_EXPANDED_NODES: usize = 250_000;
const MAX_EXPANDED_PATH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
struct ExpansionSize {
    nodes: usize,
    path_bytes: usize,
    depth: usize,
}

fn expansion_error() -> StagingError {
    StagingError::InvalidTree {
        reason: "REAPI directory tree exceeds expanded node or path-byte budget".to_owned(),
    }
}

fn measure_expansion(
    digest: Digest,
    directories: &HashMap<Digest, reapi::Directory>,
    measured: &mut HashMap<Digest, ExpansionSize>,
    visiting: &mut HashSet<Digest>,
) -> Result<ExpansionSize, StagingError> {
    if let Some(size) = measured.get(&digest) {
        return Ok(*size);
    }
    if visiting.len() >= MAX_TREE_DEPTH || !visiting.insert(digest) {
        return Err(StagingError::InvalidTree {
            reason: "REAPI directory tree contains a cycle or exceeds maximum depth".to_owned(),
        });
    }
    let directory = directories
        .get(&digest)
        .ok_or_else(|| StagingError::InvalidTree {
            reason: format!("missing Directory {digest}"),
        })?;
    let mut size = ExpansionSize {
        nodes: 1,
        path_bytes: 0,
        depth: 1,
    };
    for name in directory
        .files
        .iter()
        .map(|file| file.name.as_str())
        .chain(directory.symlinks.iter().map(|link| link.name.as_str()))
    {
        size.nodes = size.nodes.checked_add(1).ok_or_else(expansion_error)?;
        size.path_bytes = size
            .path_bytes
            .checked_add(name.len())
            .ok_or_else(expansion_error)?;
        if size.nodes > MAX_EXPANDED_NODES || size.path_bytes > MAX_EXPANDED_PATH_BYTES {
            return Err(expansion_error());
        }
    }
    for child in &directory.directories {
        let proto = child
            .digest
            .as_ref()
            .ok_or(StagingError::MissingField { field: "digest" })?;
        let child_digest = from_proto(proto).map_err(|source| StagingError::InvalidDigest {
            field: "directory.digest",
            source,
        })?;
        let child_size = measure_expansion(child_digest, directories, measured, visiting)?;
        size.nodes = size
            .nodes
            .checked_add(child_size.nodes)
            .ok_or_else(expansion_error)?;
        // Every descendant receives "name/"; the child root itself has no trailing slash.
        let prefix_bytes = child
            .name
            .len()
            .checked_add(1)
            .and_then(|prefix| prefix.checked_mul(child_size.nodes))
            .and_then(|bytes| bytes.checked_sub(1))
            .ok_or_else(expansion_error)?;
        size.path_bytes = size
            .path_bytes
            .checked_add(child_size.path_bytes)
            .and_then(|bytes| bytes.checked_add(prefix_bytes))
            .ok_or_else(expansion_error)?;
        size.depth = size.depth.max(child_size.depth + 1);
        if size.nodes > MAX_EXPANDED_NODES || size.path_bytes > MAX_EXPANDED_PATH_BYTES {
            return Err(expansion_error());
        }
        if size.depth > MAX_TREE_DEPTH {
            return Err(StagingError::InvalidTree {
                reason: "REAPI directory tree exceeds maximum depth".to_owned(),
            });
        }
    }
    visiting.remove(&digest);
    measured.insert(digest, size);
    Ok(size)
}

fn build_inventory(
    root_digest: Digest,
    dir_map: &HashMap<Digest, reapi::Directory>,
) -> Result<TreeInventory, StagingError> {
    struct Frame<'a> {
        digest: Digest,
        rel_path: PathBuf,
        dir: &'a reapi::Directory,
        pending: VecDeque<(PathBuf, Digest)>,
        file_count: usize,
    }

    // Count the DAG with memoization before allocating any expanded paths. A diamond with
    // billions of occurrences is rejected in work proportional to its distinct directories.
    measure_expansion(
        root_digest,
        dir_map,
        &mut HashMap::new(),
        &mut HashSet::new(),
    )?;

    let root_dir = dir_map
        .get(&root_digest)
        .ok_or_else(|| StagingError::InvalidTree {
            reason: format!("missing root Directory {root_digest}"),
        })?;

    let mut inventory = TreeInventory::default();
    let mut visiting = HashSet::new();
    visiting.insert(root_digest);
    let mut stack = vec![Frame {
        digest: root_digest,
        rel_path: PathBuf::new(),
        dir: root_dir,
        pending: enter_subdirs(Path::new(""), root_dir)?,
        file_count: root_dir.files.len(),
    }];

    while let Some(top) = stack.last_mut() {
        if let Some((child_rel, child_digest)) = top.pending.pop_front() {
            if stack.len() >= MAX_TREE_DEPTH {
                return Err(StagingError::InvalidTree {
                    reason: "REAPI directory tree exceeds maximum depth".to_owned(),
                });
            }
            if !visiting.insert(child_digest) {
                return Err(StagingError::InvalidTree {
                    reason: format!("cycle detected at Directory {child_digest}"),
                });
            }
            let child_dir =
                dir_map
                    .get(&child_digest)
                    .ok_or_else(|| StagingError::InvalidTree {
                        reason: format!("missing child Directory {child_digest}"),
                    })?;
            inventory.dirs.push(child_rel.clone());
            let pending = enter_subdirs(&child_rel, child_dir)?;
            let file_count = child_dir.files.len();
            stack.push(Frame {
                digest: child_digest,
                rel_path: child_rel,
                dir: child_dir,
                pending,
                file_count,
            });
            continue;
        }

        let Some(finished) = stack.pop() else {
            break;
        };
        visiting.remove(&finished.digest);
        collect_directory_nodes(&finished.rel_path, finished.dir, &mut inventory)?;
        if !finished.rel_path.as_os_str().is_empty() {
            inventory.subtrees.push(TreeSubtree {
                rel_path: finished.rel_path,
                digest: finished.digest,
                file_count: finished.file_count,
            });
        }
        if let Some(parent) = stack.last_mut() {
            parent.file_count = parent.file_count.saturating_add(finished.file_count);
        }
    }

    inventory.dirs.sort_by_key(|path| path.components().count());
    Ok(inventory)
}

fn enter_subdirs(
    parent_rel: &Path,
    dir: &reapi::Directory,
) -> Result<VecDeque<(PathBuf, Digest)>, StagingError> {
    let mut pending = VecDeque::with_capacity(dir.directories.len());
    for child in &dir.directories {
        validate_node_name(&child.name)?;
        let proto_digest = child
            .digest
            .as_ref()
            .ok_or(StagingError::MissingField { field: "digest" })?;
        let digest = from_proto(proto_digest).map_err(|source| StagingError::InvalidDigest {
            field: "directory.digest",
            source,
        })?;
        pending.push_back((parent_rel.join(&child.name), digest));
    }
    Ok(pending)
}

fn collect_directory_nodes(
    dir_rel: &Path,
    dir: &reapi::Directory,
    inventory: &mut TreeInventory,
) -> Result<(), StagingError> {
    let mut seen = HashSet::new();
    for subdir in &dir.directories {
        validate_node_name(&subdir.name)?;
        if !seen.insert(subdir.name.as_str()) {
            return Err(StagingError::InvalidTree {
                reason: format!("duplicate entry `{}`", subdir.name),
            });
        }
    }
    for file in &dir.files {
        validate_node_name(&file.name)?;
        if !seen.insert(file.name.as_str()) {
            return Err(StagingError::InvalidTree {
                reason: format!("duplicate entry `{}`", file.name),
            });
        }
        let proto_digest = file
            .digest
            .as_ref()
            .ok_or(StagingError::MissingField { field: "digest" })?;
        let digest = from_proto(proto_digest).map_err(|source| StagingError::InvalidDigest {
            field: "file.digest",
            source,
        })?;
        inventory.files.push(InputFile {
            rel_path: dir_rel.join(&file.name),
            digest,
            executable: file.is_executable,
        });
    }
    for sym in &dir.symlinks {
        validate_node_name(&sym.name)?;
        if !seen.insert(sym.name.as_str()) {
            return Err(StagingError::InvalidTree {
                reason: format!("duplicate entry `{}`", sym.name),
            });
        }
        if sym.target.is_empty() || sym.target.contains('\0') {
            return Err(StagingError::InvalidPath {
                path: sym.target.clone(),
                reason: "symlink target is empty or contains NUL",
            });
        }
        inventory.symlinks.push(InputSymlink {
            rel_path: dir_rel.join(&sym.name),
            target: sym.target.clone(),
        });
    }
    Ok(())
}

fn validate_node_name(name: &str) -> Result<(), StagingError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(StagingError::InvalidPath {
            path: name.to_owned(),
            reason: "invalid REAPI directory entry name",
        });
    }
    if RESERVED_WORK_DIRS.contains(&name) {
        return Err(StagingError::InvalidPath {
            path: name.to_owned(),
            reason: "entry uses reserved worker directory name",
        });
    }
    Ok(())
}

fn validate_relative_path(raw: &str, allow_empty: bool) -> Result<PathBuf, StagingError> {
    if raw.is_empty() {
        if allow_empty {
            return Ok(PathBuf::new());
        }
        return Err(StagingError::InvalidPath {
            path: raw.to_owned(),
            reason: "path must not be empty",
        });
    }
    if raw.starts_with('/') || raw.ends_with('/') || raw.contains('\0') {
        return Err(StagingError::InvalidPath {
            path: raw.to_owned(),
            reason: "path must be relative without leading/trailing '/' or NUL",
        });
    }
    let mut out = PathBuf::new();
    for segment in raw.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(StagingError::InvalidPath {
                path: raw.to_owned(),
                reason: "path contains empty, `.`, or `..` segment",
            });
        }
        if RESERVED_WORK_DIRS.contains(&segment) {
            return Err(StagingError::InvalidPath {
                path: raw.to_owned(),
                reason: "path references reserved worker directory",
            });
        }
        out.push(segment);
    }
    Ok(out)
}

#[allow(deprecated)]
fn collect_and_validate_outputs(
    command: &reapi::Command,
    cwd_rel: &Path,
) -> Result<Vec<PathBuf>, StagingError> {
    let mut out_paths = Vec::new();
    for raw in &command.output_paths {
        let rel = validate_relative_path(raw, false)?;
        out_paths.push(join_rel(cwd_rel, &rel));
    }
    for raw in &command.output_files {
        let rel = validate_relative_path(raw, false)?;
        out_paths.push(join_rel(cwd_rel, &rel));
    }
    for raw in &command.output_directories {
        let rel = validate_relative_path(raw, true)?;
        out_paths.push(join_rel(cwd_rel, &rel));
    }
    Ok(out_paths)
}

fn join_rel(base: &Path, rel: &Path) -> PathBuf {
    if base.as_os_str().is_empty() {
        rel.to_path_buf()
    } else if rel.as_os_str().is_empty() {
        base.to_path_buf()
    } else {
        base.join(rel)
    }
}

fn select_read_only_candidates(
    inventory: &TreeInventory,
    cwd_rel: &Path,
    output_paths: &[PathBuf],
    min_files: usize,
) -> Vec<TreeSubtree> {
    let mut eligible: Vec<TreeSubtree> = inventory
        .subtrees
        .iter()
        .filter(|subtree| {
            subtree.file_count >= min_files
                && !subtree.rel_path.as_os_str().is_empty()
                // One-way check: `cwd` stays writable, while subtrees beneath `cwd` remain eligible.
                && !cwd_rel.starts_with(&subtree.rel_path)
                && !overlaps_any_output(&subtree.rel_path, output_paths)
                && !symlink_escapes_or_overlaps(
                    &subtree.rel_path,
                    &inventory.symlinks,
                    cwd_rel,
                    output_paths,
                )
        })
        .cloned()
        .collect();

    eligible.sort_by(|left, right| {
        Reverse(left.file_count)
            .cmp(&Reverse(right.file_count))
            .then_with(|| {
                left.rel_path
                    .components()
                    .count()
                    .cmp(&right.rel_path.components().count())
            })
            .then_with(|| left.rel_path.cmp(&right.rel_path))
    });

    let mut selected: Vec<TreeSubtree> = Vec::with_capacity(eligible.len().min(MAX_RO_BINDS));
    for candidate in eligible {
        if selected.len() >= MAX_RO_BINDS {
            break;
        }
        let overlaps_selected = selected.iter().any(|kept| {
            candidate.rel_path.starts_with(&kept.rel_path)
                || kept.rel_path.starts_with(&candidate.rel_path)
        });
        if !overlaps_selected {
            selected.push(candidate);
        }
    }
    selected
}

fn overlaps_any_output(tree: &Path, outputs: &[PathBuf]) -> bool {
    outputs
        .iter()
        .any(|out| tree.starts_with(out) || out.starts_with(tree))
}

fn symlink_escapes_or_overlaps(
    tree: &Path,
    symlinks: &[InputSymlink],
    cwd_rel: &Path,
    outputs: &[PathBuf],
) -> bool {
    for sym in symlinks {
        // An ancestor within the input root is a symlink, or cwd/output traverses through a symlink.
        if tree.starts_with(&sym.rel_path)
            || cwd_rel.starts_with(&sym.rel_path)
            || outputs.iter().any(|out| out.starts_with(&sym.rel_path))
        {
            return true;
        }

        let parent = sym.rel_path.parent().unwrap_or_else(|| Path::new(""));
        let resolved_in_root = resolve_relative_in_root(parent, &sym.target);

        if sym.rel_path.starts_with(tree) {
            // Symlink is inside the candidate subtree: reject if absolute or escapes `tree`.
            if sym.target.starts_with('/') {
                return true;
            }
            let Some(resolved) = resolved_in_root else {
                return true;
            };
            if !resolved.starts_with(tree) {
                return true;
            }
        } else if let Some(resolved) = resolved_in_root
            && (resolved.starts_with(tree) || tree.starts_with(&resolved))
        {
            // External symlink target resolves into or above the candidate subtree.
            return true;
        }
    }
    false
}

/// Resolves `target` relative to `parent` inside the input root, returning `None` if `target` is
/// absolute or climbs above the input root via `..`.
fn resolve_relative_in_root(parent: &Path, target: &str) -> Option<PathBuf> {
    if target.starts_with('/') {
        return None;
    }
    let mut stack: Vec<&std::ffi::OsStr> = parent
        .components()
        .filter_map(|component| match component {
            Component::Normal(seg) => Some(seg),
            _ => None,
        })
        .collect();

    for segment in target.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                stack.pop()?;
            }
            other => stack.push(std::ffi::OsStr::new(other)),
        }
    }

    let mut resolved = PathBuf::new();
    for seg in stack {
        resolved.push(seg);
    }
    Some(resolved)
}

fn is_under_any(path: &Path, prefixes: &[PathBuf]) -> bool {
    prefixes.iter().any(|prefix| path.starts_with(prefix))
}

async fn fetch_unbound_blobs<F, S>(
    files: &[InputFile],
    tiered: &Tiered<F, S>,
    concurrency: NonZeroUsize,
) -> Result<(Vec<CasPin>, HashSet<CacheKey>), StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let mut unique_keys = Vec::new();
    let mut seen = HashSet::new();
    for file in files {
        if file.digest.is_empty() {
            continue;
        }
        let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
        if seen.insert(key) {
            unique_keys.push((file.digest, file.executable, key));
        }
    }

    let results: Vec<Result<(CacheKey, FetchOutcome), StagingError>> =
        futures::stream::iter(unique_keys)
            .map(|(digest, executable, key)| async move {
                let outcome = tiered.fetch_to_local(digest, executable).await?;
                Ok((key, outcome))
            })
            .buffer_unordered(concurrency.get())
            .collect()
            .await;

    let mut pins = Vec::new();
    let mut bypassed = HashSet::new();
    for item in results {
        let (key, outcome) = item?;
        match outcome {
            FetchOutcome::Empty => {}
            FetchOutcome::Cached(pin) => pins.push(pin),
            FetchOutcome::Bypassed => {
                bypassed.insert(key);
            }
            FetchOutcome::Missing => {
                return Err(StagingError::MissingBlob {
                    kind: "input file",
                    digest: key.digest,
                });
            }
        }
    }

    Ok((pins, bypassed))
}

#[allow(deprecated)]
fn output_parent_dirs(
    working_dir: &Path,
    command: &reapi::Command,
) -> Result<Vec<PathBuf>, StagingError> {
    let mut parents = Vec::new();
    let mut seen = HashSet::new();
    let all_outputs = command
        .output_paths
        .iter()
        .chain(command.output_files.iter())
        .chain(command.output_directories.iter());
    for raw in all_outputs {
        if raw.is_empty() {
            continue;
        }
        let rel = validate_relative_path(raw, false)?;
        if let Some(parent) = rel.parent()
            && !parent.as_os_str().is_empty()
        {
            let full = working_dir.join(parent);
            if seen.insert(full.clone()) {
                parents.push(full);
            }
        }
    }
    Ok(parents)
}

#[allow(clippy::too_many_arguments)]
async fn materialize_workspace(
    work_dir: &Path,
    working_dir: &Path,
    tmp_dir: &Path,
    unbound_dirs: &[PathBuf],
    mount_points: &[PathBuf],
    output_parents: &[PathBuf],
    unbound_files: &[InputFile],
    unbound_symlinks: &[InputSymlink],
    bypassed_keys: &HashSet<CacheKey>,
    disk: &DiskCas,
    blocking: Option<&BlockingScope>,
    #[cfg(test)] before_materialize: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<(), StagingError> {
    let work_dir = work_dir.to_path_buf();
    let working_dir = working_dir.to_path_buf();
    let tmp_dir = tmp_dir.to_path_buf();
    let unbound_dirs = unbound_dirs.to_vec();
    let mount_points = mount_points.to_vec();
    let output_parents = output_parents.to_vec();
    let files = unbound_files.to_vec();
    let symlinks = unbound_symlinks.to_vec();
    let bypassed = bypassed_keys.clone();
    let disk = disk.clone();

    let run_materialize = move |cancelled: &AtomicBool| -> Result<(), StagingError> {
        #[cfg(test)]
        if let Some(hook) = before_materialize {
            hook();
        }
        check_cancelled(cancelled)?;
        create_dir(&work_dir)?;
        create_dir(&tmp_dir)?;
        create_dir(&working_dir)?;

        for rel_dir in &unbound_dirs {
            check_cancelled(cancelled)?;
            create_dir(&work_dir.join(rel_dir))?;
        }
        for mount_point in &mount_points {
            check_cancelled(cancelled)?;
            create_dir(mount_point)?;
        }
        for parent in &output_parents {
            check_cancelled(cancelled)?;
            create_dir(parent)?;
        }

        for file in &files {
            check_cancelled(cancelled)?;
            let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
            if !file.digest.is_empty() && bypassed.contains(&key) {
                continue;
            }
            let dest = work_dir.join(&file.rel_path);
            let linked = disk.hardlink_or_reflink_to(file.digest, file.executable, &dest)?;
            if !linked {
                return Err(StagingError::MissingBlob {
                    kind: "input file",
                    digest: file.digest,
                });
            }
        }

        for sym in &symlinks {
            check_cancelled(cancelled)?;
            let dest = work_dir.join(&sym.rel_path);
            if let Some(parent) = dest.parent() {
                create_dir(parent)?;
            }
            std::os::unix::fs::symlink(&sym.target, &dest)
                .map_err(|source| StagingError::Io { path: dest, source })?;
        }

        Ok(())
    };

    if let Some(scope) = blocking {
        scope.run(run_materialize).await.map_err(|err| {
            PortError::backend(format!("staging materialization task failed: {err}"))
        })??;
    } else {
        tokio::task::spawn_blocking(move || run_materialize(&AtomicBool::new(false)))
            .await
            .map_err(|err| {
                PortError::backend(format!("staging materialization task failed: {err}"))
            })??;
    }

    Ok(())
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), StagingError> {
    if cancelled.load(Ordering::Acquire) {
        Err(StagingError::Cancelled)
    } else {
        Ok(())
    }
}

fn create_dir(path: &Path) -> Result<(), StagingError> {
    fs::create_dir_all(path).map_err(|source| StagingError::Io {
        path: path.to_path_buf(),
        source,
    })
}

async fn stream_bypassed_files<F, S>(
    files: &[InputFile],
    bypassed_keys: &HashSet<CacheKey>,
    work_dir: &Path,
    tiered: &Tiered<F, S>,
    concurrency: NonZeroUsize,
    blocking: Option<&BlockingScope>,
) -> Result<(), StagingError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    if bypassed_keys.is_empty() {
        return Ok(());
    }
    if blocking.is_some_and(BlockingScope::is_cancelled) {
        return Err(StagingError::Cancelled);
    }

    let direct_jobs: Vec<(Digest, bool, PathBuf)> = files
        .iter()
        .filter_map(|file| {
            let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
            if !file.digest.is_empty() && bypassed_keys.contains(&key) {
                Some((file.digest, file.executable, work_dir.join(&file.rel_path)))
            } else {
                None
            }
        })
        .collect();

    let results: Vec<Result<(), StagingError>> = futures::stream::iter(direct_jobs)
        .map(|(digest, executable, dest)| async move {
            let found = if let Some(scope) = blocking {
                tiered
                    .fetch_direct_to_scoped(digest, &dest, executable, scope)
                    .await?
            } else {
                tiered.fetch_direct_to(digest, &dest, executable).await?
            };
            if !found {
                return Err(StagingError::MissingBlob {
                    kind: "input file",
                    digest,
                });
            }
            Ok(())
        })
        .buffer_unordered(concurrency.get())
        .collect()
        .await;

    for result in results {
        result?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::fs;
    use std::num::NonZeroUsize;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use gang_cas::{DiskCas, Tiered, TreeCache, TreeCacheConfig};
    use gang_reapi::build::bazel::remote::execution::v2 as reapi;
    use gang_reapi::leadingterm::gang::worker::v1 as worker_proto;
    use gang_reapi::to_proto;
    use gang_spi::MAX_RO_BINDS;
    use gang_testkit::{MemoryContentStore, TestResult};
    use gang_types::{AttemptId, Digest, NonZeroBytes, Percent};
    use prost::Message as _;
    use tempfile::TempDir;

    use super::{
        BlockingScope, DirectoryCache, StageConfig, StagingError, stage_attempt,
        stage_attempt_with_scope,
    };

    fn store_proto<M: prost::Message>(store: &MemoryContentStore, msg: &M) -> Digest {
        let bytes = Bytes::from(msg.encode_to_vec());
        store.insert(bytes)
    }

    fn make_dir(
        store: &MemoryContentStore,
        files: Vec<(&str, Digest, bool)>,
        subdirs: Vec<(&str, Digest)>,
        symlinks: Vec<(&str, &str)>,
    ) -> TestResult<Digest> {
        let dir = reapi::Directory {
            files: files
                .into_iter()
                .map(|(name, digest, exec)| {
                    Ok(reapi::FileNode {
                        name: name.to_owned(),
                        digest: Some(to_proto(&digest)?),
                        is_executable: exec,
                        node_properties: None,
                    })
                })
                .collect::<Result<Vec<_>, gang_reapi::DigestConversionError>>()?,
            directories: subdirs
                .into_iter()
                .map(|(name, digest)| {
                    Ok(reapi::DirectoryNode {
                        name: name.to_owned(),
                        digest: Some(to_proto(&digest)?),
                    })
                })
                .collect::<Result<Vec<_>, gang_reapi::DigestConversionError>>()?,
            symlinks: symlinks
                .into_iter()
                .map(|(name, target)| reapi::SymlinkNode {
                    name: name.to_owned(),
                    target: target.to_owned(),
                    node_properties: None,
                })
                .collect(),
            node_properties: None,
        };
        Ok(store_proto(store, &dir))
    }

    fn make_assign(
        attempt: &str,
        action_digest: Digest,
        action: Option<&reapi::Action>,
        command: Option<&reapi::Command>,
    ) -> TestResult<worker_proto::Assign> {
        Ok(worker_proto::Assign {
            attempt: attempt.to_owned(),
            operation: "op-1".to_owned(),
            action_digest: Some(to_proto(&action_digest)?),
            action: action.map(|a| Bytes::from(a.encode_to_vec())),
            command: command.map(|c| Bytes::from(c.encode_to_vec())),
            ..Default::default()
        })
    }

    fn simple_config(min_files: usize) -> StageConfig {
        StageConfig {
            download_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
            tree_cache_min_files: NonZeroUsize::new(min_files).expect("min_files non-zero"),
            directory_cache_budget: NonZeroBytes::mib(8),
        }
    }

    #[tokio::test]
    async fn rejects_exponential_directory_dag_before_materialization() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let mut root = make_dir(&slow, vec![], vec![], vec![])?;
        for _ in 0..30 {
            root = make_dir(&slow, vec![], vec![("a", root), ("b", root)], vec![])?;
        }
        let command = reapi::Command {
            arguments: vec!["unused".into()],
            ..Default::default()
        };
        let action = reapi::Action {
            command_digest: Some(to_proto(&store_proto(&slow, &command))?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let assign = make_assign(
            "00000000-0000-4000-8000-000000000777",
            store_proto(&slow, &action),
            Some(&action),
            Some(&command),
        )?;
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, slow);
        let result = stage_attempt(
            &assign,
            &tmp.path().join("work"),
            &tiered,
            None,
            &DirectoryCache::new(NonZeroBytes::mib(4)),
            None,
            &simple_config(2),
        )
        .await;
        assert!(
            matches!(result, Err(StagingError::InvalidTree { .. })),
            "diamond expansion was accepted: {result:?}"
        );
        Ok(())
    }

    #[test]
    fn accepts_twenty_thousand_file_inventory() -> TestResult {
        let empty = to_proto(&Digest::EMPTY)?;
        let directory = reapi::Directory {
            files: (0..20_000)
                .map(|index| reapi::FileNode {
                    name: format!("file-{index}"),
                    digest: Some(empty.clone()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let digest = Digest::from_bytes(&directory.encode_to_vec());
        let inventory = super::build_inventory(digest, &HashMap::from([(digest, directory)]))?;
        assert_eq!(inventory.files.len(), 20_000);
        Ok(())
    }

    #[test]
    fn rejects_expanded_path_byte_budget_with_few_nodes() -> TestResult {
        let leaf = reapi::Directory {
            files: vec![reapi::FileNode {
                name: "f".repeat(1024),
                digest: Some(to_proto(&Digest::EMPTY)?),
                ..Default::default()
            }],
            ..Default::default()
        };
        let leaf_digest = Digest::from_bytes(&leaf.encode_to_vec());
        let mut map = HashMap::from([(leaf_digest, leaf)]);
        let mut root = leaf_digest;
        for _ in 0..16 {
            let directory = reapi::Directory {
                directories: vec![
                    reapi::DirectoryNode {
                        name: "a".into(),
                        digest: Some(to_proto(&root)?),
                    },
                    reapi::DirectoryNode {
                        name: "b".into(),
                        digest: Some(to_proto(&root)?),
                    },
                ],
                ..Default::default()
            };
            root = Digest::from_bytes(&directory.encode_to_vec());
            map.insert(root, directory);
        }
        assert!(matches!(
            super::build_inventory(root, &map),
            Err(StagingError::InvalidTree { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stages_tree_cache_binds_under_empty_cwd_and_releases_on_release_for_upload()
    -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let tree_cache = TreeCache::open(
            tmp.path().join("trees"),
            Some(tmp.path().join("graveyard")),
            disk.clone(),
            TreeCacheConfig {
                min_files: 2,
                max_trees: 16,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(4));

        // Build `external/toolchain` with 2 files (qualifies for RO bind) + `src/main.c` + `empty.txt`.
        let tc1 = slow.insert(Bytes::from_static(b"gcc-binary"));
        let tc2 = slow.insert(Bytes::from_static(b"stdlib-header"));
        let src = slow.insert(Bytes::from_static(b"int main() { return 0; }"));

        let tc_dir = make_dir(
            &slow,
            vec![("gcc", tc1, true), ("stdio.h", tc2, false)],
            vec![],
            vec![("cc", "gcc")],
        )?;
        let ext_dir = make_dir(&slow, vec![], vec![("toolchain", tc_dir)], vec![])?;
        let src_dir = make_dir(&slow, vec![("main.c", src, false)], vec![], vec![])?;
        let root_digest = make_dir(
            &slow,
            vec![("empty.txt", Digest::EMPTY, false)],
            vec![("external", ext_dir), ("src", src_dir)],
            vec![("link.c", "src/main.c")],
        )?;

        let command = reapi::Command {
            arguments: vec!["external/toolchain/gcc".to_owned()],
            output_paths: vec![
                "bazel-out/bin/app".to_owned(),
                "external/gen/out.txt".to_owned(),
            ],
            working_directory: String::new(), // cwd == ""
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &command);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_digest)?),
            ..Default::default()
        };
        let action_digest = store_proto(&slow, &action);

        let attempt_id = AttemptId::generate()?;
        let assign = make_assign(attempt_id.as_str(), action_digest, None, None)?;

        let work_root = tmp.path().join("work");
        let mut staged = stage_attempt(
            &assign,
            &work_root,
            &tiered,
            Some(&tree_cache),
            &dir_cache,
            None,
            &simple_config(2),
        )
        .await?;

        // `<attempt>/work` and `<attempt>/tmp` must both exist.
        assert!(staged.work_dir().is_dir());
        assert!(staged.tmp_dir().is_dir());
        assert_eq!(staged.working_dir(), staged.work_dir());
        // Output parent `bazel-out/bin` pre-created.
        assert!(staged.working_dir().join("bazel-out/bin").is_dir());

        // `external/toolchain` was bound even though `cwd == ""`!
        assert_eq!(staged.read_only_trees().len(), 1);
        assert_eq!(
            staged.read_only_trees()[0].relative_target,
            Path::new("work/external/toolchain")
        );
        assert_eq!(
            staged.read_only_trees()[0].target,
            staged.work_dir().join("external/toolchain")
        );
        // Empty mountpoint directory was created, not populated with unbound files.
        assert!(staged.work_dir().join("external/toolchain").is_dir());
        assert!(!staged.work_dir().join("external/toolchain/gcc").exists());

        // TreeLease and CasPins are held while StagedAttempt is active.
        assert_eq!(staged.leased_tree_count(), 1);
        assert_eq!(tree_cache.leased_count(), 1);
        assert!(staged.pin_count() >= 1);

        // Unbound files & symlinks materialized properly.
        assert_eq!(
            fs::read(staged.work_dir().join("src/main.c"))?,
            b"int main() { return 0; }"
        );
        assert_eq!(fs::read(staged.work_dir().join("empty.txt"))?, b"");
        assert_eq!(
            fs::read_link(staged.work_dir().join("link.c"))?,
            Path::new("src/main.c")
        );
        // DirectoryCache cached the root and child directories.
        assert!(dir_cache.contains(&root_digest));
        assert!(dir_cache.contains(&tc_dir));

        // Releasing for upload drops both TreeLease and CasPins.
        staged.release_for_upload();
        assert_eq!(staged.leased_tree_count(), 0);
        assert_eq!(staged.pin_count(), 0);
        assert_eq!(tree_cache.leased_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn excludes_cwd_ancestors_and_output_overlaps_in_both_directions() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let tree_cache = TreeCache::open(
            tmp.path().join("trees"),
            Some(tmp.path().join("graveyard")),
            disk,
            TreeCacheConfig {
                min_files: 2,
                max_trees: 16,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(4));

        let b1 = slow.insert(Bytes::from_static(b"blob-1"));
        let b2 = slow.insert(Bytes::from_static(b"blob-2"));
        let two_files = |n1, n2| {
            make_dir(
                &slow,
                vec![(n1, b1, false), (n2, b2, false)],
                vec![],
                vec![],
            )
        };

        let sub_ro = two_files("c", "d")?;
        let out_nested = two_files("e", "f")?;
        let out_parent_leaf = two_files("g", "h")?;
        let ok_tree = two_files("i", "j")?;
        let out_anc = make_dir(&slow, vec![], vec![("nested", out_nested)], vec![])?;
        let sub_work_full = make_dir(
            &slow,
            vec![("a", b1, false), ("b", b2, false)],
            vec![
                ("out_anc", out_anc),
                ("out_parent", out_parent_leaf),
                ("ok_tree", ok_tree),
            ],
            vec![],
        )?;
        let sub_dir = make_dir(
            &slow,
            vec![],
            vec![("dir", sub_work_full), ("ro", sub_ro)],
            vec![],
        )?;
        let root_digest = make_dir(&slow, vec![], vec![("sub", sub_dir)], vec![])?;

        let command = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            working_directory: "sub/dir".to_owned(),
            output_paths: vec!["out_anc".to_owned(), "out_parent/leaf".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &command);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_digest)?),
            ..Default::default()
        };
        let assign = make_assign(
            AttemptId::generate()?.as_str(),
            store_proto(&slow, &action),
            Some(&action),
            Some(&command),
        )?;

        let staged = stage_attempt(
            &assign,
            &tmp.path().join("work"),
            &tiered,
            Some(&tree_cache),
            &dir_cache,
            None,
            &simple_config(2),
        )
        .await?;

        let bound_targets: BTreeSet<PathBuf> = staged
            .read_only_trees()
            .iter()
            .map(|t| t.relative_target.clone())
            .collect();
        assert_eq!(
            bound_targets,
            BTreeSet::from([
                PathBuf::from("work/sub/ro"),
                PathBuf::from("work/sub/dir/ok_tree"),
            ])
        );
        assert!(staged.working_dir().join("out_anc/nested/e").exists());
        assert!(staged.working_dir().join("out_parent/g").exists());
        Ok(())
    }

    #[tokio::test]
    async fn caps_ro_binds_at_64() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let tree_cache = TreeCache::open(
            tmp.path().join("trees-cap"),
            Some(tmp.path().join("graveyard-cap")),
            disk,
            TreeCacheConfig {
                min_files: 1,
                max_trees: 128,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(8));

        let b = slow.insert(Bytes::from_static(b"payload"));
        let mut subdirs = Vec::new();
        for idx in 0..70_usize {
            let sub = make_dir(&slow, vec![("f", b, false)], vec![], vec![])?;
            subdirs.push((format!("t_{idx:02}"), sub));
        }
        let sub_refs: Vec<(&str, Digest)> = subdirs.iter().map(|(n, d)| (n.as_str(), *d)).collect();
        let root_70 = make_dir(&slow, vec![], sub_refs, vec![])?;

        let cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &cmd);
        let action_70 = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_70)?),
            ..Default::default()
        };
        let assign_70 = make_assign(
            AttemptId::generate()?.as_str(),
            store_proto(&slow, &action_70),
            Some(&action_70),
            Some(&cmd),
        )?;
        let staged_70 = stage_attempt(
            &assign_70,
            &tmp.path().join("work-70"),
            &tiered,
            Some(&tree_cache),
            &dir_cache,
            None,
            &simple_config(1),
        )
        .await?;
        assert_eq!(staged_70.read_only_trees().len(), MAX_RO_BINDS);
        let staged_file_count = (0..70_usize)
            .filter(|idx| staged_70.work_dir().join(format!("t_{idx:02}/f")).exists())
            .count();
        assert_eq!(staged_file_count, 70 - MAX_RO_BINDS);
        Ok(())
    }

    #[tokio::test]
    async fn falls_back_to_normal_staging_when_tree_cache_returns_none() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(8));
        let tiny_cache = TreeCache::open(
            tmp.path().join("trees-tiny"),
            Some(tmp.path().join("graveyard-tiny")),
            disk,
            TreeCacheConfig {
                min_files: 1,
                max_trees: 1,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;

        let b = slow.insert(Bytes::from_static(b"payload"));
        let cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &cmd);
        let d_a = make_dir(&slow, vec![("a", b, false)], vec![], vec![])?;
        let d_b = make_dir(&slow, vec![("b", b, false)], vec![], vec![])?;
        let root_a = make_dir(&slow, vec![], vec![("a_dir", d_a)], vec![])?;
        let root_b = make_dir(&slow, vec![], vec![("b_dir", d_b)], vec![])?;

        let act_a = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_a)?),
            ..Default::default()
        };
        let act_b = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root_b)?),
            ..Default::default()
        };

        let hold_a = stage_attempt(
            &make_assign(
                AttemptId::generate()?.as_str(),
                store_proto(&slow, &act_a),
                Some(&act_a),
                Some(&cmd),
            )?,
            &tmp.path().join("work-tiny"),
            &tiered,
            Some(&tiny_cache),
            &dir_cache,
            None,
            &simple_config(1),
        )
        .await?;
        assert_eq!(hold_a.read_only_trees().len(), 1);

        let staged_b = stage_attempt(
            &make_assign(
                AttemptId::generate()?.as_str(),
                store_proto(&slow, &act_b),
                Some(&act_b),
                Some(&cmd),
            )?,
            &tmp.path().join("work-tiny"),
            &tiered,
            Some(&tiny_cache),
            &dir_cache,
            None,
            &simple_config(1),
        )
        .await?;
        assert!(staged_b.read_only_trees().is_empty());
        assert_eq!(fs::read(staged_b.work_dir().join("b_dir/b"))?, b"payload");
        Ok(())
    }

    #[tokio::test]
    async fn stages_bypassed_blobs_directly() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 8).await?;
        let tiered = Tiered::new(disk, slow.clone());
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(1));

        let big_blob = slow.insert(Bytes::from_static(b"0123456789abcdef"));
        let root = make_dir(&slow, vec![("big.sh", big_blob, true)], vec![], vec![])?;
        let valid_cmd = reapi::Command {
            arguments: vec!["./big.sh".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &valid_cmd);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let action_digest = store_proto(&slow, &action);

        let staged = stage_attempt(
            &make_assign(
                AttemptId::generate()?.as_str(),
                action_digest,
                Some(&action),
                Some(&valid_cmd),
            )?,
            &tmp.path().join("work"),
            &tiered,
            None,
            &dir_cache,
            None,
            &simple_config(512),
        )
        .await?;
        let big_path = staged.work_dir().join("big.sh");
        assert_eq!(fs::read(&big_path)?, b"0123456789abcdef");
        assert_eq!(fs::metadata(&big_path)?.permissions().mode() & 0o111, 0o111);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_path_traversal_and_reserved_names() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, slow.clone());
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(1));

        let root = make_dir(&slow, vec![], vec![], vec![])?;
        let valid_cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &valid_cmd);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let action_digest = store_proto(&slow, &action);
        let work_root = tmp.path().join("work");
        let cfg = simple_config(512);

        for bad_attempt in [".graveyard", ".side_channels", "../escape"] {
            let assign = make_assign(bad_attempt, action_digest, Some(&action), Some(&valid_cmd))?;
            let err = stage_attempt(&assign, &work_root, &tiered, None, &dir_cache, None, &cfg)
                .await
                .expect_err("bad attempt id rejected");
            assert!(matches!(err, StagingError::InvalidAttemptId(_)));
        }

        for bad_cwd in ["../up", "/abs/dir", ".graveyard/x", ".side_channels"] {
            let mut bad_cmd = valid_cmd.clone();
            bad_cmd.working_directory = bad_cwd.to_owned();
            let id = AttemptId::generate()?;
            let assign = make_assign(id.as_str(), action_digest, Some(&action), Some(&bad_cmd))?;
            let err = stage_attempt(&assign, &work_root, &tiered, None, &dir_cache, None, &cfg)
                .await
                .expect_err("bad cwd rejected");
            assert!(matches!(err, StagingError::InvalidPath { .. }));
        }

        for bad_out in [
            "../secret",
            "/etc/passwd",
            ".graveyard/out",
            ".side_channels/f",
        ] {
            let mut bad_cmd = valid_cmd.clone();
            bad_cmd.output_paths = vec![bad_out.to_owned()];
            let id = AttemptId::generate()?;
            let assign = make_assign(id.as_str(), action_digest, Some(&action), Some(&bad_cmd))?;
            let err = stage_attempt(&assign, &work_root, &tiered, None, &dir_cache, None, &cfg)
                .await
                .expect_err("bad output path rejected");
            assert!(matches!(err, StagingError::InvalidPath { .. }));
        }

        let bad_dir = reapi::Directory {
            files: vec![reapi::FileNode {
                name: "../evil".to_owned(),
                digest: Some(to_proto(&Digest::EMPTY)?),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let bad_root = store_proto(&slow, &bad_dir);
        let bad_action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&bad_root)?),
            ..Default::default()
        };
        let id = AttemptId::generate()?;
        let assign = make_assign(
            id.as_str(),
            store_proto(&slow, &bad_action),
            Some(&bad_action),
            Some(&valid_cmd),
        )?;
        let err = stage_attempt(&assign, &work_root, &tiered, None, &dir_cache, None, &cfg)
            .await
            .expect_err("bad directory entry rejected");
        assert!(matches!(err, StagingError::InvalidPath { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn directory_cache_evicts_lru_within_budget() {
        let dir1 = Arc::new(reapi::Directory {
            files: vec![reapi::FileNode {
                name: "aaaa".to_owned(),
                digest: None,
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        });
        let dir2 = Arc::new(reapi::Directory {
            files: vec![reapi::FileNode {
                name: "bbbb".to_owned(),
                digest: None,
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        });
        let d1 = Digest::from_bytes(&dir1.encode_to_vec());
        let d2 = Digest::from_bytes(&dir2.encode_to_vec());
        // Budget fits one entry, not two.
        let budget = NonZeroBytes::new(gang_types::Bytes::new(d1.size() + 2)).expect("non-zero");
        let cache = DirectoryCache::new(budget);

        cache.insert(d1, Arc::clone(&dir1));
        assert!(cache.contains(&d1));
        cache.insert(d2, Arc::clone(&dir2));
        assert!(!cache.contains(&d1));
        assert!(cache.contains(&d2));
        assert!(cache.used_bytes() <= budget.get().get());
    }

    #[tokio::test]
    async fn ancestor_wins_and_escaping_symlink_is_excluded() -> TestResult {
        let tmp = TempDir::new()?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let tree_cache = TreeCache::open(
            tmp.path().join("trees"),
            Some(tmp.path().join("graveyard")),
            disk,
            TreeCacheConfig {
                min_files: 2,
                max_trees: 16,
                tree_pin_percent: Percent::from_percent(50),
            },
        )
        .await?;
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(4));

        let b = slow.insert(Bytes::from_static(b"data"));
        let child = make_dir(
            &slow,
            vec![("b", b, false), ("c", b, false)],
            vec![],
            vec![],
        )?;
        let parent = make_dir(&slow, vec![("a", b, false)], vec![("child", child)], vec![])?;
        let escaping = make_dir(
            &slow,
            vec![("x", b, false), ("y", b, false)],
            vec![],
            vec![("escape", "../outside")],
        )?;
        let root = make_dir(
            &slow,
            vec![],
            vec![("parent", parent), ("escaping", escaping)],
            vec![],
        )?;

        let cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &cmd);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let staged = stage_attempt(
            &make_assign(
                AttemptId::generate()?.as_str(),
                store_proto(&slow, &action),
                Some(&action),
                Some(&cmd),
            )?,
            &tmp.path().join("work"),
            &tiered,
            Some(&tree_cache),
            &dir_cache,
            None,
            &simple_config(2),
        )
        .await?;

        assert_eq!(staged.read_only_trees().len(), 1);
        assert_eq!(
            staged.read_only_trees()[0].relative_target,
            Path::new("work/parent")
        );
        assert_eq!(
            fs::read_link(staged.work_dir().join("escaping/escape"))?,
            Path::new("../outside")
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_joins_blocking_materialization_before_dir_exists()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::future::Future as _;
        use std::task::Poll;

        struct ReleaseLatch {
            done: parking_lot::Mutex<bool>,
            cv: parking_lot::Condvar,
        }
        impl ReleaseLatch {
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
        struct LatchGuard(Arc<ReleaseLatch>);
        impl Drop for LatchGuard {
            fn drop(&mut self) {
                self.0.release();
            }
        }

        let tmp = tempfile::tempdir()?;
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let slow = MemoryContentStore::new();
        let tiered = Tiered::new(disk, slow.clone());
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(8));

        let payload = slow.insert(Bytes::from_static(b"hello-stage"));
        let root = make_dir(&slow, vec![("input.txt", payload, false)], vec![], vec![])?;
        let cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &cmd);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let attempt_id = AttemptId::generate()?;
        let assign = make_assign(
            attempt_id.as_str(),
            store_proto(&slow, &action),
            Some(&action),
            Some(&cmd),
        )?;

        let work_root = tmp.path().join("work");
        let entered = Arc::new(tokio::sync::Notify::new());
        let latch = Arc::new(ReleaseLatch {
            done: parking_lot::Mutex::new(false),
            cv: parking_lot::Condvar::new(),
        });
        let _guard = LatchGuard(Arc::clone(&latch));
        let entered_hook = Arc::clone(&entered);
        let latch_hook = Arc::clone(&latch);
        let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            entered_hook.notify_one();
            latch_hook.wait();
        });

        let blocking = BlockingScope::new();
        let stage_cfg = simple_config(64);
        let staging_future = stage_attempt_with_scope(
            &assign,
            &work_root,
            &tiered,
            None,
            &dir_cache,
            None,
            &stage_cfg,
            Some(&blocking),
            false,
            Some(hook),
        );
        let outcome = tokio::select! {
            biased;
            () = entered.notified() => None,
            res = staging_future => Some(res),
        };
        assert!(outcome.is_none(), "cancellation must interrupt staging");

        let mut join_fut = std::pin::pin!(blocking.cancel_and_join());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(join_fut.as_mut().poll(cx).is_pending())).await;
        latch.release();
        join_fut.await;

        assert!(
            pending,
            "cancel_and_join must remain Pending while blocking materialization is held"
        );
        assert!(
            !work_root.join(attempt_id.as_str()).exists(),
            "cancelled blocking materialization must never recreate the attempt directory"
        );
        Ok(())
    }

    #[tokio::test]
    async fn forced_bypass_cancellation_drains_direct_write_scope_before_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        // Tiny DiskCas budget forces `big.bin` onto the `stream_bypassed_files` direct-write path.
        let disk = DiskCas::open(tmp.path().join("cas"), 16).await?;
        let slow = MemoryContentStore::new();
        let tiered = Tiered::new(disk, slow.clone());
        let dir_cache = DirectoryCache::new(NonZeroBytes::mib(8));

        let payload = Bytes::from(vec![b'b'; 128]);
        let payload_digest = slow.insert(payload);
        let root = make_dir(
            &slow,
            vec![("big.bin", payload_digest, false)],
            vec![],
            vec![],
        )?;
        let cmd = reapi::Command {
            arguments: vec!["/bin/true".to_owned()],
            ..Default::default()
        };
        let cmd_digest = store_proto(&slow, &cmd);
        let action = reapi::Action {
            command_digest: Some(to_proto(&cmd_digest)?),
            input_root_digest: Some(to_proto(&root)?),
            ..Default::default()
        };
        let attempt_id = AttemptId::generate()?;
        let assign = make_assign(
            attempt_id.as_str(),
            store_proto(&slow, &action),
            Some(&action),
            Some(&cmd),
        )?;

        let work_root = tmp.path().join("work");
        let blocking = BlockingScope::new();
        let stage_cfg = simple_config(64);
        let staged = stage_attempt_with_scope(
            &assign,
            &work_root,
            &tiered,
            None,
            &dir_cache,
            None,
            &stage_cfg,
            Some(&blocking),
            false,
            None,
        )
        .await?;
        assert_eq!(fs::read(staged.work_dir().join("big.bin"))?.len(), 128);

        // Pre-cancelled scope refuses direct-write staging cleanly and leaves no attempt dir.
        let attempt_id2 = AttemptId::generate()?;
        let assign2 = make_assign(
            attempt_id2.as_str(),
            store_proto(&slow, &action),
            Some(&action),
            Some(&cmd),
        )?;
        let cancelled = BlockingScope::new();
        cancelled.cancel();
        let err = stage_attempt_with_scope(
            &assign2,
            &work_root,
            &tiered,
            None,
            &dir_cache,
            None,
            &stage_cfg,
            Some(&cancelled),
            false,
            None,
        )
        .await
        .expect_err("cancelled scope must fail closed");
        assert!(matches!(err, StagingError::Cancelled));
        cancelled.cancel_and_join().await;
        assert!(!work_root.join(attempt_id2.as_str()).exists());
        Ok(())
    }
}
