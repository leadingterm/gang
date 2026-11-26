//! The cache stores: the content-addressed store and the action cache, both kept under one directory
//! and opened as [`FilesystemContentStore`] and [`FilesystemActionStore`].
//!
//! The directory is created on first use, not when the plugin is built, so a node that only runs
//! workers never creates it. The stores are single-process, so workers reach them through the
//! controller.

use std::collections::HashMap;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use gang_cas::DiskCas;
use gang_os::PathIo;
use gang_os::fs::{ensure_private_dir, require_private_dir, space};
use gang_spi::{
    ActionStore, BlobStream, ByteRange, CacheStorePort, ContentStore, EngineStore, MeasuredUsage,
    Plugin, PluginContext, PortError, StoreAccess, StoreKind,
};
use gang_types::{AbsolutePathBuf, Bytes, Digest, EnvironmentName, Percent};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::errors::LocalError;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

/// Settings for the filesystem cache plugin, read from the `[cache.filesystem]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "FilesystemCacheDoc")]
pub struct FilesystemCacheConfig {
    /// Absolute path to the directory holding the content store and the action cache. Created with
    /// mode `0700` on first use when missing; an existing directory must already be `0700` and
    /// owned by the current user, and is never changed.
    pub path: AbsolutePathBuf,
    /// Share of the total capacity of the filesystem that holds `path` given to the content store
    /// as its eviction budget, in percent. Default `25`.
    pub content_store_percent: Percent,
    /// Share of the total capacity of the filesystem that holds `path` given to the action cache
    /// as its eviction budget, in percent. Default `2`.
    pub action_store_percent: Percent,
}

/// The shape of `[cache.filesystem]` as written, checked into [`FilesystemCacheConfig`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilesystemCacheDoc {
    path: AbsolutePathBuf,
    #[serde(default = "default_content_store_percent")]
    content_store_percent: Percent,
    #[serde(default = "default_action_store_percent")]
    action_store_percent: Percent,
}

/// The serde default of `content_store_percent`.
fn default_content_store_percent() -> Percent {
    Percent::from_percent(25)
}

/// The serde default of `action_store_percent`.
fn default_action_store_percent() -> Percent {
    Percent::from_percent(2)
}

/// The two store shares of a [`FilesystemCacheConfig`] add up to more than the whole capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("content_store_percent and action_store_percent must sum to at most 100")]
pub struct StorePercentsError;

impl TryFrom<FilesystemCacheDoc> for FilesystemCacheConfig {
    type Error = StorePercentsError;

    fn try_from(doc: FilesystemCacheDoc) -> Result<Self, Self::Error> {
        if u16::from(doc.content_store_percent.get()) + u16::from(doc.action_store_percent.get())
            > 100
        {
            return Err(StorePercentsError);
        }
        Ok(Self {
            path: doc.path,
            content_store_percent: doc.content_store_percent,
            action_store_percent: doc.action_store_percent,
        })
    }
}

/// The cache stores, kept in one directory on the local filesystem.
#[derive(Debug)]
pub struct FilesystemCache {
    path: PathBuf,
    content_percent: Percent,
    action_percent: Percent,
    budgets: OnceLock<(Bytes, Bytes)>,
    content_store: OnceLock<FilesystemContentStore>,
    action_stores: Mutex<HashMap<String, FilesystemActionStore>>,
}

impl Plugin for FilesystemCache {
    type Config = FilesystemCacheConfig;

    fn build(
        config: FilesystemCacheConfig,
        _context: &PluginContext<'_>,
    ) -> Result<Self, PortError> {
        Ok(Self {
            path: config.path.into_path_buf(),
            content_percent: config.content_store_percent,
            action_percent: config.action_store_percent,
            budgets: OnceLock::new(),
            content_store: OnceLock::new(),
            action_stores: Mutex::new(HashMap::new()),
        })
    }
}

impl FilesystemCache {
    /// Creates the directory tree under `path` and returns the content and action budgets. `path`
    /// is the configured directory: when it exists and is not private it is refused and left
    /// unchanged. The budgets are cut from the capacity of `path`'s filesystem on first use and
    /// reused after that.
    fn prepare(&self) -> Result<(Bytes, Bytes), LocalError> {
        require_private_dir(&self.path)?;
        for directory in [
            content_dir(&self.path),
            content_temp_dir(&self.path),
            action_dir(&self.path),
            action_temp_dir(&self.path),
        ] {
            ensure_private_dir(&directory)?;
        }
        if let Some(budgets) = self.budgets.get() {
            return Ok(*budgets);
        }
        let capacity = space(&self.path)?.total;
        let computed = budgets(capacity, self.content_percent, self.action_percent);
        Ok(*self.budgets.get_or_init(|| computed))
    }

    /// Returns the content-addressed store backed by [`DiskCas`] under `path`, preparing the
    /// directory tree and caching the store handle on first call.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if the cache root is not private, its subdirectories cannot be
    /// created, or filesystem capacity cannot be queried.
    pub fn open_content_store(&self) -> Result<FilesystemContentStore, PortError> {
        let (budget, _) = self.prepare().map_err(PortError::from)?;
        if let Some(store) = self.content_store.get() {
            return Ok(store.clone());
        }
        let store = FilesystemContentStore::new(content_dir(&self.path), budget.get());
        Ok(self.content_store.get_or_init(|| store).clone())
    }

    /// Returns the environment-scoped [`FilesystemActionStore`] under `path`, preparing its
    /// directories, scanning any existing entries, and caching the store handle on first call.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if the cache root is not private, the environment's action
    /// directories cannot be created or scanned, or filesystem capacity cannot be queried.
    pub fn open_action_store(
        &self,
        environment: &EnvironmentName,
    ) -> Result<FilesystemActionStore, PortError> {
        let (_, budget) = self.prepare().map_err(PortError::from)?;
        let mut stores = self.action_stores.lock();
        if let Some(store) = stores.get(environment.as_str()) {
            return Ok(store.clone());
        }
        let env_root = environment
            .cache_namespace()
            .map(|name| self.path.join("environments").join(name));
        let env_root = env_root.as_deref().unwrap_or(&self.path);
        let content_path = action_dir(env_root);
        let temp_path = action_temp_dir(env_root);
        ensure_private_dir(&content_path).map_err(LocalError::from)?;
        ensure_private_dir(&temp_path).map_err(LocalError::from)?;
        let store = FilesystemActionStore::open(content_path, temp_path, budget.get())?;
        stores.insert(environment.as_str().to_owned(), store.clone());
        Ok(store)
    }
}

/// Local [`ContentStore`] backed by [`DiskCas`] and lazily opened on first async I/O.
#[derive(Debug, Clone)]
pub struct FilesystemContentStore {
    inner: Arc<FilesystemContentStoreInner>,
}

#[derive(Debug)]
struct FilesystemContentStoreInner {
    root: PathBuf,
    budget_bytes: u64,
    cas: tokio::sync::OnceCell<DiskCas>,
}

impl FilesystemContentStore {
    /// Creates a lazy handle to a [`DiskCas`] rooted at `root` with `budget_bytes`.
    #[must_use]
    pub fn new(root: PathBuf, budget_bytes: u64) -> Self {
        Self {
            inner: Arc::new(FilesystemContentStoreInner {
                root,
                budget_bytes,
                cas: tokio::sync::OnceCell::new(),
            }),
        }
    }

    /// Returns the root directory of the underlying [`DiskCas`].
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Returns the eviction budget in bytes.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.inner.budget_bytes
    }

    /// Opens or returns the underlying [`DiskCas`] instance.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if [`DiskCas::open`] fails.
    pub async fn disk_cas(&self) -> Result<&DiskCas, PortError> {
        self.inner
            .cas
            .get_or_try_init(|| DiskCas::open(&self.inner.root, self.inner.budget_bytes))
            .await
    }
}

impl ContentStore for FilesystemContentStore {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        self.disk_cas().await?.find_missing(digests, missing).await
    }

    async fn read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        self.disk_cas().await?.read(digest, range).await
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        self.disk_cas().await?.write(digest, body).await
    }

    fn usage(&self) -> Option<EngineStore> {
        match self.inner.cas.get() {
            Some(cas) => cas.usage(),
            None => Some(EngineStore {
                store: StoreKind::Content,
                used: 0,
                budget: self.inner.budget_bytes,
                evicted: 0,
            }),
        }
    }
}

/// Environment-scoped local [`ActionStore`] backed by a sharded directory tree and an O(1) LRU
/// index bounded by `budget_bytes`.
#[derive(Debug, Clone)]
pub struct FilesystemActionStore {
    inner: Arc<ActionStoreInner>,
}

#[derive(Debug)]
struct ActionStoreInner {
    content_dir: PathBuf,
    temp_dir: PathBuf,
    budget_bytes: u64,
    state: Mutex<ActionStoreState>,
    used_bytes: AtomicU64,
    evicted_bytes: AtomicU64,
}

#[derive(Debug)]
struct ActionStoreState {
    lru: ActionLru,
}

impl FilesystemActionStore {
    /// Opens or initializes a [`FilesystemActionStore`] rooted at `content_dir` using `temp_dir`
    /// for atomic writes and bounded by `budget_bytes`.
    ///
    /// Scans existing shard directories, removes stale temporary files and malformed entries,
    /// reconstructs LRU order from file timestamps, and evicts any excess above `budget_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`PortError`] if `content_dir` or `temp_dir` cannot be scanned or created.
    pub fn open(
        content_dir: impl AsRef<Path>,
        temp_dir: impl AsRef<Path>,
        budget_bytes: u64,
    ) -> Result<Self, PortError> {
        let content_dir = content_dir.as_ref().to_path_buf();
        let temp_dir = temp_dir.as_ref().to_path_buf();

        let entries = scan_action_root(&content_dir, &temp_dir).map_err(PortError::from)?;
        let mut lru = ActionLru::from_scan(entries);
        let evicted = lru.evict_until(budget_bytes);
        for digest in evicted {
            let _ = fs::remove_file(action_entry_path(&content_dir, digest));
        }
        let used = lru.used_bytes;
        let evicted_total = lru.evicted_bytes;

        Ok(Self {
            inner: Arc::new(ActionStoreInner {
                content_dir,
                temp_dir,
                budget_bytes,
                state: Mutex::new(ActionStoreState { lru }),
                used_bytes: AtomicU64::new(used),
                evicted_bytes: AtomicU64::new(evicted_total),
            }),
        })
    }

    /// Returns the action content directory path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.inner.content_dir
    }

    /// Returns the eviction budget in bytes.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.inner.budget_bytes
    }
}

impl ActionStore for FilesystemActionStore {
    async fn get(&self, action: Digest) -> Result<Option<bytes::Bytes>, PortError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || get_action_sync(&inner, action))
            .await
            .map_err(PortError::backend)?
            .map_err(PortError::from)
    }

    async fn put(&self, action: Digest, result: bytes::Bytes) -> Result<(), PortError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || put_action_sync(&inner, action, &result))
            .await
            .map_err(PortError::backend)?
            .map_err(PortError::from)
    }

    fn usage(&self) -> Option<EngineStore> {
        Some(EngineStore {
            store: StoreKind::Action,
            used: self.inner.used_bytes.load(Ordering::Acquire),
            budget: self.inner.budget_bytes,
            evicted: self.inner.evicted_bytes.load(Ordering::Acquire),
        })
    }
}

fn get_action_sync(
    inner: &ActionStoreInner,
    action: Digest,
) -> Result<Option<bytes::Bytes>, LocalError> {
    if !inner.state.lock().lru.contains(action) {
        return Ok(None);
    }

    let path = action_entry_path(&inner.content_dir, action);
    match fs::read(&path) {
        Ok(bytes) => {
            let _ = inner.state.lock().lru.touch(action);
            Ok(Some(bytes::Bytes::from(bytes)))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut state = inner.state.lock();
            if !path.exists() && state.lru.remove(action).is_some() {
                inner
                    .used_bytes
                    .store(state.lru.used_bytes, Ordering::Release);
            }
            Ok(None)
        }
        Err(error) => Err(LocalError::Io(PathIo::new("read", &path, error))),
    }
}

fn put_action_sync(
    inner: &ActionStoreInner,
    action: Digest,
    result: &[u8],
) -> Result<(), LocalError> {
    let size = u64::try_from(result.len()).unwrap_or(u64::MAX);
    let dest_path = action_entry_path(&inner.content_dir, action);

    if size > inner.budget_bytes {
        let mut state = inner.state.lock();
        if state.lru.remove(action).is_some() {
            let _ = fs::remove_file(&dest_path);
            inner
                .used_bytes
                .store(state.lru.used_bytes, Ordering::Release);
        }
        return Ok(());
    }

    ensure_shard_dir(&inner.content_dir, &action)?;
    let (tmp_path, mut guard) = write_action_temp(&inner.temp_dir, result)?;

    let mut state = inner.state.lock();
    fs::rename(&tmp_path, &dest_path)
        .map_err(|error| LocalError::Io(PathIo::new("publish action result", &dest_path, error)))?;
    guard.defuse();

    state.lru.insert_or_replace(action, size);
    let evicted = state.lru.evict_until(inner.budget_bytes);
    for evicted_digest in evicted {
        let _ = fs::remove_file(action_entry_path(&inner.content_dir, evicted_digest));
    }
    inner
        .used_bytes
        .store(state.lru.used_bytes, Ordering::Release);
    inner
        .evicted_bytes
        .store(state.lru.evicted_bytes, Ordering::Release);
    Ok(())
}

struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn ensure_shard_dir(content_dir: &Path, action: &Digest) -> Result<PathBuf, LocalError> {
    let shard = content_dir.join(shard_dir_name(action));
    if shard.is_dir() {
        return Ok(shard);
    }
    match DirBuilder::new().mode(0o700).create(&shard) {
        Ok(()) => Ok(shard),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(shard),
        Err(error) => Err(LocalError::Io(PathIo::new(
            "create directory",
            &shard,
            error,
        ))),
    }
}

fn write_action_temp(
    temp_dir: &Path,
    result: &[u8],
) -> Result<(PathBuf, TempFileGuard), LocalError> {
    let seq = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let tmp_path = temp_dir.join(format!(".ac-{}-{seq}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)
        .map_err(|error| LocalError::Io(PathIo::new("create temporary file", &tmp_path, error)))?;
    let guard = TempFileGuard::new(tmp_path.clone());
    file.write_all(result)
        .map_err(|error| LocalError::Io(PathIo::new("write", &tmp_path, error)))?;
    drop(file);
    Ok((tmp_path, guard))
}

fn shard_dir_name(digest: &Digest) -> String {
    format!("{:02x}", digest.hash()[0])
}

fn action_entry_path(content_dir: &Path, action: Digest) -> PathBuf {
    content_dir
        .join(shard_dir_name(&action))
        .join(format!("{}-{}", action.hex(), action.size()))
}

fn parse_action_entry_name(name: &str) -> Option<Digest> {
    let (hex_part, size_part) = name.split_once('-')?;
    if hex_part.len() != 64
        || !hex_part
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    if size_part.is_empty() || !size_part.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let size = size_part.parse::<u64>().ok()?;
    Digest::from_hex_and_size(hex_part, size).ok()
}

fn scan_action_root(
    content_dir: &Path,
    temp_dir: &Path,
) -> Result<Vec<(Digest, u64, u64)>, LocalError> {
    if temp_dir.exists() {
        fs::remove_dir_all(temp_dir).map_err(|error| {
            LocalError::Io(PathIo::new("clean temporary directory", temp_dir, error))
        })?;
    }
    DirBuilder::new()
        .mode(0o700)
        .recursive(true)
        .create(temp_dir)
        .map_err(|error| LocalError::Io(PathIo::new("create directory", temp_dir, error)))?;
    DirBuilder::new()
        .mode(0o700)
        .recursive(true)
        .create(content_dir)
        .map_err(|error| LocalError::Io(PathIo::new("create directory", content_dir, error)))?;

    let mut entries = Vec::new();
    let dir_iter = fs::read_dir(content_dir)
        .map_err(|error| LocalError::Io(PathIo::new("read directory", content_dir, error)))?;

    for entry_res in dir_iter {
        let entry = entry_res
            .map_err(|error| LocalError::Io(PathIo::new("read directory", content_dir, error)))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let Some(shard_name) = file_name.to_str() else {
            remove_unknown_path(&path)?;
            continue;
        };
        let is_valid_shard = shard_name.len() == 2
            && shard_name
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            && entry
                .file_type()
                .map_err(|error| LocalError::Io(PathIo::new("inspect", &path, error)))?
                .is_dir();
        if !is_valid_shard {
            remove_unknown_path(&path)?;
            continue;
        }
        scan_action_shard(&path, shard_name, &mut entries)?;
    }

    Ok(entries)
}

fn scan_action_shard(
    shard_path: &Path,
    shard_name: &str,
    entries: &mut Vec<(Digest, u64, u64)>,
) -> Result<(), LocalError> {
    let iter = fs::read_dir(shard_path)
        .map_err(|error| LocalError::Io(PathIo::new("read directory", shard_path, error)))?;
    for entry_res in iter {
        let entry = entry_res
            .map_err(|error| LocalError::Io(PathIo::new("read directory", shard_path, error)))?;
        let path = entry.path();
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(LocalError::Io(PathIo::new("inspect", &path, error))),
        };
        if !meta.file_type().is_file() {
            remove_unknown_path(&path)?;
            continue;
        }
        let Some(digest) = entry
            .file_name()
            .to_str()
            .and_then(parse_action_entry_name)
            .filter(|digest| shard_dir_name(digest) == shard_name)
        else {
            fs::remove_file(&path)
                .map_err(|error| LocalError::Io(PathIo::new("remove", &path, error)))?;
            continue;
        };
        entries.push((digest, meta.len(), metadata_lru_stamp(&meta)));
    }
    Ok(())
}

fn remove_unknown_path(path: &Path) -> Result<(), LocalError> {
    let meta = fs::symlink_metadata(path)
        .map_err(|error| LocalError::Io(PathIo::new("inspect", path, error)))?;
    if meta.file_type().is_dir() {
        fs::remove_dir_all(path)
            .map_err(|error| LocalError::Io(PathIo::new("remove directory", path, error)))
    } else {
        fs::remove_file(path).map_err(|error| LocalError::Io(PathIo::new("remove", path, error)))
    }
}

fn metadata_lru_stamp(meta: &fs::Metadata) -> u64 {
    let atime = nanos_stamp(meta.atime(), meta.atime_nsec());
    let mtime = nanos_stamp(meta.mtime(), meta.mtime_nsec());
    let ctime = nanos_stamp(meta.ctime(), meta.ctime_nsec());
    atime.max(mtime).max(ctime)
}

fn nanos_stamp(secs: i64, nsecs: i64) -> u64 {
    let secs = u64::try_from(secs).unwrap_or(0);
    let nsecs = u64::try_from(nsecs).unwrap_or(0);
    secs.saturating_mul(1_000_000_000).saturating_add(nsecs)
}

#[derive(Debug)]
struct ActionNode {
    digest: Digest,
    size: u64,
    stamp: u64,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Debug)]
enum ActionSlot {
    Occupied(ActionNode),
    Vacant { next_free: Option<usize> },
}

#[derive(Debug, Default)]
struct ActionLru {
    index: HashMap<Digest, usize>,
    slots: Vec<ActionSlot>,
    free_head: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    stamp_clock: u64,
    used_bytes: u64,
    evicted_bytes: u64,
}

impl ActionLru {
    fn from_scan(entries: Vec<(Digest, u64, u64)>) -> Self {
        let mut lru = Self {
            stamp_clock: 1,
            ..Self::default()
        };
        let mut ordered = Vec::with_capacity(entries.len());
        for (digest, size, stamp) in entries {
            lru.stamp_clock = lru.stamp_clock.max(stamp.saturating_add(1));
            if let Some(&existing_idx) = lru.index.get(&digest) {
                if let Some(old_size) = lru.node_mut(existing_idx).map(|node| {
                    let prev = node.size;
                    node.size = size;
                    node.stamp = stamp;
                    prev
                }) {
                    lru.used_bytes = lru.used_bytes.saturating_sub(old_size).saturating_add(size);
                }
                continue;
            }
            let idx = lru.alloc_slot(ActionNode {
                digest,
                size,
                stamp,
                prev: None,
                next: None,
            });
            lru.index.insert(digest, idx);
            lru.used_bytes = lru.used_bytes.saturating_add(size);
            ordered.push((stamp, idx));
        }

        ordered.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
        let mut prev = None;
        for (_, idx) in ordered {
            if let Some(node) = lru.node_mut(idx) {
                node.prev = prev;
                node.next = None;
            }
            if let Some(prev_idx) = prev {
                if let Some(prev_node) = lru.node_mut(prev_idx) {
                    prev_node.next = Some(idx);
                }
            } else {
                lru.head = Some(idx);
            }
            prev = Some(idx);
        }
        lru.tail = prev;
        lru
    }

    fn next_stamp(&mut self) -> u64 {
        let stamp = self.stamp_clock;
        self.stamp_clock = self.stamp_clock.saturating_add(1);
        stamp
    }

    fn contains(&self, digest: Digest) -> bool {
        self.index.contains_key(&digest)
    }

    fn touch(&mut self, digest: Digest) -> bool {
        let stamp = self.next_stamp();
        let Some(&idx) = self.index.get(&digest) else {
            return false;
        };
        if let Some(node) = self.node_mut(idx) {
            node.stamp = stamp;
        }
        self.move_to_front(idx);
        true
    }

    fn insert_or_replace(&mut self, digest: Digest, size: u64) {
        let stamp = self.next_stamp();
        if let Some(&idx) = self.index.get(&digest) {
            if let Some(node) = self.node_mut(idx) {
                let old_size = node.size;
                node.size = size;
                node.stamp = stamp;
                self.used_bytes = self
                    .used_bytes
                    .saturating_sub(old_size)
                    .saturating_add(size);
            }
            self.move_to_front(idx);
            return;
        }
        let idx = self.alloc_slot(ActionNode {
            digest,
            size,
            stamp,
            prev: None,
            next: None,
        });
        self.index.insert(digest, idx);
        self.link_front(idx);
        self.used_bytes = self.used_bytes.saturating_add(size);
    }

    fn remove(&mut self, digest: Digest) -> Option<u64> {
        let idx = self.index.remove(&digest)?;
        let size = self.node(idx)?.size;
        self.unlink(idx);
        self.free_slot(idx);
        self.used_bytes = self.used_bytes.saturating_sub(size);
        Some(size)
    }

    fn evict_until(&mut self, target_bytes: u64) -> Vec<Digest> {
        let mut evicted = Vec::new();
        while self.used_bytes > target_bytes {
            let Some(idx) = self.tail else {
                break;
            };
            let Some((digest, size)) = self.node(idx).map(|node| (node.digest, node.size)) else {
                break;
            };
            self.unlink(idx);
            self.index.remove(&digest);
            self.free_slot(idx);
            self.used_bytes = self.used_bytes.saturating_sub(size);
            self.evicted_bytes = self.evicted_bytes.saturating_add(size);
            evicted.push(digest);
        }
        evicted
    }

    fn alloc_slot(&mut self, node: ActionNode) -> usize {
        if let Some(idx) = self.free_head {
            let next_free = match self.slots.get(idx) {
                Some(ActionSlot::Vacant { next_free }) => *next_free,
                _ => None,
            };
            self.free_head = next_free;
            if let Some(slot) = self.slots.get_mut(idx) {
                *slot = ActionSlot::Occupied(node);
            }
            return idx;
        }
        let idx = self.slots.len();
        self.slots.push(ActionSlot::Occupied(node));
        idx
    }

    fn free_slot(&mut self, idx: usize) {
        if let Some(slot) = self.slots.get_mut(idx) {
            *slot = ActionSlot::Vacant {
                next_free: self.free_head,
            };
            self.free_head = Some(idx);
        }
    }

    fn node(&self, idx: usize) -> Option<&ActionNode> {
        match self.slots.get(idx) {
            Some(ActionSlot::Occupied(node)) => Some(node),
            _ => None,
        }
    }

    fn node_mut(&mut self, idx: usize) -> Option<&mut ActionNode> {
        match self.slots.get_mut(idx) {
            Some(ActionSlot::Occupied(node)) => Some(node),
            _ => None,
        }
    }

    fn unlink(&mut self, idx: usize) {
        let Some((prev, next)) = self.node(idx).map(|node| (node.prev, node.next)) else {
            return;
        };
        if let Some(prev_idx) = prev {
            if let Some(prev_node) = self.node_mut(prev_idx) {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_idx) = next {
            if let Some(next_node) = self.node_mut(next_idx) {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }
        if let Some(node) = self.node_mut(idx) {
            node.prev = None;
            node.next = None;
        }
    }

    fn link_front(&mut self, idx: usize) {
        let old_head = self.head;
        if let Some(node) = self.node_mut(idx) {
            node.prev = None;
            node.next = old_head;
        } else {
            return;
        }
        if let Some(head_idx) = old_head
            && let Some(head_node) = self.node_mut(head_idx)
        {
            head_node.prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.link_front(idx);
    }
}

impl CacheStorePort for FilesystemCache {
    type Content = FilesystemContentStore;
    type Actions = FilesystemActionStore;

    fn content_store(&self) -> Result<Self::Content, PortError> {
        self.open_content_store()
    }

    fn action_store(
        &self,
        environment: &gang_types::EnvironmentName,
    ) -> Result<Self::Actions, PortError> {
        self.open_action_store(environment)
    }

    /// The filesystem stores are single-process, so only the controller opens them.
    fn worker_access(&self) -> StoreAccess {
        StoreAccess::ViaController
    }

    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError> {
        Ok(None)
    }
}

/// The eviction budgets of the content store and the action cache: the given shares of
/// `capacity`, each rounded down.
#[must_use]
pub(crate) fn budgets(capacity: Bytes, content: Percent, action: Percent) -> (Bytes, Bytes) {
    (content.of_bytes(capacity), action.of_bytes(capacity))
}

/// The content-addressed blob store, under `root`.
fn content_dir(root: &Path) -> PathBuf {
    root.join("cas").join("content")
}

/// Staging area for content-addressed writes, on the same filesystem as the content store.
fn content_temp_dir(root: &Path) -> PathBuf {
    root.join("cas").join("tmp")
}

/// The action cache entries, under `root`.
fn action_dir(root: &Path) -> PathBuf {
    root.join("ac").join("content")
}

/// Staging area for action cache writes, on the same filesystem as the action cache.
fn action_temp_dir(root: &Path) -> PathBuf {
    root.join("ac").join("tmp")
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::test_support::plugin_context;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn config(path: &Path) -> FilesystemCacheConfig {
        FilesystemCacheConfig {
            path: AbsolutePathBuf::try_from(path).expect("absolute cache path"),
            content_store_percent: default_content_store_percent(),
            action_store_percent: default_action_store_percent(),
        }
    }

    fn parse(value: serde_json::Value) -> Result<FilesystemCacheConfig, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[tokio::test]
    async fn filesystem_has_no_provider_measurement() -> TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;
        assert_eq!(cache.measured_usage().await?, None);
        assert!(!root.exists());
        Ok(())
    }

    #[test]
    fn budgets_are_percentages_of_the_capacity_rounded_down() {
        let (content, action) = budgets(
            Bytes::new(1 << 30),
            Percent::from_percent(25),
            Percent::from_percent(2),
        );

        assert_eq!(content, Bytes::new(268_435_456));
        assert_eq!(action, Bytes::new(21_474_836));
    }

    #[test]
    fn first_content_store_creates_the_cas_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        let store = cache.content_store().expect("content store");

        assert!(root.join("cas").join("content").is_dir());
        assert!(root.join("cas").join("tmp").is_dir());
        let capacity = space(&root).expect("statvfs of the cache directory").total;
        let (content_budget, _) = budgets(
            capacity,
            default_content_store_percent(),
            default_action_store_percent(),
        );
        assert_eq!(store.root(), root.join("cas").join("content"));
        assert_eq!(store.budget_bytes(), content_budget.get());
    }

    #[test]
    fn first_action_store_creates_the_ac_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        cache
            .action_store(&gang_types::EnvironmentName::default())
            .expect("action store");

        assert!(root.join("ac").join("content").is_dir());
        assert!(root.join("ac").join("tmp").is_dir());
    }

    #[test]
    fn environment_versions_isolate_actions_but_keep_the_initial_layout() -> TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;
        let initial = cache.action_store(&gang_types::EnvironmentName::default())?;
        assert_eq!(initial.root(), root.join("ac/content"));
        let bumped = gang_types::EnvironmentName::try_from("debian-13/2".to_owned())?;
        let store = cache.action_store(&bumped)?;
        assert_eq!(
            store.root(),
            root.join("environments/debian-13/2/ac/content")
        );
        assert!(root.join("environments/debian-13/2/ac/content").is_dir());
        assert_eq!(cache.content_store()?.root(), root.join("cas/content"));
        Ok(())
    }

    #[test]
    fn directories_are_owner_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        cache.content_store().expect("content store");

        let mode = std::fs::metadata(root.join("cas").join("content"))
            .expect("content directory")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn store_shares_default_to_25_and_2_percent() -> TestResult {
        let parsed = parse(serde_json::json!({ "path": "/var/cache/gang" }))?;

        assert_eq!(parsed.content_store_percent.get(), 25);
        assert_eq!(parsed.action_store_percent.get(), 2);
        Ok(())
    }

    #[test]
    fn relative_cache_path_is_rejected_at_parse() {
        assert!(parse(serde_json::json!({ "path": "relative/cache" })).is_err());
    }

    #[test]
    fn store_shares_that_sum_to_at_most_100_parse() -> TestResult {
        let parsed = parse(serde_json::json!({
            "path": "/var/cache/gang",
            "content_store_percent": 50,
            "action_store_percent": 50,
        }))?;

        assert_eq!(parsed.content_store_percent.get(), 50);
        assert_eq!(parsed.action_store_percent.get(), 50);
        Ok(())
    }

    #[test]
    fn store_shares_summing_above_100_are_rejected_at_parse() {
        let error = parse(serde_json::json!({
            "path": "/var/cache/gang",
            "content_store_percent": 80,
            "action_store_percent": 30,
        }))
        .expect_err("shares sum to 110");

        assert!(error.to_string().contains("must sum to at most 100"));
    }

    #[test]
    fn store_shares_outside_1_to_100_are_rejected_at_parse() {
        for (content, action) in [(0, 2), (25, 0), (101, 2)] {
            let settings = parse(serde_json::json!({
                "path": "/var/cache/gang",
                "content_store_percent": content,
                "action_store_percent": action,
            }));

            assert!(settings.is_err(), "shares {content} and {action}");
        }
    }

    #[test]
    fn cache_path_other_users_can_open_is_refused_and_left_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        std::fs::create_dir(&root).expect("create cache directory");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("chmod cache directory");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        let error = cache
            .content_store()
            .expect_err("a 0755 cache path is refused");

        assert!(matches!(error, PortError::Config(_)));
        let mode = std::fs::metadata(&root)
            .expect("cache directory")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);
        assert!(!root.join("cas").exists());
    }

    #[tokio::test]
    async fn open_content_store_passes_contract_and_reports_usage() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;
        assert!(!root.exists());

        let content = cache.open_content_store()?;
        gang_testkit::content_store_contract(&content).await?;

        let capacity = space(&root)?.total;
        let (expected_budget, _) = budgets(
            capacity,
            default_content_store_percent(),
            default_action_store_percent(),
        );
        let usage = content.usage().ok_or("content store must report usage")?;
        assert_eq!(usage.store, StoreKind::Content);
        assert_eq!(usage.budget, expected_budget.get());
        assert!(usage.used > 0);
        assert_eq!(usage.evicted, 0);
        Ok(())
    }

    #[tokio::test]
    async fn open_action_store_passes_contract_and_reports_usage() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;

        let actions = cache.open_action_store(&EnvironmentName::default())?;
        gang_testkit::action_store_contract(&actions).await?;

        let capacity = space(&root)?.total;
        let (_, expected_budget) = budgets(
            capacity,
            default_content_store_percent(),
            default_action_store_percent(),
        );
        let usage = actions.usage().ok_or("action store must report usage")?;
        assert_eq!(usage.store, StoreKind::Action);
        assert_eq!(usage.budget, expected_budget.get());
        assert!(usage.used > 0);
        assert_eq!(usage.evicted, 0);
        Ok(())
    }

    #[tokio::test]
    async fn open_stores_are_lazy_and_cached_per_instance() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;
        assert!(!root.exists());

        let cas_a = cache.open_content_store()?;
        let cas_b = cache.open_content_store()?;
        let blob = bytes::Bytes::from_static(b"cached-content-handle");
        let blob_digest = Digest::from_bytes(&blob);
        cas_a
            .write(blob_digest, BlobStream::from_bytes(blob.clone()))
            .await?;
        let read_back = cas_b
            .read(blob_digest, ByteRange::FULL)
            .await?
            .ok_or("blob written via cas_a must be visible via cas_b")?;
        assert_eq!(read_back.collect().await?, blob);

        let env = EnvironmentName::default();
        let ac_a = cache.open_action_store(&env)?;
        let ac_b = cache.open_action_store(&env)?;
        let action = Digest::from_bytes(b"cached-action-handle");
        let result = bytes::Bytes::from_static(b"cached-action-result");
        ac_a.put(action, result.clone()).await?;
        assert_eq!(ac_b.get(action).await?, Some(result));
        Ok(())
    }

    #[tokio::test]
    async fn open_action_store_isolates_environments_and_matches_layout() -> gang_testkit::TestResult
    {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let cache = FilesystemCache::build(config(&root), &plugin_context())?;

        let initial_env = EnvironmentName::default();
        let bumped_env = EnvironmentName::try_from("debian-13/2".to_owned())?;
        let initial_ac = cache.open_action_store(&initial_env)?;
        let bumped_ac = cache.open_action_store(&bumped_env)?;

        assert_eq!(initial_ac.root(), root.join("ac/content"));
        assert_eq!(
            bumped_ac.root(),
            root.join("environments/debian-13/2/ac/content")
        );

        let action = Digest::from_bytes(b"shared-action-digest");
        let result_v1 = bytes::Bytes::from_static(b"result-v1");
        let result_v2 = bytes::Bytes::from_static(b"result-v2");

        initial_ac.put(action, result_v1.clone()).await?;
        assert_eq!(bumped_ac.get(action).await?, None);

        bumped_ac.put(action, result_v2.clone()).await?;
        assert_eq!(initial_ac.get(action).await?, Some(result_v1));
        assert_eq!(bumped_ac.get(action).await?, Some(result_v2));

        let expected_v1_file = action_entry_path(&root.join("ac/content"), action);
        let expected_v2_file =
            action_entry_path(&root.join("environments/debian-13/2/ac/content"), action);
        assert!(expected_v1_file.is_file());
        assert!(expected_v2_file.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn action_store_evicts_lru_within_budget_and_handles_oversized()
    -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let content = dir.path().join("ac/content");
        let temp = dir.path().join("ac/tmp");
        let store = FilesystemActionStore::open(&content, &temp, 20)?;

        let a = Digest::from_bytes(b"action-a");
        let b = Digest::from_bytes(b"action-b");
        let c = Digest::from_bytes(b"action-c");
        let d = Digest::from_bytes(b"action-d");

        let val_a = bytes::Bytes::from_static(b"aaaaaaaaaa"); // 10 bytes
        let val_b = bytes::Bytes::from_static(b"bbbbbbbbbb"); // 10 bytes
        let val_c = bytes::Bytes::from_static(b"cccccccccc"); // 10 bytes
        let val_a2 = bytes::Bytes::from_static(b"aaaaaaaaaaaa"); // 12 bytes
        let val_oversized = bytes::Bytes::from_static(b"0123456789012345678901234"); // 25 bytes

        store.put(a, val_a.clone()).await?;
        store.put(b, val_b.clone()).await?;
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 20,
                budget: 20,
                evicted: 0,
            })
        );

        // Touch `a` via `get`, making `b` the LRU entry.
        assert_eq!(store.get(a).await?, Some(val_a));

        // Inserting `c` (10 bytes) evicts `b` (10 bytes), keeping `a` and `c`.
        store.put(c, val_c.clone()).await?;
        assert_eq!(store.get(b).await?, None);
        assert!(!action_entry_path(&content, b).exists());
        assert_eq!(store.get(c).await?, Some(val_c.clone()));
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 20,
                budget: 20,
                evicted: 10,
            })
        );

        // Replacing `a` (10 -> 12 bytes) evicts `c` (10 bytes), not `a` itself.
        store.put(a, val_a2.clone()).await?;
        assert_eq!(store.get(c).await?, None);
        assert_eq!(store.get(a).await?, Some(val_a2.clone()));
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 12,
                budget: 20,
                evicted: 20,
            })
        );

        // An oversized new key bypasses without evicting warm entries.
        store.put(d, val_oversized.clone()).await?;
        assert_eq!(store.get(d).await?, None);
        assert_eq!(store.get(a).await?, Some(val_a2));
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 12,
                budget: 20,
                evicted: 20,
            })
        );

        // Overwriting an existing key with an oversized payload removes the stale entry.
        store.put(a, val_oversized).await?;
        assert_eq!(store.get(a).await?, None);
        assert!(!action_entry_path(&content, a).exists());
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 0,
                budget: 20,
                evicted: 20,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn action_store_startup_scan_cleans_stale_and_evicts_excess() -> gang_testkit::TestResult
    {
        let dir = tempfile::tempdir()?;
        let content = dir.path().join("ac/content");
        let temp = dir.path().join("ac/tmp");

        let a = Digest::from_bytes(b"scan-a");
        let b = Digest::from_bytes(b"scan-b");
        let val_a = bytes::Bytes::from_static(b"1234567890");
        let val_b = bytes::Bytes::from_static(b"abcdefghij");

        {
            let store = FilesystemActionStore::open(&content, &temp, 100)?;
            store.put(a, val_a.clone()).await?;
            // Sleep slightly or touch via put so timestamps differ clearly when reopening.
            std::thread::sleep(std::time::Duration::from_millis(20));
            store.put(b, val_b.clone()).await?;
        }

        // Plant stale temp file, malformed entry, and unknown directory.
        std::fs::write(temp.join(".ac-stale"), b"leftover")?;
        std::fs::create_dir_all(content.join("00"))?;
        std::fs::write(content.join("00/garbage"), b"junk")?;
        std::fs::create_dir_all(content.join("zz"))?;
        std::fs::write(content.join("zz/junk"), b"junk")?;

        // Reopen with budget fitting only one 10-byte entry.
        let reopened = FilesystemActionStore::open(&content, &temp, 10)?;
        assert!(!temp.join(".ac-stale").exists());
        assert!(!content.join("00/garbage").exists());
        assert!(!content.join("zz").exists());
        assert_eq!(
            reopened.usage(),
            Some(EngineStore {
                store: StoreKind::Action,
                used: 10,
                budget: 10,
                evicted: 10,
            })
        );
        // `b` was newer, so `b` remains and `a` was evicted.
        assert_eq!(reopened.get(a).await?, None);
        assert_eq!(reopened.get(b).await?, Some(val_b));
        Ok(())
    }

    #[tokio::test]
    async fn content_store_budget_eviction_via_filesystem_content_store() -> gang_testkit::TestResult
    {
        let dir = tempfile::tempdir()?;
        let cas_root = dir.path().join("cas/content");
        let blob_a = bytes::Bytes::from_static(b"cas-evict-aaaa");
        let blob_b = bytes::Bytes::from_static(b"cas-evict-bbbb");
        let digest_a = Digest::from_bytes(&blob_a);
        let digest_b = Digest::from_bytes(&blob_b);

        let store = FilesystemContentStore::new(cas_root, digest_a.size());
        store
            .write(digest_a, BlobStream::from_bytes(blob_a))
            .await?;
        store
            .write(digest_b, BlobStream::from_bytes(blob_b.clone()))
            .await?;

        assert!(store.read(digest_a, ByteRange::FULL).await?.is_none());
        let got_b = store
            .read(digest_b, ByteRange::FULL)
            .await?
            .ok_or("blob_b should remain resident")?;
        assert_eq!(got_b.collect().await?, blob_b);
        assert_eq!(
            store.usage(),
            Some(EngineStore {
                store: StoreKind::Content,
                used: digest_b.size(),
                budget: digest_a.size(),
                evicted: digest_a.size(),
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn content_store_refuses_oversized_authoritative_write() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let store = FilesystemContentStore::new(dir.path().join("content"), 4);
        let bytes = bytes::Bytes::from_static(b"too large");
        let digest = Digest::from_bytes(&bytes);
        assert!(matches!(
            store.write(digest, BlobStream::from_bytes(bytes)).await,
            Err(PortError::Rejected {
                reason: gang_spi::Rejection::ResourceExhausted,
                ..
            })
        ));
        let mut missing = Vec::new();
        store.find_missing(&[digest], &mut missing).await?;
        assert_eq!(missing, vec![digest]);
        Ok(())
    }

    #[test]
    fn open_stores_refuse_non_private_root_and_leave_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        std::fs::create_dir(&root).expect("create cache directory");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("chmod cache directory");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        assert!(matches!(
            cache.open_content_store(),
            Err(PortError::Config(_))
        ));
        assert!(matches!(
            cache.open_action_store(&EnvironmentName::default()),
            Err(PortError::Config(_))
        ));
        assert!(!root.join("cas").exists());
        assert!(!root.join("ac").exists());
    }
}
