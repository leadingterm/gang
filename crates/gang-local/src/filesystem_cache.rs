//! The cache stores: the content-addressed store and the action cache, both kept under one directory
//! and described to the build engine as filesystem stores with an eviction budget.
//!
//! The directory is created on first use, not when the plugin is built, so a node that only runs
//! workers never creates it. The `NativeLink` filesystem store is single-process, so workers reach
//! the stores through the controller.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use gang_os::fs::{ensure_private_dir, require_private_dir, space};
use gang_spi::{CacheStorePort, Plugin, PluginContext, PortError, RenderedStoreSpec, StoreAccess};
use gang_types::{AbsolutePathBuf, Bytes, Percent};
use serde::{Deserialize, Serialize};

use crate::errors::LocalError;

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
pub struct FilesystemCache {
    path: PathBuf,
    content_percent: Percent,
    action_percent: Percent,
    budgets: OnceLock<(Bytes, Bytes)>,
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
}

impl CacheStorePort for FilesystemCache {
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
        let (budget, _) = self.prepare().map_err(PortError::from)?;
        content_store_spec(&self.path, budget).map_err(PortError::from)
    }

    fn action_store(&self) -> Result<RenderedStoreSpec, PortError> {
        let (_, budget) = self.prepare().map_err(PortError::from)?;
        action_store_spec(&self.path, budget).map_err(PortError::from)
    }

    /// The `NativeLink` filesystem stores are single-process, so only the controller opens them.
    fn worker_access(&self) -> StoreAccess {
        StoreAccess::ViaController
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

/// Describes the content-addressed store (CAS) under `root` as a filesystem store, evicting least
/// recently used blobs once it holds `budget` bytes.
fn content_store_spec(root: &Path, budget: Bytes) -> Result<RenderedStoreSpec, LocalError> {
    filesystem_store(&content_dir(root), &content_temp_dir(root), budget)
}

/// Describes the action cache under `root` as a filesystem store, evicting entries once it holds
/// `budget` bytes.
fn action_store_spec(root: &Path, budget: Bytes) -> Result<RenderedStoreSpec, LocalError> {
    filesystem_store(&action_dir(root), &action_temp_dir(root), budget)
}

fn filesystem_store(
    content: &Path,
    temp: &Path,
    budget: Bytes,
) -> Result<RenderedStoreSpec, LocalError> {
    Ok(RenderedStoreSpec(serde_json::json!({
        "filesystem": {
            "content_path": utf8(content)?,
            "temp_path": utf8(temp)?,
            "eviction_policy": { "max_bytes": budget.get() },
        }
    })))
}

/// The engine's store description is JSON, so every path in it must be UTF-8.
fn utf8(path: &Path) -> Result<&str, LocalError> {
    path.to_str()
        .ok_or_else(|| LocalError::NonUtf8Path(path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::test_support::plugin_context;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const CONTENT_BUDGET: Bytes = Bytes::new(3 << 20);
    const ACTION_BUDGET: Bytes = Bytes::new(5 << 10);

    fn path_at(spec: &RenderedStoreSpec, key: &str) -> Option<String> {
        spec.0["filesystem"][key].as_str().map(str::to_owned)
    }

    fn max_bytes(spec: &RenderedStoreSpec) -> Option<u64> {
        spec.0["filesystem"]["eviction_policy"]["max_bytes"].as_u64()
    }

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

    #[test]
    fn stores_parse_back_with_their_paths_under_the_root() -> TestResult {
        let dir = tempfile::tempdir()?;

        let content = content_store_spec(dir.path(), CONTENT_BUDGET)?;
        assert_eq!(
            path_at(&content, "content_path"),
            Some(dir.path().join("cas/content").display().to_string())
        );
        assert_eq!(
            path_at(&content, "temp_path"),
            Some(dir.path().join("cas/tmp").display().to_string())
        );

        let action = action_store_spec(dir.path(), ACTION_BUDGET)?;
        assert_eq!(
            path_at(&action, "content_path"),
            Some(dir.path().join("ac/content").display().to_string())
        );
        assert_eq!(
            path_at(&action, "temp_path"),
            Some(dir.path().join("ac/tmp").display().to_string())
        );
        Ok(())
    }

    #[test]
    fn content_and_action_stores_use_different_paths() -> TestResult {
        let dir = tempfile::tempdir()?;

        let content = content_store_spec(dir.path(), CONTENT_BUDGET)?;
        let action = action_store_spec(dir.path(), ACTION_BUDGET)?;

        assert_ne!(content, action);
        assert_ne!(
            path_at(&content, "content_path"),
            path_at(&action, "content_path")
        );
        assert_ne!(
            path_at(&content, "temp_path"),
            path_at(&action, "temp_path")
        );
        Ok(())
    }

    #[test]
    fn each_store_carries_the_budget_it_was_given() -> TestResult {
        let dir = tempfile::tempdir()?;

        let content = content_store_spec(dir.path(), CONTENT_BUDGET)?;
        let action = action_store_spec(dir.path(), ACTION_BUDGET)?;

        assert_eq!(max_bytes(&content), Some(CONTENT_BUDGET.get()));
        assert_eq!(max_bytes(&action), Some(ACTION_BUDGET.get()));
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
    fn non_utf8_root_is_rejected() {
        let root = PathBuf::from(OsStr::from_bytes(b"/tmp/gang-\xff"));

        assert!(matches!(
            content_store_spec(&root, CONTENT_BUDGET),
            Err(LocalError::NonUtf8Path(_))
        ));
        assert!(matches!(
            action_store_spec(&root, ACTION_BUDGET),
            Err(LocalError::NonUtf8Path(_))
        ));
    }

    #[test]
    fn first_content_store_creates_the_cas_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        let spec = cache.content_store().expect("content store");

        assert!(root.join("cas").join("content").is_dir());
        assert!(root.join("cas").join("tmp").is_dir());
        let capacity = space(&root).expect("statvfs of the cache directory").total;
        let (content_budget, _) = budgets(
            capacity,
            default_content_store_percent(),
            default_action_store_percent(),
        );
        assert_eq!(max_bytes(&spec), Some(content_budget.get()));
    }

    #[test]
    fn first_action_store_creates_the_ac_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("cache");
        let cache =
            FilesystemCache::build(config(&root), &plugin_context()).expect("valid cache config");

        cache.action_store().expect("action store");

        assert!(root.join("ac").join("content").is_dir());
        assert!(root.join("ac").join("tmp").is_dir());
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
}
