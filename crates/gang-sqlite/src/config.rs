use std::path::PathBuf;

use gang_types::NonZeroDuration;
use serde::{Deserialize, Serialize};

/// The `busy_timeout` used when the configuration omits it: five seconds.
const DEFAULT_BUSY_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(5);

/// How strictly commits are flushed to disk, the `PRAGMA synchronous` level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Synchronous {
    /// Safe against application crashes and fast under WAL. A power loss may roll back the
    /// most recent commits.
    #[default]
    Normal,
    /// Also survives power loss, at the cost of an fsync per commit.
    Full,
}

impl Synchronous {
    /// The level keyword for `PRAGMA synchronous`. Fixed text, never user input.
    #[must_use]
    pub const fn as_pragma(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }
}

/// How the database journals its writes, the `PRAGMA journal_mode` level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalMode {
    /// Write-ahead logging: readers do not block the writer, and commits append to a log.
    #[default]
    Wal,
    /// The rollback journal: the classic mode, rewriting the database file on each commit.
    Delete,
}

impl JournalMode {
    /// The mode keyword for `PRAGMA journal_mode`. Fixed text, never user input.
    #[must_use]
    pub const fn as_pragma(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Delete => "DELETE",
        }
    }
}

/// Settings of the `SQLite` state store, from `[database.sqlite]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    /// The database file. Created on first use when missing.
    pub path: PathBuf,
    /// How long a statement waits for a competing writer before failing. Defaults to
    /// five seconds when omitted.
    #[serde(default = "default_busy_timeout")]
    pub busy_timeout: NonZeroDuration,
    /// How strictly commits are flushed to disk. Defaults to [`Synchronous::Normal`].
    #[serde(default)]
    pub synchronous: Synchronous,
    /// How the database journals its writes. Defaults to [`JournalMode::Wal`].
    #[serde(default)]
    pub journal_mode: JournalMode,
}

impl SqliteConfig {
    /// A configuration for the database at `path`, with every other setting at its default.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            synchronous: Synchronous::default(),
            journal_mode: JournalMode::default(),
        }
    }
}

fn default_busy_timeout() -> NonZeroDuration {
    DEFAULT_BUSY_TIMEOUT
}
