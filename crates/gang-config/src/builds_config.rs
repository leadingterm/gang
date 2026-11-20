//! The `[builds]` section: limits for in-memory build insight.

use std::num::NonZeroUsize;

use gang_types::NonZeroDuration;
use serde::{Deserialize, Serialize};

/// The default number of builds held in memory: 256.
const DEFAULT_MAX_LIVE: NonZeroUsize = NonZeroUsize::new(256).expect("256 is non-zero");
/// The default time before a build becomes idle: five minutes.
const DEFAULT_IDLE_AFTER: NonZeroDuration = NonZeroDuration::from_mins(5);
/// The default number of observations waiting for the recorder: 65536.
const DEFAULT_QUEUE: NonZeroUsize = NonZeroUsize::new(65_536).expect("65536 is non-zero");
/// The default number of mnemonic rows per build: 32.
const DEFAULT_MNEMONICS: NonZeroUsize = NonZeroUsize::new(32).expect("32 is non-zero");
/// The default number of most-missed targets per build: 20.
const DEFAULT_TARGETS: NonZeroUsize = NonZeroUsize::new(20).expect("20 is non-zero");
/// The default number of slowest actions per build: 20.
const DEFAULT_SLOWEST_ACTIONS: NonZeroUsize = NonZeroUsize::new(20).expect("20 is non-zero");
/// The default number of timeline buckets per build: 120.
const DEFAULT_TIMELINE_BUCKETS: NonZeroUsize = NonZeroUsize::new(120).expect("120 is non-zero");

/// Settings of in-memory build insight. Every field has a default, so the section may be omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BuildsConfig {
    /// Builds held in memory; at the cap the least recently active is dropped. Defaults to 256.
    pub max_live: NonZeroUsize,
    /// Time without an observation after which a build is idle. Defaults to five minutes.
    pub idle_after: NonZeroDuration,
    /// Observations waiting for the recorder; more are counted as unrecorded. Defaults to 65536.
    pub queue: NonZeroUsize,
    /// Mnemonic rows per build; the rest fold into other mnemonics. Defaults to 32.
    pub mnemonics: NonZeroUsize,
    /// Most-missed targets per build. Defaults to 20.
    pub targets: NonZeroUsize,
    /// Slowest actions per build. Defaults to 20.
    pub slowest_actions: NonZeroUsize,
    /// Timeline buckets per build. Defaults to 120.
    pub timeline_buckets: NonZeroUsize,
}

impl Default for BuildsConfig {
    fn default() -> Self {
        Self {
            max_live: DEFAULT_MAX_LIVE,
            idle_after: DEFAULT_IDLE_AFTER,
            queue: DEFAULT_QUEUE,
            mnemonics: DEFAULT_MNEMONICS,
            targets: DEFAULT_TARGETS,
            slowest_actions: DEFAULT_SLOWEST_ACTIONS,
            timeline_buckets: DEFAULT_TIMELINE_BUCKETS,
        }
    }
}
