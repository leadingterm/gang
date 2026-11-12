//! The `[execution]` section: how `gang exec` prepares an action's inputs.

use serde::{Deserialize, Serialize};

/// The `[execution]` section: how `gang exec` prepares an action before it runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Whether action inputs stay hard links to the cached files. Defaults to `keep`.
    #[serde(default)]
    pub input_links: InputLinks,
}

/// Whether `gang exec` leaves an action's inputs hard-linked to the cached files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputLinks {
    /// Inputs stay hard links to the cached files, as `NativeLink` provides them.
    #[default]
    Keep,
    /// Every hard-linked input is replaced by a reflink or copy before the action runs.
    Break,
}
