//! The `[execution]` section: how `gang exec` prepares an action's inputs.

use serde::{Deserialize, Serialize};

/// The `[execution]` section: how `gang exec` prepares an action before it runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Whether action inputs stay hard links to the cached files. Defaults to `keep`.
    #[serde(default)]
    pub input_links: InputLinks,
    /// How read-only input trees are staged into an action directory. Defaults to `bind`.
    #[serde(default)]
    pub input_trees: InputTrees,
    /// Public egress policy, required by fleets that grant network access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<gang_types::PublicEgressPolicy>,
}

/// Whether `gang exec` leaves an action's inputs hard-linked to the cached files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputLinks {
    /// Inputs stay hard links to the cached files from the worker cache.
    #[default]
    Keep,
    /// Every hard-linked input is replaced by a reflink or copy before the action runs.
    Break,
}

/// How read-only input trees are staged into an action directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputTrees {
    /// Read-only input trees are bind-mounted when the sandbox supports read-only binds.
    #[default]
    Bind,
    /// Input trees are hard-linked into the action directory with no tree cache.
    Link,
}

#[cfg(test)]
mod tests {
    use super::{ExecutionConfig, InputLinks, InputTrees};

    #[test]
    fn input_trees_defaults_to_bind_and_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let default: ExecutionConfig = toml::from_str("")?;
        assert_eq!(default.input_links, InputLinks::Keep);
        assert_eq!(default.input_trees, InputTrees::Bind);

        let linked: ExecutionConfig = toml::from_str("input_trees = \"link\"")?;
        assert_eq!(linked.input_trees, InputTrees::Link);
        let rendered = toml::to_string(&linked)?;
        assert!(rendered.contains("input_trees = \"link\""), "{rendered}");
        assert_eq!(toml::from_str::<ExecutionConfig>(&rendered)?, linked);

        assert!(toml::from_str::<ExecutionConfig>("input_trees = \"copy\"").is_err());
        Ok(())
    }
}
