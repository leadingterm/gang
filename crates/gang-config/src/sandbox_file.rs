//! The sandbox file the worker writes beside the `gang-exec` entrypoint and `gang exec` reads
//! back from there.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use gang_types::AbsolutePathBuf;
use serde::{Deserialize, Serialize};

use crate::execution_config::ExecutionConfig;
use crate::plugin_set::{PluginSet, SandboxSelection};

/// The file name of the sandbox file: a fixed sibling of the `gang-exec` entrypoint.
pub const SANDBOX_FILE_NAME: &str = "sandbox.toml";

/// Why a sandbox file cannot be parsed, validated, or rendered.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub enum SandboxFileError {
    /// The sandbox file text is not valid TOML or does not match its schema; unknown sandbox
    /// plugins and unknown fields are reported here with the accepted keys.
    #[error("invalid sandbox file")]
    Parse(#[source] toml::de::Error),
    /// A configuration could not be rendered as TOML.
    #[error("cannot render configuration")]
    Render(#[from] toml::ser::Error),
    /// The sandbox file's selected sandbox has no `[sandbox.<plugin>]` section.
    #[error("selected sandbox `{0}` has no [sandbox.{0}] section")]
    #[fix("add the [sandbox.<plugin>] section, or select a configured sandbox")]
    SelectedNotConfigured(String),
}

/// The configuration a worker hands to `gang exec`: the work directory, the selected sandbox,
/// the sandbox plugin sections, the `[execution]` section and the `[limits.<plugin>]` section
/// when there is one, without the rest of `gang.toml`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct SandboxFile<P: PluginSet> {
    /// `NativeLink`'s work directory root: each action gets a directory directly under it.
    pub work_directory: AbsolutePathBuf,
    /// The sandbox of the worker's fleet: the one every action runs in.
    pub selected: P::SandboxKey,
    /// The sandbox plugin sections.
    pub sandbox: P::Sandbox,
    /// The `[execution]` section.
    #[serde(default)]
    pub execution: ExecutionConfig,
    /// The `[limits.<plugin>]` section, when one is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<P::Limits>,
}

impl<P: PluginSet> SandboxFile<P> {
    /// The path of the sandbox file for a `gang-exec` entrypoint: the sibling the worker writes
    /// and no action can write.
    #[must_use]
    pub fn beside(entrypoint: &Path) -> PathBuf {
        entrypoint.with_file_name(SANDBOX_FILE_NAME)
    }

    /// Renders the file as TOML.
    ///
    /// # Errors
    ///
    /// [`SandboxFileError::Render`] when the sections cannot be serialized as TOML.
    pub fn to_toml(&self) -> Result<String, SandboxFileError> {
        Ok(toml::to_string(self)?)
    }

    /// Checks the references between sections that serde cannot express.
    fn validate(self) -> Result<Self, SandboxFileError> {
        if !self.sandbox.is_configured(self.selected) {
            return Err(SandboxFileError::SelectedNotConfigured(
                self.selected.to_string(),
            ));
        }
        Ok(self)
    }
}

impl<P: PluginSet> FromStr for SandboxFile<P> {
    type Err = SandboxFileError;

    /// Parses and validates the text of a sandbox file.
    ///
    /// # Errors
    ///
    /// [`SandboxFileError::Parse`] for malformed TOML, unknown fields and unknown sandbox
    /// plugins, and [`SandboxFileError::SelectedNotConfigured`] when the selected sandbox
    /// has no section.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        toml::from_str::<Self>(text)
            .map_err(SandboxFileError::Parse)?
            .validate()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use gang_types::Fix as _;

    use super::SandboxFileError;

    #[test]
    fn parse_failure_has_no_fix_and_does_not_repeat_its_source() {
        let source = toml::from_str::<toml::Table>("a = ").expect_err("invalid TOML");
        let error = SandboxFileError::Parse(source);
        assert!(error.fix().is_none());
        let source = error
            .source()
            .expect("parse error has a source")
            .to_string();
        assert!(!source.is_empty());
        assert!(!error.to_string().contains(&source));
    }

    #[test]
    fn missing_selected_section_fix_names_the_section_to_add() {
        let error = SandboxFileError::SelectedNotConfigured("bwrap".to_owned());
        let fix = error.fix().expect("the variant has a fix");
        assert!(fix.contains("[sandbox.<plugin>]"), "{fix}");
        assert!(!fix.contains('\n'), "{fix}");
    }
}
