use std::str::FromStr;

use gang_types::InvocationId;
use gang_wire::{Build, name};

/// A conjunction of build fields, or an empty filter matching every build.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildFilter {
    principal: Option<String>,
    invocation: Option<String>,
    correlated: Option<String>,
}

/// A malformed build filter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid build filter")]
pub struct BuildFilterError;

impl FromStr for BuildFilter {
    type Err = BuildFilterError;

    fn from_str(mut text: &str) -> Result<Self, Self::Err> {
        let mut filter = Self::default();
        if text.is_empty() {
            return Ok(filter);
        }
        loop {
            let (field, rest) = text.trim_start().split_once('=').ok_or(BuildFilterError)?;
            let field = field.trim_end();
            let rest = rest
                .trim_start()
                .strip_prefix('"')
                .ok_or(BuildFilterError)?;
            let (value, rest) = rest.split_once('"').ok_or(BuildFilterError)?;
            let (slot, canonical) = match field {
                "principal" => {
                    let identity =
                        name::parse::<name::Principals>(value).map_err(|_| BuildFilterError)?;
                    (
                        &mut filter.principal,
                        name::of::<name::Principals>(&identity),
                    )
                }
                "invocationId" => (
                    &mut filter.invocation,
                    InvocationId::try_new(value)
                        .map_err(|_| BuildFilterError)?
                        .to_string(),
                ),
                "correlatedInvocationsId" => (
                    &mut filter.correlated,
                    InvocationId::try_new(value)
                        .map_err(|_| BuildFilterError)?
                        .to_string(),
                ),
                _ => return Err(BuildFilterError),
            };
            if slot.replace(canonical).is_some() {
                return Err(BuildFilterError);
            }
            if rest.is_empty() {
                return Ok(filter);
            }
            text = rest.strip_prefix(" AND ").ok_or(BuildFilterError)?;
        }
    }
}

impl BuildFilter {
    /// Returns whether every specified field matches the build.
    #[must_use]
    pub fn matches(&self, build: &Build) -> bool {
        self.principal
            .as_ref()
            .is_none_or(|v| v == &build.principal)
            && self
                .invocation
                .as_ref()
                .is_none_or(|v| v == &build.invocation_id)
            && self
                .correlated
                .as_ref()
                .is_none_or(|v| v == &build.correlated_invocations_id)
    }
}
