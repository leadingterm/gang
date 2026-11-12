//! Verdicts: what a scenario concludes about the system under test.

use std::borrow::Cow;
use std::fmt;

use crate::errors::{E2eError, Result};

/// How one check ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The system behaved as required.
    Passed,
    /// The system did not behave as required; the text says how.
    Failed(String),
    /// The check does not apply here; the text says why.
    Skipped(String),
}

/// One named verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Check {
    /// What was checked, as a statement that should hold.
    pub name: Cow<'static, str>,
    /// The verdict.
    pub outcome: Outcome,
}

impl Check {
    /// A check that passed.
    pub fn passed(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            outcome: Outcome::Passed,
        }
    }

    /// A check that failed for `reason`.
    pub fn failed(name: impl Into<Cow<'static, str>>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            outcome: Outcome::Failed(reason.into()),
        }
    }

    /// A check that does not apply because of `reason`.
    pub fn skipped(name: impl Into<Cow<'static, str>>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            outcome: Outcome::Skipped(reason.into()),
        }
    }

    /// One skipped check for each of `names`, all because of `reason`.
    #[must_use]
    pub fn skipped_all(names: impl IntoIterator<Item = &'static str>, reason: &str) -> Vec<Self> {
        names
            .into_iter()
            .map(|name| Self::skipped(name, reason))
            .collect()
    }

    /// Passes when `holds`, otherwise fails with `otherwise`.
    pub fn expect(
        name: impl Into<Cow<'static, str>>,
        holds: bool,
        otherwise: impl FnOnce() -> String,
    ) -> Self {
        if holds {
            Self::passed(name)
        } else {
            Self::failed(name, otherwise())
        }
    }

    /// Whether the check failed.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(self.outcome, Outcome::Failed(_))
    }

    /// Whether the check passed.
    #[must_use]
    pub const fn is_passed(&self) -> bool {
        matches!(self.outcome, Outcome::Passed)
    }

    /// Whether the check does not apply here.
    #[must_use]
    pub const fn is_skipped(&self) -> bool {
        matches!(self.outcome, Outcome::Skipped(_))
    }
}

/// Turns the timeout of an awaited condition into a failed check named `name`, so that a system
/// that never reaches the state is a verdict, not a harness error. Other errors pass through.
///
/// # Errors
///
/// Any error of `result` that is not [`E2eError::Timeout`].
pub fn awaited<T>(name: impl Into<Cow<'static, str>>, result: Result<T>) -> Result<Check> {
    match result {
        Ok(_) => Ok(Check::passed(name)),
        Err(error @ E2eError::Timeout { .. }) => Ok(Check::failed(
            name,
            gang_types::error_chain(&error).to_string(),
        )),
        Err(error) => Err(error),
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outcome {
            Outcome::Passed => write!(f, "pass  {}", self.name),
            Outcome::Failed(reason) => write!(f, "FAIL  {}: {reason}", self.name),
            Outcome::Skipped(reason) => write!(f, "skip  {} ({reason})", self.name),
        }
    }
}

/// Every verdict of one run, in the order reached.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct Report {
    /// The verdicts.
    pub checks: Vec<Check>,
}

impl Report {
    /// Appends verdicts.
    pub fn extend(&mut self, checks: impl IntoIterator<Item = Check>) {
        self.checks.extend(checks);
    }

    /// The failed checks.
    pub fn failures(&self) -> impl Iterator<Item = &Check> {
        self.checks.iter().filter(|check| check.is_failure())
    }

    /// Whether no check failed. Skipped checks do not count against a run.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failures().next().is_none()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for check in &self.checks {
            writeln!(f, "{check}")?;
        }
        let failed = self.failures().count();
        write!(f, "{} checks, {failed} failed", self.checks.len())
    }
}
