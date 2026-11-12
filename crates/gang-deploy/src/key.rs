//! Where a reviewed plan is stored: the key prefix, the object key, and the object version the
//! plan store returned.

use crate::errors::{DeployError, Expected, Result};
use crate::ids::{PrNumber, RepositoryId, RunAttempt, RunId};
use crate::newtype::text_newtype;
use crate::sha::Sha;

/// Value the AWS CLI prints for a missing version id.
const NO_VERSION: &str = "None";

/// Accepts a relative, non-empty key prefix.
fn check_prefix(text: &str) -> Result<()> {
    if text.is_empty() {
        Err(DeployError::EmptyPrefix)
    } else if text.starts_with('/') {
        Err(DeployError::RootedPrefix)
    } else {
        Ok(())
    }
}

/// Accepts a non-empty object key.
fn check_key(text: &str) -> Result<()> {
    if text.is_empty() {
        Err(DeployError::Invalid {
            text: text.to_owned(),
            expected: Expected::PlanKey,
        })
    } else {
        Ok(())
    }
}

/// Accepts a version id the plan store returned for an upload.
fn check_version(text: &str) -> Result<()> {
    if text.is_empty() || text == NO_VERSION {
        Err(DeployError::NoVersion)
    } else {
        Ok(())
    }
}

text_newtype! {
    /// The key prefix of the plan store: not empty and not starting with `/`.
    PlanPrefix, check_prefix
}

text_newtype! {
    /// The object key of a stored plan.
    PlanKey, check_key
}

text_newtype! {
    /// The object version id of a stored plan: not empty and not the AWS CLI's `None`.
    PlanVersion, check_version
}

impl PlanKey {
    /// The key of the plan of attempt `run_attempt` of run `run_id`, for the tree `tree` of the
    /// merge of pull request `pr_number` at head `head` in repository `repository_id`:
    /// `<prefix>/<repository_id>/<pr_number>/<head>/<tree>/<run_id>-<run_attempt>.tfplan`, where
    /// one trailing `/` of the prefix is dropped.
    #[must_use]
    pub fn new(
        prefix: &PlanPrefix,
        repository_id: RepositoryId,
        pr_number: PrNumber,
        head: &Sha,
        tree: &Sha,
        run_id: RunId,
        run_attempt: RunAttempt,
    ) -> Self {
        let prefix = prefix.as_str();
        let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
        Self(format!(
            "{prefix}/{repository_id}/{pr_number}/{head}/{tree}/{run_id}-{run_attempt}.tfplan"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{PlanKey, PlanPrefix, PlanVersion};
    use crate::sha::Sha;

    const HEAD: &str = "1111111111111111111111111111111111111111";
    const TREE: &str = "4444444444444444444444444444444444444444";

    fn key(prefix: &str) -> String {
        PlanKey::new(
            &prefix.parse().unwrap(),
            "42".parse().expect("positive repository id"),
            "7".parse().expect("positive pull request number"),
            &HEAD.parse::<Sha>().unwrap(),
            &TREE.parse::<Sha>().unwrap(),
            "99".parse().expect("positive run id"),
            "2".parse().expect("positive run attempt"),
        )
        .to_string()
    }

    #[test]
    fn plan_key_lays_out_prefix_ids_tree_run_and_attempt() {
        assert_eq!(
            key("plans"),
            format!("plans/42/7/{HEAD}/{TREE}/99-2.tfplan")
        );
    }

    #[test]
    fn plan_key_trims_a_trailing_slash_from_the_prefix() {
        assert_eq!(key("plans/"), key("plans"));
    }

    #[test]
    fn plan_prefix_rejects_an_empty_prefix() {
        let error = "".parse::<PlanPrefix>().unwrap_err();
        assert_eq!(error.to_string(), "plan prefix must not be empty");
    }

    #[test]
    fn plan_prefix_rejects_a_prefix_starting_with_a_slash() {
        let error = "/plans".parse::<PlanPrefix>().unwrap_err();
        assert_eq!(error.to_string(), "plan prefix must not start with /");
    }

    #[test]
    fn plan_version_rejects_an_empty_and_a_none_version_id() {
        for text in ["", "None"] {
            let error = text.parse::<PlanVersion>().unwrap_err();
            assert_eq!(error.to_string(), "plan upload returned no version id");
        }
        assert_eq!("ver-1".parse::<PlanVersion>().unwrap().as_str(), "ver-1");
    }
}
