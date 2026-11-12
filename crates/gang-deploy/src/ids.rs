//! GitHub identifiers: the repository name and the numeric ids of repositories, pull requests,
//! workflow runs and comments.

use crate::errors::{DeployError, Expected, Result};
use crate::newtype::{number_newtype, text_newtype};

/// Accepts `owner/name`: two non-empty parts and exactly one slash.
fn check_repository(text: &str) -> Result<()> {
    match text.split_once('/') {
        Some((owner, name)) if !owner.is_empty() && !name.is_empty() && !name.contains('/') => {
            Ok(())
        }
        _ => Err(DeployError::Invalid {
            text: text.to_owned(),
            expected: Expected::Repository,
        }),
    }
}

text_newtype! {
    /// A GitHub repository as `owner/name`, as `GITHUB_REPOSITORY` names it.
    Repository, check_repository
}

number_newtype! {
    /// The numeric id of a GitHub repository, as `GITHUB_REPOSITORY_ID` gives it.
    RepositoryId
}

number_newtype! {
    /// The number of a pull request.
    PrNumber
}

number_newtype! {
    /// The id of a workflow run.
    RunId
}

number_newtype! {
    /// The attempt of a workflow run, from 1.
    RunAttempt
}

number_newtype! {
    /// The id of an issue comment.
    CommentId
}

#[cfg(test)]
mod tests {
    use super::{PrNumber, Repository};

    #[test]
    fn repository_accepts_owner_and_name() {
        assert_eq!(
            "example-org/gang".parse::<Repository>().unwrap().as_str(),
            "example-org/gang"
        );
    }

    #[test]
    fn repository_rejects_a_name_without_owner_and_extra_slashes() {
        for text in ["gang", "/gang", "example-org/", "a/b/c", ""] {
            let error = text.parse::<Repository>().unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("`{text}` is not an owner/name repository")
            );
        }
    }

    #[test]
    fn numbers_parse_from_digits_and_serialize_as_json_numbers() {
        let number: PrNumber = "7".parse().unwrap();
        assert_eq!(serde_json::to_string(&number).unwrap(), "7");
        assert!("seven".parse::<PrNumber>().is_err());
        assert!(serde_json::from_str::<PrNumber>("\"7\"").is_err());
    }

    #[test]
    fn ids_reject_zero() {
        macro_rules! reject_zero {
            ($($id:ty),+ $(,)?) => {$(
                assert!(matches!(
                    "0".parse::<$id>(),
                    Err(crate::errors::DeployError::Invalid {
                        expected: crate::errors::Expected::PositiveNumber,
                        ..
                    })
                ));
                assert!(serde_json::from_str::<$id>("0").is_err());
            )+};
        }
        reject_zero!(
            super::RepositoryId,
            super::PrNumber,
            super::RunId,
            super::RunAttempt,
            super::CommentId,
        );
    }
}
