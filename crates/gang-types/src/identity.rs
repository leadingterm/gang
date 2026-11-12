//! The identity a credential is issued to: the provider that vouched for it and the principal
//! that provider names.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{IdentityError, Principal, ProviderName};

/// A principal scoped to the identity provider that names it.
///
/// Renders as `provider:principal`. The provider is a slug and never contains `:`, so parsing
/// splits at the first `:`; a principal may contain `:` itself, as in
/// `repo:org/name:ref:refs/heads/main`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// The identity provider that names the principal.
    pub provider: ProviderName,
    /// The principal, as the provider names it.
    pub principal: Principal,
}

impl Identity {
    /// Pairs a provider with the principal it names.
    #[must_use]
    pub fn new(provider: ProviderName, principal: Principal) -> Self {
        Self {
            provider,
            principal,
        }
    }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.provider, self.principal)
    }
}

impl FromStr for Identity {
    type Err = IdentityError;

    /// Parses `provider:principal`, splitting at the first `:`.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::MissingSeparator`] when there is no `:`,
    /// [`IdentityError::Provider`] when the text before the first `:` is not a valid provider
    /// name, or [`IdentityError::Principal`] when the text after it is not a valid principal.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((provider, principal)) = value.split_once(':') else {
            return Err(IdentityError::MissingSeparator {
                input: value.to_owned(),
            });
        };
        Ok(Self::new(
            ProviderName::try_new(provider).map_err(IdentityError::Provider)?,
            Principal::try_new(principal).map_err(IdentityError::Principal)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NameError;

    fn identity(provider: &str, principal: &str) -> Identity {
        Identity::new(
            ProviderName::try_new(provider).expect("test provider is a valid name"),
            Principal::try_new(principal).expect("test principal is a valid name"),
        )
    }

    #[test]
    fn display_and_parse_round_trip() {
        let original = identity("aws-workers", "dev@example.com");
        let text = original.to_string();
        assert_eq!(text, "aws-workers:dev@example.com");
        assert_eq!(text.parse::<Identity>(), Ok(original));
    }

    #[test]
    fn parse_splits_at_first_colon_so_principal_may_contain_colons() {
        let parsed: Identity = "github-actions:repo:org/name:ref:refs/heads/main"
            .parse()
            .expect("text with a valid provider and colons in the principal parses");
        assert_eq!(parsed.provider.as_str(), "github-actions");
        assert_eq!(
            parsed.principal.as_str(),
            "repo:org/name:ref:refs/heads/main"
        );
        assert_eq!(
            parsed.to_string(),
            "github-actions:repo:org/name:ref:refs/heads/main"
        );
    }

    #[test]
    fn parse_rejects_text_without_separator() {
        assert_eq!(
            "aws-workers".parse::<Identity>(),
            Err(IdentityError::MissingSeparator {
                input: "aws-workers".to_owned(),
            })
        );
    }

    #[test]
    fn parse_rejects_invalid_provider() {
        assert_eq!(
            "AWS:dev".parse::<Identity>(),
            Err(IdentityError::Provider(NameError::InvalidCharacter {
                kind: "provider name",
                character: 'A',
            }))
        );
        assert_eq!(
            ":dev".parse::<Identity>(),
            Err(IdentityError::Provider(NameError::Empty {
                kind: "provider name",
            }))
        );
    }

    #[test]
    fn parse_rejects_empty_principal() {
        assert_eq!(
            "aws-workers:".parse::<Identity>(),
            Err(IdentityError::Principal(NameError::Empty {
                kind: "principal",
            }))
        );
    }

    #[test]
    fn serializes_as_provider_and_principal_fields() {
        let json = serde_json::to_string(&identity("aws-workers", "dev@example.com"))
            .expect("identity serializes to JSON");
        assert_eq!(
            json,
            r#"{"provider":"aws-workers","principal":"dev@example.com"}"#
        );
    }

    #[test]
    fn deserializes_from_provider_and_principal_fields() {
        let parsed: Identity =
            serde_json::from_str(r#"{"provider":"aws-workers","principal":"dev@example.com"}"#)
                .expect("well-formed identity JSON deserializes");
        assert_eq!(parsed, identity("aws-workers", "dev@example.com"));
    }

    #[test]
    fn deserialize_rejects_unknown_and_invalid_fields() {
        assert!(
            serde_json::from_str::<Identity>(
                r#"{"provider":"aws-workers","principal":"dev","extra":1}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Identity>(r#"{"provider":"AWS","principal":"dev"}"#).is_err()
        );
        assert!(serde_json::from_str::<Identity>(r#"{"provider":"aws-workers"}"#).is_err());
    }

    #[test]
    fn orders_by_provider_then_principal() {
        let early = identity("aws-workers", "zed");
        let late = identity("github-actions", "alice");
        assert!(early < late);
        assert!(identity("aws-workers", "alice") < identity("aws-workers", "bob"));
    }
}
