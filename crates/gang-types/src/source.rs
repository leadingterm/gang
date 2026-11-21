//! Stable, principal-scoped identities for rate and activity history.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{Identity, Principal, ProviderName, SourceIdError};

/// The first eight bytes of SHA-256 of the exact `provider:principal` identity.
///
/// This identifies an authenticated source across invocations without retaining its principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId([u8; 8]);

impl SourceId {
    /// Validates borrowed identity text and hashes it without allocating.
    ///
    /// Splits at the first colon; the principal may itself contain colons and Unicode.
    #[must_use]
    pub fn from_identity_str(identity: &str) -> Option<Self> {
        let (provider, principal) = identity.split_once(':')?;
        ProviderName::validate(provider).ok()?;
        Principal::validate(principal).ok()?;
        Some(Self::from_digest(Sha256::digest(identity.as_bytes())))
    }

    fn from_digest(digest: sha2::digest::Output<Sha256>) -> Self {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&digest[..8]);
        Self(bytes)
    }
}

impl From<&Identity> for SourceId {
    fn from(identity: &Identity) -> Self {
        let mut hash = Sha256::new();
        hash.update(identity.provider.as_str().as_bytes());
        hash.update(b":");
        hash.update(identity.principal.as_str().as_bytes());
        Self::from_digest(hash.finalize())
    }
}

impl AsRef<[u8]> for SourceId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for SourceId {
    type Err = SourceIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != 16
            || !text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(SourceIdError);
        }
        let mut bytes = [0; 8];
        hex::decode_to_slice(text, &mut bytes).map_err(|_| SourceIdError)?;
        Ok(Self(bytes))
    }
}

impl Serialize for SourceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = SourceId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("16 lowercase hexadecimal characters")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{BuildKey, InvocationId};

    #[test]
    fn source_id_is_eight_bytes_with_canonical_sha256_prefix() {
        let identity: Identity = "example:alice".parse().unwrap();
        let id = SourceId::from(&identity);
        let reference = Sha256::digest(b"example:alice");
        assert_eq!(size_of::<SourceId>(), 8);
        assert_eq!(id.as_ref(), &reference[..8]);
        assert_eq!(id.to_string(), hex::encode(&reference[..8]));
        assert_eq!(id.to_string().len(), 16);
        assert_eq!(id.to_string().parse::<SourceId>(), Ok(id));
    }

    #[test]
    fn source_id_ignores_invocation_but_separates_identity_components() {
        let principal = Arc::new("example:alice".parse::<Identity>().unwrap());
        let first = BuildKey {
            principal: Arc::clone(&principal),
            invocation: InvocationId::try_new("first").unwrap(),
        };
        let second = BuildKey {
            principal,
            invocation: InvocationId::try_new("second").unwrap(),
        };
        assert_eq!(
            SourceId::from(first.principal.as_ref()),
            SourceId::from(second.principal.as_ref())
        );
        for other in ["example:bob", "other:alice", "exampl:ealice"] {
            assert_ne!(
                SourceId::from_identity_str("example:alice"),
                SourceId::from_identity_str(other)
            );
        }
        assert_ne!(
            SourceId::from_identity_str("ab:c"),
            SourceId::from_identity_str("a:bc")
        );
    }

    #[test]
    fn borrowed_identity_hash_matches_typed_identity_and_validation() {
        for text in [
            "example:alice",
            "github-actions:repo:org/name:ref:refs/heads/main",
            "example:élève:東京",
        ] {
            let identity: Identity = text.parse().unwrap();
            assert_eq!(
                SourceId::from_identity_str(text),
                Some(SourceId::from(&identity))
            );
        }
        for text in [
            "",
            "example",
            ":alice",
            "example:",
            "EXAMPLE:alice",
            "bad/provider:alice",
            "example:ali\nce",
            "example:ali\u{7f}ce",
        ] {
            assert!(text.parse::<Identity>().is_err());
            assert_eq!(SourceId::from_identity_str(text), None, "{text:?}");
        }
    }

    #[test]
    fn source_id_rejects_noncanonical_hex_and_roundtrips_serde() {
        let id = SourceId::from_identity_str("example:alice").unwrap();
        let encoded = serde_json::to_string(&id).unwrap();
        assert_eq!(encoded, format!("\"{id}\""));
        assert_eq!(serde_json::from_str::<SourceId>(&encoded).unwrap(), id);
        for text in [
            "0123456789abcde",
            "0123456789abcdef0",
            "0123456789abcdeF",
            "0123456789abcdeg",
            "é23456789abcdef",
        ] {
            assert_eq!(text.parse::<SourceId>(), Err(SourceIdError));
            assert!(serde_json::from_str::<SourceId>(&format!("\"{text}\"")).is_err());
        }
    }

    #[test]
    fn borrowed_identity_hash_allocates_nothing_when_warm() {
        let text = "github-actions:repo:org/name:ref:refs/heads/main";
        assert!(SourceId::from_identity_str(text).is_some());
        let counts = allocation_counter::measure(|| {
            for _ in 0..100 {
                std::hint::black_box(SourceId::from_identity_str(text));
            }
        });
        assert_eq!(
            counts.count_total, 0,
            "borrowed source extraction allocated"
        );
    }
}
