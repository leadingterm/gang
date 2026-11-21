//! Validated identity of the environment in which an action executes.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::{ActionEnvironmentError, EnvironmentNameError};

const INITIAL_ENVIRONMENT: &str = "default/1";

/// A server-owned, deliberately versioned action environment name.
///
/// Names consist of slash-separated safe path components, for example `debian-13/1`.
/// Security-only image rebuilds preserve the name. The initial `default/1` name
/// deliberately retains the existing unprefixed action cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EnvironmentName(String);

impl EnvironmentName {
    /// The action-cache subdirectory, absent for the initial warm-cache environment.
    #[must_use]
    pub fn cache_namespace(&self) -> Option<&str> {
        (self.0 != INITIAL_ENVIRONMENT).then_some(self.0.as_str())
    }

    /// The configured environment version.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for EnvironmentName {
    fn default() -> Self {
        Self(INITIAL_ENVIRONMENT.to_owned())
    }
}

impl TryFrom<String> for EnvironmentName {
    type Error = EnvironmentNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !value.contains('/')
            || !value.split('/').all(|part| {
                !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    })
            })
        {
            return Err(EnvironmentNameError);
        }
        Ok(Self(value))
    }
}

impl From<EnvironmentName> for String {
    fn from(value: EnvironmentName) -> Self {
        value.0
    }
}

#[cfg(test)]
mod environment_name_tests {
    use super::EnvironmentName;

    #[test]
    fn initial_environment_is_warm_and_reviewed_bumps_are_isolated() {
        let initial = EnvironmentName::default();
        assert_eq!(initial.as_str(), "default/1");
        assert_eq!(initial.cache_namespace(), None);
        let bumped = EnvironmentName::try_from("debian-13/2".to_owned()).expect("versioned name");
        assert_eq!(bumped.cache_namespace(), Some("debian-13/2"));
        assert_eq!(
            serde_json::to_string(&bumped).expect("serialize"),
            "\"debian-13/2\""
        );
        assert_eq!(
            serde_json::from_str::<EnvironmentName>("\"debian-13/2\"").expect("deserialize"),
            bumped
        );
    }

    #[test]
    fn environment_names_cannot_escape_the_cache_namespace() {
        for value in [
            "",
            "unversioned",
            "/1",
            "../1",
            "debian/../1",
            "debian//1",
            "debian/1/",
            "debian\\1",
        ] {
            assert!(
                EnvironmentName::try_from(value.to_owned()).is_err(),
                "{value}"
            );
        }
    }
}

/// A rootfs digest or an explicit host environment assertion.
///
/// Rootfs identities are exactly 64 lowercase hexadecimal characters. Host
/// assertions use `host:` followed by ASCII letters, digits, `.`, `_` or `-`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActionEnvironment(String);

impl ActionEnvironment {
    /// Returns the canonical identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this identity names an immutable rootfs.
    #[must_use]
    pub fn is_rootfs(&self) -> bool {
        !self.0.starts_with("host:")
    }
}

impl AsRef<str> for ActionEnvironment {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ActionEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn validate(text: &str) -> Result<(), ActionEnvironmentError> {
    let valid = if let Some(host) = text.strip_prefix("host:") {
        !host.is_empty()
            && host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    } else {
        text.len() == 64
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if valid {
        Ok(())
    } else {
        Err(ActionEnvironmentError)
    }
}

impl FromStr for ActionEnvironment {
    type Err = ActionEnvironmentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        validate(text)?;
        Ok(Self(text.to_owned()))
    }
}

impl TryFrom<String> for ActionEnvironment {
    type Error = ActionEnvironmentError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        validate(&text)?;
        Ok(Self(text))
    }
}

impl serde::Serialize for ActionEnvironment {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for ActionEnvironment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EnvironmentVisitor;

        impl serde::de::Visitor<'_> for EnvironmentVisitor {
            type Value = ActionEnvironment;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an action environment identity")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map_err(E::custom)
            }

            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                ActionEnvironment::try_from(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_string(EnvironmentVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_identity_reuses_its_allocation() {
        let text = String::from("host:example");
        let ptr = text.as_ptr();
        let identity = ActionEnvironment::try_from(text).unwrap();
        assert_eq!(identity.as_str().as_ptr(), ptr);
        let value = serde_json::Value::String("host:example".to_owned());
        assert_eq!(
            serde_json::from_value::<ActionEnvironment>(value).unwrap(),
            identity
        );
        assert!(ActionEnvironment::try_from(String::from("host:")).is_err());
    }

    #[test]
    fn identities_round_trip_and_distinguish_namespaces() {
        let digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        for (text, rootfs) in [(digest, true), ("host:local-1.example_test", false)] {
            let identity: ActionEnvironment = text.parse().unwrap();
            assert_eq!(identity.as_str(), text);
            assert_eq!(identity.as_ref(), text);
            assert_eq!(identity.to_string(), text);
            assert_eq!(identity.is_rootfs(), rootfs);
            let json = serde_json::to_string(&identity).unwrap();
            assert_eq!(
                serde_json::from_str::<ActionEnvironment>(&json).unwrap(),
                identity
            );
        }
    }

    #[test]
    fn rejects_invalid_identity_text() {
        for text in [
            "",
            "host:",
            "host:two words",
            "host:a/b",
            "host:a\n",
            "0123",
            "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
        ] {
            assert!(text.parse::<ActionEnvironment>().is_err(), "{text:?}");
        }
    }
}
