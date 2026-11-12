//! The identity a worker presents to the mock IMDS: its instance id and the secret that the server
//! generated for that instance at launch.

use gang_types::InstanceId;

use crate::errors::LocalError;
use crate::processes::{INSTANCE_ENV, INSTANCE_SECRET_ENV};

/// Random bytes behind each secret, hex encoded into twice as many characters.
const SECRET_BYTES: usize = 32;

gang_types::define_secret! {
    /// The bearer secret that proves a request comes from the worker launched for one instance.
    /// The secret is wiped on drop.
    InstanceSecret
}

impl InstanceSecret {
    /// Draws a fresh secret from the OS random source.
    pub(crate) fn generate() -> Result<Self, LocalError> {
        let mut bytes = [0_u8; SECRET_BYTES];
        getrandom::fill(&mut bytes).map_err(|source| LocalError::Random(source.to_string()))?;
        Ok(Self::new(hex::encode(bytes)))
    }
}

/// The instance and secret a worker process was launched with, read from its environment.
#[derive(Debug, Clone)]
pub(crate) struct WorkerIdentity {
    /// The instance this worker is.
    pub(crate) instance: InstanceId,
    /// The secret the worker presents with every request to the mock IMDS.
    pub(crate) secret: InstanceSecret,
}

impl WorkerIdentity {
    /// Reads the identity from `INSTANCE_ENV` and `INSTANCE_SECRET_ENV`. A variable that is not valid
    /// Unicode counts as missing.
    pub(crate) fn from_environment() -> Result<Self, LocalError> {
        Self::from_values(
            std::env::var(INSTANCE_ENV).ok().as_deref(),
            std::env::var(INSTANCE_SECRET_ENV).ok().as_deref(),
        )
    }

    /// Builds the identity from the two variable values, `None` meaning the variable is unset.
    pub(crate) fn from_values(
        instance: Option<&str>,
        secret: Option<&str>,
    ) -> Result<Self, LocalError> {
        let instance = instance.ok_or(LocalError::NotAWorker {
            variable: INSTANCE_ENV,
        })?;
        let secret = secret.ok_or(LocalError::NotAWorker {
            variable: INSTANCE_SECRET_ENV,
        })?;
        Ok(Self {
            instance: InstanceId::try_new(instance)?,
            secret: InstanceSecret::new(secret),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn generated_secrets_are_64_lowercase_hex_characters_and_differ() -> TestResult {
        let first = InstanceSecret::generate()?;
        let second = InstanceSecret::generate()?;

        for secret in [&first, &second] {
            assert_eq!(secret.expose().len(), 64);
            assert!(
                secret
                    .expose()
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
        }
        assert!(!first.matches(second.expose()));
        Ok(())
    }

    #[test]
    fn matches_accepts_only_the_same_value() {
        let secret = InstanceSecret::new("secret-1");

        assert!(secret.matches("secret-1"));
        assert!(!secret.matches("secret-2"));
        assert!(!secret.matches("secret-10"));
        assert!(!secret.matches(""));
    }

    #[test]
    fn secrets_match_the_secret_text() {
        assert!(InstanceSecret::new("a").matches(InstanceSecret::new("a").expose()));
        assert!(!InstanceSecret::new("a").matches(InstanceSecret::new("b").expose()));
    }

    #[test]
    fn secret_serializes_as_a_plain_string() -> TestResult {
        let secret = InstanceSecret::new("secret-1");
        let json = serde_json::to_string(&secret)?;
        assert_eq!(json, "\"secret-1\"");
        let decoded: InstanceSecret = serde_json::from_str(&json)?;
        assert!(secret.matches(decoded.expose()));
        Ok(())
    }

    #[test]
    fn debug_hides_the_secret() -> TestResult {
        let secret = InstanceSecret::new("hunter2");

        assert_eq!(format!("{secret:?}"), "InstanceSecret(<redacted>)");
        let identity = WorkerIdentity {
            instance: InstanceId::try_new("local-1")?,
            secret,
        };
        assert!(!format!("{identity:?}").contains("hunter2"));
        Ok(())
    }

    #[test]
    fn from_values_names_the_missing_instance_variable() {
        let error = WorkerIdentity::from_values(None, Some("secret-1"))
            .expect_err("the instance is missing");

        assert!(matches!(
            error,
            LocalError::NotAWorker { variable } if variable == INSTANCE_ENV
        ));
    }

    #[test]
    fn from_values_names_the_missing_secret_variable() {
        let error =
            WorkerIdentity::from_values(Some("local-1"), None).expect_err("the secret is missing");

        assert!(matches!(
            error,
            LocalError::NotAWorker { variable } if variable == INSTANCE_SECRET_ENV
        ));
    }

    #[test]
    fn from_values_rejects_an_invalid_instance_id() {
        let error = WorkerIdentity::from_values(Some("Not An Id"), Some("secret-1"))
            .expect_err("the instance id is invalid");

        assert!(matches!(error, LocalError::Name(_)));
    }

    #[test]
    fn from_values_builds_the_identity() -> TestResult {
        let identity = WorkerIdentity::from_values(Some("local-1"), Some("secret-1"))?;

        assert_eq!(identity.instance.as_str(), "local-1");
        assert!(identity.secret.matches("secret-1"));
        Ok(())
    }
}
