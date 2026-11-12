//! SDK client tunables, read from `[provisioner.aws.client]` and `[signer.kms.client]`.

use std::num::NonZeroU32;

use gang_types::NonZeroDuration;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Bounds and retries for each AWS SDK client. Every key has a default, and an empty table
/// selects all of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClientSettingsDoc")]
pub struct ClientSettings {
    /// Bound on each AWS operation. Defaults to 10 seconds.
    pub request_timeout: NonZeroDuration,
    /// Attempts per AWS operation, including the first. Defaults to 3.
    pub max_attempts: NonZeroU32,
    /// Bound on establishing each connection. Defaults to 3100 milliseconds.
    pub connect_timeout: NonZeroDuration,
    /// Base delay of the exponential backoff between attempts of one operation. Defaults to one second.
    pub retry_initial_backoff: NonZeroDuration,
    /// Ceiling of that backoff. Never less than `retry_initial_backoff`. Defaults to 20 seconds.
    pub retry_max_backoff: NonZeroDuration,
}

impl Default for ClientSettings {
    fn default() -> Self {
        Self {
            request_timeout: NonZeroDuration::from_secs(10),
            max_attempts: NonZeroU32::MIN.saturating_add(2),
            connect_timeout: NonZeroDuration::from_millis(3100),
            retry_initial_backoff: NonZeroDuration::from_secs(1),
            retry_max_backoff: NonZeroDuration::from_secs(20),
        }
    }
}

/// A client table whose values are not consistent with each other.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum ClientSettingsError {
    /// The backoff ceiling is below the backoff base.
    #[error("retry_max_backoff must be at least retry_initial_backoff")]
    BackoffOrder,
}

/// The wire form of [`ClientSettings`], checked by its conversion. Keys are as in the config.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClientSettingsDoc {
    /// Bound on each AWS operation.
    request_timeout: NonZeroDuration,
    /// Attempts per AWS operation, including the first.
    max_attempts: NonZeroU32,
    /// Bound on establishing each connection.
    connect_timeout: NonZeroDuration,
    /// Base delay of the exponential backoff.
    retry_initial_backoff: NonZeroDuration,
    /// Ceiling of the exponential backoff.
    retry_max_backoff: NonZeroDuration,
}

impl Default for ClientSettingsDoc {
    fn default() -> Self {
        ClientSettings::default().into()
    }
}

impl From<ClientSettings> for ClientSettingsDoc {
    fn from(settings: ClientSettings) -> Self {
        Self {
            request_timeout: settings.request_timeout,
            max_attempts: settings.max_attempts,
            connect_timeout: settings.connect_timeout,
            retry_initial_backoff: settings.retry_initial_backoff,
            retry_max_backoff: settings.retry_max_backoff,
        }
    }
}

impl TryFrom<ClientSettingsDoc> for ClientSettings {
    type Error = ClientSettingsError;

    fn try_from(doc: ClientSettingsDoc) -> Result<Self, Self::Error> {
        if doc.retry_max_backoff < doc.retry_initial_backoff {
            return Err(ClientSettingsError::BackoffOrder);
        }
        Ok(Self {
            request_timeout: doc.request_timeout,
            max_attempts: doc.max_attempts,
            connect_timeout: doc.connect_timeout,
            retry_initial_backoff: doc.retry_initial_backoff,
            retry_max_backoff: doc.retry_max_backoff,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn defaults_match_the_documented_values() {
        let settings = ClientSettings::default();
        assert_eq!(settings.request_timeout, NonZeroDuration::from_secs(10));
        assert_eq!(settings.max_attempts.get(), 3);
        assert_eq!(settings.connect_timeout, NonZeroDuration::from_millis(3100));
        assert_eq!(
            settings.retry_initial_backoff,
            NonZeroDuration::from_secs(1)
        );
        assert_eq!(settings.retry_max_backoff, NonZeroDuration::from_secs(20));
    }

    #[test]
    fn empty_table_selects_the_defaults() -> Result<(), Box<dyn Error>> {
        let settings: ClientSettings = serde_json::from_str("{}")?;
        assert_eq!(settings, ClientSettings::default());
        Ok(())
    }

    #[test]
    fn zero_timeout_is_rejected() {
        let result = serde_json::from_str::<ClientSettings>(r#"{"request_timeout": "0s"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn zero_attempts_is_rejected() {
        let result = serde_json::from_str::<ClientSettings>(r#"{"max_attempts": 0}"#);
        assert!(result.is_err());
    }

    #[test]
    fn backoff_order_is_rejected() {
        let result = serde_json::from_str::<ClientSettings>(
            r#"{"retry_initial_backoff": "10s", "retry_max_backoff": "5s"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn equal_backoffs_are_accepted() -> Result<(), Box<dyn Error>> {
        let settings: ClientSettings =
            serde_json::from_str(r#"{"retry_initial_backoff": "5s", "retry_max_backoff": "5s"}"#)?;
        assert_eq!(settings.retry_max_backoff, settings.retry_initial_backoff);
        Ok(())
    }

    #[test]
    fn unknown_key_is_rejected() {
        let result = serde_json::from_str::<ClientSettings>(r#"{"request_tmeout": "5s"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn round_trips_through_serde_json() -> Result<(), Box<dyn Error>> {
        let settings = ClientSettings {
            request_timeout: NonZeroDuration::from_secs(30),
            max_attempts: NonZeroU32::MIN.saturating_add(4),
            connect_timeout: NonZeroDuration::from_millis(500),
            retry_initial_backoff: NonZeroDuration::from_millis(250),
            retry_max_backoff: NonZeroDuration::from_mins(2),
        };
        let json = serde_json::to_string(&settings)?;
        let parsed: ClientSettings = serde_json::from_str(&json)?;
        assert_eq!(parsed, settings);
        Ok(())
    }
}
