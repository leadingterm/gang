//! The session record: what the last login stored, and what the client needs to mint bearer
//! tokens without asking the user again.

use gang_types::{CaPin, ProviderName, Url};
use serde::{Deserialize, Serialize};

use crate::ClientError;
use crate::home::ClientHome;
use crate::proof_source::ProofSource;

/// How the session obtains identity-provider proofs when a bearer token must be minted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum SessionKind {
    /// Obtained through an identity provider's interactive login; proofs come from the stored
    /// id token, renewed with the refresh token. `login` is the provider's login configuration.
    Interactive {
        /// The provider's login configuration, as published by the server.
        login: serde_json::Value,
    },
    /// Obtained through the server's device authorization flow.
    Device,
    /// Obtained from a workload proof; every token is minted from a fresh read of `source`.
    Workload {
        /// Where the workload proof is read from.
        source: ProofSource,
    },
}

/// The stored session, persisted as `session.json` in the client directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Session {
    /// The server URL the session authenticates against.
    pub server: Url,
    /// The pin of the Gang CA public key, or `None` when the server is trusted through the
    /// system roots or is served over plain HTTP.
    pub ca_pin: Option<CaPin>,
    /// The identity provider that authenticated the login.
    pub provider: ProviderName,
    /// How proofs are obtained.
    pub kind: SessionKind,
}

impl Session {
    /// Loads the stored session, or `None` when no session is stored.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Session`] when the stored file is not a valid session, and
    /// [`ClientError::Io`] when it cannot be read.
    pub(crate) fn load(home: &ClientHome) -> Result<Option<Self>, ClientError> {
        let path = home.session_file();
        let Some(contents) = ClientHome::read(&path)? else {
            return Ok(None);
        };
        serde_json::from_slice(&contents)
            .map(Some)
            .map_err(|source| ClientError::Session { path, source })
    }

    /// Stores the session, replacing any previous one.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Session`] when the session cannot be encoded, and
    /// [`ClientError::Io`] when the file cannot be written.
    pub(crate) fn store(&self, home: &ClientHome) -> Result<(), ClientError> {
        let path = home.session_file();
        let contents = serde_json::to_vec_pretty(self).map_err(|source| ClientError::Session {
            path: path.clone(),
            source,
        })?;
        home.write_public(&path, &contents)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use gang_types::{CaPin, ProviderName, Url};
    use tempfile::TempDir;

    use super::{Session, SessionKind};
    use crate::ClientError;
    use crate::proof_source::ProofSource;
    use crate::test_support::{ca_pin, home_in};

    fn session(kind: SessionKind, ca_pin: Option<CaPin>) -> Session {
        Session {
            server: Url::parse("https://gang.test").expect("valid url"),
            ca_pin,
            provider: ProviderName::try_new("google").expect("valid provider name"),
            kind,
        }
    }

    fn interactive() -> SessionKind {
        SessionKind::Interactive {
            login: serde_json::json!({ "issuer": "https://idp.test" }),
        }
    }

    fn workload() -> SessionKind {
        SessionKind::Workload {
            source: ProofSource::File {
                path: PathBuf::from("/run/secrets/proof"),
            },
        }
    }

    #[test]
    fn interactive_session_round_trips() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let stored = session(interactive(), Some(ca_pin()));

        stored.store(&home).expect("store succeeds");
        let loaded = Session::load(&home).expect("load succeeds");

        assert_eq!(loaded, Some(stored));
    }

    #[test]
    fn workload_session_without_pin_round_trips() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let stored = session(workload(), None);

        stored.store(&home).expect("store succeeds");
        let loaded = Session::load(&home).expect("load succeeds");

        assert_eq!(loaded, Some(stored));
    }

    #[test]
    fn load_of_absent_session_is_none() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        let loaded = Session::load(&home).expect("absence is not an error");

        assert_eq!(loaded, None);
    }

    #[test]
    fn garbage_session_is_a_session_error() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.write_public(&home.session_file(), b"{ not json")
            .expect("write succeeds");

        let result = Session::load(&home);

        assert!(matches!(result, Err(ClientError::Session { .. })));
    }

    #[test]
    fn unknown_kind_is_a_session_error() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let text = r#"{"server":"https://gang.test","ca_pin":null,"provider":"google","kind":{"kind":"pigeon"}}"#;
        home.write_public(&home.session_file(), text.as_bytes())
            .expect("write succeeds");

        let result = Session::load(&home);

        assert!(matches!(result, Err(ClientError::Session { .. })));
    }
}
