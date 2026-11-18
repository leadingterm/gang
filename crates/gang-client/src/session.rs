//! The session record: what the last login stored, and what the client needs to mint bearer
//! tokens without asking the user again.

use gang_types::{CaPin, Proof, ProviderName, RefreshToken, Url};
use serde::{Deserialize, Serialize};

use crate::ClientError;
use crate::home::ClientHome;
use crate::proof_source::ProofSource;
use crate::stored::StoredToken;

const LOGIN_ROLLBACK_FILE: &str = "login-rollback.json";

#[derive(Serialize, Deserialize)]
struct LoginBackup {
    session: Option<Vec<u8>>,
    token: Option<Vec<u8>>,
    id_token: Option<Vec<u8>>,
    refresh_token: Option<Vec<u8>>,
}

impl LoginBackup {
    fn capture(home: &ClientHome) -> Result<Self, ClientError> {
        Ok(Self {
            session: home.read(&home.session_file())?,
            token: home.read(&home.token_file())?,
            id_token: home.read(&home.id_token_file())?,
            refresh_token: home.read(&home.refresh_token_file())?,
        })
    }

    fn restore(&self, home: &ClientHome) -> Result<(), ClientError> {
        restore_file(home, &home.id_token_file(), self.id_token.as_deref(), false)?;
        restore_file(
            home,
            &home.refresh_token_file(),
            self.refresh_token.as_deref(),
            false,
        )?;
        restore_file(home, &home.token_file(), self.token.as_deref(), false)?;
        restore_file(home, &home.session_file(), self.session.as_deref(), true)
    }
}

fn restore_file(
    home: &ClientHome,
    path: &std::path::Path,
    contents: Option<&[u8]>,
    public: bool,
) -> Result<(), ClientError> {
    match contents {
        Some(contents) if public => home.write_public(path, contents),
        Some(contents) => home.write_private(path, contents),
        None => home.remove(path),
    }
}

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
    /// The caller must hold the client lock.
    pub(crate) fn load(home: &ClientHome) -> Result<Option<Self>, ClientError> {
        let path = home.session_file();
        let Some(contents) = home.read(&path)? else {
            return Ok(None);
        };
        serde_json::from_slice(&contents)
            .map(Some)
            .map_err(|source| ClientError::Session { path, source })
    }

    /// Replaces a login while retaining a private rollback journal until commit.
    /// The caller must hold the client lock.
    pub(crate) fn replace(
        &self,
        home: &ClientHome,
        token: &StoredToken,
        proof: Option<&Proof>,
        refresh: Option<&RefreshToken>,
    ) -> Result<(), ClientError> {
        self.replace_with(home, token, proof, refresh, ClientHome::remove)
    }

    /// [`Session::replace`] with the journal removal that commits the replacement supplied by
    /// the caller, so tests can fail the commit after the replacement is published.
    fn replace_with(
        &self,
        home: &ClientHome,
        token: &StoredToken,
        proof: Option<&Proof>,
        refresh: Option<&RefreshToken>,
        remove_journal: fn(&ClientHome, &std::path::Path) -> Result<(), ClientError>,
    ) -> Result<(), ClientError> {
        let session_path = home.session_file();
        let token_path = home.token_file();
        let journal_path = home.dir().join(LOGIN_ROLLBACK_FILE);
        let session = serde_json::to_vec_pretty(self).map_err(|source| ClientError::Session {
            path: session_path.clone(),
            source,
        })?;
        let token = serde_json::to_vec(token).map_err(|source| ClientError::Session {
            path: token_path.clone(),
            source,
        })?;
        let backup = LoginBackup::capture(home)?;
        let journal = serde_json::to_vec(&backup).map_err(|source| ClientError::Session {
            path: journal_path.clone(),
            source,
        })?;
        home.write_private(&journal_path, &journal)?;

        let result = (|| {
            restore_file(
                home,
                &home.id_token_file(),
                proof.map(|p| p.expose().as_bytes()),
                false,
            )?;
            restore_file(
                home,
                &home.refresh_token_file(),
                refresh.map(|r| r.expose().as_bytes()),
                false,
            )?;
            home.write_private(&token_path, &token)?;
            home.write_public(&session_path, &session)
        })();
        if let Err(original) = result {
            backup.restore(home)?;
            home.remove(&journal_path)?;
            return Err(original);
        }
        // Credential publication is complete. Journal removal can fail after unlinking, so a
        // removal error must not start a rollback that may no longer be recoverable.
        remove_journal(home, &journal_path)
    }

    /// Restores any interrupted replacement. The caller must hold the client lock.
    pub(crate) fn recover(home: &ClientHome) -> Result<(), ClientError> {
        let path = home.dir().join(LOGIN_ROLLBACK_FILE);
        let Some(contents) = home.read(&path)? else {
            return Ok(());
        };
        let backup: LoginBackup =
            serde_json::from_slice(&contents).map_err(|source| ClientError::Session {
                path: path.clone(),
                source,
            })?;
        backup.restore(home)?;
        home.remove(&path)
    }

    /// Stores the session, replacing any previous one.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Session`] when the session cannot be encoded, and
    /// [`ClientError::Io`] when the file cannot be written.
    #[cfg(test)]
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
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use gang_clock::Timestamp;
    use gang_types::{AccessToken, CaPin, ProviderName, RefreshToken, Url};
    use tempfile::TempDir;

    use super::{LOGIN_ROLLBACK_FILE, LoginBackup, Session, SessionKind};
    use crate::ClientError;
    use crate::home::ClientHome;
    use crate::proof_source::ProofSource;
    use crate::stored::StoredToken;
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

    /// Sessions beneath an untrusted writable parent are not accepted.
    #[test]
    fn session_under_a_writable_parent_is_refused() {
        if rustix::process::geteuid().as_raw() == 0 {
            return;
        }
        let tmp = TempDir::new().expect("temp dir");
        let shared = tmp.path().join("shared");
        std::fs::create_dir(&shared).expect("shared directory is created");
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777))
            .expect("shared directory permissions are set");
        let home = ClientHome::try_new(shared.join("gang")).expect("home path is valid");
        std::fs::create_dir(home.dir()).expect("home directory is created");
        std::fs::set_permissions(home.dir(), std::fs::Permissions::from_mode(0o700))
            .expect("home directory permissions are set");
        let contents =
            serde_json::to_vec(&session(SessionKind::Device, None)).expect("session is serialized");
        std::fs::write(home.session_file(), contents).expect("session is written");

        Session::load(&home)
            .expect_err("a session planted under a writable parent must be refused");
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

    fn journal_backup(home: &ClientHome) {
        let backup = LoginBackup::capture(home).expect("capture old files");
        let bytes = serde_json::to_vec(&backup).expect("encode backup");
        home.write_private(&home.dir().join(LOGIN_ROLLBACK_FILE), &bytes)
            .expect("write journal");
    }

    #[tokio::test]
    async fn interrupted_replacements_restore_each_prefix() {
        for old_credentials in [false, true] {
            for prefix in 0..=4 {
                let tmp = TempDir::new().expect("temp dir");
                let home = home_in(&tmp);
                let old = session(interactive(), Some(ca_pin()));
                old.store(&home).expect("store old session");
                let paths = [
                    home.id_token_file(),
                    home.refresh_token_file(),
                    home.token_file(),
                    home.session_file(),
                ];
                if old_credentials {
                    for (path, value) in paths.iter().take(3).zip([
                        b"old-id".as_slice(),
                        b"old-refresh",
                        b"old-token",
                    ]) {
                        home.write_private(path, value)
                            .expect("write old credential");
                    }
                }
                journal_backup(&home);
                let new_session =
                    serde_json::to_vec(&session(workload(), None)).expect("encode session");
                let replacements = [
                    b"new-id".as_slice(),
                    b"new-refresh",
                    b"new-token",
                    new_session.as_slice(),
                ];
                for (index, (path, value)) in
                    paths.iter().zip(replacements).take(prefix).enumerate()
                {
                    if index == 3 {
                        home.write_public(path, value).expect("publish session");
                    } else {
                        home.write_private(path, value).expect("publish credential");
                    }
                }
                let guard = home.lock().await.expect("recover under lock");
                assert_eq!(Session::load(&home).expect("load session"), Some(old));
                for (path, expected) in
                    paths
                        .iter()
                        .take(3)
                        .zip([b"old-id".as_slice(), b"old-refresh", b"old-token"])
                {
                    assert_eq!(
                        home.read(path).expect("read credential").as_deref(),
                        old_credentials.then_some(expected)
                    );
                }
                assert!(
                    home.read(&home.dir().join(LOGIN_ROLLBACK_FILE))
                        .expect("read journal")
                        .is_none()
                );
                drop(guard);
                let guard = home.lock().await.expect("repeated recovery");
                assert_eq!(
                    Session::load(&home).expect("load session"),
                    Some(session(interactive(), Some(ca_pin())))
                );
                drop(guard);
            }
        }
    }

    #[tokio::test]
    async fn recovery_removes_files_absent_before_login() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        journal_backup(&home);
        home.write_private(&home.id_token_file(), b"new-id")
            .expect("write proof");
        home.write_private(&home.refresh_token_file(), b"new-refresh")
            .expect("write refresh");
        home.write_private(&home.token_file(), b"new-token")
            .expect("write token");
        session(SessionKind::Device, None)
            .store(&home)
            .expect("write session");
        let guard = home.lock().await.expect("recover");
        assert!(Session::load(&home).expect("load session").is_none());
        for path in [
            home.id_token_file(),
            home.refresh_token_file(),
            home.token_file(),
        ] {
            assert!(home.read(&path).expect("read credential").is_none());
        }
        drop(guard);
    }

    #[tokio::test]
    async fn committed_replacement_survives_later_lock() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let old = session(interactive(), None);
        old.store(&home).expect("store old session");
        home.write_private(&home.id_token_file(), b"old-id")
            .expect("store old proof");
        let new = session(SessionKind::Device, None);
        let token = StoredToken {
            token: AccessToken::new("new-token"),
            issued_at: Timestamp::UNIX_EPOCH,
            expires_at: Timestamp::UNIX_EPOCH,
        };
        let refresh = RefreshToken::new("new-refresh");
        let guard = home.lock().await.expect("lock");
        new.replace(&home, &token, None, Some(&refresh))
            .expect("replace");
        drop(guard);
        let guard = home.lock().await.expect("lock after commit");
        assert_eq!(Session::load(&home).expect("load session"), Some(new));
        assert!(
            home.read(&home.id_token_file())
                .expect("read proof")
                .is_none()
        );
        assert_eq!(
            home.read(&home.refresh_token_file())
                .expect("read refresh")
                .as_deref(),
            Some(b"new-refresh".as_slice())
        );
        let stored: StoredToken = serde_json::from_slice(
            &home
                .read(&home.token_file())
                .expect("read token")
                .expect("token exists"),
        )
        .expect("parse token");
        assert_eq!(stored.token.expose(), token.token.expose());
        drop(guard);
    }

    /// Unlinks the journal, then fails as a directory sync failure would after the unlink.
    fn remove_then_fail(home: &ClientHome, path: &std::path::Path) -> Result<(), ClientError> {
        home.remove(path)?;
        Err(gang_os::PathIo::new("sync directory", path, std::io::ErrorKind::Other.into()).into())
    }

    #[tokio::test]
    async fn journal_removal_failure_after_publication_keeps_replacement() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let old = session(interactive(), None);
        old.store(&home).expect("store old session");
        home.write_private(&home.id_token_file(), b"old-id")
            .expect("store old proof");
        let new = session(SessionKind::Device, None);
        let token = StoredToken {
            token: AccessToken::new("new-token"),
            issued_at: Timestamp::UNIX_EPOCH,
            expires_at: Timestamp::UNIX_EPOCH,
        };
        let refresh = RefreshToken::new("new-refresh");
        let guard = home.lock().await.expect("lock");
        let result = new.replace_with(&home, &token, None, Some(&refresh), remove_then_fail);
        assert!(result.is_err());
        assert!(
            home.read(&home.dir().join(LOGIN_ROLLBACK_FILE))
                .expect("read journal")
                .is_none()
        );
        drop(guard);
        let guard = home.lock().await.expect("lock after failed commit");
        assert_eq!(Session::load(&home).expect("load session"), Some(new));
        assert!(
            home.read(&home.id_token_file())
                .expect("read proof")
                .is_none()
        );
        assert_eq!(
            home.read(&home.refresh_token_file())
                .expect("read refresh")
                .as_deref(),
            Some(b"new-refresh".as_slice())
        );
        let stored: StoredToken = serde_json::from_slice(
            &home
                .read(&home.token_file())
                .expect("read token")
                .expect("token exists"),
        )
        .expect("parse token");
        assert_eq!(stored.token.expose(), token.token.expose());
        drop(guard);
    }

    #[tokio::test]
    async fn recovery_failure_retains_journal_and_lock_retries() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let old = session(interactive(), None);
        old.store(&home).expect("store old session");
        home.write_private(&home.id_token_file(), b"old-id")
            .expect("store proof");
        journal_backup(&home);
        home.remove(&home.id_token_file()).expect("remove proof");
        std::fs::create_dir(home.id_token_file()).expect("obstruct proof restoration");
        assert!(home.lock().await.is_err());
        assert!(
            home.read(&home.dir().join(LOGIN_ROLLBACK_FILE))
                .expect("read journal")
                .is_some()
        );
        std::fs::remove_dir(home.id_token_file()).expect("remove obstruction");
        let guard = home.lock().await.expect("retry recovery");
        assert_eq!(Session::load(&home).expect("load session"), Some(old));
        assert_eq!(
            home.read(&home.id_token_file())
                .expect("read proof")
                .as_deref(),
            Some(b"old-id".as_slice())
        );
        assert!(
            home.read(&home.dir().join(LOGIN_ROLLBACK_FILE))
                .expect("read journal")
                .is_none()
        );
        drop(guard);
    }
}
