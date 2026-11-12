//! The client's private directory: the session, the identity provider's proofs and the bearer token.
//!
//! Every write goes through a temporary file in the destination's directory and is renamed
//! into place, so readers never observe a partial file.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use gang_os::PathIo;
use gang_os::fs::{self, PRIVATE_FILE};

use crate::{ClientError, ClientOptions};

/// Environment variable that selects the client directory on a CI runner.
const RUNNER_TEMP_VAR: &str = "RUNNER_TEMP";

/// Environment variable with the user's home directory.
const HOME_VAR: &str = "HOME";

/// Name of the Gang directory, below `$RUNNER_TEMP` or `$HOME/.config`.
const CLIENT_DIR_NAME: &str = "gang";

/// Directory below `$HOME` that holds per-user configuration.
const CONFIG_DIR_NAME: &str = ".config";

/// File holding the stored session (see `Session`).
const SESSION_FILE: &str = "session.json";

/// File holding the client options.
const OPTIONS_FILE: &str = "client.toml";

/// File holding the identity provider's latest proof.
const ID_TOKEN_FILE: &str = "id-token";

/// File holding the minted Gang bearer token (see `StoredToken`).
const TOKEN_FILE: &str = "token";

/// File holding the identity provider's refresh token.
const REFRESH_TOKEN_FILE: &str = "refresh-token";

/// File that serializes the client operations which rewrite the identity files.
const LOCK_FILE: &str = "lock";

/// Permission bits of public files: owner read and write, everyone read.
const PUBLIC_FILE_MODE: u32 = 0o644;

/// The client's private directory and the files it keeps there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHome {
    dir: PathBuf,
}

impl ClientHome {
    /// Uses `dir` as the client directory. The path becomes absolute; nothing is created.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Io`] when the current directory cannot be determined for a
    /// relative path.
    pub fn try_new(dir: impl Into<PathBuf>) -> Result<Self, ClientError> {
        let path: PathBuf = dir.into();
        let dir =
            std::path::absolute(&path).map_err(|source| PathIo::new("resolve", &path, source))?;
        Ok(Self { dir })
    }

    /// Locates the client directory from the environment.
    ///
    /// `$RUNNER_TEMP/gang` when `RUNNER_TEMP` is set and non-empty, otherwise
    /// `$HOME/.config/gang` when `HOME` is set and non-empty.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::HomeUnknown`] when neither variable is usable.
    pub fn resolve(env: impl Fn(&str) -> Option<OsString>) -> Result<Self, ClientError> {
        if let Some(runner_temp) = env(RUNNER_TEMP_VAR).filter(|value| !value.is_empty()) {
            return Self::try_new(PathBuf::from(runner_temp).join(CLIENT_DIR_NAME));
        }
        if let Some(home) = env(HOME_VAR).filter(|value| !value.is_empty()) {
            return Self::try_new(
                PathBuf::from(home)
                    .join(CONFIG_DIR_NAME)
                    .join(CLIENT_DIR_NAME),
            );
        }
        Err(ClientError::HomeUnknown)
    }

    /// The client directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Reads options from `client.toml` in the client directory.
    /// If the file is absent, returns [`ClientOptions::default`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Io`] if the file cannot be read and
    /// [`ClientError::Options`] if its contents are invalid.
    pub fn options(&self) -> Result<ClientOptions, ClientError> {
        let path = self.options_file();
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(source) if source.kind() == ErrorKind::NotFound => {
                return Ok(ClientOptions::default());
            }
            Err(source) => return Err(PathIo::new("read", &path, source).into()),
        };
        toml::from_str(&contents).map_err(|source| ClientError::Options { path, source })
    }

    /// The client options file.
    fn options_file(&self) -> PathBuf {
        self.dir.join(OPTIONS_FILE)
    }

    /// The stored session.
    #[must_use]
    pub fn session_file(&self) -> PathBuf {
        self.dir.join(SESSION_FILE)
    }

    /// The identity provider's latest proof.
    #[must_use]
    pub fn id_token_file(&self) -> PathBuf {
        self.dir.join(ID_TOKEN_FILE)
    }

    /// The minted Gang bearer token.
    #[must_use]
    pub fn token_file(&self) -> PathBuf {
        self.dir.join(TOKEN_FILE)
    }

    /// The identity provider's refresh token.
    #[must_use]
    pub fn refresh_token_file(&self) -> PathBuf {
        self.dir.join(REFRESH_TOKEN_FILE)
    }

    /// The lock file that serializes the client operations which rewrite the identity files.
    #[must_use]
    pub fn lock_file(&self) -> PathBuf {
        self.dir.join(LOCK_FILE)
    }

    /// Writes `contents` to `path` with mode 0600, creating the directory first.
    pub(crate) fn write_private(&self, path: &Path, contents: &[u8]) -> Result<(), ClientError> {
        self.create_dir()?;
        fs::write_atomic(path, contents, PRIVATE_FILE)?;
        Ok(())
    }

    /// Writes `contents` to `path` with mode 0644, creating the directory first.
    pub(crate) fn write_public(&self, path: &Path, contents: &[u8]) -> Result<(), ClientError> {
        self.create_dir()?;
        fs::write_atomic(path, contents, PUBLIC_FILE_MODE)?;
        Ok(())
    }

    /// Reads `path`, or `None` when it does not exist.
    pub(crate) fn read(path: &Path) -> Result<Option<Vec<u8>>, ClientError> {
        fs::read_optional(path).map_err(ClientError::from)
    }

    /// Removes `path`; succeeds when it is already absent.
    pub(crate) fn remove(path: &Path) -> Result<(), ClientError> {
        fs::remove_optional(path).map_err(ClientError::from)
    }

    /// Creates the client directory (and missing parents) with mode 0700 when absent.
    pub(crate) fn create_dir(&self) -> Result<(), ClientError> {
        fs::ensure_private_dir(&self.dir).map_err(ClientError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::Path;

    use tempfile::TempDir;

    use super::ClientHome;
    use crate::ClientError;
    use crate::test_support::{home_in, mode};

    /// An environment with exactly the given variables.
    fn env_of(vars: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<OsString> {
        move |name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    #[test]
    fn resolve_prefers_runner_temp() {
        let home = ClientHome::resolve(env_of(&[
            ("RUNNER_TEMP", "/runner/temp"),
            ("HOME", "/home/dev"),
        ]))
        .expect("runner temp is set");

        assert_eq!(home.dir(), Path::new("/runner/temp/gang"));
    }

    #[test]
    fn resolve_ignores_empty_runner_temp() {
        let home = ClientHome::resolve(env_of(&[("RUNNER_TEMP", ""), ("HOME", "/home/dev")]))
            .expect("home is set");

        assert_eq!(home.dir(), Path::new("/home/dev/.config/gang"));
    }

    #[test]
    fn resolve_falls_back_to_home() {
        let home = ClientHome::resolve(env_of(&[("HOME", "/home/dev")])).expect("home is set");

        assert_eq!(home.dir(), Path::new("/home/dev/.config/gang"));
    }

    #[test]
    fn resolve_without_usable_variables_fails() {
        let result = ClientHome::resolve(env_of(&[("RUNNER_TEMP", ""), ("HOME", "")]));

        assert!(matches!(result, Err(ClientError::HomeUnknown)));
    }

    #[test]
    fn missing_options_file_uses_defaults() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        assert_eq!(
            home.options().expect("options load"),
            crate::ClientOptions::default()
        );
    }

    #[test]
    fn options_file_overrides_a_default() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.create_dir().expect("directory is created");
        std::fs::write(
            home.dir().join("client.toml"),
            "browser_command = 'browser'\n",
        )
        .expect("options are written");

        let options = home.options().expect("options load");

        assert_eq!(options.browser_command.as_deref(), Some("browser"));
    }

    #[test]
    fn unknown_option_reports_the_options_file() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.create_dir().expect("directory is created");
        let path = home.dir().join("client.toml");
        std::fs::write(&path, "unknown_option = true\n").expect("options are written");

        assert!(matches!(
            home.options(),
            Err(ClientError::Options { path: error_path, .. }) if error_path == path
        ));
    }

    #[test]
    fn options_file_read_failure_is_an_io_error() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.create_dir().expect("directory is created");
        std::fs::create_dir(home.dir().join("client.toml")).expect("directory is created");

        assert!(matches!(home.options(), Err(ClientError::Io(_))));
    }

    #[test]
    fn private_file_is_owner_only_and_creates_its_directory() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        home.write_private(&home.token_file(), b"key")
            .expect("write succeeds");

        assert_eq!(mode(&home.token_file()), 0o600);
        assert_eq!(mode(home.dir()), 0o700);
    }

    #[test]
    fn public_file_is_world_readable() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        home.write_public(&home.session_file(), b"session")
            .expect("write succeeds");

        assert_eq!(mode(&home.session_file()), 0o644);
    }

    #[test]
    fn overwrite_replaces_content() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        home.write_private(&home.refresh_token_file(), b"first")
            .expect("first write succeeds");
        home.write_private(&home.refresh_token_file(), b"second")
            .expect("second write succeeds");

        let contents = ClientHome::read(&home.refresh_token_file()).expect("read succeeds");
        assert_eq!(contents.as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn read_of_missing_file_is_none() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        let contents = ClientHome::read(&home.token_file()).expect("read succeeds");

        assert_eq!(contents, None);
    }

    #[test]
    fn read_that_fails_for_another_reason_is_an_error() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.create_dir().expect("directory is created");

        let result = ClientHome::read(home.dir());

        assert!(matches!(result, Err(ClientError::Io(_))));
    }

    #[test]
    fn remove_of_missing_file_is_ok() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        ClientHome::remove(&home.refresh_token_file()).expect("removing an absent file succeeds");
    }
}
