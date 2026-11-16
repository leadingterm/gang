//! The client's private directory: the session, the identity provider's proofs and the bearer token.
//!
//! Reads, writes and the lock go through the verified private directory. Writes use a
//! temporary file in that directory and are renamed into place, so readers never observe
//! a partial file.

use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use gang_os::PathIo;
use gang_os::fs::{self, Mode, PRIVATE_FILE, PUBLIC_FILE, PrivateDir};

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
pub(crate) const LOCK_FILE: &str = "lock";

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
        let Some(contents) = self.read(&path)? else {
            return Ok(ClientOptions::default());
        };
        let contents = String::from_utf8(contents)
            .map_err(|_| PathIo::new("read", &path, ErrorKind::InvalidData.into()))?;
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
        self.write_file(path, contents, PRIVATE_FILE)
    }

    /// Writes `contents` to `path` with mode 0644, creating the directory first.
    pub(crate) fn write_public(&self, path: &Path, contents: &[u8]) -> Result<(), ClientError> {
        self.write_file(path, contents, PUBLIC_FILE)
    }

    /// Publishes a client file through its verified directory.
    fn write_file(&self, path: &Path, contents: &[u8], mode: Mode) -> Result<(), ClientError> {
        let dir = self.ensure_dir()?;
        let name = self.file_name(path, "write")?;
        dir.write_atomic(name, contents, mode)?;
        Ok(())
    }

    /// Reads `path`, or `None` when it does not exist.
    pub(crate) fn read(&self, path: &Path) -> Result<Option<Vec<u8>>, ClientError> {
        let name = self.file_name(path, "read")?;
        let Some(dir) = self.open_dir()? else {
            return Ok(None);
        };
        dir.read_optional(name).map_err(ClientError::from)
    }

    /// Removes `path`; succeeds when it is already absent.
    pub(crate) fn remove(&self, path: &Path) -> Result<(), ClientError> {
        let name = self.file_name(path, "remove")?;
        let Some(dir) = self.open_dir()? else {
            return Ok(());
        };
        dir.remove_optional(name).map_err(ClientError::from)
    }

    /// Opens the client directory after the private-directory checks, or `None` when absent.
    fn open_dir(&self) -> Result<Option<PrivateDir>, ClientError> {
        fs::open_private_dir(&self.dir).map_err(ClientError::from)
    }

    /// Opens the client directory after the checks, creating it with mode 0700 when absent.
    pub(crate) fn ensure_dir(&self) -> Result<PrivateDir, ClientError> {
        fs::ensure_private_dir(&self.dir).map_err(ClientError::from)
    }

    /// Returns the name of a client file relative to the client directory.
    fn file_name<'a>(
        &self,
        path: &'a Path,
        action: &'static str,
    ) -> Result<&'a OsStr, ClientError> {
        path.strip_prefix(&self.dir)
            .map(Path::as_os_str)
            .map_err(|_| PathIo::new(action, path, ErrorKind::InvalidInput.into()).into())
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
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
        home.ensure_dir().expect("directory is created");
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
        home.ensure_dir().expect("directory is created");
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
        home.ensure_dir().expect("directory is created");
        std::fs::create_dir(home.dir().join("client.toml")).expect("directory is created");

        assert!(matches!(home.options(), Err(ClientError::Io(_))));
    }

    /// Options beneath an untrusted writable parent are not accepted.
    #[test]
    fn options_under_a_writable_parent_are_refused() {
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
        std::fs::write(
            home.dir().join("client.toml"),
            "browser_command = '/bin/false'\n",
        )
        .expect("options are written");

        home.options()
            .expect_err("options planted under a writable parent must be refused");
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

        let contents = home
            .read(&home.refresh_token_file())
            .expect("read succeeds");
        assert_eq!(contents.as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn read_of_missing_file_is_none() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        let contents = home.read(&home.token_file()).expect("read succeeds");

        assert_eq!(contents, None);
    }

    #[test]
    fn read_that_fails_for_another_reason_is_an_error() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        home.ensure_dir().expect("directory is created");

        let result = home.read(home.dir());

        assert!(matches!(result, Err(ClientError::Io(_))));
    }

    #[test]
    fn remove_of_missing_file_is_ok() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);

        home.remove(&home.refresh_token_file())
            .expect("removing an absent file succeeds");
    }
}
