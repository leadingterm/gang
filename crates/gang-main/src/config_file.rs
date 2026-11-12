//! Reads and parses the `gang.toml` a command was given.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use gang_config::{Document, GangConfig};

use crate::errors::{MainError, Result};
use crate::local_record::ServerDirectory;
use crate::plugins::{Plugins, local_state_dir};
use crate::workspace;

/// A validated `gang.toml` and the absolute path it was read from.
pub(crate) struct LoadedConfig {
    /// The configuration file, made absolute against the working directory.
    pub(crate) path: PathBuf,
    /// The validated configuration.
    pub(crate) config: GangConfig<Plugins>,
}

impl LoadedConfig {
    /// Reads and validates the configuration at `path`.
    ///
    /// # Errors
    ///
    /// [`MainError::ReadConfig`] when the file cannot be read, [`MainError::ParseConfig`] when
    /// its text is not a valid configuration.
    pub(crate) fn load(path: &Path) -> Result<Self> {
        Self::load_with(path, |key| std::env::var_os(key))
    }

    /// Reads and validates the configuration at `path`, with the environment supplied by `env`
    /// for resolving a relative `path` against `BUILD_WORKING_DIRECTORY`. The file is read from,
    /// and reported under, that same absolute path.
    ///
    /// # Errors
    ///
    /// As [`LoadedConfig::load`].
    pub(crate) fn load_with(path: &Path, env: impl Fn(&str) -> Option<OsString>) -> Result<Self> {
        let (absolute, text) = Self::read(path, env)?;
        Self::parse(absolute, &text)
    }

    /// Reads a configuration file and returns its absolute path and contents.
    fn read(path: &Path, env: impl Fn(&str) -> Option<OsString>) -> Result<(PathBuf, String)> {
        let absolute = workspace::absolute(path.to_path_buf(), env);
        let text = std::fs::read_to_string(&absolute).map_err(|source| MainError::ReadConfig {
            path: absolute.clone(),
            source,
        })?;
        Ok((absolute, text))
    }

    /// Validates configuration text read from `absolute`.
    fn parse(absolute: PathBuf, text: &str) -> Result<Self> {
        let config =
            text.parse::<GangConfig<Plugins>>()
                .map_err(|source| MainError::ParseConfig {
                    path: absolute.clone(),
                    source,
                })?;
        Ok(Self {
            path: absolute,
            config,
        })
    }

    /// Reads and validates the local configuration selected by `explicit` or by the workspace,
    /// and returns it with the state directory of its `[provisioner.local]` section.
    ///
    /// # Errors
    ///
    /// As [`LoadedConfig::load`], and [`MainError::NotLocalMode`] when the configuration does not
    /// select the local provisioner.
    pub(crate) fn local(explicit: Option<PathBuf>) -> Result<(Self, ServerDirectory)> {
        let path = workspace::local_config(explicit, |key| std::env::var_os(key));
        let loaded = Self::load(&path)?;
        let directory = loaded.local_directory()?;
        Ok((loaded, directory))
    }

    /// Reads and validates the local configuration, returning its editable document and the local
    /// server's state directory.
    ///
    /// # Errors
    ///
    /// As [`LoadedConfig::local`], or [`MainError::ParseConfig`] if the document cannot be parsed.
    pub(crate) fn local_document(
        explicit: Option<PathBuf>,
    ) -> Result<(Self, Document<Plugins>, ServerDirectory)> {
        let path = workspace::local_config(explicit, |key| std::env::var_os(key));
        let (absolute, text) = Self::read(&path, |key| std::env::var_os(key))?;
        let document =
            Document::<Plugins>::parse(&text).map_err(|source| MainError::ParseConfig {
                path: absolute.clone(),
                source,
            })?;
        let config = document
            .clone()
            .resolve()
            .map_err(|source| MainError::ParseConfig {
                path: absolute.clone(),
                source,
            })?;
        let loaded = Self {
            path: absolute,
            config,
        };
        let directory = loaded.local_directory()?;
        Ok((loaded, document, directory))
    }

    /// Returns the local provisioner's state directory, or rejects a non-local configuration.
    fn local_directory(&self) -> Result<ServerDirectory> {
        let state_dir = local_state_dir(&self.config.provisioner).ok_or(MainError::NotLocalMode)?;
        Ok(ServerDirectory::new(state_dir))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ffi::OsString;
    use std::path::{Component, Path, PathBuf};

    use super::LoadedConfig;
    use crate::errors::MainError;

    /// A minimal valid local configuration: every section the schema requires, with the
    /// local provisioner, cache, signer, scheduler, database and sandboxes, placeholder paths.
    pub(crate) const VALID_LOCAL_CONFIG: &str = r#"
poll_interval = "500ms"
client_action_timeout = "1m"
default_isa = "x86-64"
listen = "127.0.0.1:9443"
worker_listen = "127.0.0.1:9090"
endpoint = "https://127.0.0.1:9443"

[[fleets]]
name = "local-worker-x86-64"
isa = "x86-64"
sandbox = "bwrap"
min_instances = 0
max_instances = 4
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 4
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"

[provisioner.local]
state_dir = "/tmp/gang-local"
mock_imds_listen = "127.0.0.1:16925"

[cache.filesystem]
path = "/tmp/gang-local/cache"

[signer.file]
key_file = "/tmp/gang-local/ca.key"

[scheduler.nativelink]
worker_endpoint = "grpcs://127.0.0.1:9090"
scheduler_name = "MAIN_SCHEDULER"

[database.sqlite]
path = "/tmp/gang-local/gang.db"

[sandbox.noop]

[sandbox.bwrap]
readonly_bind_paths = ["/nix/store", "/run/current-system/sw"]
permit_network_property = false

[server]
trust_domain = "gang.local"

[authentication.clients.token]
token_ttl = "10m"
clock_skew = "30s"
max_cached_tokens = 4096
providers = ["local-workers"]

[authentication.workers.mtls]
provider = "local-workers"
certificate_ttl = "1d"
max_cached_certificates = 4096

[identity.providers.local-workers.oidc]
issuer_url = "http://127.0.0.1:16925"
audience = "gang"
principal_claim = "sub"
required_claims = {}
"#;

    /// The error a load returned, panicking when it succeeded.
    fn load_error(path: &Path) -> MainError {
        match LoadedConfig::load(path) {
            Ok(_) => panic!("{} loaded but was expected to fail", path.display()),
            Err(error) => error,
        }
    }

    /// The path of `target` written relative to `base`, both absolute, without symlink
    /// resolution, so that `base.join(result)` names `target` lexically.
    fn relative_to(base: &Path, target: &Path) -> PathBuf {
        let base: Vec<Component<'_>> = base.components().collect();
        let target: Vec<Component<'_>> = target.components().collect();
        let common = base
            .iter()
            .zip(&target)
            .take_while(|(left, right)| left == right)
            .count();
        let mut relative = PathBuf::new();
        for _ in common..base.len() {
            relative.push("..");
        }
        for component in &target[common..] {
            relative.push(component.as_os_str());
        }
        relative
    }

    #[test]
    fn missing_file_is_read_config_error_with_the_given_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing.toml");

        match load_error(&path) {
            MainError::ReadConfig { path: reported, .. } => assert_eq!(reported, path),
            other => panic!("expected ReadConfig, got {other:?}"),
        }
    }

    #[test]
    fn invalid_toml_is_parse_config_error() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.toml");
        std::fs::write(&path, "poll_interval = \n[[").expect("write configuration");

        assert!(matches!(load_error(&path), MainError::ParseConfig { .. }));
    }

    #[test]
    fn unknown_top_level_key_is_parse_config_error_naming_the_key() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.toml");
        let text = format!("bogus_setting = 1\n{VALID_LOCAL_CONFIG}");
        std::fs::write(&path, text).expect("write configuration");

        match load_error(&path) {
            error @ MainError::ParseConfig { .. } => {
                let (out, _stdout, stderr) = crate::output::Output::captured();
                out.failure("gang server", &error);
                let rendered = stderr.text();
                assert!(rendered.contains("bogus_setting"), "{rendered}");
            }
            other => panic!("expected ParseConfig, got {other:?}"),
        }
    }

    #[test]
    fn unknown_provisioner_backend_lists_the_accepted_backends() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.toml");
        let text = VALID_LOCAL_CONFIG.replace("[provisioner.local]", "[provisioner.nope]");
        std::fs::write(&path, text).expect("write configuration");

        let error = load_error(&path);
        assert!(matches!(error, MainError::ParseConfig { .. }), "{error:?}");
        let (out, _stdout, stderr) = crate::output::Output::captured();
        out.failure("gang server", &error);
        let rendered = stderr.text();
        assert!(rendered.contains("aws"), "{rendered}");
        assert!(rendered.contains("local"), "{rendered}");
    }

    #[test]
    fn valid_local_configuration_loads_with_its_absolute_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.local.toml");
        std::fs::write(&path, VALID_LOCAL_CONFIG).expect("write configuration");

        let loaded = LoadedConfig::load(&path).expect("valid configuration loads");

        assert_eq!(loaded.path, path);
    }

    #[test]
    fn relative_path_is_made_absolute_against_the_current_directory() {
        let current = std::env::current_dir().expect("current directory");
        let directory = tempfile::tempdir().expect("temporary directory");
        let absolute = directory.path().join("gang.local.toml");
        std::fs::write(&absolute, VALID_LOCAL_CONFIG).expect("write configuration");
        let relative = relative_to(&current, &absolute);
        assert!(relative.is_relative(), "{}", relative.display());

        let loaded = LoadedConfig::load(&relative).expect("valid configuration loads");

        assert!(loaded.path.is_absolute());
        assert_eq!(loaded.path, current.join(&relative));
    }

    /// The environment of a process whose `BUILD_WORKING_DIRECTORY` is `directory`.
    fn working_directory_env(directory: &Path) -> impl Fn(&str) -> Option<OsString> {
        let directory = directory.as_os_str().to_owned();
        move |key| (key == "BUILD_WORKING_DIRECTORY").then(|| directory.clone())
    }

    #[test]
    fn relative_path_is_read_from_the_working_directory() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("rel.toml");
        std::fs::write(&path, VALID_LOCAL_CONFIG).expect("write configuration");

        let loaded = LoadedConfig::load_with(
            Path::new("rel.toml"),
            working_directory_env(directory.path()),
        )
        .expect("valid configuration loads");

        assert_eq!(loaded.path, path);
    }

    #[test]
    fn missing_relative_file_reports_read_error_with_the_absolute_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing.toml");

        match LoadedConfig::load_with(
            Path::new("missing.toml"),
            working_directory_env(directory.path()),
        ) {
            Err(MainError::ReadConfig { path: reported, .. }) => assert_eq!(reported, path),
            Err(other) => panic!("expected ReadConfig, got {other:?}"),
            Ok(_) => panic!("missing file loaded"),
        }
    }

    #[test]
    fn invalid_relative_file_reports_parse_error_with_the_absolute_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("rel.toml");
        std::fs::write(&path, "poll_interval = \n[[").expect("write configuration");

        match LoadedConfig::load_with(
            Path::new("rel.toml"),
            working_directory_env(directory.path()),
        ) {
            Err(MainError::ParseConfig { path: reported, .. }) => assert_eq!(reported, path),
            Err(other) => panic!("expected ParseConfig, got {other:?}"),
            Ok(_) => panic!("invalid file loaded"),
        }
    }

    #[test]
    fn local_returns_the_state_directory_of_the_local_provisioner() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.local.toml");
        std::fs::write(&path, VALID_LOCAL_CONFIG).expect("write configuration");

        let (_loaded, server) =
            LoadedConfig::local(Some(path)).expect("valid local configuration loads");

        assert!(
            server.config_path().starts_with("/tmp/gang-local"),
            "{}",
            server.config_path().display()
        );
    }

    #[test]
    fn local_document_has_the_validated_listen_address() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.local.toml");
        std::fs::write(&path, VALID_LOCAL_CONFIG).expect("write configuration");

        let (loaded, document, server) =
            LoadedConfig::local_document(Some(path)).expect("valid local configuration loads");

        assert_eq!(document.listen, loaded.config.listen);
        assert!(server.config_path().starts_with("/tmp/gang-local"));
    }

    #[test]
    fn local_document_reports_parse_and_validation_errors_with_the_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("gang.local.toml");
        let invalid = VALID_LOCAL_CONFIG
            .replace("listen = \"127.0.0.1:9443\"", "listen = \"0.0.0.0:9443\"")
            .replace("endpoint = \"https://127.0.0.1:9443\"\n", "");

        for (text, validation_error) in [("invalid TOML [[", false), (invalid.as_str(), true)] {
            std::fs::write(&path, text).expect("write configuration");
            match LoadedConfig::local_document(Some(path.clone())) {
                Err(MainError::ParseConfig {
                    path: reported,
                    source,
                }) => {
                    assert_eq!(reported, path);
                    if validation_error {
                        assert!(matches!(
                            source,
                            gang_config::ConfigError::EndpointRequired { .. }
                        ));
                    } else {
                        assert!(matches!(source, gang_config::ConfigError::Gang(_)));
                    }
                }
                Err(other) => panic!("expected ParseConfig, got {other:?}"),
                Ok(_) => panic!("invalid configuration loaded"),
            }
        }
    }
}
