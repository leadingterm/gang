//! The certificate authority's signing key, kept in a file on the local filesystem. Signatures are
//! ECDSA P-256 over SHA-256 in DER form, and the public key is exported as a `SubjectPublicKeyInfo`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use gang_spi::{CertificateSignerPort, Plugin, PluginContext, PortError, SignatureAlgorithm};
use gang_types::AbsolutePathBuf;
use p256::ecdsa::{DerSignature, SigningKey, signature::Signer};
use p256::pkcs8::EncodePublicKey;
use serde::{Deserialize, Serialize};

use crate::errors::LocalError;
use crate::key_file;

/// Signs with the CA key loaded from a key file.
pub(crate) struct CaSigner {
    key: SigningKey,
    public_key_der: Vec<u8>,
}

impl CaSigner {
    /// Loads the CA key at `path`, generating it on first use, and caches its public key.
    pub(crate) fn load_or_create(path: &Path) -> Result<Self, LocalError> {
        let key = key_file::load_or_create(path)?;
        let public_key_der = key
            .verifying_key()
            .to_public_key_der()
            .map_err(|_| LocalError::Key {
                path: path.to_path_buf(),
            })?
            .as_bytes()
            .to_vec();
        Ok(Self {
            key,
            public_key_der,
        })
    }

    /// The `SubjectPublicKeyInfo` DER of the CA key, as handed to certificate consumers.
    #[must_use]
    pub(crate) fn public_key_der(&self) -> &[u8] {
        &self.public_key_der
    }

    /// Signs `message` with ECDSA P-256 over SHA-256 and returns the DER-encoded signature.
    #[must_use]
    pub(crate) fn sign(&self, message: &[u8]) -> Vec<u8> {
        let signature: DerSignature = self.key.sign(message);
        signature.as_bytes().to_vec()
    }
}

/// Settings for the file signer plugin, read from the `[signer.file]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSignerConfig {
    /// Absolute path to the PKCS#8 P-256 private key. Created on first use with mode `0600`.
    /// Its parent directory is created with mode `0700` on first use when missing. An existing
    /// parent must already be `0700` and owned by the current user; it is never changed. The path
    /// must name a file inside a directory other than the filesystem root.
    pub key_file: AbsolutePathBuf,
}

/// The certificate authority's key, loaded from `key_file` on first use. The key never leaves the
/// signer; only the public key and signatures are returned.
pub struct FileSigner {
    /// The paths where the key lives.
    paths: Arc<KeyPaths>,
    /// The key, loaded on first use.
    signer: tokio::sync::OnceCell<CaSigner>,
}

/// The key file and its containing directory.
struct KeyPaths {
    /// The key file path.
    file: PathBuf,
    /// The directory that holds the key file; created on first use when missing, never changed.
    directory: PathBuf,
}

impl Plugin for FileSigner {
    type Config = FileSignerConfig;

    fn build(config: FileSignerConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let key_file = config.key_file.as_path();
        let directory = match key_file.parent() {
            Some(parent) if parent.parent().is_some() && key_file.file_name().is_some() => {
                parent.to_path_buf()
            }
            _ => {
                return Err(PortError::config(format!(
                    "key_file {} must name a file inside a directory other than /",
                    key_file.display()
                )));
            }
        };
        Ok(Self {
            paths: Arc::new(KeyPaths {
                file: config.key_file.into_path_buf(),
                directory,
            }),
            signer: tokio::sync::OnceCell::new(),
        })
    }
}

impl FileSigner {
    /// Returns the key, loading it on the blocking pool on first use.
    async fn signer(&self) -> Result<&CaSigner, PortError> {
        self.signer
            .get_or_try_init(|| async {
                let paths = Arc::clone(&self.paths);
                tokio::task::spawn_blocking(move || paths.load())
                    .await
                    .map_err(PortError::backend)?
                    .map_err(PortError::from)
            })
            .await
    }
}

impl KeyPaths {
    /// Creates the directory when missing and loads the key. An existing directory must already
    /// be private. Key creation is atomic, and the signer serializes first-use initialization.
    fn load(&self) -> Result<CaSigner, LocalError> {
        gang_os::fs::require_private_dir(&self.directory)?;
        CaSigner::load_or_create(&self.file)
    }
}

impl CertificateSignerPort for FileSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        Ok(self.signer().await?.public_key_der().to_vec())
    }

    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
        Ok(self.signer().await?.sign(message))
    }
}

#[cfg(test)]
mod tests {
    use gang_testkit::signer_contract::signer_contract;

    use super::*;
    use crate::test_support::plugin_context;

    #[tokio::test]
    async fn signer_satisfies_contract() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let config = FileSignerConfig {
            key_file: AbsolutePathBuf::try_new(dir.path().join("signer").join("ca.key"))?,
        };
        let signer = FileSigner::build(config, &plugin_context())?;
        signer_contract(&signer).await
    }

    #[tokio::test]
    async fn first_public_key_creates_the_key_in_a_private_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir()?;
        let directory = dir.path().join("signer");
        let key_file = directory.join("ca.key");
        let config = FileSignerConfig {
            key_file: AbsolutePathBuf::try_new(key_file.clone())?,
        };
        let signer = FileSigner::build(config, &plugin_context()).expect("valid signer config");

        signer.public_key().await?;

        assert!(key_file.is_file());
        let mode = std::fs::metadata(&directory)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        Ok(())
    }

    #[test]
    fn relative_key_file_is_rejected_at_parse() {
        let config = serde_json::json!({ "key_file": "ca.key" });
        assert!(serde_json::from_value::<FileSignerConfig>(config).is_err());
    }

    #[test]
    fn key_file_without_a_parent_directory_is_a_config_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = FileSignerConfig {
            key_file: AbsolutePathBuf::try_new("/")?,
        };
        assert!(matches!(
            FileSigner::build(config, &plugin_context()),
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[test]
    fn a_key_file_in_the_root_directory_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let config = FileSignerConfig {
            key_file: AbsolutePathBuf::try_new("/ca.key")?,
        };
        assert!(matches!(
            FileSigner::build(config, &plugin_context()),
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn key_in_a_directory_other_users_can_open_is_refused_and_left_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir()?;
        let directory = dir.path().join("signer");
        std::fs::create_dir(&directory)?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))?;
        let key_file = directory.join("ca.key");
        let config = FileSignerConfig {
            key_file: AbsolutePathBuf::try_new(key_file.clone())?,
        };
        let signer = FileSigner::build(config, &plugin_context()).expect("valid signer config");

        let error = signer
            .paths
            .load()
            .err()
            .ok_or("loaded a key from a 0755 directory")?;
        assert!(matches!(
            error,
            LocalError::PrivateDir(gang_os::fs::PrivateDirError::NotPrivate { mode: 0o755, .. })
        ));
        assert!(matches!(
            signer.public_key().await,
            Err(PortError::Config(_))
        ));
        let mode = std::fs::metadata(&directory)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
        assert!(!key_file.exists());
        Ok(())
    }
}
