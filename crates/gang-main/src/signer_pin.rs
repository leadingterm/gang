//! The certificate authority pin of the `[signer]` key: the one place that hashes a signer's
//! public key.

use gang_spi::{CertificateSignerPort, PluginContext, PortError};
use gang_types::CaPin;

use crate::errors::MainError;
use crate::plugins::{Signer, SignerConfig};

/// The CA pin of `signer`: the SHA-256 of its public key's `SubjectPublicKeyInfo`.
///
/// # Errors
///
/// [`PortError`] when the public key cannot be read.
pub(crate) async fn signer_ca_pin(signer: &Signer) -> Result<CaPin, PortError> {
    Ok(CaPin::of_spki(&signer.public_key().await?))
}

/// Builds the configured signer and returns its CA pin.
///
/// # Errors
///
/// [`MainError::Plugin`] when the signer cannot be built or its public key cannot be read.
pub(crate) async fn configured_ca_pin(
    signer: SignerConfig,
    context: &PluginContext<'_>,
) -> crate::errors::Result<CaPin> {
    let signer = Signer::build(signer, context)?;
    signer_ca_pin(&signer)
        .await
        .map_err(|source| MainError::Plugin {
            domain: Signer::DOMAIN,
            source,
        })
}

/// A file signer whose key lives under `dir`, at `signer/ca.key`.
#[cfg(test)]
pub(crate) fn file_signer(dir: &std::path::Path) -> SignerConfig {
    use gang_local::FileSignerConfig;
    use gang_types::AbsolutePathBuf;

    SignerConfig::File(FileSignerConfig {
        key_file: AbsolutePathBuf::try_new(dir.join("signer").join("ca.key"))
            .expect("test signer directory is absolute"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn signer_ca_pin_is_the_pin_of_the_signers_public_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("gang.toml");
        let context = PluginContext {
            config_path: &config_path,
        };

        let signer = Signer::build(file_signer(dir.path()), &context)
            .unwrap_or_else(|error| panic!("the signer builds: {error}"));
        let pin = signer_ca_pin(&signer).await.expect("pin derives");
        let spki = signer.public_key().await.expect("public key");
        assert_eq!(pin, CaPin::of_spki(&spki));

        let again = Signer::build(file_signer(dir.path()), &context)
            .unwrap_or_else(|error| panic!("the signer builds again: {error}"));
        assert_eq!(pin, signer_ca_pin(&again).await.expect("pin derives again"));

        let other_dir = tempfile::tempdir().expect("tempdir");
        let other = Signer::build(file_signer(other_dir.path()), &context)
            .unwrap_or_else(|error| panic!("the other signer builds: {error}"));
        assert_ne!(pin, signer_ca_pin(&other).await.expect("other pin derives"));
    }
}
