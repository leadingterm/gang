//! Provisioning of the file signer's P-256 key for a Local Mode deployment.
//!
//! The server loads the PKCS#8 key this module writes (see `gang-local`'s key file signer), so the
//! certificate authority it signs with is the one whose public key hashes to the returned [`CaPin`].

use std::io;
use std::path::Path;

use gang_types::CaPin;
use p256::ecdsa::SigningKey;
use p256::elliptic_curve::Generate;
use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

use crate::errors::{E2eError, Result};

/// Generates a P-256 signing key, writes it to `path` as PKCS#8 DER, and returns the pin of its
/// public key.
///
/// The parent directory is created owner-only (`0700`) when absent, and the key file is created
/// owner-only (`0600`). An existing `path` is an error: a key is never silently replaced.
pub(crate) fn provision(path: &Path) -> Result<CaPin> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent.filter(|parent| !parent.is_dir()) {
        gang_os::fs::ensure_private_dir(parent)
            .map_err(E2eError::system_from("creating the signer key directory"))?;
    }

    let key =
        SigningKey::try_generate().map_err(E2eError::system_from("generating a signer key"))?;
    let der = key.to_pkcs8_der().map_err(E2eError::system_from(format!(
        "encoding the signer key for {}",
        path.display()
    )))?;

    let published = gang_os::fs::write_new_atomic(path, der.as_bytes(), gang_os::fs::PRIVATE_FILE)
        .map_err(E2eError::system_from("writing the signer key"))?;
    if !published {
        let exists = io::Error::from(io::ErrorKind::AlreadyExists);
        return Err(E2eError::io(format!(
            "creating the signer key {}",
            path.display()
        ))(exists));
    }

    let spki = key
        .verifying_key()
        .to_public_key_der()
        .map_err(E2eError::system_from(format!(
            "encoding the signer public key for {}",
            path.display()
        )))?;
    Ok(CaPin::of_spki(spki.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
    use sha2::{Digest, Sha256};

    use super::provision;
    use crate::errors::{E2eError, Result};

    fn io_error(context: &str) -> impl FnOnce(std::io::Error) -> E2eError + '_ {
        E2eError::io(context)
    }

    #[test]
    fn creates_owner_only_key_in_owner_only_parent() -> Result<()> {
        let base = tempfile::tempdir().map_err(io_error("creating a temporary directory"))?;
        let parent = base.path().join("keys");
        let path = parent.join("signer.der");

        provision(&path)?;

        let parent_mode = fs::metadata(&parent)
            .map_err(io_error("reading the key directory"))?
            .permissions()
            .mode();
        assert_eq!(parent_mode & 0o777, 0o700);
        let file_mode = fs::metadata(&path)
            .map_err(io_error("reading the key file"))?
            .permissions()
            .mode();
        assert_eq!(file_mode & 0o777, 0o600);
        Ok(())
    }

    #[test]
    fn returned_pin_is_the_digest_of_the_stored_public_key() -> Result<()> {
        let base = tempfile::tempdir().map_err(io_error("creating a temporary directory"))?;
        let path = base.path().join("signer.der");

        let pin = provision(&path)?;

        let der = fs::read(&path).map_err(io_error("reading the key file"))?;
        let key = SigningKey::from_pkcs8_der(&der)
            .map_err(E2eError::system_from("decoding the signer key"))?;
        let spki = key
            .verifying_key()
            .to_public_key_der()
            .map_err(E2eError::system_from("encoding the public key"))?;
        let expected: [u8; 32] = Sha256::digest(spki.as_bytes()).into();
        assert_eq!(pin.as_bytes(), &expected);
        Ok(())
    }

    #[test]
    fn provisioning_an_existing_path_fails_and_keeps_the_key() -> Result<()> {
        let base = tempfile::tempdir().map_err(io_error("creating a temporary directory"))?;
        let path = base.path().join("signer.der");
        provision(&path)?;
        let before = fs::read(&path).map_err(io_error("reading the key file"))?;

        let second = provision(&path);

        assert!(second.is_err());
        let after = fs::read(&path).map_err(io_error("reading the key file"))?;
        assert_eq!(before, after);
        Ok(())
    }
}
