//! The public key that verifies Gang's bearer tokens.

use std::fmt;

use gang_types::CaPin;
use p256::ecdsa::VerifyingKey;
use p256::pkcs8::DecodePublicKey;

use crate::errors::TokenError;

/// A P-256 public key and the key id that names it in token headers.
///
/// The key id is the lowercase hex SHA-256 of the DER `SubjectPublicKeyInfo`, the same digest
/// that pins the certificate authority.
#[derive(Clone)]
pub struct TokenKey {
    verifying: VerifyingKey,
    kid: String,
}

impl TokenKey {
    /// Loads a P-256 key from its DER `SubjectPublicKeyInfo`.
    ///
    /// # Errors
    ///
    /// [`TokenError::Key`] when `der` is not a P-256 public key.
    pub fn from_spki(der: &[u8]) -> Result<Self, TokenError> {
        let verifying = VerifyingKey::from_public_key_der(der)
            .map_err(|error| TokenError::Key(error.to_string()))?;
        Ok(Self {
            verifying,
            kid: CaPin::of_spki(der).digest_hex().to_string(),
        })
    }

    /// The key id carried in the header of every token this key signs.
    #[must_use]
    pub fn kid(&self) -> &str {
        &self.kid
    }

    /// The verifying key, for checking signatures.
    pub(crate) fn verifying(&self) -> &VerifyingKey {
        &self.verifying
    }
}

impl fmt::Debug for TokenKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenKey")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::CertificateSignerPort as _;
    use gang_testkit::TestSigner;

    use super::*;

    #[tokio::test]
    async fn debug_shows_the_key_id_and_not_the_key_material() {
        let signer = TestSigner::new(1).expect("the seed is a valid scalar");
        let der = signer.public_key().await.expect("test public key encodes");
        let key = TokenKey::from_spki(&der).expect("test public key loads");
        assert_eq!(
            format!("{key:?}"),
            format!("TokenKey {{ kid: {:?}, .. }}", key.kid())
        );
    }
}
