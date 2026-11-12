//! Certificate signing: the certificate authority's key, wherever it lives.

use crate::{PortError, port};

/// The signing key of the certificate authority. The key never leaves the signer.
#[port]
pub trait CertificateSignerPort: Send + Sync {
    /// The algorithm of the key; certificates are signed with it.
    fn algorithm(&self) -> SignatureAlgorithm;

    /// The public key as a DER `SubjectPublicKeyInfo`.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the public key cannot be read.
    async fn public_key(&self) -> Result<Vec<u8>, PortError>;

    /// Signs `message` (hashed as the algorithm prescribes) and returns the DER signature.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the key cannot sign.
    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError>;
}

/// A signature scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignatureAlgorithm {
    /// ECDSA over the NIST P-256 curve with SHA-256.
    EcdsaP256Sha256,
}
