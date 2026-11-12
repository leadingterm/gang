//! The client's P-256 key pair and the PKCS#10 certificate signing request built from it.

use std::fmt;

use der::Encode;
use gang_types::{CertificateRequestPem, PrivateKeyPem};
use p256::SecretKey;
use p256::ecdsa::{DerSignature, SigningKey};
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use x509_cert::builder::{Builder, RequestBuilder};

use crate::certificate::{common_name, encode_pem};
use crate::errors::MtlsError;

/// Common name of every certificate signing request built from a key pair.
const SUBJECT: &str = "gang";

/// PEM label of a PKCS#10 certificate signing request.
const REQUEST_LABEL: &str = "CERTIFICATE REQUEST";

/// Number of random draws attempted before generation gives up. A uniform 32-byte draw is a
/// valid P-256 scalar with probability of about 1 - 2^-32, so exhausting this bound signals a
/// broken entropy source rather than bad luck.
const MAX_KEY_ATTEMPTS: usize = 8;

/// A P-256 ECDSA key pair held by a client.
///
/// The signing key and its PKCS#8 PEM encoding are kept together so the key can be stored and
/// later restored with [`KeyPair::from_private_key`]. The type is not `Clone`, and `Debug`
/// never prints the key material.
pub struct KeyPair {
    signing_key: SigningKey,
    private_key: PrivateKeyPem,
}

impl KeyPair {
    /// Generates a fresh key pair from the operating system's randomness.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::Entropy`] when the operating system provides no randomness or no
    /// valid P-256 scalar is drawn within the fixed attempt bound, and [`MtlsError::KeyEncoding`]
    /// when the key cannot be encoded as PKCS#8.
    pub fn generate() -> Result<Self, MtlsError> {
        let signing_key = SigningKey::from(draw_secret_key()?);
        Self::from_signing_key(signing_key)
    }

    /// Restores the key pair whose private key is `private_key`.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::InvalidPrivateKey`] unless `private_key` is a PKCS#8 PEM block that
    /// holds a P-256 ECDSA key.
    pub fn from_private_key(private_key: PrivateKeyPem) -> Result<Self, MtlsError> {
        let signing_key = SigningKey::from_pkcs8_pem(private_key.expose())
            .map_err(|_| MtlsError::InvalidPrivateKey)?;
        Ok(Self {
            signing_key,
            private_key,
        })
    }

    /// The PKCS#8 PEM private key. Callers must not log or persist the result.
    #[must_use]
    pub fn private_key(&self) -> &PrivateKeyPem {
        &self.private_key
    }

    /// Builds a PKCS#10 certificate signing request for this key pair.
    ///
    /// The request is signed with ECDSA over SHA-256 using RFC 6979 deterministic nonces, so
    /// the same key pair always yields the same request bytes.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::Encoding`] or [`MtlsError::RequestBuild`] when the request cannot be
    /// assembled or encoded.
    pub fn certificate_request(&self) -> Result<CertificateRequestPem, MtlsError> {
        let subject = common_name(SUBJECT)?;
        let request = RequestBuilder::new(subject)?.build::<_, DerSignature>(&self.signing_key)?;
        let der = request.to_der()?;
        let pem = encode_pem(REQUEST_LABEL, &der)?;
        Ok(CertificateRequestPem::try_new(pem)?)
    }

    /// Wraps `signing_key` with its PKCS#8 PEM encoding.
    fn from_signing_key(signing_key: SigningKey) -> Result<Self, MtlsError> {
        let pem = signing_key.to_pkcs8_pem(p256::pkcs8::LineEnding::LF)?;
        let private_key = PrivateKeyPem::try_new(pem.as_str())?;
        Ok(Self {
            signing_key,
            private_key,
        })
    }
}

impl fmt::Debug for KeyPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyPair(<redacted>)")
    }
}

/// Draws a random P-256 scalar, retrying a bounded number of times on invalid values.
fn draw_secret_key() -> Result<SecretKey, MtlsError> {
    let mut bytes = [0_u8; 32];
    for _ in 0..MAX_KEY_ATTEMPTS {
        getrandom::fill(&mut bytes).map_err(|_| MtlsError::Entropy)?;
        if let Ok(secret) = SecretKey::from_slice(&bytes) {
            return Ok(secret);
        }
    }
    Err(MtlsError::Entropy)
}
