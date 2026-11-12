//! Deterministic P-256 certificate signers for tests.
//!
//! Every signer here signs with a fixed scalar, so a test that needs the certificate
//! authority's public key can reproduce it from [`AUTHORITY_SEED`] alone. The failing
//! signers ([`FailingSigner`], [`WrongKeySigner`], [`GarbageSigner`]) break one property
//! each, so tests can check how callers react to a bad signature source.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use gang_spi::{CertificateSignerPort, PortError, SignatureAlgorithm};
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{DerSignature, SigningKey, VerifyingKey};
use p256::pkcs8::EncodePublicKey;

/// The seed of the certificate authority's key in every test that builds one.
pub const AUTHORITY_SEED: u8 = 7;

/// The P-256 signing key whose scalar is `seed` repeated 32 times.
///
/// Seeds `0` and `255` are not valid P-256 scalars.
///
/// # Errors
///
/// Returns [`p256::ecdsa::Error`] when `seed` does not give a valid P-256 scalar.
pub fn signing_key(seed: u8) -> Result<SigningKey, p256::ecdsa::Error> {
    SigningKey::from_slice(&[seed; 32])
}

/// A signer over the P-256 key for `seed`, which counts its calls and can be put into an outage.
///
/// During an outage every `sign` fails with [`PortError::Unavailable`]; `public_key` keeps
/// working, as it does against a live signer that is merely busy.
#[derive(Debug)]
pub struct TestSigner {
    key: SigningKey,
    outage: Arc<AtomicBool>,
    public_key_reads: AtomicUsize,
    sign_calls: AtomicUsize,
}

impl TestSigner {
    /// A signer over the key for `seed`, with no outage.
    ///
    /// # Errors
    ///
    /// Returns [`p256::ecdsa::Error`] when `seed` does not give a valid P-256 scalar.
    pub fn new(seed: u8) -> Result<Self, p256::ecdsa::Error> {
        Ok(Self {
            key: signing_key(seed)?,
            outage: Arc::new(AtomicBool::new(false)),
            public_key_reads: AtomicUsize::new(0),
            sign_calls: AtomicUsize::new(0),
        })
    }

    /// A signer over the certificate authority's key, [`AUTHORITY_SEED`].
    ///
    /// # Errors
    ///
    /// Returns [`p256::ecdsa::Error`] only if [`AUTHORITY_SEED`] stopped being a valid scalar.
    pub fn authority() -> Result<Self, p256::ecdsa::Error> {
        Self::new(AUTHORITY_SEED)
    }

    /// Replaces the outage switch with `outage`, so the caller can flip it from outside.
    #[must_use]
    pub fn with_outage(self, outage: Arc<AtomicBool>) -> Self {
        Self { outage, ..self }
    }

    /// A handle on the outage switch: storing `true` makes every `sign` fail.
    #[must_use]
    pub fn outage(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.outage)
    }

    /// The verifying key that checks this signer's signatures.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        *self.key.verifying_key()
    }

    /// How many times `public_key` has been called.
    #[must_use]
    pub fn public_key_reads(&self) -> usize {
        self.public_key_reads.load(Ordering::SeqCst)
    }

    /// How many times `sign` has been called, including calls that failed during an outage.
    #[must_use]
    pub fn sign_calls(&self) -> usize {
        self.sign_calls.load(Ordering::SeqCst)
    }
}

impl CertificateSignerPort for TestSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        self.public_key_reads.fetch_add(1, Ordering::SeqCst);
        spki_der(self.key.verifying_key())
    }

    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
        self.sign_calls.fetch_add(1, Ordering::SeqCst);
        if self.outage.load(Ordering::SeqCst) {
            return Err(PortError::unavailable("the test signer is down"));
        }
        sign_der(&self.key, message)
    }
}

/// A signer whose every call fails with [`PortError::Unavailable`].
#[derive(Debug, Clone, Copy, Default)]
pub struct FailingSigner;

impl CertificateSignerPort for FailingSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        Err(PortError::unavailable("signer offline"))
    }

    async fn sign(&self, _message: &[u8]) -> Result<Vec<u8>, PortError> {
        Err(PortError::unavailable("signer offline"))
    }
}

/// A signer that advertises the authority's public key but signs with another key, so its
/// signatures never verify under the advertised key.
#[derive(Debug)]
pub struct WrongKeySigner {
    advertised: VerifyingKey,
    signing: SigningKey,
}

impl WrongKeySigner {
    /// Advertises the key for [`AUTHORITY_SEED`] and signs with the key for seed `9`.
    ///
    /// # Errors
    ///
    /// Returns [`p256::ecdsa::Error`] if either fixed seed stops being a valid scalar.
    pub fn new() -> Result<Self, p256::ecdsa::Error> {
        Ok(Self {
            advertised: *signing_key(AUTHORITY_SEED)?.verifying_key(),
            signing: signing_key(9)?,
        })
    }
}

impl CertificateSignerPort for WrongKeySigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        spki_der(&self.advertised)
    }

    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
        sign_der(&self.signing, message)
    }
}

/// A signer that advertises the authority's public key and returns bytes that are not a DER
/// signature.
#[derive(Debug)]
pub struct GarbageSigner {
    key: SigningKey,
}

impl GarbageSigner {
    /// Advertises the key for [`AUTHORITY_SEED`].
    ///
    /// # Errors
    ///
    /// Returns [`p256::ecdsa::Error`] if [`AUTHORITY_SEED`] stops being a valid scalar.
    pub fn new() -> Result<Self, p256::ecdsa::Error> {
        Ok(Self {
            key: signing_key(AUTHORITY_SEED)?,
        })
    }
}

impl CertificateSignerPort for GarbageSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        spki_der(self.key.verifying_key())
    }

    async fn sign(&self, _message: &[u8]) -> Result<Vec<u8>, PortError> {
        Ok(b"not a der signature".to_vec())
    }
}

/// The DER `SubjectPublicKeyInfo` of `key`.
fn spki_der(key: &VerifyingKey) -> Result<Vec<u8>, PortError> {
    key.to_public_key_der()
        .map(|document| document.as_bytes().to_vec())
        .map_err(|error| PortError::backend(error.to_string()))
}

/// The DER ECDSA signature of `message` under `key`.
fn sign_der(key: &SigningKey, message: &[u8]) -> Result<Vec<u8>, PortError> {
    let signature: DerSignature = key
        .try_sign(message)
        .map_err(|error| PortError::backend(error.to_string()))?;
    Ok(signature.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use gang_spi::{CertificateSignerPort, PortError};
    use p256::ecdsa::signature::Verifier as _;
    use p256::ecdsa::{DerSignature, VerifyingKey};
    use p256::pkcs8::DecodePublicKey;

    use super::{FailingSigner, GarbageSigner, TestSigner, WrongKeySigner, signing_key};
    use crate::TestResult;

    /// Decodes the DER public key a signer advertises.
    fn decode_public_key(der: &[u8]) -> Result<VerifyingKey, Box<dyn std::error::Error>> {
        VerifyingKey::from_public_key_der(der).map_err(|error| error.to_string().into())
    }

    #[tokio::test]
    async fn signature_verifies_under_the_advertised_key() -> TestResult {
        let signer = TestSigner::new(1)?;
        let signature = signer.sign(b"message").await?;
        let advertised = decode_public_key(&signer.public_key().await?)?;
        let signature =
            DerSignature::try_from(signature.as_slice()).map_err(|error| error.to_string())?;
        advertised.verify(b"message", &signature)?;
        assert_eq!(advertised, signer.verifying_key());
        Ok(())
    }

    #[tokio::test]
    async fn same_seed_gives_the_same_public_key_and_other_seeds_differ() -> TestResult {
        let first = TestSigner::new(1)?.public_key().await?;
        let again = TestSigner::new(1)?.public_key().await?;
        let other = TestSigner::new(2)?.public_key().await?;
        assert_eq!(first, again);
        assert_ne!(first, other);
        Ok(())
    }

    #[test]
    fn only_valid_scalars_build_a_key() {
        assert!(signing_key(0).is_err());
        assert!(signing_key(255).is_err());
        assert!(signing_key(7).is_ok());
    }

    #[tokio::test]
    async fn calls_are_counted() -> TestResult {
        let signer = TestSigner::new(1)?;
        assert_eq!(signer.public_key_reads(), 0);
        assert_eq!(signer.sign_calls(), 0);
        signer.public_key().await?;
        signer.public_key().await?;
        signer.sign(b"message").await?;
        assert_eq!(signer.public_key_reads(), 2);
        assert_eq!(signer.sign_calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn outage_fails_sign_only_and_is_counted() -> TestResult {
        let signer = TestSigner::new(1)?;
        let switch = signer.outage();
        switch.store(true, Ordering::SeqCst);
        assert!(matches!(
            signer.sign(b"message").await,
            Err(PortError::Unavailable(_))
        ));
        assert!(signer.public_key().await.is_ok());
        assert_eq!(signer.sign_calls(), 1);
        switch.store(false, Ordering::SeqCst);
        assert!(signer.sign(b"message").await.is_ok());
        assert_eq!(signer.sign_calls(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn failing_signer_fails_every_call_as_unavailable() -> TestResult {
        let signer = FailingSigner;
        assert!(matches!(
            signer.public_key().await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            signer.sign(b"m").await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn with_outage_shares_the_switch_with_the_caller() -> TestResult {
        let outage = Arc::new(AtomicBool::new(false));
        let signer = TestSigner::authority()?.with_outage(Arc::clone(&outage));
        assert!(Arc::ptr_eq(&signer.outage(), &outage));
        assert!(signer.sign(b"message").await.is_ok());
        outage.store(true, Ordering::SeqCst);
        assert!(matches!(
            signer.sign(b"message").await,
            Err(PortError::Unavailable(_))
        ));
        assert!(signer.public_key().await.is_ok());
        outage.store(false, Ordering::SeqCst);
        assert!(signer.sign(b"message").await.is_ok());
        assert!(signer.public_key().await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn wrong_key_signature_does_not_verify_under_the_advertised_key() -> TestResult {
        let signer = WrongKeySigner::new()?;
        let advertised = decode_public_key(&signer.public_key().await?)?;
        let signature = signer.sign(b"message").await?;
        let signature =
            DerSignature::try_from(signature.as_slice()).map_err(|error| error.to_string())?;
        assert!(advertised.verify(b"message", &signature).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn garbage_signature_is_not_der_and_key_is_the_authority() -> TestResult {
        let signer = GarbageSigner::new()?;
        let signature = signer.sign(b"message").await?;
        assert!(DerSignature::try_from(signature.as_slice()).is_err());
        let advertised = decode_public_key(&signer.public_key().await?)?;
        assert_eq!(advertised, TestSigner::authority()?.verifying_key());
        Ok(())
    }
}
