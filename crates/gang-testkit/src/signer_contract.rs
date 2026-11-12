//! The contract suite for [`CertificateSignerPort`].
//!
//! Each adapter supplies a signer to the same checks of its algorithm, public key and signatures.

use gang_spi::{CertificateSignerPort, SignatureAlgorithm};
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;

use crate::TestResult;

/// Checks that a signer exposes a stable P-256 public key and produces valid DER signatures.
///
/// # Errors
///
/// Fails when the signer fails, or its public key or signatures cannot be parsed or verified.
///
/// # Panics
///
/// Panics when the signer reports the wrong algorithm, changes its public key, or a signature
/// verifies for a different message.
pub async fn signer_contract<S: CertificateSignerPort>(signer: &S) -> TestResult {
    assert_eq!(signer.algorithm(), SignatureAlgorithm::EcdsaP256Sha256);
    let public_key = signer.public_key().await?;
    assert_eq!(public_key, signer.public_key().await?);
    let key = VerifyingKey::from_public_key_der(&public_key)?;

    let first = b"first contract message";
    let second = b"second contract message";
    let first_signature = Signature::from_der(&signer.sign(first).await?)?;
    let second_signature = Signature::from_der(&signer.sign(second).await?)?;
    key.verify(first, &first_signature)?;
    key.verify(second, &second_signature)?;
    assert!(key.verify(second, &first_signature).is_err());
    assert!(key.verify(first, &second_signature).is_err());
    Ok(())
}
