//! Signing through the certificate authority port, and the ECDSA P-256 key and algorithm helpers.

use const_oid::ObjectIdentifier;
use const_oid::db::rfc5912::{ECDSA_WITH_SHA_256, ID_EC_PUBLIC_KEY, SECP_256_R_1};
use der::asn1::BitString;
use gang_spi::CertificateSignerPort;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use spki::{AlgorithmIdentifierOwned, SubjectPublicKeyInfoOwned};

use crate::errors::MtlsError;

/// Signs the DER `tbs_der` through `signer` and checks the signature against `public_key`.
///
/// # Errors
///
/// Returns [`MtlsError::Signer`] when the signer fails, [`MtlsError::SignatureInvalid`] when
/// the signature is not DER ECDSA or does not verify with `public_key`, and
/// [`MtlsError::Encoding`] when the signature cannot be wrapped as a bit string.
pub(crate) async fn sign<S: CertificateSignerPort>(
    signer: &S,
    public_key: &VerifyingKey,
    tbs_der: &[u8],
) -> Result<BitString, MtlsError> {
    let signature_der = signer.sign(tbs_der).await?;
    let signature = Signature::from_der(&signature_der).map_err(|_| MtlsError::SignatureInvalid)?;
    public_key
        .verify(tbs_der, &signature)
        .map_err(|_| MtlsError::SignatureInvalid)?;
    Ok(BitString::from_bytes(&signature_der)?)
}

/// Returns the algorithm identifier for ecdsa-with-SHA256, which has no parameters.
pub(crate) fn ecdsa_sha256() -> AlgorithmIdentifierOwned {
    AlgorithmIdentifierOwned {
        oid: ECDSA_WITH_SHA_256,
        parameters: None,
    }
}

/// Decodes a DER `SubjectPublicKeyInfo` as an ECDSA P-256 verifying key.
///
/// # Errors
///
/// Returns [`MtlsError::UnsupportedKey`] unless the algorithm is id-ecPublicKey on prime256v1
/// and the key bits are a valid SEC1 point.
pub(crate) fn verifying_key(spki: &SubjectPublicKeyInfoOwned) -> Result<VerifyingKey, MtlsError> {
    if spki.algorithm.oid != ID_EC_PUBLIC_KEY {
        return Err(MtlsError::UnsupportedKey);
    }
    let curve = spki
        .algorithm
        .parameters
        .as_ref()
        .map(|parameters| parameters.clone().decode_as::<ObjectIdentifier>())
        .transpose()
        .map_err(|_| MtlsError::UnsupportedKey)?;
    if curve != Some(SECP_256_R_1) {
        return Err(MtlsError::UnsupportedKey);
    }
    VerifyingKey::from_sec1_bytes(spki.subject_public_key.raw_bytes())
        .map_err(|_| MtlsError::UnsupportedKey)
}
