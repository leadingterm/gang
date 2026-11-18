//! Domain-separated controller-to-controller authentication proofs.

use std::{fmt, time::Duration};

use der::Decode;
use gang_clock::Timestamp;
use gang_spi::{CertificateSignerPort, ControllerId, SignatureAlgorithm};
use gang_types::NonZeroDuration;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use spki::SubjectPublicKeyInfoOwned;

use crate::{MtlsError, ProofError, signing};

/// Maximum controller proof header value size in bytes.
pub const MAX_PROOF_BYTES: usize = 512;
/// Header carrying a controller proof.
pub const PROOF_HEADER: &str = "x-gang-controller";
const DOMAIN: &[u8] = b"gang-controller-proof-v1\0";

/// The CA public key controller proofs verify under.
#[derive(Clone)]
pub struct ControllerKey(VerifyingKey);

impl ControllerKey {
    /// Decodes a P-256 `SubjectPublicKeyInfo`.
    ///
    /// # Errors
    /// Returns [`ProofError::Key`] for an invalid or unsupported key.
    pub fn from_spki(spki_der: &[u8]) -> Result<Self, ProofError> {
        let spki = SubjectPublicKeyInfoOwned::from_der(spki_der)
            .map_err(|error| ProofError::Key(MtlsError::from(error)))?;
        signing::verifying_key(&spki)
            .map(Self)
            .map_err(ProofError::Key)
    }
}

/// A freshly signed proof header value.
pub struct ControllerProof(String);

impl ControllerProof {
    /// Returns the header value; callers should treat it as sensitive.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ControllerProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ControllerProof(..)")
    }
}

fn valid_controller(controller: &str) -> bool {
    (1..=64).contains(&controller.len())
        && controller
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn message(issuer: &str, expires: &str, controller: &str) -> Vec<u8> {
    let mut message =
        Vec::with_capacity(DOMAIN.len() + issuer.len() + expires.len() + controller.len() + 2);
    message.extend_from_slice(DOMAIN);
    message.extend_from_slice(issuer.as_bytes());
    message.push(0);
    message.extend_from_slice(expires.as_bytes());
    message.push(0);
    message.extend_from_slice(controller.as_bytes());
    message
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    result
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn decode_hex(text: &str, out: &mut [u8; 72]) -> Result<usize, ProofError> {
    if !(16..=144).contains(&text.len()) || !text.len().is_multiple_of(2) {
        return Err(ProofError::Malformed);
    }
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = digit(pair[0]).ok_or(ProofError::Malformed)?;
        let low = digit(pair[1]).ok_or(ProofError::Malformed)?;
        out[index] = (high << 4) | low;
    }
    Ok(text.len() / 2)
}

/// Signs a proof valid until `now + ttl` in Unix milliseconds.
///
/// # Errors
/// Returns [`ProofError`] if the inputs cannot be encoded or the signer fails.
pub async fn mint<S: CertificateSignerPort>(
    signer: &S,
    issuer: &str,
    controller: &ControllerId,
    now: Timestamp,
    ttl: NonZeroDuration,
) -> Result<ControllerProof, ProofError> {
    if signer.algorithm() != SignatureAlgorithm::EcdsaP256Sha256 {
        return Err(ProofError::Algorithm);
    }
    if !valid_controller(controller.as_str()) {
        return Err(ProofError::Controller);
    }
    if issuer.as_bytes().contains(&0) {
        return Err(ProofError::Issuer);
    }
    let ttl_ms = u64::try_from(ttl.get().as_millis()).map_err(|_| ProofError::Overflow)?;
    let expires = now
        .as_unix_millis()
        .checked_add(ttl_ms)
        .ok_or(ProofError::Overflow)?
        .to_string();
    let signature = signer
        .sign(&message(issuer, &expires, controller.as_str()))
        .await
        .map_err(ProofError::Sign)?;
    Ok(ControllerProof(format!(
        "gang-controller.v1.{expires}.{}.{}",
        controller.as_str(),
        encode_hex(&signature)
    )))
}

/// Verifies a proof and returns the controller it names.
///
/// The expiry is exclusive, with `skew` allowed on both sides of the lifetime window.
///
/// # Errors
/// Returns [`ProofError`] if the proof is malformed, outside the window, or invalid.
pub fn verify(
    key: &ControllerKey,
    proof: &str,
    issuer: &str,
    now: Timestamp,
    skew: Duration,
    max_ttl: NonZeroDuration,
) -> Result<ControllerId, ProofError> {
    if proof.len() > MAX_PROOF_BYTES {
        return Err(ProofError::Malformed);
    }
    let mut fields = proof.split('.');
    let (Some(prefix), Some(version), Some(expires), Some(controller), Some(signature), None) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return Err(ProofError::Malformed);
    };
    if prefix != "gang-controller" || version != "v1" {
        return Err(ProofError::Malformed);
    }
    if expires.is_empty()
        || expires.len() > 20
        || (expires.len() > 1 && expires.starts_with('0'))
        || !expires.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ProofError::Malformed);
    }
    let expires_ms = expires.parse::<u64>().map_err(|_| ProofError::Malformed)?;
    if !valid_controller(controller) {
        return Err(ProofError::Malformed);
    }
    let mut der = [0; 72];
    let len = decode_hex(signature, &mut der)?;
    let skew_ms = u64::try_from(skew.as_millis()).map_err(|_| ProofError::Overflow)?;
    let max_ms = u64::try_from(max_ttl.get().as_millis()).map_err(|_| ProofError::Overflow)?;
    let now_ms = now.as_unix_millis();
    let valid_until = expires_ms
        .checked_add(skew_ms)
        .ok_or(ProofError::Overflow)?;
    if valid_until <= now_ms {
        return Err(ProofError::Expired);
    }
    let horizon = now_ms
        .checked_add(max_ms)
        .and_then(|n| n.checked_add(skew_ms))
        .ok_or(ProofError::Overflow)?;
    if expires_ms > horizon {
        return Err(ProofError::TooLong);
    }
    if issuer.as_bytes().contains(&0) {
        return Err(ProofError::Issuer);
    }
    let signature = Signature::from_der(&der[..len]).map_err(|_| ProofError::Signature)?;
    key.0
        .verify(&message(issuer, expires, controller), &signature)
        .map_err(|_| ProofError::Signature)?;
    Ok(ControllerId::new(controller))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::TestSigner;

    const ISSUER: &str = "https://gang.example";
    const NOW: u64 = 1_700_000_000_123;

    async fn fixture() -> (TestSigner, ControllerKey, ControllerProof) {
        let signer = TestSigner::new(1).unwrap();
        let key = ControllerKey::from_spki(&signer.public_key().await.unwrap()).unwrap();
        let proof = mint(
            &signer,
            ISSUER,
            &ControllerId::new("a"),
            Timestamp::from_unix_millis(NOW),
            NonZeroDuration::from_millis(100),
        )
        .await
        .unwrap();
        (signer, key, proof)
    }

    fn check(
        key: &ControllerKey,
        proof: &str,
        issuer: &str,
        now: u64,
        skew: u64,
        ttl: u64,
    ) -> Result<ControllerId, ProofError> {
        verify(
            key,
            proof,
            issuer,
            Timestamp::from_unix_millis(now),
            Duration::from_millis(skew),
            NonZeroDuration::from_millis(ttl),
        )
    }

    #[tokio::test]
    async fn roundtrip_and_time_boundaries() {
        let (_, key, proof) = fixture().await;
        assert_eq!(format!("{proof:?}"), "ControllerProof(..)");
        assert_eq!(
            check(&key, proof.as_str(), ISSUER, NOW, 0, 100).unwrap(),
            ControllerId::new("a")
        );
        assert!(check(&key, proof.as_str(), ISSUER, NOW + 109, 10, 100).is_ok());
        assert!(matches!(
            check(&key, proof.as_str(), ISSUER, NOW + 110, 10, 100),
            Err(ProofError::Expired)
        ));
        assert!(check(&key, proof.as_str(), ISSUER, NOW, 10, 90).is_ok());
        assert!(matches!(
            check(&key, proof.as_str(), ISSUER, NOW, 9, 90),
            Err(ProofError::TooLong)
        ));
        assert!(matches!(
            check(&key, proof.as_str(), ISSUER, NOW, 0, 99),
            Err(ProofError::TooLong)
        ));
    }

    #[tokio::test]
    async fn malformed_and_signature_checks() {
        let (_, key, proof) = fixture().await;
        let parts: Vec<_> = proof.as_str().split('.').collect();
        let make = |expiry: &str, controller: &str, signature: &str| {
            format!("gang-controller.v1.{expiry}.{controller}.{signature}")
        };
        for expiry in [
            "+1",
            "01",
            "",
            "123456789012345678901",
            "1x",
            "18446744073709551616",
        ] {
            assert!(matches!(
                check(&key, &make(expiry, "a", parts[4]), ISSUER, NOW, 0, 100),
                Err(ProofError::Malformed)
            ));
        }
        for controller in ["", "a.b", "é", &"a".repeat(65)] {
            assert!(matches!(
                check(
                    &key,
                    &make(parts[2], controller, parts[4]),
                    ISSUER,
                    NOW,
                    0,
                    100
                ),
                Err(ProofError::Malformed)
            ));
        }
        for signature in [
            "",
            "00000000000000",
            "AA00000000000000",
            "é000000000000000",
            "00000000000000000",
        ] {
            assert!(matches!(
                check(&key, &make(parts[2], "a", signature), ISSUER, NOW, 0, 100),
                Err(ProofError::Malformed)
            ));
        }
        for value in [
            "gang-controller.v2.1.a.0000000000000000".to_string(),
            "gang-controller.v1.1.a".to_string(),
            format!("{}.extra", proof.as_str()),
            "x".repeat(513),
        ] {
            assert!(matches!(
                check(&key, &value, ISSUER, NOW, 0, 100),
                Err(ProofError::Malformed)
            ));
        }
        assert!(matches!(
            check(&key, proof.as_str(), "https://other.example", NOW, 0, 100),
            Err(ProofError::Signature)
        ));
        assert!(matches!(
            check(&key, &make(parts[2], "b", parts[4]), ISSUER, NOW, 0, 100),
            Err(ProofError::Signature)
        ));
        assert!(matches!(
            check(
                &key,
                &make(parts[2], "a", &parts[4][..parts[4].len() - 2]),
                ISSUER,
                NOW,
                0,
                100
            ),
            Err(ProofError::Signature)
        ));
        assert!(matches!(
            check(
                &key,
                &make(parts[2], "a", &format!("{}00", parts[4])),
                ISSUER,
                NOW,
                0,
                100
            ),
            Err(ProofError::Signature | ProofError::Malformed)
        ));
        let other = TestSigner::new(2).unwrap();
        let other_key = ControllerKey::from_spki(&other.public_key().await.unwrap()).unwrap();
        assert!(matches!(
            check(&other_key, proof.as_str(), ISSUER, NOW, 0, 100),
            Err(ProofError::Signature)
        ));
    }

    #[tokio::test]
    async fn domain_separation_and_input_errors() {
        let (signer, key, proof) = fixture().await;
        let jws_input = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJhIn0";
        let signature = signer.sign(jws_input.as_bytes()).await.unwrap();
        let jws = format!("{jws_input}.{}", encode_hex(&signature));
        assert!(matches!(
            check(&key, &jws, ISSUER, NOW, 0, 100),
            Err(ProofError::Malformed)
        ));
        let parts: Vec<_> = proof.as_str().split('.').collect();
        let forged = format!(
            "gang-controller.v1.{}.a.{}",
            parts[2],
            encode_hex(&signature)
        );
        assert!(matches!(
            check(&key, &forged, ISSUER, NOW, 0, 100),
            Err(ProofError::Signature)
        ));
        assert!(matches!(
            mint(
                &signer,
                ISSUER,
                &ControllerId::new("a.b"),
                Timestamp::from_unix_millis(NOW),
                NonZeroDuration::from_millis(1)
            )
            .await,
            Err(ProofError::Controller)
        ));
        assert!(matches!(
            mint(
                &signer,
                "a\0b",
                &ControllerId::new("a"),
                Timestamp::from_unix_millis(NOW),
                NonZeroDuration::from_millis(1)
            )
            .await,
            Err(ProofError::Issuer)
        ));
        assert!(matches!(
            check(&key, proof.as_str(), "a\0b", NOW, 0, 100),
            Err(ProofError::Issuer)
        ));
        assert!(matches!(
            mint(
                &signer,
                ISSUER,
                &ControllerId::new("a"),
                Timestamp::from_unix_millis(u64::MAX - 1),
                NonZeroDuration::from_millis(2)
            )
            .await,
            Err(ProofError::Overflow)
        ));
        let max = format!("gang-controller.v1.{}.a.{}", u64::MAX, parts[4]);
        assert!(matches!(
            check(&key, &max, ISSUER, NOW, 1, 100),
            Err(ProofError::Overflow)
        ));
        assert!(matches!(
            verify(
                &key,
                proof.as_str(),
                ISSUER,
                Timestamp::from_unix_millis(NOW),
                Duration::MAX,
                NonZeroDuration::from_millis(100)
            ),
            Err(ProofError::Overflow)
        ));
        let mut wrong_curve = signer.public_key().await.unwrap();
        let oid = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];
        let position = wrong_curve
            .windows(oid.len())
            .position(|window| window == oid)
            .unwrap();
        wrong_curve[position + oid.len() - 1] = 8;
        assert!(matches!(
            ControllerKey::from_spki(&wrong_curve),
            Err(ProofError::Key(_))
        ));
    }
}
