//! ES256 key generation for in-process token issuers.

use gang_types::Es256Key;
use p256::SecretKey;
use p256::elliptic_curve::Generate as _;

/// Generates a fresh ES256 signing key identified by `kid`.
///
/// # Errors
///
/// Returns an error if the operating system's random source fails.
pub fn generate_es256_key(kid: impl Into<String>) -> Result<Es256Key, getrandom::Error> {
    let secret = SecretKey::try_generate()?;
    Ok(Es256Key::new(secret.into(), kid))
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
    use serde_json::json;

    use super::*;

    #[test]
    fn generated_key_signs_tokens_verifiable_with_its_jwk() {
        let key = generate_es256_key("test-key").expect("the random source generates a key");
        let token = key
            .sign(&json!({"sub": "alice"}))
            .expect("claims serialize");
        let (message, encoded_signature) = token.rsplit_once('.').expect("token has a signature");
        let mut point = vec![4];
        point.extend(
            URL_SAFE_NO_PAD
                .decode(key.jwk().x())
                .expect("x is base64url"),
        );
        point.extend(
            URL_SAFE_NO_PAD
                .decode(key.jwk().y())
                .expect("y is base64url"),
        );
        let verifier = VerifyingKey::from_sec1_bytes(&point).expect("the JWK is a P-256 point");
        let signature = Signature::from_slice(
            &URL_SAFE_NO_PAD
                .decode(encoded_signature)
                .expect("signature is base64url"),
        )
        .expect("the signature is 64 bytes");
        assert!(verifier.verify(message.as_bytes(), &signature).is_ok());
        assert_eq!(key.kid(), "test-key");
    }
}
