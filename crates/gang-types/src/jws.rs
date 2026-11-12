//! ES256 signing and public-key publication.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KeyType {
    #[serde(rename = "EC")]
    Ec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Curve {
    #[serde(rename = "P-256")]
    P256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Algorithm {
    #[serde(rename = "ES256")]
    Es256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KeyUse {
    #[serde(rename = "sig")]
    Signature,
}

#[derive(Serialize)]
struct Header<'a> {
    alg: &'static str,
    typ: &'static str,
    kid: &'a str,
}

/// A P-256 public key as a JWK, the form a JWKS document publishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    kty: KeyType,
    crv: Curve,
    kid: String,
    alg: Algorithm,
    #[serde(rename = "use")]
    key_use: KeyUse,
    x: String,
    y: String,
}

impl Jwk {
    /// The key identifier.
    #[must_use]
    pub fn kid(&self) -> &str {
        &self.kid
    }

    /// The base64url-encoded public x coordinate.
    #[must_use]
    pub fn x(&self) -> &str {
        &self.x
    }

    /// The base64url-encoded public y coordinate.
    #[must_use]
    pub fn y(&self) -> &str {
        &self.y
    }
}

/// An ES256 signing key and its publishable public JWK.
///
/// ```
/// use gang_types::Es256Key;
/// use p256::ecdsa::SigningKey;
/// use serde_json::json;
///
/// let signing = SigningKey::from_slice(&[1; 32])?;
/// let key = Es256Key::new(signing, "example");
/// let token = key.sign(&json!({"sub": "alice"}))?;
/// assert_eq!(token.split('.').count(), 3);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Es256Key {
    signing: SigningKey,
    jwk: Jwk,
}

impl std::fmt::Debug for Es256Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Es256Key")
            .field("jwk", &self.jwk)
            .finish_non_exhaustive()
    }
}

impl Es256Key {
    /// Constructs a signer whose tokens and JWK carry `kid`.
    #[must_use]
    pub fn new(signing: SigningKey, kid: impl Into<String>) -> Self {
        let kid = kid.into();
        let point = signing.verifying_key().to_sec1_point(false);
        // An uncompressed SEC1 point is 0x04 || x || y, with 32 bytes per coordinate.
        let (x, y) = point.as_bytes()[1..].split_at(32);
        let jwk = Jwk {
            kty: KeyType::Ec,
            crv: Curve::P256,
            kid,
            alg: Algorithm::Es256,
            key_use: KeyUse::Signature,
            x: URL_SAFE_NO_PAD.encode(x),
            y: URL_SAFE_NO_PAD.encode(y),
        };
        Self { signing, jwk }
    }

    /// The key identifier used in tokens and the JWK.
    #[must_use]
    pub fn kid(&self) -> &str {
        self.jwk.kid()
    }

    /// The public key ready to publish in a JWKS.
    #[must_use]
    pub fn jwk(&self) -> &Jwk {
        &self.jwk
    }

    /// Signs claims as a compact ES256 JWT.
    ///
    /// # Errors
    ///
    /// Returns an error if the claims cannot be serialized as JSON.
    pub fn sign<T: Serialize>(&self, claims: &T) -> Result<String, serde_json::Error> {
        let header = serde_json::to_vec(&Header {
            alg: "ES256",
            typ: "JWT",
            kid: self.kid(),
        })?;
        let payload = serde_json::to_vec(claims)?;
        let mut token = String::with_capacity((header.len() + payload.len()) * 4 / 3 + 90);
        URL_SAFE_NO_PAD.encode_string(header, &mut token);
        token.push('.');
        URL_SAFE_NO_PAD.encode_string(payload, &mut token);
        let signature: Signature = self.signing.sign(token.as_bytes());
        token.push('.');
        URL_SAFE_NO_PAD.encode_string(signature.to_bytes(), &mut token);
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
    use serde_json::json;

    use super::*;

    fn fixed_key() -> Es256Key {
        let mut scalar = [0; 32];
        scalar[31] = 1;
        Es256Key::new(
            SigningKey::from_slice(&scalar).expect("one is a valid P-256 scalar"),
            "key-1",
        )
    }

    fn verify(token: &str, jwk: &Jwk) -> Result<(), Box<dyn std::error::Error>> {
        let (message, encoded_signature) = token.rsplit_once('.').ok_or("missing signature")?;
        let mut point = vec![4];
        point.extend(URL_SAFE_NO_PAD.decode(jwk.x())?);
        point.extend(URL_SAFE_NO_PAD.decode(jwk.y())?);
        let key = VerifyingKey::from_sec1_bytes(&point)?;
        let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(encoded_signature)?)?;
        key.verify(message.as_bytes(), &signature)?;
        Ok(())
    }

    #[test]
    fn token_verifies_with_published_coordinates_and_exact_header()
    -> Result<(), Box<dyn std::error::Error>> {
        let key = fixed_key();
        let token = key.sign(&json!({"sub": "alice"}))?;
        verify(&token, key.jwk())?;
        let header = token.split('.').next().ok_or("missing header")?;
        let header: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header)?)?;
        assert_eq!(
            header,
            json!({"alg": "ES256", "typ": "JWT", "kid": "key-1"})
        );
        Ok(())
    }

    #[test]
    fn fixed_key_publishes_expected_jwk() -> Result<(), Box<dyn std::error::Error>> {
        let key = fixed_key();
        assert_eq!(
            serde_json::to_value(key.jwk())?,
            json!({
                "kty": "EC", "crv": "P-256", "kid": "key-1", "alg": "ES256", "use": "sig",
                "x": "axfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5RdiYwpY",
                "y": "T-NC4v4af5uO5-tKfA-eFivOM1drMV7Oy7ZAaDe_UfU"
            })
        );
        Ok(())
    }

    #[test]
    fn tampered_payload_does_not_verify() -> Result<(), Box<dyn std::error::Error>> {
        let key = fixed_key();
        let token = key.sign(&json!({"sub": "alice"}))?;
        let (header, rest) = token.split_once('.').ok_or("missing payload")?;
        let (_, signature) = rest.split_once('.').ok_or("missing signature")?;
        let tampered = format!(
            "{header}.{}.{signature}",
            URL_SAFE_NO_PAD.encode(br#"{"sub":"bob"}"#)
        );
        assert!(verify(&tampered, key.jwk()).is_err());
        Ok(())
    }

    #[test]
    fn signing_same_claims_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let key = fixed_key();
        let claims = json!({"sub": "alice"});
        assert_eq!(key.sign(&claims)?, key.sign(&claims)?);
        Ok(())
    }
}
