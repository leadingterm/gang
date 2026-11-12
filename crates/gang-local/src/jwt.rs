use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::Timestamp;
use gang_types::{Audience, Es256Key, InstanceId, TokenTtl};
use p256::ecdsa::SigningKey;
use serde::Serialize;
use serde_json::json;

use crate::errors::LocalError;

/// Mints ES256 workload JWTs and publishes the verifying key as a JWKS document.
pub(crate) struct JwtMinter {
    key: Es256Key,
    issuer: String,
    audience: Audience,
    /// Lifetime of each minted token; `exp` is `iat` plus this, in whole seconds.
    ttl: TokenTtl,
}

/// The registered claims of a workload token.
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    iat: u64,
    nbf: u64,
    exp: u64,
}

impl JwtMinter {
    /// The key id is derived from the public key, so a restarted minter over the same key file keeps its `kid`.
    pub(crate) fn new(
        key: &SigningKey,
        issuer: String,
        audience: Audience,
        ttl: TokenTtl,
    ) -> Result<Self, LocalError> {
        // The existing key identifier is the base64url-encoded public x coordinate.
        let point = key.verifying_key().to_sec1_point(false);
        let x = point.x().ok_or(LocalError::WorkloadKey)?;
        let key_id = URL_SAFE_NO_PAD.encode(x);
        Ok(Self {
            key: Es256Key::new(key.clone(), key_id),
            issuer,
            audience,
            ttl,
        })
    }

    /// Signs a compact JWS whose `sub` is the instance id; times are whole unix seconds, not milliseconds.
    pub(crate) fn mint(
        &self,
        subject: &InstanceId,
        issued_at: Timestamp,
    ) -> Result<String, LocalError> {
        let iat = issued_at.as_unix_millis() / 1000;
        let exp = iat.saturating_add(self.ttl.as_units());
        let claims = Claims {
            iss: &self.issuer,
            sub: subject.as_str(),
            aud: self.audience.as_str(),
            iat,
            nbf: iat,
            exp,
        };
        self.key.sign(&claims).map_err(LocalError::Sign)
    }

    /// A JWK Set holding the single P-256 key that verifies this minter's tokens.
    #[must_use]
    pub(crate) fn jwks(&self) -> serde_json::Value {
        json!({ "keys": [self.key.jwk()] })
    }

    /// `OpenID` discovery document; `jwks_uri` is always served under the issuer.
    #[must_use]
    pub(crate) fn discovery(&self) -> serde_json::Value {
        json!({
            "issuer": self.issuer,
            "jwks_uri": format!("{}/jwks.json", self.issuer),
            "response_types_supported": ["id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["ES256"],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};

    fn test_key() -> SigningKey {
        // A fixed in-range scalar keeps the key and therefore the kid deterministic.
        SigningKey::from_slice(&[7u8; 32]).expect("fixed scalar is a valid P-256 key")
    }

    fn minter() -> Result<JwtMinter, LocalError> {
        JwtMinter::new(
            &test_key(),
            "http://127.0.0.1:8080".to_owned(),
            Audience::try_new("gang").expect("gang is a valid audience"),
            TokenTtl::MAX,
        )
    }

    fn instance() -> InstanceId {
        InstanceId::try_new("local-1".to_owned()).expect("local-1 is a valid instance id")
    }

    fn issued_at() -> Timestamp {
        Timestamp::from_unix_millis(1_700_000_000_123)
    }

    fn verifying_key_from_jwks(
        jwks: &serde_json::Value,
    ) -> Result<VerifyingKey, base64::DecodeError> {
        let jwk = &jwks["keys"][0];
        let x = URL_SAFE_NO_PAD.decode(jwk["x"].as_str().expect("x is a string"))?;
        let y = URL_SAFE_NO_PAD.decode(jwk["y"].as_str().expect("y is a string"))?;
        let mut sec1 = vec![0x04];
        sec1.extend_from_slice(&x);
        sec1.extend_from_slice(&y);
        Ok(VerifyingKey::from_sec1_bytes(&sec1).expect("jwks key is a valid point"))
    }

    #[test]
    fn mint_then_verify_with_jwks() -> Result<(), Box<dyn std::error::Error>> {
        let minter = minter()?;
        let token = minter.mint(&instance(), issued_at())?;

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let signing_input = format!("{}.{}", parts[0], parts[1]);

        let header: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0])?)?;
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], minter.key.kid());

        let claims: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1])?)?;
        assert_eq!(claims["iss"], "http://127.0.0.1:8080");
        assert_eq!(claims["sub"], "local-1");
        assert_eq!(claims["aud"], "gang");
        assert_eq!(claims["iat"], 1_700_000_000_u64);
        assert_eq!(claims["nbf"], 1_700_000_000_u64);
        assert_eq!(claims["exp"], 1_700_003_600_u64);

        let jwks = minter.jwks();
        assert_eq!(jwks["keys"][0]["kid"], minter.key.kid());
        assert_eq!(jwks["keys"][0]["kty"], "EC");
        assert_eq!(jwks["keys"][0]["crv"], "P-256");
        assert_eq!(jwks["keys"][0]["alg"], "ES256");
        assert_eq!(jwks["keys"][0]["use"], "sig");
        assert_eq!(jwks["keys"][0].as_object().map(|key| key.len()), Some(7));
        let verifying = verifying_key_from_jwks(&jwks)?;
        let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(parts[2])?)
            .expect("signature is 64 bytes");
        assert!(
            verifying
                .verify(signing_input.as_bytes(), &signature)
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn tampered_payload_fails_verification() -> Result<(), Box<dyn std::error::Error>> {
        let minter = minter()?;
        let token = minter.mint(&instance(), issued_at())?;
        let parts: Vec<&str> = token.split('.').collect();

        let mut claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1])?)?;
        claims["sub"] = serde_json::Value::from("local-2");
        let forged_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let forged_input = format!("{}.{forged_payload}", parts[0]);

        let verifying = verifying_key_from_jwks(&minter.jwks())?;
        let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(parts[2])?)
            .expect("signature is 64 bytes");
        assert!(
            verifying
                .verify(forged_input.as_bytes(), &signature)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn discovery_points_at_the_jwks() -> Result<(), Box<dyn std::error::Error>> {
        let discovery = minter()?.discovery();
        assert_eq!(discovery["issuer"], "http://127.0.0.1:8080");
        assert_eq!(discovery["jwks_uri"], "http://127.0.0.1:8080/jwks.json");
        assert_eq!(discovery["response_types_supported"], json!(["id_token"]));
        assert_eq!(discovery["subject_types_supported"], json!(["public"]));
        assert_eq!(
            discovery["id_token_signing_alg_values_supported"],
            json!(["ES256"])
        );
        Ok(())
    }

    #[test]
    fn key_id_is_stable_for_a_key() -> Result<(), Box<dyn std::error::Error>> {
        let first = minter()?;
        let second = minter()?;
        assert_eq!(first.key.kid(), second.key.kid());

        let other = JwtMinter::new(
            &SigningKey::from_slice(&[9u8; 32]).expect("fixed scalar is a valid P-256 key"),
            "http://127.0.0.1:8080".to_owned(),
            Audience::try_new("gang").expect("gang is a valid audience"),
            TokenTtl::MAX,
        )?;
        assert_ne!(first.key.kid(), other.key.kid());

        // The kid is the URL-safe base64 of the affine x coordinate.
        let x = URL_SAFE_NO_PAD.decode(first.key.kid())?;
        assert_eq!(x.len(), 32);
        let jwks = first.jwks();
        assert_eq!(jwks["keys"][0]["x"], first.key.kid());
        Ok(())
    }
}
