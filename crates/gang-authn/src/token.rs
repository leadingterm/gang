//! Gang's bearer tokens: ES256 JWTs that name the identity they were issued to.
//!
//! Every token names the [`TokenUse`] it is valid for in its `use` claim, so a token minted for a
//! browser session cannot be presented as a client bearer credential, or the reverse.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::{CertificateSignerPort, IssuedToken, PortError, SignatureAlgorithm};
use gang_types::{AccessToken, Identity, Url};
use p256::ecdsa::Signature;
use p256::ecdsa::signature::Verifier as _;
use serde::{Deserialize, Serialize};

use crate::errors::TokenError;
use crate::jose::{self, ALGORITHM, ClaimsIn, ClaimsOut, HeaderIn, HeaderOut, JWT_TYPE};
use crate::key::TokenKey;

/// The audience every Gang token names.
pub const AUDIENCE: &str = "gang";

/// What a token may be presented for. Carried in the `use` claim; a verifier accepts a token only
/// for the use it expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenUse {
    /// Presented in `Authorization: Bearer`.
    Client,
    /// Presented only in the session cookie.
    Session,
}

/// The origin of `endpoint` as an issuer: `scheme://host[:port]`, with any path, query, or
/// default port removed.
#[must_use]
pub fn issuer_of(endpoint: &Url) -> String {
    endpoint.origin().ascii_serialization()
}

/// Checks an issuer against the endpoint's serialized origin without allocating.
fn issued_by(endpoint: &Url, iss: &str) -> bool {
    let Some(host) = endpoint.host_str() else {
        return false;
    };
    let Some(rest) = iss
        .strip_prefix(endpoint.scheme())
        .and_then(|value| value.strip_prefix("://"))
        .and_then(|value| value.strip_prefix(host))
    else {
        return false;
    };
    match endpoint.port() {
        None => rest.is_empty(),
        Some(port) => rest.strip_prefix(':').is_some_and(|value| {
            !value.is_empty()
                && (value.len() == 1 || !value.starts_with('0'))
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && value.parse::<u16>() == Ok(port)
        }),
    }
}

/// Mints a bearer token for `identity`, valid from `now` for `ttl`, signed by `signer`.
///
/// `key` is the public key `signer` holds; its key id goes in the token header.
///
/// `usage` is the use the token is valid for; it is written to the `use` claim.
///
/// # Errors
///
/// [`PortError::Config`] when `signer` is not an ECDSA P-256 key, or the signer's error when it
/// cannot sign.
pub async fn mint<S: CertificateSignerPort>(
    signer: &S,
    key: &TokenKey,
    issuer: &str,
    identity: &Identity,
    now: Timestamp,
    ttl: Duration,
    usage: TokenUse,
) -> Result<IssuedToken, PortError> {
    if signer.algorithm() != SignatureAlgorithm::EcdsaP256Sha256 {
        return Err(PortError::config(
            "token signing requires an ecdsa p-256 key",
        ));
    }
    let issued_at = now.as_unix_millis() / 1000;
    let expires_at = (now + ttl).as_unix_millis().div_ceil(1000);
    let subject = identity.to_string();
    let header = jose::encode_segment(&HeaderOut {
        alg: ALGORITHM,
        typ: JWT_TYPE,
        kid: key.kid(),
    })
    .map_err(PortError::backend)?;
    let claims = jose::encode_segment(&ClaimsOut {
        iss: issuer,
        aud: AUDIENCE,
        sub: &subject,
        iat: issued_at,
        nbf: issued_at,
        exp: expires_at,
        usage,
    })
    .map_err(PortError::backend)?;
    let signing_input = format!("{header}.{claims}");
    let der = signer.sign(signing_input.as_bytes()).await?;
    let signature = jose::encode_signature(&der)?;
    Ok(IssuedToken {
        token: AccessToken::new(format!("{signing_input}.{signature}")),
        expires_in: ttl,
    })
}

/// The claims of a verified token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The identity the token was issued to.
    pub identity: Identity,
    /// The last instant the token is accepted, `exp` plus the clock skew allowance.
    pub valid_until: Timestamp,
}

/// Verifies a bearer token against `key` and the issuer origin of `endpoint`, at time `now`.
///
/// `skew` widens the validity window on both ends to tolerate clock drift between the signer
/// and the verifier.
///
/// `usage` is the use the caller expects; a token issued for another use is refused.
///
/// # Errors
///
/// [`TokenError`] naming the first check the token fails: its shape, algorithm, key id,
/// signature, issuer, audience, use, validity window, or subject.
pub fn verify(
    token: &str,
    key: &TokenKey,
    endpoint: &Url,
    now: Timestamp,
    skew: Duration,
    usage: TokenUse,
) -> Result<Verified, TokenError> {
    let (header_segment, claims_segment, signature_segment) = jose::segments(token)?;
    let mut buffer = [0; jose::MAX_SEGMENT_DECODED];
    let header: HeaderIn<'_> = jose::parse_segment(header_segment, &mut buffer)?;
    if header.alg != ALGORITHM {
        return Err(TokenError::Algorithm);
    }
    if header.typ.as_deref().is_some_and(|typ| typ != JWT_TYPE) {
        return Err(TokenError::Malformed);
    }
    if header.kid != key.kid() {
        return Err(TokenError::Signature);
    }
    drop(header);
    let signing_input = &token[..header_segment.len() + 1 + claims_segment.len()];
    let signature_bytes = jose::decode_signature_segment(signature_segment)?;
    let signature = Signature::from_slice(&signature_bytes).map_err(|_| TokenError::Signature)?;
    key.verifying()
        .verify(signing_input.as_bytes(), &signature)
        .map_err(|_| TokenError::Signature)?;

    let claims: ClaimsIn<'_> = jose::parse_segment(claims_segment, &mut buffer)?;
    if !issued_by(endpoint, &claims.iss) {
        return Err(TokenError::Issuer);
    }
    if claims.aud != AUDIENCE {
        return Err(TokenError::Audience);
    }
    if claims.usage != usage {
        return Err(TokenError::Usage);
    }
    let valid_until = Timestamp::from_unix_millis(claims.exp.saturating_mul(1000)) + skew;
    if valid_until <= now {
        return Err(TokenError::Expired);
    }
    let valid_from = Timestamp::from_unix_millis(claims.nbf.saturating_mul(1000)) - skew;
    if valid_from > now {
        return Err(TokenError::NotYetValid);
    }
    let identity = claims
        .sub
        .parse::<Identity>()
        .map_err(|_| TokenError::Subject)?;
    Ok(Verified {
        identity,
        valid_until,
    })
}

#[cfg(test)]
mod tests {
    use gang_testkit::TestSigner;

    use super::*;
    use crate::jose::MAX_TOKEN_BYTES;

    const ISSUER: &str = "https://gang.example.com";
    const NOW_MS: u64 = 1_800_000_000_000;
    const TTL: Duration = Duration::from_mins(1);
    const SKEW: Duration = Duration::from_secs(5);
    const SKEW_MS: u64 = 5_000;

    async fn fixture(seed: u8) -> (TestSigner, TokenKey) {
        let signer = TestSigner::new(seed).expect("the seed is a valid scalar");
        let der = signer.public_key().await.expect("test public key encodes");
        let key = TokenKey::from_spki(&der).expect("test public key loads");
        (signer, key)
    }

    /// A client token for alice issued at `issued_ms` with the shared TTL.
    async fn mint_at(signer: &TestSigner, key: &TokenKey, issued_ms: u64) -> String {
        let identity: Identity = "example-idp:alice@example.com"
            .parse()
            .expect("the subject is an identity");
        mint(
            signer,
            key,
            ISSUER,
            &identity,
            Timestamp::from_unix_millis(issued_ms),
            TTL,
            TokenUse::Client,
        )
        .await
        .expect("token mints")
        .token
        .expose()
        .to_owned()
    }

    fn verify_at(token: &str, key: &TokenKey, millis: u64) -> Result<Verified, TokenError> {
        verify(
            token,
            key,
            &Url::parse(ISSUER).expect("issuer is a URL"),
            Timestamp::from_unix_millis(millis),
            SKEW,
            TokenUse::Client,
        )
    }

    #[test]
    fn issued_by_matches_serialized_origin() {
        for endpoint in [
            "https://gang.example.com:443/path",
            "https://gang.example.com:8443/path",
            "https://[::1]:8443/path",
        ] {
            let endpoint = Url::parse(endpoint).expect("endpoint is a URL");
            let issuer = issuer_of(&endpoint);
            for candidate in [
                issuer.as_str(),
                "https://other.example.com",
                "https://gang.example.com:1234",
                "http://gang.example.com",
                "https://gang.example.com/",
                "https://gang.example.com:08443",
            ] {
                assert_eq!(
                    issued_by(&endpoint, candidate),
                    issuer == candidate,
                    "{endpoint} against {candidate}"
                );
            }
        }
    }

    #[tokio::test]
    async fn expiry_is_refused_exactly_at_expiry_plus_skew() {
        let (signer, key) = fixture(1).await;
        let token = mint_at(&signer, &key, NOW_MS).await;
        let expiry_ms = NOW_MS + 60_000;

        assert!(verify_at(&token, &key, expiry_ms + SKEW_MS - 1).is_ok());
        assert_eq!(
            verify_at(&token, &key, expiry_ms + SKEW_MS).expect_err("token has expired"),
            TokenError::Expired
        );
    }

    #[tokio::test]
    async fn not_before_is_accepted_exactly_at_not_before_minus_skew() {
        let (signer, key) = fixture(1).await;
        let token = mint_at(&signer, &key, NOW_MS).await;

        assert!(verify_at(&token, &key, NOW_MS - SKEW_MS).is_ok());
        assert_eq!(
            verify_at(&token, &key, NOW_MS - SKEW_MS - 1).expect_err("token is not yet valid"),
            TokenError::NotYetValid
        );
    }

    #[tokio::test]
    async fn token_at_the_size_limit_reaches_the_signature_check() {
        let (signer, key) = fixture(1).await;
        let token = mint_at(&signer, &key, NOW_MS).await;
        let head = token.rsplit_once('.').expect("token has a signature").0;
        let padded = |total: usize| format!("{head}.{}", "A".repeat(total - head.len() - 1));
        let at_limit = padded(MAX_TOKEN_BYTES);
        let over_limit = padded(MAX_TOKEN_BYTES + 1);
        assert_eq!(at_limit.len(), MAX_TOKEN_BYTES);
        assert_eq!(over_limit.len(), MAX_TOKEN_BYTES + 1);

        assert_eq!(
            verify_at(&at_limit, &key, NOW_MS).expect_err("signature does not verify"),
            TokenError::Signature
        );
        assert_eq!(
            verify_at(&over_limit, &key, NOW_MS).expect_err("token is too long"),
            TokenError::Malformed
        );
    }
}
