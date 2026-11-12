//! The bearer token the client keeps between requests, and the reading of the identity
//! provider's proofs that the client needs.

use base64::Engine as _;
use base64::alphabet::URL_SAFE;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use gang_clock::Timestamp;
use gang_types::AccessToken;
use serde::{Deserialize, Serialize};

/// Decodes JWT segments: base64url with or without padding.
const BASE64URL: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// A minted bearer token, as stored in the `token` file. The token is a secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredToken {
    /// The bearer token.
    pub token: AccessToken,
    /// When the token was minted.
    pub issued_at: Timestamp,
    /// When the token stops being accepted.
    pub expires_at: Timestamp,
}

/// The expiry recorded in the `exp` claim of a JWT, read without verifying its signature.
///
/// Returns `None` when `proof` is not a JWT (three dot-separated segments whose middle one is
/// base64url-encoded JSON), or when its claims carry no integer `exp`.
pub(crate) fn jwt_expiry(proof: &str) -> Option<Timestamp> {
    let parts: Vec<&str> = proof.split('.').collect();
    let [_, payload, _] = parts.as_slice() else {
        return None;
    };
    let claims: serde_json::Value =
        serde_json::from_slice(&BASE64URL.decode(payload).ok()?).ok()?;
    let seconds = claims.get("exp")?.as_u64()?;
    Some(Timestamp::from_unix_millis(seconds.checked_mul(1_000)?))
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_types::AccessToken;

    use super::{StoredToken, jwt_expiry};

    /// `{"exp":1893456000}`, base64url-encoded without padding (18 bytes, 24 characters).
    const EXP_PAYLOAD: &str = "eyJleHAiOjE4OTM0NTYwMDB9";
    /// `{"alg":"HS256"}`, the usual JWT header.
    const HEADER: &str = "eyJhbGciOiJIUzI1NiJ9";
    /// The expiry of [`EXP_PAYLOAD`]: 2030-01-01T00:00:00Z.
    const EXP_MILLIS: u64 = 1_893_456_000_000;

    #[test]
    fn jwt_expiry_reads_exp_claim() {
        let proof = format!("{HEADER}.{EXP_PAYLOAD}.c2ln");

        assert_eq!(
            jwt_expiry(&proof),
            Some(Timestamp::from_unix_millis(EXP_MILLIS))
        );
    }

    #[test]
    fn jwt_expiry_accepts_unpadded_tail_of_one_byte() {
        // `{"exp":1893456000} ` has 19 bytes: the last group carries a single byte.
        let proof = format!("{HEADER}.eyJleHAiOjE4OTM0NTYwMDB9IA.c2ln");

        assert_eq!(
            jwt_expiry(&proof),
            Some(Timestamp::from_unix_millis(EXP_MILLIS))
        );
    }

    #[test]
    fn jwt_expiry_accepts_unpadded_tail_of_two_bytes() {
        // `{"exp":1893456000}  ` has 20 bytes: the last group carries two bytes.
        let proof = format!("{HEADER}.eyJleHAiOjE4OTM0NTYwMDB9ICA.c2ln");

        assert_eq!(
            jwt_expiry(&proof),
            Some(Timestamp::from_unix_millis(EXP_MILLIS))
        );
    }

    #[test]
    fn jwt_expiry_accepts_padded_payload() {
        let proof = format!("{HEADER}.eyJleHAiOjE4OTM0NTYwMDB9IA==.c2ln");

        assert_eq!(
            jwt_expiry(&proof),
            Some(Timestamp::from_unix_millis(EXP_MILLIS))
        );
    }

    #[test]
    fn jwt_without_exp_has_no_expiry() {
        // `{}`
        let proof = format!("{HEADER}.e30.c2ln");

        assert_eq!(jwt_expiry(&proof), None);
    }

    #[test]
    fn opaque_token_has_no_expiry() {
        assert_eq!(jwt_expiry("opaque-token"), None);
    }

    #[test]
    fn two_segments_have_no_expiry() {
        assert_eq!(jwt_expiry(&format!("{HEADER}.{EXP_PAYLOAD}")), None);
    }

    #[test]
    fn four_segments_have_no_expiry() {
        assert_eq!(jwt_expiry(&format!("{HEADER}.{EXP_PAYLOAD}.c2ln.x")), None);
    }

    #[test]
    fn empty_payload_has_no_expiry() {
        assert_eq!(jwt_expiry("a..c"), None);
    }

    #[test]
    fn standard_base64_alphabet_is_rejected() {
        // `+` and `/` are not base64url characters.
        assert_eq!(jwt_expiry("a.e+30.c"), None);
    }

    #[test]
    fn impossible_length_is_rejected() {
        // One dangling base64 character cannot encode a whole byte.
        assert_eq!(jwt_expiry("a.e.c"), None);
    }

    #[test]
    fn stored_token_debug_omits_the_secret() {
        let stored = StoredToken {
            token: AccessToken::new("bearer-secret"),
            issued_at: Timestamp::UNIX_EPOCH,
            expires_at: Timestamp::UNIX_EPOCH,
        };

        let text = format!("{stored:?}");

        assert!(!text.contains("bearer-secret"));
        assert!(text.contains("expires_at"));
    }
}
