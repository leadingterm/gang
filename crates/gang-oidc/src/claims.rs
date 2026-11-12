use std::collections::BTreeMap;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::Principal;
use serde_json::Value;

use crate::errors::{ClaimError, OidcError};

pub(crate) type Claims = serde_json::Map<String, Value>;

/// Rejects a token whose validity window does not contain `now`.
///
/// `exp` is required and the token expires once `now` reaches `exp` plus the skew.
/// `nbf` is optional; a present `nbf` that is not a numeric date fails closed.
pub(crate) fn check_time(claims: &Claims, now: Timestamp, skew: Duration) -> Result<(), OidcError> {
    let expires_at = numeric_date(claims, "exp").ok_or(OidcError::MissingClaim("exp"))?;
    if now >= expires_at + skew {
        return Err(OidcError::Expired);
    }
    if claims.contains_key("nbf") {
        let starts_at = numeric_date(claims, "nbf").ok_or(OidcError::NotYetValid)?;
        if now + skew < starts_at {
            return Err(OidcError::NotYetValid);
        }
    }
    Ok(())
}

/// Requires the `nonce` claim to equal the nonce the login started with.
pub(crate) fn check_nonce(claims: &Claims, expected: &str) -> Result<(), OidcError> {
    match claims.get("nonce").and_then(Value::as_str) {
        Some(nonce) if nonce == expected => Ok(()),
        _ => Err(OidcError::NonceMismatch),
    }
}

/// Admission rules applied to every verified claim set.
pub(crate) struct Policy<'a> {
    pub(crate) principal_claim: &'a str,
    pub(crate) required_claims: &'a BTreeMap<String, Value>,
}

impl Policy<'_> {
    /// Checks the required claims, then extracts the principal.
    pub(crate) fn admit(&self, claims: &Claims) -> Result<Principal, OidcError> {
        for (name, expected) in self.required_claims {
            if claims.get(name) != Some(expected) {
                return Err(ClaimError::Required(name.clone()).into());
            }
        }
        claims
            .get(self.principal_claim)
            .and_then(Value::as_str)
            .and_then(|value| Principal::try_new(value).ok())
            .ok_or_else(|| ClaimError::Principal(self.principal_claim.to_owned()).into())
    }
}

fn numeric_date(claims: &Claims, name: &str) -> Option<Timestamp> {
    let seconds = claims.get(name)?.as_u64()?;
    Some(Timestamp::from_unix_millis(seconds.saturating_mul(1_000)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EXP: u64 = 1_000;
    const NBF: u64 = 2_000;
    const SKEW: Duration = Duration::from_secs(30);

    fn claims(value: Value) -> Claims {
        let Value::Object(map) = value else {
            panic!("test claims are a JSON object")
        };
        map
    }

    fn at_millis(millis: u64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    #[test]
    fn token_is_valid_just_before_expiry_plus_skew() {
        let token = claims(json!({ "exp": EXP }));
        let last_valid = at_millis((EXP + 30) * 1_000 - 1);
        assert!(check_time(&token, last_valid, SKEW).is_ok());
    }

    #[test]
    fn token_expires_exactly_at_expiry_plus_skew() {
        let token = claims(json!({ "exp": EXP }));
        let boundary = at_millis((EXP + 30) * 1_000);
        assert!(matches!(
            check_time(&token, boundary, SKEW),
            Err(OidcError::Expired)
        ));
    }

    #[test]
    fn token_without_exp_is_rejected() {
        let token = claims(json!({ "sub": "dev" }));
        assert!(matches!(
            check_time(&token, at_millis(0), SKEW),
            Err(OidcError::MissingClaim("exp"))
        ));
    }

    #[test]
    fn non_numeric_exp_is_rejected_as_missing() {
        let token = claims(json!({ "exp": "1000" }));
        assert!(matches!(
            check_time(&token, at_millis(0), SKEW),
            Err(OidcError::MissingClaim("exp"))
        ));
    }

    #[test]
    fn token_without_nbf_is_valid_at_any_time_before_expiry() {
        let token = claims(json!({ "exp": EXP }));
        assert!(check_time(&token, at_millis(0), SKEW).is_ok());
    }

    #[test]
    fn token_is_not_valid_before_nbf_minus_skew() {
        let token = claims(json!({ "exp": 9_999, "nbf": NBF }));
        let too_early = at_millis((NBF - 30) * 1_000 - 1);
        assert!(matches!(
            check_time(&token, too_early, SKEW),
            Err(OidcError::NotYetValid)
        ));
    }

    #[test]
    fn token_is_valid_at_nbf_minus_skew() {
        let token = claims(json!({ "exp": 9_999, "nbf": NBF }));
        let earliest = at_millis((NBF - 30) * 1_000);
        assert!(check_time(&token, earliest, SKEW).is_ok());
    }

    #[test]
    fn non_numeric_nbf_fails_closed() {
        let token = claims(json!({ "exp": 9_999, "nbf": "0" }));
        assert!(matches!(
            check_time(&token, at_millis(0), SKEW),
            Err(OidcError::NotYetValid)
        ));
    }

    #[test]
    fn matching_nonce_is_accepted() {
        let token = claims(json!({ "nonce": "n-1" }));
        assert!(check_nonce(&token, "n-1").is_ok());
    }

    #[test]
    fn different_nonce_is_rejected() {
        let token = claims(json!({ "nonce": "n-2" }));
        assert!(matches!(
            check_nonce(&token, "n-1"),
            Err(OidcError::NonceMismatch)
        ));
    }

    #[test]
    fn absent_nonce_is_rejected() {
        let token = claims(json!({}));
        assert!(matches!(
            check_nonce(&token, "n-1"),
            Err(OidcError::NonceMismatch)
        ));
    }

    #[test]
    fn non_string_nonce_is_rejected() {
        let token = claims(json!({ "nonce": 1 }));
        assert!(matches!(
            check_nonce(&token, "1"),
            Err(OidcError::NonceMismatch)
        ));
    }

    fn policy(required: &BTreeMap<String, Value>) -> Policy<'_> {
        Policy {
            principal_claim: "email",
            required_claims: required,
        }
    }

    fn required(entries: Value) -> BTreeMap<String, Value> {
        let Value::Object(map) = entries else {
            panic!("test requirements are a JSON object")
        };
        map.into_iter().collect()
    }

    #[test]
    fn admit_returns_principal_when_requirements_hold() {
        let requirements = required(json!({ "email_verified": true }));
        let token = claims(json!({ "email": "dev@example.test", "email_verified": true }));
        let principal = policy(&requirements)
            .admit(&token)
            .expect("token is admitted");
        assert_eq!(principal.as_str(), "dev@example.test");
    }

    #[test]
    fn boolean_requirement_rejects_string_true() {
        let requirements = required(json!({ "email_verified": true }));
        let token = claims(json!({ "email": "dev@example.test", "email_verified": "true" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(name))) if name == "email_verified"
        ));
    }

    #[test]
    fn boolean_requirement_rejects_false() {
        let requirements = required(json!({ "email_verified": true }));
        let token = claims(json!({ "email": "dev@example.test", "email_verified": false }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(_)))
        ));
    }

    #[test]
    fn numeric_requirement_rejects_string_of_same_value() {
        let requirements = required(json!({ "tenant_level": 2 }));
        let token = claims(json!({ "email": "dev@example.test", "tenant_level": "2" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(_)))
        ));
    }

    #[test]
    fn string_requirement_is_compared_exactly() {
        let requirements = required(json!({ "team": "alpha" }));
        let token = claims(json!({ "email": "dev@example.test", "team": "Alpha" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(_)))
        ));
    }

    #[test]
    fn missing_required_claim_is_rejected() {
        let requirements = required(json!({ "team": "alpha" }));
        let token = claims(json!({ "email": "dev@example.test" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(name))) if name == "team"
        ));
    }

    #[test]
    fn missing_principal_claim_is_rejected() {
        let requirements = BTreeMap::new();
        let token = claims(json!({ "sub": "1" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Principal(name))) if name == "email"
        ));
    }

    #[test]
    fn empty_principal_claim_is_rejected() {
        let requirements = BTreeMap::new();
        let token = claims(json!({ "email": "" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Principal(_)))
        ));
    }

    #[test]
    fn non_string_principal_claim_is_rejected() {
        let requirements = BTreeMap::new();
        let token = claims(json!({ "email": 42 }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Principal(_)))
        ));
    }

    #[test]
    fn required_claims_are_checked_before_principal() {
        let requirements = required(json!({ "team": "alpha" }));
        let token = claims(json!({ "team": "beta" }));
        assert!(matches!(
            policy(&requirements).admit(&token),
            Err(OidcError::Policy(ClaimError::Required(_)))
        ));
    }
}
