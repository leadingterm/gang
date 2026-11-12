use gang_spi::{PortError, ProofVerifierPort};
use gang_types::{Principal, Proof};
use jsonwebtoken::{Algorithm, Validation, decode, decode_header};

use crate::claims::{Claims, check_nonce, check_time};
use crate::errors::OidcError;
use crate::provider::Provider;

/// Asymmetric algorithms only. Symmetric (`HS*`) and `none` tokens are never accepted, so a
/// public key cannot be used as an HMAC secret.
const ALLOWED_ALGORITHMS: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

/// The position of an allowed algorithm in `ALLOWED_ALGORITHMS`, or None for any other.
#[derive(Clone, Copy)]
pub(crate) struct AllowedAlgorithm(usize);

impl AllowedAlgorithm {
    /// Finds an algorithm in the allowlist.
    fn of(algorithm: Algorithm) -> Option<Self> {
        ALLOWED_ALGORITHMS
            .iter()
            .position(|allowed| *allowed == algorithm)
            .map(Self)
    }
}

/// Precomputed token validation rules for each permitted signing algorithm.
#[derive(Debug)]
pub(crate) struct TokenRules {
    by_algorithm: [Validation; ALLOWED_ALGORITHMS.len()],
}

impl TokenRules {
    /// Builds rules for the discovered issuer and its configured audiences.
    pub(crate) fn new(issuer: &str, audiences: &[&str]) -> Self {
        Self {
            by_algorithm: std::array::from_fn(|i| {
                validation(ALLOWED_ALGORITHMS[i], audiences, issuer)
            }),
        }
    }

    /// Returns the rules for an allowed algorithm.
    pub(crate) fn get(&self, algorithm: AllowedAlgorithm) -> &Validation {
        &self.by_algorithm[algorithm.0]
    }
}

impl Provider {
    /// Verifies an identity token and returns the principal it names.
    ///
    /// `nonce`, when given, must match the token's `nonce` claim.
    pub(crate) async fn verify_id_token(
        &self,
        token: &str,
        nonce: Option<&str>,
    ) -> Result<Principal, OidcError> {
        let header = decode_header(token).map_err(OidcError::Token)?;
        let algorithm = AllowedAlgorithm::of(header.alg).ok_or(OidcError::DisallowedAlgorithm)?;
        let discovery = self.discovery().await?;
        let validation = discovery.token_rules().get(algorithm);
        let key = self
            .keys()
            .decoding_key(
                self.http(),
                &discovery.jwks_uri,
                self.clock(),
                header.kid.as_deref(),
            )
            .await?;
        let claims = decode::<Claims>(token, key.key()?, validation)
            .map_err(OidcError::Token)?
            .claims;
        check_time(
            &claims,
            self.clock().wall(),
            self.config().tokens.clock_skew,
        )?;
        if let Some(expected) = nonce {
            check_nonce(&claims, expected)?;
        }
        self.policy().admit(&claims)
    }
}

impl ProofVerifierPort for Provider {
    async fn verify(&self, proof: &Proof) -> Result<Principal, PortError> {
        self.verify_id_token(proof.expose(), None)
            .await
            .map_err(PortError::from)
    }
}

/// The validation rules for an identity token. Expiry and not-before are checked separately
/// against the injected clock, with skew, in `check_time`.
fn validation(algorithm: Algorithm, audiences: &[&str], issuer: &str) -> Validation {
    let mut validation = Validation::new(algorithm);
    validation.set_audience(audiences);
    validation.set_issuer(&[issuer]);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims = ["iss", "aud"].into_iter().map(String::from).collect();
    validation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_rules_only_allow_configured_algorithms() {
        let rules = TokenRules::new("https://issuer.example", &["audience"]);
        for algorithm in ALLOWED_ALGORITHMS {
            let allowed = AllowedAlgorithm::of(algorithm).expect("allowed algorithm");
            assert_eq!(rules.get(allowed).algorithms, vec![algorithm]);
        }
        assert!(AllowedAlgorithm::of(Algorithm::HS256).is_none());
    }
}
