//! Why a bearer token or its verifying key was refused.

use gang_spi::PortFailure;

/// Why a bearer token was refused, or why a verifying key could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, PortFailure)]
pub enum TokenError {
    /// The token is not three well-formed base64url JSON segments.
    #[error("malformed token")]
    #[failure(rejected(InvalidProof))]
    Malformed,
    /// The header names an algorithm other than ES256.
    #[error("token algorithm is not es256")]
    #[failure(rejected(InvalidProof))]
    Algorithm,
    /// The key id names another key, or the signature does not verify.
    #[error("token signature does not verify")]
    #[failure(rejected(InvalidProof))]
    Signature,
    /// The token was issued by another issuer.
    #[error("token issuer does not match")]
    #[failure(rejected(InvalidProof))]
    Issuer,
    /// The token is addressed to another audience.
    #[error("token audience does not match")]
    #[failure(rejected(InvalidProof))]
    Audience,
    /// The token expired before the current time, allowing for clock skew.
    #[error("token has expired")]
    #[failure(rejected(InvalidProof))]
    Expired,
    /// The token is not valid yet, allowing for clock skew.
    #[error("token is not valid yet")]
    #[failure(rejected(InvalidProof))]
    NotYetValid,
    /// The subject does not name an identity.
    #[error("token subject is not an identity")]
    #[failure(rejected(InvalidProof))]
    Subject,
    /// The token is valid for another use than the one expected.
    #[error("token is not valid for this use")]
    #[failure(rejected(InvalidProof))]
    Usage,
    /// The verifying key could not be loaded.
    #[error("invalid token key: {0}")]
    Key(String),
}

#[cfg(test)]
mod tests {
    use gang_testkit::assert_sources_not_repeated;

    use super::TokenError;

    #[test]
    fn no_variant_repeats_its_source() {
        let errors = [
            TokenError::Malformed,
            TokenError::Algorithm,
            TokenError::Signature,
            TokenError::Issuer,
            TokenError::Audience,
            TokenError::Expired,
            TokenError::NotYetValid,
            TokenError::Subject,
            TokenError::Usage,
            TokenError::Key("invalid key".to_owned()),
        ];

        assert_sources_not_repeated(&errors);
    }
}
