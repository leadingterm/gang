//! Errors of the `OpenID` Connect crate and their mapping onto port errors.

use gang_spi::{PortError, Rejection};
use gang_types::NonZeroBytes;

/// Everything that can go wrong inside the crate. Converted to a [`PortError`] at the port
/// boundary, where the single [`From`] implementation decides the outcome.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OidcError {
    // Unavailable.
    #[error("issuer unreachable")]
    Transport(#[source] gang_tls::HttpError),
    #[error("issuer answered with status {status}")]
    ServerError { status: u16 },
    #[error("issuer's discovery document is unavailable; retrying later")]
    DiscoveryUnavailable,
    /// The issuer's signing keys cannot be fetched and none are cached.
    #[error("issuer's signing keys are unavailable")]
    KeysUnavailable,

    // Backend.
    #[error("issuer answered with unexpected status {status}")]
    UnexpectedStatus { status: u16 },
    #[error("issuer sent an unreadable body")]
    Body(#[source] serde_json::Error),
    #[error("issuer body exceeds the {limit} limit")]
    BodyTooLarge { limit: NonZeroBytes },
    #[error("discovery issuer `{found}` does not match `{expected}`")]
    DiscoveryIssuerMismatch { expected: String, found: String },
    #[error("provider does not advertise `{0}`")]
    MissingEndpoint(&'static str),
    #[error("provider advertises an unusable `{0}`")]
    InvalidEndpoint(&'static str),
    #[error("provider key is unusable")]
    Jwks(#[source] jsonwebtoken::errors::Error),
    #[error("provider discovery url is invalid")]
    InvalidDiscoveryUrl,
    #[error("operating system provided no randomness")]
    Entropy,
    #[error("token endpoint refused the request: {error}")]
    OAuth { error: String },
    #[error("login is not configured")]
    LoginNotConfigured,

    // Rejection::InvalidProof.
    #[error("token verification failed")]
    Token(#[source] jsonwebtoken::errors::Error),
    #[error("token algorithm is not allowed")]
    DisallowedAlgorithm,
    #[error("no matching signing key")]
    UnknownKey,
    #[error("token has no key id and the provider has several keys")]
    AmbiguousKey,
    #[error("token is expired")]
    Expired,
    #[error("token is not valid yet")]
    NotYetValid,
    #[error("token lacks the `{0}` claim")]
    MissingClaim(&'static str),
    #[error("token nonce does not match the login")]
    NonceMismatch,
    #[error("login state does not match")]
    StateMismatch,
    #[error("callback does not match the redirect uri")]
    RedirectMismatch,
    #[error("callback carries no authorization code")]
    MissingCode,
    #[error("login session is malformed")]
    MalformedSession(#[source] serde_json::Error),
    #[error("provider reported `{0}` at the callback")]
    CallbackError(String),

    // Rejection::PolicyDenied.
    #[error(transparent)]
    Policy(#[from] ClaimError),
    #[error("user or provider denied access")]
    AccessDenied,
}

/// A claim check that the provider's policy failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ClaimError {
    #[error("principal claim `{0}` is missing or not a non-empty string")]
    Principal(String),
    #[error("required claim `{0}` is missing or differs")]
    Required(String),
}

impl From<OidcError> for PortError {
    fn from(error: OidcError) -> Self {
        match error {
            OidcError::Transport(cause) => Self::unavailable(cause),
            OidcError::ServerError { .. }
            | OidcError::DiscoveryUnavailable
            | OidcError::KeysUnavailable => Self::unavailable(error),

            OidcError::UnexpectedStatus { .. }
            | OidcError::Body(_)
            | OidcError::BodyTooLarge { .. }
            | OidcError::DiscoveryIssuerMismatch { .. }
            | OidcError::MissingEndpoint(_)
            | OidcError::InvalidEndpoint(_)
            | OidcError::Jwks(_)
            | OidcError::InvalidDiscoveryUrl
            | OidcError::Entropy => Self::backend(error),

            OidcError::OAuth { error: code } => match code.as_str() {
                "invalid_grant" => Self::refused(Rejection::InvalidProof),
                "access_denied" => Self::refused(Rejection::PolicyDenied),
                _ => Self::backend(OidcError::OAuth { error: code }),
            },
            OidcError::LoginNotConfigured => Self::Unsupported { operation: "login" },

            OidcError::Token(_)
            | OidcError::DisallowedAlgorithm
            | OidcError::UnknownKey
            | OidcError::AmbiguousKey
            | OidcError::Expired
            | OidcError::NotYetValid
            | OidcError::MissingClaim(_)
            | OidcError::NonceMismatch
            | OidcError::StateMismatch
            | OidcError::RedirectMismatch
            | OidcError::MissingCode
            | OidcError::MalformedSession(_)
            | OidcError::CallbackError(_) => Self::refused(Rejection::InvalidProof),

            OidcError::Policy(_) | OidcError::AccessDenied => {
                Self::refused(Rejection::PolicyDenied)
            }
        }
    }
}

impl From<gang_tls::HttpError> for OidcError {
    fn from(error: gang_tls::HttpError) -> Self {
        match error {
            gang_tls::HttpError::BodyTooLarge { limit, .. } => Self::BodyTooLarge { limit },
            other => Self::Transport(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::{PortError, Rejection};
    use gang_tls::HttpError;
    use gang_types::NonZeroBytes;

    use super::{ClaimError, OidcError};

    #[test]
    fn no_variant_repeats_its_source() {
        let json_error = || {
            serde_json::from_str::<serde_json::Value>("{").expect_err("incomplete JSON is invalid")
        };
        let jwt_error =
            || jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken);
        let errors = [
            OidcError::Transport(HttpError::TimedOut),
            OidcError::ServerError { status: 503 },
            OidcError::DiscoveryUnavailable,
            OidcError::KeysUnavailable,
            OidcError::UnexpectedStatus { status: 400 },
            OidcError::Body(json_error()),
            OidcError::BodyTooLarge {
                limit: NonZeroBytes::kib(1),
            },
            OidcError::DiscoveryIssuerMismatch {
                expected: "expected".to_owned(),
                found: "found".to_owned(),
            },
            OidcError::MissingEndpoint("token_endpoint"),
            OidcError::InvalidEndpoint("jwks_uri"),
            OidcError::Jwks(jwt_error()),
            OidcError::InvalidDiscoveryUrl,
            OidcError::Entropy,
            OidcError::OAuth {
                error: "invalid_request".to_owned(),
            },
            OidcError::LoginNotConfigured,
            OidcError::Token(jwt_error()),
            OidcError::DisallowedAlgorithm,
            OidcError::UnknownKey,
            OidcError::AmbiguousKey,
            OidcError::Expired,
            OidcError::NotYetValid,
            OidcError::MissingClaim("sub"),
            OidcError::NonceMismatch,
            OidcError::StateMismatch,
            OidcError::RedirectMismatch,
            OidcError::MissingCode,
            OidcError::MalformedSession(json_error()),
            OidcError::CallbackError("invalid_request".to_owned()),
            OidcError::Policy(ClaimError::Required("hd".to_owned())),
            OidcError::AccessDenied,
        ];

        gang_testkit::assert_sources_not_repeated(&errors);
    }

    #[test]
    fn transport_is_unavailable() {
        let port: PortError = OidcError::Transport(HttpError::TimedOut).into();
        assert!(matches!(port, PortError::Unavailable(_)));
    }

    #[test]
    fn oversized_body_is_the_body_limit() {
        assert!(matches!(
            OidcError::from(HttpError::BodyTooLarge {
                status: http::StatusCode::OK,
                limit: NonZeroBytes::kib(1),
            }),
            OidcError::BodyTooLarge { limit } if limit == NonZeroBytes::kib(1)
        ));
    }

    #[test]
    fn server_error_is_unavailable() {
        let port: PortError = OidcError::ServerError { status: 503 }.into();
        assert!(matches!(port, PortError::Unavailable(_)));
    }

    #[test]
    fn token_failure_rejects_the_proof() {
        let cause =
            jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidToken);
        let port: PortError = OidcError::Token(cause).into();
        assert!(matches!(
            port,
            PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            }
        ));
    }

    #[test]
    fn missing_id_token_rejects_the_proof() {
        let port: PortError = OidcError::MissingClaim("id_token").into();
        assert!(matches!(
            port,
            PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            }
        ));
    }

    #[test]
    fn policy_failure_denies_the_policy() {
        let port: PortError = OidcError::Policy(ClaimError::Required("hd".to_owned())).into();
        assert!(matches!(
            port,
            PortError::Rejected {
                reason: Rejection::PolicyDenied,
                ..
            }
        ));
    }

    #[test]
    fn oauth_invalid_grant_rejects_the_proof() {
        let port: PortError = OidcError::OAuth {
            error: "invalid_grant".to_owned(),
        }
        .into();
        assert!(matches!(
            port,
            PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            }
        ));
    }

    #[test]
    fn oauth_access_denied_denies_the_policy() {
        let port: PortError = OidcError::OAuth {
            error: "access_denied".to_owned(),
        }
        .into();
        assert!(matches!(
            port,
            PortError::Rejected {
                reason: Rejection::PolicyDenied,
                ..
            }
        ));
    }

    #[test]
    fn other_oauth_error_is_backend() {
        let port: PortError = OidcError::OAuth {
            error: "unsupported_grant_type".to_owned(),
        }
        .into();
        assert!(matches!(port, PortError::Backend(_)));
    }

    #[test]
    fn login_not_configured_is_unsupported() {
        let port: PortError = OidcError::LoginNotConfigured.into();
        assert!(matches!(
            port,
            PortError::Unsupported { operation: "login" }
        ));
    }

    #[test]
    fn missing_endpoint_is_backend() {
        let port: PortError = OidcError::MissingEndpoint("token_endpoint").into();
        assert!(matches!(port, PortError::Backend(_)));
    }

    #[test]
    fn invalid_endpoint_is_backend() {
        let port: PortError = OidcError::InvalidEndpoint("jwks_uri").into();
        assert!(matches!(port, PortError::Backend(_)));
    }
}
