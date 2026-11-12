use gang_types::{Url, is_loopback_url};
use serde::Deserialize;

use crate::errors::OidcError;
use crate::http::Http;
use crate::verify::TokenRules;

/// The deserialized subset of the issuer's `OpenID` Connect discovery document.
#[derive(Deserialize)]
struct Document {
    issuer: String,
    jwks_uri: Url,
    authorization_endpoint: Option<Url>,
    token_endpoint: Option<Url>,
}

/// The checked discovery document and its precomputed token validation rules.
#[derive(Debug)]
pub(crate) struct Discovery {
    pub(crate) jwks_uri: Url,
    authorization_endpoint: Option<Url>,
    token_endpoint: Option<Url>,
    token_rules: TokenRules,
}

impl Discovery {
    /// Fetches `{issuer}/.well-known/openid-configuration` and checks its issuer and endpoints.
    pub(crate) async fn fetch(
        http: &Http,
        issuer_url: &Url,
        audiences: &[&str],
    ) -> Result<Self, OidcError> {
        let issuer = issuer_url.as_str().trim_end_matches('/');
        let document_url = Url::parse(&format!("{issuer}/.well-known/openid-configuration"))
            .map_err(|_| OidcError::InvalidDiscoveryUrl)?;
        let document: Document = http.get_json(&document_url).await?;
        check_issuer(issuer_url.as_str(), &document.issuer)?;
        check_endpoint(issuer_url, "jwks_uri", &document.jwks_uri)?;
        if let Some(endpoint) = &document.authorization_endpoint {
            check_endpoint(issuer_url, "authorization_endpoint", endpoint)?;
        }
        if let Some(endpoint) = &document.token_endpoint {
            check_endpoint(issuer_url, "token_endpoint", endpoint)?;
        }
        let token_rules = TokenRules::new(&document.issuer, audiences);
        Ok(Self {
            jwks_uri: document.jwks_uri,
            authorization_endpoint: document.authorization_endpoint,
            token_endpoint: document.token_endpoint,
            token_rules,
        })
    }

    /// Returns the validation rules for the discovered issuer.
    pub(crate) fn token_rules(&self) -> &TokenRules {
        &self.token_rules
    }

    /// Returns the authorization endpoint, required for logins.
    pub(crate) fn authorization_endpoint(&self) -> Result<&Url, OidcError> {
        self.authorization_endpoint
            .as_ref()
            .ok_or(OidcError::MissingEndpoint("authorization_endpoint"))
    }

    /// Returns the token endpoint, required for logins.
    pub(crate) fn token_endpoint(&self) -> Result<&Url, OidcError> {
        self.token_endpoint
            .as_ref()
            .ok_or(OidcError::MissingEndpoint("token_endpoint"))
    }
}

/// Compares issuers exactly, ignoring only a trailing slash on either side.
fn check_issuer(expected: &str, found: &str) -> Result<(), OidcError> {
    if expected.trim_end_matches('/') == found.trim_end_matches('/') {
        Ok(())
    } else {
        Err(OidcError::DiscoveryIssuerMismatch {
            expected: expected.to_owned(),
            found: found.to_owned(),
        })
    }
}

/// Checks that an endpoint uses HTTPS, or loopback HTTP when the issuer uses loopback HTTP,
/// and has no fragment.
fn check_endpoint(issuer_url: &Url, name: &'static str, endpoint: &Url) -> Result<(), OidcError> {
    let transport_allowed = match endpoint.scheme() {
        "https" => true,
        "http" => issuer_url.scheme() == "http" && is_loopback_url(endpoint),
        _ => false,
    };
    if transport_allowed && endpoint.fragment().is_none() {
        Ok(())
    } else {
        Err(OidcError::InvalidEndpoint(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_issuers_match() {
        assert!(check_issuer("https://idp.example.test", "https://idp.example.test").is_ok());
    }

    #[test]
    fn trailing_slash_is_ignored_on_either_side() {
        assert!(check_issuer("https://idp.example.test/", "https://idp.example.test").is_ok());
        assert!(check_issuer("https://idp.example.test", "https://idp.example.test/").is_ok());
    }

    #[test]
    fn different_issuer_is_rejected() {
        assert!(matches!(
            check_issuer("https://idp.example.test", "https://other.example.test"),
            Err(OidcError::DiscoveryIssuerMismatch { .. })
        ));
    }

    #[test]
    fn issuer_path_must_match_exactly() {
        assert!(matches!(
            check_issuer(
                "https://idp.example.test/tenant",
                "https://idp.example.test"
            ),
            Err(OidcError::DiscoveryIssuerMismatch { .. })
        ));
    }

    #[test]
    fn endpoint_transport_and_fragment_are_checked() {
        let https_issuer = Url::parse("https://idp.example.test").expect("valid issuer");
        let http_issuer = Url::parse("http://127.0.0.1:8080").expect("valid issuer");
        for (issuer, endpoint, allowed) in [
            (&https_issuer, "https://idp.example.test/jwks", true),
            (&http_issuer, "http://localhost:8080/jwks", true),
            (&http_issuer, "http://idp.example.test/jwks", false),
            (&https_issuer, "http://127.0.0.1/jwks", false),
            (&http_issuer, "javascript:alert(1)", false),
            (&http_issuer, "file:///tmp/jwks", false),
            (&https_issuer, "https://idp.example.test/jwks#key", false),
            (&https_issuer, "https://idp.example.test/jwks?key=1", true),
        ] {
            let endpoint = Url::parse(endpoint).expect("valid endpoint url");
            let result = check_endpoint(issuer, "jwks_uri", &endpoint);
            if allowed {
                assert!(result.is_ok(), "{endpoint}");
            } else {
                assert!(
                    matches!(result, Err(OidcError::InvalidEndpoint("jwks_uri"))),
                    "{endpoint}"
                );
            }
        }
    }
}
