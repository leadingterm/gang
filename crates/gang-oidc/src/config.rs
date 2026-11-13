//! Configuration of one `OpenID` Connect provider, as written under
//! `[identity.providers.<name>.oidc]`. The same value is sent to clients as their login
//! parameters, so it round-trips through serde unchanged.

use std::collections::BTreeMap;
use std::time::Duration;

use gang_types::{Audience, ClientSecret, NonZeroBytes, NonZeroDuration, Url, is_loopback_url};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Caching of the issuer's discovery document and signing keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcCache {
    /// How long a fetched signing-key set is trusted before it is refetched. Defaults to one hour.
    pub key_ttl: NonZeroDuration,
    /// Minimum time between refetches of the signing keys. A refetch is triggered by a token
    /// whose key id is unknown: the first such miss after a fetch always refetches, and later
    /// misses wait this long. A failed fetch is not retried for this long either, and a stale
    /// key set is served meanwhile. Defaults to 30 seconds.
    pub key_refresh_interval: NonZeroDuration,
    /// How long a fetched discovery document is trusted before it is refetched. Defaults to one hour.
    pub discovery_ttl: NonZeroDuration,
    /// How long the issuer is left alone after a failed discovery fetch; a stale document is
    /// served meanwhile if one is held. Defaults to 30 seconds.
    pub discovery_retry_interval: NonZeroDuration,
}

impl Default for OidcCache {
    fn default() -> Self {
        Self {
            key_ttl: NonZeroDuration::from_hours(1),
            key_refresh_interval: NonZeroDuration::from_secs(30),
            discovery_ttl: NonZeroDuration::from_hours(1),
            discovery_retry_interval: NonZeroDuration::from_secs(30),
        }
    }
}

/// Requests made to the issuer for discovery, signing keys and token exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcHttp {
    /// The total time allowed for one request to the issuer, including reading the body.
    /// Defaults to 10 seconds.
    pub timeout: NonZeroDuration,
    /// The largest response body accepted from the issuer, for discovery, keys and token
    /// exchange. Defaults to 1 MiB.
    pub max_response_size: NonZeroBytes,
}

impl Default for OidcHttp {
    fn default() -> Self {
        Self {
            timeout: NonZeroDuration::from_secs(10),
            max_response_size: NonZeroBytes::mib(1),
        }
    }
}

/// Checks applied to the claims of identity tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcTokens {
    /// Tolerance applied to `exp` and `nbf` to absorb clock drift between hosts. A token
    /// expires `clock_skew` after its `exp`. Zero is exact. Defaults to 30 seconds.
    #[serde(with = "gang_types::duration")]
    pub clock_skew: Duration,
}

impl Default for OidcTokens {
    fn default() -> Self {
        Self {
            clock_skew: Duration::from_secs(30),
        }
    }
}

/// An issuer identifier with a secure transport and no query or fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Url", into = "Url")]
pub struct IssuerUrl(Url);

/// An issuer URL violates the requirements for an issuer identifier.
#[derive(Debug, Error)]
pub enum IssuerUrlError {
    /// The issuer does not use HTTPS or loopback HTTP.
    #[error("issuer url must use https, or http for a loopback address")]
    Transport,
    /// The issuer includes a query or fragment.
    #[error("issuer url must not carry a query or fragment")]
    QueryOrFragment,
}

impl IssuerUrl {
    /// Returns the validated issuer URL.
    #[must_use]
    pub fn as_url(&self) -> &Url {
        &self.0
    }
}

impl TryFrom<Url> for IssuerUrl {
    type Error = IssuerUrlError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        let transport_allowed = match url.scheme() {
            "https" => true,
            "http" => is_loopback_url(&url),
            _ => false,
        };
        if !transport_allowed {
            return Err(IssuerUrlError::Transport);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(IssuerUrlError::QueryOrFragment);
        }
        Ok(Self(url))
    }
}

impl From<IssuerUrl> for Url {
    fn from(issuer: IssuerUrl) -> Self {
        issuer.0
    }
}

/// A provider configuration violates a login or principal claim rule.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The principal claim is empty.
    #[error("principal claim must not be empty")]
    EmptyPrincipalClaim,
    /// The login scopes omit the required `openid` scope.
    #[error("login scopes must contain `openid`")]
    MissingOpenidScope,
    /// The login flow owns the named authorization parameter.
    #[error("authorization parameter `{0}` is set by the login flow")]
    ReservedAuthorizationParam(String),
    /// `login.offline` repeats a scope or authorization parameter the login already sets.
    #[error("`{0}` is already set by the login and cannot repeat in `login.offline`")]
    RepeatedInOffline(String),
}

const RESERVED_AUTHORIZATION_PARAMS: &[&str] = &[
    "response_type",
    "client_id",
    "redirect_uri",
    "scope",
    "state",
    "nonce",
    "code_challenge",
    "code_challenge_method",
];

/// How Gang verifies identity tokens from one provider and, optionally, runs logins.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawOidcConfig", into = "RawOidcConfig")]
pub struct OidcConfig {
    /// The issuer identifier. Must match the `iss` claim of every token and the discovery
    /// document. `https`, or `http` only for a loopback host.
    pub issuer_url: IssuerUrl,
    /// The expected `aud` claim of identity tokens. Also the OAuth client id for logins.
    pub audience: Audience,
    /// Further client ids whose ID tokens are accepted, for example the client the dashboard
    /// logs in with. A token is accepted when its `aud` is `audience` or one of these.
    pub additional_audiences: Vec<Audience>,
    /// The claim that names the principal, for example `email` or `sub`.
    pub principal_claim: String,
    /// Claims every token must carry with exactly these JSON values.
    pub required_claims: BTreeMap<String, serde_json::Value>,
    /// Interactive login settings. Without them the provider only verifies proofs.
    pub login: Option<LoginConfig>,
    /// Caching of the issuer's discovery document and signing keys.
    pub cache: OidcCache,
    /// Requests made to the issuer.
    pub http: OidcHttp,
    /// Checks applied to the claims of identity tokens.
    pub tokens: OidcTokens,
}

/// Settings for interactive logins through the provider's authorization endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawLoginConfig", into = "RawLoginConfig")]
pub struct LoginConfig {
    /// The client secret of a native application. Native clients are not confidential, so
    /// this is not a secret in the strict sense.
    pub client_secret: Option<ClientSecret>,
    /// The scopes to request. Must contain `openid`.
    pub scopes: Vec<String>,
    /// Extra query parameters for the authorization request.
    pub authorization_params: BTreeMap<String, String>,
    /// Added to the authorization request only when the login must yield a refresh token (ADR-38).
    pub offline: OfflineLogin,
}

/// What a login that must yield a refresh token adds to its authorization request (ADR-38).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineLogin {
    /// Scopes added to the login's own, for example `offline_access`.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Query parameters added to the login's own, for example Google's
    /// `access_type = "offline"` and `prompt = "consent"`.
    #[serde(default)]
    pub authorization_params: BTreeMap<String, String>,
}

impl OfflineLogin {
    /// Whether the table adds nothing; an empty table is omitted when serialized.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty() && self.authorization_params.is_empty()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOidcConfig {
    issuer_url: IssuerUrl,
    audience: Audience,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    additional_audiences: Vec<Audience>,
    principal_claim: String,
    #[serde(default)]
    required_claims: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    login: Option<LoginConfig>,
    #[serde(default)]
    cache: OidcCache,
    #[serde(default)]
    http: OidcHttp,
    #[serde(default)]
    tokens: OidcTokens,
}

impl TryFrom<RawOidcConfig> for OidcConfig {
    type Error = ConfigError;

    fn try_from(raw: RawOidcConfig) -> Result<Self, Self::Error> {
        if raw.principal_claim.is_empty() {
            return Err(ConfigError::EmptyPrincipalClaim);
        }
        Ok(Self {
            issuer_url: raw.issuer_url,
            audience: raw.audience,
            additional_audiences: raw.additional_audiences,
            principal_claim: raw.principal_claim,
            required_claims: raw.required_claims,
            login: raw.login,
            cache: raw.cache,
            http: raw.http,
            tokens: raw.tokens,
        })
    }
}

impl From<OidcConfig> for RawOidcConfig {
    fn from(config: OidcConfig) -> Self {
        Self {
            issuer_url: config.issuer_url,
            audience: config.audience,
            additional_audiences: config.additional_audiences,
            principal_claim: config.principal_claim,
            required_claims: config.required_claims,
            login: config.login,
            cache: config.cache,
            http: config.http,
            tokens: config.tokens,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLoginConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_secret: Option<ClientSecret>,
    scopes: Vec<String>,
    #[serde(default)]
    authorization_params: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "OfflineLogin::is_empty")]
    offline: OfflineLogin,
}

impl TryFrom<RawLoginConfig> for LoginConfig {
    type Error = ConfigError;

    fn try_from(raw: RawLoginConfig) -> Result<Self, Self::Error> {
        if !raw.scopes.iter().any(|scope| scope == "openid") {
            return Err(ConfigError::MissingOpenidScope);
        }
        if let Some(name) = raw
            .authorization_params
            .keys()
            .find(|name| RESERVED_AUTHORIZATION_PARAMS.contains(&name.as_str()))
        {
            return Err(ConfigError::ReservedAuthorizationParam(name.clone()));
        }
        if let Some(name) = raw
            .offline
            .authorization_params
            .keys()
            .find(|name| RESERVED_AUTHORIZATION_PARAMS.contains(&name.as_str()))
        {
            return Err(ConfigError::ReservedAuthorizationParam(name.clone()));
        }
        if let Some(name) = raw
            .offline
            .authorization_params
            .keys()
            .find(|name| raw.authorization_params.contains_key(*name))
        {
            return Err(ConfigError::RepeatedInOffline(name.clone()));
        }
        if let Some(scope) = raw
            .offline
            .scopes
            .iter()
            .find(|scope| raw.scopes.contains(scope))
        {
            return Err(ConfigError::RepeatedInOffline(scope.clone()));
        }
        Ok(Self {
            client_secret: raw.client_secret,
            scopes: raw.scopes,
            authorization_params: raw.authorization_params,
            offline: raw.offline,
        })
    }
}

impl From<LoginConfig> for RawLoginConfig {
    fn from(config: LoginConfig) -> Self {
        Self {
            client_secret: config.client_secret,
            scopes: config.scopes,
            authorization_params: config.authorization_params,
            offline: config.offline,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::OidcConfig;

    fn valid_provider() -> Value {
        json!({
            "issuer_url": "https://identity.example.com",
            "audience": "gang",
            "additional_audiences": ["dashboard"],
            "principal_claim": "email",
            "login": {
                "client_secret": "secret",
                "scopes": ["openid", "profile"],
                "authorization_params": {"prompt": "login"}
            }
        })
    }

    #[test]
    fn an_http_issuer_that_is_not_loopback_is_rejected_when_parsed() {
        let mut config = valid_provider();
        config["issuer_url"] = json!("http://identity.example.com");

        assert!(serde_json::from_value::<OidcConfig>(config).is_err());
    }

    #[test]
    fn an_empty_audience_is_rejected_when_parsed() {
        let mut config = valid_provider();
        config["audience"] = json!("");

        assert!(serde_json::from_value::<OidcConfig>(config).is_err());
    }

    #[test]
    fn login_scopes_without_openid_are_rejected_when_parsed() {
        let mut config = valid_provider();
        config["login"]["scopes"] = json!(["profile"]);

        assert!(serde_json::from_value::<OidcConfig>(config).is_err());
    }

    #[test]
    fn a_reserved_authorization_param_is_rejected_when_parsed() {
        let mut config = valid_provider();
        config["login"]["authorization_params"] = json!({"client_id": "other"});

        assert!(serde_json::from_value::<OidcConfig>(config).is_err());
    }

    #[test]
    fn an_issuer_with_a_query_is_rejected_when_parsed() {
        let mut config = valid_provider();
        config["issuer_url"] = json!("https://identity.example.com?tenant=one");

        assert!(serde_json::from_value::<OidcConfig>(config).is_err());
    }

    #[test]
    fn issuer_transport_rules_are_enforced_when_parsed() {
        for issuer in [
            "https://idp.example.test",
            "https://idp.example.test/realms/alpha",
            "http://127.0.0.1:8080",
            "http://127.10.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:9000",
        ] {
            let mut config = valid_provider();
            config["issuer_url"] = json!(issuer);
            assert!(
                serde_json::from_value::<OidcConfig>(config).is_ok(),
                "{issuer}"
            );
        }
        for issuer in [
            "http://idp.example.test",
            "http://10.0.0.1",
            "http://[::2]:8080",
            "http://localhost.example.test",
            "ftp://idp.example.test",
            "file:///etc/idp",
        ] {
            let mut config = valid_provider();
            config["issuer_url"] = json!(issuer);
            let error = serde_json::from_value::<OidcConfig>(config)
                .expect_err("issuer transport is invalid");
            assert!(
                error
                    .to_string()
                    .contains("issuer url must use https, or http for a loopback address"),
                "{issuer}: {error}"
            );
        }
    }

    #[test]
    fn issuer_queries_and_fragments_are_rejected_when_parsed() {
        for issuer in [
            "https://idp.example.test/?tenant=alpha",
            "https://idp.example.test/#alpha",
            "http://127.0.0.1:8080/?x=1",
        ] {
            let mut config = valid_provider();
            config["issuer_url"] = json!(issuer);
            let error = serde_json::from_value::<OidcConfig>(config)
                .expect_err("issuer query or fragment is invalid");
            assert!(
                error
                    .to_string()
                    .contains("issuer url must not carry a query or fragment"),
                "{issuer}: {error}"
            );
        }
    }

    #[test]
    fn empty_scopes_are_rejected_when_parsed() {
        let mut config = valid_provider();
        config["login"]["scopes"] = json!([]);
        let error = serde_json::from_value::<OidcConfig>(config).expect_err("openid is required");
        assert!(
            error
                .to_string()
                .contains("login scopes must contain `openid`")
        );
    }

    #[test]
    fn client_secret_is_redacted_in_config_debug_output() {
        let config: OidcConfig =
            serde_json::from_value(valid_provider()).expect("valid provider parses");
        let debug = format!("{config:?}");
        assert!(!debug.contains("\"secret\""));
        assert!(debug.contains("ClientSecret(<redacted>)"));
    }

    #[test]
    fn a_valid_provider_round_trips_through_serde() {
        let config: OidcConfig =
            serde_json::from_value(valid_provider()).expect("valid provider parses");
        let serialized = serde_json::to_string(&config).expect("provider serializes");
        let reparsed: OidcConfig =
            serde_json::from_str(&serialized).expect("serialized provider parses");

        assert_eq!(config.issuer_url, reparsed.issuer_url);
        assert_eq!(config.audience, reparsed.audience);
        assert_eq!(config.additional_audiences, reparsed.additional_audiences);
        assert_eq!(config.principal_claim, reparsed.principal_claim);
        assert_eq!(config.required_claims, reparsed.required_claims);
        assert_eq!(config.cache, reparsed.cache);
        assert_eq!(config.http, reparsed.http);
        assert_eq!(config.tokens, reparsed.tokens);
        let login = config.login.as_ref().expect("login is configured");
        let reparsed_login = reparsed.login.as_ref().expect("login is configured");
        assert_eq!(login.scopes, reparsed_login.scopes);
        assert_eq!(
            login.authorization_params,
            reparsed_login.authorization_params
        );
        let secret = login.client_secret.as_ref().expect("secret is configured");
        let reparsed_secret = reparsed_login
            .client_secret
            .as_ref()
            .expect("secret is configured");
        assert!(secret.matches(reparsed_secret.expose()));
    }
}
