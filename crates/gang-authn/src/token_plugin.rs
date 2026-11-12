//! The `token` plugin: gang-signed bearer tokens minted against an identity provider's proof.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_spi::{
    Authenticated, AuthenticationHost, AuthenticationPort, Capabilities, CertificateSignerPort,
    ClientCertificates, CredentialLifecyclePort, Evidence, IssuedToken, Plugin, PluginContext,
    PortError, Scheme, Subject, TokenIssue,
};
use gang_types::{AuthenticationSettings, Identity, ProviderName, TokenTtl, parse_bearer};
use http::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::{Memo, TokenError, TokenKey, TokenUse, issuer_of, mint, verify};

/// The `[token]` section: settings of the bearer token plugin.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "RawTokenConfig", into = "RawTokenConfig")]
pub struct TokenConfig {
    /// How long a minted token is valid, from 1 minute to 1 hour in whole seconds. Defaults to 10 minutes.
    pub token_ttl: TokenTtl,
    /// The clock skew tolerated when checking a token's validity window; zero is exact. Defaults to 30 seconds.
    pub clock_skew: Duration,
    /// The number of verified tokens kept in memory. Defaults to 4096.
    pub max_cached_tokens: NonZeroUsize,
    /// The identity providers allowed to mint a token, sorted and deduplicated; never empty.
    providers: Vec<ProviderName>,
}

impl TokenConfig {
    /// Replaces the identity providers allowed to mint tokens, sorted and deduplicated.
    ///
    /// # Errors
    /// Returns [`NoTokenProviders`] when providers is empty.
    pub fn set_providers(
        &mut self,
        providers: impl IntoIterator<Item = ProviderName>,
    ) -> Result<(), NoTokenProviders> {
        self.providers = normalize_providers(providers.into_iter().collect())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
struct RawTokenConfig {
    token_ttl: TokenTtl,
    #[serde(with = "gang_types::duration")]
    clock_skew: Duration,
    max_cached_tokens: NonZeroUsize,
    providers: Vec<ProviderName>,
}

impl Default for RawTokenConfig {
    fn default() -> Self {
        Self {
            token_ttl: TokenTtl::from_units(600),
            clock_skew: Duration::from_secs(30),
            max_cached_tokens: const { NonZeroUsize::new(4096).expect("4096 is non-zero") },
            providers: Vec::new(),
        }
    }
}

/// The identity provider list cannot be empty.
#[derive(Debug, Error)]
#[error("providers must name at least one identity provider")]
pub struct NoTokenProviders;

fn normalize_providers(
    mut providers: Vec<ProviderName>,
) -> Result<Vec<ProviderName>, NoTokenProviders> {
    if providers.is_empty() {
        return Err(NoTokenProviders);
    }
    providers.sort_unstable();
    providers.dedup();
    Ok(providers)
}

impl TryFrom<RawTokenConfig> for TokenConfig {
    type Error = NoTokenProviders;

    fn try_from(raw: RawTokenConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            token_ttl: raw.token_ttl,
            clock_skew: raw.clock_skew,
            max_cached_tokens: raw.max_cached_tokens,
            providers: normalize_providers(raw.providers)?,
        })
    }
}

impl From<TokenConfig> for RawTokenConfig {
    fn from(config: TokenConfig) -> Self {
        Self {
            token_ttl: config.token_ttl,
            clock_skew: config.clock_skew,
            max_cached_tokens: config.max_cached_tokens,
            providers: config.providers,
        }
    }
}

impl AuthenticationSettings for TokenConfig {
    fn requires_tls(&self) -> bool {
        false
    }

    fn providers(&self) -> &[ProviderName] {
        &self.providers
    }
}

/// A verified token as the memo keeps it: the use it was verified for, and its identity.
#[derive(Clone)]
struct Cached {
    usage: TokenUse,
    authenticated: Authenticated,
}

/// The bearer token plugin. It verifies Gang-signed tokens on requests and mints them against a
/// proof from one of its providers.
pub struct Token {
    config: TokenConfig,
    key: OnceCell<TokenKey>,
    memo: Memo<[u8; 32], Cached>,
}

impl Token {
    fn new(config: TokenConfig) -> Self {
        let memo = Memo::new(config.max_cached_tokens);
        Self {
            config,
            key: OnceCell::new(),
            memo,
        }
    }

    /// The authority's public key, read from the signer on first use.
    async fn key<H: AuthenticationHost>(&self, host: &H) -> Result<&TokenKey, PortError> {
        self.key.get_or_try_init(|| fetch_key(host)).await
    }

    /// Mints a token of `usage` for the identity that `request.proof` proves, if the provider is
    /// one of ours.
    async fn issue<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &TokenIssue<'_>,
        usage: TokenUse,
    ) -> Result<IssuedToken, PortError> {
        if self
            .config
            .providers
            .binary_search(request.provider)
            .is_err()
        {
            return Err(PortError::NotFound(Subject::Provider(
                request.provider.clone(),
            )));
        }
        let principal = host.verify(request.provider, request.proof).await?;
        let identity = Identity::new(request.provider.clone(), principal);
        let key = self.key(host).await?;
        let issuer = issuer_of(host.endpoint());
        mint(
            host.signer(),
            key,
            &issuer,
            &identity,
            host.now(),
            self.config.token_ttl.get(),
            usage,
        )
        .await
    }
}

async fn fetch_key<H: AuthenticationHost>(host: &H) -> Result<TokenKey, PortError> {
    let der = host.signer().public_key().await?;
    Ok(TokenKey::from_spki(&der)?)
}

impl Plugin for Token {
    type Config = TokenConfig;

    fn build(config: TokenConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self::new(config))
    }
}

/// The memo key of a bearer token: its SHA-256, so live credentials are never held in the memo.
fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

impl AuthenticationPort for Token {
    fn scheme(&self) -> Scheme {
        Scheme::Token
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            certificates: false,
            tokens: true,
            sessions: true,
        }
    }

    fn client_certificates(&self) -> ClientCertificates {
        ClientCertificates::Never
    }

    async fn authenticate<H: AuthenticationHost>(
        &self,
        host: &H,
        evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        let bearer = evidence
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_bearer);
        let (token, expected) = if let Some(token) = bearer {
            (token, TokenUse::Client)
        } else if let Some(token) = evidence.session {
            (token, TokenUse::Session)
        } else {
            return Ok(None);
        };
        let now = host.now();
        let token_digest = digest(token);
        let cached = self.memo.get(&token_digest, now);
        if let Some(cached) = cached {
            if cached.usage != expected {
                return Err(PortError::from(TokenError::Usage));
            }
            return Ok(Some(cached.authenticated));
        }
        let key = self.key(host).await?;
        let verified = verify(
            token,
            key,
            host.endpoint(),
            now,
            self.config.clock_skew,
            expected,
        )?;
        // The token is accepted until its expiry plus the tolerated clock skew.
        let authenticated = Authenticated {
            identity: Arc::new(verified.identity),
            valid_until: Some(verified.valid_until),
        };
        self.memo.insert(
            token_digest,
            Cached {
                usage: expected,
                authenticated: authenticated.clone(),
            },
            verified.valid_until,
            now,
        );
        Ok(Some(authenticated))
    }
}

impl CredentialLifecyclePort for Token {
    async fn issue_token<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &TokenIssue<'_>,
    ) -> Result<IssuedToken, PortError> {
        self.issue(host, request, TokenUse::Client).await
    }

    async fn issue_session<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &TokenIssue<'_>,
    ) -> Result<IssuedToken, PortError> {
        self.issue(host, request, TokenUse::Session).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn parse(json: &str) -> Result<TokenConfig, serde_json::Error> {
        serde_json::from_str(json)
    }

    fn build(json: &str) -> Result<Token, PortError> {
        let config = parse(json).expect("the configuration parses");
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        Token::build(config, &context)
    }

    #[test]
    fn a_token_section_without_providers_is_rejected_when_parsed() {
        assert!(parse("{}").is_err());
    }

    #[test]
    fn an_empty_provider_list_is_rejected_when_parsed() {
        assert!(parse(r#"{"providers": []}"#).is_err());
    }

    #[test]
    fn a_token_ttl_over_an_hour_is_rejected_when_parsed() {
        assert!(parse(r#"{"token_ttl": "2h", "providers": ["example-idp"]}"#).is_err());
    }

    #[test]
    fn a_zero_clock_skew_is_accepted() {
        let config = parse(r#"{"clock_skew": "0s", "providers": ["example-idp"]}"#)
            .expect("zero clock skew is accepted");
        assert_eq!(config.clock_skew, Duration::ZERO);
    }

    #[test]
    fn omitted_settings_take_their_defaults() {
        let config = parse(r#"{"providers": ["example"]}"#).expect("the configuration parses");

        assert_eq!(config.token_ttl.get(), Duration::from_mins(10));
        assert_eq!(config.clock_skew, Duration::from_secs(30));
        assert_eq!(config.max_cached_tokens.get(), 4096);
        assert_eq!(config.providers.len(), 1);
    }

    #[test]
    fn configuration_round_trips_through_json() {
        let config = parse(r#"{"providers": ["example"]}"#).expect("the configuration parses");
        let json = serde_json::to_string(&config).expect("the configuration serializes");
        assert_eq!(parse(&json).expect("the configuration parses"), config);
    }

    #[test]
    fn token_ttl_accepts_both_bounds() {
        assert!(build(r#"{"token_ttl": "1m", "providers": ["example"]}"#).is_ok());
        assert!(build(r#"{"token_ttl": "1h", "providers": ["example"]}"#).is_ok());
    }

    #[test]
    fn token_ttl_outside_the_bounds_is_rejected() {
        for ttl in ["59s", "3601s", "0s"] {
            let json = format!(r#"{{"token_ttl": "{ttl}", "providers": ["example"]}}"#);
            assert!(parse(&json).is_err(), "token_ttl {ttl} must be rejected");
        }
    }

    #[test]
    fn zero_max_cached_tokens_is_rejected_by_the_type() {
        assert!(parse(r#"{"max_cached_tokens": 0, "providers": ["example"]}"#).is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(parse(r#"{"providers": ["example"], "bogus": 1}"#).is_err());
        assert!(parse(r#"{"providers": [{"example": {}}]}"#).is_err());
    }

    #[test]
    fn providers_are_sorted_and_deduplicated() {
        let config =
            parse(r#"{"providers": ["zeta", "alpha", "zeta"]}"#).expect("the configuration parses");
        let providers: Vec<&str> = config
            .providers()
            .iter()
            .map(ProviderName::as_str)
            .collect();
        assert_eq!(providers, ["alpha", "zeta"]);
    }

    #[test]
    fn setting_providers_sorts_deduplicates_and_rejects_empty() {
        let mut config = parse(r#"{"providers": ["example"]}"#).expect("the configuration parses");
        let providers =
            ["zeta", "alpha", "zeta"].map(|name| name.parse().expect("the provider name is valid"));
        config
            .set_providers(providers)
            .expect("providers are nonempty");
        let providers: Vec<&str> = config
            .providers()
            .iter()
            .map(ProviderName::as_str)
            .collect();
        assert_eq!(providers, ["alpha", "zeta"]);

        assert!(matches!(config.set_providers([]), Err(NoTokenProviders)));
        let providers: Vec<&str> = config
            .providers()
            .iter()
            .map(ProviderName::as_str)
            .collect();
        assert_eq!(providers, ["alpha", "zeta"]);
    }

    #[test]
    fn token_does_not_require_tls() {
        let config = parse(r#"{"providers": ["example"]}"#).expect("the configuration parses");
        assert!(!config.requires_tls());
    }

    #[test]
    fn token_offers_tokens_and_sessions() {
        let Ok(token) = build(r#"{"providers": ["example"]}"#) else {
            panic!("the configuration builds");
        };
        let capabilities = token.capabilities();
        assert!(capabilities.tokens);
        assert!(capabilities.sessions);
        assert!(!capabilities.certificates);
    }

    #[test]
    fn digest_is_the_sha256_of_the_token() {
        assert_eq!(
            digest("abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }
}
