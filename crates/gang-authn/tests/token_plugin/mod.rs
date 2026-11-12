//! The `token` plugin through its ports: minting a bearer token against a proof, authenticating
//! requests with it, its expiry, its memo, and the operations it does not offer.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gang_authn::{Token, TokenConfig};
use gang_clock::Timestamp;
use gang_spi::{
    Authenticated, AuthenticationPort, Capabilities, CertificateIssue, CertificateTarget,
    ClientCertificates, CredentialLifecyclePort, IssuedToken, Peer, Plugin, PluginContext,
    PortError, Rejection, Scheme, Subject, TokenIssue,
};
use gang_types::{CertificateRequestPem, Identity, Proof, ProviderName};
use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderValue};

use crate::support::{TestHost, bearer, evidence, identity, session_evidence};

const PROVIDER: &str = "example-idp";
const ALICE: &str = "alice@example.com";
const BOB: &str = "bob@example.com";
const ALICE_PROOF: &str = "alice-proof";
const BOB_PROOF: &str = "bob-proof";
const CSR: &str = "-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----";

/// The instant every test starts at, on a whole second so expiry claims land exactly.
fn start() -> Timestamp {
    Timestamp::from_unix_millis(1_800_000_000_000)
}

/// A host that accepts Alice's and Bob's proofs from `PROVIDER`.
fn host() -> TestHost {
    let host = TestHost::new(start());
    host.accept_proof(PROVIDER, ALICE_PROOF, ALICE);
    host.accept_proof(PROVIDER, BOB_PROOF, BOB);
    host
}

fn plugin(json: &str) -> Token {
    let config: TokenConfig = serde_json::from_str(json).expect("the configuration parses");
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    Token::build(config, &context).expect("the configuration builds")
}

fn authorization(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(value).expect("test header is a valid value"),
    );
    headers
}

async fn issue(
    plugin: &Token,
    host: &TestHost,
    provider: &str,
    proof: &str,
) -> Result<IssuedToken, PortError> {
    let provider = ProviderName::try_new(provider).expect("test provider is a valid name");
    let proof = Proof::new(proof);
    plugin
        .issue_token(
            host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await
}

async fn issue_token(plugin: &Token, host: &TestHost, proof: &str) -> String {
    issue(plugin, host, PROVIDER, proof)
        .await
        .expect("the proof is accepted")
        .token
        .expose()
        .to_owned()
}

async fn authenticated(
    plugin: &Token,
    host: &TestHost,
    headers: &HeaderMap,
) -> Result<Option<Authenticated>, PortError> {
    let peer = Peer::default();
    plugin.authenticate(host, &evidence(&peer, headers)).await
}

async fn authenticate(
    plugin: &Token,
    host: &TestHost,
    headers: &HeaderMap,
) -> Result<Option<Identity>, PortError> {
    authenticated(plugin, host, headers)
        .await
        .map(|found| found.map(|authenticated| authenticated.identity.as_ref().clone()))
}

async fn issue_session(
    plugin: &Token,
    host: &TestHost,
    provider: &str,
    proof: &str,
) -> Result<IssuedToken, PortError> {
    let provider = ProviderName::try_new(provider).expect("test provider is a valid name");
    let proof = Proof::new(proof);
    plugin
        .issue_session(
            host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await
}

async fn session_token(plugin: &Token, host: &TestHost, proof: &str) -> String {
    issue_session(plugin, host, PROVIDER, proof)
        .await
        .expect("the proof is accepted")
        .token
        .expose()
        .to_owned()
}

/// Authenticates a request carrying `headers` and the session cookie `session`.
async fn authenticated_with_session(
    plugin: &Token,
    host: &TestHost,
    headers: &HeaderMap,
    session: &str,
) -> Result<Option<Authenticated>, PortError> {
    let peer = Peer::default();
    plugin
        .authenticate(host, &session_evidence(&peer, headers, session))
        .await
}

async fn authenticate_with_session(
    plugin: &Token,
    host: &TestHost,
    headers: &HeaderMap,
    session: &str,
) -> Result<Option<Identity>, PortError> {
    authenticated_with_session(plugin, host, headers, session)
        .await
        .map(|found| found.map(|authenticated| authenticated.identity.as_ref().clone()))
}

#[test]
fn scheme_is_token() {
    assert_eq!(
        plugin(r#"{"providers": ["example-idp"]}"#).scheme(),
        Scheme::Token
    );
}

#[test]
fn offers_tokens_sessions_and_no_certificates() {
    assert_eq!(
        plugin(r#"{"providers": ["example-idp"]}"#).capabilities(),
        Capabilities {
            certificates: false,
            tokens: true,
            sessions: true,
        }
    );
}

#[test]
fn never_asks_clients_for_a_certificate() {
    assert_eq!(
        plugin(r#"{"providers": ["example-idp"]}"#).client_certificates(),
        ClientCertificates::Never
    );
}

#[tokio::test]
async fn token_for_an_accepted_proof_authenticates_as_provider_and_principal() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    let found = authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");

    assert_eq!(found, Some(identity(PROVIDER, ALICE)));
}

#[tokio::test]
async fn issued_token_expires_in_the_configured_ttl() {
    let host = host();
    let plugin = plugin(r#"{"token_ttl": "2m", "providers": ["example-idp"]}"#);

    let issued = issue(&plugin, &host, PROVIDER, ALICE_PROOF)
        .await
        .expect("the proof is accepted");

    assert_eq!(issued.expires_in, Duration::from_mins(2));
}

#[tokio::test]
async fn unknown_provider_is_not_found() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = issue(&plugin, &host, "unknown-idp", ALICE_PROOF).await;

    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Provider(name))) if name.to_string() == "unknown-idp"
    ));
}

#[tokio::test]
async fn provider_known_to_host_but_not_to_plugin_is_not_found() {
    let host = host();
    host.accept_proof("other-idp", "other-proof", "carol@example.com");
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = issue(&plugin, &host, "other-idp", "other-proof").await;

    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Provider(name))) if name.to_string() == "other-idp"
    ));
}

#[tokio::test]
async fn rejected_proof_is_refused_as_invalid_proof() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = issue(&plugin, &host, PROVIDER, "wrong-proof").await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn host_revoked_principal_is_refused_as_principal_revoked() {
    let host = host();
    host.revoke_principal_in_host(&identity(PROVIDER, ALICE));
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = issue(&plugin, &host, PROVIDER, ALICE_PROOF).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::PrincipalRevoked,
            ..
        })
    ));
}

#[tokio::test]
async fn request_without_authorization_header_is_not_ours() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let found = authenticate(&plugin, &host, &HeaderMap::new())
        .await
        .expect("a request without a credential is not an error");

    assert_eq!(found, None);
}

#[tokio::test]
async fn basic_credential_is_not_ours() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let found = authenticate(&plugin, &host, &authorization("Basic dXNlcjpwYXNz"))
        .await
        .expect("a basic credential is not an error");

    assert_eq!(found, None);
}

#[tokio::test]
async fn lowercase_bearer_scheme_is_accepted() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    let found = authenticate(&plugin, &host, &authorization(&format!("bearer {token}")))
        .await
        .expect("the token is valid");

    assert_eq!(found, Some(identity(PROVIDER, ALICE)));
}

#[tokio::test]
async fn garbage_bearer_token_is_rejected_as_invalid_proof() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = authenticate(&plugin, &host, &authorization("Bearer garbage")).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn token_is_valid_until_expiry_plus_clock_skew() {
    let host = host();
    let plugin = plugin(r#"{"token_ttl": "1m", "clock_skew": "5s", "providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    // Expiry is at 60s and the skew allowance runs to 65s.
    host.advance(Duration::from_secs(64));
    let found = authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is still valid just before expiry plus skew");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    host.advance(Duration::from_secs(2));
    let result = authenticate(&plugin, &host, &bearer(&token)).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn token_authentication_reports_when_the_token_stops_being_valid() {
    let host = host();
    let plugin = plugin(r#"{"token_ttl": "1m", "clock_skew": "5s", "providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    // The token expires at `now + token_ttl` rounded up to a whole second, as `mint` writes it,
    // and the verifier accepts it until that expiry plus the clock skew.
    let expires_at = (start() + Duration::from_mins(1))
        .as_unix_millis()
        .div_ceil(1000)
        * 1000;
    let expected = Authenticated {
        identity: Arc::new(identity(PROVIDER, ALICE)),
        valid_until: Some(Timestamp::from_unix_millis(expires_at) + Duration::from_secs(5)),
    };

    let fresh = authenticated(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");
    assert_eq!(fresh, Some(expected.clone()));

    let memoized = authenticated(&plugin, &host, &bearer(&token))
        .await
        .expect("the memoized token is valid");
    assert_eq!(memoized, Some(expected));
}

#[tokio::test]
async fn memoized_token_stops_authenticating_once_its_validity_window_closes() {
    let host = host();
    let plugin = plugin(r#"{"token_ttl": "1m", "clock_skew": "5s", "providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;
    let found = authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    // Past expiry but inside the skew allowance, the memoized identity is still served.
    host.advance(Duration::from_secs(61));
    let found = authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the memoized token is still valid inside the skew allowance");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    host.advance(Duration::from_secs(5));
    let result = authenticate(&plugin, &host, &bearer(&token)).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn repeated_authentication_reads_the_signer_key_once() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");
    authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");

    assert_eq!(host.signer_key_reads(), 1);
}

#[tokio::test]
async fn tokens_authenticate_correctly_when_the_memo_holds_one_entry() {
    let host = host();
    let plugin = plugin(r#"{"max_cached_tokens": 1, "providers": ["example-idp"]}"#);
    let alice_token = issue_token(&plugin, &host, ALICE_PROOF).await;
    let bob_token = issue_token(&plugin, &host, BOB_PROOF).await;

    let found = authenticate(&plugin, &host, &bearer(&alice_token))
        .await
        .expect("alice's token is valid");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    let found = authenticate(&plugin, &host, &bearer(&bob_token))
        .await
        .expect("bob's token is valid");
    assert_eq!(found, Some(identity(PROVIDER, BOB)));

    // Bob's token has evicted alice's, so alice's is verified again.
    let found = authenticate(&plugin, &host, &bearer(&alice_token))
        .await
        .expect("alice's evicted token is still valid");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));
}

#[tokio::test]
async fn issue_certificate_is_unsupported() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let provider = ProviderName::try_new(PROVIDER).expect("test provider is a valid name");
    let proof = Proof::new(ALICE_PROOF);
    let csr = CertificateRequestPem::try_new(CSR).expect("test request is a valid PEM block");

    let result = plugin
        .issue_certificate(
            &host,
            &CertificateIssue {
                provider: &provider,
                proof: &proof,
                csr: &csr,
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "issue_certificate"
        })
    ));
}

#[tokio::test]
async fn revoke_certificates_is_unsupported() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let alice = identity(PROVIDER, ALICE);

    let result = plugin
        .revoke_certificates(&host, &CertificateTarget::Identity(&alice))
        .await;

    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "revoke_certificates"
        })
    ));
}

#[tokio::test]
async fn revoked_serials_is_unsupported() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = plugin.revoked_serials(&host).await;

    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "revoked_serials"
        })
    ));
}

#[tokio::test]
async fn a_session_token_authenticates_as_a_session_cookie() {
    let host = host();
    let plugin = plugin(r#"{"token_ttl": "1m", "clock_skew": "5s", "providers": ["example-idp"]}"#);
    let token = session_token(&plugin, &host, ALICE_PROOF).await;

    let found = authenticated_with_session(&plugin, &host, &HeaderMap::new(), &token)
        .await
        .expect("the session token is valid");

    let expires_at = (start() + Duration::from_mins(1))
        .as_unix_millis()
        .div_ceil(1000)
        * 1000;
    let expected = Authenticated {
        identity: Arc::new(identity(PROVIDER, ALICE)),
        valid_until: Some(Timestamp::from_unix_millis(expires_at) + Duration::from_secs(5)),
    };
    assert_eq!(found, Some(expected));
}

#[tokio::test]
async fn a_session_token_is_refused_as_a_bearer() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = session_token(&plugin, &host, ALICE_PROOF).await;

    let result = authenticate(&plugin, &host, &bearer(&token)).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn a_session_token_stays_refused_as_a_bearer_after_it_authenticated_as_a_cookie() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = session_token(&plugin, &host, ALICE_PROOF).await;
    let found = authenticate_with_session(&plugin, &host, &HeaderMap::new(), &token)
        .await
        .expect("the session token is valid");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    let result = authenticate(&plugin, &host, &bearer(&token)).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn a_client_token_is_refused_as_a_session_cookie() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;

    let result = authenticate_with_session(&plugin, &host, &HeaderMap::new(), &token).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn a_client_token_stays_refused_as_a_session_cookie_after_it_authenticated_as_a_bearer() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let token = issue_token(&plugin, &host, ALICE_PROOF).await;
    let found = authenticate(&plugin, &host, &bearer(&token))
        .await
        .expect("the token is valid");
    assert_eq!(found, Some(identity(PROVIDER, ALICE)));

    let result = authenticate_with_session(&plugin, &host, &HeaderMap::new(), &token).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn a_bearer_token_wins_over_a_session_cookie() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let alice_token = issue_token(&plugin, &host, ALICE_PROOF).await;
    let bob_session = session_token(&plugin, &host, BOB_PROOF).await;

    let found = authenticate_with_session(&plugin, &host, &bearer(&alice_token), &bob_session)
        .await
        .expect("the bearer token is valid");

    assert_eq!(found, Some(identity(PROVIDER, ALICE)));
}

#[tokio::test]
async fn an_invalid_bearer_token_is_refused_even_with_a_valid_session_cookie() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let session = session_token(&plugin, &host, ALICE_PROOF).await;

    let result = authenticate_with_session(&plugin, &host, &bearer("garbage"), &session).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn a_basic_credential_falls_through_to_the_session_cookie() {
    let host = host();
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);
    let session = session_token(&plugin, &host, BOB_PROOF).await;

    let found = authenticate_with_session(
        &plugin,
        &host,
        &authorization("Basic dXNlcjpwYXNz"),
        &session,
    )
    .await
    .expect("the session token is valid");

    assert_eq!(found, Some(identity(PROVIDER, BOB)));
}

#[tokio::test]
async fn session_for_a_provider_known_to_host_but_not_to_plugin_is_not_found() {
    let host = host();
    host.accept_proof("other-idp", "other-proof", "carol@example.com");
    let plugin = plugin(r#"{"providers": ["example-idp"]}"#);

    let result = issue_session(&plugin, &host, "other-idp", "other-proof").await;

    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Provider(name))) if name.to_string() == "other-idp"
    ));
}
