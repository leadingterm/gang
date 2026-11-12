//! Login flows against the in-process test issuer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::support::{RefreshMode, TestIssuer, WEB_CLIENT_ID, web_client};
use gang_clock::{TestClock, Timestamp};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport, PortError,
    ProofVerifierPort, Rejection,
};
use gang_types::{ClientSecret, LoginSession, RefreshToken, Url};
use serde_json::{Value, json};

const START_MS: u64 = 1_800_000_000_000;
const REDIRECT: &str = "http://127.0.0.1:43111/callback";

type TestProvider = Provider;

fn new_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(START_MS)))
}

fn provider(clock: &Arc<TestClock>, config: OidcConfig) -> TestProvider {
    Provider::new(config, clock.clone()).expect("valid test configuration")
}

fn redirect_uri() -> Url {
    Url::parse(REDIRECT).expect("valid redirect uri")
}

fn callback(base: &str, pairs: &[(&str, &str)]) -> Url {
    Url::parse_with_params(base, pairs).expect("valid callback url")
}

fn query(url: &Url) -> HashMap<String, String> {
    url.query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect()
}

fn param(url: &Url, key: &str) -> String {
    url.query_pairs().find(|pair| pair.0 == key).map_or_else(
        || panic!("url has no `{key}` parameter"),
        |pair| pair.1.into_owned(),
    )
}

fn session_field(session: &LoginSession, field: &str) -> String {
    let state: Value = serde_json::from_str(session.expose()).expect("session is json");
    state[field]
        .as_str()
        .expect("session field is a string")
        .to_owned()
}

fn principal_text(config: &OidcConfig, claims: &Value) -> String {
    claims[config.principal_claim.as_str()]
        .as_str()
        .expect("principal claim is a string")
        .to_owned()
}

fn rejection<T>(result: Result<T, PortError>) -> Rejection {
    match result {
        Err(PortError::Rejected {
            reason: rejection, ..
        }) => rejection,
        Err(other) => panic!("expected a rejection, got {other}"),
        Ok(_) => panic!("expected a rejection, got success"),
    }
}

async fn begin(provider: &TestProvider) -> LoginChallenge {
    begin_as(provider, None).await
}

async fn begin_as(provider: &TestProvider, client: Option<&LoginClient>) -> LoginChallenge {
    let redirect = redirect_uri();
    provider
        .begin_login(&LoginRequest {
            redirect_uri: &redirect,
            client,
        })
        .await
        .expect("login starts")
}

async fn login(issuer: &TestIssuer<'_>, provider: &TestProvider, claims: &Value) -> LoginGrant {
    let challenge = begin(provider).await;
    let callback = issuer.approve(&challenge.authorization_url, claims);
    provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("login completes")
}

async fn verified(provider: &TestProvider, grant: &LoginGrant) -> String {
    provider
        .verify(&grant.proof)
        .await
        .expect("proof verifies")
        .as_str()
        .to_owned()
}

#[tokio::test]
async fn javascript_authorization_endpoint_is_rejected_before_login() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    issuer.set_discovery_authorization_endpoint(Some("javascript:alert(1)".to_owned()));
    let provider = provider(&clock, issuer.login_config());
    let redirect = redirect_uri();

    assert!(matches!(
        provider
            .begin_login(&LoginRequest {
                redirect_uri: &redirect,
                client: None,
            })
            .await,
        Err(PortError::Backend(_))
    ));
}

#[tokio::test]
async fn begin_login_builds_a_pkce_authorization_url() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let config = issuer.login_config();
    let provider = provider(&clock, config.clone());

    let first = begin(&provider).await;
    let params = query(&first.authorization_url);

    assert_eq!(params["response_type"], "code");
    assert_eq!(params["client_id"], config.audience.as_str());
    assert_eq!(params["redirect_uri"], REDIRECT);
    assert_eq!(params["scope"], "openid email");
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(params["code_challenge"].len(), 43);
    assert_eq!(params["prompt"], "consent");
    assert_ne!(params["state"], params["nonce"]);

    let second = begin(&provider).await;
    let second_params = query(&second.authorization_url);
    assert_ne!(params["state"], second_params["state"]);
    assert_ne!(
        session_field(&first.session, "verifier"),
        session_field(&second.session, "verifier")
    );
}

#[tokio::test]
async fn login_round_trip_yields_a_verifiable_proof_and_refresh_token() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let config = issuer.login_config();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);

    let grant = login(&issuer, &provider, &claims).await;

    assert!(grant.refresh_token.is_some());
    assert_eq!(verified(&provider, &grant).await, expected);
}

#[tokio::test]
async fn callback_pasted_as_text_completes_the_same_login() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let config = issuer.login_config();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);

    let direct = begin(&provider).await;
    let direct_callback = issuer.approve(&direct.authorization_url, &claims);
    let direct_grant = provider
        .complete_login(direct.session, &direct_callback)
        .await
        .expect("login completes");

    let pasted = begin(&provider).await;
    let pasted_callback = issuer.approve(&pasted.authorization_url, &claims);
    let pasted_text = Url::parse(pasted_callback.as_str()).expect("callback text parses");
    let pasted_grant = provider
        .complete_login(pasted.session, &pasted_text)
        .await
        .expect("pasted callback completes");

    assert_eq!(verified(&provider, &direct_grant).await, expected);
    assert_eq!(verified(&provider, &pasted_grant).await, expected);
}

#[tokio::test]
async fn callback_with_a_different_state_is_rejected_before_the_token_request() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let approved = issuer.approve(&challenge.authorization_url, &claims);
    let code = param(&approved, "code");
    let forged = callback(REDIRECT, &[("code", &code), ("state", "forged-state")]);

    assert_eq!(
        rejection(provider.complete_login(challenge.session, &forged).await),
        Rejection::InvalidProof
    );
    assert_eq!(issuer.token_requests(), 0);
}

#[tokio::test]
async fn callback_to_another_host_port_or_path_is_rejected_without_a_token_request() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    for base in [
        "http://127.0.0.1:43112/callback",
        "http://127.0.0.1:43111/other",
        "http://localhost:43111/callback",
    ] {
        let challenge = begin(&provider).await;
        let approved = issuer.approve(&challenge.authorization_url, &claims);
        let code = param(&approved, "code");
        let state = param(&challenge.authorization_url, "state");
        let moved = callback(base, &[("code", &code), ("state", &state)]);

        assert_eq!(
            rejection(provider.complete_login(challenge.session, &moved).await),
            Rejection::InvalidProof,
            "callback at {base}"
        );
    }
    assert_eq!(issuer.token_requests(), 0);
}

#[tokio::test]
async fn access_denied_at_the_callback_is_a_policy_denial() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let denied = TestIssuer::deny(&challenge.authorization_url);

    assert_eq!(
        rejection(provider.complete_login(challenge.session, &denied).await),
        Rejection::PolicyDenied
    );
}

#[tokio::test]
async fn callback_without_a_code_is_rejected_without_a_token_request() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let state = param(&challenge.authorization_url, "state");
    let codeless = callback(REDIRECT, &[("state", &state)]);

    assert_eq!(
        rejection(provider.complete_login(challenge.session, &codeless).await),
        Rejection::InvalidProof
    );
    assert_eq!(issuer.token_requests(), 0);
}

#[tokio::test]
async fn code_from_another_flow_fails_the_pkce_check_at_the_token_endpoint() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let first = begin(&provider).await;
    let second = begin(&provider).await;
    let second_issued = issuer.approve(&second.authorization_url, &claims);
    let code = param(&second_issued, "code");
    let state = param(&first.authorization_url, "state");
    let mismatched = callback(REDIRECT, &[("code", &code), ("state", &state)]);

    assert_eq!(
        rejection(provider.complete_login(first.session, &mismatched).await),
        Rejection::InvalidProof
    );
    assert_eq!(issuer.token_requests(), 1);
}

#[tokio::test]
async fn id_token_with_the_wrong_nonce_is_rejected() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let callback = issuer.approve_with_wrong_nonce(&challenge.authorization_url, &claims);

    assert_eq!(
        rejection(provider.complete_login(challenge.session, &callback).await),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn replayed_callback_is_rejected_because_the_code_is_single_use() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    let replay = LoginSession::new(challenge.session.expose());

    provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("first use of the code completes");

    assert_eq!(
        rejection(provider.complete_login(replay, &callback).await),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn provider_without_a_login_table_is_unsupported() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let mut config = issuer.config();
    config.login = None;
    let provider = provider(&clock, config);
    let redirect = redirect_uri();

    assert_eq!(provider.login(), LoginSupport::Unsupported);
    assert!(matches!(
        provider
            .begin_login(&LoginRequest {
                redirect_uri: &redirect,
                client: None,
            })
            .await,
        Err(PortError::Unsupported { operation: "login" })
    ));
    assert!(matches!(
        provider
            .complete_login(LoginSession::new("{}"), &redirect)
            .await,
        Err(PortError::Unsupported { operation: "login" })
    ));
    assert!(matches!(
        provider
            .renew(&RefreshToken::new("unused-refresh-token"), None)
            .await,
        Err(PortError::Unsupported { operation: "login" })
    ));
}

#[tokio::test]
async fn provider_with_a_login_table_is_interactive() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.login_config());

    assert_eq!(provider.login(), LoginSupport::Interactive);
}

#[tokio::test]
async fn renew_with_the_stored_refresh_token_yields_a_verifiable_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let config = issuer.login_config();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);

    let first = login(&issuer, &provider, &claims).await;
    let refresh = first.refresh_token.expect("login issues a refresh token");
    let renewed = provider
        .renew(&refresh, None)
        .await
        .expect("renewal succeeds");

    assert_eq!(verified(&provider, &renewed).await, expected);
}

#[tokio::test]
async fn rotating_issuer_replaces_the_refresh_token_and_retires_the_old_one() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    issuer.set_refresh_mode(RefreshMode::IdTokenRotating);
    let claims = issuer.valid_claims();
    let config = issuer.login_config();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);

    let first = login(&issuer, &provider, &claims).await;
    let first_refresh = first.refresh_token.expect("login issues a refresh token");

    let rotated = provider
        .renew(&first_refresh, None)
        .await
        .expect("renewal succeeds");
    let second_refresh = rotated
        .refresh_token
        .clone()
        .expect("rotation issues a new refresh token");
    assert!(!second_refresh.matches(first_refresh.expose()));
    assert_eq!(verified(&provider, &rotated).await, expected);

    provider
        .renew(&second_refresh, None)
        .await
        .expect("the rotated refresh token renews");
    assert_eq!(
        rejection(provider.renew(&first_refresh, None).await),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn renewal_without_an_id_token_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let first = login(&issuer, &provider, &claims).await;
    let refresh = first.refresh_token.expect("login issues a refresh token");

    issuer.set_refresh_mode(RefreshMode::NoIdToken);
    assert_eq!(
        rejection(provider.renew(&refresh, None).await),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn unknown_refresh_token_is_rejected() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.login_config());

    assert_eq!(
        rejection(
            provider
                .renew(&RefreshToken::new("never-issued-refresh-token"), None)
                .await
        ),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn expired_id_token_from_the_issuer_is_rejected_at_login() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.login_config());

    let mut claims = issuer.valid_claims();
    claims["exp"] = json!(START_MS / 1000 + 60);

    let challenge = begin(&provider).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    clock.advance(Duration::from_mins(2));

    assert_eq!(
        rejection(provider.complete_login(challenge.session, &callback).await),
        Rejection::InvalidProof
    );
}

#[tokio::test]
async fn wrong_client_secret_is_a_backend_failure() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let mut config = issuer.login_config();
    config
        .login
        .as_mut()
        .expect("login table present")
        .client_secret = Some(ClientSecret::new("not-the-issuer-secret"));
    let provider = provider(&clock, config);

    let challenge = begin(&provider).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    let error = provider
        .complete_login(challenge.session, &callback)
        .await
        .expect_err("issuer rejects the wrong client secret");

    assert!(matches!(error, PortError::Backend(_)), "got {error}");
}

#[tokio::test]
async fn web_client_login_authorizes_with_the_web_client_id() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider(&clock, issuer.config_accepting_web_client());
    let web = web_client();

    let challenge = begin_as(&provider, Some(&web)).await;

    assert_eq!(
        param(&challenge.authorization_url, "client_id"),
        WEB_CLIENT_ID
    );
}

#[tokio::test]
async fn web_client_login_is_exchanged_as_the_web_client_and_verifies() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.config_accepting_web_client());
    let web = web_client();

    let challenge = begin_as(&provider, Some(&web)).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    let grant = provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("login completes");

    assert_eq!(verified(&provider, &grant).await, "dev@example.test");
    assert_eq!(issuer.code_exchanges(), [WEB_CLIENT_ID]);
}

#[tokio::test]
async fn login_without_an_override_authorizes_with_the_configured_client() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let provider = provider(&clock, issuer.login_config());

    let challenge = begin(&provider).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("login completes");

    assert_eq!(
        param(&challenge.authorization_url, "client_id"),
        issuer.config().audience.as_str()
    );
    assert_eq!(issuer.code_exchanges(), [issuer.config().audience.as_str()]);
}

#[tokio::test]
async fn renew_without_a_client_uses_the_configured_client() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let config = issuer.config_accepting_web_client();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);
    let web = web_client();

    let challenge = begin_as(&provider, Some(&web)).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    let first = provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("login completes");
    let refresh = first.refresh_token.expect("login issues a refresh token");
    let renewed = provider
        .renew(&refresh, None)
        .await
        .expect("renewal succeeds");

    assert_eq!(verified(&provider, &renewed).await, expected);
    assert_eq!(
        issuer.refresh_clients(),
        [issuer.config().audience.as_str()]
    );
}

#[tokio::test]
async fn renew_with_the_web_client_uses_that_client() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let claims = issuer.valid_claims();
    let config = issuer.config_accepting_web_client();
    let expected = principal_text(&config, &claims);
    let provider = provider(&clock, config);
    let web = web_client();

    let challenge = begin_as(&provider, Some(&web)).await;
    let callback = issuer.approve(&challenge.authorization_url, &claims);
    let first = provider
        .complete_login(challenge.session, &callback)
        .await
        .expect("login completes");
    let refresh = first.refresh_token.expect("login issues a refresh token");
    let renewed = provider
        .renew(&refresh, Some(&web))
        .await
        .expect("renewal succeeds");

    assert_eq!(verified(&provider, &renewed).await, expected);
    assert_eq!(issuer.refresh_clients(), [WEB_CLIENT_ID]);
}
