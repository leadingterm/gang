//! The in-process issuer, verified through the real `gang-oidc` provider.

use std::error::Error;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{SystemClock, TestClock, Timestamp};
use gang_e2e::{
    AuthorizationRecord, RefreshOutcome, Settings, SubjectPolicy, TestIssuer, TokenSpec,
};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{PortError, ProofVerifierPort};
use gang_tls::{
    HttpClient, HttpProtocol, HttpReply, HttpRequest, HttpSettings, HttpTransport, RequestBody,
};
use gang_types::{NonZeroBytes, NonZeroDuration, Principal, Proof, Url};
use http::{Method, StatusCode, header};
use serde_json::json;
use tokio::runtime::Builder;

type TestResult = Result<(), Box<dyn Error>>;

/// The audience the deployment accepts; must equal the issuer's admitted audience.
const AUDIENCE: &str = "gang-e2e";
/// The organization the deployment admits; must equal the issuer's admitted organization.
const ORGANIZATION: &str = "gang-e2e";
/// Deadline for each issuer request.
const HTTP_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);
/// Maximum buffered issuer response.
const MAX_BODY: NonZeroBytes = NonZeroBytes::kib(64);
/// Authorization parameters requesting offline access and consent.
const OFFLINE: [(&str, &str); 2] = [gang_e2e::OFFLINE_ACCESS, gang_e2e::CONSENT_PROMPT];

/// A provider configured the way a deployment would be: the issuer, the audience, the
/// principal claim `sub`, and the organization required exactly.
fn provider_for(issuer: &TestIssuer) -> Result<Provider, Box<dyn Error>> {
    let config: OidcConfig = serde_json::from_value(json!({
        "issuer_url": issuer.url(),
        "audience": AUDIENCE,
        "principal_claim": "sub",
        "required_claims": { "org": ORGANIZATION },
    }))?;
    Ok(Provider::new(config, Arc::new(SystemClock::new()))?)
}

/// Starts an issuer, mints a token for `spec`, and asks the provider to verify it. The outer
/// error is a failure of the harness; the inner one is the provider's verdict. The issuer is
/// dropped here, on a plain thread, once the verification is over.
fn verify(spec: &TokenSpec) -> Result<Result<Principal, PortError>, Box<dyn Error>> {
    let issuer = TestIssuer::start(&Settings::default())?;
    let provider = provider_for(&issuer)?;
    let token = issuer.mint(spec)?;
    let runtime = Builder::new_current_thread().enable_all().build()?;
    Ok(runtime.block_on(ProofVerifierPort::verify(&provider, &Proof::new(token))))
}

#[test]
fn admitted_token_verifies_to_its_subject() -> TestResult {
    let principal = verify(&TokenSpec::admitted("subject-1"))??;

    assert_eq!(principal.as_str(), "subject-1");
    Ok(())
}

#[test]
fn foreign_organization_token_is_refused() -> TestResult {
    let result = verify(&TokenSpec::foreign_organization("subject-1"))?;

    assert!(
        matches!(result, Err(PortError::Rejected { .. })),
        "a token from another organization is refused, got {result:?}"
    );
    Ok(())
}

#[test]
fn foreign_audience_token_is_refused() -> TestResult {
    let result = verify(&TokenSpec::foreign_audience("subject-1"))?;

    assert!(
        matches!(result, Err(PortError::Rejected { .. })),
        "a token for another audience is refused, got {result:?}"
    );
    Ok(())
}

/// A plain HTTP client and the runtime driving its requests.
struct IssuerClient {
    client: HttpClient,
    runtime: tokio::runtime::Runtime,
}

impl IssuerClient {
    /// Sends form parameters either as a query string or as a POST body.
    fn send(
        &self,
        method: Method,
        mut url: Url,
        fields: &[(&str, &str)],
    ) -> Result<HttpReply, Box<dyn Error>> {
        let body = if method == Method::GET {
            url.query_pairs_mut().extend_pairs(fields.iter().copied());
            None
        } else {
            let mut encoded = Url::parse("http://localhost/")?;
            encoded
                .query_pairs_mut()
                .extend_pairs(fields.iter().copied());
            Some(RequestBody {
                content_type: "application/x-www-form-urlencoded",
                bytes: encoded.query().unwrap_or_default().to_owned().into(),
            })
        };
        Ok(self.runtime.block_on(self.client.send(HttpRequest {
            method,
            url: &url,
            bearer: None,
            headers: &[],
            body,
        }))?)
    }
}

fn http_client() -> Result<IssuerClient, Box<dyn Error>> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let client = HttpClient::try_new(&HttpSettings {
        transport: HttpTransport::Plain,
        protocol: HttpProtocol::Negotiated,
        timeout: HTTP_TIMEOUT,
        max_body: MAX_BODY,
        clock: Arc::new(SystemClock::new()),
    })?;
    Ok(IssuerClient { client, runtime })
}

/// Authorizes a subject and returns the redirect location.
fn authorize(
    issuer: &TestIssuer,
    client: &IssuerClient,
    verifier: &str,
    params: &[(&str, &str)],
    subject: &str,
) -> Result<String, Box<dyn Error>> {
    use sha2::{Digest, Sha256};
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut fields = vec![
        ("response_type", "code"),
        ("client_id", "gang-e2e"),
        (
            "redirect_uri",
            "https://client.example/callback?existing=value",
        ),
        ("scope", "openid profile"),
        ("state", "state-1"),
        ("nonce", "nonce-1"),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
        ("login_hint", subject),
    ];
    fields.extend_from_slice(params);
    let response = client.send(Method::GET, issuer.url().join("authorize")?, &fields)?;
    assert_eq!(response.status, StatusCode::FOUND);
    Ok(response
        .headers
        .get(header::LOCATION)
        .ok_or("missing location")?
        .to_str()?
        .to_owned())
}

/// Extracts the authorization code and state from a redirect.
fn code_and_state(location: &str) -> Result<(String, String), Box<dyn Error>> {
    let url = Url::parse(location)?;
    let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    Ok((
        pairs.get("code").ok_or("missing code")?.clone(),
        pairs.get("state").ok_or("missing state")?.clone(),
    ))
}

/// Exchanges an authorization code for tokens.
fn exchange(
    issuer: &TestIssuer,
    client: &IssuerClient,
    code: &str,
    verifier: &str,
    client_id: &str,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "authorization_code"),
            ("code", code),
            (
                "redirect_uri",
                "https://client.example/callback?existing=value",
            ),
            ("code_verifier", verifier),
            ("client_id", client_id),
        ],
    )?;
    assert_eq!(response.status, StatusCode::OK);
    Ok(serde_json::from_slice(&response.body)?)
}

/// Sends a refresh token grant as the public client.
fn refresh(
    issuer: &TestIssuer,
    client: &IssuerClient,
    token: &str,
) -> Result<HttpReply, Box<dyn Error>> {
    client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", token),
            ("client_id", "gang-e2e"),
        ],
    )
}

#[test]
fn authorization_redirects_with_code_and_state() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let (code, state) = code_and_state(&authorize(
        &issuer,
        &client,
        "verifier-123",
        &OFFLINE,
        "subject-1",
    )?)?;
    assert!(!code.is_empty());
    assert_eq!(state, "state-1");
    Ok(())
}

#[test]
fn authorization_code_exchange_checks_verifier_and_is_single_use() -> TestResult {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let verifier = "verifier-123";
    let (code, _) = code_and_state(&authorize(
        &issuer,
        &client,
        verifier,
        &OFFLINE,
        "subject-1",
    )?)?;
    let token_url = issuer.url().join("token")?;
    let post = |code: &str, verifier: &str| {
        client.send(
            Method::POST,
            token_url.clone(),
            &[
                ("grant_type", "authorization_code"),
                ("code", code),
                (
                    "redirect_uri",
                    "https://client.example/callback?existing=value",
                ),
                ("code_verifier", verifier),
                ("client_id", "gang-e2e"),
            ],
        )
    };
    let wrong = post(&code, "wrong-verifier")?;
    assert_eq!(wrong.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&wrong.body)?["error"],
        "invalid_grant"
    );
    let (code, _) = code_and_state(&authorize(
        &issuer,
        &client,
        verifier,
        &OFFLINE,
        "subject-1",
    )?)?;
    let response = post(&code, verifier)?;
    let tokens: serde_json::Value = serde_json::from_slice(&response.body)?;
    assert_eq!(tokens["token_type"], "Bearer");
    let payload = tokens["id_token"]
        .as_str()
        .ok_or("missing id_token")?
        .split('.')
        .nth(1)
        .ok_or("missing payload")?;
    let claims: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    assert_eq!(claims["sub"], "subject-1");
    assert_eq!(claims["aud"], "gang-e2e");
    assert_eq!(claims["nonce"], "nonce-1");
    let reused = post(&code, verifier)?;
    assert_eq!(reused.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&reused.body)?["error"],
        "invalid_grant"
    );
    Ok(())
}

#[test]
fn dashboard_client_requires_secret() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", "unknown"),
            ("client_id", gang_e2e::DASHBOARD_CLIENT_ID),
        ],
    )?;
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
        "invalid_client"
    );
    Ok(())
}

#[test]
fn public_client_rejects_client_secret() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    for secret in ["secret", ""] {
        let response = client.send(
            Method::POST,
            issuer.url().join("token")?,
            &[
                ("grant_type", "refresh_token"),
                ("refresh_token", "unknown"),
                ("client_id", AUDIENCE),
                ("client_secret", secret),
            ],
        )?;
        assert_eq!(response.status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
            "invalid_client"
        );
    }
    Ok(())
}

#[test]
fn invalid_redirect_does_not_prevent_a_valid_exchange() -> TestResult {
    use sha2::{Digest, Sha256};

    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let authorize_url = issuer.url().join("authorize")?;
    let verifier = "verifier-123";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let response = client.send(
        Method::GET,
        authorize_url,
        &[
            ("response_type", "code"),
            ("client_id", AUDIENCE),
            ("redirect_uri", "not a url"),
            ("scope", "openid"),
            ("state", "state-1"),
            ("nonce", "nonce-1"),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("login_hint", "subject-1"),
        ],
    )?;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert!(response.headers.get(header::LOCATION).is_none());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
        "invalid_request"
    );

    let (code, _) = code_and_state(&authorize(
        &issuer,
        &client,
        verifier,
        &OFFLINE,
        "subject-1",
    )?)?;
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            (
                "redirect_uri",
                "https://client.example/callback?existing=value",
            ),
            ("code_verifier", verifier),
            ("client_id", AUDIENCE),
        ],
    )?;
    assert_eq!(response.status, StatusCode::OK);
    let tokens: serde_json::Value = serde_json::from_slice(&response.body)?;
    assert!(tokens["id_token"].as_str().is_some());
    Ok(())
}

#[test]
fn refresh_returns_no_refresh_token_counts_and_can_be_rejected() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let verifier = "verifier-123";
    let (code, _) = code_and_state(&authorize(
        &issuer,
        &client,
        verifier,
        &OFFLINE,
        "subject-1",
    )?)?;
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            (
                "redirect_uri",
                "https://client.example/callback?existing=value",
            ),
            ("code_verifier", verifier),
            ("client_id", "gang-e2e"),
        ],
    )?;
    let initial: serde_json::Value = serde_json::from_slice(&response.body)?;
    let refresh = initial["refresh_token"]
        .as_str()
        .ok_or("missing refresh_token")?
        .to_owned();
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", &refresh),
            ("client_id", "gang-e2e"),
        ],
    )?;
    let tokens: serde_json::Value = serde_json::from_slice(&response.body)?;
    assert!(tokens.get("refresh_token").is_none());
    assert_eq!(issuer.refresh_grants(&refresh), 1);
    issuer.reject_refresh_token(&refresh);
    let response = client.send(
        Method::POST,
        issuer.url().join("token")?,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", &refresh),
            ("client_id", "gang-e2e"),
        ],
    )?;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
        "invalid_grant"
    );
    Ok(())
}

#[test]
fn expired_token_is_expired() -> TestResult {
    let settings = Settings::default();
    let lifetime = settings.token_lifetime.as_secs();
    let now = 1_700_000_000_u64;
    let clock = TestClock::new(Timestamp::from_unix_millis(now * 1000));
    let issuer = TestIssuer::start_with_clock(&settings, Arc::new(clock))?;
    let token = issuer.mint_expired(&TokenSpec::admitted("subject-1"))?;
    let payload = token.split('.').nth(1).ok_or("missing payload")?;
    let claims: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    assert_eq!(claims["iat"], now - 2 * lifetime);
    assert_eq!(claims["nbf"], now - 2 * lifetime);
    assert_eq!(claims["exp"], now - lifetime);
    Ok(())
}

/// Authorizes and exchanges a code for a subject.
fn login(
    issuer: &TestIssuer,
    client: &IssuerClient,
    params: &[(&str, &str)],
    subject: &str,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let verifier = "verifier-123";
    let (code, state) = code_and_state(&authorize(issuer, client, verifier, params, subject)?)?;
    assert_eq!(state, "state-1");
    exchange(issuer, client, &code, verifier, "gang-e2e")
}

/// Only offline authorization issues a refresh token and records consent.
#[test]
fn refresh_token_only_for_offline_authorization() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let offline = login(&issuer, &client, &OFFLINE, "subject-1")?;
    assert!(offline["refresh_token"].as_str().is_some());
    let online = login(&issuer, &client, &[], "subject-1")?;
    assert!(online.get("refresh_token").is_none());
    assert_eq!(
        issuer.authorizations("subject-1"),
        vec![
            AuthorizationRecord {
                client_id: "gang-e2e".to_owned(),
                offline: true,
                consent: true
            },
            AuthorizationRecord {
                client_id: "gang-e2e".to_owned(),
                offline: false,
                consent: false
            },
        ]
    );
    Ok(())
}

/// A provider without refresh tokens omits them even for offline access.
#[test]
fn without_refresh_tokens_policy_omits_them_even_offline() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    issuer.set_policy("subject-1", SubjectPolicy::WithoutRefreshTokens);
    let tokens = login(&issuer, &client, &OFFLINE, "subject-1")?;
    assert!(tokens.get("refresh_token").is_none());
    Ok(())
}

/// Reusing a retired token revokes its entire rotation family.
#[test]
fn rotation_retires_the_presented_token_and_reuse_revokes_the_family() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    issuer.set_policy("subject-1", SubjectPolicy::Rotating);
    let tokens = login(&issuer, &client, &OFFLINE, "subject-1")?;
    let t1 = tokens["refresh_token"]
        .as_str()
        .ok_or("missing refresh_token")?;
    let rotated = refresh(&issuer, &client, t1)?;
    assert_eq!(rotated.status, StatusCode::OK);
    let rotated_tokens: serde_json::Value = serde_json::from_slice(&rotated.body)?;
    let t2 = rotated_tokens["refresh_token"]
        .as_str()
        .ok_or("missing rotated refresh_token")?;
    assert_ne!(t1, t2);
    for token in [t1, t2] {
        let response = refresh(&issuer, &client, token)?;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
            "invalid_grant"
        );
    }
    let records = issuer.refreshes("subject-1");
    assert_eq!(records.len(), 3);
    assert_eq!(
        records
            .iter()
            .map(|record| &record.outcome)
            .collect::<Vec<_>>(),
        vec![
            &RefreshOutcome::Rotated {
                issued: t2.to_owned()
            },
            &RefreshOutcome::ReuseDetected,
            &RefreshOutcome::Refused
        ]
    );
    assert!(records.iter().all(|record| record.client_id == "gang-e2e"));
    assert_eq!(
        records
            .iter()
            .map(|record| record.refresh_token.as_str())
            .collect::<Vec<_>>(),
        vec![t1, t1, t2]
    );
    Ok(())
}

/// A suspended subject is denied authorization and refresh.
#[test]
fn suspended_subject_is_denied_and_refused() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    let tokens = login(&issuer, &client, &OFFLINE, "subject-1")?;
    let token = tokens["refresh_token"]
        .as_str()
        .ok_or("missing refresh_token")?;
    issuer.set_policy("subject-1", SubjectPolicy::Suspended);
    let location = authorize(&issuer, &client, "verifier-123", &OFFLINE, "subject-1")?;
    let url = Url::parse(&location)?;
    let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(
        pairs.get("error").map(String::as_str),
        Some("access_denied")
    );
    assert_eq!(pairs.get("state").map(String::as_str), Some("state-1"));
    assert!(!pairs.contains_key("code"));
    let response = refresh(&issuer, &client, token)?;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body)?["error"],
        "invalid_grant"
    );
    let records = issuer.refreshes("subject-1");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, RefreshOutcome::Refused);
    assert_eq!(issuer.authorizations("subject-1").len(), 2);
    Ok(())
}

/// Authorization records are isolated by subject.
#[test]
fn records_are_per_subject() -> TestResult {
    let issuer = TestIssuer::start(&Settings::default())?;
    let client = http_client()?;
    login(&issuer, &client, &OFFLINE, "subject-1")?;
    login(&issuer, &client, &[], "subject-2")?;
    assert_eq!(
        issuer.authorizations("subject-1"),
        vec![AuthorizationRecord {
            client_id: "gang-e2e".to_owned(),
            offline: true,
            consent: true
        }]
    );
    assert_eq!(
        issuer.authorizations("subject-2"),
        vec![AuthorizationRecord {
            client_id: "gang-e2e".to_owned(),
            offline: false,
            consent: false
        }]
    );
    Ok(())
}
