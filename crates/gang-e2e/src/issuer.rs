//! In-process `OpenID` Connect issuer for the identity scenarios.

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{Clock, SystemClock};
use gang_testkit::generate_es256_key;
use gang_types::{Es256Key, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};

use crate::errors::{E2eError, Result};
use crate::settings::Settings;

/// The audience every admitted token carries, and the one the deployment is configured with.
pub const AUDIENCE: &str = "gang-e2e";
/// The organization every admitted token carries.
pub const ORGANIZATION: &str = "gang-e2e";
/// The claim that names the organization of a token.
pub const ORGANIZATION_CLAIM: &str = "org";
/// The confidential dashboard client identifier.
pub const DASHBOARD_CLIENT_ID: &str = "gang-e2e-dashboard";
/// The shared secret required by the confidential dashboard client.
pub const DASHBOARD_CLIENT_SECRET: &str = "gang-e2e-dashboard-secret";
/// The authorization parameter that asks Google-style providers for a refresh token. The e2e
/// deployments send it in `[identity.providers.e2e.oidc.login.offline]`.
pub const OFFLINE_ACCESS: (&str, &str) = ("access_type", "offline");
/// The parameter that asks for consent again, sent beside [`OFFLINE_ACCESS`] as Google needs it.
pub const CONSENT_PROMPT: (&str, &str) = ("prompt", "consent");

/// How the issuer treats the logins and refresh tokens of one subject (the `login_hint`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubjectPolicy {
    /// A code grant returns a refresh token only when its authorization request carried
    /// [`OFFLINE_ACCESS`]; a refresh grant returns no new refresh token.
    #[default]
    Standard,
    /// As `Standard`, but every refresh grant rotates: it returns a new refresh token of the same
    /// family and retires the one presented; presenting a retired token revokes the whole family.
    Rotating,
    /// A suspended account: authorization requests are answered with `error=access_denied` and no
    /// code, and every refresh grant is refused.
    Suspended,
    /// A misconfigured provider: code grants never return a refresh token.
    WithoutRefreshTokens,
}

/// One authorization request the issuer answered for a subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRecord {
    /// The `client_id` of the request.
    pub client_id: String,
    /// Whether the request carried [`OFFLINE_ACCESS`].
    pub offline: bool,
    /// Whether the request carried [`CONSENT_PROMPT`].
    pub consent: bool,
}

/// How the issuer answered one refresh grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// Granted without a new refresh token.
    Granted,
    /// Granted with the new refresh token `issued`; the presented one is retired.
    Rotated {
        /// The refresh token returned in place of the presented one.
        issued: String,
    },
    /// Refused: rejected token, wrong client, suspended subject, or revoked family.
    Refused,
    /// Refused because the token was already retired; its whole family is now revoked.
    ReuseDetected,
}

/// One refresh grant the issuer answered for a subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshRecord {
    /// The authenticated `client_id` of the grant.
    pub client_id: String,
    /// The refresh token presented.
    pub refresh_token: String,
    /// The answer.
    pub outcome: RefreshOutcome,
}
const KEY_ID: &str = "gang-e2e-key";

/// The claims of a token to mint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSpec {
    /// The `sub` claim, which names the subject.
    pub subject: String,
    /// The `aud` claim.
    pub audience: String,
    /// The value of [`ORGANIZATION_CLAIM`].
    pub organization: String,
}

impl TokenSpec {
    /// A token the deployment admits: [`AUDIENCE`] and [`ORGANIZATION`].
    #[must_use]
    pub fn admitted(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            audience: AUDIENCE.to_owned(),
            organization: ORGANIZATION.to_owned(),
        }
    }
    /// A token from an organization the deployment does not admit.
    #[must_use]
    pub fn foreign_organization(subject: impl Into<String>) -> Self {
        Self {
            organization: "other-org".to_owned(),
            ..Self::admitted(subject)
        }
    }
    /// A token for an audience the deployment does not accept.
    #[must_use]
    pub fn foreign_audience(subject: impl Into<String>) -> Self {
        Self {
            audience: "other-audience".to_owned(),
            ..Self::admitted(subject)
        }
    }
}

struct Documents {
    discovery: Value,
    jwks: Value,
}
#[derive(Default)]
struct Grants {
    codes: HashMap<String, CodeGrant>,
    refresh: HashMap<String, RefreshGrant>,
    policies: HashMap<String, SubjectPolicy>,
    authorizations: HashMap<String, Vec<AuthorizationRecord>>,
    refreshes: HashMap<String, Vec<RefreshRecord>>,
    revoked_families: HashSet<u64>,
    next_family: u64,
}
struct CodeGrant {
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    nonce: String,
    subject: String,
    offline: bool,
}
struct RefreshGrant {
    client_id: String,
    subject: String,
    grants: usize,
    rejected: bool,
    family: u64,
    retired: bool,
}
#[derive(Clone)]
struct AppState {
    documents: Arc<Documents>,
    grants: Arc<parking_lot::Mutex<Grants>>,
    key: Arc<Es256Key>,
    url: Url,
    lifetime: Duration,
    clock: Arc<dyn Clock>,
}

/// An in-process `OpenID` Connect issuer serving discovery, JWKS, and authorization endpoints.
pub struct TestIssuer {
    url: Url,
    token_lifetime: Duration,
    clock: Arc<dyn Clock>,
    key: Arc<Es256Key>,
    grants: Arc<parking_lot::Mutex<Grants>>,
    _server: Runtime,
}

impl TestIssuer {
    /// Binds the issuer to an ephemeral loopback port, generates its key, and starts serving.
    ///
    /// # Errors
    ///
    /// Returns an issuer error if binding, key generation, or runtime initialization fails.
    pub fn start(settings: &Settings) -> Result<Self> {
        Self::start_with_clock(settings, Arc::new(SystemClock::new()))
    }
    /// Starts the issuer using `clock` for token timestamps.
    ///
    /// # Errors
    ///
    /// Returns an issuer error if binding, key generation, or runtime initialization fails.
    pub fn start_with_clock(settings: &Settings, clock: Arc<dyn Clock>) -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").map_err(E2eError::issuer_from("binding 127.0.0.1"))?;
        listener
            .set_nonblocking(true)
            .map_err(E2eError::issuer_from("configuring the issuer's socket"))?;
        let address = listener
            .local_addr()
            .map_err(E2eError::issuer_from("reading the issuer's address"))?;
        let url = Url::parse(&format!("http://{address}"))
            .map_err(E2eError::issuer_from("forming the issuer url"))?;
        let key = Arc::new(
            generate_es256_key(KEY_ID)
                .map_err(E2eError::issuer_from("generating a signing key"))?,
        );
        let documents = Arc::new(Documents {
            discovery: json!({
                "issuer": base_url(&url),
                "jwks_uri": format!("{}/jwks", base_url(&url)),
                "authorization_endpoint": format!("{}/authorize", base_url(&url)),
                "token_endpoint": format!("{}/token", base_url(&url)),
            }),
            jwks: json!({"keys": [key.jwk()]}),
        });
        let grants = Arc::new(parking_lot::Mutex::new(Grants::default()));
        let state = AppState {
            documents: documents.clone(),
            grants: grants.clone(),
            key: key.clone(),
            url: url.clone(),
            lifetime: settings.token_lifetime,
            clock: clock.clone(),
        };
        let server = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("gang-e2e-issuer")
            .enable_io()
            .build()
            .map_err(E2eError::issuer_from("starting the issuer runtime"))?;
        let listener = {
            let _entered = server.enter();
            tokio::net::TcpListener::from_std(listener)
                .map_err(E2eError::issuer_from("registering the issuer's socket"))?
        };
        server.spawn(async move {
            let _ = axum::serve(listener, router(state)).await;
        });
        Ok(Self {
            url,
            token_lifetime: settings.token_lifetime,
            clock,
            key,
            grants,
            _server: server,
        })
    }
    /// The issuer URL without a trailing slash.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }
    /// Mints a valid ES256 token for `spec`.
    ///
    /// # Errors
    ///
    /// Returns an issuer error if signing fails.
    pub fn mint(&self, spec: &TokenSpec) -> Result<String> {
        let now = self.clock.wall().as_unix_millis() / 1000;
        self.sign_claims(
            &spec.subject,
            &spec.audience,
            &spec.organization,
            now,
            now + self.token_lifetime.as_secs(),
            None,
        )
    }
    /// Mints a token whose expiration is one lifetime before the current time.
    ///
    /// # Errors
    ///
    /// Returns an issuer error if signing fails.
    pub fn mint_expired(&self, spec: &TokenSpec) -> Result<String> {
        let now = self.clock.wall().as_unix_millis() / 1000;
        let lifetime = self.token_lifetime.as_secs();
        self.sign_claims(
            &spec.subject,
            &spec.audience,
            &spec.organization,
            now.saturating_sub(lifetime.saturating_mul(2)),
            now.saturating_sub(lifetime),
            None,
        )
    }
    /// Returns the number of successful refresh grants made with `refresh_token`.
    #[must_use]
    pub fn refresh_grants(&self, refresh_token: &str) -> usize {
        self.grants
            .lock()
            .refresh
            .get(refresh_token)
            .map_or(0, |grant| grant.grants)
    }
    /// Rejects later refresh attempts using `refresh_token`.
    pub fn reject_refresh_token(&self, refresh_token: &str) {
        if let Some(grant) = self.grants.lock().refresh.get_mut(refresh_token) {
            grant.rejected = true;
        }
    }
    /// Sets how the issuer treats `subject` from now on (default [`SubjectPolicy::Standard`]).
    pub fn set_policy(&self, subject: &str, policy: SubjectPolicy) {
        self.grants
            .lock()
            .policies
            .insert(subject.to_owned(), policy);
    }
    /// The authorization requests answered for `subject`, oldest first.
    #[must_use]
    pub fn authorizations(&self, subject: &str) -> Vec<AuthorizationRecord> {
        self.grants
            .lock()
            .authorizations
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }
    /// The refresh grants answered for `subject`, oldest first.
    #[must_use]
    pub fn refreshes(&self, subject: &str) -> Vec<RefreshRecord> {
        self.grants
            .lock()
            .refreshes
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }
    fn sign_claims(
        &self,
        subject: &str,
        audience: &str,
        organization: &str,
        iat: u64,
        exp: u64,
        nonce: Option<&str>,
    ) -> Result<String> {
        let claims = claims(
            base_url(&self.url),
            subject,
            audience,
            organization,
            iat,
            exp,
            nonce,
        );
        sign_jwt(&self.key, &claims)
    }
}

/// Signs the supplied claims with the issuer's key.
fn sign_jwt(key: &Es256Key, claims: &Value) -> Result<String> {
    key.sign(claims)
        .map_err(E2eError::issuer_from("signing a token"))
}

fn claims(
    issuer: &str,
    subject: &str,
    audience: &str,
    organization: &str,
    iat: u64,
    exp: u64,
    nonce: Option<&str>,
) -> Value {
    let mut value = json!({
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": iat,
        "nbf": iat,
        "exp": exp,
        "org": organization,
    });
    if let Some(nonce) = nonce {
        value["nonce"] = json!(nonce);
    }
    value
}
fn base_url(url: &Url) -> &str {
    url.as_str().trim_end_matches('/')
}
fn router(state: AppState) -> Router {
    Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/authorize", get(authorize))
        .route("/token", post(token))
        .with_state(state)
}
async fn discovery(State(state): State<AppState>) -> Json<Value> {
    Json(state.documents.discovery.clone())
}
async fn jwks(State(state): State<AppState>) -> Json<Value> {
    Json(state.documents.jwks.clone())
}
#[derive(Deserialize)]
struct Authorize {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    scope: Option<String>,
    state: Option<String>,
    nonce: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    login_hint: Option<String>,
    access_type: Option<String>,
    prompt: Option<String>,
}
async fn authorize(
    State(state): State<AppState>,
    Form(query): Form<Authorize>,
) -> std::result::Result<Response, IssuerHttpError> {
    let (
        Some("code"),
        Some(client),
        Some(redirect),
        Some(scope),
        Some(csrf),
        Some(nonce),
        Some(challenge),
        Some("S256"),
        Some(subject),
    ) = (
        query.response_type.as_deref(),
        query.client_id,
        query.redirect_uri,
        query.scope,
        query.state,
        query.nonce,
        query.code_challenge,
        query.code_challenge_method.as_deref(),
        query.login_hint,
    )
    else {
        return Err(IssuerHttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ));
    };
    if (client != AUDIENCE && client != DASHBOARD_CLIENT_ID)
        || !scope.split_ascii_whitespace().any(|s| s == "openid")
    {
        return Err(IssuerHttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ));
    }
    let Ok(mut target) = Url::parse(&redirect) else {
        return Err(IssuerHttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ));
    };
    let offline = query.access_type.as_deref() == Some(OFFLINE_ACCESS.1);
    let consent = query.prompt.as_deref() == Some(CONSENT_PROMPT.1);
    let mut grants = state.grants.lock();
    grants
        .authorizations
        .entry(subject.clone())
        .or_default()
        .push(AuthorizationRecord {
            client_id: client.clone(),
            offline,
            consent,
        });
    if grants.policies.get(&subject) == Some(&SubjectPolicy::Suspended) {
        target
            .query_pairs_mut()
            .append_pair("error", "access_denied")
            .append_pair("state", &csrf);
    } else {
        let code = random_value()?;
        grants.codes.insert(
            code.clone(),
            CodeGrant {
                client_id: client,
                redirect_uri: redirect,
                code_challenge: challenge,
                nonce,
                subject,
                offline,
            },
        );
        target
            .query_pairs_mut()
            .append_pair("code", &code)
            .append_pair("state", &csrf);
    }
    Ok((
        StatusCode::FOUND,
        [(axum::http::header::LOCATION, target.to_string())],
    )
        .into_response())
}
#[derive(Deserialize)]
struct TokenForm {
    grant_type: Option<String>,
    code: Option<String>,
    redirect_uri: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
}

/// The two clients accepted by the token endpoint.
#[derive(Clone, Copy)]
enum ClientId {
    /// The public command-line client.
    Cli,
    /// The confidential dashboard client.
    Dashboard,
}

impl ClientId {
    /// Authenticates a client identifier and its optional secret.
    fn parse(id: Option<&str>, secret: Option<&str>) -> std::result::Result<Self, IssuerHttpError> {
        match (id, secret) {
            (Some(AUDIENCE), None) => Ok(Self::Cli),
            (Some(DASHBOARD_CLIENT_ID), Some(DASHBOARD_CLIENT_SECRET)) => Ok(Self::Dashboard),
            _ => Err(IssuerHttpError::new(
                StatusCode::UNAUTHORIZED,
                "invalid_client",
            )),
        }
    }

    /// The identifier to include in grants and tokens.
    fn as_str(self) -> &'static str {
        match self {
            Self::Cli => AUDIENCE,
            Self::Dashboard => DASHBOARD_CLIENT_ID,
        }
    }
}
async fn token(
    State(state): State<AppState>,
    Form(form): Form<TokenForm>,
) -> std::result::Result<Response, IssuerHttpError> {
    let client =
        ClientId::parse(form.client_id.as_deref(), form.client_secret.as_deref())?.as_str();
    match form.grant_type.as_deref() {
        Some("authorization_code") => {
            let (Some(code), Some(redirect), Some(verifier)) =
                (form.code, form.redirect_uri, form.code_verifier)
            else {
                return Err(IssuerHttpError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_grant",
                ));
            };
            let Some(grant) = state.grants.lock().codes.remove(&code) else {
                return Err(IssuerHttpError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_grant",
                ));
            };
            let digest = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
            if grant.client_id != client
                || grant.redirect_uri != redirect
                || grant.code_challenge != digest
            {
                return Err(IssuerHttpError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_grant",
                ));
            }
            let now = state.clock.wall().as_unix_millis() / 1000;
            let id_token = signed(&state, &grant.subject, client, now, Some(&grant.nonce))?;
            let access = random_value()?;
            let mut response = json!({
                "token_type": "Bearer",
                "access_token": access,
                "id_token": id_token,
            });
            let mut grants = state.grants.lock();
            if grant.offline
                && grants.policies.get(&grant.subject) != Some(&SubjectPolicy::WithoutRefreshTokens)
            {
                let refresh = random_value()?;
                let family = grants.next_family;
                grants.next_family += 1;
                grants.refresh.insert(
                    refresh.clone(),
                    RefreshGrant {
                        client_id: client.to_owned(),
                        subject: grant.subject,
                        grants: 0,
                        rejected: false,
                        family,
                        retired: false,
                    },
                );
                response["refresh_token"] = json!(refresh);
            }
            Ok(Json(response).into_response())
        }
        Some("refresh_token") => {
            let Some(refresh) = form.refresh_token else {
                return Err(IssuerHttpError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_grant",
                ));
            };
            refresh_token(&state, client, refresh)
        }
        _ => Err(IssuerHttpError::new(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
        )),
    }
}

/// Answers a refresh grant and records its outcome atomically with token-family changes.
fn refresh_token(
    state: &AppState,
    client: &str,
    refresh: String,
) -> std::result::Result<Response, IssuerHttpError> {
    let mut grants = state.grants.lock();
    let Some(grant) = grants.refresh.get(&refresh) else {
        return Err(IssuerHttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
        ));
    };
    let subject = grant.subject.clone();
    let family = grant.family;
    let policy = grants.policies.get(&subject).copied().unwrap_or_default();
    let outcome = if grant.rejected
        || grant.client_id != client
        || policy == SubjectPolicy::Suspended
        || grants.revoked_families.contains(&family)
    {
        RefreshOutcome::Refused
    } else if policy == SubjectPolicy::Rotating && grant.retired {
        grants.revoked_families.insert(family);
        RefreshOutcome::ReuseDetected
    } else {
        let now = state.clock.wall().as_unix_millis() / 1000;
        let id_token = signed(state, &subject, client, now, None)?;
        let access = random_value()?;
        let issued = if policy == SubjectPolicy::Rotating {
            Some(random_value()?)
        } else {
            None
        };
        if let Some(grant) = grants.refresh.get_mut(&refresh) {
            grant.grants += 1;
            if issued.is_some() {
                grant.retired = true;
            }
        }
        let mut response =
            json!({"token_type":"Bearer", "access_token":access, "id_token":id_token});
        let outcome = if let Some(token) = issued {
            grants.refresh.insert(
                token.clone(),
                RefreshGrant {
                    client_id: client.to_owned(),
                    subject: subject.clone(),
                    grants: 0,
                    rejected: false,
                    family,
                    retired: false,
                },
            );
            response["refresh_token"] = json!(token);
            RefreshOutcome::Rotated { issued: token }
        } else {
            RefreshOutcome::Granted
        };
        grants
            .refreshes
            .entry(subject)
            .or_default()
            .push(RefreshRecord {
                client_id: client.to_owned(),
                refresh_token: refresh,
                outcome,
            });
        return Ok(Json(response).into_response());
    };
    grants
        .refreshes
        .entry(subject)
        .or_default()
        .push(RefreshRecord {
            client_id: client.to_owned(),
            refresh_token: refresh,
            outcome,
        });
    Err(IssuerHttpError::new(
        StatusCode::BAD_REQUEST,
        "invalid_grant",
    ))
}
fn signed(
    state: &AppState,
    subject: &str,
    client: &str,
    now: u64,
    nonce: Option<&str>,
) -> Result<String> {
    sign_jwt(
        &state.key,
        &claims(
            base_url(&state.url),
            subject,
            client,
            ORGANIZATION,
            now,
            now + state.lifetime.as_secs(),
            nonce,
        ),
    )
}
fn random_value() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(E2eError::issuer_from("generating a random value"))?;
    Ok(hex::encode(bytes))
}
/// An OAuth error returned by the issuer's HTTP endpoints.
struct IssuerHttpError {
    status: StatusCode,
    code: &'static str,
}

impl IssuerHttpError {
    /// Constructs an OAuth error response.
    fn new(status: StatusCode, code: &'static str) -> Self {
        Self { status, code }
    }
}

impl From<E2eError> for IssuerHttpError {
    fn from(_: E2eError) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "server_error")
    }
}

impl IntoResponse for IssuerHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error":self.code}))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_redirect_creates_no_code_grant() -> Result<()> {
        let settings = Settings::default();
        let issuer = TestIssuer::start(&settings)?;
        let state = AppState {
            documents: Arc::new(Documents {
                discovery: json!({}),
                jwks: json!({}),
            }),
            grants: issuer.grants.clone(),
            key: issuer.key.clone(),
            url: issuer.url.clone(),
            lifetime: settings.token_lifetime,
            clock: issuer.clock.clone(),
        };
        let query = Authorize {
            response_type: Some("code".into()),
            client_id: Some(AUDIENCE.into()),
            redirect_uri: Some("not a url".into()),
            scope: Some("openid".into()),
            state: Some("state".into()),
            nonce: Some("nonce".into()),
            code_challenge: Some("challenge".into()),
            code_challenge_method: Some("S256".into()),
            login_hint: Some("subject".into()),
            access_type: None,
            prompt: None,
        };
        let runtime = Builder::new_current_thread()
            .build()
            .map_err(E2eError::issuer_from("starting the test runtime"))?;
        let response = runtime
            .block_on(authorize(State(state), Form(query)))
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(issuer.grants.lock().codes.is_empty());
        Ok(())
    }
}
