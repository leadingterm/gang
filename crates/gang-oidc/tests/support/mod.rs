//! In-process `OpenID` Connect issuer shared by the `gang-oidc` integration tests.
//!
//! The issuer serves discovery, JWKS and the token endpoint on a
//! loopback port. Authorization is simulated: the test approves or denies an
//! authorization URL directly, and the issuer records the grant for the token
//! endpoint.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::{Form, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_oidc::{
    IssuerUrl, LoginConfig, OfflineLogin, OidcCache, OidcConfig, OidcHttp, OidcTokens,
};
use gang_spi::LoginClient;
use gang_testkit::generate_es256_key;
use gang_types::{Audience, ClientSecret, Es256Key, NonZeroDuration, Url};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const AUDIENCE: &str = "gang-test";
const CLIENT_SECRET: &str = "secret";
/// Client id of the second registered client, which browser logins run under.
pub const WEB_CLIENT_ID: &str = "gang-web-test";
/// Secret of the second registered client.
pub const WEB_CLIENT_SECRET: &str = "web-secret";
/// Every client the issuer authenticates at the token endpoint.
const CLIENTS: [(&str, &str); 2] = [
    (AUDIENCE, CLIENT_SECRET),
    (WEB_CLIENT_ID, WEB_CLIENT_SECRET),
];

/// Returns the web client as a login override, the way the dashboard logs in with it.
pub fn web_client() -> LoginClient {
    LoginClient {
        id: WEB_CLIENT_ID.to_owned(),
        secret: ClientSecret::new(WEB_CLIENT_SECRET),
    }
}

/// How the issuer answers refresh grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefreshMode {
    /// Refresh returns an identity token; the refresh token stays valid and is not rotated.
    #[default]
    IdToken,
    /// Refresh returns an identity token and a new refresh token; the presented one is invalidated.
    IdTokenRotating,
    /// Refresh returns no identity token; the refresh token stays valid and the grant fails.
    NoIdToken,
}

/// An in-process `OpenID` Connect issuer bound to `127.0.0.1` on an ephemeral port.
pub struct TestIssuer<'clock> {
    clock: &'clock TestClock,
    url: Url,
    shared: Arc<Shared>,
}

struct Shared {
    registry: Mutex<Registry>,
    jwks_fetches: AtomicUsize,
    discovery_fetches: AtomicUsize,
    discovery_failing: AtomicBool,
    /// While set, discovery answers `500` with a body larger than any test cap.
    discovery_oversized: AtomicBool,
    token_requests: AtomicUsize,
    /// While set, discovery requests are held until released; see `set_discovery_held`.
    discovery_held: AtomicBool,
    /// Signalled by a held discovery request as it arrives.
    discovery_arrived: Notify,
    /// Signalled by the test to let one held discovery request answer.
    discovery_release: Notify,
    jwks_failing: AtomicBool,
    /// While set, JWKS requests are held until released; see `set_jwks_held`.
    jwks_held: AtomicBool,
    /// Signalled by a held JWKS request as it arrives.
    jwks_arrived: Notify,
    /// Signalled by the test to let one held JWKS request answer.
    jwks_release: Notify,
    /// Issuer identifier the discovery document advertises instead of the real one, if set.
    discovery_issuer: Mutex<Option<String>>,
    /// JWKS URI advertised instead of the real endpoint, if set.
    discovery_jwks_uri: Mutex<Option<String>>,
    /// Authorization endpoint advertised instead of the real endpoint, if set.
    discovery_authorization_endpoint: Mutex<Option<String>>,
    /// Raw JWKs published after the issuer's own keys; see `publish_jwk`.
    extra_jwks: Mutex<Vec<Value>>,
}

struct Registry {
    /// Issuer identifier without a trailing slash.
    issuer: String,
    /// Active signing keys, oldest first; the last key is current.
    keys: Vec<Es256Key>,
    codes: HashMap<String, PendingCode>,
    refresh_tokens: HashMap<String, Map<String, Value>>,
    refresh_mode: RefreshMode,
    counter: u64,
    /// Client ids that exchanged an authorization code, oldest first.
    exchanged_by: Vec<String>,
    /// Client ids that renewed a refresh token, oldest first.
    refreshed_by: Vec<String>,
}

impl Registry {
    fn fresh(&mut self, prefix: &str) -> String {
        self.counter += 1;
        format!("{prefix}-{}", self.counter)
    }

    fn add_key(&mut self) {
        let kid = self.fresh("key");
        self.keys
            .push(generate_es256_key(kid).expect("generating a signing key"));
    }

    fn current_key(&self) -> &Es256Key {
        self.keys
            .last()
            .expect("the issuer always holds a signing key")
    }

    fn issue_refresh(&mut self, claims: Map<String, Value>) -> String {
        let token = self.fresh("refresh");
        self.refresh_tokens.insert(token.clone(), claims);
        token
    }
}

struct PendingCode {
    challenge: String,
    redirect_uri: String,
    claims: Map<String, Value>,
    /// The client the code was issued to; its identity token carries this id as `aud`.
    client_id: String,
}

impl<'clock> TestIssuer<'clock> {
    /// Starts the issuer on `127.0.0.1` with an ephemeral port and one signing key.
    ///
    /// Token timestamps are read from `clock`, so tests control expiry by advancing it.
    pub async fn start(clock: &'clock TestClock) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the test issuer to loopback");
        let address = listener
            .local_addr()
            .expect("the test issuer has a local address");
        let url = Url::parse(&format!("http://{address}"))
            .expect("a loopback socket address forms a valid url");

        let mut registry = Registry {
            issuer: url.as_str().trim_end_matches('/').to_owned(),
            keys: Vec::new(),
            codes: HashMap::new(),
            refresh_tokens: HashMap::new(),
            refresh_mode: RefreshMode::default(),
            counter: 0,
            exchanged_by: Vec::new(),
            refreshed_by: Vec::new(),
        };
        registry.add_key();
        let shared = Arc::new(Shared {
            registry: Mutex::new(registry),
            jwks_fetches: AtomicUsize::new(0),
            discovery_fetches: AtomicUsize::new(0),
            discovery_failing: AtomicBool::new(false),
            discovery_oversized: AtomicBool::new(false),
            token_requests: AtomicUsize::new(0),
            discovery_held: AtomicBool::new(false),
            discovery_arrived: Notify::new(),
            discovery_release: Notify::new(),
            jwks_failing: AtomicBool::new(false),
            jwks_held: AtomicBool::new(false),
            jwks_arrived: Notify::new(),
            jwks_release: Notify::new(),
            discovery_issuer: Mutex::new(None),
            discovery_jwks_uri: Mutex::new(None),
            discovery_authorization_endpoint: Mutex::new(None),
            extra_jwks: Mutex::new(Vec::new()),
        });

        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/token", post(token))
            .with_state(Arc::clone(&shared));
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serving the test issuer");
        });

        Self { clock, url, shared }
    }

    /// Returns the issuer URL, which is also the `iss` claim of every token.
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    /// Returns a configuration for this issuer without a login table.
    pub fn config(&self) -> OidcConfig {
        OidcConfig {
            issuer_url: IssuerUrl::try_from(self.url()).expect("test issuer is valid"),
            audience: Audience::try_new(AUDIENCE).expect("test audience is valid"),
            additional_audiences: Vec::new(),
            principal_claim: "email".to_owned(),
            required_claims: BTreeMap::new(),
            login: None,
            cache: OidcCache::default(),
            // This bound is far beyond any test and never decides a verdict.
            http: OidcHttp {
                timeout: NonZeroDuration::from_hours(24),
                ..OidcHttp::default()
            },
            tokens: OidcTokens::default(),
        }
    }

    /// Returns a configuration for this issuer with a login table.
    pub fn login_config(&self) -> OidcConfig {
        OidcConfig {
            login: Some(LoginConfig {
                client_secret: Some(ClientSecret::new(CLIENT_SECRET)),
                scopes: vec!["openid".to_owned(), "email".to_owned()],
                authorization_params: BTreeMap::from([("prompt".to_owned(), "consent".to_owned())]),
                offline: OfflineLogin::default(),
            }),
            ..self.config()
        }
    }

    /// Returns a configuration for this issuer with a login table that also accepts the
    /// identity tokens of the web client.
    pub fn config_accepting_web_client(&self) -> OidcConfig {
        OidcConfig {
            additional_audiences: vec![
                Audience::try_new(WEB_CLIENT_ID).expect("web audience is valid"),
            ],
            ..self.login_config()
        }
    }

    /// Returns the client ids the issuer exchanged authorization codes for, oldest first.
    pub fn code_exchanges(&self) -> Vec<String> {
        self.shared.registry.lock().exchanged_by.clone()
    }

    /// Returns the client ids that renewed a refresh token, oldest first.
    pub fn refresh_clients(&self) -> Vec<String> {
        self.shared.registry.lock().refreshed_by.clone()
    }

    /// Returns claims for a valid identity whose `iat` is now and whose `exp` is one hour away.
    pub fn valid_claims(&self) -> Value {
        let now = self.clock.wall();
        json!({
            "iss": self.shared.registry.lock().issuer.clone(),
            "aud": AUDIENCE,
            "sub": "subject-1",
            "email": "dev@example.test",
            "email_verified": true,
            "iat": unix_seconds(now),
            "exp": unix_seconds(now + Duration::from_hours(1)),
        })
    }

    /// Signs `claims` with the current key, ES256, carrying its `kid`.
    pub fn mint(&self, claims: &Value) -> String {
        self.shared
            .registry
            .lock()
            .current_key()
            .sign(claims)
            .expect("signing a test token with the current key")
    }

    /// Adds a new signing key, makes it current, and keeps the previous keys published.
    pub fn rotate(&self) {
        self.shared.registry.lock().add_key();
    }

    /// Removes every signing key except the current one.
    pub fn retire_older_keys(&self) {
        let mut registry = self.shared.registry.lock();
        let current = registry
            .keys
            .pop()
            .expect("the issuer always holds a signing key");
        registry.keys.clear();
        registry.keys.push(current);
    }

    /// Returns how many times the JWKS endpoint has been fetched.
    pub fn jwks_fetches(&self) -> usize {
        self.shared.jwks_fetches.load(Ordering::SeqCst)
    }

    /// Returns how many times the discovery document has been fetched.
    pub fn discovery_fetches(&self) -> usize {
        self.shared.discovery_fetches.load(Ordering::SeqCst)
    }

    /// Makes the discovery endpoint answer `500` from now on, or serve the document again.
    pub fn set_discovery_failing(&self, failing: bool) {
        self.shared
            .discovery_failing
            .store(failing, Ordering::SeqCst);
    }

    /// Makes the discovery document advertise `issuer` as its issuer instead of the real one,
    /// or advertise the real one again when `issuer` is `None`.
    pub fn set_discovery_issuer(&self, issuer: Option<String>) {
        *self.shared.discovery_issuer.lock() = issuer;
    }

    /// Overrides the advertised JWKS URI, or restores the real endpoint with `None`.
    pub fn set_discovery_jwks_uri(&self, endpoint: Option<String>) {
        *self.shared.discovery_jwks_uri.lock() = endpoint;
    }

    /// Overrides the advertised authorization endpoint, or restores it with `None`.
    pub fn set_discovery_authorization_endpoint(&self, endpoint: Option<String>) {
        *self.shared.discovery_authorization_endpoint.lock() = endpoint;
    }

    /// Makes the discovery endpoint answer `500` with an oversized body from now on, or serve
    /// the document again.
    pub fn set_discovery_oversized_failure(&self, oversized: bool) {
        self.shared
            .discovery_oversized
            .store(oversized, Ordering::SeqCst);
    }

    /// Makes the discovery endpoint hold each request until `release_discovery` is called,
    /// or stops holding requests from now on. Held requests are still counted on arrival.
    pub fn set_discovery_held(&self, held: bool) {
        self.shared.discovery_held.store(held, Ordering::SeqCst);
    }

    /// Lets one held discovery request answer. Call only while a held request is in flight
    /// or will arrive, so no permit is left over for a later request.
    pub fn release_discovery(&self) {
        self.shared.discovery_release.notify_one();
    }

    /// Resolves once a held discovery request has reached the issuer.
    pub async fn wait_for_held_discovery(&self) {
        self.shared.discovery_arrived.notified().await;
    }

    /// Makes the JWKS endpoint answer `500` from now on, or serve the key set again.
    pub fn set_jwks_failing(&self, failing: bool) {
        self.shared.jwks_failing.store(failing, Ordering::SeqCst);
    }

    /// Makes the JWKS endpoint hold each request until `release_jwks` is called,
    /// or stops holding requests from now on. Held requests are still counted on arrival.
    pub fn set_jwks_held(&self, held: bool) {
        self.shared.jwks_held.store(held, Ordering::SeqCst);
    }

    /// Lets one held JWKS request answer. Call only while a held request is in flight
    /// or will arrive, so no permit is left over for a later request.
    pub fn release_jwks(&self) {
        self.shared.jwks_release.notify_one();
    }

    /// Resolves once a held JWKS request has reached the issuer.
    pub async fn wait_for_held_jwks(&self) {
        self.shared.jwks_arrived.notified().await;
    }

    /// Publishes `jwk` in the key set the JWKS endpoint serves, after the issuer's own keys.
    pub fn publish_jwk(&self, jwk: Value) {
        self.shared.extra_jwks.lock().push(jwk);
    }

    /// Returns how many requests have reached the token endpoint.
    pub fn token_requests(&self) -> usize {
        self.shared.token_requests.load(Ordering::SeqCst)
    }

    /// Selects how refresh grants are answered from now on.
    pub fn set_refresh_mode(&self, mode: RefreshMode) {
        self.shared.registry.lock().refresh_mode = mode;
    }

    /// Simulates the user approving an authorization request.
    ///
    /// Returns the callback URL carrying a fresh code and the request's state.
    /// The identity token issued for the code carries `claims` and the request's nonce.
    pub fn approve(&self, authorization_url: &Url, claims: &Value) -> Url {
        self.record_approval(authorization_url, claims, str::to_owned)
    }

    /// Simulates an approval whose identity token carries a nonce different from the request's.
    pub fn approve_with_wrong_nonce(&self, authorization_url: &Url, claims: &Value) -> Url {
        self.record_approval(authorization_url, claims, |nonce| {
            format!("{nonce}-tampered")
        })
    }

    /// Simulates the user denying an authorization request.
    ///
    /// Returns the callback URL with `error=access_denied` and the request's state.
    pub fn deny(authorization_url: &Url) -> Url {
        let query = query_parameters(authorization_url);
        let redirect_uri = parameter(&query, "redirect_uri");
        let state = parameter(&query, "state");
        callback(
            &redirect_uri,
            &[("error", "access_denied"), ("state", state.as_str())],
        )
    }

    fn record_approval(
        &self,
        authorization_url: &Url,
        claims: &Value,
        token_nonce: impl FnOnce(&str) -> String,
    ) -> Url {
        let query = query_parameters(authorization_url);
        assert_eq!(
            query.get("response_type").map(String::as_str),
            Some("code"),
            "the authorization request asks for a code"
        );
        assert_eq!(
            query.get("code_challenge_method").map(String::as_str),
            Some("S256"),
            "the authorization request uses S256"
        );
        let redirect_uri = parameter(&query, "redirect_uri");
        let state = parameter(&query, "state");
        let nonce = parameter(&query, "nonce");
        let client_id = parameter(&query, "client_id");
        let mut claims = claims
            .as_object()
            .cloned()
            .expect("approved claims are a JSON object");
        claims.insert("nonce".to_owned(), Value::String(token_nonce(&nonce)));
        claims.insert("aud".to_owned(), Value::String(client_id.clone()));

        let mut registry = self.shared.registry.lock();
        let code = registry.fresh("code");
        registry.codes.insert(
            code.clone(),
            PendingCode {
                challenge: parameter(&query, "code_challenge"),
                redirect_uri: redirect_uri.clone(),
                client_id,
                claims,
            },
        );
        callback(
            &redirect_uri,
            &[("code", code.as_str()), ("state", state.as_str())],
        )
    }
}

fn unix_seconds(at: Timestamp) -> u64 {
    at.as_unix_millis() / 1000
}

fn query_parameters(url: &Url) -> HashMap<String, String> {
    url.query_pairs().into_owned().collect()
}

fn parameter(query: &HashMap<String, String>, name: &str) -> String {
    query
        .get(name)
        .cloned()
        .unwrap_or_else(|| panic!("the authorization url carries `{name}`"))
}

fn callback(redirect_uri: &str, params: &[(&str, &str)]) -> Url {
    let mut url = Url::parse(redirect_uri).expect("the registered redirect uri is absolute");
    {
        let mut pairs = url.query_pairs_mut();
        for (name, value) in params {
            pairs.append_pair(name, value);
        }
    }
    url
}

fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn oauth_error(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

/// Whether `form` authenticates as `client_id` with that client's registered secret.
fn client_authenticated(form: &HashMap<String, String>, client_id: &str) -> bool {
    CLIENTS.iter().any(|&(id, secret)| {
        id == client_id
            && form.get("client_id").map(String::as_str) == Some(id)
            && form.get("client_secret").map(String::as_str) == Some(secret)
    })
}

async fn discovery(State(shared): State<Arc<Shared>>) -> Response {
    shared.discovery_fetches.fetch_add(1, Ordering::SeqCst);
    if shared.discovery_held.load(Ordering::SeqCst) {
        shared.discovery_arrived.notify_one();
        shared.discovery_release.notified().await;
    }
    if shared.discovery_oversized.load(Ordering::SeqCst) {
        return (StatusCode::INTERNAL_SERVER_ERROR, "x".repeat(4096)).into_response();
    }
    if shared.discovery_failing.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let issuer = shared.registry.lock().issuer.clone();
    let advertised = shared
        .discovery_issuer
        .lock()
        .clone()
        .unwrap_or_else(|| issuer.clone());
    Json(json!({
        "issuer": advertised,
        "jwks_uri": shared.discovery_jwks_uri.lock().clone().unwrap_or_else(|| format!("{issuer}/jwks")),
        "authorization_endpoint": shared.discovery_authorization_endpoint.lock().clone().unwrap_or_else(|| format!("{issuer}/authorize")),
        "token_endpoint": format!("{issuer}/token"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["ES256"],
    }))
    .into_response()
}

async fn jwks(State(shared): State<Arc<Shared>>) -> Response {
    shared.jwks_fetches.fetch_add(1, Ordering::SeqCst);
    if shared.jwks_held.load(Ordering::SeqCst) {
        shared.jwks_arrived.notify_one();
        shared.jwks_release.notified().await;
    }
    if shared.jwks_failing.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let mut keys: Vec<Value> = shared
        .registry
        .lock()
        .keys
        .iter()
        .map(|key| json!(key.jwk()))
        .collect();
    keys.extend(shared.extra_jwks.lock().iter().cloned());
    Json(json!({ "keys": keys })).into_response()
}

async fn token(
    State(shared): State<Arc<Shared>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    shared.token_requests.fetch_add(1, Ordering::SeqCst);
    let mut registry = shared.registry.lock();
    match form.get("grant_type").map(String::as_str) {
        Some("authorization_code") => exchange_code(&mut registry, &form),
        Some("refresh_token") => refresh(&mut registry, &form),
        _ => oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
    }
}

fn exchange_code(registry: &mut Registry, form: &HashMap<String, String>) -> Response {
    let Some(pending) = form
        .get("code")
        .and_then(|code| registry.codes.remove(code))
    else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };
    let redirect_matches = form.get("redirect_uri") == Some(&pending.redirect_uri);
    let verifier_matches = form
        .get("code_verifier")
        .is_some_and(|verifier| challenge(verifier) == pending.challenge);
    if !redirect_matches || !verifier_matches {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }
    if !client_authenticated(form, &pending.client_id) {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    }
    registry.exchanged_by.push(pending.client_id);
    issue_tokens(registry, pending.claims, true, true)
}

fn refresh(registry: &mut Registry, form: &HashMap<String, String>) -> Response {
    let client_id = form.get("client_id").map_or(AUDIENCE, String::as_str);
    if !client_authenticated(form, client_id) {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    }
    registry.refreshed_by.push(client_id.to_owned());
    let Some(presented) = form.get("refresh_token") else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };
    let rotating = registry.refresh_mode == RefreshMode::IdTokenRotating;
    let found = if rotating {
        registry.refresh_tokens.remove(presented)
    } else {
        registry.refresh_tokens.get(presented).cloned()
    };
    let Some(mut claims) = found else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };
    claims.remove("nonce");
    let with_id_token = registry.refresh_mode != RefreshMode::NoIdToken;
    issue_tokens(registry, claims, with_id_token, rotating)
}

/// Builds a token response for `claims`. `with_id_token` adds the identity token and
/// `with_refresh` adds a newly registered refresh token.
fn issue_tokens(
    registry: &mut Registry,
    claims: Map<String, Value>,
    with_id_token: bool,
    with_refresh: bool,
) -> Response {
    let mut body = Map::new();
    if with_id_token {
        let id_token = registry
            .current_key()
            .sign(&claims)
            .expect("signing a test token with the current key");
        body.insert("id_token".to_owned(), Value::String(id_token));
    }
    if with_refresh {
        let refresh_token = registry.issue_refresh(claims);
        body.insert("refresh_token".to_owned(), Value::String(refresh_token));
    }
    body.insert("token_type".to_owned(), Value::String("Bearer".to_owned()));
    (StatusCode::OK, Json(body)).into_response()
}
