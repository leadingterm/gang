//! Shared fakes for the `gang-client` integration tests: an in-process Gang server (plaintext, or
//! TLS with the fixture CA) answering the server, authenticate, revoke and restore endpoints,
//! identity providers that mint JWT-shaped proofs, a scripted console and a shared test clock.

use std::net::{IpAddr, Ipv4Addr};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use gang_client::{Client, ClientHome, ClientOptions, Console, LoginProviders};
use gang_clock::{Clock, TestClock, Timestamp};
use gang_mtls::{CertificateAuthority, KeyPair, ServerCertificateRequest, SubjectName};
use gang_spi::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport, PortError,
    Rejection,
};
use gang_testkit::{TestSigner, ready_now};
use gang_tls::{
    AcceptorSettings, Alpn, ClientAuth, RevocationSet, TlsAcceptor, ca_pin_of_certificate,
};
use gang_types::{
    AccessToken, CaPin, CertSerial, CertificatePem, Identity, LoginSession, NonZeroDuration,
    PrivateKeyPem, Proof, ProviderName, RefreshToken, TrustDomain, Url,
};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Authentication, Certificate,
    CreateCertificateRequest, ErrorReason, ErrorResponse, Isa, NoAuthentication,
    RevokePrincipalResponse, Server, Status, TokenAuthentication, name, routes,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

/// The certificates the fake server serves, issued once per process at runtime.
struct Fixture {
    /// The fake Gang CA certificate; every served server info carries it.
    ca: CertificatePem,
    /// The server certificate for `127.0.0.1`, signed by `ca`.
    server_certificate: CertificatePem,
    /// The private key of `server_certificate`.
    server_key: PrivateKeyPem,
}

static FIXTURE: LazyLock<Fixture> = LazyLock::new(|| {
    ready_now(async {
        let trust_domain =
            TrustDomain::try_new("gang.example").expect("fixture trust domain is valid");
        let authority = CertificateAuthority::establish(
            TestSigner::authority().expect("the authority seed is a valid P-256 scalar"),
            Arc::new(TestClock::new(START)),
            trust_domain,
            Duration::from_mins(1),
        )
        .await
        .expect("establishing the fixture certificate authority");
        let server_key = KeyPair::generate().expect("generating the fixture server key");
        let csr = server_key
            .certificate_request()
            .expect("the fixture server key yields a certificate request");
        let names = [SubjectName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST))];
        let issued = authority
            .issue_server(&ServerCertificateRequest {
                csr: &csr,
                names: &names,
                lifetime: NonZeroDuration::from_hours(87_600),
            })
            .await
            .expect("issuing the fixture server certificate");
        Fixture {
            ca: authority.certificate().clone(),
            server_certificate: issued.certificate,
            server_key: server_key.private_key().clone(),
        }
    })
    .expect("the fixture authority only awaits its in-memory signer, which is always ready")
});

/// Returns the fixture certificates, generated on first use.
///
/// The authority runs on the same clock every client verifies against, at [`START`], so the
/// certificates it issues are valid whenever a test client reads the time. The CA certificate
/// never expires, so it stays valid whatever the test clock reads.
fn fixture() -> &'static Fixture {
    &FIXTURE
}

/// The clock every fake starts from: 2024-01-01T00:00:00Z.
pub const START: Timestamp = Timestamp::from_unix_millis(1_704_067_200_000);

/// Lifetime of a bearer token the fake server issues unless a test changes it: ten minutes.
const DEFAULT_TOKEN_TTL: Duration = Duration::from_mins(10);

/// Lifetime of a certificate in the fake server's answers: one day.
const CERTIFICATE_LIFETIME: Duration = Duration::from_hours(24);

/// Lifetime of the `exp` claim of the proofs the fake providers mint: one hour.
const PROOF_LIFETIME: Duration = Duration::from_hours(1);

/// The longest a fake-server TLS handshake may take; long enough that no test's own timeout
/// is ever preceded by it.
const HANDSHAKE_TIMEOUT: NonZeroDuration = NonZeroDuration::from_hours(1);

/// How the fake console answers a login.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleMode {
    /// The browser opens and is redirected to the client's loopback listener.
    Browser,
    /// The user pastes a correct redirect URL into the terminal.
    Paste,
    /// Nothing returns to the client; the login waits until it times out.
    Silent,
    /// The user pastes a redirect URL carrying a forged state.
    WrongState,
    /// The browser's callback first arrives with an oversized request head; the client answers
    /// that head itself, then the browser's normal callback follows.
    OversizedHead,
}

/// The server document offering token authentication with the given interactive providers, each
/// with its provider-specific configuration. `with_ca` attaches the fixture CA as the server's CA.
#[must_use]
pub fn token_info(
    providers: &[(&str, serde_json::Value)],
    with_ca: bool,
) -> Server<serde_json::Value> {
    Server {
        name: name::SERVER.to_owned(),
        remote_executor: Url::parse("grpcs://exec.example:443").expect("executor url parses"),
        remote_cache: Url::parse("grpcs://cache.example:443").expect("cache url parses"),
        ca_certificate: with_ca.then(|| fixture().ca.clone()),
        default_isa: Isa::X86_64,
        authentication: Authentication::Token(TokenAuthentication {
            login: providers
                .iter()
                .map(|(provider, config)| {
                    (
                        ProviderName::try_new(*provider).expect("provider name is valid"),
                        config.clone(),
                    )
                })
                .collect(),
        }),
    }
}

/// The server document with no authentication and no CA.
#[must_use]
pub fn no_auth_info() -> Server<serde_json::Value> {
    Server {
        name: name::SERVER.to_owned(),
        remote_executor: Url::parse("grpcs://exec.example:443").expect("executor url parses"),
        remote_cache: Url::parse("grpcs://cache.example:443").expect("cache url parses"),
        ca_certificate: None,
        default_isa: Isa::X86_64,
        authentication: Authentication::None(NoAuthentication {}),
    }
}

/// The server document offering only the interactive `google` provider, with the fixture CA.
#[must_use]
pub fn google() -> Server<serde_json::Value> {
    token_info(&[("google", serde_json::json!({}))], true)
}

/// A response every request is answered with, set by [`FakeServer::force_response_with_headers`].
struct ForcedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

struct ServerState {
    info: Server<serde_json::Value>,
    clock: Arc<TestClock>,
    /// `(provider, proof)` pairs the token endpoint accepts.
    accepted: Vec<(ProviderName, String)>,
    /// Every token request received, accepted or not.
    token_requests: Vec<(ProviderName, String)>,
    token_ttl: Duration,
    tokens_issued: usize,
    /// Issued bearer tokens with their expiry on the shared clock.
    tokens: Vec<(String, Timestamp)>,
    /// `(path, bearer)` for every request to a bearer-authenticated endpoint.
    calls: Vec<(String, Option<String>)>,
    /// `(method, path, bearer, request)` for every certificate request received.
    certificate_requests: Vec<(String, String, Option<String>, CreateCertificateRequest)>,
    revoked_certificates: Vec<CertSerial>,
    revoked_principals: Vec<Identity>,
    restored_principals: Vec<Identity>,
    forced: Option<ForcedResponse>,
}

type SharedServerState = Arc<Mutex<ServerState>>;

/// An in-process Gang server answering the server, authenticate, revoke and restore endpoints.
pub struct FakeServer {
    url: Url,
    state: SharedServerState,
}

impl FakeServer {
    /// Starts the server over plaintext HTTP on `127.0.0.1` at an ephemeral port, for the lifetime
    /// of the test. Its URL is `http://127.0.0.1:{port}/`.
    pub async fn start_plain(info: Server<serde_json::Value>, clock: Arc<TestClock>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the fake server");
        let address = listener
            .local_addr()
            .expect("fake server has a local address");
        let url = Url::parse(&format!("http://{address}/")).expect("loopback url parses");
        let state = new_state(info, clock);
        let app = router(Arc::clone(&state));
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener
                    .accept()
                    .await
                    .expect("accepting fake server connections");
                let app = app.clone();
                tokio::spawn(async move {
                    // HTTP/2 only: a client that opens with HTTP/1.1 gets no answer, so the
                    // tests prove the client speaks HTTP/2 from the first byte.
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .http2_only()
                        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app))
                        .await;
                });
            }
        });
        Self { url, state }
    }

    /// Starts the server over TLS on `127.0.0.1` at an ephemeral port, presenting the fixture
    /// certificate, which the fixture CA signs. Its URL is `https://127.0.0.1:{port}/`; trust
    /// it with [`FakeServer::ca_pin`].
    pub async fn start_tls(info: Server<serde_json::Value>, clock: Arc<TestClock>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the fake server");
        let address = listener
            .local_addr()
            .expect("fake server has a local address");
        let url = Url::parse(&format!("https://{address}/")).expect("loopback url parses");
        let fixture = fixture();
        let acceptor = TlsAcceptor::try_new(&AcceptorSettings {
            certificate: &fixture.server_certificate,
            private_key: &fixture.server_key,
            client_ca: &fixture.ca,
            revocations: &RevocationSet::default(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            client_auth: ClientAuth::None,
            alpn: Alpn::H2AndHttp1,
            clock: clock.clone(),
        })
        .expect("the fixture server certificate matches its key");
        let state = new_state(info, clock);
        let app = router(Arc::clone(&state));
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener
                    .accept()
                    .await
                    .expect("accepting fake server connections");
                let app = app.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app))
                        .await;
                });
            }
        });
        Self { url, state }
    }

    /// Returns the server's base URL.
    #[must_use]
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    /// Accepts `proof` from `provider` on later token requests.
    pub fn accept_proof(&self, provider: &str, proof: &str) {
        let provider = ProviderName::try_new(provider).expect("provider name is valid");
        self.state
            .lock()
            .accepted
            .push((provider, proof.to_owned()));
    }

    /// Returns every token request received so far, accepted or rejected, in order.
    #[must_use]
    pub fn token_requests(&self) -> Vec<(ProviderName, String)> {
        self.state.lock().token_requests.clone()
    }

    /// Sets the `lifetime` of tokens issued from now on; the default is ten minutes.
    pub fn set_token_ttl(&self, ttl: Duration) {
        self.state.lock().token_ttl = ttl;
    }

    /// Returns the serials of the certificates revoked by serial, in order.
    #[must_use]
    pub fn revoked_certificates(&self) -> Vec<CertSerial> {
        self.state.lock().revoked_certificates.clone()
    }

    /// Returns the principals revoked, in order.
    #[must_use]
    pub fn revoked_principals(&self) -> Vec<Identity> {
        self.state.lock().revoked_principals.clone()
    }

    /// Returns the principals restored, in order.
    #[must_use]
    pub fn restored_principals(&self) -> Vec<Identity> {
        self.state.lock().restored_principals.clone()
    }

    /// Returns `(path, bearer)` for every request to a bearer-authenticated endpoint, in order;
    /// `bearer` is the token from the `authorization` header, `None` when absent. Requests that
    /// fail authentication are recorded too.
    #[must_use]
    pub fn calls(&self) -> Vec<(String, Option<String>)> {
        self.state.lock().calls.clone()
    }

    /// `(method, path, bearer, request)` for every certificate request received.
    #[must_use]
    pub fn certificate_requests(
        &self,
    ) -> Vec<(String, String, Option<String>, CreateCertificateRequest)> {
        self.state.lock().certificate_requests.clone()
    }

    /// Answers every later request of every endpoint with `status` and `body`.
    pub fn force_response(&self, status: u16, body: Vec<u8>) {
        self.force_response_with_headers(status, &[], body);
    }

    /// Answers every request with `status`, the response `headers` and `body`.
    pub fn force_response_with_headers(
        &self,
        status: u16,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        self.state.lock().forced = Some(ForcedResponse {
            status,
            headers,
            body,
        });
    }
}

/// Returns the fixture CA certificate the server's TLS certificate chains to.
#[must_use]
pub fn ca_certificate() -> CertificatePem {
    fixture().ca.clone()
}

/// The certificate the fake server issues from `POST /v1/certificates`.
#[must_use]
pub fn issued_certificate_pem() -> CertificatePem {
    fixture().server_certificate.clone()
}

/// Returns the pin of the fixture CA public key: the trust anchor TLS logins use.
#[must_use]
pub fn ca_pin() -> CaPin {
    ca_pin_of_certificate(&ca_certificate()).expect("fixture ca certificate yields a pin")
}

fn new_state(info: Server<serde_json::Value>, clock: Arc<TestClock>) -> SharedServerState {
    Arc::new(Mutex::new(ServerState {
        info,
        clock,
        accepted: Vec::new(),
        token_requests: Vec::new(),
        token_ttl: DEFAULT_TOKEN_TTL,
        tokens_issued: 0,
        tokens: Vec::new(),
        calls: Vec::new(),
        certificate_requests: Vec::new(),
        revoked_certificates: Vec::new(),
        revoked_principals: Vec::new(),
        restored_principals: Vec::new(),
        forced: None,
    }))
}

fn router(state: SharedServerState) -> Router {
    Router::new().fallback(dispatch).with_state(state)
}

/// The bearer token in the request's `authorization` header, if it carries one.
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::to_owned)
}

/// Whether `bearer` is a token this fake issued and has not yet expired on the shared clock.
fn authorized(state: &ServerState, bearer: Option<&str>) -> bool {
    let Some(token) = bearer else {
        return false;
    };
    let now = state.clock.wall();
    state
        .tokens
        .iter()
        .any(|(issued, expires_at)| issued == token && now < *expires_at)
}

fn forced_answer(state: &ServerState) -> Option<Response> {
    state.forced.as_ref().map(forced_response)
}

async fn dispatch(
    State(shared): State<SharedServerState>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> Response {
    let method_name = method.to_string();
    let method = if method == axum::http::Method::GET {
        routes::Method::Get
    } else if method == axum::http::Method::POST {
        routes::Method::Post
    } else {
        return route_not_found();
    };
    let Some((route, raw)) = routes::lookup(method, uri.path()) else {
        return route_not_found();
    };
    let Ok(id) = raw.map(name::decode).transpose() else {
        return route_not_found();
    };
    let id = id.as_deref();
    let mut state = shared.lock();
    let bearer = bearer_token(&headers);
    let bearer_route = route.permission.is_some();
    if bearer_route {
        state.calls.push((uri.path().to_owned(), bearer.clone()));
    }
    if let Some(answer) = forced_answer(&state) {
        return answer;
    }
    if bearer_route && !authorized(&state, bearer.as_deref()) {
        return unauthenticated();
    }
    if *route == routes::GET_SERVER {
        let now = UNIX_EPOCH + Duration::from_millis(state.clock.wall().as_unix_millis());
        (
            [(header::DATE, httpdate::fmt_http_date(now))],
            Json(state.info.clone()),
        )
            .into_response()
    } else if *route == routes::AUTHENTICATE_SESSION {
        authenticate(&mut state, &body)
    } else if *route == routes::CREATE_CERTIFICATE {
        issue_certificate(&mut state, method_name, uri.path(), bearer, &body)
    } else if route.id == routes::REVOKE_CERTIFICATE.id {
        revoke_certificate(&mut state, id)
    } else if route.id == routes::REVOKE_PRINCIPAL.id {
        revoke_principal(&mut state, id)
    } else if route.id == routes::RESTORE_PRINCIPAL.id {
        restore_principal(&mut state, id)
    } else {
        route_not_found()
    }
}

fn authenticate(state: &mut ServerState, body: &str) -> Response {
    let request: AuthenticateRequest = match serde_json::from_str(body) {
        Ok(request) => request,
        Err(error) => {
            return api_error(
                ErrorReason::MalformedRequest,
                &format!("malformed authenticate request: {error}"),
            );
        }
    };
    state
        .token_requests
        .push((request.provider.clone(), request.proof.expose().to_owned()));
    let accepted = state
        .accepted
        .iter()
        .any(|(provider, proof)| *provider == request.provider && proof == request.proof.expose());
    if !accepted {
        return api_error(ErrorReason::ProofRejected, "proof rejected");
    }
    state.tokens_issued += 1;
    let token = format!("gang-token-{}", state.tokens_issued);
    let lifetime = state.token_ttl;
    let expires_at = state.clock.wall() + lifetime;
    state.tokens.push((token.clone(), expires_at));
    Json(AuthenticateResponse {
        token: AccessToken::new(token),
        lifetime,
    })
    .into_response()
}

fn issue_certificate(
    state: &mut ServerState,
    method: String,
    path: &str,
    bearer: Option<String>,
    body: &str,
) -> Response {
    let request: CreateCertificateRequest = match serde_json::from_str(body) {
        Ok(request) => request,
        Err(error) => {
            return api_error(
                ErrorReason::MalformedRequest,
                &format!("malformed certificate request: {error}"),
            );
        }
    };
    state
        .certificate_requests
        .push((method, path.to_owned(), bearer, request));
    let now = state.clock.wall();
    let serial = CertSerial::try_new("0c0c").expect("the fixture serial is valid");
    Json(Certificate {
        name: name::of::<name::Certificates>(&serial),
        principal: name::of::<name::Principals>(&certificate_holder()),
        create_time: now,
        expire_time: now + CERTIFICATE_LIFETIME,
        revoke_time: None,
        pem_certificate: Some(fixture().server_certificate.clone()),
        ca_certificate: Some(fixture().ca.clone()),
    })
    .into_response()
}

fn revoke_certificate(state: &mut ServerState, id: Option<&str>) -> Response {
    let Some(serial) = id.and_then(|id| CertSerial::try_new(id).ok()) else {
        return api_error(ErrorReason::CertificateNotFound, "no such certificate");
    };
    let answer = certificate(&serial, &certificate_holder(), state.clock.wall());
    state.revoked_certificates.push(serial);
    Json(answer).into_response()
}

fn revoke_principal(state: &mut ServerState, id: Option<&str>) -> Response {
    let Some(identity) = id.and_then(|id| id.parse::<Identity>().ok()) else {
        return api_error(ErrorReason::PrincipalNotFound, "no such principal");
    };
    let answer = default_revocation(&identity, state.clock.wall());
    state.revoked_principals.push(identity);
    Json(answer).into_response()
}

fn restore_principal(state: &mut ServerState, id: Option<&str>) -> Response {
    let Some(identity) = id.and_then(|id| id.parse::<Identity>().ok()) else {
        return api_error(ErrorReason::PrincipalNotFound, "no such principal");
    };
    let now = state.clock.wall();
    let mut restored = principal(&identity, now);
    restored.revoke_time = None;
    state.restored_principals.push(identity);
    Json(restored).into_response()
}

/// The identity every certificate the fake revokes by serial is issued to.
#[must_use]
pub fn certificate_holder() -> Identity {
    Identity::new(
        ProviderName::try_new("corp-ci").expect("fixture provider is valid"),
        gang_types::Principal::try_new("worker-a").expect("fixture principal is valid"),
    )
}

/// A certificate of `holder`, revoked at `now`.
fn certificate(serial: &CertSerial, holder: &Identity, now: Timestamp) -> Certificate {
    Certificate {
        name: name::of::<name::Certificates>(serial),
        principal: name::of::<name::Principals>(holder),
        create_time: now,
        expire_time: now + CERTIFICATE_LIFETIME,
        revoke_time: Some(now),
        pem_certificate: None,
        ca_certificate: None,
    }
}

/// The principal resource of `identity`, revoked at `now`.
fn principal(identity: &Identity, now: Timestamp) -> gang_wire::Principal {
    gang_wire::Principal {
        name: name::of::<name::Principals>(identity),
        provider: identity.provider.clone(),
        principal: identity.principal.clone(),
        create_time: now,
        last_seen_time: now,
        revoke_time: Some(now),
    }
}

/// The answer to a principal revocation when a test has not set one: the principal, revoked,
/// and two fixed certificates it held.
fn default_revocation(identity: &Identity, now: Timestamp) -> RevokePrincipalResponse {
    RevokePrincipalResponse {
        principal: principal(identity, now),
        certificates: ["0a0a", "0b0b"]
            .into_iter()
            .map(|serial| {
                certificate(
                    &CertSerial::try_new(serial).expect("fixture serial is valid"),
                    identity,
                    now,
                )
            })
            .collect(),
    }
}

fn route_not_found() -> Response {
    api_error(ErrorReason::RouteNotFound, "no such route")
}

fn unauthenticated() -> Response {
    api_error(ErrorReason::CredentialsMissing, "bearer token required")
}

fn api_error(reason: ErrorReason, message: &str) -> Response {
    let status = Status::new(reason, message, []);
    let code = StatusCode::from_u16(status.code).expect("error status is a valid http code");
    (code, Json(ErrorResponse::from(status))).into_response()
}

fn forced_response(forced: &ForcedResponse) -> Response {
    let status = StatusCode::from_u16(forced.status).expect("forced status is a valid http code");
    let mut answer_headers = HeaderMap::new();
    for (name, value) in &forced.headers {
        answer_headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("forced header name is valid"),
            HeaderValue::from_str(value).expect("forced header value is valid"),
        );
    }
    (status, answer_headers, forced.body.clone()).into_response()
}

/// Encodes `bytes` as unpadded base64url, the alphabet JWT segments use.
pub(super) fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let mut group = [0_u8; 3];
        group[..chunk.len()].copy_from_slice(chunk);
        let value = (u32::from(group[0]) << 16) | (u32::from(group[1]) << 8) | u32::from(group[2]);
        for position in 0..=chunk.len() {
            let shift = 18 - 6 * u32::try_from(position).expect("position is at most three");
            let index = usize::try_from((value >> shift) & 0x3f).expect("six bits fit in usize");
            out.push(char::from(ALPHABET[index]));
        }
    }
    out
}

/// Returns the permission bits of `path`.
pub(super) fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .expect("path has metadata")
        .permissions()
        .mode()
        & 0o777
}

/// The provider named `name`, which the tests use with valid names only.
pub(super) fn provider(name: &str) -> ProviderName {
    ProviderName::try_new(name).expect("provider name is valid")
}

/// A JWT-shaped proof `header.payload.signature` whose payload is `{"exp": <seconds>}` for
/// `expires_at`. The signature segment carries `sequence`, so every grant's proof is distinct.
fn jwt_proof(sequence: usize, expires_at: Timestamp) -> Proof {
    let header = base64url(br#"{"alg":"ES256"}"#);
    let payload =
        base64url(format!(r#"{{"exp":{}}}"#, expires_at.as_unix_millis() / 1000).as_bytes());
    let signature = base64url(format!("sig-{sequence}").as_bytes());
    Proof::new(format!("{header}.{payload}.{signature}"))
}

#[derive(Default)]
struct ProviderState {
    clock: Option<Arc<TestClock>>,
    begins: AtomicUsize,
    completes: AtomicUsize,
    grants: AtomicUsize,
    renews: AtomicUsize,
    revoked: AtomicBool,
    withhold: AtomicBool,
    latest_refresh: Mutex<Option<String>>,
}

/// Identity providers whose behaviour the test scripts; every provider built from one
/// `FakeProviders` shares its state. Proofs expire [`PROOF_LIFETIME`] after they are minted.
#[derive(Clone)]
pub struct FakeProviders {
    state: Arc<ProviderState>,
}

impl FakeProviders {
    /// Creates providers that mint proofs against `clock`.
    #[must_use]
    pub fn new(clock: Arc<TestClock>) -> Self {
        Self {
            state: Arc::new(ProviderState {
                clock: Some(clock),
                ..ProviderState::default()
            }),
        }
    }

    /// Returns how many `begin_login` calls have been made so far.
    #[must_use]
    pub fn begin_login_calls(&self) -> usize {
        self.state.begins.load(Ordering::SeqCst)
    }

    /// Returns how many `complete_login` calls have been made so far.
    #[must_use]
    pub fn complete_login_calls(&self) -> usize {
        self.state.completes.load(Ordering::SeqCst)
    }

    /// Returns how many renewals have been attempted so far.
    #[must_use]
    pub fn renew_calls(&self) -> usize {
        self.state.renews.load(Ordering::SeqCst)
    }

    /// Makes every refresh token fail to renew, including the latest one.
    pub fn revoke_refresh_tokens(&self) {
        self.state.revoked.store(true, Ordering::SeqCst);
    }

    /// Makes renewals validate the presented refresh token but return no rotated one, so the
    /// latest token stays the one the client already holds.
    pub fn withhold_rotated_refresh_tokens(&self) {
        self.state.withhold.store(true, Ordering::SeqCst);
    }

    fn clock(&self) -> &Arc<TestClock> {
        self.state
            .clock
            .as_ref()
            .expect("fake providers are built with a clock")
    }

    /// Mints the proof for grant number `sequence`, expiring one hour from now on the clock.
    fn mint_proof(&self, sequence: usize) -> Proof {
        jwt_proof(sequence, self.clock().wall() + PROOF_LIFETIME)
    }

    /// Issues the next grant and makes its refresh token the only one that renews.
    fn issue_grant(&self) -> LoginGrant {
        let n = self.state.grants.fetch_add(1, Ordering::SeqCst) + 1;
        let refresh = format!("refresh-{n}");
        *self.state.latest_refresh.lock() = Some(refresh.clone());
        LoginGrant {
            proof: self.mint_proof(n),
            refresh_token: Some(RefreshToken::new(refresh)),
        }
    }
}

impl LoginProviders for FakeProviders {
    type Config = serde_json::Value;
    type Provider = FakeProvider;

    fn build(&self, config: serde_json::Value) -> Result<FakeProvider, PortError> {
        let interactive = config
            .get("interactive")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        Ok(FakeProvider {
            interactive,
            providers: self.clone(),
        })
    }
}

/// One fake identity provider built by [`FakeProviders`].
pub struct FakeProvider {
    interactive: bool,
    providers: FakeProviders,
}

impl LoginPort for FakeProvider {
    fn login(&self) -> LoginSupport {
        if self.interactive {
            LoginSupport::Interactive
        } else {
            LoginSupport::Unsupported
        }
    }

    async fn begin_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError> {
        let n = self.providers.state.begins.fetch_add(1, Ordering::SeqCst) + 1;
        let state = format!("state-{n}");
        let authorization_url = Url::parse_with_params(
            "https://idp.test/authorize",
            [
                ("redirect_uri", request.redirect_uri.as_str()),
                ("state", state.as_str()),
            ],
        )
        .expect("authorization url parses");
        Ok(LoginChallenge {
            authorization_url,
            state: state.clone(),
            session: LoginSession::new(state),
        })
    }

    async fn complete_login(
        &self,
        session: LoginSession,
        callback_url: &Url,
    ) -> Result<LoginGrant, PortError> {
        self.providers
            .state
            .completes
            .fetch_add(1, Ordering::SeqCst);
        let param = |key: &str| {
            callback_url
                .query_pairs()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.into_owned())
        };
        let code_ok = param("code").as_deref() == Some("authorization-code");
        let state_ok = param("state").as_deref() == Some(session.expose());
        if !(code_ok && state_ok) {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        Ok(self.providers.issue_grant())
    }

    async fn renew(
        &self,
        refresh_token: &RefreshToken,
        _client: Option<&LoginClient>,
    ) -> Result<LoginGrant, PortError> {
        let state = &self.providers.state;
        state.renews.fetch_add(1, Ordering::SeqCst);
        let current = state.latest_refresh.lock().as_deref() == Some(refresh_token.expose());
        if !current || state.revoked.load(Ordering::SeqCst) {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        if state.withhold.load(Ordering::SeqCst) {
            let n = state.grants.fetch_add(1, Ordering::SeqCst) + 1;
            return Ok(LoginGrant {
                proof: self.providers.mint_proof(n),
                refresh_token: None,
            });
        }
        Ok(self.providers.issue_grant())
    }
}

#[derive(Default)]
struct ConsoleState {
    announced: Mutex<Vec<Url>>,
    arrived: Notify,
    oversized: Mutex<Option<String>>,
}

/// A console that records each announced login URL and answers according to its mode.
///
/// Clones share the recorded URLs, so the test can read them through its own handle while the
/// client holds another.
#[derive(Clone)]
pub struct FakeConsole {
    mode: ConsoleMode,
    state: Arc<ConsoleState>,
}

impl FakeConsole {
    /// Creates a console that answers according to `mode`.
    #[must_use]
    pub fn new(mode: ConsoleMode) -> Self {
        Self {
            mode,
            state: Arc::new(ConsoleState::default()),
        }
    }

    /// Returns the authorization URLs announced so far, in order.
    #[must_use]
    pub fn announced(&self) -> Vec<Url> {
        self.state.announced.lock().clone()
    }

    /// Returns the status line the loopback listener answered the oversized callback head with,
    /// or `None` when it closed without an answer or no oversized head was sent yet.
    #[must_use]
    pub fn oversized_status(&self) -> Option<String> {
        self.state.oversized.lock().clone()
    }
}

impl Console for FakeConsole {
    fn announce(&self, url: &Url) {
        self.state.announced.lock().push(url.clone());
        self.state.arrived.notify_one();
        match self.mode {
            ConsoleMode::Browser => {
                let callback = callback_url(url, &query_param(url, "state"));
                tokio::spawn(deliver_browser_callback(callback));
            }
            ConsoleMode::OversizedHead => {
                let callback = callback_url(url, &query_param(url, "state"));
                tokio::spawn(deliver_oversized_callback(
                    callback,
                    Arc::clone(&self.state),
                ));
            }
            ConsoleMode::Paste | ConsoleMode::WrongState | ConsoleMode::Silent => {}
        }
    }

    fn pasted_redirect(&self) -> impl Future<Output = Url> + Send {
        let mode = self.mode;
        let state = Arc::clone(&self.state);
        async move {
            match mode {
                ConsoleMode::Browser | ConsoleMode::OversizedHead | ConsoleMode::Silent => {
                    std::future::pending::<Url>().await
                }
                ConsoleMode::Paste | ConsoleMode::WrongState => {
                    let announced = next_announced(&state).await;
                    let state_value = if mode == ConsoleMode::WrongState {
                        "forged".to_owned()
                    } else {
                        query_param(&announced, "state")
                    };
                    callback_url(&announced, &state_value)
                }
            }
        }
    }
}

/// Returns the most recently announced URL, waiting until one is announced.
async fn next_announced(state: &ConsoleState) -> Url {
    loop {
        let arrived = state.arrived.notified();
        if let Some(url) = state.announced.lock().last().cloned() {
            return url;
        }
        arrived.await;
    }
}

/// Returns the value of the query parameter `key` in `url`.
fn query_param(url: &Url, key: &str) -> String {
    url.query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
        .expect("authorization url carries the parameter")
}

/// Builds the redirect the provider sends after the user approves: the `redirect_uri` of the
/// authorization URL with a valid code and the given state.
fn callback_url(authorization: &Url, state: &str) -> Url {
    let redirect = query_param(authorization, "redirect_uri");
    let mut callback = Url::parse(&redirect).expect("redirect uri parses");
    callback
        .query_pairs_mut()
        .append_pair("code", "authorization-code")
        .append_pair("state", state);
    callback
}

/// Opens the callback like a browser would: a plain HTTP request to the loopback listener.
async fn deliver_browser_callback(callback: Url) {
    let (host, port, target) = callback_endpoint(&callback);
    let mut stream = TcpStream::connect((host.as_str(), port))
        .await
        .expect("connecting to the loopback callback");
    let request =
        format!("GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("sending the callback request");
}

/// Splits a loopback callback into its host, its port and the request target.
fn callback_endpoint(callback: &Url) -> (String, u16, String) {
    let host = callback.host_str().expect("callback has a host").to_owned();
    let port = callback
        .port_or_known_default()
        .expect("callback has a port");
    let mut target = callback.path().to_owned();
    if let Some(query) = callback.query() {
        target = format!("{target}?{query}");
    }
    (host, port, target)
}

/// Sends the callback first with an extra 2048-byte header, records the status line of the
/// answer, then delivers the normal callback as a browser would.
async fn deliver_oversized_callback(callback: Url, state: Arc<ConsoleState>) {
    let status = send_oversized_head(&callback).await;
    *state.oversized.lock() = status;
    deliver_browser_callback(callback).await;
}

/// Returns the first line of the loopback listener's answer to an oversized callback head, or
/// `None` when the connection closes before any answer.
async fn send_oversized_head(callback: &Url) -> Option<String> {
    let (host, port, target) = callback_endpoint(callback);
    let mut stream = TcpStream::connect((host.as_str(), port)).await.ok()?;
    let padding = "a".repeat(2048);
    let request = format!(
        "GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\nX-Padding: {padding}\r\n\
         Connection: close\r\n\r\n"
    );
    // The listener may answer and close before the whole head is written; its answer still counts.
    let _ = stream.write_all(request.as_bytes()).await;
    let mut answer = Vec::new();
    let mut chunk = [0_u8; 256];
    while !answer.windows(2).any(|pair| pair == b"\r\n") {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => answer.extend_from_slice(&chunk[..read]),
        }
    }
    let end = answer.windows(2).position(|pair| pair == b"\r\n")?;
    Some(String::from_utf8_lossy(&answer[..end]).into_owned())
}

/// The state every login test starts from: a temporary client home, a fake server, fake
/// providers and a console in the requested mode, all sharing one clock.
pub struct Harness {
    /// The fake Gang server.
    pub server: FakeServer,
    /// The fake identity providers.
    pub providers: FakeProviders,
    /// The clock every component reads.
    pub clock: Arc<TestClock>,
    /// The client's home directory.
    pub home: ClientHome,
    console: FakeConsole,
    _dir: TempDir,
}

impl Harness {
    /// Starts a plaintext fake server with `info`, and a harness around it whose clock starts at
    /// 2024-01-01T00:00:00Z.
    pub async fn start_plain(mode: ConsoleMode, info: Server<serde_json::Value>) -> Self {
        let clock = Arc::new(TestClock::new(START));
        let server = FakeServer::start_plain(info, clock.clone()).await;
        Self::assemble(mode, server, clock)
    }

    /// Starts a TLS fake server with `info` (see [`FakeServer::start_tls`]), and a harness around
    /// it whose clock starts at 2024-01-01T00:00:00Z.
    pub async fn start_tls(mode: ConsoleMode, info: Server<serde_json::Value>) -> Self {
        let clock = Arc::new(TestClock::new(START));
        let server = FakeServer::start_tls(info, clock.clone()).await;
        Self::assemble(mode, server, clock)
    }

    fn assemble(mode: ConsoleMode, server: FakeServer, clock: Arc<TestClock>) -> Self {
        let dir = TempDir::new().expect("creating a temporary client home");
        let home = ClientHome::try_new(dir.path().join("client"))
            .expect("temporary client home path is valid");
        Self {
            server,
            providers: FakeProviders::new(clock.clone()),
            clock,
            home,
            console: FakeConsole::new(mode),
            _dir: dir,
        }
    }

    /// Creates a fresh client over the shared home, providers, console and clock.
    #[must_use]
    pub fn client(&self) -> Client<FakeProviders, FakeConsole, Arc<TestClock>> {
        self.client_with(ClientOptions::default())
    }

    /// Creates a fresh client with `options` over the shared home, providers, console and clock.
    #[must_use]
    pub fn client_with(
        &self,
        options: ClientOptions,
    ) -> Client<FakeProviders, FakeConsole, Arc<TestClock>> {
        Client::new(
            self.home.clone(),
            options,
            self.providers.clone(),
            self.console.clone(),
            self.clock.clone(),
        )
    }

    /// Returns the console the clients created by [`Harness::client`] use.
    #[must_use]
    pub fn console(&self) -> &FakeConsole {
        &self.console
    }
}
