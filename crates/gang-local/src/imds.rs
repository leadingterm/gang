//! Mock EC2 instance metadata service and OIDC issuer for local workers.
//!
//! Serves the instance metadata a worker reads (identity document, spot interruption
//! notice, workload token). Each instance authenticates with the secret it was launched
//! with, presented as a bearer token. The discovery and JWKS documents that verify
//! workload tokens are public. Plain HTTP only.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use gang_clock::Clock;
use gang_types::InstanceId;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::errors::LocalError;
use crate::jwt::JwtMinter;
use crate::records::{InstanceRecord, RecordStore};

/// Header naming the instance a request acts for.
///
/// Stands in for the per-VM network identity that EC2 gives each instance.
pub(crate) const INSTANCE_HEADER: &str = "x-gang-instance";

/// Shared state behind every route.
///
/// Read-only: instances and preemption notices live in the record store, so a restarted
/// server keeps serving the same fleet.
pub(crate) struct ImdsState<C> {
    records: RecordStore,
    minter: JwtMinter,
    clock: C,
}

impl<C: Clock + 'static> ImdsState<C> {
    /// Builds the state over the record store, the token minter and the clock that stamps tokens.
    #[must_use]
    pub(crate) fn new(records: RecordStore, minter: JwtMinter, clock: C) -> Self {
        Self {
            records,
            minter,
            clock,
        }
    }

    /// Returns the clock that stamps tokens and expiries.
    #[must_use]
    pub(crate) fn clock(&self) -> &C {
        &self.clock
    }

    /// Builds the router whose handlers read this state.
    pub(crate) fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route(
                "/latest/dynamic/instance-identity/document",
                get(identity_document::<C>),
            )
            .route(
                "/latest/meta-data/spot/instance-action",
                get(instance_action::<C>),
            )
            .route(
                "/latest/meta-data/gang/workload-token",
                get(workload_token::<C>),
            )
            .route("/.well-known/openid-configuration", get(discovery::<C>))
            .route("/jwks.json", get(jwks::<C>))
            .with_state(Arc::clone(self))
    }

    /// Authenticates the request and returns the record of the instance it names.
    ///
    /// A missing or malformed instance header is 400. A missing or non-bearer `Authorization`
    /// header, an unknown instance and a wrong secret are all 401, so the response does not
    /// reveal which instances exist.
    fn authorize(&self, headers: &HeaderMap) -> Result<InstanceRecord, StatusCode> {
        let id = instance_header(headers)?;
        let presented = bearer(headers).ok_or(StatusCode::UNAUTHORIZED)?;
        let record = self
            .records
            .read(&id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if record.secret.matches(presented) {
            Ok(record)
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

/// Runs `work` on the blocking pool: it reads instance records from disk.
async fn off_runtime<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, StatusCode> + Send + 'static,
) -> Result<T, StatusCode> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

/// Serves the instance identity document as JSON.
async fn identity_document<C: Clock + 'static>(
    State(state): State<Arc<ImdsState<C>>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    off_runtime(move || {
        let record = state.authorize(&headers)?;
        Ok(json_response(&json!({
            "instanceId": record.id.as_str(),
            "fleet": record.fleet.as_str(),
            "architecture": record.isa.as_str(),
        })))
    })
    .await
}

/// Serves the spot interruption notice; 404 with an empty body when none is recorded.
async fn instance_action<C: Clock + 'static>(
    State(state): State<Arc<ImdsState<C>>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    off_runtime(move || {
        let record = state.authorize(&headers)?;
        let terminate_at = state
            .records
            .read_preemption(&record.id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match terminate_at {
            Some(terminate_at) => Ok(json_response(&json!({
                "action": "terminate",
                "terminate_at_unix_ms": terminate_at.as_unix_millis(),
            }))),
            None => Err(StatusCode::NOT_FOUND),
        }
    })
    .await
}

/// Mints a workload token for the instance named by the request.
async fn workload_token<C: Clock + 'static>(
    State(state): State<Arc<ImdsState<C>>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    off_runtime(move || {
        let record = state.authorize(&headers)?;
        let token = state
            .minter
            .mint(&record.id, state.clock().wall())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(text_response(token))
    })
    .await
}

/// Serves the `OpenID` discovery document; needs no credentials.
async fn discovery<C: Clock + 'static>(State(state): State<Arc<ImdsState<C>>>) -> Response {
    json_response(&state.minter.discovery())
}

/// Serves the public key set that verifies workload tokens; needs no credentials.
async fn jwks<C: Clock + 'static>(State(state): State<Arc<ImdsState<C>>>) -> Response {
    json_response(&state.minter.jwks())
}

/// Returns the text of a header, or `None` when it is absent or not visible ASCII.
fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

/// Returns the credential of an `Authorization: Bearer <text>` header, or `None` for any other.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, header::AUTHORIZATION.as_str())?.strip_prefix("Bearer ")
}

/// Parses the instance header into a validated id; absent or invalid is 400.
fn instance_header(headers: &HeaderMap) -> Result<InstanceId, StatusCode> {
    let text = header_text(headers, INSTANCE_HEADER).ok_or(StatusCode::BAD_REQUEST)?;
    InstanceId::try_new(text.to_owned()).map_err(|_| StatusCode::BAD_REQUEST)
}

/// Builds a 200 response carrying `value` as `application/json`.
fn json_response(value: &Value) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Builds a 200 response carrying `body` as `text/plain`.
fn text_response(body: String) -> Response {
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain")], body).into_response()
}

/// Mock IMDS and OIDC issuer bound to a listener, ready to serve.
///
/// Created when the provisioner attaches; the listener is bound before the server runs,
/// so the chosen port is known up front.
pub(crate) struct MockImds {
    listener: TcpListener,
    router: Router,
}

impl MockImds {
    /// Pairs a bound listener with the router that will answer on it.
    #[must_use]
    pub(crate) fn new(listener: TcpListener, router: Router) -> Self {
        Self { listener, router }
    }

    /// Returns the address the listener is bound to.
    ///
    /// # Errors
    ///
    /// Returns `LocalError::MockImdsListen` when the listener cannot report its address.
    pub(crate) fn local_addr(&self) -> Result<SocketAddr, LocalError> {
        self.listener
            .local_addr()
            .map_err(LocalError::MockImdsListen)
    }

    /// Serves requests until the returned future is dropped.
    ///
    /// # Errors
    ///
    /// Returns `LocalError::MockImdsListen` when the server stops on an I/O error.
    pub(crate) async fn serve(self) -> Result<(), LocalError> {
        axum::serve(self.listener, self.router)
            .await
            .map_err(LocalError::MockImdsListen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::error::Error;

    use axum::http::{HeaderName, HeaderValue, Method};
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use bytes::Bytes;
    use gang_clock::{SystemClock, TestClock, Timestamp};
    use gang_tls::{HttpClient, HttpProtocol, HttpRequest, HttpSettings, HttpTransport};
    use gang_types::{Audience, FleetName, Isa, NonZeroBytes, NonZeroDuration, TokenTtl, Url};
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::{Signature, VerifyingKey};
    use tempfile::TempDir;

    use crate::imds_client::ImdsClient;
    use crate::key_file;
    use crate::layout::StateLayout;
    use crate::worker_identity::{InstanceSecret, WorkerIdentity};

    const NOW_MILLIS: u64 = 1_700_000_000_000;
    const IDENTITY_PATH: &str = "/latest/dynamic/instance-identity/document";
    const SPOT_PATH: &str = "/latest/meta-data/spot/instance-action";
    const WORKLOAD_PATH: &str = "/latest/meta-data/gang/workload-token";
    /// Every route that acts for an instance and so requires its credentials.
    const INSTANCE_PATHS: [&str; 3] = [IDENTITY_PATH, SPOT_PATH, WORKLOAD_PATH];
    const AUTHORIZATION: &str = "authorization";

    struct Harness {
        addr: SocketAddr,
        issuer: String,
        records: RecordStore,
        _dir: TempDir,
    }

    impl Harness {
        async fn start() -> Result<Self, Box<dyn Error>> {
            let dir = tempfile::tempdir()?;
            let layout = StateLayout::new(dir.path().join("state"));
            let key = key_file::load_or_create(&layout.jwt_key_path())?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let issuer = format!("http://{addr}");
            let records = RecordStore::new(layout);
            let minter = JwtMinter::new(
                &key,
                issuer.clone(),
                Audience::try_new("gang")?,
                TokenTtl::MAX,
            )?;
            let clock = TestClock::new(Timestamp::from_unix_millis(NOW_MILLIS));
            let state = Arc::new(ImdsState::new(records.clone(), minter, clock));
            let _server = tokio::spawn(MockImds::new(listener, state.router()).serve());
            Ok(Self {
                addr,
                issuer,
                records,
                _dir: dir,
            })
        }

        fn client(&self) -> ImdsClient {
            ImdsClient::new(
                self.addr,
                NonZeroDuration::from_secs(5),
                Arc::new(SystemClock::new()),
            )
            .expect("plain HTTP client builds")
        }

        /// Records an instance launched with `secret` and returns the identity a worker holds for it.
        fn add_instance(&self, id: &str, secret: &str) -> Result<WorkerIdentity, Box<dyn Error>> {
            let instance = InstanceId::try_new(id.to_owned())?;
            let secret = InstanceSecret::new(secret);
            self.records.write(&InstanceRecord {
                id: instance.clone(),
                fleet: FleetName::try_new("workers-arm")?,
                isa: Isa::Aarch64,
                pid: Some(1),
                launched_at: Timestamp::from_unix_millis(NOW_MILLIS),
                terminating_since: None,
                secret: secret.clone(),
            })?;
            Ok(WorkerIdentity { instance, secret })
        }

        /// Sends a request with arbitrary headers, so tests can omit or forge credentials.
        async fn send(
            &self,
            method: Method,
            path: &str,
            headers: &[(&str, &str)],
        ) -> Result<(StatusCode, Bytes), Box<dyn Error>> {
            let client = HttpClient::try_new(&HttpSettings {
                transport: HttpTransport::Plain,
                protocol: HttpProtocol::Negotiated,
                timeout: NonZeroDuration::from_secs(5),
                max_body: NonZeroBytes::kib(64),
                clock: Arc::new(SystemClock::new()),
            })?;
            let url = Url::parse(&format!("http://{}{path}", self.addr))?;
            let mut request_headers = Vec::with_capacity(headers.len());
            for (name, value) in headers {
                request_headers.push((
                    HeaderName::from_bytes(name.as_bytes())?,
                    HeaderValue::from_str(value)?,
                ));
            }
            let response = client
                .send(HttpRequest {
                    method,
                    url: &url,
                    bearer: None,
                    headers: &request_headers,
                    body: None,
                })
                .await?;
            Ok((response.status, response.body))
        }
    }

    /// The `Authorization` value that presents `secret` as a bearer credential.
    fn bearer_for(secret: &str) -> String {
        format!("Bearer {secret}")
    }

    #[tokio::test]
    async fn identity_document_names_the_instance() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let worker = harness.add_instance("local-1", "secret-1")?;
        let identity = harness.client().identity(&worker).await?;
        assert_eq!(identity.instance.as_str(), "local-1");
        assert_eq!(identity.isa, Isa::Aarch64);
        assert_eq!(identity.fleet.as_str(), "workers-arm");
        Ok(())
    }

    #[tokio::test]
    async fn requests_without_a_bearer_secret_are_unauthorized() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        harness.add_instance("local-1", "secret-1")?;
        for path in INSTANCE_PATHS {
            let (missing, _) = harness
                .send(Method::GET, path, &[(INSTANCE_HEADER, "local-1")])
                .await?;
            assert_eq!(
                missing,
                StatusCode::UNAUTHORIZED,
                "{path} without authorization"
            );

            let (basic, _) = harness
                .send(
                    Method::GET,
                    path,
                    &[
                        (INSTANCE_HEADER, "local-1"),
                        (AUTHORIZATION, "Basic c2VjcmV0LTE="),
                    ],
                )
                .await?;
            assert_eq!(basic, StatusCode::UNAUTHORIZED, "{path} with basic scheme");

            let (bare, _) = harness
                .send(
                    Method::GET,
                    path,
                    &[(INSTANCE_HEADER, "local-1"), (AUTHORIZATION, "secret-1")],
                )
                .await?;
            assert_eq!(
                bare,
                StatusCode::UNAUTHORIZED,
                "{path} without bearer scheme"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn requests_presenting_the_wrong_secret_are_unauthorized() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let worker = harness.add_instance("local-1", "secret-1")?;
        let wrong = bearer_for("secret-2");
        for path in INSTANCE_PATHS {
            let (status, _) = harness
                .send(
                    Method::GET,
                    path,
                    &[
                        (INSTANCE_HEADER, "local-1"),
                        (AUTHORIZATION, wrong.as_str()),
                    ],
                )
                .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} with wrong secret");
        }

        let imposter = WorkerIdentity {
            instance: worker.instance.clone(),
            secret: InstanceSecret::new("secret-2"),
        };
        let error = harness
            .client()
            .identity(&imposter)
            .await
            .err()
            .ok_or("identity with the wrong secret must fail")?;
        assert!(matches!(error, LocalError::ImdsStatus { status: 401 }));
        Ok(())
    }

    #[tokio::test]
    async fn an_instance_cannot_act_with_another_instances_secret() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        harness.add_instance("local-1", "secret-1")?;
        harness.add_instance("local-2", "secret-2")?;
        let stolen = bearer_for("secret-1");
        for path in INSTANCE_PATHS {
            let (status, _) = harness
                .send(
                    Method::GET,
                    path,
                    &[
                        (INSTANCE_HEADER, "local-2"),
                        (AUTHORIZATION, stolen.as_str()),
                    ],
                )
                .await?;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{path} impersonating local-1"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn unknown_instance_is_unauthorized_whatever_the_secret() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        harness.add_instance("local-1", "secret-1")?;
        let ghost = WorkerIdentity {
            instance: InstanceId::try_new("local-99".to_owned())?,
            secret: InstanceSecret::new("secret-1"),
        };
        let error = harness
            .client()
            .identity(&ghost)
            .await
            .err()
            .ok_or("identity of an unknown instance must fail")?;
        assert!(matches!(error, LocalError::ImdsStatus { status: 401 }));

        let presented = bearer_for("secret-1");
        for path in INSTANCE_PATHS {
            let (status, _) = harness
                .send(
                    Method::GET,
                    path,
                    &[
                        (INSTANCE_HEADER, "local-99"),
                        (AUTHORIZATION, presented.as_str()),
                    ],
                )
                .await?;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{path} for unknown instance"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_malformed_instance_header_is_bad_request() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        harness.add_instance("local-1", "secret-1")?;
        let presented = bearer_for("secret-1");
        let (missing, _) = harness
            .send(
                Method::GET,
                IDENTITY_PATH,
                &[(AUTHORIZATION, presented.as_str())],
            )
            .await?;
        assert_eq!(missing, StatusCode::BAD_REQUEST);

        let (malformed, _) = harness
            .send(
                Method::GET,
                IDENTITY_PATH,
                &[
                    (INSTANCE_HEADER, "not a name"),
                    (AUTHORIZATION, presented.as_str()),
                ],
            )
            .await?;
        assert_eq!(malformed, StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn preemption_is_absent_then_reported() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let worker = harness.add_instance("local-1", "secret-1")?;
        let client = harness.client();
        assert!(client.preemption(&worker).await?.is_none());

        let terminate_at = Timestamp::from_unix_millis(NOW_MILLIS + 120_000);
        harness
            .records
            .write_preemption(&worker.instance, terminate_at)?;
        let presented = bearer_for("secret-1");
        let (status, body) = harness
            .send(
                Method::GET,
                SPOT_PATH,
                &[
                    (INSTANCE_HEADER, "local-1"),
                    (AUTHORIZATION, presented.as_str()),
                ],
            )
            .await?;
        assert_eq!(status, StatusCode::OK);
        let document: Value = serde_json::from_slice(&body)?;
        assert_eq!(
            document["terminate_at_unix_ms"],
            json!(terminate_at.as_unix_millis())
        );
        let notice = client
            .preemption(&worker)
            .await?
            .ok_or("a recorded preemption must be reported")?;
        assert_eq!(notice.terminate_at, terminate_at);
        Ok(())
    }

    #[tokio::test]
    async fn workload_token_verifies_against_served_jwks() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let worker = harness.add_instance("local-1", "secret-1")?;
        let token = harness.client().workload_token(&worker).await?;

        let (status, jwks_body) = harness.send(Method::GET, "/jwks.json", &[]).await?;
        assert_eq!(status, StatusCode::OK);
        let jwks: Value = serde_json::from_slice(&jwks_body)?;
        let key = &jwks["keys"][0];
        let kid = key["kid"].as_str().ok_or("jwks key has no kid")?;
        let x = URL_SAFE_NO_PAD.decode(key["x"].as_str().ok_or("jwks key has no x")?)?;
        let y = URL_SAFE_NO_PAD.decode(key["y"].as_str().ok_or("jwks key has no y")?)?;
        let mut sec1 = vec![0x04];
        sec1.extend_from_slice(&x);
        sec1.extend_from_slice(&y);
        let verifying_key =
            VerifyingKey::from_sec1_bytes(&sec1).map_err(|_| "jwks key is not a P-256 point")?;

        let [header_b64, claims_b64, signature_b64]: [&str; 3] = token
            .expose()
            .split('.')
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| "token must have three segments")?;
        let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header_b64)?)?;
        assert_eq!(header["alg"].as_str(), Some("ES256"));
        assert_eq!(header["kid"].as_str(), Some(kid));

        let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims_b64)?)?;
        let issued_at = NOW_MILLIS / 1000;
        assert_eq!(claims["iss"].as_str(), Some(harness.issuer.as_str()));
        assert_eq!(claims["sub"].as_str(), Some("local-1"));
        assert_eq!(claims["aud"].as_str(), Some("gang"));
        assert_eq!(claims["iat"].as_u64(), Some(issued_at));
        assert_eq!(claims["nbf"].as_u64(), Some(issued_at));
        assert_eq!(claims["exp"].as_u64(), Some(issued_at + 3600));

        let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(signature_b64)?)
            .map_err(|_| "signature is not 64 bytes")?;
        let signing_input = format!("{header_b64}.{claims_b64}");
        verifying_key
            .verify(signing_input.as_bytes(), &signature)
            .map_err(|_| "signature does not verify")?;
        Ok(())
    }

    #[tokio::test]
    async fn workload_token_names_only_the_authenticated_instance() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let first = harness.add_instance("local-1", "secret-1")?;
        let second = harness.add_instance("local-2", "secret-2")?;
        let client = harness.client();
        for worker in [&first, &second] {
            let token = client.workload_token(worker).await?;
            let claims_b64 = token
                .expose()
                .split('.')
                .nth(1)
                .ok_or("token must have a claims segment")?;
            let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims_b64)?)?;
            assert_eq!(claims["sub"].as_str(), Some(worker.instance.as_str()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn discovery_document_is_served_without_credentials() -> Result<(), Box<dyn Error>> {
        let harness = Harness::start().await?;
        let (status, body) = harness
            .send(Method::GET, "/.well-known/openid-configuration", &[])
            .await?;
        assert_eq!(status, StatusCode::OK);
        let document: Value = serde_json::from_slice(&body)?;
        let jwks_uri = format!("{}/jwks.json", harness.issuer);
        assert_eq!(document["jwks_uri"].as_str(), Some(jwks_uri.as_str()));

        let (jwks_status, _) = harness.send(Method::GET, "/jwks.json", &[]).await?;
        assert_eq!(jwks_status, StatusCode::OK);
        Ok(())
    }
}
