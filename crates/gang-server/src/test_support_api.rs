//! Requests to the Gang API (`/v1/*`) that the tests send to a [`Controller`], and the responses
//! they read back.

use std::collections::BTreeSet;
use std::convert::Infallible;

use crate::gate::Caller;
use crate::{Deployment, Parts, Server, ServerSettings};
use bytes::Bytes;
use gang_spi::{ApiRequestBody, ApiResponseBody, BoxedError, ControllerReady, Peer};
use gang_testkit::{Controller, FakeScheduler};
use gang_types::{AccessToken, Identity, Principal, Proof, ProviderName};
use gang_wire::routes::{AUTHENTICATE_SESSION, GET_SERVER};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Detail, ERROR_DOMAIN, ErrorReason, ErrorResponse,
    Status,
};
use http::header::{
    AUTHORIZATION, CONTENT_TYPE, HeaderValue, LOCATION, SET_COOKIE, WWW_AUTHENTICATE,
};
use http::{HeaderMap, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

/// Decodes one complete SSE frame emitted by the server.
pub fn decode_sse(bytes: &[u8]) -> gang_wire::Frame {
    let mut frames = gang_state::sse::Decoder::new()
        .push(bytes)
        .expect("the stream writes well-formed frames");
    assert_eq!(frames.len(), 1, "each chunk is one frame");
    frames.remove(0)
}

/// A fully collected HTTP response.
#[derive(Debug)]
pub struct ApiReply {
    /// HTTP status returned by the server.
    pub status: StatusCode,
    /// HTTP headers returned by the server.
    pub headers: HeaderMap,
    /// Fully collected response body.
    pub body: Bytes,
}

impl ApiReply {
    /// Deserializes the response body as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).expect("the body is JSON of the expected type")
    }

    /// Asserts an OK status and deserializes the response body as JSON.
    pub fn ok<T: DeserializeOwned>(&self) -> T {
        assert_eq!(
            self.status,
            StatusCode::OK,
            "status of {}",
            String::from_utf8_lossy(&self.body)
        );
        self.json()
    }

    /// Asserts an OK status and returns the JSON body.
    pub fn value(&self) -> serde_json::Value {
        self.ok()
    }

    /// Returns the response body as UTF-8 text.
    pub fn text(&self) -> &str {
        std::str::from_utf8(&self.body).expect("the response body is UTF-8")
    }

    /// Checks the error response against the expected reason and returns its status.
    pub fn refused(&self, reason: ErrorReason) -> Status {
        let text = String::from_utf8_lossy(&self.body);
        assert_eq!(self.status.as_u16(), reason.http(), "status of {text}");
        let ErrorResponse { error } = serde_json::from_slice(&self.body)
            .unwrap_or_else(|error| panic!("the body is an error envelope ({error}): {text}"));
        assert_eq!(error.code, reason.http(), "envelope code of {text}");
        assert_eq!(error.status, reason.status(), "envelope status of {text}");
        let infos = error
            .details
            .iter()
            .filter(|detail| matches!(detail, Detail::ErrorInfo(_)))
            .count();
        assert_eq!(infos, 1, "ErrorInfo appears exactly once in {text}");
        let info = error.error_info().expect("the error carries its ErrorInfo");
        assert_eq!(info.reason, reason.as_str(), "reason of {text}");
        assert_eq!(info.domain, ERROR_DOMAIN, "domain of {text}");
        let keys: BTreeSet<&str> = info.metadata.keys().map(String::as_str).collect();
        let expected: BTreeSet<&str> = reason.metadata_keys().iter().copied().collect();
        assert_eq!(keys, expected, "metadata keys of {text}");
        let challenge = self.headers.get(WWW_AUTHENTICATE);
        if self.status == StatusCode::UNAUTHORIZED {
            assert_eq!(
                challenge.map(http::HeaderValue::as_bytes),
                Some(&b"Bearer"[..])
            );
        } else {
            assert!(challenge.is_none(), "only a 401 carries a challenge");
        }
        error
    }

    /// Returns all Set-Cookie header values.
    pub fn set_cookies(&self) -> Vec<String> {
        self.headers
            .get_all(SET_COOKIE)
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .expect("set-cookie headers are visible ascii")
                    .to_owned()
            })
            .collect()
    }

    /// Returns the Location header value, if present.
    pub fn location(&self) -> Option<String> {
        self.headers.get(LOCATION).map(|value| {
            value
                .to_str()
                .expect("location headers are visible ascii")
                .to_owned()
        })
    }

    /// Returns the value of the named response cookie, if present.
    pub fn set_cookie_value(&self, name: &str) -> Option<String> {
        let prefix = format!("{name}=");
        self.set_cookies().iter().find_map(|cookie| {
            cookie
                .strip_prefix(&prefix)
                .map(|rest| rest.split(';').next().unwrap_or_default().to_owned())
        })
    }
}

/// Collects an HTTP response into an `ApiReply`.
pub async fn read(response: http::Response<ApiResponseBody>) -> ApiReply {
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("the response body is read")
        .to_bytes();
    ApiReply {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}

/// Identity under the provider shared by server fixtures.
pub fn identity(principal: &str) -> Identity {
    Identity::new(
        ProviderName::try_new("example-idp").expect("provider name is valid"),
        Principal::try_new(principal).expect("principal is valid"),
    )
}

/// Prepares the plaintext test server.
pub async fn prepare_plain(
    parts: Parts<crate::test_support::PlainDeployment>,
) -> Server<crate::test_support::PlainDeployment> {
    match Server::prepare(parts, crate::test_support::plain_settings()).await {
        Ok(server) => server,
        Err(error) => panic!("server prepares: {error}"),
    }
}
/// Starts a server over `parts` with `settings`, runs `body` against its controller once the
/// controller is published, and stops the server afterwards.
///
/// # Panics
///
/// Panics when the server does not prepare, or does not return `Ok` after it is stopped.
pub async fn serve<D, F, Fut>(
    parts: Parts<D>,
    settings: ServerSettings,
    scheduler: FakeScheduler,
    body: F,
) where
    D: Deployment,
    F: FnOnce(Controller) -> Fut,
    Fut: Future<Output = ()>,
{
    let server = match Server::prepare(parts, settings).await {
        Ok(server) => server,
        Err(error) => panic!("server prepares: {error}"),
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let driver = async {
        let controller = scheduler
            .controller()
            .await
            .expect("the scheduler publishes its controller");
        body(controller).await;
        let _ = stop.send(());
    };
    let (result, ()) = tokio::join!(
        server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ),
        driver
    );
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}

/// A request with `method` for `path` on the Gang API. A `body` is sent as JSON, and a `bearer`
/// token is sent as an `Authorization: Bearer` header.
///
/// # Panics
///
/// Panics when `method` or `path` is not a valid request line.
#[must_use]
pub fn api_request(
    method: &str,
    path: &str,
    bearer: Option<&AccessToken>,
    body: Option<serde_json::Value>,
) -> Request<ApiRequestBody> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = bearer {
        builder = builder.header(AUTHORIZATION, bearer_value(token));
    }
    let bytes = match body {
        Some(value) => {
            builder = builder.header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            serde_json::to_vec(&value).expect("test JSON serializes")
        }
        None => Vec::new(),
    };
    builder
        .body(boxed(Bytes::from(bytes)))
        .expect("test request is well formed")
}

/// A request with `method` for `path` on the Gang API whose body is `bytes` as given, for tests
/// of body handling. A `bearer` token is sent as an `Authorization: Bearer` header.
///
/// # Panics
///
/// Panics when `method` or `path` is not a valid request line.
#[must_use]
pub fn raw_api_request(
    method: &str,
    path: &str,
    bearer: Option<&AccessToken>,
    bytes: Vec<u8>,
) -> Request<ApiRequestBody> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = bearer {
        builder = builder.header(AUTHORIZATION, bearer_value(token));
    }
    builder
        .body(boxed(Bytes::from(bytes)))
        .expect("test request is well formed")
}

/// The parts of a `GET` request for `path` on the Gang API, optionally bearing `bearer`, for
/// driving a [`RequestGate`](gang_spi::RequestGate) directly.
///
/// # Panics
///
/// Panics when `path` is not a valid request target.
#[must_use]
pub fn request_parts(path: &str, bearer: Option<&AccessToken>) -> http::request::Parts {
    api_request("GET", path, bearer, None).into_parts().0
}

/// Whether the public gate recognises `token` as a caller and lets it through: the gate admits
/// the request on the open Gang API paths, so the answer is read from the [`Caller`] it records.
///
/// # Panics
///
/// Panics when the gate refuses an open path.
pub async fn admits(controller: &Controller, token: &AccessToken) -> bool {
    let mut parts = request_parts(GET_SERVER.template, Some(token));
    controller
        .admit_public(&Peer::default(), &mut parts)
        .await
        .expect("the public gate admits every request on the Gang API");
    matches!(Caller::of(&parts.extensions), Caller::Authenticated(_))
}

/// Trades `proof` from the identity provider `provider` for a bearer token through the
/// controller's `POST /v1/session:authenticate`.
///
/// # Panics
///
/// Panics when the controller does not answer `200 OK` with a token.
pub async fn mint_token(controller: &Controller, provider: &str, proof: &str) -> AccessToken {
    let body = AuthenticateRequest {
        provider: ProviderName::try_new(provider).expect("test provider name is valid"),
        proof: Proof::new(proof),
    };
    let request = api_request(
        "POST",
        AUTHENTICATE_SESSION.template,
        None,
        Some(serde_json::to_value(&body).expect("authenticate request serializes")),
    );
    let reply = read(controller.call_api(request).await).await;
    assert_eq!(reply.status, StatusCode::OK, "minting a token succeeds");
    let response: AuthenticateResponse = reply.json();
    response.token
}

/// The `Authorization` header value that presents `token`.
fn bearer_value(token: &AccessToken) -> HeaderValue {
    HeaderValue::from_str(&format!("Bearer {}", token.expose()))
        .expect("bearer token is a valid header value")
}

/// A request body holding `bytes`.
fn boxed(bytes: Bytes) -> ApiRequestBody {
    Full::new(bytes)
        .map_err(|never: Infallible| -> BoxedError { match never {} })
        .boxed_unsync()
}
