//! Follower forwarding and leader-local dashboard behaviour.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::SystemClock;
use gang_insight::Outcome;
use gang_spi::{ApiService, Authenticated};
use gang_testkit::TestResult;
use gang_tls::{
    HttpClient, HttpError, HttpProtocol, HttpReply, HttpSettings, HttpTransport, RevocationSet,
    TransportError,
};
use gang_types::{NonZeroBytes, NonZeroDuration, PermissionSet, Url};
use gang_wire::routes::{GET_STATE, LIST_BUILDS, WATCH_STATE};
use http::StatusCode;
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::api::{Api, ApiDashboard, ApiParts};
use crate::api_test_support::ADMIN;
use crate::authorizer::Authorizer;
use crate::gate::Caller;
use crate::hub::{HubConfig, StateHub};
use crate::leader_routes::{LeaderRoutes, RELAYED_HEADER, classify};
use crate::leadership::Leading;
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::test_support::{Harness, HarnessOptions, api_request, identity, role_for, tls_settings};

/// Paths of every leader-owned route a follower relays.
const LEADER_PATHS: [&str; 4] = [
    GET_STATE.template,
    WATCH_STATE.template,
    LIST_BUILDS.template,
    "/v1/builds/0123456789abcdef",
];

fn client() -> Result<HttpClient, gang_tls::HttpError> {
    HttpClient::try_new(&HttpSettings {
        transport: HttpTransport::Plain,
        protocol: HttpProtocol::Negotiated,
        timeout: NonZeroDuration::from_secs(5),
        max_body: NonZeroBytes::kib(1),
        clock: Arc::new(SystemClock::default()),
    })
}

fn api(endpoint: Url, leading: Leading) -> TestResult<Api<crate::test_support::TlsDeployment>> {
    let harness = Harness::token(HarnessOptions::default());
    let core = harness.core.clone();
    let info = serde_json::from_value(serde_json::json!({
        "name": "server", "remoteExecutor": "grpcs://gang.example.com:443",
        "remoteCache": "grpcs://gang.example.com:443", "defaultIsa": "x86-64",
        "authentication": {"none": {}}
    }))?;
    let hub = StateHub::new(
        HubConfig {
            max_viewers: NonZeroUsize::MIN,
            retry_after: Duration::from_secs(5),
            lag_frames: NonZeroUsize::MIN,
            recent_events: NonZeroUsize::MIN,
        },
        gang_wire::State::default(),
    );
    Ok(Api::new(ApiParts {
        core: core.clone(),
        revocations: Revocations::new(core.clone(), RevocationSet::from_serials(Vec::new())),
        info,
        authorizer: Authorizer::new(vec![role_for("administrator", PermissionSet::all(), ADMIN)]),
        limits: tls_settings().limits,
        limiter: Arc::new(RateLimiter::new(
            &tls_settings().rate_limit,
            core.host.clock.clone(),
        )),
        dashboard: Some(ApiDashboard {
            hub,
            heartbeat: NonZeroDuration::from_hours(1),
        }),
        leader_routes: Some(LeaderRoutes::new(leading, client()?, endpoint)),
        ingest: None,
        builds: crate::builds::BuildsCell::new(&gang_config::BuildsConfig::default()),
        login_provider: None,
        device: None,
    }))
}

fn request(path: &str) -> http::Request<gang_spi::ApiRequestBody> {
    let mut request = api_request("GET", path, None, None);
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer caller"),
    );
    request.headers_mut().insert(
        http::header::COOKIE,
        http::HeaderValue::from_static("session=caller"),
    );
    request
        .extensions_mut()
        .insert(Caller::Authenticated(Authenticated {
            identity: Arc::new(identity(ADMIN)),
            valid_until: None,
        }));
    request
}

async fn read_head(stream: &mut tokio::net::TcpStream) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await?;
        bytes.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn follower_refuses_a_revoked_caller_before_relaying() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let api = api(endpoint, Leading::default())?;
    let mut request = request(LIST_BUILDS.template);
    request
        .extensions_mut()
        .insert(Caller::Refused(Arc::new(gang_spi::PortError::refused(
            gang_spi::Rejection::PrincipalRevoked,
        ))));
    let mut call = Box::pin(api.call(request));
    let response = match futures::poll!(call.as_mut()) {
        std::task::Poll::Ready(response) => response,
        std::task::Poll::Pending => panic!("revoked caller reached the relay"),
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(gang_testkit::ready_now(listener.accept()).is_none());
    Ok(())
}

#[tokio::test]
async fn follower_rejects_an_already_relayed_request() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let api = api(endpoint, Leading::default())?;
    let mut request = request(LIST_BUILDS.template);
    request
        .headers_mut()
        .insert(RELAYED_HEADER, http::HeaderValue::from_static("1"));
    let mut call = Box::pin(api.call(request));
    let response = match futures::poll!(call.as_mut()) {
        std::task::Poll::Ready(response) => response,
        std::task::Poll::Pending => panic!("relayed request reached the relay"),
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(gang_testkit::ready_now(listener.accept()).is_none());
    Ok(())
}

#[tokio::test]
async fn follower_forwards_leader_routes_and_credentials() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let server = tokio::spawn(async move {
        for path in LEADER_PATHS {
            let (mut stream, _) = listener.accept().await?;
            let head = read_head(&mut stream).await?;
            assert!(head.starts_with(&format!("GET {path}?test=1 ")));
            assert!(
                head.to_ascii_lowercase()
                    .contains("authorization: bearer caller\r\n")
            );
            assert!(
                head.to_ascii_lowercase()
                    .contains("cookie: session=caller\r\n")
            );
            assert!(
                head.to_ascii_lowercase()
                    .contains(&format!("{RELAYED_HEADER}: 1\r\n"))
            );
            stream
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let api = api(endpoint, Leading::default())?;
    for path in LEADER_PATHS {
        let response = api.call(request(&format!("{path}?test=1"))).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            &response
                .into_body()
                .collect()
                .await
                .map_err(std::io::Error::other)?
                .to_bytes()[..],
            b"hello"
        );
    }
    server.await??;
    Ok(())
}

#[tokio::test]
async fn follower_relays_sse_before_upstream_ends() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let (finish, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        read_head(&mut stream).await?;
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n9\r\ndata: 1\n\n\r\n").await?;
        let _ = wait.await;
        stream.write_all(b"0\r\n\r\n").await
    });
    let api = api(endpoint, Leading::default())?;
    let mut body = api.call(request(WATCH_STATE.template)).await.into_body();
    let frame = body
        .frame()
        .await
        .ok_or("missing event")?
        .map_err(std::io::Error::other)?;
    assert_eq!(
        &frame.into_data().map_err(|_| "not data")?[..],
        b"data: 1\n\n"
    );
    finish.send(()).map_err(|()| "upstream stopped")?;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn leader_serves_local_state_without_connecting() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let leading = Leading::default();
    leading.set(gang_clock::Timestamp::UNIX_EPOCH);
    let api = api(endpoint, leading)?;
    let response = api.call(request(GET_STATE.template)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let state: gang_wire::State = serde_json::from_slice(
        &response
            .into_body()
            .collect()
            .await
            .map_err(std::io::Error::other)?
            .to_bytes(),
    )?;
    assert_eq!(state.name, gang_wire::State::default().name);
    assert!(gang_testkit::ready_now(listener.accept()).is_none());
    Ok(())
}

#[tokio::test]
async fn unavailable_leader_returns_unavailable_error_envelope() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    drop(listener);
    let api = api(endpoint, Leading::default())?;
    let response = api.call(request(GET_STATE.template)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: gang_wire::ErrorResponse = serde_json::from_slice(
        &response
            .into_body()
            .collect()
            .await
            .map_err(std::io::Error::other)?
            .to_bytes(),
    )?;
    assert_eq!(
        error.error,
        gang_wire::ErrorReason::DependencyUnavailable.into()
    );
    Ok(())
}

#[test]
fn ingest_reply_and_error_classification() {
    for status in [StatusCode::OK, StatusCode::CREATED, StatusCode::NO_CONTENT] {
        assert_eq!(
            classify(Ok(HttpReply {
                status,
                headers: http::HeaderMap::new(),
                body: bytes::Bytes::new(),
            })),
            Outcome::Accepted
        );
    }
    for status in [
        StatusCode::MULTIPLE_CHOICES,
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        assert_eq!(
            classify(Ok(HttpReply {
                status,
                headers: http::HeaderMap::new(),
                body: bytes::Bytes::new(),
            })),
            Outcome::NotApplied
        );
    }
    let request_error = http::Request::builder()
        .method("invalid method")
        .body(())
        .expect_err("a method containing a space is invalid");
    for error in [
        HttpError::UnsupportedScheme {
            scheme: "ftp".to_owned(),
        },
        HttpError::Request(request_error),
        HttpError::Tls(TransportError::HandshakeTimeout),
    ] {
        assert_eq!(classify(Err(error)), Outcome::NotApplied);
    }
    for error in [
        HttpError::TimedOut,
        HttpError::Body(Box::new(std::io::Error::other("response interrupted"))),
        HttpError::BodyTooLarge {
            status: StatusCode::OK,
            limit: NonZeroBytes::kib(1),
        },
    ] {
        assert_eq!(classify(Err(error)), Outcome::Unknown);
    }
}
