//! Rate limiting of the routes that are open to anyone, and the client address the public gate
//! records for them, driven through a running server on the injected clock.

use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::ServerSettings;
use crate::client_address::ClientAddress;
use gang_config::DashboardConfig;
use gang_spi::{ApiRequestBody, LoginClient, Peer};
use gang_testkit::Controller;
use gang_types::{AccessToken, ClientSecret, NonZeroDuration, PermissionSet, ProviderName};
use gang_wire::routes::{AUTHENTICATE_SESSION, GET_SERVER, GET_STATE, LOGIN_PATH};
use gang_wire::{Code, ErrorReason, ErrorResponse};
use http::header::{CACHE_CONTROL, RETRY_AFTER};
use http::request::Parts;
use http::{Request, StatusCode};
use serde_json::{Value, json};

use crate::test_support::{
    ApiReply, api_request, mint_token, plain_parts, plain_settings, read, request_with_header,
    role_for, serve, tls_parts, tls_settings,
};

const PROVIDER: &str = "example-idp";
const ADMIN: &str = "admin";
const ADMIN_PROOF: &str = "admin-proof";
const ALICE_PROOF: &str = "alice-proof";

/// The burst of the global bucket of the open routes.
const GLOBAL_BURST: u32 = 2;

/// The refill interval of the global bucket of the open routes, in whole seconds.
const REFILL_SECONDS: u64 = 10;

/// The burst of the client bucket: generous enough that only the global bucket binds.
const CLIENT_BURST: u32 = 1_000;

/// The header a proxy appends the client address to.
const FORWARDED_FOR: &str = "x-forwarded-for";

/// The address of the client a proxy forwards.
const FORWARDED_CLIENT: &str = "203.0.113.7";

/// The address a client claims through `X-Forwarded-For` when no proxy is trusted.
const SPOOFED_CLIENT: &str = "198.51.100.9";

/// The address of a client that dials the server directly.
const CLIENT: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));

/// The address of a trusted proxy, inside `10.0.0.0/8`.
const PROXY: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3));

/// The rate limit of the open routes: a global bucket admitting [`GLOBAL_BURST`] requests at once
/// and earning one back every [`REFILL_SECONDS`], and a client bucket that never binds.
fn global_bucket_of_two() -> Value {
    json!({
        "client": { "burst": CLIENT_BURST, "refill_interval": "1s" },
        "global": { "burst": GLOBAL_BURST, "refill_interval": format!("{REFILL_SECONDS}s") },
    })
}

/// A rate limit that trusts the proxies of `10.0.0.0/8`.
fn trusting_ten_slash_eight() -> Value {
    json!({ "trusted_proxies": ["10.0.0.0/8"] })
}

/// `settings` with its rate limit parsed from `rate_limit`.
fn with_rate_limit(settings: ServerSettings, rate_limit: Value) -> ServerSettings {
    ServerSettings {
        rate_limit: serde_json::from_value(rate_limit)
            .expect("the rate limit configuration is valid"),
        ..settings
    }
}

/// Settings with an interactive login and a single-request bucket.
fn login_settings(enabled: bool) -> ServerSettings {
    ServerSettings {
        dashboard: DashboardConfig {
            enabled,
            ..DashboardConfig::default()
        },
        login: Some(crate::LoginSettings {
            provider: ProviderName::try_new(PROVIDER).expect("provider name is valid"),
            client: LoginClient {
                id: "web-client".to_owned(),
                secret: ClientSecret::new("secret"),
            },
            ttl: NonZeroDuration::from_mins(10),
            max_pending: NonZeroUsize::new(8).expect("8 is nonzero"),
        }),
        ..with_rate_limit(
            tls_settings(),
            json!({
                "client": { "burst": 1, "refill_interval": "10s" },
                "global": { "burst": 100, "refill_interval": "10s" }
            }),
        )
    }
}

#[tokio::test]
async fn disabled_dashboard_does_not_advertise_or_charge_login() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        login_settings(false),
        handles.scheduler,
        |controller| async move {
            let session = get(&controller, "/v1/session").await;
            assert_eq!(session.status, StatusCode::OK);
            let body: Value = serde_json::from_slice(&session.body).expect("session is JSON");
            assert!(body.get("login").is_none());

            let login = get(&controller, LOGIN_PATH).await;
            assert_eq!(login.status, StatusCode::UNAUTHORIZED);
            let error: ErrorResponse = serde_json::from_slice(&login.body).expect("API error");
            assert_eq!(error.error.reason(), Some(ErrorReason::CredentialsMissing));
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::OK
            );
            assert_eq!(
                get(&controller, LOGIN_PATH).await.status,
                StatusCode::UNAUTHORIZED
            );
        },
    )
    .await;
}

#[tokio::test]
async fn unmounted_authentication_does_not_spend_the_bucket() {
    let (parts, _dials, handles) = plain_parts();
    let settings = with_rate_limit(
        plain_settings(),
        json!({
            "client": { "burst": 1, "refill_interval": "10s" },
            "global": { "burst": 100, "refill_interval": "10s" }
        }),
    );
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let request = api_request("POST", AUTHENTICATE_SESSION.template, None, None);
            assert_eq!(
                send(&controller, request).await.status,
                StatusCode::NOT_FOUND
            );
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::OK
            );
            let request = api_request("POST", AUTHENTICATE_SESSION.template, None, None);
            assert_eq!(
                send(&controller, request).await.status,
                StatusCode::NOT_FOUND
            );
        },
    )
    .await;
}

/// Sends `request` through the controller's public gate, then to the API, and reads the reply.
async fn send(controller: &Controller, request: Request<ApiRequestBody>) -> ApiReply {
    let (mut parts, body) = request.into_parts();
    let peer = Peer {
        address: Some(CLIENT),
        ..Peer::default()
    };
    controller
        .admit_public(&peer, &mut parts)
        .await
        .expect("the public gate admits the request");
    read(controller.call_api(Request::from_parts(parts, body)).await).await
}

/// A `GET` of `path` without credentials.
async fn get(controller: &Controller, path: &str) -> ApiReply {
    send(controller, api_request("GET", path, None, None)).await
}

/// A `GET` of `path` as the holder of `token`.
async fn get_as(controller: &Controller, path: &str, token: &AccessToken) -> ApiReply {
    send(controller, api_request("GET", path, Some(token), None)).await
}

/// The seconds of the `Retry-After` header of `reply`.
fn retry_after(reply: &ApiReply) -> u64 {
    reply
        .headers
        .get(RETRY_AFTER)
        .expect("the answer carries Retry-After")
        .to_str()
        .expect("Retry-After is visible ASCII")
        .parse()
        .expect("Retry-After is whole seconds")
}

/// The client address the public gate records for a `GET` of the open server description that
/// carries `X-Forwarded-For: forwarded` from `peer`.
async fn recorded_address(controller: &Controller, peer: &Peer, forwarded: &str) -> Option<IpAddr> {
    let mut parts: Parts =
        request_with_header("GET", GET_SERVER.template, &[], FORWARDED_FOR, forwarded)
            .into_parts()
            .0;
    controller
        .admit_public(peer, &mut parts)
        .await
        .expect("the public gate admits the open server description");
    parts
        .extensions
        .get::<ClientAddress>()
        .map(|address| address.0)
}

#[tokio::test]
async fn open_route_refusal_carries_retry_info_and_recovers_on_the_clock() {
    let (parts, _dials, handles) = plain_parts();
    let clock = handles.clock.clone();
    let settings = with_rate_limit(plain_settings(), global_bucket_of_two());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            for _ in 0..GLOBAL_BURST {
                assert_eq!(
                    get(&controller, GET_SERVER.template).await.status,
                    StatusCode::OK
                );
            }

            clock.advance(Duration::from_millis(500));
            let limited = get(&controller, GET_SERVER.template).await;
            assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(limited.headers[CACHE_CONTROL], "no-store");
            let wait = retry_after(&limited);
            assert_eq!(wait, REFILL_SECONDS, "the next token is one refill away");

            let envelope: ErrorResponse =
                serde_json::from_slice(&limited.body).expect("the refusal is an error envelope");
            assert_eq!(envelope.error.status, Code::ResourceExhausted);
            assert_eq!(envelope.error.reason(), Some(ErrorReason::RateLimited));
            assert_eq!(
                envelope.error.retry_delay(),
                Some(Duration::from_secs(wait)),
                "RetryInfo carries the Retry-After delay"
            );

            clock.advance(Duration::from_secs(wait));
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::OK
            );
        },
    )
    .await;
}

#[tokio::test]
async fn authenticate_shares_the_open_route_bucket() {
    let (parts, _dials, handles) = tls_parts();
    let settings = with_rate_limit(tls_settings(), global_bucket_of_two());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::OK
            );

            let authenticate = api_request(
                "POST",
                AUTHENTICATE_SESSION.template,
                None,
                Some(json!({ "provider": PROVIDER, "proof": ALICE_PROOF })),
            );
            let answered = send(&controller, authenticate).await;
            assert_ne!(
                answered.status,
                StatusCode::TOO_MANY_REQUESTS,
                "the second open-route request is still admitted"
            );

            let limited = get(&controller, GET_SERVER.template).await;
            assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
        },
    )
    .await;
}

#[tokio::test]
async fn closed_routes_are_served_while_the_open_route_bucket_is_empty() {
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        roles: vec![role_for("administrator", PermissionSet::all(), ADMIN)],
        ..with_rate_limit(tls_settings(), global_bucket_of_two())
    };
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            // The token exchange is an open route too, so it spends one of the two tokens.
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::OK
            );
            assert_eq!(
                get(&controller, GET_SERVER.template).await.status,
                StatusCode::TOO_MANY_REQUESTS,
                "the open-route bucket is empty"
            );

            for _ in 0..=GLOBAL_BURST {
                assert_eq!(
                    get_as(&controller, GET_STATE.template, &admin).await.status,
                    StatusCode::OK,
                    "the state is served while the open-route bucket is empty"
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn login_page_is_refused_with_retry_after_when_the_bucket_is_empty() {
    let (parts, _dials, handles) = tls_parts();
    let settings = with_rate_limit(login_settings(true), global_bucket_of_two());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            for _ in 0..GLOBAL_BURST {
                assert_eq!(
                    get(&controller, GET_SERVER.template).await.status,
                    StatusCode::OK
                );
            }

            for path in [LOGIN_PATH, gang_wire::CALLBACK_PATH] {
                let login = get(&controller, path).await;
                assert_eq!(login.status, StatusCode::TOO_MANY_REQUESTS);
                assert!(login.body.is_empty(), "the login refusal has no body");
                assert_eq!(login.headers[CACHE_CONTROL], "no-store");
                assert_eq!(retry_after(&login), REFILL_SECONDS);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn public_gate_records_the_peer_address_and_ignores_forwarded_for_without_trusted_proxies() {
    let (parts, _dials, handles) = plain_parts();
    serve(
        parts,
        plain_settings(),
        handles.scheduler,
        |controller| async move {
            let peer = Peer {
                address: Some(CLIENT),
                ..Peer::default()
            };
            let recorded = recorded_address(&controller, &peer, SPOOFED_CLIENT).await;
            assert_eq!(recorded, Some(CLIENT));
        },
    )
    .await;
}

#[tokio::test]
async fn public_gate_records_the_forwarded_client_behind_a_trusted_proxy() {
    let (parts, _dials, handles) = plain_parts();
    let settings = with_rate_limit(plain_settings(), trusting_ten_slash_eight());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let peer = Peer {
                address: Some(PROXY),
                ..Peer::default()
            };
            let recorded = recorded_address(&controller, &peer, FORWARDED_CLIENT).await;
            assert_eq!(recorded, Some(CLIENT));
        },
    )
    .await;
}

#[tokio::test]
async fn public_gate_ignores_forwarded_for_from_an_untrusted_peer() {
    let (parts, _dials, handles) = plain_parts();
    let settings = with_rate_limit(plain_settings(), trusting_ten_slash_eight());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let peer = Peer {
                address: Some(CLIENT),
                ..Peer::default()
            };
            let recorded = recorded_address(&controller, &peer, SPOOFED_CLIENT).await;
            assert_eq!(recorded, Some(CLIENT));
        },
    )
    .await;
}

#[tokio::test]
async fn public_gate_records_no_address_when_the_peer_has_none() {
    let (parts, _dials, handles) = plain_parts();
    let settings = with_rate_limit(plain_settings(), trusting_ten_slash_eight());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let recorded = recorded_address(&controller, &Peer::default(), FORWARDED_CLIENT).await;
            assert_eq!(recorded, None);
        },
    )
    .await;
}

#[tokio::test]
async fn worker_gate_records_no_client_address() {
    let (parts, _dials, handles) = tls_parts();
    let settings = with_rate_limit(tls_settings(), trusting_ten_slash_eight());
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let peer = Peer {
                address: Some(PROXY),
                ..Peer::default()
            };
            let mut parts = request_with_header(
                "GET",
                GET_SERVER.template,
                &[],
                FORWARDED_FOR,
                FORWARDED_CLIENT,
            )
            .into_parts()
            .0;
            let _outcome = controller.admit_worker(&peer, &mut parts).await;
            assert!(
                parts.extensions.get::<ClientAddress>().is_none(),
                "the worker surface records no client address"
            );
        },
    )
    .await;
}
