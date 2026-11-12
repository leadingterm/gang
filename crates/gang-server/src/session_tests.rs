//! The browser routes under `/ui` (`/ui/login`, `/ui/callback`, `/ui/logout`), driven through the
//! public gate and the routes.

use std::num::NonZeroUsize;

use crate::LoginSettings;
use gang_spi::LoginClient;
use gang_types::{ClientSecret, NonZeroDuration, ProviderName};
use gang_wire::{CALLBACK_PATH, LOGIN_PATH, LOGOUT_PATH, UiRoute};
use http::StatusCode;
use http::header::{ALLOW, CACHE_CONTROL, HeaderName};

use crate::test_support::{
    ApiReply, Harness, HarnessOptions, PlainDeployment, TlsDeployment, read, request,
    request_with_header,
};

const PROVIDER: &str = "example-idp";
const SECRET: &str = "dashboard-secret-value";
const EXPECTED_CLEAR: &str =
    "__Host-gang_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
const EXPECTED_HTTP_CLEAR: &str = "gang_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";

fn provider() -> ProviderName {
    ProviderName::try_new(PROVIDER).expect("provider name is valid")
}

/// Sends `method` for `path` with `cookies` through the harness and reads the response.
async fn reply_to(
    harness: &Harness<TlsDeployment>,
    method: &str,
    path: &str,
    cookies: &[(&str, &str)],
) -> ApiReply {
    let response = harness
        .send(request(method, path, cookies))
        .await
        .expect("the gate admits the request");
    read(response).await
}

/// The text of the header `name` of `reply`.
fn header(reply: &ApiReply, name: HeaderName) -> &str {
    reply
        .headers
        .get(name)
        .expect("the header is set")
        .to_str()
        .expect("the header is visible ascii")
}

/// Whether `reply` carries the secret in its body or in any header value.
fn mentions_secret(reply: &ApiReply) -> bool {
    reply.text().contains(SECRET)
        || reply
            .headers
            .values()
            .any(|value| value.to_str().is_ok_and(|text| text.contains(SECRET)))
}

#[tokio::test]
async fn get_of_logout_is_method_not_allowed() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    let get_logout = reply_to(&harness, "GET", LOGOUT_PATH, &[]).await;
    assert_eq!(get_logout.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(header(&get_logout, ALLOW), "POST");
    assert_eq!(header(&get_logout, CACHE_CONTROL), "no-store");
    assert!(get_logout.body.is_empty());
}

const SAME_ORIGIN: Option<(&str, &str)> = Some(("sec-fetch-site", "same-origin"));

/// Sends `POST /ui/logout` with `cookies` and the optional extra `header` through the harness.
async fn post_logout(
    harness: &Harness<TlsDeployment>,
    cookies: &[(&str, &str)],
    header: Option<(&str, &str)>,
) -> ApiReply {
    let request = match header {
        Some((name, value)) => request_with_header("POST", LOGOUT_PATH, cookies, name, value),
        None => request("POST", LOGOUT_PATH, cookies),
    };
    let response = harness
        .send(request)
        .await
        .expect("the gate admits the request");
    read(response).await
}

#[tokio::test]
async fn logout_clears_the_session_cookie_with_or_without_a_session() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    let bare = post_logout(&harness, &[], SAME_ORIGIN).await;
    assert_eq!(bare.status, StatusCode::NO_CONTENT);
    assert_eq!(header(&bare, CACHE_CONTROL), "no-store");
    assert_eq!(bare.set_cookies(), vec![EXPECTED_CLEAR.to_owned()]);

    let cookie = harness.signed_in_cookie("admin-proof").await;
    let cookies = [(harness.core.cookies.session, cookie.as_str())];
    let signed_in = post_logout(&harness, &cookies, SAME_ORIGIN).await;
    assert_eq!(signed_in.status, StatusCode::NO_CONTENT);
    assert_eq!(header(&signed_in, CACHE_CONTROL), "no-store");
    assert_eq!(signed_in.set_cookies(), vec![EXPECTED_CLEAR.to_owned()]);
}

#[tokio::test]
async fn logout_from_the_endpoint_origin_clears_the_session_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = post_logout(&harness, &[], Some(("origin", "https://gang.example.test"))).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(header(&reply, CACHE_CONTROL), "no-store");
    assert_eq!(reply.set_cookies(), vec![EXPECTED_CLEAR.to_owned()]);
}

#[tokio::test]
async fn logout_from_a_foreign_origin_is_refused_without_clearing_the_session() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = harness.signed_in_cookie("admin-proof").await;
    let cookies = [(harness.core.cookies.session, cookie.as_str())];
    let reply = post_logout(&harness, &cookies, Some(("origin", "https://evil.example"))).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(header(&reply, CACHE_CONTROL), "no-store");
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn logout_refuses_cross_site_and_same_site_fetches_without_a_matching_origin() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    for site in ["cross-site", "same-site"] {
        let reply = post_logout(&harness, &[], Some(("sec-fetch-site", site))).await;
        assert_eq!(
            reply.status,
            StatusCode::FORBIDDEN,
            "{site} logout is refused"
        );
        assert_eq!(header(&reply, CACHE_CONTROL), "no-store");
        assert!(reply.set_cookies().is_empty());
    }
}

#[tokio::test]
async fn logout_without_origin_or_fetch_metadata_is_refused() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = post_logout(&harness, &[], None).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(header(&reply, CACHE_CONTROL), "no-store");
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn logout_refuses_an_origin_that_differs_in_scheme_or_port() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    for origin in ["http://gang.example.test", "https://gang.example.test:8443"] {
        let reply = post_logout(&harness, &[], Some(("origin", origin))).await;
        assert_eq!(
            reply.status,
            StatusCode::FORBIDDEN,
            "{origin} logout is refused"
        );
        assert!(reply.set_cookies().is_empty());
    }
}

#[tokio::test]
async fn http_endpoint_logout_accepts_its_own_origin_and_same_origin_fetches() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions {
        endpoint: "http://gang.example.test",
        ..HarnessOptions::default()
    });

    let by_origin = post_logout(&harness, &[], Some(("origin", "http://gang.example.test"))).await;
    assert_eq!(by_origin.status, StatusCode::NO_CONTENT);
    assert_eq!(
        by_origin.set_cookies(),
        vec![EXPECTED_HTTP_CLEAR.to_owned()]
    );

    let by_fetch = post_logout(&harness, &[], SAME_ORIGIN).await;
    assert_eq!(by_fetch.status, StatusCode::NO_CONTENT);
    assert_eq!(by_fetch.set_cookies(), vec![EXPECTED_HTTP_CLEAR.to_owned()]);

    let foreign = post_logout(&harness, &[], Some(("origin", "https://gang.example.test"))).await;
    assert_eq!(foreign.status, StatusCode::FORBIDDEN);
    assert!(foreign.set_cookies().is_empty());
}

#[tokio::test]
async fn unmounted_login_routes_return_not_found_for_all_methods() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions {
        login: false,
        ..HarnessOptions::default()
    });
    for path in [LOGIN_PATH, CALLBACK_PATH] {
        for method in ["GET", "POST"] {
            let reply = reply_to(&harness, method, path, &[]).await;
            assert_eq!(reply.status, StatusCode::NOT_FOUND, "{method} {path}");
            assert!(reply.body.is_empty(), "{method} {path}");
        }
    }
}

#[tokio::test]
async fn anonymous_clients_do_not_mount_the_login_routes() {
    let harness = Harness::<PlainDeployment>::anonymous(HarnessOptions::default());
    for route in [UiRoute::Login, UiRoute::Callback] {
        let response = harness
            .routes
            .answer(route, request("GET", route.path(), &[]))
            .await;
        let reply = read(response).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.is_empty());
    }
}

#[tokio::test]
async fn the_client_secret_appears_in_no_sign_in_response_or_settings_debug() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    let begun = reply_to(&harness, "GET", LOGIN_PATH, &[]).await;
    assert!(!mentions_secret(&begun));

    let logout = post_logout(&harness, &[], SAME_ORIGIN).await;
    assert!(!mentions_secret(&logout));

    let settings = LoginSettings {
        provider: provider(),
        client: LoginClient {
            id: "web-client".to_owned(),
            secret: ClientSecret::new(SECRET),
        },
        ttl: NonZeroDuration::from_mins(10),
        max_pending: NonZeroUsize::new(8).expect("8 is non-zero"),
    };
    assert!(!format!("{settings:?}").contains(SECRET));
}
