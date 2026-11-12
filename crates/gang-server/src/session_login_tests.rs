//! Behavioural tests of the browser sign-in: `/ui/login`, `/ui/callback` and the session it issues.

use std::time::Duration;

use crate::gate::Caller;
use gang_spi::{Peer, Rejection, RequestGate};
use gang_types::{Identity, Principal, ProviderName};
use gang_wire::{ERROR_PATH, routes::GET_SESSION};
use http::StatusCode;
use http::header::CACHE_CONTROL;

use crate::test_support::{
    ApiReply, BeginFailure, Harness, HarnessOptions, TlsDeployment, read, request,
    request_with_header,
};

/// The session cookie name on an `https` endpoint.
const SESSION_COOKIE: &str = "__Host-gang_session";

/// The sign-in cookie name on an `https` endpoint.
const LOGIN_COOKIE: &str = "__Host-gang_login";

/// The lifetime of a pending sign-in and of the session it issues: 10 minutes.
const TTL: Duration = Duration::from_mins(10);

/// The client secret the harness configures; it must never leave the server.
const SECRET: &str = "dashboard-secret-value";

/// The `Set-Cookie` header that stores `state` as the sign-in cookie.
fn login_set_cookie(state: &str) -> String {
    format!("{LOGIN_COOKIE}={state}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=600")
}

/// The `Set-Cookie` header that clears the sign-in cookie.
fn cleared_login_cookie() -> String {
    format!("{LOGIN_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0")
}

/// The `Set-Cookie` header that stores `token` as the session cookie.
fn session_set_cookie(token: &str) -> String {
    format!("{SESSION_COOKIE}={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=600")
}

/// Sends `request` through the harness and reads the whole reply.
async fn exchange(
    harness: &Harness<TlsDeployment>,
    request: http::Request<gang_spi::ApiRequestBody>,
) -> ApiReply {
    let response = harness
        .send(request)
        .await
        .expect("the gate admits the request");
    read(response).await
}

/// Starts a sign-in at `path` and returns its state, read from the authorization URL.
async fn start(harness: &Harness<TlsDeployment>, path: &str) -> String {
    let begun = exchange(harness, request("GET", path, &[])).await;
    assert_eq!(begun.status, StatusCode::FOUND, "sign-in redirects");
    state_of(&begun)
}

/// Starts a sign-in and returns its state.
async fn begin(harness: &Harness<TlsDeployment>) -> String {
    start(harness, "/ui/login").await
}

/// Sends the callback with the query `query`, carrying the sign-in cookie `cookie` when given.
async fn callback(harness: &Harness<TlsDeployment>, query: &str, cookie: Option<&str>) -> ApiReply {
    let cookies: Vec<(&str, &str)> = cookie
        .map(|value| (LOGIN_COOKIE, value))
        .into_iter()
        .collect();
    exchange(
        harness,
        request("GET", &format!("/ui/callback?{query}"), &cookies),
    )
    .await
}

/// Completes the sign-in `state` with `admin-proof`, carrying its own sign-in cookie.
async fn finish(harness: &Harness<TlsDeployment>, state: &str) -> ApiReply {
    callback(
        harness,
        &format!("state={state}&code=admin-proof"),
        Some(state),
    )
    .await
}

/// The state of a sign-in, read from the `state` parameter of its authorization URL.
fn state_of(reply: &ApiReply) -> String {
    let location = reply
        .location()
        .expect("sign-in redirects to the identity provider");
    let url = gang_types::Url::parse(&location).expect("the authorization URL is absolute");
    url.query_pairs()
        .find(|(name, _)| name == "state")
        .map(|(_, value)| value.into_owned())
        .expect("the authorization URL carries a state")
}

/// Asserts that `reply` is an uncached answer.
fn assert_no_store(reply: &ApiReply) {
    assert_eq!(
        reply
            .headers
            .get(CACHE_CONTROL)
            .map(http::HeaderValue::as_bytes),
        Some(b"no-store".as_slice())
    );
}

/// Asserts that `reply` sent the browser to the error page with `reason`, issued no session and
/// cleared the sign-in cookie.
fn assert_failed(reply: &ApiReply, reason: &str) {
    assert_eq!(reply.status, StatusCode::FOUND);
    assert_no_store(reply);
    assert_eq!(
        reply.location(),
        Some(format!("{ERROR_PATH}?reason={reason}"))
    );
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_none());
    assert!(reply.set_cookies().contains(&cleared_login_cookie()));
}

/// Asserts that `reply` completed a sign-in and redirected to `next`.
fn assert_signed_in(reply: &ApiReply, next: &str) {
    assert_eq!(reply.status, StatusCode::FOUND);
    assert_no_store(reply);
    assert_eq!(reply.location().as_deref(), Some(next));
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_some());
    assert!(reply.set_cookies().contains(&cleared_login_cookie()));
}

/// Asserts that `reply` contains no client secret in its headers or body.
fn assert_no_secret(reply: &ApiReply) {
    for value in reply.headers.values() {
        assert!(
            !String::from_utf8_lossy(value.as_bytes()).contains(SECRET),
            "a response header carries the client secret"
        );
    }
    assert!(
        !reply.text().contains(SECRET),
        "a response body carries the client secret"
    );
}

#[tokio::test]
async fn sign_in_redirects_to_the_provider_and_issues_a_session() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    let begun = exchange(&harness, request("GET", "/ui/login?next=/ui/workers", &[])).await;
    assert_eq!(begun.status, StatusCode::FOUND);
    assert_no_store(&begun);
    let state = state_of(&begun);
    let location = begun.location().expect("sign-in redirects");
    assert!(location.starts_with("https://idp.example.test/authorize?"));
    assert_eq!(begun.set_cookies(), vec![login_set_cookie(&state)]);

    let seen = harness.verifier.begun();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].redirect_uri.as_str(),
        "https://gang.example.test/ui/callback"
    );
    assert_eq!(
        seen[0].client,
        Some(("web-client".to_owned(), SECRET.to_owned()))
    );

    let done = finish(&harness, &state).await;
    assert_signed_in(&done, "/ui/workers");
    let token = done
        .set_cookie_value(SESSION_COOKIE)
        .expect("callback sets the session cookie");
    assert!(done.set_cookies().contains(&session_set_cookie(&token)));

    let cookies = [(SESSION_COOKIE, token.as_str())];
    let (mut parts, _) = request("GET", GET_SESSION.template, &cookies).into_parts();
    harness
        .gate
        .admit(&Peer::default(), &mut parts)
        .await
        .expect("the gate admits the session cookie");
    let authenticated = Caller::of(&parts.extensions)
        .as_authenticated()
        .expect("the token authenticates the caller");
    assert_eq!(authenticated.identity.provider.as_str(), "example-idp");
    assert_eq!(authenticated.identity.principal.as_str(), "admin");
}

#[tokio::test]
async fn callback_url_is_rebuilt_from_the_endpoint_not_the_host_header() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    let done = exchange(
        &harness,
        request_with_header(
            "GET",
            &format!("/ui/callback?state={state}&code=admin-proof"),
            &[(LOGIN_COOKIE, state.as_str())],
            "host",
            "evil.example",
        ),
    )
    .await;
    assert_signed_in(&done, "/ui");

    let completed = harness.verifier.completed();
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0].as_str(),
        format!("https://gang.example.test/ui/callback?state={state}&code=admin-proof")
    );
}

#[tokio::test]
async fn mismatched_or_missing_sign_in_cookie_fails_without_consuming_the_attempt() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    let mismatched = callback(
        &harness,
        "state=state-forged&code=admin-proof",
        Some(state.as_str()),
    )
    .await;
    assert_failed(&mismatched, "failed");

    let absent = callback(&harness, &format!("state={state}&code=admin-proof"), None).await;
    assert_failed(&absent, "failed");

    let done = finish(&harness, &state).await;
    assert_signed_in(&done, "/ui");
}

#[tokio::test]
async fn completed_sign_in_cannot_be_replayed() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    assert_signed_in(&finish(&harness, &state).await, "/ui");
    assert_failed(&finish(&harness, &state).await, "failed");
}

#[tokio::test]
async fn sign_in_older_than_the_ttl_fails() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    harness.clock.advance(TTL + Duration::from_secs(1));

    assert_failed(&finish(&harness, &state).await, "failed");
}

#[tokio::test]
async fn denied_callback_reports_denied() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    let denied = callback(
        &harness,
        &format!("state={state}&error=access_denied"),
        Some(state.as_str()),
    )
    .await;
    assert_failed(&denied, "denied");
}

#[tokio::test]
async fn unavailable_provider_at_begin_redirects_to_error_without_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    harness.verifier.fail_begin(BeginFailure::Unavailable);

    let reply = exchange(&harness, request("GET", "/ui/login", &[])).await;

    assert_eq!(reply.status, StatusCode::FOUND);
    assert_no_store(&reply);
    assert_eq!(
        reply.location(),
        Some(format!("{ERROR_PATH}?reason=unavailable"))
    );
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn policy_denied_at_begin_redirects_to_denied_without_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    harness
        .verifier
        .fail_begin(BeginFailure::Rejected(Rejection::PolicyDenied));

    let reply = exchange(&harness, request("GET", "/ui/login", &[])).await;

    assert_eq!(reply.status, StatusCode::FOUND);
    assert_no_store(&reply);
    assert_eq!(
        reply.location(),
        Some(format!("{ERROR_PATH}?reason=denied"))
    );
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn principal_revoked_before_the_callback_is_denied() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    let provider = ProviderName::try_new("example-idp").expect("provider name is valid");
    let admin = Principal::try_new("admin").expect("principal name is valid");
    harness
        .core
        .host
        .revoked_principals
        .revoke(Identity::new(provider, admin));

    assert_failed(&finish(&harness, &state).await, "denied");
}

#[tokio::test]
async fn unknown_proof_fails() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;

    let reply = callback(
        &harness,
        &format!("state={state}&code=bogus"),
        Some(state.as_str()),
    )
    .await;
    assert_failed(&reply, "failed");
}

#[tokio::test]
async fn non_ascii_next_falls_back_without_losing_the_session() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = start(&harness, "/ui/login?next=/ui/caf%C3%A9").await;

    assert_signed_in(&finish(&harness, &state).await, "/ui");
}

#[tokio::test]
async fn next_is_kept_only_when_it_is_a_safe_ui_path() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cases = [
        // Kept.
        ("/ui/login?next=%2Fui", "/ui"),
        ("/ui/login?next=%2Fui%2Fworkers%2Fw1", "/ui/workers/w1"),
        ("/ui/login?next=%2Fui%3Ftab%3D1", "/ui?tab=1"),
        ("/ui/login?next=%2Fui%2Fa.b", "/ui/a.b"),
        ("/ui/login?next=%2Fui%2F%252ea", "/ui/%2ea"),
        // Replaced by `/ui`.
        ("/ui/login", "/ui"),
        ("/ui/login?next=%2F%2Fevil.example", "/ui"),
        ("/ui/login?next=https%3A%2F%2Fevil.example", "/ui"),
        ("/ui/login?next=%2Fv1%2Fstate", "/ui"),
        ("/ui/login?next=%2Fui%2F%2Fx", "/ui"),
        ("/ui/login?next=%2Fuix", "/ui"),
        ("/ui/login?next=%2Fui%2F..%2Fv1", "/ui"),
        ("/ui/login?next=%2Fui%2F%252e%252e%2Fv1", "/ui"),
        ("/ui/login?next=%2Fui%2F.%252E%2Fv1", "/ui"),
        ("/ui/login?next=%2Fui%2F%252E.%2Fv1", "/ui"),
        ("/ui/login?next=%2Fui%2F%252E%252e", "/ui"),
        ("/ui/login?next=%2Fui%0d%0ax", "/ui"),
        ("/ui/login?next=%2Fui%2Fx%5Cy", "/ui"),
        ("/ui/login?next=%2Fui%2Fhello%20world", "/ui"),
    ];
    for (login_path, expected) in cases {
        let state = start(&harness, login_path).await;
        let reply = finish(&harness, &state).await;
        assert_eq!(
            reply.location().as_deref(),
            Some(expected),
            "next from {login_path}"
        );
    }
}

#[tokio::test]
async fn oldest_pending_sign_in_is_evicted_when_the_cap_is_full() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions {
        max_pending: 2,
        ..HarnessOptions::default()
    });
    let first = begin(&harness).await;
    let second = begin(&harness).await;
    let third = begin(&harness).await;

    assert_failed(&finish(&harness, &first).await, "failed");
    assert_signed_in(&finish(&harness, &second).await, "/ui");
    assert_signed_in(&finish(&harness, &third).await, "/ui");
}

#[tokio::test]
async fn expired_pending_sign_in_is_evicted_before_a_live_one() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions {
        max_pending: 2,
        ..HarnessOptions::default()
    });
    let first = begin(&harness).await;

    harness.clock.advance(TTL + Duration::from_secs(1));

    let second = begin(&harness).await;
    let third = begin(&harness).await;

    assert_failed(&finish(&harness, &first).await, "failed");
    assert_signed_in(&finish(&harness, &second).await, "/ui");
    assert_signed_in(&finish(&harness, &third).await, "/ui");
}

#[tokio::test]
async fn plain_endpoint_uses_unprefixed_cookies_without_secure() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions {
        endpoint: "http://gang.example.test",
        ..HarnessOptions::default()
    });

    let begun = exchange(&harness, request("GET", "/ui/login?next=/ui/workers", &[])).await;
    assert_eq!(begun.status, StatusCode::FOUND);
    let state = state_of(&begun);
    assert_eq!(
        begun.set_cookies(),
        vec![format!(
            "gang_login={state}; HttpOnly; SameSite=Lax; Path=/; Max-Age=600"
        )]
    );
    let seen = harness.verifier.begun();
    assert_eq!(
        seen[0].redirect_uri.as_str(),
        "http://gang.example.test/ui/callback"
    );

    let done = exchange(
        &harness,
        request(
            "GET",
            &format!("/ui/callback?state={state}&code=admin-proof"),
            &[("gang_login", state.as_str())],
        ),
    )
    .await;
    assert_eq!(done.status, StatusCode::FOUND);
    assert_eq!(done.location().as_deref(), Some("/ui/workers"));
    let token = done
        .set_cookie_value("gang_session")
        .expect("callback sets the session cookie");
    let cookies = done.set_cookies();
    assert!(cookies.contains(&format!(
        "gang_session={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=600"
    )));
    assert!(cookies.contains(&"gang_login=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0".to_owned()));
}

#[tokio::test]
async fn client_secret_and_session_token_stay_out_of_responses_and_urls() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    let begun = exchange(&harness, request("GET", "/ui/login?next=/ui/workers", &[])).await;
    let state = state_of(&begun);
    let done = finish(&harness, &state).await;
    let token = done
        .set_cookie_value(SESSION_COOKIE)
        .expect("callback sets the session cookie");

    assert_no_secret(&begun);
    assert_no_secret(&done);

    let done_location = done.location().expect("callback redirects");
    assert!(
        !done_location.contains(&token),
        "the session token is in a Location"
    );
    assert!(!begun.location().unwrap_or_default().contains(SECRET));

    let mut urls: Vec<String> = harness
        .verifier
        .begun()
        .iter()
        .map(|login| login.redirect_uri.as_str().to_owned())
        .collect();
    urls.extend(
        harness
            .verifier
            .completed()
            .iter()
            .map(|url| url.as_str().to_owned()),
    );
    urls.extend(begun.location());
    urls.push(done_location);
    for url in &urls {
        assert!(!url.contains(SECRET), "a URL carries the client secret");
    }
}
