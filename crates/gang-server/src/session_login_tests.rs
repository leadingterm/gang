//! Behavioural tests of the browser sign-in: `/ui/login`, `/ui/callback` and the session it issues.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::device_logins::Redeem;
use crate::gate::Caller;
use crate::renewal_seal::RenewalSeal;
use gang_spi::{Peer, Rejection, RequestGate};
use gang_types::{ClientSecret, Identity, Principal, ProviderName};
use gang_wire::{ERROR_PATH, routes::GET_SESSION};
use http::StatusCode;
use http::header::CACHE_CONTROL;
use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

use crate::test_support::{
    ApiReply, BeginFailure, Harness, HarnessOptions, TlsDeployment, raw_api_request, read, request,
    request_with_header,
};

/// A field recorder for the events emitted by a single request.
#[derive(Default)]
struct EventFields(BTreeMap<String, String>);

impl Visit for EventFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

/// Collects INFO events without changing the request's tracing context.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl<S: Subscriber> Layer<S> for Events {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::INFO {
            let mut fields = EventFields::default();
            event.record(&mut fields);
            self.0.lock().push(fields.0);
        }
    }
}

impl Events {
    fn matching(&self, message: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .iter()
            .filter(|fields| fields.get("message").is_some_and(|value| value == message))
            .cloned()
            .collect()
    }
}

/// Runs requests on the subscriber's thread so scoped events are captured.
fn recorded(f: impl FnOnce(&tokio::runtime::Runtime, &Events)) {
    let events = Events::default();
    let subscriber = Registry::default().with(events.clone());
    gang_testkit::with_subscriber(subscriber, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        f(&runtime, &events);
    });
}

fn assert_event(events: &Events, message: &str, outcome: &str, principal: Option<&str>) {
    let matching = events.matching(message);
    assert_eq!(matching.len(), 1, "{message}: {matching:?}");
    let fields = &matching[0];
    assert!(
        fields
            .get("outcome")
            .is_some_and(|value| value.starts_with(outcome)),
        "{fields:?}"
    );
    assert_eq!(fields.get("principal").map(String::as_str), principal);
}

/// The session cookie name on an `https` endpoint.
const SESSION_COOKIE: &str = "__Host-gang_session";

/// The sign-in cookie name on an `https` endpoint.
const LOGIN_COOKIE: &str = "__Host-gang_login";

/// The sealed renewal cookie name on an HTTPS endpoint.
const RENEWAL_COOKIE: &str = "__Host-gang_renewal";

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
    let location = if reply.status == StatusCode::OK {
        reply
            .json::<gang_wire::ApproveDeviceAuthorizationResponse>()
            .authorization_url
            .to_string()
    } else {
        reply
            .location()
            .expect("sign-in redirects to the identity provider")
    };
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

fn assert_device_failed(reply: &ApiReply, reason: &str) {
    assert_eq!(reply.status, StatusCode::FOUND);
    assert_no_store(reply);
    assert_eq!(
        reply.location(),
        Some(format!("/ui/device?reason={reason}"))
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
async fn dashboard_sign_in_without_a_renewal_cookie_requests_offline_access() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());

    start(&harness, "/ui/login?next=/ui").await;

    let seen = harness.verifier.begun();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].offline);
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

/// Signs in with a refresh token and returns the sealed renewal cookie.
async fn renewal_cookie(harness: &Harness<TlsDeployment>) -> String {
    harness
        .verifier
        .issue_refresh_token("admin-proof", "refresh");
    let state = begin(harness).await;
    finish(harness, &state)
        .await
        .set_cookie_value(RENEWAL_COOKIE)
        .expect("sign-in sets a renewal cookie")
}

/// Opens a test renewal cookie using the same key and origin as the server.
fn open_renewal(value: &str) -> crate::renewal_seal::Renewal {
    RenewalSeal::new(&ClientSecret::new(SECRET), "https://gang.example.test")
        .open(value)
        .expect("renewal opens")
}

/// Posts a same-origin renewal request carrying the supplied cookie.
async fn renew(harness: &Harness<TlsDeployment>, cookie: Option<&str>) -> ApiReply {
    let cookies: Vec<_> = cookie
        .map(|value| (RENEWAL_COOKIE, value))
        .into_iter()
        .collect();
    exchange(
        harness,
        request_with_header(
            "POST",
            "/ui/renew",
            &cookies,
            "sec-fetch-site",
            "same-origin",
        ),
    )
    .await
}

/// Asserts that the renewal cookie is cleared.
fn assert_renewal_cleared(reply: &ApiReply) {
    assert!(reply.set_cookies().contains(&format!(
        "{RENEWAL_COOKIE}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0"
    )));
}

#[tokio::test]
async fn a_first_sign_in_is_offline_and_sets_the_renewal_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    harness
        .verifier
        .issue_refresh_token("admin-proof", "refresh");
    let state = begin(&harness).await;
    assert!(harness.verifier.begun()[0].offline);
    let reply = finish(&harness, &state).await;
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_some());
    let cookie = reply
        .set_cookie_value(RENEWAL_COOKIE)
        .expect("renewal cookie");
    assert_eq!(open_renewal(&cookie).principal, "admin");
    assert!(reply.set_cookies().contains(&format!(
        "{RENEWAL_COOKIE}={cookie}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=34560000"
    )));
}

#[tokio::test]
async fn a_sign_in_without_a_refresh_token_sets_no_renewal_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let state = begin(&harness).await;
    let reply = finish(&harness, &state).await;
    assert_renewal_cleared(&reply);
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_some());
}

#[tokio::test]
async fn a_sign_in_with_a_renewal_cookie_is_not_offline_and_rechecks_the_sealed_token() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    let original = open_renewal(&cookie);
    harness.clock.advance(Duration::from_secs(10));
    harness.verifier.renew_to("refresh", "admin-proof", None);
    let started = exchange(
        &harness,
        request("GET", "/ui/login", &[(RENEWAL_COOKIE, &cookie)]),
    )
    .await;
    let state = state_of(&started);
    assert!(!harness.verifier.begun()[1].offline);
    let reply = finish(&harness, &state).await;
    assert_signed_in(&reply, "/ui");
    assert_eq!(
        harness.verifier.renewals(),
        vec![("refresh".into(), Some("web-client".into()))]
    );
    let new = open_renewal(
        &reply
            .set_cookie_value(RENEWAL_COOKIE)
            .expect("renewal resealed"),
    );
    assert_eq!(
        new.expire_time,
        original.expire_time + Duration::from_secs(10)
    );
}

#[tokio::test]
async fn a_failed_recheck_restarts_the_sign_in_as_offline() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    let started = exchange(
        &harness,
        request(
            "GET",
            "/ui/login?next=/ui/workers",
            &[(RENEWAL_COOKIE, &cookie)],
        ),
    )
    .await;
    let reply = finish(&harness, &state_of(&started)).await;
    assert_eq!(reply.status, StatusCode::FOUND);
    assert!(
        reply
            .location()
            .expect("provider redirect")
            .starts_with("https://idp.example.test/authorize?")
    );
    assert!(harness.verifier.begun()[2].offline);
    assert!(reply.set_cookie_value(LOGIN_COOKIE).is_some());
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_none());
    assert_renewal_cleared(&reply);
}

#[tokio::test]
async fn renew_issues_a_session_and_reseals_only_a_rotated_token() {
    for rotated in [None, Some("rotated")] {
        let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
        let cookie = renewal_cookie(&harness).await;
        let original = open_renewal(&cookie);
        harness.verifier.renew_to("refresh", "admin-proof", rotated);
        let reply = renew(&harness, Some(&cookie)).await;
        assert_eq!(reply.status, StatusCode::NO_CONTENT);
        assert_no_store(&reply);
        assert!(reply.set_cookie_value(SESSION_COOKIE).is_some());
        if let Some(rotated) = rotated {
            let new = open_renewal(
                &reply
                    .set_cookie_value(RENEWAL_COOKIE)
                    .expect("rotated cookie"),
            );
            assert_eq!(new.expire_time, original.expire_time);
            assert_eq!(new.refresh_token.expose(), rotated);
        } else {
            assert!(reply.set_cookie_value(RENEWAL_COOKIE).is_none());
        }
    }
}

#[tokio::test]
async fn renew_past_the_absolute_lifetime_is_401_even_with_a_valid_refresh_token() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    harness.verifier.renew_to("refresh", "admin-proof", None);
    harness.clock.advance(Duration::from_secs(43_201));
    let reply = renew(&harness, Some(&cookie)).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert!(harness.verifier.renewals().is_empty());
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn renew_for_another_principal_is_401() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    harness.verifier.accept("other-proof", "other");
    harness.verifier.renew_to("refresh", "other-proof", None);
    let reply = renew(&harness, Some(&cookie)).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_renewal_cleared(&reply);
}

#[tokio::test]
async fn renew_of_a_revoked_principal_is_403_and_clears_the_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    harness.verifier.renew_to("refresh", "admin-proof", None);
    let provider = ProviderName::try_new("example-idp").expect("provider valid");
    let principal = Principal::try_new("admin").expect("principal valid");
    harness
        .core
        .host
        .revoked_principals
        .revoke(Identity::new(provider, principal));
    let reply = renew(&harness, Some(&cookie)).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_renewal_cleared(&reply);
}

#[tokio::test]
async fn renew_when_the_provider_is_unavailable_is_503_and_keeps_the_cookie() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    harness.verifier.renew_unavailable("refresh");
    let reply = renew(&harness, Some(&cookie)).await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(reply.set_cookies().is_empty());
}

#[tokio::test]
async fn renew_without_a_cookie_is_401() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = renew(&harness, None).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_renewal_cleared(&reply);
}

#[tokio::test]
async fn renew_with_a_tampered_cookie_is_401() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = format!("{}!", renewal_cookie(&harness).await);
    let reply = renew(&harness, Some(&cookie)).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_renewal_cleared(&reply);
}

#[tokio::test]
async fn cross_site_renew_is_403() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    let reply = exchange(
        &harness,
        request_with_header(
            "POST",
            "/ui/renew",
            &[(RENEWAL_COOKIE, &cookie)],
            "sec-fetch-site",
            "cross-site",
        ),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert!(reply.set_cookies().is_empty());
    assert!(harness.verifier.renewals().is_empty());
}

#[tokio::test]
async fn two_renewals_of_one_token_make_one_grant() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let cookie = renewal_cookie(&harness).await;
    harness
        .verifier
        .renew_to("refresh", "admin-proof", Some("rotated"));
    assert_eq!(
        renew(&harness, Some(&cookie)).await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        renew(&harness, Some(&cookie)).await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(harness.verifier.renewals().len(), 1);
}

#[tokio::test]
async fn renew_is_post_only() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = exchange(&harness, request("GET", "/ui/renew", &[])).await;
    assert_eq!(reply.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        reply.headers.get(http::header::ALLOW).expect("allow"),
        "POST"
    );
}

#[tokio::test]
async fn logout_clears_the_session_and_renewal_cookies() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = exchange(
        &harness,
        request_with_header("POST", "/ui/logout", &[], "sec-fetch-site", "same-origin"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_renewal_cleared(&reply);
    assert!(reply.set_cookies().contains(&format!(
        "{SESSION_COOKIE}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0"
    )));
}

/// A harness with device login enabled.
fn device_harness() -> Harness<TlsDeployment> {
    Harness::<TlsDeployment>::token(HarnessOptions {
        device: true,
        ..HarnessOptions::default()
    })
}

/// Creates a device authorization using the service shared by the routes.
fn create_device(harness: &Harness<TlsDeployment>) -> gang_wire::DeviceAuthorization {
    let device = harness.routes.device().expect("device login is mounted");
    device
        .logins
        .create(None, &device.verification_uri, &device.provider)
        .expect("device authorization is created")
}

/// Extracts the user code from a device authorization name.
fn device_code(authorization: &gang_wire::DeviceAuthorization) -> &str {
    authorization
        .name
        .strip_prefix("deviceAuthorizations/")
        .expect("device name")
}

/// Submits the device code from a browser with the given fetch-site.
async fn approve_device(harness: &Harness<TlsDeployment>, code: &str, site: &str) -> ApiReply {
    let mut request = raw_api_request(
        "POST",
        "/ui/device",
        None,
        serde_json::to_vec(&serde_json::json!({ "userCode": code })).expect("request encodes"),
    );
    request.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    request.headers_mut().insert(
        "sec-fetch-site",
        http::HeaderValue::from_str(site).expect("fetch site header"),
    );
    exchange(harness, request).await
}

#[tokio::test]
async fn device_page_get_is_the_dashboard_page() {
    let harness = device_harness();
    let page = exchange(&harness, request("GET", "/ui/device", &[])).await;
    let dashboard = read(
        gang_ui::respond(&http::Method::GET, "/ui", &http::HeaderMap::new())
            .map(gang_spi::full_body),
    )
    .await;
    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(page.body, dashboard.body);
    assert_eq!(
        page.headers
            .get(http::header::CONTENT_TYPE)
            .expect("content type"),
        "text/html; charset=utf-8"
    );
}

#[tokio::test]
async fn device_page_head_and_other_methods_follow_the_ui_contract() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let head = exchange(&harness, request("HEAD", "/ui/device", &[])).await;
    assert_eq!(head.status, StatusCode::OK);
    assert!(head.body.is_empty());
    let other = exchange(&harness, request("PUT", "/ui/device", &[])).await;
    assert_eq!(other.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        other.headers.get(http::header::ALLOW).expect("allow"),
        "GET, HEAD, POST"
    );
}

#[tokio::test]
async fn cross_site_device_approval_is_403_and_starts_nothing() {
    let harness = device_harness();
    let authorization = create_device(&harness);
    let reply = approve_device(&harness, device_code(&authorization), "cross-site").await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert!(reply.body.is_empty());
    assert!(harness.verifier.begun().is_empty());
}

#[tokio::test]
async fn an_unknown_code_is_not_found() {
    let harness = device_harness();
    let reply = approve_device(&harness, "invalid", "same-origin").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert!(reply.body.is_empty());
    assert!(harness.verifier.begun().is_empty());
}

#[tokio::test]
async fn malformed_device_approval_bodies_are_rejected() {
    let harness = device_harness();
    for (body, content_type, status) in [
        (
            r"userCode=X",
            "application/x-www-form-urlencoded",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (r"userCode=X", "application/json", StatusCode::BAD_REQUEST),
        (
            r#"{"userCode":"X","extra":true}"#,
            "Application/JSON; charset=utf-8",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let mut request = raw_api_request("POST", "/ui/device", None, body.as_bytes().to_vec());
        request.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static(content_type),
        );
        request.headers_mut().insert(
            "sec-fetch-site",
            http::HeaderValue::from_static("same-origin"),
        );
        let reply = exchange(&harness, request).await;
        assert_eq!(reply.status, status);
        assert!(reply.body.is_empty());
        assert!(reply.set_cookies().is_empty());
    }
    assert!(harness.verifier.begun().is_empty());
}

#[tokio::test]
async fn a_device_approval_signs_in_offline_with_the_web_client() {
    let harness = device_harness();
    let authorization = create_device(&harness);
    let reply = approve_device(&harness, device_code(&authorization), "same-origin").await;
    assert_eq!(reply.status, StatusCode::OK);
    let answer: gang_wire::ApproveDeviceAuthorizationResponse = reply.json();
    let expected = gang_types::Url::parse_with_params(
        "https://idp.example.test/authorize",
        &[
            ("client_id", "web-client"),
            ("redirect_uri", "https://gang.example.test/ui/callback"),
            ("state", "state-1"),
        ],
    )
    .expect("provider URL parses");
    assert_eq!(answer.authorization_url, expected);
    assert_no_store(&reply);
    let state = state_of(&reply);
    assert_eq!(reply.set_cookies(), vec![login_set_cookie(&state)]);
    assert!(reply.set_cookie_value(SESSION_COOKIE).is_none());
    let begun = harness.verifier.begun();
    assert_eq!(begun.len(), 1);
    assert!(begun[0].offline);
    assert_eq!(
        begun[0].client,
        Some(("web-client".to_owned(), SECRET.to_owned()))
    );
}

#[tokio::test]
async fn device_approval_reports_sign_in_start_failures() {
    for (failure, status) in [
        (BeginFailure::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
        (
            BeginFailure::Rejected(Rejection::PolicyDenied),
            StatusCode::FORBIDDEN,
        ),
        (
            BeginFailure::Rejected(Rejection::InvalidProof),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let harness = device_harness();
        let authorization = create_device(&harness);
        harness.verifier.fail_begin(failure);
        let reply = approve_device(&harness, device_code(&authorization), "same-origin").await;
        assert_eq!(reply.status, status);
        assert!(reply.body.is_empty());
        assert!(reply.set_cookies().is_empty());
    }
}

#[tokio::test]
async fn the_device_callback_approves_and_lands_on_the_approved_page() {
    let harness = device_harness();
    let authorization = create_device(&harness);
    harness
        .verifier
        .issue_refresh_token("admin-proof", "device-refresh");
    let begun = approve_device(&harness, device_code(&authorization), "same-origin").await;
    let done = finish(&harness, &state_of(&begun)).await;
    assert_eq!(done.status, StatusCode::FOUND);
    assert_eq!(done.location().as_deref(), Some("/ui/device/approved"));
    assert_eq!(done.set_cookies(), vec![cleared_login_cookie()]);
    let device = harness.routes.device().expect("device login");
    let approval = device
        .logins
        .redeem(
            device_code(&authorization),
            authorization
                .secret
                .as_ref()
                .expect("create returns a secret"),
        )
        .expect("approval can be redeemed");
    assert_eq!(approval.principal, "admin");
    assert_eq!(approval.grant.proof.expose(), "admin-proof");
    assert_eq!(
        approval
            .grant
            .refresh_token
            .expect("refresh token")
            .expose(),
        "device-refresh"
    );
}

#[tokio::test]
async fn a_device_callback_without_a_refresh_token_leaves_the_entry_pending() {
    let harness = device_harness();
    let authorization = create_device(&harness);
    let begun = approve_device(&harness, device_code(&authorization), "same-origin").await;
    let done = finish(&harness, &state_of(&begun)).await;
    assert_device_failed(&done, "failed");
    assert!(done.set_cookie_value(RENEWAL_COOKIE).is_none());
    let device = harness.routes.device().expect("device login");
    assert!(matches!(
        device.logins.redeem(
            device_code(&authorization),
            authorization.secret.as_ref().expect("secret")
        ),
        Err(Redeem::Pending)
    ));
}

#[tokio::test]
async fn a_second_approval_is_refused() {
    let harness = device_harness();
    let authorization = create_device(&harness);
    harness
        .verifier
        .issue_refresh_token("admin-proof", "device-refresh");
    let first = approve_device(&harness, device_code(&authorization), "same-origin").await;
    let second = approve_device(&harness, device_code(&authorization), "same-origin").await;
    let approved = finish(&harness, &state_of(&first)).await;
    assert_eq!(approved.location().as_deref(), Some("/ui/device/approved"));
    assert_device_failed(&finish(&harness, &state_of(&second)).await, "failed");
}

#[tokio::test]
async fn device_approval_is_404_when_device_login_is_off() {
    let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
    let reply = approve_device(&harness, "BCDF-GHJK", "same-origin").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert!(harness.verifier.begun().is_empty());
}

#[test]
fn refused_device_approvals_log_the_outcome_once() {
    for (code, site, status) in [
        ("invalid", "cross-site", StatusCode::FORBIDDEN),
        ("invalid", "same-origin", StatusCode::NOT_FOUND),
    ] {
        recorded(|runtime, events| {
            let harness = device_harness();
            let reply = runtime.block_on(approve_device(&harness, code, site));
            assert_eq!(reply.status, status);
            assert_event(events, "device approval refused", status.as_str(), None);
            assert!(events.matching("device approval refused")[0].contains_key("client"));
        });
    }
}

#[test]
fn failed_device_callback_logs_principal_and_returns_to_device_page() {
    recorded(|runtime, events| {
        let harness = device_harness();
        let authorization = create_device(&harness);
        let code = device_code(&authorization);
        let begun = runtime.block_on(approve_device(&harness, code, "same-origin"));
        let reply = runtime.block_on(finish(&harness, &state_of(&begun)));
        assert_device_failed(&reply, "failed");
        assert_event(events, "device approval failed", "failed", Some("admin"));
        assert_eq!(
            events.matching("device approval failed")[0]
                .get("code")
                .map(String::as_str),
            Some(code)
        );
    });
}

#[test]
fn browser_renewal_logs_principal_and_outcome_once() {
    recorded(|runtime, events| {
        let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
        let cookie = runtime.block_on(renewal_cookie(&harness));
        harness.verifier.renew_to("refresh", "admin-proof", None);
        let reply = runtime.block_on(renew(&harness, Some(&cookie)));
        assert_eq!(reply.status, StatusCode::NO_CONTENT);
        assert_event(events, "browser renewal", "204", Some("admin"));
    });
    recorded(|runtime, events| {
        let harness = Harness::<TlsDeployment>::token(HarnessOptions::default());
        let reply = runtime.block_on(renew(&harness, None));
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
        assert_event(events, "browser renewal", "401", None);
    });
}
