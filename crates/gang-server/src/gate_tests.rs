//! The public and worker gates with the browser session cookie, driven through a running server.
//!
//! On the open paths of the public surface the gate admits every request and records what it
//! saw in the request's [`Caller`] and client address. Elsewhere it stores nothing about the
//! caller: it admits authenticated requests and refuses the rest.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use crate::client_address::ClientAddress;
use crate::gate::Caller;
use gang_authn::anonymous;
use gang_clock::Clock;
use gang_spi::{CertStatus, IssuedCertRecord, Peer, PortError, Rejection};
use gang_testkit::Controller;
use gang_types::{AccessToken, Identity, Principal, ProviderName};
use gang_wire::routes::{
    AUTHENTICATE_SESSION, GET_SERVER, GET_SESSION, LIST_EVENTS, REVOKE_PRINCIPAL,
};
use http::StatusCode;

use crate::test_support::{
    SESSION_TTL, api_request, authority, client_certificate, cookie_parts, identity, mint_token,
    plain_parts, plain_settings, read, serve, session_token, tls_parts, tls_settings,
};

const PROVIDER: &str = "example-idp";
const ADMIN: &str = "admin";
const ADMIN_PROOF: &str = "admin-proof";
const ALICE: &str = "alice";
const ALICE_PROOF: &str = "alice-proof";

/// The identity provider whose certificates the workers present.
const WORKER_PROVIDER: &str = "example-workers";

/// The worker principal whose certificate the gate admits.
const WORKER: &str = "worker-1";

/// The address of the client that sends the test requests, as the socket peer sees it.
const CLIENT_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));

/// The session cookie name of the https deployment.
const SESSION_COOKIE: &str = "__Host-gang_session";

/// The session cookie name of the http deployment.
const HTTP_SESSION_COOKIE: &str = "gang_session";

/// How far past its expiry a token is still accepted: the test token configuration's clock skew.
const CLOCK_SKEW: Duration = Duration::from_secs(30);

/// The gRPC path of execution, which the public gate does not open.
const EXECUTE_PATH: &str = "/google.devtools.remoteexecution.v2.Execution/Execute";

/// A path of the browser surface, which the public gate opens like the API.
const UI_PATH: &str = "/ui/login";

/// The path of the scaling events, a cookie path that needs a permission.
const EVENTS_PATH: &str = LIST_EVENTS.template;

/// The path of the caller's session, a cookie path that is open to anonymous callers.
const SESSION_PATH: &str = GET_SESSION.template;

/// The path of the server description, which does not read the session cookie.
const SERVER_PATH: &str = GET_SERVER.template;

/// The path of the token exchange, which does not read the session cookie.
const AUTHENTICATE_PATH: &str = AUTHENTICATE_SESSION.template;

/// The caller the gate stored in `parts`.
fn caller_of(parts: &http::request::Parts) -> &Caller {
    parts
        .extensions
        .get::<Caller>()
        .expect("the gate stores a caller")
}

/// Asserts that the gate stored no caller for `parts`: an anonymous request has none.
fn assert_anonymous(parts: &http::request::Parts, context: &str) {
    assert!(
        parts.extensions.get::<Caller>().is_none(),
        "{context}: an anonymous request stores no caller"
    );
}

/// Asserts that the gate recorded the given rejection reason for `parts`.
fn assert_refused(parts: &http::request::Parts, rejection: Rejection, context: &str) {
    let caller = caller_of(parts);
    assert!(
        matches!(caller, Caller::Refused(refusal)
            if matches!(&**refusal, PortError::Rejected { reason: found, .. } if *found == rejection)),
        "{context}: a refused credential is recorded as {rejection:?}: {caller:?}"
    );
}

/// Revokes `principal` under the test provider as the principal bearing `admin`, through the
/// API, and returns the status of the response.
async fn revoke(controller: &Controller, admin: &AccessToken, principal: &str) -> StatusCode {
    let path = REVOKE_PRINCIPAL.path(
        &format!("{PROVIDER}:{principal}")
            .parse()
            .expect("valid identity"),
    );
    let request = api_request("POST", &path, Some(admin), None);
    read(controller.call_api(request).await).await.status
}

/// The session token `token` with the first character of its signature changed, so that the
/// signature no longer verifies.
fn tampered(token: &AccessToken) -> AccessToken {
    let text = token.expose();
    let (payload, signature) = text.rsplit_once('.').expect("the token has a signature");
    let first = if signature.starts_with('A') { 'B' } else { 'A' };
    AccessToken::new(format!("{payload}.{first}{}", &signature[1..]))
}

#[tokio::test]
async fn get_with_session_cookie_authenticates_the_cookie_identity() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut parts = cookie_parts("GET", EVENTS_PATH, Some((SESSION_COOKIE, &token)), None);
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the public gate admits a session cookie");
            let caller = caller_of(&parts);
            let authenticated = caller
                .as_authenticated()
                .expect("the session cookie authenticates the caller");
            assert_eq!(authenticated.identity.as_ref(), &identity(ALICE));
            assert_eq!(
                authenticated.valid_until,
                Some(clock.wall() + SESSION_TTL + CLOCK_SKEW)
            );
            assert!(!matches!(caller, Caller::Refused(_)), "nothing is refused");
        },
    )
    .await;
}

#[tokio::test]
async fn head_with_session_cookie_authenticates_the_cookie_identity() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut parts = cookie_parts("HEAD", EVENTS_PATH, Some((SESSION_COOKIE, &token)), None);
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the public gate admits a session cookie");
            let caller = caller_of(&parts);
            let authenticated = caller
                .as_authenticated()
                .expect("the session cookie authenticates the caller");
            assert_eq!(authenticated.identity.as_ref(), &identity(ALICE));
            assert_eq!(
                authenticated.valid_until,
                Some(clock.wall() + SESSION_TTL + CLOCK_SKEW)
            );
            assert!(!matches!(caller, Caller::Refused(_)), "nothing is refused");
        },
    )
    .await;
}

#[tokio::test]
async fn post_ignores_session_cookie_on_listed_and_token_paths() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            for path in [EVENTS_PATH, AUTHENTICATE_PATH] {
                let mut parts = cookie_parts("POST", path, Some((SESSION_COOKIE, &token)), None);
                controller
                    .admit_public(&Peer::default(), &mut parts)
                    .await
                    .expect("the public gate admits an open path");
                assert_anonymous(
                    &parts,
                    &format!("a POST to {path} does not read the cookie"),
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn get_of_unlisted_path_ignores_session_cookie() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut parts = cookie_parts("GET", SERVER_PATH, Some((SESSION_COOKIE, &token)), None);
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the public gate admits an open path");
            assert_anonymous(&parts, "a GET of an unlisted path does not read the cookie");
        },
    )
    .await;
}

#[tokio::test]
async fn worker_gate_ignores_session_cookie() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut with_cookie =
                cookie_parts("GET", EVENTS_PATH, Some((SESSION_COOKIE, &token)), None);
            let mut without_cookie = cookie_parts("GET", EVENTS_PATH, None, None);
            let outcome = controller
                .admit_worker(&Peer::default(), &mut with_cookie)
                .await;
            let baseline = controller
                .admit_worker(&Peer::default(), &mut without_cookie)
                .await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "the worker gate refuses a session cookie: {outcome:?}"
            );
            assert!(
                matches!(
                    baseline,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "the worker gate refuses a request without a cookie: {baseline:?}"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn bearer_client_token_wins_over_session_cookie() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    handles.verifier.accept(ADMIN_PROOF, ADMIN);
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let admin_session = session_token(&clock, &identity(ADMIN)).await;
            let mut parts = cookie_parts(
                "GET",
                EVENTS_PATH,
                Some((SESSION_COOKIE, &admin_session)),
                Some(&alice),
            );
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the public gate admits the bearer token");
            let authenticated = caller_of(&parts)
                .as_authenticated()
                .expect("the bearer token authenticates the caller");
            assert_eq!(authenticated.identity.as_ref(), &identity(ALICE));
        },
    )
    .await;
}

#[tokio::test]
async fn request_without_credentials_is_anonymous_on_open_paths_and_refused_elsewhere() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            for path in [EVENTS_PATH, SESSION_PATH, SERVER_PATH, UI_PATH] {
                let mut parts = cookie_parts("GET", path, None, None);
                controller
                    .admit_public(&Peer::default(), &mut parts)
                    .await
                    .expect("the public gate admits an open path");
                assert_anonymous(&parts, &format!("a request for {path} without credentials"));
            }

            let mut closed = cookie_parts("GET", EXECUTE_PATH, None, None);
            let outcome = controller.admit_public(&Peer::default(), &mut closed).await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "a request without credentials for a non-open path is refused: {outcome:?}"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn session_token_as_bearer_is_recorded_as_refused_on_open_paths() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let session = session_token(&clock, &identity(ALICE)).await;
            for path in [EVENTS_PATH, SESSION_PATH, UI_PATH] {
                let mut as_bearer = cookie_parts("GET", path, None, Some(&session));
                controller
                    .admit_public(&Peer::default(), &mut as_bearer)
                    .await
                    .expect("the public gate admits an open path");
                assert_refused(
                    &as_bearer,
                    Rejection::InvalidProof,
                    &format!("a session token as the bearer token on {path}"),
                );
            }

            let mut closed = cookie_parts("GET", EXECUTE_PATH, None, Some(&session));
            let outcome = controller.admit_public(&Peer::default(), &mut closed).await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "a session token is refused as a bearer token on a non-open path: {outcome:?}"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn client_token_as_session_cookie_is_admitted_as_anonymous() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let client = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let mut as_cookie =
                cookie_parts("GET", EVENTS_PATH, Some((SESSION_COOKIE, &client)), None);
            controller
                .admit_public(&Peer::default(), &mut as_cookie)
                .await
                .expect("the public gate admits a listed path");
            assert_anonymous(&as_cookie, "a client token presented as the session cookie");
        },
    )
    .await;
}

#[tokio::test]
async fn tampered_session_cookie_is_admitted_as_anonymous_on_open_paths() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = tampered(&session_token(&clock, &identity(ALICE)).await);
            for path in [EVENTS_PATH, SESSION_PATH] {
                let mut parts = cookie_parts("GET", path, Some((SESSION_COOKIE, &token)), None);
                controller
                    .admit_public(&Peer::default(), &mut parts)
                    .await
                    .expect("the public gate admits an open path");
                assert_anonymous(&parts, &format!("a tampered session cookie on {path}"));
            }
        },
    )
    .await;
}

#[tokio::test]
async fn expired_session_cookie_is_admitted_as_anonymous_on_open_paths() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            clock.advance(SESSION_TTL + CLOCK_SKEW + Duration::from_secs(1));
            for path in [EVENTS_PATH, SESSION_PATH] {
                let mut parts = cookie_parts("GET", path, Some((SESSION_COOKIE, &token)), None);
                controller
                    .admit_public(&Peer::default(), &mut parts)
                    .await
                    .expect("the public gate admits an open path");
                assert_anonymous(&parts, &format!("an expired session cookie on {path}"));
            }
        },
    )
    .await;
}

#[tokio::test]
async fn tampered_session_cookie_with_bearer_garbage_is_recorded_as_refused() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = tampered(&session_token(&clock, &identity(ALICE)).await);
            for garbage in ["garbage", "a.b.c", "not-a-token"] {
                let bearer = AccessToken::new(garbage.to_owned());
                for path in [EVENTS_PATH, SESSION_PATH] {
                    let mut parts =
                        cookie_parts("GET", path, Some((SESSION_COOKIE, &token)), Some(&bearer));
                    controller
                        .admit_public(&Peer::default(), &mut parts)
                        .await
                        .expect("the public gate admits an open path");
                    assert_refused(
                        &parts,
                        Rejection::InvalidProof,
                        &format!("a bad bearer {garbage} on {path}"),
                    );
                }
            }
        },
    )
    .await;
}

#[tokio::test]
async fn refused_session_cookie_on_non_open_path_stays_refused() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let session = session_token(&clock, &identity(ALICE)).await;
            let tampered_session = tampered(&session);
            let client = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            for token in [&tampered_session, &client] {
                let mut parts =
                    cookie_parts("GET", EXECUTE_PATH, Some((SESSION_COOKIE, token)), None);
                let outcome = controller.admit_public(&Peer::default(), &mut parts).await;
                assert!(
                    matches!(
                        outcome,
                        Err(PortError::Rejected {
                            reason: Rejection::InvalidProof,
                            ..
                        })
                    ),
                    "a refused cookie on a non-open path is refused: {outcome:?}"
                );
            }

            let mut worker = cookie_parts(
                "GET",
                EXECUTE_PATH,
                Some((SESSION_COOKIE, &tampered_session)),
                None,
            );
            let outcome = controller.admit_worker(&Peer::default(), &mut worker).await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "a refused cookie on the worker surface is refused: {outcome:?}"
            );

            clock.advance(SESSION_TTL + CLOCK_SKEW + Duration::from_secs(1));
            let mut expired =
                cookie_parts("GET", EXECUTE_PATH, Some((SESSION_COOKIE, &session)), None);
            let outcome = controller
                .admit_public(&Peer::default(), &mut expired)
                .await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::InvalidProof,
                        ..
                    })
                ),
                "an expired cookie on a non-open path is refused: {outcome:?}"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn revoked_principal_is_recorded_as_refused_on_open_paths() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ADMIN_PROOF, ADMIN);
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let session = session_token(&clock, &identity(ALICE)).await;

            let mut before =
                cookie_parts("GET", EVENTS_PATH, Some((SESSION_COOKIE, &session)), None);
            controller
                .admit_public(&Peer::default(), &mut before)
                .await
                .expect("the public gate admits alice's session before revocation");
            assert!(
                matches!(caller_of(&before), Caller::Authenticated(_)),
                "alice's session authenticates before revocation"
            );

            let status = revoke(&controller, &admin, ALICE).await;
            assert_eq!(status, StatusCode::OK);

            for path in [EVENTS_PATH, SESSION_PATH] {
                let mut by_cookie =
                    cookie_parts("GET", path, Some((SESSION_COOKIE, &session)), None);
                controller
                    .admit_public(&Peer::default(), &mut by_cookie)
                    .await
                    .expect("the public gate admits an open path");
                assert_refused(
                    &by_cookie,
                    Rejection::PrincipalRevoked,
                    &format!("a revoked principal's session cookie on {path}"),
                );
            }

            for path in [EVENTS_PATH, SESSION_PATH, UI_PATH] {
                let mut by_bearer = cookie_parts("GET", path, None, Some(&alice));
                controller
                    .admit_public(&Peer::default(), &mut by_bearer)
                    .await
                    .expect("the public gate admits an open path");
                assert_refused(
                    &by_bearer,
                    Rejection::PrincipalRevoked,
                    &format!("a revoked principal's bearer token on {path}"),
                );
            }

            let mut closed = cookie_parts("GET", EXECUTE_PATH, None, Some(&alice));
            let outcome = controller.admit_public(&Peer::default(), &mut closed).await;
            assert!(
                matches!(
                    outcome,
                    Err(PortError::Rejected {
                        reason: Rejection::PrincipalRevoked,
                        ..
                    })
                ),
                "a revoked principal is refused on a non-open path: {outcome:?}"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn plaintext_deployment_ignores_session_cookie() {
    let (parts, _dials, handles) = plain_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        plain_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut parts = cookie_parts(
                "GET",
                EVENTS_PATH,
                Some((HTTP_SESSION_COOKIE, &token)),
                None,
            );
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the plaintext gate admits every caller");
            let caller = caller_of(&parts);
            let authenticated = caller
                .as_authenticated()
                .expect("the plaintext gate stores a caller");
            let anonymous = anonymous();
            assert_eq!(authenticated.identity.as_ref(), &anonymous);
            assert_eq!(authenticated.valid_until, None);
            assert!(!matches!(caller, Caller::Refused(_)), "nothing is refused");
        },
    )
    .await;
}

#[tokio::test]
async fn http_cookie_name_is_ignored_under_https() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = session_token(&clock, &identity(ALICE)).await;
            let mut parts = cookie_parts(
                "GET",
                EVENTS_PATH,
                Some((HTTP_SESSION_COOKIE, &token)),
                None,
            );
            controller
                .admit_public(&Peer::default(), &mut parts)
                .await
                .expect("the public gate admits an open path");
            assert_anonymous(&parts, "the http cookie name under https");
        },
    )
    .await;
}

#[tokio::test]
async fn an_authenticated_request_on_a_grpc_path_stores_no_caller() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let peer = Peer {
                address: Some(CLIENT_ADDRESS),
                ..Peer::default()
            };
            let mut parts = cookie_parts("GET", EXECUTE_PATH, None, Some(&alice));
            controller
                .admit_public(&peer, &mut parts)
                .await
                .expect("the public gate admits a client token on a grpc path");
            assert!(
                parts.extensions.get::<Caller>().is_none(),
                "a grpc path stores no caller"
            );
            assert!(
                parts.extensions.get::<ClientAddress>().is_none(),
                "a grpc path stores no client address"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn an_authenticated_request_on_an_open_path_stores_its_caller_and_address() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let peer = Peer {
                address: Some(CLIENT_ADDRESS),
                ..Peer::default()
            };
            let mut parts = cookie_parts("GET", SESSION_PATH, None, Some(&alice));
            controller
                .admit_public(&peer, &mut parts)
                .await
                .expect("the public gate admits a client token on an open path");
            let authenticated = caller_of(&parts)
                .as_authenticated()
                .expect("the client token authenticates the caller");
            assert_eq!(authenticated.identity.as_ref(), &identity(ALICE));
            assert_eq!(
                parts.extensions.get::<ClientAddress>(),
                Some(&ClientAddress(CLIENT_ADDRESS))
            );
        },
    )
    .await;
}

#[tokio::test]
async fn the_worker_gate_stores_no_caller() {
    let (parts, _dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (issued, _key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    let issued_at = handles.clock.wall();
    handles.repository.insert(IssuedCertRecord {
        serial: issued.serial.clone(),
        identity: Identity::new(
            ProviderName::try_new(WORKER_PROVIDER).expect("test provider name is valid"),
            Principal::try_new(WORKER).expect("test principal is valid"),
        ),
        issued_at,
        expires_at: issued_at + Duration::from_hours(24),
        status: CertStatus::Active,
    });
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let worker = Peer {
                certificate: Some(issued.serial.clone()),
                ..Peer::default()
            };
            let mut parts = cookie_parts("GET", EXECUTE_PATH, None, None);
            controller
                .admit_worker(&worker, &mut parts)
                .await
                .expect("a ledgered worker certificate is admitted");
            assert!(
                parts.extensions.get::<Caller>().is_none(),
                "the worker gate stores no caller"
            );
        },
    )
    .await;
}
