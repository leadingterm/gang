//! The contract every answer of the API honours, whichever route gave it: every route of the
//! route table is served, every JSON key is `lowerCamelCase`, and every refusal is the error
//! envelope with its `ErrorInfo` exactly once.

use std::pin::pin;
use std::time::Duration;

use gang_clock::TestClock;
use gang_spi::{ScalingAuditEvent, ScalingEventKind, StateRepositoryPort};
use gang_testkit::{Controller, FakeRepository, TestResult, worker_id};
use gang_types::{AccessToken, FleetName, Proof, ProviderName, ScalingCause};
use gang_wire::routes::{
    AUTHENTICATE_SESSION, CREATE_CERTIFICATE, GET_SERVER, GET_SESSION, GET_STATE, LIST_EVENTS,
    LIST_PRINCIPALS, LIST_ROLES, RESTORE_PRINCIPAL, REVOKE_CERTIFICATE, REVOKE_PRINCIPAL, ROUTES,
    Route, WATCH_STATE,
};
use gang_wire::{AuthenticateRequest, CreateCertificateRequest, ErrorReason};
use http::StatusCode;
use http::header::CONTENT_TYPE;
use http_body_util::BodyExt;
use serde_json::{Value, json};

use crate::api_test_support::{
    ADMIN_PROOF, ALICE, ALICE_PROOF, BODY_LIMIT, PROVIDER, WORKER_PROOF, WORKER_PROVIDER, get,
    post, principal_path, revoke_principal, send, worker_certificate,
};
use crate::test_support::{
    ApiReply as Answer, FakeVerifier, admits, api_request, at, decode_sse, identity, mint_token,
    raw_api_request, serve, signing_request, tls_parts, tls_settings,
};

const SERIAL: &str = "b1";
const HUNG_PROOF: &str = "hung-proof";

/// Whether `key` is `lowerCamelCase`: a lowercase ASCII letter followed by ASCII alphanumerics.
fn is_lower_camel_case(key: &str) -> bool {
    let mut characters = key.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_lowercase())
        && characters.all(|character| character.is_ascii_alphanumeric())
}

/// Asserts that every object key in `value`, found at `path`, is `lowerCamelCase`. The exceptions
/// are the plugin-owned `login` objects, the keys of the fleet `properties` map, which are data,
/// and the `@type` of an error detail.
fn assert_lower_camel_case_keys(value: &Value, path: &str) {
    match value {
        Value::Object(members) => {
            for (key, member) in members {
                assert!(
                    key == "@type" || is_lower_camel_case(key),
                    "key {key:?} at {path} is not lowerCamelCase"
                );
                if key != "login" && key != "properties" {
                    assert_lower_camel_case_keys(member, &format!("{path}.{key}"));
                }
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                assert_lower_camel_case_keys(item, &format!("{path}[{index}]"));
            }
        }
        _ => {}
    }
}

/// One request of the sweep over the route table: the route, its path, and its body.
struct Probe {
    route: Route,
    path: String,
    body: Option<Value>,
}

/// One probe per route of the route table, in an order in which each succeeds: a principal is
/// revoked and then restored.
fn probes() -> Vec<Probe> {
    let provider = ProviderName::try_new(PROVIDER).expect("provider name is valid");
    let worker_provider = ProviderName::try_new(WORKER_PROVIDER).expect("provider name is valid");
    let authenticate = AuthenticateRequest {
        provider,
        proof: Proof::new(ALICE_PROOF),
    };
    let create = CreateCertificateRequest {
        provider: worker_provider,
        proof: Proof::new(WORKER_PROOF),
        pem_csr: signing_request(),
    };
    let alice = identity(ALICE);
    let serial = gang_types::CertSerial::try_new(SERIAL).expect("fixture serial is valid");
    let probe = |route: Route, path: Option<String>, body: Option<Value>| Probe {
        path: path.unwrap_or_else(|| route.template.to_owned()),
        route,
        body,
    };
    vec![
        probe(GET_SERVER, None, None),
        probe(
            AUTHENTICATE_SESSION,
            None,
            Some(serde_json::to_value(authenticate).expect("request serializes")),
        ),
        probe(
            CREATE_CERTIFICATE,
            None,
            Some(serde_json::to_value(create).expect("request serializes")),
        ),
        probe(
            *REVOKE_CERTIFICATE,
            Some(REVOKE_CERTIFICATE.path(&serial)),
            None,
        ),
        probe(LIST_PRINCIPALS, None, None),
        probe(*REVOKE_PRINCIPAL, Some(REVOKE_PRINCIPAL.path(&alice)), None),
        probe(
            *RESTORE_PRINCIPAL,
            Some(RESTORE_PRINCIPAL.path(&alice)),
            None,
        ),
        probe(LIST_ROLES, None, None),
        probe(LIST_EVENTS, None, None),
        probe(GET_STATE, None, None),
        probe(WATCH_STATE, None, None),
        probe(GET_SESSION, None, None),
    ]
}

/// A ledger event of `kind` in the test fleet at `millis`.
fn ledger_event(millis: u64, kind: ScalingEventKind) -> ScalingAuditEvent {
    ScalingAuditEvent {
        at: at(millis),
        fleet: FleetName::try_new("gang-x86").expect("fixture fleet name is valid"),
        kind,
        cause: Some(ScalingCause::Demand),
    }
}

/// The JSON payload of the first frame the stream of `controller` writes for `token`.
async fn first_frame_payload(controller: &Controller, token: &AccessToken) -> Value {
    let request = api_request("GET", WATCH_STATE.template, Some(token), None);
    let mut response = controller.call_api(request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    let chunk = response
        .body_mut()
        .frame()
        .await
        .expect("the stream writes a frame")
        .expect("the frame reads")
        .into_data()
        .expect("stream frames carry data");
    assert!(
        matches!(decode_sse(&chunk), gang_wire::Frame::Snapshot(_)),
        "the first frame is a snapshot"
    );
    let text = String::from_utf8(chunk.to_vec()).expect("stream frames are UTF-8");
    let data = text
        .lines()
        .find_map(|line| line.strip_prefix("data:"))
        .expect("the frame carries data");
    serde_json::from_str(data.trim_start()).expect("the payload is JSON")
}

#[tokio::test]
async fn every_route_is_served_and_every_json_key_is_lower_camel_case() -> TestResult {
    let (parts, _dials, handles) = tls_parts();
    handles.repository.insert(worker_certificate(SERIAL));
    for event in [
        ledger_event(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        ledger_event(
            2_000,
            ScalingEventKind::DrainStarted {
                worker: worker_id("local-1")?,
            },
        ),
    ] {
        handles
            .repository
            .record_scaling_event(&event)
            .await
            .expect("the fixture scaling event is recorded");
    }
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            assert!(
                admits(&controller, &alice).await,
                "alice is in the registry"
            );

            let probes = probes();
            let covered: Vec<Route> = probes.iter().map(|probe| probe.route).collect();
            assert_eq!(covered, ROUTES, "the sweep covers exactly the route table");

            for probe in probes {
                let path = probe.path;
                let label = format!("{} {path}", probe.route.method.as_str());
                let payload = if probe.route == WATCH_STATE {
                    first_frame_payload(&controller, &admin).await
                } else {
                    let request =
                        api_request(probe.route.method.as_str(), &path, Some(&admin), probe.body);
                    let (parts, body) = controller.call_api(request).await.into_parts();
                    let bytes = body.collect().await.expect("the body reads").to_bytes();
                    assert_eq!(
                        parts.status,
                        StatusCode::OK,
                        "{label}: {}",
                        String::from_utf8_lossy(&bytes)
                    );
                    let content_type = parts.headers[CONTENT_TYPE].to_str().expect("ASCII");
                    assert!(content_type.starts_with("application/json"), "{label}");
                    serde_json::from_slice(&bytes).expect("the body is JSON")
                };
                assert_lower_camel_case_keys(&payload, &label);
            }
        },
    )
    .await;
    Ok(())
}

/// Posts the JSON `body` to the route `template` without a credential.
async fn post_json(controller: &Controller, template: &str, body: Value) -> Answer {
    send(controller, api_request("POST", template, None, Some(body))).await
}

/// Posts the raw `bytes` to the route `template` without a credential.
async fn post_raw(controller: &Controller, template: &str, bytes: Vec<u8>) -> Answer {
    send(controller, raw_api_request("POST", template, None, bytes)).await
}

/// Posts the JSON `body` to the route `template` without a credential, and advances `clock` past
/// the request timeout while the request is pending.
async fn post_past_deadline(
    controller: &Controller,
    clock: &TestClock,
    template: &str,
    body: Value,
) -> Answer {
    let timeout = tls_settings().limits.request_timeout;
    let mut answer = pin!(post_json(controller, template, body));
    // The first poll runs the request until it waits on its handler and on the deadline.
    assert!(
        futures::poll!(answer.as_mut()).is_pending(),
        "the request waits"
    );
    clock.advance(timeout.get() + Duration::from_secs(1));
    answer.await
}

/// A refusal for every reason the fakes can provoke, each with the reason it must carry. Revokes
/// `alice`, so it runs last.
async fn refusals(
    controller: &Controller,
    repository: &FakeRepository,
    verifier: &FakeVerifier,
    clock: &TestClock,
) -> Vec<(Answer, ErrorReason)> {
    let admin = mint_token(controller, PROVIDER, ADMIN_PROOF).await;
    let alice = mint_token(controller, PROVIDER, ALICE_PROOF).await;
    let bogus = AccessToken::new("not.a.valid.token");
    let events = LIST_EVENTS.template;
    let authenticate = AUTHENTICATE_SESSION.template;
    let unknown_provider = json!({ "provider": "nobody-idp", "proof": ALICE_PROOF });
    let forged_proof = json!({ "provider": PROVIDER, "proof": "forged-proof" });
    let missing_certificate = REVOKE_CERTIFICATE
        .path(&gang_types::CertSerial::try_new("ff00").expect("fixture serial is valid"));
    let missing_principal = RESTORE_PRINCIPAL.path(&identity("nobody"));
    let bad_token_path = format!("{events}?pageToken=garbage");
    let mut refusals = vec![
        (
            get(controller, events, None).await,
            ErrorReason::CredentialsMissing,
        ),
        (
            get(controller, GET_SERVER.template, Some(&bogus)).await,
            ErrorReason::CredentialsInvalid,
        ),
        (
            get(controller, events, Some(&alice)).await,
            ErrorReason::MissingPermission,
        ),
        (
            get(controller, "/v1/nope", Some(&admin)).await,
            ErrorReason::RouteNotFound,
        ),
        (
            post_raw(controller, authenticate, b"{".to_vec()).await,
            ErrorReason::MalformedRequest,
        ),
        (
            post_raw(controller, authenticate, vec![b' '; BODY_LIMIT + 1]).await,
            ErrorReason::RequestTooLarge,
        ),
        (
            get(controller, &bad_token_path, Some(&admin)).await,
            ErrorReason::InvalidPageToken,
        ),
        (
            post_json(controller, authenticate, unknown_provider).await,
            ErrorReason::UnknownProvider,
        ),
        (
            post_json(controller, authenticate, forged_proof).await,
            ErrorReason::ProofRejected,
        ),
        (
            post(controller, &missing_certificate, &admin).await,
            ErrorReason::CertificateNotFound,
        ),
        (
            post(controller, &missing_principal, &admin).await,
            ErrorReason::PrincipalNotFound,
        ),
    ];

    repository.fail_scaling_reads(true);
    refusals.push((
        get(controller, events, Some(&admin)).await,
        ErrorReason::DependencyUnavailable,
    ));
    repository.fail_scaling_reads(false);

    revoke_principal(controller, &admin, ALICE).await;
    refusals.push((
        get(controller, events, Some(&alice)).await,
        ErrorReason::PrincipalRevoked,
    ));
    verifier.hang(HUNG_PROOF);
    let hung = json!({ "provider": PROVIDER, "proof": HUNG_PROOF });
    refusals.push((
        post_past_deadline(controller, clock, authenticate, hung).await,
        ErrorReason::RequestTimedOut,
    ));
    refusals
}

#[tokio::test]
async fn every_refusal_is_the_error_envelope_with_one_error_info() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    let verifier = handles.verifier.clone();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let refusals = refusals(&controller, &repository, &verifier, &clock).await;

            let mut exercised = Vec::new();
            for (answer, reason) in &refusals {
                answer.refused(*reason);
                assert_lower_camel_case_keys(&answer.json(), reason.as_str());
                exercised.push(*reason);
            }
            let unexercised: Vec<ErrorReason> = ErrorReason::ALL
                .into_iter()
                .filter(|reason| !exercised.contains(reason))
                .collect();
            assert_eq!(
                unexercised,
                [
                    ErrorReason::PolicyDenied,
                    ErrorReason::RateLimited,
                    ErrorReason::InternalError,
                ],
                "only the reasons the fakes cannot provoke here are left out; RATE_LIMITED is covered by the rate limit tests"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_body_of_exactly_the_size_limit_is_accepted_and_one_byte_more_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let path = principal_path(&REVOKE_PRINCIPAL, ALICE);
            let padded = |len: usize| {
                let mut bytes = b"{}".to_vec();
                bytes.resize(len, b' ');
                bytes
            };

            let too_large = raw_api_request("POST", &path, Some(&admin), padded(BODY_LIMIT + 1));
            send(&controller, too_large)
                .await
                .refused(ErrorReason::RequestTooLarge);

            let at_limit = raw_api_request("POST", &path, Some(&admin), padded(BODY_LIMIT));
            let _revoked: Value = send(&controller, at_limit).await.ok();
        },
    )
    .await;
}

#[tokio::test]
async fn a_request_that_outlasts_the_deadline_is_answered_504() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    handles.verifier.hang(HUNG_PROOF);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let body = json!({ "provider": PROVIDER, "proof": HUNG_PROOF });
            let answer =
                post_past_deadline(&controller, &clock, AUTHENTICATE_SESSION.template, body).await;

            assert_eq!(answer.status, StatusCode::GATEWAY_TIMEOUT);
            answer.refused(ErrorReason::RequestTimedOut);
        },
    )
    .await;
}
