//! Controller observation ingestion through the API boundary.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, stream};
use gang_authn::anonymous;
use gang_clock::{Clock, TestClock};
use gang_config::{BuildsConfig, EndpointTrust};
use gang_mtls::{ControllerKey, MAX_PROOF_BYTES, PROOF_HEADER, mint};
use gang_spi::{
    ApiRequestBody, ApiService, Authenticated, AuthenticationPort, BoxedError,
    CertificateSignerPort, ControllerId, Observation, ObservationSink, Observations, Peer,
    RequestGate, StateRepositoryPort,
};
use gang_testkit::TestSigner;
use gang_tls::RevocationSet;
use gang_types::{NonZeroDuration, PermissionSet};
use gang_wire::ErrorReason;
use http::{
    Request,
    header::{AUTHORIZATION, CONTENT_LENGTH},
};
use http_body_util::{BodyExt, StreamBody};
use serde_json::{Value, json};

use crate::api::{Api, ApiParts};
use crate::authorizer::Authorizer;
use crate::builds::BuildsCell;
use crate::deployment::Stack;
use crate::gate::{Caller, ExecutionListener, Gate};
use crate::info::wire_server;
use crate::ingest::Ingest;
use crate::leadership::Leading;
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::test_support::{ApiReply, identity, read, role_for, tls_settings};
use crate::test_support_deployment::TlsDeployment;
use crate::test_support_session::session_token;
use crate::test_support_session_harness::{Harness, HarnessOptions};

const PATH: &str = "/v1/builds:ingest";
const ISSUER: &str = "https://gang.example.test";
const LIMIT: usize = 1024;

struct Fixture<K: Stack = TlsDeployment> {
    harness: Harness<K>,
    api: Api<K>,
    observations: Observations,
}

async fn fixture(leader: bool, mounted: bool) -> Fixture {
    fixture_on(Harness::token(HarnessOptions::default()), leader, mounted).await
}

async fn fixture_on<K: Stack<Signer = TestSigner, Clock = TestClock>>(
    harness: Harness<K>,
    leader: bool,
    mounted: bool,
) -> Fixture<K> {
    let mut settings = tls_settings();
    settings.limits.max_request_size = LIMIT;
    let info = wire_server(
        &harness.core.host.endpoint,
        settings.default_isa,
        EndpointTrust::System,
        None,
        harness.core.clients.scheme(),
        &harness.core.host.providers,
    )
    .expect("server info");
    let leading = Leading::default();
    if leader {
        leading.set();
    }
    let (sink, observations) = ObservationSink::channel(
        NonZeroUsize::new(1).expect("nonzero"),
        NonZeroUsize::new(8).expect("nonzero"),
    );
    let ingest = if mounted {
        let public_key = harness
            .core
            .host
            .signer
            .public_key()
            .await
            .expect("public key");
        let key = ControllerKey::from_spki(&public_key).expect("controller key");
        Some(Ingest::new(
            sink,
            leading,
            key,
            ISSUER.to_owned(),
            Duration::ZERO,
            NonZeroDuration::from_secs(10),
            8,
        ))
    } else {
        None
    };
    let api = Api::new(ApiParts {
        core: Arc::clone(&harness.core),
        revocations: Revocations::new(
            Arc::clone(&harness.core),
            RevocationSet::from_serials(Vec::new()),
        ),
        info,
        authorizer: Authorizer::new(vec![role_for(
            "administrator",
            PermissionSet::all(),
            "admin",
        )]),
        limits: settings.limits,
        limiter: Arc::new(RateLimiter::new(
            &settings.rate_limit,
            Arc::clone(&harness.clock),
        )),
        dashboard: None,
        leader_routes: None,
        builds: BuildsCell::new(&BuildsConfig::default()),
        login_provider: None,
        device: None,
        ingest,
    });
    Fixture {
        harness,
        api,
        observations,
    }
}

async fn proof<K: Stack>(f: &Fixture<K>, issuer: &str) -> String {
    mint(
        &*f.harness.core.host.signer,
        issuer,
        &ControllerId::new("follower"),
        f.harness.clock.wall(),
        NonZeroDuration::from_secs(10),
    )
    .await
    .expect("proof mints")
    .as_str()
    .to_owned()
}

fn request(bytes: Vec<u8>, polls: Arc<AtomicUsize>, execution: bool) -> Request<ApiRequestBody> {
    let mut bytes = Some(Bytes::from(bytes));
    let body = StreamBody::new(stream::poll_fn(move |_| {
        polls.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(
            bytes
                .take()
                .map(|bytes| Ok::<_, BoxedError>(http_body::Frame::data(bytes))),
        )
    }))
    .boxed_unsync();
    let mut request = Request::builder()
        .method("POST")
        .uri(PATH)
        .body(body)
        .expect("request");
    if execution {
        request.extensions_mut().insert(ExecutionListener);
    }
    request
}

async fn send<K: Stack>(f: &Fixture<K>, request: Request<ApiRequestBody>) -> ApiReply {
    read(f.api.call(request).await).await
}

/// Sends `request` through `gate`, as its listener does, then to the API.
async fn through<K: Stack>(
    f: &Fixture<K>,
    gate: &Gate<K, K::Clients>,
    request: Request<ApiRequestBody>,
) -> ApiReply
where
    K::Clients: 'static,
{
    let (mut parts, body) = request.into_parts();
    gate.admit(&Peer::default(), &mut parts)
        .await
        .expect("the gate admits open paths");
    send(f, Request::from_parts(parts, body)).await
}

fn batch() -> Value {
    json!({"origins":[{"principal":"example-idp:alice","invocationId":"one"}],"calls":[{"origin":0,"rpc":"GET_ACTION_RESULT","outcome":"OK"}],"lost":[],"unattributed":0})
}

fn encoded(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("JSON")
}

#[tokio::test]
async fn listener_and_leadership_are_checked_before_the_body() {
    let f = fixture(true, true).await;
    let proof = proof(&f, ISSUER).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(encoded(&batch()), Arc::clone(&polls), false);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    send(&f, req).await.refused(ErrorReason::RouteNotFound);
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    let mut req = request(encoded(&batch()), Arc::clone(&polls), false);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    let (mut parts, body) = req.into_parts();
    let worker = Gate::worker(Arc::clone(&f.harness.core));
    assert!(worker.admit(&Peer::default(), &mut parts).await.is_err());
    send(&f, Request::from_parts(parts, body))
        .await
        .refused(ErrorReason::RouteNotFound);
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    let f = fixture(true, false).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(encoded(&batch()), Arc::clone(&polls), true);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    send(&f, req).await.refused(ErrorReason::RouteNotFound);
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    let f = fixture(false, true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let req = request(encoded(&batch()), Arc::clone(&polls), true);
    send(&f, req)
        .await
        .refused(ErrorReason::DependencyUnavailable);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ingest_requests_never_initialize_signer_key() {
    let f = fixture(true, true).await;
    let key_reads = f.harness.core.host.signer.public_key_reads();
    let valid = proof(&f, ISSUER).await;
    for _ in 0..2 {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut req = request(encoded(&batch()), Arc::clone(&polls), true);
        req.headers_mut()
            .insert(PROOF_HEADER, "x".parse().expect("header"));
        send(&f, req).await.refused(ErrorReason::CredentialsInvalid);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(f.harness.core.host.signer.public_key_reads(), key_reads);

        let mut req = request(encoded(&batch()), Arc::new(AtomicUsize::new(0)), true);
        req.headers_mut()
            .insert(PROOF_HEADER, valid.parse().expect("header"));
        assert_eq!(send(&f, req).await.value(), json!({}));
        assert_eq!(f.harness.core.host.signer.public_key_reads(), key_reads);
    }
}

/// The execution listener serves a follower's ingest whatever the client plugin would say about
/// the caller: under `none` its principal is neither sighted nor checked for revocation.
#[tokio::test]
async fn execution_ingest_neither_sights_nor_refuses_the_anonymous_principal() {
    let f = fixture_on(Harness::anonymous(HarnessOptions::default()), true, true).await;
    let gate = f.harness.gate.clone().executing();
    let proof = proof(&f, ISSUER).await;
    let host = &f.harness.core.host;
    let ingest = || {
        let mut req = request(encoded(&batch()), Arc::new(AtomicUsize::new(0)), false);
        req.headers_mut()
            .insert(PROOF_HEADER, proof.parse().expect("header"));
        req
    };

    assert_eq!(through(&f, &gate, ingest()).await.value(), json!({}));
    assert!(
        host.repository
            .principal(&anonymous())
            .await
            .expect("the principals are readable")
            .is_none(),
        "a follower's ingest records no principal sighting"
    );

    host.revoked_principals.revoke(anonymous());
    assert_eq!(through(&f, &gate, ingest()).await.value(), json!({}));
}

/// The public listener does not serve the ingest, but a refused credential is reported first, as
/// on every other route, and the body is never read.
#[tokio::test]
async fn public_ingest_reports_a_refused_credential_first() {
    let f = fixture(true, true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(encoded(&batch()), Arc::clone(&polls), false);
    req.headers_mut()
        .insert(AUTHORIZATION, "Bearer junk".parse().expect("bearer"));
    through(&f, &f.harness.gate, req)
        .await
        .refused(ErrorReason::CredentialsInvalid);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn controller_proof_is_required_before_polling_the_body() {
    let f = fixture(true, true).await;
    let valid = proof(&f, ISSUER).await;
    let wrong_issuer = proof(&f, "https://other.example.test").await;
    let mut bad_signature = valid.clone().into_bytes();
    let last = bad_signature.len() - 1;
    bad_signature[last] = if bad_signature[last] == b'0' {
        b'1'
    } else {
        b'0'
    };
    let bad_signature = String::from_utf8(bad_signature).expect("ascii proof");
    let token = session_token(&f.harness.clock, &identity("admin")).await;
    for (headers, admin, reason) in [
        (vec![], false, ErrorReason::CredentialsMissing),
        (vec![], true, ErrorReason::CredentialsMissing),
        (
            vec!["x".repeat(MAX_PROOF_BYTES + 1)],
            false,
            ErrorReason::CredentialsInvalid,
        ),
        (
            vec![valid.clone(), valid.clone()],
            false,
            ErrorReason::CredentialsInvalid,
        ),
        (vec![bad_signature], false, ErrorReason::CredentialsInvalid),
        (vec![wrong_issuer], false, ErrorReason::CredentialsInvalid),
        (
            vec![token.expose().to_owned()],
            true,
            ErrorReason::CredentialsInvalid,
        ),
    ] {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut req = request(encoded(&batch()), Arc::clone(&polls), true);
        for value in headers {
            req.headers_mut()
                .append(PROOF_HEADER, value.parse().expect("header"));
        }
        if admin {
            req.headers_mut().insert(
                AUTHORIZATION,
                format!("Bearer {}", token.expose())
                    .parse()
                    .expect("bearer"),
            );
            req.extensions_mut()
                .insert(Caller::Authenticated(Authenticated {
                    identity: Arc::new(identity("admin")),
                    valid_until: None,
                }));
        }
        send(&f, req).await.refused(reason);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    f.harness.clock.advance(Duration::from_secs(10));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(encoded(&batch()), Arc::clone(&polls), true);
    req.headers_mut()
        .insert(PROOF_HEADER, valid.parse().expect("header"));
    send(&f, req).await.refused(ErrorReason::CredentialsInvalid);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn authenticated_size_limits_and_atomic_validation() {
    let mut f = fixture(true, true).await;
    let proof = proof(&f, ISSUER).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(encoded(&batch()), Arc::clone(&polls), true);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    req.headers_mut().insert(
        CONTENT_LENGTH,
        (LIMIT + 1).to_string().parse().expect("length"),
    );
    send(&f, req).await.refused(ErrorReason::RequestTooLarge);
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    let polls = Arc::new(AtomicUsize::new(0));
    let mut req = request(vec![b'x'; LIMIT + 1], Arc::clone(&polls), true);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    send(&f, req).await.refused(ErrorReason::RequestTooLarge);
    assert!(polls.load(Ordering::SeqCst) > 0);

    let mut invalid = batch();
    invalid["calls"]
        .as_array_mut()
        .expect("calls")
        .push(json!({"origin":1,"rpc":"EXECUTE","outcome":"OK"}));
    let mut req = request(encoded(&invalid), Arc::new(AtomicUsize::new(0)), true);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    send(&f, req).await.refused(ErrorReason::MalformedRequest);
    assert!(f.observations.next().now_or_never().is_none());
    assert_eq!(
        f.observations
            .take_lost(|_, _| panic!("invalid batch applied loss")),
        0
    );
}

#[tokio::test]
async fn accepted_batch_applies_calls_losses_and_queue_overflow() {
    let mut f = fixture(true, true).await;
    let proof = proof(&f, ISSUER).await;
    let mut value = batch();
    value["calls"]
        .as_array_mut()
        .expect("calls")
        .push(json!({"origin":0,"rpc":"EXECUTE","outcome":"ERROR"}));
    value["lost"] = json!([{"origin":0,"count":3}]);
    value["unattributed"] = json!(2);
    let mut req = request(encoded(&value), Arc::new(AtomicUsize::new(0)), true);
    req.headers_mut()
        .insert(PROOF_HEADER, proof.parse().expect("header"));
    assert_eq!(send(&f, req).await.value(), json!({}));
    let observation = f
        .observations
        .next()
        .await
        .expect("queued call")
        .into_observation()
        .expect("call delivery");
    let Observation::Call(call) = observation else {
        panic!("expected call")
    };
    assert_eq!(call.origin.build.principal.as_ref(), &identity("alice"));
    assert_eq!(call.origin.build.invocation.as_str(), "one");
    let mut losses = Vec::new();
    let unattributed = f
        .observations
        .take_lost(|key, count| losses.push((key, count)));
    assert_eq!(unattributed, 2);
    assert_eq!(losses, vec![(call.origin.build, 4)]);
}
