//! Controller observation ingestion through the API boundary.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, stream};
use gang_authn::anonymous;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_config::{BuildsConfig, EndpointTrust, LeadershipConfig};
use gang_insight::Ingested;
use gang_mtls::{ControllerKey, MAX_PROOF_BYTES, PROOF_HEADER, mint};
use gang_spi::{
    ApiRequestBody, ApiService, Authenticated, AuthenticationPort, BoxedError, Call, CallOutcome,
    CertificateSignerPort, ControllerId, Observation, ObservationSink, Observations, Origin, Peer,
    RequestGate, Rpc, StateRepositoryPort, Traffic, Transferred,
};
use gang_testkit::TestSigner;
use gang_tls::RevocationSet;
use gang_types::{BuildKey, InvocationId, NonZeroDuration, PermissionSet};
use gang_wire::{ErrorReason, IngestTraffic};
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
/// The most follower traffic series an ingest keeps.
const MAX_SERIES: NonZeroUsize = NonZeroUsize::new(8).expect("8 is non-zero");
/// When the ingesting leader began leading.
const TERM: Timestamp = Timestamp::from_unix_millis(100_000);

/// Proofs are checked without skew and live ten seconds.
fn leadership() -> LeadershipConfig {
    LeadershipConfig {
        ttl: NonZeroDuration::from_secs(10),
        clock_skew: Duration::ZERO,
        ..LeadershipConfig::default()
    }
}

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
        leading.set(harness.clock.wall());
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
            &leadership(),
            8,
            MAX_SERIES,
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

// Catches the API dropping the decoded traffic, keying a fresh series per request, or counting a
// resent batch twice.
#[tokio::test]
async fn accepted_traffic_counts_its_increase_once() {
    let f = fixture(true, true).await;
    let proof = proof(&f, ISSUER).await;
    let mut value = batch();
    value["traffic"] = json!({
        "startTime": f.harness.clock.wall().to_rfc3339(),
        "lookups": 4,
        "hits": 3,
        "uploadedBytes": 2,
        "downloadedBytes": 1,
    });
    for _ in 0..2 {
        let mut req = request(encoded(&value), Arc::new(AtomicUsize::new(0)), true);
        req.headers_mut()
            .insert(PROOF_HEADER, proof.parse().expect("header"));
        assert_eq!(send(&f, req).await.value(), json!({}));
    }
    assert_eq!(
        f.observations.traffic(),
        Traffic {
            lookups: 4,
            hits: 3,
            uploaded: 2,
            downloaded: 1,
        }
    );
}

/// An ingest whose leader leads since [`TERM`], with the observations its sink feeds.
async fn traffic_ingest() -> (Ingest, Observations) {
    capped_ingest(MAX_SERIES).await
}

/// [`traffic_ingest`] keeping at most `max_series` series.
async fn capped_ingest(max_series: NonZeroUsize) -> (Ingest, Observations) {
    let signer = TestSigner::authority().expect("signer");
    let public_key = signer.public_key().await.expect("public key");
    let key = ControllerKey::from_spki(&public_key).expect("controller key");
    let leading = Leading::default();
    leading.set(TERM);
    let (sink, observations) = ObservationSink::channel(
        NonZeroUsize::new(8).expect("nonzero"),
        NonZeroUsize::new(8).expect("nonzero"),
    );
    let ingest = Ingest::new(
        sink,
        leading,
        key,
        ISSUER.to_owned(),
        &leadership(),
        8,
        max_series,
    );
    (ingest, observations)
}

/// Totals that differ per counter, so a mixed-up counter shows, and whose growth never has more
/// hits than lookups, as real traffic does.
fn scaled(n: u64) -> Traffic {
    Traffic {
        lookups: n,
        hits: n / 2,
        uploaded: 3 * n,
        downloaded: 4 * n,
    }
}

/// A batch carrying only the cumulative totals `scaled(n)` of a process started at `start`.
fn report(start: Timestamp, n: u64) -> Ingested {
    let totals = scaled(n);
    Ingested {
        calls: Vec::new(),
        lost: Vec::new(),
        unattributed: 0,
        traffic: Some(IngestTraffic {
            start_time: start,
            lookups: totals.lookups,
            hits: totals.hits,
            uploaded_bytes: totals.uploaded,
            downloaded_bytes: totals.downloaded,
        }),
    }
}

// Catches counting cumulative totals instead of their increase, counting a resend twice, and a
// restart that is ignored or subtracted instead of counted in full.
#[tokio::test]
async fn series_counts_increases_and_a_restart_in_full() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let first = TERM + Duration::from_secs(1);
    let restart = TERM + Duration::from_secs(5);
    let restarted = Traffic {
        lookups: 18,
        hits: 8,
        uploaded: 54,
        downloaded: 72,
    };
    for (now, start, n, total) in [
        (2_000, first, 10, scaled(10)),
        (3_000, first, 15, scaled(15)),
        (4_000, first, 15, scaled(15)),
        (6_000, restart, 3, restarted),
    ] {
        ingest.apply(&a, TERM + Duration::from_millis(now), report(start, n));
        assert_eq!(observations.traffic(), total, "after {n} at {now}");
    }
}

// Catches counting traffic a follower served before this leader's term.
#[tokio::test]
async fn series_started_before_the_term_only_sets_the_baseline() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let start = TERM - Duration::from_mins(1);
    ingest.apply(&a, TERM + Duration::from_secs(1), report(start, 1000));
    assert_eq!(observations.traffic(), Traffic::default());
    ingest.apply(&a, TERM + Duration::from_secs(2), report(start, 1004));
    assert_eq!(observations.traffic(), scaled(4));
}

// Catches a delayed, lower report lowering the stored totals, so the next report recounts.
#[tokio::test]
async fn regressing_totals_never_lower_the_series() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let start = TERM + Duration::from_secs(1);
    ingest.apply(&a, TERM + Duration::from_secs(2), report(start, 10));
    ingest.apply(&a, TERM + Duration::from_secs(3), report(start, 8));
    assert_eq!(observations.traffic(), scaled(10));
    ingest.apply(&a, TERM + Duration::from_secs(4), report(start, 12));
    assert_eq!(observations.traffic(), scaled(12));
}

/// A batch carrying one call and the cumulative `totals` of a process started at `start`.
fn report_with_call(start: Timestamp, totals: Traffic, call: Call) -> Ingested {
    Ingested {
        calls: vec![call],
        lost: Vec::new(),
        unattributed: 0,
        traffic: Some(IngestTraffic {
            start_time: start,
            lookups: totals.lookups,
            hits: totals.hits,
            uploaded_bytes: totals.uploaded,
            downloaded_bytes: totals.downloaded,
        }),
    }
}

/// A cache-service call of the build `one` of `alice`.
fn cas_call() -> Call {
    Call {
        origin: Origin {
            build: BuildKey {
                principal: Arc::new(identity("alice")),
                invocation: InvocationId::try_new("one").expect("valid invocation"),
            },
            correlated: None,
            tool: None,
            target: None,
            mnemonic: None,
        },
        rpc: Rpc::Cas,
        outcome: CallOutcome::Ok,
        transferred: Transferred::default(),
    }
}

// Catches a report where any counter went backwards being folded counter by counter (taking
// the higher of each, or clamping), which counts growth from a report that cannot be trusted
// and then undercounts the next valid one; and catches the rejection dropping the batch's calls.
#[tokio::test]
async fn a_report_with_any_counter_going_backwards_is_rejected_whole() {
    let (ingest, mut observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let start = TERM + Duration::from_secs(1);
    let valid = Traffic {
        lookups: 10,
        hits: 6,
        uploaded: 100,
        downloaded: 100,
    };
    let mixed = Traffic {
        lookups: 10,
        hits: 7,
        uploaded: 99,
        downloaded: 101,
    };
    let later = Traffic {
        lookups: 12,
        hits: 8,
        uploaded: 105,
        downloaded: 103,
    };
    ingest.apply(
        &a,
        TERM + Duration::from_secs(2),
        report_with_call(start, valid, cas_call()),
    );
    assert_eq!(observations.traffic(), valid);
    ingest.apply(
        &a,
        TERM + Duration::from_secs(3),
        report_with_call(start, mixed, cas_call()),
    );
    assert_eq!(
        observations.traffic(),
        valid,
        "a backwards counter rejects the whole report"
    );
    ingest.apply(
        &a,
        TERM + Duration::from_secs(4),
        report_with_call(start, later, cas_call()),
    );
    assert_eq!(
        observations.traffic(),
        later,
        "growth counts from the last valid report"
    );
    for _ in 0..3 {
        let delivered = observations
            .next()
            .await
            .and_then(gang_spi::Delivery::into_observation);
        assert_eq!(
            delivered,
            Some(Observation::Call(cas_call())),
            "calls still record"
        );
    }
}

// Catches accepting growth no traffic can produce: every hit is a lookup, so a report adding
// more hits than lookups is rejected whole even though no counter went backwards; otherwise a
// step would show a hit without its lookup. The next valid report grows from the last valid one.
#[tokio::test]
async fn a_report_adding_more_hits_than_lookups_is_rejected_whole() {
    let (ingest, mut observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let start = TERM + Duration::from_secs(1);
    let valid = Traffic {
        lookups: 10,
        hits: 6,
        uploaded: 100,
        downloaded: 100,
    };
    let impossible = Traffic {
        lookups: 10,
        hits: 7,
        uploaded: 110,
        downloaded: 105,
    };
    let later = Traffic {
        lookups: 12,
        hits: 8,
        uploaded: 120,
        downloaded: 110,
    };
    ingest.apply(
        &a,
        TERM + Duration::from_secs(2),
        report_with_call(start, valid, cas_call()),
    );
    let at = TERM + Duration::from_secs(3);
    ingest.apply(&a, at, report_with_call(start, impossible, cas_call()));
    assert_eq!(
        observations.traffic(),
        valid,
        "a hit without its lookup rejects the report"
    );
    ingest.apply(
        &a,
        TERM + Duration::from_secs(4),
        report_with_call(start, later, cas_call()),
    );
    assert_eq!(
        observations.traffic(),
        later,
        "growth counts from the last valid report"
    );
    for _ in 0..3 {
        let delivered = observations
            .next()
            .await
            .and_then(gang_spi::Delivery::into_observation);
        assert_eq!(
            delivered,
            Some(Observation::Call(cas_call())),
            "calls still record"
        );
    }
}

// Catches a rejected report refreshing its series, which would make the cap evict a series that
// reported validly more recently instead.
#[tokio::test]
async fn a_rejected_report_keeps_its_series_least_recently_updated() {
    let (ingest, observations) = capped_ingest(NonZeroUsize::new(2).expect("nonzero")).await;
    let (a, b, c) = (
        ControllerId::new("a"),
        ControllerId::new("b"),
        ControllerId::new("c"),
    );
    let start = TERM + Duration::from_secs(1);
    let at = |secs| TERM + Duration::from_secs(secs);
    ingest.apply(&a, at(2), report(start, 10));
    ingest.apply(&b, at(3), report(start, 10));
    ingest.apply(&a, at(4), report(start, 9));
    ingest.apply(&c, at(5), report(start, 5));
    assert_eq!(observations.traffic(), scaled(25));
    ingest.apply(&b, at(6), report(start, 12));
    assert_eq!(
        observations.traffic(),
        scaled(27),
        "b was kept and adds only its growth"
    );
}

// Catches forgetting a quiet follower's watermark (by age, or while others report), which would
// count its whole totals again when it reports once more.
#[tokio::test]
async fn quiet_series_resume_without_recounting() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    let start = TERM + Duration::from_secs(1);
    ingest.apply(&a, TERM + Duration::from_secs(2), report(start, 10));
    for hour in 1..=24 {
        let now = TERM + Duration::from_hours(hour);
        ingest.apply(&b, now, report(TERM - Duration::from_secs(1), 0));
    }
    ingest.apply(&a, TERM + Duration::from_hours(25), report(start, 10));
    assert_eq!(
        observations.traffic(),
        scaled(10),
        "an unchanged report adds nothing"
    );
    ingest.apply(&a, TERM + Duration::from_secs(90_001), report(start, 13));
    assert_eq!(observations.traffic(), scaled(13));
}

// Catches a follower cut off for longer than the dashboard window being treated as new when it
// reconnects: only what it counted meanwhile is added.
#[tokio::test]
async fn a_partitioned_follower_adds_only_what_it_counted_meanwhile() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let start = TERM + Duration::from_secs(1);
    ingest.apply(&a, TERM + Duration::from_secs(2), report(start, 10));
    ingest.apply(&a, TERM + Duration::from_hours(2), report(start, 25));
    assert_eq!(observations.traffic(), scaled(25));
}

// Catches an unbounded map, evicting anything but the least recently updated series, a surviving
// series recounted after an eviction, and an evicted series that can never report again.
#[tokio::test]
async fn the_cap_evicts_the_least_recently_updated_series() {
    let (ingest, observations) = capped_ingest(NonZeroUsize::new(2).expect("nonzero")).await;
    let (a, b, c) = (
        ControllerId::new("a"),
        ControllerId::new("b"),
        ControllerId::new("c"),
    );
    let start = TERM + Duration::from_secs(1);
    let at = |secs| TERM + Duration::from_secs(secs);
    ingest.apply(&a, at(2), report(start, 10));
    ingest.apply(&b, at(3), report(start, 10));
    ingest.apply(&c, at(4), report(start, 5));
    assert_eq!(observations.traffic(), scaled(25));
    ingest.apply(&b, at(5), report(start, 12));
    assert_eq!(
        observations.traffic(),
        scaled(27),
        "b survived and adds only its growth"
    );
    ingest.apply(&a, at(6), report(start, 10));
    assert_eq!(
        observations.traffic(),
        scaled(37),
        "evicted a counts as a new process"
    );
}

// Catches keying series by anything in the payload: two controllers reporting the same process
// start and totals are two series.
#[tokio::test]
async fn series_are_keyed_by_the_admitted_controller() {
    let (ingest, observations) = traffic_ingest().await;
    let start = TERM + Duration::from_secs(1);
    let now = TERM + Duration::from_secs(2);
    ingest.apply(&ControllerId::new("a"), now, report(start, 10));
    ingest.apply(&ControllerId::new("b"), now, report(start, 10));
    assert_eq!(observations.traffic(), scaled(20));
    ingest.apply(&ControllerId::new("a"), now, report(start, 10));
    assert_eq!(observations.traffic(), scaled(20));
}

// Catches a delayed batch of an older process replacing the newer series, which would count the
// old totals and then the newer process in full again.
#[tokio::test]
async fn delayed_older_process_does_not_replace_the_newer_one() {
    let (ingest, observations) = traffic_ingest().await;
    let a = ControllerId::new("a");
    let older = TERM + Duration::from_secs(1);
    let newer = TERM + Duration::from_secs(5);
    ingest.apply(&a, TERM + Duration::from_secs(6), report(newer, 5));
    ingest.apply(&a, TERM + Duration::from_secs(7), report(older, 100));
    assert_eq!(observations.traffic(), scaled(5));
    ingest.apply(&a, TERM + Duration::from_secs(8), report(newer, 7));
    assert_eq!(observations.traffic(), scaled(7));
}

// Catches counting the bytes of forwarded calls on top of the series increase that already holds
// them, and dropping the calls themselves.
#[tokio::test]
async fn forwarded_calls_are_recorded_without_counting_traffic() {
    let (ingest, mut observations) = traffic_ingest().await;
    let call = Call {
        origin: Origin {
            build: BuildKey {
                principal: Arc::new(identity("alice")),
                invocation: InvocationId::try_new("one").expect("valid invocation"),
            },
            correlated: None,
            tool: None,
            target: None,
            mnemonic: None,
        },
        rpc: Rpc::Cas,
        outcome: CallOutcome::Ok,
        transferred: Transferred {
            uploaded: 50,
            downloaded: 70,
        },
    };
    let mut batch = report(TERM + Duration::from_secs(1), 1);
    batch.calls.push(call.clone());
    ingest.apply(
        &ControllerId::new("a"),
        TERM + Duration::from_secs(2),
        batch,
    );
    assert_eq!(observations.traffic(), scaled(1));
    let delivered = observations
        .next()
        .await
        .and_then(gang_spi::Delivery::into_observation);
    assert_eq!(delivered, Some(Observation::Call(call)));
}
