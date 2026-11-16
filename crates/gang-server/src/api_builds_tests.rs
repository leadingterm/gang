//! Live build API and recorder behavior.

use std::num::NonZeroUsize;
use std::sync::Arc;

use gang_clock::{TestClock, Timestamp};
use gang_config::{BuildsConfig, EndpointTrust};
use gang_spi::{
    ApiService, Authenticated, Call, CallOutcome, Observation, ObservationSink, Origin, Rpc, Scheme,
};
use gang_tls::RevocationSet;
use gang_types::{BuildKey, InvocationId, Permission, PermissionSet};
use gang_wire::{Build, ErrorReason, ListBuildsResponse};

use crate::api::{Api, ApiParts};
use crate::api_test_support::violated_fields;
use crate::authorizer::Authorizer;
use crate::builds::BuildsCell;
use crate::gate::Caller;
use crate::info::wire_server;
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::test_support::{ApiReply, api_request, identity, read, role_for, tls_settings};
use crate::test_support_deployment::TlsDeployment;
use crate::test_support_session_harness::{Harness, HarnessOptions};

fn observation(invocation: &str) -> Observation {
    Observation::Call(Call {
        origin: Origin {
            build: BuildKey {
                principal: Arc::new(identity("alice")),
                invocation: InvocationId::try_new(invocation).expect("valid invocation"),
            },
            correlated: None,
            tool: None,
            target: None,
            mnemonic: None,
        },
        rpc: Rpc::GetActionResult,
        outcome: CallOutcome::Ok,
    })
}

fn fixture(permission: Permission) -> (Api<TlsDeployment>, BuildsCell) {
    let harness = Harness::token(HarnessOptions::default());
    let settings = tls_settings();
    let info = wire_server(
        &harness.core.host.endpoint,
        settings.default_isa,
        EndpointTrust::System,
        None,
        Scheme::Token,
        &harness.core.host.providers,
    )
    .expect("server info");
    let builds = BuildsCell::new(&BuildsConfig::default());
    let api = Api::new(ApiParts {
        core: Arc::clone(&harness.core),
        revocations: Revocations::new(
            Arc::clone(&harness.core),
            RevocationSet::from_serials(Vec::new()),
        ),
        info,
        authorizer: Authorizer::new(vec![role_for(
            "reader",
            PermissionSet::EMPTY.with(permission),
            "alice",
        )]),
        limits: settings.limits,
        limiter: Arc::new(RateLimiter::new(
            &settings.rate_limit,
            Arc::clone(&harness.clock),
        )),
        dashboard: None,
        builds: builds.clone(),
        login_provider: None,
        device: None,
    });
    (api, builds)
}

async fn get(api: &Api<TlsDeployment>, path: &str) -> ApiReply {
    let mut request = api_request("GET", path, None, None);
    request
        .extensions_mut()
        .insert(Caller::Authenticated(Authenticated {
            identity: Arc::new(identity("alice")),
            valid_until: None,
        }));
    read(api.call(request).await).await
}

#[tokio::test]
async fn builds_read_is_enforced() {
    let (denied, _) = fixture(Permission::EventsRead);
    let (allowed, builds) = fixture(Permission::BuildsRead);
    let obs = observation("one");
    let path = format!("/v1/builds/{}", obs.origin().build.id());
    builds.observe(Timestamp::from_unix_millis(1_000), &obs);
    for route in ["/v1/builds", path.as_str()] {
        get(&denied, route)
            .await
            .refused(ErrorReason::MissingPermission);
        assert_eq!(get(&allowed, route).await.status, http::StatusCode::OK);
    }
}

#[tokio::test]
async fn list_and_get_show_recorded_builds() {
    let (api, builds) = fixture(Permission::BuildsRead);
    let first = observation("one");
    let second = observation("two");
    builds.observe(Timestamp::from_unix_millis(1_000), &first);
    builds.observe(Timestamp::from_unix_millis(2_000), &second);
    let all: ListBuildsResponse = get(&api, "/v1/builds").await.ok();
    assert_eq!(
        all.builds
            .iter()
            .map(|b| b.invocation_id.as_str())
            .collect::<Vec<_>>(),
        ["two", "one"]
    );
    let filtered: ListBuildsResponse =
        get(&api, "/v1/builds?filter=invocationId%20%3D%20%22one%22")
            .await
            .ok();
    assert_eq!(filtered.builds.len(), 1);
    assert_eq!(filtered.builds[0].invocation_id, "one");
    let page: ListBuildsResponse = get(&api, "/v1/builds?pageSize=1").await.ok();
    assert_eq!(page.builds[0].invocation_id, "two");
    let token = page.next_page_token.expect("another page follows");
    let next: ListBuildsResponse = get(
        &api,
        &format!("/v1/builds?pageSize=1&pageToken={}", token.as_str()),
    )
    .await
    .ok();
    assert_eq!(next.builds[0].invocation_id, "one");
    assert!(next.next_page_token.is_none());
    let full: Build = get(&api, &format!("/v1/builds/{}", first.origin().build.id()))
        .await
        .ok();
    assert!(full.timeline.is_some());
    let error = get(&api, "/v1/builds/0000000000000000")
        .await
        .refused(ErrorReason::BuildNotFound);
    assert_eq!(
        crate::api_test_support::metadata(&error, "build"),
        "builds/0000000000000000"
    );
    let error = get(&api, "/v1/builds/malformed")
        .await
        .refused(ErrorReason::MalformedRequest);
    assert_eq!(violated_fields(&error), ["build"]);
    let error = get(&api, "/v1/builds?filter=bad")
        .await
        .refused(ErrorReason::MalformedRequest);
    assert_eq!(violated_fields(&error), ["filter"]);
}

#[tokio::test]
async fn the_recorder_folds_observations_and_losses() {
    let builds = BuildsCell::new(&BuildsConfig::default());
    let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let obs = observation("one");
    let id = obs.origin().build.id();
    sink.record(obs.clone());
    sink.record(obs);
    let now = Timestamp::from_unix_millis(1_000);
    let clock = TestClock::new(now);
    let recorder = builds.record(observations, &clock);
    tokio::pin!(recorder);
    tokio::select! {
        biased;
        never = &mut recorder => match never {},
        () = tokio::task::yield_now() => {},
    }
    let build = builds
        .read(|builds| builds.get(id, now))
        .expect("recorded build");
    assert_eq!(build.unrecorded, 1);
    assert_eq!(build.usage.cache.hits, 1);
}

#[tokio::test]
async fn recorder_drains_losses_without_another_observation() {
    let builds = BuildsCell::new(&BuildsConfig::default());
    let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let obs = observation("one");
    let id = obs.origin().build.id();
    let now = Timestamp::from_unix_millis(1_000);
    let clock = TestClock::new(now);
    sink.record(obs.clone());
    let recorder = builds.record(observations, &clock);
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    let first = builds
        .read(|builds| builds.get(id, now))
        .expect("first observation folded");
    assert_eq!(first.usage.cache.hits, 1);
    assert_eq!(first.unrecorded, 0);
    sink.record(obs.clone());
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, now))
        .expect("recorded build");
    assert_eq!(build.unrecorded, 1);
    assert_eq!(build.usage.cache.hits, 2);
}
