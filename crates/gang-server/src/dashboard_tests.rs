//! The dashboard's startup state and routes. The tests drive the dashboard both directly and
//! through a prepared server.

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::dashboard::Dashboard;
use crate::{LoginSettings, Server, ServerError, ServerSettings};
use gang_clock::Timestamp;
use gang_config::DashboardConfig;
use gang_spi::{
    ApiResponseBody, ControllerReady, LoginClient, ScalingAuditEvent, ScalingEventKind,
    SchedulerSnapshot, StateRepositoryPort,
};
use gang_testkit::{Controller, FakeRepository};
use gang_types::{
    AccessToken, ClientSecret, FleetName, NonZeroDuration, ProviderName, SessionMaxAge,
};
use gang_wire::routes::{
    GET_SERVER, GET_SESSION, GET_STATE, RESTORE_PRINCIPAL, REVOKE_PRINCIPAL, WATCH_STATE,
};
use gang_wire::{ErrorReason, ErrorResponse, PassState, Session, SkipReason, State, name};
use http::StatusCode;
use http::header::{CONTENT_SECURITY_POLICY, CONTENT_TYPE};
use http_body_util::BodyExt;
use tokio::sync::oneshot;

use crate::test_support::{
    api_request, decode_sse, mint_token, plain_parts, plain_settings, read, serve, start_clock,
    tls_parts, tls_settings,
};

const PROVIDER: &str = "example-idp";
const ADMIN_PROOF: &str = "admin-proof";
const ALICE: &str = "alice";

/// A scaling audit event at `millis`.
fn scaled_out(millis: u64) -> ScalingAuditEvent {
    ScalingAuditEvent {
        at: Timestamp::from_unix_millis(millis),
        fleet: FleetName::try_new("gang-x86").expect("fixture fleet name is valid"),
        kind: ScalingEventKind::ScaledOut {
            from: 0,
            to: 1,
            detail: None,
        },
        cause: None,
    }
}

#[tokio::test]
async fn the_state_starts_skipped_with_the_newest_events_oldest_first() {
    let repository = FakeRepository::default();
    for millis in [1_000, 2_000, 3_000] {
        repository
            .record_scaling_event(&scaled_out(millis))
            .await
            .expect("the ledger accepts the fixture event");
    }
    let config = DashboardConfig {
        recent_events: NonZeroUsize::new(2).expect("2 is non-zero"),
        ..DashboardConfig::default()
    };
    let clock = start_clock();
    let dashboard = match Dashboard::start(
        config,
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &clock,
        &repository,
    )
    .await
    {
        Ok(dashboard) => dashboard,
        Err(error) => panic!("dashboard starts: {error}"),
    };
    let bytes = dashboard
        .api()
        .hub
        .state_json()
        .expect("the initial state encodes");
    let state: State = serde_json::from_slice(&bytes).expect("the state is JSON of the state type");
    assert_eq!(state.name, name::STATE);
    assert_eq!(state.seq, 0);
    assert_eq!(state.provisioner, "test");
    assert_eq!(state.events_limit, 2);
    assert!(state.fleets.is_empty());
    assert!(state.workers.is_empty());
    assert!(state.instances.is_empty());
    assert!(state.actions.is_none());
    assert_eq!(
        state.pass.state,
        PassState::Skipped,
        "the pass is skipped: {:?}",
        state.pass
    );
    assert_eq!(
        state.pass.skip_reason,
        Some(SkipReason::SchedulerUnavailable)
    );
    assert_eq!(
        state.pass.last_ok_time, None,
        "no pass has succeeded: {:?}",
        state.pass
    );
    let names: Vec<&str> = state
        .events
        .iter()
        .map(|event| event.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["events/2", "events/3"],
        "the newest two events, oldest first"
    );
}

#[tokio::test]
async fn forwarding_publishes_the_current_revocation_epoch() -> gang_testkit::TestResult {
    let repository = FakeRepository::default();
    let clock = start_clock();
    let dashboard = Dashboard::start(
        DashboardConfig::default(),
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &clock,
        &repository,
    )
    .await?;
    let revoked = crate::principals::RevokedPrincipals::new();
    revoked.revoke(gang_types::Identity::new(
        ProviderName::try_new("example-idp")?,
        gang_types::Principal::try_new("alice")?,
    ));
    let forward = dashboard.forward_principals(&revoked);
    tokio::pin!(forward);
    assert!(futures::poll!(forward.as_mut()).is_pending());
    let state: State = serde_json::from_slice(&dashboard.api().hub.state_json()?)?;
    assert_eq!(state.principals_epoch, 1);
    Ok(())
}

#[tokio::test]
async fn promotion_reloads_events_written_while_following() -> gang_testkit::TestResult {
    let repository = FakeRepository::default();
    let clock = start_clock();
    let dashboard = Dashboard::start(
        DashboardConfig::default(),
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &clock,
        &repository,
    )
    .await?;
    repository.record_scaling_event(&scaled_out(1_000)).await?;
    dashboard.reload_from(&repository).await?;
    dashboard.reload_from(&repository).await?;
    let state: State = serde_json::from_slice(&dashboard.api().hub.state_json()?)?;
    assert_eq!(state.events.len(), 1);
    assert_eq!(state.events[0].name, "events/1");
    Ok(())
}

#[tokio::test]
async fn prepare_refuses_when_the_scaling_events_cannot_be_read() {
    let (parts, _dials, handles) = tls_parts();
    handles.repository.fail_scaling_reads(true);
    let settings = ServerSettings {
        dashboard: DashboardConfig {
            enabled: true,
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    match Server::prepare(parts, settings).await {
        Err(ServerError::DashboardEvents(_)) => {}
        Err(error) => panic!("prepare reports the unreadable events: {error}"),
        Ok(_) => panic!("prepare must fail when the scaling events cannot be read"),
    }
}

#[tokio::test]
async fn a_disabled_dashboard_builds_nothing_and_serves_no_state() {
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        dashboard: DashboardConfig {
            enabled: false,
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    serve(
        parts,
        settings,
        handles.scheduler.clone(),
        |controller| async move {
            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            for route in [GET_STATE, WATCH_STATE] {
                let request = api_request("GET", route.template, Some(&token), None);
                let response = controller.call_api(request).await;
                let reply = read(response).await;
                let status = reply.status;
                let envelope = reply.json::<ErrorResponse>();
                assert_eq!(status, StatusCode::NOT_FOUND, "{}", route.template);
                assert_eq!(
                    envelope.error.reason(),
                    Some(ErrorReason::RouteNotFound),
                    "{}",
                    route.template
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn ui_root_serves_the_dashboard_html_with_its_csp() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler.clone(),
        |controller| async move {
            for path in ["/ui", "/ui/"] {
                let response = controller
                    .call_api(api_request("GET", path, None, None))
                    .await;
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                assert!(
                    response.headers().contains_key(CONTENT_SECURITY_POLICY),
                    "{path} carries the CSP header"
                );
                let status = read(response).await.status;
                assert_eq!(status, StatusCode::OK, "{path}");
                assert!(
                    content_type.is_some_and(|value| value.starts_with("text/html")),
                    "{path} is text/html"
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn session_reports_the_anonymous_identity_without_login_settings() {
    let (parts, _dials, handles) = plain_parts();
    serve(
        parts,
        plain_settings(),
        handles.scheduler.clone(),
        |controller| async move {
            let response = controller
                .call_api(api_request("GET", GET_SESSION.template, None, None))
                .await;
            let reply = read(response).await;
            let status = reply.status;
            let session = reply.json::<Session>();
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                session.principal.as_deref(),
                Some("principals/none%3Aanonymous"),
                "anonymous: {session:?}"
            );
            assert_eq!(session.permissions, Vec::new(), "no permissions");
            assert_eq!(session.login, None, "no login configured");
        },
    )
    .await;
}

#[tokio::test]
async fn a_disabled_dashboard_does_not_serve_ui() {
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        dashboard: DashboardConfig {
            enabled: false,
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    serve(
        parts,
        settings,
        handles.scheduler.clone(),
        |controller| async move {
            // An anonymous request to a path the API does not know is refused as unauthenticated.
            let response = controller
                .call_api(api_request("GET", "/ui", None, None))
                .await;
            let reply = read(response).await;
            let status = reply.status;
            let body = reply.body;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(
                !String::from_utf8_lossy(&body).contains("<html"),
                "the dashboard's HTML is not served"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn server_info_never_contains_the_dashboard_client_secret() {
    const SECRET: &str = "distinctive-dashboard-client-secret-7f3a";
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        login: Some(LoginSettings {
            provider: ProviderName::try_new(PROVIDER).expect("test provider name is valid"),
            client: LoginClient {
                id: "web-client".to_owned(),
                secret: ClientSecret::new(SECRET),
            },
            ttl: NonZeroDuration::from_mins(10),
            session_max_age: SessionMaxAge::from_units(43_200),
            renewal_reuse: NonZeroDuration::from_mins(1),
            device: None,
        }),
        ..tls_settings()
    };
    serve(
        parts,
        settings,
        handles.scheduler.clone(),
        |controller| async move {
            let response = controller
                .call_api(api_request("GET", GET_SERVER.template, None, None))
                .await;
            let reply = read(response).await;
            let status = reply.status;
            let body = reply.body;
            assert_eq!(status, StatusCode::OK);
            let text = String::from_utf8_lossy(&body);
            assert!(
                !text.contains(SECRET),
                "the server info leaks the secret: {text}"
            );
        },
    )
    .await;
}

async fn warning_api_fixture() -> (
    crate::api::Api<crate::test_support_deployment::TlsDeployment>,
    crate::hub::StateHub,
) {
    use std::sync::Arc;

    use crate::api::{Api, ApiParts};
    use crate::authorizer::Authorizer;
    use crate::rate_limit::RateLimiter;
    use crate::revocation::Revocations;
    use crate::test_support::{Harness, HarnessOptions, role_for};
    use gang_config::EndpointTrust;
    use gang_spi::Scheme;
    use gang_tls::RevocationSet;
    use gang_types::PermissionSet;

    let harness = Harness::token(HarnessOptions::default());
    let settings = tls_settings();
    let dashboard = Dashboard::start(
        DashboardConfig::default(),
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &*harness.clock,
        &harness.core.host.repository,
    )
    .await
    .expect("dashboard starts");
    let hub = dashboard.api().hub;
    let info = crate::info::wire_server(
        &harness.core.host.endpoint,
        settings.default_isa,
        EndpointTrust::System,
        None,
        Scheme::Token,
        &harness.core.host.providers,
    )
    .expect("server info");
    let api = Api::new(ApiParts {
        readiness: crate::health::Readiness::for_test(),
        core: Arc::clone(&harness.core),
        revocations: Revocations::new(
            Arc::clone(&harness.core),
            RevocationSet::from_serials(Vec::new()),
            crate::health::Readiness::for_test(),
        ),
        info,
        authorizer: Authorizer::new(vec![role_for("reader", PermissionSet::all(), ALICE)]),
        limits: settings.limits,
        dashboard: Some(dashboard.api()),
        builds: crate::builds::BuildsCell::new(&gang_config::BuildsConfig::default()),
        limiter: Arc::new(RateLimiter::new(
            &settings.rate_limit,
            Arc::clone(&harness.clock),
        )),
        login_provider: None,
        device: None,
    });
    (api, hub)
}

async fn warning_api_get(
    api: &crate::api::Api<crate::test_support_deployment::TlsDeployment>,
    path: &str,
) -> http::Response<ApiResponseBody> {
    use gang_spi::{ApiService, Authenticated};
    use std::sync::Arc;

    let mut request = api_request("GET", path, None, None);
    request
        .extensions_mut()
        .insert(crate::gate::Caller::Authenticated(Authenticated {
            identity: Arc::new(crate::test_support::identity(ALICE)),
            valid_until: None,
        }));
    let response = api.call(request).await;
    assert_eq!(response.status(), StatusCode::OK);
    response
}

fn raw_sse_payload(text: &str) -> serde_json::Value {
    let data = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .expect("SSE frame has data");
    serde_json::from_str(data).expect("SSE data is JSON")
}

#[tokio::test]
async fn state_and_watch_preserve_and_clear_launch_warning() {
    use gang_wire::{AwsEvent, AwsEventKind, Event, EventChange, Fleet, LaunchWarning};

    let (api, hub) = warning_api_fixture().await;
    let response = warning_api_get(&api, WATCH_STATE.template).await;
    let mut stream = response.into_body();
    let initial = next_text(&mut stream).await.expect("initial snapshot");
    assert_eq!(sse_event(&initial), "snapshot");

    let at = Timestamp::from_unix_millis(1_000);
    let reason = "insufficient capacity";
    let failure = Event {
        name: "events/aws-failure".to_owned(),
        create_time: at,
        fleet: "fleets/example-fleet".to_owned(),
        cause: None,
        change: EventChange::Aws {
            event: AwsEvent {
                id: "failure".to_owned(),
                at,
                kind: AwsEventKind::CapacityUnavailable {
                    reason: reason.to_owned(),
                },
            },
        },
    };
    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.fleets = vec![Fleet {
            name: "fleets/example-fleet".to_owned(),
            launch_warning: Some(LaunchWarning {
                reason: reason.to_owned(),
                at,
            }),
            ..Fleet::default()
        }];
        cur.status_events = vec![failure.clone()];
    });
    let delta = next_text(&mut stream).await.expect("warning delta");
    assert_eq!(sse_event(&delta), "delta");
    let expected = serde_json::json!({"reason": reason, "at": "1970-01-01T00:00:01.000Z"});
    let delta = raw_sse_payload(&delta);
    assert_eq!(delta["fleets"]["upsert"][0]["launchWarning"], expected);
    assert_eq!(
        delta["statusEvents"][0],
        serde_json::to_value(&failure).expect("event JSON")
    );

    let response = warning_api_get(&api, GET_STATE.template).await;
    let json: serde_json::Value = read(response).await.json();
    assert_eq!(json["fleets"][0]["launchWarning"], expected);
    assert_eq!(
        json["statusEvents"][0],
        serde_json::to_value(&failure).expect("event JSON")
    );
    let response = warning_api_get(&api, WATCH_STATE.template).await;
    let mut viewer = response.into_body();
    let snapshot = next_text(&mut viewer).await.expect("warning snapshot");
    assert_eq!(sse_event(&snapshot), "snapshot");
    let snapshot = raw_sse_payload(&snapshot);
    assert_eq!(snapshot["fleets"][0]["launchWarning"], expected);
    assert_eq!(snapshot["statusEvents"][0], json["statusEvents"][0]);

    hub.publish(|cur, prev, seq, _| {
        cur.clone_from(prev);
        cur.seq = seq;
        cur.fleets[0].launch_warning = None;
    });
    let delta = next_text(&mut viewer).await.expect("cleared warning delta");
    assert_eq!(sse_event(&delta), "delta");
    let delta = raw_sse_payload(&delta);
    assert!(delta["fleets"]["upsert"][0].get("launchWarning").is_none());
    let response = warning_api_get(&api, GET_STATE.template).await;
    let cleared: serde_json::Value = read(response).await.json();
    assert!(cleared["fleets"][0].get("launchWarning").is_none());
    assert_eq!(cleared["statusEvents"][0], json["statusEvents"][0]);
}

fn assert_fleet_sample_shape(fleet: &serde_json::Value) {
    assert_eq!(fleet["name"], "fleets/example-fleet");
    assert_eq!(fleet["max"], 4);
    assert!(fleet["desired"].is_u64());
    for field in ["pending", "inService", "terminating", "warm"] {
        assert!(fleet["instances"][field].is_u64(), "{field}");
    }
}

#[tokio::test]
async fn snapshot_carries_the_recent_window_after_completions() {
    let (parts, _dials, handles) = tls_parts();
    let scheduler = handles.scheduler.clone();
    let clock = handles.clock.clone();
    let settings = ServerSettings {
        poll_interval: NonZeroDuration::from_secs(10),
        dashboard: DashboardConfig {
            history_window: NonZeroDuration::from_mins(5),
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let response = controller
                .call_api(api_request("GET", WATCH_STATE.template, Some(&token), None))
                .await;
            assert_eq!(response.status(), StatusCode::OK);
            let mut stream = response.into_body();
            let first = next_text(&mut stream).await.expect("initial snapshot");
            let gang_wire::Frame::Snapshot(initial) = decode_sse(first.as_bytes()) else {
                panic!("first frame is a snapshot");
            };
            wait_for_initial_observation(&mut stream, &initial.recent).await;

            for completed in [4, 9, 15] {
                scheduler.set_snapshot(SchedulerSnapshot {
                    completed,
                    ..SchedulerSnapshot::default()
                });
                clock.advance(Duration::from_secs(10));
                loop {
                    let text = next_text(&mut stream).await.expect("completed pass");
                    if let gang_wire::Frame::Delta(delta) = decode_sse(text.as_bytes())
                        && delta.completed == Some(completed)
                    {
                        let census = &sse_payload(&text)["recent"]["samples"];
                        let entries = census.as_array().expect("delta samples");
                        let fleet = &entries.last().expect("latest sample")["fleets"][0];
                        assert_fleet_sample_shape(fleet);
                        break;
                    }
                }
            }

            let response = controller
                .call_api(api_request("GET", GET_STATE.template, Some(&token), None))
                .await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = response
                .into_body()
                .collect()
                .await
                .expect("state body")
                .to_bytes();
            let state: State = serde_json::from_slice(&bytes).expect("state JSON");
            let json: serde_json::Value = serde_json::from_slice(&bytes).expect("state JSON");
            let fleet = &json["recent"]["samples"][3]["fleets"][0];
            assert_fleet_sample_shape(fleet);
            assert_eq!(state.recent.window, Duration::from_mins(5));
            assert_eq!(state.recent.step, Duration::from_secs(10));
            assert_eq!(state.recent.samples.len(), 4);
            assert_eq!(
                state
                    .recent
                    .samples
                    .iter()
                    .map(|sample| sample.completed)
                    .collect::<Vec<_>>(),
                vec![0, 4, 5, 6]
            );
            assert!(state.recent.samples.iter().all(|s| s.passes == 1));
            assert!(state.recent.samples.windows(2).all(|pair| {
                pair[1].start.as_unix_millis() - pair[0].start.as_unix_millis() == 10_000
            }));

            let response = controller
                .call_api(api_request("GET", WATCH_STATE.template, Some(&token), None))
                .await;
            assert_eq!(response.status(), StatusCode::OK);
            let mut stream = response.into_body();
            let first = next_text(&mut stream).await.expect("new viewer snapshot");
            let gang_wire::Frame::Snapshot(snapshot) = decode_sse(first.as_bytes()) else {
                panic!("first frame is a snapshot");
            };
            assert_eq!(snapshot.recent, state.recent);
            assert_eq!(
                sse_payload(&first)["recent"]["samples"][3]["fleets"],
                json["recent"]["samples"][3]["fleets"]
            );
        },
    )
    .await;
}

async fn wait_for_initial_observation(body: &mut ApiResponseBody, initial: &gang_wire::Recent) {
    if initial
        .samples
        .last()
        .is_none_or(|sample| sample.scheduler_known != Some(true))
    {
        loop {
            let text = next_text(body).await.expect("initial observation");
            if let gang_wire::Frame::Delta(delta) = decode_sse(text.as_bytes())
                && delta.recent.as_ref().is_some_and(|recent| {
                    recent
                        .samples
                        .last()
                        .is_some_and(|sample| sample.scheduler_known == Some(true))
                })
            {
                break;
            }
        }
    }
}

/// The next frame of `body` as text, or `None` when the body ends or fails.
async fn next_text(body: &mut ApiResponseBody) -> Option<String> {
    let frame = body.frame().await?.ok()?;
    let data = frame.into_data().ok()?;
    Some(String::from_utf8_lossy(&data).into_owned())
}

#[tokio::test]
async fn shutdown_closes_viewer_streams_before_run_returns() {
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        dashboard: DashboardConfig {
            enabled: true,
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    let server = match Server::prepare(parts, settings).await {
        Ok(server) => server,
        Err(error) => panic!("server prepares: {error}"),
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let viewer = async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("the controller is published");
        let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
        let request = api_request("GET", WATCH_STATE.template, Some(&token), None);
        let response = controller.call_api(request).await;
        let (response_parts, mut body) = response.into_parts();
        assert_eq!(response_parts.status, StatusCode::OK);
        let snapshot = next_text(&mut body)
            .await
            .expect("the viewer gets the snapshot first");
        assert!(
            snapshot.contains("event: snapshot"),
            "the first frame is the snapshot: {snapshot}"
        );
        let _ = stop.send(());
        // Heartbeats may still arrive while the server shuts down; the closed frame ends them.
        let closed = loop {
            let frame = next_text(&mut body)
                .await
                .expect("the stream ends with a closed frame");
            if frame.contains("event: closed") {
                break frame;
            }
            assert!(
                frame.contains("event: tick"),
                "only heartbeats precede the closed frame: {frame}"
            );
        };
        assert!(
            closed.starts_with("retry: "),
            "the closed frame carries a retry hint: {closed}"
        );
        assert_eq!(sse_payload(&closed)["reason"], "SHUTDOWN");
        assert!(
            next_text(&mut body).await.is_none(),
            "the stream ends after the closed frame"
        );
    };
    let (result, ()) = tokio::join!(
        server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ),
        viewer
    );
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}

/// The event name of an SSE frame.
fn sse_event(text: &str) -> &'static str {
    decode_sse(text.as_bytes()).event_name()
}

/// The JSON payload of an SSE frame.
fn sse_payload(text: &str) -> serde_json::Value {
    match decode_sse(text.as_bytes()) {
        gang_wire::Frame::Snapshot(value) => serde_json::to_value(value),
        gang_wire::Frame::Delta(value) => serde_json::to_value(value),
        gang_wire::Frame::Tick(value) => serde_json::to_value(value),
        gang_wire::Frame::Closed(value) => serde_json::to_value(value),
    }
    .expect("frame payload serializes")
}

/// Posts `path` as the holder of `admin` and returns the status of the answer.
async fn post_as(controller: &Controller, admin: &AccessToken, path: &str) -> StatusCode {
    let response = controller
        .call_api(api_request("POST", path, Some(admin), None))
        .await;
    read(response).await.status
}

#[tokio::test]
async fn a_revocation_advances_the_principals_epoch_on_the_live_stream() {
    let (parts, _dials, handles) = tls_parts();
    let settings = ServerSettings {
        dashboard: DashboardConfig {
            enabled: true,
            heartbeat: NonZeroDuration::from_hours(1),
            ..DashboardConfig::default()
        },
        ..tls_settings()
    };
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let request = api_request("GET", WATCH_STATE.template, Some(&admin), None);
            let response = controller.call_api(request).await;
            let (response_parts, mut body) = response.into_parts();
            assert_eq!(response_parts.status, StatusCode::OK);
            let snapshot = next_text(&mut body)
                .await
                .expect("the viewer gets the snapshot first");
            assert_eq!(sse_event(&snapshot), "snapshot");
            assert_eq!(sse_payload(&snapshot)["principalsEpoch"], 0);

            let alice = format!("{PROVIDER}:{ALICE}");
            let revoke = REVOKE_PRINCIPAL.path(&alice.parse().expect("valid identity"));
            assert_eq!(
                post_as(&controller, &admin, &revoke).await,
                StatusCode::OK,
                "the administrator revokes alice"
            );
            let revoked = next_text(&mut body)
                .await
                .expect("the revocation is streamed");
            assert_eq!(sse_event(&revoked), "delta");
            assert_eq!(sse_payload(&revoked)["principalsEpoch"], 1);

            let restore = RESTORE_PRINCIPAL.path(&alice.parse().expect("valid identity"));
            assert_eq!(
                post_as(&controller, &admin, &restore).await,
                StatusCode::OK,
                "the administrator restores alice"
            );
            let restored = next_text(&mut body)
                .await
                .expect("the restoration is streamed");
            assert_eq!(sse_event(&restored), "delta");
            assert_eq!(sse_payload(&restored)["principalsEpoch"], 2);
        },
    )
    .await;
}
