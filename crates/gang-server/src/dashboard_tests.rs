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
    StateRepositoryPort,
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
        kind: ScalingEventKind::ScaledOut { from: 0, to: 1 },
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
            max_pending: NonZeroUsize::new(8).expect("8 is non-zero"),
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
