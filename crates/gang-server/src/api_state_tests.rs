//! `GET /v1/state` and `GET /v1/state:watch`: the dashboard state and its live stream.

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::ServerSettings;
use gang_config::DashboardConfig;
use gang_spi::ApiResponseBody;
use gang_state::sse::Decoder;
use gang_testkit::Controller;
use gang_types::{AccessToken, NonZeroDuration, Permission, PermissionSet};
use gang_wire::routes::{GET_STATE, WATCH_STATE};
use gang_wire::{ErrorReason, Frame, State};
use http::StatusCode;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use http_body_util::BodyExt;

use crate::api_test_support::{
    ADMIN, ADMIN_PROOF, ALICE, ALICE_PROOF, PROVIDER, get, metadata, revoke_principal, send,
    settings_with_alice,
};
use crate::test_support::{api_request, mint_token, role_for, serve, tls_parts, tls_settings};

/// Settings whose roles let [`ADMIN`] administer everything and [`ALICE`] read the state, with the
/// given dashboard.
fn state_reader_settings(dashboard: DashboardConfig) -> ServerSettings {
    ServerSettings {
        dashboard,
        roles: vec![
            role_for("administrator", PermissionSet::all(), ADMIN),
            role_for(
                "state-reader",
                PermissionSet::EMPTY.with(Permission::StateRead),
                ALICE,
            ),
        ],
        ..tls_settings()
    }
}

/// The dashboard settings of the stream tests: a heartbeat that never falls due in a test.
fn stream_config(max_viewers: NonZeroUsize) -> DashboardConfig {
    DashboardConfig {
        max_viewers,
        heartbeat: NonZeroDuration::from_hours(1),
        ..DashboardConfig::default()
    }
}

/// Opens the state stream for the holder of `token`. The response is the stream: its body is read
/// frame by frame.
async fn open_stream(
    controller: &Controller,
    token: &AccessToken,
) -> http::Response<ApiResponseBody> {
    let request = api_request("GET", WATCH_STATE.template, Some(token), None);
    controller.call_api(request).await
}

/// The next frame the stream writes.
async fn next_frame(body: &mut ApiResponseBody) -> Frame {
    let chunk = body
        .frame()
        .await
        .expect("the stream writes a frame")
        .expect("the frame reads");
    let data = chunk.into_data().expect("stream frames carry data");
    let mut frames = Decoder::new()
        .push(&data)
        .expect("the stream writes well-formed frames");
    assert_eq!(frames.len(), 1, "each chunk is one frame");
    frames.remove(0)
}

/// The reason of a `closed` frame, as it is written on the wire.
fn closed_reason(frame: &Frame) -> &str {
    let Frame::Closed(closed) = frame else {
        panic!("the stream closes");
    };
    closed.reason.as_str()
}

#[tokio::test]
async fn state_answers_the_hub_state_to_a_state_reader() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        state_reader_settings(DashboardConfig::default()),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let request = api_request("GET", GET_STATE.template, Some(&admin), None);

            let response = controller.call_api(request).await;

            assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
            let (parts, body) = response.into_parts();
            assert_eq!(parts.status, StatusCode::OK);
            let bytes = body.collect().await.expect("the body reads").to_bytes();
            let state: State = serde_json::from_slice(&bytes).expect("the body is a state");
            assert_eq!(state.name, "state");
            assert_eq!(state.seq, 0);
            assert_eq!(state.provisioner, "test");
            let value: serde_json::Value =
                serde_json::from_slice(&bytes).expect("the body is JSON");
            assert!(value.get("schema").is_none(), "the state carries no schema");
        },
    )
    .await;
}

#[tokio::test]
async fn state_refuses_a_caller_without_state_read() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("events-reader", Permission::EventsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            for template in [GET_STATE.template, WATCH_STATE.template] {
                let error = get(&controller, template, Some(&alice))
                    .await
                    .refused(ErrorReason::MissingPermission);
                assert_eq!(metadata(&error, "permission"), "state.read");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn state_and_its_stream_refuse_an_anonymous_request() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        state_reader_settings(DashboardConfig::default()),
        handles.scheduler,
        |controller| async move {
            for template in [GET_STATE.template, WATCH_STATE.template] {
                get(&controller, template, None)
                    .await
                    .refused(ErrorReason::CredentialsMissing);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn state_and_its_stream_are_missing_routes_when_the_dashboard_is_not_mounted() {
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
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for template in [GET_STATE.template, WATCH_STATE.template] {
                let error = get(&controller, template, Some(&admin))
                    .await
                    .refused(ErrorReason::RouteNotFound);
                assert_eq!(metadata(&error, "path"), template);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn the_stream_opens_with_the_snapshot_then_turns_away_a_viewer_beyond_the_cap() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        state_reader_settings(stream_config(NonZeroUsize::MIN)),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let mut first = open_stream(&controller, &alice).await;
            assert_eq!(first.status(), StatusCode::OK);
            assert_eq!(first.headers()[CONTENT_TYPE], "text/event-stream");
            let Frame::Snapshot(state) = next_frame(first.body_mut()).await else {
                panic!("the stream opens with the snapshot");
            };
            assert_eq!(state.name, "state");

            let mut second = open_stream(&controller, &alice).await;
            let closed = next_frame(second.body_mut()).await;
            assert_eq!(closed_reason(&closed), "VIEWER_CAP");
            let Frame::Closed(closed) = closed else {
                panic!("the viewer is turned away");
            };
            assert!(
                closed.retry_delay.is_some(),
                "the viewer is told when to retry"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn the_stream_closes_when_the_session_expires() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        state_reader_settings(stream_config(NonZeroUsize::MIN)),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            // The token is accepted until ten minutes and thirty seconds after it was minted: the
            // stream opens three seconds before that instant and must close once it passes.
            clock.advance(Duration::from_mins(10) + Duration::from_secs(27));
            let mut stream = open_stream(&controller, &alice).await;
            let snapshot = next_frame(stream.body_mut()).await;
            assert!(matches!(snapshot, Frame::Snapshot(_)));

            clock.advance(Duration::from_secs(4));
            let closed = next_frame(stream.body_mut()).await;
            assert_eq!(closed_reason(&closed), "EXPIRED");
        },
    )
    .await;
}

#[tokio::test]
async fn revoking_a_viewer_closes_its_stream() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        state_reader_settings(stream_config(NonZeroUsize::MIN)),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let mut stream = open_stream(&controller, &alice).await;
            let snapshot = next_frame(stream.body_mut()).await;
            assert!(matches!(snapshot, Frame::Snapshot(_)));

            let answer = revoke_principal(&controller, &admin, ALICE).await;
            assert_eq!(answer.status, StatusCode::OK);

            let closed = next_frame(stream.body_mut()).await;
            assert_eq!(closed_reason(&closed), "REVOKED");
        },
    )
    .await;
}

#[tokio::test]
async fn the_stream_is_refused_to_a_revoked_viewer_with_the_error_envelope() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        state_reader_settings(stream_config(NonZeroUsize::MIN)),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            revoke_principal(&controller, &admin, ALICE).await;

            let request = api_request("GET", WATCH_STATE.template, Some(&alice), None);

            send(&controller, request)
                .await
                .refused(ErrorReason::PrincipalRevoked);
        },
    )
    .await;
}
