//! Session identity requests use the stored bearer and preserve anonymous responses.

use std::sync::Arc;
use std::time::Duration;

use gang_client::{
    ClientError, ClientOptions, ProofSource, ServerApi, SessionState, WorkloadLogin,
};
use gang_clock::TestClock;
use gang_tls::ServerTrust;
use gang_wire::{ErrorReason, ErrorResponse, Status};
use tempfile::TempDir;

use super::support::*;

/// Stores a workload login and reusable bearer token in the harness home.
async fn store_workload_login(harness: &Harness, dir: &TempDir) {
    harness.server.accept_proof("google", "workload-proof");
    let proof = dir.path().join("proof");
    std::fs::write(&proof, "workload-proof").expect("write proof");
    let _logged_in = harness
        .client()
        .login_workload(WorkloadLogin {
            server: &harness.server.url(),
            provider: &provider("google"),
            source: ProofSource::File { path: proof },
            ca_pin: None,
        })
        .await
        .expect("workload login");
}

/// A rejected cached bearer requires a new login rather than aborting setup.
#[tokio::test]
async fn session_state_requires_login_for_unauthorized_bearer() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("temporary proof directory");
    store_workload_login(&harness, &dir).await;
    harness
        .server
        .force_response(401, b"bearer refused".to_vec());
    let result = harness.client().session_state().await;
    assert!(
        matches!(result, Ok(SessionState::NeedsLogin)),
        "an unauthorized bearer must require a new login"
    );
    assert_eq!(
        harness.server.calls(),
        vec![("/v1/session".to_owned(), Some("gang-token-1".to_owned()))]
    );
}

/// A valid bearer preserves the server's identified principal.
#[tokio::test]
async fn session_state_returns_identified_principal() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("temporary proof directory");
    store_workload_login(&harness, &dir).await;
    let result = harness.client().session_state().await;
    assert_eq!(
        result.expect("session lookup"),
        SessionState::Active {
            principal: Some("google:user@example.org".to_owned())
        },
        "a usable bearer must retain its principal"
    );
}

/// A server-revoked principal cannot reuse its cached bearer.
#[tokio::test]
async fn session_state_requires_login_for_revoked_principal() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("temporary proof directory");
    store_workload_login(&harness, &dir).await;
    let reason = ErrorReason::PrincipalRevoked;
    let body = serde_json::to_vec(&ErrorResponse::from(Status::new(
        reason,
        reason.message(),
        [],
    )))
    .expect("serialize server error");
    harness.server.force_response(403, body);
    let result = harness.client().session_state().await;
    assert!(
        matches!(result, Ok(SessionState::NeedsLogin)),
        "a revoked principal must require a new login"
    );
    assert_eq!(
        harness.server.calls(),
        vec![("/v1/session".to_owned(), Some("gang-token-1".to_owned()))]
    );
}

/// An expired device bearer whose refresh is rejected needs a new login.
#[tokio::test]
async fn session_state_requires_login_for_rejected_device_refresh() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("temporary proof directory");
    store_workload_login(&harness, &dir).await;
    let path = harness.home.session_file();
    let mut session: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read session")).expect("session JSON");
    session["kind"] = serde_json::json!({"kind": "device"});
    std::fs::write(
        &path,
        serde_json::to_vec(&session).expect("serialize session"),
    )
    .expect("write device session");
    std::fs::write(harness.home.refresh_token_file(), "rejected-refresh")
        .expect("write refresh token");
    harness.clock.advance(Duration::from_mins(11));
    let result = harness.client().session_state().await;
    assert!(
        matches!(result, Ok(SessionState::NeedsLogin)),
        "a rejected device refresh must require a new login"
    );
    assert_eq!(
        harness.server.token_requests().last().unwrap().1,
        "rejected-refresh"
    );
}

/// Rejected proof exchanges need login, while unrelated server errors propagate.
#[tokio::test]
async fn session_state_handles_rejected_proof_exchange() {
    for status in [401, 403, 500] {
        let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
        let dir = TempDir::new().expect("temporary proof directory");
        store_workload_login(&harness, &dir).await;
        harness.clock.advance(Duration::from_mins(11));
        harness
            .server
            .force_response(status, b"exchange refused".to_vec());
        let result = harness.client().session_state().await;
        if status == 500 {
            assert!(matches!(
                result,
                Err(ClientError::Server { status: 500, .. })
            ));
        } else {
            assert!(
                matches!(result, Ok(SessionState::NeedsLogin)),
                "a rejected proof exchange must require a new login"
            );
        }
    }
}

/// Logs in through the fake workload provider and checks the session identity and bearer.
#[tokio::test]
async fn whoami_presents_the_stored_bearer() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("temporary proof directory");
    let proof = dir.path().join("proof");
    std::fs::write(&proof, "workload-proof").expect("write proof");
    let server = harness.server.url();
    let provider = provider("google");
    let client = harness.client();
    let logged = client
        .login_workload(WorkloadLogin {
            server: &server,
            provider: &provider,
            source: ProofSource::File { path: proof },
            ca_pin: None,
        })
        .await
        .expect("workload login");
    assert_eq!(logged.provider, provider);

    assert_eq!(
        client.whoami().await.expect("session"),
        Some("google:user@example.org".to_owned())
    );
    assert_eq!(
        harness.server.calls(),
        vec![("/v1/session".to_owned(), Some("gang-token-1".to_owned()))]
    );
    harness.server.force_response(
        200,
        br#"{"name":"session","principal":"principals/bad%ZZ","permissions":[]}"#.to_vec(),
    );
    assert!(matches!(client.whoami().await, Err(ClientError::Body(_))));
}

/// An anonymous session is valid and has no principal.
#[tokio::test]
async fn anonymous_session_has_no_principal() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
        Arc::new(TestClock::new(START)),
    )
    .expect("server api");
    assert_eq!(
        api.session(None)
            .await
            .expect("anonymous session")
            .principal,
        None
    );
}

/// Without a stored login, identity lookup reports the token error.
#[tokio::test]
async fn whoami_requires_a_stored_login() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    assert!(matches!(
        harness.client().whoami().await,
        Err(ClientError::NotLoggedIn)
    ));
    assert!(harness.server.calls().is_empty());
}
