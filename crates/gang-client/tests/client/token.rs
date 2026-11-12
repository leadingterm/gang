//! Bearer token minting: the cached token inside the refresh margin, the re-mint once the margin
//! is reached, the stored identity-provider proof of an interactive session, and the workload
//! session's re-read of its proof source.

use std::path::Path;
use std::time::Duration;

use gang_client::{
    ClientError, ClientOptions, InteractiveLogin, LoggedIn, ProofSource, WorkloadLogin,
};
use gang_clock::Timestamp;
use tempfile::TempDir;

use super::support::*;

/// The proof the fake identity providers mint for grant `sequence`, expiring at `expires_at`.
///
/// Interactive login mints its proof before a test can read it, and the fake server accepts a
/// proof only by its exact text, so the text is rebuilt here in the providers' format.
fn minted_proof(sequence: usize, expires_at: Timestamp) -> String {
    let header = base64url(br#"{"alg":"ES256"}"#);
    let payload =
        base64url(format!(r#"{{"exp":{}}}"#, expires_at.as_unix_millis() / 1000).as_bytes());
    let signature = base64url(format!("sig-{sequence}").as_bytes());
    format!("{header}.{payload}.{signature}")
}

fn write_proof(path: &Path, proof: &str) {
    std::fs::write(path, proof).expect("proof file is writable");
}

async fn login_workload(harness: &Harness, source: &Path) -> Result<LoggedIn, ClientError> {
    let server = harness.server.url();
    let name = provider("google");
    harness
        .client()
        .login_workload(WorkloadLogin {
            server: &server,
            provider: &name,
            source: ProofSource::File {
                path: source.to_path_buf(),
            },
            ca_pin: None,
        })
        .await
}

async fn login_interactive(harness: &Harness) -> Result<LoggedIn, ClientError> {
    let server = harness.server.url();
    harness
        .client()
        .login(&InteractiveLogin {
            server: &server,
            provider: None,
            ca_pin: None,
        })
        .await
}

#[tokio::test]
async fn cached_token_is_reused_inside_the_refresh_margin() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    write_proof(&source, "workload-proof");
    let _logged_in = login_workload(&harness, &source)
        .await
        .expect("workload login");

    let first = harness.client().token().await.expect("first token");
    let second = harness.client().token().await.expect("second token");

    assert_eq!(first.value.expose(), "gang-token-1");
    assert!(second.value.matches(first.value.expose()));
    assert_eq!(second.expires_at, first.expires_at);
    assert_eq!(harness.server.token_requests().len(), 1);
}

#[tokio::test]
async fn token_is_reminted_and_stored_once_the_refresh_margin_is_reached() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    write_proof(&source, "workload-proof");
    let _logged_in = login_workload(&harness, &source)
        .await
        .expect("workload login");

    harness
        .clock
        .advance(Duration::from_mins(9) + Duration::from_secs(30));
    let minted = harness.client().token().await.expect("token is re-minted");

    assert_eq!(minted.value.expose(), "gang-token-2");
    assert_eq!(
        minted.expires_at,
        START + Duration::from_mins(19) + Duration::from_secs(30)
    );
    assert_eq!(harness.server.token_requests().len(), 2);
    let stored =
        std::fs::read_to_string(harness.home.token_file()).expect("token file is readable");
    assert!(stored.contains("gang-token-2"));
}

#[tokio::test]
async fn short_lived_token_is_refreshed_at_four_minutes() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.set_token_ttl(Duration::from_mins(5));
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    write_proof(&source, "workload-proof");
    let _logged_in = login_workload(&harness, &source)
        .await
        .expect("workload login");

    harness
        .clock
        .advance(Duration::from_mins(3) + Duration::from_secs(59));
    let reused = harness.client().token().await.expect("token is reused");
    assert_eq!(reused.value.expose(), "gang-token-1");
    assert_eq!(harness.server.token_requests().len(), 1);

    harness.clock.advance(Duration::from_secs(2));
    let renewed = harness.client().token().await.expect("token is re-minted");
    assert_eq!(renewed.value.expose(), "gang-token-2");
    assert_eq!(harness.server.token_requests().len(), 2);
}

#[tokio::test]
async fn refresh_margin_is_capped_at_a_fifth_of_the_token_lifetime() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.set_token_ttl(Duration::from_mins(5));
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    write_proof(&source, "workload-proof");
    let _logged_in = login_workload(&harness, &source)
        .await
        .expect("workload login");
    let client = harness.client_with(ClientOptions {
        token_refresh_margin: Duration::from_mins(2),
        ..ClientOptions::default()
    });

    harness
        .clock
        .advance(Duration::from_mins(3) + Duration::from_secs(59));
    let reused = client.token().await.expect("token is reused");
    assert_eq!(reused.value.expose(), "gang-token-1");

    harness.clock.advance(Duration::from_secs(2));
    let renewed = client.token().await.expect("token is re-minted");
    assert_eq!(renewed.value.expose(), "gang-token-2");
}

#[tokio::test]
async fn workload_session_rereads_its_proof_on_every_mint() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof-1");
    harness.server.accept_proof("google", "workload-proof-2");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    write_proof(&source, "workload-proof-1");
    let _logged_in = login_workload(&harness, &source)
        .await
        .expect("workload login");

    write_proof(&source, "workload-proof-2");
    harness
        .clock
        .advance(Duration::from_mins(9) + Duration::from_secs(30));
    harness.client().token().await.expect("token is re-minted");

    let requests = harness.server.token_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1, "workload-proof-1");
    assert_eq!(requests[1].1, "workload-proof-2");
}

#[tokio::test]
async fn token_without_a_session_is_not_logged_in() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;

    let error = harness
        .client()
        .token()
        .await
        .expect_err("no session is stored");

    assert!(matches!(error, ClientError::NotLoggedIn));
}

#[tokio::test]
async fn fresh_stored_id_token_is_reused_for_the_mint() {
    let harness = Harness::start_plain(ConsoleMode::Browser, google()).await;
    let proof = minted_proof(1, START + Duration::from_hours(1));
    harness.server.accept_proof("google", &proof);
    let _logged_in = login_interactive(&harness)
        .await
        .expect("interactive login");

    harness
        .clock
        .advance(Duration::from_mins(9) + Duration::from_secs(30));
    let minted = harness.client().token().await.expect("token is re-minted");

    assert_eq!(minted.value.expose(), "gang-token-2");
    let requests = harness.server.token_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].1, proof);
    assert_eq!(harness.providers.renew_calls(), 0);
}

#[tokio::test]
async fn expired_id_token_is_renewed_through_the_refresh_token() {
    let harness = Harness::start_plain(ConsoleMode::Browser, google()).await;
    harness
        .server
        .accept_proof("google", &minted_proof(1, START + Duration::from_hours(1)));
    let _logged_in = login_interactive(&harness)
        .await
        .expect("interactive login");

    let renewed_at = START + Duration::from_hours(1) + Duration::from_secs(1);
    let renewed_proof = minted_proof(2, renewed_at + Duration::from_hours(1));
    harness.server.accept_proof("google", &renewed_proof);
    harness
        .clock
        .advance(Duration::from_hours(1) + Duration::from_secs(1));
    let minted = harness.client().token().await.expect("token is re-minted");

    assert_eq!(minted.value.expose(), "gang-token-2");
    assert_eq!(harness.providers.renew_calls(), 1);
    let refresh = std::fs::read_to_string(harness.home.refresh_token_file())
        .expect("refresh token file is readable");
    assert_eq!(refresh, "refresh-2", "the rotated refresh token is stored");
    let requests = harness.server.token_requests();
    assert_eq!(
        requests.last().map(|request| request.1.as_str()),
        Some(renewed_proof.as_str())
    );
}

#[tokio::test]
async fn revoked_refresh_token_is_reported_as_renewal_rejected() {
    let harness = Harness::start_plain(ConsoleMode::Browser, google()).await;
    harness
        .server
        .accept_proof("google", &minted_proof(1, START + Duration::from_hours(1)));
    let _logged_in = login_interactive(&harness)
        .await
        .expect("interactive login");
    harness.providers.revoke_refresh_tokens();

    harness
        .clock
        .advance(Duration::from_hours(1) + Duration::from_secs(1));
    let error = harness
        .client()
        .token()
        .await
        .expect_err("a revoked refresh token cannot renew");

    assert!(matches!(error, ClientError::RenewalRejected(_)));
}

#[tokio::test]
async fn stored_id_token_rejected_by_the_server_is_renewed_once_and_retried() {
    let harness = Harness::start_plain(ConsoleMode::Browser, google()).await;
    harness
        .server
        .accept_proof("google", &minted_proof(1, START + Duration::from_hours(1)));
    let _logged_in = login_interactive(&harness)
        .await
        .expect("interactive login");

    let rejected = minted_proof(99, START + Duration::from_hours(1));
    std::fs::write(harness.home.id_token_file(), &rejected).expect("id token file is writable");
    let renewed_at = START + Duration::from_mins(9) + Duration::from_secs(30);
    let renewed_proof = minted_proof(2, renewed_at + Duration::from_hours(1));
    harness.server.accept_proof("google", &renewed_proof);
    harness
        .clock
        .advance(Duration::from_mins(9) + Duration::from_secs(30));

    let minted = harness.client().token().await.expect("token is re-minted");

    assert_eq!(minted.value.expose(), "gang-token-2");
    assert_eq!(harness.providers.renew_calls(), 1);
    let requests = harness.server.token_requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].1, rejected);
    assert_eq!(requests[2].1, renewed_proof);
}

#[tokio::test]
async fn renewed_token_rejected_again_fails_without_another_retry() {
    let harness = Harness::start_plain(ConsoleMode::Browser, google()).await;
    harness
        .server
        .accept_proof("google", &minted_proof(1, START + Duration::from_hours(1)));
    let _logged_in = login_interactive(&harness)
        .await
        .expect("interactive login");

    std::fs::write(
        harness.home.id_token_file(),
        minted_proof(99, START + Duration::from_hours(1)),
    )
    .expect("id token file is writable");
    harness
        .clock
        .advance(Duration::from_mins(9) + Duration::from_secs(30));

    let error = harness
        .client()
        .token()
        .await
        .expect_err("the server rejects the renewed proof too");

    assert!(matches!(error, ClientError::Server { status: 401, .. }));
    assert_eq!(harness.providers.renew_calls(), 1);
    assert_eq!(harness.server.token_requests().len(), 3);
}
