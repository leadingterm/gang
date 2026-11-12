//! Workload login: a proof from the workload's own identity provider, stored without a refresh
//! token.

use std::path::PathBuf;

use gang_client::{ClientError, LoggedIn, ProofSource, WorkloadLogin};
use gang_types::ProviderName;
use tempfile::TempDir;

use super::support::*;

/// Writes `text` to a proof file in a fresh directory and returns the file's path.
fn proof_file(dir: &TempDir, text: &str) -> PathBuf {
    let path = dir.path().join("proof");
    std::fs::write(&path, text).expect("writing the proof file");
    path
}

/// Logs in with `source` against the harness's TLS server, pinned to its CA.
async fn login_workload(
    harness: &Harness,
    provider_name: &ProviderName,
    source: ProofSource,
) -> Result<LoggedIn, ClientError> {
    let server = harness.server.url();
    let pin = ca_pin();
    harness
        .client()
        .login_workload(WorkloadLogin {
            server: &server,
            provider: provider_name,
            source,
            ca_pin: Some(&pin),
        })
        .await
}

/// Asserts that a failed workload login stored no session and no identity or bearer file.
fn assert_nothing_stored(harness: &Harness) {
    let home = &harness.home;
    for path in [
        home.session_file(),
        home.refresh_token_file(),
        home.id_token_file(),
        home.token_file(),
    ] {
        assert!(!path.exists(), "{} is not stored", path.display());
    }
}

#[tokio::test]
async fn accepted_proof_stores_a_workload_session() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "workload-proof"),
    };

    let _logged_in = login_workload(&harness, &provider("google"), source)
        .await
        .expect("the accepted proof logs in");

    let bytes = std::fs::read(harness.home.session_file()).expect("reading the session");
    let session: serde_json::Value =
        serde_json::from_slice(&bytes).expect("the session file parses");
    assert_eq!(session["kind"]["kind"], "workload");
    assert_eq!(session["provider"], "google");
}

#[tokio::test]
async fn accepted_proof_stores_no_refresh_token_and_no_id_token() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "workload-proof"),
    };

    let _logged_in = login_workload(&harness, &provider("google"), source)
        .await
        .expect("the accepted proof logs in");

    assert!(!harness.home.refresh_token_file().exists());
    assert!(!harness.home.id_token_file().exists());
}

#[tokio::test]
async fn accepted_proof_stores_the_minted_token() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "workload-proof"),
    };

    let _logged_in = login_workload(&harness, &provider("google"), source)
        .await
        .expect("the accepted proof logs in");

    assert!(harness.home.token_file().exists());
}

#[tokio::test]
async fn login_reports_the_provider_it_logged_in_with() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "workload-proof"),
    };

    let logged = login_workload(&harness, &provider("google"), source)
        .await
        .expect("the accepted proof logs in");

    assert_eq!(logged.provider, provider("google"));
}

#[tokio::test]
async fn proof_file_text_is_trimmed_before_the_token_request() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "  workload-proof\n"),
    };

    let _logged_in = login_workload(&harness, &provider("google"), source)
        .await
        .expect("the trimmed proof is accepted");

    assert_eq!(
        harness.server.token_requests(),
        vec![(provider("google"), "workload-proof".to_owned())]
    );
}

#[tokio::test]
async fn rejected_proof_surfaces_status_401_and_stores_nothing() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "unregistered-proof"),
    };

    let error = login_workload(&harness, &provider("google"), source)
        .await
        .expect_err("the server rejects an unregistered proof");

    assert!(
        matches!(error, ClientError::Server { status: 401, .. }),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn empty_proof_file_is_an_empty_token() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "   \n"),
    };

    let error = login_workload(&harness, &provider("google"), source)
        .await
        .expect_err("a blank proof is refused");

    assert!(
        matches!(error, ClientError::EmptyToken),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn missing_proof_file_is_an_io_error() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: dir.path().join("absent-proof"),
    };

    let error = login_workload(&harness, &provider("google"), source)
        .await
        .expect_err("the proof file does not exist");

    assert!(
        matches!(&error, ClientError::Io(failed) if failed.action == "read"),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn server_without_authentication_has_no_workload_login() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    let server = harness.server.url();
    let dir = TempDir::new().expect("creating the proof directory");
    let source = ProofSource::File {
        path: proof_file(&dir, "workload-proof"),
    };

    let error = harness
        .client()
        .login_workload(WorkloadLogin {
            server: &server,
            provider: &provider("google"),
            source,
            ca_pin: None,
        })
        .await
        .expect_err("a server without authentication has no login");

    assert!(
        matches!(error, ClientError::NoAuthentication),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn command_that_cannot_start_is_a_token_command_error() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let source = ProofSource::Command {
        argv: vec!["gang-test-no-such-proof-command".to_owned()],
    };

    let error = login_workload(&harness, &provider("google"), source)
        .await
        .expect_err("the proof command does not exist");

    assert!(
        matches!(error, ClientError::TokenCommand { .. }),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}
