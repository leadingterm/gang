//! The credential helper's reply: the Bazel JSON for the stored session's token, the host check
//! against the session's server, and the request validation.

use std::future::{Future, poll_fn};
use std::path::Path;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use gang_client::{ClientError, ProofSource, WorkloadLogin};
use gang_os::FileLock;
use tempfile::TempDir;

use super::support::*;

async fn login_workload(harness: &Harness, source: &Path) {
    let server = harness.server.url();
    let name = provider("google");
    let _logged_in = harness
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
        .expect("workload login");
}

/// A harness logged in through a workload proof, whose tokens last ten minutes.
async fn logged_in_harness() -> Harness {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.set_token_ttl(Duration::from_mins(10));
    harness.server.accept_proof("google", "workload-proof");
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let source = dir.path().join("proof");
    std::fs::write(&source, "workload-proof").expect("writing the proof file");
    login_workload(&harness, &source).await;
    harness
}

fn request_for(harness: &Harness, path: &str) -> String {
    let uri = harness
        .server
        .url()
        .join(path)
        .expect("request path joins onto the server url");
    serde_json::json!({ "uri": uri }).to_string()
}

/// Parses a credential reply, so that tests compare its values rather than its key order.
fn parsed(reply: &str) -> serde_json::Value {
    serde_json::from_str(reply).expect("the reply is JSON")
}

#[tokio::test]
async fn reply_carries_the_bearer_token_and_its_expiry() {
    let harness = logged_in_harness().await;
    let request = request_for(&harness, "/cache/blob");

    let reply = harness
        .client()
        .get(&request)
        .await
        .expect("the credential reply");

    assert_eq!(
        parsed(&reply),
        serde_json::json!({
            "headers": {"authorization": ["Bearer gang-token-1"]},
            "expires": "2024-01-01T00:10:00Z",
        })
    );
}

#[tokio::test]
async fn unchanged_token_gives_the_same_reply_without_a_new_mint() {
    let harness = logged_in_harness().await;
    let request = request_for(&harness, "/cache/blob");

    let first = harness
        .client()
        .get(&request)
        .await
        .expect("first credential reply");
    let second = harness
        .client()
        .get(&request)
        .await
        .expect("second credential reply");

    assert_eq!(parsed(&first), parsed(&second));
    assert_eq!(harness.server.token_requests().len(), 1);
}

#[tokio::test]
async fn request_for_another_host_is_a_host_mismatch() {
    let harness = logged_in_harness().await;
    let request = r#"{"uri":"https://other.example/cache/blob"}"#;

    let error = harness
        .client()
        .get(request)
        .await
        .expect_err("the host differs from the session's server");

    assert!(matches!(error, ClientError::HostMismatch { .. }));
    assert_eq!(harness.server.token_requests().len(), 1);
}

#[tokio::test]
async fn get_checks_the_host_against_the_session_it_mints_for() {
    let first = logged_in_harness().await;
    let second = logged_in_harness().await;
    let request = request_for(&first, "/cache/blob");
    let client = first.client();
    let lock = FileLock::exclusive_blocking(&first.home.lock_file()).expect("lock is taken");
    let mut get = pin!(client.get(&request));
    let polled = poll_fn(|cx| Poll::Ready(get.as_mut().poll(cx))).await;
    assert!(polled.is_pending(), "get must wait for the client lock");

    let mut session: serde_json::Value = serde_json::from_slice(
        &std::fs::read(second.home.session_file()).expect("read second session"),
    )
    .expect("second session is JSON");
    let server = second
        .server
        .url()
        .as_str()
        .replace("127.0.0.1", "localhost");
    session["server"] = server.into();
    std::fs::write(
        first.home.session_file(),
        serde_json::to_vec(&session).expect("encode second session"),
    )
    .expect("store second session");
    std::fs::copy(second.home.token_file(), first.home.token_file())
        .expect("store second bearer token");
    drop(lock);

    assert!(matches!(
        get.await,
        Err(ClientError::HostMismatch { requested, server })
            if requested == "127.0.0.1" && server == "localhost"
    ));
}

#[tokio::test]
async fn malformed_request_json_is_a_credential_request_error() {
    let harness = logged_in_harness().await;

    let error = harness
        .client()
        .get("uri=https://cache.example.com/")
        .await
        .expect_err("the request is not JSON");

    assert!(matches!(error, ClientError::CredentialRequest(_)));
}

#[tokio::test]
async fn reply_without_a_session_is_not_logged_in() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let request = request_for(&harness, "/cache/blob");

    let error = harness
        .client()
        .get(&request)
        .await
        .expect_err("no session is stored");

    assert!(matches!(error, ClientError::NotLoggedIn));
}
