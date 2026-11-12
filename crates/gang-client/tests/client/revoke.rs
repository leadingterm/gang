//! `Client::revoke_certificate`, `Client::revoke_principal` and `Client::restore_principal`: the
//! bearer token the client minted authenticates the request, and the server's answer is returned
//! unchanged.

use std::sync::Arc;
use std::time::Duration;

use gang_client::{Client, ClientError, ProofSource, WorkloadLogin};
use gang_clock::TestClock;
use gang_types::{CertSerial, Identity, Principal, ProviderName};
use gang_wire::routes::REVOKE_CERTIFICATE;
use serde_json::json;
use tempfile::TempDir;

use super::support::*;

const CI_PROOF: &str = "ci-proof";

fn provider() -> ProviderName {
    ProviderName::try_new("corp-ci").expect("provider name is valid in test")
}

fn identity() -> Identity {
    Identity::new(
        provider(),
        Principal::try_new("worker-a").expect("principal is valid in test"),
    )
}

fn serial() -> CertSerial {
    CertSerial::try_new("0c0c").expect("serial is valid in test")
}

fn certificate_path() -> String {
    REVOKE_CERTIFICATE.path(&serial())
}

/// Starts a server that accepts the workload proof, and logs a workload session in through the
/// client it returns. No browser is involved. The proof file is read again for every token that
/// is minted, so the returned directory must outlive the test's use of the client.
async fn logged_in() -> (
    Harness,
    Client<FakeProviders, FakeConsole, Arc<TestClock>>,
    TempDir,
) {
    let harness = Harness::start_plain(
        ConsoleMode::Silent,
        token_info(&[("corp-ci", json!({}))], false),
    )
    .await;
    harness.server.accept_proof("corp-ci", CI_PROOF);
    let dir = TempDir::new().expect("creating a temporary proof directory");
    let proof = dir.path().join("proof");
    std::fs::write(&proof, CI_PROOF).expect("writing the proof file");
    let client = harness.client();
    let url = harness.server.url();
    let name = provider();
    let _logged_in = client
        .login_workload(WorkloadLogin {
            server: &url,
            provider: &name,
            source: ProofSource::File { path: proof },
            ca_pin: None,
        })
        .await
        .expect("workload login succeeds");
    (harness, client, dir)
}

#[tokio::test]
async fn certificate_revocation_reaches_the_server_and_returns_its_answer() {
    let (harness, client, _proof) = logged_in().await;

    let response = client
        .revoke_certificate(&serial())
        .await
        .expect("the revocation succeeds");

    assert_eq!(harness.server.revoked_certificates(), vec![serial()]);
    assert_eq!(response.name, "certificates/0c0c");
    assert_eq!(response.revoke_time, Some(START));
}

#[tokio::test]
async fn principal_revocation_reaches_the_server_and_returns_its_answer() {
    let (harness, client, _proof) = logged_in().await;

    let response = client
        .revoke_principal(&identity())
        .await
        .expect("the revocation succeeds");

    assert_eq!(harness.server.revoked_principals(), vec![identity()]);
    assert_eq!(response.principal.name, "principals/corp-ci%3Aworker-a");
    assert_eq!(response.principal.provider, provider());
    assert_eq!(response.principal.revoke_time, Some(START));
    assert_eq!(
        response
            .certificates
            .iter()
            .map(|certificate| certificate.name.as_str())
            .collect::<Vec<_>>(),
        ["certificates/0a0a", "certificates/0b0b"]
    );
}

#[tokio::test]
async fn revoke_presents_the_bearer_token_the_client_minted() {
    let (harness, client, _proof) = logged_in().await;
    let token = client.token().await.expect("the session yields a token");

    client
        .revoke_certificate(&serial())
        .await
        .expect("the revocation succeeds");

    assert_eq!(
        harness.server.calls().last(),
        Some(&(certificate_path(), Some(token.value.expose().to_owned())))
    );
}

#[tokio::test]
async fn principal_identity_is_percent_encoded_in_the_path() {
    let (harness, client, _proof) = logged_in().await;

    client
        .revoke_principal(&identity())
        .await
        .expect("the revocation succeeds");
    client
        .restore_principal(&identity())
        .await
        .expect("the restore succeeds");

    let paths: Vec<String> = harness
        .server
        .calls()
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        paths,
        [
            "/v1/principals/corp-ci%3Aworker-a:revoke",
            "/v1/principals/corp-ci%3Aworker-a:restore"
        ]
    );
}

#[tokio::test]
async fn revoke_without_a_session_is_not_logged_in() {
    let harness = Harness::start_plain(
        ConsoleMode::Silent,
        token_info(&[("corp-ci", json!({}))], false),
    )
    .await;

    let error = harness
        .client()
        .revoke_certificate(&serial())
        .await
        .expect_err("there is no session to revoke with");

    assert!(matches!(error, ClientError::NotLoggedIn));
}

#[tokio::test]
async fn server_unauthorized_answer_is_a_server_error_with_status_401() {
    let (harness, client, _proof) = logged_in().await;
    harness
        .server
        .force_response(401, b"the bearer token was refused".to_vec());

    let error = client
        .revoke_certificate(&serial())
        .await
        .expect_err("the server refuses the token");

    let ClientError::Server { status, .. } = error else {
        panic!("expected a server error, got {error}");
    };
    assert_eq!(status, 401);
}

#[tokio::test]
async fn expired_token_is_reminted_before_the_revocation() {
    let (harness, client, _proof) = logged_in().await;
    let previous = client.token().await.expect("the session yields a token");
    harness.clock.advance(Duration::from_hours(1));

    client
        .revoke_certificate(&serial())
        .await
        .expect("the revocation succeeds with a fresh token");

    let current = client.token().await.expect("the session yields a token");
    assert!(!current.value.matches(previous.value.expose()));
    assert_eq!(
        harness.server.calls().last(),
        Some(&(certificate_path(), Some(current.value.expose().to_owned())))
    );
    assert_eq!(harness.server.token_requests().len(), 2);
}

#[tokio::test]
async fn restore_sends_the_request_and_returns_the_answer() {
    let (harness, client, _proof) = logged_in().await;

    let response = client
        .restore_principal(&identity())
        .await
        .expect("the restore succeeds");

    assert_eq!(harness.server.restored_principals(), vec![identity()]);
    assert_eq!(response.name, "principals/corp-ci%3Aworker-a");
    assert_eq!(response.principal, identity().principal);
    assert_eq!(response.revoke_time, None);
}

#[tokio::test]
async fn restore_without_a_session_is_not_logged_in() {
    let harness = Harness::start_plain(
        ConsoleMode::Silent,
        token_info(&[("corp-ci", json!({}))], false),
    )
    .await;

    let error = harness
        .client()
        .restore_principal(&identity())
        .await
        .expect_err("there is no session to restore with");

    assert!(matches!(error, ClientError::NotLoggedIn));
}
