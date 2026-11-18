//! Workload login: a proof from the workload's own identity provider, stored without a refresh
//! token.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use gang_client::{
    Client, ClientError, ClientHome, ClientOptions, LoggedIn, ProofSource, WorkloadLogin,
};
use gang_clock::TestClock;
use gang_testkit::ChildTest;
use gang_types::{ProviderName, Url};
use tempfile::TempDir;

use super::support::*;

const PROOF_DIRECTORY_CHILD: ChildTest = ChildTest::new("client::workload::proof_directory_child");
const PROOF_DIRECTORY_ROLE: &str = "GANG_TEST_PROOF_DIRECTORY_ROLE";
const PROOF_DIRECTORY_HOME: &str = "GANG_TEST_PROOF_DIRECTORY_HOME";
const PROOF_DIRECTORY_SERVER: &str = "GANG_TEST_PROOF_DIRECTORY_SERVER";
const PROOF_DIRECTORY_MODE: &str = "GANG_TEST_PROOF_DIRECTORY_MODE";

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
async fn relative_proof_file_stays_bound_to_login_directory() {
    proof_directory_scenario("file").await;
}

#[tokio::test]
async fn relative_proof_command_stays_bound_to_login_directory() {
    proof_directory_scenario("relative-command").await;
}

#[tokio::test]
async fn bare_proof_command_keeps_path_lookup() {
    proof_directory_scenario("bare-command").await;
}

/// Exercises login and renewal from different directories for each proof source.
async fn proof_directory_scenario(mode: &str) {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.server.accept_proof("google", "proof-from-a");
    harness.server.accept_proof("google", "proof-from-b");
    let a = TempDir::new().expect("creating login directory");
    let b = TempDir::new().expect("creating renewal directory");
    let bin = match mode {
        "file" => {
            proof_file(&a, "proof-from-a");
            proof_file(&b, "proof-from-b");
            None
        }
        "relative-command" | "bare-command" => {
            let helper = std::env::current_dir()
                .expect("finding the test working directory")
                .join(std::env::var_os("FAKE_CREDENTIAL_HELPER").expect("helper is declared"))
                .canonicalize()
                .expect("resolving the declared helper");
            let bin = (mode == "bare-command")
                .then(|| TempDir::new().expect("creating command search directory"));
            let directory = bin.as_ref().map_or(a.path(), |dir| dir.path());
            std::os::unix::fs::symlink(helper, directory.join("get-proof"))
                .expect("linking the declared helper");
            bin
        }
        _ => panic!("unknown proof source mode: {mode}"),
    };

    tokio::time::timeout(Duration::from_secs(30), async {
        for (role, dir) in [("login", &a), ("renew", &b)] {
            if role == "renew" {
                harness.clock.advance(Duration::from_hours(1));
            }
            // Re-execute the declared test binary to isolate each working directory.
            let mut command = tokio::process::Command::from(
                PROOF_DIRECTORY_CHILD
                    .command()
                    .expect("locating the test binary"),
            );
            command
                .current_dir(dir.path())
                .env(PROOF_DIRECTORY_ROLE, role)
                .env(PROOF_DIRECTORY_MODE, mode)
                .env(PROOF_DIRECTORY_HOME, harness.home.dir())
                .env(PROOF_DIRECTORY_SERVER, harness.server.url().as_str())
                .kill_on_drop(true);
            if let Some(bin) = &bin {
                command.env("PATH", bin.path());
            }
            let output = command.output().await.expect("running the child test");
            PROOF_DIRECTORY_CHILD.assert_completed(&output);
        }
    })
    .await
    .expect("both child tests finish before the deadline");

    assert_eq!(
        harness.server.token_requests(),
        vec![
            (provider("google"), "proof-from-a".to_owned()),
            (provider("google"), "proof-from-a".to_owned()),
        ]
    );
}

#[tokio::test]
#[ignore = "child test: run by proof directory scenarios"]
async fn proof_directory_child() {
    let role = std::env::var(PROOF_DIRECTORY_ROLE).expect("child role is set");
    let mode = std::env::var(PROOF_DIRECTORY_MODE).expect("proof source mode is set");
    let home =
        ClientHome::try_new(std::env::var_os(PROOF_DIRECTORY_HOME).expect("client home is set"))
            .expect("client home is valid");
    let server = Url::parse(&std::env::var(PROOF_DIRECTORY_SERVER).expect("server URL is set"))
        .expect("server URL is valid");
    let now = match role.as_str() {
        "login" => START,
        "renew" => START + Duration::from_hours(1),
        _ => panic!("unknown child role: {role}"),
    };
    let clock = Arc::new(TestClock::new(now));
    let client = Client::new(
        home,
        ClientOptions::default(),
        FakeProviders::new(clock.clone()),
        FakeConsole::new(ConsoleMode::Silent),
        clock,
    );
    match role.as_str() {
        "login" => {
            let _logged_in = client
                .login_workload(WorkloadLogin {
                    server: &server,
                    provider: &provider("google"),
                    source: match mode.as_str() {
                        "file" => ProofSource::File {
                            path: "./proof".into(),
                        },
                        "relative-command" | "bare-command" => ProofSource::Command {
                            argv: vec![
                                if mode == "relative-command" {
                                    "./get-proof"
                                } else {
                                    "get-proof"
                                }
                                .to_owned(),
                                "proof".to_owned(),
                                "proof-from-a".to_owned(),
                            ],
                        },
                        _ => panic!("unknown proof source mode: {mode}"),
                    },
                    ca_pin: None,
                })
                .await
                .expect("workload login succeeds");
        }
        "renew" => {
            client.token().await.expect("workload token is renewed");
        }
        _ => unreachable!("role was checked above"),
    }
    PROOF_DIRECTORY_CHILD.complete();
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
