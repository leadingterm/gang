//! The CA pin a session records is enforced on every request made with that session: minting a
//! bearer token, and the administrative calls that present one.

use std::time::Duration;

use gang_client::{ClientError, InteractiveLogin};
use gang_clock::Timestamp;
use gang_types::{CaPin, CertSerial};

use super::support::*;

/// A pin no certificate of the fixture CA matches: the pin of another authority.
fn other_ca_pin() -> CaPin {
    CaPin::new([7; 32])
}

/// The proof the fake providers mint for grant `sequence` when the clock reads `minted_at`.
fn proof(sequence: usize, minted_at: Timestamp) -> String {
    let expires_at = (minted_at + Duration::from_hours(1)).as_unix_millis() / 1_000;
    format!(
        "{}.{}.{}",
        base64url(br#"{"alg":"ES256"}"#),
        base64url(format!(r#"{{"exp":{expires_at}}}"#).as_bytes()),
        base64url(format!("sig-{sequence}").as_bytes()),
    )
}

/// Logs in through the browser over TLS, pinned to the fixture CA, and returns the harness.
async fn logged_in_over_tls() -> Harness {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));
    let server = harness.server.url();
    let pin = ca_pin();
    let _logged_in = harness
        .client()
        .login(&InteractiveLogin {
            server: &server,
            provider: None,
            ca_pin: Some(&pin),
            browser: gang_client::LocalBrowser::Available,
        })
        .await
        .expect("the TLS login succeeds");
    harness
}

/// Rewrites the stored session so that it records `pin`, as a session of another CA would.
fn repin_session(harness: &Harness, pin: &CaPin) {
    let path = harness.home.session_file();
    let bytes = std::fs::read(&path).expect("reading the session");
    let mut session: serde_json::Value =
        serde_json::from_slice(&bytes).expect("the session file parses");
    session["ca_pin"] = serde_json::to_value(pin).expect("the ca pin encodes");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&session).expect("the session encodes"),
    )
    .expect("writing the session");
}

/// Asserts that the only request the server received is the login's, and no bearer-authenticated
/// request reached it: the wrong pin refused the handshake before anything was sent.
fn assert_no_request_after_login(harness: &Harness) {
    assert_eq!(harness.server.token_requests().len(), 1);
    assert!(harness.server.calls().is_empty());
}

#[tokio::test]
async fn token_mint_is_refused_when_the_session_pin_is_another_cas() {
    let harness = logged_in_over_tls().await;
    repin_session(&harness, &other_ca_pin());
    // The stored token expires ten minutes after the login, so the client has to mint again.
    harness.clock.advance(Duration::from_mins(10));

    let error = harness
        .client()
        .token()
        .await
        .expect_err("the other CA's pin refuses the server");

    assert!(
        matches!(error, ClientError::Transport { .. }),
        "unexpected error: {error}"
    );
    assert_no_request_after_login(&harness);
}

#[tokio::test]
async fn principal_revocation_is_refused_when_the_session_pin_is_another_cas() {
    let harness = logged_in_over_tls().await;
    repin_session(&harness, &other_ca_pin());

    // The stored token is still fresh, so only the revocation request itself reaches the server.
    let error = harness
        .client()
        .revoke_principal(&certificate_holder())
        .await
        .expect_err("the other CA's pin refuses the server");

    assert!(
        matches!(error, ClientError::Transport { .. }),
        "unexpected error: {error}"
    );
    assert_no_request_after_login(&harness);
    assert!(harness.server.revoked_principals().is_empty());
}

#[tokio::test]
async fn certificate_revocation_is_refused_when_the_session_pin_is_another_cas() {
    let harness = logged_in_over_tls().await;
    repin_session(&harness, &other_ca_pin());
    let serial = CertSerial::try_new("0c0c").expect("serial is valid in test");

    let error = harness
        .client()
        .revoke_certificate(&serial)
        .await
        .expect_err("the other CA's pin refuses the server");

    assert!(
        matches!(error, ClientError::Transport { .. }),
        "unexpected error: {error}"
    );
    assert_no_request_after_login(&harness);
    assert!(harness.server.revoked_certificates().is_empty());
}
