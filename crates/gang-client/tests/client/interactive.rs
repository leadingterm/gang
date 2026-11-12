//! Interactive login: the loopback redirect, the pasted URL, provider selection, and the
//! failures that must leave nothing stored.

use std::time::Duration;

use gang_client::{ClientError, ClientOptions, InteractiveLogin, LoggedIn};
use gang_clock::Timestamp;
use gang_types::{Bytes, NonZeroBytes, ProviderName};
use serde_json::json;

use super::support::*;

fn session_json(harness: &Harness) -> serde_json::Value {
    let bytes = std::fs::read(harness.home.session_file()).expect("reading the session");
    serde_json::from_slice(&bytes).expect("the session file parses")
}

/// The proof the fake providers mint for grant `sequence` when the clock reads `minted_at`.
/// Mirrors the fake's own construction, which the fake server must accept.
fn proof(sequence: usize, minted_at: Timestamp) -> String {
    let expires_at = (minted_at + Duration::from_hours(1)).as_unix_millis() / 1_000;
    format!(
        "{}.{}.{}",
        base64url(br#"{"alg":"ES256"}"#),
        base64url(format!(r#"{{"exp":{expires_at}}}"#).as_bytes()),
        base64url(format!("sig-{sequence}").as_bytes()),
    )
}

/// Runs an interactive login against the harness's TLS server, pinned to its CA.
async fn login(
    harness: &Harness,
    options: ClientOptions,
    requested: Option<&ProviderName>,
) -> Result<LoggedIn, ClientError> {
    let server = harness.server.url();
    let pin = ca_pin();
    harness
        .client_with(options)
        .login(&InteractiveLogin {
            server: &server,
            provider: requested,
            ca_pin: Some(&pin),
        })
        .await
}

/// Asserts that a failed login stored no session and no identity or bearer file.
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
async fn single_interactive_provider_is_chosen_when_none_is_requested() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let logged = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    assert_eq!(logged.provider, provider("google"));
}

#[tokio::test]
async fn browser_login_stores_an_interactive_session_for_the_provider() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    let session = session_json(&harness);
    assert_eq!(session["kind"]["kind"], "interactive");
    assert_eq!(session["provider"], "google");
}

#[tokio::test]
async fn browser_login_stores_the_refresh_token_privately() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    let path = harness.home.refresh_token_file();
    assert_eq!(
        std::fs::read_to_string(&path).expect("reading the refresh token"),
        "refresh-1"
    );
    assert_eq!(mode(&path), 0o600);
}

#[tokio::test]
async fn browser_login_stores_the_id_token_privately() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    let path = harness.home.id_token_file();
    assert_eq!(
        std::fs::read_to_string(&path).expect("reading the id token"),
        proof(1, START)
    );
    assert_eq!(mode(&path), 0o600);
}

#[tokio::test]
async fn browser_login_stores_the_minted_token_privately() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    let path = harness.home.token_file();
    assert!(path.exists(), "the bearer token is stored");
    assert_eq!(mode(&path), 0o600);
}

#[tokio::test]
async fn login_expiry_is_the_clock_now_plus_the_servers_lifetime() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.set_token_ttl(Duration::from_mins(7));
    harness.clock.advance(Duration::from_mins(2));
    let minted_at = START + Duration::from_mins(2);
    harness.server.accept_proof("google", &proof(1, minted_at));

    let logged = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    assert_eq!(logged.expires_at, minted_at + Duration::from_mins(7));
}

#[tokio::test]
async fn login_sends_exactly_one_token_request_carrying_the_provider_proof() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    assert_eq!(
        harness.server.token_requests(),
        vec![(provider("google"), proof(1, START))]
    );
}

#[tokio::test]
async fn login_over_tls_records_the_ca_pin_in_the_session() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));
    let pin = ca_pin();

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    assert_eq!(
        session_json(&harness)["ca_pin"],
        serde_json::to_value(pin).expect("the ca pin encodes")
    );
}

#[tokio::test]
async fn server_without_authentication_has_no_login() {
    let harness = Harness::start_plain(ConsoleMode::Browser, no_auth_info()).await;
    let server = harness.server.url();

    let error = harness
        .client()
        .login(&InteractiveLogin {
            server: &server,
            provider: None,
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
async fn several_interactive_providers_without_a_choice_are_ambiguous() {
    let harness = Harness::start_tls(
        ConsoleMode::Silent,
        token_info(&[("google", json!({})), ("okta", json!({}))], true),
    )
    .await;

    let error = login(&harness, ClientOptions::default(), None)
        .await
        .expect_err("two providers need a choice");

    assert!(
        matches!(error, ClientError::AmbiguousProvider { .. }),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn a_server_offering_no_provider_has_no_interactive_provider() {
    let harness = Harness::start_tls(ConsoleMode::Silent, token_info(&[], true)).await;

    let error = login(&harness, ClientOptions::default(), None)
        .await
        .expect_err("no provider is offered");

    assert!(
        matches!(error, ClientError::NoInteractiveProvider),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn requested_provider_absent_from_the_server_is_not_offered() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;

    let error = login(&harness, ClientOptions::default(), Some(&provider("okta")))
        .await
        .expect_err("okta is not in the server's login list");

    assert!(
        matches!(error, ClientError::ProviderNotOffered { .. }),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn pasted_url_login_stores_the_session() {
    let harness = Harness::start_tls(ConsoleMode::Paste, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let logged = login(
        &harness,
        ClientOptions::default(),
        Some(&provider("google")),
    )
    .await
    .expect("the pasted login succeeds");

    assert_eq!(logged.provider, provider("google"));
    assert_eq!(harness.console().announced().len(), 1);
    assert!(harness.home.session_file().exists());
    assert!(harness.home.refresh_token_file().exists());
}

#[tokio::test]
async fn forged_state_is_rejected_with_the_provider_error_and_stores_nothing() {
    let harness = Harness::start_tls(ConsoleMode::WrongState, google()).await;

    let error = login(&harness, ClientOptions::default(), None)
        .await
        .expect_err("a forged state is refused");

    assert!(
        matches!(error, ClientError::Provider(_)),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn oversized_callback_head_is_answered_431_and_login_continues() {
    let harness = Harness::start_tls(ConsoleMode::OversizedHead, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let options = ClientOptions {
        callback_max_head_size: NonZeroBytes::new(Bytes::new(1024))
            .expect("the callback head limit is not zero"),
        ..ClientOptions::default()
    };
    let logged = login(&harness, options, None)
        .await
        .expect("the login continues after the oversized callback");

    assert_eq!(logged.provider, provider("google"));
    let status = harness
        .console()
        .oversized_status()
        .expect("the oversized callback was answered");
    assert!(
        status.contains(" 431 "),
        "the oversized callback is answered 431, got {status}"
    );
}

#[tokio::test]
async fn login_without_a_redirect_times_out_and_stores_nothing() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;

    let options = ClientOptions::default();
    let timeout = options.login_timeout.get();
    let (result, ()) = tokio::join!(login(&harness, options, None), async {
        while harness.console().announced().is_empty() {
            tokio::task::yield_now().await;
        }
        harness.clock.advance(timeout);
    });
    let error = result.expect_err("nothing arrives, so the login times out");

    assert!(
        matches!(error, ClientError::LoginTimedOut),
        "unexpected error: {error}"
    );
    assert_nothing_stored(&harness);
}

#[tokio::test]
async fn successful_login_begins_and_completes_exactly_one_provider_login() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));

    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    assert_eq!(harness.providers.begin_login_calls(), 1);
    assert_eq!(harness.providers.complete_login_calls(), 1);
}

#[tokio::test]
async fn renewal_without_a_rotated_refresh_token_keeps_the_stored_one() {
    let harness = Harness::start_tls(ConsoleMode::Browser, google()).await;
    harness.server.accept_proof("google", &proof(1, START));
    let _logged_in = login(&harness, ClientOptions::default(), None)
        .await
        .expect("the loopback login succeeds");

    harness.providers.withhold_rotated_refresh_tokens();
    let renewed_proof = proof(2, START + Duration::from_hours(1) + Duration::from_secs(1));
    harness.server.accept_proof("google", &renewed_proof);
    harness
        .clock
        .advance(Duration::from_hours(1) + Duration::from_secs(1));

    let minted = harness.client().token().await.expect("token is re-minted");

    assert_eq!(minted.value.expose(), "gang-token-2");
    assert_eq!(harness.providers.renew_calls(), 1);
    assert_eq!(
        std::fs::read_to_string(harness.home.refresh_token_file())
            .expect("refresh token file is readable"),
        "refresh-1",
        "no rotated token was returned, so the stored one is kept"
    );
    assert_eq!(
        harness.server.token_requests().last(),
        Some(&(provider("google"), renewed_proof))
    );
}
