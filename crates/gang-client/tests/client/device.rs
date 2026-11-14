//! Device login and renewal against the scripted server.

use std::time::Duration;

use gang_client::{ClientError, InteractiveLogin, LocalBrowser};
use gang_clock::Timestamp;
use gang_wire::{Authentication, DeviceLogin};

use super::support::{ConsoleMode, Harness, START, base64url, google, provider};

/// The first proof minted by the fake provider.
fn proof() -> String {
    let expiry = (START + Duration::from_hours(1)).as_unix_millis() / 1_000;
    format!(
        "{}.{}.{}",
        base64url(br#"{"alg":"ES256"}"#),
        base64url(format!(r#"{{"exp":{expiry}}}"#).as_bytes()),
        base64url(b"sig-1")
    )
}

/// Server information with device login enabled.
fn device_info() -> gang_wire::Server<serde_json::Value> {
    let mut info = google();
    if let Authentication::Token(token) = &mut info.authentication {
        token.device_login = Some(DeviceLogin {
            provider: provider("google"),
        });
    }
    info
}

/// Runs a device-capable interactive login.
async fn login(h: &Harness, browser: LocalBrowser) -> Result<gang_client::LoggedIn, ClientError> {
    let server = h.server.url();
    h.client()
        .login(&InteractiveLogin {
            server: &server,
            provider: None,
            ca_pin: None,
            browser,
        })
        .await
}

/// Advances one poll after the client has processed the preceding answer.
async fn advance_poll(h: &Harness, count: usize, delay: Duration) {
    h.server.wait_device_calls(count).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    h.clock.advance(delay);
}

#[tokio::test]
async fn device_login_without_a_browser_polls_until_approved() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    h.server
        .device_answers(&[(400, None), (400, None), (200, None)]);
    let (result, ()) = tokio::join!(login(&h, LocalBrowser::Unavailable), async {
        for count in 1..=3 {
            advance_poll(&h, count, Duration::from_secs(5)).await;
        }
    });
    let logged = result.expect("device login succeeds");
    assert_eq!(logged.provider, provider("google"));
    assert_eq!(h.console().devices().len(), 1);
    assert!(h.console().announced().is_empty());
    assert_eq!(h.console().paste_reads(), 0);
    let session: serde_json::Value =
        serde_json::from_slice(&std::fs::read(h.home.session_file()).expect("session"))
            .expect("json");
    assert_eq!(session["kind"]["kind"], "device");
    assert_eq!(
        std::fs::read_to_string(h.home.refresh_token_file()).expect("refresh"),
        "device-refresh"
    );
    assert!(h.home.token_file().exists());
}

#[tokio::test]
async fn device_login_survives_a_client_clock_ahead_of_the_server() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    let server_time = Timestamp::from_unix_millis(START.as_unix_millis() - 3_600_000);
    h.server.device_create_time(server_time);
    h.server.device_answers(&[(400, None), (200, None)]);
    let login = login(&h, LocalBrowser::Unavailable);
    tokio::pin!(login);
    let result = tokio::select! {
        result = &mut login => result,
        () = async {
            advance_poll(&h, 1, Duration::from_secs(5)).await;
            advance_poll(&h, 2, Duration::from_secs(5)).await;
        } => login.await,
    };
    let logged = result.expect("device login succeeds despite clock skew");
    assert_eq!(logged.provider, provider("google"));
    assert_eq!(h.server.device_calls().len(), 3);
    let announcements = h.console().devices();
    assert_eq!(announcements[0].create_time, server_time);
    assert_eq!(
        announcements[0].expire_time,
        server_time + Duration::from_mins(10)
    );
    assert!(h.home.session_file().exists());
}

#[tokio::test]
async fn device_login_honours_retry_after_on_429() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    h.server.device_answers(&[(429, Some(17)), (200, None)]);
    let (result, ()) = tokio::join!(login(&h, LocalBrowser::Unavailable), async {
        advance_poll(&h, 1, Duration::from_secs(5)).await;
        advance_poll(&h, 2, Duration::from_secs(17)).await;
    });
    let logged = result.expect("approved");
    assert_eq!(logged.provider, provider("google"));
    let calls = h.server.device_calls();
    assert_eq!(
        calls[2].1.saturating_duration_since(calls[1].1),
        Duration::from_secs(17)
    );
}

#[tokio::test]
async fn device_login_ends_at_the_code_expiry() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    h.server.device_ttl(Duration::from_secs(5));
    let (result, ()) = tokio::join!(login(&h, LocalBrowser::Unavailable), async {
        while h.console().devices().is_empty() {
            tokio::task::yield_now().await;
        }
        h.clock.advance(Duration::from_secs(5));
    });
    let error = result.expect_err("device login times out");
    assert!(matches!(error, ClientError::DeviceApprovalTimedOut));
    assert_eq!(
        error.to_string(),
        "login timed out waiting for the device approval"
    );
    assert!(!h.home.session_file().exists());
    assert!(!h.home.token_file().exists());
}

#[tokio::test]
async fn device_login_404_is_expired() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    h.server.device_answers(&[(404, None)]);
    let (result, ()) = tokio::join!(login(&h, LocalBrowser::Unavailable), async {
        advance_poll(&h, 1, Duration::from_secs(5)).await;
    });
    assert!(matches!(result, Err(ClientError::DeviceLoginExpired)));
}

#[tokio::test]
async fn with_a_browser_the_loopback_login_can_win_the_race() {
    let h = Harness::start_plain(ConsoleMode::Browser, device_info()).await;
    h.server.accept_proof("google", &proof());
    let logged = login(&h, LocalBrowser::Available)
        .await
        .expect("loopback wins");
    assert_eq!(logged.provider, provider("google"));
    assert_eq!(h.providers.complete_login_calls(), 1);
    assert_eq!(h.console().paste_reads(), 0);
}

#[tokio::test]
async fn with_a_browser_the_device_login_can_win_the_race() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    h.server.device_answers(&[(200, None)]);
    let (result, ()) = tokio::join!(login(&h, LocalBrowser::Available), async {
        advance_poll(&h, 1, Duration::from_secs(5)).await;
    });
    let logged = result.expect("device wins");
    assert_eq!(logged.provider, provider("google"));
    assert_eq!(h.providers.complete_login_calls(), 0);
    assert_eq!(h.console().paste_reads(), 0);
}

#[tokio::test]
async fn device_offered_but_another_provider_requested_uses_the_loopback_login() {
    let mut info = device_info();
    if let Authentication::Token(token) = &mut info.authentication {
        token.login.insert(provider("other"), serde_json::json!({}));
    }
    let h = Harness::start_plain(ConsoleMode::Browser, info).await;
    h.server.accept_proof("other", &proof());
    let server = h.server.url();
    let name = provider("other");
    let logged = h
        .client()
        .login(&InteractiveLogin {
            server: &server,
            provider: Some(&name),
            ca_pin: None,
            browser: LocalBrowser::Unavailable,
        })
        .await
        .expect("loopback");
    assert_eq!(logged.provider, name);
    assert!(h.server.device_calls().is_empty());
}

/// Creates a device session and advances past its bearer token expiry.
async fn expired_session(h: &Harness) {
    h.server.device_answers(&[(200, None)]);
    let (result, ()) = tokio::join!(login(h, LocalBrowser::Unavailable), async {
        advance_poll(h, 1, Duration::from_secs(5)).await;
    });
    let logged = result.expect("device login");
    assert_eq!(logged.provider, provider("google"));
    h.clock.advance(Duration::from_mins(11));
}

#[tokio::test]
async fn a_device_session_renews_through_the_server_and_stores_the_rotated_token_first() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    expired_session(&h).await;
    h.server.accept_proof("google", "device-refresh");
    h.server.device_refresh(Some("rotated"));
    h.server.block_token_write(h.home.token_file());
    assert!(h.client().token().await.is_err());
    assert_eq!(
        std::fs::read_to_string(h.home.refresh_token_file()).expect("refresh"),
        "rotated"
    );
    assert_eq!(h.server.token_requests()[0].1, "device-refresh");
}

#[tokio::test]
async fn a_rejected_device_refresh_token_forgets_the_login() {
    let h = Harness::start_plain(ConsoleMode::Silent, device_info()).await;
    expired_session(&h).await;
    assert!(matches!(
        h.client().token().await,
        Err(ClientError::LoginRejected)
    ));
    assert!(!h.home.session_file().exists());
    assert!(!h.home.token_file().exists());
    assert!(!h.home.refresh_token_file().exists());
}
