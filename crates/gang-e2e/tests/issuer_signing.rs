//! The in-process issuer signs tokens without the caller installing a crypto provider.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{TestClock, Timestamp};
use gang_e2e::{Settings, TestIssuer, TokenSpec};
use serde_json::Value;

#[test]
fn a_minted_token_is_signed_without_any_provider_installed_by_the_caller() {
    let issuer = TestIssuer::start(&Settings::default()).expect("the issuer starts");

    let token = issuer
        .mint(&TokenSpec::admitted("someone"))
        .expect("the issuer mints a token");

    let segments: Vec<&str> = token.split('.').collect();
    assert_eq!(
        segments.len(),
        3,
        "a compact JWS has three segments, got {token}"
    );
    assert!(
        segments.iter().all(|segment| !segment.is_empty()),
        "every JWS segment is non-empty, got {token}"
    );

    let header_bytes = URL_SAFE_NO_PAD
        .decode(segments[0])
        .expect("the header segment is base64url");
    let header: Value = serde_json::from_slice(&header_bytes).expect("the header is JSON");

    assert_eq!(header["alg"], "ES256", "the token is signed with ES256");
}

#[test]
fn a_minted_token_takes_its_issue_and_expiry_times_from_the_injected_clock() {
    const ISSUED_AT: u64 = 1_700_000_000;
    let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
        ISSUED_AT * 1000,
    )));
    let settings = Settings::default();
    let issuer = TestIssuer::start_with_clock(&settings, clock).expect("the issuer starts");

    let token = issuer
        .mint(&TokenSpec::admitted("someone"))
        .expect("the issuer mints a token");

    let payload = token.split('.').nth(1).expect("the token has a payload");
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .expect("the payload segment is base64url");
    let claims: Value = serde_json::from_slice(&payload_bytes).expect("the payload is JSON");

    let lifetime = settings.token_lifetime.as_secs();
    assert_eq!(claims["iat"], ISSUED_AT);
    assert_eq!(claims["nbf"], ISSUED_AT);
    assert_eq!(claims["exp"], ISSUED_AT + lifetime);
}
