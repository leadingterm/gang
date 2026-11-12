//! Proof verification: id-token acceptance and rejection, required claims and discovery caching.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use super::support::TestIssuer;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{PortError, ProofVerifierPort, Rejection};
use gang_testkit::generate_es256_key;
use gang_types::{Audience, Bytes, NonZeroBytes, Proof, Url};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};

const START_MILLIS: u64 = 1_800_000_000_000;
const TOKEN_LIFETIME_SECS: u64 = 3600;
const USER: &str = "dev@example.test";
const RSA_MODULUS: &str = "0PCcF-PRA4WQYkHrmwIEuKg3p2bHjzaBRMYL1cfs2_ZMW7qG0ABKox0bnQHYnBuzGCfq4XMRmq0O6fQdz3pjCOJ3Eb1F0Sm8jc7Lr7NdNKO-aHpgI7hT8CnMQsiTQN_LCqOa1yQfsoQFQQuD2jVjviz7kExGnsiY_VmoGdItYL7y3S0oH5LAGEOsJ0kLPGAfltdaGcqTBnEtFNNEsr9LGvbmWszOST24v1GXn5FofEpk282K-gyPowFmOfA4Th4B6zOJ6TCXbxs9BrIGaPhL2Jqrlw2D43w6vCSdppj_kFuy4vak9Y4iuOaEA7-VMFwmeIIhQXKRl8vmdfQqO3jBEQ";
const P256_SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];

fn new_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(START_MILLIS)))
}

fn now_secs(clock: &TestClock) -> u64 {
    clock.wall().as_unix_millis() / 1000
}

/// Claims for [`USER`], valid for an hour from the clock's current reading.
fn user_claims(issuer: &TestIssuer<'_>, clock: &TestClock) -> Value {
    let now = now_secs(clock);
    let mut claims = issuer.valid_claims();
    claims["iat"] = json!(now);
    claims["exp"] = json!(now + TOKEN_LIFETIME_SECS);
    claims[issuer.config().principal_claim.as_str()] = json!(USER);
    claims
}

fn provider_for(issuer: &TestIssuer<'_>, clock: &Arc<TestClock>) -> Provider {
    provider_with(issuer, clock, |_| {})
}

/// Builds a provider whose configuration is the issuer's, adjusted by `adjust`.
fn provider_with(
    issuer: &TestIssuer<'_>,
    clock: &Arc<TestClock>,
    adjust: impl FnOnce(&mut OidcConfig),
) -> Provider {
    let mut config = issuer.config();
    adjust(&mut config);
    Provider::new(config, clock.clone()).expect("provider builds")
}

fn is_rejected(result: &Result<gang_types::Principal, PortError>, rejection: Rejection) -> bool {
    matches!(result, Err(PortError::Rejected { reason: found, .. }) if *found == rejection)
}

fn invalid_proof(result: &Result<gang_types::Principal, PortError>) -> bool {
    is_rejected(result, Rejection::InvalidProof)
}

fn policy_denied(result: &Result<gang_types::Principal, PortError>) -> bool {
    is_rejected(result, Rejection::PolicyDenied)
}

/// A token signed by a key the issuer never publishes.
fn unpublished_key_token(claims: &Value) -> String {
    generate_es256_key("unpublished")
        .expect("generating a signing key")
        .sign(claims)
        .expect("token signs")
}

fn encode_segment(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Fetches the issuer's JWKS over plain HTTP from a blocking thread.
async fn fetch_jwks(issuer_url: Url) -> Value {
    tokio::task::spawn_blocking(move || {
        let address = format!(
            "{}:{}",
            issuer_url.host_str().expect("issuer has a host"),
            issuer_url.port().expect("issuer has a port")
        );
        let mut stream = std::net::TcpStream::connect(address).expect("issuer accepts connections");
        stream
            .write_all(b"GET /jwks HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .expect("request writes");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("response reads");
        let body = response
            .split_once("\r\n\r\n")
            .expect("response has a body")
            .1;
        serde_json::from_str::<Value>(body).expect("jwks parses")
    })
    .await
    .expect("blocking fetch completes")
}

/// An HS256 token keyed with the issuer's published public key, under the published key id.
async fn hmac_token_with_public_key(issuer: &TestIssuer<'_>, claims: &Value) -> String {
    let jwks = fetch_jwks(issuer.url()).await;
    let key = &jwks["keys"][0];
    let kid = key["kid"].as_str().expect("key has a kid").to_owned();
    let x = URL_SAFE_NO_PAD
        .decode(key["x"].as_str().expect("key has x"))
        .expect("x decodes");
    let y = URL_SAFE_NO_PAD
        .decode(key["y"].as_str().expect("key has y"))
        .expect("y decodes");
    let mut der = P256_SPKI_PREFIX.to_vec();
    der.push(0x04);
    der.extend_from_slice(&x);
    der.extend_from_slice(&y);
    let _ = jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default();
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid);
    encode(&header, claims, &EncodingKey::from_secret(&der)).expect("token signs")
}

/// An unsecured (`alg: none`) token with an empty signature segment.
fn none_token(claims: &Value) -> String {
    let header = json!({"alg": "none", "typ": "JWT"});
    format!(
        "{}.{}.",
        encode_segment(header.to_string().as_bytes()),
        encode_segment(claims.to_string().as_bytes())
    )
}

#[tokio::test]
async fn valid_token_is_accepted_with_the_email_as_principal() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let token = issuer.mint(&user_claims(&issuer, &clock));
    let principal = provider
        .verify(&Proof::new(token))
        .await
        .expect("valid token is accepted");

    assert_eq!(principal.as_str(), USER);
}

#[tokio::test]
async fn wrong_audience_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let mut claims = user_claims(&issuer, &clock);
    claims["aud"] = json!("some-other-client");

    let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn token_for_an_additional_audience_is_accepted() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.additional_audiences =
            vec![Audience::try_new("some-other-client").expect("test audience is valid")];
    });
    let mut claims = user_claims(&issuer, &clock);
    claims["aud"] = json!("some-other-client");

    let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;

    assert_eq!(result.expect("proof verifies").as_str(), USER);
}

#[tokio::test]
async fn token_for_an_unlisted_audience_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.additional_audiences =
            vec![Audience::try_new("listed-client").expect("test audience is valid")];
    });
    let mut claims = user_claims(&issuer, &clock);
    claims["aud"] = json!("unlisted-client");

    let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn wrong_issuer_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let mut claims = user_claims(&issuer, &clock);
    claims["iss"] = json!("https://other.example.test/");

    let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn token_is_accepted_within_the_clock_skew_and_rejected_after_it() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let mut claims = user_claims(&issuer, &clock);
    claims["exp"] = json!(now_secs(&clock) + TOKEN_LIFETIME_SECS);
    let token = issuer.mint(&claims);

    clock.advance(Duration::from_secs(TOKEN_LIFETIME_SECS + 29));
    provider
        .verify(&Proof::new(token.clone()))
        .await
        .expect("token inside the skew allowance is accepted");

    clock.advance(Duration::from_secs(2));
    let result = provider.verify(&Proof::new(token)).await;
    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn expired_token_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let token = issuer.mint(&user_claims(&issuer, &clock));

    clock.advance(Duration::from_secs(TOKEN_LIFETIME_SECS + 31));
    let result = provider.verify(&Proof::new(token)).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn token_without_exp_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let mut claims = user_claims(&issuer, &clock);
    claims
        .as_object_mut()
        .expect("claims are an object")
        .remove("exp");

    let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn token_not_yet_valid_is_invalid_proof_but_small_nbf_skew_is_accepted() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let now = now_secs(&clock);

    let mut future = user_claims(&issuer, &clock);
    future["nbf"] = json!(now + TOKEN_LIFETIME_SECS);
    let result = provider.verify(&Proof::new(issuer.mint(&future))).await;
    assert!(invalid_proof(&result));

    let mut slightly_early = user_claims(&issuer, &clock);
    slightly_early["nbf"] = json!(now + 10);
    provider
        .verify(&Proof::new(issuer.mint(&slightly_early)))
        .await
        .expect("nbf within the skew allowance is accepted");
}

#[tokio::test]
async fn token_signed_by_an_unpublished_key_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let result = provider
        .verify(&Proof::new(unpublished_key_token(&user_claims(
            &issuer, &clock,
        ))))
        .await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn hs256_token_keyed_with_the_public_key_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let token = hmac_token_with_public_key(&issuer, &user_claims(&issuer, &clock)).await;
    let result = provider.verify(&Proof::new(token)).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn alg_none_token_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let result = provider
        .verify(&Proof::new(none_token(&user_claims(&issuer, &clock))))
        .await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn garbage_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    for garbage in ["not-a-token", "a.b.c", "....", "%%%.%%%.%%%"] {
        let result = provider.verify(&Proof::new(garbage)).await;
        assert!(invalid_proof(&result), "{garbage} should be invalid");
    }
}

/// A token whose header names `alg` and `kid`, with arbitrary signature bytes.
fn token_with_header(alg: &str, kid: &str, claims: &Value) -> String {
    let header = json!({"alg": alg, "typ": "JWT", "kid": kid});
    format!(
        "{}.{}.{}",
        encode_segment(header.to_string().as_bytes()),
        encode_segment(claims.to_string().as_bytes()),
        encode_segment(&[0x13; 64])
    )
}

#[tokio::test]
async fn es256_token_naming_an_rsa_key_is_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    issuer.publish_jwk(json!({
        "kty": "RSA",
        "kid": "rsa-1",
        "alg": "RS256",
        "use": "sig",
        "n": RSA_MODULUS,
        "e": "AQAB"
    }));
    let provider = provider_for(&issuer, &clock);

    let token = token_with_header("ES256", "rsa-1", &user_claims(&issuer, &clock));
    let result = provider.verify(&Proof::new(token)).await;

    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn non_es256_algorithms_under_the_published_kid_are_invalid_proof() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let jwks = fetch_jwks(issuer.url()).await;
    let kid = jwks["keys"][0]["kid"].as_str().expect("key has a kid");

    for alg in ["RS256", "PS256", "EdDSA"] {
        let token = token_with_header(alg, kid, &user_claims(&issuer, &clock));
        let result = provider.verify(&Proof::new(token)).await;
        assert!(invalid_proof(&result), "{alg} token should be invalid");
    }
}

#[tokio::test]
async fn missing_principal_claim_is_policy_denied() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    let principal_claim = issuer.config().principal_claim;

    let mut missing = user_claims(&issuer, &clock);
    missing
        .as_object_mut()
        .expect("claims are an object")
        .remove(&principal_claim);
    assert!(policy_denied(
        &provider.verify(&Proof::new(issuer.mint(&missing))).await
    ));

    for value in [json!(42), json!(""), json!(["dev@example.test"])] {
        let mut claims = user_claims(&issuer, &clock);
        claims[principal_claim.as_str()] = value.clone();
        let result = provider.verify(&Proof::new(issuer.mint(&claims))).await;
        assert!(policy_denied(&result), "principal {value} should be denied");
    }
}

#[tokio::test]
async fn required_claims_must_match_exactly() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let mut config = issuer.config();
    config
        .required_claims
        .insert("email_verified".to_owned(), json!(true));
    config
        .required_claims
        .insert("tenant".to_owned(), json!("alpha"));
    let provider = Provider::new(config, clock.clone()).expect("provider builds");

    let mut matching = user_claims(&issuer, &clock);
    matching["email_verified"] = json!(true);
    matching["tenant"] = json!("alpha");
    let principal = provider
        .verify(&Proof::new(issuer.mint(&matching)))
        .await
        .expect("matching required claims are accepted");
    assert_eq!(principal.as_str(), USER);

    let mut unverified = matching.clone();
    unverified["email_verified"] = json!(false);
    assert!(policy_denied(
        &provider.verify(&Proof::new(issuer.mint(&unverified))).await
    ));

    let mut stringly = matching.clone();
    stringly["email_verified"] = json!("true");
    assert!(policy_denied(
        &provider.verify(&Proof::new(issuer.mint(&stringly))).await
    ));

    let mut other_tenant = matching.clone();
    other_tenant["tenant"] = json!("beta");
    assert!(policy_denied(
        &provider
            .verify(&Proof::new(issuer.mint(&other_tenant)))
            .await
    ));

    let mut missing = matching.clone();
    missing
        .as_object_mut()
        .expect("claims are an object")
        .remove("tenant");
    assert!(policy_denied(
        &provider.verify(&Proof::new(issuer.mint(&missing))).await
    ));
}

#[tokio::test]
async fn discovery_is_fetched_once_across_many_verifications() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    assert_eq!(issuer.discovery_fetches(), 0);

    for _ in 0..5 {
        let token = issuer.mint(&user_claims(&issuer, &clock));
        provider
            .verify(&Proof::new(token))
            .await
            .expect("valid token is accepted");
    }

    assert_eq!(issuer.discovery_fetches(), 1);
}

#[tokio::test]
async fn zero_clock_skew_expires_the_token_exactly_at_exp() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.tokens.clock_skew = Duration::ZERO;
    });
    let token = issuer.mint(&user_claims(&issuer, &clock));

    clock.advance(Duration::from_secs(TOKEN_LIFETIME_SECS - 1));
    provider
        .verify(&Proof::new(token.clone()))
        .await
        .expect("token is valid one second before exp");

    clock.advance(Duration::from_secs(1));
    let result = provider.verify(&Proof::new(token)).await;
    assert!(invalid_proof(&result));
}

#[tokio::test]
async fn discovery_document_over_the_response_cap_is_a_backend_failure() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.http.max_response_size =
            NonZeroBytes::new(Bytes::new(1)).expect("one byte is non-zero");
    });
    let token = issuer.mint(&user_claims(&issuer, &clock));

    let result = provider.verify(&Proof::new(token)).await;

    assert!(matches!(result, Err(PortError::Backend(_))));
}

#[tokio::test]
async fn oversized_server_error_is_still_unavailable() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.http.max_response_size =
            NonZeroBytes::new(Bytes::new(1)).expect("one byte is non-zero");
    });
    issuer.set_discovery_oversized_failure(true);
    let token = issuer.mint(&user_claims(&issuer, &clock));

    let result = provider.verify(&Proof::new(token)).await;

    assert!(matches!(result, Err(PortError::Unavailable(_))));
}
