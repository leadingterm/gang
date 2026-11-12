//! Minting and verifying Gang's bearer tokens, with fixed keys and injected times.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_authn::{AUDIENCE, TokenError, TokenKey, TokenUse, issuer_of, mint, verify};
use gang_clock::Timestamp;
use gang_spi::{CertificateSignerPort, PortError, Rejection};
use gang_testkit::{TestSigner, signing_key};
use gang_types::{CaPin, Identity, Url};
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{DerSignature, Signature, SigningKey};

use crate::support::identity;

const ISSUER: &str = "https://gang.example.com";
const SUBJECT: &str = "example-idp:alice@example.com";
const NOW_MS: u64 = 1_800_000_000_000;
const NOW_SECS: u64 = NOW_MS / 1000;
const EXP_MS: u64 = NOW_MS + 60_000;
const TTL: Duration = Duration::from_mins(1);
const SKEW: Duration = Duration::from_secs(5);

/// The P-256 key for `seed`, which the tests here only build from valid scalars.
fn signing(seed: u8) -> SigningKey {
    signing_key(seed).expect("the seed is a valid scalar")
}

/// A signer and the token key for its public half.
async fn fixture(seed: u8) -> (TestSigner, TokenKey) {
    let signer = TestSigner::new(seed).expect("the seed is a valid scalar");
    let der = signer.public_key().await.expect("test public key encodes");
    let key = TokenKey::from_spki(&der).expect("test public key loads");
    (signer, key)
}

fn at(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn endpoint() -> Url {
    Url::parse(ISSUER).expect("issuer is a URL")
}

fn alice() -> Identity {
    identity("example-idp", "alice@example.com")
}

fn header_json(alg: &str, kid: &str) -> String {
    format!(r#"{{"alg":"{alg}","typ":"JWT","kid":"{kid}"}}"#)
}

fn claims_json(issuer: &str, audience: &str, not_before: u64, expires: u64) -> String {
    format!(
        r#"{{"iss":"{issuer}","aud":"{audience}","sub":"{SUBJECT}","iat":{not_before},"nbf":{not_before},"exp":{expires},"use":"client"}}"#
    )
}

/// Signs `header` and `claims` with `signing`, the way a hand-built token would be.
fn sign_compact(signing: &SigningKey, header: &str, claims: &str) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let der: DerSignature = signing.try_sign(input.as_bytes()).expect("test key signs");
    let signature = Signature::from_der(der.as_bytes()).expect("test signature is DER");
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

fn segment(token: &str, index: usize) -> &str {
    token
        .split('.')
        .nth(index)
        .expect("token has the requested segment")
}

fn replace_segment(token: &str, index: usize, value: &str) -> String {
    token
        .split('.')
        .enumerate()
        .map(|(position, text)| if position == index { value } else { text })
        .collect::<Vec<_>>()
        .join(".")
}

/// A valid client token for `alice` minted at `NOW_MS` with the shared TTL.
async fn fresh_token(signer: &TestSigner, key: &TokenKey) -> String {
    mint(
        signer,
        key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        TTL,
        TokenUse::Client,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned()
}

#[tokio::test]
async fn round_trip_returns_identity_and_validity_window() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;

    let verified = verify(
        &token,
        &key,
        &endpoint(),
        at(NOW_MS),
        SKEW,
        TokenUse::Client,
    )
    .expect("fresh token verifies");

    assert_eq!(verified.identity, alice());
    assert_eq!(verified.valid_until, at(EXP_MS + 5_000));
}

#[tokio::test]
async fn expires_in_equals_ttl() {
    let (signer, key) = fixture(1).await;

    let issued = mint(
        &signer,
        &key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        TTL,
        TokenUse::Client,
    )
    .await
    .expect("token mints");

    assert_eq!(issued.expires_in, TTL);
}

#[tokio::test]
async fn expiry_rounds_up_to_whole_seconds() {
    let (signer, key) = fixture(1).await;
    let ttl = Duration::from_millis(1_500);
    let token = mint(
        &signer,
        &key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        ttl,
        TokenUse::Client,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned();

    let verified = verify(
        &token,
        &key,
        &endpoint(),
        at(NOW_MS),
        Duration::ZERO,
        TokenUse::Client,
    )
    .expect("token verifies at issue time");

    assert_eq!(verified.valid_until, at(NOW_MS + 2_000));
}

#[tokio::test]
async fn principals_with_separators_survive_the_round_trip() {
    let (signer, key) = fixture(1).await;
    for principal in ["repo:org/name:ref:refs/heads/main", "dev@example.com"] {
        let subject = identity("example-idp", principal);
        let token = mint(
            &signer,
            &key,
            ISSUER,
            &subject,
            at(NOW_MS),
            TTL,
            TokenUse::Client,
        )
        .await
        .expect("token mints")
        .token
        .expose()
        .to_owned();

        let verified = verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client,
        )
        .expect("token verifies");

        assert_eq!(verified.identity, subject);
    }
}

#[tokio::test]
async fn tampered_payload_is_a_signature_failure() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;
    let other = mint(
        &signer,
        &key,
        ISSUER,
        &identity("example-idp", "bob@example.com"),
        at(NOW_MS),
        TTL,
        TokenUse::Client,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned();
    let forged = replace_segment(&token, 1, segment(&other, 1));

    assert_eq!(
        verify(
            &forged,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("forged payload is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn tampered_signature_is_a_signature_failure() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;
    let signature = segment(&token, 2);
    let first = if signature.starts_with('A') { "B" } else { "A" };
    let altered = replace_segment(&token, 2, &format!("{first}{}", &signature[1..]));

    assert_eq!(
        verify(
            &altered,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("altered signature is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn malformed_tokens_are_malformed() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;
    let without_signature = token.rsplit_once('.').expect("token has a signature").0;
    let candidates = [
        String::new(),
        without_signature.to_owned(),
        format!("{token}.AAAA"),
        "a.b.c".to_owned(),
        "A".repeat(16 * 1024),
    ];

    for candidate in &candidates {
        assert_eq!(
            verify(
                candidate,
                &key,
                &endpoint(),
                at(NOW_MS),
                SKEW,
                TokenUse::Client
            )
            .expect_err("malformed token is refused"),
            TokenError::Malformed,
            "candidate of length {}",
            candidate.len()
        );
    }
}

#[tokio::test]
async fn truncated_signature_is_a_signature_failure() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;

    assert_eq!(
        verify(
            &token[..token.len() - 1],
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("truncated signature is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn empty_signature_is_a_signature_failure() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;
    let without_signature = token.rsplit_once('.').expect("token has a signature").0;

    assert_eq!(
        verify(
            &format!("{without_signature}."),
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("unsigned token is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn token_signed_by_another_key_is_a_signature_failure() {
    let (_, key) = fixture(1).await;
    let claims = claims_json(ISSUER, AUDIENCE, NOW_SECS, NOW_SECS + 60);
    let token = sign_compact(&signing(2), &header_json("ES256", key.kid()), &claims);

    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("foreign signature is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn token_from_another_signer_is_refused_by_key_id() {
    let (_, key) = fixture(1).await;
    let (other_signer, other_key) = fixture(2).await;
    let token = mint(
        &other_signer,
        &other_key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        TTL,
        TokenUse::Client,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned();

    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("another key id is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn wrong_key_id_is_a_signature_failure() {
    let (_, key) = fixture(1).await;
    let claims = claims_json(ISSUER, AUDIENCE, NOW_SECS, NOW_SECS + 60);
    let token = sign_compact(&signing(1), &header_json("ES256", "not-the-key"), &claims);

    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("wrong kid is refused"),
        TokenError::Signature
    );
}

#[tokio::test]
async fn wrong_issuer_is_refused() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;

    assert_eq!(
        verify(
            &token,
            &key,
            &Url::parse("https://other.example.com").expect("issuer is a URL"),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("other issuer is refused"),
        TokenError::Issuer
    );
}

#[tokio::test]
async fn audience_other_than_gang_is_refused() {
    let (_, key) = fixture(1).await;
    let claims = claims_json(ISSUER, "other-service", NOW_SECS, NOW_SECS + 60);
    let token = sign_compact(&signing(1), &header_json("ES256", key.kid()), &claims);

    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("other audience is refused"),
        TokenError::Audience
    );
}

#[tokio::test]
async fn expiry_is_judged_with_clock_skew() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;

    assert!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(EXP_MS + 4_999),
            SKEW,
            TokenUse::Client
        )
        .is_ok()
    );
    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(EXP_MS + 5_000),
            SKEW,
            TokenUse::Client
        )
        .expect_err("token has expired"),
        TokenError::Expired
    );
    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(EXP_MS + 10_000),
            SKEW,
            TokenUse::Client
        )
        .expect_err("token has expired"),
        TokenError::Expired
    );
}

#[tokio::test]
async fn not_before_is_judged_with_clock_skew() {
    let (signer, key) = fixture(1).await;
    let token = fresh_token(&signer, &key).await;

    assert!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS - 5_000),
            SKEW,
            TokenUse::Client
        )
        .is_ok()
    );
    assert!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS - 4_000),
            SKEW,
            TokenUse::Client
        )
        .is_ok()
    );
    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS - 6_000),
            SKEW,
            TokenUse::Client
        )
        .expect_err("token is not yet valid"),
        TokenError::NotYetValid
    );
}

#[tokio::test]
async fn algorithms_other_than_es256_are_refused() {
    let (_, key) = fixture(1).await;
    let claims = claims_json(ISSUER, AUDIENCE, NOW_SECS, NOW_SECS + 60);
    let payload = URL_SAFE_NO_PAD.encode(&claims);
    let unsigned = format!(
        "{}.{payload}.",
        URL_SAFE_NO_PAD.encode(header_json("none", key.kid()))
    );
    let hmac = format!(
        "{}.{payload}.{}",
        URL_SAFE_NO_PAD.encode(header_json("HS256", key.kid())),
        URL_SAFE_NO_PAD.encode(b"not a signature")
    );

    for token in [unsigned, hmac] {
        assert_eq!(
            verify(
                &token,
                &key,
                &endpoint(),
                at(NOW_MS),
                SKEW,
                TokenUse::Client
            )
            .expect_err("non-es256 algorithm is refused"),
            TokenError::Algorithm
        );
    }
}

#[tokio::test]
async fn session_token_verifies_as_session() {
    let (signer, key) = fixture(1).await;
    let token = mint(
        &signer,
        &key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        TTL,
        TokenUse::Session,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned();

    let verified = verify(
        &token,
        &key,
        &endpoint(),
        at(NOW_MS),
        SKEW,
        TokenUse::Session,
    )
    .expect("session token verifies as session");

    assert_eq!(verified.identity, alice());
}

#[tokio::test]
async fn tokens_are_refused_for_the_other_use() {
    let (signer, key) = fixture(1).await;
    let client = fresh_token(&signer, &key).await;
    let session = mint(
        &signer,
        &key,
        ISSUER,
        &alice(),
        at(NOW_MS),
        TTL,
        TokenUse::Session,
    )
    .await
    .expect("token mints")
    .token
    .expose()
    .to_owned();

    assert_eq!(
        verify(
            &session,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("session token is refused as client"),
        TokenError::Usage
    );
    assert_eq!(
        verify(
            &client,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Session
        )
        .expect_err("client token is refused as session"),
        TokenError::Usage
    );
}

#[tokio::test]
async fn token_without_use_claim_is_malformed() {
    let (_, key) = fixture(1).await;
    let claims = format!(
        r#"{{"iss":"{ISSUER}","aud":"{AUDIENCE}","sub":"{SUBJECT}","iat":{NOW_SECS},"nbf":{NOW_SECS},"exp":{}}}"#,
        NOW_SECS + 60
    );
    let token = sign_compact(&signing(1), &header_json("ES256", key.kid()), &claims);

    assert_eq!(
        verify(
            &token,
            &key,
            &endpoint(),
            at(NOW_MS),
            SKEW,
            TokenUse::Client
        )
        .expect_err("token without a use is refused"),
        TokenError::Malformed
    );
}

#[test]
fn issuer_keeps_non_default_port_and_drops_path() {
    let endpoint =
        Url::parse("https://gang.example.com:8443/api/v1?x=1").expect("test endpoint parses");
    assert_eq!(issuer_of(&endpoint), "https://gang.example.com:8443");

    let default_port =
        Url::parse("https://gang.example.com:443/api").expect("test endpoint parses");
    assert_eq!(issuer_of(&default_port), "https://gang.example.com");
}

#[tokio::test]
async fn key_id_is_lowercase_hex_sha256_of_the_spki() {
    let (signer, key) = fixture(1).await;
    let der = signer.public_key().await.expect("test public key encodes");

    let expected = CaPin::of_spki(&der).digest_hex().to_string();

    assert_eq!(key.kid(), expected);
}

#[test]
fn key_loading_rejects_non_spki_input() {
    assert!(matches!(
        TokenKey::from_spki(b"not a public key"),
        Err(TokenError::Key(_))
    ));
}

#[test]
fn token_errors_map_to_port_errors() {
    assert!(matches!(
        PortError::from(TokenError::Expired),
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        }
    ));
    assert!(matches!(
        PortError::from(TokenError::Usage),
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        }
    ));
    assert!(matches!(
        PortError::from(TokenError::Key("bad key".to_owned())),
        PortError::Backend(_)
    ));
}
