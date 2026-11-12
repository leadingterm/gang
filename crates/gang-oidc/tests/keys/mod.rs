//! Signing-key cache behaviour: rotation, unknown key ids, retirement, and failing or held
//! refetches, driven by the injected clock.

use std::sync::Arc;
use std::time::Duration;

use super::support::TestIssuer;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{PortError, ProofVerifierPort, Rejection};
use gang_testkit::{generate_es256_key, park_then_signal, ready_now};
use gang_types::{NonZeroDuration, Principal, Proof};
use serde_json::{Value, json};
use tokio::sync::oneshot;

const START_MILLIS: u64 = 1_800_000_000_000;
const TOKEN_LIFETIME_SECS: u64 = 10 * 3600;
const KEY_TTL_SECS: u64 = 3600;
const KEY_REFRESH_INTERVAL_SECS: u64 = 30;

fn new_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(START_MILLIS)))
}

fn provider_for(issuer: &TestIssuer<'_>, clock: &Arc<TestClock>) -> Provider {
    Provider::new(issuer.config(), clock.clone()).expect("provider builds")
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

/// Claims that stay valid across the clock advances used in these tests.
fn long_lived_claims(issuer: &TestIssuer<'_>, clock: &TestClock) -> Value {
    let now = clock.wall().as_unix_millis() / 1000;
    let mut claims = issuer.valid_claims();
    claims["iat"] = json!(now);
    claims["exp"] = json!(now + TOKEN_LIFETIME_SECS);
    claims[issuer.config().principal_claim.as_str()] = json!("dev@example.test");
    claims
}

/// A token signed by a key the issuer never publishes.
fn unpublished_key_token(claims: &Value) -> String {
    let key = generate_es256_key("unpublished").expect("generating a signing key");
    key.sign(claims).expect("token signs")
}

fn is_invalid_proof<T>(result: &Result<T, PortError>) -> bool {
    matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    )
}

/// Verifies a freshly minted token at the clock's current reading.
async fn verify_now(
    provider: &Provider,
    issuer: &TestIssuer<'_>,
    clock: &TestClock,
) -> Result<Principal, PortError> {
    let token = issuer.mint(&long_lived_claims(issuer, clock));
    provider.verify(&Proof::new(token)).await
}

fn is_unavailable<T>(result: &Result<T, PortError>) -> bool {
    matches!(result, Err(PortError::Unavailable(_)))
}

#[tokio::test]
async fn token_signed_with_a_rotated_key_verifies_after_one_refetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let before_rotation = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(before_rotation.clone()))
        .await
        .expect("token of the initial key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    issuer.rotate();
    let after_rotation = issuer.mint(&long_lived_claims(&issuer, &clock));
    clock.advance(Duration::from_secs(31));

    let principal = provider
        .verify(&Proof::new(after_rotation))
        .await
        .expect("token of the rotated key verifies");
    assert_eq!(principal.as_str(), "dev@example.test");
    assert_eq!(issuer.jwks_fetches(), 2);

    provider
        .verify(&Proof::new(before_rotation))
        .await
        .expect("token of the previous key still verifies from cache");
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn unknown_key_ids_within_the_miss_interval_cause_one_refetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let known = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(known))
        .await
        .expect("token of the published key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    let claims = long_lived_claims(&issuer, &clock);
    let first = provider
        .verify(&Proof::new(unpublished_key_token(&claims)))
        .await;
    assert!(is_invalid_proof(&first));
    assert_eq!(issuer.jwks_fetches(), 2);

    let second = provider
        .verify(&Proof::new(unpublished_key_token(&claims)))
        .await;
    assert!(is_invalid_proof(&second));
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn retired_key_is_rejected_once_the_cache_passes_max_age() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let retired = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(retired.clone()))
        .await
        .expect("token of the initial key verifies");

    issuer.rotate();
    issuer.retire_older_keys();
    clock.advance(Duration::from_secs(3601));

    let rejected = provider.verify(&Proof::new(retired)).await;
    assert!(is_invalid_proof(&rejected));

    let current = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(current))
        .await
        .expect("token of the current key verifies after the reload");
}

#[tokio::test]
async fn retired_key_verifies_from_cache_before_max_age() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let retired = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(retired.clone()))
        .await
        .expect("token of the initial key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    issuer.rotate();
    issuer.retire_older_keys();
    clock.advance(Duration::from_mins(1));

    provider
        .verify(&Proof::new(retired))
        .await
        .expect("cached key still verifies before the max age");
    assert_eq!(issuer.jwks_fetches(), 1);
}

#[tokio::test]
async fn unknown_key_refetch_waits_for_the_configured_refresh_interval() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.cache.key_refresh_interval = NonZeroDuration::from_mins(10);
    });

    let known = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(known))
        .await
        .expect("token of the initial key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    // The first miss refetches, and the key is still unknown.
    let claims = long_lived_claims(&issuer, &clock);
    let miss = provider
        .verify(&Proof::new(unpublished_key_token(&claims)))
        .await;
    assert!(is_invalid_proof(&miss));
    assert_eq!(issuer.jwks_fetches(), 2);

    issuer.rotate();
    let rotated = issuer.mint(&long_lived_claims(&issuer, &clock));
    clock.advance(Duration::from_secs(599));
    let too_soon = provider.verify(&Proof::new(rotated.clone())).await;
    assert!(is_invalid_proof(&too_soon));
    assert_eq!(issuer.jwks_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    provider
        .verify(&Proof::new(rotated))
        .await
        .expect("rotated key verifies once the refresh interval has elapsed");
    assert_eq!(issuer.jwks_fetches(), 3);
}

#[tokio::test]
async fn cached_key_set_expires_at_the_configured_ttl() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_with(&issuer, &clock, |config| {
        config.cache.key_ttl = NonZeroDuration::from_mins(10);
    });

    let token = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(token.clone()))
        .await
        .expect("token verifies with the first fetch");
    assert_eq!(issuer.jwks_fetches(), 1);

    clock.advance(Duration::from_secs(599));
    provider
        .verify(&Proof::new(token.clone()))
        .await
        .expect("token verifies from the cache just before the ttl");
    assert_eq!(issuer.jwks_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    provider
        .verify(&Proof::new(token))
        .await
        .expect("token verifies after the refetch at the ttl");
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn stale_key_set_keeps_verifying_through_a_failing_refetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    assert_eq!(issuer.jwks_fetches(), 1);

    issuer.set_jwks_failing(true);
    clock.advance(Duration::from_secs(KEY_TTL_SECS));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale key set is used when the refetch fails");
    assert_eq!(issuer.jwks_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale key set is used while the retry is gated");
    assert_eq!(issuer.jwks_fetches(), 2);

    clock.advance(Duration::from_secs(KEY_REFRESH_INTERVAL_SECS));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale key set is used when the next refetch fails");
    assert_eq!(issuer.jwks_fetches(), 3);
}

#[tokio::test]
async fn failed_unknown_key_refetch_keeps_the_cached_keys() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    let known = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(known.clone()))
        .await
        .expect("token of the initial key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    issuer.rotate();
    let rotated = issuer.mint(&long_lived_claims(&issuer, &clock));
    issuer.set_jwks_failing(true);
    let miss = provider.verify(&Proof::new(rotated)).await;
    assert!(is_unavailable(&miss));
    assert_eq!(issuer.jwks_fetches(), 2);

    provider
        .verify(&Proof::new(known))
        .await
        .expect("token of the cached key still verifies after the failed refetch");
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn rotated_key_during_a_jwks_outage_is_unavailable_on_every_attempt() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token of the initial key verifies");
    issuer.rotate();
    let rotated = issuer.mint(&long_lived_claims(&issuer, &clock));
    issuer.set_jwks_failing(true);

    let first = provider.verify(&Proof::new(rotated.clone())).await;
    assert!(is_unavailable(&first));
    assert_eq!(issuer.jwks_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    let second = provider.verify(&Proof::new(rotated)).await;
    assert!(is_unavailable(&second));
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn held_key_refetch_does_not_block_verification_with_a_cached_key() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = Arc::new(provider_for(&issuer, &clock));

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    clock.advance(Duration::from_secs(KEY_TTL_SECS));

    issuer.set_jwks_held(true);
    let held_token = issuer.mint(&long_lived_claims(&issuer, &clock));
    let refresher = tokio::spawn({
        let provider = Arc::clone(&provider);
        async move { provider.verify(&Proof::new(held_token)).await }
    });
    issuer.wait_for_held_jwks().await;
    assert_eq!(issuer.jwks_fetches(), 2);

    let stale = ready_now(verify_now(&provider, &issuer, &clock))
        .expect("a verification is not blocked behind the held refetch");
    stale.expect("token verifies from the cached key set while the refetch is held");
    assert!(
        !refresher.is_finished(),
        "the held refetch has not answered yet"
    );
    assert_eq!(issuer.jwks_fetches(), 2);

    issuer.set_jwks_held(false);
    issuer.release_jwks();
    refresher
        .await
        .expect("the held verification task does not panic")
        .expect("the held verification completes once the refetch answers");
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn unknown_key_refetch_in_flight_does_not_block_known_key_verification() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = Arc::new(provider_for(&issuer, &clock));

    let known = issuer.mint(&long_lived_claims(&issuer, &clock));
    provider
        .verify(&Proof::new(known.clone()))
        .await
        .expect("token of the initial key verifies");
    assert_eq!(issuer.jwks_fetches(), 1);

    issuer.rotate();
    let rotated = issuer.mint(&long_lived_claims(&issuer, &clock));
    issuer.set_jwks_held(true);
    let unknown_refetch = tokio::spawn({
        let provider = Arc::clone(&provider);
        async move { provider.verify(&Proof::new(rotated)).await }
    });
    issuer.wait_for_held_jwks().await;
    assert_eq!(issuer.jwks_fetches(), 2);

    let known_proof = Proof::new(known);
    let known_result = ready_now(provider.verify(&known_proof))
        .expect("a known key is not blocked behind the held refetch");
    known_result.expect("token of the cached key verifies while the refetch is held");
    assert!(
        !unknown_refetch.is_finished(),
        "the held refetch has not answered yet"
    );

    issuer.set_jwks_held(false);
    issuer.release_jwks();
    unknown_refetch
        .await
        .expect("the unknown-key verification task does not panic")
        .expect("token of the rotated key verifies once the refetch answers");
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn concurrent_verifications_without_cached_keys_share_one_fetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = Arc::new(provider_for(&issuer, &clock));

    // A failed first fetch caches the discovery document but no keys, so the callers below meet
    // in the key cache only.
    issuer.set_jwks_failing(true);
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    clock.advance(Duration::from_secs(KEY_REFRESH_INTERVAL_SECS));
    issuer.set_jwks_failing(false);
    issuer.set_jwks_held(true);

    let mut verifications = Vec::new();
    let mut parked = Vec::new();
    for _ in 0..4 {
        let proof = Proof::new(issuer.mint(&long_lived_claims(&issuer, &clock)));
        let provider = Arc::clone(&provider);
        let (armed, parks) = oneshot::channel();
        parked.push(parks);
        verifications.push(tokio::spawn(async move {
            park_then_signal(provider.verify(&proof), armed).await
        }));
    }
    // One caller waits on the held fetch, every other one on the fetch in flight.
    for parks in parked {
        parks
            .await
            .expect("every verification waits in the key cache");
    }
    issuer.wait_for_held_jwks().await;

    issuer.set_jwks_held(false);
    issuer.release_jwks();
    for verification in verifications {
        verification
            .await
            .expect("the verification task does not panic")
            .expect("the verification completes with the single fetched key set");
    }
    assert_eq!(issuer.jwks_fetches(), 2);
}

#[tokio::test]
async fn failing_issuer_without_cached_keys_is_asked_once_per_refresh_interval() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    issuer.set_jwks_failing(true);

    let first = verify_now(&provider, &issuer, &clock).await;
    assert!(is_unavailable(&first));
    assert_eq!(issuer.jwks_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    clock.advance(Duration::from_secs(KEY_REFRESH_INTERVAL_SECS - 2));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    assert_eq!(issuer.jwks_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    assert_eq!(issuer.jwks_fetches(), 2);
}
