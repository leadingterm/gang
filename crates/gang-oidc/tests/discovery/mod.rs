//! Discovery document cache behaviour: ttl expiry, retry gating after failed fetches, and stale
//! documents, driven by the injected clock.

use std::sync::Arc;
use std::time::Duration;

use super::support::TestIssuer;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{PortError, ProofVerifierPort};
use gang_testkit::{park_then_signal, ready_now};
use gang_types::{NonZeroDuration, Principal, Proof};
use serde_json::{Value, json};
use tokio::sync::oneshot;

const START_MILLIS: u64 = 1_800_000_000_000;
const TOKEN_LIFETIME_SECS: u64 = 10 * 3600;
const DISCOVERY_TTL_SECS: u64 = 3600;
const RETRY_INTERVAL_SECS: u64 = 30;

fn new_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(START_MILLIS)))
}

fn provider_for(issuer: &TestIssuer<'_>, clock: &Arc<TestClock>) -> Provider {
    provider_with(issuer, clock, |_| {})
}

/// Builds a provider with explicit discovery timings, adjusted by `adjust`.
fn provider_with(
    issuer: &TestIssuer<'_>,
    clock: &Arc<TestClock>,
    adjust: impl FnOnce(&mut OidcConfig),
) -> Provider {
    let mut config = issuer.config();
    config.cache.discovery_ttl = NonZeroDuration::from_hours(DISCOVERY_TTL_SECS / 3600);
    config.cache.discovery_retry_interval = NonZeroDuration::from_secs(RETRY_INTERVAL_SECS);
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

/// Verifies a freshly minted token at the clock's current reading.
async fn verify_now(
    provider: &Provider,
    issuer: &TestIssuer<'_>,
    clock: &TestClock,
) -> Result<Principal, PortError> {
    let token = issuer.mint(&long_lived_claims(issuer, clock));
    provider.verify(&Proof::new(token)).await
}

fn is_unavailable(result: &Result<Principal, PortError>) -> bool {
    matches!(result, Err(PortError::Unavailable(_)))
}

#[tokio::test]
async fn non_loopback_http_jwks_uri_is_rejected_before_fetching_keys() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    issuer.set_discovery_jwks_uri(Some("http://idp.example.test/jwks".to_owned()));
    let provider = provider_for(&issuer, &clock);

    assert!(matches!(
        verify_now(&provider, &issuer, &clock).await,
        Err(PortError::Backend(_))
    ));
    assert_eq!(issuer.discovery_fetches(), 1);
    assert_eq!(issuer.jwks_fetches(), 0);
}

#[tokio::test]
async fn discovery_is_fetched_once_for_verifications_within_the_ttl() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    clock.advance(Duration::from_mins(20));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies from the cache");
    clock.advance(Duration::from_secs(DISCOVERY_TTL_SECS - 1 - 1200));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies from the cache just before the ttl");

    assert_eq!(issuer.discovery_fetches(), 1);
}

#[tokio::test]
async fn discovery_is_fetched_once_again_when_the_ttl_passes() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    clock.advance(Duration::from_secs(DISCOVERY_TTL_SECS - 1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies from the cache just before the ttl");
    assert_eq!(issuer.discovery_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    for _ in 0..3 {
        verify_now(&provider, &issuer, &clock)
            .await
            .expect("token verifies after the refetch at the ttl");
    }
    assert_eq!(issuer.discovery_fetches(), 2);
}

#[tokio::test]
async fn failing_issuer_without_a_cached_document_is_asked_once_per_retry_interval() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    issuer.set_discovery_failing(true);

    let first = verify_now(&provider, &issuer, &clock).await;
    assert!(is_unavailable(&first));
    assert_eq!(issuer.discovery_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    clock.advance(Duration::from_secs(RETRY_INTERVAL_SECS - 2));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    assert_eq!(issuer.discovery_fetches(), 1);

    clock.advance(Duration::from_secs(1));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    clock.advance(Duration::from_secs(1));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    assert_eq!(issuer.discovery_fetches(), 2);

    issuer.set_discovery_failing(false);
    clock.advance(Duration::from_secs(RETRY_INTERVAL_SECS - 2));
    assert!(is_unavailable(
        &verify_now(&provider, &issuer, &clock).await
    ));
    assert_eq!(issuer.discovery_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies once the healthy issuer is fetched after the interval");
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies from the cache afterwards");
    assert_eq!(issuer.discovery_fetches(), 3);
}

#[tokio::test]
async fn stale_document_keeps_verifying_through_failing_refreshes() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    assert_eq!(issuer.discovery_fetches(), 1);

    issuer.set_discovery_failing(true);
    clock.advance(Duration::from_hours(DISCOVERY_TTL_SECS / 3600));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used when the refresh fails");
    assert_eq!(issuer.discovery_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used while the retry is gated");
    clock.advance(Duration::from_secs(RETRY_INTERVAL_SECS - 2));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used while the retry is gated");
    assert_eq!(issuer.discovery_fetches(), 2);

    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used when the next refresh fails");
    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used while the retry is gated");
    assert_eq!(issuer.discovery_fetches(), 3);

    issuer.set_discovery_failing(false);
    clock.advance(Duration::from_secs(RETRY_INTERVAL_SECS - 2));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("stale document is used until the retry interval passes");
    assert_eq!(issuer.discovery_fetches(), 3);

    clock.advance(Duration::from_secs(1));
    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies after a healthy refresh");
    assert_eq!(issuer.discovery_fetches(), 4);
}

#[tokio::test]
async fn stale_document_serves_verifications_while_a_refresh_is_held() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    // Keys outlive the discovery document, so the stale verification below needs no fetch at all.
    let provider = Arc::new(provider_with(&issuer, &clock, |config| {
        config.cache.key_ttl = NonZeroDuration::from_hours(2);
    }));

    verify_now(&provider, &issuer, &clock)
        .await
        .expect("token verifies with the first fetch");
    clock.advance(Duration::from_secs(DISCOVERY_TTL_SECS));

    issuer.set_discovery_held(true);
    let held_token = issuer.mint(&long_lived_claims(&issuer, &clock));
    let refresher = tokio::spawn({
        let provider = Arc::clone(&provider);
        async move { provider.verify(&Proof::new(held_token)).await }
    });
    issuer.wait_for_held_discovery().await;
    assert_eq!(issuer.discovery_fetches(), 2);

    let stale = ready_now(verify_now(&provider, &issuer, &clock))
        .expect("a verification is not blocked behind the held refresh");
    stale.expect("token verifies from the stale document while the refresh is held");
    assert!(
        !refresher.is_finished(),
        "the held refresh has not answered yet"
    );
    assert_eq!(issuer.discovery_fetches(), 2);

    issuer.set_discovery_held(false);
    issuer.release_discovery();
    refresher
        .await
        .expect("the held verification task does not panic")
        .expect("the held verification completes once the refresh answers");
    assert_eq!(issuer.discovery_fetches(), 2);
}

#[tokio::test]
async fn concurrent_verifications_without_a_cached_document_share_one_fetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = Arc::new(provider_for(&issuer, &clock));

    issuer.set_discovery_held(true);
    let mut verifications = Vec::new();
    let mut parked = Vec::new();
    for _ in 0..2 {
        let proof = Proof::new(issuer.mint(&long_lived_claims(&issuer, &clock)));
        let provider = Arc::clone(&provider);
        let (armed, parks) = oneshot::channel();
        parked.push(parks);
        verifications.push(tokio::spawn(async move {
            park_then_signal(provider.verify(&proof), armed).await
        }));
    }
    // One caller waits on the held fetch, the other on the fetch in flight.
    for parks in parked {
        parks
            .await
            .expect("every verification waits in the discovery cache");
    }
    issuer.wait_for_held_discovery().await;

    issuer.set_discovery_held(false);
    issuer.release_discovery();
    for verification in verifications {
        verification
            .await
            .expect("the verification task does not panic")
            .expect("the verification completes with the single fetched document");
    }
    assert_eq!(issuer.discovery_fetches(), 1);
}

#[tokio::test]
async fn discovery_naming_another_issuer_rejects_tokens_before_any_key_fetch() {
    let clock = new_clock();
    let issuer = TestIssuer::start(&clock).await;
    let provider = provider_for(&issuer, &clock);
    issuer.set_discovery_issuer(Some("https://other.example.test".to_owned()));

    let Err(error) = verify_now(&provider, &issuer, &clock).await else {
        panic!("a discovery document naming another issuer must not verify a token");
    };
    assert!(
        matches!(error, PortError::Backend(_)),
        "an issuer mismatch is a backend failure, got {error}"
    );
    assert!(
        error.to_string().contains("does not match"),
        "the failure names the mismatched issuer, got {error}"
    );
    assert_eq!(issuer.jwks_fetches(), 0);
}
