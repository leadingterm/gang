//! Cross-controller single-flight sharing of browser refresh grants.

use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{
    ExpiringRecordPort, Insert, LoginGrant, PortError, ProofVerifierPort, RecordKind, RecordVersion,
};
use gang_types::{Proof, RefreshToken};
use serde::{Deserialize, Serialize};

use crate::records;
use crate::renewal_seal::RenewalSeal;

/// Interval between reads while another controller is renewing a token.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Bounds how long a crashed controller can leave a flight in progress.
const IN_FLIGHT_TTL: Duration = Duration::from_secs(15);

/// Why a shared refresh grant failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RenewalFailure {
    /// The provider refused the refresh token.
    Rejected,
    /// The provider could not be reached or failed.
    Unavailable,
    /// The shared record store failed.
    StorageUnavailable,
}

#[derive(Serialize, Deserialize)]
enum Flight {
    InFlight,
    Finished(Result<String, RenewalFailure>),
}

#[derive(Serialize, Deserialize)]
struct Grant {
    proof: String,
    refresh_token: Option<String>,
}

/// The reuse window for shared refresh grants.
pub(crate) struct RenewalFlights {
    reuse: Duration,
}

impl RenewalFlights {
    /// Creates a flight store with the given reuse window.
    pub(crate) fn new(reuse: Duration) -> Self {
        Self { reuse }
    }

    /// Joins an existing flight or performs the one grant for this token.
    pub(crate) async fn renew<R, C, V, F>(
        &self,
        repository: &R,
        clock: &C,
        seal: &RenewalSeal,
        token: &RefreshToken,
        verifier: &V,
        grant: impl FnOnce() -> F,
    ) -> Result<LoginGrant, RenewalFailure>
    where
        R: ExpiringRecordPort,
        C: Clock,
        V: ProofVerifierPort + ?Sized,
        F: Future<Output = Result<LoginGrant, PortError>>,
    {
        let key = records::key(RecordKind::RenewalFlight, token.expose().as_bytes());
        let mut retried = false;
        loop {
            let now = clock.wall();
            let deadline = clock.monotonic() + IN_FLIGHT_TTL;
            let expires_at = now + IN_FLIGHT_TTL;
            let version = RecordVersion::fresh().map_err(|_| RenewalFailure::StorageUnavailable)?;
            match records::insert(repository, key, &Flight::InFlight, version, now, expires_at)
                .await
            {
                Ok(Insert::Inserted) => {
                    let remaining = deadline.saturating_duration_since(clock.monotonic());
                    let outcome = if remaining.is_zero() {
                        None
                    } else {
                        clock.timeout(remaining, grant()).await.ok()
                    };
                    let now = clock.wall();
                    let Some(outcome) = outcome else {
                        records::remove(repository, &key, version, now)
                            .await
                            .map_err(|_| RenewalFailure::StorageUnavailable)?;
                        return Err(RenewalFailure::Unavailable);
                    };
                    let grant = match outcome {
                        Ok(grant) => grant,
                        Err(PortError::Rejected { .. }) => {
                            if let Err(error) = records::replace(
                                repository,
                                &key,
                                version,
                                &Flight::Finished(Err(RenewalFailure::Rejected)),
                                now,
                                now + self.reuse,
                            )
                            .await
                            {
                                tracing::warn!(%error, "cannot store rejected renewal flight");
                            }
                            return Err(RenewalFailure::Rejected);
                        }
                        Err(_) => {
                            records::remove(repository, &key, version, now)
                                .await
                                .map_err(|_| RenewalFailure::StorageUnavailable)?;
                            return Err(RenewalFailure::Unavailable);
                        }
                    };
                    let value = Grant {
                        proof: grant.proof.expose().to_owned(),
                        refresh_token: grant
                            .refresh_token
                            .as_ref()
                            .map(|value| value.expose().to_owned()),
                    };
                    let sealed = serde_json::to_vec(&value)
                        .ok()
                        .and_then(|encoded| seal.seal_record(&key, &encoded).ok());
                    if let Some(sealed) = sealed {
                        if let Err(error) = records::replace(
                            repository,
                            &key,
                            version,
                            &Flight::Finished(Ok(sealed)),
                            now,
                            now + self.reuse,
                        )
                        .await
                        {
                            tracing::warn!(%error, "cannot store completed renewal flight");
                        }
                    } else if let Err(error) = records::remove(repository, &key, version, now).await
                    {
                        tracing::warn!(%error, "cannot remove renewal flight after sealing failure");
                    }
                    return Ok(grant);
                }
                Ok(Insert::Exists) => loop {
                    let now = clock.wall();
                    let record = records::get::<_, Flight>(repository, &key, now)
                        .await
                        .map_err(|_| RenewalFailure::StorageUnavailable)?;
                    match record.map(|record| record.value) {
                        Some(Flight::Finished(result)) => {
                            return Self::open(seal, &key, result, verifier).await;
                        }
                        Some(Flight::InFlight) => clock.sleep(POLL_INTERVAL).await,
                        None if !retried => {
                            retried = true;
                            break;
                        }
                        None => return Err(RenewalFailure::Unavailable),
                    }
                },
                Err(_) => return Err(RenewalFailure::StorageUnavailable),
            }
        }
    }

    async fn open<V: ProofVerifierPort + ?Sized>(
        seal: &RenewalSeal,
        key: &gang_spi::RecordKey,
        result: Result<String, RenewalFailure>,
        verifier: &V,
    ) -> Result<LoginGrant, RenewalFailure> {
        let sealed = result?;
        let plaintext = seal
            .open_record(key, &sealed)
            .map_err(|_| RenewalFailure::StorageUnavailable)?;
        let grant: Grant =
            serde_json::from_slice(&plaintext).map_err(|_| RenewalFailure::StorageUnavailable)?;
        let proof = Proof::new(grant.proof);
        let principal = verifier.verify(&proof).await.map_err(|error| match error {
            PortError::Rejected { .. } => RenewalFailure::Rejected,
            _ => RenewalFailure::Unavailable,
        })?;
        Ok(LoginGrant {
            proof,
            refresh_token: grant.refresh_token.map(RefreshToken::new),
            principal,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_clock::TestClock;
    use gang_spi::{LoginGrant, LoginSupport, PortError, Rejection};
    use gang_testkit::{FakeRepository, TestResult};
    use gang_types::{ClientSecret, Principal, Proof, RefreshToken};
    use tokio::sync::Notify;

    use super::{IN_FLIGHT_TTL, POLL_INTERVAL, RenewalFailure, RenewalFlights};
    use crate::renewal_seal::RenewalSeal;
    use crate::test_support_verifier::FakeVerifier;

    fn verifier() -> FakeVerifier {
        let verifier = FakeVerifier::new(LoginSupport::Interactive);
        for proof in [
            "retry",
            "cached",
            "unexpected",
            "proof",
            "old",
            "new",
            "third",
            "retried",
            "first",
            "other",
        ] {
            verifier.accept(proof, "user@example.org");
        }
        verifier
    }

    fn principal() -> Principal {
        Principal::try_new("user@example.org").expect("fixture principal is valid")
    }

    #[tokio::test]
    async fn hung_grant_expires_and_allows_a_new_flight() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let token = RefreshToken::new("hung");
        let mut first =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || {
                    std::future::pending()
                }),
            );
        assert!(futures::poll!(first.as_mut()).is_pending());
        clock.advance(IN_FLIGHT_TTL);
        assert!(matches!(first.await, Err(RenewalFailure::Unavailable)));
        let grant = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Ok(LoginGrant {
                    proof: Proof::new("retry"),
                    principal: principal(),
                    refresh_token: None,
                })
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(grant.proof.expose(), "retry");
        Ok(())
    }

    #[tokio::test]
    async fn large_clock_skew_preserves_rejections_and_cached_grants() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let rejected = RefreshToken::new("rejected");
        let outcome = flights
            .renew(&repository, &clock, &seal, &rejected, &verifier, || async {
                Err(PortError::refused(Rejection::InvalidProof))
            })
            .await;
        assert!(matches!(outcome, Err(RenewalFailure::Rejected)));

        let token = RefreshToken::new("valid");
        let grant = || async {
            Ok(LoginGrant {
                proof: Proof::new("cached"),
                principal: principal(),
                refresh_token: None,
            })
        };
        flights
            .renew(&repository, &clock, &seal, &token, &verifier, grant)
            .await
            .map_err(|error| format!("{error:?}"))?;
        verifier.accept("cached", "reverified@example.org");
        let joined = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Ok(LoginGrant {
                    proof: Proof::new("unexpected"),
                    principal: principal(),
                    refresh_token: None,
                })
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(
            joined.principal,
            Principal::try_new("reverified@example.org")?
        );
        assert_eq!(joined.proof.expose(), "cached");
        Ok(())
    }

    async fn cached_verification_failure(expected: RenewalFailure) -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Ok(LoginGrant {
                    proof: Proof::new("cached"),
                    principal: principal(),
                    refresh_token: Some(RefreshToken::new("rotated")),
                })
            })
            .await
            .map_err(|error| format!("{error:?}"))?;

        match expected {
            RenewalFailure::Rejected => verifier.reject("cached", Rejection::InvalidProof),
            RenewalFailure::Unavailable => verifier.unavailable("cached"),
            RenewalFailure::StorageUnavailable => panic!("not a verifier failure"),
        }
        let result = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                panic!("cached grant must not rotate the refresh token again")
            })
            .await;
        assert!(matches!(result, Err(error) if error == expected));
        Ok(())
    }

    #[tokio::test]
    async fn cached_grant_is_rejected_when_proof_is_no_longer_valid() -> TestResult {
        cached_verification_failure(RenewalFailure::Rejected).await
    }

    #[tokio::test]
    async fn cached_grant_reports_verifier_unavailable() -> TestResult {
        cached_verification_failure(RenewalFailure::Unavailable).await
    }

    #[tokio::test]
    async fn rotated_grant_survives_a_stale_replace() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_secs(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let result = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                clock.advance(Duration::from_mins(1));
                Ok(LoginGrant {
                    proof: Proof::new("proof"),
                    principal: principal(),
                    refresh_token: Some(RefreshToken::new("rotated")),
                })
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert!(
            result
                .refresh_token
                .ok_or("missing rotated token")?
                .matches("rotated")
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_owner_does_not_overwrite_a_reinserted_flight() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let first_gate = Notify::new();
        let second_gate = Notify::new();
        let mut first =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    first_gate.notified().await;
                    Ok(LoginGrant {
                        proof: Proof::new("old"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(futures::poll!(first.as_mut()).is_pending());
        clock.advance(Duration::from_secs(15));
        let mut second =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    second_gate.notified().await;
                    Ok(LoginGrant {
                        proof: Proof::new("new"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(futures::poll!(second.as_mut()).is_pending());
        first_gate.notify_one();
        assert_eq!(
            first
                .await
                .map_err(|error| format!("{error:?}"))?
                .proof
                .expose(),
            "old"
        );
        let mut third =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    Ok(LoginGrant {
                        proof: Proof::new("third"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(
            futures::poll!(third.as_mut()).is_pending(),
            "old flight overwrote the new flight"
        );
        second_gate.notify_one();
        assert_eq!(
            second
                .await
                .map_err(|error| format!("{error:?}"))?
                .proof
                .expose(),
            "new"
        );
        clock.advance(POLL_INTERVAL);
        assert_eq!(
            third
                .await
                .map_err(|error| format!("{error:?}"))?
                .proof
                .expose(),
            "new"
        );
        Ok(())
    }

    #[tokio::test]
    async fn transient_failure_is_not_reused() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let first = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Err(PortError::unavailable("provider unavailable"))
            })
            .await;
        assert!(matches!(first, Err(RenewalFailure::Unavailable)));
        let second = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Ok(LoginGrant {
                    proof: Proof::new("retried"),
                    principal: principal(),
                    refresh_token: None,
                })
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(second.proof.expose(), "retried");
        Ok(())
    }

    #[tokio::test]
    async fn follower_retries_after_abandoned_flight_expires() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_mins(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let release = Notify::new();
        let mut first =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    release.notified().await;
                    Ok(LoginGrant {
                        proof: Proof::new("first"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(futures::poll!(first.as_mut()).is_pending());
        let mut second =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    Ok(LoginGrant {
                        proof: Proof::new("retried"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(first);
        clock.advance(Duration::from_secs(15));
        let result = match futures::poll!(second.as_mut()) {
            std::task::Poll::Ready(result) => result.map_err(|error| format!("{error:?}"))?,
            std::task::Poll::Pending => panic!("abandoned flight still blocks the follower"),
        };
        assert_eq!(result.proof.expose(), "retried");
        Ok(())
    }

    #[tokio::test]
    async fn two_controllers_share_one_renewal() -> TestResult {
        let first = crate::test_support_session_harness::Harness::token(
            crate::test_support_session_harness::HarnessOptions::default(),
        );
        let host = &first.core.host;
        let second = crate::core::Core::<crate::test_support_deployment::TlsDeployment>::new(
            crate::host::Host {
                proxy_protocol: host.proxy_protocol.clone(),
                repository: host.repository.clone(),
                signer: std::sync::Arc::clone(&host.signer),
                security: crate::security::Security::Plaintext,
                providers: crate::test_support_deployment::providers().0,
                clock: std::sync::Arc::clone(&host.clock),
                endpoint: crate::test_support_deployment::endpoint("https://gang.example.test"),
                revoked_principals: crate::principals::RevokedPrincipals::new(),
                sightings: crate::principals::PrincipalSightings::new(
                    Duration::from_mins(1),
                    std::num::NonZeroUsize::new(10).expect("ten is nonzero"),
                ),
            },
            crate::test_support_deployment::token(),
            crate::test_support_deployment::mtls(),
        );
        let clock = &first.clock;
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_secs(10));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let calls = AtomicUsize::new(0);
        let release = Notify::new();
        let a = flights.renew(
            &first.core.host.repository,
            clock.as_ref(),
            &seal,
            &token,
            &verifier,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(LoginGrant {
                    proof: Proof::new("proof"),
                    principal: principal(),
                    refresh_token: None,
                })
            },
        );
        let b = flights.renew(
            &second.host.repository,
            clock.as_ref(),
            &seal,
            &token,
            &verifier,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(LoginGrant {
                    proof: Proof::new("other"),
                    principal: principal(),
                    refresh_token: None,
                })
            },
        );
        let (a, b, ()) = tokio::join!(a, b, async {
            tokio::task::yield_now().await;
            release.notify_one();
            tokio::task::yield_now().await;
            clock.advance(POLL_INTERVAL);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            a.map_err(|error| format!("{error:?}"))?.proof.expose(),
            "proof"
        );
        assert_eq!(
            b.map_err(|error| format!("{error:?}"))?.proof.expose(),
            "proof"
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_flight_returns_first_grant_and_follower_retries() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_secs(1));
        let verifier = verifier();
        let token = RefreshToken::new("token");
        let release = Notify::new();
        let mut first =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    release.notified().await;
                    Ok(LoginGrant {
                        proof: Proof::new("proof"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        let mut second =
            Box::pin(
                flights.renew(&repository, &clock, &seal, &token, &verifier, || async {
                    Ok(LoginGrant {
                        proof: Proof::new("retried"),
                        principal: principal(),
                        refresh_token: None,
                    })
                }),
            );
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        clock.advance(IN_FLIGHT_TTL);
        release.notify_one();
        let first = first.await.map_err(|error| format!("{error:?}"))?;
        assert_eq!(first.proof.expose(), "proof");
        let second = second.await.map_err(|error| format!("{error:?}"))?;
        assert_eq!(second.proof.expose(), "retried");
        Ok(())
    }

    #[tokio::test]
    async fn outcomes_are_reused_until_expiry_and_tokens_are_independent() -> TestResult {
        let repository = FakeRepository::default();
        let clock = TestClock::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let flights = RenewalFlights::new(Duration::from_secs(10));
        let verifier = verifier();
        let calls = AtomicUsize::new(0);
        let token = RefreshToken::new("first");
        for expected in [1, 1, 2] {
            if expected == 2 {
                clock.advance(Duration::from_secs(10));
            }
            let result = flights
                .renew(&repository, &clock, &seal, &token, &verifier, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(LoginGrant {
                        proof: Proof::new("proof"),
                        principal: principal(),
                        refresh_token: None,
                    })
                })
                .await;
            assert!(result.is_ok());
            assert_eq!(calls.load(Ordering::SeqCst), expected);
        }
        let rejected = RefreshToken::new("rejected");
        for _ in 0..2 {
            let result = flights
                .renew(&repository, &clock, &seal, &rejected, &verifier, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(PortError::refused(Rejection::InvalidProof))
                })
                .await;
            assert!(matches!(result, Err(RenewalFailure::Rejected)));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        repository.fail_records(true);
        let result = flights
            .renew(&repository, &clock, &seal, &token, &verifier, || async {
                Ok(LoginGrant {
                    proof: Proof::new("unexpected"),
                    principal: principal(),
                    refresh_token: None,
                })
            })
            .await;
        assert!(matches!(result, Err(RenewalFailure::StorageUnavailable)));
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_record_store_refuses_renewal_with_503() -> TestResult {
        use crate::renewal_seal::Renewal;
        use crate::test_support_session_harness::{Harness, request_with_header};
        use gang_clock::Clock;
        use gang_types::ProviderName;

        let harness =
            Harness::token(crate::test_support_session_harness::HarnessOptions::default());
        let seal = RenewalSeal::new(
            &ClientSecret::new("dashboard-secret-value"),
            "https://gang.example.test",
        );
        let cookie = seal.seal(&Renewal {
            provider: ProviderName::try_new("example-idp")?,
            principal: "admin".to_owned(),
            refresh_token: RefreshToken::new("refresh"),
            expire_time: harness.clock.wall() + Duration::from_mins(1),
        })?;
        harness.core.host.repository.fail_records(true);
        let request = request_with_header(
            "POST",
            "/ui/renew",
            &[(harness.core.cookies.renewal, &cookie)],
            "origin",
            "https://gang.example.test",
        );
        let response = harness.send(request).await?;
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }
}
