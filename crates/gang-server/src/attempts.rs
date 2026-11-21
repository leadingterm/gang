//! Browser sign-ins awaiting their provider's callback, shared across controllers.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::{ExpiringRecordPort, Insert, PortError, RecordKind, RecordVersion, Remove};
use gang_types::LoginSession;
use serde::{Deserialize, Serialize};

use crate::device_logins::ApprovalTarget;
use crate::records;
use crate::renewal_seal::{Renewal, RenewalSeal, Sealed};

/// A browser sign-in that has started and awaits its callback.
pub(crate) struct Attempt {
    /// The plugin's state for the sign-in.
    pub(crate) session: LoginSession,
    /// The operation completed by the callback.
    pub(crate) purpose: Purpose,
}

/// The operation a browser sign-in completes.
pub(crate) enum Purpose {
    /// A dashboard session, optionally rechecking an existing renewal.
    Session {
        /// The path the browser is sent to after a successful sign-in.
        next: http::HeaderValue,
        /// The sealed login to recheck, or `None` for an offline sign-in.
        renewal: Option<Renewal>,
    },
    /// Approval of a pending device login.
    Device {
        /// The original pending device authorization.
        target: ApprovalTarget,
    },
}

#[derive(Serialize, Deserialize)]
struct Body {
    session: String,
    purpose: StoredPurpose,
}

#[derive(Serialize, Deserialize)]
enum StoredPurpose {
    Session {
        next: String,
        renewal: Option<Sealed>,
    },
    Device {
        code: String,
        device_digest: [u8; 32],
    },
}

/// The lifetime of pending sign-ins.
pub(crate) struct LoginAttempts {
    ttl: Duration,
}

impl LoginAttempts {
    /// Creates a store with the configured lifetime.
    pub(crate) fn new(ttl: Duration) -> Self {
        Self { ttl }
    }

    /// Stores an attempt under the digest of its state.
    pub(crate) async fn insert<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        seal: &RenewalSeal,
        state: &str,
        attempt: Attempt,
        now: Timestamp,
    ) -> Result<(), PortError> {
        let purpose = match attempt.purpose {
            Purpose::Session { next, renewal } => StoredPurpose::Session {
                next: next.to_str().map_err(PortError::backend)?.to_owned(),
                renewal: renewal.as_ref().map(Sealed::from),
            },
            Purpose::Device { target } => StoredPurpose::Device {
                code: target.code.to_string(),
                device_digest: target.device_digest,
            },
        };
        let body = Body {
            session: attempt.session.expose().to_owned(),
            purpose,
        };
        let plaintext = serde_json::to_vec(&body).map_err(PortError::backend)?;
        let key = records::key(RecordKind::LoginAttempt, state.as_bytes());
        let sealed = seal
            .seal_record(&key, &plaintext)
            .map_err(PortError::backend)?;
        match records::insert(
            repository,
            key,
            &sealed,
            RecordVersion::fresh().map_err(PortError::backend)?,
            now,
            now + self.ttl,
        )
        .await?
        {
            Insert::Inserted => Ok(()),
            Insert::Exists => Err(PortError::backend("duplicate login state")),
        }
    }

    /// Removes and returns a live attempt, exactly once.
    pub(crate) async fn take<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        seal: &RenewalSeal,
        state: &str,
        now: Timestamp,
    ) -> Result<Option<Attempt>, PortError> {
        let key = records::key(RecordKind::LoginAttempt, state.as_bytes());
        let Some(record) = records::get::<_, String>(repository, &key, now).await? else {
            return Ok(None);
        };
        let plaintext = seal
            .open_record(&key, &record.value)
            .map_err(PortError::backend)?;
        let body: Body = serde_json::from_slice(&plaintext).map_err(PortError::backend)?;
        let purpose = match body.purpose {
            StoredPurpose::Session { next, renewal } => Purpose::Session {
                next: http::HeaderValue::from_str(&next).map_err(PortError::backend)?,
                renewal: renewal
                    .map(Renewal::try_from)
                    .transpose()
                    .map_err(PortError::backend)?,
            },
            StoredPurpose::Device {
                code,
                device_digest,
            } => Purpose::Device {
                target: ApprovalTarget {
                    code: code.parse().map_err(PortError::backend)?,
                    device_digest,
                },
            },
        };
        if records::remove(repository, &key, record.version, now).await? == Remove::Stale {
            return Ok(None);
        }
        Ok(Some(Attempt {
            session: LoginSession::new(body.session),
            purpose,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_testkit::{FakeRepository, TestResult};
    use gang_types::{ClientSecret, LoginSession};

    use super::{Attempt, LoginAttempts, Purpose};
    use crate::renewal_seal::RenewalSeal;

    #[tokio::test]
    async fn large_renewal_is_stored_without_cookie_size_inflation() -> TestResult {
        use crate::renewal_seal::Renewal;
        use gang_types::{ProviderName, RefreshToken};

        let repository = FakeRepository::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let attempts = LoginAttempts::new(Duration::from_secs(10));
        let now = Timestamp::from_unix_millis(1_000);
        let renewal = Renewal {
            provider: ProviderName::try_new("provider")?,
            principal: "person".to_owned(),
            refresh_token: RefreshToken::new("r".repeat(2_900)),
            expire_time: now + Duration::from_mins(1),
        };
        attempts
            .insert(
                &repository,
                &seal,
                "state",
                Attempt {
                    session: LoginSession::new("pkce"),
                    purpose: Purpose::Session {
                        next: http::HeaderValue::from_static("/ui"),
                        renewal: Some(renewal.clone()),
                    },
                },
                now,
            )
            .await?;
        let attempt = attempts
            .take(&repository, &seal, "state", now)
            .await?
            .ok_or("missing attempt")?;
        match attempt.purpose {
            Purpose::Session {
                renewal: stored, ..
            } => assert_eq!(stored, Some(renewal)),
            Purpose::Device { .. } => panic!("wrong purpose"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn another_controller_completes_the_attempt_once() -> TestResult {
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
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let attempts = LoginAttempts::new(Duration::from_secs(10));
        let now = Timestamp::from_unix_millis(1_000);
        attempts
            .insert(
                &first.core.host.repository,
                &seal,
                "state",
                Attempt {
                    session: LoginSession::new("pkce"),
                    purpose: Purpose::Session {
                        next: http::HeaderValue::from_static("/ui"),
                        renewal: None,
                    },
                },
                now,
            )
            .await?;
        let attempt = attempts
            .take(&second.host.repository, &seal, "state", now)
            .await?
            .expect("shared attempt");
        assert_eq!(attempt.session.expose(), "pkce");
        assert!(
            attempts
                .take(&first.core.host.repository, &seal, "state", now)
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_attempt_cannot_be_taken() -> TestResult {
        let repository = FakeRepository::default();
        let seal = RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org");
        let ttl = Duration::from_secs(10);
        let attempts = LoginAttempts::new(ttl);
        let now = Timestamp::from_unix_millis(1_000);
        attempts
            .insert(
                &repository,
                &seal,
                "state",
                Attempt {
                    session: LoginSession::new("pkce"),
                    purpose: Purpose::Session {
                        next: http::HeaderValue::from_static("/ui"),
                        renewal: None,
                    },
                },
                now,
            )
            .await?;
        assert!(
            attempts
                .take(&repository, &seal, "state", now + ttl)
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_record_store_refuses_login_with_503() -> TestResult {
        let harness = crate::test_support_session_harness::Harness::token(
            crate::test_support_session_harness::HarnessOptions::default(),
        );
        harness.core.host.repository.fail_records(true);
        let request = crate::test_support_session_harness::request("GET", "/ui/login", &[]);
        let response = harness.send(request).await?;
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }
}
