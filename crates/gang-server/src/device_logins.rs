//! Short-lived, database-backed device authorizations and their one-time grants.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{Clock, Timestamp};
use gang_spi::{
    ExpiringRecordPort, Insert, LoginClient, LoginGrant, PortError, RecordId, RecordKey,
    RecordKind, RecordVersion, Remove, Replace,
};
use gang_types::{DeviceSecret, Principal, Proof, ProviderName, RefreshToken, Url};
use gang_wire::{
    DeviceAuthorization, DeviceAuthorizationState, USER_CODE_ALPHABET, USER_CODE_LENGTH,
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

use crate::records;
use crate::renewal_flights::RenewalFlights;
use crate::renewal_seal::RenewalSeal;
use crate::settings::DeviceSettings;

/// Largest multiple of the alphabet length within the number of possible byte values.
const UNBIASED_BYTE_BOUND: usize =
    (1usize << u8::BITS) / USER_CODE_ALPHABET.len() * USER_CODE_ALPHABET.len();

/// A normalized device user code.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UserCode([u8; USER_CODE_LENGTH]);

/// A pending device authorization identified by its code and original device record.
#[derive(Debug, Clone)]
pub(crate) struct ApprovalTarget {
    pub(crate) code: UserCode,
    pub(crate) device_digest: [u8; 32],
}

impl UserCode {
    /// Draws each character uniformly, rejecting the incomplete final byte range.
    fn generate(random: &SystemRandom) -> Result<Self, DeviceError> {
        let mut code = [0; USER_CODE_LENGTH];
        let mut filled = 0;
        while filled < code.len() {
            let mut bytes = [0; USER_CODE_LENGTH];
            random
                .fill(&mut bytes)
                .map_err(|_| DeviceError::Randomness)?;
            for byte in bytes {
                if usize::from(byte) < UNBIASED_BYTE_BOUND {
                    code[filled] =
                        USER_CODE_ALPHABET.as_bytes()[usize::from(byte) % USER_CODE_ALPHABET.len()];
                    filled += 1;
                    if filled == code.len() {
                        break;
                    }
                }
            }
        }
        Ok(Self(code))
    }
}

/// An invalid device user code.
#[derive(Debug, thiserror::Error)]
#[error("invalid device user code")]
pub(crate) struct InvalidUserCode;

impl FromStr for UserCode {
    type Err = InvalidUserCode;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut code = [0; USER_CODE_LENGTH];
        let mut count = 0;
        for byte in input.bytes().filter(|byte| !matches!(byte, b'-' | b' ')) {
            let byte = byte.to_ascii_uppercase();
            if count == code.len() || !USER_CODE_ALPHABET.as_bytes().contains(&byte) {
                return Err(InvalidUserCode);
            }
            code[count] = byte;
            count += 1;
        }
        if count != code.len() {
            return Err(InvalidUserCode);
        }
        Ok(Self(code))
    }
}

impl fmt::Display for UserCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, byte) in self.0.iter().enumerate() {
            if index == USER_CODE_LENGTH / 2 {
                f.write_str("-")?;
            }
            write!(f, "{}", char::from(*byte))?;
        }
        Ok(())
    }
}

/// Failure to obtain cryptographically secure randomness.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeviceError {
    /// The operating system's random source failed.
    #[error("device login randomness unavailable")]
    Randomness,
    /// The record store failed.
    #[error(transparent)]
    Storage(#[from] PortError),
}

/// The provider grant and verified principal accepted by the browser.
#[derive(Debug)]
pub(crate) struct Approval {
    /// The normalized authorization code.
    pub(crate) code: UserCode,
    /// The provider's login grant.
    pub(crate) grant: LoginGrant,
}

/// Persisted device state. Credential-bearing approval data is encrypted.
#[derive(Serialize, Deserialize)]
struct Pending {
    client_address: Option<IpAddr>,
    create_time: u64,
    last_poll: Option<u64>,
    approval: Option<String>,
    provider: ProviderName,
    verification_uri: Url,
}

#[derive(Serialize, Deserialize)]
struct UserRecord {
    device_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct GrantBody {
    proof: String,
    refresh_token: Option<String>,
    principal: String,
}

/// The outcome of a redemption that did not return an approval.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Redeem {
    /// The authorization or secret is invalid, expired, or already consumed.
    NotFound,
    /// The authorization is still awaiting approval.
    Pending,
    /// The previous poll was too recent.
    TooSoon(Duration),
}

/// Device login configuration shared by the API and browser routes.
pub(crate) struct DeviceLogins<C: Clock> {
    /// Lifetime of each authorization.
    ttl: Duration,
    /// Minimum interval between polls.
    poll_interval: Duration,
    /// Source of wall time.
    clock: Arc<C>,
}

/// The device-login state and provider shared by the mounted routes.
pub(crate) struct MountedDevice<C: Clock, V> {
    /// Pending and approved device authorizations.
    pub(crate) logins: DeviceLogins<C>,
    /// The dashboard sign-in provider.
    pub(crate) provider: ProviderName,
    /// The provider's confidential web client.
    pub(crate) client: LoginClient,
    /// The interactive provider's verifier.
    pub(crate) verifier: Arc<V>,
    /// Authenticated encryption shared with browser renewals.
    pub(crate) seal: Arc<RenewalSeal>,
    /// Refresh grants shared with browser renewals.
    pub(crate) flights: Arc<RenewalFlights>,
    /// The dashboard page where a code is entered.
    pub(crate) verification_uri: Url,
}

impl<C: Clock> DeviceLogins<C> {
    /// Creates the device service with the configured lifetime and poll interval.
    pub(crate) fn new(settings: &DeviceSettings, clock: Arc<C>) -> Self {
        Self {
            ttl: settings.ttl.get(),
            poll_interval: settings.poll_interval.get(),
            clock,
        }
    }

    /// Creates both indexes atomically and returns the secret only this once.
    pub(crate) async fn create<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        client: Option<IpAddr>,
        verification_uri: &Url,
        provider: &ProviderName,
    ) -> Result<DeviceAuthorization, DeviceError> {
        let random = SystemRandom::new();
        loop {
            let mut secret_bytes = [0; 32];
            random
                .fill(&mut secret_bytes)
                .map_err(|_| DeviceError::Randomness)?;
            let secret = DeviceSecret::new(URL_SAFE_NO_PAD.encode(secret_bytes));
            let device_key = records::key(RecordKind::DeviceCode, secret.expose().as_bytes());
            let code = UserCode::generate(&random)?;
            let user_key = records::key(RecordKind::UserCode, &code.0);
            let now = self.clock.wall();
            let expires_at = now + self.ttl;
            let pending = Pending {
                client_address: client,
                create_time: now.as_unix_millis(),
                last_poll: None,
                approval: None,
                provider: provider.clone(),
                verification_uri: verification_uri.clone(),
            };
            let user = UserRecord {
                device_digest: *device_key.id.as_bytes(),
            };
            if records::insert_pair(
                repository,
                (device_key, &pending),
                (user_key, &user),
                now,
                expires_at,
            )
            .await?
                == Insert::Inserted
            {
                tracing::info!(code = %code, client = ?client, "device login requested");
                return Ok(self.resource(&code, &pending, expires_at, Some(secret)));
            }
        }
    }

    /// Resolves a normalized user code to the device record.
    async fn lookup<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        code: &UserCode,
        now: Timestamp,
    ) -> Result<Option<(RecordKey, records::Stored<Pending>, RecordVersion)>, PortError> {
        let user_key = records::key(RecordKind::UserCode, &code.0);
        let Some(user) = records::get::<_, UserRecord>(repository, &user_key, now).await? else {
            return Ok(None);
        };
        let key = RecordKey {
            kind: RecordKind::DeviceCode,
            id: RecordId::new(user.value.device_digest),
        };
        Ok(records::get(repository, &key, now)
            .await?
            .map(|value| (key, value, user.version)))
    }

    /// Returns a live authorization without its secret.
    pub(crate) async fn get<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        code: &str,
    ) -> Result<Option<DeviceAuthorization>, PortError> {
        let Ok(code) = code.parse::<UserCode>() else {
            return Ok(None);
        };
        Ok(self
            .lookup(repository, &code, self.clock.wall())
            .await?
            .map(|(_, stored, _)| self.resource(&code, &stored.value, stored.expires_at, None)))
    }

    /// Redeems an approved grant once, or reports the pending poll state.
    pub(crate) async fn redeem<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        seal: &RenewalSeal,
        code: &str,
        secret: &DeviceSecret,
    ) -> Result<Result<Approval, Redeem>, PortError> {
        let Ok(code) = code.parse::<UserCode>() else {
            return Ok(Err(Redeem::NotFound));
        };
        let expected = records::key(RecordKind::DeviceCode, secret.expose().as_bytes());
        for _ in 0..2 {
            let now = self.clock.wall();
            let Some((key, mut stored, user_version)) = self.lookup(repository, &code, now).await?
            else {
                return Ok(Err(Redeem::NotFound));
            };
            if std::hint::black_box(
                key.id
                    .as_bytes()
                    .iter()
                    .zip(expected.id.as_bytes())
                    .fold(0, |acc, (x, y)| acc | (x ^ y)),
            ) != 0
            {
                return Ok(Err(Redeem::NotFound));
            }
            if let Some(last) = stored.value.last_poll {
                let elapsed = now.saturating_duration_since(Timestamp::from_unix_millis(last));
                if elapsed < self.poll_interval {
                    return Ok(Err(Redeem::TooSoon(
                        self.poll_interval.saturating_sub(elapsed),
                    )));
                }
            }
            if let Some(encrypted) = &stored.value.approval {
                let plaintext = seal
                    .open_record(&key, encrypted)
                    .map_err(PortError::backend)?;
                let grant: GrantBody =
                    serde_json::from_slice(&plaintext).map_err(PortError::backend)?;
                let principal = Principal::try_new(grant.principal).map_err(PortError::backend)?;
                if records::remove(repository, &key, stored.version, now).await? == Remove::Removed
                {
                    let user_key = records::key(RecordKind::UserCode, &code.0);
                    if let Err(error) =
                        records::remove(repository, &user_key, user_version, now).await
                    {
                        tracing::warn!(%error, "cannot remove redeemed user code");
                    }
                    return Ok(Ok(Approval {
                        code,
                        grant: LoginGrant {
                            proof: Proof::new(grant.proof),
                            refresh_token: grant.refresh_token.map(RefreshToken::new),
                            principal,
                        },
                    }));
                }
            } else {
                stored.value.last_poll = Some(now.as_unix_millis());
                if matches!(
                    records::replace(
                        repository,
                        &key,
                        stored.version,
                        &stored.value,
                        now,
                        stored.expires_at
                    )
                    .await?,
                    Replace::Replaced(_)
                ) {
                    return Ok(Err(Redeem::Pending));
                }
            }
        }
        Err(contention())
    }

    /// Begins an approval only for a live, still-pending code.
    pub(crate) async fn begin_approval<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        code: &str,
    ) -> Result<Option<ApprovalTarget>, PortError> {
        let Ok(code) = code.parse::<UserCode>() else {
            return Ok(None);
        };
        Ok(self
            .lookup(repository, &code, self.clock.wall())
            .await?
            .and_then(|(key, stored, _)| {
                stored.value.approval.is_none().then_some(ApprovalTarget {
                    code,
                    device_digest: *key.id.as_bytes(),
                })
            }))
    }

    /// Accepts the first approval of a live code.
    pub(crate) async fn approve<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        seal: &RenewalSeal,
        target: &ApprovalTarget,
        approval: Approval,
    ) -> Result<bool, PortError> {
        let body = GrantBody {
            proof: approval.grant.proof.expose().to_owned(),
            refresh_token: approval
                .grant
                .refresh_token
                .as_ref()
                .map(|token| token.expose().to_owned()),
            principal: approval.grant.principal.as_str().to_owned(),
        };
        for _ in 0..2 {
            let now = self.clock.wall();
            let Some((key, mut stored, _)) = self.lookup(repository, &target.code, now).await?
            else {
                return Ok(false);
            };
            if key.id.as_bytes() != &target.device_digest || stored.value.approval.is_some() {
                return Ok(false);
            }
            let encrypted = seal
                .seal_record(
                    &key,
                    &serde_json::to_vec(&body).map_err(PortError::backend)?,
                )
                .map_err(PortError::backend)?;
            stored.value.approval = Some(encrypted);
            if matches!(
                records::replace(
                    repository,
                    &key,
                    stored.version,
                    &stored.value,
                    now,
                    stored.expires_at
                )
                .await?,
                Replace::Replaced(_)
            ) {
                return Ok(true);
            }
        }
        Err(contention())
    }

    fn resource(
        &self,
        code: &UserCode,
        pending: &Pending,
        expires_at: Timestamp,
        secret: Option<DeviceSecret>,
    ) -> DeviceAuthorization {
        DeviceAuthorization {
            name: format!("deviceAuthorizations/{code}"),
            provider: pending.provider.clone(),
            verification_uri: pending.verification_uri.clone(),
            client_address: pending.client_address.map(|address| address.to_string()),
            state: if pending.approval.is_some() {
                DeviceAuthorizationState::Approved
            } else {
                DeviceAuthorizationState::Pending
            },
            create_time: Timestamp::from_unix_millis(pending.create_time),
            expire_time: expires_at,
            poll_interval: self.poll_interval,
            secret,
        }
    }
}

/// A repeated optimistic-write conflict is retryable by the caller.
fn contention() -> PortError {
    PortError::Unavailable(Box::new(std::io::Error::other(
        "device login write contention",
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::TestClock;
    use gang_testkit::{FakeRepository, TestResult};
    use gang_types::{ClientSecret, DeviceCodeTtl, DevicePollInterval};

    fn table(clock: Arc<TestClock>) -> DeviceLogins<TestClock> {
        DeviceLogins::new(
            &DeviceSettings {
                ttl: DeviceCodeTtl::from_units(60),
                poll_interval: DevicePollInterval::from_units(5),
            },
            clock,
        )
    }
    fn seal() -> RenewalSeal {
        RenewalSeal::new(&ClientSecret::new("secret"), "https://example.org")
    }
    async fn create(
        table: &DeviceLogins<TestClock>,
        repository: &FakeRepository,
    ) -> Result<DeviceAuthorization, DeviceError> {
        table
            .create(
                repository,
                Some("192.0.2.1".parse().expect("valid address")),
                &Url::parse("https://example.org/ui/device").expect("valid url"),
                &ProviderName::try_new("example").expect("valid provider"),
            )
            .await
    }
    fn code(resource: &DeviceAuthorization) -> &str {
        resource
            .name
            .strip_prefix("deviceAuthorizations/")
            .expect("resource name")
    }
    fn approval(code: &UserCode, principal: &str) -> Approval {
        Approval {
            code: code.clone(),
            grant: LoginGrant {
                proof: Proof::new(principal),
                refresh_token: Some(RefreshToken::new("refresh")),
                principal: Principal::try_new(principal).expect("valid principal"),
            },
        }
    }
    #[test]
    fn user_codes_use_the_alphabet_and_normalize_input() -> TestResult {
        let random = SystemRandom::new();
        for _ in 0..256 {
            let code = UserCode::generate(&random)?;
            let text = code.to_string();
            assert_eq!(text.len(), USER_CODE_LENGTH + 1);
            assert_eq!(text.parse::<UserCode>()?, code);
            assert!(
                text.bytes()
                    .filter(|byte| *byte != b'-')
                    .all(|byte| USER_CODE_ALPHABET.as_bytes().contains(&byte))
            );
        }
        assert_eq!("bcdf-ghjk".parse::<UserCode>()?, " BCDF GHJK ".parse()?);
        assert!("BCDF-GHJ".parse::<UserCode>().is_err());
        assert!("BCDF-GHJA".parse::<UserCode>().is_err());
        Ok(())
    }
    #[tokio::test]
    async fn shared_records_preserve_poll_approval_and_single_redemption() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let a = table(Arc::clone(&clock));
        let b = table(Arc::clone(&clock));
        let repository = FakeRepository::default();
        let resource = create(&a, &repository).await?;
        let secret = resource.secret.as_ref().expect("secret");
        assert_eq!(URL_SAFE_NO_PAD.decode(secret.expose())?.len(), 32);
        assert!(matches!(
            b.redeem(&repository, &seal(), code(&resource), secret)
                .await?,
            Err(Redeem::Pending)
        ));
        assert!(
            matches!(b.redeem(&repository, &seal(), code(&resource), secret).await?, Err(Redeem::TooSoon(d)) if d == Duration::from_secs(5))
        );
        let user_code = b
            .begin_approval(&repository, code(&resource))
            .await?
            .expect("pending");
        assert!(
            b.approve(
                &repository,
                &seal(),
                &user_code,
                approval(&user_code.code, "alice")
            )
            .await?
        );
        assert!(
            !a.approve(
                &repository,
                &seal(),
                &user_code,
                approval(&user_code.code, "bob")
            )
            .await?
        );
        assert!(
            b.get(&repository, code(&resource))
                .await?
                .expect("live")
                .secret
                .is_none()
        );
        clock.advance(Duration::from_secs(5));
        let first = b
            .redeem(&repository, &seal(), code(&resource), secret)
            .await?
            .expect("approved");
        assert_eq!(first.grant.principal.as_str(), "alice");
        assert_eq!(first.grant.proof.expose(), "alice");
        let user_key = records::key(RecordKind::UserCode, &user_code.code.0);
        assert!(repository.get(&user_key, clock.wall()).await?.is_none());
        assert!(matches!(
            b.redeem(&repository, &seal(), code(&resource), secret)
                .await?,
            Err(Redeem::NotFound)
        ));
        Ok(())
    }
    #[tokio::test]
    async fn concurrent_redemptions_have_one_winner() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let a = table(Arc::clone(&clock));
        let b = table(clock);
        let repository = FakeRepository::default();
        let resource = create(&a, &repository).await?;
        let user_code = a
            .begin_approval(&repository, code(&resource))
            .await?
            .expect("pending");
        assert!(
            a.approve(
                &repository,
                &seal(),
                &user_code,
                approval(&user_code.code, "alice")
            )
            .await?
        );
        let secret = resource.secret.as_ref().expect("secret");
        let first_seal = seal();
        let second_seal = seal();
        let (first, second) = tokio::join!(
            a.redeem(&repository, &first_seal, code(&resource), secret),
            b.redeem(&repository, &second_seal, code(&resource), secret),
        );
        let first = first?;
        let second = second?;
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!(matches!(first, Err(Redeem::NotFound)) || matches!(second, Err(Redeem::NotFound)));
        Ok(())
    }
    #[tokio::test]
    async fn approval_cannot_follow_a_reused_user_code() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let table = table(Arc::clone(&clock));
        let repository = FakeRepository::default();
        let first = create(&table, &repository).await?;
        let saved_code = table
            .begin_approval(&repository, code(&first))
            .await?
            .expect("pending");
        clock.advance(Duration::from_mins(1));

        let second = create(&table, &repository).await?;
        let secret = second.secret.as_ref().expect("secret");
        let device_key = records::key(RecordKind::DeviceCode, secret.expose().as_bytes());
        let user_key = records::key(RecordKind::UserCode, &saved_code.code.0);
        let now = clock.wall();
        // The newly generated code could itself equal the saved code.
        if let Some(existing) = records::get::<_, UserRecord>(&repository, &user_key, now).await? {
            assert!(matches!(
                records::remove(&repository, &user_key, existing.version, now).await?,
                Remove::Removed
            ));
        }
        assert!(matches!(
            records::insert(
                &repository,
                user_key,
                &UserRecord {
                    device_digest: *device_key.id.as_bytes(),
                },
                RecordVersion::fresh().map_err(PortError::backend)?,
                now,
                now + Duration::from_mins(1),
            )
            .await?,
            Insert::Inserted
        ));

        assert!(
            !table
                .approve(
                    &repository,
                    &seal(),
                    &saved_code,
                    approval(&saved_code.code, "alice")
                )
                .await?
        );
        assert!(matches!(
            table
                .redeem(&repository, &seal(), code(&second), secret)
                .await?,
            Err(Redeem::Pending)
        ));
        Ok(())
    }
    #[tokio::test]
    async fn expiration_wrong_secret_and_capacity() -> TestResult {
        let clock = Arc::new(TestClock::default());
        let table = table(Arc::clone(&clock));
        let repository = FakeRepository::default();
        let first = create(&table, &repository).await?;
        let second = create(&table, &repository).await?;
        assert!(table.get(&repository, code(&first)).await?.is_some());
        assert!(table.get(&repository, code(&second)).await?.is_some());
        assert!(matches!(
            table
                .redeem(
                    &repository,
                    &seal(),
                    code(&first),
                    &DeviceSecret::new("wrong")
                )
                .await?,
            Err(Redeem::NotFound)
        ));
        let user_code = table
            .begin_approval(&repository, code(&first))
            .await?
            .expect("pending");
        clock.advance(Duration::from_mins(1));
        assert!(table.get(&repository, code(&first)).await?.is_none());
        assert!(
            !table
                .approve(
                    &repository,
                    &seal(),
                    &user_code,
                    approval(&user_code.code, "alice")
                )
                .await?
        );
        Ok(())
    }
}
