//! Bounded, short-lived device authorizations and their one-time grants.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::{LoginClient, LoginGrant};
use gang_types::{DeviceSecret, ProviderName, Url};
use gang_wire::{DeviceAuthorization, DeviceAuthorizationState};
use parking_lot::Mutex;
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};

use crate::settings::DeviceSettings;
use crate::ttl_map::TtlMap;

/// The alphabet used for device user codes.
const ALPHABET: &[u8; 20] = b"BCDFGHJKLMNPQRSTVWXZ";

/// A normalized eight-character device user code.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UserCode([u8; 8]);

impl UserCode {
    /// Draws each character uniformly, rejecting the incomplete final byte range.
    fn generate(random: &SystemRandom) -> Result<Self, DeviceError> {
        let mut code = [0; 8];
        let mut filled = 0;
        while filled < code.len() {
            let mut bytes = [0; 8];
            random
                .fill(&mut bytes)
                .map_err(|_| DeviceError::Randomness)?;
            for byte in bytes {
                if byte < 240 {
                    code[filled] = ALPHABET[usize::from(byte % 20)];
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
        let mut code = [0; 8];
        let mut count = 0;
        for byte in input.bytes().filter(|byte| !matches!(byte, b'-' | b' ')) {
            let byte = byte.to_ascii_uppercase();
            if count == code.len() || !ALPHABET.contains(&byte) {
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
            if index == 4 {
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
}

/// The provider grant and verified principal accepted by the browser.
#[derive(Debug)]
pub(crate) struct Approval {
    /// The normalized authorization code.
    pub(crate) code: UserCode,
    /// The provider's login grant.
    pub(crate) grant: LoginGrant,
    /// The verified principal.
    pub(crate) principal: String,
}

/// A pending or approved device authorization.
struct Pending {
    /// The SHA-256 of the secret, never the secret itself.
    secret_digest: [u8; 32],
    /// The creator's observed address.
    client_address: Option<IpAddr>,
    /// The wall-clock creation time.
    create_time: Timestamp,
    /// The wall-clock expiry time sent to clients.
    expire_time: Timestamp,
    /// The last accepted pending poll, without changing insertion order.
    last_poll: Option<Tick>,
    /// The first accepted approval, without changing insertion order.
    approval: Option<Approval>,
    /// The provider displayed to the approver.
    provider: ProviderName,
    /// The approval page displayed to the requester.
    verification_uri: Url,
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

/// A bounded table of device logins, shared by the API and browser routes.
pub(crate) struct DeviceLogins<C: Clock> {
    /// Entries expire and are evicted in creation order.
    entries: Mutex<TtlMap<UserCode, Pending>>,
    /// Lifetime of each authorization.
    ttl: Duration,
    /// Minimum interval between polls.
    poll_interval: Duration,
    /// Source of monotonic and wall time.
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
    /// The dashboard page where a code is entered.
    pub(crate) verification_uri: Url,
}

impl<C: Clock, V> MountedDevice<C, V> {
    /// Creates the shared state for a mounted device login.
    pub(crate) fn new(
        settings: &DeviceSettings,
        provider: ProviderName,
        client: LoginClient,
        verifier: Arc<V>,
        verification_uri: Url,
        clock: Arc<C>,
    ) -> Self {
        Self {
            logins: DeviceLogins::new(settings, clock),
            provider,
            client,
            verifier,
            verification_uri,
        }
    }
}

impl<C: Clock> DeviceLogins<C> {
    /// Creates an empty table with the configured capacity, lifetime, and poll interval.
    pub(crate) fn new(settings: &DeviceSettings, clock: Arc<C>) -> Self {
        let ttl = settings.ttl.get();
        Self {
            entries: Mutex::new(TtlMap::new(settings.max_pending, ttl)),
            ttl,
            poll_interval: settings.poll_interval.get(),
            clock,
        }
    }

    /// Creates a device authorization and returns its secret only this once.
    pub(crate) fn create(
        &self,
        client: Option<IpAddr>,
        verification_uri: &Url,
        provider: &ProviderName,
    ) -> Result<DeviceAuthorization, DeviceError> {
        let random = SystemRandom::new();
        let mut secret_bytes = [0; 32];
        random
            .fill(&mut secret_bytes)
            .map_err(|_| DeviceError::Randomness)?;
        let secret = DeviceSecret::new(URL_SAFE_NO_PAD.encode(secret_bytes));
        let secret_digest = secret_digest(&secret);
        let mut entries = self.entries.lock();
        let now = self.clock.monotonic();
        let code = loop {
            let code = UserCode::generate(&random)?;
            if entries.get_live(&code, now).is_none() {
                break code;
            }
        };
        let create_time = self.clock.wall();
        let pending = Pending {
            secret_digest,
            client_address: client,
            create_time,
            expire_time: create_time + self.ttl,
            last_poll: None,
            approval: None,
            provider: provider.clone(),
            verification_uri: verification_uri.clone(),
        };
        let resource = self.resource(&code, &pending, Some(secret));
        entries.insert(code.clone(), pending, now);
        tracing::info!(code = %code, client = ?client, "device login requested");
        Ok(resource)
    }

    /// Returns a live authorization without its secret.
    pub(crate) fn get(&self, code: &str) -> Option<DeviceAuthorization> {
        let code = code.parse::<UserCode>().ok()?;
        let entries = self.entries.lock();
        let pending = entries.get_live(&code, self.clock.monotonic())?;
        Some(self.resource(&code, pending, None))
    }

    /// Redeems an approved grant once, or reports the pending poll state.
    pub(crate) fn redeem(&self, code: &str, secret: &DeviceSecret) -> Result<Approval, Redeem> {
        let code = code.parse::<UserCode>().map_err(|_| Redeem::NotFound)?;
        let mut entries = self.entries.lock();
        let now = self.clock.monotonic();
        let pending = entries.get_live_mut(&code, now).ok_or(Redeem::NotFound)?;
        let actual = secret_digest(secret);
        if std::hint::black_box(
            pending
                .secret_digest
                .iter()
                .zip(&actual)
                .fold(0, |acc, (x, y)| acc | (x ^ y)),
        ) != 0
        {
            return Err(Redeem::NotFound);
        }
        if let Some(last) = pending.last_poll {
            let elapsed = now.saturating_duration_since(last);
            if elapsed < self.poll_interval {
                return Err(Redeem::TooSoon(self.poll_interval.saturating_sub(elapsed)));
            }
        }
        if pending.approval.is_none() {
            pending.last_poll = Some(now);
            return Err(Redeem::Pending);
        }
        entries
            .take_live(&code, now)
            .and_then(|entry| entry.approval)
            .ok_or(Redeem::NotFound)
    }

    /// Begins an approval only for a live, still-pending code.
    pub(crate) fn begin_approval(&self, code: &str) -> Option<UserCode> {
        let code = code.parse::<UserCode>().ok()?;
        let entries = self.entries.lock();
        let pending = entries.get_live(&code, self.clock.monotonic())?;
        if pending.approval.is_some() {
            return None;
        }
        Some(code)
    }

    /// Accepts the first approval of a live code.
    pub(crate) fn approve(&self, code: &UserCode, mut approval: Approval) -> bool {
        let mut entries = self.entries.lock();
        let Some(pending) = entries.get_live_mut(code, self.clock.monotonic()) else {
            return false;
        };
        if pending.approval.is_some() {
            return false;
        }
        approval.code = code.clone();
        pending.approval = Some(approval);
        true
    }

    /// Constructs the public view without changing the table or its eviction order.
    fn resource(
        &self,
        code: &UserCode,
        pending: &Pending,
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
            create_time: pending.create_time,
            expire_time: pending.expire_time,
            poll_interval: self.poll_interval,
            secret,
        }
    }
}

/// Hashes the exposed random secret; entries never retain the secret itself.
fn secret_digest(secret: &DeviceSecret) -> [u8; 32] {
    let digest = digest(&SHA256, secret.expose().as_bytes());
    let mut bytes = [0; 32];
    bytes.copy_from_slice(digest.as_ref());
    bytes
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use gang_clock::TestClock;
    use gang_types::{DeviceCodeTtl, DevicePollInterval, Proof, RefreshToken};

    use super::*;

    /// Creates a table with deterministic time.
    fn table(capacity: usize) -> (DeviceLogins<TestClock>, Arc<TestClock>) {
        let clock = Arc::new(TestClock::default());
        let settings = DeviceSettings {
            ttl: DeviceCodeTtl::from_units(60),
            poll_interval: DevicePollInterval::from_units(5),
            max_pending: NonZeroUsize::new(capacity).expect("nonzero capacity"),
        };
        (DeviceLogins::new(&settings, Arc::clone(&clock)), clock)
    }

    /// Creates an authorization for the test provider.
    fn create(table: &DeviceLogins<TestClock>) -> DeviceAuthorization {
        table
            .create(
                Some("192.0.2.1".parse().expect("valid ip address")),
                &Url::parse("https://example.org/ui/device").expect("valid url"),
                &ProviderName::try_new("example").expect("valid provider"),
            )
            .expect("randomness available")
    }

    /// Extracts the code from a created resource name.
    fn code(resource: &DeviceAuthorization) -> &str {
        resource
            .name
            .strip_prefix("deviceAuthorizations/")
            .expect("resource name")
    }

    /// Makes a grant that identifies its approver.
    fn approval(code: &UserCode, principal: &str) -> Approval {
        Approval {
            code: code.clone(),
            grant: LoginGrant {
                proof: Proof::new(principal),
                refresh_token: Some(RefreshToken::new("refresh")),
            },
            principal: principal.to_owned(),
        }
    }

    #[test]
    fn user_codes_use_the_alphabet_and_format() {
        let random = SystemRandom::new();
        for _ in 0..256 {
            let code = UserCode::generate(&random).expect("randomness available");
            let text = code.to_string();
            assert_eq!(text.len(), 9);
            assert_eq!(text.as_bytes()[4], b'-');
            assert!(
                text.bytes()
                    .filter(|byte| *byte != b'-')
                    .all(|byte| ALPHABET.contains(&byte))
            );
        }
    }

    #[test]
    fn user_code_input_ignores_case_hyphens_and_spaces() {
        let expected: UserCode = "BCDF-GHJK".parse().expect("valid code");
        assert_eq!(
            "bcdf-ghjk".parse::<UserCode>().expect("valid code"),
            expected
        );
        assert_eq!(
            " BCDF GHJK ".parse::<UserCode>().expect("valid code"),
            expected
        );
        assert!("BCDF-GHJ".parse::<UserCode>().is_err());
        assert!("BCDF-GHJA".parse::<UserCode>().is_err());
    }

    #[test]
    fn a_secret_has_256_bits() {
        let (table, _) = table(2);
        let resource = create(&table);
        let secret = resource.secret.expect("create returns secret");
        assert_eq!(secret.expose().len(), 43);
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(secret.expose())
                .expect("base64url")
                .len(),
            32
        );
    }

    #[test]
    fn redeem_before_approval_is_pending_then_too_soon() {
        let (table, clock) = table(2);
        let resource = create(&table);
        let secret = resource.secret.as_ref().expect("secret");
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::Pending)
        ));
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::TooSoon(d)) if d == Duration::from_secs(5)
        ));
        clock.advance(Duration::from_secs(5));
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::Pending)
        ));
    }

    #[test]
    fn approval_then_redeem_returns_the_grant_once() {
        let (table, _) = table(2);
        let resource = create(&table);
        let user_code = table.begin_approval(code(&resource)).expect("pending");
        assert!(table.approve(&user_code, approval(&user_code, "alice")));
        assert!(table.get(code(&resource)).expect("live").secret.is_none());
        let secret = resource.secret.as_ref().expect("secret");
        let result = table.redeem(code(&resource), secret).expect("approved");
        assert_eq!(result.code, user_code);
        assert_eq!(result.principal, "alice");
        assert_eq!(result.grant.proof.expose(), "alice");
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::NotFound)
        ));
    }

    #[test]
    fn unknown_expired_and_wrong_secret_are_not_found() {
        let (table, clock) = table(2);
        let resource = create(&table);
        let secret = resource.secret.as_ref().expect("secret");
        assert!(matches!(
            table.redeem("invalid", secret),
            Err(Redeem::NotFound)
        ));
        let unknown = if code(&resource) == "BCDF-GHJK" {
            "BCDF-GHJL"
        } else {
            "BCDF-GHJK"
        };
        assert!(matches!(
            table.redeem(unknown, secret),
            Err(Redeem::NotFound)
        ));
        assert!(matches!(
            table.redeem(code(&resource), &DeviceSecret::new("wrong")),
            Err(Redeem::NotFound)
        ));
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::Pending)
        ));
        clock.advance(Duration::from_secs(60));
        assert!(matches!(
            table.redeem(code(&resource), secret),
            Err(Redeem::NotFound)
        ));
        assert!(table.get(code(&resource)).is_none());
    }

    #[test]
    fn the_first_approval_wins() {
        let (table, _) = table(2);
        let resource = create(&table);
        let user_code = table.begin_approval(code(&resource)).expect("pending");
        assert!(table.approve(&user_code, approval(&user_code, "alice")));
        assert!(!table.approve(&user_code, approval(&user_code, "bob")));
        assert!(table.begin_approval(code(&resource)).is_none());
        assert_eq!(
            table
                .redeem(code(&resource), resource.secret.as_ref().expect("secret"))
                .expect("approved")
                .principal,
            "alice"
        );
    }

    #[test]
    fn an_expired_code_cannot_be_approved() {
        let (table, clock) = table(2);
        let resource = create(&table);
        let user_code = table.begin_approval(code(&resource)).expect("pending");
        clock.advance(Duration::from_secs(60));
        assert!(table.begin_approval(code(&resource)).is_none());
        assert!(!table.approve(&user_code, approval(&user_code, "alice")));
    }

    #[test]
    fn a_full_table_evicts_the_oldest() {
        let (table, clock) = table(2);
        let first = create(&table);
        clock.advance(Duration::from_secs(1));
        let second = create(&table);
        assert!(table.get(code(&first)).is_some());
        clock.advance(Duration::from_secs(1));
        let third = create(&table);
        assert!(table.get(code(&first)).is_none());
        assert!(table.get(code(&second)).is_some());
        assert!(table.get(code(&third)).is_some());
    }
}
