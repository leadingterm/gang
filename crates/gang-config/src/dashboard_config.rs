//! The `[dashboard]` section: settings of the operator dashboard that `gang server` serves.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{
    AbsolutePathBuf, ClientSecret, DeviceCodeTtl, DevicePollInterval, NonZeroDuration,
    ProviderName, SessionMaxAge,
};
use serde::{Deserialize, Serialize};

use crate::client_secret::ClientSecretSource;

/// The default for `enabled`: the dashboard is served.
const DEFAULT_ENABLED: bool = true;

/// The default most viewers connected at once: 64.
const DEFAULT_MAX_VIEWERS: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// The default `Retry-After` given to a viewer turned away: five seconds.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(5);

/// The default interval between dashboard heartbeats: ten seconds.
const DEFAULT_HEARTBEAT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The default number of frames a viewer may fall behind by: 32.
const DEFAULT_LAG_FRAMES: NonZeroUsize = NonZeroUsize::new(32).expect("32 is non-zero");

/// The default number of recent events kept for a viewer that reconnects: 200.
const DEFAULT_RECENT_EVENTS: NonZeroUsize = NonZeroUsize::new(200).expect("200 is non-zero");

/// The default most running entries listed in a snapshot: 2000.
const DEFAULT_MAX_LISTED_RUNNING: usize = 2000;

/// The default most queued entries listed in a snapshot: 200.
const DEFAULT_MAX_LISTED_QUEUED: usize = 200;

/// The default interval between detail refreshes: ten seconds.
const DEFAULT_DETAIL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The default lifetime of a dashboard login: ten minutes.
const DEFAULT_LOGIN_TTL: NonZeroDuration = NonZeroDuration::from_mins(10);

/// The default number of logins in progress at once: 1024.
const DEFAULT_MAX_PENDING_LOGINS: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is non-zero");

/// The default lifetime of a device authorization: ten minutes.
const DEFAULT_DEVICE_TTL: DeviceCodeTtl = DeviceCodeTtl::from_units(600);

/// The default interval between device polls: five seconds.
const DEFAULT_DEVICE_POLL_INTERVAL: DevicePollInterval = DevicePollInterval::from_units(5);

/// The default number of device authorizations pending at once: 1024.
const DEFAULT_DEVICE_MAX_PENDING: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is non-zero");

/// The default absolute browser session lifetime: twelve hours.
const DEFAULT_SESSION_MAX_AGE: SessionMaxAge = SessionMaxAge::from_units(43_200);

/// The default sharing window for a renewal grant: one minute.
const DEFAULT_RENEWAL_REUSE: NonZeroDuration = NonZeroDuration::from_mins(1);

/// How many `detail_interval`s an action may go unrefreshed before it is stale, when
/// `actions_stale_after` is unset.
const STALE_INTERVAL_MULTIPLE: u32 = 3;

/// The `[dashboard.login]` table: the confidential client the dashboard logs viewers in with.
/// Absent, the dashboard has no login. Exactly one of `client_secret` and `client_secret_file` is
/// set; the file is read once by `gang server`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawDashboardLogin", into = "RawDashboardLogin")]
pub struct DashboardLogin {
    /// The identity provider that authenticates viewers: a key of `[identity.providers]`. The
    /// reserved `none` is not allowed.
    pub provider: ProviderName,
    /// The client identifier the provider issued to the dashboard. Must not be blank.
    pub client_id: String,
    /// The client secret the dashboard presents to the provider: written inline, or kept in a
    /// file. An inline secret must not be blank. Its `Debug` output is redacted.
    pub client_secret: ClientSecretSource,
}

/// The `[dashboard.login]` table as written in `gang.toml`, before its fields are checked.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDashboardLogin {
    provider: ProviderName,
    client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_secret: Option<ClientSecret>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_secret_file: Option<AbsolutePathBuf>,
}

/// Why `[dashboard.login]` has an invalid client identifier or secret source.
#[derive(Debug, thiserror::Error)]
enum LoginSecretError {
    /// Both or neither of `client_secret` and `client_secret_file` are set.
    #[error("set exactly one of client_secret and client_secret_file")]
    NotExactlyOne,
    /// `client_id` holds nothing but whitespace.
    #[error("client_id must not be blank")]
    BlankClientId,
    /// `client_secret` holds nothing but whitespace.
    #[error("client_secret is empty")]
    EmptyInline,
}

impl TryFrom<RawDashboardLogin> for DashboardLogin {
    type Error = LoginSecretError;

    fn try_from(raw: RawDashboardLogin) -> Result<Self, Self::Error> {
        if raw.client_id.trim().is_empty() {
            return Err(LoginSecretError::BlankClientId);
        }
        let client_secret = match (raw.client_secret, raw.client_secret_file) {
            (Some(secret), None) if secret.expose().trim().is_empty() => {
                return Err(LoginSecretError::EmptyInline);
            }
            (Some(secret), None) => ClientSecretSource::Inline(secret),
            (None, Some(path)) => ClientSecretSource::File(path),
            (Some(_), Some(_)) | (None, None) => return Err(LoginSecretError::NotExactlyOne),
        };
        Ok(Self {
            provider: raw.provider,
            client_id: raw.client_id,
            client_secret,
        })
    }
}

impl From<DashboardLogin> for RawDashboardLogin {
    fn from(login: DashboardLogin) -> Self {
        let (client_secret, client_secret_file) = match login.client_secret {
            ClientSecretSource::Inline(secret) => (Some(secret), None),
            ClientSecretSource::File(path) => (None, Some(path)),
        };
        Self {
            provider: login.provider,
            client_id: login.client_id,
            client_secret,
            client_secret_file,
        }
    }
}

/// Settings of device login (ADR-37) under `[dashboard.device]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawDeviceConfig")]
pub struct DeviceConfig {
    /// Mounts device login (ADR-37) when the dashboard sign-in is mounted. Defaults to true.
    pub enabled: bool,
    /// How long a device authorization lasts (ADR-37), from 1m to 30m. Defaults to 10m.
    pub ttl: DeviceCodeTtl,
    /// How often a device may poll (ADR-37), from 1s to 1h and shorter than `ttl`. Defaults to 5s.
    pub poll_interval: DevicePollInterval,
    /// The most device authorizations pending at once (ADR-37). Defaults to 1024.
    pub max_pending: NonZeroUsize,
}

impl Default for DeviceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl: DEFAULT_DEVICE_TTL,
            poll_interval: DEFAULT_DEVICE_POLL_INTERVAL,
            max_pending: DEFAULT_DEVICE_MAX_PENDING,
        }
    }
}

/// The `[dashboard.device]` table before its timing is checked.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawDeviceConfig {
    enabled: bool,
    ttl: DeviceCodeTtl,
    poll_interval: DevicePollInterval,
    max_pending: NonZeroUsize,
}

impl Default for RawDeviceConfig {
    fn default() -> Self {
        let device = DeviceConfig::default();
        Self {
            enabled: device.enabled,
            ttl: device.ttl,
            poll_interval: device.poll_interval,
            max_pending: device.max_pending,
        }
    }
}

impl TryFrom<RawDeviceConfig> for DeviceConfig {
    type Error = &'static str;

    fn try_from(raw: RawDeviceConfig) -> Result<Self, Self::Error> {
        if raw.poll_interval.get() >= raw.ttl.get() {
            return Err("dashboard.device poll_interval must be shorter than ttl");
        }
        Ok(Self {
            enabled: raw.enabled,
            ttl: raw.ttl,
            poll_interval: raw.poll_interval,
            max_pending: raw.max_pending,
        })
    }
}

/// Settings of the operator dashboard. Every field has a default, so the section may be
/// omitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DashboardConfig {
    /// Whether `gang server` serves the dashboard. Defaults to true.
    pub enabled: bool,
    /// The most viewers connected at once; a viewer beyond it is turned away with
    /// `retry_after`. Defaults to 64.
    pub max_viewers: NonZeroUsize,
    /// The retry delay given to a viewer turned away at `max_viewers`, sent as `retryDelay` in the
    /// `closed` event and the SSE `retry:` field. Zero is allowed. Defaults to five seconds.
    #[serde(with = "gang_types::duration")]
    pub retry_after: Duration,
    /// The interval between heartbeats on a viewer's stream. Must be non-zero. Defaults to ten
    /// seconds.
    pub heartbeat: NonZeroDuration,
    /// How many frames a viewer may fall behind before it is resynced with a fresh snapshot.
    /// Defaults to 32.
    pub lag_frames: NonZeroUsize,
    /// How many recent events are kept for a viewer that reconnects. Defaults to 200.
    pub recent_events: NonZeroUsize,
    /// The most running entries listed in a snapshot. Defaults to 2000.
    pub max_listed_running: usize,
    /// The most queued entries listed in a snapshot. Defaults to 200.
    pub max_listed_queued: usize,
    /// The interval between detail refreshes. Must be non-zero. Defaults to ten seconds.
    pub detail_interval: NonZeroDuration,
    /// The age after which an action is shown as stale. Must exceed `detail_interval`. Defaults
    /// to unset, which means three times `detail_interval`; read it through
    /// [`Self::actions_stale_after`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions_stale_after: Option<NonZeroDuration>,
    /// How long a started browser sign-in stays open before its callback arrives. Must be
    /// non-zero. Defaults to ten minutes. The short-lived token lifetime is the token plugin's
    /// `token_ttl`.
    pub login_ttl: NonZeroDuration,
    /// The most logins in progress at once. Defaults to 1024.
    pub max_pending_logins: NonZeroUsize,
    /// The absolute lifetime of a browser session (ADR-39): renewal stops this long after the
    /// sign-in that set the renewal cookie, and the next sign-in asks no consent. From 1m to 30d.
    /// Defaults to 12 hours.
    pub session_max_age: SessionMaxAge,
    /// How long one refresh grant's result is shared by renewals of the same refresh token
    /// (ADR-39), so two tabs never present one token twice. Must be non-zero. Defaults to 1 minute.
    pub renewal_reuse: NonZeroDuration,
    /// Settings of device login (ADR-37).
    pub device: DeviceConfig,
    /// The login the dashboard requires. Defaults to absent, which means the dashboard has no
    /// login.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login: Option<DashboardLogin>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_ENABLED,
            max_viewers: DEFAULT_MAX_VIEWERS,
            retry_after: DEFAULT_RETRY_AFTER,
            heartbeat: DEFAULT_HEARTBEAT,
            lag_frames: DEFAULT_LAG_FRAMES,
            recent_events: DEFAULT_RECENT_EVENTS,
            max_listed_running: DEFAULT_MAX_LISTED_RUNNING,
            max_listed_queued: DEFAULT_MAX_LISTED_QUEUED,
            detail_interval: DEFAULT_DETAIL_INTERVAL,
            actions_stale_after: None,
            login_ttl: DEFAULT_LOGIN_TTL,
            max_pending_logins: DEFAULT_MAX_PENDING_LOGINS,
            session_max_age: DEFAULT_SESSION_MAX_AGE,
            renewal_reuse: DEFAULT_RENEWAL_REUSE,
            device: DeviceConfig::default(),
            login: None,
        }
    }
}

impl DashboardConfig {
    /// The age after which an action is shown as stale: the explicit `actions_stale_after`,
    /// else three times `detail_interval`.
    #[must_use]
    pub fn actions_stale_after(&self) -> Duration {
        match self.actions_stale_after {
            Some(stale_after) => stale_after.get(),
            None => self
                .detail_interval
                .get()
                .saturating_mul(STALE_INTERVAL_MULTIPLE),
        }
    }
}
