//! The `[server]` section: settings of `gang server` that are not plugin choices.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{Bytes, NonZeroDuration, Percent, TrustDomain};
use serde::{Deserialize, Serialize};

use crate::ca_pin_setting::CaPinSetting;
use crate::rate_limit_config::RateLimitConfig;
use crate::role_config::RoleConfig;

/// How `gang server` carries its connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerTransport {
    /// Gang terminates TLS itself, with listener certificates signed by its own CA.
    #[default]
    Tls,
    /// Gang serves without TLS, behind a tunnel or proxy that terminates TLS.
    Plaintext,
}

/// How `gang server` carries its connections, with the trust domain its TLS mode requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    /// Gang terminates TLS itself, with listener certificates signed by its own CA.
    Tls {
        /// The SPIFFE trust domain of Gang's CA.
        trust_domain: TrustDomain,
    },
    /// Gang serves without TLS, behind a tunnel or proxy that terminates TLS.
    Plaintext,
}

impl Transport {
    /// The transport's kind, without its trust domain.
    #[must_use]
    pub const fn kind(&self) -> ServerTransport {
        match self {
            Self::Tls { .. } => ServerTransport::Tls,
            Self::Plaintext => ServerTransport::Plaintext,
        }
    }
}

/// The default lifetime of the listener server certificates: 7 days.
const DEFAULT_CERTIFICATE_TTL: NonZeroDuration = NonZeroDuration::from_hours(7 * 24);

/// The default share of `certificate_ttl` after which a listener certificate is reissued: 50
/// percent.
const DEFAULT_REISSUE_AT_PERCENT: Percent = Percent::from_percent(50);

/// The default `notBefore` backdate of an issued certificate: one minute.
const DEFAULT_CERTIFICATE_BACKDATE: Duration = Duration::from_mins(1);

/// The default longest a TLS handshake may take: ten seconds.
const DEFAULT_HANDSHAKE_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The default number of TLS handshakes that may be in flight at once: 256.
const DEFAULT_MAX_PENDING_HANDSHAKES: NonZeroUsize =
    NonZeroUsize::new(256).expect("256 is non-zero");

/// The default pause after a failed `accept` before accepting again: one hundred milliseconds.
const DEFAULT_ACCEPT_ERROR_BACKOFF: NonZeroDuration = NonZeroDuration::from_millis(100);

/// The default interval between records of a principal's last-seen time: one minute.
const DEFAULT_PRINCIPAL_SEEN_INTERVAL: NonZeroDuration = NonZeroDuration::from_mins(1);

/// The default number of principals whose last-seen writes are tracked: 10,000.
const DEFAULT_MAX_TRACKED_PRINCIPALS: NonZeroUsize =
    NonZeroUsize::new(10_000).expect("10,000 is non-zero");

/// The default largest control API request body accepted: 64 KiB.
const DEFAULT_MAX_REQUEST_SIZE: Bytes = Bytes::kib(64);

/// The default time a control API request may take before it is abandoned: 30 seconds.
const DEFAULT_REQUEST_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(30);

/// Settings of `gang server`. The section is required. Every field except the TLS trust
/// domain has a default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawServer", into = "RawServer")]
pub struct ServerConfig {
    /// Whether Gang terminates TLS, and under which trust domain. Defaults to `tls`, which
    /// requires `trust_domain`. `mtls` workers require `tls`.
    pub transport: Transport,
    /// The lifetime of the listener server certificates `gang server` issues to itself and
    /// rotates. Client certificate lifetimes are not set here: each plugin sets its own, such
    /// as the `mtls` worker plugin's `certificate_ttl`. Defaults to 7 days.
    pub certificate_ttl: NonZeroDuration,
    /// The share of `certificate_ttl` after which a listener certificate is reissued, in
    /// percent from 1 to 100. Defaults to 50.
    pub reissue_at_percent: Percent,
    /// How far before issuance an issued certificate's `notBefore` is set (listener and worker
    /// certificates alike), absorbing clock skew between the issuer and verifiers. Defaults to
    /// one minute.
    pub certificate_backdate: Duration,
    /// How clients and workers verify the server's CA: a fixed pin, or `signer` for the pin of
    /// the `[signer]` key's public key, which a worker derives at start. `None` verifies against
    /// the system roots. The server derives its own pin and ignores this field. Defaults to none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_pin: Option<CaPinSetting>,
    /// The longest a TLS handshake with a client or server may take. Defaults to ten seconds.
    pub handshake_timeout: NonZeroDuration,
    /// How many TLS handshakes may be in flight at once; further connections wait in the OS
    /// backlog. Defaults to 256.
    pub max_pending_handshakes: NonZeroUsize,
    /// Pause after a failed `accept` before accepting again. Defaults to one hundred
    /// milliseconds.
    pub accept_error_backoff: NonZeroDuration,
    /// How often a principal's last-seen time is recorded. Defaults to one minute.
    pub principal_seen_interval: NonZeroDuration,
    /// Maximum tracked principal sightings; eviction can cause early writes. Defaults to 10,000.
    pub max_tracked_principals: NonZeroUsize,
    /// The roles that grant permissions to principals. No role ships by default: an empty table
    /// authorizes nobody.
    pub roles: Vec<RoleConfig>,
    /// The largest control API request body accepted. Defaults to 64 KiB.
    pub max_request_size: Bytes,
    /// How long a control API request may take before it is abandoned. Defaults to 30 seconds.
    pub request_timeout: NonZeroDuration,
    /// Limits on the routes that are open to anyone. Defaults to the bucket and tracking values
    /// documented on `RateLimitConfig`.
    pub rate_limit: RateLimitConfig,
}

/// The `[server]` table as written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawServer {
    transport: ServerTransport,
    #[serde(skip_serializing_if = "Option::is_none")]
    trust_domain: Option<TrustDomain>,
    certificate_ttl: NonZeroDuration,
    reissue_at_percent: Percent,
    #[serde(with = "gang_types::duration")]
    certificate_backdate: Duration,
    #[serde(skip_serializing_if = "Option::is_none")]
    ca_pin: Option<CaPinSetting>,
    handshake_timeout: NonZeroDuration,
    max_pending_handshakes: NonZeroUsize,
    accept_error_backoff: NonZeroDuration,
    principal_seen_interval: NonZeroDuration,
    max_tracked_principals: NonZeroUsize,
    roles: Vec<RoleConfig>,
    max_request_size: Bytes,
    request_timeout: NonZeroDuration,
    rate_limit: RateLimitConfig,
}

impl Default for RawServer {
    fn default() -> Self {
        Self {
            transport: ServerTransport::default(),
            trust_domain: None,
            certificate_ttl: DEFAULT_CERTIFICATE_TTL,
            reissue_at_percent: DEFAULT_REISSUE_AT_PERCENT,
            certificate_backdate: DEFAULT_CERTIFICATE_BACKDATE,
            ca_pin: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            max_pending_handshakes: DEFAULT_MAX_PENDING_HANDSHAKES,
            accept_error_backoff: DEFAULT_ACCEPT_ERROR_BACKOFF,
            principal_seen_interval: DEFAULT_PRINCIPAL_SEEN_INTERVAL,
            max_tracked_principals: DEFAULT_MAX_TRACKED_PRINCIPALS,
            roles: Vec::new(),
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// The `tls` transport was selected without a `trust_domain`.
#[derive(Debug, thiserror::Error)]
#[error("trust_domain is required when transport is \"tls\"")]
pub(crate) struct MissingTrustDomain;

impl TryFrom<RawServer> for ServerConfig {
    type Error = MissingTrustDomain;

    fn try_from(raw: RawServer) -> Result<Self, Self::Error> {
        let transport = match (raw.transport, raw.trust_domain) {
            (ServerTransport::Tls, Some(trust_domain)) => Transport::Tls { trust_domain },
            (ServerTransport::Tls, None) => return Err(MissingTrustDomain),
            // A trust domain is ignored under plaintext.
            (ServerTransport::Plaintext, _) => Transport::Plaintext,
        };
        Ok(Self {
            transport,
            certificate_ttl: raw.certificate_ttl,
            reissue_at_percent: raw.reissue_at_percent,
            certificate_backdate: raw.certificate_backdate,
            ca_pin: raw.ca_pin,
            handshake_timeout: raw.handshake_timeout,
            max_pending_handshakes: raw.max_pending_handshakes,
            accept_error_backoff: raw.accept_error_backoff,
            principal_seen_interval: raw.principal_seen_interval,
            max_tracked_principals: raw.max_tracked_principals,
            roles: raw.roles,
            max_request_size: raw.max_request_size,
            request_timeout: raw.request_timeout,
            rate_limit: raw.rate_limit,
        })
    }
}

impl From<ServerConfig> for RawServer {
    fn from(config: ServerConfig) -> Self {
        let (transport, trust_domain) = match config.transport {
            Transport::Tls { trust_domain } => (ServerTransport::Tls, Some(trust_domain)),
            Transport::Plaintext => (ServerTransport::Plaintext, None),
        };
        Self {
            transport,
            trust_domain,
            certificate_ttl: config.certificate_ttl,
            reissue_at_percent: config.reissue_at_percent,
            certificate_backdate: config.certificate_backdate,
            ca_pin: config.ca_pin,
            handshake_timeout: config.handshake_timeout,
            max_pending_handshakes: config.max_pending_handshakes,
            accept_error_backoff: config.accept_error_backoff,
            principal_seen_interval: config.principal_seen_interval,
            max_tracked_principals: config.max_tracked_principals,
            roles: config.roles,
            max_request_size: config.max_request_size,
            request_timeout: config.request_timeout,
            rate_limit: config.rate_limit,
        }
    }
}
