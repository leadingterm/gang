//! The `[server]` section: settings of `gang server` that are not plugin choices.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{Bytes, NonZeroDuration, Percent, TrustDomain};
use serde::{Deserialize, Serialize};

use crate::ca_pin_setting::CaPinSetting;
use crate::proxy_protocol_config::ProxyProtocolConfig;
use crate::rate_limit_config::RateLimitConfig;
use crate::role_config::RoleConfig;
use crate::{Http2Config, OpenFileLimit, WholeSeconds};

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

/// What clients see at `endpoint`, and so which roots they verify it with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointTrust {
    /// Gang's own listener certificate: clients verify `endpoint` against the Gang CA, which
    /// `GET /v1/server` advertises.
    #[default]
    Ca,
    /// A publicly trusted TLS terminator, such as a load balancer that re-encrypts to Gang,
    /// presents `endpoint`: clients verify it against their system roots, and `GET /v1/server`
    /// advertises no CA.
    System,
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

/// The default time a refused TLS connection stays open: two seconds.
const DEFAULT_REFUSAL_LINGER: NonZeroDuration = NonZeroDuration::from_secs(2);

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

/// The default number of health connections open at once: 64.
const DEFAULT_HEALTH_MAX_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// The smallest read buffer the HTTP/1 server accepts: 8 KiB.
const MIN_HEALTH_MAX_HEADER_SIZE: Bytes = Bytes::kib(8);

/// The default largest request head a health connection may send: 8 KiB.
const DEFAULT_HEALTH_MAX_HEADER_SIZE: Bytes = Bytes::kib(8);

/// Default controller leadership lease lifetime.
const DEFAULT_LEADERSHIP_TTL: NonZeroDuration = NonZeroDuration::from_secs(10);
/// Default allowance for clock skew between controllers.
const DEFAULT_LEADERSHIP_CLOCK_SKEW: Duration = Duration::from_secs(2);
/// Default interval between revocation refreshes.
const DEFAULT_REVOCATION_REFRESH_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(5);

/// Default time a worker may be silent before the scheduler drops it: 10 seconds.
pub const DEFAULT_WORKER_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10);

/// Controller leadership lease timing settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LeadershipConfig {
    /// Lease lifetime. Defaults to ten seconds.
    pub ttl: NonZeroDuration,
    /// Interval between renewals. Defaults to three seconds.
    pub renew_interval: NonZeroDuration,
    /// Minimum validity required before a provisioner mutation. Defaults to two seconds.
    pub call_timeout: NonZeroDuration,
    /// Allowance for clock skew. Defaults to two seconds.
    #[serde(with = "gang_types::duration")]
    pub clock_skew: Duration,
}

impl Default for LeadershipConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_LEADERSHIP_TTL,
            renew_interval: NonZeroDuration::from_secs(
                gang_types::handover::DEFAULT_RENEW_INTERVAL.as_secs(),
            ),
            call_timeout: NonZeroDuration::from_secs(
                gang_types::handover::DEFAULT_CALL_TIMEOUT.as_secs(),
            ),
            clock_skew: DEFAULT_LEADERSHIP_CLOCK_SKEW,
        }
    }
}

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
    /// What clients see at `endpoint`: `ca` or `system`. Applies under `tls`; a plaintext server
    /// advertises no CA either way. Defaults to `ca`.
    pub endpoint_trust: EndpointTrust,
    /// The longest a TLS handshake with a client or server may take. Defaults to ten seconds.
    pub handshake_timeout: NonZeroDuration,
    /// How many TLS handshakes may be in flight at once; further connections wait in the OS
    /// backlog. Defaults to 256.
    pub max_pending_handshakes: NonZeroUsize,
    /// How long a refused TLS connection stays open for its client to read the alert.
    pub refusal_linger: NonZeroDuration,
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
    /// When present, require trusted PROXY v2 headers on all configured listeners.
    pub proxy_protocol: Option<ProxyProtocolConfig>,
    /// Controller leadership timing. Defaults to the values on [`LeadershipConfig`].
    pub leadership: LeadershipConfig,
    /// Time a newer standby must be routable before requesting handover. Defaults to zero.
    pub standby_routability_delay: Duration,
    /// Interval between revocation refreshes. Defaults to five seconds.
    pub revocation_refresh_interval: NonZeroDuration,
    /// How many health listener connections may be open at once; further connections wait in the
    /// OS backlog. Defaults to 64.
    pub health_max_connections: NonZeroUsize,
    /// The largest request head a health listener connection may send, at least 8 KiB. Defaults
    /// to 8 KiB.
    pub health_max_header_size: Bytes,
    /// HTTP/2 connection and message limits and timeouts.
    pub http2: Http2Config,
    /// Time a worker may be silent before the scheduler drops it. Defaults to 10 seconds.
    pub worker_timeout: WholeSeconds,
    /// Process open-file limit. Defaults to 24,576.
    pub max_open_files: OpenFileLimit,
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
    endpoint_trust: EndpointTrust,
    handshake_timeout: NonZeroDuration,
    max_pending_handshakes: NonZeroUsize,
    refusal_linger: NonZeroDuration,
    accept_error_backoff: NonZeroDuration,
    principal_seen_interval: NonZeroDuration,
    max_tracked_principals: NonZeroUsize,
    roles: Vec<RoleConfig>,
    max_request_size: Bytes,
    request_timeout: NonZeroDuration,
    rate_limit: RateLimitConfig,
    proxy_protocol: Option<ProxyProtocolConfig>,
    leadership: LeadershipConfig,
    #[serde(with = "gang_types::duration")]
    standby_routability_delay: Duration,
    revocation_refresh_interval: NonZeroDuration,
    health_max_connections: NonZeroUsize,
    health_max_header_size: Bytes,
    http2: Http2Config,
    worker_timeout: WholeSeconds,
    max_open_files: OpenFileLimit,
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
            endpoint_trust: EndpointTrust::default(),
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            max_pending_handshakes: DEFAULT_MAX_PENDING_HANDSHAKES,
            refusal_linger: DEFAULT_REFUSAL_LINGER,
            accept_error_backoff: DEFAULT_ACCEPT_ERROR_BACKOFF,
            principal_seen_interval: DEFAULT_PRINCIPAL_SEEN_INTERVAL,
            max_tracked_principals: DEFAULT_MAX_TRACKED_PRINCIPALS,
            roles: Vec::new(),
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            rate_limit: RateLimitConfig::default(),
            proxy_protocol: None,
            leadership: LeadershipConfig::default(),
            standby_routability_delay: Duration::ZERO,
            revocation_refresh_interval: DEFAULT_REVOCATION_REFRESH_INTERVAL,
            health_max_connections: DEFAULT_HEALTH_MAX_CONNECTIONS,
            health_max_header_size: DEFAULT_HEALTH_MAX_HEADER_SIZE,
            http2: Http2Config::default(),
            worker_timeout: DEFAULT_WORKER_TIMEOUT,
            max_open_files: OpenFileLimit::default(),
        }
    }
}

/// A `[server]` table that names no valid server configuration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidServer {
    /// The `tls` transport was selected without a `trust_domain`.
    #[error("trust_domain is required when transport is \"tls\"")]
    MissingTrustDomain,
    /// The health header size is below the HTTP/1 server's smallest read buffer, or does not
    /// fit in memory.
    #[error(
        "health_max_header_size must be at least {min} and fit in memory, not {0}",
        min = MIN_HEALTH_MAX_HEADER_SIZE
    )]
    HealthMaxHeaderSize(Bytes),
}

impl TryFrom<RawServer> for ServerConfig {
    type Error = InvalidServer;

    fn try_from(raw: RawServer) -> Result<Self, Self::Error> {
        let transport = match (raw.transport, raw.trust_domain) {
            (ServerTransport::Tls, Some(trust_domain)) => Transport::Tls { trust_domain },
            (ServerTransport::Tls, None) => return Err(InvalidServer::MissingTrustDomain),
            // A trust domain is ignored under plaintext.
            (ServerTransport::Plaintext, _) => Transport::Plaintext,
        };
        let header_size = raw.health_max_header_size;
        if header_size < MIN_HEALTH_MAX_HEADER_SIZE || usize::try_from(header_size.get()).is_err() {
            return Err(InvalidServer::HealthMaxHeaderSize(header_size));
        }
        Ok(Self {
            transport,
            certificate_ttl: raw.certificate_ttl,
            reissue_at_percent: raw.reissue_at_percent,
            certificate_backdate: raw.certificate_backdate,
            ca_pin: raw.ca_pin,
            endpoint_trust: raw.endpoint_trust,
            handshake_timeout: raw.handshake_timeout,
            max_pending_handshakes: raw.max_pending_handshakes,
            refusal_linger: raw.refusal_linger,
            accept_error_backoff: raw.accept_error_backoff,
            principal_seen_interval: raw.principal_seen_interval,
            max_tracked_principals: raw.max_tracked_principals,
            roles: raw.roles,
            max_request_size: raw.max_request_size,
            request_timeout: raw.request_timeout,
            rate_limit: raw.rate_limit,
            proxy_protocol: raw.proxy_protocol,
            leadership: raw.leadership,
            standby_routability_delay: raw.standby_routability_delay,
            revocation_refresh_interval: raw.revocation_refresh_interval,
            health_max_connections: raw.health_max_connections,
            health_max_header_size: raw.health_max_header_size,
            http2: raw.http2,
            worker_timeout: raw.worker_timeout,
            max_open_files: raw.max_open_files,
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
            endpoint_trust: config.endpoint_trust,
            handshake_timeout: config.handshake_timeout,
            max_pending_handshakes: config.max_pending_handshakes,
            refusal_linger: config.refusal_linger,
            accept_error_backoff: config.accept_error_backoff,
            principal_seen_interval: config.principal_seen_interval,
            max_tracked_principals: config.max_tracked_principals,
            roles: config.roles,
            max_request_size: config.max_request_size,
            request_timeout: config.request_timeout,
            rate_limit: config.rate_limit,
            proxy_protocol: config.proxy_protocol,
            leadership: config.leadership,
            standby_routability_delay: config.standby_routability_delay,
            revocation_refresh_interval: config.revocation_refresh_interval,
            health_max_connections: config.health_max_connections,
            health_max_header_size: config.health_max_header_size,
            http2: config.http2,
            worker_timeout: config.worker_timeout,
            max_open_files: config.max_open_files,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointTrust, ServerConfig};

    #[test]
    fn connection_settings_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let config: ServerConfig = toml::from_str(
            "transport = 'plaintext'\nworker_timeout = '17s'\nmax_open_files = 8192\n[http2]\nreceive_window = '65535B'\nmax_connections = 42",
        )?;
        assert_eq!(config.worker_timeout, crate::WholeSeconds::from_units(17));
        assert_eq!(config.max_open_files.get(), 8192);
        assert_eq!(config.http2.receive_window.get().get(), 65_535);
        assert_eq!(config.http2.max_connections.get(), 42);
        assert_eq!(
            toml::from_str::<ServerConfig>(&toml::to_string(&config)?)?,
            config
        );
        Ok(())
    }

    #[test]
    fn standby_routability_delay_defaults_and_round_trips() -> Result<(), Box<dyn std::error::Error>>
    {
        let default: ServerConfig = toml::from_str("transport = 'plaintext'")?;
        assert_eq!(default.standby_routability_delay, std::time::Duration::ZERO);
        let config: ServerConfig =
            toml::from_str("transport = 'plaintext'\nstandby_routability_delay = '10s'")?;
        assert_eq!(
            config.standby_routability_delay,
            std::time::Duration::from_secs(10)
        );
        let serialized = toml::to_string(&config)?;
        let reparsed: ServerConfig = toml::from_str(&serialized)?;
        assert_eq!(reparsed, config);
        Ok(())
    }

    #[test]
    fn connection_settings_reject_invalid_values() {
        assert!(
            toml::from_str::<ServerConfig>("transport = 'plaintext'\nworker_timeout = '0s'")
                .is_err()
        );
        assert!(
            toml::from_str::<ServerConfig>("transport = 'plaintext'\nmax_open_files = 7").is_err()
        );
        assert!(
            toml::from_str::<ServerConfig>("transport = 'plaintext'\n[http2]\nunknown = 1")
                .is_err()
        );
    }

    #[test]
    fn endpoint_trust_defaults_to_ca() -> Result<(), Box<dyn std::error::Error>> {
        let config: ServerConfig = toml::from_str("transport = \"plaintext\"")?;
        assert_eq!(config.endpoint_trust, EndpointTrust::Ca);
        Ok(())
    }

    #[test]
    fn endpoint_trust_system_parses_and_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let config: ServerConfig =
            toml::from_str("transport = \"plaintext\"\nendpoint_trust = \"system\"")?;
        assert_eq!(config.endpoint_trust, EndpointTrust::System);
        assert!(toml::to_string(&config)?.contains("endpoint_trust = \"system\""));
        Ok(())
    }

    #[test]
    fn endpoint_trust_rejects_an_unknown_value() {
        assert!(
            toml::from_str::<ServerConfig>(
                "transport = \"plaintext\"\nendpoint_trust = \"public\""
            )
            .is_err()
        );
    }
}
