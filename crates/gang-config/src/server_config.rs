//! The `[server]` section: settings of `gang server` that are not plugin choices.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{Bytes, NonZeroBytes, NonZeroDuration, Percent, TrustDomain};
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
/// Default time allowed for a controller incarnation lookup.
const DEFAULT_LEADERSHIP_INCARNATION_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);
/// Default interval between revocation refreshes.
const DEFAULT_REVOCATION_REFRESH_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(5);
/// Default maximum graceful shutdown duration.
const DEFAULT_STOP_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(2460);

/// Default time a worker may be silent before the scheduler drops it: 10 seconds.
pub const DEFAULT_WORKER_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10);

/// Default capacity of each scheduler actor inbox: 4,096 messages.
const DEFAULT_SCHEDULER_QUEUE_DEPTH: NonZeroUsize =
    NonZeroUsize::new(4096).expect("4096 is non-zero");

/// Default worker/client messages processed per scheduler actor iteration: 64.
const DEFAULT_SCHEDULER_BATCH: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Default per-worker outbound channel depth: 256 messages.
const DEFAULT_WORKER_OUTBOUND_DEPTH: NonZeroUsize =
    NonZeroUsize::new(256).expect("256 is non-zero");

/// Default time to wait for a worker's `Accept`/`Decline` after `Assign`: 10 seconds.
const DEFAULT_DISPATCH_ACK_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// Default maximum queued jobs inspected per scheduler dispatch scan: 256.
const DEFAULT_DISPATCH_SCAN_LIMIT: NonZeroUsize = NonZeroUsize::new(256).expect("256 is non-zero");

/// Default maximum combined `Action` + `Command` bytes inlined in `Assign`: 64 KiB.
const DEFAULT_ASSIGN_INLINE_MAX: Bytes = Bytes::kib(64);

/// Fixed `Assign` / `ControllerMessage` envelope overhead reserved inside
/// `http2.max_message_size`: 4 KiB.
const ASSIGN_FRAME_OVERHEAD: Bytes = Bytes::kib(4);

/// Default total bytes of CAS blobs allowed in flight simultaneously: 1 GiB.
const DEFAULT_MAX_INFLIGHT_BLOB_BYTES: NonZeroBytes = NonZeroBytes::gib(1);

/// Default `ByteStream.Read` response chunk size: 64 KiB.
const DEFAULT_READ_CHUNK: NonZeroBytes = NonZeroBytes::kib(64);

/// Controller leadership lease timing settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LeadershipConfig {
    /// Lease lifetime. Defaults to ten seconds.
    pub ttl: NonZeroDuration,
    /// Interval between renewals. Defaults to one second.
    pub renew_interval: NonZeroDuration,
    /// Minimum validity required before a provisioner mutation. Defaults to one second.
    pub call_timeout: NonZeroDuration,
    /// Timeout for incarnation lookup, independent of lease timing. Defaults to ten seconds.
    pub incarnation_timeout: NonZeroDuration,
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
            incarnation_timeout: DEFAULT_LEADERSHIP_INCARNATION_TIMEOUT,
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
    /// Maximum graceful shutdown duration. Defaults to 41 minutes.
    pub stop_timeout: NonZeroDuration,
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
    /// Capacity of each scheduler actor inbox. Defaults to 4,096.
    pub scheduler_queue_depth: NonZeroUsize,
    /// Worker/client messages processed per scheduler actor iteration. Defaults to 64.
    pub scheduler_batch: NonZeroUsize,
    /// Per-worker outbound channel capacity. Defaults to 256.
    pub worker_outbound_depth: NonZeroUsize,
    /// Time to wait for a worker's `Accept`/`Decline` after `Assign`. Defaults to 10 seconds.
    pub dispatch_ack_timeout: NonZeroDuration,
    /// Maximum queued jobs inspected per scheduler dispatch scan. Defaults to 256.
    pub dispatch_scan_limit: NonZeroUsize,
    /// Maximum combined `Action` + `Command` bytes inlined in `Assign`. Must be below
    /// `http2.max_message_size` minus the fixed `Assign` frame overhead. Defaults to 64 KiB.
    pub assign_inline_max: Bytes,
    /// Total bytes of CAS blobs allowed in flight simultaneously. Defaults to 1 GiB.
    pub max_inflight_blob_bytes: NonZeroBytes,
    /// `ByteStream.Read` response chunk size. Defaults to 64 KiB.
    pub read_chunk: NonZeroBytes,
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
    stop_timeout: NonZeroDuration,
    revocation_refresh_interval: NonZeroDuration,
    health_max_connections: NonZeroUsize,
    health_max_header_size: Bytes,
    http2: Http2Config,
    worker_timeout: WholeSeconds,
    max_open_files: OpenFileLimit,
    scheduler_queue_depth: NonZeroUsize,
    scheduler_batch: NonZeroUsize,
    worker_outbound_depth: NonZeroUsize,
    dispatch_ack_timeout: NonZeroDuration,
    dispatch_scan_limit: NonZeroUsize,
    assign_inline_max: Bytes,
    max_inflight_blob_bytes: NonZeroBytes,
    read_chunk: NonZeroBytes,
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
            stop_timeout: DEFAULT_STOP_TIMEOUT,
            revocation_refresh_interval: DEFAULT_REVOCATION_REFRESH_INTERVAL,
            health_max_connections: DEFAULT_HEALTH_MAX_CONNECTIONS,
            health_max_header_size: DEFAULT_HEALTH_MAX_HEADER_SIZE,
            http2: Http2Config::default(),
            worker_timeout: DEFAULT_WORKER_TIMEOUT,
            max_open_files: OpenFileLimit::default(),
            scheduler_queue_depth: DEFAULT_SCHEDULER_QUEUE_DEPTH,
            scheduler_batch: DEFAULT_SCHEDULER_BATCH,
            worker_outbound_depth: DEFAULT_WORKER_OUTBOUND_DEPTH,
            dispatch_ack_timeout: DEFAULT_DISPATCH_ACK_TIMEOUT,
            dispatch_scan_limit: DEFAULT_DISPATCH_SCAN_LIMIT,
            assign_inline_max: DEFAULT_ASSIGN_INLINE_MAX,
            max_inflight_blob_bytes: DEFAULT_MAX_INFLIGHT_BLOB_BYTES,
            read_chunk: DEFAULT_READ_CHUNK,
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
    /// `assign_inline_max` does not leave room for the fixed `Assign` frame overhead inside
    /// `http2.max_message_size`.
    #[error(
        "assign_inline_max ({assign_inline_max}) must be below http2.max_message_size ({max_message_size}) minus {overhead} Assign overhead"
    )]
    AssignInlineMaxTooLarge {
        assign_inline_max: Bytes,
        max_message_size: NonZeroBytes,
        overhead: Bytes,
    },
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
        let max_message_size = raw.http2.max_message_size;
        let max_inline = max_message_size.get().saturating_sub(ASSIGN_FRAME_OVERHEAD);
        if raw.assign_inline_max >= max_inline {
            return Err(InvalidServer::AssignInlineMaxTooLarge {
                assign_inline_max: raw.assign_inline_max,
                max_message_size,
                overhead: ASSIGN_FRAME_OVERHEAD,
            });
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
            stop_timeout: raw.stop_timeout,
            revocation_refresh_interval: raw.revocation_refresh_interval,
            health_max_connections: raw.health_max_connections,
            health_max_header_size: raw.health_max_header_size,
            http2: raw.http2,
            worker_timeout: raw.worker_timeout,
            max_open_files: raw.max_open_files,
            scheduler_queue_depth: raw.scheduler_queue_depth,
            scheduler_batch: raw.scheduler_batch,
            worker_outbound_depth: raw.worker_outbound_depth,
            dispatch_ack_timeout: raw.dispatch_ack_timeout,
            dispatch_scan_limit: raw.dispatch_scan_limit,
            assign_inline_max: raw.assign_inline_max,
            max_inflight_blob_bytes: raw.max_inflight_blob_bytes,
            read_chunk: raw.read_chunk,
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
            stop_timeout: config.stop_timeout,
            revocation_refresh_interval: config.revocation_refresh_interval,
            health_max_connections: config.health_max_connections,
            health_max_header_size: config.health_max_header_size,
            http2: config.http2,
            worker_timeout: config.worker_timeout,
            max_open_files: config.max_open_files,
            scheduler_queue_depth: config.scheduler_queue_depth,
            scheduler_batch: config.scheduler_batch,
            worker_outbound_depth: config.worker_outbound_depth,
            dispatch_ack_timeout: config.dispatch_ack_timeout,
            dispatch_scan_limit: config.dispatch_scan_limit,
            assign_inline_max: config.assign_inline_max,
            max_inflight_blob_bytes: config.max_inflight_blob_bytes,
            read_chunk: config.read_chunk,
        }
    }
}

#[cfg(test)]
mod tests {
    use gang_types::{Bytes, NonZeroBytes};

    use super::{ASSIGN_FRAME_OVERHEAD, EndpointTrust, ServerConfig};

    #[test]
    fn stop_timeout_defaults_and_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let default: ServerConfig = toml::from_str("transport = 'plaintext'")?;
        assert_eq!(
            default.stop_timeout,
            gang_types::NonZeroDuration::from_secs(2460)
        );
        let configured: ServerConfig =
            toml::from_str("transport = 'plaintext'\nstop_timeout = '43m'")?;
        assert_eq!(
            configured.stop_timeout,
            gang_types::NonZeroDuration::from_secs(2580)
        );
        assert_eq!(
            toml::from_str::<ServerConfig>(&toml::to_string(&configured)?)?,
            configured
        );
        assert!(
            toml::from_str::<ServerConfig>("transport = 'plaintext'\nstop_timeout = '0s'").is_err()
        );
        Ok(())
    }

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
    fn leadership_incarnation_timeout_defaults_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let default: ServerConfig = toml::from_str("transport = 'plaintext'")?;
        assert_eq!(
            default.leadership.incarnation_timeout.get(),
            std::time::Duration::from_secs(10)
        );
        let config: ServerConfig =
            toml::from_str("transport = 'plaintext'\n[leadership]\nincarnation_timeout = '25s'")?;
        assert_eq!(
            config.leadership.incarnation_timeout.get(),
            std::time::Duration::from_secs(25)
        );
        assert_eq!(
            toml::from_str::<ServerConfig>(&toml::to_string(&config)?)?,
            config
        );
        assert!(
            toml::from_str::<ServerConfig>(
                "transport = 'plaintext'\n[leadership]\nincarnation_timeout = '0s'"
            )
            .is_err()
        );
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

    #[test]
    fn native_engine_settings_default_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let default: ServerConfig = toml::from_str("transport = 'plaintext'")?;
        assert_eq!(default.scheduler_queue_depth.get(), 4096);
        assert_eq!(default.scheduler_batch.get(), 64);
        assert_eq!(default.worker_outbound_depth.get(), 256);
        assert_eq!(
            default.dispatch_ack_timeout.get(),
            std::time::Duration::from_secs(10)
        );
        assert_eq!(default.dispatch_scan_limit.get(), 256);
        assert_eq!(default.assign_inline_max, Bytes::kib(64));
        assert_eq!(default.max_inflight_blob_bytes, NonZeroBytes::gib(1));
        assert_eq!(default.read_chunk, NonZeroBytes::kib(64));

        let custom: ServerConfig = toml::from_str(
            "\
transport = 'plaintext'
scheduler_queue_depth = 8192
scheduler_batch = 128
worker_outbound_depth = 512
dispatch_ack_timeout = '25s'
dispatch_scan_limit = 1024
assign_inline_max = '0B'
max_inflight_blob_bytes = '2GiB'
read_chunk = '128KiB'
",
        )?;
        assert_eq!(custom.scheduler_queue_depth.get(), 8192);
        assert_eq!(custom.scheduler_batch.get(), 128);
        assert_eq!(custom.worker_outbound_depth.get(), 512);
        assert_eq!(
            custom.dispatch_ack_timeout.get(),
            std::time::Duration::from_secs(25)
        );
        assert_eq!(custom.dispatch_scan_limit.get(), 1024);
        assert_eq!(custom.assign_inline_max, Bytes::new(0));
        assert_eq!(custom.max_inflight_blob_bytes, NonZeroBytes::gib(2));
        assert_eq!(custom.read_chunk, NonZeroBytes::kib(128));
        assert_eq!(
            toml::from_str::<ServerConfig>(&toml::to_string(&custom)?)?,
            custom
        );
        Ok(())
    }

    #[test]
    fn native_engine_settings_reject_invalid_values() {
        for input in [
            "transport = 'plaintext'\nscheduler_queue_depth = 0",
            "transport = 'plaintext'\nscheduler_batch = 0",
            "transport = 'plaintext'\nworker_outbound_depth = 0",
            "transport = 'plaintext'\ndispatch_ack_timeout = '0s'",
            "transport = 'plaintext'\ndispatch_scan_limit = 0",
            "transport = 'plaintext'\nmax_inflight_blob_bytes = '0B'",
            "transport = 'plaintext'\nread_chunk = '0B'",
        ] {
            assert!(
                toml::from_str::<ServerConfig>(input).is_err(),
                "accepted {input}"
            );
        }
    }

    #[test]
    fn assign_inline_max_must_fit_within_http2_message_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let max_msg = Bytes::kib(128);
        let at_limit = max_msg.saturating_sub(ASSIGN_FRAME_OVERHEAD);
        let just_below = at_limit.saturating_sub(Bytes::new(1));

        let valid = format!(
            "transport = 'plaintext'\nassign_inline_max = '{just_below}'\n[http2]\nmax_message_size = '{max_msg}'"
        );
        assert_eq!(
            toml::from_str::<ServerConfig>(&valid)?.assign_inline_max,
            just_below
        );

        let equal_limit = format!(
            "transport = 'plaintext'\nassign_inline_max = '{at_limit}'\n[http2]\nmax_message_size = '{max_msg}'"
        );
        assert!(toml::from_str::<ServerConfig>(&equal_limit).is_err());

        let too_small_message =
            "transport = 'plaintext'\n[http2]\nmax_message_size = '64KiB'".to_owned();
        assert!(toml::from_str::<ServerConfig>(&too_small_message).is_err());

        let overhead_only = format!(
            "transport = 'plaintext'\nassign_inline_max = '0B'\n[http2]\nmax_message_size = '{ASSIGN_FRAME_OVERHEAD}'"
        );
        assert!(toml::from_str::<ServerConfig>(&overhead_only).is_err());
        Ok(())
    }
}
