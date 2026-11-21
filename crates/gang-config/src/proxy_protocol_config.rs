//! Trusted PROXY protocol listener settings.

use std::num::NonZeroUsize;

use gang_types::{Bytes, NonZeroDuration};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};

const DEFAULT_HEADER_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);
const DEFAULT_MAX_PENDING_HEADERS: NonZeroUsize = NonZeroUsize::new(256).expect("256 is non-zero");
const DEFAULT_MAX_HEADER_SIZE: Bytes = Bytes::kib(4);
const MIN_HEADER_SIZE: u64 = 16;
const MAX_HEADER_SIZE: u64 = 65_551;

/// The `[server.proxy_protocol]` table requires PROXY v2 on all listeners.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawProxyProtocolConfig", into = "RawProxyProtocolConfig")]
pub struct ProxyProtocolConfig {
    /// Networks of direct socket peers allowed to send headers. Trust only actual proxy addresses.
    pub trusted_cidrs: Vec<IpNet>,
    /// Maximum time to read a header. Defaults to ten seconds; must be nonzero.
    pub header_timeout: NonZeroDuration,
    /// Maximum concurrent header reads per listener. Defaults to 256; must be nonzero.
    pub max_pending_headers: NonZeroUsize,
    /// Maximum total header size, from 16 through 65551 bytes. Defaults to 4 KiB.
    pub max_header_size: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawProxyProtocolConfig {
    trusted_cidrs: Vec<IpNet>,
    header_timeout: NonZeroDuration,
    max_pending_headers: NonZeroUsize,
    max_header_size: Bytes,
}

impl Default for RawProxyProtocolConfig {
    fn default() -> Self {
        Self {
            trusted_cidrs: Vec::new(),
            header_timeout: DEFAULT_HEADER_TIMEOUT,
            max_pending_headers: DEFAULT_MAX_PENDING_HEADERS,
            max_header_size: DEFAULT_MAX_HEADER_SIZE,
        }
    }
}

impl Default for ProxyProtocolConfig {
    fn default() -> Self {
        let raw = RawProxyProtocolConfig::default();
        Self {
            trusted_cidrs: raw.trusted_cidrs,
            header_timeout: raw.header_timeout,
            max_pending_headers: raw.max_pending_headers,
            max_header_size: raw.max_header_size,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidProxyProtocolConfig {
    #[error("trusted_cidrs must not be empty when proxy protocol is enabled")]
    MissingTrustedCidrs,
    #[error("max_header_size must be between 16 and 65551 bytes and fit in memory, not {0}")]
    MaxHeaderSize(Bytes),
}

impl TryFrom<RawProxyProtocolConfig> for ProxyProtocolConfig {
    type Error = InvalidProxyProtocolConfig;

    fn try_from(raw: RawProxyProtocolConfig) -> Result<Self, Self::Error> {
        if raw.trusted_cidrs.is_empty() {
            return Err(InvalidProxyProtocolConfig::MissingTrustedCidrs);
        }
        let size = raw.max_header_size.get();
        if !(MIN_HEADER_SIZE..=MAX_HEADER_SIZE).contains(&size) || usize::try_from(size).is_err() {
            return Err(InvalidProxyProtocolConfig::MaxHeaderSize(
                raw.max_header_size,
            ));
        }
        Ok(Self {
            trusted_cidrs: raw.trusted_cidrs,
            header_timeout: raw.header_timeout,
            max_pending_headers: raw.max_pending_headers,
            max_header_size: raw.max_header_size,
        })
    }
}

impl From<ProxyProtocolConfig> for RawProxyProtocolConfig {
    fn from(config: ProxyProtocolConfig) -> Self {
        Self {
            trusted_cidrs: config.trusted_cidrs,
            header_timeout: config.header_timeout,
            max_pending_headers: config.max_pending_headers,
            max_header_size: config.max_header_size,
        }
    }
}
