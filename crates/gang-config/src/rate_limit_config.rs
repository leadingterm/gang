//! The `[server.rate_limit]` section: limits on the routes that are open to anyone.

use std::num::{NonZeroU32, NonZeroUsize};

use gang_types::NonZeroDuration;
use ipnet::IpNet;
use serde::{Deserialize, Serialize};

/// The default burst of the per-client bucket: 30 requests.
const DEFAULT_CLIENT_BURST: NonZeroU32 = NonZeroU32::new(30).expect("30 is non-zero");

/// The default refill interval of the per-client bucket: two seconds, so 30 requests a minute
/// sustained.
const DEFAULT_CLIENT_REFILL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(2);

/// The default burst of the global bucket: 500 requests.
const DEFAULT_GLOBAL_BURST: NonZeroU32 = NonZeroU32::new(500).expect("500 is non-zero");

/// The default refill interval of the global bucket: 50 milliseconds, so 20 requests a second
/// sustained.
const DEFAULT_GLOBAL_REFILL_INTERVAL: NonZeroDuration = NonZeroDuration::from_millis(50);

/// The default number of distinct clients tracked at once: 10,000.
const DEFAULT_MAX_TRACKED_CLIENTS: NonZeroUsize =
    NonZeroUsize::new(10_000).expect("10000 is non-zero");

/// A number of leading IPv6 address bits, from 1 through 128.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Ipv6PrefixLength(u8);

/// An IPv6 prefix length outside the valid range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ipv6 prefix length {0} is outside 1..=128")]
pub struct Ipv6PrefixLengthError(u8);

impl Ipv6PrefixLength {
    /// The default prefix length, 64 bits.
    pub const DEFAULT: Self = Self(64);

    /// Creates a prefix length from its number of bits.
    ///
    /// # Errors
    ///
    /// Returns [`Ipv6PrefixLengthError`] if `bits` is outside `1..=128`.
    pub fn try_new(bits: u8) -> Result<Self, Ipv6PrefixLengthError> {
        if (1..=128).contains(&bits) {
            Ok(Self(bits))
        } else {
            Err(Ipv6PrefixLengthError(bits))
        }
    }

    /// Returns the number of prefix bits.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for Ipv6PrefixLength {
    type Error = Ipv6PrefixLengthError;

    fn try_from(bits: u8) -> Result<Self, Self::Error> {
        Self::try_new(bits)
    }
}

impl From<Ipv6PrefixLength> for u8 {
    fn from(length: Ipv6PrefixLength) -> Self {
        length.get()
    }
}

/// One token bucket: `burst` requests may arrive at once; the bucket earns one request back every
/// `refill_interval`.
///
/// When a bucket table is written, both keys are required; the table as a whole is optional and
/// defaults as a unit (see [`RateLimitConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    /// The number of requests that may arrive at once. Required when the bucket table is
    /// written. Must be non-zero.
    pub burst: NonZeroU32,
    /// The time the bucket takes to earn back one request. Required when the bucket table is
    /// written. Must be non-zero, in the `gang_types::duration` format.
    pub refill_interval: NonZeroDuration,
}

/// The `[server.rate_limit]` table: limits on the routes that are open to anyone. Every field has
/// a default, so the table may be omitted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RateLimitConfig {
    /// The bucket each client address draws from. Defaults to burst 30 with `refill_interval`
    /// 2 seconds, which is 30 requests a minute sustained.
    pub client: BucketConfig,
    /// The bucket shared by all clients. Defaults to burst 500 with `refill_interval` 50
    /// milliseconds, which is 20 requests a second sustained.
    pub global: BucketConfig,
    /// The number of distinct clients whose buckets are kept at once. Must be non-zero. Defaults
    /// to 10,000.
    pub max_tracked_clients: NonZeroUsize,
    /// IPv6 clients are limited as one when they share their first this-many bits, in bits from
    /// 1 to 128. One subscriber usually owns a /64, so this defaults to 64. IPv4 and
    /// IPv4-mapped clients are always limited by their whole address.
    pub ipv6_prefix_length: Ipv6PrefixLength,
    /// Networks in CIDR notation, such as `"10.0.0.0/8"` or `"192.0.2.1/32"`. The
    /// `X-Forwarded-For` header is believed only on connections that come from one of them; a
    /// single address is written with its full prefix (`/32`, `/128`). Defaults to none, so the
    /// header is ignored.
    pub trusted_proxies: Vec<IpNet>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            client: BucketConfig {
                burst: DEFAULT_CLIENT_BURST,
                refill_interval: DEFAULT_CLIENT_REFILL_INTERVAL,
            },
            global: BucketConfig {
                burst: DEFAULT_GLOBAL_BURST,
                refill_interval: DEFAULT_GLOBAL_REFILL_INTERVAL,
            },
            max_tracked_clients: DEFAULT_MAX_TRACKED_CLIENTS,
            ipv6_prefix_length: Ipv6PrefixLength::DEFAULT,
            trusted_proxies: Vec::new(),
        }
    }
}
