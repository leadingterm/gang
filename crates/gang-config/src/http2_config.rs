//! HTTP/2 connection limits and timeouts for the server.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use gang_types::{BoundedBytes, NonZeroBytes, NonZeroDuration};
use serde::{Deserialize, Serialize};

use crate::WholeSeconds;

/// The smallest receive window HTTP/2 can honour (RFC 9113, section 6.9.2).
const MIN_RECEIVE_WINDOW: u64 = 65_535;
/// The largest flow-control window HTTP/2 allows (RFC 9113, section 6.9.1).
const MAX_RECEIVE_WINDOW: u64 = (1 << 31) - 1;

/// Bytes a client may send on a connection or stream before acknowledgement.
pub type ReceiveWindow = BoundedBytes<1, MIN_RECEIVE_WINDOW, MAX_RECEIVE_WINDOW>;

/// Settings of every connection the controller is handed. Every field has a default.
///
/// An HTTP/2 connection holds at most `receive_window` unread bytes, and an HTTP/1.1
/// connection at most hyper's 408 KiB read buffer. A gRPC request decodes at most one
/// `max_message_size` message (plus one 16 KiB HTTP/2 frame) at a time, on at most
/// `max_concurrent_streams` requests per connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Http2Config {
    /// Largest gRPC message a connection accepts. Default: `4MiB`.
    pub max_message_size: NonZeroBytes,
    /// Maximum connections on the public listener. Default: 128.
    pub max_connections: NonZeroUsize,
    /// Shortest interval between a listener's refused-connection logs. Default: `60s`.
    pub refusal_log_interval: NonZeroDuration,
    /// How long an unproven public connection may stay idle. Default: `30s`.
    pub unproven_idle_timeout: NonZeroDuration,
    /// Maximum concurrent requests on one HTTP/2 connection. Default: 100.
    pub max_concurrent_streams: NonZeroU32,
    /// Receive window per connection and stream, from 65535 to 2147483647 bytes.
    /// Default: `16MiB`.
    pub receive_window: ReceiveWindow,
    /// How long a `ByteStream` write survives a client disconnect. Default: `10s`.
    pub stream_idle_timeout: WholeSeconds,
    /// Interval of HTTP/2 keepalive pings sent to clients. Default: `30s`.
    pub keepalive_interval: NonZeroDuration,
    /// Time for open connections to finish after connection streams end. Default: `30s`.
    #[serde(with = "gang_types::duration")]
    pub shutdown_grace: Duration,
    /// Time allowed to send an HTTP/1.1 request head. Default: `30s`.
    pub header_read_timeout: NonZeroDuration,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            max_message_size: NonZeroBytes::mib(4),
            max_connections: NonZeroUsize::MIN.saturating_add(127),
            refusal_log_interval: NonZeroDuration::from_secs(60),
            unproven_idle_timeout: NonZeroDuration::from_secs(30),
            max_concurrent_streams: NonZeroU32::MIN.saturating_add(99),
            receive_window: ReceiveWindow::from_units(16 * 1024 * 1024),
            stream_idle_timeout: WholeSeconds::from_units(10),
            keepalive_interval: NonZeroDuration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
            header_read_timeout: NonZeroDuration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Http2Config;

    #[test]
    fn receive_window_bounds_and_unknown_fields() {
        assert!(toml::from_str::<Http2Config>("receive_window = '65535B'").is_ok());
        assert!(toml::from_str::<Http2Config>("receive_window = '2147483647B'").is_ok());
        assert!(toml::from_str::<Http2Config>("receive_window = '65534B'").is_err());
        assert!(toml::from_str::<Http2Config>("receive_window = '2147483648B'").is_err());
        assert!(toml::from_str::<Http2Config>("unknown = 1").is_err());
    }
}
