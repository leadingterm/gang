//! Private, newline-delimited control protocol for the network fixture.

use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::RawFd;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use gang_types::PublicEgressPolicy;
use serde::{Deserialize, Serialize};

/// Duplicates an inherited control stream without taking ownership of the original descriptor.
///
/// Both descriptors are marked close-on-exec.
///
/// # Errors
///
/// Returns an I/O error if the descriptor is invalid or cannot be duplicated.
pub fn inherited_unix_stream(fd: RawFd) -> io::Result<UnixStream> {
    gang_os::duplicate_inherited_fd(fd).map(UnixStream::from)
}

/// Addresses and identity used by the fixture and the action probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressTargets {
    /// Public IPv4 listener.
    pub public_ipv4: SocketAddr,
    /// Public IPv6 listener.
    pub public_ipv6: SocketAddr,
    /// Protected listeners by fixture label.
    pub blocked: BTreeMap<String, SocketAddr>,
    /// Public DNS server address.
    pub dns: IpAddr,
    /// Expected HTTP response and datagram contents.
    pub nonce: String,
    /// Expected action user ID.
    pub uid: u32,
    /// Expected action group ID.
    pub gid: u32,
    /// Socket and control operation deadline.
    #[serde(with = "gang_types::duration")]
    pub timeout: Duration,
    /// Policy installed by the fixture.
    pub policy: PublicEgressPolicy,
}

/// Commands sent to the peer over its inherited control stream.
#[derive(Debug, Serialize, Deserialize)]
pub enum PeerCommand {
    /// Bind listeners and report their actual ports.
    Start,
    /// Reset all counters after the reachability baseline.
    Reset,
    /// Return the current counters.
    Counts,
    /// Terminate the peer.
    Stop,
}

/// Replies from the peer over its inherited control stream.
#[derive(Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    /// The child has entered its network namespace and is ready for setup.
    Booted,
    /// All listeners are bound and available.
    Ready(Box<EgressTargets>),
    /// Current counts keyed by label and protocol.
    Counts(BTreeMap<String, u64>),
    /// The peer is stopping.
    Stopped,
}
