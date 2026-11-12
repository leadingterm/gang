//! Loopback ports selected outside the host's ephemeral allocation range.

use std::collections::HashSet;
use std::net::{Ipv4Addr, TcpListener};
use std::sync::LazyLock;

use parking_lot::Mutex;

use crate::errors::{E2eError, Result};

/// Ports below this floor have a high concentration of well-known and registered services.
const PORT_FLOOR: u16 = 10_000;
/// The IANA dynamic range start, also the default ephemeral range start on macOS.
#[cfg(not(target_os = "linux"))]
const IANA_DYNAMIC_START: u16 = 49_152;
/// Maximum random candidates examined before giving up on a reservation.
const MAX_ATTEMPTS: usize = 1_000;

/// Ports remain reserved for the lifetime of this process, even after a placement is dropped.
static RESERVED: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Reads the first port from Linux's whitespace-separated ephemeral port range.
#[cfg(any(target_os = "linux", test))]
fn parse_ephemeral_start(range: &str) -> Option<u16> {
    range.split_whitespace().next()?.parse().ok()
}

/// Reads the first port the Linux host may allocate as an ephemeral port.
#[cfg(target_os = "linux")]
fn ephemeral_start() -> Result<u16> {
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .map_err(E2eError::system_from("reading the ephemeral port range"))?;
    parse_ephemeral_start(&range).ok_or_else(|| E2eError::system("invalid ephemeral port range"))
}

/// Chooses a free loopback port outside the ephemeral range and reserves it in this process.
///
/// The probe socket is dropped before return; another process can still bind the chosen port.
///
/// # Errors
///
/// Returns a system error if the range or randomness cannot be read, or no port is found.
pub(crate) fn reserve() -> Result<u16> {
    #[cfg(target_os = "linux")]
    let start = ephemeral_start()?;
    #[cfg(not(target_os = "linux"))]
    let start = IANA_DYNAMIC_START;
    let width = u32::from(start.saturating_sub(PORT_FLOOR));
    if width == 0 {
        return Err(E2eError::system(
            "no free loopback port below the ephemeral range",
        ));
    }
    // Reject the incomplete final bucket so each candidate has the same probability.
    let limit = u32::MAX - u32::MAX % width;
    for _ in 0..MAX_ATTEMPTS {
        let mut bytes = [0_u8; 4];
        getrandom::fill(&mut bytes)
            .map_err(E2eError::system_from("reading operating system randomness"))?;
        let value = u32::from_ne_bytes(bytes);
        if value >= limit {
            continue;
        }
        let offset = u16::try_from(value % width)
            .map_err(E2eError::system_from("converting a random port offset"))?;
        let port = PORT_FLOOR + offset;
        let mut reserved = RESERVED.lock();
        if !reserved.contains(&port) && TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok() {
            reserved.insert(port);
            return Ok(port);
        }
    }
    Err(E2eError::system(
        "no free loopback port below the ephemeral range",
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "linux"))]
    use super::IANA_DYNAMIC_START;
    #[cfg(target_os = "linux")]
    use super::ephemeral_start;
    use super::{PORT_FLOOR, parse_ephemeral_start, reserve};

    #[test]
    fn reservations_are_distinct_and_below_the_ephemeral_range() {
        let first = reserve().expect("first port is reserved");
        let second = reserve().expect("second port is reserved");
        #[cfg(target_os = "linux")]
        let start = ephemeral_start().expect("ephemeral range is available");
        #[cfg(not(target_os = "linux"))]
        let start = IANA_DYNAMIC_START;
        assert_ne!(first, second);
        assert!((PORT_FLOOR..start).contains(&first));
        assert!((PORT_FLOOR..start).contains(&second));
    }

    #[test]
    fn parses_linux_ephemeral_range() {
        assert_eq!(parse_ephemeral_start("32768\t60999\n"), Some(32_768));
    }
}
