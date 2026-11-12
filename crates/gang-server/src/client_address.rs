//! The address a request really came from. The socket peer is the client unless it is a trusted
//! proxy, in which case the client is the last `X-Forwarded-For` entry that is not itself a
//! trusted proxy. Entries are read from the right so that a client cannot prepend a fake address
//! to a chain the proxies appended to.

use std::net::IpAddr;

use http::HeaderMap;
use ipnet::{IpNet, Ipv4Net};

/// The header a trusted proxy appends the address it received a request from to.
const FORWARDED_FOR: &str = "x-forwarded-for";

/// The address of the client that sent a request, stored in the request's extensions by the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientAddress(pub(crate) IpAddr);

/// The proxies whose `X-Forwarded-For` header is believed. An empty set believes none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TrustedProxies {
    networks: Vec<IpNet>,
}

impl TrustedProxies {
    /// The proxies inside `networks`. An IPv4-mapped IPv6 network of prefix length 96 or more is
    /// stored as the IPv4 network it names, so it matches the IPv4 peers it covers.
    #[must_use]
    pub(crate) fn new(networks: Vec<IpNet>) -> Self {
        Self {
            networks: networks.into_iter().map(canonical_network).collect(),
        }
    }

    /// The client of a request that arrived from `peer` with `headers`.
    ///
    /// `None` when there is no peer. A peer outside every trusted network is the client, and the
    /// header is ignored. A trusted peer's chain is walked from its last entry to its first: the
    /// first entry outside the trusted networks is the client. A malformed entry ends the walk
    /// and the peer is the client. If every entry is trusted, the leftmost one is the client.
    #[must_use]
    pub(crate) fn client(&self, peer: Option<IpAddr>, headers: &HeaderMap) -> Option<IpAddr> {
        let peer = peer?.to_canonical();
        if !self.trusts(peer) {
            return Some(peer);
        }
        let mut leftmost = None;
        let entries = headers
            .get_all(FORWARDED_FOR)
            .iter()
            .rev()
            .flat_map(|value| value.as_bytes().rsplit(|byte| *byte == b','))
            .map(<[u8]>::trim_ascii)
            .filter(|entry| !entry.is_empty());
        for entry in entries {
            let Some(address) = parse_entry(entry) else {
                return Some(peer);
            };
            if !self.trusts(address) {
                return Some(address);
            }
            leftmost = Some(address);
        }
        leftmost.or(Some(peer))
    }

    /// Whether `address` lies inside a trusted network. `address` must already be canonical.
    fn trusts(&self, address: IpAddr) -> bool {
        self.networks
            .iter()
            .any(|network| network.contains(&address))
    }
}

/// Parses one `X-Forwarded-For` entry, canonicalised. `None` if it is not an address.
fn parse_entry(entry: &[u8]) -> Option<IpAddr> {
    std::str::from_utf8(entry)
        .ok()?
        .parse::<IpAddr>()
        .ok()
        .map(|address| address.to_canonical())
}

/// The prefix length of the IPv4-mapped block `::ffff:0:0/96`, which spans every IPv4 address.
const IPV4_MAPPED_PREFIX_LENGTH: u8 = 96;

/// `network` as a canonical peer address is compared with it: an IPv4-mapped IPv6 network of
/// prefix length `IPV4_MAPPED_PREFIX_LENGTH` or more becomes the IPv4 network it names. Shorter
/// prefixes span more than IPv4 and stay IPv6.
fn canonical_network(network: IpNet) -> IpNet {
    let IpNet::V6(net) = network else {
        return network;
    };
    let (Some(v4), Some(prefix_length)) = (
        net.addr().to_ipv4_mapped(),
        net.prefix_len().checked_sub(IPV4_MAPPED_PREFIX_LENGTH),
    ) else {
        return network;
    };
    Ipv4Net::new(v4, prefix_length).map_or(network, IpNet::V4)
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use http::{HeaderMap, HeaderValue};
    use ipnet::IpNet;

    use super::{FORWARDED_FOR, TrustedProxies};

    fn ip(text: &str) -> IpAddr {
        text.parse().expect("valid address")
    }

    fn proxies(networks: &[&str]) -> TrustedProxies {
        TrustedProxies::new(
            networks
                .iter()
                .map(|network| network.parse::<IpNet>().expect("valid network"))
                .collect(),
        )
    }

    fn headers(lines: &[&'static str]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for line in lines {
            map.append(FORWARDED_FOR, HeaderValue::from_static(line));
        }
        map
    }

    #[test]
    fn no_trusted_proxies_ignore_the_header() {
        let client =
            TrustedProxies::default().client(Some(ip("10.0.0.1")), &headers(&["203.0.113.7"]));
        assert_eq!(client, Some(ip("10.0.0.1")));
    }

    #[test]
    fn untrusted_peer_ignores_a_spoofed_header() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(Some(ip("198.51.100.9")), &headers(&["203.0.113.7"]));
        assert_eq!(client, Some(ip("198.51.100.9")));
    }

    #[test]
    fn trusted_peer_with_a_single_entry_is_the_client() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(Some(ip("10.0.0.1")), &headers(&["203.0.113.7"]));
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn chain_of_client_then_trusted_proxy_returns_the_client() {
        let trusted = proxies(&["10.0.0.0/8", "172.16.0.0/12"]);
        let client = trusted.client(Some(ip("172.16.0.5")), &headers(&["203.0.113.7, 10.0.0.2"]));
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn fake_entry_left_of_a_genuine_chain_is_not_believed() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(
            Some(ip("10.0.0.1")),
            &headers(&["198.51.100.66, 203.0.113.7, 10.0.0.2"]),
        );
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn multiple_header_lines_form_one_chain_in_order() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(
            Some(ip("10.0.0.1")),
            &headers(&["198.51.100.66", "203.0.113.7", "10.0.0.2"]),
        );
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn malformed_entry_falls_back_to_the_peer() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(
            Some(ip("10.0.0.1")),
            &headers(&["203.0.113.7, not-an-ip, 10.0.0.2"]),
        );
        assert_eq!(client, Some(ip("10.0.0.1")));
    }

    #[test]
    fn all_trusted_chain_returns_the_leftmost_entry() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(Some(ip("10.0.0.1")), &headers(&["10.0.0.5, 10.0.0.6"]));
        assert_eq!(client, Some(ip("10.0.0.5")));
    }

    #[test]
    fn ipv4_mapped_peer_matches_an_ipv4_network() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let client = trusted.client(
            Some(ip("::ffff:10.0.0.1")),
            &headers(&["::ffff:203.0.113.7"]),
        );
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn ipv6_clients_are_resolved() {
        let trusted = proxies(&["2001:db8:ffff::/48"]);
        let client = trusted.client(Some(ip("2001:db8:ffff::1")), &headers(&["2001:db8:1::42"]));
        assert_eq!(client, Some(ip("2001:db8:1::42")));
    }

    #[test]
    fn no_peer_yields_no_client() {
        let trusted = proxies(&["10.0.0.0/8"]);
        assert_eq!(trusted.client(None, &headers(&["203.0.113.7"])), None);
    }

    #[test]
    fn ipv4_mapped_network_trusts_the_ipv4_peer() {
        let trusted = proxies(&["::ffff:10.0.0.0/104"]);
        let client = trusted.client(Some(ip("10.1.2.3")), &headers(&["203.0.113.7"]));
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn ipv4_mapped_network_trusts_the_mapped_spelling_of_the_peer() {
        let trusted = proxies(&["::ffff:10.0.0.0/104"]);
        let client = trusted.client(Some(ip("::ffff:10.1.2.3")), &headers(&["203.0.113.7"]));
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn ipv4_mapped_host_network_trusts_exactly_that_address() {
        let trusted = proxies(&["::ffff:192.0.2.1/128"]);
        let trusted_peer = trusted.client(Some(ip("192.0.2.1")), &headers(&["203.0.113.7"]));
        assert_eq!(trusted_peer, Some(ip("203.0.113.7")));

        let neighbour = trusted.client(Some(ip("192.0.2.2")), &headers(&["203.0.113.7"]));
        assert_eq!(neighbour, Some(ip("192.0.2.2")));
    }

    #[test]
    fn ipv6_network_shorter_than_96_bits_stays_ipv6() {
        let trusted = proxies(&["::ffff:0:0/95"]);
        let client = trusted.client(Some(ip("::fffe:1:2")), &headers(&["2001:db8::7"]));
        assert_eq!(client, Some(ip("2001:db8::7")));
    }

    /// The client `proxies` resolves for `peer` and `headers`, asserting that resolving it
    /// allocates nothing.
    fn client_without_allocating(
        proxies: &TrustedProxies,
        peer: Option<IpAddr>,
        headers: &HeaderMap,
    ) -> Option<IpAddr> {
        let mut client = None;
        let counts = allocation_counter::measure(|| {
            client = proxies.client(peer, headers);
        });
        assert_eq!(counts.count_total, 0, "resolving the client allocated");
        client
    }

    #[test]
    fn a_trusted_proxy_chain_is_resolved_without_allocating() {
        let trusted = proxies(&["10.0.0.0/8", "172.16.0.0/12"]);
        let map = headers(&[
            "198.51.100.66, 203.0.113.7",
            "10.0.0.2",
            "172.16.0.3, 10.0.0.4",
        ]);
        let client = client_without_allocating(&trusted, Some(ip("10.0.0.1")), &map);
        assert_eq!(client, Some(ip("203.0.113.7")));
    }

    #[test]
    fn all_trusted_chain_is_resolved_without_allocating() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let map = headers(&["10.0.0.5, 10.0.0.6"]);
        let client = client_without_allocating(&trusted, Some(ip("10.0.0.1")), &map);
        assert_eq!(client, Some(ip("10.0.0.5")));
    }

    #[test]
    fn malformed_entry_is_resolved_without_allocating() {
        let trusted = proxies(&["10.0.0.0/8"]);
        let map = headers(&["203.0.113.7, not-an-ip, 10.0.0.2"]);
        let client = client_without_allocating(&trusted, Some(ip("10.0.0.1")), &map);
        assert_eq!(client, Some(ip("10.0.0.1")));
    }
}
