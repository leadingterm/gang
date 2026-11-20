//! Fleet action grants, resource ceilings and public egress policy.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU32;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Bytes, NonZeroBytes, NonZeroDuration, Resources, property};

crate::string_enum! {
    /// A capability a fleet may grant to actions.
    pub enum Capability("capability") {
        /// Public network egress.
        Network = "network",
    }
}

impl Capability {
    /// Platform property requesting this capability.
    #[must_use]
    pub const fn property(self) -> &'static str {
        match self {
            Self::Network => property::NETWORK,
        }
    }
}

crate::string_enum! {
    /// Whether an action requests network access.
    pub enum NetworkRequest("network request") {
        /// The action needs no network access.
        NotRequested = "not-requested",
        /// The action requests network access.
        Requested = "requested",
    }
}

/// Per-action limits; absent limits use the physical worker capacity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceCeilings {
    /// Maximum whole CPUs per action.
    pub cpu_count: Option<NonZeroU32>,
    /// Maximum memory per action.
    pub memory: Option<NonZeroBytes>,
    /// Maximum disk per action.
    pub disk: Option<NonZeroBytes>,
}

impl ResourceCeilings {
    /// Limits the physical resources to the configured action ceilings.
    #[must_use]
    pub fn effective(&self, instance: Resources) -> Resources {
        Resources {
            vcpus: self
                .cpu_count
                .map_or(instance.vcpus, |v| instance.vcpus.min(v.get())),
            memory: self.memory.map_or(instance.memory, |v| {
                Bytes::new(instance.memory.get().min(v.as_u64()))
            }),
            disk: self.disk.map_or(instance.disk, |v| {
                Bytes::new(instance.disk.get().min(v.as_u64()))
            }),
        }
    }
}

/// Immutable action policy for a fleet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FleetActionPolicy {
    /// Capabilities granted to actions.
    pub grants: BTreeSet<Capability>,
    /// Per-action resource ceilings.
    pub resources: ResourceCeilings,
}

/// An invalid fleet action policy.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActionPolicyError {
    /// A ceiling exceeds physical capacity.
    #[error("{resource} action ceiling exceeds physical capacity")]
    ExceedsCapacity {
        /// The resource whose ceiling exceeds capacity.
        resource: &'static str,
    },
    /// A size is not representable in whole KiB.
    #[error("{resource} action ceiling must be a whole number of KiB")]
    NotWholeKib {
        /// The resource whose ceiling is not a whole KiB.
        resource: &'static str,
    },
}

impl FleetActionPolicy {
    /// Validates the configured ceilings against the physical instance.
    ///
    /// # Errors
    /// Returns an error for ceilings above capacity or fractional KiB sizes.
    pub fn validate(&self, instance: Resources) -> Result<(), ActionPolicyError> {
        if self
            .resources
            .cpu_count
            .is_some_and(|v| v.get() > instance.vcpus)
        {
            return Err(ActionPolicyError::ExceedsCapacity {
                resource: "cpu_count",
            });
        }
        for (resource, value, capacity) in [
            ("memory", self.resources.memory, instance.memory),
            ("disk", self.resources.disk, instance.disk),
        ] {
            if let Some(value) = value {
                if value.as_u64() % 1024 != 0 {
                    return Err(ActionPolicyError::NotWholeKib { resource });
                }
                if value.as_u64() > capacity.get() {
                    return Err(ActionPolicyError::ExceedsCapacity { resource });
                }
            }
        }
        Ok(())
    }
}

const fn v4(a: u8, b: u8, c: u8, d: u8, prefix: u8) -> IpNet {
    IpNet::new_assert(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), prefix)
}
const fn v6(segments: [u16; 8], prefix: u8) -> IpNet {
    IpNet::new_assert(
        IpAddr::V6(Ipv6Addr::new(
            segments[0],
            segments[1],
            segments[2],
            segments[3],
            segments[4],
            segments[5],
            segments[6],
            segments[7],
        )),
        prefix,
    )
}

/// IPv4 ranges never permitted as public action destinations.
pub const FORBIDDEN_IPV4: [IpNet; 15] = [
    v4(0, 0, 0, 0, 8),
    v4(10, 0, 0, 0, 8),
    v4(100, 64, 0, 0, 10),
    v4(127, 0, 0, 0, 8),
    v4(169, 254, 0, 0, 16),
    v4(172, 16, 0, 0, 12),
    v4(192, 0, 0, 0, 24),
    v4(192, 0, 2, 0, 24),
    v4(192, 88, 99, 0, 24),
    v4(192, 168, 0, 0, 16),
    v4(198, 18, 0, 0, 15),
    v4(198, 51, 100, 0, 24),
    v4(203, 0, 113, 0, 24),
    v4(224, 0, 0, 0, 4),
    v4(240, 0, 0, 0, 4),
];
/// IPv6 public unicast prefix; addresses outside it are never public action destinations.
pub const PUBLIC_IPV6: IpNet = v6([0x2000, 0, 0, 0, 0, 0, 0, 0], 3);
/// Exceptions excluded from the public IPv6 unicast prefix.
pub const FORBIDDEN_IPV6: [IpNet; 4] = [
    v6([0x2001, 0, 0, 0, 0, 0, 0, 0], 23),
    v6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0], 32),
    v6([0x2002, 0, 0, 0, 0, 0, 0, 0], 16),
    v6([0x3fff, 0, 0, 0, 0, 0, 0, 0], 20),
];

/// Whether an address is a permitted public destination before deployment-specific exclusions.
#[must_use]
pub fn is_public_destination(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(_) => !FORBIDDEN_IPV4.iter().any(|net| net.contains(&address)),
        IpAddr::V6(_) => {
            PUBLIC_IPV6.contains(&address)
                && !FORBIDDEN_IPV6.iter().any(|net| net.contains(&address))
        }
    }
}

/// Public egress configuration shared with trusted sandbox setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicEgressPolicy {
    /// Deployment networks actions must never reach.
    pub protected_networks: Box<[IpNet]>,
    /// Literal public DNS resolvers outside protected networks.
    pub dns_servers: Box<[IpAddr]>,
    /// Deadline for setting up an action's network.
    #[serde(default = "default_setup_timeout")]
    pub setup_timeout: NonZeroDuration,
}

const fn default_setup_timeout() -> NonZeroDuration {
    NonZeroDuration::from_secs(10)
}

/// An invalid public egress policy.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EgressPolicyError {
    /// The protected network list is empty.
    #[error("protected networks must not be empty")]
    EmptyProtectedNetworks,
    /// The DNS server list is empty.
    #[error("dns servers must not be empty")]
    EmptyDnsServers,
    /// A DNS server is not a public address.
    #[error("dns server {0} is not public")]
    NonPublicDns(IpAddr),
    /// A DNS server belongs to a protected network.
    #[error("dns server {0} belongs to a protected network")]
    ProtectedDns(IpAddr),
}

impl PublicEgressPolicy {
    /// Validates resolver addresses and the required protected network list.
    ///
    /// # Errors
    /// Returns an error for empty lists, non-public DNS or protected DNS addresses.
    pub fn validate(&self) -> Result<(), EgressPolicyError> {
        if self.protected_networks.is_empty() {
            return Err(EgressPolicyError::EmptyProtectedNetworks);
        }
        if self.dns_servers.is_empty() {
            return Err(EgressPolicyError::EmptyDnsServers);
        }
        for &dns in &*self.dns_servers {
            if !is_public_destination(dns) {
                return Err(EgressPolicyError::NonPublicDns(dns));
            }
            if self.protected_networks.iter().any(|net| net.contains(&dns)) {
                return Err(EgressPolicyError::ProtectedDns(dns));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance() -> Resources {
        Resources {
            vcpus: 8,
            memory: Bytes::new(8 << 30),
            disk: Bytes::new(20 << 30),
        }
    }

    #[test]
    fn network_request_uses_one_closed_parser() {
        for request in [NetworkRequest::NotRequested, NetworkRequest::Requested] {
            assert_eq!(request.as_str().parse::<NetworkRequest>(), Ok(request));
        }
        let error = "networked".parse::<NetworkRequest>().unwrap_err();
        assert_eq!(error.kind(), "network request");
        assert_eq!(error.expected(), &["not-requested", "requested"]);
    }

    #[test]
    fn strict_policy_parsing_and_validation() {
        let policy: FleetActionPolicy = toml::from_str(
            "grants = [\"network\", \"network\"]\n[resources]\ncpu_count = 4\nmemory = \"2GiB\"\n",
        )
        .unwrap();
        assert_eq!(policy.grants.len(), 1);
        assert_eq!(policy.validate(instance()), Ok(()));
        assert_eq!(policy.resources.effective(instance()).vcpus, 4);
        assert_eq!(
            policy.resources.effective(instance()).memory,
            Bytes::new(2 << 30)
        );
        for input in [
            "grants = [\"other\"]",
            "unknown = true",
            "[resources]\nunknown = 1",
            "[resources]\ncpu_count = 0",
            "[resources]\nmemory = \"0B\"",
        ] {
            assert!(
                toml::from_str::<FleetActionPolicy>(input).is_err(),
                "{input}"
            );
        }
        let too_large: FleetActionPolicy = toml::from_str("[resources]\ncpu_count = 9").unwrap();
        assert!(matches!(
            too_large.validate(instance()),
            Err(ActionPolicyError::ExceedsCapacity {
                resource: "cpu_count"
            })
        ));
        let fractional: FleetActionPolicy =
            toml::from_str("[resources]\nmemory = \"1025B\"").unwrap();
        assert!(matches!(
            fractional.validate(instance()),
            Err(ActionPolicyError::NotWholeKib { resource: "memory" })
        ));
        let too_large: FleetActionPolicy = toml::from_str("[resources]\ndisk = \"21GiB\"").unwrap();
        assert!(matches!(
            too_large.validate(instance()),
            Err(ActionPolicyError::ExceedsCapacity { resource: "disk" })
        ));
    }

    #[test]
    fn resolver_policy_requires_public_unprotected_addresses() {
        let text = "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"8.8.8.8\"]";
        let policy: PublicEgressPolicy = toml::from_str(text).unwrap();
        assert_eq!(policy.setup_timeout, NonZeroDuration::from_secs(10));
        assert_eq!(policy.validate(), Ok(()));
        for input in [
            "protected_networks = []\ndns_servers = [\"8.8.8.8\"]",
            "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = []",
            "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"10.1.1.1\"]",
            "protected_networks = [\"8.8.8.0/24\"]\ndns_servers = [\"8.8.8.8\"]",
        ] {
            assert!(
                toml::from_str::<PublicEgressPolicy>(input)
                    .unwrap()
                    .validate()
                    .is_err(),
                "{input}"
            );
        }
        for input in [
            "protected_networks = [\"not-a-network\"]\ndns_servers = [\"8.8.8.8\"]",
            "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"dns.example\"]",
            "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"8.8.8.8\"]\nsetup_timeout = \"0s\"",
            "protected_networks = [\"10.0.0.0/8\"]\ndns_servers = [\"8.8.8.8\"]\nextra = true",
        ] {
            assert!(
                toml::from_str::<PublicEgressPolicy>(input).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn public_address_ranges_and_translation_boundaries() {
        for address in [
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.169.254",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "fc00::1",
            "fe80::1",
            "2001::1",
            "2001:db8::1",
            "2002::1",
            "3fff::1",
        ] {
            assert!(
                !is_public_destination(address.parse().unwrap()),
                "{address}"
            );
        }
        for address in [
            "1.1.1.1",
            "100.63.255.255",
            "100.128.0.0",
            "172.32.0.0",
            "2001:4860:4860::8888",
            "2606:4700:4700::1111",
            "4000::1",
        ] {
            let expected = address != "4000::1";
            assert_eq!(
                is_public_destination(address.parse().unwrap()),
                expected,
                "{address}"
            );
        }
    }
}
