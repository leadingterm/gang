//! Compilation of trusted public-egress policy into an nftables output chain.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::process::Command;

use gang_spi::ActionExecutionSpec;
use gang_types::{
    FORBIDDEN_IPV4, FORBIDDEN_IPV6, PUBLIC_IPV6, PublicEgressPolicy, Url, is_public_destination,
};
use serde::Deserialize;

use crate::config::EgressPrograms;

const TABLE: &str = "gang_egress";
const INTERFACE: &str = "tap0";

/// Run trusted helpers without inheriting either the worker or action environment.
pub(crate) fn helper_command(program: &Path, launcher: &[OsString]) -> Command {
    let mut command = match launcher.split_first() {
        Some((prefix, args)) => {
            let mut command = Command::new(prefix);
            command.args(args).arg(program);
            command
        }
        None => Command::new(program),
    };
    command.env_clear();
    command
}

#[derive(Deserialize)]
struct Interface {
    addr_info: Vec<InterfaceAddress>,
}

#[derive(Deserialize)]
struct InterfaceAddress {
    family: String,
    local: IpAddr,
    scope: String,
}

fn inventory(json: &[u8]) -> io::Result<Vec<IpAddr>> {
    let interfaces: Vec<Interface> = serde_json::from_slice(json)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if interfaces.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty host interface inventory",
        ));
    }
    let mut addresses = Vec::new();
    for interface in interfaces {
        for address in interface.addr_info {
            let valid_family = matches!(
                (address.family.as_str(), address.local),
                ("inet", IpAddr::V4(_)) | ("inet6", IpAddr::V6(_))
            );
            if !valid_family {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid host address family",
                ));
            }
            if address.scope == "global" {
                addresses.push(address.local);
            }
        }
    }
    Ok(addresses)
}

fn require_protected(policy: &PublicEgressPolicy, address: IpAddr, source: &str) -> io::Result<()> {
    if policy
        .protected_networks
        .iter()
        .any(|network| network.contains(&address))
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{source} address {address} is not covered by a protected network"),
        ))
    }
}

/// Inspect only the local host and the explicitly supplied Gang endpoints.
pub(crate) async fn prepare_policy(
    helpers: &EgressPrograms,
    spec: &ActionExecutionSpec<'_>,
    policy: &PublicEgressPolicy,
    endpoints: &[Url],
) -> io::Result<String> {
    if !helpers.ip.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ip helper must be absolute",
        ));
    }
    let mut command = tokio::process::Command::from(helper_command(&helpers.ip, spec.launcher));
    command.args(["-j", "address", "show"]).kill_on_drop(true);
    let output = command.output().await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "ip address inventory failed: {}",
            output.status
        )));
    }
    let addresses = inventory(&output.stdout)?;
    for &address in &addresses {
        require_protected(policy, address, "host")?;
    }
    for endpoint in endpoints {
        let host = endpoint.host_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Gang endpoint has no host")
        })?;
        // URL's IPv6 host is bracketed; the resolver and IpAddr use unbracketed literals.
        let host = host
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host);
        let resolved: Vec<IpAddr> = if let Ok(address) = host.parse() {
            vec![address]
        } else {
            tokio::net::lookup_host((host, 0))
                .await?
                .map(|socket| socket.ip())
                .collect()
        };
        if resolved.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Gang endpoint resolved to no addresses",
            ));
        }
        for address in resolved {
            if is_public_destination(address) {
                require_protected(policy, address, "Gang endpoint")?;
            }
        }
    }
    rules(policy, &addresses)
}

/// Produce an output-only ruleset. All interpolated addresses are typed values.
pub(crate) fn rules(policy: &PublicEgressPolicy, host_addresses: &[IpAddr]) -> io::Result<String> {
    let mut result = String::new();
    let write_error = |error| io::Error::other(error);
    writeln!(result, "table inet {TABLE} {{").map_err(write_error)?;
    writeln!(
        result,
        " chain output {{ type filter hook output priority 0; policy drop;"
    )
    .map_err(write_error)?;
    // Loopback is namespace-local, and must precede the special-address rejects.
    writeln!(result, "  oifname \"lo\" ip daddr 127.0.0.0/8 accept").map_err(write_error)?;
    writeln!(result, "  oifname \"lo\" ip6 daddr ::1 accept").map_err(write_error)?;
    // Kernel-generated rejects to this namespace's own addresses must reach their sockets.
    writeln!(result, "  fib daddr type local accept").map_err(write_error)?;
    // Router/neighbor discovery and MLD are link control, not private data access.
    writeln!(result, "  oifname \"{INTERFACE}\" ip6 hoplimit 255 ip6 daddr {{ fe80::/10, ff02::/16 }} icmpv6 type {{ nd-router-solicit, nd-neighbor-solicit, nd-neighbor-advert }} accept").map_err(write_error)?;
    writeln!(result, "  oifname \"{INTERFACE}\" ip6 hoplimit 255 ip6 daddr fd00::2 icmpv6 type nd-neighbor-solicit accept").map_err(write_error)?;
    writeln!(
        result,
        "  oifname \"{INTERFACE}\" ip6 hoplimit 1 ip6 daddr ff02::16 icmpv6 type 143 accept"
    )
    .map_err(write_error)?;
    for network in FORBIDDEN_IPV4 {
        writeln!(
            result,
            "  ip daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(write_error)?;
    }
    writeln!(
        result,
        "  ip6 daddr != {PUBLIC_IPV6} reject with icmpx type admin-prohibited"
    )
    .map_err(write_error)?;
    for network in FORBIDDEN_IPV6 {
        writeln!(
            result,
            "  ip6 daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(write_error)?;
    }
    for address in host_addresses {
        let family = if address.is_ipv4() { "ip" } else { "ip6" };
        writeln!(
            result,
            "  {family} daddr {address} reject with icmpx type admin-prohibited"
        )
        .map_err(write_error)?;
    }
    for network in &policy.protected_networks {
        let family = if network.addr().is_ipv4() {
            "ip"
        } else {
            "ip6"
        };
        writeln!(
            result,
            "  {family} daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(write_error)?;
    }
    writeln!(result, "  meta l4proto {{ tcp, udp }} accept").map_err(write_error)?;
    writeln!(result, "  reject with icmpx type admin-prohibited\n }}\n}}").map_err(write_error)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::NonZeroDuration;

    fn policy() -> PublicEgressPolicy {
        PublicEgressPolicy {
            protected_networks: vec![
                "8.8.4.0/24".parse().unwrap(),
                "10.2.0.0/16".parse().unwrap(),
                "2606:4700::/32".parse().unwrap(),
            ]
            .into_boxed_slice(),
            dns_servers: vec!["1.1.1.1".parse().unwrap()].into_boxed_slice(),
            setup_timeout: NonZeroDuration::from_secs(10),
        }
    }

    #[test]
    fn nft_precedence_excludes_special_protected_and_host_destinations() {
        let text = rules(
            &policy(),
            &["9.9.9.9".parse().unwrap(), "2606:4860::1".parse().unwrap()],
        )
        .unwrap();
        let accept = text.find("meta l4proto { tcp, udp } accept").unwrap();
        let local = text.find("fib daddr type local accept").unwrap();
        assert!(local < accept);
        for (offset, _) in text.match_indices(" reject") {
            assert!(local < offset);
        }
        for rule in [
            "ip daddr 10.0.0.0/8",
            "ip daddr 169.254.0.0/16",
            "ip6 daddr != 2000::/3",
            "ip6 daddr 2001::/23",
            "ip6 daddr 2002::/16",
            "ip6 daddr 3fff::/20",
            "ip daddr 8.8.4.0/24",
            "ip daddr 10.2.0.0/16",
            "ip6 daddr 2606:4700::/32",
            "ip daddr 9.9.9.9",
            "ip6 daddr 2606:4860::1",
        ] {
            assert!(text.find(rule).unwrap() < accept, "{rule}");
        }
        assert!(
            text.find("oifname \"lo\" ip daddr 127.0.0.0/8 accept")
                .unwrap()
                < text.find("ip daddr 127.0.0.0/8 reject").unwrap()
        );
        assert!(
            text.find("icmpv6 type nd-neighbor-solicit accept").unwrap()
                < text.find("ip6 daddr != 2000::/3").unwrap()
        );
        assert!(text.contains("ip6 hoplimit 1 ip6 daddr ff02::16 icmpv6 type 143 accept"));
        assert!(!text.contains("saddr"));
        assert!(!text.contains("hook forward"));
        assert!(text.contains("policy drop"));
    }

    #[test]
    fn classifier_boundaries_and_translations_match_rules_inputs() {
        for address in ["8.8.8.8", "1.1.1.1", "2606:4860:4860::8888", "3ffe::1"] {
            assert!(is_public_destination(address.parse().unwrap()), "{address}");
        }
        for address in [
            "10.0.0.1",
            "169.254.169.254",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "fd00::1",
            "2001:db8::1",
            "2002:808:808::1",
            "3fff::1",
            "4000::1",
        ] {
            assert!(
                !is_public_destination(address.parse().unwrap()),
                "{address}"
            );
        }
    }

    #[test]
    fn inventory_is_strict_and_coverage_includes_private_global_addresses() {
        let json = br#"[{"addr_info":[{"family":"inet","local":"10.2.1.1","scope":"global"},{"family":"inet6","local":"::1","scope":"host"}]}]"#;
        let addresses = inventory(json).unwrap();
        assert_eq!(addresses, vec!["10.2.1.1".parse::<IpAddr>().unwrap()]);
        assert!(require_protected(&policy(), addresses[0], "host").is_ok());
        assert!(require_protected(&policy(), "9.9.9.9".parse().unwrap(), "host").is_err());
        assert!(require_protected(&policy(), "8.8.4.8".parse().unwrap(), "Gang endpoint").is_ok());
        assert!(
            inventory(
                br#"[{"addr_info":[{"family":"inet6","local":"1.1.1.1","scope":"global"}]}]"#
            )
            .is_err()
        );
        assert!(inventory(br#"[{"addr_info":[{"family":"inet","scope":"global"}]}]"#).is_err());
    }

    #[test]
    fn helper_clears_environment_and_preserves_launcher_prefix() {
        let command = helper_command(
            Path::new("/usr/bin/ip"),
            &["/launcher".into(), "scope".into()],
        );
        assert_eq!(command.get_program(), "/launcher");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["scope", "/usr/bin/ip"]
        );
        assert_eq!(command.get_envs().count(), 0);
    }
}
