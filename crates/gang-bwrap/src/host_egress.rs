//! Host-wide enforcement and root-owned attestation for action public egress.

use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::net::{IpAddr, ToSocketAddrs};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use gang_types::{
    FORBIDDEN_IPV4, FORBIDDEN_IPV6, PUBLIC_IPV6, PublicEgressPolicy, Url, is_public_destination,
};
use serde::{Deserialize, Serialize};

/// Root-owned record published only after the host rules are installed.
pub const HOST_EGRESS_POLICY_PATH: &str = "/run/gang/action-egress.json";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostEgressPolicy {
    policy: PublicEgressPolicy,
    controller_endpoints: Vec<Url>,
    cgroup: PathBuf,
    cgroup_id: u64,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

fn trusted(path: &Path, directory: bool) -> io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.uid() != 0
        || meta.mode() & 0o022 != 0
        || (directory && !meta.is_dir())
        || (!directory && !meta.is_file())
    {
        return Err(invalid(format!(
            "untrusted host egress path {}",
            path.display()
        )));
    }
    Ok(())
}

fn endpoint_host(endpoint: &Url) -> io::Result<&str> {
    let host = endpoint
        .host_str()
        .ok_or_else(|| invalid("endpoint has no host"))?;
    Ok(host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host))
}

fn addresses_protected(
    policy: &PublicEgressPolicy,
    addresses: impl Iterator<Item = IpAddr>,
) -> io::Result<()> {
    let mut addresses = addresses.peekable();
    if addresses.peek().is_none() {
        return Err(invalid("endpoint resolved to no addresses"));
    }
    for address in addresses {
        if is_public_destination(address)
            && !policy
                .protected_networks
                .iter()
                .any(|net| net.contains(&address))
        {
            return Err(invalid(format!(
                "endpoint address {address} is not protected"
            )));
        }
    }
    Ok(())
}

fn endpoints_protected(policy: &PublicEgressPolicy, endpoints: &[Url]) -> io::Result<()> {
    for endpoint in endpoints {
        let host = endpoint_host(endpoint)?;
        match host.parse() {
            Ok(address) => addresses_protected(policy, std::iter::once(address))?,
            Err(_) => {
                addresses_protected(policy, (host, 0).to_socket_addrs()?.map(|addr| addr.ip()))?;
            }
        }
    }
    Ok(())
}

fn rules(policy: &PublicEgressPolicy, cgroup: &Path) -> io::Result<String> {
    let relative = cgroup
        .strip_prefix(CGROUP_ROOT)
        .map_err(|_| invalid("cgroup outside cgroup v2 root"))?;
    let name = relative
        .to_str()
        .ok_or_else(|| invalid("non-UTF-8 cgroup path"))?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
    {
        return Err(invalid("invalid cgroup path"));
    }
    let level = relative.components().count();
    let mut text = format!(
        "add table inet gang_action_egress\ndelete table inet gang_action_egress\ntable inet gang_action_egress {{\n chain output {{ type filter hook output priority 0; policy accept;\n  socket cgroupv2 level {level} \"{name}\" jump public_egress\n }}\n chain public_egress {{\n  fib daddr type local reject with icmpx type admin-prohibited\n"
    );
    for network in FORBIDDEN_IPV4 {
        writeln!(
            text,
            "  ip daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(io::Error::other)?;
    }
    writeln!(
        text,
        "  ip6 daddr != {PUBLIC_IPV6} reject with icmpx type admin-prohibited"
    )
    .map_err(io::Error::other)?;
    for network in FORBIDDEN_IPV6 {
        writeln!(
            text,
            "  ip6 daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(io::Error::other)?;
    }
    for network in &policy.protected_networks {
        let family = if network.addr().is_ipv4() {
            "ip"
        } else {
            "ip6"
        };
        writeln!(
            text,
            "  {family} daddr {network} reject with icmpx type admin-prohibited"
        )
        .map_err(io::Error::other)?;
    }
    text.push_str(
        "  meta l4proto { tcp, udp } accept\n  reject with icmpx type admin-prohibited\n }\n}\n",
    );
    Ok(text)
}

/// Installs the worker's static host policy and publishes its attestation after success.
///
/// # Errors
/// Returns an error if the configuration, cgroup, nft transaction or attestation is invalid.
pub fn install_host_egress(
    policy: Option<&PublicEgressPolicy>,
    controller_endpoints: &[Url],
    cgroup: &Path,
    nft: &Path,
) -> io::Result<()> {
    let record = Path::new(HOST_EGRESS_POLICY_PATH);
    let dir = record
        .parent()
        .ok_or_else(|| invalid("invalid record path"))?;
    fs::create_dir_all(dir)?;
    trusted(dir, true)?;
    match fs::remove_file(record) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error),
    }
    let Some(policy) = policy else {
        return Ok(());
    };
    if !nft.is_absolute() {
        return Err(invalid("nft path must be absolute"));
    }
    policy.validate().map_err(io::Error::other)?;
    endpoints_protected(policy, controller_endpoints)?;
    let cgroup = fs::canonicalize(cgroup)?;
    if !fs::metadata(&cgroup)?.is_dir() {
        return Err(invalid("cgroup is not a directory"));
    }
    let cgroup_id = fs::metadata(&cgroup)?.ino();
    let script = rules(policy, &cgroup)?;
    let mut child = Command::new(nft)
        .args(["-f", "-"])
        .env_clear()
        .stdin(Stdio::piped())
        .spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("nft stdin unavailable"))?;
    let result = stdin.write_all(script.as_bytes());
    drop(stdin);
    let status = child.wait()?;
    result?;
    if !status.success() {
        return Err(io::Error::other(format!("nft failed: {status}")));
    }
    let state = HostEgressPolicy {
        policy: policy.clone(),
        controller_endpoints: controller_endpoints.to_vec(),
        cgroup,
        cgroup_id,
    };
    let data = serde_json::to_vec(&state).map_err(io::Error::other)?;
    let temporary = dir.join(format!(".action-egress.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(&data)?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o644))?;
        fs::rename(&temporary, record)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Verifies the static policy and the action's direct membership before starting pasta.
/// Changed controller endpoints are resolved asynchronously before their addresses are checked.
///
/// # Errors
/// Returns an error when the trusted record, action cgroup or endpoint protection does not match.
pub async fn verify_host_egress(
    policy: &PublicEgressPolicy,
    controller_endpoints: &[Url],
    action_cgroup: &Path,
) -> io::Result<()> {
    let record = Path::new(HOST_EGRESS_POLICY_PATH);
    trusted(
        record
            .parent()
            .ok_or_else(|| invalid("invalid record path"))?,
        true,
    )?;
    trusted(record, false)?;
    let state: HostEgressPolicy =
        serde_json::from_slice(&fs::read(record)?).map_err(io::Error::other)?;
    if &state.policy != policy || !action_cgroup.is_absolute() {
        return Err(invalid("host egress policy mismatch"));
    }
    let action = fs::canonicalize(action_cgroup)?;
    if action != action_cgroup
        || action.parent() != Some(state.cgroup.as_path())
        || fs::metadata(&state.cgroup)?.ino() != state.cgroup_id
    {
        return Err(invalid("action cgroup does not match host egress policy"));
    }
    if state.controller_endpoints != controller_endpoints {
        for endpoint in controller_endpoints {
            let host = endpoint_host(endpoint)?;
            match host.parse() {
                Ok(address) => addresses_protected(policy, std::iter::once(address))?,
                Err(_) => addresses_protected(
                    policy,
                    tokio::net::lookup_host((host, 0))
                        .await?
                        .map(|addr| addr.ip()),
                )?,
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_addresses_require_complete_protection() {
        let policy: PublicEgressPolicy =
            toml::from_str("protected_networks = [\"8.8.4.0/24\"]\ndns_servers = [\"1.1.1.1\"]")
                .unwrap();
        let protected =
            ["127.0.0.1", "::1", "8.8.4.4"].map(|address| address.parse::<IpAddr>().unwrap());
        assert!(addresses_protected(&policy, protected.into_iter()).is_ok());
        let mixed = ["8.8.4.4", "8.8.8.8"].map(|address| address.parse::<IpAddr>().unwrap());
        assert!(addresses_protected(&policy, mixed.into_iter()).is_err());
        assert!(addresses_protected(&policy, std::iter::empty()).is_err());
    }

    #[test]
    fn literal_endpoint_hosts_preserve_ipv4_and_ipv6() {
        let policy: PublicEgressPolicy =
            toml::from_str("protected_networks = [\"8.8.4.0/24\"]\ndns_servers = [\"1.1.1.1\"]")
                .unwrap();
        let endpoints = ["https://127.0.0.1", "https://[::1]", "https://8.8.4.4"]
            .map(|endpoint| Url::parse(endpoint).unwrap());
        assert_eq!(endpoint_host(&endpoints[1]).unwrap(), "::1");
        assert!(endpoints_protected(&policy, &endpoints).is_ok());
        assert!(endpoints_protected(&policy, &[Url::parse("https://8.8.8.8").unwrap()]).is_err());
    }

    #[test]
    fn rejects_protected_before_allowing_transport() {
        let policy: PublicEgressPolicy =
            toml::from_str("protected_networks = [\"8.8.4.0/24\"]\ndns_servers = [\"1.1.1.1\"]")
                .unwrap();
        let text = rules(
            &policy,
            Path::new("/sys/fs/cgroup/system.slice/gang-worker.service/actions"),
        )
        .unwrap();
        assert!(
            text.contains("socket cgroupv2 level 3 \"system.slice/gang-worker.service/actions\"")
        );
        assert!(
            text.find("fib daddr type local reject").unwrap() < text.find("meta l4proto").unwrap()
        );
        assert!(
            text.find("ip daddr 8.8.4.0/24 reject").unwrap() < text.find("meta l4proto").unwrap()
        );
        assert!(!text.contains("oifname"));
    }
}
