//! Privileged worker host policy installation.

use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::process::{Command, ExitCode};

use gang_types::PublicEgressPolicy;
use serde::Deserialize;

use crate::cli::PrepareActionEgressArgs;
use crate::config_file::LoadedConfig;
use crate::errors::{MainError, Result};

pub(crate) fn run(args: PrepareActionEgressArgs) -> Result<ExitCode> {
    // Invalidate any previous grant before inventory or configuration checks can fail.
    gang_bwrap::host_egress::install_host_egress(None, &[], &args.cgroup, &args.nft)
        .map_err(MainError::HostEgress)?;
    let config = LoadedConfig::load(&args.config)?.config;
    if let Some(policy) = &config.execution.egress {
        validate_host_addresses(&args.ip, policy).map_err(MainError::HostEgress)?;
    }
    let mut endpoints: Vec<_> = config.endpoint.into_iter().collect();
    if let Some(worker) = config.worker_endpoint {
        endpoints.push(worker);
    } else if config.execution.egress.is_some() {
        return Err(MainError::HostEgress(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker_endpoint is required",
        )));
    }
    gang_bwrap::host_egress::install_host_egress(
        config.execution.egress.as_ref(),
        &endpoints,
        &args.cgroup,
        &args.nft,
    )
    .map_err(MainError::HostEgress)?;
    Ok(ExitCode::SUCCESS)
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

fn validate_host_addresses(ip: &Path, policy: &PublicEgressPolicy) -> io::Result<()> {
    if !ip.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ip path must be absolute",
        ));
    }
    let output = Command::new(ip)
        .args(["-j", "address", "show"])
        .env_clear()
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "host address inventory failed: {}",
            output.status
        )));
    }
    let interfaces: Vec<Interface> =
        serde_json::from_slice(&output.stdout).map_err(io::Error::other)?;
    if interfaces.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty host interface inventory",
        ));
    }
    for address in interfaces
        .into_iter()
        .flat_map(|interface| interface.addr_info)
    {
        if !matches!(
            (address.family.as_str(), address.local),
            ("inet", IpAddr::V4(_)) | ("inet6", IpAddr::V6(_))
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid host address family",
            ));
        }
        if address.scope == "global"
            && !policy
                .protected_networks
                .iter()
                .any(|network| network.contains(&address.local))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "host address {} is not covered by a protected network",
                    address.local
                ),
            ));
        }
    }
    Ok(())
}
