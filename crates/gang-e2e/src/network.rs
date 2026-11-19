//! Admission and behavioral checks for isolated public egress.

use crate::check::Check;
use crate::egress_wire::EgressTargets;
use crate::errors::Result;
use crate::names;
use crate::report::ProbeReport;
use crate::system::{PermitNetwork, Probe, Sandbox, System, Variant};
use crate::verdict;

/// Runs the network scenario for a denied or granted fleet.
///
/// # Errors
/// Returns an error when the system cannot be driven or its reports cannot be read.
pub fn network<S: System>(
    system: &S,
    variant: &Variant,
    targets: &EgressTargets,
) -> Result<Vec<Check>> {
    if variant.sandbox == Sandbox::Noop {
        return Ok(vec![Check::skipped(
            names::network::SUPPORTED,
            "the noop sandbox does not isolate",
        )]);
    }
    system.start_server()?;
    if variant.permit_network == PermitNetwork::Denied {
        let outcome = system.build(&[Probe::NetworkRequested])?;
        let diagnostic = outcome.stderr.to_ascii_lowercase().replace('_', " ");
        return Ok(vec![Check::expect(
            names::network::ADMISSION,
            !outcome.succeeded()
                && diagnostic.contains("invalid argument")
                && diagnostic.contains("network")
                && diagnostic.contains("fleet")
                && system.report(Probe::NetworkRequested).is_err(),
            || outcome.exit_summary(),
        )]);
    }
    let outcome = system.build(&[Probe::NetworkIsolated, Probe::NetworkRequested])?;
    let mut checks = Vec::new();
    if !crate::check::record_build(
        &mut checks,
        &outcome,
        verdict::remote_execution(&outcome, 2, system.stderr_tail_lines()),
    ) {
        return Ok(checks);
    }
    let isolated = system.report(Probe::NetworkIsolated)?;
    let requested = system.report(Probe::NetworkRequested)?;
    for (label, report, online) in [("offline", &isolated, false), ("granted", &requested, true)] {
        let net = report.get("net")?;
        let host = system.host().net.as_deref();
        let valid = net
            .strip_prefix("net:[")
            .and_then(|s| s.strip_suffix(']'))
            .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
        checks.push(Check::expect(
            format!("{label} action has its own network namespace"),
            valid && host.is_some_and(|host| net != host),
            || format!("action namespace {net}, host namespace {host:?}"),
        ));
        for (key, expected) in [
            ("uid", targets.uid.to_string()),
            ("gid", targets.gid.to_string()),
        ] {
            let value = report.get(key)?;
            checks.push(Check::expect(
                format!("{label} action retains nonzero {key}"),
                expected != "0" && value == expected,
                || format!("expected nonzero {expected}, got {value}"),
            ));
        }
        for key in ["loopback4", "loopback6", "fd_leak"] {
            check_value(&mut checks, label, report, key, true)?;
        }
        let mutation = report.get("nft_mutation")?;
        checks.push(Check::expect(
            format!("{label} action cannot modify nftables"),
            mutation == "permission_denied",
            || format!("probe reported {mutation}"),
        ));
        for key in ["public4", "public6", "udp4", "udp6", "dns_public"] {
            check_value(&mut checks, label, report, key, online)?;
        }
        check_value(&mut checks, label, report, "dns_private", false)?;
        for key in
            targets
                .blocked
                .keys()
                .map(String::as_str)
                .chain(["alias4", "dns_alias4", "alias6"])
        {
            for protocol in ["tcp", "udp"] {
                check_value(
                    &mut checks,
                    label,
                    report,
                    &format!("blocked_{key}_{protocol}"),
                    false,
                )?;
            }
        }
    }
    Ok(checks)
}

fn check_value(
    checks: &mut Vec<Check>,
    label: &str,
    report: &ProbeReport,
    key: &str,
    allowed: bool,
) -> Result<()> {
    let value = report.get(key)?;
    let passed = if allowed {
        value == "ok"
    } else {
        matches!(
            value,
            "permission_denied"
                | "network_unreachable"
                | "host_unreachable"
                | "connection_refused"
                | "connection_reset"
                | "address_not_available"
                | "not_connected"
        )
    };
    checks.push(Check::expect(
        format!(
            "{label} {key} {}",
            if allowed { "succeeds" } else { "is blocked" }
        ),
        passed,
        || format!("probe reported {value}"),
    ));
    Ok(())
}
