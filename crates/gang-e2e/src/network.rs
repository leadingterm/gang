//! The network scenario: an action that requests the network shares the host's network namespace
//! exactly when the configuration permits it, and an action that does not request it stays
//! isolated.

use crate::check::Check;
use crate::errors::Result;
use crate::report::Namespaces;
use crate::system::{Probe, System, Variant};
use crate::verdict;

/// Runs the network scenario against `system` under `variant`, and returns its verdicts in the
/// order reached. The caller chooses `variant.permit_network`.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn network<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    system.start_server()?;
    let outcome = system.build(&[Probe::Isolated, Probe::NetworkRequested])?;
    checks.push(verdict::remote_execution(
        &outcome,
        2,
        system.stderr_tail_lines(),
    ));

    let isolated = Namespaces::try_from(&system.report(Probe::Isolated)?)?;
    let requested = Namespaces::try_from(&system.report(Probe::NetworkRequested)?)?;
    checks.extend(verdict::network(
        variant.sandbox,
        variant.permit_network,
        system.host(),
        &isolated,
        &requested,
    ));

    Ok(checks)
}
