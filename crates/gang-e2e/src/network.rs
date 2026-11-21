//! Admission checks for the network capability.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::system::{Probe, System};

/// Checks that an ungranted network request is rejected before execution.
///
/// # Errors
/// Returns an error when the system cannot be driven.
pub fn network<S: System>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::NetworkRequested])?;
    let diagnostic = outcome.stderr.to_ascii_lowercase().replace('_', " ");
    Ok(vec![Check::expect(
        names::network::ADMISSION,
        !outcome.succeeded()
            && diagnostic.contains("invalid argument")
            && diagnostic.contains("network")
            && diagnostic.contains("fleet")
            && system.report(Probe::NetworkRequested).is_err(),
        || outcome.exit_summary(),
    )])
}
