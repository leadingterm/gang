//! The FHS tools scenario: a script with `#!/usr/bin/env bash` runs with Bazel's strict action
//! `PATH`, finding bash, sh and its tools at `/bin` and `/usr/bin`.

use crate::check::Check;
use crate::errors::Result;
use crate::names::fhs_tools;
use crate::system::{Probe, System};
use crate::verdict;

/// Runs the FHS tools scenario against `system`, and returns its verdicts in the order reached.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn fhs_tools<S: System>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::FhsTools])?;
    let mut checks = vec![verdict::remote_execution(
        &outcome,
        1,
        system.stderr_tail_lines(),
    )];
    if !outcome.succeeded() {
        checks.push(Check::failed(
            fhs_tools::SCRIPT_RUNS,
            "the probe's build failed",
        ));
        return Ok(checks);
    }
    let report = system.report(Probe::FhsTools)?;
    let state = report.get("fhs_tools")?;
    checks.push(Check::expect(
        fhs_tools::SCRIPT_RUNS,
        state == "ran",
        || format!("the script did not run: {state}"),
    ));
    Ok(checks)
}
