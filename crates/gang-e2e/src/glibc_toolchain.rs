//! The glibc toolchain scenario: a dynamically linked glibc program, as the toolchains Bazel
//! downloads are (`rules_rust`'s rustc and cargo, prebuilt tools), starts inside the sandbox.

use crate::check::Check;
use crate::errors::Result;
use crate::names::glibc_toolchain;
use crate::system::{Probe, System};
use crate::verdict;

/// Runs the glibc toolchain scenario against `system`, and returns its verdicts in the order reached.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn glibc_toolchain<S: System>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::GlibcTool])?;
    let mut checks = vec![verdict::remote_execution(
        &outcome,
        1,
        system.stderr_tail_lines(),
    )];
    if !outcome.succeeded() {
        checks.push(Check::failed(
            glibc_toolchain::TOOL_RUNS,
            "the probe's build failed",
        ));
        return Ok(checks);
    }
    let report = system.report(Probe::GlibcTool)?;
    let state = report.get("glibc_tool")?;
    checks.push(Check::expect(
        glibc_toolchain::TOOL_RUNS,
        state == "ran",
        || format!("the program did not start: {state}"),
    ));
    Ok(checks)
}
