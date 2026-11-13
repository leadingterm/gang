//! The input root scenario: an action whose command runs below its input root reads the inputs
//! beside its working directory.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::report::InputRootReport;
use crate::system::{Probe, System, Variant};
use crate::verdict;

/// Builds the input root probe on `system` and checks what it read, as `variant` predicts.
///
/// # Errors
///
/// When the server cannot start, Bazel cannot run, or the report cannot be read.
pub fn input_root<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::InputRoot])?;
    let mut checks = vec![Check::expect(
        names::input_root::BUILD_SUCCEEDS,
        outcome.succeeded(),
        || outcome.exit_summary(),
    )];
    let report = InputRootReport::try_from(&system.report(Probe::InputRoot)?)?;
    checks.extend(verdict::input_root(variant.input_links, &report));
    Ok(checks)
}
