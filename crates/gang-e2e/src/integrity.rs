//! The input-integrity scenario: an action that tries to modify a cached input must not change
//! what a later action sees, unless the workspace deliberately keeps its input links.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::report::{PoisonReport, VictimReport};
use crate::system::{Probe, System, Variant};
use crate::verdict;

/// Builds the poisoning action and the action that runs after it, and judges whether the later
/// action saw the modified input, as the variant's `input_links` predicts.
///
/// # Errors
///
/// When the server cannot be started, Bazel cannot be run, or a probe report cannot be read.
pub fn input_integrity<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::Victim])?;
    let mut checks = vec![Check::expect(
        names::integrity::BUILD_SUCCEEDS,
        outcome.succeeded(),
        || outcome.exit_summary(),
    )];
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::integrity::ONE_WORKER,
        workers == 1,
        || format!("{workers} workers ran the actions"),
    ));
    let poison = PoisonReport::try_from(&system.report(Probe::Poison)?)?;
    checks.push(verdict::poison_attempted(&poison));
    let victim = VictimReport::try_from(&system.report(Probe::Victim)?)?;
    checks.push(verdict::input_integrity(variant.input_links, &victim));
    Ok(checks)
}
