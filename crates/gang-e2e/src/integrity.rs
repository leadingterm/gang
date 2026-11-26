//! The input-integrity scenario: an action that tries to modify a cached input must not change
//! what a later action sees, unless the workspace deliberately keeps its input links.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::report::{BoundTreeReport, PoisonReport, VictimReport};
use crate::system::{Probe, Sandbox, System, Variant};
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
    let mut checks = Vec::new();
    if !crate::check::record_build(
        &mut checks,
        &outcome,
        Check::expect(
            names::integrity::BUILD_SUCCEEDS,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ),
    ) {
        return Ok(checks);
    }
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

/// Builds the bound-tree poisoning probe under a variant with `input_trees = "bind"` and a low
/// `tree_cache_min_files` threshold, verifying that `chmod` and writes inside the bound subtree
/// fail with `EROFS` under `bwrap` (and skips cleanly under `noop`).
///
/// # Errors
///
/// When the server cannot be started, Bazel cannot be run, or the probe report cannot be read.
pub fn bound_tree_integrity<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    if variant.sandbox == Sandbox::Noop {
        return Ok(vec![Check::skipped(
            names::integrity::BOUND_TREE_READ_ONLY,
            "the noop sandbox does not mount read-only input trees",
        )]);
    }
    system.start_server()?;
    let outcome = system.build(&[Probe::BoundTreePoison])?;
    let mut checks = Vec::new();
    if !crate::check::record_build(
        &mut checks,
        &outcome,
        Check::expect(
            names::integrity::BUILD_SUCCEEDS,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ),
    ) {
        return Ok(checks);
    }
    let report = BoundTreeReport::try_from(&system.report(Probe::BoundTreePoison)?)?;
    checks.push(verdict::bound_tree_read_only(variant.sandbox, &report));
    Ok(checks)
}
