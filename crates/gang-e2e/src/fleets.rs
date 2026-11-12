//! The fleets scenario: a deployment with three fleets places each action on the fleet its exec
//! properties select, and overflows to the next fleet when the first is at its instance limit.

use crate::check::Check;
use crate::drain::{DrainChecks, settle};
use crate::errors::Result;
use crate::event_log::sequence;
use crate::names;
use crate::report::Namespaces;
use crate::system::{LARGE_FLEET, NOOP_FLEET, Probe, SMALL_FLEET, Sandbox, System, Variant};
use crate::verdict;

/// What a step reads from the report of its probes once they ran.
#[derive(Debug, Clone, Copy)]
enum Observed {
    /// The probe ran under the sandbox of the variant, which is isolated under bwrap.
    Native(Probe),
    /// The probe ran under the noop sandbox of the noop fleet.
    Noop(Probe),
    /// Nothing beyond the build: the actions only had to run.
    Ran,
}

/// One build of the scenario: the probes it builds, and the fleets that must scale out for them.
#[derive(Debug, Clone, Copy)]
struct Step {
    /// What the step's actions ask for, as it reads in the name of a check.
    request: &'static str,
    /// The probes of the build.
    probes: &'static [Probe],
    /// The fleets that scale out, once each and in this order, and no other.
    fleets: &'static [&'static str],
    /// The fleets that scale out instead when the variant's own sandbox is noop.
    native_noop: Option<&'static [&'static str]>,
    /// What the probes' reports must show.
    observed: Observed,
}

impl Step {
    /// The fleets that must scale out when the variant's own sandbox is `native`.
    fn expected(&self, native: Sandbox) -> &'static [&'static str] {
        match self.native_noop {
            Some(fleets) if native == Sandbox::Noop => fleets,
            _ => self.fleets,
        }
    }
}

/// The steps in the order they run. Each starts from a drained deployment.
const STEPS: [Step; 5] = [
    Step {
        request: names::fleets::UNCONSTRAINED,
        probes: &[Probe::Unconstrained],
        fleets: &[SMALL_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::Unconstrained),
    },
    Step {
        request: names::fleets::PINNED,
        probes: &[Probe::FleetRequested],
        fleets: &[LARGE_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::FleetRequested),
    },
    Step {
        request: names::fleets::MEMORY,
        probes: &[Probe::MemoryRequested],
        fleets: &[LARGE_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::MemoryRequested),
    },
    Step {
        request: names::fleets::NOOP_SANDBOX,
        probes: &[Probe::SandboxRequested],
        fleets: &[NOOP_FLEET],
        native_noop: Some(&[SMALL_FLEET]),
        observed: Observed::Noop(Probe::SandboxRequested),
    },
    Step {
        request: names::fleets::OVERFLOW,
        probes: &[Probe::OverflowFirst, Probe::OverflowSecond],
        fleets: &[SMALL_FLEET, LARGE_FLEET],
        native_noop: None,
        observed: Observed::Ran,
    },
];

/// Runs the fleets scenario against `system` under `variant`, and returns its verdicts in the
/// order reached.
///
/// `system` declares three fleets, in preference order: a small one of one instance, a large one
/// of two instances, and one under the noop sandbox. Every step builds its probes from a drained
/// deployment, and the scale-outs it caused are the ones recorded between the history read before
/// the build and the history read after it. The scale-outs must be exactly those of the fleets
/// the step's exec properties select. Where the variant's own sandbox is noop, the first two
/// fleets run under it too, so a request for the noop sandbox fits the first fleet.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn fleets<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    system.start_server()?;
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::fleets::START_AT_ZERO,
        workers == 0,
        || format!("{workers} workers were running before the first build"),
    ));

    for step in &STEPS {
        let before = system.scaling_events()?;
        let since = before.last().map_or(Ok(0), sequence)?;
        let outcome = system.build(step.probes)?;
        let actions = u32::try_from(step.probes.len()).unwrap_or(u32::MAX);
        checks.push(labelled(
            step.request,
            verdict::remote_execution(&outcome, actions, system.stderr_tail_lines()),
        ));
        let after = system.scaling_events()?;
        let expected = step.expected(variant.sandbox);
        checks.push(verdict::placed_on(
            names::fleets::placed(step.request, expected),
            &before,
            &after,
            expected,
        ));
        if outcome.succeeded() {
            observe(system, *variant, step, &mut checks)?;
        }
        settle(
            system,
            since,
            DrainChecks {
                drained: names::fleets::drained(step.request).into(),
                terminated: names::fleets::terminated(step.request).into(),
            },
            &mut checks,
        )?;
    }
    Ok(checks)
}

/// Judges what the probes of `step` reported.
fn observe<S: System>(
    system: &S,
    variant: Variant,
    step: &Step,
    checks: &mut Vec<Check>,
) -> Result<()> {
    match step.observed {
        Observed::Native(probe) => {
            let report = Namespaces::try_from(&system.report(probe)?)?;
            checks.extend(
                verdict::namespaces(variant.sandbox, system.host(), &report)
                    .into_iter()
                    .map(|check| labelled(step.request, check)),
            );
        }
        Observed::Noop(probe) => {
            let report = Namespaces::try_from(&system.report(probe)?)?;
            checks.push(labelled(
                step.request,
                verdict::noop_fleet_unisolated(variant.sandbox, system.host(), &report),
            ));
        }
        Observed::Ran => {}
    }
    Ok(())
}

/// `check` with the request of its step in front of its name, so that the same verdict of two
/// steps reads apart.
fn labelled(request: &str, check: Check) -> Check {
    Check {
        name: names::fleets::labelled(request, &check.name).into(),
        outcome: check.outcome,
    }
}
