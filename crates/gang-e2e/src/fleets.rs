//! The fleets scenario: a deployment with three fleets places each action on the fleet its exec
//! properties select, and overflows to the next fleet when the first is at its instance limit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use gang_wire::State;

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::control::{Control, latest_holds};
use crate::drain::{DrainChecks, settle};
use crate::errors::{E2eError, Result};
use crate::event_log::sequence;
use crate::names;
use crate::report::Namespaces;
use crate::system::{LARGE_FLEET, NOOP_FLEET, Probe, SMALL_FLEET, Sandbox, System, Variant};
use crate::verdict;

/// How the actions of a step reach the passes that place them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arrival {
    /// As the build sends them.
    AsSent,
    /// With initially empty history, all at once: every pass is skipped until both
    /// actions are queued, so the first placing pass sees both before either can execute.
    Together,
}

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
    /// How the build's actions reach the passes.
    arrival: Arrival,
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

/// The serial builds, each starting from a drained deployment.
const DRAINED_STARTS: [Step; 4] = [
    Step {
        request: names::fleets::UNCONSTRAINED,
        probes: &[Probe::Unconstrained],
        fleets: &[SMALL_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::Unconstrained),
        arrival: Arrival::AsSent,
    },
    Step {
        request: names::fleets::PINNED,
        probes: &[Probe::FleetRequested],
        fleets: &[LARGE_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::FleetRequested),
        arrival: Arrival::AsSent,
    },
    Step {
        request: names::fleets::MEMORY,
        probes: &[Probe::MemoryRequested],
        fleets: &[LARGE_FLEET],
        native_noop: None,
        observed: Observed::Native(Probe::MemoryRequested),
        arrival: Arrival::AsSent,
    },
    Step {
        request: names::fleets::NOOP_SANDBOX,
        probes: &[Probe::SandboxRequested],
        fleets: &[NOOP_FLEET],
        native_noop: Some(&[SMALL_FLEET]),
        observed: Observed::Noop(Probe::SandboxRequested),
        arrival: Arrival::AsSent,
    },
];

// Each cold action reserves the small fleet's full physical CPU at factor one.
// One pass sees both queued; the small fleet's one-instance limit forces the second
// action to overflow onto the next declared fleet. A second arrival after the first
// completed could reuse the small worker.
const COLD_PAIR: Step = Step {
    request: names::fleets::OVERFLOW,
    probes: &[Probe::OverflowFirst, Probe::OverflowSecond],
    fleets: &[SMALL_FLEET, LARGE_FLEET],
    native_noop: None,
    observed: Observed::Ran,
    arrival: Arrival::Together,
};

/// Runs the initial zero-worker check and the four serial drained-start builds.
///
/// `system` declares three fleets, in preference order: a small one of one instance, a large one
/// of two instances, and one under the noop sandbox. Every step builds its probes from a drained
/// deployment, and the scale-outs it caused are the ones recorded between the history read before
/// the build and the history read after it. The scale-outs must be exactly those of the fleets
/// the step's exec properties select. Where the variant's own sandbox is noop, the first two
/// fleets run under it too, so a request for the noop sandbox fits the first fleet.
///
/// The caller combines these verdicts with [`run_cold_pair`], in that order, using separate
/// deployments. This function starts its server and leaves it running for caller-owned cleanup.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn run_drained_starts<S: Control>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    system.start_server()?;
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::fleets::START_AT_ZERO,
        workers == 0,
        || format!("{workers} workers were running before the first build"),
    ));

    for step in &DRAINED_STARTS {
        run_step(system, variant, step, &mut checks)?;
    }
    Ok(checks)
}

/// Runs only the cold overflow pair and its drain checks on a fresh deployment.
///
/// The caller must supply a separate, never-used controller and history namespace with
/// [`ControlKnobs::FLEETS`](crate::ControlKnobs::FLEETS), whose cold CPU reservation
/// saturates the first worker. Supply is held until both actions are queued together.
/// This function starts its server and leaves it running for caller cleanup.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn run_cold_pair<S: Control>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = Vec::new();
    run_step(system, variant, &COLD_PAIR, &mut checks)?;
    Ok(checks)
}

/// Builds one request, judges its placement and report, then waits for its workers to drain.
fn run_step<S: Control>(
    system: &S,
    variant: &Variant,
    step: &Step,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let before = system.scaling_events()?;
    let since = before.last().map_or(Ok(0), sequence)?;
    let outcome = match step.arrival {
        Arrival::AsSent => system.build(step.probes)?,
        Arrival::Together => build_together(system, step.probes)?,
    };
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
        observe(system, variant, step, checks)?;
    }
    settle(
        system,
        since,
        DrainChecks {
            drained: names::fleets::drained(step.request).into(),
            terminated: names::fleets::terminated(step.request).into(),
        },
        checks,
    )
}

/// Builds `probes` while every pass is skipped, and lets the passes run again once the live state
/// shows all of their actions queued, or once the build ended without queueing them all. The
/// deployment is drained, so no worker can run an action before the first pass that places them.
///
/// # Errors
///
/// When the supply cannot be held or released, Bazel cannot be run, the build thread panics, or
/// the live state cannot be observed.
fn build_together<S: Control>(system: &S, probes: &[Probe]) -> Result<BuildOutcome> {
    let ended = AtomicBool::new(false);
    system.hold_supply()?;
    thread::scope(|scope| {
        let build = scope.spawn(|| {
            let outcome = system.build(probes);
            ended.store(true, Ordering::Release);
            outcome
        });
        let all_queued = system.await_stream(&mut |frames| {
            ended.load(Ordering::Acquire)
                || latest_holds(frames, |state| queued(state) >= probes.len())
        });
        let released = system.release_supply();
        let outcome = build
            .join()
            .map_err(|_| E2eError::system("the build thread panicked"))??;
        released?;
        // A failed build is judged by its own checks, whatever the state showed of it.
        if outcome.succeeded() {
            all_queued?;
        }
        Ok(outcome)
    })
}

/// The number of queued actions `state` lists.
fn queued(state: &State) -> usize {
    state
        .actions
        .as_ref()
        .map_or(0, |actions| actions.queued.len())
}

/// Judges what the probes of `step` reported.
fn observe<S: System>(
    system: &S,
    variant: &Variant,
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
