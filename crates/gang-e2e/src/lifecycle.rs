//! The lifecycle scenario: a fleet starts empty, scales out for a cold build, drains when idle,
//! and serves a warm rebuild from the remote cache without starting a worker.

use gang_wire::{Event, EventChange};

use crate::check::Check;
use crate::drain::{DrainChecks, settle};
use crate::errors::{E2eError, Result};
use crate::names;
use crate::report::{IsolationReport, Namespaces};
use crate::system::{Probe, System, Variant};
use crate::verdict;

/// Runs the lifecycle scenario against `system` under `variant`, and returns its verdicts in the
/// order reached.
///
/// The scale-out baseline is read only after the fleet has drained and every drained worker is
/// recorded as terminated, so the warm rebuild is compared against a settled history.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn lifecycle<S: System>(system: &S, variant: &Variant) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    system.start_server()?;
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::lifecycle::STARTS_AT_ZERO,
        workers == 0,
        || format!("{workers} workers were running before the first build"),
    ));

    let cold = system.build(&[Probe::Isolated, Probe::NetworkRequested, Probe::Pinned])?;
    checks.push(verdict::remote_execution(
        &cold,
        3,
        system.stderr_tail_lines(),
    ));

    // Worker registrations are recorded by the next control pass.
    let events = match system
        .await_history(&mut |history| crate::event_log::connected_workers(history) >= 1)
    {
        Ok(events) => events,
        Err(E2eError::Timeout { .. }) => system.scaling_events()?,
        Err(error) => return Err(error),
    };
    checks.push(verdict::first_scale_out_from_zero(&events));
    let connected = crate::event_log::connected_workers(&events);
    checks.push(Check::expect(
        names::lifecycle::COLD_BUILD_ONE_WORKER,
        connected == 1,
        || {
            let mut worker_events = events
                .iter()
                .rev()
                .filter(|event| {
                    matches!(
                        event.change,
                        EventChange::WorkerRegistered { .. }
                            | EventChange::WorkerDeregistered { .. }
                    )
                })
                .take(20)
                .map(|event| {
                    format!(
                        "seq={:?} time={:?} change={:?}",
                        event.name, event.create_time, event.change
                    )
                })
                .collect::<Vec<_>>();
            worker_events.reverse();
            let mut reason = format!("{connected} workers connected after the cold build");
            for event in worker_events {
                reason.push('\n');
                reason.push_str(&event);
            }
            reason
        },
    ));

    let isolated = IsolationReport::try_from(&system.report(Probe::Isolated)?)?;
    let requested = Namespaces::try_from(&system.report(Probe::NetworkRequested)?)?;
    checks.extend(verdict::namespaces(
        variant.sandbox,
        system.host(),
        &isolated.namespaces,
    ));
    checks.push(verdict::siblings_hidden(variant.sandbox, &isolated));
    checks.extend(verdict::working_directory(system.tmp_path(), &isolated));
    checks.extend(verdict::network(
        variant.sandbox,
        variant.permit_network,
        system.host(),
        &isolated.namespaces,
        &requested,
    ));

    let failing = system.build(&[Probe::ExitThree])?;
    checks.push(verdict::exit_status(&failing, 3));

    settle(
        system,
        0,
        DrainChecks {
            drained: names::lifecycle::IDLE_DRAIN.into(),
            terminated: names::lifecycle::EVERY_DRAIN_TERMINATED.into(),
        },
        &mut checks,
    )?;
    let events = system.scaling_events()?;
    checks.push(verdict::worker_ids(&events));
    let baseline = scaled_out_count(&events);

    system.clean()?;
    let warm = system.build(&[Probe::Isolated, Probe::NetworkRequested, Probe::Pinned])?;
    checks.push(verdict::cache_hit(&warm, 3, system.stderr_tail_lines()));
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::lifecycle::CACHE_HIT_NEEDS_NO_WORKER,
        workers == 0,
        || format!("{workers} workers were started for a cache hit"),
    ));
    checks.push(verdict::scale_outs(
        &system.scaling_events()?,
        names::lifecycle::WARM_REBUILD_DID_NOT_SCALE_OUT,
        baseline,
    ));

    Ok(checks)
}

/// The number of `ScaledOut` events in `events`.
fn scaled_out_count(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event.change, EventChange::ScaledOut { .. }))
        .count()
}
