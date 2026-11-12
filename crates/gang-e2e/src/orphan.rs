//! The orphan scenario: an in-service instance whose worker stops answering is flagged past its
//! boot grace, then terminated exactly once with cause `ORPHAN`.

use gang_wire::{Cause, Lifecycle, State};

use crate::check::Check;
use crate::control::{Control, latest_holds, states, streamed, streamed_events, terminates};
use crate::errors::{E2eError, Result};
use crate::names::orphan as names;

/// Runs the orphan scenario against a deployment with one standing instance (see
/// [`ControlKnobs::ORPHAN`](crate::ControlKnobs::ORPHAN)).
///
/// The standing worker is stopped with `SIGSTOP` once it connected: its process keeps the
/// instance alive, but the scheduler loses the worker, so the instance is in service without one.
/// Killing the worker instead would end the instance: the Local provisioner reaps a record whose
/// process is gone. Whatever the verdicts, every worker is resumed at the end, so none outlives
/// the deployment stopped.
///
/// # Errors
///
/// When the deployment cannot be started or observed, or does not stop exactly one worker.
pub fn orphan<S: Control>(system: &S) -> Result<Vec<Check>> {
    let checks = stopped_orphan(system);
    system.continue_workers()?;
    checks
}

/// The orphan scenario up to its verdicts; the stopped worker may still be stopped when it ends.
fn stopped_orphan<S: Control>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let connected = system
        .await_stream(&mut |frames| latest_holds(frames, |state| !state.workers.is_empty()))?;
    let instance = states(&connected)?
        .last()
        .and_then(|state| state.workers.first())
        .map(|worker| worker.instance.clone())
        .ok_or_else(|| E2eError::system("the state stream ended before a worker connected"))?;
    let stopped = system.stop_workers()?;
    if stopped != 1 {
        return Err(E2eError::system(format!(
            "expected to stop the one standing worker, stopped {stopped}"
        )));
    }

    let mut checks = Vec::new();
    let watched =
        system.await_stream(&mut |frames| streamed(frames, |event| terminates(event, &instance)));
    let frames = match watched {
        Ok(frames) => frames,
        Err(error @ E2eError::Timeout { .. }) => {
            checks.push(Check::failed(
                names::TERMINATED_AS_ORPHAN,
                gang_types::error_chain(&error).to_string(),
            ));
            checks.extend(Check::skipped_all(
                [names::FLAGGED_PAST_BOOT_GRACE, names::TERMINATED_ONCE],
                "the orphan was not terminated",
            ));
            return Ok(checks);
        }
        Err(error) => return Err(error),
    };
    let flagged = flagged_before_termination(&states(&frames)?, &instance);
    checks.push(Check::expect(
        names::FLAGGED_PAST_BOOT_GRACE,
        flagged,
        || {
            format!(
                "{instance} was never listed in service with pastBootGrace before its termination"
            )
        },
    ));
    let cause = streamed_events(&frames)?
        .into_iter()
        .find(|event| terminates(event, &instance))
        .and_then(|event| event.cause.clone());
    checks.push(Check::expect(
        names::TERMINATED_AS_ORPHAN,
        cause == Some(Cause::Orphan),
        || format!("{instance} was terminated with cause {cause:?}"),
    ));

    let removed =
        system.await_stream(&mut |frames| latest_holds(frames, |state| !lists(state, &instance)));
    checks.push(match removed {
        Ok(_) => {
            let terminations = system
                .scaling_events()?
                .iter()
                .filter(|event| terminates(event, &instance))
                .count();
            Check::expect(names::TERMINATED_ONCE, terminations == 1, || {
                format!("{instance} was terminated {terminations} times")
            })
        }
        Err(error @ E2eError::Timeout { .. }) => Check::failed(
            names::TERMINATED_ONCE,
            format!(
                "{instance} was never removed: {}",
                gang_types::error_chain(&error)
            ),
        ),
        Err(error) => return Err(error),
    });
    Ok(checks)
}

/// Whether some state, up to the first that records `instance`'s termination, lists `instance`
/// in service with `pastBootGrace`.
fn flagged_before_termination(states: &[State], instance: &str) -> bool {
    let until = states
        .iter()
        .position(|state| state.events.iter().any(|event| terminates(event, instance)))
        .map_or(states.len(), |index| index.saturating_add(1));
    states.iter().take(until).any(|state| {
        state.instances.iter().any(|row| {
            row.name == instance && row.lifecycle == Lifecycle::InService && row.past_boot_grace
        })
    })
}

/// Whether `state` lists `instance`, as an instance without a worker or as the host of one.
fn lists(state: &State, instance: &str) -> bool {
    state.instances.iter().any(|row| row.name == instance)
        || state
            .workers
            .iter()
            .any(|worker| worker.instance == instance)
}
