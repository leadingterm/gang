//! The drain-cancel scenario: an action queued while the idle worker drains cancels the drain and
//! runs on that worker; a server restarted during a drain terminates that instance exactly once.

use gang_wire::{Event, EventChange, Frame, State};

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::control::{
    Control, drain_started, latest_holds, states, streamed, streamed_events, terminates,
};
use crate::errors::{E2eError, Result};
use crate::event_log::sequence;
use crate::names::drain_cancel as names;
use crate::system::Probe;

/// The probe whose build scales the fleet out to its one worker.
const FIRST: Probe = Probe::Pinned;

/// The probe whose action is queued while that worker drains.
const SECOND: Probe = Probe::Unconstrained;

/// The probe whose build is running when the drain the restart interrupts starts.
const THIRD: Probe = Probe::Isolated;

/// Runs the drain-cancel scenario against a deployment whose fleet has at most one instance and
/// none standing (see [`ControlKnobs::DRAIN_CANCEL`](crate::ControlKnobs::DRAIN_CANCEL)).
///
/// Every drain the scenario reacts to is awaited on a stream opened while a build is running, so
/// the stream is open before the drain starts. The pass after the one that starts a drain would
/// finish it, so the scenario holds the provisioner's supply as soon as the first drain is
/// streamed: passes are skipped until the second action shows in the scheduler's queue, and the
/// first pass after the release sees both the draining worker and the queued action. The server
/// is killed as soon as a drain is streamed during the third build, within one `poll_interval` of
/// the pass that started it, and started again; the third build's own outcome is not judged.
///
/// # Errors
///
/// When the deployment cannot be driven or observed, or the first build fails.
pub fn drain_cancel<S: Control>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut drained = Vec::new();
    let first = system.build_during(&[FIRST], &mut || {
        drained = system
            .await_stream(&mut |frames| streamed(frames, |event| drain_started(event).is_some()))?;
        system.hold_supply()
    })?;
    if !first.succeeded() {
        return Err(E2eError::system(format!(
            "the build that scales the fleet out failed: {}",
            first.stderr_tail(system.stderr_tail_lines())
        )));
    }
    let mut checks = cancel_checks(system, &drained)?;
    checks.extend(restart_checks(system)?);
    Ok(checks)
}

/// Queues the second action while the drain the frames `drained` streamed is held, and judges that
/// it cancelled the drain and ran on the resumed worker.
fn cancel_checks<S: Control>(system: &S, drained: &[Frame]) -> Result<Vec<Check>> {
    let (worker, instance) = draining(drained)?;
    let second = system.build_during(&[SECOND], &mut || {
        let queued = system.await_stream(&mut |frames| latest_holds(frames, has_queued_action));
        system.release_supply()?;
        queued?;
        match system.await_stream(&mut |frames| {
            streamed(frames, |event| ends_drain(event, &worker, &instance))
        }) {
            Ok(_) | Err(E2eError::Timeout { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    })?;
    let history = system.scaling_events()?;
    let cancel = match cancelled(&history, &worker, &instance) {
        Ok(()) => Check::passed(names::DRAIN_CANCELLED),
        Err(reason) => Check::failed(names::DRAIN_CANCELLED, reason),
    };
    let serves = resumed_serves(&history, &worker, &second, system.stderr_tail_lines());
    Ok(vec![cancel, serves])
}

/// Kills and starts the server as soon as a drain is streamed during the third build, and judges
/// how the interrupted drain ends.
fn restart_checks<S: Control>(system: &S) -> Result<Vec<Check>> {
    let mut redrained = Vec::new();
    system.build_during(&[THIRD], &mut || {
        redrained = system
            .await_stream(&mut |frames| streamed(frames, |event| drain_started(event).is_some()))?;
        system.kill_server()?;
        system.start_server()
    })?;
    let (_, instance) = draining(&redrained)?;
    let restarted = streamed_events(&redrained)?
        .last()
        .map(|event| sequence(event))
        .transpose()?
        .unwrap_or(0);
    let before = system.scaling_events()?;
    match system
        .await_history(&mut |history| history.iter().any(|event| terminates(event, &instance)))
    {
        Ok(_) | Err(E2eError::Timeout { .. }) => {}
        Err(error) => return Err(error),
    }
    let emptied = match system.await_stream(&mut |frames| latest_holds(frames, is_empty)) {
        Ok(_) => Ok(()),
        Err(error @ E2eError::Timeout { .. }) => Err(gang_types::error_chain(&error).to_string()),
        Err(error) => return Err(error),
    };
    let processes = system.worker_count()?;
    let after = system.scaling_events()?;
    Ok(vec![
        terminated_once(&before, &after, &instance),
        no_cancel_after(&after, restarted)?,
        match emptied {
            Ok(()) => Check::expect(names::RESTART_LEAVES_NOTHING, processes == 0, || {
                format!("{processes} worker processes outlived {instance}")
            }),
            Err(reason) => Check::failed(names::RESTART_LEAVES_NOTHING, reason),
        },
    ])
}

/// The worker whose drain the frames streamed first, and its instance, both as references.
fn draining(frames: &[Frame]) -> Result<(String, String)> {
    let worker = streamed_events(frames)?
        .into_iter()
        .find_map(drain_started)
        .map(str::to_owned)
        .ok_or_else(|| E2eError::system("the state stream ended before a drain started"))?;
    let instance = states(frames)?
        .last()
        .and_then(|state| state.workers.iter().find(|row| row.name == worker))
        .map(|row| row.instance.clone())
        .ok_or_else(|| E2eError::system(format!("the draining {worker} is not in the state")))?;
    Ok((worker, instance))
}

/// Whether the state lists an action waiting in the scheduler's queue.
fn has_queued_action(state: &State) -> bool {
    state
        .actions
        .as_ref()
        .is_some_and(|actions| !actions.queued.is_empty())
}

/// Whether the state lists no worker and no instance.
fn is_empty(state: &State) -> bool {
    state.workers.is_empty() && state.instances.is_empty()
}

/// Whether `event` ends the drain of `worker`: its cancellation, or the termination of
/// `instance`.
fn ends_drain(event: &Event, worker: &str, instance: &str) -> bool {
    matches!(&event.change, EventChange::DrainCancelled { worker: cancelled } if cancelled == worker)
        || terminates(event, instance)
}

/// Whether `event` concerns `worker` or its `instance`.
fn concerns(event: &Event, worker: &str, instance: &str) -> bool {
    match &event.change {
        EventChange::DrainStarted { worker: named }
        | EventChange::DrainCancelled { worker: named }
        | EventChange::WorkerRegistered { worker: named }
        | EventChange::WorkerDeregistered { worker: named } => named == worker,
        EventChange::Terminated { instance: named } => named == instance,
        EventChange::ScaledOut { .. }
        | EventChange::ExitedEarly { .. }
        | EventChange::Unrecognized(_) => false,
    }
}

/// Judges that the first drain of `worker` was cancelled: the first event about `worker` or
/// `instance` after its first `drain_started` is a `drain_cancelled`.
fn cancelled(history: &[Event], worker: &str, instance: &str) -> std::result::Result<(), String> {
    let mut after = history
        .iter()
        .skip_while(|event| drain_started(event) != Some(worker));
    if after.next().is_none() {
        return Err(format!("no drain of {worker} was recorded"));
    }
    match after.find(|event| concerns(event, worker, instance)) {
        Some(event) if matches!(event.change, EventChange::DrainCancelled { .. }) => Ok(()),
        Some(event) => Err(format!(
            "after the drain of {worker} started, {} recorded {:?}",
            event.name, event.change
        )),
        None => Err(format!("the drain of {worker} was never cancelled")),
    }
}

/// Judges that the queued build succeeded remotely after the recorded drain of `worker` started,
/// without the fleet scaling out afterward.
fn resumed_serves(
    history: &[Event],
    worker: &str,
    second: &BuildOutcome,
    tail_lines: usize,
) -> Check {
    let mut after = history
        .iter()
        .skip_while(|event| drain_started(event) != Some(worker));
    let drain_seen = after.next().is_some();
    let scale_outs = after
        .filter(|event| matches!(event.change, EventChange::ScaledOut { .. }))
        .count();
    let remote = second.processes.remote();
    Check::expect(
        names::RESUMED_WORKER_SERVES,
        second.succeeded() && remote >= 1 && drain_seen && scale_outs == 0,
        || {
            format!(
                "the queued build {} with {remote} remote processes; {}; build stderr: {}",
                if second.succeeded() {
                    "succeeded"
                } else {
                    "failed"
                },
                if drain_seen {
                    format!("the fleet scaled out {scale_outs} times after the drain started")
                } else {
                    format!("no drain of {worker} was recorded")
                },
                second.stderr_tail(tail_lines)
            )
        },
    )
}

/// Judges that `instance` was terminated exactly once, after the restart: `before` is the history
/// the restarted server started from, `after` the history once the deployment emptied.
fn terminated_once(before: &[Event], after: &[Event], instance: &str) -> Check {
    let early = before
        .iter()
        .filter(|event| terminates(event, instance))
        .count();
    if early > 0 {
        return Check::failed(
            names::RESTART_TERMINATES_ONCE,
            format!(
                "the drain of {instance} finished before the server was killed, so the restart did not interrupt it"
            ),
        );
    }
    let total = after
        .iter()
        .filter(|event| terminates(event, instance))
        .count();
    Check::expect(names::RESTART_TERMINATES_ONCE, total == 1, || {
        format!("{instance} was terminated {total} times after the restart")
    })
}

/// Judges that no drain was cancelled after the restart; `restarted` is the sequence number of
/// the newest event the restarted server started from.
fn no_cancel_after(history: &[Event], restarted: u64) -> Result<Check> {
    let mut cancelled = false;
    for event in history {
        if sequence(event)? > restarted
            && matches!(event.change, EventChange::DrainCancelled { .. })
        {
            cancelled = true;
        }
    }
    if !cancelled {
        return Ok(Check::passed(names::NO_CANCEL_AFTER_RESTART));
    }
    let mut cancels = Vec::new();
    for event in history {
        if sequence(event)? > restarted
            && matches!(event.change, EventChange::DrainCancelled { .. })
        {
            cancels.push(event.name.as_str());
        }
    }
    Ok(Check::failed(
        names::NO_CANCEL_AFTER_RESTART,
        format!("the restarted server recorded {cancels:?}"),
    ))
}
