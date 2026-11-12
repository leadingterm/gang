//! Scheduler admission of memory and disk requests, worker fleet disk validation, and cold starts.

use std::collections::BTreeSet;

use gang_wire::{EventChange, Frame};

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::event_log::{describe_early_exit, early_exit, scale_out_recorded};
use crate::execution::{
    COLD_START_MISFIT_MESSAGE, ESCALATION_MAX_STEPS, Execution, NO_FLEET_ACCEPTS,
};
use crate::names::{cold_start, disk_guard, escalation, oversized_fleet};
use crate::system::{Limits, Probe, System};

/// Runs the memory escalation scenario.
///
/// Under one allowed escalation at 200 percent with soft enforcement, the probe outgrows every
/// reservation it receives, so each attempt is killed: it must run exactly two times and the build
/// must fail.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn memory_escalation<S: Limits>(system: &S, stderr_tail_lines: usize) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = system.build(&[Probe::MemoryEscalation])?;
    let attempts = system.attempts()?;
    let expected = usize::try_from(ESCALATION_MAX_STEPS)
        .map_err(E2eError::system_from("the escalation steps"))?
        + 1;

    Ok(vec![
        Check::expect(escalation::FAILS, !outcome.succeeded(), || {
            format!(
                "the build ended with exit code {:?}; the build said:\n{}",
                outcome.exit_code,
                outcome.stderr_tail(stderr_tail_lines)
            )
        }),
        Check::expect(escalation::ATTEMPTS, attempts == expected, || {
            format!("the probe ran {attempts} times, expected {expected}")
        }),
    ])
}

/// An action whose disk no fleet accepts fails immediately without running; the worker still serves.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn unplaceable_disk<S: Limits, E: Execution>(
    system: &S,
    exec: &E,
    stderr_tail_lines: usize,
) -> Result<Vec<Check>> {
    system.start_server()?;
    let outcome = match system.build(&[Probe::DiskGuarded]) {
        Ok(outcome) => outcome,
        Err(error) => {
            let attempts = system.attempts()?;
            return Ok(vec![
                Check::failed(
                    disk_guard::REFUSED,
                    format!(
                        "the build did not end: {}; the server log ends with:\n{}",
                        gang_types::error_chain(&error),
                        exec.server_log_tail()?
                    ),
                ),
                Check::failed(disk_guard::NAMES_NO_FLEET, "the build did not end"),
                Check::expect(disk_guard::NEVER_RAN, attempts == 0, || {
                    format!("the action no fleet accepts ran {attempts} times")
                }),
                Check::failed(
                    disk_guard::SERVING,
                    "not run: the build of the unplaceable action did not end",
                ),
            ]);
        }
    };
    let attempts = system.attempts()?;
    let after = system.build(&[Probe::Pinned])?;

    Ok(vec![
        Check::expect(disk_guard::REFUSED, !outcome.succeeded(), || {
            "the build of an unplaceable action succeeded".to_owned()
        }),
        Check::expect(
            disk_guard::NAMES_NO_FLEET,
            outcome.stderr.contains(NO_FLEET_ACCEPTS),
            || {
                format!(
                    "the build did not name the refusal; it said:\n{}",
                    outcome.stderr_tail(stderr_tail_lines)
                )
            },
        ),
        Check::expect(disk_guard::NEVER_RAN, attempts == 0, || {
            format!("the refused action ran {attempts} times")
        }),
        Check::expect(disk_guard::SERVING, after.succeeded(), || {
            format!(
                "the next build ended with exit code {:?}; the build said:\n{}",
                after.exit_code,
                after.stderr_tail(stderr_tail_lines)
            )
        }),
    ])
}

/// B26(a): a worker whose scratch cannot hold its fleet's disk refuses to enroll and reports both sizes.
///
/// A build drives a scale-out. After that scale-out is recorded, the scenario waits on the state
/// stream until it shows a replacement instance or a worker. Local Mode reaps a refusing instance
/// and launches a replacement. The server is then stopped.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn oversized_fleet<S: System, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    let (history, seen, live) = std::thread::scope(|scope| {
        let build = scope.spawn(|| system.build(&[Probe::Pinned]));
        let history = match system.await_history(&mut |events| {
            scale_out_recorded(events) && early_exit(events, None).is_some()
        }) {
            Ok(events) => Ok(events),
            Err(E2eError::Timeout { .. }) => system.scaling_events(),
            Err(error) => Err(error),
        };
        let frames = system.await_stream(&mut |frames| {
            let seen = Seen::of(frames);
            seen.worker.is_some() || seen.instances.len() > 1
        });
        // Evidence for a wait that timed out: what the server lists right now.
        let live = system
            .live_state()
            .ok()
            .map(|state| Seen::of(&[Frame::Snapshot(state)]));
        let killed = system.kill_server();
        let joined = build
            .join()
            .map_err(|_| E2eError::system("the build thread panicked"));
        killed?;
        let _ = joined?;
        let history = history?;
        match frames {
            Ok(frames) => Ok((history, Some(Seen::of(&frames)), live)),
            Err(E2eError::Timeout { .. }) => Ok((history, None, live)),
            Err(error) => Err(error),
        }
    })?;
    let logs = exec.worker_logs()?;
    let start = logs.len().saturating_sub(2000);
    let start = logs
        .char_indices()
        .find(|(index, _)| *index >= start)
        .map_or(logs.len(), |(index, _)| index);
    let tail = logs.get(start..).unwrap_or_default();
    let enrolled = seen.as_ref().and_then(|seen| seen.worker.clone());
    let refusals = logs.matches("declares disk_per_instance").count();
    let exit = early_exit(&history, None);
    let exit_matches = history.iter().any(|event| {
        matches!(&event.change, EventChange::ExitedEarly { failure, .. }
            if failure.contains("declares disk_per_instance"))
    });
    Ok(vec![
        Check::expect(
            oversized_fleet::WORKER_REFUSES,
            logs.contains("declares disk_per_instance")
                && logs.contains("but this worker's scratch volume leaves its actions")
                && exit_matches,
            || {
                let evidence = exit.map_or_else(
                    || "no early exit was recorded".to_owned(),
                    describe_early_exit,
                );
                if logs.is_empty() {
                    format!("no worker logged anything; {evidence}")
                } else {
                    format!("worker logs end with:\n{tail}\n{evidence}")
                }
            },
        ),
        Check::expect(
            oversized_fleet::NO_WORKER_ENROLLS,
            seen.as_ref()
                .is_some_and(|seen| seen.worker.is_none() && seen.instances.len() > 1),
            || {
                enrolled.map_or_else(
                    || {
                        format!(
                            "no replacement of the refusing instance was seen within the history \
                             timeout; the server then listed instances {:?} and worker {:?}; the \
                             worker logs hold {refusals} refusals",
                            live.as_ref().map(|seen| &seen.instances),
                            live.as_ref().and_then(|seen| seen.worker.as_ref()),
                        )
                    },
                    |worker| format!("a worker registered: {worker}"),
                )
            },
        ),
    ])
}

/// What the state frames of a stream showed of the instances and the workers.
struct Seen {
    /// Every instance any frame listed.
    instances: BTreeSet<String>,
    /// The first worker any frame listed.
    worker: Option<String>,
}

impl Seen {
    /// Collects the instances and the first worker of `frames`: each snapshot's rows and each
    /// delta's upserted rows.
    fn of(frames: &[Frame]) -> Self {
        let mut instances = BTreeSet::new();
        let mut worker = None;
        for frame in frames {
            let (listed, workers): (Vec<&str>, Vec<&str>) = match frame {
                Frame::Snapshot(state) => (
                    state
                        .instances
                        .iter()
                        .map(|row| row.name.as_str())
                        .collect(),
                    state.workers.iter().map(|row| row.name.as_str()).collect(),
                ),
                Frame::Delta(delta) => (
                    delta
                        .instances
                        .iter()
                        .flat_map(|keyed| &keyed.upsert)
                        .map(|row| row.name.as_str())
                        .collect(),
                    delta
                        .workers
                        .iter()
                        .flat_map(|keyed| &keyed.upsert)
                        .map(|row| row.name.as_str())
                        .collect(),
                ),
                Frame::Tick(_) | Frame::Closed(_) => (Vec::new(), Vec::new()),
            };
            instances.extend(listed.into_iter().map(str::to_owned));
            if worker.is_none() {
                worker = workers.first().map(|name| (*name).to_owned());
            }
        }
        Self { instances, worker }
    }
}

/// Runs the cold-start misfit scenario.
///
/// A configuration whose cold start no fleet accepts is a startup error: `gang server` exits
/// with status 1 before listening and names the cold start.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn cold_start_misfit<E: Execution>(exec: &E) -> Result<Vec<Check>> {
    let exit = exec.start_refused_server()?;
    let start = exit.log.len().saturating_sub(2000);
    let start = exit
        .log
        .char_indices()
        .find(|(index, _)| *index >= start)
        .map_or(0, |(index, _)| index);
    let tail = &exit.log[start..];

    Ok(vec![
        Check::expect(cold_start::REFUSED, !exit.announced, || {
            "gang server started listening".to_owned()
        }),
        Check::expect(cold_start::EXIT_CODE, exit.code == Some(1), || {
            format!("gang server ended with exit code {:?}", exit.code)
        }),
        Check::expect(
            cold_start::MESSAGE,
            exit.log.contains(COLD_START_MISFIT_MESSAGE),
            || {
                format!(
                    "the log does not contain `{COLD_START_MISFIT_MESSAGE}`; it ends with:\n{tail}"
                )
            },
        ),
    ])
}
