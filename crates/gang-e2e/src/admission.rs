//! Scheduler admission of memory and disk requests, worker fleet disk validation, and cold starts.

use std::collections::BTreeSet;

use gang_wire::{Event, EventChange, Frame};
use tokio::sync::oneshot;

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::event_log::{describe_early_exit, early_exit, scale_out_recorded};
use crate::execution::{
    COLD_START_MISFIT_MESSAGE, ESCALATION_MAX_STEPS, Execution, NO_FLEET_ACCEPTS, REFUSED_DISK_KEY,
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
    let attempts = system.attempt_records()?.len();
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
            let attempts = system.attempt_records()?.len();
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
    let attempts = system.attempt_records()?.len();
    let after = system.build(&[Probe::Pinned])?;

    Ok(vec![
        Check::expect(disk_guard::REFUSED, !outcome.succeeded(), || {
            "the build of an unplaceable action succeeded".to_owned()
        }),
        Check::expect(
            disk_guard::NAMES_NO_FLEET,
            outcome.stderr.contains(NO_FLEET_ACCEPTS) && outcome.stderr.contains(REFUSED_DISK_KEY),
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
/// The state stream is attached before a build creates demand. Demand remains active until durable
/// history records a scale-out and disk refusals from two distinct physical instances, or any worker
/// enrolls. Refusing instances need never survive until a live-state sample. The build and server
/// are then stopped; every observed frame must remain free of workers.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn oversized_fleet<S: System, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    let observed = observe_refusals(system)?;
    let logs = exec.worker_logs()?;
    let start = logs.len().saturating_sub(2000);
    let start = logs
        .char_indices()
        .find(|(index, _)| *index >= start)
        .map_or(logs.len(), |(index, _)| index);
    let tail = logs.get(start..).unwrap_or_default();
    let exit = early_exit(&observed.history, None);
    let exit_matches = !observed.refusing_instances().is_empty();
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
            observed.completed && observed.worker.is_none() && observed.replaced(),
            || {
                observed.worker.as_ref().map_or_else(
                    || {
                        format!(
                            "no scale-out with two distinct disk-refusing instances was observed \
                             before the stream ended; recorded disk-refusing instances: {:?}",
                            observed.refusing_instances(),
                        )
                    },
                    |worker| format!("a worker registered: {worker}"),
                )
            },
        ),
    ])
}

/// Evidence retained across the whole monitored demand window.
#[derive(Default)]
struct Refusals {
    history: Vec<Event>,
    worker: Option<String>,
    completed: bool,
}

impl Refusals {
    /// Physical instances whose durable failure explains the disk guard.
    fn refusing_instances(&self) -> BTreeSet<&str> {
        self.history
            .iter()
            .filter_map(|event| match &event.change {
                EventChange::ExitedEarly { instance, failure }
                    if failure.contains("declares disk_per_instance") =>
                {
                    Some(instance.as_str())
                }
                _ => None,
            })
            .collect()
    }

    /// Sustained demand caused at least two physical instances to refuse their fleet.
    fn replaced(&self) -> bool {
        scale_out_recorded(&self.history) && self.refusing_instances().len() >= 2
    }

    /// Retains even a worker that disappears again before the final frame.
    fn observe_workers(&mut self, frames: &[Frame]) {
        if self.worker.is_some() {
            return;
        }
        self.worker = frames.iter().find_map(|frame| match frame {
            Frame::Snapshot(state) => state.workers.first().map(|worker| worker.name.clone()),
            Frame::Delta(delta) => delta
                .workers
                .iter()
                .flat_map(|keyed| &keyed.upsert)
                .next()
                .map(|worker| worker.name.clone()),
            Frame::Tick(_) | Frame::Closed(_) => None,
        });
    }
}

/// Attaches observation before creating demand and owns its cancellation on every return path.
fn observe_refusals<S: System>(system: &S) -> Result<Refusals> {
    const STOP: &str = "the oversized-fleet observation ended";
    std::thread::scope(|scope| {
        let (stop, receiver) = oneshot::channel();
        let mut receiver = Some(receiver);
        let mut build = None;
        let mut observed = Refusals::default();
        let mut operation_error = None;
        let mut build_ended = false;
        let frames = system.await_stream(&mut |frames| {
            observed.observe_workers(frames);
            if frames
                .iter()
                .any(|frame| matches!(frame, Frame::Snapshot(_)))
                && let Some(receiver) = receiver.take()
            {
                build = Some(
                    scope.spawn(move || system.build_until_stopped(&[Probe::Pinned], receiver)),
                );
            }
            match system.scaling_events() {
                Ok(history) => observed.history = history,
                Err(error) => {
                    operation_error = Some(error);
                    return true;
                }
            }
            if observed.worker.is_some() {
                return true;
            }
            if build
                .as_ref()
                .is_some_and(std::thread::ScopedJoinHandle::is_finished)
            {
                build_ended = true;
                return true;
            }
            observed.replaced()
        });
        let _ = stop.send(STOP.to_owned());
        let joined = build.map(|build| {
            build
                .join()
                .map_err(|_| E2eError::system("the build thread panicked"))
        });
        let killed = system.kill_server();
        if let Some(error) = operation_error {
            return Err(error);
        }
        killed?;
        if let Some(joined) = joined {
            match joined? {
                Err(E2eError::System { context, .. }) if context == STOP => {}
                outcome => {
                    let _ = outcome?;
                }
            }
        }
        if build_ended {
            return Err(E2eError::system(
                "the oversized-fleet build ended before two distinct disk refusals were observed",
            ));
        }
        match frames {
            Ok(frames) => {
                observed.observe_workers(&frames);
                observed.completed = true;
            }
            Err(E2eError::Timeout { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(observed)
    })
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
            exit.log.contains(COLD_START_MISFIT_MESSAGE) && exit.log.contains("fit no fleet"),
            || {
                format!(
                    "the log does not identify `{COLD_START_MISFIT_MESSAGE}` and `fit no fleet`; it ends with:\n{tail}"
                )
            },
        ),
    ])
}
