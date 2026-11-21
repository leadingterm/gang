//! Work-aware scaling under held launches and held actions.

use std::time::Duration;

use gang_wire::{Event, EventChange, Frame, PassState, State};

use crate::check::Check;
use crate::control::{Control, latest_holds, states};
use crate::errors::{E2eError, Result};
use crate::event_log::sequence;
use crate::execution::{Execution, Run, held_build};
use crate::names::work_aware as names;
use crate::system::{Probe, System};

fn registered(history: &[Event]) -> bool {
    history
        .iter()
        .any(|event| matches!(event.change, EventChange::WorkerRegistered { .. }))
}

fn observation_or_false(observation: Result<bool>) -> Result<bool> {
    match observation {
        Err(E2eError::Timeout { .. }) => Ok(false),
        result => result,
    }
}

struct CancelOnUnwind<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for CancelOnUnwind<F> {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Some(cancel) = self.0.take()
        {
            cancel();
        }
    }
}

/// Counts distinct observed successful passes, not frames carrying the same pass.
fn consecutive(frames: &[Frame], count: usize, condition: impl Fn(&State) -> bool) -> Result<bool> {
    let mut last = None;
    let mut good = 0;
    for state in states(frames)? {
        if state.pass.state != PassState::Ok {
            good = 0;
            continue;
        }
        let Some(sample) = state
            .recent
            .samples
            .iter()
            .rev()
            .find(|sample| sample.passes > 0)
        else {
            continue;
        };
        let pass = (sample.start, sample.passes);
        if last == Some(pass) {
            continue;
        }
        last = Some(pass);
        if condition(&state) {
            good += 1;
            if good >= count {
                return Ok(true);
            }
        } else {
            good = 0;
        }
    }
    Ok(false)
}

fn wait_passes<S: System>(
    system: &S,
    count: usize,
    condition: impl Fn(&State) -> bool,
) -> Result<Vec<Frame>> {
    let frames = system
        .await_stream(&mut |frames| consecutive(frames, count, &condition).unwrap_or(true))?;
    if !consecutive(&frames, count, condition)? {
        return Err(E2eError::system(
            "state stream ended before the requested successful passes",
        ));
    }
    Ok(frames)
}

/// The first scale-out must precede ten further successful passes, not merely ten frames.
fn warm_passes(frames: &[Frame]) -> Result<bool> {
    let mut seen = false;
    let mut last = None;
    let mut count = 0;
    for state in states(frames)? {
        if !seen {
            if state
                .events
                .iter()
                .any(|event| matches!(event.change, EventChange::ScaledOut { from: 0, to: 1, .. }))
            {
                seen = true;
                last = state
                    .recent
                    .samples
                    .iter()
                    .rev()
                    .find(|sample| sample.passes > 0)
                    .map(|sample| (sample.start, sample.passes));
            }
            continue;
        }
        if state.pass.state != PassState::Ok {
            continue;
        }
        if let Some(sample) = state
            .recent
            .samples
            .iter()
            .rev()
            .find(|sample| sample.passes > 0)
        {
            let pass = (sample.start, sample.passes);
            if last != Some(pass) {
                last = Some(pass);
                count += 1;
                if count >= 10 {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn warmup<S: Control, E: Execution>(system: &S, exec: &E) -> Result<(crate::BuildOutcome, State)> {
    system.hold_launches()?;
    if let Err(error) = system.start_server() {
        let _ = system.release_launches();
        return Err(error);
    }
    let result = held_build(system, exec, &[Probe::WorkPlain], |_| {
        let observed = system
            .await_stream(&mut |frames| warm_passes(frames).unwrap_or(true))
            .and_then(|frames| {
                if warm_passes(&frames)? {
                    Ok(())
                } else {
                    Err(E2eError::system(
                        "state stream ended before ten passes after scale-out",
                    ))
                }
            });
        let released = system.release_launches();
        observed?;
        released
    });
    if result.is_err() {
        let _ = system.release_launches();
    }
    let (outcome, ()) = result?;
    let history = system.await_history(&mut |history| registered(history))?;
    if !registered(&history) {
        return Err(E2eError::system("history ended before a worker registered"));
    }
    let state = system.live_state()?;
    Ok((outcome, state))
}

fn boundary<S: System>(system: &S) -> Result<u64> {
    system.scaling_events()?.last().map_or(Ok(0), sequence)
}

fn scale_outs_after<S: System>(system: &S, since: u64) -> Result<Vec<Event>> {
    system
        .scaling_events()?
        .into_iter()
        .filter_map(|event| match sequence(&event) {
            Ok(seq) if seq > since && matches!(event.change, EventChange::ScaledOut { .. }) => {
                Some(Ok(event))
            }
            Err(error) => Some(Err(error)),
            _ => None,
        })
        .collect()
}

fn short_observation(frames: &[Frame], since: u64) -> Result<Option<bool>> {
    for state in states(frames)? {
        for event in &state.events {
            if matches!(event.change, EventChange::ScaledOut { .. }) && sequence(event)? > since {
                return Ok(Some(false));
            }
        }
    }
    if consecutive(frames, 3, |state| {
        state.queue.total == 1
            && state
                .fleets
                .iter()
                .map(|fleet| fleet.demand.running)
                .sum::<u32>()
                == 2
    })? {
        return Ok(Some(true));
    }
    Ok(None)
}

fn build_check(name: &'static str, outcome: &crate::BuildOutcome) -> Check {
    Check::expect(name, outcome.succeeded(), || outcome.exit_summary())
}

/// Judges a burst's scale-outs, measured readiness and build outcome.
#[must_use]
pub fn burst_checks(
    events: &[Event],
    readiness: Option<Duration>,
    outcome: &crate::BuildOutcome,
) -> Vec<Check> {
    let mut checks = vec![Check::expect(
        names::READINESS,
        readiness.is_some_and(|d| d >= Duration::from_secs(9)),
        || format!("readiness was {readiness:?}, expected at least 9s"),
    )];
    checks.extend(burst_purchase_checks(events, outcome));
    checks
}

fn burst_purchase_checks(events: &[Event], outcome: &crate::BuildOutcome) -> Vec<Check> {
    vec![
        Check::expect(
            names::BURST_SCALE_OUT,
            !events.is_empty()
                && events
                    .iter()
                    .filter(|e| matches!(e.change, EventChange::ScaledOut { .. }))
                    .all(|e| matches!(e.change, EventChange::ScaledOut { to: 0..=1, .. }))
                && events
                    .iter()
                    .any(|e| matches!(e.change, EventChange::ScaledOut { .. })),
            || format!("burst scale-outs: {events:?}"),
        ),
        build_check(names::BURST_BUILD, outcome),
    ]
}

/// Judges the largest desired fleet capacity observed during the burst.
pub fn burst_capacity_check(max_desired: u32) -> Check {
    Check::expect("burst capacity never exceeds one", max_desired <= 1, || {
        format!("the burst reached desired capacity {max_desired}")
    })
}

/// Judges a held short-work build and its scaling history.
#[must_use]
pub fn short_checks(events: &[Event], observed: bool, outcome: &crate::BuildOutcome) -> Vec<Check> {
    vec![
        Check::expect(names::QUEUED, observed, || {
            "two executing actions and one queued action were not observed for three passes"
                .to_owned()
        }),
        Check::expect(
            names::QUEUED_SCALE_OUT,
            !events
                .iter()
                .any(|event| matches!(event.change, EventChange::ScaledOut { .. })),
            || format!("scale-outs during build: {events:?}"),
        ),
        build_check(names::QUEUED_BUILD, outcome),
    ]
}

/// Judges whether a held backlog grew the fleet and its build succeeded.
#[must_use]
pub fn backlog_checks(reached: bool, outcome: &crate::BuildOutcome) -> Vec<Check> {
    vec![
        Check::expect(names::BACKLOG_WORKERS, reached, || {
            "fewer than three workers connected while work was held".to_owned()
        }),
        build_check(names::BACKLOG_BUILD, outcome),
    ]
}

/// A trained short burst from zero buys only one worker and measures launch readiness.
///
/// # Errors
/// When the deployment or its observations cannot be driven.
pub fn burst_from_zero<S: Control, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    let (warm, state) = warmup(system, exec)?;
    let mut checks = vec![build_check(names::WARMUP_BUILD, &warm)];
    if !warm.succeeded() {
        return Ok(checks);
    }
    let probes = (0..160)
        .map(|i| {
            format!("burst_{i:03}")
                .parse::<Probe>()
                .map_err(E2eError::system_from("parsing burst probe"))
        })
        .collect::<Result<Vec<_>>>()?;
    // Train the actual targets on serving capacity: the first cold action's mnemonic
    // fallback includes CAS setup and need not describe the short burst.
    let training_run = Run::fresh()?;
    system.hold_supply()?;
    let trained = system.build_run(&probes, &training_run);
    let released = system.release_supply();
    let trained = trained?;
    released?;
    checks.push(build_check("burst duration training succeeds", &trained));
    if !trained.succeeded() {
        return Ok(checks);
    }
    let value = serde_json::to_value(&state)
        .map_err(E2eError::system_from("serializing readiness state"))?;
    let readiness = value
        .get("fleets")
        .and_then(|v| v.as_array())
        .and_then(|v| v.first())
        .and_then(|v| v.get("demand"))
        .and_then(|v| v.get("readiness"))
        .and_then(|v| v.as_str())
        .and_then(|v| gang_wire::duration::parse(v).ok());
    let drained = system.await_stream(&mut |frames| {
        latest_holds(frames, |s| {
            s.workers.is_empty()
                && s.instances.is_empty()
                && s.fleets.iter().all(|f| f.desired == 0)
        })
    });
    checks.push(crate::check::awaited(
        names::WARM_DRAIN,
        drained.map(|_| ()),
    )?);
    if checks.last().is_some_and(Check::is_failure) {
        return Ok(checks);
    }
    checks.extend(run_burst(system, exec, BurstReadiness::Trained(readiness))?);
    Ok(checks)
}

/// The same 160-action burst with no training, learned durations or readiness samples.
///
/// # Errors
/// Returns deployment, observation or build errors after releasing and joining the build.
pub fn cold_burst_from_zero<S: Control, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    run_burst(system, exec, BurstReadiness::Cold)
}

#[derive(Clone, Copy)]
enum BurstReadiness {
    Cold,
    Trained(Option<Duration>),
}

fn run_burst<S: Control, E: Execution>(
    system: &S,
    exec: &E,
    readiness: BurstReadiness,
) -> Result<Vec<Check>> {
    let probes = (0..160)
        .map(|i| {
            format!("burst_{i:03}")
                .parse::<Probe>()
                .map_err(E2eError::system_from("parsing burst probe"))
        })
        .collect::<Result<Vec<_>>>()?;
    system.hold_launches()?;
    let since = match boundary(system) {
        Ok(value) => value,
        Err(error) => {
            let _ = system.release_launches();
            return Err(error);
        }
    };
    let result = held_build(system, exec, &probes, |_| {
        let observed = wait_passes(system, 3, |state| state.queue.total == 160);
        let released = system.release_launches();
        let before = observed?;
        released?;
        let before_states = states(&before)?;
        let completed = before_states
            .last()
            .ok_or_else(|| E2eError::system("no state while the burst was queued"))?
            .completed
            .checked_add(160)
            .ok_or_else(|| E2eError::system("burst completion count overflowed"))?;
        let after = system.await_stream(&mut |frames| {
            latest_holds(frames, |state| state.completed >= completed)
        })?;
        let after_states = states(&after)?;
        if after_states
            .last()
            .is_none_or(|state| state.completed < completed)
        {
            return Err(E2eError::system(
                "state stream ended before the burst completed",
            ));
        }
        before_states
            .iter()
            .chain(&after_states)
            .flat_map(|state| state.fleets.iter().map(|fleet| fleet.desired))
            .max()
            .ok_or_else(|| E2eError::system("burst states contained no fleet"))
    });
    if result.is_err() {
        let _ = system.release_launches();
    }
    let (outcome, max_desired) = result?;
    let events = scale_outs_after(system, since)?;
    let mut checks = match readiness {
        BurstReadiness::Trained(readiness) => burst_checks(&events, readiness, &outcome),
        BurstReadiness::Cold => burst_purchase_checks(&events, &outcome),
    };
    checks.push(burst_capacity_check(max_desired));
    Ok(checks)
}

/// Short executing work absorbs a queued action without another worker.
///
/// # Errors
/// When the deployment or its observations cannot be driven.
pub fn queued_behind_short_work<S: Control, E: Execution>(
    system: &S,
    exec: &E,
) -> Result<Vec<Check>> {
    let (warm, _) = warmup(system, exec)?;
    let mut checks = vec![build_check(names::WARMUP_BUILD, &warm)];
    if !warm.succeeded() {
        return Ok(checks);
    }
    let warm_run = Run::fresh()?;
    let companion_warm = system.build_additional_run(&[Probe::WorkPlain], &warm_run)?;
    checks.push(build_check(
        "companion client warmup succeeds",
        &companion_warm,
    ));
    if !companion_warm.succeeded() {
        return Ok(checks);
    }
    exec.set_go(false)?;
    let since = boundary(system)?;
    let (held_outcome, companion) =
        held_build(system, exec, &[Probe::WorkHeld0, Probe::WorkHeld1], |_| {
            let running = system
                .await_stream(&mut |frames| {
                    latest_holds(frames, |state| {
                        state
                            .fleets
                            .iter()
                            .map(|fleet| fleet.demand.running)
                            .sum::<u32>()
                            == 2
                    })
                })
                .and_then(|frames| {
                    Ok(states(&frames)?.last().is_some_and(|state| {
                        state
                            .fleets
                            .iter()
                            .map(|fleet| fleet.demand.running)
                            .sum::<u32>()
                            == 2
                    }))
                });
            if !observation_or_false(running)? {
                return Ok(None);
            }
            let run = Run::fresh()?;
            std::thread::scope(|scope| {
                let build = scope.spawn(|| system.build_additional_run(&[Probe::WorkPlain], &run));
                let _guard = CancelOnUnwind(Some(|| {
                    let _ = exec.set_go(true);
                    let _ = system.kill_server();
                }));
                let observation = observation_or_false(
                    system
                        .await_stream(&mut |frames| {
                            short_observation(frames, since)
                                .map_or(true, |observed| observed.is_some())
                        })
                        .and_then(|frames| {
                            short_observation(&frames, since)?.ok_or_else(|| {
                            E2eError::system(
                                "state stream ended before queued work or scale-out was observed",
                            )
                        })
                        }),
                );
                let released = exec.set_go(true);
                if observation.is_err() || released.is_err() {
                    let _ = system.kill_server();
                }
                let completed = build
                    .join()
                    .map_err(|_| E2eError::system("the companion build thread panicked"));
                let observed = observation?;
                released?;
                Ok(Some((observed, completed??)))
            })
        })?;
    let events = scale_outs_after(system, since)?;
    if let Some((observed, plain_outcome)) = companion {
        checks.extend(short_checks(&events, observed, &plain_outcome));
    } else {
        checks.push(Check::expect(names::QUEUED, false, || {
            "both held actions did not start before the observation deadline".to_owned()
        }));
    }
    checks.push(build_check(names::QUEUED_HELD_BUILD, &held_outcome));
    Ok(checks)
}

/// A persistent backlog of held actions grows the fleet to three connected workers.
///
/// # Errors
/// When the deployment or its observations cannot be driven.
pub fn backlog_grows_fleet<S: Control, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    exec.set_go(false)?;
    let _since = boundary(system)?;
    let (outcome, reached) = held_build(
        system,
        exec,
        &[Probe::WorkHeld0, Probe::WorkHeld1, Probe::WorkHeld2],
        |_| {
            observation_or_false(
                system
                    .await_stream(&mut |frames| {
                        latest_holds(frames, |state| state.workers.len() >= 3)
                    })
                    .and_then(|frames| {
                        Ok(states(&frames)?
                            .last()
                            .is_some_and(|state| state.workers.len() >= 3))
                    }),
            )
        },
    )?;
    Ok(backlog_checks(reached, &outcome))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_history_survives_worker_drain() {
        let event = |change| Event {
            name: "events/1".to_owned(),
            create_time: gang_clock::Timestamp::from_unix_millis(0),
            fleet: "fleets/test".to_owned(),
            cause: None,
            change,
        };
        let history = [
            event(EventChange::WorkerRegistered {
                worker: "workers/1".to_owned(),
            }),
            event(EventChange::WorkerDeregistered {
                worker: "workers/1".to_owned(),
            }),
        ];
        assert!(registered(&history));
        assert!(!registered(&history[1..]));
    }

    #[test]
    fn cold_burst_verdict_requires_one_worker_and_success_without_training() {
        let event = |to| Event {
            name: "events/1".to_owned(),
            create_time: gang_clock::Timestamp::from_unix_millis(0),
            fleet: "fleets/local".to_owned(),
            cause: None,
            change: EventChange::ScaledOut {
                from: 0,
                to,
                detail: None,
            },
        };
        let success = crate::BuildOutcome::new(Some(0), String::new());
        let checks = burst_purchase_checks(&[event(1)], &success);
        assert_eq!(checks.len(), 2);
        assert!(checks.iter().all(|check| !check.is_failure()));
        assert!(burst_purchase_checks(&[event(2)], &success)[0].is_failure());
        let failure = crate::BuildOutcome::new(Some(1), String::new());
        assert!(burst_purchase_checks(&[event(1)], &failure)[1].is_failure());
    }

    #[test]
    fn observation_timeout_is_a_failed_observation_but_other_errors_propagate() {
        let observed = observation_or_false(Err(E2eError::timeout("observation", Duration::ZERO)))
            .expect("timeout is a verdict");
        let build = crate::BuildOutcome::new(Some(0), String::new());
        assert!(short_checks(&[], observed, &build)[0].is_failure());
        assert!(backlog_checks(observed, &build)[0].is_failure());
        assert!(observation_or_false(Err(E2eError::system("broken stream"))).is_err());
        assert!(observation_or_false(Ok(true)).expect("observed"));
    }

    #[test]
    fn scoped_unwind_releases_companion_before_join() {
        let (send, receive) = std::sync::mpsc::channel();
        let released = std::sync::atomic::AtomicBool::new(false);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|scope| {
                let released = &released;
                scope.spawn(move || {
                    if receive.recv_timeout(Duration::from_secs(1)).is_ok() {
                        released.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                });
                let _guard = CancelOnUnwind(Some(|| {
                    let _ = send.send(());
                }));
                panic!("observation panicked");
            });
        }));
        assert!(unwound.is_err());
        assert!(released.load(std::sync::atomic::Ordering::SeqCst));
    }
}
