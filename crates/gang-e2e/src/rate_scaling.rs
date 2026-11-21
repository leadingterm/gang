//! Rate-based scenarios use streamed action targets as per-action release handles.
//! All waits are state/event driven and bounded beyond the controller's 50s settle window.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use gang_wire::{Cause, Event, EventChange, Phase, State};

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::held_build::{Hold, joined};
use crate::identity::{Credentials, Identity, Issuance};
use crate::issuer::TokenSpec;
use crate::names::work_aware as names;
use crate::secured::Secured;
use crate::stack::Stack;
use crate::system::{Probe, System};

const OBSERVE: Duration = Duration::from_secs(150);
const SOURCE_A: &[Probe] = &[Probe::RateA0, Probe::RateA1, Probe::RateA2];
const SOURCE_B: &[Probe] = &[
    Probe::RateB0,
    Probe::RateB1,
    Probe::RateB2,
    Probe::RateB3,
    Probe::RateB4,
];

fn directory<'a>(stack: &'a Stack<'_>) -> Result<&'a Path> {
    stack
        .signal_path()
        .parent()
        .ok_or_else(|| E2eError::system("hold signal has no parent"))
}

fn go(stack: &Stack<'_>) -> Result<PathBuf> {
    let path = directory(stack)?.join("go");
    fs::write(&path, b"").map_err(E2eError::io("holding rate probes"))?;
    let directory = directory(stack)?;
    for (source, lanes) in [("a", 3), ("b", 5)] {
        for lane in 0..lanes {
            for step in 0..64 {
                let path = directory.join(format!("release-rate_{source}_{lane}_{step:02}"));
                fs::write(path, b"").map_err(E2eError::io("preparing rate probe release"))?;
            }
        }
    }
    for (name, count) in [("hold", 24), ("return", 8)] {
        for index in 0..count {
            let path = directory.join(format!("release-rate_{name}_{index:02}"));
            fs::write(path, b"").map_err(E2eError::io("preparing rate probe release"))?;
        }
    }
    Ok(path)
}

pub(super) fn release(directory: &Path, target: &str) -> Result<()> {
    let label = target.rsplit(':').next().unwrap_or(target);
    if !label.starts_with("rate_")
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(E2eError::system(format!(
            "unexpected rate probe target {target:?}"
        )));
    }
    fs::write(directory.join(format!("release-{label}")), b"1")
        .map_err(E2eError::io("releasing a rate probe"))
}

fn refill(directory: &Path, state: &State) -> Result<()> {
    if let Some(actions) = &state.actions {
        for action in &actions.running {
            release(directory, &action.target)?;
        }
    }
    Ok(())
}

fn desired(state: &State) -> u32 {
    state
        .fleets
        .iter()
        .map(|fleet| fleet.desired)
        .fold(0, u32::saturating_add)
}

fn running(state: &State) -> u32 {
    state
        .workers
        .iter()
        .map(|worker| worker.running)
        .fold(0, u32::saturating_add)
}

fn elapsed(state: &State, since: u64) -> Duration {
    Duration::from_millis(state.update_time.as_unix_millis().saturating_sub(since))
}

fn horizon(state: &State) -> Duration {
    // Fixture payback=50%, idle_timeout=20s. Cold B=2s gives H=11s,
    // B+H=13s. Recompute with measured B instead of assuming local startup speed.
    let boot = state
        .fleets
        .first()
        .and_then(|fleet| fleet.demand.readiness)
        .unwrap_or(Duration::from_secs(2));
    boot.saturating_add(boot.saturating_add(Duration::from_secs(20)) / 2)
}

fn wait(
    stack: &Stack<'_>,
    phase: &'static str,
    mut observe: impl FnMut(&State) -> Result<bool>,
) -> Result<State> {
    let mut failure = None;
    let result = stack.wait_live_state(OBSERVE, |state| match observe(state) {
        Ok(done) => done,
        Err(error) => {
            failure = Some(error);
            true
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    match result {
        Err(E2eError::Timeout { condition, waited }) => Err(E2eError::timeout(
            format!(
                "{phase}: {condition}; live fleet snapshot: {:?}; durable scaling events: {:?}",
                stack.live_state(),
                stack.scaling_events(),
            ),
            waited,
        )),
        result => result,
    }
}

fn after(stack: &Stack<'_>, since: u64) -> Result<Vec<Event>> {
    stack
        .scaling_events()?
        .into_iter()
        .filter_map(|event| match crate::event_log::sequence(&event) {
            Ok(sequence) if sequence > since => Some(Ok(event)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn boundary(stack: &Stack<'_>) -> Result<u64> {
    stack
        .scaling_events()?
        .last()
        .map_or(Ok(0), crate::event_log::sequence)
}

/// Judges scale-outs for eight closed-loop lanes against their two-worker cold resource bound.
/// Refill replaces completions; it does not add reservations beyond those lanes.
#[must_use]
pub fn stream_checks(events: &[Event], refilled: bool) -> Vec<Check> {
    let purchases: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.change {
            EventChange::ScaledOut { from, to, detail } => Some((*from, *to, detail)),
            _ => None,
        })
        .collect();
    vec![
        Check::expect(names::STREAM_REFILL, refilled, || {
            "stream did not complete fresh actions continuously beyond measured B+H".to_owned()
        }),
        Check::expect(
            names::STREAM_STEPS,
            !purchases.is_empty() && purchases.iter().all(|(_, to, _)| *to <= 2),
            || format!("expected at most two workers for bounded refill; got {purchases:?}"),
        ),
        Check::expect(
            names::DECISION_DETAIL,
            !purchases.is_empty()
                && purchases.iter().all(|(_, _, detail)| {
                    detail
                        .as_ref()
                        .is_some_and(|detail| !detail.trim().is_empty())
                }),
            || format!("missing scale-out decision detail: {purchases:?}"),
        ),
    ]
}

/// One authenticated developer drives eight dependency lanes, refilling after every observation.
///
/// # Errors
/// Returns deployment, observation, cancellation or build errors after joining the build.
pub fn sustained_stream(stack: &Stack<'_>) -> Result<Vec<Check>> {
    stack.start_server()?;
    let directory = directory(stack)?;
    let go = go(stack)?;
    let run = Run::fresh()?;
    let probes: Vec<_> = SOURCE_A.iter().chain(SOURCE_B).copied().collect();
    let since = boundary(stack)?;
    std::thread::scope(|scope| {
        let hold = Hold::new(&go, |root| stack.kill_bazel_in(root));
        let build = hold.build(scope, Some("rate-stream"), |_, stop| {
            stack.build_additional_in(&probes, &run, "rate-stream", stop)
        });
        let mut started = None;
        let mut completed = None;
        let mut ceiling = true;
        let observation = wait(
            stack,
            "sustained stream remains resource bounded",
            |state| {
                refill(directory, state)?;
                ceiling &= desired(state) <= 2;
                if running(state) > 0 {
                    started.get_or_insert(state.update_time.as_unix_millis());
                    completed.get_or_insert(state.completed);
                }
                Ok(started.is_some_and(|start| {
                    elapsed(state, start) > horizon(state)
                        && elapsed(state, start) >= Duration::from_secs(30)
                }) && completed.is_some_and(|count| state.completed >= count.saturating_add(16))
                    && (1..=2).contains(&desired(state)))
            },
        );
        let observed = hold.settle(observation, || Ok(()));
        let outcome = joined(build);
        hold.disarm();
        observed?;
        let outcome = outcome?;
        let mut checks = stream_checks(&after(stack, since)?, true);
        checks.push(Check::expect(names::STREAM_CEILING, ceiling, || {
            "desired capacity exceeded the two-worker cold resource bound".to_owned()
        }));
        checks.push(Check::expect(
            names::STREAM_BUILD,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ));
        Ok(checks)
    })
}

fn issue(system: &Secured<'_>, subject: &str) -> Result<Credentials> {
    match system.issue(&TokenSpec::admitted(subject))? {
        Issuance::Issued(credentials) => Ok(credentials),
        Issuance::Refused(reason) => Err(E2eError::system(format!(
            "{subject} login refused: {reason}"
        ))),
    }
}

fn has_source_b_work(state: &State) -> bool {
    state.actions.as_ref().is_some_and(|actions| {
        actions
            .running
            .iter()
            .any(|action| action.target.contains(":rate_b_"))
            || actions
                .queued
                .iter()
                .any(|action| action.target.contains(":rate_b_"))
    })
}

fn has_both_builds(state: &State, principal_a: &str, principal_b: &str) -> bool {
    state
        .builds
        .iter()
        .any(|build| build.principal == principal_a)
        && state
            .builds
            .iter()
            .any(|build| build.principal == principal_b)
}

/// Two authenticated closed-loop streams remain within their combined resource bound.
///
/// # Errors
/// Returns login, deployment, observation or build errors after releasing/cancelling and joining.
pub fn two_sources(system: &Secured<'_>) -> Result<Vec<Check>> {
    let stack = system.stack();
    let first = issue(system, "rate-source-a")?;
    let second = issue(system, "rate-source-b")?;
    let principal_a = gang_wire::name::of::<gang_wire::name::Principals>(&first.identity()?);
    let principal_b = gang_wire::name::of::<gang_wire::name::Principals>(&second.identity()?);
    let directory = directory(stack)?;
    let go = go(stack)?;
    let run_a = Run::fresh()?;
    let run_b = Run::fresh()?;
    let since = boundary(stack)?;
    std::thread::scope(|scope| {
        let hold = Hold::new(&go, |root| stack.kill_bazel_in(root));
        let a = hold.build(scope, Some("rate-source-a"), |_, stop| {
            system.build_run_as_until(&first, SOURCE_A, &run_a, "rate-source-a", stop)
        });
        let mut start_a = None;
        let mut a_at_floor = true;
        let mature = wait(stack, "source A maturity", |state| {
            refill(directory, state)?;
            a_at_floor &= desired(state) <= 1;
            if running(state) > 0 {
                start_a.get_or_insert(state.update_time.as_unix_millis());
            }
            Ok(start_a.is_some_and(|start| {
                elapsed(state, start) > horizon(state)
                    && elapsed(state, start) >= Duration::from_secs(30)
            }) && state.completed >= 12)
        });
        if let Err(error) = mature {
            let _ = hold.release();
            hold.cancel("source A observation failed");
            let _ = joined(a);
            hold.disarm();
            return Err(error);
        }
        let b = hold.build(scope, Some("rate-source-b"), |_, stop| {
            system.build_run_as_until(&second, SOURCE_B, &run_b, "rate-source-b", stop)
        });
        let mut start_b = None;
        let mut isolated = true;
        let mut b_targets = BTreeSet::new();
        let mut authenticated_builds = false;
        let observation = wait(stack, "source B persistence", |state| {
            refill(directory, state)?;
            authenticated_builds |= has_both_builds(state, &principal_a, &principal_b);
            if let Some(actions) = &state.actions {
                for action in &actions.running {
                    if action.target.contains(":rate_b_") {
                        b_targets.insert(action.target.clone());
                    }
                }
            }
            let b_seen = has_source_b_work(state);
            if b_seen {
                start_b.get_or_insert(state.update_time.as_unix_millis());
            }
            isolated &= desired(state) <= 2;
            Ok(start_b.is_some_and(|start| {
                elapsed(state, start) > horizon(state)
                    && elapsed(state, start) >= Duration::from_secs(30)
            }) && b_targets.len() >= 10
                && (1..=2).contains(&desired(state)))
        });
        let observed = hold.settle(observation, || Ok(()));
        let outcome_a = joined(a);
        let outcome_b = joined(b);
        hold.disarm();
        observed?;
        let a = outcome_a?;
        let b = outcome_b?;
        let mut checks = stream_checks(&after(stack, since)?, true);
        checks.push(Check::expect(
            names::SOURCE_ISOLATION,
            first.principal != second.principal
                && first.home != second.home
                && first.token != second.token
                && a_at_floor
                && isolated
                && authenticated_builds,
            || {
                format!(
                    "distinct streams: A at floor={a_at_floor}, both at floor={isolated}, authenticated builds={authenticated_builds}"
                )
            },
        ));
        checks.push(Check::expect(
            names::SOURCES_BUILD,
            a.succeeded() && b.succeeded(),
            || {
                format!(
                    "source A: {}; source B: {}",
                    a.exit_summary(),
                    b.exit_summary()
                )
            },
        ));
        Ok(checks)
    })
}

fn indexed(prefix: &str, count: usize) -> Result<Vec<Probe>> {
    (0..count)
        .map(|index| {
            format!("{prefix}_{index:02}")
                .parse()
                .map_err(E2eError::system_from("parsing rate probe"))
        })
        .collect()
}

fn lone_survivor(state: &State) -> Option<&str> {
    if state.workers.len() == 1
        && state.instances.is_empty()
        && desired(state) == 1
        && state.queue.total == 0
    {
        state.workers.first().map(|worker| worker.instance.as_str())
    } else {
        None
    }
}

/// Judges audited busy cordons, not idle expiry, and successful completion of held work.
#[must_use]
pub fn utilization_checks(events: &[Event], max_cordons: usize, success: bool) -> Vec<Check> {
    let drains: Vec<_> = events
        .iter()
        .filter(|event| {
            event.cause == Some(Cause::Utilization)
                && matches!(&event.change, EventChange::DrainStarted { detail: Some(detail), .. }
                if !detail.trim().is_empty())
        })
        .collect();
    vec![
        Check::expect(names::UTILIZATION_DRAINS, drains.len() >= 2, || {
            format!("expected two detailed UTILIZATION drains: {events:?}")
        }),
        Check::expect(names::CORDON_BOUND, max_cordons <= 1, || {
            format!("maximum simultaneous physical cordons was {max_cordons}, allowed at most one")
        }),
        Check::expect(names::UTILIZATION_BUILD, success, || {
            "a held or returning-load build failed".to_owned()
        }),
    ]
}

fn utilization_build_check(outcome: &crate::BuildOutcome, second: &crate::BuildOutcome) -> Check {
    Check::expect(
        names::UTILIZATION_BUILD,
        outcome.succeeded() && second.succeeded(),
        || {
            format!(
                "held build: {}; returning build: {}",
                outcome.exit_summary(),
                second.exit_summary()
            )
        },
    )
}

/// Distributed light load drains three workers to one; held and returning-wave builds finish.
///
/// # Errors
/// Returns deployment, observation or build errors after joining every owned build.
pub fn utilization_scale_in(system: &Secured<'_>) -> Result<Vec<Check>> {
    let stack = system.stack();
    let return_credentials = issue(system, "rate-return")?;
    let directory = directory(stack)?;
    let go = go(stack)?;
    let run = Run::fresh()?;
    let return_run = Run::fresh()?;
    let probes = indexed("rate_hold", 24)?;
    let returning = indexed("rate_return", 8)?;
    let since = boundary(stack)?;
    std::thread::scope(|scope| {
        let hold = Hold::new(&go, |root| stack.kill_bazel_in(root));
        let build = hold.build(scope, Some("rate-utilization"), |_, stop| {
            stack.build_additional_in(&probes, &run, "rate-utilization", stop)
        });
        let mut keep = BTreeSet::new();
        let mut max_cordons = 0;
        let mut survivor = None;
        let observation = wait(stack, "busy utilization drains", |state| {
            let Some(actions) = &state.actions else {
                return Ok(false);
            };
            if keep.is_empty() {
                if state.workers.len() != 3 || actions.running.len() != 24 {
                    return Ok(false);
                }
                // Keep exactly one sampled action on EACH physical worker. Target labels,
                // not dispatch order, choose releases, so distributed scheduling stays intact.
                // Each worker retains one busy action: no idle_timeout path is eligible.
                // At N=3, removing one gives 3/(2*8)=18.75% load; at N=2,
                // even counting the victim's executing action, 3/(1*8)=37.5%.
                // Both are below the default 70% waterline. Half-life=2s,
                // dwell=4s and max_cordons=1 permit successive busy cordons after
                // the 50s settle window; idle_timeout=5m exceeds this 150s wait.
                let mut by_worker = BTreeMap::new();
                for action in &actions.running {
                    by_worker.entry(&action.worker).or_insert(&action.target);
                }
                if by_worker.len() != 3 {
                    return Ok(false);
                }
                keep.extend(by_worker.into_values().cloned());
            }
            let draining: BTreeSet<_> = state
                .workers
                .iter()
                .filter(|worker| worker.phase == Phase::Draining)
                .map(|worker| worker.instance.as_str())
                .collect();
            max_cordons = max_cordons.max(draining.len());
            for action in &actions.running {
                let cordoned = state.workers.iter().any(|worker| {
                    worker.name == action.worker && draining.contains(worker.instance.as_str())
                });
                if !keep.contains(&action.target) || cordoned {
                    release(directory, &action.target)?;
                }
            }
            if running(state) == 1
                && let Some(instance) = lone_survivor(state)
            {
                survivor = Some(instance.to_owned());
                return Ok(true);
            }
            Ok(false)
        });
        if let Err(error) = observation {
            let _ = hold.release();
            hold.cancel("utilization observation failed");
            let _ = joined(build);
            hold.disarm();
            return Err(error);
        }
        let second = hold.build(scope, Some("rate-return"), |_, stop| {
            // Learned E2eProbe service times apply across principals. The returning
            // wave may be cheap enough for the survivor; no purchase is required.
            system.build_run_as_until(
                &return_credentials,
                &returning,
                &return_run,
                "rate-return",
                stop,
            )
        });
        let observed = hold.settle(Ok(()), || Ok(()));
        let outcome = joined(build);
        let second = joined(second);
        hold.disarm();
        observed?;
        let outcome = outcome?;
        let second = second?;
        let success = outcome.succeeded() && second.succeeded();
        wait(stack, "builds finish on the surviving worker", |state| {
            Ok(running(state) == 0
                && survivor
                    .as_deref()
                    .is_some_and(|instance| lone_survivor(state) == Some(instance)))
        })?;
        let mut checks = utilization_checks(&after(stack, since)?, max_cordons, success);
        checks[2] = utilization_build_check(&outcome, &second);
        checks.push(Check::expect(
            names::LIGHT_LOAD_SPREAD,
            keep.len() == 3,
            || format!("kept {} actions instead of one per worker", keep.len()),
        ));
        checks.push(Check::passed(names::SURVIVING_WORKER));
        Ok(checks)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(change: EventChange, cause: Option<Cause>) -> Event {
        Event {
            name: "events/1".to_owned(),
            create_time: gang_clock::Timestamp::from_unix_millis(0),
            fleet: "fleets/local".to_owned(),
            cause,
            change,
        }
    }

    #[test]
    fn cold_fixture_payback_horizon_is_thirteen_seconds() {
        assert_eq!(horizon(&State::default()), Duration::from_secs(13));
    }

    #[test]
    fn stream_rejects_ceiling_jump_and_missing_decision_detail() {
        let scale = |from, to, detail| {
            event(
                EventChange::ScaledOut { from, to, detail },
                Some(Cause::Demand),
            )
        };
        let valid = [scale(0, 1, Some("floor".to_owned()))];
        assert!(
            stream_checks(&valid, true)
                .iter()
                .all(|check| !check.is_failure())
        );
        assert!(
            stream_checks(&[scale(0, 3, None)], true)
                .iter()
                .filter(|check| check.is_failure())
                .count()
                >= 2
        );
        assert!(
            stream_checks(
                &[
                    scale(0, 1, Some("floor".to_owned())),
                    scale(1, 3, Some("extra".to_owned())),
                ],
                true,
            )[1]
            .is_failure()
        );
        assert!(stream_checks(&valid, false)[0].is_failure());
    }

    #[test]
    fn utilization_requires_cause_detail_cordon_limit_and_build_success() {
        let drain = |cause, detail| {
            event(
                EventChange::DrainStarted {
                    worker: "workers/local-1".to_owned(),
                    detail,
                },
                cause,
            )
        };
        let valid = [
            drain(Some(Cause::Utilization), Some("N-1 feasible".to_owned())),
            drain(Some(Cause::Utilization), Some("waterline held".to_owned())),
        ];
        assert!(
            utilization_checks(&valid, 1, true)
                .iter()
                .all(|check| !check.is_failure())
        );
        assert!(!utilization_checks(&valid, 0, true)[1].is_failure());
        let wrong = [drain(Some(Cause::IdleTimeout), None)];
        assert!(
            utilization_checks(&wrong, 2, false)
                .iter()
                .all(Check::is_failure)
        );
    }

    #[test]
    fn utilization_build_failure_reports_both_outcomes() {
        for (held_code, returning_code) in [(1, 0), (0, 2)] {
            let held = crate::BuildOutcome::new(Some(held_code), "held output".to_owned());
            let returning =
                crate::BuildOutcome::new(Some(returning_code), "returning output".to_owned());
            let check = utilization_build_check(&held, &returning);
            assert_eq!(
                check.outcome,
                crate::check::Outcome::Failed(format!(
                    "held build: the build exited with Some({held_code}): held output; returning build: the build exited with Some({returning_code}): returning output"
                ))
            );
        }
        let success = crate::BuildOutcome::new(Some(0), String::new());
        assert!(utilization_build_check(&success, &success).is_passed());
    }

    #[test]
    fn utilization_fixture_cannot_expire_idle_before_its_observation_deadline() {
        let knobs = crate::control::ControlKnobs::UTILIZATION_SCALE_IN;
        let target = knobs.vcpus_per_instance.expect("fixture vcpus");
        let waterline = u32::from(
            gang_config::ScalingConfig::default()
                .scale_in_waterline_percent
                .get(),
        );
        assert!(3 * 100 < target * waterline);
        assert!(knobs.idle_timeout.expect("fixture idle timeout") > OBSERVE);
    }

    #[test]
    fn independent_target_releases_do_not_release_the_other_source() {
        let directory = tempfile::tempdir().expect("temporary gate directory");
        release(directory.path(), "//:rate_a_0_00").expect("release A");
        assert!(directory.path().join("release-rate_a_0_00").exists());
        assert!(!directory.path().join("release-rate_b_0_00").exists());
        assert!(!directory.path().join("go").exists());
        assert!(release(directory.path(), "//:../../go").is_err());
    }
}
