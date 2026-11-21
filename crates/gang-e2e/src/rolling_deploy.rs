//! Rolling controller replacement while a build has executing and queued actions.

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitStatus;

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::execution::{JOURNAL_END, Run};
use crate::held_build::{Hold, joined};
use crate::journal::{Attempts, Journal, overlapping_attempts, record_attempt};
use crate::names::rolling_deploy::{ATTEMPTS, BUILD, HELD, LEADER, LEADER_RETIRED, RETIRED};
use crate::system::Probe;
use crate::upgrade_evidence::BuildEvidence;

const EXECUTING: usize = 2;
const PROBES: [Probe; 8] = [
    Probe::ControllerLoss0,
    Probe::ControllerLoss1,
    Probe::ControllerLoss2,
    Probe::ControllerLoss3,
    Probe::ControllerLoss4,
    Probe::ControllerLoss5,
    Probe::ControllerLoss6,
    Probe::ControllerLoss7,
];

// A retiring leader retains its lease until executing work and the final history flush finish.
fn replacement(cluster: &Cluster<'_>, retired: usize) -> Result<usize> {
    cluster.wait_for_leader_except(Some(retired))
}

fn complete_retirements<T>(
    release: impl FnOnce() -> Result<()>,
    handles: [T; 2],
    mut finish: impl FnMut(T) -> Result<ExitStatus>,
) -> Result<Vec<ExitStatus>> {
    release()?;
    handles.into_iter().map(&mut finish).collect()
}

fn verdicts(
    cluster: &Cluster<'_>,
    outcome: &BuildOutcome,
    attempts: &Attempts,
    held: usize,
    retired_leader: bool,
    statuses: &[ExitStatus],
) -> Result<Vec<Check>> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for ((action, _), attempt) in attempts {
        if attempt.end.is_none() {
            return Err(E2eError::system("action attempt did not end"));
        }
        *counts.entry(action).or_default() += 1;
    }
    let distinct: BTreeSet<_> = counts.keys().collect();
    // A re-submitted queued action is invisible to this journal; it never started.
    let once = distinct.len() == PROBES.len()
        && counts.values().all(|count| *count == 1)
        && !overlapping_attempts(attempts);
    let leader = cluster.wait_for_leader();
    Ok(vec![
        Check::expect(HELD, held == EXECUTING && held < PROBES.len(), || {
            format!("{held} actions were executing")
        }),
        Check::expect(BUILD, outcome.succeeded(), || outcome.exit_summary()),
        Check::expect(ATTEMPTS, once, || format!("action attempts: {counts:?}")),
        Check::expect(
            RETIRED,
            statuses.len() == 2 && statuses.iter().all(ExitStatus::success),
            || format!("retirement statuses: {statuses:?}"),
        ),
        Check::expect(LEADER_RETIRED, retired_leader, || {
            "neither retired controller was the leader".to_owned()
        }),
        Check::expect(LEADER, leader.is_ok(), || {
            format!("no final leader: {leader:?}")
        }),
    ])
}

fn finish_journal(journal: &Journal, run: &Run, attempts: &mut Attempts) -> Result<()> {
    loop {
        let line = journal.read_line()?;
        if line == JOURNAL_END {
            return Ok(());
        }
        record_attempt(&line, run.as_ref(), attempts)?;
    }
}

fn held_attempts(journal: &Journal, run: &Run, attempts: &mut Attempts) -> Result<usize> {
    while attempts.len() < EXECUTING {
        let line = journal.read_line()?;
        if line == JOURNAL_END {
            return Err(E2eError::system("build ended before actions were held"));
        }
        record_attempt(&line, run.as_ref(), attempts)?;
    }
    Ok(attempts
        .values()
        .filter(|attempt| attempt.end.is_none())
        .count())
}

/// Replaces the initial controllers while the build is held, then judges its journal.
///
/// # Errors
/// Returns an error if the cluster, journal, or build cannot be observed.
pub fn rolling_deploy(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    let signal = cluster.stack().signal_path();
    let journal = Journal::create(
        &signal.with_file_name("journal"),
        cluster.stack().settings().hold_timeout,
    )?;
    let go = signal.with_file_name("go");
    let run = Run::fresh()?;
    journal.prepare_attempts(run.as_ref(), PROBES.iter().map(|probe| probe.as_str()))?;
    let bep = signal.with_file_name("rolling-deploy.bep");
    let rpc = signal.with_file_name("rolling-deploy.rpc");
    let stack = cluster.stack();
    let driver = stack.bazel().with_build_logs(&bep, &rpc);
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let remote = stack.administrator_remote(&credentials)?;
    std::thread::scope(|scope| -> Result<Vec<Check>> {
        let pending = Hold::new(&go, |root| cluster.stack().kill_bazel_in(root));
        let build = pending.build(scope, None, |_, stop| {
            let outcome = driver.build_until(&remote, &PROBES, Some(&run), stop);
            journal.write_line(JOURNAL_END)?;
            outcome
        });
        let mut attempts = Attempts::new();
        let observation = (|| -> Result<(usize, bool, Vec<ExitStatus>)> {
            let held = held_attempts(&journal, &run, &mut attempts)?;
            cluster
                .stack()
                .wait_live_state(cluster.stack().settings().history_timeout, |state| {
                    state.queue.total > 0
                })?;
            let initial_leader = cluster.wait_for_leader()?;
            cluster.start_controller()?;
            let retired_leader = initial_leader == 0 || initial_leader == 1;
            let first = cluster.begin_retire(0)?;
            if initial_leader == 0 {
                pending.release()?;
                replacement(cluster, 0)?;
            }
            cluster.start_controller()?;
            let leader = cluster.wait_for_leader()?;
            let second = cluster.begin_retire(1)?;
            let statuses = complete_retirements(
                || {
                    pending.release()?;
                    if leader == 1 {
                        replacement(cluster, 1)?;
                    }
                    Ok(())
                },
                [first, second],
                |retiring| cluster.finish_retire(retiring),
            )?;
            Ok((held, retired_leader, statuses))
        })();
        let settled = pending.settle(observation, || {
            finish_journal(&journal, &run, &mut attempts)
        });
        let completed = joined(build);
        pending.disarm();
        let (held, retired_leader, statuses) = settled?;
        let outcome = completed?;
        let evidence = BuildEvidence::read(&bep, &rpc)?;
        let mut checks = verdicts(
            cluster,
            &outcome,
            &attempts,
            held,
            retired_leader,
            &statuses,
        )?;
        checks.push(Check::expect(
            "rolling deploy: zero failed actions",
            evidence.finished_success && evidence.failed_actions == 0,
            || format!("build evidence: {evidence:?}"),
        ));
        Ok(checks)
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::os::unix::process::ExitStatusExt;

    use parking_lot::Mutex;

    use super::*;

    #[test]
    fn release_precedes_retirement_completion_for_former_leaders() {
        for former_leader in [0, 1] {
            let released = Cell::new(false);
            let events = Mutex::new(Vec::new());
            let result = complete_retirements(
                || {
                    events.lock().push("release");
                    released.set(true);
                    Ok(())
                },
                [0, 1],
                |index| {
                    events
                        .lock()
                        .push(if index == 0 { "finish 0" } else { "finish 1" });
                    if index == former_leader && !released.get() {
                        return Err(E2eError::system("former leader is still draining"));
                    }
                    Ok(ExitStatus::from_raw(0))
                },
            );
            assert!(result.is_ok(), "former leader {former_leader}: {result:?}");
            assert_eq!(*events.lock(), ["release", "finish 0", "finish 1"]);
        }
    }
}
