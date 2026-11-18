//! Leader and follower loss during a held, partially queued build.

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use crate::check::Check;
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::execution::{JOURNAL_END, Run};
use crate::held_build::{Hold, joined};
use crate::journal::{Attempts, Journal, overlapping_attempts, record_attempt};
use crate::names::controller_loss::{
    BUILD, HELD, NO_OVERLAP, REEXECUTIONS, TAKEOVER, TWO_WORKERS, WORKER_PID,
};
use crate::system::Probe;

const TTL: Duration = Duration::from_secs(10);
const CLOCK_SKEW: Duration = Duration::from_secs(2);
const MARGIN: Duration = Duration::from_secs(5);
fn takeover_bound() -> Duration {
    TTL.saturating_add(CLOCK_SKEW)
        .saturating_add(gang_types::health::health_check().healthy_window())
        .saturating_add(MARGIN)
}
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

/// Reads and judges attempt starts immediately, including while takeover is in progress.
fn read_attempts(journal: &Journal, run: &Run, starts: &mpsc::Sender<usize>) -> Result<Attempts> {
    let mut attempts = Attempts::new();
    loop {
        let line = journal.read_line()?;
        if line == JOURNAL_END {
            return Ok(attempts);
        }
        record_attempt(&line, run.as_ref(), &mut attempts)?;
        let _ = starts.send(attempts.len());
    }
}

/// Waits for held and queued actions, then kills the selected controller and times takeover.
fn observe_loss(
    cluster: &Cluster<'_>,
    follower: bool,
    starts: &Receiver<usize>,
) -> Result<(usize, bool, bool, Duration)> {
    let timeout = cluster.stack().settings().hold_timeout;
    let started = Instant::now();
    let executing = loop {
        let count = starts
            .recv_timeout(timeout.saturating_sub(started.elapsed()))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    E2eError::timeout("held actions to start", timeout)
                }
                mpsc::RecvTimeoutError::Disconnected => {
                    E2eError::system("journal ended before actions were held")
                }
            })?;
        if count >= EXECUTING {
            break count;
        }
    };
    cluster
        .stack()
        .wait_live_state(cluster.stack().settings().history_timeout, |state| {
            state.queue.total > 0
        })?;
    let two_workers = match cluster
        .stack()
        .wait_live_state(cluster.stack().settings().startup_timeout, |state| {
            state.workers.len() >= 2
        }) {
        Ok(_) => true,
        Err(E2eError::Timeout { .. }) => false,
        Err(error) => return Err(error),
    };
    let executing = starts.try_iter().last().unwrap_or(executing);
    let before = cluster.wait_for_leader()?;
    let victim = if follower {
        (0..2)
            .find(|index| *index != before)
            .ok_or_else(|| E2eError::system("no follower"))?
    } else {
        before
    };
    let started = Instant::now();
    cluster.kill(victim)?;
    let available = cluster
        .wait_for_leader_matching(
            takeover_bound().saturating_sub(started.elapsed()),
            |leader| (follower && leader == before) || (!follower && leader != before),
        )
        .is_ok();
    Ok((executing, two_workers, available, started.elapsed()))
}

/// Kills either the leader or its follower during a build with two executing actions.
///
/// # Errors
/// Returns an error if the cluster, journal or build cannot be observed.
pub fn controller_loss(cluster: &Cluster<'_>, follower: bool) -> Result<Vec<Check>> {
    let signal = cluster.stack().signal_path();
    let journal = Journal::create(
        &signal.with_file_name("journal"),
        cluster.stack().settings().hold_timeout,
    )?;
    let go = signal.with_file_name("go");
    let run = Run::fresh()?;
    journal.prepare_attempts(run.as_ref(), PROBES.iter().map(|probe| probe.as_str()))?;
    std::thread::scope(|scope| -> Result<Vec<Check>> {
        let (starts, observed_starts) = mpsc::channel();
        let journal_ref = &journal;
        let run_ref = &run;
        let reader = scope.spawn(move || read_attempts(journal_ref, run_ref, &starts));
        let hold = Hold::new(&go, |root| cluster.stack().kill_bazel_in(root));
        let build = hold.build(scope, None, |cancel, stop| {
            let outcome = cluster.stack().build_run_until(&PROBES, &run, cancel, stop);
            journal.write_line(JOURNAL_END)?;
            outcome
        });
        let observation = observe_loss(cluster, follower, &observed_starts);
        let settled = hold.settle(observation, || Ok(()));
        let completed = joined(build);
        let attempts = reader
            .join()
            .map_err(|_| E2eError::system("controller-loss journal thread panicked"))?;
        hold.disarm();
        let attempts = attempts?;
        let (executing, two_workers, elected, elapsed) = settled?;
        let outcome = completed?;
        let mut per_action = BTreeMap::<&str, usize>::new();
        for (action, _) in attempts.keys() {
            *per_action.entry(action).or_default() += 1;
        }
        let repeats: usize = per_action
            .values()
            .map(|count| count.saturating_sub(1))
            .sum();
        let limit = if follower { 0 } else { executing };
        Ok(vec![
            Check::expect(
                HELD,
                executing >= EXECUTING && executing < PROBES.len(),
                || format!("{executing} actions were executing"),
            ),
            Check::expect(TWO_WORKERS, two_workers, || {
                "a second worker did not register while the build was held".to_owned()
            }),
            Check::expect(
                WORKER_PID,
                !attempts.is_empty()
                    && attempts
                        .iter()
                        .all(|((_, pid), attempt)| attempt.worker != *pid),
                || format!("worker ids must not be attempt ids: {attempts:?}"),
            ),
            Check::expect(BUILD, outcome.succeeded(), || outcome.exit_summary()),
            Check::expect(TAKEOVER, elected && elapsed <= takeover_bound(), || {
                format!(
                    "leader available={elected} after {elapsed:?}; bound {:?}",
                    takeover_bound()
                )
            }),
            Check::expect(
                REEXECUTIONS,
                repeats <= limit && per_action.len() == PROBES.len(),
                || {
                    format!(
                        "{repeats} re-executions, limit {limit}, {} distinct actions",
                        per_action.len()
                    )
                },
            ),
            Check::expect(NO_OVERLAP, !overlapping_attempts(&attempts), || {
                format!("overlapping attempts: {attempts:?}")
            }),
        ])
    })
}
