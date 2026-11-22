//! Rolling controller replacement while a build has executing and queued actions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::process::ExitStatus;
use std::time::Duration;

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

// Release only after an actual successor is healthy, never after a guessed routing delay.
fn release_after_promotion(
    promote: impl FnOnce() -> Result<()>,
    observe_held: impl FnOnce() -> Result<()>,
    release: impl FnOnce() -> Result<()>,
) -> Result<()> {
    promote()?;
    observe_held()?;
    release()
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

fn server_stop_timeout(config: &str) -> Result<Duration> {
    let document: toml::Value = toml::from_str(config).map_err(crate::template::template_error)?;
    let server: gang_config::ServerConfig = document
        .get("server")
        .cloned()
        .ok_or_else(|| E2eError::system("missing controller server configuration"))?
        .try_into()
        .map_err(crate::template::template_error)?;
    Ok(server.stop_timeout.get())
}

fn observe_stable_host_priority(cluster: &Cluster<'_>) -> Result<()> {
    let old = cluster.wait_for_leader()?;
    let original_pid = cluster.controller_pid(old)?;
    let successor = cluster.start_controller()?;
    cluster
        .wait_for_controller_leader_within(successor, cluster.stack().settings().startup_timeout)?;
    cluster.wait_for_controller_health(successor)?;
    cluster.observe_restarted_standby(old, successor, original_pid)?;
    cluster.deregister_controller(old)?;
    cluster.retire(old)?;
    Ok(())
}

fn replace_controllers(
    cluster: &Cluster<'_>,
    attempts: &Attempts,
    work_is_held: impl Fn() -> Result<bool>,
    release: impl FnOnce() -> Result<()>,
) -> Result<(bool, Vec<ExitStatus>)> {
    let initial_leader = cluster.wait_for_leader()?;
    let config = fs::read_to_string(cluster.release_config_path(initial_leader)?)
        .map_err(E2eError::io("reading original controller configuration"))?;
    let stop_timeout = server_stop_timeout(&config)?;
    let original_holder = cluster.controller_identity(initial_leader)?;
    let original_attempts: BTreeMap<_, _> = attempts
        .iter()
        .map(|(key, attempt)| (key.clone(), (attempt.worker, attempt.start)))
        .collect();
    cluster.observe_unprepared_candidate(initial_leader, &work_is_held)?;
    if !cluster.controller_is_leader(initial_leader)?
        || !cluster.controller_is_healthy(initial_leader)?
    {
        return Err(E2eError::system(
            "original controller lost leadership after unprepared candidate collection",
        ));
    }
    let retired_leader = cluster.controller_is_leader(initial_leader)?;
    let next = cluster.start_controller()?;
    let mut retiring = None;
    release_after_promotion(
        || {
            cluster
                .wait_for_leader_matching(cluster.stack().settings().startup_timeout, |leader| {
                    leader == next
                })?;
            cluster.wait_for_controller_health(next)
        },
        || {
            if !work_is_held()?
                || cluster.controller_identity(initial_leader)? != original_holder
                || original_attempts.iter().any(|(key, &(worker, start))| {
                    !attempts.get(key).is_some_and(|attempt| {
                        attempt.worker == worker && attempt.start == start && attempt.end.is_none()
                    })
                })
            {
                return Err(E2eError::system(
                    "original controller/worker/timestamp changed while work was held",
                ));
            }
            Ok(())
        },
        || {
            retiring = Some(cluster.begin_retire(initial_leader)?);
            release()
        },
    )?;
    let retiring = retiring.ok_or_else(|| E2eError::system("missing retirement signal"))?;
    let natural = cluster.finish_retire_within(&retiring, stop_timeout)?;
    let other = usize::from(initial_leader == 0);
    cluster.deregister_controller(initial_leader)?;
    cluster.deregister_controller(other)?;
    Ok((retired_leader, vec![natural, cluster.retire(other)?]))
}

/// Replaces the initial controllers while the build is held, then judges its journal.
///
/// # Errors
/// Returns an error if the cluster, journal, or build cannot be observed.
pub fn rolling_deploy(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    observe_stable_host_priority(cluster)?;
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
            let (retired_leader, statuses) = replace_controllers(
                cluster,
                &attempts,
                || Ok(!build.is_finished() && pending.is_held()?),
                || pending.release(),
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
    use parking_lot::Mutex;

    use super::*;

    #[test]
    fn stop_budget_comes_from_rendered_server_configuration() {
        let default = server_stop_timeout("[server]\ntransport = 'plaintext'\n")
            .expect("default server configuration");
        let configured =
            server_stop_timeout("[server]\ntransport = 'plaintext'\nstop_timeout = '43m'\n")
                .expect("configured stop timeout");
        assert_eq!(configured, Duration::from_mins(43));
        assert_ne!(default, configured);
        assert!(
            server_stop_timeout("[server]\ntransport = 'plaintext'\nstop_timeout = '0s'\n")
                .is_err()
        );
    }

    #[test]
    fn successor_health_and_original_identity_precede_release() {
        let events = Mutex::new(Vec::new());
        release_after_promotion(
            || {
                events.lock().push("healthy");
                Ok(())
            },
            || {
                events.lock().push("original held");
                Ok(())
            },
            || {
                events.lock().push("release");
                Ok(())
            },
        )
        .expect("handover");
        assert_eq!(events.into_inner(), ["healthy", "original held", "release"]);
    }

    #[test]
    fn failed_promotion_never_releases_original_work() {
        release_after_promotion(
            || Err(E2eError::system("successor unhealthy")),
            || panic!("must not observe after failed promotion"),
            || panic!("must not release after failed promotion"),
        )
        .expect_err("unhealthy successor");
    }
}
