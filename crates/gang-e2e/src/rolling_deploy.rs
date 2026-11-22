//! Rolling controller replacement both idle and with executing and queued actions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use gang_client::{ClientOptions, ServerApi};
use gang_clock::SystemClock;
use gang_tls::ServerTrust;

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
const IDLE_CLIENTS: usize = 4;
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

fn idle_client_requests(
    cluster: &Cluster<'_>,
    stop: &AtomicBool,
    ready: mpsc::Sender<()>,
) -> Result<usize> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(E2eError::io("starting idle rolling-deploy client"))?;
    let endpoint = cluster.stack().endpoint()?;
    let pin = cluster.stack().ca_pin()?;
    let mut completed = 0;
    loop {
        // Every caller also starts one final request after the observed NLB transition.
        let finishing = stop.load(Ordering::Acquire);
        // A new client forces a fresh public TCP/TLS connection on every request. Only the
        // production API's existing not-started retry policy may recover a refusal.
        let client = ServerApi::try_new(
            &endpoint,
            ServerTrust::Pin(&pin),
            &ClientOptions::default(),
            Arc::new(SystemClock::new()),
        )
        .map_err(E2eError::system_from("creating idle rolling-deploy client"))?;
        runtime
            .block_on(client.server(None))
            .map_err(E2eError::system_from("idle public TLS request"))?;
        completed += 1;
        if completed == 1 {
            ready
                .send(())
                .map_err(E2eError::system_from("announcing idle client readiness"))?;
        }
        if finishing {
            return Ok(completed);
        }
    }
}

fn idle_replacement(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    // No build has been launched: neither an executing attempt nor a held action can keep
    // the outgoing process alive and conceal premature listener shutdown.
    stack.wait_live_state(stack.settings().history_timeout, |state| {
        state.queue.total == 0 && state.workers.iter().all(|worker| worker.running == 0)
    })?;
    let old = cluster.wait_for_leader()?;
    let original_pid = cluster.suppress_controller_restart(old)?;
    let config = fs::read_to_string(cluster.release_config_path(old)?)
        .map_err(E2eError::io("reading idle controller configuration"))?;
    let stop_timeout = server_stop_timeout(&config)?;
    let stop = AtomicBool::new(false);
    let completed = std::thread::scope(|scope| -> Result<usize> {
        let (ready, receivers) = mpsc::channel();
        let clients: Vec<_> = (0..IDLE_CLIENTS)
            .map(|_| {
                let ready = ready.clone();
                let stop = &stop;
                scope.spawn(move || {
                    let result = idle_client_requests(cluster, stop, ready);
                    if result.is_err() {
                        stop.store(true, Ordering::Release);
                    }
                    result
                })
            })
            .collect();
        drop(ready);
        let observation = (|| -> Result<()> {
            for _ in 0..IDLE_CLIENTS {
                receivers
                    .recv_timeout(stack.settings().startup_timeout)
                    .map_err(E2eError::system_from("waiting for idle public TLS clients"))?;
            }
            let successor = cluster.start_controller()?;
            cluster
                .wait_for_controller_leader_within(successor, stack.settings().startup_timeout)?;
            cluster.wait_for_controller_health(successor)?;
            // Observe the actual NLB threshold transition, not a guessed routing delay.
            cluster.wait_for_serving(&[successor])?;
            let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
            crate::upgrade_probe::probe_refusal(
                &stack.endpoint()?,
                &credentials,
                cluster.controller_public_target(old)?,
                bytes::Bytes::from_static(b"{}"),
                stack.settings().handshake_timeout,
            )?;
            cluster.observe_yielded_controller(old, original_pid)?;
            Ok(())
        })();
        stop.store(true, Ordering::Release);
        let mut completed = 0;
        let mut client_error = None;
        for client in clients {
            match client
                .join()
                .map_err(|_| E2eError::system("idle public TLS client panicked"))
                .and_then(|result| result)
            {
                Ok(count) => completed += count,
                Err(error) => {
                    client_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = client_error {
            return Err(error);
        }
        observation?;
        Ok(completed)
    })?;
    // The supervisor still owns this exact child. Only now deliver real SIGTERM and
    // require its successful, naturally reaped exit within the configured stop budget.
    cluster.observe_yielded_controller(old, original_pid)?;
    let retiring = cluster.begin_retire(old)?;
    let status = cluster.finish_retire_within(&retiring, stop_timeout)?;
    cluster.deregister_controller(old)?;
    Ok(vec![
        Check::expect(
            "rolling deploy idle: concurrent public TLS requests have zero transport errors",
            completed >= IDLE_CLIENTS * 2,
            || format!("{completed} requests completed across {IDLE_CLIENTS} clients"),
        ),
        Check::passed(
            "rolling deploy idle: original PID serves complete refusal and health until SIGTERM",
        ),
        Check::expect(
            "rolling deploy idle: original controller exits successfully after SIGTERM",
            status.success(),
            || format!("original PID {original_pid} retirement status: {status}"),
        ),
    ])
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
    let idle_checks = idle_replacement(cluster)?;
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
        checks.extend(idle_checks);
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
