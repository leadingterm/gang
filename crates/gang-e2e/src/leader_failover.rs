//! Failover while queued demand requires another worker, including a paused former leader.

use std::collections::BTreeSet;
use std::fs;
use std::time::{Duration, Instant};

use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, ServerTrust};
use gang_types::health::CLIENT_REROUTE_BOUND;
use gang_wire::page::{PAGE_SIZE_MAX, PageToken};
use gang_wire::routes::LIST_EVENTS;
use gang_wire::{Event, EventChange, ListEventsResponse, PassState, SkipReason, State};
use http::Method;
use http::header::{AUTHORIZATION, HeaderValue};

use crate::check::{Check, awaited};
use crate::cluster::Cluster;
use crate::control::Control;
use crate::errors::{E2eError, Result};
use crate::event_log::sequence;
use crate::execution::Run;
use crate::held_build::{Hold, joined};
use crate::http::Http;
use crate::journal::Journal;
use crate::names::leader_failover::{
    BUILD, DEMAND, OLD_WORKERS, SCALE_OUT, TAKEOVER, ZOMBIE_EVENTS, ZOMBIE_EXIT, ZOMBIE_RECORDS,
    ZOMBIE_REROUTED,
};
use crate::sse::{self, EventStream, STREAM_OK};
use crate::state_api::StateApi;
use crate::system::Probe;
use crate::tls_client;

// Lease TTL (10s), clock skew (2s), renewal interval (3s), plus a margin.
const PAUSE_PAST_VALIDITY: Duration = Duration::from_secs(16);
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
const LOST_LEADERSHIP: &str = "another controller took the leader lease";

fn instance_names(state: &State) -> BTreeSet<String> {
    state
        .workers
        .iter()
        .map(|worker| worker.instance.clone())
        .chain(state.instances.iter().map(|instance| instance.name.clone()))
        .collect()
}

fn records(cluster: &Cluster<'_>) -> Result<BTreeSet<String>> {
    let directory = cluster.stack().placement().state_dir().join("instances");
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(&directory).map_err(E2eError::io("listing local instances"))? {
        let entry = entry.map_err(E2eError::io("reading local instance directory"))?;
        if entry.path().join("instance.json").is_file() {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| E2eError::system("local instance name is not UTF-8"))?;
            names.insert(name);
        }
    }
    Ok(names)
}

fn event_history(cluster: &Cluster<'_>, leader: usize) -> Result<Vec<Event>> {
    let stack = cluster.stack();
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let pem = credentials
        .authority
        .as_deref()
        .map(tls_client::certificate_pem)
        .transpose()?;
    let client = Http::try_new(
        HttpTransport::Tls {
            trust: pem.as_ref().map_or(ServerTrust::System, ServerTrust::Ca),
        },
        HttpProtocol::Http2,
        stack.settings().handshake_timeout,
    )?;
    let mut authorization = HeaderValue::from_str(&format!("Bearer {}", credentials.token))
        .map_err(E2eError::system_from(
            "the bearer token is not a header value",
        ))?;
    authorization.set_sensitive(true);
    let headers = [(AUTHORIZATION, authorization)];
    let base = cluster
        .direct_endpoint(leader)
        .join(LIST_EVENTS.template)
        .map_err(E2eError::parse("the events list URL"))?;
    let deadline = Instant::now() + stack.settings().history_timeout;
    let mut events = Vec::new();
    let mut token: Option<PageToken> = None;
    loop {
        if Instant::now() >= deadline {
            return Err(E2eError::timeout(
                "reading the durable events list",
                stack.settings().history_timeout,
            ));
        }
        let mut url = base.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("pageSize", &PAGE_SIZE_MAX.to_string());
            if let Some(token) = &token {
                query.append_pair("pageToken", token.as_str());
            }
        }
        let response = client.send(HttpRequest {
            method: Method::GET,
            url: &url,
            bearer: None,
            headers: &headers,
            body: None,
        })?;
        if !response.status.is_success() {
            return Err(E2eError::system(format!(
                "{url} answered {}: {}",
                response.status,
                String::from_utf8_lossy(&response.body)
            )));
        }
        let page: ListEventsResponse =
            serde_json::from_slice(&response.body).map_err(E2eError::parse("the events list"))?;
        events.extend(page.events);
        match page.next_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    events.reverse();
    Ok(events)
}

fn wait_state(
    cluster: &Cluster<'_>,
    leader: usize,
    done: impl FnMut(&State) -> bool,
) -> Result<State> {
    wait_state_until(
        cluster,
        leader,
        Instant::now() + cluster.stack().settings().history_timeout,
        done,
    )
}

fn wait_state_until(
    cluster: &Cluster<'_>,
    leader: usize,
    deadline: Instant,
    mut done: impl FnMut(&State) -> bool,
) -> Result<State> {
    let stack = cluster.stack();
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let mut stream = StateApi::with_bearer(
        &cluster.direct_endpoint(leader),
        &credentials,
        stack.settings().handshake_timeout,
    )?
    .open_stream()?;
    if stream.status() != STREAM_OK {
        return Err(E2eError::system(format!(
            "state stream answered {}",
            stream.status()
        )));
    }
    sse::await_state(
        &mut stream,
        deadline.saturating_duration_since(Instant::now()),
        &mut done,
    )
}

// Logs survive retirement, unlike instance records, and name every spawned worker.
fn launched(cluster: &Cluster<'_>) -> Result<BTreeSet<String>> {
    let directory = cluster.stack().placement().state_dir().join("logs");
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(directory).map_err(E2eError::io("listing local worker logs"))? {
        let entry = entry.map_err(E2eError::io("reading local worker log"))?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "log") {
            let name = path
                .file_stem()
                .and_then(|name| name.to_str())
                .ok_or_else(|| E2eError::system("local worker log name is not UTF-8"))?;
            names.insert(format!("instances/{name}"));
        }
    }
    Ok(names)
}

fn old_instances_intact(
    cluster: &Cluster<'_>,
    old: &BTreeSet<String>,
    events: &[Event],
) -> Result<bool> {
    for name in old {
        let reference = format!("instances/{name}");
        if events.iter().any(|event| {
            matches!(&event.change,
            EventChange::Terminated { instance } if instance == &reference)
        }) {
            return Ok(false);
        }
        let path = cluster
            .stack()
            .placement()
            .state_dir()
            .join("instances")
            .join(name)
            .join("instance.json");
        match fs::read(&path) {
            Ok(bytes) => {
                let record: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(E2eError::system_from("reading local instance record"))?;
                if !record
                    .get("terminating_since")
                    .is_some_and(serde_json::Value::is_null)
                {
                    return Ok(false);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !events.iter().any(|event| {
                    matches!(&event.change,
                    EventChange::ExitedEarly { instance, .. } if instance == &reference)
                }) {
                    return Ok(false);
                }
            }
            Err(error) => return Err(E2eError::io("reading local instance record")(error)),
        }
    }
    Ok(true)
}

fn suffix<'a>(before: &[Event], after: &'a [Event]) -> Result<&'a [Event]> {
    let floor = before.last().map(sequence).transpose()?.unwrap_or(0);
    for (index, event) in after.iter().enumerate() {
        if sequence(event)? > floor {
            return Ok(&after[index..]);
        }
    }
    Ok(&after[after.len()..])
}

fn changes(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event.change,
                EventChange::ScaledOut { .. } | EventChange::Terminated { .. }
            )
        })
        .count()
}

fn capacity_check(
    cluster: &Cluster<'_>,
    before_launches: &BTreeSet<String>,
    state: &State,
    events: &[Event],
) -> Result<Check> {
    let launches = launched(cluster)?;
    let added: BTreeSet<_> = launches.difference(before_launches).cloned().collect();
    // Local's supply reconciliation replaces an early-exited worker without a
    // second ScaleOut decision. Its retained log and ExitedEarly event explain
    // the extra launch without excusing an additional live worker.
    let failed = events
        .iter()
        .filter(|event| match &event.change {
            EventChange::ExitedEarly { instance, .. } => added.contains(instance),
            _ => false,
        })
        .count();
    let scale_outs = events
        .iter()
        .filter(|event| matches!(event.change, EventChange::ScaledOut { .. }))
        .count();
    let instances = instance_names(state).len();
    Ok(Check::expect(
        SCALE_OUT,
        instances == 2 && scale_outs <= 1 && added.len().saturating_sub(failed) <= 1,
        || {
            format!(
                "{instances} instances, {scale_outs} scale-outs, launches {added:?}, {failed} early exits"
            )
        },
    ))
}

fn observe_zombie(
    cluster: &Cluster<'_>,
    old: usize,
    leader: usize,
    started: Instant,
    release: impl FnOnce() -> Result<()>,
) -> Result<Vec<Check>> {
    // Keep the old process stopped beyond lease expiry and until the new
    // leader's demand scale-out is visible and its instances are present.
    wait_state(cluster, leader, |state| {
        started.elapsed() >= PAUSE_PAST_VALIDITY && instance_names(state).len() >= 2
    })?;
    let before_resume = event_history(cluster, leader)?;
    let local_before = launched(cluster)?;
    cluster.resume(old)?;
    // Lost leadership drains executing work before exiting.
    release()?;
    let status = cluster.wait_exit(old)?;
    let log = cluster.log(old)?;
    let after_exit = event_history(cluster, leader)?;
    let local_after = launched(cluster)?;
    let new_events = suffix(&before_resume, &after_exit)?;
    let added: Vec<_> = local_after.difference(&local_before).collect();
    Ok(vec![
        Check::expect(ZOMBIE_EVENTS, changes(new_events) == 0, || {
            format!("changes after resume: {new_events:?}")
        }),
        Check::expect(ZOMBIE_RECORDS, added.is_empty(), || {
            format!("new local instance records after resume: {added:?}")
        }),
        Check::expect(
            ZOMBIE_EXIT,
            !status.success() && log.contains(LOST_LEADERSHIP),
            || {
                format!(
                    "exit {status}; lost leadership message present: {}",
                    log.contains(LOST_LEADERSHIP)
                )
            },
        ),
    ])
}

fn wait_for_rerouted_demand(
    cluster: &Cluster<'_>,
    leader: usize,
    old: usize,
    started: Instant,
) -> Result<Check> {
    let mut recovery = None;
    let result = wait_state_until(cluster, leader, started + CLIENT_REROUTE_BOUND, |state| {
        if state.queue.total > 0 || state.workers.iter().any(|worker| worker.running > 0) {
            recovery = Some(started.elapsed());
            true
        } else {
            false
        }
    });
    let check = awaited(ZOMBIE_REROUTED, result)?;
    if check.is_failure() {
        Ok(Check::failed(
            ZOMBIE_REROUTED,
            format!(
                "no demand reached controller {leader} within {CLIENT_REROUTE_BOUND:?} of freezing controller {old}: Bazel stayed pinned to the frozen controller"
            ),
        ))
    } else {
        if let Some(recovery) = recovery {
            eprintln!(
                "leader failover: Bazel reached the new leader {recovery:?} after the freeze (bound {CLIENT_REROUTE_BOUND:?})"
            );
        }
        Ok(check)
    }
}

fn demand_ready(state: &State) -> bool {
    state.actions.as_ref().is_some_and(|actions| {
        !actions.queued.is_empty()
            && actions.running.len() + actions.queued.len() == PROBES.len()
            && actions.truncated.running == 0
            && actions.truncated.queued == 0
    }) && !state.workers.is_empty()
        && instance_names(state).len() == 1
}

/// Records queued demand and the observed leader takeover.
fn initial_checks(
    state: &State,
    old_instances: &BTreeSet<String>,
    old: usize,
    leader: usize,
) -> Vec<Check> {
    vec![
        Check::expect(
            DEMAND,
            state
                .actions
                .as_ref()
                .is_some_and(|actions| !actions.queued.is_empty())
                && !old_instances.is_empty(),
            || {
                format!(
                    "queued detail count {}, existing instances {old_instances:?}",
                    state
                        .actions
                        .as_ref()
                        .map_or(0, |actions| actions.queued.len())
                )
            },
        ),
        Check::expect(TAKEOVER, leader != old, || {
            format!("old leader {old}, observed {leader}")
        }),
    ]
}

fn hold_supply_until_skipped(cluster: &Cluster<'_>, old: usize) -> Result<()> {
    let stack = cluster.stack();
    stack.hold_supply()?;
    if let Err(error) = wait_state(cluster, old, |state| {
        state.pass.state == PassState::Skipped
            && state.pass.skip_reason == Some(SkipReason::ProvisionerUnavailable)
    }) {
        if let Err(release_error) = stack.release_supply() {
            eprintln!("failed to restore supply after barrier error: {release_error}");
        }
        return Err(error);
    }
    Ok(())
}

fn restore_supply(cluster: &Cluster<'_>, observation: Result<Vec<Check>>) -> Result<Vec<Check>> {
    match (observation, cluster.stack().release_supply()) {
        (Err(error), Err(release_error)) => {
            eprintln!("failed to restore supply after observation error: {release_error}");
            Err(error)
        }
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(checks), Ok(())) => Ok(checks),
    }
}

fn build_check(checks: &[Check], outcome: &crate::build_output::BuildOutcome) -> Check {
    if checks
        .iter()
        .any(|check| check.name == ZOMBIE_REROUTED && check.is_failure())
    {
        Check::skipped(BUILD, "no rerouted demand")
    } else {
        Check::expect(BUILD, outcome.succeeded(), || outcome.exit_summary())
    }
}

/// Kills or pauses the leader while held actions create queued demand.
///
/// The suite must configure one minimum and two maximum instances, two CPUs per
/// instance, and the lease timing documented above. Each variant needs a fresh cluster.
///
/// # Errors
/// Returns an error if the cluster, journal, or build cannot be observed.
pub fn leader_failover(cluster: &Cluster<'_>, zombie: bool) -> Result<Vec<Check>> {
    let old = cluster.wait_for_leader()?;
    // Minimum capacity makes the old-worker verdict non-vacuous before submitting demand.
    wait_state(cluster, old, |state| !state.workers.is_empty())?;
    let before = event_history(cluster, old)?;
    let signal = cluster.stack().signal_path();
    let journal = Journal::create(
        &signal.with_file_name("journal"),
        cluster.stack().settings().hold_timeout,
    )?;
    let go = signal.with_file_name("go");
    let run = Run::fresh()?;
    journal.prepare_attempts(run.as_ref(), PROBES.iter().map(|probe| probe.as_str()))?;
    std::thread::scope(|scope| -> Result<Vec<Check>> {
        let stack = cluster.stack();
        hold_supply_until_skipped(cluster, old)?;
        let mut supply_held = true;
        let hold = Hold::new(&go, |root| cluster.stack().kill_bazel_in(root));
        let build = hold.build(scope, None, |cancel, stop| {
            cluster.stack().build_run_until(&PROBES, &run, cancel, stop)
        });
        let observation = (|| -> Result<Vec<Check>> {
            let state = wait_state(cluster, old, demand_ready)?;
            let old_instances = records(cluster)?;
            let before_launches = launched(cluster)?;
            let started = Instant::now();
            if zombie {
                cluster.pause(old)?;
            } else {
                cluster.kill(old)?;
            }
            let observed = (|| -> Result<Vec<Check>> {
                supply_held = false;
                stack.release_supply()?;
                let leader = cluster.wait_for_leader_matching(
                    cluster.stack().settings().startup_timeout,
                    |leader| leader != old,
                )?;
                let mut rerouted = None;
                if zombie {
                    rerouted = Some(wait_for_rerouted_demand(cluster, leader, old, started)?);
                }
                let mut checks = initial_checks(&state, &old_instances, old, leader);
                if let Some(check) = rerouted {
                    let failed = check.is_failure();
                    checks.push(check);
                    if failed {
                        cluster.resume(old)?;
                        checks.extend(Check::skipped_all(
                            [
                                ZOMBIE_EVENTS,
                                ZOMBIE_RECORDS,
                                ZOMBIE_EXIT,
                                SCALE_OUT,
                                OLD_WORKERS,
                            ],
                            "no rerouted demand",
                        ));
                        return Ok(checks);
                    }
                }
                let new_state =
                    wait_state(cluster, leader, |state| instance_names(state).len() >= 2)?;
                if zombie {
                    checks.extend(observe_zombie(cluster, old, leader, started, || {
                        hold.release()
                    })?);
                }
                let events = event_history(cluster, leader)?;
                let demand_events = suffix(&before, &events)?;
                checks.push(capacity_check(
                    cluster,
                    &before_launches,
                    &new_state,
                    demand_events,
                )?);
                let intact = old_instances_intact(cluster, &old_instances, demand_events)?;
                checks.push(Check::expect(OLD_WORKERS, intact, || {
                    format!("an old instance was terminated or disappeared: {old_instances:?}")
                }));
                Ok(checks)
            })();
            if zombie && observed.is_err() {
                // A stopped child must not be left behind even when observation fails.
                let _ = cluster.resume(old);
            }
            observed
        })();
        let observation = if supply_held {
            restore_supply(cluster, observation)
        } else {
            observation
        };
        let settled = hold.settle(observation, || Ok(()));
        // Keep the FIFO alive for the probe writers until the build has finished.
        let _journal = &journal;
        let completed = joined(build);
        hold.disarm();
        let mut checks = settled?;
        let outcome = completed?;
        checks.push(build_check(&checks, &outcome));
        Ok(checks)
    })
}

#[cfg(test)]
mod tests {
    use gang_wire::{Actions, Instance, QueuedAction, RunningAction, State, Worker};

    use super::{PROBES, demand_ready};

    fn full_demand() -> State {
        State {
            workers: vec![Worker {
                instance: "instances/one".to_owned(),
                ..Worker::default()
            }],
            actions: Some(Actions {
                running: vec![RunningAction::default(); PROBES.len() - 1],
                queued: vec![QueuedAction::default()],
                ..Actions::default()
            }),
            ..State::default()
        }
    }

    #[test]
    fn demand_uses_action_details_when_queue_summary_is_stale() {
        let state = full_demand();
        assert_eq!(state.queue.total, 0);
        assert!(demand_ready(&state));
    }

    #[test]
    fn demand_requires_complete_untruncated_details_and_queued_work() {
        let mut state = full_demand();
        let actions = state.actions.as_mut().expect("fixture has actions");
        actions.running.pop();
        assert!(!demand_ready(&state));

        let mut state = full_demand();
        let actions = state.actions.as_mut().expect("fixture has actions");
        actions.queued.clear();
        actions.running.push(RunningAction::default());
        assert!(!demand_ready(&state));

        for running in [true, false] {
            let mut state = full_demand();
            let actions = state.actions.as_mut().expect("fixture has actions");
            if running {
                actions.truncated.running = 1;
            } else {
                actions.truncated.queued = 1;
            }
            assert!(!demand_ready(&state));
        }
    }

    #[test]
    fn demand_requires_one_instance_and_a_worker() {
        let mut state = full_demand();
        state.instances.push(Instance {
            name: "instances/two".to_owned(),
            ..Instance::default()
        });
        assert!(!demand_ready(&state));

        let mut state = full_demand();
        state.workers.clear();
        assert!(!demand_ready(&state));
    }
}
