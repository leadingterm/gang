//! Held actions and retried demand during a shared database outage.

use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gang_clock::{Clock, SystemClock};
use gang_tls::{HttpProtocol, HttpTransport, ServerTrust};
use gang_wire::EventChange;
use gang_wire::routes::{CREATE_CERTIFICATE, CREATE_DEVICE_AUTHORIZATION};
use http::Method;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Digest, GetActionResultRequest, RequestMetadata, digest_function,
};

use crate::check::{Check, awaited};
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::execution::{JOURNAL_END, Run};
use crate::h2_frames::H2Channel;
use crate::held_build::{Hold, joined};
use crate::identity::Admission;
use crate::journal::Journal;
use crate::names::database_outage::{
    BUILD, DEMAND_PENDING, ENROLLMENT_REFUSED, HELD, LEADER, LOGIN, NO_EVENTS, ONE_SCALE_OUT,
    PARTITION_BUILD, PARTITION_EXIT, PARTITION_FENCED, PARTITION_HELD, PARTITION_SINGLE,
    PARTITION_TAKEOVER, PAUSED, REFUSED, RESUMED, REVOCATION_REFUSED,
};
use crate::process;
use crate::server::{Presented, Request, ServerApi};
use crate::stack::{DEMAND_ROOT, FOLLOWER_ROOT};
use crate::system::{Probe, System};

// These are the default leadership settings. The suite must not override them.
const TTL: Duration = Duration::from_secs(10);
const RENEW_INTERVAL: Duration = Duration::from_secs(3);
const CLOCK_SKEW: Duration = Duration::from_secs(2);
const VALIDITY: Duration = TTL.saturating_sub(CLOCK_SKEW);
const OUTAGE: Duration = TTL.saturating_mul(3);
const UNAVAILABLE: u16 = 503;
const CREATED: u16 = 200;
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

fn device_login(cluster: &Cluster<'_>) -> Result<u16> {
    let request = Request::new(Method::POST, CREATE_DEVICE_AUTHORIZATION.template);
    Ok(cluster.stack().send(&request)?.status)
}

fn worker_enrollment(cluster: &Cluster<'_>) -> Result<Request> {
    let stack = cluster.stack();
    let instances = stack.placement().state_dir().join("instances");
    let entries = fs::read_dir(&instances).map_err(E2eError::io("reading local instances"))?;
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry.map_err(E2eError::io("reading local instance"))?;
        let path = entry.path().join("instance.json");
        if path.is_file() {
            records.push(path);
        }
    }
    let [record] = records.as_slice() else {
        return Err(E2eError::system("expected exactly one local instance"));
    };
    let bytes = fs::read(record).map_err(E2eError::io("reading local instance record"))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(E2eError::system_from("parsing local instance record"))?;
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| E2eError::system("instance record has no id"))?;
    let secret = value
        .get("secret")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| E2eError::system("instance record has no secret"))?;
    let access = gang_types::AccessToken::new(secret);
    let header =
        http::HeaderValue::from_str(id).map_err(E2eError::system_from("encoding instance id"))?;
    let headers = [(http::HeaderName::from_static("x-gang-instance"), header)];
    let url = gang_types::Url::parse(&format!(
        "http://127.0.0.1:{}/latest/meta-data/gang/workload-token",
        stack.placement().imds_port()
    ))
    .map_err(E2eError::system_from("forming metadata URL"))?;
    let http = crate::http::Http::try_new(
        gang_tls::HttpTransport::Plain,
        gang_tls::HttpProtocol::Negotiated,
        stack.settings().handshake_timeout,
    )?;
    let reply = http.send(gang_tls::HttpRequest {
        method: Method::GET,
        url: &url,
        bearer: Some(&access),
        headers: &headers,
        body: None,
    })?;
    if !reply.status.is_success() {
        return Err(E2eError::system(format!(
            "worker metadata answered {}",
            reply.status
        )));
    }
    let proof =
        std::str::from_utf8(&reply.body).map_err(E2eError::system_from("decoding worker proof"))?;
    let config = fs::read_to_string(stack.placement().config_file())
        .map_err(E2eError::io("reading worker configuration"))?;
    let document: toml::Value = toml::from_str(&config).map_err(crate::template::template_error)?;
    let provider = document
        .get("authentication")
        .and_then(|value| value.get("workers"))
        .and_then(|value| value.get("mtls"))
        .and_then(|value| value.get("provider"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| E2eError::system("worker provider is missing"))?;
    let key =
        gang_mtls::KeyPair::generate().map_err(E2eError::system_from("generating worker key"))?;
    let csr = key
        .certificate_request()
        .map_err(E2eError::system_from("generating worker CSR"))?;
    let body = serde_json::to_vec(
        &serde_json::json!({"provider": provider, "proof": proof, "pemCsr": csr}),
    )
    .map_err(E2eError::system_from("serializing worker enrollment"))?;
    Ok(Request::new(Method::POST, CREATE_CERTIFICATE.template).body("application/json", body))
}

fn outage_requests(cluster: &Cluster<'_>) -> Result<(Request, Request)> {
    let stack = cluster.stack();
    let enroll = worker_enrollment(cluster)?;
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let revoke = Request::new(
        Method::POST,
        format!(
            "/v1/principals/{}%3Aoutage-enrollment:revoke",
            crate::template::PROVIDER
        ),
    )
    .presenting(Presented::Bearer(credentials.token));
    Ok((enroll, revoke))
}

/// Reopens a balancer stream once if the controller serving it exits during handoff.
fn wait_state(
    cluster: &Cluster<'_>,
    mut done: impl FnMut(&gang_wire::State) -> bool,
) -> Result<gang_wire::State> {
    let stack = cluster.stack();
    let timeout = stack.settings().history_timeout;
    let started = Instant::now();
    match stack.wait_live_state(timeout, &mut done) {
        Ok(state) => Ok(state),
        Err(error) => {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(error);
            }
            stack.wait_live_state(remaining, &mut done)
        }
    }
}

fn variant_error(count: usize, variant: &str, error: &E2eError) -> E2eError {
    E2eError::system(format!(
        "{count} controllers: {variant}: {}",
        gang_types::error_chain(error)
    ))
}

fn absent_action_result_request() -> GetActionResultRequest {
    GetActionResultRequest {
        action_digest: Some(Digest {
            hash: "0000000000000000000000000000000000000000000000000000000000000000".into(),
            size_bytes: 0,
        }),
        digest_function: digest_function::Value::Sha256.into(),
        ..Default::default()
    }
}

fn frontends_unready(serving: &[usize], workers: &[usize]) -> bool {
    serving.is_empty() && workers.is_empty()
}

fn observe_readiness_loss(
    cluster: &Cluster<'_>,
    leader: usize,
    clock: &SystemClock,
    mut demand_pending: impl FnMut() -> bool,
    client: &crate::http::Http,
    channel: &mut H2Channel,
) -> Result<(u16, Option<u32>, bool, bool, bool)> {
    let started = clock.monotonic();
    let endpoint = cluster.direct_endpoint(leader);
    let mut status = None;
    let mut grpc = None;
    let mut no_other_ready = true;
    let mut workers_at_lapse = None;
    let mut no_new_workers = true;
    let mut worker_error = None;
    let mut pending = true;
    wait_state(cluster, |_| {
        if clock.monotonic().saturating_duration_since(started) >= VALIDITY {
            let serving = cluster.serving();
            let workers = cluster.leaders();
            no_other_ready &= serving
                .iter()
                .chain(workers.iter())
                .all(|&index| index == leader);
            if status.is_some() {
                no_other_ready &= workers.is_empty();
                pending &= demand_pending();
            }
            if status.is_none() && frontends_unready(&serving, &workers) {
                pending &= demand_pending();
                let response = crate::login_across::exchange(
                    client,
                    &endpoint,
                    &Request::new(Method::POST, CREATE_DEVICE_AUTHORIZATION.template),
                );
                let rpc = channel.get_action_result(
                    &absent_action_result_request(),
                    &RequestMetadata::default(),
                    cluster.stack().settings().command_timeout,
                );
                match (response, rpc) {
                    (Ok(response), Ok(code)) => {
                        status = Some(response.status);
                        grpc = Some(code);
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        worker_error = Some(error);
                        return true;
                    }
                }
            }
            match cluster.worker_count() {
                Ok(count) => {
                    let initial = *workers_at_lapse.get_or_insert(count);
                    no_new_workers &= count <= initial;
                }
                Err(error) => {
                    worker_error = Some(error);
                    return true;
                }
            }
        }
        status.is_some() && clock.monotonic().saturating_duration_since(started) >= OUTAGE
    })?;
    if let Some(error) = worker_error {
        return Err(error);
    }
    Ok((
        status.ok_or_else(|| E2eError::system("outage client request was not sent"))?,
        grpc.flatten(),
        no_other_ready,
        no_new_workers && workers_at_lapse == Some(1),
        pending,
    ))
}

fn scaling_during_outage(
    events: &[gang_wire::Event],
    last_event: Option<&str>,
    start: gang_clock::Timestamp,
    end: gang_clock::Timestamp,
) -> usize {
    let later = last_event
        .and_then(|name| events.iter().position(|event| event.name == name))
        .map_or(events, |index| &events[index + 1..]);
    later
        .iter()
        .filter(|event| {
            event.create_time >= start
                && event.create_time <= end
                && matches!(
                    event.change,
                    EventChange::ScaledOut { .. }
                        | EventChange::Withdrawn { .. }
                        | EventChange::Terminated { .. }
                )
        })
        .count()
}

fn outage_clients(cluster: &Cluster<'_>, leader: usize) -> Result<(crate::http::Http, H2Channel)> {
    let pin = cluster.stack().ca_pin()?;
    let credentials = cluster.stack().administrator()?.credentials(&pin)?;
    let client = crate::http::Http::try_new(
        HttpTransport::Tls {
            trust: ServerTrust::Pin(&pin),
        },
        HttpProtocol::Http2,
        cluster.stack().settings().command_timeout,
    )?;
    let config = gang_tls::client_config(
        ServerTrust::Pin(&pin),
        None,
        &[b"h2"],
        Arc::new(SystemClock::new()),
    )
    .map_err(E2eError::system_from("configuring outage request TLS"))?;
    let mut channel = match H2Channel::connect(
        Arc::new(config),
        &cluster.direct_endpoint(leader),
        Some(&credentials.token),
        cluster.stack().settings().handshake_timeout,
    )? {
        Admission::Accepted(channel) => channel,
        Admission::Refused => {
            return Err(E2eError::system("leader refused outage test connection"));
        }
    };
    let baseline = channel.get_action_result(
        &absent_action_result_request(),
        &RequestMetadata::default(),
        cluster.stack().settings().command_timeout,
    )?;
    if baseline != Some(5) {
        return Err(E2eError::system(format!(
            "database outage baseline grpc-status {baseline:?}, expected 5"
        )));
    }
    Ok((client, channel))
}

fn observe_outage(
    cluster: &Cluster<'_>,
    build: &std::thread::ScopedJoinHandle<'_, Result<crate::build_output::BuildOutcome>>,
    journal: &Journal,
    run: &Run,
    demand_pending: impl FnMut() -> bool,
) -> Result<Vec<Check>> {
    let line = journal.read_line()?;
    if line == JOURNAL_END || !line.starts_with(&format!("{} loss start ", run.as_ref())) {
        return Err(E2eError::system(format!(
            "build did not hold an action: {line}"
        )));
    }
    wait_state(cluster, |state| state.workers.len() == 1)?;
    let leader = cluster.wait_for_leader()?;
    // Prime the administrator credential and take the last durable event before the outage.
    let before = cluster.scaling_events()?;
    let last_event = before.last().map(|event| event.name.clone());
    let (enroll, revoke) = outage_requests(cluster)?;
    let (client, mut channel) = outage_clients(cluster, leader)?;
    let clock = SystemClock::new();
    let outage_wall = clock.wall();
    cluster.database().set_unavailable(true)?;
    let during = observe_readiness_loss(
        cluster,
        leader,
        &clock,
        demand_pending,
        &client,
        &mut channel,
    );
    let endpoint = cluster.direct_endpoint(leader);
    let enroll_status = crate::login_across::exchange(&client, &endpoint, &enroll);
    let revoke_status = crate::login_across::exchange(&client, &endpoint, &revoke);
    cluster.database().set_unavailable(false)?;
    let restored_wall = clock.wall();
    let (status, grpc, no_other_ready, no_new_workers, pending) = during?;
    let enroll_status = enroll_status?.status;
    let revoke_status = revoke_status?.status;
    cluster.wait_for_single_leader(leader)?;
    cluster.wait_for_serving(&[leader])?;
    let login = device_login(cluster)?;
    let after = cluster.scaling_events()?;
    let outage_events =
        scaling_during_outage(&after, last_event.as_deref(), outage_wall, restored_wall);
    let resumed = awaited(
        RESUMED,
        wait_state(cluster, |state| state.workers.len() == 2).map(|_| ()),
    )?;
    let recoveries = cluster
        .scaling_events()?
        .iter()
        .filter(|event| {
            event.create_time >= restored_wall
                && matches!(event.change, EventChange::ScaledOut { from: 1, to: 2 })
        })
        .count();
    Ok(vec![
        Check::expect(HELD, !build.is_finished(), || {
            "build finished before the outage ended".to_owned()
        }),
        Check::expect(ENROLLMENT_REFUSED, enroll_status == UNAVAILABLE, || {
            format!("enrollment answered {enroll_status}, expected {UNAVAILABLE}")
        }),
        Check::expect(REVOCATION_REFUSED, revoke_status == UNAVAILABLE, || {
            format!("revocation answered {revoke_status}, expected {UNAVAILABLE}")
        }),
        Check::expect(REFUSED, status == UNAVAILABLE, || {
            format!("device login answered {status}, expected {UNAVAILABLE}")
        }),
        Check::expect(PAUSED, no_new_workers, || {
            "worker count increased after lease validity lapsed".to_owned()
        }),
        Check::expect(DEMAND_PENDING, pending, || {
            "new demand did not remain pending while the lease was invalid".to_owned()
        }),
        Check::expect(
            "database outage: promoted cache requests remain admitted",
            grpc == Some(5),
            || format!("grpc-status {grpc:?}, expected 5"),
        ),
        resumed,
        Check::expect(ONE_SCALE_OUT, recoveries == 1, || {
            format!("recovery recorded {recoveries} scale-outs from one to two")
        }),
        Check::expect(NO_EVENTS, outage_events == 0, || {
            format!("{outage_events} scaling events were recorded during the outage")
        }),
        Check::expect(
            LEADER,
            no_other_ready && cluster.leader() == Some(leader),
            || {
                format!(
                    "no controller ready after lapse: {no_other_ready}; original leader {leader}, current leader {:?}",
                    cluster.leader()
                )
            },
        ),
        Check::expect(LOGIN, login == CREATED, || {
            format!("device login answered {login}, expected {CREATED}")
        }),
    ])
}

/// Holds a build while the shared database is unavailable for three lease lifetimes.
///
/// # Errors
/// Returns an error if the build, controller, or its observations cannot be driven.
pub fn database_outage(cluster: &Cluster<'_>, controller_count: usize) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    let signal = stack.signal_path();
    let journal = Journal::create(
        &signal.with_file_name("journal"),
        stack.settings().hold_timeout,
    )
    .map_err(|error| variant_error(controller_count, "database outage setup", &error))?;
    let go = signal.with_file_name("go");
    let run = Run::fresh()?;
    let demand_run = Run::fresh()?;
    journal.prepare_attempts(run.as_ref(), PROBES[..2].iter().map(|probe| probe.as_str()))?;
    journal.prepare_attempts(
        demand_run.as_ref(),
        PROBES[2..4].iter().map(|probe| probe.as_str()),
    )?;
    let mut checks = std::thread::scope(|scope| -> Result<Vec<Check>> {
        let hold = Hold::new(&go, |root| stack.kill_bazel_in(root));
        let build = hold.build(scope, None, |cancel, stop| {
            let outcome = stack.build_run_until(&PROBES[..2], &run, cancel, stop);
            journal.write_line(JOURNAL_END)?;
            outcome
        });
        // Only server startup overlaps the held build; demand still starts after lease lapse.
        let mut preparation = Some(scope.spawn(|| stack.prepare_additional_in(DEMAND_ROOT)));
        let mut demand = None;
        let observation = observe_outage(cluster, &build, &journal, &run, || {
            let handle = demand.get_or_insert_with(|| {
                let preparation = preparation.take();
                let demand_run = &demand_run;
                let journal = &journal;
                hold.build(scope, Some(DEMAND_ROOT), move |_, stop| {
                    let preparation = preparation.ok_or_else(|| {
                        E2eError::system("demand Bazel preparation was already consumed")
                    })?;
                    let prepared = preparation.join().map_err(|_| {
                        E2eError::system("demand Bazel preparation thread panicked")
                    })??;
                    let outcome = prepared.build_until(&PROBES[2..4], demand_run, stop);
                    journal.write_line(JOURNAL_END)?;
                    outcome
                })
            });
            !handle.is_finished()
        });
        // Always restore the database before settling, including on an observation error.
        let restore = cluster.database().set_unavailable(false);
        let settled = hold.settle(observation, || Ok(()));
        let outcome = joined(build);
        let demand_outcome = demand.map(joined);
        hold.disarm();
        let mut checks = settled?;
        restore?;
        let outcome = outcome?;
        let demand_outcome = demand_outcome
            .ok_or_else(|| E2eError::system("outage demand build never started"))??;
        checks.push(Check::expect(
            BUILD,
            outcome.succeeded() && demand_outcome.succeeded(),
            || {
                format!(
                    "initial: {}; demand: {}",
                    outcome.exit_summary(),
                    demand_outcome.exit_summary()
                )
            },
        ));
        Ok(checks)
    })
    .map_err(|error| variant_error(controller_count, "database outage", &error))?;
    if controller_count == 2 {
        for _ in 0..2 {
            while journal.read_line()? != JOURNAL_END {}
        }
        fs::remove_file(&go).map_err(E2eError::io("resetting the held-action signal"))?;
        checks.extend(
            partition(cluster, &journal, &go)
                .map_err(|error| variant_error(controller_count, "database partition", &error))?,
        );
    }
    Ok(checks)
}

/// Worker processes launched with one controller's configuration.
fn controller_workers(cluster: &Cluster<'_>, controller: usize) -> BTreeSet<u32> {
    let config = cluster
        .stack()
        .placement()
        .root()
        .join(format!("controller-{controller}.toml"));
    let snapshot = process::snapshot();
    process::workers_of(&snapshot, &config)
        .into_iter()
        .map(|worker| worker.pid)
        .collect()
}

/// The harness must allow the former leader to detect loss and finish its configured drain.
fn check_exit_deadline(cluster: &Cluster<'_>) -> Result<()> {
    let stack = cluster.stack();
    let text = fs::read_to_string(stack.placement().config_file()).map_err(E2eError::io(
        "reading the partition controller configuration",
    ))?;
    let config: toml::Value = toml::from_str(&text).map_err(crate::template::template_error)?;
    let grace = config
        .get("server")
        .and_then(|value| value.get("http2"))
        .and_then(|value| value.get("shutdown_grace"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| E2eError::system("missing configured server.http2.shutdown_grace"))?;
    let shutdown_grace = gang_types::duration::parse(grace)
        .map_err(E2eError::system_from("parsing server.http2.shutdown_grace"))?;
    let required = TTL
        .saturating_add(RENEW_INTERVAL)
        .saturating_add(shutdown_grace);
    let timeout = stack.settings().startup_timeout;
    if timeout < required {
        return Err(E2eError::system(format!(
            "controller exit timeout {timeout:?} is below ttl + renewal interval + shutdown grace ({required:?})"
        )));
    }
    Ok(())
}

fn single_leader_check(cluster: &Cluster<'_>, follower: usize) -> Check {
    let single = cluster.wait_for_single_leader(follower).is_ok();
    let leaders = cluster.leaders();
    Check::expect(PARTITION_SINGLE, single && leaders == [follower], || {
        format!("healthy leader checks: {leaders:?}")
    })
}

fn partition(cluster: &Cluster<'_>, journal: &Journal, go: &std::path::Path) -> Result<Vec<Check>> {
    let run = Run::fresh()?;
    let demand_run = Run::fresh()?;
    let follower_run = Run::fresh()?;
    journal.prepare_attempts(run.as_ref(), PROBES[..4].iter().map(|probe| probe.as_str()))?;
    journal.prepare_attempts(
        demand_run.as_ref(),
        PROBES[4..].iter().map(|probe| probe.as_str()),
    )?;
    journal.prepare_attempts(
        follower_run.as_ref(),
        PROBES.iter().map(|probe| probe.as_str()),
    )?;
    std::thread::scope(|scope| -> Result<Vec<Check>> {
        let stack = cluster.stack();
        let hold = Hold::new(go, |root| stack.kill_bazel_in(root));
        let build = hold.build(scope, None, |cancel, stop| {
            let outcome = stack.build_run_until(&PROBES[..4], &run, cancel, stop);
            journal.write_line(JOURNAL_END)?;
            outcome
        });
        let mut demand_preparation = Some(scope.spawn(|| stack.prepare_additional_in(DEMAND_ROOT)));
        let mut follower_preparation =
            Some(scope.spawn(|| stack.prepare_additional_in(FOLLOWER_ROOT)));
        let mut demand = None;
        let mut follower = None;
        let mut cut_leader = None;
        let observation = observe_partition(
            cluster,
            journal,
            || hold.release(),
            &run,
            &build,
            |kind| match kind {
                Demand::Fenced => {
                    let preparation = demand_preparation.take();
                    demand = Some(hold.build(scope, Some(DEMAND_ROOT), |_, stop| {
                        let preparation = preparation.ok_or_else(|| {
                            E2eError::system("fenced demand preparation already consumed")
                        })?;
                        let prepared = preparation.join().map_err(|_| {
                            E2eError::system("demand Bazel preparation thread panicked")
                        })??;
                        prepared.build_until(&PROBES[4..], &demand_run, stop)
                    }));
                }
                Demand::Follower => {
                    let preparation = follower_preparation.take();
                    follower = Some(hold.build(scope, Some(FOLLOWER_ROOT), |_, stop| {
                        let preparation = preparation.ok_or_else(|| {
                            E2eError::system("follower demand preparation already consumed")
                        })?;
                        let prepared = preparation.join().map_err(|_| {
                            E2eError::system("follower Bazel preparation thread panicked")
                        })??;
                        prepared.build_until(&PROBES, &follower_run, stop)
                    }));
                }
            },
            &mut cut_leader,
        );
        if let Some(old) = cut_leader {
            cluster.cut_database(old, false);
        }
        let settled = hold.settle(observation, || Ok(()));
        let outcome = joined(build);
        let demand_outcome = demand.map(joined);
        let follower_outcome = follower.map(joined);
        hold.disarm();
        let mut checks = settled?;
        let outcome = outcome?;
        let demand_outcome = demand_outcome
            .ok_or_else(|| E2eError::system("partition demand build never started"))??;
        let follower_outcome = follower_outcome
            .ok_or_else(|| E2eError::system("partition follower build never started"))??;
        checks.push(Check::expect(
            PARTITION_BUILD,
            outcome.succeeded() && demand_outcome.succeeded() && follower_outcome.succeeded(),
            || {
                format!(
                    "initial: {}; demand: {}; follower: {}",
                    outcome.exit_summary(),
                    demand_outcome.exit_summary(),
                    follower_outcome.exit_summary()
                )
            },
        ));
        Ok(checks)
    })
}

enum Demand {
    Fenced,
    Follower,
}

fn partition_leader(cluster: &Cluster<'_>, journal: &Journal, run: &Run) -> Result<usize> {
    let line = journal.read_line()?;
    if line == JOURNAL_END || !line.starts_with(&format!("{} loss start ", run.as_ref())) {
        return Err(E2eError::system(format!(
            "partition build did not hold an action: {line}"
        )));
    }
    wait_state(cluster, |state| state.workers.len() == 2)?;
    let leaders = cluster.leaders();
    let [old] = leaders.as_slice() else {
        return Err(E2eError::system(format!(
            "expected one leader before partition, found {leaders:?}"
        )));
    };
    Ok(*old)
}

fn observe_partition(
    cluster: &Cluster<'_>,
    journal: &Journal,
    release: impl FnOnce() -> Result<()>,
    run: &Run,
    build: &std::thread::ScopedJoinHandle<'_, Result<crate::build_output::BuildOutcome>>,
    mut start_demand: impl FnMut(Demand),
    cut_leader: &mut Option<usize>,
) -> Result<Vec<Check>> {
    let old = partition_leader(cluster, journal, run)?;
    check_exit_deadline(cluster)?;
    let follower = usize::from(old == 0);
    let clock = SystemClock::new();
    cluster.cut_database(old, true);
    *cut_leader = Some(old);
    let started = clock.monotonic();
    let mut takeover = false;
    let mut held = !build.is_finished();
    let mut baseline = None;
    let mut fenced = true;
    let mut follower_acted = false;
    let mut fenced_started = false;
    let mut follower_started = false;
    let timeout = cluster.stack().settings().history_timeout;
    let wait_started = Instant::now();
    let mut observe = |_: &gang_wire::State| {
        takeover |= cluster.leaders().contains(&follower);
        if takeover && !follower_started {
            cluster.unroute_public(old);
            start_demand(Demand::Follower);
            follower_started = true;
        }
        held &= !build.is_finished();
        if clock.monotonic().saturating_duration_since(started) >= VALIDITY {
            let original = baseline.get_or_insert_with(|| controller_workers(cluster, old));
            if !fenced_started {
                start_demand(Demand::Fenced);
                fenced_started = true;
            }
            fenced &= *original == controller_workers(cluster, old);
            follower_acted |= !controller_workers(cluster, follower).is_empty();
        }
        let elapsed = clock.monotonic().saturating_duration_since(started);
        elapsed >= OUTAGE && (follower_acted || elapsed >= OUTAGE.saturating_mul(2))
    };
    if let Err(error) = cluster.stack().wait_live_state(timeout, &mut observe) {
        cluster.wait_for_leader_except(Some(old))?;
        cluster.unroute_public(old);
        cluster.wait_for_serving(&[follower])?;
        let remaining = timeout.saturating_sub(wait_started.elapsed());
        if remaining.is_zero() {
            return Err(error);
        }
        cluster.stack().wait_live_state(remaining, &mut observe)?;
    }
    cluster.cut_database(old, false);
    let healed_at = clock.wall();
    let healed_after = clock.monotonic().saturating_duration_since(started);
    *cut_leader = None;
    // The old leader drains executing actions before it exits.
    release()?;
    if !takeover {
        return Err(E2eError::system(format!(
            "cut leader {old}; follower {follower} did not take over; healed at {healed_at:?} after {healed_after:?}"
        )));
    }
    let exit = cluster.wait_exit(old).map_err(|error| {
        E2eError::system(format!(
            "cut leader {old}; healed at {healed_at:?} after {healed_after:?}; {}",
            gang_types::error_chain(&error)
        ))
    })?;
    let log = cluster.log(old)?;
    Ok(vec![
        Check::expect(PARTITION_HELD, held, || {
            "build ended during the partition".to_owned()
        }),
        Check::expect(PARTITION_TAKEOVER, takeover, || {
            format!("follower {follower} was not observed leading")
        }),
        Check::expect(
            PARTITION_FENCED,
            fenced && baseline.is_some() && follower_acted,
            || {
                format!(
                    "old leader worker set unchanged: {fenced}; follower launched a worker: {follower_acted}"
                )
            },
        ),
        Check::expect(
            PARTITION_EXIT,
            !exit.success() && log.contains(LOST_LEADERSHIP),
            || {
                format!(
                    "exit {exit}; lost-leadership message present: {}",
                    log.contains(LOST_LEADERSHIP)
                )
            },
        ),
        single_leader_check(cluster, follower),
    ])
}

#[cfg(test)]
mod tests {
    use super::frontends_unready;

    #[test]
    fn readiness_loss_waits_for_both_frontends() {
        assert!(frontends_unready(&[], &[]));
        assert!(!frontends_unready(&[], &[0]));
        assert!(!frontends_unready(&[0], &[]));
        assert!(!frontends_unready(&[0], &[0]));
    }
}
