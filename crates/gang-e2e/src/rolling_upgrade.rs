//! Previous-release upgrades and candidate refreshes with held original-controller work.
//! The held build keeps its rendered rc byte-for-byte; fresh work uses both that rc
//! and a separately rendered candidate rc.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread::{Scope, ScopedJoinHandle};
use std::time::{Duration, Instant};

use gang_types::Url;

use crate::bazel::Bazel;
use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::cluster::{Cluster, Retiring};
use crate::errors::{E2eError, Result};
use crate::execution::{JOURNAL_END, Run};
use crate::flags::Remote;
use crate::held_build::{Hold, joined};
use crate::identity::Credentials;
use crate::issuer::TestIssuer;
use crate::journal::{Attempts, Journal, overlapping_attempts, record_attempt};
use crate::process;
use crate::settings::{DeploymentInputs, ReleaseInput, Settings};
use crate::system::{Probe, Variant};
use crate::upgrade_credentials::ColdHelpers;
use crate::upgrade_evidence::BuildEvidence;
use crate::upgrade_fixture::ReleaseFixture;
use crate::upgrade_probe::{
    Availability, DirectTarget, probe_authentication, probe_capabilities, probe_refusal,
};

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

fn version(binary: &Path, timeout: Duration) -> Result<String> {
    let mut command = Command::new(binary);
    command.arg("--version");
    let output = process::run(command, None, timeout)?;
    if !output.succeeded() {
        return Err(E2eError::system(format!(
            "{} --version failed: {}",
            binary.display(),
            output.combined()
        )));
    }
    Ok(output.stdout.trim().to_owned())
}

fn executor(rc: &Path) -> Result<Url> {
    let text = fs::read_to_string(rc).map_err(E2eError::io("reading old client bazelrc"))?;
    let address = crate::cli::executor(&text)
        .ok_or_else(|| E2eError::system("old client bazelrc lacks remote executor"))?;
    let address = address.trim_matches(['\'', '"']);
    let address = address
        .strip_prefix("grpcs://")
        .ok_or_else(|| E2eError::system("old executor is not TLS"))?;
    Url::parse(&format!("https://{address}"))
        .map_err(E2eError::system_from("parsing old remote executor"))
}

fn logged_bazel(bazel: &Bazel, bep: &Path, rpc: &Path) -> Bazel {
    bazel.with_build_logs(bep, rpc)
}

fn read_held(journal: &Journal, run: &Run, attempts: &mut Attempts) -> Result<()> {
    while attempts.len() < 2 {
        let line = journal.read_line()?;
        if line == JOURNAL_END {
            return Err(E2eError::system(
                "initial build ended before two actions started",
            ));
        }
        record_attempt(&line, run.as_ref(), attempts)?;
    }
    if attempts
        .values()
        .filter(|attempt| attempt.end.is_none())
        .count()
        != 2
    {
        return Err(E2eError::system("initial actions were not held"));
    }
    Ok(())
}

fn drain(journal: &Journal, run: &Run, attempts: &mut Attempts) -> Result<()> {
    loop {
        let line = journal.read_line()?;
        if line == JOURNAL_END {
            return Ok(());
        }
        record_attempt(&line, run.as_ref(), attempts)?;
    }
}

// Keys are action and attempt PID; values are original worker PID and start timestamp.
type InFlight = BTreeMap<(String, u32), (u32, u64)>;

fn capture_in_flight(attempts: &Attempts) -> InFlight {
    attempts
        .iter()
        .map(|(key, attempt)| (key.clone(), (attempt.worker, attempt.start)))
        .collect()
}

fn uninterrupted(frozen: &InFlight, final_attempts: &Attempts) -> bool {
    !frozen.is_empty()
        && frozen.iter().all(|(key, &(worker, start))| {
            final_attempts.get(key).is_some_and(|attempt| {
                attempt.worker == worker && attempt.start == start && attempt.end.is_some()
            })
        })
}

fn exact_attempts(attempts: &Attempts, expected: usize) -> bool {
    let mut counts = BTreeMap::new();
    for ((action, _), attempt) in attempts {
        if attempt.end.is_none() {
            return false;
        }
        *counts.entry(action).or_insert(0usize) += 1;
    }
    counts.len() == expected
        && counts.values().all(|count| *count == 1)
        && !overlapping_attempts(attempts)
}

fn build_ok(outcome: &BuildOutcome, evidence: &BuildEvidence, expected: usize) -> bool {
    outcome.succeeded()
        && outcome.processes.remote() as usize == expected
        && evidence.finished_success
        && evidence.failed_actions == 0
        && evidence.rpc_calls > 0
        && evidence.successful_actions >= expected
}

fn availability_ok(gap: Duration, limit: Duration) -> bool {
    gap <= limit
}

fn detail(name: &str, ok: bool, metrics: String) -> Check {
    let label = format!("{name}: {metrics}");
    if ok {
        Check::passed(label)
    } else {
        Check::failed(label, metrics)
    }
}
fn preflight(
    cluster: &Cluster<'_>,
    journal: &Journal,
    run: &Run,
    old_binary: &Path,
    attempts: &mut Attempts,
) -> Result<()> {
    read_held(journal, run, attempts)?;
    let workers: BTreeSet<_> = attempts.values().map(|attempt| attempt.worker).collect();
    if workers.len() != 1 {
        return Err(E2eError::system(
            "initial actions did not run on one worker",
        ));
    }
    for worker in workers {
        let executable = fs::canonicalize(format!("/proc/{worker}/exe"))
            .map_err(E2eError::io("resolving initial worker executable"))?;
        let expected = fs::canonicalize(old_binary)
            .map_err(E2eError::io("resolving old release executable"))?;
        if executable != expected {
            return Err(E2eError::system(
                "initial worker is not running the old release executable",
            ));
        }
    }
    let stack = cluster.stack();
    stack.wait_live_state(stack.settings().history_timeout, |state| {
        state.queue.total > 0 && state.workers.len() == 1
    })?;
    Ok(())
}

#[derive(Debug, Default)]
struct DrainWitness {
    elapsed: Duration,
    old_leader_before: bool,
    old_leader_after: bool,
    credentials: Option<HeldDrainWitness>,
    stop_started: Option<Duration>,
    restarted_pid: Option<u32>,
    retired: bool,
}

#[derive(Clone, Copy, Debug)]
struct HeldState {
    build_unfinished: bool,
    hold_unreleased: bool,
    old_live: bool,
}

impl HeldState {
    fn valid(self) -> bool {
        self.build_unfinished && self.hold_unreleased && self.old_live
    }
}

fn worker_executable(pid: u32) -> Result<PathBuf> {
    fs::canonicalize(format!("/proc/{pid}/exe"))
        .map_err(E2eError::io("observing original held worker executable"))
}

#[derive(Debug)]
struct HeldDrainWitness {
    started: Duration,
    finished: Duration,
    deadline: Duration,
    refusal: Option<(u16, bool)>,
    refusal_required: bool,
    controller_pid: u32,
    before: HeldState,
    after: HeldState,
    owner_rechecked: bool,
}

impl HeldDrainWitness {
    fn valid(&self, released: Duration, _gap_limit: Duration) -> bool {
        self.before.valid()
            && self.after.valid()
            && self.owner_rechecked
            && (!self.refusal_required || self.refusal == Some((503, true)))
            && self.controller_pid != 0
            && self.started < self.finished
            && self.finished <= self.deadline
            && self.finished < released
    }

    fn receipt(&self) -> String {
        format!(
            "held witness start/end/deadline={:?}/{:?}/{:?}, direct refusal={:?}, refusal required={}, original controllerPID={}, held before-launch/after-health={:?}/{:?}, successor healthy owner rechecked={}",
            self.started,
            self.finished,
            self.deadline,
            self.refusal,
            self.refusal_required,
            self.controller_pid,
            self.before,
            self.after,
            self.owner_rechecked,
        )
    }
}

fn drain_credentials_ok(drain: &DrainWitness, required: bool, gap_limit: Duration) -> bool {
    if !required {
        return true;
    }
    match (&drain.credentials, drain.stop_started) {
        (Some(witness), Some(stop)) => witness.valid(stop, gap_limit),
        _ => false,
    }
}

struct Handoff<'a> {
    client: &'a OldClient,
    original_held: &'a dyn Fn() -> Result<(bool, bool)>,
    old_identity: Option<(DirectTarget, u32, u32, PathBuf)>,
    original_holder: Arc<str>,
    before_transition: Option<HeldState>,
    applied: Instant,
    continuity_observed: bool,
}

impl Handoff<'_> {
    fn observe_held(&self, cluster: &Cluster<'_>) -> Result<(DirectTarget, HeldState)> {
        let target = cluster.controller_public_target(0)?;
        let (original_target, controller_pid, worker_pid, executable) = self
            .old_identity
            .as_ref()
            .ok_or_else(|| E2eError::system("original held identity missing"))?;
        if original_target.address != target.address
            || original_target.proxy_protocol_v2 != target.proxy_protocol_v2
        {
            return Err(E2eError::system(
                "original controller public target changed",
            ));
        }
        if cluster.controller_pid(0)? != *controller_pid {
            return Err(E2eError::system("original held controller process changed"));
        }
        if worker_executable(*worker_pid)? != *executable {
            return Err(E2eError::system("original held worker process changed"));
        }
        let (build_unfinished, hold_unreleased) = (self.original_held)()?;
        let state = HeldState {
            build_unfinished,
            hold_unreleased,
            old_live: worker_executable(*worker_pid).is_ok_and(|actual| actual == *executable),
        };
        if !state.valid() {
            return Err(E2eError::system(
                "drain witness requires original unfinished held build and worker",
            ));
        }
        Ok((target, state))
    }
}

fn remaining_budget(start: Instant, budget: Duration) -> Result<Duration> {
    let remaining = budget.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        return Err(E2eError::timeout("held drain admission witness", budget));
    }
    Ok(remaining)
}

fn held_drain(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    monitor: &RunningMonitor<'_>,
    next: usize,
    handoff: &mut Handoff<'_>,
) -> Result<(DrainWitness, PromotionTiming)> {
    let settings = cluster.stack().settings();
    let start = Instant::now();
    let budget = settings.upgrade_gap_limit;
    let promotion = confirm_promotion(cluster, next, handoff.client, start, Some(budget))?;
    cluster.wait_for_controller_health_within(next, remaining_budget(start, budget)?)?;
    let (target, _) = handoff.observe_held(cluster)?;
    let before = handoff.before_transition.ok_or_else(|| {
        E2eError::system("original held witness before replacement launch missing")
    })?;
    let started = handoff.applied.elapsed();
    let refusal = if releases.mode == Refresh::ReleaseUpgrade {
        None
    } else {
        let body =
            ColdHelpers::proof_request(cluster.stack().gang_cli(), &handoff.client.endpoint)?;
        let response = probe_refusal(
            &handoff.client.endpoint,
            &handoff.client.credentials,
            target,
            body,
            remaining_budget(start, budget)?.min(settings.upgrade_probe_timeout),
        )?;
        Some((response.status, response.marker))
    };
    monitor.mint_pair()?;
    handoff
        .client
        .assert_warm_credentials(cluster, next, remaining_budget(start, budget)?)?;
    let (_, after) = handoff.observe_held(cluster)?;
    let credentials = HeldDrainWitness {
        started,
        finished: handoff.applied.elapsed(),
        deadline: start.duration_since(handoff.applied) + budget,
        refusal,
        refusal_required: releases.mode != Refresh::ReleaseUpgrade,
        controller_pid: cluster.controller_pid(0)?,
        before,
        after,
        owner_rechecked: cluster.controller_is_leader(next)?
            && cluster.controller_is_healthy(next)?,
    };
    if !credentials.valid(handoff.applied.elapsed(), budget) {
        return Err(E2eError::system(
            "invalid original-process held drain witness",
        ));
    }
    Ok((
        DrainWitness {
            elapsed: start.elapsed(),
            old_leader_before: false,
            old_leader_after: cluster.controller_is_leader(0)?,
            credentials: Some(credentials),
            stop_started: None,
            ..DrainWitness::default()
        },
        promotion,
    ))
}

#[derive(Clone, Copy)]
struct PromotionTiming {
    lease: Duration,
    client: Duration,
}

fn confirm_promotion(
    cluster: &Cluster<'_>,
    index: usize,
    client: &OldClient,
    start: Instant,
    budget: Option<Duration>,
) -> Result<PromotionTiming> {
    let settings = cluster.stack().settings();
    let mut remaining_lease = settings.upgrade_gap_limit.saturating_sub(start.elapsed());
    if let Some(budget) = budget {
        remaining_lease = remaining_lease.min(remaining_budget(start, budget)?);
    }
    cluster.wait_for_controller_leader_within(index, remaining_lease)?;
    let lease = start.elapsed();
    let activation = Instant::now();
    let remaining = || match budget {
        Some(budget) => budget.saturating_sub(start.elapsed()),
        None => settings
            .startup_timeout
            .saturating_sub(activation.elapsed()),
    };
    loop {
        let timeout = remaining();
        if timeout.is_zero() {
            return Err(E2eError::timeout(
                "candidate authenticated client promotion",
                settings.startup_timeout,
            ));
        }
        if probe_capabilities(
            &client.endpoint,
            &client.credentials,
            timeout.min(settings.upgrade_probe_timeout),
        )
        .is_ok()
            && cluster.controller_is_leader(index)?
        {
            return Ok(PromotionTiming {
                lease,
                client: start.elapsed(),
            });
        }
        std::thread::sleep(remaining().min(settings.upgrade_probe_interval));
    }
}

fn handoff<K: Fn(Option<&str>) -> Result<()>>(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    context: &mut Handoff<'_>,
    monitor: &RunningMonitor<'_>,
    hold: &Hold<'_, K>,
) -> Result<(usize, Option<Retiring>, PromotionTiming, DrainWitness)> {
    cluster.apply_release(&releases.candidate)?;
    let startup = cluster.stack().settings().startup_timeout;
    let startup_started = Instant::now();
    let next = cluster.start_controller_release_within(
        releases.candidate_binary,
        &releases.candidate,
        startup,
    )?;
    if releases.mode == Refresh::ConfigOnly {
        assert_installed_transition(cluster, next, releases)?;
    }
    cluster.wait_for_controller_serve_within(
        next,
        startup.saturating_sub(startup_started.elapsed()),
    )?;
    let (mut witness, timing) = held_drain(cluster, releases, monitor, next, context)?;
    witness.stop_started = Some(context.applied.elapsed());
    context.observe_held(cluster)?;
    context.client.assert_original_rc()?;
    let retiring = if releases.mode == Refresh::ReleaseUpgrade {
        cluster.deregister_controller(0)?;
        Some(cluster.begin_retire(0)?)
    } else {
        None
    };
    context.continuity_observed = true;
    hold.release()?;
    Ok((next, retiring, timing, witness))
}

enum MonitorCommand {
    Stop,
    Mint(mpsc::SyncSender<Result<()>>),
}

fn verify_cold_pair(
    helpers: ColdHelpers,
    endpoint: &Url,
    original: &Credentials,
    timeout: Duration,
) -> Result<()> {
    let tokens = helpers
        .mint_pair()
        .map_err(E2eError::system_from("held-drain cold credential pair"))?;
    let mut credentials = original.clone();
    for token in tokens {
        credentials.token = token;
        probe_capabilities(endpoint, &credentials, timeout)?;
    }
    Ok(())
}

struct MonitorArgs<'a> {
    endpoint: Url,
    credentials: Credentials,
    settings: &'a crate::settings::Settings,
    ready: mpsc::SyncSender<bool>,
    commands: mpsc::Receiver<MonitorCommand>,
    cold: ColdHelpers,
}

fn monitor_availability(args: MonitorArgs<'_>) -> (Availability, Duration, usize, usize) {
    let start = Instant::now();
    // This clock starts before apply/launch and never resets at promotion or health detection.
    // Command execution below also remains inside it, so cold mints cannot hide a success gap.
    let mut availability = Availability::default();
    let mut first = Some(args.ready);
    let mut previous_failed = false;
    let mut retries = 0;
    let mut cold = Some(args.cold);
    let mut cold_mints = 0;
    loop {
        let success = probe_capabilities(
            &args.endpoint,
            &args.credentials,
            args.settings.upgrade_probe_timeout,
        )
        .is_ok();
        if previous_failed {
            retries += 1;
        }
        previous_failed = !success;
        availability.sample(start.elapsed(), success);
        if let Some(ready) = first.take() {
            let _ = ready.send(success);
        }
        match args
            .commands
            .recv_timeout(args.settings.upgrade_probe_interval)
        {
            Ok(MonitorCommand::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Ok(MonitorCommand::Mint(reply)) => {
                let result = cold
                    .take()
                    .ok_or_else(|| E2eError::system("cold helpers already consumed"))
                    .and_then(|helpers| {
                        verify_cold_pair(
                            helpers,
                            &args.endpoint,
                            &args.credentials,
                            args.settings.upgrade_probe_timeout,
                        )
                    });
                if result.is_ok() {
                    cold_mints = 2;
                }
                let _ = reply.send(result);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    let gap = availability.finish(start.elapsed());
    (availability, gap, retries, cold_mints)
}

struct VerdictInputs<'a> {
    initial_outcome: &'a BuildOutcome,
    initial_evidence: &'a BuildEvidence,
    initial_attempts: &'a Attempts,
    in_flight: &'a InFlight,
    post_outcome: &'a BuildOutcome,
    post_evidence: &'a BuildEvidence,
    post_attempts: &'a Attempts,
    availability: &'a Availability,
    gap: Duration,
    retries: usize,
    cold_mints: usize,
    timing: PromotionTiming,
    drain: &'a DrainWitness,
    status: std::process::ExitStatus,
    leader: bool,
    gap_limit: Duration,
    old_version: &'a str,
    candidate_version: &'a str,
    elapsed: Duration,
    rollout: &'a RolloutWitness,
    mode: Refresh,
}

fn final_verdict(input: &VerdictInputs<'_>) -> Vec<Check> {
    let workers: BTreeSet<_> = input
        .initial_attempts
        .values()
        .chain(input.post_attempts.values())
        .map(|attempt| attempt.worker)
        .collect();
    let attempts = input.initial_attempts.len() + input.post_attempts.len();
    let ok = build_ok(input.initial_outcome, input.initial_evidence, PROBES.len())
        && exact_attempts(input.initial_attempts, PROBES.len())
        && uninterrupted(input.in_flight, input.initial_attempts)
        && build_ok(input.post_outcome, input.post_evidence, 1)
        && exact_attempts(input.post_attempts, 1)
        && input.in_flight.len() == 2
        && input.status.success()
        && input.leader
        && input.cold_mints == 2
        && input.drain.retired
        && (input.mode == Refresh::ReleaseUpgrade
            || input.drain.restarted_pid.is_some_and(|pid| {
                input.drain.credentials.as_ref().is_some_and(|held| {
                    held.refusal_required
                        && held.refusal == Some((503, true))
                        && pid != held.controller_pid
                })
            }))
        && drain_credentials_ok(input.drain, true, input.gap_limit)
        && input.timing.lease <= input.gap_limit
        && availability_ok(input.gap, input.gap_limit)
        && input.rollout.valid();

    let drain_receipt = input.drain.credentials.as_ref().map_or_else(
        || "original-process held drain witness absent".to_owned(),
        HeldDrainWitness::receipt,
    );
    let metrics = format!(
        "ordinary-refresh; successor healthy before held work release; drain elapsed={:?}, old leader before/after={}/{}, versions={:?}->{:?}, initial exit={:?} post exit={:?}, failed actions={}/{}, successful actions={}/{}, attempts={attempts}, reexecutions={}, workers={workers:?}, frozen in-flight={:?}, rpc calls={}/{}, rpc failures={}/{}, rpc retries={}/{}, execute/wait correlations={}/{}, warmcold_mints={}, raw probe failures={}, repeated raw probes after failures={}, bounded raw max gap={:?} (sampled, not zero-error client evidence), lease takeover={:?}, client promotion={:?}, old exit={}, final leader={}, total={:?}, {drain_receipt}, stop apply offset={:?}, refresh={:?}, restarted_controllerPID={:?}, old_host_retired={}, rollout={:?}",
        input.drain.elapsed,
        input.drain.old_leader_before,
        input.drain.old_leader_after,
        input.old_version,
        input.candidate_version,
        input.initial_outcome.exit_code,
        input.post_outcome.exit_code,
        input.initial_evidence.failed_actions,
        input.post_evidence.failed_actions,
        input.initial_evidence.successful_actions,
        input.post_evidence.successful_actions,
        attempts.saturating_sub(9),
        input.in_flight,
        input.initial_evidence.rpc_calls,
        input.post_evidence.rpc_calls,
        input.initial_evidence.rpc_failures,
        input.post_evidence.rpc_failures,
        input.initial_evidence.rpc_retry_attempts,
        input.post_evidence.rpc_retry_attempts,
        input.initial_evidence.execute_wait_correlations,
        input.post_evidence.execute_wait_correlations,
        input.cold_mints,
        input.availability.failures,
        input.retries,
        input.gap,
        input.timing.lease,
        input.timing.client,
        input.status,
        input.leader,
        input.elapsed,
        input.drain.stop_started,
        input.mode,
        input.drain.restarted_pid,
        input.drain.retired,
        input.rollout,
    );
    vec![detail("rolling upgrade", ok, metrics)]
}

struct Releases<'a> {
    old: ReleaseInput,
    candidate_binary: &'a Path,
    old_version: String,
    candidate_version: String,
    old_fixture: ReleaseFixture,
    candidate: ReleaseFixture,
    mode: Refresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refresh {
    ReleaseUpgrade,
    Candidate,
    ConfigOnly,
}

fn require_max_only(old: &str, candidate: &str) -> Result<()> {
    let mut old: toml::Value = toml::from_str(old).map_err(crate::template::template_error)?;
    let candidate: toml::Value =
        toml::from_str(candidate).map_err(crate::template::template_error)?;
    let maximum = old
        .get_mut("fleets")
        .and_then(toml::Value::as_array_mut)
        .and_then(|fleets| fleets.first_mut())
        .and_then(|fleet| fleet.get_mut("max_instances"))
        .ok_or_else(|| E2eError::system("seed fleet maximum missing"))?;
    if maximum.as_integer() != Some(1) {
        return Err(E2eError::system("seed fleet maximum is not one"));
    }
    *maximum = toml::Value::Integer(2);
    if old != candidate {
        return Err(E2eError::system(
            "configuration transition is not solely fleet maximum 1->2",
        ));
    }
    Ok(())
}

fn assert_installed_transition(
    cluster: &Cluster<'_>,
    next: usize,
    releases: &Releases<'_>,
) -> Result<()> {
    let read = |index| {
        fs::read_to_string(cluster.release_config_path(index)?)
            .map_err(E2eError::io("reading installed refresh configuration"))
    };
    let old = read(0)?;
    let new = read(next)?;
    if old != cluster.render_release_config_for(0, &releases.old_fixture)?
        || new != cluster.render_release_config_for(next, &releases.candidate)?
    {
        return Err(E2eError::system(
            "installed configuration differs from selected fixture",
        ));
    }
    // Compare in one topology: per-host listener/database-link addresses intentionally differ.
    require_max_only(
        &old,
        &cluster.render_release_config_for(0, &releases.candidate)?,
    )?;
    let expected = fs::canonicalize(releases.candidate_binary)
        .map_err(E2eError::io("resolving candidate executable"))?;
    for index in [0, next] {
        if worker_executable(cluster.controller_pid(index)?)? != expected {
            return Err(E2eError::system(
                "config-only controllers do not share the pinned executable",
            ));
        }
    }
    Ok(())
}

struct OldClient {
    remote: Remote,
    credentials: Credentials,
    endpoint: Url,
    original_rc: Vec<u8>,
    original_session: Vec<u8>,
}

impl OldClient {
    fn assert_original_rc(&self) -> Result<()> {
        let current = fs::read(self.remote.bazelrc())
            .map_err(E2eError::io("reading original old client bazelrc"))?;
        if current != self.original_rc {
            return Err(E2eError::system(
                "original old client bazelrc bytes changed",
            ));
        }
        Ok(())
    }

    fn assert_warm_credentials(
        &self,
        cluster: &Cluster<'_>,
        leader: usize,
        timeout: Duration,
    ) -> Result<()> {
        self.assert_original_rc()?;
        let cli = cluster.stack().gang_cli();
        if ColdHelpers::session_bytes(cli, &self.endpoint)? != self.original_session {
            return Err(E2eError::system(
                "original workload session changed during upgrade",
            ));
        }
        let probe_timeout = timeout.min(cluster.stack().settings().upgrade_probe_timeout);
        let proof = ColdHelpers::proof_request(cli, &self.endpoint)?;
        let response = probe_authentication(
            &self.endpoint,
            &self.credentials,
            cluster.controller_public_target(leader)?,
            proof,
            probe_timeout,
        )?;
        if response.status != 200 || response.marker {
            return Err(E2eError::system(
                "successor refused the original session's bound workload proof",
            ));
        }
        let token = ColdHelpers::warm_token(cli, &self.endpoint, &self.original_session, timeout)?;
        let mut credentials = self.credentials.clone();
        credentials.token = token;
        probe_capabilities(&self.endpoint, &credentials, probe_timeout)?;
        probe_capabilities(&self.endpoint, &self.credentials, probe_timeout)?;
        if !cluster.controller_is_leader(leader)? || !cluster.controller_is_healthy(leader)? {
            return Err(E2eError::system(
                "warm credentials were not witnessed at the healthy successor",
            ));
        }
        self.assert_original_rc()
    }
}

struct LoggedBuild {
    driver: Bazel,
    run: Run,
    bep: PathBuf,
    rpc: PathBuf,
}

struct Builds {
    journal: Journal,
    go: PathBuf,
    initial: LoggedBuild,
    post: LoggedBuild,
    old_post: LoggedBuild,
}

struct Upgrade<'a> {
    cluster: Cluster<'a>,
    releases: Releases<'a>,
    client: OldClient,
    builds: Builds,
    started: Instant,
}

struct ObservedBuild {
    outcome: BuildOutcome,
    evidence: BuildEvidence,
    attempts: Attempts,
    executables: BTreeMap<u32, PathBuf>,
}

struct Promotion {
    index: usize,
    status: std::process::ExitStatus,
    timing: PromotionTiming,
    drain: DrainWitness,
}

#[derive(Debug)]
struct WorkerRollout {
    replacement_required: bool,
    old_exited: bool,
    old_workers_remaining: usize,
}

impl WorkerRollout {
    fn valid(&self) -> bool {
        !self.replacement_required || (self.old_exited && self.old_workers_remaining == 0)
    }
}

#[derive(Debug)]
struct RolloutWitness {
    workers: WorkerRollout,
    old_client_current: bool,
    candidate_client_current: bool,
}

impl RolloutWitness {
    fn valid(&self) -> bool {
        self.workers.valid() && self.old_client_current && self.candidate_client_current
    }
}

fn current_executables(
    attempts: &Attempts,
    executables: &BTreeMap<u32, PathBuf>,
    candidate: &Path,
) -> bool {
    !attempts.is_empty()
        && attempts.values().all(|attempt| {
            executables
                .get(&attempt.worker)
                .is_some_and(|path| path == candidate)
        })
}

type MonitorOutcome = (Availability, Duration, usize, usize);

struct RunningMonitor<'scope> {
    commands: mpsc::Sender<MonitorCommand>,
    first: mpsc::Receiver<bool>,
    join: Option<ScopedJoinHandle<'scope, MonitorOutcome>>,
}

struct MonitorStart<'a> {
    endpoint: Url,
    credentials: Credentials,
    settings: &'a Settings,
    cold: ColdHelpers,
}

impl<'scope> RunningMonitor<'scope> {
    fn start<'env>(scope: &'scope Scope<'scope, 'env>, start: MonitorStart<'env>) -> Self
    where
        'env: 'scope,
    {
        let (commands, monitor_commands) = mpsc::channel();
        let (ready, first) = mpsc::sync_channel(1);
        let args = MonitorArgs {
            endpoint: start.endpoint,
            credentials: start.credentials,
            settings: start.settings,
            ready,
            commands: monitor_commands,
            cold: start.cold,
        };
        let join = scope.spawn(move || monitor_availability(args));
        Self {
            commands,
            first,
            join: Some(join),
        }
    }

    fn finish(&mut self) -> Result<MonitorOutcome> {
        let _ = self.commands.send(MonitorCommand::Stop);
        self.join
            .take()
            .ok_or_else(|| E2eError::system("upgrade monitor already finished"))?
            .join()
            .map_err(|_| E2eError::system("upgrade monitor panicked"))
    }

    fn mint_pair(&self) -> Result<()> {
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(MonitorCommand::Mint(reply))
            .map_err(|_| E2eError::system("availability monitor stopped before cold mints"))?;
        result
            .recv()
            .map_err(|_| E2eError::system("availability monitor stopped during cold mints"))?
    }
}

impl Drop for RunningMonitor<'_> {
    fn drop(&mut self) {
        let _ = self.commands.send(MonitorCommand::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn select_releases(invocation: &DeploymentInputs, mode: Refresh) -> Result<Releases<'_>> {
    let inputs = invocation
        .upgrade
        .as_ref()
        .ok_or_else(|| E2eError::system("upgrade inputs missing"))?;
    let old = if mode == Refresh::ReleaseUpgrade {
        inputs.previous.clone()
    } else {
        ReleaseInput {
            binary: invocation.gang.clone(),
            fixture: inputs.candidate_fixture.clone(),
        }
    };
    let binary = &invocation.gang;
    let fixture = &inputs.candidate_fixture;
    // Artifact selection, not semver inequality, distinguishes published -> candidate.
    let old_version = version(&old.binary, invocation.settings.command_timeout)?;
    let candidate_version = version(binary, invocation.settings.command_timeout)?;
    let old_fixture = ReleaseFixture::read(&old.fixture)?.seeded()?;
    let candidate = ReleaseFixture::read(fixture)?;
    let candidate = if mode == Refresh::ConfigOnly {
        require_max_only(&old_fixture.gang_toml, &candidate.gang_toml)?;
        crate::upgrade_config_restart::verify_publication(&old_fixture, &candidate)?;
        if old_fixture.controller.instances != 1
            || candidate.controller.instances != 1
            || fs::canonicalize(&old.binary).map_err(E2eError::io("resolving seed binary"))?
                != fs::canonicalize(binary).map_err(E2eError::io("resolving promoted binary"))?
        {
            return Err(E2eError::system(
                "config-only refresh requires count one and the same executable",
            ));
        }
        candidate
    } else {
        candidate.seeded()?
    };
    Ok(Releases {
        old,
        candidate_binary: binary,
        old_version,
        candidate_version,
        old_fixture,
        candidate,
        mode,
    })
}

fn prepare_client(cluster: &Cluster<'_>) -> Result<OldClient> {
    let stack = cluster.stack();
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let remote = stack.administrator_remote(&credentials)?;
    let original_rc = fs::read(remote.bazelrc())
        .map_err(E2eError::io("capturing original old client bazelrc"))?;
    let endpoint = executor(remote.bazelrc())?;
    let original_session = ColdHelpers::session_bytes(stack.gang_cli(), &endpoint)?;
    probe_capabilities(
        &endpoint,
        &credentials,
        stack.settings().upgrade_probe_timeout,
    )?;
    cluster.wait_for_controller_leader_within(0, stack.settings().startup_timeout)?;
    stack.wait_live_state(stack.settings().startup_timeout, |state| {
        state.workers.len() == 1
    })?;
    Ok(OldClient {
        remote,
        credentials,
        endpoint,
        original_rc,
        original_session,
    })
}

fn prepare_builds(cluster: &Cluster<'_>) -> Result<Builds> {
    let stack = cluster.stack();
    let signal = stack.signal_path();
    let journal = Journal::create(
        &signal.with_file_name("journal"),
        stack.settings().hold_timeout,
    )?;
    let initial_run = Run::fresh()?;
    let post_run = Run::fresh()?;
    let old_post_run = Run::fresh()?;
    journal.prepare_attempts(
        initial_run.as_ref(),
        PROBES.iter().map(|probe| probe.as_str()),
    )?;
    journal.prepare_attempts(post_run.as_ref(), [Probe::ControllerLoss0.as_str()])?;
    journal.prepare_attempts(old_post_run.as_ref(), [Probe::ControllerLoss0.as_str()])?;
    let make = |name: &str, run| {
        let bep = signal.with_file_name(format!("upgrade-{name}.bep"));
        let rpc = signal.with_file_name(format!("upgrade-{name}.rpc"));
        let driver = logged_bazel(stack.bazel(), &bep, &rpc);
        LoggedBuild {
            driver,
            run,
            bep,
            rpc,
        }
    };
    Ok(Builds {
        journal,
        go: signal.with_file_name("go"),
        initial: make("initial", initial_run),
        post: make("post", post_run),
        old_post: make("old-post", old_post_run),
    })
}

pub(crate) fn rolling_upgrade<'a>(
    invocation: &'a DeploymentInputs,
    variant: &Variant,
    issuer: &'a TestIssuer,
) -> Result<Vec<Check>> {
    let mut checks = run_upgrade(invocation, variant, issuer, Refresh::ReleaseUpgrade)?;
    checks.extend(run_upgrade(
        invocation,
        variant,
        issuer,
        Refresh::Candidate,
    )?);
    checks.extend(run_upgrade(
        invocation,
        variant,
        issuer,
        Refresh::ConfigOnly,
    )?);
    Ok(checks)
}

fn run_upgrade<'a>(
    invocation: &'a DeploymentInputs,
    variant: &Variant,
    issuer: &'a TestIssuer,
    mode: Refresh,
) -> Result<Vec<Check>> {
    let releases = select_releases(invocation, mode)?;
    let cluster = Cluster::start_release(
        invocation,
        variant,
        issuer,
        &releases.old.binary,
        &releases.old_fixture,
    )?;
    upgrade_with_releases(cluster, releases)
}

pub(crate) fn upgrade_cluster<'a>(
    cluster: Cluster<'a>,
    invocation: &'a DeploymentInputs,
) -> Result<Vec<Check>> {
    let releases = select_releases(invocation, Refresh::ReleaseUpgrade)?;
    upgrade_with_releases(cluster, releases)
}

fn upgrade_with_releases<'a>(cluster: Cluster<'a>, releases: Releases<'a>) -> Result<Vec<Check>> {
    let client = prepare_client(&cluster)?;
    let builds = prepare_builds(&cluster)?;
    Upgrade {
        cluster,
        releases,
        client,
        builds,
        started: Instant::now(),
    }
    .run()
}

struct InitialObservation {
    handoff: Result<(
        usize,
        std::process::ExitStatus,
        PromotionTiming,
        DrainWitness,
    )>,
    build: Result<BuildOutcome>,
    attempts: Attempts,
    in_flight: InFlight,
    continuity_observed: bool,
}

impl Upgrade<'_> {
    fn run(&self) -> Result<Vec<Check>> {
        let result = std::thread::scope(|scope| {
            let hold = Arc::new(Hold::new(&self.builds.go, |root| {
                self.cluster.stack().kill_bazel_in(root)
            }));
            let (stop, stopped) = mpsc::channel();
            let (ready, first) = mpsc::sync_channel(1);
            let continuity = (self.releases.mode == Refresh::ConfigOnly)
                .then(|| scope.spawn(move || self.client_continuity(&stopped, ready)));
            let result = if continuity.is_some() && first.recv().is_err() {
                Err(E2eError::system(
                    "protected client continuity failed before apply",
                ))
            } else {
                self.run_scoped(scope, &hold)
            };
            if result.is_err() {
                hold.cancel("upgrade scenario failed");
            }
            drop(stop);
            let result = match continuity {
                Some(join) => join.join()
                    .map_err(|_| E2eError::system("protected client continuity panicked"))?
                    .map_err(|client| match &result {
                        Err(scenario) => E2eError::system(format!(
                            "protected client continuity failed: {client}; upgrade scenario also failed: {scenario}"
                        )),
                        Ok(_) => client,
                    })
                    .and_then(|commands| result.map(|mut checks| {
                        checks.push(Check::passed(format!(
                            "config-only: {commands} uncached protected GangCli get commands sampled before, across and after refresh at installed authentication-budget cadence; zero command failures; existing client retries only; original rc/session/bound proof reused; original raw availability sampling remains separate"
                        )));
                        checks.push(Check::passed(format!(
                            "config-only count-one refresh: actual pinned selectors {:?}->{:?}; exact installed seed maximum 1 -> promoted maximum 2, only deployed input delta; same canonical controller executable; offline SSM/IAM retention model, real local processes",
                            self.releases.old_fixture.configuration.controller.reference,
                            self.releases.candidate.configuration.controller.reference,
                        )));
                        checks
                    })),
                None => result,
            };
            Arc::try_unwrap(hold)
                .map_err(|_| E2eError::system("hold still shared"))?
                .disarm();
            result
        });
        self.cluster.diagnose(result)
    }

    fn client_continuity(
        &self,
        stopped: &mpsc::Receiver<()>,
        ready: mpsc::SyncSender<()>,
    ) -> Result<usize> {
        let stack = self.cluster.stack();
        let root = stack.placement().root().join("continuity-credentials");
        let configuration = fs::read_to_string(self.cluster.release_config_path(0)?)
            .map_err(E2eError::io("reading installed authentication limits"))?;
        let interval = ColdHelpers::continuity_interval(
            &configuration,
            stack.settings().upgrade_probe_interval,
        )?;
        let mint = || {
            self.client.assert_original_rc()?;
            if ColdHelpers::session_bytes(stack.gang_cli(), &self.client.endpoint)?
                != self.client.original_session
            {
                return Err(E2eError::system(
                    "continuity witness original session changed",
                ));
            }
            // Fresh homes have no bearer cache: each command must authenticate the original proof.
            ColdHelpers::prepare(
                stack.gang_cli(),
                &self.client.endpoint,
                &root,
                stack.settings().command_timeout,
            )?
            .mint_pair()
            .map_err(E2eError::system_from(
                "continuous protected cold credential pair",
            ))?;
            Ok(())
        };
        let mut commands = 0;
        let mut ready = Some(ready);
        loop {
            mint()?;
            commands += 2;
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
            }
            match stopped.recv_timeout(interval) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // A final fresh pair is launched after all old-exit and post-build observations.
                    mint()?;
                    return Ok(commands + 2);
                }
            }
        }
    }

    fn original_identity(
        &self,
        attempts: &Attempts,
    ) -> Result<Option<(DirectTarget, u32, u32, PathBuf)>> {
        let worker = attempts
            .values()
            .next()
            .ok_or_else(|| E2eError::system("original held worker missing"))?
            .worker;
        let executable = worker_executable(worker)?;
        let expected = fs::canonicalize(&self.releases.old.binary)
            .map_err(E2eError::io("resolving old release executable"))?;
        if executable != expected {
            return Err(E2eError::system("original held worker executable changed"));
        }
        Ok(Some((
            self.cluster.controller_public_target(0)?,
            self.cluster.controller_pid(0)?,
            worker,
            executable,
        )))
    }

    fn run_scoped<'scope, 'env, K>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        hold: &Arc<Hold<'env, K>>,
    ) -> Result<Vec<Check>>
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()> + Send + Sync + 'env,
    {
        self.client.assert_original_rc()?;
        let initial = hold.build(scope, None, |_, stop| {
            let result = self.builds.initial.driver.build_until(
                &self.client.remote,
                &PROBES,
                Some(&self.builds.initial.run),
                stop,
            );
            self.builds.journal.write_line(JOURNAL_END)?;
            result
        });
        let mut attempts = Attempts::new();
        preflight(
            &self.cluster,
            &self.builds.journal,
            &self.builds.initial.run,
            &self.releases.old.binary,
            &mut attempts,
        )?;
        let in_flight = capture_in_flight(&attempts);
        let mut monitor = self.start_monitor(scope)?;
        if !monitor.first.recv().unwrap_or(false) {
            return Err(E2eError::system("pre-apply capability monitor failed"));
        }
        let original_held = || Ok((!initial.is_finished(), hold.is_held()?));
        let old_identity = self.original_identity(&attempts)?;
        let mut context = Handoff {
            client: &self.client,
            original_held: &original_held,
            old_identity,
            original_holder: self.cluster.controller_identity(0)?,
            before_transition: None,
            applied: Instant::now(),
            continuity_observed: false,
        };
        if context.old_identity.is_some() {
            let (_, before) = context.observe_held(&self.cluster)?;
            context.before_transition = Some(before);
        }
        let applied = Instant::now();
        context.applied = applied;
        let promotion = handoff(&self.cluster, &self.releases, &mut context, &monitor, hold);
        let mut continuity_observed = context.continuity_observed;
        let promotion = match promotion {
            Ok((next, retiring, timing, mut witness)) => {
                continuity_observed = false;
                let status = match retiring {
                    Some(retiring) => self.cluster.finish_retire(retiring),
                    None => self.cluster.wait_exit(0).and_then(|status| {
                        let (_, original_pid, _, _) =
                            context.old_identity.as_ref().ok_or_else(|| {
                                E2eError::system("original controller evidence missing")
                            })?;
                        let restarted_pid = self.cluster.wait_for_host_restart(
                            0,
                            *original_pid,
                            &context.original_holder,
                        )?;
                        let body = ColdHelpers::proof_request(
                            self.cluster.stack().gang_cli(),
                            &self.client.endpoint,
                        )?;
                        probe_refusal(
                            &self.client.endpoint,
                            &self.client.credentials,
                            self.cluster.controller_public_target(0)?,
                            body,
                            self.cluster.stack().settings().upgrade_probe_timeout,
                        )?;
                        witness.restarted_pid = Some(restarted_pid);
                        self.cluster.deregister_controller(0)?;
                        self.cluster.retire(0)?;
                        Ok(status)
                    }),
                };
                status.map(|status| {
                    witness.retired = true;
                    (next, status, timing, witness)
                })
            }
            Err(error) => Err(error),
        };
        let settled = hold.settle(promotion, || {
            drain(
                &self.builds.journal,
                &self.builds.initial.run,
                &mut attempts,
            )
        });
        let observation = InitialObservation {
            handoff: settled,
            build: joined(initial),
            attempts,
            in_flight,
            continuity_observed,
        };
        self.finish_upgrade(scope, hold, observation, &mut monitor)
    }

    fn finish_upgrade<'scope, 'env, K>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        hold: &Hold<'env, K>,
        observation: InitialObservation,
        monitor: &mut RunningMonitor<'_>,
    ) -> Result<Vec<Check>>
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()> + Send + Sync + 'env,
    {
        let promotion = match observation.handoff {
            Ok((index, status, timing, drain)) => Promotion {
                index,
                status,
                timing,
                drain,
            },
            Err(error) => {
                return Self::handoff_failure(error, monitor, observation.continuity_observed);
            }
        };
        let (initial, promotion) =
            self.finish_initial(observation.build, observation.attempts, promotion)?;
        let candidate = fs::canonicalize(self.releases.candidate_binary)
            .map_err(E2eError::io("resolving candidate release executable"))?;
        let old_binary = fs::canonicalize(&self.releases.old.binary)
            .map_err(E2eError::io("resolving old release executable"))?;
        let replacement_required = old_binary != candidate;
        let old_pid = observation
            .in_flight
            .values()
            .next()
            .ok_or_else(|| E2eError::system("original held worker missing"))?
            .0;
        let old_exited = if replacement_required {
            process::await_gone(old_pid, self.cluster.stack().settings().startup_timeout)?
                && !Path::new(&format!("/proc/{old_pid}/exe")).exists()
        } else {
            !Path::new(&format!("/proc/{old_pid}/exe")).exists()
        };
        self.client.assert_warm_credentials(
            &self.cluster,
            promotion.index,
            self.cluster.stack().settings().command_timeout,
        )?;
        self.client.assert_original_rc()?;
        let old_post = self.post_build(
            scope,
            hold,
            &self.builds.old_post,
            self.client.remote.clone(),
        )?;
        let candidate_remote = self.candidate_remote()?;
        let post = self.post_build(scope, hold, &self.builds.post, candidate_remote)?;
        let old_workers_remaining = self.old_workers_remaining(&old_binary)?;
        let rollout = RolloutWitness {
            workers: WorkerRollout {
                replacement_required,
                old_exited,
                old_workers_remaining,
            },
            old_client_current: current_executables(
                &old_post.attempts,
                &old_post.executables,
                &candidate,
            ),
            candidate_client_current: current_executables(
                &post.attempts,
                &post.executables,
                &candidate,
            ),
        };
        let outcome = monitor.finish()?;
        let mut checks = self.report(
            &initial,
            &observation.in_flight,
            &promotion,
            &post,
            &outcome,
            &rollout,
        )?;
        self.client.assert_original_rc()?;
        checks.push(detail(
            "fresh old client build",
            build_ok(&old_post.outcome, &old_post.evidence, 1)
                && exact_attempts(&old_post.attempts, 1)
                && rollout.old_client_current,
            format!(
                "{}, failed actions={}, successful actions={}, attempts={}",
                old_post.outcome.exit_summary(),
                old_post.evidence.failed_actions,
                old_post.evidence.successful_actions,
                old_post.attempts.len(),
            ),
        ));
        checks.push(Check::passed(
            "original warm session and bound proof accepted by the successor while held and after old exit; original rc unchanged",
        ));
        Ok(checks)
    }

    fn old_workers_remaining(&self, old_binary: &Path) -> Result<usize> {
        let entries = process::snapshot();
        let workers =
            process::workers_of(&entries, &self.cluster.stack().placement().config_file());
        let mut old_workers_remaining = 0;
        for worker in workers {
            match fs::canonicalize(format!("/proc/{}/exe", worker.pid)) {
                Ok(executable) => {
                    if executable == old_binary {
                        old_workers_remaining += 1;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(E2eError::io("observing fleet worker executable")(error)),
            }
        }
        Ok(old_workers_remaining)
    }

    fn start_monitor<'scope, 'env>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
    ) -> Result<RunningMonitor<'scope>>
    where
        'env: 'scope,
    {
        let stack = self.cluster.stack();
        let cold = ColdHelpers::prepare(
            stack.gang_cli(),
            &stack.endpoint()?,
            &stack.placement().root().join("cold-credentials"),
            stack.settings().command_timeout,
        )?;
        Ok(RunningMonitor::start(
            scope,
            MonitorStart {
                endpoint: self.client.endpoint.clone(),
                credentials: self.client.credentials.clone(),
                settings: self.cluster.stack().settings(),
                cold,
            },
        ))
    }

    fn finish_initial(
        &self,
        build: Result<BuildOutcome>,
        attempts: Attempts,
        promotion: Promotion,
    ) -> Result<(ObservedBuild, Promotion)> {
        let outcome = build?;
        let evidence = BuildEvidence::read(&self.builds.initial.bep, &self.builds.initial.rpc)?;
        Ok((
            ObservedBuild {
                outcome,
                evidence,
                attempts,
                executables: BTreeMap::new(),
            },
            promotion,
        ))
    }

    fn handoff_failure(
        error: E2eError,
        monitor: &mut RunningMonitor<'_>,
        continuity_observed: bool,
    ) -> Result<Vec<Check>> {
        let outcome = monitor.finish()?;
        if continuity_observed && matches!(error, E2eError::Timeout { .. }) {
            return Ok(vec![detail(
                "rolling upgrade",
                false,
                format!(
                    "ordinary-refresh; promotion: {error}; cold mints={}, probe failures={}, retries={}, max gap={:?}; build evidence unavailable after cancellation",
                    outcome.3, outcome.0.failures, outcome.2, outcome.1
                ),
            )]);
        }
        Err(error)
    }

    fn candidate_remote(&self) -> Result<Remote> {
        let stack = self.cluster.stack();
        let home = stack.placement().root().join("upgrade-candidate-client");
        fs::create_dir_all(&home).map_err(E2eError::io("creating candidate rc home"))?;
        let endpoint = stack.endpoint()?;
        let bazelrc = crate::secured::render_deployment_bazelrc(
            self.releases.candidate_binary,
            &stack.placement().config_file(),
            &endpoint,
            Some(stack.ca_pin()?),
            &stack.placement().workspace(),
            &home,
            stack.settings().command_timeout,
        )?;
        self.client.assert_original_rc()?;
        Ok(Remote::Gang {
            host: endpoint
                .host_str()
                .ok_or_else(|| E2eError::system("candidate endpoint has no host"))?
                .to_owned(),
            bazelrc,
            home: self.client.credentials.home.clone(),
            helper: self.releases.candidate_binary.to_path_buf(),
        })
    }

    fn post_build<'scope, 'env, K>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        hold: &Hold<'env, K>,
        build: &'env LoggedBuild,
        remote: Remote,
    ) -> Result<ObservedBuild>
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()>,
    {
        let mut attempts = Attempts::new();
        let mut executables = BTreeMap::new();
        let post = hold.build(scope, None, move |_, stop| {
            let result = build.driver.build_until(
                &remote,
                &[Probe::ControllerLoss0],
                Some(&build.run),
                stop,
            );
            self.builds.journal.write_line(JOURNAL_END)?;
            result
        });
        let settled = hold.settle(Ok(()), || {
            loop {
                let line = self.builds.journal.read_line()?;
                if line == JOURNAL_END {
                    return Ok(());
                }
                record_attempt(&line, build.run.as_ref(), &mut attempts)?;
                for attempt in attempts.values() {
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        executables.entry(attempt.worker)
                    {
                        entry.insert(worker_executable(attempt.worker)?);
                    }
                }
            }
        });
        let result = joined(post);
        settled?;
        Ok(ObservedBuild {
            outcome: result?,
            evidence: BuildEvidence::read(&build.bep, &build.rpc)?,
            attempts,
            executables,
        })
    }

    fn report(
        &self,
        initial: &ObservedBuild,
        in_flight: &InFlight,
        promotion: &Promotion,
        post: &ObservedBuild,
        monitor: &MonitorOutcome,
        rollout: &RolloutWitness,
    ) -> Result<Vec<Check>> {
        let stack = self.cluster.stack();
        let leader = self.cluster.controller_is_leader(promotion.index)?;
        Ok(final_verdict(&VerdictInputs {
            mode: self.releases.mode,
            initial_outcome: &initial.outcome,
            initial_evidence: &initial.evidence,
            initial_attempts: &initial.attempts,
            in_flight,
            post_outcome: &post.outcome,
            post_evidence: &post.evidence,
            post_attempts: &post.attempts,
            availability: &monitor.0,
            gap: monitor.1,
            retries: monitor.2,
            cold_mints: monitor.3,
            timing: promotion.timing,
            drain: &promotion.drain,
            status: promotion.status,
            leader,
            gap_limit: stack.settings().upgrade_gap_limit,
            old_version: &self.releases.old_version,
            candidate_version: &self.releases.candidate_version,
            elapsed: self.started.elapsed(),
            rollout,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_only_requires_exact_maximum_transition_and_no_other_delta() {
        let old = "poll_interval = '50ms'\n[[fleets]]\nmax_instances = 1";
        let new = "poll_interval = '50ms'\n[[fleets]]\nmax_instances = 2";
        assert!(require_max_only(old, new).is_ok());
        for invalid in [
            new.replace("50ms", "51ms"),
            new.replace("= 2", "= 3"),
            old.to_owned(),
            "poll_interval = '50ms'".to_owned(),
        ] {
            assert!(require_max_only(old, &invalid).is_err());
        }
        assert!(require_max_only(new, new).is_err());
    }

    #[test]
    fn old_rc_witness_preserves_exact_rendered_bytes() -> gang_testkit::TestResult {
        let root = tempfile::tempdir()?;
        let rc = root.path().join("old-client.bazelrc");
        let original =
            b"build --remote_executor=grpcs://127.0.0.1:1234\r\ncommon --remote_timeout=60";
        fs::write(&rc, original)?;
        let client = OldClient {
            remote: Remote::Plain {
                bazelrc: rc.clone(),
                home: root.path().to_path_buf(),
            },
            credentials: Credentials {
                principal: "old".to_owned(),
                home: root.path().to_path_buf(),
                token: "old".to_owned(),
                authority: None,
            },
            endpoint: Url::parse("https://127.0.0.1:1234")?,
            original_rc: original.to_vec(),
            original_session: Vec::new(),
        };
        client.assert_original_rc()?;
        fs::write(&rc, String::from_utf8_lossy(original).replace("\r\n", "\n"))?;
        assert!(client.assert_original_rc().is_err());
        fs::write(&rc, original)?;
        client.assert_original_rc()?;
        Ok(())
    }

    #[test]
    fn failed_action_and_long_gap_fail_normal_verdict() {
        let outcome = BuildOutcome::new(Some(0), "INFO: 8 processes: 8 remote.".to_owned());
        let mut evidence = BuildEvidence {
            successful_actions: 8,
            finished_success: true,
            rpc_calls: 1,
            ..BuildEvidence::default()
        };
        assert!(build_ok(&outcome, &evidence, 8));
        evidence.successful_actions = 9;
        assert!(build_ok(&outcome, &evidence, 8));
        evidence.failed_actions = 1;
        assert!(!build_ok(&outcome, &evidence, 8));
        let mut availability = Availability::default();
        availability.sample(Duration::from_secs(5), false);
        assert!(!availability_ok(
            availability.finish(Duration::from_secs(5)),
            Duration::from_secs(4)
        ));
    }

    fn verdict_attempts() -> (InFlight, Attempts, Attempts) {
        let mut initial_attempts = Attempts::new();
        for (index, probe) in PROBES.iter().take(2).enumerate() {
            let pid = index + 100;
            let action = probe.as_str();
            record_attempt(
                &format!("run loss start {action} 42 {pid} 10 exclusive"),
                "run",
                &mut initial_attempts,
            )
            .expect("start");
        }
        let in_flight = capture_in_flight(&initial_attempts);
        for (index, probe) in PROBES.iter().enumerate() {
            let pid = index + 100;
            let action = probe.as_str();
            let worker = if index < 2 { 42 } else { 43 };
            if index >= 2 {
                record_attempt(
                    &format!("run loss start {action} {worker} {pid} 10 exclusive"),
                    "run",
                    &mut initial_attempts,
                )
                .expect("start");
            }
            record_attempt(
                &format!("run loss end {action} {worker} {pid} 20"),
                "run",
                &mut initial_attempts,
            )
            .expect("end");
        }
        let mut post_attempts = Attempts::new();
        let action = Probe::ControllerLoss0.as_str();
        record_attempt(
            &format!("post loss start {action} 43 200 30 exclusive"),
            "post",
            &mut post_attempts,
        )
        .expect("start");
        record_attempt(
            &format!("post loss end {action} 43 200 40"),
            "post",
            &mut post_attempts,
        )
        .expect("end");
        (in_flight, initial_attempts, post_attempts)
    }

    #[test]
    fn frozen_attempts_must_finish_unchanged_without_reexecution() {
        let (frozen, mut initial, _) = verdict_attempts();
        assert_eq!(frozen.len(), 2);
        assert!(uninterrupted(&frozen, &initial));
        assert!(exact_attempts(&initial, PROBES.len()));
        assert!(!uninterrupted(&InFlight::new(), &initial));
        let key = frozen.keys().next().expect("frozen attempt").clone();
        let attempt = initial.get_mut(&key).expect("attempt");
        attempt.worker += 1;
        assert!(!uninterrupted(&frozen, &initial));
        let attempt = initial.get_mut(&key).expect("attempt");
        attempt.worker -= 1;
        attempt.start += 1;
        assert!(!uninterrupted(&frozen, &initial));
        let attempt = initial.get_mut(&key).expect("attempt");
        attempt.start -= 1;
        let end = attempt.end.take();
        assert!(!uninterrupted(&frozen, &initial));
        initial.get_mut(&key).expect("attempt").end = end;
        let removed = initial.remove(&key).expect("attempt");
        assert!(!uninterrupted(&frozen, &initial));
        initial.insert(key.clone(), removed);
        record_attempt(
            &format!("run loss start {} 43 999 30 exclusive", key.0),
            "run",
            &mut initial,
        )
        .expect("reexecution start");
        record_attempt(
            &format!("run loss end {} 43 999 40", key.0),
            "run",
            &mut initial,
        )
        .expect("reexecution end");
        assert!(uninterrupted(&frozen, &initial));
        assert!(!exact_attempts(&initial, PROBES.len()));
    }

    #[test]
    fn post_upgrade_attempts_must_finish_once() {
        let (_, _, mut post) = verdict_attempts();
        assert!(exact_attempts(&post, 1));
        post.values_mut().next().expect("post attempt").end = None;
        assert!(!exact_attempts(&post, 1));
    }

    #[test]
    fn replacement_requires_the_candidate_executable_for_each_journal_worker() {
        let (_, _, post) = verdict_attempts();
        let candidate = Path::new("/candidate/gang");
        let mut executables = BTreeMap::new();
        assert!(!current_executables(&post, &executables, candidate));
        executables.insert(43, PathBuf::from("/old/gang"));
        assert!(!current_executables(&post, &executables, candidate));
        executables.insert(43, candidate.to_path_buf());
        assert!(current_executables(&post, &executables, candidate));
        assert!(!current_executables(
            &Attempts::new(),
            &executables,
            candidate
        ));
    }

    fn reexecuted_attempts() -> Attempts {
        let (_, mut attempts, _) = verdict_attempts();
        let action = PROBES[0].as_str();
        for line in [
            format!("run loss start {action} 43 999 30 exclusive"),
            format!("run loss end {action} 43 999 40"),
        ] {
            record_attempt(&line, "run", &mut attempts).expect("reexecution");
        }
        attempts
    }

    struct VerdictFixture {
        initial_outcome: BuildOutcome,
        post_outcome: BuildOutcome,
        initial_evidence: BuildEvidence,
        post_evidence: BuildEvidence,
        in_flight: InFlight,
        initial_attempts: Attempts,
        post_attempts: Attempts,
        availability: Availability,
        drain: DrainWitness,
        rollout: RolloutWitness,
    }

    impl VerdictFixture {
        fn new() -> Self {
            let (in_flight, initial_attempts, post_attempts) = verdict_attempts();
            Self {
                initial_outcome: BuildOutcome::new(
                    Some(0),
                    "INFO: 8 processes: 8 remote.".to_owned(),
                ),
                post_outcome: BuildOutcome::new(Some(0), "INFO: 1 process: 1 remote.".to_owned()),
                initial_evidence: BuildEvidence {
                    successful_actions: 8,
                    finished_success: true,
                    rpc_calls: 1,
                    ..BuildEvidence::default()
                },
                post_evidence: BuildEvidence {
                    successful_actions: 1,
                    finished_success: true,
                    rpc_calls: 1,
                    ..BuildEvidence::default()
                },
                in_flight,
                initial_attempts,
                post_attempts,
                availability: Availability::default(),
                drain: DrainWitness::default(),
                rollout: RolloutWitness {
                    workers: WorkerRollout {
                        replacement_required: true,
                        old_exited: true,
                        old_workers_remaining: 0,
                    },
                    old_client_current: true,
                    candidate_client_current: true,
                },
            }
        }

        fn inputs(&self) -> VerdictInputs<'_> {
            use std::os::unix::process::ExitStatusExt;

            VerdictInputs {
                initial_outcome: &self.initial_outcome,
                initial_evidence: &self.initial_evidence,
                initial_attempts: &self.initial_attempts,
                in_flight: &self.in_flight,
                post_outcome: &self.post_outcome,
                post_evidence: &self.post_evidence,
                post_attempts: &self.post_attempts,
                availability: &self.availability,
                gap: Duration::ZERO,
                retries: 0,
                cold_mints: 2,
                timing: PromotionTiming {
                    lease: Duration::ZERO,
                    client: Duration::ZERO,
                },
                mode: Refresh::Candidate,
                drain: &self.drain,
                status: std::process::ExitStatus::from_raw(0),
                leader: true,
                gap_limit: Duration::from_secs(4),
                old_version: "0.3.5",
                candidate_version: "candidate",
                elapsed: Duration::ZERO,
                rollout: &self.rollout,
            }
        }
    }

    fn assert_invalid_cold_mint_counts(input: &mut VerdictInputs<'_>) {
        for count in [0, 1, 3] {
            input.cold_mints = count;
            assert!(final_verdict(input).iter().any(Check::is_failure));
        }
    }
    #[test]
    fn final_verdict_requires_exactly_two_cold_mints() {
        let fixture = VerdictFixture::new();
        let mut input = fixture.inputs();

        assert!(final_verdict(&input).iter().any(Check::is_failure));
        let held_drain = credential_drain();
        input.drain = &held_drain;
        assert!(final_verdict(&input).iter().all(Check::is_passed));
        let mut missing_restart = credential_drain();
        missing_restart.restarted_pid = None;
        input.drain = &missing_restart;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        let mut unretired = credential_drain();
        unretired.retired = false;
        input.drain = &unretired;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.drain = &held_drain;
        let mut invalid_refusal = credential_drain();
        invalid_refusal
            .credentials
            .as_mut()
            .expect("held witness")
            .refusal = Some((200, false));
        input.drain = &invalid_refusal;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.drain = &held_drain;
        input.timing.lease = Duration::from_secs(5);
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.timing.lease = Duration::ZERO;
        input.drain = &held_drain;
        assert!(final_verdict(&input).iter().all(Check::is_passed));
        let missing_exit = RolloutWitness {
            workers: WorkerRollout {
                old_exited: false,
                ..fixture.rollout.workers
            },
            ..fixture.rollout
        };
        input.rollout = &missing_exit;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        let current_worker = RolloutWitness {
            workers: WorkerRollout {
                replacement_required: false,
                old_exited: false,
                old_workers_remaining: 1,
            },
            ..fixture.rollout
        };
        input.rollout = &current_worker;
        assert!(final_verdict(&input).iter().all(Check::is_passed));
        let stale_remains = RolloutWitness {
            workers: WorkerRollout {
                old_workers_remaining: 1,
                ..fixture.rollout.workers
            },
            ..fixture.rollout
        };
        input.rollout = &stale_remains;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        let wrong_executable = RolloutWitness {
            workers: WorkerRollout {
                ..fixture.rollout.workers
            },
            candidate_client_current: false,
            ..fixture.rollout
        };
        input.rollout = &wrong_executable;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.rollout = &fixture.rollout;
        let empty = InFlight::new();
        input.in_flight = &empty;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.in_flight = &fixture.in_flight;
        let reexecuted = reexecuted_attempts();
        input.initial_attempts = &reexecuted;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.initial_attempts = &fixture.initial_attempts;
        let interrupted = BuildOutcome::new(Some(1), "build failed".to_owned());
        input.initial_outcome = &interrupted;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.initial_outcome = &fixture.initial_outcome;
        let failures = BuildEvidence {
            failed_actions: 1,
            ..fixture.initial_evidence
        };
        input.initial_evidence = &failures;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.initial_evidence = &fixture.initial_evidence;
        let (_, mut changed, _) = verdict_attempts();
        changed
            .get_mut(fixture.in_flight.keys().next().expect("frozen attempt"))
            .expect("attempt")
            .worker += 1;
        input.initial_attempts = &changed;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.initial_attempts = &fixture.initial_attempts;
        assert_invalid_cold_mint_counts(&mut input);
    }

    fn credential_drain() -> DrainWitness {
        let state = HeldState {
            build_unfinished: true,
            hold_unreleased: true,
            old_live: true,
        };
        DrainWitness {
            elapsed: Duration::from_secs(8),
            restarted_pid: Some(43),
            retired: true,
            credentials: Some(HeldDrainWitness {
                started: Duration::from_millis(100),
                finished: Duration::from_millis(111),
                deadline: Duration::from_millis(8090),
                refusal: Some((503, true)),
                refusal_required: true,
                controller_pid: 42,
                before: state,
                after: state,
                owner_rechecked: true,
            }),
            stop_started: Some(Duration::from_secs(9)),
            ..DrainWitness::default()
        }
    }

    #[test]
    fn every_ordinary_plain_refresh_requires_a_prepared_drain_witness() {
        let mut drain = credential_drain();
        let gap = Duration::from_secs(4);
        assert!(drain_credentials_ok(&drain, true, gap));
        drain.credentials = None;
        assert!(!drain_credentials_ok(&drain, true, gap));
        assert!(drain_credentials_ok(&drain, false, gap));
    }

    #[test]
    fn drain_rejects_completed_released_or_dead_observations() {
        let mutations: [fn(&mut HeldDrainWitness); 7] = [
            |w| w.before.build_unfinished = false,
            |w| w.after.build_unfinished = false,
            |w| w.before.hold_unreleased = false,
            |w| w.after.hold_unreleased = false,
            |w| w.before.old_live = false,
            |w| w.after.old_live = false,
            |w| w.owner_rechecked = false,
        ];
        for mutate in mutations {
            let mut drain = credential_drain();
            mutate(drain.credentials.as_mut().expect("credential fixture"));
            assert!(!drain_credentials_ok(&drain, true, Duration::from_secs(4)));
        }
    }

    #[test]
    fn held_drain_requires_complete_not_started_refusal_from_original_process() {
        let mutations: [fn(&mut HeldDrainWitness); 7] = [
            |w| w.refusal = Some((200, false)),
            |w| w.refusal = Some((503, false)),
            |w| w.refusal = None,
            |w| w.controller_pid = 0,
            |w| w.started = w.finished,
            |w| w.deadline = Duration::ZERO,
            |w| w.deadline = w.finished.saturating_sub(Duration::from_nanos(1)),
        ];
        for mutate in mutations {
            let mut drain = credential_drain();
            mutate(drain.credentials.as_mut().expect("held witness"));
            assert!(!drain_credentials_ok(&drain, true, Duration::from_secs(4)));
        }
    }

    #[test]
    fn held_witness_precedes_release_and_needs_no_routing_timer() {
        let mut drain = credential_drain();
        let gap = Duration::from_secs(4);
        drain.elapsed = Duration::ZERO;
        assert!(drain_credentials_ok(&drain, true, gap));
        drain.stop_started = None;
        assert!(!drain_credentials_ok(&drain, true, gap));
        drain.stop_started = Some(Duration::from_millis(111));
        assert!(!drain_credentials_ok(&drain, true, gap));
        assert!(remaining_budget(Instant::now(), Duration::ZERO).is_err());
    }
}
