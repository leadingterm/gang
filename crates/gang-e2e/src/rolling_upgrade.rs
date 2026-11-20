//! Mixed-release controller handoff with an old client and an enrolled old worker.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::settings::{DeploymentInputs, ReleaseInput, Settings, UpgradeProcedure};
use crate::system::{Probe, Variant};
use crate::upgrade_credentials::{ColdHelpers, DrainCredentialWitness, DrainCredentials};
use crate::upgrade_evidence::BuildEvidence;
use crate::upgrade_fixture::ReleaseFixture;
use crate::upgrade_probe::{Availability, DirectTarget, probe_capabilities};

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

fn version_is(actual: &str, expected: &str) -> bool {
    actual.split_whitespace().any(|part| part == expected)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Calibration {
    None,
    RemovedPort,
    AliveDrain,
}

// Calibration is deliberately narrower than an arbitrary post-apply failure.
#[derive(Clone, Copy)]
enum Continuity {
    None,
    RemovedPortGap,
    AliveDrainGap,
}

fn calibrated(phase0: bool, continuity: Continuity) -> bool {
    phase0 && !matches!(continuity, Continuity::None)
}

fn calibration_sample_eligible(calibration: Calibration, applied: bool, draining: bool) -> bool {
    applied
        && match calibration {
            Calibration::None => false,
            Calibration::RemovedPort => true,
            Calibration::AliveDrain => draining,
        }
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
    drain_started: Duration,
    started: Duration,
    finished: Duration,
    deadline: Duration,
    direct: DrainCredentialWitness,
    before: HeldState,
    after: HeldState,
    owner_rechecked: bool,
}

impl HeldDrainWitness {
    fn valid(&self, stop: Duration, gap_limit: Duration) -> bool {
        let direct = &self.direct;
        self.before.valid()
            && self.after.valid()
            && self.owner_rechecked
            && direct.shared.status == 200
            && direct.shared.marker_absent
            && direct.shared.token_valid
            && direct.token_valid
            && direct.shared_started < direct.shared_finished
            && direct.shared_finished <= direct.shared_verification_started
            && direct.shared_verification_started < direct.shared_verification_finished
            && direct.shared_verification_finished <= direct.mint_started
            && direct.mint_started < direct.mint_finished
            && direct.mint_finished <= direct.verification_started
            && direct.verification_started < direct.verification_finished
            && direct.mint_finished.saturating_sub(direct.mint_started) <= gap_limit
            && self
                .started
                .checked_add(direct.verification_finished)
                .is_some_and(|finished| finished <= self.finished)
            && self.started < self.finished
            && self.finished <= self.deadline
            && self.finished < stop
    }

    fn receipt(&self) -> String {
        let direct = &self.direct;
        format!(
            "drain_cold_mints=1, leader-RPC/name witness not measured (known limit), witness apply offsets drain/start/end/deadline={:?}/{:?}/{:?}/{:?}, witness-relative shared start/end={:?}/{:?}, shared status={}, marker absent={}, shared NLB verification start/end={:?}/{:?}, shared token valid={}, NLB mint start/end={:?}/{:?}, new NLB verification start/end={:?}/{:?}, new token valid={}, held before/after={:?}/{:?}, incoming owner rechecked={}",
            self.drain_started,
            self.started,
            self.finished,
            self.deadline,
            direct.shared_started,
            direct.shared_finished,
            direct.shared.status,
            direct.shared.marker_absent,
            direct.shared_verification_started,
            direct.shared_verification_finished,
            direct.shared.token_valid,
            direct.mint_started,
            direct.mint_finished,
            direct.verification_started,
            direct.verification_finished,
            direct.token_valid,
            self.before,
            self.after,
            self.owner_rechecked,
        )
    }
}

fn requires_drain_credentials(procedure: UpgradeProcedure, calibration: Calibration) -> bool {
    procedure == UpgradeProcedure::InstanceRefresh && calibration == Calibration::None
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

fn drain_policy_ok(drain: &DrainWitness, warmup: Duration, window: Duration) -> bool {
    let (Some(witness), Some(stop)) = (&drain.credentials, drain.stop_started) else {
        return false;
    };
    let budget = warmup.min(window);
    !budget.is_zero()
        && drain.elapsed >= window
        && witness.drain_started <= witness.started
        && witness.drain_started.checked_add(budget) == Some(witness.deadline)
        && witness
            .drain_started
            .checked_add(window)
            .is_some_and(|end| end <= stop)
}

struct Handoff<'a> {
    client: &'a OldClient,
    original_held: &'a dyn Fn() -> Result<(bool, bool)>,
    credentials: Option<DrainCredentials>,
    old_identity: Option<(DirectTarget, u32, PathBuf)>,
    applied: Instant,
    continuity_observed: bool,
    verified_gap: bool,
}

impl Handoff<'_> {
    fn observe_held(&self, cluster: &Cluster<'_>) -> Result<(DirectTarget, HeldState)> {
        let (build_unfinished, hold_unreleased) = (self.original_held)()?;
        let target = cluster.controller_public_target(0)?;
        let (original_target, worker_pid, executable) = self
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
        if worker_executable(*worker_pid)? != *executable {
            return Err(E2eError::system("original held worker process changed"));
        }
        let state = HeldState {
            build_unfinished,
            hold_unreleased,
            old_live: true,
        };
        if !state.valid() {
            return Err(E2eError::system(
                "drain witness requires original unfinished held build",
            ));
        }
        Ok((target, state))
    }
}

fn remaining_budget(start: Instant, budget: Duration) -> Result<Duration> {
    let remaining = budget.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        return Err(E2eError::timeout("held drain credential witness", budget));
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
    let window = drain_window(releases, settings);
    let budget = warmup_window(releases, settings).min(window);
    let drain_started = start.duration_since(handoff.applied);
    let deadline = drain_started + budget;
    let old_leader_before = cluster.controller_is_leader(0)?;
    monitor.draining.store(true, Ordering::SeqCst);
    let observed = (|| {
        remaining_budget(start, budget)?;
        let promotion = confirm_promotion(cluster, next, handoff.client, start, Some(budget))?;
        let (target, before) = handoff.observe_held(cluster)?;
        let credentials = handoff
            .credentials
            .take()
            .ok_or_else(|| E2eError::system("required drain credentials missing"))?;
        let remaining = remaining_budget(start, budget)?;
        let started = handoff.applied.elapsed();
        let direct = credentials.witness(
            target,
            &handoff.client.credentials,
            settings.upgrade_probe_timeout,
            remaining,
        )?;
        let owner_rechecked = cluster.controller_is_leader(next)?;
        let (after_target, after) = handoff.observe_held(cluster)?;
        if after_target.address != target.address
            || after_target.proxy_protocol_v2 != target.proxy_protocol_v2
        {
            return Err(E2eError::system(
                "old controller changed during drain witness",
            ));
        }
        remaining_budget(start, budget)?;
        let credentials = HeldDrainWitness {
            drain_started,
            started,
            finished: handoff.applied.elapsed(),
            deadline,
            direct,
            before,
            after,
            owner_rechecked,
        };
        if !credentials.valid(handoff.applied.elapsed(), settings.upgrade_gap_limit) {
            return Err(E2eError::system("invalid held drain credential witness"));
        }
        Ok((credentials, promotion))
    })();
    let result = observed.and_then(|(credentials, promotion)| {
        while start.elapsed() < window && !monitor.failed() {
            std::thread::sleep(
                window
                    .saturating_sub(start.elapsed())
                    .min(Duration::from_millis(100)),
            );
        }
        Ok((
            DrainWitness {
                elapsed: start.elapsed(),
                old_leader_before,
                old_leader_after: cluster.controller_is_leader(0)?,
                credentials: Some(credentials),
                stop_started: None,
            },
            promotion,
        ))
    });
    monitor.draining.store(false, Ordering::SeqCst);
    result
}

fn scaled_window(actual_seconds: u64, cap: Duration) -> Duration {
    Duration::from_secs(actual_seconds).min(cap)
}

fn warmup_window(releases: &Releases<'_>, settings: &Settings) -> Duration {
    scaled_window(
        releases.candidate.controller.warmup_seconds,
        settings.upgrade_warmup_window,
    )
}

fn drain_window(releases: &Releases<'_>, settings: &Settings) -> Duration {
    scaled_window(
        releases.candidate.controller.deregistration_delay_seconds,
        settings.upgrade_drain_window,
    )
}

fn post_exit_refresh_steps(procedure: UpgradeProcedure) -> &'static [TerminationStep] {
    match procedure {
        UpgradeProcedure::PreStop => &[TerminationStep::Deregister, TerminationStep::Drain],
        UpgradeProcedure::InstanceRefresh => &[],
    }
}

fn post_exit_refresh(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    witness: &mut DrainWitness,
) -> Result<()> {
    for step in post_exit_refresh_steps(releases.procedure) {
        match step {
            TerminationStep::Deregister => cluster.deregister_controller(0)?,
            TerminationStep::Drain => {
                let start = Instant::now();
                std::thread::sleep(drain_window(releases, cluster.stack().settings()));
                witness.elapsed = start.elapsed();
            }
            TerminationStep::Stop => {
                return Err(E2eError::system("unexpected stop after old exit"));
            }
        }
    }
    Ok(())
}

fn alive_drain(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    monitor: &RunningMonitor<'_>,
) -> Result<DrainWitness> {
    let old_leader_before = cluster.controller_is_leader(0)?;
    if releases.calibration == Calibration::AliveDrain && !old_leader_before {
        return Err(E2eError::system(
            "old controller was not leader at drain start",
        ));
    }
    monitor.draining.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let window = drain_window(releases, cluster.stack().settings());
    while start.elapsed() < window && !monitor.failed() {
        std::thread::sleep(
            window
                .saturating_sub(start.elapsed())
                .min(Duration::from_millis(100)),
        );
    }
    monitor.draining.store(false, Ordering::SeqCst);
    let witness = DrainWitness {
        elapsed: start.elapsed(),
        old_leader_before,
        old_leader_after: cluster.controller_is_leader(0)?,
        ..DrainWitness::default()
    };
    Ok(witness)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminationStep {
    Stop,
    Deregister,
    Drain,
}

fn termination_steps(procedure: UpgradeProcedure) -> &'static [TerminationStep] {
    match procedure {
        UpgradeProcedure::PreStop => &[TerminationStep::Stop],
        UpgradeProcedure::InstanceRefresh => &[
            TerminationStep::Deregister,
            TerminationStep::Drain,
            TerminationStep::Stop,
        ],
    }
}

struct Terminated {
    retiring: Retiring,
    witness: DrainWitness,
    stop_started: Instant,
    promotion: Option<PromotionTiming>,
}

fn terminate_old(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    monitor: &RunningMonitor<'_>,
    next: usize,
    handoff: &mut Handoff<'_>,
) -> Result<Terminated> {
    let mut retiring = None;
    let mut stop_started = None;
    let mut witness = DrainWitness::default();
    let mut promotion = None;
    let required = requires_drain_credentials(releases.procedure, releases.calibration);
    for step in termination_steps(releases.procedure) {
        match step {
            TerminationStep::Stop => {
                witness.stop_started = Some(handoff.applied.elapsed());
                if !drain_credentials_ok(
                    &witness,
                    required,
                    cluster.stack().settings().upgrade_gap_limit,
                ) {
                    return Err(E2eError::system(
                        "required pre-stop drain witness missing or invalid",
                    ));
                }
                if required {
                    handoff.observe_held(cluster)?;
                }
                stop_started = Some(Instant::now());
                retiring = Some(cluster.begin_retire(0)?);
                handoff.continuity_observed = true;
            }
            TerminationStep::Deregister => {
                cluster.deregister_controller(0)?;
                handoff.continuity_observed = true;
            }
            TerminationStep::Drain => {
                let drained = if required {
                    held_drain(cluster, releases, monitor, next, handoff).map(
                        |(witness, timing)| {
                            promotion = Some(timing);
                            witness
                        },
                    )
                } else {
                    alive_drain(cluster, releases, monitor)
                };
                if drained.is_err() && releases.calibration == Calibration::AliveDrain {
                    handoff.continuity_observed = false;
                }
                witness = drained?;
                if monitor.failed() {
                    if releases.calibration == Calibration::AliveDrain && !witness.old_leader_after
                    {
                        handoff.continuity_observed = false;
                    } else {
                        handoff.verified_gap = true;
                    }
                    return Err(E2eError::system(format!(
                        "measured continuity failure; alive drain witness: {witness:?}"
                    )));
                }
            }
        }
    }
    Ok(Terminated {
        retiring: retiring
            .ok_or_else(|| E2eError::system("termination did not signal old controller"))?,
        witness,
        stop_started: stop_started
            .ok_or_else(|| E2eError::system("termination did not record stop time"))?,
        promotion,
    })
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

fn warmup_guard(old_leader: bool, incoming_healthy: bool) -> Result<()> {
    if !old_leader && !incoming_healthy {
        return Err(E2eError::system(
            "outgoing controller retired before candidate NLB health",
        ));
    }
    Ok(())
}

fn plain_warmup(
    cluster: &Cluster<'_>,
    next: usize,
    window: Duration,
    monitor: &RunningMonitor<'_>,
    client: &OldClient,
    calibration: Calibration,
) -> Result<()> {
    let start = Instant::now();
    let mut minted = false;
    loop {
        if !minted {
            let old_leader = cluster.controller_is_leader(0)?;
            let incoming_healthy = cluster.controller_is_healthy(next)?;
            warmup_guard(old_leader, incoming_healthy)?;
            if start.elapsed() < window && incoming_healthy {
                mint_warm_pair(cluster, next, monitor, client)?;
                minted = true;
            }
        }
        if start.elapsed() >= window {
            break;
        }
        std::thread::sleep(
            window
                .saturating_sub(start.elapsed())
                .min(Duration::from_millis(100)),
        );
    }
    if !minted && calibration == Calibration::None {
        return Err(E2eError::system(
            "candidate did not become NLB healthy during warmup for cold mints",
        ));
    }
    Ok(())
}

fn handoff<K: Fn(Option<&str>) -> Result<()>>(
    cluster: &Cluster<'_>,
    releases: &Releases<'_>,
    context: &mut Handoff<'_>,
    monitor: &RunningMonitor<'_>,
    hold: &Hold<'_, K>,
) -> Result<(usize, Retiring, PromotionTiming, DrainWitness)> {
    cluster.apply_release(&releases.candidate)?;
    monitor.applied.store(true, Ordering::SeqCst);
    let startup = cluster.stack().settings().startup_timeout;
    let startup_started = Instant::now();
    let next = cluster.start_controller_release_within(
        releases.candidate_binary,
        &releases.candidate,
        startup,
    )?;
    cluster.wait_for_controller_serve_within(
        next,
        startup.saturating_sub(startup_started.elapsed()),
    )?;
    let warmup = warmup_window(releases, cluster.stack().settings());
    if releases.procedure == UpgradeProcedure::PreStop {
        let deadline = startup
            .saturating_add(warmup)
            .saturating_sub(startup_started.elapsed());
        cluster.wait_for_controller_health_within(next, deadline)?;
        mint_warm_pair(cluster, next, monitor, context.client)?;
    } else {
        plain_warmup(
            cluster,
            next,
            warmup,
            monitor,
            context.client,
            releases.calibration,
        )?;
    }
    let terminated = terminate_old(cluster, releases, monitor, next, context)?;
    let promotion = match terminated.promotion {
        Some(promotion) => Ok(promotion),
        None => confirm_promotion(cluster, next, context.client, terminated.stop_started, None),
    };
    hold.release()?;
    Ok((next, terminated.retiring, promotion?, terminated.witness))
}

// This runs only after the real old helper or its verification has failed. Observations
// cannot replace that failure, and each direct request keeps the original authority and CA.
fn mint_warm_pair(
    cluster: &Cluster<'_>,
    next: usize,
    monitor: &RunningMonitor<'_>,
    client: &OldClient,
) -> Result<()> {
    let Err(error) = monitor.mint_pair() else {
        return Ok(());
    };
    let old_leading = diagnostic_bool(&cluster.controller_is_leader(0));
    let incoming_leading = diagnostic_bool(&cluster.controller_is_leader(next));
    let incoming_healthy = diagnostic_bool(&cluster.controller_is_healthy(next));
    let targets = [
        cluster.controller_public_target(0),
        cluster.controller_public_target(next),
    ];
    let request = cluster.stack().endpoint().and_then(|endpoint| {
        ColdHelpers::diagnostic_body(cluster.stack().gang_cli(), &endpoint)
            .map(|body| (endpoint, body))
    });
    let observations = targets.map(|target| {
        let observed = target.and_then(|target| {
            let (endpoint, body) = request
                .as_ref()
                .map_err(|_| E2eError::system("warm diagnostic unavailable"))?;
            crate::upgrade_probe::probe_authentication(
                endpoint,
                &client.credentials,
                target,
                body.clone(),
                cluster.stack().settings().upgrade_probe_timeout,
            )
        });
        match observed {
            Ok(observed) => format!(
                "http_status={}, marker={}, elapsed={:?}",
                observed.status, observed.marker, observed.elapsed
            ),
            Err(_) => "unavailable".to_owned(),
        }
    });
    Err(E2eError::system_from(format!(
        "warm credential failure: old_leading={old_leading}, incoming_leading={incoming_leading}, incoming_healthy={incoming_healthy}; old_direct=({}); incoming_direct=({})",
        observations[0], observations[1]
    ))(error))
}

fn diagnostic_bool(observation: &Result<bool>) -> &'static str {
    match observation {
        Ok(true) => "true",
        Ok(false) => "false",
        Err(_) => "unavailable",
    }
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
    let tokens = helpers.mint_pair()?;
    let mut credentials = original.clone();
    for token in tokens {
        credentials.token = token;
        probe_capabilities(endpoint, &credentials, timeout)?;
    }
    Ok(())
}

struct MonitorArgs<'a, K: Fn(Option<&str>) -> Result<()>> {
    endpoint: Url,
    credentials: Credentials,
    settings: &'a crate::settings::Settings,
    ready: mpsc::SyncSender<bool>,
    gap_send: mpsc::SyncSender<()>,
    commands: mpsc::Receiver<MonitorCommand>,
    cold: ColdHelpers,
    applied: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    gap_failed: Arc<AtomicBool>,
    hold: Arc<Hold<'a, K>>,
    calibration: Calibration,
}

fn monitor_availability<K: Fn(Option<&str>) -> Result<()>>(
    args: MonitorArgs<'_, K>,
) -> (Availability, Duration, usize, usize) {
    let start = Instant::now();
    let mut availability = Availability::default();
    let mut first = Some(args.ready);
    let mut previous_failed = false;
    let mut retries = 0;
    let mut last_success = Duration::ZERO;
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
        let elapsed = start.elapsed();
        if success {
            last_success = elapsed;
        } else if calibration_sample_eligible(
            args.calibration,
            args.applied.load(Ordering::SeqCst),
            args.draining.load(Ordering::SeqCst),
        ) && elapsed.saturating_sub(last_success) > args.settings.upgrade_gap_limit
            && !args.gap_failed.swap(true, Ordering::SeqCst)
        {
            let _ = args.gap_send.try_send(());
            let _ = args.hold.release();
            args.hold.cancel("old executor availability gap exceeded");
        }
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
    post_outcome: &'a BuildOutcome,
    post_evidence: &'a BuildEvidence,
    post_attempts: &'a Attempts,
    availability: &'a Availability,
    gap: Duration,
    retries: usize,
    cold_mints: usize,
    timing: PromotionTiming,
    procedure: UpgradeProcedure,
    warmup_policy: (Duration, Duration),
    drain_policy: (Duration, Duration),
    drain: &'a DrainWitness,
    status: std::process::ExitStatus,
    leader: bool,
    gap_limit: Duration,
    old_version: &'a str,
    candidate_version: &'a str,
    elapsed: Duration,
    negative_control: bool,
}

fn final_verdict(input: &VerdictInputs<'_>) -> Vec<Check> {
    let workers: BTreeSet<_> = input
        .initial_attempts
        .values()
        .chain(input.post_attempts.values())
        .map(|attempt| attempt.worker)
        .collect();
    let attempts = input.initial_attempts.len() + input.post_attempts.len();
    let once = exact_attempts(input.initial_attempts, PROBES.len())
        && exact_attempts(input.post_attempts, 1);
    let ok = build_ok(input.initial_outcome, input.initial_evidence, 8)
        && build_ok(input.post_outcome, input.post_evidence, 1)
        && once
        && workers.len() == 1
        && input.status.success()
        && input.leader
        && input.cold_mints == 2
        && drain_credentials_ok(
            input.drain,
            input.procedure == UpgradeProcedure::InstanceRefresh && !input.negative_control,
            input.gap_limit,
        )
        && (input.procedure != UpgradeProcedure::InstanceRefresh
            || input.negative_control
            || drain_policy_ok(input.drain, input.warmup_policy.1, input.drain_policy.1))
        && input.timing.lease <= input.gap_limit
        && availability_ok(input.gap, input.gap_limit);
    let termination = match input.procedure {
        UpgradeProcedure::PreStop => "pre-stop SIGTERM then deregistration",
        UpgradeProcedure::InstanceRefresh => "modeled late notification/hook then SIGTERM",
    };
    let drain_receipt = input.drain.credentials.as_ref().map_or_else(
        || "drain_cold_mints=0, held drain credentials absent, leader-RPC/name witness not measured (known limit)".to_owned(),
        HeldDrainWitness::receipt,
    );
    let metrics = format!(
        "procedure={:?}, operator_step={}, observed_plain_guard_checked={}, termination={termination}, warmup actual/scaled={:?}/{:?}, drain actual/scaled={:?}/{:?}, drain elapsed={:?}, old leader before/after={}/{}, versions={:?}->{:?}, initial exit={:?} post exit={:?}, failed actions={}/{}, successful actions={}/{}, attempts={attempts}, reexecutions={}, workers={workers:?}, rpc calls={}/{}, rpc failures={}/{}, rpc retries={}/{}, execute/wait correlations={}/{}, warmcold_mints={}, probe failures={}, repeated probes after failures={}, max gap={:?}, lease takeover={:?}, client promotion={:?}, old exit={}, final leader={}, total={:?}, {drain_receipt}, stop apply offset={:?}",
        input.procedure,
        input.procedure == UpgradeProcedure::PreStop,
        input.procedure == UpgradeProcedure::InstanceRefresh,
        input.warmup_policy.0,
        input.warmup_policy.1,
        input.drain_policy.0,
        input.drain_policy.1,
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
    );
    vec![detail(
        if input.negative_control {
            "upgrade negative control"
        } else {
            "rolling upgrade"
        },
        !input.negative_control && ok,
        metrics,
    )]
}

struct Releases<'a> {
    old: &'a ReleaseInput,
    candidate_binary: &'a Path,
    old_version: String,
    candidate_version: String,
    old_fixture: ReleaseFixture,
    candidate: ReleaseFixture,
    negative_control: bool,
    calibration: Calibration,
    procedure: UpgradeProcedure,
    settings: &'a Settings,
}

struct OldClient {
    remote: Remote,
    credentials: Credentials,
    endpoint: Url,
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
}

struct Promotion {
    index: usize,
    status: std::process::ExitStatus,
    timing: PromotionTiming,
    drain: DrainWitness,
}

type MonitorOutcome = (Availability, Duration, usize, usize);

struct RunningMonitor<'scope> {
    commands: mpsc::Sender<MonitorCommand>,
    gap: mpsc::Receiver<()>,
    first: mpsc::Receiver<bool>,
    applied: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    gap_failed: Arc<AtomicBool>,
    join: Option<ScopedJoinHandle<'scope, MonitorOutcome>>,
}

struct MonitorStart<'a, K: Fn(Option<&str>) -> Result<()>> {
    endpoint: Url,
    credentials: Credentials,
    settings: &'a Settings,
    hold: Arc<Hold<'a, K>>,
    calibration: Calibration,
    cold: ColdHelpers,
}

impl<'scope> RunningMonitor<'scope> {
    fn start<'env, K>(scope: &'scope Scope<'scope, 'env>, start: MonitorStart<'env, K>) -> Self
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()> + Send + Sync + 'env,
    {
        let (commands, monitor_commands) = mpsc::channel();
        let (ready, first) = mpsc::sync_channel(1);
        let (gap_send, gap) = mpsc::sync_channel(1);
        let applied = Arc::new(AtomicBool::new(false));
        let gap_failed = Arc::new(AtomicBool::new(false));
        let draining = Arc::new(AtomicBool::new(false));
        let args = MonitorArgs {
            endpoint: start.endpoint,
            credentials: start.credentials,
            settings: start.settings,
            ready,
            gap_send,
            commands: monitor_commands,
            cold: start.cold,
            applied: Arc::clone(&applied),
            draining: Arc::clone(&draining),
            gap_failed: Arc::clone(&gap_failed),
            hold: start.hold,
            calibration: start.calibration,
        };
        let join = scope.spawn(move || monitor_availability(args));
        Self {
            commands,
            gap,
            first,
            applied,
            draining,
            gap_failed,
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

    fn failed(&self) -> bool {
        self.gap_failed.load(Ordering::SeqCst)
    }

    fn wait_gap(&self, settings: &Settings, applied: Instant) {
        let remaining = settings.upgrade_gap_limit.saturating_sub(applied.elapsed())
            + settings.upgrade_probe_timeout
            + settings.upgrade_probe_interval;
        let _ = self.gap.recv_timeout(remaining);
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

fn select_releases(
    invocation: &DeploymentInputs,
    calibration: Calibration,
) -> Result<Releases<'_>> {
    let inputs = invocation
        .upgrade
        .as_ref()
        .ok_or_else(|| E2eError::system("upgrade inputs missing"))?;
    let old = match calibration {
        Calibration::RemovedPort => &inputs.historical,
        Calibration::AliveDrain => &inputs.calibration,
        Calibration::None => &inputs.previous,
    };
    let (binary, fixture) = if calibration == Calibration::None {
        (&invocation.gang, &inputs.candidate_fixture)
    } else {
        (&inputs.calibration.binary, &inputs.calibration.fixture)
    };
    let old_version = version(&old.binary, invocation.settings.command_timeout)?;
    let candidate_version = version(binary, invocation.settings.command_timeout)?;
    let expected_old = match calibration {
        Calibration::RemovedPort => Some("0.3.1"),
        Calibration::AliveDrain => Some("0.3.2"),
        Calibration::None => None,
    };
    if let Some(expected) = expected_old
        && (!version_is(&old_version, expected) || !version_is(&candidate_version, "0.3.2"))
    {
        return Err(E2eError::system(format!(
            "negative control requires {expected} -> 0.3.2, got {old_version:?} -> {candidate_version:?}"
        )));
    }
    Ok(Releases {
        old,
        candidate_binary: binary,
        old_version,
        candidate_version,
        old_fixture: ReleaseFixture::read(&old.fixture)?,
        candidate: ReleaseFixture::read(fixture)?,
        settings: &invocation.settings,
        negative_control: calibration != Calibration::None,
        calibration,
        procedure: if calibration == Calibration::None {
            invocation.settings.upgrade_procedure
        } else {
            UpgradeProcedure::InstanceRefresh
        },
    })
}

fn prepare_client(cluster: &Cluster<'_>) -> Result<OldClient> {
    let stack = cluster.stack();
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let remote = stack.administrator_remote(&credentials)?;
    let endpoint = executor(remote.bazelrc())?;
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
    journal.prepare_attempts(
        initial_run.as_ref(),
        PROBES.iter().map(|probe| probe.as_str()),
    )?;
    journal.prepare_attempts(post_run.as_ref(), [Probe::ControllerLoss0.as_str()])?;
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
    })
}

pub(crate) fn rolling_upgrade<'a>(
    invocation: &'a DeploymentInputs,
    variant: &Variant,
    issuer: &'a TestIssuer,
    negative_control: bool,
) -> Result<Vec<Check>> {
    if negative_control {
        let mut checks = run_upgrade(invocation, variant, issuer, Calibration::RemovedPort)?;
        checks.extend(run_upgrade(
            invocation,
            variant,
            issuer,
            Calibration::AliveDrain,
        )?);
        return Ok(checks);
    }
    run_upgrade(invocation, variant, issuer, Calibration::None)
}

fn run_upgrade<'a>(
    invocation: &'a DeploymentInputs,
    variant: &Variant,
    issuer: &'a TestIssuer,
    calibration: Calibration,
) -> Result<Vec<Check>> {
    let releases = select_releases(invocation, calibration)?;
    let cluster = Cluster::start_release(
        invocation,
        variant,
        issuer,
        &releases.old.binary,
        &releases.old_fixture,
    )?;
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
    continuity_observed: bool,
    verified_gap: bool,
    applied: Instant,
    removed: bool,
}

fn calibration_check(
    outcome: &MonitorOutcome,
    explanation: &str,
    releases: &Releases<'_>,
) -> Check {
    detail(
        "upgrade negative control",
        true,
        format!(
            "control={:?}, procedure={:?}, warmup actual/scaled={:?}/{:?}, drain actual/scaled={:?}/{:?}, versions={:?}->{:?}; {explanation}; cleanup cancellation is not a modeled lifecycle signal; cold mints={}, probe failures={}, probe retries={}, longest gap={:?}; build evidence unavailable after cancellation",
            releases.calibration,
            releases.procedure,
            Duration::from_secs(releases.candidate.controller.warmup_seconds),
            warmup_window(releases, releases.settings),
            Duration::from_secs(releases.candidate.controller.deregistration_delay_seconds),
            drain_window(releases, releases.settings),
            releases.old_version,
            releases.candidate_version,
            outcome.3,
            outcome.0.failures,
            outcome.2,
            outcome.1
        ),
    )
}

impl Upgrade<'_> {
    fn run(&self) -> Result<Vec<Check>> {
        let result = std::thread::scope(|scope| {
            let hold = Arc::new(Hold::new(&self.builds.go, |root| {
                self.cluster.stack().kill_bazel_in(root)
            }));
            let result = self.run_scoped(scope, &hold);
            if result.is_err() {
                hold.cancel("upgrade scenario failed");
            }
            Arc::try_unwrap(hold)
                .map_err(|_| E2eError::system("hold still shared"))?
                .disarm();
            result
        });
        self.cluster.diagnose(result)
    }

    fn original_identity(
        &self,
        attempts: &Attempts,
    ) -> Result<Option<(DirectTarget, u32, PathBuf)>> {
        if !requires_drain_credentials(self.releases.procedure, self.releases.calibration) {
            return Ok(None);
        }
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
        let removed = self.releases.old_fixture.execution.is_some()
            && self.releases.candidate.execution.is_none();
        let mut monitor = self.start_monitor(scope, hold, removed)?;
        if !monitor.first.recv().unwrap_or(false) {
            return Err(E2eError::system("pre-apply capability monitor failed"));
        }
        let credentials = self.prepare_drain_credentials()?;
        let original_held = || Ok((!initial.is_finished(), hold.is_held()?));
        let old_identity = self.original_identity(&attempts)?;
        let mut context = Handoff {
            client: &self.client,
            original_held: &original_held,
            credentials,
            old_identity,
            applied: Instant::now(),
            continuity_observed: false,
            verified_gap: false,
        };
        if context.old_identity.is_some() {
            context.observe_held(&self.cluster)?;
        }
        let applied = Instant::now();
        context.applied = applied;
        let promotion = handoff(&self.cluster, &self.releases, &mut context, &monitor, hold);
        let mut continuity_observed = context.continuity_observed;
        let verified_gap = context.verified_gap;
        let promotion = match promotion {
            Ok((next, retiring, timing, mut witness)) => {
                if self.releases.negative_control && removed {
                    monitor.wait_gap(self.cluster.stack().settings(), applied);
                    if monitor.failed() {
                        let _ = hold.release();
                        hold.cancel("old executor availability gap exceeded");
                        drop(retiring);
                        let _ = joined(initial);
                        let outcome = monitor.finish()?;
                        return Ok(vec![calibration_check(
                            &outcome,
                            "removed executor",
                            &self.releases,
                        )]);
                    }
                }
                continuity_observed = false;
                self.cluster.finish_retire(retiring).and_then(|status| {
                    post_exit_refresh(&self.cluster, &self.releases, &mut witness)?;
                    Ok((next, status, timing, witness))
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
            continuity_observed,
            verified_gap,
            applied,
            removed,
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
        let (removed, applied) = (observation.removed, observation.applied);
        let promotion = match observation.handoff {
            Ok((index, status, timing, drain)) => Promotion {
                index,
                status,
                timing,
                drain,
            },
            Err(error) => {
                return self.handoff_failure(
                    error,
                    monitor,
                    observation.verified_gap,
                    removed,
                    observation.continuity_observed,
                );
            }
        };
        if self.releases.negative_control && monitor.failed() {
            let outcome = monitor.finish()?;
            return Ok(vec![calibration_check(
                &outcome,
                "removed executor",
                &self.releases,
            )]);
        }
        let (initial, promotion) =
            match self.finish_initial(observation.build, observation.attempts, promotion) {
                Ok(complete) => complete,
                Err(error) => return self.initial_build_failure(error, monitor, removed, applied),
            };
        let post = self.post_build(scope, hold);
        if self.releases.negative_control && removed {
            monitor.wait_gap(self.cluster.stack().settings(), applied);
        }
        let outcome = monitor.finish()?;
        if self.releases.negative_control && monitor.failed() {
            return Ok(vec![calibration_check(
                &outcome,
                "removed executor",
                &self.releases,
            )]);
        }
        self.report(&initial, &promotion, &post?, &outcome)
    }

    fn prepare_drain_credentials(&self) -> Result<Option<DrainCredentials>> {
        if !requires_drain_credentials(self.releases.procedure, self.releases.calibration) {
            return Ok(None);
        }
        let stack = self.cluster.stack();
        DrainCredentials::prepare(
            stack.gang_cli(),
            &self.client.endpoint,
            &stack.placement().root().join("drain-credentials"),
            stack.settings().upgrade_gap_limit,
        )
        .map(Some)
    }

    fn start_monitor<'scope, 'env, K>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        hold: &Arc<Hold<'env, K>>,
        removed: bool,
    ) -> Result<RunningMonitor<'scope>>
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()> + Send + Sync + 'env,
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
                hold: Arc::clone(hold),
                calibration: if self.releases.calibration == Calibration::RemovedPort && !removed {
                    Calibration::None
                } else {
                    self.releases.calibration
                },
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
            },
            promotion,
        ))
    }

    fn handoff_failure(
        &self,
        error: E2eError,
        monitor: &mut RunningMonitor<'_>,
        verified_gap: bool,
        removed: bool,
        continuity_observed: bool,
    ) -> Result<Vec<Check>> {
        let outcome = monitor.finish()?;
        let continuity = if verified_gap && monitor.failed() {
            match self.releases.calibration {
                Calibration::RemovedPort if removed => Continuity::RemovedPortGap,
                Calibration::AliveDrain => Continuity::AliveDrainGap,
                _ => Continuity::None,
            }
        } else {
            Continuity::None
        };
        let accepted = calibrated(true, continuity);
        if self.releases.negative_control && accepted {
            return Ok(vec![calibration_check(
                &outcome,
                &format!("handoff: {error}"),
                &self.releases,
            )]);
        }
        if !self.releases.negative_control
            && continuity_observed
            && matches!(error, E2eError::Timeout { .. })
        {
            return Ok(vec![detail(
                "rolling upgrade",
                false,
                format!(
                    "procedure={:?}; promotion: {error}; cold mints={}, probe failures={}, retries={}, max gap={:?}; build evidence unavailable after cancellation",
                    self.releases.procedure, outcome.3, outcome.0.failures, outcome.2, outcome.1
                ),
            )]);
        }
        Err(error)
    }

    fn initial_build_failure(
        &self,
        error: E2eError,
        monitor: &mut RunningMonitor<'_>,
        removed: bool,
        applied: Instant,
    ) -> Result<Vec<Check>> {
        if self.releases.negative_control && removed {
            monitor.wait_gap(self.cluster.stack().settings(), applied);
        }
        let outcome = monitor.finish()?;
        if self.releases.negative_control && monitor.failed() {
            return Ok(vec![calibration_check(
                &outcome,
                &format!("initial build evidence unavailable: {error}"),
                &self.releases,
            )]);
        }
        Err(error)
    }

    fn post_build<'scope, 'env, K>(
        &'env self,
        scope: &'scope Scope<'scope, 'env>,
        hold: &Hold<'env, K>,
    ) -> Result<ObservedBuild>
    where
        'env: 'scope,
        K: Fn(Option<&str>) -> Result<()>,
    {
        let mut attempts = Attempts::new();
        let post = hold.build(scope, None, |_, stop| {
            let result = self.builds.post.driver.build_until(
                &self.client.remote,
                &[Probe::ControllerLoss0],
                Some(&self.builds.post.run),
                stop,
            );
            self.builds.journal.write_line(JOURNAL_END)?;
            result
        });
        let settled = hold.settle(Ok(()), || {
            drain(&self.builds.journal, &self.builds.post.run, &mut attempts)
        });
        let result = joined(post);
        settled?;
        Ok(ObservedBuild {
            outcome: result?,
            evidence: BuildEvidence::read(&self.builds.post.bep, &self.builds.post.rpc)?,
            attempts,
        })
    }

    fn report(
        &self,
        initial: &ObservedBuild,
        promotion: &Promotion,
        post: &ObservedBuild,
        monitor: &MonitorOutcome,
    ) -> Result<Vec<Check>> {
        let stack = self.cluster.stack();
        let leader = self.cluster.controller_is_leader(promotion.index)?;
        Ok(final_verdict(&VerdictInputs {
            initial_outcome: &initial.outcome,
            initial_evidence: &initial.evidence,
            initial_attempts: &initial.attempts,
            post_outcome: &post.outcome,
            post_evidence: &post.evidence,
            post_attempts: &post.attempts,
            availability: &monitor.0,
            gap: monitor.1,
            retries: monitor.2,
            cold_mints: monitor.3,
            timing: promotion.timing,
            procedure: self.releases.procedure,
            warmup_policy: (
                Duration::from_secs(self.releases.candidate.controller.warmup_seconds),
                warmup_window(&self.releases, stack.settings()),
            ),
            drain_policy: (
                Duration::from_secs(
                    self.releases
                        .candidate
                        .controller
                        .deregistration_delay_seconds,
                ),
                drain_window(&self.releases, stack.settings()),
            ),
            drain: &promotion.drain,
            status: promotion.status,
            leader,
            gap_limit: stack.settings().upgrade_gap_limit,
            old_version: &self.releases.old_version,
            candidate_version: &self.releases.candidate_version,
            elapsed: self.started.elapsed(),
            negative_control: self.releases.negative_control,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade_credentials::SharedCredentialWitness;

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

    fn verdict_attempts() -> (Attempts, Attempts) {
        let mut initial_attempts = Attempts::new();
        for (index, probe) in PROBES.iter().enumerate() {
            let pid = index + 100;
            let action = probe.as_str();
            record_attempt(
                &format!("run loss start {action} 42 {pid} 10 exclusive"),
                "run",
                &mut initial_attempts,
            )
            .expect("start");
            record_attempt(
                &format!("run loss end {action} 42 {pid} 20"),
                "run",
                &mut initial_attempts,
            )
            .expect("end");
        }
        let mut post_attempts = Attempts::new();
        let action = Probe::ControllerLoss0.as_str();
        record_attempt(
            &format!("post loss start {action} 42 200 30 exclusive"),
            "post",
            &mut post_attempts,
        )
        .expect("start");
        record_attempt(
            &format!("post loss end {action} 42 200 40"),
            "post",
            &mut post_attempts,
        )
        .expect("end");
        (initial_attempts, post_attempts)
    }

    #[test]
    fn final_verdict_requires_exactly_two_cold_mints() {
        use std::os::unix::process::ExitStatusExt;

        let initial_outcome = BuildOutcome::new(Some(0), "INFO: 8 processes: 8 remote.".to_owned());
        let post_outcome = BuildOutcome::new(Some(0), "INFO: 1 process: 1 remote.".to_owned());
        let initial_evidence = BuildEvidence {
            successful_actions: 8,
            finished_success: true,
            rpc_calls: 1,
            ..BuildEvidence::default()
        };
        let post_evidence = BuildEvidence {
            successful_actions: 1,
            finished_success: true,
            rpc_calls: 1,
            ..BuildEvidence::default()
        };
        let (initial_attempts, post_attempts) = verdict_attempts();
        let availability = Availability::default();
        let drain = DrainWitness::default();
        let mut input = VerdictInputs {
            initial_outcome: &initial_outcome,
            initial_evidence: &initial_evidence,
            initial_attempts: &initial_attempts,
            post_outcome: &post_outcome,
            post_evidence: &post_evidence,
            post_attempts: &post_attempts,
            availability: &availability,
            gap: Duration::ZERO,
            retries: 0,
            cold_mints: 2,
            timing: PromotionTiming {
                lease: Duration::ZERO,
                client: Duration::ZERO,
            },
            procedure: UpgradeProcedure::PreStop,
            warmup_policy: (Duration::ZERO, Duration::ZERO),
            drain_policy: (Duration::ZERO, Duration::ZERO),
            drain: &drain,
            status: std::process::ExitStatus::from_raw(0),
            leader: true,
            gap_limit: Duration::from_secs(4),
            old_version: "0.3.2",
            candidate_version: "candidate",
            elapsed: Duration::ZERO,
            negative_control: false,
        };
        input.procedure = UpgradeProcedure::InstanceRefresh;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        let held_drain = credential_drain();
        input.warmup_policy = (Duration::from_secs(21), Duration::from_secs(21));
        input.drain_policy = (Duration::from_mins(5), Duration::from_secs(8));
        input.drain = &held_drain;
        assert!(final_verdict(&input).iter().all(Check::is_passed));
        let mut shortened_drain = credential_drain();
        shortened_drain.elapsed = Duration::from_secs(7);
        input.drain = &shortened_drain;
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.drain = &held_drain;
        input.timing.lease = Duration::from_secs(5);
        assert!(final_verdict(&input).iter().any(Check::is_failure));
        input.timing.lease = Duration::ZERO;
        input.drain = &drain;
        input.procedure = UpgradeProcedure::PreStop;
        assert!(final_verdict(&input).iter().all(Check::is_passed));
        for count in [0, 1, 3] {
            input.cold_mints = count;
            assert!(final_verdict(&input).iter().any(Check::is_failure));
        }
    }

    #[test]
    fn preflight_failure_never_calibrates() {
        assert!(!calibrated(false, Continuity::RemovedPortGap));
        assert!(!calibrated(false, Continuity::AliveDrainGap));
    }

    #[test]
    fn historical_continuity_failure_calibrates() {
        assert!(calibrated(true, Continuity::RemovedPortGap));
        assert!(calibrated(true, Continuity::AliveDrainGap));
        assert!(!calibrated(true, Continuity::None));
    }

    #[test]
    fn drain_calibration_requires_an_applied_during_drain_sample() {
        assert!(!calibration_sample_eligible(
            Calibration::AliveDrain,
            false,
            true
        ));
        assert!(!calibration_sample_eligible(
            Calibration::AliveDrain,
            true,
            false
        ));
        assert!(calibration_sample_eligible(
            Calibration::AliveDrain,
            true,
            true
        ));
        assert!(calibration_sample_eligible(
            Calibration::RemovedPort,
            true,
            false
        ));
        assert!(!calibration_sample_eligible(Calibration::None, true, true));
    }

    #[test]
    fn scaled_windows_preserve_zero_and_never_exceed_actual_policy() {
        let cap = Duration::from_secs(8);
        assert_eq!(scaled_window(0, cap), Duration::ZERO);
        assert_eq!(scaled_window(3, cap), Duration::from_secs(3));
        assert_eq!(scaled_window(300, cap), cap);
    }

    #[test]
    fn warmup_guard_rejects_only_absence_of_both_old_leader_and_new_health() {
        assert!(warmup_guard(true, false).is_ok());
        assert!(warmup_guard(true, true).is_ok());
        assert!(warmup_guard(false, true).is_ok());
        assert!(warmup_guard(false, false).is_err());
    }

    #[test]
    fn procedures_preserve_real_termination_order() {
        assert_eq!(
            termination_steps(UpgradeProcedure::PreStop),
            &[TerminationStep::Stop]
        );
        assert_eq!(
            post_exit_refresh_steps(UpgradeProcedure::PreStop),
            &[TerminationStep::Deregister, TerminationStep::Drain]
        );
        assert!(post_exit_refresh_steps(UpgradeProcedure::InstanceRefresh).is_empty());
        assert_eq!(
            termination_steps(UpgradeProcedure::InstanceRefresh),
            &[
                TerminationStep::Deregister,
                TerminationStep::Drain,
                TerminationStep::Stop
            ]
        );
    }

    fn credential_drain() -> DrainWitness {
        let state = HeldState {
            build_unfinished: true,
            hold_unreleased: true,
            old_live: true,
        };
        DrainWitness {
            elapsed: Duration::from_secs(8),
            credentials: Some(HeldDrainWitness {
                drain_started: Duration::from_millis(90),
                started: Duration::from_millis(100),
                finished: Duration::from_millis(111),
                deadline: Duration::from_millis(8090),
                direct: DrainCredentialWitness {
                    shared_started: Duration::from_millis(1),
                    shared_finished: Duration::from_millis(2),
                    shared: SharedCredentialWitness {
                        status: 200,
                        marker_absent: true,
                        token_valid: true,
                    },
                    shared_verification_started: Duration::from_millis(3),
                    shared_verification_finished: Duration::from_millis(4),
                    mint_started: Duration::from_millis(7),
                    mint_finished: Duration::from_millis(8),
                    verification_started: Duration::from_millis(9),
                    verification_finished: Duration::from_millis(10),
                    token_valid: true,
                },
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
        assert!(requires_drain_credentials(
            UpgradeProcedure::InstanceRefresh,
            Calibration::None
        ));
        for calibration in [Calibration::RemovedPort, Calibration::AliveDrain] {
            assert!(!requires_drain_credentials(
                UpgradeProcedure::InstanceRefresh,
                calibration
            ));
        }
        assert!(!requires_drain_credentials(
            UpgradeProcedure::PreStop,
            Calibration::None
        ));
        let mut drain = credential_drain();
        let gap = Duration::from_secs(4);
        assert!(drain_credentials_ok(&drain, true, gap));
        drain.credentials = None;
        assert!(!drain_credentials_ok(&drain, true, gap));
        assert!(drain_credentials_ok(&drain, false, gap));
    }

    #[test]
    fn held_drain_rejects_completed_released_cancelled_or_dead_observations() {
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
    fn held_drain_rejects_wrong_shared_or_token_evidence() {
        let mutations: [fn(&mut HeldDrainWitness); 5] = [
            |w| w.direct.shared.status = 503,
            |w| w.direct.shared.status = 0,
            |w| w.direct.shared.marker_absent = false,
            |w| w.direct.shared.token_valid = false,
            |w| w.direct.token_valid = false,
        ];
        for mutate in mutations {
            let mut drain = credential_drain();
            mutate(drain.credentials.as_mut().expect("credential fixture"));
            assert!(!drain_credentials_ok(&drain, true, Duration::from_secs(4)));
        }
    }

    #[test]
    fn held_drain_rejects_deadlines_reordered_work_and_post_stop_witnesses() {
        let gap = Duration::from_secs(4);
        let mut drain = credential_drain();
        drain.stop_started = None;
        assert!(!drain_credentials_ok(&drain, true, gap));
        drain.stop_started = Some(Duration::from_millis(99));
        assert!(!drain_credentials_ok(&drain, true, gap));
        drain.stop_started = Some(Duration::from_millis(111));
        assert!(!drain_credentials_ok(&drain, true, gap));
        let mutations: [fn(&mut HeldDrainWitness); 15] = [
            |w| w.deadline = Duration::ZERO,
            |w| w.finished = Duration::from_millis(105),
            |w| w.started = w.finished,
            |w| w.direct.mint_finished += Duration::from_secs(5),
            |w| w.direct.shared_started = Duration::from_millis(3),
            |w| w.direct.shared_started = w.direct.shared_finished,
            |w| w.direct.shared_verification_started = Duration::from_millis(1),
            |w| w.direct.shared_verification_started = w.direct.shared_verification_finished,
            |w| w.direct.mint_started = Duration::from_millis(3),
            |w| w.direct.mint_started = w.direct.mint_finished,
            |w| w.direct.verification_started = Duration::from_millis(7),
            |w| w.direct.verification_started = w.direct.verification_finished,
            |w| w.direct.verification_finished = Duration::MAX,
            |w| w.deadline = w.finished.saturating_sub(Duration::from_nanos(1)),
            |w| w.direct.mint_finished = w.direct.verification_started + Duration::from_nanos(1),
        ];
        for mutate in mutations {
            let mut drain = credential_drain();
            mutate(drain.credentials.as_mut().expect("credential fixture"));
            assert!(!drain_credentials_ok(&drain, true, gap));
        }
        assert!(remaining_budget(Instant::now(), Duration::ZERO).is_err());
    }

    #[test]
    fn held_drain_rejects_slow_mint_even_when_total_drain_deadline_is_met() {
        let mut drain = credential_drain();
        let witness = drain.credentials.as_mut().expect("credential fixture");
        witness.direct.mint_finished = Duration::from_secs(5);
        witness.direct.verification_started = Duration::from_millis(5001);
        witness.direct.verification_finished = Duration::from_secs(6);
        witness.finished = Duration::from_secs(7);
        witness.deadline = Duration::from_secs(8);
        drain.stop_started = Some(Duration::from_secs(8));
        assert!(!drain_credentials_ok(&drain, true, Duration::from_secs(4)));
        assert!(drain_credentials_ok(&drain, true, Duration::from_secs(5)));
    }

    #[test]
    fn held_drain_requires_one_absolute_budget_and_the_full_modeled_window() {
        let warmup = Duration::from_secs(21);
        let window = Duration::from_secs(8);
        let mut drain = credential_drain();
        assert!(drain_policy_ok(&drain, warmup, window));
        assert!(!drain_policy_ok(&drain, Duration::ZERO, window));
        assert!(!drain_policy_ok(&drain, warmup, Duration::ZERO));
        assert!(!drain_policy_ok(&drain, Duration::from_secs(4), window));
        drain.elapsed = window.saturating_sub(Duration::from_nanos(1));
        assert!(!drain_policy_ok(&drain, warmup, window));
        let mutations: [fn(&mut HeldDrainWitness); 3] = [
            |witness| witness.deadline += Duration::from_nanos(1),
            |witness| witness.drain_started = witness.started + Duration::from_nanos(1),
            |witness| witness.drain_started = Duration::MAX,
        ];
        for mutate in mutations {
            let mut drain = credential_drain();
            mutate(drain.credentials.as_mut().expect("witness"));
            assert!(!drain_policy_ok(&drain, warmup, window));
        }
        let mut drain = credential_drain();
        let witness = drain.credentials.as_ref().expect("witness");
        drain.stop_started = Some(
            (witness.drain_started + window)
                .checked_sub(Duration::from_nanos(1))
                .expect("positive modeled window in fixture"),
        );
        assert!(!drain_policy_ok(&drain, warmup, window));
    }
}
