//! The suite: the scenarios of one gate, each run against a fresh deployment, and their verdicts.

use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use gang_config::InputLinks;
use parking_lot::Mutex;

use crate::access::{IdentityKnobs, SignIn, identity};
use crate::admission::{cold_start_misfit, memory_escalation, oversized_fleet, unplaceable_disk};
use crate::browser::sessions;
use crate::builds::builds;
use crate::check::{Check, Report, prefix_checks};
use crate::checked_in::{ShippedConfigs, checked_in_configs};
use crate::cli::cli;
use crate::cli_terminal::LocalTerminal;
use crate::cluster::Engine;
use crate::control::ControlKnobs;
use crate::cross_revocation::{CROSS_REVOCATION_SUBJECT, cross_revocation};
use crate::dashboard::{dashboard, running_metadata};
use crate::disk::disk;
use crate::drain_cancel::drain_cancel;
use crate::errors::Result;
use crate::execution::ExecutionKnobs;
use crate::fhs_tools::fhs_tools;
use crate::fleets::{run_cold_pair, run_drained_starts};
use crate::glibc_toolchain::glibc_toolchain;
use crate::input_root::input_root;
use crate::integrity::input_integrity;
use crate::issuer::TestIssuer;
use crate::launch_failure::launch_failure;
use crate::lifecycle::lifecycle;
use crate::limits::limits;
use crate::memory::memory;
use crate::names;
use crate::network::network;
use crate::observer::Observer;
use crate::orphan::orphan;
use crate::plaintext::plaintext;
use crate::preemption::{preemption, worker_renewal};
use crate::restart::restart;
use crate::revocation::revocation;
use crate::sandbox_contract::sandbox_contract;
use crate::scheduled_minimum::scheduled_minimum;
use crate::secured::Secured;
use crate::server::ServerKnobs;
use crate::server_scenarios::{
    api, certificate_rotation, event_paging, request_limits, viewer_cap,
};
use crate::settings::DeploymentInputs;
use crate::setup::setup;
use crate::skipped_pass::skipped_pass;
use crate::stack::Stack;
use crate::stale_scope::stale_scope;
use crate::system::{ActionLimits, FleetLayout, Sandbox, SoftMemory, System, Variant};
use crate::template::Transport;
use crate::timeout::{action_timeout, exit_signals};
use crate::work_aware::{backlog_grows_fleet, burst_from_zero, queued_behind_short_work};

/// The configuration of one harness run.
#[derive(Debug, Clone)]
pub struct Invocation {
    /// The resolved inputs for every deployment in this run.
    pub inputs: DeploymentInputs,
    /// The scenarios to run; empty selects the gate's default scenarios
    /// ([`Gate::scenarios`]). A selected scenario that the gate cannot select is ignored;
    /// [`unknown_scenarios`] finds those for a caller to reject.
    pub scenarios: Vec<Scenario>,
}

/// Which scenarios a run covers. Later gates include the scenarios of earlier ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gate {
    /// The deployment scenarios.
    One,
    /// Gate one plus the scenarios whose first gate is two.
    Two,
}

impl Gate {
    /// The scenarios this gate runs by default, in the order a run reaches them. [`run`] derives
    /// its selection from this list.
    pub fn scenarios(self) -> impl Iterator<Item = Scenario> {
        Scenario::ALL
            .iter()
            .copied()
            .filter(move |scenario| scenario.gate() <= self)
    }

    /// The scenarios this gate adds to the earlier gates, in the order a run reaches them.
    pub fn additions(self) -> impl Iterator<Item = Scenario> {
        Scenario::ALL
            .iter()
            .copied()
            .filter(move |scenario| scenario.gate() == self)
    }
}

impl FromStr for Gate {
    type Err = UnknownGate;

    /// Parses `1` or `one` into [`Gate::One`], and `2` or `two` into [`Gate::Two`].
    ///
    /// # Errors
    ///
    /// [`UnknownGate`] for any other text.
    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        match text {
            "1" => Ok(Self::One),
            "2" => Ok(Self::Two),
            other => Err(UnknownGate(other.to_owned())),
        }
    }
}

/// Text that names no gate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown gate `{0}`; expected 1 or 2")]
pub struct UnknownGate(pub String);

gang_types::string_enum! {
    /// One scenario of the suite, selectable by the name [`as_str`](Scenario::as_str) returns.
    pub enum Scenario("scenario") {
        /// A fleet starts empty, scales out for a cold build, drains, and serves a warm rebuild from
        /// the cache.
        Lifecycle = "lifecycle",
        /// An ungranted network request is rejected before execution.
        Network = "network",
        /// An action that modifies a cached input does not change what a later action sees, unless
        /// the workspace keeps its input links.
        InputIntegrity = "input-integrity",
        /// The server is killed during a scale-out and started again; the build survives.
        Restart = "restart",
        /// Per-action cgroup limits apply, and are cleaned up once an action ends.
        Limits = "limits",
        /// An action that outgrows its soft memory reservation is killed by `NativeLink` and
        /// requeued with a doubled reservation, on which it completes, and the worker keeps serving.
        Memory = "memory",
        /// An action that writes past its disk reservation is killed by its worker, and a
        /// neighbour that shares the worker still finishes.
        Disk = "disk",
        /// A deployment with three fleets places each action on the fleet its exec properties
        /// select, and overflows to the next fleet when the first is full.
        Fleets = "fleets",
        /// An action that outlives `action_timeout` under bwrap fails, gets SIGTERM before
        /// SIGKILL, and leaves nothing behind.
        ActionTimeout = "action-timeout",
        /// The same under the noop sandbox, plus the exit status of actions killed by a signal.
        ActionTimeoutNoop = "action-timeout-noop",
        /// A bwrap action sees no worker environment, a fresh /tmp and read-only binds, runs in
        /// its own scope, and an out-of-memory kill is cleaned up.
        SandboxContract = "sandbox-contract",
        /// An action whose command runs below its input root reads an input beside its working
        /// directory, directly and through a relative symlink.
        InputRoot = "input-root",
        /// A dynamically linked glibc program, as the toolchains Bazel downloads are, runs inside the
        /// bwrap sandbox.
        GlibcToolchain = "glibc-toolchain",
        /// A `#!/usr/bin/env bash` script runs inside the bwrap sandbox with Bazel's strict action
        /// `PATH`.
        FhsTools = "fhs-tools",
        /// The scope and stray process of a SIGKILLed `gang exec` are swept by the next action,
        /// and a dead worker's are gone before its replacement serves.
        StaleScope = "stale-scope",
        /// An action still over its memory after the allowed escalations fails and is not
        /// requeued again.
        MemoryEscalation = "memory-escalation",
        /// An action whose disk no fleet accepts fails at once.
        UnplaceableDisk = "unplaceable-disk",
        /// A worker whose scratch cannot hold its fleet's disk refuses to enroll.
        OversizedFleet = "oversized-fleet",
        /// A cold start no fleet accepts stops `gang server` at startup.
        ColdStartMisfit = "cold-start-misfit",
        /// A preempted spot worker stops without draining and its action is requeued.
        Preemption = "preemption",
        /// A worker renews its certificate mid-action and keeps serving.
        WorkerRenewal = "worker-renewal",
        /// An instance whose worker stops answering is flagged past its boot grace, then
        /// terminated once as an orphan.
        Orphan = "orphan",
        /// An action queued while the idle worker drains cancels the drain; a server restarted
        /// during the next drain terminates that instance once.
        DrainCancel = "drain-cancel",
        /// A pass whose provisioner supply read fails is skipped and counted; a good pass clears
        /// the count.
        SkippedPass = "skipped-pass",
        /// The `gang` command line without a harness stack: exit codes and failure records,
        /// `--quiet` and the colour switches, `gang local up` / `down` with and without TLS,
        /// `gang doctor` against a fake Bazel, and the endpoint flags of `gang worker`.
        Cli = "cli",
        /// Every checked-in `configs/*.toml` starts `gang server` once moved into a placement;
        /// the AWS one passes configuration loading.
        CheckedInConfigs = "checked-in-configs",
        /// A deployment without TLS serves builds over the plaintext transport.
        Plaintext = "plaintext",
        /// TLS and authenticated `gang setup` resolves a home rc conflict and builds remotely.
        Setup = "setup",
        /// A revoked certificate's connection is closed and new ones are refused. Gate two only.
        Revocation = "revocation",
        /// A session cookie opens the live state stream, requests without one are refused, and
        /// revoking the principal closes the open stream. Gate two only.
        Dashboard = "dashboard",
        /// A failed worker launch is reported, then withdrawn when its demand is abandoned.
        LaunchFailure = "launch_failure",
        /// The API contracts of one deployment: authentication challenges, unknown routes,
        /// paging, the error envelope, dashboard assets, cookie-only writes, cross-origin logout,
        /// and a graceful shutdown. Gate two only.
        Api = "api",
        /// The request size limit and rate limits of open routes, including dashboard sign-in.
        /// Gate two only.
        RequestLimits = "request-limits",
        /// A viewer over the dashboard's viewer limit is turned away with a retry delay. Gate two
        /// only.
        ViewerCap = "viewer-cap",
        /// The listener certificate is reissued before it expires, and established connections
        /// survive it. Gate two only.
        CertificateRotation = "certificate-rotation",
        /// Who may administer, how the client logs in and renews, which credentials are refused,
        /// how the dashboard signs a browser in, how a revoked worker certificate is cut off, and
        /// that a revocation survives a restart. Gate two only.
        Identity = "identity",
        /// A signed-in browser renews silently until the absolute lifetime, then signs in without
        /// consent; revocation, logout and a suspended account end renewal. Gate two only.
        Sessions = "sessions",
        /// A build survives loss of either the leader or a follower.
        ControllerLoss = "controller-loss",
        /// Controllers are replaced while a build is in progress.
        RollingDeploy = "rolling-deploy",
        /// Previous-release and candidate-to-candidate ordinary refreshes while work is held.
        RollingUpgrade = "rolling-upgrade",
        /// A previous release is replaced by instance refresh while work is held.
        RollingUpgradeAutomatic = "rolling-upgrade-automatic",
        /// Published old controllers and workers refetch preserved configuration after publication.
        ConfigRefetchRestart = "config-refetch-restart",
        /// Revocations propagate between controllers.
        CrossRevocation = "cross-revocation",
        /// Queued demand survives loss or suspension of the leader.
        LeaderFailover = "leader-failover",
        /// Device and browser logins continue across controllers.
        LoginAcross = "login-across",
        /// Builds and scaling recover from a database outage.
        DatabaseOutage = "database-outage",
        /// Two authenticated builds, cold then warm, are attributed separately with their cache
        /// and execution usage and timestamps visible to the administrator. Gate two only.
        Builds = "builds",
        /// Followers serve shared requests, refuse leader-only requests, and builds use the leader.
        FollowerBuilds = "follower-builds",
        /// A kernel action-pool OOM is classified and requeued without a generic retry.
        PoolOom = "pool-oom",
        /// Sustained pool pressure kills and requeues only the furthest-over-reservation action.
        Pressure = "pressure",
        /// A fleet with `min_instances = 0` keeps one worker with no build while a scheduled
        /// minimum is in force, and the scale-out carries cause `MINIMUM`. Gate two only.
        ScheduledMinimum = "scheduled-minimum",
        /// A burst from zero does not buy more than one worker.
        BurstFromZero = "burst-from-zero",
        /// Queued work waits for short running work rather than buying capacity.
        QueuedBehindShortWork = "queued-behind-short-work",
        /// A persistent backlog grows the fleet.
        BacklogGrowsFleet = "backlog-grows-fleet",
        /// The burst's one-worker bound without any training build.
        ColdBurstFromZero = "cold-burst-from-zero",
        /// One authenticated workload continuously refills past its payback horizon.
        SustainedStream = "sustained-stream",
        /// Distinct principals retain separate activity ages and offered load.
        TwoSources = "two-sources",
        /// Distributed busy workers cordon and drain for measured utilization.
        UtilizationScaleIn = "utilization-scale-in",
    }
}

impl Scenario {
    /// Seconds for LPT balancing, rounded up (minimum one) from warm x86 CI timings.
    /// Provisional estimates after fixture changes: sessions 60s (was 181.1s), fleets 75s
    /// (169.4s), leader failover 75s (120.4s), and database outage 100s (164.5s).
    /// Cold burst 20s and rate scenarios 65/95/110s are provisional fixture estimates,
    /// not measured improvements; the eight-shard launcher-budget test uses these too.
    #[must_use]
    pub const fn weight(self) -> u64 {
        match self {
            Self::Lifecycle | Self::StaleScope => 56,
            Self::Network | Self::Restart => 24,
            Self::InputIntegrity => 33,
            Self::Limits => 28,
            Self::SandboxContract | Self::QueuedBehindShortWork => 32,
            Self::Memory | Self::Revocation | Self::Builds => 23,
            Self::UnplaceableDisk | Self::InputRoot => 27,
            Self::Disk => 57,
            Self::Fleets | Self::LeaderFailover => 75,
            Self::ActionTimeout => 49,
            Self::ActionTimeoutNoop => 43,
            Self::WorkerRenewal | Self::LaunchFailure => 34,
            Self::GlibcToolchain => 26,
            Self::FhsTools | Self::ColdBurstFromZero => 20,
            Self::MemoryEscalation => 13,
            Self::OversizedFleet => 12,
            Self::Pressure | Self::PoolOom => 16,
            Self::ColdStartMisfit | Self::CheckedInConfigs | Self::Api => 1,
            Self::Preemption => 52,
            Self::Orphan => 29,
            Self::DrainCancel => 126,
            Self::SkippedPass | Self::RequestLimits => 6,
            Self::Cli => 3,
            Self::Plaintext => 54,
            Self::Setup => 88,
            Self::Dashboard => 18,
            Self::CrossRevocation => 22,
            Self::ViewerCap => 2,
            Self::CertificateRotation => 14,
            Self::Identity => 80,
            Self::Sessions => 60,
            Self::ControllerLoss => 84,
            Self::RollingDeploy
            | Self::BurstFromZero
            | Self::RollingUpgrade
            | Self::RollingUpgradeAutomatic => 62,
            Self::FollowerBuilds => 66,
            Self::LoginAcross => 15,
            Self::ScheduledMinimum => 5,
            Self::BacklogGrowsFleet => 10,
            Self::DatabaseOutage => 100,
            Self::ConfigRefetchRestart => 120,
            Self::SustainedStream => 65,
            Self::TwoSources => 95,
            Self::UtilizationScaleIn => 110,
        }
    }

    /// The first gate whose default list contains this scenario.
    #[must_use]
    pub const fn gate(self) -> Gate {
        match self {
            Self::Lifecycle
            | Self::Network
            | Self::InputIntegrity
            | Self::Restart
            | Self::Limits
            | Self::Memory
            | Self::Disk
            | Self::Fleets
            | Self::ActionTimeout
            | Self::ActionTimeoutNoop
            | Self::SandboxContract
            | Self::InputRoot
            | Self::GlibcToolchain
            | Self::FhsTools
            | Self::StaleScope
            | Self::MemoryEscalation
            | Self::UnplaceableDisk
            | Self::OversizedFleet
            | Self::ColdStartMisfit
            | Self::Preemption
            | Self::WorkerRenewal
            | Self::Orphan
            | Self::DrainCancel
            | Self::SkippedPass
            | Self::Cli
            | Self::CheckedInConfigs
            | Self::Plaintext
            | Self::Setup => Gate::One,
            Self::Revocation
            | Self::Dashboard
            | Self::LaunchFailure
            | Self::Api
            | Self::RequestLimits
            | Self::ViewerCap
            | Self::CertificateRotation
            | Self::Identity
            | Self::Builds
            | Self::FollowerBuilds
            | Self::Sessions
            | Self::ScheduledMinimum
            | Self::BurstFromZero
            | Self::QueuedBehindShortWork
            | Self::BacklogGrowsFleet
            | Self::ColdBurstFromZero
            | Self::SustainedStream
            | Self::TwoSources
            | Self::UtilizationScaleIn
            | Self::ControllerLoss
            | Self::RollingDeploy
            | Self::RollingUpgrade
            | Self::RollingUpgradeAutomatic
            | Self::ConfigRefetchRestart
            | Self::CrossRevocation
            | Self::LeaderFailover
            | Self::LoginAcross
            | Self::DatabaseOutage
            | Self::Pressure
            | Self::PoolOom => Gate::Two,
        }
    }
}

/// Runs the scenarios of `gate` that `invocation` selects, up to
/// `Settings::parallel_scenarios` at once, and returns their verdicts in the order of
/// [`Scenario::ALL`]. Each finished scenario prints one progress line to stderr with its check
/// count and wall time. A scenario that cannot run to completion contributes one failed check;
/// the run never stops early.
pub fn run(invocation: &Invocation, gate: Gate) -> Report {
    let mut report = Report::default();
    let issuer = match TestIssuer::start(&invocation.inputs.settings) {
        Ok(issuer) => issuer,
        Err(error) => {
            report.extend([Check::failed(
                names::suite::ISSUER_STARTS,
                gang_types::error_chain(&error).to_string(),
            )]);
            return report;
        }
    };
    let sandbox = Sandbox::native();
    let ctx = RunContext {
        inputs: &invocation.inputs,
        issuer: &issuer,
        sandbox,
        baseline: Variant::baseline(sandbox),
    };
    let selected: Vec<Scenario> = Scenario::ALL
        .iter()
        .copied()
        .filter(|scenario| covers(gate, &invocation.scenarios, *scenario))
        .collect();
    let verdicts = run_bounded(
        &selected,
        invocation.inputs.settings.parallel_scenarios,
        |scenario| {
            let started = Instant::now();
            let checks = crate::stack::with_failure_evidence(|| scenario.run(&ctx));
            let failed = checks.iter().filter(|check| check.is_failure()).count();
            eprintln!(
                "scenario {scenario}: {} checks, {failed} failed, {:.1}s",
                checks.len(),
                started.elapsed().as_secs_f64()
            );
            checks
        },
    );
    for (scenario, checks) in selected.iter().zip(verdicts) {
        report.extend(checks.unwrap_or_else(|| {
            vec![Check::failed(
                names::suite::scenario_completes(scenario),
                "the scenario's thread panicked",
            )]
        }));
    }
    report
}

/// What a scenario run starts from: the inputs, the in-process issuer, the sandbox, and the
/// baseline variant that the scenarios derive their variants from.
struct RunContext<'a> {
    inputs: &'a DeploymentInputs,
    issuer: &'a TestIssuer,
    sandbox: Sandbox,
    baseline: Variant,
}

impl Scenario {
    /// The verdicts of this scenario on fresh deployments. This match is the only mapping from a
    /// scenario to its behaviour.
    fn run(self, ctx: &RunContext<'_>) -> Vec<Check> {
        match self {
            Self::Lifecycle => lifecycle_checks(ctx),
            Self::Network => network_checks(ctx),
            Self::InputIntegrity => input_integrity_checks(ctx),
            Self::InputRoot => input_root_checks(ctx),
            Self::Restart => restart_checks(ctx),
            Self::RollingUpgrade | Self::RollingUpgradeAutomatic => upgrade_checks(self, ctx),
            Self::ConfigRefetchRestart => config_restart_checks(ctx),
            Self::ControllerLoss
            | Self::RollingDeploy
            | Self::CrossRevocation
            | Self::LeaderFailover
            | Self::LoginAcross
            | Self::FollowerBuilds
            | Self::DatabaseOutage => cluster_scenario_checks(self, ctx),
            Self::Limits => limits_checks(ctx),
            Self::Memory => memory_checks(ctx),
            Self::Pressure => pressure_checks(ctx),
            Self::PoolOom => pool_oom_checks(ctx),
            Self::Disk => disk_checks(ctx),
            Self::Fleets => fleets_checks(ctx),
            Self::ActionTimeout => execution_checks(ctx, ExecutionScenario::ActionTimeout),
            Self::ActionTimeoutNoop => execution_checks(ctx, ExecutionScenario::ActionTimeoutNoop),
            Self::SandboxContract => execution_checks(ctx, ExecutionScenario::SandboxContract),
            Self::GlibcToolchain => glibc_toolchain_checks(ctx),
            Self::FhsTools => fhs_tools_checks(ctx),
            Self::StaleScope => execution_checks(ctx, ExecutionScenario::StaleScope),
            Self::MemoryEscalation => execution_checks(ctx, ExecutionScenario::MemoryEscalation),
            Self::UnplaceableDisk => execution_checks(ctx, ExecutionScenario::UnplaceableDisk),
            Self::OversizedFleet => execution_checks(ctx, ExecutionScenario::OversizedFleet),
            Self::ColdStartMisfit => execution_checks(ctx, ExecutionScenario::ColdStartMisfit),
            Self::Preemption => execution_checks(ctx, ExecutionScenario::Preemption),
            Self::WorkerRenewal => execution_checks(ctx, ExecutionScenario::WorkerRenewal),
            Self::Orphan => control_checks(self, ctx, ControlKnobs::ORPHAN, orphan),
            Self::ScheduledMinimum => scheduled_minimum_checks(ctx),
            Self::BurstFromZero
            | Self::ColdBurstFromZero
            | Self::QueuedBehindShortWork
            | Self::BacklogGrowsFleet
            | Self::SustainedStream
            | Self::TwoSources
            | Self::UtilizationScaleIn => work_aware_dispatch(self, ctx),
            Self::DrainCancel => {
                control_checks(self, ctx, ControlKnobs::DRAIN_CANCEL, drain_cancel)
            }
            Self::SkippedPass => {
                control_checks(self, ctx, ControlKnobs::SKIPPED_PASS, skipped_pass)
            }
            Self::Cli => attempt(self, || {
                let terminal = LocalTerminal::create(ctx.inputs)?;
                cli(&terminal)
            }),
            Self::CheckedInConfigs => attempt(self, || {
                checked_in_configs(&ShippedConfigs::new(ctx.inputs))
            }),
            Self::Plaintext => plaintext_checks(self, ctx, |stack| plaintext(stack)),
            Self::Setup => attempt(self, || {
                let secured = Secured::create(ctx.inputs, &ctx.baseline, ctx.issuer)?;
                setup(&secured)
            }),
            Self::Revocation => attempt(self, || {
                let secured = Secured::create(ctx.inputs, &ctx.baseline, ctx.issuer)?;
                revocation(&secured)
            }),
            Self::Dashboard => dashboard_checks(ctx),
            Self::LaunchFailure => {
                control_checks(self, ctx, ControlKnobs::LAUNCH_FAILURE, launch_failure)
            }
            Self::Api => attempt(self, || {
                api(
                    &server_deployment(ctx, ServerKnobs::NONE, IdentityKnobs::NONE)?,
                    ctx.inputs.settings.history_timeout,
                )
            }),
            Self::RequestLimits => attempt(self, || {
                request_limits(&server_deployment(
                    ctx,
                    ServerKnobs::TIGHT_LIMITS,
                    IdentityKnobs::SIGN_IN,
                )?)
            }),
            Self::ViewerCap => attempt(self, || {
                viewer_cap(
                    &server_deployment(ctx, ServerKnobs::ONE_VIEWER, IdentityKnobs::NONE)?,
                    ctx.inputs.settings.history_timeout,
                )
            }),
            Self::CertificateRotation => attempt(self, || {
                certificate_rotation(&server_deployment(
                    ctx,
                    ServerKnobs::SHORT_CERTIFICATE,
                    IdentityKnobs::NONE,
                )?)
            }),
            Self::Identity => identity_checks(ctx),
            Self::Sessions => sessions_checks(ctx),
            Self::Builds => attempt(self, || builds(ctx.inputs, &ctx.baseline, ctx.issuer)),
        }
    }
}

fn identity_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Identity, || {
        let variant = Variant {
            identity: IdentityKnobs::IDENTITY,
            ..ctx.baseline
        };
        let secured = Secured::create(ctx.inputs, &variant, ctx.issuer)?;
        identity(&secured)
    })
}

fn lifecycle_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Lifecycle, || {
        let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Tls, ctx.issuer)?;
        lifecycle(&stack, &ctx.baseline).map(|checks| with_event_paging(checks, &stack))
    })
}

fn upgrade_checks(scenario: Scenario, ctx: &RunContext<'_>) -> Vec<Check> {
    if cfg!(target_os = "linux") {
        attempt(scenario, || {
            let variant = required_cluster_variant(scenario, &ctx.baseline)?;
            let automatic_inputs =
                (scenario == Scenario::RollingUpgradeAutomatic).then(|| ctx.inputs.clone());
            let inputs = automatic_inputs.as_ref().unwrap_or(ctx.inputs);
            crate::rolling_upgrade::rolling_upgrade(inputs, &variant, ctx.issuer)
        })
    } else {
        vec![Check::skipped(
            scenario.as_str(),
            "mixed-version held probes require Linux",
        )]
    }
}

fn config_restart_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    let scenario = Scenario::ConfigRefetchRestart;
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            scenario.as_str(),
            "release restart probes require Linux",
        )];
    }
    attempt(scenario, || {
        let variant = required_cluster_variant(scenario, &ctx.baseline)?;
        crate::upgrade_config_restart::config_refetch_restart(ctx.inputs, &variant, ctx.issuer)
    })
}

/// The drained-start and cold-pair verdicts on independent, fresh placement deployments.
fn fleets_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    let variant = Variant {
        fleets: FleetLayout::Placement,
        control: ControlKnobs::FLEETS,
        ..ctx.baseline
    };
    parallel_variant_checks([false, true], |cold_pair| {
        attempt(Scenario::Fleets, || {
            let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
            if cold_pair {
                run_cold_pair(&stack, &variant)
            } else {
                run_drained_starts(&stack, &variant)
            }
        })
    })
}

/// The verdicts of the scheduled-minimum scenario on a fresh deployment.
fn scheduled_minimum_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    control_checks(
        Scenario::ScheduledMinimum,
        ctx,
        ControlKnobs::SCHEDULED_MINIMUM,
        scheduled_minimum,
    )
}

fn work_aware_dispatch(scenario: Scenario, ctx: &RunContext<'_>) -> Vec<Check> {
    match scenario {
        Scenario::BurstFromZero => work_aware_checks(
            scenario,
            ctx,
            ControlKnobs::BURST_FROM_ZERO,
            |stack, exec| burst_from_zero(stack, exec),
        ),
        Scenario::QueuedBehindShortWork => work_aware_checks(
            scenario,
            ctx,
            ControlKnobs::QUEUED_BEHIND_SHORT_WORK,
            |stack, exec| queued_behind_short_work(stack, exec),
        ),
        Scenario::BacklogGrowsFleet => work_aware_checks(
            scenario,
            ctx,
            ControlKnobs::BACKLOG_GROWS_FLEET,
            |stack, exec| backlog_grows_fleet(stack, exec),
        ),
        Scenario::ColdBurstFromZero => work_aware_checks(
            scenario,
            ctx,
            ControlKnobs::COLD_BURST_FROM_ZERO,
            |stack, exec| crate::cold_burst_from_zero(stack, exec),
        ),
        Scenario::SustainedStream => {
            work_aware_checks(scenario, ctx, ControlKnobs::SUSTAINED_STREAM, |stack, _| {
                crate::sustained_stream(stack)
            })
        }
        Scenario::TwoSources => attempt(scenario, || {
            let variant = Variant {
                control: ControlKnobs::TWO_SOURCES,
                ..ctx.baseline
            };
            let secured = Secured::create(ctx.inputs, &variant, ctx.issuer)?;
            crate::two_sources(&secured)
        }),
        Scenario::UtilizationScaleIn => attempt(scenario, || {
            let variant = Variant {
                control: ControlKnobs::UTILIZATION_SCALE_IN,
                ..ctx.baseline
            };
            let secured = Secured::create(ctx.inputs, &variant, ctx.issuer)?;
            crate::utilization_scale_in(&secured)
        }),
        _ => attempt(scenario, || {
            Err(crate::errors::E2eError::system("not a work-aware scenario"))
        }),
    }
}

fn work_aware_checks(
    scenario: Scenario,
    ctx: &RunContext<'_>,
    control: ControlKnobs,
    run: impl FnOnce(&Stack, &Observer) -> Result<Vec<Check>>,
) -> Vec<Check> {
    attempt(scenario, || {
        let variant = Variant {
            control,
            ..ctx.baseline
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        let exec = Observer::new(&stack, &ctx.inputs.settings)?;
        run(&stack, &exec)
    })
}

/// The verdicts of the dashboard scenario on fresh deployments.
fn dashboard_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Dashboard, || {
        let variant = Variant {
            control: ControlKnobs {
                detail_interval: Some(std::time::Duration::from_secs(1)),
                ..ControlKnobs::NONE
            },
            ..ctx.baseline
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        let metadata = running_metadata(&stack, ctx.inputs.settings.history_timeout)?;
        let secured = Secured::create(ctx.inputs, &ctx.baseline, ctx.issuer)?;
        let mut checks = vec![metadata];
        checks.extend(dashboard(&secured, ctx.inputs.settings.history_timeout)?);
        Ok(checks)
    })
}

/// The verdicts of `scenario` run by `checks` on a fresh plaintext deployment.
fn plaintext_checks(
    scenario: Scenario,
    ctx: &RunContext<'_>,
    checks: impl FnOnce(&Stack<'_>) -> Result<Vec<Check>>,
) -> Vec<Check> {
    attempt(scenario, || {
        let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Plain, ctx.issuer)?;
        checks(&stack)
    })
}

/// The verdicts of the browser sessions scenario on a short-lived deployment.
fn sessions_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Sessions, || {
        let variant = Variant {
            identity: IdentityKnobs::SESSIONS,
            ..ctx.baseline
        };
        let secured = Secured::create(ctx.inputs, &variant, ctx.issuer)?;
        sessions(&secured, ctx.inputs.settings.history_timeout)
    })
}

/// Checks network capability admission without an egress fixture.
fn network_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Network, || {
        let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Tls, ctx.issuer)?;
        network(&stack)
    })
}

/// The verdicts of the glibc toolchain scenario, or one skipped check where the sandbox is the host.
fn glibc_toolchain_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    match ctx.sandbox {
        Sandbox::Bwrap => attempt(Scenario::GlibcToolchain, || {
            ctx.inputs.action_tools.glibc_tool_program()?;
            let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Tls, ctx.issuer)?;
            glibc_toolchain(&stack)
        }),
        Sandbox::Noop => vec![Check::skipped(
            names::glibc_toolchain::TOOL_RUNS,
            "the noop sandbox runs actions on the host, whose own loader starts the program",
        )],
    }
}

/// The verdicts of the FHS tools scenario, or one skipped check where the sandbox is the host.
fn fhs_tools_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    match ctx.sandbox {
        Sandbox::Bwrap => attempt(Scenario::FhsTools, || {
            let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Tls, ctx.issuer)?;
            fhs_tools(&stack)
        }),
        Sandbox::Noop => vec![Check::skipped(
            names::fhs_tools::SCRIPT_RUNS,
            "the noop sandbox runs actions on the host, whose own /bin and /usr/bin they see",
        )],
    }
}

/// The verdicts of the input integrity scenario, on one deployment that breaks input links and
/// one that keeps them.
fn input_integrity_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    let mut checks = Vec::new();
    for (label, links) in [("break", InputLinks::Break), ("keep", InputLinks::Keep)] {
        let variant = Variant {
            input_links: links,
            ..ctx.baseline
        };
        checks.extend(attempt(
            format_args!("{} {label}", Scenario::InputIntegrity),
            || {
                let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
                input_integrity(&stack, &variant)
            },
        ));
    }
    checks
}

/// The verdicts of the input root scenario, on the baseline deployment.
fn input_root_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::InputRoot, || {
        let stack = Stack::create(ctx.inputs, &ctx.baseline, Transport::Tls, ctx.issuer)?;
        input_root(&stack, &ctx.baseline)
    })
}

/// The verdicts of the limits scenario, or one skipped check where the platform has no cgroups.
fn limits_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::limits::CGROUPS_ENFORCED,
            "cgroups exist only on Linux",
        )];
    }
    attempt(Scenario::Limits, || {
        // Under the noop sandbox nothing but `cgroup.kill` ends the orphaned process; under
        // bwrap the PID namespace teardown would kill it and the scenario would not test the
        // cgroup. A healthy allocation above the reservation must finish on its first attempt.
        let variant = Variant {
            sandbox: Sandbox::Noop,
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Off,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        limits(&stack, stack.stderr_tail_lines())
    })
}

/// The verdicts of the restart scenario, or one skipped check where the platform has no cgroups.
fn restart_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::restart::BUILD_STILL_RUNNING,
            "the held build needs cgroups, which exist only on Linux",
        )];
    }
    attempt(Scenario::Restart, || {
        // The held build runs under the noop sandbox with cgroup limits, as in `limits_checks`:
        // the orphan and the held build are ended by the cgroup, not by the sandbox.
        let variant = Variant {
            sandbox: Sandbox::Noop,
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Off,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        restart(&stack)
    })
}
const LOGIN_ACROSS_SUBJECT: &str = "login-across";
const DATABASE_OUTAGE_SUBJECT: &str = "database-outage";

/// Every cluster deployment selects its identity here so concurrent scenarios cannot share
/// a subject whose issuer policy is changed by another scenario.
fn cluster_variant(scenario: Scenario, baseline: &Variant) -> Option<Variant> {
    let mut variant = match scenario {
        Scenario::ControllerLoss
        | Scenario::RollingDeploy
        | Scenario::RollingUpgrade
        | Scenario::RollingUpgradeAutomatic
        | Scenario::ConfigRefetchRestart
        | Scenario::FollowerBuilds => Variant::baseline(Sandbox::Noop),
        Scenario::LeaderFailover => Variant {
            control: ControlKnobs::LEADER_FAILOVER,
            ..Variant::baseline(Sandbox::Noop)
        },
        Scenario::CrossRevocation => Variant {
            identity: IdentityKnobs {
                standing_worker: crate::access::StandingWorker::One,
                ..IdentityKnobs::SESSIONS
            },
            ..*baseline
        },
        Scenario::LoginAcross => Variant {
            identity: IdentityKnobs::IDENTITY,
            ..*baseline
        },
        Scenario::DatabaseOutage => Variant {
            identity: IdentityKnobs::SIGN_IN,
            control: ControlKnobs::DATABASE_OUTAGE,
            ..Variant::baseline(Sandbox::Noop)
        },
        _ => return None,
    };
    let subject = match scenario {
        Scenario::CrossRevocation => CROSS_REVOCATION_SUBJECT,
        Scenario::LoginAcross => LOGIN_ACROSS_SUBJECT,
        Scenario::DatabaseOutage => DATABASE_OUTAGE_SUBJECT,
        _ => return Some(variant),
    };
    variant.identity.sign_in = SignIn::Subject(subject);
    Some(variant)
}

fn required_cluster_variant(scenario: Scenario, baseline: &Variant) -> Result<Variant> {
    cluster_variant(scenario, baseline)
        .ok_or_else(|| crate::errors::E2eError::system("not a cluster scenario"))
}

fn controller_loss_inputs(inputs: &DeploymentInputs) -> Result<DeploymentInputs> {
    let mut invocation = inputs.clone();
    let mut document: toml::Value =
        toml::from_str(&invocation.base_config).map_err(crate::template::template_error)?;
    let fleet = document
        .get_mut("fleets")
        .and_then(toml::Value::as_array_mut)
        .and_then(|fleets| fleets.first_mut())
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| crate::errors::E2eError::system("missing first fleet"))?;
    fleet.insert("max_instances".into(), toml::Value::Integer(2));
    fleet.insert("vcpus_per_instance".into(), toml::Value::Integer(1));
    fleet.insert(
        "target_actions_per_instance".into(),
        toml::Value::Integer(1),
    );
    let server = document
        .get_mut("server")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| crate::errors::E2eError::system("missing server configuration"))?;
    let leadership =
        toml::from_str("ttl = \"10s\"\nrenew_interval = \"3s\"\nclock_skew = \"2s\"\n")
            .map_err(crate::template::template_error)?;
    server.insert("leadership".into(), leadership);
    invocation.base_config = toml::to_string(&document).map_err(crate::template::template_error)?;
    Ok(invocation)
}

/// Runs the leader and follower variants in independent deployments.
fn controller_loss_checks(ctx: &RunContext<'_>, engine: Engine) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::controller_loss::HELD,
            "the controller-loss probes identify workers through Linux process ancestry",
        )];
    }
    attempt(Scenario::ControllerLoss, || {
        let invocation = controller_loss_inputs(ctx.inputs)?;
        let variant = required_cluster_variant(Scenario::ControllerLoss, &ctx.baseline)?;
        let mut checks = Vec::new();
        for follower in [false, true] {
            let cluster =
                crate::cluster::Cluster::start(&invocation, &variant, ctx.issuer, 2, engine)?;
            let variant_checks =
                cluster.diagnose(crate::controller_loss::controller_loss(&cluster, follower))?;
            checks.extend(prefix_checks(
                variant_checks,
                if follower { "follower" } else { "leader" },
            ));
        }
        Ok(checks)
    })
}

/// Applies the worker capacity needed by the held-build cluster scenarios.
fn cluster_inputs(ctx: &RunContext<'_>, min: i64, max: i64) -> Result<DeploymentInputs> {
    let mut invocation = ctx.inputs.clone();
    let mut document: toml::Value =
        toml::from_str(&invocation.base_config).map_err(crate::template::template_error)?;
    let fleet = document
        .get_mut("fleets")
        .and_then(toml::Value::as_array_mut)
        .and_then(|fleets| fleets.first_mut())
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| crate::errors::E2eError::system("missing first fleet"))?;
    fleet.insert("min_instances".into(), toml::Value::Integer(min));
    fleet.insert("max_instances".into(), toml::Value::Integer(max));
    fleet.insert("vcpus_per_instance".into(), toml::Value::Integer(2));
    invocation.base_config = toml::to_string(&document).map_err(crate::template::template_error)?;
    Ok(invocation)
}

/// Runs the leader-loss and zombie variants on separate deployments.
fn leader_failover_checks(ctx: &RunContext<'_>, engine: Engine) -> Result<Vec<Check>> {
    let invocation = cluster_inputs(ctx, 1, 2)?;
    let variant = required_cluster_variant(Scenario::LeaderFailover, &ctx.baseline)?;
    Ok(parallel_variant_checks([false, true], |zombie| {
        prefix_checks(
            attempt(Scenario::LeaderFailover, || {
                let cluster =
                    crate::cluster::Cluster::start(&invocation, &variant, ctx.issuer, 2, engine)?;
                cluster.diagnose(crate::leader_failover::leader_failover(&cluster, zombie))
            }),
            if zombie { "zombie" } else { "killed" },
        )
    }))
}

/// Runs the outage scenario with one and two controllers on separate deployments.
fn database_outage_checks(ctx: &RunContext<'_>, engine: Engine) -> Result<Vec<Check>> {
    let invocation = cluster_inputs(ctx, 1, 3)?;
    let variant = required_cluster_variant(Scenario::DatabaseOutage, &ctx.baseline)?;
    Ok(parallel_variant_checks([1, 2], |count| {
        prefix_checks(
            attempt(Scenario::DatabaseOutage, || {
                let cluster = crate::cluster::Cluster::start(
                    &invocation,
                    &variant,
                    ctx.issuer,
                    count,
                    engine,
                )?;
                cluster.diagnose(crate::database_outage::database_outage(&cluster, count))
            }),
            &format!("{count} controllers"),
        )
    }))
}

/// Runs each cluster scenario on its own appropriately configured deployment.
fn cluster_scenario_checks(scenario: Scenario, ctx: &RunContext<'_>) -> Vec<Check> {
    cluster_engine_checks(|engine| cluster_scenario_engine_checks(scenario, ctx, engine))
}

fn cluster_engine_checks(run: impl Fn(Engine) -> Vec<Check> + Sync) -> Vec<Check> {
    parallel_variant_checks([Engine::DynamoDb, Engine::Postgres], |engine| {
        prefix_checks(run(engine), engine.as_str())
    })
}

/// Runs isolated variants concurrently, retaining evidence and verdict order on each worker.
/// Join every worker before propagating a panic so no deployment outlives the variant group.
fn parallel_variant_checks<T: Send, const N: usize>(
    variants: [T; N],
    run: impl Fn(T) -> Vec<Check> + Sync,
) -> Vec<Check> {
    thread::scope(|scope| {
        let handles = variants.map(|variant| {
            let run = &run;
            scope.spawn(move || crate::stack::with_failure_evidence(|| run(variant)))
        });
        let results = handles.map(std::thread::ScopedJoinHandle::join);
        results
            .into_iter()
            .flat_map(|result| match result {
                Ok(checks) => checks,
                Err(panic) => std::panic::resume_unwind(panic),
            })
            .collect()
    })
}

fn cluster_scenario_engine_checks(
    scenario: Scenario,
    ctx: &RunContext<'_>,
    engine: Engine,
) -> Vec<Check> {
    if scenario == Scenario::ControllerLoss {
        return controller_loss_checks(ctx, engine);
    }
    if !cfg!(target_os = "linux")
        && matches!(
            scenario,
            Scenario::RollingDeploy | Scenario::LeaderFailover | Scenario::DatabaseOutage
        )
    {
        return vec![Check::skipped(
            scenario.as_str(),
            "held probes require Linux process ancestry",
        )];
    }
    attempt(scenario, || match scenario {
        Scenario::RollingDeploy => {
            let invocation = cluster_inputs(ctx, 0, 1)?;
            let variant = required_cluster_variant(scenario, &ctx.baseline)?;
            let cluster =
                crate::cluster::Cluster::start(&invocation, &variant, ctx.issuer, 2, engine)?;
            cluster.diagnose(crate::rolling_deploy::rolling_deploy(&cluster))
        }
        Scenario::FollowerBuilds => {
            let invocation = cluster_inputs(ctx, 0, 1)?;
            let variant = required_cluster_variant(scenario, &ctx.baseline)?;
            let cluster =
                crate::cluster::Cluster::start(&invocation, &variant, ctx.issuer, 3, engine)?;
            cluster.diagnose(crate::follower_builds::follower_builds(&cluster))
        }
        Scenario::CrossRevocation => {
            let variant = required_cluster_variant(scenario, &ctx.baseline)?;
            let cluster =
                crate::cluster::Cluster::start(ctx.inputs, &variant, ctx.issuer, 2, engine)?;
            cluster.diagnose(cross_revocation(&cluster, ctx.issuer))
        }
        Scenario::LeaderFailover => leader_failover_checks(ctx, engine),
        Scenario::LoginAcross => {
            let variant = required_cluster_variant(scenario, &ctx.baseline)?;
            let cluster =
                crate::cluster::Cluster::start(ctx.inputs, &variant, ctx.issuer, 2, engine)?;
            cluster.diagnose(crate::login_across::login_across(&cluster))
        }
        Scenario::DatabaseOutage => database_outage_checks(ctx, engine),
        _ => Err(crate::errors::E2eError::system("not a cluster scenario")),
    })
}

/// The verdicts of the memory scenario, or one skipped check where the platform has no cgroups.
fn memory_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::memory::CGROUPS_ENFORCED,
            "cgroups exist only on Linux",
        )];
    }
    attempt(Scenario::Memory, || {
        // Soft memory enforcement is explicitly selected for this regression.
        let variant = Variant {
            sandbox: Sandbox::Noop,
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Enforced,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        memory(&stack, stack.stderr_tail_lines())
    })
}

/// Runs a kernel OOM against a bounded pool with userspace memory enforcement disabled.
fn pool_oom_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::suite::scenario_completes(Scenario::PoolOom),
            "pool OOM observation requires Linux cgroups",
        )];
    }
    attempt(Scenario::PoolOom, || {
        let policy = crate::execution::ResourcePolicy::pool_oom_scenario()?;
        let capacity = policy
            .fleet_memory
            .ok_or_else(|| crate::E2eError::system("pool OOM fixture has no fleet capacity"))?;
        let escalation_percent = policy.memory.escalation_percent.get();
        let execution = ExecutionKnobs {
            resource_policy: Some(policy),
            ..ExecutionKnobs::NONE
        };
        let variant = Variant {
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Off,
            execution,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        let exec = Observer::new(&stack, &ctx.inputs.settings)?;
        crate::oom::pool_oom(&stack, &exec, escalation_percent, capacity.as_u64() / 1024)
    })
}

/// Runs the bounded genuine-PSI scenario on Linux.
fn pressure_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    if !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::suite::scenario_completes(Scenario::Pressure),
            "memory pressure requires Linux cgroups",
        )];
    }
    attempt(Scenario::Pressure, || {
        let execution = ExecutionKnobs {
            resource_policy: Some(crate::execution::ResourcePolicy::pressure_scenario()?),
            ..ExecutionKnobs::NONE
        };
        let variant = Variant {
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Off,
            execution,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        let exec = Observer::new(&stack, &ctx.inputs.settings)?;
        crate::pressure::pressure(&stack, &exec)
    })
}

/// The verdicts of the disk scenario, on one fresh deployment. The disk sampler is checked only
/// where the platform has one.
fn disk_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Disk, || {
        let variant = Variant {
            soft_disk: true,
            ..Variant::baseline(Sandbox::native())
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        disk(&stack, stack.stderr_tail_lines(), cfg!(target_os = "linux"))
    })
}

/// Execution scenarios, excluding scenarios handled by other deployment paths.
#[derive(Clone, Copy)]
enum ExecutionScenario {
    /// Timeout under bwrap.
    ActionTimeout,
    /// Timeout under the noop sandbox.
    ActionTimeoutNoop,
    /// Sandbox isolation contract.
    SandboxContract,
    /// Stale action scope cleanup.
    StaleScope,
    /// Memory limit escalation.
    MemoryEscalation,
    /// Disk placement failure.
    UnplaceableDisk,
    /// Oversized fleet placement.
    OversizedFleet,
    /// Cold start placement failure.
    ColdStartMisfit,
    /// Worker preemption.
    Preemption,
    /// Worker certificate renewal.
    WorkerRenewal,
}

impl ExecutionScenario {
    /// The corresponding suite scenario.
    const fn scenario(self) -> Scenario {
        match self {
            Self::ActionTimeout => Scenario::ActionTimeout,
            Self::ActionTimeoutNoop => Scenario::ActionTimeoutNoop,
            Self::SandboxContract => Scenario::SandboxContract,
            Self::StaleScope => Scenario::StaleScope,
            Self::MemoryEscalation => Scenario::MemoryEscalation,
            Self::UnplaceableDisk => Scenario::UnplaceableDisk,
            Self::OversizedFleet => Scenario::OversizedFleet,
            Self::ColdStartMisfit => Scenario::ColdStartMisfit,
            Self::Preemption => Scenario::Preemption,
            Self::WorkerRenewal => Scenario::WorkerRenewal,
        }
    }
}

/// The variant of an execution scenario, and whether it needs Linux (bwrap, cgroups or /proc).
/// Every variant but the knob it tests is the baseline; the scenarios that observe action scopes
/// add per-action cgroups, with `NativeLink`'s memory enforcement off where an out-of-memory kill
/// is checked.
fn execution_variant(scenario: ExecutionScenario, baseline: &Variant) -> (Variant, bool) {
    let scoped = |sandbox: Sandbox, execution: ExecutionKnobs, soft_memory: SoftMemory| Variant {
        sandbox,
        limits: ActionLimits::Cgroup,
        soft_memory,
        execution,
        ..Variant::baseline(sandbox)
    };
    let knob = |execution: ExecutionKnobs| Variant {
        execution,
        ..*baseline
    };
    let short = ExecutionKnobs::SHORT_TIMEOUT;
    match scenario {
        ExecutionScenario::ActionTimeout => {
            (scoped(Sandbox::Bwrap, short, SoftMemory::Enforced), true)
        }
        ExecutionScenario::ActionTimeoutNoop => {
            (scoped(Sandbox::Noop, short, SoftMemory::Enforced), true)
        }
        ExecutionScenario::SandboxContract => (
            scoped(Sandbox::Bwrap, ExecutionKnobs::NONE, SoftMemory::Off),
            true,
        ),
        ExecutionScenario::StaleScope => (
            scoped(Sandbox::Noop, ExecutionKnobs::NONE, SoftMemory::Off),
            true,
        ),
        ExecutionScenario::MemoryEscalation => (
            scoped(
                Sandbox::Noop,
                ExecutionKnobs::ONE_STEP_ESCALATION,
                SoftMemory::Enforced,
            ),
            true,
        ),
        ExecutionScenario::OversizedFleet => (knob(ExecutionKnobs::OVERSIZED_FLEET), false),
        ExecutionScenario::ColdStartMisfit => (knob(ExecutionKnobs::COLD_START_MISFIT), false),
        ExecutionScenario::WorkerRenewal => (knob(ExecutionKnobs::SHORT_LIVED_CERTIFICATES), false),
        ExecutionScenario::UnplaceableDisk | ExecutionScenario::Preemption => (*baseline, false),
    }
}

/// The verdicts of an execution scenario on its own deployment, or one skipped check where the
/// scenario needs Linux and the platform is another.
fn execution_checks(ctx: &RunContext<'_>, scenario: ExecutionScenario) -> Vec<Check> {
    let (variant, linux_only) = execution_variant(scenario, &ctx.baseline);
    if linux_only && !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::suite::scenario_completes(scenario.scenario()),
            "bwrap, cgroups and /proc exist only on Linux",
        )];
    }
    attempt(scenario.scenario(), || {
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        if matches!(scenario, ExecutionScenario::SandboxContract) {
            crate::sample::stage_action_environment(
                &stack.placement().workspace(),
                &ctx.inputs.postgres,
            )?;
        }
        let exec = Observer::new(&stack, &ctx.inputs.settings)?;
        let tail = stack.stderr_tail_lines();
        match scenario {
            ExecutionScenario::ActionTimeout => action_timeout(&stack, &exec, Sandbox::Bwrap),
            ExecutionScenario::ActionTimeoutNoop => {
                let mut checks = action_timeout(&stack, &exec, Sandbox::Noop)?;
                checks.extend(exit_signals(&stack, tail)?);
                Ok(checks)
            }
            ExecutionScenario::SandboxContract => {
                let mut checks = sandbox_contract(&stack, &exec, tail)?;
                checks.push(stack.rootfs_test()?);
                Ok(checks)
            }
            ExecutionScenario::StaleScope => {
                stale_scope(&stack, &exec, tail, ctx.inputs.settings.hold_timeout)
            }
            ExecutionScenario::MemoryEscalation => memory_escalation(&stack, tail),
            ExecutionScenario::UnplaceableDisk => unplaceable_disk(&stack, &exec, tail),
            ExecutionScenario::OversizedFleet => oversized_fleet(&stack, &exec),
            ExecutionScenario::ColdStartMisfit => cold_start_misfit(&exec),
            ExecutionScenario::Preemption => preemption(&stack, &exec),
            ExecutionScenario::WorkerRenewal => worker_renewal(&stack, &exec),
        }
    })
}

/// The verdicts of a control scenario: `run` against one fresh deployment whose control loop is
/// tuned by `knobs`.
fn control_checks<'a>(
    scenario: Scenario,
    ctx: &RunContext<'a>,
    knobs: ControlKnobs,
    run: impl FnOnce(&Stack<'a>) -> Result<Vec<Check>>,
) -> Vec<Check> {
    attempt(scenario, || {
        let variant = Variant {
            control: knobs,
            ..ctx.baseline
        };
        let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
        run(&stack)
    })
}

/// A started deployment for a server scenario: the baseline under the server and identity knobs,
/// over TLS, with no worker running.
///
/// # Errors
///
/// When the deployment cannot be created or its server does not start.
fn server_deployment<'a>(
    ctx: &RunContext<'a>,
    server: ServerKnobs,
    identity: IdentityKnobs,
) -> Result<Stack<'a>> {
    let variant = Variant {
        identity,
        server,
        ..ctx.baseline
    };
    let stack = Stack::create(ctx.inputs, &variant, Transport::Tls, ctx.issuer)?;
    stack.start_server()?;
    Ok(stack)
}

/// The lifecycle verdicts followed by the event-paging verdicts on the same settled deployment. A
/// paging run that cannot complete adds one failed check instead of losing the lifecycle verdicts.
fn with_event_paging(mut checks: Vec<Check>, stack: &Stack<'_>) -> Vec<Check> {
    match event_paging(stack) {
        Ok(paging) => checks.extend(paging),
        Err(error) => checks.push(Check::failed(
            names::event_paging::WALK_COMPLETE,
            gang_types::error_chain(&error).to_string(),
        )),
    }
    checks
}
/// Whether the run covers `scenario`: an empty selection covers every scenario.
#[must_use]
fn selected(scenarios: &[Scenario], scenario: Scenario) -> bool {
    scenarios.is_empty() || scenarios.contains(&scenario)
}

/// Whether a run of `gate` with the selection `chosen` runs `scenario`. An empty selection runs
/// the gate's default list; a named selection runs the named scenarios that the gate selects.
#[must_use]
fn covers(gate: Gate, chosen: &[Scenario], scenario: Scenario) -> bool {
    scenario.gate() <= gate && selected(chosen, scenario)
}

/// The scenarios of `selected` that `gate` does not run, in the order given.
///
/// A caller rejects these rather than letting [`run`] ignore them.
#[must_use]
pub fn unknown_scenarios(gate: Gate, selected: &[Scenario]) -> Vec<Scenario> {
    selected
        .iter()
        .copied()
        .filter(|scenario| scenario.gate() > gate)
        .collect()
}

/// Runs one scenario; an error it returns becomes a failed check named after the scenario.
fn attempt(name: impl fmt::Display, scenario: impl FnOnce() -> Result<Vec<Check>>) -> Vec<Check> {
    match scenario() {
        Ok(checks) => checks,
        Err(error) => vec![Check::failed(
            names::suite::scenario_completes(name),
            gang_types::error_chain(&error).to_string(),
        )],
    }
}

/// Applies `run` to every item of `items` on at most `workers` threads at once, each thread taking
/// the next item not yet started, and returns the results in the order of `items`. The result of
/// an item whose thread panicked is `None`; the other items still run.
fn run_bounded<T: Sync, R: Send>(
    items: &[T],
    workers: NonZeroUsize,
    run: impl Fn(&T) -> R + Sync,
) -> Vec<Option<R>> {
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<R>>> = Mutex::new(items.iter().map(|_| None).collect());
    thread::scope(|scope| {
        let handles: Vec<_> = (0..workers.get().min(items.len()))
            .map(|_| {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(item) = items.get(index) else {
                            break;
                        };
                        let result = run(item);
                        results.lock()[index] = Some(result);
                    }
                })
            })
            .collect();
        for handle in handles {
            // A panicked thread leaves its item `None`; joining it here keeps the scope from
            // re-raising the panic.
            drop(handle.join());
        }
    });
    results.into_inner()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::settings::Settings;

    use super::*;

    #[test]
    fn controller_loss_fixture_has_two_held_action_slots_and_two_cpus() {
        let inputs = DeploymentInputs {
            gang: PathBuf::default(),
            bazel: PathBuf::default(),
            postgres: PathBuf::default(),
            base_config: "[server]\n[[fleets]]\nmax_instances = 4\nvcpus_per_instance = 4\ntarget_actions_per_instance = 4\n".into(),
            sample: PathBuf::default(),
            configs: PathBuf::default(),
            settings: Settings::default(),
            action_tools: crate::settings::ActionTools {
                shell: PathBuf::default(),
                path: String::default(),
                bwrap: None,
                glibc_tool: None,
                action_rootfs: None,
            },
            upgrade: None,
        };
        let invocation = controller_loss_inputs(&inputs).expect("fixture renders");
        let document: toml::Value = toml::from_str(&invocation.base_config).expect("valid TOML");
        let fleet = &document["fleets"][0];
        let workers = fleet["max_instances"].as_integer().expect("max instances");
        let cpus = fleet["vcpus_per_instance"].as_integer().expect("vCPUs");
        let slots = fleet["target_actions_per_instance"]
            .as_integer()
            .expect("target actions");
        assert_eq!(workers, 2);
        assert_eq!(workers * slots, 2);
        assert_eq!(workers * cpus, 2);
    }

    #[test]
    fn cluster_engines_run_on_separate_threads_in_verdict_order() {
        let parent = thread::current().id();
        let threads = Mutex::new(Vec::new());
        let checks = cluster_engine_checks(|_| {
            threads.lock().push(thread::current().id());
            vec![Check::passed("check")]
        });
        let threads = threads.into_inner();
        assert_eq!(threads.len(), 2);
        assert_ne!(threads[0], parent);
        assert_ne!(threads[1], parent);
        assert_ne!(threads[0], threads[1]);
        assert_eq!(checks[0].name, "dynamodb: check");
        assert_eq!(checks[1].name, "postgres: check");
    }

    #[test]
    fn independent_variants_overlap_and_keep_complete_verdict_order() {
        let both_started = std::sync::Barrier::new(2);
        for variants in [[false, true], [true, false]] {
            let checks = parallel_variant_checks(variants, |variant| {
                both_started.wait();
                if variant {
                    vec![Check::passed("second")]
                } else {
                    vec![
                        Check::failed("first", "variant failure"),
                        Check::passed("first end"),
                    ]
                }
            });
            let names: Vec<_> = checks.iter().map(|check| check.name.as_ref()).collect();
            assert_eq!(
                names,
                if variants[0] {
                    vec!["second", "first", "first end"]
                } else {
                    vec!["first", "first end", "second"]
                }
            );
            assert_eq!(checks.iter().filter(|check| check.is_failure()).count(), 1);
        }
    }

    #[test]
    fn independent_variant_panics_join_every_worker_before_propagating() {
        let both_started = std::sync::Barrier::new(2);
        let completed = AtomicUsize::new(0);
        let result = std::panic::catch_unwind(|| {
            parallel_variant_checks([true, false], |panic| {
                both_started.wait();
                assert!(!panic, "variant panic");
                completed.fetch_add(1, Ordering::SeqCst);
                vec![Check::passed("completed")]
            })
        });
        assert!(result.is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cluster_sign_in_subjects_are_isolated_from_mutated_issuer_policies() {
        let mutated = [
            crate::access::SIGN_IN_SUBJECT,
            crate::browser::BROWSER_SUBJECT,
        ];
        for scenario in [
            Scenario::ControllerLoss,
            Scenario::RollingDeploy,
            Scenario::RollingUpgrade,
            Scenario::RollingUpgradeAutomatic,
            Scenario::ConfigRefetchRestart,
            Scenario::FollowerBuilds,
            Scenario::CrossRevocation,
            Scenario::LeaderFailover,
            Scenario::DatabaseOutage,
            Scenario::LoginAcross,
        ] {
            let variant = cluster_variant(scenario, &Variant::baseline(Sandbox::Noop))
                .expect("listed scenarios are cluster scenarios");
            if let Some(subject) = variant.identity.sign_in.subject(variant.identity.sessions) {
                assert!(!mutated.contains(&subject), "{scenario}: {subject}");
            }
        }
    }

    #[test]
    fn cluster_control_presets_are_scenario_specific() {
        for scenario in [
            Scenario::ControllerLoss,
            Scenario::RollingDeploy,
            Scenario::RollingUpgrade,
            Scenario::RollingUpgradeAutomatic,
            Scenario::ConfigRefetchRestart,
            Scenario::FollowerBuilds,
            Scenario::CrossRevocation,
            Scenario::LeaderFailover,
            Scenario::DatabaseOutage,
            Scenario::LoginAcross,
        ] {
            let variant = cluster_variant(scenario, &Variant::baseline(Sandbox::Noop))
                .expect("listed scenarios are cluster scenarios");
            let expected = match scenario {
                Scenario::LeaderFailover => ControlKnobs::LEADER_FAILOVER,
                Scenario::DatabaseOutage => ControlKnobs::DATABASE_OUTAGE,
                _ => ControlKnobs::NONE,
            };
            assert_eq!(variant.control, expected, "{scenario}");
        }
    }

    #[test]
    fn every_scenario_name_round_trips() {
        for scenario in Scenario::ALL.iter().copied() {
            assert_eq!(
                scenario.to_string().parse::<Scenario>().ok(),
                Some(scenario)
            );
        }
    }

    #[test]
    fn scenario_names_are_kebab_case() {
        assert_eq!(Scenario::InputIntegrity.to_string(), "input-integrity");
        assert_eq!(Scenario::InputRoot.to_string(), "input-root");
        assert_eq!(Scenario::Lifecycle.to_string(), "lifecycle");
        assert_eq!(Scenario::Revocation.to_string(), "revocation");
        assert_eq!(Scenario::Sessions.to_string(), "sessions");
        assert_eq!(Scenario::Builds.to_string(), "builds");
    }

    #[test]
    fn launch_failure_is_selectable_only_from_gate_two() {
        assert_eq!(
            "launch_failure".parse::<Scenario>().ok(),
            Some(Scenario::LaunchFailure)
        );
        assert!(
            !Gate::One
                .scenarios()
                .any(|scenario| scenario == Scenario::LaunchFailure)
        );
        assert!(
            Gate::Two
                .scenarios()
                .any(|scenario| scenario == Scenario::LaunchFailure)
        );
    }

    #[test]
    fn upgrades_are_gate_two_only_and_selectable_by_name() {
        for (name, scenario) in [
            ("rolling-upgrade", Scenario::RollingUpgrade),
            (
                "rolling-upgrade-automatic",
                Scenario::RollingUpgradeAutomatic,
            ),
            ("config-refetch-restart", Scenario::ConfigRefetchRestart),
        ] {
            assert_eq!(name.parse::<Scenario>().ok(), Some(scenario));
            assert!(!Gate::One.scenarios().any(|item| item == scenario));
            assert!(Gate::Two.additions().any(|item| item == scenario));
            assert!(scenario.weight() > 0);
        }
    }

    #[test]
    fn unknown_scenario_names_are_rejected() {
        assert!("input_integrity".parse::<Scenario>().is_err());
        assert!("Lifecycle".parse::<Scenario>().is_err());
        assert!("".parse::<Scenario>().is_err());
    }

    #[test]
    fn gates_parse_from_numbers() {
        assert_eq!("1".parse::<Gate>().ok(), Some(Gate::One));
        assert_eq!("2".parse::<Gate>().ok(), Some(Gate::Two));
        assert!("3".parse::<Gate>().is_err());
    }

    #[test]
    fn empty_selection_covers_every_scenario() {
        for scenario in Scenario::ALL.iter().copied() {
            assert!(selected(&[], scenario));
        }
    }

    #[test]
    fn non_empty_selection_covers_only_its_scenarios() {
        let chosen = [Scenario::Network, Scenario::Limits];
        assert!(selected(&chosen, Scenario::Network));
        assert!(selected(&chosen, Scenario::Limits));
        assert!(!selected(&chosen, Scenario::Lifecycle));
        assert!(!selected(&chosen, Scenario::Revocation));
    }

    #[test]
    fn gate_one_runs_every_scenario_but_the_gate_two_ones() {
        let runs: Vec<Scenario> = Gate::One.scenarios().collect();
        let gate_two_only = [
            Scenario::Pressure,
            Scenario::PoolOom,
            Scenario::Revocation,
            Scenario::Dashboard,
            Scenario::LaunchFailure,
            Scenario::Api,
            Scenario::RequestLimits,
            Scenario::ViewerCap,
            Scenario::CertificateRotation,
            Scenario::Identity,
            Scenario::Sessions,
            Scenario::ControllerLoss,
            Scenario::RollingDeploy,
            Scenario::RollingUpgrade,
            Scenario::RollingUpgradeAutomatic,
            Scenario::ConfigRefetchRestart,
            Scenario::CrossRevocation,
            Scenario::LeaderFailover,
            Scenario::LoginAcross,
            Scenario::DatabaseOutage,
            Scenario::Builds,
            Scenario::FollowerBuilds,
            Scenario::ScheduledMinimum,
            Scenario::BurstFromZero,
            Scenario::QueuedBehindShortWork,
            Scenario::BacklogGrowsFleet,
            Scenario::ColdBurstFromZero,
            Scenario::SustainedStream,
            Scenario::TwoSources,
            Scenario::UtilizationScaleIn,
        ];
        for scenario in gate_two_only {
            assert!(!runs.contains(&scenario), "{scenario}");
        }
        assert!(
            Scenario::ALL
                .iter()
                .copied()
                .filter(|scenario| { !gate_two_only.contains(scenario) })
                .all(|scenario| runs.contains(&scenario))
        );
    }

    #[test]
    fn scheduled_minimum_and_work_aware_are_gate_two_additions() {
        assert_eq!(
            "scheduled-minimum".parse::<Scenario>().ok(),
            Some(Scenario::ScheduledMinimum)
        );
        assert!(
            !Gate::One
                .scenarios()
                .any(|scenario| scenario == Scenario::ScheduledMinimum)
        );
        let additions: Vec<_> = Gate::Two.additions().collect();
        assert!(additions.ends_with(&[
            Scenario::ScheduledMinimum,
            Scenario::BurstFromZero,
            Scenario::QueuedBehindShortWork,
            Scenario::BacklogGrowsFleet,
            Scenario::ColdBurstFromZero,
            Scenario::SustainedStream,
            Scenario::TwoSources,
            Scenario::UtilizationScaleIn,
        ]));
    }

    #[test]
    fn gate_two_runs_every_scenario() {
        let runs: Vec<Scenario> = Gate::Two.scenarios().collect();
        assert_eq!(runs, Scenario::ALL.to_vec());
    }

    #[test]
    fn gate_two_additions_complete_gate_one() {
        let additions: Vec<_> = Gate::Two.additions().collect();
        let expected: Vec<_> = Scenario::ALL
            .iter()
            .copied()
            .filter(|scenario| scenario.gate() == Gate::Two)
            .collect();
        assert_eq!(additions, expected);
        let combined: Vec<_> = Gate::One.scenarios().chain(additions).collect();
        assert_eq!(combined, Gate::Two.scenarios().collect::<Vec<_>>());
    }

    #[test]
    fn gate_one_scenarios_are_a_prefix_of_gate_two() {
        let one: Vec<Scenario> = Gate::One.scenarios().collect();
        let two: Vec<Scenario> = Gate::Two.scenarios().collect();
        assert_eq!(two, Scenario::ALL.to_vec());
        assert_eq!(one.as_slice(), &two[..one.len()]);
    }

    #[test]
    fn plaintext_runs_by_default_in_both_gates() {
        assert_eq!(Scenario::Plaintext.gate(), Gate::One);
        for gate in [Gate::One, Gate::Two] {
            assert!(
                gate.scenarios()
                    .any(|scenario| scenario == Scenario::Plaintext)
            );
        }
    }

    #[test]
    fn cli_scenarios_run_in_gate_one() {
        for scenario in [Scenario::Cli, Scenario::CheckedInConfigs] {
            assert_eq!(scenario.gate(), Gate::One);
        }
        assert_eq!("cli".parse::<Scenario>().ok(), Some(Scenario::Cli));
        assert_eq!(
            "checked-in-configs".parse::<Scenario>().ok(),
            Some(Scenario::CheckedInConfigs)
        );
    }

    #[test]
    fn unknown_scenarios_are_those_the_gate_does_not_run() {
        let chosen = [Scenario::Revocation, Scenario::Network];
        assert_eq!(
            unknown_scenarios(Gate::One, &chosen),
            vec![Scenario::Revocation]
        );
        assert!(unknown_scenarios(Gate::Two, &chosen).is_empty());
    }

    #[test]
    fn unknown_scenarios_keep_the_order_given() {
        let chosen = [
            Scenario::Revocation,
            Scenario::Lifecycle,
            Scenario::Revocation,
        ];
        assert_eq!(
            unknown_scenarios(Gate::One, &chosen),
            vec![Scenario::Revocation, Scenario::Revocation]
        );
    }

    #[test]
    fn empty_selection_has_no_unknown_scenarios() {
        assert!(unknown_scenarios(Gate::One, &[]).is_empty());
    }

    #[test]
    fn fleets_scenario_runs_by_default_in_both_gates() {
        for gate in [Gate::One, Gate::Two] {
            assert!(
                gate.scenarios()
                    .any(|scenario| scenario == Scenario::Fleets)
            );
            assert!(covers(gate, &[], Scenario::Fleets));
            assert!(unknown_scenarios(gate, &[Scenario::Fleets]).is_empty());
        }
    }

    #[test]
    fn fleets_scenario_follows_disk_in_every_gate_list() {
        for gate in [Gate::One, Gate::Two] {
            let runs: Vec<Scenario> = gate.scenarios().collect();
            let disk = runs.iter().position(|scenario| *scenario == Scenario::Disk);
            let fleets = runs
                .iter()
                .position(|scenario| *scenario == Scenario::Fleets);
            assert_eq!(fleets, disk.map(|index| index + 1));
        }
    }

    #[test]
    fn memory_scenario_follows_limits_in_every_gate_list() {
        for gate in [Gate::One, Gate::Two] {
            let runs: Vec<Scenario> = gate.scenarios().collect();
            let limits = runs
                .iter()
                .position(|scenario| *scenario == Scenario::Limits);
            let memory = runs
                .iter()
                .position(|scenario| *scenario == Scenario::Memory);
            assert_eq!(memory, limits.map(|index| index + 1));
        }
    }

    #[test]
    fn disk_scenario_follows_memory_in_every_gate_list() {
        for gate in [Gate::One, Gate::Two] {
            let runs: Vec<Scenario> = gate.scenarios().collect();
            let memory = runs
                .iter()
                .position(|scenario| *scenario == Scenario::Memory);
            let disk = runs.iter().position(|scenario| *scenario == Scenario::Disk);
            assert_eq!(disk, memory.map(|index| index + 1));
        }
    }

    #[test]
    fn memory_scenario_is_selectable_in_both_gates() {
        for gate in [Gate::One, Gate::Two] {
            assert!(covers(gate, &[], Scenario::Memory));
            assert!(unknown_scenarios(gate, &[Scenario::Memory]).is_empty());
        }
    }

    #[test]
    fn every_scenario_is_listed_once_in_all() {
        for scenario in Scenario::ALL.iter().copied() {
            assert_eq!(
                Scenario::ALL
                    .iter()
                    .copied()
                    .filter(|other| *other == scenario)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn disk_scenario_is_selectable_in_gate_one() {
        assert!(unknown_scenarios(Gate::One, &[Scenario::Disk]).is_empty());
    }

    #[test]
    fn setup_scenario_is_in_gate_one() {
        assert_eq!(Scenario::Setup.gate(), Gate::One);
        assert!(unknown_scenarios(Gate::One, &[Scenario::Setup]).is_empty());
        assert_eq!("setup".parse::<Scenario>().ok(), Some(Scenario::Setup));
    }

    #[test]
    fn a_named_selection_runs_the_fleets_scenario_alone() {
        let chosen = [Scenario::Fleets];
        assert!(covers(Gate::One, &chosen, Scenario::Fleets));
        assert!(!covers(Gate::One, &chosen, Scenario::Lifecycle));
    }

    #[test]
    fn dashboard_scenario_runs_in_gate_two_only() {
        assert!(
            Gate::Two
                .scenarios()
                .any(|scenario| scenario == Scenario::Dashboard)
        );
        assert!(
            !Gate::One
                .scenarios()
                .any(|scenario| scenario == Scenario::Dashboard)
        );
        assert_eq!(
            unknown_scenarios(Gate::One, &[Scenario::Dashboard]),
            vec![Scenario::Dashboard]
        );
    }

    #[test]
    fn bounded_runs_return_results_in_item_order() {
        let items: Vec<u32> = (0..10).collect();
        let workers = NonZeroUsize::new(3).expect("three is not zero");
        let results = run_bounded(&items, workers, |item| item * 2);
        assert_eq!(
            results,
            items.iter().map(|item| Some(item * 2)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn bounded_runs_never_exceed_the_worker_count() {
        let active = AtomicUsize::new(0);
        let most = AtomicUsize::new(0);
        let items: Vec<u32> = (0..16).collect();
        let workers = NonZeroUsize::new(2).expect("two is not zero");
        run_bounded(&items, workers, |_| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            active.fetch_sub(1, Ordering::SeqCst);
        });
        assert!(most.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    fn a_panicking_item_is_none_and_the_others_still_run() {
        let items: Vec<u32> = (0..5).collect();
        let workers = NonZeroUsize::new(2).expect("two is not zero");
        let results = run_bounded(&items, workers, |item| {
            assert_ne!(*item, 2, "item two panics");
            *item
        });
        assert_eq!(results, vec![Some(0), Some(1), None, Some(3), Some(4)]);
    }

    #[test]
    fn an_empty_list_runs_nothing() {
        let items: Vec<u32> = Vec::new();
        let workers = NonZeroUsize::new(4).expect("four is not zero");
        assert!(run_bounded(&items, workers, |item| *item).is_empty());
    }
}
