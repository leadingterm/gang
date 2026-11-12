//! The suite: the scenarios of one gate, each run against a fresh deployment, and their verdicts.

use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use gang_config::InputLinks;
use parking_lot::Mutex;

use crate::access::{IdentityKnobs, identity};
use crate::admission::{cold_start_misfit, memory_escalation, oversized_fleet, unplaceable_disk};
use crate::check::{Check, Report};
use crate::checked_in::{ShippedConfigs, checked_in_configs};
use crate::cli::cli;
use crate::cli_terminal::LocalTerminal;
use crate::control::ControlKnobs;
use crate::dashboard::dashboard;
use crate::disk::disk;
use crate::drain_cancel::drain_cancel;
use crate::errors::Result;
use crate::execution::ExecutionKnobs;
use crate::fleets::fleets;
use crate::integrity::input_integrity;
use crate::issuer::TestIssuer;
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
use crate::secured::Secured;
use crate::server::ServerKnobs;
use crate::server_scenarios::{
    api, certificate_rotation, event_paging, request_limits, viewer_cap,
};
use crate::settings::{ActionTools, Settings};
use crate::skipped_pass::skipped_pass;
use crate::stack::Stack;
use crate::stale_scope::stale_scope;
use crate::system::{
    ActionLimits, FleetLayout, PermitNetwork, Sandbox, SoftMemory, System, Variant,
};
use crate::template::Transport;
use crate::timeout::{action_timeout, exit_signals};

/// The configuration of one harness run.
#[derive(Debug, Clone)]
pub struct Invocation {
    /// The `gang` binary under test.
    pub gang: PathBuf,
    /// The Bazel binary the harness drives.
    pub bazel: PathBuf,
    /// The base `gang.toml` the variants are applied to.
    pub base_config: String,
    /// The sample workspace the builds run in.
    pub sample: PathBuf,
    /// The directory of the checked-in gang.toml files.
    pub configs: PathBuf,
    /// The tunables of the harness.
    pub settings: Settings,
    /// The resolved tools used by the sample's actions.
    pub action_tools: ActionTools,
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
            "1" | "one" => Ok(Self::One),
            "2" | "two" => Ok(Self::Two),
            other => Err(UnknownGate(other.to_owned())),
        }
    }
}

/// Text that names no gate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown gate `{0}`; expected 1, 2, one or two")]
pub struct UnknownGate(pub String);

gang_types::string_enum! {
    /// One scenario of the suite, selectable by the name [`as_str`](Scenario::as_str) returns.
    pub enum Scenario("scenario") {
        /// A fleet starts empty, scales out for a cold build, drains, and serves a warm rebuild from
        /// the cache.
        Lifecycle = "lifecycle",
        /// An action that requests the network shares the host's network exactly when permitted.
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
        /// A revoked certificate's connection is closed and new ones are refused. Gate two only.
        Revocation = "revocation",
        /// A session cookie opens the live state stream, requests without one are refused, and
        /// revoking the principal closes the open stream. Gate two only.
        Dashboard = "dashboard",
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
    }
}

impl Scenario {
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
            | Self::Plaintext => Gate::One,
            Self::Revocation
            | Self::Dashboard
            | Self::Api
            | Self::RequestLimits
            | Self::ViewerCap
            | Self::CertificateRotation
            | Self::Identity => Gate::Two,
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
    let issuer = match TestIssuer::start(&invocation.settings) {
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
        invocation,
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
        invocation.settings.parallel_scenarios,
        |scenario| {
            let started = Instant::now();
            let checks = scenario.run(&ctx);
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

/// What a scenario run starts from: the invocation, the in-process issuer, the sandbox, and the
/// baseline variant that the scenarios derive their variants from.
struct RunContext<'a> {
    invocation: &'a Invocation,
    issuer: &'a TestIssuer,
    sandbox: Sandbox,
    baseline: Variant,
}

impl Scenario {
    /// The verdicts of this scenario on fresh deployments. This match is the only mapping from a
    /// scenario to its behaviour.
    fn run(self, ctx: &RunContext<'_>) -> Vec<Check> {
        match self {
            Self::Lifecycle => attempt(self, || {
                let stack =
                    Stack::create(ctx.invocation, &ctx.baseline, Transport::Tls, ctx.issuer)?;
                lifecycle(&stack, &ctx.baseline).map(|checks| with_event_paging(checks, &stack))
            }),
            Self::Network => network_checks(ctx),
            Self::InputIntegrity => input_integrity_checks(ctx),
            Self::Restart => restart_checks(ctx),
            Self::Limits => limits_checks(ctx),
            Self::Memory => memory_checks(ctx),
            Self::Disk => disk_checks(ctx),
            Self::Fleets => {
                let variant = Variant {
                    fleets: FleetLayout::Placement,
                    ..ctx.baseline
                };
                attempt(self, || {
                    let stack =
                        Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
                    fleets(&stack, &variant)
                })
            }
            Self::ActionTimeout => execution_checks(ctx, ExecutionScenario::ActionTimeout),
            Self::ActionTimeoutNoop => execution_checks(ctx, ExecutionScenario::ActionTimeoutNoop),
            Self::SandboxContract => execution_checks(ctx, ExecutionScenario::SandboxContract),
            Self::StaleScope => execution_checks(ctx, ExecutionScenario::StaleScope),
            Self::MemoryEscalation => execution_checks(ctx, ExecutionScenario::MemoryEscalation),
            Self::UnplaceableDisk => execution_checks(ctx, ExecutionScenario::UnplaceableDisk),
            Self::OversizedFleet => execution_checks(ctx, ExecutionScenario::OversizedFleet),
            Self::ColdStartMisfit => execution_checks(ctx, ExecutionScenario::ColdStartMisfit),
            Self::Preemption => execution_checks(ctx, ExecutionScenario::Preemption),
            Self::WorkerRenewal => execution_checks(ctx, ExecutionScenario::WorkerRenewal),
            Self::Orphan => control_checks(self, ctx, ControlKnobs::ORPHAN, orphan),
            Self::DrainCancel => {
                control_checks(self, ctx, ControlKnobs::DRAIN_CANCEL, drain_cancel)
            }
            Self::SkippedPass => {
                control_checks(self, ctx, ControlKnobs::SKIPPED_PASS, skipped_pass)
            }
            Self::Cli => attempt(self, || {
                let terminal = LocalTerminal::create(ctx.invocation)?;
                cli(&terminal)
            }),
            Self::CheckedInConfigs => attempt(self, || {
                checked_in_configs(&ShippedConfigs::new(ctx.invocation))
            }),
            Self::Plaintext => attempt(self, || {
                let stack =
                    Stack::create(ctx.invocation, &ctx.baseline, Transport::Plain, ctx.issuer)?;
                plaintext(&stack)
            }),
            Self::Revocation => attempt(self, || {
                let secured = Secured::create(ctx.invocation, &ctx.baseline, ctx.issuer)?;
                revocation(&secured)
            }),
            Self::Dashboard => attempt(self, || {
                let secured = Secured::create(ctx.invocation, &ctx.baseline, ctx.issuer)?;
                dashboard(&secured, ctx.invocation.settings.history_timeout)
            }),
            Self::Api => attempt(self, || {
                api(
                    &server_deployment(ctx, ServerKnobs::NONE, IdentityKnobs::NONE)?,
                    ctx.invocation.settings.history_timeout,
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
                    ctx.invocation.settings.history_timeout,
                )
            }),
            Self::CertificateRotation => attempt(self, || {
                certificate_rotation(&server_deployment(
                    ctx,
                    ServerKnobs::SHORT_CERTIFICATE,
                    IdentityKnobs::NONE,
                )?)
            }),
            Self::Identity => attempt(self, || {
                let variant = Variant {
                    identity: IdentityKnobs::IDENTITY,
                    ..ctx.baseline
                };
                let secured = Secured::create(ctx.invocation, &variant, ctx.issuer)?;
                identity(&secured)
            }),
        }
    }
}

/// The verdicts of the network scenario, or one skipped check where the sandbox does not isolate.
fn network_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    match ctx.sandbox {
        Sandbox::Bwrap => {
            let permitted = Variant {
                permit_network: PermitNetwork::Permitted,
                ..ctx.baseline
            };
            attempt(Scenario::Network, || {
                let stack = Stack::create(ctx.invocation, &permitted, Transport::Tls, ctx.issuer)?;
                network(&stack, &permitted)
            })
        }
        Sandbox::Noop => vec![Check::skipped(
            names::network::PERMITTED_REQUEST_SHARES,
            "the noop sandbox does not isolate",
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
                let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
                input_integrity(&stack, &variant)
            },
        ));
    }
    checks
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
        // cgroup. `NativeLink`'s soft memory enforcement is off, so only the kernel's
        // `memory.max` can end the memory hog.
        let variant = Variant {
            sandbox: Sandbox::Noop,
            limits: ActionLimits::Cgroup,
            soft_memory: SoftMemory::Off,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
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
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
        restart(&stack)
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
        // `NativeLink`'s soft enforcement stays on: the grower's first attempt ends only by
        // its kill.
        let variant = Variant {
            sandbox: Sandbox::Noop,
            limits: ActionLimits::Cgroup,
            ..Variant::baseline(Sandbox::Noop)
        };
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
        memory(&stack, stack.stderr_tail_lines())
    })
}

/// The verdicts of the disk scenario, on one fresh deployment. The disk sampler is checked only
/// where the platform has one.
fn disk_checks(ctx: &RunContext<'_>) -> Vec<Check> {
    attempt(Scenario::Disk, || {
        let variant = Variant::baseline(Sandbox::native());
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
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
fn execution_variant(scenario: ExecutionScenario, baseline: Variant) -> (Variant, bool) {
    let scoped = |sandbox: Sandbox, execution: ExecutionKnobs, soft_memory: SoftMemory| Variant {
        sandbox,
        limits: ActionLimits::Cgroup,
        soft_memory,
        execution,
        ..Variant::baseline(sandbox)
    };
    let knob = |execution: ExecutionKnobs| Variant {
        execution,
        ..baseline
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
        ExecutionScenario::UnplaceableDisk | ExecutionScenario::Preemption => (baseline, false),
    }
}

/// The verdicts of an execution scenario on its own deployment, or one skipped check where the
/// scenario needs Linux and the platform is another.
fn execution_checks(ctx: &RunContext<'_>, scenario: ExecutionScenario) -> Vec<Check> {
    let (variant, linux_only) = execution_variant(scenario, ctx.baseline);
    if linux_only && !cfg!(target_os = "linux") {
        return vec![Check::skipped(
            names::suite::scenario_completes(scenario.scenario()),
            "bwrap, cgroups and /proc exist only on Linux",
        )];
    }
    attempt(scenario.scenario(), || {
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
        let exec = Observer::new(&stack, &ctx.invocation.settings)?;
        let tail = stack.stderr_tail_lines();
        match scenario {
            ExecutionScenario::ActionTimeout => action_timeout(&stack, &exec, Sandbox::Bwrap),
            ExecutionScenario::ActionTimeoutNoop => {
                let mut checks = action_timeout(&stack, &exec, Sandbox::Noop)?;
                checks.extend(exit_signals(&stack, tail)?);
                Ok(checks)
            }
            ExecutionScenario::SandboxContract => sandbox_contract(&stack, &exec, tail),
            ExecutionScenario::StaleScope => {
                stale_scope(&stack, &exec, tail, ctx.invocation.settings.hold_timeout)
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
        let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
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
    let stack = Stack::create(ctx.invocation, &variant, Transport::Tls, ctx.issuer)?;
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
    use super::*;

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
        assert_eq!(Scenario::Lifecycle.to_string(), "lifecycle");
        assert_eq!(Scenario::Revocation.to_string(), "revocation");
    }

    #[test]
    fn unknown_scenario_names_are_rejected() {
        assert!("input_integrity".parse::<Scenario>().is_err());
        assert!("Lifecycle".parse::<Scenario>().is_err());
        assert!("".parse::<Scenario>().is_err());
    }

    #[test]
    fn gates_parse_from_numbers_and_words() {
        assert_eq!("1".parse::<Gate>().ok(), Some(Gate::One));
        assert_eq!("one".parse::<Gate>().ok(), Some(Gate::One));
        assert_eq!("2".parse::<Gate>().ok(), Some(Gate::Two));
        assert_eq!("two".parse::<Gate>().ok(), Some(Gate::Two));
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
            Scenario::Revocation,
            Scenario::Dashboard,
            Scenario::Api,
            Scenario::RequestLimits,
            Scenario::ViewerCap,
            Scenario::CertificateRotation,
            Scenario::Identity,
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
