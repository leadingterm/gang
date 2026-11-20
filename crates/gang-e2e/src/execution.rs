//! The contract the execution scenarios share: the configuration knobs they vary, what they
//! observe of a deployment beyond [`System`], and the held build they all run.
//!
//! Execution probes use action-local links to a harness-owned journal and persistent release flag.
//! Attempts on different workers share the same underlying inodes.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use gang_config::{ColdStartTuning, EscalationPercent, MemoryTuning};
use gang_types::{Bytes, HeadroomPercent, NonZeroBytes, NonZeroDuration, Percent};

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::system::{Probe, System};

/// `[actions] timeout` under [`ActionTimeout::Short`], in seconds.
/// `kill_grace` keeps its default of five seconds.
pub const SHORT_ACTION_TIMEOUT_SECS: u32 = 5;

/// `[actions.memory] max_steps` under [`MemoryEscalation::OneStep`].
pub const ESCALATION_MAX_STEPS: u32 = 1;

/// `[actions.memory] escalation_percent` under [`MemoryEscalation::OneStep`].
pub const ESCALATION_PERCENT: u32 = 200;

/// The oversized fleet disk, far above the scratch volume of any host.
pub const OVERSIZED_FLEET_DISK: &str = "1024TiB";

/// The relaunch backoff of the oversized fleet, so replacements of refusing instances follow each
/// other quickly.
pub const REFUSAL_RELAUNCH_BACKOFF: &str = "1s";

/// The refusal reported when no fleet accepts an action.
pub const NO_FLEET_ACCEPTS: &str = "no fleet accepts this action";

/// The platform property the disk-guarded probe asks too much of; the refusal names it.
pub const REFUSED_DISK_KEY: &str = "disk_kb";

/// `[actions.cold_start] memory` under [`ColdStart::Misfit`]: above the base fleet's
/// `memory_per_instance`, so no fleet accepts the cold start.
pub const MISFIT_COLD_START_MEMORY: &str = "64GiB";

/// The configuration key named by a cold start that fits no fleet.
pub const COLD_START_MISFIT_MESSAGE: &str = "actions.cold_start";

/// `[authentication.workers.mtls] certificate_ttl` under [`WorkerCertificates::ShortLived`], in
/// seconds.
pub const WORKER_CERTIFICATE_TTL_SECS: u64 = 30;

/// `[worker] certificate_refresh_percent` under [`WorkerCertificates::ShortLived`].
pub const CERTIFICATE_REFRESH_PERCENT: u8 = 50;

/// The line [`Execution::end_lines`] writes to the journal; no probe writes it.
pub const JOURNAL_END: &str = "e2e-journal-end";

/// `[actions] timeout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionTimeout {
    /// The default timeout.
    Default,
    /// [`SHORT_ACTION_TIMEOUT_SECS`].
    Short,
}

/// `[actions.memory] max_steps` and `escalation_percent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryEscalation {
    /// The default escalation.
    Default,
    /// [`ESCALATION_MAX_STEPS`] steps of [`ESCALATION_PERCENT`].
    OneStep,
}

/// The fleet's `disk_per_instance`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetDisk {
    /// The harness's Local Mode fleet disk, `template::FLEET_DISK`.
    Default,
    /// [`OVERSIZED_FLEET_DISK`] with [`REFUSAL_RELAUNCH_BACKOFF`].
    Oversized,
}

/// `[actions.cold_start]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdStart {
    /// The base configuration's cold start.
    Default,
    /// A cold start of [`MISFIT_COLD_START_MEMORY`], which no fleet accepts.
    Misfit,
}

/// The lifetime of the workers' certificates and when they renew them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCertificates {
    /// The base configuration's lifetime and the default refresh point.
    Default,
    /// [`WORKER_CERTIFICATE_TTL_SECS`], refreshed at [`CERTIFICATE_REFRESH_PERCENT`].
    ShortLived,
}

/// Explicit resource policy and worker bound for a resource scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePolicy {
    /// Memory enforcement and pressure tuning.
    pub memory: MemoryTuning,
    /// Headroom applied to measured peaks when escalating or learning.
    pub reservation_headroom_percent: HeadroomPercent,
    /// Generic retry budget, independent of resource requeues.
    pub max_job_retries: NonZeroU32,
    /// Declared fleet memory, when different from the base fleet.
    pub fleet_memory: Option<NonZeroBytes>,
    /// Launcher memory bound for each worker, when required by the scenario.
    pub worker_memory_max: Option<Bytes>,
    /// Cold-start policy override for the fixture.
    pub cold_start: Option<ColdStartTuning>,
    /// Declared CPUs per fleet instance, when overridden.
    pub fleet_vcpus: Option<NonZeroU32>,
}

impl Default for ResourcePolicy {
    fn default() -> Self {
        let actions = gang_config::ActionsConfig::default();
        Self {
            memory: actions.memory,
            reservation_headroom_percent: actions.reservation_headroom_percent,
            max_job_retries: actions.max_job_retries,
            fleet_memory: None,
            worker_memory_max: None,
            cold_start: None,
            fleet_vcpus: None,
        }
    }
}

impl ResourcePolicy {
    /// The bounded, non-default policy used by the neighbour-pressure scenario.
    ///
    /// # Errors
    ///
    /// Returns an error if the fixture's escalation percentage is invalid.
    pub fn pressure_scenario() -> Result<Self> {
        let memory = MemoryTuning {
            reserve: NonZeroBytes::mib(32),
            pressure_stall_percent: Percent::from_percent(7),
            pressure_poll_interval: NonZeroDuration::from_millis(50),
            pressure_kill_after: NonZeroDuration::from_millis(1000),
            sample_interval: NonZeroDuration::from_millis(20),
            escalation_percent: EscalationPercent::try_new(250)
                .map_err(E2eError::system_from("the pressure escalation percentage"))?,
            max_steps: NonZeroU32::MIN.saturating_add(2),
            ..MemoryTuning::default()
        };
        Ok(Self {
            memory,
            reservation_headroom_percent: HeadroomPercent::from_percent(25),
            max_job_retries: NonZeroU32::MIN,
            fleet_memory: Some(NonZeroBytes::mib(512)),
            worker_memory_max: Some(NonZeroBytes::mib(768).get()),
            cold_start: Some(ColdStartTuning {
                enabled: false,
                ..ColdStartTuning::default()
            }),
            fleet_vcpus: Some(NonZeroU32::MIN.saturating_add(1)),
        })
    }

    /// A 480 MiB kernel pool in which both held pressure fixtures cannot coexist.
    ///
    /// # Errors
    ///
    /// Returns an error if the fixture's escalation percentage is invalid.
    pub fn pool_oom_scenario() -> Result<Self> {
        let mut policy = Self::pressure_scenario()?;
        policy.memory.enforcement = gang_config::MemoryEnforcement::None;
        policy.worker_memory_max = Some(NonZeroBytes::mib(512).get());
        policy.fleet_memory = Some(NonZeroBytes::mib(448));
        Ok(policy)
    }
}

/// The configuration knobs of the execution scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionKnobs {
    /// `[actions] timeout`.
    pub action_timeout: ActionTimeout,
    /// `[actions.memory]` escalation.
    pub memory_escalation: MemoryEscalation,
    /// The fleet's disk per instance.
    pub fleet_disk: FleetDisk,
    /// `[actions.cold_start]`.
    pub cold_start: ColdStart,
    /// The workers' certificate lifetime and refresh point.
    pub worker_certificates: WorkerCertificates,
    /// Resource policy override for a scenario.
    pub resource_policy: Option<ResourcePolicy>,
}

impl ExecutionKnobs {
    /// Every knob at the base configuration.
    pub const NONE: Self = Self {
        action_timeout: ActionTimeout::Default,
        memory_escalation: MemoryEscalation::Default,
        fleet_disk: FleetDisk::Default,
        cold_start: ColdStart::Default,
        worker_certificates: WorkerCertificates::Default,
        resource_policy: None,
    };

    /// The knobs of the action timeout scenarios: [`ActionTimeout::Short`].
    pub const SHORT_TIMEOUT: Self = Self {
        action_timeout: ActionTimeout::Short,
        ..Self::NONE
    };

    /// The knobs of the memory escalation scenario: [`MemoryEscalation::OneStep`].
    pub const ONE_STEP_ESCALATION: Self = Self {
        memory_escalation: MemoryEscalation::OneStep,
        ..Self::NONE
    };

    /// The knobs of the oversized fleet scenario: [`FleetDisk::Oversized`].
    pub const OVERSIZED_FLEET: Self = Self {
        fleet_disk: FleetDisk::Oversized,
        ..Self::NONE
    };

    /// The knobs of the misfit cold start scenario: [`ColdStart::Misfit`].
    pub const COLD_START_MISFIT: Self = Self {
        cold_start: ColdStart::Misfit,
        ..Self::NONE
    };

    /// The knobs of the worker renewal scenario: [`WorkerCertificates::ShortLived`].
    pub const SHORT_LIVED_CERTIFICATES: Self = Self {
        worker_certificates: WorkerCertificates::ShortLived,
        ..Self::NONE
    };

    /// Every set of knobs an execution scenario runs under, [`Self::NONE`] first.
    pub const PRESETS: &[Self] = &[
        Self::NONE,
        Self::SHORT_TIMEOUT,
        Self::ONE_STEP_ESCALATION,
        Self::OVERSIZED_FLEET,
        Self::COLD_START_MISFIT,
        Self::SHORT_LIVED_CERTIFICATES,
    ];
}

/// What a process of the deployment is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// A `gang worker` of the deployment.
    Worker,
    /// A `gang exec` of one of its workers.
    GangExec,
    /// The guard of a `gang exec`, identified by its creator's process id.
    GangGuard {
        /// Process id of the `gang exec` that started the guard.
        creator: u32,
    },
    /// A `bwrap` one of its `gang exec`s started.
    Bwrap,
    /// A probe's process whose arguments carry the journal's path and this `e2e-` marker word.
    Marked(String),
}

/// One process of the deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proc {
    /// The process id, in the harness's namespace.
    pub pid: u32,
    /// What the process is.
    pub role: Role,
    /// The absolute directory of its cgroup, or `None` where there are no cgroups.
    pub cgroup: Option<PathBuf>,
    /// The Local Mode instance a [`Role::GangExec`] runs on, from its entrypoint path
    /// (`<state_dir>/instances/<instance>/...`); `None` for the other roles.
    pub instance: Option<String>,
}

/// How a `gang server` that was expected to refuse its configuration ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerExit {
    /// Whether it announced a listener before its output closed.
    pub announced: bool,
    /// Its exit code; `None` when it was killed by a signal, or killed by the harness after it
    /// announced.
    pub code: Option<i32>,
    /// Its log.
    pub log: String,
}

/// Identity generations observed at a particular wall-clock time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generations {
    /// The generations held by the worker, in ascending order.
    pub held: Vec<u64>,
    /// The wall clock after the generations were listed.
    pub read_at: SystemTime,
}

/// The deadline halfway between worker refresh and certificate expiry.
///
/// The worker computes its refresh time from `not_after` in `gang-worker`'s `acquire`.
#[must_use]
pub fn renewal_deadline(not_after: SystemTime) -> SystemTime {
    not_after
        - Duration::from_secs(WORKER_CERTIFICATE_TTL_SECS)
            * u32::from(100 - CERTIFICATE_REFRESH_PERCENT)
            / 200
}

/// What the execution scenarios observe of a deployment beyond [`System`].
///
/// Methods take `&self` and the trait is `Sync`, so the thread that runs a build can end the
/// journal while a scenario waits on it.
pub trait Execution: Sync {
    /// Blocks for the next line of the journal. Returns `None` for the [`JOURNAL_END`] line.
    ///
    /// # Errors
    ///
    /// When the journal cannot be read.
    fn next_line(&self) -> Result<Option<String>>;

    /// Writes [`JOURNAL_END`] to the journal, so that a pending or future
    /// [`next_line`](Self::next_line) returns `None`.
    ///
    /// # Errors
    ///
    /// When the journal cannot be written.
    fn end_lines(&self) -> Result<()>;

    /// Creates (`true`) or removes (`false`) the flag file that releases the held probes. Removing
    /// an absent flag is not an error.
    ///
    /// # Errors
    ///
    /// When the flag cannot be created or removed.
    fn set_go(&self, go: bool) -> Result<()>;

    /// The live processes of the deployment: workers, their `gang exec`s, guards, `bwrap`s, and
    /// marked probe processes. Zombies are not live.
    ///
    /// # Errors
    ///
    /// When the process table or a process's cgroup cannot be read.
    fn processes(&self) -> Result<Vec<Proc>>;

    /// The process ids `cgroup.procs` of `cgroup` lists; empty when the cgroup is gone.
    ///
    /// # Errors
    ///
    /// When the cgroup exists and its processes cannot be read.
    fn cgroup_members(&self, cgroup: &Path) -> Result<Vec<u32>>;

    /// The child cgroups of `cgroup`, as absolute directories; empty when it is gone.
    ///
    /// # Errors
    ///
    /// When the cgroup exists and cannot be listed.
    fn cgroup_children(&self, cgroup: &Path) -> Result<Vec<PathBuf>>;

    /// Whether the cgroup directory `cgroup` exists.
    ///
    /// # Errors
    ///
    /// When the filesystem cannot be queried.
    fn cgroup_exists(&self, cgroup: &Path) -> Result<bool>;

    /// Sends `SIGKILL` to the process `pid` alone, not to its group; a process that is gone is
    /// not an error.
    ///
    /// # Errors
    ///
    /// When the signal cannot be sent.
    fn kill(&self, pid: u32) -> Result<()>;

    /// Waits at most `timeout` for `pid` to exit, returning whether it is gone.
    ///
    /// # Errors
    ///
    /// When the process cannot be observed or the wait fails.
    fn await_gone(&self, pid: u32, timeout: Duration) -> Result<bool>;

    /// Freezes (`frozen`) or thaws the cgroup tree at `cgroup` through its `cgroup.freeze`, then
    /// waits at most `timeout` until its `cgroup.events` reports that state, returning whether it
    /// did. A cgroup that is gone is not an error: it reports `false`.
    ///
    /// # Errors
    ///
    /// When the cgroup exists and its `cgroup.freeze` or `cgroup.events` cannot be written,
    /// watched or read.
    fn freeze(&self, cgroup: &Path, frozen: bool, timeout: Duration) -> Result<bool>;

    /// The names of the environment variables of the worker `worker` that the harness's own
    /// environment lacks: what the worker's launch added, such as its instance secret.
    ///
    /// # Errors
    ///
    /// When the worker's environment cannot be read.
    fn worker_only_environment(&self, worker: u32) -> Result<Vec<String>>;

    /// The `gang-action-*` directories under the noop sandbox's scratch directory.
    ///
    /// # Errors
    ///
    /// When the scratch directory exists and cannot be listed.
    fn temp_dirs(&self) -> Result<Vec<PathBuf>>;

    /// Whether `path` exists.
    ///
    /// # Errors
    ///
    /// When the filesystem cannot be queried.
    fn path_exists(&self, path: &Path) -> Result<bool>;

    /// Writes a spot interruption notice for the Local Mode instance `instance`, terminating now,
    /// which the mock metadata service then reports to the instance's worker.
    ///
    /// # Errors
    ///
    /// When the instance has no record or the notice cannot be written.
    fn request_preemption(&self, instance: &str) -> Result<()>;

    /// The identity generations the worker of `instance` holds (`identity/<n>`), ascending.
    ///
    /// # Errors
    ///
    /// When the identity directory exists and cannot be listed.
    fn identity_generations(&self, instance: &str) -> Result<Vec<u64>>;

    /// The expiration time of `generation`'s certificate for `instance`.
    ///
    /// # Errors
    ///
    /// When the certificate is absent or cannot be parsed.
    fn certificate_not_after(&self, instance: &str, generation: u64) -> Result<SystemTime>;

    /// Waits once until `at`, then lists the worker's identity generations.
    ///
    /// # Errors
    ///
    /// When the identity directory cannot be listed.
    fn generations_at(&self, instance: &str, at: SystemTime) -> Result<Generations>;

    /// Starts `gang server` on the deployment's configuration, expecting it to refuse it, and
    /// returns how it ended. A server that announces a listener and keeps running is killed.
    ///
    /// # Errors
    ///
    /// When the server cannot be started or its log cannot be read.
    fn start_refused_server(&self) -> Result<ServerExit>;

    /// The last bytes of the server's log, as many as the settings' log tail allows; empty when
    /// there is no log yet.
    ///
    /// # Errors
    ///
    /// When the log exists and cannot be read.
    fn server_log_tail(&self) -> Result<String>;

    /// Everything the deployment's Local Mode workers logged (`<state_dir>/logs/*.log`),
    /// concatenated in name order; empty when none logged.
    ///
    /// # Errors
    ///
    /// When a log exists and cannot be read.
    fn worker_logs(&self) -> Result<String>;
}

/// One held build's identity. Its actions are distinct from those of every other build, so no
/// earlier result or attempt stands in for them, and every journal line its probes write starts
/// with it and a space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run(String);

impl Run {
    /// A run no other build has: 16 hex digits from the operating system's randomness.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::System`] when the operating system cannot provide randomness.
    pub fn fresh() -> Result<Self> {
        crate::sample::salt().map(Self)
    }
}

impl AsRef<str> for Run {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The journal of one held build, read by the scenario while the build runs.
pub struct Lines<'e, E: Execution> {
    exec: &'e E,
    run: &'e Run,
    ended: bool,
}

impl<E: Execution> Lines<'_, E> {
    /// Blocks for the next journal line of this run whose text starts with `prefix`, skipping the
    /// others, and returns its text without the run. Lines of other runs are skipped too: an
    /// earlier build's attempt can write after that build ended. Returns `None` once the build
    /// ended without such a line.
    ///
    /// # Errors
    ///
    /// When the journal cannot be read.
    pub fn await_prefix(&mut self, prefix: &str) -> Result<Option<String>> {
        while !self.ended {
            match self.exec.next_line()? {
                Some(line) => {
                    if let Some(text) = self.text_of(&line)
                        && text.starts_with(prefix)
                    {
                        return Ok(Some(text.to_owned()));
                    }
                }
                None => self.ended = true,
            }
        }
        Ok(None)
    }

    /// The text of `line` when this run wrote it: the line without its run and the space after.
    fn text_of<'l>(&self, line: &'l str) -> Option<&'l str> {
        line.strip_prefix(self.run.as_ref())?.strip_prefix(' ')
    }

    /// Reads the journal up to the end of the build, so the next build starts on an empty one.
    fn drain(&mut self) -> Result<()> {
        while !self.ended {
            if self.exec.next_line()?.is_none() {
                self.ended = true;
            }
        }
        Ok(())
    }
}

/// Releases, kills, and drains a held build whose observation unwinds, so that the scope can join
/// its thread instead of waiting on a build no one releases or reads.
struct UnwindGuard<'e, S: System, E: Execution> {
    system: &'e S,
    lines: Lines<'e, E>,
}

impl<S: System, E: Execution> Drop for UnwindGuard<'_, S, E> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = self.lines.exec.set_go(true);
            let _ = self.system.kill_server();
            let _ = self.lines.drain();
        }
    }
}

/// Builds `probes` as a fresh [`Run`] on a thread while `during` observes the build through that
/// run's journal lines, releases the go flag after `during` returns, panics, or fails, then drains
/// and joins the build.
///
/// The build thread ends the journal when its build returns, so a wait of `during` for a line
/// that never comes ends with the build, and so does the final read. The journal is read to its
/// end before the build is joined, so a deployment that advances a held action only as its
/// journal is read (a simulation) can finish it. When `during` or the release fails, the server
/// is killed at best effort so that a held build fails at once; that kill and the build failure
/// it induces never hide the error that caused them.
///
/// # Errors
///
/// The first error of `during`, the release, the final read, and the build, in that order, and
/// [`E2eError::System`] when the build thread panicked or no run can be drawn.
pub fn held_build<S: System, E: Execution, T>(
    system: &S,
    exec: &E,
    probes: &[Probe],
    during: impl FnOnce(&mut Lines<'_, E>) -> Result<T>,
) -> Result<(BuildOutcome, T)> {
    let run = Run::fresh()?;
    std::thread::scope(|scope| {
        let build = scope.spawn(|| {
            let outcome = system.build_run(probes, &run);
            exec.end_lines()?;
            outcome
        });
        let mut guard = UnwindGuard {
            system,
            lines: Lines {
                exec,
                run: &run,
                ended: false,
            },
        };
        let observed = during(&mut guard.lines);
        let released = exec.set_go(true);
        if observed.is_err() || released.is_err() {
            let _ = system.kill_server();
        }
        let drained = guard.lines.drain();
        let joined = build.join();
        let value = observed?;
        released?;
        drained?;
        let outcome = joined.map_err(|_| E2eError::system("the build thread panicked"))??;
        Ok((outcome, value))
    })
}

/// The scope of the action the first live `gang exec` runs, if one runs.
///
/// # Errors
///
/// The errors of [`Execution::processes`] and [`scope_of`].
pub fn running_scope<E: Execution>(exec: &E) -> Result<Option<PathBuf>> {
    let processes = exec.processes()?;
    let Some(process) = processes
        .iter()
        .find(|process| process.role == Role::GangExec)
    else {
        return Ok(None);
    };
    scope_of(exec, process)
}

/// The scope `gang_exec` created for its action: the child of `<subtree>/actions` whose name
/// starts with `<pid>-`, where `<subtree>` holds the worker leaf `gang exec` runs in. A `gang
/// exec` caught inside its own scope (`<subtree>/actions/<scope>`) yields that scope.
///
/// # Errors
///
/// The errors of [`Execution::cgroup_children`].
pub fn scope_of<E: Execution>(exec: &E, gang_exec: &Proc) -> Result<Option<PathBuf>> {
    let Some(cgroup) = &gang_exec.cgroup else {
        return Ok(None);
    };
    let Some(parent) = cgroup.parent() else {
        return Ok(None);
    };
    if parent.file_name().is_some_and(|name| name == ACTIONS_DIR) {
        return Ok(Some(cgroup.clone()));
    }
    let prefix = format!("{}-", gang_exec.pid);
    Ok(exec
        .cgroup_children(&parent.join(ACTIONS_DIR))?
        .into_iter()
        .find(|scope| {
            scope
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        }))
}

/// The directory of the action scopes beside a worker leaf.
pub const ACTIONS_DIR: &str = "actions";

/// The name of a worker's own leaf cgroup.
pub const WORKER_LEAF: &str = "worker";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_deadline_is_halfway_between_refresh_and_expiry() {
        let not_after = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        assert_eq!(
            renewal_deadline(not_after),
            not_after - Duration::from_millis(7_500)
        );
    }
}
