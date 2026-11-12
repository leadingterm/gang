//! What a scenario sees of the system under test, as one trait, so that the scenarios run
//! against the real `gang` binary ([`Stack`](crate::Stack)) and against a simulation in tests.

use std::fmt;
use std::path::{Path, PathBuf};

use gang_config::InputLinks;
use gang_wire::{Event, Frame, State};

use crate::access::IdentityKnobs;
use crate::build_output::BuildOutcome;
use crate::control::ControlKnobs;
use crate::errors::Result;
use crate::execution::ExecutionKnobs;
use crate::host::Host;
use crate::report::ProbeReport;
use crate::server::ServerKnobs;

/// The sandbox the workers run actions under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sandbox {
    /// Runs the action directly (macOS).
    Noop,
    /// Runs the action under bubblewrap (Linux).
    Bwrap,
}

impl Sandbox {
    /// The sandbox of this platform: `bwrap` on Linux, `noop` elsewhere.
    #[must_use]
    pub const fn native() -> Self {
        if cfg!(target_os = "linux") {
            Self::Bwrap
        } else {
            Self::Noop
        }
    }

    /// The key of its `[sandbox.<key>]` section and its `sandbox` value in a `[[fleets]]` entry.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Bwrap => "bwrap",
        }
    }
}

impl fmt::Display for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.key())
    }
}

/// Whether `[sandbox.bwrap] permit_network_property` is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitNetwork {
    /// An action never shares the host network.
    Denied,
    /// An action that requests the network shares the host network.
    Permitted,
}

/// Memory of the `memory_kb` platform property of the limit probes, in KiB.
pub const HOLD_MEMORY_KB: u64 = 65_536;

/// The `cpu_count` platform property of the limit probes.
pub const HOLD_CPU_COUNT: u32 = 1;

/// The `[limits.cgroup] memory_headroom_percent` the workers are configured with.
pub const LIMIT_HEADROOM_PERCENT: u32 = 100;

/// The `[limits.cgroup] max_pids` the workers are configured with.
pub const LIMIT_MAX_PIDS: u32 = 4096;

/// The `[limits.cgroup] cpu_period` the workers are configured with, in microseconds (`100ms`).
pub const LIMIT_CPU_PERIOD_MICROS: u64 = 100_000;

/// Whether the workers enforce per-action limits (`[limits.cgroup]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionLimits {
    /// The workers set no per-action limits.
    Absent,
    /// The workers run every action in a cgroup with memory, CPU and pid limits.
    Cgroup,
}

/// Whether `NativeLink`'s soft memory enforcement runs beside the kernel's memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftMemory {
    /// `NativeLink` samples each action's memory and kills one that outgrows its reservation, so
    /// the scheduler requeues it on a larger one: its default.
    Enforced,
    /// `[scheduler.nativelink.memory] enforcement = "none"`: `NativeLink` never ends an action
    /// for its memory, so only the kernel limit can.
    Off,
}

/// The first fleet of [`FleetLayout::Placement`]: the base fleet's size, one instance at most.
pub const SMALL_FLEET: &str = "local-small";

/// The second fleet of [`FleetLayout::Placement`]: twice the size of [`SMALL_FLEET`], the same
/// sandbox, two instances at most.
pub const LARGE_FLEET: &str = "local-large";

/// The third fleet of [`FleetLayout::Placement`]: the size of [`SMALL_FLEET`], the noop sandbox,
/// one instance at most.
pub const NOOP_FLEET: &str = "local-noop";

/// The `[[fleets]]` a deployment declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetLayout {
    /// The one fleet of the base configuration.
    Single,
    /// Three fleets in preference order: [`SMALL_FLEET`], [`LARGE_FLEET`] and [`NOOP_FLEET`].
    Placement,
}

/// The configuration knobs the scenarios vary; everything else comes from the base `gang.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Variant {
    /// The sandbox of the workers.
    pub sandbox: Sandbox,
    /// `[sandbox.bwrap] permit_network_property`.
    pub permit_network: PermitNetwork,
    /// `[execution] input_links`.
    pub input_links: InputLinks,
    /// Whether the workers enforce per-action limits (`[limits.cgroup]`).
    pub limits: ActionLimits,
    /// The `[[fleets]]` the deployment declares.
    pub fleets: FleetLayout,
    /// Whether `NativeLink`'s soft memory enforcement runs beside the kernel limit.
    pub soft_memory: SoftMemory,
    /// The identity family's knobs.
    pub identity: IdentityKnobs,
    /// The control-loop keys of the control scenarios.
    pub control: ControlKnobs,
    /// The server family's `[server]`, `[server.rate_limit]` and `[dashboard]` knobs.
    pub server: ServerKnobs,
    /// The knobs of the execution scenarios.
    pub execution: ExecutionKnobs,
}

impl Variant {
    /// The configuration most scenarios run under: no network permission, input links broken.
    #[must_use]
    pub const fn baseline(sandbox: Sandbox) -> Self {
        Self {
            sandbox,
            permit_network: PermitNetwork::Denied,
            input_links: InputLinks::Break,
            limits: ActionLimits::Absent,
            fleets: FleetLayout::Single,
            soft_memory: SoftMemory::Enforced,
            identity: IdentityKnobs::NONE,
            control: ControlKnobs::NONE,
            server: ServerKnobs::NONE,
            execution: ExecutionKnobs::NONE,
        }
    }
}

gang_types::string_enum! {
    /// A target of the sample workspace, each an action that reports what it observed.
    pub enum Probe("probe") {
        /// Records the net and mnt namespaces, the visibility of a stand-in for the CAS, and whether
        /// its working directory is writable.
        Isolated = "isolated",
        /// Records the net and mnt namespaces; requests `network = requested`.
        NetworkRequested = "network_requested",
        /// Records that it ran; sets the `ISA` exec property explicitly.
        Pinned = "pinned",
        /// Runs `sh -c 'exit 3'`.
        ExitThree = "exit_three",
        /// Tries to modify its cached input in place.
        Poison = "poison",
        /// Runs after [`Probe::Poison`] and records whether the shared input was modified.
        Victim = "victim",
        /// Runs in its own cgroup scope and waits until released, leaving an orphan process behind.
        Hold = "hold",
        /// Allocates far beyond its hard memory ceiling.
        MemoryHog = "memory_hog",
        /// Grows to a plateau above the soft limit of its reservation and below the kernel's ceiling,
        /// and holds it until `NativeLink`'s sampler kills it; the scheduler requeues it with a
        /// doubled reservation, on which it completes.
        MemoryGrower = "memory_grower",
        /// Writes 64 MiB under its working directory, past its disk reservation, and waits to be
        /// killed by the worker.
        DiskHog = "disk_hog",
        /// Writes 7.5 MiB in steps while the disk hog runs, then reports whether its file is intact.
        DiskNeighbour = "disk_neighbour",
        /// Records the net and mnt namespaces; sets no exec property but the ISA.
        Unconstrained = "fleet_unconstrained",
        /// Records the net and mnt namespaces; requests `fleet = local-large`.
        FleetRequested = "fleet_requested",
        /// Records the net and mnt namespaces; requests `memory_kb` above the first fleet's size.
        MemoryRequested = "memory_requested",
        /// Records the net and mnt namespaces; requests `sandbox = noop`.
        SandboxRequested = "sandbox_requested",
        /// Records that it ran; sets no exec property but the ISA. Is built beside
        /// [`Probe::OverflowSecond`].
        OverflowFirst = "overflow_first",
        /// Records that it ran; sets no exec property but the ISA. Is built beside
        /// [`Probe::OverflowFirst`].
        OverflowSecond = "overflow_second",
        /// Outgrows the soft limit of every reservation it is given and holds, so every attempt
        /// is killed; appends one line per attempt to the attempts file.
        MemoryEscalation = "memory_escalation",
        /// Reserves 512 TiB of disk, more than any host has free.
        DiskGuarded = "disk_guarded",
        /// Starts a background child that ignores SIGTERM, then waits forever.
        TimeoutChild = "timeout_child",
        /// Waits forever; on SIGTERM it reports to the journal and exits 0 a second later.
        TermTrap = "term_trap",
        /// Waits forever and keeps running after SIGTERM.
        TermIgnorer = "term_ignorer",
        /// Sends itself SIGKILL.
        SelfKill = "self_kill",
        /// Sends itself SIGTERM.
        SelfTerm = "self_term",
        /// Records its environment's variable names, its /tmp, whether each mount point is
        /// writable, and whether its working directory is writable.
        SandboxContract = "sandbox_contract",
        /// Leaves a process in its own process group behind and waits for the go flag.
        Stray = "stray",
        /// Reports to the journal and waits for the go flag.
        HeldRelease = "held_release",
        /// Reports to the journal, waits for the go flag, then allocates far beyond its hard
        /// memory ceiling.
        HeldHog = "held_hog",
    }
}
impl Probe {
    /// The Bazel label of the target.
    #[must_use]
    pub fn label(self) -> String {
        format!("//:{}", self.as_str())
    }

    /// The path of the target's report under the workspace, through the `bazel-bin` symlink.
    #[must_use]
    pub fn report_path(self) -> String {
        format!("bazel-bin/{}.report", self.as_str())
    }
}

/// A running Gang deployment under one [`Variant`], and the Bazel workspace that uses it.
///
/// Methods take `&self` and the trait is `Sync`, so a scenario can run a build on one thread
/// while it kills and restarts the server on another.
pub trait System: Sync {
    /// Starts `gang server` and returns once its client listener accepts connections.
    ///
    /// # Errors
    ///
    /// When the server cannot be started or does not start listening.
    fn start_server(&self) -> Result<()>;

    /// Kills `gang server` with `SIGKILL`; workers it started keep running, as instances do.
    ///
    /// # Errors
    ///
    /// When the server cannot be killed.
    fn kill_server(&self) -> Result<()>;

    /// The process id of the running server, or `None` when it is not running.
    fn server_pid(&self) -> Option<u32>;

    /// The number of `gang worker` processes of this deployment.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read.
    fn worker_count(&self) -> Result<usize>;

    /// Every scaling event the server recorded, oldest first.
    ///
    /// # Errors
    ///
    /// When the server's scaling history cannot be read.
    fn scaling_events(&self) -> Result<Vec<Event>>;

    /// The HTTP status an anonymous request to the events list answers, over plain HTTP. The
    /// plaintext deployment binds `none:anonymous` to the administrator role, so the route must
    /// admit the caller with `200`.
    ///
    /// # Errors
    ///
    /// When the system has no plaintext API or the request cannot be made.
    fn anonymous_admin_status(&self) -> Result<u16>;

    /// The live state, as `GET /v1/state` answers it to the deployment's own administrator: an
    /// anonymous caller of a plaintext deployment, the administrator's bearer token otherwise.
    ///
    /// # Errors
    ///
    /// When the state cannot be requested or the answer is not a state.
    fn live_state(&self) -> Result<State>;

    /// Opens `GET /v1/state:watch` as [`System::live_state`] reads the state and returns every
    /// frame it carried, oldest first, once `done` holds for them.
    ///
    /// `done` is evaluated on the frames read so far after each new frame, starting with the
    /// snapshot. The wait is bounded by [`Settings::history_timeout`](crate::settings::Settings),
    /// one deadline for the whole wait; ticks do not extend it. A stream that ends before `done`
    /// holds is an error.
    ///
    /// # Errors
    ///
    /// When the stream cannot be opened or does not answer with `200`, cannot be decoded, ends
    /// before `done` holds, or `done` does not hold by the deadline ([`E2eError::Timeout`]).
    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>>;

    /// Builds `probes` in the sample workspace against the remote executor and returns how it
    /// ended. A build that fails is `Ok` with a failed [`BuildOutcome`].
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome>;

    /// Removes the workspace's build outputs (`bazel clean`), keeping the remote cache.
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    fn clean(&self) -> Result<()>;

    /// The report a built probe wrote.
    ///
    /// # Errors
    ///
    /// When the probe has not been built or its report cannot be parsed.
    fn report(&self, probe: Probe) -> Result<ProbeReport>;

    /// The namespaces and paths of the machine the harness runs on.
    fn host(&self) -> &Host;

    /// Returns the whole recorded scaling history, oldest first, once `done` holds for it.
    ///
    /// `done` is evaluated on the history read so far first, then after each frame of the state
    /// stream that follows it. The wait is on the stream, bounded by `Settings::history_timeout`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`](crate::E2eError::Timeout) when `done` still rejects the history once
    /// the timeout has passed; otherwise when the system cannot be observed.
    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>>;

    /// The directory the sandbox mounts as an empty tmpfs (`[sandbox.bwrap] tmp_path`).
    fn tmp_path(&self) -> &Path;

    /// How many trailing lines of a process's stderr a failed check quotes.
    fn stderr_tail_lines(&self) -> usize;
}

/// The cgroup scope of a running action and the limits the kernel applied to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeFacts {
    /// The cgroup directory of the running action.
    pub directory: PathBuf,
    /// The working directory of the running action.
    pub working_directory: PathBuf,
    /// The trimmed contents of `memory.max`.
    pub memory_max: String,
    /// The trimmed contents of `memory.oom.group`.
    pub oom_group: String,
    /// The trimmed contents of `cpu.max`.
    pub cpu_max: String,
    /// The trimmed contents of `pids.max`.
    pub pids_max: String,
}

/// Observes and releases the per-action limits of a [`System`] whose workers enforce them.
pub trait Limits: System {
    /// The scope of the hold probe's orphan process, or `None` until it runs in an action scope.
    ///
    /// # Errors
    ///
    /// When the process table or the cgroup files cannot be read.
    fn running_scope(&self) -> Result<Option<ScopeFacts>>;

    /// Lets the hold probe finish.
    ///
    /// # Errors
    ///
    /// When the release marker cannot be written.
    fn release(&self, scope: &ScopeFacts) -> Result<()>;

    /// Whether the hold probe's orphan process exists.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read.
    fn orphan_alive(&self) -> Result<bool>;

    /// Whether the cgroup directory of `scope` still exists.
    ///
    /// # Errors
    ///
    /// When the filesystem cannot be queried.
    fn scope_exists(&self, scope: &ScopeFacts) -> Result<bool>;

    /// Blocks until the hold probe's orphan starts or the hold build is abandoned, and reports
    /// which happened.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when the hold action does not signal within the settings' hold
    /// timeout.
    ///
    /// When the signal pipe cannot be read or contains an unexpected byte.
    fn await_hold(&self) -> Result<crate::Signalled>;

    /// Wakes a pending or future `await_hold` with abandonment; does not end the hold build.
    ///
    /// # Errors
    ///
    /// When the signal pipe cannot be written.
    fn abandon_hold(&self) -> Result<()>;

    /// The number of attempts the memory grower made: one line per attempt in the counter file
    /// the action appends to.
    ///
    /// # Errors
    ///
    /// When the counter file exists but cannot be read.
    fn attempts(&self) -> Result<usize>;
}

#[cfg(test)]
mod tests {
    use super::Probe;

    #[test]
    fn probe_names_are_unique_and_parse_back() {
        for probe in Probe::ALL {
            assert_eq!(probe.as_str().parse::<Probe>(), Ok(probe));
        }

        let mut names = Probe::ALL.map(Probe::as_str);
        names.sort_unstable();
        assert!(names.windows(2).all(|pair| pair[0] != pair[1]));
    }
}
