//! Action limits: the platform properties an action requests and the environment variables the worker exports them to.

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use gang_types::{Bytes, CpuWeight, NonZeroBytes};

use crate::{PortError, port};

/// The environment variable the worker configuration exports the action's `cpu_count` property to, read by `gang exec`.
/// Unset or empty means not requested.
pub const CPU_COUNT_ENV: &str = "GANG_CPU_COUNT";

/// The environment variable the worker configuration exports the action's `memory_kb` property to, read by `gang exec`.
/// Unset or empty means not requested.
pub const MEMORY_KB_ENV: &str = "GANG_MEMORY_KB";

/// Worker-provided side-channel path, withheld from the action.
pub const SIDE_CHANNEL_FILE_ENV: &str = "GANG_SIDE_CHANNEL_FILE";

/// The name of the link to the `gang` binary that guards abandoned action scopes.
pub const GUARD_LINK_NAME: &str = "gang-guard";

/// The name of the link to the `gang` binary, beside `gang exec`'s entrypoint and sandbox file,
/// through which a command starts inside an action's limits scope:
/// `gang-join <scope> <program> [<argument>...]` joins the scope, then executes the program in
/// place, so the program's first instruction already runs under the scope's limits.
pub const JOIN_LINK_NAME: &str = "gang-join";

/// One action's limits scope, as [`ActionLimitsPort::enter`] created it.
#[gang_macros::settings]
pub trait ActionScope: std::fmt::Debug + Send {
    /// The command prefix that starts a process inside this scope: that process and everything
    /// it starts run under the scope's limits from their first instruction.
    fn launcher(&self) -> &[std::ffi::OsString];
    /// Absolute cgroup path for exact memory accounting, when available.
    fn memory_cgroup(&self) -> Option<std::path::PathBuf>;
}

/// Kernel limits for one action. Optional: with no limits plugin an action runs unconfined.
#[port]
pub trait ActionLimitsPort: Send + Sync {
    /// What one action's limits scope holds between `enter` and `leave`: the plugin's own state,
    /// handed back to the plugin that created it, and the launcher into the scope.
    type Scope: ActionScope;

    /// Called once by `gang worker` before it takes actions: moves the worker into its own leaf
    /// scope and makes the scope that holds the actions' scopes ready.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the scopes cannot be prepared, the subtree is not delegated, or the kernel lacks a required interface file.
    async fn prepare(
        &self,
        reserve: gang_types::Bytes,
    ) -> Result<Option<ActionMemoryPool>, PortError>;

    /// Creates a scope for one action and applies `limits` to it. The caller never enters the
    /// scope: the sandbox starts its first process through the scope's
    /// [`ActionScope::launcher`], so the kernel killing the scope (for example on an
    /// out-of-memory event) can never end the caller. The returned `Scope` is the only handle to
    /// the scope: `leave` consumes it.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the scope cannot be created or limited.
    async fn enter(&self, limits: &ActionLimits) -> Result<Self::Scope, PortError>;

    /// Reads what the action used, ends every process left in the scope, waits until the scope is
    /// empty, and removes it. Consumes `scope`, the value [`ActionLimitsPort::enter`] returned for
    /// this action.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the usage cannot be read or processes remain in the scope after the
    /// wait.
    async fn leave(&self, scope: Self::Scope) -> Result<ActionUsage, PortError>;

    /// Ends processes in scopes created by `creator` whose recorded creator has gone, waits until
    /// each scope is empty, and removes it. A zombie or a process with a different start time
    /// counts as gone. `gang-guard` calls this after the `gang exec` it watches exits, including
    /// when it was killed with `SIGKILL`. Each wait is bounded by the configured removal timeout.
    ///
    /// # Errors
    ///
    /// [`PortError`] when a scope cannot be read, killed, or removed, or processes remain after the wait.
    async fn reap_abandoned(&self, creator: u32) -> Result<(), PortError>;
}

/// The worker-wide kernel memory backstop for action scopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionMemoryPool {
    /// Absolute path to the action pool cgroup.
    pub cgroup: PathBuf,
    /// Actual kernel-enforced capacity after page rounding.
    pub capacity: NonZeroBytes,
}

/// What an action reserves. `None`: the action did not request it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionLimits {
    /// The number of CPUs the action reserves.
    pub cpu_count: Option<NonZeroU32>,
    /// The memory the action reserves; `None` when it asked for none.
    pub memory: Option<NonZeroBytes>,
    /// The `cgroup` v2 `cpu.weight` share for the action's scope; `None` leaves the parent's
    /// default.
    pub cpu_weight: Option<CpuWeight>,
}

/// What an action used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionUsage {
    /// The highest memory the action held at once, if the kernel exposes it.
    pub peak_memory: Option<Bytes>,
    /// The CPU time the action consumed.
    pub cpu_time: Duration,
    /// Processes killed by any kernel OOM killer in this scope or its descendants, as counted
    /// by the hierarchical `oom_kill` entry of the scope's `memory.events`.
    pub oom_kills: u64,
}
