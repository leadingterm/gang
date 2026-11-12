//! The cgroup v2 protocol: how the worker and its actions are arranged under the delegated subtree.
//!
//! Under the subtree `S` the worker was started in, the worker lives in the leaf `S/worker` and
//! every action in `S/actions/<pid>-<starttime>-<serial>`. cgroup v2 allows processes only in
//! leaves, so the worker must leave `S` before controllers can be enabled for `S`'s children.
//!
//! An action runs in three steps. [`Controller::enter`] creates its scope and applies the limits;
//! the caller never enters the scope, so a kernel OOM kill of the scope never ends the caller. The
//! sandbox then starts its first process through the launcher, the `gang-join` trampoline, which
//! moves itself into the scope before it executes anything. [`Controller::leave`] reads the usage,
//! kills the scope, waits until it is empty and removes it.
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use gang_clock::{Clock, Poll};
use gang_spi::{ActionLimits, ActionScope, ActionUsage};

use crate::config::{CgroupConfig, Delegation};
use crate::errors::CgroupError;
use crate::fs::{
    CONTROLLERS, CPU_MAX, CPU_STAT, Cgroupfs, EVENTS, KILL, MEMBERSHIP_FILE, MEMORY_MAX,
    MEMORY_OOM_GROUP, MEMORY_PEAK, MEMORY_SWAP_MAX, PIDS_MAX, PROCS, SUBTREE_CONTROL,
};
use crate::limits::KernelLimits;
use crate::parse;

/// The leaf the worker moves into.
const WORKER_LEAF: &str = "worker";
/// The cgroup that holds one child per running action.
const ACTIONS_DIR: &str = "actions";
/// The controllers the limits need; each must be delegated to the worker's subtree.
const REQUIRED_CONTROLLERS: [&str; 3] = ["cpu", "memory", "pids"];
/// The `subtree_control` value enabling every controller in [`REQUIRED_CONTROLLERS`].
/// Keep this in the same order as that list.
const ENABLE_CONTROLLERS: &str = "+cpu +memory +pids";

/// Numbers the scopes created in this process, so their names stay distinct.
static SERIAL: AtomicU64 = AtomicU64::new(0);

/// The cgroup scope of one running action.
///
/// `enter` creates it and returns it; `leave` takes it back. It holds the action's cgroup and the
/// launcher that starts a process inside that cgroup.
#[derive(Debug)]
pub struct CgroupScope {
    /// The action's cgroup, relative to the cgroup root.
    pub(crate) cgroup: PathBuf,
    /// The `gang-join` link followed by the scope's absolute directory: starts a process inside
    /// `cgroup`.
    launcher: Box<[OsString]>,
}

impl ActionScope for CgroupScope {
    fn launcher(&self) -> &[OsString] {
        &self.launcher
    }
}

/// Arranges the worker and its actions on a cgroup filesystem.
#[derive(Debug)]
pub(crate) struct Controller<F, C> {
    pub(crate) fs: F,
    pub(crate) clock: C,
    config: CgroupConfig,
    /// The absolute path of the `gang-join` link that starts each action's processes in its scope.
    join: PathBuf,
    /// Source of the serials that make the names of this controller's scopes distinct.
    serial: &'static AtomicU64,
}

impl<F: Cgroupfs, C: Clock> Controller<F, C> {
    /// Arranges the actions on `fs`, waits on `clock`, and takes its limits from `config`.
    /// `join` is the absolute path of the `gang-join` link that starts each action's processes
    /// inside its scope.
    pub(crate) fn new(fs: F, clock: C, config: CgroupConfig, join: PathBuf) -> Self {
        Self {
            fs,
            clock,
            config,
            join,
            serial: &SERIAL,
        }
    }

    /// Uses `serial` for scope names instead of the process-wide counter.
    #[cfg(test)]
    pub(crate) fn with_serial(mut self, serial: &'static AtomicU64) -> Self {
        self.serial = serial;
        self
    }

    /// Checks that the subtree is delegated (unless the config trusts the operator) and has the
    /// controllers and kernel features the limits need, moves the worker into its leaf, enables
    /// the controllers for the actions, and disposes of the scopes a previous worker left behind.
    /// Safe to repeat from the worker's leaf.
    pub(crate) async fn prepare(&self) -> Result<(), CgroupError> {
        let own = self.own_cgroup()?;
        let subtree = worker_subtree(&own).unwrap_or(&own);
        delegated(subtree)?;
        if self.config.delegation == Delegation::SystemdMarker {
            let is_delegated = self
                .fs
                .delegated(subtree)
                .map_err(CgroupError::io(subtree))?;
            if !is_delegated {
                return Err(CgroupError::NotDelegated {
                    cgroup: subtree.to_owned(),
                });
            }
        }
        self.require_controllers(subtree)?;

        let worker = subtree.join(WORKER_LEAF);
        let actions = subtree.join(ACTIONS_DIR);
        self.create_if_missing(&worker)?;
        self.create_if_missing(&actions)?;
        self.move_here(&worker)?;
        self.write(subtree, SUBTREE_CONTROL, ENABLE_CONTROLLERS)?;
        self.write(&actions, SUBTREE_CONTROL, ENABLE_CONTROLLERS)?;

        for file in [KILL, MEMORY_PEAK] {
            if !self.fs.has_file(&actions, file) {
                return Err(CgroupError::MissingKernelFeature { file });
            }
        }

        for stale in self.children(&actions)? {
            self.dispose(&stale).await?;
        }
        Ok(())
    }

    /// Sweeps abandoned scopes, creates the action's scope and applies `requested` to it. The
    /// calling process stays where it is: the sandbox's first process joins the scope through the
    /// launcher, so the kernel killing the scope never ends the caller.
    pub(crate) fn enter(&self, requested: &ActionLimits) -> Result<CgroupScope, CgroupError> {
        let own = self.own_cgroup()?;
        let subtree = worker_subtree(&own).ok_or_else(|| CgroupError::NotPrepared {
            cgroup: own.clone(),
        })?;
        delegated(subtree)?;

        let actions = subtree.join(ACTIONS_DIR);
        self.sweep(&actions);
        let pid = std::process::id();
        let start = self
            .fs
            .process_start(pid)
            .ok_or(CgroupError::ProcessStart { pid })?;
        let creator = parse::Creator { pid, start };
        let cgroup = actions.join(parse::scope_name(creator, self.next_serial()));
        self.fs.create(&cgroup).map_err(CgroupError::io(&cgroup))?;

        let limits = KernelLimits::new(&self.config, requested);
        if let Err(error) = self.apply(&cgroup, &limits) {
            // The scope holds no process yet; the failure to report is the one from `apply`.
            self.fs.remove(&cgroup).ok();
            return Err(error);
        }
        let launcher: Box<[OsString]> = [
            self.join.clone().into_os_string(),
            self.fs.absolute(&cgroup).into_os_string(),
        ]
        .into();
        Ok(CgroupScope { cgroup, launcher })
    }

    /// Reads the action's usage, kills every process left in the scope, waits until the scope is
    /// empty and removes it.
    pub(crate) async fn leave(&self, scope: CgroupScope) -> Result<ActionUsage, CgroupError> {
        let cgroup = scope.cgroup;
        let usage = self.usage(&cgroup);
        let disposed = self.dispose(&cgroup).await;
        if let Some(actions) = cgroup.parent() {
            self.sweep(actions);
        }
        let usage = usage?;
        disposed?;
        Ok(usage)
    }

    /// Waits for and removes abandoned scopes created by `creator`.
    pub(crate) async fn reap_abandoned(&self, creator: u32) -> Result<(), CgroupError> {
        let own = self.own_cgroup()?;
        let subtree = worker_subtree(&own).ok_or_else(|| CgroupError::NotPrepared {
            cgroup: own.clone(),
        })?;
        delegated(subtree)?;
        for child in self.children(&subtree.join(ACTIONS_DIR))? {
            if parse::scope_creator(&child).is_some_and(|scope| scope.pid == creator)
                && self.abandoned(&child)
            {
                self.dispose(&child).await?;
            }
        }
        Ok(())
    }

    fn own_cgroup(&self) -> Result<PathBuf, CgroupError> {
        self.fs
            .own_cgroup()
            .map_err(CgroupError::io(Path::new(MEMBERSHIP_FILE)))
    }

    fn next_serial(&self) -> u64 {
        self.serial.fetch_add(1, Ordering::Relaxed)
    }

    fn require_controllers(&self, subtree: &Path) -> Result<(), CgroupError> {
        let available = self.read(subtree, CONTROLLERS)?;
        match REQUIRED_CONTROLLERS
            .into_iter()
            .find(|required| !available.split_whitespace().any(|name| name == *required))
        {
            Some(controller) => Err(CgroupError::MissingController {
                cgroup: subtree.to_owned(),
                controller,
            }),
            None => Ok(()),
        }
    }

    fn apply(&self, cgroup: &Path, limits: &KernelLimits) -> Result<(), CgroupError> {
        if let Some(memory) = limits.memory {
            self.write(cgroup, MEMORY_MAX, &memory.to_string())?;
            // Absent when the kernel has no swap accounting; the write is then skipped.
            if self.fs.has_file(cgroup, MEMORY_SWAP_MAX) {
                self.write(cgroup, MEMORY_SWAP_MAX, &limits.swap.to_string())?;
            }
        }
        self.write(cgroup, MEMORY_OOM_GROUP, limits.oom_group.value())?;
        if let Some(cpu) = limits.cpu {
            self.write(cgroup, CPU_MAX, &cpu.to_string())?;
        }
        self.write(cgroup, PIDS_MAX, &limits.pids.to_string())
    }

    fn usage(&self, cgroup: &Path) -> Result<ActionUsage, CgroupError> {
        Ok(ActionUsage {
            peak_memory: parse::peak_memory(&self.read(cgroup, MEMORY_PEAK)?)?,
            cpu_time: parse::cpu_time(&self.read(cgroup, CPU_STAT)?)?,
        })
    }

    /// Kills every process in `cgroup`, waits until none is left, and removes it with its
    /// descendants. A cgroup that is already gone, for example removed by a racing reaper, counts
    /// as removed.
    async fn dispose(&self, cgroup: &Path) -> Result<(), CgroupError> {
        self.kill(cgroup)?;
        self.wait_until_empty(cgroup).await?;
        self.remove_tree(cgroup)
    }

    /// Kills every process in `cgroup` without waiting. Removes `cgroup` with its descendants
    /// and returns `true` once it is empty; otherwise returns `false` and leaves it for a later
    /// sweep.
    fn discard(&self, cgroup: &Path) -> Result<bool, CgroupError> {
        self.kill(cgroup)?;
        if self.is_empty(cgroup)? {
            self.remove_tree(cgroup)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Kills every process in `cgroup`. A cgroup that is already gone has none left.
    fn kill(&self, cgroup: &Path) -> Result<(), CgroupError> {
        match self.fs.write(cgroup, KILL, "1") {
            Err(error) if error.kind() != ErrorKind::NotFound => {
                Err(CgroupError::io(&cgroup.join(KILL))(error))
            }
            _ => Ok(()),
        }
    }

    /// Waits until `cgroup.events` reports `populated 0`, pausing between checks. A cgroup that
    /// is already gone counts as empty.
    async fn wait_until_empty(&self, cgroup: &Path) -> Result<(), CgroupError> {
        let poll = Poll::new(
            self.config.removal_timeout,
            self.config.removal_poll_interval,
        );
        for _ in 0..poll.attempts() {
            if self.is_empty(cgroup)? {
                return Ok(());
            }
            self.clock.sleep(poll.interval().get()).await;
        }
        if self.is_empty(cgroup)? {
            Ok(())
        } else {
            Err(CgroupError::StillPopulated {
                cgroup: cgroup.to_owned(),
                timeout: self.config.removal_timeout,
            })
        }
    }

    /// Whether `cgroup` has no process left. A cgroup that is already gone is empty.
    fn is_empty(&self, cgroup: &Path) -> Result<bool, CgroupError> {
        let events = match self.fs.read(cgroup, EVENTS) {
            Ok(events) => events,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(CgroupError::io(&cgroup.join(EVENTS))(error)),
        };
        Ok(!parse::populated(&events)?)
    }

    /// Removes `cgroup` and its descendants, deepest first. A cgroup that is already gone is
    /// not an error.
    fn remove_tree(&self, cgroup: &Path) -> Result<(), CgroupError> {
        let mut order = Vec::new();
        self.descendants_first(cgroup, &mut order)?;
        for scope in order {
            if let Err(error) = self.fs.remove(&scope)
                && error.kind() != ErrorKind::NotFound
            {
                return Err(CgroupError::io(&scope)(error));
            }
        }
        Ok(())
    }

    fn descendants_first(
        &self,
        cgroup: &Path,
        order: &mut Vec<PathBuf>,
    ) -> Result<(), CgroupError> {
        let children = match self.fs.children(cgroup) {
            Ok(children) => children,
            Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(CgroupError::io(cgroup)(error)),
        };
        for child in children {
            self.descendants_first(&child, order)?;
        }
        order.push(cgroup.to_owned());
        Ok(())
    }

    /// Kills every process left in the scopes under `actions` whose creator process is gone and
    /// removes each scope once it is empty. A creator is gone once its pid no longer runs with
    /// the recorded start time, so a recycled pid never keeps a foreign scope alive. A scope whose
    /// creator is alive may still be joining, so it is never touched.
    ///
    /// Never waits: a scope that does not empty never delays [`Self::enter`] or [`Self::leave`].
    /// Best effort: such a scope, or one that cannot be removed now, is handled by a later sweep,
    /// or by the next [`Self::prepare`], which waits and fails.
    fn sweep(&self, actions: &Path) {
        let Ok(children) = self.fs.children(actions) else {
            return;
        };
        for child in children {
            if !self.abandoned(&child) {
                continue;
            }
            // A scope that is still populated or cannot be removed now is handled by a later sweep.
            match self.discard(&child) {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    cgroup = %child.display(),
                    "an abandoned action scope is still populated; a later sweep removes it"
                ),
                Err(error) => tracing::warn!(
                    error = %gang_types::error_chain(&error),
                    "cannot remove an abandoned action scope; a later sweep retries"
                ),
            }
        }
    }

    /// A recognized scope is abandoned when its creator no longer has the recorded start time.
    fn abandoned(&self, cgroup: &Path) -> bool {
        parse::scope_creator(cgroup)
            .is_some_and(|creator| self.fs.process_start(creator.pid) != Some(creator.start))
    }

    fn create_if_missing(&self, cgroup: &Path) -> Result<(), CgroupError> {
        match self.fs.create(cgroup) {
            Err(error) if error.kind() != ErrorKind::AlreadyExists => {
                Err(CgroupError::io(cgroup)(error))
            }
            _ => Ok(()),
        }
    }

    fn move_here(&self, cgroup: &Path) -> Result<(), CgroupError> {
        self.write(cgroup, PROCS, &std::process::id().to_string())
    }

    fn children(&self, cgroup: &Path) -> Result<Vec<PathBuf>, CgroupError> {
        self.fs.children(cgroup).map_err(CgroupError::io(cgroup))
    }

    fn read(&self, cgroup: &Path, file: &str) -> Result<String, CgroupError> {
        self.fs
            .read(cgroup, file)
            .map_err(CgroupError::io(&cgroup.join(file)))
    }

    fn write(&self, cgroup: &Path, file: &str, value: &str) -> Result<(), CgroupError> {
        self.fs
            .write(cgroup, file, value)
            .map_err(CgroupError::io(&cgroup.join(file)))
    }
}

/// The subtree above `cgroup` when `cgroup` is the worker's leaf.
fn worker_subtree(cgroup: &Path) -> Option<&Path> {
    if cgroup.file_name() == Some(OsStr::new(WORKER_LEAF)) {
        cgroup.parent()
    } else {
        None
    }
}

/// A subtree must be below the root cgroup, which has no delegation.
fn delegated(subtree: &Path) -> Result<(), CgroupError> {
    if subtree.as_os_str().is_empty() {
        Err(CgroupError::NoDelegatedSubtree)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ENABLE_CONTROLLERS, REQUIRED_CONTROLLERS};

    #[test]
    fn enabled_controllers_match_required_controllers() {
        assert_eq!(
            ENABLE_CONTROLLERS,
            REQUIRED_CONTROLLERS
                .map(|name| format!("+{name}"))
                .join(" ")
        );
    }
}
