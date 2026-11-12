//! Guard reaping and stale action scopes after `gang exec` is `SIGKILL`ed.
//! A guard normally reaps the scope, including a stray outside the process group; without the
//! guard, the next action sweeps it, and a dead worker's are gone before its replacement serves.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::{Execution, Lines, Proc, Role, held_build};
use crate::names::stale_scope::{
    GONE_BEFORE_REPLACEMENT_SERVES, GUARD_REAPS_SCOPE, SCOPE_OUTLIVES_EXEC, STRAY_STARTED,
    SWEPT_BY_NEXT_ACTION,
};
use crate::oom::oom;
use crate::system::{Probe, System};

/// The marker carried by the stray process.
const STRAY_MARKER: &str = "e2e-stray";

/// What the journal and process table reveal about a stray.
#[derive(Debug)]
enum StrayObservation {
    /// No matching process was observed.
    NotStarted,
    /// The process was found without a cgroup scope.
    MissingScope { pid: u32 },
    /// The process and its scope were found.
    Started(Stray),
}

impl StrayObservation {
    /// Returns the stray or describes why it cannot be used for a scope check.
    fn into_started(self, stray: &str) -> std::result::Result<Stray, String> {
        match self {
            Self::NotStarted => Err(format!("{stray} never reported its pid and scope")),
            Self::MissingScope { pid } => Err(format!("{stray} {pid} has no cgroup scope")),
            Self::Started(stray) => Ok(stray),
        }
    }
}

#[derive(Debug)]
struct Stray {
    pid: u32,
    scope: PathBuf,
}

/// Runs under the noop sandbox with per-action cgroups and `NativeLink`'s memory enforcement off. It first runs the
/// out-of-memory checks, then verifies guard reaping, the next action's sweep, and that a dead
/// worker's stale scope and stray are gone before its replacement serves, whichever component
/// removes them.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn stale_scope<S: System, E: Execution>(
    system: &S,
    exec: &E,
    stderr_tail_lines: usize,
    hold_timeout: Duration,
) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = oom(system, exec, Probe::Pinned, stderr_tail_lines)?;

    let (observation, reaped) = guard_reaps(system, exec, hold_timeout)?;
    if let Err(failure) = observation.into_started("the stray") {
        checks.push(Check::failed(STRAY_STARTED, failure.clone()));
        checks.extend(Check::skipped_all(
            [
                GUARD_REAPS_SCOPE,
                SCOPE_OUTLIVES_EXEC,
                SWEPT_BY_NEXT_ACTION,
                GONE_BEFORE_REPLACEMENT_SERVES,
            ],
            &format!("not reached: {failure}"),
        ));
        return Ok(checks);
    }
    checks.push(Check::passed(STRAY_STARTED));
    checks.push(Check::expect(GUARD_REAPS_SCOPE, reaped, || {
        "the guard did not finish reaping the scope and stray".to_owned()
    }));
    checks.extend(stale_construction(system, exec, hold_timeout)?);
    Ok(checks)
}

/// Observes whether the live guard reaps a killed exec's scope and stray.
fn guard_reaps<S: System, E: Execution>(
    system: &S,
    exec: &E,
    hold_timeout: Duration,
) -> Result<(StrayObservation, bool)> {
    exec.set_go(false)?;
    let (_held, observed) = held_build(system, exec, &[Probe::Stray], |lines| {
        let stray = match await_stray(exec, lines)? {
            StrayObservation::Started(stray) => stray,
            observation => return Ok((observation, false)),
        };
        let processes = exec.processes()?;
        let gang_exec = processes.iter().find(|p| p.role == Role::GangExec);
        let guard = gang_exec.and_then(|creator| {
            processes.iter().find(|p| {
                p.role
                    == (Role::GangGuard {
                        creator: creator.pid,
                    })
            })
        });
        if let Some(creator) = gang_exec {
            exec.kill(creator.pid)?;
        }
        let gone = match guard {
            Some(guard) => exec.await_gone(guard.pid, hold_timeout)?,
            None => false,
        };
        let reaped = gone && !exec.cgroup_exists(&stray.scope)? && !alive(exec, stray.pid)?;
        if !gone && let Some(guard) = guard {
            exec.kill(guard.pid)?;
        }
        cleanup(exec, &stray)?;
        Ok((StrayObservation::Started(stray), reaped))
    })?;
    Ok(observed)
}

/// Constructs a stale scope without a guard, then observes both worker sweeps.
fn stale_construction<S: System, E: Execution>(
    system: &S,
    exec: &E,
    hold_timeout: Duration,
) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    exec.set_go(false)?;
    let (_held, stray) = held_build(system, exec, &[Probe::Stray], |lines| {
        let stray = match await_stray(exec, lines)? {
            StrayObservation::Started(stray) => stray,
            observation => return Ok((observation, false)),
        };
        let killed = kill_guards_and_execs(exec, hold_timeout)?;
        Ok((StrayObservation::Started(stray), killed))
    })?;
    let (observation, guards_killed) = stray;
    let stray = match observation.into_started("the second stray") {
        Ok(stray) => stray,
        Err(failure) => {
            checks.push(Check::failed(SCOPE_OUTLIVES_EXEC, failure.clone()));
            checks.extend(Check::skipped_all(
                [SWEPT_BY_NEXT_ACTION, GONE_BEFORE_REPLACEMENT_SERVES],
                &format!("not reached: {failure}"),
            ));
            return Ok(checks);
        }
    };
    if !guards_killed {
        cleanup(exec, &stray)?;
        checks.push(Check::failed(
            SCOPE_OUTLIVES_EXEC,
            "the gang exec or its guard was not observed",
        ));
        checks.extend(Check::skipped_all(
            [SWEPT_BY_NEXT_ACTION, GONE_BEFORE_REPLACEMENT_SERVES],
            "not reached: the gang exec or its guard was not observed",
        ));
        return Ok(checks);
    }

    // While the worker lives, its embedded NativeLink signals every process group the action held
    // once `gang exec` is gone, so the stray may already be dead. With the guard gone, the
    // scope must still exist until a sweep removes it.
    if !exec.cgroup_exists(&stray.scope)? {
        cleanup(exec, &stray)?;
        checks.push(Check::failed(
            SCOPE_OUTLIVES_EXEC,
            format!(
                "the scope {} went with its gang exec",
                stray.scope.display()
            ),
        ));
        checks.extend(Check::skipped_all(
            [SWEPT_BY_NEXT_ACTION, GONE_BEFORE_REPLACEMENT_SERVES],
            "not reached: the scope went with its gang exec",
        ));
        return Ok(checks);
    }
    checks.push(Check::passed(SCOPE_OUTLIVES_EXEC));

    let next = system.build(&[Probe::Isolated])?;
    let mut failures = Vec::new();
    if !next.succeeded() {
        failures.push(format!(
            "the next action ended with exit code {:?}",
            next.exit_code
        ));
    }
    if exec.cgroup_exists(&stray.scope)? {
        failures.push(format!(
            "the stale scope {} still exists",
            stray.scope.display()
        ));
    }
    if alive(exec, stray.pid)? {
        failures.push(format!("the stray {} still runs", stray.pid));
    }
    if failures.is_empty() {
        checks.push(Check::passed(SWEPT_BY_NEXT_ACTION));
    } else {
        checks.push(Check::failed(SWEPT_BY_NEXT_ACTION, failures.join("; ")));
    }
    cleanup(exec, &stray)?;

    replaced_worker(system, exec, &mut checks, hold_timeout)?;
    Ok(checks)
}

fn replaced_worker<S: System, E: Execution>(
    system: &S,
    exec: &E,
    checks: &mut Vec<Check>,
    hold_timeout: Duration,
) -> Result<()> {
    exec.set_go(false)?;
    let (_held, stray) = held_build(system, exec, &[Probe::Stray], |lines| {
        let stray = match await_stray(exec, lines)? {
            StrayObservation::Started(stray) => stray,
            observation => return Ok((observation, Vec::new())),
        };
        // Freeze the worker's subtree first: its NativeLink cannot signal the stray, and the frozen
        // worker keeps its instance lock, so Local Mode cannot retire its cgroup while the stray's
        // survival is observed. The worker is killed only after that observation.
        let mut unmet = Vec::new();
        let subtree = worker_subtree(&stray.scope);
        let frozen = match subtree {
            Some(subtree) => exec.freeze(subtree, true, hold_timeout)?,
            None => false,
        };
        if !frozen {
            unmet.push(format!(
                "the worker's cgroup above {} did not freeze",
                stray.scope.display()
            ));
        }
        if !kill_guards_and_execs(exec, hold_timeout)? {
            unmet.push("the gang exec or its guard was not observed to exit".to_owned());
        }
        if !alive(exec, stray.pid)? {
            unmet.push(format!("the stray {} died with its gang exec", stray.pid));
        }
        if !exec.cgroup_exists(&stray.scope)? {
            unmet.push(format!(
                "the scope {} went with its gang exec",
                stray.scope.display()
            ));
        }
        for worker in roles(exec, &Role::Worker)? {
            exec.kill(worker)?;
            if !exec.await_gone(worker, hold_timeout)? {
                unmet.push(format!("the worker {worker} did not exit"));
            }
        }
        if frozen && let Some(subtree) = subtree {
            exec.freeze(subtree, false, hold_timeout)?;
        }
        // Released only now, so the requeued attempt runs on the replacement worker.
        exec.set_go(true)?;
        Ok((StrayObservation::Started(stray), unmet))
    })?;
    let (observation, unmet) = stray;
    let stray = match observation.into_started("the second stray") {
        Ok(stray) => stray,
        Err(failure) => {
            checks.push(Check::failed(GONE_BEFORE_REPLACEMENT_SERVES, failure));
            return Ok(());
        }
    };
    let next = system.build(&[Probe::Unconstrained])?;
    let mut failures = Vec::new();
    if !unmet.is_empty() {
        failures.push(format!(
            "the stray did not outlive its killed gang exec and worker: {}",
            unmet.join("; ")
        ));
    }
    if !next.succeeded() {
        failures.push(format!(
            "the build on the replacement worker ended with exit code {:?}",
            next.exit_code
        ));
    }
    if exec.cgroup_exists(&stray.scope)? {
        failures.push(format!(
            "the stale scope {} still exists",
            stray.scope.display()
        ));
    }
    if alive(exec, stray.pid)? {
        failures.push(format!("the stray {} still runs", stray.pid));
    }
    if failures.is_empty() {
        checks.push(Check::passed(GONE_BEFORE_REPLACEMENT_SERVES));
    } else {
        checks.push(Check::failed(
            GONE_BEFORE_REPLACEMENT_SERVES,
            failures.join("; "),
        ));
    }
    cleanup(exec, &stray)?;
    Ok(())
}

/// Reads a stray's pid from the journal and finds its process scope.
///
/// # Errors
///
/// Returns journal, process-table, and parse errors.
fn await_stray<E: Execution>(exec: &E, lines: &mut Lines<'_, E>) -> Result<StrayObservation> {
    let Some(line) = lines.await_prefix("stray ")? else {
        return Ok(StrayObservation::NotStarted);
    };
    let pid: u32 = line
        .trim_start_matches("stray ")
        .trim()
        .parse()
        .map_err(E2eError::parse("stray pid"))?;
    Ok(observe_stray(pid, exec.processes()?))
}

/// Finds the marked process and its scope in the process table.
fn observe_stray(pid: u32, processes: Vec<Proc>) -> StrayObservation {
    let Some(process) = processes.into_iter().find(|process| {
        process.pid == pid
            && matches!(&process.role, Role::Marked(marker) if marker == STRAY_MARKER)
    }) else {
        return StrayObservation::NotStarted;
    };
    match process.cgroup {
        Some(scope) => StrayObservation::Started(Stray { pid, scope }),
        None => StrayObservation::MissingScope { pid },
    }
}

/// Kills each observed exec's guard, then the exec itself, waiting for each to exit; returns
/// whether every exec had a guard and both exited.
fn kill_guards_and_execs<E: Execution>(exec: &E, timeout: Duration) -> Result<bool> {
    let processes = exec.processes()?;
    let creators: Vec<_> = processes
        .iter()
        .filter(|p| p.role == Role::GangExec)
        .collect();
    let mut all_found = !creators.is_empty();
    for creator in creators {
        if let Some(guard) = processes.iter().find(|p| {
            p.role
                == (Role::GangGuard {
                    creator: creator.pid,
                })
        }) {
            exec.kill(guard.pid)?;
            all_found &= exec.await_gone(guard.pid, timeout)?;
        } else {
            all_found = false;
        }
        exec.kill(creator.pid)?;
        all_found &= exec.await_gone(creator.pid, timeout)?;
    }
    Ok(all_found)
}

/// Returns process ids whose role equals `role`.
///
/// # Errors
///
/// Returns process-table errors.
fn roles<E: Execution>(exec: &E, role: &Role) -> Result<Vec<u32>> {
    Ok(exec
        .processes()?
        .into_iter()
        .filter(|process| &process.role == role)
        .map(|process| process.pid)
        .collect())
}

/// Returns whether `pid` is a live deployment process.
///
/// # Errors
///
/// Returns process-table errors.
fn alive<E: Execution>(exec: &E, pid: u32) -> Result<bool> {
    Ok(exec.processes()?.iter().any(|process| process.pid == pid))
}

/// The worker's delegated subtree that holds the action scope `scope`: `<subtree>/actions/<scope>`.
fn worker_subtree(scope: &Path) -> Option<&Path> {
    scope.parent()?.parent()
}

/// Kills the stray if it is still alive.
///
/// # Errors
///
/// Returns process-table and signal errors.
fn cleanup<E: Execution>(exec: &E, stray: &Stray) -> Result<()> {
    if alive(exec, stray.pid)? {
        exec.kill(stray.pid)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{STRAY_MARKER, StrayObservation, observe_stray};
    use crate::execution::{Proc, Role};

    #[test]
    fn missing_scope_names_the_observed_process() {
        let observation = observe_stray(
            42,
            vec![Proc {
                pid: 42,
                role: Role::Marked(STRAY_MARKER.to_owned()),
                cgroup: None,
                instance: None,
            }],
        );
        assert!(matches!(
            &observation,
            StrayObservation::MissingScope { pid: 42 }
        ));
        assert_eq!(
            observation.into_started("the stray").err().as_deref(),
            Some("the stray 42 has no cgroup scope")
        );
    }
}
