//! The action timeout scenarios: an action that outlives `action_timeout` fails the build and leaves no
//! process, scope or temporary directory behind; SIGTERM comes first and an action that exits within `kill_grace`
//! reaches its own exit while `gang exec` waits for it; one that ignores SIGTERM is killed when the grace ends. And
//! the exit status of an action killed by a signal.

use std::path::PathBuf;

use crate::check::Check;
use crate::errors::Result;
use crate::execution::{Execution, Lines, Role, held_build, running_scope};
use crate::names::{action_timeout, exit_signal};
use crate::system::{Probe, Sandbox, System};

/// Runs under `[scheduler.nativelink.worker] action_timeout = "5s"` with per-action cgroups;
/// `sandbox` says whether the action's own `TMPDIR` is expected (noop) or none at all (bwrap).
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn action_timeout<S: System, E: Execution>(
    system: &S,
    exec: &E,
    sandbox: Sandbox,
) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = Vec::new();

    let (outcome, started) = held_build(system, exec, &[Probe::TimeoutChild], |lines| {
        Ok(lines.await_prefix("child started")?.is_some())
    })?;
    checks.push(Check::expect(
        action_timeout::CHILD_STARTED,
        started,
        || "the background child never wrote to the journal".to_owned(),
    ));
    checks.push(Check::expect(
        action_timeout::TIMEOUT_FAILS,
        !outcome.succeeded(),
        || format!("the build ended with exit code {:?}", outcome.exit_code),
    ));
    checks.push(if started {
        Check::expect(
            action_timeout::CHILD_GONE,
            !marked_alive(exec, "e2e-timeout-child")?,
            || "the background child still runs".to_owned(),
        )
    } else {
        Check::failed(
            action_timeout::CHILD_GONE,
            "the background child was never observed",
        )
    });

    let (_trapped, trap) = held_build(system, exec, &[Probe::TermTrap], |lines| {
        observe_trap(exec, lines)
    })?;
    checks.push(Check::expect(action_timeout::TRAP_RAN, trap.term, || {
        "the action never reported SIGTERM".to_owned()
    }));
    checks.push(Check::expect(
        action_timeout::EXEC_WAITS,
        trap.exec_waited,
        || "no gang exec was running while the action ran its SIGTERM trap".to_owned(),
    ));
    checks.push(Check::expect(
        action_timeout::TRAP_EXITS_CLEANLY,
        trap.clean,
        || "the action was killed before its trap finished".to_owned(),
    ));
    checks.push(scope_removed(exec, trap.scope.as_ref())?);
    let leftovers = exec.temp_dirs()?;
    let temp_failure = match sandbox {
        Sandbox::Noop => match trap.tmpdir.as_ref() {
            None => Some("the action reported no per-action TMPDIR".to_owned()),
            Some(dir)
                if !dir
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("gang-action-")) =>
            {
                Some(format!(
                    "TMPDIR {} is not a gang-action-* directory",
                    dir.display()
                ))
            }
            Some(dir) if exec.path_exists(dir)? => {
                Some(format!("TMPDIR {} still exists", dir.display()))
            }
            Some(_) if !leftovers.is_empty() => {
                Some(format!("gang-action-* directories left: {leftovers:?}"))
            }
            Some(_) => None,
        },
        Sandbox::Bwrap if trap.scope.is_none() => {
            Some("the timed-out action was never observed running".to_owned())
        }
        Sandbox::Bwrap if !leftovers.is_empty() => {
            Some(format!("gang-action-* directories left: {leftovers:?}"))
        }
        Sandbox::Bwrap => None,
    };
    checks.push(match temp_failure {
        Some(reason) => Check::failed(action_timeout::TEMP_CLEAN, reason),
        None => Check::passed(action_timeout::TEMP_CLEAN),
    });

    check_ignorer(system, exec, &mut checks)?;
    Ok(checks)
}

fn check_ignorer<S: System, E: Execution>(
    system: &S,
    exec: &E,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let (ignorer_outcome, ignorer) = held_build(system, exec, &[Probe::TermIgnorer], |lines| {
        observe_ignorer(exec, lines)
    })?;
    checks.push(Check::expect(
        action_timeout::IGNORER_SURVIVES_GRACE,
        ignorer.term && ignorer.survived,
        || {
            if ignorer.term {
                "the action was killed within a second of SIGTERM".to_owned()
            } else {
                "the action never reported SIGTERM".to_owned()
            }
        },
    ));
    let mut failures = Vec::new();
    if ignorer_outcome.succeeded() {
        failures.push("the build succeeded".to_owned());
    }
    match ignorer.scope.as_ref() {
        None => failures.push("no scope was observed".to_owned()),
        Some(scope) if exec.cgroup_exists(scope)? => {
            failures.push(format!("its scope {} still exists", scope.display()));
        }
        Some(_) => {}
    }
    if gang_exec_alive(exec)? {
        failures.push("a gang exec still runs".to_owned());
    }
    checks.push(if failures.is_empty() {
        Check::passed(action_timeout::IGNORER_KILLED)
    } else {
        Check::failed(action_timeout::IGNORER_KILLED, failures.join("; "))
    });

    Ok(())
}

#[derive(Debug, Default)]
struct Trap {
    tmpdir: Option<PathBuf>,
    scope: Option<PathBuf>,
    term: bool,
    exec_waited: bool,
    clean: bool,
}

/// Observes the SIGTERM trap's journal and the live action scope.
///
/// # Errors
///
/// Returns errors from journal and process observations.
fn observe_trap<E: Execution>(exec: &E, lines: &mut Lines<'_, E>) -> Result<Trap> {
    let Some(started) = lines.await_prefix("trap started")? else {
        return Ok(Trap::default());
    };
    let tmpdir = started
        .strip_prefix("trap started ")
        .filter(|dir| *dir != "none")
        .map(PathBuf::from);
    let scope = running_scope(exec)?;
    let term = lines.await_prefix("trap term")?.is_some();
    let exec_waited = term && gang_exec_alive(exec)?;
    let clean = term && lines.await_prefix("trap clean")?.is_some();
    Ok(Trap {
        tmpdir,
        scope,
        term,
        exec_waited,
        clean,
    })
}

#[derive(Debug, Default)]
struct Ignorer {
    scope: Option<PathBuf>,
    term: bool,
    survived: bool,
}

/// Observes the SIGTERM-ignoring action through its journal and scope.
///
/// # Errors
///
/// Returns errors from journal and process observations.
fn observe_ignorer<E: Execution>(exec: &E, lines: &mut Lines<'_, E>) -> Result<Ignorer> {
    let Some(_) = lines.await_prefix("ignore started")? else {
        return Ok(Ignorer::default());
    };
    let scope = running_scope(exec)?;
    let term = lines.await_prefix("ignore term")?.is_some();
    let survived = term && lines.await_prefix("ignore survived")?.is_some();
    Ok(Ignorer {
        scope,
        term,
        survived,
    })
}

/// Whether any live `gang exec` process is present.
///
/// # Errors
///
/// Returns errors from process observations.
fn gang_exec_alive<E: Execution>(exec: &E) -> Result<bool> {
    Ok(exec
        .processes()?
        .iter()
        .any(|process| process.role == Role::GangExec))
}

/// Whether any live probe process has the given marker.
///
/// # Errors
///
/// Returns errors from process observations.
fn marked_alive<E: Execution>(exec: &E, marker: &str) -> Result<bool> {
    Ok(exec
        .processes()?
        .iter()
        .any(|process| matches!(&process.role, Role::Marked(value) if value == marker)))
}

/// Checks that an observed action scope has been removed.
///
/// # Errors
///
/// Returns errors from cgroup observations.
fn scope_removed<E: Execution>(exec: &E, scope: Option<&PathBuf>) -> Result<Check> {
    let Some(scope) = scope else {
        return Ok(Check::failed(
            action_timeout::SCOPE_REMOVED,
            "no scope was observed for the timed-out action",
        ));
    };
    Ok(Check::expect(
        action_timeout::SCOPE_REMOVED,
        !exec.cgroup_exists(scope)?,
        || format!("{} still exists", scope.display()),
    ))
}

/// Checks signal-derived action exit statuses on a running noop deployment; the caller started
/// the server. SIGKILL is reported as 137 and SIGTERM as 143 (`128 + signal`).
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn exit_signals<S: System>(system: &S, stderr_tail_lines: usize) -> Result<Vec<Check>> {
    let killed = system.build(&[Probe::SelfKill])?;
    let mut checks = vec![Check::expect(
        exit_signal::SIGKILL,
        !killed.succeeded() && killed.action_killed(),
        || {
            format!(
                "exit {:?}; no action reported `(Exit 137)` or `(Killed)`; the build said:\n{}",
                killed.exit_code,
                killed.stderr_tail(stderr_tail_lines)
            )
        },
    )];
    let termed = system.build(&[Probe::SelfTerm])?;
    checks.push(Check::expect(
        exit_signal::SIGTERM,
        // Bazel renders exit status 128 + 15 as `(Terminated)`, as it renders 137 as `(Killed)`.
        !termed.succeeded() && (termed.action_exit(143) || termed.stderr.contains("(Terminated)")),
        || {
            format!(
                "exit {:?}; no action reported `(Exit 143)` or `(Terminated)`; the build said:\n{}",
                termed.exit_code,
                termed.stderr_tail(stderr_tail_lines)
            )
        },
    ));
    Ok(checks)
}
