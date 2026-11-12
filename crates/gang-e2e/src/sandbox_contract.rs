//! The sandbox contract under bwrap: an action sees none of the worker's own environment, a fresh empty
//! tmpfs on /tmp, every read-only bind read-only and its working directory writable; it runs with bwrap inside its
//! own scope while `gang exec` stays in the worker's leaf; and an out-of-memory kill is cleaned up.

use std::collections::BTreeSet;
use std::thread::ScopedJoinHandle;

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::{Execution, Role, WORKER_LEAF};
use crate::hold::Signalled;
use crate::names::sandbox_contract;
use crate::oom::oom;
use crate::system::{Limits, Probe, System};

/// Proves the bwrap sandbox contract, held-action cgroup placement, OOM cleanup, and continued service.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn sandbox_contract<S: Limits, E: Execution>(
    system: &S,
    exec: &E,
    stderr_tail_lines: usize,
) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = environment(system, exec, stderr_tail_lines)?;
    held_in_scope(system, exec, &mut checks)?;
    checks.extend(oom(system, exec, Probe::Pinned, stderr_tail_lines)?);
    Ok(checks)
}

fn environment<S: System, E: Execution>(
    system: &S,
    exec: &E,
    stderr_tail_lines: usize,
) -> Result<Vec<Check>> {
    let outcome = system.build(&[Probe::SandboxContract])?;
    if !outcome.succeeded() {
        let reason = format!(
            "the probe failed with exit code {:?}; the build said:\n{}",
            outcome.exit_code,
            outcome.stderr_tail(stderr_tail_lines)
        );
        return Ok(vec![
            Check::failed(sandbox_contract::WORKER_ENV_HIDDEN, reason.clone()),
            Check::failed(sandbox_contract::TMP_FRESH, reason.clone()),
            Check::failed(sandbox_contract::READONLY_BINDS, reason.clone()),
            Check::failed(sandbox_contract::WORKDIR_WRITABLE, reason),
        ]);
    }
    let report = system.report(Probe::SandboxContract)?;
    let worker = exec
        .processes()?
        .into_iter()
        .find(|p| p.role == Role::Worker);
    let (hidden, reason) = if let Some(worker) = worker {
        let own: BTreeSet<String> = exec
            .worker_only_environment(worker.pid)?
            .into_iter()
            .collect();
        let seen: BTreeSet<&str> = report
            .get("env_names")?
            .split(',')
            .filter(|name| !name.is_empty())
            .collect();
        let leaked: Vec<_> = own
            .iter()
            .filter(|name| seen.contains(name.as_str()))
            .collect();
        if own.is_empty() {
            (
                false,
                "the worker's environment has no variable of its own to look for".to_owned(),
            )
        } else {
            let pass = leaked.is_empty();
            (pass, format!("the action sees the worker's {leaked:?}"))
        }
    } else {
        (false, "no worker of the deployment runs".to_owned())
    };
    let mut checks = vec![Check::expect(
        sandbox_contract::WORKER_ENV_HIDDEN,
        hidden,
        || reason,
    )];

    let entries = report.get("tmp_entries")?;
    let fs = report.get("tmp_fs")?;
    checks.push(Check::expect(
        sandbox_contract::TMP_FRESH,
        entries.is_empty() && fs == "tmpfs",
        || format!("/tmp holds `{entries}` on a `{fs}` filesystem"),
    ));

    let paths = exec.readonly_bind_paths()?;
    let mut offenders = Vec::new();
    for path in &paths {
        match report.get(&format!("mount:{path}")) {
            Ok("erofs") => {}
            Ok(value) => offenders.push(format!("{path}: {value}")),
            Err(_) => offenders.push(format!("{path}: not mounted")),
        }
    }
    let readonly_reason = if paths.is_empty() {
        "no readonly_bind_paths are configured".to_owned()
    } else {
        offenders.join("; ")
    };
    checks.push(Check::expect(
        sandbox_contract::READONLY_BINDS,
        !paths.is_empty() && offenders.is_empty(),
        || readonly_reason,
    ));
    let cwd = report.get("cwd_writable")?;
    checks.push(Check::expect(
        sandbox_contract::WORKDIR_WRITABLE,
        cwd == "yes",
        || "the working directory is not writable".to_owned(),
    ));
    Ok(checks)
}

fn held_in_scope<S: Limits, E: Execution>(
    system: &S,
    exec: &E,
    checks: &mut Vec<Check>,
) -> Result<()> {
    std::thread::scope(|scope| {
        let hold = scope.spawn(|| {
            let outcome = system.build(&[Probe::Hold]);
            system.abandon_hold()?;
            outcome
        });
        let signalled = match system.await_hold() {
            Ok(signalled) => signalled,
            Err(error) => {
                // The held build must end before its thread is joined.
                let _ = system.kill_server();
                let _ = system.abandon_hold();
                let _ = join_build(hold);
                return Err(error);
            }
        };
        let facts = match signalled {
            Signalled::Started => system.running_scope()?,
            Signalled::Abandoned => None,
        };
        let Some(facts) = facts else {
            checks.push(Check::failed(
                sandbox_contract::HOLD_HAS_SCOPE,
                match signalled {
                    Signalled::Started => "no scope was observed for the hold action",
                    Signalled::Abandoned => "the hold build ended before its orphan signalled",
                },
            ));
            // A held build that runs without a scope ends only with the server, which is then
            // started again so that the out-of-memory checks still run.
            if signalled == Signalled::Started {
                system.kill_server()?;
                let _ = join_build(hold);
                system.start_server()?;
            } else {
                let _ = join_build(hold);
            }
            return Ok(());
        };
        checks.push(Check::passed(sandbox_contract::HOLD_HAS_SCOPE));
        let members = exec.cgroup_members(&facts.directory)?;
        let procs = exec.processes()?;
        let bwrap = procs
            .iter()
            .any(|proc| proc.role == Role::Bwrap && members.contains(&proc.pid));
        checks.push(Check::expect(
            sandbox_contract::BWRAP_IN_SCOPE,
            bwrap,
            || {
                format!(
                    "the scope {} holds {members:?}, no bwrap of the deployment",
                    facts.directory.display()
                )
            },
        ));
        let execs: Vec<_> = procs
            .iter()
            .filter(|proc| proc.role == Role::GangExec)
            .collect();
        let cgroups: Vec<_> = execs.iter().map(|proc| proc.cgroup.as_ref()).collect();
        let exec_outside = !execs.is_empty()
            && execs.iter().all(|proc| {
                !members.contains(&proc.pid)
                    && proc.cgroup.as_ref().is_some_and(|c| {
                        c.file_name().is_some_and(|name| name == WORKER_LEAF)
                            && c != &facts.directory
                    })
            });
        let exec_reason = if execs.is_empty() {
            "no gang exec runs".to_owned()
        } else {
            format!("gang exec runs in {cgroups:?}, not in its worker leaf")
        };
        checks.push(Check::expect(
            sandbox_contract::EXEC_OUTSIDE_SCOPE,
            exec_outside,
            || exec_reason,
        ));
        system.release(&facts)?;
        let outcome = join_build(hold)?;
        checks.push(Check::expect(
            sandbox_contract::HOLD_COMPLETES,
            outcome.succeeded(),
            || {
                format!(
                    "the held action ended with exit code {:?}",
                    outcome.exit_code
                )
            },
        ));
        Ok(())
    })
}

fn join_build(handle: ScopedJoinHandle<'_, Result<BuildOutcome>>) -> Result<BuildOutcome> {
    handle
        .join()
        .map_err(|_| E2eError::system("the build thread panicked"))?
}
