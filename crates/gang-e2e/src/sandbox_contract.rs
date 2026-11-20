//! The sandbox contract under bwrap: the worker environment is hidden, the rootfs is
//! read-only, /tmp is fresh, and the action's working directory is writable.

use std::collections::BTreeSet;

use crate::check::Check;
use crate::errors::Result;
use crate::execution::{Execution, Role, WORKER_LEAF};
use crate::hold::Signalled;
use crate::limits::{end_held_build, join_build};
use crate::names::sandbox_contract;
use crate::oom::oom;
use crate::report::ProbeReport;
use crate::system::{Limits, Probe, ScopeFacts, System};

/// Proves the bwrap sandbox contract, held-action cgroup placement, cleanup, and continued service.
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
            Check::failed(sandbox_contract::WORKDIR_WRITABLE, reason.clone()),
            Check::failed(sandbox_contract::IDENTITY, reason.clone()),
            Check::failed(sandbox_contract::NSS, reason.clone()),
            Check::failed(sandbox_contract::ETC_CONTENTS, reason.clone()),
            Check::failed(sandbox_contract::IDENTITY_ROOTS_READONLY, reason.clone()),
            Check::failed(sandbox_contract::ROOTS, reason.clone()),
            Check::failed(sandbox_contract::ROOTS_OVERRIDE, reason.clone()),
            Check::failed(sandbox_contract::POSTGRES, reason),
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

    let mut offenders = Vec::new();
    for path in ["/", "/etc", "/usr"] {
        match report.get(&format!("mount:{path}")) {
            Ok("erofs") => {}
            Ok(value) => offenders.push(format!("{path}: {value}")),
            Err(_) => offenders.push(format!("{path}: not mounted")),
        }
    }
    checks.push(Check::expect(
        sandbox_contract::READONLY_BINDS,
        offenders.is_empty(),
        || offenders.join("; "),
    ));
    let cwd = report.get("cwd_writable")?;
    checks.push(Check::expect(
        sandbox_contract::WORKDIR_WRITABLE,
        cwd == "yes",
        || "the working directory is not writable".to_owned(),
    ));
    action_environment_checks(&report, &mut checks)?;
    Ok(checks)
}

fn action_environment_checks(report: &ProbeReport, checks: &mut Vec<Check>) -> Result<()> {
    for (key, name) in [
        ("identity", sandbox_contract::IDENTITY),
        ("nss", sandbox_contract::NSS),
        ("etc_contents", sandbox_contract::ETC_CONTENTS),
        (
            "identity_roots_readonly",
            sandbox_contract::IDENTITY_ROOTS_READONLY,
        ),
        ("roots", sandbox_contract::ROOTS),
        ("roots_override", sandbox_contract::ROOTS_OVERRIDE),
        ("postgres", sandbox_contract::POSTGRES),
    ] {
        let value = report.get(key)?;
        checks.push(Check::expect(name, value == "yes", || {
            format!("{key}: {value}")
        }));
    }
    Ok(())
}

/// How the hold of the sandbox contract ended before its build is joined.
enum Held {
    /// The action was released and its build completes.
    Released,
    /// No scope was observed; the server was killed to end the build when `restart` holds.
    Unscoped { restart: bool },
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
        let observation = match observe_hold(system, exec, checks) {
            Ok(observation) => observation,
            Err(error) => {
                end_held_build(system, hold);
                return Err(error);
            }
        };
        match observation {
            Held::Released => {
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
            }
            Held::Unscoped { restart } => {
                let _ = join_build(hold);
                // The out-of-memory checks need the server the unscoped hold took down.
                if restart {
                    system.start_server()?;
                }
                Ok(())
            }
        }
    })
}

/// Awaits the hold, checks its scope, and releases it. A held build that runs without a scope
/// ends only with the server, which is killed.
fn observe_hold<S: Limits, E: Execution>(
    system: &S,
    exec: &E,
    checks: &mut Vec<Check>,
) -> Result<Held> {
    let signalled = system.await_hold()?;
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
        let restart = signalled == Signalled::Started;
        if restart {
            system.kill_server()?;
        }
        return Ok(Held::Unscoped { restart });
    };
    checks.push(Check::passed(sandbox_contract::HOLD_HAS_SCOPE));
    scope_checks(exec, &facts, checks)?;
    system.release(&facts)?;
    Ok(Held::Released)
}

/// Checks that bwrap runs inside the held action's scope and `gang exec` outside it.
fn scope_checks<E: Execution>(exec: &E, facts: &ScopeFacts, checks: &mut Vec<Check>) -> Result<()> {
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
                    c.file_name().is_some_and(|name| name == WORKER_LEAF) && c != &facts.directory
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
    Ok(())
}
