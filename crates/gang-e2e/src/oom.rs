//! Held action scope cleanup and kernel action-pool OOM classification.

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::{Execution, held_build, running_scope};
use crate::names::oom as oom_names;
use crate::system::{AttemptRecord, Limits, Probe, System};

/// Checks eventual completion, removal of the first observed scope, and continued service.
///
/// The held action allocates memory after the scope is observed; success alone does not imply
/// a kernel kill. The separate pool-OOM scenario observes `memory.events` explicitly.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn oom<S: System, E: Execution>(
    system: &S,
    exec: &E,
    next: Probe,
    stderr_tail_lines: usize,
) -> Result<Vec<Check>> {
    exec.set_go(false)?;
    let (hog, scope) = held_build(system, exec, &[Probe::HeldHog], |lines| {
        let scope = match lines.await_prefix("hog started")? {
            Some(_) => running_scope(exec)?,
            None => None,
        };
        Ok(scope)
    })?;
    let mut checks = vec![Check::expect(oom_names::KILLED, hog.succeeded(), || {
        format!(
            "exit {:?}; the build said:\n{}",
            hog.exit_code,
            hog.stderr_tail(stderr_tail_lines)
        )
    })];

    checks.push(match &scope {
        None => Check::failed(
            oom_names::SCOPE_REMOVED,
            "no scope was observed for the action",
        ),
        Some(scope) => Check::expect(
            oom_names::SCOPE_REMOVED,
            !exec.cgroup_exists(scope)?,
            || format!("{} still exists", scope.display()),
        ),
    });

    let after = system.build(&[next])?;
    checks.push(Check::expect(oom_names::SERVING, after.succeeded(), || {
        format!(
            "the next action ended with exit code {:?}; the build said:\n{}",
            after.exit_code,
            after.stderr_tail(stderr_tail_lines)
        )
    }));
    Ok(checks)
}

/// Judges the kernel pool OOM and the resource requeue independently of the build exit.
#[must_use]
pub fn pool_oom_verdicts(
    events: u64,
    succeeded: bool,
    attempts: &[AttemptRecord],
    escalation_percent: u32,
    capacity_kb: u64,
) -> Vec<Check> {
    let mut victim = None;
    let mut neighbour = None;
    let mut valid = true;
    let mut retried = false;
    for attempt in attempts {
        let last = if attempt.action == Probe::PressureVictim.as_str() {
            &mut victim
        } else if attempt.action == Probe::PressureNeighbour.as_str() {
            &mut neighbour
        } else {
            valid = false;
            continue;
        };
        let memory = attempt.memory_kb;
        valid &= memory > 0 && memory <= capacity_kb;
        if let Some(previous) = *last {
            retried = true;
            let previous = u128::from(previous);
            let required = (previous * u128::from(escalation_percent))
                .div_ceil(100)
                .max(previous + 1)
                .min(u128::from(capacity_kb));
            valid &= u128::from(memory) > previous && u128::from(memory) >= required;
        }
        *last = Some(memory);
    }
    vec![
        Check::expect("kernel pool OOM is observed", events > 0, || {
            format!("pool oom_kill count: {events}")
        }),
        Check::expect(
            "kernel pool OOM is requeued with a larger reservation",
            succeeded && valid && retried && victim.is_some() && neighbour.is_some(),
            || format!("build succeeded={succeeded}, attempts={attempts:?}"),
        ),
    ]
}

/// Runs two held allocations that jointly exceed a bounded action pool, then releases the survivor.
///
/// # Errors
///
/// Returns errors from the deployment, journal, or cgroup observation.
pub fn pool_oom<S: Limits, E: Execution>(
    system: &S,
    exec: &E,
    escalation_percent: u32,
    capacity_kb: u64,
) -> Result<Vec<Check>> {
    system.start_server()?;
    exec.set_go(false)?;
    let (build, events) = held_build(
        system,
        exec,
        &[Probe::PressureVictim, Probe::PressureNeighbour],
        |lines| {
            let mut victim = None;
            let mut neighbour = None;
            while victim.is_none() || neighbour.is_none() {
                let Some(line) = lines.await_prefix("pressure ")? else {
                    break;
                };
                let mut words = line.split_whitespace();
                let (Some("pressure"), Some(role), Some(pid), None) =
                    (words.next(), words.next(), words.next(), words.next())
                else {
                    return Err(E2eError::system(format!(
                        "invalid pool OOM journal line: {line}"
                    )));
                };
                let pid = pid
                    .parse()
                    .map_err(E2eError::system_from("parsing pool OOM probe pid"))?;
                match role {
                    "victim" if victim.is_none() => victim = Some(pid),
                    "neighbour" if neighbour.is_none() => neighbour = Some(pid),
                    _ => {
                        return Err(E2eError::system(format!(
                            "unexpected pool OOM journal line: {line}"
                        )));
                    }
                }
            }
            match (victim, neighbour) {
                (Some(victim), Some(neighbour)) => system.await_pool_oom(victim, neighbour),
                _ => Ok(0),
            }
        },
    )?;
    let attempts = system.attempt_records()?;
    Ok(pool_oom_verdicts(
        events,
        build.succeeded(),
        &attempts,
        escalation_percent,
        capacity_kb,
    ))
}
