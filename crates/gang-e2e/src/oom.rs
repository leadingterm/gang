//! The out-of-memory kill: an action past its cgroup's `memory.max` is killed by the kernel, `gang exec`
//! survives it and removes the action's scope itself, and the worker keeps serving.

use crate::check::Check;
use crate::errors::Result;
use crate::execution::{Execution, held_build, running_scope};
use crate::names::oom as oom_names;
use crate::system::{Probe, System};

/// Proves that a kernel OOM kill removes its action scope and leaves the worker serving.
///
/// [`Probe::HeldHog`] waits for the go flag before it allocates, so its scope is observed while
/// it runs; the harness then releases it, the kernel kills it at `memory.max`, and that scope must
/// be gone once the build ends. This requires a running deployment with per-action cgroups and
/// `NativeLink`'s memory enforcement off. `next` is a probe this deployment has not built yet; it
/// must succeed after the kill.
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
    let mut checks = vec![Check::expect(
        oom_names::KILLED,
        !hog.succeeded() && hog.action_killed(),
        || {
            format!(
                "exit {:?}; no action reported `(Exit 137)` or `(Killed)`; the build said:\n{}",
                hog.exit_code,
                hog.stderr_tail(stderr_tail_lines)
            )
        },
    )];

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
