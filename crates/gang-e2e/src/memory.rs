//! The soft memory scenario: an action that outgrows its memory reservation is killed by
//! `NativeLink`'s sampler and requeued on a doubled reservation, on which it completes, and the
//! worker keeps serving.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::system::{Limits, Probe};
use crate::verdict;

/// Runs the soft memory scenario against `system` and returns its checks.
///
/// [`Probe::MemoryGrower`] plateaus above the soft limit of its reservation and below the kernel's
/// ceiling, and holds the plateau on its first attempt, so only `NativeLink`'s soft kill can end
/// that attempt. The scheduler requeues it on a doubled reservation, on which it completes at
/// once: the build must succeed and the host-side counter must hold exactly two attempts. The
/// next action must still succeed.
///
/// The suite runs this under [`Sandbox::Noop`](crate::system::Sandbox::Noop) with the workers'
/// cgroup limits and `NativeLink`'s soft memory enforcement on.
///
/// `stderr_tail_lines` is how many lines of the build's stderr the failed serving check quotes.
///
/// # Errors
///
/// Returns the errors of the system's operations. A system that misbehaves is a failed
/// [`Check`].
pub fn memory<S: Limits>(system: &S, stderr_tail_lines: usize) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = Vec::new();

    let grower = system.build(&[Probe::MemoryGrower])?;
    checks.push(verdict::soft_memory_requeue(&grower, system.attempts()?));

    let after = system.build(&[Probe::Pinned])?;
    checks.push(Check::expect(
        names::memory::SERVING_AFTER_KILL,
        after.succeeded(),
        || {
            format!(
                "the next action ended with exit code {:?}; the build said:\n{}",
                after.exit_code,
                after.stderr_tail(stderr_tail_lines)
            )
        },
    ));

    Ok(checks)
}
