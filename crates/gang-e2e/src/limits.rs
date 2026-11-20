//! The per-action limits scenario: scope enforcement, cleanup, healthy memory spikes, and continued
//! worker service.

use std::thread::ScopedJoinHandle;

use crate::Signalled;
use crate::build_output::BuildOutcome;
use crate::check::{Check, awaited};
use crate::errors::{E2eError, Result};
use crate::names;
use crate::system::{Limits, Probe};
use crate::verdict;

/// Runs the per-action limits scenario against `system` and returns its checks.
///
/// The held action's scope and orphan cleanup are checked first. The memory hog allocates 192 MiB
/// against an initial 64 MiB reservation on a healthy worker. Its first attempt must succeed;
/// reservations are scheduling estimates, not per-action memory ceilings.
///
/// The suite runs this under [`Sandbox::Noop`](crate::system::Sandbox::Noop): under bwrap the PID
/// namespace teardown kills the orphan anyway, so the scenario would not observe the cgroup kill
/// that ends it.
///
/// `stderr_tail_lines` is how many lines of the memory hog's stderr its failed check quotes.
///
/// # Errors
///
/// Returns the errors of the system's operations, and [`E2eError::System`] when the thread that
/// runs the held action panics. A hold action that never starts is a failed check, not a hang.
pub fn limits<S: Limits>(system: &S, stderr_tail_lines: usize) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = Vec::new();

    if !held_action(system, &mut checks)? {
        return Ok(checks);
    }

    // A healthy worker must not kill an action for exceeding its reservation.
    let hog = system.build(&[Probe::MemoryHog])?;
    let attempts = system.attempt_records()?;
    checks.push(Check::expect(
        names::limits::HARD_CEILING,
        hog.succeeded()
            && attempts.len() == 1
            && attempts.first().is_some_and(|attempt| {
                attempt.action == Probe::MemoryHog.as_str()
                    && attempt.memory_kb == crate::system::HOLD_MEMORY_KB
            }),
        || {
            format!(
                "the probe recorded {attempts:?}, expected one 64 MiB start; exit {:?}\n{}",
                hog.exit_code,
                hog.stderr_tail(stderr_tail_lines)
            )
        },
    ));

    let after = system.build(&[Probe::Pinned])?;
    checks.push(Check::expect(
        names::memory::SERVING_AFTER_KILL,
        after.succeeded(),
        || after.exit_summary(),
    ));

    Ok(checks)
}

/// Starts a held action, checks its scope while it runs, then releases it and waits for it to
/// finish. Returns whether a scope was observed; when the action did not signal, or none was
/// observed, the failed check is recorded in `checks` and the server is killed so the build ends.
///
/// # Errors
///
/// Returns the errors of the system's operations, and [`E2eError::System`] when the thread that
/// runs the held action panics.
fn held_action<S: Limits>(system: &S, checks: &mut Vec<Check>) -> Result<bool> {
    std::thread::scope(|scope| {
        let hold = scope.spawn(|| {
            let outcome = system.build(&[Probe::Hold]);
            system.abandon_hold()?;
            outcome
        });

        let mut prepare = || -> Result<_> {
            let signal = hold_check(system.await_hold())?;
            let facts = if signal.is_passed() {
                system.running_scope()?
            } else {
                None
            };
            let Some(facts) = facts else {
                if signal.is_failure() {
                    checks.push(signal);
                } else {
                    checks.push(Check::failed(
                        names::limits::HOLD_HAS_SCOPE,
                        "no scope was observed for the hold action",
                    ));
                }
                system.kill_server()?;
                return Ok(None);
            };

            checks.extend(verdict::scope_limits(&facts));
            checks.push(Check::expect(
                names::limits::ORPHAN_RUNS,
                system.orphan_alive()?,
                || "no process carries the orphan marker".to_owned(),
            ));
            system.release(&facts)?;
            Ok(Some(facts))
        };
        let facts = match prepare() {
            Ok(Some(facts)) => facts,
            Ok(None) => {
                let _ = join_build(hold);
                return Ok(false);
            }
            Err(error) => {
                end_held_build(system, hold);
                return Err(error);
            }
        };
        let outcome = join_build(hold)?;
        checks.push(Check::expect(
            names::limits::HOLD_COMPLETES,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ));
        checks.push(verdict::orphan_killed(system.orphan_alive()?));
        checks.push(verdict::scope_removed(system.scope_exists(&facts)?));
        Ok(true)
    })
}

/// Converts the hold protocol outcome into its check, preserving timeout handling.
///
/// # Errors
///
/// Returns any signal error other than a timeout.
pub(crate) fn hold_check(signal: Result<Signalled>) -> Result<Check> {
    match signal {
        Ok(Signalled::Started) => awaited(names::limits::HOLD_SIGNALLED, Ok(())),
        Ok(Signalled::Abandoned) => Ok(Check::failed(
            names::limits::HOLD_SIGNALLED,
            "the hold build ended before its orphan signalled",
        )),
        Err(error) => awaited(names::limits::HOLD_SIGNALLED, Err::<(), _>(error)),
    }
}

/// Ends a held build and joins its thread at best effort, so that the caller returns its own
/// error: the build is cancelled, the server killed, and a pending `await_hold` woken.
pub(crate) fn end_held_build<S: Limits>(
    system: &S,
    hold: ScopedJoinHandle<'_, Result<BuildOutcome>>,
) {
    let _ = system.cancel_hold();
    let _ = system.kill_server();
    let _ = system.abandon_hold();
    let _ = join_build(hold);
}

/// Waits for the thread that runs an action and returns what the action reported.
///
/// # Errors
///
/// Returns [`E2eError::System`] when the thread panicked, and the error of the build otherwise.
pub(crate) fn join_build(
    handle: ScopedJoinHandle<'_, Result<BuildOutcome>>,
) -> Result<BuildOutcome> {
    handle
        .join()
        .map_err(|_| E2eError::system("the build thread panicked"))?
}
