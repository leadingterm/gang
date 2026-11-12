//! The restart scenario: the server is killed while a build is scaling out and started again. The
//! build must survive, the scale-out must not repeat, and the fleet must converge to one worker.

use std::thread::{self, ScopedJoinHandle};

use crate::check::{Check, awaited};
use crate::errors::{E2eError, Result};
use crate::event_log::scale_out_recorded;
use crate::names;
use crate::system::{Limits, Probe, ScopeFacts, System};
use crate::verdict;

/// What the restart observes while the held build runs.
struct Restarted {
    /// The checks judged so far.
    checks: Vec<Check>,
    /// The pid of the killed server.
    before: Option<u32>,
    /// The scope of the held build once the restarted server runs it.
    scope: Option<ScopeFacts>,
    /// Whether the hold action signalled that its orphan runs. When it did not, the failed check
    /// is in `checks` and `scope` is `None`.
    signalled: bool,
}

/// Records the scale-out, kills and restarts the server while `build` holds, and finds the held
/// build's scope. A hold that never signals is a failed check, and no scope is looked for.
fn restart_holding<S: System + Limits, T>(
    system: &S,
    build: &ScopedJoinHandle<'_, T>,
) -> Result<Restarted> {
    let recorded = system.await_history(&mut scale_out_recorded);
    let mut checks = vec![awaited(
        names::restart::SCALE_OUT_RECORDED,
        recorded.map(|_| ()),
    )?];
    checks.push(Check::expect(
        names::restart::BUILD_STILL_RUNNING,
        !build.is_finished(),
        || "the held build finished before the server was killed".to_owned(),
    ));
    let before = system.server_pid();
    system.kill_server()?;
    system.start_server()?;
    let signal = crate::limits::hold_check(system.await_hold())?;
    let signalled = signal.is_passed();
    if !signalled {
        checks.push(signal);
    }
    Ok(Restarted {
        checks,
        before,
        scope: if signalled {
            system.running_scope()?
        } else {
            None
        },
        signalled,
    })
}

/// Kills and restarts the server while a held build scales out, and judges the outcome.
///
/// # Errors
///
/// When the server cannot be started, killed, or observed, or Bazel cannot be run.
pub fn restart<S: System + Limits>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    thread::scope(|scope| -> Result<Vec<Check>> {
        let handle = scope.spawn(|| {
            let outcome = system.build(&[Probe::Hold]);
            let released = system.abandon_hold();
            outcome.and_then(|build| released.map(|()| build))
        });
        let Restarted {
            mut checks,
            before,
            scope: facts,
            signalled,
        } = match restart_holding(system, &handle) {
            Ok(restarted) => restarted,
            Err(error) => {
                crate::limits::end_held_build(system);
                return Err(error);
            }
        };
        let Some(facts) = facts else {
            if signalled {
                checks.push(Check::failed(
                    names::limits::HOLD_HAS_SCOPE,
                    "no scope was observed for the held build",
                ));
            }
            // The server must go first: the build then fails at once instead of at its timeout.
            if let Err(error) = system.kill_server() {
                crate::limits::end_held_build(system);
                return Err(error);
            }
            let _ = handle.join();
            return Ok(checks);
        };
        if let Err(error) = system.release(&facts) {
            crate::limits::end_held_build(system);
            return Err(error);
        }
        let outcome = handle
            .join()
            .map_err(|_| E2eError::system("the build thread panicked"))??;
        let events = system.scaling_events()?;
        checks.push(Check::expect(
            names::restart::BUILD_SURVIVES,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ));
        let after = system.server_pid();
        checks.push(Check::expect(
            names::restart::SERVER_RESTARTED,
            before.is_some() && after.is_some() && before != after,
            || format!("the server process went from {before:?} to {after:?}"),
        ));
        checks.push(verdict::scale_outs(
            &events,
            names::restart::SCALE_OUT_NOT_REPEATED,
            1,
        ));
        let workers = system.worker_count()?;
        checks.push(Check::expect(
            names::restart::CONVERGED,
            workers == 1,
            || format!("{workers} workers remain"),
        ));
        Ok(checks)
    })
}
