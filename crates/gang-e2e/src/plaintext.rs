//! The plaintext scenario: a plaintext deployment (`[server] transport = "plaintext"` with
//! `[authentication.*] none`) serves a cold build through `grpc://` with no credential helper.
//! Its `none:anonymous` caller is bound to the administrator role, so its admin routes admit an
//! anonymous caller, and its live state follows the fleet from zero workers to one and back.

use crate::check::{Check, awaited};
use crate::errors::Result;
use crate::names;
use crate::system::{Probe, System};
use crate::verdict;

/// Runs the plaintext scenario against `system` and returns its verdicts in the order reached.
///
/// The state is read before the build and after the drain; the scale-out and the drain are judged
/// from the stream opened after the build, which the server publishes whatever the delay between
/// a build's end and its publication. The completed count is judged on the last state, which the
/// server has published whatever the delay between an action and its count.
///
/// # Errors
///
/// When the system cannot be driven or observed; a system that misbehaves is a failed [`Check`].
pub fn plaintext<S: System>(system: &S) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    system.start_server()?;
    let workers = system.worker_count()?;
    checks.push(Check::expect(
        names::plaintext::STARTS_WITH_NO_WORKERS,
        workers == 0,
        || format!("{workers} workers were running before the build"),
    ));
    let idle = system.live_state()?;
    checks.push(verdict::state_idle(&idle));

    let outcome = system.build(&[Probe::Unconstrained])?;
    checks.push(verdict::remote_execution(
        &outcome,
        1,
        system.stderr_tail_lines(),
    ));

    match system.await_stream(&mut verdict::drain_streamed) {
        Ok(frames) => {
            checks.push(verdict::scaled_out_streamed(&frames));
            checks.push(Check::passed(names::plaintext::DRAIN_STREAMED));
            checks.push(verdict::stream_starts_with_snapshot(frames.first()));
            checks.push(verdict::stream_applies(&frames));
            checks.extend(verdict::stream_causes(&frames));
        }
        Err(error) => checks.push(awaited(
            names::plaintext::DRAIN_STREAMED,
            Err::<(), _>(error),
        )?),
    }
    let drained = system.live_state()?;
    checks.push(verdict::state_drained(&drained));
    checks.push(verdict::completed_increased(
        idle.completed,
        drained.completed,
    ));

    let status = system.anonymous_admin_status()?;
    checks.push(verdict::anonymous_administrator_admitted(status));

    Ok(checks)
}
