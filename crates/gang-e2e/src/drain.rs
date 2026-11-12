//! Judges the drain of the workers a step started, shared by the scenarios that scale out.

use std::borrow::Cow;

use crate::check::{Check, awaited};
use crate::errors::Result;
use crate::event_log::{connected_workers, events_after_seq, fleet_drained, scale_out_recorded};
use crate::system::System;
use crate::verdict;

/// The names of the two verdicts of a drain.
pub(crate) struct DrainChecks {
    /// The name of the verdict that the fleet drains to zero workers.
    pub drained: Cow<'static, str>,
    /// The name of the verdict that every drained worker is recorded as terminated.
    pub terminated: Cow<'static, str>,
}

/// Judges the drain of the workers a step started, from the events recorded after `since`.
///
/// A step that recorded no scale-out started no worker: it is judged from one read of the history
/// (no worker may be connected) and `terminated` is skipped. Otherwise the drain is awaited, with
/// the deadline of [`System::await_history`]; a deadline that passes is a failed check.
///
/// # Errors
///
/// When the system cannot be observed; a system that never drains is a failed [`Check`].
pub(crate) fn settle<S: System>(
    system: &S,
    since: u64,
    names: DrainChecks,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let history = system.scaling_events()?;
    if !scale_out_recorded(events_after_seq(&history, since)) {
        let connected = connected_workers(&history);
        checks.push(Check::expect(names.drained, connected == 0, || {
            format!("{connected} workers are still connected")
        }));
        checks.push(Check::skipped(
            names.terminated,
            "the step started no worker",
        ));
        return Ok(());
    }
    let drained =
        system.await_history(&mut |history| fleet_drained(events_after_seq(history, since)));
    checks.push(awaited(names.drained, drained.map(|_| ()))?);
    let terminated = system.await_history(&mut |history| {
        verdict::every_drain_terminated(events_after_seq(history, since))
    });
    checks.push(awaited(names.terminated, terminated.map(|_| ()))?);
    Ok(())
}
