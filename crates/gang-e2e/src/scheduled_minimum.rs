//! A standing worker created solely by a scheduled minimum, with its cause and policy visible.

use gang_wire::{Cause, EventChange};

use crate::check::{Check, awaited};
use crate::control::{Control, latest_holds, states};
use crate::errors::Result;
use crate::names::scheduled_minimum as names;

/// Starts a deployment without a build and checks its scheduled minimum.
///
/// # Errors
///
/// When the server cannot start or the state stream cannot be read.
pub fn scheduled_minimum<S: Control>(system: &S) -> Result<Vec<Check>> {
    system.start_server()?;
    let frames = match system
        .await_stream(&mut |frames| latest_holds(frames, |state| !state.workers.is_empty()))
    {
        Ok(frames) => frames,
        Err(error) => {
            let mut checks = vec![awaited(names::WORKER_STANDS, Err::<(), _>(error))?];
            checks.extend(Check::skipped_all(
                [names::SCALED_OUT_FOR_MINIMUM, names::POLICY_MIN],
                "no worker connected",
            ));
            return Ok(checks);
        }
    };
    let states = states(&frames)?;
    let events: Vec<_> = states.iter().flat_map(|state| &state.events).collect();
    let scaled = events.iter().any(|event| {
        matches!(event.change, EventChange::ScaledOut { from: 0, to: 1 })
            && event.cause == Some(Cause::Minimum)
    });
    let min = states
        .last()
        .and_then(|state| state.fleets.first())
        .map(|fleet| fleet.policy.min);
    Ok(vec![
        Check::passed(names::WORKER_STANDS),
        Check::expect(names::SCALED_OUT_FOR_MINIMUM, scaled, || {
            format!("no scale-out from 0 to 1 with cause MINIMUM in streamed events: {events:?}")
        }),
        Check::expect(names::POLICY_MIN, min == Some(1), || {
            format!("expected policy minimum 1 with configured min_instances 0, got {min:?}")
        }),
    ])
}
