//! A failed local launch remains visible until abandoned demand is withdrawn.

use std::time::Duration;

use gang_wire::{Event, EventChange, PassState, State};
use tokio::sync::oneshot;

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::stack::Stack;
use crate::system::{Probe, System};

const OBSERVE_TIMEOUT: Duration = Duration::from_mins(1);
const ABANDON_TIMEOUT: Duration = Duration::from_secs(20);
const WITHDRAW_TIMEOUT: Duration = Duration::from_secs(6);

fn failed_request(state: &State) -> bool {
    state.queue.total > 0
        && state.fleets.first().is_some_and(|fleet| {
            fleet.launch_failure.is_some() && fleet.desired > fleet.instances.in_service
        })
}

/// Judges the visible failure, the pass that reported it, and its event history.
#[must_use]
pub fn failure_checks(state: &State, events: &[Event]) -> Vec<Check> {
    let fleet = state.fleets.first();
    let reason = fleet.and_then(|row| row.launch_failure.as_ref());
    let count = fleet.map_or(0, |row| {
        events
            .iter()
            .filter(|event| {
                event.fleet == row.name && matches!(event.change, EventChange::LaunchFailed { .. })
            })
            .count()
    });
    vec![
        Check::expect(
            "launch failure is visible with unmet demand",
            failed_request(state)
                && reason.is_some_and(|failure| !failure.reason.trim().is_empty()),
            || format!("state: {state:?}"),
        ),
        Check::expect("one launch failure event is recorded", count == 1, || {
            format!("recorded {count} launch failure events")
        }),
        Check::expect(
            "the pass that reports the failure ran",
            state.pass.state == PassState::Ok,
            || format!("pass: {:?}", state.pass),
        ),
    ]
}

/// Judges that an abandoned fleet request was cleared and recorded.
#[must_use]
pub fn withdrawn_checks(state: &State, events: &[Event], fleet: &str) -> Vec<Check> {
    let row = state.fleets.iter().find(|row| row.name == fleet);
    let withdrawn = events
        .iter()
        .any(|event| event.fleet == fleet && matches!(event.change, EventChange::Withdrawn { .. }));
    vec![
        Check::expect(
            "abandoned launch request is cleared",
            row.is_some_and(|row| row.desired == 0 && row.launch_failure.is_none()),
            || format!("fleet: {row:?}"),
        ),
        Check::expect("withdrawal is recorded", withdrawn, || {
            format!("no withdrawal event for {fleet}")
        }),
    ]
}

/// Runs the failure and withdrawal scenario with the real Bazel client.
pub(crate) fn launch_failure(stack: &Stack<'_>) -> Result<Vec<Check>> {
    stack.start_server()?;
    std::thread::scope(|scope| {
        let (stop, stopped) = oneshot::channel();
        let build = scope.spawn(|| stack.build_until_stopped(&[Probe::Pinned], stopped));
        let failed = stack.wait_live_state(OBSERVE_TIMEOUT, failed_request);
        let history = stack.scaling_events();
        let _ = stop.send("launch failure observed".to_owned());
        let joined = build
            .join()
            .map_err(|_| E2eError::system("the build thread panicked"));
        let killed = stack.kill_bazel();
        let _ = joined?;
        killed?;
        let failed = failed?;
        let mut checks = failure_checks(&failed, &history?);
        let fleet = failed
            .fleets
            .first()
            .ok_or_else(|| E2eError::system("the state has no fleet"))?
            .name
            .clone();
        stack.wait_live_state(ABANDON_TIMEOUT, |state| state.queue.total == 0)?;
        let cleared =
            stack.wait_live_state(WITHDRAW_TIMEOUT, |state| {
                state.fleets.iter().any(|row| {
                    row.name == fleet && row.desired == 0 && row.launch_failure.is_none()
                }) && state.events.iter().any(|event| {
                    event.fleet == fleet && matches!(event.change, EventChange::Withdrawn { .. })
                })
            })?;
        let final_history = stack.scaling_events()?;
        let failure_count = final_history
            .iter()
            .filter(|event| {
                event.fleet == fleet && matches!(event.change, EventChange::LaunchFailed { .. })
            })
            .count();
        checks.push(Check::expect(
            "launch failure is not repeated before withdrawal",
            failure_count == 1,
            || format!("recorded {failure_count} launch failure events"),
        ));
        checks.extend(withdrawn_checks(&cleared, &final_history, &fleet));
        Ok(checks)
    })
}
