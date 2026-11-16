//! Verdicts for failed launches and withdrawal of abandoned demand.

use gang_clock::Timestamp;
use gang_e2e::Check;
use gang_e2e::launch_failure::{failure_checks, withdrawn_checks};
use gang_wire::{Event, EventChange, Fleet, LaunchFailure, PassState, Queue, State};

const FLEET: &str = "fleets/test";

fn event(change: EventChange) -> Event {
    Event {
        name: "events/1".to_owned(),
        create_time: Timestamp::from_unix_millis(0),
        fleet: FLEET.to_owned(),
        cause: None,
        change,
    }
}

fn failed_state() -> State {
    State {
        queue: Queue {
            total: 1,
            ..Queue::default()
        },
        fleets: vec![Fleet {
            name: FLEET.to_owned(),
            desired: 1,
            launch_failure: Some(LaunchFailure {
                reason: "spawn failed".to_owned(),
            }),
            ..Fleet::default()
        }],
        ..State::default()
    }
}

#[test]
fn failure_requires_reason_unmet_demand_and_one_fleet_event() {
    let state = failed_state();
    let failure = event(EventChange::LaunchFailed {
        from: 0,
        to: 1,
        reason: "spawn failed".to_owned(),
    });
    assert!(
        failure_checks(&state, std::slice::from_ref(&failure))
            .iter()
            .all(Check::is_passed)
    );
    assert!(failure_checks(&state, &[failure.clone(), failure.clone()])[1].is_failure());
    assert!(failure_checks(&state, &[])[1].is_failure());
    let mut other = failure.clone();
    other.fleet = "fleets/other".to_owned();
    assert!(failure_checks(&state, &[other])[1].is_failure());
    let mut skipped = state.clone();
    skipped.pass.state = PassState::Skipped;
    assert!(failure_checks(&skipped, std::slice::from_ref(&failure))[2].is_failure());
    let mut missing_reason = state.clone();
    missing_reason.fleets[0]
        .launch_failure
        .as_mut()
        .expect("failure exists")
        .reason
        .clear();
    assert!(failure_checks(&missing_reason, std::slice::from_ref(&failure))[0].is_failure());
    let mut no_queue = state.clone();
    no_queue.queue.total = 0;
    assert!(failure_checks(&no_queue, std::slice::from_ref(&failure))[0].is_failure());
    let mut met = state;
    met.fleets[0].instances.in_service = 1;
    assert!(failure_checks(&met, &[failure])[0].is_failure());
}

#[test]
fn withdrawal_requires_zero_desired_no_failure_and_matching_event() {
    let state = State {
        fleets: vec![Fleet {
            name: FLEET.to_owned(),
            ..Fleet::default()
        }],
        ..State::default()
    };
    let withdrawal = event(EventChange::Withdrawn { from: 1, to: 0 });
    assert!(
        withdrawn_checks(&state, std::slice::from_ref(&withdrawal), FLEET)
            .iter()
            .all(Check::is_passed)
    );
    assert!(withdrawn_checks(&state, &[], FLEET)[1].is_failure());
    let mut other = withdrawal.clone();
    other.fleet = "fleets/other".to_owned();
    assert!(withdrawn_checks(&state, &[other], FLEET)[1].is_failure());
    let mut pinned = state.clone();
    pinned.fleets[0].desired = 1;
    assert!(withdrawn_checks(&pinned, std::slice::from_ref(&withdrawal), FLEET)[0].is_failure());
    let mut lingering_failure = state;
    lingering_failure.fleets[0].launch_failure = Some(LaunchFailure {
        reason: "spawn failed".to_owned(),
    });
    assert!(withdrawn_checks(&lingering_failure, &[withdrawal], FLEET)[0].is_failure());
}
