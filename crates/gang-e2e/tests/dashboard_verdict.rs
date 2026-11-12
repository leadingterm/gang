//! Behaviour of the pure verdicts of the dashboard scenarios over in-memory observations: the live
//! state, the frames of the state stream and the answers to requests with and without a session.

use std::collections::BTreeMap;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_e2e::verdict;
use gang_e2e::{Check, Outcome, follow, names};
use gang_wire::name::Collection;
use gang_wire::{
    Cause, Closed, ClosedReason, Delta, Demand, Event, EventChange, Fleet, Frame, Instance,
    InstanceCounts, InstanceSize, Keyed, Lifecycle, Pass, PassState, Phase, Policy, Queue, State,
    Tick, Worker, WorkerCounts, name,
};

const FLEET: &str = "local-worker-x86-64";
const WORKER: &str = "local-1-1f1d946a-7368-6397-b958-0648f8aa0688";
const INSTANCE: &str = "local-1";

fn at(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn fleet_name() -> String {
    format!("{}/{FLEET}", name::Fleets::NAME)
}

fn worker_name() -> String {
    format!("{}/{WORKER}", name::Workers::NAME)
}

fn instance_name() -> String {
    format!("{}/{INSTANCE}", name::Instances::NAME)
}

fn fleet(in_service: u32) -> Fleet {
    Fleet {
        name: fleet_name(),
        isa: gang_wire::Isa::X86_64,
        sandbox: "noop".to_owned(),
        properties: BTreeMap::new(),
        instance_size: Some(InstanceSize {
            vcpus: 4,
            memory: 8 << 30,
        }),
        policy: Policy {
            min: 0,
            max: 4,
            target_actions: 4,
            idle_timeout: Duration::from_secs(15),
            max_drain_wait: Duration::from_mins(1),
            boot_grace: Duration::from_secs(30),
        },
        desired: in_service,
        instances: InstanceCounts {
            pending: 0,
            in_service,
            terminating: 0,
            warm: 0,
        },
        workers: WorkerCounts {
            busy: 0,
            idle: in_service,
            draining: 0,
            unconnected: 0,
        },
        demand: Demand {
            running: 0,
            queued: 0,
            candidates: 0,
            needed: in_service,
            vcpus: 0,
            memory: 0,
        },
    }
}

fn worker(phase: Phase) -> Worker {
    Worker {
        name: worker_name(),
        fleet: fleet_name(),
        instance: instance_name(),
        phase,
        phase_time: at(0),
        running: 0,
        slots: Some(4),
        paused: false,
        launch_time: at(0),
    }
}

/// A state at `seq` whose one fleet counts `in_service` instances and which lists `workers`.
fn state(seq: u64, in_service: u32, workers: Vec<Worker>, completed: u64) -> State {
    State {
        name: name::STATE.to_owned(),
        seq,
        update_time: at(seq),
        provisioner: "local".to_owned(),
        pass: Pass {
            state: PassState::Ok,
            interval: Duration::from_millis(500),
            applied: 0,
            failed: 0,
            skip_reason: None,
            last_ok_time: None,
            skipped: None,
        },
        queue: Queue {
            total: 0,
            unplaceable: 0,
            oldest_queue_time: None,
        },
        fleets: vec![fleet(in_service)],
        workers,
        instances: Vec::new(),
        actions: None,
        events: Vec::new(),
        events_limit: 100,
        completed,
        principals_epoch: 0,
    }
}

fn running() -> State {
    state(1, 1, vec![worker(Phase::Idle)], 1)
}

fn empty() -> State {
    state(0, 0, Vec::new(), 0)
}

fn event(seq: u64, change: EventChange, cause: Option<Cause>) -> Event {
    Event {
        name: name::of::<name::Events>(&seq),
        create_time: at(seq),
        fleet: fleet_name(),
        cause,
        change,
    }
}

fn scaled_out(seq: u64, cause: Option<Cause>) -> Event {
    event(seq, EventChange::ScaledOut { from: 0, to: 1 }, cause)
}

fn drain_started(seq: u64, cause: Option<Cause>) -> Event {
    let change = EventChange::DrainStarted {
        worker: worker_name(),
    };
    event(seq, change, cause)
}

fn terminated(seq: u64, cause: Option<Cause>) -> Event {
    let change = EventChange::Terminated {
        instance: instance_name(),
    };
    event(seq, change, cause)
}

/// A delta from `base` to `seq` that changes nothing but carries `events`.
fn delta(base: u64, seq: u64, events: Vec<Event>) -> Delta {
    Delta {
        seq,
        base,
        update_time: at(seq),
        pass: None,
        queue: None,
        fleets: None,
        workers: None,
        instances: None,
        actions: None,
        events,
        completed: None,
        principals_epoch: None,
    }
}

/// The delta that ends the drain: the fleet counts nothing and the worker is gone.
fn emptying(base: u64, seq: u64, events: Vec<Event>) -> Delta {
    Delta {
        fleets: Some(Keyed {
            upsert: vec![fleet(0)],
            remove: Vec::new(),
        }),
        workers: Some(Keyed {
            upsert: Vec::new(),
            remove: vec![worker_name()],
        }),
        ..delta(base, seq, events)
    }
}

/// The delta that scales the fleet out: the fleet counts one instance and the worker is listed.
fn scaling_out(base: u64, seq: u64, events: Vec<Event>) -> Delta {
    Delta {
        fleets: Some(Keyed {
            upsert: vec![fleet(1)],
            remove: Vec::new(),
        }),
        workers: Some(Keyed {
            upsert: vec![worker(Phase::Idle)],
            remove: Vec::new(),
        }),
        ..delta(base, seq, events)
    }
}

fn snapshot(state: State, events: Vec<Event>) -> Frame {
    Frame::Snapshot(State { events, ..state })
}

fn tick() -> Frame {
    Frame::Tick(Tick {
        seq: 0,
        time: at(0),
    })
}

fn closed(reason: ClosedReason) -> Frame {
    Frame::Closed(Closed {
        reason,
        retry_delay: None,
    })
}

fn failure(check: &Check) -> &str {
    assert!(
        check.is_failure(),
        "expected a failure, got {:?}",
        check.outcome
    );
    match &check.outcome {
        Outcome::Failed(text) => text.as_str(),
        Outcome::Passed | Outcome::Skipped(_) => "",
    }
}

/// The frames of a stream that watched one worker run and drain.
fn drained_stream(with_causes: bool) -> Vec<Frame> {
    let cause = |cause: Cause| with_causes.then_some(cause);
    vec![
        snapshot(running(), vec![scaled_out(1, cause(Cause::Demand))]),
        Frame::Delta(delta(
            1,
            2,
            vec![drain_started(2, cause(Cause::IdleTimeout))],
        )),
        Frame::Delta(emptying(
            2,
            3,
            vec![terminated(3, cause(Cause::DrainFinished))],
        )),
    ]
}

#[test]
fn an_empty_state_is_idle_and_drained() {
    assert!(verdict::state_idle(&empty()).is_passed());
    assert!(verdict::state_drained(&empty()).is_passed());
    assert!(verdict::fleet_is_empty(&empty()));
}

#[test]
fn a_state_with_an_instance_or_a_worker_is_not_idle() {
    let busy = verdict::state_idle(&running());
    assert!(busy.is_failure());
    assert!(failure(&busy).contains("1 instances counted by the fleets"));
    assert!(failure(&busy).contains("1 workers"));

    let workerless = state(0, 1, Vec::new(), 0);
    assert!(verdict::state_idle(&workerless).is_failure());
    let instanceless = state(0, 0, vec![worker(Phase::Idle)], 0);
    assert!(verdict::state_idle(&instanceless).is_failure());
}

#[test]
fn an_instance_without_a_worker_keeps_the_fleet_from_being_empty() {
    let mut lingering = empty();
    lingering.instances.push(Instance {
        name: instance_name(),
        fleet: fleet_name(),
        lifecycle: Lifecycle::Terminating,
        launch_time: at(0),
        past_boot_grace: false,
    });
    assert!(!verdict::fleet_is_empty(&lingering));
    assert!(verdict::state_drained(&lingering).is_failure());
}

#[test]
fn terminating_instances_count_as_instances() {
    let mut terminating = empty();
    terminating.fleets[0].instances.terminating = 1;
    assert_eq!(verdict::instances_reported(&terminating), 1);
    assert!(!verdict::fleet_is_empty(&terminating));
}

#[test]
fn a_scale_out_after_the_snapshot_shows_an_instance_and_a_worker() {
    let frames = vec![
        snapshot(empty(), Vec::new()),
        Frame::Delta(scaling_out(0, 1, vec![scaled_out(1, Some(Cause::Demand))])),
    ];
    assert!(verdict::scaled_out_streamed(&frames).is_passed());
}

#[test]
fn a_snapshot_of_the_running_fleet_shows_an_instance_and_a_worker() {
    let frames = vec![snapshot(running(), Vec::new())];
    assert!(verdict::scaled_out_streamed(&frames).is_passed());
}

#[test]
fn a_stream_that_only_holds_an_instance_without_a_worker_fails() {
    let frames = vec![
        snapshot(state(1, 1, Vec::new(), 0), Vec::new()),
        Frame::Delta(delta(1, 2, Vec::new())),
    ];
    let check = verdict::scaled_out_streamed(&frames);
    assert!(failure(&check).contains("no frame of the stream showed an instance and a worker"));
    assert!(
        failure(&check).contains("1 instances counted by the fleets, 0 listed without a worker")
    );
}

#[test]
fn an_empty_fleet_alone_does_not_show_a_scale_out() {
    let frames = vec![snapshot(empty(), Vec::new())];
    let check = verdict::scaled_out_streamed(&frames);
    assert!(failure(&check).contains("0 instances counted by the fleets"));
    let silent = verdict::scaled_out_streamed(&[]);
    assert!(failure(&silent).contains("the stream carried no state"));
}

#[test]
fn a_delta_before_the_snapshot_fails_the_scale_out_check() {
    let frames = vec![Frame::Delta(scaling_out(0, 1, Vec::new()))];
    let check = verdict::scaled_out_streamed(&frames);
    assert!(failure(&check).contains("a delta arrived before the snapshot"));
}

#[test]
fn the_completed_count_must_rise() {
    assert!(verdict::completed_increased(0, 1).is_passed());
    let flat = verdict::completed_increased(3, 3);
    assert!(failure(&flat).contains("went from 3 to 3"));
    assert!(verdict::completed_increased(3, 2).is_failure());
}

#[test]
fn an_anonymous_administrator_is_admitted_with_200() {
    let admitted = verdict::anonymous_administrator_admitted(200);
    assert_eq!(admitted.name, names::plaintext::ADMIN_ADMITTED);
    assert!(admitted.is_passed());
    for status in [401, 403, 404] {
        let refused = verdict::anonymous_administrator_admitted(status);
        assert!(failure(&refused).contains(&format!("answered {status}, not 200")));
    }
}

#[test]
fn the_first_frame_must_be_a_snapshot() {
    let first = snapshot(empty(), Vec::new());
    assert!(verdict::stream_starts_with_snapshot(Some(&first)).is_passed());
    let late = tick();
    let check = verdict::stream_starts_with_snapshot(Some(&late));
    assert!(failure(&check).contains("`tick`"));
    let none = verdict::stream_starts_with_snapshot(None);
    assert!(failure(&none).contains("ended without an event"));
}

#[test]
fn streamed_events_join_snapshot_and_delta_events_once_per_name() {
    let frames = [
        snapshot(running(), vec![scaled_out(1, None), drain_started(2, None)]),
        Frame::Delta(delta(
            1,
            2,
            vec![drain_started(2, None), terminated(3, None)],
        )),
        tick(),
    ];
    let names: Vec<&str> = verdict::streamed_events(&frames)
        .iter()
        .map(|event| event.name.as_str())
        .collect();
    assert_eq!(names, ["events/1", "events/2", "events/3"]);
}

#[test]
fn a_stream_with_every_cause_passes_the_cause_checks() {
    let checks = verdict::stream_causes(&drained_stream(true));
    assert_eq!(checks.len(), 3);
    assert!(checks.iter().all(Check::is_passed), "{checks:?}");
}

#[test]
fn a_stream_without_causes_fails_every_cause_check() {
    let checks = verdict::stream_causes(&drained_stream(false));
    assert_eq!(checks.len(), 3);
    assert!(checks.iter().all(Check::is_failure));
    assert!(failure(&checks[0]).contains("scaled_out (no cause)"));
    assert!(failure(&checks[2]).contains("terminated (no cause)"));
}

#[test]
fn a_cause_that_belongs_to_another_decision_fails_its_check() {
    let frames = [
        snapshot(
            running(),
            vec![
                scaled_out(1, Some(Cause::Demand)),
                drain_started(2, Some(Cause::Orphan)),
            ],
        ),
        Frame::Delta(delta(2, 3, vec![terminated(3, Some(Cause::DrainFinished))])),
    ];
    let checks = verdict::stream_causes(&frames);
    assert!(checks[0].is_passed());
    assert!(failure(&checks[1]).contains("drain_started (ORPHAN)"));
    assert!(checks[2].is_passed());
}

#[test]
fn a_stream_that_carried_no_event_fails_the_cause_checks() {
    let checks = verdict::stream_causes(&[snapshot(empty(), Vec::new())]);
    assert!(checks.iter().all(Check::is_failure));
    assert!(failure(&checks[0]).contains("[]"));
}

#[test]
fn deltas_that_apply_in_order_pass_the_apply_check() {
    let frames = drained_stream(true);
    assert!(verdict::stream_applies(&frames).is_passed());
    let mirror = follow(&frames);
    let held = mirror.state.expect("the stream was followed");
    assert!(verdict::fleet_is_empty(&held));
    assert_eq!(held.seq, 3);
    assert_eq!(held.events.len(), 3);
}

#[test]
fn a_delta_from_another_state_fails_the_apply_check() {
    let frames = [
        snapshot(running(), Vec::new()),
        Frame::Delta(delta(5, 6, Vec::new())),
    ];
    let check = verdict::stream_applies(&frames);
    assert!(failure(&check).contains("does not match"));
    assert!(follow(&frames).state.is_none());
}

#[test]
fn a_delta_before_the_snapshot_fails_the_apply_check() {
    let frames = [Frame::Delta(delta(0, 1, Vec::new()))];
    let check = verdict::stream_applies(&frames);
    assert!(failure(&check).contains("before the snapshot"));
}

#[test]
fn a_later_snapshot_replaces_the_state() {
    let frames = [
        snapshot(running(), Vec::new()),
        Frame::Delta(delta(1, 2, Vec::new())),
        snapshot(empty(), Vec::new()),
        Frame::Delta(delta(0, 1, Vec::new())),
    ];
    let held = follow(&frames).state.expect("the stream was followed");
    assert!(verdict::fleet_is_empty(&held));
    assert_eq!(held.seq, 1);
}

#[test]
fn the_drain_is_streamed_once_a_termination_empties_the_fleet() {
    let frames = drained_stream(true);
    assert!(!verdict::drain_streamed(&frames[..1]));
    assert!(!verdict::drain_streamed(&frames[..2]));
    assert!(verdict::drain_streamed(&frames));
}

#[test]
fn a_drain_without_a_termination_is_not_streamed() {
    let frames = [
        snapshot(running(), Vec::new()),
        Frame::Delta(emptying(1, 2, vec![drain_started(2, None)])),
    ];
    assert!(!verdict::drain_streamed(&frames));
}

#[test]
fn a_termination_with_workers_left_is_not_a_streamed_drain() {
    let frames = [
        snapshot(running(), Vec::new()),
        Frame::Delta(delta(1, 2, vec![terminated(2, None)])),
    ];
    assert!(!verdict::drain_streamed(&frames));
}

#[test]
fn a_stream_that_cannot_be_followed_ends_the_wait() {
    let frames = [
        snapshot(running(), Vec::new()),
        Frame::Delta(delta(9, 10, Vec::new())),
    ];
    assert!(verdict::drain_streamed(&frames));
}

#[test]
fn only_401_refuses_a_request_without_a_cookie() {
    let refused = verdict::cookieless_refused("/v1/state", 401);
    assert_eq!(
        refused.name,
        names::dashboard::cookieless_refused("/v1/state")
    );
    assert!(refused.is_passed());
    for status in [200, 403, 404] {
        let served = verdict::cookieless_refused("/v1/state:watch", status);
        assert!(failure(&served).contains(&format!("answered {status}, not 401")));
        assert!(served.name.contains("/v1/state:watch"));
    }
}

#[test]
fn the_stream_must_be_a_200_event_stream() {
    assert!(verdict::stream_opened(200, Some("text/event-stream")).is_passed());
    assert!(verdict::stream_opened(200, Some("text/event-stream; charset=utf-8")).is_passed());
    assert!(verdict::stream_opened(401, Some("text/event-stream")).is_failure());
    assert!(verdict::stream_opened(200, Some("application/json")).is_failure());
    let headless = verdict::stream_opened(200, None);
    assert!(failure(&headless).contains("None"));
}

#[test]
fn a_closed_event_with_the_reason_revoked_passes() {
    let frames = [tick(), closed(ClosedReason::Revoked)];
    assert!(verdict::closed_on_revocation(&frames, false).is_passed());
}

#[test]
fn a_closed_event_with_another_reason_fails() {
    for reason in [
        ClosedReason::Expired,
        ClosedReason::ViewerCap,
        ClosedReason::Shutdown,
    ] {
        let check = verdict::closed_on_revocation(&[closed(reason.clone())], false);
        assert!(failure(&check).contains(&reason.to_string()));
    }
}

#[test]
fn a_stream_that_stays_open_or_ends_without_closed_fails() {
    let open = verdict::closed_on_revocation(&[tick(), tick()], false);
    assert!(failure(&open).contains("2 frames"));
    assert!(failure(&open).contains("stayed open"));
    let ended = verdict::closed_on_revocation(&[], true);
    assert!(failure(&ended).contains("ended"));
}
