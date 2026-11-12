//! Round trips and wire shapes of [`gang_state::diff`] and [`gang_state::apply`].

use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_state::{ApplyError, apply, diff};
use gang_types::{FleetName, InstanceId, WorkerId};
use gang_wire::name;
use gang_wire::{
    Actions, Demand, Fleet, Instance, InstanceCounts, InstanceSize, Isa, Lifecycle, Pass, Phase,
    Policy, Queue, QueuedAction, RunningAction, SkipReason, State, Truncated, Worker, WorkerCounts,
};
use gang_wire::{Cause, Event, EventChange};
use serde_json::json;

/// Number of state pairs the random round trip checks.
const PAIRS: usize = 200;
/// Seed of the xorshift generator; every run sees the same pairs.
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
/// Slots of the worker, instance and running-action pools.
const WORKERS: usize = 8;
const INSTANCES: usize = 5;
const RUNNING: usize = 6;
/// Fleet names, in declaration order.
const FLEETS: &[&str] = &["alpha", "beta", "gamma", "delta"];
/// Staleness of the sampled actions, constant for a run as the builder sets it.
const STALE_AFTER: Duration = Duration::from_secs(1);

/// A xorshift generator with a fixed seed, so every run produces the same states.
struct Rng(u64);

impl Rng {
    fn step(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.step() % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn small(&mut self) -> u32 {
        u32::try_from(self.below(1_000)).unwrap_or(0)
    }
}

/// Picks one of `options` at random.
fn pick<T: Clone>(rng: &mut Rng, options: &[T]) -> Result<T, Box<dyn Error>> {
    let len = u64::try_from(options.len())?;
    let index = usize::try_from(rng.below(len))?;
    Ok(options.get(index).ok_or("no options to pick from")?.clone())
}

fn timestamp(rng: &mut Rng) -> Timestamp {
    Timestamp::from_unix_millis(1_700_000_000_000 + rng.below(100_000))
}

fn fleet_name(fleet: &str) -> Result<String, Box<dyn Error>> {
    Ok(name::of::<name::Fleets>(&FleetName::try_new(fleet)?))
}

fn worker_name(slot: usize) -> Result<String, Box<dyn Error>> {
    Ok(name::of::<name::Workers>(&WorkerId::try_new(format!(
        "w-{slot:02}"
    ))?))
}

fn instance_name(slot: usize) -> Result<String, Box<dyn Error>> {
    Ok(name::of::<name::Instances>(&InstanceId::try_new(format!(
        "i-{slot:02}"
    ))?))
}

fn event_name(seq: u64) -> String {
    name::of::<name::Events>(&seq)
}

fn timestamped(seq: u64) -> Timestamp {
    Timestamp::from_unix_millis(1_700_000_000_000 + seq)
}

/// A scaling event at `seq` in the fixed fleet `alpha`.
fn event(seq: u64) -> Result<Event, Box<dyn Error>> {
    Ok(Event {
        name: event_name(seq),
        create_time: timestamped(seq),
        fleet: fleet_name("alpha")?,
        cause: Some(Cause::Demand),
        change: EventChange::ScaledOut { from: 1, to: 2 },
    })
}

fn ok_pass(applied: u32, failed: u32) -> Pass {
    Pass::ok(Duration::from_secs(1), applied, failed)
}

/// A state with one fleet-free, action-free shape; tests set the parts they need.
fn base_state() -> State {
    State {
        name: name::STATE.to_owned(),
        seq: 1,
        update_time: timestamped(1),
        provisioner: "gang-test".to_owned(),
        pass: ok_pass(0, 0),
        events_limit: 4,
        ..State::default()
    }
}

fn worker(slot: usize, phase: Phase, running: u32) -> Result<Worker, Box<dyn Error>> {
    Ok(Worker {
        name: worker_name(slot)?,
        fleet: fleet_name("alpha")?,
        instance: instance_name(slot % INSTANCES)?,
        phase,
        phase_time: timestamped(1),
        running,
        slots: Some(4),
        paused: false,
        launch_time: timestamped(1),
    })
}

fn instance(slot: usize, lifecycle: Lifecycle) -> Result<Instance, Box<dyn Error>> {
    Ok(Instance {
        name: instance_name(slot)?,
        fleet: fleet_name("alpha")?,
        lifecycle,
        launch_time: timestamped(1),
        past_boot_grace: false,
    })
}

fn running(slot: usize, priority: i32) -> Result<RunningAction, Box<dyn Error>> {
    Ok(RunningAction {
        name: name::of::<name::Actions>(&format!("op-{slot:02}")),
        fleet: fleet_name("alpha")?,
        worker: worker_name(slot % WORKERS)?,
        digest: format!("{slot:02}/10"),
        target: "//pkg:target".to_owned(),
        mnemonic: "GoCompile".to_owned(),
        start_time: timestamped(1),
        priority,
    })
}

fn queued(op: &str) -> Result<QueuedAction, Box<dyn Error>> {
    Ok(QueuedAction {
        name: name::of::<name::Actions>(&op.to_owned()),
        digest: "ab/3".to_owned(),
        target: "//pkg:queued".to_owned(),
        mnemonic: "Link".to_owned(),
        queue_time: timestamped(1),
        priority: 0,
        candidates: vec![fleet_name("alpha")?],
    })
}

fn actions(running: Vec<RunningAction>, queued: Vec<QueuedAction>, truncated: u32) -> Actions {
    Actions {
        sample_time: timestamped(2),
        stale_after: STALE_AFTER,
        running,
        queued,
        truncated: Truncated {
            running: truncated,
            queued: 0,
        },
    }
}

/// Builds the random generator's state pool: a fixed population evolved one step at a time.
struct World {
    seq: u64,
    update_time: Timestamp,
    pass: Pass,
    queue: Queue,
    fleets: Vec<Fleet>,
    workers: Vec<Option<Worker>>,
    instances: Vec<Option<Instance>>,
    running: Vec<Option<RunningAction>>,
    actions_on: bool,
    sample_time: Timestamp,
    queued: Vec<QueuedAction>,
    truncated: Truncated,
    completed: u64,
    principals_epoch: u64,
    events_limit: u32,
    log: Vec<Event>,
}

fn random_pass(rng: &mut Rng) -> Result<Pass, Box<dyn Error>> {
    if rng.chance(50) {
        return Ok(ok_pass(rng.small() % 4, rng.small() % 2));
    }
    Ok(Pass::skipped(
        Duration::from_secs(1),
        pick(
            rng,
            &[
                SkipReason::SchedulerUnavailable,
                SkipReason::ProvisionerUnavailable,
            ],
        )?,
        rng.chance(50).then(|| timestamp(rng)),
        rng.small() % 3,
    ))
}

fn random_queue(rng: &mut Rng) -> Queue {
    Queue {
        total: rng.small() % 9,
        unplaceable: rng.small() % 3,
        oldest_queue_time: rng.chance(50).then(|| timestamp(rng)),
    }
}

fn random_fleet(rng: &mut Rng, fleet: &str) -> Result<Fleet, Box<dyn Error>> {
    let mut properties = BTreeMap::new();
    properties.insert("zone".to_owned(), "a".to_owned());
    Ok(Fleet {
        name: fleet_name(fleet)?,
        isa: Isa::X86_64,
        sandbox: "none".to_owned(),
        properties,
        instance_size: Some(InstanceSize {
            vcpus: 4,
            memory: 8_589_934_592,
        }),
        policy: Policy {
            min: 0,
            max: 8,
            target_actions: 4,
            idle_timeout: Duration::from_mins(1),
            max_drain_wait: Duration::from_mins(5),
            boot_grace: Duration::from_mins(2),
        },
        desired: rng.small() % 9,
        instances: InstanceCounts {
            pending: rng.small() % 2,
            in_service: rng.small() % 5,
            terminating: rng.small() % 2,
            warm: rng.small() % 3,
        },
        workers: WorkerCounts {
            busy: rng.small() % 3,
            idle: rng.small() % 3,
            draining: rng.small() % 2,
            unconnected: rng.small() % 2,
        },
        demand: Demand {
            running: rng.small() % 6,
            queued: rng.small() % 6,
            candidates: rng.small() % 6,
            needed: rng.small() % 6,
            vcpus: rng.small(),
            memory: u64::from(rng.small()),
        },
    })
}

/// Evolves each slot: an empty slot may fill, a filled one may empty or change, and most keep
/// their value, so the two states share keys and many rows are unchanged.
fn evolve<T: Clone>(
    rng: &mut Rng,
    rows: &mut [Option<T>],
    make: impl Fn(&mut Rng, usize) -> Result<T, Box<dyn Error>>,
    tweak: impl Fn(&mut Rng, &mut T) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    for (slot, row) in rows.iter_mut().enumerate() {
        let roll = rng.below(100);
        if row.is_none() {
            if roll < 30 {
                *row = Some(make(rng, slot)?);
            }
        } else if roll < 10 {
            *row = None;
        } else if roll < 40
            && let Some(value) = row.as_mut()
        {
            tweak(rng, value)?;
        }
    }
    Ok(())
}

fn random_phase(rng: &mut Rng) -> Result<Phase, Box<dyn Error>> {
    pick(rng, &[Phase::Busy, Phase::Idle, Phase::Draining])
}

fn random_lifecycle(rng: &mut Rng) -> Result<Lifecycle, Box<dyn Error>> {
    pick(
        rng,
        &[
            Lifecycle::Pending,
            Lifecycle::InService,
            Lifecycle::Terminating,
        ],
    )
}

fn random_worker(rng: &mut Rng, slot: usize) -> Result<Worker, Box<dyn Error>> {
    let phase = random_phase(rng)?;
    let mut value = worker(slot, phase, rng.small() % 4)?;
    value.slots = rng.chance(50).then(|| rng.small() % 8);
    value.paused = rng.chance(20);
    value.phase_time = timestamp(rng);
    Ok(value)
}

fn tweak_worker(rng: &mut Rng, value: &mut Worker) -> Result<(), Box<dyn Error>> {
    value.phase = random_phase(rng)?;
    value.running = rng.small() % 4;
    value.paused = rng.chance(20);
    Ok(())
}

fn random_instance(rng: &mut Rng, slot: usize) -> Result<Instance, Box<dyn Error>> {
    let mut value = instance(slot, random_lifecycle(rng)?)?;
    value.past_boot_grace = rng.chance(50);
    Ok(value)
}

fn tweak_instance(rng: &mut Rng, value: &mut Instance) -> Result<(), Box<dyn Error>> {
    value.lifecycle = random_lifecycle(rng)?;
    value.past_boot_grace = rng.chance(50);
    Ok(())
}

fn random_running(rng: &mut Rng, slot: usize) -> Result<RunningAction, Box<dyn Error>> {
    let priority = i32::try_from(rng.small()).unwrap_or(0);
    running(slot, priority)
}

fn tweak_running(rng: &mut Rng, value: &mut RunningAction) {
    value.priority = i32::try_from(rng.small()).unwrap_or(0);
    value.digest = format!("{:02}/{}", rng.small() % 100, rng.small());
}

/// Changes the contents of the fleets in place. The fleet set and its declaration order are fixed
/// once the world is built, as they are for a running process.
fn evolve_fleets(rng: &mut Rng, fleets: &mut [Fleet]) {
    for fleet in fleets.iter_mut() {
        if rng.chance(30) {
            fleet.desired = rng.small() % 9;
        }
    }
}

fn random_queued(rng: &mut Rng) -> Result<Vec<QueuedAction>, Box<dyn Error>> {
    let count = rng.below(3);
    (0..count)
        .map(|index| queued(&format!("q-{index}")))
        .collect()
}

fn scaling_event(rng: &mut Rng, seq: u64) -> Result<Event, Box<dyn Error>> {
    Ok(Event {
        name: event_name(seq),
        create_time: timestamp(rng),
        fleet: fleet_name(pick(rng, FLEETS)?)?,
        cause: rng.chance(50).then_some(Cause::Demand),
        change: EventChange::ScaledOut {
            from: rng.small() % 4,
            to: rng.small() % 8,
        },
    })
}

fn push_event(rng: &mut Rng, log: &mut Vec<Event>) -> Result<(), Box<dyn Error>> {
    let seq = u64::try_from(log.len())? + 1;
    log.push(scaling_event(rng, seq)?);
    Ok(())
}

fn random_world(rng: &mut Rng) -> Result<World, Box<dyn Error>> {
    let mut world = World {
        seq: 1 + rng.below(1_000),
        update_time: timestamp(rng),
        pass: random_pass(rng)?,
        queue: random_queue(rng),
        fleets: Vec::new(),
        workers: vec![None; WORKERS],
        instances: vec![None; INSTANCES],
        running: vec![None; RUNNING],
        actions_on: rng.chance(60),
        sample_time: timestamp(rng),
        queued: random_queued(rng)?,
        truncated: Truncated {
            running: rng.small() % 3,
            queued: rng.small() % 3,
        },
        completed: rng.below(1_000),
        principals_epoch: rng.below(100),
        events_limit: 1 + u32::try_from(rng.below(4))?,
        log: Vec::new(),
    };
    for (slot, row) in world.workers.iter_mut().enumerate() {
        if rng.chance(50) {
            *row = Some(random_worker(rng, slot)?);
        }
    }
    for (slot, row) in world.instances.iter_mut().enumerate() {
        if rng.chance(50) {
            *row = Some(random_instance(rng, slot)?);
        }
    }
    for (slot, row) in world.running.iter_mut().enumerate() {
        if rng.chance(50) {
            *row = Some(random_running(rng, slot)?);
        }
    }
    for fleet in FLEETS {
        if rng.chance(50) {
            world.fleets.push(random_fleet(rng, fleet)?);
        }
    }
    for _ in 0..rng.below(5) {
        push_event(rng, &mut world.log)?;
    }
    Ok(world)
}

/// Moves the world one step on. Actions, once on, stay on: this generator only switches them on,
/// and the builder keeps the previous frame's actions when the controller has no sample.
fn advance(rng: &mut Rng, world: &mut World) -> Result<(), Box<dyn Error>> {
    world.seq += 1;
    world.update_time = timestamp(rng);
    if rng.chance(50) {
        world.pass = random_pass(rng)?;
    }
    if rng.chance(50) {
        world.queue = random_queue(rng);
    }
    evolve(rng, &mut world.workers[..], random_worker, tweak_worker)?;
    evolve(
        rng,
        &mut world.instances[..],
        random_instance,
        tweak_instance,
    )?;
    evolve(rng, &mut world.running[..], random_running, |rng, value| {
        tweak_running(rng, value);
        Ok(())
    })?;
    evolve_fleets(rng, &mut world.fleets);
    if !world.actions_on && rng.chance(40) {
        world.actions_on = true;
    }
    if rng.chance(30) {
        world.sample_time = timestamp(rng);
    }
    if rng.chance(30) {
        world.queued = random_queued(rng)?;
    }
    if rng.chance(30) {
        world.truncated = Truncated {
            running: rng.small() % 3,
            queued: rng.small() % 3,
        };
    }
    if rng.chance(30) {
        world.completed += u64::from(rng.small());
    }
    if rng.chance(20) {
        world.principals_epoch = rng.below(100);
    }
    for _ in 0..rng.below(3) {
        push_event(rng, &mut world.log)?;
    }
    Ok(())
}

/// The state the world currently describes. The events are the last `events_limit` of the log.
fn snapshot(world: &World) -> State {
    let end = world.log.len();
    let limit = usize::try_from(world.events_limit).unwrap_or(usize::MAX);
    let start = end.saturating_sub(limit);
    let events = world
        .log
        .get(start..end)
        .map(<[Event]>::to_vec)
        .unwrap_or_default();
    let running: Vec<RunningAction> = world.running.iter().flatten().cloned().collect();
    State {
        name: name::STATE.to_owned(),
        seq: world.seq,
        update_time: world.update_time,
        provisioner: "gang-test".to_owned(),
        pass: world.pass.clone(),
        queue: world.queue.clone(),
        fleets: world.fleets.clone(),
        workers: world.workers.iter().flatten().cloned().collect(),
        instances: world.instances.iter().flatten().cloned().collect(),
        actions: world.actions_on.then(|| Actions {
            sample_time: world.sample_time,
            stale_after: STALE_AFTER,
            running,
            queued: world.queued.clone(),
            truncated: world.truncated,
        }),
        events,
        events_limit: world.events_limit,
        completed: world.completed,
        principals_epoch: world.principals_epoch,
    }
}

#[test]
fn random_pairs_round_trip() -> Result<(), Box<dyn Error>> {
    let mut rng = Rng(SEED);
    for pair in 0..PAIRS {
        let mut world = random_world(&mut rng)?;
        let prev = snapshot(&world);
        advance(&mut rng, &mut world)?;
        let cur = snapshot(&world);
        if let Some(delta) = diff(&prev, &cur) {
            let mut state = prev.clone();
            apply(&mut state, &delta)?;
            assert_eq!(state, cur, "pair {pair} does not round trip");
        } else {
            let mut same = cur.clone();
            same.seq = prev.seq;
            same.update_time = prev.update_time;
            assert_eq!(same, prev, "pair {pair} has no delta but differs");
        }
    }
    Ok(())
}

#[test]
fn interleaved_worker_changes_round_trip() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.workers = [1, 3, 5, 7, 9, 11]
        .into_iter()
        .map(|slot| worker(slot, Phase::Idle, 0))
        .collect::<Result<_, _>>()?;

    let mut cur = prev.clone();
    cur.seq += 1;
    cur.update_time = timestamped(cur.seq);
    cur.workers = [0, 1, 4, 5, 8, 9, 12]
        .into_iter()
        .map(|slot| worker(slot, if slot == 5 { Phase::Busy } else { Phase::Idle }, 0))
        .collect::<Result<_, _>>()?;

    let delta = diff(&prev, &cur).ok_or("expected worker changes")?;
    apply(&mut prev, &delta)?;
    assert_eq!(prev, cur);
    Ok(())
}

#[test]
fn unchanged_state_has_no_delta() -> Result<(), Box<dyn Error>> {
    let mut rng = Rng(SEED);
    let world = random_world(&mut rng)?;
    let state = snapshot(&world);
    assert_eq!(diff(&state, &state.clone()), None);
    Ok(())
}

#[test]
fn first_actions_sample_carries_every_field() -> Result<(), Box<dyn Error>> {
    let prev = base_state();
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.actions = Some(actions(
        vec![running(0, 1)?, running(1, 2)?],
        vec![queued("q-0")?],
        3,
    ));
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let patch = delta.actions.ok_or("expected an actions patch")?;
    assert_eq!(patch.sample_time, Some(timestamped(2)));
    assert_eq!(patch.stale_after, Some(STALE_AFTER));
    assert_eq!(patch.running.map(|keyed| keyed.upsert.len()), Some(2));
    assert_eq!(patch.queued.as_ref().map(Vec::len), Some(1));
    assert_eq!(
        patch.truncated,
        Some(Truncated {
            running: 3,
            queued: 0
        })
    );
    Ok(())
}

#[test]
fn later_actions_sample_carries_only_changes() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.actions = Some(actions(vec![running(0, 1)?], vec![], 0));
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.actions = Some(Actions {
        queued: vec![queued("q-0")?],
        stale_after: STALE_AFTER * 2,
        ..cur.actions.clone().ok_or("actions set")?
    });
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let patch = delta.actions.ok_or("expected an actions patch")?;
    assert_eq!(patch.sample_time, None);
    assert_eq!(patch.stale_after, None);
    assert_eq!(patch.running, None);
    assert_eq!(patch.truncated, None);
    assert_eq!(patch.queued.as_ref().map(Vec::len), Some(1));
    Ok(())
}

#[test]
fn events_are_appended_only() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.events_limit = 10;
    prev.events = vec![event(1)?, event(2)?];
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.events = vec![event(1)?, event(2)?, event(3)?, event(4)?];
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let names: Vec<&str> = delta
        .events
        .iter()
        .map(|event| event.name.as_str())
        .collect();
    assert_eq!(names, ["events/3", "events/4"]);
    let mut state = prev.clone();
    apply(&mut state, &delta)?;
    assert_eq!(state, cur);
    Ok(())
}

#[test]
fn events_after_a_full_rollover_are_all_new() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.events_limit = 2;
    prev.events = vec![event(1)?, event(2)?];
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.events = vec![event(5)?, event(6)?];
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    assert_eq!(delta.events, cur.events);
    Ok(())
}

#[test]
fn apply_trims_events_to_the_limit() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.events_limit = 3;
    prev.events = vec![event(1)?, event(2)?, event(3)?];
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.events = vec![event(3)?, event(4)?, event(5)?];
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let mut state = prev.clone();
    apply(&mut state, &delta)?;
    assert_eq!(state.events, cur.events);
    Ok(())
}

#[test]
fn base_mismatch_leaves_state_unchanged() -> Result<(), Box<dyn Error>> {
    let prev = base_state();
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.completed = 7;
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let mut other = prev.clone();
    other.seq = 9;
    let before = other.clone();
    assert_eq!(
        apply(&mut other, &delta),
        Err(ApplyError::BaseMismatch { state: 9, base: 1 })
    );
    assert_eq!(other, before);
    Ok(())
}

#[test]
fn worker_upsert_and_instance_remove_have_the_wire_shape() -> Result<(), Box<dyn Error>> {
    let mut prev = base_state();
    prev.workers = vec![worker(0, Phase::Idle, 0)?];
    prev.instances = vec![instance(1, Lifecycle::InService)?];
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.workers = vec![worker(0, Phase::Busy, 1)?];
    cur.instances = Vec::new();
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let value = serde_json::to_value(&delta)?;
    assert_eq!(value["seq"], json!(2));
    assert_eq!(value["base"], json!(1));
    assert_eq!(value["workers"]["upsert"][0]["name"], json!("workers/w-00"));
    assert_eq!(value["workers"]["upsert"][0]["phase"], json!("BUSY"));
    assert_eq!(value["instances"]["remove"], json!(["instances/i-01"]));
    assert!(value["instances"].get("upsert").is_none());
    Ok(())
}

#[test]
fn vanished_fleet_is_removed_and_dropped() -> Result<(), Box<dyn Error>> {
    let mut rng = Rng(SEED);
    let mut prev = base_state();
    prev.fleets = vec![
        random_fleet(&mut rng, FLEETS[0])?,
        random_fleet(&mut rng, FLEETS[1])?,
    ];
    let mut cur = prev.clone();
    cur.seq = 2;
    cur.fleets = vec![random_fleet(&mut rng, FLEETS[1])?];
    let delta = diff(&prev, &cur).ok_or("expected a delta")?;
    let value = serde_json::to_value(&delta)?;
    assert_eq!(value["fleets"]["remove"], json!([fleet_name(FLEETS[0])?]));
    let mut state = prev.clone();
    apply(&mut state, &delta)?;
    assert_eq!(state, cur);
    Ok(())
}
