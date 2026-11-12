//! Reusing a frame across passes: a refill must equal a blank fill, the event window must slide
//! correctly, and an unsampled pass must round-trip through diff and apply.

use gang_state::apply;
use gang_wire::{Event, EventChange};

use super::*;

/// The provisioner options of one fleet.
type Options = Vec<(&'static str, String)>;

/// A scaling event named `name`, the only field that the event window reads.
fn event(name: &str) -> Event {
    Event {
        name: name.to_owned(),
        create_time: ts(0),
        fleet: "fleets/alpha".to_owned(),
        cause: None,
        change: EventChange::ScaledOut { from: 1, to: 2 },
    }
}

/// The provisioner option `key` set to `value`.
fn option(key: &'static str, value: &str) -> (&'static str, String) {
    (key, value.to_owned())
}

/// Appends a fleet `name` with `options`, `desired` instances and the given supply.
fn push_fleet(
    world: &mut World,
    name: &str,
    options: Options,
    desired: u32,
    instances: Vec<FleetInstance>,
) -> TestResult {
    let spec = policy(name, 100, 4)?;
    world.properties.push(properties(&spec, options));
    world.policies.push(spec);
    world.supplies.push(FleetSupply {
        desired,
        instances,
        warm: 0,
        exits: Vec::new(),
    });
    world.outlook.push(FleetOutlook::default());
    Ok(())
}

/// The supply entry of instance `index` in `lifecycle`, launched at `launched` milliseconds.
fn instance(
    index: u64,
    lifecycle: InstanceLifecycle,
    launched: u64,
) -> Result<FleetInstance, Box<dyn Error>> {
    Ok(fleet_instance(&instance_id(index)?, lifecycle, launched))
}

/// Appends the worker of instance `index`, in `fleet`, with `running` actions.
fn worker(world: &mut World, index: u64, fleet: &str, running: u32, draining: bool) -> TestResult {
    let id = worker_on(&instance_id(index)?)?;
    world.snapshot.workers.push(worker_state(
        &id,
        fleet,
        running,
        draining,
        NonZeroU32::new(8),
    )?);
    Ok(())
}

/// Appends a queued action since `since` milliseconds, with candidate fleets `fleets`.
fn enqueue(world: &mut World, since: u64, fleets: &[usize]) {
    world.snapshot.queued.push(queued(since, fleet_set(fleets)));
}

/// A running action on the worker of instance `index`.
fn running(
    index: u64,
    op: &str,
    started: u64,
    priority: i32,
) -> Result<RunningActionView, Box<dyn Error>> {
    Ok(RunningActionView {
        info: ActionInfo {
            op: op.to_owned(),
            digest: format!("d/{op}"),
            target: format!("//t:{op}"),
            mnemonic: "Compile".to_owned(),
            priority,
        },
        worker: worker_on(&instance_id(index)?)?,
        started: ts(started),
    })
}

/// A queued action since `queued_since` milliseconds, with candidate fleets `fleets`.
fn waiting(op: &str, queued_since: u64, priority: i32, fleets: &[usize]) -> QueuedActionView {
    QueuedActionView {
        info: ActionInfo {
            op: op.to_owned(),
            digest: format!("d/{op}"),
            target: format!("//t:{op}"),
            mnemonic: "Link".to_owned(),
            priority,
        },
        queued_since: ts(queued_since),
        candidates: fleet_set(fleets),
    }
}

/// An actions sample taken at `at` milliseconds.
fn sample(
    at: u64,
    running: Vec<RunningActionView>,
    queued: Vec<QueuedActionView>,
) -> ActionsSample {
    ActionsSample {
        at: ts(at),
        view: ActionsView {
            running,
            queued,
            truncated_running: 0,
            truncated_queued: 0,
        },
    }
}

/// Six worlds that change what a refill must overwrite: fleet property keys, fleets in and out
/// of declaration order, workers and instances that appear and vanish, queued and sampled
/// actions, and the candidate fleets of queued rows.
fn churn_world(step: usize) -> Result<World, Box<dyn Error>> {
    match step {
        0 => churn_step_0(),
        1 => churn_step_1(),
        2 => churn_step_2(),
        3 => churn_step_3(),
        4 => churn_step_4(),
        5 => churn_step_5(),
        _ => Err(format!("no churn world {step}").into()),
    }
}

fn churn_step_0() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.a", "1"), option("custom.b", "2")],
        2,
        vec![
            instance(1, InstanceLifecycle::InService, 10)?,
            instance(2, InstanceLifecycle::Pending, 20)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "beta",
        vec![option("custom.c", "x")],
        1,
        vec![instance(3, InstanceLifecycle::InService, 30)?],
    )?;
    worker(&mut world, 1, "alpha", 1, false)?;
    worker(&mut world, 3, "beta", 0, true)?;
    enqueue(&mut world, 100, &[0, 1]);
    enqueue(&mut world, 200, &[1]);
    world.actions = Some(sample(
        4_000,
        vec![
            running(1, "op-b", 3_000, -2)?,
            running(3, "op-a", 3_100, 5)?,
        ],
        vec![
            waiting("op-q", 3_500, 1, &[0, 1]),
            waiting("op-r", 3_600, 0, &[1]),
        ],
    ));
    Ok(world)
}

fn churn_step_1() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.a", "1"), option("custom.d", "4")],
        3,
        vec![
            instance(1, InstanceLifecycle::InService, 10)?,
            instance(2, InstanceLifecycle::InService, 20)?,
            instance(5, InstanceLifecycle::InService, 50)?,
            instance(6, InstanceLifecycle::Terminating, 60)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "beta",
        vec![option("custom.c", "y")],
        1,
        vec![instance(3, InstanceLifecycle::InService, 30)?],
    )?;
    push_fleet(
        &mut world,
        "gamma",
        vec![option("custom.e", "e"), option("custom.a", "1")],
        0,
        vec![instance(4, InstanceLifecycle::InService, 40)?],
    )?;
    worker(&mut world, 1, "alpha", 3, false)?;
    worker(&mut world, 3, "beta", 0, true)?;
    worker(&mut world, 5, "alpha", 0, false)?;
    worker(&mut world, 9, "gamma", 2, false)?;
    enqueue(&mut world, 100, &[0, 1]);
    enqueue(&mut world, 200, &[1]);
    enqueue(&mut world, 250, &[2]);
    world.actions = Some(sample(
        5_000,
        vec![
            running(1, "op-b", 3_000, -2)?,
            running(5, "op-c", 4_500, 0)?,
            running(3, "op-a", 3_100, 5)?,
            running(9, "op-d", 4_800, 1)?,
        ],
        vec![
            waiting("op-q", 3_500, 1, &[0, 1]),
            waiting("op-r", 3_600, 0, &[1]),
            waiting("op-t", 4_700, 2, &[0, 1, 2]),
        ],
    ));
    Ok(world)
}

fn churn_step_2() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.a", "9")],
        1,
        vec![instance(5, InstanceLifecycle::InService, 50)?],
    )?;
    push_fleet(&mut world, "beta", Vec::new(), 0, Vec::new())?;
    worker(&mut world, 5, "alpha", 0, false)?;
    world.actions = Some(sample(
        6_000,
        vec![running(5, "op-c", 4_500, 0)?],
        Vec::new(),
    ));
    Ok(world)
}

fn churn_step_3() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "beta",
        vec![option("custom.z", "q"), option("custom.a", "1")],
        2,
        vec![
            instance(1, InstanceLifecycle::InService, 10)?,
            instance(3, InstanceLifecycle::Pending, 30)?,
            instance(8, InstanceLifecycle::InService, 80)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.b", "2"), option("custom.a", "1")],
        1,
        vec![instance(7, InstanceLifecycle::InService, 70)?],
    )?;
    worker(&mut world, 1, "beta", 0, false)?;
    worker(&mut world, 3, "alpha", 0, true)?;
    worker(&mut world, 7, "alpha", 2, false)?;
    enqueue(&mut world, 300, &[0]);
    world.actions = Some(sample(
        7_000,
        vec![running(7, "op-e", 6_500, 0)?],
        vec![
            waiting("op-s", 6_800, 0, &[0]),
            waiting("op-t", 6_900, 3, &[0, 1]),
            waiting("op-u", 6_950, 1, &[1]),
        ],
    ));
    Ok(world)
}

fn churn_step_4() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.a", "1")],
        0,
        vec![
            instance(7, InstanceLifecycle::Terminating, 70)?,
            instance(9, InstanceLifecycle::Pending, 90)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "beta",
        vec![option("custom.c", "x"), option("custom.b", "3")],
        0,
        Vec::new(),
    )?;
    worker(&mut world, 1, "beta", 0, false)?;
    worker(&mut world, 7, "alpha", 0, false)?;
    world.actions = Some(sample(
        8_000,
        Vec::new(),
        vec![waiting("op-v", 7_900, 0, &[1])],
    ));
    Ok(world)
}

fn churn_step_5() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    push_fleet(
        &mut world,
        "alpha",
        vec![option("custom.a", "1"), option("custom.b", "2")],
        2,
        vec![
            instance(1, InstanceLifecycle::InService, 10)?,
            instance(5, InstanceLifecycle::InService, 50)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "beta",
        vec![option("custom.c", "y")],
        1,
        vec![
            instance(3, InstanceLifecycle::InService, 30)?,
            instance(8, InstanceLifecycle::InService, 80)?,
        ],
    )?;
    push_fleet(
        &mut world,
        "gamma",
        vec![option("custom.e", "e")],
        1,
        vec![
            instance(4, InstanceLifecycle::InService, 40)?,
            instance(9, InstanceLifecycle::Pending, 90)?,
        ],
    )?;
    worker(&mut world, 1, "alpha", 1, false)?;
    worker(&mut world, 3, "beta", 0, true)?;
    worker(&mut world, 5, "alpha", 0, false)?;
    worker(&mut world, 7, "gamma", 0, false)?;
    worker(&mut world, 9, "gamma", 1, false)?;
    enqueue(&mut world, 100, &[0, 1]);
    enqueue(&mut world, 400, &[2]);
    world.actions = Some(sample(
        9_000,
        vec![
            running(1, "op-b", 3_000, -2)?,
            running(9, "op-d", 8_800, 1)?,
            running(3, "op-a", 3_100, 5)?,
        ],
        vec![
            waiting("op-q", 3_500, 1, &[0, 1]),
            waiting("op-w", 8_500, 0, &[2]),
            waiting("op-x", 8_600, 0, &[]),
        ],
    ));
    Ok(world)
}

/// Fills `cur` for `world` on top of `prev`, with the given scaling `events` as the window.
fn fill_window(
    builder: &mut Builder,
    cur: &mut State,
    prev: &State,
    seq: u64,
    world: &World,
    wall: u64,
    events: &[Event],
) -> TestResult {
    let passes = world.passes();
    let catalog = FleetCatalog::try_new(world.properties.clone())?;
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut world.snapshot.clone());
    let inputs = Inputs {
        wall: ts(wall),
        pass: PassStatus::Ok {
            applied: 0,
            failed: 0,
            hosted: &hosted,
        },
        fleets: &passes,
        catalog: &catalog,
        actions: world.actions.as_ref(),
        events,
        principals_epoch: 7,
    };
    builder.fill(cur, prev, seq, &inputs);
    Ok(())
}

#[test]
fn reused_frames_equal_chained_blank_fills_across_changing_worlds() -> TestResult {
    let mut reused = Builder::new(config());
    let mut reference = Builder::new(config());
    let mut a = blank();
    let mut b = blank();
    let mut expected = blank();
    for step in 0..6 {
        let world = churn_world(step)?;
        let seq = u64::try_from(step)? + 1;
        let wall = 10_000 + 1_000 * seq;
        let (cur, prev) = if step % 2 == 0 {
            (&mut a, &b)
        } else {
            (&mut b, &a)
        };
        fill_into(&mut reused, cur, prev, seq, &world, wall, ok(0))?;

        expected = fill(&mut reference, &expected, seq, &world, wall, ok(0))?;
        assert_eq!(*cur, expected, "reused frame differs at pass {step}");
    }
    Ok(())
}

#[test]
fn event_window_slide_is_correct_across_reused_frames() -> TestResult {
    let events: Vec<Event> = (1..=7).map(|n| event(&format!("e{n}"))).collect();
    let windows: [&[Event]; 5] = [
        &events[0..2],
        &events[1..4],
        &events[4..6],
        &events[0..0],
        &events[6..7],
    ];
    let world = smaller_world()?;
    let mut builder = Builder::new(config());
    let mut a = blank();
    let mut b = blank();
    for (index, window) in windows.iter().enumerate() {
        let seq = u64::try_from(index)? + 1;
        let (cur, prev) = if index % 2 == 0 {
            (&mut a, &b)
        } else {
            (&mut b, &a)
        };
        fill_window(&mut builder, cur, prev, seq, &world, 1_000 * seq, window)?;
        assert_eq!(cur.events, *window, "event window {index} after the slide");
    }
    Ok(())
}

#[test]
fn a_pass_without_a_sample_after_a_sample_round_trips_through_diff_and_apply() -> TestResult {
    let mut world = larger_world()?;
    let mut builder = Builder::new(config());
    let first = fill(&mut builder, &blank(), 1, &world, 5_000, ok(0))?;
    assert!(first.actions.is_some(), "the first pass carries a sample");

    world.actions = None;
    let second = fill(&mut builder, &first, 2, &world, 6_000, ok(0))?;
    assert_eq!(second.actions, first.actions, "no sample keeps the actions");

    if let Some(delta) = diff(&first, &second) {
        assert!(
            delta.actions.is_none(),
            "an unsampled pass moves no actions"
        );
        let mut rebuilt = first.clone();
        apply(&mut rebuilt, &delta)?;
        assert_eq!(rebuilt, second);
    } else {
        let mut aligned = second.clone();
        aligned.seq = first.seq;
        aligned.update_time = first.update_time;
        assert_eq!(aligned, first);
    }
    Ok(())
}
