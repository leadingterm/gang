//! Behaviour of the state builder, driven by hand-built scheduler and provisioner inputs.

mod reuse;

use std::error::Error;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::{TestClock, Timestamp};
use gang_config::FleetPolicy;
use gang_reconciler::{
    Action, FleetOutlook, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler,
    TerminationCause, WorkerCounts,
};
use gang_spi::{
    ActionInfo, ActionsView, EngineStore, FleetInstance, FleetSupply, MeasuredUsage,
    QueuedActionView, RunningActionView, SchedulerSnapshot, StoreKind, Traffic,
};
use gang_state::{ActionsSample, Builder, Inputs, PassStatus, diff};
use gang_testkit::{TestResult, worker_on};
use gang_types::{
    ActionResources, Bytes, FleetCatalog, FleetName, FleetProperties, FleetSet, InstanceId,
    InstanceLifecycle, WorkerId, property,
};
use gang_wire::name;
use gang_wire::{Pass, PassState, SkipReason, State};

use crate::support::{
    blank, build_key, config, fleet_instance, instance_id, policy, properties, queued, ts,
    worker_state,
};

/// Everything one control pass hands to the builder, owned so that the inputs can borrow it.
#[derive(Default)]
struct World {
    passes: Vec<FleetPass>,
    policies: Vec<FleetPolicy>,
    properties: Vec<FleetProperties>,
    supplies: Vec<FleetSupply>,
    outlook: Vec<FleetOutlook>,
    past_boot_grace: Vec<Vec<bool>>,
    snapshot: SchedulerSnapshot,
    actions: Option<ActionsSample>,
    builds: Option<gang_insight::Builds>,
    traffic: Traffic,
    measured: Option<MeasuredUsage>,
}

impl World {
    /// The fleet passes of this world, one per policy in declaration order.
    fn passes(&self) -> Vec<FleetPass> {
        if !self.passes.is_empty() {
            return self.passes.clone();
        }
        self.policies
            .iter()
            .enumerate()
            .map(|(index, policy)| FleetPass {
                policy: policy.clone(),
                supply: self.supplies.get(index).cloned().unwrap_or_default(),
                outlook: self.outlook.get(index).copied().unwrap_or_default(),
                past_boot_grace: self.past_boot_grace.get(index).cloned().unwrap_or_default(),
            })
            .collect()
    }
}

/// The result of one control pass, as handed to `fill`.
#[derive(Clone, Debug)]
enum Outcome {
    Ok { applied: u32, failed: u32 },
    Skipped(SkipReason),
}

fn ok(applied: u32) -> Outcome {
    Outcome::Ok { applied, failed: 0 }
}

fn action_info(op: &str, digest: &str, target: &str, mnemonic: &str, priority: i32) -> ActionInfo {
    ActionInfo {
        op: op.to_owned(),
        digest: digest.to_owned(),
        target: target.to_owned(),
        mnemonic: mnemonic.to_owned(),
        priority,
        build: None,
    }
}

fn running_action(info: ActionInfo, worker: &WorkerId, started: Timestamp) -> RunningActionView {
    RunningActionView {
        info,
        worker: worker.clone(),
        started,
    }
}

fn queued_action(
    info: ActionInfo,
    queued_since: Timestamp,
    candidates: FleetSet,
) -> QueuedActionView {
    QueuedActionView {
        info,
        queued_since,
        candidates,
        refusal: None,
    }
}

fn worker_name(id: &WorkerId) -> String {
    name::of::<name::Workers>(id)
}

fn instance_name(id: &InstanceId) -> String {
    name::of::<name::Instances>(id)
}

/// Builds one frame of `world` on top of `prev`.
fn fill(
    builder: &mut Builder,
    prev: &State,
    seq: u64,
    world: &World,
    wall: u64,
    outcome: Outcome,
) -> Result<State, Box<dyn Error>> {
    let mut cur = blank();
    fill_into(builder, &mut cur, prev, seq, world, wall, outcome)?;
    Ok(cur)
}

/// Builds one frame of `world` on top of `prev` into the caller-owned frame `cur`, which keeps
/// the rows it already holds.
fn fill_into(
    builder: &mut Builder,
    cur: &mut State,
    prev: &State,
    seq: u64,
    world: &World,
    wall: u64,
    outcome: Outcome,
) -> TestResult {
    let passes = world.passes();
    let catalog = FleetCatalog::try_new(world.properties.clone())?;
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut world.snapshot.clone());
    let pass = match outcome {
        Outcome::Ok { applied, failed } => PassStatus::Ok {
            applied,
            failed,
            hosted: &hosted,
        },
        Outcome::Skipped(reason) => PassStatus::Skipped(reason),
    };
    let empty_builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let inputs = Inputs {
        wall: ts(wall),
        pass,
        fleets: &passes,
        catalog: &catalog,
        actions: world.actions.as_ref(),
        events: &[],
        builds: world.builds.as_ref().unwrap_or(&empty_builds),
        traffic: world.traffic,
        measured: world.measured.as_ref(),
        principals_epoch: 7,
    };
    builder.fill(cur, prev, seq, &inputs);
    Ok(())
}

fn find_worker<'a>(cur: &'a State, id: &WorkerId) -> Option<&'a gang_wire::Worker> {
    let wanted = worker_name(id);
    cur.workers.iter().find(|worker| worker.name == wanted)
}

fn past_grace(cur: &State, id: &InstanceId) -> Option<bool> {
    let wanted = instance_name(id);
    cur.instances
        .iter()
        .find(|instance| instance.name == wanted)
        .map(|instance| instance.past_boot_grace)
}

/// One running action named `op` on `worker`, started at `started` milliseconds.
fn running_on(worker: &WorkerId, op: &str, started: u64) -> RunningActionView {
    RunningActionView {
        info: ActionInfo {
            op: op.to_owned(),
            digest: format!("d/{op}"),
            target: format!("//t:{op}"),
            mnemonic: "Compile".to_owned(),
            priority: 0,
            build: None,
        },
        worker: worker.clone(),
        started: ts(started),
    }
}

/// An actions sample taken at `at` milliseconds, with the given running actions and no
/// queued ones or truncation.
fn sample_at(at: u64, running: Vec<RunningActionView>) -> ActionsSample {
    ActionsSample {
        at: ts(at),
        view: ActionsView {
            running,
            queued: Vec::new(),
            truncated_running: 0,
            truncated_queued: 0,
        },
    }
}

#[test]
fn policy_min_is_the_minimum_in_force() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 30_000, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    world.outlook = vec![FleetOutlook {
        minimum: 2,
        ..FleetOutlook::default()
    }];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let raised = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert_eq!(raised.fleets[0].policy.min, 2);

    world.outlook[0].minimum = 0;
    let baseline = fill(&mut builder, &raised, 2, &world, 2_000, ok(0))?;
    assert_eq!(baseline.fleets[0].policy.min, 0);
    Ok(())
}

#[test]
fn fleets_follow_declaration_order_and_units() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("beta", 120_000, 4)?, policy("alpha", 30_000, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 2,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            abandoned: 0,
            desired: 1,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    ];
    let mut demand = FleetOutlook::default().demand;
    demand.add_queued(ActionResources {
        cpu_millicores: 2000,
        memory: Bytes::new(1_024),
        disk: Bytes::new(0),
    });
    world.outlook = vec![
        FleetOutlook {
            minimum: 0,
            needed: 2,
            demand,
            queued: 1,
            candidates: 3,
            workers: WorkerCounts::default(),
            readiness: None,
            expected_wait: None,
        },
        FleetOutlook::default(),
    ];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let cur = fill(&mut builder, &blank(), 5, &world, 6_000, ok(2))?;

    assert_eq!(cur.name, "state");
    assert_eq!(cur.seq, 5);
    assert_eq!(cur.update_time, ts(6_000));
    assert_eq!(cur.provisioner, "test-provisioner");
    assert_eq!(cur.events_limit, 10);
    assert_eq!(cur.principals_epoch, 7);
    let names: Vec<&str> = cur.fleets.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["fleets/beta", "fleets/alpha"]);
    assert_eq!(cur.fleets[0].desired, 2);
    assert_eq!(cur.fleets[1].desired, 1);
    assert_eq!(cur.fleets[0].demand.needed, 2);
    assert_eq!(cur.fleets[0].demand.candidates, 3);
    assert_eq!(cur.fleets[0].demand.queued, 1);
    assert_eq!(cur.fleets[0].demand.vcpus, 2);
    assert_eq!(cur.fleets[0].demand.memory, 1_024);
    Ok(())
}

#[test]
fn fleet_durations_follow_each_outlook_and_round_trip() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut prev = blank();
    let mut cur = blank();
    let values = [
        (None, None),
        (Some(Duration::ZERO), Some(Duration::from_millis(1500))),
        (Some(Duration::from_millis(250)), Some(Duration::ZERO)),
        (None, Some(Duration::from_secs(2))),
        (Some(Duration::from_secs(3)), None),
        (None, None),
    ];
    for (index, (readiness, expected_wait)) in values.into_iter().enumerate() {
        world.outlook = vec![FleetOutlook {
            readiness,
            expected_wait,
            ..FleetOutlook::default()
        }];
        fill_into(
            &mut builder,
            &mut cur,
            &prev,
            index as u64 + 1,
            &world,
            index as u64 * 1_000,
            ok(0),
        )?;
        assert_eq!(cur.fleets[0].demand.readiness, readiness);
        assert_eq!(cur.fleets[0].demand.expected_wait, expected_wait);
        if index > 0 {
            let delta = diff(&prev, &cur).ok_or("missing fleet delta")?;
            let mut applied = prev.clone();
            gang_state::apply(&mut applied, &delta)?;
            assert_eq!(applied, cur);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    Ok(())
}

#[test]
fn provisioner_properties_exclude_reserved_keys() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, vec![("custom.option", "v".to_owned())])];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let cur = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    let props = &cur.fleets[0].properties;
    assert_eq!(props.get("custom.option"), Some(&"v".to_owned()));
    for key in property::RESERVED {
        assert!(!props.contains_key(key), "reserved key {key} leaked");
    }
    Ok(())
}

#[test]
fn counts_workers_by_phase_and_instances_by_lifecycle() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    let busy = instance_id(1)?;
    let idle = instance_id(2)?;
    let draining = instance_id(3)?;
    let pending = instance_id(4)?;
    let unhosted = instance_id(5)?;
    let other = instance_id(9)?;
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 4,
            warm: 3,
            exits: Vec::new(),
            launch_failure: None,
            instances: vec![
                fleet_instance(&busy, InstanceLifecycle::InService, 0),
                fleet_instance(&idle, InstanceLifecycle::InService, 0),
                fleet_instance(&draining, InstanceLifecycle::Terminating, 0),
                fleet_instance(&pending, InstanceLifecycle::Pending, 0),
                fleet_instance(&unhosted, InstanceLifecycle::InService, 0),
            ],
        },
        FleetSupply {
            abandoned: 0,
            desired: 0,
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
            instances: Vec::new(),
        },
    ];
    world.snapshot.workers = vec![
        worker_state(&worker_on(&busy)?, "alpha", 2, false, NonZeroU32::new(8))?,
        worker_state(&worker_on(&idle)?, "alpha", 0, false, NonZeroU32::new(8))?,
        worker_state(&worker_on(&draining)?, "alpha", 1, true, NonZeroU32::new(8))?,
        worker_state(&worker_on(&other)?, "beta", 5, false, NonZeroU32::new(8))?,
    ];
    world.outlook = vec![
        FleetOutlook {
            workers: WorkerCounts {
                busy: 1,
                idle: 1,
                draining: 1,
                unconnected: 1,
                running: 3,
            },
            ..FleetOutlook::default()
        },
        FleetOutlook {
            workers: WorkerCounts {
                busy: 1,
                running: 5,
                ..WorkerCounts::default()
            },
            ..FleetOutlook::default()
        },
    ];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let cur = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    let alpha = &cur.fleets[0];
    assert_eq!(
        alpha.workers,
        gang_wire::WorkerCounts {
            busy: 1,
            idle: 1,
            draining: 1,
            unconnected: 1,
        }
    );
    assert_eq!(alpha.demand.running, 3);
    assert_eq!(
        alpha.instances,
        gang_wire::InstanceCounts {
            pending: 1,
            in_service: 3,
            terminating: 1,
            warm: 3,
        }
    );
    assert_eq!(alpha.desired, 4);
    let beta = &cur.fleets[1];
    assert_eq!(
        beta.workers,
        gang_wire::WorkerCounts {
            busy: 1,
            idle: 0,
            draining: 0,
            unconnected: 0,
        }
    );
    assert_eq!(beta.demand.running, 5);
    let listed: Vec<&str> = cur.instances.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(listed, [instance_name(&pending), instance_name(&unhosted)]);
    Ok(())
}

#[test]
fn worker_phase_and_phase_time_copy_or_reset_across_frames() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let instance = instance_id(1)?;
    let worker = worker_on(&instance)?;
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 1,
        instances: vec![fleet_instance(&instance, InstanceLifecycle::InService, 0)],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let first = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    let first_row = find_worker(&first, &worker).ok_or("worker missing")?;
    assert_eq!(first_row.phase, gang_wire::Phase::Busy);
    assert_eq!(first_row.phase_time, ts(1_000));
    assert_eq!(first_row.launch_time, ts(0));
    assert_eq!(first_row.instance, instance_name(&instance));

    world.snapshot.workers[0].running_actions = 3;
    let second = fill(&mut builder, &first, 2, &world, 2_000, ok(0))?;
    let second_row = find_worker(&second, &worker).ok_or("worker missing")?;
    assert_eq!(second_row.phase, gang_wire::Phase::Busy);
    assert_eq!(
        second_row.phase_time,
        ts(1_000),
        "same phase keeps phase_time"
    );
    assert_eq!(second_row.running, 3);

    world.snapshot.workers[0].running_actions = 0;
    let third = fill(&mut builder, &second, 3, &world, 3_000, ok(0))?;
    let third_row = find_worker(&third, &worker).ok_or("worker missing")?;
    assert_eq!(third_row.phase, gang_wire::Phase::Idle);
    assert_eq!(
        third_row.phase_time,
        ts(3_000),
        "a phase change resets phase_time"
    );

    world.snapshot.workers[0].draining = true;
    let fourth = fill(&mut builder, &third, 4, &world, 4_000, ok(0))?;
    let fourth_row = find_worker(&fourth, &worker).ok_or("worker missing")?;
    assert_eq!(fourth_row.phase, gang_wire::Phase::Draining);
    assert_eq!(fourth_row.phase_time, ts(4_000));
    Ok(())
}

#[test]
fn launch_time_of_a_worker_without_instance_is_carried_over() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let worker = worker_on(&instance_id(7)?)?;
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let first = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    let first_row = find_worker(&first, &worker).ok_or("worker missing")?;
    assert_eq!(first_row.launch_time, ts(1_000));

    let second = fill(&mut builder, &first, 2, &world, 2_000, ok(0))?;
    let second_row = find_worker(&second, &worker).ok_or("worker missing")?;
    assert_eq!(
        second_row.launch_time,
        ts(1_000),
        "the first sighting is kept"
    );
    if let Some(delta) = diff(&first, &second) {
        assert!(delta.workers.is_none(), "an unchanged worker moves no row");
    }
    Ok(())
}

#[test]
fn merge_pairs_workers_and_instances_with_interleaving_ids() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    let beta = policy("beta", 100, 4)?;
    world.properties = vec![
        properties(&alpha, Vec::new()),
        properties(&beta, Vec::new()),
    ];
    world.policies = vec![alpha, beta];
    let in_service = |name: &str, launched: u64| -> Result<FleetInstance, Box<dyn Error>> {
        Ok(fleet_instance(
            &InstanceId::try_new(name)?,
            InstanceLifecycle::InService,
            launched,
        ))
    };
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 3,
            instances: vec![
                in_service("local-1", 10)?,
                in_service("local-10", 20)?,
                in_service("local-2", 30)?,
            ],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            abandoned: 0,
            desired: 2,
            instances: vec![in_service("local-20", 40)?, in_service("local-3", 50)?],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    ];
    world.outlook = vec![FleetOutlook::default(); 2];
    let hosted = [
        ("local-1", "alpha"),
        ("local-10", "alpha"),
        ("local-20", "beta"),
        ("local-99", "alpha"),
    ];
    let mut workers = Vec::new();
    for (name, fleet) in hosted {
        let worker = worker_on(&InstanceId::try_new(name)?)?;
        workers.push(worker_state(&worker, fleet, 1, false, NonZeroU32::new(8))?);
    }
    world.snapshot.workers = workers;

    let frame = fill(
        &mut Builder::new(config(), gang_state::CacheBaseline::default()),
        &blank(),
        1,
        &world,
        1_000,
        ok(0),
    )?;
    assert!(
        frame
            .workers
            .windows(2)
            .all(|pair| pair[0].name < pair[1].name)
    );
    for (name, launched) in [
        ("local-1", ts(10)),
        ("local-10", ts(20)),
        ("local-20", ts(40)),
        ("local-99", ts(1_000)),
    ] {
        let worker = worker_on(&InstanceId::try_new(name)?)?;
        let row = find_worker(&frame, &worker).ok_or("worker missing")?;
        assert_eq!(row.instance, instance_name(&InstanceId::try_new(name)?));
        assert_eq!(row.launch_time, launched, "the host of {name}");
    }
    let unhosted: Vec<&str> = frame
        .instances
        .iter()
        .map(|instance| instance.name.as_str())
        .collect();
    assert_eq!(
        unhosted,
        [
            instance_name(&InstanceId::try_new("local-2")?),
            instance_name(&InstanceId::try_new("local-3")?)
        ]
    );
    Ok(())
}

/// A second worker on `instance`, sorting after the shared test worker.
fn renewed_worker(instance: &InstanceId) -> TestResult<WorkerId> {
    Ok(WorkerId::try_new(format!(
        "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
        instance.worker_name()
    ))?)
}

#[test]
fn every_worker_on_an_instance_gets_its_launch_time() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let instance = instance_id(1)?;
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 1,
        instances: vec![fleet_instance(&instance, InstanceLifecycle::InService, 10)],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let old = worker_on(&instance)?;
    let new = renewed_worker(&instance)?;
    world.snapshot.workers = vec![
        worker_state(&old, "alpha", 1, false, NonZeroU32::new(8))?,
        worker_state(&new, "alpha", 0, false, NonZeroU32::new(8))?,
    ];

    let frame = fill(
        &mut Builder::new(config(), gang_state::CacheBaseline::default()),
        &blank(),
        1,
        &world,
        1_000,
        ok(0),
    )?;
    for worker in [&old, &new] {
        let row = find_worker(&frame, worker).ok_or("worker missing")?;
        assert_eq!(row.launch_time, ts(10), "the launch time of {}", row.name);
    }
    assert!(frame.instances.is_empty(), "the instance is hosted");
    Ok(())
}

#[test]
fn slots_are_none_for_unbounded_workers() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let unbounded = worker_on(&instance_id(1)?)?;
    let bounded = worker_on(&instance_id(2)?)?;
    world.snapshot.workers = vec![
        worker_state(&unbounded, "alpha", 0, false, None)?,
        worker_state(&bounded, "alpha", 0, false, NonZeroU32::new(4))?,
    ];
    world.snapshot.workers[1].paused = true;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let cur = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    let unbounded_row = find_worker(&cur, &unbounded).ok_or("worker missing")?;
    assert_eq!(unbounded_row.slots, None);
    assert!(!unbounded_row.paused);
    let bounded_row = find_worker(&cur, &bounded).ok_or("worker missing")?;
    assert_eq!(bounded_row.slots, Some(4));
    assert!(bounded_row.paused);
    Ok(())
}

#[test]
fn instances_without_worker_are_listed_with_boot_grace() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let pending = instance_id(4)?;
    let young = instance_id(5)?;
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 2,
        instances: vec![
            fleet_instance(&pending, InstanceLifecycle::Pending, 0),
            fleet_instance(&young, InstanceLifecycle::InService, 1_000),
        ],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook {
        workers: WorkerCounts {
            unconnected: 1,
            ..WorkerCounts::default()
        },
        ..FleetOutlook::default()
    }];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let before = fill(&mut builder, &blank(), 1, &world, 1_099, ok(0))?;
    assert_eq!(past_grace(&before, &young), Some(false));
    assert_eq!(past_grace(&before, &pending), Some(false));
    assert_eq!(before.fleets[0].workers.unconnected, 1);

    world.past_boot_grace = vec![vec![false, true]];

    let after = fill(&mut builder, &before, 2, &world, 1_100, ok(0))?;
    assert_eq!(past_grace(&after, &young), Some(true));
    assert_eq!(past_grace(&after, &pending), Some(false));
    assert_eq!(after.instances.len(), 2);
    Ok(())
}

#[test]
fn old_unserved_instance_uses_the_pass_boot_grace_verdict() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let instance = instance_id(5)?;
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 1,
        instances: vec![fleet_instance(&instance, InstanceLifecycle::InService, 0)],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.past_boot_grace = vec![vec![false]];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let before = fill(&mut builder, &blank(), 1, &world, 3_600_000, ok(0))?;
    assert_eq!(past_grace(&before, &instance), Some(false));
    world.past_boot_grace[0][0] = true;
    let after = fill(&mut builder, &before, 2, &world, 3_600_000, ok(0))?;
    assert_eq!(past_grace(&after, &instance), Some(true));
    Ok(())
}

#[test]
fn unplaceable_plus_charged_equals_queue_total() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 0,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            abandoned: 0,
            desired: 0,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    ];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    for i in 0_u32..64 {
        let total = i % 9;
        let first = (i * 5) % (total + 1);
        let second = (i * 3) % (total - first + 1);
        world.snapshot.queued = (0..total)
            .map(|k| queued(u64::from(k) * 10, FleetSet::default()))
            .collect();
        world.outlook = vec![
            FleetOutlook {
                queued: first,
                ..FleetOutlook::default()
            },
            FleetOutlook {
                queued: second,
                ..FleetOutlook::default()
            },
        ];

        let cur = fill(&mut builder, &blank(), u64::from(i), &world, 1_000, ok(0))?;

        assert_eq!(cur.queue.total, total);
        assert_eq!(
            cur.fleets[0].demand.queued + cur.fleets[1].demand.queued + cur.queue.unplaceable,
            cur.queue.total,
            "sweep case {i}"
        );
        let oldest = if total == 0 { None } else { Some(ts(0)) };
        assert_eq!(cur.queue.oldest_queue_time, oldest);
    }
    Ok(())
}

#[test]
fn oldest_queue_time_is_the_earliest_queued_since_whatever_the_order() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    world.snapshot.queued = [300, 200, 100]
        .into_iter()
        .map(|since| queued(since, FleetSet::default()))
        .collect();
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let cur = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    assert_eq!(cur.queue.total, 3);
    assert_eq!(cur.queue.oldest_queue_time, Some(ts(100)));
    Ok(())
}

#[test]
fn actions_follow_sample_and_drop_vanished_workers() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    world.supplies = vec![FleetSupply::default(); 2];
    world.outlook = vec![FleetOutlook::default(), FleetOutlook::default()];
    let present = worker_on(&instance_id(1)?)?;
    let gone = worker_on(&instance_id(2)?)?;
    world.snapshot.workers = vec![worker_state(
        &present,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let without = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert!(without.actions.is_none());

    let mut candidates = FleetSet::default();
    candidates.insert(0);
    candidates.insert(1);
    world.actions = Some(ActionsSample {
        at: ts(4_000),
        view: ActionsView {
            running: vec![
                running_action(
                    action_info("op-b", "d/1", "//t:b", "Compile", -2),
                    &present,
                    ts(3_000),
                ),
                running_action(action_info("op-a", "d/2", "", "", 5), &gone, ts(3_100)),
            ],
            queued: vec![queued_action(
                action_info("op-q", "d/3", "//t:q", "Link", 1),
                ts(3_500),
                candidates,
            )],
            truncated_running: 3,
            truncated_queued: 4,
        },
    });

    let with = fill(&mut builder, &without, 2, &world, 5_000, ok(0))?;
    let actions = with.actions.as_ref().ok_or("actions missing")?;
    assert_eq!(actions.sample_time, ts(4_000));
    assert_eq!(actions.stale_after, Duration::from_secs(5));
    assert_eq!(
        actions.running.len(),
        1,
        "the vanished worker's row is dropped"
    );
    assert_eq!(actions.running[0].name, "actions/op-b");
    assert_eq!(actions.running[0].fleet, "fleets/alpha");
    assert_eq!(actions.running[0].worker, worker_name(&present));
    assert_eq!(actions.running[0].priority, -2);
    assert_eq!(actions.queued[0].priority, 1);
    assert_eq!(
        actions.queued[0].candidates,
        ["fleets/alpha", "fleets/beta"]
    );
    assert_eq!(
        actions.truncated,
        gang_wire::Truncated {
            running: 3,
            queued: 4,
        }
    );
    Ok(())
}

#[test]
fn skipped_pass_keeps_rows_and_counts_skips() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let instance = instance_id(1)?;
    let worker = worker_on(&instance)?;
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 2,
        instances: vec![
            fleet_instance(&instance, InstanceLifecycle::InService, 0),
            fleet_instance(&instance_id(2)?, InstanceLifecycle::Pending, 0),
        ],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    world.snapshot.completed = 9;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    let mut before_any_success = Builder::new(config(), gang_state::CacheBaseline::default());
    let early = fill(
        &mut before_any_success,
        &blank(),
        1,
        &world,
        500,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(
        early.pass,
        skipped_pass(SkipReason::SchedulerUnavailable, None, 1)
    );

    let ok_frame = fill(&mut builder, &blank(), 2, &world, 1_000, ok(1))?;
    assert_eq!(ok_frame.pass, Pass::ok(Duration::from_secs(1), 1, 0));

    world.snapshot.workers[0].running_actions = 9;
    world.snapshot.completed = 42;
    let skip = Outcome::Skipped(SkipReason::SchedulerUnavailable);
    let first_skip = fill(&mut builder, &ok_frame, 3, &world, 2_000, skip.clone())?;
    assert_eq!(first_skip.fleets, ok_frame.fleets);
    assert_eq!(first_skip.workers, ok_frame.workers);
    assert_eq!(first_skip.instances, ok_frame.instances);
    assert_eq!(first_skip.queue, ok_frame.queue);
    assert_eq!(first_skip.completed, ok_frame.completed);
    assert_eq!(first_skip.update_time, ts(2_000));
    assert_eq!(
        first_skip.pass,
        skipped_pass(SkipReason::SchedulerUnavailable, Some(ts(1_000)), 1)
    );

    let second_skip = fill(&mut builder, &first_skip, 4, &world, 3_000, skip.clone())?;
    assert_eq!(
        second_skip.pass,
        skipped_pass(SkipReason::SchedulerUnavailable, Some(ts(1_000)), 2)
    );

    let recovered = fill(&mut builder, &second_skip, 5, &world, 4_000, ok(2))?;
    assert_eq!(recovered.pass.state, PassState::Ok);
    assert_eq!(recovered.pass.applied, 2);
    assert_eq!(recovered.completed, 42);
    assert_eq!(find_worker(&recovered, &worker).map(|w| w.running), Some(9));

    let after_recovery = fill(&mut builder, &recovered, 6, &world, 5_000, skip)?;
    assert_eq!(
        after_recovery.pass,
        skipped_pass(SkipReason::SchedulerUnavailable, Some(ts(4_000)), 1)
    );
    Ok(())
}

#[test]
fn skipped_pass_refreshes_the_actions_part() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let worker = worker_on(&instance_id(1)?)?;
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    world.actions = Some(sample_at(4_000, vec![running_on(&worker, "op-a", 3_000)]));
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let ok_frame = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    world.actions = Some(sample_at(
        6_000,
        vec![
            running_on(&worker, "op-a", 3_000),
            running_on(&worker, "op-b", 5_000),
        ],
    ));
    let skipped = fill(
        &mut builder,
        &ok_frame,
        2,
        &world,
        2_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;

    assert_eq!(skipped.pass.state, PassState::Skipped);
    assert_eq!(skipped.fleets, ok_frame.fleets);
    assert_eq!(skipped.workers, ok_frame.workers);
    assert_eq!(skipped.instances, ok_frame.instances);
    let actions = skipped.actions.as_ref().ok_or("actions missing")?;
    assert_eq!(actions.sample_time, ts(6_000));
    let names: Vec<&str> = actions
        .running
        .iter()
        .map(|row| row.name.as_str())
        .collect();
    assert_eq!(names, ["actions/op-a", "actions/op-b"]);
    let worker_row = find_worker(&skipped, &worker).ok_or("worker missing")?;
    assert_eq!(worker_row.fleet, "fleets/alpha");
    for row in &actions.running {
        assert_eq!(row.fleet, worker_row.fleet);
    }
    Ok(())
}

#[test]
fn a_pass_without_a_sample_keeps_the_previous_actions() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 0,
        instances: Vec::new(),
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let worker = worker_on(&instance_id(1)?)?;
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());

    world.actions = None;
    let blank_prev = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert!(blank_prev.actions.is_none());

    world.actions = Some(sample_at(4_000, vec![running_on(&worker, "op-a", 3_000)]));
    let sampled = fill(&mut builder, &blank(), 2, &world, 2_000, ok(0))?;
    assert_eq!(
        sampled
            .actions
            .as_ref()
            .map(|actions| actions.running.len()),
        Some(1)
    );

    world.actions = None;
    let kept = fill(&mut builder, &sampled, 3, &world, 3_000, ok(0))?;
    assert_eq!(kept.actions, sampled.actions);
    Ok(())
}

/// The pass a skipped control pass reports, at the test builder's interval.
fn skipped_pass(reason: SkipReason, last_ok_time: Option<Timestamp>, skipped: u32) -> Pass {
    Pass::skipped(Duration::from_secs(1), reason, last_ok_time, skipped)
}

#[test]
fn scale_sweep_matches_a_naive_reference() -> TestResult {
    const INSTANCES: u64 = 2_000;
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let mut instances = Vec::new();
    for index in 0..INSTANCES {
        instances.push(fleet_instance(
            &instance_id(index)?,
            InstanceLifecycle::InService,
            index * 10,
        ));
    }
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 2_000,
        instances,
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let mut workers = Vec::new();
    for index in (0..INSTANCES).step_by(2) {
        let worker = worker_on(&instance_id(index)?)?;
        workers.push(worker_state(
            &worker,
            "alpha",
            1,
            false,
            NonZeroU32::new(8),
        )?);
    }
    for index in 0..INSTANCES / 2 {
        let ghost = InstanceId::try_new(format!("g-{index}"))?;
        workers.push(worker_state(
            &worker_on(&ghost)?,
            "alpha",
            0,
            false,
            NonZeroU32::new(8),
        )?);
    }
    workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    world.snapshot.workers = workers;

    let wall = 10_000;
    let frame = fill(
        &mut Builder::new(config(), gang_state::CacheBaseline::default()),
        &blank(),
        1,
        &world,
        wall,
        ok(0),
    )?;

    // Naive reference: every worker is scanned against every instance, and every instance
    // against every worker.
    let keys: Vec<Option<InstanceId>> = world
        .snapshot
        .workers
        .iter()
        .map(|worker| worker.id.instance())
        .collect();
    let supply = &world.supplies[0].instances;
    let mut expected_workers: Vec<(String, String, Timestamp)> = Vec::new();
    for (worker, key) in world.snapshot.workers.iter().zip(&keys) {
        let Some(key) = key else { continue };
        let launched = supply
            .iter()
            .find(|instance| instance.id == *key)
            .map_or(ts(wall), |instance| instance.launched_at);
        expected_workers.push((worker_name(&worker.id), instance_name(key), launched));
    }
    expected_workers.sort_by(|a, b| a.0.cmp(&b.0));
    let actual_workers: Vec<(String, String, Timestamp)> = frame
        .workers
        .iter()
        .map(|worker| {
            (
                worker.name.clone(),
                worker.instance.clone(),
                worker.launch_time,
            )
        })
        .collect();
    assert_eq!(actual_workers, expected_workers);

    let mut expected_unhosted: Vec<String> = supply
        .iter()
        .filter(|instance| !keys.iter().flatten().any(|key| *key == instance.id))
        .map(|instance| instance_name(&instance.id))
        .collect();
    expected_unhosted.sort();
    let actual_unhosted: Vec<String> = frame
        .instances
        .iter()
        .map(|instance| instance.name.clone())
        .collect();
    assert_eq!(actual_unhosted, expected_unhosted);
    assert_eq!(actual_unhosted.len(), 1_000);
    Ok(())
}

#[test]
fn a_worker_of_another_fleet_does_not_host_the_instance() -> TestResult {
    let mut world = World::default();
    let home = policy("fleet-a", 30_000, 4)?;
    let foreign = policy("fleet-b", 30_000, 4)?;
    let home_name = FleetName::try_new("fleet-a")?;
    let foreign_name = FleetName::try_new("fleet-b")?;
    world.properties = vec![
        properties(&home, Vec::new()),
        properties(&foreign, Vec::new()),
    ];
    world.policies = vec![home, foreign];
    let instance = instance_id(1)?;
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 1,
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
            instances: vec![fleet_instance(&instance, InstanceLifecycle::InService, 0)],
        },
        FleetSupply::default(),
    ];
    world.snapshot.workers = vec![worker_state(
        &worker_on(&instance)?,
        "fleet-b",
        0,
        false,
        NonZeroU32::new(4),
    )?];

    let mut passes = world.passes();
    let mut reconciler = Reconciler::new(&world.policies);
    let mut plan = Plan::new();
    let clock = TestClock::new(ts(100_000));
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut world.snapshot.clone());
    reconciler.reconcile(&mut passes, &hosted, PassTime::read(&clock), &mut plan);
    clock.advance(Duration::from_secs(30));
    reconciler.reconcile(&mut passes, &hosted, PassTime::read(&clock), &mut plan);
    assert_eq!(
        plan.steps,
        [PlanStep {
            fleet: 0,
            action: Action::Terminate {
                instance: instance.clone(),
                cause: TerminationCause::Orphan,
            },
        }]
    );

    world.passes = passes;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let cur = fill(&mut builder, &blank(), 1, &world, 130_000, ok(0))?;

    assert_eq!(cur.instances.len(), 1);
    assert_eq!(cur.instances[0].name, instance_name(&instance));
    assert!(cur.instances[0].past_boot_grace);

    let home_row = cur
        .fleets
        .iter()
        .find(|row| row.name == name::of::<name::Fleets>(&home_name))
        .ok_or("fleet-a row missing")?;
    assert_eq!(home_row.workers.unconnected, 1);

    let worker = find_worker(&cur, &worker_on(&instance)?).ok_or("worker row missing")?;
    assert_eq!(worker.fleet, name::of::<name::Fleets>(&foreign_name));
    Ok(())
}

/// A fleet set holding the fleet indices `indices`.
fn fleet_set(indices: &[usize]) -> FleetSet {
    let mut set = FleetSet::default();
    for &index in indices {
        set.insert(index);
    }
    set
}

/// Two fleets, three workers (one draining, one on an instance no fleet supplies), two
/// unconnected instances, two queued demands, and two running and two queued actions.
fn larger_world() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    let beta = policy("beta", 100, 4)?;
    world.properties = vec![
        properties(&alpha, Vec::new()),
        properties(&beta, Vec::new()),
    ];
    world.policies = vec![alpha, beta];
    world.supplies = vec![
        FleetSupply {
            abandoned: 0,
            desired: 2,
            instances: vec![
                fleet_instance(&instance_id(1)?, InstanceLifecycle::InService, 10),
                fleet_instance(&instance_id(2)?, InstanceLifecycle::InService, 20),
            ],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            abandoned: 0,
            desired: 1,
            instances: vec![
                fleet_instance(&instance_id(3)?, InstanceLifecycle::InService, 30),
                fleet_instance(&instance_id(4)?, InstanceLifecycle::InService, 40),
            ],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    ];
    world.outlook = vec![
        FleetOutlook {
            queued: 1,
            ..FleetOutlook::default()
        },
        FleetOutlook::default(),
    ];
    let w1 = worker_on(&instance_id(1)?)?;
    let w3 = worker_on(&instance_id(3)?)?;
    let w9 = worker_on(&instance_id(9)?)?;
    world.snapshot.workers = vec![
        worker_state(&w1, "alpha", 1, false, NonZeroU32::new(8))?,
        worker_state(&w3, "beta", 0, true, NonZeroU32::new(8))?,
        worker_state(&w9, "alpha", 1, false, NonZeroU32::new(8))?,
    ];
    world.snapshot.queued = vec![
        queued(100, fleet_set(&[0, 1])),
        queued(200, fleet_set(&[1])),
    ];
    world.actions = Some(ActionsSample {
        at: ts(4_000),
        view: ActionsView {
            running: vec![
                running_action(
                    action_info("op-b", "d/1", "//t:b", "Compile", -2),
                    &w1,
                    ts(3_000),
                ),
                running_action(action_info("op-a", "d/2", "", "", 5), &w3, ts(3_100)),
            ],
            queued: vec![
                queued_action(
                    action_info("op-q", "d/3", "//t:q", "Link", 1),
                    ts(3_500),
                    fleet_set(&[0, 1]),
                ),
                queued_action(
                    action_info("op-r", "d/4", "//t:r", "Link", 0),
                    ts(3_600),
                    fleet_set(&[1]),
                ),
            ],
            truncated_running: 0,
            truncated_queued: 0,
        },
    });
    Ok(world)
}

/// One fleet, one worker on a supplied instance, one queued demand, and one running and one
/// queued action: fewer rows and candidates than `larger_world` in every section.
fn smaller_world() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    let alpha = policy("alpha", 500, 2)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        abandoned: 0,
        desired: 1,
        instances: vec![fleet_instance(
            &instance_id(1)?,
            InstanceLifecycle::InService,
            50,
        )],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }];
    world.outlook = vec![FleetOutlook::default()];
    let w1 = worker_on(&instance_id(1)?)?;
    world.snapshot.workers = vec![worker_state(&w1, "alpha", 0, false, NonZeroU32::new(8))?];
    world.snapshot.queued = vec![queued(300, fleet_set(&[0]))];
    world.actions = Some(ActionsSample {
        at: ts(4_500),
        view: ActionsView {
            running: vec![running_action(
                action_info("op-c", "d/5", "//t:c", "Compile", 0),
                &w1,
                ts(4_000),
            )],
            queued: vec![queued_action(
                action_info("op-s", "d/6", "//t:s", "Link", 0),
                ts(4_200),
                fleet_set(&[0]),
            )],
            truncated_running: 0,
            truncated_queued: 0,
        },
    });
    Ok(world)
}

#[test]
fn refilling_a_non_empty_frame_equals_filling_a_blank_one() -> TestResult {
    let larger = larger_world()?;
    let smaller = smaller_world()?;
    let larger_frame = fill(
        &mut Builder::new(config(), gang_state::CacheBaseline::default()),
        &blank(),
        9,
        &larger,
        7_000,
        ok(0),
    )?;
    assert_eq!(larger_frame.fleets.len(), 2);
    assert_eq!(larger_frame.workers.len(), 3);
    assert_eq!(larger_frame.instances.len(), 2);
    assert_eq!(larger_frame.queue.total, 2);
    assert_eq!(
        larger_frame.actions.as_ref().map(|a| a.running.len()),
        Some(2)
    );
    assert_eq!(
        larger_frame.actions.as_ref().map(|a| a.queued.len()),
        Some(2)
    );
    let smaller_frame = fill(
        &mut Builder::new(config(), gang_state::CacheBaseline::default()),
        &blank(),
        9,
        &smaller,
        7_000,
        ok(0),
    )?;
    assert_eq!(smaller_frame.fleets.len(), 1);
    assert_eq!(smaller_frame.workers.len(), 1);
    assert_eq!(smaller_frame.instances.len(), 0);

    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    fill_into(&mut builder, &mut cur, &blank(), 9, &larger, 7_000, ok(0))?;
    assert_eq!(cur, larger_frame, "the larger world fills a blank frame");
    fill_into(&mut builder, &mut cur, &blank(), 9, &smaller, 7_000, ok(0))?;
    assert_eq!(
        cur, smaller_frame,
        "the smaller world refilled into the larger frame equals a blank fill"
    );

    let mut cur = blank();
    fill_into(&mut builder, &mut cur, &blank(), 9, &smaller, 7_000, ok(0))?;
    fill_into(&mut builder, &mut cur, &blank(), 9, &larger, 7_000, ok(0))?;
    assert_eq!(
        cur, larger_frame,
        "the larger world refilled into the smaller frame equals a blank fill"
    );
    Ok(())
}

#[test]
fn running_actions_are_sorted_by_encoded_name_and_round_trip() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply::default()];
    world.outlook = vec![FleetOutlook::default()];
    let worker = worker_on(&instance_id(1)?)?;
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        2,
        false,
        NonZeroU32::new(8),
    )?];
    world.actions = Some(sample_at(1_000, vec![running_on(&worker, "a-1", 500)]));
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let prev = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;

    world.actions = Some(sample_at(
        2_000,
        vec![
            running_on(&worker, "a-1", 500),
            running_on(&worker, "a/1", 600),
        ],
    ));
    let cur = fill(&mut builder, &prev, 2, &world, 2_000, ok(0))?;
    let actions = cur.actions.as_ref().ok_or("actions missing")?;
    let names: Vec<_> = actions
        .running
        .iter()
        .map(|row| row.name.as_str())
        .collect();
    assert_eq!(names, ["actions/a%2F1", "actions/a-1"]);
    let delta = diff(&prev, &cur).ok_or("diff missing")?;
    let mut rebuilt = prev;
    gang_state::apply(&mut rebuilt, &delta)?;
    assert_eq!(rebuilt, cur);
    Ok(())
}

#[test]
fn events_keep_only_the_newest_configured_window() -> TestResult {
    let events: Vec<_> = (1..=3)
        .map(|index| gang_wire::Event {
            name: format!("events/{index}"),
            create_time: ts(index),
            fleet: "fleets/alpha".to_owned(),
            cause: None,
            change: gang_wire::EventChange::ScaledOut { from: 1, to: 2 },
        })
        .collect();
    let mut settings = config();
    settings.events_limit = 2;
    let mut builder = Builder::new(settings, gang_state::CacheBaseline::default());
    let catalog = FleetCatalog::try_new(Vec::new())?;
    let builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let mut prev = blank();
    let initial = Inputs {
        wall: ts(0),
        pass: PassStatus::Skipped(SkipReason::SchedulerUnavailable),
        fleets: &[],
        catalog: &catalog,
        actions: None,
        events: &[],
        builds: &builds,
        traffic: Traffic::default(),
        measured: None,
        principals_epoch: 0,
    };
    builder.fill(&mut prev, &blank(), 0, &initial);
    let mut cur = blank();
    let inputs = Inputs {
        wall: ts(10),
        pass: PassStatus::Skipped(SkipReason::SchedulerUnavailable),
        fleets: &[],
        catalog: &catalog,
        actions: None,
        events: &events,
        builds: &builds,
        traffic: Traffic::default(),
        measured: None,
        principals_epoch: 0,
    };
    builder.fill(&mut cur, &prev, 1, &inputs);
    assert_eq!(cur.events, events[1..]);
    let delta = diff(&prev, &cur).ok_or("diff missing")?;
    let mut rebuilt = prev;
    gang_state::apply(&mut rebuilt, &delta)?;
    assert_eq!(rebuilt, cur);
    Ok(())
}

/// A fleet outlook with the given number of running workers.
fn running_outlook(running: u32) -> FleetOutlook {
    FleetOutlook {
        workers: WorkerCounts {
            running,
            ..WorkerCounts::default()
        },
        ..FleetOutlook::default()
    }
}

#[test]
fn recent_samples_follow_completed_passes_and_restarts() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut prev = builder.initial(ts(0), 7);
    assert!(prev.recent.samples.is_empty());
    for (index, completed) in [0, 5, 12, 12, 20].into_iter().enumerate() {
        world.snapshot.completed = completed;
        world.snapshot.queued = (0..index)
            .map(|n| queued(n as u64, FleetSet::default()))
            .collect();
        world.outlook = vec![
            running_outlook(u32::try_from(index).expect("test fleets are few")),
            running_outlook(2),
        ];
        let cur = fill(
            &mut builder,
            &prev,
            index as u64 + 1,
            &world,
            index as u64 * 4_000,
            ok(0),
        )?;
        let delta = diff(&prev, &cur).ok_or("missing delta")?;
        let mut applied = prev;
        gang_state::apply(&mut applied, &delta)?;
        assert_eq!(applied, cur);
        prev = cur;
    }
    assert_eq!(
        prev.recent.samples,
        vec![
            gang_wire::Sample {
                start: ts(0),
                completed: 12,
                running: 4,
                queued: 2,
                passes: 3,
                skipped: 0,
                lookups: 0,
                hits: 0,
                ..gang_wire::Sample::default()
            },
            gang_wire::Sample {
                start: ts(10_000),
                completed: 8,
                running: 6,
                queued: 4,
                passes: 2,
                skipped: 0,
                lookups: 0,
                hits: 0,
                ..gang_wire::Sample::default()
            },
        ]
    );

    let skipped = fill(
        &mut builder,
        &prev,
        6,
        &world,
        18_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    let sample = skipped
        .recent
        .samples
        .last()
        .ok_or("missing skipped sample")?;
    assert_eq!(
        (sample.passes, sample.skipped, sample.running, sample.queued),
        (2, 1, 6, 4)
    );
    world.snapshot.completed = 3;
    let restarted = fill(&mut builder, &skipped, 7, &world, 24_000, ok(0))?;
    let sample = restarted
        .recent
        .samples
        .last()
        .ok_or("missing restarted sample")?;
    assert_eq!((sample.completed, sample.passes, sample.skipped), (3, 1, 0));
    for (before, after) in [(&prev, &skipped), (&skipped, &restarted)] {
        let delta = diff(before, after).ok_or("missing delta")?;
        let mut applied = before.clone();
        gang_state::apply(&mut applied, &delta)?;
        assert_eq!(&applied, after);
    }
    Ok(())
}

#[test]
fn worker_phase_history_keeps_the_span_overlapping_the_window() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let instance = instance_id(1)?;
    let worker = worker_on(&instance)?;
    world.snapshot.workers = vec![worker_state(&worker, "alpha", 0, false, None)?];
    let mut settings = config();
    settings.history_window = Duration::from_secs(10);
    let mut builder = Builder::new(settings, gang_state::CacheBaseline::default());
    let mut prev = builder.initial(ts(0), 7);
    for (seq, wall, running) in [(1, 1_000, 0), (2, 4_000, 1), (3, 7_000, 0)] {
        world.snapshot.workers[0].running_actions = running;
        let cur = fill(&mut builder, &prev, seq, &world, wall, ok(0))?;
        let delta = diff(&prev, &cur).ok_or("missing delta")?;
        let mut applied = prev;
        gang_state::apply(&mut applied, &delta)?;
        assert_eq!(applied, cur);
        prev = cur;
    }
    let spans = &find_worker(&prev, &worker).ok_or("worker missing")?.phases;
    assert_eq!(
        spans,
        &[
            gang_wire::PhaseSpan {
                phase: gang_wire::Phase::Idle,
                start: ts(1_000),
            },
            gang_wire::PhaseSpan {
                phase: gang_wire::Phase::Busy,
                start: ts(4_000),
            },
            gang_wire::PhaseSpan {
                phase: gang_wire::Phase::Idle,
                start: ts(7_000),
            },
        ]
    );
    let trimmed = fill(&mut builder, &prev, 4, &world, 14_000, ok(0))?;
    let spans = &find_worker(&trimmed, &worker)
        .ok_or("worker missing")?
        .phases;
    assert_eq!(
        spans,
        &[
            gang_wire::PhaseSpan {
                phase: gang_wire::Phase::Busy,
                start: ts(4_000),
            },
            gang_wire::PhaseSpan {
                phase: gang_wire::Phase::Idle,
                start: ts(7_000),
            },
        ]
    );
    let delta = diff(&prev, &trimmed).ok_or("missing delta")?;
    gang_state::apply(&mut prev, &delta)?;
    assert_eq!(prev, trimmed);
    Ok(())
}

#[test]
fn cache_samples_follow_counter_deltas_skips_and_restarts() -> TestResult {
    let mut world = World::default();
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let initial = builder.initial(ts(0), 0);
    world.traffic.lookups = 10;
    world.traffic.hits = 7;
    let first = fill(&mut builder, &initial, 1, &world, 1_000, ok(0))?;
    let sample = first.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.hits), (10, 7));
    world.traffic.lookups = 30;
    world.traffic.hits = 20;
    let skipped = fill(
        &mut builder,
        &first,
        2,
        &world,
        11_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    let sample = skipped.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.hits), (0, 0));
    world.traffic.lookups = 20;
    world.traffic.hits = 14;
    let resumed = fill(&mut builder, &skipped, 3, &world, 21_000, ok(0))?;
    let sample = resumed.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.hits), (10, 7));
    world.traffic.lookups = 3;
    world.traffic.hits = 2;
    let restarted = fill(&mut builder, &resumed, 4, &world, 31_000, ok(0))?;
    let sample = restarted.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.hits), (3, 2));
    Ok(())
}

#[test]
fn recent_builds_expire_on_skipped_pass_and_round_trip() -> TestResult {
    let mut settings = config();
    settings.history_window = Duration::from_secs(10);
    let mut builder = Builder::new(settings, gang_state::CacheBaseline::default());
    let mut world = World::default();
    let mut builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let old = build_key("old")?;
    let new = build_key("new")?;
    builds.lose(ts(10_000), &old, 1);
    builds.lose(ts(20_000), &new, 1);
    world.builds = Some(builds);
    let mut applied_initial = builder.initial(ts(0), 7);
    let first = fill(&mut builder, &applied_initial, 1, &world, 20_000, ok(0))?;
    let mut expected = vec![
        format!("builds/{}", old.id()),
        format!("builds/{}", new.id()),
    ];
    expected.sort();
    assert_eq!(
        first
            .builds
            .iter()
            .map(|build| build.name.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let added = diff(&applied_initial, &first).ok_or("missing initial delta")?;
    assert_eq!(
        added
            .builds
            .as_ref()
            .ok_or("missing builds delta")?
            .upsert
            .len(),
        2
    );
    gang_state::apply(&mut applied_initial, &added)?;
    assert_eq!(applied_initial, first);
    let next = fill(
        &mut builder,
        &first,
        2,
        &world,
        20_001,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(next.builds.len(), 1);
    assert_eq!(next.builds[0].name, format!("builds/{}", new.id()));
    let delta = diff(&first, &next).ok_or("missing delta")?;
    assert_eq!(
        delta.builds.as_ref().ok_or("missing builds delta")?.remove,
        vec![format!("builds/{}", old.id())]
    );
    let mut applied = first;
    gang_state::apply(&mut applied, &delta)?;
    assert_eq!(applied, next);
    Ok(())
}

#[test]
fn action_build_names_are_written_and_cleared_on_reuse() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let worker = worker_on(&instance_id(1)?)?;
    world.snapshot.workers = vec![worker_state(
        &worker,
        "alpha",
        1,
        false,
        NonZeroU32::new(8),
    )?];
    let key = build_key("action")?;
    let mut info = action_info("running", "d/1", "", "", 0);
    info.build = Some(key.id());
    let mut queued_info = action_info("queued", "d/2", "", "", 0);
    queued_info.build = Some(key.id());
    world.actions = Some(ActionsSample {
        at: ts(1_000),
        view: ActionsView {
            running: vec![running_action(info, &worker, ts(900))],
            queued: vec![queued_action(queued_info, ts(900), FleetSet::default())],
            truncated_running: 0,
            truncated_queued: 0,
        },
    });
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut frame = blank();
    fill_into(&mut builder, &mut frame, &blank(), 1, &world, 1_000, ok(0))?;
    let actions = frame.actions.as_ref().ok_or("missing actions")?;
    let name = format!("builds/{}", key.id());
    assert_eq!(actions.running[0].build, name);
    assert_eq!(actions.queued[0].build, name);
    let sample = world.actions.as_mut().ok_or("missing sample")?;
    sample.view.running[0].info.build = None;
    sample.view.queued[0].info.build = None;
    let prev = frame.clone();
    fill_into(&mut builder, &mut frame, &prev, 2, &world, 2_000, ok(0))?;
    let actions = frame.actions.as_ref().ok_or("missing actions")?;
    assert!(actions.running[0].build.is_empty());
    assert!(actions.queued[0].build.is_empty());
    Ok(())
}

fn cache_counts(sample: &gang_wire::Sample) -> (u32, u32, u64, u64, u64) {
    (
        sample.lookups,
        sample.hits,
        sample.uploaded_bytes,
        sample.downloaded_bytes,
        sample.evicted_bytes,
    )
}

// Catches mixing counters from different source generations after a partial decrease.
#[test]
fn traffic_restart_resets_the_entire_counter_tuple() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World {
        traffic: Traffic {
            lookups: 100,
            hits: 90,
            uploaded: 200,
            downloaded: 300,
        },
        ..World::default()
    };
    let initial = builder.initial(ts(0), 7);
    let first = fill(&mut builder, &initial, 1, &world, 1_000, ok(0))?;
    world.traffic = Traffic {
        lookups: 120,
        hits: 50,
        uploaded: 250,
        downloaded: 350,
    };
    let next = fill(&mut builder, &first, 2, &world, 11_000, ok(0))?;
    let sample = next.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(cache_counts(sample), (120, 50, 250, 350, 0));
    assert!(sample.hits <= sample.lookups);
    Ok(())
}

// Catches counting pre-leadership traffic or an unknown eviction baseline as new usage.
#[test]
fn leadership_baseline_ignores_unknown_evictions_until_first_success() -> TestResult {
    let initial_traffic = Traffic {
        lookups: 10,
        hits: 8,
        uploaded: 100,
        downloaded: 200,
    };
    let mut builder = Builder::new(
        config(),
        gang_state::CacheBaseline {
            traffic: initial_traffic,
            evicted: None,
        },
    );
    let mut world = World {
        traffic: initial_traffic,
        snapshot: SchedulerSnapshot {
            stores: vec![EngineStore {
                store: StoreKind::Content,
                used: 1,
                budget: 10,
                evicted: 70,
            }],
            ..SchedulerSnapshot::default()
        },
        ..World::default()
    };
    let initial = builder.initial(ts(0), 7);
    let skipped = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    world.traffic.uploaded += 10;
    let first = fill(&mut builder, &skipped, 2, &world, 11_000, ok(0))?;
    assert_eq!(
        cache_counts(first.recent.samples.last().ok_or("missing sample")?),
        (0, 0, 10, 0, 0)
    );
    world.snapshot.stores[0].evicted = 75;
    let second = fill(&mut builder, &first, 3, &world, 21_000, ok(0))?;
    assert_eq!(
        cache_counts(second.recent.samples.last().ok_or("missing sample")?),
        (0, 0, 0, 0, 5)
    );
    Ok(())
}

// Catches counting pre-leadership evictions or losing increments before the first pass.
#[test]
fn known_eviction_baseline_counts_only_new_evictions() -> TestResult {
    let mut builder = Builder::new(
        config(),
        gang_state::CacheBaseline {
            traffic: Traffic::default(),
            evicted: Some(60),
        },
    );
    let world = World {
        snapshot: SchedulerSnapshot {
            stores: vec![EngineStore {
                store: StoreKind::Content,
                used: 1,
                budget: 10,
                evicted: 70,
            }],
            ..SchedulerSnapshot::default()
        },
        ..World::default()
    };
    let initial = builder.initial(ts(0), 7);
    let first = fill(&mut builder, &initial, 1, &world, 1_000, ok(0))?;
    assert_eq!(
        cache_counts(first.recent.samples.last().ok_or("missing sample")?),
        (0, 0, 0, 0, 10)
    );
    Ok(())
}

// Catches cumulative-instead-of-delta accounting and stale stores or measurements.
#[test]
fn cache_traffic_stores_and_measurements_follow_successful_passes() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World {
        traffic: Traffic {
            lookups: 10,
            hits: 7,
            uploaded: 100,
            downloaded: 200,
        },
        ..World::default()
    };
    world.snapshot.stores = vec![
        EngineStore {
            store: StoreKind::Content,
            used: 42,
            budget: 100,
            evicted: 20,
        },
        EngineStore {
            store: StoreKind::Action,
            used: 8,
            budget: 30,
            evicted: 5,
        },
    ];
    world.measured = Some(MeasuredUsage {
        bytes: 500,
        objects: 9,
        measured: ts(123),
    });
    let initial = builder.initial(ts(0), 7);
    let first = fill(&mut builder, &initial, 1, &world, 1_000, ok(0))?;
    let sample = first.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(cache_counts(sample), (10, 7, 100, 200, 25));
    assert_eq!(
        first.cache.stores,
        vec![
            gang_wire::StoreUsage {
                store: gang_wire::CacheStore::Content,
                used_bytes: 42,
                budget_bytes: 100
            },
            gang_wire::StoreUsage {
                store: gang_wire::CacheStore::Action,
                used_bytes: 8,
                budget_bytes: 30
            },
        ]
    );
    assert_eq!(
        first.cache.bucket,
        Some(gang_wire::BucketUsage {
            bytes: 500,
            objects: 9,
            measure_time: ts(123)
        })
    );

    world.traffic = Traffic {
        lookups: 14,
        hits: 9,
        uploaded: 120,
        downloaded: 260,
    };
    world.snapshot.stores[0].evicted = 24;
    world.snapshot.stores[1].evicted = 8;
    world.snapshot.stores[0].used = 51;
    world.measured = Some(MeasuredUsage {
        bytes: 700,
        objects: 11,
        measured: ts(456),
    });
    let second = fill(&mut builder, &first, 2, &world, 11_000, ok(0))?;
    let sample = second.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(cache_counts(sample), (4, 2, 20, 60, 7));
    assert_eq!(second.cache.stores[0].used_bytes, 51);
    assert_eq!(
        second
            .cache
            .bucket
            .as_ref()
            .map(|b| (b.bytes, b.objects, b.measure_time)),
        Some((700, 11, ts(456)))
    );
    Ok(())
}

// Catches sampling on a skipped pass, losing the last measurement, and reset mishandling.
#[test]
fn cache_skips_preserve_stores_and_latest_measurement() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World {
        traffic: Traffic {
            lookups: 14,
            hits: 9,
            uploaded: 120,
            downloaded: 260,
        },
        measured: Some(MeasuredUsage {
            bytes: 700,
            objects: 11,
            measured: ts(456),
        }),
        snapshot: SchedulerSnapshot {
            stores: vec![
                EngineStore {
                    store: StoreKind::Content,
                    used: 51,
                    budget: 100,
                    evicted: 24,
                },
                EngineStore {
                    store: StoreKind::Action,
                    used: 8,
                    budget: 30,
                    evicted: 8,
                },
            ],
            ..SchedulerSnapshot::default()
        },
        ..World::default()
    };
    let initial = builder.initial(ts(0), 7);
    let second = fill(&mut builder, &initial, 2, &world, 11_000, ok(0))?;

    world.traffic = Traffic {
        lookups: 16,
        hits: 10,
        uploaded: 140,
        downloaded: 280,
    };
    world.snapshot.stores[0].evicted = 99;
    let skipped = fill(
        &mut builder,
        &second,
        3,
        &world,
        21_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(skipped.cache.stores, second.cache.stores);
    assert_eq!(skipped.cache.bucket, second.cache.bucket);
    let sample = skipped.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(
        (
            sample.lookups,
            sample.hits,
            sample.uploaded_bytes,
            sample.downloaded_bytes,
            sample.evicted_bytes
        ),
        (0, 0, 0, 0, 0)
    );
    world.traffic = Traffic {
        lookups: 2,
        hits: 1,
        uploaded: 3,
        downloaded: 4,
    };
    world.snapshot.stores[0].evicted = 1;
    world.snapshot.stores[1].evicted = 1;
    let restarted = fill(&mut builder, &skipped, 4, &world, 31_000, ok(0))?;
    let sample = restarted.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(
        (
            sample.lookups,
            sample.hits,
            sample.uploaded_bytes,
            sample.downloaded_bytes,
            sample.evicted_bytes
        ),
        (2, 1, 3, 4, 2)
    );
    assert_eq!(restarted.cache.bucket, second.cache.bucket);
    Ok(())
}

// Catches sampling skipped passes, coupling traffic and eviction resets,
// and replacing rather than accumulating increments within a step.
#[test]
fn cache_counters_bridge_skips_and_reset_sources_separately() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World {
        traffic: Traffic {
            lookups: 10,
            hits: 8,
            uploaded: 100,
            downloaded: 200,
        },
        ..World::default()
    };
    world.snapshot.stores = vec![EngineStore {
        store: StoreKind::Content,
        used: 1,
        budget: 2,
        evicted: 30,
    }];
    let initial = builder.initial(ts(0), 7);
    let first = fill(&mut builder, &initial, 1, &world, 1_000, ok(0))?;
    world.traffic = Traffic {
        lookups: 12,
        hits: 9,
        uploaded: 120,
        downloaded: 220,
    };
    world.snapshot.stores[0].evicted = 35;
    let skipped = fill(
        &mut builder,
        &first,
        2,
        &world,
        2_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    world.traffic = Traffic {
        lookups: 15,
        hits: 2,
        uploaded: 140,
        downloaded: 4,
    };
    world.snapshot.stores[0].evicted = 40;
    let next = fill(&mut builder, &skipped, 3, &world, 3_000, ok(0))?;
    let sample = next.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(
        (
            sample.lookups,
            sample.hits,
            sample.uploaded_bytes,
            sample.downloaded_bytes,
            sample.evicted_bytes
        ),
        (25, 10, 240, 204, 40)
    );
    world.traffic = Traffic {
        lookups: 18,
        hits: 3,
        uploaded: 150,
        downloaded: 9,
    };
    world.snapshot.stores[0].evicted = 2;
    let after_eviction_reset = fill(&mut builder, &next, 4, &world, 11_000, ok(0))?;
    let sample = after_eviction_reset
        .recent
        .samples
        .last()
        .ok_or("missing sample")?;
    assert_eq!(cache_counts(sample), (3, 1, 10, 5, 2));
    Ok(())
}
