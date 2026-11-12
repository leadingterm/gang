//! A steady-state builder pass must not allocate once the builder's buffers have grown.

use std::error::Error;
use std::num::NonZeroU32;

use gang_config::FleetPolicy;
use gang_reconciler::{FleetOutlook, FleetPass, HostedSnapshot, WorkerCounts};
use gang_spi::{
    ActionInfo, ActionsView, FleetInstance, FleetSupply, QueuedActionView, RunningActionView,
    SchedulerSnapshot,
};
use gang_state::{ActionsSample, Builder, Inputs, PassStatus};
use gang_testkit::{TestResult, worker_on};
use gang_types::{Bytes, FleetCatalog, FleetProperties, FleetSet, InstanceLifecycle, Resources};
use gang_wire::{Cause, Event, EventChange, name};

use crate::support::{
    blank, config, fleet_instance, instance_id, policy, properties, queued, ts, worker_state,
};

/// Passes run before measurement so the builder's buffers reach their steady-state size.
const WARM_UP_PASSES: usize = 4;

/// Fleet names in declaration order. The first fleet carries a non-reserved worker property.
const FLEETS: [&str; 3] = ["alpha", "beta", "gamma"];

/// Supply instances, spread across the fleets in turn.
const INSTANCES: u64 = 40;

/// The first this many instances host a worker; the rest have none.
const HOSTED: u64 = 24;

/// Executing actions in the sample, each on one of the first workers.
const RUNNING: u64 = 12;

/// Queued actions in the sample, each accepted by the first two fleets.
const QUEUED: u64 = 12;

/// Wall-clock time of the measured pass, in milliseconds since the epoch.
const WALL_MILLIS: u64 = 6_000;

/// Everything one control pass hands to the builder that the fixture owns, so that the inputs can
/// borrow it.
struct World {
    passes: Vec<FleetPass>,
    catalog: FleetCatalog,
    hosted: HostedSnapshot,
    actions: Option<ActionsSample>,
    events: Vec<Event>,
}

fn ok(applied: u32, hosted: &HostedSnapshot) -> PassStatus<'_> {
    PassStatus::Ok {
        applied,
        failed: 0,
        hosted,
    }
}

/// The fleet index an instance belongs to; instances rotate across the fleets.
fn fleet_of(index: u64) -> Result<usize, Box<dyn Error>> {
    Ok(usize::try_from(index)? % FLEETS.len())
}

/// A set of the first two fleets, the candidates of every queued action.
fn two_fleets() -> Result<FleetSet, Box<dyn Error>> {
    Ok(FleetSet::try_from_indices([0, 1])?)
}

/// The reconciler's outlook of one fleet: two needed instances and one queued demand.
fn outlook() -> FleetOutlook {
    let mut demand = FleetOutlook::default().demand;
    demand.add_queued(Resources {
        vcpus: 2,
        memory: Bytes::new(1_024),
        disk: Bytes::new(0),
    });
    FleetOutlook {
        needed: 2,
        demand,
        queued: 4,
        candidates: 2,
        workers: WorkerCounts::default(),
    }
}

/// The inputs of one pass over `world`. Only borrows, so building it allocates nothing.
fn inputs(world: &World) -> Inputs<'_> {
    Inputs {
        wall: ts(WALL_MILLIS),
        pass: ok(0, &world.hosted),
        fleets: &world.passes,
        catalog: &world.catalog,
        actions: world.actions.as_ref(),
        events: &world.events,
        principals_epoch: 7,
    }
}

/// The fleet policies and their properties, in declaration order. The first fleet carries one
/// custom option.
fn fleet_policies() -> Result<(Vec<FleetPolicy>, Vec<FleetProperties>), Box<dyn Error>> {
    let policies: Vec<FleetPolicy> = FLEETS
        .iter()
        .map(|&name| policy(name, 60_000, 16))
        .collect::<Result<_, _>>()?;
    let fleet_properties: Vec<FleetProperties> = policies
        .iter()
        .enumerate()
        .map(|(index, policy)| {
            let options = if index == 0 {
                vec![("custom.option", "v".to_owned())]
            } else {
                Vec::new()
            };
            properties(policy, options)
        })
        .collect();
    Ok((policies, fleet_properties))
}

/// The supply of each fleet: 40 instances spread across the fleets, lifecycles in rotation.
fn supplies() -> Result<Vec<FleetSupply>, Box<dyn Error>> {
    let mut fleets: Vec<Vec<FleetInstance>> = vec![Vec::new(); FLEETS.len()];
    for index in 0..INSTANCES {
        let lifecycle = match index % 8 {
            6 => InstanceLifecycle::Pending,
            7 => InstanceLifecycle::Terminating,
            _ => InstanceLifecycle::InService,
        };
        let id = instance_id(index)?;
        fleets[fleet_of(index)?].push(fleet_instance(&id, lifecycle, 1_000 + index));
    }
    let mut supplies = Vec::new();
    for instances in fleets {
        supplies.push(FleetSupply {
            desired: u32::try_from(instances.len())?,
            instances,
            warm: 0,
            exits: Vec::new(),
        });
    }
    Ok(supplies)
}

/// The scheduler snapshot: 24 hosted workers in mixed states and 12 queued actions.
fn scheduler_snapshot() -> Result<SchedulerSnapshot, Box<dyn Error>> {
    let mut workers = Vec::new();
    for index in 0..HOSTED {
        let fleet = FLEETS[fleet_of(index)?];
        let running = if index % 2 == 0 { 2 } else { 0 };
        let draining = index % 6 == 5;
        let id = worker_on(&instance_id(index)?)?;
        workers.push(worker_state(
            &id,
            fleet,
            running,
            draining,
            NonZeroU32::new(4),
        )?);
    }
    workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    let mut queued_actions = Vec::new();
    for index in 0..QUEUED {
        queued_actions.push(queued(index, two_fleets()?));
    }
    Ok(SchedulerSnapshot {
        completed: 100,
        workers,
        queued: queued_actions,
    })
}

/// The actions sample: 12 running actions on the first workers and 12 queued actions.
fn actions_sample() -> Result<ActionsSample, Box<dyn Error>> {
    let mut running = Vec::new();
    for index in 0..RUNNING {
        running.push(RunningActionView {
            info: ActionInfo {
                op: format!("ops/op-{index}"),
                digest: format!("{index:064x}/{}", 10 + index),
                target: "//pkg:target".to_owned(),
                mnemonic: "Javac".to_owned(),
                priority: 0,
            },
            worker: worker_on(&instance_id(index)?)?,
            started: ts(5_000 + index),
        });
    }
    let mut queued = Vec::new();
    for index in 0..QUEUED {
        queued.push(QueuedActionView {
            info: ActionInfo {
                op: format!("ops/queued-{index}"),
                digest: format!("{index:064x}/{}", 20 + index),
                target: "//pkg:queued".to_owned(),
                mnemonic: "Genrule".to_owned(),
                priority: 0,
            },
            queued_since: ts(4_000 + index),
            candidates: two_fleets()?,
        });
    }
    Ok(ActionsSample {
        at: ts(WALL_MILLIS),
        view: ActionsView {
            running,
            queued,
            truncated_running: 0,
            truncated_queued: 0,
        },
    })
}

/// The events of the sample: a scale-out, a drain and a registration.
fn events() -> Result<Vec<Event>, Box<dyn Error>> {
    Ok(vec![
        Event {
            name: "events/1".to_owned(),
            create_time: ts(1_000),
            fleet: "fleets/alpha".to_owned(),
            cause: Some(Cause::Demand),
            change: EventChange::ScaledOut { from: 1, to: 2 },
        },
        Event {
            name: "events/2".to_owned(),
            create_time: ts(2_000),
            fleet: "fleets/alpha".to_owned(),
            cause: Some(Cause::IdleTimeout),
            change: EventChange::DrainStarted {
                worker: name::of::<name::Workers>(&worker_on(&instance_id(0)?)?),
            },
        },
        Event {
            name: "events/3".to_owned(),
            create_time: ts(3_000),
            fleet: "fleets/beta".to_owned(),
            cause: None,
            change: EventChange::WorkerRegistered {
                worker: name::of::<name::Workers>(&worker_on(&instance_id(1)?)?),
            },
        },
    ])
}

/// Builds the world: three fleet policies, 24 hosted workers in mixed states in an indexed
/// snapshot, and an actions sample of 12 running and 12 queued rows.
fn world() -> Result<World, Box<dyn Error>> {
    let (policies, properties) = fleet_policies()?;
    let passes = policies
        .into_iter()
        .zip(supplies()?)
        .map(|(policy, supply)| FleetPass {
            policy,
            supply,
            outlook: outlook(),
            past_boot_grace: Vec::new(),
        })
        .collect();
    let mut snapshot = scheduler_snapshot()?;
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut snapshot);
    Ok(World {
        passes,
        catalog: FleetCatalog::try_new(properties)?,
        hosted,
        actions: Some(actions_sample()?),
        events: events()?,
    })
}

/// Once the builder has grown its buffers, a completed pass with unchanged inputs allocates
/// nothing. The two frames alternate as the server swaps them after each fill.
#[test]
fn steady_completed_pass_allocates_nothing() -> TestResult {
    let world = world()?;
    let view = inputs(&world);
    let mut builder = Builder::new(config());
    let mut cur = blank();
    let mut prev = blank();

    let mut seq = 0;
    for _ in 0..WARM_UP_PASSES {
        seq += 1;
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }

    seq += 1;
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, seq, &view);
    });

    assert_eq!(
        info.count_total, 0,
        "steady-state pass made {} allocations",
        info.count_total
    );
    assert_eq!(
        info.bytes_total, 0,
        "steady-state pass allocated {} bytes",
        info.bytes_total
    );
    Ok(())
}
