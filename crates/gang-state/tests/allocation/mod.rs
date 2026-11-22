//! A steady-state builder pass must not allocate once the builder's buffers have grown.

use std::error::Error;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_config::FleetPolicy;
use gang_reconciler::{FleetOutlook, FleetPass, HostedSnapshot, WorkerCounts};
use gang_spi::{
    ActionInfo, ActionsView, EngineStore, FleetInstance, FleetSupply, QueuedActionView,
    RunningActionView, SchedulerSnapshot, StoreKind, Traffic,
};
use gang_state::{ActionsSample, Builder, Inputs, PassStatus};
use gang_testkit::{TestResult, worker_on};
use gang_types::{
    ActionResources, Bytes, FleetCatalog, FleetProperties, FleetSet, InstanceLifecycle,
};
use gang_wire::{Cause, Event, EventChange, SkipReason, name};

use crate::support::{
    blank, build_key, config, fleet_instance, instance_id, policy, properties, queued, ts,
    worker_state,
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
    builds: gang_insight::Builds,
}

fn ok(applied: u32) -> PassStatus<'static> {
    PassStatus::Ok {
        applied,
        failed: 0,
        error: None,
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
    demand.add_queued(ActionResources {
        cpu_millicores: 2000,
        memory: Bytes::new(1_024),
        disk: Bytes::new(0),
    });
    FleetOutlook {
        minimum: 0,
        needed: 2,
        demand,
        queued: 4,
        candidates: 2,
        workers: WorkerCounts::default(),
        readiness: None,
        expected_wait: None,
        load: Some(125),
        backlog: Some(Duration::from_secs(42)),
        utilization: Some(735),
    }
}

/// The inputs of one pass over `world`. Only borrows, so building it allocates nothing.
fn inputs(world: &World) -> Inputs<'_> {
    Inputs {
        wall: ts(WALL_MILLIS),
        pass: ok(0),
        fleets: &world.passes,
        catalog: &world.catalog,
        actions: world.actions.as_ref(),
        events: &world.events,
        infrastructure: &[],
        builds: &world.builds,
        traffic: Traffic {
            lookups: 12,
            hits: 8,
            uploaded: 4096,
            downloaded: 8192,
        },
        measured: None,
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
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: u32::try_from(instances.len())?,
            instances,
            warm: 0,
            exits: Vec::new(),
            launch_failure: Some(gang_spi::LaunchFailure {
                reason: "capacity unavailable".to_owned(),
            }),
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
        stores: vec![
            EngineStore {
                store: StoreKind::Content,
                used: 512,
                budget: 1024,
                evicted: 20,
            },
            EngineStore {
                store: StoreKind::Action,
                used: 64,
                budget: 128,
                evicted: 3,
            },
        ],
        workers,
        queued: queued_actions,
        executing: Vec::new(),
        ..SchedulerSnapshot::default()
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
                build: None,
                reservation: ActionResources::default(),
                learned_reservation: None,
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
                build: None,
                reservation: ActionResources::default(),
                learned_reservation: None,
            },
            queued_since: ts(4_000 + index),
            candidates: FleetSet::default(),
            refusal: Some(gang_types::Refusal::Combination),
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
            change: EventChange::ScaledOut {
                from: 1,
                to: 2,
                detail: Some("backlog requires capacity".to_owned()),
            },
        },
        Event {
            name: "events/2".to_owned(),
            create_time: ts(2_000),
            fleet: "fleets/alpha".to_owned(),
            cause: Some(Cause::IdleTimeout),
            change: EventChange::DrainStarted {
                worker: name::of::<name::Workers>(&worker_on(&instance_id(0)?)?),
                detail: Some("idle timeout elapsed".to_owned()),
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
            handover_protected: Vec::new(),
        })
        .collect();
    let mut snapshot = scheduler_snapshot()?;
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut snapshot);
    let key = build_key("allocation")?;
    let mut builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    builds.lose(ts(WALL_MILLIS), &key, 1);
    let mut actions = actions_sample()?;
    for action in &mut actions.view.running {
        action.info.build = Some(key.id());
    }
    for action in &mut actions.view.queued {
        action.info.build = Some(key.id());
    }
    Ok(World {
        passes,
        catalog: FleetCatalog::try_new(properties)?,
        hosted,
        actions: Some(actions),
        events: events()?,
        builds,
    })
}

/// Once the builder has grown its buffers, a completed pass with unchanged inputs allocates
/// nothing. The two frames alternate as the server swaps them after each fill.
#[test]
fn steady_completed_pass_allocates_nothing() -> TestResult {
    let world = world()?;
    let mut view = inputs(&world);
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    let supplies = vec![true; world.passes.len()];
    let observed = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        ..inputs(&world)
    };
    builder.fill(&mut cur, &prev, 0, &observed);
    std::mem::swap(&mut cur, &mut prev);

    let mut seq = 0;
    for _ in 0..WARM_UP_PASSES {
        seq += 1;
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }

    view.traffic.uploaded += 128;
    view.traffic.downloaded += 256;
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

/// Alternating frames retain their capacity when unchanged passes are skipped.
#[test]
fn steady_skipped_pass_allocates_nothing() -> TestResult {
    let world = world()?;
    let view = inputs(&world);
    let skipped = Inputs {
        pass: PassStatus::Skipped {
            reason: SkipReason::SchedulerUnavailable,
            error: None,
        },
        actions: None,
        ..inputs(&world)
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    let supplies = vec![true; world.passes.len()];
    let observed = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        ..inputs(&world)
    };
    builder.fill(&mut cur, &prev, 0, &observed);
    std::mem::swap(&mut cur, &mut prev);

    let mut seq = 0;
    for _ in 0..WARM_UP_PASSES {
        seq += 1;
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }
    for _ in 0..WARM_UP_PASSES {
        seq += 1;
        builder.fill(&mut cur, &prev, seq, &skipped);
        std::mem::swap(&mut cur, &mut prev);
    }

    seq += 1;
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, seq, &skipped);
    });

    assert_eq!(
        info.count_total, 0,
        "steady-state skipped pass made {} allocations",
        info.count_total
    );
    assert_eq!(
        info.bytes_total, 0,
        "steady-state skipped pass allocated {} bytes",
        info.bytes_total
    );
    Ok(())
}

#[test]
fn steady_replacement_projection_allocates_nothing() -> TestResult {
    use gang_spi::{FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind};
    use gang_types::FleetName;
    let mut world = world()?;
    let old = instance_id(0)?;
    let new = instance_id(3)?;
    let pass = &mut world.passes[0];
    pass.supply.requested =
        Some(u32::try_from(pass.supply.instances.len() - 1).expect("fixture count fits u32"));
    let observations = [FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "rebalance".into(),
            at: ts(5_000),
            kind: InfrastructureEventKind::ReplacementLaunched {
                instance: new,
                replacing: Some(old),
            },
        },
    }];
    let mut view = inputs(&world);
    view.infrastructure = &observations;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    let supplies = vec![true; world.passes.len()];
    let observed = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        ..inputs(&world)
    };
    builder.fill(&mut cur, &prev, 0, &observed);
    std::mem::swap(&mut cur, &mut prev);
    for seq in 1..=WARM_UP_PASSES as u64 {
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }
    assert_eq!(prev.status_events.len(), 1);
    assert_eq!(
        prev.workers
            .iter()
            .filter(|worker| worker.infrastructure.is_some())
            .count(),
        2
    );
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, WARM_UP_PASSES as u64 + 1, &view);
    });
    assert_eq!(info.count_total, 0, "steady replacement pass allocated");
    assert_eq!(
        info.bytes_total, 0,
        "steady replacement pass allocated bytes"
    );
    Ok(())
}

#[test]
fn steady_partial_observation_allocates_nothing() -> TestResult {
    let world = world()?;
    let supplies = vec![true; world.passes.len()];
    let view = Inputs {
        pass: PassStatus::Observed {
            hosted: None,
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        ..inputs(&world)
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    for seq in 1..=WARM_UP_PASSES as u64 {
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, WARM_UP_PASSES as u64 + 1, &view);
    });
    assert_eq!(info.count_total, 0, "steady observation allocated");
    assert_eq!(info.bytes_total, 0, "steady observation allocated bytes");
    Ok(())
}

#[test]
fn steady_hosted_inventory_observation_allocates_nothing() -> TestResult {
    let mut world = world()?;
    let hosted = &world.hosted;
    for pass in &mut world.passes {
        let fleet = &pass.policy.name;
        pass.supply
            .instances
            .retain(|instance| hosted.workers_on(fleet, &instance.id).next().is_some());
    }
    let supplies = vec![true; world.passes.len()];
    let view = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        actions: None,
        ..inputs(&world)
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    let hosted_count = usize::try_from(HOSTED)?;
    for seq in 1..=WARM_UP_PASSES as u64 {
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }
    assert_eq!(prev.workers.len(), hosted_count);
    assert!(prev.instances.is_empty());
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, WARM_UP_PASSES as u64 + 1, &view);
    });
    assert_eq!(info.count_total, 0, "steady hosted observation allocated");
    assert_eq!(
        info.bytes_total, 0,
        "steady hosted observation allocated bytes"
    );
    assert_eq!(cur.workers.len(), hosted_count);
    assert!(cur.instances.is_empty());
    Ok(())
}

#[test]
fn observation_reuses_instance_buffers_across_identity_churn() -> TestResult {
    let world = world()?;
    let supplies = vec![true; world.passes.len()];
    let mut observer_passes = world.passes.clone();
    for pass in &mut observer_passes {
        pass.outlook = FleetOutlook::default();
        pass.past_boot_grace.clear();
    }
    let view = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        fleets: &observer_passes,
        actions: None,
        ..inputs(&world)
    };
    let mut replacement = observer_passes.clone();
    let row = replacement[0]
        .supply
        .instances
        .iter_mut()
        .find(|instance| instance.id == instance_id(24).expect("fixture id"))
        .ok_or("fixture instance")?;
    row.id = instance_id(90)?;
    let changed = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        fleets: &replacement,
        actions: None,
        ..inputs(&world)
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    for seq in 1..=WARM_UP_PASSES as u64 {
        builder.fill(&mut cur, &prev, seq, &view);
        std::mem::swap(&mut cur, &mut prev);
    }
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, WARM_UP_PASSES as u64 + 1, &changed);
    });
    assert_eq!(info.count_total, 0, "identity churn allocated");
    assert_eq!(info.bytes_total, 0, "identity churn allocated bytes");
    Ok(())
}

#[test]
fn observation_reuses_worker_buffers_across_generation_churn() -> TestResult {
    let world = world()?;
    let supplies = vec![true; world.passes.len()];
    let mut snapshot = world.hosted.snapshot().clone();
    let id = instance_id(0)?;
    let replacement = gang_types::WorkerId::try_new(format!(
        "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
        id.worker_name()
    ))?;
    let original = worker_on(&id)?;
    let worker = snapshot
        .workers
        .iter_mut()
        .find(|worker| worker.id == original)
        .ok_or("fixture worker")?;
    worker.id = replacement;
    snapshot.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    let mut changed_hosted = HostedSnapshot::default();
    changed_hosted.replace(&mut snapshot);
    let view = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&world.hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        actions: None,
        ..inputs(&world)
    };
    let changed = Inputs {
        pass: PassStatus::Observed {
            hosted: Some(&changed_hosted),
            supplies: &supplies,
            fresh_supplies: &supplies,
        },
        actions: None,
        ..inputs(&world)
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut cur = blank();
    let mut prev = blank();
    for seq in 1..=WARM_UP_PASSES as u64 {
        let input = if seq % 2 == 0 { &changed } else { &view };
        builder.fill(&mut cur, &prev, seq, input);
        std::mem::swap(&mut cur, &mut prev);
    }
    let info = allocation_counter::measure(|| {
        builder.fill(&mut cur, &prev, WARM_UP_PASSES as u64 + 1, &view);
    });
    assert_eq!(info.count_total, 0, "worker generation churn allocated");
    assert_eq!(
        info.bytes_total, 0,
        "worker generation churn allocated bytes"
    );
    Ok(())
}
