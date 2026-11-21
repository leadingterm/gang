//! Behaviour of the state builder, driven by hand-built scheduler and provisioner inputs.

mod reuse;

use std::error::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{TestClock, Timestamp};
use gang_config::{ActionsConfig, FleetPolicy, ScalingConfig};
use gang_reconciler::{
    Action, FleetOutlook, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler,
    SourceProfiles, TerminationCause, WorkerCounts,
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
    infrastructure: Vec<gang_spi::FleetInfrastructureEvent>,
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
    Observed { known: bool, supplies: Vec<bool> },
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
        reservation: ActionResources::default(),
        learned_reservation: None,
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
    let mut observer_passes = passes.clone();
    for pass in &mut observer_passes {
        pass.outlook = FleetOutlook::default();
        pass.past_boot_grace.clear();
    }
    let catalog = FleetCatalog::try_new(world.properties.clone())?;
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut world.snapshot.clone());
    let fresh = vec![true; passes.len()];
    let empty_builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let observed = Inputs {
        wall: ts(wall),
        pass: PassStatus::Observed {
            hosted: Some(&hosted),
            supplies: &fresh,
            fresh_supplies: &fresh,
        },
        fleets: &observer_passes,
        catalog: &catalog,
        actions: world.actions.as_ref(),
        events: &[],
        infrastructure: &world.infrastructure,
        builds: world.builds.as_ref().unwrap_or(&empty_builds),
        traffic: world.traffic,
        measured: world.measured.as_ref(),
        principals_epoch: 7,
    };
    let mut observed_state = blank();
    let base = if matches!(&outcome, Outcome::Ok { .. }) {
        builder.fill(&mut observed_state, prev, seq, &observed);
        &observed_state
    } else {
        prev
    };
    let pass = match outcome {
        Outcome::Ok { applied, failed } => PassStatus::Ok {
            applied,
            failed,
            error: None,
        },
        Outcome::Skipped(reason) => PassStatus::Skipped {
            reason,
            error: None,
        },
        Outcome::Observed {
            known,
            ref supplies,
        } => PassStatus::Observed {
            hosted: known.then_some(&hosted),
            supplies,
            fresh_supplies: supplies,
        },
    };
    let projection_passes = if matches!(&pass, PassStatus::Observed { .. }) {
        &observer_passes
    } else {
        &passes
    };
    let empty_builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let inputs = Inputs {
        wall: ts(wall),
        pass,
        fleets: projection_passes,
        catalog: &catalog,
        actions: world.actions.as_ref(),
        events: &[],
        infrastructure: &world.infrastructure,
        builds: world.builds.as_ref().unwrap_or(&empty_builds),
        traffic: world.traffic,
        measured: world.measured.as_ref(),
        principals_epoch: 7,
    };
    builder.fill(cur, base, seq, &inputs);
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
            reservation: ActionResources::default(),
            learned_reservation: None,
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
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 2,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            requested: None,
            events: Vec::new(),
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
            load: None,
            backlog: None,
            utilization: None,
        },
        FleetOutlook::default(),
    ];
    world.snapshot.queued = vec![queued(0, fleet_set(&[0]))];
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
fn fleet_rate_diagnostics_follow_outlook_and_round_trip() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut prev = blank();
    let mut cur = blank();
    let values = [
        (None, None, None),
        (Some(125), Some(Duration::from_secs(42)), Some(735)),
        (Some(0), Some(Duration::ZERO), Some(0)),
        (Some(3), None, None),
        (None, Some(Duration::from_secs(1)), None),
        (None, None, Some(1)),
        (None, None, None),
    ];
    for (index, (load, backlog, utilization)) in values.into_iter().enumerate() {
        world.outlook = vec![FleetOutlook {
            load,
            backlog,
            utilization,
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
        let demand = cur.fleets[0].demand;
        assert_eq!(demand.load, load);
        assert_eq!(demand.backlog, backlog);
        assert_eq!(demand.utilization, utilization);
        if index > 0 {
            let delta = diff(&prev, &cur).ok_or("missing fleet rate diagnostic delta")?;
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
        requested: None,
        events: Vec::new(),
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

fn worker_cache_world() -> Result<(World, [WorkerId; 4]), Box<dyn Error>> {
    use gang_spi::WorkerCacheStats;
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|p| properties(p, Vec::new()))
        .collect();
    let ids = [
        worker_on(&instance_id(1)?)?,
        worker_on(&instance_id(2)?)?,
        worker_on(&instance_id(3)?)?,
        worker_on(&instance_id(4)?)?,
    ];
    world.snapshot.workers = vec![
        worker_state(&ids[0], "alpha", 0, false, None)?,
        worker_state(&ids[1], "alpha", 0, false, None)?,
        worker_state(&ids[2], "alpha", 0, false, None)?,
        worker_state(&ids[3], "beta", 0, false, None)?,
    ];
    let stats = WorkerCacheStats {
        local_read_requests: 1,
        local_read_bytes: 2,
        slow_read_requests: 3,
        slow_read_bytes: 4,
        evictions: 5,
        evicted_bytes: 6,
        size_bytes: 7,
        budget_bytes: 8,
    };
    world.snapshot.workers[0].cache = Some(stats);
    world.snapshot.workers[1].cache = Some(stats);
    world.snapshot.workers[3].cache = Some(WorkerCacheStats::default());
    Ok((world, ids))
}

#[test]
fn worker_cache_reports_current_store_measurements() -> TestResult {
    use gang_wire::{CacheReads, FleetWorkerCache, WorkerCache};
    let (world, ids) = worker_cache_world()?;
    let expected = WorkerCache {
        local: CacheReads {
            requests: 1,
            bytes: 2,
        },
        remote: CacheReads {
            requests: 3,
            bytes: 4,
        },
        evictions: 5,
        evicted: 6,
        used: 7,
        budget: 8,
    };
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let first = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert_eq!(
        find_worker(&first, &ids[0]).ok_or("worker")?.cache,
        Some(expected)
    );
    assert_eq!(
        find_worker(&first, &ids[1]).ok_or("worker")?.cache,
        Some(expected)
    );
    assert_eq!(find_worker(&first, &ids[2]).ok_or("worker")?.cache, None);
    assert_eq!(
        find_worker(&first, &ids[3]).ok_or("worker")?.cache,
        Some(WorkerCache::default())
    );
    assert_eq!(
        first.fleets[0].worker_cache,
        FleetWorkerCache {
            reporting: 2,
            workers: 3,
            totals: WorkerCache {
                local: CacheReads {
                    requests: 2,
                    bytes: 4
                },
                remote: CacheReads {
                    requests: 6,
                    bytes: 8
                },
                evictions: 10,
                evicted: 12,
                used: 14,
                budget: 16,
            },
        }
    );
    assert_eq!(
        first.fleets[1].worker_cache,
        FleetWorkerCache {
            reporting: 1,
            workers: 1,
            totals: WorkerCache::default(),
        }
    );

    Ok(())
}

#[test]
fn worker_cache_disappearance_and_restart_replace_totals() -> TestResult {
    use gang_spi::WorkerCacheStats;
    use gang_wire::{FleetWorkerCache, WorkerCache};
    let (mut world, ids) = worker_cache_world()?;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let first = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    let unavailable = fill(
        &mut builder,
        &first,
        2,
        &world,
        2_000,
        Outcome::Observed {
            known: false,
            supplies: vec![true, true],
        },
    )?;
    assert_eq!(
        unavailable.fleets[0].worker_cache,
        first.fleets[0].worker_cache
    );
    assert_eq!(
        find_worker(&unavailable, &ids[0])
            .ok_or("stale worker")?
            .cache,
        find_worker(&first, &ids[0]).ok_or("worker")?.cache
    );

    // One report vanishes, one worker disconnects, and another store restarts at zero.
    world.snapshot.workers[0].cache = None;
    world.snapshot.workers[1].cache = Some(WorkerCacheStats::default());
    world.snapshot.workers.remove(2);
    let mut second = first.clone();
    fill_into(
        &mut builder,
        &mut second,
        &unavailable,
        3,
        &world,
        3_000,
        Outcome::Observed {
            known: true,
            supplies: vec![true, true],
        },
    )?;
    assert_eq!(find_worker(&second, &ids[0]).ok_or("worker")?.cache, None);
    assert_eq!(
        second.fleets[0].worker_cache,
        FleetWorkerCache {
            reporting: 1,
            workers: 2,
            totals: WorkerCache::default(),
        }
    );

    Ok(())
}

#[test]
fn worker_cache_snapshot_and_delta_round_trip_over_sse() -> TestResult {
    use gang_wire::{Frame, FrameRef};
    let (mut world, _) = worker_cache_world()?;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let first = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    world.snapshot.workers[0].cache = None;
    world.snapshot.workers.remove(2);
    let second = fill(&mut builder, &first, 2, &world, 2_000, ok(0))?;
    let delta = diff(&first, &second).ok_or("missing cache delta")?;
    let snapshot_bytes = gang_state::sse::encode(FrameRef::Snapshot(&first))?;
    let delta_bytes = gang_state::sse::encode(FrameRef::Delta(&delta))?;
    let mut decoder = gang_state::sse::Decoder::new();
    let frames = decoder.push(&[snapshot_bytes, delta_bytes].concat())?;
    let [Frame::Snapshot(mut rebuilt), Frame::Delta(change)] =
        <[Frame; 2]>::try_from(frames).map_err(|_| "expected snapshot and delta")?
    else {
        return Err("wrong frame types".into());
    };
    gang_state::apply(&mut rebuilt, &change)?;
    assert_eq!(rebuilt, second);
    Ok(())
}

#[test]
fn worker_cache_fleet_sums_saturate_each_measurement() -> TestResult {
    use gang_spi::WorkerCacheStats;
    use gang_wire::{CacheReads, WorkerCache};
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|p| properties(p, Vec::new()))
        .collect();
    let stats = WorkerCacheStats {
        local_read_requests: u64::MAX,
        local_read_bytes: u64::MAX,
        slow_read_requests: u64::MAX,
        slow_read_bytes: u64::MAX,
        evictions: u64::MAX,
        evicted_bytes: u64::MAX,
        size_bytes: u64::MAX,
        budget_bytes: u64::MAX,
    };
    for index in 1..=2 {
        let mut worker = worker_state(&worker_on(&instance_id(index)?)?, "alpha", 0, false, None)?;
        worker.cache = Some(stats);
        world.snapshot.workers.push(worker);
    }
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let snapshot = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert_eq!(
        snapshot.fleets[0].worker_cache.totals,
        WorkerCache {
            local: CacheReads {
                requests: u64::MAX,
                bytes: u64::MAX
            },
            remote: CacheReads {
                requests: u64::MAX,
                bytes: u64::MAX
            },
            evictions: u64::MAX,
            evicted: u64::MAX,
            used: u64::MAX,
            budget: u64::MAX,
        }
    );
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
            requested: None,
            events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
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

    assert_fleet_counts(&cur, &pending, &unhosted);
    Ok(())
}

fn assert_fleet_counts(cur: &State, pending: &InstanceId, unhosted: &InstanceId) {
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
    assert_eq!(listed, [instance_name(pending), instance_name(unhosted)]);
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
        requested: None,
        events: Vec::new(),
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
    assert!(!first_row.stale);
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
    world.snapshot.workers[0].current_build = false;
    world.snapshot.workers[0].running_actions = 2;
    let fourth = fill(&mut builder, &third, 4, &world, 4_000, ok(0))?;
    let fourth_row = find_worker(&fourth, &worker).ok_or("worker missing")?;
    assert_eq!(fourth_row.phase, gang_wire::Phase::Draining);
    assert!(fourth_row.stale);
    assert_eq!(fourth_row.running, 2);
    assert_eq!(fourth_row.phase_time, ts(4_000));

    world.snapshot.workers[0].current_build = true;
    let fifth = fill(&mut builder, &fourth, 5, &world, 5_000, ok(0))?;
    let fifth_row = find_worker(&fifth, &worker).ok_or("worker missing")?;
    assert!(!fifth_row.stale);
    assert_eq!(fifth_row.phase, gang_wire::Phase::Draining);
    Ok(())
}

#[test]
fn launch_time_of_a_worker_without_instance_is_carried_over() -> TestResult {
    let mut world = World::default();
    let alpha = policy("alpha", 100, 4)?;
    world.properties = vec![properties(&alpha, Vec::new())];
    world.policies = vec![alpha];
    world.supplies = vec![FleetSupply {
        requested: None,
        events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 0,
            instances: Vec::new(),
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
        FleetSupply {
            requested: None,
            events: Vec::new(),
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
        assert_eq!(cur.queue.unplaceable, total);
        assert_eq!(cur.fleets[0].demand.queued, first);
        assert_eq!(cur.fleets[1].demand.queued, second);
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
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
    let scaling = Arc::new(ScalingConfig::default());
    let profiles = SourceProfiles::new(&scaling, ActionsConfig::default().history_window);
    let mut reconciler = Reconciler::new(
        &world.policies,
        scaling,
        profiles,
        ActionsConfig::default().cpu_oversubscription,
    );
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
                mode: gang_spi::TerminationMode::ScaleIn,
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
            requested: None,
            events: Vec::new(),
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
            requested: None,
            events: Vec::new(),
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
        requested: None,
        events: Vec::new(),
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
            change: gang_wire::EventChange::ScaledOut {
                from: 1,
                to: 2,
                detail: None,
            },
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
        pass: PassStatus::Skipped {
            reason: SkipReason::SchedulerUnavailable,
            error: None,
        },
        fleets: &[],
        catalog: &catalog,
        actions: None,
        events: &[],
        infrastructure: &[],
        builds: &builds,
        traffic: Traffic::default(),
        measured: None,
        principals_epoch: 0,
    };
    builder.fill(&mut prev, &blank(), 0, &initial);
    let mut cur = blank();
    let inputs = Inputs {
        wall: ts(10),
        pass: PassStatus::Skipped {
            reason: SkipReason::SchedulerUnavailable,
            error: None,
        },
        fleets: &[],
        catalog: &catalog,
        actions: None,
        events: &events,
        infrastructure: &[],
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

fn history_world() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 7)?];
    world.properties = world
        .policies
        .iter()
        .map(|p| properties(p, Vec::new()))
        .collect();
    world.supplies = vec![
        FleetSupply {
            desired: 3,
            warm: 1,
            instances: vec![fleet_instance(
                &instance_id(1)?,
                InstanceLifecycle::Pending,
                0,
            )],
            ..FleetSupply::default()
        },
        FleetSupply::default(),
    ];
    Ok(world)
}

fn history_control(
    builder: &mut Builder,
    prev: &State,
    world: &World,
) -> Result<State, Box<dyn Error>> {
    let passes = world.passes();
    let catalog = FleetCatalog::try_new(world.properties.clone())?;
    let builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let inputs = Inputs {
        wall: ts(3_500),
        pass: PassStatus::Ok {
            applied: 0,
            failed: 0,
        },
        fleets: &passes,
        catalog: &catalog,
        actions: None,
        events: &[],
        infrastructure: &[],
        builds: &builds,
        traffic: Traffic::default(),
        measured: None,
        principals_epoch: 7,
    };
    let mut control = blank();
    builder.fill(&mut control, prev, 4, &inputs);
    Ok(control)
}

fn fleet_census(state: &State) -> Vec<gang_wire::FleetInstanceSample> {
    state
        .fleets
        .iter()
        .map(|fleet| gang_wire::FleetInstanceSample {
            name: fleet.name.clone(),
            instances: fleet.instances,
            desired: fleet.desired,
            max: fleet.policy.max,
        })
        .collect()
}

#[test]
fn fleet_history_tracks_independent_supply_observations() -> TestResult {
    let mut world = history_world()?;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let initial = builder.initial(ts(0), 7);
    let first = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Observed {
            known: false,
            supplies: vec![true, false],
        },
    )?;
    let sample = first.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(sample.scheduler_known, Some(false));
    assert_eq!(sample.fleets.len(), 1);
    assert_eq!(sample.fleets[0].name, "fleets/alpha");
    assert_eq!(sample.fleets[0].instances.pending, 1);
    assert_eq!(sample.fleets[0].instances.warm, 1);
    assert_eq!(sample.fleets[0].desired, 3);
    assert_eq!(sample.fleets[0].max, first.fleets[0].policy.max);

    let second = fill(
        &mut builder,
        &first,
        2,
        &world,
        2_000,
        Outcome::Observed {
            known: true,
            supplies: vec![false, true],
        },
    )?;
    let sample = second.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(second.fleets[0].instances.pending, 1);
    assert_eq!(sample.fleets.len(), 1);
    assert_eq!(sample.fleets[0].name, "fleets/beta");
    assert_eq!(
        sample.fleets[0].instances,
        gang_wire::InstanceCounts::default()
    );
    assert_eq!(sample.fleets[0].desired, 0);

    world.supplies[0].desired = 2;
    let recovered = fill(
        &mut builder,
        &second,
        3,
        &world,
        3_000,
        Outcome::Observed {
            known: false,
            supplies: vec![true, true],
        },
    )?;
    let sample = recovered.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(sample.fleets.len(), 2);
    assert_eq!(sample.fleets[0].desired, 2);
    assert_eq!(sample.fleets[1].name, "fleets/beta");
    let control = history_control(&mut builder, &recovered, &world)?;
    assert_eq!(control.recent.samples[0].fleets, sample.fleets);
    let skipped = fill(
        &mut builder,
        &control,
        5,
        &world,
        4_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(skipped.recent.samples[0].fleets, sample.fleets);
    let later = fill(
        &mut builder,
        &skipped,
        6,
        &world,
        30_000,
        Outcome::Observed {
            known: true,
            supplies: vec![true, false],
        },
    )?;
    assert!(later.recent.samples[1].fleets.is_empty());
    assert!(later.recent.samples[2].fleets.is_empty());
    assert_eq!(later.recent.samples[3].fleets[0].name, "fleets/alpha");
    for (before, after) in [
        (&initial, &first),
        (&first, &second),
        (&second, &recovered),
        (&recovered, &control),
        (&control, &skipped),
        (&skipped, &later),
    ] {
        let delta = diff(before, after).ok_or("missing delta")?;
        let mut applied = before.clone();
        gang_state::apply(&mut applied, &delta)?;
        assert_eq!(&applied, after);
    }
    Ok(())
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
                running: 0,
                queued: 2,
                passes: 3,
                skipped: 0,
                lookups: 0,
                hits: 0,
                scheduler_known: Some(true),
                fleets: fleet_census(&prev),
                ..gang_wire::Sample::default()
            },
            gang_wire::Sample {
                start: ts(10_000),
                completed: 8,
                running: 0,
                queued: 4,
                passes: 2,
                skipped: 0,
                lookups: 0,
                hits: 0,
                scheduler_known: Some(true),
                fleets: fleet_census(&prev),
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
        (2, 1, 0, 4)
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
    assert_eq!((sample.lookups, sample.hits), (20, 13));
    world.traffic.lookups = 20;
    world.traffic.hits = 14;
    let resumed = fill(&mut builder, &skipped, 3, &world, 21_000, ok(0))?;
    let sample = resumed.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.hits), (20, 14));
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
#[test]
fn observations_sample_traffic_and_reestablish_scheduler_baselines() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World::default();
    world.snapshot.completed = 10;
    world.snapshot.stores.push(EngineStore {
        store: StoreKind::Content,
        used: 1,
        budget: 10,
        evicted: 20,
    });
    let initial = builder.initial(ts(0), 7);
    let first = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    world.traffic.lookups = 3;
    let skipped = fill(
        &mut builder,
        &first,
        2,
        &world,
        11_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(
        skipped
            .recent
            .samples
            .last()
            .ok_or("missing sample")?
            .lookups,
        3
    );
    world.traffic.lookups = 5;
    let unavailable = fill(
        &mut builder,
        &skipped,
        3,
        &world,
        21_000,
        Outcome::Observed {
            known: false,
            supplies: vec![],
        },
    )?;
    let sample = unavailable.recent.samples.last().ok_or("missing sample")?;
    assert_eq!((sample.lookups, sample.scheduler_known), (2, Some(false)));
    world.snapshot.completed = 30;
    world.snapshot.stores[0].evicted = 50;
    let recovered = fill(
        &mut builder,
        &unavailable,
        4,
        &world,
        31_000,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    let sample = recovered.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(
        (
            sample.completed,
            sample.evicted_bytes,
            sample.scheduler_known
        ),
        (0, 0, Some(false))
    );
    world.snapshot.completed = 32;
    world.snapshot.stores[0].evicted = 54;
    let next = fill(
        &mut builder,
        &recovered,
        5,
        &world,
        41_000,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    let sample = next.recent.samples.last().ok_or("missing sample")?;
    assert_eq!(
        (
            sample.completed,
            sample.evicted_bytes,
            sample.scheduler_known
        ),
        (2, 4, Some(true))
    );
    assert_eq!(next.pass, skipped.pass);
    Ok(())
}

#[test]
fn fresh_supply_is_projected_when_scheduler_is_unavailable() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World::default();
    world.policies = vec![policy("alpha", 0, 4)?, policy("beta", 0, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|policy| properties(policy, Vec::new()))
        .collect();
    world.supplies = vec![FleetSupply::default(), FleetSupply::default()];
    world.supplies[1].desired = 2;
    let initial = builder.initial(ts(0), 7);
    let first = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Observed {
            known: true,
            supplies: vec![true, true],
        },
    )?;
    world.supplies[0].desired = 3;
    world.supplies[0].instances.push(fleet_instance(
        &instance_id(1)?,
        InstanceLifecycle::Pending,
        0,
    ));
    world.supplies[0].warm = 1;
    world.supplies[1].desired = 4;
    let next = fill(
        &mut builder,
        &first,
        2,
        &world,
        11_000,
        Outcome::Observed {
            known: false,
            supplies: vec![true, false],
        },
    )?;
    assert_eq!(
        (next.fleets[0].desired, next.fleets[0].instances.warm),
        (3, 1)
    );
    assert_eq!(next.fleets[0].instances.pending, 1);
    assert_eq!(next.instances.len(), 1);
    assert_eq!(next.fleets[1].desired, 2);
    assert_eq!(
        next.recent
            .samples
            .last()
            .ok_or("missing sample")?
            .scheduler_known,
        Some(false)
    );
    Ok(())
}

#[test]
fn delayed_control_snapshot_does_not_replace_newer_observations() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World::default();
    let policy = policy("alpha", 0, 4)?;
    world.properties.push(properties(&policy, Vec::new()));
    world.policies.push(policy);
    let instance = instance_id(1)?;
    world.supplies.push(FleetSupply {
        desired: 2,
        launch_failure: Some(gang_spi::LaunchFailure {
            reason: "new".to_owned(),
        }),
        instances: vec![fleet_instance(&instance, InstanceLifecycle::InService, 0)],
        ..FleetSupply::default()
    });
    world.snapshot.workers.push(worker_state(
        &worker_on(&instance)?,
        "alpha",
        3,
        false,
        None,
    )?);
    world.snapshot.queued.push(queued(0, fleet_set(&[0])));
    world.snapshot.completed = 20;
    let initial = builder.initial(ts(0), 7);
    let observed = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Observed {
            known: true,
            supplies: vec![true],
        },
    )?;
    let catalog = FleetCatalog::try_new(world.properties.clone())?;
    let mut passes = world.passes();
    passes[0].supply.desired = 9;
    passes[0].supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "old".to_owned(),
    });
    passes[0].supply.instances.clear();
    let builds = gang_insight::Builds::new(&gang_config::BuildsConfig::default());
    let inputs = Inputs {
        wall: ts(2_000),
        pass: PassStatus::Ok {
            applied: 0,
            failed: 0,
            error: None,
        },
        fleets: &passes,
        catalog: &catalog,
        actions: None,
        events: &[],
        infrastructure: &[],
        builds: &builds,
        traffic: Traffic::default(),
        measured: None,
        principals_epoch: 7,
    };
    let mut control = blank();
    builder.fill(&mut control, &observed, 2, &inputs);
    assert_eq!(control.completed, 20);
    assert_eq!(control.fleets[0].desired, 2);
    assert_eq!(control.fleets[0].instances.in_service, 1);
    assert_eq!(
        control.fleets[0]
            .launch_failure
            .as_ref()
            .map(|failure| failure.reason.as_str()),
        Some("new")
    );
    assert_eq!(control.fleets[0].demand.running, 3);
    assert_eq!(passes[0].outlook.queued, 0);
    assert_eq!(control.fleets[0].demand.queued, 0);
    assert_eq!(control.queue.total, 1);
    world.snapshot.completed = 22;
    let next = fill(
        &mut builder,
        &control,
        3,
        &world,
        11_000,
        Outcome::Observed {
            known: true,
            supplies: vec![true],
        },
    )?;
    assert_eq!(
        next.recent
            .samples
            .last()
            .ok_or("missing sample")?
            .completed,
        2
    );
    Ok(())
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
        (2, 1, 20, 20, 0)
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
        (27, 11, 260, 224, 40)
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

#[test]
fn infrastructure_projection_tracks_connected_overlap_and_clears_completed_pair() -> TestResult {
    use gang_spi::{FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind};
    use gang_wire::{AwsEventKind, EventChange, InfrastructurePhase};
    let old = instance_id(1)?;
    let new = instance_id(2)?;
    let policy = policy("alpha", 0, 4)?;
    let mut world = World::default();
    world.properties.push(properties(&policy, vec![]));
    world.policies.push(policy);
    world.supplies.push(FleetSupply {
        desired: 2,
        requested: Some(1),
        instances: vec![
            fleet_instance(&old, InstanceLifecycle::InService, 1),
            fleet_instance(&new, InstanceLifecycle::InService, 2),
        ],
        ..FleetSupply::default()
    });
    world.snapshot.workers = vec![
        worker_state(&worker_on(&old)?, "alpha", 1, false, None)?,
        worker_state(&worker_on(&new)?, "alpha", 0, false, None)?,
    ];
    world.infrastructure.push(FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "launch".into(),
            at: ts(2),
            kind: InfrastructureEventKind::ReplacementLaunched {
                instance: new.clone(),
                replacing: None,
            },
        },
    });
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let frame = fill(&mut builder, &blank(), 1, &world, 3, ok(0))?;
    assert_eq!(frame.fleets[0].desired, 1);
    assert_eq!(
        find_worker(&frame, &worker_on(&old)?)
            .and_then(|w| w.infrastructure.as_ref())
            .map(|s| &s.phase),
        Some(&InfrastructurePhase::Replacing)
    );
    assert_eq!(
        find_worker(&frame, &worker_on(&new)?)
            .and_then(|w| w.infrastructure.as_ref())
            .map(|s| &s.phase),
        Some(&InfrastructurePhase::Replacement)
    );
    assert_eq!(frame.fleets[0].instances.in_service, 2);
    assert_eq!(frame.workers.len(), 2);
    assert!(
        matches!(&frame.status_events[0].change, EventChange::Aws { event } if matches!(&event.kind, AwsEventKind::ReplacementLaunched { replacing: Some(peer), .. } if peer == &instance_name(&old)))
    );
    world.supplies[0].instances[0].lifecycle = InstanceLifecycle::Terminating;
    let terminating = fill(&mut builder, &frame, 2, &world, 3, ok(0))?;
    assert_eq!(
        terminating
            .workers
            .iter()
            .filter(|worker| worker.infrastructure.is_some())
            .count(),
        2
    );
    assert_eq!(terminating.fleets[0].instances.terminating, 1);
    assert_eq!(terminating.fleets[0].instances.in_service, 1);
    assert_eq!(terminating.workers.len(), 2);
    world.supplies[0]
        .instances
        .retain(|instance| instance.id != old);
    world
        .snapshot
        .workers
        .retain(|worker| worker.id == worker_on(&new).expect("valid worker"));
    world.infrastructure.push(FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "termination".into(),
            at: ts(3),
            kind: InfrastructureEventKind::InstanceTerminated {
                instance: old.clone(),
                reason: gang_spi::InfrastructureTerminationReason::Rebalance,
            },
        },
    });
    let completed = fill(&mut builder, &terminating, 3, &world, 4, ok(0))?;
    assert_eq!(completed.fleets[0].instances.in_service, 1);
    assert!(completed.workers[0].infrastructure.is_none());
    assert!(
        matches!(&completed.status_events[0].change, EventChange::Aws { event } if matches!(&event.kind, AwsEventKind::ReplacementLaunched { replacing: Some(peer), .. } if peer == &instance_name(&old)))
    );
    let mut restarted = Builder::new(config(), gang_state::CacheBaseline::default());
    let replayed = fill(&mut restarted, &blank(), 1, &world, 4, ok(0))?;
    assert!(replayed.workers[0].infrastructure.is_none());
    assert!(
        matches!(&replayed.status_events[0].change, EventChange::Aws { event } if matches!(&event.kind, AwsEventKind::ReplacementLaunched { replacing: Some(peer), .. } if peer == &instance_name(&old)))
    );
    Ok(())
}

#[test]
fn ambiguous_replacement_does_not_invent_a_pair() -> TestResult {
    use gang_spi::{FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind};
    let old = instance_id(1)?;
    let other = instance_id(2)?;
    let new = instance_id(3)?;
    let policy = policy("alpha", 0, 4)?;
    let mut pass = FleetPass::new(policy);
    pass.supply.requested = Some(1);
    pass.supply.instances = vec![
        fleet_instance(&old, InstanceLifecycle::InService, 1),
        fleet_instance(&other, InstanceLifecycle::InService, 1),
        fleet_instance(&new, InstanceLifecycle::InService, 2),
    ];
    let records = vec![FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "launch".into(),
            at: ts(2),
            kind: InfrastructureEventKind::ReplacementLaunched {
                instance: new.clone(),
                replacing: None,
            },
        },
    }];
    let mut world = World::default();
    world.properties.push(properties(&pass.policy, vec![]));
    world.passes.push(pass);
    world.infrastructure = records;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let frame = fill(&mut builder, &blank(), 1, &world, 3, ok(0))?;
    let old_row = frame
        .instances
        .iter()
        .find(|row| row.name == instance_name(&old))
        .ok_or("missing old instance")?;
    let new_row = frame
        .instances
        .iter()
        .find(|row| row.name == instance_name(&new))
        .ok_or("missing replacement")?;
    assert!(old_row.infrastructure.is_none());
    assert_eq!(
        new_row.infrastructure.as_ref().map(|status| &status.phase),
        Some(&gang_wire::InfrastructurePhase::Replacement)
    );
    assert!(
        matches!(&frame.status_events[0].change, gang_wire::EventChange::Aws { event } if matches!(&event.kind, gang_wire::AwsEventKind::ReplacementLaunched { replacing: None, .. }))
    );
    Ok(())
}

#[test]
fn ordinary_instance_launch_has_no_rebalance_phase() -> TestResult {
    use gang_spi::{FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind};
    use gang_wire::{AwsEventKind, EventChange};
    let instance = instance_id(1)?;
    let policy = policy("alpha", 0, 4)?;
    let mut pass = FleetPass::new(policy);
    pass.supply
        .instances
        .push(fleet_instance(&instance, InstanceLifecycle::InService, 1));
    let records = vec![FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "ordinary".into(),
            at: ts(1),
            kind: InfrastructureEventKind::InstanceLaunched {
                instance: instance.clone(),
            },
        },
    }];
    let mut world = World::default();
    world.properties.push(properties(&pass.policy, vec![]));
    world.passes.push(pass);
    world.infrastructure = records;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let frame = fill(&mut builder, &blank(), 1, &world, 2, ok(0))?;
    assert!(frame.instances[0].infrastructure.is_none());
    assert!(
        matches!(&frame.status_events[0].change, EventChange::Aws { event } if matches!(&event.kind, AwsEventKind::InstanceLaunched { .. }))
    );
    Ok(())
}

#[test]
fn newest_risk_notice_wins_independent_of_record_order() -> TestResult {
    use gang_spi::{
        FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind,
        InfrastructureInterruptionAction,
    };
    let instance = instance_id(1)?;
    let mut pass = FleetPass::new(policy("alpha", 0, 4)?);
    pass.supply
        .instances
        .push(fleet_instance(&instance, InstanceLifecycle::InService, 1));
    let fleet = FleetName::try_new("alpha")?;
    let notice = |id: &str, at, kind| FleetInfrastructureEvent {
        fleet: fleet.clone(),
        event: InfrastructureEvent {
            id: id.into(),
            at: ts(at),
            kind,
        },
    };
    let mut world = World::default();
    world.properties.push(properties(&pass.policy, vec![]));
    world.passes.push(pass);
    world.infrastructure = vec![
        notice(
            "new",
            3,
            InfrastructureEventKind::SpotInterruption {
                instance: instance.clone(),
                action: InfrastructureInterruptionAction::Terminate,
            },
        ),
        notice(
            "old",
            2,
            InfrastructureEventKind::RebalanceRecommendation {
                instance: instance.clone(),
            },
        ),
    ];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    for _ in 0..2 {
        let frame = fill(&mut builder, &blank(), 1, &world, 4, ok(0))?;
        let status = frame.instances[0]
            .infrastructure
            .as_ref()
            .ok_or("missing risk")?;
        assert_eq!(
            status.reason,
            gang_wire::InfrastructureReason::SpotInterruption
        );
        world.infrastructure.reverse();
    }
    Ok(())
}

#[test]
fn terminating_members_do_not_create_an_unpaired_overshoot() -> TestResult {
    use gang_spi::{FleetInfrastructureEvent, InfrastructureEvent, InfrastructureEventKind};
    let old = instance_id(1)?;
    let new = instance_id(2)?;
    let mut pass = FleetPass::new(policy("alpha", 0, 4)?);
    pass.supply.requested = Some(1);
    pass.supply.instances = vec![
        fleet_instance(&old, InstanceLifecycle::Terminating, 3),
        fleet_instance(&new, InstanceLifecycle::InService, 2),
    ];
    let mut world = World::default();
    world.properties.push(properties(&pass.policy, vec![]));
    world.passes.push(pass);
    world.infrastructure = vec![FleetInfrastructureEvent {
        fleet: FleetName::try_new("alpha")?,
        event: InfrastructureEvent {
            id: "launch".into(),
            at: ts(2),
            kind: InfrastructureEventKind::ReplacementLaunched {
                instance: new.clone(),
                replacing: None,
            },
        },
    }];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let frame = fill(&mut builder, &blank(), 1, &world, 4, ok(0))?;
    let replacement = frame
        .instances
        .iter()
        .find(|row| row.name == instance_name(&new))
        .ok_or("missing replacement")?;
    assert!(replacement.infrastructure.is_none());
    Ok(())
}

mod repeated_rebalance {
    use std::error::Error;

    use gang_reconciler::FleetPass;
    use gang_spi::{
        FleetInfrastructureEvent, FleetSupply, InfrastructureEvent, InfrastructureEventKind,
        InfrastructureInterruptionAction, InfrastructureTerminationReason,
    };
    use gang_state::Builder;
    use gang_testkit::{TestResult, worker_on};
    use gang_types::{FleetName, InstanceId, InstanceLifecycle};
    use gang_wire::{AwsEventKind, EventChange, InfrastructurePhase, InfrastructureReason, State};

    use super::{World, fill, find_worker, instance_name, ok};
    use crate::support::{
        blank, config, fleet_instance, instance_id, policy, properties, ts, worker_state,
    };

    fn observation(
        id: &str,
        at: u64,
        kind: InfrastructureEventKind,
    ) -> Result<FleetInfrastructureEvent, Box<dyn Error>> {
        Ok(FleetInfrastructureEvent {
            fleet: FleetName::try_new("alpha")?,
            event: InfrastructureEvent {
                id: id.into(),
                at: ts(at),
                kind,
            },
        })
    }

    fn launch(
        id: &str,
        at: u64,
        instance: &InstanceId,
        replacing: Option<InstanceId>,
    ) -> Result<FleetInfrastructureEvent, Box<dyn Error>> {
        observation(
            id,
            at,
            InfrastructureEventKind::ReplacementLaunched {
                instance: instance.clone(),
                replacing,
            },
        )
    }

    fn termination(
        id: &str,
        at: u64,
        instance: &InstanceId,
        reason: InfrastructureTerminationReason,
    ) -> Result<FleetInfrastructureEvent, Box<dyn Error>> {
        observation(
            id,
            at,
            InfrastructureEventKind::InstanceTerminated {
                instance: instance.clone(),
                reason,
            },
        )
    }

    fn assert_pair(frame: &State, id: &str, old: &InstanceId, new: &InstanceId) -> TestResult {
        let event = frame
            .status_events
            .iter()
            .find_map(|row| match &row.change {
                EventChange::Aws { event } if event.id == id => Some(event),
                _ => None,
            })
            .ok_or("replacement launch missing from status history")?;
        assert!(
            matches!(&event.kind, AwsEventKind::ReplacementLaunched { instance, replacing: Some(peer) }
                if instance == &instance_name(new) && peer == &instance_name(old))
        );
        Ok(())
    }

    fn sequential_world(
        first: &InstanceId,
        second: &InstanceId,
        with_risk: bool,
        reason: InfrastructureTerminationReason,
    ) -> Result<World, Box<dyn Error>> {
        let policy = policy("alpha", 0, 4)?;
        let mut world = World::default();
        world.properties.push(properties(&policy, vec![]));
        world.policies.push(policy);
        world.supplies.push(FleetSupply {
            desired: 1,
            requested: Some(1),
            instances: vec![fleet_instance(second, InstanceLifecycle::InService, 2)],
            ..FleetSupply::default()
        });
        world.snapshot.workers = vec![worker_state(&worker_on(second)?, "alpha", 0, false, None)?];
        world.infrastructure = vec![
            launch("first-launch", 2, second, None)?,
            termination("first-termination", 3, first, reason)?,
        ];
        if with_risk {
            world.infrastructure.push(observation(
                "first-risk",
                1,
                InfrastructureEventKind::RebalanceRecommendation {
                    instance: first.clone(),
                },
            )?);
        }
        Ok(world)
    }

    fn begin_second_rebalance(
        world: &mut World,
        second: &InstanceId,
        third: &InstanceId,
        with_risk: bool,
    ) -> TestResult {
        if with_risk {
            world.infrastructure.extend([
                observation(
                    "second-risk",
                    5,
                    InfrastructureEventKind::RebalanceRecommendation {
                        instance: second.clone(),
                    },
                )?,
                observation(
                    "second-interruption",
                    6,
                    InfrastructureEventKind::SpotInterruption {
                        instance: second.clone(),
                        action: InfrastructureInterruptionAction::Terminate,
                    },
                )?,
            ]);
        }
        world
            .infrastructure
            .push(launch("second-launch", 7, third, None)?);
        world.supplies[0].desired = 2;
        world.supplies[0]
            .instances
            .push(fleet_instance(third, InstanceLifecycle::InService, 7));
        world
            .snapshot
            .workers
            .push(worker_state(&worker_on(third)?, "alpha", 0, false, None)?);
        Ok(())
    }

    fn assert_sequential_overlap(
        overlap: &State,
        second: &InstanceId,
        third: &InstanceId,
        with_risk: bool,
    ) -> TestResult {
        let old = find_worker(overlap, &worker_on(second)?)
            .and_then(|worker| worker.infrastructure.as_ref())
            .ok_or("missing replacing worker")?;
        assert_eq!(old.phase, InfrastructurePhase::Replacing);
        assert_eq!(
            old.reason,
            if with_risk {
                InfrastructureReason::SpotInterruption
            } else {
                InfrastructureReason::SpotRebalance
            }
        );
        assert_eq!(
            find_worker(overlap, &worker_on(third)?)
                .and_then(|worker| worker.infrastructure.as_ref())
                .map(|status| &status.phase),
            Some(&InfrastructurePhase::Replacement)
        );
        Ok(())
    }

    fn assert_restarted_overlap(
        world: &World,
        overlap: &State,
        second: &InstanceId,
        third: &InstanceId,
    ) -> TestResult {
        let mut restarted = Builder::new(config(), gang_state::CacheBaseline::default());
        let replayed = fill(&mut restarted, &blank(), 1, world, 8, ok(0))?;
        assert_eq!(replayed.status_events, overlap.status_events);
        for member in [second, third] {
            assert_eq!(
                find_worker(&replayed, &worker_on(member)?)
                    .and_then(|worker| worker.infrastructure.as_ref()),
                find_worker(overlap, &worker_on(member)?)
                    .and_then(|worker| worker.infrastructure.as_ref())
            );
        }
        Ok(())
    }

    fn assert_completed_history(
        finished: &State,
        world: &World,
        first: &InstanceId,
        second: &InstanceId,
        third: &InstanceId,
    ) -> TestResult {
        assert_pair(finished, "first-launch", first, second)?;
        assert_pair(finished, "second-launch", second, third)?;
        assert!(
            finished
                .workers
                .iter()
                .all(|worker| worker.infrastructure.is_none())
        );
        assert!(
            finished
                .instances
                .iter()
                .all(|row| row.infrastructure.is_none())
        );
        assert!(world.infrastructure.iter().all(|record| {
            !matches!(
                &record.event.kind,
                InfrastructureEventKind::ReplacementLaunched {
                    replacing: Some(_),
                    ..
                }
            )
        }));
        Ok(())
    }

    #[test]
    fn sequential_rebalances_preserve_pairs_and_current_roles_in_either_record_order() -> TestResult
    {
        let first = instance_id(1)?;
        let second = instance_id(2)?;
        let third = instance_id(3)?;
        for reverse in [false, true] {
            for with_risk in [false, true] {
                let reason = if with_risk {
                    InfrastructureTerminationReason::SpotReclaimed
                } else {
                    InfrastructureTerminationReason::Rebalance
                };
                let mut world = sequential_world(&first, &second, with_risk, reason.clone())?;
                if reverse {
                    world.infrastructure.reverse();
                }
                let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
                let completed = fill(&mut builder, &blank(), 1, &world, 4, ok(0))?;
                assert_pair(&completed, "first-launch", &first, &second)?;
                assert!(completed.workers[0].infrastructure.is_none());
                if reverse {
                    world.infrastructure.reverse();
                }
                begin_second_rebalance(&mut world, &second, &third, with_risk)?;
                if reverse {
                    world.infrastructure.reverse();
                }
                let overlap = fill(&mut builder, &completed, 2, &world, 8, ok(0))?;
                assert_pair(&overlap, "first-launch", &first, &second)?;
                assert_pair(&overlap, "second-launch", &second, &third)?;
                assert_sequential_overlap(&overlap, &second, &third, with_risk)?;
                assert_restarted_overlap(&world, &overlap, &second, &third)?;
                if reverse {
                    world.infrastructure.reverse();
                }
                world
                    .infrastructure
                    .push(termination("second-termination", 9, &second, reason)?);
                world.supplies[0].desired = 1;
                world.supplies[0].instances.remove(0);
                world.snapshot.workers.remove(0);
                if reverse {
                    world.infrastructure.reverse();
                }
                let finished = fill(&mut builder, &overlap, 3, &world, 10, ok(0))?;
                assert_completed_history(&finished, &world, &first, &second, &third)?;
            }
        }
        Ok(())
    }

    fn ambiguous_world() -> Result<World, Box<dyn Error>> {
        let mut pass = FleetPass::new(policy("alpha", 0, 8)?);
        pass.supply.desired = 5;
        pass.supply.requested = Some(4);
        pass.supply.instances = vec![
            fleet_instance(&instance_id(2)?, InstanceLifecycle::InService, 2),
            fleet_instance(&instance_id(3)?, InstanceLifecycle::InService, 2),
            fleet_instance(&instance_id(4)?, InstanceLifecycle::InService, 1),
            fleet_instance(&instance_id(5)?, InstanceLifecycle::InService, 1),
            fleet_instance(&instance_id(6)?, InstanceLifecycle::InService, 7),
        ];
        let mut world = World::default();
        world.properties.push(properties(&pass.policy, vec![]));
        world.passes.push(pass);
        world.infrastructure = vec![
            launch("prior-launch", 2, &instance_id(2)?, None)?,
            launch("other-prior-launch", 2, &instance_id(3)?, None)?,
            termination(
                "prior-termination",
                3,
                &instance_id(1)?,
                InfrastructureTerminationReason::Rebalance,
            )?,
            termination(
                "other-prior-termination",
                4,
                &instance_id(8)?,
                InfrastructureTerminationReason::Rebalance,
            )?,
            observation(
                "current-risk",
                6,
                InfrastructureEventKind::RebalanceRecommendation {
                    instance: instance_id(5)?,
                },
            )?,
            launch("current-launch", 7, &instance_id(6)?, None)?,
        ];
        Ok(world)
    }

    fn assert_uninvolved_members(frame: &State) -> TestResult {
        for member in [instance_id(2)?, instance_id(3)?, instance_id(4)?] {
            let row = frame
                .instances
                .iter()
                .find(|row| row.name == instance_name(&member))
                .ok_or("missing uninvolved member")?;
            assert!(row.infrastructure.is_none());
        }
        Ok(())
    }

    fn assert_initial_ambiguous_history(frame: &State) -> TestResult {
        assert_uninvolved_members(frame)?;
        for (member, phase) in [
            (instance_id(5)?, InfrastructurePhase::Replacing),
            (instance_id(6)?, InfrastructurePhase::Replacement),
        ] {
            let row = frame
                .instances
                .iter()
                .find(|row| row.name == instance_name(&member))
                .ok_or("missing current rebalance member")?;
            assert_eq!(
                row.infrastructure.as_ref().map(|status| &status.phase),
                Some(&phase)
            );
        }
        let current_old = instance_id(5)?;
        for row in &frame.status_events {
            if let EventChange::Aws { event } = &row.change {
                match event.id.as_str() {
                    "prior-launch" | "other-prior-launch" => assert!(matches!(
                        &event.kind,
                        AwsEventKind::ReplacementLaunched {
                            replacing: None,
                            ..
                        }
                    )),
                    "current-launch" => assert!(matches!(
                        &event.kind,
                        AwsEventKind::ReplacementLaunched { replacing: Some(peer), .. }
                            if peer == &instance_name(&current_old)
                    )),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn add_ambiguous_launch(world: &mut World) -> TestResult {
        let ambiguous_new = instance_id(7)?;
        world.passes[0].supply.instances.push(fleet_instance(
            &ambiguous_new,
            InstanceLifecycle::InService,
            7,
        ));
        world
            .infrastructure
            .push(launch("ambiguous-launch", 7, &ambiguous_new, None)?);
        Ok(())
    }

    fn extend_ambiguous_overlap(world: &mut World) -> TestResult {
        let current_old = instance_id(5)?;
        let later_new = instance_id(9)?;
        world.passes[0]
            .supply
            .instances
            .retain(|member| member.id != current_old);
        world.passes[0].supply.instances.push(fleet_instance(
            &later_new,
            InstanceLifecycle::InService,
            10,
        ));
        world.infrastructure.extend([
            observation(
                "later-risk",
                8,
                InfrastructureEventKind::RebalanceRecommendation {
                    instance: instance_id(4)?,
                },
            )?,
            termination(
                "current-termination",
                9,
                &current_old,
                InfrastructureTerminationReason::Rebalance,
            )?,
            launch("later-launch", 10, &later_new, None)?,
        ]);
        Ok(())
    }

    fn add_source_pair(world: &mut World) -> TestResult {
        let known_new = instance_id(10)?;
        world.passes[0].supply.instances.push(fleet_instance(
            &known_new,
            InstanceLifecycle::InService,
            11,
        ));
        world.infrastructure.push(launch(
            "known-launch",
            11,
            &known_new,
            Some(instance_id(4)?),
        )?);
        Ok(())
    }

    fn assert_unpaired_except(frame: &State, except: Option<&str>) {
        assert!(frame.status_events.iter().all(|row| {
            !matches!(&row.change, EventChange::Aws { event }
                if Some(event.id.as_str()) != except
                    && matches!(&event.kind, AwsEventKind::ReplacementLaunched { replacing: Some(_), .. }))
        }));
    }

    #[test]
    fn ambiguous_completed_launches_and_bystanders_do_not_inherit_later_overlap() -> TestResult {
        let current_old = instance_id(5)?;
        for reverse in [false, true] {
            let mut world = ambiguous_world()?;
            if reverse {
                world.infrastructure.reverse();
            }
            let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
            let frame = fill(&mut builder, &blank(), 1, &world, 8, ok(0))?;
            assert_initial_ambiguous_history(&frame)?;
            if reverse {
                world.infrastructure.reverse();
            }
            // A second contemporaneous launch removes the inferred pair, not the risk notice.
            add_ambiguous_launch(&mut world)?;
            if reverse {
                world.infrastructure.reverse();
            }
            let ambiguous = fill(&mut builder, &frame, 2, &world, 8, ok(0))?;
            assert_unpaired_except(&ambiguous, None);
            assert_uninvolved_members(&ambiguous)?;
            assert_eq!(
                ambiguous
                    .instances
                    .iter()
                    .find(|row| row.name == instance_name(&current_old))
                    .and_then(|row| row.infrastructure.as_ref())
                    .map(|status| &status.phase),
                Some(&InfrastructurePhase::Replacing)
            );
            if reverse {
                world.infrastructure.reverse();
            }
            // One termination cannot finish two launches or let a third steal an old member.
            extend_ambiguous_overlap(&mut world)?;
            if reverse {
                world.infrastructure.reverse();
            }
            let overlapping = fill(&mut builder, &ambiguous, 3, &world, 11, ok(0))?;
            assert_unpaired_except(&overlapping, None);
            if reverse {
                world.infrastructure.reverse();
            }
            // Source-provided pairing remains authoritative even in an ambiguous episode.
            add_source_pair(&mut world)?;
            if reverse {
                world.infrastructure.reverse();
            }
            let known = fill(&mut builder, &overlapping, 4, &world, 12, ok(0))?;
            assert_pair(&known, "known-launch", &instance_id(4)?, &instance_id(10)?)?;
            assert_unpaired_except(&known, Some("known-launch"));
        }
        Ok(())
    }

    fn assert_healthy_worker_is_authoritative(
        builder: &mut Builder,
        connected: &State,
        world: &World,
    ) -> TestResult {
        // A matching healthy worker remains authoritative even if another generation
        // on the same physical instance has an old infrastructure status.
        let mut mixed = connected.clone();
        mixed.workers[0].infrastructure = None;
        let mut other = connected.workers[0].clone();
        other.name.push('z');
        mixed.workers.push(other);
        mixed.workers.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let healthy = fill(
            builder,
            &mixed,
            3,
            world,
            12,
            super::Outcome::Observed {
                known: true,
                supplies: vec![true],
            },
        )?;
        assert!(healthy.workers[0].infrastructure.is_none());
        Ok(())
    }

    fn assert_healthy_instance_is_authoritative(
        builder: &mut Builder,
        disconnected: &State,
        rotated: &State,
        world: &World,
        new: &InstanceId,
    ) -> TestResult {
        // An existing healthy instance is authoritative over a stale worker row.
        let mut mixed = disconnected.clone();
        mixed
            .instances
            .iter_mut()
            .find(|row| row.name == instance_name(new))
            .ok_or("healthy instance")?
            .infrastructure = None;
        mixed.workers.push(rotated.workers[0].clone());
        let healthy = fill(
            builder,
            &mixed,
            5,
            world,
            14,
            super::Outcome::Observed {
                known: true,
                supplies: vec![true],
            },
        )?;
        assert!(
            healthy
                .instances
                .iter()
                .find(|row| row.name == instance_name(new))
                .ok_or("healthy instance")?
                .infrastructure
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn replacement_status_follows_physical_instance_across_worker_generations() -> TestResult {
        let old = instance_id(1)?;
        let new = instance_id(2)?;
        let mut world =
            sequential_world(&old, &new, true, InfrastructureTerminationReason::Rebalance)?;
        world.snapshot.workers.clear();
        world.supplies[0]
            .instances
            .push(fleet_instance(&old, InstanceLifecycle::InService, 0));
        world.infrastructure = vec![launch("replacement", 2, &new, Some(old.clone()))?];
        let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
        let instance = fill(&mut builder, &blank(), 1, &world, 10, ok(0))?;
        let status = instance
            .instances
            .iter()
            .find(|row| row.name == instance_name(&new))
            .and_then(|row| row.infrastructure.clone())
            .ok_or("replacement status")?;
        let first_id = worker_on(&new)?;
        world
            .snapshot
            .workers
            .push(worker_state(&first_id, "alpha", 0, false, None)?);
        let connected = fill(
            &mut builder,
            &instance,
            2,
            &world,
            11,
            super::Outcome::Observed {
                known: true,
                supplies: vec![true],
            },
        )?;
        assert_eq!(connected.workers[0].infrastructure.as_ref(), Some(&status));
        assert_healthy_worker_is_authoritative(&mut builder, &connected, &world)?;
        let outage = fill(
            &mut builder,
            &connected,
            3,
            &world,
            12,
            super::Outcome::Observed {
                known: false,
                supplies: vec![true],
            },
        )?;
        assert!(
            outage
                .instances
                .iter()
                .all(|row| row.name != instance_name(&new))
        );
        assert_eq!(outage.workers[0].infrastructure.as_ref(), Some(&status));
        let second_id = gang_types::WorkerId::try_new(format!(
            "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
            new.worker_name()
        ))?;
        world.snapshot.workers[0] = worker_state(&second_id, "alpha", 0, false, None)?;
        let rotated = fill(
            &mut builder,
            &connected,
            3,
            &world,
            12,
            super::Outcome::Observed {
                known: true,
                supplies: vec![true],
            },
        )?;
        assert_eq!(rotated.workers[0].infrastructure.as_ref(), Some(&status));
        world.snapshot.workers.clear();
        let disconnected = fill(
            &mut builder,
            &rotated,
            4,
            &world,
            13,
            super::Outcome::Observed {
                known: true,
                supplies: vec![true],
            },
        )?;
        let row = disconnected
            .instances
            .iter()
            .find(|row| row.name == instance_name(&new))
            .ok_or("disconnected instance")?;
        assert_eq!(row.infrastructure.as_ref(), Some(&status));
        assert_healthy_instance_is_authoritative(
            &mut builder,
            &disconnected,
            &rotated,
            &world,
            &new,
        )?;
        Ok(())
    }
}

#[test]
fn unobserved_step_is_promoted_by_first_healthy_observation() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World::default();
    world.snapshot.queued.push(queued(0, FleetSet::default()));
    let initial = builder.initial(ts(0), 7);
    let pending = fill(
        &mut builder,
        &initial,
        1,
        &world,
        1_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(pending.scheduler_known, None);
    assert_eq!(
        pending
            .recent
            .samples
            .last()
            .ok_or("sample")?
            .scheduler_known,
        Some(false)
    );
    let healthy = fill(
        &mut builder,
        &pending,
        2,
        &world,
        1_001,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    assert_eq!(healthy.scheduler_known, Some(true));
    assert_eq!(
        healthy
            .recent
            .samples
            .last()
            .ok_or("sample")?
            .scheduler_known,
        Some(true)
    );
    Ok(())
}

#[test]
fn scheduler_current_recovers_while_step_completeness_remains_sticky() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let mut world = World::default();
    world.snapshot.queued.push(queued(0, FleetSet::default()));
    let initial = builder.initial(ts(0), 7);
    let healthy = fill(
        &mut builder,
        &initial,
        2,
        &world,
        1_001,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    let failed = fill(
        &mut builder,
        &healthy,
        3,
        &world,
        1_002,
        Outcome::Observed {
            known: false,
            supplies: vec![],
        },
    )?;
    let recovered = fill(
        &mut builder,
        &failed,
        4,
        &world,
        1_003,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    assert_eq!(recovered.scheduler_known, Some(true));
    assert_eq!(
        recovered
            .recent
            .samples
            .last()
            .ok_or("sample")?
            .scheduler_known,
        Some(false)
    );
    assert_eq!(recovered.recent.samples.last().ok_or("sample")?.queued, 1);
    let again = fill(
        &mut builder,
        &recovered,
        5,
        &world,
        1_004,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    assert_eq!(
        again.recent.samples.last().ok_or("sample")?.scheduler_known,
        Some(false)
    );
    for (a, b) in [(&healthy, &failed), (&failed, &recovered)] {
        let delta = diff(a, b).ok_or("delta")?;
        let mut patched = a.clone();
        gang_state::apply(&mut patched, &delta)?;
        assert_eq!(&patched, b);
    }
    Ok(())
}

#[test]
fn metadata_in_outage_opens_unknown_step_until_rebaselined() -> TestResult {
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let world = World::default();
    let initial = builder.initial(ts(0), 7);
    let outage = fill(
        &mut builder,
        &initial,
        6,
        &world,
        11_000,
        Outcome::Observed {
            known: false,
            supplies: vec![],
        },
    )?;
    let metadata = fill(
        &mut builder,
        &outage,
        7,
        &world,
        21_000,
        Outcome::Skipped(SkipReason::SchedulerUnavailable),
    )?;
    assert_eq!(metadata.scheduler_known, Some(false));
    assert_eq!(
        metadata
            .recent
            .samples
            .last()
            .ok_or("sample")?
            .scheduler_known,
        Some(false)
    );
    let restored = fill(
        &mut builder,
        &metadata,
        8,
        &world,
        21_001,
        Outcome::Observed {
            known: true,
            supplies: vec![],
        },
    )?;
    assert_eq!(restored.scheduler_known, Some(true));
    assert_eq!(
        restored
            .recent
            .samples
            .last()
            .ok_or("sample")?
            .scheduler_known,
        Some(false)
    );
    for (a, b) in [(&outage, &metadata), (&metadata, &restored)] {
        let delta = diff(a, b).ok_or("delta")?;
        let mut patched = a.clone();
        gang_state::apply(&mut patched, &delta)?;
        assert_eq!(&patched, b);
    }
    Ok(())
}

#[test]
fn observer_keeps_control_grace_and_filters_hosted_from_failed_supply() -> TestResult {
    let mut world = World::default();
    world.policies = vec![policy("alpha", 100, 4)?, policy("beta", 100, 4)?];
    world.properties = world
        .policies
        .iter()
        .map(|p| properties(p, Vec::new()))
        .collect();
    let id = instance_id(1)?;
    world.supplies = vec![FleetSupply::default(), FleetSupply::default()];
    world.supplies[1]
        .instances
        .push(fleet_instance(&id, InstanceLifecycle::InService, 0));
    world.past_boot_grace = vec![vec![], vec![true]];
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let control = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    assert_eq!(past_grace(&control, &id), Some(true));
    world.past_boot_grace.clear();
    let observed = fill(
        &mut builder,
        &control,
        2,
        &world,
        1_001,
        Outcome::Observed {
            known: true,
            supplies: vec![true, true],
        },
    )?;
    assert_eq!(past_grace(&observed, &id), Some(true));
    world.snapshot.workers.push(worker_state(
        &worker_on(&id)?,
        "beta",
        0,
        false,
        NonZeroU32::new(4),
    )?);
    let connected = fill(
        &mut builder,
        &observed,
        3,
        &world,
        1_002,
        Outcome::Observed {
            known: true,
            supplies: vec![true, false],
        },
    )?;
    assert_eq!(past_grace(&connected, &id), None);
    assert_eq!(connected.fleets[1].workers.unconnected, 0);
    assert_eq!(connected.fleets[1].instances, observed.fleets[1].instances);
    assert_eq!(connected.workers.len(), 1);
    Ok(())
}

#[test]
fn observer_preserves_reconciler_spill_attribution() -> TestResult {
    let mut world = World {
        policies: vec![policy("alpha", 0, 2)?, policy("beta", 0, 10)?],
        ..World::default()
    };
    world.properties = world
        .policies
        .iter()
        .map(|p| properties(p, Vec::new()))
        .collect();
    world.supplies = vec![FleetSupply::default(), FleetSupply::default()];
    let candidates = FleetSet::prefix(2)?;
    let cpu_per_instance = u64::from(world.policies[0].vcpus_per_instance.get()) * 1_000;
    // Each action reserves one whole instance with CPU oversubscription disabled below.
    world.snapshot.queued = (0..3)
        .map(|i| {
            let mut action = queued(i, candidates);
            action.resources.cpu_millicores = cpu_per_instance;
            action
        })
        .collect();
    let mut passes = world.passes();
    let scaling = Arc::new(ScalingConfig::default());
    let profiles = SourceProfiles::new(&scaling, ActionsConfig::default().history_window);
    let mut reconciler = Reconciler::new(
        &world.policies,
        scaling,
        profiles,
        gang_types::nonzero_u32(1),
    );
    let mut hosted = HostedSnapshot::default();
    hosted.replace(&mut world.snapshot.clone());
    let clock = TestClock::new(ts(1_000));
    reconciler.reconcile(
        &mut passes,
        &hosted,
        PassTime::read(&clock),
        &mut Plan::new(),
    );
    assert_eq!((passes[0].outlook.queued, passes[1].outlook.queued), (2, 1));
    world.passes = passes;
    let mut builder = Builder::new(config(), gang_state::CacheBaseline::default());
    let control = fill(&mut builder, &blank(), 1, &world, 1_000, ok(0))?;
    let observed = fill(
        &mut builder,
        &control,
        2,
        &world,
        1_001,
        Outcome::Observed {
            known: true,
            supplies: vec![true, true],
        },
    )?;
    assert_eq!(
        (
            observed.fleets[0].demand.queued,
            observed.fleets[1].demand.queued
        ),
        (2, 1)
    );
    assert_eq!((observed.queue.total, observed.queue.unplaceable), (3, 0));
    Ok(())
}
