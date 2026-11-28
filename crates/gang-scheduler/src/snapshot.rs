//! Direct domain-to-[`SchedulerSnapshot`] projection (`§5.9`).
//!
//! Reads directly from [`Operations`], [`OperationQueue`], [`WorkerRegistry`], [`CordonRegistry`],
//! [`Attempts`], [`CapacityCatalog`], [`ActionDurations`], and [`SourceRates`] into caller-reused
//! buffers without decoding properties or performing secondary lookups.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::{ExecutingDemand, QueuedDemand, SchedulerSnapshot, WorkerState};
use gang_types::{Mnemonic, TargetLabel};

use crate::attempts::{AttemptPhase, Attempts};
use crate::capacity::CapacityCatalog;
use crate::cordons::CordonRegistry;
use crate::durations::ActionDurations;
use crate::operations::{Now, Operation, OperationStage, Operations};
use crate::queue::OperationQueue;
use crate::rates::SourceRates;
use crate::workers::WorkerRegistry;

/// Projects current scheduler domain state into `into`, clearing and reusing its buffers (`§5.9`).
///
/// - `workers` are ordered by [`WorkerId`](gang_types::WorkerId) ascending.
/// - `queued` follows [`OperationQueue::global_iter`] (`(Reverse(priority), seq, OperationKey)`).
/// - `executing` is ordered by operation name ascending and indexes into `into.workers`.
/// - `stores` is cleared because the pure scheduler owns no store metrics (the controller shell
///   populates `stores` separately).
#[allow(
    clippy::too_many_arguments,
    reason = "pure snapshot projection reads explicit domain tables without hidden state"
)]
pub fn fill_snapshot(
    now: Now,
    operations: &Operations,
    queue: &OperationQueue,
    workers: &WorkerRegistry,
    cordons: &CordonRegistry,
    attempts: &Attempts,
    capacity: &CapacityCatalog,
    durations: &mut ActionDurations,
    rates: &SourceRates,
    into: &mut SchedulerSnapshot,
) {
    into.completed = attempts.completed_count();
    into.source_rates_at = now.tick;
    rates.snapshot_into(&mut into.source_rates);
    cordons.snapshot_into(&mut into.cordons);
    capacity.fleet_capacity(&mut into.fleet_capacity);

    into.workers.clear();
    for (_, worker) in workers.workers_sorted() {
        into.workers.push(WorkerState {
            id: worker.id.clone(),
            fleet: worker.fleet_name.clone(),
            environment: worker.environment.clone(),
            current_build: worker.is_current_build(),
            running_actions: worker.inflight_attempts,
            max_inflight_actions: worker.max_inflight,
            capacity: Some(worker.ledger.capacity()),
            reserved: Some(worker.ledger.reserved()),
            cache: worker.cache_stats,
            draining: worker.drain.is_draining(),
            paused: worker.drain.is_paused() || worker.has_disk_hold(),
            last_seen: worker.last_seen.wall,
        });
    }

    into.queued.clear();
    for entry in queue.global_iter() {
        let Some(op) = operations.get(entry.key) else {
            continue;
        };
        if op.stage != OperationStage::Queued {
            continue;
        }
        into.queued.push(QueuedDemand {
            source: op.source,
            candidates: op.candidates,
            resources: op.reservation,
            queued_since: op.queued_since.wall,
            priority: op.priority,
            expected: expected_duration(op, durations, now.wall),
        });
    }

    into.executing.clear();
    let mut active_rows = Vec::new();
    for (op_key, attempt_key) in attempts.active_operations() {
        let Some(attempt) = attempts.get(attempt_key) else {
            continue;
        };
        if attempt.phase == AttemptPhase::Executed {
            continue;
        }
        let Some(op) = operations.get(op_key) else {
            continue;
        };
        if op.stage != OperationStage::Running {
            continue;
        }
        let Some(worker) = workers.get(attempt.worker) else {
            continue;
        };
        let Ok(worker_index) = into
            .workers
            .binary_search_by(|state| state.id.cmp(&worker.id))
        else {
            continue;
        };
        let started = attempt.accepted_at.unwrap_or(attempt.dispatched_at).wall;
        active_rows.push((op.name.as_str(), attempt, op, worker_index, started));
    }
    active_rows.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (_, attempt, op, worker_index, started) in active_rows {
        into.executing.push(ExecutingDemand {
            source: op.source,
            candidates: op.candidates,
            resources: attempt.reservation,
            worker: worker_index,
            started,
            expected: expected_duration(op, durations, now.wall),
        });
    }

    into.stores.clear();
}

/// Allocates and returns a fresh [`SchedulerSnapshot`] projected from the domain tables (`§5.9`).
#[allow(
    clippy::too_many_arguments,
    reason = "pure snapshot projection reads explicit domain tables without hidden state"
)]
#[must_use]
pub fn project_snapshot(
    now: Now,
    operations: &Operations,
    queue: &OperationQueue,
    workers: &WorkerRegistry,
    cordons: &CordonRegistry,
    attempts: &Attempts,
    capacity: &CapacityCatalog,
    durations: &mut ActionDurations,
    rates: &SourceRates,
) -> SchedulerSnapshot {
    let mut snapshot = SchedulerSnapshot::default();
    fill_snapshot(
        now,
        operations,
        queue,
        workers,
        cordons,
        attempts,
        capacity,
        durations,
        rates,
        &mut snapshot,
    );
    snapshot
}

fn expected_duration(
    op: &Operation,
    durations: &mut ActionDurations,
    now: Timestamp,
) -> Option<Duration> {
    let target = op.target.as_ref().map_or("", TargetLabel::as_str);
    let mnemonic = op.mnemonic.as_ref().map_or("", Mnemonic::as_str);
    durations.expected(target, mnemonic, now)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_config::{HistoryWindow, ScalingConfig};
    use gang_spi::WorkerCacheStats;
    use gang_types::{
        ActionResources, AttemptId, Bytes, Digest, FleetActionPolicy, FleetCatalog, FleetName,
        FleetProperties, FleetSet, InstanceId, Isa, Mnemonic, OperationName, Resources, SourceId,
        TargetLabel,
    };

    use super::*;
    use crate::operations::{AdmitOutcome, NewOperation, canonical_platform_key};
    use crate::placement::place_operation;
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn catalog() -> FleetCatalog {
        let fleet = |name: &str, mem_gib: u64| {
            FleetProperties::new(
                &FleetName::try_new(name).expect("fleet"),
                Isa::X86_64,
                &"bwrap",
                &"host:test".parse().expect("env"),
                Resources {
                    vcpus: 8,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(100),
                },
                &FleetActionPolicy::default(),
                std::iter::empty(),
            )
        };
        FleetCatalog::try_new(vec![fleet("alpha", 32), fleet("beta", 16)]).expect("catalog")
    }

    fn register_worker(
        workers: &mut WorkerRegistry,
        fleet: &str,
        instance: &str,
        seed: u8,
        mem_gib: u64,
    ) -> crate::workers::WorkerKey {
        workers
            .register(
                now(1),
                "0.3.9",
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
                    fleet: FleetName::try_new(fleet).expect("fleet"),
                    gang_version: "0.3.9".to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 16_000,
                        memory: Bytes::gib(mem_gib),
                        disk: Bytes::gib(100),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 8_000,
                        memory: Bytes::gib(mem_gib),
                        disk: Bytes::gib(100),
                    },
                    max_inflight: Some(NonZeroU32::new(4).expect("4")),
                    single_use: false,
                    environment: Some("host:test".parse().expect("env")),
                    uuid_bytes: [seed; 16],
                },
            )
            .expect("register")
    }

    fn admit_op(
        ops: &mut Operations,
        queue: &mut OperationQueue,
        name: &str,
        priority: i32,
        at: Now,
    ) -> crate::operations::OperationKey {
        let AdmitOutcome::Inserted { key, .. } = ops.admit(
            NewOperation {
                name: OperationName::try_new(name).expect("op"),
                action_digest: Digest::from_bytes(name.as_bytes()),
                platform_key: canonical_platform_key([("ISA", "x86-64")]),
                skip_cache_lookup: false,
                do_not_cache: false,
                source: SourceId::from_identity_str("local:alice"),
                build: None,
                target: Some(TargetLabel::try_new("//app:bin").expect("target")),
                mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
                candidates: FleetSet::try_from_indices([0, 1]).expect("fleets"),
                priority,
                has_explicit_cpu_floor: false,
                reservation: ActionResources {
                    cpu_millicores: 2_000,
                    memory: Bytes::gib(2),
                    disk: Bytes::gib(1),
                },
                learned_reservation: None,
                initial_watchers: 1,
            },
            at,
            queue,
        ) else {
            panic!("insert");
        };
        key
    }

    fn seed_history(durations: &mut ActionDurations, rates: &mut SourceRates) {
        durations.record("//app:bin", "Rustc", 1_500, now(2).wall);
        let alice = SourceId::from_identity_str("local:alice");
        rates.record(
            alice,
            0,
            ActionResources {
                cpu_millicores: 2_000,
                memory: Bytes::gib(2),
                disk: Bytes::gib(1),
            },
            Some(Duration::from_millis(1_500)),
            now(2).tick,
        );
    }

    #[test]
    fn projects_workers_cordons_demand_rates_memory_and_reuses_buffers() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let mut cordons = CordonRegistry::from_catalog(&cat).expect("cordons");
        let mut capacity = CapacityCatalog::new(cat.clone());
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(cat.len()).expect("queue");
        let mut attempts = Attempts::new();
        let mut durations = ActionDurations::new(
            NonZeroUsize::new(16).expect("16"),
            HistoryWindow::from_units(1),
        );
        let mut rates = SourceRates::new(&ScalingConfig::default(), cat.len()).expect("rates");

        // Register two workers out of WorkerId order (`i-z` before `i-a`).
        let w_z = register_worker(&mut workers, "alpha", "i-z", 2, 30);
        let w_a = register_worker(&mut workers, "alpha", "i-a", 1, 32);
        workers.set_paused(w_z, true);
        workers.on_keepalive(
            w_a,
            now(5),
            Some(WorkerCacheStats {
                local_read_requests: 7,
                ..WorkerCacheStats::default()
            }),
            None,
        );
        capacity.refresh_local(&workers);

        let alpha = FleetName::try_new("alpha").expect("alpha");
        let inst_z = InstanceId::try_new("i-z").expect("i-z");
        cordons
            .set_draining(&alpha, &inst_z, true, &mut workers)
            .expect("cordon");
        seed_history(&mut durations, &mut rates);

        let op_exec = admit_op(&mut ops, &mut queue, "operations/exec", 100, now(3));
        let placed = place_operation(op_exec, &mut ops, &mut queue, &mut workers).expect("placed");
        assert_eq!(placed.worker, w_a);
        attempts
            .dispatch(
                placed,
                AttemptId::from_random_bytes([9; 16]),
                now(3),
                Duration::from_secs(10),
                Duration::from_mins(5),
            )
            .expect("dispatch");
        attempts.on_accept(&AttemptId::from_random_bytes([9; 16]), now(4), &ops);

        let _op_low = admit_op(&mut ops, &mut queue, "operations/q-low", 10, now(4));
        let _op_gate = admit_op(&mut ops, &mut queue, "operations/q-gate", 250, now(5));

        let snap = project_snapshot(
            now(6),
            &ops,
            &queue,
            &workers,
            &cordons,
            &attempts,
            &capacity,
            &mut durations,
            &rates,
        );

        assert_eq!(snap.source_rates_at, now(6).tick);
        assert_eq!(snap.source_rates.len(), 1);
        assert_eq!(snap.cordons.len(), 1);
        assert_eq!(snap.cordons[0].instance.as_ref(), &inst_z);
        assert_eq!(snap.fleet_capacity[0].capacity.memory, Bytes::gib(30));
        assert_eq!(snap.workers.len(), 2);
        assert!(snap.workers[0].id < snap.workers[1].id);
        assert_eq!(snap.workers[0].running_actions, 1);
        assert!(!snap.workers[0].draining);
        assert_eq!(snap.workers[0].cache.expect("cache").local_read_requests, 7);
        assert_eq!(snap.workers[1].running_actions, 0);
        assert!(snap.workers[1].draining);
        assert!(snap.workers[1].paused);

        assert_eq!(snap.queued.len(), 2);
        assert_eq!(
            (snap.queued[0].priority, snap.queued[1].priority),
            (250, 10)
        );
        assert_eq!(snap.queued[0].expected, Some(Duration::from_millis(1_500)));

        assert_eq!(snap.executing.len(), 1);
        assert_eq!(snap.executing[0].worker, 0);
        assert_eq!(snap.executing[0].started, now(4).wall);
        assert_eq!(
            snap.executing[0].expected,
            Some(Duration::from_millis(1_500))
        );

        let mut reused = snap;
        fill_snapshot(
            now(7),
            &ops,
            &queue,
            &workers,
            &cordons,
            &attempts,
            &capacity,
            &mut durations,
            &rates,
            &mut reused,
        );
        assert_eq!(reused.source_rates_at, now(7).tick);
        assert_eq!(reused.workers.len(), 2);
    }
}
