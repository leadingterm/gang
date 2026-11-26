//! Distributed operation-driven and worker-driven placement (`ADR-60` / §5.4).
//!
//! No packing or placement configuration knob exists:
//! - **Operation-driven:** selects the highest-preference candidate fleet with an eligible worker;
//!   within that fleet picks the worker with the lowest reserved CPU fraction (exact integer
//!   cross-product), then fewest running gate-tier attempts, then least recently assigned, then
//!   ascending `WorkerId`.
//! - **Worker-driven:** walks `queue[fleet]` in `(Reverse(priority), seq, OperationKey)` order and
//!   assigns fitting operations until the worker is full or `dispatch_scan_limit` non-fitting
//!   entries have been skipped.

use std::cmp::Ordering;

use gang_types::ActionResources;

use crate::ledger::WorkerLedger;
use crate::operations::{Operation, OperationKey, OperationStage, Operations};
use crate::qos;
use crate::queue::OperationQueue;
use crate::workers::{Worker, WorkerKey, WorkerRegistry};

/// A committed assignment emitted by operation-driven or worker-driven placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacedAssignment {
    /// Key of the operation moved to [`OperationStage::Running`].
    pub operation: OperationKey,
    /// Key of the worker whose ledger now holds the reservation.
    pub worker: WorkerKey,
    /// Resource reservation held for this attempt.
    pub reservation: ActionResources,
    /// Priority of the operation at dispatch time.
    pub priority: i32,
    /// Out-of-band `cgroup` v2 `cpu.weight` for this attempt.
    pub cpu_weight: gang_types::CpuWeight,
}

/// Returns `true` when `worker` is eligible to execute `op` right now.
#[must_use]
pub fn is_eligible(worker: &Worker, op: &Operation) -> bool {
    is_eligible_with_state(worker, worker.ledger, worker.inflight_attempts, op)
}

const fn is_eligible_with_state(
    worker: &Worker,
    ledger: WorkerLedger,
    inflight_attempts: u32,
    op: &Operation,
) -> bool {
    if worker.drain.blocks_assignment()
        || !worker.is_current_build()
        || worker.has_disk_hold()
        || worker.is_single_use_exhausted()
        || !matches!(op.stage, OperationStage::Queued)
        || !op.candidates.contains(worker.fleet)
    {
        return false;
    }
    if worker.is_single_use() && inflight_attempts > 0 {
        return false;
    }
    if let Some(max) = worker.max_inflight
        && inflight_attempts >= max.get()
    {
        return false;
    }
    ledger.fits(op.reservation)
}

/// Ranks two eligible workers within the same candidate fleet (smaller = more preferred).
///
/// Order:
/// 1. Lowest reserved CPU fraction (exact `u128` cross-product)
/// 2. Fewest running gate-tier attempts
/// 3. Least recently assigned (`last_assigned_seq`)
/// 4. Ascending `WorkerId`
#[must_use]
pub fn compare_workers(left: &Worker, right: &Worker) -> Ordering {
    left.ledger
        .cmp_cpu_fraction(&right.ledger)
        .then_with(|| left.gate_running.cmp(&right.gate_running))
        .then_with(|| left.last_assigned_seq.cmp(&right.last_assigned_seq))
        .then_with(|| left.id.cmp(&right.id))
}

/// Selects the best eligible worker for `op` across its candidate fleets in preference order.
#[must_use]
pub fn select_worker_for_operation(op: &Operation, workers: &WorkerRegistry) -> Option<WorkerKey> {
    if !matches!(op.stage, OperationStage::Queued) {
        return None;
    }
    for fleet in op.candidates {
        let mut best: Option<(WorkerKey, &Worker)> = None;
        for &worker_key in workers.fleet_workers(fleet) {
            let Some(worker) = workers.get(worker_key) else {
                continue;
            };
            if !is_eligible(worker, op) {
                continue;
            }
            match best {
                None => best = Some((worker_key, worker)),
                Some((_, current_best)) => {
                    if compare_workers(worker, current_best) == Ordering::Less {
                        best = Some((worker_key, worker));
                    }
                }
            }
        }
        if let Some((winner, _)) = best {
            return Some(winner);
        }
    }
    None
}

/// Attempts to place a single queued operation (`op_key`) onto its best eligible worker.
///
/// On success, reserves on the worker's ledger, removes `op_key` from all queue indices, marks the
/// operation [`OperationStage::Running`], and returns the [`PlacedAssignment`].
pub fn place_operation(
    op_key: OperationKey,
    operations: &mut Operations,
    queue: &mut OperationQueue,
    workers: &mut WorkerRegistry,
) -> Option<PlacedAssignment> {
    let op = operations.get(op_key)?;
    let worker_key = select_worker_for_operation(op, workers)?;
    let reservation = op.reservation;
    let priority = op.priority;
    let cpu_weight = op.cpu_weight;
    let is_gate = qos::is_gate(priority);

    if workers
        .commit_assignment(worker_key, reservation, is_gate)
        .is_err()
    {
        return None;
    }
    operations.mark_running(op_key, queue);

    Some(PlacedAssignment {
        operation: op_key,
        worker: worker_key,
        reservation,
        priority,
        cpu_weight,
    })
}

/// Scans `queue[worker.fleet]` in priority/sequence order and assigns fitting operations to
/// `worker_key` until `worker_key` is full or `scan_limit` non-fitting entries are skipped.
///
/// Reuses `out` (clearing it first) to avoid heap allocations in steady state.
pub fn scan_queue_for_worker(
    worker_key: WorkerKey,
    scan_limit: usize,
    operations: &mut Operations,
    queue: &mut OperationQueue,
    workers: &mut WorkerRegistry,
    out: &mut Vec<PlacedAssignment>,
) {
    out.clear();
    let Some(worker) = workers.get(worker_key) else {
        return;
    };
    if !worker.is_available_for_dispatch() {
        return;
    }

    let fleet = worker.fleet;
    let mut sim_ledger = worker.ledger;
    let mut sim_inflight = worker.inflight_attempts;
    let mut skipped = 0_usize;

    {
        let Some(fleet_entries) = queue.fleet_iter(fleet) else {
            return;
        };
        for queue_entry in fleet_entries {
            let Some(op) = operations.get(queue_entry.key) else {
                continue;
            };
            if !is_eligible_with_state(worker, sim_ledger, sim_inflight, op) {
                skipped = skipped.saturating_add(1);
                if skipped >= scan_limit {
                    break;
                }
                continue;
            }
            if sim_ledger.try_reserve(op.reservation).is_err() {
                skipped = skipped.saturating_add(1);
                if skipped >= scan_limit {
                    break;
                }
                continue;
            }
            sim_inflight = sim_inflight.saturating_add(1);
            out.push(PlacedAssignment {
                operation: queue_entry.key,
                worker: worker_key,
                reservation: op.reservation,
                priority: op.priority,
                cpu_weight: op.cpu_weight,
            });

            let slot_full = worker.is_single_use()
                || worker
                    .max_inflight
                    .is_some_and(|max| sim_inflight >= max.get())
                || sim_ledger.is_completely_full();
            if slot_full {
                break;
            }
        }
    }

    for assignment in out.iter() {
        let is_gate = qos::is_gate(assignment.priority);
        if workers
            .commit_assignment(assignment.worker, assignment.reservation, is_gate)
            .is_ok()
        {
            operations.mark_running(assignment.operation, queue);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::{
        ActionResources, Bytes, Digest, FleetActionPolicy, FleetCatalog, FleetName,
        FleetProperties, FleetSet, InstanceId, Isa, OperationName, Resources,
    };

    use super::*;
    use crate::operations::{AdmitOutcome, NewOperation, Now, canonical_platform_key};
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn fleet_props(name: &str) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet name"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(32),
                disk: Bytes::gib(200),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )
    }

    fn two_fleet_catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet_props("preferred"), fleet_props("fallback")])
            .expect("catalog")
    }

    fn register_worker(
        reg: &mut WorkerRegistry,
        fleet: &str,
        instance: &str,
        seed: u8,
        cpu_millicores: u64,
        mem_gib: u64,
    ) -> WorkerKey {
        reg.register(
            now(1),
            "0.3.9",
            false,
            WorkerRegistration {
                instance: Arc::new(InstanceId::try_new(instance).expect("instance")),
                fleet: FleetName::try_new(fleet).expect("fleet"),
                gang_version: "0.3.9".to_owned(),
                properties: BTreeMap::new(),
                capacity: ActionResources {
                    cpu_millicores,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(100),
                },
                ceilings: ActionResources {
                    cpu_millicores,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(100),
                },
                max_inflight: None,
                single_use: false,
                environment: None,
                uuid_bytes: [seed; 16],
            },
        )
        .expect("register worker")
    }

    fn admit_op(
        ops: &mut Operations,
        queue: &mut OperationQueue,
        name: &str,
        candidates: &[usize],
        priority: i32,
        cpu_millicores: u64,
        mem_gib: u64,
    ) -> OperationKey {
        let AdmitOutcome::Inserted { key, .. } = ops.admit(
            NewOperation {
                name: OperationName::try_new(name).expect("op name"),
                action_digest: Digest::from_bytes(name.as_bytes()),
                platform_key: canonical_platform_key([("ISA", "x86-64")]),
                skip_cache_lookup: false,
                do_not_cache: false,
                source: None,
                build: None,
                target: None,
                mnemonic: None,
                candidates: FleetSet::try_from_indices(candidates.iter().copied())
                    .expect("candidates"),
                priority,
                reservation: ActionResources {
                    cpu_millicores,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(1),
                },
                learned_reservation: None,
                initial_watchers: 1,
            },
            now(1),
            queue,
        ) else {
            panic!("expected insert");
        };
        key
    }

    #[test]
    fn operation_driven_prefers_higher_preference_fleet_over_idler_fallback() {
        let cat = two_fleet_catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(cat.len()).expect("queue");

        let pref = register_worker(&mut workers, "preferred", "i-pref", 1, 8_000, 16);
        let _fb = register_worker(&mut workers, "fallback", "i-fb", 2, 16_000, 32);

        // Give preferred worker 50% CPU load while fallback is 0% loaded.
        workers
            .commit_assignment(
                pref,
                ActionResources {
                    cpu_millicores: 4_000,
                    memory: Bytes::gib(1),
                    disk: Bytes::gib(1),
                },
                false,
            )
            .expect("reserve");

        let op = admit_op(
            &mut ops,
            &mut queue,
            "operations/pref",
            &[0, 1],
            100,
            2_000,
            2,
        );
        let placed = place_operation(op, &mut ops, &mut queue, &mut workers).expect("placed");
        assert_eq!(placed.worker, pref);
        assert!(queue.is_empty());
        assert_eq!(ops.get(op).expect("op").stage, OperationStage::Running);
    }

    #[test]
    fn distributed_tie_breaks_cpu_fraction_then_gate_then_lru_then_worker_id() {
        let cat = two_fleet_catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(cat.len()).expect("queue");

        // Workers with identical capacity (8000m CPU):
        // w_a: 25% CPU reserved (2000/8000)
        // w_b: 50% CPU reserved (4000/8000)
        // w_c: 25% CPU reserved (1000/4000) -> same fraction as w_a, 1 gate_running
        // w_d: 25% CPU reserved (2000/8000), 0 gate_running, higher last_assigned_seq
        let w_b = register_worker(&mut workers, "preferred", "i-b", 2, 8_000, 16);
        let w_c = register_worker(&mut workers, "preferred", "i-c", 3, 4_000, 16);
        let w_d = register_worker(&mut workers, "preferred", "i-d", 4, 8_000, 16);
        let w_a = register_worker(&mut workers, "preferred", "i-a", 1, 8_000, 16);

        let r1k = ActionResources {
            cpu_millicores: 1_000,
            memory: Bytes::gib(1),
            disk: Bytes::gib(1),
        };
        let r2k = ActionResources {
            cpu_millicores: 2_000,
            memory: Bytes::gib(1),
            disk: Bytes::gib(1),
        };
        let r4k = ActionResources {
            cpu_millicores: 4_000,
            memory: Bytes::gib(1),
            disk: Bytes::gib(1),
        };

        workers.commit_assignment(w_b, r4k, false).expect("b");
        workers.commit_assignment(w_c, r1k, true).expect("c");
        workers.commit_assignment(w_d, r2k, false).expect("d");
        // Keep w_a at 25% with seq=4? No, give w_a 0 gate_running and assign it BEFORE w_d?
        // Actually w_d got seq=3 above; let's assign w_a BEFORE w_d by committing w_a then w_d?
        // Here w_d already got seq=3, so if w_a gets seq=4, w_d has lower last_assigned_seq (LRU)!
        workers.commit_assignment(w_a, r2k, false).expect("a");

        // w_b has 50%; w_c has 25% + 1 gate; w_d and w_a have 25% + 0 gate, w_d has seq=3 < w_a seq=4.
        let op1 = admit_op(&mut ops, &mut queue, "operations/t1", &[0], 100, 1_000, 1);
        let p1 = place_operation(op1, &mut ops, &mut queue, &mut workers).expect("p1");
        assert_eq!(p1.worker, w_d);

        // Now test WorkerId tie-break when fraction, gate_running, and last_assigned_seq all match (0).
        let mut workers2 = WorkerRegistry::from_catalog(&cat).expect("registry2");
        let w_z = register_worker(&mut workers2, "preferred", "i-z", 9, 8_000, 16);
        let w_m = register_worker(&mut workers2, "preferred", "i-m", 8, 8_000, 16);
        let op2 = admit_op(&mut ops, &mut queue, "operations/t2", &[0], 100, 1_000, 1);
        let p2 = place_operation(op2, &mut ops, &mut queue, &mut workers2).expect("p2");
        assert_eq!(p2.worker, w_m);
        assert_ne!(w_z, w_m);
    }

    #[test]
    fn worker_driven_scan_respects_priority_and_dispatch_scan_limit() {
        let cat = two_fleet_catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(cat.len()).expect("queue");

        // Enqueue 3 huge priority-200 ops (too big for worker), then 1 fitting priority-100 op.
        let _big1 = admit_op(
            &mut ops,
            &mut queue,
            "operations/big1",
            &[0],
            200,
            16_000,
            1,
        );
        let _big2 = admit_op(
            &mut ops,
            &mut queue,
            "operations/big2",
            &[0],
            200,
            16_000,
            1,
        );
        let _big3 = admit_op(
            &mut ops,
            &mut queue,
            "operations/big3",
            &[0],
            200,
            16_000,
            1,
        );
        let small = admit_op(
            &mut ops,
            &mut queue,
            "operations/small",
            &[0],
            100,
            2_000,
            2,
        );

        let w = register_worker(&mut workers, "preferred", "i-small", 1, 4_000, 8);
        let mut assigned = Vec::new();

        // With scan_limit = 2, scanning stops after skipping big1 and big2.
        scan_queue_for_worker(w, 2, &mut ops, &mut queue, &mut workers, &mut assigned);
        assert!(assigned.is_empty());

        // With scan_limit = 4, it skips the 3 big ops and assigns `small`.
        scan_queue_for_worker(w, 4, &mut ops, &mut queue, &mut workers, &mut assigned);
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].operation, small);
        assert_eq!(queue.len(), 3);
        assert_eq!(ops.get(small).expect("op").stage, OperationStage::Running);
    }

    #[test]
    fn ineligible_workers_never_receive_assignments() {
        let cat = two_fleet_catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("registry");
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(cat.len()).expect("queue");

        let mut reg = WorkerRegistration {
            instance: Arc::new(InstanceId::try_new("i-1").expect("instance")),
            fleet: FleetName::try_new("preferred").expect("fleet"),
            gang_version: "0.3.8".to_owned(), // stale build
            properties: BTreeMap::new(),
            capacity: ActionResources {
                cpu_millicores: 8_000,
                memory: Bytes::gib(16),
                disk: Bytes::gib(100),
            },
            ceilings: ActionResources::default(),
            max_inflight: Some(NonZeroU32::MIN),
            single_use: false,
            environment: None,
            uuid_bytes: [1; 16],
        };
        let stale = workers
            .register(now(1), "0.3.9", false, reg.clone())
            .expect("stale");
        reg.gang_version = "0.3.9".to_owned();
        reg.uuid_bytes = [2; 16];
        let good = workers.register(now(1), "0.3.9", false, reg).expect("good");

        let op1 = admit_op(&mut ops, &mut queue, "operations/1", &[0], 100, 1_000, 1);
        let op2 = admit_op(&mut ops, &mut queue, "operations/2", &[0], 100, 1_000, 1);

        // `stale` is skipped; `good` takes `op1` and hits `max_inflight = 1`.
        let p1 = place_operation(op1, &mut ops, &mut queue, &mut workers).expect("p1");
        assert_eq!(p1.worker, good);
        assert!(place_operation(op2, &mut ops, &mut queue, &mut workers).is_none());

        // Disk hold blocks dispatch until keepalive clears it.
        workers
            .on_attempt_aborted(good, p1.reservation, false)
            .expect("abort");
        assert!(workers.set_disk_hold(good));
        assert!(place_operation(op2, &mut ops, &mut queue, &mut workers).is_none());
        assert_eq!(workers.on_keepalive(good, now(2), None, None), Some(true));
        let p2 = place_operation(op2, &mut ops, &mut queue, &mut workers).expect("p2");
        assert_eq!(p2.worker, good);
        assert_ne!(stale, good);
    }
}
