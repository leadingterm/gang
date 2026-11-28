//! Direct domain-to-[`ActionsView`] projection bounded by [`ListingLimits`] (`§5.9`).
//!
//! Reads queued operations directly from [`OperationQueue::global_iter`] in `O(limit)` without
//! sorting, and running operations from [`Attempts`] ordered by operation name.

use gang_spi::{ActionInfo, ActionsView, ListingLimits, QueuedActionView, RunningActionView};
use gang_types::Refusal;

use crate::attempts::{AttemptPhase, Attempts};
use crate::operations::{Operation, OperationStage, Operations};
use crate::queue::OperationQueue;
use crate::workers::WorkerRegistry;

/// Projects running and queued actions into `into`, bounded by `limits` and reusing `into`'s
/// buffers (`§5.9`).
pub fn fill_actions(
    limits: ListingLimits,
    operations: &Operations,
    queue: &OperationQueue,
    attempts: &Attempts,
    workers: &WorkerRegistry,
    into: &mut ActionsView,
) {
    into.running.clear();
    into.queued.clear();
    into.truncated_running = 0;
    into.truncated_queued = 0;

    let mut running_rows = Vec::new();
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
        let started = attempt.accepted_at.unwrap_or(attempt.dispatched_at).wall;
        running_rows.push((op, worker.id.clone(), started));
    }
    running_rows.sort_unstable_by(|left, right| left.0.name.cmp(&right.0.name));

    let total_running = running_rows.len();
    into.truncated_running =
        u64::try_from(total_running.saturating_sub(limits.running)).unwrap_or(u64::MAX);
    for (op, worker, started) in running_rows.into_iter().take(limits.running) {
        into.running.push(RunningActionView {
            info: action_info(op),
            worker,
            started,
        });
    }

    for entry in queue.global_iter().take(limits.queued) {
        let Some(op) = operations.get(entry.key) else {
            continue;
        };
        if op.stage != OperationStage::Queued {
            continue;
        }
        let refusal = op.candidates.is_empty().then_some(Refusal::Combination);
        into.queued.push(QueuedActionView {
            info: action_info(op),
            queued_since: op.queued_since.wall,
            candidates: op.candidates,
            refusal,
        });
    }
    into.truncated_queued =
        u64::try_from(queue.len().saturating_sub(into.queued.len())).unwrap_or(u64::MAX);
}

/// Allocates and returns a fresh [`ActionsView`] bounded by `limits` (`§5.9`).
#[must_use]
pub fn project_actions(
    limits: ListingLimits,
    operations: &Operations,
    queue: &OperationQueue,
    attempts: &Attempts,
    workers: &WorkerRegistry,
) -> ActionsView {
    let mut view = ActionsView::default();
    fill_actions(limits, operations, queue, attempts, workers, &mut view);
    view
}

fn action_info(op: &Operation) -> ActionInfo {
    ActionInfo {
        op: op.name.as_str().to_owned(),
        digest: op.action_digest.to_string(),
        target: op
            .target
            .as_ref()
            .map_or_else(String::new, |t| t.as_str().to_owned()),
        mnemonic: op
            .mnemonic
            .as_ref()
            .map_or_else(String::new, |m| m.as_str().to_owned()),
        priority: op.priority,
        build: op.build,
        reservation: op.reservation,
        learned_reservation: op.learned_reservation,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::{
        ActionResources, AttemptId, BuildId, Bytes, Digest, FleetActionPolicy, FleetCatalog,
        FleetName, FleetProperties, FleetSet, InstanceId, Isa, Mnemonic, OperationName, Resources,
        TargetLabel,
    };

    use super::*;
    use crate::operations::{AdmitOutcome, NewOperation, Now, canonical_platform_key};
    use crate::placement::place_operation;
    use crate::workers::WorkerRegistration;

    fn now(secs: u64) -> Now {
        Now {
            tick: Tick::from_elapsed(Duration::from_secs(secs)),
            wall: Timestamp::from_unix_millis(secs * 1_000),
        }
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("build-x86").expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus: 8,
                memory: Bytes::gib(32),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )])
        .expect("catalog")
    }

    fn register_worker(workers: &mut WorkerRegistry, seed: u8) -> crate::workers::WorkerKey {
        workers
            .register(
                now(1),
                "0.3.9",
                false,
                WorkerRegistration {
                    instance: Arc::new(InstanceId::try_new("i-1").expect("instance")),
                    fleet: FleetName::try_new("build-x86").expect("fleet"),
                    gang_version: "0.3.9".to_owned(),
                    properties: BTreeMap::new(),
                    capacity: ActionResources {
                        cpu_millicores: 16_000,
                        memory: Bytes::gib(32),
                        disk: Bytes::gib(100),
                    },
                    ceilings: ActionResources {
                        cpu_millicores: 8_000,
                        memory: Bytes::gib(32),
                        disk: Bytes::gib(100),
                    },
                    max_inflight: None,
                    single_use: false,
                    environment: None,
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
                source: None,
                build: Some("12rsfjsg79g6h7f3".parse::<BuildId>().expect("build")),
                target: Some(TargetLabel::try_new("//pkg:target").expect("target")),
                mnemonic: Some(Mnemonic::try_new("Link").expect("mnemonic")),
                candidates: FleetSet::try_from_indices([0]).expect("fleet"),
                priority,
                has_explicit_cpu_floor: false,
                reservation: ActionResources {
                    cpu_millicores: 1_000,
                    memory: Bytes::gib(1),
                    disk: Bytes::gib(1),
                },
                learned_reservation: Some(ActionResources {
                    cpu_millicores: 2_000,
                    memory: Bytes::gib(4),
                    disk: Bytes::gib(1),
                }),
                initial_watchers: 1,
            },
            at,
            queue,
        ) else {
            panic!("insert");
        };
        key
    }

    #[test]
    fn actions_projects_running_sorted_and_queued_in_global_order_with_limits() {
        let cat = catalog();
        let mut workers = WorkerRegistry::from_catalog(&cat).expect("workers");
        let worker_key = register_worker(&mut workers, 1);
        let worker_id = workers.get(worker_key).expect("worker").id.clone();

        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let mut attempts = Attempts::new();

        // Admit two running ops out of name order (`operations/r-b` before `operations/r-a`).
        let r_b = admit_op(&mut ops, &mut queue, "operations/r-b", 100, now(2));
        let r_a = admit_op(&mut ops, &mut queue, "operations/r-a", 100, now(3));
        for (idx, op_key) in [r_b, r_a].into_iter().enumerate() {
            let placed =
                place_operation(op_key, &mut ops, &mut queue, &mut workers).expect("place");
            let attempt_id = AttemptId::from_random_bytes([u8::try_from(idx + 1).expect("u8"); 16]);
            attempts
                .dispatch(
                    placed,
                    attempt_id.clone(),
                    now(4),
                    Duration::from_secs(10),
                    Duration::from_mins(5),
                )
                .expect("dispatch");
            if op_key == r_a {
                attempts.on_accept(&attempt_id, now(5), &ops);
            }
        }

        // Admit three queued ops with varying priorities and timestamps.
        let _q_low = admit_op(&mut ops, &mut queue, "operations/q-low", 0, now(6));
        let _q_gate_old = admit_op(&mut ops, &mut queue, "operations/q-gate-old", 250, now(7));
        let _q_gate_new = admit_op(&mut ops, &mut queue, "operations/q-gate-new", 250, now(8));

        let view = project_actions(
            ListingLimits {
                running: 1,
                queued: 2,
            },
            &ops,
            &queue,
            &attempts,
            &workers,
        );
        assert_eq!(view.truncated_running, 1);
        assert_eq!(view.truncated_queued, 1);

        assert_eq!(view.running.len(), 1);
        assert_eq!(view.running[0].info.op, "operations/r-a");
        assert_eq!(view.running[0].worker, worker_id);
        assert_eq!(view.running[0].started, now(5).wall);
        assert_eq!(view.running[0].info.target, "//pkg:target");
        assert_eq!(view.running[0].info.mnemonic, "Link");
        assert!(view.running[0].info.learned_reservation.is_some());

        assert_eq!(view.queued.len(), 2);
        assert_eq!(view.queued[0].info.op, "operations/q-gate-old");
        assert_eq!(view.queued[0].info.priority, 250);
        assert_eq!(view.queued[0].queued_since, now(7).wall);
        assert!(view.queued[0].refusal.is_none());
        assert_eq!(view.queued[1].info.op, "operations/q-gate-new");
    }
}
