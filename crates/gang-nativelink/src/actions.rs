//! Live listing of the actions `NativeLink` executes and holds queued, for the scheduler snapshot.

use std::sync::Arc;

use futures::StreamExt;
use gang_clock::Timestamp;
use gang_spi::{ActionInfo, ActionsView, ListingLimits, QueuedActionView, RunningActionView};
use gang_types::{FleetCatalog, WorkerId};
use nativelink_error::Error;
use nativelink_scheduler::worker_scheduler::WorkerSummary;
use nativelink_util::action_messages::{
    ActionInfo as NativeActionInfo, ActionState, WorkerId as NativeWorkerId,
};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, OperationFilter, OperationStageFlags, OrderDirection,
};
use nativelink_util::origin_event::OriginMetadata;

use crate::snapshot::managed_worker;

/// Reads the running and queued actions into `into`.
///
/// The listings are: the running operations of each managed worker with running actions, then
/// the queued operations in descending priority order. The running operations are sorted by id;
/// action info for those past `limits.running` is not read and they are counted in
/// `truncated_running`. The buffers in `into` are cleared and reused.
///
/// # Errors
///
/// Returns the scheduler's error when a listing or an operation read fails.
pub(crate) async fn list(
    scheduler: &dyn ClientStateManager,
    workers: &[WorkerSummary],
    fleets: &FleetCatalog,
    limits: ListingLimits,
    into: &mut ActionsView,
) -> Result<(), Error> {
    into.running.clear();
    into.queued.clear();
    into.truncated_running = 0;
    into.truncated_queued = 0;
    running(scheduler, workers, limits.running, into).await?;
    queued(scheduler, fleets, limits.queued, into).await
}

/// One running operation and the worker running it, before its action info is read.
struct RunningRow {
    /// The operation state used to sort the listing.
    state: Arc<ActionState>,
    /// Metadata attached to the originating request.
    origin: Option<OriginMetadata>,
    /// The worker running this operation.
    worker: WorkerId,
    /// The operation result used to read action info.
    result: Box<dyn ActionStateResult>,
}

/// Fills the running actions, associating each with the worker that runs it.
async fn running(
    scheduler: &dyn ClientStateManager,
    workers: &[WorkerSummary],
    limit: usize,
    into: &mut ActionsView,
) -> Result<(), Error> {
    let mut rows = Vec::new();
    for summary in workers.iter().filter(|summary| summary.running_actions > 0) {
        let Some((worker, _)) = managed_worker(summary) else {
            continue;
        };
        let mut operations = scheduler
            .filter_operations(OperationFilter {
                stages: OperationStageFlags::Executing,
                worker_id: Some(NativeWorkerId::from(summary.id.clone())),
                ..OperationFilter::default()
            })
            .await?;
        while let Some(result) = operations.next().await {
            let (state, origin) = result.as_state().await?;
            rows.push(RunningRow {
                state,
                origin,
                worker: worker.clone(),
                result,
            });
        }
    }

    rows.sort_by(|a, b| {
        a.state
            .client_operation_id
            .cmp(&b.state.client_operation_id)
    });
    into.truncated_running = u64::try_from(rows.len().saturating_sub(limit)).unwrap_or(u64::MAX);
    for RunningRow {
        state,
        origin,
        worker,
        result,
    } in rows.into_iter().take(limit)
    {
        let row = read(state, origin, &*result).await?;
        into.running.push(RunningActionView {
            info: ActionInfo {
                op: row.op,
                digest: row.digest,
                target: row.target,
                mnemonic: row.mnemonic,
                priority: row.info.priority,
            },
            worker,
            started: row.started,
        });
    }
    Ok(())
}

/// Fills the queued actions in the order the scheduler yields them, highest priority first.
async fn queued(
    scheduler: &dyn ClientStateManager,
    fleets: &FleetCatalog,
    limit: usize,
    into: &mut ActionsView,
) -> Result<(), Error> {
    let mut listing = scheduler
        .filter_operations(OperationFilter {
            stages: OperationStageFlags::Queued,
            order_by_priority_direction: Some(OrderDirection::Desc),
            ..OperationFilter::default()
        })
        .await?;
    while let Some(result) = listing.next().await {
        if into.queued.len() >= limit {
            into.truncated_queued += 1;
            continue;
        }
        let (state, origin) = result.as_state().await?;
        let row = read(state, origin, &*result).await?;
        into.queued.push(QueuedActionView {
            info: ActionInfo {
                op: row.op,
                digest: row.digest,
                target: row.target,
                mnemonic: row.mnemonic,
                priority: row.info.priority,
            },
            queued_since: Timestamp::from(row.info.insert_timestamp),
            candidates: fleets
                .accepting(|key| row.info.platform_properties.get(key).map(String::as_str)),
        });
    }
    Ok(())
}

/// One operation read from a listing, with the fields both listings need.
struct Listed {
    op: String,
    digest: String,
    target: String,
    mnemonic: String,
    started: Timestamp,
    info: Arc<NativeActionInfo>,
}

/// Reads an operation's action info and combines it with its state and request metadata.
async fn read(
    state: Arc<ActionState>,
    origin: Option<OriginMetadata>,
    result: &dyn ActionStateResult,
) -> Result<Listed, Error> {
    let (info, _) = result.as_action_info().await?;
    let (target, mnemonic) = origin.and_then(|origin| origin.bazel_metadata).map_or_else(
        || (String::new(), String::new()),
        |metadata| (metadata.target_id, metadata.action_mnemonic),
    );
    Ok(Listed {
        op: state.client_operation_id.to_string(),
        digest: format!(
            "{}/{}",
            state.action_digest.packed_hash(),
            state.action_digest.size_bytes()
        ),
        target,
        mnemonic,
        started: Timestamp::from(state.last_transition_timestamp),
        info,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use async_trait::async_trait;
    use futures::stream;
    use gang_types::{Bytes, FleetName, FleetProperties, Isa, Resources, property};
    use nativelink_error::{Code, Error};
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
    use nativelink_scheduler::worker_scheduler::WorkerSummary;
    use nativelink_util::action_messages::{
        ActionInfo as NativeActionInfo, ActionStage, ActionState, ActionUniqueKey,
        ActionUniqueQualifier, OperationId, WorkerId as NativeWorkerId,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::operation_state_manager::{
        ActionStateResult, ActionStateResultStream, ClientStateManager, OperationFilter,
        OperationStageFlags,
    };
    use nativelink_util::origin_event::OriginMetadata;
    use parking_lot::Mutex;

    use super::*;

    const WORKER_A: &str = "local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b";
    const WORKER_B: &str = "local-2-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6c";
    const WORKER_IDLE: &str = "local-3-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6d";
    const DIGEST: &str = "0303030303030303030303030303030303030303030303030303030303030303/30";

    const ALL: ListingLimits = ListingLimits {
        running: 100,
        queued: 100,
    };

    /// A fake operation: the state and action info a listing yields, plus its request metadata.
    #[derive(Clone)]
    struct Row {
        state: Arc<ActionState>,
        info: Arc<NativeActionInfo>,
        origin: Option<RequestMetadata>,
        state_reads: Arc<AtomicUsize>,
        info_reads: Arc<AtomicUsize>,
    }

    impl Row {
        fn origin(&self) -> OriginMetadata {
            OriginMetadata {
                identity: String::new(),
                bazel_metadata: self.origin.clone(),
            }
        }
    }

    #[async_trait]
    impl ActionStateResult for Row {
        async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            self.state_reads.fetch_add(1, Ordering::Relaxed);
            Ok((Arc::clone(&self.state), Some(self.origin())))
        }

        async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Err(Error::new(
                Code::Unimplemented,
                "changed is not faked".to_owned(),
            ))
        }

        async fn as_action_info(
            &self,
        ) -> Result<(Arc<NativeActionInfo>, Option<OriginMetadata>), Error> {
            self.info_reads.fetch_add(1, Ordering::Relaxed);
            Ok((Arc::clone(&self.info), Some(self.origin())))
        }
    }

    /// A fake scheduler that serves fixed listings and records every filter it receives.
    #[derive(Default)]
    struct Fake {
        queued: Vec<Row>,
        by_worker: HashMap<String, Vec<Row>>,
        calls: Mutex<Vec<OperationFilter>>,
    }

    impl Fake {
        fn calls(&self) -> Vec<OperationFilter> {
            self.calls.lock().clone()
        }
    }

    impl MetricsComponent for Fake {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData<'_>,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl ClientStateManager for Fake {
        async fn add_action(
            &self,
            _client_operation_id: OperationId,
            _action_info: Arc<NativeActionInfo>,
        ) -> Result<Box<dyn ActionStateResult>, Error> {
            Err(Error::new(
                Code::Unimplemented,
                "add_action is not faked".to_owned(),
            ))
        }

        async fn filter_operations(
            &self,
            filter: OperationFilter,
        ) -> Result<ActionStateResultStream<'_>, Error> {
            let rows = match &filter.worker_id {
                Some(worker) => self.by_worker.get(&worker.0).cloned().unwrap_or_default(),
                None if filter.stages == OperationStageFlags::Queued => self.queued.clone(),
                None => Vec::new(),
            };
            self.calls.lock().push(filter);
            let listing: ActionStateResultStream<'_> = Box::pin(stream::iter(
                rows.into_iter()
                    .map(|row| Box::new(row) as Box<dyn ActionStateResult>),
            ));
            Ok(listing)
        }
    }

    fn digest(byte: u8, size: u64) -> DigestInfo {
        DigestInfo::new([byte; 32], size)
    }

    fn bazel(target: &str, mnemonic: &str) -> RequestMetadata {
        RequestMetadata {
            target_id: target.to_owned(),
            action_mnemonic: mnemonic.to_owned(),
            ..RequestMetadata::default()
        }
    }

    fn info(priority: i32, inserted_ms: u64, platform: &[(&str, &str)]) -> Arc<NativeActionInfo> {
        Arc::new(NativeActionInfo {
            command_digest: digest(1, 10),
            input_root_digest: digest(2, 20),
            timeout: Duration::from_mins(1),
            platform_properties: platform
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            priority,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH + Duration::from_millis(inserted_ms),
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest: digest(3, 30),
            }),
        })
    }

    fn row(
        op: &str,
        stage: ActionStage,
        started_ms: u64,
        info: Arc<NativeActionInfo>,
        origin: Option<RequestMetadata>,
    ) -> Row {
        Row {
            state: Arc::new(ActionState {
                stage,
                last_transition_timestamp: UNIX_EPOCH + Duration::from_millis(started_ms),
                client_operation_id: OperationId::String(op.to_owned()),
                action_digest: digest(3, 30),
            }),
            info,
            origin,
            state_reads: Arc::new(AtomicUsize::new(0)),
            info_reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn summary(id: &str, running: u32) -> WorkerSummary {
        WorkerSummary {
            id: id.to_owned(),
            running_actions: running,
            max_inflight_tasks: 4,
            is_paused: false,
            is_draining: false,
            last_update_timestamp: 100,
            platform_properties: HashMap::from([(
                property::FLEET.to_owned(),
                "fleet-a".to_owned(),
            )]),
            available_platform_properties: HashMap::new(),
            free_memory_kb: None,
        }
    }

    fn fleet(name: &str, isa: Isa) -> Result<FleetProperties, Box<dyn std::error::Error>> {
        Ok(FleetProperties::new(
            &FleetName::try_new(name)?,
            isa,
            &"docker",
            Resources {
                vcpus: 4,
                memory: Bytes::new(8_589_934_592),
                disk: Bytes::new(0),
            },
            Vec::<(&'static str, String)>::new(),
        ))
    }

    fn catalog() -> Result<FleetCatalog, Box<dyn std::error::Error>> {
        Ok(FleetCatalog::try_new(vec![
            fleet("fleet-a", Isa::X86_64)?,
            fleet("fleet-b", Isa::Aarch64)?,
        ])?)
    }

    async fn run(
        fake: &Fake,
        workers: &[WorkerSummary],
        fleets: &FleetCatalog,
        limits: ListingLimits,
    ) -> Result<ActionsView, Box<dyn std::error::Error>> {
        let mut view = ActionsView::default();
        list(fake, workers, fleets, limits, &mut view)
            .await
            .map_err(|error| error.to_string())?;
        Ok(view)
    }

    #[tokio::test]
    async fn operations_are_associated_with_the_worker_running_them()
    -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let op_a = row(
            "op-a",
            ActionStage::Executing,
            1_000,
            info(5, 0, &[]),
            Some(bazel("//app:bin", "CppCompile")),
        );
        let op_b = row(
            "op-b",
            ActionStage::Executing,
            2_000,
            info(3, 0, &[]),
            Some(bazel("//b:lib", "Javac")),
        );
        let fake = Fake {
            by_worker: HashMap::from([
                (WORKER_A.to_owned(), vec![op_a]),
                (WORKER_B.to_owned(), vec![op_b]),
            ]),
            ..Fake::default()
        };
        let workers = [summary(WORKER_A, 1), summary(WORKER_B, 1)];

        let view = run(&fake, &workers, &fleets, ALL).await?;

        assert_eq!(
            view.running,
            vec![
                RunningActionView {
                    info: ActionInfo {
                        op: "op-a".to_owned(),
                        digest: DIGEST.to_owned(),
                        target: "//app:bin".to_owned(),
                        mnemonic: "CppCompile".to_owned(),
                        priority: 5,
                    },
                    worker: WorkerId::try_new(WORKER_A)?,
                    started: Timestamp::from_unix_millis(1_000),
                },
                RunningActionView {
                    info: ActionInfo {
                        op: "op-b".to_owned(),
                        digest: DIGEST.to_owned(),
                        target: "//b:lib".to_owned(),
                        mnemonic: "Javac".to_owned(),
                        priority: 3,
                    },
                    worker: WorkerId::try_new(WORKER_B)?,
                    started: Timestamp::from_unix_millis(2_000),
                },
            ]
        );
        assert_eq!(view.truncated_running, 0);
        Ok(())
    }

    #[tokio::test]
    async fn queued_actions_carry_their_metadata_and_candidates()
    -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let first = row(
            "q-1",
            ActionStage::Queued,
            0,
            info(7, 4_000, &[("cpu", "2")]),
            Some(bazel("//q:one", "Genrule")),
        );
        let second = row("q-2", ActionStage::Queued, 0, info(2, 5_000, &[]), None);
        let fake = Fake {
            queued: vec![first, second],
            ..Fake::default()
        };

        let view = run(&fake, &[], &fleets, ALL).await?;

        assert_eq!(
            view.queued,
            vec![
                QueuedActionView {
                    info: ActionInfo {
                        op: "q-1".to_owned(),
                        digest: DIGEST.to_owned(),
                        target: "//q:one".to_owned(),
                        mnemonic: "Genrule".to_owned(),
                        priority: 7,
                    },
                    queued_since: Timestamp::from_unix_millis(4_000),
                    candidates: fleets.accepting(|key| (key == "cpu").then_some("2")),
                },
                QueuedActionView {
                    info: ActionInfo {
                        op: "q-2".to_owned(),
                        digest: DIGEST.to_owned(),
                        target: String::new(),
                        mnemonic: String::new(),
                        priority: 2,
                    },
                    queued_since: Timestamp::from_unix_millis(5_000),
                    candidates: fleets.accepting(|_| None),
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn limits_keep_the_first_rows_and_count_the_rest()
    -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let op_a = row("op-a", ActionStage::Executing, 1_000, info(0, 0, &[]), None);
        let op_b = row("op-b", ActionStage::Executing, 2_000, info(0, 0, &[]), None);
        let op_c = row("op-c", ActionStage::Executing, 3_000, info(0, 0, &[]), None);
        let fake = Fake {
            queued: vec![
                row("q-1", ActionStage::Queued, 0, info(0, 1_000, &[]), None),
                row("q-2", ActionStage::Queued, 0, info(0, 2_000, &[]), None),
                row("q-3", ActionStage::Queued, 0, info(0, 3_000, &[]), None),
            ],
            by_worker: HashMap::from([
                (WORKER_A.to_owned(), vec![op_a]),
                (WORKER_B.to_owned(), vec![op_b, op_c]),
            ]),
            ..Fake::default()
        };
        let workers = [summary(WORKER_A, 1), summary(WORKER_B, 1)];
        let limits = ListingLimits {
            running: 2,
            queued: 1,
        };

        let view = run(&fake, &workers, &fleets, limits).await?;

        let running: Vec<&str> = view
            .running
            .iter()
            .map(|action| action.info.op.as_str())
            .collect();
        assert_eq!(running, ["op-a", "op-b"]);
        assert_eq!(view.truncated_running, 1);
        let queued: Vec<&str> = view
            .queued
            .iter()
            .map(|action| action.info.op.as_str())
            .collect();
        assert_eq!(queued, ["q-1"]);
        assert_eq!(view.truncated_queued, 2);
        Ok(())
    }

    #[tokio::test]
    async fn running_rows_beyond_the_limit_are_counted_but_not_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let op_a = row("op-a", ActionStage::Executing, 1_000, info(0, 0, &[]), None);
        let op_b = row("op-b", ActionStage::Executing, 2_000, info(0, 0, &[]), None);
        let op_c = row("op-c", ActionStage::Executing, 3_000, info(0, 0, &[]), None);
        let fake = Fake {
            by_worker: HashMap::from([(
                WORKER_A.to_owned(),
                vec![op_c.clone(), op_a.clone(), op_b.clone()],
            )]),
            ..Fake::default()
        };
        let workers = [summary(WORKER_A, 3)];
        let limits = ListingLimits {
            running: 1,
            queued: 0,
        };

        let view = run(&fake, &workers, &fleets, limits).await?;

        let running: Vec<&str> = view
            .running
            .iter()
            .map(|action| action.info.op.as_str())
            .collect();
        assert_eq!(running, ["op-a"]);
        assert_eq!(view.truncated_running, 2);
        for operation in [&op_a, &op_b, &op_c] {
            assert_eq!(operation.state_reads.load(Ordering::Relaxed), 1);
        }
        assert_eq!(op_a.info_reads.load(Ordering::Relaxed), 1);
        assert_eq!(op_b.info_reads.load(Ordering::Relaxed), 0);
        assert_eq!(op_c.info_reads.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn running_excludes_workers_missing_from_the_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let mut missing_fleet = summary(WORKER_B, 1);
        missing_fleet.platform_properties.clear();
        let mut invalid_fleet = summary(WORKER_IDLE, 1);
        invalid_fleet
            .platform_properties
            .insert(property::FLEET.to_owned(), "Invalid Fleet".to_owned());
        let workers = [
            summary(WORKER_A, 1),
            missing_fleet,
            invalid_fleet,
            summary("legacy-worker", 1),
        ];
        let fake = Fake {
            by_worker: workers
                .iter()
                .map(|worker| {
                    (
                        worker.id.clone(),
                        vec![row(
                            &worker.id,
                            ActionStage::Executing,
                            0,
                            info(0, 0, &[]),
                            None,
                        )],
                    )
                })
                .collect(),
            ..Fake::default()
        };
        let mut snapshot = gang_spi::SchedulerSnapshot::default();
        crate::snapshot::fill(&workers, &[], &fleets, 0, &mut snapshot);

        let view = run(&fake, &workers, &fleets, ALL).await?;

        assert_eq!(snapshot.workers.len(), 1);
        assert_eq!(view.running.len(), 1);
        assert!(view.running.iter().all(|action| {
            snapshot
                .workers
                .iter()
                .any(|worker| worker.id == action.worker)
        }));
        assert_eq!(view.truncated_running, 0);
        Ok(())
    }

    #[tokio::test]
    async fn idle_workers_are_never_queried() -> Result<(), Box<dyn std::error::Error>> {
        let fleets = catalog()?;
        let fake = Fake::default();
        let workers = [
            summary(WORKER_A, 1),
            summary(WORKER_IDLE, 0),
            summary(WORKER_B, 2),
        ];

        run(&fake, &workers, &fleets, ALL).await?;

        assert!(
            !fake.calls().iter().any(|call| {
                call.worker_id == Some(NativeWorkerId::from(WORKER_IDLE.to_owned()))
            })
        );
        Ok(())
    }
}
