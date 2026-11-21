//! Conversion of `NativeLink`'s typed listings into the scheduler snapshot Gang reads.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use gang_clock::Timestamp;
use gang_spi::{
    ExecutingDemand, FleetMemoryCapacity, QueuedDemand, SchedulerSnapshot, WorkerState,
};
use gang_types::{
    ActionResources, Bytes, FleetCatalog, FleetName, FleetSet, NonZeroBytes, Refusal, SourceId,
    WorkerId, property,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_scheduler::worker_scheduler::WorkerSummary;
use nativelink_util::action_messages::{ActionInfo as NativeActionInfo, ActionState, OperationId};
use nativelink_util::operation_state_manager::{ClientStateManager, OperationStageFlags};
use nativelink_util::origin_event::OriginMetadata;
use parking_lot::Mutex;

use crate::actions;
use crate::durations::ActionDurations;
use crate::source_rates::SourceRates;

/// Milliseconds in one second, the unit of `last_update_timestamp`.
const MILLIS_PER_SECOND: u64 = 1000;

/// Returns the identity and fleet of a worker managed by Gang.
pub(crate) fn managed_worker(summary: &WorkerSummary) -> Option<(WorkerId, FleetName)> {
    let id = WorkerId::try_new(summary.id.as_str()).ok()?;
    id.instance_name()?;
    let fleet =
        FleetName::try_new(summary.platform_properties.get(property::FLEET)?.as_str()).ok()?;
    Some((id, fleet))
}

/// Converts a connected worker into a scheduler worker.
///
/// Returns `None` for workers without a managed id or valid fleet. Missing or invalid
/// environments are retained as unknown.
/// `NativeLink` reports `max_inflight_tasks` 0 for a worker without a limit, which maps to
/// `None`, as do counts above `u32::MAX`. A paused worker is not draining: only the draining
/// flag counts; the pause itself is reported by `paused`.
fn worker_state(summary: &WorkerSummary) -> Option<WorkerState> {
    let (id, fleet) = managed_worker(summary)?;
    let environment = summary
        .platform_properties
        .get(property::ACTION_ENVIRONMENT)
        .and_then(|value| value.parse().ok());
    let max_inflight_actions = u32::try_from(summary.max_inflight_tasks)
        .ok()
        .and_then(NonZeroU32::new);
    Some(WorkerState {
        id,
        fleet,
        environment,
        running_actions: summary.running_actions,
        max_inflight_actions,
        draining: summary.is_draining,
        paused: summary.is_paused,
        last_seen: Timestamp::from_unix_millis(
            summary
                .last_update_timestamp
                .saturating_mul(MILLIS_PER_SECOND),
        ),
    })
}

/// Matches the original admission intent, not memory-derived whole-worker CPU.
pub(crate) fn candidates(
    info: &NativeActionInfo,
    fleets: &FleetCatalog,
) -> Result<FleetSet, Refusal> {
    let reservation = info.memory_reservation.as_ref();
    let candidates = fleets.admit(
        info.platform_properties
            .iter()
            .filter(|(key, _)| {
                reservation.is_none()
                    || !matches!(key.as_str(), property::MEMORY_KB | property::CPU_MILLICORES)
            })
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )?;
    let candidates = original_minima(info, fleets, candidates);
    if candidates.is_empty() {
        Err(Refusal::Combination)
    } else {
        Ok(candidates)
    }
}

/// Matching for already admitted actions, without allocating another validation request.
pub(crate) fn accepting(info: &NativeActionInfo, fleets: &FleetCatalog) -> FleetSet {
    let candidates = fleets.accepting(|key| {
        if info.memory_reservation.is_some()
            && matches!(key, property::MEMORY_KB | property::CPU_MILLICORES)
        {
            None
        } else {
            info.platform_properties.get(key).map(String::as_str)
        }
    });
    original_minima(info, fleets, candidates)
}

fn original_minima(
    info: &NativeActionInfo,
    fleets: &FleetCatalog,
    mut candidates: FleetSet,
) -> FleetSet {
    if let Some(reservation) = &info.memory_reservation {
        for index in candidates {
            let Some(fleet) = fleets.fleets().get(index) else {
                candidates.remove(index);
                continue;
            };
            if [
                (property::MEMORY_KB, reservation.minimum_kb),
                (property::CPU_MILLICORES, reservation.cpu_before_memory_fit),
            ]
            .into_iter()
            .any(|(key, minimum)| {
                minimum.is_some_and(|minimum| fleet.limit(key).is_none_or(|limit| minimum > limit))
            }) {
                candidates.remove(index);
            }
        }
    }
    candidates
}

/// Converts a queued operation into a scheduler demand.
///
/// Candidates reflect fleet selectors and resource policy; old client environment labels
/// impose no scheduling constraint. Malformed resource values contribute zero.
fn queued_demand(
    info: &NativeActionInfo,
    fleets: &FleetCatalog,
    expected: Option<Duration>,
    source: Option<SourceId>,
) -> QueuedDemand {
    QueuedDemand {
        source,
        candidates: candidates(info, fleets).unwrap_or_default(),
        resources: resources(&info.platform_properties),
        queued_since: Timestamp::from(info.insert_timestamp),
        priority: info.priority,
        expected,
    }
}

fn parse<T: std::str::FromStr + Default>(properties: &HashMap<String, String>, key: &str) -> T {
    properties
        .get(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

/// Reads the resources an action requests from its platform properties.
///
/// Missing or malformed properties request zero. Whole cores are converted to millicores, and
/// memory and disk KiB values are converted to bytes with saturation.
pub(crate) fn resources(properties: &HashMap<String, String>) -> ActionResources {
    let memory_kb: u64 = parse(properties, property::MEMORY_KB);
    let disk_kb: u64 = parse(properties, property::DISK_KB);
    let millicores: u64 = parse(properties, property::CPU_MILLICORES);
    let cores: u32 = parse(properties, property::CPU_COUNT);
    ActionResources {
        cpu_millicores: millicores
            .max(u64::from(cores).saturating_mul(property::MILLICORES_PER_CORE)),
        memory: Bytes::new(memory_kb.saturating_mul(Bytes::kib(1).get())),
        disk: Bytes::new(disk_kb.saturating_mul(Bytes::kib(1).get())),
    }
}

fn expected(
    origin: Option<&OriginMetadata>,
    durations: &Mutex<ActionDurations>,
    now: Timestamp,
) -> Option<Duration> {
    let metadata = origin?.bazel_metadata.as_ref()?;
    durations
        .lock()
        .expected(&metadata.target_id, &metadata.action_mnemonic, now)
}

/// Only snapshots share this scratch. Completions never acquire its async mutex.
#[derive(Default)]
pub(crate) struct Scratch {
    rows: HashMap<StateKey, RowLocation>,
    queued_live: Vec<bool>,
}

struct StateKey(Arc<ActionState>);

impl Borrow<OperationId> for StateKey {
    fn borrow(&self) -> &OperationId {
        &self.0.client_operation_id
    }
}

impl PartialEq for StateKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.client_operation_id == other.0.client_operation_id
    }
}

impl Eq for StateKey {}

impl Hash for StateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.client_operation_id.hash(state);
    }
}

#[derive(Clone, Copy)]
enum RowLocation {
    Queued(usize),
    Executing(usize),
}

struct ScratchGuard<'a>(&'a mut Scratch);

impl Drop for ScratchGuard<'_> {
    fn drop(&mut self) {
        self.0.rows.clear();
        self.0.queued_live.clear();
    }
}

/// Overwrites the snapshot while retaining the capacity of its buffers.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fill(
    scheduler: &dyn ClientStateManager,
    workers: &[WorkerSummary],
    fleets: &FleetCatalog,
    durations: &Mutex<ActionDurations>,
    now: Timestamp,
    completed: u64,
    rates: &SourceRates,
    scratch: &mut Scratch,
    into: &mut SchedulerSnapshot,
) -> Result<(), Error> {
    let scratch = ScratchGuard(scratch);
    into.source_rates_at = rates.snapshot_into(&mut into.source_rates);
    into.completed = completed;
    into.workers.clear();
    into.workers.extend(workers.iter().filter_map(worker_state));
    into.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    into.queued.clear();
    into.executing.clear();
    into.fleet_memory.clear();
    for (fleet, properties) in fleets.fleets().iter().enumerate() {
        let capacity = properties
            .capacity(property::MEMORY_KB)
            .and_then(|kb| kb.checked_mul(1024))
            .and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes)))
            .ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "invalid physical memory for fleet {}",
                    properties.name()
                )
            })?;
        into.fleet_memory
            .push(FleetMemoryCapacity { fleet, capacity });
    }

    let mut listing = scheduler
        .filter_operations(actions::queued_filter())
        .await?;
    while let Some(result) = listing.next().await {
        let Some(operation) =
            actions::read_operation(&*result, OperationStageFlags::Queued).await?
        else {
            continue;
        };
        let id = &operation.state.client_operation_id;
        if scratch.0.rows.contains_key(id) {
            continue;
        }
        let source = rates.source(operation.origin.as_ref());
        let duration = expected(operation.origin.as_ref(), durations, now);
        let demand = queued_demand(&operation.info, fleets, duration, source);
        let index = into.queued.len();
        into.queued.push(demand);
        scratch.0.queued_live.push(true);
        scratch
            .0
            .rows
            .insert(StateKey(operation.state), RowLocation::Queued(index));
    }

    for (worker, state) in into
        .workers
        .iter()
        .enumerate()
        .filter(|(_, state)| state.running_actions > 0)
    {
        let mut listing = scheduler
            .filter_operations(actions::executing_filter(state.id.as_str()))
            .await?;
        while let Some(result) = listing.next().await {
            let Some(operation) =
                actions::read_operation(&*result, OperationStageFlags::Executing).await?
            else {
                continue;
            };
            let source = rates.source(operation.origin.as_ref());
            let row = ExecutingDemand {
                worker,
                source,
                candidates: accepting(&operation.info, fleets),
                resources: resources(&operation.info.platform_properties),
                started: Timestamp::from(operation.state.last_transition_timestamp),
                expected: expected(operation.origin.as_ref(), durations, now),
            };
            match scratch.0.rows.get_mut(&operation.state.client_operation_id) {
                Some(location @ RowLocation::Queued(_)) => {
                    if let RowLocation::Queued(index) = *location {
                        scratch.0.queued_live[index] = false;
                    }
                    *location = RowLocation::Executing(into.executing.len());
                    into.executing.push(row);
                }
                Some(RowLocation::Executing(index)) => into.executing[*index] = row,
                None => {
                    let index = into.executing.len();
                    scratch
                        .0
                        .rows
                        .insert(StateKey(operation.state), RowLocation::Executing(index));
                    into.executing.push(row);
                }
            }
        }
    }
    let mut index = 0;
    into.queued.retain(|_| {
        let live = scratch.0.queued_live[index];
        index += 1;
        live
    });
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::actions::tests::{Fake, bazel, info, row};
    use gang_types::{FleetProperties, FleetSet, Isa, Resources};
    use nativelink_util::action_messages::ActionStage;
    use nativelink_util::operation_state_manager::OrderDirection;

    use super::*;

    const WORKER: &str = "local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b";
    const OTHER_WORKER: &str = "local-2-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6c";
    const LEGACY_WORKER: &str = "legacy-worker";

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn summary(id: &str) -> WorkerSummary {
        WorkerSummary {
            id: id.to_owned(),
            running_actions: 0,
            max_inflight_tasks: 4,
            is_paused: false,
            is_draining: false,
            last_update_timestamp: 100,
            platform_properties: props(&[
                (property::FLEET, "fleet-a"),
                (property::ACTION_ENVIRONMENT, "host:test"),
            ]),
            available_platform_properties: HashMap::new(),
            free_memory_kb: None,
            latest_attempt_deadline: None,
        }
    }

    fn demand(priority: i32, pairs: &[(&str, &str)]) -> Arc<NativeActionInfo> {
        info(priority, 5000, pairs)
    }

    fn queued_demand(info: &NativeActionInfo, fleets: &FleetCatalog) -> QueuedDemand {
        super::queued_demand(info, fleets, None, None)
    }

    pub(crate) fn history() -> Mutex<ActionDurations> {
        Mutex::new(ActionDurations::new(
            NonZeroUsize::new(32).expect("nonzero"),
            gang_config::HistoryWindow::from_units(7),
        ))
    }
    async fn fill(
        scheduler: &dyn ClientStateManager,
        workers: &[WorkerSummary],
        fleets: &FleetCatalog,
        durations: &Mutex<ActionDurations>,
        now: Timestamp,
        completed: u64,
        into: &mut SchedulerSnapshot,
    ) -> Result<(), Error> {
        let clock = Arc::new(gang_clock::TestClock::new(now));
        let rates = SourceRates::new(&gang_config::ScalingConfig::default(), fleets.len(), clock)
            .expect("bounded test table");
        super::fill(
            scheduler,
            workers,
            fleets,
            durations,
            now,
            completed,
            &rates,
            &mut Scratch::default(),
            into,
        )
        .await
    }

    /// Two fleets: `fleet-a` on x86-64 and `fleet-b` on aarch64.
    pub(crate) fn catalog() -> FleetCatalog {
        let fleet = |name: &str, isa: Isa| {
            FleetProperties::new(
                &FleetName::try_new(name).expect("valid fleet name"),
                isa,
                &"docker",
                &"host:test".parse().expect("valid environment"),
                Resources {
                    vcpus: 4,
                    memory: Bytes::gib(8),
                    disk: Bytes::new(100 << 30),
                },
                &gang_types::FleetActionPolicy::default(),
                Vec::<(&'static str, String)>::new(),
            )
        };
        FleetCatalog::try_new(vec![
            fleet("fleet-a", Isa::X86_64),
            fleet("fleet-b", Isa::Aarch64),
        ])
        .expect("valid catalog")
    }

    fn candidates(entry: &NativeActionInfo) -> Vec<usize> {
        queued_demand(entry, &catalog()).candidates.iter().collect()
    }

    #[test]
    fn managed_worker_converts_every_field() {
        let mut listing = summary(WORKER);
        listing.running_actions = 2;
        listing.max_inflight_tasks = 8;
        listing.is_draining = true;
        listing.last_update_timestamp = 1_700_000_000;

        let state = worker_state(&listing).expect("managed worker converts");

        assert_eq!(
            state,
            WorkerState {
                id: WorkerId::try_new(WORKER).expect("valid worker id"),
                fleet: FleetName::try_new("fleet-a").expect("valid fleet name"),
                environment: Some("host:test".parse().expect("valid environment")),
                running_actions: 2,
                max_inflight_actions: NonZeroU32::new(8),
                draining: true,
                paused: false,
                last_seen: Timestamp::from_unix_millis(1_700_000_000_000),
            }
        );
    }

    #[test]
    fn non_gang_worker_is_skipped() {
        assert_eq!(worker_state(&summary(LEGACY_WORKER)), None);
    }

    #[test]
    fn invalid_worker_id_is_skipped() {
        assert_eq!(worker_state(&summary("")), None);
    }

    #[test]
    fn worker_without_a_valid_fleet_is_skipped() {
        let mut missing = summary(WORKER);
        missing.platform_properties.clear();
        assert_eq!(worker_state(&missing), None);

        let mut invalid = summary(WORKER);
        invalid.platform_properties = props(&[(property::FLEET, "Not A Fleet")]);
        assert_eq!(worker_state(&invalid), None);
    }

    #[test]
    fn unknown_worker_environment_preserves_connected_state() {
        let mut worker = summary(WORKER);
        worker.running_actions = 2;
        worker.is_paused = true;
        worker.is_draining = true;
        worker
            .platform_properties
            .remove(property::ACTION_ENVIRONMENT);
        for environment in [None, Some("legacy")] {
            if let Some(value) = environment {
                worker
                    .platform_properties
                    .insert(property::ACTION_ENVIRONMENT.to_owned(), value.to_owned());
            }
            let state = worker_state(&worker).expect("managed worker remains connected");
            assert_eq!(
                state.id,
                WorkerId::try_new(WORKER).expect("valid worker id")
            );
            assert_eq!(
                state.fleet,
                FleetName::try_new("fleet-a").expect("valid fleet")
            );
            assert_eq!(state.environment, None);
            assert_eq!(state.running_actions, 2);
            assert!(state.paused);
            assert!(state.draining);
            assert_eq!(state.last_seen, Timestamp::from_unix_millis(100_000));
        }
    }

    #[test]
    fn demand_ignores_old_environment_label() {
        let entry = demand(
            0,
            &[
                (property::ISA, "aarch64"),
                (property::ACTION_ENVIRONMENT, "host:old"),
            ],
        );
        let demand = queued_demand(&entry, &catalog());
        assert_eq!(demand.candidates.iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn old_environment_demand_keeps_authenticated_source() {
        let entry = demand(0, &[(property::ACTION_ENVIRONMENT, "host:old")]);
        let source = SourceId::from_identity_str("local:alice");
        let demand = super::queued_demand(&entry, &catalog(), None, source);
        assert_eq!(demand.source, source);
        assert_eq!(demand.candidates.iter().collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn missing_or_arbitrary_environment_label_preserves_queue_and_source() {
        let mut entry = demand(0, &[]);
        Arc::make_mut(&mut entry)
            .platform_properties
            .remove(property::ACTION_ENVIRONMENT);
        let source = SourceId::from_identity_str("local:alice");
        assert_eq!(
            super::queued_demand(&entry, &catalog(), None, source).source,
            source
        );
        Arc::make_mut(&mut entry).platform_properties.insert(
            property::ACTION_ENVIRONMENT.to_owned(),
            "invalid".to_owned(),
        );
        assert_eq!(
            super::queued_demand(&entry, &catalog(), None, source).source,
            source
        );
    }

    #[test]
    fn zero_and_oversized_max_inflight_map_to_no_limit() {
        let cases = [
            (0_u64, None),
            (u64::from(u32::MAX) + 1, None),
            (u64::MAX, None),
            (7, NonZeroU32::new(7)),
        ];
        for (reported, expected) in cases {
            let mut listing = summary(WORKER);
            listing.max_inflight_tasks = reported;
            let state = worker_state(&listing).expect("managed worker converts");
            assert_eq!(state.max_inflight_actions, expected, "reported {reported}");
        }
    }

    #[test]
    fn paused_worker_is_not_draining() {
        let mut paused = summary(WORKER);
        paused.is_paused = true;
        let state = worker_state(&paused).expect("managed worker converts");
        assert!(!state.draining);
        assert!(state.paused);

        let mut draining = summary(WORKER);
        draining.is_draining = true;
        let state = worker_state(&draining).expect("managed worker converts");
        assert!(state.draining);
    }

    #[test]
    fn seconds_convert_to_millis_and_saturate() {
        let mut listing = summary(WORKER);
        listing.last_update_timestamp = 42;
        let state = worker_state(&listing).expect("managed worker converts");
        assert_eq!(state.last_seen, Timestamp::from_unix_millis(42_000));

        listing.last_update_timestamp = u64::MAX;
        let state = worker_state(&listing).expect("managed worker converts");
        assert_eq!(state.last_seen, Timestamp::from_unix_millis(u64::MAX));
    }

    #[test]
    fn unconstrained_demand_is_accepted_by_every_fleet() {
        let entry = demand(0, &[]);
        assert_eq!(candidates(&entry), vec![0, 1]);
    }

    #[test]
    fn isa_pinned_demand_is_accepted_by_matching_fleets_only() {
        let entry = demand(0, &[(property::ISA, "aarch64")]);
        assert_eq!(candidates(&entry), vec![1]);
    }

    #[test]
    fn fleet_pinned_demand_is_accepted_by_that_fleet_only() {
        let entry = demand(0, &[(property::FLEET, "fleet-a")]);
        assert_eq!(candidates(&entry), vec![0]);
    }

    #[test]
    fn contradictory_demand_keeps_the_action_with_no_candidates() {
        let entry = demand(
            0,
            &[(property::ISA, "aarch64"), (property::FLEET, "fleet-a")],
        );
        let action = queued_demand(&entry, &catalog());
        assert_eq!(action.candidates, FleetSet::EMPTY);
        assert_eq!(action.priority, 0);
    }

    #[test]
    fn undeclared_demand_keys_have_no_candidates() {
        let entry = demand(0, &[("color", "teal")]);
        assert!(candidates(&entry).is_empty());
    }

    #[test]
    fn demand_with_resources_converts() {
        let entry = demand(
            3,
            &[
                (property::ISA, "x86-64"),
                (property::CPU_COUNT, "4"),
                (property::MEMORY_KB, "1024"),
            ],
        );

        let action = queued_demand(&entry, &catalog());

        assert_eq!(
            action,
            QueuedDemand {
                source: None,
                candidates: FleetSet::prefix(1).expect("fleet count in range"),
                resources: ActionResources {
                    cpu_millicores: 4000,
                    memory: Bytes::kib(1024),
                    disk: Bytes::new(0),
                },
                queued_since: Timestamp::from_unix_millis(5000),
                priority: 3,
                expected: None,
            }
        );
    }

    #[test]
    fn demand_cpu_uses_the_larger_reservation() {
        let millicores = demand(0, &[(property::CPU_MILLICORES, "1500")]);
        assert_eq!(
            queued_demand(&millicores, &catalog())
                .resources
                .cpu_millicores,
            1500
        );
        let both = demand(
            0,
            &[
                (property::CPU_COUNT, "4"),
                (property::CPU_MILLICORES, "250"),
            ],
        );
        assert_eq!(
            queued_demand(&both, &catalog()).resources.cpu_millicores,
            4000,
            "whole-core request must not be hidden by a smaller millicore request"
        );
        let huge = demand(0, &[(property::CPU_MILLICORES, "18446744073709551615")]);
        assert_eq!(
            queued_demand(&huge, &catalog()).resources.cpu_millicores,
            u64::MAX
        );
    }

    #[test]
    fn individual_resource_properties_are_independent() {
        let cases = [
            (
                property::MEMORY_KB,
                "1024",
                ActionResources {
                    cpu_millicores: 0,
                    memory: Bytes::kib(1024),
                    disk: Bytes::new(0),
                },
            ),
            (
                property::CPU_COUNT,
                "4",
                ActionResources {
                    cpu_millicores: 4000,
                    memory: Bytes::new(0),
                    disk: Bytes::new(0),
                },
            ),
            (
                property::DISK_KB,
                "42",
                ActionResources {
                    cpu_millicores: 0,
                    memory: Bytes::new(0),
                    disk: Bytes::kib(42),
                },
            ),
        ];
        for (key, value, expected) in cases {
            let entry = demand(0, &[(key, value)]);
            assert_eq!(queued_demand(&entry, &catalog()).resources, expected);
        }
    }

    #[test]
    fn malformed_property_zeroes_only_its_dimension() {
        let entry = demand(
            0,
            &[
                (property::CPU_COUNT, "four"),
                (property::MEMORY_KB, "1024"),
                (property::DISK_KB, "42"),
            ],
        );
        assert_eq!(
            queued_demand(&entry, &catalog()).resources,
            ActionResources {
                cpu_millicores: 0,
                memory: Bytes::kib(1024),
                disk: Bytes::kib(42),
            }
        );
    }

    #[test]
    fn memory_kib_overflow_saturates() {
        let max_memory = u64::MAX.to_string();
        let entry = demand(0, &[(property::MEMORY_KB, &max_memory)]);
        assert_eq!(
            queued_demand(&entry, &catalog()).resources,
            ActionResources {
                cpu_millicores: 0,
                memory: Bytes::new(u64::MAX),
                disk: Bytes::new(0),
            }
        );
    }

    #[test]
    fn disk_kb_converts_to_bytes() {
        let entry = demand(
            0,
            &[
                (property::CPU_COUNT, "4"),
                (property::MEMORY_KB, "1024"),
                (property::DISK_KB, "52428800"),
            ],
        );

        let action = queued_demand(&entry, &catalog());

        assert_eq!(
            action.resources,
            ActionResources {
                cpu_millicores: 4000,
                memory: Bytes::kib(1024),
                disk: Bytes::new(50 << 30),
            }
        );
    }

    #[test]
    fn missing_or_malformed_disk_is_zero() {
        let base = [(property::CPU_COUNT, "4"), (property::MEMORY_KB, "1024")];
        let absent = demand(0, &base);
        let malformed = demand(0, &[base[0], base[1], (property::DISK_KB, "fifty")]);

        for entry in [absent, malformed] {
            let action = queued_demand(&entry, &catalog());
            assert_eq!(
                action.resources,
                ActionResources {
                    cpu_millicores: 4000,
                    memory: Bytes::kib(1024),
                    disk: Bytes::new(0),
                }
            );
        }
    }

    #[test]
    fn disk_kib_overflow_saturates() {
        let max_disk = u64::MAX.to_string();
        let entry = demand(0, &[(property::DISK_KB, &max_disk)]);
        assert_eq!(
            queued_demand(&entry, &catalog()).resources.disk,
            Bytes::new(u64::MAX)
        );
    }

    fn populated_listing() -> ([WorkerSummary; 3], Fake) {
        let workers = [
            WorkerSummary {
                running_actions: 1,
                ..summary(OTHER_WORKER)
            },
            WorkerSummary {
                running_actions: 1,
                ..summary(LEGACY_WORKER)
            },
            WorkerSummary {
                running_actions: 1,
                ..summary(WORKER)
            },
        ];
        let fake = Fake {
            queued: vec![
                row(
                    "q1",
                    ActionStage::Queued,
                    0,
                    info(9, 1_000, &[(property::ISA, "aarch64")]),
                    Some(bazel("//app:bin", "Compile")),
                ),
                row("q2", ActionStage::Queued, 0, info(9, 2_000, &[]), None),
                row(
                    "q3",
                    ActionStage::Queued,
                    0,
                    info(1, 3_000, &[(property::FLEET, "missing")]),
                    None,
                ),
            ],
            by_worker: HashMap::from([
                (
                    OTHER_WORKER.to_owned(),
                    vec![row(
                        "r2",
                        ActionStage::Executing,
                        4_000,
                        info(0, 0, &[]),
                        None,
                    )],
                ),
                (
                    WORKER.to_owned(),
                    vec![row(
                        "r1",
                        ActionStage::Executing,
                        5_000,
                        info(0, 0, &[]),
                        Some(bazel("//app:bin", "Compile")),
                    )],
                ),
            ]),
            ..Default::default()
        };
        (workers, fake)
    }

    #[tokio::test]
    async fn fill_retains_running_workers_with_unknown_environments() {
        let (mut workers, fake) = populated_listing();
        workers[0]
            .platform_properties
            .remove(property::ACTION_ENVIRONMENT);
        workers[2]
            .platform_properties
            .insert(property::ACTION_ENVIRONMENT.to_owned(), "legacy".to_owned());
        let mut snapshot = SchedulerSnapshot::default();
        fill(
            &fake,
            &workers,
            &catalog(),
            &history(),
            Timestamp::from_unix_millis(10_000),
            0,
            &mut snapshot,
        )
        .await
        .expect("snapshot");
        assert_eq!(snapshot.workers.len(), 2);
        assert!(
            snapshot
                .workers
                .iter()
                .all(|worker| { worker.environment.is_none() && worker.running_actions == 1 })
        );
        assert_eq!(snapshot.executing.len(), 2);
        assert_eq!(snapshot.executing[0].worker, 0);
        assert_eq!(snapshot.executing[1].worker, 1);
    }

    #[tokio::test]
    async fn fill_maps_metadata_and_sorted_workers() {
        let now = Timestamp::from_unix_millis(10_000);
        let history = history();
        history.lock().record("//app:bin", "Compile", 2_000, now);
        let (workers, fake) = populated_listing();
        let mut into = SchedulerSnapshot::default();
        fill(&fake, &workers, &catalog(), &history, now, 5, &mut into)
            .await
            .expect("snapshot");
        assert_eq!(into.completed, 5);
        assert_eq!(
            into.workers
                .iter()
                .map(|w| w.id.as_str())
                .collect::<Vec<_>>(),
            vec![WORKER, OTHER_WORKER]
        );
        assert_eq!(
            into.queued.iter().map(|q| q.priority).collect::<Vec<_>>(),
            vec![9, 9, 1]
        );
        assert_eq!(
            into.queued[0].queued_since,
            Timestamp::from_unix_millis(1_000)
        );
        assert_eq!(
            into.queued[0].candidates.iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(into.queued[0].expected, Some(Duration::from_secs(2)));
        assert_eq!(into.queued[1].expected, None);
        assert!(into.queued[2].candidates.is_empty());
        assert_eq!(
            into.executing,
            vec![
                ExecutingDemand {
                    worker: 0,
                    source: None,
                    candidates: FleetSet::prefix(2).expect("two fleets"),
                    resources: ActionResources::default(),
                    started: Timestamp::from_unix_millis(5_000),
                    expected: Some(Duration::from_secs(2))
                },
                ExecutingDemand {
                    worker: 1,
                    source: None,
                    candidates: FleetSet::prefix(2).expect("two fleets"),
                    resources: ActionResources::default(),
                    started: Timestamp::from_unix_millis(4_000),
                    expected: None
                },
            ]
        );
        let calls = fake.calls();
        assert_eq!(
            calls[0].order_by_priority_direction,
            Some(OrderDirection::Desc)
        );
    }

    #[tokio::test]
    async fn fill_overwrites_and_reuses_all_buffers() {
        let now = Timestamp::from_unix_millis(10_000);
        let history = history();
        let (workers, fake) = populated_listing();
        let mut into = SchedulerSnapshot::default();
        fill(&fake, &workers, &catalog(), &history, now, 5, &mut into)
            .await
            .expect("snapshot");
        let capacities = (
            into.workers.capacity(),
            into.queued.capacity(),
            into.executing.capacity(),
        );
        fill(
            &Fake::default(),
            &[summary(OTHER_WORKER)],
            &catalog(),
            &history,
            now,
            6,
            &mut into,
        )
        .await
        .expect("snapshot");
        assert_eq!(into.completed, 6);
        assert_eq!(into.workers.len(), 1);
        assert!(into.queued.is_empty());
        assert!(into.executing.is_empty());
        assert!(into.workers.capacity() >= capacities.0);
        assert!(into.queued.capacity() >= capacities.1);
        assert!(into.executing.capacity() >= capacities.2);
    }

    #[tokio::test]
    async fn vanished_records_are_skipped_and_other_errors_propagate_in_both_views() {
        use gang_spi::{ActionsView, ListingLimits};
        use nativelink_error::Code;
        let now = Timestamp::from_unix_millis(10_000);
        let history = history();
        let fleets = catalog();
        let mut state_missing = row("missing", ActionStage::Queued, 0, info(0, 0, &[]), None);
        state_missing.state_error = Some(Code::NotFound);
        let mut info_invalid = row("invalid", ActionStage::Queued, 0, info(0, 0, &[]), None);
        info_invalid.info_error = Some(Code::InvalidArgument);
        let good = row("good", ActionStage::Queued, 0, info(0, 0, &[]), None);
        let fake = Fake {
            queued: vec![state_missing, info_invalid, good],
            ..Default::default()
        };
        let mut snapshot = SchedulerSnapshot::default();
        fill(&fake, &[], &fleets, &history, now, 0, &mut snapshot)
            .await
            .expect("skip vanished");
        assert_eq!(snapshot.queued.len(), 1);
        let mut view = ActionsView::default();
        let limits = ListingLimits {
            running: 10,
            queued: 10,
        };
        actions::list(&fake, &[], &fleets, limits, &mut view)
            .await
            .expect("skip vanished");
        assert_eq!(view.queued.len(), 1);

        for state_read in [true, false] {
            let mut bad = row("bad", ActionStage::Queued, 0, info(0, 0, &[]), None);
            if state_read {
                bad.state_error = Some(Code::Internal);
            } else {
                bad.info_error = Some(Code::Internal);
            }
            let fake = Fake {
                queued: vec![bad],
                ..Default::default()
            };
            assert!(
                fill(&fake, &[], &fleets, &history, now, 0, &mut snapshot)
                    .await
                    .is_err()
            );
            assert!(
                actions::list(&fake, &[], &fleets, limits, &mut view)
                    .await
                    .is_err()
            );
        }
        let fake = Fake {
            filter_error: Some(Code::Internal),
            ..Default::default()
        };
        assert!(
            fill(&fake, &[], &fleets, &history, now, 0, &mut snapshot)
                .await
                .is_err()
        );
        assert!(
            actions::list(&fake, &[], &fleets, limits, &mut view)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn executing_reads_skip_vanished_and_propagate_other_errors() {
        use gang_spi::{ActionsView, ListingLimits};
        use nativelink_error::Code;
        let now = Timestamp::from_unix_millis(10_000);
        let history = history();
        let fleets = catalog();
        let workers = [WorkerSummary {
            running_actions: 3,
            ..summary(WORKER)
        }];
        let limits = ListingLimits {
            running: 10,
            queued: 10,
        };
        for code in [Code::NotFound, Code::InvalidArgument, Code::Internal] {
            for state_read in [true, false] {
                let mut bad = row("bad", ActionStage::Executing, 0, info(0, 0, &[]), None);
                if state_read {
                    bad.state_error = Some(code);
                } else {
                    bad.info_error = Some(code);
                }
                let good = row("good", ActionStage::Executing, 1, info(0, 0, &[]), None);
                let fake = Fake {
                    by_worker: HashMap::from([(WORKER.to_owned(), vec![bad, good])]),
                    ..Default::default()
                };
                let mut snapshot = SchedulerSnapshot::default();
                let mut view = ActionsView::default();
                let snapshot_result =
                    fill(&fake, &workers, &fleets, &history, now, 0, &mut snapshot).await;
                let view_result = actions::list(&fake, &workers, &fleets, limits, &mut view).await;
                if code == Code::Internal {
                    assert!(snapshot_result.is_err());
                    assert!(view_result.is_err());
                } else {
                    snapshot_result.expect("vanished executing action");
                    view_result.expect("vanished executing action");
                    assert_eq!(snapshot.executing.len(), 1);
                    assert_eq!(view.running.len(), 1);
                }
            }
        }
    }
    #[derive(Clone)]
    struct Retained {
        state: Arc<ActionState>,
        info: Arc<NativeActionInfo>,
        origin: Option<OriginMetadata>,
    }

    #[async_trait::async_trait]
    impl nativelink_util::operation_state_manager::ActionStateResult for Retained {
        async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            Ok((Arc::clone(&self.state), self.origin.clone()))
        }

        async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
            self.as_state().await
        }

        async fn as_action_info(
            &self,
        ) -> Result<(Arc<NativeActionInfo>, Option<OriginMetadata>), Error> {
            Ok((Arc::clone(&self.info), self.origin.clone()))
        }
    }

    fn retained(id: &str, stage: ActionStage, identity: Option<&str>, priority: i32) -> Retained {
        let info = info(
            priority,
            0,
            &[
                (property::CPU_MILLICORES, "2000"),
                (property::ACTION_ENVIRONMENT, "host:old"),
            ],
        );
        Retained {
            state: Arc::new(ActionState {
                stage,
                client_operation_id: OperationId::String(id.to_owned()),
                action_digest: info.digest(),
                last_transition_timestamp: std::time::UNIX_EPOCH,
            }),
            info,
            origin: identity.map(|identity| OriginMetadata {
                identity: identity.to_owned(),
                bazel_metadata: None,
            }),
        }
    }

    #[derive(Default)]
    struct Listings {
        queued: Vec<Retained>,
        executing: HashMap<String, Vec<Retained>>,
        during_queue: Option<(Arc<SourceRates>, Arc<gang_clock::TestClock>)>,
        park_queue: bool,
    }

    impl nativelink_metric::MetricsComponent for Listings {
        fn publish(
            &self,
            _: nativelink_metric::MetricKind,
            _: nativelink_metric::MetricFieldData<'_>,
        ) -> Result<nativelink_metric::MetricPublishKnownKindData, nativelink_metric::Error>
        {
            Ok(nativelink_metric::MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait::async_trait]
    impl ClientStateManager for Listings {
        async fn add_action(
            &self,
            _: OperationId,
            _: Arc<NativeActionInfo>,
        ) -> Result<Box<dyn nativelink_util::operation_state_manager::ActionStateResult>, Error>
        {
            Err(nativelink_error::make_err!(
                nativelink_error::Code::Unimplemented,
                "listing only"
            ))
        }

        async fn filter_operations(
            &self,
            filter: nativelink_util::operation_state_manager::OperationFilter,
        ) -> Result<nativelink_util::operation_state_manager::ActionStateResultStream<'_>, Error>
        {
            let queued = filter.stages == OperationStageFlags::Queued;
            if queued && let Some((rates, clock)) = &self.during_queue {
                clock.advance(Duration::from_secs(2));
                rates.record(
                    None,
                    0,
                    ActionResources::default(),
                    Some(Duration::from_secs(1)),
                );
            }
            let rows = if queued {
                self.queued.clone()
            } else {
                filter
                    .worker_id
                    .as_ref()
                    .and_then(|worker| self.executing.get(&worker.0))
                    .cloned()
                    .unwrap_or_default()
            };
            let stream = futures::stream::iter(rows.into_iter().map(|row| {
                Box::new(row)
                    as Box<dyn nativelink_util::operation_state_manager::ActionStateResult>
            }));
            if queued && self.park_queue {
                Ok(Box::pin(stream.chain(futures::stream::pending())))
            } else {
                Ok(Box::pin(stream))
            }
        }
    }

    fn rate_fixture() -> (Arc<SourceRates>, Arc<gang_clock::TestClock>) {
        let clock = Arc::new(gang_clock::TestClock::new(Timestamp::from_unix_millis(
            100_000,
        )));
        let rates = Arc::new(
            SourceRates::new(&gang_config::ScalingConfig::default(), 2, clock.clone())
                .expect("bounded table"),
        );
        (rates, clock)
    }

    #[tokio::test]
    async fn retained_sources_do_not_require_bazel_metadata_or_invocations() {
        let (rates, clock) = rate_fixture();
        let mut same_a = retained("a", ActionStage::Queued, Some("local:alice"), 5);
        same_a.origin.as_mut().expect("origin").bazel_metadata = Some(bazel("//a", "Compile"));
        let mut same_b = same_a.clone();
        same_b.state = retained("b", ActionStage::Queued, None, 5).state;
        same_b
            .origin
            .as_mut()
            .expect("origin")
            .bazel_metadata
            .as_mut()
            .expect("metadata")
            .tool_invocation_id = "different-invocation".to_owned();
        let listing = Listings {
            queued: vec![
                same_a,
                same_b,
                retained("no-metadata", ActionStage::Queued, Some("local:alice"), 4),
                retained("other", ActionStage::Queued, Some("oidc:alice"), 3),
                retained("missing", ActionStage::Queued, None, 2),
                retained("empty", ActionStage::Queued, Some(""), 1),
            ],
            ..Default::default()
        };
        let mut snapshot = SchedulerSnapshot::default();
        super::fill(
            &listing,
            &[],
            &catalog(),
            &history(),
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut Scratch::default(),
            &mut snapshot,
        )
        .await
        .expect("snapshot");
        let alice = SourceId::from_identity_str("local:alice");
        assert!(alice.is_some());
        assert!(snapshot.queued[..3].iter().all(|row| row.source == alice));
        assert_ne!(snapshot.queued[3].source, alice);
        assert_eq!(snapshot.queued[4].source, None);
        assert_eq!(snapshot.queued[5].source, None);
        assert_eq!(snapshot.queued[2].resources.cpu_millicores, 2000);
    }

    #[tokio::test]
    async fn queue_to_execution_and_worker_moves_collapse_to_latest_execution() {
        let (rates, clock) = rate_fixture();
        let moving = "same-long-string-prefix-but-distinct-operation-moving";
        let other = "same-long-string-prefix-but-distinct-operation-other";
        let listing = Listings {
            queued: vec![
                retained("first", ActionStage::Queued, None, 9),
                retained(moving, ActionStage::Queued, Some("local:alice"), 8),
                retained(other, ActionStage::Queued, None, 7),
                retained("last", ActionStage::Queued, None, 6),
            ],
            executing: HashMap::from([
                (
                    WORKER.to_owned(),
                    vec![retained(
                        moving,
                        ActionStage::Executing,
                        Some("local:alice"),
                        0,
                    )],
                ),
                (
                    OTHER_WORKER.to_owned(),
                    vec![retained(
                        moving,
                        ActionStage::Executing,
                        Some("local:alice"),
                        0,
                    )],
                ),
            ]),
            ..Default::default()
        };
        let workers = [
            WorkerSummary {
                running_actions: 1,
                ..summary(WORKER)
            },
            WorkerSummary {
                running_actions: 1,
                ..summary(OTHER_WORKER)
            },
        ];
        let mut snapshot = SchedulerSnapshot::default();
        let mut scratch = Scratch::default();
        super::fill(
            &listing,
            &workers,
            &catalog(),
            &history(),
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut scratch,
            &mut snapshot,
        )
        .await
        .expect("snapshot");
        assert_eq!(
            snapshot
                .queued
                .iter()
                .map(|row| row.priority)
                .collect::<Vec<_>>(),
            [9, 7, 6]
        );
        assert_eq!(snapshot.executing.len(), 1);
        assert_eq!(snapshot.executing[0].worker, 1);
        assert_eq!(
            snapshot.executing[0].source,
            SourceId::from_identity_str("local:alice")
        );
        assert_eq!(snapshot.executing[0].resources.cpu_millicores, 2000);
        assert_eq!(
            snapshot.executing[0].candidates,
            FleetSet::prefix(2).expect("two fleets")
        );
        assert!(scratch.rows.is_empty());
    }

    #[tokio::test]
    async fn row_read_precedes_listings_and_uses_already_advanced_shared_clock() {
        let (rates, clock) = rate_fixture();
        clock.advance(Duration::from_hours(4));
        let before = gang_clock::Clock::monotonic(&*clock);
        let listing = Listings {
            during_queue: Some((Arc::clone(&rates), Arc::clone(&clock))),
            ..Default::default()
        };
        let mut snapshot = SchedulerSnapshot::default();
        let mut scratch = Scratch::default();
        super::fill(
            &listing,
            &[],
            &catalog(),
            &history(),
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut scratch,
            &mut snapshot,
        )
        .await
        .expect("first frame");
        assert_eq!(snapshot.source_rates_at, before);
        assert!(snapshot.source_rates.is_empty());
        super::fill(
            &Listings::default(),
            &[],
            &catalog(),
            &history(),
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut scratch,
            &mut snapshot,
        )
        .await
        .expect("next frame");
        assert_eq!(snapshot.source_rates.len(), 1);
        assert_eq!(
            snapshot.source_rates[0].created_at,
            before + Duration::from_secs(2)
        );
        assert_eq!(snapshot.source_rates[0].at, snapshot.source_rates_at);
    }

    #[tokio::test]
    async fn cancelled_listing_releases_arc_keys_and_next_fill_overwrites_rows() {
        let (rates, clock) = rate_fixture();
        let operation = retained("cancelled", ActionStage::Queued, None, 9);
        let state = Arc::clone(&operation.state);
        let listing = Listings {
            queued: vec![operation],
            park_queue: true,
            ..Default::default()
        };
        let history = history();
        let fleets = catalog();
        let mut snapshot = SchedulerSnapshot::default();
        let mut scratch = Scratch::default();
        let mut future = Box::pin(super::fill(
            &listing,
            &[],
            &fleets,
            &history,
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut scratch,
            &mut snapshot,
        ));
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert!(Arc::strong_count(&state) > 2);
        drop(future);
        assert_eq!(Arc::strong_count(&state), 2);
        assert!(scratch.rows.is_empty());
        assert!(scratch.queued_live.is_empty());
        let capacities = (scratch.rows.capacity(), scratch.queued_live.capacity());
        super::fill(
            &Listings::default(),
            &[],
            &fleets,
            &history,
            gang_clock::Clock::wall(&*clock),
            0,
            &rates,
            &mut scratch,
            &mut snapshot,
        )
        .await
        .expect("clean next frame");
        assert!(snapshot.queued.is_empty());
        assert!(snapshot.executing.is_empty());
        assert_eq!(
            (scratch.rows.capacity(), scratch.queued_live.capacity()),
            capacities
        );
    }

    #[test]
    fn full_string_state_keys_reuse_arc_and_map_storage_without_allocating() {
        let a = retained("full-string-id-A", ActionStage::Queued, None, 0);
        let b = retained("full-string-id-B", ActionStage::Queued, None, 0);
        let mut scratch = Scratch::default();
        scratch.rows.reserve(2);
        let measured = allocation_counter::measure(|| {
            scratch
                .rows
                .insert(StateKey(Arc::clone(&a.state)), RowLocation::Queued(0));
            scratch
                .rows
                .insert(StateKey(Arc::clone(&b.state)), RowLocation::Queued(1));
            assert!(matches!(
                scratch.rows.get(&a.state.client_operation_id),
                Some(RowLocation::Queued(0))
            ));
            assert!(matches!(
                scratch.rows.get(&b.state.client_operation_id),
                Some(RowLocation::Queued(1))
            ));
            scratch.rows.clear();
        });
        assert_eq!(measured.count_total, 0);
    }
    #[test]
    fn candidates_use_original_floor_and_cpu_before_memory_promotion() {
        let fleets = FleetCatalog::try_new(
            [("small", 16, 32), ("large", 8, 64)]
                .into_iter()
                .map(|(name, vcpus, memory)| {
                    FleetProperties::new(
                        &FleetName::try_new(name).unwrap(),
                        Isa::X86_64,
                        &"docker",
                        &"host:test".parse().unwrap(),
                        Resources {
                            vcpus,
                            memory: Bytes::gib(memory),
                            disk: Bytes::gib(100),
                        },
                        &gang_types::FleetActionPolicy::default(),
                        [],
                    )
                })
                .collect(),
        )
        .unwrap();
        let mut action = demand(
            0,
            &[
                (property::MEMORY_KB, "33554432"),
                (property::CPU_MILLICORES, "16000"),
            ],
        );
        assert_eq!(
            queued_demand(&action, &fleets)
                .candidates
                .iter()
                .collect::<Vec<_>>(),
            vec![0]
        );
        Arc::make_mut(&mut action).memory_reservation =
            Some(nativelink_util::action_messages::MemoryReservation {
                minimum_kb: Some(40 * 1024 * 1024),
                desired_kb: Some(70 * 1024 * 1024),
                cpu_before_memory_fit: Some(1000),
                fitted_revision: None,
            });
        assert_eq!(
            queued_demand(&action, &fleets)
                .candidates
                .iter()
                .collect::<Vec<_>>(),
            vec![1]
        );
        Arc::make_mut(&mut action)
            .memory_reservation
            .as_mut()
            .unwrap()
            .cpu_before_memory_fit = Some(12000);
        assert!(queued_demand(&action, &fleets).candidates.is_empty());
        let reservation = Arc::make_mut(&mut action)
            .memory_reservation
            .as_mut()
            .unwrap();
        reservation.minimum_kb = None;
        reservation.cpu_before_memory_fit = None;
        assert_eq!(
            queued_demand(&action, &fleets).candidates,
            FleetSet::prefix(2).unwrap()
        );
        Arc::make_mut(&mut action)
            .memory_reservation
            .as_mut()
            .unwrap()
            .minimum_kb = Some(0);
        assert_eq!(
            queued_demand(&action, &fleets).candidates,
            FleetSet::prefix(2).unwrap()
        );
    }

    #[tokio::test]
    async fn snapshot_reports_current_physical_memory_not_action_ceiling_and_reuses_buffer() {
        let mut policy = gang_types::FleetActionPolicy::default();
        policy.resources.memory = Some(NonZeroBytes::gib(4));
        let fleets = FleetCatalog::try_new(vec![FleetProperties::new(
            &FleetName::try_new("measured").unwrap(),
            Isa::X86_64,
            &"docker",
            &"host:test".parse().unwrap(),
            Resources {
                vcpus: 16,
                memory: Bytes::gib(63),
                disk: Bytes::gib(100),
            },
            &policy,
            [],
        )])
        .unwrap();
        let (sender, receiver) = tokio::sync::watch::channel(Arc::new(
            nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot::new(Vec::new()),
        ));
        let runtime = crate::capacity::RuntimeCatalog::new(fleets, receiver);
        let mut snapshot = SchedulerSnapshot::default();
        let mut buffer = None;
        for memory_kb in [62_229 * 1024, 61 * 1024 * 1024] {
            let mut row = nativelink_config::schedulers::ResourceCapacity::default();
            row.exact
                .insert(property::FLEET.to_owned(), "measured".to_owned());
            row.minimum
                .insert(property::MEMORY_KB.to_owned(), memory_kb);
            sender
                .send(Arc::new(
                    nativelink_scheduler::worker_scheduler::ResourceCapacitySnapshot::new(vec![
                        row,
                    ]),
                ))
                .unwrap();
            let current = runtime.current().unwrap();
            fill(
                &Fake::default(),
                &[],
                &current,
                &history(),
                Timestamp::from_unix_millis(0),
                0,
                &mut snapshot,
            )
            .await
            .unwrap();
            assert_eq!(
                snapshot.fleet_memory,
                vec![FleetMemoryCapacity {
                    fleet: 0,
                    capacity: NonZeroBytes::new(Bytes::kib(memory_kb)).unwrap(),
                }]
            );
            assert_eq!(
                current.fleets()[0].limit(property::MEMORY_KB),
                Some(4 * 1024 * 1024)
            );
            if let Some(pointer) = buffer {
                assert_eq!(snapshot.fleet_memory.as_ptr(), pointer);
            }
            buffer = Some(snapshot.fleet_memory.as_ptr());
        }
    }
}
