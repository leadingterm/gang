//! Conversion of `NativeLink`'s typed listings into the scheduler snapshot Gang reads.

use std::collections::HashMap;
use std::num::NonZeroU32;

use gang_clock::Timestamp;
use gang_spi::{QueuedDemand, SchedulerSnapshot, WorkerState};
use gang_types::{Bytes, FleetCatalog, FleetName, Resources, WorkerId, property};
use nativelink_scheduler::admin::QueuedDemand as NativeQueuedDemand;
use nativelink_scheduler::worker_scheduler::WorkerSummary;

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
/// Returns `None` for workers Gang does not manage: those whose id is not a valid worker id
/// naming an instance, and those whose `fleet` property is missing or is not a valid fleet
/// name. `NativeLink` reports `max_inflight_tasks` 0 for a worker without a limit, which maps to
/// `None`, as do counts above `u32::MAX`. A paused worker is not draining: only the draining
/// flag counts; the pause itself is reported by `paused`.
fn worker_state(summary: &WorkerSummary) -> Option<WorkerState> {
    let (id, fleet) = managed_worker(summary)?;
    let max_inflight_actions = u32::try_from(summary.max_inflight_tasks)
        .ok()
        .and_then(NonZeroU32::new);
    Some(WorkerState {
        id,
        fleet,
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

/// Converts a queued operation into a scheduler demand.
///
/// The action's candidates are the fleets that accept its platform properties. An action no
/// fleet accepts is kept with empty candidates and counts for nothing in the attribution.
/// Each resource dimension is zero when the corresponding property is missing or malformed.
fn queued_demand(demand: &NativeQueuedDemand, fleets: &FleetCatalog) -> QueuedDemand {
    QueuedDemand {
        candidates: fleets.accepting(|key| demand.platform_properties.get(key).map(String::as_str)),
        resources: resources(&demand.platform_properties),
        queued_since: Timestamp::from_unix_millis(demand.queued_since_ms),
        priority: demand.priority,
    }
}

/// Reads the resources an action requests from its platform properties.
///
/// Missing or malformed properties request zero. Memory and disk KiB values are converted to
/// bytes with saturation.
fn resources(properties: &HashMap<String, String>) -> Resources {
    fn parse<T: std::str::FromStr + Default>(properties: &HashMap<String, String>, key: &str) -> T {
        properties
            .get(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or_default()
    }

    let memory_kb: u64 = parse(properties, property::MEMORY_KB);
    let disk_kb: u64 = parse(properties, property::DISK_KB);
    Resources {
        vcpus: parse(properties, property::CPU_COUNT),
        memory: Bytes::new(memory_kb.saturating_mul(Bytes::kib(1).get())),
        disk: Bytes::new(disk_kb.saturating_mul(Bytes::kib(1).get())),
    }
}

/// Overwrites `into` with managed workers sorted by id, every queued demand in input order,
/// and the completed count. Existing buffers are cleared and reused, so their capacity is kept.
pub(crate) fn fill(
    workers: &[WorkerSummary],
    demand: &[NativeQueuedDemand],
    fleets: &FleetCatalog,
    completed: u64,
    into: &mut SchedulerSnapshot,
) {
    into.completed = completed;
    into.workers.clear();
    into.workers.extend(workers.iter().filter_map(worker_state));
    into.workers.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    into.queued.clear();
    into.queued
        .extend(demand.iter().map(|entry| queued_demand(entry, fleets)));
}

#[cfg(test)]
mod tests {
    use gang_types::{FleetProperties, FleetSet, Isa};

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
            platform_properties: props(&[(property::FLEET, "fleet-a")]),
            available_platform_properties: HashMap::new(),
            free_memory_kb: None,
        }
    }

    fn demand(priority: i32, pairs: &[(&str, &str)]) -> NativeQueuedDemand {
        NativeQueuedDemand {
            operation_id: format!("op-{priority}"),
            queued_since_ms: 5000,
            priority,
            platform_properties: props(pairs),
            client_last_seen_ms: None,
        }
    }

    /// Two fleets: `fleet-a` on x86-64 and `fleet-b` on aarch64.
    fn catalog() -> FleetCatalog {
        let fleet = |name: &str, isa: Isa| {
            FleetProperties::new(
                &FleetName::try_new(name).expect("valid fleet name"),
                isa,
                &"docker",
                Resources {
                    vcpus: 4,
                    memory: Bytes::gib(8),
                    disk: Bytes::new(100 << 30),
                },
                Vec::<(&'static str, String)>::new(),
            )
        };
        FleetCatalog::try_new(vec![
            fleet("fleet-a", Isa::X86_64),
            fleet("fleet-b", Isa::Aarch64),
        ])
        .expect("valid catalog")
    }

    fn candidates(entry: &NativeQueuedDemand) -> Vec<usize> {
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
    fn undeclared_demand_keys_are_ignored() {
        let entry = demand(0, &[("color", "teal")]);
        assert_eq!(candidates(&entry), vec![0, 1]);
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
                candidates: FleetSet::prefix(1).expect("fleet count in range"),
                resources: Resources {
                    vcpus: 4,
                    memory: Bytes::kib(1024),
                    disk: Bytes::new(0),
                },
                queued_since: Timestamp::from_unix_millis(5000),
                priority: 3,
            }
        );
    }

    #[test]
    fn individual_resource_properties_are_independent() {
        let cases = [
            (
                property::MEMORY_KB,
                "1024",
                Resources {
                    vcpus: 0,
                    memory: Bytes::kib(1024),
                    disk: Bytes::new(0),
                },
            ),
            (
                property::CPU_COUNT,
                "4",
                Resources {
                    vcpus: 4,
                    memory: Bytes::new(0),
                    disk: Bytes::new(0),
                },
            ),
            (
                property::DISK_KB,
                "42",
                Resources {
                    vcpus: 0,
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
            Resources {
                vcpus: 0,
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
            Resources {
                vcpus: 0,
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
            Resources {
                vcpus: 4,
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
                Resources {
                    vcpus: 4,
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

    #[test]
    fn fill_clears_previous_content_preserves_order_and_reuses_capacity() {
        let first_workers = [
            WorkerSummary {
                running_actions: 1,
                ..summary(OTHER_WORKER)
            },
            WorkerSummary {
                running_actions: 2,
                ..summary(LEGACY_WORKER)
            },
            WorkerSummary {
                running_actions: 3,
                ..summary(WORKER)
            },
        ];
        let first_demand = [
            demand(1, &[(property::ISA, "aarch64")]),
            demand(2, &[]),
            demand(3, &[(property::FLEET, "missing")]),
            demand(4, &[(property::ISA, "x86-64")]),
        ];
        let fleets = catalog();
        let mut into = SchedulerSnapshot::default();
        fill(&first_workers, &first_demand, &fleets, 5, &mut into);
        assert_eq!(into.completed, 5);

        let running: Vec<u32> = into.workers.iter().map(|w| w.running_actions).collect();
        assert_eq!(
            running,
            vec![3, 1],
            "unmanaged worker is left out and workers are sorted"
        );
        let priorities: Vec<i32> = into.queued.iter().map(|a| a.priority).collect();
        assert_eq!(priorities, vec![1, 2, 3, 4], "every action is kept");
        assert!(into.queued[2].candidates.is_empty(), "no fleet accepts it");
        let worker_capacity = into.workers.capacity();
        let queued_capacity = into.queued.capacity();

        let second_workers = [WorkerSummary {
            running_actions: 9,
            ..summary(OTHER_WORKER)
        }];
        fill(&second_workers, &[], &fleets, 6, &mut into);
        assert_eq!(into.completed, 6);

        assert_eq!(into.workers.len(), 1);
        assert_eq!(into.workers[0].running_actions, 9);
        assert_eq!(into.queued.len(), 0);
        assert!(into.workers.capacity() >= worker_capacity);
        assert!(into.queued.capacity() >= queued_capacity);
    }

    #[test]
    fn fill_sorts_workers_by_id() {
        let workers = [summary(OTHER_WORKER), summary(WORKER)];
        let mut into = SchedulerSnapshot::default();
        fill(&workers, &[], &catalog(), 0, &mut into);
        let ids: Vec<&str> = into
            .workers
            .iter()
            .map(|worker| worker.id.as_str())
            .collect();
        assert_eq!(ids, vec![WORKER, OTHER_WORKER]);
    }
}
