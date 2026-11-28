//! Unified operation admission pipeline (`§5.3`).
//!
//! Replaces the three-stage decorator chain (`PlaceableActions → HistoricalResourceScheduler →
//! FittedActions`) with a single pure admission step:
//! 1. Validate original client properties against the current capacity catalog.
//! 2. Compute `QoS` CPU weight and optional gate-tier audit marker.
//! 3. Look up learned resource reservations from [`ResourceSketch`] (or cold-start defaults).
//! 4. Fit reservations to admitted fleets and current physical capacity facts.
//! 5. Deduplicate (with priority promotion) or insert into [`Operations`] and [`OperationQueue`].

use std::collections::BTreeMap;

use gang_config::ActionsConfig;
use gang_types::{
    ActionResources, BuildId, CpuWeight, Digest, Mnemonic, OperationName, Refusal, SourceId,
    TargetLabel,
};
use thiserror::Error;

use crate::capacity::CapacityCatalog;
use crate::fitting::{
    ClientFloors, LearnedBounds, fit_reservation, lookup_bounded_history, validate_properties,
};
use crate::operations::{
    AdmitError, AdmitOutcome, NewOperation, Now, OperationKey, Operations, canonical_platform_key,
};
use crate::qos::{self, GateAudit};
use crate::queue::OperationQueue;
use crate::sketch::ResourceSketch;

/// Input parameters for admitting one `Execute` request into the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRequest {
    /// Unique `REAPI` operation resource name.
    pub name: OperationName,
    /// Digest of the `REAPI` `Action`.
    pub action_digest: Digest,
    /// Canonical `<hash>/<size>` digest of the `REAPI` `Command` (for nameless sketch fallback).
    pub command_digest: String,
    /// Raw client `platform_properties`.
    pub platform_properties: BTreeMap<String, String>,
    /// Whether `ActionCache` lookup was skipped (`true` disables deduplication).
    pub skip_cache_lookup: bool,
    /// Whether writing the `ActionResult` to `ActionCache` is disabled.
    pub do_not_cache: bool,
    /// Authenticated first-creator source, if present.
    pub source: Option<SourceId>,
    /// First submitter's build identifier, if present.
    pub build: Option<BuildId>,
    /// Bazel target label from request metadata, if present.
    pub target: Option<TargetLabel>,
    /// Bazel mnemonic from request metadata, if present.
    pub mnemonic: Option<Mnemonic>,
    /// Client `ExecutionPolicy.priority`.
    pub priority: i32,
    /// Initial active client stream watcher count (normally `1` for `Execute`).
    pub initial_watchers: u32,
}

/// Per-operation admission metadata retained alongside [`Operation`](crate::operations::Operation)
/// for retries, escalation, and sketch observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionProfile {
    /// Identity of this action's resource history.
    pub history_key: Option<crate::sketch::SketchKey>,
    /// Immutable original client resource floors.
    pub floors: ClientFloors,
    /// Pre-fit desired reservation (after configured minima raising).
    pub desired_reservation: ActionResources,
    /// CPU reservation in millicores before whole-worker physical-memory promotion.
    pub cpu_before_memory_fit: u64,
    /// Fleet chosen during initial fitting.
    pub fitted_fleet: usize,
    /// [`CapacityCatalog::revision`] observed during admission.
    pub capacity_revision: u64,
}

/// Successful outcome of [`admit_action`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// A new operation was inserted into [`Operations`] and [`OperationQueue`].
    Inserted {
        /// Generation-checked handle of the newly admitted operation.
        key: OperationKey,
        /// Assigned monotonic admission sequence number.
        seq: u64,
        /// Assigned `cgroup` v2 CPU weight.
        cpu_weight: CpuWeight,
        /// Gate-tier audit marker when `priority >= 200`.
        gate_audit: Option<GateAudit>,
        /// Admission profile retained for escalation and sketch observation.
        profile: ActionProfile,
    },
    /// The request deduplicated onto an active non-terminal operation.
    Deduplicated {
        /// Handle of the existing active operation.
        key: OperationKey,
        /// `true` when incoming priority strictly raised the existing operation's priority.
        promoted: bool,
        /// Effective priority after any promotion.
        priority: i32,
        /// Effective `cgroup` v2 CPU weight after any promotion.
        cpu_weight: CpuWeight,
        /// Gate-tier audit marker when the incoming request itself entered at gate priority.
        gate_audit: Option<GateAudit>,
    },
}

/// Errors returned by [`admit_action`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdmissionError {
    /// No declared fleet accepts the client's original platform properties or floors.
    #[error("no fleet accepts this action: {0}")]
    Refused(Refusal),
    /// Registry insertion failed due to duplicate operation name or slab exhaustion.
    #[error(transparent)]
    Registry(#[from] AdmitError),
}

/// Admits one `Execute` request: validates client properties against `capacity.current()`,
/// computes `QoS` weight and gate audit, looks up learned/cold-start reservations, fits to the
/// admitted fleets and observed capacity facts, and deduplicates or enqueues the operation (`§5.3`).
///
/// # Errors
///
/// Returns [`AdmissionError::Refused`] when no declared fleet satisfies the request, or
/// [`AdmissionError::Registry`] on duplicate operation name or slab exhaustion.
pub fn admit_action(
    request: AdmissionRequest,
    now: Now,
    actions: &ActionsConfig,
    capacity: &CapacityCatalog,
    sketch: &mut ResourceSketch,
    operations: &mut Operations,
    queue: &mut OperationQueue,
) -> Result<AdmissionOutcome, AdmissionError> {
    let catalog = capacity.current();
    let validated = validate_properties(
        catalog,
        request
            .platform_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
    .map_err(AdmissionError::Refused)?;

    let gate_audit = qos::gate_audit(request.source, request.priority);
    let learned = lookup_bounded_history(
        sketch,
        request.target.as_ref().map(TargetLabel::as_str),
        request.mnemonic.as_ref().map(Mnemonic::as_str),
        &request.command_digest,
        now.wall.as_unix_millis() / 1000,
        &actions.cold_start,
        LearnedBounds::for_candidates(catalog, &validated),
    );

    let fit = fit_reservation(
        catalog,
        validated.candidates,
        &validated.selectors,
        validated.floors,
        learned,
        actions.memory.min_reservation,
        actions.disk.min_reservation,
    )
    .map_err(AdmissionError::Refused)?;

    let platform_key = operation_platform_key(&request.platform_properties);

    let profile = ActionProfile {
        history_key: Some(crate::sketch::SketchKey::for_attempt(
            request.target.as_ref().map(TargetLabel::as_str),
            request.mnemonic.as_ref().map(Mnemonic::as_str),
            &request.command_digest,
        )),
        floors: validated.floors,
        desired_reservation: fit.desired,
        cpu_before_memory_fit: fit.cpu_before_memory_fit,
        fitted_fleet: fit.fleet,
        capacity_revision: capacity.revision(),
    };

    let outcome = operations.admit(
        NewOperation {
            name: request.name,
            action_digest: request.action_digest,
            platform_key,
            skip_cache_lookup: request.skip_cache_lookup,
            do_not_cache: request.do_not_cache,
            source: request.source,
            build: request.build,
            target: request.target,
            mnemonic: request.mnemonic,
            candidates: validated.candidates,
            priority: request.priority,
            has_explicit_cpu_floor: validated.floors.cpu_millicores.is_some(),
            reservation: fit.reservation,
            learned_reservation: fit.learned_reservation,
            initial_watchers: request.initial_watchers,
        },
        now,
        queue,
    );

    match outcome {
        AdmitOutcome::Inserted {
            key,
            seq,
            cpu_weight,
        } => Ok(AdmissionOutcome::Inserted {
            key,
            seq,
            cpu_weight,
            gate_audit,
            profile,
        }),
        AdmitOutcome::Deduplicated {
            key,
            promoted,
            priority,
            cpu_weight,
        } => Ok(AdmissionOutcome::Deduplicated {
            key,
            promoted,
            priority,
            cpu_weight,
            gate_audit,
        }),
        AdmitOutcome::Rejected(err) => Err(AdmissionError::Registry(err)),
    }
}

fn operation_platform_key(properties: &BTreeMap<String, String>) -> String {
    canonical_platform_key(
        properties
            .iter()
            .filter(|(key, value)| {
                !gang_types::property::HARMLESS_PROPERTIES.contains(&key.as_str())
                    && !(key.as_str() == gang_types::property::NETWORK
                        && (value.is_empty()
                            || value.as_str() == gang_types::NetworkRequest::NotRequested.as_str()))
            })
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::{
        Bytes, FleetActionPolicy, FleetCatalog, FleetName, FleetProperties, Isa, Percent,
        Resources, property,
    };

    use super::*;
    use crate::sketch::{Budget, Policy};

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
                memory: Bytes::gib(16),
                disk: Bytes::gib(100),
            },
            &FleetActionPolicy::default(),
            std::iter::empty(),
        )])
        .expect("catalog")
    }

    fn sketch() -> ResourceSketch {
        ResourceSketch::with_shards(
            Policy {
                headroom_percent: 20,
                cpu_reservation_percentile: Percent::from_percent(50),
                classes: Vec::new(),
                reserve_cpu: true,
                reserve_memory: true,
                reserve_disk: true,
            },
            Budget {
                target: 8,
                mnemonic: 8,
                command: 8,
                probation: 8,
            },
            Duration::from_hours(24),
            1,
        )
    }

    #[test]
    fn admit_action_inserts_with_gate_audit_and_deduplicates_with_promotion() {
        let cap = CapacityCatalog::new(catalog());
        let mut sk = sketch();
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let actions = ActionsConfig::default();

        let req1 = AdmissionRequest {
            name: OperationName::try_new("operations/op1").expect("name"),
            action_digest: Digest::from_bytes(b"action-1"),
            command_digest: "cmd-1".to_owned(),
            platform_properties: [(property::ISA.to_owned(), "x86-64".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: SourceId::from_identity_str("local:alice"),
            build: None,
            target: Some(TargetLabel::try_new("//app:bin").expect("target")),
            mnemonic: Some(Mnemonic::try_new("Rustc").expect("mnemonic")),
            priority: 100,
            initial_watchers: 1,
        };

        let out1 = admit_action(req1, now(1), &actions, &cap, &mut sk, &mut ops, &mut queue)
            .expect("admit 1");
        let AdmissionOutcome::Inserted {
            key,
            cpu_weight,
            gate_audit,
            profile,
            ..
        } = out1
        else {
            panic!("expected insert");
        };
        assert_eq!(cpu_weight, CpuWeight::INTERACTIVE);
        assert!(gate_audit.is_none());
        assert_eq!(profile.fitted_fleet, 0);
        // Cold start applied (250m, 4GiB, 4GiB).
        let op = ops.get(key).expect("op");
        assert_eq!(op.reservation.cpu_millicores, 250);
        assert_eq!(op.reservation.memory, Bytes::gib(4));
        assert_eq!(op.reservation.disk, Bytes::gib(4));

        // Second request with harmless metadata and gate priority deduplicates & promotes.
        let req2 = AdmissionRequest {
            name: OperationName::try_new("operations/op2").expect("name"),
            action_digest: Digest::from_bytes(b"action-1"),
            command_digest: "cmd-1".to_owned(),
            platform_properties: [
                (property::ISA.to_owned(), "x86-64".to_owned()),
                (
                    property::ACTION_ENVIRONMENT.to_owned(),
                    "host:other".to_owned(),
                ),
            ]
            .into_iter()
            .collect(),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: SourceId::from_identity_str("local:bob"),
            build: None,
            target: None,
            mnemonic: None,
            priority: 250,
            initial_watchers: 1,
        };

        let out2 = admit_action(req2, now(2), &actions, &cap, &mut sk, &mut ops, &mut queue)
            .expect("admit 2");
        let AdmissionOutcome::Deduplicated {
            key: dedup_key,
            promoted,
            priority,
            cpu_weight,
            gate_audit,
        } = out2
        else {
            panic!("expected dedup");
        };
        assert_eq!(dedup_key, key);
        assert!(promoted);
        assert_eq!(priority, 250);
        assert_eq!(cpu_weight, CpuWeight::GATE);
        assert!(gate_audit.is_some());
    }

    #[test]
    fn admit_action_refuses_unknown_property() {
        let cap = CapacityCatalog::new(catalog());
        let mut sk = sketch();
        let mut ops = Operations::new();
        let mut queue = OperationQueue::new(1).expect("queue");
        let actions = ActionsConfig::default();

        let req = AdmissionRequest {
            name: OperationName::try_new("operations/bad").expect("name"),
            action_digest: Digest::from_bytes(b"bad"),
            command_digest: "bad".to_owned(),
            platform_properties: [("unknown".to_owned(), "1".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: false,
            do_not_cache: false,
            source: None,
            build: None,
            target: None,
            mnemonic: None,
            priority: 0,
            initial_watchers: 1,
        };

        let err = admit_action(req, now(1), &actions, &cap, &mut sk, &mut ops, &mut queue)
            .expect_err("refused");
        assert!(matches!(
            err,
            AdmissionError::Refused(Refusal::Unknown { .. })
        ));
    }
}
