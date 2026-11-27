//! Memory and disk resource-kill classification, retry escalation, and terminal refusal (`§5.7`).
//!
//! Single owner of the `SPEC §3.2` escalation formula:
//! `next = min(cap, max(ceil(peak * (100 + headroom) / 100), ceil(reservation * escalation_percent / 100), reservation + 1))`
//!
//! Separate memory and disk step budgets apply independently of `max_job_retries`. Soft disk kills
//! fail terminally instead of requeueing. A measured peak above the largest eligible ceiling, a
//! further kill at a lower policy cap, a further kill while holding physical capacity and
//! whole-worker CPU, or budget exhaustion produces a single terminal failure naming action, peak,
//! capacity, and reason.

use std::fmt;

use gang_config::ActionsConfig;
use gang_types::{ActionResources, Bytes, FleetSet, OperationName, property};
use thiserror::Error;

use crate::capacity::CapacityCatalog;
use crate::fitting::ClientFloors;
use crate::operations::RetryCounts;

/// Resource dimension targeted by a worker or kernel resource kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KillResource {
    /// Memory OOM, pressure, soft ceiling, or side-channel memory failure.
    Memory,
    /// Disk pressure or soft disk ceiling kill.
    Disk,
}

impl fmt::Display for KillResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => formatter.write_str("memory"),
            Self::Disk => formatter.write_str("disk"),
        }
    }
}

/// Classified cause of a resource kill on an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KillCause {
    /// Kernel `cgroup` `oom_kill` event (`oom_kills > 0`).
    Oom,
    /// Worker memory `PSI` pressure kill.
    MemoryPressure,
    /// Worker soft memory ceiling breach kill.
    SoftMemory,
    /// Worker side-channel `failure: memory` (applied only when no kill was already classified).
    SideChannelMemory,
    /// Worker filesystem disk pressure kill.
    DiskPressure,
    /// Worker soft disk ceiling breach kill (always terminal; never requeues).
    SoftDisk,
}

impl KillCause {
    /// Returns the resource dimension (`Memory` or `Disk`) associated with this kill cause.
    #[must_use]
    pub const fn resource(self) -> KillResource {
        match self {
            Self::Oom | Self::MemoryPressure | Self::SoftMemory | Self::SideChannelMemory => {
                KillResource::Memory
            }
            Self::DiskPressure | Self::SoftDisk => KillResource::Disk,
        }
    }

    /// Returns `true` when this kill cause is allowed to escalate and requeue (`false` for soft
    /// disk kills).
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        !matches!(self, Self::SoftDisk)
    }
}

/// Classifies a resource kill from explicit worker kill classification, `cgroup` `oom_kills`, and
/// the worker side-channel `failure: memory` flag (`c41a9690`).
///
/// An explicit worker classification or kernel `oom_kills > 0` takes precedence; side-channel
/// `failure: memory` promotes to [`KillCause::SideChannelMemory`] only when no kill was already
/// classified.
#[must_use]
pub const fn classify_kill(
    reported_kill: Option<KillCause>,
    oom_kills: u64,
    side_channel_memory_failure: bool,
) -> Option<KillCause> {
    if let Some(kill) = reported_kill {
        return Some(kill);
    }
    if oom_kills > 0 {
        return Some(KillCause::Oom);
    }
    if side_channel_memory_failure {
        return Some(KillCause::SideChannelMemory);
    }
    None
}

/// Terminal reason an escalation cannot requeue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EscalationFailureReason {
    /// Soft disk ceiling kills fail instead of requeueing.
    SoftDiskKill,
    /// Measured peak usage strictly exceeded the largest eligible fleet ceiling.
    PeakExceedsCapacity,
    /// Attempt was already killed at a policy cap below physical worker capacity.
    AtPolicyCap,
    /// Attempt was already killed while holding physical capacity and whole-worker CPU.
    AtPhysicalCapacity,
    /// Separate memory or disk escalation step budget is exhausted.
    StepBudgetExhausted,
    /// No candidate fleet in the current catalog satisfies the operation's original floors.
    NoEligibleFleet,
}

impl fmt::Display for EscalationFailureReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SoftDiskKill => formatter.write_str("exceeded its disk reservation"),
            Self::PeakExceedsCapacity => {
                formatter.write_str("measured peak exceeds maximum eligible fleet capacity")
            }
            Self::AtPolicyCap => {
                formatter.write_str("killed while already at effective policy cap")
            }
            Self::AtPhysicalCapacity => {
                formatter.write_str("killed while holding physical capacity and whole-worker CPU")
            }
            Self::StepBudgetExhausted => formatter.write_str("escalation step budget exhausted"),
            Self::NoEligibleFleet => {
                formatter.write_str("no candidate fleet satisfies original resource floors")
            }
        }
    }
}

/// Single terminal failure emitted when a resource kill cannot be escalated further (`§5.7`).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "action {action} failed due to {resource} kill ({reason}; measured_peak_kb={peak}, capacity_kb={capacity_kb})",
    peak = format_peak(*measured_peak_kb)
)]
pub struct EscalationFailure {
    /// Name of the failed operation.
    pub action: OperationName,
    /// Resource dimension that suffered the kill.
    pub resource: KillResource,
    /// Specific kill cause.
    pub cause: KillCause,
    /// Measured peak usage in `KiB`, if reported.
    pub measured_peak_kb: Option<u64>,
    /// Largest eligible effective ceiling in `KiB` (or `0` if no eligible fleet exists).
    pub capacity_kb: u64,
    /// Terminal escalation refusal reason.
    pub reason: EscalationFailureReason,
}

fn format_peak(peak: Option<u64>) -> String {
    peak.map_or_else(|| "unmeasured".to_owned(), |kb| kb.to_string())
}

/// Outcome of evaluating a resource kill under `§5.7`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EscalationDecision {
    /// Requeue the operation with the escalated `next_reservation`.
    Retry {
        /// Updated reservation to apply to the operation.
        next_reservation: ActionResources,
        /// Resource dimension whose step counter must be incremented.
        resource: KillResource,
        /// Target fleet attaining the ceiling used for escalation.
        target_fleet: usize,
    },
    /// Fail the operation terminally without requeueing.
    Fail(EscalationFailure),
}

/// Inputs required by [`evaluate_escalation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationInput<'a> {
    /// `REAPI` operation name for diagnostic failure messages.
    pub action: &'a OperationName,
    /// Classified resource kill cause.
    pub cause: KillCause,
    /// Measured peak memory (when `cause` is memory) or disk (when `cause` is disk) in `KiB`.
    pub measured_peak_kb: Option<u64>,
    /// Current reservation held by the killed attempt.
    pub current_reservation: ActionResources,
    /// CPU reservation in millicores prior to any whole-worker physical-memory promotion.
    pub cpu_before_memory_fit: u64,
    /// Immutable original client resource floors.
    pub floors: ClientFloors,
    /// Candidate fleets admitted at admission time.
    pub candidates: FleetSet,
    /// Retry and escalation step counts already consumed by the operation.
    pub retry_counts: RetryCounts,
}

/// Evaluates whether a resource kill may escalate and requeue or must terminate (`§5.7`).
#[must_use]
pub fn evaluate_escalation(
    input: &EscalationInput<'_>,
    actions: &ActionsConfig,
    capacity: &CapacityCatalog,
) -> EscalationDecision {
    let resource = input.cause.resource();
    let Some(best) = largest_eligible_ceiling(
        capacity,
        input.candidates,
        input.floors,
        input.current_reservation,
        resource,
    ) else {
        return EscalationDecision::Fail(failure_for(
            input,
            resource,
            0,
            EscalationFailureReason::NoEligibleFleet,
        ));
    };

    let (current_kb, steps_used, max_steps, escalation_percent) = match resource {
        KillResource::Memory => (
            input.current_reservation.memory.get().div_ceil(1024),
            input.retry_counts.memory_steps,
            actions.memory.max_steps.get(),
            u64::from(actions.memory.escalation_percent.get()),
        ),
        KillResource::Disk => (
            input.current_reservation.disk.get().div_ceil(1024),
            input.retry_counts.disk_steps,
            actions.disk.max_steps.get(),
            u64::from(actions.disk.escalation_percent.get()),
        ),
    };

    if let Some(reason) = terminal_reason(input, best, current_kb, steps_used, max_steps) {
        return EscalationDecision::Fail(failure_for(input, resource, best.cap_kb, reason));
    }

    let headroom = u64::from(actions.reservation_headroom_percent.get());
    let from_peak = input
        .measured_peak_kb
        .map_or(0, |peak| ceil_percent(peak, 100 + headroom));
    let from_multiplier = ceil_percent(current_kb, escalation_percent);
    let from_step = current_kb.saturating_add(1);
    let next_kb = best
        .cap_kb
        .min(from_peak.max(from_multiplier).max(from_step));

    let mut next_reservation = input.current_reservation;
    match resource {
        KillResource::Memory => next_reservation.memory = Bytes::kib(next_kb),
        KillResource::Disk => next_reservation.disk = Bytes::kib(next_kb),
    }

    // Reserve whole-worker CPU (bounded by CPU action ceiling) when reaching full physical
    // capacity without a lower policy cap on the constrained resource; keep non-whole CPU
    // otherwise (clamping to the target fleet's CPU ceiling).
    if next_kb >= best.cap_kb && !best.is_policy_cap {
        next_reservation.cpu_millicores = best.cpu_ceiling_millicores;
    } else {
        let base_cpu = input
            .cpu_before_memory_fit
            .max(input.floors.cpu_millicores.unwrap_or(0));
        next_reservation.cpu_millicores = base_cpu.min(best.cpu_ceiling_millicores);
    }

    EscalationDecision::Retry {
        next_reservation,
        resource,
        target_fleet: best.fleet,
    }
}

const fn terminal_reason(
    input: &EscalationInput<'_>,
    best: BestCeiling,
    current_kb: u64,
    steps_used: u32,
    max_steps: u32,
) -> Option<EscalationFailureReason> {
    if !input.cause.is_retryable() {
        return Some(EscalationFailureReason::SoftDiskKill);
    }
    if let Some(peak_kb) = input.measured_peak_kb
        && peak_kb > best.cap_kb
    {
        return Some(EscalationFailureReason::PeakExceedsCapacity);
    }
    if current_kb >= best.cap_kb {
        if best.is_policy_cap {
            return Some(EscalationFailureReason::AtPolicyCap);
        }
        if input.current_reservation.cpu_millicores >= best.cpu_ceiling_millicores {
            return Some(EscalationFailureReason::AtPhysicalCapacity);
        }
    }
    if steps_used >= max_steps {
        return Some(EscalationFailureReason::StepBudgetExhausted);
    }
    None
}

fn failure_for(
    input: &EscalationInput<'_>,
    resource: KillResource,
    capacity_kb: u64,
    reason: EscalationFailureReason,
) -> EscalationFailure {
    EscalationFailure {
        action: input.action.clone(),
        resource,
        cause: input.cause,
        measured_peak_kb: input.measured_peak_kb,
        capacity_kb,
        reason,
    }
}

#[derive(Debug, Clone, Copy)]
struct BestCeiling {
    fleet: usize,
    cap_kb: u64,
    is_policy_cap: bool,
    cpu_ceiling_millicores: u64,
}

fn largest_eligible_ceiling(
    capacity: &CapacityCatalog,
    candidates: FleetSet,
    floors: ClientFloors,
    current: ActionResources,
    killed: KillResource,
) -> Option<BestCeiling> {
    let catalog = capacity.current();
    let mut best: Option<BestCeiling> = None;

    for fleet_idx in candidates {
        if !floors.fits_fleet(catalog, fleet_idx) {
            continue;
        }
        let Some(fleet_cap) = capacity.fleet(fleet_idx) else {
            continue;
        };
        let Some(props) = catalog.fleets().get(fleet_idx) else {
            continue;
        };

        // Ensure other requested/escalated minimums still fit the fleet's effective ceiling.
        let other_fits = match killed {
            KillResource::Memory => {
                let disk_req = floors
                    .disk_kb
                    .unwrap_or(0)
                    .max(current.disk.get().div_ceil(1024));
                props
                    .limit(property::DISK_KB)
                    .is_some_and(|limit| disk_req <= limit)
            }
            KillResource::Disk => {
                let mem_req = floors
                    .memory_kb
                    .unwrap_or(0)
                    .max(current.memory.get().div_ceil(1024));
                props
                    .limit(property::MEMORY_KB)
                    .is_some_and(|limit| mem_req <= limit)
            }
        };
        if !other_fits {
            continue;
        }

        let (cap_kb, is_policy_cap) = match killed {
            KillResource::Memory => (
                fleet_cap.memory_ceiling_kb,
                fleet_cap.memory_limited_by_policy(),
            ),
            KillResource::Disk => (
                fleet_cap.disk_ceiling_kb,
                fleet_cap.disk_limited_by_policy(),
            ),
        };

        let candidate = BestCeiling {
            fleet: fleet_idx,
            cap_kb,
            is_policy_cap,
            cpu_ceiling_millicores: fleet_cap.cpu_ceiling_millicores,
        };

        let replace = match best {
            None => true,
            Some(prev) => {
                // Prefer higher ceiling; break ties by preferring physical capacity (not policy
                // capped), then catalog preference order.
                (candidate.cap_kb, !candidate.is_policy_cap) > (prev.cap_kb, !prev.is_policy_cap)
            }
        };
        if replace {
            best = Some(candidate);
        }
    }
    best
}

fn ceil_percent(value: u64, percent: u64) -> u64 {
    if value == 0 {
        return 0;
    }
    let product = u128::from(value).saturating_mul(u128::from(percent));
    u64::try_from(product.div_ceil(100)).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use gang_config::EscalationPercent;
    use gang_types::{
        FleetActionPolicy, FleetCatalog, FleetName, FleetProperties, Isa, NonZeroBytes, Resources,
    };

    use super::*;

    fn fleet_with_caps(
        name: &str,
        vcpus: u32,
        mem_gib: u64,
        disk_gib: u64,
        mem_cap_gib: Option<u64>,
        cpu_cap: Option<u32>,
    ) -> FleetProperties {
        let mut actions = FleetActionPolicy::default();
        actions.resources.memory = mem_cap_gib.map(NonZeroBytes::gib);
        actions.resources.cpu_count = cpu_cap.and_then(NonZeroU32::new);
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(disk_gib),
            },
            &actions,
            std::iter::empty(),
        )
    }

    fn op_name() -> OperationName {
        OperationName::try_new("operations/esc-1").expect("name")
    }

    #[test]
    fn repeated_memory_kills_never_escalate_past_observed_eligible_capacity() {
        let cat = FleetCatalog::try_new(vec![
            fleet_with_caps("eligible", 16, 128, 100, None, None),
            fleet_with_caps("other", 32, 256, 100, None, None),
        ])
        .expect("catalog");
        let mut cap = CapacityCatalog::new(cat, NonZeroU32::MIN);
        cap.set_local_observations([gang_spi::FleetResourceCapacity {
            fleet: 0,
            capacity: gang_types::Resources {
                vcpus: 16,
                memory: Bytes::gib(60),
                disk: Bytes::gib(100),
            },
        }])
        .expect("observed memory");
        let name = op_name();
        let actions = ActionsConfig::default();
        for cause in [KillCause::Oom, KillCause::MemoryPressure] {
            let mut input = EscalationInput {
                action: &name,
                cause,
                measured_peak_kb: None,
                current_reservation: ActionResources {
                    cpu_millicores: 890,
                    memory: Bytes::gib(4),
                    disk: Bytes::gib(1),
                },
                cpu_before_memory_fit: 890,
                floors: ClientFloors::default(),
                candidates: FleetSet::try_from_indices([0]).expect("candidates"),
                retry_counts: RetryCounts::default(),
            };
            loop {
                match evaluate_escalation(&input, &actions, &cap) {
                    EscalationDecision::Retry {
                        next_reservation, ..
                    } => {
                        assert!(next_reservation.memory <= Bytes::gib(60));
                        assert!(next_reservation.memory > input.current_reservation.memory);
                        input.current_reservation = next_reservation;
                        input.retry_counts.memory_steps += 1;
                    }
                    EscalationDecision::Fail(failure) => {
                        assert_eq!(failure.reason, EscalationFailureReason::AtPhysicalCapacity);
                        assert_eq!(input.current_reservation.memory, Bytes::gib(60));
                        break;
                    }
                }
            }
        }
    }

    #[test]
    fn escalation_formula_and_whole_cpu_promotion_at_physical_cap() {
        let cat = FleetCatalog::try_new(vec![fleet_with_caps("a", 8, 16, 100, None, Some(6))])
            .expect("catalog");
        let cap = CapacityCatalog::new(cat, NonZeroU32::new(2).expect("nonzero"));
        let actions = ActionsConfig::default(); // 20% headroom, 200% escalation, 8 max_steps
        let name = op_name();
        let candidates = FleetSet::try_from_indices([0]).expect("candidates");

        // Step 1: reserved 4 GiB, measured peak 6 GiB ->
        // max(ceil(6GiB * 1.2) = 7.2GiB, ceil(4GiB * 2.0) = 8GiB, 4GiB + 1KiB) = 8 GiB.
        let d1 = evaluate_escalation(
            &EscalationInput {
                action: &name,
                cause: KillCause::Oom,
                measured_peak_kb: Some(6 * 1024 * 1024),
                current_reservation: ActionResources {
                    cpu_millicores: 1_000,
                    memory: Bytes::gib(4),
                    disk: Bytes::gib(1),
                },
                cpu_before_memory_fit: 1_000,
                floors: ClientFloors::default(),
                candidates,
                retry_counts: RetryCounts::default(),
            },
            &actions,
            &cap,
        );
        let EscalationDecision::Retry {
            next_reservation,
            resource,
            ..
        } = d1
        else {
            panic!("expected retry");
        };
        assert_eq!(resource, KillResource::Memory);
        assert_eq!(next_reservation.memory, Bytes::gib(8));
        assert_eq!(next_reservation.cpu_millicores, 1_000);

        // Step 2: reserved 8 GiB, measured peak 14 GiB ->
        // ceil(14GiB * 1.2) = 16.8GiB, clamped to 16 GiB (headroom overshoot is NOT impossible
        // demand!), and since 16 GiB equals physical capacity without a lower policy cap, CPU is
        // promoted to CPU ceiling (6_000m, NOT physical 8_000m or oversubscribed 16_000m).
        let d2 = evaluate_escalation(
            &EscalationInput {
                action: &name,
                cause: KillCause::MemoryPressure,
                measured_peak_kb: Some(14 * 1024 * 1024),
                current_reservation: next_reservation,
                cpu_before_memory_fit: 1_000,
                floors: ClientFloors::default(),
                candidates,
                retry_counts: RetryCounts {
                    memory_steps: 1,
                    ..RetryCounts::default()
                },
            },
            &actions,
            &cap,
        );
        let EscalationDecision::Retry {
            next_reservation: r2,
            ..
        } = d2
        else {
            panic!("expected retry with clamp");
        };
        assert_eq!(r2.memory, Bytes::gib(16));
        assert_eq!(r2.cpu_millicores, 6_000);

        // Step 3: further kill while holding physical capacity (16 GiB) and whole-worker CPU
        // (6_000m) is terminal.
        let d3 = evaluate_escalation(
            &EscalationInput {
                action: &name,
                cause: KillCause::Oom,
                measured_peak_kb: Some(16 * 1024 * 1024),
                current_reservation: r2,
                cpu_before_memory_fit: 1_000,
                floors: ClientFloors::default(),
                candidates,
                retry_counts: RetryCounts {
                    memory_steps: 2,
                    ..RetryCounts::default()
                },
            },
            &actions,
            &cap,
        );
        let EscalationDecision::Fail(err) = d3 else {
            panic!("expected terminal failure");
        };
        assert_eq!(err.reason, EscalationFailureReason::AtPhysicalCapacity);
        assert!(err.to_string().contains("operations/esc-1"));
        assert!(err.to_string().contains("capacity_kb="));
    }

    fn test_escalation(
        cause: KillCause,
        peak_gib: u64,
        mem_gib: u64,
        disk_gib: u64,
        memory_steps: u32,
        actions: &ActionsConfig,
        cap: &CapacityCatalog,
    ) -> EscalationDecision {
        let name = op_name();
        let candidates = FleetSet::try_from_indices([0]).expect("candidates");
        evaluate_escalation(
            &EscalationInput {
                action: &name,
                cause,
                measured_peak_kb: Some(peak_gib * 1024 * 1024),
                current_reservation: ActionResources {
                    cpu_millicores: 1_000,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(disk_gib),
                },
                cpu_before_memory_fit: 1_000,
                floors: ClientFloors::default(),
                candidates,
                retry_counts: RetryCounts {
                    memory_steps,
                    ..RetryCounts::default()
                },
            },
            actions,
            cap,
        )
    }

    #[test]
    fn peak_above_cap_policy_cap_soft_disk_and_budget_exhaustion_fail() {
        let cat = FleetCatalog::try_new(vec![fleet_with_caps("capped", 8, 16, 100, Some(8), None)])
            .expect("catalog");
        let cap = CapacityCatalog::new(cat, NonZeroU32::new(2).expect("nonzero"));
        let mut actions = ActionsConfig::default();
        actions.memory.escalation_percent = EscalationPercent::try_new(150).expect("150");
        actions.memory.max_steps = NonZeroU32::new(2).expect("2");

        // Peak > cap (9 GiB > 8 GiB policy cap) -> terminal PeakExceedsCapacity.
        let over = test_escalation(KillCause::Oom, 9, 4, 1, 0, &actions, &cap);
        assert!(matches!(
            over,
            EscalationDecision::Fail(EscalationFailure {
                reason: EscalationFailureReason::PeakExceedsCapacity,
                ..
            })
        ));

        // Already at policy cap (8 GiB < 16 GiB physical) -> terminal AtPolicyCap immediately.
        let at_cap = test_escalation(KillCause::Oom, 8, 8, 1, 1, &actions, &cap);
        assert!(matches!(
            at_cap,
            EscalationDecision::Fail(EscalationFailure {
                reason: EscalationFailureReason::AtPolicyCap,
                ..
            })
        ));

        // Step budget exhaustion.
        let exhausted = test_escalation(KillCause::Oom, 5, 4, 1, 2, &actions, &cap);
        assert!(matches!(
            exhausted,
            EscalationDecision::Fail(EscalationFailure {
                reason: EscalationFailureReason::StepBudgetExhausted,
                ..
            })
        ));

        // Soft disk kill fails immediately; pressure disk kill retries.
        let soft_disk = test_escalation(KillCause::SoftDisk, 2, 1, 2, 0, &actions, &cap);
        assert!(matches!(
            soft_disk,
            EscalationDecision::Fail(EscalationFailure {
                reason: EscalationFailureReason::SoftDiskKill,
                ..
            })
        ));

        let pressure_disk = test_escalation(KillCause::DiskPressure, 3, 1, 2, 0, &actions, &cap);
        assert!(matches!(
            pressure_disk,
            EscalationDecision::Retry {
                resource: KillResource::Disk,
                ..
            }
        ));
    }

    #[test]
    fn side_channel_memory_classification_respects_precedence() {
        assert_eq!(
            classify_kill(Some(KillCause::DiskPressure), 1, true),
            Some(KillCause::DiskPressure)
        );
        assert_eq!(classify_kill(None, 2, true), Some(KillCause::Oom));
        assert_eq!(
            classify_kill(None, 0, true),
            Some(KillCause::SideChannelMemory)
        );
        assert_eq!(classify_kill(None, 0, false), None);
    }
}
