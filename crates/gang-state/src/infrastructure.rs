//! Pure projection of operational infrastructure observations onto current fleet membership.

use gang_reconciler::FleetPass;
use gang_spi::{
    FleetInfrastructureEvent, InfrastructureEventKind, InfrastructureInterruptionAction,
    InfrastructureTerminationReason,
};
use gang_types::InstanceId;
use gang_wire::{
    AwsEvent, AwsEventKind, AwsInterruptionAction, AwsTerminationReason, Event, EventChange,
    InfrastructurePhase, InfrastructureReason, name,
};

/// Projects the newest canonical launch outcome independently of the bounded event history.
pub(crate) fn fill_launch_warnings(
    fleets: &mut [gang_wire::Fleet],
    records: &[FleetInfrastructureEvent],
    scratch: &mut String,
) {
    for fleet in fleets {
        let latest = if fleet
            .instances
            .pending
            .saturating_add(fleet.instances.in_service)
            < fleet.desired
        {
            records
                .iter()
                .filter(|record| {
                    matches!(
                        record.event.kind,
                        InfrastructureEventKind::CapacityUnavailable { .. }
                            | InfrastructureEventKind::InstanceLaunched { .. }
                            | InfrastructureEventKind::ReplacementLaunched { .. }
                    ) && same_name::<name::Fleets>(&fleet.name, &record.fleet, scratch)
                })
                .max_by(|a, b| {
                    a.event
                        .at
                        .cmp(&b.event.at)
                        .then_with(|| name::cmp_of::<name::AwsEvents>(&a.event.id, &b.event.id))
                })
        } else {
            None
        };
        if let Some(record) = latest
            && let InfrastructureEventKind::CapacityUnavailable { reason } = &record.event.kind
        {
            if let Some(warning) = &mut fleet.launch_warning {
                warning.reason.clone_from(reason);
                warning.at = record.event.at;
            } else {
                fleet.launch_warning = Some(gang_wire::LaunchWarning {
                    reason: reason.clone(),
                    at: record.event.at,
                });
            }
        } else {
            fleet.launch_warning = None;
        }
    }
}

/// Converts a canonical observation into a status event, enriching a supported pairing.
#[must_use]
pub(crate) fn status_event(
    record: &FleetInfrastructureEvent,
    pass: Option<&FleetPass>,
    records: &[FleetInfrastructureEvent],
) -> Event {
    let source = &record.event;
    let instance_name = |instance: &InstanceId| name::of::<name::Instances>(instance);
    let kind = match &source.kind {
        InfrastructureEventKind::RebalanceRecommendation { instance } => {
            AwsEventKind::RebalanceRecommendation {
                instance: instance_name(instance),
            }
        }
        InfrastructureEventKind::InstanceLaunched { instance } => AwsEventKind::InstanceLaunched {
            instance: instance_name(instance),
        },
        InfrastructureEventKind::SpotInterruption { instance, action } => {
            AwsEventKind::SpotInterruption {
                instance: instance_name(instance),
                action: match action {
                    InfrastructureInterruptionAction::Terminate => AwsInterruptionAction::Terminate,
                    InfrastructureInterruptionAction::Stop => AwsInterruptionAction::Stop,
                    InfrastructureInterruptionAction::Hibernate => AwsInterruptionAction::Hibernate,
                },
            }
        }
        InfrastructureEventKind::ReplacementLaunched {
            instance,
            replacing,
        } => AwsEventKind::ReplacementLaunched {
            instance: instance_name(instance),
            replacing: replacing
                .as_ref()
                .or_else(|| {
                    pass.and_then(|pass| replacement_peer(pass, records, instance, source.at, None))
                })
                .map(instance_name),
        },
        InfrastructureEventKind::InstanceTerminated { instance, reason } => {
            let (reason, reason_detail) = match reason {
                InfrastructureTerminationReason::Rebalance => {
                    (AwsTerminationReason::Rebalance, None)
                }
                InfrastructureTerminationReason::SpotReclaimed => {
                    (AwsTerminationReason::SpotReclaimed, None)
                }
                InfrastructureTerminationReason::Other(detail) => {
                    (AwsTerminationReason::Other, Some(detail.clone()))
                }
            };
            AwsEventKind::InstanceTerminated {
                instance: instance_name(instance),
                reason,
                reason_detail,
            }
        }
        InfrastructureEventKind::CapacityUnavailable { reason } => {
            AwsEventKind::CapacityUnavailable {
                reason: reason.clone(),
            }
        }
    };
    Event {
        name: name::of::<name::AwsEvents>(&source.id),
        create_time: source.at,
        fleet: name::of::<name::Fleets>(&record.fleet),
        cause: None,
        change: EventChange::Aws {
            event: AwsEvent {
                id: source.id.clone(),
                at: source.at,
                kind,
            },
        },
    }
}

fn same_name<C: name::Collection>(existing: &str, id: &C::Id, scratch: &mut String) -> bool {
    name::write_of::<C>(scratch, id);
    existing == scratch
}

pub(crate) fn status_event_matches(
    event: &Event,
    record: &FleetInfrastructureEvent,
    pass: Option<&FleetPass>,
    records: &[FleetInfrastructureEvent],
    scratch: &mut String,
) -> bool {
    let source = &record.event;
    if event.create_time != source.at
        || event.cause.is_some()
        || !same_name::<name::AwsEvents>(&event.name, &source.id, scratch)
        || !same_name::<name::Fleets>(&event.fleet, &record.fleet, scratch)
    {
        return false;
    }
    let EventChange::Aws { event } = &event.change else {
        return false;
    };
    if event.id != source.id || event.at != source.at {
        return false;
    }
    match (&event.kind, &source.kind) {
        (
            AwsEventKind::RebalanceRecommendation { instance: a },
            InfrastructureEventKind::RebalanceRecommendation { instance: b },
        )
        | (
            AwsEventKind::InstanceLaunched { instance: a },
            InfrastructureEventKind::InstanceLaunched { instance: b },
        ) => same_name::<name::Instances>(a, b, scratch),
        (
            AwsEventKind::SpotInterruption {
                instance: a,
                action: x,
            },
            InfrastructureEventKind::SpotInterruption {
                instance: b,
                action: y,
            },
        ) => {
            let same_action = matches!(
                (x, y),
                (
                    AwsInterruptionAction::Terminate,
                    InfrastructureInterruptionAction::Terminate
                ) | (
                    AwsInterruptionAction::Stop,
                    InfrastructureInterruptionAction::Stop
                ) | (
                    AwsInterruptionAction::Hibernate,
                    InfrastructureInterruptionAction::Hibernate
                )
            );
            same_action && same_name::<name::Instances>(a, b, scratch)
        }
        (
            AwsEventKind::ReplacementLaunched {
                instance: a,
                replacing: x,
            },
            InfrastructureEventKind::ReplacementLaunched {
                instance: b,
                replacing: y,
            },
        ) => {
            let peer = y.as_ref().or_else(|| {
                pass.and_then(|pass| replacement_peer(pass, records, b, source.at, None))
            });
            same_name::<name::Instances>(a, b, scratch)
                && match (x, peer) {
                    (Some(name), Some(id)) => same_name::<name::Instances>(name, id, scratch),
                    (None, None) => true,
                    _ => false,
                }
        }
        (
            AwsEventKind::InstanceTerminated {
                instance: a,
                reason: x,
                reason_detail: detail,
            },
            InfrastructureEventKind::InstanceTerminated {
                instance: b,
                reason: y,
            },
        ) => {
            let same_reason = match (x, y) {
                (AwsTerminationReason::Rebalance, InfrastructureTerminationReason::Rebalance)
                | (
                    AwsTerminationReason::SpotReclaimed,
                    InfrastructureTerminationReason::SpotReclaimed,
                ) => detail.is_none(),
                (AwsTerminationReason::Other, InfrastructureTerminationReason::Other(text)) => {
                    detail.as_deref() == Some(text.as_str())
                }
                _ => false,
            };
            same_reason && same_name::<name::Instances>(a, b, scratch)
        }
        (
            AwsEventKind::CapacityUnavailable { reason: a },
            InfrastructureEventKind::CapacityUnavailable { reason: b },
        ) => a == b,
        _ => false,
    }
}

/// Ordinary terminations cannot close rebalance episodes without supporting launch evidence.
fn rebalance_termination(
    pass: &FleetPass,
    records: &[FleetInfrastructureEvent],
    record: &FleetInfrastructureEvent,
) -> bool {
    let InfrastructureEventKind::InstanceTerminated { instance, reason } = &record.event.kind
    else {
        return false;
    };
    if matches!(reason, InfrastructureTerminationReason::Rebalance) {
        return true;
    }
    records.iter().any(|launch| {
        if launch.fleet != pass.policy.name || launch.event.at > record.event.at {
            return false;
        }
        let InfrastructureEventKind::ReplacementLaunched { replacing, .. } = &launch.event.kind
        else {
            return false;
        };
        match replacing {
            Some(old) => old == instance,
            None => records.iter().any(|risk| {
                risk.fleet == pass.policy.name
                    && risk.event.at <= launch.event.at
                    && matches!(
                        &risk.event.kind,
                        InfrastructureEventKind::RebalanceRecommendation { instance: old }
                            | InfrastructureEventKind::SpotInterruption { instance: old, .. }
                            if old == instance
                    )
            }),
        }
    })
}

/// Finds boundaries only when all overlapping launches have completed. One termination
/// cannot split an ambiguous overlap and assign its remaining old member to a later launch.
fn rebalance_episode(
    pass: &FleetPass,
    records: &[FleetInfrastructureEvent],
    at: gang_clock::Timestamp,
) -> (Option<gang_clock::Timestamp>, Option<gang_clock::Timestamp>) {
    let mut cursor = None;
    let mut before = None;
    let mut pending = 0usize;
    loop {
        // Observations may arrive in any order. Walk their timestamps without sorting,
        // copying, or allocating a second collection of canonical facts.
        let next = records
            .iter()
            .filter(|record| {
                record.fleet == pass.policy.name
                    && cursor.is_none_or(|previous| record.event.at > previous)
                    && (matches!(
                        &record.event.kind,
                        InfrastructureEventKind::ReplacementLaunched { .. }
                    ) || rebalance_termination(pass, records, record))
            })
            .map(|record| record.event.at)
            .min();
        let Some(next) = next else {
            return (before, None);
        };
        let mut terminated = 0usize;
        for record in records
            .iter()
            .filter(|record| record.fleet == pass.policy.name && record.event.at == next)
        {
            match &record.event.kind {
                InfrastructureEventKind::ReplacementLaunched { .. } => pending += 1,
                InfrastructureEventKind::InstanceTerminated { .. }
                    if rebalance_termination(pass, records, record) =>
                {
                    terminated += 1;
                }
                _ => {}
            }
        }
        pending = pending.saturating_sub(terminated);
        if pending == 0 && terminated != 0 {
            if next >= at {
                return (before, Some(next));
            }
            before = Some(next);
        }
        cursor = Some(next);
    }
}

/// Returns the uniquely supported old member for a replacement, if the evidence identifies one.
#[must_use]
fn replacement_peer<'a>(
    pass: &'a FleetPass,
    records: &'a [FleetInfrastructureEvent],
    replacement: &InstanceId,
    at: gang_clock::Timestamp,
    explicit: Option<&'a InstanceId>,
) -> Option<&'a InstanceId> {
    if let Some(old) = explicit {
        return Some(old);
    }
    let (before, completed) = rebalance_episode(pass, records, at);
    if records.iter().any(|record| {
        record.fleet == pass.policy.name
            && before.is_none_or(|before| record.event.at > before)
            && completed.is_none_or(|completed| record.event.at <= completed)
            && matches!(
                &record.event.kind,
                InfrastructureEventKind::ReplacementLaunched { instance, .. }
                    if instance != replacement
            )
    }) {
        return None;
    }
    let mut identified: Option<&InstanceId> = None;
    for record in records
        .iter()
        .filter(|record| record.fleet == pass.policy.name)
    {
        let candidate = match &record.event.kind {
            InfrastructureEventKind::RebalanceRecommendation { instance }
            | InfrastructureEventKind::SpotInterruption { instance, .. }
                if record.event.at <= at =>
            {
                Some(instance)
            }
            InfrastructureEventKind::InstanceTerminated {
                instance,
                reason: InfrastructureTerminationReason::Rebalance,
            } if Some(record.event.at) == completed => Some(instance),
            _ => None,
        };
        if let Some(candidate) = candidate.filter(|candidate| {
            *candidate != replacement
                && !pass
                    .supply
                    .instances
                    .iter()
                    .any(|member| &member.id == *candidate && member.launched_at >= at)
                && !records.iter().any(|record| {
                    record.fleet == pass.policy.name
                        && match &record.event.kind {
                            InfrastructureEventKind::InstanceTerminated { instance, .. } => {
                                instance == *candidate && record.event.at < at
                            }
                            InfrastructureEventKind::InstanceLaunched { instance }
                            | InfrastructureEventKind::ReplacementLaunched { instance, .. } => {
                                instance == *candidate && record.event.at >= at
                            }
                            _ => false,
                        }
                })
        }) {
            if identified.is_some_and(|old| old != candidate) {
                return None;
            }
            identified = Some(candidate);
        }
    }
    if identified.is_some() || completed.is_some() {
        return identified;
    }
    let members = &pass.supply.instances;
    if members.len() <= pass.supply.requested.unwrap_or(pass.supply.desired) as usize {
        return None;
    }
    let mut candidates = members.iter().filter(|member| {
        &member.id != replacement && member.launched_at < at
            && !records.iter().any(|record| record.fleet == pass.policy.name
                && record.event.at < at
                && matches!(&record.event.kind, InfrastructureEventKind::InstanceTerminated { instance, .. } if instance == &member.id))
    });
    let first = candidates.next()?;
    if candidates.next().is_some() {
        None
    } else {
        Some(&first.id)
    }
}

fn live_count(pass: &FleetPass) -> usize {
    pass.supply
        .instances
        .iter()
        .filter(|member| member.lifecycle != gang_types::InstanceLifecycle::Terminating)
        .count()
}

pub(crate) struct MemberStatus<'a> {
    pub(crate) phase: InfrastructurePhase,
    pub(crate) reason: InfrastructureReason,
    pub(crate) other: Option<&'a InstanceId>,
}

pub(crate) fn member_status_ref<'a>(
    pass: &'a FleetPass,
    records: &'a [FleetInfrastructureEvent],
    member: &InstanceId,
) -> Option<MemberStatus<'a>> {
    let mut risk: Option<((gang_clock::Timestamp, &str), InfrastructureReason)> = None;
    let mut pairing: Option<((gang_clock::Timestamp, &str), &InstanceId)> = None;
    let mut replacement: Option<((gang_clock::Timestamp, &str), Option<&InstanceId>)> = None;
    for record in records
        .iter()
        .filter(|record| record.fleet == pass.policy.name)
    {
        let key = (record.event.at, record.event.id.as_str());
        let reason = match &record.event.kind {
            InfrastructureEventKind::RebalanceRecommendation { instance } if instance == member => {
                Some(InfrastructureReason::SpotRebalance)
            }
            InfrastructureEventKind::SpotInterruption { instance, .. } if instance == member => {
                Some(InfrastructureReason::SpotInterruption)
            }
            InfrastructureEventKind::ReplacementLaunched {
                instance,
                replacing,
            } => {
                let peer =
                    replacement_peer(pass, records, instance, record.event.at, replacing.as_ref());
                if instance == member {
                    let active = match peer {
                        Some(old) => pass
                            .supply
                            .instances
                            .iter()
                            .any(|current| &current.id == old),
                        None => {
                            replacing.is_none()
                                && !records.iter().any(|later| {
                                    later.fleet == pass.policy.name
                                        && later.event.at >= record.event.at
                                        && rebalance_termination(pass, records, later)
                                })
                                && live_count(pass)
                                    > pass.supply.requested.unwrap_or(pass.supply.desired) as usize
                        }
                    };
                    if active
                        && replacement
                            .as_ref()
                            .is_none_or(|(previous, _)| key > *previous)
                    {
                        replacement = Some((key, peer));
                    }
                    None
                } else if peer == Some(member)
                    && pass
                        .supply
                        .instances
                        .iter()
                        .any(|current| &current.id == instance)
                {
                    if pairing.as_ref().is_none_or(|(previous, _)| key > *previous) {
                        pairing = Some((key, instance));
                    }
                    None
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(reason) = reason
            && risk.as_ref().is_none_or(|(previous, _)| key > *previous)
        {
            risk = Some((key, reason));
        }
    }
    // A member's own newer risk is not its historical role as somebody else's replacement.
    // Keep the provider's interruption reason rather than replacing it with a launch reason.
    if let Some((_, reason)) = risk {
        return Some(MemberStatus {
            phase: InfrastructurePhase::Replacing,
            reason,
            other: pairing.map(|(_, peer)| peer),
        });
    }
    if let Some((_, peer)) = replacement {
        return Some(MemberStatus {
            phase: InfrastructurePhase::Replacement,
            reason: InfrastructureReason::SpotRebalance,
            other: peer,
        });
    }
    pairing.map(|(_, peer)| MemberStatus {
        phase: InfrastructurePhase::Replacing,
        reason: InfrastructureReason::SpotRebalance,
        other: Some(peer),
    })
}
