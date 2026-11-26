//! Quality-of-service (`QoS`) priority classification and `cgroup` v2 `cpu.weight` mapping.
//!
//! Priority stays higher-runs-first across the scheduler. This module owns the single mapping from
//! `REAPI` `ExecutionPolicy.priority` onto [`PriorityTier`] and [`CpuWeight`], plus the gate-tier
//! audit marker emitted when a caller submits gate-priority work (`priority >= 200`).

use std::fmt;

use gang_types::{CpuWeight, SourceId};
use serde::{Deserialize, Serialize};

/// Minimum `ExecutionPolicy.priority` classified into [`PriorityTier::Gate`].
pub const GATE_PRIORITY_THRESHOLD: i32 = 200;

/// Minimum `ExecutionPolicy.priority` classified into [`PriorityTier::Interactive`].
pub const INTERACTIVE_PRIORITY_THRESHOLD: i32 = 100;

/// Scheduler `QoS` tier derived from `ExecutionPolicy.priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriorityTier {
    /// Background work (`priority < 100`, including `0` and negative values).
    Background,
    /// Interactive developer builds (`100..=199`).
    Interactive,
    /// Gate / release-critical builds (`priority >= 200`).
    Gate,
}

impl PriorityTier {
    /// Classifies an `REAPI` `ExecutionPolicy.priority` into its [`PriorityTier`].
    #[must_use]
    pub const fn from_priority(priority: i32) -> Self {
        if priority >= GATE_PRIORITY_THRESHOLD {
            Self::Gate
        } else if priority >= INTERACTIVE_PRIORITY_THRESHOLD {
            Self::Interactive
        } else {
            Self::Background
        }
    }

    /// Returns the `cgroup` v2 [`CpuWeight`] assigned to this tier.
    #[must_use]
    pub const fn weight(self) -> CpuWeight {
        match self {
            Self::Gate => CpuWeight::GATE,
            Self::Interactive => CpuWeight::INTERACTIVE,
            Self::Background => CpuWeight::BACKGROUND,
        }
    }

    /// Returns `true` when this tier is [`PriorityTier::Gate`].
    #[must_use]
    pub const fn is_gate(self) -> bool {
        matches!(self, Self::Gate)
    }

    /// Returns the canonical lowercase name of this tier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gate => "gate",
            Self::Interactive => "interactive",
            Self::Background => "background",
        }
    }
}

impl fmt::Display for PriorityTier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Audit marker emitted when an admitted operation enters the gate tier (`priority >= 200`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateAudit {
    /// Authenticated caller source, if present.
    pub source: Option<SourceId>,
    /// Admitted `ExecutionPolicy.priority`.
    pub priority: i32,
}

/// Classifies an `REAPI` `ExecutionPolicy.priority` into its [`PriorityTier`].
#[must_use]
pub const fn classify(priority: i32) -> PriorityTier {
    PriorityTier::from_priority(priority)
}

/// Maps an `REAPI` `ExecutionPolicy.priority` onto its single-core `cgroup` v2 [`CpuWeight`].
#[must_use]
pub const fn weight(priority: i32) -> CpuWeight {
    CpuWeight::from_priority(priority)
}

/// Maps an `REAPI` `ExecutionPolicy.priority` and reserved CPU millicores onto a core-scaled
/// `cgroup` v2 [`CpuWeight`] (`clamp(tier_per_core * max(1, ceil(millicores / 1000)), 1, 10_000)`).
#[must_use]
pub const fn weight_for_reservation(priority: i32, reserved_cpu_millicores: u64) -> CpuWeight {
    CpuWeight::for_priority_and_millicores(priority, reserved_cpu_millicores)
}

/// Returns `true` when `priority` falls in the gate tier (`priority >= 200`).
#[must_use]
pub const fn is_gate(priority: i32) -> bool {
    priority >= GATE_PRIORITY_THRESHOLD
}

/// Returns a [`GateAudit`] marker if and only if `priority` is in the gate tier (`>= 200`).
#[must_use]
pub const fn gate_audit(source: Option<SourceId>, priority: i32) -> Option<GateAudit> {
    if is_gate(priority) {
        Some(GateAudit { source, priority })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_classification_and_weights_cover_all_boundaries() {
        let cases = [
            (
                i32::MIN,
                PriorityTier::Background,
                CpuWeight::BACKGROUND,
                20,
            ),
            (-100, PriorityTier::Background, CpuWeight::BACKGROUND, 20),
            (-1, PriorityTier::Background, CpuWeight::BACKGROUND, 20),
            (0, PriorityTier::Background, CpuWeight::BACKGROUND, 20),
            (99, PriorityTier::Background, CpuWeight::BACKGROUND, 20),
            (100, PriorityTier::Interactive, CpuWeight::INTERACTIVE, 100),
            (199, PriorityTier::Interactive, CpuWeight::INTERACTIVE, 100),
            (200, PriorityTier::Gate, CpuWeight::GATE, 1000),
            (201, PriorityTier::Gate, CpuWeight::GATE, 1000),
            (i32::MAX, PriorityTier::Gate, CpuWeight::GATE, 1000),
        ];

        for (priority, expected_tier, expected_weight, expected_raw) in cases {
            assert_eq!(classify(priority), expected_tier, "priority {priority}");
            assert_eq!(weight(priority), expected_weight, "priority {priority}");
            assert_eq!(
                expected_tier.weight().get(),
                expected_raw,
                "priority {priority}"
            );
            assert_eq!(
                is_gate(priority),
                expected_tier.is_gate(),
                "priority {priority}"
            );
        }
    }

    #[test]
    fn gate_audit_emits_only_for_gate_tier() {
        let source = SourceId::from_identity_str("example:alice");
        assert!(source.is_some());

        assert_eq!(gate_audit(source, 199), None);
        assert_eq!(gate_audit(source, 100), None);
        assert_eq!(gate_audit(source, 0), None);
        assert_eq!(gate_audit(source, -1), None);

        assert_eq!(
            gate_audit(source, 200),
            Some(GateAudit {
                source,
                priority: 200,
            })
        );
        assert_eq!(
            gate_audit(None, i32::MAX),
            Some(GateAudit {
                source: None,
                priority: i32::MAX,
            })
        );
    }

    #[test]
    fn tier_ordering_and_display() {
        assert!(PriorityTier::Background < PriorityTier::Interactive);
        assert!(PriorityTier::Interactive < PriorityTier::Gate);
        assert_eq!(PriorityTier::Background.to_string(), "background");
        assert_eq!(PriorityTier::Interactive.to_string(), "interactive");
        assert_eq!(PriorityTier::Gate.to_string(), "gate");
    }
}
