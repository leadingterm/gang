//! The inputs of a rate-based decision, as copied into the step that carries it out.
//!
//! A step records these scalars when the planner chooses it. The audit text is formatted from
//! them only after the caller applied the step, so a pass that plans a decision allocates no
//! strings. Every float is a finite, non-negative [`Metric`], which keeps the types `Eq` and
//! the steps comparable in tests.

use std::time::Duration;

use gang_spi::NonNegativeFinite;

pub use crate::vectors::ScalingDimension;

/// A finite, non-negative scalar of a decision summary.
pub type Metric = NonNegativeFinite;

/// Converts a computed value into a [`Metric`].
///
/// A value that is not a number or is negative becomes zero and infinity saturates at the
/// largest finite value, so a summary never fails to record a decision.
#[must_use]
pub fn metric(value: f64) -> Metric {
    if value.is_nan() || value <= 0.0 {
        return Metric::default();
    }
    Metric::try_new(value.min(f64::MAX)).unwrap_or_default()
}

/// The resource inputs behind one scale-out decision.
///
/// Demand and capacity are worker-equivalents in the selected limiting dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScaleDecision {
    /// The dimension with the largest normalized demand.
    pub dimension: ScalingDimension,
    /// Demand in worker-equivalents in the limiting dimension.
    pub demand: Metric,
    /// Reliable serving capacity in worker-equivalents in the limiting dimension.
    pub capacity: Metric,
    /// Instances serving at the pass.
    pub reliable: u32,
    /// Instances booting or requested but not serving.
    pub booting: u32,
    /// Instances needed for the resource demand and configured minimum.
    pub needed: u32,
    /// Instances needed beyond the reliable ones.
    pub extra_instances: u32,
}

/// The inputs behind one utilization cordon. Resource values are worker-equivalents
/// in the selected limiting dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CordonDecision {
    /// The readiness the planner used.
    pub readiness: Duration,
    /// The payback horizon `H`.
    pub horizon: Duration,
    /// The dimension with the tightest demand to capacity ratio after the cordon.
    pub dimension: ScalingDimension,
    /// Instances serving before the cordon.
    pub serving_instances: u32,
    /// Capacity left without the victim, in the limiting dimension.
    pub remaining_capacity: Metric,
    /// All-source load charged to the fleet.
    pub total_load: Metric,
    /// Queued work waiting on the fleet.
    pub queued_work: Metric,
    /// The waterline, a percentage of capacity.
    pub waterline_percent: u8,
    /// How long the waterline has held without interruption.
    pub dwell: Duration,
    /// When the victim empties, counted from the pass.
    pub empty_after: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_clamps_invalid_values() {
        assert_eq!(metric(f64::NAN).get().to_bits(), 0.0_f64.to_bits());
        assert_eq!(metric(-1.0).get().to_bits(), 0.0_f64.to_bits());
        assert_eq!(metric(f64::INFINITY).get().to_bits(), f64::MAX.to_bits());
        assert_eq!(metric(2.5).get().to_bits(), 2.5_f64.to_bits());
    }

    #[test]
    fn summaries_default_to_zero() {
        let decision = ScaleDecision::default();
        assert_eq!(decision.extra_instances, 0);
        assert_eq!(decision.capacity.get().to_bits(), 0.0_f64.to_bits());
    }
}
