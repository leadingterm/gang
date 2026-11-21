//! Event-time completion moments shared by the scheduler and the planner.

use std::fmt;

use gang_clock::Tick;
use gang_types::SourceId;

/// A finite, nonnegative scalar with one canonical representation for zero.
///
/// Completion moments retain full precision until the planner produces display metrics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct NonNegativeFinite(u64);

impl NonNegativeFinite {
    /// Returns `None` for negative, infinite or NaN values; normalizes negative zero.
    #[must_use]
    pub fn try_new(value: f64) -> Option<Self> {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        Some(Self(if value == 0.0 { 0 } else { value.to_bits() }))
    }

    /// Returns the unquantized numeric value.
    #[must_use]
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl fmt::Debug for NonNegativeFinite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// Decayed resource-time work per second, independent of any fleet's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceRates {
    /// Action-seconds per second (concurrent actions).
    pub actions: NonNegativeFinite,
    /// Millicore-seconds per second (millicores).
    pub cpu_millicores: NonNegativeFinite,
    /// Memory-byte-seconds per second (bytes).
    pub memory: NonNegativeFinite,
    /// Disk-byte-seconds per second (bytes).
    pub disk: NonNegativeFinite,
}

/// Event-time exponential moments for one source and preferred-fleet cell.
///
/// Ticks use the server's injected clock. Rows are unique and sorted by
/// `(source, preferred_fleet)` in a scheduler snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionRates {
    /// Authenticated source, or the reserved unattributed source.
    pub source: Option<SourceId>,
    /// Preferred fleet's catalog index, independent of the actual execution fleet.
    pub preferred_fleet: usize,
    /// Changes when the adapter cell is reused or reinitialized.
    pub generation: u64,
    /// Initialization of this cell under this generation.
    pub created_at: Tick,
    /// Last verified completion receipt.
    pub at: Tick,
    /// All verified completed actions per second, including untimed completions.
    pub completions: NonNegativeFinite,
    /// Verified completions with valid execution timing, per second.
    pub timed_completions: NonNegativeFinite,
    /// Work from valid timed completions only; equal start/end is valid zero work.
    pub work: ResourceRates,
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    fn hash(value: NonNegativeFinite) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn finite_moments_reject_invalid_values() {
        for value in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            -f64::MIN_POSITIVE,
            -f64::from_bits(1),
        ] {
            assert_eq!(NonNegativeFinite::try_new(value), None);
        }
    }

    #[test]
    fn finite_moments_normalize_zero_equality_and_hash() {
        let positive = NonNegativeFinite::try_new(0.0).unwrap();
        let negative = NonNegativeFinite::try_new(-0.0).unwrap();
        assert_eq!(positive, negative);
        assert_eq!(positive, NonNegativeFinite::default());
        assert_eq!(negative.get().to_bits(), 0.0_f64.to_bits());
        assert_eq!(hash(positive), hash(negative));
        assert_eq!(format!("{negative:?}"), "0.0");
    }

    #[test]
    fn finite_moments_preserve_full_precision_and_numeric_debug() {
        for value in [f64::from_bits(1), f64::MIN_POSITIVE, 0.1, 1.0, f64::MAX] {
            let moment = NonNegativeFinite::try_new(value).unwrap();
            assert_eq!(moment.get().to_bits(), value.to_bits());
            assert_eq!(moment, NonNegativeFinite::try_new(value).unwrap());
            assert_eq!(format!("{moment:?}"), format!("{value:?}"));
        }
        assert_ne!(
            NonNegativeFinite::try_new(1.0),
            NonNegativeFinite::try_new(1.0 + f64::EPSILON)
        );
        assert_eq!(
            ResourceRates::default().actions.get().to_bits(),
            0.0_f64.to_bits()
        );
    }
}
