use std::ops::Add;
use std::time::Duration;

/// A monotonic reading, measured as time since an arbitrary per-clock origin.
///
/// Ticks are only comparable with ticks taken from the same clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Tick(Duration);

impl Tick {
    /// The origin of a clock, the reading at which it was created.
    pub const ORIGIN: Self = Self(Duration::ZERO);

    /// Creates a tick that lies `elapsed` after the origin.
    #[must_use]
    pub const fn from_elapsed(elapsed: Duration) -> Self {
        Self(elapsed)
    }

    /// Returns the time elapsed since the origin.
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.0
    }

    /// Returns the duration from `earlier` to `self`, or zero when `earlier` is later.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

impl Add<Duration> for Tick {
    type Output = Self;

    /// Advances the tick by `rhs`, saturating at the largest representable duration.
    fn add(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(rhs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn later_elapsed_orders_after_earlier() {
        let earlier = Tick::from_elapsed(Duration::from_secs(1));
        let later = Tick::from_elapsed(Duration::from_secs(2));
        assert!(earlier < later);
        assert!(Tick::ORIGIN < earlier);
    }

    #[test]
    fn add_advances_elapsed() {
        let tick = Tick::ORIGIN + Duration::from_millis(5);
        assert_eq!(tick.elapsed(), Duration::from_millis(5));
    }

    #[test]
    fn add_saturates_at_maximum() {
        let tick = Tick::from_elapsed(Duration::MAX) + Duration::from_secs(1);
        assert_eq!(tick, Tick::from_elapsed(Duration::MAX));
    }

    #[test]
    fn duration_since_earlier_is_difference() {
        let earlier = Tick::from_elapsed(Duration::from_millis(100));
        let later = Tick::from_elapsed(Duration::from_millis(350));
        assert_eq!(
            later.saturating_duration_since(earlier),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn duration_since_later_is_zero() {
        let earlier = Tick::from_elapsed(Duration::from_millis(100));
        let later = Tick::from_elapsed(Duration::from_millis(350));
        assert_eq!(earlier.saturating_duration_since(later), Duration::ZERO);
    }
}
