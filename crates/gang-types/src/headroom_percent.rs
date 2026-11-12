//! Percentage added to a reservation as headroom.

/// A percentage added on top of a reservation; zero adds nothing and values may exceed 100.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct HeadroomPercent(u16);

impl HeadroomPercent {
    /// Creates headroom from an integer percentage.
    #[must_use]
    pub const fn from_percent(percent: u16) -> Self {
        Self(percent)
    }

    /// Returns the percentage added to the reservation.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Adds headroom to `base`, rounding down and saturating at `u64::MAX`.
    #[must_use]
    pub fn scale_u64(self, base: u64) -> u64 {
        let scaled = u128::from(base) * (100 + u128::from(self.0)) / 100;
        u64::try_from(scaled).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::HeadroomPercent;

    #[test]
    fn adds_headroom_and_rounds_down() {
        assert_eq!(HeadroomPercent::from_percent(0).scale_u64(3), 3);
        assert_eq!(HeadroomPercent::from_percent(50).scale_u64(3), 4);
        assert_eq!(HeadroomPercent::from_percent(100).scale_u64(3), 6);
        assert_eq!(HeadroomPercent::from_percent(150).scale_u64(4), 10);
    }

    #[test]
    fn saturates_only_after_scaling() {
        assert_eq!(
            HeadroomPercent::from_percent(0).scale_u64(u64::MAX),
            u64::MAX
        );
        assert_eq!(
            HeadroomPercent::from_percent(50).scale_u64(u64::MAX),
            u64::MAX
        );
    }

    #[test]
    fn serde_round_trips_an_integer() {
        let value = HeadroomPercent::from_percent(150);
        assert_eq!(
            serde_json::to_string(&value).expect("serialize headroom"),
            "150"
        );
        assert_eq!(
            serde_json::from_str::<HeadroomPercent>("150").expect("parse headroom"),
            value
        );
    }

    #[test]
    fn accepts_full_u16_range_and_rejects_larger_values() {
        let maximum = HeadroomPercent::from_percent(u16::MAX);
        assert_eq!(maximum.get(), u16::MAX);
        assert_eq!(
            serde_json::from_str::<HeadroomPercent>("65535").expect("parse maximum"),
            maximum
        );
        assert!(serde_json::from_str::<HeadroomPercent>("65536").is_err());
        assert!(serde_json::from_str::<HeadroomPercent>("-1").is_err());
    }
}
