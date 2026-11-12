//! Byte sizes written as `<digits><unit>`, such as `29GiB`.

use std::fmt;
use std::str::FromStr;

use crate::QuantityError;
use crate::quantity::Units;

/// A size in bytes.
///
/// Parses from `<digits><unit>` with no whitespace or fraction, where the unit is one of
/// `B`, `KiB`, `MiB`, `GiB` or `TiB` (powers of 1024). [`Display`](fmt::Display) prints the
/// largest unit that divides the size exactly, so the two round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bytes(u64);

impl Bytes {
    /// Creates a size of `count` bytes.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// Returns the size in bytes.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds two sizes, returning `None` on overflow.
    #[must_use]
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(sum) => Some(Self(sum)),
            None => None,
        }
    }

    /// Adds two sizes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Subtracts `rhs`, clamping at zero.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Creates a size of `count` kibibytes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn kib(count: u64) -> Self {
        Self(count.saturating_mul(1 << 10))
    }

    /// Creates a size of `count` mebibytes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn mib(count: u64) -> Self {
        Self(count.saturating_mul(1 << 20))
    }

    /// Creates a size of `count` gibibytes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn gib(count: u64) -> Self {
        Self(count.saturating_mul(1 << 30))
    }

    /// Creates a size of `count` tebibytes, saturating at `u64::MAX` bytes.
    #[must_use]
    pub const fn tib(count: u64) -> Self {
        Self(count.saturating_mul(1 << 40))
    }

    /// Returns the size as a `usize`, saturating on targets where `usize` is narrower than `u64`.
    #[must_use]
    pub fn as_usize(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }
}

impl From<u64> for Bytes {
    fn from(count: u64) -> Self {
        Self(count)
    }
}

impl From<Bytes> for u64 {
    fn from(size: Bytes) -> Self {
        size.0
    }
}

const BYTES: Units = Units {
    kind: "size",
    table: &[
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ],
    zero_unit: "B",
};

impl FromStr for Bytes {
    type Err = QuantityError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        BYTES.parse(input).map(Self)
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        BYTES.write(f, u128::from(self.0))
    }
}

impl_parsed_serde!(Bytes, "a size such as \"29GiB\"");

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn json_deserializes_borrowed_and_owned_strings() {
        let expected = Bytes::gib(29);
        assert_eq!(
            serde_json::from_str::<Bytes>("\"29GiB\"").expect("valid size"),
            expected
        );
        assert_eq!(
            serde_json::from_value::<Bytes>(serde_json::Value::String("29GiB".to_owned()))
                .expect("valid size"),
            expected
        );
    }

    #[test]
    fn saturating_add_clamps_at_maximum() {
        assert_eq!(
            Bytes::new(u64::MAX).saturating_add(Bytes::new(1)),
            Bytes::new(u64::MAX)
        );
        assert_eq!(Bytes::new(1).saturating_add(Bytes::new(2)), Bytes::new(3));
    }

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        size: Bytes,
    }

    #[test]
    fn parses_exact_units() {
        assert_eq!("0B".parse::<Bytes>(), Ok(Bytes::new(0)));
        assert_eq!("512MiB".parse::<Bytes>(), Ok(Bytes::new(512 * MIB)));
        assert_eq!("29GiB".parse::<Bytes>(), Ok(Bytes::new(29 * GIB)));
        assert_eq!("1TiB".parse::<Bytes>(), Ok(Bytes::new(1 << 40)));
    }

    #[test]
    fn rejects_malformed_sizes() {
        assert!(matches!(
            "".parse::<Bytes>(),
            Err(QuantityError::Empty { .. })
        ));
        assert!(matches!(
            "GiB".parse::<Bytes>(),
            Err(QuantityError::MissingNumber { .. })
        ));
        assert!(matches!(
            "29".parse::<Bytes>(),
            Err(QuantityError::MissingUnit { .. })
        ));
        assert!(matches!(
            "29gib".parse::<Bytes>(),
            Err(QuantityError::UnknownUnit { .. })
        ));
        assert!(matches!(
            "29GB".parse::<Bytes>(),
            Err(QuantityError::UnknownUnit { .. })
        ));
        assert!(matches!(
            "29 GiB".parse::<Bytes>(),
            Err(QuantityError::InvalidNumber { .. })
        ));
        assert!(matches!(
            "1.5GiB".parse::<Bytes>(),
            Err(QuantityError::InvalidNumber { .. })
        ));
        assert!(matches!(
            "-1B".parse::<Bytes>(),
            Err(QuantityError::InvalidNumber { .. })
        ));
        assert!(matches!(
            "99999999999999999999B".parse::<Bytes>(),
            Err(QuantityError::Overflow { .. })
        ));
        assert!(matches!(
            "18446744073709551615TiB".parse::<Bytes>(),
            Err(QuantityError::Overflow { .. })
        ));
    }

    #[test]
    fn unknown_unit_is_rejected() {
        assert!(matches!(
            "29XB".parse::<Bytes>(),
            Err(QuantityError::UnknownUnit { .. })
        ));
    }

    #[test]
    fn display_uses_largest_exact_unit_and_round_trips() {
        let cases = [
            (0, "0B"),
            (1, "1B"),
            (1500, "1500B"),
            (1024, "1KiB"),
            (512 * MIB, "512MiB"),
            (1536 * MIB, "1536MiB"),
            (29 * GIB, "29GiB"),
            (1 << 40, "1TiB"),
        ];
        for (count, text) in cases {
            let size = Bytes::new(count);
            assert_eq!(size.to_string(), text);
            assert_eq!(text.parse::<Bytes>(), Ok(size));
        }
    }

    #[test]
    fn checked_add_and_saturating_sub() {
        assert_eq!(
            Bytes::new(1).checked_add(Bytes::new(2)),
            Some(Bytes::new(3))
        );
        assert_eq!(Bytes::new(u64::MAX).checked_add(Bytes::new(1)), None);
        assert_eq!(Bytes::new(1).saturating_sub(Bytes::new(5)), Bytes::new(0));
    }

    #[test]
    fn unit_constructors_scale_by_powers_of_1024() {
        assert_eq!(Bytes::kib(3).get(), 3 << 10);
        assert_eq!(Bytes::mib(3).get(), 3 << 20);
        assert_eq!(Bytes::gib(8).get(), 8 << 30);
        assert_eq!(Bytes::tib(2).get(), 2 << 40);
    }

    #[test]
    fn unit_constructors_saturate_at_max() {
        assert_eq!(Bytes::tib(u64::MAX).get(), u64::MAX);
        assert_eq!(Bytes::gib(u64::MAX).get(), u64::MAX);
        assert_eq!(Bytes::kib(u64::MAX).get(), u64::MAX);
    }

    #[test]
    fn as_usize_returns_size_or_saturates() {
        assert_eq!(Bytes::new(0).as_usize(), 0);
        assert_eq!(u64::try_from(Bytes::gib(1).as_usize()).unwrap(), GIB);
        assert_eq!(Bytes::new(u64::MAX).as_usize(), usize::MAX);
    }

    #[test]
    fn toml_round_trips_as_string() {
        let holder = Holder {
            size: Bytes::new(29 * GIB),
        };
        let text = toml::to_string(&holder).unwrap();
        assert_eq!(text.trim(), "size = \"29GiB\"");
        assert_eq!(toml::from_str::<Holder>(&text).unwrap(), holder);
    }

    #[test]
    fn json_round_trips_as_string() {
        let text = serde_json::to_string(&Bytes::new(1500)).unwrap();
        assert_eq!(text, "\"1500B\"");
        assert_eq!(
            serde_json::from_str::<Bytes>(&text).unwrap(),
            Bytes::new(1500)
        );
    }

    #[test]
    fn rejects_bare_integers() {
        assert!(serde_json::from_str::<Bytes>("29").is_err());
        assert!(toml::from_str::<Holder>("size = 29").is_err());
    }
}
