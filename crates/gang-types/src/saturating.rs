//! Lossy integer conversions that clamp to the target's maximum instead of failing.

/// Converts a value, saturating at the target's maximum instead of failing.
pub trait SaturatingInto<T> {
    /// Converts, saturating at the target's maximum instead of failing.
    fn saturating_into(self) -> T;
}

macro_rules! saturating_impl {
    ($from:ty => $to:ty) => {
        impl SaturatingInto<$to> for $from {
            fn saturating_into(self) -> $to {
                <$to>::try_from(self).unwrap_or(<$to>::MAX)
            }
        }
    };
}

saturating_impl!(usize => u32);
saturating_impl!(u64 => u32);
saturating_impl!(u32 => usize);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_that_fit_are_preserved() {
        assert_eq!(5_usize.saturating_into(), 5_u32);
        assert_eq!(7_u32.saturating_into(), 7_usize);
        assert_eq!(9_u64.saturating_into(), 9_u32);
    }

    #[test]
    fn values_above_maximum_clamp() {
        let above: u64 = u64::from(u32::MAX) + 1;
        assert_eq!(above.saturating_into(), u32::MAX);
        assert_eq!(u64::MAX.saturating_into(), u32::MAX);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn usize_maximum_clamps_to_u32_maximum() {
        assert_eq!(usize::MAX.saturating_into(), u32::MAX);
    }
}
