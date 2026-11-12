//! Construction of non-zero integers for constants and tests.

use std::num::NonZeroU32;

/// Constructs a non-zero integer, including in a constant expression.
///
/// Passing zero in a constant expression causes a compile error.
///
/// # Panics
///
/// Panics if `n` is zero.
#[must_use]
pub const fn nonzero_u32(n: u32) -> NonZeroU32 {
    assert!(n != 0, "value must be nonzero");
    NonZeroU32::MIN.saturating_add(n - 1)
}

#[cfg(test)]
mod tests {
    use super::nonzero_u32;

    const VALUE: std::num::NonZeroU32 = nonzero_u32(42);

    #[test]
    fn constructs_nonzero_values_including_boundaries() {
        assert_eq!(VALUE.get(), 42);
        assert_eq!(nonzero_u32(1).get(), 1);
        assert_eq!(nonzero_u32(u32::MAX).get(), u32::MAX);
    }

    #[test]
    #[should_panic(expected = "value must be nonzero")]
    fn zero_panics() {
        let _ = nonzero_u32(0);
    }
}
