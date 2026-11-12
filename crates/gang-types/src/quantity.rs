//! Shared parser and formatter for `<digits><unit>` quantities, such as byte sizes and durations.
//!
//! A quantity is a non-negative integer with no whitespace or fraction, followed by one
//! case-sensitive unit from a [`Units`] table. A value is written with the largest unit that
//! divides it exactly, so the written form parses back to the same value.

use std::fmt;

use crate::QuantityError;

/// The units of one kind of quantity.
pub(crate) struct Units {
    /// What is being parsed, such as `size` or `duration`, used in error messages.
    pub kind: &'static str,
    /// Unit text and factor, ordered by factor descending (largest first). The last entry must
    /// have a factor of 1 so that every value can be written.
    pub table: &'static [(&'static str, u64)],
    /// The unit written for a zero value.
    pub zero_unit: &'static str,
}

impl Units {
    /// Parses `<digits><unit>` into `count * factor`.
    ///
    /// The unit text starts at the first ASCII letter and is matched case-sensitively.
    ///
    /// # Errors
    ///
    /// Returns a [`QuantityError`] for empty input, a missing or malformed number, a missing or
    /// unknown unit, or a value that does not fit in a `u64`.
    pub(crate) fn parse(&self, input: &str) -> Result<u64, QuantityError> {
        let kind = self.kind;
        if input.is_empty() {
            return Err(QuantityError::Empty {
                kind,
                expected: self.expected(),
            });
        }
        let unit_start = input
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(input.len());
        let (number, unit) = input.split_at(unit_start);
        if number.is_empty() {
            return Err(QuantityError::MissingNumber {
                kind,
                input: input.to_owned(),
                expected: self.expected(),
            });
        }
        if !number.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(QuantityError::InvalidNumber {
                kind,
                input: input.to_owned(),
            });
        }
        if unit.is_empty() {
            return Err(QuantityError::MissingUnit {
                kind,
                input: input.to_owned(),
                expected: self.expected(),
            });
        }
        let Some(&(_, factor)) = self.table.iter().find(|&&(name, _)| name == unit) else {
            return Err(QuantityError::UnknownUnit {
                kind,
                unit: unit.to_owned(),
                expected: self.expected(),
            });
        };
        let overflow = || QuantityError::Overflow {
            kind,
            input: input.to_owned(),
        };
        let count: u64 = number.parse().map_err(|_| overflow())?;
        count.checked_mul(factor).ok_or_else(overflow)
    }

    /// Writes `value` with the largest unit that divides it exactly, as `{value / factor}{unit}`.
    /// Zero is written as `0{zero_unit}`.
    ///
    /// # Errors
    ///
    /// Returns [`fmt::Error`] only if the formatter fails, or if the table does not end in a
    /// factor of 1, which would leave some values unwritable.
    pub(crate) fn write(&self, formatter: &mut fmt::Formatter<'_>, value: u128) -> fmt::Result {
        if value == 0 {
            return write!(formatter, "0{}", self.zero_unit);
        }
        for &(unit, factor) in self.table {
            let factor = u128::from(factor);
            if value.is_multiple_of(factor) {
                return write!(formatter, "{}{unit}", value / factor);
            }
        }
        Err(fmt::Error)
    }

    /// The accepted unit names, smallest first, joined with `, `.
    fn expected(&self) -> String {
        self.table
            .iter()
            .rev()
            .map(|&(unit, _)| unit)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::Units;
    use crate::QuantityError;

    static SIZE: Units = Units {
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

    static DURATION: Units = Units {
        kind: "duration",
        table: &[
            ("d", 86_400_000),
            ("h", 3_600_000),
            ("m", 60_000),
            ("s", 1_000),
            ("ms", 1),
        ],
        zero_unit: "s",
    };

    /// Displays a value through [`Units::write`].
    struct Written(&'static Units, u128);

    impl fmt::Display for Written {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.write(formatter, self.1)
        }
    }

    const SIZE_EXPECTED: &str = "B, KiB, MiB, GiB, TiB";
    const DURATION_EXPECTED: &str = "ms, s, m, h, d";

    #[test]
    fn parses_every_unit() {
        assert_eq!(SIZE.parse("1B"), Ok(1));
        assert_eq!(SIZE.parse("1KiB"), Ok(1 << 10));
        assert_eq!(SIZE.parse("1MiB"), Ok(1 << 20));
        assert_eq!(SIZE.parse("1GiB"), Ok(1 << 30));
        assert_eq!(SIZE.parse("1TiB"), Ok(1 << 40));
        assert_eq!(DURATION.parse("1ms"), Ok(1));
        assert_eq!(DURATION.parse("1s"), Ok(1_000));
        assert_eq!(DURATION.parse("1m"), Ok(60_000));
        assert_eq!(DURATION.parse("1h"), Ok(3_600_000));
        assert_eq!(DURATION.parse("1d"), Ok(86_400_000));
        assert_eq!(DURATION.parse("0s"), Ok(0));
    }

    #[test]
    fn rejects_empty_input() {
        assert_eq!(
            SIZE.parse(""),
            Err(QuantityError::Empty {
                kind: "size",
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse(""),
            Err(QuantityError::Empty {
                kind: "duration",
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_number() {
        assert_eq!(
            SIZE.parse("GiB"),
            Err(QuantityError::MissingNumber {
                kind: "size",
                input: "GiB".to_owned(),
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse("ms"),
            Err(QuantityError::MissingNumber {
                kind: "duration",
                input: "ms".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_invalid_number() {
        for input in ["1.5s", "-1s", " 1s", "1_000s", "1 s", "+1s"] {
            assert_eq!(
                DURATION.parse(input),
                Err(QuantityError::InvalidNumber {
                    kind: "duration",
                    input: input.to_owned(),
                }),
                "input {input:?}"
            );
        }
        assert_eq!(
            SIZE.parse("1.5GiB"),
            Err(QuantityError::InvalidNumber {
                kind: "size",
                input: "1.5GiB".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_unit() {
        assert_eq!(
            SIZE.parse("29"),
            Err(QuantityError::MissingUnit {
                kind: "size",
                input: "29".to_owned(),
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse("10"),
            Err(QuantityError::MissingUnit {
                kind: "duration",
                input: "10".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn unknown_unit_lists_accepted_units_smallest_first() {
        assert_eq!(
            DURATION.parse("10x"),
            Err(QuantityError::UnknownUnit {
                kind: "duration",
                unit: "x".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            SIZE.parse("29KB"),
            Err(QuantityError::UnknownUnit {
                kind: "size",
                unit: "KB".to_owned(),
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_compound_durations() {
        assert_eq!(
            DURATION.parse("1h30m"),
            Err(QuantityError::UnknownUnit {
                kind: "duration",
                unit: "h30m".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn units_are_case_sensitive() {
        assert_eq!(
            SIZE.parse("1kib"),
            Err(QuantityError::UnknownUnit {
                kind: "size",
                unit: "kib".to_owned(),
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            SIZE.parse("1Gib"),
            Err(QuantityError::UnknownUnit {
                kind: "size",
                unit: "Gib".to_owned(),
                expected: SIZE_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse("1M"),
            Err(QuantityError::UnknownUnit {
                kind: "duration",
                unit: "M".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse("5MS"),
            Err(QuantityError::UnknownUnit {
                kind: "duration",
                unit: "MS".to_owned(),
                expected: DURATION_EXPECTED.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_numbers_beyond_u64() {
        assert_eq!(
            SIZE.parse("18446744073709551616B"),
            Err(QuantityError::Overflow {
                kind: "size",
                input: "18446744073709551616B".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_products_beyond_u64() {
        // 2^34 * 2^30 = 2^64 does not fit; one less does.
        assert_eq!(
            SIZE.parse("17179869184GiB"),
            Err(QuantityError::Overflow {
                kind: "size",
                input: "17179869184GiB".to_owned(),
            })
        );
        assert_eq!(
            SIZE.parse("17179869183GiB"),
            Ok(((1 << 34) - 1) * (1 << 30))
        );
        // 213503982335 * 86400000 exceeds u64::MAX; 213503982334 * 86400000 does not.
        assert_eq!(
            DURATION.parse("213503982335d"),
            Err(QuantityError::Overflow {
                kind: "duration",
                input: "213503982335d".to_owned(),
            })
        );
        assert_eq!(
            DURATION.parse("213503982334d"),
            Ok(213_503_982_334 * 86_400_000)
        );
    }

    #[test]
    fn writes_largest_exact_unit() {
        assert_eq!(Written(&SIZE, 1 << 40).to_string(), "1TiB");
        assert_eq!(Written(&SIZE, 1536 << 20).to_string(), "1536MiB");
        assert_eq!(Written(&SIZE, 1 << 10).to_string(), "1KiB");
        assert_eq!(Written(&SIZE, 1500).to_string(), "1500B");
        assert_eq!(Written(&DURATION, 90_000).to_string(), "90s");
        assert_eq!(Written(&DURATION, 120_000).to_string(), "2m");
        assert_eq!(Written(&DURATION, 3_600_000).to_string(), "1h");
        assert_eq!(Written(&DURATION, 172_800_000).to_string(), "2d");
        assert_eq!(Written(&DURATION, 1_500).to_string(), "1500ms");
        assert_eq!(Written(&DURATION, 90_060_000).to_string(), "1501m");
    }

    #[test]
    fn writes_zero_with_zero_unit() {
        assert_eq!(Written(&SIZE, 0).to_string(), "0B");
        assert_eq!(Written(&DURATION, 0).to_string(), "0s");
    }
}
