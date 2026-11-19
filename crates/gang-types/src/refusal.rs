//! Explanations for requests that no fleet accepts.

use std::fmt::{self, Display, Formatter};

/// Why no fleet accepts a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// An undeclared request property.
    Unknown {
        /// The unrecognized property name.
        key: String,
    },
    /// A capability value is malformed.
    InvalidCapability {
        /// The requested property.
        key: &'static str,
        /// The malformed value.
        requested: String,
        /// The closed-enum parser error.
        error: crate::UnknownVariantError,
    },
    /// A property is incompatible with the baseline-eligible fleets.
    Incompatible {
        /// The requested property.
        key: &'static str,
        /// The requested value.
        requested: String,
        /// Fleets eligible before applying this property, in preference order.
        fleets: Vec<String>,
    },
    /// A minimum key asks for more than the largest fleet offers.
    Minimum {
        /// The requested property.
        key: &'static str,
        /// The requested value.
        requested: String,
        /// The largest limit offered by a fleet.
        largest: u64,
    },
    /// A minimum key has a value that is not a non-negative integer.
    InvalidMinimum {
        /// The requested property.
        key: &'static str,
        /// The requested value.
        requested: String,
    },
    /// An exact key asks for a value no fleet declares.
    Exact {
        /// The requested property.
        key: &'static str,
        /// The requested value.
        requested: String,
        /// Distinct fleet values, sorted in ascending order.
        offered: Vec<String>,
    },
    /// Every key holds on some fleet, but no single fleet holds them all.
    Combination,
}

impl Display for Refusal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { key } => write!(f, "unknown property {key}"),
            Self::InvalidCapability { key, error, .. } => write!(f, "{key}: {error}"),
            Self::Incompatible {
                key,
                requested,
                fleets,
            } => write!(
                f,
                "{key} {requested} matches no eligible fleet (eligible fleets: {})",
                fleets.join(", ")
            ),
            Self::Minimum {
                key,
                requested,
                largest,
            } => {
                write!(
                    f,
                    "{key} {requested} is above the largest fleet's {largest}"
                )
            }
            Self::InvalidMinimum { key, requested } => {
                write!(f, "{key} {requested} is not a non-negative integer")
            }
            Self::Exact {
                key,
                requested,
                offered,
            } if offered.is_empty() => {
                write!(
                    f,
                    "{key} {requested} matches no fleet (no fleet declares {key})"
                )
            }
            Self::Exact {
                key,
                requested,
                offered,
            } => {
                write!(f, "{key} {requested} matches no fleet (fleets offer ")?;
                for (index, value) in offered.iter().enumerate() {
                    if index != 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(value)?;
                }
                f.write_str(")")
            }
            Self::Combination => f.write_str("no single fleet accepts every requested property"),
        }
    }
}
