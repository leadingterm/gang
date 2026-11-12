//! Typed errors for the value types in this crate.

use thiserror::Error;

use crate::MAX_FLEETS;

/// Why a `<digits><unit>` quantity (a byte size or a configuration duration) did not parse.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QuantityError {
    /// The input was empty.
    #[error("empty {kind}, expected a number followed by one of {expected}")]
    Empty {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The accepted units, smallest first.
        expected: String,
    },
    /// The input had no leading number.
    #[error("missing number in {kind} `{input}`, expected a number followed by one of {expected}")]
    MissingNumber {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The rejected input.
        input: String,
        /// The accepted units, smallest first.
        expected: String,
    },
    /// The number part contained something other than ASCII digits.
    #[error("invalid number in {kind} `{input}`, expected ASCII digits only")]
    InvalidNumber {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The rejected input.
        input: String,
    },
    /// The input had no unit suffix.
    #[error("missing unit in {kind} `{input}`, expected one of {expected}")]
    MissingUnit {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The rejected input.
        input: String,
        /// The accepted units, smallest first.
        expected: String,
    },
    /// The unit suffix is not one of the accepted units.
    #[error("unknown unit `{unit}` in {kind}, expected one of {expected}")]
    UnknownUnit {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The rejected unit suffix.
        unit: String,
        /// The accepted units, smallest first.
        expected: String,
    },
    /// The quantity does not fit in a `u64` number of bytes or milliseconds.
    #[error("{kind} `{input}` is too large")]
    Overflow {
        /// What was being parsed, such as `size` or `duration`.
        kind: &'static str,
        /// The rejected input.
        input: String,
    },
}

/// Errors from validating a name newtype such as [`ProviderName`](crate::ProviderName).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NameError {
    /// The value was empty.
    #[error("empty {kind}")]
    Empty {
        /// Human-readable name of the rejected type.
        kind: &'static str,
    },
    /// The value contained a character the type does not allow.
    #[error("invalid character '{character}' in {kind}")]
    InvalidCharacter {
        /// Human-readable name of the rejected type.
        kind: &'static str,
        /// The first disallowed character.
        character: char,
    },
    /// The value is longer than the type allows.
    #[error("{kind} is longer than {max} characters")]
    TooLong {
        /// Human-readable name of the rejected type.
        kind: &'static str,
        /// The longest value the type allows, in characters.
        max: usize,
    },
    /// The value starts or ends with a hyphen, which the type forbids at either edge.
    #[error("{kind} must not start or end with '-'")]
    HyphenAtEdge {
        /// Human-readable name of the rejected type.
        kind: &'static str,
    },
}

/// Errors from parsing an [`Identity`](crate::Identity) written as `provider:principal`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentityError {
    /// The text has no `:` between the provider and the principal.
    #[error("missing ':' in identity `{input}`, expected provider:principal")]
    MissingSeparator {
        /// The rejected input.
        input: String,
    },
    /// The text before the first `:` is not a valid provider name.
    #[error("invalid provider in identity: {0}")]
    Provider(#[source] NameError),
    /// The text after the first `:` is not a valid principal.
    #[error("invalid principal in identity: {0}")]
    Principal(#[source] NameError),
}

/// Errors from validating a PEM newtype such as [`CertificatePem`](crate::CertificatePem).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PemError {
    /// The text is not a PEM block with the required label.
    #[error("not a PEM {label} block")]
    Malformed {
        /// The label the block must carry, such as `CERTIFICATE`.
        label: &'static str,
    },
}

/// Errors from parsing a [`CaPin`](crate::CaPin).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CaPinError {
    /// The text does not start with `sha256:`.
    #[error("a ca pin starts with sha256:")]
    MissingPrefix,
    /// The text does not have 64 hex characters after the prefix.
    #[error("a ca pin has 64 hex characters after sha256:, got {found}")]
    Length {
        /// The number of hex characters found after the prefix.
        found: usize,
    },
    /// A character is not a hex digit.
    #[error("invalid character '{character}' in ca pin, expected a hex digit")]
    InvalidCharacter {
        /// The first character that is not a hex digit.
        character: char,
    },
}

/// Errors from building a fleet catalog.
#[derive(Debug, PartialEq, Eq, Error)]
#[error("{count} fleets configured, at most {} are supported", MAX_FLEETS)]
pub struct FleetCountError {
    /// The number of fleets that was configured.
    pub count: usize,
}

/// A fleet index that a [`FleetSet`](crate::FleetSet) cannot hold.
#[derive(Debug, PartialEq, Eq, Error)]
#[error(
    "fleet index {index} is out of range, at most {} fleets are supported",
    MAX_FLEETS
)]
pub struct FleetIndexError {
    /// The index that was out of range.
    pub index: usize,
}
