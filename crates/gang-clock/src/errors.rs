/// The deadline passed before the future completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("deadline elapsed")]
pub struct Elapsed;

/// Errors produced when parsing an RFC 3339 timestamp.
#[derive(Debug, thiserror::Error)]
pub enum Rfc3339Error {
    /// The text is not a well-formed RFC 3339 timestamp.
    #[error("invalid RFC 3339 timestamp: {0}")]
    Malformed(#[from] time::error::Parse),
    /// The instant lies before the Unix epoch and cannot be represented.
    #[error("timestamp is before the Unix epoch")]
    BeforeEpoch,
}
