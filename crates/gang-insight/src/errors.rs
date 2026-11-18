//! Errors from forwarding and ingesting build observations.

/// An invalid forwarded observation request.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// A principal is not a valid identity.
    #[error("invalid observation identity")]
    Identity(#[from] gang_types::IdentityError),
    /// An origin field is not a valid name.
    #[error("invalid observation name")]
    Name(#[from] gang_types::NameError),
    /// An observation references an absent origin.
    #[error("observation origin index {index} is out of range")]
    Origin {
        /// The invalid origin index.
        index: u32,
    },
    /// The request contains more losses than the configured limit.
    #[error("too many observation losses")]
    TooManyLosses,
    /// A loss has a zero count.
    #[error("observation loss count is zero")]
    ZeroLoss,
    /// A wire enum value is not recognized.
    #[error("unknown observation value")]
    UnknownValue,
}

/// An observation outbox cannot be constructed with the supplied limits.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// The request cap cannot hold the minimum observation request.
    #[error("observation request cap is smaller than {needed} bytes")]
    CapTooSmall {
        /// The minimum supported request cap, in bytes.
        needed: usize,
    },
}
