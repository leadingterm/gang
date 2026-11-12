//! Typed errors for parsing wire values: durations, names, page parameters and stream frames.

/// Why a wire duration string could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireDurationError {
    /// The text is not seconds with an optional fraction and an `s` suffix.
    #[error(
        "duration must be seconds with optional fraction and an `s` suffix, such as `600s` or `0.250s`"
    )]
    Malformed,
    /// The duration is too large to represent.
    #[error("duration is too large")]
    OutOfRange,
}

/// Why a wire resource name could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceNameError {
    /// The name belongs to a different collection than the one requested.
    #[error("name is not in the {collection} collection")]
    WrongCollection {
        /// Collection the caller expected, such as `workers`.
        collection: &'static str,
    },
    /// The resource id is not a single non-empty percent-encoded path segment.
    #[error("resource id must be one non-empty percent-encoded path segment")]
    MalformedId,
    /// The percent-decoded resource id is not valid UTF-8.
    #[error("resource id is not valid utf-8")]
    NotUtf8,
    /// The resource id is well-formed but rejected by its collection's rules.
    #[error("{collection} id is invalid")]
    InvalidId {
        /// Collection whose id rules rejected the value, such as `workers`.
        collection: &'static str,
    },
}

/// Why pagination parameters were rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    /// The requested page size is negative.
    #[error("page size must not be negative")]
    NegativePageSize,
    /// The page token is not one this server issued or cannot be decoded.
    #[error("page token is invalid")]
    InvalidToken,
}

/// Why a server-sent event could not be decoded into a stream frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The event name is not one of the frame names.
    #[error("unknown frame event `{name}`")]
    UnknownEvent {
        /// The event name that was received.
        name: String,
    },
    /// The event's data is not the JSON of its frame.
    #[error("{event} frame data is invalid: {reason}")]
    Data {
        /// The frame's event name.
        event: &'static str,
        /// What is wrong with the data.
        reason: String,
    },
}
