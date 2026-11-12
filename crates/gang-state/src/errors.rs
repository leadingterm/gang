//! Errors of applying deltas and of encoding and decoding state frames.

/// A delta cannot be applied to the state it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    /// The delta was diffed from a different state sequence than the one being patched.
    #[error("delta base {base} does not match state sequence {state}")]
    BaseMismatch {
        /// The sequence number of the state being patched.
        state: u64,
        /// The sequence number the delta was diffed from.
        base: u64,
    },
}

/// A state or delta cannot be encoded as a frame.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    /// The payload could not be serialized as JSON.
    #[error("cannot encode frame: {0}")]
    Json(#[from] serde_json::Error),
}

/// A byte stream cannot be decoded into frames.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// A line of the stream is not valid UTF-8.
    #[error("stream line is not utf-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    /// An event of the stream is not a frame: its name is unknown or its payload does not parse.
    #[error("cannot decode frame: {0}")]
    Frame(#[from] gang_wire::FrameError),
}
