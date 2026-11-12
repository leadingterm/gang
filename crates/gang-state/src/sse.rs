//! The live state stream as server-sent events: [`encode`] for the frames of `GET /v1/state:watch`,
//! [`event`] for one event built from JSON the caller has already serialized, [`state_json`] for
//! the bare JSON body of `GET /v1/state`, and the [`Decoder`] that reads the stream back into
//! [`Frame`]s. Every encoded frame ends with a blank line, and every JSON payload sits on one
//! `data:` line because `serde_json` never writes a raw newline.

use std::mem;
use std::time::Duration;

use bytes::Bytes;
use gang_wire::{Frame, FrameRef, State};

use crate::{DecodeError, EncodeError};

/// The event type of a stream event that names none.
const DEFAULT_EVENT: &str = "message";

/// Initial capacity for one encoded frame.
const INITIAL_FRAME_CAPACITY: usize = 128;

/// Returns `frame` as SSE text. Snapshots, deltas and ticks carry their sequence number as the
/// `id:`. A `closed` frame with a retry delay leads with the SSE `retry:` reconnect delay in
/// milliseconds.
///
/// # Errors
///
/// Returns [`EncodeError`] when the payload cannot be serialized as JSON.
pub fn encode<'a>(frame: impl Into<FrameRef<'a>>) -> Result<Bytes, EncodeError> {
    let frame = frame.into();
    let mut buf = Vec::with_capacity(INITIAL_FRAME_CAPACITY);
    header(&mut buf, frame.event_name(), frame.id(), frame.retry());
    frame.write_data(&mut buf)?;
    buf.extend_from_slice(b"\n\n");
    Ok(Bytes::from(buf))
}

/// Returns `state` as the bare JSON body of `GET /v1/state`.
///
/// # Errors
///
/// Returns [`EncodeError`] when the state cannot be serialized as JSON.
pub fn state_json(state: &State) -> Result<Bytes, EncodeError> {
    Ok(Bytes::from(serde_json::to_string(state)?))
}

/// Builds one SSE event from `json`, the payload of the frame named `name`, serialized as one line
/// by `serde_json`. For a frame whose payload is `json`, the bytes equal those of [`encode`].
#[must_use]
pub fn event(name: &str, id: Option<u64>, retry: Option<Duration>, json: &[u8]) -> Bytes {
    // The fixed header text and the longest decimal numbers take fewer than 90 bytes.
    let mut buf = Vec::with_capacity(name.len() + json.len() + 96);
    header(&mut buf, name, id, retry);
    buf.extend_from_slice(json);
    buf.extend_from_slice(b"\n\n");
    Bytes::from(buf)
}

/// Writes the header lines of an event: `retry:` first when `retry` is set, then `event:`, then
/// `id:` when `id` is set, then the `data: ` prefix that the payload follows.
fn header(buf: &mut Vec<u8>, name: &str, id: Option<u64>, retry: Option<Duration>) {
    if let Some(delay) = retry {
        buf.extend_from_slice(b"retry: ");
        buf.extend_from_slice(itoa::Buffer::new().format(delay.as_millis()).as_bytes());
        buf.push(b'\n');
    }
    buf.extend_from_slice(b"event: ");
    buf.extend_from_slice(name.as_bytes());
    buf.push(b'\n');
    if let Some(id) = id {
        buf.extend_from_slice(b"id: ");
        buf.extend_from_slice(itoa::Buffer::new().format(id).as_bytes());
        buf.push(b'\n');
    }
    buf.extend_from_slice(b"data: ");
}

/// Reads an SSE byte stream back into [`Frame`]s.
///
/// Bytes may arrive in chunks split anywhere. Lines end in `\n` or `\r\n`; comment lines are
/// skipped. An event is dispatched at its blank line, with its `data:` lines joined by `\n`.
/// The `id:` and `retry:` lines are read and dropped, because the payload of a frame already
/// carries its sequence number and its retry delay. An event without `data:` is dropped, and
/// one that is cut off at the end of the stream never completes.
#[derive(Debug, Default)]
pub struct Decoder {
    /// The bytes of the line that has not ended yet.
    line: Vec<u8>,
    /// The `event:` of the event being read.
    event: Option<String>,
    /// The `data:` lines of the event being read, joined by `\n`.
    data: Option<String>,
}

impl Decoder {
    /// Creates a decoder at the start of a stream.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads the next chunk of the stream and returns the frames it completes, in order.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] when a line is not UTF-8 or an event is not a frame. The stream is
    /// malformed then and the decoder must be dropped; the frames completed before the error in
    /// the same chunk are not returned.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Frame>, DecodeError> {
        let mut frames = Vec::new();
        let mut rest = chunk;
        while let Some(end) = rest.iter().position(|&byte| byte == b'\n') {
            let (head, tail) = rest.split_at(end);
            rest = tail.get(1..).unwrap_or_default();
            let frame = if self.line.is_empty() {
                self.read_line(head)
            } else {
                let mut line = mem::take(&mut self.line);
                line.extend_from_slice(head);
                let frame = self.read_line(&line);
                line.clear();
                self.line = line;
                frame
            };
            frames.extend(frame?);
        }
        self.line.extend_from_slice(rest);
        Ok(frames)
    }

    /// Reads one complete line, without its `\n`. Returns the frame that the line completes.
    fn read_line(&mut self, line: &[u8]) -> Result<Option<Frame>, DecodeError> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let line = std::str::from_utf8(line)?;
        if line.is_empty() {
            return self.dispatch();
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => match &mut self.data {
                Some(data) => {
                    data.push('\n');
                    data.push_str(value);
                }
                None => self.data = Some(value.to_owned()),
            },
            // Comment lines have an empty field; `id`, `retry` and unknown fields carry nothing
            // that the payload does not.
            _ => {}
        }
        Ok(None)
    }

    /// Ends the event being read, and returns its frame when it has data.
    fn dispatch(&mut self) -> Result<Option<Frame>, DecodeError> {
        let event = self.event.take();
        let Some(data) = self.data.take() else {
            return Ok(None);
        };
        let event = event.as_deref().unwrap_or(DEFAULT_EVENT);
        Ok(Some(Frame::from_parts(event, &data)?))
    }
}
