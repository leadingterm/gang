//! The simulated state stream, listener certificates and held connection.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use gang_e2e::{
    CERTIFICATE_TTL, Channel, ChannelEvent, E2eError, Handshake, Leaf, RawStream, Reply,
    SessionCookie, VIEWER_RETRY_AFTER,
};
use gang_wire::{Closed, ClosedReason};
use parking_lot::Mutex;

use super::behaviour::{
    ADMIN_SESSION, Behaviour, Established, FirstChain, FirstViewer, LeafLifetime, Reissue,
    ReissuedChain, RetryLine, ShutdownStreams, SimState, ViewerCap, reissue_interval,
};

/// Creates a serialized SSE closed event using the selected retry-line convention.
pub(crate) fn closed_frame(reason: ClosedReason, behaviour: Behaviour) -> Vec<u8> {
    let json = serde_json::to_string(&Closed {
        reason,
        retry_delay: Some(VIEWER_RETRY_AFTER),
    })
    .unwrap_or_default();
    let retry = match behaviour.retry_line {
        RetryLine::Millis => VIEWER_RETRY_AFTER.as_millis(),
        RetryLine::Seconds => u128::from(VIEWER_RETRY_AFTER.as_secs()),
    };
    format!("retry: {retry}\nevent: closed\ndata: {json}\n\n").into_bytes()
}

/// An in-memory raw state stream for exercising stream scenarios.
pub(crate) struct SimStream {
    /// Shared simulated process state.
    state: Arc<Mutex<SimState>>,
    /// Stream behavior under test.
    behaviour: Behaviour,
    /// HTTP response status.
    status: u16,
    /// Pending body chunks.
    chunks: VecDeque<Vec<u8>>,
    /// Whether this stream occupies the sole viewer slot.
    viewer: bool,
    /// Whether the body has ended.
    closed: bool,
}

impl SimStream {
    /// Opens a simulated stream for a session cookie.
    pub(crate) fn open(
        state: &Arc<Mutex<SimState>>,
        behaviour: Behaviour,
        cookie: &SessionCookie,
    ) -> gang_e2e::Result<Self> {
        let mut state_guard = state.lock();
        if !state_guard.running {
            return Err(E2eError::system("the simulated server is not running"));
        }
        let mut stream = Self {
            state: Arc::clone(state),
            behaviour,
            status: 200,
            chunks: VecDeque::new(),
            viewer: false,
            closed: false,
        };
        if cookie.value != ADMIN_SESSION {
            stream.status = 401;
        } else if (state_guard.viewers == 0 && behaviour.first_viewer == FirstViewer::Refused)
            || (state_guard.viewers >= 1 && behaviour.viewer_cap == ViewerCap::Enforced)
        {
            stream
                .chunks
                .push_back(closed_frame(ClosedReason::ViewerCap, behaviour));
        } else {
            state_guard.viewers += 1;
            stream.viewer = true;
            stream.chunks.push_back(b"event: snap".to_vec());
            stream.chunks.push_back(b"shot\ndata: {}\n\n".to_vec());
        }
        Ok(stream)
    }
}

impl RawStream for SimStream {
    fn status(&self) -> u16 {
        self.status
    }

    fn next_chunk_within(&mut self, budget: Duration) -> gang_e2e::Result<Option<Vec<u8>>> {
        if let Some(chunk) = self.chunks.pop_front() {
            return Ok(Some(chunk));
        }
        if self.closed {
            return Ok(None);
        }
        let state = self.state.lock();
        if !state.running {
            drop(state);
            self.closed = true;
            return match self.behaviour.shutdown {
                ShutdownStreams::Closes => {
                    Ok(Some(closed_frame(ClosedReason::Shutdown, self.behaviour)))
                }
                ShutdownStreams::Drops => Ok(None),
            };
        }
        Err(E2eError::timeout(
            "the simulated stream to send a chunk",
            budget,
        ))
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        if self.viewer {
            let mut state = self.state.lock();
            state.viewers = state.viewers.saturating_sub(1);
        }
    }
}

/// Returns the simulated certificate generation at the state's current time.
pub(crate) fn generation(state: &SimState, behaviour: Behaviour) -> u32 {
    if behaviour.reissue == Reissue::Never {
        return 0;
    }
    let Ok(elapsed) = state.now.duration_since(state.started) else {
        return 0;
    };
    u32::try_from(elapsed.as_millis() / reissue_interval().as_millis().max(1)).unwrap_or(u32::MAX)
}

/// Returns the simulated leaf certificate handshake.
pub(crate) fn leaf(state: &SimState, behaviour: Behaviour) -> Handshake {
    let current_generation = generation(state, behaviour);
    if (current_generation == 0 && behaviour.first_chain == FirstChain::Foreign)
        || (current_generation > 0 && behaviour.reissued_chain == ReissuedChain::Foreign)
    {
        return Handshake::Refused("the chain does not verify against the pin".to_owned());
    }
    let not_before = state.started + reissue_interval() * current_generation;
    let lifetime = match behaviour.lifetime {
        LeafLifetime::Ttl => CERTIFICATE_TTL,
        LeafLifetime::Week => Duration::from_hours(7 * 24),
    };
    Handshake::Verified(Leaf {
        serial: vec![
            u8::try_from(current_generation).map_or(u8::MAX, |value| value.saturating_add(1)),
        ],
        not_before,
        not_after: not_before + lifetime,
    })
}

/// An in-memory connection held open across certificate reissue.
pub(crate) struct SimHeld {
    /// Shared simulated process state.
    state: Arc<Mutex<SimState>>,
    /// Connection behavior under test.
    behaviour: Behaviour,
    /// Generation when this connection was opened.
    generation: u32,
}

impl SimHeld {
    /// Opens a held connection at the state's current certificate generation.
    pub(crate) fn new(state: &Arc<Mutex<SimState>>, behaviour: Behaviour) -> Self {
        let generation = generation(&state.lock(), behaviour);
        Self {
            state: Arc::clone(state),
            behaviour,
            generation,
        }
    }
}

impl Channel for SimHeld {
    fn ping(&mut self) -> gang_e2e::Result<ChannelEvent> {
        let state = self.state.lock();
        if self.behaviour.established == Established::Dropped
            && generation(&state, self.behaviour) != self.generation
        {
            Ok(ChannelEvent::Closed)
        } else {
            Ok(ChannelEvent::PingAcknowledged)
        }
    }

    fn request(&mut self) -> gang_e2e::Result<Reply> {
        Ok(Reply::Served)
    }
}
