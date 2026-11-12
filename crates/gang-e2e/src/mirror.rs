//! The state a client holds after it applied the frames of the live state stream in order.

use gang_wire::{Frame, State};

/// What a client of the state stream holds after the frames read so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mirror {
    /// The state: the last snapshot with every later delta applied. `None` until a snapshot
    /// arrived or once the stream broke.
    pub state: Option<State>,
    /// Why the stream cannot be followed: a delta before the first snapshot, or a delta that was
    /// not diffed from the client's state.
    pub broken: Option<String>,
}

/// Applies `frames` in order, the way the dashboard does: a snapshot replaces the state, a delta
/// patches it, and ticks and closing frames leave it alone. Following stops at the first frame
/// that cannot be applied.
#[must_use]
pub fn follow(frames: &[Frame]) -> Mirror {
    let mut state: Option<State> = None;
    for frame in frames {
        match frame {
            Frame::Snapshot(snapshot) => state = Some(snapshot.clone()),
            Frame::Delta(delta) => {
                let Some(held) = state.as_mut() else {
                    return broken("a delta arrived before the snapshot".to_owned());
                };
                if let Err(error) = gang_state::apply(held, delta) {
                    return broken(gang_types::error_chain(&error).to_string());
                }
            }
            Frame::Tick(_) | Frame::Closed(_) => {}
        }
    }
    Mirror {
        state,
        broken: None,
    }
}

/// A mirror that cannot follow the stream, for `reason`.
fn broken(reason: String) -> Mirror {
    Mirror {
        state: None,
        broken: Some(reason),
    }
}
