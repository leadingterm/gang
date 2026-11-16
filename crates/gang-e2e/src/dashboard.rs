//! The dashboard scenario of a deployment whose clients authenticate with bearer tokens: the live
//! state refuses a request that carries no session cookie, a session cookie minted for the
//! administrator opens the state stream, and revoking that principal ends the open stream with
//! a `closed` event.

use std::time::{Duration, Instant};

use gang_wire::Frame;
use gang_wire::routes::{GET_STATE, WATCH_STATE};

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::identity::Identity;
use crate::names;
use crate::sse::{EventStream, STREAM_OK};
use crate::stack::Stack;
use crate::system::{Probe, System};
use crate::verdict;

/// Observes real Bazel request metadata on a held running action.
pub(crate) fn running_metadata(stack: &Stack<'_>, timeout: Duration) -> Result<Check> {
    stack.start_server()?;
    std::thread::scope(|scope| {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let build = scope.spawn(|| stack.build_until_stopped(&[Probe::Hold], stopped));
        let state = stack.wait_live_state(timeout, |state| {
            state
                .actions
                .as_ref()
                .is_some_and(|actions| !actions.running.is_empty())
        });
        let _ = stop.send("running action observed".to_owned());
        let joined = build
            .join()
            .map_err(|_| E2eError::system("the build thread panicked"));
        let killed = stack.kill_bazel();
        let _ = joined?;
        killed?;
        match state {
            Ok(state) => Ok(metadata_check(&state)),
            Err(error @ E2eError::Timeout { .. }) => Ok(Check::failed(
                "running action has Bazel target and mnemonic",
                gang_types::error_chain(&error).to_string(),
            )),
            Err(error) => Err(error),
        }
    })
}

fn metadata_check(state: &gang_wire::State) -> Check {
    let action = state
        .actions
        .as_ref()
        .and_then(|actions| actions.running.first());
    Check::expect(
        "running action has Bazel target and mnemonic",
        action.is_some_and(|row| !row.target.trim().is_empty() && !row.mnemonic.trim().is_empty()),
        || format!("running action: {action:?}"),
    )
}

/// Checks that requests without a session cookie are refused, opens the state stream with the
/// session cookie of the administrator, revokes the administrator, and judges whether the open
/// stream ends with `closed` and the reason `revoked` within `timeout`.
///
/// # Errors
///
/// When the administrator cannot be logged in, a request cannot be made, or the stream cannot be
/// read or decoded.
pub fn dashboard<I: Identity>(identity: &I, timeout: Duration) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    for route in [GET_STATE, WATCH_STATE] {
        let path = route.template;
        let status = identity.status(path, None)?;
        checks.push(verdict::cookieless_refused(path, status));
    }

    let administrator = identity.administrator()?;
    let cookie = identity.session_cookie(&administrator)?;
    let mut stream = identity.open_state_stream(&cookie)?;
    checks.push(verdict::stream_opened(
        stream.status(),
        stream.content_type(),
    ));
    if stream.status() != STREAM_OK {
        return Ok(checks);
    }
    let first = stream.next_event()?;
    checks.push(verdict::stream_starts_with_snapshot(first.as_ref()));

    identity.revoke(&administrator)?;
    let check = match read_until_closed(&mut stream, timeout) {
        Ok((frames, ended)) => verdict::closed_on_revocation(&frames, ended),
        Err(error @ E2eError::Timeout { .. }) => Check::failed(
            names::dashboard::CLOSED_ON_REVOCATION,
            gang_types::error_chain(&error).to_string(),
        ),
        Err(error) => return Err(error),
    };
    checks.push(check);

    Ok(checks)
}

/// Reads frames from `stream` until a `closed` frame or the end of the stream, within one
/// deadline `timeout` after the call. Returns the frames read and whether the stream ended.
///
/// # Errors
///
/// [`E2eError::Timeout`] when neither comes before the deadline; otherwise when the stream
/// cannot be read or decoded.
fn read_until_closed<S: EventStream>(
    stream: &mut S,
    timeout: Duration,
) -> Result<(Vec<Frame>, bool)> {
    let deadline = Instant::now() + timeout;
    let mut frames = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(E2eError::timeout(
                "the state stream to close after the revocation",
                timeout,
            ));
        }
        let Some(frame) = stream.next_event_within(remaining)? else {
            return Ok((frames, true));
        };
        let closed = matches!(frame, Frame::Closed(_));
        frames.push(frame);
        if closed {
            return Ok((frames, false));
        }
    }
}

#[cfg(test)]
mod tests {
    use gang_wire::{Actions, RunningAction, State};

    use super::metadata_check;

    #[test]
    fn running_metadata_requires_both_target_and_mnemonic() {
        let mut state = State {
            actions: Some(Actions {
                running: vec![RunningAction {
                    target: "//sample:hold".to_owned(),
                    mnemonic: "Genrule".to_owned(),
                    ..RunningAction::default()
                }],
                ..Actions::default()
            }),
            ..State::default()
        };
        assert!(metadata_check(&state).is_passed());
        let row = &mut state.actions.as_mut().expect("sample exists").running[0];
        row.target.clear();
        assert!(metadata_check(&state).is_failure());
        let row = &mut state.actions.as_mut().expect("sample exists").running[0];
        row.target = "//sample:hold".to_owned();
        row.mnemonic.clear();
        assert!(metadata_check(&state).is_failure());
    }
}
