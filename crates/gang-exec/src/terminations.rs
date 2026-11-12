//! The termination signals `gang exec` keeps for the whole run.
//!
//! `NativeLink` sends `SIGTERM` or `SIGINT` to the process group of `gang exec` when an action
//! times out or is cancelled. These are real handlers, not `SIG_IGN`: the action's dispositions
//! reset at exec, so the action starts with the default ones, while `gang exec` survives
//! `NativeLink`'s `SIGTERM` and lives through the kill grace, relaying each request to the action.

use gang_spi::Termination;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{self, UnboundedReceiver};

/// Listens for termination signals until the run ends, sending each one on the returned channel.
///
/// Must be called inside a tokio runtime. The handlers are installed before this returns, so a
/// signal sent after it returns is never delivered with the default disposition. The listener
/// stops when the receiver is dropped.
///
/// # Errors
///
/// [`std::io::Error`] when a signal handler cannot be installed.
pub(crate) fn listen() -> std::io::Result<UnboundedReceiver<Termination>> {
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let termination = tokio::select! {
                Some(()) = terminate.recv() => Termination::Terminate,
                Some(()) = interrupt.recv() => Termination::Interrupt,
                else => break,
            };
            if sender.send(termination).is_err() {
                break;
            }
        }
    });
    Ok(receiver)
}
