//! Reaps scopes after the action entrypoint exits, even when it was killed.

use std::io;

use gang_spi::ActionLimitsPort;

use crate::ExecError;

/// Waits for the watched `gang exec` to exit, then removes its abandoned action scopes.
///
/// The `gang-engine` worker sends SIGTERM and, after `kill_grace`, attempts to kill the
/// action scope or process group, with a direct child PID fallback if the child changed groups.
/// The guardian lives in the worker's process group, which the worker does not signal, and
/// installs no termination handlers so it dies when the worker's group is torn down.
/// `exited` completes once the kernel has ended `creator` (`gang_os::ParentExit`), never
/// earlier, so every scope `creator` left behind is recognised as abandoned. Only scopes
/// belonging to `creator` are reaped.
///
/// # Errors
///
/// [`ExecError::Watch`] when the exit cannot be observed, and [`ExecError::Reap`] if abandoned
/// scopes cannot be removed.
pub async fn guard<L: ActionLimitsPort>(
    limits: &L,
    creator: u32,
    exited: impl Future<Output = io::Result<()>>,
) -> Result<(), ExecError> {
    exited.await.map_err(ExecError::Watch)?;
    limits
        .reap_abandoned(creator)
        .await
        .map_err(ExecError::Reap)
}
