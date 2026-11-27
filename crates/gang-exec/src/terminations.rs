//! The termination signals `gang exec` keeps for the whole run.
//!
//! The worker sends `SIGTERM` or `SIGINT` to the process group of `gang exec` when an action
//! times out or is cancelled. These are real handlers, not `SIG_IGN`: the action's dispositions
//! reset at exec, so the action starts with the default ones, while `gang exec` survives
//! the worker's `SIGTERM` and lives through the kill grace, relaying each request to the action.

use std::io;
use std::pin::Pin;
use std::task::Poll;

use futures::{Stream, stream};
use gang_spi::Termination;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::backend::SignalDelivery;
use signal_hook::iterator::exfiltrator::SignalOnly;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::UnixStream;

/// Listens for termination signals until the returned stream is dropped.
///
/// Must be called inside a tokio runtime. The handlers are installed before this returns, so a
/// signal sent after it returns is never delivered with the default disposition. Dropping the
/// stream unregisters its handlers. Repeated signals of the same type may coalesce, and the
/// order of different signal types is unspecified.
///
/// # Errors
///
/// Returns an I/O error if the signal pipe or a signal handler cannot be created. The stream
/// reports I/O errors encountered while reading the pipe.
pub(crate) fn listen() -> io::Result<impl Stream<Item = io::Result<Termination>> + Unpin> {
    let (read, write) = UnixStream::pair()?;
    let mut delivery =
        SignalDelivery::with_pipe(read, write.into_std()?, SignalOnly, [SIGTERM, SIGINT])?;
    Ok(stream::poll_fn(move |cx| {
        loop {
            if let Some(signal) = delivery.pending().next() {
                // Only SIGTERM and SIGINT are registered above.
                let termination = if signal == SIGTERM {
                    Termination::Terminate
                } else {
                    Termination::Interrupt
                };
                return Poll::Ready(Some(Ok(termination)));
            }
            let mut byte = [0; 1];
            let mut buf = ReadBuf::new(&mut byte);
            match Pin::new(delivery.get_read_mut()).poll_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok(())) if buf.filled().is_empty() => {
                    // The delivery owns the writer, so EOF cannot be a normal shutdown.
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "termination signal pipe closed",
                    ))));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::ffi::OsString;
    use std::process::Stdio;
    use std::time::Duration;

    use gang_config::InputLinks;
    use gang_spi::{
        ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionLimits, NetworkAccess,
        Termination,
    };
    use gang_testkit::{CallLog, ChildTest, RecordingLimits, ScriptedSandbox};

    use super::listen;
    use crate::{Action, execute};

    const SIGNAL_BEFORE_EXECUTE: ChildTest =
        ChildTest::new("terminations::tests::signal_before_execute_child");

    #[tokio::test]
    async fn pending_os_signal_prevents_action_start() -> Result<(), Box<dyn Error>> {
        let child = {
            let _spawn = gang_testkit::fork_gate::spawner();
            tokio::process::Command::from(SIGNAL_BEFORE_EXECUTE.command()?)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?
        };
        let output =
            tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await??;
        SIGNAL_BEFORE_EXECUTE.assert_completed(&output);
        Ok(())
    }

    #[test]
    #[ignore = "child test: run by pending_os_signal_prevents_action_start"]
    fn signal_before_execute_child() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            for (signal, expected) in [
                (signal_hook::consts::SIGTERM, Termination::Terminate),
                (signal_hook::consts::SIGINT, Termination::Interrupt),
            ] {
                for with_limits in [false, true] {
                    let directory = tempfile::tempdir()?;
                    let input = directory.path().join("input");
                    std::fs::write(&input, b"input")?;
                    std::fs::hard_link(&input, directory.path().join("linked-input"))?;
                    let log = CallLog::default();
                    let sandbox =
                        ScriptedSandbox::try_exiting(&log, ActionExecutionOutcome::Exited(0))?;
                    let limits = RecordingLimits::new(&log);
                    let argv = [OsString::from("cmd")];
                    let action = Action {
                        spec: ActionExecutionSpec {
                            command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
                            environment: &[],
                            action_directory: directory.path(),
                            working_directory: directory.path(),
                            read_only_trees: &[],
                            network: NetworkAccess::Offline,
                            memory_cgroup: None,
                            launcher: &[],
                        },
                        limits: ActionLimits {
                            cpu_count: None,
                            memory: None,
                            cpu_weight: None,
                        },
                    };
                    let mut terminations = listen()?;
                    signal_hook::low_level::raise(signal)?;
                    let outcome = execute::<_, RecordingLimits>(
                        &sandbox,
                        with_limits.then_some(&limits),
                        InputLinks::Break,
                        &action,
                        &mut terminations,
                        |_| {},
                        || Ok(()),
                    )
                    .await?;
                    assert_eq!(
                        outcome.outcome,
                        ActionExecutionOutcome::Signaled(expected.signal())
                    );
                    if with_limits {
                        assert_eq!(log.calls(), ["limits.enter", "limits.leave"]);
                    } else {
                        assert!(log.calls().is_empty());
                    }
                }
            }
            Ok::<(), Box<dyn Error>>(())
        })?;
        SIGNAL_BEFORE_EXECUTE.complete();
        Ok(())
    }
}
