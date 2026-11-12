use std::fmt;
use std::io;

use tokio::signal::unix::{SignalKind, signal};

use crate::sys::{signal_set, thread_mask};

/// SIGTERM and SIGINT blocked on the calling thread until dropped; the previous mask is then restored.
#[must_use = "the signals are unblocked again when the guard drops"]
pub struct TerminationsBlocked {
    previous: libc::sigset_t,
    // The previous mask belongs to the thread that created this guard.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl fmt::Debug for TerminationsBlocked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TerminationsBlocked")
            .finish_non_exhaustive()
    }
}

/// Blocks SIGTERM and SIGINT on the calling thread, so a child spawned while the guard lives inherits them blocked.
///
/// # Errors
///
/// Returns the error from `pthread_sigmask(3)` if the mask cannot be changed.
pub fn block_terminations() -> io::Result<TerminationsBlocked> {
    let blocked = signal_set(&[libc::SIGTERM, libc::SIGINT]);
    let previous = thread_mask(libc::SIG_BLOCK, Some(&blocked))?;
    Ok(TerminationsBlocked {
        previous,
        _not_send: std::marker::PhantomData,
    })
}

impl Drop for TerminationsBlocked {
    fn drop(&mut self) {
        let _ = thread_mask(libc::SIG_SETMASK, Some(&self.previous));
    }
}

/// Empties the calling thread's signal mask, so the program it execs next starts with no blocked signals.
///
/// # Errors
///
/// Returns the error from `pthread_sigmask(3)` if the mask cannot be changed.
pub fn clear_mask() -> io::Result<()> {
    thread_mask(libc::SIG_SETMASK, Some(&signal_set(&[]))).map(|_| ())
}

/// Installs SIGINT and SIGTERM handlers now and returns a future that completes on the first of them.
///
/// Handlers are installed at call time, not on first poll, so a signal arriving between the call and
/// the first poll is not lost. Must be called inside a tokio runtime. The handlers stay installed for
/// the process.
///
/// # Errors
///
/// Returns an error when either signal handler cannot be installed.
pub fn shutdown_signal() -> std::io::Result<impl Future<Output = ()> + Send + 'static> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::process::Stdio;

    use gang_testkit::ChildTest;
    use rustix::process::{Signal, getpid, kill_process};

    use super::{block_terminations, clear_mask, shutdown_signal, signal_set, thread_mask};
    use crate::sys::contains_signal as contains;

    const PRINT_CHILD_TERMINATION_MASK: ChildTest =
        ChildTest::new("signal::tests::print_child_termination_mask");
    const INTERRUPT_THEN_TERMINATE_CHILD: ChildTest =
        ChildTest::new("signal::tests::interrupt_then_terminate_child");

    #[test]
    fn termination_guard_restores_the_previous_thread_mask() -> io::Result<()> {
        std::thread::spawn(|| -> io::Result<()> {
            clear_mask()?;
            let prior = signal_set(&[libc::SIGUSR1]);
            thread_mask(libc::SIG_BLOCK, Some(&prior))?;
            let guard = block_terminations()?;
            let blocked = thread_mask(libc::SIG_BLOCK, None)?;
            assert!(contains(&blocked, libc::SIGTERM));
            assert!(contains(&blocked, libc::SIGINT));
            drop(guard);
            let restored = thread_mask(libc::SIG_BLOCK, None)?;
            assert!(!contains(&restored, libc::SIGTERM));
            assert!(!contains(&restored, libc::SIGINT));
            assert!(contains(&restored, libc::SIGUSR1));
            Ok(())
        })
        .join()
        .expect("signal mask test thread must not panic")
    }

    #[test]
    fn clearing_mask_removes_all_blocked_signals() -> io::Result<()> {
        std::thread::spawn(|| -> io::Result<()> {
            let blocked = signal_set(&[libc::SIGUSR1]);
            thread_mask(libc::SIG_BLOCK, Some(&blocked))?;
            assert!(contains(
                &thread_mask(libc::SIG_BLOCK, None)?,
                libc::SIGUSR1
            ));
            clear_mask()?;
            let mask = thread_mask(libc::SIG_BLOCK, None)?;
            for signal in 1..=128 {
                assert!(!contains(&mask, signal), "signal {signal} remained blocked");
            }
            Ok(())
        })
        .join()
        .expect("signal mask test thread must not panic")
    }

    #[test]
    fn children_inherit_termination_mask_only_while_guard_lives() -> io::Result<()> {
        std::thread::spawn(|| -> io::Result<()> {
            clear_mask()?;
            let child_mask = || -> io::Result<String> {
                let mut command = PRINT_CHILD_TERMINATION_MASK.command()?;
                command.stdout(Stdio::piped());
                let child = {
                    let _spawn = gang_testkit::fork_gate::spawner();
                    command.spawn()?
                };
                let output = child.wait_with_output()?;
                PRINT_CHILD_TERMINATION_MASK.assert_completed(&output);
                Ok(String::from_utf8_lossy(&output.stdout).into_owned())
            };
            let guard = block_terminations()?;
            let blocked = child_mask()?;
            assert!(blocked.contains("child-sigterm-blocked=true"), "{blocked}");
            drop(guard);
            let unblocked = child_mask()?;
            assert!(
                unblocked.contains("child-sigterm-blocked=false"),
                "{unblocked}"
            );
            Ok(())
        })
        .join()
        .expect("signal mask test thread must not panic")
    }

    #[test]
    #[ignore = "child process helper of the signal mask test"]
    fn print_child_termination_mask() -> io::Result<()> {
        let mask = thread_mask(libc::SIG_BLOCK, None)?;
        println!("child-sigterm-blocked={}", contains(&mask, libc::SIGTERM));
        PRINT_CHILD_TERMINATION_MASK.complete();
        Ok(())
    }

    /// Runs the signal test in a child process so the raised signals reach only that child.
    #[test]
    fn completes_on_interrupt_then_terminate() -> Result<(), Box<dyn std::error::Error>> {
        let mut command = INTERRUPT_THEN_TERMINATE_CHILD.command()?;
        command
            .env("GANG_OS_SIGNAL_CHILD", "1")
            .stdout(Stdio::piped());
        let child = {
            let _spawn = gang_testkit::fork_gate::spawner();
            command.spawn()?
        };
        let output = child.wait_with_output()?;
        INTERRUPT_THEN_TERMINATE_CHILD.assert_completed(&output);
        Ok(())
    }

    /// Child-process body of `completes_on_interrupt_then_terminate`; a no-op unless re-executed by it.
    #[ignore = "child process helper of the signal test"]
    #[tokio::test]
    async fn interrupt_then_terminate_child() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("GANG_OS_SIGNAL_CHILD").is_none() {
            return Ok(());
        }
        let interrupted = shutdown_signal()?;
        kill_process(getpid(), Signal::INT)?;
        interrupted.await;

        let terminated = shutdown_signal()?;
        kill_process(getpid(), Signal::TERM)?;
        terminated.await;
        INTERRUPT_THEN_TERMINATE_CHILD.complete();
        Ok(())
    }
}
