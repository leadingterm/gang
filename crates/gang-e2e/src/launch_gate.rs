//! The harness subprocess that waits for a held worker launch and then execs its original argv.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

use rustix::event::{Nsecs, PollFd, PollFlags, Timespec, poll};
use rustix::fs::{Mode, OFlags, open};
use rustix::io::Errno;

/// Runs the launch-gate subprocess, then replaces it with the configured worker launcher.
#[must_use]
pub fn run_launch_gate(arguments: &[OsString]) -> ExitCode {
    match launch(arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("worker launch gate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn launch(arguments: &[OsString]) -> io::Result<()> {
    let [path, timeout, program, args @ ..] = arguments else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing launch gate arguments",
        ));
    };
    let millis = timeout
        .to_str()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid launch gate timeout")
        })?;
    wait(Path::new(path), Duration::from_millis(millis))?;
    Err(Command::new(program).args(args).exec())
}

fn wait(path: &Path, timeout: Duration) -> io::Result<()> {
    let fd = match open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    wait_opened(File::from(fd), timeout)
}

fn wait_opened(mut file: File, timeout: Duration) -> io::Result<()> {
    let started = Instant::now();
    loop {
        // A reader opened after the keeper closed gets EOF even if poll never reports HUP.
        match file.read(&mut [0_u8; 1]) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(io::Error::other("unexpected launch gate data")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "launch gate timed out",
            ));
        }
        // On macOS, XNU's poll(2) does not wake on FIFO writer EOF when no bytes were written;
        // cap each poll slice so the non-blocking read() probe above observes EOF promptly.
        #[cfg(target_os = "macos")]
        let poll_wait = remaining.min(Duration::from_millis(25));
        #[cfg(not(target_os = "macos"))]
        let poll_wait = remaining;
        let timespec = Timespec {
            tv_sec: i64::try_from(poll_wait.as_secs()).unwrap_or(i64::MAX),
            tv_nsec: Nsecs::from(poll_wait.subsec_nanos()),
        };
        let mut fds = [PollFd::new(&file, PollFlags::IN)];
        match poll(&mut fds, Some(&timespec)) {
            Ok(0) => {
                if started.elapsed() >= timeout {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "launch gate timed out",
                    ));
                }
            }
            Ok(_)
                if fds[0]
                    .revents()
                    .intersects(PollFlags::ERR | PollFlags::NVAL) =>
            {
                return Err(io::Error::other("launch gate poll failed"));
            }
            Ok(_) | Err(Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File, OpenOptions};
    use std::io;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use rustix::fs::{Mode, OFlags, open};

    use super::{wait, wait_opened};

    const TIMEOUT: Duration = Duration::from_secs(30);

    #[test]
    fn release_broadcasts_to_readers_even_if_the_gate_is_held_again() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("gate");
        gang_os::fs::create_private_fifo(&path).map_err(|error| error.source)?;
        let keeper = OpenOptions::new().read(true).write(true).open(&path)?;
        assert_eq!(
            wait(&path, Duration::ZERO).expect_err("held gate").kind(),
            io::ErrorKind::TimedOut
        );

        let ready = Arc::new(Barrier::new(4));
        let mut readers = Vec::new();
        for _ in 0..3 {
            let fd = open(
                &path,
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            let ready = Arc::clone(&ready);
            readers.push(thread::spawn(move || {
                ready.wait();
                wait_opened(File::from(fd), TIMEOUT)
            }));
        }
        fs::remove_file(&path)?;
        drop(keeper);
        gang_os::fs::create_private_fifo(&path).map_err(|error| error.source)?;
        let _next_keeper = OpenOptions::new().read(true).write(true).open(&path)?;
        ready.wait();
        for reader in readers {
            reader.join().expect("reader thread does not panic")?;
        }
        assert_eq!(
            wait(&path, Duration::ZERO)
                .expect_err("new gate remains held")
                .kind(),
            io::ErrorKind::TimedOut
        );
        Ok(())
    }

    #[test]
    fn missing_gate_does_not_delay_launch() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        wait(&directory.path().join("absent"), Duration::ZERO)
    }

    #[test]
    fn release_wakes_already_waiting_reader() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("gate");
        gang_os::fs::create_private_fifo(&path).map_err(|error| error.source)?;
        let keeper = OpenOptions::new().read(true).write(true).open(&path)?;
        let fd = open(
            &path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let reader = thread::spawn(move || wait_opened(File::from(fd), Duration::from_secs(5)));
        thread::sleep(Duration::from_millis(50));
        drop(keeper);
        reader.join().expect("reader thread does not panic")
    }
}
