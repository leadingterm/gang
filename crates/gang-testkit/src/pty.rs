//! Bounded pseudo-terminal driver for interactive command tests.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

/// A command's exit status and combined terminal output.
#[derive(Debug)]
pub struct PtyOutput {
    /// Child exit status.
    pub status: ExitStatus,
    /// Terminal output, decoded lossily.
    pub transcript: String,
}

/// Runs a command on a PTY, answering the current line whenever `answer` returns text.
///
/// Answers must include their terminating newline. The terminal is the command's alone: it never
/// becomes the caller's controlling terminal. Each line is answered at most once. The timeout
/// covers both child exit and terminal output; on failure the child's process group is killed and
/// the child is reaped.
///
/// # Errors
///
/// Returns an I/O error if the terminal, child, reader, or timeout fails.
pub fn run_answering(
    mut command: Command,
    answer: impl Fn(&str) -> Option<&'static str> + Send + 'static,
    timeout: Duration,
) -> io::Result<PtyOutput> {
    let master = open_master()?;
    let slave = open_slave(&master)?;
    command
        .process_group(0)
        .stdin(Stdio::from(slave.try_clone()?))
        .stdout(Stdio::from(slave.try_clone()?))
        .stderr(Stdio::from(slave));
    let mut reader = File::from(master);
    let mut writer = reader.try_clone()?;
    let mut child = command.spawn()?;
    drop(command);
    let started = Instant::now();
    let result = (|| -> io::Result<PtyOutput> {
        let pid = i32::try_from(child.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| io::Error::other("invalid child process id"))?;
        let (exit_tx, exit_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("pty-child-exit".into())
            .spawn(move || {
                let result = loop {
                    match waitid(
                        WaitId::Pid(pid),
                        WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
                    ) {
                        Err(Errno::INTR) => {}
                        result => break result.map(|_| ()).map_err(io::Error::from),
                    }
                };
                let _ = exit_tx.send(result);
            })?;
        let (output_tx, output_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("pty-reader".into())
            .spawn(move || {
                let result = (|| -> io::Result<String> {
                    let mut bytes = Vec::new();
                    let mut byte = [0];
                    let mut line_start = 0;
                    let mut answered_line = None;
                    loop {
                        match reader.read(&mut byte) {
                            Ok(0) => break,
                            Ok(_) => bytes.push(byte[0]),
                            Err(error)
                                if error.raw_os_error() == Some(Errno::IO.raw_os_error()) =>
                            {
                                break;
                            }
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            Err(error) => return Err(error),
                        }
                        if byte[0] == b'\n' {
                            line_start = bytes.len();
                        }
                        if answered_line != Some(line_start)
                            && let Some(response) =
                                answer(&String::from_utf8_lossy(&bytes[line_start..]))
                        {
                            writer.write_all(response.as_bytes())?;
                            answered_line = Some(line_start);
                        }
                    }
                    Ok(String::from_utf8_lossy(&bytes).into_owned())
                })();
                let _ = output_tx.send(result);
            })?;
        let receive_error = |error| match error {
            mpsc::RecvTimeoutError::Timeout => {
                io::Error::new(io::ErrorKind::TimedOut, "PTY command timed out")
            }
            mpsc::RecvTimeoutError::Disconnected => io::Error::other("PTY worker stopped"),
        };
        exit_rx
            .recv_timeout(timeout.saturating_sub(started.elapsed()))
            .map_err(receive_error)??;
        let transcript = output_rx
            .recv_timeout(timeout.saturating_sub(started.elapsed()))
            .map_err(receive_error)??;
        Ok(PtyOutput {
            status: child.wait()?,
            transcript,
        })
    })();
    if result.is_err() {
        if let Some(pid) = i32::try_from(child.id()).ok().and_then(Pid::from_raw) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        } else {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
    result
}

/// Opens the terminal's master side, close-on-exec on every platform.
fn open_master() -> io::Result<OwnedFd> {
    #[cfg(not(target_vendor = "apple"))]
    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC)?;
    // Apple's `posix_openpt` has no close-on-exec flag, so the descriptor flag is set right after.
    #[cfg(target_vendor = "apple")]
    let master = {
        let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
        rustix::io::fcntl_setfd(&master, rustix::io::FdFlags::CLOEXEC)?;
        master
    };
    Ok(master)
}

/// Opens the terminal's slave side for the command. It never becomes the caller's controlling
/// terminal: a session leader without one (a systemd service) would acquire it, making the command
/// a background job that stops at its first read.
fn open_slave(master: &OwnedFd) -> io::Result<File> {
    grantpt(master)?;
    unlockpt(master)?;
    let name = ptsname(master, Vec::new())?;
    let slave = rustix::fs::open(
        name.as_c_str(),
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(File::from(slave))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, Write};
    use std::process::Command;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::run_answering;
    use crate::ChildTest;

    const PROMPTING_CHILD: ChildTest = ChildTest::new("pty::tests::prompting_child");
    const SESSION_LEADER_DRIVER: ChildTest = ChildTest::new("pty::tests::session_leader_driver");

    #[test]
    fn answers_prompt_and_collects_terminal_output() -> io::Result<()> {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf 'choice: '; read answer; printf 'result=%s\\n' \"$answer\"",
        ]);
        let output = run_answering(
            command,
            |line| line.ends_with("choice: ").then_some("yes\n"),
            Duration::from_secs(10),
        )?;
        assert_eq!(output.status.code(), Some(0));
        assert!(output.transcript.contains("result=yes"));
        Ok(())
    }

    #[test]
    fn times_out_when_child_waits_for_input() {
        let mut command = Command::new("sh");
        command.args(["-c", "read answer"]);
        let result = run_answering(command, |_| None, Duration::from_millis(100));
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(io::ErrorKind::TimedOut)
        );
    }

    /// A prompt remains eligible only until its first answer is written.
    #[test]
    fn each_line_is_answered_once() -> io::Result<()> {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf 'Q? '; read a; echo \"got:$a\""]);
        let output = run_answering(
            command,
            |line| line.contains("Q?").then_some("y\n"),
            Duration::from_secs(10),
        )?;
        assert!(output.status.success(), "the command must succeed");
        assert_eq!(
            output.transcript, "Q? y\r\ngot:y\r\n",
            "a prompt line must be answered once"
        );
        Ok(())
    }

    /// A timed-out command cannot leave its descendants holding the terminal.
    #[test]
    fn timeout_kills_the_process_group() -> Result<(), Box<dyn std::error::Error>> {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let dir = std::env::temp_dir().join(format!("gang-pty-{}-{unique}", std::process::id()));
        fs::create_dir(&dir)?;
        let pid_file = dir.join("pid");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 300 & echo $! > \"$1\"; wait", "sh"])
            .arg(&pid_file);
        let result = run_answering(command, |_| None, Duration::from_secs(1));
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(io::ErrorKind::TimedOut),
            "a waiting command must time out"
        );
        let raw_pid: i32 = fs::read_to_string(&pid_file)?.trim().parse()?;
        fs::remove_dir_all(&dir)?;
        let pid = rustix::process::Pid::from_raw(raw_pid)
            .ok_or_else(|| io::Error::other("invalid grandchild process id"))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let gone = loop {
            #[cfg(target_os = "linux")]
            let gone = process_is_gone(fs::read_to_string(format!("/proc/{raw_pid}/stat")))?;
            #[cfg(not(target_os = "linux"))]
            let gone = rustix::process::test_kill_process(pid).is_err();
            #[cfg(target_os = "linux")]
            let _ = pid;
            if gone || Instant::now() >= deadline {
                break gone;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(gone, "a grandchild survived the timeout");
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn process_is_gone(stat: io::Result<String>) -> io::Result<bool> {
        match stat {
            Ok(stat) => Ok(stat
                .rsplit_once(')')
                .and_then(|(_, fields)| fields.split_whitespace().next())
                == Some("Z")),
            // Linux returns ESRCH if the process is reaped between opening and reading stat.
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) =>
            {
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    /// A process can be reaped after its proc stat file is opened but before it is read.
    #[cfg(target_os = "linux")]
    #[test]
    fn reaped_process_is_gone_when_proc_stat_read_returns_esrch() -> io::Result<()> {
        let mut child = PROMPTING_CHILD
            .command()?
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn()?;
        let stat = fs::File::open(format!("/proc/{}/stat", child.id()))?;
        child.kill()?;
        child.wait()?;
        let stat = io::read_to_string(stat);
        assert_eq!(
            stat.as_ref().err().and_then(io::Error::raw_os_error),
            Some(rustix::io::Errno::SRCH.raw_os_error()),
            "reading proc stat after reaping must exercise the ESRCH race"
        );
        assert!(process_is_gone(stat)?, "a reaped process is gone");
        Ok(())
    }

    /// The terminal program of `drives_a_terminal_from_a_session_leader`: asks for one line and echoes it.
    #[test]
    #[ignore = "child test: run on a PTY by session_leader_driver"]
    fn prompting_child() {
        print!("choice: ");
        io::stdout().flush().expect("the prompt is flushed");
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .expect("the answer is read from the terminal");
        println!("result={}", answer.trim_end());
        PROMPTING_CHILD.complete();
    }

    /// Drives `prompting_child` from a new session that has no controlling terminal, as a systemd
    /// service does.
    #[test]
    #[ignore = "child test: run by drives_a_terminal_from_a_session_leader"]
    fn session_leader_driver() -> io::Result<()> {
        rustix::process::setsid()?;
        let output = run_answering(
            PROMPTING_CHILD.command()?,
            |line| line.ends_with("choice: ").then_some("yes\n"),
            Duration::from_secs(10),
        )?;
        assert_eq!(output.status.code(), Some(0), "{}", output.transcript);
        assert!(
            output.transcript.contains("result=yes")
                && PROMPTING_CHILD.completed_in(&output.transcript),
            "{}",
            output.transcript
        );
        SESSION_LEADER_DRIVER.complete();
        Ok(())
    }

    /// A driver that leads a session without a controlling terminal must not take the PTY as its
    /// own terminal: the command would be a background job of it and stop at its first read.
    #[test]
    fn drives_a_terminal_from_a_session_leader() -> io::Result<()> {
        let output = SESSION_LEADER_DRIVER.command()?.output()?;
        SESSION_LEADER_DRIVER.assert_completed(&output);
        Ok(())
    }
}
