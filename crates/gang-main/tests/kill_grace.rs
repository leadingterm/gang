//! Checks that a terminated `noop` action keeps its grace period under `gang-exec`, and that
//! the host removes the action's scratch directory once the action has exited.
#![cfg(target_os = "linux")]

mod support;

use std::error::Error;
use std::io::{BufRead, BufReader, Lines, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{ChildStdout, Command, Stdio};
use std::time::Duration;

use gang_config::SANDBOX_FILE_NAME;
use gang_testkit::ChildTest;
use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process_group};
use support::{required_path, wait_within};
use tokio::signal::unix::{SignalKind, signal};

type TestResult = Result<(), Box<dyn Error>>;

/// Deadline for the host process to exit once its action has been released.
const CHILD_LIMIT: Duration = Duration::from_mins(1);

/// The environment variable that turns this test binary into the action.
const ACTION_VARIABLE: &str = "GANG_GRACE_ACTION";

const GRACE_ACTION: ChildTest = ChildTest::new("grace_action");

/// The prefix of every line the action prints, so libtest's own output can be told apart.
const ACTION_PREFIX: &str = "grace:";

/// The text of a sandbox file that selects the `noop` sandbox with its default section, keeps
/// action inputs linked and sets no limits.
fn sandbox_file(work_directory: &Path) -> String {
    format!(
        "work_directory = \"{}\"\nselected = \"noop\"\n\n[sandbox.noop]\n\n[execution]\ninput_links = \"keep\"\n",
        work_directory.display()
    )
}

/// Reads the next action line of the host's stdout and returns it from `ACTION_PREFIX` onwards,
/// dropping any libtest text printed before it on the same line. Lines without `ACTION_PREFIX`
/// are skipped, and the function fails if the host closes stdout first.
fn next_action_line(lines: &mut Lines<BufReader<ChildStdout>>) -> Result<String, Box<dyn Error>> {
    for line in lines.by_ref() {
        let line = line?;
        if let Some(start) = line.find(ACTION_PREFIX) {
            return Ok(line[start..].to_owned());
        }
    }
    Err("the host closed stdout before the expected action line".into())
}

/// Kills every process in the group led by `pid`, treating a group that is already gone as
/// killed.
fn kill_group(pid: Pid) -> Result<(), Box<dyn Error>> {
    match kill_process_group(pid, Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// A `noop` action that is terminated runs its trap and keeps its grace period: the host waits
/// for the action to finish, so it exits with the action's status rather than by the signal,
/// and it removes the action's scratch directory afterwards.
#[test]
fn a_terminated_noop_action_keeps_its_grace_and_its_host_cleans_up() -> TestResult {
    let dir = tempfile::tempdir()?;
    let exec = dir.path().join("gang-exec");
    std::os::unix::fs::symlink(required_path("GANG_BIN")?, &exec)?;
    let work = dir.path().join("work");
    std::fs::create_dir(&work)?;
    // `NativeLink` runs every action in `<work>/<id>/work`; `gang exec` rejects any other directory.
    let working_directory = work.join("a1/work");
    std::fs::create_dir_all(&working_directory)?;
    std::fs::write(dir.path().join(SANDBOX_FILE_NAME), sandbox_file(&work))?;

    // The action is this test binary: no host program runs.
    let test_binary = std::env::current_exe()?;
    // The host gets its own process group, so the group signals below never reach the test.
    let mut host = Command::new(&exec)
        .arg(&test_binary)
        .args(GRACE_ACTION.arguments())
        .env_clear()
        .env(ACTION_VARIABLE, "1")
        .current_dir(&working_directory)
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let pid = Pid::from_child(&host);
    let mut stdin = host.stdin.take().ok_or("the host's stdin was not piped")?;
    let stdout = host
        .stdout
        .take()
        .ok_or("the host's stdout was not piped")?;
    let mut lines = BufReader::new(stdout).lines();

    let tmpdir_line = next_action_line(&mut lines)?;
    let scratch = PathBuf::from(
        tmpdir_line
            .strip_prefix("grace:tmpdir ")
            .ok_or("the action did not print its TMPDIR first")?,
    );
    assert!(
        scratch.try_exists()?,
        "the action's TMPDIR does not exist while the action runs"
    );
    assert_eq!(next_action_line(&mut lines)?, "grace:ready");

    kill_process_group(pid, Signal::TERM)?;
    assert_eq!(
        next_action_line(&mut lines)?,
        "grace:terminating",
        "the action's SIGTERM handler did not run"
    );
    assert!(
        host.try_wait()?.is_none(),
        "the host exited while the action was still in its grace period"
    );

    stdin.write_all(b"go\n")?;
    drop(stdin);
    let status = wait_within(host, CHILD_LIMIT, false);
    // Stragglers of the action must not outlive the test, whatever the host did.
    kill_group(pid)?;
    let status = status?.status;

    assert_eq!(
        status.code(),
        Some(7),
        "the host should exit with the action's status, not by the signal"
    );
    assert!(
        !scratch.try_exists()?,
        "the host did not remove the action's TMPDIR after the action exited"
    );
    Ok(())
}

/// The action the grace test runs, ignored by the ordinary run; does nothing unless
/// `GANG_GRACE_ACTION` is set. It announces its TMPDIR and readiness, waits for SIGTERM,
/// announces that, reads one line from stdin and exits with status 7.
#[ignore = "child process helper of the grace test"]
#[test]
fn grace_action() {
    if std::env::var_os(ACTION_VARIABLE).is_none() {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the action runtime starts")
        .block_on(async {
            let mut terminate =
                signal(SignalKind::terminate()).expect("the SIGTERM handler installs");
            let tmpdir = std::env::var_os("TMPDIR").expect("the action has a TMPDIR");
            println!("grace:tmpdir {}", Path::new(&tmpdir).display());
            println!("grace:ready");
            terminate
                .recv()
                .await
                .expect("the SIGTERM stream stays open");
            println!("grace:terminating");
            let mut line = String::new();
            std::io::stdin()
                .read_line(&mut line)
                .expect("the host sends a line on stdin");
            std::process::exit(7);
        });
}
