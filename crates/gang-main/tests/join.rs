//! Checks the `gang-join` trampoline as a black box: the command runs in place after it has
//! joined the scope, and a failed join never starts the command.
#![cfg(target_os = "linux")]

mod support;

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use gang_spi::JOIN_LINK_NAME;
use gang_testkit::ChildTest;
use support::{required_path, wait_within};

type TestResult = Result<(), Box<dyn Error>>;

/// Deadline for the `gang-join` process and the command it becomes.
const CHILD_LIMIT: Duration = Duration::from_mins(1);

/// The environment variable that turns this test binary into the command.
const ACTION_VARIABLE: &str = "GANG_JOIN_ACTION";

/// The test that runs as the command.
const JOIN_ACTION: ChildTest = ChildTest::new("join_action");

/// The prefix of the line the command prints with its pid, so libtest's own output is skipped.
const PID_PREFIX: &str = "join:pid ";

/// The file that stands in for cgroupfs's `cgroup.procs` in the scope directory.
const PROCS_FILE: &str = "cgroup.procs";

/// Links `gang-join` in `root` to the `gang` binary, so the binary dispatches on that argv0.
fn join_link(root: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let link = root.join(JOIN_LINK_NAME);
    std::os::unix::fs::symlink(required_path("GANG_BIN")?, &link)?;
    Ok(link)
}

/// Builds `<link> <scope> <this test binary> <JOIN_ACTION arguments>` with an empty environment
/// except the variable that makes the test binary act as the command.
fn join_command(link: &Path, scope: &Path) -> Result<Command, Box<dyn Error>> {
    let mut command = Command::new(link);
    command
        .arg(scope)
        // The command is this test binary: no host program runs.
        .arg(std::env::current_exe()?)
        .args(JOIN_ACTION.arguments())
        .env_clear()
        .env(ACTION_VARIABLE, "1");
    Ok(command)
}

/// Returns the pid the command reported, the text after [`PID_PREFIX`] on its line.
fn reported_pid(stdout: &[u8]) -> Result<String, Box<dyn Error>> {
    let text = String::from_utf8_lossy(stdout);
    text.lines()
        .find_map(|line| line.split_once(PID_PREFIX))
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .map(str::to_owned)
        .ok_or_else(|| "the command did not report its pid".into())
}

/// The command under `gang-join`: this test binary, run as [`JOIN_ACTION`].
#[ignore = "child process helper of the join test"]
#[test]
fn join_action() {
    if std::env::var_os(ACTION_VARIABLE).is_none() {
        return;
    }
    println!("{PID_PREFIX}{}", std::process::id());
    JOIN_ACTION.complete();
}

#[test]
fn the_command_runs_inside_the_scope_after_joining() -> TestResult {
    let root = tempfile::tempdir()?;
    let scope = root.path().join("scope");
    fs::create_dir(&scope)?;
    fs::write(scope.join(PROCS_FILE), b"")?;
    let link = join_link(root.path())?;
    let child = join_command(&link, &scope)?
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = wait_within(child, CHILD_LIMIT, true)?;
    assert!(
        output.status.success(),
        "gang-join failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        JOIN_ACTION.completed_in(&stdout),
        "the command did not complete: {stdout}"
    );
    let pid = reported_pid(&output.stdout)?;
    assert_eq!(fs::read_to_string(scope.join(PROCS_FILE))?.trim(), pid);
    Ok(())
}

#[test]
fn a_failed_join_never_runs_the_command() -> TestResult {
    let root = tempfile::tempdir()?;
    let scope = root.path().join("scope");
    fs::create_dir(&scope)?;
    let link = join_link(root.path())?;
    let child = join_command(&link, &scope)?
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = wait_within(child, CHILD_LIMIT, true)?;
    assert!(!output.status.success(), "a failed join must exit non-zero");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains(PID_PREFIX),
        "the command must not run after a failed join: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&scope.display().to_string()),
        "stderr must name the scope: {stderr}"
    );
    Ok(())
}
