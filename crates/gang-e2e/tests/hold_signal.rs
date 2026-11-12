//! Exercises the hold probe's actual shell command against its signal pipe.

use std::process::Command;
use std::time::Duration;

use gang_e2e::{SIGNAL_COMMAND, Signal, Signalled};

/// Bounds only a broken test; a successful wait returns as soon as the byte arrives.
const WAIT: Duration = Duration::from_secs(30);

#[test]
fn shell_command_signals_started() {
    let directory = tempfile::tempdir().expect("temporary directory is created");
    let path = directory.path().join("signal pipe");
    let signal = Signal::create(&path).expect("signal pipe is created");
    let bash = std::env::var_os("GANG_E2E_TEST_BASH").expect("Bazel supplies pinned Bash");

    // A pinned shell is necessary here because the command under test is shell code.
    let status = Command::new(bash)
        .arg("-c")
        .arg(SIGNAL_COMMAND)
        .env_clear()
        .env("E2E_SIGNAL", &path)
        .status()
        .expect("pinned Bash starts");
    assert!(status.success(), "signal command failed: {status}");
    assert_eq!(
        signal.wait(WAIT).expect("signal is received"),
        Signalled::Started
    );
}
