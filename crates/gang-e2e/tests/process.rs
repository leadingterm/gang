//! Tests of the process table reader, the worker selection and the environment scrubbing.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use gang_e2e::E2eError;
use gang_e2e::process::{ProcessEntry, run_until, scrub_environment, snapshot, workers_of};
use gang_testkit::ChildTest;

const SCRUB_CHILD: ChildTest = ChildTest::new("assert_leaking_variables_are_removed");
const PARKED_CHILD: ChildTest = ChildTest::new("parked_child");
const SNAPSHOT_CHILD: ChildTest =
    ChildTest::new("snapshot_contains_current_process_with_arguments");

/// Environment variable that marks a re-run of this test binary as the child that inherits the
/// leaking variables.
const CHILD_MARKER: &str = "GANG_E2E_SCRUB_CHILD";

/// Variables an outer `bazel run` leaks, which the child process is started with.
const LEAKING_FOR_CHILD: [(&str, &str); 2] = [
    ("BAZELRC", "/dev/null"),
    ("BUILD_WORKSPACE_DIRECTORY", "/tmp"),
];

#[test]
fn snapshot_contains_current_process_with_arguments() {
    assert!(
        snapshot()
            .iter()
            .any(|entry| entry.pid == std::process::id() && !entry.args.is_empty())
    );
    SNAPSHOT_CHILD.complete();
}

#[test]
fn workers_of_selects_only_workers_of_the_given_config() {
    let entries: Vec<ProcessEntry> = [
        (10, vec!["/usr/bin/gang", "worker", "/tmp/a/gang.toml"]),
        (11, vec!["/usr/bin/gang", "worker", "/tmp/b/gang.toml"]),
        (
            12,
            vec!["/usr/bin/gang", "server", "--config", "/tmp/a/gang.toml"],
        ),
        (
            13,
            vec!["/usr/bin/gang", "worker", "--config=/tmp/a/gang.toml"],
        ),
        (
            14,
            vec!["/usr/bin/gang", "worker", "--config", "/tmp/a/gang.toml"],
        ),
    ]
    .into_iter()
    .map(|(pid, args)| ProcessEntry {
        pid,
        args: args.into_iter().map(str::to_owned).collect(),
    })
    .collect();
    let pids: Vec<u32> = workers_of(&entries, Path::new("/tmp/a/gang.toml"))
        .iter()
        .map(|entry| entry.pid)
        .collect();
    assert_eq!(pids, vec![10, 13, 14]);
}

#[test]
fn scrub_environment_removes_exactly_the_leaking_variables() {
    let mut command = Command::new("gang");
    scrub_environment(&mut command);

    let removed: BTreeSet<OsString> = command
        .get_envs()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| name.to_owned())
        .collect();
    let expected: BTreeSet<OsString> = std::env::vars_os()
        .map(|(name, _)| name)
        .filter(|name| {
            let name = name.to_string_lossy();
            name.starts_with("BUILD_")
                || name.starts_with("TEST_")
                || name.contains("RUNFILES")
                || name == "BAZELRC"
        })
        .collect();
    assert_eq!(removed, expected);
}

/// Asserts, inside the child process, that a scrubbed command removes the leaking variables the
/// child inherited, and keeps `HOME` and `PATH`.
#[ignore = "child process helper of the process tests"]
#[test]
fn assert_leaking_variables_are_removed() {
    if std::env::var_os(CHILD_MARKER).is_none() {
        return;
    }
    for (name, _) in LEAKING_FOR_CHILD {
        assert!(
            std::env::var_os(name).is_some(),
            "the child must inherit {name}"
        );
    }

    let mut command = Command::new("gang");
    scrub_environment(&mut command);
    let removed: BTreeSet<OsString> = command
        .get_envs()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| name.to_owned())
        .collect();

    for (name, _) in LEAKING_FOR_CHILD {
        assert!(
            removed.contains(OsStr::new(name)),
            "{name} must be removed from a scrubbed command"
        );
    }
    for name in ["HOME", "PATH"] {
        assert!(
            !removed.contains(OsStr::new(name)),
            "{name} must be kept by a scrubbed command"
        );
    }
    SCRUB_CHILD.complete();
}

#[test]
fn bazelrc_does_not_leak_into_a_scrubbed_command() {
    // The ignored child helper checks the scrubbed environment in a separate process.

    // `BAZELRC` is set only on the child process, so the test process keeps no global state.
    let mut child = SCRUB_CHILD
        .command()
        .expect("the test binary path is known");
    child.env(CHILD_MARKER, "1");
    for (name, value) in LEAKING_FOR_CHILD {
        child.env(name, value);
    }
    let output = child.output().expect("the test binary can be re-run");
    SCRUB_CHILD.assert_completed(&output);
}

/// A child process that remains alive until its process group is killed.
#[ignore = "child process helper of the process tests"]
#[test]
fn parked_child() {
    std::thread::park();
}

#[test]
fn run_until_gives_up_with_the_abort_reason() {
    let command = PARKED_CHILD
        .command()
        .expect("the test binary path is known");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    sender
        .send("worker refused its fleet".to_owned())
        .expect("receiver is alive");
    let error = run_until(command, None, Duration::from_mins(1), receiver)
        .expect_err("the abort stops the command");
    assert!(matches!(error, E2eError::System { .. }));
    assert!(error.to_string().contains("worker refused its fleet"));
}

#[test]
fn run_until_ignores_a_dropped_abort() {
    let command = SNAPSHOT_CHILD
        .command()
        .expect("the test binary path is known");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    drop(sender);
    let output = run_until(command, None, Duration::from_mins(1), receiver)
        .expect("the command completes despite the dropped sender");
    assert_eq!(output.code, Some(0));
    assert!(
        SNAPSHOT_CHILD.completed_in(&output.stdout),
        "the child did not complete:\n{}{}",
        output.stdout,
        output.stderr
    );
}
