//! A stand-in for `bwrap` in this crate's unit tests.
//!
//! It receives bubblewrap's argv, reports a pid on the `--info-fd` descriptor the way bubblewrap
//! does, and then runs the action named after `--`. The report goes through `/proc/self/fd`, so
//! the binary is Linux only. It uses std and tokio only.
//!
//! Actions:
//! - `exit <code>`: reports its own pid and exits with `<code>`.
//! - `hold <path>`: reports its own pid, opens `<path>` for writing, and blocks until killed.
//! - `sandbox <path>`: starts a leader in its own process group that runs the action, reports the
//!   leader's pid, and exits with the leader's status.
//! - `starting <path>`: as `sandbox`, but the leader only records that it is idle and waits.
//! - `environment <path>`: writes its environment as `NAME=VALUE` lines to `<path>` and exits 0.
//!   It reports no pid.
//! - `report <payload>`: writes the payload to the info fd and stays alive with the fd open.
//! - `mask <path>`: writes its blocked-signal mask and its and its parent's process groups.
//!
//! The leader and the action are this binary re-executed with a leading `leader` or `action`
//! argument. The leader ignores `SIGTERM` as the sandbox's PID-namespace init does. The action
//! records `ready` and then `terminated` in `<path>` around its `SIGTERM` and exits.

use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command, ExitStatus, Stdio};

use tokio::signal::unix::{Signal, SignalKind, signal};

/// The first argument that makes this binary the sandbox's leader.
const LEADER: &str = "leader";
/// The first argument that makes this binary the action.
const ACTION: &str = "action";

/// Prints `message` to standard error and exits with status 2.
fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("fake_bwrap: {message}");
    std::process::exit(2)
}

/// The exit status of a child: its code, or `128 + n` when signal `n` killed it.
fn exit_code(status: ExitStatus) -> i32 {
    status
        .code()
        .or_else(|| status.signal().map(|signal| 128 + signal))
        .unwrap_or_else(|| fail("child has neither an exit code nor a signal"))
}

/// Writes `{"child-pid": pid}` to the `--info-fd` descriptor `info_fd`.
fn report(info_fd: i32, pid: u32) {
    let path = format!("/proc/self/fd/{info_fd}");
    let mut file = OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap_or_else(|error| fail(format!("opening {path}: {error}")));
    file.write_all(format!("{{\"child-pid\": {pid}}}").as_bytes())
        .unwrap_or_else(|error| fail(format!("writing the report: {error}")));
}

/// Opens `path` for appending, which the test reads as a FIFO.
fn open_record(path: &OsStr) -> File {
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap_or_else(|error| fail(format!("opening {}: {error}", path.display())))
}

/// Writes `line` and a newline to `record`.
fn write_line(record: &mut File, line: &str) {
    writeln!(record, "{line}").unwrap_or_else(|error| fail(format!("writing {line}: {error}")));
}

/// Splits bubblewrap's argv at `--`, returning the `--info-fd` descriptor and the action argv.
fn split_arguments(argv: &[OsString]) -> (i32, &[OsString]) {
    let mut info_fd = None;
    let mut arguments = argv.iter();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--info-fd") => {
                let value = arguments
                    .next()
                    .and_then(|value| value.to_str())
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or_else(|| fail("--info-fd needs a descriptor number"));
                info_fd = Some(value);
            }
            Some("--") => {
                let info_fd = info_fd.unwrap_or_else(|| fail("--info-fd is missing before --"));
                return (info_fd, arguments.as_slice());
            }
            _ => {}
        }
    }
    fail("bwrap argv has no `--` separator")
}

/// Splits a mode argument from its operands.
fn split_mode(arguments: &[OsString]) -> (&str, &[OsString]) {
    let Some((mode, operands)) = arguments.split_first() else {
        fail("no mode given")
    };
    let Some(mode) = mode.to_str() else {
        fail("the mode is not UTF-8")
    };
    (mode, operands)
}

/// Returns the single path operand of a mode.
fn path_operand(operands: &[OsString]) -> &OsStr {
    match operands {
        [path] => path.as_os_str(),
        _ => fail("the mode takes exactly one path"),
    }
}

/// Builds the current-thread runtime the leader and the action run on.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| fail(format!("building the runtime: {error}")))
}

/// Registers the `SIGTERM` listener, which must happen inside the runtime.
fn sigterm() -> Signal {
    signal(SignalKind::terminate())
        .unwrap_or_else(|error| fail(format!("listening for SIGTERM: {error}")))
}

/// Runs as bubblewrap, where `argv` is everything after the program name.
fn bwrap(argv: &[OsString]) -> ! {
    let (info_fd, action) = split_arguments(argv);
    let action = action
        .strip_prefix(&[OsString::from("/gang-reset")])
        .unwrap_or_else(|| fail("missing reset trampoline"));
    let (mode, operands) = split_mode(action);
    match mode {
        "mask" => {
            let status = std::fs::read_to_string("/proc/self/status")
                .unwrap_or_else(|error| fail(format!("reading status: {error}")));
            let mask = status
                .lines()
                .find(|line| line.starts_with("SigBlk:"))
                .unwrap_or_else(|| fail("missing SigBlk"));
            let stat = std::fs::read_to_string("/proc/self/stat")
                .unwrap_or_else(|error| fail(format!("reading stat: {error}")));
            let fields: Vec<_> = stat
                .rsplit_once(") ")
                .unwrap_or_else(|| fail("invalid stat"))
                .1
                .split_whitespace()
                .collect();
            let parent = fields.get(1).unwrap_or_else(|| fail("missing parent"));
            let group = fields.get(2).unwrap_or_else(|| fail("missing group"));
            let parent_stat = std::fs::read_to_string(format!("/proc/{parent}/stat"))
                .unwrap_or_else(|error| fail(format!("reading parent stat: {error}")));
            let parent_group = parent_stat
                .rsplit_once(") ")
                .unwrap_or_else(|| fail("invalid parent stat"))
                .1
                .split_whitespace()
                .nth(2)
                .unwrap_or_else(|| fail("missing parent group"));
            std::fs::write(
                path_operand(operands),
                format!("{mask}\n{group}\n{parent_group}\n"),
            )
            .unwrap_or_else(|error| fail(format!("writing mask: {error}")));
            std::process::exit(0)
        }
        "exit" => {
            report(info_fd, std::process::id());
            let code = match operands {
                [code] => code
                    .to_str()
                    .and_then(|code| code.parse::<i32>().ok())
                    .unwrap_or_else(|| fail("exit takes a numeric code")),
                _ => fail("exit takes exactly one code"),
            };
            std::process::exit(code)
        }
        "hold" => {
            report(info_fd, std::process::id());
            // The test opens the FIFO for reading. This writer stays open until the process dies.
            let _started = open_record(path_operand(operands));
            loop {
                std::thread::park();
            }
        }
        "report" => {
            let payload = match operands {
                [payload] => payload
                    .to_str()
                    .unwrap_or_else(|| fail("report needs UTF-8")),
                _ => fail("report takes exactly one payload"),
            };
            let path = format!("/proc/self/fd/{info_fd}");
            let mut file = OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap_or_else(|error| fail(format!("opening {path}: {error}")));
            file.write_all(payload.as_bytes())
                .unwrap_or_else(|error| fail(format!("writing the report: {error}")));
            loop {
                std::thread::park();
            }
        }
        "sandbox" => supervise(info_fd, "action", path_operand(operands)),
        "starting" => supervise(info_fd, "idle", path_operand(operands)),
        "environment" => {
            let path = path_operand(operands);
            let mut record = File::create(path).unwrap_or_else(|error| {
                fail(format!("creating {}: {error}", path.to_string_lossy()))
            });
            for (name, value) in std::env::vars_os() {
                write_line(
                    &mut record,
                    &format!("{}={}", name.to_string_lossy(), value.to_string_lossy()),
                );
            }
            std::process::exit(0)
        }
        other => fail(format!("unknown mode {other:?}")),
    }
}

/// Starts the leader, reports its pid as the sandbox's first process, and exits with its status.
fn supervise(info_fd: i32, leader_mode: &str, path: &OsStr) -> ! {
    let executable =
        std::env::current_exe().unwrap_or_else(|error| fail(format!("finding self: {error}")));
    let mut leader = Command::new(executable)
        .arg(LEADER)
        .arg(leader_mode)
        .arg(path)
        .process_group(0)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| fail(format!("starting the leader: {error}")));
    report(info_fd, leader.id());
    // `Child::wait` closes the child's stdin before waiting; keeping it here means an idle leader
    // reaches end of file only once this process is gone, never while it waits.
    let _leader_stdin = leader.stdin.take();
    let status = leader
        .wait()
        .unwrap_or_else(|error| fail(format!("waiting for the leader: {error}")));
    std::process::exit(exit_code(status))
}

/// Runs as the sandbox's leader. It ignores `SIGTERM`, as the sandbox's init does, and either runs
/// the action or waits until bubblewrap dies.
fn leader(arguments: &[OsString]) -> ! {
    let (mode, operands) = split_mode(arguments);
    let path = path_operand(operands).to_os_string();
    let code = runtime().block_on(async move {
        let _ignored_sigterm = sigterm();
        match mode {
            "action" => {
                let executable = std::env::current_exe()
                    .unwrap_or_else(|error| fail(format!("finding self: {error}")));
                let mut child = tokio::process::Command::new(executable)
                    .arg(ACTION)
                    .arg(&path)
                    .spawn()
                    .unwrap_or_else(|error| fail(format!("starting the action: {error}")));
                let status = child
                    .wait()
                    .await
                    .unwrap_or_else(|error| fail(format!("waiting for the action: {error}")));
                exit_code(status)
            }
            "idle" => {
                write_line(&mut open_record(&path), "idle");
                let mut rest = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut rest)
                    .unwrap_or_else(|error| fail(format!("reading stdin: {error}")));
                0
            }
            other => fail(format!("unknown leader mode {other:?}")),
        }
    });
    std::process::exit(code)
}

/// Runs as the action. It records `ready` once `SIGTERM` is handled, then records `terminated`
/// when `SIGTERM` arrives and exits.
fn action(arguments: &[OsString]) -> ! {
    let path = path_operand(arguments);
    // Mimic gang-reset in the sandbox action, not in the outer launcher.
    gang_os::clear_mask().unwrap_or_else(|error| fail(format!("resetting mask: {error}")));
    // The record stays open until the action exits, so a reader of the FIFO sees `terminated`
    // after `ready` without end of file in between.
    let mut record = open_record(path);
    runtime().block_on(async {
        let mut terminate = sigterm();
        write_line(&mut record, "ready");
        terminate.recv().await;
        write_line(&mut record, "terminated");
    });
    std::process::exit(0)
}

/// Dispatches on the leader and action markers; anything else is bubblewrap's argv.
fn main() {
    let argv: Vec<OsString> = std::env::args_os().skip(1).collect();
    match argv.split_first() {
        Some((first, rest)) if first.to_str() == Some(LEADER) => leader(rest),
        Some((first, rest)) if first.to_str() == Some(ACTION) => action(rest),
        _ => bwrap(&argv),
    }
}
