//! A stand-in for both a launcher and the gang worker in gang-local's process tests.
//!
//! When the first argument is `worker` it is the worker: it parks its thread forever, so only a
//! signal ends it. Otherwise it is the launcher: it execs the arguments in place, so the process
//! keeps its pid, its standard input (the lifetime lock) and its signal path. It uses std only.

use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::process::{Command, exit};

/// The first argument that makes this binary the worker.
const WORKER: &str = "worker";

/// Exit status for a usage error or an exec that failed.
const FAILURE: i32 = 2;

/// Runs as the worker, or execs the arguments in place as the launcher.
fn main() {
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    let Some((program, operands)) = arguments.split_first() else {
        eprintln!("fake_launcher: expected a worker marker or a program to exec");
        exit(FAILURE);
    };
    if program.as_os_str() == WORKER {
        loop {
            std::thread::park();
        }
    }
    let error = Command::new(program).args(operands).exec();
    eprintln!("fake_launcher: exec {}: {error}", program.to_string_lossy());
    exit(FAILURE);
}
