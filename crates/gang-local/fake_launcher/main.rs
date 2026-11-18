//! A stand-in for both a launcher and the gang worker in gang-local's process tests.
//!
//! When the first argument is `worker` it is the worker: it parks its thread forever, so only a
//! signal ends it. Otherwise it is the launcher: it execs the arguments in place, so the process
//! keeps its pid, its standard input (the lifetime lock) and its signal path. The hidden bootstrap
//! mode uses the real gang-local child publication protocol before exec.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
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
    if program.as_os_str() == "__local-worker" {
        let [
            record_flag,
            record,
            lock_flag,
            lifetime_lock,
            separator,
            program,
            arguments @ ..,
        ] = operands
        else {
            eprintln!(
                "fake_launcher: expected __local-worker --record PATH --lifetime-lock PATH -- PROGRAM ARGS"
            );
            exit(FAILURE);
        };
        if record_flag != "--record" || lock_flag != "--lifetime-lock" || separator != "--" {
            eprintln!(
                "fake_launcher: expected __local-worker --record PATH --lifetime-lock PATH -- PROGRAM ARGS"
            );
            exit(FAILURE);
        }
        if let Some(path) = std::env::var_os("GANG_TEST_BOOTSTRAP_GATE") {
            let mut gate = UnixStream::connect(path).unwrap_or_else(|error| {
                eprintln!("fake_launcher: connect bootstrap gate: {error}");
                exit(FAILURE);
            });
            gate.write_all(&std::process::id().to_ne_bytes())
                .unwrap_or_else(|error| {
                    eprintln!("fake_launcher: announce bootstrap: {error}");
                    exit(FAILURE);
                });
            let mut release = [0];
            gate.read_exact(&mut release).unwrap_or_else(|error| {
                eprintln!("fake_launcher: wait for bootstrap gate: {error}");
                exit(FAILURE);
            });
            if release != *b"G" {
                eprintln!("fake_launcher: invalid bootstrap gate byte");
                exit(FAILURE);
            }
        }
        match gang_local::bootstrap_worker(
            std::path::Path::new(record),
            std::path::Path::new(lifetime_lock),
            program,
            arguments,
        ) {
            Ok(never) => match never {},
            Err(error) => {
                eprintln!("fake_launcher: bootstrap: {error}");
                exit(FAILURE);
            }
        }
    }
    if program.as_os_str() == WORKER {
        let _witness = std::env::var_os("GANG_TEST_WORKER_WITNESS").map(|path| {
            let mut stream = UnixStream::connect(path).unwrap_or_else(|error| {
                eprintln!("fake_launcher: connect worker witness: {error}");
                exit(FAILURE);
            });
            stream.write_all(&[1]).unwrap_or_else(|error| {
                eprintln!("fake_launcher: notify worker witness: {error}");
                exit(FAILURE);
            });
            stream
        });
        loop {
            std::thread::park();
        }
    }
    let error = Command::new(program).args(operands).exec();
    eprintln!("fake_launcher: exec {}: {error}", program.to_string_lossy());
    exit(FAILURE);
}
