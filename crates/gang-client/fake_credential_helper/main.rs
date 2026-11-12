//! A stand-in for a Bazel credential helper in this crate's unit tests.
//!
//! Protocol:
//! - The probe runs `<fake> get` and writes `{"uri":"<uri>"}` to stdin.
//! - Test URIs are `https://gang.test/<mode>/<arg>/<anything>`.
//! - `reply/<b64>`: read stdin to EOF, write the URL-safe unpadded base64-decoded bytes to stdout,
//!   and exit 0.
//! - `unread/<b64>`: write the decoded bytes to stdout and exit 0 without reading the rest of
//!   stdin.
//! - `exit/<code>`: read stdin to EOF and exit with the decimal `<code>`.
//! - `echo/-`: read stdin to EOF, write the first argument, a newline, and the whole request, then
//!   exit 0.
//! - `hang/-`: read stdin to EOF, then block until killed.
//! - Invoked as `<fake> hang`, block until killed without reading stdin.
//! - Anything else, or EOF before the mode and argument are known: write an error to stderr and
//!   exit 2.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use std::io::{Read, Write};

/// What a request asks the fake to do.
enum Behaviour {
    /// Reads its request before returning the decoded bytes.
    Reply(Vec<u8>),
    /// Returns the decoded bytes without reading the remainder of the request.
    Unread(Vec<u8>),
    /// Exits with the requested code after reading the request.
    Exit(i32),
    /// Writes its first argument and the full request after reading it.
    Echo,
    /// Blocks after reading the request.
    Hang,
}

/// Prints `message` to standard error and exits with status 2.
fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

/// Writes all bytes to standard output, exiting with status 2 if writing fails.
fn write_stdout(bytes: &[u8]) {
    std::io::stdout()
        .lock()
        .write_all(bytes)
        .unwrap_or_else(|error| fail(format!("writing stdout: {error}")));
}

/// The mode and argument of a request, once the `/` ending the argument has arrived.
fn header(request: &[u8]) -> Option<(&[u8], &[u8])> {
    let prefix = b"https://gang.test/";
    let start = request
        .windows(prefix.len())
        .position(|window| window == prefix)?
        + prefix.len();
    let mode_end = request[start..].iter().position(|byte| *byte == b'/')? + start;
    let arg_start = mode_end + 1;
    let arg_end = request[arg_start..].iter().position(|byte| *byte == b'/')? + arg_start;
    Some((&request[start..mode_end], &request[arg_start..arg_end]))
}

/// The behaviour `mode` and `arg` ask for; fails on anything else.
fn behaviour(mode: &str, arg: &str) -> Behaviour {
    match mode {
        "reply" => Behaviour::Reply(
            URL_SAFE_NO_PAD
                .decode(arg)
                .unwrap_or_else(|error| fail(format!("decoding reply: {error}"))),
        ),
        "unread" => Behaviour::Unread(
            URL_SAFE_NO_PAD
                .decode(arg)
                .unwrap_or_else(|error| fail(format!("decoding reply: {error}"))),
        ),
        "exit" => Behaviour::Exit(
            arg.parse::<i32>()
                .unwrap_or_else(|error| fail(format!("invalid exit code: {error}"))),
        ),
        "echo" if arg == "-" => Behaviour::Echo,
        "hang" if arg == "-" => Behaviour::Hang,
        _ => fail(format!(
            "unsupported credential helper mode/argument: {mode}/{arg}"
        )),
    }
}

/// Starts the fake credential helper.
fn main() {
    if std::env::args_os().nth(1).is_some_and(|arg| arg == "hang") {
        loop {
            std::thread::park();
        }
    }
    let mut stdin = std::io::stdin().lock();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    let (mode, arg) = loop {
        let count = stdin
            .read(&mut buffer)
            .unwrap_or_else(|error| fail(format!("reading stdin: {error}")));
        if count == 0 {
            fail("stdin ended before the URI argument was known");
        }
        request.extend_from_slice(&buffer[..count]);
        if let Some(header) = header(&request) {
            break header;
        }
    };
    let mode = std::str::from_utf8(mode).unwrap_or_else(|error| fail(error));
    let arg = std::str::from_utf8(arg).unwrap_or_else(|error| fail(error));
    let behaviour = behaviour(mode, arg);

    if !matches!(&behaviour, Behaviour::Unread(_)) {
        stdin
            .read_to_end(&mut request)
            .unwrap_or_else(|error| fail(format!("reading stdin: {error}")));
    }

    match behaviour {
        Behaviour::Reply(bytes) | Behaviour::Unread(bytes) => write_stdout(&bytes),
        Behaviour::Exit(code) => std::process::exit(code),
        Behaviour::Echo => {
            let argument = std::env::args_os().nth(1).unwrap_or_default();
            write_stdout(argument.to_string_lossy().as_bytes());
            write_stdout(b"\n");
            write_stdout(&request);
        }
        Behaviour::Hang => loop {
            std::thread::park();
        },
    }
}
