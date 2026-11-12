//! A stand-in for Bazel that `gang doctor --bazel` runs: the harness's own `local` binary,
//! selected by an environment variable, so no program outside the harness runs. It answers
//! `build --nobuild --build_event_json_file=<path> <options>` as Bazel reports explicit options:
//! one build event whose canonical command line lists every `--name[=value]` option it was
//! given, in order.

use std::ffi::OsString;
use std::process::ExitCode;

use serde_json::json;

/// The variable that makes the `local` binary act as Bazel; its value is ignored.
pub const FAKE_BAZEL_ENV: &str = "GANG_E2E_FAKE_BAZEL";

/// The option that names the file the build events are written to.
const EVENTS_OPTION: &str = "--build_event_json_file=";

/// Whether this process was started as the fake Bazel.
#[must_use]
pub fn fake_bazel_requested() -> bool {
    std::env::var_os(FAKE_BAZEL_ENV).is_some()
}

/// Answers one Bazel invocation whose arguments, without the program, are `args`: writes the
/// build event of its options, one JSON line, to the file `--build_event_json_file=` names and
/// exits 0. Exits 2 with a message on standard error when an argument is not UTF-8, no such
/// option is given, or the file cannot be written.
pub fn fake_bazel(args: impl Iterator<Item = OsString>) -> ExitCode {
    let mut strings = Vec::new();
    for arg in args {
        let Ok(text) = arg.into_string() else {
            eprintln!("fake bazel: an argument is not UTF-8");
            return ExitCode::from(2);
        };
        strings.push(text);
    }
    let Some(path) = strings
        .iter()
        .find_map(|arg| arg.strip_prefix(EVENTS_OPTION))
    else {
        eprintln!("fake bazel: no {EVENTS_OPTION}<path> option");
        return ExitCode::from(2);
    };
    if let Err(error) = std::fs::write(path, command_line_event(&strings) + "\n") {
        eprintln!("fake bazel: cannot write {path}: {error}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}

/// The canonical command line build event, as one JSON line without a newline, of a Bazel
/// invocation with `args`: each argument starting with `--` is an option, `--name=value` with
/// `optionValue` `value` and `--name` without one; other arguments, such as the command
/// `build`, are not options.
#[must_use]
pub fn command_line_event(args: &[String]) -> String {
    let options: Vec<serde_json::Value> = args
        .iter()
        .filter_map(|arg| {
            let body = arg.strip_prefix("--")?;
            Some(match body.split_once('=') {
                Some((name, value)) => json!({"optionName": name, "optionValue": value}),
                None => json!({"optionName": body}),
            })
        })
        .collect();
    json!({
        "structuredCommandLine": {
            "commandLineLabel": "canonical",
            "sections": [{
                "sectionLabel": "command options",
                "optionList": {"option": options},
            }],
        }
    })
    .to_string()
}
