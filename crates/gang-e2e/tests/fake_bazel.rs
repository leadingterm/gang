//! The fake Bazel answers with the canonical command line `gang doctor` reads.

use std::error::Error;

use gang_e2e::{command_line_event, fake_bazel_requested};
use serde_json::json;

/// The canonical command line of `args`, parsed back from its JSON line.
fn parsed(args: &[&str]) -> Result<serde_json::Value, Box<dyn Error>> {
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    let line = command_line_event(&args);
    Ok(serde_json::from_str(&line)?)
}

#[test]
fn options_with_values_are_listed_in_order() -> Result<(), Box<dyn Error>> {
    let event = parsed(&[
        "build",
        "--nobuild",
        "--remote_executor=grpc://127.0.0.1:1",
        "--remote_cache=",
    ])?;
    assert_eq!(
        event,
        json!({
            "structuredCommandLine": {
                "commandLineLabel": "canonical",
                "sections": [{
                    "sectionLabel": "command options",
                    "optionList": {"option": [
                        {"optionName": "nobuild"},
                        {"optionName": "remote_executor", "optionValue": "grpc://127.0.0.1:1"},
                        {"optionName": "remote_cache", "optionValue": ""},
                    ]},
                }],
            }
        })
    );
    Ok(())
}

#[test]
fn words_that_are_not_options_are_skipped() -> Result<(), Box<dyn Error>> {
    let event = parsed(&["build", "//:all"])?;
    assert_eq!(
        event,
        json!({
            "structuredCommandLine": {
                "commandLineLabel": "canonical",
                "sections": [{
                    "sectionLabel": "command options",
                    "optionList": {"option": []},
                }],
            }
        })
    );
    Ok(())
}

#[test]
fn the_event_is_one_line() {
    let args = vec!["build".to_owned(), "--nobuild".to_owned()];
    assert!(!command_line_event(&args).contains('\n'));
}

#[test]
fn it_is_not_requested_by_default() {
    assert!(!fake_bazel_requested());
}
