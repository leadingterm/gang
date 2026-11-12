//! Command-line entry point: `gang-bindings <command>`.
//!
//! Prints generated artifacts that the dashboard build checks in or consumes.
//! Commands:
//! - `headers`: the production security headers as a JSON object.
//! - `wire-ts`: TypeScript declarations for the dashboard wire types.

mod headers;
mod wire_ts;

use std::ffi::OsString;
use std::io::{self, Write as _};
use std::process::ExitCode;

const USAGE_EXIT: u8 = 2;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("usage: gang-bindings <headers|wire-ts>")]
    Usage,
    #[error("{0}")]
    UnknownCommand(#[from] gang_types::UnknownVariantError),
    #[error("write stdout: {0}")]
    Write(#[from] io::Error),
    #[error("serialize json: {0}")]
    SerializeJson(#[from] serde_json::Error),
    #[error("decode headers as utf-8: {0}")]
    DecodeHeaders(#[from] std::string::FromUtf8Error),
}

gang_types::string_enum! {
    /// A subcommand of `gang-bindings`.
    pub enum Command("command") {
        /// Print production security headers.
        Headers = "headers",
        /// Print TypeScript declarations for wire types.
        WireTs = "wire-ts",
    }
}

impl Command {
    fn render(self) -> Result<String, Error> {
        match self {
            Self::Headers => headers::render(),
            Self::WireTs => wire_ts::render(),
        }
    }
}

fn main() -> ExitCode {
    match run(std::env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gang-bindings: {error}");
            match error {
                Error::Usage | Error::UnknownCommand(_) => ExitCode::from(USAGE_EXIT),
                Error::Write(_) | Error::SerializeJson(_) | Error::DecodeHeaders(_) => {
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn run(args: impl IntoIterator<Item = OsString>) -> Result<(), Error> {
    let command = parse(args)?;
    let mut stdout = io::stdout().lock();
    stdout.write_all(command.render()?.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Command, Error> {
    let mut args = args.into_iter();
    match (args.next(), args.next()) {
        (Some(name), None) => Ok(name.to_str().ok_or(Error::Usage)?.parse()?),
        _ => Err(Error::Usage),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn known_commands_are_recognised() {
        for (command, text) in [(Command::Headers, "headers"), (Command::WireTs, "wire-ts")] {
            assert_eq!(command.to_string(), text);
            assert!(matches!(parse(args(&[text])), Ok(parsed) if parsed == command));
        }
    }

    #[test]
    fn missing_and_extra_arguments_are_usage_errors() {
        for items in [&[][..], &["headers", "extra"]] {
            assert!(matches!(parse(args(items)), Err(Error::Usage)), "{items:?}");
        }
    }

    #[test]
    fn unknown_command_lists_the_accepted_names() {
        let error = parse(args(&["nope"])).expect_err("not a command");
        assert_eq!(
            error.to_string(),
            "unknown command `nope`, expected one of headers, wire-ts"
        );
    }

    #[test]
    fn production_table_is_rendered_with_every_header() {
        let rendered = Command::Headers.render().expect("render headers");
        for (name, value) in gang_ui::SECURITY_HEADERS {
            assert!(
                rendered.contains(&format!("  \"{name}\": \"{value}\"")),
                "{name}"
            );
        }
    }
}
