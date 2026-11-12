//! Command-line entry point: `gang-embed <dist-dir> <out.rs>`.

use std::error::Error as _;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use gang_embed::EmbedError;

fn main() -> ExitCode {
    let (dist, out) = match parse_args(std::env::args_os().skip(1)) {
        Ok(paths) => paths,
        Err(error) => return report(&error),
    };
    match gang_embed::generate(&dist, &out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => report(&error),
    }
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<(PathBuf, PathBuf), EmbedError> {
    let mut args = args.into_iter();
    match (args.next(), args.next(), args.next()) {
        (Some(dist), Some(out), None) => Ok((PathBuf::from(dist), PathBuf::from(out))),
        _ => Err(EmbedError::Usage),
    }
}

fn report(error: &EmbedError) -> ExitCode {
    let mut line = format!("gang-embed: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        let _ = write!(line, ": {cause}");
        source = cause.source();
    }
    eprintln!("{line}");
    ExitCode::FAILURE
}
