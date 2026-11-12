//! Dispatches the `gang-exec`, `gang-guard`, `gang-join`, and `gang-reset` multicall links.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use gang_spi::{EXEC_LINK_NAME, GUARD_LINK_NAME, JOIN_LINK_NAME, RESET_LINK_NAME};

use crate::cli::{Cli, Command, LocalCommand};
use crate::errors::Result;
use crate::output::{Output, Record};
use crate::{
    bazel_command, ca_pin_command, client_command, deploy_command, doctor_command, exec_command,
    guard_command, help, join_command, local_down, local_up, options_command, reset_command,
    server_command, worker_command,
};

/// What a process runs, decided from its argv0 and arguments.
#[derive(Debug)]
pub(crate) struct Parsed {
    /// The parsed command to run.
    pub(crate) command: Command,
    /// The path the process was started through: the sandbox file sits beside it.
    pub(crate) entrypoint: PathBuf,
    /// Whether `--quiet` was given.
    pub(crate) quiet: bool,
}

/// A link invocation and its borrowed, unparsed arguments.
enum Multicall<'a> {
    Join(&'a [OsString]),
    Exec {
        entrypoint: &'a Path,
        argv: &'a [OsString],
    },
    Guard {
        entrypoint: &'a Path,
        creator: Option<u32>,
    },
    Reset(&'a [OsString]),
}

impl Multicall<'_> {
    /// The link name used to report a failure.
    fn subject(&self) -> &'static str {
        match self {
            Self::Join(_) => JOIN_LINK_NAME,
            Self::Exec { .. } => EXEC_LINK_NAME,
            Self::Guard { .. } => GUARD_LINK_NAME,
            Self::Reset(_) => RESET_LINK_NAME,
        }
    }
}

/// Identifies a multicall link by the final component of argv0.
fn multicall(args: &[OsString]) -> Option<Multicall<'_>> {
    let (argv0, arguments) = args.split_first()?;
    let entrypoint = Path::new(argv0);
    let link = entrypoint.file_name()?;
    if link == OsStr::new(JOIN_LINK_NAME) {
        Some(Multicall::Join(arguments))
    } else if link == OsStr::new(EXEC_LINK_NAME) {
        Some(Multicall::Exec {
            entrypoint,
            argv: arguments,
        })
    } else if link == OsStr::new(GUARD_LINK_NAME) {
        let creator = match arguments {
            [value] => value.to_str().and_then(|value| value.parse::<u32>().ok()),
            _ => None,
        };
        Some(Multicall::Guard {
            entrypoint,
            creator,
        })
    } else if link == OsStr::new(RESET_LINK_NAME) {
        Some(Multicall::Reset(arguments))
    } else {
        None
    }
}

/// Parses an ordinary `gang` subcommand.
pub(crate) fn parse(args: &[OsString]) -> std::result::Result<Parsed, clap::Error> {
    let entrypoint = args.first().map(PathBuf::from).unwrap_or_default();
    let Cli { quiet, command } = Cli::try_parse_from(args)?;
    Ok(Parsed {
        command,
        entrypoint,
        quiet,
    })
}

/// Runs the process named by `args` (argv0 first) and returns its exit code.
///
/// Help and version screens go to stdout with exit code 0. A usage error is a `fail` record on
/// stderr with exit code 2. A failing command prints its error as a `fail` record on stderr and
/// exits with failure.
#[must_use]
pub fn run(args: &[OsString]) -> ExitCode {
    if let Some(call) = multicall(args) {
        let subject = call.subject();
        let result = match call {
            Multicall::Join(arguments) => Err(join_command::run(arguments)),
            Multicall::Exec { entrypoint, argv } => exec_command::run(entrypoint, argv),
            Multicall::Guard {
                entrypoint,
                creator,
            } => {
                let Some(creator) = creator else {
                    Output::from_environment().step(
                        &Record::fail("requires exactly one creator process ID").subject(subject),
                    );
                    return ExitCode::from(2);
                };
                guard_command::run(entrypoint, creator)
            }
            Multicall::Reset(arguments) => reset_command::run(arguments),
        };
        return match result {
            Ok(code) => code,
            Err(error) => Output::from_environment().failure(subject, &error),
        };
    }
    let output = Output::from_environment();
    let Ok(Parsed {
        command,
        entrypoint,
        quiet,
    }) = parse(args)
    else {
        return help::report(&output, args, help::asks_for_banner(args));
    };
    output.set_quiet(quiet);
    let subject = command.subject();
    match execute(command, &entrypoint, &output) {
        Ok(code) => code,
        Err(error) => output.failure(subject, &error),
    }
}

/// Calls the handler for a command.
fn execute(command: Command, entrypoint: &Path, out: &Output) -> Result<ExitCode> {
    match command {
        Command::Exec(args) => exec_command::run(entrypoint, &args.command),
        Command::Login(args) => client_command::login(args, out),
        Command::Get => bazel_command::get(out),
        Command::Bazelrc(args) => bazel_command::bazelrc(&args, out),
        Command::Revoke(args) => client_command::revoke(args, out),
        Command::Restore(args) => client_command::restore(&args, out),
        Command::Server(args) => server_command::run(&args.config, out),
        Command::Worker(args) => worker_command::run(args, out),
        Command::Local(LocalCommand::Up(args)) => local_up::run(args, out),
        Command::Local(LocalCommand::Down(args)) => local_down::run(args, out),
        Command::Options(args) => options_command::run(args.provisioner.as_deref(), out),
        Command::Doctor(args) => doctor_command::run(args, out),
        Command::CaPin(args) => ca_pin_command::run(&args.config, out),
        Command::Deploy(command) => deploy_command::run(command, std::io::stdin().lock(), out),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::path::PathBuf;

    use clap::error::ErrorKind;

    use super::*;

    fn args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(|item| OsString::from(*item)).collect()
    }

    fn try_parse(items: Vec<OsString>) -> std::result::Result<Parsed, clap::Error> {
        parse(&items)
    }

    fn parse_str(items: &[&str]) -> Parsed {
        try_parse(args(items)).expect("parses")
    }

    fn expect_exec(parsed: Parsed) -> (PathBuf, Vec<OsString>) {
        match parsed.command {
            Command::Exec(exec) => (parsed.entrypoint, exec.command),
            other => panic!("expected an action run, got {other:?}"),
        }
    }

    fn expect_exec_link(items: &[OsString]) -> (&Path, &[OsString]) {
        let Some(Multicall::Exec { entrypoint, argv }) = multicall(items) else {
            panic!("expected the exec multicall");
        };
        (entrypoint, argv)
    }

    #[test]
    fn an_exec_link_failure_is_reported_as_gang_exec() {
        let argv = args(&["/work/bin/gang-exec", "echo"]);
        let Some(call @ Multicall::Exec { .. }) = multicall(&argv) else {
            panic!("expected the exec multicall");
        };
        assert_eq!(call.subject(), EXEC_LINK_NAME);
        assert_eq!(call.subject(), "gang-exec");
    }

    #[test]
    fn absolute_link_passes_arguments_through() {
        let items = args(&["/work/bin/gang-exec", "echo", "hi"]);
        let (entrypoint, argv) = expect_exec_link(&items);
        assert_eq!(entrypoint, Path::new("/work/bin/gang-exec"));
        assert_eq!(argv, args(&["echo", "hi"]));
    }

    #[test]
    fn relative_link_keeps_its_name_as_entrypoint() {
        let items = args(&["gang-exec", "echo", "hi"]);
        let (entrypoint, argv) = expect_exec_link(&items);
        assert_eq!(entrypoint, Path::new("gang-exec"));
        assert_eq!(argv, args(&["echo", "hi"]));
    }

    #[test]
    fn link_without_arguments_runs_an_empty_argv() {
        let items = args(&["gang-exec"]);
        let (entrypoint, argv) = expect_exec_link(&items);
        assert_eq!(entrypoint, Path::new("gang-exec"));
        assert!(argv.is_empty());
    }

    #[test]
    fn link_passes_flag_looking_arguments_untouched() {
        let items = args(&["/work/bin/gang-exec", "--version", "--help", "-q", "-x"]);
        let (_, argv) = expect_exec_link(&items);
        assert_eq!(argv, args(&["--version", "--help", "-q", "-x"]));
    }

    #[test]
    fn join_link_passes_its_arguments_untouched_to_the_join_command() {
        let argv = args(&[
            "/work/bin/gang-join",
            "/scope",
            "/bin/sh",
            "--version",
            "-q",
        ]);
        let Some(call @ Multicall::Join(_)) = multicall(&argv) else {
            panic!("expected the join multicall");
        };
        assert_eq!(call.subject(), JOIN_LINK_NAME);
        let Multicall::Join(arguments) = call else {
            panic!("expected the join multicall");
        };
        assert_eq!(arguments, &argv[1..]);
    }

    #[test]
    fn only_the_join_link_name_selects_the_join_command() {
        assert!(matches!(
            multicall(&args(&["/work/bin/gang-join"])),
            Some(Multicall::Join(_))
        ));
        assert!(!matches!(
            multicall(&args(&["/work/bin/gang-exec", "x"])),
            Some(Multicall::Join(_))
        ));
        assert!(multicall(&args(&["/usr/bin/gang-joiner", "x"])).is_none());
    }

    #[test]
    fn guard_and_reset_links_preserve_their_arguments_and_subjects() {
        let guard = args(&["/work/bin/gang-guard", "42"]);
        let Some(call @ Multicall::Guard { .. }) = multicall(&guard) else {
            panic!("expected the guard multicall");
        };
        assert_eq!(call.subject(), GUARD_LINK_NAME);
        let Multicall::Guard {
            entrypoint,
            creator,
        } = call
        else {
            panic!("expected the guard multicall");
        };
        assert_eq!(entrypoint, Path::new("/work/bin/gang-guard"));
        assert_eq!(creator, Some(42));

        let reset = args(&["/work/bin/gang-reset", "--", "command", "--flag"]);
        let Some(call @ Multicall::Reset(_)) = multicall(&reset) else {
            panic!("expected the reset multicall");
        };
        assert_eq!(call.subject(), RESET_LINK_NAME);
        let Multicall::Reset(arguments) = call else {
            panic!("expected the reset multicall");
        };
        assert_eq!(arguments, &reset[1..]);
    }

    #[test]
    fn guard_link_requires_exactly_one_numeric_creator() {
        for argv in [
            args(&["/work/bin/gang-guard"]),
            args(&["/work/bin/gang-guard", "bad"]),
            args(&["/work/bin/gang-guard", "4294967296"]),
            vec![OsString::from("gang-guard"), OsString::from_vec(vec![0xff])],
            args(&["/work/bin/gang-guard", "1", "extra"]),
        ] {
            assert_eq!(run(&argv), ExitCode::from(2));
        }
        assert_eq!(
            run(&args(&["/nonexistent/gang-guard", "1"])),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn server_subcommand_parses_to_a_command() {
        let parsed = parse_str(&["/usr/bin/gang", "server", "--config", "x.toml"]);
        assert!(matches!(parsed.command, Command::Server(_)));
        assert!(!parsed.quiet);
    }

    #[test]
    fn doctor_subcommand_parses_its_arguments() {
        let command = parse_str(&[
            "/usr/bin/gang",
            "doctor",
            "--bazel",
            "bazelisk",
            "--",
            "--config=ci",
        ])
        .command;
        let Command::Doctor(doctor) = command else {
            panic!("expected the doctor command");
        };
        assert_eq!(doctor.bazel, OsString::from("bazelisk"));
        assert_eq!(doctor.bazel_args, args(&["--config=ci"]));
    }

    #[test]
    fn exec_subcommand_with_separator_runs_the_trailing_arguments() {
        let (entrypoint, argv) =
            expect_exec(parse_str(&["/usr/bin/gang", "exec", "--", "sh", "-c", "x"]));
        assert_eq!(entrypoint, PathBuf::from("/usr/bin/gang"));
        assert_eq!(argv, args(&["sh", "-c", "x"]));
    }

    #[test]
    fn exec_subcommand_without_separator_runs_the_trailing_arguments() {
        let (entrypoint, argv) =
            expect_exec(parse_str(&["/usr/bin/gang", "exec", "sh", "-c", "x"]));
        assert_eq!(entrypoint, PathBuf::from("/usr/bin/gang"));
        assert_eq!(argv, args(&["sh", "-c", "x"]));
    }

    #[test]
    fn exec_subcommand_without_a_command_is_a_usage_error() {
        assert!(try_parse(args(&["/usr/bin/gang", "exec"])).is_err());
    }

    #[test]
    fn unknown_subcommand_is_a_usage_error() {
        match try_parse(args(&["/usr/bin/gang", "frobnicate"])) {
            Err(error) => assert_eq!(error.kind(), ErrorKind::InvalidSubcommand),
            Ok(_) => panic!("an unknown subcommand must not parse"),
        }
    }

    #[test]
    fn names_that_only_contain_the_link_name_are_not_multicall() {
        for name in ["gang-exec-helper", "my-gang-exec"] {
            let path = format!("/work/bin/{name}");
            let parsed = parse_str(&[&path, "server", "--config", "x.toml"]);
            assert!(
                matches!(parsed.command, Command::Server(_)),
                "{name} must parse as a subcommand"
            );
        }
    }

    #[test]
    fn non_utf8_link_path_is_handled_without_panicking() {
        let items = vec![
            OsString::from_vec(b"/work/bin/\xff/gang-exec".to_vec()),
            OsString::from("echo"),
            OsString::from("hi"),
        ];
        let (entrypoint, argv) = expect_exec_link(&items);
        assert_eq!(entrypoint, Path::new(&items[0]));
        assert_eq!(argv, args(&["echo", "hi"]));
    }

    #[test]
    fn non_utf8_gang_path_parses_its_subcommand() {
        let gang = OsString::from_vec(b"/work/bin/\xff/gang".to_vec());
        let parsed = try_parse(vec![
            gang,
            OsString::from("server"),
            OsString::from("--config"),
            OsString::from("x.toml"),
        ])
        .expect("parses");
        assert!(matches!(parsed.command, Command::Server(_)));
    }

    #[test]
    fn quiet_is_accepted_before_and_after_the_subcommand() {
        for line in [
            &["gang", "-q", "server", "--config", "x.toml"][..],
            &["gang", "--quiet", "server", "--config", "x.toml"],
            &["gang", "server", "--config", "x.toml", "-q"],
            &["gang", "server", "-q", "--config", "x.toml"],
            &["gang", "server", "--config", "x.toml", "--quiet"],
        ] {
            let parsed = parse_str(line);
            assert!(parsed.quiet, "{line:?} must be quiet");
            assert!(matches!(parsed.command, Command::Server(_)));
        }
    }

    #[test]
    fn quiet_is_accepted_on_a_nested_subcommand() {
        let parsed = parse_str(&["gang", "local", "up", "-q"]);
        assert!(parsed.quiet);
        assert!(matches!(
            parsed.command,
            Command::Local(LocalCommand::Up(_))
        ));
        let parsed = parse_str(&["gang", "-q", "local", "down"]);
        assert!(parsed.quiet);
        assert!(matches!(
            parsed.command,
            Command::Local(LocalCommand::Down(_))
        ));
    }

    #[test]
    fn quiet_is_off_by_default() {
        assert!(!parse_str(&["gang", "options"]).quiet);
    }

    #[test]
    fn every_command_has_a_subject_named_after_its_command_line() {
        let cases: [(&[&str], &str); 15] = [
            (
                &["gang", "login", "--server", "https://example.com"],
                "gang login",
            ),
            (&["gang", "get"], "gang get"),
            (
                &["gang", "bazelrc", "--server", "https://example.com"],
                "gang bazelrc",
            ),
            (&["gang", "revoke", "--serial", "ff"], "gang revoke"),
            (
                &["gang", "restore", "--principal", "idp:alice"],
                "gang restore",
            ),
            (&["gang", "server", "--config", "x.toml"], "gang server"),
            (&["gang", "worker", "--config", "x.toml"], "gang worker"),
            (&["gang", "exec", "true"], "gang exec"),
            (&["gang", "local", "up"], "gang local up"),
            (&["gang", "local", "down"], "gang local down"),
            (&["gang", "options"], "gang options"),
            (&["gang", "doctor"], "gang doctor"),
            (&["gang", "deploy", "policy"], "gang deploy policy"),
            (
                &["gang", "deploy", "plan-comment"],
                "gang deploy plan-comment",
            ),
            (
                &["gang", "deploy", "manifest", "--plan-version", "v1"],
                "gang deploy manifest",
            ),
        ];
        for (line, subject) in cases {
            assert_eq!(parse_str(line).command.subject(), subject, "{line:?}");
        }
    }
}
