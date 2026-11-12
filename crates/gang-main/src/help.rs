//! Help screens, the version banner and usage errors: everything clap prints about the command
//! line itself, restyled to the palette of the terminal it prints to.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::builder::Styles;
use clap::builder::styling::Style;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, ColorChoice, Command, CommandFactory};

use crate::cli::Cli;
use crate::errors::MainError;
use crate::output::{Line, Mark, Output, Record};
use crate::palette::{Palette, Role};
use crate::wrap::{columns, wrap_text};

/// The top-level help sections: a heading and the command paths listed under it, in order.
///
/// clap has no subcommand groups, so the root help template is built from this table; every
/// visible command appears exactly once and each row's text is the command's own doc comment.
pub(crate) const GROUPS: &[(&str, &[&str])] = &[
    ("Build", &["login", "bazelrc", "doctor"]),
    ("Local farm", &["local up", "local down"]),
    (
        "Operate",
        &["server", "ca-pin", "options", "revoke", "restore"],
    ),
];

/// Exit code of a usage error; clap reports the same code for every such error.
const USAGE_EXIT_CODE: u8 = 2;
/// Indent of a command row.
const ROW_INDENT: usize = 2;
/// Column where a command row's description starts when no command name is longer: the column of
/// the option descriptions below it (`  -V, --version` plus two spaces).
const DESCRIPTION_COLUMN: usize = 17;
/// Width assumed for a stdout that is not a terminal; the width clap itself assumes.
const DEFAULT_WIDTH: usize = 100;
/// The least width a description is wrapped to, however narrow the terminal.
const MIN_DESCRIPTION_WIDTH: usize = 20;
/// First line of the version banner's tagline.
const TAGLINE_FIRST: &str = "Bazel remote execution on workers";
/// Second line of the version banner's tagline.
const TAGLINE_SECOND: &str = "that exist only while there is work.";
/// The version every `--version` screen prints.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The command with help, color and styles decided by what stdout supports, and its help laid
/// out for the width of stdout's terminal.
pub(crate) fn command(output: &Output) -> Command {
    command_at(output, output.stdout_width().unwrap_or(DEFAULT_WIDTH))
}

/// The command with its help laid out for a terminal `width` columns wide.
///
/// clap moves every description onto its own line as soon as one of them would wrap, which
/// loses the aligned columns of the design. The descriptions are therefore wrapped here, at the
/// column where clap starts them, and clap itself is told the width is unlimited.
pub(crate) fn command_at(output: &Output, width: usize) -> Command {
    let palette = output.stdout_palette();
    let styles = palette.map_or_else(Styles::plain, Palette::clap_styles);
    let mut command = Cli::command()
        .color(if palette.is_some() {
            ColorChoice::Always
        } else {
            ColorChoice::Never
        })
        .styles(styles.clone());
    command.build();
    let mut command = lay_out(command, width);
    if let Some(palette) = palette {
        command = style_examples(command, styles.get_header(), &palette.style(Role::Muted));
        command = style_usage(command, &styles);
    }
    let template = help_template(&command, &styles);
    command.help_template(template)
}

/// Wraps the descriptions of the arguments of `command` and of every command below it for
/// `width` columns and switches clap's own wrapping off.
fn lay_out(command: Command, width: usize) -> Command {
    let shown = |arg: &&Arg| !arg.is_hide_set();
    let positional_column = description_column(command.get_positionals().filter(shown));
    let option_column = description_column(
        command
            .get_arguments()
            .filter(|arg| !arg.is_positional())
            .filter(shown),
    );
    command
        .term_width(0)
        .mut_args(|arg| {
            let column = if arg.is_positional() {
                positional_column
            } else {
                option_column
            };
            wrap_description(arg, width.saturating_sub(column).max(MIN_DESCRIPTION_WIDTH))
        })
        .mut_subcommands(|sub| lay_out(sub, width))
}

/// The column where clap starts the descriptions of `args`, by clap's own rule: the longest
/// argument (four columns wider when it has a long flag) plus four columns of indent and gutter.
fn description_column<'a>(args: impl Iterator<Item = &'a Arg>) -> usize {
    args.map(|arg| {
        let width = columns(&arg.to_string());
        if arg.get_long().is_some() {
            width + 4
        } else {
            width
        }
    })
    .fold(2, usize::max)
        + 4
}

/// `arg` with its description, and the default value clap would append to it, wrapped to `width`
/// characters per line.
fn wrap_description(arg: Arg, width: usize) -> Arg {
    if arg.is_hide_set() {
        return arg;
    }
    let Some(help) = arg.get_help().map(ToString::to_string) else {
        return arg;
    };
    let defaults: Vec<String> = arg
        .get_default_values()
        .iter()
        .map(|value| {
            let value = value.to_string_lossy();
            if value.contains(char::is_whitespace) {
                format!("{value:?}")
            } else {
                value.into_owned()
            }
        })
        .collect();
    let shows_default =
        arg.get_action().takes_values() && !arg.is_hide_default_value_set() && !defaults.is_empty();
    let text = if shows_default {
        format!("{help} [default: {}]", defaults.join(", "))
    } else {
        help
    };
    let wrapped = wrap_text(&text, 0, 0, Some(width)).join("\n");
    if shows_default {
        arg.help(wrapped).hide_default_value(true)
    } else {
        arg.help(wrapped)
    }
}

/// The command at `path` (names separated by spaces) below `root`.
fn find<'a>(root: &'a Command, path: &str) -> Option<&'a Command> {
    path.split(' ')
        .try_fold(root, |command, name| command.find_subcommand(name))
}

/// The root help template: usage, the [`GROUPS`] rows, the options and the examples.
fn help_template(root: &Command, styles: &Styles) -> String {
    let header = styles.get_header();
    let literal = styles.get_literal();
    let column = GROUPS
        .iter()
        .flat_map(|(_, paths)| paths.iter())
        .map(|path| ROW_INDENT + path.len() + 2)
        .max()
        .unwrap_or_default()
        .max(DESCRIPTION_COLUMN);
    let mut template = String::from("{about-with-newline}\n{usage-heading} {usage}\n\n");
    for (heading, paths) in GROUPS {
        let _ = writeln!(template, "{header}{heading}:{header:#}");
        for path in *paths {
            let Some(command) = find(root, path) else {
                continue;
            };
            let about = command
                .get_about()
                .map(ToString::to_string)
                .unwrap_or_default();
            let padding = column - ROW_INDENT - path.len();
            let _ = writeln!(
                template,
                "{space:ROW_INDENT$}{literal}{path}{literal:#}{space:padding$}{about}",
                space = "",
            );
        }
        template.push('\n');
    }
    let _ = write!(
        template,
        "{header}Options:{header:#}\n{{options}}{{after-help}}"
    );
    template
}

/// Applies the palette to the `Examples:` text of `command` and of every command below it: the
/// heading takes the header style and `#` comment lines the muted one.
fn style_examples(command: Command, header: &Style, muted: &Style) -> Command {
    let command = if let Some(text) = command.get_after_help().map(ToString::to_string) {
        command.after_help(style_example_text(&text, header, muted))
    } else {
        command
    };
    command.mut_subcommands(|sub| style_examples(sub, header, muted))
}

/// One `Examples:` block with its heading and comment lines styled.
fn style_example_text(text: &str, header: &Style, muted: &Style) -> String {
    text.lines()
        .map(|line| {
            let body = line.trim_start();
            if line == "Examples:" {
                format!("{header}{line}{header:#}")
            } else if body.starts_with('#') {
                let indent = &line[..line.len() - body.len()];
                format!("{indent}{muted}{body}{muted:#}")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Gives the overridden usage line of `command` and of every command below it the colors clap
/// gives a generated one: the command path in the literal style, the rest as placeholders.
fn style_usage(command: Command, styles: &Styles) -> Command {
    let command = if let Some(usage) = command.get_overridden_usage().map(ToString::to_string) {
        command.override_usage(styled_usage(&usage, styles))
    } else {
        command
    };
    command.mut_subcommands(|sub| style_usage(sub, styles))
}

/// One usage line (without `Usage: `) with its command path and its arguments styled.
fn styled_usage(usage: &str, styles: &Styles) -> String {
    let literal = styles.get_literal();
    let placeholder = styles.get_placeholder();
    let mut split = 0;
    for token in usage.split(' ') {
        if token.starts_with(['-', '[', '<']) {
            break;
        }
        split += token.len() + 1;
    }
    let (name, arguments) = usage.split_at(split.min(usage.len()));
    let name = name.trim_end();
    if arguments.is_empty() {
        format!("{literal}{name}{literal:#}")
    } else {
        format!("{literal}{name}{literal:#} {placeholder}{arguments}{placeholder:#}")
    }
}

/// Reports whether the arguments ask for the long version screen: `--version` without `-V`, and
/// without `--quiet`, under which nothing is decorated.
pub(crate) fn asks_for_banner(args: &[OsString]) -> bool {
    let has = |flag: &str| args.iter().any(|arg| arg == flag);
    has("--version") && !has("-V") && !has("-q") && !has("--quiet")
}

/// The version banner as stdout shows it: the mark beside the version, the tagline and the
/// platform; empty when stdout does not decorate.
fn version_banner(output: &Output) -> String {
    let lines = [
        Line::new().strong(format!("gang {VERSION}")),
        Line::from(TAGLINE_FIRST),
        Line::from(TAGLINE_SECOND),
        Line::new(),
        Line::new().muted(format!(
            "{} {}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
    ];
    output.stdout_decoration(Some(Mark::Full), &lines)
}

/// Reports a clap error: help and version are printed by clap (the long version screen as a
/// banner when `banner` is set and stdout decorates), help for a missing subcommand goes to
/// stderr, every other error becomes a `fail` record on stderr. Returns the exit code: 0 for
/// help and version, 2 for usage errors.
pub(crate) fn report(output: &Output, args: &[OsString], banner: bool) -> ExitCode {
    let error = command(output)
        .try_get_matches_from(args)
        .err()
        .unwrap_or_else(|| {
            clap::Error::raw(
                ErrorKind::InvalidValue,
                "styled command accepted arguments rejected by the parser",
            )
        });
    match error.kind() {
        ErrorKind::DisplayVersion if banner => {
            let text = version_banner(output);
            if text.is_empty() {
                return print_clap(&error);
            }
            match print_banner(&text) {
                Ok(()) => ExitCode::SUCCESS,
                Err(cause) => output.failure("gang", &MainError::Stdout(cause)),
            }
        }
        ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => print_help_to_stderr(output, &error),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => print_clap(&error),
        _ => {
            output.step(&usage_record(&error, args));
            ExitCode::from(USAGE_EXIT_CODE)
        }
    }
}

/// Lets clap print its own help or version screen and maps its exit code.
fn print_clap(error: &clap::Error) -> ExitCode {
    // A failed write to the terminal leaves no channel to report it on, so it is ignored.
    let _ = error.print();
    clap_exit_code(error)
}

/// The exit code of a clap error: the code clap gives it, or the usage code when that does not
/// fit in a byte.
fn clap_exit_code(error: &clap::Error) -> ExitCode {
    ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(USAGE_EXIT_CODE))
}

/// The text of a clap help screen: styled with ANSI escapes only when `ansi`, and always ending
/// with a newline.
fn help_text(ansi: bool, error: &clap::Error) -> String {
    let rendered = error.render();
    let mut text = if ansi {
        rendered.ansi().to_string()
    } else {
        rendered.to_string()
    };
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text
}

/// Prints a help screen that clap sends to stderr, colored only when stderr is.
fn print_help_to_stderr(output: &Output, error: &clap::Error) -> ExitCode {
    let text = help_text(output.stderr_ansi(), error);
    // A failed write to stderr leaves no channel to report it on, so it is ignored.
    let _ = std::io::stderr().write_all(text.as_bytes());
    clap_exit_code(error)
}

/// Writes the banner `text` to `writer`, ending it with exactly one newline, and flushes.
fn write_banner(writer: &mut impl Write, text: &str) -> io::Result<()> {
    writer.write_all(text.as_bytes())?;
    if !text.ends_with('\n') {
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

/// Writes the banner to stdout, ending it with a newline.
fn print_banner(text: &str) -> io::Result<()> {
    write_banner(&mut std::io::stdout().lock(), text)
}

/// The `fail` record of a usage error: `gang <command>: <summary>` with one fix.
fn usage_record(error: &clap::Error, args: &[OsString]) -> Record {
    let subject = command_path(args);
    let fix = fix_for(error, &subject, args);
    Record::fail(summary(error)).subject(subject).fix(fix)
}

/// The one line that tells how to correct a usage error of the command `subject`.
fn fix_for(error: &clap::Error, subject: &str, args: &[OsString]) -> String {
    let see = format!("see {subject} --help");
    let suggestion = match error.kind() {
        ErrorKind::UnknownArgument => context_text(error, ContextKind::SuggestedArg),
        ErrorKind::InvalidSubcommand => context_text(error, ContextKind::SuggestedSubcommand),
        _ => None,
    };
    if let Some(suggestion) = suggestion {
        format!("did you mean {suggestion}? {see}")
    } else if error.kind() == ErrorKind::MissingRequiredArgument {
        match group_alternatives(error) {
            Some(alternatives) => {
                let mut invocation = String::from("gang");
                for argument in args.iter().skip(1) {
                    let argument = argument.to_string_lossy();
                    invocation.push(' ');
                    if argument.chars().any(char::is_whitespace) {
                        invocation.push('\'');
                        invocation.push_str(&argument.replace('\'', "'\\''"));
                        invocation.push('\'');
                    } else {
                        invocation.push_str(&argument);
                    }
                }
                alternatives
                    .iter()
                    .map(|alternative| format!("{invocation} {alternative}"))
                    .collect::<Vec<_>>()
                    .join(", or ")
            }
            None => usage_line(error).unwrap_or(see),
        }
    } else {
        see
    }
}

/// The command a command line addressed: `gang` followed by every subcommand name it contains,
/// each below the previous one.
fn command_path(args: &[OsString]) -> String {
    let root = Cli::command();
    let mut current = &root;
    let mut path = vec![root.get_name()];
    for arg in args.iter().skip(1) {
        let Some(arg) = arg.to_str() else { continue };
        if arg == "--" {
            break;
        }
        if let Some(sub) = current.find_subcommand(arg) {
            path.push(sub.get_name());
            current = sub;
        }
    }
    path.join(" ")
}

/// The alternatives of a required argument group that was not given: clap names the group as
/// `<--a <A>|--b <B>>`.
fn group_alternatives(error: &clap::Error) -> Option<Vec<String>> {
    let Some(ContextValue::Strings(required)) = error.get(ContextKind::InvalidArg) else {
        return None;
    };
    let [group] = required.as_slice() else {
        return None;
    };
    let inner = group.strip_prefix('<')?.strip_suffix('>')?;
    inner
        .contains('|')
        .then(|| inner.split('|').map(str::to_owned).collect())
}

/// The summary of a usage error.
fn summary(error: &clap::Error) -> Line {
    let argument = || context_text(error, ContextKind::InvalidArg);
    let known = match error.kind() {
        ErrorKind::UnknownArgument => {
            argument().map(|argument| Line::from(format!("unexpected argument {argument}")))
        }
        ErrorKind::MissingRequiredArgument => missing_summary(error),
        ErrorKind::InvalidValue | ErrorKind::ValueValidation => argument().map(|argument| {
            let flag = argument.split_whitespace().next().unwrap_or_default();
            Line::from(format!("invalid value for {flag}"))
        }),
        _ => None,
    };
    known.unwrap_or_else(|| Line::from(first_line(error)))
}

/// The summary of a missing required argument or group of alternatives.
fn missing_summary(error: &clap::Error) -> Option<Line> {
    if let Some(alternatives) = group_alternatives(error) {
        return Some(Line::from(format!(
            "name one of {}",
            alternatives.join(" or ")
        )));
    }
    match error.get(ContextKind::InvalidArg) {
        Some(ContextValue::Strings(required)) if !required.is_empty() => {
            let verb = if required.len() == 1 { "is" } else { "are" };
            Some(Line::from(format!(
                "{} {verb} required",
                required.join(", ")
            )))
        }
        _ => None,
    }
}

/// clap's own message: the first line of its rendering without the `error: ` prefix, starting
/// in lower case.
fn first_line(error: &clap::Error) -> String {
    let rendered = error.to_string();
    let line = rendered.lines().next().unwrap_or_default();
    let line = line.strip_prefix("error: ").unwrap_or(line);
    let mut chars = line.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_lowercase().chain(chars).collect()
    })
}

/// The usage line clap attached to an error, without its `Usage: ` title.
fn usage_line(error: &clap::Error) -> Option<String> {
    let usage = context_text(error, ContextKind::Usage)?;
    let line = usage.lines().next()?;
    Some(line.strip_prefix("Usage: ").unwrap_or(line).to_owned())
}

/// One text value of an error's context: the string itself, or the first of a list.
fn context_text(error: &clap::Error, kind: ContextKind) -> Option<String> {
    match error.get(kind)? {
        ContextValue::String(text) => Some(text.clone()),
        ContextValue::Strings(texts) => texts.first().cloned(),
        ContextValue::StyledStr(text) => Some(text.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    /// Parses `items` with the real command and reports the error it must produce, returning
    /// what the process writes to stderr and its exit code.
    fn usage_failure(items: &[&str]) -> (String, ExitCode) {
        let (output, _stdout, stderr) = Output::captured();
        let args = args(items);
        let code = report(&output, &args, false);
        (stderr.text(), code)
    }

    /// The paths of every visible command that has no visible subcommand of its own.
    fn visible_leaves(command: &Command, prefix: &str, leaves: &mut Vec<String>) {
        for sub in command.get_subcommands() {
            if sub.is_hide_set() || sub.get_name() == "help" {
                continue;
            }
            let path = if prefix.is_empty() {
                sub.get_name().to_owned()
            } else {
                format!("{prefix} {}", sub.get_name())
            };
            if sub
                .get_subcommands()
                .any(|child| !child.is_hide_set() && child.get_name() != "help")
            {
                visible_leaves(sub, &path, leaves);
            } else {
                leaves.push(path);
            }
        }
    }

    #[test]
    fn groups_list_every_visible_command_exactly_once() {
        let mut leaves = Vec::new();
        visible_leaves(&Cli::command(), "", &mut leaves);
        let mut listed: Vec<String> = GROUPS
            .iter()
            .flat_map(|(_, paths)| paths.iter().map(ToString::to_string))
            .collect();
        leaves.sort();
        listed.sort();
        assert_eq!(listed, leaves);
    }

    #[test]
    fn groups_list_no_hidden_command() {
        let root = Cli::command();
        for (_, paths) in GROUPS {
            for path in *paths {
                let command = find(&root, path).expect("a listed command exists");
                assert!(!command.is_hide_set(), "{path} is hidden but listed");
            }
        }
    }

    #[test]
    fn get_worker_exec_and_deploy_are_hidden_but_have_help() {
        let (output, _, _) = Output::captured();
        let mut root = command_at(&output, 80);
        for name in ["get", "worker", "exec", "deploy"] {
            let sub = root.find_subcommand_mut(name).expect("the command exists");
            assert!(sub.is_hide_set(), "{name} must be hidden");
            assert!(sub.render_help().to_string().contains("Examples:"));
        }
    }

    #[test]
    fn root_help_has_no_row_for_a_hidden_command() {
        let (output, _, _) = Output::captured();
        let mut root = command_at(&output, 80);
        let help = root.render_help().to_string();
        for name in ["get", "worker", "exec", "deploy"] {
            let listed = help
                .lines()
                .any(|line| line.split_whitespace().next() == Some(name));
            assert!(!listed, "{name} must not be listed in the root help");
        }
    }

    #[test]
    fn help_wraps_descriptions_at_the_requested_width() {
        let (output, _, _) = Output::captured();
        let mut root = command_at(&output, 60);
        let login = root.find_subcommand_mut("login").expect("login exists");
        let help = login.render_help().to_string();
        let options = help
            .lines()
            .skip_while(|line| *line != "Options:")
            .take_while(|line| *line != "Examples:");
        for line in options {
            assert!(columns(line) <= 60, "{line:?} is wider than 60");
        }
    }

    #[test]
    fn every_command_has_an_examples_section_with_a_comment() {
        fn check(command: &Command, path: &str) {
            let after = command
                .get_after_help()
                .map(ToString::to_string)
                .unwrap_or_default();
            assert!(after.starts_with("Examples:\n"), "{path} has no examples");
            assert!(after.contains("  # "), "{path} has no commented example");
            for sub in command.get_subcommands() {
                check(sub, &format!("{path} {}", sub.get_name()));
            }
        }
        check(&Cli::command(), "gang");
    }

    #[test]
    fn styled_examples_mute_comment_lines_only() {
        let (output, _, _) = Output::captured_styled(Palette::Ansi, 80);
        let root = command(&output);
        let after = root
            .get_after_help()
            .expect("root has examples")
            .ansi()
            .to_string();
        let muted = Palette::Ansi.style(Role::Muted);
        let comment = format!("{muted}# a laptop: log in, then point Bazel at the farm{muted:#}");
        assert!(after.contains(&comment));
        assert!(after.contains("  gang login --server https://gang.example.com\n"));
    }

    #[test]
    fn styled_usage_splits_the_command_path_from_its_arguments() {
        let styles = Palette::Ansi.clap_styles();
        let literal = styles.get_literal();
        let placeholder = styles.get_placeholder();
        assert_eq!(
            styled_usage("gang login --server <URL> [OPTIONS]", &styles),
            format!(
                "{literal}gang login{literal:#} {placeholder}--server <URL> [OPTIONS]{placeholder:#}"
            )
        );
    }

    /// Reports whether `code` is the exit code of a usage error.
    fn is_usage_exit(code: ExitCode) -> bool {
        format!("{code:?}") == format!("{:?}", ExitCode::from(USAGE_EXIT_CODE))
    }

    #[test]
    fn unknown_argument_suggests_the_closest_flag() {
        let (stderr, code) =
            usage_failure(&["gang", "login", "--sever", "https://gang.example.com"]);
        assert_eq!(
            stderr,
            "  fail  gang login: unexpected argument --sever\n   fix  did you mean --server? see gang login --help\n"
        );
        assert!(is_usage_exit(code));
    }

    #[test]
    fn missing_required_argument_fixes_with_the_usage_line() {
        let (stderr, code) = usage_failure(&["gang", "bazelrc"]);
        assert_eq!(
            stderr,
            "  fail  gang bazelrc: --server <URL> is required\n   fix  gang bazelrc --server <URL>\n"
        );
        assert!(is_usage_exit(code));
    }

    #[test]
    fn missing_required_group_lists_its_alternatives_in_the_fix() {
        let (stderr, code) = usage_failure(&["gang", "revoke"]);
        assert_eq!(
            stderr,
            "  fail  gang revoke: name one of --serial <HEX> or --principal <PROVIDER:PRINCIPAL>\n   fix  gang revoke --serial <HEX>, or gang revoke --principal <PROVIDER:PRINCIPAL>\n"
        );
        assert!(is_usage_exit(code));
    }

    #[test]
    fn a_missing_proof_fix_keeps_the_given_arguments() {
        let (stderr, code) = usage_failure(&[
            "gang",
            "login",
            "--server",
            "https://gang.example.com",
            "--workload",
            "--provider",
            "example",
        ]);
        let fix = stderr
            .lines()
            .find(|line| line.contains("fix"))
            .expect("fix");
        assert!(fix.contains("--server"), "{fix}");
        assert!(fix.contains("--provider"), "{fix}");
        assert!(is_usage_exit(code));
    }

    #[test]
    fn missing_subcommand_help_is_clap_help_for_stderr() {
        let (output, _, _) = Output::captured();
        for line in [&["gang"][..], &["gang", "local"]] {
            let error = command(&output)
                .try_get_matches_from(line)
                .expect_err("a subcommand is required");
            assert_eq!(
                error.kind(),
                ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            );
            assert!(!error.render().to_string().contains('\u{1b}'));
        }
    }

    #[test]
    fn invalid_value_names_the_flag_without_its_placeholder() {
        let (stderr, _) = usage_failure(&["gang", "local", "up", "--poll-interval", "0s"]);
        assert_eq!(
            stderr,
            "  fail  gang local up: invalid value for --poll-interval\n   fix  see gang local up --help\n"
        );
    }

    #[test]
    fn unknown_subcommand_uses_clap_message_in_lower_case() {
        let (stderr, _) = usage_failure(&["gang", "frobnicate"]);
        assert_eq!(
            stderr,
            "  fail  gang: unrecognized subcommand 'frobnicate'\n   fix  see gang --help\n"
        );
    }

    #[test]
    fn help_and_version_are_not_usage_errors() {
        let (output, _stdout, stderr) = Output::captured();
        for flag in ["--help", "-h", "--version", "-V"] {
            let error = command(&output)
                .try_get_matches_from(["gang", flag])
                .expect_err("clap stops at help and version");
            assert!(matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ));
        }
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn banner_is_asked_for_by_the_long_flag_alone() {
        assert!(asks_for_banner(&args(&["gang", "--version"])));
        assert!(!asks_for_banner(&args(&["gang", "-V"])));
        assert!(!asks_for_banner(&args(&["gang", "-V", "--version"])));
        assert!(!asks_for_banner(&args(&["gang", "login"])));
        assert!(!asks_for_banner(&args(&["gang", "-q", "--version"])));
        assert!(!asks_for_banner(&args(&["gang", "--version", "--quiet"])));
    }

    #[test]
    fn banner_is_empty_when_stdout_does_not_decorate() {
        let (output, _, _) = Output::captured();
        assert_eq!(version_banner(&output), "");
    }

    #[test]
    fn banner_carries_version_tagline_and_platform_when_stdout_decorates() {
        let (output, _, _) = Output::captured_styled(Palette::Ansi, 80);
        let banner = version_banner(&output);
        for part in [
            format!("gang {VERSION}"),
            TAGLINE_FIRST.to_owned(),
            TAGLINE_SECOND.to_owned(),
            format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
        ] {
            assert!(banner.contains(&part), "banner lacks {part:?}: {banner:?}");
        }
    }

    #[test]
    fn a_banner_gets_exactly_one_trailing_newline() {
        for text in ["gang 1.0", "gang 1.0\n"] {
            let mut written = Vec::new();
            write_banner(&mut written, text).expect("writing to a vector succeeds");
            assert_eq!(written, b"gang 1.0\n");
        }
    }

    #[test]
    fn help_text_is_plain_without_ansi_and_ends_with_a_newline() {
        let (output, _, _) = Output::captured();
        let error = command(&output)
            .try_get_matches_from(["gang", "--help"])
            .expect_err("clap stops at help");
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        let text = help_text(false, &error);
        assert!(!text.is_empty());
        assert!(!text.contains('\u{1b}'));
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn a_help_request_exits_zero_and_a_usage_error_exits_two() {
        let (output, _, _) = Output::captured();
        let help = command(&output)
            .try_get_matches_from(["gang", "--help"])
            .expect_err("clap stops at help");
        assert_eq!(clap_exit_code(&help), ExitCode::SUCCESS);
        let unknown = command(&output)
            .try_get_matches_from(["gang", "frobnicate"])
            .expect_err("an unknown subcommand is rejected");
        assert_eq!(clap_exit_code(&unknown), ExitCode::from(USAGE_EXIT_CODE));
    }
}
