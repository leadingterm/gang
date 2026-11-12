//! The cli scenario: the `gang` command line as an operator meets it, with no harness stack. Exit
//! codes and failure records, `--quiet` and the colour switches, `gang local up` / `down` with
//! and without TLS, `gang doctor` against a fake Bazel, and the endpoint flags of `gang worker`.

use std::borrow::Cow;
use std::path::Path;

use gang_types::announcement::{Announcement, last_ca_pin};
use gang_types::{CaPin, Url};

use crate::check::Check;
use crate::errors::Result;
use crate::names;

use crate::Ran;

/// The colour environment of a command. Every command runs with `NO_COLOR`, `CLICOLOR`,
/// `CLICOLOR_FORCE` and `TERM` removed; these variants then set some back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Colour {
    /// None of them set.
    Plain,
    /// `CLICOLOR_FORCE=1`.
    Forced,
    /// `CLICOLOR_FORCE=1` and `NO_COLOR=1`.
    ForcedNoColor,
}

/// The two local deployments of the cli scenario, both `configs/gang.local.toml` moved into the
/// scenario's placement and sharing its state directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Local {
    /// The configuration as shipped (no fleet minimum); `gang local up` serves TLS.
    Pinned,
    /// The same with its first fleet keeping one worker (`min_instances = 1`), started with
    /// `gang local up --insecure`.
    Insecure,
}

/// A configuration `gang server` must refuse before it announces anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// A file that is not a `gang.toml`.
    Unparsable,
    /// The pinned configuration with `listen = "0.0.0.0:0"` and no `endpoint`.
    PublicListenWithoutEndpoint,
}

/// One `gang` command line of the cli scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    /// `gang revoke`, naming no target.
    RevokeWithoutTarget,
    /// `gang bogus`, an unknown subcommand.
    UnknownSubcommand,
    /// `gang doctor` with no server and no stored login; `gang -q doctor` when `quiet`.
    DoctorWithoutServer {
        /// Whether `-q` is given.
        quiet: bool,
    },
    /// `gang server --config <the refused configuration>`.
    Server(Refused),
    /// `gang local up --config <local>`, with `--insecure` for [`Local::Insecure`].
    Up(Local),
    /// `gang local down --config <local>`, `gang -q local down ...` when `quiet`, in `colour`.
    Down {
        /// The deployment whose configuration is given.
        local: Local,
        /// Whether `-q` is given.
        quiet: bool,
        /// The colour environment.
        colour: Colour,
    },
    /// `gang bazelrc --server <server>`, with no pin.
    Bazelrc {
        /// The server.
        server: Url,
    },
    /// `gang doctor --server <server> --bazel <fake Bazel> -- --remote_executor=<executor>`.
    Doctor {
        /// The server.
        server: Url,
        /// The value the fake Bazel reports for `--remote_executor`.
        executor: String,
    },
    /// `gang worker --config <the pinned configuration>`, with each endpoint flag given verbatim
    /// when `Some`.
    Worker {
        /// The value of `--endpoint`.
        endpoint: Option<String>,
        /// The value of `--worker-endpoint`.
        worker_endpoint: Option<String>,
    },
}

/// The `gang` command line as the cli scenario drives and observes it.
pub trait Terminal {
    /// Runs `call` to completion within the harness deadline.
    ///
    /// # Errors
    ///
    /// When the command cannot be started, or [`Timeout`](crate::errors::E2eError::Timeout)
    /// once its deadline passed.
    fn run(&self, call: &Call) -> Result<Ran>;

    /// The text of the server log at `path`, the `log` field `gang local up` printed.
    ///
    /// # Errors
    ///
    /// When the file cannot be read.
    fn server_log(&self, path: &Path) -> Result<String>;

    /// Whether the plaintext server at `public` reports a connected worker on its state stream
    /// before the harness deadline; `false` once the deadline passed.
    ///
    /// # Errors
    ///
    /// When the stream cannot be opened or followed.
    fn worker_connects(&self, public: &Url) -> Result<bool>;

    /// The last bytes of what this scenario's Local Mode workers logged, as many as the
    /// settings' log tail allows; empty when none logged.
    ///
    /// # Errors
    ///
    /// When a worker log exists and cannot be read.
    fn worker_log_tail(&self) -> Result<String>;

    /// The argument lists, program first, of the running `gang worker` processes of this
    /// scenario's deployments.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read.
    fn worker_commands(&self) -> Result<Vec<Vec<String>>>;
}

/// The error `gang server` prints for a public listen address without an endpoint.
const PUBLIC_LISTEN_ERROR: &str =
    "endpoint is required when listen `0.0.0.0:0` is not a loopback address";

/// The error `gang worker` prints under mTLS without an endpoint.
const WORKER_ENDPOINT_MISSING: &str =
    "endpoint is required to enroll the worker when worker authentication is mtls";

/// The fragment of the error `gang local up` prints when `server.lock` is already held.
const LOCK_HELD: &str = "is held by a running gang server";

/// The record `gang local down` prints when no server runs.
const NOT_RUNNING: &str = "no gang server was running";

/// The record `gang local down` prints after stopping the server.
const STOPPED: &str = "stopped the gang server";

/// The record `gang local down` prints when it terminated no workers.
const NO_WORKERS: &str = "terminated 0 workers";

/// The record `gang local down` prints when it terminated one worker.
const ONE_WORKER: &str = "terminated 1 worker";

/// An executor no server in this scenario has.
const MISMATCHED_EXECUTOR: &str = "grpc://127.0.0.1:1";

/// A value that is not a URL.
const NOT_A_URL: &str = "not a url";

/// Removes CSI escapes from output, borrowing the input when it contains none.
#[must_use]
pub fn plain(text: &str) -> Cow<'_, str> {
    if !text.contains("\x1b[") {
        return Cow::Borrowed(text);
    }
    let mut plain = String::with_capacity(text.len());
    let mut rest = text;
    while let Some((before, after)) = rest.split_once("\x1b[") {
        plain.push_str(before);
        let end = after
            .char_indices()
            .find(|(_, c)| ('\x40'..='\x7e').contains(c))
            .map_or(after.len(), |(index, c)| index + c.len_utf8());
        rest = after.get(end..).unwrap_or_default();
    }
    plain.push_str(rest);
    Cow::Owned(plain)
}

/// Whether `text` contains a colour escape.
pub(crate) fn coloured(text: &str) -> bool {
    text.contains('\x1b')
}

/// The status printed in a record's margin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowStatus {
    /// A successful check or operation.
    Ok,
    /// A warning.
    Warn,
    /// A failed check or operation.
    Fail,
    /// A check or operation that was skipped.
    Skip,
}

/// A status record and its associated fix, borrowed from plain output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusRow<'a> {
    /// The record's status.
    pub status: RowStatus,
    /// The check or command name, or an empty string for an unnamed record.
    pub subject: &'a str,
    /// The record's summary.
    pub detail: &'a str,
    /// The first associated fix, if any.
    pub fix: Option<&'a str>,
}

/// Parses status records from output already stripped of ANSI escapes with [`plain`].
///
/// Command records separate their subject with a colon; list records separate it from their
/// summary with at least two spaces or a tab. An unnamed record has an empty subject.
#[must_use]
pub fn status_rows(text: &str) -> Vec<StatusRow<'_>> {
    let mut rows: Vec<StatusRow<'_>> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        let Some((word, rest)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let rest = rest.trim();
        if word == "fix" {
            if let Some(row) = rows.last_mut()
                && row.fix.is_none()
            {
                row.fix = Some(rest);
            }
            continue;
        }
        let status = match word {
            "ok" => RowStatus::Ok,
            "warn" => RowStatus::Warn,
            "fail" => RowStatus::Fail,
            "skip" => RowStatus::Skip,
            _ => continue,
        };
        if rest.is_empty() {
            continue;
        }
        let separator = rest.find("  ").or_else(|| rest.find('\t'));
        let colon = rest.find(": ");
        let (subject, detail) = match (separator, colon) {
            (_, Some(index)) if separator.is_none_or(|gap| index < gap) => {
                (&rest[..index], rest[index + 2..].trim())
            }
            (Some(index), _) => (&rest[..index], rest[index..].trim()),
            _ => ("", rest),
        };
        rows.push(StatusRow {
            status,
            subject: subject.trim(),
            detail,
            fix: None,
        });
    }
    rows
}

/// The text of the successful records in `text`.
pub(crate) fn ok_records(text: &str) -> Vec<String> {
    let text = plain(text);
    status_rows(&text)
        .into_iter()
        .filter(|row| row.status == RowStatus::Ok)
        .map(|row| {
            if row.subject.is_empty() {
                row.detail.to_owned()
            } else {
                format!("{}  {}", row.subject, row.detail)
            }
        })
        .collect()
}

/// The `key  value` lines of `text`, in order, split at the first two blanks.
pub(crate) fn fields(text: &str) -> Vec<(String, String)> {
    plain(text)
        .lines()
        .filter_map(|line| {
            let (key, value) = line.trim_start().split_once("  ")?;
            if key.is_empty() || key.chars().any(char::is_whitespace) {
                return None;
            }
            Some((key.to_owned(), value.trim().to_owned()))
        })
        .collect()
}

/// The value of the first field named `key` in `text`.
pub(crate) fn field(text: &str, key: &str) -> Option<String> {
    fields(text)
        .into_iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}

/// Whether the field `key` of `text` parses as a URL with the scheme `scheme`.
fn scheme_is(text: &str, key: &str, scheme: &str) -> bool {
    field(text, key)
        .and_then(|value| Url::parse(&value).ok())
        .is_some_and(|url| url.scheme() == scheme)
}

/// The rest of the first `fail  subject: ...` line of `text`, after the colon.
pub(crate) fn failure(text: &str, subject: &str) -> Option<String> {
    let text = plain(text);
    status_rows(&text)
        .into_iter()
        .find(|row| row.status == RowStatus::Fail && row.subject == subject)
        .map(|row| row.detail.to_owned())
}

/// Whether `text` has a `fail  subject` row, where `subject` ends at a blank or at the end of
/// the line.
pub(crate) fn fails_row(text: &str, subject: &str) -> bool {
    let text = plain(text);
    status_rows(&text).iter().any(|row| {
        row.status == RowStatus::Fail
            && (row.subject == subject || (row.subject.is_empty() && row.detail == subject))
    })
}

/// Whether some line of `text` is one of the server's startup announcements.
pub(crate) fn announced(text: &str) -> bool {
    text.lines()
        .any(|line| line.trim().parse::<Announcement>().is_ok())
}

/// The value of the last `--remote_executor=` word in `rc`.
pub(crate) fn executor(rc: &str) -> Option<String> {
    rc.lines()
        .flat_map(str::split_whitespace)
        .filter_map(|word| word.strip_prefix("--remote_executor="))
        .next_back()
        .map(str::to_owned)
}

/// The argument after the first `flag`, or the text after `flag=` in an argument of that form.
pub(crate) fn launched_with<'a>(command: &'a [String], flag: &str) -> Option<&'a str> {
    let prefix = format!("{flag}=");
    command.iter().enumerate().find_map(|(index, arg)| {
        if arg.as_str() == flag {
            command.get(index + 1).map(String::as_str)
        } else {
            arg.strip_prefix(prefix.as_str())
        }
    })
}

/// Whether both texts parse as URLs and are equal once trailing slashes are removed.
pub(crate) fn same_url(left: &str, right: &str) -> bool {
    match (Url::parse(left), Url::parse(right)) {
        (Ok(left), Ok(right)) => {
            left.as_str().trim_end_matches('/') == right.as_str().trim_end_matches('/')
        }
        _ => false,
    }
}

/// The exit code, stdout and stderr of `ran`, for a failed check's reason.
pub(crate) fn outcome(ran: &Ran) -> String {
    format!(
        "exit {:?}; stdout: {:?}; stderr: {:?}",
        ran.code,
        ran.stdout.trim(),
        ran.stderr.trim()
    )
}

/// Runs the cli scenario against `terminal` and returns its verdicts in the order reached.
///
/// # Errors
///
/// When the command line cannot be driven or observed; a binary that misbehaves is a failed
/// [`Check`].
pub fn cli<T: Terminal>(terminal: &T) -> Result<Vec<Check>> {
    let mut checks = Vec::new();
    refusals(terminal, &mut checks)?;
    switches(terminal, &mut checks)?;
    pinned(terminal, &mut checks)?;
    insecure(terminal, &mut checks)?;
    Ok(checks)
}

/// Usage errors and refused configurations, which run before any server exists.
fn refusals<T: Terminal>(terminal: &T, checks: &mut Vec<Check>) -> Result<()> {
    let revoke = terminal.run(&Call::RevokeWithoutTarget)?;
    checks.push(Check::expect(
        names::cli::REVOKE_WITHOUT_TARGET,
        revoke.code == Some(2),
        || outcome(&revoke),
    ));

    let bogus = terminal.run(&Call::UnknownSubcommand)?;
    checks.push(Check::expect(
        names::cli::UNKNOWN_SUBCOMMAND,
        bogus.code == Some(2),
        || outcome(&bogus),
    ));

    let doctor = terminal.run(&Call::DoctorWithoutServer { quiet: false })?;
    checks.push(Check::expect(
        names::cli::DOCTOR_WITHOUT_SERVER,
        doctor.code == Some(1) && failure(&doctor.stderr, "gang doctor").is_some(),
        || outcome(&doctor),
    ));

    let bad = terminal.run(&Call::Server(Refused::Unparsable))?;
    checks.push(Check::expect(
        names::cli::UNPARSABLE_CONFIG,
        bad.code == Some(1)
            && failure(&bad.stderr, "gang server")
                .is_some_and(|s| s.starts_with("invalid configuration"))
            && !announced(&bad.stdout),
        || outcome(&bad),
    ));

    let open = terminal.run(&Call::Server(Refused::PublicListenWithoutEndpoint))?;
    checks.push(Check::expect(
        names::cli::PUBLIC_LISTEN_NEEDS_ENDPOINT,
        open.code == Some(1)
            && open.stderr.contains(PUBLIC_LISTEN_ERROR)
            && !announced(&open.stdout),
        || outcome(&open),
    ));
    Ok(())
}

/// The `--quiet` and colour switches of `gang local down` and `gang doctor`, which run before
/// any server exists.
fn switches<T: Terminal>(terminal: &T, checks: &mut Vec<Check>) -> Result<()> {
    let quiet = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: true,
        colour: Colour::Plain,
    })?;
    checks.push(Check::expect(
        names::cli::QUIET_DROPS_OK,
        quiet.code == Some(0) && quiet.combined().trim().is_empty(),
        || outcome(&quiet),
    ));

    let quiet_doctor = terminal.run(&Call::DoctorWithoutServer { quiet: true })?;
    checks.push(Check::expect(
        names::cli::QUIET_KEEPS_FAILURE,
        quiet_doctor.code == Some(1) && failure(&quiet_doctor.stderr, "gang doctor").is_some(),
        || outcome(&quiet_doctor),
    ));

    let piped = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: false,
        colour: Colour::Plain,
    })?;
    checks.push(Check::expect(
        names::cli::PIPED_PLAIN,
        !coloured(&piped.combined())
            && ok_records(&piped.stdout).first().map(String::as_str) == Some(NOT_RUNNING),
        || outcome(&piped),
    ));

    let forced = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: false,
        colour: Colour::Forced,
    })?;
    checks.push(Check::expect(
        names::cli::CLICOLOR_FORCE_COLOURS,
        coloured(&forced.stdout)
            && ok_records(&forced.stdout).first().map(String::as_str) == Some(NOT_RUNNING),
        || outcome(&forced),
    ));

    let no_color = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: false,
        colour: Colour::ForcedNoColor,
    })?;
    checks.push(Check::expect(
        names::cli::NO_COLOR_WINS,
        !coloured(&no_color.combined())
            && ok_records(&no_color.stdout).first().map(String::as_str) == Some(NOT_RUNNING),
        || outcome(&no_color),
    ));
    Ok(())
}

/// `gang local up` and `down` of the pinned deployment, and the worker endpoint flags.
fn pinned<T: Terminal>(terminal: &T, checks: &mut Vec<Check>) -> Result<()> {
    let up = terminal.run(&Call::Up(Local::Pinned))?;
    let pin = field(&up.stdout, "ca-pin").and_then(|text| text.parse::<CaPin>().ok());
    let log = field(&up.stdout, "log");
    checks.push(Check::expect(
        names::cli::UP_REPORTS,
        up.code == Some(0)
            && scheme_is(&up.stdout, "public", "https")
            && scheme_is(&up.stdout, "worker", "grpcs")
            && pin.is_some()
            && log.as_deref().is_some_and(|log| !log.is_empty())
            && field(&up.stdout, "ui").is_none(),
        || outcome(&up),
    ));
    match (log, pin) {
        (Some(log), Some(pin)) => {
            let logged = last_ca_pin(&terminal.server_log(Path::new(&log))?);
            checks.push(Check::expect(
                names::cli::UP_PIN_ANNOUNCED,
                logged == Some(pin),
                || format!("the server log announced {logged:?}; gang local up printed {pin:?}"),
            ));
        }
        _ => checks.push(Check::failed(
            names::cli::UP_PIN_ANNOUNCED,
            format!(
                "gang local up printed no ca-pin or no log: {}",
                outcome(&up)
            ),
        )),
    }

    let worker = terminal.run(&Call::Worker {
        endpoint: None,
        worker_endpoint: None,
    })?;
    checks.push(Check::expect(
        names::cli::WORKER_NEEDS_ENDPOINT,
        worker.code == Some(1)
            && failure(&worker.stderr, "gang worker")
                .is_some_and(|s| s.contains(WORKER_ENDPOINT_MISSING)),
        || outcome(&worker),
    ));

    let bad_endpoint = terminal.run(&Call::Worker {
        endpoint: Some(NOT_A_URL.to_owned()),
        worker_endpoint: None,
    })?;
    checks.push(Check::expect(
        names::cli::ENDPOINT_FLAG_VALIDATED,
        bad_endpoint.code == Some(2) && bad_endpoint.stderr.contains("--endpoint"),
        || outcome(&bad_endpoint),
    ));

    let bad_worker = terminal.run(&Call::Worker {
        endpoint: None,
        worker_endpoint: Some(NOT_A_URL.to_owned()),
    })?;
    checks.push(Check::expect(
        names::cli::WORKER_ENDPOINT_FLAG_VALIDATED,
        bad_worker.code == Some(2) && bad_worker.stderr.contains("--worker-endpoint"),
        || outcome(&bad_worker),
    ));

    let again = terminal.run(&Call::Up(Local::Pinned))?;
    checks.push(Check::expect(
        names::cli::UP_TWICE_REFUSED,
        again.code == Some(1)
            && failure(&again.stderr, "gang local up")
                .is_some_and(|s| s.contains("server.lock") && s.contains(LOCK_HELD)),
        || outcome(&again),
    ));

    let down = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: false,
        colour: Colour::Plain,
    })?;
    let records = ok_records(&down.stdout);
    checks.push(Check::expect(
        names::cli::DOWN_STOPS,
        down.code == Some(0)
            && matches!(records.as_slice(), [first, second]
                if first.starts_with(STOPPED) && second.as_str() == NO_WORKERS),
        || outcome(&down),
    ));

    let down_again = terminal.run(&Call::Down {
        local: Local::Pinned,
        quiet: false,
        colour: Colour::Plain,
    })?;
    checks.push(Check::expect(
        names::cli::DOWN_AGAIN,
        down_again.code == Some(0) && ok_records(&down_again.stdout) == [NOT_RUNNING, NO_WORKERS],
        || outcome(&down_again),
    ));
    Ok(())
}

/// `gang local up --insecure` and its server: the bazelrc, the worker and the doctor checks,
/// then `gang local down` of the same deployment.
fn insecure<T: Terminal>(terminal: &T, checks: &mut Vec<Check>) -> Result<()> {
    let up = terminal.run(&Call::Up(Local::Insecure))?;
    let public = field(&up.stdout, "public");
    let worker_field = field(&up.stdout, "worker");
    let expected_ui = public
        .as_ref()
        .map(|public| format!("{}/ui/", public.trim_end_matches('/')));
    checks.push(Check::expect(
        names::cli::INSECURE_UP_REPORTS,
        up.code == Some(0)
            && scheme_is(&up.stdout, "public", "http")
            && scheme_is(&up.stdout, "worker", "grpc")
            && expected_ui.is_some()
            && field(&up.stdout, "ui") == expected_ui
            && field(&up.stdout, "ca-pin").is_none(),
        || outcome(&up),
    ));

    let endpoints = public
        .as_deref()
        .and_then(|public| Url::parse(public).ok().map(|server| (public, server)));
    match endpoints {
        Some((public, server)) => {
            served(terminal, checks, &server, public, worker_field.as_deref())?;
        }
        None => {
            for name in [
                names::cli::INSECURE_BAZELRC_UNPINNED,
                names::cli::WORKER_CONNECTS,
                names::cli::WORKER_FLAGS,
                names::cli::DOCTOR_MISMATCH_FAILS,
                names::cli::DOCTOR_MATCH_PASSES,
            ] {
                checks.push(Check::failed(
                    name,
                    format!(
                        "gang local up --insecure printed no public endpoint: {}",
                        outcome(&up)
                    ),
                ));
            }
        }
    }

    let stop = terminal.run(&Call::Down {
        local: Local::Insecure,
        quiet: false,
        colour: Colour::Plain,
    })?;
    let records = ok_records(&stop.stdout);
    checks.push(Check::expect(
        names::cli::INSECURE_DOWN_TERMINATES,
        stop.code == Some(0)
            && matches!(records.as_slice(), [first, second]
                if first.starts_with(STOPPED) && second.as_str() == ONE_WORKER),
        || outcome(&stop),
    ));
    Ok(())
}

/// The checks of an `--insecure` server that announced `public` and `worker`, reached at
/// `server`.
fn served<T: Terminal>(
    terminal: &T,
    checks: &mut Vec<Check>,
    server: &Url,
    public: &str,
    worker: Option<&str>,
) -> Result<()> {
    let connected = terminal.worker_connects(server)?;
    let log_tail = if connected {
        String::new()
    } else {
        terminal.worker_log_tail()?
    };
    checks.push(Check::expect(
        names::cli::WORKER_CONNECTS,
        connected,
        || format!("no worker connected before the deadline; worker log tail: {log_tail:?}"),
    ));

    let commands = terminal.worker_commands()?;
    let flags = worker.is_some_and(|worker| {
        !commands.is_empty()
            && commands.iter().all(|command| {
                launched_with(command, "--endpoint").is_some_and(|value| same_url(value, public))
                    && launched_with(command, "--worker-endpoint")
                        .is_some_and(|value| same_url(value, worker))
            })
    });
    checks.push(Check::expect(names::cli::WORKER_FLAGS, flags, || {
        format!("worker commands {commands:?}; reported public {public:?}, worker {worker:?}")
    }));

    let rc = terminal.run(&Call::Bazelrc {
        server: server.clone(),
    })?;
    let named = executor(&rc.stdout);
    checks.push(Check::expect(
        names::cli::INSECURE_BAZELRC_UNPINNED,
        rc.code == Some(0)
            && named.is_some()
            && !rc.combined().contains("tls_certificate")
            && !rc.combined().contains("gang-ca.pem"),
        || outcome(&rc),
    ));

    let mismatch = terminal.run(&Call::Doctor {
        server: server.clone(),
        executor: MISMATCHED_EXECUTOR.to_owned(),
    })?;
    checks.push(Check::expect(
        names::cli::DOCTOR_MISMATCH_FAILS,
        mismatch.code == Some(1) && fails_row(&mismatch.combined(), "remote_executor"),
        || outcome(&mismatch),
    ));

    match named {
        Some(executor) => {
            let matched = terminal.run(&Call::Doctor {
                server: server.clone(),
                executor,
            })?;
            checks.push(Check::expect(
                names::cli::DOCTOR_MATCH_PASSES,
                matched.code == Some(0),
                || outcome(&matched),
            ));
        }
        None => checks.push(Check::failed(
            names::cli::DOCTOR_MATCH_PASSES,
            format!("the bazelrc named no remote_executor: {}", outcome(&rc)),
        )),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_removes_colour_escapes() {
        assert_eq!(plain("\x1b[32mok\x1b[0m  x"), "ok  x");
        assert!(coloured("\x1b[32mok"));
        assert!(!coloured(&plain("\x1b[32mok")));
    }

    #[test]
    fn status_rows_strip_colour_and_attach_only_the_current_fix() {
        let output = plain(
            "\x1b[31mfail\x1b[0m  gang get: refused\n\
             \tfix  log in\n\
             warn  credentials  expiring\n\
             fail  credentials_extra  revoked\n\
             \tfix  restore access\n",
        );
        let rows = status_rows(&output);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].subject, "gang get");
        assert_eq!(rows[0].detail, "refused");
        assert_eq!(rows[0].fix, Some("log in"));
        assert_eq!(rows[1].status, RowStatus::Warn);
        assert_eq!(rows[1].fix, None);
        assert_eq!(rows[2].subject, "credentials_extra");
        assert!(!fails_row(&output, "credentials"));
        assert!(fails_row("fail  credentials", "credentials"));
    }

    #[test]
    fn ok_records_take_the_text_after_blanks() {
        assert_eq!(
            ok_records("    ok  terminated 0 workers\nok\nfail  x\nok  no gang server was running"),
            vec!["terminated 0 workers", "no gang server was running"]
        );
    }

    #[test]
    fn fields_split_at_two_blanks() {
        assert_eq!(
            field("public  https://127.0.0.1:4\n", "public").as_deref(),
            Some("https://127.0.0.1:4")
        );
        assert_eq!(field("x y\n", "x"), None);
    }

    #[test]
    fn failure_matches_subject_and_colon() {
        assert_eq!(
            failure(
                "  fail  gang doctor: no server given and no stored login",
                "gang doctor"
            )
            .as_deref(),
            Some("no server given and no stored login")
        );
        assert_eq!(failure("  fail  gang doctorx: y", "gang doctor"), None);
    }

    #[test]
    fn fails_row_matches_a_doctor_row() {
        assert!(fails_row(
            "  fail  remote_executor  grpc://x",
            "remote_executor"
        ));
        assert!(!fails_row(
            "  fail  remote_executor_x  y",
            "remote_executor"
        ));
    }

    #[test]
    fn executor_is_the_last_flag() {
        assert_eq!(
            executor("build --remote_executor=grpc://a\nbuild --remote_executor=grpc://b")
                .as_deref(),
            Some("grpc://b")
        );
        assert_eq!(executor("build"), None);
    }

    #[test]
    fn launched_with_reads_both_forms() {
        let spaced = vec![
            "gang".to_owned(),
            "--endpoint".to_owned(),
            "https://a".to_owned(),
        ];
        let joined = vec!["gang".to_owned(), "--worker-endpoint=grpc://b".to_owned()];
        assert_eq!(launched_with(&spaced, "--endpoint"), Some("https://a"));
        assert_eq!(
            launched_with(&joined, "--worker-endpoint"),
            Some("grpc://b")
        );
        assert_eq!(launched_with(&joined, "--endpoint"), None);
    }

    #[test]
    fn same_url_ignores_a_trailing_slash() {
        assert!(same_url("https://a/", "https://a"));
        assert!(!same_url("https://a", "https://b"));
        assert!(!same_url("not a url", "not a url"));
    }

    #[test]
    fn a_plain_line_is_no_announcement() {
        assert!(!announced("  fail  gang doctor: x"));
    }
}
