//! The `gang doctor` command: diagnoses this machine's Bazel and login setup against a Gang server
//! and reports one record per finding.

use std::process::ExitCode;
use std::sync::Arc;

use gang_client::{
    BazelCommand, Check, ClientHome, ClientProbe, Doctor, Event, Finding, Report, Status,
    StoredLogin, stored_login,
};
use gang_clock::SystemClock;
use gang_tls::ServerTrust;
use gang_types::{CaPin, Url};

use crate::cli::DoctorArgs;
use crate::errors::{MainError, Result};
use crate::output::{Line, Mark, Output, Progress, Record};
use crate::palette::Role;
use crate::record::COLUMN;
use crate::runtime;
use crate::wrap::columns;

/// Runs `gang doctor` and returns success only when the report is healthy.
pub(crate) fn run(args: DoctorArgs, out: &Output) -> Result<ExitCode> {
    let home = ClientHome::resolve(|key| std::env::var_os(key))?;
    let stored = stored_login(&home)?;
    let (server, ca_pin) = resolve_target(args.server, args.ca_pin, stored)?;
    let host = host_name(&server);
    let options = home.options()?;
    let clock = Arc::new(SystemClock::new());
    let probe = ClientProbe::try_new(&server, ca_pin.as_ref(), &options, clock.clone())?;
    let bazel = BazelCommand::new(args.bazel, args.bazel_args, &options, clock.clone());
    let mut stream = Stream::new(out);
    let report = runtime::block_on(async {
        let shutdown = gang_os::shutdown_signal().map_err(MainError::Signals)?;
        let doctor = Doctor::new(
            &bazel,
            &probe,
            &*clock,
            &server,
            ServerTrust::from(ca_pin.as_ref()),
            &options,
        );
        let mut observe = |event: Event<'_>| stream.observe(event);
        let diagnosis = doctor.diagnose_with(&mut observe);
        Ok::<_, MainError>(diagnose_until(diagnosis, shutdown).await)
    })?;
    stream.finish()?;
    if let Some(report) = &report {
        summarize(out, report, &host);
    }
    Ok(exit_code(report.as_ref()))
}

/// The exit code of a run: success only when a report exists and no check failed. An
/// interrupted run has no report and fails.
fn exit_code(report: Option<&Report>) -> ExitCode {
    match report {
        Some(report) if report.is_healthy() => ExitCode::SUCCESS,
        _ => ExitCode::FAILURE,
    }
}

/// Waits for the diagnosis and returns its result, unless `shutdown` fires first: then there is
/// no result.
async fn diagnose_until<R>(
    diagnosis: impl Future<Output = R>,
    shutdown: impl Future<Output = ()>,
) -> Option<R> {
    tokio::select! {
        report = diagnosis => Some(report),
        () = shutdown => None,
    }
}

/// Chooses the server and CA pin to diagnose. An explicit server is used with only an explicit
/// pin; otherwise the stored login supplies the server, and an explicit pin still wins over its pin.
fn resolve_target(
    server: Option<Url>,
    ca_pin: Option<CaPin>,
    stored: Option<StoredLogin>,
) -> Result<(Url, Option<CaPin>)> {
    if let Some(server) = server {
        return Ok((server, ca_pin));
    }
    let Some(stored) = stored else {
        return Err(MainError::NoServer);
    };
    Ok((stored.server, ca_pin.or(stored.ca_pin)))
}

/// The host of `server`, as the summary names it; the whole URL when it has none.
fn host_name(server: &Url) -> String {
    server
        .host_str()
        .map_or_else(|| server.to_string(), str::to_owned)
}

/// The width of the subject column: the longest check name plus two spaces. The rows are printed
/// one by one, so the column cannot follow the longest subject of a call.
fn subject_width() -> usize {
    Check::ALL
        .iter()
        .map(|check| columns(check.as_str()))
        .max()
        .unwrap_or(0)
        + 2
}

/// What the doctor is doing while `check` runs, for the wait line; `None` for a check that
/// never waits.
const fn activity(check: Check) -> Option<&'static str> {
    match check {
        Check::Server => Some("asking the server"),
        Check::Bazel => Some("asking Bazel for its effective flags"),
        Check::Credentials => Some("running the credential helper"),
        Check::Tls
        | Check::RemoteExecutor
        | Check::RemoteCache
        | Check::TlsCertificate
        | Check::CredentialHelper
        | Check::Clock => None,
    }
}

/// The wait line of `check`: its name padded to the subject column, then what the doctor is
/// doing. `None` for a check that never waits.
fn wait_label(check: Check) -> Option<String> {
    activity(check).map(|activity| {
        format!(
            "{:<width$}{activity}",
            check.as_str(),
            width = subject_width()
        )
    })
}

/// Prints each finding the moment the doctor reports it, and keeps one wait line for the check
/// that is running.
struct Stream<'o> {
    /// Where rows and the wait line go.
    out: &'o Output,
    /// The wait line of the check in flight; dropping it clears the line.
    live: Option<Progress>,
    /// The first error from writing a row. The doctor cannot be stopped from its observer, so
    /// later rows are skipped and the error is returned by [`Stream::finish`].
    failure: Option<MainError>,
}

impl<'o> Stream<'o> {
    /// A stream that writes to `out`.
    fn new(out: &'o Output) -> Self {
        Self {
            out,
            live: None,
            failure: None,
        }
    }

    /// Reacts to one step of the doctor: clears the wait line, then starts the next one or
    /// prints the finished row.
    fn observe(&mut self, event: Event<'_>) {
        self.live = None;
        match event {
            Event::Started(check) => {
                self.live = wait_label(check).map(|label| self.out.progress(&label));
            }
            Event::Finished(finding) => self.print(finding),
        }
    }

    /// Prints the row of `finding` in the fixed subject column.
    fn print(&mut self, finding: &Finding) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.out.list_padded(subject_width(), &[record(finding)]) {
            self.failure = Some(error);
        }
    }

    /// Clears the wait line and returns the first error from writing a row, if any.
    fn finish(mut self) -> Result<()> {
        self.live = None;
        self.failure.map_or(Ok(()), Err)
    }
}

/// The record of a finding: its status, the check as subject, the summary, the cause and the
/// fix when there are any.
fn record(finding: &Finding) -> Record {
    let summary = finding.summary.as_str();
    let mut record = Record::new(finding.status.into(), summary).subject(finding.check.as_str());
    if let Some(cause) = &finding.cause {
        record = record.cause(cause.as_str());
    }
    if let Some(fix) = &finding.fix {
        record = record.fix(fix.as_str());
    }
    record
}

/// The summary below the list, shown on a terminal only: a blank row, then either the inline mark
/// with the all-green message or the count of every status that occurs.
fn summarize(out: &Output, report: &Report, host: &str) {
    let findings = report.findings();
    out.decoration(None, &[Line::new()]);
    if findings.iter().all(|finding| finding.status == Status::Ok) {
        let line = Line::new()
            .accent(format!("all {} checks ok", findings.len()))
            .muted(format!(" · Bazel is set up for {host}"));
        out.decoration(Some(Mark::Inline), &[line]);
    } else {
        out.decoration(None, &[counts(findings).indented(COLUMN)]);
    }
}

/// `<k> fail · <k> warn · <k> ok · <k> skipped`, leaving out the statuses that do not occur.
fn counts(findings: &[Finding]) -> Line {
    let tally = |status: Status| {
        findings
            .iter()
            .filter(|finding| finding.status == status)
            .count()
    };
    let parts = [
        (tally(Status::Fail), "fail", Role::Fail),
        (tally(Status::Warn), "warn", Role::Warn),
        (tally(Status::Ok), "ok", Role::Muted),
        (tally(Status::Skip), "skipped", Role::Muted),
    ];
    let mut line = Line::new();
    for (count, word, role) in parts.into_iter().filter(|(count, ..)| *count > 0) {
        if !line.is_empty() {
            line = line.muted(" · ");
        }
        line = line.span(role, format!("{count} {word}"));
    }
    line
}

#[cfg(test)]
mod tests {
    use crate::output::text_of;
    use crate::palette::{Palette, strip_ansi as strip_sgr};

    use super::*;

    /// The text of a valid pin whose digest repeats one byte.
    fn pin_text(byte_hex: &str) -> String {
        format!("sha256:{}", byte_hex.repeat(32))
    }

    /// A valid pin whose digest repeats one byte.
    fn pin(byte_hex: &str) -> CaPin {
        pin_text(byte_hex)
            .parse::<CaPin>()
            .expect("test pin text is valid")
    }

    /// The text of an optional pin, for comparing the pins a resolution returns.
    fn text(pin: Option<&CaPin>) -> Option<String> {
        pin.map(ToString::to_string)
    }

    /// A valid URL.
    fn url(text: &str) -> Url {
        text.parse::<Url>().expect("test url is valid")
    }

    /// A stored login for `server` with the given pin.
    fn stored(server: &str, ca_pin: Option<CaPin>) -> StoredLogin {
        StoredLogin {
            server: url(server),
            ca_pin,
        }
    }

    #[test]
    fn explicit_server_takes_only_the_explicit_pin() {
        let (server, ca_pin) = resolve_target(
            Some(url("https://gang.example.com")),
            None,
            Some(stored("https://stored.example.com", Some(pin("aa")))),
        )
        .expect("an explicit server resolves");
        assert_eq!(server, url("https://gang.example.com"));
        assert_eq!(text(ca_pin.as_ref()), None);
    }

    #[test]
    fn explicit_server_and_pin_are_used_as_given() {
        let (server, ca_pin) =
            resolve_target(Some(url("https://gang.example.com")), Some(pin("cc")), None)
                .expect("an explicit server resolves");
        assert_eq!(server, url("https://gang.example.com"));
        assert_eq!(text(ca_pin.as_ref()), Some(pin_text("cc")));
    }

    #[test]
    fn stored_login_supplies_the_server_and_its_pin() {
        let (server, ca_pin) = resolve_target(
            None,
            None,
            Some(stored("https://stored.example.com", Some(pin("aa")))),
        )
        .expect("the stored login resolves");
        assert_eq!(server, url("https://stored.example.com"));
        assert_eq!(text(ca_pin.as_ref()), Some(pin_text("aa")));
    }

    #[test]
    fn explicit_pin_wins_over_the_stored_pin() {
        let (server, ca_pin) = resolve_target(
            None,
            Some(pin("cc")),
            Some(stored("https://stored.example.com", Some(pin("aa")))),
        )
        .expect("the stored login resolves");
        assert_eq!(server, url("https://stored.example.com"));
        assert_eq!(text(ca_pin.as_ref()), Some(pin_text("cc")));
    }

    #[test]
    fn stored_login_without_a_pin_resolves_without_one() {
        let (_, ca_pin) =
            resolve_target(None, None, Some(stored("https://stored.example.com", None)))
                .expect("the stored login resolves");
        assert_eq!(text(ca_pin.as_ref()), None);
    }

    #[test]
    fn no_server_and_no_stored_login_is_an_error() {
        assert!(matches!(
            resolve_target(None, None, None),
            Err(MainError::NoServer)
        ));
    }

    #[test]
    fn the_host_is_the_host_of_the_server_url_without_scheme_or_port() {
        assert_eq!(
            host_name(&url("https://gang.example.com")),
            "gang.example.com"
        );
        assert_eq!(host_name(&url("https://127.0.0.1:50051/ui/")), "127.0.0.1");
    }

    const HOST: &str = "gang.example.com";

    fn all_green() -> Report {
        Report::new(vec![
            Finding::ok(Check::Server, "https://gang.example.com answers"),
            Finding::ok(Check::Tls, "trusted by the system roots"),
            Finding::ok(Check::Bazel, "Bazel 8.7.0 reported its effective flags"),
            Finding::ok(Check::RemoteExecutor, "grpcs://gang.example.com:443"),
            Finding::ok(Check::RemoteCache, "grpcs://gang.example.com:443"),
            Finding::ok(Check::TlsCertificate, "none needed; the system roots apply"),
            Finding::ok(Check::CredentialHelper, "gang.example.com=gang"),
            Finding::ok(Check::Credentials, "gang get returns a token"),
            Finding::ok(Check::Clock, "within 1s of the server"),
        ])
    }

    fn problems() -> Report {
        Report::new(vec![
            Finding::ok(Check::Server, "https://gang.example.com answers"),
            Finding::ok(Check::Tls, "trusted by the system roots"),
            Finding::ok(Check::Bazel, "Bazel 8.7.0 reported its effective flags"),
            Finding::ok(Check::RemoteExecutor, "grpcs://gang.example.com:443"),
            Finding::fail(
                Check::RemoteCache,
                "grpcs://cache.corp.example.com from ~/.bazelrc",
                "remove --remote_cache from ~/.bazelrc; actions upload elsewhere and fail with \"missing from CAS\"",
            ),
            Finding::ok(Check::TlsCertificate, "none needed; the system roots apply"),
            Finding::ok(Check::CredentialHelper, "gang.example.com=gang"),
            Finding::fail(
                Check::Credentials,
                "not logged in",
                "gang login --server https://gang.example.com",
            ),
            Finding::warn(
                Check::Clock,
                "41s ahead of the server",
                "sync this machine's clock; tokens are refused beyond the skew limit",
            ),
        ])
    }

    fn server_down() -> Report {
        Report::new(vec![
            Finding::fail(
                Check::Server,
                "cannot reach https://gang.example.com",
                "check --server; for a farm on this machine, run gang local up",
            ),
            Finding::skip(Check::Tls, "needs the server"),
            Finding::ok(Check::Bazel, "Bazel 8.7.0 reported its effective flags"),
            Finding::skip(Check::RemoteExecutor, "needs the server"),
            Finding::skip(Check::RemoteCache, "needs the server"),
            Finding::skip(Check::TlsCertificate, "needs the server"),
            Finding::skip(Check::CredentialHelper, "needs the server"),
            Finding::skip(Check::Credentials, "needs the server"),
            Finding::skip(Check::Clock, "needs the server"),
        ])
    }

    /// Prints `report` the way a run does: each finding as its own event, then the summary.
    fn present(out: &Output, report: &Report, host: &str) -> Result<()> {
        let mut stream = Stream::new(out);
        for finding in report.findings() {
            stream.observe(Event::Finished(finding));
        }
        stream.finish()?;
        summarize(out, report, host);
        Ok(())
    }

    #[test]
    fn all_green_prints_one_row_per_check_and_nothing_else_when_piped() {
        let (out, stdout, stderr) = Output::captured();
        present(&out, &all_green(), HOST).expect("write to memory");
        assert_eq!(
            stdout.text(),
            text_of(&[
                "    ok  server             https://gang.example.com answers",
                "    ok  tls                trusted by the system roots",
                "    ok  bazel              Bazel 8.7.0 reported its effective flags",
                "    ok  remote_executor    grpcs://gang.example.com:443",
                "    ok  remote_cache       grpcs://gang.example.com:443",
                "    ok  tls_certificate    none needed; the system roots apply",
                "    ok  credential_helper  gang.example.com=gang",
                "    ok  credentials        gang get returns a token",
                "    ok  clock              within 1s of the server",
            ])
        );
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn problems_print_their_fix_rows_in_report_order() {
        let (out, stdout, stderr) = Output::captured();
        present(&out, &problems(), HOST).expect("write to memory");
        assert_eq!(
            stdout.text(),
            text_of(&[
                "    ok  server             https://gang.example.com answers",
                "    ok  tls                trusted by the system roots",
                "    ok  bazel              Bazel 8.7.0 reported its effective flags",
                "    ok  remote_executor    grpcs://gang.example.com:443",
                "  fail  remote_cache       grpcs://cache.corp.example.com from ~/.bazelrc",
                "   fix  remove --remote_cache from ~/.bazelrc; actions upload elsewhere and fail with \"missing from CAS\"",
                "    ok  tls_certificate    none needed; the system roots apply",
                "    ok  credential_helper  gang.example.com=gang",
                "  fail  credentials        not logged in",
                "   fix  gang login --server https://gang.example.com",
                "  warn  clock              41s ahead of the server",
                "   fix  sync this machine's clock; tokens are refused beyond the skew limit",
            ])
        );
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn a_down_server_makes_the_dependent_checks_skip() {
        let (out, stdout, _) = Output::captured();
        present(&out, &server_down(), HOST).expect("write to memory");
        assert_eq!(
            stdout.text(),
            text_of(&[
                "  fail  server             cannot reach https://gang.example.com",
                "   fix  check --server; for a farm on this machine, run gang local up",
                "  skip  tls                needs the server",
                "    ok  bazel              Bazel 8.7.0 reported its effective flags",
                "  skip  remote_executor    needs the server",
                "  skip  remote_cache       needs the server",
                "  skip  tls_certificate    needs the server",
                "  skip  credential_helper  needs the server",
                "  skip  credentials        needs the server",
                "  skip  clock              needs the server",
            ])
        );
    }

    #[test]
    fn quiet_keeps_the_deviations_in_the_same_columns() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        present(&out, &problems(), HOST).expect("write to memory");
        assert_eq!(
            stdout.text(),
            text_of(&[
                "  fail  remote_cache       grpcs://cache.corp.example.com from ~/.bazelrc",
                "   fix  remove --remote_cache from ~/.bazelrc; actions upload elsewhere and fail with \"missing from CAS\"",
                "  fail  credentials        not logged in",
                "   fix  gang login --server https://gang.example.com",
                "  warn  clock              41s ahead of the server",
                "   fix  sync this machine's clock; tokens are refused beyond the skew limit",
            ])
        );
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn quiet_prints_nothing_for_a_healthy_report() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        present(&out, &all_green(), HOST).expect("write to memory");
        assert_eq!(stdout.text(), "");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn the_all_green_summary_is_the_inline_mark_and_a_message_after_a_blank_row() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        present(&out, &all_green(), HOST).expect("write to memory");
        let summary = stderr.text();
        assert_eq!(
            strip_sgr(&summary),
            "\n▀▄▀▀▄▀  all 9 checks ok · Bazel is set up for gang.example.com\n"
        );
        assert!(summary.contains(
            "\x1b[1m\x1b[32mall 9 checks ok\x1b[0m\x1b[2m · Bazel is set up for gang.example.com\x1b[0m"
        ));
    }

    #[test]
    fn the_counts_row_sits_at_the_message_column_with_fail_and_warn_colored() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        present(&out, &problems(), HOST).expect("write to memory");
        let summary = stderr.text();
        assert_eq!(strip_sgr(&summary), "\n        2 fail · 1 warn · 6 ok\n");
        assert!(summary.contains(
            "        \x1b[1m\x1b[31m2 fail\x1b[0m\x1b[2m · \x1b[0m\x1b[1m\x1b[33m1 warn\x1b[0m\x1b[2m · 6 ok\x1b[0m"
        ));
    }

    #[test]
    fn the_counts_row_names_skipped_checks_and_leaves_out_empty_categories() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        present(&out, &server_down(), HOST).expect("write to memory");
        assert_eq!(
            strip_sgr(&stderr.text()),
            "\n        1 fail · 1 ok · 7 skipped\n"
        );
    }

    #[test]
    fn a_warning_alone_still_gets_the_counts_row_not_the_all_green_message() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        let report = Report::new(vec![
            Finding::ok(Check::Server, "answers"),
            Finding::warn(Check::Clock, "41s ahead", "sync it"),
        ]);
        present(&out, &report, HOST).expect("write to memory");
        assert_eq!(strip_sgr(&stderr.text()), "\n        1 warn · 1 ok\n");
    }

    #[test]
    fn the_summary_is_not_printed_when_stderr_is_not_a_terminal() {
        let (out, _, stderr) = Output::captured();
        present(&out, &problems(), HOST).expect("write to memory");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn each_row_is_printed_when_its_finding_arrives_in_the_final_column() {
        let (out, stdout, stderr) = Output::captured();
        let mut stream = Stream::new(&out);

        stream.observe(Event::Started(Check::Server));
        assert_eq!(stdout.text(), "");

        let server = Finding::ok(Check::Server, "answers");
        stream.observe(Event::Finished(&server));
        assert_eq!(
            stdout.text(),
            text_of(&["    ok  server             answers"])
        );

        stream.observe(Event::Started(Check::Bazel));
        let bazel = Finding::fail(Check::Bazel, "cannot run", "install Bazel");
        stream.observe(Event::Finished(&bazel));
        assert_eq!(
            stdout.text(),
            text_of(&[
                "    ok  server             answers",
                "  fail  bazel              cannot run",
                "   fix  install Bazel",
            ])
        );

        stream.finish().expect("write to memory");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn the_subject_column_is_the_longest_check_name_plus_two() {
        assert_eq!(subject_width(), "credential_helper".len() + 2);
    }

    #[test]
    fn only_the_checks_that_wait_have_an_activity() {
        assert_eq!(activity(Check::Server), Some("asking the server"));
        assert_eq!(
            activity(Check::Bazel),
            Some("asking Bazel for its effective flags")
        );
        assert_eq!(
            activity(Check::Credentials),
            Some("running the credential helper")
        );
        assert_eq!(activity(Check::Clock), None);
    }

    #[test]
    fn the_wait_label_pads_the_check_name_to_the_subject_column() {
        assert_eq!(
            wait_label(Check::Server).as_deref(),
            Some("server             asking the server")
        );
        assert_eq!(wait_label(Check::Tls), None);
    }

    #[test]
    fn a_cause_and_a_fix_ride_on_the_record_of_the_finding() {
        let finding = Finding::fail(Check::Server, "cannot reach it", "check --server")
            .with_cause("connection refused");
        assert_eq!(
            record(&finding),
            Record::fail("cannot reach it")
                .subject("server")
                .cause("connection refused")
                .fix("check --server")
        );
    }

    #[test]
    fn exit_code_follows_the_report() {
        let warnings_only = Report::new(vec![
            Finding::ok(Check::Server, "server"),
            Finding::warn(
                Check::Clock,
                "41s ahead of the server",
                "sync this machine's clock",
            ),
            Finding::skip(Check::Tls, "needs the server"),
        ]);
        assert_eq!(exit_code(Some(&warnings_only)), ExitCode::SUCCESS);
        assert_eq!(exit_code(Some(&all_green())), ExitCode::SUCCESS);
        assert_eq!(exit_code(Some(&problems())), ExitCode::FAILURE);
        assert_eq!(exit_code(None), ExitCode::FAILURE);
    }

    #[tokio::test]
    async fn an_interrupted_diagnosis_yields_no_report() {
        let report = diagnose_until(std::future::pending::<u8>(), std::future::ready(())).await;
        assert_eq!(report, None);
    }

    #[tokio::test]
    async fn a_finished_diagnosis_yields_its_report() {
        let report = diagnose_until(async { 7_u8 }, std::future::pending()).await;
        assert_eq!(report, Some(7));
    }
}
