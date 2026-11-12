//! The typed result of `gang doctor`: one [`Finding`] per [`Check`], in report order.
//!
//! The types carry data only. Rendering (colors, symbols, layout) belongs to the binary.

use gang_types::string_enum;

string_enum! {
    /// One diagnosis `gang doctor` performs. Declaration order is the report order.
    pub enum Check("doctor check") {
        /// The Gang server answers its information endpoint.
        Server = "server",
        /// The certificate of the server is trusted, or the connection is plaintext on loopback.
        Tls = "tls",
        /// Bazel reports its effective options.
        Bazel = "bazel",
        /// Bazel's `--remote_executor` points at the server's executor.
        RemoteExecutor = "remote_executor",
        /// Bazel's `--remote_cache` points at the server's cache.
        RemoteCache = "remote_cache",
        /// Bazel trusts the CA certificate the server needs, when it needs one.
        TlsCertificate = "tls_certificate",
        /// Bazel's `--credential_helper` covers the server's cache host.
        CredentialHelper = "credential_helper",
        /// The credential helper returns a token the server accepts.
        Credentials = "credentials",
        /// This machine's clock agrees with the server's clock.
        Clock = "clock",
    }
}

string_enum! {
    /// How a check came out.
    pub enum Status("doctor status") {
        /// The check passed.
        Ok = "ok",
        /// The check found something worth attention that does not break the setup.
        Warn = "warn",
        /// The check found a broken setup.
        Fail = "fail",
        /// The check could not run because a check it depends on failed.
        Skip = "skip",
    }
}

/// The outcome of one [`Check`]: a status, a one-line summary, and a one-line fix when there is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// The check this finding belongs to.
    pub check: Check,
    /// How the check came out.
    pub status: Status,
    /// What was observed, on one line.
    pub summary: String,
    /// What to do about it, on one line. `None` when nothing needs doing.
    pub fix: Option<String>,
    /// The underlying reason on one line, printed under the summary. `None` when there is
    /// nothing to add.
    pub cause: Option<String>,
}

impl Finding {
    /// A passing finding with no fix.
    pub fn ok(check: Check, summary: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Ok,
            summary: one_line(summary),
            fix: None,
            cause: None,
        }
    }

    /// A finding that could not run, with no fix.
    pub fn skip(check: Check, summary: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Skip,
            summary: one_line(summary),
            fix: None,
            cause: None,
        }
    }

    /// A finding that needs attention but does not break the setup.
    pub fn warn(check: Check, summary: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Warn,
            summary: one_line(summary),
            fix: Some(one_line(fix)),
            cause: None,
        }
    }

    /// A finding that shows a broken setup.
    pub fn fail(check: Check, summary: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            check,
            status: Status::Fail,
            summary: one_line(summary),
            fix: Some(one_line(fix)),
            cause: None,
        }
    }

    /// This finding with the underlying reason, on one line, printed under the summary.
    #[must_use]
    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(one_line(cause));
        self
    }
}

/// Collapses every run of whitespace, newlines included, into one space and trims the ends.
fn one_line(text: impl Into<String>) -> String {
    text.into().split_whitespace().collect::<Vec<_>>().join(" ")
}

/// All findings of one `gang doctor` run, ordered by [`Check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    findings: Vec<Finding>,
}

impl Report {
    /// Builds a report. The findings are stably sorted by their check, so findings of the same
    /// check keep the order they were given in.
    #[must_use]
    pub fn new(mut findings: Vec<Finding>) -> Self {
        findings.sort_by_key(|finding| finding.check);
        Self { findings }
    }

    /// The findings, in report order.
    #[must_use]
    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// `true` when no finding has [`Status::Fail`]. Warnings and skipped checks do not count.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.findings
            .iter()
            .all(|finding| finding.status != Status::Fail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_names_are_stable() {
        let statuses = [
            (Status::Ok, "ok"),
            (Status::Warn, "warn"),
            (Status::Fail, "fail"),
            (Status::Skip, "skip"),
        ];
        for (status, name) in statuses {
            assert_eq!(status.as_str(), name);
        }
    }

    #[test]
    fn report_order_is_declaration_order() {
        assert!(Check::ALL.is_sorted());
        let names = Check::NAMES;
        for (index, name) in names.iter().enumerate() {
            assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
            assert!(!names[..index].contains(name));
        }
    }

    #[test]
    fn a_cause_is_one_line_and_constructors_leave_it_empty() {
        assert_eq!(Finding::ok(Check::Server, "x").cause, None);
        assert_eq!(Finding::skip(Check::Server, "x").cause, None);
        assert_eq!(Finding::warn(Check::Server, "x", "y").cause, None);
        assert_eq!(Finding::fail(Check::Server, "x", "y").cause, None);
        let caused = Finding::fail(Check::Server, "x", "y").with_cause(" connection\n refused ");
        assert_eq!(caused.cause.as_deref(), Some("connection refused"));
        assert_eq!(caused.summary, "x");
    }

    #[test]
    fn constructors_collapse_whitespace_to_one_line() {
        let ok = Finding::ok(Check::Server, "  first line\n\n  second\tline  ");
        assert_eq!(ok.summary, "first line second line");
        assert_eq!(ok.fix, None);

        let skip = Finding::skip(Check::Tls, "needs\na\nreachable\n\nserver");
        assert_eq!(skip.summary, "needs a reachable server");
        assert_eq!(skip.fix, None);

        let warn = Finding::warn(Check::Clock, "clock\nslow", " run\n  ntp \n now ");
        assert_eq!(warn.summary, "clock slow");
        assert_eq!(warn.fix.as_deref(), Some("run ntp now"));

        let fail = Finding::fail(Check::Bazel, "a\n\tb", "c\r\nd");
        assert_eq!(fail.summary, "a b");
        assert_eq!(fail.fix.as_deref(), Some("c d"));
    }

    #[test]
    fn constructors_set_their_status() {
        assert_eq!(Finding::ok(Check::Clock, "x").status, Status::Ok);
        assert_eq!(Finding::skip(Check::Clock, "x").status, Status::Skip);
        assert_eq!(Finding::warn(Check::Clock, "x", "y").status, Status::Warn);
        assert_eq!(Finding::fail(Check::Clock, "x", "y").status, Status::Fail);
    }

    #[test]
    fn new_orders_findings_by_check_and_keeps_equal_checks_in_given_order() {
        let report = Report::new(vec![
            Finding::ok(Check::Clock, "clock"),
            Finding::fail(Check::Server, "first server", "fix"),
            Finding::ok(Check::Tls, "tls"),
            Finding::warn(Check::Server, "second server", "fix"),
        ]);
        let order: Vec<(Check, &str)> = report
            .findings()
            .iter()
            .map(|finding| (finding.check, finding.summary.as_str()))
            .collect();
        assert_eq!(
            order,
            vec![
                (Check::Server, "first server"),
                (Check::Server, "second server"),
                (Check::Tls, "tls"),
                (Check::Clock, "clock"),
            ]
        );
    }

    #[test]
    fn empty_report_is_healthy() {
        assert!(Report::new(Vec::new()).is_healthy());
        assert!(Report::new(Vec::new()).findings().is_empty());
    }

    #[test]
    fn is_healthy_ignores_warnings_and_skips() {
        let report = Report::new(vec![
            Finding::ok(Check::Server, "server"),
            Finding::warn(Check::Clock, "clock", "sync"),
            Finding::skip(Check::Credentials, "needs the server"),
        ]);
        assert!(report.is_healthy());
    }

    #[test]
    fn is_healthy_is_false_with_any_failure() {
        let report = Report::new(vec![
            Finding::ok(Check::Server, "server"),
            Finding::warn(Check::Clock, "clock", "sync"),
            Finding::fail(Check::Credentials, "not logged in", "log in"),
        ]);
        assert!(!report.is_healthy());
    }
}
