//! Styled lines and records, and how a record becomes rows of the grid.
//!
//! The grid: columns `0..MARGIN` hold one right-aligned token (a status word, `fix`, `next`, or a
//! field key); the message starts at column `COLUMN`.

use std::fmt::Write as _;

use crate::palette::{Palette, Role};
use crate::wrap::{columns, fold, hang, hang_multiline};

/// Width of the margin: the one token that sits right-aligned left of the message.
pub(crate) const MARGIN: usize = 6;

/// Column where messages, field values, fix texts and notes start.
pub(crate) const COLUMN: usize = 8;

/// The marker in front of a cause line.
const CAUSE: &str = "↳ ";

/// Columns between the longest subject of a list and its summaries.
const LIST_GAP: usize = 2;

/// A run of text in one role.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Span {
    /// What the text means.
    role: Role,
    /// The text itself.
    text: String,
}

/// One row of output: spans of text, each with a role.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Line {
    /// The spans in order; adjacent spans never share a role.
    spans: Vec<Span>,
}

impl Line {
    /// An empty line.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Appends text in `role`, merging it into the last span when the role matches.
    pub(crate) fn span(mut self, role: Role, text: impl Into<String>) -> Self {
        let text = text.into();
        if text.is_empty() {
            return self;
        }
        match self.spans.last_mut() {
            Some(last) if last.role == role => last.text.push_str(&text),
            _ => self.spans.push(Span { role, text }),
        }
        self
    }

    /// Appends text in the default ink.
    pub(crate) fn plain(self, text: impl Into<String>) -> Self {
        self.span(Role::Plain, text)
    }

    /// Appends `columns` blanks in the default ink; a zero pad leaves the line unchanged.
    pub(crate) fn pad(self, columns: usize) -> Self {
        self.plain(" ".repeat(columns))
    }

    /// Appends bold text.
    pub(crate) fn strong(self, text: impl Into<String>) -> Self {
        self.span(Role::Strong, text)
    }

    /// Appends muted text.
    pub(crate) fn muted(self, text: impl Into<String>) -> Self {
        self.span(Role::Muted, text)
    }

    /// Appends accent text.
    pub(crate) fn accent(self, text: impl Into<String>) -> Self {
        self.span(Role::Accent, text)
    }

    /// The spans in order, each with its role.
    pub(crate) fn spans(&self) -> impl Iterator<Item = (Role, &str)> {
        self.spans
            .iter()
            .map(|span| (span.role, span.text.as_str()))
    }

    /// The text of the line with every style dropped.
    pub(crate) fn text(&self) -> String {
        self.spans.iter().map(|span| span.text.as_str()).collect()
    }

    /// The line as terminal text: spans in their palette style, or verbatim without a palette.
    pub(crate) fn paint(&self, palette: Option<Palette>) -> String {
        let mut painted = String::new();
        for span in &self.spans {
            match palette
                .map(|palette| palette.style(span.role))
                .filter(|style| !style.is_plain())
            {
                Some(style) => {
                    let _ = write!(painted, "{style}{}{style:#}", span.text);
                }
                None => painted.push_str(&span.text),
            }
        }
        painted
    }

    /// Width in terminal columns.
    pub(crate) fn width(&self) -> usize {
        self.spans.iter().map(|span| columns(&span.text)).sum()
    }

    /// Whether the line has no text at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// This line followed by `other`.
    pub(crate) fn append(mut self, other: Self) -> Self {
        for span in other.spans {
            self = self.span(span.role, span.text);
        }
        self
    }

    /// This line behind `columns` spaces; an empty line stays empty.
    pub(crate) fn indented(self, columns: usize) -> Self {
        if self.is_empty() {
            return self;
        }
        Self::new().pad(columns).append(self)
    }
}

impl From<&str> for Line {
    fn from(text: &str) -> Self {
        Self::new().plain(text)
    }
}

impl From<String> for Line {
    fn from(text: String) -> Self {
        Self::new().plain(text)
    }
}

/// The margin of a row: `token` right-aligned in `role`, then blanks up to [`COLUMN`].
pub(crate) fn margin(token: &str, role: Role) -> Line {
    Line::new()
        .pad(MARGIN.saturating_sub(columns(token)))
        .span(role, token)
        .pad(COLUMN - MARGIN)
}

/// How a record ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Status {
    /// Done, or healthy.
    Ok,
    /// Done, but worth a look.
    Warn,
    /// Not done.
    Fail,
    /// Not attempted.
    Skip,
}

impl Status {
    /// The word in the margin.
    fn word(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Skip => "skip",
        }
    }

    /// The role of the word.
    fn role(self) -> Role {
        match self {
            Self::Ok => Role::Ok,
            Self::Warn => Role::Warn,
            Self::Fail => Role::Fail,
            Self::Skip => Role::Muted,
        }
    }
}

impl From<gang_client::Status> for Status {
    fn from(status: gang_client::Status) -> Self {
        match status {
            gang_client::Status::Ok => Self::Ok,
            gang_client::Status::Warn => Self::Warn,
            gang_client::Status::Fail => Self::Fail,
            gang_client::Status::Skip => Self::Skip,
        }
    }
}

/// One outcome: a status, an optional subject, a summary, the causes behind it and a fix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    /// How it ended.
    status: Status,
    /// The check name or `gang <command>`.
    subject: Option<String>,
    /// What happened.
    summary: Line,
    /// Why, outermost first; each one gets its own row, or several when it has several lines.
    causes: Vec<String>,
    /// What to do about it, in one line.
    fix: Option<String>,
}

impl Record {
    /// A record with `status` and `summary` and nothing else.
    pub(crate) fn new(status: Status, summary: impl Into<Line>) -> Self {
        Self {
            status,
            subject: None,
            summary: summary.into(),
            causes: Vec::new(),
            fix: None,
        }
    }

    /// An `ok` record.
    pub(crate) fn ok(summary: impl Into<Line>) -> Self {
        Self::new(Status::Ok, summary)
    }

    /// A `warn` record.
    pub(crate) fn warn(summary: impl Into<Line>) -> Self {
        Self::new(Status::Warn, summary)
    }

    /// A `fail` record.
    pub(crate) fn fail(summary: impl Into<Line>) -> Self {
        Self::new(Status::Fail, summary)
    }

    /// Names what the record is about: a check, or `gang <command>`.
    pub(crate) fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Appends a cause.
    pub(crate) fn cause(mut self, cause: impl Into<String>) -> Self {
        self.causes.push(cause.into());
        self
    }

    /// Sets the one-line fix.
    pub(crate) fn fix(mut self, fix: impl Into<String>) -> Self {
        self.fix = Some(fix.into());
        self
    }

    /// Whether `--quiet` drops the record.
    pub(crate) fn quiet_drops(&self) -> bool {
        matches!(self.status, Status::Ok | Status::Skip)
    }

    /// The rows of the record, folded to `width` columns when there is one. The first line of a
    /// cause sits on its `↳` row and folds like the summary; further lines of a cause follow
    /// under it, keep their own leading blanks and fold at the same column.
    pub(crate) fn rows(&self, width: Option<usize>) -> Vec<Line> {
        self.render(None, width)
    }

    /// The rows of the record as an item of a list whose subject column is `subject_width`
    /// columns wide, the gap to the summary included: the subject, without a colon, is padded so
    /// every summary starts in one column. A subject wider than the column pushes its own
    /// summary right by [`LIST_GAP`] columns past it.
    pub(crate) fn list_rows(&self, subject_width: usize, width: Option<usize>) -> Vec<Line> {
        self.render(Some(subject_width), width)
    }

    /// The rows of the record; `subject_width` is set for a list item.
    fn render(&self, subject_width: Option<usize>, width: Option<usize>) -> Vec<Line> {
        let status = margin(self.status.word(), self.status.role());
        let mut rows = if let Some(subject_column) = subject_width {
            // The summary starts after the padded subject; its rows hang there.
            let subject = self.subject.as_deref().unwrap_or_default();
            let subject_columns = columns(subject);
            let subject_column = subject_column.max(subject_columns + LIST_GAP);
            let column = COLUMN + subject_column;
            let pad = subject_column - subject_columns;
            let lead = status.plain(subject).pad(pad);
            hang(&lead, column, fold(&self.summary, width, column))
        } else {
            let mut message = Line::new();
            if let Some(subject) = &self.subject {
                message = message.plain(subject).plain(": ");
            }
            message = message.append(self.summary.clone());
            hang(&status, COLUMN, fold(&message, width, COLUMN))
        };
        let indent = COLUMN + columns(CAUSE);
        for cause in &self.causes {
            rows.extend(hang_multiline(
                &Line::new().pad(COLUMN).muted(CAUSE),
                indent,
                width,
                cause.trim_end().lines().map(|text| Line::new().muted(text)),
            ));
        }
        if let Some(fix) = &self.fix {
            rows.extend(hang(
                &margin("fix", Role::Accent),
                COLUMN,
                fold(&Line::from(fix.as_str()), width, COLUMN),
            ));
        }
        rows
    }
}

/// The `fail` record of an error: its message, the causes of its source chain, and `fix`.
pub(crate) fn failure_record(
    subject: &str,
    error: &(dyn std::error::Error + 'static),
    fix: Option<String>,
) -> Record {
    let chain = gang_types::error_chain(error);
    let mut record = Record::fail(chain.summary()).subject(subject);
    for cause in chain.causes() {
        record = record.cause(cause.as_str());
    }
    if let Some(fix) = fix {
        record = record.fix(fix);
    }
    record
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use crate::palette::{Palette, Role};

    use super::{Line, Record, Status, failure_record, margin};

    #[test]
    fn each_client_status_converts_to_the_same_named_status() {
        assert_eq!(Status::from(gang_client::Status::Ok), Status::Ok);
        assert_eq!(Status::from(gang_client::Status::Warn), Status::Warn);
        assert_eq!(Status::from(gang_client::Status::Fail), Status::Fail);
        assert_eq!(Status::from(gang_client::Status::Skip), Status::Skip);
    }

    fn texts(record: &Record, width: Option<usize>) -> Vec<String> {
        record.rows(width).iter().map(Line::text).collect()
    }

    #[test]
    fn the_margin_right_aligns_its_token_and_leaves_two_blank_columns() {
        assert_eq!(margin("ok", Role::Ok).text(), "    ok  ");
        assert_eq!(margin("fix", Role::Accent).text(), "   fix  ");
        assert_eq!(margin("public", Role::Muted).text(), "public  ");
    }

    #[test]
    fn a_record_is_status_subject_and_summary() {
        let record = Record::fail("cannot reach the server").subject("gang login");
        assert_eq!(
            texts(&record, None),
            ["  fail  gang login: cannot reach the server"]
        );
    }

    #[test]
    fn the_subject_is_plain_ink() {
        let record = Record::warn("careful").subject("gang login");
        let rows = record.rows(None);
        assert_eq!(
            rows[0].paint(Some(Palette::Ansi)),
            "  \x1b[1m\x1b[33mwarn\x1b[0m  gang login: careful"
        );
    }

    #[test]
    fn a_skip_record_has_its_word_in_the_margin() {
        assert_eq!(
            texts(&Record::new(Status::Skip, "needs the server"), None),
            ["  skip  needs the server"]
        );
    }

    #[test]
    fn a_list_item_pads_its_subject_so_summaries_share_a_column() {
        let record = Record::ok("answers").subject("server");
        let rows: Vec<String> = record.list_rows(19, None).iter().map(Line::text).collect();
        assert_eq!(rows, ["    ok  server             answers"]);
    }

    #[test]
    fn a_list_item_keeps_its_cause_and_fix_rows() {
        let record = Record::fail("cannot reach")
            .subject("server")
            .cause("refused")
            .fix("run it");
        let rows: Vec<String> = record.list_rows(8, None).iter().map(Line::text).collect();
        assert_eq!(
            rows,
            [
                "  fail  server  cannot reach",
                "        ↳ refused",
                "   fix  run it"
            ]
        );
    }

    #[test]
    fn a_list_item_folds_under_its_summary_column() {
        let record = Record::ok("alpha beta gamma delta epsilon").subject("ab");
        let rows: Vec<String> = record
            .list_rows(4, Some(30))
            .iter()
            .map(Line::text)
            .collect();
        assert_eq!(
            rows,
            ["    ok  ab  alpha beta gamma", "            delta epsilon"]
        );
    }

    #[test]
    fn the_fix_and_cause_rows_of_a_list_item_keep_their_own_hang() {
        let record = Record::fail("x")
            .subject("server")
            .cause("alpha beta gamma delta")
            .fix("alpha beta gamma delta");
        let rows: Vec<String> = record
            .list_rows(8, Some(24))
            .iter()
            .map(Line::text)
            .collect();
        assert_eq!(
            rows,
            [
                "  fail  server  x",
                "        ↳ alpha beta",
                "          gamma delta",
                "   fix  alpha beta gamma",
                "        delta",
            ]
        );
    }

    #[test]
    fn causes_and_the_fix_get_their_own_rows() {
        let record = Record::fail("a").cause("b").cause("c").fix("run d");
        assert_eq!(
            texts(&record, None),
            ["  fail  a", "        ↳ b", "        ↳ c", "   fix  run d"]
        );
    }

    #[test]
    fn a_multi_line_cause_continues_under_its_marker_verbatim() {
        let record = Record::fail("cannot parse the configuration")
            .cause("TOML parse error at line 1, column 5\n  |\n1 | key = = 2\n  |     ^\n")
            .fix("fix the file");
        assert_eq!(
            texts(&record, None),
            [
                "  fail  cannot parse the configuration",
                "        ↳ TOML parse error at line 1, column 5",
                "            |",
                "          1 | key = = 2",
                "            |     ^",
                "   fix  fix the file",
            ]
        );
    }

    #[test]
    fn continuation_lines_of_a_cause_fold_under_the_cause() {
        let record =
            Record::fail("x").cause("first line that is long\nsecond line that is just as long");
        assert_eq!(
            texts(&record, Some(24)),
            [
                "  fail  x",
                "        ↳ first line",
                "          that is long",
                "          second line",
                "          that is just",
                "          as long",
            ]
        );
    }

    #[test]
    fn causes_preserve_blank_continuation_rows() {
        let record = Record::fail("x").cause("first\n\n \t\nsecond");
        assert_eq!(
            texts(&record, Some(24)),
            ["  fail  x", "        ↳ first", "", "", "          second"]
        );
    }

    #[test]
    fn a_trailing_newline_of_a_cause_adds_no_row() {
        let record = Record::fail("x").cause("why\n\n");
        assert_eq!(texts(&record, None), ["  fail  x", "        ↳ why"]);
    }

    #[test]
    fn a_long_fix_folds_under_its_own_start() {
        let record = Record::fail("x").fix("alpha beta gamma delta");
        assert_eq!(
            texts(&record, Some(20)),
            ["  fail  x", "   fix  alpha beta", "        gamma delta"]
        );
    }

    /// Error with a fixed message and an optional source, like a library error that repeats its cause.
    #[derive(Debug)]
    struct Link {
        message: &'static str,
        source: Option<Box<Link>>,
    }

    impl Link {
        fn leaf(message: &'static str) -> Self {
            Self {
                message,
                source: None,
            }
        }

        fn caused_by(message: &'static str, source: Link) -> Self {
            Self {
                message,
                source: Some(Box::new(source)),
            }
        }
    }

    impl fmt::Display for Link {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for Link {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_deref()
                .map(|cause| cause as &(dyn std::error::Error + 'static))
        }
    }

    #[test]
    fn a_source_with_blank_text_adds_no_cause_row() {
        let error = Link::caused_by("load failed", Link::leaf("  "));
        let record = failure_record("gang", &error, None);
        assert!(record.causes.is_empty());
    }

    #[test]
    fn the_failure_record_names_the_subject_and_carries_the_fix() {
        let error = Link::caused_by("cannot reach the server", Link::leaf("refused"));
        let record = failure_record("gang login", &error, Some("gang local up".to_owned()));
        assert_eq!(
            texts(&record, None),
            [
                "  fail  gang login: cannot reach the server",
                "        ↳ refused",
                "   fix  gang local up",
            ]
        );
    }
}
