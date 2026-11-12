//! Output of the `gang` binary: the only code that knows about terminals, styles and the mark.
//!
//! The streams: stdout carries the result (data, key/value fields, `ok` records), stderr carries
//! steps, notes, warnings, errors, progress and decorations. Once a command has written `data`,
//! its records go to stderr too, so `gang bazelrc > .bazelrc` stays clean.
//!
//! Piped output has the same lines as a terminal's without color, wrapping, progress and
//! decorations; the grid they share is described in `record`.

use std::borrow::Cow;
use std::fmt::Display;
use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anstream::AutoStream;
use gang_types::Fix as _;
use parking_lot::Mutex;
use terminal_size::{Width, terminal_size_of};

use crate::errors::{MainError, Result};
use crate::look::{Look, decide};
use crate::mark::{self, BANNER_GAP, BANNER_INDENT};
use crate::palette::{Palette, Role};
pub(crate) use crate::progress::Progress;
use crate::record::{COLUMN, MARGIN, failure_record, margin};
pub(crate) use crate::record::{Line, Record};
use crate::wrap::{columns, fold, fold_indented, hang_multiline, wrap_text};

/// Where a decoration puts the mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The banner picture, with text beside it.
    Full,
    /// The inline mark in the margin, with the first line of text after it.
    Inline,
}

/// The rows of a decoration for a stream `width` columns wide. Rows fold at blanks under their
/// own indent, beside the mark or at the message column; one word wider than its row stays whole.
fn decoration_rows(
    mark: Option<Mark>,
    lines: &[Line],
    truecolor: bool,
    width: Option<usize>,
) -> Vec<Line> {
    match mark {
        None => lines
            .iter()
            .flat_map(|line| fold_indented(line, width))
            .collect(),
        Some(Mark::Full) => banner_rows(
            &folded_beside(
                lines,
                width,
                BANNER_INDENT + mark::banner_width() + BANNER_GAP,
            ),
            truecolor,
        ),
        Some(Mark::Inline) => inline_rows(&folded_beside(lines, width, COLUMN), truecolor),
    }
}

/// The lines folded for text that starts at column `column`: every row of a folded line is a
/// line of its own.
fn folded_beside(lines: &[Line], width: Option<usize>, column: usize) -> Vec<Line> {
    lines
        .iter()
        .flat_map(|line| fold(line, width, column))
        .collect()
}

/// The banner on the left, its text rows starting at one column; rows of text below the picture
/// start at that column too.
fn banner_rows(lines: &[Line], truecolor: bool) -> Vec<Line> {
    let picture = mark::banner(truecolor);
    let text_column = BANNER_INDENT + mark::banner_width() + BANNER_GAP;
    (0..picture.len().max(lines.len()))
        .map(|index| {
            let text = lines.get(index).cloned().unwrap_or_default();
            match picture.get(index) {
                Some(art) if text.is_empty() => art.clone().indented(BANNER_INDENT),
                Some(art) => {
                    let head = Line::new().pad(BANNER_INDENT).append(art.clone());
                    let pad = text_column.saturating_sub(BANNER_INDENT + art.width());
                    head.pad(pad).append(text)
                }
                None => text.indented(text_column),
            }
        })
        .collect()
}

/// The inline mark in the margin, then the first line at the message column; further lines sit
/// at the message column too.
fn inline_rows(lines: &[Line], truecolor: bool) -> Vec<Line> {
    let art = mark::inline(truecolor);
    let pad = COLUMN.saturating_sub(art.width());
    let first = lines.first().cloned().unwrap_or_default();
    let head = if first.is_empty() {
        art
    } else {
        art.pad(pad).append(first)
    };
    let mut rows = vec![head];
    rows.extend(
        lines
            .iter()
            .skip(1)
            .map(|line| line.clone().indented(COLUMN)),
    );
    rows
}

/// A writer and the wait line currently drawn on it.
struct Writer {
    /// Where the bytes go.
    target: Box<dyn Write + Send>,
    /// Columns of the live wait line on screen; 0 when there is none.
    live: usize,
}

impl Writer {
    /// Blanks the live wait line and returns the cursor to its start.
    fn clear_live(&mut self) -> io::Result<()> {
        if self.live == 0 {
            return Ok(());
        }
        let blanks = self.live;
        self.live = 0;
        write!(self.target, "\r{:blanks$}\r", "")
    }
}

/// One output stream with its look.
struct Sink {
    /// The stream, under a lock so the progress ticker and the command never interleave.
    writer: Mutex<Writer>,
    /// The palette when the stream is colored.
    palette: Option<Palette>,
    /// Whether the stream carries decorations and progress.
    decorate: bool,
    /// Columns to fold to; `None` never folds.
    width: Option<usize>,
}

impl Sink {
    /// A sink over `target`.
    fn new(target: Box<dyn Write + Send>, look: Look, width: Option<usize>) -> Self {
        Self {
            writer: Mutex::new(Writer { target, live: 0 }),
            palette: look.palette,
            decorate: look.decorate,
            width,
        }
    }

    /// The locked writer.
    fn writer(&self) -> parking_lot::MutexGuard<'_, Writer> {
        self.writer.lock()
    }

    /// Whether this stream shows the truecolor mark.
    fn truecolor(&self) -> bool {
        self.palette == Some(Palette::Truecolor)
    }

    /// The rows as terminal text, one newline after each.
    fn paint_rows(&self, rows: &[Line]) -> String {
        let mut text = String::new();
        for row in rows {
            text.push_str(&row.paint(self.palette));
            text.push('\n');
        }
        text
    }

    /// Writes `text` after clearing the live wait line, then flushes.
    fn write(&self, text: &str) -> io::Result<()> {
        let mut writer = self.writer();
        writer.clear_live()?;
        writer.target.write_all(text.as_bytes())?;
        writer.target.flush()
    }

    /// Clears the live wait line, if any.
    fn clear_live(&self) -> io::Result<()> {
        let mut writer = self.writer();
        if writer.live == 0 {
            return Ok(());
        }
        writer.clear_live()?;
        writer.target.flush()
    }

    /// Draws `text`, `columns` wide, as the live wait line over the previous one.
    fn draw_live(&self, text: &str, columns: usize) -> io::Result<()> {
        let mut writer = self.writer();
        let leftover = writer.live.saturating_sub(columns);
        writer.live = columns;
        write!(writer.target, "\r{text}{:leftover$}", "")?;
        writer.target.flush()
    }
}

/// State shared by every clone of an [`Output`] and by the progress ticker.
pub(crate) struct Shared {
    /// Standard output.
    out: Sink,
    /// Standard error.
    err: Sink,
    /// Whether `--quiet` is on.
    quiet: AtomicBool,
    /// Whether `data` has been written.
    data_written: AtomicBool,
}

impl Shared {
    /// Draws the wait line for `label` after `elapsed` on stderr. Errors are ignored: stderr has
    /// no channel to report them.
    pub(crate) fn draw_wait(&self, label: &str, elapsed: Duration) {
        let line = crate::progress::wait_line(label, elapsed, self.err.width);
        let _ = self
            .err
            .draw_live(&line.paint(self.err.palette), line.width());
    }

    /// Clears the wait line from stderr.
    pub(crate) fn clear_wait(&self) {
        let _ = self.err.clear_live();
    }
}

/// Which stream a row goes to.
#[derive(Clone, Copy)]
enum Stream {
    /// Standard output.
    Out,
    /// Standard error.
    Err,
}

/// The terminal output of one `gang` process: cheap to clone, usable from any thread.
#[derive(Clone)]
pub(crate) struct Output {
    /// The sinks and flags every clone shares.
    shared: Arc<Shared>,
}

impl Output {
    /// The real stdout and stderr, each decided from the environment and whether it is a
    /// terminal.
    pub(crate) fn from_environment() -> Self {
        let env = |key: &str| std::env::var_os(key);
        let stdout_terminal = io::stdout().is_terminal();
        let stderr_terminal = io::stderr().is_terminal();
        let stdout_look = decide(env, stdout_terminal);
        let stderr_look = decide(env, stderr_terminal);
        let width = |terminal: bool, size: Option<(Width, terminal_size::Height)>| {
            size.filter(|_| terminal)
                .map(|(Width(columns), _)| usize::from(columns))
                .filter(|&columns| columns > 0)
        };
        Self::assemble(
            Sink::new(
                Box::new(AutoStream::new(io::stdout(), stdout_look.color_choice())),
                stdout_look,
                width(stdout_terminal, terminal_size_of(io::stdout())),
            ),
            Sink::new(
                Box::new(AutoStream::new(io::stderr(), stderr_look.color_choice())),
                stderr_look,
                width(stderr_terminal, terminal_size_of(io::stderr())),
            ),
        )
    }

    /// An output over two sinks.
    fn assemble(out: Sink, err: Sink) -> Self {
        Self {
            shared: Arc::new(Shared {
                out,
                err,
                quiet: AtomicBool::new(false),
                data_written: AtomicBool::new(false),
            }),
        }
    }

    /// Turns `--quiet` on or off.
    pub(crate) fn set_quiet(&self, quiet: bool) {
        self.shared.quiet.store(quiet, Ordering::SeqCst);
    }

    /// Whether `--quiet` is on.
    fn quiet(&self) -> bool {
        self.shared.quiet.load(Ordering::SeqCst)
    }

    /// Whether stdout carries decorations.
    pub(crate) fn stdout_decorates(&self) -> bool {
        self.shared.out.decorate
    }

    /// Whether stderr takes decorations right now.
    fn decorating(&self) -> bool {
        self.shared.err.decorate && !self.quiet()
    }

    /// Where records and fields go: stdout until data has been written, then stderr.
    fn primary(&self) -> Stream {
        if self.shared.data_written.load(Ordering::SeqCst) {
            Stream::Err
        } else {
            Stream::Out
        }
    }

    /// The sink of a stream.
    fn sink(&self, stream: Stream) -> &Sink {
        match stream {
            Stream::Out => &self.shared.out,
            Stream::Err => &self.shared.err,
        }
    }

    /// Writes rows to stdout; a failed write is the command's failure. The wait line on stderr
    /// is cleared and its lock held until the rows are written, so the ticker cannot redraw
    /// in between. Lock order is always stderr, then stdout.
    fn emit_out(&self, rows: &[Line]) -> Result<()> {
        let text = self.shared.out.paint_rows(rows);
        let written = {
            let mut wait = self.shared.err.writer();
            if wait.live > 0 {
                let _ = wait.clear_live();
                let _ = wait.target.flush();
            }
            self.shared.out.write(&text)
        };
        written.map_err(MainError::Stdout)
    }

    /// Writes rows to stderr; a failed write is ignored, there is nowhere to report it.
    fn emit_err(&self, rows: &[Line]) {
        let text = self.shared.err.paint_rows(rows);
        let _ = self.shared.err.write(&text);
    }

    /// Writes the rows `build` makes, for the width of the primary stream, to the primary stream.
    fn emit_primary(&self, build: impl FnOnce(Option<usize>) -> Vec<Line>) -> Result<()> {
        let stream = self.primary();
        let rows = build(self.sink(stream).width);
        match stream {
            Stream::Out => self.emit_out(&rows),
            Stream::Err => {
                self.emit_err(&rows);
                Ok(())
            }
        }
    }

    /// Writes the outcome record to the primary stream. `--quiet` drops `ok` and `skip` records.
    pub(crate) fn result(&self, record: &Record) -> Result<()> {
        if self.quiet() && record.quiet_drops() {
            return Ok(());
        }
        self.emit_primary(|width| record.rows(width))
    }

    /// Writes a step record to stderr. `--quiet` drops `ok` and `skip` records.
    pub(crate) fn step(&self, record: &Record) {
        if self.quiet() && record.quiet_drops() {
            return;
        }
        self.emit_err(&record.rows(self.shared.err.width));
    }

    /// Writes records with subjects to the primary stream, summaries in one column: the subject,
    /// without a colon, is padded to `subject_width` columns, the gap to the summaries included,
    /// so rows written one call at a time keep one column. `--quiet` drops `ok` and `skip`
    /// records without moving the column.
    pub(crate) fn list_padded(&self, subject_width: usize, records: &[Record]) -> Result<()> {
        let quiet = self.quiet();
        self.emit_primary(|width| {
            records
                .iter()
                .filter(|record| !(quiet && record.quiet_drops()))
                .flat_map(|record| record.list_rows(subject_width, width))
                .collect()
        })
    }

    /// Writes one line of data to stdout: never wrapped, never dropped by `--quiet`. From here
    /// on records go to stderr.
    pub(crate) fn data(&self, line: impl Into<Line>) -> Result<()> {
        self.emit_out(&[line.into()])?;
        self.shared.data_written.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Writes a key/value block to the primary stream: muted keys right-aligned in the margin,
    /// values at the message column. A value folds at blanks under the message column on a
    /// terminal and keeps each of its lines on rows of their own; a word wider than the row stays
    /// whole. Not dropped by `--quiet`.
    pub(crate) fn fields(&self, pairs: &[(&str, &dyn Display)]) -> Result<()> {
        self.emit_primary(|width| {
            pairs
                .iter()
                .flat_map(|(key, value)| {
                    debug_assert!(
                        columns(key) <= MARGIN,
                        "field key {key} does not fit the margin"
                    );
                    let value = value.to_string();
                    hang_multiline(
                        &margin(key, Role::Muted),
                        COLUMN,
                        width,
                        value.lines().map(Line::from),
                    )
                })
                .collect()
        })
    }

    /// Writes an empty row to the primary stream, separating groups. Dropped by `--quiet`.
    pub(crate) fn gap(&self) -> Result<()> {
        if self.quiet() {
            return Ok(());
        }
        self.emit_primary(|_| vec![Line::new()])
    }

    /// Writes `text` to stdout, every row indented by `indent` columns and folded to the
    /// terminal width at that indent; piped, each line of `text` is one indented row. Not
    /// dropped by `--quiet`.
    pub(crate) fn block(&self, indent: usize, text: &str) -> Result<()> {
        let width = self.stdout_width();
        let rows: Vec<Line> = text
            .lines()
            .flat_map(|paragraph| -> Vec<Line> {
                if paragraph.trim().is_empty() {
                    return vec![Line::new()];
                }
                wrap_text(paragraph, indent, indent, width)
                    .into_iter()
                    .map(Line::from)
                    .collect()
            })
            .collect();
        self.emit_out(&rows)
    }

    /// The width stdout wraps to; `None` when stdout is not a terminal.
    pub(crate) fn stdout_width(&self) -> Option<usize> {
        self.shared.out.width
    }

    /// Writes an instruction line to stderr at the message column, folded at blanks under that
    /// column on a terminal. Dropped by `--quiet`.
    pub(crate) fn note(&self, line: impl Into<Line>) {
        if self.quiet() {
            return;
        }
        self.notice(line);
    }

    /// Writes an instruction line to stderr at the message column, folded at blanks under that
    /// column on a terminal, that `--quiet` keeps: something the user cannot proceed without.
    pub(crate) fn notice(&self, line: impl Into<Line>) {
        let width = self.shared.err.width;
        self.emit_err(&fold_indented(&line.into().indented(COLUMN), width));
    }

    /// Starts the live `wait` line on stderr. Inert unless stderr decorates and not `--quiet`.
    pub(crate) fn progress(&self, label: &str) -> Progress {
        if self.decorating() {
            Progress::start(Arc::clone(&self.shared), label)
        } else {
            Progress::inert()
        }
    }

    /// Writes a decoration to stderr: nothing unless stderr decorates and not `--quiet`. With a
    /// mark, the picture is on the left and `lines` sit beside it; without, `lines` are the rows.
    /// On a terminal, rows wider than the stream fold at blanks under their own indent.
    pub(crate) fn decoration(&self, mark: Option<Mark>, lines: &[Line]) {
        if !self.decorating() {
            return;
        }
        let err = &self.shared.err;
        self.emit_err(&decoration_rows(mark, lines, err.truecolor(), err.width));
    }

    /// Writes an empty row to stderr, also when piped. Dropped by `--quiet`. A blank row that
    /// only a terminal shows is `decoration(None, &[Line::new()])`.
    pub(crate) fn blank(&self) {
        if self.quiet() {
            return;
        }
        self.emit_err(&[Line::new()]);
    }

    /// Writes the `next` decoration: a blank row, then `next` in the margin and `command` at the
    /// message column. The command row is never folded, so the command stays copyable.
    pub(crate) fn next(&self, command: &str) {
        if !self.decorating() {
            return;
        }
        self.emit_err(&[Line::new(), margin("next", Role::Accent).plain(command)]);
    }

    /// Writes the failure record of `error` to stderr and returns the failure exit code. Never
    /// dropped by `--quiet`.
    pub(crate) fn failure(&self, subject: &str, error: &MainError) -> ExitCode {
        let record = failure_record(subject, error, error.fix().map(Cow::into_owned));
        self.emit_err(&record.rows(self.shared.err.width));
        ExitCode::FAILURE
    }

    /// Whether stderr is colored: what logging passes to its ANSI switch.
    pub(crate) fn stderr_ansi(&self) -> bool {
        self.shared.err.palette.is_some()
    }

    /// The palette of stdout; `None` when stdout is plain.
    pub(crate) fn stdout_palette(&self) -> Option<Palette> {
        self.shared.out.palette
    }

    /// A decoration as text for stdout's look; empty when stdout does not decorate.
    pub(crate) fn stdout_decoration(&self, mark: Option<Mark>, lines: &[Line]) -> String {
        let out = &self.shared.out;
        if !out.decorate {
            return String::new();
        }
        out.paint_rows(&decoration_rows(mark, lines, out.truecolor(), out.width))
    }
}

/// Formats a duration in tenths of a second, such as `2.4s`.
pub(crate) fn elapsed(duration: Duration) -> String {
    let tenths = duration.as_millis() / 100;
    format!("{}.{}s", tenths / 10, tenths % 10)
}

/// An in-memory stream for tests.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<Vec<u8>>>);

#[cfg(test)]
impl Capture {
    /// Everything written so far.
    pub(crate) fn text(&self) -> String {
        let bytes = self.0.lock();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

#[cfg(test)]
impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl Output {
    /// An output over two in-memory streams: plain, never folded, no decorations. Returns the
    /// output, then what it wrote to stdout and to stderr.
    pub(crate) fn captured() -> (Self, Capture, Capture) {
        Self::capture_with(
            Look {
                palette: None,
                decorate: false,
            },
            None,
        )
    }

    /// An output over two in-memory streams that look like terminals `width` columns wide in
    /// `palette`.
    pub(crate) fn captured_styled(palette: Palette, width: usize) -> (Self, Capture, Capture) {
        Self::capture_with(
            Look {
                palette: Some(palette),
                decorate: true,
            },
            Some(width),
        )
    }

    /// An output over two in-memory streams with one look and width for both.
    fn capture_with(look: Look, width: Option<usize>) -> (Self, Capture, Capture) {
        let stdout = Capture::default();
        let stderr = Capture::default();
        let output = Self::assemble(
            Sink::new(Box::new(stdout.clone()), look, width),
            Sink::new(Box::new(stderr.clone()), look, width),
        );
        (output, stdout, stderr)
    }
}

/// Joins lines as stream text, ending each line with a newline.
#[cfg(test)]
#[must_use]
pub(crate) fn text_of(lines: &[&str]) -> String {
    lines.iter().flat_map(|line| [*line, "\n"]).collect()
}

#[cfg(test)]
mod behavior {
    use std::process::ExitCode;
    use std::time::Duration;

    use super::{Capture, Line, Look, Mark, Output, Palette, Record, Sink, elapsed};
    use crate::errors::MainError;
    use crate::record::Status;

    /// A pair of terminal-like streams of `width` columns with the given color decision.
    fn terminal(
        palette: Option<Palette>,
        decorate: bool,
        width: Option<usize>,
    ) -> (Output, Capture, Capture) {
        Output::capture_with(Look { palette, decorate }, width)
    }

    /// The stderr text of a record written as a step.
    fn stepped(output: &Output, stderr: &Capture, record: &Record) -> String {
        output.step(record);
        stderr.text()
    }

    #[test]
    fn an_ok_result_goes_to_stdout_in_the_margin_form() {
        let (out, stdout, stderr) = Output::captured();
        out.result(&Record::ok("logged in with google · token valid for 12h"))
            .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "    ok  logged in with google · token valid for 12h\n"
        );
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn a_failure_record_lists_its_cause_and_fix_on_their_own_rows() {
        let (out, _, stderr) = Output::captured();
        let record = Record::fail("cannot reach the server https://gang.example.com")
            .subject("gang login")
            .cause("invalid peer certificate: UnknownIssuer")
            .fix("add --ca-pin sha256:<hex>; gang server prints it at startup");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  fail  gang login: cannot reach the server https://gang.example.com\n\
             \x20       ↳ invalid peer certificate: UnknownIssuer\n\
             \x20  fix  add --ca-pin sha256:<hex>; gang server prints it at startup\n"
        );
    }

    #[test]
    fn a_warning_with_a_cause_and_a_fix_has_the_same_shape() {
        let (out, _, stderr) = Output::captured();
        let record = Record::warn("cannot start the browser xdg-open")
            .cause("No such file or directory (os error 2)")
            .fix("open the address below yourself");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  warn  cannot start the browser xdg-open\n\
             \x20       ↳ No such file or directory (os error 2)\n\
             \x20  fix  open the address below yourself\n"
        );
    }

    #[test]
    fn quiet_drops_ok_records_and_keeps_warn_and_fail() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        out.result(&Record::ok("done")).expect("write to memory");
        out.result(&Record::fail("lost").fix("retry"))
            .expect("write to memory");
        out.step(&Record::ok("started"));
        out.step(&Record::warn("careful"));
        assert_eq!(stdout.text(), "  fail  lost\n   fix  retry\n");
        assert_eq!(stderr.text(), "  warn  careful\n");
    }

    #[test]
    fn quiet_keeps_data_fields_and_blocks_and_drops_notes_and_gaps() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        out.data("rc text").expect("write to memory");
        out.fields(&[("log", &"/tmp/server.log")])
            .expect("write to memory");
        out.block(2, "description").expect("write to memory");
        out.gap().expect("write to memory");
        out.note("instruction");
        assert_eq!(stdout.text(), "rc text\n  description\n");
        assert_eq!(stderr.text(), "   log  /tmp/server.log\n");
    }

    #[test]
    fn a_long_message_folds_at_the_terminal_width_with_a_hanging_indent() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        let record =
            Record::fail("several identity providers offer interactive login: google, okta")
                .subject("gang login")
                .fix("add --provider google, or --provider okta");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  fail  gang login: several identity providers offer interactive login: google,\n\
             \x20       okta\n\
             \x20  fix  add --provider google, or --provider okta\n"
        );
    }

    #[test]
    fn a_long_fix_folds_under_its_own_start() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        let record = Record::fail("the gang server printed no ca-pin within 30s")
            .subject("gang local up")
            .fix(
                "read /tmp/gang-local/server/server.log; on a slow machine raise --startup-timeout",
            );
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  fail  gang local up: the gang server printed no ca-pin within 30s\n\
             \x20  fix  read /tmp/gang-local/server/server.log; on a slow machine raise\n\
             \x20       --startup-timeout\n"
        );
    }

    #[test]
    fn a_long_summary_folds_before_the_word_that_does_not_fit() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        let record = Record::fail(
            "/tmp/gang-local/server/server.lock is held by a running gang server or a gang local down in progress",
        )
        .subject("gang local up")
        .fix("gang local down");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  fail  gang local up: /tmp/gang-local/server/server.lock is held by a running\n\
             \x20       gang server or a gang local down in progress\n\
             \x20  fix  gang local down\n"
        );
    }

    #[test]
    fn a_cause_folds_under_the_text_after_its_marker() {
        let (out, _, stderr) = terminal(None, true, Some(24));
        let record = Record::fail("x").cause("alpha beta gamma delta");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  fail  x\n\
             \x20       ↳ alpha beta\n\
             \x20         gamma delta\n"
        );
    }

    #[test]
    fn fields_align_values_on_the_message_column() {
        let (out, stdout, _) = Output::captured();
        out.fields(&[
            ("public", &"https://127.0.0.1:50051"),
            ("worker", &"grpcs://127.0.0.1:9090"),
            ("log", &"/tmp/gang-local/server/server.log"),
        ])
        .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "public  https://127.0.0.1:50051\n\
             worker  grpcs://127.0.0.1:9090\n\
             \x20  log  /tmp/gang-local/server/server.log\n"
        );
    }

    #[test]
    fn field_values_are_displayed_not_padded_or_folded() {
        let (out, stdout, _) = terminal(None, true, Some(20));
        let pin = format!("sha256:{}", "ab".repeat(20));
        out.fields(&[("ca-pin", &pin)]).expect("write to memory");
        assert_eq!(
            stdout.text(),
            format!("ca-pin  sha256:{}\n", "ab".repeat(20))
        );
    }

    #[test]
    fn a_block_is_one_indented_row_when_piped() {
        let (out, stdout, _) = Output::captured();
        out.block(6, "aaa bbb ccc ddd eee")
            .expect("write to memory");
        assert_eq!(stdout.text(), "      aaa bbb ccc ddd eee\n");
    }

    #[test]
    fn a_block_folds_at_the_terminal_width_with_its_indent_on_every_row() {
        let (out, stdout, _) = terminal(None, true, Some(20));
        out.block(6, "aaa bbb ccc ddd eee")
            .expect("write to memory");
        assert_eq!(stdout.text(), "      aaa bbb ccc\n      ddd eee\n");
    }

    #[test]
    fn a_block_keeps_its_lines_and_blank_rows() {
        let (out, stdout, _) = Output::captured();
        out.block(2, "one\n\ntwo").expect("write to memory");
        assert_eq!(stdout.text(), "  one\n\n  two\n");
    }

    #[test]
    fn records_go_to_stderr_once_data_was_written() {
        let (out, stdout, stderr) = Output::captured();
        out.data("[build]").expect("write to memory");
        out.result(&Record::ok("wrote .bazelrc"))
            .expect("write to memory");
        out.fields(&[("log", &"x")]).expect("write to memory");
        out.block(2, "text").expect("write to memory");
        assert_eq!(stdout.text(), "[build]\n  text\n");
        assert_eq!(stderr.text(), "    ok  wrote .bazelrc\n   log  x\n");
    }

    #[test]
    fn a_note_starts_at_the_message_column_and_folds_under_it() {
        let (out, _, stderr) = terminal(None, true, Some(10));
        out.note("continue in the browser");
        assert_eq!(
            stderr.text(),
            "        continue\n        in\n        the\n        browser\n"
        );
    }

    #[test]
    fn data_is_written_verbatim_even_on_a_colored_stream() {
        let (out, stdout, _) = Output::captured_styled(Palette::Truecolor, 80);
        out.data("listening: 127.0.0.1:50051")
            .expect("write to memory");
        assert_eq!(stdout.text(), "listening: 127.0.0.1:50051\n");
    }

    #[test]
    fn gap_writes_an_empty_row_to_the_primary_stream() {
        let (out, stdout, _) = Output::captured();
        out.gap().expect("write to memory");
        assert_eq!(stdout.text(), "\n");
    }

    #[test]
    fn truecolor_styles_the_status_word_only() {
        let (out, stdout, _) = Output::captured_styled(Palette::Truecolor, 80);
        out.result(&Record::ok("logged in"))
            .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "    \x1b[38;2;38;137;70mok\x1b[0m  logged in\n"
        );
    }

    #[test]
    fn a_styled_failure_has_a_plain_subject_a_muted_cause_and_an_accent_fix_label() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        let record = Record::fail("cannot reach the server")
            .subject("gang login")
            .cause("refused")
            .fix("gang local up");
        assert_eq!(
            stepped(&out, &stderr, &record),
            "  \x1b[1m\x1b[31mfail\x1b[0m  gang login: cannot reach the server\n\
             \x20       \x1b[2m↳ refused\x1b[0m\n\
             \x20  \x1b[1m\x1b[32mfix\x1b[0m  gang local up\n"
        );
    }

    #[test]
    fn failure_returns_exit_code_one_and_a_fail_record() {
        let (out, _, stderr) = Output::captured();
        let code = out.failure("gang server", &MainError::NoServer);
        assert_eq!(code, ExitCode::FAILURE);
        let text = stderr.text();
        assert!(text.trim_start().starts_with("fail"), "{text:?}");
        assert!(text.contains("gang server"), "{text:?}");
    }

    #[test]
    fn a_styled_warning_is_bold() {
        let (out, _, stderr) = Output::captured_styled(Palette::Truecolor, 80);
        out.step(&Record::warn("careful"));
        let text = stderr.text();
        let warn = text.find("warn").expect("status written");
        let careful = text.find("careful").expect("message written");
        assert!(text.contains('\u{1b}'), "{text:?}");
        assert!(warn < careful, "{text:?}");
        let (plain, _, plain_err) = Output::captured();
        plain.step(&Record::warn("careful"));
        assert!(!plain_err.text().contains('\u{1b}'));
    }

    #[test]
    fn styled_fields_have_muted_keys() {
        let (out, stdout, _) = Output::captured_styled(Palette::Ansi, 80);
        out.fields(&[("log", &"x")]).expect("write to memory");
        let text = stdout.text();
        let key = text.find("log").expect("key written");
        let value = text.find('x').expect("value written");
        assert!(text.contains('\u{1b}'), "{text:?}");
        assert!(key < value, "{text:?}");
        let (plain, plain_out, _) = Output::captured();
        plain.fields(&[("log", &"x")]).expect("write to memory");
        assert!(!plain_out.text().contains('\u{1b}'));
    }

    #[test]
    fn a_full_mark_has_the_picture_left_of_the_text_rows() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.decoration(
            Some(Mark::Full),
            &[
                Line::new().strong("gang local up").muted("  0.1.0"),
                Line::new().muted("configs/gang.local.toml"),
                Line::new(),
                Line::new().muted("local-worker-x86-64  □□□□  0 of 4"),
            ],
        );
        assert_eq!(
            stderr.text(),
            "   ▄▀▀▄      ▄▀▀▄     gang local up  0.1.0\n\
             \x20 ▄▀  ▀▀▀▀▀▀▀▀  ▀▄    configs/gang.local.toml\n\
             \x20 ██▀███    ███▀██\n\
             \x20  ▀▀▄   ▄▄   ▄▀▀     local-worker-x86-64  □□□□  0 of 4\n\
             \x20     ▀▀█▄▄█▀▀\n"
        );
    }

    #[test]
    fn text_rows_below_the_picture_start_at_the_text_column() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        let lines: Vec<Line> = (1..=6).map(|n| Line::from(format!("row {n}"))).collect();
        out.decoration(Some(Mark::Full), &lines);
        let text = stderr.text();
        let rows: Vec<&str> = text.lines().collect();
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[5], format!("{}row 6", " ".repeat(22)));
        assert!(rows[4].ends_with("row 5"));
    }

    #[test]
    fn the_truecolor_banner_sets_fixed_colors() {
        let (out, _, stderr) = Output::captured_styled(Palette::Truecolor, 80);
        out.decoration(Some(Mark::Full), &[]);
        let text = stderr.text();
        assert_eq!(text.lines().count(), 5);
        assert!(text.starts_with("   \x1b[38;2;139;148;138m▄"));
    }

    #[test]
    fn a_decoration_without_a_mark_writes_its_lines_as_given() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.decoration(None, &[Line::from("a"), Line::new(), Line::from("  b")]);
        assert_eq!(stderr.text(), "a\n\n  b\n");
    }

    #[test]
    fn decorations_vanish_when_stderr_does_not_decorate_or_under_quiet() {
        let (out, _, stderr) = Output::captured();
        out.decoration(Some(Mark::Full), &[Line::from("x")]);
        out.next("gang bazelrc");
        assert_eq!(stderr.text(), "");

        let (out, _, stderr) = terminal(None, true, Some(80));
        out.set_quiet(true);
        out.decoration(Some(Mark::Full), &[Line::from("x")]);
        out.blank();
        out.next("gang bazelrc");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn next_is_a_blank_row_and_a_command_at_the_message_column() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.next("gang bazelrc --server https://127.0.0.1:50051 > .bazelrc");
        assert_eq!(
            stderr.text(),
            "\n  next  gang bazelrc --server https://127.0.0.1:50051 > .bazelrc\n"
        );
    }

    #[test]
    fn stdout_decoration_is_empty_unless_stdout_decorates() {
        let (out, _, _) = Output::captured();
        assert_eq!(out.stdout_decoration(None, &[Line::from("x")]), "");
        let (out, _, _) = terminal(None, true, Some(80));
        assert_eq!(
            out.stdout_decoration(None, &[Line::from("gang 0.1.0")]),
            "gang 0.1.0\n"
        );
    }

    #[test]
    fn stdout_palette_and_stderr_ansi_follow_the_streams() {
        let (out, _, _) = Output::captured();
        assert_eq!(out.stdout_palette(), None);
        assert!(!out.stderr_ansi());
        let (out, _, _) = Output::captured_styled(Palette::Ansi, 80);
        assert_eq!(out.stdout_palette(), Some(Palette::Ansi));
        assert!(out.stderr_ansi());
    }

    #[test]
    fn progress_is_inert_without_a_decorating_stderr() {
        let (out, stdout, stderr) = Output::captured();
        let wait = out.progress("starting the gang server");
        wait.finish();
        assert_eq!(stdout.text(), "");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn the_live_wait_line_is_redrawn_in_place_and_cleared_by_any_other_write() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.shared.draw_wait("waiting", Duration::from_secs(12));
        out.shared.draw_wait("ab", Duration::from_secs(1));
        out.note("hello");
        out.shared.clear_wait();
        assert_eq!(
            stderr.text(),
            format!(
                "\r  wait  waiting · 12s\r  wait  ab · 1s{}\r{}\r        hello\n",
                " ".repeat(6),
                " ".repeat(15)
            )
        );
    }

    #[test]
    fn a_stdout_write_clears_the_live_wait_line_on_stderr() {
        let (out, stdout, stderr) = terminal(None, true, Some(80));
        out.shared.draw_wait("ab", Duration::from_secs(2));
        out.result(&Record::ok("done")).expect("write to memory");
        assert_eq!(stdout.text(), "    ok  done\n");
        assert_eq!(
            stderr.text(),
            format!("\r  wait  ab · 2s\r{}\r", " ".repeat(15))
        );
    }

    #[test]
    fn a_writer_that_fails_on_stdout_is_a_stdout_error() {
        struct Closed;
        impl std::io::Write for Closed {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "closed",
                ))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let look = Look {
            palette: None,
            decorate: false,
        };
        let out = Output::assemble(
            Sink::new(Box::new(Closed), look, None),
            Sink::new(Box::new(Closed), look, None),
        );
        assert!(matches!(
            out.result(&Record::ok("x")),
            Err(crate::errors::MainError::Stdout(_))
        ));
        out.step(&Record::fail("lost"));
    }

    #[test]
    fn elapsed_shows_tenths_of_a_second() {
        assert_eq!(elapsed(Duration::from_millis(2_400)), "2.4s");
        assert_eq!(elapsed(Duration::from_millis(1_149)), "1.1s");
        assert_eq!(elapsed(Duration::from_millis(0)), "0.0s");
        assert_eq!(elapsed(Duration::from_millis(61_999)), "61.9s");
    }

    #[test]
    fn blank_is_an_empty_stderr_row_that_pipes_keep() {
        let (out, _, stderr) = Output::captured();
        out.blank();
        assert_eq!(stderr.text(), "\n");
    }

    #[test]
    fn blank_is_dropped_by_quiet() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.set_quiet(true);
        out.blank();
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn a_skip_step_has_its_word_in_the_margin() {
        let (out, _, stderr) = Output::captured();
        assert_eq!(
            stepped(
                &out,
                &stderr,
                &Record::new(Status::Skip, "needs the server")
            ),
            "  skip  needs the server\n"
        );
    }

    #[test]
    fn a_list_pads_subjects_so_summaries_share_a_column() {
        let (out, stdout, _) = Output::captured();
        out.list_padded(
            19,
            &[
                Record::ok("https://gang.example.com answers").subject("server"),
                Record::ok("trusted by the system roots").subject("tls"),
                Record::ok("gang.example.com=gang").subject("credential_helper"),
            ],
        )
        .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "    ok  server             https://gang.example.com answers\n\
             \x20   ok  tls                trusted by the system roots\n\
             \x20   ok  credential_helper  gang.example.com=gang\n"
        );
    }

    #[test]
    fn a_long_summary_in_a_list_hangs_at_the_summary_column() {
        let (out, stdout, _) = terminal(None, true, Some(40));
        out.list_padded(
            19,
            &[
                Record::ok("alpha beta gamma delta").subject("server"),
                Record::ok("x").subject("credential_helper"),
            ],
        )
        .expect("write to memory");
        assert_eq!(
            stdout.text(),
            format!(
                "    ok  server             alpha beta\n{}gamma delta\n    ok  credential_helper  x\n",
                " ".repeat(27)
            )
        );
    }

    #[test]
    fn a_styled_list_row_keeps_the_subject_plain_and_the_word_colored() {
        let (out, stdout, _) = Output::captured_styled(Palette::Ansi, 80);
        out.list_padded(8, &[Record::ok("answers").subject("server")])
            .expect("write to memory");
        assert_eq!(stdout.text(), "    \x1b[32mok\x1b[0m  server  answers\n");
    }

    #[test]
    fn a_failing_list_item_keeps_its_cause_and_fix_rows() {
        let (out, stdout, _) = Output::captured();
        out.list_padded(
            13,
            &[
                Record::fail("not logged in")
                    .subject("credentials")
                    .cause("no stored login")
                    .fix("gang login --server https://gang.example.com"),
                Record::new(Status::Skip, "needs the server").subject("tls"),
            ],
        )
        .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "  fail  credentials  not logged in\n\
             \x20       ↳ no stored login\n\
             \x20  fix  gang login --server https://gang.example.com\n\
             \x20 skip  tls          needs the server\n"
        );
    }

    #[test]
    fn quiet_drops_ok_and_skip_list_items_without_moving_the_column() {
        let (out, stdout, _) = Output::captured();
        out.set_quiet(true);
        out.list_padded(
            8,
            &[
                Record::ok("answers").subject("server"),
                Record::new(Status::Skip, "needs the server").subject("tls"),
                Record::warn("41s ahead of the server")
                    .subject("clock")
                    .fix("sync this machine's clock"),
            ],
        )
        .expect("write to memory");
        assert_eq!(
            stdout.text(),
            "  warn  clock   41s ahead of the server\n\
             \x20  fix  sync this machine's clock\n"
        );
    }

    #[test]
    fn a_list_goes_to_stderr_once_data_was_written() {
        let (out, stdout, stderr) = Output::captured();
        out.data("x").expect("write to memory");
        out.list_padded(8, &[Record::ok("answers").subject("server")])
            .expect("write to memory");
        assert_eq!(stdout.text(), "x\n");
        assert_eq!(stderr.text(), "    ok  server  answers\n");
    }

    #[test]
    fn an_inline_mark_sits_in_the_margin_and_the_text_starts_at_the_message_column() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.decoration(
            Some(Mark::Inline),
            &[
                Line::from("all 9 checks ok · Bazel is set up for gang.example.com"),
                Line::from("second"),
            ],
        );
        assert_eq!(
            stderr.text(),
            "▀▄▀▀▄▀  all 9 checks ok · Bazel is set up for gang.example.com\n        second\n"
        );
    }

    #[test]
    fn an_inline_mark_alone_is_one_row() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.decoration(Some(Mark::Inline), &[]);
        assert_eq!(stderr.text(), "▀▄▀▀▄▀\n");
    }

    #[test]
    fn a_blank_decoration_row_is_a_terminal_only_blank() {
        let (out, _, stderr) = Output::captured();
        out.decoration(None, &[Line::new()]);
        assert_eq!(stderr.text(), "");
        let (out, _, stderr) = terminal(None, true, Some(80));
        out.decoration(None, &[Line::new()]);
        assert_eq!(stderr.text(), "\n");
    }

    #[test]
    fn a_notice_is_kept_by_quiet_and_a_note_is_not() {
        let (out, _, stderr) = Output::captured();
        out.set_quiet(true);
        out.note("optional");
        out.notice("needed");
        assert_eq!(stderr.text(), "        needed\n");
    }

    #[test]
    fn stdout_width_is_the_width_of_the_stdout_look() {
        assert_eq!(Output::captured().0.stdout_width(), None);
        assert_eq!(terminal(None, true, Some(72)).0.stdout_width(), Some(72));
    }

    #[test]
    fn a_multi_line_cause_is_written_row_by_row() {
        let (out, _, stderr) = terminal(None, true, Some(80));
        let record = Record::fail("cannot parse").cause("bad key\n  |  ^\n");
        out.step(&record);
        let expected = [
            "  fail  cannot parse",
            "        ↳ bad key",
            "            |  ^",
        ];
        assert_eq!(stderr.text(), format!("{}\n", expected.join("\n")));
    }
}

#[cfg(test)]
mod folding {
    use super::{Capture, Line, Look, Mark, Output, Record};
    use crate::mark::{self, BANNER_GAP, BANNER_INDENT};
    use crate::record::COLUMN;
    use crate::wrap::columns;

    /// The terminal widths every folding rule is checked at.
    const WIDTHS: [usize; 4] = [40, 60, 80, 120];

    /// A word no row can hold at these widths.
    const LONG: &str =
        "/Users/someone/.config/gang/credential-helper/state/a-very-long-directory-name";

    /// The column of the summaries of a list whose longest subject is `credential_helper`.
    const SUMMARY_COLUMN: usize = COLUMN + "credential_helper".len() + 2;

    /// Streams that look like terminals `width` columns wide, without color.
    fn terminal(width: usize) -> (Output, Capture, Capture) {
        Output::capture_with(
            Look {
                palette: None,
                decorate: true,
            },
            Some(width),
        )
    }

    /// The rows of written `text`.
    fn lines_of(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    /// The column at which the last blank-separated word of `row` starts.
    fn last_word_start(row: &str) -> usize {
        let row = row.trim_end();
        row.rfind(' ').map_or(0, |at| columns(&row[..=at]))
    }

    /// Asserts the folding rules for `rows` written to a terminal `width` columns wide: no row
    /// starts at column 0, every row that starts at the message column or beyond starts at one of
    /// the `hangs`, and a row wider than the terminal ends in a word that sits at a hang column:
    /// one word that no row could hold.
    fn assert_folded(rows: &[String], width: usize, hangs: &[usize]) {
        for row in rows.iter().filter(|row| !row.is_empty()) {
            assert!(
                row.starts_with(' '),
                "row at column 0 in {width} columns: {row:?}"
            );
            let indent = row.len() - row.trim_start().len();
            if indent >= COLUMN {
                assert!(
                    hangs.contains(&indent),
                    "row at column {indent} in {width} columns: {row:?}"
                );
            }
            if columns(row) > width {
                assert!(
                    hangs.contains(&last_word_start(row)),
                    "row wider than {width} that is not one long word: {row:?}"
                );
            }
        }
    }

    /// Asserts that the words of `text` follow each other unbroken somewhere in `rows`.
    fn assert_words_whole(rows: &[String], text: &str) {
        let written: Vec<&str> = rows.iter().flat_map(|row| row.split_whitespace()).collect();
        let wanted: Vec<&str> = text.split_whitespace().collect();
        assert!(
            written
                .windows(wanted.len())
                .any(|window| window == wanted.as_slice()),
            "words of {text:?} were split in {rows:?}"
        );
    }

    /// Doctor-like records with long summaries, causes and fixes.
    fn doctor_records() -> Vec<Record> {
        vec![
            Record::ok(
                "https://gang.example.com answers on the public listener with a certificate that has eleven days left",
            )
            .subject("server"),
            Record::fail(format!(
                "the credential helper at {LONG} exited with status 1 before it printed a token"
            ))
            .subject("credential_helper")
            .cause(format!(
                "the helper at {LONG} could not read the stored login for the server"
            ))
            .fix(format!(
                "run gang login --server https://gang.example.com/some/very/long/path/{LONG}, then gang doctor again"
            )),
            Record::warn(
                "this machine's clock is 41s ahead of the server and tokens may be rejected",
            )
            .subject("clock")
            .fix("sync the clock of this machine with an NTP server and run gang doctor again"),
        ]
    }

    #[test]
    fn a_doctor_like_list_folds_every_row_under_its_hang_at_every_width() {
        for width in WIDTHS {
            let (out, stdout, _) = terminal(width);
            out.list_padded(SUMMARY_COLUMN - COLUMN, &doctor_records())
                .expect("write to memory");
            let rows = lines_of(&stdout.text());
            assert_folded(&rows, width, &[COLUMN, COLUMN + 2, SUMMARY_COLUMN]);
            assert_words_whole(&rows, LONG);
            assert_words_whole(&rows, "answers on the public listener with a certificate");
            assert_words_whole(&rows, "then gang doctor again");
            assert_words_whole(&rows, "sync the clock of this machine with an NTP server");
        }
    }

    #[test]
    fn rows_written_one_at_a_time_keep_the_column_of_one_call() {
        for width in WIDTHS {
            let records = doctor_records();
            let (out, whole, _) = terminal(width);
            out.list_padded(SUMMARY_COLUMN - COLUMN, &records)
                .expect("write to memory");
            let (out, apart, _) = terminal(width);
            for record in &records {
                out.list_padded(SUMMARY_COLUMN - COLUMN, std::slice::from_ref(record))
                    .expect("write to memory");
            }
            assert_eq!(apart.text(), whole.text());
        }
    }

    #[test]
    fn a_list_padded_wider_than_its_subjects_moves_every_summary() {
        let (out, stdout, _) = terminal(80);
        out.list_padded(10, &[Record::ok("answers").subject("tls")])
            .expect("write to memory");
        assert_eq!(stdout.text(), "    ok  tls       answers\n");
    }

    #[test]
    fn a_long_fix_row_folds_under_the_fix_text_at_every_width() {
        for width in WIDTHS {
            let (out, _, stderr) = terminal(width);
            let fix = format!(
                "run gang login --server https://gang.example.com, then {LONG} and gang doctor again to check the whole setup"
            );
            out.step(
                &Record::fail("not logged in")
                    .subject("gang doctor")
                    .fix(fix.clone()),
            );
            let rows = lines_of(&stderr.text());
            assert_folded(&rows, width, &[COLUMN]);
            assert_words_whole(&rows, &fix);
        }
    }

    #[test]
    fn a_long_cause_and_its_further_lines_fold_under_the_cause_text() {
        for width in WIDTHS {
            let (out, _, stderr) = terminal(width);
            let second = format!(
                "    caused by: the file {LONG} is not readable for this user, so nothing was read"
            );
            out.step(
                &Record::fail("cannot read").cause(format!("first line of the cause\n{second}")),
            );
            let rows = lines_of(&stderr.text());
            assert_folded(&rows, width, &[COLUMN, COLUMN + 2, COLUMN + 6]);
            assert_words_whole(&rows, second.trim());
            assert_words_whole(&rows, "first line of the cause");
        }
    }

    #[test]
    fn a_field_value_folds_at_blanks_under_the_message_column() {
        for width in WIDTHS {
            let (out, stdout, _) = terminal(width);
            let value =
                format!("{LONG} alpha beta gamma delta epsilon zeta eta theta iota kappa lambda");
            out.fields(&[("log", &value), ("pin", &"sha256:abcdef")])
                .expect("write to memory");
            let rows = lines_of(&stdout.text());
            assert_folded(&rows, width, &[COLUMN]);
            assert_words_whole(&rows, &value);
            assert!(rows[0].starts_with(&format!("   log  {LONG}")), "{rows:?}");
            assert_eq!(
                rows.last().map(String::as_str),
                Some("   pin  sha256:abcdef")
            );
        }
    }

    #[test]
    fn a_field_value_of_one_long_word_stays_whole_on_its_own_row() {
        let (out, stdout, _) = terminal(40);
        out.fields(&[("log", &LONG)]).expect("write to memory");
        assert_eq!(stdout.text(), format!("   log  {LONG}\n"));
    }

    #[test]
    fn data_is_one_row_on_a_narrow_terminal() {
        let (out, stdout, _) = terminal(20);
        let line = "x".repeat(200);
        out.data(line.as_str()).expect("write to memory");
        assert_eq!(stdout.text(), format!("{line}\n"));
    }

    #[test]
    fn a_field_value_with_several_lines_keeps_each_line_at_the_message_column() {
        let (out, stdout, _) = Output::captured();
        out.fields(&[("log", &"one\ntwo")])
            .expect("write to memory");
        assert_eq!(stdout.text(), "   log  one\n        two\n");
    }

    #[test]
    fn field_values_preserve_blank_continuation_rows() {
        let (out, stdout, _) = Output::captured();
        out.fields(&[("log", &"one\n\n \t\ntwo")])
            .expect("write to memory");
        assert_eq!(stdout.text(), "   log  one\n\n\n        two\n");
    }

    #[test]
    fn a_row_exactly_as_wide_as_the_terminal_stays_whole_and_one_column_more_folds() {
        for width in WIDTHS {
            let room = width - COLUMN;
            let exact = format!("{} {}", "a".repeat(10), "b".repeat(room - 11));
            let (out, _, stderr) = terminal(width);
            out.step(&Record::fail(exact));
            let rows = lines_of(&stderr.text());
            assert_eq!(rows.len(), 1, "{rows:?}");
            assert_eq!(columns(&rows[0]), width);

            let over = format!("{} {}", "a".repeat(10), "b".repeat(room - 10));
            let (out, _, stderr) = terminal(width);
            out.step(&Record::fail(over));
            let rows = lines_of(&stderr.text());
            assert_eq!(rows.len(), 2, "{rows:?}");
            assert!(rows.iter().all(|row| columns(row) <= width), "{rows:?}");
            assert!(rows[1].starts_with("        b"), "{rows:?}");
        }
    }

    #[test]
    fn rows_are_measured_in_columns_not_bytes() {
        let (out, _, stderr) = terminal(24);
        out.step(&Record::fail("日本語 日本語 日本語"));
        assert_eq!(stderr.text(), "  fail  日本語 日本語\n        日本語\n");
    }

    #[test]
    fn a_note_folds_under_the_message_column_and_keeps_an_address_whole() {
        for width in WIDTHS {
            let (out, _, stderr) = terminal(width);
            let sentence =
                "browser on another machine? paste the address it fails to load here, then wait";
            let address = format!("https://gang.example.com/login?state={LONG}");
            out.note(Line::new().muted(sentence));
            out.notice(Line::from(address.as_str()));
            out.notice(Line::from("log in at:"));
            let rows = lines_of(&stderr.text());
            assert_folded(&rows, width, &[COLUMN]);
            assert_words_whole(&rows, sentence);
            assert!(rows.iter().any(|row| row.trim() == address), "{rows:?}");
        }
    }

    #[test]
    fn a_decoration_without_a_mark_folds_under_its_own_indent() {
        for width in WIDTHS {
            let (out, _, stderr) = terminal(width);
            let hint = "gang get is Bazel's credential helper: it reads Bazel's request on stdin, so run gang doctor to check your credentials.";
            out.decoration(None, &[Line::new().plain(" ".repeat(COLUMN)).muted(hint)]);
            out.decoration(None, &[Line::new().plain("  ").muted(hint)]);
            let rows = lines_of(&stderr.text());
            assert_folded(&rows, width, &[COLUMN, 2]);
            assert_words_whole(&rows, hint);
            assert!(rows.iter().all(|row| columns(row) <= width), "{rows:?}");
        }
    }

    #[test]
    fn text_beside_a_mark_folds_at_the_text_column() {
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
        let (out, _, stderr) = terminal(48);
        out.decoration(Some(Mark::Full), &[Line::from(text)]);
        let rows = lines_of(&stderr.text());
        assert!(rows.iter().all(|row| columns(row) <= 48), "{rows:?}");
        let column = BANNER_INDENT + mark::banner_width() + BANNER_GAP;
        let beside: Vec<String> = rows
            .iter()
            .map(|row| row.chars().skip(column).collect::<String>())
            .collect();
        let words: Vec<&str> = beside
            .iter()
            .flat_map(|row| row.split_whitespace())
            .collect();
        assert_eq!(words.join(" "), text);

        let (out, _, stderr) = terminal(30);
        out.decoration(
            Some(Mark::Inline),
            &[Line::from("alpha beta gamma delta epsilon zeta")],
        );
        assert_eq!(
            stderr.text(),
            "▀▄▀▀▄▀  alpha beta gamma delta\n        epsilon zeta\n"
        );
    }

    #[test]
    fn the_next_command_is_never_folded() {
        let (out, _, stderr) = terminal(40);
        let command = "gang bazelrc --server https://gang.example.com:50051 --ca-pin sha256:abcdef > .bazelrc";
        out.next(command);
        assert_eq!(stderr.text(), format!("\n  next  {command}\n"));
    }

    #[test]
    fn a_block_folds_under_its_indent_at_every_width() {
        for width in WIDTHS {
            let (out, stdout, _) = terminal(width);
            let text = format!(
                "alpha beta gamma delta {LONG} epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma"
            );
            out.block(6, &text).expect("write to memory");
            let rows = lines_of(&stdout.text());
            assert_folded(&rows, width, &[6]);
            assert_words_whole(&rows, &text);
            assert!(rows.iter().all(|row| row.starts_with("      ")));
        }
    }
}
