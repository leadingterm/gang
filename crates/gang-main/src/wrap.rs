//! Greedy word wrapping of styled lines and of plain text, counted in terminal columns.
//!
//! Rows break at blanks only and drop the blanks at a break; a word wider than a row gets a row
//! of its own and is never split. Non-breaking spaces belong to their word.

use std::{borrow::Cow, mem};

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::palette::Role;
use crate::record::Line;

/// Whether `c` is a blank a row may break at: whitespace that is not a no-break space.
fn is_blank(c: char) -> bool {
    c.is_whitespace() && !matches!(c, '\u{a0}' | '\u{2007}' | '\u{202f}')
}

/// Applies `lead` to the first of `rows` and `rest` to the others, in order; empty input gives
/// empty output.
fn lead_then<T, U>(rows: Vec<T>, lead: impl FnOnce(T) -> U, rest: impl FnMut(T) -> U) -> Vec<U> {
    let mut rows = rows.into_iter();
    let Some(first) = rows.next() else {
        return Vec::new();
    };
    std::iter::once(lead(first)).chain(rows.map(rest)).collect()
}

/// Columns `c` occupies on a terminal: 0 for controls and combining marks, 2 for wide East Asian
/// and emoji characters, 1 otherwise.
pub(crate) fn char_columns(c: char) -> usize {
    c.width().unwrap_or(0)
}

/// Columns `text` occupies on a terminal.
pub(crate) fn columns(text: &str) -> usize {
    text.width()
}

/// A word or a run of blanks, in one role.
struct Token<'a> {
    /// What the text means.
    role: Role,
    /// The text; blanks are normalized to spaces.
    text: Cow<'a, str>,
    /// Whether this is a run of blanks.
    blank: bool,
}

/// Splits a line into alternating words and runs of blanks.
fn tokens(line: &Line) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    for (role, text) in line.spans() {
        let mut rest = text;
        while let Some(first) = rest.chars().next() {
            let blank = is_blank(first);
            let end = rest
                .find(|c: char| is_blank(c) != blank)
                .unwrap_or(rest.len());
            let (head, tail) = rest.split_at(end);
            tokens.push(Token {
                role,
                text: if blank && !head.bytes().all(|byte| byte == b' ') {
                    Cow::Owned(" ".repeat(head.chars().count()))
                } else {
                    Cow::Borrowed(head)
                },
                blank,
            });
            rest = tail;
        }
    }
    tokens
}

/// Total width of tokens.
fn tokens_width(tokens: &[Token<'_>]) -> usize {
    tokens.iter().map(|token| columns(&token.text)).sum()
}

/// Greedy word wrap state.
struct Folder {
    /// Columns available on the first row.
    first: usize,
    /// Columns available on every later row.
    rest: usize,
    /// Finished rows.
    rows: Vec<Line>,
    /// The row being filled.
    row: Line,
    /// Columns used in `row`.
    col: usize,
}

impl Folder {
    /// Columns available on the row being filled.
    fn avail(&self) -> usize {
        if self.rows.is_empty() {
            self.first
        } else {
            self.rest
        }
    }

    /// Places `word` after `gap`, on a new row when it does not fit.
    fn place(&mut self, gap: &[Token<'_>], word: &[Token<'_>]) {
        if word.is_empty() {
            return;
        }
        let gap_width = tokens_width(gap);
        let word_width = tokens_width(word);
        if self.col > 0 && self.col + gap_width + word_width > self.avail() {
            self.rows.push(mem::take(&mut self.row));
            self.col = 0;
        }
        if self.col > 0 || self.rows.is_empty() {
            for token in gap {
                self.row = mem::take(&mut self.row).span(token.role, token.text.as_ref());
            }
            self.col += gap_width;
        }
        for token in word {
            self.row = mem::take(&mut self.row).span(token.role, token.text.as_ref());
        }
        self.col += word_width;
    }

    /// The rows, with the last one included.
    fn finish(mut self) -> Vec<Line> {
        if !self.row.is_empty() || self.rows.is_empty() {
            self.rows.push(self.row);
        }
        self.rows
    }
}

/// Wraps `line` into rows of at most `first` columns for the first row and `rest` for the others.
fn wrap(line: &Line, first: usize, rest: usize) -> Vec<Line> {
    let mut folder = Folder {
        first,
        rest,
        rows: Vec::new(),
        row: Line::new(),
        col: 0,
    };
    let mut gap = Vec::new();
    let mut word = Vec::new();
    for token in tokens(line) {
        if token.blank {
            if !word.is_empty() {
                folder.place(&gap, &word);
                gap.clear();
                word.clear();
            }
            gap.push(token);
        } else {
            word.push(token);
        }
    }
    folder.place(&gap, &word);
    folder.finish()
}

/// `line` folded to `width` columns for text that starts at column `indent`, or the line itself
/// when there is no width to fold to.
pub(crate) fn fold(line: &Line, width: Option<usize>, indent: usize) -> Vec<Line> {
    width.map_or_else(
        || vec![line.clone()],
        |width| {
            let avail = width.saturating_sub(indent).max(1);
            wrap(line, avail, avail)
        },
    )
}

/// Puts `lead` in front of the first row of `body` and indents the others by `indent` columns.
pub(crate) fn hang(lead: &Line, indent: usize, body: Vec<Line>) -> Vec<Line> {
    lead_then(
        body,
        |row| lead.clone().append(row),
        |row| row.indented(indent),
    )
}

/// Folds and hangs the first line on `lead` and later lines under `indent`.
/// Whitespace-only continuation lines remain empty rows.
pub(crate) fn hang_multiline(
    lead: &Line,
    indent: usize,
    width: Option<usize>,
    lines: impl IntoIterator<Item = Line>,
) -> Vec<Line> {
    let mut lines = lines.into_iter();
    let first = lines.next().unwrap_or_default();
    let mut rows = hang(lead, indent, fold(&first, width, indent));
    let continuation = Line::new().pad(indent);
    for line in lines {
        if line.spans().all(|(_, text)| text.trim().is_empty()) {
            rows.push(Line::new());
        } else {
            rows.extend(hang(&continuation, indent, fold(&line, width, indent)));
        }
    }
    rows
}

/// `line`, whose own leading blanks are its indent, folded to `width` columns with every later
/// row hanging under that indent; the line itself when there is no width to fold to.
pub(crate) fn fold_indented(line: &Line, width: Option<usize>) -> Vec<Line> {
    let Some(width) = width else {
        return vec![line.clone()];
    };
    let indent = line
        .spans()
        .flat_map(|(_, text)| text.chars())
        .take_while(|&c| is_blank(c))
        .count();
    let rest = width.saturating_sub(indent).max(1);
    lead_then(
        wrap(line, width.max(1), rest),
        |row| row,
        |row| row.indented(indent),
    )
}

/// Plain `text` wrapped to lines of at most `width` characters, the indentation included: the
/// first line is indented by `first_indent` spaces and every later one by `hang_indent`. Without
/// a `width` the text is one line. A word longer than a line keeps a line of its own.
pub(crate) fn wrap_text(
    text: &str,
    first_indent: usize,
    hang_indent: usize,
    width: Option<usize>,
) -> Vec<String> {
    let line = Line::from(text);
    let rows = width.map_or_else(
        || vec![line.clone()],
        |width| {
            wrap(
                &line,
                width.saturating_sub(first_indent).max(1),
                width.saturating_sub(hang_indent).max(1),
            )
        },
    );
    let render = |row: Line, indent: usize| {
        if row.is_empty() {
            String::new()
        } else {
            format!("{:indent$}{}", "", row.text())
        }
    };
    lead_then(
        rows,
        |row| render(row, first_indent),
        |row| render(row, hang_indent),
    )
}

#[cfg(test)]
mod tests {
    use crate::palette::Palette;
    use crate::record::Line;

    use super::{columns, fold, fold_indented, hang, wrap, wrap_text};

    fn texts(rows: &[Line]) -> Vec<String> {
        rows.iter().map(Line::text).collect()
    }

    #[test]
    fn tokens_borrow_words_and_spaces_and_own_normalized_tabs() {
        let line = Line::from("alpha  beta\tgamma");
        let tokens = super::tokens(&line);
        assert!(matches!(
            &tokens[0].text,
            std::borrow::Cow::Borrowed("alpha")
        ));
        assert!(matches!(&tokens[1].text, std::borrow::Cow::Borrowed("  ")));
        assert!(matches!(
            &tokens[2].text,
            std::borrow::Cow::Borrowed("beta")
        ));
        assert!(matches!(&tokens[3].text, std::borrow::Cow::Owned(text) if text == " "));
        assert!(matches!(
            &tokens[4].text,
            std::borrow::Cow::Borrowed("gamma")
        ));
    }

    #[test]
    fn fold_normalizes_a_tab_to_a_space() {
        let rows = fold(&Line::from("alpha\tbeta"), Some(20), 0);
        assert_eq!(texts(&rows), ["alpha beta"]);
    }

    #[test]
    fn wrap_never_breaks_a_word_longer_than_the_width() {
        let rows = wrap(&Line::from("aa bbbbbbbbbbbb cc"), 5, 5);
        assert_eq!(texts(&rows), ["aa", "bbbbbbbbbbbb", "cc"]);
    }

    #[test]
    fn wrap_fills_each_row_up_to_the_width() {
        let rows = wrap(&Line::from("alpha beta gamma"), 10, 10);
        assert_eq!(texts(&rows), ["alpha beta", "gamma"]);
    }

    #[test]
    fn wrap_gives_the_first_row_its_own_width() {
        let rows = wrap(&Line::from("alpha beta gamma delta"), 10, 5);
        assert_eq!(texts(&rows), ["alpha beta", "gamma", "delta"]);
    }

    #[test]
    fn wrap_keeps_the_roles_of_spans_that_meet_inside_a_word() {
        let line = Line::new().strong("ab ").plain("cd").muted("ef gh");
        let rows = wrap(&line, 7, 7);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].paint(Some(Palette::Ansi)),
            "\x1b[1mab \x1b[0mcd\x1b[2mef\x1b[0m"
        );
        assert_eq!(texts(&rows), ["ab cdef", "gh"]);
    }

    #[test]
    fn wrap_of_an_empty_line_is_one_empty_row() {
        assert_eq!(texts(&wrap(&Line::new(), 5, 5)), [""]);
    }

    #[test]
    fn fold_without_a_width_keeps_the_line() {
        let rows = fold(&Line::from("alpha beta gamma"), None, 8);
        assert_eq!(texts(&rows), ["alpha beta gamma"]);
    }

    #[test]
    fn fold_wraps_at_the_width_minus_the_indent() {
        let rows = fold(&Line::from("alpha beta gamma"), Some(18), 8);
        assert_eq!(texts(&rows), ["alpha beta", "gamma"]);
    }

    #[test]
    fn hang_leads_the_first_row_and_indents_the_rest() {
        let rows = hang(
            &Line::from("lead "),
            4,
            vec![Line::from("one"), Line::from("two")],
        );
        assert_eq!(texts(&rows), ["lead one", "    two"]);
    }

    #[test]
    fn wrap_text_indents_the_first_line_and_the_hanging_lines_separately() {
        assert_eq!(
            wrap_text("alpha beta gamma", 2, 4, Some(12)),
            ["  alpha beta", "    gamma"]
        );
    }

    #[test]
    fn wrap_text_without_a_width_is_one_indented_line() {
        assert_eq!(
            wrap_text("alpha beta gamma", 2, 4, None),
            ["  alpha beta gamma"]
        );
    }

    #[test]
    fn wrap_text_keeps_a_long_word_on_its_own_line() {
        assert_eq!(
            wrap_text("a bbbbbbbbbbbb c", 0, 0, Some(5)),
            ["a", "bbbbbbbbbbbb", "c"]
        );
    }

    #[test]
    fn wrap_text_of_nothing_is_one_empty_line() {
        assert_eq!(wrap_text("", 3, 3, Some(10)), [""]);
    }

    #[test]
    fn columns_count_wide_characters_twice_and_combining_marks_not_at_all() {
        assert_eq!(columns("abc"), 3);
        assert_eq!(columns("日本語"), 6);
        assert_eq!(columns("e\u{301}"), 1);
        assert_eq!(columns("↳ ·…"), 4);
    }

    #[test]
    fn columns_of_the_check_mark_is_two() {
        assert_eq!(columns("\u{2705}"), 2);
    }

    #[test]
    fn columns_of_a_hebrew_letter_with_a_vowel_point_is_one() {
        assert_eq!(columns("\u{5d1}\u{5b0}"), 1);
    }

    #[test]
    fn wrap_counts_a_check_mark_by_its_real_width() {
        let rows = wrap(&Line::from("\u{2705}\u{2705} ab"), 6, 6);
        assert_eq!(texts(&rows), ["\u{2705}\u{2705}", "ab"]);
    }

    #[test]
    fn wrap_counts_columns_not_characters() {
        let rows = wrap(&Line::from("日本語 日本語"), 13, 13);
        assert_eq!(texts(&rows), ["日本語 日本語"]);
        let rows = wrap(&Line::from("日本語 日本語"), 12, 12);
        assert_eq!(texts(&rows), ["日本語", "日本語"]);
    }

    #[test]
    fn wrap_breaks_at_spaces_only_never_at_a_no_break_space() {
        let rows = wrap(&Line::from("alpha\u{a0}beta gamma"), 8, 8);
        assert_eq!(texts(&rows), ["alpha\u{a0}beta", "gamma"]);
    }

    #[test]
    fn fold_indented_hangs_later_rows_under_the_leading_blanks() {
        let rows = fold_indented(&Line::from("    alpha beta gamma"), Some(14));
        assert_eq!(texts(&rows), ["    alpha beta", "    gamma"]);
        let rows = fold_indented(&Line::from("alpha beta"), None);
        assert_eq!(texts(&rows), ["alpha beta"]);
    }
}
