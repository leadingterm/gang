//! Preview the exact file changes planned by setup.

use std::fmt::Write as _;

use gang_client::{Change, GANG_CA, GANG_DIR};

use crate::record::Line;

/// Number of unchanged lines shown on either side of an edit.
const CONTEXT: usize = 3;

/// Renders a change with its path, operation, backup, and a compact line diff.
pub(crate) fn preview(change: &Change, full: bool) -> Vec<Line> {
    if !full
        && change.path.file_name().is_some_and(|name| name == GANG_CA)
        && change
            .path
            .parent()
            .and_then(|parent| parent.file_name())
            .is_some_and(|name| name == GANG_DIR)
    {
        let operation = if change.before.is_none() {
            "new file"
        } else {
            "edit"
        };
        return vec![Line::new().strong(format!(
            "{} ({operation}, CA certificate, {} bytes)",
            change.path.display(),
            change.after.len()
        ))];
    }
    let old_lines: Vec<&[u8]> = change
        .before
        .as_deref()
        .unwrap_or_default()
        .split_inclusive(|b| *b == b'\n')
        .collect();
    let new_lines: Vec<&[u8]> = change.after.split_inclusive(|b| *b == b'\n').collect();
    let prefix = old_lines
        .iter()
        .zip(&new_lines)
        .take_while(|(a, b)| a == b)
        .count();
    let suffix = old_lines[prefix..]
        .iter()
        .rev()
        .zip(new_lines[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let operation = if change.before.is_none() {
        "new file"
    } else if change
        .after
        .starts_with(change.before.as_deref().unwrap_or_default())
    {
        "append"
    } else {
        "edit"
    };
    let mut header = format!("{} ({operation}", change.path.display());
    if let Some(backup) = &change.backup {
        let _ = write!(header, ", backup {}", backup.display());
    }
    header.push(')');
    let mut lines = vec![Line::new().strong(header)];
    let all = full || change.before.is_none();
    let start = if all {
        0
    } else {
        prefix.saturating_sub(CONTEXT)
    };
    let end = if all { suffix } else { suffix.min(CONTEXT) };
    for line in &old_lines[start..prefix] {
        push_line(&mut lines, "  ", line);
    }
    for line in &old_lines[prefix..old_lines.len() - suffix] {
        push_line(&mut lines, "- ", line);
    }
    for line in &new_lines[prefix..new_lines.len() - suffix] {
        push_line(&mut lines, "+ ", line);
    }
    for line in &new_lines[new_lines.len() - suffix..new_lines.len() - suffix + end] {
        push_line(&mut lines, "  ", line);
    }
    lines
}

/// Renders a byte line, preserving visible CR and missing-newline information.
fn push_line(lines: &mut Vec<Line>, prefix: &str, bytes: &[u8]) {
    let terminated = bytes.ends_with(b"\n");
    let text = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let (text, carriage_return) = match text.strip_suffix(b"\r") {
        Some(text) => (text, "\\r"),
        None => (text, ""),
    };
    let rendered = format!("{prefix}{}{carriage_return}", String::from_utf8_lossy(text));
    lines.push(if prefix == "+ " {
        Line::new().plain(rendered)
    } else {
        Line::new().muted(rendered)
    });
    if !terminated {
        lines.push(Line::new().muted("\\ no newline at end of file"));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Builds an append with more than three lines of preceding context.
    fn append() -> Change {
        Change {
            path: PathBuf::from(".bazelrc"),
            before: Some(b"one\ntwo\nthree\nfour\nfive\n".to_vec()),
            after: b"one\ntwo\nthree\nfour\nfive\nsix\nseven\n".to_vec(),
            backup: None,
        }
    }

    #[test]
    fn append_shows_only_three_context_lines() {
        let rows: Vec<String> = preview(&append(), false).iter().map(Line::text).collect();
        assert_eq!(
            rows,
            [
                ".bazelrc (append)",
                "  three",
                "  four",
                "  five",
                "+ six",
                "+ seven"
            ]
        );
    }

    #[test]
    fn full_shows_all_context() {
        let rows: Vec<String> = preview(&append(), true).iter().map(Line::text).collect();
        assert_eq!(
            rows,
            [
                ".bazelrc (append)",
                "  one",
                "  two",
                "  three",
                "  four",
                "  five",
                "+ six",
                "+ seven"
            ]
        );
    }

    #[test]
    fn edit_shows_removed_added_and_backup() {
        let change = Change {
            path: PathBuf::from("rc"),
            before: Some(b"a\nb\nc\n".to_vec()),
            after: b"a\nx\nc\n".to_vec(),
            backup: Some(PathBuf::from("rc.bak")),
        };
        let rows: Vec<String> = preview(&change, false).iter().map(Line::text).collect();
        assert_eq!(
            rows,
            ["rc (edit, backup rc.bak)", "  a", "- b", "+ x", "  c"]
        );
    }

    #[test]
    fn new_file_shows_every_added_line() {
        let change = Change {
            path: PathBuf::from("new"),
            before: None,
            after: b"one\ntwo\n".to_vec(),
            backup: None,
        };
        let rows: Vec<String> = preview(&change, false).iter().map(Line::text).collect();
        assert_eq!(rows, ["new (new file)", "+ one", "+ two"]);
    }

    /// Compact certificate previews summarize; full previews show the PEM.
    #[test]
    fn certificate_is_summarized_in_compact_and_shown_in_full_preview() {
        for before in [None, Some(b"old certificate\n".to_vec())] {
            let operation = if before.is_none() { "new file" } else { "edit" };
            let change = Change {
                path: PathBuf::from("/workspace/.gang/ca.pem"),
                before,
                after: b"-----BEGIN CERTIFICATE-----\nsecret\n-----END CERTIFICATE-----\n".to_vec(),
                backup: None,
            };
            let compact: Vec<String> = preview(&change, false).iter().map(Line::text).collect();
            assert_eq!(
                compact,
                [format!(
                    "/workspace/.gang/ca.pem ({operation}, CA certificate, {} bytes)",
                    change.after.len()
                )]
            );
            let full: Vec<String> = preview(&change, true).iter().map(Line::text).collect();
            assert!(
                full.ends_with(&[
                    "+ -----BEGIN CERTIFICATE-----".to_owned(),
                    "+ secret".to_owned(),
                    "+ -----END CERTIFICATE-----".to_owned(),
                ]),
                "full certificate preview must show each PEM line"
            );
        }
    }

    /// Appending a newline changes the old unterminated line as well.
    #[test]
    fn append_shows_missing_eof_newline() {
        let change = Change {
            path: PathBuf::from(".bazelrc"),
            before: Some(b"build --foo".to_vec()),
            after: b"build --foo\n# managed\ntry-import %workspace%/.gang/bazelrc\n".to_vec(),
            backup: None,
        };
        let rows: Vec<String> = preview(&change, false).iter().map(Line::text).collect();
        assert!(
            rows == [
                ".bazelrc (append)",
                "- build --foo",
                "\\ no newline at end of file",
                "+ build --foo",
                "+ # managed",
                "+ try-import %workspace%/.gang/bazelrc",
            ],
            "append preview must show the missing EOF newline and changed line"
        );
    }

    /// A CRLF-to-LF edit is visible rather than treated as unchanged.
    #[test]
    fn crlf_to_lf_is_a_change() {
        let change = Change {
            path: PathBuf::from("rc"),
            before: Some(b"build --foo\r\n".to_vec()),
            after: b"build --foo\n".to_vec(),
            backup: None,
        };
        let rows: Vec<String> = preview(&change, false).iter().map(Line::text).collect();
        assert!(
            rows == ["rc (edit)", "- build --foo\\r", "+ build --foo"],
            "CRLF-to-LF preview must show the visible carriage return and replacement"
        );
    }
}
