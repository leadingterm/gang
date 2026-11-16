//! Byte-preserving parser for Bazel rc entries.
//!
//! The rules are those of the Bazel client, which reads rc files before the server sees them
//! (`src/main/cpp/rc_file.cc` and `Tokenize` in `src/main/cpp/util/strings.cc`): a `\` before a
//! line break joins the lines, each line is stripped of surrounding whitespace, words are separated
//! by space, tab, carriage return or line feed, `#` outside quotes starts a comment, `\` takes the
//! next byte literally everywhere (inside single quotes too), quotes are removed, and a word that
//! ends up empty is dropped.

use std::ops::Range;

/// Bytes that separate words: `kSeparator` in Bazel's `strings.cc`.
const SEPARATORS: &[u8] = b" \n\t\r";
/// Bytes stripped from both ends of a line: `ascii_isspace` in Bazel's `strings.h`.
const WHITESPACE: &[u8] = b" \t\n\x0b\x0c\r";
/// Starts a comment outside quotes.
const COMMENT: u8 = b'#';

/// A parsed rc file.
#[derive(Debug)]
pub(crate) struct RcFile {
    /// Logical entries that hold at least one word.
    pub(crate) entries: Vec<Entry>,
}

/// One logical rc entry and its original byte spans.
#[derive(Debug)]
pub(crate) struct Entry {
    /// Complete logical entry, including its terminating newline.
    pub(crate) span: Range<usize>,
    /// First physical line, numbered from one.
    pub(crate) line: usize,
    /// Words including the section.
    pub(crate) tokens: Vec<Token>,
}

/// A word's original bytes and its value as Bazel reads it.
#[derive(Debug)]
pub(crate) struct Token {
    /// Span of every source byte the word consumed, quotes and escapes included.
    pub(crate) span: Range<usize>,
    /// Unquoted, unescaped value.
    pub(crate) value: Vec<u8>,
}

impl RcFile {
    /// Parse logical entries without changing any source bytes.
    pub(crate) fn parse(contents: &[u8]) -> Self {
        let mut entries = Vec::new();
        let mut start = 0;
        let mut line = 1;
        while start < contents.len() {
            let (end, physical) = logical_end(contents, start);
            let tokens = tokens(contents, start..end);
            if !tokens.is_empty() {
                entries.push(Entry {
                    span: start..end,
                    line,
                    tokens,
                });
            }
            start = end;
            line += physical;
        }
        Self { entries }
    }
}

/// The end of the logical entry starting at `start`, after its terminating newline, and the
/// number of physical lines it spans. A line whose last byte before `\n` or `\r\n` is `\`
/// continues onto the next one.
fn logical_end(contents: &[u8], start: usize) -> (usize, usize) {
    let mut line_start = start;
    let mut physical = 0;
    loop {
        physical += 1;
        let Some(offset) = contents[line_start..].iter().position(|b| *b == b'\n') else {
            return (contents.len(), physical);
        };
        let newline = line_start + offset;
        let body = &contents[line_start..newline];
        let body = body.strip_suffix(b"\r").unwrap_or(body);
        line_start = newline + 1;
        if !body.ends_with(b"\\") || line_start == contents.len() {
            return (line_start, physical);
        }
    }
}

/// Splits the entry at `span` into words.
fn tokens(contents: &[u8], span: Range<usize>) -> Vec<Token> {
    // Bazel removes every `\` + line break before splitting lines; `source` holds the offsets of
    // the bytes that remain.
    let mut source = Vec::with_capacity(span.len());
    let mut i = span.start;
    while i < span.end {
        if contents[i] == b'\\' {
            if i + 1 < span.end && contents[i + 1] == b'\n' {
                i += 2;
                continue;
            }
            if i + 2 < span.end && contents[i + 1..i + 3] == *b"\r\n" {
                i += 3;
                continue;
            }
        }
        source.push(i);
        i += 1;
    }
    let byte = |k: usize| contents[source[k]];
    let mut first = 0;
    let mut end = source.len();
    while first < end && WHITESPACE.contains(&byte(first)) {
        first += 1;
    }
    while end > first && WHITESPACE.contains(&byte(end - 1)) {
        end -= 1;
    }

    let mut result = Vec::new();
    let mut k = first;
    while k < end {
        while k < end && SEPARATORS.contains(&byte(k)) {
            k += 1;
        }
        if k == end || byte(k) == COMMENT {
            break;
        }
        let start = k;
        let mut value = Vec::new();
        let mut quote = None;
        let mut comment = false;
        while k < end && (quote.is_some() || !SEPARATORS.contains(&byte(k))) {
            let b = byte(k);
            k += 1;
            if b == b'\\' {
                if k == end {
                    break;
                }
                value.push(byte(k));
                k += 1;
                continue;
            }
            match quote {
                Some(open) if b == open => quote = None,
                None if b == COMMENT => {
                    k -= 1;
                    comment = true;
                    break;
                }
                None if b == b'\'' || b == b'"' => quote = Some(b),
                Some(_) | None => value.push(b),
            }
        }
        if !value.is_empty() {
            result.push(Token {
                span: source[start]..source[k - 1] + 1,
                value,
            });
        }
        if comment {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values of every entry's words, as text.
    fn words(contents: &[u8]) -> Vec<Vec<String>> {
        RcFile::parse(contents)
            .entries
            .iter()
            .map(|entry| {
                entry
                    .tokens
                    .iter()
                    .map(|token| String::from_utf8_lossy(&token.value).into_owned())
                    .collect()
            })
            .collect()
    }

    /// Quotes are removed, and `\` escapes the next byte inside single and double quotes alike.
    #[test]
    fn quotes_and_escapes_follow_the_bazel_client() {
        assert_eq!(
            words(br#"build '--remote_header=X=a\b' "--a=b c" --d=\"e\" ''"#),
            [["build", "--remote_header=X=ab", "--a=b c", "--d=\"e\""]],
            "rc words must follow Bazel's client tokenizer"
        );
        assert_eq!(words(br"build 'foo\'"), [["build", "foo'"]]);
    }

    /// A `\` line break joins lines before comments are stripped, as the Bazel client does.
    #[test]
    fn continuations_join_before_comments() {
        let parsed = RcFile::parse(b"build --a # note \\\n--hidden\nbuild --b\\\\\n--c\n");
        let lines: Vec<usize> = parsed.entries.iter().map(|entry| entry.line).collect();
        assert_eq!(lines, [1, 3]);
        assert_eq!(
            words(b"build --a # note \\\n--hidden\nbuild --b\\\\\n--c\n"),
            [vec!["build", "--a"], vec!["build", "--b--c"]]
        );
        assert_eq!(
            words(b"build \\\r\n  --x=1\r\n\x0cbuild --y\\"),
            [["build", "--x=1"], ["build", "--y"]]
        );
    }

    /// A non-UTF-8 word keeps its entry, so the other words of the entry stay visible.
    #[test]
    fn non_utf8_words_keep_their_entry() {
        let parsed = RcFile::parse(b"build --remote_header=A=b --path=\xff\n");
        assert_eq!(
            parsed.entries.len(),
            1,
            "a non-UTF-8 word must not drop its entry"
        );
        assert_eq!(parsed.entries[0].tokens[1].value, b"--remote_header=A=b");
        assert_eq!(parsed.entries[0].tokens[2].value, b"--path=\xff");
    }

    /// Spans cover exactly the consumed source bytes, quotes and continuations included.
    #[test]
    fn spans_cover_the_source_words() {
        let contents = b"build \"--a=b c\" --x\\\n=y # c\n";
        let parsed = RcFile::parse(contents);
        let spans: Vec<&[u8]> = parsed.entries[0]
            .tokens
            .iter()
            .map(|token| &contents[token.span.clone()])
            .collect();
        assert_eq!(spans, [&b"build"[..], b"\"--a=b c\"", b"--x\\\n=y"]);
        assert_eq!(parsed.entries[0].span, 0..contents.len());
    }
}
