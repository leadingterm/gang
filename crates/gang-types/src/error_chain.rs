//! One sentence for an error and its source chain, without repeating text the message already has.

use std::error::Error;
use std::fmt;

/// An error message joined with the causes from its source chain.
///
/// Build one with [`error_chain`]. [`Display`](fmt::Display) writes the whole sentence, in the form
/// `summary: cause: cause`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorChain {
    /// The error's own message, before any cause is appended.
    summary: String,
    /// The causes that were appended to the sentence, outermost first.
    causes: Vec<String>,
    /// The full sentence: `summary` followed by `: ` and each cause.
    sentence: String,
}

impl ErrorChain {
    /// The error's own message, as [`ToString::to_string`] renders it.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// The causes appended to the sentence, outermost first.
    ///
    /// Sources skipped because their text is blank, or because the sentence already ends with
    /// their text, are not listed.
    #[must_use]
    pub fn causes(&self) -> &[String] {
        &self.causes
    }
}

impl fmt::Display for ErrorChain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.sentence)
    }
}

/// Builds the [`ErrorChain`] of `error`.
///
/// Each source in the chain has its trailing whitespace trimmed. A source whose text is then
/// empty is skipped. Otherwise a source is appended as `: text`, unless the sentence built so
/// far already ends with that text at a segment boundary: the sentence equals the text, or the
/// text follows a `: ` separator. A skipped source still has its own sources considered.
#[must_use]
pub fn error_chain(error: &(dyn Error + 'static)) -> ErrorChain {
    let summary = error.to_string();
    let mut sentence = summary.clone();
    let mut causes = Vec::new();
    let mut source = error.source();
    while let Some(current) = source {
        let text = current.to_string();
        let text = text.trim_end();
        if !text.is_empty() && !ends_with_segment(&sentence, text) {
            sentence.push_str(": ");
            sentence.push_str(text);
            causes.push(text.to_owned());
        }
        source = current.source();
    }
    ErrorChain {
        summary,
        causes,
        sentence,
    }
}

/// Reports whether `sentence` is `text`, or ends with `text` right after a `: ` separator, so a
/// match never starts inside a word.
fn ends_with_segment(sentence: &str, text: &str) -> bool {
    sentence == text
        || sentence
            .strip_suffix(text)
            .is_some_and(|head| head.ends_with(": "))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use thiserror::Error;

    use super::{ErrorChain, error_chain};

    /// A leaf error with no source.
    #[derive(Debug, Error)]
    #[error("{0}")]
    struct Leaf(&'static str);

    /// An error with a fixed message and one source.
    #[derive(Debug, Error)]
    #[error("{message}")]
    struct Wrap<E: Error + 'static> {
        /// The message of this layer.
        message: &'static str,
        /// The source of this layer.
        #[source]
        source: E,
    }

    fn chain_of(error: &(dyn Error + 'static)) -> (String, Vec<String>, String) {
        let chain: ErrorChain = error_chain(error);
        (
            chain.summary().to_owned(),
            chain.causes().to_vec(),
            chain.to_string(),
        )
    }

    #[test]
    fn no_source_has_no_causes() {
        let error = Leaf("inner");
        assert_eq!(
            chain_of(&error),
            ("inner".to_owned(), Vec::new(), "inner".to_owned())
        );
    }

    #[test]
    fn source_is_appended() {
        let error = Wrap {
            message: "outer",
            source: Leaf("inner"),
        };
        assert_eq!(
            chain_of(&error),
            (
                "outer".to_owned(),
                vec!["inner".to_owned()],
                "outer: inner".to_owned()
            )
        );
    }

    #[test]
    fn source_already_the_tail_of_the_message_is_skipped() {
        let error = Wrap {
            message: "wrapper: inner",
            source: Leaf("inner"),
        };
        assert_eq!(
            chain_of(&error),
            (
                "wrapper: inner".to_owned(),
                Vec::new(),
                "wrapper: inner".to_owned()
            )
        );
    }

    #[test]
    fn source_equal_to_whole_message_is_skipped() {
        let error = Wrap {
            message: "inner",
            source: Leaf("inner"),
        };
        assert_eq!(
            chain_of(&error),
            ("inner".to_owned(), Vec::new(), "inner".to_owned())
        );
    }

    #[test]
    fn skipped_source_still_shows_its_own_source() {
        let error = Wrap {
            message: "outer: inner",
            source: Wrap {
                message: "inner",
                source: Leaf("root"),
            },
        };
        assert_eq!(
            chain_of(&error),
            (
                "outer: inner".to_owned(),
                vec!["root".to_owned()],
                "outer: inner: root".to_owned()
            )
        );
    }

    #[test]
    fn trailing_whitespace_of_a_cause_is_trimmed() {
        let error = Wrap {
            message: "outer",
            source: Leaf("inner \n"),
        };
        assert_eq!(
            chain_of(&error),
            (
                "outer".to_owned(),
                vec!["inner".to_owned()],
                "outer: inner".to_owned()
            )
        );
    }

    #[test]
    fn match_inside_a_word_is_not_skipped() {
        let error = Wrap {
            message: "xinner",
            source: Leaf("inner"),
        };
        assert_eq!(
            chain_of(&error),
            (
                "xinner".to_owned(),
                vec!["inner".to_owned()],
                "xinner: inner".to_owned()
            )
        );
    }

    #[test]
    fn a_blank_source_is_skipped() {
        let error = Wrap {
            message: "outer",
            source: Leaf("  \n"),
        };
        assert_eq!(
            chain_of(&error),
            ("outer".to_owned(), Vec::new(), "outer".to_owned())
        );
    }
}
