//! Shared checks for the error types' `every_variant()` tests.

use std::error::Error;

/// Asserts that no error variant repeats the text of its source chain.
///
/// The rule, for each variant: walk its whole source chain (`error.source()`, then that
/// source's source, and so on). Every source must have a non-empty message, and the variant's
/// own message must not contain that message. `gang_types::error_chain` prints
/// `summary: cause`, so a message that repeats a cause prints it twice. Use this in
/// `every_variant()` tests, one call over all of a type's variants.
///
/// # Panics
///
/// Panics when a variant breaks the rule; the message names the variant and the offending
/// cause text.
pub fn assert_sources_not_repeated<E: Error>(variants: &[E]) {
    for error in variants {
        let rendered = error.to_string();
        let mut source = error.source();
        while let Some(cause) = source {
            let text = cause.to_string();
            assert!(!text.is_empty(), "{error:?}: a source has an empty message");
            assert!(
                !rendered.contains(&text),
                "{error:?}: its message repeats the source `{text}`"
            );
            source = cause.source();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;

    /// A manual error whose source chain is built from boxed nodes.
    #[derive(Debug)]
    struct Chain {
        message: &'static str,
        source: Option<Box<Chain>>,
    }

    impl fmt::Display for Chain {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message)
        }
    }

    impl Error for Chain {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source
                .as_deref()
                .map(|cause| cause as &(dyn Error + 'static))
        }
    }

    fn leaf(message: &'static str) -> Chain {
        Chain {
            message,
            source: None,
        }
    }

    fn wrap(message: &'static str, source: Chain) -> Chain {
        Chain {
            message,
            source: Some(Box::new(source)),
        }
    }

    fn passes_check<E: Error>(variants: &[E]) -> bool {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_sources_not_repeated(variants);
        }))
        .is_ok()
    }

    #[test]
    fn clean_error_with_source_passes() {
        let clean = wrap("failed to load", leaf("disk full"));
        assert!(passes_check(&[clean]));
    }

    #[test]
    fn error_without_source_passes() {
        assert!(passes_check(&[leaf("alone")]));
    }

    #[test]
    fn empty_variant_list_passes() {
        let variants: [Chain; 0] = [];
        assert!(passes_check(&variants));
    }

    #[test]
    fn message_containing_its_source_is_caught() {
        let repeating = wrap("could not read: disk", leaf("disk"));
        assert!(!passes_check(&[repeating]));
    }

    #[test]
    fn message_containing_a_deeper_source_is_caught() {
        let repeating = wrap("ouch: deep", wrap("middle", leaf("deep")));
        assert!(!passes_check(&[repeating]));
    }

    #[test]
    fn empty_source_message_is_caught() {
        let empty = wrap("outer", leaf(""));
        assert!(!passes_check(&[empty]));
    }
}
