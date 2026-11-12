//! Weak comparison of `If-None-Match` against an entity tag.

use http::HeaderValue;

const ANY: &str = "*";
const WEAK_PREFIX: &str = "W/";
const LIST_SEPARATOR: char = ',';

/// Returns `true` when `if_none_match` matches `etag` under weak comparison.
///
/// A missing or non-UTF-8 header never matches. The value `*` matches any
/// entity tag. Otherwise the value is a comma-separated list of entity tags;
/// each item is trimmed and may carry a leading `W/`, which is ignored.
pub(crate) fn not_modified(if_none_match: Option<&HeaderValue>, etag: &str) -> bool {
    let Some(value) = if_none_match.and_then(|header| header.to_str().ok()) else {
        return false;
    };
    let value = value.trim();
    if value == ANY {
        return true;
    }
    value
        .split(LIST_SEPARATOR)
        .map(str::trim)
        .any(|candidate| candidate.strip_prefix(WEAK_PREFIX).unwrap_or(candidate) == etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETAG: &str = "\"abc123\"";

    fn check(value: &'static str) -> bool {
        not_modified(Some(&HeaderValue::from_static(value)), ETAG)
    }

    #[test]
    fn absent_header_does_not_match() {
        assert!(
            !not_modified(None, ETAG),
            "an absent If-None-Match must not match"
        );
    }

    #[test]
    fn exact_strong_match() {
        assert!(check("\"abc123\""), "identical strong tag must match");
    }

    #[test]
    fn weak_prefix_matches() {
        assert!(check("W/\"abc123\""), "W/ tag must match weakly");
    }

    #[test]
    fn match_in_middle_of_list() {
        assert!(
            check("\"one\", \"abc123\", \"two\""),
            "a matching item anywhere in the list must match"
        );
    }

    #[test]
    fn list_without_match() {
        assert!(
            !check("\"one\", \"two\""),
            "a list with no matching item must not match"
        );
    }

    #[test]
    fn star_matches_any_tag() {
        assert!(check("*"), "`*` must match any entity tag");
    }

    #[test]
    fn empty_value_does_not_match() {
        assert!(!check(""), "an empty If-None-Match must not match");
    }

    #[test]
    fn lowercase_weak_prefix_is_not_stripped() {
        assert!(
            !check("w/\"abc123\""),
            "lowercase w/ is not the weak prefix and must not match"
        );
    }

    #[test]
    fn unquoted_tag_does_not_match() {
        assert!(
            !check("abc123"),
            "an unquoted tag is not the etag and must not match"
        );
    }

    #[test]
    fn whitespace_around_items_is_ignored() {
        assert!(
            check("   W/\"abc123\"   "),
            "surrounding whitespace must be trimmed"
        );
        assert!(
            check("\"one\" ,   \"abc123\"  ,  \"two\""),
            "whitespace around list items must be trimmed"
        );
    }

    #[test]
    fn non_utf8_header_does_not_match() {
        let header =
            HeaderValue::from_bytes(&[b'"', 0xff, b'"']).expect("obs-text is a valid header byte");
        assert!(
            !not_modified(Some(&header), ETAG),
            "a non-UTF-8 If-None-Match must not match"
        );
    }
}
