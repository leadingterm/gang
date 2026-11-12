//! Pagination for list responses.
//!
//! A list request carries `pageSize` and `pageToken`. Page sizes are clamped to
//! [`PAGE_SIZE_MAX`], and an absent or zero size selects [`PAGE_SIZE_DEFAULT`]. A list
//! response carries `nextPageToken` only when more results follow; the last page omits it.
//!
//! Tokens are opaque, URL-safe keyset cursors. The payload is a version byte, the request's
//! filter text, a NUL separator, and the cursor. A token that is malformed, has an unknown
//! version, or was issued for a different filter is invalid.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::PageError;

/// Page size used when a request omits `pageSize` or sends zero.
pub const PAGE_SIZE_DEFAULT: usize = 100;

/// Largest page size a request may ask for; larger requests are clamped to this value.
pub const PAGE_SIZE_MAX: usize = 1000;

/// Version byte that prefixes the payload of every [`PageToken`].
const TOKEN_VERSION: u8 = 1;

/// Separates the filter text from the cursor inside a token payload.
const SEPARATOR: u8 = 0;

/// Resolves a requested `pageSize` into the number of items to return.
///
/// `None` and `Some(0)` select [`PAGE_SIZE_DEFAULT`]. Sizes above [`PAGE_SIZE_MAX`] are
/// clamped to it.
///
/// # Errors
///
/// Returns [`PageError::NegativePageSize`] when `requested` is negative.
pub fn page_size(requested: Option<i64>) -> Result<usize, PageError> {
    match requested {
        None | Some(0) => Ok(PAGE_SIZE_DEFAULT),
        Some(size) if size < 0 => Err(PageError::NegativePageSize),
        Some(size) => {
            Ok(usize::try_from(size).map_or(PAGE_SIZE_MAX, |size| size.min(PAGE_SIZE_MAX)))
        }
    }
}

/// Opaque `pageToken` text that resumes a list after a known cursor.
///
/// Deserialization accepts any string; [`PageToken::cursor`] performs validation against the
/// request's filter.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct PageToken(String);

impl PageToken {
    /// Mints a token that resumes a list at `cursor` for requests using `filter`.
    ///
    /// `filter` is the request's filter text, or the empty string when absent. The filter
    /// must not contain NUL bytes; a token minted for such a filter never validates.
    #[must_use]
    pub fn new(filter: &str, cursor: &str) -> Self {
        let mut payload = Vec::with_capacity(filter.len() + cursor.len() + 2);
        payload.push(TOKEN_VERSION);
        payload.extend_from_slice(filter.as_bytes());
        payload.push(SEPARATOR);
        payload.extend_from_slice(cursor.as_bytes());
        Self(URL_SAFE_NO_PAD.encode(payload))
    }

    /// The token a list response carries when more results follow: `Some` exactly when `more` is true and `last` (the cursor of the final row of this page) is present.
    #[must_use]
    pub fn after(filter: &str, more: bool, last: Option<&str>) -> Option<Self> {
        if !more {
            return None;
        }
        last.map(|cursor| Self::new(filter, cursor))
    }

    /// Returns the cursor carried by this token, provided it was issued for `filter`.
    ///
    /// # Errors
    ///
    /// Returns [`PageError::InvalidToken`] when the token is not valid URL-safe base64 without
    /// padding, has an unknown version byte, lacks the filter separator, was issued for a
    /// different filter, or carries a cursor that is not UTF-8.
    pub fn cursor(&self, filter: &str) -> Result<String, PageError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| PageError::InvalidToken)?;
        let Some((&version, rest)) = bytes.split_first() else {
            return Err(PageError::InvalidToken);
        };
        if version != TOKEN_VERSION {
            return Err(PageError::InvalidToken);
        }
        let Some(separator) = rest.iter().position(|&byte| byte == SEPARATOR) else {
            return Err(PageError::InvalidToken);
        };
        let (filter_bytes, tail) = rest.split_at(separator);
        if filter_bytes != filter.as_bytes() {
            return Err(PageError::InvalidToken);
        }
        let Some((_, cursor_bytes)) = tail.split_first() else {
            return Err(PageError::InvalidToken);
        };
        std::str::from_utf8(cursor_bytes)
            .map(str::to_owned)
            .map_err(|_| PageError::InvalidToken)
    }

    /// Returns the opaque token text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PageToken {
    /// Wraps the text a client sent as `pageToken`, unchecked; [`PageToken::cursor`] validates it.
    fn from(text: String) -> Self {
        Self(text)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn token_from_text(text: &str) -> PageToken {
        serde_json::from_value(json!(text)).expect("any string deserializes")
    }

    fn token_with_payload(payload: &[u8]) -> PageToken {
        token_from_text(&URL_SAFE_NO_PAD.encode(payload))
    }

    #[test]
    fn page_size_resolves_requests() {
        let cases = [
            (None, 100),
            (Some(0), 100),
            (Some(1), 1),
            (Some(1000), 1000),
            (Some(1001), 1000),
            (Some(i64::MAX), 1000),
        ];
        for (requested, expected) in cases {
            assert_eq!(
                page_size(requested),
                Ok(expected),
                "requested {requested:?}"
            );
        }
    }

    #[test]
    fn negative_page_size_is_rejected() {
        assert_eq!(page_size(Some(-1)), Err(PageError::NegativePageSize));
    }

    #[test]
    fn token_round_trips_cursor_for_empty_filter() {
        let token = PageToken::new("", "cursor-1");
        assert_eq!(token.cursor(""), Ok("cursor-1".to_owned()));
    }

    #[test]
    fn token_round_trips_cursor_for_named_filter() {
        let token = PageToken::new("revokeTime:*", "cursor-2");
        assert_eq!(token.cursor("revokeTime:*"), Ok("cursor-2".to_owned()));
    }

    #[test]
    fn token_for_empty_filter_is_invalid_for_named_filter() {
        let token = PageToken::new("", "cursor");
        assert_eq!(token.cursor("revokeTime:*"), Err(PageError::InvalidToken));
    }

    #[test]
    fn token_for_named_filter_is_invalid_for_empty_filter() {
        let token = PageToken::new("revokeTime:*", "cursor");
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn malformed_text_is_invalid() {
        let token = token_from_text("not base64!");
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn empty_token_is_invalid() {
        let token = token_from_text("");
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn wrong_version_is_invalid() {
        let token = token_with_payload(&[2, 0, b'x']);
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn missing_separator_is_invalid() {
        let token = token_with_payload(&[1, b'x']);
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn non_utf8_cursor_is_invalid() {
        let token = token_with_payload(&[1, 0, 0xff]);
        assert_eq!(token.cursor(""), Err(PageError::InvalidToken));
    }

    #[test]
    fn token_text_is_url_safe() {
        let token = PageToken::new("revokeTime:*", "cursor/with+symbols?and=padding~~");
        assert!(
            token
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
    }

    #[test]
    fn cursor_with_nul_bytes_round_trips() {
        let cursor = "a\0b\0\0c";
        let token = PageToken::new("filter", cursor);
        assert_eq!(token.cursor("filter"), Ok(cursor.to_owned()));
    }

    #[test]
    fn token_serializes_as_bare_string() {
        let token = PageToken::new("", "cursor");
        let value = serde_json::to_value(&token).expect("token serializes");
        assert_eq!(value, json!(token.as_str()));
        let round_trip: PageToken = serde_json::from_value(value).expect("token deserializes");
        assert_eq!(round_trip, token);
    }

    #[test]
    fn after_issues_token_when_more_and_last_present() {
        let token = PageToken::after("filter", true, Some("last-row"))
            .expect("more with last yields a token");
        assert_eq!(token.cursor("filter"), Ok("last-row".to_owned()));
    }

    #[test]
    fn after_is_none_without_last() {
        assert_eq!(PageToken::after("filter", true, None), None);
    }

    #[test]
    fn after_is_none_without_more() {
        assert_eq!(PageToken::after("filter", false, Some("last-row")), None);
    }

    #[test]
    fn after_token_is_invalid_for_other_filter() {
        let token = PageToken::after("filter", true, Some("last-row"))
            .expect("more with last yields a token");
        assert_eq!(token.cursor("other"), Err(PageError::InvalidToken));
    }
}
