//! Form-decoded query parameters shared by the API and browser login.

use std::borrow::Cow;

/// Returns the first value of `key`, borrowing when no decoding is needed.
pub(crate) fn query_param<'q>(query: Option<&'q str>, key: &str) -> Option<Cow<'q, str>> {
    form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}
