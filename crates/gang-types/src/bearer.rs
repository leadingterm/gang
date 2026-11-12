/// The token of an RFC 6750 `Authorization: Bearer <token>` header value: the scheme compared
/// case-insensitively, one or more spaces, trailing whitespace trimmed; `None` for another
/// scheme or an empty token.
#[must_use]
pub fn parse_bearer(header_value: &str) -> Option<&str> {
    let (scheme, credentials) = header_value.split_once(' ')?;
    let token = credentials.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::parse_bearer;

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        assert_eq!(parse_bearer("bearer token"), Some("token"));
        assert_eq!(parse_bearer("BEARER token"), Some("token"));
    }

    #[test]
    fn bearer_accepts_multiple_spaces_and_trims_trailing_whitespace() {
        assert_eq!(parse_bearer("Bearer  token \t"), Some("token"));
    }

    #[test]
    fn bearer_rejects_empty_tokens() {
        assert_eq!(parse_bearer("Bearer "), None);
        assert_eq!(parse_bearer("Bearer   \t"), None);
    }

    #[test]
    fn bearer_rejects_other_schemes_and_missing_separator() {
        assert_eq!(parse_bearer("Basic x"), None);
        assert_eq!(parse_bearer("Bearertoken"), None);
    }
}
