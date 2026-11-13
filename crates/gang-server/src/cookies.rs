//! The browser cookies of a session: the names the session and sign-in cookies carry on an
//! endpoint, and the reader that finds one in a request's `Cookie` headers.

use std::fmt;
use std::time::Duration;

use gang_types::{AccessToken, Url};
use gang_wire::CookieNames;
use http::HeaderMap;
use http::header::{COOKIE, HeaderValue};

/// The longest cookie lifetime browsers keep (RFC 6265bis caps Max-Age at 400 days): the renewal
/// cookie outlives `session_max_age` so the sign-in after it asks no consent; the sealed
/// `expire_time` alone bounds renewal (ADR-39).
const RENEWAL_COOKIE_MAX_AGE: Duration = Duration::from_hours(400 * 24);

/// A cookie value or formatted header is invalid.
#[derive(Debug, thiserror::Error)]
#[error("invalid cookie value or header")]
pub(crate) struct InvalidCookieValue;

/// The supported cross-site cookie policies.
#[derive(Debug, Clone, Copy)]
enum SameSite {
    /// Only send the cookie with same-site requests.
    Strict,
    /// Allow the cookie on top-level safe cross-site navigations.
    Lax,
}

impl fmt::Display for SameSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Strict => "Strict",
            Self::Lax => "Lax",
        })
    }
}

/// The names and attributes of the session cookies of one endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionCookies {
    /// Name of the cookie that carries the browser session.
    pub(crate) session: &'static str,
    /// Name of the cookie that carries the browser sign-in.
    pub(crate) login: &'static str,
    /// Name of the cookie that carries the sealed renewal.
    pub(crate) renewal: &'static str,
    /// Whether the cookies are set with the `Secure` attribute.
    pub(crate) secure: bool,
}

impl SessionCookies {
    /// The cookies of an endpoint at `endpoint`: the `__Host-` names and the `Secure` attribute
    /// under `https`, the plain names otherwise.
    #[must_use]
    pub(crate) fn for_endpoint(endpoint: &Url) -> Self {
        let secure = endpoint.scheme() == "https";
        let names = CookieNames::for_https(secure);
        Self {
            session: names.session,
            login: names.login,
            renewal: names.renewal,
            secure,
        }
    }

    /// Logs a warning when the cookies are sent without `Secure`, which is the case on an
    /// endpoint that is not served over `https`.
    pub(crate) fn warn_insecure(&self) {
        if !self.secure {
            tracing::warn!(
                "session cookies are sent without Secure because the endpoint is not https"
            );
        }
    }

    /// The `Set-Cookie` header that starts a browser session with `token`, expiring after
    /// `max_age` (whole seconds).
    ///
    /// # Errors
    /// Returns an error when the value is not a cookie-octet sequence or valid header value.
    pub(crate) fn session_header(
        &self,
        token: &AccessToken,
        max_age: Duration,
    ) -> Result<HeaderValue, InvalidCookieValue> {
        self.attributes(self.session, token.expose(), SameSite::Strict, max_age)
    }

    /// The `Set-Cookie` header that holds the sign-in `state` while the browser visits the
    /// identity provider, expiring after `max_age` (whole seconds).
    ///
    /// # Errors
    /// Returns an error when the value is not a cookie-octet sequence or valid header value.
    pub(crate) fn login_header(
        &self,
        state: &str,
        max_age: Duration,
    ) -> Result<HeaderValue, InvalidCookieValue> {
        self.attributes(self.login, state, SameSite::Lax, max_age)
    }

    /// The sealed renewal cookie, retained beyond the sealed renewal's expiry.
    pub(crate) fn renewal_header(&self, sealed: &str) -> Result<HeaderValue, InvalidCookieValue> {
        self.attributes(
            self.renewal,
            sealed,
            SameSite::Strict,
            RENEWAL_COOKIE_MAX_AGE,
        )
    }

    /// The header that clears the renewal cookie.
    pub(crate) fn clear_renewal_header(&self) -> Result<HeaderValue, InvalidCookieValue> {
        self.attributes(self.renewal, "", SameSite::Strict, Duration::ZERO)
    }

    /// The `Set-Cookie` header that clears the session cookie.
    ///
    /// # Errors
    /// Returns an error when the cookie cannot be represented as a header value.
    pub(crate) fn clear_session_header(&self) -> Result<HeaderValue, InvalidCookieValue> {
        self.session_header(&AccessToken::new(""), Duration::ZERO)
    }

    /// The `Set-Cookie` header that clears the sign-in cookie.
    ///
    /// # Errors
    /// Returns an error when the cookie cannot be represented as a header value.
    pub(crate) fn clear_login_header(&self) -> Result<HeaderValue, InvalidCookieValue> {
        self.login_header("", Duration::ZERO)
    }

    /// The cookie `name=value` with its attributes; `Secure` is only set under `https`.
    fn attributes(
        &self,
        name: &str,
        value: &str,
        same_site: SameSite,
        max_age: Duration,
    ) -> Result<HeaderValue, InvalidCookieValue> {
        if value
            .bytes()
            .any(|b| b <= 0x20 || b >= 0x7f || matches!(b, b'"' | b',' | b';' | b'\\'))
        {
            return Err(InvalidCookieValue);
        }
        let secure = if self.secure { "Secure; " } else { "" };
        HeaderValue::try_from(format!(
            "{name}={value}; HttpOnly; {secure}SameSite={same_site}; Path=/; Max-Age={}",
            max_age.as_secs()
        ))
        .map_err(|_| InvalidCookieValue)
    }
}

/// The value of the first cookie named `name` in the `Cookie` headers of `headers`. Every
/// `Cookie` header is read, since a request may split its cookies across several. Headers that
/// are not valid UTF-8 are skipped. An empty value is returned as `Some("")`.
#[must_use]
pub(crate) fn cookie_value<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|header| header.split(';'))
        .find_map(|pair| {
            let (key, value) = pair.trim_matches(' ').split_once('=')?;
            (key.trim_matches(' ') == name).then_some(value)
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_types::{AccessToken, Url};
    use http::HeaderMap;
    use http::header::{COOKIE, HeaderValue};

    use super::{SessionCookies, cookie_value};

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url parses")
    }

    fn headers(values: &[&'static [u8]]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(
                COOKIE,
                HeaderValue::from_bytes(value).expect("test header value is valid"),
            );
        }
        headers
    }

    #[test]
    fn https_endpoint_uses_host_prefixed_secure_names() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        assert_eq!(cookies.session, "__Host-gang_session");
        assert_eq!(cookies.login, "__Host-gang_login");
        assert!(cookies.secure);
    }

    #[test]
    fn http_endpoint_uses_plain_names() {
        let cookies = SessionCookies::for_endpoint(&url("http://127.0.0.1:8080/"));
        assert_eq!(cookies.session, "gang_session");
        assert_eq!(cookies.login, "gang_login");
        assert!(!cookies.secure);
    }

    #[test]
    fn finds_a_single_pair() {
        let map = headers(&[b"gang_session=abc"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("abc"));
    }

    #[test]
    fn finds_a_pair_among_several_with_spaces() {
        let map = headers(&[b"theme=dark;   gang_session=abc ;gang_login=xyz"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("abc"));
        assert_eq!(cookie_value(&map, "gang_login"), Some("xyz"));
    }

    #[test]
    fn reads_every_cookie_header() {
        let map = headers(&[b"theme=dark", b"gang_session=abc"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("abc"));
    }

    #[test]
    fn first_duplicate_wins() {
        let map = headers(&[
            b"gang_session=first; gang_session=second",
            b"gang_session=third",
        ]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("first"));
    }

    #[test]
    fn similarly_named_cookies_do_not_match() {
        let map = headers(&[b"x_gang_session=wrong; gang_session2=also_wrong"]);
        assert_eq!(cookie_value(&map, "gang_session"), None);
        let map = headers(&[b"gang_session2=wrong; x_gang_session=also_wrong; gang_session=right"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("right"));
    }

    #[test]
    fn absent_cookie_is_none() {
        let map = headers(&[b"theme=dark"]);
        assert_eq!(cookie_value(&map, "gang_session"), None);
        assert_eq!(cookie_value(&HeaderMap::new(), "gang_session"), None);
    }

    #[test]
    fn empty_value_is_some_empty() {
        let map = headers(&[b"gang_session=; theme=dark"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some(""));
    }

    #[test]
    fn non_utf8_header_is_skipped() {
        let map = headers(&[b"gang_session=bad\xff", b"gang_session=good"]);
        assert_eq!(cookie_value(&map, "gang_session"), Some("good"));
        let map = headers(&[b"gang_session=bad\xff"]);
        assert_eq!(cookie_value(&map, "gang_session"), None);
    }

    fn text(value: &HeaderValue) -> &str {
        value.to_str().expect("header value is visible ascii")
    }

    #[test]
    fn session_header_under_https_is_host_prefixed_and_secure() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        let header = cookies
            .session_header(&AccessToken::new("token-value"), Duration::from_mins(10))
            .expect("token is a valid header value");
        assert_eq!(
            text(&header),
            "__Host-gang_session=token-value; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=600"
        );
    }

    #[test]
    fn session_header_under_http_omits_secure_and_host_prefix() {
        let cookies = SessionCookies::for_endpoint(&url("http://127.0.0.1:8080/"));
        let header = cookies
            .session_header(&AccessToken::new("token-value"), Duration::from_mins(10))
            .expect("token is a valid header value");
        assert_eq!(
            text(&header),
            "gang_session=token-value; HttpOnly; SameSite=Strict; Path=/; Max-Age=600"
        );
    }

    #[test]
    fn login_header_under_https_is_host_prefixed_lax_and_secure() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        let header = cookies
            .login_header("state-1", Duration::from_mins(10))
            .expect("state is a valid header value");
        assert_eq!(
            text(&header),
            "__Host-gang_login=state-1; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=600"
        );
    }

    #[test]
    fn login_header_under_http_omits_secure_and_host_prefix() {
        let cookies = SessionCookies::for_endpoint(&url("http://127.0.0.1:8080/"));
        let header = cookies
            .login_header("state-1", Duration::from_mins(10))
            .expect("state is a valid header value");
        assert_eq!(
            text(&header),
            "gang_login=state-1; HttpOnly; SameSite=Lax; Path=/; Max-Age=600"
        );
    }

    #[test]
    fn max_age_counts_whole_seconds() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        let header = cookies
            .login_header("state-1", Duration::from_millis(90_900))
            .expect("state is a valid header value");
        assert!(text(&header).ends_with("; Max-Age=90"));
        let header = cookies
            .session_header(
                &AccessToken::new("token-value"),
                Duration::from_millis(90_900),
            )
            .expect("token is a valid header value");
        assert!(text(&header).ends_with("; Max-Age=90"));
    }

    #[test]
    fn clear_headers_expire_immediately_under_https() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        assert_eq!(
            text(
                &cookies
                    .clear_session_header()
                    .expect("clear header is valid")
            ),
            "__Host-gang_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0"
        );
        assert_eq!(
            text(&cookies.clear_login_header().expect("clear header is valid")),
            "__Host-gang_login=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0"
        );
    }

    #[test]
    fn clear_headers_expire_immediately_under_http() {
        let cookies = SessionCookies::for_endpoint(&url("http://127.0.0.1:8080/"));
        assert_eq!(
            text(
                &cookies
                    .clear_session_header()
                    .expect("clear header is valid")
            ),
            "gang_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"
        );
        assert_eq!(
            text(&cookies.clear_login_header().expect("clear header is valid")),
            "gang_login=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"
        );
    }

    #[test]
    fn cookie_values_reject_attribute_injection_and_spaces() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        for value in ["token; Domain=evil.example", "token value"] {
            assert!(
                cookies
                    .session_header(&AccessToken::new(value), Duration::from_secs(60))
                    .is_err()
            );
            assert!(
                cookies
                    .login_header(value, Duration::from_secs(60))
                    .is_err()
            );
        }
    }

    #[test]
    fn cookie_values_reject_non_cookie_octets() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        for value in ["\"", ",", ";", "\\", "\u{7f}", "é"] {
            assert!(
                cookies
                    .login_header(value, Duration::from_secs(60))
                    .is_err()
            );
        }
        assert!(
            cookies
                .login_header("!#$%&'()*+-./:<>?@[]^_`{|}~", Duration::from_secs(60))
                .is_ok()
        );
    }

    #[test]
    fn token_with_control_character_is_rejected() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        assert!(
            cookies
                .session_header(&AccessToken::new("bad\ntoken"), Duration::from_mins(10))
                .is_err()
        );
    }

    #[test]
    fn state_with_control_character_is_rejected() {
        let cookies = SessionCookies::for_endpoint(&url("https://gang.example/"));
        assert!(
            cookies
                .login_header("bad\u{7f}state", Duration::from_mins(10))
                .is_err()
        );
    }
}
