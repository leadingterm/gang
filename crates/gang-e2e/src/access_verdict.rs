//! The pure verdicts of the identity scenario over `gang` output, HTTP answers and doctor reports.

use gang_types::Url;

use crate::Ran;
use crate::cli::{RowStatus, plain, status_rows};
use crate::server::Answer;

/// A successful verdict or the reason the observed value did not meet it.
pub type Verdict = std::result::Result<(), String>;

/// The value variant expected for a cookie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieValue<'a> {
    /// The cookie value must equal this string.
    Exactly(&'a str),
    /// The cookie value must not be empty.
    NonEmpty,
}

/// The expected maximum age constraint for a cookie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxAge {
    /// The maximum age must equal this number.
    Exactly(u64),
    /// The maximum age must be positive.
    Positive,
}

/// A parsed Set-Cookie header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetCookie {
    /// Cookie name.
    pub name: String,
    /// Cookie value.
    pub value: String,
    /// Cookie attributes in header order.
    pub attributes: Vec<(String, Option<String>)>,
}

impl SetCookie {
    /// Parses a Set-Cookie header.
    #[must_use]
    pub fn parse(header: &str) -> Option<Self> {
        let mut parts = header.split(';');
        let (name, value) = parts.next()?.split_once('=')?;
        let attributes = parts
            .filter_map(|part| {
                let part = part.trim();
                if part.is_empty() {
                    return None;
                }
                let (key, value) = part.split_once('=').map_or((part, None), |(key, value)| {
                    (key.trim(), Some(value.trim().to_owned()))
                });
                Some((key.to_owned(), value))
            })
            .collect();
        Some(Self {
            name: name.trim().to_owned(),
            value: value.trim().to_owned(),
            attributes,
        })
    }

    /// Returns the first matching attribute's optional value.
    #[must_use]
    pub fn attribute(&self, name: &str) -> Option<Option<&str>> {
        self.attributes
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_deref())
    }
}

/// The expected attributes and value of a cookie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedCookie<'a> {
    /// Expected cookie name.
    pub name: &'a str,
    /// Expected cookie value constraint.
    pub value: CookieValue<'a>,
    /// Expected `SameSite` attribute.
    pub same_site: &'a str,
    /// Expected Max-Age constraint.
    pub max_age: MaxAge,
}

/// Parses all Set-Cookie response headers.
#[must_use]
pub fn set_cookies(answer: &Answer) -> Vec<SetCookie> {
    answer
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("set-cookie"))
        .filter_map(|(_, value)| SetCookie::parse(value))
        .collect()
}

/// Checks that the response contains exactly the expected cookie and security attributes.
///
/// # Errors
///
/// Returns a description when the cookie does not match.
pub fn cookie_set(answer: &Answer, expected: &ExpectedCookie<'_>) -> Verdict {
    let cookies: Vec<_> = set_cookies(answer)
        .into_iter()
        .filter(|cookie| cookie.name == expected.name)
        .collect();
    let fail = |reason: &str| Err(format!("{reason}; cookie seen: {cookies:?}"));
    if cookies.len() != 1 {
        return fail("expected exactly one cookie with the requested name");
    }
    let cookie = &cookies[0];
    let valid_value = match expected.value {
        CookieValue::Exactly(value) => cookie.value == value,
        CookieValue::NonEmpty => !cookie.value.is_empty(),
    };
    if !valid_value {
        return fail("cookie value did not match");
    }
    if cookie.attribute("HttpOnly").is_none() {
        return fail("HttpOnly attribute was absent");
    }
    if cookie.attribute("Secure").is_some() != expected.name.starts_with("__Host-") {
        return fail("Secure attribute did not match cookie name");
    }
    if cookie
        .attribute("SameSite")
        .flatten()
        .is_none_or(|value| !value.eq_ignore_ascii_case(expected.same_site))
    {
        return fail("SameSite attribute did not match");
    }
    if cookie.attribute("Path").flatten() != Some("/") {
        return fail("Path attribute was not /");
    }
    let max_age = cookie
        .attribute("Max-Age")
        .flatten()
        .and_then(|value| value.parse::<u64>().ok());
    if !matches!(
        (max_age, expected.max_age),
        (Some(actual), MaxAge::Exactly(expected)) if actual == expected
    ) && !matches!((max_age, expected.max_age), (Some(actual), MaxAge::Positive) if actual > 0)
    {
        return fail("Max-Age attribute did not match");
    }
    Ok(())
}

/// Returns the first value of a query parameter.
#[must_use]
pub fn query_value(url: &str, key: &str) -> Option<String> {
    Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

/// Checks the required OAuth authorization request parameters.
///
/// # Errors
///
/// Returns a description when the authorization request is invalid.
pub fn authorization_request(location: &str, client_id: &str, redirect_uri: &str) -> Verdict {
    let Ok(url) = Url::parse(location) else {
        return Err(format!("invalid authorization URL: {location}"));
    };
    for key in [
        "response_type",
        "client_id",
        "redirect_uri",
        "scope",
        "state",
        "nonce",
        "code_challenge",
        "code_challenge_method",
    ] {
        if url.query_pairs().filter(|(name, _)| name == key).count() != 1 {
            return Err(format!("{key} did not occur exactly once in {location}"));
        }
    }
    let value = |key| {
        url.query_pairs()
            .find(|(name, _)| name == key)
            .map_or_else(String::new, |(_, value)| value.into_owned())
    };
    for (key, actual, expected) in [
        ("response_type", value("response_type"), "code".to_owned()),
        ("client_id", value("client_id"), client_id.to_owned()),
        (
            "redirect_uri",
            value("redirect_uri"),
            redirect_uri.to_owned(),
        ),
        (
            "code_challenge_method",
            value("code_challenge_method"),
            "S256".to_owned(),
        ),
    ] {
        if actual != expected {
            return Err(format!("{key} was {actual:?} in {location}"));
        }
    }
    let scope = value("scope");
    if !scope
        .split_ascii_whitespace()
        .any(|scope| scope == "openid")
    {
        return Err(format!("scope lacked openid in {location}"));
    }
    for key in ["state", "nonce"] {
        if value(key).is_empty() {
            return Err(format!("{key} was empty in {location}"));
        }
    }
    let challenge = value("code_challenge");
    if challenge.len() != 43
        || !challenge
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(format!("code_challenge was invalid in {location}"));
    }
    Ok(())
}

/// Checks that the answer redirects to the expected location.
///
/// # Errors
///
/// Returns a description when the redirect does not match.
pub fn redirected_to(answer: &Answer, location: &str) -> Verdict {
    if answer.status == 302 && answer.header("location") == Some(location) {
        Ok(())
    } else {
        Err(format!(
            "answered {} with location {:?}; expected 302 and {location:?}",
            answer.status,
            answer.header("location")
        ))
    }
}

/// Checks the response status.
///
/// # Errors
///
/// Returns a description when the status does not match.
pub fn status_is(answer: &Answer, status: u16) -> Verdict {
    if answer.status == status {
        Ok(())
    } else {
        Err(format!("answered {}: {}", answer.status, answer.text()))
    }
}

/// Checks for the specified line after trimming line-ending whitespace.
#[must_use]
pub fn has_line(text: &str, line: &str) -> bool {
    text.lines().any(|entry| entry.trim_end() == line)
}

/// Checks that a command reported the expected failure and fix lines.
///
/// # Errors
///
/// Returns a description when the command output does not match.
pub fn failure_reported(outcome: &Ran, subject: &str, message: &str, fix: &str) -> Verdict {
    let failure = format!("  fail  {subject}: {message}");
    let fix_line = format!("   fix  {fix}");
    let text = plain(&outcome.stderr);
    if outcome.code == Some(1)
        && status_rows(&text).iter().any(|row| {
            row.status == RowStatus::Fail
                && row.subject == subject
                && row.detail == message
                && row.fix == Some(fix)
        })
    {
        Ok(())
    } else {
        Err(format!(
            "exit {:?}; expected the lines `{failure}` and `{fix_line}` in: {}",
            outcome.code, outcome.stderr
        ))
    }
}

/// Returns the login cookie name appropriate for the endpoint scheme.
#[must_use]
pub fn login_cookie_name(endpoint: &Url) -> &'static str {
    gang_wire::CookieNames::for_https(endpoint.scheme() == "https").login
}
