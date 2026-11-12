//! Cache-control values and the security header set applied to every response.

use http::{HeaderMap, HeaderName, HeaderValue};

/// `Cache-Control` for responses that must never be stored.
pub(crate) const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

/// `Cache-Control` for content-addressed assets, which never change.
pub(crate) const IMMUTABLE: HeaderValue =
    HeaderValue::from_static("public, max-age=31536000, immutable");

/// `Allow` for the methods this handler serves.
pub(crate) const ALLOW: HeaderValue = HeaderValue::from_static("GET, HEAD");

/// The security headers set on every response, as lower-case `(name, value)` pairs.
pub const SECURITY_HEADERS: [(&str, &str); 6] = [
    (
        "content-security-policy",
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
         font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; \
         frame-ancestors 'none'",
    ),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
    ("cross-origin-opener-policy", "same-origin"),
    ("cross-origin-resource-policy", "same-origin"),
    (
        "permissions-policy",
        "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
    ),
];

/// Parses one table entry at compile time, so an invalid name or value fails the build
/// instead of panicking on the request path.
const fn entry(index: usize) -> (HeaderName, HeaderValue) {
    let (name, value) = SECURITY_HEADERS[index];
    (
        HeaderName::from_static(name),
        HeaderValue::from_static(value),
    )
}

const PARSED: [(HeaderName, HeaderValue); 6] =
    [entry(0), entry(1), entry(2), entry(3), entry(4), entry(5)];

/// Inserts every header of [`SECURITY_HEADERS`] into `headers`, replacing any existing values.
pub(crate) fn secure(headers: &mut HeaderMap) {
    for (name, value) in PARSED {
        headers.insert(name, value);
    }
}
