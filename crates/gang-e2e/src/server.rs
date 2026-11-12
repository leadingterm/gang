//! What the server scenarios see of a deployment, as one trait: raw HTTP exchanges with its
//! public listener, the raw bytes of its state stream, the leaf certificate a fresh TLS
//! connection is shown, a connection held open, and a graceful shutdown. The `api`,
//! `request-limits`, `viewer-cap`, `certificate-rotation` and event-paging scenarios run against
//! the real [`Stack`](crate::Stack) and against a simulation in tests through it.
//!
//! The configuration knobs of those scenarios are [`ServerKnobs`]; the values they render are the
//! constants below, which the scenarios and the simulation read too.

use std::time::{Duration, SystemTime};

use http::Method;

use crate::errors::{E2eError, Result};
use crate::identity::{Channel, Credentials};
use crate::session::SessionCookie;

/// `[server] max_request_size` of the request-limits deployment, in bytes (`"1KiB"`).
pub const MAX_REQUEST_SIZE: usize = 1024;

/// `[server.rate_limit.client] burst` of the request-limits deployment.
///
/// Starting the deployment spends requests of the loopback caller's own bucket (`gang bazelrc`
/// and the logins), so the burst leaves room for them, and the checks present fresh caller
/// addresses through `X-Forwarded-For` from the trusted loopback proxy, each with a full bucket.
pub const RATE_LIMIT_BURST: u32 = 16;

/// `[server.rate_limit.client] refill_interval` of the request-limits deployment: no request is
/// refilled within a run, so the request after the burst is refused whatever the timing.
pub const RATE_LIMIT_REFILL: Duration = Duration::from_hours(1);

/// `[server.rate_limit] trusted_proxies` of the request-limits deployment: the loopback
/// networks, where the harness connects from.
pub const TRUSTED_PROXIES: [&str; 2] = ["127.0.0.0/8", "::1/128"];

/// `[dashboard] retry_after` of the viewer-cap deployment; not the default, so the answer shows
/// the configured value.
pub const VIEWER_RETRY_AFTER: Duration = Duration::from_secs(3);

/// `[server] certificate_ttl` of the certificate-rotation deployment.
pub const CERTIFICATE_TTL: Duration = Duration::from_secs(20);

/// `[server] reissue_at_percent` of the certificate-rotation deployment.
pub const REISSUE_AT_PERCENT: u32 = 50;

/// `[server] certificate_backdate` of the certificate-rotation deployment: none, so a leaf's
/// `notBefore` is its issuance instant truncated to whole seconds.
pub const CERTIFICATE_BACKDATE: Duration = Duration::ZERO;

/// The `[server]`, `[server.rate_limit]` and `[dashboard]` keys the server scenarios vary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerKnobs {
    /// The request size limit and the rate limit.
    pub request_limits: RequestLimits,
    /// The dashboard's viewer limit.
    pub viewers: ViewerLimit,
    /// The lifetime of the listener certificate.
    pub certificate: CertificateLifetime,
}

impl ServerKnobs {
    /// Every knob absent: the base configuration.
    pub const NONE: Self = Self {
        request_limits: RequestLimits::Default,
        viewers: ViewerLimit::Default,
        certificate: CertificateLifetime::Default,
    };

    /// The request-limits scenario's knobs: [`RequestLimits::Tight`].
    pub const TIGHT_LIMITS: Self = Self {
        request_limits: RequestLimits::Tight,
        ..Self::NONE
    };

    /// The viewer-cap scenario's knobs: [`ViewerLimit::One`].
    pub const ONE_VIEWER: Self = Self {
        viewers: ViewerLimit::One,
        ..Self::NONE
    };

    /// The certificate-rotation scenario's knobs: [`CertificateLifetime::Short`].
    pub const SHORT_CERTIFICATE: Self = Self {
        certificate: CertificateLifetime::Short,
        ..Self::NONE
    };

    /// Every preset a server scenario runs under, the base configuration first.
    pub const PRESETS: &[Self] = &[
        Self::NONE,
        Self::TIGHT_LIMITS,
        Self::ONE_VIEWER,
        Self::SHORT_CERTIFICATE,
    ];
}

/// `[server] max_request_size` and `[server.rate_limit]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestLimits {
    /// The base configuration's limits.
    Default,
    /// [`MAX_REQUEST_SIZE`], and a client bucket of [`RATE_LIMIT_BURST`] requests refilled every
    /// [`RATE_LIMIT_REFILL`] behind [`TRUSTED_PROXIES`].
    Tight,
}

/// `[dashboard] max_viewers` and `retry_after`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewerLimit {
    /// The base configuration's limit.
    Default,
    /// One viewer at a time; the next is turned away with [`VIEWER_RETRY_AFTER`].
    One,
}

/// `[server] certificate_ttl`, `reissue_at_percent` and `certificate_backdate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateLifetime {
    /// The base configuration's lifetime.
    Default,
    /// [`CERTIFICATE_TTL`], reissued at [`REISSUE_AT_PERCENT`], backdated by
    /// [`CERTIFICATE_BACKDATE`].
    Short,
}

/// What a request presents to authenticate itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Presented {
    /// No credential.
    Nothing,
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// The session cookie, as a browser sends it.
    Cookie(SessionCookie),
}

/// The content type and bytes of a request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    /// The value of the `content-type` header.
    pub content_type: &'static str,
    /// The request body bytes.
    pub bytes: Vec<u8>,
}

/// One HTTP request to the public listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// The method.
    pub method: Method,
    /// The path and query, such as `/v1/principals?pageSize=1`.
    pub target: String,
    /// The credential the request carries.
    pub presented: Presented,
    /// Further headers, with lower-case names.
    pub headers: Vec<(&'static str, String)>,
    /// The body and its content type, if present.
    pub body: Option<Body>,
}

impl Request {
    /// A request of `method` for `target` that presents nothing and has no body.
    #[must_use]
    pub fn new(method: Method, target: impl Into<String>) -> Self {
        Self {
            method,
            target: target.into(),
            presented: Presented::Nothing,
            headers: Vec::new(),
            body: None,
        }
    }

    /// A `GET` of `target` that presents nothing.
    #[must_use]
    pub fn get(target: impl Into<String>) -> Self {
        Self::new(Method::GET, target)
    }

    /// The same request presenting `presented`.
    #[must_use]
    pub fn presenting(self, presented: Presented) -> Self {
        Self { presented, ..self }
    }

    /// The same request with the header `name` (lower case) set to `value` as well.
    #[must_use]
    pub fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// The same request with a body of the given content type.
    #[must_use]
    pub fn body(self, content_type: &'static str, bytes: Vec<u8>) -> Self {
        Self {
            body: Some(Body {
                content_type,
                bytes,
            }),
            ..self
        }
    }

    /// The request line, such as `GET /v1/principals`, for failure reasons.
    #[must_use]
    pub fn line(&self) -> String {
        format!("{} {}", self.method, self.target)
    }
}

/// The answer to a [`Request`]: its status, its headers with lower-case names in the order
/// received, and its body exactly as sent, never decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// The HTTP status.
    pub status: u16,
    /// The headers, lower-case names.
    pub headers: Vec<(String, String)>,
    /// The body bytes.
    pub body: Vec<u8>,
}

impl Answer {
    /// An answer with the given status and no headers or body.
    #[must_use]
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// An answer containing a JSON representation of `value`.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be serialized.
    pub fn json(status: u16, value: &impl serde::Serialize) -> Result<Self> {
        let body = serde_json::to_vec(value)
            .map_err(E2eError::system_from("serializing the server answer"))?;
        Ok(Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body,
        })
    }

    /// Adds a header to this answer.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The value of the first header called `name` (lower case), if any.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    /// The body as text, lossily, for failure reasons.
    #[must_use]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// An open `GET /v1/state:watch` response, read as raw bytes so that the SSE fields the frame
/// decoder drops (`retry:`) stay visible.
pub trait RawStream {
    /// The HTTP status of the response.
    fn status(&self) -> u16;

    /// The next chunk of the body, waiting at most `budget`; `None` once the body ended.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`](crate::E2eError::Timeout) when no chunk arrives within `budget`;
    /// otherwise when the connection fails.
    fn next_chunk_within(&mut self, budget: Duration) -> Result<Option<Vec<u8>>>;
}

/// The leaf certificate a server presented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaf {
    /// The serial number, big-endian bytes as encoded.
    pub serial: Vec<u8>,
    /// The start of the validity window.
    pub not_before: SystemTime,
    /// The end of the validity window.
    pub not_after: SystemTime,
}

/// How a fresh TLS connection that trusts only the deployment's CA pin ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handshake {
    /// The server's chain verified against the pin; its leaf.
    Verified(Leaf),
    /// The handshake failed; the error chain.
    Refused(String),
}

/// How the server process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// It exited with this status code.
    Code(i32),
    /// A signal ended it.
    Signal(i32),
}

/// The public listener of a running deployment, as the server scenarios drive it.
pub trait ServerApi: Sync {
    /// The open state stream.
    type Stream: RawStream;
    /// A connection held open across a certificate reissue.
    type Held: Channel;

    /// The administrator of the deployment, logged in.
    ///
    /// # Errors
    ///
    /// When the administrator cannot log in.
    fn administrator_credentials(&self) -> Result<Credentials>;

    /// The browser session cookie of the principal of `who`.
    ///
    /// # Errors
    ///
    /// When the cookie cannot be minted.
    fn cookie_of(&self, who: &Credentials) -> Result<SessionCookie>;

    /// Sends `request` on a fresh HTTP/2 connection over server-TLS that trusts only the
    /// deployment's CA pin, follows no redirect, and returns the whole answer.
    ///
    /// # Errors
    ///
    /// When the request cannot be sent or the answer cannot be read; any status is `Ok`.
    fn send(&self, request: &Request) -> Result<Answer>;

    /// Opens `GET /v1/state:watch` presenting `cookie`, and returns once the headers arrived.
    ///
    /// # Errors
    ///
    /// When the request cannot be sent; any status is `Ok`.
    fn watch(&self, cookie: &SessionCookie) -> Result<Self::Stream>;

    /// Opens a fresh TLS connection that trusts only the deployment's CA pin and reports the leaf
    /// certificate the server presented.
    ///
    /// # Errors
    ///
    /// When the connection cannot be attempted; a failed handshake is `Ok` with
    /// [`Handshake::Refused`].
    fn leaf(&self) -> Result<Handshake>;

    /// Opens an HTTP/2 connection presenting the bearer token of `credentials` and keeps it.
    ///
    /// # Errors
    ///
    /// When the connection cannot be opened or the server refuses it.
    fn hold(&self, credentials: &Credentials) -> Result<Self::Held>;

    /// Returns once the deployment's wall clock has reached `at`: the one wait on a deadline the
    /// deployment published itself.
    fn wait_until(&self, at: SystemTime);

    /// Sends `SIGTERM` to the server and waits at most `timeout` for it to exit.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`](crate::E2eError::Timeout) when it has not exited by then; otherwise
    /// when no server runs or it cannot be signalled.
    fn terminate(&self, timeout: Duration) -> Result<Exit>;
}

#[cfg(test)]
mod tests {
    use serde::{Serialize, Serializer, ser::Error as _};

    use super::Answer;

    /// A value that fails to serialize.
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("serialization failed"))
        }
    }

    #[test]
    fn answers_preserve_status_headers_and_json() -> Result<(), Box<dyn std::error::Error>> {
        let empty = Answer::empty(204).with_header("x-example", "value");
        assert_eq!(empty.status, 204);
        assert_eq!(empty.header("x-example"), Some("value"));
        assert!(empty.body.is_empty());

        let json = Answer::json(201, &[1, 2])?;
        assert_eq!(json.status, 201);
        assert_eq!(json.header("content-type"), Some("application/json"));
        assert_eq!(json.body, b"[1,2]");
        assert!(Answer::json(200, &Unserializable).is_err());
        Ok(())
    }
}
