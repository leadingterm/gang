//! Reads the live state that the server publishes through its API: the snapshot of
//! `GET /v1/state` and the stream of `GET /v1/state:watch`.
//!
//! A plaintext deployment is reached over plain HTTP; a deployment with TLS is reached over
//! server-TLS that trusts the Gang CA, as [`EventFollower`](crate::EventFollower) does. What a
//! request presents is chosen when the API is built: nothing, a bearer token, or a session cookie.

use std::collections::VecDeque;
use std::path::Path;
use std::time::{Duration, Instant};

use gang_state::sse::Decoder;
use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, ServerTrust};
use gang_types::Url;
use gang_wire::routes::{GET_STATE, WATCH_STATE};
use gang_wire::{Frame, State};
use http::Method;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, COOKIE, HeaderName, HeaderValue};

use crate::errors::{E2eError, Result};
use crate::http::{Chunks, Http};
use crate::identity::Credentials;
use crate::session::SessionCookie;
use crate::sse::EventStream;
use crate::tls_client;

/// The status of a response that carries the stream.
const STATUS_OK: u16 = 200;

/// The media type of the stream.
const EVENT_STREAM: &str = "text/event-stream";

/// The state API of a deployment, reached with one fixed way of presenting itself.
pub struct StateApi {
    /// The HTTP client.
    client: Http,
    /// The public endpoint of the server.
    api: Url,
    /// The credential passed with each request, when present.
    credential: Vec<(HeaderName, HeaderValue)>,
}

impl StateApi {
    /// The API at `api`, presenting nothing. Over `https` the server must chain to the Gang CA
    /// certificate at `authority`. `handshake_timeout` bounds connection and response headers.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the TLS client or the HTTP client cannot be built, and
    /// [`E2eError::Io`] when the runtime cannot start.
    pub fn anonymous(
        api: &Url,
        authority: Option<&Path>,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        Self::build(api, authority, None, handshake_timeout)
    }

    /// The API at `api`, presenting the bearer token of `credentials` and trusting their CA.
    /// `handshake_timeout` bounds connection and response headers.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the token is not a header value, the TLS client or the HTTP
    /// client cannot be built, and [`E2eError::Io`] when the runtime cannot start.
    pub fn with_bearer(
        api: &Url,
        credentials: &Credentials,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        let authorization = format!("Bearer {}", credentials.token);
        Self::build(
            api,
            credentials.authority.as_deref(),
            Some((AUTHORIZATION, authorization.as_str())),
            handshake_timeout,
        )
    }

    /// The API at `api`, presenting `cookie`. Over `https` the server must chain to the Gang CA
    /// certificate at `authority`. `handshake_timeout` bounds connection and response headers.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the cookie is not a header value, the TLS client or the HTTP
    /// client cannot be built, and [`E2eError::Io`] when the runtime cannot start.
    pub fn with_cookie(
        api: &Url,
        authority: Option<&Path>,
        cookie: &SessionCookie,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        Self::build(
            api,
            authority,
            Some((COOKIE, cookie.header().as_str())),
            handshake_timeout,
        )
    }

    /// Builds the client and runtime. Requests are bounded through the response head, or through
    /// the body for buffered requests; an opened stream is governed by its caller's deadline.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the TLS client or the HTTP client cannot be built, and
    /// [`E2eError::Io`] when the runtime cannot start.
    fn build(
        api: &Url,
        authority: Option<&Path>,
        credential: Option<(HeaderName, &str)>,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        let pem = if api.scheme() == "https" {
            authority.map(tls_client::certificate_pem).transpose()?
        } else {
            None
        };
        let transport = if api.scheme() == "https" {
            HttpTransport::Tls {
                trust: pem.as_ref().map_or(ServerTrust::System, ServerTrust::Ca),
            }
        } else {
            HttpTransport::Plain
        };
        let client = Http::try_new(transport, HttpProtocol::Http2, handshake_timeout)?;
        let mut headers = Vec::new();
        if let Some((name, value)) = credential {
            let mut value = HeaderValue::from_str(value).map_err(E2eError::system_from(
                "the credential is not a header value",
            ))?;
            value.set_sensitive(true);
            headers.push((name, value));
        }
        Ok(Self {
            client,
            api: api.clone(),
            credential: headers,
        })
    }

    /// The full URL of `path` on the server.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when `path` cannot be joined with the endpoint.
    fn url(&self, path: &str) -> Result<Url> {
        self.api
            .join(path)
            .map_err(E2eError::parse("the state api url"))
    }

    /// The HTTP status of a `GET` of `path`. The body is not read.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the request fails, [`E2eError::Timeout`] when connection or
    /// response headers exceed the configured timeout, and [`E2eError::Parse`] for an invalid path.
    pub fn status(&self, path: &str) -> Result<u16> {
        let url = self.url(path)?;
        self.client
            .open(Self::request(&url, &self.credential))
            .map(|response| response.status().as_u16())
    }

    /// The live state, as `GET /v1/state` answers it.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the request fails or the server answers a non-success status
    /// (the error names the status and body), and [`E2eError::Parse`] when the body is not a
    /// state.
    /// [`E2eError::Timeout`] when the request and response body exceed the configured timeout.
    pub fn state(&self) -> Result<State> {
        let url = self.url(GET_STATE.template)?;
        let reply = self.client.send(Self::request(&url, &self.credential))?;
        let status = reply.status.as_u16();
        if !(200..300).contains(&status) {
            let body = String::from_utf8_lossy(&reply.body);
            return Err(E2eError::system(format!("{url} answered {status}: {body}")));
        }
        serde_json::from_slice(&reply.body).map_err(E2eError::parse("the live state"))
    }

    /// Opens the state stream with a `GET` of `/v1/state:watch` and returns once the server
    /// answered with its headers. A response that is not a `200` has no frames.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the request fails, [`E2eError::Timeout`] when connection or
    /// response headers exceed the configured timeout, and [`E2eError::Parse`] for an invalid path.
    pub fn open_stream(self) -> Result<StateStream> {
        let url = self.url(WATCH_STATE.template)?;
        let mut headers = self.credential.clone();
        headers.push((ACCEPT, HeaderValue::from_static(EVENT_STREAM)));
        let response = self.client.open(Self::request(&url, &headers))?;
        Ok(StateStream {
            status: response.status().as_u16(),
            content_type: response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            response,
            decoder: Decoder::new(),
            pending: VecDeque::new(),
        })
    }

    /// Constructs a GET with the headers selected for this request.
    fn request<'a>(url: &'a Url, headers: &'a [(HeaderName, HeaderValue)]) -> HttpRequest<'a> {
        HttpRequest {
            method: Method::GET,
            url,
            bearer: None,
            headers,
            body: None,
        }
    }
}

/// An open response of the state stream route. Dropping it closes the connection.
pub struct StateStream {
    /// The response, whose body is the stream.
    response: Chunks,
    /// Turns the body into frames.
    decoder: Decoder,
    /// Frames decoded from a chunk and not yet taken.
    pending: VecDeque<Frame>,
    /// The HTTP status of the response.
    status: u16,
    /// The `Content-Type` of the response.
    content_type: Option<String>,
}

impl EventStream for StateStream {
    fn status(&self) -> u16 {
        self.status
    }

    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    fn next_event(&mut self) -> Result<Option<Frame>> {
        self.next_frame(None)
    }

    fn next_event_within(&mut self, budget: Duration) -> Result<Option<Frame>> {
        self.next_frame(Some((budget, Instant::now() + budget)))
    }
}

impl StateStream {
    /// Reads the next frame. With a deadline, every wait for a chunk of the body ends at it.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when no chunk arrives before the deadline;
    /// [`E2eError::System`] when the connection fails; [`E2eError::Parse`] when the bytes are
    /// not frames.
    fn next_frame(&mut self, deadline: Option<(Duration, Instant)>) -> Result<Option<Frame>> {
        if self.status != STATUS_OK {
            return Ok(None);
        }
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            let chunk = match deadline {
                Some((budget, at)) => self
                    .response
                    .next_within(
                        at.saturating_duration_since(Instant::now()),
                        "the state stream to send a frame",
                    )
                    .map_err(|error| match error {
                        E2eError::Timeout { condition, .. } => E2eError::timeout(condition, budget),
                        other => other,
                    })?,
                None => self.response.next()?,
            };
            let Some(chunk) = chunk else {
                return Ok(None);
            };
            let frames = self
                .decoder
                .push(&chunk)
                .map_err(E2eError::parse("the state stream"))?;
            self.pending.extend(frames);
        }
    }
}
