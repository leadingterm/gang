//! One HTTP client for Gang: plain HTTP, or HTTPS through this crate's [`TlsConnector`].
//!
//! Buffered requests enforce a whole-request timeout and a response-body cap. Streaming requests
//! bound only the response head; callers bound their own body reads. Redirects are never followed.
//!
//! ```
//! use std::sync::Arc;
//! use gang_clock::SystemClock;
//! use gang_tls::{HttpClient, HttpError, HttpProtocol, HttpRequest, HttpSettings, HttpTransport};
//! use gang_types::{NonZeroBytes, NonZeroDuration, Url};
//!
//! let settings = HttpSettings {
//!     transport: HttpTransport::Plain,
//!     protocol: HttpProtocol::Negotiated,
//!     timeout: NonZeroDuration::from_secs(5),
//!     max_body: NonZeroBytes::kib(1),
//!     clock: Arc::new(SystemClock::default()),
//! };
//! let client = HttpClient::try_new(&settings)?;
//! let url = Url::parse("https://localhost/")?;
//! let request = HttpRequest {
//!     method: http::Method::GET,
//!     url: &url,
//!     bearer: None,
//!     headers: &[],
//!     body: None,
//! };
//! let outcome = tokio::runtime::Builder::new_current_thread()
//!     .enable_all()
//!     .build()?
//!     .block_on(client.send(request));
//! assert!(matches!(outcome, Err(HttpError::UnsupportedScheme { .. })));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use gang_clock::Clock;
use gang_types::{AccessToken, NonZeroBytes, NonZeroDuration, Url};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tower::Service;

use crate::acceptor::ALPN_H2;
use crate::{Alpn, ConnectorSettings, ServerTrust, TlsConnector, TransportError};

/// How a client reaches servers.
pub enum HttpTransport<'a> {
    /// Plain TCP. Refuses `https` URLs; builds no TLS configuration.
    Plain,
    /// TLS 1.3 trusting servers as `trust` says. Refuses `http` URLs.
    Tls {
        /// How the server certificate is trusted.
        trust: ServerTrust<'a>,
    },
}

/// The URL scheme a transport serves: each [`HttpTransport`] serves exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpScheme {
    /// `http`, served by [`HttpTransport::Plain`].
    Http,
    /// `https`, served by [`HttpTransport::Tls`].
    Https,
}

impl HttpScheme {
    /// The scheme of `url`; `None` when it is neither `http` nor `https`.
    #[must_use]
    pub fn of(url: &Url) -> Option<Self> {
        match url.scheme() {
            "http" => Some(Self::Http),
            "https" => Some(Self::Https),
            _ => None,
        }
    }
}

impl HttpTransport<'_> {
    /// The scheme this transport serves.
    #[must_use]
    pub fn scheme(&self) -> HttpScheme {
        match self {
            Self::Plain => HttpScheme::Http,
            Self::Tls { .. } => HttpScheme::Https,
        }
    }
}

/// The HTTP versions a client uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProtocol {
    /// HTTP/2 only: `h2` over TLS, or prior knowledge (h2c) over plain TCP.
    Http2,
    /// `h2` then `http/1.1` over TLS, with hyper choosing from the negotiated protocol; HTTP/1.1
    /// over plain TCP.
    Negotiated,
}

/// Settings of an [`HttpClient`].
pub struct HttpSettings<'a> {
    /// How servers are reached and trusted.
    pub transport: HttpTransport<'a>,
    /// The HTTP versions used.
    pub protocol: HttpProtocol,
    /// Limit on the whole buffered request, or through the response head for a stream.
    pub timeout: NonZeroDuration,
    /// Largest response body accepted by buffered requests.
    pub max_body: NonZeroBytes,
    /// Clock used for request deadlines and TLS certificate validity.
    pub clock: Arc<dyn Clock>,
}

/// A request body and the content type that describes it.
pub struct RequestBody {
    /// Value of the `content-type` header.
    pub content_type: &'static str,
    /// The body bytes.
    pub bytes: Bytes,
}

/// A request to send.
pub struct HttpRequest<'a> {
    /// The request method.
    pub method: Method,
    /// The absolute URL to request.
    pub url: &'a Url,
    /// Bearer token sent in the `authorization` header, when present.
    pub bearer: Option<&'a AccessToken>,
    /// Extra request headers, sent as given after bearer and content-type headers.
    pub headers: &'a [(HeaderName, HeaderValue)],
    /// Request body, when present.
    pub body: Option<RequestBody>,
}

/// A complete response.
#[derive(Debug)]
pub struct HttpReply {
    /// The response status.
    pub status: StatusCode,
    /// The response headers.
    pub headers: HeaderMap,
    /// The response body, within the configured cap.
    pub body: Bytes,
}

/// A response whose body can be read incrementally without a timeout or size cap.
#[derive(Debug)]
pub struct HttpStream {
    /// The response status.
    pub status: StatusCode,
    /// The response headers.
    pub headers: HeaderMap,
    body: Incoming,
}

impl HttpStream {
    /// Returns the next data frame, skipping trailers, or `None` at the end of the body.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Body`] if reading the body fails.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, HttpError> {
        while let Some(frame) = self.body.frame().await {
            let frame = frame.map_err(|error| HttpError::Body(Box::new(error)))?;
            if let Ok(data) = frame.into_data() {
                return Ok(Some(data));
            }
        }
        Ok(None)
    }
}

/// An error raised while sending a request.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// The TLS configuration could not be built.
    #[error("tls configuration failed")]
    Tls(#[from] TransportError),
    /// The URL scheme is not served by this client's transport.
    #[error("url scheme `{scheme}` is not served by this client")]
    UnsupportedScheme {
        /// The scheme of the rejected URL.
        scheme: String,
    },
    /// The request could not be built.
    #[error("request could not be built")]
    Request(#[source] http::Error),
    /// The connection or protocol failed.
    #[error("request failed")]
    Connect(#[source] hyper_util::client::legacy::Error),
    /// Reading the response body failed.
    #[error("reading the response body failed")]
    Body(#[source] Box<dyn Error + Send + Sync>),
    /// The response head arrived, but its body exceeds the configured cap.
    #[error("response body exceeds the {limit} limit")]
    BodyTooLarge {
        /// The status of the oversized response.
        status: StatusCode,
        /// The cap that was exceeded, the client's `max_body`.
        limit: NonZeroBytes,
    },
    /// The request timeout elapsed.
    #[error("request timed out")]
    TimedOut,
}

type BoxError = Box<dyn Error + Send + Sync>;

/// A connection to a server, over TCP or over TLS.
enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl tokio::io::AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        match self {
            Self::Plain(_) => Connected::new(),
            Self::Tls(stream) => {
                let connected = Connected::new();
                if stream.get_ref().1.alpn_protocol() == Some(ALPN_H2) {
                    connected.negotiated_h2()
                } else {
                    connected
                }
            }
        }
    }
}

/// Opens connections: TCP, then TLS when a connector is configured for `https` URIs.
#[derive(Clone)]
struct Connector {
    http: HttpConnector,
    tls: Option<TlsConnector>,
}

type ConnectFuture = Pin<Box<dyn Future<Output = Result<TokioIo<Stream>, BoxError>> + Send>>;

impl Service<Uri> for Connector {
    type Response = TokioIo<Stream>;
    type Error = BoxError;
    type Future = ConnectFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(connect(self.http.clone(), self.tls.clone(), uri))
    }
}

/// Connects TCP to `uri`, then runs TLS when `tls` is set.
/// [`HttpClient::prepare`] has checked that the URL scheme is the one the transport serves.
async fn connect(
    mut http: HttpConnector,
    tls: Option<TlsConnector>,
    uri: Uri,
) -> Result<TokioIo<Stream>, BoxError> {
    let tcp = Service::call(&mut http, uri.clone()).await?.into_inner();
    let stream = match tls {
        Some(tls) => {
            let host = uri.host().ok_or("uri has no host")?;
            let host = host
                .strip_prefix('[')
                .and_then(|inner| inner.strip_suffix(']'))
                .unwrap_or(host);
            let name = ServerName::try_from(host.to_owned())?;
            Stream::Tls(Box::new(tls.connect(name, tcp).await?))
        }
        None => Stream::Plain(tcp),
    };
    Ok(TokioIo::new(stream))
}

/// A client that sends requests over plain HTTP or HTTPS.
pub struct HttpClient {
    client: Client<Connector, Full<Bytes>>,
    /// The scheme the transport serves; requests to any other scheme are refused.
    scheme: HttpScheme,
    timeout: NonZeroDuration,
    /// Largest response body accepted.
    max_body: NonZeroBytes,
    clock: Arc<dyn Clock>,
}

impl HttpClient {
    /// Builds a client for `settings`.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Tls`] when the TLS configuration cannot be built.
    pub fn try_new(settings: &HttpSettings<'_>) -> Result<Self, HttpError> {
        let alpn = match settings.protocol {
            HttpProtocol::Http2 => Alpn::H2,
            HttpProtocol::Negotiated => Alpn::H2AndHttp1,
        };
        let tls = match &settings.transport {
            HttpTransport::Plain => None,
            HttpTransport::Tls { trust } => {
                let connector = TlsConnector::try_new(&ConnectorSettings {
                    trust: *trust,
                    identity: None,
                    handshake_timeout: settings.timeout,
                    alpn,
                    clock: Arc::clone(&settings.clock),
                })?;
                Some(connector)
            }
        };
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .http2_only(settings.protocol == HttpProtocol::Http2)
            .build::<_, Full<Bytes>>(Connector { http, tls });
        Ok(Self {
            client,
            scheme: settings.transport.scheme(),
            timeout: settings.timeout,
            max_body: settings.max_body,
            clock: Arc::clone(&settings.clock),
        })
    }

    /// Sends `request` and reads the whole reply, within the configured timeout and body cap.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::UnsupportedScheme`] before any I/O when the URL scheme does not match
    /// the transport, and the other [`HttpError`] variants for request, connection, body and
    /// timeout failures.
    pub async fn send(&self, request: HttpRequest<'_>) -> Result<HttpReply, HttpError> {
        let built = self.prepare(request)?;
        match Clock::timeout(&self.clock, self.timeout.get(), self.exchange(built)).await {
            Ok(reply) => reply,
            Err(_) => Err(HttpError::TimedOut),
        }
    }

    /// Opens a response for long-lived streams such as server-sent events.
    ///
    /// The configured timeout covers connection, request, and response head only. The body has
    /// no size cap or timeout; the caller must bound its own reads.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::UnsupportedScheme`] before I/O for a mismatched scheme, or another
    /// [`HttpError`] for request, connection, or timeout failures.
    pub async fn open(&self, request: HttpRequest<'_>) -> Result<HttpStream, HttpError> {
        let built = self.prepare(request)?;
        match Clock::timeout(&self.clock, self.timeout.get(), self.head(built)).await {
            Ok(response) => response,
            Err(_) => Err(HttpError::TimedOut),
        }
    }

    /// Validates the transport scheme and builds the request before any I/O.
    fn prepare(&self, request: HttpRequest<'_>) -> Result<http::Request<Full<Bytes>>, HttpError> {
        if HttpScheme::of(request.url) != Some(self.scheme) {
            return Err(HttpError::UnsupportedScheme {
                scheme: request.url.scheme().to_owned(),
            });
        }
        Self::build_request(request)
    }

    fn build_request(request: HttpRequest<'_>) -> Result<http::Request<Full<Bytes>>, HttpError> {
        let mut builder = http::Request::builder()
            .method(request.method)
            .uri(request.url.as_str());
        if let Some(token) = request.bearer {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
                .map_err(|error| HttpError::Request(error.into()))?;
            value.set_sensitive(true);
            builder = builder.header(AUTHORIZATION, value);
        }
        let body = match request.body {
            Some(body) => {
                builder = builder.header(CONTENT_TYPE, body.content_type);
                body.bytes
            }
            None => Bytes::new(),
        };
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        builder.body(Full::new(body)).map_err(HttpError::Request)
    }

    /// Acquires a response head without consuming its body.
    async fn head(&self, request: http::Request<Full<Bytes>>) -> Result<HttpStream, HttpError> {
        let response = self
            .client
            .request(request)
            .await
            .map_err(HttpError::Connect)?;
        let (parts, body) = response.into_parts();
        Ok(HttpStream {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    }

    async fn exchange(&self, request: http::Request<Full<Bytes>>) -> Result<HttpReply, HttpError> {
        let HttpStream {
            status,
            headers,
            body,
        } = self.head(request).await?;
        let body = Limited::new(body, self.max_body.get().as_usize())
            .collect()
            .await
            .map_err(|error| {
                if error.downcast_ref::<LengthLimitError>().is_some() {
                    HttpError::BodyTooLarge {
                        status,
                        limit: self.max_body,
                    }
                } else {
                    HttpError::Body(error)
                }
            })?
            .to_bytes();
        Ok(HttpReply {
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_http_error_repeats_its_source() {
        let request_error = http::Request::builder()
            .method("invalid method")
            .body(())
            .expect_err("a method containing a space is invalid");
        // hyper_util::client::legacy::Error has no public constructor.
        let errors = [
            HttpError::Tls(TransportError::HandshakeTimeout),
            HttpError::UnsupportedScheme {
                scheme: "ftp".to_owned(),
            },
            HttpError::Request(request_error),
            HttpError::Body(Box::new(std::io::Error::other("connection broke"))),
            HttpError::BodyTooLarge {
                status: StatusCode::OK,
                limit: NonZeroBytes::kib(1),
            },
            HttpError::TimedOut,
        ];

        gang_testkit::assert_sources_not_repeated(&errors);
    }
}
