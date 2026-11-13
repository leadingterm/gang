//! The harness's HTTP client: one `gang_tls::HttpClient` and the runtime that drives it, so
//! blocking harness code sends requests and reads streamed responses.

use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gang_clock::SystemClock;
use gang_tls::{
    HttpClient, HttpError, HttpProtocol, HttpReply, HttpRequest, HttpSettings, HttpStream,
    HttpTransport, TransportError,
};
use gang_types::NonZeroDuration;
use http::{HeaderMap, StatusCode};
use tokio::runtime::{Builder, Runtime};

use crate::errors::{E2eError, Result};

/// The largest buffered response body accepted by the harness.
const MAX_BODY: gang_types::NonZeroBytes = gang_types::NonZeroBytes::mib(16);

/// A blocking client and the runtime that drives its requests; the runtime has its own worker
/// thread, so pooled connections keep answering the server's keep-alive pings between requests,
/// as a browser's do.
pub(crate) struct Http {
    runtime: Arc<Runtime>,
    client: HttpClient,
    timeout: Duration,
}

impl Http {
    /// Builds a client for the given transport, protocol and request timeout.
    pub(crate) fn try_new(
        transport: HttpTransport<'_>,
        protocol: HttpProtocol,
        timeout: Duration,
    ) -> Result<Self> {
        let timeout_setting = NonZeroDuration::try_new(timeout)
            .map_err(E2eError::system_from("invalid http timeout"))?;
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("gang-e2e-http")
                .enable_all()
                .build()
                .map_err(E2eError::io("starting the http runtime"))?,
        );
        let client = HttpClient::try_new(&HttpSettings {
            transport,
            protocol,
            timeout: timeout_setting,
            max_body: MAX_BODY,
            clock: Arc::new(SystemClock::new()),
        })
        .map_err(E2eError::system_from("building the http client"))?;
        Ok(Self {
            runtime,
            client,
            timeout,
        })
    }

    /// Sends a request and reads its complete, size-limited response.
    pub(crate) fn send(&self, request: HttpRequest<'_>) -> Result<HttpReply> {
        let url = request.url;
        self.runtime
            .block_on(self.client.send(request))
            .map_err(|error| self.request_error(error, url))
    }

    /// Opens a response whose body is read chunk by chunk.
    pub(crate) fn open(&self, request: HttpRequest<'_>) -> Result<Chunks> {
        let url = request.url;
        let stream = self
            .runtime
            .block_on(self.client.open(request))
            .map_err(|error| self.request_error(error, url))?;
        Ok(Chunks {
            runtime: Arc::clone(&self.runtime),
            stream,
        })
    }

    /// Preserves the source of HTTP failures and identifies request timeouts.
    fn request_error(&self, error: HttpError, url: &gang_types::Url) -> E2eError {
        if matches!(error, HttpError::TimedOut) || is_handshake_timeout(&error) {
            E2eError::timeout(format!("a reply from {url}"), self.timeout)
        } else {
            E2eError::system_from(format!("requesting {url}"))(error)
        }
    }
}

/// Finds a TLS handshake timeout through the HTTP connector's wrapped errors.
fn is_handshake_timeout(error: &(dyn Error + 'static)) -> bool {
    if matches!(
        error.downcast_ref::<TransportError>(),
        Some(TransportError::HandshakeTimeout)
    ) {
        return true;
    }
    // `io::Error::source` can skip its wrapped error itself.
    if error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref)
        .is_some_and(|inner| is_handshake_timeout(inner))
    {
        return true;
    }
    error.source().is_some_and(is_handshake_timeout)
}

/// A streamed response and the runtime that drives its body.
pub(crate) struct Chunks {
    runtime: Arc<Runtime>,
    stream: HttpStream,
}

/// What reading the next chunk of a stream gave.
pub(crate) enum Read {
    /// A chunk of the body.
    Chunk(Bytes),
    /// The end of the body.
    Ended,
    /// The stop was notified first.
    Stopped,
}

impl Chunks {
    /// Returns the response status.
    #[must_use]
    pub(crate) fn status(&self) -> StatusCode {
        self.stream.status
    }

    /// Returns the response headers.
    #[must_use]
    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.stream.headers
    }

    /// Reads the next chunk without a deadline, or returns `None` at end of body.
    pub(crate) fn next(&mut self) -> Result<Option<Bytes>> {
        self.runtime
            .block_on(self.stream.next_chunk())
            .map_err(E2eError::system_from("reading the response"))
    }

    /// Reads the next chunk within `budget`, reporting `condition` on timeout.
    pub(crate) fn next_within(
        &mut self,
        budget: Duration,
        condition: &str,
    ) -> Result<Option<Bytes>> {
        self.runtime
            .block_on(async { tokio::time::timeout(budget, self.stream.next_chunk()).await })
            .map_err(|_| E2eError::timeout(condition, budget))?
            .map_err(E2eError::system_from("reading the response"))
    }

    /// Reads the next chunk within `budget`, or stops without reading when `stop` is notified;
    /// reports `condition` on timeout.
    pub(crate) fn next_within_or_stop(
        &mut self,
        budget: Duration,
        condition: &str,
        stop: &tokio::sync::Notify,
    ) -> Result<Read> {
        self.runtime.block_on(async {
            tokio::select! {
                biased;
                () = stop.notified() => Ok(Read::Stopped),
                chunk = tokio::time::timeout(budget, self.stream.next_chunk()) => {
                    chunk
                        .map_err(|_| E2eError::timeout(condition, budget))?
                        .map(|chunk| chunk.map_or(Read::Ended, Read::Chunk))
                        .map_err(E2eError::system_from("reading the response"))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::time::Duration;

    use gang_tls::{HttpError, HttpProtocol, HttpRequest, HttpTransport, TransportError};
    use gang_types::Url;
    use http::Method;

    use super::{Http, is_handshake_timeout};
    use crate::errors::E2eError;

    #[test]
    fn handshake_timeout_is_found_through_io_wrappers() {
        let error = io::Error::other(io::Error::other(TransportError::HandshakeTimeout));
        assert!(is_handshake_timeout(&error));
        let other = io::Error::other(TransportError::NoCertificate);
        assert!(!is_handshake_timeout(&other));
    }

    #[test]
    fn handshake_timeout_maps_to_request_timeout() {
        let timeout = Duration::from_secs(1);
        let client = Http::try_new(HttpTransport::Plain, HttpProtocol::Http2, timeout)
            .expect("the plain client is built");
        let url = Url::parse("https://localhost/").expect("the url is valid");
        let error = client.request_error(HttpError::Tls(TransportError::HandshakeTimeout), &url);
        assert!(matches!(
            error,
            E2eError::Timeout { waited, .. } if waited == timeout
        ));
    }

    #[test]
    fn mismatched_scheme_is_rejected_before_io() {
        let client = Http::try_new(
            HttpTransport::Plain,
            HttpProtocol::Http2,
            Duration::from_secs(1),
        )
        .expect("the plain client is built");
        let url = Url::parse("https://localhost/").expect("the url is valid");
        let request = || HttpRequest {
            method: Method::GET,
            url: &url,
            bearer: None,
            headers: &[],
            body: None,
        };
        for error in [client.send(request()).err(), client.open(request()).err()] {
            let error = error.expect("the scheme is rejected");
            assert!(matches!(error, E2eError::System { .. }));
            assert!(matches!(
                error
                    .source()
                    .and_then(|source| source.downcast_ref::<HttpError>()),
                Some(HttpError::UnsupportedScheme { .. })
            ));
        }
    }
}
