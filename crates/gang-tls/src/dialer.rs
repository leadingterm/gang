//! Opening client connections: TCP, then TLS through this crate's [`TlsConnector`].

use std::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tower::Service;

use crate::TlsConnector;
use crate::acceptor::ALPN_H2;
use crate::revoked_alert::{AlertWatched, RevokedAlert};

type BoxError = Box<dyn Error + Send + Sync>;

/// A connection opened by a [`Dialer`]: TCP, or TLS over TCP watched for the server's
/// `certificate_revoked` alert.
pub struct DialedStream(Stream);

enum Stream {
    Plain(TcpStream),
    Tls(Box<AlertWatched<TlsStream<TcpStream>>>),
}

impl tokio::io::AsyncRead for DialedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().0 {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for DialedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.get_mut().0 {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().0 {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().0 {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Connection for DialedStream {
    fn connected(&self) -> Connected {
        match &self.0 {
            Stream::Plain(_) => Connected::new(),
            Stream::Tls(stream) => {
                let connected = Connected::new();
                if stream.get_ref().get_ref().1.alpn_protocol() == Some(ALPN_H2) {
                    connected.negotiated_h2()
                } else {
                    connected
                }
            }
        }
    }
}

/// Opens connections to the host and port of a URI: TCP, then TLS when built with a
/// [`TlsConnector`]. The URI's scheme is not consulted: the caller chooses the transport.
/// Every TLS stream reports the server's `certificate_revoked` alert to [`Dialer::revoked`].
#[derive(Clone)]
pub struct Dialer {
    http: HttpConnector,
    tls: Option<TlsConnector>,
    revoked: RevokedAlert,
}

impl Dialer {
    /// A dialer that runs TLS through `tls` when it is set.
    #[must_use]
    pub fn new(tls: Option<TlsConnector>) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        Self {
            http,
            tls,
            revoked: RevokedAlert::default(),
        }
    }

    /// Sends TCP keepalive probes on dialed connections idle for `interval`.
    #[must_use]
    pub fn with_tcp_keepalive(mut self, interval: Duration) -> Self {
        self.http.set_keepalive(Some(interval));
        self
    }

    /// Raised when a server refuses this dialer's client certificate as revoked.
    #[must_use]
    pub fn revoked(&self) -> &RevokedAlert {
        &self.revoked
    }
}

type ConnectFuture = Pin<Box<dyn Future<Output = Result<TokioIo<DialedStream>, BoxError>> + Send>>;

impl Service<Uri> for Dialer {
    type Response = TokioIo<DialedStream>;
    type Error = BoxError;
    type Future = ConnectFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(connect(
            self.http.clone(),
            self.tls.clone(),
            self.revoked.clone(),
            uri,
        ))
    }
}

async fn connect(
    mut http: HttpConnector,
    tls: Option<TlsConnector>,
    revoked: RevokedAlert,
    uri: Uri,
) -> Result<TokioIo<DialedStream>, BoxError> {
    let tcp = Service::call(&mut http, uri.clone()).await?.into_inner();
    let stream = match tls {
        Some(tls) => {
            let host = uri.host().ok_or("uri has no host")?;
            let host = host
                .strip_prefix('[')
                .and_then(|inner| inner.strip_suffix(']'))
                .unwrap_or(host);
            let name = ServerName::try_from(host.to_owned())?;
            Stream::Tls(Box::new(revoked.watch(tls.connect(name, tcp).await?)))
        }
        None => Stream::Plain(tcp),
    };
    Ok(TokioIo::new(DialedStream(stream)))
}
