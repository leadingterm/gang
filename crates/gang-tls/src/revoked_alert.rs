//! The client side of live revocation: learning that a server refused this client's
//! certificate as revoked, whatever the protocol above the TLS stream does with that error.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rustls::AlertDescription;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

/// Raised once a watched stream reads the server's TLS `certificate_revoked` alert. Clones share
/// one signal.
#[derive(Debug, Clone, Default)]
pub struct RevokedAlert {
    shared: Arc<Shared>,
}

/// The first alert read, and the token cancelled once it is set.
#[derive(Debug, Default)]
struct Shared {
    revoked: OnceLock<CertificateRevoked>,
    raised: CancellationToken,
}

impl RevokedAlert {
    /// Wraps `stream` so that a read failing with the `certificate_revoked` alert raises this
    /// signal.
    #[must_use]
    pub fn watch<S>(&self, stream: S) -> AlertWatched<S> {
        AlertWatched {
            stream,
            alert: self.clone(),
        }
    }

    /// Completes once a watched stream has read the alert, with the first alert read.
    pub async fn raised(&self) -> CertificateRevoked {
        loop {
            if let Some(revoked) = self.shared.revoked.get() {
                return revoked.clone();
            }
            self.shared.raised.cancelled().await;
        }
    }

    /// Raises the signal with `revoked` unless it is already raised.
    fn raise(&self, revoked: CertificateRevoked) {
        if self.shared.revoked.set(revoked).is_ok() {
            self.shared.raised.cancel();
        }
    }
}

/// The server refused the client certificate as revoked: the `certificate_revoked` alert it sent.
#[derive(Debug, Clone, thiserror::Error)]
#[error("the server refused the client certificate as revoked")]
pub struct CertificateRevoked(#[source] rustls::Error);

/// A client stream whose failed reads are checked for the server's `certificate_revoked` alert.
pub struct AlertWatched<S> {
    stream: S,
    alert: RevokedAlert,
}

impl<S> AlertWatched<S> {
    /// The watched stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }
}

/// The alert carried by `error` when it is the server's `certificate_revoked` alert.
fn revoked_alert(error: &io::Error) -> Option<CertificateRevoked> {
    match error.get_ref()?.downcast_ref::<rustls::Error>()? {
        alert @ rustls::Error::AlertReceived(AlertDescription::CertificateRevoked) => {
            Some(CertificateRevoked(alert.clone()))
        }
        _ => None,
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for AlertWatched<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let read = Pin::new(&mut self.stream).poll_read(cx, buf);
        if let Poll::Ready(Err(error)) = &read
            && let Some(revoked) = revoked_alert(error)
        {
            self.alert.raise(revoked);
        }
        read
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for AlertWatched<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}
