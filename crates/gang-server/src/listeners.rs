//! Listening endpoints: plain TCP, or TLS whose server certificate is reissued before it expires.
//! Reissuing rebuilds the acceptor on the same bound listener, so established connections are
//! never disturbed.

use std::collections::VecDeque;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::{FutureExt, Stream, StreamExt, stream::BoxStream};
use gang_clock::{Clock, Tick};
use gang_mtls::{CertificateAuthority, KeyPair, ServerCertificateRequest, SubjectName};
use gang_spi::{Accepted, CertificateSignerPort, Peer};
use gang_tls::{
    AcceptorSettings, Alpn, ClientAuth, IncomingSettings, Listener, RevocableStream, RevocationSet,
    TlsAcceptor, accept_retrying,
};
use gang_types::{NonZeroDuration, Percent};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, oneshot, watch};

use crate::errors::ServerError;
use crate::settings::HandshakeSettings;

/// How a TLS endpoint names its certificate and authenticates its clients.
pub(crate) struct TlsPolicy {
    /// Names the server certificate is issued for; the first is its common name.
    pub(crate) names: Vec<SubjectName>,
    /// Whether clients must present a certificate.
    pub(crate) client_auth: ClientAuth,
    /// Serials of client certificates that are refused and cut off live.
    pub(crate) revoked_certificates: RevocationSet,
    /// Lifetime of each server certificate.
    pub(crate) certificate_ttl: NonZeroDuration,
    /// How far through its lifetime a server certificate is reissued.
    pub(crate) reissue_at_percent: Percent,
    /// Handshake limits.
    pub(crate) handshake: HandshakeSettings,
    /// How long a listener waits after a failed accept before accepting again.
    pub(crate) accept_error_backoff: NonZeroDuration,
    /// The application protocols the listener offers during the handshake.
    pub(crate) alpn: Alpn,
}

/// A listener that accepts connections: plain TCP, or TLS under the certificate authority.
pub(crate) enum Accepting<L: Listener, S, C> {
    /// Connections are served as received, with no verified peer.
    Plain {
        /// The raw listener.
        listener: L,
        /// Pause after a failed accept before the next attempt.
        backoff: NonZeroDuration,
        /// Clock used for accept-error backoff.
        clock: Arc<C>,
    },
    /// Connections complete a TLS handshake before they are returned.
    Tls(Box<TlsEndpoint<L, S, C>>),
}

impl<L: Listener, S: CertificateSignerPort, C: Clock + 'static> Accepting<L, S, C> {
    /// A plain endpoint on `listener`; failed accepts pause for `accept_error_backoff`.
    #[must_use]
    pub(crate) fn plain(listener: L, accept_error_backoff: NonZeroDuration, clock: Arc<C>) -> Self {
        Self::Plain {
            listener,
            backoff: accept_error_backoff,
            clock,
        }
    }

    /// A TLS endpoint on `listener`, presenting a certificate `authority` issues for `policy`.
    ///
    /// # Errors
    ///
    /// Returns an error when the server certificate cannot be issued or the acceptor cannot be
    /// built.
    pub(crate) async fn secured(
        listener: L,
        authority: Arc<CertificateAuthority<S, Arc<C>>>,
        policy: TlsPolicy,
        clock: Arc<C>,
    ) -> Result<Self, ServerError> {
        let listener = Arc::new(Mutex::new(listener));
        let reissue_after = policy
            .reissue_at_percent
            .of_duration(policy.certificate_ttl.get());
        let (incoming, rotate_at) =
            issue(&listener, &authority, &policy, reissue_after, &clock).await?;
        Ok(Self::Tls(Box::new(TlsEndpoint {
            listener,
            authority,
            policy,
            clock,
            reissue_after,
            incoming,
            rotate_at,
            retiring: Vec::new(),
            drained: VecDeque::new(),
        })))
    }

    /// Waits for the next accepted connection; `None` only when the listener can accept no more.
    pub(crate) async fn accept(&mut self) -> Option<Accepted> {
        match self {
            Self::Plain {
                listener,
                backoff,
                clock,
            } => {
                let (io, addr) = accept_retrying(listener, *backoff, clock, |error| {
                    tracing::warn!(%error, "cannot accept a connection");
                })
                .await;
                Some(Accepted {
                    connection: Box::new(io),
                    peer: Peer {
                        certificate: None,
                        address: L::peer_ip(&addr),
                    },
                })
            }
            Self::Tls(endpoint) => endpoint.accept().await,
        }
    }
}

impl<L: Listener, S: CertificateSignerPort + 'static, C: Clock + 'static> Accepting<L, S, C> {
    /// The listener as a stream of its connections, owning it. Each item is one accepted
    /// connection. The stream ends only when the listener can accept no more, and then fires
    /// `ended`.
    #[must_use]
    pub(crate) fn into_stream(self, ended: oneshot::Sender<()>) -> BoxStream<'static, Accepted> {
        futures::stream::unfold(
            (self, Some(ended)),
            |(mut endpoint, mut ended)| async move {
                let Some(accepted) = endpoint.accept().await else {
                    if let Some(ended) = ended.take() {
                        let _ = ended.send(());
                    }
                    return None;
                };
                Some((accepted, (endpoint, ended)))
            },
        )
        .boxed()
    }
}

/// Connections that completed a TLS handshake, as a type-erased stream.
type Established = Pin<Box<dyn Stream<Item = Accepted> + Send>>;

/// A TLS acceptor: its established connections and the sender that retires it. Dropping `retire`
/// retires the acceptor, so its listener accepts no new sockets.
struct Acceptor {
    incoming: Established,
    retire: watch::Sender<()>,
}

/// An acceptor replaced by a reissue. It accepts no new connections and only finishes the
/// handshakes it already holds, until `deadline`, one handshake timeout after the reissue, which
/// bounds every handshake it holds.
struct Retiring {
    incoming: Established,
    deadline: Tick,
}

/// A TLS endpoint: its acceptor completes handshakes, and its certificate is reissued at
/// `rotate_at`.
pub(crate) struct TlsEndpoint<L: Listener, S, C> {
    listener: Arc<Mutex<L>>,
    authority: Arc<CertificateAuthority<S, Arc<C>>>,
    policy: TlsPolicy,
    clock: Arc<C>,
    reissue_after: Duration,
    incoming: Acceptor,
    rotate_at: Tick,
    retiring: Vec<Retiring>,
    /// Connections taken from retiring acceptors at their deadline, delivered before anything else.
    drained: VecDeque<Accepted>,
}

impl<L: Listener, S: CertificateSignerPort, C: Clock + 'static> TlsEndpoint<L, S, C> {
    async fn accept(&mut self) -> Option<Accepted> {
        loop {
            if let Some(accepted) = self.drained.pop_front() {
                return Some(accepted);
            }
            let retire_at = self.retiring.iter().map(|retiring| retiring.deadline).min();
            tokio::select! {
                biased;
                () = sleep_until_or_forever(&*self.clock, Some(self.rotate_at)) => self.rotate().await,
                () = sleep_until_or_forever(&*self.clock, retire_at) => self.retire_expired(),
                accepted = next_connection(&mut self.incoming.incoming, &mut self.retiring) => return accepted,
            }
        }
    }

    /// Replaces the acceptor with one holding a fresh certificate. The replaced acceptor accepts
    /// no new connections; it only finishes the handshakes it already holds, until its retirement
    /// deadline. A failure keeps the current acceptor and retries after the accept error backoff.
    async fn rotate(&mut self) {
        match issue(
            &self.listener,
            &self.authority,
            &self.policy,
            self.reissue_after,
            &self.clock,
        )
        .await
        {
            Ok((acceptor, rotate_at)) => {
                let replaced = std::mem::replace(&mut self.incoming, acceptor);
                drop(replaced.retire);
                self.retiring.push(Retiring {
                    incoming: replaced.incoming,
                    deadline: self.clock.monotonic()
                        + self.policy.handshake.handshake_timeout.get(),
                });
                self.rotate_at = rotate_at;
            }
            Err(error) => {
                tracing::error!(%error, "cannot reissue the server certificate; keeping the current one");
                self.rotate_at = self.clock.monotonic() + self.policy.accept_error_backoff.get();
            }
        }
    }

    /// Drops every retiring acceptor past its deadline, first taking the connections it has
    /// already buffered.
    fn retire_expired(&mut self) {
        let now = self.clock.monotonic();
        let mut index = 0;
        while index < self.retiring.len() {
            if self.retiring[index].deadline > now {
                index += 1;
                continue;
            }
            let mut retired = self.retiring.remove(index);
            while let Some(Some(accepted)) = retired.incoming.next().now_or_never() {
                self.drained.push_back(accepted);
            }
        }
    }
}

/// Polls the current acceptor, then each retiring one, for the next connection. Ends when the
/// current acceptor ends; a retiring acceptor that ends is forgotten.
async fn next_connection(
    current: &mut Established,
    retiring: &mut Vec<Retiring>,
) -> Option<Accepted> {
    std::future::poll_fn(|cx| {
        if let Poll::Ready(accepted) = current.poll_next_unpin(cx) {
            return Poll::Ready(accepted);
        }
        let mut index = 0;
        while index < retiring.len() {
            match retiring[index].incoming.poll_next_unpin(cx) {
                Poll::Ready(Some(accepted)) => return Poll::Ready(Some(accepted)),
                Poll::Ready(None) => {
                    retiring.remove(index);
                }
                Poll::Pending => index += 1,
            }
        }
        Poll::Pending
    })
    .await
}

/// Sleeps until `deadline` on `clock`, or forever when there is none.
async fn sleep_until_or_forever(clock: &impl Clock, deadline: Option<Tick>) {
    match deadline {
        Some(deadline) => clock.sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Issues a server certificate for `policy` and builds an acceptor over the shared `listener`.
/// Returns the acceptor and the instant the certificate is due for reissue.
async fn issue<L, S, C>(
    listener: &Arc<Mutex<L>>,
    authority: &CertificateAuthority<S, Arc<C>>,
    policy: &TlsPolicy,
    reissue_after: Duration,
    clock: &Arc<C>,
) -> Result<(Acceptor, Tick), ServerError>
where
    L: Listener,
    S: CertificateSignerPort,
    C: Clock + 'static,
{
    let key = KeyPair::generate().map_err(ServerError::ServerCertificate)?;
    let csr = key
        .certificate_request()
        .map_err(ServerError::ServerCertificate)?;
    let issued = authority
        .issue_server(&ServerCertificateRequest {
            csr: &csr,
            names: &policy.names,
            lifetime: policy.certificate_ttl,
        })
        .await
        .map_err(ServerError::ServerCertificate)?;
    let rotate_at = clock.monotonic() + reissue_after;
    let acceptor = TlsAcceptor::try_new(&AcceptorSettings {
        certificate: &issued.certificate,
        private_key: key.private_key(),
        client_ca: authority.certificate(),
        revocations: &policy.revoked_certificates,
        handshake_timeout: policy.handshake.handshake_timeout,
        client_auth: policy.client_auth,
        alpn: policy.alpn,
        clock: Arc::<C>::clone(clock),
    })?;
    let (retire, retired) = watch::channel(());
    let incoming = acceptor.incoming(
        SharedListener {
            listener: Arc::clone(listener),
            retired,
        },
        IncomingSettings {
            max_pending_handshakes: policy.handshake.max_pending_handshakes,
            accept_error_backoff: policy.accept_error_backoff,
        },
        |failure| tracing::debug!(?failure, "a connection did not complete its handshake"),
    );
    let established: Established = Box::pin(StreamExt::map(incoming, to_accepted));
    Ok((
        Acceptor {
            incoming: established,
            retire,
        },
        rotate_at,
    ))
}

fn to_accepted<IO>(stream: RevocableStream<IO>) -> Accepted
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let peer = Peer {
        certificate: stream.peer_serial().cloned(),
        address: stream.address(),
    };
    Accepted {
        connection: Box::new(stream),
        peer,
    }
}

/// A listener shared by the acceptors built over it; it stops accepting once `retired` ends.
struct SharedListener<L> {
    listener: Arc<Mutex<L>>,
    retired: watch::Receiver<()>,
}

impl<L: Listener> Listener for SharedListener<L> {
    type Io = L::Io;
    type Addr = L::Addr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        let Self { listener, retired } = self;
        tokio::select! {
            biased;
            _ = retired.changed() => std::future::pending().await,
            accepted = async { listener.lock().await.accept().await } => accepted,
        }
    }

    fn peer_ip(addr: &Self::Addr) -> Option<IpAddr> {
        L::peer_ip(addr)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use futures::StreamExt;
    use gang_clock::TestClock;
    use gang_mtls::SubjectName;
    use gang_testkit::TestSigner;
    use gang_tls::{Alpn, ClientAuth, RevocationSet};
    use gang_types::{NonZeroDuration, Percent};
    use tokio::sync::oneshot;

    use super::{Accepting, TlsPolicy};
    use crate::settings::HandshakeSettings;
    use crate::test_support::{MemoryListener, authority, memory_listener, start_clock};

    /// An endpoint whose acceptor has ended yields no connections, and its stream ends and
    /// reports that it ended.
    #[tokio::test]
    async fn an_endpoint_that_can_accept_no_more_ends_its_stream_and_says_so() {
        let clock = start_clock();
        let authority = Arc::new(authority(&clock).await);
        let (listener, _dialer) = memory_listener();
        let policy = TlsPolicy {
            names: vec![SubjectName::Dns("api.example.test".to_owned())],
            alpn: Alpn::H2,
            client_auth: ClientAuth::Optional,
            revoked_certificates: RevocationSet::from_serials(Vec::new()),
            certificate_ttl: NonZeroDuration::from_hours(1),
            reissue_at_percent: Percent::from_percent(50),
            handshake: HandshakeSettings {
                handshake_timeout: NonZeroDuration::from_secs(10),
                max_pending_handshakes: NonZeroUsize::new(256).expect("256 is non-zero"),
            },
            accept_error_backoff: NonZeroDuration::from_millis(100),
        };
        let mut accepting = Accepting::<MemoryListener, TestSigner, TestClock>::secured(
            listener,
            authority,
            policy,
            Arc::clone(&clock),
        )
        .await
        .expect("endpoint is secured");
        let Accepting::Tls(endpoint) = &mut accepting else {
            panic!("a secured endpoint is TLS")
        };
        endpoint.incoming.incoming = Box::pin(futures::stream::empty());

        let (ended, signal) = oneshot::channel();
        let mut stream = accepting.into_stream(ended);
        assert!(
            stream.next().await.is_none(),
            "an endpoint that can accept no more ends its stream"
        );
        assert_eq!(signal.await, Ok(()), "the end is reported");
    }
}
