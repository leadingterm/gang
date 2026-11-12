//! Server-side TLS: TLS 1.3 only, application protocols chosen per listener by [`Alpn`],
//! client certificates verified against the CA and checked for revocation, and live cut-off of
//! revoked peers.

use std::fmt;
use std::sync::Arc;

use gang_clock::Clock;
use gang_types::{CertificatePem, NonZeroDuration, PrivateKeyPem};
use rustls::ServerConfig;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::CertificateDer;
use rustls::server::{NoServerSessionStorage, WebPkiClientVerifier};
use rustls::version::TLS13;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::errors::TransportError;
use crate::revocation::RevocationSet;
use crate::stream::RevocableStream;
use crate::time::time_provider;
use crate::tls_material::{certificate_chain, leaf_serial, private_key_der, root_store};
use crate::verifier::RevokingClientVerifier;

/// The `h2` application protocol, offered by every [`Alpn`] choice and by the connector.
pub(crate) const ALPN_H2: &[u8] = b"h2";

/// The `http/1.1` application protocol, offered after `h2` by [`Alpn::H2AndHttp1`].
const ALPN_HTTP1: &[u8] = b"http/1.1";

/// How the server authenticates clients by certificate.
///
/// Under `Required` and `Optional` a certificate that is presented is always verified against
/// the client CA and refused when revoked; the variants differ only for clients without one.
/// Under `None` the server never asks for a certificate, so none is verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuth {
    /// A client without a certificate is refused.
    Required,
    /// A client without a certificate is accepted. Its stream has no peer identity and never
    /// revokes.
    Optional,
    /// No client certificate is requested. Every client is accepted with no peer identity, and
    /// no certificate is verified or checked for revocation.
    None,
}

/// The application protocols a listener offers during the TLS handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alpn {
    /// Only `h2`.
    H2,
    /// `h2`, then `http/1.1`.
    H2AndHttp1,
}

impl Alpn {
    /// The protocol identifiers in preference order.
    #[must_use]
    pub const fn as_slices(self) -> &'static [&'static [u8]] {
        match self {
            Self::H2 => &[ALPN_H2],
            Self::H2AndHttp1 => &[ALPN_H2, ALPN_HTTP1],
        }
    }
}

/// Server identity, client trust anchor, client authentication mode and limits for a
/// [`TlsAcceptor`].
#[derive(Clone)]
pub struct AcceptorSettings<'a> {
    /// Server leaf certificate, issued by the certificate authority. Any further certificates in
    /// the document are presented after it.
    pub certificate: &'a CertificatePem,
    /// Private key matching `certificate`.
    pub private_key: &'a PrivateKeyPem,
    /// Trust anchor for client certificates. Its first certificate is also presented to every
    /// client, after the leaf.
    pub client_ca: &'a CertificatePem,
    /// Revoked serials; a client whose certificate is revoked is rejected and cut off live.
    pub revocations: &'a RevocationSet,
    /// The longest a handshake may take.
    pub handshake_timeout: NonZeroDuration,
    /// How client certificates are requested, and whether a client without one may connect.
    pub client_auth: ClientAuth,
    /// Application protocols offered to every client during the handshake.
    pub alpn: Alpn,
    /// Clock used for certificate validity and handshake deadlines.
    pub clock: Arc<dyn Clock>,
}

/// Accepts TLS connections. Under [`ClientAuth::Required`] and [`ClientAuth::Optional`] a
/// presented client certificate must come from the configured CA; under [`ClientAuth::None`]
/// no client certificate is requested at all.
///
/// Session resumption is disabled, so every connection runs a full handshake and re-verifies
/// any requested client certificate against the current revocation set.
#[derive(Clone)]
pub struct TlsAcceptor {
    acceptor: tokio_rustls::TlsAcceptor,
    revocations: RevocationSet,
    handshake_timeout: NonZeroDuration,
    client_auth: ClientAuth,
    /// Clock shared with the incoming accept loop.
    pub(crate) clock: Arc<dyn Clock>,
}

impl fmt::Debug for TlsAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsAcceptor")
            .field("handshake_timeout", &self.handshake_timeout)
            .field("client_auth", &self.client_auth)
            .finish_non_exhaustive()
    }
}

impl TlsAcceptor {
    /// Builds the server configuration from `settings`.
    ///
    /// # Errors
    ///
    /// Returns the material or configuration error when the certificate, key or client CA is
    /// unusable.
    pub fn try_new(settings: &AcceptorSettings<'_>) -> Result<Self, TransportError> {
        let provider = Arc::new(default_provider());
        let chain = presented_chain(settings)?;
        let key = private_key_der(settings.private_key)?;
        let builder = ServerConfig::builder_with_details(
            Arc::clone(&provider),
            time_provider(Arc::clone(&settings.clock)),
        )
        .with_protocol_versions(&[&TLS13])
        .map_err(TransportError::Tls)?;
        let builder = match settings.client_auth {
            ClientAuth::None => builder.with_no_client_auth(),
            ClientAuth::Required | ClientAuth::Optional => {
                let roots = Arc::new(root_store(settings.client_ca)?);
                let verifier_builder =
                    WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider));
                let verifier_builder = if settings.client_auth == ClientAuth::Optional {
                    verifier_builder.allow_unauthenticated()
                } else {
                    verifier_builder
                };
                let client_verifier = verifier_builder
                    .build()
                    .map_err(TransportError::ClientVerifier)?;
                let verifier =
                    RevokingClientVerifier::new(client_verifier, settings.revocations.clone());
                builder.with_client_cert_verifier(Arc::new(verifier))
            }
        };
        let mut config = builder
            .with_single_cert(chain, key)
            .map_err(TransportError::Tls)?;
        config.alpn_protocols = settings
            .alpn
            .as_slices()
            .iter()
            .map(|protocol| protocol.to_vec())
            .collect();
        config.session_storage = Arc::new(NoServerSessionStorage {});
        config.send_tls13_tickets = 0;

        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
            revocations: settings.revocations.clone(),
            handshake_timeout: settings.handshake_timeout,
            client_auth: settings.client_auth,
            clock: Arc::clone(&settings.clock),
        })
    }

    /// Runs the server handshake on `io` and returns the stream, which closes as soon as the
    /// client's certificate is revoked.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::HandshakeTimeout`] when the handshake exceeds the configured
    /// timeout, and [`TransportError::Handshake`] when the handshake fails, including a presented
    /// client certificate that is refused. Under [`ClientAuth::Required`] a client without a
    /// certificate is refused, by [`TransportError::NoPeerCertificate`] if rustls ever lets it
    /// through. A presented certificate that cannot be identified fails with the leaf serial
    /// error.
    pub async fn accept<IO>(
        &self,
        io: IO,
    ) -> Result<RevocableStream<tokio_rustls::server::TlsStream<IO>>, TransportError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.handshake(io).await
    }

    /// Shared handshake for [`Self::accept`] and the incoming connection stream.
    pub(crate) async fn handshake<IO>(
        &self,
        io: IO,
    ) -> Result<RevocableStream<tokio_rustls::server::TlsStream<IO>>, TransportError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let stream = Clock::timeout(
            &self.clock,
            self.handshake_timeout.get(),
            self.acceptor.accept(io),
        )
        .await
        .map_err(|_| TransportError::HandshakeTimeout)?
        .map_err(TransportError::Handshake)?;
        let watch = match stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|chain| chain.first())
        {
            Some(leaf) => Some(self.revocations.watch(&leaf_serial(leaf)?)),
            None => match self.client_auth {
                ClientAuth::Required => return Err(TransportError::NoPeerCertificate),
                ClientAuth::Optional | ClientAuth::None => None,
            },
        };
        Ok(RevocableStream::new(stream, watch))
    }
}

/// The chain presented to every client: the server leaf, then the client CA's certificate.
///
/// The CA certificate is sent because a client that trusts the CA by pin has no CA file to build
/// the chain from; the pin is matched against this certificate.
fn presented_chain(
    settings: &AcceptorSettings<'_>,
) -> Result<Vec<CertificateDer<'static>>, TransportError> {
    let mut chain = certificate_chain(settings.certificate)?;
    let ca = certificate_chain(settings.client_ca)?
        .into_iter()
        .next()
        .ok_or(TransportError::NoCertificate)?;
    chain.push(ca);
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use gang_clock::{TestClock, Timestamp};
    use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority};

    use crate::tls_material::key_body;

    use super::*;

    #[test]
    fn alpn_slices_keep_protocol_preference_order() {
        assert_eq!(Alpn::H2.as_slices(), &[ALPN_H2]);
        assert_eq!(Alpn::H2AndHttp1.as_slices(), &[ALPN_H2, ALPN_HTTP1]);
    }

    #[test]
    fn debug_output_never_prints_the_server_private_key() {
        let authority = TestAuthority::new("gang test ca").expect("test ca builds");
        let leaf = authority
            .issue(
                vec!["localhost".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                [0x01; 16],
                "localhost",
            )
            .expect("test server leaf issues");
        let revocations = RevocationSet::default();
        let acceptor = TlsAcceptor::try_new(&AcceptorSettings {
            certificate: &leaf.pem,
            private_key: &leaf.key_pem,
            client_ca: &authority.pem,
            revocations: &revocations,
            handshake_timeout: NonZeroDuration::from_secs(10),
            client_auth: ClientAuth::Required,
            alpn: Alpn::H2,
            clock: Arc::new(TestClock::new(Timestamp::UNIX_EPOCH)),
        })
        .expect("test acceptor builds");

        let debug = format!("{acceptor:?}");
        assert!(debug.contains("TlsAcceptor"));
        assert!(!debug.contains(&key_body(&leaf.key_pem)));
    }
}
